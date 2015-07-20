package com.monovore.coast
package samza
package safe

import com.monovore.coast.core._
import com.monovore.coast.samza.Path

private[samza] object TaskCompiler {

  trait Context {
    def getStore[A, B](path: String, default: B): CoastState[A, B]
  }

  type Offset = Long

  type MessageSink[-A, -B] = (Offset, A, B) => Offset

  type ByteSink = MessageSink[Array[Byte], Array[Byte]]

  case class Dispatch(
    inputStream: String,
    downstreamPath: Path,
    handler: (Offset, Array[Byte], Array[Byte]) => Offset
  )

}

class TaskCompiler(context: TaskCompiler.Context) {

  import TaskCompiler._

  def compile[A, B](node: Node[A, B], sink: MessageSink[A, B], root: String): Seq[Dispatch] =
    compile(node, sink, Path(root), Path(root))

  def compile[A, B](ent: Node[A, B], sink: MessageSink[A, B], prefix: Path, downstreamPath: Path): Seq[Dispatch] = ent match {
    case source: Source[A, B] => {

      val thing = Dispatch(source.source, downstreamPath, { (offset: Long, key: Array[Byte], value: Array[Byte]) =>

        val a = source.keyFormat.read(key)
        val b = source.valueFormat.read(value)

        sink(offset, a, b)
      })

      Seq(thing)
    }
    case clock: Clock => sys.error("Clocks not implemented yet!")
    case merge: Merge[A, B] => {

      var maxOffset: Long = 0L

      val downstreamSink = (offset: Long, key: A, value: B) => {
        maxOffset = sink(math.max(maxOffset, offset), key, value)
        maxOffset
      }

      merge.upstreams
        .flatMap { case (name, up) => compile(up, downstreamSink, prefix / name, downstreamPath) }
    }
    case pure: PureTransform[A, b0, B] => {

      val transformed = (offset: Long, key: A, value: b0) => {
        val update = pure.function(key)
        val output = update(value)
        output.foldLeft(offset)(sink(_, key, _))
      }

      compile(pure.upstream, transformed, prefix, downstreamPath)
    }
    case group: GroupBy[A, B, a0] => {

      val task = (offset: Long, key: a0, value: B) => {
        val newKey = group.groupBy(key)(value)
        sink(offset, newKey, value)
      }

      compile(group.upstream, task, prefix, downstreamPath)
    }
    case trans: StatefulTransform[s, A, b0, B] => {

      val store = context.getStore[A, s](prefix.toString, trans.init)

      val transformed = (offset: Long, key: A, value: b0) => {

        store.update(key, offset) { (downstreamOffset, state) =>

          val (newState, output) = trans.transformer(key)(state, value)

          val newDownstreamOffset = output.foldLeft(downstreamOffset)(sink(_, key, _))

          newDownstreamOffset -> newState
        }
      }

      compile(trans.upstream, transformed, prefix.next, prefix.next)
    }
  }
}