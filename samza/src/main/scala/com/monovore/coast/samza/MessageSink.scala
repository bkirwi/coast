package com.monovore.coast
package samza

import com.monovore.coast.model._
import org.apache.samza.config.Config
import org.apache.samza.task.TaskContext
import org.apache.samza.util.Logging

trait MessageSink[-K, -V] extends Serializable {

  def execute(stream: String, offset: Long, key: K, value: V): Long

  def flush(): Unit

}

object MessageSink {

  type Bytes = Array[Byte]
  type ByteSink = MessageSink[Array[Byte], Array[Byte]]

  trait Factory extends Serializable {
    def make(config: Config, context: TaskContext, sink: ByteSink): ByteSink
  }

  class FromElement[A, B](sink: Sink[A, B]) extends Factory {

    override def make(config: Config, context: TaskContext, finalSink: ByteSink) = {

      def compileSource[A, B](source: Source[A, B], sink: MessageSink[A, B], prefix: List[String]) = {

        // TODO: offset pairs!

        new MessageSink[Bytes, Bytes] with Logging {

          val store = context.getStore(formatPath(prefix)).asInstanceOf[CoastStore[Unit, Long]]

          info(s"Restored ${formatPath(prefix)} with next offset ${store.nextOffset}")

          override def execute(stream: String, offset: Long, key: Bytes, value: Bytes): Long = {

            if (offset >= store.nextOffset) {

              val (_, downstreamOffset) = store.getOrElse(unit, 0L)

              if (downstreamOffset == 0 && store.nextOffset != 0) sys.error(s"$downstreamOffset ${store.nextOffset}")

              val a = source.keyFormat.read(key)
              val b = source.valueFormat.read(value)

              val nextOffset = sink.execute(stream, downstreamOffset, a, b)

              store.put(unit, nextOffset)

            }

            store.nextOffset
          }

          override def flush(): Unit = {
            sink.flush()
            store.flush()
          }
        }
      }

      def compilePure[A, B, B0](trans: PureTransform[A, B0, B], sink: MessageSink[A, B], prefix: List[String]) = {

        val transformed = new MessageSink[A, B0] {

          override def execute(stream: String, offset: Long, key: A, value: B0): Long = {
            val update = trans.function(key)
            val output = update(value)
            output.foldLeft(offset)(sink.execute(stream, _, key, _))
          }

          override def flush(): Unit = sink.flush()
        }

        compile(trans.upstream, transformed, prefix)
      }

      def compileAggregate[S, A, B, B0](trans: Aggregate[S, A, B0, B], sink: MessageSink[A, B], prefix: List[String]) = {
//
//        val store = context.getStore(samza.formatPath(prefix)).asInstanceOf[KeyValueStore[A, S]]
//
//        val transformed = new MessageSink[A, B0] {
//
//          override def execute(stream: String, key: A, value: B0): Unit = {
//
//            val update = trans.transformer(key)
//
//            val state = Option(store.get(key))
//              .getOrElse(trans.init)
//
//            val (newState, output) = update(state, value)
//            store.put(key, newState)
//            output.foreach(sink.execute(stream, key, _))
//          }
//
//          override def nextOffset(offset: Long): Unit = {
//
//          }
//        }
//
//        compile(trans.upstream, transformed, "aggregated" :: prefix)
        ???
      }

      def compileGroupBy[A, B, A0](gb: GroupBy[A, B, A0], sink: MessageSink[A, B], prefix: List[String]) = {

        val task = new MessageSink[A0, B] {

          override def execute(stream: String, offset: Long, key: A0, value: B): Long = {
            val newKey = gb.groupBy(value)
            sink.execute(stream, offset, newKey, value)
          }

          override def flush(): Unit = sink.flush()
        }

        compile(gb.upstream, task, prefix)
      }

      def compileMerge[A, B](merge: Merge[A, B], sink: MessageSink[A, B], prefix: List[String]) = {
//
//        val upstreamSinks = merge.upstreams.zipWithIndex
//          .map { case (up, i) => compile(up, sink, s"merged-$i" :: prefix) }
//
//        new MessageSink[Bytes, Bytes] {
//
//          override def execute(stream: String, key: Bytes, value: Bytes): Unit = {
//
//            upstreamSinks.foreach { _.execute(stream, key, value) }
//          }
//
//          override def nextOffset(offset: Long): Unit = sink.nextOffset(offset)
//        }
        ???
      }

      def compile[A, B](ent: Node[A, B], sink: MessageSink[A, B], prefix: List[String]): ByteSink = {

        ent match {
          case source @ Source(_) => compileSource(source, sink, prefix)
          case merge @ Merge(_) => compileMerge(merge, sink, prefix)
          case trans @ Aggregate(_, _, _) => compileAggregate(trans, sink, prefix)
          case pure @ PureTransform(_, _) => compilePure(pure, sink, prefix)
          case group @ GroupBy(_, _) => compileGroupBy(group, sink, prefix)
        }
      }

      val last = new MessageSink[A, B] with Logging {

        var nextOffset: Long = _

//        val storage = context.getStore("offsets").asInstanceOf[CoastStore[Unit, Unit]]

//        info(s"Starting thing at ${storage.nextOffset}")

        override def execute(stream: String, offset: Long, key: A, value: B): Long = {

          val keyBytes = sink.keyFormat.write(key)
          val valueBytes = sink.valueFormat.write(value)

          finalSink.execute(stream, offset, keyBytes, valueBytes)
        }

        override def flush(): Unit = finalSink.flush()
      }

      val name = config.get(samza.TaskName)

      compile(sink.element, last, List(name))
    }
  }
}