package com.monovore.coast.samza

import com.monovore.coast._
import com.monovore.coast.model._
import org.apache.samza.util.Logging

private[samza] object TaskCompiler {

  trait Context {
    def getStore[P, A, B](path: String, default: B): CoastState[Int, A, B]
  }
}

private[samza] class TaskCompiler(context: TaskCompiler.Context) {

  import MessageSink.{Bytes, ByteSink}

  def compileSource[A, B](source: Source[A, B], sink: MessageSink[A, B], prefix: List[String]) = {

    val store = context.getStore[Int, Unit, Unit](formatPath(prefix), unit)

    store.downstream -> new MessageSink[Bytes, Bytes] with Logging {

      override def execute(stream: String, partition: Int, offset: Long, key: Bytes, value: Bytes): Long = {

        if (stream == source.source) {
          
          store.update(partition, unit, offset) { (downstreamOffset, _) =>

            val a = source.keyFormat.read(key)
            val b = source.valueFormat.read(value)

            sink.execute(stream, 0, downstreamOffset, a, b) -> unit
          }
        } else offset
      }
    }
  }

  def compilePure[A, B, B0](trans: PureTransform[A, B0, B], sink: MessageSink[A, B], prefix: List[String]) = {

    val transformed = new MessageSink[A, B0] {

      override def execute(stream: String, partition: Int, offset: Long, key: A, value: B0): Long = {
        val update = trans.function(key)
        val output = update(value)
        output.foldLeft(offset)(sink.execute(stream, partition, _, key, _))
      }
    }

    compile(trans.upstream, transformed, prefix)
  }

  def compileAggregate[S, A, B, B0](trans: Aggregate[S, A, B0, B], sink: MessageSink[A, B], prefix: List[String]) = {

    val transformed = new MessageSink[A, B0] with Logging {

      val store = context.getStore[Int, A, S](samza.formatPath(prefix), trans.init)

      override def execute(stream: String, partition: Int, offset: Long, key: A, value: B0): Long = {

        store.update(partition, key, offset) { (downstreamOffset, state) =>

          val update = trans.transformer(key)

          val (newState, output) = update(state, value)

          val newDownstreamOffset = output.foldLeft(downstreamOffset)(sink.execute(stream, partition, _, key, _))

          newDownstreamOffset -> newState
        }
      }
    }

    compile(trans.upstream, transformed, "aggregated" :: prefix)
  }

  def compileGroupBy[A, B, A0](gb: GroupBy[A, B, A0], sink: MessageSink[A, B], prefix: List[String]) = {

    val task = new MessageSink[A0, B] {

      override def execute(stream: String, partition: Int, offset: Long, key: A0, value: B): Long = {
        val newKey = gb.groupBy(key)(value)
        sink.execute(stream, partition, offset, newKey, value)
      }
    }

    compile(gb.upstream, task, prefix)
  }

  def compileMerge[A, B](merge: Merge[A, B], sink: MessageSink[A, B], prefix: List[String]) = {

    var maxOffset: Long = 0L

    val downstreamSink = new MessageSink[A, B] with Logging {

      override def execute(stream: String, partition: Int, offset: Long, key: A, value: B): Long = {
        maxOffset = math.max(maxOffset, offset)
        maxOffset = sink.execute(stream, partition, maxOffset, key, value)
        maxOffset
      }
    }

    val (offsets, upstreamSinks) = merge.upstreams
      .map { case (name, up) => compile(up, downstreamSink, name :: prefix) }
      .unzip

    maxOffset = offsets.max

    offsets.max -> new MessageSink[Bytes, Bytes] {

      override def execute(stream: String, partition: Int, offset: Long, key: Bytes, value: Bytes): Long = {

        upstreamSinks.foreach { s => s.execute(stream, partition, offset, key, value) }

        offset
      }
    }
  }

  def compile[A, B](ent: Node[A, B], sink: MessageSink[A, B], prefix: List[String]): Long -> ByteSink = {

    ent match {
      case source @ Source(_) => compileSource(source, sink, prefix)
      case merge @ Merge(_) => compileMerge(merge, sink, prefix)
      case trans @ Aggregate(_, _, _) => compileAggregate(trans, sink, prefix)
      case pure @ PureTransform(_, _) => compilePure(pure, sink, prefix)
      case group @ GroupBy(_, _) => compileGroupBy(group, sink, prefix)
    }
  }

  def compileSink[A, B](sink: Sink[A, B], messageSink: ByteSink, name: String): ByteSink = {

    val formatted = new MessageSink[A, B] with Logging {

      var nextOffset: Long = _

      override def execute(stream: String, partition: Int, offset: Long, key: A, value: B): Long = {

        val keyBytes = sink.keyFormat.write(key)
        val valueBytes = sink.valueFormat.write(value)

        val newPartition = sink.keyPartitioner.hash(key).asInt()

        messageSink.execute(stream, newPartition, offset, keyBytes, valueBytes)
      }
    }

    val (_, compiledNode) = compile(sink.element, formatted, name :: Nil)

    compiledNode
  }
}
