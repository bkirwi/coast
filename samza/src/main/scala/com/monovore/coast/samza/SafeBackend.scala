package com.monovore.coast
package samza

import model._
import com.monovore.coast.wire.BinaryFormat
import org.apache.samza.config.{Config, MapConfig}
import org.apache.samza.task.TaskContext
import org.apache.samza.util.Logging

object SafeBackend extends SamzaBackend {

  def apply(baseConfig: Config = new MapConfig()): ConfigGenerator = new SafeConfigGenerator(baseConfig)

  class SinkFactory[A, B](sinkNode: Sink[A, B]) extends CoastTask.Factory {

    override def make(config: Config, context: TaskContext, whatSink: CoastTask.Receiver): CoastTask.Receiver = {

      val streamName = config.get(samza.TaskName)

      val partitionIndex = context.getTaskName.getTaskName.split("\\W+").last.toInt // ICK!

      val regroupedStreams = config.get(RegroupedStreams).split(",")
        .filter { _.nonEmpty }
        .toSet

      val partitions = SamzaBackend.getPartitions(config, CoastSystem, streamName)

      val offsetThreshold =
        if (regroupedStreams(streamName)) 0L
        else partitions(partitionIndex)

      val finalSink = (stream: String, partition: Int, offset: Long, key: Array[Byte], value: Array[Byte]) => {

        val payload =
          if (regroupedStreams(streamName)) {
            BinaryFormat.write(FullMessage(streamName, partitionIndex, offset, value))
          }
          else value

        if (offset >= offsetThreshold) {
          whatSink.send(streamName, partition, offset, key, payload)
        }

        offset + 1
      }

      val compiler = new TaskCompiler(new TaskCompiler.Context {
        override def getStore[P, A, B](path: String, default: B): CoastState[Int, A, B] =
          context.getStore(path).asInstanceOf[CoastStorageEngine[A, B]].withDefault(default)
      })

      val compiled = compiler.compileSink(sinkNode, finalSink, streamName, partitions.size)

      val mergeStream = s"coast.merge.$streamName"

      new CoastTask.Receiver {

        override def send(stream: String, partition: Int, offset: Long, key: Array[Byte], value: Array[Byte]) {

          if (stream == mergeStream) {

            val fullMessage = BinaryFormat.read[FullMessage](value)

            compiled(fullMessage.stream, partition, fullMessage.offset, key, fullMessage.value)

          } else {

            // Ensure the message is framed
            // If it comes from a regrouped stream, it's framed already
            val valueBytes =
              if (regroupedStreams(stream)) value
              else BinaryFormat.write(FullMessage(stream, 0, offset, value))

            whatSink.send(
              mergeStream,
              partitionIndex,
              -1,
              key,
              valueBytes
            )
          }
        }
      }
    }
  }
}

private[samza] object TaskCompiler {

  trait Context {
    def getStore[P, A, B](path: String, default: B): CoastState[Int, A, B]
  }

  type Offset = Long
  type MessageSink[-A, -B] = (String, Int, Offset, A, B) => Offset
  type ByteSink = MessageSink[Array[Byte], Array[Byte]]

}

private[samza] class TaskCompiler(context: TaskCompiler.Context) {

  import TaskCompiler._

  def compileSource[A, B](source: Source[A, B], sink: MessageSink[A, B], prefix: Path) = {

    val store = context.getStore[Int, Unit, Unit](prefix.toString, unit)

    (stream: String, partition: Int, offset: Long, key: Array[Byte], value: Array[Byte]) => {

      if (stream == source.source) {

        store.update(partition, unit, offset) { (downstreamOffset, _) =>

          val a = source.keyFormat.read(key)
          val b = source.valueFormat.read(value)

          sink(stream, 0, downstreamOffset, a, b) -> unit
        }
      } else offset
    }
  }

  def compilePure[A, B, B0](trans: PureTransform[A, B0, B], sink: MessageSink[A, B], prefix: Path) = {

    val transformed = (stream: String, partition: Int, offset: Long, key: A, value: B0) => {
      val update = trans.function(key)
      val output = update(value)
      output.foldLeft(offset)(sink(stream, partition, _, key, _))
    }

    compile(trans.upstream, transformed, prefix)
  }

  def compileStateTrans[S, A, B, B0](trans: StatefulTransform[S, A, B0, B], sink: MessageSink[A, B], prefix: Path) = {

    val store = context.getStore[Int, A, S](prefix.toString, trans.init)

    val transformed = (stream: String, partition: Int, offset: Long, key: A, value: B0) => {

      store.update(partition, key, offset) { (downstreamOffset, state) =>

        val update = trans.transformer(key)

        val (newState, output) = update(state, value)

        val newDownstreamOffset = output.foldLeft(downstreamOffset)(sink(stream, partition, _, key, _))

        newDownstreamOffset -> newState
      }
    }

    compile(trans.upstream, transformed, prefix.next)
  }

  def compileGroupBy[A, B, A0](gb: GroupBy[A, B, A0], sink: MessageSink[A, B], prefix: Path) = {

    val task = (stream: String, partition: Int, offset: Long, key: A0, value: B) => {
      val newKey = gb.groupBy(key)(value)
      sink(stream, partition, offset, newKey, value)
    }

    compile(gb.upstream, task, prefix)
  }

  def compileMerge[A, B](merge: Merge[A, B], sink: MessageSink[A, B], prefix: Path) = {

    var maxOffset: Long = 0L

    val downstreamSink = (stream: String, partition: Int, offset: Long, key: A, value: B) => {
      maxOffset = sink(stream, partition, math.max(maxOffset, offset), key, value)
      maxOffset
    }

    val upstreamSinks = merge.upstreams
      .map { case (name, up) => compile(up, downstreamSink, prefix / name) }

    (stream: String, partition: Int, offset: Long, key: Array[Byte], value: Array[Byte]) => {

      upstreamSinks.foreach { s => s(stream, partition, offset, key, value) }

      offset
    }
  }

  def compile[A, B](ent: Node[A, B], sink: MessageSink[A, B], prefix: Path): ByteSink = {

    ent match {
      case source @ Source(_) => compileSource(source, sink, prefix)
      case merge @ Merge(_) => compileMerge(merge, sink, prefix)
      case trans @ StatefulTransform(_, _, _) => compileStateTrans(trans, sink, prefix)
      case pure @ PureTransform(_, _) => compilePure(pure, sink, prefix)
      case group @ GroupBy(_, _) => compileGroupBy(group, sink, prefix)
    }
  }

  def compileSink[A, B](sink: Sink[A, B], messageSink: ByteSink, name: String, partitions: Int): ByteSink = {

    var nextOffset: Long = 0L

    val formatted = (stream: String, partition: Int, offset: Long, key: A, value: B) => {

      val keyBytes = sink.keyFormat.write(key)
      val valueBytes = sink.valueFormat.write(value)

      val newPartition = sink.keyPartitioner.partition(key, partitions)

      messageSink(name, newPartition, offset, keyBytes, valueBytes)
    }

    val compiledNode = compile(sink.element, formatted, Path(name))

    compiledNode
  }
}

