package com.monovore.coast
package samza

import com.monovore.coast.model._
import com.monovore.coast.wire.WireFormat
import org.apache.samza.Partition
import org.apache.samza.config.Config
import org.apache.samza.system.SystemFactory
import org.apache.samza.task.TaskContext
import org.apache.samza.util.Logging

import collection.JavaConverters._

trait MessageSink[-K, -V] extends Serializable {

  def execute(stream: String, partition: Int, offset: Long, key: K, value: V): Long
}

object MessageSink {

  type Bytes = Array[Byte]
  type ByteSink = MessageSink[Bytes, Bytes]

  trait Factory extends Serializable {
    def make(config: Config, context: TaskContext, sink: ByteSink): ByteSink
  }

  class FromElement[A, B](sink: Sink[A, B]) extends Factory {

    override def make(config: Config, context: TaskContext, whatSink: ByteSink) = {

      val taskName = config.get(samza.TaskName)

      val partitionIndex = context.getTaskName.getTaskName.split("\\W+").last.toInt // ICK!

      val offsetThreshold = {

        val systemFactory = config.getNewInstance[SystemFactory](s"systems.$CoastSystem.samza.factory")
        val admin = systemFactory.getAdmin(CoastSystem, config)
        val meta = admin.getSystemStreamMetadata(Set(taskName).asJava).asScala
          .getOrElse(taskName, sys.error(s"Couldn't find metadata on output stream $taskName"))

        val partitionMeta = meta.getSystemStreamPartitionMetadata.asScala

        partitionMeta(new Partition(partitionIndex)).getUpcomingOffset.toLong
      }

      val finalSink = new MessageSink.ByteSink {

        override def execute(stream: String, partition: Int, offset: Long, key: Bytes, value: Bytes): Long = {

          if (offset >= offsetThreshold) {
            whatSink.execute(taskName, partition, offset, key, value)
          }

          offset + 1
        }
      }

      def compileSource[A, B](source: Source[A, B], sink: MessageSink[A, B], prefix: List[String]) = {

        val store = context.getStore(formatPath(prefix)).asInstanceOf[CoastStore[Unit, Unit]]

        store.downstreamOffset -> new MessageSink[Bytes, Bytes] with Logging {

          override def execute(stream: String, partition: Int, offset: Long, key: Bytes, value: Bytes): Long = {

            if (stream == source.source) {
              store.handle(offset, unit, unit) { (downstreamOffset, _) =>

                val a = source.keyFormat.read(key)
                val b = source.valueFormat.read(value)

                sink.execute(stream, partition, downstreamOffset, a, b) -> unit
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

          val store = context.getStore(samza.formatPath(prefix)).asInstanceOf[CoastStore[A, S]]

          override def execute(stream: String, partition: Int, offset: Long, key: A, value: B0): Long = {
            store.handle(offset, key, trans.init) { (downstreamOffset, state) =>

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

        maxOffset = offsets.max // FIXME: This seems to not matter?

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

      val last = new MessageSink[A, B] with Logging {

        var nextOffset: Long = _

        override def execute(stream: String, partition: Int, offset: Long, key: A, value: B): Long = {

          val keyBytes = sink.keyFormat.write(key)
          val valueBytes = sink.valueFormat.write(value)

          finalSink.execute(stream, partition, offset, keyBytes, valueBytes)
        }
      }

      val name = config.get(samza.TaskName)

      val (_, thing) = compile(sink.element, last, List(name))

      val mergeStream = s"coast.merge.$taskName"

      new MessageSink.ByteSink {

        override def execute(stream: String, partition: Int, offset: Long, key: Bytes, value: Bytes): Long = {

          if (stream == mergeStream) {

            val fullMessage = WireFormat.read[FullMessage](value)

            thing.execute(fullMessage.stream, partition, fullMessage.offset, key, fullMessage.value)

          } else {

            whatSink.execute(
              mergeStream,
              partitionIndex,
              0,
              key,
              WireFormat.write(
                FullMessage(stream, 0, offset, value)
              )
            )
          }
        }
      }
    }
  }
}