package com.monovore.coast.samza

import com.monovore.coast.model.Sink
import com.monovore.coast.samza
import com.monovore.coast.samza.MessageSink._
import com.monovore.coast.wire.BinaryFormat
import org.apache.samza.Partition
import org.apache.samza.config.{MapConfig, Config}
import org.apache.samza.system.SystemFactory
import org.apache.samza.task.TaskContext

import scala.collection.JavaConverters._

object Safe extends (Config => ConfigGenerator) {

  def apply(baseConfig: Config = new MapConfig()): ConfigGenerator = new SafeConfigGenerator(baseConfig)

  class SinkFactory[A, B](sinkNode: Sink[A, B]) extends MessageSink.Factory {

    override def make(config: Config, context: TaskContext, whatSink: ByteSink) = {

      val taskName = config.get(samza.TaskName)

      val partitionIndex = context.getTaskName.getTaskName.split("\\W+").last.toInt // ICK!

      val regroupedStreams = config.get(RegroupedStreams).split(",")
        .filter { _.nonEmpty }
        .toSet

      // Left(size) if the stream is regrouped, Right(offset) if it isn't
      val (numPartitions, offsetThreshold) = {

        val systemFactory = config.getNewInstance[SystemFactory](s"systems.$CoastSystem.samza.factory")
        val admin = systemFactory.getAdmin(CoastSystem, config)
        val meta = admin.getSystemStreamMetadata(Set(taskName).asJava).asScala
          .getOrElse(taskName, sys.error(s"Couldn't find metadata on output stream $taskName"))

        val partitionMeta = meta.getSystemStreamPartitionMetadata.asScala

        partitionMeta.size -> {
          if (regroupedStreams(taskName)) 0L
          else partitionMeta(new Partition(partitionIndex)).getUpcomingOffset.toLong
        }
      }

      val finalSink = new MessageSink.ByteSink {

        override def execute(stream: String, partition: Int, offset: Long, key: Bytes, value: Bytes): Long = {

          val payload =
            if (regroupedStreams(taskName)) {
              BinaryFormat.write(FullMessage(taskName, partitionIndex, offset, value))
            } else {
              value
            }

          if (offset >= offsetThreshold) {
            whatSink.execute(taskName, partition, offset, key, payload)
          }

          offset + 1
        }
      }

      val name = config.get(samza.TaskName)

      val compiler = new TaskCompiler(new TaskCompiler.Context {
        override def getStore[P, A, B](path: String, default: B): CoastState[Int, A, B] =
          context.getStore(path).asInstanceOf[CoastStorageEngine[A, B]].withDefault(default)
      })

      val compiled = compiler.compileSink(sinkNode, finalSink, name, numPartitions)

      val mergeStream = s"coast.merge.$taskName"

      new MessageSink.ByteSink {

        override def execute(stream: String, partition: Int, offset: Long, key: Bytes, value: Bytes): Long = {

          if (stream == mergeStream) {

            val fullMessage = BinaryFormat.read[FullMessage](value)

            compiled.execute(fullMessage.stream, partition, fullMessage.offset, key, fullMessage.value)

          } else {

            // Ensure the message is framed
            // If it comes from a regrouped stream, it's framed already
            val valueBytes =
              if (regroupedStreams(stream)) value
              else BinaryFormat.write(FullMessage(stream, 0, offset, value))

            whatSink.execute(
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
