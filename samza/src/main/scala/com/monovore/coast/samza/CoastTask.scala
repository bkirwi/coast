package com.monovore.coast
package samza

import java.util

import com.monovore.coast.samza.MessageSink.Bytes
import com.monovore.coast.wire.WireFormat
import org.apache.samza.Partition
import org.apache.samza.config.Config
import org.apache.samza.system._
import org.apache.samza.task._
import org.apache.samza.util.Logging

import scala.collection.JavaConverters._

class CoastTask extends StreamTask with InitableTask with Logging {

  var taskName: String = _

  var mergeStream: String = _

  var partitionIndex: Int = _

  var collector: MessageCollector = _

  var sink: MessageSink.ByteSink = _

  var initialSink: MessageSink.ByteSink = _

  override def init(config: Config, context: TaskContext): Unit = {

    val factory = SerializationUtil.fromBase64[MessageSink.Factory](config.get(samza.TaskKey))

    taskName = config.get(samza.TaskName)

    mergeStream = s"coast.merge.$taskName"

    partitionIndex = context.getTaskName.getTaskName.split("\\W+").last.toInt // ICK!

    val offsetThreshold = {

      val systemFactory = config.getNewInstance[SystemFactory](s"systems.$CoastSystem.samza.factory")
      val admin = systemFactory.getAdmin(CoastSystem, config)
      val meta = admin.getSystemStreamMetadata(Set(taskName).asJava).asScala
        .getOrElse(taskName, sys.error(s"Couldn't find metadata on output stream $taskName"))

      val partitionMeta = meta.getSystemStreamPartitionMetadata.asScala

      partitionMeta(new Partition(partitionIndex)).getUpcomingOffset.toLong
    }

    info(s"Starting task: [$taskName $partitionIndex] at offset $offsetThreshold")

    val finalSink = new MessageSink.ByteSink {

      val outputStream = new SystemStream(CoastSystem, taskName)

      override def execute(stream: String, partition: Int, offset: Long, key: Array[Byte], value: Array[Byte]): Long = {

        val out = new OutgoingMessageEnvelope(new SystemStream(CoastSystem, stream), util.Arrays.hashCode(key), key, value)

        collector.send(out)

        offset + 1
      }

      override def init(offset: Long): Unit = {}
    }

    val thresholdingSink = new MessageSink.ByteSink {

      override def init(offset: Long): Unit = finalSink.init(offset)

      override def execute(stream: String, partition: Int, offset: Long, key: Bytes, value: Bytes): Long = {

        if (offset >= offsetThreshold) {
          finalSink.execute(taskName, partition, offset, key, value)
        }

        offset + 1
      }
    }

    sink = factory.make(config, context, thresholdingSink)

    initialSink = new MessageSink.ByteSink {

      override def init(offset: Long): Unit = {}

      override def execute(stream: String, partition: Int, offset: Long, key: Bytes, value: Bytes): Long = {

        if (stream == mergeStream) {

          val fullMessage = WireFormat.read[FullMessage](value)

          sink.execute(fullMessage.stream, partition, fullMessage.offset, key, fullMessage.value)

        } else {

          finalSink.execute(
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

    sink.init(0L)
  }

  override def process(
    envelope: IncomingMessageEnvelope,
    collector: MessageCollector,
    coordinator: TaskCoordinator
  ): Unit = {

    val stream = envelope.getSystemStreamPartition.getSystemStream.getStream

    val partition = envelope.getSystemStreamPartition.getPartition.getPartitionId

    val offset = envelope.getOffset.toLong

    val key = envelope.getKey.asInstanceOf[Array[Byte]]

    val message = envelope.getMessage.asInstanceOf[Array[Byte]]

    this.collector = collector

    initialSink.execute(stream, partition, offset, key, message)

    this.collector = null
  }
}
