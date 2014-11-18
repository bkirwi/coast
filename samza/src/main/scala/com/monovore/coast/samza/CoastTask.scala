package com.monovore.coast
package samza

import com.monovore.coast.format.WireFormat
import org.apache.samza.config.Config
import org.apache.samza.system._
import org.apache.samza.task._
import org.apache.samza.util.Logging

import scala.collection.JavaConverters._

class CoastTask extends StreamTask with InitableTask with WindowableTask with Logging {

  var taskName: String = _

  var mergeStream: String = _

  var collector: MessageCollector = _

  var sink: MessageSink.ByteSink = _

  override def init(config: Config, context: TaskContext): Unit = {

    val factory = SerializationUtil.fromBase64[MessageSink.Factory](config.get(samza.TaskKey))

    taskName = config.get(samza.TaskName)

    mergeStream = s"coast.merge.$taskName"

    val offsetThreshold = {

      val systemFactory = config.getNewInstance[SystemFactory](s"systems.$CoastSystem.samza.factory")
      val admin = systemFactory.getAdmin(CoastSystem, config)
      val meta = admin.getSystemStreamMetadata(Set(taskName).asJava).asScala
        .getOrElse(taskName, sys.error(s"Couldn't find metadata on output stream $taskName"))
      val partitionMeta = meta.getSystemStreamPartitionMetadata.asScala
      assert(partitionMeta.size == 1, "FIXME: assuming a single partition here")
      partitionMeta.values.head.getUpcomingOffset.toLong
    }

    info(s"Starting task with output offset of $offsetThreshold")

    val finalSink = new MessageSink.ByteSink {

      val outputStream = new SystemStream(CoastSystem, taskName)

      override def execute(stream: String, offset: Long, key: Array[Byte], value: Array[Byte]): Long = {

        if (offset >= offsetThreshold) {
          val out = new OutgoingMessageEnvelope(outputStream, key, value)
          collector.send(out)
        }

        offset + 1
      }

      override def flush(): Unit = {
        debug(s"Flushing output stream $outputStream")
      }

      override def init(offset: Long): Unit = {}
    }

    sink = factory.make(config, context, finalSink)

    sink.init(0L)
  }

  override def process(
    envelope: IncomingMessageEnvelope,
    collector: MessageCollector,
    coordinator: TaskCoordinator
  ): Unit = {

    if (envelope.getSystemStreamPartition.getSystemStream.getStream == mergeStream) {

      val key = Option(envelope.getKey.asInstanceOf[Array[Byte]]).getOrElse(Array.empty[Byte])

      val message = envelope.getMessage.asInstanceOf[Array[Byte]]

      val fullMessage = WireFormat.read[FullMessage](message)

      this.collector = collector

      sink.execute(fullMessage.stream, fullMessage.offset, key, fullMessage.value)

    } else {

      val inputOffset = envelope.getOffset.toLong

      val stream = envelope.getSystemStreamPartition.getSystemStream.getStream
      val key = Option(envelope.getKey.asInstanceOf[Array[Byte]]).getOrElse(Array.empty[Byte])
      val message = envelope.getMessage.asInstanceOf[Array[Byte]]

      collector.send(new OutgoingMessageEnvelope(
        new SystemStream(CoastSystem, mergeStream),
        envelope.getSystemStreamPartition.getPartition.getPartitionId,
        key,
        WireFormat.write(
          FullMessage(stream, 0, inputOffset, message)
        )
      ))
    }
  }

  override def window(collector: MessageCollector, coordinator: TaskCoordinator): Unit = {

//    this.collector = collector

//    sink.flush()

//    coordinator.commit(TaskCoordinator.RequestScope.CURRENT_TASK)
  }
}
