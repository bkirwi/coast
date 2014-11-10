package com.monovore.coast
package samza

import org.apache.samza.config.Config
import org.apache.samza.system._
import org.apache.samza.task._
import org.apache.samza.util.Logging

import scala.collection.JavaConverters._

class CoastTask extends StreamTask with InitableTask with WindowableTask with Logging {

  var collector: MessageCollector = _

  var sink: MessageSink.ByteSink = _

  override def init(config: Config, context: TaskContext): Unit = {

    val factory = SerializationUtil.fromBase64[MessageSink.Factory](config.get(samza.TaskKey))

    val output = config.get(samza.TaskName)

    val offsetThreshold = {

      val systemFactory = config.getNewInstance[SystemFactory]("systems.kafka.samza.factory")
      val admin = systemFactory.getAdmin("kafka", config)
      val meta = admin.getSystemStreamMetadata(Set(output).asJava).asScala
        .getOrElse(output, sys.error(s"Couldn't find metadata on output stream $output"))
      val partitionMeta = meta.getSystemStreamPartitionMetadata.asScala
      assert(partitionMeta.size == 1, "FIXME: assuming a single partition here")
      partitionMeta.values.head.getUpcomingOffset.toLong
    }

    info(s"Starting task with output offset of $offsetThreshold")

    val finalSink = new MessageSink.ByteSink {

      val outputStream = new SystemStream("kafka", output)

      override def execute(stream: String, offset: Long, key: Array[Byte], value: Array[Byte]): Long = {

        if (offset >= offsetThreshold) {
          val out = new OutgoingMessageEnvelope(outputStream, key, value)
          collector.send(out)
        }

        offset + 1
      }

      override def flush(): Unit = {
        info(s"Flushing output stream $outputStream")
        collector.flush(outputStream)
      }
    }

    sink = factory.make(config, context, finalSink)
  }

  override def process(
    envelope: IncomingMessageEnvelope,
    collector: MessageCollector,
    coordinator: TaskCoordinator
  ): Unit = {

    val inputOffset = envelope.getOffset.toLong

    val stream = envelope.getSystemStreamPartition.getSystemStream.getStream
    val key = Option(envelope.getKey.asInstanceOf[Array[Byte]]).getOrElse(Array.empty[Byte])
    val message = envelope.getMessage.asInstanceOf[Array[Byte]]

    this.collector = collector

    sink.execute(stream, inputOffset, key, message)
  }

  override def window(collector: MessageCollector, coordinator: TaskCoordinator): Unit = {

    this.collector = collector

    sink.flush()

    coordinator.commit(TaskCoordinator.RequestScope.CURRENT_TASK)
  }
}
