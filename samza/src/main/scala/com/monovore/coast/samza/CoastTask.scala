package com.monovore.coast
package samza

import org.apache.samza.config.Config
import org.apache.samza.system._
import org.apache.samza.task._
import org.apache.samza.util.Logging

class CoastTask extends StreamTask with InitableTask with Logging {

  var collector: MessageCollector = _

  var sink: MessageSink.ByteSink = _

  override def init(config: Config, context: TaskContext): Unit = {

    val factory = SerializationUtil.fromBase64[MessageSink.Factory](config.get(samza.TaskKey))

    val finalSink = new MessageSink.ByteSink {

      override def execute(stream: String, partition: Int, offset: Long, key: Array[Byte], value: Array[Byte]): Long = {

        val out = new OutgoingMessageEnvelope(new SystemStream(CoastSystem, stream), partition, key, value)
        collector.send(out)

        offset + 1
      }
    }

    sink = factory.make(config, context, finalSink)
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

    sink.execute(stream, partition, offset, key, message)

    this.collector = null
  }
}
