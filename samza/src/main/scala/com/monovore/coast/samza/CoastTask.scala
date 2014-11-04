package com.monovore.coast
package samza

import org.apache.samza.config.Config
import org.apache.samza.system.{SystemStream, OutgoingMessageEnvelope, IncomingMessageEnvelope}
import org.apache.samza.task.TaskCoordinator.RequestScope
import org.apache.samza.task._

class CoastTask extends StreamTask with InitableTask {

  var collector: MessageCollector = _
  var sink: MessageSink.ByteSink = _

  override def init(config: Config, context: TaskContext): Unit = {

    val factory = SerializationUtil.fromBase64[MessageSink.Factory](config.get(samza.TaskKey))
    val output = config.get(samza.TaskName)

    val finalSink = new MessageSink.ByteSink {

      override def execute(stream: String, key: Array[Byte], value: Array[Byte]): Unit = {

        val out = new OutgoingMessageEnvelope(new SystemStream("kafka", output), key, value)

        collector.send(out)
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
    val key = Option(envelope.getKey.asInstanceOf[Array[Byte]]).getOrElse(Array.empty[Byte])
    val message = envelope.getMessage.asInstanceOf[Array[Byte]]

    this.collector = collector

    sink.execute(stream, key, message)

    coordinator.commit(RequestScope.CURRENT_TASK)
  }
}
