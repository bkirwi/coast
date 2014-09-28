package com.monovore.coast
package samza

import org.apache.samza.config.Config
import org.apache.samza.system.{SystemStream, OutgoingMessageEnvelope, IncomingMessageEnvelope}
import org.apache.samza.task._

class Whatever extends StreamTask with InitableTask {

  override def init(config: Config, context: TaskContext): Unit = {
    val thing = config.get("string")
  }

  override def process(
    envelope: IncomingMessageEnvelope,
    collector: MessageCollector,
    coordinator: TaskCoordinator
  ): Unit = {

    val key = envelope.getKey
    val offset = envelope.getOffset
    val message = envelope.getMessage

    collector.send(new OutgoingMessageEnvelope(
      new SystemStream("system", "stream"),
      "partitionKey",
      "key",
      "message"
    ))
  }
}
