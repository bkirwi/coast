package com.monovore.coast
package samza

import org.apache.samza.config.Config
import org.apache.samza.system._
import org.apache.samza.task._
import org.apache.samza.util.Logging

class CoastTask extends StreamTask with InitableTask with WindowableTask with Logging {

  var collector: MessageCollector = _

  var receiver: CoastTask.Receiver = _

  override def init(config: Config, context: TaskContext): Unit = {

    info("Initializing CoastTask...")

    val factory = SerializationUtil.fromBase64[CoastTask.Factory](config.get(samza.TaskKey))

    val finalReceiver = new CoastTask.Receiver {

      override def send(stream: SystemStream, partition: Int, offset: Long, key: Array[Byte], value: Array[Byte]) {

        val out = new OutgoingMessageEnvelope(stream, partition, key, value)

        collector.send(out)
      }
    }

    receiver = factory.make(config, context, finalReceiver)

    info("Initialization complete.")
  }

  override def process(
    envelope: IncomingMessageEnvelope,
    collector: MessageCollector,
    coordinator: TaskCoordinator
  ): Unit = {

    val stream = envelope.getSystemStreamPartition.getSystemStream
    val partition = envelope.getSystemStreamPartition.getPartition.getPartitionId
    val offset = envelope.getOffset.toLong
    val key = envelope.getKey.asInstanceOf[Array[Byte]]
    val message = envelope.getMessage.asInstanceOf[Array[Byte]]

    this.collector = collector

    receiver.send(stream, partition, offset, key, message)

    this.collector = null
  }

  override def window(collector: MessageCollector, coordinator: TaskCoordinator): Unit = {

    this.collector = collector

    receiver.window()

    this.collector = null
  }
}

object CoastTask {

  trait Receiver {
    def send(systemStream: SystemStream, partition: Int, offset: Long, key: Array[Byte], value: Array[Byte])

    def window() {}
  }

  trait Factory extends Serializable {
    def make(config: Config, context: TaskContext, receiver: Receiver): Receiver
  }
}
