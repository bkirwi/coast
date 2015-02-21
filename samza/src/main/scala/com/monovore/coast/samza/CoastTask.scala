package com.monovore.coast
package samza

import org.apache.samza.config.Config
import org.apache.samza.system._
import org.apache.samza.task._
import org.apache.samza.util.Logging

class CoastTask extends StreamTask with InitableTask with Logging {

  var collector: MessageCollector = _

  var receiver: CoastTask.Receiver = _

  override def init(config: Config, context: TaskContext): Unit = {

    val factory = SerializationUtil.fromBase64[CoastTask.Factory](config.get(samza.TaskKey))

    val finalReceiver = new CoastTask.Receiver {

      override def send(stream: String, partition: Int, offset: Long, key: Array[Byte], value: Array[Byte]) {

        val out = new OutgoingMessageEnvelope(new SystemStream(CoastSystem, stream), partition, key, value)

        collector.send(out)
      }
    }

    receiver = factory.make(config, context, finalReceiver)
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

    receiver.send(stream, partition, offset, key, message)

    this.collector = null
  }
}

object CoastTask {

  trait Receiver {
    def send(stream: String, partition: Int, offset: Long, key: Array[Byte], value: Array[Byte])
  }

  trait Factory extends Serializable {
    def make(config: Config, context: TaskContext, receiver: Receiver): Receiver
  }
}
