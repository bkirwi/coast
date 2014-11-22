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

  var collector: MessageCollector = _

  var initialSink: MessageSink.ByteSink = _

  override def init(config: Config, context: TaskContext): Unit = {

    val factory = SerializationUtil.fromBase64[MessageSink.Factory](config.get(samza.TaskKey))

    val finalSink = new MessageSink.ByteSink {

      override def execute(stream: String, partition: Int, offset: Long, key: Array[Byte], value: Array[Byte]): Long = {

        val out = new OutgoingMessageEnvelope(new SystemStream(CoastSystem, stream), util.Arrays.hashCode(key), key, value)

        collector.send(out)

        offset + 1
      }
    }

    initialSink = factory.make(config, context, finalSink)
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
