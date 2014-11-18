package com.monovore.coast.samza

import java.io.File
import java.util

import com.google.common.primitives.Longs
import org.apache.samza.container.SamzaContainerContext
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.serializers.Serde
import org.apache.samza.storage.kv.{KeyValueStore, SerializedKeyValueStore, BaseKeyValueStorageEngineFactory, KeyValueStorageEngine}
import org.apache.samza.storage.{StorageEngineFactory, StorageEngine}
import org.apache.samza.system.{OutgoingMessageEnvelope, SystemStreamPartition, IncomingMessageEnvelope}
import org.apache.samza.task.MessageCollector
import org.apache.samza.util.Logging

import collection.JavaConverters._

class CoastStore[A, B](
  underlying: KeyValueStore[A, B],
  keySerde: Serde[A],
  valueSerde: Serde[B],
  collector: MessageCollector,
  ssp: SystemStreamPartition
) extends StorageEngine with Logging {

  val partition: Int = ssp.getPartition.getPartitionId

  var nextOffset: Long = 0L

  var downstreamOffset: Long = 0L

  override def restore(messages: util.Iterator[IncomingMessageEnvelope]): Unit = {

    for (message <- messages.asScala) {

      val key = keySerde.fromBytes(message.getKey.asInstanceOf[Array[Byte]])

      val valueBytes = message.getMessage.asInstanceOf[Array[Byte]]

      nextOffset = Longs.fromByteArray(valueBytes.slice(0, 8))
      downstreamOffset = Longs.fromByteArray(valueBytes.slice(8, 16))

      underlying.put(key, valueSerde.fromBytes(valueBytes.drop(16)))
    }

    info(s"Restored offsets for $ssp: [upstream: $nextOffset, downstream: $downstreamOffset]")
  }

  def handle(offset: Long, key: A, default: B)(block: (Long, B) => (Long, B)): Long = {

    if (offset >= nextOffset) {

      val value = getOrElse(key, default)

      val (newOffset, newValue) = block(downstreamOffset, value)

      downstreamOffset = newOffset

      put(key, newValue)

      nextOffset
    } else {
      offset + 1
    }
  }

  def put(key: A, value: B): Unit = {

    nextOffset += 1

    val valueBytes =
      Longs.toByteArray(nextOffset) ++ Longs.toByteArray(downstreamOffset) ++ valueSerde.toBytes(value)

    collector.send(new OutgoingMessageEnvelope(ssp, partition, keySerde.toBytes(key), valueBytes))

    underlying.put(key, value)
  }

  def getOrElse(key: A, default: B): B = Option(underlying.get(key)).getOrElse(default)

  override def flush(): Unit = {
    underlying.flush()
  }

  override def stop(): Unit = {
    underlying.close()
  }
}

class CoastStoreFactory[A, B] extends StorageEngineFactory[A, B] {

  override def getStorageEngine(
    storeName: String,
    storeDir: File,
    keySerde: Serde[A],
    msgSerde: Serde[B],
    collector: MessageCollector,
    registry: MetricsRegistry,
    changeLogSystemStreamPartition: SystemStreamPartition,
    containerContext: SamzaContainerContext
  ): StorageEngine = {

    val backingFactory = containerContext.config
      .getNewInstance[BaseKeyValueStorageEngineFactory[_, _]](s"stores.$storeName.subfactory")

    val underlying =
      backingFactory.getKVStore(storeName, storeDir, registry, changeLogSystemStreamPartition, containerContext)

    val serialized = new SerializedKeyValueStore[A, B](underlying, keySerde, msgSerde)

    new CoastStore[A, B](serialized, keySerde, msgSerde, collector, changeLogSystemStreamPartition)
  }
}
