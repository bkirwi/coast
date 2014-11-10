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
  underlying: KeyValueStore[A, (Long, B)],
  keySerde: Serde[A],
  valueSerde: Serde[B],
  collector: MessageCollector,
  ssp: SystemStreamPartition
) extends StorageEngine with Logging {

  val partition: Int = ssp.getPartition.getPartitionId

  var upstreamOffset: Long = 0L

  var nextOffset: Long = 0L

  override def restore(messages: util.Iterator[IncomingMessageEnvelope]): Unit = {

    for (message <- messages.asScala) {

      val offset = message.getOffset.toLong

      val key = keySerde.fromBytes(message.getKey.asInstanceOf[Array[Byte]])
      val value = valueSerde.fromBytes(message.getMessage.asInstanceOf[Array[Byte]])

      underlying.put(key, offset -> value)

      nextOffset = offset + 1
    }
  }
  
  def put(key: A, value: B): Unit = {

    collector.send(new OutgoingMessageEnvelope(ssp, partition, keySerde.toBytes(key), valueSerde.toBytes(value)))

    nextOffset += 1

    underlying.put(key, nextOffset -> value)
  }

  def getOrElse(key: A, default: B): (Long, B) = Option(underlying.get(key)).getOrElse(0L -> default)

  override def flush(): Unit = {

    info(s"Flushing system stream ${ssp.getSystemStream}")
    collector.flush(ssp)

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

    val serialized = new SerializedKeyValueStore[A, (Long, B)](underlying, keySerde, new Serde[(Long, B)] {

      override def toBytes(pair: (Long, B)): Array[Byte] = {
        Longs.toByteArray(pair._1) ++ msgSerde.toBytes(pair._2)
      }

      override def fromBytes(bytes: Array[Byte]): (Long, B) = {
        Longs.fromByteArray(bytes.take(8)) -> msgSerde.fromBytes(bytes.drop(8))
      }
    })

    new CoastStore[A, B](serialized, keySerde, msgSerde, collector, changeLogSystemStreamPartition)
  }
}
