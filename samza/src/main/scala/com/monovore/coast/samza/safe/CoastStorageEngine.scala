package com.monovore.coast.samza.safe

import java.io.File
import java.util

import com.monovore.coast.wire.{Protocol, Serializer}
import org.apache.samza.container.SamzaContainerContext
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.serializers.Serde
import org.apache.samza.storage.kv.{BaseKeyValueStorageEngineFactory, KeyValueStore, SerializedKeyValueStore}
import org.apache.samza.storage.{StorageEngine, StorageEngineFactory}
import org.apache.samza.system.{IncomingMessageEnvelope, OutgoingMessageEnvelope, SystemStreamPartition}
import org.apache.samza.task.MessageCollector
import org.apache.samza.util.Logging

import scala.collection.JavaConverters._

trait CoastState[K, V] {
  
  def upstream: Long
  
  def downstream: Long
  
  def state(key: K): V
  
  def push(key: K, value: V, upstream: Long, downstream: Long): Unit
  
  def update(key: K, upstreamOffset: Long)(block: (Long, V) => (Long, V)): Long = {

    val minOffset = upstream

    val nextUpstream = upstreamOffset + 1

    if (upstreamOffset >= minOffset) {

      val currentState = state(key)

      val (nextDownstream, nextState) = block(downstream, currentState)
      
      push(key, nextState, nextUpstream, nextDownstream)
    }
    
    nextUpstream
  }
}

class CoastStorageEngine[K, V](
  underlying: KeyValueStore[K, V],
  keySerde: Serde[K],
  valueSerde: Serde[V],
  collector: MessageCollector,
  ssp: SystemStreamPartition,
  keyFormat: Serializer[Array[Byte]],
  valueFormat: Serializer[(Long, Long, Array[Byte])]
) extends StorageEngine with Logging { store =>

  val partitionID: Int = ssp.getPartition.getPartitionId

  var nextOffset: Long = 0L

  var downstreamOffset: Long = 0L

  override def restore(messages: util.Iterator[IncomingMessageEnvelope]): Unit = {

    for (message <- messages.asScala) {

      val keyBytes = keyFormat.fromArray(message.getKey.asInstanceOf[Array[Byte]])

      val (up, down, valueBytes) = valueFormat.fromArray(message.getMessage.asInstanceOf[Array[Byte]])

      nextOffset = up

      downstreamOffset = down

      underlying.put(keySerde.fromBytes(keyBytes), valueSerde.fromBytes(valueBytes))
    }

    info(s"Restored offsets for $ssp: [upstream: $nextOffset, downstream: $downstreamOffset]")
  }

  def withDefault(default: V): CoastState[K, V] = new CoastState[K, V] {

    override def upstream: Long = store.nextOffset

    override def downstream: Long = store.downstreamOffset

    override def state(key: K): V = Option(store.underlying.get(key)).getOrElse(default)

    override def push(key: K, value: V, upstream: Long, downstream: Long): Unit = {

      store.nextOffset = upstream
      store.downstreamOffset = downstream
      store.underlying.put(key, value)

      val keyBytes = keyFormat.toArray(keySerde.toBytes(key))
      val valueBytes = valueFormat.toArray(upstream, downstream, valueSerde.toBytes(value))
      collector.send(new OutgoingMessageEnvelope(ssp, store.partitionID, keyBytes, valueBytes))
    }
  }

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

    import Protocol.common._

    val keyFormat = implicitly[Serializer[Array[Byte]]]
    val valueFormat = implicitly[Serializer[(Long, Long, Array[Byte])]]

    new CoastStorageEngine[A, B](serialized, keySerde, msgSerde, collector, changeLogSystemStreamPartition, keyFormat, valueFormat)
  }
}
