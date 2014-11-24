package com.monovore.coast

import com.google.common.primitives.{Ints, Longs}
import com.monovore.coast.wire.BinaryFormat
import com.monovore.coast.model._
import org.apache.samza.config.{Config, MapConfig}

import scala.collection.JavaConverters._

package object samza {

  val TaskKey = "coast.task.serialized.base64"
  val TaskName = "coast.task.name"
  val RegroupedStreams = "coast.streams.regrouped"

  val CoastSystem = "coast-system"

  def formatPath(path: List[String]): String = {
    if (path.isEmpty) "."
    else path.reverse.mkString(".")
  }

  def isRegrouped[A, B](node: Node[A, B]): Boolean = node match {
    case _: Source[_, _] => false
    case trans: Transform[_, _, _, _] => isRegrouped(trans.upstream)
    case merge: Merge[_, _] => merge.upstreams.exists { case (_, up) => isRegrouped(up) }
    case group: GroupBy[_, _, _] => true
  }

  private[this] def sourcesFor[A, B](element: Node[A, B]): Set[String] = element match {
    case Source(name) => Set(name)
    case Transform(up, _, _) => sourcesFor(up)
    case Merge(ups) => ups.flatMap { case (_, up) => sourcesFor(up) }.toSet
    case GroupBy(up, _) => sourcesFor(up)
  }

  val longPairFormat = new BinaryFormat[(Long, Long)] {
    override def write(value: (Long, Long)): Array[Byte] = {
      Longs.toByteArray(value._1) ++ Longs.toByteArray(value._2)
    }
    override def read(bytes: Array[Byte]): (Long, Long) = {
      Longs.fromByteArray(bytes.take(8)) -> Longs.fromByteArray(bytes.drop(8))
    }
  }

  case class Storage(name: String, keyString: String, valueString: String)

  private[this] def storageFor[A, B](element: Node[A, B], path: List[String]): Seq[Storage] = element match {
    case Source(_) => Seq(Storage(
      name = formatPath(path),
      keyString = SerializationUtil.toBase64(wire.pretty.UnitFormat),
      valueString = SerializationUtil.toBase64(wire.pretty.UnitFormat)
    ))
    case PureTransform(up, _) => storageFor(up, path)
    case Merge(ups) => {
      ups.flatMap { case (branch, up) => storageFor(up, branch :: path)}
    }
    case agg @ Aggregate(up, _, _) => {
      val upstreamed = storageFor(up, "aggregated" :: path)
      upstreamed :+ Storage(
        name = formatPath(path),
        keyString = SerializationUtil.toBase64(agg.keyFormat),
        valueString = SerializationUtil.toBase64(agg.stateFormat)
      )
    }
    case GroupBy(up, _) => storageFor(up, path)
  }

  def configureFlow(flow: Flow[_])(
    baseConfig: Config = new MapConfig()
  ): Map[String, Config] = {

    val baseConfigMap = baseConfig.asScala.toMap

    val regrouped = flow.bindings
      .flatMap { case (name, sink) =>
        Some(name).filter { _ => isRegrouped(sink.element) }
      }

    val configs = flow.bindings.map { case (name -> sink) =>

      val inputs = (sourcesFor(sink.element) + s"coast.merge.$name")
        .map { i => s"$CoastSystem.$i" }

      val storage = storageFor(sink.element, List(name))

      val streamDelays = storage
        .map { case Storage(s, _, _) => s -> (s.count { _ == '.'} + 1) }

      val factory: MessageSink.Factory = new MessageSink.FromElement(sink)

      val configMap = Map(

        // Job
        "job.name" -> name,

        // Task
        "task.class" -> "com.monovore.coast.samza.CoastTask",
        "task.inputs" -> inputs.mkString(","),

        "serializers.registry.string.class" -> "org.apache.samza.serializers.StringSerdeFactory",
        "serializers.registry.bytes.class" -> "org.apache.samza.serializers.ByteSerdeFactory",

        // TODO: checkpoints should be configurable
        "task.checkpoint.factory" -> "org.apache.samza.checkpoint.kafka.KafkaCheckpointManagerFactory",
        "task.checkpoint.system" -> "coast-system",

        // Kafka system
        s"systems.$CoastSystem.samza.offset.default" -> "oldest",
        s"systems.$CoastSystem.producer.producer.type" -> "sync",
        s"systems.$CoastSystem.producer.message.send.max.retries" -> "0",
        s"systems.$CoastSystem.producer.request.required.acks" -> "1",
        s"systems.$CoastSystem.samza.offset.default" -> "oldest",
        s"systems.$CoastSystem.samza.factory" -> "com.monovore.coast.samza.CoastKafkaSystemFactory",
        s"systems.$CoastSystem.delays" -> streamDelays.map { case (s, i) => s"coast.changelog.$s/$i" }.mkString(","),

        // Coast-specific
        TaskKey -> SerializationUtil.toBase64(factory),
        TaskName -> name,
        RegroupedStreams -> regrouped.mkString(",")
      )

//      val offsetStorage = Storage(
//        s"offsets",
//        SerializationUtil.toBase64(format.pretty.UnitFormat),
//        SerializationUtil.toBase64(format.pretty.UnitFormat)
//      )

      val storageMap = storage
        .map { case Storage(name, keyFormat, msgFormat) =>

          val keyName = s"coast-key-$name"
          val msgName = s"coast-msg-$name"

          Map(
            s"stores.$name.factory" -> "com.monovore.coast.samza.CoastStoreFactory",
            s"stores.$name.subfactory" -> "org.apache.samza.storage.kv.inmemory.InMemoryKeyValueStorageEngineFactory",
            s"stores.$name.key.serde" -> keyName,
            s"stores.$name.msg.serde" -> msgName,
            s"stores.$name.changelog" -> s"$CoastSystem.coast.changelog.$name",
            s"serializers.registry.$keyName.class" -> "com.monovore.coast.samza.CoastSerdeFactory",
            s"serializers.registry.$keyName.serialized.base64" -> keyFormat,
            s"serializers.registry.$msgName.class" -> "com.monovore.coast.samza.CoastSerdeFactory",
            s"serializers.registry.$msgName.serialized.base64" -> msgFormat
          )
        }
        .flatten.toMap

      name -> new MapConfig(
        (baseConfigMap ++ configMap ++ storageMap).asJava
      )
    }

    configs.toMap
  }

  def config(pairs: (String -> String)*): Config = new MapConfig(pairs.toMap.asJava)
}
