package com.monovore.coast
package samza

import com.monovore.coast.model._

import org.apache.samza.config.{MapConfig, Config}
import collection.JavaConverters._

trait ConfigGenerator {
  def configure(graph: Graph): Map[String, Config]
}

object ConfigGenerator {

  def isRegrouped[A, B](node: Node[A, B]): Boolean = node match {
    case _: Source[_, _] => false
    case trans: Transform[_, _, _, _] => isRegrouped(trans.upstream)
    case merge: Merge[_, _] => merge.upstreams.exists { case (_, up) => isRegrouped(up) }
    case group: GroupBy[_, _, _] => true
  }

  def sourcesFor[A, B](element: Node[A, B]): Set[String] = element match {
    case Source(name) => Set(name)
    case Transform(up, _, _) => sourcesFor(up)
    case Merge(ups) => ups.flatMap { case (_, up) => sourcesFor(up) }.toSet
    case GroupBy(up, _) => sourcesFor(up)
  }

  case class Storage(name: String, keyString: String, valueString: String)
}

class SafeConfigGenerator(baseConfig: Config = new MapConfig()) extends ConfigGenerator {

  import ConfigGenerator._

  private[this] def storageFor[A, B](element: Node[A, B], path: List[String]): Seq[Storage] = element match {
    case Source(_) => Seq(Storage( // SAFE
      name = formatPath(path),
      keyString = SerializationUtil.toBase64(wire.pretty.UnitFormat),
      valueString = SerializationUtil.toBase64(wire.pretty.UnitFormat)
    ))
    case PureTransform(up, _) => storageFor(up, path)
    case Merge(ups) => {
      ups.flatMap { case (branch, up) => storageFor(up, branch :: path)}
    }
    case agg @ StatefulTransform(up, _, _) => {
      val upstreamed = storageFor(up, "aggregated" :: path)
      upstreamed :+ Storage(
        name = formatPath(path),
        keyString = SerializationUtil.toBase64(agg.keyFormat),
        valueString = SerializationUtil.toBase64(agg.stateFormat)
      )
    }
    case GroupBy(up, _) => storageFor(up, path)
  }

  def configure(graph: Graph): Map[String, Config] = {

    val baseConfigMap = baseConfig.asScala.toMap

    val regrouped = graph.bindings
      .flatMap { case (name, sink) =>
        Some(name).filter { _ => isRegrouped(sink.element) }
      }

    val configs = graph.bindings.map { case (name -> sink) =>

      // SAFE
      val inputs = (sourcesFor(sink.element) + s"coast.merge.$name")
        .map { i => s"$CoastSystem.$i" }

      val storage = storageFor(sink.element, List(name))

      // SAFE ???
      val streamDelays = storage
        .map { case Storage(s, _, _) => s -> (s.count { _ == '.'} + 1) }

      val factory: MessageSink.Factory = new Safe.SinkFactory(sink)

      val configMap = Map(

        // Job
        "job.name" -> name,

        // Task
        "task.class" -> "com.monovore.coast.samza.CoastTask",
        "task.inputs" -> inputs.mkString(","),

        // TODO: checkpoints should be configurable
        "task.checkpoint.factory" -> "org.apache.samza.checkpoint.kafka.KafkaCheckpointManagerFactory",
        "task.checkpoint.system" -> "coast-system",

        // Kafka system
        s"systems.$CoastSystem.samza.offset.default" -> "oldest",
        s"systems.$CoastSystem.producer.producer.type" -> "sync",           // SAFE
        s"systems.$CoastSystem.producer.message.send.max.retries" -> "0",
        s"systems.$CoastSystem.producer.request.required.acks" -> "1",
        s"systems.$CoastSystem.samza.factory" -> "com.monovore.coast.samza.CoastKafkaSystemFactory",
        s"systems.$CoastSystem.delays" -> streamDelays.map { case (s, i) => s"coast.changelog.$s/$i" }.mkString(","),

        // Coast-specific
        TaskKey -> SerializationUtil.toBase64(factory),
        TaskName -> name,
        RegroupedStreams -> regrouped.mkString(",")   // SAFE ???
      )

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
            s"stores.$name.coast.simple" -> "false",
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
}
