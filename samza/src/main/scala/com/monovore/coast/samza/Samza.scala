package com.monovore.coast
package samza

import com.google.common.hash.Hashing
import org.apache.samza.config.{Config, MapConfig}

import scala.collection.JavaConverters._

object Samza {

  val KafkaBrokers = "192.168.80.20:9092"
  val ZookeeperHosts = "192.168.80.20:2181"

  val TaskKey = "coast.task.serialized.base64"
  val TaskName = "coast.task.name"

  def formatPath(path: List[String]): String = {
    if (path.isEmpty) "/"
    else path.reverse.map { "/" + _ }.mkString
  }

  private[this] def sourcesFor[A, B](element: Element[A, B]): Set[String] = element match {
    case Source(name) => Set(name)
    case Transform(up, _, _) => sourcesFor(up)
    case Merge(ups) => ups.flatMap(sourcesFor).toSet
    case GroupBy(up, _) => sourcesFor(up)
  }

  case class Storage(name: String, keyString: String, valueString: String)

  private[this] def storageFor[A, B](element: Element[A, B], path: List[String]): Seq[Storage] = element match {
    case Source(_) => Seq.empty
    case PureTransform(up, _) => storageFor(up, path)
    case Merge(ups) => {
      ups.zipWithIndex
        .flatMap { case (up, i) => storageFor(up, s"merge-$i" :: path)}
    }
    case agg @ Aggregate(up, _, _) => {
      val upstreamed = storageFor(up, "aggregated" :: path)
      upstreamed :+ Storage(
        name = Samza.formatPath(path),
        keyString = SerializationUtil.toBase64(agg.keyFormat),
        valueString = SerializationUtil.toBase64(agg.stateFormat)
      )
    }
    case GroupBy(up, _) => storageFor(up, path)
  }

  def configFor(flow: Flow[_])(
    system: String = "kafka",
    baseConfig: Config = new MapConfig()
  ): Map[String, Config] = {

    val baseConfigMap = baseConfig.asScala.toMap

    val configs = flow.bindings.map { case (name -> sink) =>

      val inputs = sourcesFor(sink.element)

      val storage = storageFor(sink.element, List(name))

      val factory: MessageSink.Factory = new MessageSink.FromElement(sink)

      val configMap = Map(

        // Job
        "job.name" -> name,

        // Task
        "task.class" -> "com.monovore.coast.samza.CoastTask",
        "task.inputs" -> inputs.map { i => s"$system.$i"}.mkString(","),

        "serializers.registry.string.class" -> "org.apache.samza.serializers.StringSerdeFactory",
        "serializers.registry.bytes.class" -> "org.apache.samza.serializers.ByteSerdeFactory",

        // Coast-specific
        TaskKey -> SerializationUtil.toBase64(factory),
        TaskName -> name
      )

      val storageMap = storage
        .map { case Storage(name, keyFormat, msgFormat) =>

          val keyName = s"coast-key-$name"
          val msgName = s"coast-msg-$name"

          Map(
            s"stores.$name.factory" -> "org.apache.samza.storage.kv.KeyValueStorageEngineFactory",
            s"stores.$name.key.serde" -> keyName,
            s"stores.$name.msg.serde" -> msgName,
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
