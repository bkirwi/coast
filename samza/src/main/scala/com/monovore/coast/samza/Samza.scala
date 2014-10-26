package com.monovore.coast
package samza

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

  private[this] def storageFor[A, B](element: Element[A, B], path: List[String]): Set[String] = element match {
    case Source(_) => Set.empty
    case PureTransform(up, _) => storageFor(up, path)
    case Merge(ups) => {
      ups.zipWithIndex
        .map { case (up, i) => storageFor(up, s"merge-$i" :: path)}
        .flatten.toSet
    }
    case Aggregate(up, _, _) => {
      val upstreamed = storageFor(up, "aggregated" :: path)
      upstreamed + Samza.formatPath(path)
    }
    case GroupBy(up, _) => storageFor(up, path)
  }

  def configFor(flow: Flow[_])(
    system: String = "kafka",
    baseConfig: Config = new MapConfig()
  ): Map[String, Config] = {

    val baseConfigMap = baseConfig.asScala.toMap

    val configs = flow.bindings.map { case (name -> element) =>

      val inputs = sourcesFor(element)

      val factory: MessageSink.Factory = new MessageSink.FromElement(element)

      val storage = storageFor(element, List(name))

      val configMap = Map(

        // Job
        "job.name" -> name,

        // Task
        "task.class" -> "com.monovore.coast.samza.CoastTask",
        "task.inputs" -> inputs.map { i => s"$system.$i"}.mkString(","),

        "serializers.registry.string.class" -> "org.apache.samza.serializers.StringSerdeFactory",

        // Coast-specific
        TaskKey -> SerializationUtil.toBase64(factory),
        TaskName -> name
      )

      val storageMap = storage
        .map { name =>

          Map(
            s"stores.$name.factory" -> "org.apache.samza.storage.kv.KeyValueStorageEngineFactory",
            s"stores.$name.key.serde" -> "string",
            s"stores.$name.msg.serde" -> "string"
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
