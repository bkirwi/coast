package com.monovore.coast.samza

import com.monovore.coast.samza.ConfigGenerator.Storage
import org.apache.samza.config.{Config, MapConfig}

import collection.JavaConverters._

object SamzaConfig {

  def from(pairs: (String, String)*) = new MapConfig(pairs.toMap.asJava)

  def format(config: Config): String = {

    config.entrySet().asScala.toSeq
      .sortBy { _.getKey }
      .map { pair =>  s"  ${pair.getKey} -> [${pair.getValue}]"  }
      .mkString("\n")
  }

  case class Base(base: Config) {

    val system = base.get("coast.system.name", "coast-system")

    val mergePrefix = base.get("coast.prefix.merge", "coast.mg")

    val changelogPrefix = base.get("coast.prefix.changelog", "coast.cl")

    val checkpointPrefix = base.get("coast.prefix.checkpoint", "coast.cp")

    def changelogStream(storage: String) = {
      s"$changelogPrefix.$storage"
    }

    def storageConfig(storage: Storage) = {

      import storage._

      val defaults = base.subset("coast.default.stores.").asScala.toMap
        .map { case (key, value) => s"stores.$name.$key" -> value }

      val keyName = s"$name.key"
      val msgName = s"$name.msg"

      defaults ++ Map(
        s"stores.$name.key.serde" -> keyName,
        s"stores.$name.msg.serde" -> msgName,
        s"serializers.registry.$keyName.class" -> "com.monovore.coast.samza.CoastSerdeFactory",
        s"serializers.registry.$keyName.serialized.base64" -> SerializationUtil.toBase64(keyString),
        s"serializers.registry.$msgName.class" -> "com.monovore.coast.samza.CoastSerdeFactory",
        s"serializers.registry.$msgName.serialized.base64" -> SerializationUtil.toBase64(valueString)
      )
    }
  }
}
