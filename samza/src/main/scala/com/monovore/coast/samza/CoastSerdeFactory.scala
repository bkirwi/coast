package com.monovore.coast.samza

import com.monovore.coast.WireFormat
import org.apache.samza.config.Config
import org.apache.samza.config.serializers.JsonConfigSerializer
import org.apache.samza.serializers.{Serde, SerdeFactory}

class CoastSerdeFactory[A] extends SerdeFactory[A] {

  override def getSerde(name: String, config: Config): Serde[A] = {

    println(JsonConfigSerializer.toJson(config))

    val format = SerializationUtil.fromBase64[WireFormat[A]](
      config.get(ConfigSerdeFactory.keyForSerde(name, "serialized", "base64"))
    )

    new Serde[A] {

      override def fromBytes(bytes: Array[Byte]): A = format.read(bytes)

      override def toBytes(value: A): Array[Byte] = format.write(value)
    }
  }
}

object ConfigSerdeFactory {
  def keyForSerde(keys: String*): String = s"serializers.registry.${keys.mkString(".")}"
}