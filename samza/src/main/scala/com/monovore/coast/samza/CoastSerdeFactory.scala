package com.monovore.coast.samza

import com.monovore.coast.wire.BinaryFormat
import org.apache.samza.config.Config
import org.apache.samza.serializers.{Serde, SerdeFactory}

class CoastSerde[A](format: BinaryFormat[A]) extends Serde[A] {

  override def fromBytes(bytes: Array[Byte]): A = format.read(bytes)

  override def toBytes(value: A): Array[Byte] = format.write(value)
}

class CoastSerdeFactory[A] extends SerdeFactory[A] {

  override def getSerde(name: String, config: Config): Serde[A] = {

    val format = SerializationUtil.fromBase64[BinaryFormat[A]](
      config.get(ConfigSerdeFactory.keyForSerde(name, "serialized", "base64"))
    )

    new CoastSerde[A](format)
  }
}

object ConfigSerdeFactory {
  def keyForSerde(keys: String*): String = s"serializers.registry.${keys.mkString(".")}"
}