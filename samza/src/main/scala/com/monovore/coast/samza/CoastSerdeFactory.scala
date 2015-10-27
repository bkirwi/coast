package com.monovore.coast.samza

import com.monovore.coast.wire.Serializer
import org.apache.samza.config.Config
import org.apache.samza.serializers.{Serde, SerdeFactory}

class CoastSerde[A](format: Serializer[A]) extends Serde[A] {

  override def fromBytes(bytes: Array[Byte]): A = format.fromArray(bytes)

  override def toBytes(value: A): Array[Byte] = format.toArray(value)
}

class CoastSerdeFactory[A] extends SerdeFactory[A] {

  override def getSerde(name: String, config: Config): Serde[A] = {

    val format = SerializationUtil.fromBase64[Serializer[A]](
      config.get(s"serializers.registry.$name.serialized.base64")
    )

    new CoastSerde[A](format)
  }
}