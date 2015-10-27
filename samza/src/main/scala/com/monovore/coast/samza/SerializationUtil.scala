package com.monovore.coast
package samza

import com.google.common.io.BaseEncoding
import com.monovore.coast.wire.Serializer

object SerializationUtil {

  val Encoding = BaseEncoding.base64Url.omitPadding

  def toBase64[A](any: A): String = {
    val bytes = Serializer.fromJavaSerialization[A].toArray(any)
    Encoding.encode(bytes)
  }

  def fromBase64[A](string: String): A = {
    val bytes = Encoding.decode(string)
    Serializer.fromJavaSerialization[A].fromArray(bytes)
  }
}
