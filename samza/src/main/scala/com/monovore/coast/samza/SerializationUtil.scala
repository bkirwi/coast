package com.monovore.coast
package samza

import com.google.common.io.BaseEncoding
import com.monovore.coast.wire.BinaryFormat

object SerializationUtil {

  val Encoding = BaseEncoding.base64Url.omitPadding

  def toBase64[A](any: A): String = {
    val bytes = BinaryFormat.javaSerialization[A].write(any)
    Encoding.encode(bytes)
  }

  def fromBase64[A](string: String): A = {
    val bytes = Encoding.decode(string)
    BinaryFormat.javaSerialization[A].read(bytes)
  }
}
