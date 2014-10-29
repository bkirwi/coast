package com.monovore.coast
package samza

import com.google.common.io.BaseEncoding

object SerializationUtil {

  val Encoding = BaseEncoding.base64Url.omitPadding

  def toBase64[A](any: A): String = {
    val bytes = format.javaSerialization.forAnything[A].write(any)
    Encoding.encode(bytes)
  }

  def fromBase64[A](string: String): A = {
    val bytes = Encoding.decode(string)
    format.javaSerialization.forAnything[A].read(bytes)
  }
}
