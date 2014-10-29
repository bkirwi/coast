package com.monovore.coast.samza

import com.google.common.io.BaseEncoding
import com.monovore.coast.WireFormats

object SerializationUtil {

  val Encoding = BaseEncoding.base64Url.omitPadding

  def toBase64[A](any: A): String = {
    val bytes = WireFormats.javaSerialization[A].write(any)
    Encoding.encode(bytes)
  }

  def fromBase64[A](string: String): A = {
    val bytes = Encoding.decode(string)
    WireFormats.javaSerialization[A].read(bytes)
  }
}
