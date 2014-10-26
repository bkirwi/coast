package com.monovore.coast.samza

import java.io.{ObjectInputStream, ByteArrayInputStream, ObjectOutputStream, ByteArrayOutputStream}

import com.google.common.io.BaseEncoding

object SerializationUtil {

  val Encoding = BaseEncoding.base64Url.omitPadding

  def toBase64[A](any: A): String = {
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(any)
    oos.close()
    Encoding.encode(baos.toByteArray)
  }

  def fromBase64[A](string: String): A = {
    val bytes = Encoding.decode(string)
    val bais = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bais)
    val a = ois.readObject().asInstanceOf[A]
    ois.close()
    a
  }
}
