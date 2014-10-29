package com.monovore.coast

import java.io.{ObjectInputStream, ByteArrayInputStream, ObjectOutputStream, ByteArrayOutputStream}

import com.google.common.base.Charsets

package object format {

  object javaSerialization {

    implicit def forAnything[A] = new WireFormat[A] {

      override def write(value: A): Array[Byte] = {
        val baos = new ByteArrayOutputStream()
        val oos = new ObjectOutputStream(baos)
        oos.writeObject(value)
        oos.close()
        baos.toByteArray
      }

      override def read(bytes: Array[Byte]): A = {
        val bais = new ByteArrayInputStream(bytes)
        val ois = new ObjectInputStream(bais)
        val value = ois.readObject().asInstanceOf[A]
        ois.close()
        value
      }
    }
  }

  object pretty {

    implicit object IntFormat extends WireFormat[Int] {
      override def write(value: Int): Array[Byte] = value.toString.getBytes(Charsets.UTF_8)
      override def read(bytes: Array[Byte]): Int = new String(bytes, Charsets.UTF_8).toInt
    }

    implicit object StringFormat extends WireFormat[String] {
      override def write(value: String): Array[Byte] = value.getBytes(Charsets.UTF_8)
      override def read(bytes: Array[Byte]): String = new String(bytes, Charsets.UTF_8)
    }
  }
}
