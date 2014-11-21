package com.monovore.coast

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util

import com.google.common.base.Charsets.UTF_8

package object format {

  object javaSerialization {

    implicit def formatFor[A] = new WireFormat[A] {

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
      override def write(value: Int): Array[Byte] = value.toString.getBytes(UTF_8)
      override def read(bytes: Array[Byte]): Int = new String(bytes, UTF_8).toInt
    }

    implicit object StringFormat extends WireFormat[String] {
      override def write(value: String): Array[Byte] = value.getBytes(UTF_8)
      override def read(bytes: Array[Byte]): String = new String(bytes, UTF_8)
    }
    
    implicit object LongFormat extends WireFormat[Long] {
      override def write(value: Long): Array[Byte] = value.toString.getBytes(UTF_8)
      override def read(bytes: Array[Byte]): Long = new String(bytes, UTF_8).toLong
    }

    implicit object UnitFormat extends WireFormat[Unit] {
      def unitBytes = "()".getBytes(UTF_8)
      override def write(value: Unit): Array[Byte] = unitBytes
      override def read(bytes: Array[Byte]): Unit = require(util.Arrays.equals(unitBytes, bytes))
    }
  }
}
