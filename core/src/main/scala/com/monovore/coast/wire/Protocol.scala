package com.monovore.coast.wire

import java.io.{ByteArrayInputStream, DataInputStream, DataOutputStream, ByteArrayOutputStream}

import com.google.common.base.Charsets

object Protocol {

  object simple {

    implicit val utf8: Serializer[String] = new Serializer[String] {
      override def toArray(value: String): Array[Byte] = value.getBytes(Charsets.UTF_8)
      override def fromArray(bytes: Array[Byte]): String = new String(bytes, Charsets.UTF_8)
    }

    implicit val longFormat: Serializer[Long] = new Serializer[Long] {
      override def toArray(value: Long): Array[Byte] = value.toString.getBytes(Charsets.US_ASCII)
      override def fromArray(bytes: Array[Byte]): Long = new String(bytes, Charsets.US_ASCII).toLong
    }

    implicit val intFormat: Serializer[Int] = new Serializer[Int] {
      override def toArray(value: Int): Array[Byte] = value.toString.getBytes(Charsets.US_ASCII)
      override def fromArray(bytes: Array[Byte]): Int = new String(bytes, Charsets.US_ASCII).toInt
    }

    implicit val unit: Serializer[Unit] = new Serializer[Unit] {
      private[this] val empty: Array[Byte] = Array.ofDim(0)
      override def toArray(value: Unit): Array[Byte] = empty
      override def fromArray(bytes: Array[Byte]): Unit = {
        require(bytes.length == 0, "Expecting empty byte array.")
      }
    }

    implicit val intPartitioner: Partitioner[Int] = Partitioner.byHashCode

    implicit val stringPartitioner: Partitioner[String] = Partitioner.byHashCode

    implicit val longPartitioner: Partitioner[Long] = Partitioner.byHashCode

    implicit object unitPartitioner extends Partitioner[Unit] {
      override def partition(a: Unit, numPartitions: Int): Int = 0
    }
  }

  object common {

    implicit def streamFormatSerializer[A](implicit format: StreamFormat[A]): Serializer[A] = new Serializer[A] {
      override def toArray(value: A): Array[Byte] = {
        val baos = new ByteArrayOutputStream()
        format.write(new DataOutputStream(baos), value)
        baos.toByteArray
      }
      override def fromArray(bytes: Array[Byte]): A = {
        format.read(new DataInputStream(new ByteArrayInputStream(bytes)))
      }
    }

    implicit val intPartitioner: Partitioner[Int] = Partitioner.byHashCode

    implicit val stringPartitioner: Partitioner[String] = Partitioner.byHashCode

    implicit val longPartitioner: Partitioner[Long] = Partitioner.byHashCode

    implicit object unitPartitioner extends Partitioner[Unit] {
      override def partition(a: Unit, numPartitions: Int): Int = 0
    }
  }

  object native {

    implicit def anyPartitioner[A]: Partitioner[A] = Partitioner.byHashCode

    implicit def anyFormat[A]: Serializer[A] = Serializer.fromJavaSerialization[A]
  }
}