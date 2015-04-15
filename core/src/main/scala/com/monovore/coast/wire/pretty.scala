package com.monovore.coast.wire

import java.util

import com.google.common.base.Charsets._
import com.google.common.hash.{Hashing, HashFunction}

/**
 * The 'pretty' wire protocol is a cool and fun way to get human-readable formatting
 * for basic types without pulling in some kind of JSON library.
 */
object pretty {

  implicit object IntFormat extends BinaryFormat[Int] {
    override def write(value: Int): Array[Byte] = value.toString.getBytes(UTF_8)
    override def read(bytes: Array[Byte]): Int = new String(bytes, UTF_8).toInt
  }

  implicit val IntPartitioner: Partitioner[Int] = Partitioner.byHashCode

  implicit object StringFormat extends BinaryFormat[String] {
    override def write(value: String): Array[Byte] = value.getBytes(UTF_8)
    override def read(bytes: Array[Byte]): String = new String(bytes, UTF_8)
  }

  implicit val StringPartitioner: Partitioner[String] = Partitioner.byHashCode

  implicit object LongFormat extends BinaryFormat[Long] {
    override def write(value: Long): Array[Byte] = value.toString.getBytes(UTF_8)
    override def read(bytes: Array[Byte]): Long = new String(bytes, UTF_8).toLong
  }

  implicit val LongPartitioner: Partitioner[Long] = Partitioner.byHashCode

  implicit object UnitFormat extends BinaryFormat[Unit] {
    def unitBytes = "()".getBytes(UTF_8)
    override def write(value: Unit): Array[Byte] = unitBytes
    override def read(bytes: Array[Byte]): Unit = require(util.Arrays.equals(unitBytes, bytes))
  }
}
