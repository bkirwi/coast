package com.monovore.coast.wire

import java.util

import com.google.common.base.Charsets._
import com.google.common.hash.{Hashing, HashFunction}

object pretty extends GuavaPartitioning {

  override protected def guavaHashFunction: HashFunction = Hashing.murmur3_32()

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
