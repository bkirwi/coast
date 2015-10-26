package com.monovore.coast.wire

import java.io.{DataInputStream, DataOutputStream}

/**
 * The 'pretty' wire protocol is a cool and fun way to get human-readable formatting
 * for basic types without pulling in some kind of JSON library.
 */
object pretty extends DefaultBinaryFormats

trait DefaultBinaryFormats {

  implicit object longFormat extends BinaryFormat[Long] {
    override def writeData(output: DataOutputStream, value: Long): Unit = output.writeLong(value)
    override def readData(input: DataInputStream): Long = input.readLong()
  }

  implicit object intFormat extends BinaryFormat[Int] {
    override def writeData(output: DataOutputStream, value: Int): Unit = output.writeInt(value)
    override def readData(input: DataInputStream): Int = input.readInt()
  }

  implicit object stringFormat extends BinaryFormat[String] {
    override def writeData(output: DataOutputStream, value: String): Unit = output.writeUTF(value)
    override def readData(input: DataInputStream): String = input.readUTF()
  }

  implicit object unitFormat extends BinaryFormat[Unit] {
    override def writeData(output: DataOutputStream, value: Unit): Unit = {}
    override def readData(input: DataInputStream): Unit = {}
  }

  implicit val bytesFormat: BinaryFormat[Array[Byte]] = new BinaryFormat[Array[Byte]] {

    override def writeData(output: DataOutputStream, value: Array[Byte]): Unit = {
      output.writeInt(value.length)
      output.write(value)
    }

    override def readData(input: DataInputStream): Array[Byte] = {
      val size = input.readInt()
      val bytes = Array.ofDim[Byte](size)
      input.readFully(bytes)
      bytes
    }
  }

  implicit def optionFormat[A](implicit format: BinaryFormat[A]): BinaryFormat[Option[A]] = new BinaryFormat[Option[A]] {

    override def writeData(output: DataOutputStream, value: Option[A]): Unit = value match{
      case Some(a) => {
        output.writeBoolean(true)
        format.writeData(output, a)
      }
      case None => output.writeBoolean(false)
    }

    override def readData(input: DataInputStream): Option[A] = {
      if (input.readBoolean()) Some(format.readData(input))
      else None
    }
  }

  implicit def tuple2Format[A : BinaryFormat, B : BinaryFormat] = new BinaryFormat[(A, B)] {

    override def writeData(output: DataOutputStream, value: (A, B)): Unit = {
      BinaryFormat.writeData(output, value._1)
      BinaryFormat.writeData(output, value._2)
    }

    override def readData(input: DataInputStream): (A, B) = {
      val a = BinaryFormat.readData[A](input)
      val b = BinaryFormat.readData[B](input)
      (a, b)
    }
  }

  implicit def tuple3Format[A : BinaryFormat, B : BinaryFormat, C : BinaryFormat] = new BinaryFormat[(A, B, C)] {

    override def writeData(output: DataOutputStream, value: (A, B, C)): Unit = {
      BinaryFormat.writeData(output, value._1)
      BinaryFormat.writeData(output, value._2)
      BinaryFormat.writeData(output, value._3)
    }

    override def readData(input: DataInputStream): (A, B, C) = {
      val a = BinaryFormat.readData[A](input)
      val b = BinaryFormat.readData[B](input)
      val c = BinaryFormat.readData[C](input)
      (a, b, c)
    }
  }

  implicit def seqFormat[A : BinaryFormat] = new BinaryFormat[Seq[A]] {

    override def writeData(output: DataOutputStream, value: Seq[A]): Unit = {
      output.writeInt(value.size)
      value.foreach(BinaryFormat.writeData(output, _))
    }

    override def readData(input: DataInputStream): Seq[A] = {
      val size = input.readInt()
      Seq.fill(size)(BinaryFormat.readData[A](input))
    }
  }

  implicit def mapFormat[A : BinaryFormat, B : BinaryFormat] = new BinaryFormat[Map[A, B]] {

    override def writeData(output: DataOutputStream, value: Map[A, B]): Unit = {
      output.writeInt(value.size)
      value.foreach { case (k, v) =>
        BinaryFormat.writeData(output, k)
        BinaryFormat.writeData(output, v)
      }
    }

    override def readData(input: DataInputStream): Map[A, B] = {
      val size = input.readInt()
      Iterator.fill(size)(BinaryFormat.readData[A](input) -> BinaryFormat.readData[B](input))
        .toMap
    }
  }

  implicit val intPartitioner: Partitioner[Int] = Partitioner.byHashCode

  implicit val stringPartitioner: Partitioner[String] = Partitioner.byHashCode

  implicit val longPartitioner: Partitioner[Long] = Partitioner.byHashCode

  implicit object unitPartitioner extends Partitioner[Unit] {
    override def partition(a: Unit, numPartitions: Int): Int = 0
  }
}
