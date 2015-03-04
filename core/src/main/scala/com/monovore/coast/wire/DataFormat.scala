package com.monovore.coast.wire

import java.io._

/**
 * Manages reading and writing data to Java's standard Data{Input,Output} classes.
 */
trait DataFormat[A] extends Serializable {

  def writeData(output: DataOutput, value: A): Unit

  def readData(input: DataInput): A
}

class DataBinaryFormat[A](dataFormat: DataFormat[A]) extends BinaryFormat[A] {

  override final def write(value: A): Array[Byte] = {

    val baos = new ByteArrayOutputStream()
    val output = new DataOutputStream(baos)

    dataFormat.writeData(output, value)

    output.flush()

    baos.toByteArray
  }

  override final def read(bytes: Array[Byte]): A = {

    val bais = new ByteArrayInputStream(bytes)
    val input = new DataInputStream(bais)

    val value = dataFormat.readData(input)

    value
  }
}

object DataFormat {

  def wireFormat[A](implicit dataFormat: DataFormat[A]): BinaryFormat[A] = new DataBinaryFormat[A](dataFormat)

  def readData[A](input: DataInput)(implicit reader: DataFormat[A]): A = reader.readData(input)

  def writeData[A](output: DataOutput, value: A)(implicit writer: DataFormat[A]) = writer.writeData(output, value)

  implicit object LongFormat extends DataFormat[Long] {
    override def writeData(output: DataOutput, value: Long): Unit = output.writeLong(value)
    override def readData(input: DataInput): Long = input.readLong()
  }

  implicit object IntFormat extends DataFormat[Int] {
    override def writeData(output: DataOutput, value: Int): Unit = output.writeInt(value)
    override def readData(input: DataInput): Int = input.readInt()
  }

  implicit object StringFormat extends DataFormat[String] {
    override def writeData(output: DataOutput, value: String): Unit = output.writeUTF(value)
    override def readData(input: DataInput): String = input.readUTF()
  }

  implicit object UnitFormat extends DataFormat[Unit] {
    override def writeData(output: DataOutput, value: Unit): Unit = {}
    override def readData(input: DataInput): Unit = {}
  }

  implicit object BytesFormat extends DataFormat[Array[Byte]] {

    override def writeData(output: DataOutput, value: Array[Byte]): Unit = {
      output.writeInt(value.length)
      output.write(value)
    }

    override def readData(input: DataInput): Array[Byte] = {
      val size = input.readInt()
      val bytes = Array.ofDim[Byte](size)
      input.readFully(bytes)
      bytes
    }
  }

  implicit def Tuple2Format[A : DataFormat, B : DataFormat] = new DataFormat[(A, B)] {

    override def writeData(output: DataOutput, value: (A, B)): Unit = {
      DataFormat.writeData(output, value._1)
      DataFormat.writeData(output, value._2)
    }

    override def readData(input: DataInput): (A, B) = {
      val a = DataFormat.readData[A](input)
      val b = DataFormat.readData[B](input)
      (a, b)
    }
  }

  implicit def Tuple3Format[A : DataFormat, B : DataFormat, C : DataFormat] = new DataFormat[(A, B, C)] {

    override def writeData(output: DataOutput, value: (A, B, C)): Unit = {
      DataFormat.writeData(output, value._1)
      DataFormat.writeData(output, value._2)
      DataFormat.writeData(output, value._3)
    }

    override def readData(input: DataInput): (A, B, C) = {
      val a = DataFormat.readData[A](input)
      val b = DataFormat.readData[B](input)
      val c = DataFormat.readData[C](input)
      (a, b, c)
    }
  }
}
