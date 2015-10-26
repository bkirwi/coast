package com.monovore.coast.wire

import java.io._

import scala.language.implicitConversions

/**
 * Manages reading and writing data to Java's standard Data{Input,Output} classes.
 */
trait BinaryFormat[A] extends Serializable {

  def writeData(output: DataOutputStream, value: A): Unit

  def readData(input: DataInputStream): A

  def write(value: A): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(baos)
    writeData(dos, value)
    baos.close()
    baos.toByteArray
  }

  def read(bytes: Array[Byte]): A = {
    val bais = new ByteArrayInputStream(bytes)
    val dis = new DataInputStream(bais)
    val value = readData(dis)
    dis.close()
    value
  }
}

object BinaryFormat {

  def readData[A](input: DataInputStream)(implicit reader: BinaryFormat[A]): A = reader.readData(input)

  def writeData[A](output: DataOutputStream, value: A)(implicit writer: BinaryFormat[A]) = writer.writeData(output, value)

  def read[A](input: Array[Byte])(implicit reader: BinaryFormat[A]): A = reader.read(input)

  def write[A](value: A)(implicit writer: BinaryFormat[A]): Array[Byte] = writer.write(value)

  def javaSerialization[A] = new BinaryFormat[A] {

    override def writeData(output: DataOutputStream, value: A) {
      val oos = new ObjectOutputStream(output)
      oos.writeObject(value)
    }

    override def readData(input: DataInputStream): A = {
      val ois = new ObjectInputStream(input)
      ois.readObject().asInstanceOf[A]
    }
  }

  object defaults extends DefaultBinaryFormats
}
