package com.monovore.coast.wire

import java.io._

import scala.language.implicitConversions

/**
 * Manages reading and writing data to Java's standard Data{Input,Output} classes.
 */
trait Serializer[A] extends Serializable {

  def write(output: DataOutputStream, value: A): Unit

  def read(input: DataInputStream): A

  def toArray(value: A): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(baos)
    write(dos, value)
    baos.close()
    baos.toByteArray
  }

  def fromArray(bytes: Array[Byte]): A = {
    val bais = new ByteArrayInputStream(bytes)
    val dis = new DataInputStream(bais)
    val value = read(dis)
    dis.close()
    value
  }
}

object Serializer {

  def read[A](input: DataInputStream)(implicit reader: Serializer[A]): A = reader.read(input)

  def write[A](output: DataOutputStream, value: A)(implicit writer: Serializer[A]) = writer.write(output, value)

  def fromArray[A](input: Array[Byte])(implicit reader: Serializer[A]): A = reader.fromArray(input)

  def toArray[A](value: A)(implicit writer: Serializer[A]): Array[Byte] = writer.toArray(value)

  def fromJavaSerialization[A] = new Serializer[A] {

    override def write(output: DataOutputStream, value: A) {
      val oos = new ObjectOutputStream(output)
      oos.writeObject(value)
    }

    override def read(input: DataInputStream): A = {
      val ois = new ObjectInputStream(input)
      ois.readObject().asInstanceOf[A]
    }
  }
}
