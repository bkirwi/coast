package com.monovore.coast.wire

import java.io._

import scala.language.implicitConversions

/**
 * Manages reading and writing data to Java's standard Data{Input,Output} classes.
 */
trait Serializer[A] extends Serializable {

  def toArray(value: A): Array[Byte]

  def fromArray(bytes: Array[Byte]): A
}

object Serializer {

  def fromArray[A](input: Array[Byte])(implicit reader: Serializer[A]): A = reader.fromArray(input)

  def toArray[A](value: A)(implicit writer: Serializer[A]): Array[Byte] = writer.toArray(value)

  def fromJavaSerialization[A] = new Serializer[A] {
    override def toArray(value: A): Array[Byte] = {
      val baos = new ByteArrayOutputStream()
      val oos = new ObjectOutputStream(baos)
      oos.writeObject(value)
      oos.close()
      baos.toByteArray
    }
    override def fromArray(bytes: Array[Byte]): A = {
      val bais = new ByteArrayInputStream(bytes)
      val ois = new ObjectInputStream(bais)
      ois.readObject().asInstanceOf[A]
    }
  }
}
