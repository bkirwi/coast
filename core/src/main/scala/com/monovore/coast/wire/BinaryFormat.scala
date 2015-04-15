package com.monovore.coast.wire

import java.io.{ObjectInputStream, ByteArrayInputStream, ObjectOutputStream, ByteArrayOutputStream}

import scala.annotation.implicitNotFound


@implicitNotFound("No binary format in scope for type ${A}")
trait BinaryFormat[A] extends Serializable {
  def write(value: A): Array[Byte]
  def read(bytes: Array[Byte]): A
}

object BinaryFormat {

  def write[A](value: A)(implicit fmt: BinaryFormat[A]): Array[Byte] = fmt.write(value)

  def read[A](bytes: Array[Byte])(implicit fmt: BinaryFormat[A]): A = fmt.read(bytes)

  def javaSerialization[A] = new BinaryFormat[A] {

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
