package com.monovore.coast

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

package object wire {

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
