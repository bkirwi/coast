package com.monovore.coast.standalone

import java.io.{DataOutputStream, DataInputStream}

import com.monovore.coast.wire.Serializer

object Framed {

  case class Key[A](source: Seq[Byte], key: A)

  object Key {
    implicit def keyFormat[A:Serializer] = new Serializer[Key[A]] {
      override def write(output: DataOutputStream, value: Key[A]): Unit = {
        Serializer.write(output, value.source)
        Serializer.write(output, value.key)
      }
      override def read(input: DataInputStream): Key[A] = {
        val source = Serializer.read[Seq[Byte]](input)
        val key = Serializer.read[A](input)
        Key(source, key)
      }
    }
  }

  case class Message[A](offset: Long, value: A)

  object Message {
    implicit def messageFormat[A:Serializer] = new Serializer[Message[A]] {
      override def write(output: DataOutputStream, value: Message[A]): Unit = {
        Serializer.write(output, value.offset)
        Serializer.write(output, value.value)
      }
      override def read(input: DataInputStream): Message[A] = {
        val offset = Serializer.read[Long](input)
        val value = Serializer.read[A](input)
        Message(offset, value)
      }
    }
  }
}
