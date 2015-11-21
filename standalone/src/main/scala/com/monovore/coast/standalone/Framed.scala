package com.monovore.coast.standalone

import java.io.{DataOutputStream, DataInputStream}

import com.monovore.coast.wire.StreamFormat

object Framed {

  case class Key[A](source: String, key: A)

  object Key {
    implicit def keyFormat[A:StreamFormat] = new StreamFormat[Key[A]] {
      override def write(output: DataOutputStream, value: Key[A]): Unit = {
        StreamFormat.write(output, value.source)
        StreamFormat.write(output, value.key)
      }
      override def read(input: DataInputStream): Key[A] = {
        val source = StreamFormat.read[String](input)
        val key = StreamFormat.read[A](input)
        Key(source, key)
      }
    }
  }

  case class Message[A](offset: Long, value: A)

  object Message {
    implicit def messageFormat[A:StreamFormat] = new StreamFormat[Message[A]] {
      override def write(output: DataOutputStream, value: Message[A]): Unit = {
        StreamFormat.write(output, value.offset)
        StreamFormat.write(output, value.value)
      }
      override def read(input: DataInputStream): Message[A] = {
        val offset = StreamFormat.read[Long](input)
        val value = StreamFormat.read[A](input)
        Message(offset, value)
      }
    }
  }
}
