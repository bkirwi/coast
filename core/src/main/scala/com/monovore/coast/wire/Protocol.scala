package com.monovore.coast.wire

import java.io.{DataInputStream, DataOutputStream}

object Protocol {

  object common {
    implicit object longFormat extends Serializer[Long] {
      override def write(output: DataOutputStream, value: Long): Unit = output.writeLong(value)
      override def read(input: DataInputStream): Long = input.readLong()
    }

    implicit object intFormat extends Serializer[Int] {
      override def write(output: DataOutputStream, value: Int): Unit = output.writeInt(value)
      override def read(input: DataInputStream): Int = input.readInt()
    }

    implicit object stringFormat extends Serializer[String] {
      override def write(output: DataOutputStream, value: String): Unit = output.writeUTF(value)
      override def read(input: DataInputStream): String = input.readUTF()
    }

    implicit object unitFormat extends Serializer[Unit] {
      override def write(output: DataOutputStream, value: Unit): Unit = {}
      override def read(input: DataInputStream): Unit = {}
    }

    implicit val bytesFormat: Serializer[Array[Byte]] = new Serializer[Array[Byte]] {

      override def write(output: DataOutputStream, value: Array[Byte]): Unit = {
        output.writeInt(value.length)
        output.write(value)
      }

      override def read(input: DataInputStream): Array[Byte] = {
        val size = input.readInt()
        val bytes = Array.ofDim[Byte](size)
        input.readFully(bytes)
        bytes
      }
    }

    implicit def optionFormat[A](implicit format: Serializer[A]): Serializer[Option[A]] = new Serializer[Option[A]] {

      override def write(output: DataOutputStream, value: Option[A]): Unit = value match{
        case Some(a) => {
          output.writeBoolean(true)
          format.write(output, a)
        }
        case None => output.writeBoolean(false)
      }

      override def read(input: DataInputStream): Option[A] = {
        if (input.readBoolean()) Some(format.read(input))
        else None
      }
    }

    implicit def tuple2Format[A : Serializer, B : Serializer] = new Serializer[(A, B)] {

      override def write(output: DataOutputStream, value: (A, B)): Unit = {
        Serializer.write(output, value._1)
        Serializer.write(output, value._2)
      }

      override def read(input: DataInputStream): (A, B) = {
        val a = Serializer.read[A](input)
        val b = Serializer.read[B](input)
        (a, b)
      }
    }

    implicit def tuple3Format[A : Serializer, B : Serializer, C : Serializer] = new Serializer[(A, B, C)] {

      override def write(output: DataOutputStream, value: (A, B, C)): Unit = {
        Serializer.write(output, value._1)
        Serializer.write(output, value._2)
        Serializer.write(output, value._3)
      }

      override def read(input: DataInputStream): (A, B, C) = {
        val a = Serializer.read[A](input)
        val b = Serializer.read[B](input)
        val c = Serializer.read[C](input)
        (a, b, c)
      }
    }

    implicit def seqFormat[A : Serializer] = new Serializer[Seq[A]] {

      override def write(output: DataOutputStream, value: Seq[A]): Unit = {
        output.writeInt(value.size)
        value.foreach(Serializer.write(output, _))
      }

      override def read(input: DataInputStream): Seq[A] = {
        val size = input.readInt()
        Seq.fill(size)(Serializer.read[A](input))
      }
    }

    implicit def mapFormat[A : Serializer, B : Serializer] = new Serializer[Map[A, B]] {

      override def write(output: DataOutputStream, value: Map[A, B]): Unit = {
        output.writeInt(value.size)
        value.foreach { case (k, v) =>
          Serializer.write(output, k)
          Serializer.write(output, v)
        }
      }

      override def read(input: DataInputStream): Map[A, B] = {
        val size = input.readInt()
        Iterator.fill(size)(Serializer.read[A](input) -> Serializer.read[B](input))
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

  object native {

    implicit def anyPartitioner[A]: Partitioner[A] = Partitioner.byHashCode

    implicit def anyFormat[A]: Serializer[A] = Serializer.fromJavaSerialization[A]
  }
}