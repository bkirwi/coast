package com.monovore.coast.wire

import java.io.{DataInputStream, DataOutputStream}

trait StreamFormat[A] extends Serializable {
  def write(output: DataOutputStream, value: A): Unit
  def read(input: DataInputStream): A
}

object StreamFormat {

  def read[A](input: DataInputStream)(implicit reader: StreamFormat[A]): A = reader.read(input)

  def write[A](output: DataOutputStream, value: A)(implicit writer: StreamFormat[A]) = writer.write(output, value)

  def fromSerializer[A](serializer: Serializer[A]) = new StreamFormat[A] {
    override def write(output: DataOutputStream, value: A): Unit = {
      val array = serializer.toArray(value)
      output.writeInt(array.length)
      output.write(array)
    }
    override def read(input: DataInputStream): A = {
      val size = input.readInt()
      val array = Array.ofDim[Byte](size)
      input.readFully(array)
      serializer.fromArray(array)
    }
  }

  implicit object longFormat extends StreamFormat[Long] {
    override def write(output: DataOutputStream, value: Long): Unit = output.writeLong(value)
    override def read(input: DataInputStream): Long = input.readLong()
  }

  implicit object intFormat extends StreamFormat[Int] {
    override def write(output: DataOutputStream, value: Int): Unit = output.writeInt(value)
    override def read(input: DataInputStream): Int = input.readInt()
  }

  implicit object stringFormat extends StreamFormat[String] {
    override def write(output: DataOutputStream, value: String): Unit = output.writeUTF(value)
    override def read(input: DataInputStream): String = input.readUTF()
  }

  implicit object unitFormat extends StreamFormat[Unit] {
    override def write(output: DataOutputStream, value: Unit): Unit = {}
    override def read(input: DataInputStream): Unit = {}
  }

  implicit val bytesFormat: StreamFormat[Array[Byte]] = new StreamFormat[Array[Byte]] {

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

  implicit def optionFormat[A](implicit format: StreamFormat[A]): StreamFormat[Option[A]] = new StreamFormat[Option[A]] {

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

  implicit def tuple2Format[A : StreamFormat, B : StreamFormat] = new StreamFormat[(A, B)] {

    override def write(output: DataOutputStream, value: (A, B)): Unit = {
      StreamFormat.write(output, value._1)
      StreamFormat.write(output, value._2)
    }

    override def read(input: DataInputStream): (A, B) = {
      val a = StreamFormat.read[A](input)
      val b = StreamFormat.read[B](input)
      (a, b)
    }
  }

  implicit def tuple3Format[A : StreamFormat, B : StreamFormat, C : StreamFormat] = new StreamFormat[(A, B, C)] {

    override def write(output: DataOutputStream, value: (A, B, C)): Unit = {
      StreamFormat.write(output, value._1)
      StreamFormat.write(output, value._2)
      StreamFormat.write(output, value._3)
    }

    override def read(input: DataInputStream): (A, B, C) = {
      val a = StreamFormat.read[A](input)
      val b = StreamFormat.read[B](input)
      val c = StreamFormat.read[C](input)
      (a, b, c)
    }
  }

  implicit def seqFormat[A : StreamFormat] = new StreamFormat[Seq[A]] {

    override def write(output: DataOutputStream, value: Seq[A]): Unit = {
      output.writeInt(value.size)
      value.foreach(StreamFormat.write(output, _))
    }

    override def read(input: DataInputStream): Seq[A] = {
      val size = input.readInt()
      Seq.fill(size)(StreamFormat.read[A](input))
    }
  }

  implicit def mapFormat[A : StreamFormat, B : StreamFormat] = new StreamFormat[Map[A, B]] {

    override def write(output: DataOutputStream, value: Map[A, B]): Unit = {
      output.writeInt(value.size)
      value.foreach { case (k, v) =>
        StreamFormat.write(output, k)
        StreamFormat.write(output, v)
      }
    }

    override def read(input: DataInputStream): Map[A, B] = {
      val size = input.readInt()
      Iterator.fill(size)(StreamFormat.read[A](input) -> StreamFormat.read[B](input))
        .toMap
    }
  }
}