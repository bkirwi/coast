package com.monovore.coast.samza

import java.io.{DataInputStream, ByteArrayInputStream, DataOutputStream, ByteArrayOutputStream}

import com.monovore.coast.wire.BinaryFormat

case class FullMessage(
  stream: String,
  partition: Int,
  offset: Long,
  value: Array[Byte]
)

object FullMessage {

  implicit val messageFormat = new BinaryFormat[FullMessage] {

    override def write(value: FullMessage): Array[Byte] = {

      val baos = new ByteArrayOutputStream()
      val dout = new DataOutputStream(baos)

      dout.writeUTF(value.stream)
      dout.writeInt(value.partition)
      dout.writeLong(value.offset)
      dout.writeInt(value.value.length)
      dout.write(value.value)

      dout.flush()
      baos.toByteArray
    }

    override def read(bytes: Array[Byte]): FullMessage = {

      val bais = new ByteArrayInputStream(bytes)
      val din = new DataInputStream(bais)

      val stream = din.readUTF()
      val partition = din.readInt()
      val offset = din.readLong()
      val valueLength = din.readInt()
      val value = Array.ofDim[Byte](valueLength)
      din.read(value, 0, valueLength)

      FullMessage(stream, partition, offset, value)
    }
  }
}
