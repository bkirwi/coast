package com.monovore.coast.samza

import java.io.{DataOutput, DataInput}

import com.monovore.coast.samza.CoastTask.Receiver
import com.monovore.coast.wire.{BinaryFormat, DataFormat}
import org.apache.samza.system.{SystemStream, SystemStreamPartition}

import Checkpoint._

import collection.mutable

case class Checkpoint(
  inputStreams: Map[(String, Int), InputState],
  mergeOffset: Long,
  outputStreams: Map[Path, Long]
)

object Checkpoint {

  case class InputState(offset: Long, qualifiers: Map[Seq[Byte], Long])

  object InputState {
    val default = InputState(0L, Map.empty)
  }

  val format: BinaryFormat[Checkpoint] = com.monovore.coast.wire.javaSerialization[Checkpoint]

  val keyFormat: BinaryFormat[Unit] = new BinaryFormat[Unit] {

    // non-empty key to prevent compacting it away
    private[this] val singleByte = Array(0.toByte)

    override def write(value: Unit): Array[Byte] = singleByte

    override def read(bytes: Array[Byte]): Unit = ()
  }

}