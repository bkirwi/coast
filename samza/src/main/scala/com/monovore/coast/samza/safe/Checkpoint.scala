package com.monovore.coast.samza.safe

import com.monovore.coast.samza.Path
import com.monovore.coast.samza.safe.Checkpoint._
import com.monovore.coast.wire.BinaryFormat

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

  // FIXME: real serialization format for this. (JSON?)
  val format: BinaryFormat[Checkpoint] = BinaryFormat.javaSerialization[Checkpoint]

  val keyFormat: BinaryFormat[Unit] = BinaryFormat.defaults.unitFormat
}