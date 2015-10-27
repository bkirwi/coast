package com.monovore.coast.samza.safe

import com.monovore.coast.samza.Path
import com.monovore.coast.samza.safe.Checkpoint._
import com.monovore.coast.wire.{Protocol, Serializer}

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
  val format: Serializer[Checkpoint] = Serializer.fromJavaSerialization[Checkpoint]

  val keyFormat: Serializer[Unit] = Protocol.common.unitFormat
}