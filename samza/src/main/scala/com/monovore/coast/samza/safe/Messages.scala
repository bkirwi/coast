package com.monovore.coast.samza.safe

import com.monovore.coast.wire.BinaryFormat

object Messages {

  import com.monovore.coast.wire.pretty._

  type StreamName = String
  type Partition = Int
  type Offset = Long
  type Qualifier = Array[Byte]

  // TODO: Case classes?

  type MergeInfo = (StreamName, Partition, Offset)

  object MergeInfo {

    val binaryFormat = implicitly[BinaryFormat[MergeInfo]]
  }
  
  type InternalMessage = (Qualifier, Offset, Array[Byte])
  
  object InternalMessage {
    
    val binaryFormat = implicitly[BinaryFormat[InternalMessage]]
  }
}
