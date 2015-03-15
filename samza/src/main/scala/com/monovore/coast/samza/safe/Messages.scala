package com.monovore.coast.samza.safe

import com.monovore.coast.wire.DataFormat

object Messages {

  type StreamName = String
  type Partition = Int
  type Offset = Long
  type Qualifier = Array[Byte]

  // TODO: Case classes?

  type MergeInfo = (StreamName, Partition, Offset)

  object MergeInfo {

    val binaryFormat = DataFormat.wireFormat[MergeInfo]
  }
  
  type InternalMessage = (Qualifier, Offset, Array[Byte])
  
  object InternalMessage {
    
    val binaryFormat = DataFormat.wireFormat[InternalMessage]
  }
}
