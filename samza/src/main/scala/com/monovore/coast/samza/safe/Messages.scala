package com.monovore.coast.samza.safe

import com.monovore.coast.wire.{Protocol, Serializer}

object Messages {

  import Protocol.common._

  type StreamName = String
  type Partition = Int
  type Offset = Long
  type Qualifier = Array[Byte]

  // TODO: Case classes?

  type MergeInfo = (StreamName, Partition, Offset)

  object MergeInfo {

    val binaryFormat = implicitly[Serializer[MergeInfo]]
  }
  
  type InternalMessage = (Qualifier, Offset, Array[Byte])
  
  object InternalMessage {
    
    val binaryFormat = implicitly[Serializer[InternalMessage]]
  }
}
