package com.monovore.coast
package samza

import com.monovore.coast.wire.DataFormat

object Messages {

//  case class MergeInfo(name: String, partition: Int, offset: Long)

  type StreamName = String
  type Partition = Int
  type Offset = Long
  
  type Qualifier = Array[Byte]

  type MergeInfo = (StreamName, Partition, Offset)

  object MergeInfo {

    val binaryFormat = DataFormat.wireFormat[MergeInfo]
  }
  
  type InternalMessage = (Qualifier, Offset, Array[Byte])
  
  object InternalMessage {
    
    val binaryFormat = DataFormat.wireFormat[InternalMessage]
  }
}
