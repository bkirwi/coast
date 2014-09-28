package com.monovore.coast.samza

import com.monovore.coast.samza.SamzaTasklet.Message

trait SamzaTasklet extends Serializable {

  def execute(message: Message): Seq[Message]

}

object SamzaTasklet {

  case class Message(stream: String, key: Array[Byte], value: Array[Byte])

}
