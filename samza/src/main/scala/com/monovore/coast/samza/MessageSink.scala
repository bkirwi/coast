package com.monovore.coast
package samza

import org.apache.samza.config.Config
import org.apache.samza.task.TaskContext

trait MessageSink[-K, -V] extends Serializable {

  def execute(stream: String, partition: Int, offset: Long, key: K, value: V): Long
}

object MessageSink {

  type Bytes = Array[Byte]

  type ByteSink = MessageSink[Bytes, Bytes]

  trait Factory extends Serializable {
    def make(config: Config, context: TaskContext, sink: ByteSink): ByteSink
  }
}