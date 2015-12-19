package com.monovore.coast.standalone

import com.lmax.disruptor.{RingBuffer, Sequence}
import com.monovore.coast.core.{Node, PureTransform, Source}
import org.apache.kafka.common.TopicPartition

/**
  *
  */
class Compiler(
  sourceToTopicPartition: String => TopicPartition,
  bufferSize: Int = 1024
) {

  case class Info[X]()

  def compile[A, B, Pull](
    node: Node[A, B],
    output: Sequence => (Pull, Output[A, B])
  ): Info[Pull] = node match {

    case src: Source[A, B] => {

      val progress = new Sequence

      val inputPartition = sourceToTopicPartition(src.source)

      val (hex, out) = output(progress)

      val taskOutput = new SourceOutput(out, src.keyFormat, src.valueFormat)

      val buffer = RingBuffer.createMultiProducer(Message.factory[Array[Byte], Array[Byte]], bufferSize)

      buffer.addGatingSequences(progress)

      val runnable = new ProduceRunnable(
        Upstream(buffer, buffer.newBarrier()),
        progress,
        taskOutput
      )

      Info()
    }
    case pure: PureTransform[A, b0, B] => {
      val newOutput = { x: Sequence =>
        val (y, o) = output(x)
        y -> new PureOutput(o, pure.function)
      }
      compile(pure.upstream, newOutput)
    }
  }

}
