package com.monovore.coast.standalone

import com.lmax.disruptor.{EventHandler, Sequence, SequenceBarrier, RingBuffer}

case class Entry[A](key: A, until: Long)

case class Upstream[A](buffer: RingBuffer[A], barrier: SequenceBarrier, progress: Sequence)

sealed trait DataOrOffset[+A]
case class Data[+A](value: A) extends DataOrOffset[A]
case class Offset(value: Long) extends DataOrOffset[Nothing]

class MergeRunnable[A, B] (
  sources: Map[A, Upstream[DataOrOffset[B]]],
  out: EventHandler[DataOrOffset[B]]
) extends EventHandler[Entry[A]] {
  override def onEvent(event: Entry[A], sequence: Long, endOfBatch: Boolean): Unit = {
    val source = sources(event.key)


  }
}

case class FunctionEventHandler[T](function: (T, Long, Boolean) => Unit) extends EventHandler[T] {
  override def onEvent(event: T, sequence: Long, endOfBatch: Boolean): Unit = function(event, sequence, endOfBatch)
}

