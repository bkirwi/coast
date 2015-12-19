package com.monovore.coast.standalone.bath

trait Output[A, B] {
  def send(key: A, value: B, offset: Long): Long
}

class CounterOutput[A, B](var highWaterMark: Long, outputFn: (A, B, Long) => Unit) extends Output[A, B] {
  override def send(key: A, value: B, offset: Long): Long = {
    if (offset >= highWaterMark) {
      outputFn(key, value, offset)
      highWaterMark = offset + 1
    }
    highWaterMark
  }
}

class PureOutput[A, B, C](fn: Nothing)


/*

Okay:



 */


