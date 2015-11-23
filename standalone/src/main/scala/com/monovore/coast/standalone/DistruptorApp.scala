package com.monovore.coast.standalone

import java.util.concurrent.Executors

import com.lmax.disruptor.{EventHandler, EventFactory}
import com.lmax.disruptor.dsl.Disruptor

object DistruptorApp {

  // Executor that will be used to construct new threads for consumers
  val executor = Executors.newCachedThreadPool();

  // Specify the size of the ring buffer, must be power of 2.
  val bufferSize = 1024;

  val factory = new EventFactory[Long] {
    override def newInstance(): Long = 8
  }

  // Construct the Disruptor
  val disruptor = new Disruptor(factory, bufferSize, executor);

  // Connect the handler
  disruptor.handleEventsWith(new EventHandler[Long] {
    override def onEvent(event: Long, sequence: Long, endOfBatch: Boolean): Unit = ???
  });

  // Start the Disruptor, starts all threads running
  disruptor.start()

  // Get the ring buffer from the Disruptor to be used for publishing.
  val ringBuffer = disruptor.getRingBuffer()

  ringBuffer.publish(9)

}
