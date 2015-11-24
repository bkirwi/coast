package com.monovore.coast.standalone

import java.util.concurrent.Executors

import com.lmax.disruptor.{EventHandler, EventFactory}
import com.lmax.disruptor.dsl.Disruptor

object DistruptorApp {

  // Executor that will be used to construct new threads for consumers
  val executor = Executors.newCachedThreadPool()

  // Specify the size of the ring buffer, must be power of 2.
  val bufferSize = 1024

  // Construct the Disruptor
  val disruptor = new Disruptor(TaskLogEntry, bufferSize, executor)

  // Connect the handler
  disruptor.handleEventsWith(new EventHandler[TaskLogEntry] {
    override def onEvent(event: TaskLogEntry, sequence: Long, endOfBatch: Boolean): Unit = {

    }
  })

  // Start the Disruptor, starts all threads running
  disruptor.start()

  // Get the ring buffer from the Disruptor to be used for publishing.
  val ringBuffer = disruptor.getRingBuffer

  ringBuffer.publish(9)

}

case class TaskLogEntry(var source: String, var offset: Long)

object TaskLogEntry extends EventFactory[TaskLogEntry] {
  override def newInstance(): TaskLogEntry = TaskLogEntry("", 0L)
}
