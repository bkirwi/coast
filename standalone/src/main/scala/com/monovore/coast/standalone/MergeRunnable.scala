package com.monovore.coast.standalone

import com.lmax.disruptor._
import com.monovore.coast.wire.{Partitioner, Serializer}
import org.apache.kafka.clients.producer.{RecordMetadata, Callback, ProducerRecord, Producer}
import org.slf4j.LoggerFactory

import scala.annotation.tailrec

case class Entry[A](key: A, until: Long)

case class Upstream[A](
  buffer: RingBuffer[A],
  barrier: SequenceBarrier // waitFor returns the _most recent available_
)

sealed trait DataOrOffset[+A]
case class Data[+A](value: A) extends DataOrOffset[A]
case class Offset(value: Long) extends DataOrOffset[Nothing]

case class Message[A, B](var key: A, var value: B, var offset: Long, var downstreamOffset: Long, var sequenceNumber: Long)

object Message {
  def factory[A, B] = new EventFactory[Message[A, B]] {
    override def newInstance(): Message[A, B] = Message(null.asInstanceOf[A], null.asInstanceOf[B], 0L, 0L, 0L)
  }
}

class MergeRunnable[A, B] (
  sources: Map[A, Upstream[DataOrOffset[B]]],
  out: EventHandler[DataOrOffset[B]]
) extends EventHandler[Entry[A]] {
  override def onEvent(event: Entry[A], sequence: Long, endOfBatch: Boolean): Unit = {
    val source = sources(event.key)


  }
}

class ProduceRunnable[A, B](
  upstream: Upstream[Message[A, B]],
  progress: Sequence,
  sink: Output[A, B]
) extends Runnable {

  private[this] val logger = LoggerFactory.getLogger(getClass)

  def run(): Unit = {

    logger.info(s"Starting ProduceRunnable")

    @tailrec def sendFrom(nextSequence: Long) {
      val sequenceBound = upstream.barrier.waitFor(nextSequence)
      logger.info(s"Messages $nextSequence to $sequenceBound")
      for (s <- nextSequence to sequenceBound) {
        val msg = upstream.buffer.get(s)
        msg.offset = sink.send(msg.offset, msg.key, msg.value)
        msg.sequenceNumber = sink.sequenceNumber()
        progress.set(s)
      }
      sendFrom(sequenceBound + 1)
    }

    try {
      sendFrom(progress.get() + 1)
    } catch {
      case ie: InterruptedException => {
        logger.info("Interrupted ProduceRunnable; shutting down")
      }
    }
  }
}

case class Cursors(offset: Long, sequence: Long)

trait Output[A, B] {
  def sequenceNumber(): Long
  def send(offset: Long, key: A, value: B): Long
}

class ProducerOutput[A, B](
  producer: Producer[Array[Byte], Array[Byte]],
  keySerializer: Serializer[A],
  valueSerializer: Serializer[B],
  partitioner: Partitioner[A],
  numPartitions: Int,
  topic: String,
  progress: Sequence,
  var sequenceNumber: Long = -1L
) extends Output[A, B] {

  private[this] val logger = LoggerFactory.getLogger(getClass)

  @volatile private[this] var sendError: Throwable = null

  override def send(offset: Long, key: A, value: B): Long = {

    if (sendError != null) throw sendError

    sequenceNumber += 1

    val keyBytes = keySerializer.toArray(key)
    val valueBytes = valueSerializer.toArray(value)
    val partition = partitioner.partition(key, numPartitions)

    val record = new ProducerRecord(topic, partition, keyBytes, valueBytes)

    val recordSequence = sequenceNumber

    producer.send(
      record, new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
          if (sendError != null) {
            // Some upstream thing already failed, so we're shutting down
          }
          else if (exception != null) {
            sendError = exception
            logger.error("Error in producer callback", exception)
          }
          else progress.set(recordSequence)
        }
      }
    )

    offset + 1
  }
}

class SourceOutput[A, B](
  output: Output[A, B],
  keySerializer: Serializer[A],
  valueSerializer: Serializer[B]
) extends Output[Array[Byte], Array[Byte]] {
  override def sequenceNumber(): Long = output.sequenceNumber()
  override def send(offset: Long, key: Array[Byte], value: Array[Byte]): Long = {
    output.send(offset, keySerializer.fromArray(key), valueSerializer.fromArray(value))
  }
}

class PureOutput[A, B, C](output: Output[A, C], function: A => B => Seq[C]) extends Output[A, B] {
  override def sequenceNumber(): Long = output.sequenceNumber()
  override def send(offset: Long, key: A, value: B): Long = {
    val messages = function(key)(value)
    var currentOffset = offset
    for (message <- messages) {
      currentOffset = output.send(currentOffset, key, message)
    }
    currentOffset
  }
}

class GroupByOutput[A, B, C](output: Output[C, B], function: B => C) extends Output[A, B] {
  override def sequenceNumber(): Long = output.sequenceNumber()
  override def send(offset: Long, key: A, value: B): Long = {
    output.send(offset, function(value), value)
  }
}

