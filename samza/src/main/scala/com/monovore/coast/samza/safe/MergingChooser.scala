package com.monovore.coast.samza.safe

import org.apache.samza.Partition
import org.apache.samza.config.Config
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.system.chooser.{MessageChooser, MessageChooserFactory}
import org.apache.samza.system.{IncomingMessageEnvelope, SystemStream, SystemStreamPartition}
import org.apache.samza.util.Logging

import collection.mutable

/**
 */
class MergingChooser(config: Config) extends MessageChooser with Logging {

  type StreamOffset = (SystemStreamPartition, Long)

  class Partitioned(mergeStream: SystemStream, merged: Map[String, SystemStream]) {

    val pending: mutable.Set[IncomingMessageEnvelope] = mutable.Set()

    val mergeQueue: mutable.Queue[StreamOffset] = mutable.Queue()

    val pendingMerge: mutable.Queue[IncomingMessageEnvelope] = mutable.Queue()

    var bootstrapping: Boolean = true

    def update(envelope: IncomingMessageEnvelope): Unit = {

      val incomingStream = envelope.getSystemStreamPartition.getSystemStream

      if (mergeStream == incomingStream) {
        if (bootstrapping) pendingMerge += envelope
      }
      else pending += envelope
    }

    def choose(): Option[IncomingMessageEnvelope] = {

      def popMessage: Option[IncomingMessageEnvelope] = for {
        (ssp, offset) <- mergeQueue.headOption
        message <- pending.find { _.getSystemStreamPartition == ssp }
      } yield {
        mergeQueue.dequeue()
        pending.remove(message)
        message
      }

      def popMerge: Option[IncomingMessageEnvelope] = for {
        message <- pendingMerge.headOption
      } yield {

        pendingMerge.dequeue()

        val (streamName, partition, offset) =
          Messages.MergeInfo.binaryFormat.fromArray(message.getMessage.asInstanceOf[Array[Byte]])

        val systemStream = merged(streamName)

        val ssp = new SystemStreamPartition(
          systemStream.getSystem,
          systemStream.getStream,
          new Partition(partition)
        )

        mergeQueue.enqueue(ssp -> offset)

        message
      }

      def newMessage: Option[IncomingMessageEnvelope] = for {
        message <- pending.headOption
        if mergeQueue.isEmpty
      } yield {

        if (bootstrapping) {
          info("Merging chooser is fully recovered; returning to normal operation.")
          bootstrapping = false
        }

        pending.remove(message)
        message
      }

      popMessage orElse popMerge orElse newMessage
    }
  }

  val partitions: mutable.Map[Partition, Partitioned] = mutable.Map()

  override def start(): Unit = {}

  override def stop(): Unit = {}

  override def register(systemStreamPartition: SystemStreamPartition, offset: String): Unit = {

    val systemStream = systemStreamPartition.getSystemStream()

    val mergeOption = Option(
      config.get(s"systems.${systemStream.getSystem}.streams.${systemStream.getStream}.merge")
    )

    mergeOption foreach { string =>

      val merged = string.split(',').toList
        .map { entry =>

          val fullName = entry //.split('=').toSeq

          val systemStream = fullName.split('.').toList match {
            case system :: streamComponents if streamComponents.nonEmpty => {
              new SystemStream(system, streamComponents.mkString("."))
            }
            case _ => throw new IllegalArgumentException(
              s"Bad system stream name: $fullName"
            )
          }

          systemStream.getStream -> systemStream
        }
        .toMap


      partitions.put(systemStreamPartition.getPartition, new Partitioned(systemStream, merged))
    }
  }

  override def update(envelope: IncomingMessageEnvelope): Unit = {

    partitions(envelope.getSystemStreamPartition.getPartition).update(envelope)
  }

  override def choose(): IncomingMessageEnvelope = {

    partitions.values.toStream.flatMap { _.choose() }.headOption.orNull
  }
}

class MergingChooserFactory extends MessageChooserFactory {

  override def getChooser(config: Config, registry: MetricsRegistry): MessageChooser = {

    new MergingChooser(config)
  }
}