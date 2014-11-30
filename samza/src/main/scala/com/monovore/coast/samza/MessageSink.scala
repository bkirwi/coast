package com.monovore.coast
package samza

import com.monovore.coast.model._
import com.monovore.coast.wire.{DataFormat, BinaryFormat}
import org.apache.samza.Partition
import org.apache.samza.config.Config
import org.apache.samza.system.SystemFactory
import org.apache.samza.task.TaskContext
import org.apache.samza.util.Logging

import scala.collection.JavaConverters._

trait MessageSink[-K, -V] extends Serializable {

  def execute(stream: String, partition: Int, offset: Long, key: K, value: V): Long
}

object MessageSink {

  type Bytes = Array[Byte]

  type ByteSink = MessageSink[Bytes, Bytes]

  trait Factory extends Serializable {
    def make(config: Config, context: TaskContext, sink: ByteSink): ByteSink
  }

  class FromElement[A, B](sinkNode: Sink[A, B]) extends Factory {

    override def make(config: Config, context: TaskContext, whatSink: ByteSink) = {

      val taskName = config.get(samza.TaskName)

      val partitionIndex = context.getTaskName.getTaskName.split("\\W+").last.toInt // ICK!

      val regroupedStreams = config.get(RegroupedStreams).split(",").filter { _.nonEmpty }.toSet

      val offsetThreshold =
        if (regroupedStreams(taskName)) 0L
        else {

          val systemFactory = config.getNewInstance[SystemFactory](s"systems.$CoastSystem.samza.factory")
          val admin = systemFactory.getAdmin(CoastSystem, config)
          val meta = admin.getSystemStreamMetadata(Set(taskName).asJava).asScala
            .getOrElse(taskName, sys.error(s"Couldn't find metadata on output stream $taskName"))

          val partitionMeta = meta.getSystemStreamPartitionMetadata.asScala

          partitionMeta(new Partition(partitionIndex)).getUpcomingOffset.toLong
        }

      val finalSink = new MessageSink.ByteSink {

        override def execute(stream: String, partition: Int, offset: Long, key: Bytes, value: Bytes): Long = {

          if (offset >= offsetThreshold) {
            whatSink.execute(taskName, partition, offset, key, value)
          }

          offset + 1
        }
      }

      val last = new MessageSink[A, B] with Logging {

        var nextOffset: Long = _

        override def execute(stream: String, partition: Int, offset: Long, key: A, value: B): Long = {

          val keyBytes = sinkNode.keyFormat.write(key)
          val valueBytes = sinkNode.valueFormat.write(value)

          val newPartition = sinkNode.keyPartitioner.hash(key).asInt()

          val payload =
            if (regroupedStreams(taskName)) {
              BinaryFormat.write(FullMessage(taskName, partitionIndex, offset, valueBytes))
            } else {
              valueBytes
            }

          finalSink.execute(stream, newPartition, offset, keyBytes, payload)
        }
      }

      val name = config.get(samza.TaskName)

      val compiler = new Compiler(new Compiler.Context {
        override def getStore[P, A, B](path: String, default: B): CoastState[Int, A, B] =
          context.getStore(path).asInstanceOf[CoastStorageEngine[A, B]].withDefault(default)
      })

      val (_, thing) = compiler.compile(sinkNode.element, last, List(name))

      val mergeStream = s"coast.merge.$taskName"

      new MessageSink.ByteSink {

        override def execute(stream: String, partition: Int, offset: Long, key: Bytes, value: Bytes): Long = {

          if (stream == mergeStream) {

            val fullMessage = BinaryFormat.read[FullMessage](value)

            thing.execute(fullMessage.stream, partition, fullMessage.offset, key, fullMessage.value)

          } else if (regroupedStreams(stream)) {

            whatSink.execute(
              mergeStream,
              partitionIndex,
              -1,
              key,
              value
            )
          } else{

            whatSink.execute(
              mergeStream,
              partitionIndex,
              -1,
              key,
              BinaryFormat.write(
                FullMessage(stream, 0, offset, value)
              )
            )
          }
        }
      }
    }
  }
}