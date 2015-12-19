package com.monovore.coast.standalone

import java.util

import com.lmax.disruptor.{EventTranslator, Sequence, RingBuffer, EventFactory}
import com.monovore.coast.core._
import com.monovore.coast.flow.Flow
import com.monovore.coast.standalone.kafka.CoastAssignor
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer.{Producer, KafkaProducer, ProducerConfig}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable

abstract class StandaloneApp {

  def appName: String = { this.getClass.getSimpleName.stripSuffix("$") }

  private[this] val logger = LoggerFactory.getLogger(this.getClass.toString.stripSuffix("$"))

  implicit val builder = Flow.builder()

  def main(args: Array[String]): Unit = {

    logger.info(s"Starting application $appName")

    val flow = builder.toFlow

    println(flow.bindings)

    val groups = CoastAssignor.topicGroups(flow)

    val groupConfig = groups.map { case (k, v) => s"coast.topics.$k" -> v.mkString(",") }

    val consumer = new KafkaConsumer(
      (groupConfig ++ Map[String, AnyRef](
        ConsumerConfig.GROUP_ID_CONFIG -> s"coast.$appName",
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
        ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG -> "8000",
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
        ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG -> classOf[CoastAssignor].getName
      )).asJava,
      new ByteArrayDeserializer,
      new ByteArrayDeserializer
    )

    val allTopics = consumer.listTopics().asScala.toMap

    val topics  = groups.flatMap { case (k, v) => v + k }.toSet

    groups.foreach { case (group, x) => println(allTopics.get(group)) }

    val producer = new KafkaProducer(
      Map[String, AnyRef](
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
        ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION -> "1"
      ).asJava,
      new ByteArraySerializer,
      new ByteArraySerializer
    )

    val topicSizes =
      consumer.listTopics().asScala.mapValues { _.size }

    val taskPartitions = CoastAssignor.partitionAssignments(groups, topicSizes)

    val taskState = mutable.Map.empty[TopicPartition, Task]

    def makeTask[A, B](topic: String, partition: Int, sink: Sink[A, B]): Task = {

      def makeOutput[A, B](node: Node[A, B], output: Output[A, B]): (Source[A, B], Output[A, B]) = node match {
        case src: Source[A, B] => (src, output)
//        case pure: PureTransform[A, b0, B] => {
//          val newOutput = new PureOutput(output, pure.function)
//          makeOutput(pure.upstream, newOutput)
//        }
      }

      val buffer = RingBuffer.createMultiProducer(Message.factory[A, B], 1024)

      val taskOutput = new ProducerOutput(
        producer,
        sink.keyFormat,
        sink.valueFormat,
        sink.keyPartitioner,
        topicSizes(topic),
        topic,
        ???
      )

      val (source, _) = makeOutput(sink.element, taskOutput)

      val out = new ProduceRunnable(
        Upstream(buffer, buffer.newBarrier()),
        new Sequence,
        taskOutput
      )

      new Task {

        val sinkThread = new Thread(out, "sink-thread")

        sinkThread.start()

        override def append(input: ConsumerRecords[Array[Byte], Array[Byte]]): Unit = {
          for (s <- taskPartitions(new TopicPartition(s"coast.log.$topic", partition))) {
            input.records(s).asScala
              .foreach { record =>
                logger.info(s"Append $record")
                buffer.publishEvent(
                  new EventTranslator[Message[A, B]] {
                    override def translateTo(event: Message[A, B], sequence: Long): Unit = {
                      event.key = Option(record.key).map(source.keyFormat.fromArray).getOrElse(record.partition.asInstanceOf[A])
                      event.value = source.valueFormat.fromArray(record.value)
                      event.offset = record.offset
                    }
                  }
                )
              }
          }
        }

        override def stop(): Unit = sinkThread.interrupt()
      }
    }

    consumer.subscribe(topics.toSeq.asJava, new ConsumerRebalanceListener {
      override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {

        val assignedTasks =
          partitions.asScala
            .filter { tp => groups.contains(tp.topic) }
            .toSet

        val runningTasks = taskState.keySet

        val removed = runningTasks -- assignedTasks

        val added = assignedTasks -- runningTasks

        logger.info(s"Rebalancing! Added [${added.mkString(", ")}], removed [${removed.mkString(", ")}]")

        for (partition <- removed) {
          taskState.remove(partition).foreach { task => task.stop() }
        }

        for (partition <- added) {

          val checkpoint =
            Option(consumer.committed(partition))
              .map { _ => Checkpoint() }
              .getOrElse(Checkpoint())

          logger.info(s"Checkpoint for partition $partition: $checkpoint")

          consumer.seekToEnd(partition)
          val upcomingOffset = consumer.position(partition)
          consumer.seek(partition, checkpoint.offset)

          logger.info(s"Offset for partition $partition: $upcomingOffset")

          for (source <- taskPartitions(partition)) {
            consumer.seek(source, checkpoint.sources.getOrElse(source, 0L))
          }

          val sinkTo = new TopicPartition(partition.topic.split("\\.")(2), partition.partition)

          taskState.put(partition, makeTask(sinkTo.topic, sinkTo.partition, flow.bindings.find { _._1 == sinkTo.topic }.get._2))
        }
        logger.info(s"Running tasks: [${taskState.mkString(", ")}]")
      }
      override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {
      }
    })

    while(true) {
      val polled = consumer.poll(1000L)
      taskState.values.foreach(_.append(polled))
      Thread.sleep(1000)
    }
  }
}

trait Task {
  def append(input: ConsumerRecords[Array[Byte], Array[Byte]])
  def position: Map[TopicPartition, Long] = ???
  def stop(): Unit
}

case class Checkpoint(
  offset: Long = 0L,
  sources: Map[TopicPartition, Long] = Map.empty
)