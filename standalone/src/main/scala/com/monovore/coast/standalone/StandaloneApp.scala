package com.monovore.coast.standalone

import java.util

import com.lmax.disruptor.EventFactory
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

    println(topicSizes)

    val taskPartitions = CoastAssignor.partitionAssignments(groups, topicSizes)

    val taskState = mutable.Map.empty[TopicPartition, Unit]

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
          taskState.remove(partition)
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

          taskState.put(partition, ())
        }
        logger.info(s"Running tasks: [${taskState.mkString(", ")}]")
      }
      override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {
      }
    })

    while(true) {
      println("polling...")
      val polled = consumer.poll(1000L)

//      polled.partitions().asScala.foreach { partition =>
//        logger.info(s"$partition ${topicState(partition).getCursor}")
//        polled.records(partition).asScala.foreach { record =>
//          topicState(partition).tryPublishEvent(
//            new EventTranslator[Record] {
//              override def translateTo(event: Record, sequence: Long): Unit = {
//                event.key = record.key
//                event.value = record.value
//              }
//            }
//          )
//        }
//      }

      Thread.sleep(1000)
    }
  }
}

trait Task {

}

object Task {

  def compile(graph: Graph, producer: Producer[Array[Byte], Array[Byte]]): Map[String, Task] = {

    type Sink[A, B] = (A, B) => Unit

    def compileNode[A, B](node: Node[A, B], sink: Sink[A, B]): Task = node match {
      case src: Source[A, B] => new Task {}
      case PureTransform(upstream, fn) => ???
    }

    ???
  }
}

case class Checkpoint(
  offset: Long = 0L,
  sources: Map[TopicPartition, Long] = Map.empty
)

case class Record(var key: Array[Byte], var value: Array[Byte])

object Record extends EventFactory[Record] {
  override def newInstance(): Record = Record(null, null)
}
//
//class Gak(
//  consumer: KafkaConsumer[Array[Byte], Array[Byte]],
//  producer: KafkaProducer[Array[Byte], Array[Byte]],
//  tasks: Map[TopicPartition, Checkpoint => Task]
//) extends Runnable {
//
//  override def run(): Unit = {
//
//
//  }
//}