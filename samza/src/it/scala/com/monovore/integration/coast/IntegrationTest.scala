package com.monovore.integration.coast

import java.nio.ByteBuffer
import java.util
import java.util.Properties

import com.monovore.coast
import kafka.api.{PartitionFetchInfo, FetchRequest, TopicMetadataRequest, OffsetRequest}
import kafka.common.{UnknownTopicOrPartitionException, TopicAndPartition}
import kafka.consumer.{Consumer, ConsumerConfig, ConsumerConnector, SimpleConsumer}
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.{TestUtils, TestZKUtils}
import kafka.zk.EmbeddedZookeeper
import org.apache.samza.job.ApplicationStatus
import org.apache.samza.job.local.ThreadJobFactory

import scala.util.Random

object IntegrationTest {

  def withKafkaCluster[A](withProps: java.util.Properties => A): A = {

    val Seq(port0) = TestUtils.choosePorts(1)

    val broker0 = TestUtils.createBrokerConfig(0, port0)
    broker0.setProperty("auto.create.topics.enable", "true")
    broker0.setProperty("num.partitions", "3")

    val config = new java.util.Properties()

    val brokers = s"localhost:$port0"
    val zkString = TestZKUtils.zookeeperConnect

    config.setProperty("metadata.broker.list", brokers)
    config.setProperty("producer.type", "sync")
    config.setProperty("request.required.acks", "1")
    config.setProperty("message.send.max.retries", "0")

    config.setProperty("zookeeper.connect", zkString)
    config.setProperty("group.id", "input-producer")
    config.setProperty("auto.offset.reset", "smallest")

    var zookeeper: EmbeddedZookeeper = null
    var server0: KafkaServer = null

    try {
      zookeeper = new EmbeddedZookeeper(zkString)

      server0 = TestUtils.createServer(new KafkaConfig(broker0))

      withProps(config)

    } finally {

      if (server0 != null) {
        server0.shutdown()
        server0.awaitShutdown()
      }

      if (zookeeper != null) {
        zookeeper.shutdown()
      }
    }
  }

  def fuzz(flow: coast.Flow[Unit], input: Messages): Messages = {

    val factory = new ThreadJobFactory

    var producer: Producer[Array[Byte], Array[Byte]] = null

    IntegrationTest.withKafkaCluster { config =>

      val producerConfig = new ProducerConfig(config)

      try {

        // Give Kafka a chance to create the partitions
        IntegrationTest.slurp(input.messages.keySet, config)
        Thread.sleep(500)

        producer = new Producer(producerConfig)

        for {
          (name, messages) <- input.messages
          (key, values) <- messages
          value <- values.grouped(100)
        } {
          producer.send(value.map { value => new KeyedMessage(name, key.toArray, util.Arrays.hashCode(key.toArray), value.toArray)}: _*)
        }

        val configs = coast.samza.configureFlow(flow)(
          baseConfig = coast.samza.config(
            "task.commit.ms" -> "500",
//            "task.window.ms" -> "300",
//            "task.chooser.class" -> "com.monovore.integration.coast.RandomMessageChooserFactory",
            "task.checkpoint.replication.factor" -> "1",
            "systems.coast-system.consumer.zookeeper.connect" -> config.getProperty("zookeeper.connect"),
            "systems.coast-system.producer.metadata.broker.list" -> config.getProperty("metadata.broker.list")
          )
        )

        // FLAIL!

        val sleeps = (0 until 4).map { _ => Random.nextInt(800) + 600} ++ Seq(8000)

        for (sleepTime <- sleeps) {

          val jobs = configs.values.toSeq
            .map { config => factory.getJob(config)}

          jobs.foreach {
            _.submit()
          }

          Thread.sleep(sleepTime)

          jobs.foreach {
            _.kill()
          }

          jobs.foreach { job =>
            job.waitForFinish(2000) match {
              case ApplicationStatus.SuccessfulFinish => ()
              case ApplicationStatus.UnsuccessfulFinish => ()
              case _ => sys.error("Taking a very long time!")
            }
          }
        }

        val outputStreams = flow.bindings.map { case (name, _) => name}

        IntegrationTest.slurp(outputStreams.toSet, config)

      } finally {

        if (producer != null) {
          producer.close()
        }
      }
    }
  }

  def slurp(topics: Set[String], config: Properties): Messages = {

    var simple: Map[Int, SimpleConsumer] = null

    try {

      val ports = config.getProperty("metadata.broker.list").split(",")
        .map { _.split(":")(1).toInt }

      simple = ports
        .map { port =>
          port -> new SimpleConsumer("localhost", port, ConsumerConfig.SocketTimeout, ConsumerConfig.SocketBufferSize, ConsumerConfig.DefaultClientId)
        }
        .toMap

      val meta = simple.values.head.send(new TopicMetadataRequest(topics.toSeq, 236))

      def toByteSeq(bb: ByteBuffer): Seq[Byte] = {
        val bytes = Array.ofDim[Byte](bb.remaining())
        bb.duplicate().get(bytes)
        bytes.toSeq
      }

      val outputMessages = meta.topicsMetadata
        .map { topic =>
          val messages = topic.partitionsMetadata
            .flatMap { partition =>
              val broker = partition.leader.get.port

              val consumer = simple(broker)

              val tp = TopicAndPartition(topic.topic, partition.partitionId)

              val offset = consumer.earliestOrLatestOffset(tp, OffsetRequest.LatestTime, 153)

              val response = consumer.fetch(new FetchRequest(
                correlationId = Random.nextInt(),
                clientId = ConsumerConfig.DefaultClientId,
                maxWait = ConsumerConfig.MaxFetchWaitMs,
                minBytes = 0,
                requestInfo = Map(
                  tp -> PartitionFetchInfo(0L, Int.MaxValue)
                )
              ))

              response.data(tp).messages.toSeq
                .map { mao => toByteSeq(mao.message.key) -> toByteSeq(mao.message.payload) }
            }

          topic.topic -> messages.groupBy { _._1 }.mapValues { _.unzip._2 }
        }
        .toMap

      Messages(outputMessages)

    } finally {

      if (simple != null) {
        simple.values.foreach { _.close() }
      }
    }
  }
}
