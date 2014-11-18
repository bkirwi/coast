package com.monovore.integration.coast

import java.util.Properties

import com.monovore.coast
import kafka.api.{TopicMetadataRequest, OffsetRequest}
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
    var consumer: ConsumerConnector = null
    var simple: SimpleConsumer = null

    IntegrationTest.withKafkaCluster { config =>

      val producerConfig = new ProducerConfig(config)

      try {

        IntegrationTest.slurp(input.messages.keySet, config)

        producer = new Producer(producerConfig)

        for {
          (name, messages) <- input.messages
          (key, values) <- messages
          value <- values.grouped(1000)
        } {
          producer.send(value.map { value => new KeyedMessage(name, key.toArray, value.toArray)}: _*)
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

        val sleeps = (0 until 4).map { _ => Random.nextInt(500) + 800} ++ Seq(8000)

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
            def wait() {
              job.waitForFinish(50) match {
                case ApplicationStatus.SuccessfulFinish => ()
                case ApplicationStatus.UnsuccessfulFinish => ()
                case _ => wait()
              }
            }
            wait()
          }
        }

        // TODO: why doesn't this work?

        //      val inputSize = input.messages.mapValues { _.values.map { _.size }.sum }
        //
        //      flow.bindings.map { case (name, _) =>
        //
        //        val cp = s"__samza_checkpoint_${name}_1"
        //
        //        val List(stream) = streams(cp)
        //        val config = configs(name)
        //
        //        val inputs = config.get("task.inputs").split(",").toSeq
        //          .map { _.split("\\.").toSeq }
        //          .map { case Seq(system, stream) => new SystemStream(system, stream) }
        //
        //        val iterator = stream.iterator()
        //
        //        val serde = new CheckpointSerde
        //
        //        def poll(): Unit = {
        //          val next = iterator.next() // Blocking!
        //
        //          val checkpoint = serde.fromBytes(next.message())
        //
        //          println(checkpoint, inputs)
        //
        //          val finished = inputs
        //            .forall { ss =>
        //              val offset = Option(checkpoint.getOffsets.get(ss)).map { _.toLong + 1 }.getOrElse(0L)
        //              offset >= inputSize.getOrElse(ss.getStream, 0)
        //            }
        //
        //          if (finished) ()
        //          else poll()
        //        }
        //
        //        poll()
        //      }

//        val checkpointStreams = flow.bindings.map { case (name, _) => s"__samza_checkpoint_${name}_1"}

        val outputStreams = flow.bindings.map { case (name, _) => name}

        IntegrationTest.slurp(outputStreams.toSet, config)

      } finally {

        if (simple != null) {
          simple.close()
        }

        if (producer != null) {
          producer.close()
        }

        if (consumer != null) {
          consumer.shutdown()
        }
      }
    }
  }

  def slurp(topics: Set[String], config: Properties): Messages = {

    var consumer: ConsumerConnector = null
    var simple: SimpleConsumer = null

    try {

      val port0 = config.getProperty("metadata.broker.list").split(":")(1).toInt

      simple = new SimpleConsumer("localhost", port0, ConsumerConfig.SocketTimeout, ConsumerConfig.SocketBufferSize, ConsumerConfig.DefaultClientId)

      val offsets = topics
        .map { case name =>

          def thing(): Long = try {
            simple.earliestOrLatestOffset(TopicAndPartition(name, 0), OffsetRequest.LatestTime, 153)
          } catch {
            case e: UnknownTopicOrPartitionException => {
              simple.send(new TopicMetadataRequest(Seq(name), 29))
              thing()
            }
          }

          name -> thing()
        }
        .toMap

      consumer = Consumer.create(new ConsumerConfig(config))

      val streams = consumer.createMessageStreams(
        topics.map { _ -> 1 }.toMap
      )

      val outputMessages = topics
        .map { case name =>

          val List(stream) = streams(name)

          name -> stream.take(offsets(name).toInt)
            .map { msg => msg.key().toSeq -> msg.message().toSeq}
            .groupBy { _._1 }
            .mapValues { _.map { _._2 }.toVector
          }
        }
        .toMap

      Messages(outputMessages)

    } finally {

      if (simple != null) {
        simple.close()
      }

      if (consumer != null) {
        consumer.shutdown()
      }
    }
  }
}
