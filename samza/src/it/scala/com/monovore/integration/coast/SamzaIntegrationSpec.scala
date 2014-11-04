package com.monovore.integration.coast

import com.monovore.coast
import com.monovore.coast.format.WireFormat
import kafka.common.TopicAndPartition
import kafka.consumer.{Consumer, ConsumerConfig, ConsumerConnector, SimpleConsumer}
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.{TestUtils, TestZKUtils}
import kafka.zk.EmbeddedZookeeper
import org.apache.samza.job.local.LocalJobFactory
import org.apache.samza.serializers.CheckpointSerde
import org.apache.samza.system.SystemStream
import org.specs2.ScalaCheck
import org.specs2.mutable._

import scala.annotation.tailrec
import scala.collection.JavaConverters._

class SamzaIntegrationSpec extends Specification with ScalaCheck {

  sequential // I'll run out of ports otherwise

  "a running samza-based job" should {

    import coast.format.pretty._

    val Foo = coast.Name[String, String]("foo")

    val Bar = coast.Name[String, String]("bar")

    "pass through data safely" in {

      val flow = coast.sink(Bar) { coast.source(Foo) }

      val inputData = Map("bar" -> Seq("baz"))

      val input = Messages().add(Foo, inputData)

      val output = IntegrationTest.fuzz(flow, input)

      output.get(Bar) must_== inputData
    }
  }
}

case class Messages(messages: Map[String, Map[Seq[Byte], Seq[Seq[Byte]]]] = Map.empty) {

  def add[A : WireFormat, B : WireFormat](name: coast.Name[A,B], messages: Map[A, Seq[B]]): Messages = {

    val formatted = messages.map { case (k, vs) =>
      WireFormat.write(k).toSeq -> vs.map { v => WireFormat.write(v).toSeq }
    }

    Messages(this.messages.updated(name.name, formatted))
  }

  def get[A : WireFormat, B : WireFormat](name: coast.Name[A, B]): Map[A, Seq[B]] = {

    val data = messages.getOrElse(name.name, Map.empty)

    data.map { case (k, vs) =>
      WireFormat.read[A](k.toArray) -> vs.map { v => WireFormat.read[B](v.toArray) }
    }
  }
}

object Messages extends Messages(Map.empty)

object IntegrationTest {

  def fuzz(flow: coast.Flow[Unit], input: Messages): Messages = {

    val factory = new LocalJobFactory

    val Seq(port0) = TestUtils.choosePorts(1)

    val broker0 = TestUtils.createBrokerConfig(0, port0)
    broker0.setProperty("auto.create.topics.enable", "true")

    val config = new java.util.Properties()

    val brokers = s"localhost:$port0"
    val zkString = TestZKUtils.zookeeperConnect

    config.put("metadata.broker.list", brokers)
    config.put("producer.type", "sync")
    config.put("request.required.acks", "1")

    config.put("zookeeper.connect", zkString)
    config.put("group.id", "radical")
    config.put("auto.offset.reset", "smallest")

    val producerConfig = new ProducerConfig(config)

    var zookeeper: EmbeddedZookeeper = null
    var server0: KafkaServer = null
    var producer: Producer[Array[Byte], Array[Byte]] = null
    var consumer: ConsumerConnector = null
    var simple: SimpleConsumer = null

    try {
      zookeeper = new EmbeddedZookeeper(zkString)

      server0 = TestUtils.createServer(new KafkaConfig(broker0))

      producer = new Producer(producerConfig)

      for {
        (name, messages) <- input.messages
        (key, values) <- messages
        value <- values
      } {
        producer.send(new KeyedMessage(name, key.toArray, value.toArray))
      }

      val configs = coast.samza.configureFlow(flow)(
        baseConfig = coast.samza.config(
          "task.checkpoint.replication.factor" -> "1",
          "systems.kafka.samza.offset.default" -> "oldest",
          "systems.kafka.samza.factory" -> "org.apache.samza.system.kafka.KafkaSystemFactory",
          "systems.kafka.consumer.zookeeper.connect" -> zkString,
          "systems.kafka.producer.metadata.broker.list" -> brokers,
          "systems.kafka.producer.producer.type" -> "sync"
        )
      )

      val jobs = configs.values.toSeq
        .map { config => factory.getJob(config) }

      jobs.foreach { _.submit() }

      consumer = Consumer.create(new ConsumerConfig(config))

      val checkpointStreams = flow.bindings.map { case (name, _) => s"__samza_checkpoint_${name}_1" }

      val outputStreams = flow.bindings.map { case (name, _) => name }

      val streams = consumer.createMessageStreams(
        (checkpointStreams ++ outputStreams).map { _ -> 1 }.toMap
      )

      val inputSize = input.messages.mapValues { _.values.map { _.size }.sum }

      flow.bindings.map { case (name, _) =>

        val cp = s"__samza_checkpoint_${name}_1"

        val List(stream) = streams(cp)
        val config = configs(name)

        val inputs = config.get("task.inputs").split(",").toSeq
          .map { _.split("\\.").toSeq }
          .map { case Seq(system, stream) => new SystemStream(system, stream) }

        val iterator = stream.iterator()

        val serde = new CheckpointSerde

        def poll(): Unit = {
          val next = iterator.next() // Blocking!

          val checkpoint = serde.fromBytes(next.message())

          val finished = inputs
            .forall { ss =>
              val offset = Option(checkpoint.getOffsets.get(ss)).map { _.toLong + 1 }.getOrElse(0L)
              offset >= inputSize.getOrElse(ss.getStream, 0)
            }

          if (finished) ()
          else poll()
        }

        poll()
      }

      jobs.foreach { _.kill() }

      simple = new SimpleConsumer("localhost", port0, ConsumerConfig.SocketTimeout, ConsumerConfig.SocketBufferSize, ConsumerConfig.DefaultClientId)

      val offsets = outputStreams
        .map { case name =>
          name -> simple.earliestOrLatestOffset(TopicAndPartition(name, 0), -1L, 77)
        }
        .toMap

      val outputMessages =
        outputStreams
          .map { case name =>

            val List(stream) = streams(name)

            name -> stream.take(offsets(name).toInt)
              .map { msg => msg.key().toSeq -> msg.message().toSeq }
              .toList
              .groupBy { _._1 }.mapValues { _.map { _._2 } }
          }
          .toMap

      Messages(outputMessages)
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

      if (server0 != null) {
        server0.shutdown
        server0.awaitShutdown()
      }

      if (zookeeper != null) {
        zookeeper.shutdown()
      }
    }
  }
}