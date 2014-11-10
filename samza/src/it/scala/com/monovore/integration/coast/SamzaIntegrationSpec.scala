package com.monovore.integration.coast

import com.monovore.coast
import com.monovore.coast.format.WireFormat
import kafka.api.OffsetRequest
import kafka.common.TopicAndPartition
import kafka.consumer.{Consumer, ConsumerConfig, ConsumerConnector, SimpleConsumer}
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.{TestUtils, TestZKUtils}
import kafka.zk.EmbeddedZookeeper
import org.apache.samza.job.ApplicationStatus
import org.apache.samza.job.local.ThreadJobFactory
import org.apache.samza.serializers.CheckpointSerde
import org.apache.samza.system.SystemStream
import org.specs2.ScalaCheck
import org.specs2.mutable._

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.util.Random

class SamzaIntegrationSpec extends Specification with ScalaCheck {

  // unfortunately, a lot of issues show up only with particular timing
  // this helps make results more reproducible
  sequential

  val BigNumber = 85000 // pretty big?

  "a running samza-based job" should {

    import coast.format.pretty._

    val Foo = coast.Name[String, Int]("foo")

    val Bar = coast.Name[String, Int]("bar")

    "pass through data safely" in {

      val flow = coast.sink(Bar) {
        coast.source(Foo)
      }

      val inputData = Map("bar" -> (1 to BigNumber))

      val input = Messages().add(Foo, inputData)

      val output = IntegrationTest.fuzz(flow, input).get(Bar)

      // collections are very large, so fail fast!
      output("bar").size must_== inputData("bar").size

      output("bar") must_== inputData("bar")
    }

    "flatMap nicely" in {

      val flow = coast.sink(Bar) {
        coast.source(Foo).flatMap { n => Seq.fill(3)(n) }
      }

      val inputData = Map("bar" -> (1 to BigNumber))

      val input = Messages().add(Foo, inputData)

      val output = IntegrationTest.fuzz(flow, input).get(Bar)

      // collections are very large, so fail fast!
      output("bar").size must_== (inputData("bar").size * 3)

      output("bar") must_== inputData("bar").flatMap { n => Seq.fill(3)(n) }
    }

    "accumulate state" in {

      val flow = coast.sink(Bar) {
        coast.source(Foo).fold(0) { (n, _) => n + 1 }.stream
      }

      val inputData = Map("bar" -> (1 to BigNumber))

      val input = Messages().add(Foo, inputData)

      val output = IntegrationTest.fuzz(flow, input).get(Bar)

      // collections are very large, so fail fast!
      output("bar").size must_== inputData("bar").size

      output("bar") must_== inputData("bar")
    }

    "compose well across multiple Samza jobs" in {

      val flow = for {

        once <- coast.label("testing") {
          coast.source(Foo).fold(1) { (n, _) => n + 1 }.stream
        }

        _ <- coast.sink(Bar) { once.map { _ + 1 } }

      } yield ()

      val inputData = Map("bar" -> (1 to BigNumber))

      val input = Messages().add(Foo, inputData)

      val output = IntegrationTest.fuzz(flow, input).get(Bar)

      // collections are very large, so fail fast!
      output("bar").size must_== inputData("bar").size

      output("bar") must_== inputData("bar").map { _ + 2 }
    }

//    "do a merge" in {
//
//      val Foo2 = coast.Name[String, Int]("foo-2")
//
//      val flow = coast.sink(Bar) { coast.merge(coast.source(Foo), coast.source(Foo2)) }
//
//      val input = Messages
//        .add(Foo, Map("test" -> (1 to BigNumber by 2)))
//        .add(Foo2, Map("test" -> (2 to BigNumber by 2)))
//
//      val output = IntegrationTest.fuzz(flow, input).get(Bar)
//
//      output("test").size must_== BigNumber
//
//      output("test").filter { _ % 2 == 1 } must_== (1 to BigNumber by 2)
//      output("test").filter { _ % 2 == 0 } must_== (2 to BigNumber by 2)
//    }
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
    }.withDefaultValue(Seq.empty[B])
  }
}

object Messages extends Messages(Map.empty)

object IntegrationTest {

  def fuzz(flow: coast.Flow[Unit], input: Messages): Messages = {

    val factory = new ThreadJobFactory

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
    config.put("group.id", "input-producer")
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
        value <- values.grouped(1000)
      } {
        producer.send(value.map { value => new KeyedMessage(name, key.toArray, value.toArray)}: _*)
      }

      val configs = coast.samza.configureFlow(flow)(
        baseConfig = coast.samza.config(
//          "task.commit.ms" -> "500",
          "task.window.ms" -> "300",
//          "task.chooser.class" -> "com.monovore.integration.coast.RandomMessageChooserFactory",
          "task.checkpoint.replication.factor" -> "1",
          "systems.kafka.samza.offset.default" -> "oldest",
          "systems.kafka.samza.factory" -> "org.apache.samza.system.kafka.KafkaSystemFactory",
          "systems.kafka.consumer.zookeeper.connect" -> zkString,
          "systems.kafka.producer.metadata.broker.list" -> brokers,
          "systems.kafka.producer.batch.num.messages" -> "10000",
          "systems.kafka.producer.request.required.acks" -> "1",
          "systems.kafka.producer.producer.type" -> "sync"
        )
      )

      // FLAIL!

      val sleeps = (1 to 3).map { _ => Random.nextInt(500) + 800 } ++ Seq(10000)

      for (sleepTime <- sleeps) {

        val jobs = configs.values.toSeq
          .map { config => factory.getJob(config) }

        jobs.foreach { _.submit() }

        Thread.sleep(sleepTime)

        jobs.foreach { _.kill() }

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

      val checkpointStreams = flow.bindings.map { case (name, _) => s"__samza_checkpoint_${name}_1" }

      val outputStreams = flow.bindings.map { case (name, _) => name }

      simple = new SimpleConsumer("localhost", port0, ConsumerConfig.SocketTimeout, ConsumerConfig.SocketBufferSize, ConsumerConfig.DefaultClientId)

      val offsets = outputStreams
        .map { case name =>
          name -> simple.earliestOrLatestOffset(TopicAndPartition(name, 0), OffsetRequest.LatestTime, 153)
        }
        .toMap

      consumer = Consumer.create(new ConsumerConfig(config))

      val streams = consumer.createMessageStreams(
        (checkpointStreams ++ outputStreams).map { _ -> 1 }.toMap
      )

      val outputMessages =
        outputStreams
          .map { case name =>

            val List(stream) = streams(name)

            name -> stream.take(offsets(name).toInt)
              .map { msg => msg.key().toSeq -> msg.message().toSeq }
              .groupBy { _._1 }.mapValues { _.map { _._2 }.toVector }
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