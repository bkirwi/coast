package com.monovore.coast.standalone

import com.monovore.coast.core.{PureTransform, Sink, Source, Node}
import com.monovore.coast.flow.{Topic, Flow}
import com.monovore.coast.wire.Protocol
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}
import org.apache.kafka.common.serialization.{ByteArraySerializer, ByteArrayDeserializer}

import scala.collection.JavaConverters._

trait StandaloneApp {

  def appName: String = { this.getClass.getSimpleName.stripSuffix("$") }

  implicit val builder = Flow.builder()

  def main(args: Array[String]): Unit = {

    println(appName)

    val flow = builder.toFlow

    println(flow.bindings)

    val consumer = new KafkaConsumer(
      Map[String, AnyRef](
        "group.id" -> s"coast-$appName",
        "bootstrap.servers" -> "localhost:9092"
      ).asJava,
      new ByteArrayDeserializer,
      new ByteArrayDeserializer
    )

    val producer = new KafkaProducer(
      Map[String, AnyRef](
        "bootstrap.servers" -> "localhost:9092"
      ).asJava,
      new ByteArraySerializer,
      new ByteArraySerializer
    )

    val topicSizes =
      consumer.listTopics().asScala.mapValues { _.size }

    println(topicSizes)

    val map = flow.bindings
      .map { case (name, sink) =>

        type Erp = Map[String, ConsumerRecord[Array[Byte], Array[Byte]] => Unit]
        def doSink[A, B](sink: Sink[A, B]): Erp = {

          def comp[A, B](node: Node[A, B], out: (A, B) => Unit): Erp = node match {
            case src: Source[A, B] => Map(
              src.source -> { record =>
                val key = Option(record.key()).map(src.keyFormat.fromArray(_)).getOrElse(record.partition().asInstanceOf[A])
                val value = src.valueFormat.fromArray(record.value())
                out(key, value)
              }
            )
            case pur: PureTransform[A, b0, B] => comp[A, b0](pur.upstream, { (a, b) =>
              pur.function(a)(b).foreach { b => println(b); out(a, b) }
            })
          }

          comp[A, B](sink.element, { (key, value) =>
            producer.send(new ProducerRecord(
              name,
              sink.keyPartitioner.partition(key, topicSizes(name)),
              sink.keyFormat.toArray(key),
              sink.valueFormat.toArray(value)
            ))
          })
        }

        doSink(sink)
      }
      .reduce { _ ++ _ }

    consumer.subscribe(map.keys.toSeq.asJava)

    while(true) {
      println("polling...")
      consumer.poll(1000L).asScala foreach { record =>

        map(record.topic())(record)
      }
    }
  }
}

object Demo extends StandaloneApp {

  import Protocol.common._

  val Sentences = Topic[Int, String]("sentences")
  val Words = Topic[Int, String]("words")

  Sentences.asSource
    .flatMap { _.split("\\s+") }
    .sinkTo(Words)
}