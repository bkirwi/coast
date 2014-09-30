package com.monovore.coast
package samza

import java.io.{ObjectOutputStream, ByteArrayOutputStream}

import com.google.common.io.BaseEncoding
import com.monovore.coast.samza.SamzaTasklet.Message
import org.apache.samza.config.{Config, MapConfig}
import org.apache.samza.job.local.LocalJobFactory

import scala.collection.JavaConverters._

object Samza {

  val Encoding = BaseEncoding.base64Url.omitPadding

  def compile[A, B](ent: Element[A, B]): (Seq[String], SamzaTasklet) = ent match {
    case Source(name) => Seq(name) -> new SamzaTasklet {
      override def execute(message: Message): Seq[Message] = Seq.empty
    }
  }

  val KafkaBrokers = "192.168.80.20:9092"
  val ZookeeperHosts = "192.168.80.20:2181"

  def toConfig(graph: Flow[_]): Seq[Config] = {

    val configs = graph.bindings.map { case (name -> element) =>

      val (inputs, thing) = compile(element)

      val textThing = {
        val baos = new ByteArrayOutputStream()
        val oos = new ObjectOutputStream(baos)
        oos.writeObject(thing)
        oos.close()
        Encoding.encode(baos.toByteArray)
      }

      val configMap = Map(

        // Job
        "job.factory.class" -> "samza.job.local.LocalJobFactory",
        "job.name" -> name,

        // Task
        "task.class" -> "com.monovore.coast.samza.CoastTask",
        "task.inputs" -> inputs.map { i => s"kafka.$i" }.mkString(","),

        // Systems
        "systems.kafka.samza.factory" -> "org.apache.samza.system.kafka.KafkaSystemFactory",
        "systems.kafka.consumer.zookeeper.connect" -> ZookeeperHosts,
        "systems.kafka.producer.metadata.broker.list" -> KafkaBrokers,

        "coast.task.serialized.base64" -> textThing
      )

      new MapConfig(configMap.asJava)
    }

    configs.toSeq
  }

  def main(args: Array[String]): Unit = {

    val Metrics = Name[String, String]("metrics")

    val graph = for {

      _ <- Graph.label("bitches") {
        Graph.source(Metrics)
      }

    } yield ()

    val factory = new LocalJobFactory

    val jobs = toConfig(graph)
      .map { config =>
        println(config)
        factory.getJob(config)
      }

    jobs.foreach { _.submit() }

    println("cool!")
  }
}
