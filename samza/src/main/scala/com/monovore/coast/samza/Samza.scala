package com.monovore.coast
package samza

import java.io.{ObjectOutputStream, ByteArrayOutputStream}

import com.google.common.io.BaseEncoding
import com.monovore.coast.samza.SamzaTasklet.Message
import org.apache.samza.config.{Config, MapConfig}

import scala.collection.JavaConverters._

object Samza {

  val Encoding = BaseEncoding.base64Url.omitPadding

  def toConfig(graph: Graph[_]): Seq[Config] = {

    val configs = graph.state.map { case (name -> element) =>

      val (inputs, thing) = Seq.empty -> new SamzaTasklet {
        override def execute(message: Message): Seq[Message] = Seq.empty
      }

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

        "coast.task.serialized.base64" -> textThing
      )

      new MapConfig(configMap.asJava)
    }

    configs.toSeq
  }
}
