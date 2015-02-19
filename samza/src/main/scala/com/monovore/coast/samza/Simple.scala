package com.monovore.coast
package samza

import com.monovore.coast.model._
import com.monovore.coast.samza.MessageSink.ByteSink
import org.apache.samza.Partition
import org.apache.samza.config.{Config, MapConfig}
import org.apache.samza.system.SystemFactory
import org.apache.samza.task.TaskContext

import scala.collection.JavaConverters._

object Simple extends (Config => ConfigGenerator) {

  def apply(baseConfig: Config): ConfigGenerator = new ConfigGenerator {

    import com.monovore.coast.samza.ConfigGenerator._

    def storageFor[A, B](element: Node[A, B], path: Path): Seq[Storage] = element match {
      case Source(_) => Seq()
      case PureTransform(up, _) => storageFor(up, path)
      case Merge(ups) => {
        ups.flatMap { case (branch, up) => storageFor(up, path / branch)}
      }
      case agg @ StatefulTransform(up, _, _) => {
        val upstreamed = storageFor(up, path.next)
        upstreamed :+ Storage(
          name = path,
          keyString = SerializationUtil.toBase64(agg.keyFormat),
          valueString = SerializationUtil.toBase64(agg.stateFormat)
        )
      }
      case GroupBy(up, _) => storageFor(up, path)
    }

    override def configure(graph: Graph): Map[String, Config] = {

      val baseConfigMap = baseConfig.asScala.toMap

      val configs = graph.bindings.map { case (name -> sink) =>

        val inputs = sourcesFor(sink.element).map { i => s"$CoastSystem.$i" }

        val storage = storageFor(sink.element, Path(name))

        val factory: MessageSink.Factory = new Simple.SinkFactory(sink)

        val configMap = Map(

          // Job
          "job.name" -> name,

          // Task
          "task.class" -> "com.monovore.coast.samza.CoastTask",
          "task.inputs" -> inputs.mkString(","),

          // TODO: checkpoints should be configurable
          "task.checkpoint.factory" -> "org.apache.samza.checkpoint.kafka.KafkaCheckpointManagerFactory",
          "task.checkpoint.system" -> "coast-system",

          // Kafka system
          s"systems.$CoastSystem.samza.offset.default" -> "oldest",

          // Coast-specific
          TaskKey -> SerializationUtil.toBase64(factory),
          TaskName -> name
        )

        val storageMap = storage
          .map { case Storage(name, keyFormat, msgFormat) =>

            val keyName = s"coast-key-$name"
            val msgName = s"coast-msg-$name"

            Map(
              s"stores.$name.factory" -> "com.monovore.coast.samza.CoastStoreFactory",
              s"stores.$name.subfactory" -> "org.apache.samza.storage.kv.inmemory.InMemoryKeyValueStorageEngineFactory",
              s"stores.$name.key.serde" -> keyName,
              s"stores.$name.msg.serde" -> msgName,
              s"stores.$name.changelog" -> s"$CoastSystem.coast.changelog.$name",
              s"stores.$name.coast.simple" -> "true",
              s"serializers.registry.$keyName.class" -> "com.monovore.coast.samza.CoastSerdeFactory",
              s"serializers.registry.$keyName.serialized.base64" -> keyFormat,
              s"serializers.registry.$msgName.class" -> "com.monovore.coast.samza.CoastSerdeFactory",
              s"serializers.registry.$msgName.serialized.base64" -> msgFormat
            )
          }
          .flatten.toMap

        name -> new MapConfig(
          (baseConfigMap ++ configMap ++ storageMap).asJava
        )
      }

      configs.toMap
    }
  }

  class SinkFactory[A, B](sink: Sink[A, B]) extends MessageSink.Factory {

    override def make(config: Config, context: TaskContext, messageSink: ByteSink): ByteSink = {

      val taskName = config.get(samza.TaskName)

      // Left(size) if the stream is regrouped, Right(offset) if it isn't
      val numPartitions = {

        val systemFactory = config.getNewInstance[SystemFactory](s"systems.$CoastSystem.samza.factory")
        val admin = systemFactory.getAdmin(CoastSystem, config)
        val meta = admin.getSystemStreamMetadata(Set(taskName).asJava).asScala
          .getOrElse(taskName, sys.error(s"Couldn't find metadata on output stream $taskName"))

        val partitionMeta = meta.getSystemStreamPartitionMetadata.asScala

        partitionMeta.size
      }

      val compiler = new TaskCompiler(new TaskCompiler.Context {

        override def getSource(path: String): CoastState[Int, Unit, Unit] = new CoastState[Int, Unit, Unit] {
          var _downstream: Long = 0L
          override def upstream(partition: Int): Long = 0L
          override def downstream: Long = _downstream
          override def state(key: Unit): Unit = ()
          override def push(partition: Int, key: Unit, value: Unit, upstream: Long, downstream: Long): Unit = {
            _downstream = downstream
          }
        }

        override def getStore[P, A, B](path: String, default: B): CoastState[Int, A, B] =
          context.getStore(path).asInstanceOf[CoastStorageEngine[A, B]].withDefault(default)
      })

      compiler.compileSink(sink, messageSink, taskName, numPartitions)
    }
  }
}
