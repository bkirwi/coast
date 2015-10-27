package com.monovore.coast
package samza

import com.monovore.coast.core._
import com.twitter.algebird.Monoid
import org.apache.samza.config.{Config, JobConfig, MapConfig, TaskConfig}
import org.apache.samza.storage.kv.KeyValueStorageEngine
import org.apache.samza.storage.kv.inmemory.InMemoryKeyValueStorageEngineFactory
import org.apache.samza.system.SystemStream
import org.apache.samza.task.TaskContext

import scala.collection.JavaConverters._

object SimpleBackend extends SamzaBackend {

  def apply(baseConfig: Config): ConfigGenerator = new ConfigGenerator {

    import com.monovore.coast.samza.ConfigGenerator._
    import SamzaConfig.className

    val base = SamzaConfig.Base(baseConfig)

    override def configure(graph: Graph): Map[String, Config] = {

      val baseConfigMap = baseConfig.asScala.toMap

      val configs = graph.bindings.map { case (name -> sink) =>

        val inputs = sourcesFor(sink.element).map { i => s"${base.system}.$i" }

        val storage = storageFor(sink.element, Path(name))

        val factory: CoastTask.Factory = new SimpleBackend.SinkFactory(base.system, sink)

        val configMap = Map(

          // Job
          JobConfig.JOB_NAME -> name,

          // Task
          TaskConfig.TASK_CLASS -> className[CoastTask],
          TaskConfig.INPUT_STREAMS -> inputs.mkString(","),

          // System
          s"systems.${base.system}.samza.offset.default" -> "oldest",

          // Coast-specific
          SamzaConfig.TaskKey -> SerializationUtil.toBase64(factory),
          SamzaConfig.TaskName -> name
        )

        val storageMap = storage
          .flatMap { case (path, storage) => base.storageConfig(storage) }
          .toMap

        name -> new MapConfig(
          (baseConfigMap ++ configMap ++ storageMap).asJava
        )
      }

      configs.toMap
    }
  }

  class SinkFactory[A, B](system: String, sink: Sink[A, B]) extends CoastTask.Factory {

    override def make(config: Config, context: TaskContext, receiver: CoastTask.Receiver): CoastTask.Receiver = {

      val outputStream = config.get(SamzaConfig.TaskName)

      val numPartitions = SamzaBackend.getPartitions(config, system, outputStream).size

      type Send[A, B] = (A, B) => Unit

      def compileNode[A, B](node: Node[A, B], path: Path, send: Send[A, B]): Map[String, Send[Array[Byte], Array[Byte]]] = node match {
        case src: Source[A, B] => Map(src.source -> { (key, value) =>
          send(src.keyFormat.fromArray(key), src.valueFormat.fromArray(value))
        })
        case pure: PureTransform[A, b0, B] => {

          compileNode(pure.upstream, path, { (key: A, value: b0) =>
            pure.function(key)(value).foreach { newValue => send(key, newValue) }
          })
        }
        case stateful: StatefulTransform[s0, A, b0, B] => {

          val state = context.getStore(path.toString).asInstanceOf[KeyValueStorageEngine[A, s0]]

          compileNode(stateful.upstream, path.next, { (key: A, value: b0) =>

            val currentState = Option(state.get(key)).getOrElse(stateful.init)

            val (newState, newValues) = stateful.transformer(key)(currentState, value)

            newValues.foreach { newValue => send(key, newValue) }

            state.put(key, newState)
          })
        }
        case group: GroupBy[A, B, a0] => {

          compileNode(group.upstream, path, { (key: a0, value: B) =>
            send(group.groupBy(key)(value), value)
          })
        }
        case merge: Merge[A, B] => {
          val compiled = merge.upstreams.map { case (branch, node) => compileNode(node, path / branch, send) }
          Monoid.sum(compiled)
        }
        case clock: Clock => sys.error("Clocks not implemented yet!")
      }

      val finalSink: Send[A, B] = { (key, value) =>

        receiver.send(
          new SystemStream(system, outputStream),
          sink.keyPartitioner.partition(key, numPartitions),
          -1,
          sink.keyFormat.toArray(key),
          sink.valueFormat.toArray(value)
        )
      }

      val compiled =
        compileNode(sink.element, Path(outputStream), finalSink)
          .withDefaultValue { (_: Array[Byte], _: Array[Byte]) => () }

      new CoastTask.Receiver {

        override def send(systemStream: SystemStream, partition: Int, offset: Long, key: Array[Byte], value: Array[Byte]) {
          compiled.apply(systemStream.getStream).apply(key, value)
        }
      }
    }
  }
}
