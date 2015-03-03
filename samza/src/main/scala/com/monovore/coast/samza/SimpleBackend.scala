package com.monovore.coast
package samza

import com.monovore.coast.model._
import com.twitter.algebird.Monoid
import org.apache.samza.config.{Config, JobConfig, MapConfig, TaskConfig}
import org.apache.samza.storage.kv.KeyValueStorageEngine
import org.apache.samza.task.TaskContext

import scala.collection.JavaConverters._

object SimpleBackend extends SamzaBackend {

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

        val factory: CoastTask.Factory = new SimpleBackend.SinkFactory(sink)

        val configMap = Map(

          // Job
          JobConfig.JOB_NAME -> name,

          // Task
          TaskConfig.TASK_CLASS -> "com.monovore.coast.samza.CoastTask",
          TaskConfig.INPUT_STREAMS -> inputs.mkString(","),

          // Kafka system
          s"systems.$CoastSystem.samza.offset.default" -> "oldest",

          // Coast-specific
          TaskKey -> SerializationUtil.toBase64(factory),
          TaskName -> name
        )

        val storageMap = storage
          .map { case storage @ Storage(name, keyFormat, msgFormat) =>

            storage.serdeConfig ++ Map(
              s"stores.$name.factory" -> "org.apache.samza.storage.kv.inmemory.InMemoryKeyValueStorageEngineFactory"
            )
          }
          .flatten.toMap

        val defaults = storage
          .map { storage =>
            ConfigGenerator.defaultsForStore(storage.path.toString, baseConfig)
          }
          .flatten.toMap

        name -> new MapConfig(
          (defaults ++ baseConfigMap ++ configMap ++ storageMap).asJava
        )
      }

      configs.toMap
    }
  }

  class SinkFactory[A, B](sink: Sink[A, B]) extends CoastTask.Factory {

    override def make(config: Config, context: TaskContext, receiver: CoastTask.Receiver): CoastTask.Receiver = {

      val outputStream = config.get(samza.TaskName)

      val numPartitions = SamzaBackend.getPartitions(config, CoastSystem, outputStream).size

      type Send[A, B] = (A, B) => Unit

      implicit def sendMonoid[A, B]: Monoid[Send[A, B]] = Monoid.from((_: A, _: B) => ()) { (left, right) =>
        { (a, b) => left(a, b); right(a, b) }
      }

      def compileNode[A, B](node: Node[A, B], path: Path, send: Send[A, B]): Map[String, Send[Array[Byte], Array[Byte]]] = node match {
        case src: Source[A, B] => Map(src.source -> { (key, value) =>
          send(src.keyFormat.read(key), src.valueFormat.read(value))
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
      }

      val finalSink: Send[A, B] = { (key, value) =>

        receiver.send(
          outputStream,
          sink.keyPartitioner.partition(key, numPartitions),
          -1,
          sink.keyFormat.write(key),
          sink.valueFormat.write(value)
        )
      }

      val compiled =
        compileNode(sink.element, Path(outputStream), finalSink)
          .withDefaultValue { (_: Array[Byte], _: Array[Byte]) => () }

      new CoastTask.Receiver {

        override def send(stream: String, partition: Int, offset: Long, key: Array[Byte], value: Array[Byte]) {
          compiled.apply(stream).apply(key, value)
        }
      }
    }
  }
}
