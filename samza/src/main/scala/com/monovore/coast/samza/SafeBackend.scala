package com.monovore.coast
package samza

import model._
import safe._

import com.google.common.base.Charsets
import org.apache.samza.Partition
import org.apache.samza.storage.kv.inmemory.InMemoryKeyValueStorageEngineFactory
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.util.Logging
import org.apache.samza.config.{TaskConfig, JobConfig, Config, MapConfig}
import org.apache.samza.storage.kv.KeyValueStore
import org.apache.samza.task.TaskContext

import collection.JavaConverters._
import reflect.ClassTag

object SafeBackend extends SamzaBackend {

  def apply(baseConfig: Config = new MapConfig()): ConfigGenerator = new SafeConfigGenerator(baseConfig)

  class SinkFactory[A, B](mergeStream: String, checkpointStream: String, sinkNode: Sink[A, B]) extends CoastTask.Factory with Logging {

    override def make(config: Config, context: TaskContext, whatSink: CoastTask.Receiver): CoastTask.Receiver = {

      val streamName = config.get(samza.TaskName)

      val partitionIndex = context.getTaskName.getTaskName.split("\\W+").last.toInt // ICK!

      info(s"Initializing safe coast backend for task [$streamName/$partitionIndex]")

      val regroupedStreams = config.get(RegroupedStreams).split(",")
        .filter { _.nonEmpty }
        .toSet

      val partitions = SamzaBackend.getPartitions(config, CoastSystem, streamName)

      val offsetThreshold =
        if (regroupedStreams(streamName)) 0L
        else {
          val offset = partitions(partitionIndex)
          info(s"Downstream offset of $offset for [$streamName/$partitionIndex]")
          offset
        }

      val finalSink = (offset: Long, key: A, value: B) => {

        val keyBytes = sinkNode.keyFormat.write(key)
        val valueBytes = sinkNode.valueFormat.write(value)

        if (offset >= offsetThreshold) {

          if (regroupedStreams(streamName)) {
            val qualifier = partitionIndex.toString.getBytes(Charsets.UTF_8)
            val payload = Messages.InternalMessage.binaryFormat.write(qualifier, offset, valueBytes)
            val newPartition = sinkNode.keyPartitioner.partition(key, partitions.size)
            whatSink.send(streamName, newPartition, -1, keyBytes, payload)
          }
          else {
            whatSink.send(streamName, partitionIndex, offset, keyBytes, valueBytes)
          }
        }

        offset + 1
      }

      val compiler = new TaskCompiler(new TaskCompiler.Context {
        override def getStore[A, B](path: String, default: B): CoastState[A, B] =
          context.getStore(path).asInstanceOf[CoastStorageEngine[A, B]].withDefault(default)
      })

      val compiled = compiler.compile(sinkNode.element, finalSink, streamName)

      val checkpointKey = s"$streamName.checkpoint"
      val checkpointStore = context.getStore(checkpointKey).asInstanceOf[KeyValueStore[Unit, Checkpoint]]

      var checkpoint: Checkpoint = Option(checkpointStore.get(unit)).getOrElse(Checkpoint(Map.empty, 0L, Map.empty))

      info(s"Restoring task [$streamName/$partitionIndex] to checkpoint: $checkpoint")

      checkpoint.inputStreams
        .foreach { case ((name, p), state) =>
          context.setStartingOffset(new SystemStreamPartition(CoastSystem, name, new Partition(p)), state.offset.toString)
        }

      context.setStartingOffset(new SystemStreamPartition(CoastSystem, mergeStream, new Partition(partitionIndex)), checkpoint.mergeOffset.toString)

      var mergeTip = checkpoint.mergeOffset

      new CoastTask.Receiver {

        import Checkpoint._

        override def send(stream: String, partition: Int, offset: Long, key: Array[Byte], value: Array[Byte]) {

          if (stream == mergeStream) {
            mergeTip = math.max(mergeTip, offset + 1)
          }
          else {

            val state = checkpoint.inputStreams.getOrElse(stream -> partition, InputState.default)

            if (offset >= state.offset) {

              if (mergeTip <= checkpoint.mergeOffset) {

                val mergeMessage = Messages.MergeInfo.binaryFormat.write(stream, partition, offset)

                whatSink.send(mergeStream, partitionIndex, checkpoint.mergeOffset, Array.empty, mergeMessage)
              }

              val (qualifier, qualifierOffset, message) =
                if (regroupedStreams(stream)) Messages.InternalMessage.binaryFormat.read(value)
                else (Array.empty[Byte], offset, value)

              val qualifierThreshold = state.qualifiers.getOrElse(qualifier, 0L)

              checkpoint =
                if (qualifierOffset >= qualifierThreshold) {

                  val result = compiled
                    .filter { _.inputStream == stream }
                    .foldLeft(checkpoint.outputStreams) { (current, dispatch) =>
                      val offset = current.getOrElse(dispatch.downstreamPath, 0L)
                      val nextOffset = dispatch.handler(offset, key, message)
                      current.updated(dispatch.downstreamPath, nextOffset)
                    }

                  checkpoint.copy(
                    inputStreams = checkpoint.inputStreams.updated(stream -> partition,
                      state.copy(
                        offset = offset + 1,
                        qualifiers = state.qualifiers.updated(qualifier, qualifierOffset + 1)
                      )
                    ),
                    mergeOffset = checkpoint.mergeOffset + 1,
                    outputStreams = result
                  )
                }
                else {
                  checkpoint.copy(
                    inputStreams = checkpoint.inputStreams.updated(stream -> partition,
                      state.copy(offset = offset + 1)
                    ),
                    mergeOffset = checkpoint.mergeOffset + 1
                  )
                }
            }
          }
        }

        override def window(): Unit = {
          debug(s"Checkpointing task [$streamName/$partitionIndex] at: $checkpoint")
          checkpointStore.put(unit, checkpoint)
        }
      }
    }
  }
}

class SafeConfigGenerator(baseConfig: Config = new MapConfig()) extends ConfigGenerator {

  import ConfigGenerator._
  
  val base = SamzaConfig.Base(baseConfig)

  def configure(graph: Graph): Map[String, Config] = {

    val baseConfigMap = baseConfig.asScala.toMap

    val regrouped = graph.bindings
      .flatMap { case (name, sink) =>
        Some(name).filter { _ => isRegrouped(sink.element) }
      }
      .toSet

    val configs = graph.bindings.map { case (name -> sink) =>

      val mergeStream = s"${base.mergePrefix}.$name"
      val checkpoint = s"$name.checkpoint"
      val checkpointStream = s"${base.checkpointPrefix}.$name"

      val inputs = sourcesFor(sink.element)

      val statePaths = storageFor(sink.element, Path(name))

      val changelogDelays = statePaths
        .map { case (path, storage) => base.changelogStream(storage.name) -> (path.branches.size + 2) }

      val delays = changelogDelays ++ Map(
        mergeStream -> 0,
        name -> 1,
        checkpointStream -> ((1 +: changelogDelays.values.toSeq).max + 1)
      )

      def className[A](implicit tag: ClassTag[A]): String = tag.runtimeClass.getName

      val factory: CoastTask.Factory = new SafeBackend.SinkFactory(mergeStream, checkpointStream, sink)

      val configMap = Map(

        // Job
        JobConfig.JOB_NAME -> name,

        // Task
        TaskConfig.TASK_CLASS -> className[CoastTask],
        TaskConfig.INPUT_STREAMS -> (inputs + mergeStream).map { i => s"$CoastSystem.$i" }.mkString(","),
        TaskConfig.MESSAGE_CHOOSER_CLASS_NAME -> className[MergingChooserFactory],

        // No-op checkpoints!
        "task.checkpoint.factory" -> className[NoopCheckpointManagerFactory],

        // Kafka system
        s"systems.$CoastSystem.samza.offset.default" -> "oldest",
        s"systems.$CoastSystem.producer.producer.type" -> "sync",
        s"systems.$CoastSystem.producer.message.send.max.retries" -> "0",
        s"systems.$CoastSystem.producer.request.required.acks" -> "1",
        s"systems.$CoastSystem.samza.factory" -> className[CoastKafkaSystemFactory],

        // Merge info
        s"systems.$CoastSystem.streams.$mergeStream.merge" -> inputs.map { i => s"$CoastSystem.$i" }.mkString(","),
        s"systems.$CoastSystem.streams.$mergeStream.samza.bootstrap" -> "true",
        s"systems.$CoastSystem.streams.$mergeStream.samza.priority" -> "0",

        // Coast-specific
        TaskKey -> SerializationUtil.toBase64(factory),
        TaskName -> name,
        RegroupedStreams -> regrouped.mkString(",")
      )


      val storageMap = statePaths
        .flatMap { case (path, storage) =>

          base.storageConfig(storage) ++ Map(
            s"stores.${storage.name}.changelog" -> s"${base.system}.${base.changelogStream(storage.name)}",
            s"stores.${storage.name}.factory" -> className[CoastStoreFactory[_, _]],
            s"stores.${storage.name}.subfactory" -> className[InMemoryKeyValueStorageEngineFactory[_, _]]
          )
        }
        .toMap

      val streamConfig = delays
        .map { case (stream, delay) =>
          s"systems.${base.system}.streams.$stream.delay" -> delay.toString
        }

      val checkpointConf = {

        base.storageConfig(Storage(checkpoint, Checkpoint.keyFormat, Checkpoint.format)) ++ Map(
          s"stores.$checkpoint.changelog" -> s"${base.system}.$checkpointStream",
          s"stores.$checkpoint.factory" -> "org.apache.samza.storage.kv.inmemory.InMemoryKeyValueStorageEngineFactory"
        )
      }

      name -> new MapConfig(
        (baseConfigMap ++ configMap ++ storageMap ++ streamConfig ++ checkpointConf).asJava
      )
    }

    configs.toMap
  }
}