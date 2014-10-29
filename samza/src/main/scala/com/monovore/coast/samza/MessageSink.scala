package com.monovore.coast
package samza

import com.google.common.base.Charsets
import org.apache.samza.config.Config
import org.apache.samza.storage.kv.KeyValueStore
import org.apache.samza.task.TaskContext

trait MessageSink[-K, -V] extends Serializable {

  def execute(stream: String, key: K, value: V): Unit

}

object MessageSink {

  type Bytes = Array[Byte]
  type ByteSink = MessageSink[Array[Byte], Array[Byte]]

  trait Factory extends Serializable {
    def make(config: Config, context: TaskContext, sink: ByteSink): ByteSink
  }

  class FromElement[A, B](sink: Sink[A, B]) extends Factory {

    override def make(config: Config, context: TaskContext, finalSink: ByteSink) = {

      def compileSource[A, B](source: Source[A, B], sink: MessageSink[A, B], prefix: List[String]) = {

        new MessageSink[Bytes, Bytes] {

          override def execute(stream: String, key: Bytes, value: Bytes): Unit = {

            val a = source.keyFormat.read(key)
            val b = source.valueFormat.read(value)

            if (stream == source.source) sink.execute(stream, a, b)
          }
        }
      }

      def compileAggregate[S, A, B, B0](trans: Aggregate[S, A, B0, B], sink: MessageSink[A, B], prefix: List[String]) = {

        val store = context.getStore(Samza.formatPath(prefix)).asInstanceOf[KeyValueStore[A, S]]

        val transformed = new MessageSink[A, B0] {

          override def execute(stream: String, key: A, value: B0): Unit = {

            val update = trans.transformer(key)

            val state = Option(store.get(key))
              .getOrElse(trans.init)

            val (newState, output) = update(state, value)
            store.put(key, newState)
            output.foreach(sink.execute(stream, key, _))
          }
        }

        compile(trans.upstream, transformed, "aggregated" :: prefix)
      }

      def compilePure[A, B, B0](trans: PureTransform[A, B0, B], sink: MessageSink[A, B], prefix: List[String]) = {

        val transformed = new MessageSink[A, B0] {

          override def execute(stream: String, key: A, value: B0): Unit = {
            val update = trans.function(key)
            val output = update(value)
            output.foreach(sink.execute(stream, key, _))
          }
        }

        compile(trans.upstream, transformed, prefix)
      }

      def compileGroupBy[A, B, A0](gb: GroupBy[A, B, A0], sink: MessageSink[A, B], prefix: List[String]) = {

        val task = new MessageSink[A0, B] {
          override def execute(stream: String, key: A0, value: B): Unit = {
            val newKey = gb.groupBy(value)
            sink.execute(stream, newKey, value)
          }
        }

        compile(gb.upstream, task, prefix)
      }

      def compileMerge[A, B](merge: Merge[A, B], sink: MessageSink[A, B], prefix: List[String]) = {

        val upstreamSinks = merge.upstreams.zipWithIndex
          .map { case (up, i) => compile(up, sink, s"merged-$i" :: prefix) }

        new MessageSink[Bytes, Bytes] {

          override def execute(stream: String, key: Bytes, value: Bytes): Unit = {

            upstreamSinks.foreach { _.execute(stream, key, value) }
          }
        }
      }

      def compile[A, B](ent: Element[A, B], sink: MessageSink[A, B], prefix: List[String]): ByteSink = {

        ent match {
          case source @ Source(_) => compileSource(source, sink, prefix)
          case merge @ Merge(_) => compileMerge(merge, sink, prefix)
          case trans @ Aggregate(_, _, _) => compileAggregate(trans, sink, prefix)
          case pure @ PureTransform(_, _) => compilePure(pure, sink, prefix)
          case group @ GroupBy(_, _) => compileGroupBy(group, sink, prefix)
        }
      }

      val last = new MessageSink[A, B] {

        override def execute(stream: String, key: A, value: B): Unit = {

          val keyBytes = sink.keyFormat.write(key)
          val valueBytes = sink.valueFormat.write(value)

          finalSink.execute(stream, keyBytes, valueBytes)
        }
      }

      val name = config.get(Samza.TaskName)

      compile(sink.element, last, List(name))
    }
  }
}