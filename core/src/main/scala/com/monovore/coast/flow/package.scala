package com.monovore.coast

import com.monovore.coast.wire.{Partitioner, BinaryFormat}
import com.monovore.coast.model.{Source, Merge, Sink}

package object flow {

  type Stream[A, +B] = StreamDef[Grouped, A, B]

  type Pool[A, +B] = PoolDef[Grouped, A, B]

  def merge[G <: AnyGrouping, A, B](upstreams: (String -> StreamDef[G, A, B])*): StreamDef[G, A, B] = {

    for ((branch -> streams) <- upstreams.groupByKey) {
      require(streams.size == 1, s"merged branches must be unique ($branch is specified ${streams.size} times)")
    }

    new StreamDef[G, A, B](Merge(upstreams.map { case (name, stream) => name -> stream.element}))
  }

  def source[A : BinaryFormat, B : BinaryFormat](topic: Topic[A,B]): Stream[A, B] =
    new StreamDef[Grouped, A, B](Source[A, B](topic.name))

  def sink[A : BinaryFormat : Partitioner, B : BinaryFormat](topic: Topic[A, B])(flow: StreamDef[Grouped, A, B]): FlowGraph[Unit] = {
    FlowGraph(Seq(topic.name -> Sink(flow.element)), ())
  }

  def stream[A : BinaryFormat : Partitioner, B : BinaryFormat](label: String)(stream: Graphable[StreamDef[AnyGrouping, A, B]]): FlowGraph[Stream[A, B]] =
    stream.toGraph.flatMap { stream =>
      FlowGraph(Seq(label -> Sink(stream.element)), new StreamDef[Grouped, A, B](Source[A, B](label)))
    }

  def pool[A : BinaryFormat : Partitioner, B : BinaryFormat](label: String)(pool: Graphable[PoolDef[AnyGrouping, A, B]]): FlowGraph[Pool[A, B]] =
    pool.toGraph.flatMap { pool =>
      FlowGraph(Seq(label -> Sink(pool.element)), new PoolDef[Grouped, A, B](pool.initial, Source[A, B](label)))
    }

  def cycle[A : BinaryFormat : Partitioner, B : BinaryFormat](label: String)(cycle: Stream[A, B] => Graphable[StreamDef[AnyGrouping, A, B]]): FlowGraph[Stream[A, B]] = {

    val stream = new StreamDef[Grouped, A, B](Source[A, B](label))

    cycle(stream).toGraph.flatMap { cycled =>
      FlowGraph(Seq(label -> Sink(cycled.element)), stream)
    }
  }

  case class Topic[A, B](name: String)

  private[coast] type Id[+A] = A

  private[coast] type From[A] = { type To[+B] = (A => B) }
}
