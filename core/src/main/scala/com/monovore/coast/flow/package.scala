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

  def source[A : BinaryFormat, B : BinaryFormat](name: Name[A,B]): Stream[A, B] =
    new StreamDef[Grouped, A, B](Source[A, B](name.name))

  def sink[A : BinaryFormat : Partitioner, B : BinaryFormat](name: Name[A, B])(flow: StreamDef[Grouped, A, B]): FlowGraph[Unit] = {
    FlowGraph(Seq(name.name -> Sink(flow.element)), ())
  }

  def stream[A : BinaryFormat : Partitioner, B : BinaryFormat](label: String)(stream: StreamDef[AnyGrouping, A, B]): FlowGraph[Stream[A, B]] =
    FlowGraph(Seq(label -> Sink(stream.element)), new StreamDef[Grouped, A, B](Source[A, B](label)))

  def pool[A : BinaryFormat : Partitioner, B : BinaryFormat](label: String)(pool: PoolDef[AnyGrouping, A, B]): FlowGraph[Pool[A, B]] =
    FlowGraph(Seq(label -> Sink(pool.element)), new PoolDef[Grouped, A, B](pool.initial, Source[A, B](label)))

  case class Name[A, B](name: String)

}
