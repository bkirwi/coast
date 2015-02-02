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

  def sink[A : BinaryFormat : Partitioner, B : BinaryFormat](topic: Topic[A, B])(flow: StreamDef[Grouped, A, B]): Flow[Unit] = {
    Flow(Seq(topic.name -> Sink(flow.element)), ())
  }

  def stream[A : BinaryFormat : Partitioner, B : BinaryFormat](label: String)(stream: FlowLike[StreamDef[AnyGrouping, A, B]]): Flow[Stream[A, B]] =
    stream.toFlow.flatMap { stream =>
      Flow(Seq(label -> Sink(stream.element)), new StreamDef[Grouped, A, B](Source[A, B](label)))
    }

  def pool[A : BinaryFormat : Partitioner, B : BinaryFormat](label: String)(pool: FlowLike[PoolDef[AnyGrouping, A, B]]): Flow[Pool[A, B]] =
    pool.toFlow.flatMap { pool =>
      Flow(Seq(label -> Sink(pool.element)), new PoolDef[Grouped, A, B](pool.initial, Source[A, B](label)))
    }

  def cycle[A : BinaryFormat : Partitioner, B : BinaryFormat](label: String)(cycle: Stream[A, B] => FlowLike[StreamDef[AnyGrouping, A, B]]): Flow[Stream[A, B]] = {

    val stream = new StreamDef[Grouped, A, B](Source[A, B](label))

    cycle(stream).toFlow.flatMap { cycled =>
      Flow(Seq(label -> Sink(cycled.element)), stream)
    }
  }

  case class Topic[A, B](name: String)

  private[coast] type Id[+A] = A

  private[coast] type From[A] = { type To[+B] = (A => B) }
}
