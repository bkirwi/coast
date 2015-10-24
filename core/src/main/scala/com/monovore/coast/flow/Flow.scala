package com.monovore.coast
package flow

import com.monovore.coast.wire.{Partitioner, BinaryFormat}
import core._

/**
 * A mechanism for maintaining name bindings.
 * @param bindings
 * @param value
 * @tparam A
 */
case class Flow[+A](bindings: Seq[String -> Sink[_, _]], value: A) extends Graph with FlowLike[A] {

  def map[B](func: A => B): Flow[B] = copy(value = func(value))

  def flatMap[B](func: A => Flow[B]): Flow[B] = {

    val result = func(value)

    val duplicateName = bindings.exists { case (name, _) =>
      result.bindings.exists { case (other, _) => name == other }
    }

    if (duplicateName) throw new IllegalArgumentException("Reused name binding!")

    Flow(bindings ++ result.bindings, result.value)
  }

  override def toFlow: Flow[A] = this
}

object Flow {

  /**
   * Creates a graph without any name bindings.
   */
  def apply[A](a: A): Flow[A] = Flow(Seq.empty, a)


  // Builder methods

  def merge[G <: AnyGrouping, A, B](upstreams: (String -> StreamDef[G, A, B])*): StreamDef[G, A, B] = {

    for ((branch -> streams) <- upstreams.groupByKey) {
      require(streams.size == 1, s"merged branches must be unique ($branch is specified ${streams.size} times)")
    }

    new StreamDef[G, A, B](Merge(upstreams.map { case (name, stream) => name -> stream.element}))
  }

  def source[A : BinaryFormat, B : BinaryFormat](topic: Topic[A,B]): GroupedStream[A, B] =
    new StreamDef[Grouped, A, B](Source[A, B](topic.name))

  def clock(seconds: Long) = new StreamDef[Grouped, Unit, Long](Clock(seconds))

  def sink[A : BinaryFormat : Partitioner, B : BinaryFormat](topic: Topic[A, B])(flow: FlowLike[GroupedStream[A, B]]): Flow[Unit] = {
    flow.toFlow.flatMap { stream => Flow(Seq(topic.name -> Sink(stream.element)), ()) }
  }

  def stream[A : BinaryFormat : Partitioner, B : BinaryFormat](label: String)(stream: FlowLike[AnyStream[A, B]]): Flow[GroupedStream[A, B]] =
    stream.toFlow.flatMap { stream =>
      Flow(Seq(label -> Sink(stream.element)), new StreamDef[Grouped, A, B](Source[A, B](label)))
    }

  def pool[A : BinaryFormat : Partitioner, B : BinaryFormat](label: String)(pool: FlowLike[AnyPool[A, B]]): Flow[GroupedPool[A, B]] =
    pool.toFlow.flatMap { pool =>
      Flow(Seq(label -> Sink(pool.element)), new PoolDef[Grouped, A, B](pool.initial, Source[A, B](label)))
    }

  def cycle[A : BinaryFormat : Partitioner, B : BinaryFormat](label: String)(cycle: GroupedStream[A, B] => FlowLike[AnyStream[A, B]]): Flow[GroupedStream[A, B]] = {

    val stream = new StreamDef[Grouped, A, B](Source[A, B](label))

    cycle(stream).toFlow.flatMap { cycled =>
      Flow(Seq(label -> Sink(cycled.element)), stream)
    }
  }

  def builder(): Builder = new Builder()

  class Builder private[Flow](private[Flow] var _bindings: Seq[(String, Sink[_, _])] = Nil) extends Graph {

    def add[A](flow: Flow[A]): A = {
      val updated = Flow(_bindings, ()).flatMap { _ => flow }
      _bindings = updated.bindings
      updated.value
    }

    def addCycle[A:BinaryFormat:Partitioner, B:BinaryFormat](name: String)(function: GroupedStream[A, B] => AnyStream[A, B]): GroupedStream[A, B] = {
      add(Flow.cycle(name)(function))
    }

    override def bindings: Seq[(String, Sink[_, _])] = _bindings

    def toFlow: Flow[Unit] = Flow(bindings, ())
  }

  def build[A](fn: Builder => A): Flow[A] = {
    val builder = new Builder()
    val out = fn(builder)
    Flow(builder.bindings, out)
  }
}

trait FlowLike[+A] {
  def toFlow: Flow[A]
}