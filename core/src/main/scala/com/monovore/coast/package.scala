package com.monovore

import com.monovore.coast.flow._
import com.monovore.coast.format.WireFormat
import com.monovore.coast.model.{Merge, Source, Sink}

package object coast {

  type Stream[A, +B] = StreamDef[Grouped, A, B]

  type Pool[A, +B] = PoolDef[Grouped, A, B]

  type Flow[A] = flow.Flow[A]

  def merge[G <: AnyGrouping, A, B](upstreams: (String -> StreamDef[G, A, B])*): StreamDef[G, A, B] = {

    for ((branch -> streams) <- upstreams.groupByKey) {
      require(streams.size == 1, s"merged branches must be unique ($branch is specified ${streams.size} times)")
    }

    new StreamDef[G, A, B](Merge(upstreams.map { case (name, stream) => name -> stream.element}))
  }

  def source[A : WireFormat, B : WireFormat](name: Name[A,B]): Stream[A, B] =
    new StreamDef[Grouped, A, B](Source[A, B](name.name))

  def sink[A : WireFormat, B : WireFormat](name: Name[A, B])(flow: StreamDef[Grouped, A, B]): Flow[Unit] = {
    Flow(Seq(name.name -> Sink(flow.element)), ())
  }

  def label[A](name: String)(value: A)(implicit lbl: Labellable[A]): Flow[lbl.Labelled] =
    lbl.label(name, value)

  case class Name[A, B](name: String)

  // IMPLEMENTATION
  // always-visible utilities; should be hidden within the coast package

  private[coast] val unit: Unit = ()

  private[coast] def some[A](a: A): Option[A] = Some(a)

  private[coast] type ->[A, B] = (A, B)

  private[coast] object -> {
    def unapply[A, B](pair: (A, B)) = Some(pair)
  }

  private[coast] implicit class SeqOps[A](underlying: Seq[A]) {
    def groupByKey[B,C](implicit proof: A <:< (B, C)): Map[B, Seq[C]] =
      underlying.groupBy { _._1 }.mapValues { _.unzip._2 }
  }

  private[coast] def assuming[A](cond: Boolean)(action: => A): Option[A] =
    if (cond) Some(action) else None

  private[coast] type Id[+A] = A

  private[coast] type From[A] = { type To[+B] = (A => B) }
}
