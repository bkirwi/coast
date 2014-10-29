package com.monovore.coast
package flow

import format._
import model._

sealed trait Labellable[-A] {

  type Labelled

  def label(name: String, value: A): Flow[Labelled]
}

object Labellable {

  implicit def labelStreams[A : WireFormat, B : WireFormat] = new Labellable[StreamDef[AnyGrouping, A, B]] {

    type Labelled = Stream[A, B]

    override def label(name: String, value: StreamDef[AnyGrouping, A, B]): Flow[Stream[A, B]] = {
      Flow(Seq(name -> Sink(value.element)), new StreamDef[Grouped, A, B](Source[A, B](name)))
    }
  }

  implicit def labelPools[A : WireFormat, B : WireFormat] = new Labellable[PoolDef[AnyGrouping, A, B]] {

    type Labelled = Pool[A, B]

    override def label(name: String, value: PoolDef[AnyGrouping, A, B]): Flow[Pool[A, B]] = {
      Flow(Seq(name -> Sink(value.element)), new PoolDef[Grouped, A, B](value.initial, Source[A, B](name)))
    }
  }
}
