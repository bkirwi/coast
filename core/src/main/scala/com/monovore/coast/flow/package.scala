package com.monovore.coast

import com.monovore.coast.wire.BinaryFormat
import com.monovore.coast.model.{Source, Merge}

package object flow {

//  type Stream[A, +B] = StreamDef[Grouped, A, B]
//
//  type Pool[A, +B] = PoolDef[Grouped, A, B]
//
//  def merge[G <: AnyGrouping, A, B](upstreams: StreamDef[G, A, B]*): StreamDef[G, A, B] =
//    new StreamDef[G, A, B](Merge(upstreams.map { _.element }))
//
//  def source[A : WireFormat, B : WireFormat](name: Name[A,B]): Stream[A, B] =
//    new StreamDef[Grouped, A, B](Source[A, B](name.name))
//
//  def sink[A : WireFormat, B : WireFormat](name: Name[A, B])(flow: StreamDef[Grouped, A, B]): Flow[Unit] = {
//    Flow(Seq(name.name -> Sink(flow.element)), ())
//  }
//
//  def label[A](name: String)(value: A)(implicit lbl: Labellable[A]): Flow[lbl.Labelled] =
//    lbl.label(name, value)

}
