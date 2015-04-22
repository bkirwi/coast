package com.monovore.coast
package flow

import scala.annotation.implicitNotFound

class AnyGrouping
class Grouped extends AnyGrouping

@implicitNotFound("Can't prove that the stream you're working with is grouped.")
trait IsGrouped[-G <: AnyGrouping] {
  def stream[A, B](s: StreamDef[G, A, B]): GroupedStream[A, B]
  def pool[A, B](p: PoolDef[G, A, B]): GroupedPool[A, B]
}

object IsGrouped {

  implicit object groupedGrouped extends IsGrouped[Grouped] {

    override def stream[A, B](s: StreamDef[Grouped, A, B]): GroupedStream[A, B] = s

    override def pool[A, B](p: PoolDef[Grouped, A, B]): GroupedPool[A, B] = p
  }
}