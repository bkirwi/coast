package com.monovore.coast
package flow

import scala.annotation.implicitNotFound

class AnyGrouping
class Grouped extends AnyGrouping

@implicitNotFound("Can't prove that the stream you're working with is grouped. You may need to label it first!")
trait IsGrouped[-G <: AnyGrouping] {
  def stream[A, B](s: StreamDef[G, A, B]): Stream[A, B]
  def pool[A, B](p: PoolDef[G, A, B]): Pool[A, B]
}

object IsGrouped {

  implicit object groupedGrouped extends IsGrouped[Grouped] {

    override def stream[A, B](s: StreamDef[Grouped, A, B]): Stream[A, B] = s

    override def pool[A, B](p: PoolDef[Grouped, A, B]): Pool[A, B] = p
  }
}