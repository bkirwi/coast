package com.monovore.coast
package flow

import scala.annotation.implicitNotFound

sealed class AnyGrouping
object Ungrouped extends AnyGrouping

sealed class Grouped extends AnyGrouping
object Grouped extends Grouped

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

// NB: Generalizes the above -- could replace it except for the janky error message
@implicitNotFound("Can't prove that the stream or topic you're working with has grouping ${G1}")
trait SameGroupingAs[-G0 <: AnyGrouping, G1 <: AnyGrouping] {
  def stream[A, B](s: StreamDef[G0, A, B]): StreamDef[G1, A, B]
  def pool[A, B](p: PoolDef[G0, A, B]): PoolDef[G1, A, B]
}

object SameGroupingAs {
  implicit def identical[G <: AnyGrouping]: SameGroupingAs[G, G] = new SameGroupingAs[G, G] {
    override def stream[A, B](s: StreamDef[G, A, B]): StreamDef[G, A, B] = s
    override def pool[A, B](p: PoolDef[G, A, B]): PoolDef[G, A, B] = p
  }
}
