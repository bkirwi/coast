package com.monovore.coast

sealed trait Element[A, +B]

case class Source[A, +B](source: String) extends Element[A, B]

case class Aggregate[S, A, B0, +B](
  upstream: Element[A, B0],
  init: S,
  transformer: A => (S, B0) => (S, Seq[B])
) extends Transform[S, A, B0, B]

sealed trait Transform[S, A, B0, +B] extends Element[A, B] {
  def upstream: Element[A, B0]
  def init: S
  def transformer: A => (S, B0) => (S, Seq[B])
}

case class PureTransform[A, B0, B](
  upstream: Element[A, B0],
  function: A => B0 => Seq[B]
) extends Transform[Unit, A, B0, B] {

  override val init: Unit = ()

  override val transformer: (A) => (Unit, B0) => (Unit, Seq[B]) = {
    a => {
      val fn = function(a)

      (_, b) => { () -> fn(b) }
    }
  }
}

object Transform {
  def unapply[S, A, B0, B](t: Transform[S, A, B0, B]): Option[(Element[A, B0], S, A => (S, B0) => (S, Seq[B]))] =
    Some((t.upstream, t.init, t.transformer))

  def apply[S, A, B0, B](e: Element[A, B0], i: S, t: A => (S, B0) => (S, Seq[B])): Transform[S, A, B0, B] =
    Aggregate(e, i, t)
}

case class Merge[A, +B](upstreams: Seq[Element[A, B]]) extends Element[A, B]

case class GroupBy[A, B, A0](upstream: Element[A0, B], groupBy: B => A) extends Element[A, B]