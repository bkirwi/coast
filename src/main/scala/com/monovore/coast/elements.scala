package com.monovore.coast

sealed trait Element[A, +B]

case class Source[A, +B](source: String) extends Element[A, B]

case class Transform[S, A, B0, +B](
  upstream: Element[A, B0],
  init: S,
  transformer: A => (S, B0) => (S, Seq[B])
) extends Element[A, B]

case class Merge[A, +B](upstreams: Seq[Element[A, B]]) extends Element[A, B]

case class GroupBy[A, B, A0](upstream: Element[A0, B], groupBy: B => A) extends Element[A, B]