package com.monovore.coast
package model

import format.WireFormat

sealed trait Node[A, +B]

case class Source[A, B](
  source: String
)(
  implicit val keyFormat: WireFormat[A],
  val valueFormat: WireFormat[B]
) extends Node[A, B]

case class Aggregate[S, A, B0, +B](
  upstream: Node[A, B0],
  init: S,
  transformer: A => (S, B0) => (S, Seq[B])
)(
  implicit val keyFormat: WireFormat[A],
  val stateFormat: WireFormat[S]
) extends Transform[S, A, B0, B]

sealed trait Transform[S, A, B0, +B] extends Node[A, B] {
  def upstream: Node[A, B0]
  def init: S
  def transformer: A => (S, B0) => (S, Seq[B])
}

case class PureTransform[A, B0, B](
  upstream: Node[A, B0],
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
  def unapply[S, A, B0, B](t: Transform[S, A, B0, B]): Option[(Node[A, B0], S, A => (S, B0) => (S, Seq[B]))] =
    Some((t.upstream, t.init, t.transformer))

  def apply[S : WireFormat, A: WireFormat, B0, B](e: Node[A, B0], i: S, t: A => (S, B0) => (S, Seq[B])): Transform[S, A, B0, B] =
    Aggregate(e, i, t)
}

case class Merge[A, +B](upstreams: Seq[String -> Node[A, B]]) extends Node[A, B]

case class GroupBy[A, B, A0](upstream: Node[A0, B], groupBy: B => A) extends Node[A, B]

case class Sink[A, B](element: Node[A, B])(implicit val keyFormat: WireFormat[A], val valueFormat: WireFormat[B])