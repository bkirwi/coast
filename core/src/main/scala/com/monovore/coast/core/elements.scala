package com.monovore.coast
package core

import com.monovore.coast.wire.{Partitioner, BinaryFormat}

sealed trait Node[A, +B]

case class Source[A, B](
  source: String
)(
  implicit val keyFormat: BinaryFormat[A],
  val valueFormat: BinaryFormat[B]
) extends Node[A, B]

case class StatefulTransform[S, A, B0, +B](
  upstream: Node[A, B0],
  init: A => S,
  transformer: A => (S, B0) => (S, Seq[B])
)(
  implicit val keyFormat: BinaryFormat[A],
  val stateFormat: BinaryFormat[S]
) extends Transform[S, A, B0, B]

sealed trait Transform[S, A, B0, +B] extends Node[A, B] {
  def upstream: Node[A, B0]
  def init: A => S
  def transformer: A => (S, B0) => (S, Seq[B])
}

case class PureTransform[A, B0, B](
  upstream: Node[A, B0],
  function: A => B0 => Seq[B]
) extends Transform[Unit, A, B0, B] {

  override val init: (A => Unit) = { _ => () }

  override val transformer: (A) => (Unit, B0) => (Unit, Seq[B]) = {
    a => {
      val fn = function(a)

      (_, b) => { () -> fn(b) }
    }
  }
}

object Transform {
  def unapply[S, A, B0, B](t: Transform[S, A, B0, B]): Option[(Node[A, B0], A => S, A => (S, B0) => (S, Seq[B]))] =
    Some((t.upstream, t.init, t.transformer))

  def apply[S : BinaryFormat, A: BinaryFormat, B0, B](e: Node[A, B0], i: S, t: A => (S, B0) => (S, Seq[B])): Transform[S, A, B0, B] =
    StatefulTransform(e, _ => i, t)
}

case class Merge[A, +B](upstreams: Seq[String -> Node[A, B]]) extends Node[A, B]

case class GroupBy[A, B, A0](upstream: Node[A0, B], groupBy: A0 => B => A) extends Node[A, B]

case class Sink[A, B](element: Node[A, B])(
  implicit val keyFormat: BinaryFormat[A],
  val valueFormat: BinaryFormat[B],
  val keyPartitioner: Partitioner[A]
)

trait Graph {
  def bindings: Seq[String -> Sink[_, _]]
}
