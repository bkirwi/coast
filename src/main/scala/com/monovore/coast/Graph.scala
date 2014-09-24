package com.monovore.coast

import scala.collection.{GenTraversable, GenTraversableOnce}

case class Name[A, B](name: String)

/**
 * A mechanism for maintaining name bindings.
 * @param state
 * @param contents
 * @tparam A
 */
case class Graph[A](state: Map[String, Flow[_, _]], contents: A) {

  def map[B](func: A => B): Graph[B] = copy(contents = func(contents))

  def flatMap[B](func: A => Graph[B]): Graph[B] = {

    val result = func(contents)

    if (state.keySet.filter(result.state.keySet).nonEmpty)
      throw new IllegalArgumentException("Reused name binding!")

    Graph(state ++ result.state, result.contents)
  }
}

sealed trait Flow[A, +B] {

  def flatMap[B0](func: B => Seq[B0]): Flow[A, B0] = Transform(this, func)

  def filter(func: B => Boolean): Flow[A, B] = flatMap { a =>
    if (func(a)) Seq(a) else Seq.empty
  }

  def map[B0](func: B => B0): Flow[A, B0] = flatMap(func andThen { b => Seq(b)})

  def pool[B0](init: B0)(func: (B0, B) => B0): Pool[A, B0] = Scan(this, init, func)

  def latestOr[B0 >: B](init: B0): Pool[A, B0] = Scan(this, init, { (_, b: B0) => b })

  def groupBy[A0](func: B => A0): Flow[A0, B] = GroupBy(this, func)

  def groupByKey[A0, B0](implicit asPair: B <:< (A0, B0)) =
    this.groupBy { _._1 }.map { _._2 }

  def flatten[B0](implicit func: B => Traversable[B0]) = this.flatMap(func andThen { _.toSeq })
}

sealed trait Pool[A, B] extends Flow[A, B]

case class Source[A, +B](source: String) extends Flow[A, B]

case class Transform[A, +B, B0](upstream: Flow[A, B0], transformer: B0 => Seq[B]) extends Flow[A, B]

case class Scan[A, B, B0](upstream: Flow[A, B0], init: B, reducer: (B, B0) => B) extends Pool[A, B] {
  object flow extends Flow[A, B]
}

case class Merge[A, +B](upstreams: Seq[Flow[A, B]]) extends Flow[A, B]

case class GroupBy[A, B, A0](upstream: Flow[A0, B], groupBy: B => A) extends Flow[A, B]


object Graph {

  def merge[A, B](upstreams: Flow[A, B]*): Flow[A, B] = Merge(upstreams)

  def source[A,B](name: Name[A,B]): Flow[A, B] = Source(name.name)

  def label[A, B](name: String)(flow: Flow[A, B]): Graph[Flow[A, B]] = {
    Graph(Map(name -> flow), Source(name))
  }

  def sink[A, B](name: Name[A, B])(flow: Flow[A, B]): Graph[Unit] = {
    Graph(Map(name.name -> flow), ())
  }
}
