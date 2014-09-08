package com.monovore.coast

import rx.lang.scala.Observable

case class Name[A, B](name: String)

class System {

  case class Graph[A](state: Map[String, Flow[_, _]], contents: A) {

    def map[B](func: A => B): Graph[B] = copy(contents = func(contents))

    def flatMap[B](func: A => Graph[B]): Graph[B] = {

      val result = func(contents)

      Graph(state ++ result.state, result.contents)
    }
  }

  sealed trait Flow[A, +B] {

    def flatMap[B0](func: B => Observable[B0]): Flow[A, B0] = Transform(this, func)

    def filter(func: B => Boolean): Flow[A, B] = flatMap { a =>
      if (func(a)) Observable.just(a) else Observable.empty
    }

    //  def flatMap[B](func: A => Iterable[B]): Flow[B] = Transform(this, func andThen { b => Observable.from(b) })
    def map[B0](func: B => B0): Flow[A, B0] = flatMap(func andThen { b => Observable.just(b)})
  }

  case class Source[A, +B](source: String) extends Flow[A, B]

  case class Transform[A, +B, B0](upstream: Flow[A, B0], transformer: B0 => Observable[B]) extends Flow[A, B]

  case class Merge[A, +B](upstreams: Seq[Flow[A, B]]) extends Flow[A, B]

  case class GroupBy[A, B, A0](upstream: Flow[A0, B], groupBy: B => A) extends Flow[A, B]

//  case class Fold[A, B, B0](upstream: Flow[A, B0], init: B, fold: (B, B0) => B) extends Flow[A, B]

  def merge[A, B](name: String)(upstreams: Flow[A, B]*): Graph[Flow[A, B]] = register(name) {
    Merge(upstreams)
  }

  def source[A,B](name: String): Graph[Flow[A, B]] = register(name)(Source(name))

  def register[A, B](name: String)(flow: Flow[A, B]): Graph[Flow[A, B]] = {
    Graph(Map(name -> flow), Source(name))
  }
}

object TestGraph {

  def main(args: Array[String]) {

    val system = new System

    val graph = for {
      first <- system.source[String, Int]("ints")
      second <- system.register("doubles") {
        for {
          int <- first
          anything <- Observable.from(1 to int)
          if int == 3
        } yield int.toDouble
      }
      merged <- system.merge("both")(first, second)
    } yield (merged)

    println(graph)
  }
}
