package com.monovore.coast

case class Name[A, B](name: String)

class System {


  /**
   * A mechanism for maintaining name bindings.
   * @param state
   * @param contents
   * @tparam A
   */
  case class Graph[A](state: toy.NameMap[Flow], contents: A) {

    def map[B](func: A => B): Graph[B] = copy(contents = func(contents))

    def flatMap[B](func: A => Graph[B]): Graph[B] = {

      val result = func(contents)

      val updated = result.state.keys
        .foldLeft(state) { (map, key) =>
          key match {
            case name: Name[aT, bT] => map.put(name, result.state.apply(name))
          }
        }

      Graph(updated, result.contents)
    }
  }

  sealed trait Flow[A, +B] {

    def flatMap[B0](func: B => Seq[B0]): Flow[A, B0] = Transform(this, func)

    def filter(func: B => Boolean): Flow[A, B] = flatMap { a =>
      if (func(a)) Seq(a) else Seq.empty
    }

    def map[B0](func: B => B0): Flow[A, B0] = flatMap(func andThen { b => Seq(b)})

    def scanLeft[B0](init: B0)(func: (B0, B) => B0): Pool[A, B0] = Scan(this, func, init)
  }

  sealed trait Pool[A, B] {
    def named(name: String): Graph[Pool[A, B]] =
      Graph(toy.NameMap.empty.put(Name[A,B](name), ???), ???)
  }

  case class Source[A, +B](source: String) extends Flow[A, B]

  case class Transform[A, +B, B0](upstream: Flow[A, B0], transformer: B0 => Seq[B]) extends Flow[A, B]

  case class Scan[A, B, B0](upstream: Flow[A, B0], reducer: (B, B0) => B, init: B) extends Pool[A, B]

//  case class Merge[A, +B](upstreams: Seq[Flow[A, B]]) extends Flow[A, B]

//  case class GroupBy[A, B, A0](upstream: Flow[A0, B], groupBy: B => A) extends Flow[A, B]

//  case class Fold[A, B, B0](upstream: Flow[A, B0], init: B, fold: (B, B0) => B) extends Flow[A, B]

//  def merge[A, B](name: String)(upstreams: Flow[A, B]*): Graph[Flow[A, B]] = register(name) {
//    Merge(upstreams)
//  }

//  def source[A,B](name: Name[A,B]): Graph[Flow[A,]]

  def source[A,B](name: String): Graph[Flow[A, B]] = register(name)(Source(name))

  def register[A, B](name: String)(flow: Flow[A, B]): Graph[Flow[A, B]] = {
    Graph(toy.NameMap.empty.put(Name[A,B](name), flow), Source(name))
  }

  def registerP[A, B](name: String)(flow: Pool[A, B]): Graph[Pool[A, B]] = {
    Graph(toy.NameMap.empty.put(Name[A,B](name), ???), ???)
  }
}
