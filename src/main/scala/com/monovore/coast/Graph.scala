package com.monovore.coast

case class Name[A, B](name: String)

/**
 * A mechanism for maintaining name bindings.
 * @param state
 * @param contents
 * @tparam A
 */
case class Graph[A](state: Map[String, Element[_, _]], contents: A) {

  def map[B](func: A => B): Graph[B] = copy(contents = func(contents))

  def flatMap[B](func: A => Graph[B]): Graph[B] = {

    val result = func(contents)

    if (state.keySet.filter(result.state.keySet).nonEmpty)
      throw new IllegalArgumentException("Reused name binding!")

    Graph(state ++ result.state, result.contents)
  }
}

sealed trait Element[A, +B]

case class Source[A, +B](source: String) extends Element[A, B]

case class Transform[S, A, B0, +B](upstream: Element[A, B0], init: S, transformer: (S, B0) => (S, Seq[B])) extends Element[A, B]

case class Merge[A, +B](upstreams: Seq[Element[A, B]]) extends Element[A, B]

// TODO: I'm not sure about the co- / contra-variance here... double-check?
case class GroupBy[A, B, A0](upstream: Element[A0, B], groupBy: B => A) extends Element[A, B]


trait Stream[A, +B] { self =>

  def element: Element[A, B]

  def transform[S, B0](init: S)(func: (S, B) => (S, Seq[B0])): Stream[A, B0] = new Stream[A, B0] {
    def element = Transform[S, A, B, B0](self.element, init, { (s, b) => func(s, b) })
  }

  def flatMap[B0](func: B => Seq[B0]): Stream[A, B0] =
    transform(unit) { (_: Unit, b) => () -> func(b) }

  def filter(func: B => Boolean): Stream[A, B] = flatMap { a =>
    if (func(a)) Seq(a) else Seq.empty
  }

  def map[B0](func: B => B0): Stream[A, B0] = flatMap(func andThen { b => Seq(b)})

  def fold[B0](init: B0)(func: (B0, B) => B0): Pool[A, B0] = new Pool[A, B0] {

    def initial = init

    def element = Transform[B0, A, B, B0](self.element, init, { case (s, b) =>
      val newS = func(s, b)
      newS -> Seq(newS)
    })
  }

  def latestOr[B0 >: B](init: B0): Pool[A, B0] = fold(init) { (_, x) => x }

  def groupBy[A0](func: B => A0): Stream[A0, B] = new Stream[A0, B] {
    def element = GroupBy(self.element, func)
  }

  def groupByKey[A0, B0](implicit asPair: B <:< (A0, B0)) =
    this.groupBy { _._1 }.map { _._2 }

  def flatten[B0](implicit func: B => Traversable[B0]) = this.flatMap(func andThen { _.toSeq })

  def join[B0](pool: Pool[A, B0]): Stream[A, B -> B0] = {

    Graph.merge(pool.stream.map(Left(_)), this.map(Right(_)))
      .transform(pool.initial) { (state, msg) =>
        msg match {
          case Left(newState) => newState -> Seq.empty
          case Right(msg) => state -> Seq(msg -> state)
        }
      }
  }
}

sealed trait Pool[A, B] { self =>

  def initial: B

  def element: Element[A, B]

  def stream: Stream[A, B] = new Stream[A, B] {
    def element = self.element
  }

  def map[B0](function: B => B0): Pool[A, B0] = // Mapped(this, function)
    this.stream.map(function).latestOr(function(this.initial))

  def join[B0](other: Pool[A, B0]): Pool[A, (B, B0)] = {
    Graph.merge(this.stream.map(Left(_)), other.stream.map(Right(_)))
      .fold(this.initial, other.initial) { (state, update) =>
        update.fold(
          { left => (left, state._2) },
          { right => (state._1, right) }
        )
      }
  }
}

object Graph {

  def merge[A, B](upstreams: Stream[A, B]*): Stream[A, B] = new Stream[A, B] {
    def element = Merge(upstreams.map { _.element })
  }

  def source[A,B](name: Name[A,B]): Stream[A, B] = new Stream[A, B] {
    def element = Source(name.name)
  }

  sealed trait Labellable[A] { def label(name: String, value: A): Graph[A] }

  implicit def labelStreams[A, B]: Labellable[Stream[A, B]] = new Labellable[Stream[A, B]] {
    override def label(name: String, value: Stream[A, B]): Graph[Stream[A, B]] = {
      Graph(Map(name -> value.element), new Stream[A, B] {
        def element = Source(name)
      })
    }
  }

  implicit def labelPools[A, B]: Labellable[Pool[A, B]] = new Labellable[Pool[A, B]] {
    override def label(name: String, value: Pool[A, B]): Graph[Pool[A, B]] = {
      Graph(Map(name -> value.element), new Pool[A, B] {
        def initial = value.initial
        def element = Source(name)
      })
    }
  }

  def label[A](name: String)(value: A)(implicit lbl: Labellable[A]): Graph[A] =
    lbl.label(name, value)

  def sink[A, B](name: Name[A, B])(flow: Stream[A, B]): Graph[Unit] = {
    Graph(Map(name.name -> flow.element), ())
  }
}
