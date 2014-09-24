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

sealed trait Stream[A, +B] extends Element[A, B] {

  def flatMap[B0](func: B => Seq[B0]): Stream[A, B0] = Transform(this, func)

  def filter(func: B => Boolean): Stream[A, B] = flatMap { a =>
    if (func(a)) Seq(a) else Seq.empty
  }

  def map[B0](func: B => B0): Stream[A, B0] = flatMap(func andThen { b => Seq(b)})

  def pool[B0](init: B0)(func: (B0, B) => B0): Pool[A, B0] = Scan(this, init, func)

  def latestOr[B0 >: B](init: B0): Pool[A, B0] = Scan(this, init, { (_, b: B0) => b })

  def groupBy[A0](func: B => A0): Stream[A0, B] = GroupBy(this, func)

  def groupByKey[A0, B0](implicit asPair: B <:< (A0, B0)) =
    this.groupBy { _._1 }.map { _._2 }

  def flatten[B0](implicit func: B => Traversable[B0]) = this.flatMap(func andThen { _.toSeq })

  def join[B0](pool: Pool[A, B0]): Stream[A, B -> B0] = {

    Graph.merge(pool.stream.map(Left(_)), this.map(Right(_)))
      .pool(pool.init -> (None: Option[B])) { (state, msg) =>
        val (curr, _) = state
        msg match {
          case Left(newState) => (newState -> None)
          case Right(b) => (curr -> Some(b))
        }
      }
      .stream
      .flatMap {
        case (s, Some(b)) => Seq(b -> s)
        case (s, None) => Nil
      }
  }
}

case class Source[A, +B](source: String) extends Stream[A, B]

case class Transform[A, +B, B0](upstream: Stream[A, B0], transformer: B0 => Seq[B]) extends Stream[A, B]

case class Merge[A, +B](upstreams: Seq[Stream[A, B]]) extends Stream[A, B]

case class GroupBy[A, B, A0](upstream: Stream[A0, B], groupBy: B => A) extends Stream[A, B]

case class PoolStream[A, B](pool: Pool[A, B]) extends Stream[A, B]


sealed trait Pool[A, B] extends Element[A, B] {

  def init: B

  def stream: Stream[A, B] = PoolStream(this)

  def map[B0](function: B => B0): Pool[A, B0] = // Mapped(this, function)
    this.stream.map(function).latestOr(function(this.init))

  def join[B0](other: Pool[A, B0]): Pool[A, (B, B0)] = {
    Graph.merge(this.stream.map(Left(_)), other.stream.map(Right(_)))
      .pool(this.init, other.init) { (state, update) =>
        update.fold(
          { left => (left, state._2) },
          { right => (state._1, right) }
        )
      }
  }
}

case class Static[A, B](init: B, name: String) extends Pool[A, B]

case class Scan[A, B, B0](upstream: Stream[A, B0], init: B, reducer: (B, B0) => B) extends Pool[A, B]

case class Mapped[A, B, B0](upstream: Pool[A, B0], mapper: B0 => B) extends Pool[A, B] {
  def init: B = mapper(upstream.init)
}

object Graph {

  def merge[A, B](upstreams: Stream[A, B]*): Stream[A, B] = Merge(upstreams)

  def source[A,B](name: Name[A,B]): Stream[A, B] = Source(name.name)

  def label[A, B](name: String)(flow: Stream[A, B]): Graph[Stream[A, B]] = {
    Graph(Map(name -> flow), Source(name))
  }

  // TODO: clean up this nonsense with a typeclass
  def labelP[A, B](name: String)(flow: Pool[A, B]): Graph[Pool[A, B]] = {
    Graph(Map(name -> flow), Static(flow.init, name))
  }

  def sink[A, B](name: Name[A, B])(flow: Stream[A, B]): Graph[Unit] = {
    Graph(Map(name.name -> flow), ())
  }
}
