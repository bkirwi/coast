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
