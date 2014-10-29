package com.monovore.coast

case class Name[A, B](name: String)

case class Sink[A, B](element: Element[A, B])(implicit val keyFormat: WireFormat[A], val valueFormat: WireFormat[B])

/**
 * A mechanism for maintaining name bindings.
 * @param bindings
 * @param value
 * @tparam A
 */
case class Flow[A](bindings: Seq[String -> Sink[_, _]], value: A) {

  def map[B](func: A => B): Flow[B] = copy(value = func(value))

  def flatMap[B](func: A => Flow[B]): Flow[B] = {

    val result = func(value)

    val duplicateName = bindings.exists { case (name, _) =>
      result.bindings.exists { case (other, _) => name == other }
    }

    if (duplicateName) throw new IllegalArgumentException("Reused name binding!")

    Flow(bindings ++ result.bindings, result.value)
  }
}

object Flow {

  def merge[G <: AnyGrouping, A, B](upstreams: StreamDef[G, A, B]*): StreamDef[G, A, B] =
    new StreamDef[G, A, B](Merge(upstreams.map { _.element }))

  def source[A : WireFormat, B : WireFormat](name: Name[A,B]): Stream[A, B] =
    new StreamDef[Grouped, A, B](Source[A, B](name.name))

  sealed trait Labellable[-A] {

    type Labelled

    def label(name: String, value: A): Flow[Labelled]
  }

  implicit def labelStreams[A : WireFormat, B : WireFormat] = new Labellable[StreamDef[AnyGrouping, A, B]] {

    type Labelled = Stream[A, B]

    override def label(name: String, value: StreamDef[AnyGrouping, A, B]): Flow[Stream[A, B]] = {
      Flow(Seq(name -> Sink(value.element)), new StreamDef[Grouped, A, B](Source[A, B](name)))
    }
  }

  implicit def labelPools[A : WireFormat, B : WireFormat] = new Labellable[PoolDef[AnyGrouping, A, B]] {

    type Labelled = Pool[A, B]

    override def label(name: String, value: PoolDef[AnyGrouping, A, B]): Flow[Pool[A, B]] = {
      Flow(Seq(name -> Sink(value.element)), new PoolDef[Grouped, A, B](value.initial, Source[A, B](name)))
    }
  }

  def label[A](name: String)(value: A)(implicit lbl: Labellable[A]): Flow[lbl.Labelled] =
    lbl.label(name, value)

  def sink[A : WireFormat, B : WireFormat](name: Name[A, B])(flow: StreamDef[Grouped, A, B]): Flow[Unit] = {
    Flow(Seq(name.name -> Sink(flow.element)), ())
  }
}
