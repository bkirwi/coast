package com.monovore.coast
package flow

import com.monovore.coast.core._
import com.monovore.coast.wire.{Partitioner, BinaryFormat}
import com.twitter.algebird.{Monoid, MonoidAggregator}

class StreamBuilder[WithKey[+_], +G <: AnyGrouping, A, +B](
  private[coast] val context: Context[A, WithKey],
  private[coast] val element: Node[A, B]
) { self =>

  def stream: StreamDef[G, A, B] = new StreamDef(element)

  def flatMap[B0](func: WithKey[B => Seq[B0]]): StreamDef[G, A, B0] = new StreamDef(
    PureTransform[A, B, B0](self.element, context.unwrap(func))
  )

  def filter(func: WithKey[B => Boolean]): StreamDef[G, A, B] = flatMap {
    context.map(func) { func =>
      { a => if (func(a)) Seq(a) else Seq.empty }
    }
  }

  def map[B0](func: WithKey[B => B0]): StreamDef[G, A, B0] =
    flatMap(context.map(func) { func => func andThen { b => Seq(b) } })

  def collect[B0](func: WithKey[PartialFunction[B,  B0]]): StreamDef[G, A, B0] =
    flatMap(context.map(func) { func =>
      { b => if (func.isDefinedAt(b)) Seq(func(b)) else Seq.empty }
    })

  def transform[S, B0](init: S)(func: WithKey[(S, B) => (S, Seq[B0])])(
    implicit isGrouped: IsGrouped[G], keyFormat: BinaryFormat[A], stateFormat: BinaryFormat[S]
  ): GroupedStream[A, B0] = {

    new StreamDef(StatefulTransform[S, A, B, B0](self.element, init, context.unwrap(func)))
  }

  def process[S, B0](init: S)(trans: WithKey[Process[S, B, B0]])(
    implicit isGrouped: IsGrouped[G], keyFormat: BinaryFormat[A], stateFormat: BinaryFormat[S]
  ): GroupedStream[A, B0] = {

    transform(init)(context.map(trans) { _.apply })
  }

  def fold[B0](init: B0)(func: WithKey[(B0, B) => B0])(
    implicit isGrouped: IsGrouped[G], keyFormat: BinaryFormat[A], stateFormat: BinaryFormat[B0]
  ): GroupedPool[A, B0] = {

    val transformer = context.map(func) { fn =>

      (state: B0, next: B) => {
        val newState = fn(state, next)
        newState -> Seq(newState)
      }
    }

    this.transform(init)(transformer).latestOr(init)
  }

  def aggregate[S, B0](aggregator: MonoidAggregator[B, S, B0])(
    implicit isGrouped: IsGrouped[G], keyFormat: BinaryFormat[A], stateFormat: BinaryFormat[S]
  ): GroupedPool[A, B0] = {

    implicit val stateMonoid = aggregator.monoid

    this.stream
      .map(aggregator.prepare)
      .sum
      .map(aggregator.present)
  }

  def grouped[B0 >: B](size: Int)(
    implicit isGrouped: IsGrouped[G], keyFormat: BinaryFormat[A], stateFormat: BinaryFormat[Seq[B0]]
  ): GroupedStream[A, Seq[B0]] = {

    require(size > 0, "Expected a positive group size")

    stream.transform(Vector.empty[B0]: Seq[B0]) { (buffer, next) =>

      if (buffer.size >= size) Vector.empty[B0] -> Seq(buffer)
      else (buffer :+ (next: B0)) -> Seq.empty[Seq[B0]]
    }
  }

  def latestOr[B0 >: B](init: B0): PoolDef[G, A, B0] =
    new PoolDef(init, element)

  def latestOption: PoolDef[G, A, Option[B]] =
    stream.map { b => Some(b) }.latestOr(None)

  def groupBy[A0](func: WithKey[B => A0]): StreamDef[AnyGrouping, A0, B] =
    new StreamDef[G, A0, B](GroupBy(self.element, context.unwrap(func)))

  def groupByKey[A0, B0](implicit asPair: B <:< (A0, B0)) =
    stream.groupBy { _._1 }.map { _._2 }

  def invert[A0, B0](implicit asPair: B <:< (A0, B0)): StreamDef[AnyGrouping, A0, (A, B0)] = {
    stream
      .withKeys.map { key => value => key -> (value: (A0, B0)) }
      .groupBy { case (_, (k, _)) => k }
      .map { case (k, (_, v)) => k -> v }
  }

  def flatten[B0](implicit func: B <:< Seq[B0]) = stream.flatMap(func)

  def flattenOption[B0](implicit func: B <:< Option[B0]) = stream.flatMap(func andThen { _.toSeq })

  def sum[B0 >: B](
    implicit monoid: Monoid[B0], isGrouped: IsGrouped[G], keyFormat: BinaryFormat[A], valueFormat: BinaryFormat[B0]
  ): GroupedPool[A, B0] = {
    stream.fold(monoid.zero)(monoid.plus)
  }

  def join[B0](pool: GroupedPool[A, B0])(
    implicit isGrouped: IsGrouped[G], keyFormat: BinaryFormat[A], b0Format: BinaryFormat[B0]
  ): GroupedStream[A, (B, B0)] = {

    Flow.merge("stream" -> isGrouped.stream(this.stream).map(Right(_)), "pool" -> pool.updates.map(Left(_)))
      .transform(pool.initial) { (state: B0, msg: Either[B0, B]) =>
        msg match {
          case Left(newState) => newState -> Seq.empty
          case Right(msg) => state -> Seq(msg -> state)
        }
      }
  }

  def zipWithKey: StreamDef[G, A, (A, B)] =
    stream.withKeys.map { k => v => (k, v) }


  // Builder-related methods

  def streamTo[B0 >: B](name: String)(
    implicit keyFormat: BinaryFormat[A], partitioner: Partitioner[A], valueFormat: BinaryFormat[B0], ctx: Flow.Builder
  ): GroupedStream[A, B0] = {
    ctx.add(Flow.stream[A, B0](name)(stream))
  }

  def sinkTo[B0 >: B](topic: Topic[A, B0])(
    implicit keyFormat: BinaryFormat[A], partitioner: Partitioner[A], valueFormat: BinaryFormat[B0], grouped: IsGrouped[G], ctx: Flow.Builder
  ): Unit = {
    ctx.add(Flow.sink(topic)(grouped.stream(stream)))
  }

  def latestByKey[K, V](
    name: String
  )(implicit
    ctx: Flow.Builder,
    partitioner: Partitioner[K],
    ordering: Ordering[K],
    isGrouped: IsGrouped[G],
    keyFormat: BinaryFormat[A],
    newKeyFormat: BinaryFormat[K],
    messageFormat: BinaryFormat[V],
    isMap: B <:< Map[K, V]
  ): GroupedPool[K, Map[A, V]] = {

    import BinaryFormat.defaults._

    stream
      .transform(Seq.empty[K]) { (last, next) =>

        val asMap = isMap(next)

        val remove =
          last.filterNot(asMap.contains).map { _ -> None }

        val add =
          asMap.toSeq
            .sortBy { _._1 }
            .map { case (k, v) => k -> Some(v) }

        add.map { _._1 } -> (remove ++ add)
      }
      .invert
      .streamTo(name)
      .fold(Map.empty[A, V]) { (map, update) =>
        update match {
          case (k, None) => map - k
          case (k, Some(v)) => map.updated(k, v)
        }
      }
  }
}

class StreamDef[+G <: AnyGrouping, A, +B](element: Node[A, B])
    extends StreamBuilder[Id, G, A, B](new NoContext[A], element) with FlowLike[StreamDef[G, A, B]] {

  def withKeys: StreamBuilder[From[A]#To, G, A, B] =
    new StreamBuilder[From[A]#To, G, A, B](new FnContext[A], element)

  override def toFlow: Flow[StreamDef[G, A, B]] = Flow(this)
}
