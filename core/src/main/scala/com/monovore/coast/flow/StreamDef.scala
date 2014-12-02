package com.monovore.coast
package flow

import com.monovore.coast.model._
import com.monovore.coast.wire.BinaryFormat

class StreamBuilder[WithKey[+_], +G <: AnyGrouping, A, +B](
  private[coast] val context: Context[A, WithKey],
  private[coast] val element: Node[A, B]
) { self =>

  def stream: StreamDef[G, A, B] = new StreamDef(element)

  def flatMap[B0](func: WithKey[B => Seq[B0]]): StreamDef[G, A, B0] = new StreamDef(
    PureTransform[A, B, B0](self.element, {
      context.unwrap(func) andThen { unkeyed =>
        (b: B) => unkeyed(b) }
    })
  )

  def filter(func: WithKey[B => Boolean]): StreamDef[G, A, B] = flatMap {
    context.map(func) { func =>
      { a => if (func(a)) Seq(a) else Seq.empty }
    }
  }

  def map[B0](func: WithKey[B => B0]): StreamDef[G, A, B0] =
    flatMap(context.map(func) { func => func andThen { b => Seq(b)} })

  def aggregate[S, B0](init: S)(func: WithKey[(S, B) => (S, Seq[B0])])(
    implicit isGrouped: IsGrouped[G], keyFormat: BinaryFormat[A], stateFormat: BinaryFormat[S]
  ): Stream[A, B0] = {

    val keyedFunc = context.unwrap(func)

    new StreamDef(Aggregate[S, A, B, B0](self.element, init, keyedFunc))
  }

  def fold[B0](init: B0)(func: WithKey[(B0, B) => B0])(
    implicit isGrouped: IsGrouped[G], keyFormat: BinaryFormat[A], stateFormat: BinaryFormat[B0]
  ): Pool[A, B0] = {

    val transformer = context.map(func) { fn =>

      (s: B0, b: B) => {
        val b0 = fn(s, b)
        b0 -> Seq(b0)
      }
    }

    new PoolDef(init, Transform(self.element, init, context.unwrap(transformer)))
  }

  def grouped[B0 >: B](size: Int)(
    implicit isGrouped: IsGrouped[G], keyFormat: BinaryFormat[A], stateFormat: BinaryFormat[Seq[B0]]
  ): Stream[A, Seq[B0]] = {

    stream.aggregate(Vector.empty[B0]: Seq[B0]) { (buffer, next) =>

      if (buffer.size >= size) Vector.empty[B0] -> Seq(buffer)
      else (buffer :+ (next: B0)) -> Seq.empty[Seq[B0]]
    }
  }

  def windowed[S](size: Int)(init: S)(function: (S, B) => S)(
    implicit isGrouped: IsGrouped[G], keyFormat: BinaryFormat[A], stateFormat: BinaryFormat[(S, Int)]
  ): Stream[A, S] = {

    require(size > 0, "Expected a positive window size")

    stream.aggregate(init -> 0) { (buffer, next) =>

      val (state, count) = buffer

      val newState = function(state, next)
      val newCount = count + 1

      if (newCount >= size) (init -> 0) -> Seq(newState)
      else (newState -> newCount) -> Seq.empty[S]
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

  def flatten[B0](implicit func: B => Traversable[B0]) = stream.flatMap(func andThen { _.toSeq })

  def join[B0](pool: Pool[A, B0])(
    implicit isGrouped: IsGrouped[G], keyFormat: BinaryFormat[A], b0Format: BinaryFormat[B0]
  ): Stream[A, B -> B0] = {

    merge("stream" -> pool.updates.map(Left(_)), "pool" -> isGrouped.stream(this.stream).map(Right(_)))
      .aggregate(pool.initial) { (state: B0, msg: Either[B0, B]) =>
        msg match {
          case Left(newState) => newState -> Seq.empty
          case Right(msg) => state -> Seq(msg -> state)
        }
      }
  }
}

class StreamDef[+G <: AnyGrouping, A, +B](element: Node[A, B]) extends StreamBuilder[Id, G, A, B](new NoContext[A], element) {

  def withKeys: StreamBuilder[From[A]#To, G, A, B] =
    new StreamBuilder[From[A]#To, G, A, B](new FnContext[A], element)
}