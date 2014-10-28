package com.monovore.coast

import scala.language.higherKinds

sealed trait Context[K, X[+_]] {
  def unwrap[A](wrapped: X[A]): (K => A)
  def map[A, B](wrapped: X[A])(function: A => B): X[B]
}

class NoContext[K] extends Context[K, Id] {
  override def unwrap[A](wrapped: Id[A]): (K) => A = { _ => wrapped }
  override def map[A, B](wrapped: Id[A])(function: (A) => B): Id[B] = function(wrapped)
}

class FnContext[K] extends Context[K, From[K]#To] {
  override def unwrap[A](wrapped: From[K]#To[A]): (K) => A = wrapped
  override def map[A, B](wrapped: From[K]#To[A])(function: (A) => B): From[K]#To[B] = wrapped andThen function
}

class StreamOps[WithKey[+_], A, +B](
  context: Context[A, WithKey],
  private[coast] val element: Element[A, B]
) { self =>

  def stream = new Stream(element)

  def flatMap[B0](func: WithKey[B => Seq[B0]]): Stream[A, B0] = new Stream(
    PureTransform[A, B, B0](self.element, {
      context.unwrap(func) andThen { unkeyed =>
        (b: B) => unkeyed(b) }
    })
  )

  def filter(func: WithKey[B => Boolean]): Stream[A, B] = flatMap {
    context.map(func) { func =>
      { a => if (func(a)) Seq(a) else Seq.empty }
    }
  }

  def map[B0](func: WithKey[B => B0]): Stream[A, B0] =
    flatMap(context.map(func) { func => func andThen { b => Seq(b)} })

  def transform[S, B0](init: S)(func: WithKey[(S, B) => (S, Seq[B0])]): Stream[A, B0] = {

    val keyedFunc = context.unwrap(func)

    new Stream[A, B0](Transform[S, A, B, B0](self.element, init, keyedFunc))
  }

  def fold[B0](init: B0)(func: WithKey[(B0, B) => B0]): Pool[A, B0] = {

    val transformer = context.map(func) { fn =>

      (s: B0, b: B) => {
        val b0 = fn(s, b)
        b0 -> Seq(b0)
      }
    }

    new Pool[A, B0](init, Transform(self.element, init, context.unwrap(transformer)))
  }

  def latestOr[B0 >: B](init: B0): Pool[A, B0] = stream.fold(init) { (_, x) => x }

  def groupBy[A0](func: B => A0): Stream[A0, B] =
    new Stream[A0, B](GroupBy(self.element, func))

  def groupByKey[A0, B0](implicit asPair: B <:< (A0, B0)) =
    stream.groupBy { _._1 }.map { _._2 }

  def flatten[B0](implicit func: B => Traversable[B0]) = stream.flatMap(func andThen { _.toSeq })

  def join[B0](pool: Pool[A, B0]): Stream[A, B -> B0] = {

    Flow.merge(pool.stream.map(Left(_)), stream.map(Right(_)))
      .transform(pool.initial) { (state: B0, msg: Either[B0, B]) =>
        msg match {
          case Left(newState) => newState -> Seq.empty
          case Right(msg) => state -> Seq(msg -> state)
        }
      }
  }
}

class Stream[A, +B](element: Element[A, B]) extends StreamOps[Id, A, B](new NoContext[A], element) {

  def withKeys: StreamOps[From[A]#To, A, B] =
    new StreamOps[From[A]#To, A, B](new FnContext[A], element)
}

class Pool[A, B](
  private[coast] val initial: B,
  private[coast] val element: Element[A, B]
) {

  def stream: Stream[A, B] = new Stream(element)

  def map[B0](function: B => B0): Pool[A, B0] =
    stream.map(function).latestOr(function(initial))

  def join[B0](other: Pool[A, B0]): Pool[A, (B, B0)] =
    Flow.merge(stream.map(Left(_)), other.stream.map(Right(_)))
      .fold(initial, other.initial) { (state, update) =>
        update.fold(
          { left => (left, state._2) },
          { right => (state._1, right) }
        )
      }
}