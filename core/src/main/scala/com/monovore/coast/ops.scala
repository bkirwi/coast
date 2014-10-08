package com.monovore.coast

import scala.language.higherKinds

trait StreamOps[A, +B] { self =>

  type WithKey[+X]

  // TODO: Applicative? Monad?
  protected def withKey[X](wk: WithKey[X]): (A => X)
  protected def mapKeyed[X, Y](x: WithKey[X])(fn: X => Y): WithKey[Y]

  def element: Element[A, B]

  def stream = make(element)

  def make[A0, B0](elem: Element[A0, B0]): Stream[A0, B0] =
    new Stream[A0, B0] { def element: Element[A0, B0] = elem }

  def flatMap[B0](func: WithKey[B => Seq[B0]]): Stream[A, B0] = make[A, B0] {
    Transform[Unit, A, B, B0](self.element, (), {
      withKey(func) andThen { unkeyed =>
        (s: Unit, b: B) => s -> unkeyed(b) }
    })
  }

  def filter(func: WithKey[B => Boolean]): Stream[A, B] = flatMap {
    mapKeyed(func) { func =>
      { a => if (func(a)) Seq(a) else Seq.empty }
    }
  }

  def map[B0](func: WithKey[B => B0]): Stream[A, B0] =
    flatMap(mapKeyed(func) { func => func andThen { b => Seq(b)} })

  def transform[S, B0](init: S)(func: WithKey[(S, B) => (S, Seq[B0])]): Stream[A, B0] = {

    val keyedFunc = withKey(func)

    new Stream[A, B0] {
      def element = Transform[S, A, B, B0](self.element, init, keyedFunc)
    }
  }

  def fold[B0](init: B0)(func: WithKey[(B0, B) => B0]): Pool[A, B0] = {

    val transformer = mapKeyed(func) { fn =>

      (s: B0, b: B) => {
        val b0 = fn(s, b)
        b0 -> Seq(b0)
      }
    }

    new Pool[A, B0] {
      override def initial: B0 = init
      override def element: Element[A, B0] = Transform(self.element, init, withKey(transformer))
    }
  }

  def latestOr[B0 >: B](init: B0): Pool[A, B0] = stream.fold(init) { (_, x) => x }

  def groupBy[A0](func: B => A0): Stream[A0, B] = new Stream[A0, B] {
    def element = GroupBy(self.element, func)
  }

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

trait Stream[A, +B] extends StreamOps[A, B] { stream =>

  override type WithKey[+X] = X

  override protected def withKey[X](wk: WithKey[X]): (A) => X = { _ => wk }
  override protected def mapKeyed[X, Y](x: WithKey[X])(fn: (X) => Y): WithKey[Y] = fn(x)

  def withKeys: KeyedOps[A, B] = KeyedOps(stream.element)
}

case class KeyedOps[A, +B](element: Element[A, B]) extends StreamOps[A, B] {

  override type WithKey[+X] = A => X

  override protected def withKey[X](wk: WithKey[X]): (A) => X = wk
  override protected def mapKeyed[X, Y](x: WithKey[X])(fn: (X) => Y): WithKey[Y] = x andThen fn
}

trait Pool[A, B] { self =>

  def initial: B

  def element: Element[A, B]

  def stream: Stream[A, B] = new Stream[A, B] {
    def element = self.element
  }

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