package com.monovore.coast
package core

/**
 * A trait that captures a stream transformation from A to B, with a state of S.
 *
 * Transformers are very composable: it's possible to build up a complex
 * transformer by plugging simpler transformers together.
 */
trait Transformer[S, -A, +B] {

  /**
   * Given the current state and a new input, it returns an updated state and a
   * sequence of outputs.
   */
  def transform(state: S, input: A): (S, Seq[B])

  def map[B0](mapFn: B => B0): Transformer[S, A, B0] =
    Transformer { (s, a) =>
      val (s0, bs) = transform(s, a)
      (s0, bs.map(mapFn))
    }

  def flatMap[A0 <: A, B0](flatMapFn: B => Transformer[S, A0, B0]): Transformer[S, A0, B0] =
    Transformer { (s: S, a: A0) =>
      val (s0, bs) = transform(s, a)

      bs.foldLeft(s0 -> Seq.empty[B0]) { (pair, b) =>
        val (s, b0s) = pair
        val (s0, moreBs) = flatMapFn(b).transform(s, a)
        s0 -> (b0s ++ moreBs)
      }
    }

  def andThen[A0 <: A, B0 >: B](other: Transformer[S, A0,  B0]): Transformer[S, A0, B0] =
    Transformer { (s, a) =>
      val (s0, b0) = transform(s, a)
      val (s1, b1) = other.transform(s0, a)
      s1 -> (b0 ++ b1)
    }

  def chain[B0](other: Transformer[S, B, B0]): Transformer[S, A, B0] =
    Transformer { (s, a) =>
      val (s0, bs) = transform(s, a)

      bs.foldLeft(s0 -> Seq.empty[B0]) { (pair, b) =>
        val (s, b0s) = pair
        val (s0, moreBs) = other.transform(s, b)
        s0 -> (b0s ++ moreBs)
      }
    }
}

object Transformer {

  def apply[S, A, B](function: (S, A) => (S, Seq[B])): Transformer[S, A, B] =
    new Transformer[S, A, B] {
      override def transform(state: S, input: A): (S, Seq[B]) = function(state, input)
    }

  def skip[S, A, B]: Transformer[S, A, B] = Transformer { (s, a) => (s, Nil) }

  def output[S, A, B](bs: B*) = Transformer { (s: S, a: A) => (s, bs) }

  def outputEach[S, A, B](bs: Seq[B]) = Transformer { (s: S, a: A) => (s, bs) }

  def onInput[S, A, B](func: A => Transformer[S, A, B]) =
    Transformer { (s: S, a: A) => func(a).transform(s, a) }

  def onState[S, A, B](func: S => Transformer[S, A, B]) =
    Transformer[S, A, B] { (s: S, a: A) => func(s).transform(s, a) }

  def setState[S](s: S) =
    Transformer[S, Any, Nothing] { (_: S, _: Any) => (s, Nil) }
}