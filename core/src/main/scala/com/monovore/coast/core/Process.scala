package com.monovore.coast
package core

import com.twitter.algebird.Aggregator

/**
 * A trait that captures a stream transformation from A to B, with a state of S.
 *
 * Processes are very composable: it's possible to build up a complex
 * stream process by plugging simpler process together.
 */
trait Process[S, -A, +B] {

  /**
   * Given the current state and a new input, it returns an updated state and a
   * sequence of outputs.
   */
  def apply(state: S, input: A): (S, Seq[B])

  def map[B0](mapFn: B => B0): Process[S, A, B0] =
    Process { (s, a) =>
      val (s0, bs) = apply(s, a)
      (s0, bs.map(mapFn))
    }

  def flatMap[A0 <: A, B0](flatMapFn: B => Process[S, A0, B0]): Process[S, A0, B0] =
    Process { (s: S, a: A0) =>
      val (s0, bs) = apply(s, a)

      bs.foldLeft(s0 -> Seq.empty[B0]) { (pair, b) =>
        val (s, b0s) = pair
        val (s0, moreBs) = flatMapFn(b).apply(s, a)
        s0 -> (b0s ++ moreBs)
      }
    }

  def andThen[A0 <: A, B0 >: B](other: Process[S, A0,  B0]): Process[S, A0, B0] =
    Process { (s, a) =>
      val (s0, b0) = apply(s, a)
      val (s1, b1) = other.apply(s0, a)
      s1 -> (b0 ++ b1)
    }

  def chain[B0](other: Process[S, B, B0]): Process[S, A, B0] =
    Process { (s, a) =>
      val (s0, bs) = apply(s, a)

      bs.foldLeft(s0 -> Seq.empty[B0]) { (pair, b) =>
        val (s, b0s) = pair
        val (s0, moreBs) = other.apply(s, b)
        s0 -> (b0s ++ moreBs)
      }
    }
}

object Process {

  def apply[S, A, B](function: (S, A) => (S, Seq[B])): Process[S, A, B] =
    new Process[S, A, B] {
      override def apply(state: S, input: A): (S, Seq[B]) = function(state, input)
    }

  def skip[S, A, B]: Process[S, A, B] = Process { (s, a) => (s, Nil) }

  def output[S, A, B](bs: B*) = Process { (s: S, a: A) => (s, bs) }

  def outputEach[S, A, B](bs: Seq[B]) = Process { (s: S, a: A) => (s, bs) }

  def on[S, A, B](func: (S, A) => Process[S, A, B]) =
    Process { (s: S, a: A) => func(s, a).apply(s, a) }

  def onInput[S, A, B](func: A => Process[S, A, B]) =
    Process { (s: S, a: A) => func(a).apply(s, a) }

  def onState[S, A, B](func: S => Process[S, A, B]) =
    Process[S, A, B] { (s: S, a: A) => func(s).apply(s, a) }

  def setState[S](s: S) =
    Process[S, Any, Nothing] { (_: S, _: Any) => (s, Nil) }

  def fromAggregator[S, A, B](aggregator: Aggregator[A, S, B]) =
    Process { (s: S, a: A) =>
      val reduced = aggregator.append(s, a)
      (reduced -> Seq(aggregator.present(reduced)))
    }
}