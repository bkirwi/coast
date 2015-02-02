package com.monovore.coast
package flow

import model._

/**
 * A mechanism for maintaining name bindings.
 * @param bindings
 * @param value
 * @tparam A
 */
case class Flow[+A](bindings: Seq[String -> Sink[_, _]], value: A) extends Graph with FlowLike[A] {

  def map[B](func: A => B): Flow[B] = copy(value = func(value))

  def flatMap[B](func: A => Flow[B]): Flow[B] = {

    val result = func(value)

    val duplicateName = bindings.exists { case (name, _) =>
      result.bindings.exists { case (other, _) => name == other }
    }

    if (duplicateName) throw new IllegalArgumentException("Reused name binding!")

    Flow(bindings ++ result.bindings, result.value)
  }

  override def toFlow: Flow[A] = this
}

object Flow {

  /**
   * Creates a graph without any name bindings.
   */
  def apply[A](a: A): Flow[A] = Flow(Seq.empty, a)
}

trait FlowLike[+A] {
  def toFlow: Flow[A]
}