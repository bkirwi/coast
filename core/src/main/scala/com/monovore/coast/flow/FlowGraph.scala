package com.monovore.coast
package flow

import model._

/**
 * A mechanism for maintaining name bindings.
 * @param bindings
 * @param value
 * @tparam A
 */
case class FlowGraph[+A](bindings: Seq[String -> Sink[_, _]], value: A) extends Graph {

  def map[B](func: A => B): FlowGraph[B] = copy(value = func(value))

  def flatMap[B](func: A => FlowGraph[B]): FlowGraph[B] = {

    val result = func(value)

    val duplicateName = bindings.exists { case (name, _) =>
      result.bindings.exists { case (other, _) => name == other }
    }

    if (duplicateName) throw new IllegalArgumentException("Reused name binding!")

    FlowGraph(bindings ++ result.bindings, result.value)
  }
}