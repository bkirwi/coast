package com.monovore.coast
package flow

import model._

/**
 * A mechanism for maintaining name bindings.
 * @param bindings
 * @param value
 * @tparam A
 */
case class FlowGraph[+A](bindings: Seq[String -> Sink[_, _]], value: A) extends Graph with Graphable[A] {

  def map[B](func: A => B): FlowGraph[B] = copy(value = func(value))

  def flatMap[B](func: A => FlowGraph[B]): FlowGraph[B] = {

    val result = func(value)

    val duplicateName = bindings.exists { case (name, _) =>
      result.bindings.exists { case (other, _) => name == other }
    }

    if (duplicateName) throw new IllegalArgumentException("Reused name binding!")

    FlowGraph(bindings ++ result.bindings, result.value)
  }

  override def toGraph: FlowGraph[A] = this
}

object FlowGraph {

  /**
   * Creates a graph without any name bindings.
   */
  def apply[A](a: A): FlowGraph[A] = FlowGraph(Seq.empty, a)
}

trait Graphable[+A] {
  def toGraph: FlowGraph[A]
}