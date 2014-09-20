package com.monovore.coast
package machine

class System[A, B](val state: Map[A, System.NodeState[A, B]]) {

  def push(address: A, message: B): System[A, B] = {

    val newState = state(address).listeners
      .foldLeft(state) { (state, address) =>
        val node = state(address)

        val updated = node.input.updated(None, node.input(None) :+ message)

        state.updated(address, node.copy(input = updated))
      }

    new System(state)
  }
}

object System {

  case class NodeState[A,B](
    input: Map[Option[A], Seq[B]],
    handler: Option[Handler[A, B]],
    listeners: Set[A]
  )

  case class Handler[A, B](handler: B => (Handler[A, B] -> Map[A, Seq[B]]))
}
