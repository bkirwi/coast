package com.monovore.coast
package machine

import com.twitter.algebird.Semigroup

case class Message(get: Any) { def cast[T]: T = get.asInstanceOf[T] }
case class State(get: Any) { def cast[T]: T = get.asInstanceOf[T] }
case class Key(get: Any) { def cast[T]: T = get.asInstanceOf[T] }

case class Actor(
  initialState: State,
  push: (State, Key, Message) => (State, Map[Key, Seq[Message]])
)

object Actor {

  val passthrough = Actor(State(unit), { case (s, k, m) => s -> Map(k -> Seq(m)) } )

  case class Data[Label](state: State, input: Map[Label, Seq[Message]] = Map.empty[Label, Seq[Message]])
}

case class System[Label](
  nodes: Map[Label, Actor] = Map.empty[Label, Actor],
  edges: Map[Label, Seq[Label]] = Map.empty[Label, Seq[Label]],
  state: Map[Label, Map[Key, Actor.Data[Label]]] = Map.empty[Label, Map[Key, Actor.Data[Label]]]
) {

  def push(from: Label, partition: Key, messages: Seq[Message]): System[Label] = {

    val newState = edges.getOrElse(from, Seq.empty)
      .foldLeft(state) { (state, to) =>

      val targetState = state.getOrElse(to, Map.empty)
      val partitioned = targetState.getOrElse(partition, Actor.Data(state = nodes.getOrElse(to, Actor.passthrough).initialState))
      val pushed = partitioned.copy(input = Semigroup.plus(partitioned.input, Map(from -> messages)))
      state.updated(to, targetState.updated(partition, pushed))
    }

    copy(state = newState)
  }

  def process(actor: Label, from: Label, key: Key): (System[Label] -> Map[Key, Seq[Message]]) = {

    val updated = {
      val node = nodes.getOrElse(actor, Actor.passthrough)
      val actorState = state.getOrElse(actor, Map.empty)
      val keyState = actorState.getOrElse(key, Actor.Data(node.initialState))
      val messages = keyState.input.getOrElse(from, Seq.empty)

      assuming(messages.nonEmpty) {

        val (newState, output) = node.push(keyState.state, key, messages.head)

        val newPartitionState = Actor.Data(newState, keyState.input.updated(from, messages.tail))

        val tidied = copy(state = state.updated(actor, actorState.updated(key, newPartitionState)))

        output.foldLeft(tidied) { (tidied, kv) => tidied.push(actor, kv._1, kv._2) } -> output
      }
    }

    updated getOrElse (this -> Map.empty)
  }

  def poke: Seq[System[Label] -> Map[Label, Map[Key, Seq[Message]]]] = {

    for {
      (label -> partitions) <- state.toSeq
      (key -> partitionState) <- partitions
      (from -> messages) <- partitionState.input
      if messages.nonEmpty
    } yield {
      val (sent, output) = process(label, from, key)
      sent -> Map(label -> output)
    }
  }
}
