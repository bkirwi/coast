package com.monovore.coast
package machine

import com.twitter.algebird.Semigroup

private[machine] case class Message(get: Any) { def cast[T]: T = get.asInstanceOf[T] }
private[machine] case class State(get: Any) { def cast[T]: T = get.asInstanceOf[T] }
private[machine] case class Key(get: Any) { def cast[T]: T = get.asInstanceOf[T] }

case class Actor(
  initialState: State,
  push: (State, Key, Message) => (State, Map[Key, Seq[Message]])
)

object Actor {

  val passthrough = Actor(State(unit), { case (s, k, m) => s -> Map(k -> Seq(m)) } )

  case class Data[Label](state: State, input: Map[Label, Seq[Message]] = Map.empty[Label, Seq[Message]])
}

object System {

  case class State[Label](stateMap: Map[Label, Map[Key, Actor.Data[Label]]] = Map.empty[Label, Map[Key, Actor.Data[Label]]])

  sealed trait Command[Label]
  case class Send[Label](from: Label, partition: Key, message: Message) extends Command[Label]
  case class Process[Label](actor: Label, from: Label, key: Key) extends Command[Label]
}

case class System[Label](
  nodes: Map[Label, Actor] = Map.empty[Label, Actor],
  edges: Map[Label, Seq[Label]] = Map.empty[Label, Seq[Label]]
) {

  def update(state: System.State[Label], command: System.Command[Label]): (System.State[Label], Map[Label, Map[Key, Seq[Message]]]) = {

    command match {

      case System.Process(actor, from, key) => {

        val updated = {

          val node = nodes.getOrElse(actor, Actor.passthrough)
          val actorState = state.stateMap.getOrElse(actor, Map.empty)
          val keyState = actorState.getOrElse(key, Actor.Data(node.initialState))
          val messages = keyState.input.getOrElse(from, Seq.empty)

          assuming(messages.nonEmpty) {

            val (newState, output) = node.push(keyState.state, key, messages.head)

            val newPartitionState = Actor.Data(newState, keyState.input.updated(from, messages.tail))

            val tidied = System.State(state.stateMap.updated(actor, actorState.updated(key, newPartitionState)))

            val steps = for {
              (key, messages) <- output
              message <- messages
            } yield System.Send(actor, key, message)

            steps.foldLeft(tidied -> Map.empty[Label, Map[Key, Seq[Message]]]) { (stateMessages, send) =>
              val (state, messages) = stateMessages
              val (newState, newMessages) = update(state, send)
              newState -> Semigroup.plus(messages, newMessages)
            }
          }
        }

        updated getOrElse (state -> Map.empty)
      }

      case System.Send(from, partition, message) => {

        val newState = edges.getOrElse(from, Seq.empty)
          .foldLeft(state) { (state, to) =>

            val targetState = state.stateMap.getOrElse(to, Map.empty)
            val partitioned = targetState.getOrElse(partition, Actor.Data(state = nodes.getOrElse(to, Actor.passthrough).initialState))
            val pushed = partitioned.copy(input = Semigroup.plus(partitioned.input, Map(from -> Seq(message))))
            System.State(state.stateMap.updated(to, targetState.updated(partition, pushed)))
          }

        newState -> Map(from -> Map(partition -> Seq(message)))
      }
    }
  }

  def commands(state: System.State[Label]): Seq[System.Command[Label]] = {

    for {
      (label -> partitions) <- state.stateMap.toSeq
      (key -> partitionState) <- partitions
      (from -> messages) <- partitionState.input
      if messages.nonEmpty
    } yield System.Process(label, from, key)
  }
}
