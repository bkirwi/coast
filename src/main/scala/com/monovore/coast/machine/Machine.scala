package com.monovore.coast
package machine

import com.twitter.algebird.Semigroup

object Machine {

  private[machine] case class Message(get: Any) { def cast[T]: T = get.asInstanceOf[T] }
  private[machine] case class State(get: Any) { def cast[T]: T = get.asInstanceOf[T] }
  private[machine] case class Partition(get: Any) { def cast[T]: T = get.asInstanceOf[T] }

  private[machine] case class Node(
    initialState: State,
    push: (State -> Message) => (State -> Seq[Message])
  )

  val passthrough = Node(State(unit), { case (state, msg) => (state, Seq(msg)) })

  case class CurrentState(state: State, input: Map[Label, Seq[Message]] = Map.empty)

  sealed trait Label
  case class Named(name: String) extends Label
  case class Anonymous(index: Int) extends Label
  case object External extends Label

  def compile(graph: Graph[_]): Machine = {

    val newID = {

      var count = 0

      () => {
        count += 1
        Anonymous(count): Label
      }
    }

    def compile[A, B](
      downstream: Label, 
      flow: Flow[A, B]
    ): (Map[Label, Node] -> Seq[Label -> Label]) = flow match {
      case Source(name) => {
        Map.empty[Label, Node] -> Seq(Named(name) -> downstream)
      }
      case Transform(upstream, transform) => {

        val id = newID()

        val (nodes -> edges) = compile(id, upstream)

        val node = Node(State(unit), { case (s, blob) =>
          s -> transform(blob.cast[B]).map(Message)
        })

        nodes.updated(id, node) -> (edges ++ Seq(id -> downstream))
      }
    }

    val (nodes, edges) = graph.state.keys
      .map { key =>

        val flow = graph.state(key)

        val (nodes, edges) = compile(Named(key.name), flow)

        nodes.updated(Named(key.name), passthrough) -> edges
      }
      .unzip


    val edgeMap = edges.flatten.groupByKey

    val nodeMap = nodes.flatten.toMap

    Machine(nodeMap, edgeMap)
  }
}

case class Machine(
  nodes: Map[Machine.Label, Machine.Node] = Map.empty,
  edges: Map[Machine.Label, Seq[Machine.Label]] = Map.empty,
  state: Map[Machine.Label, Map[Machine.Partition, Machine.CurrentState]] = Map.empty
) {

  import Machine._

  def push(from: Label, partition: Partition, messages: Seq[Message]): Machine = {

    val newState = edges.getOrElse(from, Seq.empty)
      .foldLeft(state) { (state, to) =>

        val targetState = state.getOrElse(to, Map.empty)
        val partitioned = targetState.getOrElse(partition, CurrentState(nodes.getOrElse(to, passthrough).initialState))
        val pushed = partitioned.copy(input = Semigroup.plus(partitioned.input, Map(from -> messages)))
        state.updated(to, targetState.updated(partition, pushed))
      }

    copy(state = newState)
  }

  def poke: Set[Machine -> Map[Label, Map[Partition, Seq[Message]]]] = {

    for {
      (label -> partitions) <- state.toSet
      node = nodes.getOrElse(label, passthrough)
      (partition -> partitionState) <- partitions
      (from -> messages) <- partitionState.input
      if messages.nonEmpty
    } yield {

      val (newState, output) = node.push(partitionState.state, messages.head)

      val newPartitionState = CurrentState(newState, partitionState.input.updated(from, messages.tail))

      val tidied = copy(state = state.updated(label, partitions.updated(partition, newPartitionState)))

      val sent = tidied.push(label, partition, output)

      sent -> Map(label -> Map(partition -> output))
    }
  }
}


