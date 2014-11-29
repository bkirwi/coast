package com.monovore.coast
package machine

import com.monovore.coast.flow._
import com.monovore.coast.model._

import com.twitter.algebird.Semigroup

object Machine {

  sealed trait Label
  case class Named(name: String) extends Label
  case class Anonymous(index: Int) extends Label

  def compile(graph: Graph): Machine = {

    val newID = {

      var count = 0

      () => {
        count += 1
        Anonymous(count): Label
      }
    }

    def compile[A, B](
      downstream: Label, 
      flow: Node[A, B]
    ): (Map[Label, Actor] -> Seq[Label -> Label]) = flow match {
      case Source(name) => {
        Map.empty[Label, Actor] -> Seq(Named(name) -> downstream)
      }
      case Transform(upstream, init, transformer) => {

        val id = newID()

        val (nodes -> edges) = compile(id, upstream)

        val node = Actor(State(init), { case (s, k, blob) =>
          val (newS, messages) = transformer(k.cast)(s.cast, blob.cast)
          State(newS) -> Map(k -> messages.map(Message(_)))
        })

        nodes.updated(id, node) -> (edges ++ Seq(id -> downstream))
      }
      case Merge(upstreams) => {

        val id = newID()

        val (nodes, edges) = upstreams
          .foldLeft(Map.empty[Label, Actor] -> Seq.empty[Label -> Label]) { (soFar, upstreamPair) =>

            val (name -> upstream) = upstreamPair

            val (nodes, edges) = soFar

            val (newNodes, newEdges) = compile(id, upstream)

            (nodes ++ newNodes) -> (edges ++ newEdges)
          }

        nodes.updated(id, Actor.passthrough) -> (edges ++ Seq(id -> downstream))
      }
      case GroupBy(upstream, groupBy) => {

        val id = newID()

        val (nodes, edges) = compile(id, upstream)

        val actor = Actor(State(unit), { case (s, key, input) =>
          val group = groupBy(key.cast)(input.cast)
          (s, Map(Key(group) -> Seq(input)))
        })

        nodes.updated(id, actor) -> (edges ++ Seq(id -> downstream))
      }
    }

    val (nodes, edges) = graph.bindings
      .map { case (key -> Sink(flow)) =>

        val (nodes, edges) = compile(Named(key), flow)

        nodes -> edges
      }
      .unzip


    val edgeMap = edges.flatten.groupByKey

    val nodeMap = nodes.flatten.toMap

    Machine(System(nodes = nodeMap, edges = edgeMap))
  }
}

case class Machine(system: System[Machine.Label]) {

  def push[A, B](name: Name[A, B], pairs: (A -> B)*): Machine = {

    val label = Machine.Named(name.name)

    val pushed = pairs.groupByKey.foldLeft(system) { (system, pair) =>
      val (key, messages) = pair
      system.push(label, Key(key), messages.map(Message))
    }

    Machine(pushed)
  }

  def push(messages: Messages): Machine = {

    val pairs = for {
      (name, partitioned) <- messages.messageMap
      (key, values) <- partitioned
    } yield (Machine.Named(name), key) -> values

    val pushed = pairs
      .foldLeft(system) { (system, pair) =>
        val (name, key) -> values = pair
        system.push(name, key, values)
      }

    Machine(pushed)
  }

  def next: Seq[Machine -> Messages] = {

    system.poke.map { case (system, output) =>
      val cleaned = output flatMap {
        case (Machine.Named(name) -> value) => Some(name -> value)
        case _ => None
      }
      Machine(system) -> Messages(cleaned)
    }
  }
}


case class Messages(messageMap: Map[String, Map[Key, Seq[Message]]]) {

  def apply[A, B](name: Name[A, B]): Map[A, Seq[B]] = {

    val keyed = messageMap.getOrElse(name.name, Map.empty)

    keyed.map { case (k -> v) => k.cast[A] -> v.map { _.cast[B] } }
  }

  def ++(other: Messages): Messages = Messages(Semigroup.plus(messageMap, other.messageMap))

  def isEmpty: Boolean = messageMap.values.forall { _.values.forall { _.isEmpty } }
}

object Messages {

  val empty: Messages = Messages(Map.empty)

  def from[A, B](name: Name[A, B], messages: Map[A, Seq[B]]): Messages = {

    Messages(Map(
      name.name -> messages
        .filter { case (_ -> v) => v.nonEmpty }
        .map { case (k -> v) => Key(k) -> v.map(Message) }
    ))
  }
}