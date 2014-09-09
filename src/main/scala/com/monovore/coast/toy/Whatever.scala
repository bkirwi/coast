package com.monovore.coast
package toy

import com.monovore.coast.toy.Machine.State
import rx.lang.scala.Observable
import shapeless.HMap

object Whatever {

  case class InputData[A,B,C,D](pending: Seq[(A,B)], whatever: (A, B) => Seq[(C, D)])

  type InputFor[C,D] = {
    type Data[A, B] = ((A, B)) => Seq[(C, D)]
  }

  def prepare(sys: System)(graph: sys.Graph[Unit]): Machine = {

    def compile[A,B](flow: sys.Flow[A,B]): NameMap[InputFor[A,B]#Data] = flow match {
      case sys.Source(name) => {
        NameMap.empty[InputFor[A,B]#Data].put(Name(name), { pair: (A,B) => Seq(pair) } )
      }
      case transform: sys.Transform[_, bT, b0T] => {

        val upstream: sys.Flow[A, b0T] = transform.upstream

        val compiled: NameMap[InputFor[A, b0T]#Data] = compile(upstream)

        compiled
          .mapValues(new Mapper[InputFor[A, b0T]#Data, InputFor[A, B]#Data] {
            override def convert[C, D](in: InputFor[A, b0T]#Data[C, D]): InputFor[A, B]#Data[C,D]2 = ???
          })

        ???
      }
      case _ => ???
    }

    def compress[A,B](flow: sys.Flow[A,B]): NameMap[Pairs] = flow match {
      case sys.Source(name) => NameMap.empty.put(Name(name), Seq.empty: Pairs[A,B])
      case sys.Transform(upstream, func) => compress(upstream)
//        .mapValues { _ andThen {
//        _.flatMap { case (a, b) => func(b).map { a -> _ } }
//      } }
//      case sys.GroupBy(upstream, groupBy: (B => A)) => source(upstream).mapValues { _ andThen {
//        _.map { case (a, b) => groupBy(b) -> b }
//      } }
//      case sys.Merge(upstreams) => upstreams.map(source).foldLeft(Map.empty: Info[A, B]) { _ ++ _ }
    }

//    val compiled = graph.state.mapValues { flow => compress(flow) }
//
//    val notify = compiled
//      .toSeq
//      .flatMap { case (name, what) => what.keys.map { name -> _ } }
//      .groupBy { _._1 }.mapValues { _.map { _._2 }}

    val emptyState = graph.state
      .mapValues(new Mapper[sys.Flow, Machine.State] {
        override def convert[A, B](in: sys.Flow[A, B]) = Machine.State(input = compress(in))
      })

    Machine(emptyState)
  }
}

object Machine {

  case class State[A, B](
    input: NameMap[Pairs] = NameMap.empty,
    output: Pairs[A,B] = Seq.empty
  ) {
    case class Input[C,D](pending: Seq[(C,D)], whatever: (C, D) => Seq[(A, B)])
  }
}

case class Machine(state: NameMap[Machine.State]) {

  def push[A, B](from: Name[A, B], messages: (A,B)*): Machine = {

    val withInput = state.keys.foldLeft(NameMap.empty[Machine.State]) { case (newState, name: Name[aT, bT]) =>

      val nodeState = state.apply(name)

      val updated = nodeState.input.keys.foldLeft(NameMap.empty[Pairs]) {
        case (newNodeState: NameMap[Pairs], `from`) =>
          val pairs: Pairs[A, B] = nodeState.input.apply(from)
          newNodeState.put(from, pairs ++ messages)
        case (newNodeState, _) => newNodeState
      }

      newState.put(name, nodeState.copy(input = updated))
    }

    val withOutput = {
      val fromNode = withInput.apply(from)
      withInput.put(from, fromNode.copy(output = fromNode.output ++ messages))
    }

    Machine(withOutput)
  }

  def process: Set[Process] = {

    state.keys.flatMap { case name: Name[aT, bT] =>

      val nodeState = state(name)

      nodeState.input.keys
        .flatMap { case sourceName: Name[cT, dT] =>

          val input = nodeState.input(sourceName)

          input.headOption.map { head =>
            val popped = state.put(name,
              nodeState.copy(input = nodeState.input.put(sourceName, input.tail))
            )
            ??? // Machine(popped).push(name, head)
          }
        }

        ???
    }

    ???
  }
}

trait Process {
  def next(): Machine
}
