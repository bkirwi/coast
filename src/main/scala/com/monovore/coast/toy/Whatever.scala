package com.monovore.coast
package toy

import rx.lang.scala.Observable

object Whatever {

  type Info[A, B] = Map[String, Any => Observable[(A, B)]]

  def prepare(sys: System)(graph: sys.Graph[Unit]): Machine = {

    def compress[A,B](flow: sys.Flow[A,B]): Info[A, B] = flow match {
      case sys.Source(name) => Map(name -> { x: Any => Observable.just(x.asInstanceOf[(A, B)]) })
      case sys.Transform(upstream, func) => compress(upstream).mapValues { _ andThen {
        _.flatMap { case (a, b) => func(b).map { a -> _ } }
      } }
//      case sys.GroupBy(upstream, groupBy: (B => A)) => source(upstream).mapValues { _ andThen {
//        _.map { case (a, b) => groupBy(b) -> b }
//      } }
//      case sys.Merge(upstreams) => upstreams.map(source).foldLeft(Map.empty: Info[A, B]) { _ ++ _ }
    }

    val compiled = graph.state.mapValues { flow => compress(flow) }

    val notify = compiled
      .toSeq
      .flatMap { case (name, what) => what.keys.map { name -> _ } }
      .groupBy { _._1 }.mapValues { _.map { _._2 }}

    ???
  }
}

trait Machine {
  def state[A, B](name: Name[A, B]): Seq[(A, B)]
  def push[A, B](name: Name[A, B], messages: (A,B)*): Machine
  def process: Set[Process]
}

trait Process {
  def next(): Machine
}
