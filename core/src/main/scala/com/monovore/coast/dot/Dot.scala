package com.monovore.coast
package dot

import flow._
import model._

import java.util.concurrent.atomic.AtomicInteger

import com.monovore.coast.model.{Transform, Node}

object Dot {

  sealed trait LabelType
  case class Public(name: String) extends LabelType { override def toString = "\"public-" + name + "\"" }
  case class Private(count: Int) extends LabelType { override def toString = "\"internal-" + count + "\"" }

  case class Label(global: LabelType, pretty: String)

  def apply(graph: Flow[Unit]): String = {

    val newID: String => Label = {
      val atomic = new AtomicInteger()
      (pretty) => { Label(Private(atomic.getAndIncrement), pretty) }
    }

    def sources[A, B](downstream: Label, flow: Node[A, B]): Seq[(Label, Label)] = flow match {
      case Source(name) => {
        val label = Label(Public(name), name)
        Seq(label -> downstream)
      }
      case Transform(upstream, _, _) => {
        val id = newID("transform")
        sources(id, upstream) ++ Seq(id -> downstream)
      }
      case Merge(upstreams) => {
        val id = newID("merge")
        upstreams.map { up => sources(id, up) }.flatten ++ Seq(id -> downstream)
      }
      case GroupBy(upstream, _) => {
        val id = newID("groupBy")
        sources(id, upstream) ++ Seq(id -> downstream)
      }
    }

    val chain = graph.bindings
      .flatMap { case (name -> Sink(flow)) =>
        sources(Label(Public(name), name), flow)
      }

    val nodes = chain
      .flatMap { case (k, v) => Seq(k, v) }.toSet
      .map { label: Label =>

        val Label(global, pretty) = label

        val shape = global match {
          case Public(_) => "rectangle"
          case Private(_) => "plaintext"
        }


        s"""$global [shape=$shape, label="${pretty}"];"""
      }

    val edges = chain.map { case (Label(k, _), Label(v, _)) => s"$k -> $v;" }

    s"""digraph {
       |
       |  // nodes
       |  ${ nodes.mkString("\n  ") }
       |
       |  // edges
       |  ${ edges.mkString("\n  ") }
       |}
       |""".stripMargin
  }
}
