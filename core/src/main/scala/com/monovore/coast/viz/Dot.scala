package com.monovore.coast
package viz

import java.util.concurrent.atomic.AtomicInteger

import com.monovore.coast.core._

object Dot {

  private[this] sealed trait LabelType
  private[this] case class Public(name: String) extends LabelType { override def toString = "\"public-" + name + "\"" }
  private[this] case class Private(count: Int) extends LabelType { override def toString = "\"internal-" + count + "\"" }

  private[this] case class Label(global: LabelType, pretty: String)
  private[this] case class Edge(from: Label, to: Label, tag: Option[String] = None)

  /**
   * Write out a graph description in the
   * [[http://en.wikipedia.org/wiki/DOT_(graph_description_language) DOT graph
   * description language]]. You'll probably want to hand this off to another
   * tool for visualization or further processing.
   */
  def describe(graph: Graph): String = {

    val newID: String => Label = {
      val atomic = new AtomicInteger()
      (pretty) => { Label(Private(atomic.getAndIncrement), pretty) }
    }

    def sources[A, B](downstream: Label, flow: Node[A, B]): Seq[Edge] = flow match {
      case Source(name) => {
        val label = Label(Public(name), name)
        Seq(Edge(label, downstream))
      }
      case PureTransform(upstream, _) => {
        val id = newID("transform")
        sources(id, upstream) ++ Seq(Edge(id, downstream))
      }
      case StatefulTransform(upstream, _, _) => {
        val id = newID("aggregate")
        sources(id, upstream) ++ Seq(Edge(id, downstream))
      }
      case Merge(upstreams) => {
        val id = newID("merge")
        upstreams.map { case (name -> up) => sources(id, up) }.flatten ++ Seq(Edge(id, downstream))
      }
      case GroupBy(upstream, _) => {
        val id = newID("groupBy")
        sources(id, upstream) ++ Seq(Edge(id, downstream))
      }
    }

    val chains = graph.bindings
      .map { case (name -> Sink(flow)) =>
        name -> sources(Label(Public(name), name), flow)
      }
      .toMap

    val subgraphs = for ((label, chain) <- chains) yield {

      val nodes = chain
        .flatMap { case Edge(k, v, _) => Seq(k, v) }.toSet
        .map { label: Label =>

        val Label(global, pretty) = label

        val shape = global match {
          case Public(_) => "rectangle"
          case Private(_) => "plaintext"
        }


        s"""$global [shape=$shape, label="${pretty}"];"""
      }

      val edges = chain.map { case Edge(Label(k, _), Label(v, _), tag) =>
        val options = tag.map { t => s"""[label="${t}"]""" }.getOrElse("")
        s"$k -> $v; $options"
      }

      s"""  subgraph "cluster-$label" {
         |
         |    label="$label"
         |    labeljust=l
         |
         |    // nodes
         |    ${ nodes.mkString("\n    ") }
         |
         |    // edges
         |    ${ edges.mkString("\n    ") }
         |  }
         """.stripMargin
    }

    s"""digraph flow {
       |
       |${subgraphs.mkString("\n")}
       |}
       |""".stripMargin
  }
}
