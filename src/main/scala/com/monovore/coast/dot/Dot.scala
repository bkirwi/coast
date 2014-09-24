package com.monovore.coast
package dot

import java.util.concurrent.atomic.AtomicInteger

object Dot {

  sealed trait LabelType
  case class Public(name: String) extends LabelType { override def toString = "\"public-" + name + "\"" }
  case class Private(count: Int) extends LabelType { override def toString = "\"internal-" + count + "\"" }

  case class Label(global: LabelType, pretty: String)

  def apply(graph: Graph[_]): String = {

    val newID: String => Label = {
      val atomic = new AtomicInteger()
      (pretty) => { Label(Private(atomic.getAndIncrement), pretty) }
    }

    def sources[A, B](downstream: Label, flow: Element[A, B]): Seq[(Label, Label)] = flow match {
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
    }

    val chain = graph.state.keys
      .flatMap { name =>
        val flow = graph.state(name)
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


  /*
   * Whoa there!
   */
  def main(args: Array[String]): Unit = {

    val input = Name[String, String]("whatever")

    val input2 = Name[String, String]("whatever-2")

    val graph = for {
      great <- Graph.label("great") {
        Graph.merge(Graph.source(input), Graph.source(input2))
          .map { _ + "!!!" }
          .map { _ + "???" }
      }
      _ <- Graph.label("better") {
        Graph.merge(great, Graph.source(input))
          .map { _.reverse}
      }
    } yield ()

    val output = Dot(graph)

    Console.err.print(output)
  }
}
