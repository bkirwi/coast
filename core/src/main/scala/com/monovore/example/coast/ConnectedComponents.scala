package com.monovore.example.coast

import com.monovore.coast.core.Process
import com.monovore.coast.flow._
import com.monovore.coast.wire.BinaryFormat

import scala.collection.immutable.SortedSet

/**
 * An implementation of connected components: given a stream of new edges, we
 * incrementally maintain a mapping of node id to component id -- where the id
 * for the component is the smallest id of any node in that component.
 *
 * This cribs heavily off the MR-based implementation presented here:
 *
 * http://mmds-data.org/presentations/2014_/vassilvitskii_mmds14.pdf
 */
object ConnectedComponents extends ExampleMain {

  import com.monovore.coast.wire.pretty._

  type NodeID = Long

  implicit val eventFormat = BinaryFormat.javaSerialization[SortedSet[NodeID]]

  val Edges = Topic[Long, Long]("edges")

  val Components = Topic[Long, Long]("components")

  def connect(a: NodeID, b: NodeID) = Seq(a -> b, b -> a)

  implicit val graph = Flow.builder()

  val connected =
    Edges.asSource
      .zipWithKey
      .flatMap { case (one, other) => connect(one, other) }
      .groupByKey
      .streamTo("connected-input")

  val largeStar = graph.addCycle[NodeID, NodeID]("large-star") { largeStar =>

    val smallStar =
      Flow.merge("large" -> largeStar, "input" -> connected)
        .withKeys.process(SortedSet.empty[NodeID]) { node =>

          Process.on { (neighbours, newEdge) =>

            val all = (neighbours + node)
            val least = all.min

            if (node < newEdge || all.contains(newEdge)) Process.skip
            else {
              Process.setState(SortedSet(newEdge)) andThen {
                if (least < newEdge) Process.outputEach(connect(newEdge, least))
                else Process.outputEach(all.toSeq.flatMap(connect(_, newEdge)))
              }
            }
          }
        }
        .groupByKey
        .streamTo("small-star")

    smallStar
      .withKeys.process(SortedSet.empty[NodeID]) { node =>

        Process.on { (neighbours, newEdge) =>
          val all = neighbours + node
          val least = all.min

          Process.setState(neighbours + newEdge) andThen {

            if (newEdge < least) {
              val larger = neighbours.toSeq.filter {_ > node}
              Process.outputEach(larger.flatMap {connect(_, newEdge)})
            }
            else if (newEdge < node || all.contains(newEdge)) Process.skip
            else Process.outputEach(connect(newEdge, least))
          }
        }
      }
      .groupByKey
  }

  largeStar
    .withKeys.transform(Long.MaxValue) { node => (currentOrMax, next) =>
      val current = currentOrMax min node
      val min = current min next
      if (min < current) min -> Seq(min)
      else current -> Nil
    }
    .sinkTo(Components)
}
