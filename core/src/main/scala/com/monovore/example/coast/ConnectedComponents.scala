package com.monovore.example.coast

import com.monovore.coast.flow.{GroupedPool, Flow, GroupedStream}
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
object ConnectedComponents {

  import com.monovore.coast.wire.pretty._

  type NodeID = Long

  implicit val eventFormat = BinaryFormat.javaSerialization[SortedSet[NodeID]]

  /**
   * Given a stream of graph edges, returns a stream of (node, component label) pairs,
   * where the label for a component is the smallest id of any node in that component.
   */
  def findComponents(
    input: GroupedStream[NodeID, NodeID]
  ): Flow[GroupedStream[NodeID, NodeID]] = {

    // tiny helper to create two directed edges from a single pair of nodes
    def connect(a: NodeID, b: NodeID) = Seq(a -> b, b -> a)

    // the large-star step: connect all larger neighbours to the least neighbour
    def doLargeStar(smallStar: GroupedStream[NodeID, NodeID]) =
      smallStar
        .withKeys.transform(SortedSet.empty[NodeID]) { node => (neighbours, newEdge) =>

          val all = neighbours + node
          val least = all.min
          val newNeigbours = neighbours + newEdge

          if (newEdge < least) {
            val larger = neighbours.toSeq.filter {_ > node}
            newNeigbours -> larger.flatMap { connect(_, newEdge) }
          }
          else if (newEdge < node || all.contains(newEdge)) newNeigbours -> Nil
          else newNeigbours -> connect(newEdge, least)
        }
        .groupByKey

    // the small-star step: connect all smaller (or equal) neighbours to the least neighbour
    def doSmallStar(largeStar: GroupedStream[NodeID, NodeID]) =
      largeStar
        .withKeys
        .transform(SortedSet.empty[NodeID]) { node => (neighbours, newEdge) =>

          val all = (neighbours + node)
          val least = all.min

          if (node < newEdge || all.contains(newEdge)) neighbours -> Nil
          else if (least < newEdge) SortedSet(newEdge) -> connect(newEdge, least)
          else SortedSet(newEdge) -> all.toSeq.flatMap(connect(_, newEdge))
        }
        .groupByKey

    for {

      // group the input both directions
      connectedInput <- Flow.stream("connected-input") {
        input.withKeys.flatMap { one => other => connect(one, other) }.groupByKey
      }

      // this cycle handles the core loop of the algorithm
      // large-star messages go to small-star, and vice-versa
      largeStar <- Flow.cycle[NodeID, NodeID]("large-star") { largeStar =>

        for {

          smallStar <- Flow.stream("small-star") {
            val inputs = Flow.merge("large" -> largeStar, "input" -> connectedInput)
            doSmallStar(inputs)
          }

          largeStar = {
            doLargeStar(smallStar)
          }

        } yield largeStar
      }

      // finally, we take the least message we've seen for each component
      leastComponent = {
        largeStar
          .withKeys.transform(Long.MaxValue) { node => (currentOrMax, next) =>
            val current = currentOrMax min node
            val min = current min next
            if (min < current) min -> Seq(min)
            else current -> Nil
          }
      }

    } yield leastComponent
  }
}
