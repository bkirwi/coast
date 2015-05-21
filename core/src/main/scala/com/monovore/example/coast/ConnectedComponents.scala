package com.monovore.example.coast

import com.monovore.coast.core.Transformer
import com.monovore.coast.flow.{Flow, GroupedStream}
import com.monovore.coast.wire.BinaryFormat

import scala.collection.immutable.SortedSet

object ConnectedComponents {

  import com.monovore.coast.wire.pretty._

  type NodeID = Long

  /**
   * A node is either the label for its component, or labelled with another node
   * in the same component.
   */
  sealed trait LabelState
  case class IsLabel(others: SortedSet[NodeID] = SortedSet.empty) extends LabelState
  case class HasLabel(label: NodeID) extends LabelState

  /**
   * We send a couple events whenever we pick a new label for a node: one grouped
   * under the node id that contains the new label, and one grouped under the
   * label with the new node for that component.
   */
  sealed trait Event
  case class ConnectedTo(other: NodeID) extends Event
  case class SetLabel(label: NodeID) extends Event

  implicit val labelStateFormat = BinaryFormat.javaSerialization[LabelState]
  implicit val eventFormat = BinaryFormat.javaSerialization[Event]

  /**
   * Given a stream of graph edges, returns a stream of (node, component label) pairs,
   * where the label for a component is the smallest id of any node in that component.
   *
   * The edges in the input stream should be keyed by the larger of the two ids.
   */
  def findComponents(
    stream: GroupedStream[NodeID, NodeID]
  ): Flow[GroupedStream[NodeID, NodeID]] = {

    /**
     * This cycle is the engine of the algorithm, which is loosely based on the
     * standard union-find approach. Since we need to write out a new message
     * every time the label for some node changes, we collapse the union and
     * find operations together.
     *
     * In the first phase, we send a series of ConnectedTo messages, 'walking'
     * up the tree to find the root for each of the two nodes. If they have
     * different roots, phase two involves setting all the nodes in one component
     * to the newly-discovered minimum.
     */
    val components = Flow.cycle[NodeID, Event]("component-loop") { components =>

      Flow.merge("input" -> stream.map(ConnectedTo), "components" -> components)
        .zipWithKey
        .transformWith(IsLabel(): LabelState) {

          def relabelComponent(members: SortedSet[NodeID], newLabel: NodeID) = {

            val relabeled =
              members.toSeq.map { member => newLabel -> ConnectedTo(member) }

            Transformer.setState[LabelState](HasLabel(newLabel)) andThen
              Transformer.outputEach(relabeled)
          }

          Transformer.onInput {
            case (source, ConnectedTo(target)) => {
              if (source == target) Transformer.skip
              else if (source > target) {
                Transformer.onState {
                  case HasLabel(label) => {
                    if (target == label) Transformer.skip
                    else if (target > label) Transformer.output(target -> ConnectedTo(label))
                    else {
                      Transformer.setState[LabelState](HasLabel(target)) andThen
                        Transformer.output(label -> ConnectedTo(target))
                    }
                  }
                  case IsLabel(members) => relabelComponent(members + source, target)
                }
              }
              else Transformer.onState {
                case HasLabel(repr) => Transformer.output(repr -> ConnectedTo(target))
                case IsLabel(members) => {
                  Transformer.setState[LabelState](IsLabel(members + target)) andThen
                    Transformer.output(target -> SetLabel(source))
                }
              }
            }
            case (source, SetLabel(target)) => Transformer.onState {
              case HasLabel(label) => {
                if (label > target) Transformer.setState(HasLabel(target))
                else Transformer.skip
              }
              case IsLabel(members) => {
                sys.error(s"Tried to set label for $source to $target, but $source is already the label!")
              }
            }
          }
        }
        .groupByKey

    }

    components.map { stream =>
      stream
        .collect { case SetLabel(other) => other }
        .transform(Long.MaxValue) { (label, update) =>
          if (label <= update) label -> Nil
          else update -> Seq(update)
        }
    }
  }
}
