package com.monovore.example.coast

import com.monovore.coast.core.Transformer
import com.monovore.coast.flow.{Flow, GroupedStream}
import com.monovore.coast.wire.{BinaryFormat, Partitioner}

import scala.collection.immutable.SortedSet

object ConnectedComponents {

  /**
   * Given a stream of graph edges, returns a stream of (node, component) pairs.
   */
  def canonicalize[A : Ordering : BinaryFormat : Partitioner](
    stream: GroupedStream[A, A]
  ): Flow[GroupedStream[A, A]] = {

    val ord = implicitly[Ordering[A]]
    import ord.mkOrderingOps

    sealed trait LabelState
    case class IsLabel(others: SortedSet[A] = SortedSet.empty) extends LabelState
    case class HasLabel(label: A) extends LabelState

    sealed trait Event
    case class NewLabel(label: A) extends Event
    case class ConnectedTo(other: A) extends Event

    // TODO: parameterize this on the format for A
    implicit val labelStateFormat = BinaryFormat.javaSerialization[LabelState]
    implicit val eventFormat = BinaryFormat.javaSerialization[Event]

    for {

      components <- Flow.cycle[A, Event]("components") { components =>

        Flow.merge("input" -> stream, "components" -> components.collect { case ConnectedTo(other) => other })
          .zipWithKey
          .transformWith(IsLabel(): LabelState) {

            Transformer.onInput { case (source, target) =>

              if (source == target) Transformer.skip
              else Transformer.onState {
                case HasLabel(repr) => {
                  if (target == repr) Transformer.skip
                  else if (target < repr) {
                    Transformer.setState[LabelState](HasLabel(target)) andThen
                      Transformer.output(repr -> ConnectedTo(target), source -> NewLabel(target))
                  }
                  else Transformer.output(target -> ConnectedTo(repr))
                }
                case IsLabel(members) => {

                  if (target < source) {

                    val relabelOthers = members.toSeq.map { _ -> ConnectedTo(target) }
                    val relabelSelf = Seq(target -> ConnectedTo(source), source -> NewLabel(target))

                    Transformer.setState[LabelState](HasLabel(target)) andThen
                      Transformer.outputEach(relabelOthers ++ relabelSelf)
                  }
                  else Transformer.setState[LabelState](IsLabel(members + target))
                }
              }
            }
          }
          .groupByKey

      }
    } yield {
      components.collect { case NewLabel(other) => other }
    }
  }
}
