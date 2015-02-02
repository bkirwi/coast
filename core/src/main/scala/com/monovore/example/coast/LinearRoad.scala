package com.monovore.example.coast

import com.monovore.coast
import com.monovore.coast.flow
import com.monovore.coast.model.Graph
import com.twitter.algebird.{Monoid, AveragedValue}

/**
 * Starting to stub out the standard linear road example, based on the
 * description here:
 *
 * https://cs.uwaterloo.ca/~david/cs848/stream-cql.pdf
 *
 * I'm not convinced this iss particularly faithful; the non-overlapping time
 * windows and road 'segments' seem particularly fishy. It would be better to
 * work from the official spec.
 */
object LinearRoad extends ExampleMain {

  import coast.wire.ugly._

  type VehicleID = Long

  // To simplify the calculation, let's discretize by space and time
  type Segment = Int
  type TimeWindow = Long

  case class PlaceAndTime(segment: Segment, time: TimeWindow)
  case class Summary(vehicles: Set[VehicleID] = Set.empty, averageSpeed: AveragedValue = Monoid.zero[AveragedValue])

  implicit val SummaryMonoid = Monoid(Summary.apply _, Summary.unapply _)

  val PositionReports = flow.Topic[VehicleID, (PlaceAndTime, Double)]("position-reports")
  val TotalTolls = flow.Topic[VehicleID, Double]("total-tolls")

  val graph: Graph = for {

    vehicleSpeeds <- flow.stream("reports-by-position") {
      flow.source(PositionReports).invert
    }

    tolls <- flow.stream("summaries") {

      vehicleSpeeds
        .map { case (vehicle, speed) => Summary(Set(vehicle), AveragedValue(speed)) }
        .sum
        .updates
        .flatMap { summary =>

          val toll = if (summary.averageSpeed.value < 40) 3.0 else 0.0

          summary.vehicles
            .toSeq.sorted
            .map { _ -> toll }
        }
        .invert
    }

    _ <- flow.sink(TotalTolls) {

      tolls
        .fold(Map.empty[PlaceAndTime, Double]) { _ + _ }
        .map { _.values.sum }
        .updates
    }

  } yield ()
}
