package com.monovore.example.coast

import com.monovore.coast.flow.{Flow, Topic}
import com.monovore.coast.wire.Protocol
import com.twitter.algebird.{AveragedValue, Monoid}

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

  import Protocol.native._

  type VehicleID = Long

  // To simplify the calculation, let's discretize by space and time
  type Segment = Int
  type TimeWindow = Long

  val BaseToll = 1.0 // ???

  case class PlaceAndTime(segment: Segment, time: TimeWindow)
  case class Summary(vehicles: Set[VehicleID] = Set.empty, averageSpeed: AveragedValue = Monoid.zero[AveragedValue])

  implicit val SummaryMonoid = Monoid(Summary.apply _, Summary.unapply _)

  val PositionReports = Topic[VehicleID, (PlaceAndTime, Double)]("position-reports")
  val TotalTolls = Topic[VehicleID, Double]("total-tolls")

  val graph = Flow.build { implicit builder =>

    PositionReports.asSource
      .invert
      .streamTo("reports-by-position")
      .map { case (vehicle, speed) => Summary(Set(vehicle), AveragedValue(speed)) }
      .sum
      .updates
      .map { summary =>
        if (summary.averageSpeed.value < 40) {
          val toll = BaseToll * math.pow(summary.vehicles.size - 150, 2)
          summary.vehicles.map { _ -> toll }.toMap
        } else Map.empty[VehicleID, Double]
      }
      .sumByKey("summaries")
      .updates
      .sinkTo(TotalTolls)
  }
}
