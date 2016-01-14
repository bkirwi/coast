package com.monovore.example.coast

import com.monovore.coast.machine.{Messages, Sample, Machine}
import com.monovore.example.coast.LinearRoad.PlaceAndTime
import org.scalacheck.Prop
import org.specs2.matcher.Parameters
import org.specs2.mutable._
import org.specs2.ScalaCheck

class LinearRoadSpec extends Specification with ScalaCheck {

  implicit val scalacheck = Parameters(maxSize = 20)

  "The linear road example" should {

    "not charge at times when cars are fast" in {

      val vehicle: LinearRoad.VehicleID = 7L

      val input = Messages.from(LinearRoad.PositionReports, Map(vehicle -> Seq(PlaceAndTime(10, 20) -> 50.5)))

      val testCase = Machine.compile(LinearRoad.graph).push(input)

      Prop.forAll(Sample.complete(testCase)) { output =>

        output(LinearRoad.TotalTolls)(vehicle).lastOption.getOrElse(0L) must_== 0
      }
    }

    "charge at times when cars are slow" in {

      val vehicle: LinearRoad.VehicleID = 7L

      val input = Messages.from(LinearRoad.PositionReports, Map(vehicle -> Seq(PlaceAndTime(10, 20) -> 20.5)))

      val testCase = Machine.compile(LinearRoad.graph).push(input)

      Prop.forAll(Sample.complete(testCase)) { output =>
        output(LinearRoad.TotalTolls)(vehicle).last must_!= 0
      }
    }
  }
}
