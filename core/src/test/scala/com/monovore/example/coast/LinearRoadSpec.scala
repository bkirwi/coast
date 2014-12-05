package com.monovore.example.coast

import com.monovore.coast.machine.{Sample, Machine}
import com.monovore.example.coast.LinearRoad.PlaceAndTime
import org.scalacheck.Prop
import org.specs2.mutable._
import org.specs2.ScalaCheck

class LinearRoadSpec extends Specification with ScalaCheck {

  "The linear road example" should {

    "not charge at times when cars are fast" in {

      val vehicle: LinearRoad.VehicleID = 7L

      val testCase = Machine.compile(LinearRoad.graph)
        .push(LinearRoad.PositionReports, vehicle -> (PlaceAndTime(10, 20) -> 50.0))

      Prop.forAll(Sample.complete(testCase)) { output =>

        output(LinearRoad.TotalTolls)(vehicle).last must_== 0
      }
    }

    "charge at times when cars are slow" in {

      val vehicle: LinearRoad.VehicleID = 7L

      val testCase = Machine.compile(LinearRoad.graph)
        .push(LinearRoad.PositionReports, vehicle -> (PlaceAndTime(10, 20) -> 20.5))

      Prop.forAll(Sample.complete(testCase)) { output =>
        output(LinearRoad.TotalTolls)(vehicle).last must_!= 0
      }
    }
  }
}
