package com.monovore.example.coast

import com.monovore.coast.flow.{Flow, Topic}
import com.monovore.coast.machine.{Machine, Messages, Sample}
import org.scalacheck.Prop
import org.specs2.ScalaCheck
import org.specs2.mutable._

class ConnectedComponentsSpec extends Specification with ScalaCheck {

  "connected-components finder" should {

    import com.monovore.coast.wire.ugly._

    val Edges = Topic[Int, Int]("edges")

    val Components = Topic[Int, Int]("more-components")

    val graph = Flow.sink(Components) {
      ConnectedComponents.canonicalize(Flow.source(Edges))
    }

    "find correct label for linear graph" in {

      val input = Messages.from(Edges, Map(
        1 -> Seq(0), 2 -> Seq(1), 3 -> Seq(2)
      ))

      val testCase = Machine.compile(graph).push(input)

      Prop.forAll(Sample.complete(testCase)) { output =>

        output(Components)(2).last must_== 0
        output(Components)(1).last must_== 0
      }
    }
  }
}
