package com.monovore.example.coast

import com.monovore.coast.flow.{Flow, Topic}
import com.monovore.coast.machine.{Machine, Messages, Sample}
import org.scalacheck.{Gen, Prop}
import org.specs2.ScalaCheck
import org.specs2.matcher.Parameters
import org.specs2.mutable._

class ConnectedComponentsSpec extends Specification with ScalaCheck {

  implicit val scalacheck = Parameters(maxSize = 20)

  "connected-components finder" should {

    import com.monovore.coast.wire.ugly._

    val Edges = Topic[Long, Long]("edges")

    val Components = Topic[Long, Long]("more-components")

    val graph = Flow.sink(Components) {
      ConnectedComponents.findComponents(Flow.source(Edges))
    }

    "find correct label for linear graph" in {

      val input = Messages.from(Edges, Map(
        1L -> Seq(0L), 2L -> Seq(1L), 3L -> Seq(2L)
      ))

      val testCase = Machine.compile(graph).push(input)

      Prop.forAll(Sample.complete(testCase)) { output =>

        output(Components)(2).last must_== 0
        output(Components)(1).last must_== 0
      }
    }

    "label a small random graph correctly" in {

      val gen = for {
        x <- Gen.choose(0L, 9L)
        n <- Gen.choose(1, 3)
        ys <- Gen.listOfN(n, Gen.choose(0L, x-1))
      } yield (x -> ys)

      Prop.forAll(Gen.mapOf(gen)) { inputs =>

        val input = Messages.from(Edges, inputs)
        val testCase = Machine.compile(graph).push(input)

        Prop.forAll(Sample.complete(testCase)) { output =>

          def label(id: Long) =
            output(Components).get(id)
              .flatMap { _.lastOption }
              .getOrElse(id)

          // if two nodes were connected, they should get the same label
          foreach(inputs) { case (source, targets) =>
            foreach(targets) { target =>
              label(source) must_== label(target)
            }
          }

          foreach(output(Components)) { case (source, labels) =>
            labels.reverse must beSorted
          }
        }
      }
    }
  }
}
