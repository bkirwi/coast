package com.monovore.integration.coast

import com.monovore.coast
import com.monovore.coast.flow.{Flow, Topic}
import org.specs2.ScalaCheck
import org.specs2.mutable._

class SafeIntegrationSpec extends Specification with ScalaCheck {

  // unfortunately, a lot of issues show up only with particular timing
  // this helps make results more reproducible
  sequential

  val BigNumber = 15000 // pretty big?

  "a running samza-based job" should {

    import coast.wire.pretty._

    val Foo = Topic[String, Int]("foo")

    val Bar = Topic[String, Int]("bar")

    // table of contents
    passthrough
    flatMap
    transforms
    composition
    merging
    grouping
    cycles

    def passthrough = "pass through data safely" in {

      val graph = Flow.sink(Bar) {
        Flow.source(Foo)
      }

      val inputData = Map(
        "first" -> (1 to BigNumber),
        "second" -> (1 to BigNumber),
        "third" -> (1 to BigNumber)
      )

      val input = Messages().add(Foo, inputData)

      val output = IntegrationTest.fuzz(graph, input).get(Bar)

      output("second") must_== inputData("second")
    }

    def flatMap = "flatMap nicely" in {

      val graph = Flow.sink(Bar) {
        Flow.source(Foo).flatMap { n => Seq.fill(3)(n) }
      }

      val inputData = Map(
        "foo" -> (1 to BigNumber),
        "bar" -> (1 to BigNumber)
      )

      val input = Messages().add(Foo, inputData)

      val output = IntegrationTest.fuzz(graph, input).get(Bar)

      output("bar") must_== inputData("bar").flatMap { n => Seq.fill(3)(n) }
    }

    def transforms = "accumulate state" in {

      val graph = Flow.sink(Bar) {
        Flow.source(Foo).fold(0) { (n, _) => n + 1 }.updates
      }

      val inputData = Map(
        "one" -> (1 to BigNumber),
        "two" -> (1 to BigNumber)
      )

      val input = Messages().add(Foo, inputData)

      val output = IntegrationTest.fuzz(graph, input).get(Bar)

      output("one") must_== inputData("one")
      output("two") must_== inputData("two")
    }

    def composition = "compose well across multiple Samza jobs" in {

      val graph = for {

        once <- Flow.stream("testing") {
          Flow.source(Foo).fold(1) { (n, _) => n + 1 }.updates
        }

        _ <- Flow.sink(Bar) { once.map { _ + 1 } }

      } yield ()

      val inputData = Map(
        "foo" -> (1 to BigNumber),
        "bar" -> (1 to BigNumber)
      )

      val input = Messages().add(Foo, inputData)

      val output = IntegrationTest.fuzz(graph, input).get(Bar)

      output("bar").filter { _ % 100 == 0 } must_== inputData("bar").map { _ + 2 }.filter { _ % 100 == 0 }
    }

    def merging = "do a merge" in {

      val Foo2 = Topic[String, Int]("foo-2")

      val graph = Flow.sink(Bar) {
        Flow.merge(
          "foo-1" -> Flow.source(Foo),
          "foo-2" -> Flow.source(Foo2)
        )
      }

      val input = Messages
        .add(Foo, Map("test" -> (1 to BigNumber by 2)))
        .add(Foo2, Map("test" -> (2 to BigNumber by 2)))

      val output = IntegrationTest.fuzz(graph, input).get(Bar)

      output("test").filter { _ % 2 == 1 } must_== (1 to BigNumber by 2)
      output("test").filter { _ % 2 == 0 } must_== (2 to BigNumber by 2)
    }

    def grouping = "regroup" in {

      val graph = for {

        grouped <- Flow.stream("grouped") {
          Flow.source(Foo).groupBy { n => (n % 10).toString }
        }

        _ <- Flow.sink(Bar) { grouped }
      } yield ()

      val input = Messages
        .add(Foo, Map(
          "first" -> (0 until BigNumber by 3),
          "second" -> (1 until BigNumber by 3),
          "third" -> (2 until BigNumber by 3)
        ))

      val output = IntegrationTest.fuzz(graph, input).get(Bar)

      forall(output) { case (key, values) =>
        val remainder = key.toInt
        values.sorted must_== (remainder until BigNumber by 10)
      }

      output must not be empty
    }

    def cycles = "handle cycles" in {

      val graph = for {

        looped <- Flow.cycle[String, Int]("looped") { looped =>

          val merged = Flow.merge(
            "foo" -> Flow.source(Foo),
            "loop" -> looped.filter { _ % 2 == 0 }
          )

          merged.map { _ + 1 }
        }

        _ <- Flow.sink(Bar) { looped }

      } yield ()

      val input = Messages
        .add(Foo, Map("test" -> (1 to BigNumber by 2)))

      val output = IntegrationTest.fuzz(graph, input).get(Bar)

      output("test").sorted must_== (2 to (BigNumber+1))
    }
  }
}

