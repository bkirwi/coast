package com.monovore.integration.coast

import com.monovore.coast
import com.monovore.coast.wire.{Partitioner, BinaryFormat}
import org.specs2.ScalaCheck
import org.specs2.mutable._

class SamzaIntegrationSpec extends Specification with ScalaCheck {

  // unfortunately, a lot of issues show up only with particular timing
  // this helps make results more reproducible
  sequential

  val BigNumber = 120000 // pretty big?

  "a running samza-based job" should {

    import coast.wire.pretty._

    val Foo = coast.Name[String, Int]("foo")

    val Bar = coast.Name[String, Int]("bar")

    "pass through data safely" in {

      val flow = coast.sink(Bar) {
        coast.source(Foo)
      }

      val inputData = Map(
        "foo" -> (1 to BigNumber),
        "bar" -> (1 to BigNumber),
        "baz" -> (1 to BigNumber)
      )

      val input = Messages().add(Foo, inputData)

      val output = IntegrationTest.fuzz(flow, input).get(Bar)

      output("bar") must_== inputData("bar")
    }

    "flatMap nicely" in {

      val flow = coast.sink(Bar) {
        coast.source(Foo).flatMap { n => Seq.fill(3)(n) }
      }

      val inputData = Map(
        "foo" -> (1 to BigNumber),
        "bar" -> (1 to BigNumber)
      )

      val input = Messages().add(Foo, inputData)

      val output = IntegrationTest.fuzz(flow, input).get(Bar)

      output("bar") must_== inputData("bar").flatMap { n => Seq.fill(3)(n) }
    }

    "accumulate state" in {

      val flow = coast.sink(Bar) {
        coast.source(Foo).fold(0) { (n, _) => n + 1 }.updateStream
      }

      val inputData = Map(
        "foo" -> (1 to BigNumber),
        "bar" -> (1 to BigNumber)
      )

      val input = Messages().add(Foo, inputData)

      val output = IntegrationTest.fuzz(flow, input).get(Bar)

      output("bar").size must_== inputData("bar").size

      output("bar") must_== inputData("bar")
    }

    "compose well across multiple Samza jobs" in {

      val flow = for {

        once <- coast.stream("testing") {
          coast.source(Foo).fold(1) { (n, _) => n + 1 }.updateStream
        }

        _ <- coast.sink(Bar) { once.map { _ + 1 } }

      } yield ()

      val inputData = Map(
        "foo" -> (1 to BigNumber),
        "bar" -> (1 to BigNumber)
      )

      val input = Messages().add(Foo, inputData)

      val output = IntegrationTest.fuzz(flow, input).get(Bar)

      output("bar") must_== inputData("bar").map { _ + 2 }
    }

    "do a merge" in {

      val Foo2 = coast.Name[String, Int]("foo-2")

      val flow = coast.sink(Bar) {
        coast.merge(
          "foo-1" -> coast.source(Foo),
          "foo-2" -> coast.source(Foo2)
        )
      }

      val input = Messages
        .add(Foo, Map("test" -> (1 to BigNumber by 2)))
        .add(Foo2, Map("test" -> (2 to BigNumber by 2)))

      val output = IntegrationTest.fuzz(flow, input).get(Bar)

      output("test").filter { _ % 2 == 1 }.grouped(2).foreach { case Seq(a, b) => (a + 2) must_== b }
      output("test").filter { _ % 2 == 0 }.grouped(2).foreach { case Seq(a, b) => (a + 2) must_== b }

      output("test").filter { _ % 2 == 1 } must_== (1 to BigNumber by 2)
      output("test").filter { _ % 2 == 0 } must_== (2 to BigNumber by 2)
    }

    "regroup" in {

      val flow = for {

        grouped <- coast.stream("grouped") {
          coast.source(Foo).groupBy { n => (n % 10).toString }
        }

        _ <- coast.sink(Bar) { grouped }
      } yield ()

      val input = Messages
        .add(Foo, Map("test" -> (1 to BigNumber)))

      val output = IntegrationTest.fuzz(flow, input).get(Bar)

      for {
        i <- 0 until 10
        n <- output(i.toString)
      } {
        (n % 10) must_== i
      }

      output must not be empty
    }
  }
}

case class Messages(messages: Map[String, Map[Seq[Byte], (Int, Seq[Seq[Byte]])]] = Map.empty) {

  def add[A : BinaryFormat : Partitioner, B : BinaryFormat](name: coast.Name[A,B], messages: Map[A, Seq[B]]): Messages = {

    val formatted = messages.map { case (k, vs) =>
      BinaryFormat.write(k).toSeq -> (Partitioner.hash(k).asInt, vs.map { v => BinaryFormat.write(v).toSeq })
    }

    Messages(this.messages.updated(name.name, formatted))
  }

  def get[A : BinaryFormat, B : BinaryFormat](name: coast.Name[A, B]): Map[A, Seq[B]] = {

    val data = messages.getOrElse(name.name, Map.empty)

    data.map { case (k, (_, vs) ) =>
      BinaryFormat.read[A](k.toArray) -> vs.map { v => BinaryFormat.read[B](v.toArray) }
    }.withDefaultValue(Seq.empty[B])
  }
}

object Messages extends Messages(Map.empty)

