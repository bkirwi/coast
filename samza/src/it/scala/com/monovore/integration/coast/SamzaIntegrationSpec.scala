package com.monovore.integration.coast

import com.monovore.coast
import com.monovore.coast.format.WireFormat
import org.specs2.ScalaCheck
import org.specs2.mutable._

class SamzaIntegrationSpec extends Specification with ScalaCheck {

  // unfortunately, a lot of issues show up only with particular timing
  // this helps make results more reproducible
  sequential

  val BigNumber = 80000 // pretty big?

  "a running samza-based job" should {

    import coast.format.pretty._

    val Foo = coast.Name[String, Int]("foo")

    val Bar = coast.Name[String, Int]("bar")

    "pass through data safely" in {

      val flow = coast.sink(Bar) {
        coast.source(Foo)
      }

      val inputData = Map("bar" -> (1 to BigNumber))

      val input = Messages().add(Foo, inputData)

      val output = IntegrationTest.fuzz(flow, input).get(Bar)

      output("bar") must_== inputData("bar")
    }

    "flatMap nicely" in {

      val flow = coast.sink(Bar) {
        coast.source(Foo).flatMap { n => Seq.fill(3)(n) }
      }

      val inputData = Map("bar" -> (1 to BigNumber))

      val input = Messages().add(Foo, inputData)

      val output = IntegrationTest.fuzz(flow, input).get(Bar)

      output("bar") must_== inputData("bar").flatMap { n => Seq.fill(3)(n) }
    }

    "accumulate state" in {

      val flow = coast.sink(Bar) {
        coast.source(Foo).fold(0) { (n, _) => n + 1 }.stream
      }

      val inputData = Map("bar" -> (1 to BigNumber))

      val input = Messages().add(Foo, inputData)

      val output = IntegrationTest.fuzz(flow, input).get(Bar)

      output("bar").size must_== inputData("bar").size

      output("bar") must_== inputData("bar")
    }

    "compose well across multiple Samza jobs" in {

      val flow = for {

        once <- coast.label("testing") {
          coast.source(Foo).fold(1) { (n, _) => n + 1 }.stream
        }

        _ <- coast.sink(Bar) { once.map { _ + 1 } }

      } yield ()

      val inputData = Map("bar" -> (1 to BigNumber))

      val input = Messages().add(Foo, inputData)

      val output = IntegrationTest.fuzz(flow, input).get(Bar)

      output("bar") must_== inputData("bar").map { _ + 2 }
    }

    "do a merge" in {

      val Foo2 = coast.Name[String, Int]("foo-2")

      val flow = coast.sink(Bar) { coast.merge(coast.source(Foo), coast.source(Foo2)) }

      val input = Messages
        .add(Foo, Map("test" -> (1 to BigNumber by 2)))
        .add(Foo2, Map("test" -> (2 to BigNumber by 2)))

      val output = IntegrationTest.fuzz(flow, input).get(Bar)

      output("test").filter { _ % 2 == 1 } must_== (1 to BigNumber by 2)
      output("test").filter { _ % 2 == 0 } must_== (2 to BigNumber by 2)
    }
  }
}

case class Messages(messages: Map[String, Map[Seq[Byte], Seq[Seq[Byte]]]] = Map.empty) {

  def add[A : WireFormat, B : WireFormat](name: coast.Name[A,B], messages: Map[A, Seq[B]]): Messages = {

    val formatted = messages.map { case (k, vs) =>
      WireFormat.write(k).toSeq -> vs.map { v => WireFormat.write(v).toSeq }
    }

    Messages(this.messages.updated(name.name, formatted))
  }

  def get[A : WireFormat, B : WireFormat](name: coast.Name[A, B]): Map[A, Seq[B]] = {

    val data = messages.getOrElse(name.name, Map.empty)

    data.map { case (k, vs) =>
      WireFormat.read[A](k.toArray) -> vs.map { v => WireFormat.read[B](v.toArray) }
    }.withDefaultValue(Seq.empty[B])
  }
}

object Messages extends Messages(Map.empty)

