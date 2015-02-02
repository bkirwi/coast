package com.monovore.integration.coast

import com.monovore.coast
import com.monovore.coast.flow
import com.monovore.coast.flow.Topic
import com.monovore.coast.wire.{Partitioner, BinaryFormat}
import org.scalacheck.{Prop, Gen}
import org.specs2.ScalaCheck
import org.specs2.mutable._

class SamzaIntegrationSpec extends Specification with ScalaCheck {

  // unfortunately, a lot of issues show up only with particular timing
  // this helps make results more reproducible
  sequential

  val BigNumber = 60000 // pretty big?

  "a running samza-based job" should {

    import coast.wire.pretty._

    val Foo = Topic[String, Int]("foo")

    val Bar = Topic[String, Int]("bar")

    "pass through data safely" in {

      val graph = flow.sink(Bar) {
        flow.source(Foo)
      }

      val inputData = Map(
        "foo" -> (1 to BigNumber),
        "bar" -> (1 to BigNumber),
        "baz" -> (1 to BigNumber)
      )

      val input = Messages().add(Foo, inputData)

      val output = IntegrationTest.fuzz(graph, input).get(Bar)

      output("bar") must_== inputData("bar")
    }

    "flatMap nicely" in {

      val graph = flow.sink(Bar) {
        flow.source(Foo).flatMap { n => Seq.fill(3)(n) }
      }

      val inputData = Map(
        "foo" -> (1 to BigNumber),
        "bar" -> (1 to BigNumber)
      )

      val input = Messages().add(Foo, inputData)

      val output = IntegrationTest.fuzz(graph, input).get(Bar)

      output("bar") must_== inputData("bar").flatMap { n => Seq.fill(3)(n) }
    }

    "accumulate state" in {

      val graph = flow.sink(Bar) {
        flow.source(Foo).fold(0) { (n, _) => n + 1 }.updates
      }

      val inputData = Map(
        "foo" -> (1 to BigNumber),
        "bar" -> (1 to BigNumber)
      )

      val input = Messages().add(Foo, inputData)

      val output = IntegrationTest.fuzz(graph, input).get(Bar)

      output("bar").size must_== inputData("bar").size

      output("bar") must_== inputData("bar")
    }

    "compose well across multiple Samza jobs" in {

      val graph = for {

        once <- flow.stream("testing") {
          flow.source(Foo).fold(1) { (n, _) => n + 1 }.updates
        }

        _ <- flow.sink(Bar) { once.map { _ + 1 } }

      } yield ()

      val inputData = Map(
        "foo" -> (1 to BigNumber),
        "bar" -> (1 to BigNumber)
      )

      val input = Messages().add(Foo, inputData)

      val output = IntegrationTest.fuzz(graph, input).get(Bar)

      output("bar") must_== inputData("bar").map { _ + 2 }
    }

    "do a merge" in {

      val Foo2 = Topic[String, Int]("foo-2")

      val graph = flow.sink(Bar) {
        flow.merge(
          "foo-1" -> flow.source(Foo),
          "foo-2" -> flow.source(Foo2)
        )
      }

      val input = Messages
        .add(Foo, Map("test" -> (1 to BigNumber by 2)))
        .add(Foo2, Map("test" -> (2 to BigNumber by 2)))

      val output = IntegrationTest.fuzz(graph, input).get(Bar)

      output("test").filter { _ % 2 == 1 }.grouped(2).foreach { case Seq(a, b) => (a + 2) must_== b }
      output("test").filter { _ % 2 == 0 }.grouped(2).foreach { case Seq(a, b) => (a + 2) must_== b }

      output("test").filter { _ % 2 == 1 } must_== (1 to BigNumber by 2)
      output("test").filter { _ % 2 == 0 } must_== (2 to BigNumber by 2)
    }

    "regroup" in {

      val graph = for {

        grouped <- flow.stream("grouped") {
          flow.source(Foo).groupBy { n => (n % 10).toString }
        }

        _ <- flow.sink(Bar) { grouped }
      } yield ()

      val input = Messages
        .add(Foo, Map("test" -> (1 to BigNumber)))

      val output = IntegrationTest.fuzz(graph, input).get(Bar)

      for {
        i <- 0 until 10
        n <- output(i.toString)
      } {
        (n % 10) must_== i
      }

      output must not be empty
    }

    "handle cycles" in {

      val graph = for {

        looped <- flow.cycle[String, Int]("looped") { looped =>

          val merged = flow.merge(
            "foo" -> flow.source(Foo),
            "loop" -> looped.filter { _ % 2 == 0 }
          )

          merged.map { _ + 1 }
        }

        _ <- flow.sink(Bar) { looped }

      } yield ()

      val input = Messages
        .add(Foo, Map("test" -> (1 to BigNumber by 2)))

      val output = IntegrationTest.fuzz(graph, input).get(Bar)

      output("test").sorted must_== (2 to (BigNumber+1))
    }
  }

  "a 'simple' samza-backed job" should {

    "count words" in {

      import com.monovore.example.coast.WordCount._
      import coast.wire.ugly._

      val words = Gen.oneOf("testing", "scandal", "riviera", "salad", "Thursday")
      val sentences = Gen.listOf(words).map { _.mkString(" ") }
      val sentenceList = Seq.fill(100)(sentences.sample).flatten.toSeq

      val input = Messages
        .add(Sentences, Map(0L -> sentenceList))

      val output = IntegrationTest.fuzz(graph, input, simple = true).get(WordCounts)

      val testingCount = sentenceList
        .flatMap { _.split(" ") }
        .count { _ == "testing" }

      output("testing") must_== (1 to testingCount)
    }
  }
}

case class Messages(messages: Map[String, Map[Seq[Byte], (Int => Int, Seq[Seq[Byte]])]] = Map.empty) {

  def add[A : BinaryFormat : Partitioner, B : BinaryFormat](name: Topic[A,B], messages: Map[A, Seq[B]]): Messages = {

    val formatted = messages.map { case (k, vs) =>
      val pn: (Int => Int) = implicitly[Partitioner[A]].partition(k, _)
      BinaryFormat.write(k).toSeq -> (pn, vs.map { v => BinaryFormat.write(v).toSeq })
    }

    Messages(this.messages.updated(name.name, formatted))
  }

  def get[A : BinaryFormat, B : BinaryFormat](name: Topic[A, B]): Map[A, Seq[B]] = {

    val data = messages.getOrElse(name.name, Map.empty)

    data.map { case (k, (_, vs) ) =>
      BinaryFormat.read[A](k.toArray) -> vs.map { v => BinaryFormat.read[B](v.toArray) }
    }.withDefaultValue(Seq.empty[B])
  }
}

object Messages extends Messages(Map.empty)

