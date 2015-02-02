package com.monovore.coast
package machine

import com.monovore.coast
import coast.flow
import com.monovore.coast.flow.Topic
import com.monovore.coast.model.Graph

import com.twitter.algebird.Semigroup
import org.scalacheck.Prop
import org.specs2.ScalaCheck
import org.specs2.mutable._

class MachineSpec extends Specification with ScalaCheck {

  // Tweak me
  val InputSize = 10

  import coast.wire.ugly._

  "a compiled flow" should {

    val integers = Topic[String, Int]("integers")

    val output = Topic[String, Int]("output")

    "do a basic deterministic transformation" in {

      val doubled = Topic[String, Int]("doubled")

      val graph = flow.stream("doubled") {
        flow.source(integers).map { _ * 2 }
      }

      val input = Messages.from(integers, Map(
        "foo" -> Seq(1, 3), "bar" -> Seq(2)
      ))

      val compiled = Machine.compile(graph).push(input)

      Prop.forAll(Sample.complete(compiled)) { output =>
        output(doubled) must_== Map(
          "foo" -> Seq(2, 6),
          "bar" -> Seq(4)
        )
      }
    }

    "support all operations" in {

      "pool" in {

        val graph = flow.sink(output) {
          flow.source(integers).fold(0) { _ + _ }.updates
        }

        prop { input: Map[String, Seq[Int]] =>

          val expected = input
            .mapValues { _.scanLeft(0)(_ + _).tail }
            .filter { case (_ -> v) => v.nonEmpty }

          val compiled = Machine.compile(graph).push(Messages.from(integers, input))

          Prop.forAll(Sample.complete(compiled)) { messages =>
            messages(output) must_== expected
          }
        } set (maxSize = InputSize)
      }

      "merge" in {

        val integers2 = Topic[String, Int]("integers-2")

        val graph = flow.sink(output) {

          flow.merge(
            "ints" -> flow.source(integers),
            "more" -> flow.source(integers2)
          )
        }

        prop { (input: Map[String, Seq[Int]], input2: Map[String, Seq[Int]]) =>

          val expected = Semigroup.plus(input, input2)
            .filter { case (_ -> v) => v.nonEmpty }
            .mapValues { _.sorted }

          val compiled = Machine.compile(graph)
            .push(Messages.from(integers, input))
            .push(Messages.from(integers2, input2))

          Prop.forAll(Sample.complete(compiled)) { messages =>
            messages(output).mapValues { _.sorted } must_== expected
          }
        } set (maxSize = InputSize)
      }

      "groupBy" in {

        val graph = for {

          grouped <- flow.stream("grouped") {
            flow.source(integers).groupBy { n => (n % 2 == 0).toString}
          }

          _ <- flow.sink(output) { grouped }
        } yield ()

        prop { input: Map[String, Seq[Int]] =>

          val expected = input.values.toSeq.flatten
            .groupBy { n => (n % 2 == 0).toString }
            .mapValues { _.sorted }

          val compiled = Machine.compile(graph)
            .push(Messages.from(integers, input))

          Prop.forAll(Sample.complete(compiled)) { messages =>
            messages(output).mapValues { _.sorted } must_== expected
          }
        } set (maxSize = InputSize)
      }
    }

    "obey some functor / monad-type laws" in {

      "x.map(identity) === x" in {

        val original = flow.sink(output) { flow.source(integers) }
        val mapped = flow.sink(output) { flow.source(integers).map(identity) }

        prop { (pairs: Map[String, Seq[Int]]) =>

          equivalent(Messages.from(integers, pairs), original, mapped)

        } set (maxSize = InputSize)
      }

      "x.map(f).map(g) === x.map(f andThen g)" in {

        val f: (Int => Int) = { _ * 2 }
        val g: (Int => Int) = { _ + 6 }

        val original = flow.sink(output) { flow.source(integers).map(f andThen g) }
        val mapped = flow.sink(output) { flow.source(integers).map(f).map(g) }

        prop { (pairs: Map[String, Seq[Int]]) =>

          equivalent(Messages.from(integers, pairs), original, mapped)
        } set (maxSize = InputSize)
      }

      "stream.flatMap(f).flatMap(g) === stream.flatMap(f andThen { _.flatMap(g) })" in {

        val f: (Int => Seq[Int]) = { x => Seq(x, x) }
        val g: (Int => Seq[Int]) = { x => Seq(x + 6) }

        val nested = flow.sink(output) { flow.source(integers).flatMap(f andThen { _.flatMap(g) }) }
        val chained = flow.sink(output) { flow.source(integers).flatMap(f).flatMap(g) }

        prop { (pairs: Map[String, Seq[Int]]) =>

          equivalent(Messages.from(integers, pairs), nested, chained)
        } set (maxSize = InputSize)
      }

      "stream.flatMap(lift) === stream" in {

        val noop = flow.sink(output) { flow.source(integers) }
        val mapped = flow.sink(output) { flow.source(integers).flatMap { x => List(x) } }

        prop { (pairs: Map[String, Seq[Int]]) =>

          equivalent(Messages.from(integers, pairs), noop, mapped)
        } set (maxSize = InputSize)
      }

      "pool.map(identity) === pool" in {

        val pool = flow.source(integers).latestOr(0)

        val original = flow.sink(output) { pool.updates }
        val mapped = flow.sink(output) { pool.map(identity).updates }

        prop { (pairs: Map[String, Seq[Int]]) =>

          equivalent(Messages.from(integers, pairs), original, mapped)

        } set (maxSize = InputSize)
      }

      "pool.map(f).map(g) === pool.map(f andThen g)" in {

        val f: (Int => Int) = { _ * 2 }
        val g: (Int => Int) = { _ + 6 }

        val pool = flow.source(integers).latestOr(0)

        val original = flow.sink(output) { pool.map(f andThen g).updates }
        val mapped = flow.sink(output) { pool.map(f).map(g).updates }

        prop { (pairs: Map[String, Seq[Int]]) =>

          equivalent(Messages.from(integers, pairs), original, mapped)
        } set (maxSize = InputSize)
      }
    }
  }

  def equivalent(messages: Messages, one: Graph, two: Graph): Prop = {

    prop { swap: Boolean =>

      val (sample, prove) = {

        val oneM = Machine.compile(one)
        val twoM = Machine.compile(two)

        if (swap) (twoM, oneM) else (oneM, twoM)
      }

      Prop.forAll(Sample.complete(sample.push(messages))) { output =>
        Sample.canProduce(prove.push(messages), output)
      }
    }
  }
}
