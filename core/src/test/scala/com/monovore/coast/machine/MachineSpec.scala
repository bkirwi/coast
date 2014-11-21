package com.monovore.coast
package machine

import com.monovore.coast
import com.twitter.algebird.Semigroup
import org.scalacheck.Prop
import org.specs2.ScalaCheck
import org.specs2.mutable._

class MachineSpec extends Specification with ScalaCheck {

  // Tweak me
  val InputSize = 10

  import coast.wire.ugly._

  "a compiled flow" should {

    val integers = Name[String, Int]("integers")

    val output = Name[String, Int]("output")

    "do a basic deterministic transformation" in {

      val doubled = Name[String, Int]("doubled")

      val graph = coast.label("doubled") {
        coast.source(integers).map { _ * 2 }
      }

      val compiled = Machine.compile(graph)
        .push(integers, "foo" -> 1, "bar" -> 2, "foo" -> 3)

      Prop.forAll(Sample.complete(compiled)) { output =>
        output(doubled) must_== Map(
          "foo" -> Seq(2, 6),
          "bar" -> Seq(4)
        )
      }
    }

    "support all operations" in {

      "pool" in {

        val graph = coast.sink(output) {
          coast.source(integers).fold(0) { _ + _ }.stream
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

        val integers2 = Name[String, Int]("integers-2")

        val graph = coast.sink(output) {

          coast.merge(
            "ints" -> coast.source(integers),
            "more" -> coast.source(integers2)
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

          grouped <- coast.label("grouped") {
            coast.source(integers).groupBy { n => (n % 2 == 0).toString}
          }

          _ <- coast.sink(output) { grouped }
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

        val original = coast.sink(output) { coast.source(integers) }
        val mapped = coast.sink(output) { coast.source(integers).map(identity) }

        prop { (pairs: Map[String, Seq[Int]]) =>

          equivalent(Messages.from(integers, pairs), original, mapped)

        } set (maxSize = InputSize)
      }

      "x.map(f).map(g) === x.map(f andThen g)" in {

        val f: (Int => Int) = { _ * 2 }
        val g: (Int => Int) = { _ + 6 }

        val original = coast.sink(output) { coast.source(integers).map(f andThen g) }
        val mapped = coast.sink(output) { coast.source(integers).map(f).map(g) }

        prop { (pairs: Map[String, Seq[Int]]) =>

          equivalent(Messages.from(integers, pairs), original, mapped)
        } set (maxSize = InputSize)
      }

      "stream.flatMap(f).flatMap(g) === stream.flatMap(f andThen { _.flatMap(g) })" in {

        val f: (Int => Seq[Int]) = { x => Seq(x, x) }
        val g: (Int => Seq[Int]) = { x => Seq(x + 6) }

        val nested = coast.sink(output) { coast.source(integers).flatMap(f andThen { _.flatMap(g) }) }
        val chained = coast.sink(output) { coast.source(integers).flatMap(f).flatMap(g) }

        prop { (pairs: Map[String, Seq[Int]]) =>

          equivalent(Messages.from(integers, pairs), nested, chained)
        } set (maxSize = InputSize)
      }

      "stream.flatMap(lift) === stream" in {

        val noop = coast.sink(output) { coast.source(integers) }
        val mapped = coast.sink(output) { coast.source(integers).flatMap { x => List(x) } }

        prop { (pairs: Map[String, Seq[Int]]) =>

          equivalent(Messages.from(integers, pairs), noop, mapped)
        } set (maxSize = InputSize)
      }

      "pool.map(identity) === pool" in {

        val pool = coast.source(integers).pool(0)

        val original = coast.sink(output) { pool.stream }
        val mapped = coast.sink(output) { pool.map(identity).stream }

        prop { (pairs: Map[String, Seq[Int]]) =>

          equivalent(Messages.from(integers, pairs), original, mapped)

        } set (maxSize = InputSize)
      }

      "pool.map(f).map(g) === pool.map(f andThen g)" in {

        val f: (Int => Int) = { _ * 2 }
        val g: (Int => Int) = { _ + 6 }

        val pool = coast.source(integers).pool(0)

        val original = coast.sink(output) { pool.map(f andThen g).stream }
        val mapped = coast.sink(output) { pool.map(f).map(g).stream }

        prop { (pairs: Map[String, Seq[Int]]) =>

          equivalent(Messages.from(integers, pairs), original, mapped)
        } set (maxSize = InputSize)
      }
    }
  }

  def equivalent(messages: Messages, one: Flow[_], two: Flow[_]): Prop = {

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
