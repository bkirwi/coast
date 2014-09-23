package com.monovore.coast
package machine

import org.scalacheck.{Gen, Prop}
import org.specs2.ScalaCheck
import org.specs2.mutable._

class MachineSpec extends Specification with ScalaCheck {

  // Tweak me
  val InputSize = 10

  "a compiled flow" should {

    val integers = Name[String, Int]("integers")

    "do a basic deterministic transformation" in {

      val doubled = Name[String, Int]("doubled")

      val graph = Graph.register("doubled") {
        Graph.source(integers).map { _ * 2 }
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

    "obey some functor / monad-type laws" in {

      val output = Name[String, Int]("output")

      "x.map(identity) === x" in {

        val original = Graph.register(output.name) { Graph.source(integers) }
        val mapped = Graph.register(output.name) { Graph.source(integers).map(identity) }

        prop { (pairs: Map[String, Seq[Int]]) =>

          equivalent(Messages.from(integers, pairs), original, mapped)

        } set (maxSize = InputSize)
      }

      "x.map(f).map(g) === x.map(f andThen g)" in {

        val f: (Int => Int) = { _ * 2 }
        val g: (Int => Int) = { _ + 6 }

        val original = Graph.register(output.name) { Graph.source(integers).map(f andThen g) }
        val mapped = Graph.register(output.name) { Graph.source(integers).map(f).map(g) }

        prop { (pairs: Map[String, Seq[Int]]) =>

          equivalent(Messages.from(integers, pairs), original, mapped)
        } set (maxSize = InputSize)
      }

      "x.flatMap(f).flatMap(g) === x.flatMap(f andThen { _.flatMap(g) })" in {

        val f: (Int => Seq[Int]) = { x => Seq(x, x) }
        val g: (Int => Seq[Int]) = { x => Seq(x + 6) }

        val nested = Graph.register(output.name) { Graph.source(integers).flatMap(f andThen { _.flatMap(g) }) }
        val chained = Graph.register(output.name) { Graph.source(integers).flatMap(f).flatMap(g) }

        prop { (pairs: Map[String, Seq[Int]]) =>

          equivalent(Messages.from(integers, pairs), nested, chained)
        } set (maxSize = InputSize)
      }

      "x.flatMap(lift) === x" in {

        val noop = Graph.register(output.name) { Graph.source(integers) }
        val mapped = Graph.register(output.name) { Graph.source(integers).flatMap { x => List(x) } }

        prop { (pairs: Map[String, Seq[Int]]) =>

          equivalent(Messages.from(integers, pairs), noop, mapped)
        } set (maxSize = InputSize)
      }
    }
  }

  def equivalent(messages: Messages, one: Graph[_], two: Graph[_]): Prop = {

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
