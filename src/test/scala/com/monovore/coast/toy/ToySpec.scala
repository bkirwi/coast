package com.monovore.coast
package toy

import org.scalacheck.{Gen, Prop}
import org.specs2.ScalaCheck
import org.specs2.mutable._

class ToySpec extends Specification with ScalaCheck {

  def complete(machine: Machine): Gen[Machine] = {
    if (machine.process.isEmpty) Gen.const(machine)
    else Gen.oneOf(machine.process.toSeq).flatMap { p => complete(p.next()) }
  }

  "a toy graph" should {

    val toy = new System

    val boss = Name[Unit, Int]("boss")
    val mapped = Name[Unit, Int]("mapped")

    val graph = for {
      source <- toy.source[Unit, Int](boss.name)
      mapped <- toy.register(mapped.name) { source.map { _ + 1 } }
    } yield ()

    "feed input to a simple system" in prop { (pairs: Seq[(Unit, Int)]) =>

      val machine = Whatever.prepare(toy)(graph).push(boss, pairs: _*)

      machine.state(boss).output must_== pairs.groupByKey
      machine.state(mapped).source(boss) must_== pairs.groupByKey
    }

    "run a simple graph to completion" in prop { (pairs: Seq[(Unit, Int)]) =>

      val machine = Whatever.prepare(toy)(graph)
        .push(boss, pairs: _*)

      Prop.forAll(complete(machine)) { completed =>

        completed.state(boss).output must_== pairs.groupByKey
        completed.state(mapped).source(boss) must beEmpty
        completed.state(mapped).output must_== pairs.map { case (_, n) => () -> (n+1) }.groupByKey
      }
    }

    "have no overlap between groups" in prop { (integers: Seq[Int]) =>

      val boss = Name[Boolean, Int]("boss")
      val mapped = Name[Boolean, Int]("mapped")

      val graph = for {
        source <- toy.source[Boolean, Int](boss.name)
        mapped <- toy.register(mapped.name) { source.map { _ + 1 } }
      } yield ()

      val isEven = integers.map { i => (i % 2 == 0) -> i }

      val machine = Whatever.prepare(toy)(graph)
        .push(boss, isEven: _*)

      Prop.forAll(complete(machine)) { completed =>

        val results = completed.state(mapped).output

        results(true).forall { _ % 2 != 0 } &&
          results(false).forall { _ % 2 == 0 }
      }
    }
  }
}
