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

      machine.state(boss).output must_== pairs
      machine.state(mapped).input(boss) must_== pairs
    }

    "run a simple graph to completion" in prop { (pairs: Seq[(Unit, Int)]) =>

      val machine = Whatever.prepare(toy)(graph)
        .push(boss, pairs: _*)

      Prop.forAll(complete(machine)) { completed =>

        machine.state(boss).output must_== pairs
        machine.state(mapped).input(boss) must_== Seq.empty
        machine.state(mapped).output must_== pairs.map { case (_, n) => () -> (n+1) }
      }
    }
  }
}
