package com.monovore.coast
package toy

import org.specs2.ScalaCheck
import org.specs2.mutable._

class ToySpec extends Specification with ScalaCheck {

  "a toy graph" should {

    val toy = new System

    "map" in {

      val boss = Name[Unit, Int]("boss")

      val graph = for {
        source <- toy.source[Unit, Int]("boss")
        mapped <- toy.register("mapped") { source.map { _ + 1 } }
      } yield ()

      val machine =
        Whatever.prepare(toy)(graph)
          .push(boss, () -> 1, () -> 2)

      machine.process.head.next

      prop { x: Int => x + x == 2 }

      true
    }
  }
}
