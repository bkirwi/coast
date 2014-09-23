package com.monovore.coast
package example

import org.specs2.mutable._

class ExampleSpec extends Specification {

  "a dswoosh implementation" should {

    case class Entity(item: String, name: String, minPrice: Double)

    val entities = Name[String, Entity]("entities")
    val merged = Name[String, Entity]("merged")

    for {
      bucketed <- Graph.register("bucketed") {
        Graph.merge(Graph.source(entities), Graph.source(merged))
          .flatMap { entity =>
          Seq(entity.item -> entity)
        }
          .groupByKey
      }
      _ <- Graph.register(merged.name) {

        bucketed
          .pool(Set.empty[Entity] -> (None: Option[Entity])) { (state, next) =>
            val (set, last) = state
            // do merge
            (set + next) -> None
          }
          .map { _._2 }
          .flatten
      }
    } yield ()

    "do nothing" in true
  }
}
