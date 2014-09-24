package com.monovore.coast
package example

import org.specs2.mutable._

class ExampleSpec extends Specification {

  "a dswoosh implementation" should {

    case class Entity(item: String, name: String, minPrice: Double)

    val entities = Name[String, Entity]("entities")
    val merged = Name[String, Entity]("merged")

    val graph = for {
      bucketed <- Graph.label("bucketed") {
        Graph.merge(Graph.source(entities), Graph.source(merged))
          .flatMap { entity =>
            Seq(entity.item -> entity)
          }
          .groupByKey
      }
      _ <- Graph.sink(merged) {
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

  "a denormalized indexing implementation" should {

    case class Club()

    case class Person(clubId: Int = 0)

    val clubs = Name[Int, Club]("clubs")

    // TODO: make keys visible everywhere
    val people = Name[Int, Int -> Person]("people")

    val graph = for {

      peoplePool <- Graph.label("people-pool") {
        Graph.source(people)
          .map { case pair @ (_, person) => person.clubId -> pair }
          .groupByKey
          .pool(Map.empty[Int, Person]) { _ + _ }
      }

      clubPool <- Graph.label("club-pool") {
        Graph.source(clubs).latestOr(Club())
      }

      // JOIN!
    } yield ()

    "do nothing" in true
  }
}
