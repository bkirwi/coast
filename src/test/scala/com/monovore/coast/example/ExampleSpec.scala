package com.monovore.coast
package example

import org.specs2.mutable._

class ExampleSpec extends Specification {

  "a dswoosh implementation" should {

    case class Entity(name: String, tags: Set[String], priceRange: (Double, Double))

    def buckets(entity: Entity): Seq[Int] = {
      val (low, high) = entity.priceRange
      low.toInt to high.toInt
    }

    val entities = Name[String, Entity]("entities")
    val merged = Name[Int, Entity]("merged")

    val graph = for {
      bucketed <- Graph.label("bucketed") {
        Graph.source(entities)
          .flatMap { entity => buckets(entity).map { _ -> entity } }
          .groupByKey
      }
      _ <- Graph.sink(merged) {
        Graph.merge(bucketed, Graph.source(merged))
          .fold(Set.empty[Entity] -> (None: Option[Entity])) { (state, next) =>
            val (set, last) = state
            // TODO: do merge
            (set + next) -> None
          }
          .stream
          .map { _._2 }
          .flatten
          .flatMap { entity => buckets(entity).map { _ -> entity } }
          .groupByKey
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

      peoplePool <- Graph.labelP("people-pool") {
        Graph.source(people)
          .map { case pair @ (_, person) => person.clubId -> pair }
          .groupByKey
          .fold(Map.empty[Int, Person]) { _ + _ }
      }

      clubPool <- Graph.labelP("club-pool") {
        Graph.source(clubs).latestOr(Club())
      }

      x = {
        (clubPool join peoplePool join peoplePool)
          .map { case (club -> members -> members2) =>
            "GREAT"
          }
      }
    } yield ()

    "do nothing" in true
  }
}
