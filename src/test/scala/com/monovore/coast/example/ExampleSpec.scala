package com.monovore.coast
package example

import org.specs2.mutable._

class ExampleSpec extends Specification {

  "a distributed entity resolution flow" should {

    case class Entity(name: String, tags: Set[String], priceRange: (Double, Double))

    def scope(entity: Entity): Seq[Int] = {
      val (low, high) = entity.priceRange
      low.toInt to high.toInt
    }

    def matches(a: Entity, b: Entity): Boolean = {
      a == b // TODO: real matching
    }

    def merge(a: Entity, b: Entity): Entity = {
      val (mins, maxs) = Seq(a.priceRange, b.priceRange).unzip
      Entity(a.name, a.tags ++ b.tags, mins.min -> maxs.max)
    }

    val entities = Name[String, Entity]("entities")

    val merged = Name[Int, Entity]("merged")

    val graph = for {

      // Group input into the correct scopes
      scoped <- Graph.label("bucketed") {
        Graph.source(entities)
          .flatMap { entity => scope(entity).map { _ -> entity } }
          .groupByKey
      }

      // Take all 'new' entities and (re)merge them
      // Note the circular definition here, as entities created by the merge
      // get piped back in.
      _ <- Graph.sink(merged) {
        Graph.merge(scoped, Graph.source(merged))
          .transform(Set.empty[Entity]) { (entities, nextEntity) =>
            // TODO: real swoosh
            (entities + nextEntity) -> Seq.empty
          }
          .flatMap { entity => scope(entity).map { _ -> entity } }
          .groupByKey
      }
    } yield ()

    "do nothing" in true
  }

  "a denormalized indexing implementation" should {

    // TODO: less dumb example
    case class Club()
    case class Person(clubId: Int = 0)

    // TODO: make keys visible everywhere
    val people = Name[Int, Person]("people")
    val clubs = Name[Int, Club]("clubs")
    val both = Name[Int, Club -> Set[Person]]("both")

    val graph = for {

      // Roll up 'people' under their club id
      peoplePool <- Graph.label("people-pool") {
        Graph.source(people)
          .withKeys.map { key => person => person.clubId -> (key -> person) }
          .groupByKey
          .fold(Map.empty[Int, Person]) { _ + _ }
      }

      // Roll up clubs under their id
      clubPool <- Graph.label("club-pool") {
        Graph.source(clubs).latestOr(Club())
      }

      // Join, and a trivial transformation
      _ <- Graph.sink(both) {
        (clubPool join peoplePool)
          .map { case (club -> members) =>
            club -> members.values.toSet
          }
          .stream
      }
    } yield ()

    "do nothing" in true
  }
}
