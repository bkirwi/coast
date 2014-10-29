package com.monovore.coast
package example

import com.monovore.coast
import org.specs2.mutable._

import format.javaSerialization.forAnything

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

    val entities = coast.Name[String, Entity]("entities")

    val merged = coast.Name[Int, Entity]("merged")

//    val graph = for {
//
//      // Group input into the correct scopes
//      scoped <- Flow.label("bucketed") {
//        Flow.source(entities)
//          .flatMap { entity => scope(entity).map { _ -> entity } }
//          .groupByKey
//      }
//
//      // Take all 'new' entities and (re)merge them
//      // Note the circular definition here, as entities created by the merge
//      // get piped back in.
//      _ <- Flow.sink(merged) {
//        Flow.merge(scoped, Flow.source(merged))
//          .transform(Set.empty[Entity]) { (entities, nextEntity) =>
//            // TODO: real swoosh
//            (entities + nextEntity) -> Seq.empty
//          }
//          .flatMap { entity => scope(entity).map { _ -> entity } }
//          .groupByKey
//      }
//    } yield ()

    "do nothing" in true
  }

  "a denormalized indexing implementation" should {

    // TODO: less dumb example
    case class Club()
    case class Person(clubId: Int = 0)

    // TODO: make keys visible everywhere
    val people = coast.Name[Int, Person]("people")
    val clubs = coast.Name[Int, Club]("clubs")
    val both = coast.Name[Int, Club -> Set[Person]]("both")

    val graph = for {

      // Roll up 'people' under their club id
      peopleByKey <- coast.label("people-pool") {
        coast.source(people)
          .withKeys.map { key => person => person.clubId -> (key -> person) }
          .groupByKey
      }

      // Roll up clubs under their id
      clubPool <- coast.label("club-pool") {
        coast.source(clubs).latestOr(Club())
      }

      // Join, and a trivial transformation
      _ <- coast.sink(both) {

        val peoplePool = peopleByKey.fold(Map.empty[Int, Person]) { _ + _ }

        (clubPool join peoplePool)
          .map { case (club -> members) =>
            club -> members.values.toSet
          }
          .stream
      }
    } yield ()

    "do nothing" in true
  }

  "a reach calculation" should {

    type UserID = String
    type Link = String

    val followers = coast.Name[UserID, UserID]("new-followers")
    val tweets = coast.Name[UserID, Link]("tweets")

    val reach = coast.Name[Link, Int]("reach")

    val reachFlow = for {

      followerStream <- coast.label("follower-stream") {

        val followersByUser =
          coast.source(followers).fold(Set.empty[UserID]) { _ + _ }

        coast.source(tweets)
          .join(followersByUser)
          .groupByKey
      }

      _ <- coast.sink(reach) {
        followerStream
          .fold(Set.empty[UserID]) { _ ++ _ }
          .map { _.size }
          .stream
      }

    } yield ()

    "do nothing" in true
  }
}
