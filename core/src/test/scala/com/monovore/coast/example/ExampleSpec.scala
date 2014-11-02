package com.monovore.coast
package example

import com.monovore.coast
import org.specs2.mutable._

class ExampleSpec extends Specification {
  
  import format.javaSerialization._

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
        coast.source(clubs).pool(Club())
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
