package com.monovore.example.coast

import com.monovore.coast.machine.{Sample, Messages, Machine}
import org.scalacheck.{Prop, Arbitrary, Gen}
import org.specs2.ScalaCheck
import org.specs2.matcher.Parameters
import org.specs2.mutable._
import org.scalacheck.Arbitrary.arbitrary

import scala.collection.immutable.SortedSet

class DenormalizeSpec extends Specification with ScalaCheck {

  import Denormalize._

  // we have a lot of nested collections, so let's keep things reasonable here
  implicit val scalacheck = Parameters(maxSize = 10)

  "Denormalize example" should {

    implicit val idGen = Arbitrary {
      Gen.choose(0L, 32L).map(Denormalize.ID)
    }

    implicit val usersGen = Arbitrary {
      for {
        name <- Gen.oneOf("Miguel", "Allie", "Spencer", "StarFox")
        ids <- arbitrary[Set[Denormalize.ID]].map { _.to[SortedSet] }
      } yield Denormalize.User(name, ids)
    }

    implicit val groupsGen = Arbitrary {
      for {
        name <- Gen.oneOf("Robbers", "Colts Fans", "Breadwinners")
      } yield Denormalize.Group(name)
    }

    val compiled = Machine.compile(Denormalize.graph)

    "never output a group if no groups are added" in {

      prop { input: Map[ID, Seq[Option[User]]] =>

        val messages = Messages.from(Users, input)

        val thing = compiled.push(messages)

        Prop.forAll(Sample.complete(thing)) { output =>

          forall(output(Denormalized)) { case (id, values) =>
            values.flatten must beEmpty
          }
        }
      }
    }

    "keep an accurate count of members" in {

      val users = Messages.from(Users, Map(
        ID(0) -> Seq(Some(User("Robert", groupIDs = SortedSet(ID(5), ID(6))))),
        ID(1) -> Seq(Some(User("Wilhelm", SortedSet(ID(5))))),
        ID(2) -> Seq(Some(User("Temp", SortedSet(ID(5)))), None)
      ))

      val groups = Messages.from(Groups, Map(
        ID(5) -> Seq(Some(Group("Club Mate")))
      ))

      val thing = compiled.push(users ++ groups)

      Prop.forAll(Sample.complete(thing)) { output =>

        output(Denormalized)(ID(5)).last must_== Some(
          DenormalizedGroup("Club Mate", 2)
        )
      }
    }
  }
}
