package com.monovore.example.coast

import com.monovore.coast

import scala.collection.immutable.SortedSet

object Denormalize extends ExampleMain {

  import coast.wire.ugly._

  type GroupID = Long
  type UserID = Long

  case class Group(name: String)
  case class User(name: String, groupIDs: SortedSet[GroupID])
  case class DenormalizedGroup(name: String, memberNames: Set[String])

  val Users = coast.Name[UserID, User]("users")
  val Groups = coast.Name[GroupID, Group]("groups")
  
  val Denormalized = coast.Name[GroupID, Option[DenormalizedGroup]]("denormalized-groups")

  val flow = for {

    // Roll up 'users' under their group id
    usersByKey <- coast.label("users-pool") {
      coast.source(Users)
        .withKeys.flatMap { userID => userInfo =>
          userInfo.groupIDs.toSeq.map { _ -> (userID, userInfo.name) }
        }
        .groupByKey
    }

    // Join, and a trivial transformation
    _ <- coast.sink(Denormalized) {

      val groups = coast.source(Groups).latestOrNone

      val usersPool = usersByKey.fold(Map.empty[UserID, String]) { _ + _ }

      (groups join usersPool)
        .map { case (groupOption, members) =>
          for (group <- groupOption) yield {
            DenormalizedGroup(group.name, members.values.toSet)
          }
        }
        .stream
    }
  } yield ()
}
