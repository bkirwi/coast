package com.monovore.example.coast

import com.monovore.coast
import coast.flow

import scala.collection.immutable.SortedSet

object Denormalize extends ExampleMain {

  import coast.wire.ugly._

  type GroupID = Long
  type UserID = Long

  case class Group(name: String)
  case class User(name: String, groupIDs: SortedSet[GroupID])
  case class DenormalizedGroup(name: String, memberNames: Set[String])

  val Users = flow.Name[UserID, User]("users")
  val Groups = flow.Name[GroupID, Group]("groups")
  
  val Denormalized = flow.Name[GroupID, Option[DenormalizedGroup]]("denormalized-groups")

  val graph = for {

    // Roll up 'users' under their group id
    usersByKey <- flow.stream("users-pool") {

      flow.source(Users)
        .withKeys.flatMap { userID => userInfo =>
          userInfo.groupIDs.toSeq.map { _ -> (userID, userInfo.name) }
        }
        .groupByKey
    }

    // Join, and a trivial transformation
    _ <- flow.sink(Denormalized) {

      val groups = flow.source(Groups).latestOption

      val usersPool = usersByKey.fold(Map.empty[UserID, String]) { _ + _ }

      (groups join usersPool)
        .map { case (groupOption, members) =>
          for (group <- groupOption) yield {
            DenormalizedGroup(group.name, members.values.toSet)
          }
        }
        .updates
    }
  } yield ()
}
