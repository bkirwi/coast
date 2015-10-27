package com.monovore.example.coast

import com.monovore.coast.flow.{Flow, Topic}
import com.monovore.coast.wire.Protocol

import scala.collection.immutable.SortedSet

/**
 * A sketch of a denormalization flow -- normalized models come in at the top,
 * and denormalized versions appear at the bottom.
 */
object Denormalize extends ExampleMain {

  import Protocol.native._

  case class ID(value: Long)
  type GroupID = ID
  type UserID = ID

  implicit val IDOrdering = Ordering.by { id: ID => id.value }

  case class Group(name: String)
  case class User(name: String, groupIDs: SortedSet[GroupID])
  case class DenormalizedGroup(name: String, memberNames: Set[String])

  // 'Changelog' for users and groups
  // We expect None when the data is missing or deleted, and Some(user) otherwise
  val Users = Topic[UserID, Option[User]]("users")
  val Groups = Topic[GroupID, Option[Group]]("groups")
  
  val Denormalized = Topic[GroupID, Option[DenormalizedGroup]]("denormalized-groups")

  val graph = Flow.build { implicit builder =>

    val usersPool =
      Flow.source(Users)
        .map { userOpt =>
          userOpt
            .map { user =>
              user.groupIDs.map { _ -> user.name }.toMap
            }
            .getOrElse(Map.empty)
        }
        .latestByKey("users-pool")

    val groups = Flow.source(Groups).latestOr(None)

    (groups join usersPool)
      .map { case (groupOption, members) =>
        for (group <- groupOption) yield {
          DenormalizedGroup(group.name, members.values.toSet)
        }
      }
      .updates
  }
}
