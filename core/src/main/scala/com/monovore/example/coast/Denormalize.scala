package com.monovore.example.coast

import com.monovore.coast
import coast.flow
import com.monovore.coast.flow.Topic

import scala.collection.immutable.SortedSet

/**
 * A sketch of a denormalization flow -- normalized models come in at the top,
 * and denormalized versions appear at the bottom.
 *
 * This is not super appealing at the moment, courtesy of the delete handling.
 * I suspect there's a pattern that could be abstracted out here,
 * but I'd like more examples of this relational-style manipulation before I
 * take a stab at it.
 */
object Denormalize extends ExampleMain {

  import coast.wire.ugly._

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

  val graph = for {

    // Roll up 'users' under their group id
    usersByKey <- flow.stream[GroupID, (UserID, Option[String])]("users-pool") {

      flow.source(Users)
        .latestOr(None)
        .updatedPairs
        .flatMap {
          case (None, Some(User(name, groups))) => {
            groups.map { _ -> Some(name) }.toSeq
          }
          case (Some(User(oldName, oldGroups)), Some(User(newName, newGroups))) => {

            val toAdd = if (oldName == newName) newGroups -- oldGroups else newGroups
            val toRemove = oldGroups -- newGroups

            toAdd.toSeq.map { _ -> Some(newName) } ++
              toRemove.toSeq.map { _ -> None }
          }
          case (Some(User(_, groups)), None) => {
            groups.map { _ -> None }.toSeq
          }
          case (None, None) => Seq.empty
        }
    }

    // Join, and a trivial transformation
    _ <- flow.sink(Denormalized) {

      val groups = flow.source(Groups).latestOr(None)

      val usersPool = usersByKey
        .fold(Map.empty[UserID, String]) { (state, newInfo) =>

          val (id, nameOpt) = newInfo

          nameOpt
            .map { name => state.updated(id, name) }
            .getOrElse { state - id }
        }

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
