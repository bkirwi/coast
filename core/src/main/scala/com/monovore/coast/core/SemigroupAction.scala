package com.monovore.coast.core

import com.twitter.algebird.Semigroup

trait SemigroupAction[S, A] {

  def semigroup: Semigroup[A]
  def act(state: S, action: A): S
}

object SemigroupAction {

  def act[S, A](s: S, a: A)(implicit action: SemigroupAction[S, A]): S = action.act(s, a)

  implicit def forSemigroup[A](implicit delegate: Semigroup[A]): SemigroupAction[A, A] =
    new SemigroupAction[A, A] {
      override def semigroup: Semigroup[A] = delegate
      override def act(state: A, action: A): A = delegate.plus(state, action)
    }
}

case class MapUpdates[A, +B] private[core](
  added: Map[A, B] = Map.empty[A, B],
  removed: Set[A] = Set.empty[A]
)

object MapUpdates {

  def add[A, B](map: Map[A, B]) = MapUpdates(added = map)

  def remove[A](set: Set[A]): MapUpdates[A, Nothing] = MapUpdates[A, Nothing](removed = set)

  implicit def semigroup[A, B]: Semigroup[MapUpdates[A, B]] = new Semigroup[MapUpdates[A, B]] {
    override def plus(l: MapUpdates[A, B], r: MapUpdates[A, B]): MapUpdates[A, B] = {
      MapUpdates(l.added ++ r.added -- r.removed, l.removed -- r.added.keys ++ r.removed)
    }
  }

  implicit def semigroupAction[A, B]: SemigroupAction[Map[A, B], MapUpdates[A, B]] =
    new SemigroupAction[Map[A, B], MapUpdates[A, B]] {
      override def semigroup: Semigroup[MapUpdates[A, B]] = MapUpdates.semigroup
      override def act(state: Map[A, B], action: MapUpdates[A, B]): Map[A, B] = {
        state -- action.removed ++ action.added
      }
    }
}

