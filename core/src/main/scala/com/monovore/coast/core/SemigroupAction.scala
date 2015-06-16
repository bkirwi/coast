package com.monovore.coast.core

import com.twitter.algebird.Semigroup

trait SemigroupAction[S, A] {

  def semigroup: Semigroup[A]
  def act(state: S, action: A): S
}

object SemigroupAction {

  implicit def forSemigroup[A](implicit delegate: Semigroup[A]): SemigroupAction[A, A] =
    new SemigroupAction[A, A] {
      override def semigroup: Semigroup[A] = delegate
      override def act(state: A, action: A): A = delegate.plus(state, action)
    }
}
