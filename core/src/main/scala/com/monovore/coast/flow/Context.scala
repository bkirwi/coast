package com.monovore.coast
package flow

/**
 * In the Stream / Pool builders, we often want two versions of a method: one
 * that gives access to the key, and one that doesn't (for concision). Instead
 * of duplicating all that code, we parameterize the builders by a 'context'
 * which specifies whether or not a key is expected. This means we can do both:
 *
 * stream.map { value => fn(value) }
 * stream.withKeys.map { key => value => fn(key, value) }
 */
sealed trait Context[K, X[+_]] {
  def unwrap[A](wrapped: X[A]): (K => A)
  def map[A, B](wrapped: X[A])(function: A => B): X[B]
}

class NoContext[K] extends Context[K, Id] {
  override def unwrap[A](wrapped: Id[A]): (K) => A = { _ => wrapped }
  override def map[A, B](wrapped: Id[A])(function: (A) => B): Id[B] = function(wrapped)
}

class FnContext[K] extends Context[K, From[K]#To] {
  override def unwrap[A](wrapped: From[K]#To[A]): (K) => A = wrapped
  override def map[A, B](wrapped: From[K]#To[A])(function: (A) => B): From[K]#To[B] = wrapped andThen function
}