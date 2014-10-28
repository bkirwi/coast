package com.monovore

package object coast {

  // IMPLEMENTATION
  // always-visible utilities; should be hidden within the coast package

  private[coast] val unit: Unit = ()

  private[coast] def some[A](a: A): Option[A] = Some(a)

  private[coast] type ->[A, B] = (A, B)

  private[coast] object -> {
    def unapply[A, B](pair: (A, B)) = Some(pair)
  }

  private[coast] implicit class SeqOps[A](underlying: Seq[A]) {
    def groupByKey[B,C](implicit proof: A <:< (B, C)): Map[B, Seq[C]] =
      underlying.groupBy { _._1 }.mapValues { _.unzip._2 }
  }

  private[coast] def assuming[A](cond: Boolean)(action: => A): Option[A] =
    if (cond) Some(action) else None

  private[coast] type Id[+A] = A

  private[coast] type From[A] = { type To[+B] = (A => B) }
}
