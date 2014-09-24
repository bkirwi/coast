package com.monovore

package object coast {

  val unit: Unit = ()
  def some[A](a: A): Option[A] = Some(a)

  type ->[A, B] = (A, B)

  object -> {
    def unapply[A, B](pair: (A, B)) = Some(pair)
  }

  implicit class SeqOps[A](underlying: Seq[A]) {
    def groupByKey[B,C](implicit proof: A <:< (B, C)): Map[B, Seq[C]] =
      underlying.groupBy { _._1 }.mapValues { _.unzip._2 }
  }

  def assuming[A](cond: Boolean)(action: => A): Option[A] =
    if (cond) Some(action) else None
}
