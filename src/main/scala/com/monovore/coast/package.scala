package com.monovore

import scala.language.higherKinds

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


  // Name map -- delete this stuff

  class NameMap[V[_,_]] private[coast](underlying: Map[Name[_,_], V[_,_]]) { self =>
    def apply[A,B](name: Name[A, B]): V[A, B] = underlying(name).asInstanceOf[V[A,B]]
    def put[A,B](name: Name[A, B], value: V[A, B]): NameMap[V] = new NameMap(underlying + (name -> value))
    def keys: Seq[Name[_,_]] =underlying.keys.toList
    override def toString(): String = underlying.toString()
  }

  object NameMap {
    def empty[V[_,_]]: NameMap[V] = new NameMap(Map.empty)
  }
}
