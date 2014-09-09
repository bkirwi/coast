package com.monovore.coast

import scala.language.higherKinds

package object toy {

  class NameMap[V[_,_]] private[toy](underlying: Map[Name[_,_], V[_,_]]) { self =>
    def apply[A,B](name: Name[A, B]): V[A, B] = underlying(name).asInstanceOf[V[A,B]]
    def put[A,B](name: Name[A, B], value: V[A, B]): NameMap[V] = new NameMap(underlying + (name -> value))
    def keys: Seq[Name[_,_]] =underlying.keys.toList
    override def toString(): String = underlying.toString()

    def mapValues[V0[_,_]](mapper: Mapper[V, V0]): NameMap[V0] = {
      val mapped = underlying
        .mapValues { case value => mapper.convert(value) }
        .toMap

      new NameMap[V0](mapped)
    }
  }

  trait Mapper[V[_,_],V0[_,_]] {
    def convert[A,B](in: V[A,B]): V0[A,B]
  }

  object NameMap {
    def empty[V[_,_]]: NameMap[V] = new NameMap(Map.empty)
  }

  type NamedPair[V[_,_], A, B] = (Name[A,B], V[A, B])

  type Pairs[A, B] = Seq[(A,B)]
}
