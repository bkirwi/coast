package com.monovore.coast

import com.monovore.coast.wire.{Partitioner, BinaryFormat}
import com.monovore.coast.model.{Source, Merge, Sink}

package object flow {

  type Stream[A, +B] = StreamDef[Grouped, A, B]

  type Pool[A, +B] = PoolDef[Grouped, A, B]

  case class Topic[A, B](name: String)

  private[coast] type Id[+A] = A

  private[coast] type From[A] = { type To[+B] = (A => B) }
}
