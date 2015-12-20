package com.monovore.coast

import com.monovore.coast.wire.{Serializer, Partitioner}

package object flow {

  type AnyStream[A, +B] = StreamDef[AnyGrouping, A, B]
  type GroupedStream[A, +B] = StreamDef[Grouped, A, B]

  type AnyPool[A, +B] = PoolDef[AnyGrouping, A, B]
  type GroupedPool[A, +B] = PoolDef[Grouped, A, B]

  private[coast] type Id[+A] = A

  private[coast] type From[A] = { type To[+B] = (A => B) }
}
