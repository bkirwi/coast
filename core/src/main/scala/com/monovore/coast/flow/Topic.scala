package com.monovore.coast
package flow

import com.monovore.coast.wire.Serializer

class Topic[G <: AnyGrouping, A, B] private[flow] (val name: String) {
  def asSource(implicit af: Serializer[A], bf: Serializer[B]): StreamDef[G, A, B] = Flow.source(this)
  override def toString = name
}

object Topic {
  def apply[A, B](name: String): Topic[Grouped, A, B] = new Topic(name)
}
