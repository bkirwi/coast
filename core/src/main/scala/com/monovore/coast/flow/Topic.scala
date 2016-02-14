package com.monovore.coast
package flow

import com.monovore.coast.wire.Serializer

class Topic[G <: AnyGrouping, A, B] private[flow] (
  val name: String,
  val grouping: AnyGrouping,
  val keySerializer: Serializer[A],
  val valueSerializer: Serializer[B]
) {
  def asSource(implicit af: Serializer[A], bf: Serializer[B]): StreamDef[G, A, B] = Flow.source(this)
  override def toString = name
}

object Topic {
  def apply[A : Serializer, B : Serializer](name: String): Topic[Grouped, A, B] = new Topic(name, Grouped, implicitly, implicitly)
  def ungrouped[A : Serializer, B : Serializer](name: String): Topic[AnyGrouping, A, B] = new Topic(name, Ungrouped, implicitly, implicitly)
  def keyless[A : Serializer](name: String): Topic[AnyGrouping, Unit, A] = new Topic(name, Ungrouped, wire.Protocol.common.unitFormat, implicitly)
}
