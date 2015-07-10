package com.monovore.coast.flow

import com.monovore.coast.core.{Graph, Sink}
import com.monovore.coast.wire.{BinaryFormat, Partitioner}

import scala.annotation.implicitNotFound

@implicitNotFound("This method requires a GraphBuilder in scope!")
class GraphBuilder extends Graph {

  private[this] var _bindings: Seq[(String, Sink[_, _])] = Nil

  override def bindings: Seq[(String, Sink[_, _])] = _bindings

  def add[A](flow: Flow[A]): A = {
    val updated = Flow(_bindings, ()).flatMap { _ => flow }
    _bindings = updated.bindings
    updated.value
  }

  def addCycle[A:BinaryFormat:Partitioner, B:BinaryFormat](name: String)(function: GroupedStream[A, B] => AnyStream[A, B]): GroupedStream[A, B] = {
    add(Flow.cycle(name)(function))
  }
}

object GraphBuilder {

  def toFlow[A](fn: GraphBuilder => A): Flow[A] = {
    val builder = new GraphBuilder
    val out = fn(builder)
    Flow(builder.bindings, out)
  }
}
