package com.monovore.coast
package mini

import com.monovore.coast.machine.{Message, Key}

import scala.collection.concurrent
import scala.collection.JavaConverters._

class Log[K, V](state: concurrent.Map[K, Vector[V]]) {

  def fetchAll(partition: K): Seq[V] = state.getOrElse(partition, Vector.empty)

  def fetch(partition: K, offset: Int): Option[V] = {
    val data = state.getOrElse(partition, Vector.empty)
    data.lift(offset)
  }

  def push(partition: K, message: V): Int = {
    state.get(partition) match {
      case Some(vector) => {
        val updated = vector :+ message
        val wasReplaced = state.replace(partition, vector, updated)
        if (wasReplaced) vector.length else push(partition, message)
      }
      case None => {
        val previous = state.putIfAbsent(partition, Vector(message))
        if (previous.isEmpty) 0 else push(partition, message)
      }
    }
  }
}

object Log {

  def empty[K, V](): Log[K, V] = new Log(new java.util.concurrent.ConcurrentHashMap[K, Vector[V]]().asScala)

  case class Partition(stream: String, key: Key)
}
