package com.monovore.coast.wire

import com.google.common.hash.{Funnel, HashCode, HashFunction}
import com.google.common.primitives.UnsignedInts

import scala.annotation.implicitNotFound
import scala.language.existentials

/**
 * Hashes the value A to a partition in the range [0, numPartitions). This is
 * analogous to Kafka's partitioner class, but meant to be used as a typeclass.
 * This makes it easier to configure partitioning strategies per-topic, instead
 * of per-producer-instance.
 */
@implicitNotFound("No partitioner for in scope for key type ${A}")
trait Partitioner[-A] extends Serializable {
  def partition(a: A, numPartitions: Int): Int
}

object Partitioner {

  /**
   * Our default partitioner should behave the same as Kafka's default partitioner.
   */
  val byHashCode = new Partitioner[Any] {
    override def partition(a: Any, numPartitions: Int): Int = {
      // Kafka uses bitwise ops instead of [[scala.math.abs]] to avoid strange behaviour at Int.MinValue
      (a.hashCode & 0x7fffffff) % numPartitions
    }
  }
}
