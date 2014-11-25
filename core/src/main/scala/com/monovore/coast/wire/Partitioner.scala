package com.monovore.coast.wire

import com.google.common.hash.{Funnel, HashCode, HashFunction}

import scala.annotation.implicitNotFound
import scala.language.existentials

/**
 * Hashes the value A to a hash code.
 *
 * Note: unlike the default Kafka partitioning, lexicographically-similar values
 * of the hash can be expected to go to the same partition. This allows you
 * to do locality-sensitive hashing without knowing the exact number of partitions.
 * Otherwise, pick a good mixing hash.
 */
@implicitNotFound("No partitioner for key type ${A} in scope")
trait Partitioner[-A] {
  def hash(a: A): HashCode
}

object Partitioner {
  def hash[A](a: A)(implicit partitioner: Partitioner[A]): HashCode = partitioner.hash(a)
}

/**
 * A partitioner that uses the given Guava hash function and funnel.
 */
case class GuavaPartitioner[-A](hashFunction: HashFunction, funnel: Funnel[_ >: A]) extends Partitioner[A] {

  override def hash(a: A): HashCode = hashFunction.hashObject(a, funnel)
}
