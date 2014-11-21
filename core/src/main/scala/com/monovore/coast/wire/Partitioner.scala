package com.monovore.coast.wire

import com.google.common.hash.{Funnel, HashCode, HashFunction}

import scala.language.existentials

/**
 * Hashes the value A to a hash code.
 *
 * Note: unlike the default Kafka partitioning, lexicographically-similar values
 * of the hash can be expected to go to the same partition. This allows you
 * to do locality-sensitive hashing without knowing the exact number of partitions.
 * Otherwise, pick a good mixing hash.
 */
trait Partitioner[-A] {
  def hash(a: A): HashCode
}

/**
 * A partitioner that uses the given Guava hash function and funnel.
 */
case class GuavaPartitioner[-A](hashFunction: HashFunction, funnel: Funnel[_ >: A]) extends Partitioner[A] {

  override def hash(a: A): HashCode = hashFunction.hashObject(a, funnel)
}
