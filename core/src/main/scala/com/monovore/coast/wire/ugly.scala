package com.monovore.coast.wire

/**
 * Importing coast.wire.evil._ gives you a partitioner and binary format for all
 * types. As the name suggests, this is probably not what you want:
 * `Object.hashCode()`s are not necessarily consistent across JVMs, and serialization
 * is slow and can fail at runtime.
 *
 * Nevertheless, this is sometimes useful for prototyping and testing. But be
 * careful!
 */
object ugly {

  implicit def anyPartitioner[A]: Partitioner[A] = Partitioner.byHashCode

  implicit def anyFormat[A]: BinaryFormat[A] = BinaryFormat.javaSerialization[A]
}
