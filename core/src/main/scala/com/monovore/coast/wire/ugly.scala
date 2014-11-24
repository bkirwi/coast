package com.monovore.coast.wire

import com.google.common.hash.HashCode

/**
 * Importing coast.wire.evil._ gives you a partitioner and wire format for all
 * types. As the name suggests, this is probably not what you want:
 * `Object.hashCode()`s are not necessarily consistent across JVMs, and serialization
 * is slow and can fail at runtime.
 *
 * Nevertheless, this is sometimes useful for prototyping and testing. But be
 * careful!
 */
object ugly {

  implicit def anyPartitioner[A]: Partitioner[A] = new Partitioner[A] {
    override def hash(a: A): HashCode = HashCode.fromInt(a.hashCode())
  }

  implicit def anyFormat[A]: BinaryFormat[A] = javaSerialization.formatFor[A]
}
