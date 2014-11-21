package com.monovore.coast.wire

import com.google.common.hash.{Hashing, HashFunction}
import com.monovore.coast.wire.{GuavaPartitioning, Partitioner}
import org.scalacheck.Arbitrary
import org.specs2.mutable._
import org.specs2.ScalaCheck

class GuavaSpec extends Specification with ScalaCheck {

  "guava partitioners" should {

    object guava extends GuavaPartitioning {
      override protected def guavaHashFunction: HashFunction = Hashing.murmur3_32()
    }

    def partitionerTest[A : Arbitrary](partitioner: Partitioner[A]) = {

      s"be consistent with equals" in prop { (one: A, other: A) =>

        (one != other) || {
          partitioner.hash(one) == partitioner.hash(other)
        }
      }
    }

    "long partitioner" should partitionerTest(guava.longPartitioner)

    "int partitioner" should partitionerTest(guava.intPartitioner)

    "string partitioner" should partitionerTest(guava.stringPartitioner)
  }
}
