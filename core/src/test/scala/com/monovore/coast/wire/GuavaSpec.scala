package com.monovore.coast.wire

import com.google.common.hash.{HashFunction, Hashing}
import org.scalacheck.Arbitrary
import org.specs2.ScalaCheck
import org.specs2.mutable._

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
