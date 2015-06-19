package com.monovore.coast.core

import com.twitter.algebird.Semigroup
import org.scalacheck.{Prop, Arbitrary}
import org.specs2.mutable._
import org.specs2.ScalaCheck

class SemigroupActionSpec extends Specification with ScalaCheck {

  import SemigroupAction.act
  import Semigroup.plus
  import Arbitrary.arbitrary

  "a lifted SemigroupAction" should {

    "obey that action law" in prop { (a: Int, b: Int, c: Int) =>

      act(a, plus(b, c)) must_== act(act(a, b), c)
    }
  }

  "a MapAction" should {

    val gen = for {
      map <- arbitrary[Map[Int, Int]]
      set <- arbitrary[Set[Int]]
    } yield plus(MapUpdates.add(map), MapUpdates.remove(set))

    "be a proper semigroup" in Prop.forAll(gen, gen, gen) { (a, b, c) =>

      plus(a, plus(b, c)) must_== plus(plus(a, b), c)
    }

    "obey the action law" in Prop.forAll(arbitrary[Map[Int, Int]], gen, gen) { (s, a, b) =>

      act(s, plus(a, b)) must_== act(act(s, a), b)
    }
  }
}
