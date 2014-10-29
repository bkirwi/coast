package com.monovore.coast.samza

import org.specs2.ScalaCheck
import org.specs2.mutable._

class SerializationUtilSpec extends Specification with ScalaCheck {

  "some serialization utils" should {

    "round-trip an integer" in prop { x: Int =>

      val string: String = SerializationUtil.toBase64(x)

      SerializationUtil.fromBase64[Int](string) must_== x
    }

    "round-trip a function" in {

      val func = { x: Int => x * x }

      val string: String = SerializationUtil.toBase64(func)

      val recovered = SerializationUtil.fromBase64[Int => Int](string)

      prop { x: Int => func(x) must_== recovered(x) }
    }
  }
}
