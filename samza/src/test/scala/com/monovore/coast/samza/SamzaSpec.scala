package com.monovore.coast
package samza

import com.monovore.coast
import org.apache.samza.config.serializers.JsonConfigSerializer
import org.specs2.ScalaCheck
import org.specs2.mutable._

class SamzaSpec extends Specification with ScalaCheck {

  "the samza package" should {

    "compile a simple flow" in {

      import coast.wire.pretty._

      val source = Name[String, Int]("ints")
      val sink = Name[String, Int]("bigger-ints")

      val sampleFlow = coast.sink(sink) {
        coast.source(source).fold(0) { _ + _ }.stream
      }

      val configs = samza.configureFlow(sampleFlow)(
        baseConfig = samza.config()
      )

      configs must haveSize(1)

      configs must beDefinedAt("bigger-ints")

      val config = configs("bigger-ints")

      config.get(samza.TaskName) must_== "bigger-ints"

      config.get("stores.bigger-ints.factory") must_!= null
    }
  }
}
