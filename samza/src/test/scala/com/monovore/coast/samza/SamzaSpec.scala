package com.monovore.coast
package samza

import com.monovore.coast
import coast.flow
import com.monovore.coast.flow.Topic

import org.apache.samza.config.serializers.JsonConfigSerializer
import org.specs2.ScalaCheck
import org.specs2.mutable._

class SamzaSpec extends Specification with ScalaCheck {

  "the samza package" should {

    "compile a flow" in {

      import coast.wire.pretty._

      val source = Topic[String, Int]("ints")
      val sink = Topic[String, Int]("bigger-ints")

      val sampleFlow = flow.sink(sink) {
        flow.source(source).fold(0) { _ + _ }.updates
      }

      val configs = samza.Safe().configure(sampleFlow)

      configs must haveSize(1)

      configs must beDefinedAt("bigger-ints")

      val config = configs("bigger-ints")

      config.get(samza.TaskName) must_== "bigger-ints"

      config.get("stores.bigger-ints.factory") must_!= null
    }
  }
}
