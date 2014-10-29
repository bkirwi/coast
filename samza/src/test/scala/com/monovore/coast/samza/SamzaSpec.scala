package com.monovore.coast
package samza

import org.apache.samza.config.serializers.JsonConfigSerializer
import org.specs2.ScalaCheck
import org.specs2.mutable._

class SamzaSpec extends Specification with ScalaCheck {

  "the samza package" should {

    "compile a simple flow" in {

      import WireFormats.pretty._

      val source = Name[String, Int]("ints")
      val sink = Name[String, Int]("bigger-ints")

      val flow = Flow.sink(sink) {
        Flow.source(source).fold(0) { _ + _ }.stream
      }

      val configs = Samza.configFor(flow)(
        system = "whatever",
        baseConfig = Samza.config(
        )
      )

      configs must haveSize(1)

      configs must beDefinedAt("bigger-ints")

      val config = configs("bigger-ints")

      println(JsonConfigSerializer.toJson(config))

      config.get(Samza.TaskName) must_== "bigger-ints"

      config.get("stores./bigger-ints.factory") must_!= null
    }
  }
}
