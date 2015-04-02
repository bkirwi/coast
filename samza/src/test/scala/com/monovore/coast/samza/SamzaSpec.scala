package com.monovore.coast
package samza

import com.monovore.coast
import com.monovore.coast.flow.{Flow, Topic}
import org.apache.samza.storage.kv.inmemory.InMemoryKeyValueStorageEngineFactory

import org.specs2.ScalaCheck
import org.specs2.mutable._

import collection.JavaConverters._

class SamzaSpec extends Specification with ScalaCheck {

  "the samza package" should {

    "compile a flow" in {

      import coast.wire.pretty._

      val source = Topic[String, Int]("ints")
      val sink = Topic[String, Int]("bigger-ints")

      val sampleFlow = Flow.sink(sink) {
        Flow.source(source).fold(0) { _ + _ }.updates
      }

      val backend = samza.SafeBackend(baseConfig = SamzaConfig.from(
        "coast.default.stores.factory" -> SamzaConfig.className[InMemoryKeyValueStorageEngineFactory[_, _]]
      ))

      val configs = backend.configure(sampleFlow)

      configs must haveSize(1)

      configs must beDefinedAt("bigger-ints")

      val config = configs("bigger-ints")

      config.get(SamzaConfig.TaskName) must_== "bigger-ints"

      config.get("stores.bigger-ints.factory") must_!= null
    }
  }
}
