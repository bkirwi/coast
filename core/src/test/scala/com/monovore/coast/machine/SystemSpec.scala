package com.monovore.coast
package machine

import org.specs2.mutable._

class SystemSpec extends Specification {

  "an actor system" should {

    "send a message" in {

      val machine = System[String](
        edges = Map("source" -> Seq("target"))
      )

      val pushed = machine.push("source", Key(0), Seq(Message("message")))

      pushed.state("target")(Key(0)).input must_== Map("source" -> Seq(Message("message")))
    }

    "process a message" in {

      val machine = System[String](
        state = Map("target" -> Map(Key(0) -> Actor.Data(
          state = State(unit),
          input = Map("source" -> Seq(Message("payload")))
        )))
      )

      machine.poke must haveSize(1)

      val (updated, output) = machine.process("target", "source", Key(0))

      output must_== Map(Key(0) -> Seq(Message("payload")))

      updated.state("target")(Key(0)).input("source") must beEmpty
    }
  }
}
