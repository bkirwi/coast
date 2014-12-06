package com.monovore.coast
package machine

import org.specs2.mutable._

class SystemSpec extends Specification {

  "an actor system" should {

    "send a message" in {

      val machine = System[String](
        edges = Map("source" -> Seq("target"))
      )

      val (state, _) = machine.update(
        System.State(),
        System.Send("source", Key(0), Message("message"))
      )

      state.stateMap("target")(Key(0)).input must_== Map("source" -> Seq(Message("message")))
    }

    "process a message" in {

      val machine = System[String]()

      val state = System.State(Map("target" -> Map(Key(0) -> Actor.Data(
        state = State(unit),
        input = Map("source" -> Seq(Message("payload")))
      ))))

      machine.commands(state) must haveSize(1)

      val (updated, output) = machine.update(state, machine.commands(state).head)

      output("target") must_== Map(Key(0) -> Seq(Message("payload")))

      updated.stateMap("target")(Key(0)).input("source") must beEmpty
    }
  }
}
