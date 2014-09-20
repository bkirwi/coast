package com.monovore.coast.machine

import org.specs2.mutable._

class MachineSpec extends Specification {

  import Machine._

  "an actor system" should {

    "send a message" in {

      val machine = Machine(
        edges = Map(Named("foo") -> Seq(Named("bar")))
      )

      val pushed = machine.push(
        from = Named("foo"),
        partition = Partition(0),
        messages = Seq(Message("great"))
      )

      pushed.state(Named("bar"))(Partition(0)).input must_== Map(Named("foo") -> Seq(Message("great")))
    }
  }
}
