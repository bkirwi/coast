package com.monovore.example.coast

import com.monovore.coast.core.Graph
import com.monovore.coast.flow.{Topic, Flow}

object Scheduler extends ExampleMain {

  import com.monovore.coast.wire.ugly._

  val Requests = Topic[Long, String]("requests")

  val Triggered = Topic[Long, Set[String]]("triggered")
  
  override def graph: Graph = for {

    ticks <- Flow.stream("whatever") {

      Flow.clock(seconds = 60)
        .map { _ -> "tick!" }
        .groupByKey
    }

    _ <- Flow.sink(Triggered) {

      val allEvents = Requests.asSource.map(Set(_)).sum

      (ticks join allEvents)
        .map { case (_, events) => events }
    }

  } yield ()
}
