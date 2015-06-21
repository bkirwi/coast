package com.monovore.example.coast

import com.monovore.coast.core.Graph
import com.monovore.coast.flow.Flow

object ClockExample extends ExampleMain {

  import com.monovore.coast.wire.pretty._

  override def graph: Graph = for {

    _ <- Flow.stream("whatever") {

      Flow.clock(60)
        .map { _ => "heh" }
    }

  } yield ()
}
