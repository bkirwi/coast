package com.monovore.example.coast

import com.monovore.coast
import com.monovore.coast.flow

object CopyStream extends ExampleMain {

  import coast.wire.pretty._

  val Input = flow.Name[Int, String]("input")
  val Output = flow.Name[Int, String]("output")

  val graph = flow.sink(Output) { flow.source(Input) }
}
