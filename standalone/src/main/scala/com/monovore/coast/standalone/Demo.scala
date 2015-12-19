package com.monovore.coast.standalone

import com.monovore.coast.flow.Topic
import com.monovore.coast.wire.Protocol

object Demo extends StandaloneApp {

  import Protocol.simple._

  val Sentences = Topic[Int, String]("sentences")
  val Words = Topic[Int, String]("words")

  Sentences.asSource.sinkTo(Words)
}