package com.monovore.example.coast

import com.monovore.coast.flow._
import com.monovore.coast.wire.Protocol

object WordCount extends ExampleMain {

  import Protocol.common._

  type Source = Long

  val Sentences = Topic[Source, String]("sentences")

  val WordCounts = Topic[String, Int]("word-counts")

  val graph = Flow.build { implicit builder =>

    Sentences.asSource
      .flatMap { _.split("\\s+") }
      .map { _ -> 1 }
      .groupByKey
      .streamTo("words")
      .sum.updates
      .sinkTo(WordCounts)
  }
}
