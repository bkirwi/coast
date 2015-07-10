package com.monovore.example.coast

import com.monovore.coast
import com.monovore.coast.flow.{GraphBuilder, Flow, Topic}

object WordCount extends ExampleMain {

  type Source = Long

  val Sentences = Topic[Source, String]("sentences")

  val WordCounts = Topic[String, Int]("word-counts")

  import coast.wire.pretty._

  implicit val graph = new GraphBuilder

  Sentences.asSource
    .flatMap { _.split("\\s+") }
    .map { _ -> 1 }
    .groupByKey
    .streamAs("words")
    .sum.updates
    .sinkTo(WordCounts)
}
