package com.monovore.example.coast

import com.monovore.coast
import com.monovore.coast.flow._

object WordCount extends ExampleMain {

  type Source = Long

  val Sentences = Topic[Source, String]("sentences")

  val WordCounts = Topic[String, Int]("word-counts")

  import coast.wire.pretty._

  val graph = Flow.build { implicit context =>

    Sentences.asSource
      .flatMap {_.split("\\s+")}
      .map {_ -> 1}
      .groupByKey
      .addStream("words")
      .sum.updates
      .addSink(WordCounts)
  }
}
