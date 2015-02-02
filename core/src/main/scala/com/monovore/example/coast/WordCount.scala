package com.monovore.example.coast

import com.monovore.coast
import com.monovore.coast.flow.{Flow, Topic}

object WordCount extends ExampleMain {

  type Source = Long

  val Sentences = Topic[Source, String]("sentences")

  val WordCounts = Topic[String, Int]("word-counts")

  import coast.wire.ugly._

  val graph = for {

    words <- Flow.stream("words") {

      Flow.source(Sentences)
        .flatMap { _.split("\\s+") }
        .map { _ -> 1 }
        .groupByKey
    }

    _ <- Flow.sink(WordCounts) {
      words.sum.updates
    }

  } yield ()

}
