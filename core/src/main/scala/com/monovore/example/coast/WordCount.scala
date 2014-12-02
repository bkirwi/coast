package com.monovore.example.coast

import com.monovore.coast
import com.monovore.coast.flow

object WordCount extends ExampleMain {

  type Source = Long

  val Sentences = flow.Name[Source, String]("sentences")

  val WordCounts = flow.Name[String, Int]("word-counts")

  import coast.wire.ugly._

  val graph = for {

    words <- flow.stream("words") {

      flow.source(Sentences)
        .flatMap { _.split("\\s+") }
        .map { _ -> 1 }
        .groupByKey
    }

    _ <- flow.sink(WordCounts) {

      words
        .fold(0) { _ + _ }
        .updates
    }

  } yield ()

}
