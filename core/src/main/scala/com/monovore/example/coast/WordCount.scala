package com.monovore.example.coast

import com.monovore.coast
import com.monovore.coast.flow
import com.monovore.coast.flow.Topic

object WordCount extends ExampleMain {

  type Source = Long

  val Sentences = Topic[Source, String]("sentences")

  val WordCounts = Topic[String, Int]("word-counts")

  import coast.wire.ugly._

  val graph = for {

    words <- flow.stream("words") {

      flow.source(Sentences)
        .flatMap { _.split("\\s+") }
        .map { _ -> 1 }
        .groupByKey
    }

    _ <- flow.sink(WordCounts) {
      words.sum.updates
    }

  } yield ()

}
