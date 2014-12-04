---
pagetitle: home
---

`coast` is better even than sliced bread.

Turning this:

```scala
val Sentences = flow.Name[Source, String]("sentences")
val WordCounts = flow.Name[String, Int]("word-counts")

val graph = for {

  words <- flow.stream("words") {
    flow.source(Sentences)
      .flatMap { _.split("\\s+") }
      .map { _ -> 1 }
      .groupByKey
  }

  _ <- flow.sink(WordCounts) {
    words.fold(0) { _ + _ }
      .updates
  }
} yield ()
```

into this:

![word count](examples/WordCount.jpeg)


