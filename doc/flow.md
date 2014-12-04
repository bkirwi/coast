---
pagetitle: flow
---

# Flow API

`coast` defines an API that can express a wide variety of stream
transformations, including merges, splits, folds, and joins.

Scala-idiomatic, expressive, and safe.

To get started, a single import suffices:

```scala
import com.monovore.coast.flow
```

## Sources and Sinks

```scala
val Sentences = flow.Name[Long, String]("sentences")
```

This groups a stream name together with the key and value types.

Here, `Long` is the type of the partition key, and `String` is the type of the
values in each partition.

If you want to use this stream as an input to your job, you can use it as a
source:

```scala
val sentenceStream = flow.source(Sentences)
```

`sentenceStream` is an value of type `flow.Stream` -- most of our tools for
transforming and combining data work directly on this type. For now, though,
we'll sink it directly to an output:

```
val OtherSentences = flow.Name[Long, String]("other-sentences")

val copy = flow.sink(OtherSentences) { sentenceStream }
```

Here, `copy` is a `FlowGraph` instance. A flow graph just associates stream
names to the code that produces them. In our case, we're passing through the
data from `Sentences` without any changes at all -- so this just defines a
stream-to-stream copy.

A `FlowGraph` is a complete description of a job. Typically, you'd take this
description and hand it off to some other code to interpret. If you hand this
graph off to GraphViz, for example, you get this:

![A simple copy job](examples/CopyStream.svg)\

Or you can pass it to a Samza job runner, and have it execute on the cluster.
The possibilities are limitless!

## Transforming Streams

While copying streams from one topic to another is occasionally useful, we
ususally have something more elaborate in mind.

Stream instances support many of the convenience methods you know and love from
the Scala collections API:

```scala
val transform = flow.sink(Output) { 

  flow.source(Input)
    .filter { _ % 2 == 0 }
    .map { _.toString() }        
    .flatMap { _.toSeq }        
}
```

Don't hesitate to break your stream transformations into small parts like this;
it shouldn't affect performance.

You can also merge two streams together into a single stream:

```scala
val merged = flow.merge(events, moreEvents)
```

`merged` now contains all the messages from either `events` or `moreEvents`
interleaved together.

## Pools and Joins

```scala
val pool: flow.Pool[String, Int] = stream.latestOr(0)
val updates: flow.Stream[String, Int] = pool.updates
```

```scala
stream.fold(0) { _ + _ }
```

## Composing Flows

All the dataflows we defined above are simple, taking one or more inputs and
directly producing a single output stream. On a cluster, these would all compile
to a single-stage job. However, many jobs are not so simple; they might have
multiple stages and produce multiple streams, and some of those streams might
only exist to be inputs to later stages.[^regrouping]

[^regrouping]: There's one case where you *need* to split a job up into multiple stages.
Consider the standard word-count example: you want to split the text up into
words, then group all the examples of the same word together, and count how many
you see. In this case, we need to 'shuffle' the word across the network. (I, the
author, make this mistake all the time.) `flow` tracks whether or not you've
regrouped the stream, and the compiler will warn you if you've made a mistake.

```scala
val graph = for {

  first <- flow.stream("intermediate") {
    stream.groupBy { _.toString }
  }

  _ <- flow.sink(Output) {
    first.map { _ * 2 }
  }
} yield ()
```

You can see two major sections in that `for` block above -- and indeed, this
creates a two-stage graph.

## Learning More

If you made it this far, you'll want to have a look at some complete jobs: try
the alarmingly well-commented [Twitter reach calculation][twitter-reach], or
[one of the other examples][examples].

[twitter-reach]: https://github.com/bkirwi/coast/blob/master/core/src/main/scala/com/monovore/example/coast/TwitterReach.scala
[examples]: https://github.com/bkirwi/coast/tree/master/core/src/main/scala/com/monovore/example/coast

