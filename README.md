# coast

In this dark stream-processing landscape, `coast` is a ray of light.

**NOTE:** This is a brand-new project, and many things are still a little rough
around the edges. If something's particularly confusing or broken, feel free to
[open an issue][issues].

## Why `coast`?

- **Simple:** `coast` provides a simple streaming model with strong ordering and
  exactly-once semantics. This straightforward behaviour extends across multiple
  machines, state aggregations, and even between independent jobs, making it
  easier to reason about how your entire system behaves.

- **Easy:** Streams are built up and wired together using a concise, idiomatic
  Scala API. These dataflow graphs can be as small or as large as you like:
  there's no pressure to cram all your logic in one big job, or to write a bunch
  of single-stage jobs and track their relationships by hand.

- **Kafkaesque:** `coast`'s core abstractions are patterned after Kafka's
  data model, and it's designed to fit comfortably in the middle of a larger
  Kafka-based infrastructure. By taking advantage of Kafka's messaging
  guarantees, `coast` can implement [exactly-once messaging][impossible] without
  a heavy coordination cost.

## Quick Introduction

`coast`'s streams are closely patterned after Kafka's topics: a stream has
multiple partitions, and each partition has an ordered series of values. A
stream can have any number of partitions, each of which has a unique key. (These
keys are multiplexed on to a Kafka topic where necessary, semantic-partitioning
style.) You can create a stream by pulling data from a topic, but `coast` also
has a rich API for building derivative streams: applying transformations,
merging streams together, regrouping, aggregating state, or performing joins.
These derivative streams behave just the same as streams that come directly from
Kafka, which makes it easy to build up complex stream transformations from small
pieces.

Once you've defined a stream you like, you can give it a name and publish it
back to Kafka; `coast` tracks the mapping between stream names and their
definitions.  The `flow` package defines a rich Scala API for defining streams
and composing them together to build up a dataflow graph. You can use
these graphs in multiple ways: `dot` prints the graph in GraphViz format;
`machine` is a persistent in-memory implementation that's handy for unit
testing; and the `samza` backend can compile the graph to multiple [Samza
jobs][samza] and run it on a cluster.

If this all sounds promising, you might want to read through the
[heavily-commented 'Twitter reach' example][twitter-reach], or [this fork of the
hello-samza project][hello-coast].

[samza]: http://samza.incubator.apache.org/
[hello-coast]: https://github.com/bkirwi/incubator-samza-hello-samza/tree/hello-coast 
[twitter-reach]: core/src/main/scala/com/monovore/example/coast/TwitterReach.scala
[impossible]: http://ben.kirw.in/2014/11/28/kafka-patterns/

## Mandatory Word Count Example

```scala
val Sentences = Topic[Source, String]("sentences")

val WordCounts = Topic[String, Int]("word-counts")

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
```

## Missing Pieces

There's a lot of work do still; if you're interested, have a look at the [open
issues][issues], or check back in a few weeks.

[issues]: https://github.com/bkirwi/coast/issues
