# coast

In this dark stream-processing landscape, `coast` is a ray of light.

## Why `coast`?

- **Simple:** `coast` provides a simple streaming model with strong ordering and
  exactly-once semantics. This straightforward behaviour extends across multiple
  machines, state aggregations, and even between independent jobs, making it
  easier to reason about how your entire system behaves.

- **Easy:** Streams are built up and wired together using a concise, idiomatic
  Scala API. These dataflow graphs can be as small or as large as you like:
  no need to cram all your logic in one big job, or to write a bunch
  of single-stage jobs and track their relationships by hand.

- **Kafkaesque:** `coast`'s core abstractions are patterned after Kafka's
  data model, and it's designed to fit comfortably in the middle of a larger
  Kafka-based infrastructure. By taking advantage of Kafka's messaging
  guarantees, `coast` can implement [exactly-once semantics][impossible] 
  for messages and state without a heavy coordination cost.

## Quick Introduction

`coast`'s streams are closely patterned after Kafka's topics: a stream has
multiple partitions, and each partition has an ordered series of values. A
stream can have any number of partitions, each of which has a unique key. 
You can  create a stream by pulling data from a topic, but `coast` also
has a rich API for building derivative streams: applying transformations,
merging streams together, regrouping, aggregating state, or performing joins.
Once you've defined a stream you like, you can give it a name and publish it
out to another topic. 

By defining streams and networking them together, it's possible to
express arbitrarily-complex dataflow graphs, including cycles and joins. You can 
use the resulting graphs in multiple ways: print it out as a GraphViz image,
unit-test your logic using a simple in-memory implementation, or compile the 
graph to multiple [Samza jobs][samza] and run it on a cluster.

If this all sounds promising, you might want to read through the
[heavily-commented 'Twitter reach' example][twitter-reach],
check out [this fork of the `hello-samza` project][hello-coast],
or peruse [the wiki][wiki].

[samza]: http://samza.apache.org/
[hello-coast]: https://github.com/bkirwi/incubator-samza-hello-samza/tree/hello-coast 
[twitter-reach]: core/src/main/scala/com/monovore/example/coast/TwitterReach.scala
[impossible]: http://ben.kirw.in/2014/11/28/kafka-patterns/
[wiki]: https://github.com/bkirwi/coast/wiki

## Getting Started

The 0.2.0 release is published on Bintray.
If you're using maven, you'll want to point your `pom.xml` at the repo:

```xml
<repository>
  <id>bintray-coast</id>
  <url>https://dl.bintray.com/bkirwi/maven</url>
</repository>
```

...and add `coast` to your dependencies:

```xml
<dependency>
  <groupId>com.monovore</groupId>
  <artifactId>coast-samza_2.10</artifactId>
  <version>0.2.0</version>
</dependency>
```

*Mutatis mutandis*, the same goes for SBT and Gradle.

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

## Future Work

If you're interested in what the future holds for `coast` --
or have questions or bugs to report -- 
come on over to the [issue tracker][issues].

[issues]: https://github.com/bkirwi/coast/issues
