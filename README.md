# coast

An experiment in streaming, with a focus on correctness, concision, and
friendliness.

**Caveat Haxxor:** This project is unreleased, immature, and changing rapidly. If you don't like what you see now, check back in a few weeks.

## Why Another Streaming Framework?

Consider this simple stream processing task: we want to write a job that pulls data from an input stream and write it to an output stream. (Let's say the input and output are stored in Kafka.) We'd like to copy the stream exactly: the first message in the input is the first message in the output, and so on.

None of the common large-scale streaming frameworks can do this without dropping, duplicating or reordering messages. (This includes Storm, Trident, Samza, Spark Streaming, and Kafka's MirrorMaker tool.) Of course, it's sometimes possible to make things work -- but such a simple job shouldn't require a distributed systems engineer to write.

This isn't an isolated case: many streaming calculations are similarly error-prone. Mistakes are common enough that many experts advocate using streaming frameworks for only [approximate, disposable calculations](http://en.wikipedia.org/wiki/Lambda_architecture#Speed_layer), and reproducing all the work in another system; this works, but it's a lot of effort, and it severely limits the number of applications you can build.

`coast` dreams of a better way.

## What's Here?

This project comprises:
- A simple, exactly-once streaming model; supporting transforming, splitting, and merging streams while maintainting persistent state. This model is designed to give strong guarantees about ordering and consistency, while allowing an implementation to scale to many machines.
- An idiomatic Scala API for defining streaming topologies.
- A small, in-memory backend for `coast`'s streaming model. This captures the nondeterminism that would be present in a distributed system, so testing with this helps you understand how your streaming job will behave on a real cluster.
- A pretty-printer that exports the DAG of your stream processing job to GraphViz format.

A backend that compiles the DAG into a set of Samza jobs is also in progress, but not complete. A [fork of the `hello-samza` project](https://github.com/bkirwi/incubator-samza-hello-samza/tree/hello-coast) has some examples.

## Mandatory Word Count Example

```scala
val flow = for {

  // split each sentence into words and regroup 
  countsByWord <- coast.label("counts-by-word") {
    coast.source(Sentences)
      .flatMap { sentence => sentence.split("\\s+") }
      .map { word => word -> 1 }
      .groupByKey
  }
  
  // add up the counts for each word, and stream out the list of changes
  _ <- coast.sink(WordCounts) {
    countsByWord
      .fold(0) { _ + _ }
      .stream
  }
  
} yield ()
```

## Future Work

- The Samza backend is incomplete, and all components need both polish and documentation.

- The current API only works nicely for small, pure functions. In the future, `coast` should have better support for processing that is asynchronous, nondeterministic, or requires initialization.

- Migrations are a common pain in any distributed context. `coast` has access to a lot of data and metadata, so it should be possible to provide built-in support for this. Good tooling here would be astonishingly useful.

- It should be possible to compile a `coast` flow into other popular streaming frameworks. (Storm is a good target; Spark Streaming is also promising.) This would make it easier to bring `coast`'s strong guarantees to places that have existing infrastructure for these other frameworks.
