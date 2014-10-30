# coast

An experiment in streaming, with a focus on correctness, concision, and
friendliness.

**Caveat Haxxor:** This project is unreleased, immature, and changing rapidly. If you don't like what you see now, check back in a few weeks.

## Why Another Streaming Framework?

Writing a correct, scalable stream processing application is notoriously difficult; in fact, the designer of the leading stream processing framework advocates throwing away your results and starting from scratch every few hours. This severely limits the number of applications you can build on this infrastructure.

It doesn't have to be this way. With careful design, and by leaning heavily on log-backed storage, it's possible to recover most of the simplicity of batch processing in a streaming context.

## What's Here?

This project comprises:
- A simple, exactly-once streaming model; supporting transforming, splitting, and merging streams while maintainting persistent state. This model is designed to give strong guarantees about ordering and consistency, while allowing an implementation to scale to many machines.
- An idiomatic Scala API for defining streaming topologies.
- A small, in-memory backend of `coast`'s streaming model. This captures the nondeterminism that would be present in a distributed system, so testing with this helps you understand how your streaming job will behave on a real cluster.
- A pretty-printer that exports the DAG of your stream processing job to GraphViz format.

A backend that compiles the DAG into a set of Samza jobs is also in progress, but not complete.

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
    byWord
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
