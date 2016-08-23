---
title: Flow API
---

This page covers the Scala API used to build up a streaming graph from scratch.

## Learning by Example

At the moment, the best way to learn the basics is to read through the [Twitter
Reach example][twitter-reach]: it reviews everything from defining topics to
implementing streaming joins.

[twitter-reach]: https://github.com/bkirwi/coast/blob/master/core/src/main/scala/com/monovore/example/coast/TwitterReach.scala

This page collects a little extra information:
we'll go into more detail on some finer points of the API,
and look at some unusual features.

## Partitioning and Serialization

In the `wire` package, 
`coast` defines a couple abstractions
that allow you to configure serialization and partitioning on a per-type basis.

- `BinaryFormat`: This provides a basic serialization interface. `coast`
  requires an implementation of this anywhere data is getting sent over the
  network -- whether updating state or writing back to Kafka.

- `Partitioner`: This is used to assign a 'logical' partition (indexed by an
  arbitrary key) to a 'physical' partition (indexed by a small integer). This is
  very similar to Kafka's partitioning interface.



## Named Streams and Pools

In the flow API, you can give a specific name to a stream:

```scala
val namedStream = Flow.stream("stream-name") { 
  // stream definition
}
```

On a cluster, this ensures the data is written back out to Kafka.

There are a couple of reasons you might want to do this:

- Sharing: if an intermediate stream is expensive to compute, sending it over
  the network may be cheaper than recomputing it.

- Determinism: when you merge two streams, a variety of interleavings are
  possible. By naming a stream, you ensure that all downstream processing sees
  the same ordering.

- Regrouping: one of the advantages of partitioning is that all state for
  messages with the same key can be maintained on a single node. This means
  that, when you change the partitioning of the stream, you need to shuffle data
  over the network. If you try to do some stateful processing before naming the
  stream, the compiler will warn you about it.

## Cycles

Most streaming graphs in the wild are 'acyclic' -- messages flow in one direction
from input to output. This is a good thing! Graphs without cycles have a simpler
data flow, so they're often easier to build and understand.

Unfortunately, some streaming problems are inherently cyclic. Suppose you have a stream of pages
partitioned by URL, and you want to run a continuous page-rank calculation over the
stream; updating the rank for one page may change the rank of all the pages
it links to, so the output of one partition's calculation is the
input for another. A lot of calculations that have an *iterative* structure when run
in batch have a cyclic *structure* when translated to the streaming world, so
handling cycles is an important task for a streaming framework.

You introduce a cycle like this:

```scala
val cyclicFlow = Flow.cycle[Key, Message]("stream-name") { cyclicStream =>
  // stream definition goes here
}
```

This looks like a regular stream definition, but there are a few key differences:

- The `cyclicStream` is passed in as a parameter, so you can use it as part of the
  stream's definition. (Sound self-referential? That's the cycle!)

- You have to write the key and value types explicitly... there's not enough context
  here for the compiler to infer it.
