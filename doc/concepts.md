---
pagetitle: concepts
---

# Concepts

This page explains how `coast` models a stream processing job and some of the
guarantees it offers.

## Streams

`coast`'s streaming model is based closely on Kafka's log model.

> A log is perhaps the simplest possible storage abstraction. It is an
> append-only, totally-ordered sequence of records ordered by time.

Like Kafka topics, a stream is a totally-ordered sequence of values, where new
values are added at the end over time. "The values in Kafka topic
`lucky-numbers`" is a stream; but "the values in `lucky-numbers`, but doubled"
or "the running total of the values in `lucky-numbers`" are *also* streams.
`coast` provides an expressive API for reading data streams from Kafka, doing
complex transformations, and persisting them back.

Also like Kafka, `coast` has the concept of partitioning. In `coast`, each
partition has a 'key'; `coast` preserves the ordering of messages within a
partition, but does not synchronize 'between' partitions. This allows `coast` to
scale easily across a cluster.

## Semantics and Guarantees

Traditionally, streaming computation has been difficult, for a few different
reasons.

- Stream processing jobs have to deal with new messages constantly arriving from
  multiple sources. There space of possible inputs is very large, and the
  number of possible orderings is even larger.

- Jobs are running all the time, in a failure-prone environment. In a batch job,
  you can always rerun from scratch. Most people don't test their streaming jobs
  with failures at all; even if you do test with failures, it's hard to
  reproduce the complex failure scenarios that can arise when running across
  many machines.

This is hard to get right, and without support, developers have to get it right
every single time.

`coast` tries to cut through this complexity in a few ways. It offers
exceptionally strong guarantees, supporting exactly-once semantics for both
messaging and state updates in the presence of failures. `coast` also tries to
be explicit about what it does *not* guarantee -- by testing a bunch of
different possibilities, it's possible to be more confident that your stream
processing behaves well.

### Transforming and Aggregating Streams

One of the most basic operations you can do on a stream is to take each value,
apply some transformation, and make zero or more new values out of it. For
example, you might take a stream of new sentences, and split each sentence up to
make a list of words; or you might take a stream of JSON data and filter out
objects that are missing some required keys. Chaining the resulting values
together gives you a derivative stream. Other operations on a stream require
maintaining state, like tracking the average value for some metric. `coast`
keeps track of this state, so it can restore it in case of failure.

### Merging

Combines two streams into a single stream by interleaving them together. This
preserves ordering within a stream, but in general there's no guarantees about
in what order they're interleaved.

## Keys and Partitioning

The above already gives you a pretty rich API, but it's not workable in a Big
Data Universe -- it's just not practical to funnel billions of events through a
single node in order. For this reason, we partition a stream up into many
smaller streams.

Every value in the stream has an associated key. Under a given key, all the
operations are ordered. Transformations put their values under the same key, and
merges merge values under the same key together. Since each partition is
independent, this is easy to parallelize: we just process the events for
different partitions on different machines.

### Grouping

Sometimes you need to pull values from multiple streams together; for this,
`coast` offers one additional primitive: grouping. (If you're familiar with
Hadoop-style M/R, this corresponds to a reduce or shuffle operation.) Since this
step involves changing the way a particular event is partitioned, this usually
requires sending data across the network.

## Trees and Graphs

Transforming and regrouping streams would be a very limited API: your streaming
graph would be totally linear. Merges expand this a little bit: your streaming
graph becomes a tree.

Assembling all these different operations together, you get a streaming graph:
sources at the top, sinks at the bottom, and a rich network of interleaving
data flows connecting them together. This graph, or 'flow', is a complete
definition of a streaming job.

What's next? Have a look at the [flow builder API](flow.html).
