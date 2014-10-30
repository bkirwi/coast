# Concepts

Most stream-processing systems lead to code that is hard to understand: message
ordering, reprocessing, cascading failures, etc. Entire architectures have been
designed around the idea that streaming data is fundamentally difficult, and
that the results of real-time processing just can't be trusted.

`coast` is designed around a simple set of abstractions, with the concepts of
exactly-once streaming at their heart. Etc., etc.

## Streams

A stream is an ordered, unbounded series of values. Over time, new values appear
at the end. Every individual event can be uniquely identified by how far they
are from the beginning.

### Sources and Sinks

A log is this, as a data structure. Kafka is built around this as a central
concept, Raft and MultiPaxos have a log at their heart, and many databases use
logs internally.

### Transforming and Aggregating Streams

One of the most basic operations you can do on a stream is to take each value,
apply some transformation, and make zero or more new values out of it.
(Splitting a sentence into a list of words, dropping invalid json.)
Chaining the resulting values together gives you a derivative stream.

Some of these operations require maintaining some state. (Dropping consecutive
duplicates.) `coast` calls operations like this 'aggregations'.

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
