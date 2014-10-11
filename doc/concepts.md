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

A log is this, as a data structure. Kafka is built around this as a central
concept, Raft and MultiPaxos have a log at their heart, and many databases use
logs internally.

## Transforming

Taking a stream and, for every element, mapping it to zero or more elements in
the new stream. `map`, `filter`, `flatMap`.

## Aggregating

Like the above, but with some extra state. `fold`, etc. This is enough to
implement state machines and other things.

## Merging

Combines two streams into a single stream. This preserves ordering within a
stream, but in general there's no guarantees about in what order they're
interleaved.

## Partitioning

The above already gives you a pretty rich API, but it's not workable in a Big
Data Universe -- it's just not practical to funnel billions of events through a
single node in order. For this reason, we partition a stream up into many
smaller streams.

## Grouping

Sometimes you need to roll up information between distinct streams. Grouping
operations are here for this.

