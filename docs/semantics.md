---
title: Semantics
---

Notes! Caveat lector.

In the spirit of denotational semantics, this sketches out a formal underpinning
for `coast`'s streaming operations.

# Streams are sequences

In computer science, _stream_ is often used to refer to an infinite sequence of
elements -- following one after the other, in order, and continuing indefinitely.

`coast` basically adopts this model. In practice, it's useful to think of
streams as "potentially" infinite; while we're never sitting around holding an
infinity of events, it's always possible that a new one might show up, no matter
how many we have.

This gives an easy model for most stream transformations: we're just mapping
streams to other streams.

It also gives a workable model for a single element of state that changes over
time: given an initial state, the stream can capture the evolution of that state
over time.

# Partitions as functions

While streams are convenient, they imposa an impractical total order on events
-- we still need a way to express 'independence' or 'concurrency'. As a model
for this, we use plain-old-functions. Our functions take a _partition key_, and
return a stream. This gives us our notion of independance -- the streams
returned by `f(a)` and `f(b)` have no implied ordering relationship. In Scala, a
naive type for a stream with `Int` keys and `String` values might be `Int =>
Stream[String]`.

We can still apply our usual transformations to a function-to-streams; we just
need to do it 'value-wise'

# Adding and removing concurrency

There are two operations that allow us to go from streams to
functions-of-streams and back again.

_Splitting_ a stream 'destroys' ordering information -- it takes a single stream
and splits it up into many streams. Given a function from stream values to keys,
we can get a function from keys to streams; this works a little bit like the
standard `.groupBy` function on `Scala` sequences.

_Merging_ a stream, on the other hand, 'creates' ordering information: given a
function-to-streams, it collapses all the elements from the entire range of the
function to a single stream. As mentioned above, there's no implied ordering
between the different keys... and this makes the result of a merge hopelessly
undefined. As far as this semantics goes, _any_ merging of the results is
allowed. This is an unfortunate nondeterminism, but it does an okay job of
modelling how messaging works in a world with unbounded delays.

You can get pretty far with this -- a standard regrouping operation can be
modelled as a split followed by a merge.
