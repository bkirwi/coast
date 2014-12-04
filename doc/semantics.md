---
pagetitle: semantics
---

# Semantics

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
*every single time*.

`coast` tries to cut through this complexity in a few ways. It offers
exceptionally strong guarantees, supporting exactly-once semantics for both
messaging and state updates in the presence of failures. `coast` also tries to
be explicit about what it does *not* guarantee -- by testing a bunch of
different possibilities, it's possible to be more confident that your stream
processing behaves well.

# UUUUUUUUUUUU

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
