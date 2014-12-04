---
pagetitle: concepts
---

# Concepts

This page explains how `coast` models a stream processing job and some of the
guarantees it offers.

- consistent interface across all streams
- graph as a job description (definitional / declarative)
- ordering (vs. batch)

## Streams

`coast`'s streaming model is based closely on Kafka's log model. If you're not
familiar with Kafka, their documentation has [a great
introduction][kafka-intro].

[kafka-intro]: http://kafka.apache.org/documentation.html#introduction

`coast`'s core abstraction is a 'stream'. This maps closely to a Kafka topic: a
stream is made up of multiple partitions, each of which contains an ordered,
'append'-only series of values. Indeed, a typical `coast` job will start by
streaming data from one or more Kafka topics, and end by writing one or more
streams into another set of topics.

"The values in Kafka topic `lucky-numbers`" is a stream; but "the values in
`lucky-numbers`, but doubled", "the running total of the values in
`lucky-numbers`" are *also* streams. `coast` is careful to extend Kafka's
semantics to cover these derivative streams as well. 

Transformations, state, merges, grouping.

This is nice for the programmer: there's a consistent interface.

This also makes life easier for the framework itself: since `coast` understands
the structure of your streaming job at a very fine-grained level, it can provide
exactly-once semantics for many operations without incurring as much overhead as
other frameworks do.

## ???
