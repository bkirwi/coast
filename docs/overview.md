---
title: Overview
---

`coast` borrows a number of terms from Kafka and other streaming projects,
and introduces a few new ideas of its own.
In this page,
we'll walk through some of the most important pieces
and look at how they fit together. 

## Topics, Streams, and Graphs

Think of a stream processing job as a black box: you have
messages flowing in at the top, some processing happens, and some new data flows
out at the bottom. `coast` refers to these inputs and outputs as **topics**. You
can think of the set of input and output topics as your job's "public API" --
other parts of the system only interact with it by providing it input or
consuming the output.

Of course, your job is not really a black box! With `coast`, you describe the
way data flows from input to output by defining **streams**. You can create a
stream my reading data from a topic, by transforming an existing stream, or by
combining multiple streams together. This basic vocabulary is expressive enough
to cover a wide variety of stream processing tasks. Once you've built up the
output you want, you can sink it directly to an output topic.

A complete stream processing job -- with all its inputs, outputs, and arbitrary
topology of internal streams -- is referred to as a **graph**. `coast` includes 
several utilities for testing and visualizing these graphs -- and, of course, 
executing them on a cluster.

## Partitions and Ordering

Every topic -- and every stream -- is divided up into an arbitrary number of
**partitions**. Each partition in a stream is identified by a unique key and
holds an unbounded series of messages.

Most of the basic operations in `coast` are defined 'partitionwise':

- Messages within a partition have a well-defined order, but `coast` defines no
  particular ordering between messages in different partitions or different
  streams.

- All state is local to a partition. (You can think of this as a key-value
  store, where `coast` stores the current state for each partition key.)

- When you merge multiple streams, partitions with the same key are merged
  together.

Partitioning is very useful for parallelism: different partitions can be
assigned to different nodes, so processing can scale easily across a cluster.
By default, processing for each partition happens independently, but it's easy
to repartition a stream when you want to bring data from multiple partitions
together.
This lends itself to a map/reduce style of computation --
you do as much as you can in parallel,
reshuffling the data when you need to aggregate across multiple messages.
