# Standalone

- New consumer leader election to balance work, coordinating through Kafka

  Coordination via the 'merge' stream.

## Topic Assignment

The standalone backend uses Kafka's new partition assignment machinery because,
hey, free partition assignment.

A graph is split up into a set of *tasks*,
each of which can run independently.

Each task is backed by a single partition,
used for logging nondeterministic choices.
Instead of using a single topic for all of these partitions,
we split them up into a set of named 'topic groups';
this lets us keep a stable state even if update the original graph.

The Kafka client uses a PartitionAssigner abstraction
to do the partition assignment,
which has access to the consumer config and not much else.
The assigner and a general description of the topology
are added to the config at startup,
and the final grouping of partitions into tasks
happens as part of the assignment.

To keep input and output offsets in sync,
coast commits them all as metadata in the OffsetAndMetadata record.
This means it needs to manually set those streams
to specific offsets in the rebalance callback,
but it needs to do lots of work in that callback anyways.

## Structure

Each task has a log, and stores checkpointed offsets in the offset metadata.

This includes:

- The offsets of all inputs.
  - The per-'source' offsets too.
- The offsets of all outputs.
- Offsets of all state

The task log is the coordination point for the task;

Order:
- Publish to log
- Publish task output, blocking until log is written
- Persist state, blocking on output
- Persist checkpoint

## Task Rep

Two-way flow of data: 
messages flow 'down' through the tree,
and offsets flow back up.

NB: consumer forgets its offsets when rebalancing!
This is a great reason for the tasks to store & make available the 
'next offset wanted'.

Bad: a stateful task updates state without sending output messages.
(Without a way to rewind the state to some past time,
there's no way to recover what it was when those messages were sent.)
Unless we force each state impl to keep a history of all values,
it's possible that some changes will be lost...
either because of log compaction in Kafka,
or because of a partial failure updating some KV store.
When we restart from a checkpoint,
we may need to alternate between keys that 'include' the upstream message,
which we skip,
and keys that don't,
where we do the full processing.
While that's happening,
we need a way to tell the downstream that we're 'skipping ahead'
a certain distance in the stream.

The samza backend just returns a Long with the new downstream offset;
we also need a channel for the 'ack' of that offset,
since that's likely to happen asynchronously.

It's really tempting to link the Kafka and Distruptor offsets,
since juggling two sets of offsets sounds pretty messy,
but I don't think that'll jive well with the skipping-aheads
and other weirdness that we'll need to make work.

--- 

Fairly complex async dataflow:
