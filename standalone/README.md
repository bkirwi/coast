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

