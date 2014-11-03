# Samza

Notes on the Samza backend

## Deployment

A deployed Samza looks like this:

```
/app.tgz
/app
   /bin/run-*.sh
   /lib/*.jar
/config/app-task.properties
```

The tar includes all the samza code and everything else the job needs to
run -- except for the config, which is distributed separately.

The simplest possible integration seems to be as a config generator,
creating a config object for every labelled subgraph that includes a
serialized version of the code. We can go on to run these in-process using
the local job runner, or dump them to a file and make a nice YARNy
deployable.

The downside is that the generated config is not re-editable. (Or, at
least, parts of it shouldn't be changed -- and it's not obvious which bits.)

## Merging Streams

The life of a Samza message, as I understand it:

- A message is pulled off the network by polling a SystemConsumer

- Messages are buffered in Samza, then fed one-at-a-time (per stream) into
  a MessageChooser

- The MessageChooser is polled for a new message to process; it may return
  `null` if empty or disinclined

- The new message is fed into the StreamTask's processing method.

- Samza maybe saves the input offsets -- either if the task requested it,
  or periodically.

This structure is useful for simple things, but tricky for unusual things.
Take the merge, for example: we want to remember what order the input was fed
into the task with, so we can replay it if necessary. We now need to track
two sets of offsets: the offset in the merge stream, and the offsets in the
streams that are getting merged together. (Omit either, and you'd need to
replay the merge stream each time.) But the only way to get checkpoints saved
is to push messages from these streams all the way through the pipeline -- so
either we come up with our own checkpointing mechanism, or the whole setup
has to understand the merging style, which makes things tightly coupled.

## Merge Design

`coast` gets to do whatever it wants: so for now, for simplicity,
it'll probably just do merges the invasive way.

Path 1: get all input in an arbitrary order, track the high-water mark for all
input in the body, write out a stream of (name, partition, offset) tuples.

Path 2: custom input that reads off the merged stream, pulls in data from the
original stream, and feeds it into the task.

A nuance: the merge stream should be a bootstrap stream, so we can be sure the
high-water marks are updated before new data comes in.
