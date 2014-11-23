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

## Lazy Merge Design

Simplest possible design: dump all incoming data to a single stream (including
stream name, partition, offset, payload), then consume that stream and
deduplicate before spilling it in to the main processing.

This avoids the need for a special-purpose merge stream. But it *is* pretty
heavy.

## State I Have

- Sources
    - state: (Map[partition, upstream], downstream)
    - frame: partition -> (upstream, downstream)

That's 4 extra bytes per message even for single-partition inputs -- I can
deal with that.

- Aggregations
    - state: (upstream, downstream, Map[key, value])
    - frame: key -> (upstream, downstream, value)

Can't push the offsets to the values: we always need to know the latest
downstream offset, and it involves a lot of repetition in LevelDB or whatever.

- Minimal Unification
    - state: (Map[partition, upstream], downstream, Map[key, value])
    - frame: (key, partition) -> (upstream, downstream, value)

Where {partition,key,value} might all be unit, depending.

Not the worst thing in the universe: there should be no wire overhead with
a good serialization format.

Is there ever a case where you'd want all of them together? Maybe if you
had something like `coast.source(In).fold(0) { _ + _ }`... you'd be able to
elide the original dedup.

Schematic control flow:

```scala
if (inputOffset >= offsets.get(partition) {
  val value = state.get(key)
  val (newDownstream, newValue) = doSomething(downstream, value)
  state.put(key)
  offsets.set(partition)

}
```