
Notes
==

Sharing
===

A 'pure' DAG and a tree are indistinguishable, modulo object identity. For
example, there's no way to distinguish between the following two situations:

```scala
val one = ints.map { _ + 1 } merge ints.map { _ + 1 }

val tmp = ints.map { _ + 1 }
val two = tmp merge tmp
```

This is resolved by labelling the nodes we want shared. The implementation is
responsible for ensuring that labels are unique.

```scala
for {
  tmp <- register("incremented") {
    ints.map { _ + 1 }
  }
} yield tmp merge tmp
```


Grouping
===

As a 'shuffle' operation, regrouping streams is expensive.

Doing the grouping on the downstream end would involve reading all partitions,
so this needs to happen upstream. Without reading the downstream state, though,
it can't ensure that it's written each element to the stream exactly once.

In a distributed flow, regrouping is also the only operation that forces
messages across the wire. This implies that we can get away with only requiring
labels for regrouped streams. (Of course, more labels can be added to taste.)
The label doesn't really need to be applied right away -- but it needs to be there
before any accumulating happens.

This can be managed by tracking the 'regrouped' status in the types. In the same
way Finagle does the ServiceBuilder et al, `coast` can set a type-level flag when
grouping happens and use it to control which options are available.

Sinks
===

A 'sink' is an exported queue, and as such, we'd like to ensure that:
- our writes to it are exactly-once
- it contains no 'metadata', just the keys and values we intend

This is a bit tricky! Here's the best I have so far:

Assume the incoming stream is deterministic. We can look at the output stream, and
see what the latest offset we wrote is. We then replay the input stream from the
beginning. For every message in the output, we drop and increment until we reach the
last point that we can find downstream; we then continue to write to the stream as
normal.

If the input *isn't* deterministic, it seems the best `coast` can do is at-least-once, Samza
style. For now, we can assume that all the nondeterminism is from grouping and merges, and
stick this in the types as well.

State
===

State is hard!

Node schematic:

```
Stream[A,B0]  Stream[A,B1]  Stream[A,B2]
  |              |              |
  |              |              |      (S, BN) -> (S, Seq[B)
  \--------------+--------------/
                 |
               merge
                 |
  /--------------+--------------\
  |              |              |
Stream[A, S]  Stream[A, B]  Stream[A0, B0]
 (state)       (output)      (groupBy)
```

NB: every stream should be emitted under its own name. Kafka will give us back
the offset from that, and then we can emit to the grouped stream using that
offset. This will make it easier to dedup on the client, since we can then
guarantee that everything with the same (stream, offset) are identical and
consistent.

Names for State
===

A 'name' is public, and should be usable from multiple flows. For streams, this
is straightforward -- all the data is published on Kafka or whatever. For
pools, this gets trickier, because there's an initial state as well. Sticking that
to the name would be weird and make folds awkward.

Probably should only allow folks to subscribe to streams, and keep pools internal.
Should save a lot of heartache.

There's a bunch of state implicit in all deployed topologies -- for example, every
source needs to track how far along it is in the Kafka topic. All this state needs
to live somewhere; ideally, it should be migratable when the flow structure changes.

Some possibilities:

- Handle this purely (or primarily) structurally: all the state implicit in the AST
is flattened out into 'one big state' and stored together. This requires no labelling,
since we can always refer to the individual bits by their position in the AST. On the
other hand, it's very sensitive to small refactorings -- changes as minimal as adding
another spout will change the references, and craziness may result.

- Require that every bit of state get a state-label, which is unique per-process. This
is syntactially heavyweight, and is more likely to fail at runtime. (Global names are
a necessary evil for streams and processes, but they have a cost -- a name collision
can happen anywhere, so checking is nonlocal.)

- A hybrid JSON-style nesting: individual merge / etc. statements have a k/v mapping,
and individual pieces of state can be referred to by the path down through the maps.
This makes adding and removing keys free, but changing the overall structure or
redefining keys is still janky.

Machine Tests
===

What are you trying to prove?

Flows are equivalent when they publish the same streams, and every possible output
from one is also a possible output from the other.

When testing laws, we want to declare that two flows are exactly equivalent. In
theory, this involves generating all possible outputs for each and testing that
they're identical; in practice we can probably get away with generating a bunch of
random runs through the first one and testing that it's possible to get the same
output from the second. This involves a backtracking-type search, but in the normal
case it should be much more efficient?

When testing backends, we want to assert that the output is a possible output of
the model. This can use the same backtracking-type search above.

When testing flows, we want to assert some high-level property of the model.
Generating random runs and checking the property seems to be the thing here as well.

So a) generate random runs and b) check if an output is possible, returning either a
valid trace or an error.

Principles
===

- Reasonable: the API should expose primitives that simplify both formal and
  informal reasoning.

- Testable: it should be easy to write tests that check properties about the
  defined flow, without having to spin up a full distributed system.

- Efficient: Users should be able to write flows that are roughly as efficient
  as a handwritten version. In particular, the library should not force the
  user to keep unnecessary state or trips over the network.

- Transparent: It should be simple to see the mapping between the user's flow
  and the compiled version, at least in a black-box way.

Blockers
===

- Syntax for keyed joins, access to partition key
- Minimal required labelling