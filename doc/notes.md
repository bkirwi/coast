
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

