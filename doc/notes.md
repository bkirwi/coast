
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

Principles
===

 -  Reasonable: the API should expose primitives that simplify both formal and
    informal reasoning.

 -  Testable: it should be easy to write tests that check properties about the
    defined flow, without having to spin up a full distributed system.

 -  Efficient: Users should be able to write flows that are roughly as efficient
    as a handwritten version. In particular, the library should not force the
    user to keep unnecessary state or trips over the network.

 -  Transparent: It should be simple to see the mapping between the user's flow
    and the compiled version.
