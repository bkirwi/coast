
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

