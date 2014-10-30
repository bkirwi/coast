# Flow API

`coast` defines an API that can express a wide variety of stream
transformations, including transformations, merges, splits, folds, and joins.

# Streams and Pools

The `Stream` type just represents a stream of values. The `Pool` type is like a
stream, but has a 'current' value as well.

It's easy to go back and forth between the two. The `stream.pool(init)` method
takes an initial value and makes a `Pool` that takes on each of the stream's
values in turn. Conversely, `pool.stream` returns a stream that has a new event
every time the value of the `Pool` changes.

Internally, these two are implemented using much the same primitives. The
distinction is just for convenience: it's much simpler to define joins and
things when you can talk about the 'current value', for example.

# API methods

`coast.source(name)`

`.map(fn)`, `flatMap(fn)`, `filter(fn)`, etc.

`.groupBy`, `.groupByKey`

`fold(init)(fn)`, `pool(init)`

`coast.merge(streams)`

`stream.join(pool)`

`pool.join(pool)`

# Defining Flows

This is a good API for building up `Stream`s and `Pool`s, but there are a few
things we need first.

A `Name` defines the 'public API' of a flow. `coast.source(name)` creates a
stream that reads from that resource; `coast.sink(name)(stream)` writes a stream
to that resource. There really ought to only be one writer.

Sometimes it's necessary to write out intermediate results as well. For this
use, `coast` has `coast.label(string)(stream or pool)`. This is useful for a
couple reasons:

- When writing the results out `coast` can ensure that events with the same key
  end up together. For this reason, it's often necessary to add a label after
  changing the grouping.

- If you have some intermediate results that you want to process in two
  different ways, labelling the intermediate results lets you avoid repeating
  work.

'Labelling' or 'sinking' a `Stream` returns a value of a new type, `Flow`. This
type is responsible for remembering which streams are attached to which names.
Of course, labelling a single stream defines just one name binding, which is not
very interesting. To define more complex flows, you need to be able to compose
these smaller ones together. In `coast`, the idiomatic way to do this is to use
a 'for expression', like so:

```scala
for {
  first <- coast.label("first") {
    coast.source(name).map { ... some transformation ... }.groupBy { ... }
  }

  _ <- coast.sink(Out1) {
    first.flatMap { ... }
  }

  pool <- coast.label("pool") {
    first.fold(0) { _ + _ }
  }

  _ <- coast.sink(Out2) { 
    pool.stream
  }

} yield ()
```

This makes it obvious which stream has which name, and what the full definition
is. `coast` takes care of all the wiring behind the scenes.
