---
title: Samza Backend
---

`coast` can compile a flow down to a set of Samza jobs, which can be run
locally or on the cluster.

## Overview

`coast` leverages a lot of Samza's infrastructure, from its deployment model to
its state management approach. We'll touch briefly on a few key points of Samza's
design here -- if anything is unfamiliar, you'll want to follow the links through
to Samza's documentation.

Samza's job definitions are configuration-based: a single config file specifies
all the classes and metadata necessary to run the job.
`coast` integrates with Samza by compiling its own graph representation down to a set of Samza configs,
each of which defines an independent Samza job. 
You can write these configs back out to the file system,
to take advantage of the normal Samza deployment flow... 
or just run them immediately to launch the whole data flow graph on the cluster.

The generated jobs use Samza's usual approach to state management:
state can be stored in any of Samza's `KeyValueStore`s,
and a Kafka commit log is used for persistence.

## Backends

There are currently two distinct Samza backends,
aimed at somewhat different use cases.
`SimpleBackend` provides the most direct mapping between `coast`'s streaming model and Samza's primitives.
This gives you roughly the same flexibility and performance as a raw Samza job,
along with the same guarantees --
in particular, 
message sends and state updates may happen more than once,
and a restarted task may see an inconsistent message ordering.
If you're already happy with Samza's reliability story,
this backend might be a good choice.

As the name suggests, `SafeBackend` provides stronger guarantees --
it supports exactly-once state updates and message publishing,
even in the presence of failure.
This requires tracking some extra metadata and taking particular care with message ordering,
which does have a small performance cost.

> **NB:** The 'safe' backend is relatively new --
> it depends on some new features in Samza 0.9 --
> and the current version has a couple extra limitations:
> Kafka is the only supported system,
> and all topics are expected to have the same number of partitions.
> These issues should be resolved in the next release.

These 

## Configuration

`coast` respects Samza's existing config when possible, and defines a few properties
of its own:

- `coast.system.name`: At the moment, `coast` expects all inputs and outputs to
  be part of the same Samza 'system'. The default value is `kafka`.

- `coast.default.stores.*`: `coast` may generate multiple storage instances per Samza job,
  one for every join or stateful transformation. These config keys are used to set the
  default configuration for each 

You probably want to specify a
  storage factory here -- the in-memory store or the RocksDB backed storage are
  both good choices.

## `SamzaApp`

`coast-samza` also provides a simple `SamzaApp` template, which provides a basic
command-line interface.

```scala
object MyApp extends SamzaApp(backend = SimpleBackend) {

  val graph = ??? // coast graph goes here

}
```

The resulting application can be run with the usual Samza class runner:

```bash
bin/run-class.sh org.whatever.MyApp <command> <arguments>
```

Supported commands include:

-   `run`: Runs the job on the cluster. 

    ```bash
    bin/run-class.sh org.whatever.MyApp run --config-file config/base-config.properties
    ```

-   `gen-config`: Instead of running the jobs directly, this command writes the generated configs
    back out to a file. The generated files define 'vanilla' Samza jobs: each generated file is
    completely self-contained, and can be run with Samza's standard `bin/run-job.sh` script. This
    is a bit more work than having the application launch the jobs directly, of course, but it's also a
    bit more flexible -- for example, you might run the config-generation step on a build server
    but use an existing Samza deployment process to execute each job on the cluster.

-   `print-dot`: Print out a description of the graph in GraphViz's `dot` format. By default, this
    writes to standard out, but it's often more useful to write it to a file:

    ```bash
    bin/run-class.sh org.whatever.MyApp print-dot --to-file /tmp/my-app.dot && dot -Tjpg /tmp/my-app.dot
    ```

-   `info`: Lists a bunch of information about the job: inputs, outputs, and changelogs, as well
    as the merge and checkpoint streams for the 'safe' backend. This information may be useful if you
    configure your Kafka topics manually -- you can confirm that all the listed topics exist and have
    the proper settings before launching the job for the first time.
 
