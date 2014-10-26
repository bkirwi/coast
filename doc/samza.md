# Samza

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

## Problems

The log-based strategy for exactly-once processing doesn't work well with
Samza's checkpointing. Samza has different paths for offsets and state: offsets
are maintained with the checkpointing mechanism, and state is maintained via a
changelog. But it seems like there's no coordination between the two -- state
may be saved before the offsets of the messages that contributed to it, or vice
versa. (It seems like the latter situation actually violates Samza's
exactly-once guarantee -- I should ask about this.)

I really want just one checkpoint topic, and to persist all state changes and
offset increases to that topic.

## Vagrant

