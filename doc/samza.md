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


## Vagrant

