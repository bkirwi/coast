---
pagetitle: samza
---

# Samza Backend

Coast can compile its dataflow graphs down to Samza jobs, which can be run
locally or on the cluster.

At the moment, there are actually two Samza backends: the `Safe` backend, which
preserves all of `coast`'s semantics when a task or whatever fails, and
`Simple`, which avoids all extra coordination overhead in favour of the best
possible performance. Currently, the overhead of the `Safe` backend is
non-negligible. In the near future, the coordination overhead should be reduced
significantly.

These improvements will require changing the quantity and format of data in
Kafka. As such, I can't recommend using the `Safe` backend for serious work.
