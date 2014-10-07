coast mini
===

`mini` is a local runner for flows that's intended to model some of the
failure scenarios you find in a real distributed system.

akka
===

The 'obvious' implementation requires a lot of message passing; I'd like to see
if akka makes it easier to write this out.

Compiling down the AST, not clear if the actors should be spinning up their own
children or someone else should do the wiring. Points include:

- Supervision: do we want to knock over the whole system when a node crashes, or
  can we recover? Is it easier to get the right failure semantics when we hold
  all the nodes at the top level, or just the roots?

- Completion: pulling everyone to the top level makes it easier to check for
  completion, since we already have a topological sort. Tracing it through
  doesn't seem to be too painful, though.
