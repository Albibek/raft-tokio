# raft-tokio
This crate implements a networking part of Raft consensus using tokio framework

At current state crate can be(and is) used to connect some instances of something using raft protocol between
them and knowing if node a leader or if it's not.

There is an example in `rafter` directory

# Current state
What currently works:
* leader election
* smart TCP connection handling
* getting current consensus state
* tuning raft timeouts

What doesn't work:
* client messaging
