# raft-tokio
This crate implements a networking part of Raft consensus using tokio framework

At current state crate can be(and is) used to connect some instances of something using raft protocol between 
them and knowing if node a leader or if it's not.

There is also a rafter example in rafter directory made as an example and as a test

# Current state
What currently works:
* leader election
* smart TCP connection handling
* getting current consensus state
* providing raft options

What doesn't work:
* client messaging
