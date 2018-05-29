# raft-tokio
**(WIP)**
This crate implements a networking part of Raft consensus using tokio framework

# Current state
The project is in it's early-to-middle delvelopment stage. Some commits are compiling, some even work, but nothing is
guaranteed yet.

The commits that are known to work in some stable mode may be tagged with some tag.

What currently works:
* leader election
* smart TCP connection handling

What doesn't work:
* client messaging RPC
* getting current consensus state
* providing options
