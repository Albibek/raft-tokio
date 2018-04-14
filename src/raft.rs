use std::{fmt, io, fmt::Debug};
use std::cell::RefCell;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::collections::HashMap;

use futures::{Async, Future, IntoFuture, Poll, Sink, Stream, future::Either, future::select_all,
              sync::mpsc::Receiver};
use raft_consensus::{ClientId, Consensus, ConsensusHandler, Log, ServerId, StateMachine,
                     handler::CollectHandler,
                     message::{ClientResponse, ConsensusTimeout, PeerMessage}};
use tokio::timer::Delay;
use tokio;

struct RaftPeerProtocol<S, L, M>
where
    S: Stream<Item = PeerMessage, Error = io::Error>
        + Sink<SinkItem = PeerMessage, SinkError = io::Error>
        + Send,
    L: Log,
    M: StateMachine,
{
    new_conns: Receiver<(ServerId, S)>,
    conns: HashMap<ServerId, S>,
    consensus: SharedConsensus<L, M, RefCell<RaftPeerProtocol<S, L, M>>>,
    heartbeat_timers: HashMap<ServerId, Delay>,
    election_timer: Option<Delay>,
}

impl<S, L, M> RaftPeerProtocol<S, L, M>
where
    S: Stream<Item = PeerMessage, Error = io::Error>
        + Sink<SinkItem = PeerMessage, SinkError = io::Error>
        + Send,
    L: Log,
    M: StateMachine,
{
    pub fn new(conns: Receiver<(ServerId, S)>) -> Self {
        let raft = Self {
            new_conns: conns,
            conns: HashMap::new(),
            consensus: ????,
            heartbeat_timers: HashMap::new(),
            election_timer: None,
        };
    }
}

impl<S, L, M> Future for RaftPeerProtocol<S, L, M>
where
    S: Stream<Item = PeerMessage, Error = io::Error>
        + Sink<SinkItem = PeerMessage, SinkError = io::Error>
        + 'static
        + Send,
    L: Log,
    M: StateMachine,
{
    type Item = ();
    type Error = ();
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        // First of all - check if we have any new connections in queue
        loop {
            match self.new_conns.poll() {
                Ok(Async::NotReady) => break,
                Ok(Async::Ready(Some((id, conn)))) => {
                    self.conns.insert(id, conn);
                }
                Ok(Async::Ready(None)) => {
                    // all senders are closed: finish the future
                    return Ok(Async::Ready(()));
                }
                Err(e) => {
                    // TODO logging
                    println!("Error polling connections stream: {:?}", e);
                    break;
                }
            }
        }

        // Now poll all the connections until there is no packets left
        let mut ready = true;
        while ready {
            ready = false;
            let c = self.consensus.clone();
            self.conns.retain(|id, conn| {
                let (message, res) = match conn.poll() {
                    Ok(Async::Ready(None)) => {
                        // TODO what if stream ends?
                        unimplemented!()
                    }
                    Ok(Async::Ready(Some(message))) => {
                        ready = true;
                        // TODO: trace!
                        println!("GOT RAFT MSG: {:?}", message);
                        // FIXME: process message
                        (Some(message), true)
                    }
                    Ok(Async::NotReady) => (None, true),
                    Err(_e) => {
                        // TODO: warn
                        println!("resetting connection from {:?} due to error: {:?}", id, _e);
                        // FIXME start reconnect future
                        (None, false)
                    }
                };

                if let Some(message) = message {
                    c.apply_peer_message(id.clone(), message);
                }
                res
            });
        }

        // And at last, poll all timers. Notice that timers that maybe fired "at the same time"
        // as packet came, are already clear/reset

        let ready = if let Some(ref mut timer) = self.election_timer {
            match timer.poll() {
                Ok(Async::Ready(())) => true,
                Ok(Async::NotReady) => false,
                // TODO: process err
                Err(e) => unimplemented!(),
            }
        } else {
            false
        };
        if ready {
            // FIXME call election_timeout
            self.election_timer = None
        }

        let mut expired = Vec::new();
        self.heartbeat_timers.retain(|id, timer| {
            match timer.poll() {
                Ok(Async::Ready(())) => {
                    expired.push(id.clone());
                    true
                }
                Ok(Async::NotReady) => false,
                // TODO: process err
                Err(e) => unimplemented!(),
            }
        });
        expired
            .into_iter()
            .map(|_| {
                // FIXME: call heartbeat_timeout
                ()
            })
            .last();
        Ok(Async::NotReady)
    }
}

impl<S, L, M> Debug for RaftPeerProtocol<S, L, M>
where
    S: Stream<Item = PeerMessage, Error = io::Error>
        + Sink<SinkItem = PeerMessage, SinkError = io::Error>
        + 'static
        + Send,
    L: Log,
    M: StateMachine,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // TODO more information
        write!(f, "PeerProtocol")
    }
}

//struct RaftHandler<'a, S, L, M> {
//proto: &'a RaftPeerProtocol<S, L, M>,
//}

impl<S, L, M> ConsensusHandler for RaftPeerProtocol<S, L, M>
where
    S: Stream<Item = PeerMessage, Error = io::Error>
        + Sink<SinkItem = PeerMessage, SinkError = io::Error>
        + 'static
        + Send,
    L: Log,
    M: StateMachine,
{
    fn send_peer_message(&mut self, id: ServerId, message: PeerMessage) {
        match self.conns.get_mut(&id) {
            Some(sink) => {
                // TODO err handling
                sink.start_send(message).unwrap();
            }
            None => {
                // TODO: debug
                println!(
                    "no active connection found to  {:?} for message sending",
                    id
                )
            }
        }
    }

    fn send_client_response(&mut self, id: ClientId, message: ClientResponse) {
        unimplemented!()
    }

    fn set_timeout(&mut self, timeout: ConsensusTimeout) {
        match timeout {
            ConsensusTimeout::Heartbeat(id) => {
                let mut timer =
                    tokio::timer::Delay::new(Instant::now() + Duration::from_millis(500));
                // timer is definitely not ready yet, but we need notification aobut it
                timer.poll().unwrap();
                self.heartbeat_timers.insert(id, timer);
            }
            ConsensusTimeout::Election => {
                let mut timer =
                    tokio::timer::Delay::new(Instant::now() + Duration::from_millis(500));
                // timer is definitely not ready yet, but we need notification aobut it
                timer.poll().unwrap();
                self.election_timer = Some(timer);
            }
        };
    }

    fn clear_timeout(&mut self, timeout: ConsensusTimeout) {
        match timeout {
            ConsensusTimeout::Heartbeat(id) => {
                if let None = self.heartbeat_timers.remove(&id) {
                    // TODO: warn
                    println!("hb timer is requested to be removed but it doesn't exit in list");
                };
            }
            ConsensusTimeout::Election => {
                if let None = self.election_timer.take() {
                    // TODO: warn
                    println!(
                        "election timer is requested to be removed but it doesn't exist already"
                    );
                };
            }
        };
    }

    fn done(&mut self) {
        self.conns
            .iter_mut()
            .map(|(_, out)| {
                out.poll_complete().unwrap(); // TODO error
            })
            .last();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time;
    use tokio;
    use tokio::prelude::future;

    // //#[test]
    //fn test_timer() {
    //tokio::run(future::lazy(|| {
    //let d = SharedDelay::new(time::Instant::now() + time::Duration::from_secs(1));
    //let mut dd = d.clone();
    //tokio::spawn(d.and_then(move |_| {
    //println!("DELAY");
    //dd.reset(time::Instant::now() + time::Duration::from_secs(1));
    //Ok(())
    //}));
    //future::empty()
    //}));
    //}

}
