use std::{fmt, io, fmt::Debug};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::borrow::BorrowMut;
use std::collections::HashMap;

use futures::{Async, Future, IntoFuture, Poll, Sink, Stream, future::Either, future::select_all,
              sync::mpsc::UnboundedReceiver};
use raft_consensus::{ClientId, Consensus, ConsensusHandler, Log, ServerId, StateMachine,
                     handler::CollectHandler,
                     message::{ClientResponse, ConsensusTimeout, PeerMessage}};
use tokio::timer::Delay;
use tokio;
use rand::Rng;

pub struct RaftPeerProtocol<S, L, M>
where
    S: Stream<Item = PeerMessage, Error = io::Error>
        + Sink<SinkItem = PeerMessage, SinkError = io::Error>
        + Send,
    L: Log,
    M: StateMachine,
{
    id: ServerId,
    new_conns: UnboundedReceiver<(ServerId, S)>,
    handler: CollectHandler,
    consensus: Consensus<L, M>,
    conns: HashMap<ServerId, S>,
    heartbeat_timers: HashMap<ServerId, Delay>,
    election_timer: Option<Delay>,
    rng: ::rand::ThreadRng,
}

impl<S, L, M> RaftPeerProtocol<S, L, M>
where
    S: Stream<Item = PeerMessage, Error = io::Error>
        + Sink<SinkItem = PeerMessage, SinkError = io::Error>
        + Send,
    L: Log,
    M: StateMachine,
{
    pub fn new(
        new_conns: UnboundedReceiver<(ServerId, S)>,
        id: ServerId,
        peers: Vec<ServerId>,
        log: L,
        sm: M,
    ) -> Self {
        let mut handler = CollectHandler::new();
        let mut consensus = Consensus::new(id.clone(), peers, log, sm).unwrap();
        consensus.init(&mut handler);

        Self {
            id,
            new_conns,
            handler,
            consensus,
            conns: HashMap::new(),
            heartbeat_timers: HashMap::new(),
            election_timer: None,
            rng: ::rand::thread_rng(),
        }
    }

    fn apply_messages(&mut self) {
        if self.handler.peer_messages.len() > 0 || self.handler.timeouts.len() > 0
            || self.handler.clear_timeouts.len() > 0
        {
            println!("[{:?}] Applying: {:?}", self.id, self.handler);
        } else {
            return;
        }

        for (peer, messages) in self.handler.peer_messages.iter() {
            for message in messages {
                if let Some(conn) = self.conns.get_mut(&peer) {
                    println!("[{:?}] SEND {:?} to {:?}", self.id, message, peer);
                    conn.start_send(message.clone())
                        .map(|_| ())
                        .unwrap_or_else(|e| println!("Error sending packet to {:?}", peer));
                    conn.poll_complete().map(|_| ()).unwrap_or_else(|e| {
                        println!("Error in poll_complete sending packet to {:?}", peer)
                    });
                } else {
                    // TODO print error
                }
            }
        }

        for timeout in self.handler.timeouts.iter() {
            match timeout {
                &ConsensusTimeout::Heartbeat(id) => {
                    let mut timer =
                        tokio::timer::Delay::new(Instant::now() + Duration::from_millis(300));
                    // timer is definitely not ready yet, but we need notification aobut it
                    //timer.poll().unwrap();
                    self.heartbeat_timers.insert(id, timer);
                }
                &ConsensusTimeout::Election => {
                    let mut timer = tokio::timer::Delay::new(
                        Instant::now() + Duration::from_millis(self.rng.gen_range(1000, 2000)),
                    );
                    self.election_timer = Some(timer);
                }
            };
        }

        for timeout in self.handler.clear_timeouts.iter() {
            match timeout {
                &ConsensusTimeout::Heartbeat(id) => {
                    if let None = self.heartbeat_timers.remove(&id) {
                        // TODO: warn
                        //   println!("hb timer is requested to be removed but it doesn't exit in list");
                    };
                }
                &ConsensusTimeout::Election => {
                    self.election_timer = None;

                    //if let None = self.election_timer.take() {
                    //// TODO: warn
                    //println!("[{:?}] election timer is requested to be removed but it doesn't exist already", self.id);
                    //};
                }
            };
        }
        println!("[{:?}]: STATE {:?}", self.id, self.consensus.get_state());
        self.handler.clear();
    }
}

impl<S, L, M> Future for RaftPeerProtocol<S, L, M>
where
    S: Stream<Item = PeerMessage, Error = io::Error>
        + Sink<SinkItem = PeerMessage, SinkError = io::Error>
        + Send,
    L: Log,
    M: StateMachine,
{
    type Item = ();
    type Error = ();
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.apply_messages();
        // First of all - check if we have any new connections in queue
        loop {
            match self.new_conns.poll() {
                Ok(Async::NotReady) => break,
                Ok(Async::Ready(Some((id, conn)))) => {
                    println!("[{:?}] GOT CONN from {:?}", self.id, id);
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

        let keys = self.conns.keys().cloned().collect::<Vec<ServerId>>();

        let mut remove = Vec::new();
        for id in keys.into_iter() {
            let mut ready = false;
            loop {
                {
                    let conn = self.conns.get_mut(&id).unwrap();
                    match conn.poll() {
                        Ok(Async::Ready(None)) => {
                            println!("[{:?}] CONN ENDED {:?}", self.id, id);
                            //remove.push(id)
                            // TODO what if stream ends?
                            //unimplemented!();
                            break;
                        }
                        Ok(Async::Ready(Some(message))) => {
                            // TODO: trace!
                            println!("[{:?}] RECV: {:?}", self.id, message);
                            self.consensus
                                .apply_peer_message(&mut self.handler, id.clone(), message)
                                .unwrap(); // TODO error
                            ready = true;
                        }
                        Ok(Async::NotReady) => {
                            //
                            //println!("[{:?}] NOT READY {:?}", self.id, id);
                            break;
                        }
                        Err(_e) => {
                            // TODO: warn
                            println!("resetting connection from {:?} due to error: {:?}", id, _e);
                            // FIXME start reconnect future
                            // FIXME remove conn from the list
                            remove.push(id);
                            break;
                        }
                    }
                }
                if ready {
                    self.apply_messages();
                }
            }
        }

        for id in remove.into_iter() {
            println!("[{:?}] removing {:?}", self.id, id);
            self.conns.remove(&id);
        }

        // And at last, poll all timers. Notice that timers that maybe fired "at the same time"
        // as packet came, are already clear/reset
        let ready = {
            if let Some(ref mut timer) = self.election_timer {
                match timer.poll() {
                    Ok(Async::Ready(())) => true,
                    Ok(Async::NotReady) => false,
                    // TODO: process err
                    Err(e) => unimplemented!(),
                }
            } else {
                false
            }
        };

        if ready {
            println!("[{:?}] ELECTION TIMEOUT", self.id);
            // FIXME call election_timeout
            self.consensus
                .election_timeout(&mut self.handler)
                .unwrap_or_else(|e| {
                    println!("error in election timeout: {:?}", e);
                });

            //self.election_timer = None;

            self.apply_messages();
        }

        let mut ids = Vec::new();
        for (id, timer) in self.heartbeat_timers.iter_mut() {
            match timer.poll() {
                Ok(Async::Ready(())) => {
                    println!("[{:?}] HB TIMEOUT FOR {:?}", self.id, id);
                    ids.push(id.clone());
                }
                Ok(Async::NotReady) => (),
                // TODO: process err
                Err(e) => unimplemented!(),
            };
        }
        for id in ids.into_iter() {
            self.consensus
                .heartbeat_timeout(id)
                .map(|msg| {
                    self.handler.peer_messages.insert(id, vec![msg.into()]);
                })
                .unwrap_or_else(|e| {
                    println!("[{:?}] error in consensus: {:?}", self.id, e);
                });
            self.apply_messages();
        }
        Ok(Async::NotReady)
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
