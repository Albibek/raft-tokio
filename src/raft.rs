//! Raft-related types and futures
use std::collections::HashMap;
use std::ops::Range;
use std::time::{Duration, Instant};

use futures::{future::Either,
              sync::mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
              Async,
              Future,
              Poll,
              Sink,
              Stream};

use raft_consensus::{handler::CollectHandler,
                     message::{ClientResponse, ConsensusTimeout, PeerMessage},
                     ClientId,
                     Consensus,
                     Log,
                     ServerId,
                     StateMachine};
use tokio::prelude::future::*;
use tokio::timer::Delay;
use tokio_io::{codec::{Decoder, Encoder, Framed},
               AsyncRead,
               AsyncWrite};

use error::Error;
use handshake::{Handshake, HandshakeExt};
use rand::{OsRng, Rng};
use slog::{Drain, Logger};
use slog_stdlog::StdLog;

use Connections;

/// Basic consensus settings
#[derive(Debug, Clone)]
pub struct RaftOptions {
    pub heartbeat_timeout: Duration,
    pub election_timeout: Range<Duration>,
    pub logger: Logger,
}

impl Default for RaftOptions {
    fn default() -> Self {
        Self {
            heartbeat_timeout: Duration::from_millis(250),
            election_timeout: Duration::from_millis(500)..Duration::from_millis(750),
            logger: Logger::root(StdLog.fuse(), o!()),
        }
    }
}

impl RaftOptions {
    pub fn set_logger(&mut self, logger: Logger) {
        self.logger = logger
    }
}

/// This future will handle all the actions required for raft to start:
/// perform a handshake, save the conection and pass it through channel to raft dialog if everything is OK
/// S is a stream being passed.
/// C parameter is responsible for raft packets encoding
/// H is a future supposed to make a handshake returning ServerId and the rest of the stream as a
/// result
#[derive(Clone)]
pub struct RaftStart<S, C, H> {
    self_id: ServerId,
    peers: Connections,
    tx: UnboundedSender<(ServerId, Framed<S, C>)>,
    stream: S,
    handshake: H,
    codec: C,
    is_client: bool,
}

impl<S, C, H> RaftStart<S, C, H> where {
    pub fn new(
        self_id: ServerId,
        peers: Connections,
        tx: UnboundedSender<(ServerId, Framed<S, C>)>,
        stream: S,
        codec: C,
        handshake: H,
    ) -> Self {
        Self {
            self_id,
            peers,
            tx,
            stream,
            codec,
            handshake,
            is_client: true,
        }
    }

    pub fn set_is_client(&mut self, is_client: bool) {
        self.is_client = is_client
    }
}

impl<S, C, H> IntoFuture for RaftStart<S, C, H>
where
    S: AsyncRead + AsyncWrite + Send + 'static,
    C: Encoder<Item = PeerMessage, Error = Error>
        + Decoder<Item = PeerMessage, Error = Error>
        + Send
        + 'static,
    H: Handshake<S, Item = ServerId> + Send + 'static,
{
    type Item = ();
    type Error = Error;
    type Future = Box<Future<Item = Self::Item, Error = Self::Error> + Send>;

    fn into_future(self) -> Self::Future {
        let Self {
            self_id,
            peers,
            tx,
            stream,
            codec,
            handshake,
            is_client,
        } = self;
        let fut = stream
            .with_handshake(handshake)
            .and_then(move |(id, stream)| {
                let mut new = false;
                let mut peers = peers.0.lock().unwrap();
                peers.entry(id).or_insert_with(|| {
                    new = true;
                    is_client
                });
                let winner = if is_client {
                    self_id > id
                } else {
                    id > self_id
                };
                if !new && !winner {
                    Either::A(failed(Error::DuplicateConnection(id)))
                } else {
                    let stream = stream.framed(codec);
                    Either::B(
                        tx.send((id, stream))
                            .map_err(|_| Error::SendConnection)
                            .map(|_| ()),
                    )
                }
            });
        Box::new(fut)
    }
}

/// Implements forwarding peer messages to consensus, maintaining correct state changes and timeouts
pub struct RaftPeerProtocol<S, L, M>
where
    S: Stream<Item = PeerMessage, Error = Error>
        + Sink<SinkItem = PeerMessage, SinkError = Error>
        + Send,
    L: Log,
    M: StateMachine,
{
    new_conns_rx: UnboundedReceiver<(ServerId, S)>,
    disconnect: UnboundedSender<ServerId>,
    handler: CollectHandler,
    consensus: Consensus<L, M>,
    conns: HashMap<ServerId, S>,
    heartbeat_timers: HashMap<ServerId, Delay>,
    election_timer: Option<Delay>,
    rng: OsRng,
    options: RaftOptions,
}

impl<S, L, M> RaftPeerProtocol<S, L, M>
where
    S: Stream<Item = PeerMessage, Error = Error>
        + Sink<SinkItem = PeerMessage, SinkError = Error>
        + Send,
    L: Log,
    M: StateMachine,
{
    pub fn new(
        id: ServerId,
        peers: Vec<ServerId>,
        log: L,
        sm: M,
        disconnect: UnboundedSender<ServerId>,
        options: RaftOptions,
    ) -> (Self, UnboundedSender<(ServerId, S)>) {
        let mut handler = CollectHandler::new();
        let mut consensus = Consensus::new(id.clone(), peers, log, sm).unwrap();
        consensus.init(&mut handler);

        let (new_conns_tx, new_conns_rx) = unbounded();
        let s = Self {
            new_conns_rx,
            disconnect,
            handler,
            consensus,
            conns: HashMap::new(),
            heartbeat_timers: HashMap::new(),
            election_timer: None,
            rng: OsRng::new().unwrap(),
            options,
        };
        (s, new_conns_tx)
    }

    fn apply_messages(&mut self) {
        let logger = self.options.logger.clone();
        if self.handler.peer_messages.len() > 0 || self.handler.timeouts.len() > 0
            || self.handler.clear_timeouts.len() > 0
        {
            trace!(logger, "applying handler"; "handler" => format!("{:?}", self.handler));
        } else {
            return;
        }

        for (peer, messages) in self.handler.peer_messages.iter() {
            for message in messages {
                if let Some(conn) = self.conns.get_mut(&peer) {
                    trace!(logger, "send peer message"; "message"=>format!("{:?}", message), "remote"=>peer.to_string());
                    let elog = logger.clone();
                    conn.start_send(message.clone()).map(|_| ()).unwrap_or_else(
                        |e| warn!(elog, "could not start packet send"; "remote"=>peer.to_string(), "error"=>e.to_string()),
                    );

                    let elog = logger.clone();
                    conn.poll_complete().map(|_| ()).unwrap_or_else(
                        |e| warn!(elog, "could not complete packet send"; "remote"=>peer.to_string(), "error"=>e.to_string()),
                    );
                } else {
                    info!(logger, "skipped send to non-connected peer"; "remote"=>peer.to_string());
                }
            }
        }

        for timeout in self.handler.clear_timeouts.iter() {
            match timeout {
                &ConsensusTimeout::Heartbeat(id) => {
                    if let None = self.heartbeat_timers.remove(&id) {
                        debug!(logger, "request to remove non existent heartbeat timer"; "peer"=>id.to_string());
                    };
                }
                &ConsensusTimeout::Election => {
                    self.election_timer = None;

                    if let None = self.election_timer.take() {
                        debug!(logger, "request to remove non existent election timer");
                    };
                }
            };
        }

        let mut election_reset = false;
        for timeout in self.handler.timeouts.iter() {
            match timeout {
                &ConsensusTimeout::Heartbeat(id) => {
                    let timer = self.new_heartbeat_timer();
                    self.heartbeat_timers.insert(id, timer);
                }
                &ConsensusTimeout::Election => {
                    election_reset = true;
                }
            };
        }
        if election_reset {
            let timer = self.new_election_timer();
            self.election_timer = Some(timer);
        }
        trace!(logger, "consensus state after apply"; "state"=>format!("{:?}", self.consensus.get_state()));
        self.handler.clear();
    }

    fn new_heartbeat_timer(&self) -> Delay {
        let mut timer = Delay::new(Instant::now() + self.options.heartbeat_timeout);
        // timer is definitely not ready yet, and since we don't spawn it as a future, we want a notification about it
        timer.poll().unwrap();
        timer
    }

    fn new_election_timer(&mut self) -> Delay {
        let start = self.options.election_timeout.start.as_secs() * 1_000_000_000
            + self.options.election_timeout.start.subsec_nanos() as u64;
        let end = self.options.election_timeout.end.as_secs() * 1_000_000_000
            + self.options.election_timeout.end.subsec_nanos() as u64;
        let mut timer = Delay::new(
            Instant::now() + Duration::from_millis(self.rng.gen_range(start, end) / 1_000_000),
        );
        timer.poll().unwrap();
        timer
    }
}

impl<S, L, M> Future for RaftPeerProtocol<S, L, M>
where
    S: Stream<Item = PeerMessage, Error = Error>
        + Sink<SinkItem = PeerMessage, SinkError = Error>
        + Send,
    L: Log,
    M: StateMachine,
{
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.apply_messages();
        // first of all - check if we have any new connections in queue
        loop {
            match self.new_conns_rx.poll() {
                Ok(Async::NotReady) => break,
                Ok(Async::Ready(Some((id, conn)))) => {
                    info!(self.options.logger, "peer connected"; "peer"=>id.to_string());
                    self.conns.insert(id, conn);
                }
                Ok(Async::Ready(None)) => {
                    // all senders are closed: finish the future
                    info!(self.options.logger, "connection channel closed, exiting");
                    return Ok(Async::Ready(()));
                }
                Err(()) => {
                    warn!(self.options.logger, "error polling for new connections"); // error is () here
                    break;
                }
            }
        }

        let keys = self.conns.keys().cloned().collect::<Vec<ServerId>>();

        let mut remove = Vec::new();
        for id in keys.into_iter() {
            let logger = self.options.logger.new(o!("peer" => id.to_string()));
            let mut ready;
            loop {
                // new block here works around self.conns borrowing
                {
                    let conn = self.conns.get_mut(&id).unwrap();
                    match conn.poll() {
                        Ok(Async::Ready(None)) => {
                            debug!(logger, "connection closed");
                            remove.push(id);
                            break;
                        }
                        Ok(Async::Ready(Some(message))) => {
                            trace!(logger, "got peer message"; "message"=>format!("{:?}",message));
                            self.consensus
                                .apply_peer_message(&mut self.handler, id.clone(), message)
                                .map_err(|e| Error::Consensus(e))
                                .unwrap_or_else(
                                    |e| error!(logger, "Consensus error"; "error"=> e.to_string()),
                                );
                            ready = true;
                        }
                        Ok(Async::NotReady) => {
                            //trace!(self.options.logger, "not ready"); // this is too much even for trace
                            break;
                        }
                        Err(e) => {
                            debug!(logger, "error in connection, resetting"; "error" => e.to_string());
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
            self.conns.remove(&id);
            // unbounded channel should not return errors
            self.disconnect.start_send(id).unwrap();
            self.disconnect.poll_complete().unwrap();
        }

        // And at last, poll all timers. Notice that timers that maybe fired "at the same time"
        // as packet came, are already clear/reset
        let poll = {
            if let Some(ref mut timer) = self.election_timer {
                timer.poll()
            } else {
                Ok(Async::NotReady)
            }
        };
        match poll {
            Ok(Async::Ready(())) => {
                trace!(self.options.logger, "election timeout");
                self.consensus
                    .election_timeout(&mut self.handler)
                    .map_err(|e| Error::Consensus(e))
                    .unwrap_or_else(
                        |e| error!(self.options.logger, "Consensus error"; "error"=> e.to_string()),
                    );
                self.apply_messages();
            }
            Ok(Async::NotReady) => (),
            Err(e) => {
                warn!(self.options.logger, "unexpected timer error"; "error" => e.to_string(), "timer"=>"election");
                let timer = self.new_election_timer();
                self.election_timer = Some(timer);
            }
        }

        let mut timed_out = Vec::new();
        let logger = self.options.logger.clone();
        for (id, timer) in self.heartbeat_timers.iter_mut() {
            match timer.poll() {
                Ok(Async::Ready(())) => {
                    trace!(logger, "heartbeat timeout"; "peer"=>id.to_string());
                    timed_out.push(Either::A(id.clone()));
                }
                Ok(Async::NotReady) => (),
                Err(e) => {
                    warn!(self.options.logger, "unexpected timer error"; "error" => e.to_string(), "timer"=>"heartbeat", "peer"=>id.to_string());
                    timed_out.push(Either::B(id.clone()));
                }
            };
        }

        for id in timed_out.into_iter() {
            match id {
                Either::A(id) => {
                    self.consensus
                        .heartbeat_timeout(id)
                        .map(|msg| {
                            self.handler.peer_messages.insert(id, vec![msg.into()]);
                        })
                        .map_err(|e| Error::Consensus(e))
                        .unwrap_or_else(
                            |e| error!(self.options.logger, "Consensus error"; "error"=> e.to_string()),
                        );
                    self.apply_messages();
                }
                Either::B(id) => {
                    let timer = self.new_heartbeat_timer();
                    self.heartbeat_timers.insert(id, timer);
                }
            }
        }
        Ok(Async::NotReady)
    }
}

#[cfg(test)]
mod tests {
    // TODO tests
}
