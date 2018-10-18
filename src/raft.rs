//! Raft related types and futures
use std::collections::HashMap;
use std::ops::Range;
use std::time::{Duration, Instant};

use futures::{
    future::Either,
    sync::mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
    Async, Future, Poll, Sink, Stream,
};

use raft_consensus::{
    handler::CollectHandler,
    message::{ConsensusTimeout, PeerMessage},
    state::ConsensusState,
    Consensus, Log, ServerId, StateMachine,
};
use tokio::prelude::future::*;
use tokio::timer::Delay;
use tokio_io::{AsyncRead, AsyncWrite};

use codec::IntoTransport;
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
}

impl Default for RaftOptions {
    fn default() -> Self {
        Self {
            heartbeat_timeout: Duration::from_millis(250),
            election_timeout: Duration::from_millis(500)..Duration::from_millis(750),
        }
    }
}

/// This trait allows to check if the connection is a duplicate of another one
/// made from other side. Connection will be dropped if `solve` returned false.
/// Obviously, this is only checked if more than one connection is availableю
pub trait ConnectionSolver {
    fn solve(&self, is_client: bool, local_id: ServerId, remote_id: ServerId) -> bool;
}

/// A solver where connection with lower ServerId is dropped
#[derive(Clone)]
pub struct BiggerIdSolver;

impl ConnectionSolver for BiggerIdSolver {
    fn solve(&self, is_client: bool, local_id: ServerId, remote_id: ServerId) -> bool {
        if is_client {
            local_id > remote_id
        } else {
            remote_id > local_id
        }
    }
}

/// This future will handle all the actions required for raft to start.
///
/// Actions: Perform a handshake, save the conection and pass it through channel to raft dialog if everything is OK
/// S is a stream being passed.
/// T is a parameter is responsible for encoding/decoding raft packets from connection
/// H is a future supposed to make a handshake returning ServerId and the rest of the stream as a
/// result
#[derive(Clone)]
pub struct RaftStart<S, T, H, C>
where
    S: AsyncRead + AsyncWrite + Send + 'static,
    T: IntoTransport<S, PeerMessage> + Send + 'static,
    C: ConnectionSolver + Send + 'static,
{
    self_id: ServerId,
    peers: Connections,
    tx: UnboundedSender<(ServerId, T::Transport)>,
    stream: S,
    transport: T,
    handshake: H,
    is_client: bool,
    conn_solver: C,
}

impl<S, T, H, C> RaftStart<S, T, H, C>
where
    S: AsyncRead + AsyncWrite + Send + 'static,
    T: IntoTransport<S, PeerMessage> + Send + 'static,
    C: ConnectionSolver + Send + 'static,
{
    pub fn new(
        self_id: ServerId,
        peers: Connections,
        tx: UnboundedSender<(ServerId, T::Transport)>,
        stream: S,
        transport: T,
        handshake: H,
        conn_solver: C,
    ) -> Self {
        Self {
            self_id,
            peers,
            tx,
            stream,
            transport,
            handshake,
            conn_solver,
            is_client: true,
        }
    }

    pub fn set_is_client(&mut self, is_client: bool) {
        self.is_client = is_client
    }
}

impl<S, T, H, C> IntoFuture for RaftStart<S, T, H, C>
where
    S: AsyncRead + AsyncWrite + Send + 'static,
    T: IntoTransport<S, PeerMessage> + Send + 'static,
    H: Handshake<S, Item = ServerId> + Send + 'static,
    C: ConnectionSolver + Send + 'static,
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
            transport,
            handshake,
            conn_solver,
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
                // let winner = if is_client {
                //self_id > id
                //} else {
                //id > self_id
                // };
                if !new && !conn_solver.solve(is_client, self_id, id) {
                    Either::A(failed(Error::DuplicateConnection(id)))
                } else {
                    let framed = transport.into_transport(stream); //codec.framed(stream);
                    Either::B(
                        tx.send((id, framed))
                            .map_err(|_| Error::SendConnection)
                            .map(|_| ()),
                    )
                }
            });
        Box::new(fut)
    }
}

/// Provides useful information from protocol about state changing
pub trait Notifier {
    #[allow(unused_variables)]
    fn state_changed(&mut self, old: ConsensusState, new: ConsensusState) {}
}

/// The notification handler ignoring all notifications
pub struct DoNotNotify;
impl Notifier for DoNotNotify {}

/// Implements forwarding peer messages to consensus, maintaining correct state changes and timeouts
pub struct RaftPeerProtocol<S, L, M, N>
where
    S: Stream<Item = PeerMessage, Error = Error>
        + Sink<SinkItem = PeerMessage, SinkError = Error>
        + Send,
    L: Log,
    M: StateMachine,
    N: Notifier,
{
    new_conns_rx: UnboundedReceiver<(ServerId, S)>,
    disconnect: UnboundedSender<ServerId>,
    handler: CollectHandler,
    consensus: Consensus<L, M>,
    notifier: N,
    conns: HashMap<ServerId, S>,
    heartbeat_timers: HashMap<ServerId, Delay>,
    election_timer: Option<Delay>,
    rng: OsRng,
    options: RaftOptions,
    logger: Logger,
}

impl<S, L, M, N> RaftPeerProtocol<S, L, M, N>
where
    S: Stream<Item = PeerMessage, Error = Error>
        + Sink<SinkItem = PeerMessage, SinkError = Error>
        + Send,
    L: Log,
    M: StateMachine,
    N: Notifier,
{
    pub fn new(
        id: ServerId,
        peers: Vec<ServerId>,
        log: L,
        sm: M,
        notifier: N,
        disconnect: UnboundedSender<ServerId>,
        options: RaftOptions,
    ) -> (Self, UnboundedSender<(ServerId, S)>) {
        let mut handler = CollectHandler::new();
        let mut consensus = Consensus::new(id, peers, log, sm).unwrap();
        consensus.init(&mut handler);

        let (new_conns_tx, new_conns_rx) = unbounded();
        let s = Self {
            new_conns_rx,
            disconnect,
            handler,
            consensus,
            notifier,
            conns: HashMap::new(),
            heartbeat_timers: HashMap::new(),
            election_timer: None,
            rng: OsRng::new().unwrap(),
            logger: Logger::root(StdLog.fuse(), o!()),
            options,
        };
        (s, new_conns_tx)
    }

    pub fn set_logger(&mut self, logger: Logger) {
        self.logger = logger
    }

    fn apply_messages(&mut self) {
        let logger = self.logger.clone();
        if self.handler.peer_messages.is_empty()
            && self.handler.timeouts.is_empty()
            && self.handler.clear_timeouts.is_empty()
        {
            return;
        } else {
            trace!(logger, "applying handler"; "handler" => format!("{:?}", self.handler));
        }

        for (peer, messages) in &self.handler.peer_messages {
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

        for timeout in &self.handler.clear_timeouts {
            match *timeout {
                ConsensusTimeout::Heartbeat(id) => {
                    if self.heartbeat_timers.remove(&id).is_none() {
                        debug!(logger, "request to remove non existent heartbeat timer"; "peer"=>id.to_string());
                    };
                }
                ConsensusTimeout::Election => {
                    self.election_timer = None;

                    if self.election_timer.take().is_none() {
                        debug!(logger, "request to remove non existent election timer");
                    };
                }
            };
        }

        let mut election_reset = false;
        for timeout in &self.handler.timeouts {
            match *timeout {
                ConsensusTimeout::Heartbeat(id) => {
                    let timer = self.new_heartbeat_timer();
                    self.heartbeat_timers.insert(id, timer);
                }
                ConsensusTimeout::Election => {
                    election_reset = true;
                }
            };
        }
        if election_reset {
            let timer = self.new_election_timer();
            self.election_timer = Some(timer);
        }
        trace!(logger, "apply finished"; "state"=>format!("{:?}", self.handler.state));
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
            + u64::from(self.options.election_timeout.start.subsec_nanos());
        let end = self.options.election_timeout.end.as_secs() * 1_000_000_000
            + u64::from(self.options.election_timeout.end.subsec_nanos());
        let mut timer = Delay::new(
            Instant::now() + Duration::from_millis(self.rng.gen_range(start, end) / 1_000_000),
        );
        timer.poll().unwrap();
        timer
    }
}

impl<S, L, M, N> Future for RaftPeerProtocol<S, L, M, N>
where
    S: Stream<Item = PeerMessage, Error = Error>
        + Sink<SinkItem = PeerMessage, SinkError = Error>
        + Send,
    L: Log,
    M: StateMachine,
    N: Notifier,
{
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let state = self.handler.state.clone();
        self.apply_messages();
        // first of all - check if we have any new connections in queue
        loop {
            match self.new_conns_rx.poll() {
                Ok(Async::NotReady) => break,
                Ok(Async::Ready(Some((id, conn)))) => {
                    info!(self.logger, "peer connected"; "peer"=>id.to_string());
                    self.conns.insert(id, conn);
                }
                Ok(Async::Ready(None)) => {
                    // all senders are closed: finish the future
                    info!(self.logger, "connection channel closed, exiting");
                    return Ok(Async::Ready(()));
                }
                Err(()) => {
                    warn!(self.logger, "error polling for new connections"); // error is () here
                    break;
                }
            }
        }

        let keys = self.conns.keys().cloned().collect::<Vec<ServerId>>();

        let mut remove = Vec::new();
        for id in keys {
            let logger = self.logger.new(o!("peer" => id.to_string()));
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
                                .apply_peer_message(&mut self.handler, id, message)
                                .map_err(Error::Consensus)
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

        for id in remove {
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
                trace!(self.logger, "election timeout");
                self.consensus
                    .election_timeout(&mut self.handler)
                    .map_err(Error::Consensus)
                    .unwrap_or_else(
                        |e| error!(self.logger, "Consensus error"; "error"=> e.to_string()),
                    );
                self.apply_messages();
            }
            Ok(Async::NotReady) => (),
            Err(e) => {
                warn!(self.logger, "unexpected timer error"; "error" => e.to_string(), "timer"=>"election");
                let timer = self.new_election_timer();
                self.election_timer = Some(timer);
            }
        }

        let mut timed_out = Vec::new();
        let logger = self.logger.clone();
        for (id, timer) in &mut self.heartbeat_timers {
            match timer.poll() {
                Ok(Async::Ready(())) => {
                    trace!(logger, "heartbeat timeout"; "peer"=>id.to_string());
                    timed_out.push(Either::A(*id));
                }
                Ok(Async::NotReady) => (),
                Err(e) => {
                    warn!(self.logger, "unexpected timer error"; "error" => e.to_string(), "timer"=>"heartbeat", "peer"=>id.to_string());
                    timed_out.push(Either::B(*id));
                }
            };
        }

        for id in timed_out {
            match id {
                Either::A(id) => {
                    self.consensus
                        .heartbeat_timeout(id)
                        .map(|msg| {
                            self.handler.peer_messages.insert(id, vec![msg.into()]);
                        }).map_err(Error::Consensus)
                        .unwrap_or_else(
                            |e| error!(self.logger, "Consensus error"; "error"=> e.to_string()),
                        );
                    self.apply_messages();
                }
                Either::B(id) => {
                    let timer = self.new_heartbeat_timer();
                    self.heartbeat_timers.insert(id, timer);
                }
            }
        }
        if state != self.handler.state {
            self.notifier
                .state_changed(state, self.handler.state.clone());
        }
        Ok(Async::NotReady)
    }
}

#[cfg(test)]
mod tests {
    // TODO tests
}
