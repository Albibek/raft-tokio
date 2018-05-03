use std::io;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use std::collections::HashMap;

use futures::{Async, Future, Poll, Sink, Stream, future::Either,
              sync::mpsc::{unbounded, UnboundedReceiver, UnboundedSender}, sync::oneshot};

use raft_consensus::{ClientId, Consensus, ConsensusHandler, Log, ServerId, StateMachine,
                     handler::CollectHandler,
                     message::{ClientResponse, ConsensusTimeout, PeerMessage}};
use tokio::timer::Delay;
use tokio_io::{AsyncRead, AsyncWrite, codec::{Decoder, Encoder, Framed}};
use tokio::prelude::*;
use tokio::prelude::future::*;

use slog::{Drain, Logger};
use slog_stdlog::StdLog;
use rand::Rng;
use error::Error;
use handshake::{Handshake, HandshakeExt};

use Connections;

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
    H: Handshake<S>,
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
                {
                    let mut peers = peers.0.lock().unwrap();
                    peers.entry(id).or_insert_with(|| {
                        new = true;
                        let (tx, rx) = oneshot::channel();
                        Some(tx)
                    });
                }
                let is_winner = if is_client {
                    self_id > id
                } else {
                    id > self_id
                };
                if !new && is_winner {
                    Either::A(failed(Error::DuplicateConnection(id)))
                } else {
                    let stream = stream.framed(codec);
                    Either::B(tx.send((id, stream)).map_err(|_| Error::SendConnection))
                }
            });
        Box::new(fut.then(|_| Ok(())))
    }
}

/// Implements fowarding peer messages to consensus, maintaining correct state changes and timeouts
pub struct RaftPeerProtocol<S, L, M>
where
    S: Stream<Item = PeerMessage, Error = Error>
        + Sink<SinkItem = PeerMessage, SinkError = Error>
        + Send,
    L: Log,
    M: StateMachine,
{
    id: ServerId,
    new_conns_tx: UnboundedSender<(ServerId, S)>,
    new_conns_rx: UnboundedReceiver<(ServerId, S)>,
    handler: CollectHandler,
    consensus: Consensus<L, M>,
    conns: HashMap<ServerId, S>,
    heartbeat_timers: HashMap<ServerId, Delay>,
    election_timer: Option<Delay>,
    rng: ::rand::ThreadRng,
    logger: Logger,
}

impl<S, L, M> RaftPeerProtocol<S, L, M>
where
    S: Stream<Item = PeerMessage, Error = Error>
        + Sink<SinkItem = PeerMessage, SinkError = Error>
        + Send,
    L: Log,
    M: StateMachine,
{
    pub fn new<Log: Into<Option<Logger>>>(
        //new_conns: UnboundedReceiver<(ServerId, S)>,
        id: ServerId,
        peers: Vec<ServerId>,
        log: L,
        sm: M,
        logger: Log,
    ) -> (Self, UnboundedSender<(ServerId, S)>) {
        let mut handler = CollectHandler::new();
        let mut consensus = Consensus::new(id.clone(), peers, log, sm).unwrap();
        consensus.init(&mut handler);
        let logger = logger.into().unwrap_or(Logger::root(StdLog.fuse(), o!()));
        let logger = logger.new(o!("id" => id.to_string()));

        let (new_conns_tx, new_conns_rx) = unbounded();
        let s = Self {
            id,
            new_conns_tx: new_conns_tx.clone(),
            new_conns_rx,
            handler,
            consensus,
            conns: HashMap::new(),
            heartbeat_timers: HashMap::new(),
            election_timer: None,
            rng: ::rand::thread_rng(),
            logger,
        };
        (s, new_conns_tx)
    }

    fn apply_messages(&mut self) {
        if self.handler.peer_messages.len() > 0 || self.handler.timeouts.len() > 0
            || self.handler.clear_timeouts.len() > 0
        {
            trace!(self.logger, "applying handler"; "handler" => format!("{:?}", self.handler));
        } else {
            return;
        }

        for (peer, messages) in self.handler.peer_messages.iter() {
            for message in messages {
                if let Some(conn) = self.conns.get_mut(&peer) {
                    trace!(self.logger, "send peer message"; "message"=>format!("{:?}", message), "remote"=>peer.to_string());
                    let elog = self.logger.clone();
                    conn.start_send(message.clone()).map(|_| ()).unwrap_or_else(
                        |e| warn!(elog, "could not start packet send"; "remote"=>peer.to_string(), "error"=>e.to_string()),
                    );

                    let elog = self.logger.clone();
                    conn.poll_complete().map(|_| ()).unwrap_or_else(
                        |e| warn!(elog, "could not complete packet send"; "remote"=>peer.to_string(), "error"=>e.to_string()),
                    );
                } else {
                    warn!(self.logger, "connection entry not found"; "remote"=>peer.to_string());
                }
            }
        }

        for timeout in self.handler.timeouts.iter() {
            match timeout {
                &ConsensusTimeout::Heartbeat(id) => {
                    let mut timer = Delay::new(Instant::now() + Duration::from_millis(300));
                    // timer is definitely not ready yet, but we want a notification about it
                    timer.poll().unwrap();
                    self.heartbeat_timers.insert(id, timer);
                }
                &ConsensusTimeout::Election => {
                    let mut timer = Delay::new(
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
        trace!(self.logger, "consensus state after apply"; "state"=>format!("{:?}", self.consensus.get_state()));
        self.handler.clear();
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
        // First of all - check if we have any new connections in queue
        loop {
            match self.new_conns_rx.poll() {
                Ok(Async::NotReady) => break,
                Ok(Async::Ready(Some((id, conn)))) => {
                    debug!(self.logger, "peer connected"; "id"=>id.to_string());
                    self.conns.insert(id, conn);
                }
                Ok(Async::Ready(None)) => {
                    // all senders are closed: finish the future
                    debug!(self.logger, "connection channel closed, exiting");
                    return Ok(Async::Ready(()));
                }
                Err(()) => {
                    debug!(self.logger, "error polling for new connections"); // error is () here
                    break;
                }
            }
        }

        let keys = self.conns.keys().cloned().collect::<Vec<ServerId>>();
        //let log = self.log.new(o!("id" => self.id.into()));

        let mut remove = Vec::new();
        for id in keys.into_iter() {
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
                            if let Err(e) = self.consensus.apply_peer_message(
                                &mut self.handler,
                                id.clone(),
                                message,
                            ) {
                                warn!(logger, "unexpected consensus error"; "error" => e.to_string());
                                return Err(Error::Consensus(e));
                            }
                            ready = true;
                        }
                        Ok(Async::NotReady) => {
                            //trace!(self.logger, "not ready"); // this is too much even for trace
                            break;
                        }
                        Err(e) => {
                            debug!(self.logger, "error in connection, resetting"; "error" => e.to_string());
                            remove.push(id);
                            break;
                        }
                    }
                }

                if ready {
                    // FIXME start reconnect future
                    self.apply_messages();
                }
            }
        }

        for id in remove.into_iter() {
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
