use std::net::SocketAddr;
use std::time::{Duration, Instant};
use std::collections::HashMap;

use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;
use tokio::prelude::future::*;
use tokio::timer::Delay;
use tokio_io::codec::{Decoder, Encoder, Framed};

use futures::sync::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::IntoFuture;

use slog::{Drain, Logger};
use slog_stdlog::StdLog;

use raft_consensus::ServerId;
use raft_consensus::message::*;

use raft::RaftStart;
use handshake::Handshake;
use error::Error;

use Connections;

pub struct TcpWatch<C, H> {
    id: ServerId,
    addrs: HashMap<ServerId, SocketAddr>,
    conns: Connections,
    disconnect_rx: UnboundedReceiver<ServerId>,
    new_conns: UnboundedSender<(ServerId, Framed<TcpStream, C>)>,
    codec: C,
    handshake: H,
    logger: Logger,
}

impl<C, H> TcpWatch<C, H> {
    pub fn new<L: Into<Option<Logger>>>(
        id: ServerId,
        addrs: HashMap<ServerId, SocketAddr>,
        conns: Connections,
        new_conns: UnboundedSender<(ServerId, Framed<TcpStream, C>)>,
        disconnect_rx: UnboundedReceiver<ServerId>,
        codec: C,
        handshake: H,
        logger: L,
    ) -> Self {
        let logger = logger.into().unwrap_or(Logger::root(StdLog.fuse(), o!()));
        Self {
            id,
            addrs,
            conns,
            disconnect_rx,
            new_conns,
            codec,
            handshake,
            logger,
        }
    }
}

impl<C, H> IntoFuture for TcpWatch<C, H>
where
    C: Encoder<Item = PeerMessage, Error = Error>
        + Decoder<Item = PeerMessage, Error = Error>
        + Clone
        + Send
        + 'static,
    H: Handshake<TcpStream, Item = ServerId> + Clone + Send + 'static,
{
    type Item = ();
    type Error = ();
    type Future = Box<Future<Item = Self::Item, Error = Self::Error> + Send>;

    fn into_future(self) -> Self::Future {
        let Self {
            id,
            addrs,
            conns,
            disconnect_rx,
            new_conns,
            handshake,
            codec,
            logger,
        } = self;

        let conns = conns.clone();
        let new_conns = new_conns.clone();
        let handshake = handshake.clone();
        let codec = codec.clone();

        // create a separate channel for internal disconnect signals
        // signal on this channel will mean client connection was failed before getting to
        // protocol handler, so we don't touch conns
        let (internal_tx, internal_rx) = unbounded();
        let future = disconnect_rx
            .map(|id| Either::A(id))
            .select(internal_rx.map(|id| Either::B(id)))
            .for_each(move |dc_id| {
                let mut is_client = true;
                let dc_id = match dc_id {
                    Either::A(id) => {
                        let mut conns = conns.0.lock().unwrap();
                        is_client = conns.remove(&id).unwrap_or(true);
                        id
                    }
                    Either::B(id) => id,
                };

                let addr = addrs.get(&dc_id).unwrap().clone();

                warn!(logger.clone(), "reconnect"; "peer"=>dc_id.to_string(), "remote_addr"=>addr.to_string());
                let client = TcpClient::new(addr, Duration::from_millis(300));

                let conns = conns.clone();
                let new_conns = new_conns.clone();
                let mut client_handshake = handshake.clone();
                client_handshake.set_is_client(true);
                let codec = codec.clone();
                let client_future = client.into_future().and_then(move |stream| {
                    let mut start =
                        RaftStart::new(id, conns, new_conns, stream, codec, client_handshake);
                    start.set_is_client(true);
                    start
                });

                let internal_tx = internal_tx.clone();
                let logger = logger.clone();

                let delay = if is_client && id > dc_id {
                    // we have priority on connect - reconnect immediately
                    Duration::from_millis(0)
                } else {
                    // let higher priority node try connecting first
                    Duration::from_millis(2000)
                };

                let delay = Delay::new(Instant::now() + delay).map_err(|_| ());
                delay.and_then(move |_| {
                    client_future.then(move |res| match res {
                        Ok(_) => Either::A(ok(())),
                        Err(e) => {
                            //if let Error::DuplicateConnection(_) = e {
                            if false {
                                Either::B(Either::A(ok(())))
                            } else {
                                warn!(logger, "client handshake error"; "error"=>e.to_string(), "remote"=>dc_id.to_string());
                                let delay = Delay::new(
                                    Instant::now() + Duration::from_millis(1000),
                                ).map_err(|_| ());
                                Either::B(Either::B(
                                    delay.and_then(move |_| {
                                        internal_tx.send(dc_id).then(|_| Ok(()))
                                    }),
                                ))
                            }
                        }
                    })
                })
            });
        Box::new(future)
    }
}

/// TCP client that is reconnecting forever until success
pub struct TcpClient {
    addr: SocketAddr,
    timeout: Duration,
}

impl TcpClient {
    pub fn new(addr: SocketAddr, timeout: Duration) -> Self {
        Self { addr, timeout }
    }
}

impl IntoFuture for TcpClient {
    type Item = TcpStream;
    type Error = Error;
    type Future = Box<Future<Item = Self::Item, Error = Self::Error> + Send>;

    fn into_future(self) -> Self::Future {
        let Self { addr, timeout } = self;
        let client = loop_fn(0, move |try| {
            // on a first try timeout is 0
            let dur = if try == 0 {
                Duration::from_millis(0)
            } else {
                timeout
            };

            let delay = Delay::new(Instant::now() + dur).then(|_| Ok(()));

            delay.and_then(move |()| {
                TcpStream::connect(&addr).then(move |res| match res {
                    Ok(stream) => ok(Loop::Break(stream)),
                    Err(_) => ok(Loop::Continue(try + 1)),
                })
            })
        });
        Box::new(client)
    }
}

// TODO example (take from tests)

/// TcpServer works all the time raft exists, is responsible for keeping all connections
/// to all nodes alive and in a single unique instance
pub struct TcpServer<C, H> {
    id: ServerId,
    listen: SocketAddr,
    peers: Connections,
    tx: UnboundedSender<(ServerId, Framed<TcpStream, C>)>,
    codec: C,
    handshake: H,
}

impl<C, H> TcpServer<C, H> {
    pub fn new(
        id: ServerId,
        listen: SocketAddr,
        conns: Connections,
        tx: UnboundedSender<(ServerId, Framed<TcpStream, C>)>,
        codec: C,
        handshake: H,
    ) -> Self {
        Self {
            id,
            listen,
            peers: conns,
            tx,
            codec,
            handshake,
        }
    }
}

impl<C, H> IntoFuture for TcpServer<C, H>
where
    C: Encoder<Item = PeerMessage, Error = Error>
        + Decoder<Item = PeerMessage, Error = Error>
        + Clone
        + Send
        + 'static,
    H: Handshake<TcpStream, Item = ServerId> + Clone + Send + 'static,
{
    type Item = ();
    type Error = Error;
    type Future = Box<Future<Item = Self::Item, Error = Self::Error> + Send>;

    fn into_future(self) -> Self::Future {
        let Self {
            id,
            listen,
            peers,
            tx,
            codec,
            handshake,
        } = self;
        let listener = TcpListener::bind(&listen);
        let listener = match listener {
            Ok(i) => i,
            Err(e) => return Box::new(failed(Error::Io(e))),
        };
        let fut = listener
            .incoming()
            .map_err(Error::Io)
            .for_each(move |stream| {
                let mut start = RaftStart::new(
                    id,
                    peers.clone(),
                    tx.clone(),
                    stream,
                    codec.clone(),
                    handshake.clone(),
                );
                start.set_is_client(false);
                start.into_future()
                    // this avoids server exit on connection-level errors
                    // TODO: probably add logger to log RaftStart issues from here
                    .then(|_| Ok(()))
            });
        Box::new(fut)
    }
}
