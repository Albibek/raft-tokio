use std::net::SocketAddr;
use std::time::{Duration, Instant};

use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;
use tokio::prelude::future::*;
use tokio::timer::Delay;
use Connections;
use codec::RaftCodec;
use handshake::*;
use tokio_io::codec::{Decoder, Encoder, Framed};

use raft_consensus::message::*;
use slog::{Drain, Logger};
use slog_stdlog;
use futures::sync::{oneshot, mpsc::UnboundedSender};
use error::Error;

use raft::RaftStart;
use raft_consensus::ServerId;
use handshake::{Handshake, HandshakeExt};
use std::marker::PhantomData;

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

/// TcpServer works all the time raft exists, is responsible for keeping all connections
/// to all nodes alive and in a single unique instance
pub struct TcpServer<C, H> {
    id: ServerId,
    listen: SocketAddr,
    peers: Connections,
    tx: UnboundedSender<(ServerId, Framed<TcpStream, C>)>,
    log: Logger,
    codec: C,
    handshake: H,
}

impl<C, H> TcpServer<C, H> {
    pub fn new<L: Into<Option<Logger>>>(
        id: ServerId,
        listen: SocketAddr,
        conns: Connections,
        tx: UnboundedSender<(ServerId, Framed<TcpStream, C>)>,
        codec: C,
        handshake: H,
        logger: L,
    ) -> Self {
        let logger = logger
            .into()
            .unwrap_or(Logger::root(slog_stdlog::StdLog.fuse(), o!()));
        let logger = logger.new(o!("id" => id.to_string()));
        Self {
            id,
            listen,
            peers: conns,
            tx,
            codec,
            handshake,
            log: logger,
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
    H: Handshake<TcpStream> + Clone + Send + 'static,
{
    type Item = ();
    type Error = Error;
    type Future = Box<Future<Item = Self::Item, Error = Self::Error> + Send>;

    fn into_future(self) -> Self::Future {
        let Self {
            id: selfid,
            listen,
            peers,
            tx,
            codec,
            handshake,
            log,
        } = self;
        let listener = TcpListener::bind(&listen);
        let listener = match listener {
            Ok(i) => i,
            Err(e) => return Box::new(failed(Error::Io(e))),
        };
        let log = log.new(o!("id" => selfid.to_string()));
        let fut = listener
            .incoming()
            .map_err(Error::Io)
            .for_each(move |stream| {
                let mut start = RaftStart::new(
                    selfid,
                    peers.clone(),
                    tx.clone(),
                    stream,
                    codec.clone(),
                    handshake.clone(),
                );
                start.set_is_client(false);
                start.into_future().then(|_| Ok(())) // this avoids server exit on connection errors
            });
        Box::new(fut)
    }
}
