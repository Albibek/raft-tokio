use std::net::SocketAddr;
use std::time::{Duration, Instant};

use tokio::net::TcpStream;
use tokio::prelude::*;
use tokio::prelude::future::*;
use tokio::timer::Delay;
use tokio_io::codec::Framed;
use Connections;
use codec::RaftCodec;
use handshake::*;
use tokio_io::codec::{Decoder, Encoder};

use raft_consensus::message::*;
use slog::{Drain, Logger};
use slog_stdlog;
use futures::sync::{oneshot, mpsc::UnboundedSender};
use error::Error;

use raft_consensus::ServerId;

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
                    Err(e) => {
                        //            info!(clog, "connection failed"; "error" => e.to_string());
                        ok(Loop::Continue(try + 1))
                    }
                })
            })
        });
        Box::new(client)
    }
}

/// This future will handle all the actions required for raft to start
/// S is a stream being passed. It can be TCP or UDP connection probably wrapped in TLS or any
/// other wrapping required. Please note that connection must be established and ready to send
/// packets
/// R parameter is responsible for raft packets encoding
pub struct RaftClient<S, R>
where
    S: AsyncRead + AsyncWrite,
    R: Encoder<Item = PeerMessage, Error = Error> + Decoder<Item = PeerMessage, Error = Error>,
{
    self_id: ServerId,
    remote: SocketAddr,
    peers: Connections,
    tx: UnboundedSender<(ServerId, Framed<S, R>)>,
    codec: R,
    // log: Logger,
}

impl<S, R> RaftClient<S, R>
where
    S: AsyncRead + AsyncWrite,
    R: Encoder<Item = PeerMessage, Error = Error> + Decoder<Item = PeerMessage, Error = Error>,
{
    //pub fn new<L: Into<Option<Logger>>>(
    pub fn new(
        self_id: ServerId,
        remote: SocketAddr,
        peers: Connections,
        tx: UnboundedSender<(ServerId, Framed<S, R>)>,
        codec: R,
        //       logger: L,
    ) -> Self {
        // let logger = logger
        //.into()
        //.unwrap_or(Logger::root(slog_stdlog::StdLog.fuse(), o!()));

        Self {
            self_id,
            remote,
            peers,
            tx,
            codec,
            //            log: logger,
        }
    }
}

impl<S, R> IntoFuture for RaftClient<S, R>
where
    S: AsyncRead + AsyncWrite,
    R: Encoder<Item = PeerMessage, Error = Error> + Decoder<Item = PeerMessage, Error = Error>,
{
    type Item = ();
    type Error = Error;
    type Future = Box<Future<Item = Self::Item, Error = Self::Error> + Send>;

    fn into_future(self) -> Self::Future {
        let Self {
            self_id,
            remote,
            peers,
            tx,
            codec,
            //    log,
        } = self;
        //let clog = log.new(o!("client" => self_id.to_string(), "state" => "connecting"));
        //let flog = clog.clone();
        let client = loop_fn(0, move |try| {
            //   let clog = clog.clone();
            let dur = if try == 0 {
                0
            } else {
                // TODO configurable sleep
                300
            };

            let delay = Delay::new(Instant::now() + Duration::from_millis(dur)).then(|_| Ok(()));

            delay.and_then(move |()| {
                TcpStream::connect(&remote).then(move |res| match res {
                    Ok(stream) => ok(Loop::Break(stream)),
                    Err(e) => {
                        //              info!(clog, "connection failed"; "error" => e.to_string());
                        ok(Loop::Continue(try + 1))
                    }
                })
            })
        });
        let fut = client.and_then(move |stream| {
            // info!(flog, "client connected"; "client" => remote.to_string());
            handshake.and_then(move |(id, stream)| {
                let dpeers = { peers.0.lock().unwrap().keys().cloned().collect::<Vec<_>>() };

                let mut new = false;
                {
                    let mut peers = peers.0.lock().unwrap();
                    peers.entry(id).or_insert_with(|| {
                        new = true;
                        let (tx, rx) = oneshot::channel();
                        Some(tx)
                    });
                }
                if !new && self_id > id {
                    //    info!(flog, "duplicate connection"; "remote_id" => id.to_string());
                    Either::A(ok(()))
                } else {
                    let stream = stream.framed(codec);
                    Either::B(
                        tx.send((id, stream))
                            .map_err(|_| Error::SendConnection)
                            .then(|_| Ok(())),
                    ) // TODO: process errors
                }
            })
        });
        Box::new(fut.then(|_| Ok(())))
    }
}
