use std::net::SocketAddr;

use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;
use tokio::prelude::future::*;
use tokio_io::codec::Framed;

use futures::sync::{oneshot, mpsc::UnboundedSender};

use raft_consensus::ServerId;
use slog::{Drain, Logger};
use slog_stdlog;

use error::Error;
use codec::*;
use handshake::*;

use Connections;

/// RaftServer works all the time raft exists, is responsible for keeping all connections
/// to all nodes alive and in a single unique instance
pub struct RaftServer {
    id: ServerId,
    listen: SocketAddr,
    peers: Connections,
    tx: UnboundedSender<(ServerId, Framed<TcpStream, RaftCodec>)>,
    log: Logger,
}

impl RaftServer {
    pub fn new<L: Into<Option<Logger>>>(
        id: ServerId,
        listen: SocketAddr,
        conns: Connections,
        tx: UnboundedSender<(ServerId, Framed<TcpStream, RaftCodec>)>,
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
            log: logger,
        }
    }
}

impl IntoFuture for RaftServer {
    type Item = ();
    type Error = Error;
    type Future = Box<Future<Item = Self::Item, Error = Self::Error> + Send>;

    fn into_future(self) -> Self::Future {
        let Self {
            id: selfid,
            listen,
            peers,
            tx,
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
                let fut = ok::<(), ()>(());

                //let remote = stream.peer_addr().unwrap();
                //debug!(log, "client connected"; "addr" => remote.to_string());
                ////println!("[{:?}] client {:?} connected", selfid, remote);
                //let framed = stream.framed(HelloHandshakeCodec(selfid));
                //let peers = peers.clone();

                //let tx = tx.clone(); // TODO clone only after handshake passed
                //let log = log.clone();
                //let elog = log.clone();
                //let log1 = log.clone(); // this moves to first and_then
                //let log2 = log.clone(); // this moves to second and_then
                //let fut = hs_future.and_then(move |(stream, id, peers)| {
                //let mut new = false;
                //{
                //let mut peers = peers.0.lock().unwrap();
                //let remote = peers.entry(id).or_insert_with(|| {
                //new = true;
                //let (tx, rx) = oneshot::channel();
                //Some(tx)
                //});
                //}
                //if !new && selfid < id {
                //debug!(log2, "skipping duplicate connection"; "remote_id" => id.to_string());
                //Either::A(ok(()))
                //} else {
                //let stream = stream.framed(RaftCodec);
                //Either::B(
                //tx.send((id, stream))
                //.map_err(|e| Error::SendConnection)
                //.map(|_| ()), // we don't need sender anymore
                //) // TODO: process errors
                //}
                //});
                fut.then(|_| Ok(())) // this avoids server exit on connection errors
            });
        Box::new(fut)
    }
}
