use std::io;
use std::net::SocketAddr;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;
use tokio::prelude::future::*;
use tokio_io::codec::{Decoder, Encoder, Framed};
use bytes::BytesMut;

use futures::sync::{oneshot, mpsc::UnboundedSender};

use raft_consensus::{ClientId, Consensus, ConsensusHandler, ServerId, SharedConsensus};
use slog::{Drain, Logger};
use slog_stdlog;

use codec::*;

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
    type Error = io::Error;
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
            Err(e) => return Box::new(failed(e)),
        };
        let fut = listener.incoming().for_each(move |stream| {
            let remote = stream.peer_addr().unwrap();
            debug!(log, "client connected"; "addr" => remote.to_string());
            //println!("[{:?}] client {:?} connected", selfid, remote);
            let framed = stream.framed(HandshakeCodec(selfid));
            let peers = peers.clone();

            let tx = tx.clone(); // TODO clone only after handshake passed
            let fut = framed
                .into_future()
                .map_err(|(e, _)| {
                    println!("framed err: {:?}", e);
                    e
                })
                .and_then(move |(maybe_id, stream)| {
                    println!("[{:?}] server received ID {:?}", selfid, maybe_id);
                    let id = if let Handshake::Hello(id) = maybe_id.unwrap() {
                        id
                    } else {
                        // TODO: return error
                        unimplemented!()
                    };

                    stream
                        .send(Handshake::Hello(selfid))
                        .map(move |stream| (stream.into_inner(), id, peers))
                })
                .and_then(move |(stream, id, peers)| {
                    let mut new = false;
                    {
                        let mut peers = peers.0.lock().unwrap();
                        let remote = peers.entry(id).or_insert_with(|| {
                            new = true;
                            let (tx, rx) = oneshot::channel();
                            Some(tx)
                        });
                    }
                    if !new && selfid < id {
                        println!(
                            "[{:?}] (server) duplicate connection with {:?}, skipping. {:?}",
                            selfid, id, 1
                        );
                        Either::A(ok(()))
                    } else {
                        let stream = stream.framed(RaftCodec);
                        Either::B(
                            tx.send((id, stream))
                                .map_err(|e| {
                                    println!("SEND ERROR: {:?}", e);
                                })
                                .then(|_| Ok(())),
                        ) // TODO: process errors
                    }
                });
            fut.then(|_| Ok(())) // this avoids server exit on connection errors
        });
        Box::new(fut)
    }
}
