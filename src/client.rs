use std::io;
use std::net::SocketAddr;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;
use tokio::prelude::future::*;
use tokio_io::codec::{Decoder, Encoder, Framed};
use bytes::BytesMut;
use Connections;
use codec::{Handshake, HandshakeCodec, RaftCodec};

use slog::{Drain, Logger};
use slog_stdlog;
use futures::sync::{oneshot, mpsc::UnboundedSender};

use raft_consensus::{ClientId, Consensus, ConsensusHandler, ServerId, SharedConsensus};

pub struct RaftClient {
    id: ServerId,
    remote: SocketAddr,
    peers: Connections,
    tx: UnboundedSender<(ServerId, Framed<TcpStream, RaftCodec>)>,
    log: Logger,
}

impl RaftClient {
    pub fn new<L: Into<Option<Logger>>>(
        id: ServerId,
        remote: SocketAddr,
        conns: Connections,
        tx: UnboundedSender<(ServerId, Framed<TcpStream, RaftCodec>)>,
        logger: L,
    ) -> Self {
        let logger = logger
            .into()
            .unwrap_or(Logger::root(slog_stdlog::StdLog.fuse(), o!()));

        Self {
            id,
            remote,
            peers: conns,
            tx,
            log: logger,
        }
    }
}

impl IntoFuture for RaftClient {
    type Item = ();
    type Error = io::Error;
    type Future = Box<Future<Item = Self::Item, Error = Self::Error> + Send>;

    fn into_future(self) -> Self::Future {
        let Self {
            id: selfid,
            remote,
            peers,
            tx,
            log,
        } = self;
        let client = TcpStream::connect(&remote);
        let fut = client.and_then(move |stream| {
            println!("[{:?}] client connected to {:?}", selfid, remote);
            let framed = stream.framed(HandshakeCodec(selfid));
            framed
                .send(Handshake::Hello(selfid))
                .and_then(move |stream| {
                    stream
                        .into_future()
                        .and_then(move |(maybe_id, stream)| {
                            // FIXME: process hello/ehlo correctly
                            let id = match maybe_id.unwrap() {
                                Handshake::Hello(id) => id,
                                Handshake::Ehlo(id) => id,
                            };
                            println!("[{:?}] handshake received {:?}", selfid, id);
                            Ok((id, stream.into_inner()))
                        })
                        .map_err(|(e, _)| {
                            println!("error sending handshake response: {:?}", e);
                            e
                        })
                        .and_then(move |(id, stream)| {
                            let dpeers =
                                { peers.0.lock().unwrap().keys().cloned().collect::<Vec<_>>() };

                            let mut new = false;
                            {
                                let mut peers = peers.0.lock().unwrap();
                                peers.entry(id).or_insert_with(|| {
                                    new = true;
                                    let (tx, rx) = oneshot::channel();
                                    Some(tx)
                                });
                            }
                            if !new && selfid > id {
                                //println!("neet to do something with old {:?}", remote);
                                println!(
                                "[{:?}] (client) duplicate connection with {:?}, skipping. {:?}",
                                selfid, id, dpeers
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
                        })
                })
        });
        Box::new(fut.then(|_| Ok(())))
    }
}
