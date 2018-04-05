use std::io;
use std::net::SocketAddr;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;
use tokio::prelude::future::*;
use tokio_io::codec::{Decoder, Encoder};
use bytes::BytesMut;

use futures::sync::oneshot;

use raft_consensus::{ClientId, Consensus, ConsensusHandler, ServerId, SharedConsensus};

use codec::*;

use Connections;

/// RaftServer works all the time raft exists, is responsible for keeping all connections
/// to all nodes alive and in a single unique instance
pub struct RaftServer {
    id: ServerId,
    listen: SocketAddr,
    peers: Connections,
}

impl RaftServer {
    pub fn new(id: ServerId, listen: SocketAddr, conns: Connections) -> Self {
        Self {
            id,
            listen,
            peers: conns,
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
        } = self;
        let listener = TcpListener::bind(&listen);
        let listener = match listener {
            Ok(i) => i,
            Err(e) => return Box::new(failed(e)),
        };
        let fut = listener.incoming().for_each(move |stream| {
            let remote = stream.peer_addr().unwrap();
            println!("{:?} connected", remote);
            let framed = stream.framed(HandshakeCodec(selfid));
            let peers = peers.clone();
            let fut = framed
                .into_future()
                .map_err(|(e, _)| {
                    println!("framed err: {:?}", e);
                    e
                })
                .and_then(move |(maybe_id, stream)| {
                    println!("server got ID {:?}", maybe_id);
                    let id = if let Handshake::Hello(id) = maybe_id.unwrap() {
                        id
                    } else {
                        unimplemented!()
                    };

                    let mut peers = peers.0.lock().unwrap();

                    let (tx, rx) = oneshot::channel();
                    let mut remote = peers.insert(id, Some(tx));
                    if let Some(Some(remote)) = remote {
                        if !remote.is_canceled() {
                            println!("canceling");
                            remote.send(()).unwrap();
                        } else {
                            println!("canceled");
                        }
                    }

                    stream
                        .send(Handshake::Hello(selfid))
                        .map(|stream| (stream, rx))
                })
                .and_then(|(stream, rx)| {
                    println!("{:?}", stream.into_inner());
                    println!("{:?}", rx);
                    Ok(())
                });
            fut.then(|_| Ok(())) // this avoids server exit on connection errors
        });
        Box::new(fut)
    }
}
