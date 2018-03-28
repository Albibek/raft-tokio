extern crate bytes;
///! # Connection handling.
///! In raft every node is equal to others. So main question here is, in TCP terms, which of nodes
///! should be a client and which of them should be a server. To solve this problem we make the
///! both sides to try to connect and make a first side able to do this win.
///! For achieving such behaviour we do the following:
///! * we introduce a node-global shared `Connections` structure where all alive connections
///! are accounted
///! * we consider an active connection the one that that passed the handshake first
///! * we run Raft server that accepts client connections placing any in `Connections` or
///! responding with AlreadyConnected message
///! * we run Raft client that is responsible for reconnecting and checking if a connection is lost
///! The rest of raft connection logic we hide in the `RaftDialog` which takes `TcpStream` and doesn't
///! care about side connection was made from.
extern crate futures_timer;
extern crate raft_consensus;
extern crate tokio;
extern crate tokio_io;

use std::io;
use std::net::SocketAddr;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;
use tokio::prelude::future::*;
use tokio_io::codec::{Decoder, Encoder};
use bytes::BytesMut;

use raft_consensus::{ClientId, Consensus, ConsensusHandler, ServerId, SharedConsensus};

mod codec;
use codec::*;

#[derive(Debug, Clone)]
pub struct Connections(Arc<Mutex<HashMap<ServerId, SocketAddr>>>);

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
            let fut = framed.into_future().and_then(|(maybe_id, stream)| {
                let id = maybe_id.unwrap();
                let mut peers = peers.0.lock().unwrap();
                let mut new = false;
                let mut remote = peers.entry(id).or_insert_with(|| {
                    new = true;
                    remote
                });
                if !new {
                    println!("neet to do something with old {:?}", remote)
                }

                Ok(stream.into_inner())
            });
            Ok(())
        });
        Box::new(fut)
    }
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;
    use std::thread;

    use futures_timer::ext::{FutureExt, StreamExt};
    use tokio;
    use tokio::prelude::*;
    use tokio::prelude::future::*;
    use tokio::net::TcpStream;
    use std::time::Duration;
    use std::net::SocketAddr;
    use raft_consensus::{ClientId, Consensus, ConsensusHandler, ServerId, SharedConsensus};
    use raft_consensus::message::{ClientResponse, ConsensusTimeout, PeerMessage};
    use raft_consensus::persistent_log::mem::MemLog;
    use raft_consensus::state_machine::null::NullStateMachine;
    use super::*;

    #[derive(Debug)]
    struct TokioHandler;
    impl ConsensusHandler for TokioHandler {
        fn send_peer_message(&mut self, id: ServerId, message: PeerMessage) {}
        fn send_client_response(&mut self, id: ClientId, message: ClientResponse) {}
        fn set_timeout(&mut self, timeout: ConsensusTimeout) {}
        fn clear_timeout(&mut self, timeout: ConsensusTimeout) {}
    }

    #[test]
    fn temp_test() {
        let mut nodes: HashMap<ServerId, SocketAddr> = HashMap::new();
        nodes.insert(1.into(), "127.0.0.1:9991".parse().unwrap());
        nodes.insert(2.into(), "127.0.0.1:9992".parse().unwrap());
        for (id, addr) in nodes.clone().into_iter() {
            let nodes = nodes.clone();
            thread::spawn(move || {
                let (_, mut selfad) = nodes.clone().into_iter().next().unwrap();
                let peers = nodes
                    .iter()
                    .filter(|&(iid, addr)| {
                        if *iid != id {
                            true
                        } else {
                            selfad = addr.clone();
                            true
                        }
                    })
                    .map(|(iid, _)| *iid)
                    .collect();
                let log = MemLog::new();
                let sm = NullStateMachine;
                let chandler = TokioHandler;
                let consensus = Consensus::new(id, peers, log, sm, chandler).unwrap();
                let consensus = SharedConsensus::new(consensus);
                let conns = Connections(Arc::new(Mutex::new(HashMap::new())));

                let server = RaftServer::new(id, selfad, conns.clone())
                    .into_future()
                    .map_err(move |e| println!("SERVER ERROR {:?}", e));

                tokio::run(server);
            });
        }
    }
}
