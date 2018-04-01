extern crate bytes;
///! # Connection handling.
///! In raft every node is equal to others. So main question here is, in TCP terms, which of nodes
///! should be a client and which of them should be a server. To solve this problem we make the
///! both sides to try to connect and make a last side able to do this win.
///! For achieving such behaviour we do the following:
///! * we introduce a node-global shared `Connections` structure where all alive connections
///! are accounted
///! * we consider an active connection the one that that passed the handshake
///! * we run Raft server that accepts client connections replacing anything in `Connections`
///! * we run Raft client that is responsible for reconnecting and checking if a connection is lost
///! The rest of raft connection logic we hide in the `RaftDialog` which takes `TcpStream` and doesn't
///! care about side connection was made from.
extern crate futures;
extern crate raft_consensus;
extern crate rmp_serde;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate tokio;
extern crate tokio_executor;
extern crate tokio_io;
extern crate tokio_timer;

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

mod codec;
mod server;
use codec::*;
use server::*;

#[derive(Debug, Clone)]
pub struct Connections(Arc<Mutex<HashMap<ServerId, Option<oneshot::Sender<()>>>>>);

pub struct RaftClient {
    id: ServerId,
    remote: SocketAddr,
    peers: Connections,
}

impl RaftClient {
    pub fn new(id: ServerId, remote: SocketAddr, conns: Connections) -> Self {
        Self {
            id,
            remote,
            peers: conns,
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
        } = self;
        let client = TcpStream::connect(&remote);
        let fut = client.and_then(move |stream| {
            println!("client {:?} connected to {:?}", selfid, remote);
            let framed = stream.framed(HandshakeCodec(selfid));
            let fut = framed.send(selfid).and_then(|stream| {
                stream
                    .into_future()
                    .and_then(|(maybe_id, stream)| {
                        let id = maybe_id.unwrap();
                        println!("handshake received {:?}", id);
                        Ok(())
                    })
                    .map_err(|(e, _)| {
                        println!("handshake recv error");
                        e
                    })
            });

            //let mut peers = peers.0.lock().unwrap();
            //let mut new = false;
            //let mut remote = peers.entry(id).or_insert_with(|| {
            //new = true;
            //let (tx, rx) = oneshot::channel();
            //Some(tx)
            //});
            //if !new {
            //println!("neet to do something with old {:?}", remote)
            //}

            //Ok(stream.into_inner())
            //});
            Ok(())
        });
        Box::new(fut)
    }
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;
    use std::thread;

    use tokio;
    use tokio::prelude::*;
    use tokio::prelude::future::*;
    use tokio::util::FutureExt;
    use std::time::{Duration, Instant};
    use tokio::net::TcpStream;
    use std::net::SocketAddr;
    use raft_consensus::{ClientId, Consensus, ConsensusHandler, ServerId, SharedConsensus};
    use raft_consensus::message::{ClientResponse, ConsensusTimeout, PeerMessage};
    use raft_consensus::persistent_log::mem::MemLog;
    use raft_consensus::state_machine::null::NullStateMachine;
    use tokio::executor::current_thread;
    use tokio_timer::Timer;
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
        nodes.insert(3.into(), "127.0.0.1:9993".parse().unwrap());
        nodes.insert(4.into(), "127.0.0.1:9994".parse().unwrap());
        let mut threads = Vec::new();

        for (id, addr) in nodes.clone().into_iter() {
            let nodes = nodes.clone();

            let th = thread::Builder::new()
                .name(format!("test-{:?}", id).to_string())
                .spawn(move || {
                    println!("test-{:?}{:?}", id, addr);
                    let (_, mut selfad) = nodes.clone().into_iter().next().unwrap();
                    let peers = nodes
                        .iter()
                        .filter(|&(iid, addr)| {
                            if *iid != id {
                                true
                            } else {
                                selfad = addr.clone();
                                false
                            }
                        })
                        .map(|(iid, _)| *iid)
                        .collect::<Vec<_>>();
                    let log = MemLog::new();
                    let sm = NullStateMachine;
                    let chandler = TokioHandler;
                    let consensus = Consensus::new(id, peers.clone(), log, sm, chandler).unwrap();
                    let consensus = SharedConsensus::new(consensus);
                    let conns = Connections(Arc::new(Mutex::new(HashMap::new())));

                    let server = RaftServer::new(id, selfad, conns.clone()).into_future();

                    let mut cte = current_thread::CurrentThread::new();
                    cte.spawn(server.map_err(move |e| println!("SERVER ERROR {:?}", e)));
                    let remotes = nodes
                        .iter()
                        .filter(|&(iid, addr)| if *iid != id { true } else { false })
                        .map(|(_, addr)| *addr)
                        .collect::<Vec<_>>();
                    for addr in remotes {
                        let client = RaftClient::new(id, addr, conns.clone());
                        cte.spawn(
                            client
                                .into_future()
                                .map_err(move |e| println!("CLIENT ERROR {:?}", e)),
                        );
                    }
                    cte.run_timeout(Duration::from_secs(10))
                        .unwrap_or_else(|_| {});
                })
                .unwrap();
            threads.push(th);
        }
        for t in threads {
            t.join().unwrap();
        }
    }
}
