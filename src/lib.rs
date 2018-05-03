extern crate bytes;
///! This is an implementation of Raft's networking(i.e. non-consensus) part using tokio framework
///! intended to work with raft-consensus crate where the consensus logic is implemented
///!
///! What exactly is implemented:
///! * Timers
///! * Connecting and reconnecting
///! * Decoding messages from network and passing them to consensus
///! * (Obviously) taking messages consensus required to generate and passing them to network
///!
///! # Connection handling.
///! In raft every node is equal to others. So main question here is, in TCP terms, which of nodes
///! should be a client and which of them should be a server. To solve this problem we make the
///! both sides to try to connect and make a last side able to do this win. To work around
///! total symmetry we add a rule: the connection with bigger ID wins
///! For achieving such behaviour we do the following:
///! * we introduce a node-global shared `Connections` structure where all alive connections
///! are accounted
///! * we consider an active connection the one that that passed the handshake
///! * established connection (no matter what side from) is passed to protocol via channel
extern crate failure;
#[macro_use]
extern crate failure_derive;
extern crate futures;
extern crate raft_consensus;
extern crate rand;
extern crate rmp_serde;
extern crate serde;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate slog;
extern crate slog_stdlog;
extern crate tokio;
extern crate tokio_io;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use futures::sync::oneshot;

use raft_consensus::ServerId;

pub mod codec;
pub mod tcp;
pub mod raft;
pub mod error;
pub mod handshake;

#[derive(Debug, Clone)]
pub struct Connections(Arc<Mutex<HashMap<ServerId, Option<oneshot::Sender<()>>>>>);

#[cfg(test)]
mod tests {
    extern crate slog_async;
    extern crate slog_term;
    use std::collections::HashMap;
    use std::thread;
    use std::sync::{Arc, Mutex};

    use {slog, tokio};
    use Connections;
    use slog::Drain;
    use tokio::prelude::*;
    use tokio::prelude::future::*;
    use tokio::util::FutureExt;
    use tokio::timer::Delay;
    use std::time::{Duration, Instant};
    use tokio::net::TcpStream;
    use std::net::SocketAddr;
    use futures::sync::mpsc::unbounded;
    use raft_consensus::{ClientId, Consensus, ConsensusHandler, ServerId, SharedConsensus};
    use raft_consensus::message::{ClientResponse, ConsensusTimeout, PeerMessage};
    use raft_consensus::persistent_log::mem::MemLog;
    use raft_consensus::state_machine::null::NullStateMachine;
    use tokio::runtime::current_thread::Runtime;
    use raft::{RaftPeerProtocol, RaftStart};
    use tcp::*;
    use handshake::HelloHandshake;
    use codec::RaftCodec;
    use error::Error;

    #[test]
    fn temp_test() {
        let mut nodes: HashMap<ServerId, SocketAddr> = HashMap::new();
        nodes.insert(1.into(), "127.0.0.1:9991".parse().unwrap());
        nodes.insert(2.into(), "127.0.0.1:9992".parse().unwrap());
        nodes.insert(3.into(), "127.0.0.1:9993".parse().unwrap());
        //        nodes.insert(4.into(), "127.0.0.1:9994".parse().unwrap());

        let mut threads = Vec::new();
        // Set logging
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let filter = slog::LevelFilter::new(drain, slog::Level::Trace).fuse();
        let drain = slog_async::Async::new(filter).build().fuse();
        let rlog = slog::Logger::root(drain, o!("program"=>"test"));

        for (id, addr) in nodes.clone().into_iter() {
            let nodes = nodes.clone();

            let log = rlog.new(o!("id" => format!("{:?}", id), "remote" => format!("{:?}",addr)));
            let th = thread::Builder::new()
                .name(format!("test-{:?}", id).to_string())
                .spawn(move || {
                    // prepare logger
                    let log = log.clone();

                    // get list of nodes as all except self_id
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

                    // prepare consensus
                    let raft_log = MemLog::new();
                    let sm = NullStateMachine;
                    let conns = Connections(Arc::new(Mutex::new(HashMap::new())));

                    // Prepare peer protocol handler
                    let (protocol, tx) =
                        RaftPeerProtocol::new(id, peers.clone(), raft_log, sm, log.clone());

                    // select protocol handshake type
                    let handshake = HelloHandshake::new(id);

                    // select protocol codec type
                    let codec = RaftCodec;

                    let server = empty::<(), Error>();

                    // Create the runtime
                    let mut runtime = Runtime::new().expect("creating runtime");

                    // Spawn protocol actor
                    runtime.spawn(protocol.map_err(move |e| println!("Protocol error: {:?}", e)));

                    // spawn TCP server
                    let mut srv_handshake = handshake.clone();
                    srv_handshake.set_is_client(false);
                    let server = TcpServer::new(
                        id,
                        selfad,
                        conns.clone(),
                        tx.clone(),
                        codec.clone(),
                        srv_handshake,
                    ).into_future();

                    let elog = log.clone();
                    runtime.spawn(server.map_err(move |e| error!(elog, "SERVER ERROR {:?}", e)));

                    // spawn clients
                    let remotes = nodes
                        .iter()
                        .filter(|&(iid, addr)| if *iid != id { true } else { false })
                        .map(|(_, addr)| *addr)
                        .collect::<Vec<_>>();
                    for addr in remotes {
                        warn!(log, "connecting to {:?}", addr);
                        let conn = TcpClient::new(addr, Duration::from_millis(300));

                        let conns = conns.clone();
                        let tx = tx.clone();
                        let mut client_handshake = handshake.clone();
                        client_handshake.set_is_client(true);
                        let codec = codec.clone();
                        let client_future = conn.into_future().and_then(move |stream| {
                            let mut start =
                                RaftStart::new(id, conns, tx, stream, codec, client_handshake);
                            start.set_is_client(true);
                            start
                        });
                        runtime.spawn(
                            client_future.map_err(move |e| println!("CLIENT ERROR {:?}", e)),
                        );
                    }

                    let test_timeout = Delay::new(Instant::now() + Duration::from_secs(30));
                    runtime.block_on(test_timeout).expect("runtime");
                })
                .unwrap();
            threads.push(th);
        }
        for t in threads {
            t.join().unwrap();
        }
    }
}
