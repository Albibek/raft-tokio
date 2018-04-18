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
extern crate bytes;
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
extern crate tokio_executor;
extern crate tokio_io;
extern crate tokio_reactor;
extern crate tokio_timer;

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

mod codec;
mod server;
mod client;
mod raft;
use client::RaftClient;
use codec::*;
use server::*;

#[derive(Debug, Clone)]
pub struct Connections(Arc<Mutex<HashMap<ServerId, Option<oneshot::Sender<()>>>>>);

#[cfg(test)]
mod tests {

    extern crate slog_async;
    extern crate slog_term;
    use std::collections::HashMap;
    use std::thread;

    use slog::Drain;

    use tokio;
    use tokio::prelude::*;
    use tokio::prelude::future::*;
    use tokio::util::FutureExt;
    use std::time::{Duration, Instant};
    use tokio::net::TcpStream;
    use std::net::SocketAddr;
    use futures::sync::mpsc::unbounded;
    use raft_consensus::{ClientId, Consensus, ConsensusHandler, ServerId, SharedConsensus};
    use raft_consensus::message::{ClientResponse, ConsensusTimeout, PeerMessage};
    use raft_consensus::persistent_log::mem::MemLog;
    use raft_consensus::state_machine::null::NullStateMachine;
    use tokio::executor::current_thread;
    use raft::RaftPeerProtocol;
    use super::*;

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
        let filter = slog::LevelFilter::new(drain, slog::Level::Debug).fuse();
        let drain = slog_async::Async::new(filter).build().fuse();
        let rlog = slog::Logger::root(drain, o!("program"=>"test"));
        // this lets root logger live as long as it needs
        //let _guard = slog_scope::set_global_logger(rlog.clone());

        for (id, addr) in nodes.clone().into_iter() {
            let nodes = nodes.clone();

            let log = rlog.new(o!("id" => format!("{:?}", id), "remote" => format!("{:?}",addr)));
            let th = thread::Builder::new()
                .name(format!("test-{:?}", id).to_string())
                .spawn(move || {
                    let log = log.clone();
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
                    let raft_log = MemLog::new();
                    let sm = NullStateMachine;
                    //let consensus = Consensus::new(id, peers.clone(), log, sm, chandler).unwrap();
                    //let consensus = SharedConsensus::new(consensus);
                    let conns = Connections(Arc::new(Mutex::new(HashMap::new())));

                    let (tx, rx) = unbounded();
                    let protocol = RaftPeerProtocol::new(rx, id, peers.clone(), raft_log, sm);

                    let server =
                        RaftServer::new(id, selfad, conns.clone(), tx.clone(), log.clone())
                            .into_future();

                    let mut enter = tokio_executor::enter().expect("Enter");
                    let reactor = tokio_reactor::Reactor::new().expect("reactor");

                    let handle = reactor.handle();
                    let timer = tokio_timer::timer::Timer::new(reactor);
                    let thandle = timer.handle();

                    let mut exec = current_thread::CurrentThread::new_with_park(timer);

                    tokio_reactor::with_default(&handle, &mut enter, move |e| {
                        tokio_timer::with_default(&thandle, e, move |e| {
                            exec.spawn(
                                protocol.map_err(move |e| println!("Protocol error: {:?}", e)),
                            );
                            exec.spawn(server.map_err(move |e| println!("SERVER ERROR {:?}", e)));
                            let remotes = nodes
                                .iter()
                                .filter(|&(iid, addr)| if *iid != id { true } else { false })
                                .map(|(_, addr)| *addr)
                                .collect::<Vec<_>>();
                            warn!(log, "{:?} wil conn to {:?}", id, remotes);
                            for addr in remotes {
                                let client = RaftClient::new(
                                    id,
                                    addr,
                                    conns.clone(),
                                    tx.clone(),
                                    log.clone(),
                                );
                                use tokio::timer::Delay;

                                exec.spawn(
                                    Delay::new(Instant::now() + Duration::from_millis(300))
                                        .then(|_| Ok(()))
                                        .and_then(|_| {
                                            client
                                                .into_future()
                                                .map_err(move |e| println!("CLIENT ERROR {:?}", e))
                                        }),
                                );
                            }

                            exec.enter(e)
                                .run_timeout(Duration::from_secs(30))
                                .unwrap_or_else(|e| println!("loop done with err {:?}", e))
                        })
                    });

                    println!("DONE");
                })
                .unwrap();
            threads.push(th);
        }
        for t in threads {
            t.join().unwrap();
        }
    }
}
