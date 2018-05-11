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
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{Future, IntoFuture, Sink};

use futures::sync::mpsc::unbounded;
use tokio::executor::spawn;
use tokio::net::TcpStream;

use slog::{Drain, Logger};
use slog_stdlog::StdLog;

use raft_consensus::{Log, ServerId, StateMachine};

pub mod codec;
pub mod error;
pub mod handshake;
pub mod raft;
pub mod tcp;

use codec::RaftCodec;
use handshake::{Handshake, HelloHandshake};
use raft::RaftPeerProtocol;
use tcp::{TcpServer, TcpWatch};

#[derive(Debug, Clone)]
pub struct Connections(Arc<Mutex<HashMap<ServerId, bool>>>);

// TODO: tcp retry timeout
/// Starts typical Raft with TCP connection and simple handshake
/// note that `peers` must contain a list of all peers including current node
/// also note that current runtime is used, which means this function
/// has to be executed from inside of a running future
pub fn start_raft_tcp<RL: Log + Send + 'static, RM: StateMachine, L: Into<Option<Logger>>>(
    id: ServerId,
    mut nodes: HashMap<ServerId, SocketAddr>,
    raft_log: RL,
    machine: RM,
    logger: L,
) {
    let logger = logger.into().unwrap_or(Logger::root(StdLog.fuse(), o!()));
    let conns = Connections(Arc::new(Mutex::new(HashMap::new())));

    let listen = nodes.remove(&id).unwrap();
    let node_vec = nodes.keys().cloned().collect();

    // select protocol handshake type
    let handshake = HelloHandshake::new(id);

    // select protocol codec type
    let codec = RaftCodec;

    // Spawn protocol actor
    let elog = logger.clone();

    let (disconnect_tx, disconnect_rx) = unbounded();

    // prepare peer protocol handler
    let (protocol, tx) = RaftPeerProtocol::new(
        id,
        node_vec,
        raft_log,
        machine,
        disconnect_tx.clone(),
        logger.clone(),
    );

    // create watcher
    let watcher = TcpWatch::new(
        id,
        nodes.clone(),
        conns.clone(),
        tx.clone(),
        disconnect_rx,
        codec.clone(),
        handshake.clone(),
        logger.clone(),
    );

    spawn(protocol.map_err(
        move |e| error!(elog, "protocol handler stopped with error"; "error"=>e.to_string()),
    ));

    spawn(watcher.into_future());

    // spawn TCP server
    let mut srv_handshake = handshake.clone();
    Handshake::<TcpStream>::set_is_client(&mut srv_handshake, false);
    let server = TcpServer::new(
        id,
        listen,
        conns.clone(),
        tx.clone(),
        codec.clone(),
        srv_handshake,
    ).into_future();

    let elog = logger.clone();
    spawn(
        server.map_err(move |e| error!(elog, "server exited with error"; "error"=>e.to_string())),
    );

    for (id, _) in nodes.into_iter() {
        // for an initial connection send a disconnect message
        // so watcher could reconnect
        spawn(disconnect_tx.clone().send(id).then(|_| Ok(())));
    }
}

#[cfg(test)]
mod tests {
    extern crate slog_async;
    extern crate slog_term;
    use std::collections::HashMap;
    use std::thread;

    use raft_consensus::ServerId;
    use raft_consensus::persistent_log::mem::MemLog;
    use raft_consensus::state_machine::null::NullStateMachine;
    use slog;
    use slog::Drain;
    use start_raft_tcp;
    use std::net::SocketAddr;
    use std::time::{Duration, Instant};
    use tokio::prelude::future::*;
    use tokio::prelude::*;
    use tokio::runtime::current_thread::Runtime;
    use tokio::timer::Delay;

    #[test]
    fn temp_test() {
        let mut nodes: HashMap<ServerId, SocketAddr> = HashMap::new();
        nodes.insert(1.into(), "127.0.0.1:9991".parse().unwrap());
        nodes.insert(2.into(), "127.0.0.1:9992".parse().unwrap());
        nodes.insert(3.into(), "127.0.0.1:9993".parse().unwrap());
        //nodes.insert(4.into(), "127.0.0.1:9994".parse().unwrap());

        let mut threads = Vec::new();
        // Set logging
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let filter = slog::LevelFilter::new(drain, slog::Level::Trace).fuse();
        let drain = slog_async::Async::new(filter).build().fuse();
        let rlog = slog::Logger::root(drain, o!("program"=>"test"));

        for (id, addr) in nodes.clone().into_iter() {
            let nodes = nodes.clone();

            let log = rlog.new(o!("id" => format!("{:?}", id), "local_addr" => addr.to_string()));
            let th = thread::Builder::new()
                .name(format!("test-{:?}", id).to_string())
                .spawn(move || {
                    // prepare logger
                    let log = log.clone();

                    // prepare consensus
                    let raft_log = MemLog::new();
                    let sm = NullStateMachine;

                    // Create the runtime
                    let mut runtime = Runtime::new().expect("creating runtime");

                    let raft = lazy(|| {
                        if id == ServerId(1) {
                            start_raft_tcp(id, nodes, raft_log, sm, log);
                        } else {
                            start_raft_tcp(id, nodes, raft_log, sm, log);
                        }
                        Ok(())
                    });

                    let test_timeout = Delay::new(Instant::now() + Duration::from_secs(30));
                    runtime
                        .block_on(raft.and_then(|_| test_timeout))
                        .expect("runtime");
                })
                .unwrap();
            threads.push(th);
        }
        for t in threads {
            t.join().unwrap();
        }
    }
}
