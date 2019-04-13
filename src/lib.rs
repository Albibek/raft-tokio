//! This is an implementation of Raft's networking(i.e. non-consensus) part using tokio framework
//! intended to work with raft-consensus crate where the consensus logic is implemented
//!
//! What exactly this crate can currently do:
//! * timers
//! * connecting and reconnecting nodes to each other
//! * decoding messages from network and passing them to consensus
//! * (obviously) taking messages consensus required to generate and passing them to network
//!
//! The main future - [`RaftPeerProtocol`] is stream and codec independent. Also there are
//! futures helping to deal with TCP connections.
//!
//! # TCP connection handling
//! In raft every node is equal to others. So main question here is, in TCP terms, which of nodes
//! should be a client and which of them should be a server. To solve this problem we make the
//! both sides to try to connect and make a side with bigger ServerId win. This is implemented in
//! `raft::RaftStart` future and is optional to use
//!
//! Acheiving these requirements gives a flexibility about what side the connection is established
//! from, so we could work around some typical firewall limitations(like DMZ) where it is not always possible
//! for connections to be established from one segment to another in one direction, but there is almost no
//! rules denying connecting in reverse direction
// (this is a part about internals not needed in public docs)
// For achieving such behaviour we do the following:
// * we introduce a shared `Connections` structure where all alive connections
// are accounted
// * we consider an active connection the one that that passed the handshake
// * established connection (no matter what side from) is passed to protocol via channel
// * when protocol detects disconnect, it sends the messsage to connection watcher
// * the watcher is responsible to provide a new connection to protocol

extern crate failure;
#[macro_use]
extern crate failure_derive;
extern crate futures;
pub extern crate raft_consensus;
extern crate rand;
extern crate serde;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate slog;
extern crate bytes;
extern crate capnp;
extern crate capnp_futures;
extern crate net2;
extern crate slog_stdlog;
extern crate tokio;
extern crate tokio_codec;
extern crate tokio_io;

use std::collections::HashMap;
use std::net::{SocketAddr, TcpStream as StdTcpStream};
use std::time::Duration;

use futures::{Future, IntoFuture, Sink};

use futures::sync::mpsc::unbounded;
use tokio::executor::spawn;
use tokio::net::{TcpListener, TcpStream};

use slog::{Drain, Logger};
use slog_stdlog::StdLog;

pub use raft_consensus as consensus;
use raft_consensus::{Log, ServerId, StateMachine};

pub mod codec;
pub mod error;
pub mod handshake;
pub mod raft;
pub mod tcp;

pub mod handshake_capnp {
    include!(concat!(env!("OUT_DIR"), "/schema/handshake_capnp.rs"));
}

use codec::RaftCapnpCodec;
use handshake::{Handshake, HelloHandshake};
use raft::RaftPeerProtocol;
pub use raft::{Notifier, RaftOptions};
use tcp::{Connections, TcpServer, TcpWatch};

//pub struct RaftTcpBuilder<RL, RM, L, N, F>
//where
//RL: Log + Send + 'static,
//RM: StateMachine,
//L: Into<Option<Logger>>,
//N: Notifier + Send + 'static,
//for<'r> F: FnMut(&'r mut net::TcpStream) + Clone + Send + 'static,
//{
//id: ServerId,
//nodes: HashMap<ServerId, SocketAddr>,
//node_ids: Vec<ServerId>,
//raft_log: RL,
//machine: RM,
//notifier: N,
//options: RaftOptions,
//logger: L,
//client_hook: F,
//}

//impl<RL, RM, L, N, F> RaftTcpBuilder<RL, RM, L, N, F>
//where
//RL: Log + Send + 'static,
//RM: StateMachine,
//L: Into<Option<Logger>>,
//N: Notifier + Send + 'static,
//for<'r> F: FnMut(&'r mut net::TcpStream) + Clone + Send + 'static,
//{
//pub fn new(id: ServerId) -> Self {
//Self {
//id,
//nodes: Vec:
////
//}
//}

///// Return a ready to run future
//pub fn build() -> Result<RaftTcpService<RL, RM, L, N, F>, Error> {}

///// Add a remote node
//pub fn add_node(&mut self, id: ServerId, node: SocketAddr) {
////
//}

///// Add a hook for setting socket options for new tcp connection
//pub fn set_client_hook(&mut self, hook: F) {
////
//}

//pub fn set_raft_heartbeat_timeout(&mut self, duration: Duration) {
////
//}

//}

//pub struct RaftTcpService<RL, RM, L, N, F>
//where
//RL: Log + Send + 'static,
//RM: StateMachine,
//L: Into<Option<Logger>>,
//N: Notifier + Send + 'static,
//for<'r> F: FnMut(&'r mut StdTcpStream) + Clone + Send + 'static,
//{
//id: ServerId,
//nodes: HashMap<ServerId, SocketAddr>,
//raft_log: RL,
//machine: RM,
//notifier: N,
//options: RaftOptions,
//logger: L,
//conn_hook: F,
//}

//impl IntoFuture for RaftTcpService {}

// TODO: tcp retry timeout
/// Starts typical Raft with TCP connection and simple handshake.
///
/// Note that `peers` must contain a list of all peers including current node.
/// Requires default tokio runtime to be already running, and may panic otherwise.
/// also panics on other errors
pub fn start_raft_tcp<RL, RM, L, N, F>(
    id: ServerId,
    mut nodes: HashMap<ServerId, SocketAddr>,
    raft_log: RL,
    machine: RM,
    notifier: N,
    options: RaftOptions,
    logger: L,
    conn_hook: F,
) where
    RL: Log + Send + 'static,
    RM: StateMachine,
    L: Into<Option<Logger>>,
    N: Notifier + Send + 'static,
    for<'r> F: FnMut(&'r mut StdTcpStream) -> Result<(), std::io::Error> + Clone + Send + 'static,
{
    let logger = logger
        .into()
        .unwrap_or_else(|| Logger::root(StdLog.fuse(), o!()));
    let conns = Connections::default();

    let listen = nodes.remove(&id).expect("node id not found in node list");
    let node_vec = nodes.keys().cloned().collect();

    // select protocol handshake type
    let handshake = HelloHandshake::new(id);

    // select protocol codec type
    let codec = RaftCapnpCodec;

    // Spawn protocol actor
    let elog = logger.clone();

    let (disconnect_tx, disconnect_rx) = unbounded();

    // prepare peer protocol handler
    let (mut protocol, tx) = RaftPeerProtocol::new(
        id,
        node_vec,
        raft_log,
        machine,
        notifier,
        disconnect_tx.clone(),
        options,
    );

    protocol.set_logger(logger.clone());

    let conn_maker = tcp::CustomTcpClientMaker::new(Duration::from_millis(300), conn_hook);
    // create connection watcher
    let watcher = TcpWatch::new(
        id,
        nodes.clone(),
        conns.clone(),
        tx.clone(),
        disconnect_rx,
        codec.clone(),
        handshake.clone(),
        conn_maker,
        logger.clone(),
    );

    spawn(protocol.map_err(
        move |e| error!(elog, "protocol handler stopped with error"; "error"=>e.to_string()),
    ));

    spawn(watcher.into_future());

    // spawn TCP server
    let mut srv_handshake = handshake.clone();
    Handshake::<TcpStream>::set_is_client(&mut srv_handshake, false);
    let listener = TcpListener::bind(&listen).expect("tcp listener couldn't start");

    let server = TcpServer::new(
        id,
        listener.incoming(),
        conns.clone(),
        tx.clone(),
        codec.clone(),
        srv_handshake,
    )
    .into_future();

    let elog = logger.clone();
    spawn(
        server.map_err(move |e| error!(elog, "server exited with error"; "error"=>e.to_string())),
    );

    for (id, _) in nodes {
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

    use raft::RaftOptions;
    use raft_consensus::persistent_log::mem::MemLog;
    use raft_consensus::state_machine::null::NullStateMachine;
    use raft_consensus::ServerId;
    use slog;
    use slog::Drain;
    use start_raft_tcp;
    use std::net::SocketAddr;
    use std::time::{Duration, Instant};
    use tokio::prelude::future::*;
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

        let options = RaftOptions::default();

        for (id, addr) in nodes.clone().into_iter() {
            let nodes = nodes.clone();

            let log = rlog.new(o!("id" => format!("{:?}", id), "local_addr" => addr.to_string()));
            let options = options.clone();
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
                    let notifier = ::raft::DoNotNotify;

                    let raft = lazy(|| {
                        start_raft_tcp(id, nodes, raft_log, sm, notifier, options, log, |_| Ok(()));
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
