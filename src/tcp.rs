//! Types and futures responsible for handling TCP aspects of protocol
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::future::*;
use tokio::prelude::*;
use tokio::spawn;
use tokio::timer::Delay;

use futures::sync::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::IntoFuture;

use slog::Logger;

use raft_consensus::message::*;
use raft_consensus::ServerId;

use codec::IntoTransport;
use error::Error;
use handshake::Handshake;
use raft::{ConnectionSolver, RaftStart};
use tokio::reactor::Handle;
use net2::TcpBuilder;

/// A shared connection pool to ensure client- and server-side connections to be mutually exclusive
#[derive(Debug, Clone)]
pub struct Connections(pub(crate) Arc<Mutex<HashMap<ServerId, bool>>>);

impl Default for Connections {
    fn default() -> Self {
        Connections(Arc::new(Mutex::new(HashMap::new())))
    }
}

// TODO: generalize it over stream to support TLS

/// A watcher for client connections
///
/// Accounts connections incoming over `disconnect_rx`, re-establishes them, doing a
/// specified handshake and sends the ones that passed the handshake over new_conns channel
pub struct TcpWatch<T, H, C>
where
    T: IntoTransport<TcpStream, PeerMessage> + Send + 'static,
    C: ConnectionSolver + Send + 'static,
{
    id: ServerId,
    addrs: HashMap<ServerId, SocketAddr>,
    conns: Connections,
    disconnect_rx: UnboundedReceiver<ServerId>,
    new_conns: UnboundedSender<(ServerId, T::Transport)>,
    transport: T,
    handshake: H,
    logger: Logger,
    solver: C,
    bind_addr: SocketAddr,
}

impl<T, H, C> TcpWatch<T, H, C>
where
    T: IntoTransport<TcpStream, PeerMessage> + Send + 'static,
    C: ConnectionSolver + Send + 'static,
{
    pub fn new(
        id: ServerId,
        addrs: HashMap<ServerId, SocketAddr>,
        conns: Connections,
        new_conns: UnboundedSender<(ServerId, T::Transport)>,
        disconnect_rx: UnboundedReceiver<ServerId>,
        transport: T,
        handshake: H,
        logger: Logger,
        solver: C,
        bind_addr: SocketAddr,
    ) -> Self {
        Self {
            id,
            addrs,
            conns,
            disconnect_rx,
            new_conns,
            transport,
            handshake,
            logger,
            solver,
            bind_addr,
        }
    }
}

// TODO change cloneable transport to transport factory
impl<T, H, C> IntoFuture for TcpWatch<T, H, C>
where
    T: IntoTransport<TcpStream, PeerMessage> + Clone + Send + 'static,
    H: Handshake<TcpStream, Item = ServerId> + Clone + Send + 'static,
    C: ConnectionSolver + Clone + Send + 'static,
{
    type Item = ();
    type Error = ();
    type Future = Box<Future<Item = Self::Item, Error = Self::Error> + Send>;

    fn into_future(self) -> Self::Future {
        let Self {
            id,
            addrs,
            conns,
            disconnect_rx,
            new_conns,
            handshake,
            transport,
            logger,
            solver,
            bind_addr,
        } = self;

        let conns = conns.clone();
        let new_conns = new_conns.clone();
        let handshake = handshake.clone();
        let transport = transport.clone();

        // create a separate channel for internal disconnect signals
        // signal on this channel will mean client connection was failed before
        // handshake touched conns, in which case we don't have to touch it
        let (internal_tx, internal_rx) = unbounded();

        let future = disconnect_rx
            .map(Either::A)
            .select(internal_rx.map(Either::B))
            .for_each(move |dc_id| {
                let mut is_client = true;
                let (dc_id, addr) = match dc_id {
                    Either::A(id) => {
                        let addr = &addrs[&id];
                        info!(logger.clone(), "reconnecting"; "peer"=>id.to_string(), "remote_addr"=>addr.to_string());
                        let mut conns = conns.0.lock().unwrap();
                        is_client = conns.remove(&id).unwrap_or(true);
                        (id, addr)
                    }
                    Either::B(id) => (id, &addrs[&id]),
                };
                let client = TcpClient::new(*addr, Duration::from_millis(300), bind_addr);

                let conns = conns.clone();
                let new_conns = new_conns.clone();
                let mut client_handshake = handshake.clone();
                client_handshake.set_is_client(true);
                let transport = transport.clone();
                let rsolver = solver.clone();
                let client_future = client.into_future().and_then(move |stream| {
                    let mut start =
                        RaftStart::new(id, conns, new_conns, stream, transport, client_handshake, rsolver);
                    start.set_is_client(true);
                    start
                });

                let internal_tx = internal_tx.clone();
                let logger = logger.clone();

                let delay = if solver.solve(is_client, id, dc_id)  {
                    // we have priority on connect - reconnect immediately
                    Duration::from_millis(0)
                } else {
                    // let higher priority node try connecting first
                    Duration::from_millis(2000)
                };

                let delay = Delay::new(Instant::now() + delay).map_err(|_| ());
                let future = delay.and_then(move |_| {
                    client_future.then(move |res| match res {
                        Ok(_) => Either::A(ok(())),
                        Err(e) => {
                            if let Error::DuplicateConnection(_) = e {
                                //Either::B(Either::A(ok(())))
                            } else {
                                warn!(logger, "client handshake error"; "error"=>e.to_string(), "remote"=>dc_id.to_string());
                            };
                            let delay = Delay::new(Instant::now() + Duration::from_millis(1000))
                                .map_err(|_| ());
                            Either::B(
                                delay.and_then(move |_| internal_tx.send(dc_id).then(|_| Ok(()))),
                            )
                        }
                    })
                });
                spawn(future);
                Ok(())
            });
        Box::new(future)
    }
}

/// TCP client that is reconnecting forever until success
pub struct TcpClient {
    addr: SocketAddr,
    timeout: Duration,
    bind_addr: SocketAddr,
}

impl TcpClient {
    pub fn new(addr: SocketAddr, timeout: Duration, bind_addr: SocketAddr) -> Self {
       Self { addr, timeout, bind_addr }
    }
}

impl IntoFuture for TcpClient {
    type Item = TcpStream;
    type Error = Error;
    type Future = Box<Future<Item = Self::Item, Error = Self::Error> + Send>;

    fn into_future(self) -> Self::Future {
        let Self { addr, timeout, bind_addr } = self;
        let client = loop_fn(0, move |try| {
            // on a first try timeout is 0
            let dur = if try == 0 {
                Duration::from_millis(0)
            } else {
                timeout
            };

            let delay = Delay::new(Instant::now() + dur).then(|_| Ok(()));

            delay.and_then(move |()| {
                let tcp = TcpBuilder::new_v4().unwrap();
                match tcp.bind(bind_addr) {
                    Ok(tcp_bind) => {
                        let tcp_stream = tcp_bind.to_tcp_stream().unwrap();
                        TcpStream::connect_std(tcp_stream, &addr, &Handle::default()).then(move |res| match res {
                            Ok(stream) => ok(Loop::Break(stream)),
                            Err(_) => ok(Loop::Continue(try + 1)),
                        })
                    },
                    Err(err) =>
                        panic!("unable bind raft client socket to {}: {:?}", bind_addr.to_string(), err),
                }
            })
        });
        Box::new(client)
    }
}

// TODO example (take from tests)

/// TcpServer works all the time raft exists, is responsible for keeping all connections
/// to all nodes alive and in a single unique instance
pub struct TcpServer<T, H, C>
where
    T: IntoTransport<TcpStream, PeerMessage> + Send + 'static,
    C: ConnectionSolver + Send + 'static,
{
    id: ServerId,
    listen: SocketAddr,
    peers: Connections,
    tx: UnboundedSender<(ServerId, T::Transport)>,
    transport: T,
    handshake: H,
    solver: C,
}

impl<T, H, C> TcpServer<T, H, C>
where
    T: IntoTransport<TcpStream, PeerMessage> + Send + 'static,
    C: ConnectionSolver + Send + 'static,
{
    pub fn new(
        id: ServerId,
        listen: SocketAddr,
        conns: Connections,
        tx: UnboundedSender<(ServerId, T::Transport)>,
        transport: T,
        handshake: H,
        solver: C,
    ) -> Self {
        Self {
            id,
            listen,
            peers: conns,
            tx,
            transport,
            handshake,
            solver,
        }
    }
}

impl<T, H, C> IntoFuture for TcpServer<T, H, C>
where
    T: IntoTransport<TcpStream, PeerMessage> + Clone + Send + 'static,
    H: Handshake<TcpStream, Item = ServerId> + Clone + Send + 'static,
    C: ConnectionSolver + Clone + Send + 'static,
{
    type Item = ();
    type Error = Error;
    type Future = Box<Future<Item = Self::Item, Error = Self::Error> + Send>;

    fn into_future(self) -> Self::Future {
        let Self {
            id,
            listen,
            peers,
            tx,
            transport,
            handshake,
            solver,
        } = self;
        let listener = TcpListener::bind(&listen);
        let listener = match listener {
            Ok(i) => i,
            Err(e) => return Box::new(failed(Error::Io(e))),
        };
        let fut = listener
            .incoming()
            .map_err(Error::Io)
            .for_each(move |stream| {
                let mut start = RaftStart::new(
                    id,
                    peers.clone(),
                    tx.clone(),
                    stream,
                    transport.clone(),
                    handshake.clone(),
                    solver.clone(),
                );
                start.set_is_client(false);
                start.into_future()
                    // this avoids server exit on connection-level errors
                    // TODO: probably add logger to log RaftStart issues from here
                    .then(|_| Ok(()))
            });
        Box::new(fut)
    }
}
