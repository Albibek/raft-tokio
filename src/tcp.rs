//! Types and futures responsible for handling TCP aspects of protocol
use std::collections::HashMap;
use std::io::Error as IoError;
use std::marker::PhantomData;
use std::net::{SocketAddr, TcpStream as StdTcpStream};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::net::TcpStream;
use tokio::prelude::future::*;
use tokio::prelude::*;
use tokio::reactor::Handle;
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
use net2::TcpBuilder;
use raft::RaftStart;

/// A shared connection pool to ensure client and server-side connections to be mutually exclusive
#[derive(Debug, Clone)]
pub struct Connections(pub(crate) Arc<Mutex<HashMap<ServerId, bool>>>);

impl Default for Connections {
    fn default() -> Self {
        Connections(Arc::new(Mutex::new(HashMap::new())))
    }
}

// TODO: remove box, but when futures stabilize

/// A factory that can produce connections like TCP(i.e. with different socket options) or TLS
pub trait ConnectionMaker<S, F>
where
    S: AsyncRead + AsyncWrite + Send + 'static,
    F: IntoFuture<Item = S, Error = Error, Future = Box<Future<Item = S, Error = Error> + Send>>
        + Send
        + 'static,
{
    fn make_connection(&self, addr: SocketAddr) -> F;
}

/// A watcher for client connections
///
/// This is a high level interface over making a connection - TCP or not, it doesn't matter
/// because lots of things are generic. Such approach gives a possibility to use any transport
/// with Raft without binding it to something speficic. TCP, UDP, QUIC, any future transport
/// protocol is possible.
///
/// The future itself itakes care about connections accounting: watching one that are lost on `disconnect_rx` channel,
/// re-establishing them, performing a specified handshake and sending the ones that passed the handshake over
/// `new_conns` channel
///
/// T is the resulting transport
/// S is the connection itself as tokio sees it
/// C is a hook for setting up the initial connection options before starting connecting, it's future F should create
/// the connection
pub struct TcpWatch<T, H, S, C, F>
where
    T: IntoTransport<S, PeerMessage> + Send + 'static,
    S: AsyncRead + AsyncWrite + Send + 'static,
    C: ConnectionMaker<S, F>,

    F: IntoFuture<Item = S, Error = Error, Future = Box<Future<Item = S, Error = Error> + Send>>
        + Send
        + 'static,
{
    id: ServerId,
    addrs: HashMap<ServerId, SocketAddr>,
    conns: Connections,
    disconnect_rx: UnboundedReceiver<ServerId>,
    new_conns: UnboundedSender<(ServerId, T::Transport)>,
    transport: T,
    handshake: H,
    factory: C,
    logger: Logger,
    _pd: PhantomData<F>,
}

impl<T, H, S, C, F> TcpWatch<T, H, S, C, F>
where
    T: IntoTransport<S, PeerMessage> + Send + 'static,
    S: AsyncRead + AsyncWrite + Send + 'static,
    C: ConnectionMaker<S, F>,
    F: IntoFuture<Item = S, Error = Error, Future = Box<Future<Item = S, Error = Error> + Send>>
        + Send
        + 'static,
{
    pub fn new(
        id: ServerId,
        addrs: HashMap<ServerId, SocketAddr>,
        conns: Connections,
        new_conns: UnboundedSender<(ServerId, T::Transport)>,
        disconnect_rx: UnboundedReceiver<ServerId>,
        transport: T,
        handshake: H,
        factory: C,
        logger: Logger,
    ) -> Self {
        Self {
            id,
            addrs,
            conns,
            disconnect_rx,
            new_conns,
            transport,
            handshake,
            factory,
            logger,
            _pd: PhantomData,
        }
    }
}

impl<T, H, S, C, F> IntoFuture for TcpWatch<T, H, S, C, F>
where
    T: IntoTransport<S, PeerMessage> + Clone + Send + 'static,
    H: Handshake<S, Item = ServerId> + Clone + Send + 'static,
    S: AsyncRead + AsyncWrite + Send + 'static,
    C: ConnectionMaker<S, F> + Send + 'static,
    F: IntoFuture<Item = S, Error = Error, Future = Box<Future<Item = S, Error = Error> + Send>>
        + Send
        + 'static,
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
            factory,
            transport,
            logger,
            ..
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

                //let client = TcpClient::new(*addr, Duration::from_millis(300));

                let conns = conns.clone();
                let new_conns = new_conns.clone();
                let mut client_handshake = handshake.clone();
                client_handshake.set_is_client(true);
                let transport = transport.clone();
                let client_future = factory.make_connection(*addr).into_future().and_then(move |stream| {
                    let mut start =
                        RaftStart::new(id, conns, new_conns, stream, transport, client_handshake);
                    start.set_is_client(true);
                    start
                });

                let internal_tx = internal_tx.clone();
                let logger = logger.clone();

                let delay = if is_client && id > dc_id {
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

pub struct CustomTcpClientMaker<F>
where
    for<'r> F: FnMut(&'r mut StdTcpStream) -> Result<(), IoError> + Clone + Send + 'static,
{
    timeout: Duration,
    callback: F,
}

impl<F> CustomTcpClientMaker<F>
where
    for<'r> F: FnMut(&'r mut StdTcpStream) -> Result<(), IoError> + Clone + Send + 'static,
{
    pub fn new(timeout: Duration, callback: F) -> Self {
        Self { timeout, callback }
    }
}

impl<F> ConnectionMaker<TcpStream, TcpClient<F>> for CustomTcpClientMaker<F>
where
    for<'r> F: FnMut(&'r mut StdTcpStream) -> Result<(), IoError> + Clone + Send + 'static,
{
    fn make_connection(&self, addr: SocketAddr) -> TcpClient<F> {
        TcpClient::new(addr, self.timeout.clone(), self.callback.clone())
    }
}

pub struct TcpClientMaker;

impl TcpClientMaker {
    pub fn new(
        timeout: Duration,
    ) -> CustomTcpClientMaker<impl FnMut(&mut StdTcpStream) -> Result<(), IoError> + Clone> {
        CustomTcpClientMaker::new(timeout, |_| Ok(()))
    }
}

/// TCP client that is reconnecting forever until success
pub struct TcpClient<F>
where
    for<'r> F: FnMut(&'r mut StdTcpStream) -> Result<(), IoError> + Clone + Send + 'static,
{
    addr: SocketAddr,
    timeout: Duration,
    callback: F,
}

impl<F> TcpClient<F>
where
    for<'r> F: FnMut(&'r mut StdTcpStream) -> Result<(), IoError> + Clone + Send + 'static,
{
    pub fn new(addr: SocketAddr, timeout: Duration, callback: F) -> Self {
        Self {
            addr,
            timeout,
            callback,
        }
    }
}

impl<F> IntoFuture for TcpClient<F>
where
    for<'r> F: FnMut(&'r mut StdTcpStream) -> Result<(), IoError> + Clone + Send + 'static,
{
    type Item = TcpStream;
    type Error = Error;
    type Future = Box<Future<Item = Self::Item, Error = Self::Error> + Send>;

    fn into_future(self) -> Self::Future {
        let Self {
            addr,
            timeout,
            callback,
        } = self;
        let client = loop_fn(0, move |try| {
            // on a first try timeout is 0
            let dur = if try == 0 {
                Duration::from_millis(0)
            } else {
                timeout
            };

            let delay = Delay::new(Instant::now() + dur).then(|_| Ok(()));

            let mut callback = callback.clone();
            let mut stream_init = move || -> Result<StdTcpStream, IoError> {
                let stream = match addr {
                    SocketAddr::V4(_) => TcpBuilder::new_v4()?,
                    SocketAddr::V6(_) => net2::TcpBuilder::new_v6()?,
                };

                let mut stream = stream.to_tcp_stream()?;
                callback(&mut stream)?;

                Ok(stream)
            };
            delay.and_then(move |()| match stream_init() {
                Ok(stream) => {
                    let future = TcpStream::connect_std(stream, &addr, &Handle::default()).then(
                        move |res| match res {
                            Ok(stream) => ok(Loop::Break(stream)),
                            Err(_) => ok(Loop::Continue(try + 1)),
                        },
                    );
                    Box::new(future)
                        as Box<
                            Future<Item = Loop<TcpStream, usize>, Error = Self::Error>
                                + Send
                                + 'static,
                        >
                }
                // TODO: log error
                Err(_e) => Box::new(ok(Loop::Continue(try + 1))),
            })
        });
        Box::new(client)
    }
}

// TODO example (take from tests)

/// TcpServer works all the time raft exists, is responsible for keeping all connections
/// to all nodes alive and in a single unique instance
pub struct TcpServer<T, H, S, L>
where
    T: IntoTransport<S, PeerMessage> + Send + 'static,
    S: AsyncRead + AsyncWrite + Send + 'static,
    L: Stream<Item = S, Error = IoError> + Send + 'static,
{
    id: ServerId,
    listener: L,
    peers: Connections,
    tx: UnboundedSender<(ServerId, T::Transport)>,
    transport: T,
    handshake: H,
}

impl<T, H, S, L> TcpServer<T, H, S, L>
where
    T: IntoTransport<S, PeerMessage> + Send + 'static,
    S: AsyncRead + AsyncWrite + Send + 'static,
    L: Stream<Item = S, Error = IoError> + Send + 'static,
{
    pub fn new(
        id: ServerId,
        listener: L,
        conns: Connections,
        tx: UnboundedSender<(ServerId, T::Transport)>,
        transport: T,
        handshake: H,
    ) -> Self {
        Self {
            id,
            listener,
            peers: conns,
            tx,
            transport,
            handshake,
        }
    }
}

impl<T, H, S, L> IntoFuture for TcpServer<T, H, S, L>
where
    T: IntoTransport<S, PeerMessage> + Clone + Send + 'static,
    H: Handshake<S, Item = ServerId> + Clone + Send + 'static,
    S: AsyncRead + AsyncWrite + Send + 'static,
    L: Stream<Item = S, Error = IoError> + Send + 'static,
{
    type Item = ();
    type Error = Error;
    type Future = Box<Future<Item = Self::Item, Error = Self::Error> + Send>;

    fn into_future(self) -> Self::Future {
        let Self {
            id,
            listener,
            peers,
            tx,
            transport,
            handshake,
        } = self;

        let fut = listener
            //.incoming()
            .map_err(Error::Io)
            .for_each(move |stream| {
                let mut start = RaftStart::new(
                    id,
                    peers.clone(),
                    tx.clone(),
                    stream,
                    transport.clone(),
                    handshake.clone(),
                );
                start.set_is_client(false);
                start
                    .into_future()
                    // this avoids server exit on connection-level errors
                    // TODO: probably add logger to log RaftStart issues from here
                    .then(|_| Ok(()))
            });
        Box::new(fut)
    }
}
