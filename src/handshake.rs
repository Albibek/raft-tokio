//! Some simple initial handshake traits and implementations
use bytes::BytesMut;
use tokio::prelude::future::*;
use tokio::prelude::*;
use tokio_io::codec::{Decoder, Encoder};

use bytes::{Buf, BufMut, IntoBuf};
use raft_consensus::ServerId;
use rmp_serde::decode::from_read;
use rmp_serde::encode::write;

use error::Error;

/// This is a trait useful for types that want to prepend stream's main flow
/// with some initial exchange
///
/// S here is supposed to be a connection being worked on. It can be any async read/write.
/// The most useful cases, of course are TCP or UDP connection probably wrapped in TLS or any
/// other wrapping required. Please note that in case of TCP, connection must be established and
/// ready to send packets
pub trait Handshake<S>
where
    S: AsyncRead + AsyncWrite + Send + 'static,
{
    type Item;
    type Future: Future<Item = (Self::Item, S), Error = Error> + Send + 'static;

    /// This is very likely to be implemented in the same way as `into_future`
    /// The difference is that it gets a stream as input and must return the
    /// same stream when resolved
    fn from_stream(self, stream: S) -> Self::Future;

    /// An optional convenience method allowing to implement both side handshake in one type
    /// Intended to helps determining if handshake is working on client or server currently
    fn set_is_client(&mut self, bool) {}
}

/// This traits adds with_handshake method to `Stream` allowing to prepend it
/// with specified handshake
pub trait HandshakeExt<S>
where
    S: AsyncRead + AsyncWrite + Send + 'static,
{
    fn with_handshake<H>(self, handshake: H) -> H::Future
    where
        Self: Sized,
        H: Handshake<S>;
}

impl<S> HandshakeExt<S> for S
where
    S: AsyncRead + AsyncWrite + Send + 'static,
{
    fn with_handshake<H>(self, handshake: H) -> H::Future
    where
        Self: Sized,
        H: Handshake<Self>,
    {
        handshake.from_stream(self)
    }
}

//TODO add list of allowed serverid-s
/// A simple hello-ehlo handshake. Only ServerId of each side is sent in both directions.
#[derive(Clone, Debug)]
pub struct HelloHandshake {
    self_id: ServerId,
    is_client: bool,
}

impl HelloHandshake {
    /// Set a direction of handshake to select client or server-side

    pub fn new(self_id: ServerId) -> Self {
        Self {
            self_id,
            is_client: true,
        }
    }
}

impl<S> Handshake<S> for HelloHandshake
where
    S: AsyncRead + AsyncWrite + Send + 'static,
{
    type Item = ServerId;
    type Future = Box<Future<Item = (Self::Item, S), Error = Error> + Send>;
    fn from_stream(self, stream: S) -> Self::Future {
        let Self { self_id, is_client } = self;
        let framed = stream.framed(HelloHandshakeCodec);
        if is_client {
            let future = framed.send(HelloHandshakeMessage::Hello(self_id)).and_then(
                move |stream| {
                    stream
                        .into_future()
                        .map_err(|(e, _)| e) // second error is send error, we don't need it
                        .and_then(move |(message, stream)| {
                            if let Some(HelloHandshakeMessage::Ehlo(id)) = message {
                                Either::A(ok((id, stream.into_inner())))
                            } else {
                                Either::B(failed(Error::ClientHandshake))
                            }
                        })
                },
            );
            Box::new(future)
        } else {
            let future = framed.into_future().map_err(move |(e, _)| e).and_then(
                move |(maybe_id, stream)| {
                    let id = if let HelloHandshakeMessage::Hello(id) = maybe_id.unwrap() {
                        id
                    } else {
                        return Either::A(failed(Error::ServerHandshake));
                    };

                    Either::B(
                        stream
                            .send(HelloHandshakeMessage::Ehlo(self_id))
                            .map(move |stream| (id, stream.into_inner())),
                    )
                },
            );
            Box::new(future)
        }
    }

    fn set_is_client(&mut self, is_client: bool) {
        self.is_client = is_client;
    }
}

/// A message for HelloHandshake protocol
#[derive(Debug, Serialize, Deserialize)]
pub enum HelloHandshakeMessage {
    Hello(ServerId),
    Ehlo(ServerId),
}

/// A MesssagePack encoder for handshaking
pub struct HelloHandshakeCodec;

impl Decoder for HelloHandshakeCodec {
    type Item = HelloHandshakeMessage;
    type Error = Error;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() == 0 {
            return Ok(None);
        }
        let message: HelloHandshakeMessage =
            from_read(src.take().into_buf().reader()).map_err(Error::Decoding)?;
        Ok(Some(message))
    }
}

impl Encoder for HelloHandshakeCodec {
    type Item = HelloHandshakeMessage;
    type Error = Error;
    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        write(&mut dst.writer(), &item).map_err(Error::Encoding)?;
        Ok(())
    }
}
