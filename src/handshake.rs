use tokio::prelude::*;
use tokio::prelude::future::*;
use tokio_io::codec::{Decoder, Encoder};
use bytes::BytesMut;

use raft_consensus::ServerId;
use raft_consensus::message::*;
use bytes::{Buf, BufMut, IntoBuf};
use rmp_serde::decode::from_read;
use rmp_serde::encode::write;

use error::Error;

pub trait Handshake<S>
where
    S: AsyncRead + AsyncWrite + Send + 'static,
{
    type Future: Future<Item = (ServerId, S), Error = Error> + Send + 'static;
    /// Supposed to return future, should be implemented like into_future
    fn from_stream(self, stream: S) -> Self::Future;
}

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
/// A simple hello-ehlo handshake. ServerId is sent in both directions.
/// S is a connection being worked on. It can be any async read/write.
/// The most useful cases, of course are TCP or UDP connection probably wrapped in TLS or any
/// other wrapping required. Please note that in case of TCP, connection must be established and
/// ready to send packets
#[derive(Clone, Debug)]
pub struct HelloHandshake {
    self_id: ServerId,
    is_client: bool,
}

impl HelloHandshake {
    /// Set a direction of handshake to select client or server-side
    pub fn set_is_client(&mut self, is_client: bool) {
        self.is_client = is_client;
    }

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
    type Future = Box<Future<Item = (ServerId, S), Error = Error> + Send>;
    fn from_stream(self, stream: S) -> Self::Future {
        let Self { self_id, is_client } = self;
        let framed = stream.framed(HelloHandshakeCodec(self_id));
        if is_client {
            let future = framed.send(HelloHandshakeMessage::Hello(self_id)).and_then(
                move |stream| {
                    stream
                        .into_future()
                        .map_err(|(e, _)| e) // second error is send error, we don't need it
                        .and_then(move |(id, stream)| {
                            if let Some(HelloHandshakeMessage::Ehlo(id)) = id {
                                Either::A(ok((id, stream.into_inner())))
                            } else {
                                Either::B(failed(Error::Handshake))
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
                        return Either::A(failed(Error::Handshake));
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
}

#[derive(Debug, Serialize, Deserialize)]
pub enum HelloHandshakeMessage {
    Hello(ServerId),
    Ehlo(ServerId),
}

pub struct HelloHandshakeCodec(pub ServerId);

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
