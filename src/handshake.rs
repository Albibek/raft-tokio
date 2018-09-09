//! Some simple initial handshake traits and implementations
use tokio::prelude::future::*;
use tokio::prelude::*;

use raft_consensus::ServerId;

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
    /// Intended to help determining if handshake is working on client or server currently
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

use capnp::message::Reader as CapnpReader;
use capnp::message::DEFAULT_READER_OPTIONS;
use capnp_futures::serialize::OwnedSegments;
use capnp_futures::serialize::{read_message, write_message};
use handshake_capnp::handshake as capnp_handshake;

fn ehlo_from_reader(reader: CapnpReader<OwnedSegments>) -> Result<ServerId, Error> {
    let message = reader
        .get_root::<capnp_handshake::Reader>()
        .map_err(Error::Capnp)?;
    match message.which().map_err(Error::CapnpSchema)? {
        capnp_handshake::Which::Hello(_) => Err(Error::ClientHandshake),
        capnp_handshake::Which::Ehlo(id) => Ok(id.into()),
    }
}

fn helo_from_reader(reader: CapnpReader<OwnedSegments>) -> Result<ServerId, Error> {
    let message = reader
        .get_root::<capnp_handshake::Reader>()
        .map_err(Error::Capnp)?;
    match message.which().map_err(Error::CapnpSchema)? {
        capnp_handshake::Which::Ehlo(_) => Err(Error::ServerHandshake),
        capnp_handshake::Which::Hello(id) => Ok(id.into()),
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

        if is_client {
            let mut builder = ::capnp::message::Builder::new_default();
            {
                let mut message = builder.init_root::<capnp_handshake::Builder>();
                message.set_hello(self_id.into());
            }
            let future = write_message(stream, builder)
                .and_then(move |(stream, _)| read_message(stream, DEFAULT_READER_OPTIONS))
                .map_err(Error::Capnp)
                .and_then(move |(stream, response)| {
                    match response {
                        Some(reader) => match ehlo_from_reader(reader) {
                            Ok(id) => Either::A(ok((id.into(), stream))),
                            Err(e) => Either::B(failed(e)),
                        },
                        None => Either::B(failed(Error::ClientHandshake)),
                    }
                });

            Box::new(future)
        } else {
         let future = read_message(stream, DEFAULT_READER_OPTIONS)
             .map_err(Error::Capnp)
             .and_then( move |(stream, request)| {
                     match request {
                        Some(reader) => match helo_from_reader(reader) {
                            Ok(id) => Either::A(ok((id.into(), stream))),
                            Err(e) => Either::B(failed(e)),
                        },
                        None => Either::B(failed(Error::ServerHandshake)),
                    }
        }).and_then(move |(id, stream)| {
             let mut builder = ::capnp::message::Builder::new_default();
            {
                let mut message = builder.init_root::<capnp_handshake::Builder>();
                message.set_ehlo(self_id.into());
            }
                 write_message(stream, builder).map(move |(stream, _)| {
                     (id, stream)
                 }).map_err(Error::Capnp)

             });
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

/*
/// A MesssagePack encoder for handshaking
pub struct HelloHandshakeCodec;

impl Decoder for HelloHandshakeCodec {
    type Item = HelloHandshakeMessage;
    type Error = Error;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
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
*/
