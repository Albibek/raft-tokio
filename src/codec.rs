//! Codecs for encoding/decoding Raft messages
use futures::{Async, AsyncSink, Poll, Sink, StartSend, Stream};
use tokio_io::{AsyncRead, AsyncWrite};

use capnp::message::DEFAULT_READER_OPTIONS;
use capnp::message::{Builder, HeapAllocator};
use capnp_futures;
use raft_consensus::message::*;

use error::Error;

// TODO: some day this will be in a separate crate with error generalized or event absent
// (since futures >0.1 will remove it)

/// Transport is a stream of frames converted from stream of bytes.
///
/// Unlike tokio-io/tokio-codec it doesn't introduce any buffering and is a bit more general.
/// Tokio's Decoders/Encoders are very simple to be adapted in a few lines of code.
///
/// For simplicity the Stream and Sink are currently joined in a single trait because one sided
/// streams/sinks are very rare as well as ones that use different types for input and output
///
/// At the moment ths trait is still bound to this crate due to  error type. Some day it will end up in
/// a separate crate maybe.
pub trait IntoTransport<S, F>
where
    S: AsyncRead + AsyncWrite + Send + 'static,
{
    type Transport: Stream<Item = F, Error = Error>
        + Sink<SinkItem = F, SinkError = Error>
        + Send
        + 'static;
    fn into_transport(self, stream: S) -> Self::Transport;
}

/*
// TODO: generalize codec on any Serialize/Deserialize
/// A MesssagePack codec for raft messages
#[derive(Clone, Debug)]
pub struct RaftMpackCodec;

impl Decoder for RaftMpackCodec {
    type Item = PeerMessage;
    type Error = Error;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }
        let message: PeerMessage =
            from_read(src.take().into_buf().reader()).map_err(Error::Decoding)?;
        Ok(Some(message))
    }
}

impl Encoder for RaftMpackCodec {
    type Item = PeerMessage;
    type Error = Error;
    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        write(&mut dst.writer(), &item).map_err(Error::Encoding)?;
        Ok(())
    }
}

impl<S: AsyncRead + AsyncWrite + Send + 'static> IntoTransport<S, PeerMessage> for RaftMpackCodec {
    type Transport = Framed<S, RaftMpackCodec>;

    fn into_transport(self, stream: S) -> Self::Transport {
        RaftMpackCodec.framed(stream)
    }
}
*/

#[derive(Clone, Debug)]
pub struct RaftCapnpCodec;

pub struct CapnpTransport<S> {
    inner: capnp_futures::serialize::Transport<S, Builder<HeapAllocator>>,
}

impl<S> CapnpTransport<S> {
    fn new(stream: S) -> Self {
        Self {
            inner: capnp_futures::serialize::Transport::new(stream, DEFAULT_READER_OPTIONS),
        }
    }
}

impl<S> Stream for CapnpTransport<S>
where
    S: ::std::io::Read,
{
    type Item = PeerMessage;
    type Error = Error;
    fn poll(&mut self) -> Poll<Option<Self::Item>, Error> {
        match self.inner.poll() {
            Ok(Async::Ready(Some(segments))) => match PeerMessage::from_capnp_untyped(segments) {
                Ok(message) => Ok(Async::Ready(Some(message))),
                Err(e) => Err(Error::Consensus(e)),
            },
            Ok(Async::Ready(None)) => Ok(Async::Ready(None)),
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(e) => Err(Error::Capnp(e)),
        }
    }
}

impl<S> Sink for CapnpTransport<S>
where
    S: ::std::io::Write,
{
    type SinkItem = PeerMessage;
    type SinkError = Error;
    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        let message = item.as_capnp_heap();
        match self.inner.start_send(message) {
            Ok(AsyncSink::Ready) => Ok(AsyncSink::Ready),
            Ok(AsyncSink::NotReady(_)) => Ok(AsyncSink::NotReady(item)), // TODO: there is reserialization on each retry
            Err(e) => Err(Error::Capnp(e)),
        }
    }
    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        self.inner.poll_complete().map_err(Error::Capnp)
    }
}

impl<S> IntoTransport<S, PeerMessage> for RaftCapnpCodec
where
    S: AsyncRead + AsyncWrite + Send + 'static,
{
    type Transport = CapnpTransport<S>;

    fn into_transport(self, stream: S) -> Self::Transport {
        CapnpTransport::new(stream)
    }
}
