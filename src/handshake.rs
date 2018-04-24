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
//I: IntoFuture<Item = (ServerId, S), Error = Error, Future = F>,
//F: Future<Item = (ServerId, S), Error = Error>,
pub trait Handshake<S>
where
    S: Stream<Item = PeerMessage, Error = Error> + Sink<SinkItem = PeerMessage, SinkError = Error>,
{
    fn from_stream(self_id: ServerId, stream: S) -> Self;
}

//impl From<HelloHandshakeMessage> for ServerId {
//fn from(hs: Handshake) -> Self {
//1.into()
//}
//}

/// A simple hello-ehlo handshake. ServerId is sent in both directions.
pub struct HelloHandshake<S>
where
    S: Stream<Item = PeerMessage, Error = Error> + Sink<SinkItem = PeerMessage, SinkError = Error>,
{
    self_id: ServerId,
    stream: S,
    direction: bool,
}

impl<S> HelloHandshake<S>
where
    S: Stream<Item = PeerMessage, Error = Error> + Sink<SinkItem = PeerMessage, SinkError = Error>,
{
    pub fn set_direction(&mut self, client_server: bool) {
        self.direction = client_server;
    }
}

impl<S> Handshake<S> for HelloHandshake<S>
where
    S: Stream<Item = PeerMessage, Error = Error> + Sink<SinkItem = PeerMessage, SinkError = Error>,
{
    fn from_stream(self_id: ServerId, stream: S) -> Self {
        Self {
            self_id,
            stream,
            direction: true,
        }
    }
}

impl<S> IntoFuture for HelloHandshake<S>
where
    S: Stream<Item = PeerMessage, Error = Error> + Sink<SinkItem = PeerMessage, SinkError = Error>,
{
    type Item = (ServerId, S);
    type Error = Error;
    type Future = Box<Future<Item = Self::Item, Error = Self::Error>>;

    fn into_future(self) -> Self::Future {
        let Self {
            self_id,
            stream,
            direction,
        } = self;
        let framed = stream.framed(HelloHandshakeCodec(self_id));
        if direction {
            let future = framed.send(HelloHandshakeMessage::Hello(self_id)).and_then(
                move |stream| {
                    stream.into_future().and_then(move |(id, stream)| {
                        if let Some(Handshake::Ehlo(id)) = id {
                            Either::A(ok(id, stream.into_inner()))
                        } else {
                            Either::B(failed(Error::Handshake))
                        }
                    })
                    //.map_err(|(e, _)| {
                    ////     println!("error sending handshake response: {:?}", e);
                    //e
                    //})
                },
            );
            Box::new(future)
        } else {
            let future = framed
                .into_future()
                .map_err(move |(e, _)| {
                    //warn!(elog, "framed err"; "error" => e.to_string());
                    e
                })
                .and_then(move |(maybe_id, stream)| {
                    let id = if let Handshake::Hello(id) = maybe_id.unwrap() {
                        id
                    } else {
                        return Either::A(failed(Error::Handshake));
                    };

                    //debug!(log1, "Handshake success"; "remote_id" => id.to_string());
                    Either::B(
                        stream
                            .send(Handshake::Ehlo(self_id))
                            .map(move |stream| (id, stream.into_inner())),
                    )
                });
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
        let message: Handshake =
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
