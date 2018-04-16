use std::io::{self, Read};

use tokio_io::codec::{Decoder, Encoder};
use bytes::BytesMut;

use raft_consensus::{ClientId, Entry, ServerId};
use raft_consensus::message::*;
use bytes::{Buf, BufMut, IntoBuf};
use rmp_serde::decode::{from_read, from_slice};
use rmp_serde::encode::write;

#[derive(Debug, Serialize, Deserialize)]
pub enum Handshake {
    Hello(ServerId),
    Ehlo(ServerId),
}

impl From<Handshake> for ServerId {
    fn from(hs: Handshake) -> Self {
        1.into()
    }
}

pub struct HandshakeCodec(pub ServerId);

impl Decoder for HandshakeCodec {
    type Item = Handshake;
    type Error = io::Error;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() == 0 {
            return Ok(None);
        }
        let message: Handshake =
            from_read(src.take().into_buf().reader()).map_err(|e| -> io::Error {
                //    from_slice(&src).map_err(|e| -> io::Error {
                println!("decoder err {:?}", e);
                io::ErrorKind::Other.into()
            })?;
        Ok(Some(message))
    }
}

impl Encoder for HandshakeCodec {
    type Item = Handshake;
    type Error = io::Error;
    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        write(&mut dst.writer(), &item).map_err(|e| -> io::Error {
            println!("encoder err {:?}", e);
            io::ErrorKind::Other.into()
        })?;

        Ok(())
    }
}

pub struct RaftCodec;

impl Decoder for RaftCodec {
    type Item = PeerMessage;
    type Error = io::Error;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() == 0 {
            return Ok(None);
        }
        let message: PeerMessage =
            from_read(src.take().into_buf().reader()).map_err(|e| -> io::Error {
                //    from_slice(&src).map_err(|e| -> io::Error {
                println!("decoder err {:?}", e);
                io::ErrorKind::Other.into()
            })?;
        Ok(Some(message.into()))
    }
}

impl Encoder for RaftCodec {
    type Item = PeerMessage;
    type Error = io::Error;
    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        write(&mut dst.writer(), &item).map_err(|e| -> io::Error {
            println!("encoder err {:?}", e);
            io::ErrorKind::Other.into()
        })?;

        Ok(())
    }
}
