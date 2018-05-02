use tokio_io::codec::{Decoder, Encoder};
use bytes::BytesMut;

use raft_consensus::ServerId;
use raft_consensus::message::*;
use bytes::{Buf, BufMut, IntoBuf};
use rmp_serde::decode::from_read;
use rmp_serde::encode::write;

use error::Error;

#[derive(Clone, Debug)]
pub struct RaftCodec;

// TODO: generalize codec on any Serialize/Deserialize

impl Decoder for RaftCodec {
    type Item = PeerMessage;
    type Error = Error;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() == 0 {
            return Ok(None);
        }
        let message: PeerMessage =
            from_read(src.take().into_buf().reader()).map_err(Error::Decoding)?;
        Ok(Some(message.into()))
    }
}

impl Encoder for RaftCodec {
    type Item = PeerMessage;
    type Error = Error;
    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        write(&mut dst.writer(), &item).map_err(Error::Encoding)?;
        Ok(())
    }
}
