//! Codecs for encoding/decoding Raft messages
use bytes::BytesMut;
use tokio_io::codec::{Decoder, Encoder};

use bytes::{Buf, BufMut, IntoBuf};
use raft_consensus::message::*;
use rmp_serde::decode::from_read;
use rmp_serde::encode::write;

use error::Error;

/// A MesssagePack codec for raft messages
#[derive(Clone, Debug)]
pub struct RaftCodec;

// TODO: generalize codec on any Serialize/Deserialize

impl Decoder for RaftCodec {
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

impl Encoder for RaftCodec {
    type Item = PeerMessage;
    type Error = Error;
    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        write(&mut dst.writer(), &item).map_err(Error::Encoding)?;
        Ok(())
    }
}
