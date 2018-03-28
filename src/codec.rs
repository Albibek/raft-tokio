use std::io;

use tokio_io::codec::{Decoder, Encoder};
use bytes::BytesMut;

use raft_consensus::{ClientId, Entry, ServerId};
use raft_consensus::message::*;

pub struct HandshakeCodec(pub ServerId);

impl Decoder for HandshakeCodec {
    type Item = ServerId;
    type Error = io::Error;
    fn decode(&mut self, _src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let message = 1;
        Ok(Some(message.into()))
        //
    }
}

impl Encoder for HandshakeCodec {
    type Item = ServerId;
    type Error = io::Error;
    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        Ok(())
    }
}

pub struct RaftCodec;

impl Decoder for RaftCodec {
    type Item = PeerMessage;
    type Error = io::Error;
    fn decode(&mut self, _src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // This is mocked message. TODO: return the real one
        let message = AppendEntriesRequest {
            term: 5.into(),
            prev_log_index: 3.into(),
            prev_log_term: 2.into(),
            leader_commit: 4.into(),
            entries: vec![
                Entry {
                    term: 9.into(),
                    data: "qwer".to_string().into_bytes(),
                },
            ],
        };
        Ok(Some(message.into()))
        //
    }
}

impl Encoder for RaftCodec {
    type Item = PeerMessage;
    type Error = io::Error;
    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        Ok(())
    }
}
