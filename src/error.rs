//! Error type with all possible errors
use std::io;
use raft_consensus::ServerId;
use raft_consensus::error::Error as ConsensusError;
use rmp_serde::decode::Error as DecodeError;
use rmp_serde::encode::Error as EncodeError;

#[fail(display = "Raft error")]
#[derive(Fail, Debug)]
pub enum Error {
    #[fail(display = "Consensus caused an error")]
    Consensus(#[cause] ConsensusError),
    #[fail(display = "I/O error")]
    Io(#[cause] io::Error),
    #[fail(display = "Decoding error")]
    Decoding(#[cause] DecodeError),
    #[fail(display = "Encoding error")]
    Encoding(#[cause] EncodeError),

    #[fail(display = "Handshake failed")]
    Handshake,
    #[fail(display = "Sending connection to protocol handler")]
    SendConnection,
    #[fail(display = "Connection with {:?} was removed because higher priority connection already exist", _0)]
    DuplicateConnection(ServerId),
    #[fail(display = "Third party error: {:?}", _0)]
    Other(Option<String>),
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}
