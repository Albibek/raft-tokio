//! Error type with all possible errors
use raft_consensus::error::Error as ConsensusError;
use raft_consensus::ServerId;
use rmp_serde::decode::Error as DecodeError;
use rmp_serde::encode::Error as EncodeError;
use std::io;

#[fail(display = "Raft error")]
#[derive(Fail, Debug)]
pub enum Error {
    #[fail(display = "consensus error: {:?}", _0)]
    Consensus(#[cause] ConsensusError),
    #[fail(display = "I/O error: {:?}", _0)]
    Io(#[cause] io::Error),
    #[fail(display = "decoding error")]
    Decoding(#[cause] DecodeError),
    #[fail(display = "encoding error")]
    Encoding(#[cause] EncodeError),

    #[fail(display = "client-side handshake failed")]
    ClientHandshake,
    #[fail(display = "server-side handshake failed")]
    ServerHandshake,

    #[fail(display = "sending connection to protocol handler")]
    SendConnection,
    #[fail(display = "connection with {:?} removed by higher priority connection", _0)]
    DuplicateConnection(ServerId),
    #[fail(display = "third party error: {:?}", _0)]
    Other(Option<String>),
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}
