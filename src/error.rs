//! Error type with all possible errors
use capnp;
use raft_consensus::error::Error as ConsensusError;
use raft_consensus::ServerId;
use std::io;

#[fail(display = "Raft error")]
#[derive(Fail, Debug)]
pub enum Error {
    #[fail(display = "consensus error: {:?}", _0)]
    Consensus(#[cause] ConsensusError),
    #[fail(display = "I/O error: {:?}", _0)]
    Io(#[cause] io::Error),

    #[fail(display = "capnp error")]
    Capnp(#[cause] capnp::Error),

    #[fail(display = "capnp schema error")]
    CapnpSchema(#[cause] capnp::NotInSchema),

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
