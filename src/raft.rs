use std::{fmt, io, fmt::Debug};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use std::collections::HashMap;
use futures::{Async, Future, IntoFuture, Sink, Stream, future::select_all};
use raft_consensus::{ClientId, ConsensusHandler, Log, ServerId, SharedConsensus, StateMachine,
                     handler::CollectHandler,
                     message::{ClientResponse, ConsensusTimeout, PeerMessage}};
use tokio::timer::Delay;
use tokio;

#[derive(Clone)]
struct SharedDelay {
    inner: Arc<Mutex<Delay>>,
}

impl SharedDelay {
    pub fn new(deadline: Instant) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Delay::new(deadline))),
        }
    }

    pub fn reset(&mut self, deadline: Instant) {
        let mut inner = self.inner.lock().unwrap();
        inner.reset(deadline)
    }
}

impl Future for SharedDelay {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        let mut inner = self.inner.lock().unwrap();
        inner.poll().map_err(|_| ())
    }
}

struct RaftPeerProtocol<I, O, L, M>
where
    I: Stream<Item = PeerMessage, Error = io::Error> + Send,
    O: Sink<SinkItem = PeerMessage, SinkError = io::Error>,
    L: Log,
    M: StateMachine,
{
    inner_ins: HashMap<ServerId, I>,
    inner_outs: HashMap<ServerId, O>,
    consensus: SharedConsensus<L, M, CollectHandler>,
}

impl<I, O, L, M> RaftPeerProtocol<I, O, L, M>
where
    I: Stream<Item = PeerMessage, Error = io::Error> + Send,
    O: Sink<SinkItem = PeerMessage, SinkError = io::Error>,
    L: Log,
    M: StateMachine,
{
    pub fn new(
        i: HashMap<ServerId, I>,
        o: HashMap<ServerId, O>,
        c: SharedConsensus<L, M, CollectHandler>,
    ) -> Self {
        return Self {
            inner_ins: i,
            inner_outs: o,
            consensus: c,
        };
    }
}

impl<I, O, L, M> IntoFuture for RaftPeerProtocol<I, O, L, M>
where
    I: Stream<Item = PeerMessage, Error = io::Error> + 'static + Send,
    O: Sink<SinkItem = PeerMessage, SinkError = io::Error>,
    L: Log,
    M: StateMachine,
{
    type Item = ();
    type Error = io::Error;
    type Future = Box<Future<Item = Self::Item, Error = Self::Error> + Send>;

    fn into_future(self) -> Self::Future {
        let Self {
            inner_ins,
            inner_outs,
            consensus,
        } = self;
        let ins = inner_ins
            .into_iter()
            .map(|(_, stream)| {
                //
                stream.into_future()
            })
            .collect::<Vec<_>>();
        let fut =
            select_all(ins).map(|((message, rest), _idx, _ins)| println!("PKT: {:?}", message));
        //inner_in.for_each(|message| consensus.apply_peer_message(message))
        Box::new(fut.then(|_| Ok(())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time;
    use tokio;
    use tokio::prelude::future;

    //#[test]
    fn test_timer() {
        tokio::run(future::lazy(|| {
            let d = SharedDelay::new(time::Instant::now() + time::Duration::from_secs(1));
            let mut dd = d.clone();
            tokio::spawn(d.and_then(move |_| {
                println!("DELAY");
                dd.reset(time::Instant::now() + time::Duration::from_secs(1));
                Ok(())
            }));
            future::empty()
        }));
    }

}

//impl<I, O, L, M> Debug for RaftPeerProtocol<I, O, L, M>
//where
//I: Stream<Item = PeerMessage, Error = io::Error>,
//O: Sink<SinkItem = PeerMessage, SinkError = io::Error>,
//L: Log,
//M: StateMachine,
//{
//fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//write!(f, "protocol")
//}
//}

/*
impl<I, O, L, M> ConsensusHandler for RaftPeerProtocol<I, O, L, M>
where
    I: Stream<Item = PeerMessage, Error = io::Error>,
    O: Sink<SinkItem = PeerMessage, SinkError = io::Error>,
    L: Log,
    M: StateMachine,
{
    fn send_peer_message(&mut self, id: ServerId, message: PeerMessage) {
        let conn = self.inner_outs.get_mut(id).unwrap();
        tokio::spawn(conn.send(message));
    }
    fn send_client_response(&mut self, id: ClientId, message: ClientResponse) {
        unimplemented!()
    }
    fn set_timeout(&mut self, timeout: ConsensusTimeout) {
        unimplemented!()
    }
    fn clear_timeout(&mut self, timeout: ConsensusTimeout) {}
}
  */
