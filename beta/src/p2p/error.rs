use std::io;

use libp2p::{
    kad::GetClosestPeersError, request_response::OutboundFailure,
    swarm::DialError, Multiaddr,
};

use tempfile::PersistError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum P2PNetworkError {
    #[error("kad query error")]
    KadQueryError(#[from] GetClosestPeersError),
    #[error("send request outbound failure")]
    SendRequestFailure(#[from] OutboundFailure),
    #[error("dial error")]
    DialError(#[from] DialError),
    #[error("kk")]
    NewNetworkError(#[from] libp2p::noise::Error),
    #[error("new behaviour error")]
    NewBehaviourError(String),
    #[error("multi addr format error")]
    MultiAddrFormatError(Multiaddr),
    #[error("io error")]
    IOError(#[from] io::Error),
    #[error("persis file error")]
    PersisError(#[from] PersistError),
    #[error("open stream error")]
    OpenStreamError(String),
    #[error("serde json error")]
    SerializeError(#[from] serde_json::Error),
    #[error("other error")]
    Other(String),
}
