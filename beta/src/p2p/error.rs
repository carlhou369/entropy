use libp2p::{
    kad::GetClosestPeersError, request_response::OutboundFailure, swarm::DialError, Multiaddr,
};
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
}
