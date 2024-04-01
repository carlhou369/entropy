use rand::{rngs::OsRng, RngCore};

use std::time::Duration;

use crate::p2p::error::P2PNetworkError;
use futures::{
    channel::{mpsc, oneshot},
    SinkExt,
};
use libp2p::{multiaddr::Protocol, Multiaddr};

use rand::random;

use crate::{p2p::behaviour::PeerRequest, reqres_proto::PeerRequestMessage};

use super::behaviour::Action;

// Bootstrap_peer dials an existing peer in the p2p network and get discoverable by other peers.
pub async fn bootstrap_peer(
    mut action_sender: mpsc::Sender<Action>,
    full_node: Multiaddr,
    address_local: Multiaddr,
) -> Result<(), P2PNetworkError> {
    if let Some(Protocol::P2p(peer_id)) = full_node.iter().last() {
        let (sender, receiver) = oneshot::channel();
        // Dial peer
        let action = Action::Dial {
            peer_id,
            peer_addr: full_node,
            sender,
        };
        action_sender.send(action).await.unwrap();
        receiver.await.unwrap().unwrap();

        // Bootstrap
        action_sender.send(Action::Bootstrap {}).await.unwrap();
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Acknowledge multiaddress
        let (sender, receiver) = oneshot::channel();
        action_sender
            .send(Action::SendRequest {
                peer_id,
                msg: PeerRequest(PeerRequestMessage {
                    id: format!("{}", random::<u16>()),
                    command: "multiaddress".to_string(),
                    data: address_local.clone().to_string().as_bytes().to_vec(),
                }),
                sender,
            })
            .await
            .unwrap();
        receiver.await.unwrap().unwrap();
        Ok(())
    } else {
        Err(P2PNetworkError::MultiAddrFormatError(full_node))
    }
}

/// Generate random bytes and hex it for request id. Response id is the same as it's request.
pub fn random_req_id() -> String {
    let mut rng = OsRng;
    let mut buf = [0u8; 32];
    rng.fill_bytes(&mut buf);
    hex::encode(buf)
}

pub struct Defer<F: FnOnce()> {
    cleanup: Option<F>,
}

impl<F: FnOnce()> Defer<F> {
    pub fn new(f: F) -> Defer<F> {
        Defer { cleanup: Some(f) }
    }
}

impl<F: FnOnce()> Drop for Defer<F> {
    fn drop(&mut self) {
        if let Some(cleanup) = self.cleanup.take() {
            cleanup();
        }
    }
}
