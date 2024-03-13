use std::time::Duration;

use anyhow::{anyhow, Result};
use futures::{
    channel::{mpsc, oneshot},
    SinkExt,
};
use libp2p::{multiaddr::Protocol, Multiaddr};
use log::debug;
use rand::random;

use crate::{p2p::behaviour::PeerRequest, reqres_proto::PeerRequestMessage};

use super::behaviour::Action;

pub async fn bootstrap_light(
    mut action_sender: mpsc::Sender<Action>,
    full_node: Multiaddr,
    address_local: Multiaddr,
) -> Result<()> {
    if let Some(Protocol::P2p(peer_id)) = full_node.iter().last() {
        let (sender, receiver) = oneshot::channel();
        // dial
        let action = Action::Dial {
            peer_id,
            peer_addr: full_node,
            sender,
        };
        action_sender.send(action).await.unwrap();
        receiver.await.unwrap().unwrap();
        debug!("dial {peer_id} done");

        // bootstrap
        action_sender.send(Action::Bootstrap {}).await.unwrap();
        tokio::time::sleep(Duration::from_secs(3)).await;

        // acknowledge multiaddress
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
        Err(anyhow!("full node address format error {}", full_node))
    }
}
