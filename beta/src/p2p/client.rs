use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use crate::{
    p2p::{behaviour::Network, utils::random_req_id},
    reqres_proto::{PeerRequestMessage, PeerResponseMessage},
};
use futures::{
    channel::{mpsc, oneshot},
    AsyncWriteExt, SinkExt, StreamExt,
};
use tokio::sync::Mutex;

use libp2p::{request_response::ResponseChannel, Multiaddr, PeerId};
use libp2p_stream::Control;
use log::{debug, info};

use super::{
    behaviour::{Action, ChunkID, Event, PeerRequest, PeerResponse},
    error::P2PNetworkError,
    utils::bootstrap_peer,
};

const CHUNK_RECEIVED_ACK: &str = "chunk_ack";
const CHUNK_SEND_CMD: &str = "chunk";

pub enum ChunkState {
    WaitToReceive((ChunkID, ResponseChannel<PeerResponse>, String)),
    Saved(ChunkID),
}

/// Client encapsulates the interface for interacting with the Network, handling network events.
#[derive(Clone)]
pub struct Client {
    pub local_multi_address: Multiaddr,
    pub action_sender: mpsc::Sender<Action>,
    pub event_receiver: Arc<Mutex<mpsc::Receiver<Event>>>,
    pub connected_peers: Arc<Mutex<HashSet<PeerId>>>,
    pub pending_chunks: Arc<Mutex<HashMap<ChunkID, ChunkState>>>,
    control: Control,
}

impl Client {
    pub fn new(
        local_addr: Multiaddr,
        action_sender: mpsc::Sender<Action>,
        event_receiver: Arc<Mutex<mpsc::Receiver<Event>>>,
        control: Control,
    ) -> Self {
        Self {
            local_multi_address: local_addr,
            action_sender,
            event_receiver,
            connected_peers: Arc::new(Mutex::new(HashSet::new())),
            pending_chunks: Arc::new(Mutex::new(HashMap::new())),
            control,
        }
    }

    pub async fn get_connected_peers(&self) -> Vec<PeerId> {
        let peers = self.connected_peers.lock().await;
        let mut res = Vec::new();
        for peer in peers.iter() {
            res.push(peer.to_owned());
        }
        res
    }

    pub async fn bootstrap_light(&self, full_node_addr: Multiaddr) -> Result<(), P2PNetworkError> {
        let action_sender = self.action_sender.clone();
        bootstrap_peer(
            action_sender,
            full_node_addr,
            self.local_multi_address.clone(),
        )
        .await
    }

    pub async fn request(
        &self,
        peer_id: PeerId,
        msg: PeerRequestMessage,
    ) -> Result<PeerResponse, P2PNetworkError> {
        let mut action_sender = self.action_sender.clone();
        let (sender, receiver) = oneshot::channel();
        action_sender
            .send(Action::SendRequest {
                peer_id,
                msg: PeerRequest(msg),
                sender,
            })
            .await
            .unwrap();

        receiver.await.unwrap()
    }

    pub async fn get_closest_peers(&self, key: PeerId) -> Result<Vec<PeerId>, P2PNetworkError> {
        let mut action_sender = self.action_sender.clone();
        let (sender, receiver) = oneshot::channel();
        action_sender
            .send(Action::GetPeers {
                key: key.into(),
                sender,
            })
            .await
            .unwrap();
        receiver.await.unwrap()
    }

    // Send chunk data to a peer
    pub async fn send_chunk(&self, peer_id: PeerId, chunk: Vec<u8>) -> Result<(), P2PNetworkError> {
        let mut control = self.control.clone();
        let mut action_sender_dup = self.action_sender.clone();
        let (sender, receiver) = oneshot::channel();

        let mut hasher = blake3::Hasher::new();
        hasher.update(&chunk);
        let hash = hasher.finalize();
        let msg = PeerRequest(PeerRequestMessage {
            id: random_req_id(),
            command: CHUNK_SEND_CMD.to_string(),
            data: hash.as_bytes().to_vec(),
        });

        action_sender_dup
            .send(Action::SendRequest {
                peer_id: peer_id.to_owned(),
                msg: msg.clone(),
                sender,
            })
            .await
            .unwrap();

        let mut stream = control
            .open_stream(peer_id.to_owned(), Network::stream_protocol())
            .await
            .map_err(|error| P2PNetworkError::OpenStreamError(format!("{peer_id},{error:?}")))?;
        stream.write_all(&chunk).await.unwrap();
        stream.close().await.unwrap();
        receiver.await.unwrap()?;
        Ok(())
    }

    pub async fn handle_event(&self) {
        let mut action_sender_dup = self.action_sender.clone();
        let peers_clone = self.connected_peers.clone();
        let mut event_receiver = self.event_receiver.lock().await;
        while let Some(e) = event_receiver.next().await {
            match e {
                Event::InboundRequest { request, channel } => {
                    debug!("handle inboud request");
                    let chunk_cmd = CHUNK_SEND_CMD.to_string();
                    if request.0.command == chunk_cmd {
                        let mut pendind_chunks = self.pending_chunks.lock().await;
                        let chunk_id = ChunkID(request.0.data.clone());
                        if let Some(ChunkState::Saved(_chunk_id)) = pendind_chunks.remove(&chunk_id)
                        {
                            action_sender_dup
                                .send(Action::SendResponse {
                                    response: PeerResponse(PeerResponseMessage {
                                        id: request.0.id,
                                        command: CHUNK_RECEIVED_ACK.to_string(),
                                        data: b"ack".to_vec(),
                                    }),
                                    channel,
                                })
                                .await
                                .unwrap();
                        } else {
                            pendind_chunks.insert(
                                chunk_id.clone(),
                                ChunkState::WaitToReceive((chunk_id, channel, request.0.id)),
                            );
                        }
                    } else {
                        action_sender_dup
                            .send(Action::SendResponse {
                                response: PeerResponse(PeerResponseMessage {
                                    id: request.0.id,
                                    command: request.0.command,
                                    data: b"ack".to_vec(),
                                }),
                                channel,
                            })
                            .await
                            .unwrap();
                    }
                }
                Event::ChunkReceived {
                    peer_id: _,
                    chunk_id,
                } => {
                    let mut pendind_chunks = self.pending_chunks.lock().await;
                    if let Some(ChunkState::WaitToReceive((_chunk_id, channel, req_id))) =
                        pendind_chunks.remove(&chunk_id)
                    {
                        action_sender_dup
                            .send(Action::SendResponse {
                                response: PeerResponse(PeerResponseMessage {
                                    id: req_id,
                                    command: CHUNK_RECEIVED_ACK.to_string(),
                                    data: b"ack".to_vec(),
                                }),
                                channel,
                            })
                            .await
                            .unwrap();
                    } else {
                        pendind_chunks.insert(chunk_id.clone(), ChunkState::Saved(chunk_id));
                    }
                }
                Event::IncomeConnection {
                    peer_id,
                    connection_id,
                } => {
                    let mut peers = peers_clone.lock().await;
                    peers.insert(peer_id);
                    info!("inbound connected to peerID {peer_id}, connectionID {connection_id}");
                }
                Event::ConnectionClosed {
                    peer_id,
                    connection_id,
                } => {
                    let mut peers = peers_clone.lock().await;
                    peers.remove(&peer_id);
                    info!("connection closed peerID {peer_id} connectionID {connection_id}");
                }
                _ => {}
            }
        }
    }
}
