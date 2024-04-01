use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    io::Read,
    sync::Arc,
};

use crate::{
    p2p::{behaviour::Network, utils::random_req_id},
    reqres_proto::{PeerRequestMessage, PeerResponseMessage},
    CID,
};
use futures::{
    channel::{mpsc, oneshot},
    AsyncWriteExt, SinkExt, StreamExt,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use libp2p::{request_response::ResponseChannel, Multiaddr, PeerId};
use libp2p_stream::Control;
use log::{debug, error, info};

use super::{
    behaviour::{Action, Event, PeerRequest, PeerResponse},
    error::P2PNetworkError,
    utils::bootstrap_peer,
};
use rand::{seq::SliceRandom, thread_rng};

const PUSH_CHUNK_CMD: &str = "chunk_push";
const GET_CHUNK_CMD: &str = "chunk_get";
const PUSH_CHUNK_ACK_CMD: &str = "chunk_push_ack";
const GET_CHUNK_ACK_CMD: &str = "chunk_get_ack";

pub enum PeerCommand {
    PushChunkAck,
    PushChunk,
    GetChunk,
    GetChunkAck,
    Other,
}

impl From<String> for PeerCommand {
    fn from(cmd: String) -> Self {
        match cmd.as_str() {
            PUSH_CHUNK_ACK_CMD => PeerCommand::PushChunkAck,
            PUSH_CHUNK_CMD => PeerCommand::PushChunk,
            GET_CHUNK_CMD => PeerCommand::GetChunk,
            GET_CHUNK_ACK_CMD => PeerCommand::GetChunkAck,
            _ => PeerCommand::Other,
        }
    }
}

#[derive(Debug)]
pub enum ChunkState {
    WaitToReceive(
        (
            CID,
            ResponseChannel<PeerResponse>,
            String,
            Option<oneshot::Sender<()>>,
        ),
    ), //(cid, response channel, request id)
    Saved(CID),
}

/// Client encapsulates the interface for interacting with the Network, handling network events.
#[derive(Clone)]
pub struct Client {
    pub local_multi_address: Multiaddr,
    pub local_peer_id: PeerId,
    pub action_sender: mpsc::Sender<Action>,
    pub event_receiver: Arc<Mutex<mpsc::Receiver<Event>>>,
    pub connected_peers: Arc<Mutex<HashSet<PeerId>>>,
    pub pending_chunks: Arc<Mutex<HashMap<CID, ChunkState>>>,
    control: Control,
}

impl Client {
    pub fn new(
        local_peer_id: PeerId,
        local_addr: Multiaddr,
        action_sender: mpsc::Sender<Action>,
        event_receiver: Arc<Mutex<mpsc::Receiver<Event>>>,
        control: Control,
    ) -> Self {
        Self {
            local_peer_id,
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

    pub async fn bootstrap_light(
        &self,
        full_node_addr: Multiaddr,
    ) -> Result<(), P2PNetworkError> {
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

    pub async fn get_closest_peers(
        &self,
        key: Vec<u8>,
    ) -> Result<Vec<PeerId>, P2PNetworkError> {
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

    pub async fn get_chunk(
        &self,
        peer_id: PeerId,
        chunk_id: CID,
    ) -> Result<(), P2PNetworkError> {
        let mut action_sender_dup = self.action_sender.clone();
        let (sender, receiver) = oneshot::channel();
        let data = serde_json::to_vec(&GetChunkReq {
            chunk_id,
            peer_id: self.local_peer_id.to_bytes(),
        })?;
        let msg = PeerRequest(PeerRequestMessage {
            id: random_req_id(),
            command: GET_CHUNK_CMD.to_string(),
            data,
        });
        action_sender_dup
            .send(Action::SendRequest {
                peer_id: peer_id.to_owned(),
                msg: msg.clone(),
                sender,
            })
            .await
            .unwrap();
        receiver.await.unwrap()?;
        Ok(())
    }
    // Send chunk data to a peer
    pub async fn send_chunk(
        &self,
        peer_id: PeerId,
        chunk: Vec<u8>,
    ) -> Result<(), P2PNetworkError> {
        let mut control = self.control.clone();
        let mut action_sender_dup = self.action_sender.clone();
        let (sender, receiver) = oneshot::channel();

        let mut hasher = blake3::Hasher::new();
        hasher.update(&chunk);
        let hash = hasher.finalize();
        let msg = PeerRequest(PeerRequestMessage {
            id: random_req_id(),
            command: PUSH_CHUNK_CMD.to_string(),
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

        // let mut stream = control
        //     .open_stream(peer_id.to_owned(), Network::stream_protocol())
        //     .await
        //     .map_err(|error| {
        //         error!("{error}");
        //         P2PNetworkError::OpenStreamError(format!("{peer_id},{error:?}"))
        //     })?;
        // stream.write_all(&chunk).await.unwrap();
        // stream.close().await.unwrap();
        receiver.await.unwrap()?;
        Ok(())
    }

    pub async fn random_closest_peer(
        &self,
        key: Vec<u8>,
        cnt: usize,
    ) -> Result<Vec<PeerId>, P2PNetworkError> {
        let connected_peers = self.connected_peers.lock().await;
        let closest_peers = self
            .get_closest_peers(key)
            .await?
            .into_iter()
            .collect::<HashSet<PeerId>>();
        let intersact = connected_peers
            .intersection(&closest_peers)
            .map(|x| x.to_owned())
            .collect::<Vec<PeerId>>();
        let mut rng = thread_rng();
        let chosen = intersact
            .choose_multiple(&mut rng, cnt)
            .map(|x| x.to_owned())
            .collect::<Vec<PeerId>>();
        if chosen.len() != cnt {
            return Err(P2PNetworkError::Other("not enough".to_string()));
        }
        Ok(chosen)
    }
    pub async fn handle_event(&self) {
        let mut action_sender_dup = self.action_sender.clone();
        let peers_clone = self.connected_peers.clone();
        let mut event_receiver = self.event_receiver.lock().await;
        while let Some(e) = event_receiver.next().await {
            match e {
                Event::InboundRequest { request, channel } => {
                    debug!("handle inboud request");
                    match PeerCommand::from(request.0.command.clone()) {
                        PeerCommand::PushChunkAck => {},
                        PeerCommand::GetChunkAck => {},
                        PeerCommand::PushChunk => {
                            let mut pendind_chunks =
                                self.pending_chunks.lock().await;

                            let mut chunk_id = CID::default();
                            match String::from_utf8(request.0.data.clone()) {
                                Ok(cid_str) => chunk_id = CID(cid_str),
                                Err(_) => {
                                    continue;
                                },
                            }
                            if let Some(ChunkState::Saved(_chunk_id)) =
                                pendind_chunks.remove(&chunk_id)
                            {
                                action_sender_dup
                                    .send(Action::SendResponse {
                                        response: PeerResponse(
                                            PeerResponseMessage {
                                                id: request.0.id,
                                                command: PUSH_CHUNK_ACK_CMD
                                                    .to_string(),
                                                data: b"ack".to_vec(),
                                            },
                                        ),
                                        channel,
                                    })
                                    .await
                                    .unwrap();
                            } else {
                                if !pendind_chunks.contains_key(&chunk_id) {
                                    pendind_chunks.insert(
                                        chunk_id.clone(),
                                        ChunkState::WaitToReceive((
                                            chunk_id,
                                            channel,
                                            request.0.id,
                                            None,
                                        )),
                                    );
                                }
                            }
                        },
                        PeerCommand::GetChunk => {
                            //parse req
                            let req = match serde_json::from_slice::<GetChunkReq>(
                                &request.0.data,
                            ) {
                                Ok(req) => req,
                                Err(_) => {
                                    continue;
                                },
                            };

                            let peer_id = match PeerId::from_bytes(&req.peer_id)
                            {
                                Ok(id) => id,
                                Err(_) => {
                                    continue;
                                },
                            };
                            info!(
                                "get chunk {} from {}",
                                req.chunk_id.0.clone(),
                                peer_id.clone().to_string()
                            );
                            //get chunk
                            let mut file = match std::fs::File::open(
                                req.chunk_id.0.as_str(),
                            ) {
                                Ok(file) => file,
                                Err(_) => {
                                    continue;
                                },
                            };
                            let mut chunk = Vec::new();
                            if let Err(_) = file.read_to_end(&mut chunk) {
                                continue;
                            }
                            // respond first
                            // action_sender_dup
                            //     .send(Action::SendResponse {
                            //         response: PeerResponse(
                            //             PeerResponseMessage {
                            //                 id: request.0.id,
                            //                 command: GET_CHUNK_ACK_CMD
                            //                     .to_string(),
                            //                 data: b"ack".to_vec(),
                            //             },
                            //         ),
                            //         channel,
                            //     })
                            //     .await
                            //     .unwrap();
                            //send
                            if let Err(e) =
                                self.send_chunk(peer_id, chunk).await
                            {
                                error!("send chunk error {e}");
                            } else {
                                action_sender_dup
                                    .send(Action::SendResponse {
                                        response: PeerResponse(
                                            PeerResponseMessage {
                                                id: request.0.id,
                                                command: GET_CHUNK_ACK_CMD
                                                    .to_string(),
                                                data: b"ack".to_vec(),
                                            },
                                        ),
                                        channel,
                                    })
                                    .await
                                    .unwrap();
                            };
                        },
                        PeerCommand::Other => {
                            action_sender_dup
                                .send(Action::SendResponse {
                                    response: PeerResponse(
                                        PeerResponseMessage {
                                            id: request.0.id,
                                            command: request.0.command,
                                            data: b"ack".to_vec(),
                                        },
                                    ),
                                    channel,
                                })
                                .await
                                .unwrap();
                        },
                    }
                },
                Event::ChunkReceived {
                    peer_id: _,
                    chunk_id,
                } => {
                    let mut pendind_chunks = self.pending_chunks.lock().await;
                    if let Some(ChunkState::WaitToReceive((
                        _chunk_id,
                        channel,
                        req_id,
                        ..,
                    ))) = pendind_chunks.remove(&chunk_id)
                    {
                        action_sender_dup
                            .send(Action::SendResponse {
                                response: PeerResponse(PeerResponseMessage {
                                    id: req_id,
                                    command: PUSH_CHUNK_ACK_CMD.to_string(),
                                    data: b"ack".to_vec(),
                                }),
                                channel,
                            })
                            .await
                            .unwrap();
                    } else {
                        pendind_chunks.insert(
                            chunk_id.clone(),
                            ChunkState::Saved(chunk_id),
                        );
                    }
                },
                Event::IncomeConnection {
                    peer_id,
                    connection_id,
                } => {
                    let mut peers = peers_clone.lock().await;
                    peers.insert(peer_id);
                    info!("inbound connected to peerID {peer_id}, connectionID {connection_id}");
                },
                Event::ConnectionClosed {
                    peer_id,
                    connection_id,
                } => {
                    let mut peers = peers_clone.lock().await;
                    peers.remove(&peer_id);
                    info!("connection closed peerID {peer_id} connectionID {connection_id}");
                },
                _ => {},
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GetChunkReq {
    chunk_id: CID,
    peer_id: Vec<u8>,
}

impl Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("local_multi_address", &self.local_multi_address)
            .field("action_sender", &self.action_sender)
            .field("event_receiver", &self.event_receiver)
            .field("connected_peers", &self.connected_peers)
            .field("pending_chunks", &self.pending_chunks)
            .finish()
    }
}
