use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    fmt::Debug,
    io::Read,
    sync::Arc,
};

use crate::{
    cid,
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
use tokio::sync::Semaphore;

const PUSH_CHUNK_CMD: &str = "chunk_push";
const GET_CHUNK_CMD: &str = "chunk_get";
const PUSH_CHUNK_ACK_CMD: &str = "chunk_push_ack";
const GET_CHUNK_ACK_CMD: &str = "chunk_get_ack";

const MAX_PENDING_SEND_PER_PEER: usize = 3;
const MAX_PENDING_GET_PER_PEER: usize = 1;

const CHUNK_SEND_TIMEOUT_DURATION: tokio::time::Duration =
    tokio::time::Duration::from_millis(50);

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
    pending_sends: Arc<Mutex<HashMap<PeerId, Arc<Semaphore>>>>,
    pending_gets: Arc<Mutex<HashMap<PeerId, Arc<Semaphore>>>>,
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
            pending_sends: Arc::new(Mutex::new(HashMap::new())),
            pending_gets: Arc::new(Mutex::new(HashMap::new())),
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
            .send(Action::GetPeers { key, sender })
            .await
            .unwrap();
        receiver.await.unwrap()
    }

    pub async fn get_chunk(
        &self,
        peer_id: PeerId,
        chunk_id: CID,
    ) -> Result<(), P2PNetworkError> {
        let (done_sender, done_recv) = oneshot::channel();
        let action_sender = self.action_sender.clone();
        let local_peer_id = self.local_peer_id;

        let mut pending_gets = self.pending_gets.lock().await;
        let sema = match pending_gets.entry(peer_id) {
            Entry::Occupied(o) => {
                let sema = o.into_mut().clone();
                sema
            },
            Entry::Vacant(v) => {
                let sema = Arc::new(Semaphore::new(MAX_PENDING_GET_PER_PEER));
                let sema_clone = sema.clone();
                v.insert(sema);
                sema_clone
            },
        };
        drop(pending_gets);

        let _ = tokio::spawn(async move {
            let permit = sema.acquire().await.unwrap();
            debug!("start handle get chunk to {}", peer_id);
            handle_get_chunk(
                local_peer_id,
                action_sender,
                peer_id,
                chunk_id,
                done_sender,
            )
            .await;
            drop(permit);
            debug!("done handle get chunk to {}", peer_id);
        })
        .await;
        done_recv.await.unwrap()
    }

    // Send chunk data to a peer
    pub async fn send_chunk(
        &self,
        peer_id: PeerId,
        chunk: Vec<u8>,
    ) -> Result<(), P2PNetworkError> {
        let (done_sender, done_recv) = oneshot::channel();
        let control = self.control.clone();
        let action_sender = self.action_sender.clone();

        let mut pending_sends = self.pending_sends.lock().await;
        let sema = match pending_sends.entry(peer_id) {
            Entry::Occupied(o) => {
                let sema = o.into_mut().clone();
                sema
            },
            Entry::Vacant(v) => {
                let sema = Arc::new(Semaphore::new(MAX_PENDING_SEND_PER_PEER));
                let sema_clone = sema.clone();
                v.insert(sema);
                sema_clone
            },
        };
        drop(pending_sends);

        let _ = tokio::spawn(async move {
            let permit = sema.acquire().await.unwrap();
            debug!("start handle send chunk to {}", peer_id);
            handle_send_chunk(
                control,
                action_sender,
                peer_id,
                chunk,
                done_sender,
            )
            .await;
            drop(permit);
            debug!("done handle send chunk to {}", peer_id);
        })
        .await;
        done_recv.await.unwrap()
    }

    pub async fn random_closest_peer(
        &self,
        _key: Vec<u8>,
        cnt: usize,
    ) -> Result<Vec<PeerId>, P2PNetworkError> {
        let connected_peers = self.connected_peers.lock().await;
        // let closest_peers = self
        //     .get_closest_peers(key)
        //     .await?
        //     .into_iter()
        //     .collect::<HashSet<PeerId>>();
        // let intersact = connected_peers
        //     .intersection(&closest_peers)
        //     .map(|x| x.to_owned())
        //     .collect::<Vec<PeerId>>();
        let v = connected_peers
            .iter()
            .map(|x| x.to_owned())
            .collect::<Vec<PeerId>>();
        let mut rng = thread_rng();
        let chosen = v
            .choose_multiple(&mut rng, cnt)
            .map(|x| x.to_owned())
            .collect::<Vec<PeerId>>();
        // if chosen.len() != cnt {
        //     return Err(P2PNetworkError::Other("not enough".to_string()));
        // }
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
                        PeerCommand::PushChunkAck => {
                            info!("PushChunkAck req");
                        },
                        PeerCommand::GetChunkAck => {
                            info!("PushChunkAck req");
                        },
                        PeerCommand::PushChunk => {
                            info!("push chunk req");
                            let mut pendind_chunks =
                                self.pending_chunks.lock().await;

                            let chunk_id =
                                match serde_json::from_slice::<PushChunkReq>(
                                    &request.0.data,
                                ) {
                                    Ok(req) => req.chunk_id,
                                    Err(e) => {
                                        error!("parse error {:?}", e);
                                        continue;
                                    },
                                };
                            info!("chunk id {}", chunk_id.0.clone());
                            if pendind_chunks.contains_key(&chunk_id) {
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
                        },
                        PeerCommand::GetChunk => {
                            info!("GetChunk req");
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
                                Err(e) => {
                                    error!("open file {:?}", e);
                                    continue;
                                },
                            };
                            let mut chunk = Vec::new();
                            if let Err(e) = file.read_to_end(&mut chunk) {
                                error!("read file {:?}", e);
                                continue;
                            }
                            //send
                            let control = self.control.clone();
                            let action_sender = self.action_sender.clone();
                            let (done_sender, done_recv) = oneshot::channel();
                            tokio::spawn(async move {
                                let mut action_sender_dup =
                                    action_sender.clone();
                                handle_send_chunk(
                                    control,
                                    action_sender,
                                    peer_id,
                                    chunk,
                                    done_sender,
                                )
                                .await;
                                if let Err(e) = done_recv.await.unwrap() {
                                    error!(
                                        "handle send chunk {} to {} error {}",
                                        req.chunk_id.0.clone(),
                                        peer_id,
                                        e
                                    );
                                };
                                //todo: handle error
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
                            });
                        },
                        PeerCommand::Other => {
                            info!("Other req");
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
                    debug!("handle ChunkReceived event");
                    let mut pendind_chunks = self.pending_chunks.lock().await;
                    debug!("get pending_chunks lock");
                    match pendind_chunks.remove(&chunk_id) {
                        Some(ChunkState::WaitToReceive((
                            _chunk_id,
                            channel,
                            req_id,
                            ..,
                        ))) => {
                            action_sender_dup
                                .send(Action::SendResponse {
                                    response: PeerResponse(
                                        PeerResponseMessage {
                                            id: req_id,
                                            command: PUSH_CHUNK_ACK_CMD
                                                .to_string(),
                                            data: b"ack".to_vec(),
                                        },
                                    ),
                                    channel,
                                })
                                .await
                                .unwrap();
                        },
                        Some(ChunkState::Saved(_)) => {},
                        None => {},
                    };
                    pendind_chunks
                        .insert(chunk_id.clone(), ChunkState::Saved(chunk_id));
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
                    info!("connection closed peerID {peer_id} connectionID {connection_id}");
                },
                _ => {},
            }
        }
    }
}

async fn handle_get_chunk(
    local_peer_id: PeerId,
    action_sender: mpsc::Sender<Action>,
    peer_id: PeerId,
    chunk_id: CID,
    done_sender: oneshot::Sender<Result<(), P2PNetworkError>>,
) {
    let mut action_sender_dup = action_sender.clone();
    let (sender, receiver) = oneshot::channel();
    let data = match serde_json::to_vec(&GetChunkReq {
        chunk_id: chunk_id.clone(),
        peer_id: local_peer_id.to_bytes(),
    }) {
        Ok(data) => data,
        Err(e) => {
            done_sender.send(Err(e.into())).unwrap();
            return;
        },
    };
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
    match tokio::time::timeout(CHUNK_SEND_TIMEOUT_DURATION, receiver).await {
        Ok(received) => {
            debug!(
                "get chunk {} responsed from {}",
                chunk_id.0.clone(),
                peer_id
            );
            let _ = match received.unwrap() {
                Ok(_) => done_sender.send(Ok(())),
                Err(e) => done_sender.send(Err(e)),
            };
        },
        Err(_) => {
            done_sender
                .send(Err(P2PNetworkError::ChunkTimeout))
                .unwrap();
        },
    };
}

async fn handle_send_chunk(
    control: Control,
    action_sender: mpsc::Sender<Action>,
    peer_id: PeerId,
    chunk: Vec<u8>,
    done_sender: oneshot::Sender<Result<(), P2PNetworkError>>,
) {
    let mut control = control.clone();
    let mut action_sender_dup = action_sender.clone();
    let (sender, receiver) = oneshot::channel();

    let cid = cid(&chunk);
    let data = serde_json::to_vec(&PushChunkReq {
        chunk_id: cid.clone(),
    })
    .unwrap();
    let msg = PeerRequest(PeerRequestMessage {
        id: random_req_id(),
        command: PUSH_CHUNK_CMD.to_string(),
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

    let mut stream = match control
        .open_stream(peer_id.to_owned(), Network::stream_protocol())
        .await
        .map_err(|error| {
            error!("{error}");
            P2PNetworkError::OpenStreamError(format!("{peer_id},{error:?}"))
        }) {
        Ok(stream) => stream,
        Err(e) => {
            done_sender.send(Err(e)).unwrap();
            return;
        },
    };
    debug!("handle send chunk open stream to {}", peer_id);
    if let Err(e) = stream.write_all(&chunk).await {
        error!("write to stream error {}", e);
    };
    stream.close().await.unwrap();
    debug!("handle send chunk close stream to {}", peer_id);
    match tokio::time::timeout(CHUNK_SEND_TIMEOUT_DURATION, receiver).await {
        Ok(received) => {
            debug!("send chunk {} responsed from {}", cid.0.clone(), peer_id);
            let _ = match received.unwrap() {
                Ok(_) => done_sender.send(Ok(())),
                Err(e) => done_sender.send(Err(e)),
            };
        },
        Err(_) => {
            done_sender
                .send(Err(P2PNetworkError::ChunkTimeout))
                .unwrap();
        },
    };
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GetChunkReq {
    chunk_id: CID,
    peer_id: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PushChunkReq {
    chunk_id: CID,
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
