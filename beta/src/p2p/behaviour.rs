use crate::p2p::error::P2PNetworkError;
use futures::channel::{mpsc, oneshot};
use futures::prelude::*;
use futures::StreamExt;

use libp2p::core::ConnectedPoint;
#[cfg(feature = "gossipsub")]
use libp2p::gossipsub;
use libp2p::kad::{Caching, Config, QueryId};
use libp2p::swarm::ConnectionId;
use libp2p::{
    core::Multiaddr,
    identify, identity, kad,
    multiaddr::Protocol,
    noise, ping,
    request_response::{self, OutboundRequestId, ProtocolSupport, ResponseChannel},
    swarm::{NetworkBehaviour, Swarm, SwarmEvent},
    tcp, yamux, PeerId,
};

use crate::reqres_proto::{PeerRequestMessage, PeerResponseMessage};
use libp2p::StreamProtocol;
use serde::{Deserialize, Serialize};
use std::collections::{hash_map, HashMap};

use std::str::FromStr;
use std::time::Duration;

use log::{debug, info, warn};

// Simple file exchange protocol
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerRequest(pub PeerRequestMessage);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerResponse(pub PeerResponseMessage);

#[derive(NetworkBehaviour)]
pub struct Behaviour {
    request_response: request_response::cbor::Behaviour<PeerRequest, PeerResponse>,
    kademlia: kad::Behaviour<kad::store::MemoryStore>,
    #[cfg(feature = "gossipsub")]
    gossipsub: gossipsub::Behaviour,
    identify: identify::Behaviour,
    ping: ping::Behaviour,
}

pub enum Action {
    Dial {
        peer_id: PeerId,
        peer_addr: Multiaddr,
        sender: oneshot::Sender<Result<(), P2PNetworkError>>,
    },
    SendRequest {
        peer_id: PeerId,
        msg: PeerRequest,
        sender: oneshot::Sender<Result<PeerResponse, P2PNetworkError>>,
    },
    GetPeers {
        key: Vec<u8>,
        sender: oneshot::Sender<Result<Vec<PeerId>, P2PNetworkError>>,
    },
    SendResponse {
        response: PeerResponse,
        channel: ResponseChannel<PeerResponse>,
    },
    Bootstrap {},
}

pub enum Event {
    InboundRequest {
        request: PeerRequest,
        channel: ResponseChannel<PeerResponse>,
    },
    IncomeConnection {
        peer_id: PeerId,
        connection_id: ConnectionId,
    },
    ConnectionClosed {
        peer_id: PeerId,
        connection_id: ConnectionId,
    },
    Bootstrap,
}

pub struct Network {
    swarm: Swarm<Behaviour>,
    action_receiver: mpsc::Receiver<Action>,
    event_sender: mpsc::Sender<Event>,
    pending_request:
        HashMap<OutboundRequestId, oneshot::Sender<Result<PeerResponse, P2PNetworkError>>>,
    pending_get_peers: HashMap<QueryId, oneshot::Sender<Result<Vec<PeerId>, P2PNetworkError>>>,
    pending_dial: HashMap<PeerId, oneshot::Sender<Result<(), P2PNetworkError>>>,
}

impl Network {
    pub async fn new(
        secret_key_seed: Option<[u8; 32]>,
        action_receiver: mpsc::Receiver<Action>,
        event_sender: mpsc::Sender<Event>,
    ) -> Result<Self, P2PNetworkError> {
        let id_keys = match secret_key_seed {
            Some(seed) => identity::Keypair::ed25519_from_bytes(seed).unwrap(),
            None => identity::Keypair::generate_ed25519(),
        };
        let peer_id = id_keys.public().to_peer_id();

        let mut swarm = libp2p::SwarmBuilder::with_existing_identity(id_keys)
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
                noise::Config::new,
                yamux::Config::default,
            )?
            .with_behaviour(|key| {
                #[cfg(feature = "gossipsub")]
                let gossipsub_config = gossipsub::ConfigBuilder::default()
                    .max_transmit_size(262144)
                    .build()
                    .unwrap();
                let mut kad_config = Config::default();
                kad_config
                    .set_protocol_names(vec![StreamProtocol::new("/entropy_kad")])
                    .set_caching(Caching::Enabled { max_peers: 10 });
                Ok(Behaviour {
                    kademlia: kad::Behaviour::with_config(
                        peer_id,
                        kad::store::MemoryStore::new(key.public().to_peer_id()),
                        kad_config,
                    ),
                    request_response: request_response::cbor::Behaviour::new(
                        [(StreamProtocol::new("/reqres"), ProtocolSupport::Full)],
                        request_response::Config::default(),
                    ),
                    #[cfg(feature = "gossipsub")]
                    gossipsub: gossipsub::Behaviour::new(
                        gossipsub::MessageAuthenticity::Signed(key.clone()),
                        gossipsub_config,
                    )
                    .expect("Valid configuration"),
                    identify: identify::Behaviour::new(identify::Config::new(
                        "/entropy/0.1.0".into(),
                        key.public(),
                    )),
                    ping: ping::Behaviour::new(ping::Config::new()),
                })
            })
            .map_err(|e| P2PNetworkError::NewBehaviourError(format!("{e}")))?
            .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
            .build();

        swarm
            .behaviour_mut()
            .kademlia
            .set_mode(Some(kad::Mode::Server));

        #[cfg(feature = "gossipsub")]
        swarm
            .behaviour_mut()
            .gossipsub
            .subscribe(&Self::gossip_topic())
            .unwrap();

        Ok(Self {
            swarm,
            action_receiver,
            event_sender,
            pending_request: HashMap::new(),
            pending_dial: HashMap::new(),
            pending_get_peers: HashMap::new(),
        })
    }

    pub async fn start(mut self, multiaddr: Multiaddr) {
        let peer_id = self.swarm.local_peer_id().to_owned();
        self.swarm
            .behaviour_mut()
            .kademlia
            .add_address(&peer_id, multiaddr.clone());

        self.swarm.listen_on(multiaddr.clone()).unwrap();

        self.swarm.add_external_address(multiaddr.clone());

        let mut inter = tokio::time::interval(Duration::from_secs(10));
        loop {
            tokio::select! {
                event = self.swarm.select_next_some() => self.handle_event(event).await,
                action = self.action_receiver.next() => match action {
                    Some(c) => self.handle_action(c).await,
                    None=>  return,
                },
                _ = inter.tick() => {
                    self.list_peers();
                }
            }
        }
    }

    async fn handle_event(&mut self, event: SwarmEvent<BehaviourEvent>) {
        match event {
            SwarmEvent::Behaviour(BehaviourEvent::Kademlia(
                kad::Event::OutboundQueryProgressed {
                    id: _,
                    result: kad::QueryResult::Bootstrap(res),
                    ..
                },
            )) => {
                debug!("bootstrap result {res:?}");
            }

            SwarmEvent::Behaviour(BehaviourEvent::Kademlia(
                kad::Event::OutboundQueryProgressed {
                    id,
                    result:
                        kad::QueryResult::GetClosestPeers(Ok(kad::GetClosestPeersOk { peers, key: _ })),
                    // stats,
                    ..
                },
            )) => {
                if let Some(sender) = self.pending_get_peers.remove(&id) {
                    let _ = sender.send(Ok(peers.clone()));
                    debug!("get peers progress {peers:?}");
                    if let Some(mut q) = self.swarm.behaviour_mut().kademlia.query_mut(&id) {
                        q.finish();
                    }
                }
            }
            SwarmEvent::Behaviour(BehaviourEvent::Kademlia(
                kad::Event::OutboundQueryProgressed {
                    id,
                    result: kad::QueryResult::GetClosestPeers(Err(e)),
                    step,
                    ..
                },
            )) => {
                if let Some(sender) = self.pending_get_peers.remove(&id) {
                    debug!("get closest peers error {e}");
                    if step.last {
                        let _ = sender.send(Err(P2PNetworkError::KadQueryError(e)));
                        if let Some(mut q) = self.swarm.behaviour_mut().kademlia.query_mut(&id) {
                            q.finish();
                        }
                    }
                }
            }
            SwarmEvent::Behaviour(BehaviourEvent::RequestResponse(
                request_response::Event::Message { message, .. },
            )) => match message {
                request_response::Message::Request {
                    request, channel, ..
                } => {
                    let data = request.0.data.clone();
                    if request.0.command == *"multiaddress" {
                        if let Ok(remote_address) =
                            Multiaddr::from_str(String::from_utf8(data).unwrap().as_str())
                        {
                            if let Some(Protocol::P2p(remote_peer_id)) =
                                remote_address.iter().last()
                            {
                                debug!(
                                    "add address from ack {} {}",
                                    remote_peer_id.clone(),
                                    remote_address.clone()
                                );
                                self.swarm
                                    .behaviour_mut()
                                    .kademlia
                                    .add_address(&remote_peer_id, remote_address);
                            }
                        }
                    }
                    self.event_sender
                        .send(Event::InboundRequest {
                            request: PeerRequest(request.0),
                            channel,
                        })
                        .await
                        .expect("Event receiver not to be dropped.");
                }
                request_response::Message::Response {
                    request_id,
                    response,
                } => {
                    let _ = self
                        .pending_request
                        .remove(&request_id)
                        .expect("Request to still be pending.")
                        .send(Ok(response));
                }
            },
            SwarmEvent::Behaviour(BehaviourEvent::RequestResponse(
                request_response::Event::OutboundFailure {
                    request_id, error, ..
                },
            )) => {
                let _ = self
                    .pending_request
                    .remove(&request_id)
                    .expect("Request to still be pending.")
                    .send(Err(P2PNetworkError::SendRequestFailure(error)));
            }
            SwarmEvent::Behaviour(BehaviourEvent::RequestResponse(
                request_response::Event::ResponseSent { .. },
            )) => {}
            SwarmEvent::NewListenAddr { address, .. } => {
                let local_peer_id = *self.swarm.local_peer_id();
                info!(
                    "Local node is listening on {:?}",
                    address.with(Protocol::P2p(local_peer_id))
                );
            }
            SwarmEvent::IncomingConnection { .. } => {}
            SwarmEvent::ConnectionEstablished {
                peer_id,
                connection_id,
                endpoint,
                ..
            } => {
                match endpoint {
                    ConnectedPoint::Dialer { address, .. } => {
                        if let Some(sender) = self.pending_dial.remove(&peer_id) {
                            let _ = sender.send(Ok(()));
                            self.swarm
                                .behaviour_mut()
                                .kademlia
                                .add_address(&peer_id, address.clone());
                        }
                        debug!("connection dialer address {address}");
                    }
                    ConnectedPoint::Listener { send_back_addr, .. } => {
                        self.event_sender
                            .send(Event::IncomeConnection {
                                peer_id,
                                connection_id,
                            })
                            .await
                            .expect("Event receiver not to be dropped.");
                        debug!("connection listener send back address {send_back_addr}");
                    }
                }
                debug!("connected to {peer_id}");
            }
            SwarmEvent::ConnectionClosed {
                peer_id,
                connection_id,
                num_established: _,
                endpoint: _,
                cause,
            } => {
                self.event_sender
                    .send(Event::ConnectionClosed {
                        peer_id,
                        connection_id,
                    })
                    .await
                    .expect("Event receiver not to be dropped.");
                debug!("connection closed {peer_id}, cause {cause:?}");
            }
            SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                if let Some(peer_id) = peer_id {
                    if let Some(sender) = self.pending_dial.remove(&peer_id) {
                        let _ = sender.send(Err(P2PNetworkError::DialError(error)));
                    }
                }
            }
            SwarmEvent::IncomingConnectionError { .. } => {}
            SwarmEvent::Dialing {
                peer_id: Some(peer_id),
                ..
            } => debug!("Dialing {peer_id}"),
            e => debug!("unhandle event: {e:?}"),
        }
    }

    async fn handle_action(&mut self, command: Action) {
        match command {
            Action::Bootstrap {} => {
                let _ = self.swarm.behaviour_mut().kademlia.bootstrap().unwrap();
            }
            Action::GetPeers { key, sender } => {
                let query_id = self.swarm.behaviour_mut().kademlia.get_closest_peers(key);
                self.pending_get_peers.insert(query_id, sender);
            }
            Action::Dial {
                peer_id,
                peer_addr,
                sender,
            } => {
                if let hash_map::Entry::Vacant(e) = self.pending_dial.entry(peer_id) {
                    self.swarm
                        .behaviour_mut()
                        .kademlia
                        .add_address(&peer_id, peer_addr.clone());
                    match self.swarm.dial(peer_addr.with(Protocol::P2p(peer_id))) {
                        Ok(()) => {
                            e.insert(sender);
                        }
                        Err(e) => {
                            let _ = sender.send(Err(P2PNetworkError::DialError(e)));
                        }
                    }
                } else {
                    // already dialing
                    warn!("already dialing {peer_id}")
                }
            }
            Action::SendRequest {
                peer_id,
                msg,
                sender,
            } => {
                let req_id = self
                    .swarm
                    .behaviour_mut()
                    .request_response
                    .send_request(&peer_id, msg);
                self.pending_request.insert(req_id, sender);
            }
            Action::SendResponse { response, channel } => {
                self.swarm
                    .behaviour_mut()
                    .request_response
                    .send_response(channel, response)
                    .unwrap();
            }
        }
    }

    pub fn local_peer_id(&self) -> PeerId {
        self.swarm.local_peer_id().to_owned()
    }

    #[cfg(feature = "gossipsub")]
    fn gossip_topic() -> gossipsub::IdentTopic {
        gossipsub::IdentTopic::new("entropy_gossip")
    }

    fn list_peers(&mut self) {
        for bucket in self.swarm.behaviour_mut().kademlia.kbuckets() {
            if bucket.num_entries() > 0 {
                for item in bucket.iter() {
                    debug!("Peer ID: {:?}", item.node.key);
                }
            }
        }
    }
}
