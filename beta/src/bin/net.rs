use beta::keystore::KeyStore;
use beta::p2p::behaviour::{Action, Event, Network, PeerRequest, PeerResponse};
use beta::p2p::utils::bootstrap_peer;
use beta::reqres_proto::{PeerRequestMessage, PeerResponseMessage};
use beta::server;
use clap::Parser;
use env_logger::{Builder, Env};
use futures::future;
use futures::StreamExt;
use futures::{
    channel::{mpsc, oneshot},
    future::join_all,
    SinkExt,
};
use libp2p::PeerId;
use libp2p::{multiaddr::Protocol, Multiaddr};
use log::info;
use rand::random;

use std::collections::HashSet;
use std::error::Error;
use std::path::PathBuf;

use tokio::task::spawn;
use tokio::time::{self, Duration, Instant};

use std::sync::Arc;
use tokio::sync::Mutex;

const DEFAULT_BENCH_CNT: usize = 100;

#[derive(clap::Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Sets a p2p network port
    #[arg(long, value_name = "PORT")]
    port: Option<u16>,

    /// Sets a p2p network endpoint
    #[arg(long, value_name = "P2P")]
    p2p: bool,

    /// Sets full node muldiaddress
    #[arg(short, long, value_name = "FUllADDR")]
    full: Option<String>,

    #[arg(short, long, value_name = "KEY")]
    key: Option<PathBuf>,

    #[arg(short, long, value_name = "KEY")]
    bench_peers: Option<usize>,

    /// Turn debugging information on
    #[arg(short, long, value_name = "LOGLEVL")]
    log_level: Option<String>,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> Result<(), Box<dyn Error>> {
    // console_subscriber::init();
    let cli = Cli::parse();
    let log_level = cli.log_level.unwrap_or("info".into());
    let env = Env::default().default_filter_or(&log_level);
    Builder::from_env(env).format_timestamp_millis().init();

    if !cli.p2p {
        let port = cli.port.unwrap_or_else(|| 6000 + random::<u16>() % 100);
        info!("start serve at {port}");
        let mut peers = Vec::new();
        if port == 4000 {
            for i in port + 1..port + 100 {
                peers.push(("localhost".to_string(), i));
            }
        }
        let _ = server::Peer::default()
            .start("0.0.0.0".to_string(), port, peers)
            .await;
        return Ok(());
    }

    let bench_light_node_cnt: usize = cli.bench_peers.unwrap_or(DEFAULT_BENCH_CNT);

    // Init keystore
    let path = cli.key.unwrap_or(PathBuf::from("./keystore"));
    let keystore = KeyStore::generate_from_file(path.clone()).unwrap_or(KeyStore::generate());
    keystore.save_to(path).unwrap();

    // Demo p2p
    let (mut action_sender, action_receiver) = mpsc::channel(0);
    let (event_sender, mut event_receiver) = mpsc::channel(0);

    // New p2p Network
    let network = Network::new(Some(keystore.seed), action_receiver, event_sender)
        .await
        .unwrap();

    let port = cli.port.unwrap_or_else(|| 6000 + random::<u16>() % 100);
    info!("start p2p at {port}");

    let mut address_local: Multiaddr = format!("/ip4/127.0.0.1/tcp/{}", port).parse().unwrap();
    let local_peer_id = network.local_peer_id();
    address_local = address_local.with_p2p(local_peer_id).unwrap();

    // Start P2P network listener
    spawn(network.start(address_local.clone()));

    let connected_peers = Arc::new(Mutex::new(HashSet::new()));
    let peers_clone = Arc::clone(&connected_peers);

    // Demo P2P network event handler
    let mut action_sender_dup = action_sender.clone();
    spawn(async move {
        while let Some(e) = event_receiver.next().await {
            match e {
                Event::InboundRequest { request, channel } => {
                    info!("handle inboud request {request:?}");
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
                        .unwrap()
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
    });

    if let Some(full_node_addr_str) = cli.full {
        // Start light node demo
        let full_node: Multiaddr = full_node_addr_str.parse().unwrap();
        if let Some(Protocol::P2p(peer_id)) = full_node.iter().last() {
            // Bootstrap light node
            bootstrap_peer(action_sender.clone(), full_node.clone(), address_local)
                .await
                .unwrap();

            // Light node send demo requests
            let mut inter = time::interval(Duration::from_secs(10));
            loop {
                inter.tick().await;
                let (sender, receiver) = oneshot::channel();

                info!("send request greeting");
                action_sender
                    .send(Action::SendRequest {
                        peer_id,
                        msg: PeerRequest(PeerRequestMessage {
                            id: format!("{}", random::<u16>()),
                            command: "greeting".to_string(),
                            data: b"hello".to_vec(),
                        }),
                        sender,
                    })
                    .await
                    .unwrap();

                let res = receiver.await.unwrap();
                info!("get response {:?}", res);

                let (sender, receiver) = oneshot::channel();
                // Demo discovery query random peers
                let key = PeerId::random();
                action_sender
                    .send(Action::GetPeers {
                        key: key.into(),
                        sender,
                    })
                    .await
                    .unwrap();
                let res = receiver.await.unwrap();
                info!("get closest peers {:?}", res);
            }
        } else {
            panic!("peer addr format error");
        }
    } else {
        // Start as full node demo
        let peers_clone = Arc::clone(&connected_peers);
        let mut action_sender_dup = action_sender.clone();
        let mut inter = time::interval(Duration::from_secs(1));
        loop {
            inter.tick().await;
            let peers = peers_clone.lock().await;
            let mut receivers = vec![];
            if peers.len() >= bench_light_node_cnt {
                let start_time = Instant::now();
                let msg = PeerRequest(PeerRequestMessage {
                    id: format!("{}", random::<u16>()),
                    command: "broadcast".to_string(),
                    data: b"enough peers".to_vec(),
                });
                for peer_id in peers.iter() {
                    let (sender, receiver) = oneshot::channel();
                    receivers.push(receiver);
                    action_sender_dup
                        .send(Action::SendRequest {
                            peer_id: peer_id.to_owned(),
                            msg: msg.clone(),
                            sender,
                        })
                        .await
                        .unwrap();
                }
                join_all(receivers.into_iter().map(|r| async {
                    match r.await {
                        Ok(Ok(res)) => {
                            info!("broadcast ack {:?}", res);
                        }
                        _ => panic!("response error"),
                    }
                }))
                .await;
                let dur = Instant::now() - start_time;
                info!("broadcast and wait ack takes {:?} ms", dur.as_millis());
                break;
            }
        }
    }

    let p = future::pending();
    let () = p.await;
    Ok(())
}
