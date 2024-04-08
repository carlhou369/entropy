use beta::keystore::KeyStore;
use beta::p2p::behaviour::Network;
use beta::p2p::client::Client;

use beta::peer::{self, PeerOpt};
use clap::Parser;
use env_logger::{Builder, Env};
use futures::channel::mpsc;
use futures::future;

use libp2p::{multiaddr::Protocol, Multiaddr};
use log::info;

use rand::random;
use tokio::sync::Mutex;

use std::error::Error;
use std::path::PathBuf;

use tokio::task::spawn;

use std::sync::Arc;

#[derive(clap::Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Sets a p2p network port
    #[arg(long, value_name = "P2PPORT")]
    pport: Option<u16>,

    /// Sets a full node server listen port
    #[arg(long, value_name = "SERVERPORT")]
    sport: Option<u16>,

    /// Sets full node muldiaddress
    #[arg(short, long, value_name = "FUllADDR")]
    full: Option<String>,

    #[arg(short, long, value_name = "KEY")]
    key: Option<PathBuf>,

    #[arg(short, long, value_name = "INBOUND_STREAM_MAX")]
    inbound_stream_max: Option<usize>,

    #[arg(short, long, value_name = "OUTBOUND_STREAM_MAX")]
    outbound_stream_max: Option<usize>,

    /// Turn debugging information on
    #[arg(short, long, value_name = "LOGLEVL")]
    log_level: Option<String>,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    let log_level = cli.log_level.unwrap_or("info".into());
    let env = Env::default().default_filter_or(&log_level);
    Builder::from_env(env).format_timestamp_millis().init();

    // Init keystore
    let path = cli.key.unwrap_or(PathBuf::from("./keystore"));
    let keystore = KeyStore::generate_from_file(path.clone())
        .unwrap_or(KeyStore::generate());
    keystore.save_to(path).unwrap();

    // Demo p2p
    let (action_sender, action_receiver) = mpsc::channel(100);
    let (event_sender, event_receiver) = mpsc::channel(100);

    // New p2p Network
    let network =
        Network::new(Some(keystore.seed), action_receiver, event_sender)
            .await
            .unwrap();

    let port = cli.pport.unwrap_or_else(|| 6000 + random::<u16>() % 100);
    info!("start p2p at {port}");
    let mut address_local: Multiaddr =
        format!("/ip4/127.0.0.1/tcp/{}", port).parse().unwrap();

    let local_peer_id = network.local_peer_id();
    address_local = address_local.with_p2p(local_peer_id).unwrap();
    info!("local peer id {}", local_peer_id.clone().to_string());

    // New client
    let p2p_client = Client::new(
        local_peer_id,
        address_local.clone(),
        action_sender.clone(),
        Arc::new(Mutex::new(event_receiver)),
        network.control(),
    );

    // Start P2P network listener
    spawn(network.start(address_local.clone()));

    let client_clone = p2p_client.clone();
    spawn(async move { client_clone.handle_event().await });

    match cli.full {
        Some(full_node_addr_str) => {
            // Start light node demo
            let full_node: Multiaddr = full_node_addr_str.parse().unwrap();
            if let Some(Protocol::P2p(_)) = full_node.iter().last() {
                // Bootstrap light node
                p2p_client.bootstrap_light(full_node).await.unwrap();
            } else {
                panic!("peer addr format error");
            }
        },
        None => {
            // Start full node demo
            let mut peer = peer::FullNodeService::default();
            peer.start(
                "0.0.0.0".to_string(),
                cli.sport.unwrap(),
                PeerOpt {
                    codec_opt: None,
                    max_inbound_stream: cli.inbound_stream_max,
                    max_oubound_stream: cli.outbound_stream_max,
                },
                p2p_client,
            )
            .await;
        },
    }
    let p = future::pending();
    let () = p.await;
    Ok(())
}
