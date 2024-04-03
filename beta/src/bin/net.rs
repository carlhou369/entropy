use beta::keystore::KeyStore;
use beta::p2p::behaviour::Network;
use beta::p2p::client::Client;
use beta::p2p::utils::random_req_id;
use beta::reqres_proto::PeerRequestMessage;
use beta::{cid, server};
use clap::Parser;
use env_logger::{Builder, Env};
use futures::future;
use futures::{channel::mpsc, future::join_all};
use libp2p::PeerId;
use libp2p::{multiaddr::Protocol, Multiaddr};
use log::{error, info};
use rand::distributions::Uniform;
use rand::{random, Rng};
use tokio::sync::Mutex;

use std::error::Error;
use std::path::PathBuf;

use tokio::task::spawn;
use tokio::time::{self, Duration, Instant};

use std::sync::Arc;

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

    #[arg(short, long, value_name = "Size")]
    payload_size: Option<usize>,

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

    let bench_light_node_cnt: usize =
        cli.bench_peers.unwrap_or(DEFAULT_BENCH_CNT);
    let payload_size: usize = cli.payload_size.unwrap_or(1024 * 1024);

    // Init keystore
    let path = cli.key.unwrap_or(PathBuf::from("./keystore"));
    let keystore = KeyStore::generate_from_file(path.clone())
        .unwrap_or(KeyStore::generate());
    keystore.save_to(path).unwrap();

    // Demo p2p
    let (action_sender, action_receiver) = mpsc::channel(10);
    let (event_sender, event_receiver) = mpsc::channel(10);

    // New p2p Network
    let network =
        Network::new(Some(keystore.seed), action_receiver, event_sender)
            .await
            .unwrap();

    let control = network.control();

    let port = cli.port.unwrap_or_else(|| 6000 + random::<u16>() % 100);
    info!("start p2p at {port}");
    let mut address_local: Multiaddr =
        format!("/ip4/127.0.0.1/tcp/{}", port).parse().unwrap();

    let local_peer_id = network.local_peer_id();
    address_local = address_local.with_p2p(local_peer_id).unwrap();
    info!("local peer id {}", local_peer_id.clone().to_string());

    // New client
    let client = Client::new(
        local_peer_id,
        address_local.clone(),
        action_sender.clone(),
        Arc::new(Mutex::new(event_receiver)),
        control,
    );

    // Start P2P network listener
    spawn(network.start(address_local.clone()));

    let client_clone = client.clone();
    spawn(async move { client_clone.handle_event().await });

    if let Some(full_node_addr_str) = cli.full {
        // Start light node demo
        let full_node: Multiaddr = full_node_addr_str.parse().unwrap();
        if let Some(Protocol::P2p(peer_id)) = full_node.iter().last() {
            // Bootstrap light node
            client.bootstrap_light(full_node).await.unwrap();

            // Light node send demo requests
            let mut inter = time::interval(Duration::from_secs(10));
            loop {
                inter.tick().await;

                info!("send request greeting");
                let msg = PeerRequestMessage {
                    id: random_req_id(),
                    command: "greeting".to_string(),
                    data: b"hello".to_vec(),
                };
                let res = client.request(peer_id, msg).await.unwrap();
                info!("get response {:?}", res);

                // Demo discovery query random peers
                let key = PeerId::random();
                let res = client.get_closest_peers(key.into()).await.unwrap();
                info!("get closest peers {:?}", res);
            }
        } else {
            panic!("peer addr format error");
        }
    } else {
        // Start as full node demo
        let mut rng = rand::thread_rng();
        let between = Uniform::from(0..=255);
        let payload: Vec<u8> =
            (0..payload_size).map(|_| rng.sample(between)).collect();

        // Bench send data large chunk
        let mut inter = time::interval(Duration::from_secs(1));
        loop {
            inter.tick().await;
            let peers = client.get_connected_peers().await;
            if peers.len() >= bench_light_node_cnt {
                info!("start sending chunks");
                let start_time = Instant::now();
                let mut tasks = Vec::new();
                for peer_id in peers.iter() {
                    let task =
                        client.send_chunk(peer_id.to_owned(), payload.clone());
                    tasks.push(task);
                }
                join_all(tasks).await;
                let dur = Instant::now() - start_time;
                info!(
                    "send chunks and wait ack takes {:?} ms",
                    dur.as_millis()
                );
                // test get chunk
                if let Err(e) = client.get_chunk(peers[0], cid(&payload)).await
                {
                    error!("get chunk error {e:?}");
                };
                break;
            }
        }
    }

    let p = future::pending();
    let () = p.await;
    Ok(())
}
