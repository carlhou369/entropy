use serde_json::from_str;

use beta::server;

#[tokio::main(flavor = "multi_thread", worker_threads = 20)]
async fn main() {
    // Test for 100 nodes.
    let port = std::env::args().collect::<Vec<String>>()[1].clone();
    let port = from_str(port.as_str()).unwrap();

    println!("start server at {}", port);
    let mut peers = Vec::new();
    if port == 4000 {
        for i in port + 1..port + 100 {
            peers.push(("localhost".to_string(), i));
        }
    }
    let _ = server::Peer::default()
        .start("0.0.0.0".to_string(), port, peers)
        .await;
}
