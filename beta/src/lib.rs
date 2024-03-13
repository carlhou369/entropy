pub mod codec;
pub mod server;
pub mod p2p;
pub mod keystore;
mod vrf;

pub mod reqres_proto {
    include!(concat!(env!("OUT_DIR"), "/reqres_proto.rs"));
}

#[cfg(test)]
pub mod test {
    use actix::Actor;

    use crate::server::{self, Connect};

    #[actix::test]
    async fn run() {
        let s = server::Server::new();
        println!("start server");
        let addr = s.start();

        loop {
            tokio::select! {
                _ = addr.send(Connect{addr: "Hello".to_string()}) => {

                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 20)]
    async fn test_peer() {
        env_logger::init_from_env(
            env_logger::Env::new().default_filter_or("info"),
        );
        let _ = server::Peer::default()
            .start("0.0.0.0".to_string(), 4000, Vec::new())
            .await;
    }
}
