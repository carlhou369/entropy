use std::{collections::HashMap, sync::Arc, vec};

use {
    rand::random,
    serde::{Deserialize, Serialize},
    tokio::sync::Mutex,
    wirehair::{WirehairDecoder, WirehairEncoder},
};
use {
    actix::prelude::*,
    actix_multipart::form::{bytes::Bytes, MultipartForm, MultipartFormConfig},
    actix_web::{
        get,
        web::{Data, Json, JsonConfig, Query},
        App, HttpResponse, HttpServer, Responder,
        middleware::Logger,
    },
};

#[derive(Message, Clone, Debug)]
#[rtype(result = "()")]
pub struct OutgoingMessage(pub String);

#[derive(Message, Clone, Debug)]
#[rtype(usize)]
pub struct Connect {
    pub addr: String,
}

#[derive(Message, Clone, Debug)]
#[rtype(result = "()")]
pub struct Disconnect {
    pub id: usize,
}

#[derive(Message, Clone, Debug)]
#[rtype(result = "()")]
pub struct Put {
    pub data: String,
}

#[derive(Clone)]
pub struct Server {
    id: usize,
    sessions: HashMap<usize, String>,
}

impl Server {
    pub fn new() -> Server {
        Server {
            id: 0,
            sessions: HashMap::new(),
        }
    }
}

impl Actor for Server {
    type Context = Context<Server>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.set_mailbox_capacity(10000);
    }
}

impl Handler<Connect> for Server {
    type Result = usize;
    fn handle(
        &mut self,
        msg: Connect,
        _: &mut Self::Context,
    ) -> <Server as Handler<Connect>>::Result {
        if self.id == usize::MAX {
            self.id = 0;
        }
        self.id += 1;
        self.sessions.insert(self.id, msg.addr);
        self.id
    }
}

impl Handler<Disconnect> for Server {
    type Result = ();

    fn handle(
        &mut self,
        msg: Disconnect,
        _: &mut Self::Context,
    ) -> <Server as Handler<Disconnect>>::Result {
        self.sessions.remove(&msg.id);
    }
}

impl Handler<Put> for Server {
    type Result = ();
    fn handle(&mut self, msg: Put, _: &mut Self::Context) -> <Server as Handler<Put>>::Result {
        let object_size = msg.data.len();
        let chunk_k = 8;
        let chunk_n = 10;
        let fragment_k = 16;
        let fragment_n = 40;

        let data = msg.data.into_bytes();

        let mut chunks = HashMap::new();
        let outer_encoder = WirehairEncoder::new(data, (object_size / chunk_k) as _);
        for _ in 0..chunk_n {
            let chunk_id = random();
            let mut chunk = vec![0; object_size / chunk_k];
            outer_encoder.encode(chunk_id, &mut chunk).unwrap();
            let inner_encoder =
                WirehairEncoder::new(chunk, (object_size / chunk_k / fragment_k) as _);
            let mut fragments = HashMap::new();
            for _ in 0..fragment_n {
                let fragment_id = random();
                let mut fragment = vec![0; object_size / chunk_k / fragment_k];
                inner_encoder.encode(fragment_id, &mut fragment).unwrap();
                fragments.insert(fragment_id, fragment);
            }
            chunks.insert(chunk_id, fragments);
        }
        ()
    }
}

#[derive(Default)]
pub struct Peer {}

#[derive(Debug, Default)]
struct PeerState {
    pub peers: Arc<Mutex<Vec<(String, u16)>>>,
    pub chunks: Mutex<HashMap<u32, HashMap<u32, Vec<u8>>>>,
    pub chunk_config: ChunkConfig,

    pub fragments: Mutex<HashMap<u32, Vec<u8>>>,
    pub chunk_infos: Mutex<HashMap<u64, (u64, u64, Vec<ChunkInfo>)>>,
}

#[derive(Debug, Clone, Default)]
pub struct ChunkConfig {
    pub fragment_size: usize,
    pub inner_k: usize,
    pub inner_n: usize,
    pub outer_k: usize,
    pub outer_n: usize,
}

#[derive(Debug, Clone, Default)]
struct ChunkInfo {
    chunk_id: u32,
    fragments: Vec<(u32, (String, u16))>,
}

#[get("/ping")]
async fn ping() -> impl Responder {
    "pong".to_string()
}

#[derive(Deserialize)]
struct Addr {
    ip: String,
    port: u16,
}

#[actix_web::post("/join")]
async fn join(data: Data<PeerState>, addr: Query<Addr>) -> impl Responder {
    // todo: use hash_set instead of vector
    data.peers.lock().await.push((addr.ip.clone(), addr.port));
    HttpResponse::Ok().body(format!("{:?}", data.peers.lock().await))
}

#[derive(Debug, MultipartForm)]
struct PutForm {
    #[multipart(rename = "file")]
    files: Vec<Bytes>,
}

#[actix_web::post("/put")]
async fn put(data: Data<PeerState>, MultipartForm(form): MultipartForm<PutForm>) -> impl Responder {
    let msg = form.files[0].data.as_ref().to_vec();
    let object_size = msg.len();

    let chunk_k = data.chunk_config.outer_k;
    let chunk_n = data.chunk_config.outer_n;
    let fragment_k = data.chunk_config.inner_k;
    let fragment_n = data.chunk_config.inner_n;

    let mut sum = 0u64;
    for n in msg.iter() {
        sum = sum + *n as u64;
    }

    let encoder_start = std::time::Instant::now();
    let outer_coder = WirehairEncoder::new(msg, (object_size / chunk_k) as _);
    let mut chunks = HashMap::new();

    for _ in 0..chunk_n {
        let chunk_id = random();
        let mut chunk = vec![0; object_size / chunk_k];
        outer_coder.encode(chunk_id, &mut chunk).unwrap();
        let inner_encoder = WirehairEncoder::new(chunk, (object_size / chunk_k / fragment_k) as _);
        let mut fragments = HashMap::new();
        for _ in 0..fragment_n {
            let fragment_id = random();
            let mut fragment = vec![0; object_size / chunk_k / fragment_k];
            inner_encoder.encode(fragment_id, &mut fragment).unwrap();
            fragments.insert(fragment_id, fragment);
        }
        chunks.insert(chunk_id, fragments);
    }

    println!("encode: {}s", encoder_start.elapsed().as_secs());

    let peers = data.peers.lock().await.clone();
    let mut peers = peers.iter().cycle();
    let mut chunk = Vec::new();

    let dispatch_start = std::time::Instant::now();
    for (chunk_id, fragments) in chunks.iter() {
        let mut chunk_info = ChunkInfo::default();
        chunk_info.chunk_id = *chunk_id;
        let mut set = tokio::task::JoinSet::new();
        for (fragment_id, fragment) in fragments.clone() {
            let (host, port) = peers.next().unwrap().clone();
            chunk_info.fragments.push((fragment_id, (host.clone(), port)));
            set.spawn(async move {
                //let dispatch_fragment_start = std::time::Instant::now();
                let url = format!("http://{host}:{port}/put_chunk");
                let t = &Fragment {
                    id: fragment_id,
                    data: fragment.clone(),
                };
                //println!("{}, {:?}\n\n", t.id, t.data.clone());
                let client = reqwest::Client::new();
                let _ = client.post(url).json(t).send().await;
                //println!("dispatch fragment {}: {}ms", fragment_id, dispatch_fragment_start.elapsed().as_millis());
            });
        }
        while let Some(_) = set.join_next().await {}
        chunk.push(chunk_info);
    }
    println!("dispatch: {}s", dispatch_start.elapsed().as_secs());
    *(data.chunks.lock().await) = chunks;
    data.chunk_infos.lock().await.insert(
        sum,
        (object_size as u64, (object_size / chunk_k) as u64, chunk),
    );
    // dispatch chunk
    HttpResponse::Ok().body(sum.to_string())
}

#[derive(Serialize, Deserialize)]
struct ChunkId {
    id: u64,
}

#[actix_web::post("/get")]
async fn get(data: Data<PeerState>, chunk_id: Query<ChunkId>) -> impl Responder {
    //let chunks = data.chunks.lock().await.clone();
    let chunk_k = data.chunk_config.outer_k;
    let fragment_k = data.chunk_config.inner_k;
    let chunk_infos = data.chunk_infos.lock().await.clone();
    let chunk_infos = chunk_infos.get(&chunk_id.id).unwrap();
    let object_size = chunk_infos.0 as usize;

    let mut chunks = HashMap::new();
    for chunk_info in chunk_infos.2.iter() {
        let mut chunk = HashMap::new();
        let mut set = tokio::task::JoinSet::new();
        for (fragment_id, (host, port)) in chunk_info.fragments.iter() {
            let host = host.clone();
            let port = *port;
            let fragment_id = *fragment_id;
            set.spawn(async move {
                let url = format!("http://{host}:{port}/get_chunk?id={fragment_id}&data=");
                let client = reqwest::Client::new();
                let res = client.post(url).send().await;
                match res {
                    Ok(res) => {
                        let fragment = res.json::<Fragment>().await;
                        let fragment = fragment.unwrap();
                        //println!("{}, {:?} \n\n", fragment_id, fragment.data.clone());
                        Some(fragment)
                    }
                    Err(err) => {
                        println!("{err}");
                        None
                    }
                }
            });
        }
        while let Some(res) = set.join_next().await {
            match res.unwrap() {
                Some(fragment) => {
                    chunk.insert(fragment.id, fragment.data);
                }
                None => {}
            }
        }
        chunks.insert(chunk_info.chunk_id, chunk);
    }

    let mut outer_decoder = WirehairDecoder::new(object_size as _, (object_size / chunk_k) as _);
    for (chunk_id, fragments) in chunks {
        let mut inner_decoder = WirehairDecoder::new(
            (object_size / chunk_k) as _,
            (object_size / chunk_k / fragment_k) as _,
        );
        for (fragment_id, fragment) in fragments {
            let res = inner_decoder.decode(fragment_id, &fragment).unwrap();
            if res {
                break;
            }
        }

        let mut chunk = vec![0; object_size / chunk_k];
        inner_decoder.recover(&mut chunk).unwrap();

        let res = outer_decoder.decode(chunk_id, &chunk).unwrap();
        if res {
            break;
        };
    }
    let mut object = vec![0; object_size];
    outer_decoder.recover(&mut object).unwrap();

    // let mut sum = 0u32;
    // for n in object.iter() {
    //     sum = sum + *n as u32;
    // }
    //let body = once(ok::<_, Error>(bytes::Bytes::from(file)));
    HttpResponse::Ok()
        .content_type("text/markdown")
        .body(object)
}

#[derive(Serialize, Deserialize)]
struct Fragment {
    id: u32,
    #[serde(with = "serde_bytes")]
    data: Vec<u8>,
}

#[actix_web::post("/put_chunk")]
async fn put_chunk(data: Data<PeerState>, fragment: Json<Fragment>) -> impl Responder {
    //println!("{}, {:?} \n\n", fragment.id, fragment.data.clone());

    data.fragments
        .lock()
        .await
        .insert(fragment.id, fragment.data.clone());
    HttpResponse::Ok().body("insert")
}

#[actix_web::post("/get_chunk")]
async fn get_chunk(data: Data<PeerState>, fragment: Query<Fragment>) -> impl Responder {
    match data.fragments.lock().await.get(&fragment.id) {
        Some(res) => {
            let t = &Fragment {
                id: fragment.id,
                data: res.clone(),
            };
            HttpResponse::Ok().json(t)
        }
        None => HttpResponse::Ok().body("not found"),
    }
}

impl Peer {
    pub async fn start(&mut self, host: String, port: u16, peers: Vec<(String, u16)>) {
        let addr = (host, port);
        let mut peer_state = PeerState::default();
        peer_state.chunk_config = ChunkConfig {
            fragment_size: 4 << 20,
            inner_k: 32,
            inner_n: 80,
            outer_k: 8,
            outer_n: 10,
        };

        let data = Data::new(peer_state);
        data.peers
            .lock()
            .await
            .push(("localhost".to_string(), port));
        for (host, port) in peers {
            data.peers.lock().await.push((host, port));
        }

        let _ = HttpServer::new(move || {
            App::new()
                .wrap(Logger::default())
                .app_data(
                    MultipartFormConfig::default()
                        .total_limit(1 << 30)
                        .memory_limit(1 << 30),
                )
                .app_data(JsonConfig::default().limit(1 << 30))
                .app_data(data.clone())
                .service(ping)
                .service(join)
                .service(put)
                .service(get)
                .service(put_chunk)
                .service(get_chunk)
        })
        .bind(addr)
        .expect("failed to bind address")
        .run()
        .await;
    }
}
