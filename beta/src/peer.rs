use std::{
    collections::{hash_map::Entry, HashMap},
    fs::OpenOptions,
    io::{self, Read, Write},
    sync::Arc,
    time::Duration,
};

use actix_web::web::{self, Query};
use libp2p::PeerId;
use log::{debug, error, info};
use tokio::{sync::Semaphore, task::JoinSet};

use crate::{
    cid,
    codec::wirehair_codec::{WirehairCodec, WirehairCodecOptions},
    p2p::{client::Client, error::P2PNetworkError},
    CID,
};

use std::fs;
use {
    actix_multipart::form::{bytes::Bytes, MultipartForm, MultipartFormConfig},
    actix_web::{
        get,
        middleware::Logger,
        web::{Data, JsonConfig},
        App, HttpResponse, HttpServer, Responder,
    },
};
use {
    serde::{Deserialize, Serialize},
    tokio::sync::Mutex,
};

const MAX_OUTBOUND_STREAM: usize = 30;

const MAX_INBOUND_STREAM: usize = 10;

#[derive(Debug, Clone)]
struct ContentMeta {
    pub chunk_meta: HashMap<u32, HashMap<u32, (CID, Vec<PeerId>)>>,
    pub size: usize,
}

#[derive(Default)]
pub struct FullNodeService {}

#[derive(Debug)]
struct PeerState {
    pub content_log: Mutex<HashMap<CID, ContentMeta>>,
    pub codec: WirehairCodec,
    pub p2p_client: Client,
    outbound_stream_sema: Arc<Semaphore>,
    inbound_stream_sema: Arc<Semaphore>,
}

impl PeerState {
    pub fn new(peer_opt: PeerOpt, p2p_client: Client) -> Self {
        let codec = match peer_opt.codec_opt {
            Some(option) => WirehairCodec::new_with_options(option),
            None => WirehairCodec::new(),
        };
        PeerState {
            content_log: Mutex::new(HashMap::new()),
            codec,
            p2p_client,
            outbound_stream_sema: Arc::new(Semaphore::new(
                peer_opt.max_oubound_stream.unwrap_or(MAX_OUTBOUND_STREAM),
            )),
            inbound_stream_sema: Arc::new(Semaphore::new(
                peer_opt.max_inbound_stream.unwrap_or(MAX_INBOUND_STREAM),
            )),
        }
    }
}

#[get("/healthcheck")]
async fn healthcheck() -> impl Responder {
    HttpResponse::Ok().body("ok".to_string())
}

#[derive(Debug, MultipartForm)]
struct PutForm {
    #[multipart(rename = "file")]
    files: Vec<Bytes>,
}

#[actix_web::post("/put")]
async fn put(
    data: Data<PeerState>,
    MultipartForm(form): MultipartForm<PutForm>,
) -> impl Responder {
    let msg = form.files[0].data.as_ref().to_vec();
    let object_size = msg.len();
    let msg_cid = cid(&msg);

    if data.content_log.lock().await.contains_key(&msg_cid.clone()) {
        return HttpResponse::Ok().body(msg_cid.0);
    }

    info!("put msg size {}, cid {}", object_size, msg_cid.0.clone());
    let chunks = data.codec.encode(msg);
    debug!("chunks encoded");
    let mut content_meta = HashMap::new();
    let mut set = JoinSet::new();
    for (chunk_id, chunk) in chunks.into_iter() {
        let mut chunk_meta = HashMap::new();
        for (fragment_id, fragment) in chunk.into_iter() {
            let fragment_cid = cid(&fragment);
            // save local copy
            // todo: save local
            // let mut file = fs::File::create(fragment_cid.clone().0).unwrap();
            // file.write_all(&fragment).unwrap();
            // file.flush().unwrap();

            // send fragment to random selected closest peer
            let closest = data
                .p2p_client
                .random_closest_peer(fragment_cid.clone().into(), 1)
                .await
                .unwrap();
            for peer in closest.clone().into_iter() {
                let client = data.p2p_client.clone();
                let fragment_clone = fragment.clone();
                let sema = data.outbound_stream_sema.clone();
                set.spawn(async move {
                    let permit = sema.acquire().await.unwrap();
                    let fragment_cid = cid(&fragment_clone);
                    info!(
                        "sending light node {} fragment {}",
                        peer.to_string(),
                        fragment_cid.0
                    );
                    let res = client.send_chunk(peer, fragment_clone).await;
                    info!(
                        "done sent light node {} fragment {}",
                        peer.to_string(),
                        fragment_cid.0
                    );
                    drop(permit);
                    res
                });
            }

            chunk_meta.insert(fragment_id.to_owned(), (fragment_cid, closest));
        }
        content_meta.insert(chunk_id.to_owned(), chunk_meta);
    }

    while let Some(res) = set.join_next().await {
        if let Err(e) = res.unwrap() {
            error!("{e:?}");
        };
    }

    data.content_log.lock().await.insert(
        msg_cid.clone(),
        ContentMeta {
            chunk_meta: content_meta,
            size: object_size,
        },
    );
    HttpResponse::Ok().body(msg_cid.0)
}

#[actix_web::get("/get/{chunk_id}")]
async fn get(
    data: Data<PeerState>,
    chunk_id: web::Path<String>,
) -> impl Responder {
    let cid = CID(chunk_id.into_inner());
    info!("get cid {}", cid.0.clone());
    let content_log = data.content_log.lock().await;
    let content_meta = content_log.get(&cid);
    if content_meta.is_none() {
        return HttpResponse::BadRequest()
            .body(format!("content not exist cid {}", cid.0));
    }
    let content_meta = content_meta.unwrap().to_owned();

    let object_size = content_meta.size as usize;
    let mut links: HashMap<u32, HashMap<u32, Vec<u8>>> = HashMap::new();
    let mut set = JoinSet::new();
    for (chunk_id, chunk_meta) in content_meta.chunk_meta.into_iter() {
        for (fragment_id, (fragment_cid, light_peers)) in chunk_meta.into_iter()
        {
            let client = data.p2p_client.clone();
            let sema = data.inbound_stream_sema.clone();
            set.spawn(async move {
                if let Ok(_) = read_local_content(fragment_cid.clone()) {
                    return Ok((chunk_id, fragment_id, fragment_cid.clone()));
                };
                let permit = sema.acquire().await.unwrap();
                for light_peer in light_peers.into_iter() {
                    debug!(
                        "getting fragment {} from {}",
                        fragment_cid.0.clone(),
                        light_peer.clone()
                    );
                    match client
                        .get_chunk(light_peer, fragment_cid.clone())
                        .await
                    {
                        Ok(()) => {
                            debug!(
                                "done getting fragment {} from {}",
                                fragment_cid.0.clone(),
                                light_peer.clone()
                            );
                            drop(permit);
                            tokio::time::sleep(Duration::from_millis(3)).await;
                            return Ok((
                                chunk_id,
                                fragment_id,
                                fragment_cid.clone(),
                            ));
                        },
                        Err(e) => {
                            debug!(
                                "done getting fragment {} from {}",
                                fragment_cid.0.clone(),
                                light_peer.clone()
                            );
                            error!(
                                "get chunk {} from peer {} error {}",
                                fragment_cid.0.clone(),
                                light_peer.clone(),
                                e
                            );
                            continue;
                        },
                    };
                }
                drop(permit);
                tokio::time::sleep(Duration::from_millis(1)).await;
                Err((chunk_id, fragment_id, fragment_cid.clone()))
            });
        }
    }

    while let Some(res) = set.join_next().await {
        let res = res.unwrap();
        // let (chunk_id, fragment_id, fragment_cid) = res.unwrap();
        let (chunk_id, fragment_id, fragment_cid) = res.unwrap_or_else(|x| x);
        let buf = match read_local_content(fragment_cid.clone()) {
            Ok(buf) => buf,
            Err(e) => {
                error!("read local fragment content error {}", e);
                continue;
            },
        };
        debug!("insert fragment {}", fragment_cid.0.clone());
        match links.entry(chunk_id) {
            Entry::Occupied(o) => {
                let o = o.into_mut();
                o.insert(fragment_id, buf);
            },
            Entry::Vacant(v) => {
                let mut chunk_links: HashMap<u32, Vec<u8>> = HashMap::new();
                chunk_links.insert(fragment_id, buf);
                v.insert(chunk_links);
            },
        }

        // match res {
        //     Ok((chunk_id, fragment_id, fragment_cid)) => {
        //         let mut file = match std::fs::File::open(fragment_cid.0.clone())
        //         {
        //             Ok(file) => file,
        //             Err(e) => {
        //                 error!("open file {:?}", e);
        //                 continue;
        //             },
        //         };
        //         let mut buf = Vec::new();
        //         if let Err(e) = file.read_to_end(&mut buf) {
        //             error!("read file {:?}", e);
        //             continue;
        //         }
        //         if super::cid(&buf).0 != fragment_cid.0.clone() {
        //             error!(
        //                 "fragment content inconsistent {}",
        //                 fragment_cid.0.clone()
        //             );
        //             continue;
        //         }
        //         match links.entry(chunk_id) {
        //             Entry::Occupied(o) => {
        //                 let o = o.into_mut();
        //                 o.insert(fragment_id, buf);
        //             },
        //             Entry::Vacant(v) => {
        //                 let mut chunk_links: HashMap<u32, Vec<u8>> =
        //                     HashMap::new();
        //                 chunk_links.insert(fragment_id, buf);
        //                 v.insert(chunk_links);
        //             },
        //         }
        //     },
        //     Err((chunk_id, fragment_id, _fragment_cid)) => {
        //         // return HttpResponse::InternalServerError().body(format!(
        //         //     "content {} chunk {} fragment {} not found",
        //         //     cid.0, chunk_id, fragment_id
        //         // ));
        //         error!(
        //             "content {} chunk {} fragment {} not found",
        //             cid.0, chunk_id, fragment_id
        //         );
        //         continue;
        //     },
        // }
    }

    let object = data.codec.decode(links, object_size);
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

#[derive(Debug, Clone, Copy)]
pub struct PeerOpt {
    pub codec_opt: Option<WirehairCodecOptions>,
    pub max_inbound_stream: Option<usize>,
    pub max_oubound_stream: Option<usize>,
}

impl Default for PeerOpt {
    fn default() -> Self {
        Self {
            codec_opt: Some(WirehairCodecOptions::default()),
            max_inbound_stream: Some(MAX_INBOUND_STREAM),
            max_oubound_stream: Some(MAX_OUTBOUND_STREAM),
        }
    }
}

impl FullNodeService {
    pub async fn start(
        &mut self,
        host: String,
        port: u16,
        peer_opt: PeerOpt,
        client: Client,
    ) {
        let addr = (host, port);
        let peer_state = PeerState::new(peer_opt, client);

        let data = Data::new(peer_state);
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
                .service(healthcheck)
                .service(put)
                .service(get)
        })
        .bind(addr)
        .expect("failed to bind address")
        .run()
        .await;
    }
}

fn read_local_content(fragment_cid: CID) -> Result<Vec<u8>, P2PNetworkError> {
    let mut file = match std::fs::File::open(fragment_cid.0.clone()) {
        Ok(file) => file,
        Err(e) => {
            debug!("open file {} error {:?}", e, fragment_cid.0.clone());
            return Err(e.into());
        },
    };
    let mut buf = Vec::new();
    if let Err(e) = file.read_to_end(&mut buf) {
        debug!("read file {} error {:?}", fragment_cid.0.clone(), e);
        return Err(e.into());
    }
    if super::cid(&buf).0 != fragment_cid.0.clone() {
        error!("fragment content inconsistent {}", fragment_cid.0.clone());
        return Err(P2PNetworkError::Other(format!(
            "fragment content inconsistent {}",
            fragment_cid.0.clone()
        )));
    }
    Ok(buf)
}
