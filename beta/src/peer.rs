use std::{
    collections::{hash_map::Entry, HashMap},
    io::{Read, Write},
};

use actix_web::web::{self, Query};
use libp2p::PeerId;
use log::{error, info};
use tokio::task::JoinSet;

use crate::{
    cid,
    codec::wirehair_codec::{WirehairCodec, WirehairCodecOptions},
    p2p::client::Client,
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
}

impl PeerState {
    pub fn new(
        codec_option: Option<WirehairCodecOptions>,
        p2p_client: Client,
    ) -> Self {
        let codec = match codec_option {
            Some(option) => WirehairCodec::new_with_options(option),
            None => WirehairCodec::new(),
        };
        PeerState {
            content_log: Mutex::new(HashMap::new()),
            codec,
            p2p_client,
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
    info!("msg size {}, cid {}", object_size, msg_cid.0.clone());
    let chunks = data.codec.encode(msg);
    info!("chunks encoded");
    let mut content_meta = HashMap::new();
    // let mut set = JoinSet::new();
    for (chunk_id, chunk) in chunks.into_iter() {
        let mut chunk_meta = HashMap::new();
        for (fragment_id, fragment) in chunk.into_iter() {
            let fragment_cid = cid(&fragment);
            // save local copy
            let mut file = fs::File::create(fragment_cid.clone().0).unwrap();
            file.write_all(&fragment).unwrap();
            file.flush().unwrap();

            // send fragment to random selected closest peer
            let closest = data
                .p2p_client
                .random_closest_peer(fragment_cid.clone().into(), 2)
                .await
                .unwrap();
            for peer in closest.clone().into_iter() {
                let client = data.p2p_client.clone();
                let fragment_clone = fragment.clone();
                // set.spawn(async move {
                let fragment_cid = cid(&fragment_clone);
                info!(
                    "sending light node {} fragment {}",
                    peer.to_string(),
                    fragment_cid.0
                );
                client.send_chunk(peer, fragment_clone).await.unwrap();
                info!("done")
                // });
            }

            chunk_meta.insert(fragment_id.to_owned(), (fragment_cid, closest));
        }
        content_meta.insert(chunk_id.to_owned(), chunk_meta);
    }

    // while let Some(res) = set.join_next().await {
    //     if let Err(e) = res.unwrap() {
    //         error!("{e:?}");
    //     };
    // }

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
    // let mut set = JoinSet::new();
    for (chunk_id, chunk_meta) in content_meta.chunk_meta.into_iter() {
        for (fragment_id, (fragment_cid, light_peers)) in chunk_meta.into_iter()
        {
            let client = data.p2p_client.clone();
            let res = (async move {
                for light_peer in light_peers.into_iter() {
                    match client
                        .get_chunk(light_peer, fragment_cid.clone())
                        .await
                    {
                        Ok(()) => {
                            return Ok((
                                chunk_id,
                                fragment_id,
                                fragment_cid.clone(),
                            ));
                        },
                        Err(e) => {
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
                Err((chunk_id, fragment_id, fragment_cid.clone()))
            })
            .await;
            match res {
                Ok((chunk_id, fragment_id, fragment_cid)) => {
                    info!(
                        "get chunkid {} fragmentid {} fragment_cid {}",
                        chunk_id,
                        fragment_id,
                        fragment_cid.0.clone(),
                    );
                    let resp_error =
                        HttpResponse::InternalServerError().body(format!(
                            "content {} chunk {} fragment {} not found",
                            cid.0, chunk_id, fragment_id
                        ));
                    let mut file =
                        match std::fs::File::open(fragment_cid.0.clone()) {
                            Ok(file) => file,
                            Err(e) => {
                                error!("open file {:?}", e);
                                return resp_error;
                            },
                        };
                    let mut buf = Vec::new();
                    if let Err(e) = file.read_to_end(&mut buf) {
                        error!("read file {:?}", e);
                        return resp_error;
                    }
                    match links.entry(chunk_id) {
                        Entry::Occupied(o) => {
                            let o = o.into_mut();
                            o.insert(fragment_id, buf);
                        },
                        Entry::Vacant(v) => {
                            let mut chunk_links: HashMap<u32, Vec<u8>> =
                                HashMap::new();
                            chunk_links.insert(fragment_id, buf);
                            v.insert(chunk_links);
                        },
                    }
                },
                Err((chunk_id, fragment_id, _fragment_cid)) => {
                    return HttpResponse::InternalServerError().body(format!(
                        "content {} chunk {} fragment {} not found",
                        cid.0, chunk_id, fragment_id
                    ));
                },
            }
        }
    }

    // while let Some(res) = set.join_next().await {
    //     let s = res.unwrap();
    //     match s {
    //         Ok((chunk_id, fragment_id, fragment_cid)) => {
    //             let resp_error =
    //                 HttpResponse::InternalServerError().body(format!(
    //                     "content {} chunk {} fragment {} not found",
    //                     cid.0, chunk_id, fragment_id
    //                 ));
    //             let mut file = match std::fs::File::open(fragment_cid.0.clone())
    //             {
    //                 Ok(file) => file,
    //                 Err(e) => {
    //                     error!("open file {:?}", e);
    //                     return resp_error;
    //                 },
    //             };
    //             let mut buf = Vec::new();
    //             if let Err(e) = file.read_to_end(&mut buf) {
    //                 error!("read file {:?}", e);
    //                 return resp_error;
    //             }
    //             match links.entry(chunk_id) {
    //                 Entry::Occupied(o) => {
    //                     let o = o.into_mut();
    //                     o.insert(fragment_id, buf);
    //                 },
    //                 Entry::Vacant(v) => {
    //                     let mut chunk_links: HashMap<u32, Vec<u8>> =
    //                         HashMap::new();
    //                     chunk_links.insert(fragment_id, buf);
    //                     v.insert(chunk_links);
    //                 },
    //             }
    //         },
    //         Err((chunk_id, fragment_id, _fragment_cid)) => {
    //             return HttpResponse::InternalServerError().body(format!(
    //                 "content {} chunk {} fragment {} not found",
    //                 cid.0, chunk_id, fragment_id
    //             ));
    //         },
    //     }
    // }

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

impl FullNodeService {
    pub async fn start(
        &mut self,
        host: String,
        port: u16,
        codec_option: Option<WirehairCodecOptions>,
        client: Client,
    ) {
        let addr = (host, port);
        let peer_state = PeerState::new(codec_option, client);

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
