use std::{collections::HashMap, io::Write, sync::Arc, vec};

use libp2p::PeerId;
use log::error;
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
        web::{Data, Json, JsonConfig, Query},
        App, HttpResponse, HttpServer, Responder,
    },
};
use {
    rand::random,
    serde::{Deserialize, Serialize},
    tokio::sync::Mutex,
    wirehair::{WirehairDecoder, WirehairEncoder},
};

#[derive(Debug)]
struct ContentMeta {
    pub chunk_meta: HashMap<u32, HashMap<u32, (CID, Vec<PeerId>)>>,
    pub size: usize,
}

#[derive(Default)]
pub struct Peer {}

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
    let chunks = data.codec.encode(msg);
    let mut content_meta = HashMap::new();
    let mut set = JoinSet::new();
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
                set.spawn(async move {
                    client.send_chunk(peer, fragment_clone).await
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

#[derive(Serialize, Deserialize)]
struct ChunkId {
    id: u64,
}

// #[actix_web::post("/get")]
// async fn get(
//     data: Data<PeerState>,
//     chunk_id: Query<ChunkId>,
// ) -> impl Responder {
//     //let chunks = data.chunks.lock().await.clone();
//     let chunk_k = data.chunk_config.outer_k;
//     let fragment_k = data.chunk_config.inner_k;
//     let chunk_infos = data.chunk_infos.lock().await.clone();
//     let chunk_infos = chunk_infos.get(&chunk_id.id).unwrap();
//     let object_size = chunk_infos.0 as usize;

//     let mut chunks = HashMap::new();
//     for chunk_info in chunk_infos.2.iter() {
//         let mut chunk = HashMap::new();
//         let mut set = tokio::task::JoinSet::new();
//         for (fragment_id, (host, port)) in chunk_info.fragments.iter() {
//             let host = host.clone();
//             let port = *port;
//             let fragment_id = *fragment_id;
//             set.spawn(async move {
//                 let url = format!(
//                     "http://{host}:{port}/get_chunk?id={fragment_id}&data="
//                 );
//                 let client = reqwest::Client::new();
//                 let res = client.post(url).send().await;
//                 match res {
//                     Ok(res) => {
//                         let fragment = res.json::<Fragment>().await;
//                         let fragment = fragment.unwrap();
//                         //println!("{}, {:?} \n\n", fragment_id, fragment.data.clone());
//                         Some(fragment)
//                     },
//                     Err(err) => {
//                         println!("{err}");
//                         None
//                     },
//                 }
//             });
//         }
//         while let Some(res) = set.join_next().await {
//             match res.unwrap() {
//                 Some(fragment) => {
//                     chunk.insert(fragment.id, fragment.data);
//                 },
//                 None => {},
//             }
//         }
//         chunks.insert(chunk_info.chunk_id, chunk);
//     }

//     let mut outer_decoder =
//         WirehairDecoder::new(object_size as _, (object_size / chunk_k) as _);
//     for (chunk_id, fragments) in chunks {
//         let mut inner_decoder = WirehairDecoder::new(
//             (object_size / chunk_k) as _,
//             (object_size / chunk_k / fragment_k) as _,
//         );
//         for (fragment_id, fragment) in fragments {
//             let res = inner_decoder.decode(fragment_id, &fragment).unwrap();
//             if res {
//                 break;
//             }
//         }

//         let mut chunk = vec![0; object_size / chunk_k];
//         inner_decoder.recover(&mut chunk).unwrap();

//         let res = outer_decoder.decode(chunk_id, &chunk).unwrap();
//         if res {
//             break;
//         };
//     }
//     let mut object = vec![0; object_size];
//     outer_decoder.recover(&mut object).unwrap();

//     // let mut sum = 0u32;
//     // for n in object.iter() {
//     //     sum = sum + *n as u32;
//     // }
//     //let body = once(ok::<_, Error>(bytes::Bytes::from(file)));
//     HttpResponse::Ok()
//         .content_type("text/markdown")
//         .body(object)
// }

#[derive(Serialize, Deserialize)]
struct Fragment {
    id: u32,
    #[serde(with = "serde_bytes")]
    data: Vec<u8>,
}

impl Peer {
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
            // .service(get)
        })
        .bind(addr)
        .expect("failed to bind address")
        .run()
        .await;
    }
}
