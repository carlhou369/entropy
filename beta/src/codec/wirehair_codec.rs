use std::collections::HashMap;

use rand::random;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use wirehair::{WirehairDecoder, WirehairEncoder};

#[derive(Debug, Clone, Copy)]
pub struct WirehairCodecOptions {
    chunk_k: usize,
    chunk_n: usize,
    fragment_k: usize,
    fragment_n: usize,
}

impl Default for WirehairCodecOptions {
    fn default() -> WirehairCodecOptions {
        WirehairCodecOptions {
            chunk_k: 8,
            chunk_n: 10,
            fragment_k: 32,
            fragment_n: 80,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct WirehairCodec {
    options: WirehairCodecOptions,
}

impl WirehairCodec {
    pub fn new() -> WirehairCodec {
        WirehairCodec {
            options: WirehairCodecOptions::default(),
        }
    }

    pub fn new_with_options(options: WirehairCodecOptions) -> WirehairCodec {
        WirehairCodec { options }
    }

    pub fn encode(&self, data: Vec<u8>) -> HashMap<u32, HashMap<u32, Vec<u8>>> {
        let mut links = HashMap::new();
        for (k, v) in self.encode_into_chunks(data) {
            let fragments = self.encode_into_fragments(v);
            links.insert(k, fragments);
        }

        links
    }

    // Todo: check if it's necessary to refactor.
    pub fn encode_parallelly(
        &self,
        data: Vec<u8>,
    ) -> HashMap<u32, HashMap<u32, Vec<u8>>> {
        self.encode_into_chunks(data)
            .into_par_iter()
            .map(|(chunk_id, chunk)| {
                let fragments = self.encode_into_fragments(chunk);
                (chunk_id, fragments)
            })
            .collect()
    }

    fn encode_internal(
        &self,
        data: Vec<u8>,
        k: usize,
        n: usize,
    ) -> HashMap<u32, Vec<u8>> {
        let data_size = data.len();
        let chunk_size = data_size / k;
        let chunk_encoder = WirehairEncoder::new(data, chunk_size as u32);
        let mut chunks = HashMap::new();

        for _ in 0..n {
            let chunk_id = random();
            let mut chunk = vec![0; chunk_size];
            // Todo: return err and run in multi-threads?
            chunk_encoder.encode(chunk_id, &mut chunk).unwrap();
            chunks.insert(chunk_id, chunk);
        }

        chunks
    }

    fn encode_into_chunks(&self, data: Vec<u8>) -> HashMap<u32, Vec<u8>> {
        self.encode_internal(data, self.options.chunk_k, self.options.chunk_n)
    }

    fn encode_into_fragments(&self, data: Vec<u8>) -> HashMap<u32, Vec<u8>> {
        self.encode_internal(
            data,
            self.options.fragment_k,
            self.options.fragment_n,
        )
    }

    pub fn decode(
        &self,
        links: HashMap<u32, HashMap<u32, Vec<u8>>>,
        msg_size: usize,
    ) -> Vec<u8> {
        let mut chunks = HashMap::new();
        for (chunk_id, fragments) in links {
            let msg_size = msg_size / self.options.chunk_k;
            let chunk = self.decode_into_fragments(
                fragments,
                msg_size,
                msg_size / self.options.fragment_k,
            );
            chunks.insert(chunk_id, chunk);
        }

        self.decode_into_chunks(
            chunks,
            msg_size,
            msg_size / self.options.chunk_k,
        )
    }

    fn decode_into_chunks(
        &self,
        fragments: HashMap<u32, Vec<u8>>,
        msg_size: usize,
        block_size: usize,
    ) -> Vec<u8> {
        self.decode_internal(fragments, msg_size, block_size)
    }

    fn decode_into_fragments(
        &self,
        chunks: HashMap<u32, Vec<u8>>,
        msg_size: usize,
        block_size: usize,
    ) -> Vec<u8> {
        self.decode_internal(chunks, msg_size, block_size)
    }

    fn decode_internal(
        &self,
        chunks: HashMap<u32, Vec<u8>>,
        msg_size: usize,
        block_size: usize,
    ) -> Vec<u8> {
        let mut chunk_decoder =
            WirehairDecoder::new(msg_size as u64, block_size as u32);
        for (chunk_id, chunk) in chunks {
            // Todo: check parameters for wirehair.
            if chunk_decoder.decode(chunk_id, &chunk).unwrap() {
                break;
            }
        }
        let mut chunk = vec![0; msg_size];
        // Todo: return err if necessiary.
        chunk_decoder.recover(&mut chunk).unwrap();
        chunk
    }
}

#[cfg(test)]
mod tests {
    use rand::RngCore;

    use crate::codec::wirehair_codec::WirehairCodec;

    const TEST_LARGE_FILE_SIZE: usize = 1 << 20;

    #[test]
    fn encode_large_file() {
        let mut data = vec![0; TEST_LARGE_FILE_SIZE];
        rand::thread_rng().fill_bytes(&mut data);

        let codec = WirehairCodec::new();
        let links = codec.encode(data.clone());
        let recovered = codec.decode(links, data.len());

        assert_eq!(data, recovered);
    }
}
