use std::collections::HashMap;

use {rand::random, wirehair::WirehairEncoder};

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

#[derive(Debug, Clone)]
pub struct WirehairCodec {
    option: WirehairCodecOptions,
}

impl WirehairCodec {
    pub fn new(option: WirehairCodecOptions) -> WirehairCodec {
        WirehairCodec { option }
    }

    pub fn encode(&self, data: Vec<u8>) -> HashMap<u32, HashMap<u32, Vec<u8>>> {
        let mut links = HashMap::new();
        for (k, v) in self.encode_into_chunks(data) {
            let fragments = self.encode_into_fragments(v);
            links.insert(k, fragments);
        }

        links
    }

    fn encode_internal(&self, data: Vec<u8>, k: usize, n: usize) -> HashMap<u32, Vec<u8>> {
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
        self.encode_internal(data, self.option.chunk_k, self.option.chunk_n)
    }

    fn encode_into_fragments(&self, data: Vec<u8>) -> HashMap<u32, Vec<u8>> {
        self.encode_internal(data, self.option.fragment_k, self.option.fragment_n)
    }
}

#[cfg(test)]
mod tests {
    use rand::RngCore;

    use crate::codec::wirehair_codec::{WirehairCodec, WirehairCodecOptions};

    const TEST_LARGE_FILE_SIZE: usize = 1 << 30;
    #[test]
    fn encode_large_file() {
        let mut data = vec![0; TEST_LARGE_FILE_SIZE];
        rand::thread_rng().fill_bytes(&mut data);
        let links = WirehairCodec::new(WirehairCodecOptions::default()).encode(data);
        let _ = links;
    }
}
