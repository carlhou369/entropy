#[derive(Debug, Clone, Copy)]
pub struct WirehairCodecOptions {
    outer_k: u32,
    outer_n: u32,
    inner_k: u32,
    inner_n: u32,
}

impl Default for WirehairCodecOptions {
    fn default() -> WirehairCodecOptions {
        WirehairCodecOptions {
            outer_k: 8,
            outer_n: 10,
            inner_k: 32,
            inner_n: 80,
        }
    }
}

#[warn(dead_code)]
#[derive(Debug, Clone)]
pub struct WirehairCodec {
    data: Vec<u8>,
    option: WirehairCodecOptions,
}

impl WirehairCodec {
    pub fn new(option: WirehairCodecOptions) -> WirehairCodec {
        WirehairCodec {
            data: vec![],
            option,
        }
    }
}
