use crate::codec::wirehair_codec::WirehairCodec;

pub struct Eld {}

impl Eld {
    pub fn from_data(data: Vec<u8>) -> Eld {
        let cdc = WirehairCodec::new();
        let _ = cdc.encode_parallelly(data);
        Eld {}
    }
}
