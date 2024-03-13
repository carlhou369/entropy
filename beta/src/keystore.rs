#![allow(dead_code)]
use std::{
    io::{Read, Write},
    path::PathBuf,
};

use anyhow::{anyhow, Ok, Result};
use ed25519_dalek::ed25519::signature::SignerMut;
use merlin::Transcript;
use rand::{rngs::OsRng, RngCore};
use std::fs::{File, OpenOptions};

use crate::vrf::{VrfHash, VrfPair, VrfProof, VrfPublickey};

pub struct KeyStore {
    pub seed: [u8; 32],
    vrf_pair: Option<VrfPair>,
    ed25519_secret: Option<ed25519_dalek::SigningKey>,
    path: Option<PathBuf>,
}

impl KeyStore {
    // todo: save encrypted seed
    pub fn save_to(&self, path: PathBuf) -> Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        Ok(file.write_all(&self.seed)?)
    }

    pub fn generate_from_file(path: PathBuf) -> Result<Self> {
        let mut file = File::open(path)?;
        let mut seed = [0u8; 32];
        file.read_exact(&mut seed)?;
        Self::generate_with_seed(&seed)
    }

    pub fn generate() -> Self {
        let mut seed: [u8; 32] = [0u8; 32];
        let mut rng = OsRng;
        rng.fill_bytes(&mut seed);

        Self::generate_with_seed(&seed).unwrap()
    }

    pub fn generate_with_seed(seed: &[u8; 32]) -> Result<Self> {
        let mut ctx = Transcript::new(b"generate");
        ctx.append_message(b"raw", seed);
        let mut ed25519_seed = [0u8; 32];
        ctx.challenge_bytes(b"ed25519", &mut ed25519_seed);
        let ed25519_secret = ed25519_dalek::SigningKey::from_bytes(&ed25519_seed);

        let mut vrf_seed = [0u8; 32];
        ctx.challenge_bytes(b"vrf", &mut vrf_seed);
        let vrf_pair = VrfPair::generate_with_seed(&vrf_seed)?;

        Ok(KeyStore {
            seed: seed.to_owned(),
            vrf_pair: Some(vrf_pair),
            ed25519_secret: Some(ed25519_secret),
            path: None,
        })
    }

    pub fn vrf_sign(&self, input: &[u8]) -> Result<(VrfHash, VrfProof)> {
        self.vrf_pair
            .clone()
            .ok_or(anyhow!("vrf keypair not exist"))
            .and_then(|pair| pair.vrf_sign(input))
    }

    pub fn vrf_public(&self) -> Option<VrfPublickey> {
        Some(self.vrf_pair.clone()?.get_public())
    }

    pub fn ed25519_public(&self) -> Option<ed25519_dalek::VerifyingKey> {
        Some(self.ed25519_secret.clone()?.verifying_key())
    }

    pub fn ed25519_sign(&self, msg: &[u8]) -> Result<ed25519_dalek::Signature> {
        self.ed25519_secret
            .clone()
            .ok_or(anyhow!("ed25519 keypair not exist"))
            .and_then(|key| key.clone().try_sign(msg).map_err(|e| anyhow!("{}", e)))
    }
}

#[cfg(test)]
mod tests {
    

    use crate::keystore::*;
    #[test]
    fn keystore_vrf_sign_verify() {
        let keystore = KeyStore::generate();
        let msg = b"hello mars";
        let msg_fraud = b"hello moon";

        let (vrf_hash, vrf_proof) = keystore.vrf_sign(msg).unwrap();
        let pubkey = keystore.vrf_public().unwrap();
        pubkey
            .verify_vrf(msg, vrf_hash, vrf_proof)
            .expect("should pass verify");

        pubkey
            .verify_vrf(msg_fraud, vrf_hash, vrf_proof)
            .expect_err("should not pass verfiy");
    }
    #[test]
    fn keystore_ed25519_sign_verigy() {
        let keystore = KeyStore::generate();
        let msg = b"hello mars";
        let msg_fraud = b"hello moon";
        let sig = keystore.ed25519_sign(msg).unwrap();
        let pubkey = keystore.ed25519_public().unwrap();
        pubkey.verify_strict(msg, &sig).expect("should pass verify");
        pubkey
            .verify_strict(msg_fraud, &sig)
            .expect_err("should not pass verify");
    }
}
