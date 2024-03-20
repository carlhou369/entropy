#![allow(dead_code)]
use std::io;

use rand::{rngs::OsRng, RngCore};
use schnorrkel::{
    context::SigningContext,
    points::RistrettoBoth,
    signing_context,
    vrf::{VRFInOut, VRFProof},
    ExpansionMode, MiniSecretKey, PublicKey, SignatureError,
};
use thiserror::Error;

pub type VrfHash = [u8; 32];

pub type VrfProof = [u8; 64];

#[derive(Error, Debug)]
pub enum VrfError {
    #[error("sigature error")]
    SignatureError(SignatureError),
    #[error("io error")]
    IOError(#[from] io::Error),
}

#[derive(Clone, Debug)]
pub struct VrfPair(schnorrkel::Keypair);

pub struct VrfPublickey(schnorrkel::PublicKey);

fn context() -> SigningContext {
    signing_context(b"vrf")
}

impl VrfPair {
    pub fn new() -> Self {
        let mut rng = OsRng;
        let mut seed = [0u8; 32];
        rng.fill_bytes(&mut seed);
        Self::generate_with_seed(&seed).unwrap()
    }

    pub fn generate_with_seed(seed: &[u8; 32]) -> Result<Self, VrfError> {
        let mini_secret = MiniSecretKey::from_bytes(seed).map_err(VrfError::SignatureError)?;
        Ok(VrfPair(
            mini_secret.expand_to_keypair(ExpansionMode::Ed25519),
        ))
    }

    pub fn vrf_sign(&self, input: &[u8]) -> Result<(VrfHash, VrfProof), VrfError> {
        let (in_out, proof, _) = self
            .0
            .secret
            .clone()
            .to_keypair()
            .vrf_sign(context().bytes(input));
        Ok((in_out.output.to_bytes(), proof.to_bytes()))
    }

    pub fn vrf_verify(
        &self,
        input: &[u8],
        vrf_hash: VrfHash,
        proof: VrfProof,
    ) -> Result<(), VrfError> {
        self.get_public().verify_vrf(input, vrf_hash, proof)
    }

    pub fn get_public(&self) -> VrfPublickey {
        VrfPublickey(self.0.public)
    }
}

impl VrfPublickey {
    pub fn verify_vrf(
        &self,
        input: &[u8],
        vrf_hash: VrfHash,
        proof: VrfProof,
    ) -> Result<(), VrfError> {
        let output =
            RistrettoBoth::from_bytes(vrf_hash.as_slice()).map_err(VrfError::SignatureError)?;
        let proof = VRFProof::from_bytes(&proof).map_err(VrfError::SignatureError)?;
        let in_out = VRFInOut {
            input: self.0.vrf_hash(context().bytes(input)),
            output,
        };
        self.0
            .vrf_verify(context().bytes(input), &in_out.to_preout(), &proof)
            .map_err(VrfError::SignatureError)?;
        Ok(())
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, VrfError> {
        Ok(VrfPublickey(
            PublicKey::from_bytes(bytes).map_err(VrfError::SignatureError)?,
        ))
    }

    pub fn to_bytes(&self) -> [u8; 32] {
        self.0.to_bytes()
    }
}

#[cfg(test)]
mod tests {
    use crate::vrf::VrfPair;

    #[test]
    fn vrf_sign_verify() {
        let secret = VrfPair::new();
        let msg = b"hello world";
        let (hash, proof) = secret.vrf_sign(msg).expect("should vrf hash msg");
        secret
            .vrf_verify(msg, hash, proof)
            .expect("should pass verify");
    }
}
