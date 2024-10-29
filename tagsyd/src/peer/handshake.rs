use std::io;
use std::path::Path;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use rand::Rng;
use serde::{Deserialize, Serialize};

/// The handshake each peer sends to prove it controls the private key behind
/// its advertised public key. The signature is made over the *other* peer's
/// public key, so it cannot be replayed against a third party.
///
/// `protocol_version` is advisory metadata verified *after* the signature (it
/// is not covered by the signature, which pins only the public key). It gates
/// the wire protocol: a peer whose version differs from ours is rejected with
/// [`HandshakeError::IncompatibleProtocol`]. Adding this field also changes the
/// handshake wire shape, so an old peer (which never sent it) fails to
/// deserialize a new peer's handshake at all — the desired fail-closed
/// behavior for the very first version gate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandshakeMessage {
    pub public_key: String,
    pub signature: String,
    pub protocol_version: u32,
}

/// Anything that can go wrong while building or verifying a handshake. Kept
/// as data (no panics) so the networking code can log and reject malformed
/// input instead of crashing the task.
#[derive(Debug, thiserror::Error)]
pub enum HandshakeError {
    #[error("public key is not valid base64: {0}")]
    InvalidPublicKeyEncoding(#[source] base64::DecodeError),
    #[error("signature is not valid base64: {0}")]
    InvalidSignatureEncoding(#[source] base64::DecodeError),
    #[error("public key has wrong length: expected {expected} bytes, found {found}")]
    WrongPublicKeyLength { expected: usize, found: usize },
    #[error("signature has wrong length: expected {expected} bytes, found {found}")]
    WrongSignatureLength { expected: usize, found: usize },
    #[error("public key is not a valid ed25519 key: {0}")]
    InvalidPublicKey(#[source] ed25519_dalek::SignatureError),
    #[error("signature verification failed")]
    SignatureVerificationFailed,
    /// The peer advertised a wire-protocol version different from ours. Since
    /// all devices are updated together there is no compatibility range; a
    /// mismatch is fail-closed.
    #[error("incompatible protocol version: ours is {ours}, peer's is {theirs}")]
    IncompatibleProtocol { ours: u32, theirs: u32 },
}

/// This machine's long-lived cryptographic identity: an ed25519 keypair whose
/// public half is shared with peers and whose private half never leaves disk.
pub struct Identity {
    signing_key: SigningKey,
}

impl Identity {
    /// Create a fresh random identity. Does not touch disk; call [`save`] to
    /// persist it.
    ///
    /// [`save`]: Identity::save
    pub fn generate() -> Self {
        let mut seed = [0u8; 32];
        rand::rng().fill_bytes(&mut seed);
        Identity {
            signing_key: SigningKey::from_bytes(&seed),
        }
    }

    /// Load an identity previously written by [`save`]. The file holds the
    /// base64-encoded 32-byte ed25519 seed.
    ///
    /// [`save`]: Identity::save
    pub fn load(path: &Path) -> io::Result<Self> {
        let contents = std::fs::read_to_string(path)?;
        let bytes = BASE64.decode(contents.trim()).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "identity key at {} is not valid base64: {error}",
                    path.display()
                ),
            )
        })?;
        let seed: [u8; 32] = bytes.as_slice().try_into().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "identity key at {} decoded to {} bytes, expected 32",
                    path.display(),
                    bytes.len()
                ),
            )
        })?;
        Ok(Identity {
            signing_key: SigningKey::from_bytes(&seed),
        })
    }

    /// Persist this identity to `path` as the base64-encoded 32-byte ed25519
    /// seed. Refuses to overwrite an existing file so an accidental keygen
    /// can't silently rotate (and thus invalidate) the machine's identity.
    pub fn save(&self, path: &Path) -> io::Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)?;
        use std::io::Write;
        file.write_all(BASE64.encode(self.signing_key.to_bytes()).as_bytes())
    }

    /// The base64-encoded public key advertised to peers and stored in config.
    pub fn public_key(&self) -> String {
        BASE64.encode(self.signing_key.verifying_key().to_bytes())
    }

    /// Build our half of the handshake, proving ownership of our private key
    /// by signing the peer's public key.
    pub fn sign_handshake(
        &self,
        peer_public_key: &str,
    ) -> Result<HandshakeMessage, HandshakeError> {
        let peer_public_key_bytes = BASE64
            .decode(peer_public_key)
            .map_err(HandshakeError::InvalidPublicKeyEncoding)?;
        let signature = self.signing_key.sign(&peer_public_key_bytes);
        Ok(HandshakeMessage {
            public_key: self.public_key(),
            signature: BASE64.encode(signature.to_bytes()),
            protocol_version: tagsy_core::PROTOCOL_VERSION,
        })
    }

    /// Verify a handshake received from a peer. Confirms the peer signed *our*
    /// public key with the private key matching their advertised public key,
    /// and that its advertised `protocol_version` matches ours exactly.
    ///
    /// The version is checked *after* signature verification: it is advisory
    /// metadata, not part of the signed payload, so it never weakens the auth
    /// proof. On success returns the peer's verified public key (base64). Never
    /// panics on malformed input; every failure mode is a [`HandshakeError`].
    pub fn verify_handshake(&self, message: &HandshakeMessage) -> Result<String, HandshakeError> {
        let peer_public_key_bytes = BASE64
            .decode(&message.public_key)
            .map_err(HandshakeError::InvalidPublicKeyEncoding)?;
        let peer_public_key_array: [u8; 32] =
            peer_public_key_bytes.as_slice().try_into().map_err(|_| {
                HandshakeError::WrongPublicKeyLength {
                    expected: 32,
                    found: peer_public_key_bytes.len(),
                }
            })?;
        let peer_verifying_key = VerifyingKey::from_bytes(&peer_public_key_array)
            .map_err(HandshakeError::InvalidPublicKey)?;

        let signature_bytes = BASE64
            .decode(&message.signature)
            .map_err(HandshakeError::InvalidSignatureEncoding)?;
        let signature_array: [u8; 64] = signature_bytes.as_slice().try_into().map_err(|_| {
            HandshakeError::WrongSignatureLength {
                expected: 64,
                found: signature_bytes.len(),
            }
        })?;
        let signature = Signature::from_bytes(&signature_array);

        let our_public_key_bytes = self.signing_key.verifying_key().to_bytes();
        peer_verifying_key
            .verify(&our_public_key_bytes, &signature)
            .map_err(|_| HandshakeError::SignatureVerificationFailed)?;

        // Version gate, after the auth proof: require exact equality.
        if message.protocol_version != tagsy_core::PROTOCOL_VERSION {
            return Err(HandshakeError::IncompatibleProtocol {
                ours: tagsy_core::PROTOCOL_VERSION,
                theirs: message.protocol_version,
            });
        }

        Ok(message.public_key.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A handshake Alice builds for Bob verifies on Bob's side and returns
    /// Alice's public key. The signature is made over the *verifier's* public
    /// key, so only Bob can accept this exact message.
    #[test]
    fn sign_verify_round_trip() {
        let alice = Identity::generate();
        let bob = Identity::generate();

        let message = alice.sign_handshake(&bob.public_key()).unwrap();
        assert_eq!(message.public_key, alice.public_key());

        let verified = bob.verify_handshake(&message).unwrap();
        assert_eq!(verified, alice.public_key());
    }

    /// A handshake Alice signed for Carol must not verify on Bob's side: the
    /// signature is over Carol's public key, not Bob's, so verification fails.
    /// This is the replay guard — a handshake captured on one link cannot be
    /// presented to a third party.
    #[test]
    fn wrong_peer_key_rejected() {
        let alice = Identity::generate();
        let bob = Identity::generate();
        let carol = Identity::generate();

        // Alice signs Carol's key, then tries to present it to Bob.
        let message = alice.sign_handshake(&carol.public_key()).unwrap();
        assert!(matches!(
            bob.verify_handshake(&message),
            Err(HandshakeError::SignatureVerificationFailed)
        ));
    }

    /// A public key that is not valid base64 is rejected before any crypto.
    #[test]
    fn malformed_public_key_base64_rejected() {
        let alice = Identity::generate();
        let bob = Identity::generate();

        let mut message = alice.sign_handshake(&bob.public_key()).unwrap();
        message.public_key = "not valid base64!!!".to_owned();
        assert!(matches!(
            bob.verify_handshake(&message),
            Err(HandshakeError::InvalidPublicKeyEncoding(_))
        ));
    }

    /// A signature that is not valid base64 is rejected (after the public key
    /// decodes, before signature verification).
    #[test]
    fn malformed_signature_base64_rejected() {
        let alice = Identity::generate();
        let bob = Identity::generate();

        let mut message = alice.sign_handshake(&bob.public_key()).unwrap();
        message.signature = "@@@not base64@@@".to_owned();
        assert!(matches!(
            bob.verify_handshake(&message),
            Err(HandshakeError::InvalidSignatureEncoding(_))
        ));
    }

    /// A well-formed base64 public key of the wrong length (not 32 bytes) is
    /// rejected as a wrong-length key rather than being fed to the crypto.
    #[test]
    fn wrong_public_key_length_rejected() {
        let alice = Identity::generate();
        let bob = Identity::generate();

        let mut message = alice.sign_handshake(&bob.public_key()).unwrap();
        // 16 bytes: valid base64, wrong length for an ed25519 key.
        message.public_key = BASE64.encode([0u8; 16]);
        match bob.verify_handshake(&message) {
            Err(HandshakeError::WrongPublicKeyLength { expected, found }) => {
                assert_eq!(expected, 32);
                assert_eq!(found, 16);
            }
            other => panic!("expected WrongPublicKeyLength, got {other:?}"),
        }
    }

    /// A well-formed base64 signature of the wrong length (not 64 bytes) is
    /// rejected as a wrong-length signature.
    #[test]
    fn wrong_signature_length_rejected() {
        let alice = Identity::generate();
        let bob = Identity::generate();

        let mut message = alice.sign_handshake(&bob.public_key()).unwrap();
        // 10 bytes: valid base64, wrong length for an ed25519 signature.
        message.signature = BASE64.encode([0u8; 10]);
        match bob.verify_handshake(&message) {
            Err(HandshakeError::WrongSignatureLength { expected, found }) => {
                assert_eq!(expected, 64);
                assert_eq!(found, 10);
            }
            other => panic!("expected WrongSignatureLength, got {other:?}"),
        }
    }

    /// A valid signature but a mismatched protocol version is rejected by the
    /// version gate, which reports both sides' versions.
    #[test]
    fn version_mismatch_rejected() {
        let alice = Identity::generate();
        let bob = Identity::generate();

        let mut message = alice.sign_handshake(&bob.public_key()).unwrap();
        message.protocol_version = tagsy_core::PROTOCOL_VERSION + 1;
        match bob.verify_handshake(&message) {
            Err(HandshakeError::IncompatibleProtocol { ours, theirs }) => {
                assert_eq!(ours, tagsy_core::PROTOCOL_VERSION);
                assert_eq!(theirs, tagsy_core::PROTOCOL_VERSION + 1);
            }
            other => panic!("expected IncompatibleProtocol, got {other:?}"),
        }
    }

    /// **Ordering property**: signature verification happens *before* the
    /// protocol-version gate. A message that is both wrongly-signed *and*
    /// carries a bad version must fail as `SignatureVerificationFailed`, never
    /// as `IncompatibleProtocol` — otherwise an attacker could probe the
    /// version gate without a valid signature. This is a security invariant the
    /// implementation encodes only by statement order, so it is asserted here.
    #[test]
    fn signature_verified_before_version_gate() {
        let alice = Identity::generate();
        let bob = Identity::generate();
        let carol = Identity::generate();

        // Wrong signature (signed Carol's key, presented to Bob) AND a bad
        // version. The signature failure must win.
        let mut message = alice.sign_handshake(&carol.public_key()).unwrap();
        message.protocol_version = tagsy_core::PROTOCOL_VERSION + 1;
        assert!(matches!(
            bob.verify_handshake(&message),
            Err(HandshakeError::SignatureVerificationFailed)
        ));
    }
}
