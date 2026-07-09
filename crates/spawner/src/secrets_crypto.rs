//! # Encryption-at-rest for `exchange_secrets`
//!
//! The WebUI submits exchange API credentials to the spawner, which stores
//! them in Postgres (`exchange_secrets`). Historically they were plaintext at
//! rest (internal / Tailscale-only, token-gated); this module closes that gap
//! so live keys can eventually be stored safely (crypto→FKS integration plan,
//! open question 4).
//!
//! ## Key
//!
//! `SPAWNER_SECRETS_KEY` — 64 hex chars (32 bytes) for ChaCha20-Poly1305.
//! Generate with `openssl rand -hex 32`.
//!
//! - **Unset/empty** → plaintext mode (backwards compatible, warned loudly).
//! - **Invalid** → `from_env` errors; the caller refuses to enable the DB
//!   (fail-safe: never silently store plaintext when the operator configured
//!   a key, however badly).
//!
//! ## Wire format
//!
//! `enc:v1:<base64(nonce ‖ ciphertext+tag)>` — random 12-byte nonce per
//! encryption. Values without the `enc:v1:` prefix are treated as legacy
//! plaintext rows and passed through on decrypt, so pre-existing rows keep
//! working; they are re-encrypted the next time the operator re-submits.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use chacha20poly1305::aead::{Aead, AeadCore, KeyInit, OsRng};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use tracing::warn;

const PREFIX: &str = "enc:v1:";
const NONCE_LEN: usize = 12;

/// At-rest cipher for exchange credentials. `Plaintext` preserves the
/// pre-key behaviour; `V1` is ChaCha20-Poly1305 with the env-provided key.
#[derive(Clone)]
pub enum SecretsCipher {
    Plaintext,
    V1(ChaCha20Poly1305),
}

impl SecretsCipher {
    /// Build from `SPAWNER_SECRETS_KEY`. Unset/empty → `Plaintext` (with a
    /// warning); a present-but-invalid key is an error so the caller can
    /// refuse to run with a half-configured secret store.
    pub fn from_env() -> Result<Self, String> {
        match std::env::var("SPAWNER_SECRETS_KEY") {
            Err(_) => {
                warn!(
                    "SPAWNER_SECRETS_KEY not set — exchange_secrets are stored PLAINTEXT at \
                     rest. Set a 64-hex-char key (openssl rand -hex 32) to encrypt."
                );
                Ok(Self::Plaintext)
            }
            Ok(k) if k.trim().is_empty() => {
                warn!("SPAWNER_SECRETS_KEY empty — exchange_secrets are stored PLAINTEXT at rest.");
                Ok(Self::Plaintext)
            }
            Ok(k) => {
                let bytes = hex::decode(k.trim())
                    .map_err(|e| format!("SPAWNER_SECRETS_KEY is not valid hex: {e}"))?;
                if bytes.len() != 32 {
                    return Err(format!(
                        "SPAWNER_SECRETS_KEY must be 32 bytes (64 hex chars), got {}",
                        bytes.len()
                    ));
                }
                Ok(Self::V1(ChaCha20Poly1305::new(Key::from_slice(&bytes))))
            }
        }
    }

    /// Whether values will actually be encrypted at rest.
    pub fn is_encrypting(&self) -> bool {
        matches!(self, Self::V1(_))
    }

    /// Encrypt one credential value for storage. Plaintext mode returns the
    /// value unchanged.
    pub fn encrypt(&self, plaintext: &str) -> Result<String, String> {
        match self {
            Self::Plaintext => Ok(plaintext.to_string()),
            Self::V1(cipher) => {
                let nonce = ChaCha20Poly1305::generate_nonce(&mut OsRng);
                let ct = cipher
                    .encrypt(&nonce, plaintext.as_bytes())
                    .map_err(|_| "encryption failed".to_string())?;
                let mut blob = Vec::with_capacity(NONCE_LEN + ct.len());
                blob.extend_from_slice(&nonce);
                blob.extend_from_slice(&ct);
                Ok(format!("{PREFIX}{}", B64.encode(blob)))
            }
        }
    }

    /// Decrypt one stored value. Values without the `enc:v1:` prefix are
    /// legacy plaintext rows and pass through unchanged. An encrypted value
    /// without a configured key (or with the wrong key) is an error — never
    /// return ciphertext as if it were a credential.
    pub fn decrypt(&self, stored: &str) -> Result<String, String> {
        let Some(b64) = stored.strip_prefix(PREFIX) else {
            return Ok(stored.to_string()); // legacy plaintext row
        };
        let Self::V1(cipher) = self else {
            return Err("value is encrypted but SPAWNER_SECRETS_KEY is not configured".to_string());
        };
        let blob = B64
            .decode(b64)
            .map_err(|e| format!("corrupt encrypted value (base64): {e}"))?;
        if blob.len() <= NONCE_LEN {
            return Err("corrupt encrypted value (too short)".to_string());
        }
        let (nonce, ct) = blob.split_at(NONCE_LEN);
        let pt = cipher
            .decrypt(Nonce::from_slice(nonce), ct)
            .map_err(|_| "decryption failed (wrong key or tampered value)".to_string())?;
        String::from_utf8(pt).map_err(|_| "decrypted value is not UTF-8".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v1() -> SecretsCipher {
        SecretsCipher::V1(ChaCha20Poly1305::new(Key::from_slice(&[7u8; 32])))
    }

    #[test]
    fn round_trip() {
        let c = v1();
        let stored = c.encrypt("super-secret-api-key").unwrap();
        assert!(stored.starts_with("enc:v1:"));
        assert_ne!(stored, "super-secret-api-key");
        assert_eq!(c.decrypt(&stored).unwrap(), "super-secret-api-key");
    }

    #[test]
    fn nonces_are_random() {
        let c = v1();
        assert_ne!(c.encrypt("x").unwrap(), c.encrypt("x").unwrap());
    }

    #[test]
    fn legacy_plaintext_passes_through() {
        assert_eq!(
            v1().decrypt("old-plaintext-key").unwrap(),
            "old-plaintext-key"
        );
        assert_eq!(
            SecretsCipher::Plaintext
                .decrypt("old-plaintext-key")
                .unwrap(),
            "old-plaintext-key"
        );
    }

    #[test]
    fn plaintext_mode_is_identity() {
        let c = SecretsCipher::Plaintext;
        assert_eq!(c.encrypt("k").unwrap(), "k");
        assert!(!c.is_encrypting());
    }

    #[test]
    fn encrypted_value_without_key_errors() {
        let stored = v1().encrypt("k").unwrap();
        assert!(SecretsCipher::Plaintext.decrypt(&stored).is_err());
    }

    #[test]
    fn wrong_key_fails() {
        let stored = v1().encrypt("k").unwrap();
        let other = SecretsCipher::V1(ChaCha20Poly1305::new(Key::from_slice(&[8u8; 32])));
        assert!(other.decrypt(&stored).is_err());
    }

    #[test]
    fn tampered_value_fails() {
        let c = v1();
        let stored = c.encrypt("k").unwrap();
        let mut b = stored.into_bytes();
        let last = b.len() - 1;
        b[last] = if b[last] == b'A' { b'B' } else { b'A' };
        assert!(c.decrypt(&String::from_utf8(b).unwrap()).is_err());
    }
}
