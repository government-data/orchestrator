//! Secret values sealed at rest (decision 0011: flow variables and
//! secrets): AES-256-GCM with the module's own key, a file the agent
//! generates at `module add` (quality-sdk decision 0009, `secrets.generate`,
//! 32 bytes, base64url without padding). A sealed value is the 12-byte
//! nonce followed by the ciphertext and its tag. The key is never logged.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM, NONCE_LEN};
use ring::rand::{SecureRandom, SystemRandom};

/// The sealing key.
pub struct Sealer {
    key: LessSafeKey,
    random: SystemRandom,
}

impl std::fmt::Debug for Sealer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Sealer(..)")
    }
}

/// A sealed value that does not open with this key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotOpened;

impl Sealer {
    /// The key from a secret file's content: 32 bytes, base64url.
    pub fn from_file_content(content: &str) -> Option<Self> {
        let bytes = URL_SAFE_NO_PAD.decode(content.trim()).ok()?;
        let key = UnboundKey::new(&AES_256_GCM, &bytes).ok()?;
        Some(Self {
            key: LessSafeKey::new(key),
            random: SystemRandom::new(),
        })
    }

    /// Seals `plain` under a new random nonce.
    pub fn seal(&self, plain: &[u8]) -> Result<Vec<u8>, NotOpened> {
        let mut nonce = [0u8; NONCE_LEN];
        self.random.fill(&mut nonce).map_err(|_| NotOpened)?;
        let mut data = plain.to_vec();
        self.key
            .seal_in_place_append_tag(Nonce::assume_unique_for_key(nonce), Aad::empty(), &mut data)
            .map_err(|_| NotOpened)?;
        let mut out = nonce.to_vec();
        out.extend(data);
        Ok(out)
    }

    /// Opens a sealed value.
    pub fn open(&self, sealed: &[u8]) -> Result<Vec<u8>, NotOpened> {
        if sealed.len() < NONCE_LEN {
            return Err(NotOpened);
        }
        let (nonce, data) = sealed.split_at(NONCE_LEN);
        let nonce = Nonce::try_assume_unique_for_key(nonce).map_err(|_| NotOpened)?;
        let mut data = data.to_vec();
        let plain = self
            .key
            .open_in_place(nonce, Aad::empty(), &mut data)
            .map_err(|_| NotOpened)?;
        Ok(plain.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &str = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8";

    #[test]
    fn a_value_sealed_opens_with_its_key_only() {
        let s = Sealer::from_file_content(KEY).unwrap();
        let sealed = s.seal(b"\"secret\"").unwrap();
        assert_ne!(
            s.seal(b"\"secret\"").unwrap(),
            sealed,
            "a new nonce each time"
        );
        assert_eq!(s.open(&sealed).unwrap(), b"\"secret\"");
        let other =
            Sealer::from_file_content("Hx4dHBsaGRgXFhUUExIREA8ODQwLCgkIBwYFBAMCAQA").unwrap();
        assert_eq!(other.open(&sealed), Err(NotOpened));
        let mut cut = sealed.clone();
        cut.pop();
        assert_eq!(s.open(&cut), Err(NotOpened));
        assert!(Sealer::from_file_content("short").is_none());
        assert_eq!(format!("{s:?}"), "Sealer(..)");
    }
}
