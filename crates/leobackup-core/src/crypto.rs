//! Criptografia autenticada de chunks com AES-256-GCM.
//!
//! Cada operação de `seal` usa um nonce aleatório de 96 bits. O nonce é
//! devolvido junto ao ciphertext para ser persistido no envelope do chunk.
//! A descriptografia só retorna dados após a verificação da tag de
//! autenticação; qualquer adulteração ou chave incorreta falha.

use aes_gcm::aead::rand_core::RngCore;
use aes_gcm::aead::{Aead, AeadCore, KeyInit, OsRng};
use aes_gcm::{Aes256Gcm, Nonce};

use crate::error::CoreError;

/// Tamanho da chave AES-256, em bytes.
pub const KEY_LEN: usize = 32;

/// Tamanho do nonce AES-GCM, em bytes (96 bits).
pub const NONCE_LEN: usize = 12;

/// Tamanho da tag de autenticação, em bytes.
pub const TAG_LEN: usize = 16;

/// Chave AES-256-GCM.
///
/// O `Debug` é redigido de propósito: a chave nunca deve aparecer em logs ou
/// mensagens de erro. A limpeza no `Drop` é best-effort.
#[derive(Clone)]
pub struct EncryptionKey([u8; KEY_LEN]);

impl EncryptionKey {
    /// Cria uma chave a partir de 32 bytes.
    pub const fn from_bytes(bytes: [u8; KEY_LEN]) -> Self {
        Self(bytes)
    }

    /// Gera uma chave aleatória usando o RNG do sistema.
    pub fn generate() -> Self {
        let mut bytes = [0u8; KEY_LEN];
        let mut rng = OsRng;
        rng.fill_bytes(&mut bytes);
        Self(bytes)
    }

    /// Bytes da chave. Não exponha em logs.
    pub const fn as_bytes(&self) -> &[u8; KEY_LEN] {
        &self.0
    }
}

impl std::fmt::Debug for EncryptionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("EncryptionKey([REDACTED])")
    }
}

impl Drop for EncryptionKey {
    fn drop(&mut self) {
        self.0.fill(0);
    }
}

/// Resultado da criptografia de um chunk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedData {
    /// Nonce usado na operação.
    pub nonce: [u8; NONCE_LEN],
    /// Ciphertext concatenado com a tag de autenticação.
    pub ciphertext: Vec<u8>,
}

/// Criptografa `plaintext` com AES-256-GCM e um nonce novo.
pub fn seal(key: &EncryptionKey, plaintext: &[u8]) -> Result<SealedData, CoreError> {
    let cipher = Aes256Gcm::new_from_slice(key.as_bytes())
        .map_err(|error| CoreError::Crypto(error.to_string()))?;

    let mut rng = OsRng;
    let nonce = Aes256Gcm::generate_nonce(&mut rng);
    let ciphertext = cipher
        .encrypt(&nonce, plaintext)
        .map_err(|error| CoreError::Crypto(error.to_string()))?;

    let mut nonce_bytes = [0u8; NONCE_LEN];
    nonce_bytes.copy_from_slice(&nonce[..]);

    Ok(SealedData {
        nonce: nonce_bytes,
        ciphertext,
    })
}

/// Descriptografa e autentica `ciphertext`.
///
/// Retorna erro se a chave estiver errada, os dados tiverem sido adulterados
/// ou o ciphertext estiver truncado.
pub fn open(
    key: &EncryptionKey,
    nonce: &[u8; NONCE_LEN],
    ciphertext: &[u8],
) -> Result<Vec<u8>, CoreError> {
    let cipher = Aes256Gcm::new_from_slice(key.as_bytes())
        .map_err(|error| CoreError::Crypto(error.to_string()))?;
    let nonce = Nonce::from_slice(nonce);

    cipher
        .decrypt(nonce, ciphertext)
        .map_err(|_| CoreError::Crypto("falha de autenticação ou dados corrompidos".to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> EncryptionKey {
        EncryptionKey::from_bytes([7u8; KEY_LEN])
    }

    #[test]
    fn debug_does_not_leak_key() {
        let rendered = format!("{:?}", key());
        assert!(rendered.contains("REDACTED"));
        assert!(!rendered.contains("07"));
    }

    #[test]
    fn roundtrip_preserves_content() {
        let sealed = seal(&key(), b"conteudo secreto").unwrap();
        let opened = open(&key(), &sealed.nonce, &sealed.ciphertext).unwrap();
        assert_eq!(opened, b"conteudo secreto");
    }

    #[test]
    fn nonce_is_unique_per_operation() {
        let first = seal(&key(), b"mesmo texto").unwrap();
        let second = seal(&key(), b"mesmo texto").unwrap();
        assert_ne!(first.nonce, second.nonce);
        assert_ne!(first.ciphertext, second.ciphertext);
    }

    #[test]
    fn wrong_key_fails() {
        let sealed = seal(&key(), b"conteudo").unwrap();
        let wrong = EncryptionKey::from_bytes([9u8; KEY_LEN]);
        assert!(open(&wrong, &sealed.nonce, &sealed.ciphertext).is_err());
    }

    #[test]
    fn tampered_ciphertext_fails() {
        let mut sealed = seal(&key(), b"conteudo").unwrap();
        sealed.ciphertext[0] ^= 0x01;
        assert!(open(&key(), &sealed.nonce, &sealed.ciphertext).is_err());
    }

    #[test]
    fn truncated_ciphertext_fails() {
        let sealed = seal(&key(), b"conteudo").unwrap();
        let truncated = &sealed.ciphertext[..sealed.ciphertext.len() - 1];
        assert!(open(&key(), &sealed.nonce, truncated).is_err());
    }

    #[test]
    fn generated_keys_differ() {
        assert_ne!(
            EncryptionKey::generate().as_bytes(),
            EncryptionKey::generate().as_bytes()
        );
    }
}
