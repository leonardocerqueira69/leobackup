//! Envelope persistido de um chunk.
//!
//! Formato versão 1 (todos os campos em ordem, sem padding):
//!
//! ```text
//! offset  tamanho  campo
//! 0       4        magic "LBK1"
//! 4       1        flags (bit 0 = criptografado, bit 1 = comprimido)
//! 5       12       nonce AES-GCM (zeros quando não criptografado)
//! 17      N        payload (ciphertext+tag ou bytes comprimidos)
//! ```
//!
//! Mudanças no formato exigem nova versão de magic e estratégia de
//! compatibilidade.

use crate::compression;
use crate::crypto::{self, EncryptionKey, NONCE_LEN};
use crate::error::CoreError;

/// Identificador de versão do envelope.
pub const MAGIC: [u8; 4] = *b"LBK1";

/// Byte de flags: chunk criptografado.
pub const FLAG_ENCRYPTED: u8 = 0b0000_0001;

/// Byte de flags: payload comprimido com Zstandard.
pub const FLAG_COMPRESSED: u8 = 0b0000_0010;

/// Flags conhecidas por esta versão.
pub const KNOWN_FLAGS: u8 = FLAG_ENCRYPTED | FLAG_COMPRESSED;

/// Tamanho do cabeçalho do envelope.
pub const HEADER_LEN: usize = MAGIC.len() + 1 + NONCE_LEN;

/// Opções de codificação de um chunk.
pub struct EncodeOptions<'a> {
    /// Nível do Zstandard.
    pub compression_level: i32,
    /// Chave usada quando a criptografia está habilitada.
    pub encryption: Option<&'a EncryptionKey>,
}

/// Codifica um chunk no envelope persistido.
pub fn encode_chunk(data: &[u8], options: &EncodeOptions<'_>) -> Result<Vec<u8>, CoreError> {
    let compressed = compression::compress(data, options.compression_level)?;

    let mut flags = FLAG_COMPRESSED;
    let (nonce, payload) = match options.encryption {
        Some(key) => {
            let sealed = crypto::seal(key, &compressed)?;
            flags |= FLAG_ENCRYPTED;
            (sealed.nonce, sealed.ciphertext)
        }
        None => ([0u8; NONCE_LEN], compressed),
    };

    let mut stored = Vec::with_capacity(HEADER_LEN + payload.len());
    stored.extend_from_slice(&MAGIC);
    stored.push(flags);
    stored.extend_from_slice(&nonce);
    stored.extend_from_slice(&payload);
    Ok(stored)
}

/// Decodifica e valida um chunk persistido.
///
/// `expected_size` é o tamanho original do chunk (vindo dos metadados) e é
/// usado como limite de descompressão e para validar o resultado.
pub fn decode_chunk(
    stored: &[u8],
    expected_size: u64,
    key: Option<&EncryptionKey>,
) -> Result<Vec<u8>, CoreError> {
    if stored.len() < HEADER_LEN {
        return Err(CoreError::ChunkFormat(format!(
            "envelope truncado: {} bytes, mínimo {HEADER_LEN}",
            stored.len()
        )));
    }

    if stored[0..4] != MAGIC {
        return Err(CoreError::ChunkFormat(
            "magic desconhecido no envelope do chunk".to_string(),
        ));
    }

    let flags = stored[4];
    if flags & !KNOWN_FLAGS != 0 {
        return Err(CoreError::ChunkFormat(format!(
            "flags desconhecidas no envelope: {flags:#04x}"
        )));
    }

    let mut nonce = [0u8; NONCE_LEN];
    nonce.copy_from_slice(&stored[5..HEADER_LEN]);
    let payload = &stored[HEADER_LEN..];

    let decoded_payload = if flags & FLAG_ENCRYPTED != 0 {
        let key = key.ok_or_else(|| {
            CoreError::ChunkFormat(
                "chunk criptografado encontrado, mas nenhuma chave foi fornecida".to_string(),
            )
        })?;
        crypto::open(key, &nonce, payload)?
    } else {
        payload.to_vec()
    };

    let data = if flags & FLAG_COMPRESSED != 0 {
        let max_size = usize::try_from(expected_size).map_err(|_| {
            CoreError::ChunkFormat(format!(
                "tamanho esperado do chunk excede usize: {expected_size}"
            ))
        })?;
        compression::decompress(&decoded_payload, max_size)?
    } else {
        decoded_payload
    };

    if data.len() as u64 != expected_size {
        return Err(CoreError::ChunkFormat(format!(
            "tamanho decodificado {} difere do esperado {expected_size}",
            data.len()
        )));
    }

    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encoded(data: &[u8], key: Option<&EncryptionKey>) -> Vec<u8> {
        encode_chunk(
            data,
            &EncodeOptions {
                compression_level: 3,
                encryption: key,
            },
        )
        .unwrap()
    }

    #[test]
    fn plain_roundtrip() {
        let data = b"conteudo de teste".repeat(32);
        let stored = encoded(&data, None);
        assert_eq!(&stored[0..4], &MAGIC);
        assert_eq!(stored[4], FLAG_COMPRESSED);
        assert_eq!(
            decode_chunk(&stored, data.len() as u64, None).unwrap(),
            data
        );
    }

    #[test]
    fn encrypted_roundtrip() {
        let key = EncryptionKey::from_bytes([3u8; crypto::KEY_LEN]);
        let data = b"conteudo sensivel".repeat(32);
        let stored = encoded(&data, Some(&key));
        assert_eq!(stored[4], FLAG_COMPRESSED | FLAG_ENCRYPTED);
        assert_eq!(
            decode_chunk(&stored, data.len() as u64, Some(&key)).unwrap(),
            data
        );
    }

    #[test]
    fn encrypted_chunk_requires_key() {
        let key = EncryptionKey::from_bytes([3u8; crypto::KEY_LEN]);
        let stored = encoded(b"dados", Some(&key));
        assert!(decode_chunk(&stored, 4, None).is_err());
    }

    #[test]
    fn wrong_key_fails() {
        let key = EncryptionKey::from_bytes([3u8; crypto::KEY_LEN]);
        let wrong = EncryptionKey::from_bytes([4u8; crypto::KEY_LEN]);
        let stored = encoded(b"dados", Some(&key));
        assert!(decode_chunk(&stored, 4, Some(&wrong)).is_err());
    }

    #[test]
    fn tampered_payload_fails() {
        let key = EncryptionKey::from_bytes([3u8; crypto::KEY_LEN]);
        let mut stored = encoded(b"dados", Some(&key));
        let last = stored.len() - 1;
        stored[last] ^= 0x01;
        assert!(decode_chunk(&stored, 4, Some(&key)).is_err());
    }

    #[test]
    fn rejects_bad_magic() {
        let mut stored = encoded(b"dados", None);
        stored[0] = b'X';
        assert!(decode_chunk(&stored, 4, None).is_err());
    }

    #[test]
    fn rejects_unknown_flags() {
        let mut stored = encoded(b"dados", None);
        stored[4] |= 0b1000_0000;
        assert!(decode_chunk(&stored, 4, None).is_err());
    }

    #[test]
    fn rejects_truncated_header() {
        let stored = encoded(b"dados", None);
        assert!(decode_chunk(&stored[..HEADER_LEN - 1], 4, None).is_err());
    }

    #[test]
    fn rejects_size_mismatch() {
        let data = b"dados";
        let stored = encoded(data, None);
        assert!(decode_chunk(&stored, data.len() as u64 + 1, None).is_err());
    }

    #[test]
    fn supports_uncompressed_payload() {
        let payload = b"sem compressao";
        let mut stored = Vec::new();
        stored.extend_from_slice(&MAGIC);
        stored.push(0);
        stored.extend_from_slice(&[0u8; NONCE_LEN]);
        stored.extend_from_slice(payload);
        assert_eq!(
            decode_chunk(&stored, payload.len() as u64, None).unwrap(),
            payload
        );
    }
}
