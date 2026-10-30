//! Compressão de chunks com Zstandard.

use crate::error::CoreError;

/// Comprime `data` com o nível informado do Zstandard.
pub fn compress(data: &[u8], level: i32) -> Result<Vec<u8>, CoreError> {
    zstd::bulk::compress(data, level).map_err(|error| CoreError::Compression(error.to_string()))
}

/// Descomprime `data` limitando o resultado a `max_size` bytes.
///
/// O limite evita alocações desproporcionais com dados corrompidos. O
/// tamanho real é validado pelo chamador.
pub fn decompress(data: &[u8], max_size: usize) -> Result<Vec<u8>, CoreError> {
    zstd::bulk::decompress(data, max_size)
        .map_err(|error| CoreError::Compression(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_preserves_content() {
        let data = b"texto repetitivo texto repetitivo texto repetitivo".repeat(64);
        let compressed = compress(&data, 3).unwrap();
        let restored = decompress(&compressed, data.len()).unwrap();
        assert_eq!(restored, data);
    }

    #[test]
    fn roundtrip_supports_empty_content() {
        let compressed = compress(b"", 3).unwrap();
        let restored = decompress(&compressed, 0).unwrap();
        assert!(restored.is_empty());
    }

    #[test]
    fn rejects_invalid_data() {
        assert!(decompress(b"nao e zstd", 1024).is_err());
    }

    #[test]
    fn enforces_max_size() {
        let data = vec![7u8; 4096];
        let compressed = compress(&data, 3).unwrap();
        assert!(decompress(&compressed, 16).is_err());
    }
}
