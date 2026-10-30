//! Hashing BLAKE3 usado para identificar e validar chunks.

/// Comprimento de um hash BLAKE3 em hexadecimal minúsculo.
pub const HASH_HEX_LEN: usize = 64;

/// Calcula o hash BLAKE3 de `data` e retorna em hexadecimal minúsculo.
pub fn hash_chunk(data: &[u8]) -> String {
    blake3::hash(data).to_hex().to_string()
}

/// Verifica se `value` é um hash BLAKE3 hexadecimal minúsculo de 64 caracteres.
pub fn is_valid_hash(value: &str) -> bool {
    value.len() == HASH_HEX_LEN
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_is_deterministic_and_well_formed() {
        let a = hash_chunk(b"conteudo");
        let b = hash_chunk(b"conteudo");
        assert_eq!(a, b);
        assert!(is_valid_hash(&a));
        assert_eq!(a.len(), HASH_HEX_LEN);
    }

    #[test]
    fn different_inputs_produce_different_hashes() {
        assert_ne!(hash_chunk(b"a"), hash_chunk(b"b"));
    }

    #[test]
    fn empty_input_is_supported() {
        let hash = hash_chunk(b"");
        assert!(is_valid_hash(&hash));
        // Valor conhecido do BLAKE3 para entrada vazia.
        assert_eq!(
            hash,
            "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"
        );
    }

    #[test]
    fn rejects_malformed_hashes() {
        assert!(!is_valid_hash(""));
        assert!(!is_valid_hash("abc"));
        assert!(!is_valid_hash(
            "AF1349B9F5F9A1A6A0404DEA36DCC9499BCB25C9ADC112B7CC9A93CAE41F3262"
        ));
        assert!(!is_valid_hash(
            "zf1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"
        ));
    }
}
