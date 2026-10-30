//! Token de autenticação do servidor local.
//!
//! O token é gerado na primeira execução do daemon, com 256 bits de entropia,
//! e persistido em `config/daemon.token`. O arquivo nunca é sobrescrito
//! automaticamente: um conteúdo inválido exige intervenção explícita para não
//! invalidar clientes em silêncio.
//!
//! Em Unix o arquivo é criado com modo `0600`. No Windows as permissões são
//! herdadas do diretório do repositório; restrinja o acesso à pasta de backup.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;

use crate::error::DaemonError;

/// Comprimento do token em caracteres hexadecimais (32 bytes).
pub const TOKEN_HEX_LEN: usize = 64;

const TOKEN_BYTES: usize = 32;

/// Lê o token existente ou cria um novo de forma atômica.
pub fn ensure_token(path: &Path) -> Result<String, DaemonError> {
    match fs::read(path) {
        Ok(bytes) => parse_token(path, &bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => create_token(path),
        Err(source) => Err(DaemonError::io(path, source)),
    }
}

/// Lê e valida um token já existente.
pub fn load_token(path: &Path) -> Result<String, DaemonError> {
    let bytes = fs::read(path).map_err(|source| DaemonError::io(path, source))?;
    parse_token(path, &bytes)
}

/// Gera um token hexadecimal de 256 bits.
pub fn generate_token() -> Result<String, DaemonError> {
    let mut bytes = [0u8; TOKEN_BYTES];
    getrandom::getrandom(&mut bytes)
        .map_err(|error| DaemonError::Internal(format!("RNG indisponível: {error}")))?;
    Ok(to_hex(&bytes))
}

/// Compara dois tokens em tempo constante em relação ao conteúdo.
pub fn constant_time_eq(left: &str, right: &str) -> bool {
    let left = left.as_bytes();
    let right = right.as_bytes();

    let mut difference = (left.len() ^ right.len()) as u8;
    let common = left.len().min(right.len());
    for index in 0..common {
        difference |= left[index] ^ right[index];
    }
    difference == 0
}

/// Verifica se a string tem o formato de token (hexadecimal de 64 caracteres).
pub fn is_valid_token(value: &str) -> bool {
    value.len() == TOKEN_HEX_LEN && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn create_token(path: &Path) -> Result<String, DaemonError> {
    let token = generate_token()?;

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| DaemonError::io(parent, source))?;
    }

    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }

    match options.open(path) {
        Ok(mut file) => {
            let mut contents = token.clone().into_bytes();
            contents.push(b'\n');
            file.write_all(&contents)
                .map_err(|source| DaemonError::io(path, source))?;
            file.sync_all()
                .map_err(|source| DaemonError::io(path, source))?;
            Ok(token)
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            // Outro processo criou o token entre a leitura e a criação.
            load_token(path)
        }
        Err(source) => Err(DaemonError::io(path, source)),
    }
}

fn parse_token(path: &Path, bytes: &[u8]) -> Result<String, DaemonError> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| {
            DaemonError::Internal(format!(
                "token em {} não é UTF-8 válido; remova o arquivo para gerar outro",
                path.display()
            ))
        })?
        .trim();

    if !is_valid_token(text) {
        return Err(DaemonError::Internal(format!(
            "token inválido em {}; remova o arquivo para gerar outro",
            path.display()
        )));
    }

    Ok(text.to_string())
}

fn to_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(DIGITS[(byte >> 4) as usize] as char);
        output.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_stable_token_with_restricted_permissions() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config").join("daemon.token");

        let first = ensure_token(&path).unwrap();
        assert!(is_valid_token(&first));
        assert_eq!(first.len(), TOKEN_HEX_LEN);

        let second = ensure_token(&path).unwrap();
        assert_eq!(first, second);

        let contents = fs::read_to_string(&path).unwrap();
        assert_eq!(contents.trim(), first);

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600);
        }
    }

    #[test]
    fn rejects_existing_invalid_token_without_overwriting() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("daemon.token");
        fs::write(&path, b"nao-e-um-token\n").unwrap();

        assert!(ensure_token(&path).is_err());
        assert_eq!(fs::read(&path).unwrap(), b"nao-e-um-token\n");
    }

    #[test]
    fn generated_tokens_differ() {
        assert_ne!(generate_token().unwrap(), generate_token().unwrap());
    }

    #[test]
    fn comparison_is_content_based() {
        let token = generate_token().unwrap();
        assert!(constant_time_eq(&token, &token));
        assert!(!constant_time_eq(&token, "outro"));
        assert!(!constant_time_eq("", &token));
        assert!(constant_time_eq("", ""));
    }

    #[test]
    fn validates_hex_format() {
        assert!(is_valid_token(&"a".repeat(64)));
        assert!(!is_valid_token(&"a".repeat(63)));
        assert!(!is_valid_token(&"g".repeat(64)));
    }
}
