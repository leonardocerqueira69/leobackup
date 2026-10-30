//! Chunking baseado em conteúdo (FastCDC, variante 2020).
//!
//! Os limites aceitos refletem as restrições da crate `fastcdc`. A validação
//! acontece em `ChunkingParams::new`, evitando que uma configuração inválida
//! chegue aos `assert!` internos da biblioteca.

use std::io::Read;

use fastcdc::v2020::{ChunkData, Error as FastCdcError, StreamCDC};

use crate::error::CoreError;

/// Menor `min_size` aceito pelo FastCDC.
pub const MIN_SIZE_MIN: u32 = 64;
/// Maior `min_size` aceito pelo FastCDC.
pub const MIN_SIZE_MAX: u32 = 1_048_576;
/// Menor `avg_size` aceito pelo FastCDC.
pub const AVG_SIZE_MIN: u32 = 256;
/// Maior `avg_size` aceito pelo FastCDC.
pub const AVG_SIZE_MAX: u32 = 4_194_304;
/// Menor `max_size` aceito pelo FastCDC.
pub const MAX_SIZE_MIN: u32 = 1_024;
/// Maior `max_size` aceito pelo FastCDC.
pub const MAX_SIZE_MAX: u32 = 16_777_216;

/// Parâmetros validados do FastCDC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkingParams {
    min_size: u32,
    avg_size: u32,
    max_size: u32,
}

impl ChunkingParams {
    /// Valida e constrói os parâmetros.
    pub fn new(min_size: u32, avg_size: u32, max_size: u32) -> Result<Self, CoreError> {
        if !(MIN_SIZE_MIN..=MIN_SIZE_MAX).contains(&min_size) {
            return Err(CoreError::InvalidConfig(format!(
                "chunking.min_size deve estar entre {MIN_SIZE_MIN} e {MIN_SIZE_MAX} bytes (atual: {min_size})"
            )));
        }
        if !(AVG_SIZE_MIN..=AVG_SIZE_MAX).contains(&avg_size) {
            return Err(CoreError::InvalidConfig(format!(
                "chunking.avg_size deve estar entre {AVG_SIZE_MIN} e {AVG_SIZE_MAX} bytes (atual: {avg_size})"
            )));
        }
        if !(MAX_SIZE_MIN..=MAX_SIZE_MAX).contains(&max_size) {
            return Err(CoreError::InvalidConfig(format!(
                "chunking.max_size deve estar entre {MAX_SIZE_MIN} e {MAX_SIZE_MAX} bytes (atual: {max_size})"
            )));
        }
        if !(min_size <= avg_size && avg_size <= max_size) {
            return Err(CoreError::InvalidConfig(format!(
                "chunking inválido: exige min_size <= avg_size <= max_size (min={min_size}, avg={avg_size}, max={max_size})"
            )));
        }
        Ok(Self {
            min_size,
            avg_size,
            max_size,
        })
    }

    /// Tamanho mínimo de chunk, em bytes.
    pub const fn min_size(&self) -> u32 {
        self.min_size
    }

    /// Tamanho médio de chunk, em bytes.
    pub const fn avg_size(&self) -> u32 {
        self.avg_size
    }

    /// Tamanho máximo de chunk, em bytes.
    pub const fn max_size(&self) -> u32 {
        self.max_size
    }
}

/// Um chunk produzido pelo FastCDC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chunk {
    offset: u64,
    data: Vec<u8>,
}

impl Chunk {
    /// Posição inicial do chunk no fluxo de origem.
    pub const fn offset(&self) -> u64 {
        self.offset
    }

    /// Conteúdo do chunk.
    pub fn data(&self) -> &[u8] {
        &self.data
    }
}

impl From<ChunkData> for Chunk {
    fn from(value: ChunkData) -> Self {
        Self {
            offset: value.offset,
            data: value.data,
        }
    }
}

/// Iterador de chunks sobre um leitor.
pub struct Chunker<R: Read> {
    inner: StreamCDC<R>,
}

impl<R: Read> Iterator for Chunker<R> {
    type Item = Result<Chunk, CoreError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|result| result.map(Chunk::from).map_err(map_error))
    }
}

/// Cria um iterador de chunks a partir de um leitor e parâmetros validados.
pub fn chunk_stream<R: Read>(reader: R, params: ChunkingParams) -> Chunker<R> {
    Chunker {
        inner: StreamCDC::new(reader, params.min_size, params.avg_size, params.max_size),
    }
}

fn map_error(error: FastCdcError) -> CoreError {
    CoreError::Chunking(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn params() -> ChunkingParams {
        ChunkingParams::new(1_024, 4_096, 16_384).unwrap()
    }

    #[test]
    fn rejects_out_of_range_params() {
        assert!(ChunkingParams::new(0, 4_096, 16_384).is_err());
        assert!(ChunkingParams::new(1_024, 4_096, 32).is_err());
        assert!(ChunkingParams::new(4_096, 1_024, 16_384).is_err());
    }

    #[test]
    fn empty_input_yields_no_chunks() {
        let chunks: Vec<_> = chunk_stream(Cursor::new(Vec::new()), params()).collect();
        assert!(chunks.is_empty());
    }

    #[test]
    fn small_input_yields_single_chunk() {
        let data = b"conteudo pequeno".to_vec();
        let chunks: Vec<_> = chunk_stream(Cursor::new(data.clone()), params()).collect();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].as_ref().unwrap().data(), data.as_slice());
        assert_eq!(chunks[0].as_ref().unwrap().offset(), 0);
    }

    #[test]
    fn large_input_is_deterministic_and_respects_limits() {
        let mut data = vec![0u8; 1_048_576];
        let mut state = 0x1234_5678_u64;
        for byte in &mut data {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *byte = state as u8;
        }

        let chunks: Vec<Chunk> = chunk_stream(Cursor::new(data.clone()), params())
            .map(Result::unwrap)
            .collect();

        assert!(chunks.len() > 1);
        let mut rebuilt = Vec::with_capacity(data.len());
        for chunk in &chunks {
            let len = chunk.data().len();
            assert!(len <= 16_384);
            rebuilt.extend_from_slice(chunk.data());
        }
        assert_eq!(rebuilt, data);
    }
}
