//! Varredura de diretórios de origem com consumo de memória limitado.
//!
//! A travessia é recursiva e entrega cada arquivo ao callback imediatamente,
//! sem montar uma lista completa em memória. Isso permite processar árvores
//! com muitos arquivos mantendo o uso de RAM proporcional à profundidade.
//!
//! Regras de segurança:
//! - symlinks não são seguidos (evita ciclos e saída da árvore);
//! - diretórios excluídos (tipicamente o próprio repositório) não são visitados;
//! - profundidade é limitada para evitar árvores patológicas.

use std::fs;
use std::path::{Path, PathBuf};

use crate::event::path_key;

/// Profundidade máxima de diretórios visitados.
pub const MAX_DEPTH: usize = 256;

/// Controle devolvido pelo callback de varredura.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanControl {
    /// Continua a varredura.
    Continue,
    /// Interrompe a varredura (por exemplo, em um encerramento).
    Stop,
}

/// Estatísticas de uma varredura.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ScanSummary {
    /// Arquivos regulares entregues ao callback.
    pub files: u64,
    /// Diretórios visitados.
    pub dirs: u64,
    /// Entradas que não puderam ser lidas.
    pub errors: u64,
    /// Entradas ignoradas por serem symlink, especiais ou profundas demais.
    pub skipped: u64,
    /// Se a varredura foi interrompida pelo callback.
    pub stopped: bool,
}

/// Percorre `roots` recursivamente e chama `visit` para cada arquivo regular.
///
/// Caminhos contidos em `excludes` não são visitados. Raízes inexistentes ou
/// que não sejam diretórios contam como erro e não interrompem as demais.
pub fn walk_files<F>(
    roots: &[PathBuf],
    excludes: &[PathBuf],
    max_depth: usize,
    mut visit: F,
) -> ScanSummary
where
    F: FnMut(&Path) -> ScanControl,
{
    let excluded: Vec<(String, String)> = excludes
        .iter()
        .map(|path| {
            let key = path_key(path);
            let prefix = format!("{key}{}", std::path::MAIN_SEPARATOR);
            (key, prefix)
        })
        .collect();
    let is_excluded = |path: &Path| {
        let key = path_key(path);
        excluded
            .iter()
            .any(|(excluded_key, prefix)| key == *excluded_key || key.starts_with(prefix))
    };

    let mut summary = ScanSummary::default();
    let mut stack: Vec<(PathBuf, usize)> = Vec::new();

    for root in roots {
        if is_excluded(root) {
            summary.skipped += 1;
            continue;
        }

        let metadata = match fs::symlink_metadata(root) {
            Ok(metadata) => metadata,
            Err(_) => {
                summary.errors += 1;
                continue;
            }
        };

        if metadata.file_type().is_symlink() {
            summary.skipped += 1;
            continue;
        }

        if !metadata.is_dir() {
            summary.errors += 1;
            continue;
        }

        stack.push((root.clone(), 0));
    }

    while let Some((dir, depth)) = stack.pop() {
        if is_excluded(&dir) {
            summary.skipped += 1;
            continue;
        }

        summary.dirs += 1;

        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(_) => {
                summary.errors += 1;
                continue;
            }
        };

        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(_) => {
                    summary.errors += 1;
                    continue;
                }
            };

            let file_type = match entry.file_type() {
                Ok(file_type) => file_type,
                Err(_) => {
                    summary.errors += 1;
                    continue;
                }
            };

            if file_type.is_symlink() {
                summary.skipped += 1;
                continue;
            }

            let path = entry.path();

            if file_type.is_dir() {
                if depth + 1 > max_depth {
                    summary.skipped += 1;
                } else {
                    stack.push((path, depth + 1));
                }
                continue;
            }

            if file_type.is_file() {
                summary.files += 1;
                if visit(&path) == ScanControl::Stop {
                    summary.stopped = true;
                    return summary;
                }
                continue;
            }

            summary.skipped += 1;
        }
    }

    summary
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_file(path: &Path, contents: &[u8]) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, contents).unwrap();
    }

    fn collect(roots: &[PathBuf], excludes: &[PathBuf]) -> (Vec<PathBuf>, ScanSummary) {
        let mut visited = Vec::new();
        let summary = walk_files(roots, excludes, MAX_DEPTH, |path| {
            visited.push(path.to_path_buf());
            ScanControl::Continue
        });
        visited.sort();
        (visited, summary)
    }

    #[test]
    fn visits_nested_files_and_skips_excluded_tree() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("src");
        let excluded = dir.path().join("repo");
        create_file(&root.join("a.txt"), b"a");
        create_file(&root.join("sub").join("b.txt"), b"b");
        create_file(&root.join("sub").join("deep").join("c.txt"), b"c");
        create_file(&excluded.join("data").join("chunk"), b"x");
        fs::create_dir_all(root.join("empty")).unwrap();

        let (visited, summary) = collect(std::slice::from_ref(&root), &[excluded]);

        assert_eq!(visited.len(), 3);
        assert!(visited.contains(&root.join("a.txt")));
        assert!(visited.contains(&root.join("sub").join("b.txt")));
        assert!(visited.contains(&root.join("sub").join("deep").join("c.txt")));
        assert_eq!(summary.files, 3);
        assert_eq!(summary.errors, 0);
        assert!(!summary.stopped);
    }

    #[test]
    fn callback_can_stop_scan() {
        let dir = tempfile::tempdir().unwrap();
        for name in ["a", "b", "c"] {
            create_file(&dir.path().join(name), b"x");
        }

        let mut seen = 0;
        let summary = walk_files(&[dir.path().to_path_buf()], &[], MAX_DEPTH, |_| {
            seen += 1;
            ScanControl::Stop
        });

        assert_eq!(seen, 1);
        assert_eq!(summary.files, 1);
        assert!(summary.stopped);
    }

    #[test]
    fn missing_root_counts_as_error() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("nao-existe");
        let (visited, summary) = collect(&[missing], &[]);

        assert!(visited.is_empty());
        assert_eq!(summary.errors, 1);
    }

    #[test]
    fn file_root_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("arquivo.txt");
        create_file(&file, b"x");

        let (_, summary) = collect(&[file], &[]);
        assert_eq!(summary.errors, 1);
    }

    #[test]
    fn handles_unicode_names() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("origem-ção");
        let file = root.join("não-é-acentuação-ção.txt");
        create_file(&file, b"conteudo");

        let (visited, summary) = collect(&[root], &[]);
        assert_eq!(summary.files, 1);
        assert_eq!(visited.len(), 1);
    }

    #[test]
    fn depth_limit_skips_deep_directories() {
        let dir = tempfile::tempdir().unwrap();
        let mut deep = dir.path().join("a");
        create_file(&deep.join("top.txt"), b"x");
        for index in 0..5 {
            deep = deep.join(format!("d{index}"));
        }
        create_file(&deep.join("bottom.txt"), b"x");

        let mut visited = Vec::new();
        let summary = walk_files(&[dir.path().to_path_buf()], &[], 2, |path| {
            visited.push(path.to_path_buf());
            ScanControl::Continue
        });

        assert_eq!(summary.files, 1);
        assert_eq!(visited.len(), 1);
        assert!(summary.skipped >= 1);
    }

    #[cfg(unix)]
    #[test]
    fn does_not_follow_symlinks() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("src");
        create_file(&root.join("real.txt"), b"x");
        symlink(&root, root.join("loop")).unwrap();

        let (visited, summary) = collect(&[root.clone()], &[]);
        assert_eq!(summary.files, 1);
        assert_eq!(visited.len(), 1);
        assert!(summary.skipped >= 1);
    }
}
