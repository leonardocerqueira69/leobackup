//! Tradução de eventos do `notify` para o modelo do daemon.
//!
//! O callback do watcher deve ser barato: esta camada apenas converte o
//! evento e filtra ruído, sem tocar no sistema de arquivos ou no banco.

use std::path::{Component, Path, PathBuf};

use notify::event::{EventKind, ModifyKind, RenameMode};
use notify::Event;

/// O que aconteceu com um caminho de origem.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceEventKind {
    /// O caminho foi criado, modificado ou renomeado para este nome.
    Upsert,
    /// O caminho foi removido ou renomeado para outro nome.
    Deleted,
}

/// Evento de origem já normalizado para a fila do daemon.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceEvent {
    /// Caminho afetado.
    pub path: PathBuf,
    /// Natureza da alteração.
    pub kind: SourceEventKind,
}

impl SourceEvent {
    /// Cria um evento.
    pub fn new(path: impl Into<PathBuf>, kind: SourceEventKind) -> Self {
        Self {
            path: path.into(),
            kind,
        }
    }
}

/// Converte um evento do `notify` em zero ou mais eventos de origem.
///
/// Eventos de acesso puro são ignorados. Renomeações com origem e destino
/// conhecidos geram uma remoção seguida de uma inclusão.
pub fn map_notify_event(event: &Event) -> Vec<SourceEvent> {
    let paths = &event.paths;

    match event.kind {
        EventKind::Access(_) => Vec::new(),
        EventKind::Remove(_) => paths
            .iter()
            .map(|path| SourceEvent::new(path.clone(), SourceEventKind::Deleted))
            .collect(),
        EventKind::Modify(ModifyKind::Name(RenameMode::Both)) => {
            if paths.len() < 2 {
                return paths
                    .iter()
                    .map(|path| SourceEvent::new(path.clone(), SourceEventKind::Upsert))
                    .collect();
            }

            let mut events = Vec::with_capacity(paths.len());
            events.push(SourceEvent::new(paths[0].clone(), SourceEventKind::Deleted));
            for path in &paths[1..] {
                events.push(SourceEvent::new(path.clone(), SourceEventKind::Upsert));
            }
            events
        }
        EventKind::Modify(ModifyKind::Name(RenameMode::From)) => paths
            .iter()
            .map(|path| SourceEvent::new(path.clone(), SourceEventKind::Deleted))
            .collect(),
        EventKind::Create(_) | EventKind::Modify(_) | EventKind::Any | EventKind::Other => paths
            .iter()
            .map(|path| SourceEvent::new(path.clone(), SourceEventKind::Upsert))
            .collect(),
    }
}

/// Normaliza um caminho para comparação de contenção entre diretórios.
///
/// Canonicaliza quando possível e, no Windows, remove o prefixo `\\?\`,
/// uniformiza separadores e ignora maiúsculas/minúsculas. Em caminhos que não
/// existem mais (eventos de remoção), usa a forma lexical.
pub fn path_key(path: &Path) -> String {
    let resolved = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    normalize_lexically(&resolved)
}

/// Indica se `path` está contido em `root` (ou é igual a ele).
pub fn is_within(path: &Path, root: &Path) -> bool {
    let path_value = path_key(path);
    let root_value = path_key(root);

    if path_value == root_value {
        return true;
    }

    let separator = std::path::MAIN_SEPARATOR.to_string();
    let prefix = if root_value.ends_with(&separator) {
        root_value
    } else {
        format!("{root_value}{separator}")
    };
    path_value.starts_with(&prefix)
}

/// Filtro de contenção com a chave da raiz calculada uma única vez.
///
/// Usado no callback do watcher para descartar eventos do próprio repositório
/// sem recalcular a raiz a cada evento.
#[derive(Debug, Clone)]
pub struct Containment {
    root_key: String,
    prefix: String,
}

impl Containment {
    /// Cria o filtro para uma raiz.
    pub fn new(root: &Path) -> Self {
        let root_key = path_key(root);
        let separator = std::path::MAIN_SEPARATOR.to_string();
        let prefix = if root_key.ends_with(&separator) {
            root_key.clone()
        } else {
            format!("{root_key}{separator}")
        };
        Self { root_key, prefix }
    }

    /// Indica se o caminho pertence à raiz.
    ///
    /// Usa apenas normalização lexical (sem acessar o disco) porque é chamado
    /// no callback do watcher, potencialmente para muitos eventos seguidos.
    pub fn contains(&self, path: &Path) -> bool {
        let key = normalize_lexically(path);
        key == self.root_key || key.starts_with(&self.prefix)
    }
}

fn normalize_lexically(path: &Path) -> String {
    let mut components = Vec::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if matches!(components.last(), Some(Component::Normal(_))) {
                    components.pop();
                } else {
                    components.push(component);
                }
            }
            other => components.push(other),
        }
    }

    let joined: PathBuf = components.iter().collect();
    let mut rendered = joined.to_string_lossy().into_owned();

    #[cfg(windows)]
    {
        if let Some(stripped) = rendered.strip_prefix(r"\\?\") {
            rendered = stripped.to_string();
        }
        rendered = rendered.replace('/', "\\").to_lowercase();
    }

    rendered
}

#[cfg(test)]
mod tests {
    use super::*;
    use notify::event::{AccessKind, CreateKind, DataChange, MetadataKind, RemoveKind};

    fn event(kind: EventKind, paths: &[&str]) -> Event {
        Event {
            kind,
            paths: paths.iter().map(PathBuf::from).collect(),
            attrs: Default::default(),
        }
    }

    fn kinds(events: &[SourceEvent]) -> Vec<SourceEventKind> {
        events.iter().map(|event| event.kind).collect()
    }

    #[test]
    fn maps_create_and_modify_to_upsert() {
        let create = map_notify_event(&event(EventKind::Create(CreateKind::File), &["/src/a.txt"]));
        assert_eq!(kinds(&create), vec![SourceEventKind::Upsert]);

        let data = map_notify_event(&event(
            EventKind::Modify(ModifyKind::Data(DataChange::Content)),
            &["/src/a.txt"],
        ));
        assert_eq!(kinds(&data), vec![SourceEventKind::Upsert]);

        let metadata = map_notify_event(&event(
            EventKind::Modify(ModifyKind::Metadata(MetadataKind::Any)),
            &["/src/a.txt"],
        ));
        assert_eq!(kinds(&metadata), vec![SourceEventKind::Upsert]);

        let any = map_notify_event(&event(EventKind::Any, &["/src/a.txt"]));
        assert_eq!(kinds(&any), vec![SourceEventKind::Upsert]);
    }

    #[test]
    fn maps_remove_to_deleted() {
        let removed =
            map_notify_event(&event(EventKind::Remove(RemoveKind::File), &["/src/a.txt"]));
        assert_eq!(kinds(&removed), vec![SourceEventKind::Deleted]);
    }

    #[test]
    fn ignores_pure_access_events() {
        let accessed =
            map_notify_event(&event(EventKind::Access(AccessKind::Read), &["/src/a.txt"]));
        assert!(accessed.is_empty());
    }

    #[test]
    fn maps_rename_both_to_delete_and_upsert() {
        let renamed = map_notify_event(&event(
            EventKind::Modify(ModifyKind::Name(RenameMode::Both)),
            &["/src/a.txt", "/src/b.txt"],
        ));
        assert_eq!(
            kinds(&renamed),
            vec![SourceEventKind::Deleted, SourceEventKind::Upsert]
        );
        assert_eq!(renamed[0].path, PathBuf::from("/src/a.txt"));
        assert_eq!(renamed[1].path, PathBuf::from("/src/b.txt"));
    }

    #[test]
    fn maps_rename_sides() {
        let from = map_notify_event(&event(
            EventKind::Modify(ModifyKind::Name(RenameMode::From)),
            &["/src/a.txt"],
        ));
        assert_eq!(kinds(&from), vec![SourceEventKind::Deleted]);

        let to = map_notify_event(&event(
            EventKind::Modify(ModifyKind::Name(RenameMode::To)),
            &["/src/b.txt"],
        ));
        assert_eq!(kinds(&to), vec![SourceEventKind::Upsert]);
    }

    #[test]
    fn empty_paths_yield_no_events() {
        assert!(map_notify_event(&event(EventKind::Any, &[])).is_empty());
    }

    #[test]
    fn detects_containment_without_canonicalization() {
        let root = Path::new("/backup/repo");
        assert!(is_within(Path::new("/backup/repo"), root));
        assert!(is_within(Path::new("/backup/repo/data/x"), root));
        assert!(!is_within(Path::new("/backup/repository/data"), root));
        assert!(!is_within(Path::new("/backup"), root));

        let containment = Containment::new(root);
        assert!(containment.contains(Path::new("/backup/repo/data/x")));
        assert!(!containment.contains(Path::new("/backup/repository/data")));
    }
}
