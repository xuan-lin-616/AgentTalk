use agenttalk_domain::{WorkspaceAccess, WorkspaceAuthorization};
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum WorkspaceError {
    #[error("workspace root does not exist or is not a directory")]
    InvalidRoot,
    #[error("workspace path escapes the authorized Project root")]
    OutsideRoot,
    #[error("workspace write requires explicit workspace_write permission")]
    WriteNotAuthorized,
    #[error("filesystem error: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedWorkspace {
    pub canonical_path: PathBuf,
    pub access: WorkspaceAccess,
}

pub struct WorkspaceManager {
    authorization: WorkspaceAuthorization,
}

impl WorkspaceManager {
    pub fn authorize(
        project_id: impl Into<String>,
        root: impl AsRef<Path>,
    ) -> Result<Self, WorkspaceError> {
        let root = root.as_ref();
        if !root.is_dir() {
            return Err(WorkspaceError::InvalidRoot);
        }
        let canonical_root = std::fs::canonicalize(root)?;
        Ok(Self {
            authorization: WorkspaceAuthorization {
                project_id: project_id.into(),
                canonical_root: canonical_root.to_string_lossy().into_owned(),
                revision: 1,
                validation_status: "valid".into(),
            },
        })
    }

    pub fn authorization(&self) -> &WorkspaceAuthorization {
        &self.authorization
    }

    pub fn resolve(
        &self,
        requested: Option<impl AsRef<Path>>,
        access: WorkspaceAccess,
    ) -> Result<ResolvedWorkspace, WorkspaceError> {
        let root = PathBuf::from(&self.authorization.canonical_root);
        let requested = requested
            .map(|path| path.as_ref().to_path_buf())
            .unwrap_or_else(|| root.clone());
        let canonical = std::fs::canonicalize(requested)?;
        if !canonical.starts_with(&root) {
            return Err(WorkspaceError::OutsideRoot);
        }
        Ok(ResolvedWorkspace {
            canonical_path: canonical,
            access,
        })
    }

    pub fn validate_write(&self, workspace: &ResolvedWorkspace) -> Result<(), WorkspaceError> {
        if workspace.access == WorkspaceAccess::WorkspaceWrite {
            Ok(())
        } else {
            Err(WorkspaceError::WriteNotAuthorized)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{create_dir, write};

    #[test]
    fn canonical_workspace_rejects_outside_path() {
        let root = std::env::temp_dir().join(format!("agenttalk-workspace-{}", std::process::id()));
        let child = root.join("child");
        let outside = root.with_file_name(format!("agenttalk-outside-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(&outside);
        create_dir(&root).unwrap();
        create_dir(&child).unwrap();
        create_dir(&outside).unwrap();
        write(child.join("file.txt"), "ok").unwrap();
        let manager = WorkspaceManager::authorize("project-1", &root).unwrap();
        assert!(manager
            .resolve(Some(&child), WorkspaceAccess::ReadOnly)
            .is_ok());
        assert!(matches!(
            manager.resolve(Some(&outside), WorkspaceAccess::ReadOnly),
            Err(WorkspaceError::OutsideRoot)
        ));
        let _ = std::fs::remove_dir_all(root);
        let _ = std::fs::remove_dir_all(outside);
    }

    #[test]
    fn read_only_cannot_be_used_as_write() {
        let root =
            std::env::temp_dir().join(format!("agenttalk-workspace-write-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        create_dir(&root).unwrap();
        let manager = WorkspaceManager::authorize("project-1", &root).unwrap();
        let resolved = manager
            .resolve(None::<&Path>, WorkspaceAccess::ReadOnly)
            .unwrap();
        assert!(matches!(
            manager.validate_write(&resolved),
            Err(WorkspaceError::WriteNotAuthorized)
        ));
        let _ = std::fs::remove_dir_all(root);
    }
}
