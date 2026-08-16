use agenttalk_domain::WorkspaceAccess;
use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Error, Eq, PartialEq)]
pub enum PermissionError {
    #[error("Agent is not assigned to this Project")]
    NotAssigned,
    #[error("permission escalation is not allowed")]
    Escalation,
}

/// A short-lived, in-process receipt for one explicitly selected source file.
///
/// This receipt revalidates the source identity before Core copies it. It is
/// deliberately not serialized and is not a Windows capability token; the
/// native picker/OS authorization boundary remains a separate acceptance gate.
#[derive(Debug, Eq, PartialEq)]
pub struct FileReadGrant {
    canonical_path: PathBuf,
    size: u64,
    modified: Option<std::time::SystemTime>,
    revoked: bool,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum FileReadGrantError {
    #[error("file read grant requires an absolute path")]
    NotAbsolute,
    #[error("file read grant source is not a regular file")]
    NotRegularFile,
    #[error("file read grant source cannot be a symlink")]
    Symlink,
    #[error("file read grant source is unavailable")]
    Unavailable,
    #[error("file read grant source changed after selection")]
    Changed,
    #[error("file read grant was revoked")]
    Revoked,
}

impl FileReadGrant {
    pub fn issue(source_path: &Path) -> Result<Self, FileReadGrantError> {
        if !source_path.is_absolute() {
            return Err(FileReadGrantError::NotAbsolute);
        }
        let link_metadata =
            fs::symlink_metadata(source_path).map_err(|_| FileReadGrantError::Unavailable)?;
        if link_metadata.file_type().is_symlink() {
            return Err(FileReadGrantError::Symlink);
        }
        if !link_metadata.is_file() {
            return Err(FileReadGrantError::NotRegularFile);
        }
        let canonical_path =
            fs::canonicalize(source_path).map_err(|_| FileReadGrantError::Unavailable)?;
        let metadata =
            fs::metadata(&canonical_path).map_err(|_| FileReadGrantError::Unavailable)?;
        if !metadata.is_file() {
            return Err(FileReadGrantError::NotRegularFile);
        }
        Ok(Self {
            canonical_path,
            size: metadata.len(),
            modified: metadata.modified().ok(),
            revoked: false,
        })
    }

    pub fn validate(&self) -> Result<(), FileReadGrantError> {
        if self.revoked {
            return Err(FileReadGrantError::Revoked);
        }
        let link_metadata = fs::symlink_metadata(&self.canonical_path)
            .map_err(|_| FileReadGrantError::Unavailable)?;
        if link_metadata.file_type().is_symlink() {
            return Err(FileReadGrantError::Symlink);
        }
        if !link_metadata.is_file() {
            return Err(FileReadGrantError::NotRegularFile);
        }
        let canonical_path =
            fs::canonicalize(&self.canonical_path).map_err(|_| FileReadGrantError::Unavailable)?;
        if canonical_path != self.canonical_path {
            return Err(FileReadGrantError::Changed);
        }
        let metadata =
            fs::metadata(&canonical_path).map_err(|_| FileReadGrantError::Unavailable)?;
        if metadata.len() != self.size || metadata.modified().ok() != self.modified {
            return Err(FileReadGrantError::Changed);
        }
        Ok(())
    }

    pub fn source_path(&self) -> &Path {
        &self.canonical_path
    }

    pub fn revoke(&mut self) {
        self.revoked = true;
    }
}

pub fn resolve_project_agent_permission(
    assigned: bool,
    explicit: Option<WorkspaceAccess>,
    _legacy_capabilities: Option<&[String]>,
) -> Result<WorkspaceAccess, PermissionError> {
    if !assigned {
        return Err(PermissionError::NotAssigned);
    }
    // An empty or stale legacy capability list never overrides an explicit row.
    Ok(explicit.unwrap_or(WorkspaceAccess::ReadOnly))
}

pub fn can_use_workspace(access: &WorkspaceAccess, requested: &WorkspaceAccess) -> bool {
    matches!(
        (access, requested),
        (WorkspaceAccess::None, WorkspaceAccess::None)
            | (
                WorkspaceAccess::ReadOnly,
                WorkspaceAccess::None | WorkspaceAccess::ReadOnly
            )
            | (
                WorkspaceAccess::WorkspaceWrite,
                WorkspaceAccess::None | WorkspaceAccess::ReadOnly | WorkspaceAccess::WorkspaceWrite,
            )
    )
}

pub fn downgrade(
    access: &WorkspaceAccess,
    requested: WorkspaceAccess,
) -> Result<WorkspaceAccess, PermissionError> {
    if can_use_workspace(access, &requested) {
        Ok(requested)
    } else {
        Err(PermissionError::Escalation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{create_dir_all, remove_dir_all, write};

    #[test]
    fn unassigned_agents_are_rejected_and_empty_legacy_capabilities_do_not_downgrade_explicit_write(
    ) {
        assert_eq!(
            resolve_project_agent_permission(false, Some(WorkspaceAccess::WorkspaceWrite), None),
            Err(PermissionError::NotAssigned)
        );
        assert_eq!(
            resolve_project_agent_permission(
                true,
                Some(WorkspaceAccess::WorkspaceWrite),
                Some(&[])
            ),
            Ok(WorkspaceAccess::WorkspaceWrite)
        );
    }

    #[test]
    fn one_execution_can_only_downgrade_access() {
        assert_eq!(
            downgrade(&WorkspaceAccess::WorkspaceWrite, WorkspaceAccess::ReadOnly),
            Ok(WorkspaceAccess::ReadOnly)
        );
        assert_eq!(
            downgrade(&WorkspaceAccess::ReadOnly, WorkspaceAccess::WorkspaceWrite),
            Err(PermissionError::Escalation)
        );
    }

    #[test]
    fn file_read_grant_revalidates_selection_and_revocation() {
        let root =
            std::env::temp_dir().join(format!("agenttalk-file-grant-{}", std::process::id()));
        let source = root.join("selected.txt");
        let _ = remove_dir_all(&root);
        create_dir_all(&root).unwrap();
        write(&source, "selected content").unwrap();

        let mut grant = FileReadGrant::issue(&source).unwrap();
        assert_eq!(grant.source_path(), fs::canonicalize(&source).unwrap());
        grant.validate().unwrap();

        write(&source, "changed content with another size").unwrap();
        assert_eq!(grant.validate(), Err(FileReadGrantError::Changed));

        grant.revoke();
        assert_eq!(grant.validate(), Err(FileReadGrantError::Revoked));
        let _ = remove_dir_all(&root);
    }

    #[test]
    fn file_read_grant_rejects_relative_and_symlink_sources() {
        assert_eq!(
            FileReadGrant::issue(Path::new("relative.txt")),
            Err(FileReadGrantError::NotAbsolute)
        );
        let root = std::env::temp_dir().join(format!(
            "agenttalk-file-grant-symlink-{}",
            std::process::id()
        ));
        let source = root.join("source.txt");
        let link = root.join("link.txt");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        fs::write(&source, "source").unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(&source, &link).unwrap();
        #[cfg(windows)]
        std::os::windows::fs::symlink_file(&source, &link).unwrap();
        assert_eq!(
            FileReadGrant::issue(&link),
            Err(FileReadGrantError::Symlink)
        );
        let _ = fs::remove_dir_all(&root);
    }
}
