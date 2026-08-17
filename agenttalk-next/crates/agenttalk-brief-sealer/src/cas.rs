//! Core-owned content-addressed store at `<project_root>/.agenttalk/objects/`.
//!
//! The store is intentionally tiny and authority-free: it only publishes and
//! verifies digest-addressed blobs. Journal/snapshot references belong to the
//! orchestration journal, not to this module.

use crate::fs_guard;
use sha2::{Digest, Sha256};
use std::fmt::{Display, Formatter};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

pub const CAS_DIR_NAME: &str = ".agenttalk";
pub const OBJECTS_DIR_NAME: &str = "objects";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CasObject {
    pub object_ref: String,
    pub sha256: String,
    pub size: u64,
}

#[derive(Debug)]
pub enum CasError {
    ObjectRefInvalid {
        object_ref: String,
    },
    HashMismatch {
        object_ref: String,
        expected: String,
        actual: String,
    },
    ObjectConflict {
        object_ref: String,
    },
    ReparsePoint {
        path: PathBuf,
    },
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
}

impl Display for CasError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ObjectRefInvalid { object_ref } => write!(f, "invalid object ref: {object_ref}"),
            Self::HashMismatch {
                object_ref,
                expected,
                actual,
            } => write!(
                f,
                "CAS digest mismatch for {object_ref}: expected {expected}, actual {actual}"
            ),
            Self::ObjectConflict { object_ref } => {
                write!(
                    f,
                    "CAS object already exists with different content: {object_ref}"
                )
            }
            Self::ReparsePoint { path } => write!(f, "reparse point forbidden: {}", path.display()),
            Self::Io { path, source } => write!(f, "CAS io error at {}: {source}", path.display()),
        }
    }
}

impl std::error::Error for CasError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

impl CasError {
    #[must_use]
    pub fn code_str(&self) -> &'static str {
        match self {
            Self::ObjectRefInvalid { .. } => "CAS_OBJECT_REF_INVALID",
            Self::HashMismatch { .. } => "CAS_HASH_MISMATCH",
            Self::ObjectConflict { .. } => "CAS_OBJECT_CONFLICT",
            Self::ReparsePoint { .. } => "BRIEF_REPARSE_POINT",
            Self::Io { .. } => "CAS_IO",
        }
    }
}

/// `sha256:<64 lowercase hex>` object reference.
#[must_use]
pub fn object_ref_from_sha256(sha256: &str) -> String {
    format!("sha256:{sha256}")
}

#[must_use]
pub fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn parse_object_ref(object_ref: &str) -> Option<&str> {
    let hex = object_ref.strip_prefix("sha256:")?;
    if hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Some(hex)
    } else {
        None
    }
}

#[derive(Clone, Debug)]
pub struct CoreCas {
    project_root: PathBuf,
}

impl CoreCas {
    #[must_use]
    pub fn new(project_root: impl Into<PathBuf>) -> Self {
        Self {
            project_root: project_root.into(),
        }
    }

    #[must_use]
    pub fn project_root(&self) -> &Path {
        &self.project_root
    }

    #[must_use]
    pub fn cas_root(&self) -> PathBuf {
        self.project_root.join(CAS_DIR_NAME)
    }

    #[must_use]
    pub fn objects_root(&self) -> PathBuf {
        self.cas_root().join(OBJECTS_DIR_NAME)
    }

    fn objects_handle(&self) -> Result<std::fs::File, CasError> {
        let root =
            fs_guard::open_root_handle(&self.project_root).map_err(|source| CasError::Io {
                path: self.project_root.clone(),
                source,
            })?;
        if fs_guard::handle_is_reparse(&root).map_err(|source| CasError::Io {
            path: self.project_root.clone(),
            source,
        })? {
            return Err(CasError::ReparsePoint {
                path: self.project_root.clone(),
            });
        }
        let agenttalk = fs_guard::open_or_create_directory_relative(&root, CAS_DIR_NAME)
            .map_err(|source| self.traversal_error(Path::new(CAS_DIR_NAME), source))?;
        if fs_guard::handle_is_reparse(&agenttalk).map_err(|source| CasError::Io {
            path: self.cas_root(),
            source,
        })? {
            return Err(CasError::ReparsePoint {
                path: self.cas_root(),
            });
        }
        let objects = fs_guard::open_or_create_directory_relative(&agenttalk, OBJECTS_DIR_NAME)
            .map_err(|source| {
                self.traversal_error(&Path::new(CAS_DIR_NAME).join(OBJECTS_DIR_NAME), source)
            })?;
        if fs_guard::handle_is_reparse(&objects).map_err(|source| CasError::Io {
            path: self.objects_root(),
            source,
        })? {
            return Err(CasError::ReparsePoint {
                path: self.objects_root(),
            });
        }
        Ok(objects)
    }

    fn traversal_error(&self, relative: &Path, source: std::io::Error) -> CasError {
        if source.to_string().contains("reparse point forbidden") {
            CasError::ReparsePoint {
                path: self.project_root.join(relative),
            }
        } else {
            CasError::Io {
                path: self.project_root.join(relative),
                source,
            }
        }
    }

    fn open_existing_object(
        &self,
        objects: &std::fs::File,
        name: &str,
    ) -> Result<std::fs::File, CasError> {
        fs_guard::open_relative_components(objects, Path::new(name), false).map_err(|source| {
            self.traversal_error(
                &Path::new(CAS_DIR_NAME).join(OBJECTS_DIR_NAME).join(name),
                source,
            )
        })
    }

    fn flush_objects_handle(&self, objects: &std::fs::File) -> Result<(), CasError> {
        fs_guard::flush_directory_handle(objects).map_err(|source| CasError::Io {
            path: self.objects_root(),
            source,
        })
    }

    fn delete_temp_checked(&self, temp: &std::fs::File, temp_name: &str) -> Result<(), CasError> {
        if crate::test_support::should_fail_temp_delete() {
            return Err(CasError::Io {
                path: self.objects_root().join(temp_name),
                source: std::io::Error::other("injected temp delete failure"),
            });
        }
        fs_guard::delete_file_by_handle(temp).map_err(|source| CasError::Io {
            path: self.objects_root().join(temp_name),
            source,
        })
    }

    fn cleanup_temp_best_effort(&self, temp: &std::fs::File) {
        let _ = fs_guard::delete_file_by_handle(temp);
    }

    pub fn ensure_objects_root(&self) -> Result<(), CasError> {
        let _ = self.objects_handle()?;
        Ok(())
    }
    pub fn publish(&self, bytes: &[u8]) -> Result<CasObject, CasError> {
        let objects = self.objects_handle()?;
        let sha256 = sha256_hex(bytes);
        let object_ref = object_ref_from_sha256(&sha256);
        let object_name = format!("{sha256}.blob");

        match self.open_existing_object(&objects, &object_name) {
            Ok(mut file) => {
                if fs_guard::handle_is_reparse(&file).map_err(|source| CasError::Io {
                    path: self.objects_root().join(&object_name),
                    source,
                })? {
                    return Err(CasError::ReparsePoint {
                        path: self.objects_root().join(object_name),
                    });
                }
                let mut existing = Vec::new();
                file.read_to_end(&mut existing)
                    .map_err(|source| CasError::Io {
                        path: self.objects_root().join(&object_name),
                        source,
                    })?;
                if existing != bytes {
                    return Err(CasError::ObjectConflict { object_ref });
                }
                self.flush_objects_handle(&objects)?;
                return Ok(CasObject {
                    object_ref,
                    sha256,
                    size: bytes.len() as u64,
                });
            }
            Err(error) => match error {
                CasError::Io { source, .. } if source.kind() == std::io::ErrorKind::NotFound => {}
                other => return Err(other),
            },
        }

        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|source| CasError::Io {
                path: self.objects_root(),
                source: std::io::Error::other(source),
            })?
            .as_nanos();
        let temp_name = format!(".{sha256}.{}.{nonce}.tmp", std::process::id());
        let mut temp =
            fs_guard::create_file_relative_new(&objects, &temp_name).map_err(|source| {
                self.traversal_error(
                    &Path::new(CAS_DIR_NAME)
                        .join(OBJECTS_DIR_NAME)
                        .join(&temp_name),
                    source,
                )
            })?;
        let write_result = (|| -> Result<(), std::io::Error> {
            temp.write_all(bytes)?;
            fs_guard::flush_file_handle(&temp)?;
            Ok(())
        })();
        if let Err(source) = write_result {
            self.cleanup_temp_best_effort(&temp);
            return Err(CasError::Io {
                path: self.objects_root().join(&temp_name),
                source,
            });
        }

        match fs_guard::link_file_relative(&objects, &temp, &object_name) {
            Ok(()) => {
                if let Err(error) = self.delete_temp_checked(&temp, &temp_name) {
                    let _ = self.flush_objects_handle(&objects);
                    return Err(error);
                }
                self.flush_objects_handle(&objects)?;
                Ok(CasObject {
                    object_ref,
                    sha256,
                    size: bytes.len() as u64,
                })
            }
            Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => {
                self.delete_temp_checked(&temp, &temp_name)?;
                let mut existing_file = self.open_existing_object(&objects, &object_name)?;
                if fs_guard::handle_is_reparse(&existing_file).map_err(|error| CasError::Io {
                    path: self.objects_root().join(&object_name),
                    source: error,
                })? {
                    return Err(CasError::ReparsePoint {
                        path: self.objects_root().join(object_name),
                    });
                }
                let mut existing = Vec::new();
                existing_file
                    .read_to_end(&mut existing)
                    .map_err(|error| CasError::Io {
                        path: self.objects_root().join(&object_name),
                        source: error,
                    })?;
                if existing != bytes {
                    return Err(CasError::ObjectConflict { object_ref });
                }
                self.flush_objects_handle(&objects)?;
                Ok(CasObject {
                    object_ref,
                    sha256,
                    size: bytes.len() as u64,
                })
            }
            Err(source) => {
                self.cleanup_temp_best_effort(&temp);
                Err(self.traversal_error(
                    &Path::new(CAS_DIR_NAME)
                        .join(OBJECTS_DIR_NAME)
                        .join(&object_name),
                    source,
                ))
            }
        }
    }

    pub fn read(&self, object_ref: &str) -> Result<Vec<u8>, CasError> {
        let sha256 = parse_object_ref(object_ref).ok_or_else(|| CasError::ObjectRefInvalid {
            object_ref: object_ref.to_owned(),
        })?;
        let objects = self.objects_handle()?;
        let object_name = format!("{sha256}.blob");
        let mut file = self.open_existing_object(&objects, &object_name)?;
        if fs_guard::handle_is_reparse(&file).map_err(|source| CasError::Io {
            path: self.objects_root().join(&object_name),
            source,
        })? {
            return Err(CasError::ReparsePoint {
                path: self.objects_root().join(object_name),
            });
        }
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .map_err(|source| CasError::Io {
                path: self.objects_root().join(&object_name),
                source,
            })?;
        let actual = sha256_hex(&bytes);
        if actual != sha256 {
            return Err(CasError::HashMismatch {
                object_ref: object_ref.to_owned(),
                expected: sha256.to_owned(),
                actual,
            });
        }
        Ok(bytes)
    }

    /// Return the on-disk path for a valid object reference. Does not verify
    /// existence.
    #[must_use]
    pub fn object_path(&self, object_ref: &str) -> PathBuf {
        let sha256 = parse_object_ref(object_ref).unwrap_or(object_ref);
        self.objects_root().join(format!("{sha256}.blob"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs_guard;
    use std::fs;

    #[test]
    fn flush_directory_handle_succeeds_or_reports_blocked() {
        let root = std::env::temp_dir().join(format!(
            "agenttalk-cas-flush-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(root.join(".agenttalk").join("objects")).unwrap();

        let root_handle = fs_guard::open_root_handle(&root).unwrap();
        fs_guard::flush_directory_relative(
            &root_handle,
            &Path::new(CAS_DIR_NAME).join(OBJECTS_DIR_NAME),
        )
        .unwrap();
        fs::remove_dir_all(&root).unwrap();
    }
}
