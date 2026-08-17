//! Core-owned content-addressed store at `<project_root>/.agenttalk/objects/`.
//!
//! The store is intentionally tiny and authority-free: it only publishes and
//! verifies digest-addressed blobs. Journal/snapshot references belong to the
//! orchestration journal, not to this module.

use crate::fs_guard;
use sha2::{Digest, Sha256};
use std::fmt::{Display, Formatter};
use std::fs::{self, OpenOptions};
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

    /// Ensure the CAS root and objects root exist without following reparse
    /// points. Call this before any publish.
    pub fn ensure_objects_root(&self) -> Result<(), CasError> {
        fs_guard::ensure_no_reparse_components(&self.project_root, &self.cas_root()).map_err(
            |source| CasError::Io {
                path: self.cas_root(),
                source,
            },
        )?;
        fs::create_dir_all(self.objects_root()).map_err(|source| CasError::Io {
            path: self.objects_root(),
            source,
        })?;
        fs_guard::ensure_no_reparse_components(&self.project_root, &self.objects_root()).map_err(
            |source| CasError::Io {
                path: self.objects_root(),
                source,
            },
        )?;
        Ok(())
    }

    /// Atomic, no-replace publish of exact bytes. Re-publishing the same
    /// bytes is idempotent and does not create a second object.
    ///
    /// Windows directory-entry flush is not implemented here, so this method
    /// must be described as atomic publish rather than full power-loss durable
    /// publish. The temporary file is `sync_all`ed and linked into place, but
    /// the containing directory entry is not separately flushed.
    pub fn publish(&self, bytes: &[u8]) -> Result<CasObject, CasError> {
        self.ensure_objects_root()?;
        let sha256 = sha256_hex(bytes);
        let object_ref = object_ref_from_sha256(&sha256);
        let destination = self.object_path(&object_ref);

        match fs_guard::open_no_follow(&destination) {
            Ok(mut file) => {
                if fs_guard::handle_is_reparse(&file).map_err(|source| CasError::Io {
                    path: destination.clone(),
                    source,
                })? {
                    return Err(CasError::ReparsePoint { path: destination });
                }
                let mut existing = Vec::new();
                file.read_to_end(&mut existing)
                    .map_err(|source| CasError::Io {
                        path: destination.clone(),
                        source,
                    })?;
                if existing == bytes {
                    return Ok(CasObject {
                        object_ref,
                        sha256,
                        size: bytes.len() as u64,
                    });
                }
                return Err(CasError::ObjectConflict { object_ref });
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(CasError::Io {
                    path: destination,
                    source,
                });
            }
        }

        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|source| CasError::Io {
                path: self.objects_root(),
                source: std::io::Error::other(source),
            })?
            .as_nanos();
        let temporary = self
            .objects_root()
            .join(format!(".{sha256}.{}.{nonce}.tmp", std::process::id()));

        let write_result = (|| -> Result<(), std::io::Error> {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)?;
            file.write_all(bytes)?;
            file.sync_all()?;
            Ok(())
        })();
        if let Err(source) = write_result {
            let _ = fs::remove_file(&temporary);
            return Err(CasError::Io {
                path: temporary,
                source,
            });
        }

        match fs::hard_link(&temporary, &destination) {
            Ok(()) => {
                let _ = fs::remove_file(&temporary);
                Ok(CasObject {
                    object_ref,
                    sha256,
                    size: bytes.len() as u64,
                })
            }
            Err(source)
                if source.kind() == std::io::ErrorKind::AlreadyExists
                    || source.raw_os_error() == Some(80) =>
            {
                let _ = fs::remove_file(&temporary);
                let mut existing_file =
                    fs_guard::open_no_follow(&destination).map_err(|error| CasError::Io {
                        path: destination.clone(),
                        source: error,
                    })?;
                if fs_guard::handle_is_reparse(&existing_file).map_err(|error| CasError::Io {
                    path: destination.clone(),
                    source: error,
                })? {
                    return Err(CasError::ReparsePoint { path: destination });
                }
                let mut existing = Vec::new();
                existing_file
                    .read_to_end(&mut existing)
                    .map_err(|error| CasError::Io {
                        path: destination,
                        source: error,
                    })?;
                if existing == bytes {
                    Ok(CasObject {
                        object_ref,
                        sha256,
                        size: bytes.len() as u64,
                    })
                } else {
                    Err(CasError::ObjectConflict { object_ref })
                }
            }
            Err(source) => {
                let _ = fs::remove_file(&temporary);
                Err(CasError::Io {
                    path: destination,
                    source,
                })
            }
        }
    }

    /// Read an object and fail closed when the bytes no longer match the
    /// digest embedded in the object reference.
    pub fn read(&self, object_ref: &str) -> Result<Vec<u8>, CasError> {
        let sha256 = parse_object_ref(object_ref).ok_or_else(|| CasError::ObjectRefInvalid {
            object_ref: object_ref.to_owned(),
        })?;
        let path = self.object_path(object_ref);
        let mut file = fs_guard::open_no_follow(&path).map_err(|source| CasError::Io {
            path: path.clone(),
            source,
        })?;
        if fs_guard::handle_is_reparse(&file).map_err(|source| CasError::Io {
            path: path.clone(),
            source,
        })? {
            return Err(CasError::ReparsePoint { path });
        }
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .map_err(|source| CasError::Io { path, source })?;
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
