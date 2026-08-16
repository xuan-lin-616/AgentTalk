//! Filesystem brief sealer.
//!
//! The sealer owns the four filesystem-plane steps of ADR-001 §2. It
//! validates the frozen root manifest, checks Windows physical safety, reads
//! every accepted source from its final open handle, publishes manifest and
//! source bytes into the Core-owned CAS, and then computes the frozen
//! `briefTreeDigest` through the contracts crate.
//!
//! It intentionally does **not** create an OrchestrationRun. That step
//! belongs to the orchestration journal (C4 migration/storage work package).

use crate::cas::{CasObject, CoreCas};
use crate::error::BriefSealError;
use crate::fs_guard::{self, FileIdentity};
use agenttalk_orchestration_contracts::brief::{
    BriefBytesMap, ParsedManifest, ShapeValidatedManifest,
};
use agenttalk_orchestration_contracts::registry::SchemaRegistry;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

const ROOT_MANIFEST_NAME: &str = "agenttalk-brief.json";

/// A single authoring file that has been sealed into the CAS.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SealedBriefFile {
    path: String,
    kind: String,
    format: String,
    required: bool,
    declared_sha256: String,
    size: u64,
    object_ref: String,
    context: Value,
    declared_owner_role_id: String,
}

impl SealedBriefFile {
    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }

    #[must_use]
    pub fn kind(&self) -> &str {
        &self.kind
    }

    #[must_use]
    pub fn format(&self) -> &str {
        &self.format
    }

    #[must_use]
    pub const fn required(&self) -> bool {
        self.required
    }

    #[must_use]
    pub fn declared_sha256(&self) -> &str {
        &self.declared_sha256
    }

    #[must_use]
    pub const fn size(&self) -> u64 {
        self.size
    }

    #[must_use]
    pub fn object_ref(&self) -> &str {
        &self.object_ref
    }

    #[must_use]
    pub fn context(&self) -> &Value {
        &self.context
    }

    #[must_use]
    pub fn declared_owner_role_id(&self) -> &str {
        &self.declared_owner_role_id
    }
}

/// Immutable seal product. This is **not** an OrchestrationRun and must not
/// be accepted by the scheduler.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedBriefSeal {
    brief_snapshot_id: String,
    brief_tree_digest: String,
    manifest_object_ref: String,
    manifest_size: u64,
    files: Vec<SealedBriefFile>,
    canonical_tree_record: Vec<u8>,
}

impl PreparedBriefSeal {
    #[must_use]
    pub fn brief_snapshot_id(&self) -> &str {
        &self.brief_snapshot_id
    }

    #[must_use]
    pub fn brief_tree_digest(&self) -> &str {
        &self.brief_tree_digest
    }

    #[must_use]
    pub fn manifest_object_ref(&self) -> &str {
        &self.manifest_object_ref
    }

    #[must_use]
    pub const fn manifest_size(&self) -> u64 {
        self.manifest_size
    }

    #[must_use]
    pub fn files(&self) -> &[SealedBriefFile] {
        &self.files
    }

    #[must_use]
    pub fn canonical_tree_record(&self) -> &[u8] {
        &self.canonical_tree_record
    }
}

/// Filesystem brief sealer.
#[derive(Clone, Debug)]
pub struct BriefSealer {
    project_root: PathBuf,
    cas: CoreCas,
}

impl BriefSealer {
    #[must_use]
    pub fn new(project_root: impl Into<PathBuf>) -> Self {
        let project_root = project_root.into();
        let cas = CoreCas::new(project_root.clone());
        Self { project_root, cas }
    }

    #[must_use]
    pub fn project_root(&self) -> &Path {
        &self.project_root
    }

    #[must_use]
    pub fn cas(&self) -> &CoreCas {
        &self.cas
    }

    /// Execute the filesystem-side brief seal. The function is synchronous
    /// and has no journal side effects.
    pub fn seal(
        &self,
        schema_registry: &dyn SchemaRegistry,
    ) -> Result<PreparedBriefSeal, BriefSealError> {
        self.cas.ensure_objects_root()?;

        let manifest_path = self.project_root.join(ROOT_MANIFEST_NAME);
        fs_guard::ensure_no_reparse_for_path(&self.project_root, &manifest_path).map_err(
            |source| BriefSealError::Io {
                path: manifest_path.clone(),
                source,
            },
        )?;
        let manifest_bytes = read_same_handle(&manifest_path, None)?;

        let parsed = ParsedManifest::parse(&manifest_bytes)?;
        let shape = parsed.validate_shape()?;

        let mut seen_ids = HashSet::new();
        let manifest_id = identity_for_path(&manifest_path)?;
        seen_ids.insert(manifest_id);

        let files_value = shape
            .as_value()
            .get("files")
            .and_then(Value::as_array)
            .cloned()
            .expect("shape validation guarantees files is an array");

        let mut bytes_map = SealerBytesMap::new();
        let mut sealed_files = Vec::with_capacity(files_value.len());

        for file_value in files_value {
            let path = file_value
                .get("path")
                .and_then(Value::as_str)
                .expect("shape validation guarantees file.path");
            let source_path = self.project_root.join(path);
            fs_guard::ensure_no_reparse_for_path(&self.project_root, &source_path).map_err(
                |source| BriefSealError::Io {
                    path: source_path.clone(),
                    source,
                },
            )?;

            let source_id = identity_for_path(&source_path)?;
            if seen_ids.contains(&source_id) {
                return Err(BriefSealError::PhysicalAlias {
                    path: path.to_owned(),
                });
            }
            seen_ids.insert(source_id);

            let bytes = read_same_handle(&source_path, Some(&source_id))?;
            let sha256 = crate::cas::sha256_hex(&bytes);
            let size = bytes.len() as u64;
            let object = self.cas.publish(&bytes)?;

            bytes_map.insert(path.to_owned(), bytes);
            sealed_files.push(SealedBriefFile {
                path: path.to_owned(),
                kind: file_value
                    .get("kind")
                    .and_then(Value::as_str)
                    .expect("shape validation guarantees file.kind")
                    .to_owned(),
                format: file_value
                    .get("format")
                    .and_then(Value::as_str)
                    .expect("shape validation guarantees file.format")
                    .to_owned(),
                required: file_value
                    .get("required")
                    .and_then(Value::as_bool)
                    .expect("shape validation guarantees file.required"),
                declared_sha256: file_value
                    .get("sha256")
                    .and_then(Value::as_str)
                    .expect("shape validation guarantees file.sha256")
                    .to_owned(),
                size,
                object_ref: object.object_ref,
                context: file_value
                    .get("context")
                    .cloned()
                    .expect("shape validation guarantees file.context"),
                declared_owner_role_id: file_value
                    .get("declaredOwnerRoleId")
                    .and_then(Value::as_str)
                    .expect("shape validation guarantees file.declaredOwnerRoleId")
                    .to_owned(),
            });
            let _ = sha256; // Manifest-declared hash is re-verified below.
        }

        let content = shape.validate_content(schema_registry, &bytes_map)?;

        let manifest_object = self.cas.publish(&manifest_bytes)?;
        let brief_tree_digest = content.brief_tree_digest().to_owned();
        let brief_snapshot_id = format!("brief-snapshot-{brief_tree_digest}");

        Ok(PreparedBriefSeal {
            brief_snapshot_id,
            brief_tree_digest,
            manifest_object_ref: manifest_object.object_ref,
            manifest_size: manifest_object.size,
            files: sealed_files,
            canonical_tree_record: content.canonical_tree_record_bytes().to_vec(),
        })
    }
}

fn identity_for_path(path: &Path) -> Result<FileIdentity, BriefSealError> {
    let file = File::open(path).map_err(|source| BriefSealError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let identity = fs_guard::identity_from_open_file(&file)
        .map_err(|source| BriefSealError::Io {
            path: path.to_path_buf(),
            source,
        })?
        .ok_or_else(|| BriefSealError::Io {
            path: path.to_path_buf(),
            source: std::io::Error::other("file identity is unavailable on this platform"),
        })?;
    Ok(identity)
}

fn read_same_handle(
    path: &Path,
    expected_identity: Option<&FileIdentity>,
) -> Result<Vec<u8>, BriefSealError> {
    let mut file = File::open(path).map_err(|source| BriefSealError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let before = fs_guard::identity_from_open_file(&file)
        .map_err(|source| BriefSealError::Io {
            path: path.to_path_buf(),
            source,
        })?
        .ok_or_else(|| BriefSealError::Io {
            path: path.to_path_buf(),
            source: std::io::Error::other("file identity is unavailable on this platform"),
        })?;
    if let Some(expected) = expected_identity {
        if expected != &before {
            return Err(BriefSealError::SourceHandleChanged {
                path: path.to_string_lossy().into_owned(),
            });
        }
    }

    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|source| BriefSealError::Io {
            path: path.to_path_buf(),
            source,
        })?;

    let after = fs_guard::identity_from_open_file(&file)
        .map_err(|source| BriefSealError::Io {
            path: path.to_path_buf(),
            source,
        })?
        .ok_or_else(|| BriefSealError::Io {
            path: path.to_path_buf(),
            source: std::io::Error::other("file identity is unavailable on this platform"),
        })?;
    if before != after {
        return Err(BriefSealError::SourceHandleChanged {
            path: path.to_string_lossy().into_owned(),
        });
    }
    Ok(bytes)
}

struct SealerBytesMap {
    entries: HashMap<String, Vec<u8>>,
}

impl SealerBytesMap {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    fn insert(&mut self, path: String, bytes: Vec<u8>) {
        self.entries.insert(path, bytes);
    }
}

impl BriefBytesMap for SealerBytesMap {
    fn get(&self, path: &str) -> Option<&[u8]> {
        self.entries.get(path).map(Vec::as_slice)
    }
}

// Keep the manifest type visible for compile-time checks in tests and future
// journal work.
#[allow(dead_code)]
type ManifestShape = ShapeValidatedManifest;
#[allow(dead_code)]
type ManifestObject = CasObject;
