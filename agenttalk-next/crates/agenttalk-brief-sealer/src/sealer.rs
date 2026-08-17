//! Filesystem brief sealer.
//!
//! The sealer owns the four filesystem-plane steps of ADR-001 §2 plus the
//! CAS snapshot descriptor. It validates the frozen root manifest, checks
//! Windows physical safety from final open handles, reads every accepted
//! source from its final handle, publishes manifest/source/schema bytes into
//! the Core-owned CAS, and computes the frozen `briefTreeDigest` through the
//! contracts crate.
//!
//! It intentionally does **not** create an OrchestrationRun. That step
//! belongs to the orchestration journal (C4 migration/storage work package).

use crate::cas::{sha256_hex, CasObject, CoreCas};
use crate::error::BriefSealError;
use crate::fs_guard::{self, FileIdentity};
use agenttalk_orchestration_contracts::brief::{BriefBytesMap, ParsedManifest};
use agenttalk_orchestration_contracts::error::ErrorCode;
use agenttalk_orchestration_contracts::json::{self, utf16_order};
use agenttalk_orchestration_contracts::registry::{SchemaReference, SchemaRegistry};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

const ROOT_MANIFEST_NAME: &str = "agenttalk-brief.json";
const SNAPSHOT_DESCRIPTOR_SCHEMA: &str = "agenttalk.brief.snapshot.v1";

/// A single authoring file that has been sealed into the CAS.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SealedBriefFile {
    path: String,
    kind: String,
    format: String,
    content_schema_ref: Value,
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
    pub fn content_schema_ref(&self) -> &Value {
        &self.content_schema_ref
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

/// A schema registry entry sealed into the snapshot descriptor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SealedSchemaRef {
    id: String,
    version: String,
    digest: String,
    canonical_schema_object_ref: String,
}

impl SealedSchemaRef {
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }
    #[must_use]
    pub fn version(&self) -> &str {
        &self.version
    }
    #[must_use]
    pub fn digest(&self) -> &str {
        &self.digest
    }
    #[must_use]
    pub fn canonical_schema_object_ref(&self) -> &str {
        &self.canonical_schema_object_ref
    }
}

/// Parsed snapshot descriptor read back from the CAS.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BriefSnapshotDescriptor {
    brief_tree_digest: String,
    manifest_object_ref: String,
    manifest_raw_sha256: String,
    manifest_size: u64,
    files: Vec<SealedBriefFile>,
    schemas: Vec<SealedSchemaRef>,
}

impl BriefSnapshotDescriptor {
    #[must_use]
    pub fn brief_tree_digest(&self) -> &str {
        &self.brief_tree_digest
    }
    #[must_use]
    pub fn manifest_object_ref(&self) -> &str {
        &self.manifest_object_ref
    }
    #[must_use]
    pub fn manifest_raw_sha256(&self) -> &str {
        &self.manifest_raw_sha256
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
    pub fn schemas(&self) -> &[SealedSchemaRef] {
        &self.schemas
    }
}

/// Immutable seal product. This is **not** an OrchestrationRun and must not
/// be accepted by the scheduler.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedBriefSeal {
    brief_snapshot_id: String,
    brief_tree_digest: String,
    descriptor_object_ref: String,
    descriptor_raw_sha256: String,
    descriptor_size: u64,
    manifest_object_ref: String,
    manifest_size: u64,
    files: Vec<SealedBriefFile>,
    schemas: Vec<SealedSchemaRef>,
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
    pub fn descriptor_object_ref(&self) -> &str {
        &self.descriptor_object_ref
    }
    #[must_use]
    pub fn descriptor_raw_sha256(&self) -> &str {
        &self.descriptor_raw_sha256
    }
    #[must_use]
    pub const fn descriptor_size(&self) -> u64 {
        self.descriptor_size
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
    pub fn schemas(&self) -> &[SealedSchemaRef] {
        &self.schemas
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

struct OpenedSafe {
    file: File,
    identity: FileIdentity,
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

    pub fn seal(
        &self,
        schema_registry: &dyn SchemaRegistry,
    ) -> Result<PreparedBriefSeal, BriefSealError> {
        self.cas.ensure_objects_root()?;
        let root_handle = fs_guard::open_root_handle(&self.project_root).map_err(|source| {
            BriefSealError::Io {
                path: self.project_root.clone(),
                source,
            }
        })?;
        if fs_guard::handle_is_reparse(&root_handle).map_err(|source| BriefSealError::Io {
            path: self.project_root.clone(),
            source,
        })? {
            return Err(BriefSealError::ReparsePoint {
                path: self.project_root.clone(),
            });
        }

        let manifest_path = self.project_root.join(ROOT_MANIFEST_NAME);
        let manifest = self.open_safe(&root_handle, Path::new(ROOT_MANIFEST_NAME))?;
        let mut manifest_bytes = Vec::new();
        read_handle(&manifest.file, &mut manifest_bytes, &manifest_path)?;

        let parsed = ParsedManifest::parse(&manifest_bytes)?;
        let shape = parsed.validate_shape()?;

        let mut seen_ids = HashSet::new();
        seen_ids.insert(manifest.identity);

        let files_value = shape
            .as_value()
            .get("files")
            .and_then(Value::as_array)
            .cloned()
            .expect("shape validation guarantees files is an array");

        let mut bytes_map = SealerBytesMap::new();
        let mut sealed_files = Vec::with_capacity(files_value.len());
        let mut sealed_schemas = Vec::new();

        for file_value in &files_value {
            if let Some(reference) = schema_reference_for_file(file_value)? {
                let descriptor = schema_registry.resolve(&reference).ok_or_else(|| {
                    BriefSealError::Contract(agenttalk_orchestration_contracts::ContractError::new(
                        ErrorCode::BriefSchemaRefUnresolved,
                        format!(
                            "contentSchemaRef {} version {} digest {} is not resolvable",
                            reference.id, reference.version, reference.digest
                        ),
                    ))
                })?;
                let schema_object = self.cas.publish(descriptor.canonical_bytes())?;
                sealed_schemas.push(SealedSchemaRef {
                    id: reference.id.clone(),
                    version: reference.version.clone(),
                    digest: reference.digest.clone(),
                    canonical_schema_object_ref: schema_object.object_ref,
                });
            }
        }

        for file_value in files_value {
            let path = file_value
                .get("path")
                .and_then(Value::as_str)
                .expect("shape validation guarantees file.path");
            let source_path = self.project_root.join(path);
            let opened = self.open_safe(&root_handle, Path::new(path))?;
            if seen_ids.contains(&opened.identity) {
                return Err(BriefSealError::PhysicalAlias {
                    path: path.to_owned(),
                });
            }
            seen_ids.insert(opened.identity);

            let mut bytes = Vec::new();
            read_handle(&opened.file, &mut bytes, &source_path)?;
            let after_identity = fs_guard::identity_from_open_file(&opened.file)
                .map_err(|source| BriefSealError::Io {
                    path: source_path.clone(),
                    source,
                })?
                .ok_or_else(|| BriefSealError::Io {
                    path: source_path.clone(),
                    source: std::io::Error::other("file identity is unavailable on this platform"),
                })?;
            if after_identity != opened.identity {
                return Err(BriefSealError::SourceHandleChanged {
                    path: path.to_owned(),
                });
            }

            let object = self.cas.publish(&bytes)?;
            bytes_map.insert(path.to_owned(), bytes.clone());
            sealed_files.push(SealedBriefFile {
                path: path.to_owned(),
                kind: file_value
                    .get("kind")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                format: file_value
                    .get("format")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                content_schema_ref: file_value
                    .get("contentSchemaRef")
                    .cloned()
                    .unwrap_or(Value::Null),
                required: file_value
                    .get("required")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                declared_sha256: file_value
                    .get("sha256")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                size: bytes.len() as u64,
                object_ref: object.object_ref,
                context: file_value.get("context").cloned().unwrap_or(Value::Null),
                declared_owner_role_id: file_value
                    .get("declaredOwnerRoleId")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
            });
        }

        let content = shape.validate_content(schema_registry, &bytes_map)?;
        let manifest_object = self.cas.publish(&manifest_bytes)?;
        let descriptor_value =
            build_snapshot_descriptor(&content, &manifest_object, &sealed_files, &sealed_schemas)?;
        let descriptor_canonical = json::canonicalize(&descriptor_value).map_err(|error| {
            BriefSealError::Contract(agenttalk_orchestration_contracts::ContractError::new(
                ErrorCode::BriefCanonicalEncoding,
                error.to_string(),
            ))
        })?;
        let descriptor_object = self.cas.publish(&descriptor_canonical)?;
        let brief_tree_digest = content.brief_tree_digest().to_owned();
        let brief_snapshot_id = descriptor_object.object_ref.clone();

        Ok(PreparedBriefSeal {
            brief_snapshot_id,
            brief_tree_digest,
            descriptor_object_ref: descriptor_object.object_ref,
            descriptor_raw_sha256: descriptor_object.sha256,
            descriptor_size: descriptor_object.size,
            manifest_object_ref: manifest_object.object_ref,
            manifest_size: manifest_object.size,
            files: sealed_files,
            schemas: sealed_schemas,
            canonical_tree_record: content.canonical_tree_record_bytes().to_vec(),
        })
    }
    pub fn read_snapshot_descriptor(
        &self,
        brief_snapshot_id: &str,
    ) -> Result<BriefSnapshotDescriptor, BriefSealError> {
        let bytes = self.cas.read(brief_snapshot_id)?;
        let value = json::parse_duplicate_safe(&bytes).map_err(|error| {
            BriefSealError::Contract(agenttalk_orchestration_contracts::ContractError::new(
                ErrorCode::BriefSchemaViolation,
                error.to_string(),
            ))
        })?;
        let schema_version = value
            .get("schemaVersion")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                BriefSealError::Contract(agenttalk_orchestration_contracts::ContractError::new(
                    ErrorCode::BriefSchemaViolation,
                    "snapshot descriptor is missing schemaVersion",
                ))
            })?;
        if schema_version != SNAPSHOT_DESCRIPTOR_SCHEMA {
            return Err(BriefSealError::Contract(
                agenttalk_orchestration_contracts::ContractError::new(
                    ErrorCode::BriefSchemaViolation,
                    "snapshot descriptor schemaVersion mismatch",
                ),
            ));
        }
        let brief_tree_digest = value
            .get("briefTreeDigest")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let manifest = value.get("manifest").ok_or_else(|| {
            BriefSealError::Contract(agenttalk_orchestration_contracts::ContractError::new(
                ErrorCode::BriefSchemaViolation,
                "snapshot descriptor is missing manifest",
            ))
        })?;
        let manifest_object_ref = manifest
            .get("objectRef")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let manifest_raw_sha256 = manifest
            .get("rawSha256")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let manifest_size = manifest
            .get("size")
            .and_then(Value::as_u64)
            .unwrap_or_default();

        let files = value
            .get("files")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                BriefSealError::Contract(agenttalk_orchestration_contracts::ContractError::new(
                    ErrorCode::BriefSchemaViolation,
                    "snapshot descriptor is missing files",
                ))
            })?;
        let mut sealed_files = Vec::with_capacity(files.len());
        for file in files {
            sealed_files.push(SealedBriefFile {
                path: file
                    .get("path")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                kind: file
                    .get("kind")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                format: file
                    .get("format")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                content_schema_ref: file.get("contentSchemaRef").cloned().unwrap_or(Value::Null),
                required: file
                    .get("required")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                declared_sha256: file
                    .get("rawSha256")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                size: file.get("size").and_then(Value::as_u64).unwrap_or_default(),
                object_ref: file
                    .get("objectRef")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                context: file.get("context").cloned().unwrap_or(Value::Null),
                declared_owner_role_id: file
                    .get("declaredOwnerRoleId")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
            });
        }

        let schemas = value
            .get("schemas")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                BriefSealError::Contract(agenttalk_orchestration_contracts::ContractError::new(
                    ErrorCode::BriefSchemaViolation,
                    "snapshot descriptor is missing schemas",
                ))
            })?;
        let mut sealed_schemas = Vec::with_capacity(schemas.len());
        for schema in schemas {
            sealed_schemas.push(SealedSchemaRef {
                id: schema
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                version: schema
                    .get("version")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                digest: schema
                    .get("digest")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                canonical_schema_object_ref: schema
                    .get("canonicalSchemaObjectRef")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
            });
        }

        Ok(BriefSnapshotDescriptor {
            brief_tree_digest,
            manifest_object_ref,
            manifest_raw_sha256,
            manifest_size,
            files: sealed_files,
            schemas: sealed_schemas,
        })
    }

    fn open_safe(&self, root_handle: &File, relative: &Path) -> Result<OpenedSafe, BriefSealError> {
        let file =
            fs_guard::open_relative_components(root_handle, relative, false).map_err(|source| {
                if source.to_string().contains("reparse point forbidden") {
                    BriefSealError::ReparsePoint {
                        path: self.project_root.join(relative),
                    }
                } else {
                    BriefSealError::Io {
                        path: self.project_root.join(relative),
                        source,
                    }
                }
            })?;
        if fs_guard::handle_is_reparse(&file).map_err(|source| BriefSealError::Io {
            path: self.project_root.join(relative),
            source,
        })? {
            return Err(BriefSealError::ReparsePoint {
                path: self.project_root.join(relative),
            });
        }
        let identity = fs_guard::identity_from_open_file(&file)
            .map_err(|source| BriefSealError::Io {
                path: self.project_root.join(relative),
                source,
            })?
            .ok_or_else(|| BriefSealError::Io {
                path: self.project_root.join(relative),
                source: std::io::Error::other("file identity is unavailable on this platform"),
            })?;
        Ok(OpenedSafe { file, identity })
    }
}
fn read_handle(file: &File, bytes: &mut Vec<u8>, path: &Path) -> Result<(), BriefSealError> {
    let mut handle = file;
    handle
        .read_to_end(bytes)
        .map_err(|source| BriefSealError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    Ok(())
}

fn schema_reference_for_file(file: &Value) -> Result<Option<SchemaReference>, BriefSealError> {
    let value = file
        .get("contentSchemaRef")
        .expect("shape validation guarantees contentSchemaRef");
    if value.is_null() {
        return Ok(None);
    }
    let object = value.as_object().ok_or_else(|| {
        BriefSealError::Contract(agenttalk_orchestration_contracts::ContractError::new(
            ErrorCode::BriefSchemaViolation,
            "contentSchemaRef must be an object or null",
        ))
    })?;
    Ok(Some(SchemaReference::new(
        object.get("id").and_then(Value::as_str).unwrap_or_default(),
        object
            .get("version")
            .and_then(Value::as_str)
            .unwrap_or_default(),
        object
            .get("digest")
            .and_then(Value::as_str)
            .unwrap_or_default(),
    )))
}

fn build_snapshot_descriptor(
    content: &agenttalk_orchestration_contracts::brief::ContentValidatedManifest,
    manifest_object: &CasObject,
    files: &[SealedBriefFile],
    schemas: &[SealedSchemaRef],
) -> Result<Value, BriefSealError> {
    let mut files = files.to_vec();
    files.sort_by(|left, right| utf16_order(left.path(), right.path()));
    let file_records = files
        .iter()
        .map(|file| {
            json!({
                "path": file.path,
                "kind": file.kind,
                "format": file.format,
                "contentSchemaRef": file.content_schema_ref,
                "required": file.required,
                "rawSha256": file.declared_sha256,
                "size": file.size,
                "objectRef": file.object_ref,
                "context": file.context,
                "declaredOwnerRoleId": file.declared_owner_role_id,
            })
        })
        .collect::<Vec<_>>();

    let mut schemas = schemas.to_vec();
    schemas.sort_by(|left, right| {
        (left.id(), left.version(), left.digest()).cmp(&(
            right.id(),
            right.version(),
            right.digest(),
        ))
    });
    let schema_records = schemas
        .iter()
        .map(|schema| {
            json!({
                "id": schema.id,
                "version": schema.version,
                "digest": schema.digest,
                "canonicalSchemaObjectRef": schema.canonical_schema_object_ref,
            })
        })
        .collect::<Vec<_>>();

    let canonical_tree_record = content.canonical_tree_record_bytes();
    let canonical_tree_record_sha256 = sha256_hex(canonical_tree_record);

    Ok(json!({
        "schemaVersion": SNAPSHOT_DESCRIPTOR_SCHEMA,
        "briefTreeDigest": content.brief_tree_digest(),
        "canonicalTreeRecord": content.tree_record(),
        "canonicalTreeRecordSha256": canonical_tree_record_sha256,
        "manifest": {
            "objectRef": manifest_object.object_ref,
            "rawSha256": manifest_object.sha256,
            "size": manifest_object.size,
        },
        "files": file_records,
        "schemas": schema_records,
    }))
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
