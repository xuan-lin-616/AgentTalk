//! Brief root manifest contract: shape -> content -> `briefTreeDigest`.
//!
//! The filesystem-seal plane (`BRIEF_REPARSE_POINT`, `BRIEF_PATH_ESCAPE`,
//! `BRIEF_SOURCE_HANDLE_CHANGED`, physical FileId alias rejection) is **not**
//! implemented here. This module only judges the contract bytes and an
//! explicitly supplied in-memory bytes map.

use crate::error::{ContractError, ErrorCode};
use crate::json::{self, utf16_order};
use crate::registry::{SchemaReference, SchemaRegistry};
use crate::schema;
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};
use unicode_normalization::UnicodeNormalization;

const AUTHORING_ROOTS: [&str; 6] = [
    "plan",
    "design",
    "roles",
    "constraints",
    "rhythm",
    "acceptance",
];
const ROOT_MANIFEST_NAME: &str = "agenttalk-brief.json";

/// Duplicate-key-safe parsed brief manifest. No schema or semantic rules have
/// been applied yet.
#[derive(Clone, Debug)]
pub struct ParsedManifest {
    value: Value,
}

impl ParsedManifest {
    /// Parse raw manifest bytes without last-key-wins behavior.
    pub fn parse(bytes: &[u8]) -> Result<Self, ContractError> {
        match json::parse_duplicate_safe(bytes) {
            Ok(value) => Ok(Self { value }),
            Err(json::JsonParseError::DuplicateKey { path }) => Err(ContractError::new(
                ErrorCode::BriefDuplicateKey,
                format!("duplicate object key at {path}"),
            )),
            Err(other) => Err(ContractError::new(
                ErrorCode::BriefSchemaViolation,
                other.to_string(),
            )),
        }
    }

    /// Parse raw manifest JSON from a string.
    pub fn parse_str(json: &str) -> Result<Self, ContractError> {
        Self::parse(json.as_bytes())
    }

    #[must_use]
    pub fn as_value(&self) -> &Value {
        &self.value
    }

    /// Apply the formal Draft 2020-12 schema plus the pure semantic brief
    /// rules. Duplicate keys were already rejected at parse time.
    pub fn validate_shape(self) -> Result<ShapeValidatedManifest, ContractError> {
        validate_wire_enums(&self.value)?;

        if let Some(message) = schema::first_schema_error(schema::brief_validator(), &self.value) {
            return Err(ContractError::new(ErrorCode::BriefSchemaViolation, message));
        }

        validate_roles_and_references(&self.value)?;
        validate_file_paths(&self.value)?;

        // Enforce the frozen JCS extensions over the whole manifest so a
        // malformed Unicode value or unsafe integer cannot leak into a later
        // tree digest.
        json::canonicalize(&self.value).map_err(|error| {
            ContractError::new(ErrorCode::BriefCanonicalEncoding, error.to_string())
        })?;

        Ok(ShapeValidatedManifest { value: self.value })
    }
}

/// Schema- and semantic-shape-validated manifest.
#[derive(Clone, Debug)]
pub struct ShapeValidatedManifest {
    value: Value,
}

impl ShapeValidatedManifest {
    #[must_use]
    pub fn as_value(&self) -> &Value {
        &self.value
    }

    /// Re-verify every declared file against the supplied in-memory bytes map
    /// and resolve every non-null `contentSchemaRef` through the registry.
    /// Only after this step is `briefTreeDigest` available.
    pub fn validate_content(
        self,
        schema_registry: &dyn SchemaRegistry,
        bytes_map: &dyn BriefBytesMap,
    ) -> Result<ContentValidatedManifest, ContractError> {
        let files = self
            .value
            .get("files")
            .and_then(Value::as_array)
            .expect("shape validation guarantees files is an array");

        for file in files {
            let path = file
                .get("path")
                .and_then(Value::as_str)
                .expect("shape validation guarantees file.path");

            let Some(bytes) = bytes_map.get(path) else {
                return Err(ContractError::new(
                    ErrorCode::BriefDeclaredFileMissing,
                    format!("declared file not present in the supplied bytes map: {path}"),
                ));
            };

            let declared_size = json::value_as_safe_u64(
                file.get("size")
                    .expect("shape validation guarantees file.size"),
            )
            .expect("shape validation guarantees file.size is a non-negative safe integer");
            if bytes.len() as u64 != declared_size {
                return Err(ContractError::new(
                    ErrorCode::BriefSizeMismatch,
                    format!(
                        "declared size {declared_size} does not match supplied bytes length {} for {path}",
                        bytes.len()
                    ),
                ));
            }

            let declared_sha256 = file
                .get("sha256")
                .and_then(Value::as_str)
                .expect("shape validation guarantees file.sha256");
            let actual_sha256 = json::sha256_raw_hex(bytes);
            if declared_sha256 != actual_sha256 {
                return Err(ContractError::new(
                    ErrorCode::BriefHashMismatch,
                    format!("declared sha256 {declared_sha256} does not match actual {actual_sha256} for {path}"),
                ));
            }

            if let Some(schema_reference) = schema_reference(file)? {
                if schema_registry.resolve(&schema_reference).is_none() {
                    return Err(ContractError::new(
                        ErrorCode::BriefSchemaRefUnresolved,
                        format!(
                            "contentSchemaRef {} version {} digest {} is not resolvable",
                            schema_reference.id, schema_reference.version, schema_reference.digest
                        ),
                    ));
                }
            }
        }

        let (tree_record, canonical_tree_record, digest_hex) = build_tree_record(&self.value)?;

        Ok(ContentValidatedManifest {
            value: self.value,
            tree_record,
            canonical_tree_record,
            brief_tree_digest: digest_hex,
        })
    }
}

/// Content-validated brief manifest. Only this state can produce the frozen
/// transitive `briefTreeDigest`.
#[derive(Clone, Debug)]
pub struct ContentValidatedManifest {
    value: Value,
    tree_record: Value,
    canonical_tree_record: Vec<u8>,
    brief_tree_digest: String,
}

impl ContentValidatedManifest {
    #[must_use]
    pub fn as_value(&self) -> &Value {
        &self.value
    }

    /// The exact frozen `treeRecord` JSON value.
    #[must_use]
    pub fn tree_record(&self) -> &Value {
        &self.tree_record
    }

    /// `sha256Jcs` preimage bytes of the tree record.
    #[must_use]
    pub fn canonical_tree_record_bytes(&self) -> &[u8] {
        &self.canonical_tree_record
    }

    /// Lowercase hex `briefTreeDigest`.
    #[must_use]
    pub fn brief_tree_digest(&self) -> &str {
        &self.brief_tree_digest
    }
}

/// Pure in-memory source of declared brief file bytes. Production seal must
/// use same-open-handle reads in the Core sealer; this abstraction is for
/// contract verification only.
pub trait BriefBytesMap {
    fn get(&self, path: &str) -> Option<&[u8]>;
}

/// Fake in-memory bytes map for C1 fixtures and unit tests.
#[derive(Clone, Debug, Default)]
pub struct InMemoryBriefBytesMap {
    entries: HashMap<String, Vec<u8>>,
}

impl InMemoryBriefBytesMap {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, path: impl Into<String>, bytes: impl Into<Vec<u8>>) {
        self.entries.insert(path.into(), bytes.into());
    }
}

impl BriefBytesMap for InMemoryBriefBytesMap {
    fn get(&self, path: &str) -> Option<&[u8]> {
        self.entries.get(path).map(Vec::as_slice)
    }
}

fn validate_wire_enums(manifest: &Value) -> Result<(), ContractError> {
    let Some(files) = manifest.get("files").and_then(Value::as_array) else {
        return Ok(());
    };
    for file in files {
        for (pointer, allowed) in [
            (
                "/kind",
                &[
                    "plan",
                    "design",
                    "role",
                    "constraint",
                    "rhythm",
                    "acceptance",
                ][..],
            ),
            ("/format", &["markdown", "json", "text"][..]),
            ("/context/layer", &["persistent", "shared", "role"][..]),
            ("/context/retention", &["project", "run"][..]),
            (
                "/context/workspaceAccess",
                &["none", "read_only", "workspace_write"][..],
            ),
        ] {
            let Some(value) = file.pointer(pointer) else {
                continue;
            };
            let Some(actual) = value.as_str() else {
                continue;
            };
            if !allowed.contains(&actual) {
                return Err(ContractError::new(
                    ErrorCode::BriefEnumInvalid,
                    format!("{pointer} has invalid wire enum value: {actual:?}"),
                ));
            }
        }
    }
    Ok(())
}

fn validate_roles_and_references(manifest: &Value) -> Result<(), ContractError> {
    let roles = manifest
        .get("roles")
        .and_then(Value::as_array)
        .expect("shape validation guarantees roles is an array");

    let mut role_ids = HashSet::with_capacity(roles.len());
    for role in roles {
        let role_id = role
            .get("roleId")
            .and_then(Value::as_str)
            .expect("shape validation guarantees role.roleId");
        if !role_ids.insert(role_id.to_owned()) {
            return Err(ContractError::new(
                ErrorCode::BriefSchemaViolation,
                format!("duplicate roleId: {role_id}"),
            ));
        }
    }

    for file in manifest
        .get("files")
        .and_then(Value::as_array)
        .expect("shape validation guarantees files is an array")
    {
        let context = file
            .get("context")
            .and_then(Value::as_object)
            .expect("shape validation guarantees file.context");
        let context_role_ids = context
            .get("roleIds")
            .and_then(Value::as_array)
            .expect("shape validation guarantees context.roleIds");
        for role_id in context_role_ids {
            let role_id = role_id
                .as_str()
                .expect("shape validation guarantees context.roleIds entries are strings");
            if !role_ids.contains(role_id) {
                return Err(ContractError::new(
                    ErrorCode::BriefUnknownRole,
                    format!("context.roleIds references undeclared role: {role_id}"),
                ));
            }
        }

        let owner_role_id = file
            .get("declaredOwnerRoleId")
            .and_then(Value::as_str)
            .expect("shape validation guarantees file.declaredOwnerRoleId");
        if !role_ids.contains(owner_role_id) {
            return Err(ContractError::new(
                ErrorCode::BriefUnknownRole,
                format!("declaredOwnerRoleId references undeclared role: {owner_role_id}"),
            ));
        }
    }
    Ok(())
}

fn validate_file_paths(manifest: &Value) -> Result<(), ContractError> {
    let files = manifest
        .get("files")
        .and_then(Value::as_array)
        .expect("shape validation guarantees files is an array");
    let mut exact_paths = HashSet::with_capacity(files.len());
    let mut folded_paths: HashMap<String, String> = HashMap::with_capacity(files.len());

    for file in files {
        let path = file
            .get("path")
            .and_then(Value::as_str)
            .expect("shape validation guarantees file.path");
        validate_path_lexically_and_security(path)?;

        if !exact_paths.insert(path.to_owned()) {
            return Err(ContractError::new(
                ErrorCode::BriefDuplicatePath,
                format!("duplicate normalized relative path: {path}"),
            ));
        }
        let folded = path.to_lowercase();
        if let Some(previous) = folded_paths.insert(folded.clone(), path.to_owned()) {
            return Err(ContractError::new(
                ErrorCode::BriefPathAlias,
                format!(
                    "path lowercase alias collision between {previous:?} and {path:?} ({folded:?})"
                ),
            ));
        }
    }
    Ok(())
}

fn validate_path_lexically_and_security(path: &str) -> Result<(), ContractError> {
    let lexical = |detail: &str| {
        Err(ContractError::new(
            ErrorCode::BriefPathLexicalInvalid,
            format!("{detail}: {path:?}"),
        ))
    };

    if path.is_empty() || path.starts_with('/') || path.ends_with('/') || path.contains('\\') {
        return lexical("path must be a normalized relative POSIX path");
    }
    if path.eq_ignore_ascii_case(ROOT_MANIFEST_NAME) {
        return lexical("root manifest self-reference is forbidden");
    }
    if path.len() >= 2 {
        let bytes = path.as_bytes();
        if bytes[0].is_ascii_alphabetic() && bytes[1] == b':' {
            return lexical("drive-letter absolute path is forbidden");
        }
    }
    if path.starts_with("//") {
        return lexical("UNC path is forbidden");
    }
    if !is_nfc(path) {
        return lexical("path is not NFC");
    }

    let components: Vec<&str> = path.split('/').collect();
    if components.iter().any(|component| component.is_empty()) {
        return lexical("empty path segment is forbidden");
    }
    for component in &components {
        if *component == "." || *component == ".." {
            return lexical("dot and dot-dot path segments are forbidden");
        }
        if component.ends_with('.') || component.ends_with(' ') {
            return lexical("path components ending in a trailing dot or space are forbidden");
        }
        if component.contains(':') {
            return lexical("alternate data streams (segment containing ':') are forbidden");
        }
        if is_reserved_device_name(component) {
            return lexical("reserved Windows device name segment is forbidden");
        }
    }
    for component in &components {
        if component.eq_ignore_ascii_case(".agenttalk") {
            return Err(ContractError::new(
                ErrorCode::BriefCasReference,
                format!(".agenttalk (any case variant) is forbidden as a path component: {path:?}"),
            ));
        }
        if is_forbidden_component_name(component) {
            return Err(ContractError::new(
                ErrorCode::BriefSensitiveSourceForbidden,
                format!("sensitive path component is forbidden: {component:?} in {path:?}"),
            ));
        }
    }
    if !AUTHORING_ROOTS.contains(&components[0]) {
        return lexical(
            "path must start with one of plan/ design/ roles/ constraints/ rhythm/ acceptance/",
        );
    }

    let basename = components
        .last()
        .expect("non-empty path always has a basename");
    if is_forbidden_basename(basename) {
        return Err(ContractError::new(
            ErrorCode::BriefSensitiveSourceForbidden,
            format!("sensitive basename is forbidden: {basename:?} in {path:?}"),
        ));
    }
    Ok(())
}

fn is_nfc(value: &str) -> bool {
    value.nfc().collect::<String>() == value
}

fn is_reserved_device_name(component: &str) -> bool {
    let stem = component
        .split('.')
        .next()
        .unwrap_or(component)
        .to_ascii_uppercase();
    matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || (stem.len() == 4
            && (stem.starts_with("COM") || stem.starts_with("LPT"))
            && stem.as_bytes()[3].is_ascii_digit()
            && (b'1'..=b'9').contains(&stem.as_bytes()[3]))
}

fn is_forbidden_component_name(component: &str) -> bool {
    matches!(
        component.to_ascii_lowercase().as_str(),
        ".git" | ".ssh" | ".aws" | ".azure" | ".kube" | ".gnupg"
    )
}

fn is_forbidden_basename(basename: &str) -> bool {
    let lower = basename.to_ascii_lowercase();
    let exact = matches!(
        lower.as_str(),
        ".env"
            | ".envrc"
            | ".npmrc"
            | ".pypirc"
            | ".netrc"
            | "id_rsa"
            | "id_ecdsa"
            | "id_ed25519"
            | "id_dsa"
            | "credentials"
            | "credentials.json"
            | "secrets.json"
            | "service-account.json"
    );
    let env_glob = lower.starts_with(".env.") && lower != ".env.example";
    let forbidden_extension = [
        ".pem",
        ".key",
        ".p8",
        ".p12",
        ".pfx",
        ".jks",
        ".keystore",
        ".kdbx",
    ]
    .iter()
    .any(|extension| lower.ends_with(extension));
    exact || env_glob || forbidden_extension
}

fn schema_reference(file: &Value) -> Result<Option<SchemaReference>, ContractError> {
    let content_schema_ref = file
        .get("contentSchemaRef")
        .expect("shape validation guarantees file.contentSchemaRef");
    if content_schema_ref.is_null() {
        return Ok(None);
    }
    let object = content_schema_ref.as_object().ok_or_else(|| {
        ContractError::new(
            ErrorCode::BriefSchemaViolation,
            "contentSchemaRef must be null or an object",
        )
    })?;
    let id = object.get("id").and_then(Value::as_str).ok_or_else(|| {
        ContractError::new(
            ErrorCode::BriefSchemaViolation,
            "contentSchemaRef.id must be a string",
        )
    })?;
    let version = object
        .get("version")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            ContractError::new(
                ErrorCode::BriefSchemaViolation,
                "contentSchemaRef.version must be a string",
            )
        })?;
    let digest = object
        .get("digest")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            ContractError::new(
                ErrorCode::BriefSchemaViolation,
                "contentSchemaRef.digest must be a string",
            )
        })?;
    Ok(Some(SchemaReference::new(id, version, digest)))
}

fn build_tree_record(manifest: &Value) -> Result<(Value, Vec<u8>, String), ContractError> {
    let mut roles = manifest
        .get("roles")
        .and_then(Value::as_array)
        .expect("shape validation guarantees roles is an array")
        .clone();
    roles.sort_by(|left, right| {
        let left_id = left
            .get("roleId")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let right_id = right
            .get("roleId")
            .and_then(Value::as_str)
            .unwrap_or_default();
        utf16_order(left_id, right_id)
    });

    let mut files = manifest
        .get("files")
        .and_then(Value::as_array)
        .expect("shape validation guarantees files is an array")
        .clone();
    files.sort_by(|left, right| {
        let left_path = left.get("path").and_then(Value::as_str).unwrap_or_default();
        let right_path = right
            .get("path")
            .and_then(Value::as_str)
            .unwrap_or_default();
        utf16_order(left_path, right_path)
    });

    let file_records = files
        .iter()
        .map(|file| {
            let context = file
                .get("context")
                .and_then(Value::as_object)
                .expect("shape validation guarantees file.context");
            let mut context_role_ids = context
                .get("roleIds")
                .and_then(Value::as_array)
                .expect("shape validation guarantees context.roleIds")
                .clone();
            context_role_ids.sort_by(|left, right| {
                utf16_order(
                    left.as_str().unwrap_or_default(),
                    right.as_str().unwrap_or_default(),
                )
            });

            Ok(Value::Object(Map::from_iter([
                (
                    "path".to_owned(),
                    file.get("path")
                        .cloned()
                        .expect("shape validation guarantees file.path"),
                ),
                (
                    "kind".to_owned(),
                    file.get("kind")
                        .cloned()
                        .expect("shape validation guarantees file.kind"),
                ),
                (
                    "format".to_owned(),
                    file.get("format")
                        .cloned()
                        .expect("shape validation guarantees file.format"),
                ),
                (
                    "contentSchemaRef".to_owned(),
                    file.get("contentSchemaRef")
                        .cloned()
                        .expect("shape validation guarantees file.contentSchemaRef"),
                ),
                (
                    "required".to_owned(),
                    file.get("required")
                        .cloned()
                        .expect("shape validation guarantees file.required"),
                ),
                (
                    "rawSha256".to_owned(),
                    file.get("sha256")
                        .cloned()
                        .expect("shape validation guarantees file.sha256"),
                ),
                (
                    "size".to_owned(),
                    file.get("size")
                        .cloned()
                        .expect("shape validation guarantees file.size"),
                ),
                (
                    "context".to_owned(),
                    Value::Object(Map::from_iter([
                        (
                            "layer".to_owned(),
                            context
                                .get("layer")
                                .cloned()
                                .expect("shape validation guarantees context.layer"),
                        ),
                        ("roleIds".to_owned(), Value::Array(context_role_ids)),
                        (
                            "retention".to_owned(),
                            context
                                .get("retention")
                                .cloned()
                                .expect("shape validation guarantees context.retention"),
                        ),
                        (
                            "workspaceAccess".to_owned(),
                            context
                                .get("workspaceAccess")
                                .cloned()
                                .expect("shape validation guarantees context.workspaceAccess"),
                        ),
                    ])),
                ),
                (
                    "declaredOwnerRoleId".to_owned(),
                    file.get("declaredOwnerRoleId")
                        .cloned()
                        .expect("shape validation guarantees file.declaredOwnerRoleId"),
                ),
            ])))
        })
        .collect::<Result<Vec<Value>, ContractError>>()?;

    let tree_record = Value::Object(Map::from_iter([
        (
            "schemaVersion".to_owned(),
            Value::String("agenttalk.brief.tree.v1".to_owned()),
        ),
        (
            "manifestSchemaVersion".to_owned(),
            Value::String("agenttalk.brief.manifest.v1".to_owned()),
        ),
        (
            "projectId".to_owned(),
            manifest
                .get("projectId")
                .cloned()
                .expect("shape validation guarantees projectId"),
        ),
        (
            "title".to_owned(),
            manifest
                .get("title")
                .cloned()
                .expect("shape validation guarantees title"),
        ),
        ("roles".to_owned(), Value::Array(roles)),
        ("files".to_owned(), Value::Array(file_records)),
    ]));

    let canonical = json::canonicalize(&tree_record).map_err(|error| {
        ContractError::new(ErrorCode::BriefCanonicalEncoding, error.to_string())
    })?;
    let digest = json::sha256_raw_hex(&canonical);
    Ok((tree_record, canonical, digest))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal_value() -> Value {
        serde_json::json!({
            "schemaVersion": "agenttalk.brief.manifest.v1",
            "projectId": "test",
            "title": "Test",
            "roles": [{"roleId": "pm", "displayName": "PM"}],
            "files": []
        })
    }

    #[test]
    fn empty_files_is_shape_valid_when_frozen_document_is_silent() {
        // The frozen document explicitly imposes roles >= 1 but does not impose
        // files >= 1, so the schema must not invent that boundary.
        ParsedManifest::parse_str(&minimal_value().to_string())
            .unwrap()
            .validate_shape()
            .unwrap();
    }

    #[test]
    fn duplicate_role_id_is_rejected() {
        let mut value = minimal_value();
        value["roles"] = serde_json::json!([
            {"roleId": "pm", "displayName": "PM"},
            {"roleId": "pm", "displayName": "Duplicate"}
        ]);
        let error = ParsedManifest::parse_str(&value.to_string())
            .unwrap()
            .validate_shape()
            .unwrap_err();
        assert_eq!(error.code(), ErrorCode::BriefSchemaViolation);
    }

    #[test]
    fn env_example_basename_is_allowed() {
        let mut value = minimal_value();
        value["files"] = serde_json::json!([{
            "path": "plan/.env.example",
            "kind": "plan",
            "format": "markdown",
            "contentSchemaRef": null,
            "required": false,
            "sha256": "00".repeat(32),
            "size": 0,
            "context": {"layer": "shared", "roleIds": ["pm"], "retention": "run", "workspaceAccess": "read_only"},
            "declaredOwnerRoleId": "pm"
        }]);
        ParsedManifest::parse_str(&value.to_string())
            .unwrap()
            .validate_shape()
            .unwrap();
    }
}
