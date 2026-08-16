use std::collections::BTreeSet;
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const ADAPTER_MANIFEST_SCHEMA: &str =
    include_str!("../../../../schemas/adapter/v1/manifest.schema.json");
const MAX_MANIFEST_BYTES: usize = 64 * 1024;
const MAX_STRING_BYTES: usize = 512;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManifestValidationErrorCode {
    Oversized,
    InvalidJson,
    SchemaViolation,
    InvalidIdentifier,
    InvalidText,
    InvalidLaunch,
    InvalidHash,
    SecretLiteral,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestValidationError {
    code: ManifestValidationErrorCode,
}

impl ManifestValidationError {
    fn new(code: ManifestValidationErrorCode) -> Self {
        Self { code }
    }

    pub fn code(&self) -> ManifestValidationErrorCode {
        self.code
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AdapterManifest {
    pub schema_version: String,
    pub id: String,
    pub display_name: String,
    pub category: ManifestCategory,
    pub protocol: ManifestProtocol,
    #[serde(rename = "match")]
    pub match_rules: ManifestMatch,
    pub launch: ManifestLaunch,
    pub verification: ManifestVerification,
    pub capability_policy: ManifestCapabilityPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<ManifestSource>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ManifestCategory {
    AgentProtocol,
    ModelRuntime,
    Mcp,
    Unknown,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManifestProtocol {
    pub kind: ManifestProtocolKind,
    pub major: u16,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ManifestProtocolKind {
    Acp,
    A2a,
    OpenaiCompatible,
    Ollama,
    Mcp,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManifestMatch {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub executable_names: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub publisher_subjects: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub registry_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub package_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum ManifestLaunch {
    Direct {
        transport: ManifestTransport,
        executable_ref: String,
        args: Vec<String>,
        environment_allowlist: Vec<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        credential_environment: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        archive_sha256: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sha256: Option<String>,
    },
    Npx {
        package: String,
        args: Vec<String>,
        environment_allowlist: Vec<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        credential_environment: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sha256: Option<String>,
    },
    Uvx {
        package: String,
        args: Vec<String>,
        environment_allowlist: Vec<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        credential_environment: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sha256: Option<String>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RunnerPackageKind {
    Npx,
    Uvx,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ManifestTransport {
    Stdio,
    Http,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManifestVerification {
    pub kind: ManifestVerificationKind,
    pub timeout_ms: u32,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ManifestVerificationKind {
    AcpInitialize,
    A2aCard,
    ModelMetadata,
    McpInitialize,
    Manual,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManifestCapabilityPolicy {
    pub filesystem: CapabilityRequirement,
    pub shell: CapabilityRequirement,
    pub streaming: CapabilityRequirement,
    pub cancel: CapabilityRequirement,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityRequirement {
    Required,
    Optional,
    Negotiate,
    Forbidden,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManifestSource {
    pub kind: ManifestSourceKind,
    pub id: String,
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog_sha256: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ManifestSourceKind {
    AgenttalkManifest,
    AcpRegistry,
}

impl AdapterManifest {
    pub fn validate_json_bytes(bytes: &[u8]) -> Result<Self, ManifestValidationError> {
        if bytes.len() > MAX_MANIFEST_BYTES {
            return Err(ManifestValidationError::new(
                ManifestValidationErrorCode::Oversized,
            ));
        }
        let value: Value = serde_json::from_slice(bytes)
            .map_err(|_| ManifestValidationError::new(ManifestValidationErrorCode::InvalidJson))?;
        if let Some(code) = classify_manifest_value_violation(&value) {
            return Err(ManifestValidationError::new(code));
        }
        validate_against_embedded_schema(&value)?;
        let mut manifest: Self = serde_json::from_value(value).map_err(|_| {
            ManifestValidationError::new(ManifestValidationErrorCode::SchemaViolation)
        })?;
        manifest.validate_semantics()?;
        Ok(manifest)
    }

    pub fn validate_value(value: Value) -> Result<Self, ManifestValidationError> {
        let bytes = serde_json::to_vec(&value)
            .map_err(|_| ManifestValidationError::new(ManifestValidationErrorCode::InvalidJson))?;
        Self::validate_json_bytes(&bytes)
    }

    pub fn to_sanitized_value(&self) -> Value {
        serde_json::to_value(self).expect("adapter manifest serializes")
    }

    fn validate_semantics(&mut self) -> Result<(), ManifestValidationError> {
        if self.schema_version != "agenttalk.adapter.v1" {
            return Err(ManifestValidationError::new(
                ManifestValidationErrorCode::SchemaViolation,
            ));
        }
        validate_id(&self.id)?;
        validate_display_text(&self.display_name)?;
        normalize_match_rules(&mut self.match_rules)?;
        validate_launch(&mut self.launch)?;
        if self.verification.timeout_ms < 100 || self.verification.timeout_ms > 30_000 {
            return Err(ManifestValidationError::new(
                ManifestValidationErrorCode::SchemaViolation,
            ));
        }
        if let Some(source) = &mut self.source {
            validate_id(&source.id)?;
            validate_display_text(&source.version)?;
            if let Some(revision) = &source.revision {
                validate_display_text(revision)?;
            }
            if let Some(hash) = &mut source.catalog_sha256 {
                *hash = normalize_sha256(hash)?;
            }
        }
        Ok(())
    }
}

fn classify_manifest_value_violation(value: &Value) -> Option<ManifestValidationErrorCode> {
    value
        .get("launch")
        .and_then(Value::as_object)
        .and_then(|launch| launch.get("args"))
        .and_then(Value::as_array)
        .and_then(|args| {
            args.iter().find_map(|arg| {
                let value = arg.as_str()?;
                if contains_secret_literal(value) {
                    return Some(ManifestValidationErrorCode::SecretLiteral);
                }
                looks_like_shell_command(value)
                    .then_some(ManifestValidationErrorCode::InvalidLaunch)
            })
        })
}

pub fn validate_against_embedded_schema(value: &Value) -> Result<(), ManifestValidationError> {
    let schema: Value = serde_json::from_str(ADAPTER_MANIFEST_SCHEMA)
        .map_err(|_| ManifestValidationError::new(ManifestValidationErrorCode::SchemaViolation))?;
    let validator = jsonschema::options()
        .with_draft(jsonschema::Draft::Draft202012)
        .build(&schema)
        .map_err(|_| ManifestValidationError::new(ManifestValidationErrorCode::SchemaViolation))?;
    validator
        .validate(value)
        .map_err(|_| ManifestValidationError::new(ManifestValidationErrorCode::SchemaViolation))
}

fn normalize_match_rules(rules: &mut ManifestMatch) -> Result<(), ManifestValidationError> {
    validate_string_array(&rules.executable_names, validate_executable_name)?;
    validate_string_array(&rules.publisher_subjects, validate_display_text)?;
    validate_string_array(&rules.registry_ids, validate_id)?;
    validate_string_array(&rules.package_ids, validate_package_id)?;
    if let Some(hash) = &mut rules.sha256 {
        *hash = normalize_sha256(hash)?;
    }
    Ok(())
}

fn validate_launch(launch: &mut ManifestLaunch) -> Result<(), ManifestValidationError> {
    match launch {
        ManifestLaunch::Direct {
            executable_ref,
            args,
            environment_allowlist,
            credential_environment,
            archive_sha256,
            sha256,
            ..
        } => {
            validate_executable_ref(executable_ref)?;
            validate_args(args)?;
            validate_env_allowlist(environment_allowlist)?;
            validate_credential_env_slots(credential_environment)?;
            if let Some(hash) = archive_sha256 {
                *hash = normalize_sha256(hash)?;
            }
            if let Some(hash) = sha256 {
                *hash = normalize_sha256(hash)?;
            }
        }
        ManifestLaunch::Npx {
            package,
            args,
            environment_allowlist,
            credential_environment,
            sha256,
        } => {
            validate_runner_package(RunnerPackageKind::Npx, package)?;
            validate_args(args)?;
            validate_env_allowlist(environment_allowlist)?;
            validate_credential_env_slots(credential_environment)?;
            if let Some(hash) = sha256 {
                *hash = normalize_sha256(hash)?;
            }
        }
        ManifestLaunch::Uvx {
            package,
            args,
            environment_allowlist,
            credential_environment,
            sha256,
        } => {
            validate_runner_package(RunnerPackageKind::Uvx, package)?;
            validate_args(args)?;
            validate_env_allowlist(environment_allowlist)?;
            validate_credential_env_slots(credential_environment)?;
            if let Some(hash) = sha256 {
                *hash = normalize_sha256(hash)?;
            }
        }
    }
    Ok(())
}

fn validate_args(args: &[String]) -> Result<(), ManifestValidationError> {
    if args.len() > 32 {
        return Err(ManifestValidationError::new(
            ManifestValidationErrorCode::Oversized,
        ));
    }
    for arg in args {
        validate_small_safe_string(arg)?;
        if contains_secret_literal(arg) {
            return Err(ManifestValidationError::new(
                ManifestValidationErrorCode::SecretLiteral,
            ));
        }
        if looks_like_shell_command(arg) {
            return Err(ManifestValidationError::new(
                ManifestValidationErrorCode::InvalidLaunch,
            ));
        }
    }
    Ok(())
}

fn validate_env_allowlist(values: &[String]) -> Result<(), ManifestValidationError> {
    if values.len() > 32 {
        return Err(ManifestValidationError::new(
            ManifestValidationErrorCode::Oversized,
        ));
    }
    let mut seen = BTreeSet::new();
    for value in values {
        if !seen.insert(value) {
            return Err(ManifestValidationError::new(
                ManifestValidationErrorCode::SchemaViolation,
            ));
        }
        validate_env_name(value)?;
    }
    Ok(())
}

fn validate_credential_env_slots(values: &[String]) -> Result<(), ManifestValidationError> {
    if values.len() > 32 {
        return Err(ManifestValidationError::new(
            ManifestValidationErrorCode::Oversized,
        ));
    }
    let mut seen = BTreeSet::new();
    for value in values {
        if !seen.insert(value) {
            return Err(ManifestValidationError::new(
                ManifestValidationErrorCode::SchemaViolation,
            ));
        }
        validate_env_name_syntax(value)?;
    }
    Ok(())
}

fn validate_string_array(
    values: &[String],
    validate: fn(&str) -> Result<(), ManifestValidationError>,
) -> Result<(), ManifestValidationError> {
    if values.len() > 16 {
        return Err(ManifestValidationError::new(
            ManifestValidationErrorCode::Oversized,
        ));
    }
    let mut seen = BTreeSet::new();
    for value in values {
        if !seen.insert(value) {
            return Err(ManifestValidationError::new(
                ManifestValidationErrorCode::SchemaViolation,
            ));
        }
        validate(value)?;
    }
    Ok(())
}

fn validate_id(value: &str) -> Result<(), ManifestValidationError> {
    validate_small_safe_string(value)?;
    let valid = value.len() >= 3
        && value.len() <= 128
        && value.chars().all(|ch| {
            ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '.' | '_' | '-')
        })
        && value
            .chars()
            .next()
            .is_some_and(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit())
        && value
            .chars()
            .last()
            .is_some_and(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit());
    if valid {
        Ok(())
    } else {
        Err(ManifestValidationError::new(
            ManifestValidationErrorCode::InvalidIdentifier,
        ))
    }
}

pub(crate) fn validate_runner_package(
    kind: RunnerPackageKind,
    value: &str,
) -> Result<(), ManifestValidationError> {
    validate_small_safe_string(value)?;
    if contains_secret_literal(value)
        || looks_like_path_or_uri(value)
        || value.contains("..")
        || value.starts_with('-')
        || value.starts_with('/')
        || value.starts_with("//")
        || value.contains([
            '\\', ':', '|', '&', ';', '`', '$', '(', ')', '<', '>', '"', '\'',
        ])
        || value.contains(char::is_whitespace)
    {
        return Err(ManifestValidationError::new(
            ManifestValidationErrorCode::InvalidLaunch,
        ));
    }
    let valid = value.len() <= 160
        && match kind {
            RunnerPackageKind::Npx => valid_npx_package(value),
            RunnerPackageKind::Uvx => valid_uvx_package(value),
        };
    if valid {
        Ok(())
    } else {
        Err(ManifestValidationError::new(
            ManifestValidationErrorCode::InvalidIdentifier,
        ))
    }
}

fn valid_npx_package(value: &str) -> bool {
    let Some((name, version)) = split_optional_at_version(value) else {
        return false;
    };
    valid_npm_package_name(name) && version.is_none_or(valid_pinned_version)
}

fn valid_uvx_package(value: &str) -> bool {
    if let Some((name, version)) = value.split_once("==") {
        return valid_runner_name(name) && valid_pinned_version(version);
    }
    let Some((name, version)) = split_optional_at_version(value) else {
        return false;
    };
    valid_runner_name(name) && version.is_none_or(valid_pinned_version)
}

fn split_optional_at_version(value: &str) -> Option<(&str, Option<&str>)> {
    if let Some(rest) = value.strip_prefix('@') {
        let slash = rest.find('/')?;
        let package = &rest[slash + 1..];
        if let Some(offset) = package.rfind('@') {
            let version_start = 1 + slash + 1 + offset;
            Some((&value[..version_start], Some(&value[version_start + 1..])))
        } else {
            Some((value, None))
        }
    } else if let Some(offset) = value.rfind('@') {
        Some((&value[..offset], Some(&value[offset + 1..])))
    } else {
        Some((value, None))
    }
}

fn valid_npm_package_name(value: &str) -> bool {
    if let Some(scoped) = value.strip_prefix('@') {
        let Some((scope, name)) = scoped.split_once('/') else {
            return false;
        };
        valid_runner_name(scope) && valid_runner_name(name)
    } else {
        valid_runner_name(value) && !value.contains('/')
    }
}

fn valid_runner_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 96
        && !value.starts_with(['.', '-'])
        && !value.ends_with(['.', '-'])
        && value.chars().all(|ch| {
            ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '.' | '_' | '-')
        })
}

fn valid_pinned_version(value: &str) -> bool {
    if value.len() > 64 {
        return false;
    }
    let parts = value.split('.').collect::<Vec<_>>();
    parts.len() == 3
        && parts
            .iter()
            .all(|part| !part.is_empty() && part.chars().all(|ch| ch.is_ascii_digit()))
}

fn validate_package_id(value: &str) -> Result<(), ManifestValidationError> {
    validate_small_safe_string(value)?;
    let valid = value.len() >= 3
        && value.len() <= 180
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-'));
    if valid {
        Ok(())
    } else {
        Err(ManifestValidationError::new(
            ManifestValidationErrorCode::InvalidIdentifier,
        ))
    }
}

fn validate_executable_name(value: &str) -> Result<(), ManifestValidationError> {
    validate_small_safe_string(value)?;
    let lower = value.to_ascii_lowercase();
    let valid = lower.ends_with(".exe")
        && value.len() <= 96
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-' | '+'));
    if valid && !looks_like_path_or_uri(value) && !value.contains("..") {
        Ok(())
    } else {
        Err(ManifestValidationError::new(
            ManifestValidationErrorCode::InvalidLaunch,
        ))
    }
}

fn validate_display_text(value: &str) -> Result<(), ManifestValidationError> {
    validate_small_safe_string(value)?;
    if contains_secret_literal(value) || looks_like_path_or_uri(value) {
        return Err(ManifestValidationError::new(
            ManifestValidationErrorCode::InvalidText,
        ));
    }
    Ok(())
}

fn validate_executable_ref(value: &str) -> Result<(), ManifestValidationError> {
    validate_small_safe_string(value)?;
    if value == "matched-observation" {
        return Ok(());
    }
    if contains_secret_literal(value)
        || looks_like_shell_command(value)
        || looks_like_path_or_uri(value)
        || value.to_ascii_lowercase().ends_with(".dll")
    {
        return Err(ManifestValidationError::new(
            ManifestValidationErrorCode::InvalidLaunch,
        ));
    }
    let path = PathBuf::from(value);
    if path.is_absolute() || has_traversal(&path) || value.contains(['/', '\\']) {
        return Err(ManifestValidationError::new(
            ManifestValidationErrorCode::InvalidLaunch,
        ));
    }
    let valid = value.len() <= 96
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-' | '+'))
        && value
            .chars()
            .next()
            .is_some_and(|ch| ch.is_ascii_alphanumeric());
    if valid {
        Ok(())
    } else {
        Err(ManifestValidationError::new(
            ManifestValidationErrorCode::InvalidLaunch,
        ))
    }
}

fn validate_env_name(value: &str) -> Result<(), ManifestValidationError> {
    validate_env_name_syntax(value)?;
    if !secret_env_name(value) {
        Ok(())
    } else {
        Err(ManifestValidationError::new(
            ManifestValidationErrorCode::SecretLiteral,
        ))
    }
}

fn validate_env_name_syntax(value: &str) -> Result<(), ManifestValidationError> {
    validate_small_safe_string(value)?;
    let valid = value.len() <= 64
        && value
            .chars()
            .all(|ch| ch.is_ascii_uppercase() || ch.is_ascii_digit() || ch == '_')
        && value
            .chars()
            .next()
            .is_some_and(|ch| ch.is_ascii_uppercase());
    if valid {
        Ok(())
    } else {
        Err(ManifestValidationError::new(
            ManifestValidationErrorCode::InvalidIdentifier,
        ))
    }
}

fn validate_small_safe_string(value: &str) -> Result<(), ManifestValidationError> {
    if value.len() > MAX_STRING_BYTES {
        return Err(ManifestValidationError::new(
            ManifestValidationErrorCode::Oversized,
        ));
    }
    if value.chars().any(is_forbidden_text_control) {
        return Err(ManifestValidationError::new(
            ManifestValidationErrorCode::InvalidText,
        ));
    }
    Ok(())
}

fn normalize_sha256(value: &str) -> Result<String, ManifestValidationError> {
    if value.len() == 64 && value.chars().all(|ch| ch.is_ascii_hexdigit()) {
        Ok(value.to_ascii_lowercase())
    } else {
        Err(ManifestValidationError::new(
            ManifestValidationErrorCode::InvalidHash,
        ))
    }
}

pub fn contains_secret_literal(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    [
        "authorization",
        "cookie",
        "bearer ",
        "token=",
        "api_key",
        "apikey",
        "password",
        "passwd",
        "secret=",
        "runtime_token",
        "runtimetoken",
    ]
    .iter()
    .any(|marker| lower.contains(marker))
}

fn secret_env_name(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    [
        "token",
        "secret",
        "password",
        "cookie",
        "authorization",
        "apikey",
        "api_key",
    ]
    .iter()
    .any(|marker| lower.contains(marker))
}

fn looks_like_shell_command(value: &str) -> bool {
    let lower = value.trim().to_ascii_lowercase();
    lower == "cmd"
        || lower == "cmd.exe"
        || lower.starts_with("cmd /c")
        || lower.starts_with("cmd.exe /c")
        || lower == "powershell"
        || lower == "powershell.exe"
        || lower.contains("powershell -command")
        || lower.contains("powershell.exe -command")
        || lower == "sh"
        || lower.starts_with("sh -c")
        || lower.contains(['|', '&', ';', '`'])
}

fn looks_like_path_or_uri(value: &str) -> bool {
    value.contains("://")
        || value.starts_with("\\\\")
        || value.starts_with("//")
        || looks_like_windows_absolute_path(value)
}

fn looks_like_windows_absolute_path(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() >= 3
        && bytes[1] == b':'
        && (bytes[2] == b'\\' || bytes[2] == b'/')
        && bytes[0].is_ascii_alphabetic()
}

fn has_traversal(path: &Path) -> bool {
    path.components()
        .any(|component| matches!(component, Component::ParentDir))
}

fn is_forbidden_text_control(ch: char) -> bool {
    ch.is_control()
        || matches!(
            ch,
            '\u{202a}'
                | '\u{202b}'
                | '\u{202c}'
                | '\u{202d}'
                | '\u{202e}'
                | '\u{2066}'
                | '\u{2067}'
                | '\u{2068}'
                | '\u{2069}'
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn minimal_manifest_value() -> Value {
        json!({
            "schemaVersion": "agenttalk.adapter.v1",
            "id": "org.example.agent",
            "displayName": "Example Agent",
            "category": "agent_protocol",
            "protocol": { "kind": "acp", "major": 1 },
            "match": {
                "executableNames": ["example-agent.exe"],
                "publisherSubjects": ["Example Corp"],
                "registryIds": ["example-agent"],
                "sha256": "ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789"
            },
            "launch": {
                "kind": "direct",
                "transport": "stdio",
                "executableRef": "matched-observation",
                "args": ["--acp"],
                "environmentAllowlist": ["PATH", "USERPROFILE", "LOCALAPPDATA"]
            },
            "verification": { "kind": "acp_initialize", "timeoutMs": 3000 },
            "capabilityPolicy": {
                "filesystem": "negotiate",
                "shell": "negotiate",
                "streaming": "required",
                "cancel": "required"
            }
        })
    }

    fn runner_manifest_value(kind: &str, package: &str) -> Value {
        let mut value = minimal_manifest_value();
        value["launch"] = json!({
            "kind": kind,
            "package": package,
            "args": ["--acp"],
            "environmentAllowlist": ["PATH"]
        });
        value
    }

    #[test]
    fn valid_minimal_manifest_passes_draft_2020_12() {
        let value = minimal_manifest_value();
        validate_against_embedded_schema(&value).expect("draft 2020-12 schema validates");
        let manifest = AdapterManifest::validate_value(value).expect("typed manifest validates");
        assert_eq!(manifest.schema_version, "agenttalk.adapter.v1");
        assert_eq!(
            manifest.match_rules.sha256.as_deref(),
            Some("abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789")
        );
    }

    #[test]
    fn direct_launch_archive_sha256_is_validated_separately() {
        let mut value = minimal_manifest_value();
        value["launch"]["archiveSha256"] =
            json!("ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789");
        let manifest = AdapterManifest::validate_value(value).expect("archive hash validates");
        match manifest.launch {
            ManifestLaunch::Direct {
                archive_sha256,
                sha256,
                ..
            } => {
                assert_eq!(
                    archive_sha256.as_deref(),
                    Some("abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789")
                );
                assert_eq!(sha256, None);
            }
            _ => panic!("direct launch"),
        }

        let mut invalid = minimal_manifest_value();
        invalid["launch"]["archiveSha256"] = json!("not-a-sha");
        assert_eq!(
            AdapterManifest::validate_value(invalid).unwrap_err().code(),
            ManifestValidationErrorCode::SchemaViolation
        );
    }

    #[test]
    fn credential_environment_slots_are_names_only() {
        let mut value = minimal_manifest_value();
        value["launch"]["credentialEnvironment"] = json!(["AGENT_TOKEN"]);
        let manifest = AdapterManifest::validate_value(value).expect("credential slot validates");
        let json = serde_json::to_string(&manifest).expect("serialize manifest");
        assert!(json.contains("AGENT_TOKEN"));
        assert!(!json.contains("fixture-token-value"));

        let mut invalid = minimal_manifest_value();
        invalid["launch"]["credentialEnvironment"] = json!(["agent-token=value"]);
        assert_eq!(
            AdapterManifest::validate_value(invalid).unwrap_err().code(),
            ManifestValidationErrorCode::SchemaViolation
        );
    }

    #[test]
    fn runner_launch_credential_environment_slots_are_names_only() {
        let mut value = minimal_manifest_value();
        value["launch"] = json!({
            "kind": "npx",
            "package": "@scope/example-agent",
            "args": ["--acp"],
            "environmentAllowlist": ["PATH"],
            "credentialEnvironment": ["AGENT_TOKEN"]
        });
        let manifest = AdapterManifest::validate_value(value).expect("runner credential slot");
        match manifest.launch {
            ManifestLaunch::Npx {
                environment_allowlist,
                credential_environment,
                ..
            } => {
                assert_eq!(environment_allowlist, vec!["PATH"]);
                assert_eq!(credential_environment, vec!["AGENT_TOKEN"]);
            }
            _ => panic!("npx launch"),
        }
    }

    #[test]
    fn direct_manifest_npx_rejects_uvx_double_equals_spec() {
        let value = runner_manifest_value("npx", "fast-agent-acp==0.9.30");
        assert!(AdapterManifest::validate_value(value).is_err());
    }

    #[test]
    fn direct_manifest_uvx_rejects_npm_scoped_spec() {
        let value = runner_manifest_value("uvx", "@scope/example-agent@1.2.3");
        assert!(AdapterManifest::validate_value(value).is_err());
    }

    #[test]
    fn draft_schema_npx_rejects_uvx_double_equals_spec() {
        let value = runner_manifest_value("npx", "fast-agent-acp==0.9.30");
        assert!(validate_against_embedded_schema(&value).is_err());
    }

    #[test]
    fn draft_schema_uvx_rejects_npm_scoped_spec() {
        let value = runner_manifest_value("uvx", "@scope/example-agent@1.2.3");
        assert!(validate_against_embedded_schema(&value).is_err());
    }

    #[test]
    fn unknown_root_and_nested_fields_fail_closed() {
        let mut root = minimal_manifest_value();
        root.as_object_mut()
            .expect("object")
            .insert("surprise".into(), json!(true));
        assert_eq!(
            AdapterManifest::validate_value(root).unwrap_err().code(),
            ManifestValidationErrorCode::SchemaViolation
        );

        let mut nested = minimal_manifest_value();
        nested["launch"]["shellCommand"] = json!("example-agent.exe --acp");
        assert_eq!(
            AdapterManifest::validate_value(nested).unwrap_err().code(),
            ManifestValidationErrorCode::SchemaViolation
        );
    }

    #[test]
    fn oversized_manifest_strings_args_and_arrays_are_rejected() {
        let oversized = vec![b'a'; MAX_MANIFEST_BYTES + 1];
        assert_eq!(
            AdapterManifest::validate_json_bytes(&oversized)
                .unwrap_err()
                .code(),
            ManifestValidationErrorCode::Oversized
        );

        let mut value = minimal_manifest_value();
        value["displayName"] = json!("a".repeat(700));
        assert_eq!(
            AdapterManifest::validate_value(value).unwrap_err().code(),
            ManifestValidationErrorCode::SchemaViolation
        );

        let mut value = minimal_manifest_value();
        value["launch"]["args"] =
            json!((0..40).map(|idx| format!("--arg{idx}")).collect::<Vec<_>>());
        assert_eq!(
            AdapterManifest::validate_value(value).unwrap_err().code(),
            ManifestValidationErrorCode::SchemaViolation
        );
    }

    #[test]
    fn shell_command_and_metacharacter_launches_are_rejected() {
        for arg in [
            "cmd /C run",
            "powershell -Command run",
            "sh -c run",
            "run | more",
        ] {
            let mut value = minimal_manifest_value();
            value["launch"]["args"] = json!([arg]);
            assert_eq!(
                AdapterManifest::validate_value(value).unwrap_err().code(),
                ManifestValidationErrorCode::InvalidLaunch
            );
        }
    }

    #[test]
    fn secret_values_are_rejected_without_echo() {
        let mut value = minimal_manifest_value();
        value["launch"]["args"] = json!(["--fixture-token=x"]);
        let error = AdapterManifest::validate_value(value).unwrap_err();
        assert_eq!(error.code(), ManifestValidationErrorCode::SecretLiteral);
        let debug = format!("{error:?}");
        assert!(!debug.contains("--fixture-token=x"));
        assert!(!debug.contains("fixture-token"));
    }

    #[test]
    fn invalid_hash_path_traversal_unc_and_url_executable_fail_closed() {
        let mut value = minimal_manifest_value();
        value["match"]["sha256"] = json!("1234");
        assert_eq!(
            AdapterManifest::validate_value(value).unwrap_err().code(),
            ManifestValidationErrorCode::SchemaViolation
        );
        for executable_ref in [
            "..\\agent.exe",
            "\\\\server\\share\\agent.exe",
            "https://example.invalid/agent.exe",
            "plugin.dll",
            "C:\\Program Files\\agent.exe",
        ] {
            let mut value = minimal_manifest_value();
            value["launch"]["executableRef"] = json!(executable_ref);
            assert!(AdapterManifest::validate_value(value).is_err());
        }
    }

    #[test]
    fn schema_and_typed_manifest_validation_agree() {
        let valid = minimal_manifest_value();
        assert!(validate_against_embedded_schema(&valid).is_ok());
        assert!(AdapterManifest::validate_value(valid).is_ok());

        let mut invalid = minimal_manifest_value();
        invalid["protocol"]["kind"] = json!("unknown");
        assert!(validate_against_embedded_schema(&invalid).is_err());
        assert!(AdapterManifest::validate_value(invalid).is_err());
    }

    #[test]
    fn embedded_schema_does_not_depend_on_current_directory() {
        let original = std::env::current_dir().expect("cwd");
        let temp = std::env::temp_dir();
        std::env::set_current_dir(&temp).expect("set temp cwd");
        let result = AdapterManifest::validate_value(minimal_manifest_value());
        std::env::set_current_dir(original).expect("restore cwd");
        result.expect("embedded schema works from arbitrary cwd");
    }

    #[test]
    fn manifest_fixture_files_validate_against_schema_and_typed_boundary() {
        let valid =
            include_bytes!("../../../../fixtures/discovery/adapter-manifests/valid-minimal.json");
        let manifest = AdapterManifest::validate_json_bytes(valid).expect("valid fixture");
        assert_eq!(manifest.id, "org.fixture.agent");

        let unknown = include_bytes!(
            "../../../../fixtures/discovery/adapter-manifests/invalid-unknown-root.json"
        );
        assert_eq!(
            AdapterManifest::validate_json_bytes(unknown)
                .unwrap_err()
                .code(),
            ManifestValidationErrorCode::SchemaViolation
        );

        let shell = include_bytes!(
            "../../../../fixtures/discovery/adapter-manifests/invalid-shell-launch.json"
        );
        assert_eq!(
            AdapterManifest::validate_json_bytes(shell)
                .unwrap_err()
                .code(),
            ManifestValidationErrorCode::InvalidLaunch
        );

        let secret = include_bytes!(
            "../../../../fixtures/discovery/adapter-manifests/invalid-sensitive-literal.json"
        );
        let error = AdapterManifest::validate_json_bytes(secret).unwrap_err();
        assert_eq!(error.code(), ManifestValidationErrorCode::SecretLiteral);
        assert!(!format!("{error:?}").contains("fixture-secret-value"));
    }
}
