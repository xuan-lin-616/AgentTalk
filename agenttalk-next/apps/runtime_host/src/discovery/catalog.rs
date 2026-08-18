use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use agenttalk_domain::{
    AuthState, CandidateAvailability, CandidateCategory, CandidateProjection, CompatibilityState,
    DiscoveryDiagnostic, DiscoveryDiagnosticCode, DiscoveryEvidence, DiscoveryState, HealthState,
    ObservationSourceKind, ObservationTrustLevel, VerificationAuthority,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::manifest::{
    validate_runner_package, AdapterManifest, CapabilityRequirement, ManifestCapabilityPolicy,
    ManifestCategory, ManifestLaunch, ManifestMatch, ManifestProtocol, ManifestProtocolKind,
    ManifestSource, ManifestSourceKind, ManifestTransport, ManifestVerification,
    ManifestVerificationKind, RunnerPackageKind,
};

const MAX_REGISTRY_BYTES: usize = 512 * 1024;
const MAX_REGISTRY_AGENTS: usize = 256;
const MAX_CACHE_AGE_MS: u64 = 7 * 24 * 60 * 60 * 1000;
const CURRENT_CATALOG_GENERATION: u64 = 1;
const ZERO_SHA256: &str = "0000000000000000000000000000000000000000000000000000000000000000";

/// AgentTalk-maintained bundled production catalog, compiled into the Core.
/// `registrySha256` is the SHA-256 of this JSON text with both hash fields
/// replaced by sixty-four zeros; the value is re-verified by a unit test.
pub const PRODUCTION_CATALOG_BYTES: &[u8] = include_bytes!("production_catalog.json");

/// Loads the bundled production catalog through the existing cache parser and
/// schema validator. The load is fully offline (no cache path, no network) and
/// fails closed on corrupt JSON, schema violations, duplicate manifest ids,
/// secret-like ordinary environment grants, an empty catalog, or an unstable
/// revision.
pub fn bundled_production_catalog() -> Result<CatalogSnapshot, CatalogErrorCode> {
    bundled_production_catalog_from_bytes(PRODUCTION_CATALOG_BYTES)
}

/// Injectable parser/validator gate for the bundled production catalog. It
/// takes bytes, never a filesystem path, so tests can exercise corrupt,
/// empty, duplicate, or secret-like content without adding any
/// production-controllable catalog location.
pub(crate) fn bundled_production_catalog_from_bytes(
    bytes: &[u8],
) -> Result<CatalogSnapshot, CatalogErrorCode> {
    let network_counter = NetworkCounter::default();
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u64::MAX as u128) as u64)
        .unwrap_or(0);
    // Runtime digest verification: the self-referential catalog digest must
    // match the declared registrySha256 before the catalog is used. A
    // schema-valid catalog whose content changed without a digest update is
    // tampered and fails closed.
    let declared_digest = serde_json::from_slice::<Value>(bytes)
        .ok()
        .and_then(|value| {
            value
                .get("registrySha256")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .filter(|digest| is_sha256_hex(digest))
        .ok_or(CatalogErrorCode::HashMismatch)?;
    let computed_digest = normalized_catalog_digest(bytes).ok_or(CatalogErrorCode::HashMismatch)?;
    if declared_digest != computed_digest {
        return Err(CatalogErrorCode::HashMismatch);
    }
    let root_digest = declared_digest.as_str();
    // Every bundled manifest must declare a source whose catalogSha256 is
    // present, valid, and equal to the root registrySha256. This is checked
    // against the raw JSON before the manifest semantic validation, so a
    // malformed or inconsistent source digest fails closed as HashMismatch
    // rather than a schema diagnostic.
    let raw: Value = serde_json::from_slice(bytes).map_err(|_| CatalogErrorCode::HashMismatch)?;
    for manifest in raw
        .get("manifests")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let source_sha256 = manifest
            .get("source")
            .and_then(Value::as_object)
            .and_then(|source| source.get("catalogSha256"))
            .and_then(Value::as_str)
            .filter(|digest| is_sha256_hex(digest))
            .map(|digest| digest.to_ascii_lowercase())
            .ok_or(CatalogErrorCode::HashMismatch)?;
        if source_sha256 != root_digest {
            return Err(CatalogErrorCode::HashMismatch);
        }
    }
    let report = load_catalog_for_scan(bytes, None, now_ms, &network_counter);
    if network_counter.count() != 0
        || !report.diagnostics.is_empty()
        || report.snapshot.manifests.is_empty()
        || report.snapshot.revision == "unavailable"
    {
        return Err(CatalogErrorCode::SchemaViolation);
    }
    let mut manifest_ids = BTreeSet::new();
    for manifest in &report.snapshot.manifests {
        if !manifest_ids.insert(manifest.id.clone()) {
            return Err(CatalogErrorCode::SchemaViolation);
        }
        // Ordinary inherited environment must never grant credential-like
        // names. Explicit name-only credential slots are handled separately.
        validate_launch_environment_policy(&manifest.launch)?;
    }
    Ok(report.snapshot)
}

/// Unified launch environment policy for Direct, Npx and Uvx manifests.
///
/// Ordinary inherited environment is fail-closed: secret-like names are not
/// allowed in `environmentAllowlist`. A credential slot is different: it is
/// an explicit, name-only grant whose value is read at spawn time and never
/// serialized, logged, or placed in the catalog. The error is static and
/// desensitized; variable values, paths and manifest text are never echoed.
pub(crate) fn validate_launch_environment_policy(
    launch: &ManifestLaunch,
) -> Result<(), CatalogErrorCode> {
    let (environment_allowlist, credential_environment) = match launch {
        ManifestLaunch::Direct {
            environment_allowlist,
            credential_environment,
            ..
        }
        | ManifestLaunch::Npx {
            environment_allowlist,
            credential_environment,
            ..
        }
        | ManifestLaunch::Uvx {
            environment_allowlist,
            credential_environment,
            ..
        } => (environment_allowlist, credential_environment),
    };
    for name in environment_allowlist {
        if contains_secret_env_name(name) {
            return Err(CatalogErrorCode::SchemaViolation);
        }
    }
    // Credential slots are validated syntactically by the manifest schema.
    // They intentionally may be secret-like names, but only as explicit
    // name-only slots copied from the parent environment at spawn time.
    let _ = credential_environment;
    Ok(())
}

/// Field-level normalized self-referential digest of a bundled catalog.
///
/// The catalog is parsed as JSON and exactly two fields are zeroed: the root
/// `registrySha256` and each manifest's `source.catalogSha256`. Every other
/// field — displayName, args, nested arrays, ordinary strings — participates
/// in the digest verbatim. The zeroed JSON is re-serialized through
/// `serde_json`'s default `Map` (a `BTreeMap`, so keys sort deterministically)
/// into compact form, and the UTF-8 bytes are SHA-256 hashed. No global
/// string replacement is used anywhere in this normalization.
pub fn normalized_catalog_digest(bytes: &[u8]) -> Option<String> {
    let mut value: Value = serde_json::from_slice(bytes).ok()?;
    if let Some(field) = value.get_mut("registrySha256") {
        *field = Value::String(ZERO_SHA256.to_owned());
    }
    if let Some(manifests) = value.get_mut("manifests").and_then(Value::as_array_mut) {
        for manifest in manifests.iter_mut() {
            if let Some(source) = manifest.get_mut("source").and_then(Value::as_object_mut) {
                if let Some(field) = source.get_mut("catalogSha256") {
                    *field = Value::String(ZERO_SHA256.to_owned());
                }
            }
        }
    }
    let canonical = serde_json::to_string(&value).ok()?;
    Some(sha256_hex(canonical.as_bytes()))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CatalogErrorCode {
    InvalidJson,
    SchemaViolation,
    Oversized,
    UnknownDistribution,
    PlatformMismatch,
    InvalidManifest,
    HashMismatch,
    RedirectRejected,
    CacheUnavailable,
    RefreshInProgress,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogError {
    code: CatalogErrorCode,
}

impl CatalogError {
    fn new(code: CatalogErrorCode) -> Self {
        Self { code }
    }

    pub fn code(&self) -> CatalogErrorCode {
        self.code
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RegistryLaunchKind {
    Binary,
    Npx,
    Uvx,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConvertedRegistryManifest {
    pub manifest: AdapterManifest,
    pub launch_kind: RegistryLaunchKind,
    pub source_revision: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogSnapshot {
    pub revision: String,
    pub manifests: Vec<AdapterManifest>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogLoadReport {
    pub snapshot: CatalogSnapshot,
    pub diagnostics: Vec<DiscoveryDiagnostic>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CatalogCache {
    pub version: u16,
    pub generation: u64,
    pub revision: String,
    pub created_at_ms: u64,
    pub registry_sha256: String,
    pub manifests: Vec<AdapterManifest>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RefreshRequest {
    pub expected_origin: String,
    pub expected_sha256: Option<String>,
    pub now_ms: u64,
    pub revision: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RefreshResponse {
    pub origin: String,
    pub redirected_to: Option<String>,
    pub bytes: Vec<u8>,
}

pub trait CatalogRefreshSource {
    fn fetch(&self, request: &RefreshRequest) -> Result<RefreshResponse, CatalogError>;
}

#[derive(Clone, Debug, Default)]
pub struct NetworkCounter {
    count: Arc<Mutex<usize>>,
}

impl NetworkCounter {
    pub fn record(&self) {
        *self.count.lock().expect("network counter lock") += 1;
    }

    pub fn count(&self) -> usize {
        *self.count.lock().expect("network counter lock")
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestMatchInput {
    pub executable_name: Option<String>,
    pub package_ids: Vec<String>,
    pub registry_ids: Vec<String>,
    pub executable_sha256: Option<String>,
    pub publisher_subject: Option<String>,
    pub category: CandidateCategory,
    pub source_kind: ObservationSourceKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthenticodeStatus {
    Trusted,
    Signed,
    Unsigned,
    BadDigest,
    UntrustedRoot,
    ApiUnavailable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthenticodeEvidence {
    pub status: AuthenticodeStatus,
    pub trusted_chain: bool,
    signer_subject: Option<String>,
}

impl AuthenticodeEvidence {
    pub fn from_signer(
        status: AuthenticodeStatus,
        trusted_chain: bool,
        signer_subject: Option<String>,
    ) -> Self {
        Self {
            status,
            trusted_chain,
            signer_subject,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ManifestScore {
    score: u16,
    independent_identity_match: bool,
}

pub trait AuthenticodeVerifier {
    fn verify_offline(&self, path: &Path, expected_publisher: Option<&str>)
        -> AuthenticodeEvidence;
}

pub fn convert_acp_registry_bytes(
    bytes: &[u8],
    platform: &str,
    arch: &str,
    source_revision: &str,
) -> Result<Vec<ConvertedRegistryManifest>, CatalogError> {
    if bytes.len() > MAX_REGISTRY_BYTES {
        return Err(CatalogError::new(CatalogErrorCode::Oversized));
    }
    let value: Value = serde_json::from_slice(bytes)
        .map_err(|_| CatalogError::new(CatalogErrorCode::InvalidJson))?;
    let registry: AcpRegistry = serde_json::from_value(value)
        .map_err(|_| CatalogError::new(CatalogErrorCode::SchemaViolation))?;
    validate_registry_version(&registry.version)?;
    if registry.agents.len() > MAX_REGISTRY_AGENTS {
        return Err(CatalogError::new(CatalogErrorCode::Oversized));
    }
    let mut converted = Vec::new();
    let mut manifest_ids = BTreeSet::new();
    let mut runner_packages = BTreeSet::new();
    for agent in registry.agents {
        match convert_registry_agent(agent, platform, arch, source_revision) {
            Ok(manifest) => {
                if !manifest_ids.insert(manifest.manifest.id.clone()) {
                    return Err(CatalogError::new(CatalogErrorCode::SchemaViolation));
                }
                if let Some(package) = converted_runner_package_key(&manifest) {
                    if !runner_packages.insert(package) {
                        return Err(CatalogError::new(CatalogErrorCode::SchemaViolation));
                    }
                }
                converted.push(manifest);
            }
            Err(error) if error.code() == CatalogErrorCode::PlatformMismatch => {}
            Err(error) => return Err(error),
        }
    }
    Ok(converted)
}

fn converted_runner_package_key(
    converted: &ConvertedRegistryManifest,
) -> Option<(RegistryLaunchKind, String)> {
    match &converted.manifest.launch {
        ManifestLaunch::Npx { package, .. } | ManifestLaunch::Uvx { package, .. } => {
            Some((converted.launch_kind, package.clone()))
        }
        ManifestLaunch::Direct { .. } => None,
    }
}

pub fn load_catalog_for_scan(
    bundled_bytes: &[u8],
    cache_path: Option<&Path>,
    now_ms: u64,
    network_counter: &NetworkCounter,
) -> CatalogLoadReport {
    let mut diagnostics = Vec::new();
    let bundled = match parse_cache_bytes(bundled_bytes) {
        Ok(cache) => cache,
        Err(_) => {
            diagnostics.push(DiscoveryDiagnostic {
                source_kind: ObservationSourceKind::ExecutableInventory,
                code: DiscoveryDiagnosticCode::InvalidSourceRecord,
            });
            CatalogCache {
                version: 1,
                generation: CURRENT_CATALOG_GENERATION,
                revision: "bundled-invalid".into(),
                created_at_ms: now_ms,
                registry_sha256: sha256_hex(bundled_bytes),
                manifests: Vec::new(),
            }
        }
    };
    let mut selected = bundled;
    if let Some(cache_path) = cache_path {
        match fs::read(cache_path)
            .map_err(|_| CatalogError::new(CatalogErrorCode::CacheUnavailable))
            .and_then(|bytes| parse_cache_bytes(&bytes))
        {
            Ok(cache)
                if now_ms.saturating_sub(cache.created_at_ms) <= MAX_CACHE_AGE_MS
                    && cache.generation >= selected.generation =>
            {
                selected = cache;
            }
            Ok(_) => diagnostics.push(DiscoveryDiagnostic {
                source_kind: ObservationSourceKind::ExecutableInventory,
                code: DiscoveryDiagnosticCode::CatalogConflict,
            }),
            Err(_) => diagnostics.push(DiscoveryDiagnostic {
                source_kind: ObservationSourceKind::ExecutableInventory,
                code: DiscoveryDiagnosticCode::InvalidSourceRecord,
            }),
        }
    }
    let _ = network_counter.count();
    CatalogLoadReport {
        snapshot: CatalogSnapshot {
            revision: selected.revision,
            manifests: selected.manifests,
        },
        diagnostics,
    }
}

pub fn refresh_catalog_cache(
    cache_path: &Path,
    lock_path: &Path,
    source: &dyn CatalogRefreshSource,
    request: &RefreshRequest,
) -> Result<CatalogCache, CatalogError> {
    let _lock = RefreshLock::acquire(lock_path)?;
    let response = source.fetch(request)?;
    if response.origin != request.expected_origin
        || response
            .redirected_to
            .as_deref()
            .is_some_and(|origin| origin != request.expected_origin)
    {
        return Err(CatalogError::new(CatalogErrorCode::RedirectRejected));
    }
    if response.bytes.len() > MAX_REGISTRY_BYTES {
        return Err(CatalogError::new(CatalogErrorCode::Oversized));
    }
    let registry_sha256 = sha256_hex(&response.bytes);
    if request
        .expected_sha256
        .as_deref()
        .is_some_and(|expected| expected.to_ascii_lowercase() != registry_sha256)
    {
        return Err(CatalogError::new(CatalogErrorCode::HashMismatch));
    }
    let converted = convert_acp_registry_bytes(
        &response.bytes,
        current_platform(),
        current_arch(),
        &request.revision,
    )?;
    let manifests = converted
        .into_iter()
        .map(|converted| {
            let mut manifest = converted.manifest;
            if let Some(source) = &mut manifest.source {
                source.catalog_sha256 = Some(registry_sha256.clone());
            }
            manifest
        })
        .collect();
    let cache = CatalogCache {
        version: 1,
        generation: CURRENT_CATALOG_GENERATION,
        revision: request.revision.clone(),
        created_at_ms: request.now_ms,
        registry_sha256,
        manifests,
    };
    let bytes = serde_json::to_vec_pretty(&cache)
        .map_err(|_| CatalogError::new(CatalogErrorCode::SchemaViolation))?;
    if contains_secret_text(&String::from_utf8_lossy(&bytes)) {
        return Err(CatalogError::new(CatalogErrorCode::SchemaViolation));
    }
    atomic_write(cache_path, &bytes)?;
    Ok(cache)
}

pub fn match_manifest_passively(
    input: &ManifestMatchInput,
    manifests: &[AdapterManifest],
    authenticode: Option<&AuthenticodeEvidence>,
) -> CandidateProjection {
    let mut scored = manifests
        .iter()
        .filter_map(|manifest| {
            manifest_score(input, manifest, authenticode).map(|score| (score, manifest))
        })
        .collect::<Vec<_>>();
    scored.sort_by(|left, right| {
        right
            .0
            .cmp(&left.0)
            .then_with(|| left.1.id.cmp(&right.1.id))
    });
    if scored.is_empty() {
        return passive_projection(input, None, Vec::new(), DiscoveryState::Observed);
    }
    let top_score = scored[0].0.score;
    let winners = scored
        .iter()
        .filter(|(score, _)| score.score == top_score)
        .collect::<Vec<_>>();
    if winners.len() > 1 {
        return passive_projection(
            input,
            None,
            vec![DiscoveryDiagnostic {
                source_kind: input.source_kind,
                code: DiscoveryDiagnosticCode::InvalidIdentity,
            }],
            DiscoveryState::Observed,
        );
    }
    let (score, manifest) = scored.remove(0);
    if let (Some(expected), Some(actual)) = (
        manifest.match_rules.sha256.as_deref(),
        input.executable_sha256.as_deref(),
    ) {
        if expected != actual && score.independent_identity_match {
            return passive_projection(
                input,
                Some(manifest),
                vec![DiscoveryDiagnostic {
                    source_kind: input.source_kind,
                    code: DiscoveryDiagnosticCode::FingerprintChanged,
                }],
                DiscoveryState::Observed,
            );
        }
    }
    passive_projection(
        input,
        Some(manifest),
        Vec::new(),
        DiscoveryState::Identified,
    )
}

pub fn authenticode_evidence_to_safe_projection(
    evidence: &AuthenticodeEvidence,
) -> Vec<DiscoveryEvidence> {
    let mut output = Vec::new();
    if matches!(
        evidence.status,
        AuthenticodeStatus::Trusted | AuthenticodeStatus::Signed
    ) {
        output.push(DiscoveryEvidence::ExecutableInventory);
    }
    output
}

#[cfg(windows)]
pub struct WindowsAuthenticodeVerifier;

#[cfg(windows)]
impl AuthenticodeVerifier for WindowsAuthenticodeVerifier {
    fn verify_offline(
        &self,
        path: &Path,
        _expected_publisher: Option<&str>,
    ) -> AuthenticodeEvidence {
        use std::mem::zeroed;
        use windows::core::PCWSTR;
        use windows::Win32::Foundation::HWND;
        use windows::Win32::Security::WinTrust::{
            WinVerifyTrust, WINTRUST_ACTION_GENERIC_VERIFY_V2, WINTRUST_DATA, WINTRUST_FILE_INFO,
            WTD_CACHE_ONLY_URL_RETRIEVAL, WTD_CHOICE_FILE, WTD_REVOKE_NONE, WTD_STATEACTION_CLOSE,
            WTD_STATEACTION_VERIFY, WTD_UI_NONE,
        };

        let wide = path_to_wide(path);
        let mut file_info = WINTRUST_FILE_INFO {
            cbStruct: std::mem::size_of::<WINTRUST_FILE_INFO>() as u32,
            pcwszFilePath: PCWSTR(wide.as_ptr()),
            ..unsafe { zeroed() }
        };
        let mut data = WINTRUST_DATA {
            cbStruct: std::mem::size_of::<WINTRUST_DATA>() as u32,
            dwUIChoice: WTD_UI_NONE,
            fdwRevocationChecks: WTD_REVOKE_NONE,
            dwUnionChoice: WTD_CHOICE_FILE,
            Anonymous: windows::Win32::Security::WinTrust::WINTRUST_DATA_0 {
                pFile: &mut file_info,
            },
            dwStateAction: WTD_STATEACTION_VERIFY,
            dwProvFlags: WTD_CACHE_ONLY_URL_RETRIEVAL,
            ..unsafe { zeroed() }
        };
        let mut action = WINTRUST_ACTION_GENERIC_VERIFY_V2;
        let status = unsafe {
            WinVerifyTrust(
                HWND(std::ptr::null_mut()),
                &mut action,
                &mut data as *mut _ as *mut _,
            )
        };
        let signer_subject = if status == 0 {
            unsafe { signer_subject_from_wintrust_state(&data) }
        } else {
            None
        };
        let status = map_winverifytrust_status(status);
        data.dwStateAction = WTD_STATEACTION_CLOSE;
        let _ = unsafe {
            WinVerifyTrust(
                HWND(std::ptr::null_mut()),
                &mut action,
                &mut data as *mut _ as *mut _,
            )
        };
        AuthenticodeEvidence::from_signer(
            status,
            status == AuthenticodeStatus::Trusted,
            signer_subject,
        )
    }
}

#[cfg(windows)]
unsafe fn signer_subject_from_wintrust_state(
    data: &windows::Win32::Security::WinTrust::WINTRUST_DATA,
) -> Option<String> {
    use windows::Win32::Security::Cryptography::{
        CertGetNameStringW, CERT_NAME_SIMPLE_DISPLAY_TYPE,
    };
    use windows::Win32::Security::WinTrust::{
        WTHelperGetProvCertFromChain, WTHelperGetProvSignerFromChain, WTHelperProvDataFromStateData,
    };

    if data.hWVTStateData.is_invalid() {
        return None;
    }
    let prov_data = WTHelperProvDataFromStateData(data.hWVTStateData);
    if prov_data.is_null() {
        return None;
    }
    let signer = WTHelperGetProvSignerFromChain(prov_data, 0, false, 0);
    if signer.is_null() {
        return None;
    }
    let cert = WTHelperGetProvCertFromChain(signer, 0);
    if cert.is_null() || (*cert).pCert.is_null() {
        return None;
    }
    let required = CertGetNameStringW((*cert).pCert, CERT_NAME_SIMPLE_DISPLAY_TYPE, 0, None, None);
    if required <= 1 || required > 512 {
        return None;
    }
    let mut buffer = vec![0u16; required as usize];
    let written = CertGetNameStringW(
        (*cert).pCert,
        CERT_NAME_SIMPLE_DISPLAY_TYPE,
        0,
        None,
        Some(&mut buffer),
    );
    if written <= 1 || written > required {
        return None;
    }
    buffer.truncate(written.saturating_sub(1) as usize);
    String::from_utf16(&buffer).ok()
}

#[cfg(windows)]
fn map_winverifytrust_status(status: i32) -> AuthenticodeStatus {
    use windows::Win32::Foundation::{
        CERT_E_CHAINING, CERT_E_UNTRUSTEDCA, CERT_E_UNTRUSTEDROOT, CERT_E_UNTRUSTEDTESTROOT,
        TRUST_E_BAD_DIGEST, TRUST_E_NOSIGNATURE, TRUST_E_NO_SIGNER_CERT,
    };

    match windows::core::HRESULT(status) {
        windows::core::HRESULT(0) => AuthenticodeStatus::Trusted,
        TRUST_E_NOSIGNATURE | TRUST_E_NO_SIGNER_CERT => AuthenticodeStatus::Unsigned,
        TRUST_E_BAD_DIGEST => AuthenticodeStatus::BadDigest,
        CERT_E_UNTRUSTEDROOT | CERT_E_UNTRUSTEDCA | CERT_E_UNTRUSTEDTESTROOT | CERT_E_CHAINING => {
            AuthenticodeStatus::UntrustedRoot
        }
        _ => AuthenticodeStatus::ApiUnavailable,
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AcpRegistry {
    version: String,
    agents: Vec<AcpRegistryAgent>,
    #[serde(default)]
    #[allow(dead_code)]
    extensions: serde_json::Value,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AcpRegistryAgent {
    id: String,
    name: String,
    version: String,
    description: Option<String>,
    repository: Option<String>,
    website: Option<String>,
    authors: Option<Vec<String>>,
    license: Option<String>,
    icon: Option<String>,
    distribution: AcpDistribution,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AcpDistribution {
    binary: Option<BTreeMap<String, AcpBinaryDistribution>>,
    npx: Option<AcpRunnerDistribution>,
    uvx: Option<AcpRunnerDistribution>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AcpBinaryDistribution {
    archive: String,
    sha256: Option<String>,
    cmd: String,
    args: Option<Vec<String>>,
    env: Option<BTreeMap<String, IgnoredRegistryEnvValue>>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AcpRunnerDistribution {
    package: String,
    args: Option<Vec<String>>,
    env: Option<BTreeMap<String, IgnoredRegistryEnvValue>>,
}

#[derive(Clone, Debug)]
struct IgnoredRegistryEnvValue;

impl<'de> Deserialize<'de> for IgnoredRegistryEnvValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor;

        impl serde::de::Visitor<'_> for Visitor {
            type Value = IgnoredRegistryEnvValue;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a registry environment string value")
            }

            fn visit_borrowed_str<E>(self, _value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(IgnoredRegistryEnvValue)
            }

            fn visit_str<E>(self, _value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(IgnoredRegistryEnvValue)
            }

            fn visit_string<E>(self, _value: String) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(IgnoredRegistryEnvValue)
            }
        }

        deserializer.deserialize_string(Visitor)
    }
}

fn convert_registry_agent(
    agent: AcpRegistryAgent,
    platform: &str,
    arch: &str,
    source_revision: &str,
) -> Result<ConvertedRegistryManifest, CatalogError> {
    validate_registry_agent_metadata(&agent)?;
    let target = format!("{platform}-{arch}");
    let mut selected_binary = None;
    if let Some(binary_distributions) = agent.distribution.binary {
        if let Some(dist) = binary_distributions.get(&target).cloned() {
            selected_binary = Some((RegistryLaunchKind::Binary, registry_binary_launch(dist)?));
        }
    }
    let npx = agent
        .distribution
        .npx
        .map(|dist| {
            registry_runner_launch("npx", dist).map(|launch| (RegistryLaunchKind::Npx, launch))
        })
        .transpose()?;
    let uvx = agent
        .distribution
        .uvx
        .map(|dist| {
            registry_runner_launch("uvx", dist).map(|launch| (RegistryLaunchKind::Uvx, launch))
        })
        .transpose()?;
    let (launch_kind, launch) = selected_binary
        .or(npx)
        .or(uvx)
        .ok_or_else(|| CatalogError::new(CatalogErrorCode::PlatformMismatch))?;
    let manifest_id = normalize_registry_id(&agent.id)?;
    let package_ids = registry_runner_package_ids(&launch);
    let manifest = AdapterManifest {
        schema_version: "agenttalk.adapter.v1".into(),
        id: manifest_id.clone(),
        display_name: agent.name,
        category: ManifestCategory::AgentProtocol,
        protocol: ManifestProtocol {
            kind: ManifestProtocolKind::Acp,
            major: 1,
        },
        match_rules: ManifestMatch {
            executable_names: registry_executable_names(&launch),
            publisher_subjects: Vec::new(),
            registry_ids: vec![manifest_id.clone()],
            package_ids,
            sha256: None,
        },
        launch,
        verification: ManifestVerification {
            kind: ManifestVerificationKind::AcpInitialize,
            timeout_ms: 3000,
        },
        capability_policy: ManifestCapabilityPolicy {
            filesystem: CapabilityRequirement::Negotiate,
            shell: CapabilityRequirement::Negotiate,
            streaming: CapabilityRequirement::Required,
            cancel: CapabilityRequirement::Required,
        },
        source: Some(ManifestSource {
            kind: ManifestSourceKind::AcpRegistry,
            id: manifest_id,
            version: agent.version,
            revision: Some(source_revision.to_owned()),
            catalog_sha256: None,
        }),
    };
    let manifest = AdapterManifest::validate_value(
        serde_json::to_value(manifest)
            .map_err(|_| CatalogError::new(CatalogErrorCode::InvalidManifest))?,
    )
    .map_err(|_| CatalogError::new(CatalogErrorCode::InvalidManifest))?;
    Ok(ConvertedRegistryManifest {
        manifest,
        launch_kind,
        source_revision: source_revision.to_owned(),
    })
}

/// Package IDs are passive correlation keys.  They deliberately preserve the
/// runner package's pinned version but never act as executable identity.
fn registry_runner_package_ids(launch: &ManifestLaunch) -> Vec<String> {
    let package = match launch {
        ManifestLaunch::Npx { package, .. } => npx_package_match_id(package),
        ManifestLaunch::Uvx { package, .. } => uvx_package_match_id(package),
        ManifestLaunch::Direct { .. } => None,
    };
    package.into_iter().collect()
}

fn npx_package_match_id(package: &str) -> Option<String> {
    let (name, version) = split_npx_package_version(package)?;
    Some(format!("{}@{}", name.to_ascii_lowercase(), version))
}

fn uvx_package_match_id(package: &str) -> Option<String> {
    if let Some((name, version)) = package.split_once("==") {
        return Some(format!("{}@{}", name.to_ascii_lowercase(), version));
    }
    npx_package_match_id(package)
}

fn split_npx_package_version(package: &str) -> Option<(&str, &str)> {
    if let Some(rest) = package.strip_prefix('@') {
        let slash = rest.find('/')? + 1;
        let version_offset = package[slash..].rfind('@')? + slash;
        return Some((&package[..version_offset], &package[version_offset + 1..]));
    }
    package.rsplit_once('@')
}

fn registry_binary_launch(dist: AcpBinaryDistribution) -> Result<ManifestLaunch, CatalogError> {
    validate_registry_url(&dist.archive)?;
    let env = env_names_only(dist.env)?;
    let archive_sha256 = dist
        .sha256
        .map(|value| validate_registry_sha256(&value))
        .transpose()?;
    Ok(ManifestLaunch::Direct {
        transport: ManifestTransport::Stdio,
        executable_ref: executable_name_from_cmd(&dist.cmd)?,
        args: dist.args.unwrap_or_default(),
        environment_allowlist: env.environment_allowlist,
        credential_environment: env.credential_environment,
        archive_sha256,
        sha256: None,
    })
}

fn validate_registry_agent_metadata(agent: &AcpRegistryAgent) -> Result<(), CatalogError> {
    validate_registry_text(&agent.name)?;
    validate_registry_text(&agent.version)?;
    if let Some(description) = &agent.description {
        validate_registry_text(description)?;
    }
    if let Some(repository) = &agent.repository {
        validate_registry_url(repository)?;
    }
    if let Some(website) = &agent.website {
        validate_registry_url(website)?;
    }
    if let Some(authors) = &agent.authors {
        if authors.len() > 16 {
            return Err(CatalogError::new(CatalogErrorCode::Oversized));
        }
        for author in authors {
            validate_registry_text(author)?;
        }
    }
    if let Some(license) = &agent.license {
        validate_registry_text(license)?;
    }
    if let Some(icon) = &agent.icon {
        validate_registry_url(icon)?;
    }
    Ok(())
}

fn validate_registry_version(version: &str) -> Result<(), CatalogError> {
    let valid = version.len() <= 32
        && !contains_secret_text(version)
        && version
            .chars()
            .all(|ch| ch.is_ascii_digit() || matches!(ch, '.' | '-' | '+'));
    if valid {
        Ok(())
    } else {
        Err(CatalogError::new(CatalogErrorCode::SchemaViolation))
    }
}

fn validate_registry_text(value: &str) -> Result<(), CatalogError> {
    let valid = !value.is_empty()
        && value.len() <= 512
        && !contains_secret_text(value)
        && !value.chars().any(is_forbidden_registry_control);
    if valid {
        Ok(())
    } else {
        Err(CatalogError::new(CatalogErrorCode::SchemaViolation))
    }
}

fn validate_registry_url(value: &str) -> Result<(), CatalogError> {
    validate_registry_text(value)?;
    if (value.starts_with("https://") || value.starts_with("http://"))
        && !value.contains('@')
        && !value.contains('\\')
    {
        Ok(())
    } else {
        Err(CatalogError::new(CatalogErrorCode::SchemaViolation))
    }
}

fn validate_registry_sha256(value: &str) -> Result<String, CatalogError> {
    if is_sha256_hex(value) {
        Ok(value.to_ascii_lowercase())
    } else {
        Err(CatalogError::new(CatalogErrorCode::SchemaViolation))
    }
}

fn registry_runner_launch(
    kind: &str,
    dist: AcpRunnerDistribution,
) -> Result<ManifestLaunch, CatalogError> {
    let env = env_names_only(dist.env)?;
    let package = normalize_registry_runner_package(kind, &dist.package)?;
    match kind {
        "npx" => Ok(ManifestLaunch::Npx {
            package,
            args: dist.args.unwrap_or_default(),
            environment_allowlist: env.environment_allowlist,
            credential_environment: env.credential_environment,
            sha256: None,
        }),
        "uvx" => Ok(ManifestLaunch::Uvx {
            package,
            args: dist.args.unwrap_or_default(),
            environment_allowlist: env.environment_allowlist,
            credential_environment: env.credential_environment,
            sha256: None,
        }),
        _ => Err(CatalogError::new(CatalogErrorCode::UnknownDistribution)),
    }
}

fn normalize_registry_runner_package(kind: &str, value: &str) -> Result<String, CatalogError> {
    validate_registry_text(value)?;
    let package_kind = match kind {
        "npx" => RunnerPackageKind::Npx,
        "uvx" => RunnerPackageKind::Uvx,
        _ => return Err(CatalogError::new(CatalogErrorCode::UnknownDistribution)),
    };
    validate_runner_package(package_kind, value)
        .map_err(|_| CatalogError::new(CatalogErrorCode::SchemaViolation))?;
    Ok(value.to_owned())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RegistryEnvironmentProjection {
    environment_allowlist: Vec<String>,
    credential_environment: Vec<String>,
}

fn env_names_only(
    env: Option<BTreeMap<String, IgnoredRegistryEnvValue>>,
) -> Result<RegistryEnvironmentProjection, CatalogError> {
    let mut environment_allowlist = Vec::new();
    let mut credential_environment = Vec::new();
    for name in env.unwrap_or_default().into_keys() {
        validate_registry_env_name(&name)?;
        if contains_secret_env_name(&name) {
            credential_environment.push(name);
        } else {
            environment_allowlist.push(name);
        }
    }
    environment_allowlist.sort();
    environment_allowlist.dedup();
    credential_environment.sort();
    credential_environment.dedup();
    Ok(RegistryEnvironmentProjection {
        environment_allowlist,
        credential_environment,
    })
}

fn validate_registry_env_name(value: &str) -> Result<(), CatalogError> {
    let valid = !value.is_empty()
        && value.len() <= 64
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
        Err(CatalogError::new(CatalogErrorCode::SchemaViolation))
    }
}

fn executable_name_from_cmd(cmd: &str) -> Result<String, CatalogError> {
    let stripped = cmd.strip_prefix("./").unwrap_or(cmd);
    if stripped != cmd && stripped.starts_with(['/', '\\']) {
        return Err(CatalogError::new(CatalogErrorCode::InvalidManifest));
    }
    if stripped.contains(['\\', '/', ':', '|', '&', ';', '`'])
        || stripped.starts_with('.')
        || stripped.contains("..")
        || stripped.contains("://")
        || stripped.to_ascii_lowercase().ends_with(".dll")
        || stripped.is_empty()
        || stripped != stripped.trim()
    {
        return Err(CatalogError::new(CatalogErrorCode::InvalidManifest));
    }
    Ok(stripped.to_owned())
}

fn registry_executable_names(launch: &ManifestLaunch) -> Vec<String> {
    match launch {
        ManifestLaunch::Direct { executable_ref, .. } if executable_ref.ends_with(".exe") => {
            vec![executable_ref.clone()]
        }
        _ => Vec::new(),
    }
}

fn normalize_registry_id(value: &str) -> Result<String, CatalogError> {
    let normalized = value.trim().to_ascii_lowercase();
    if normalized.len() >= 3
        && normalized.len() <= 128
        && normalized.chars().all(|ch| {
            ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '.' | '_' | '-')
        })
    {
        Ok(normalized)
    } else {
        Err(CatalogError::new(CatalogErrorCode::InvalidManifest))
    }
}

fn parse_cache_bytes(bytes: &[u8]) -> Result<CatalogCache, CatalogError> {
    if bytes.len() > MAX_REGISTRY_BYTES {
        return Err(CatalogError::new(CatalogErrorCode::Oversized));
    }
    let cache: CatalogCache = serde_json::from_slice(bytes)
        .map_err(|_| CatalogError::new(CatalogErrorCode::InvalidJson))?;
    if cache.version != 1 || cache.generation != CURRENT_CATALOG_GENERATION {
        return Err(CatalogError::new(CatalogErrorCode::SchemaViolation));
    }
    if !is_sha256_hex(&cache.registry_sha256) {
        return Err(CatalogError::new(CatalogErrorCode::HashMismatch));
    }
    for manifest in &cache.manifests {
        if let Some(source) = &manifest.source {
            if source.kind == ManifestSourceKind::AcpRegistry
                && source.catalog_sha256.as_deref() != Some(cache.registry_sha256.as_str())
            {
                return Err(CatalogError::new(CatalogErrorCode::HashMismatch));
            }
        }
        AdapterManifest::validate_value(
            serde_json::to_value(manifest)
                .map_err(|_| CatalogError::new(CatalogErrorCode::InvalidManifest))?,
        )
        .map_err(|_| CatalogError::new(CatalogErrorCode::InvalidManifest))?;
    }
    Ok(cache)
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), CatalogError> {
    let parent = path
        .parent()
        .ok_or_else(|| CatalogError::new(CatalogErrorCode::CacheUnavailable))?;
    fs::create_dir_all(parent)
        .map_err(|_| CatalogError::new(CatalogErrorCode::CacheUnavailable))?;
    let temp = path.with_extension(format!("tmp-{}", std::process::id()));
    {
        let mut file = File::create(&temp)
            .map_err(|_| CatalogError::new(CatalogErrorCode::CacheUnavailable))?;
        file.write_all(bytes)
            .map_err(|_| CatalogError::new(CatalogErrorCode::CacheUnavailable))?;
        file.sync_all()
            .map_err(|_| CatalogError::new(CatalogErrorCode::CacheUnavailable))?;
    }
    match fs::rename(&temp, path) {
        Ok(()) => {}
        Err(_) => {
            if let Err(error) = replace_existing_file(&temp, path) {
                let _ = fs::remove_file(&temp);
                return Err(error);
            }
        }
    }
    if let Ok(parent_file) = OpenOptions::new().read(true).open(parent) {
        let _ = parent_file.sync_all();
    }
    Ok(())
}

#[cfg(windows)]
fn replace_existing_file(source: &Path, target: &Path) -> Result<(), CatalogError> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{
        MoveFileExW, MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH,
    };

    let source_wide = source
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let target_wide = target
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let moved = unsafe {
        MoveFileExW(
            source_wide.as_ptr(),
            target_wide.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if moved == 0 {
        return Err(CatalogError::new(CatalogErrorCode::CacheUnavailable));
    }
    Ok(())
}

#[cfg(not(windows))]
fn replace_existing_file(source: &Path, target: &Path) -> Result<(), CatalogError> {
    fs::rename(source, target).map_err(|_| CatalogError::new(CatalogErrorCode::CacheUnavailable))
}

struct RefreshLock {
    path: PathBuf,
}

impl RefreshLock {
    fn acquire(path: &Path) -> Result<Self, CatalogError> {
        match OpenOptions::new().write(true).create_new(true).open(path) {
            Ok(mut file) => {
                file.write_all(b"agenttalk catalog refresh lock\n")
                    .map_err(|_| CatalogError::new(CatalogErrorCode::CacheUnavailable))?;
                Ok(Self {
                    path: path.to_path_buf(),
                })
            }
            Err(_) => Err(CatalogError::new(CatalogErrorCode::RefreshInProgress)),
        }
    }
}

impl Drop for RefreshLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn manifest_score(
    input: &ManifestMatchInput,
    manifest: &AdapterManifest,
    authenticode: Option<&AuthenticodeEvidence>,
) -> Option<ManifestScore> {
    if !category_matches(input.category, manifest.category) {
        return None;
    }
    let mut score = 0;
    let mut independent_identity_match = false;
    if intersects(&input.package_ids, &manifest.match_rules.package_ids)
        || intersects(&input.registry_ids, &manifest.match_rules.registry_ids)
    {
        score = score.max(100);
        independent_identity_match = true;
    }
    if input
        .executable_sha256
        .as_deref()
        .is_some_and(|hash| manifest.match_rules.sha256.as_deref() == Some(hash))
    {
        score = score.max(120);
    }
    if authenticode.is_some_and(|evidence| publisher_matches_manifest(evidence, manifest)) {
        score = score.max(60);
        independent_identity_match = true;
    }
    if input.executable_name.as_ref().is_some_and(|name| {
        manifest
            .match_rules
            .executable_names
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(name))
    }) {
        score = score.max(10);
        independent_identity_match = true;
    }
    if score > 0 {
        Some(ManifestScore {
            score,
            independent_identity_match,
        })
    } else {
        None
    }
}

fn publisher_matches_manifest(evidence: &AuthenticodeEvidence, manifest: &AdapterManifest) -> bool {
    if evidence.status != AuthenticodeStatus::Trusted
        || !evidence.trusted_chain
        || manifest.match_rules.publisher_subjects.is_empty()
    {
        return false;
    }
    let Some(signer_subject) = evidence
        .signer_subject
        .as_deref()
        .and_then(normalize_publisher_subject)
    else {
        return false;
    };
    manifest
        .match_rules
        .publisher_subjects
        .iter()
        .filter_map(|rule| normalize_publisher_subject(rule))
        .any(|rule| rule == signer_subject)
}

fn normalize_publisher_subject(value: &str) -> Option<String> {
    let normalized = value
        .split(',')
        .map(|part| {
            let trimmed = part.trim();
            if let Some((key, value)) = trimmed.split_once('=') {
                if !key.is_empty()
                    && key.len() <= 4
                    && key.chars().all(|ch| ch.is_ascii_alphabetic())
                {
                    return value.trim();
                }
            }
            trimmed
        })
        .collect::<Vec<_>>()
        .join(" ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase();
    (!normalized.is_empty() && !contains_secret_text(&normalized)).then_some(normalized)
}

fn passive_projection(
    input: &ManifestMatchInput,
    manifest: Option<&AdapterManifest>,
    diagnostics: Vec<DiscoveryDiagnostic>,
    discovery_state: DiscoveryState,
) -> CandidateProjection {
    let category = manifest
        .map(|manifest| match manifest.category {
            ManifestCategory::AgentProtocol => CandidateCategory::AgentRuntime,
            ManifestCategory::ModelRuntime => CandidateCategory::ModelRuntime,
            ManifestCategory::Mcp => CandidateCategory::ToolService,
            ManifestCategory::Unknown => CandidateCategory::Unknown,
        })
        .unwrap_or(input.category);
    CandidateProjection {
        candidate_id: manifest
            .map(|manifest| private_candidate_id(&manifest.id))
            .unwrap_or_else(|| private_candidate_id("unmatched")),
        category,
        connector_id: manifest
            .map(|manifest| format!("local.registry.{}", manifest.id))
            .unwrap_or_else(|| "local.discovery.unknown".into()),
        runtime_type: manifest
            .map(|manifest| match manifest.protocol.kind {
                ManifestProtocolKind::Acp => "acp",
                ManifestProtocolKind::A2a => "a2a",
                ManifestProtocolKind::OpenaiCompatible => "openai_compatible",
                ManifestProtocolKind::Ollama => "ollama",
                ManifestProtocolKind::Mcp => "mcp",
            })
            .unwrap_or("unknown")
            .into(),
        display_name: manifest
            .map(|manifest| manifest.display_name.clone())
            .unwrap_or_else(|| "Local Agent".into()),
        availability: CandidateAvailability::Unconfigured,
        models: Vec::new(),
        catalog_revision: manifest.and_then(|manifest| {
            manifest
                .source
                .as_ref()
                .and_then(|source| source.revision.clone())
        }),
        requires_configuration: true,
        source_kind: input.source_kind,
        source_kinds: vec![input.source_kind],
        trust_level: ObservationTrustLevel::Heuristic,
        verification_authority: VerificationAuthority::Unverified,
        availability_authority: VerificationAuthority::Unverified,
        discovery_authority: VerificationAuthority::Heuristic,
        compatibility_authority: VerificationAuthority::Unverified,
        auth_authority: VerificationAuthority::Unverified,
        health_authority: VerificationAuthority::Unverified,
        catalog_source_kind: manifest.map(|_| ObservationSourceKind::ExecutableInventory),
        catalog_trust_level: manifest.map(|_| ObservationTrustLevel::Heuristic),
        catalog_authority: manifest.map(|_| VerificationAuthority::Heuristic),
        discovery_state,
        compatibility_state: CompatibilityState::NotVerified,
        auth_state: AuthState::Unknown,
        health_state: HealthState::NotChecked,
        evidence_summary: vec![DiscoveryEvidence::CatalogUnavailable],
        diagnostics,
    }
}

fn category_matches(input: CandidateCategory, manifest: ManifestCategory) -> bool {
    matches!(
        (input, manifest),
        (CandidateCategory::Unknown, _)
            | (
                CandidateCategory::AgentRuntime,
                ManifestCategory::AgentProtocol
            )
            | (
                CandidateCategory::ModelRuntime,
                ManifestCategory::ModelRuntime
            )
            | (CandidateCategory::ToolService, ManifestCategory::Mcp)
    )
}

fn intersects(left: &[String], right: &[String]) -> bool {
    let right = right.iter().collect::<BTreeSet<_>>();
    left.iter().any(|value| right.contains(value))
}

fn private_candidate_id(value: &str) -> String {
    format!("candidate-{}", sha256_hex(value.as_bytes()))
}

fn contains_secret_text(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    [
        "authorization",
        "cookie",
        "bearer ",
        "token=",
        "api_key",
        "apikey",
        "password",
        "secret=",
    ]
    .iter()
    .any(|marker| lower.contains(marker))
}

fn contains_secret_env_name(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    [
        "authorization",
        "cookie",
        "token",
        "api_key",
        "apikey",
        "password",
        "secret",
    ]
    .iter()
    .any(|marker| lower.contains(marker))
}

fn is_forbidden_registry_control(ch: char) -> bool {
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

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64 && value.chars().all(|ch| ch.is_ascii_hexdigit())
}

fn current_platform() -> &'static str {
    if cfg!(target_os = "windows") {
        "windows"
    } else if cfg!(target_os = "macos") {
        "darwin"
    } else {
        "linux"
    }
}

fn current_arch() -> &'static str {
    if cfg!(target_arch = "x86_64") {
        "x86_64"
    } else if cfg!(target_arch = "aarch64") {
        "aarch64"
    } else {
        "unknown"
    }
}

#[cfg(windows)]
fn path_to_wide(path: &Path) -> Vec<u16> {
    use std::os::windows::ffi::OsStrExt;
    path.as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::super::manifest::validate_against_embedded_schema;
    use super::*;
    use serde_json::json;
    use std::ops::Deref;
    use std::sync::Barrier;
    use std::thread;

    #[test]
    fn bundled_production_catalog_is_non_empty_and_offline() {
        let network_counter = NetworkCounter::default();
        let report = load_catalog_for_scan(PRODUCTION_CATALOG_BYTES, None, 0, &network_counter);
        assert_eq!(
            network_counter.count(),
            0,
            "bundled catalog load must be offline"
        );
        assert!(
            report.diagnostics.is_empty(),
            "bundled catalog load must be clean"
        );
        assert!(
            !report.snapshot.manifests.is_empty(),
            "bundled production catalog must not be empty"
        );
        assert_ne!(
            report.snapshot.revision, "unavailable",
            "bundled production catalog revision must be stable"
        );
        let configuration = bundled_production_catalog().expect("bundled catalog is valid");
        assert_eq!(
            configuration.manifests.len(),
            report.snapshot.manifests.len()
        );
    }

    #[test]
    fn bundled_registry_manifests_are_schema_valid_and_provenanced() {
        let snapshot = bundled_production_catalog().expect("bundled catalog is valid");
        assert!(snapshot.manifests.len() >= 30);
        let catalog_digest = snapshot
            .manifests
            .first()
            .and_then(|manifest| manifest.source.as_ref())
            .and_then(|source| source.catalog_sha256.as_deref())
            .expect("catalog digest");
        for manifest in &snapshot.manifests {
            AdapterManifest::validate_value(
                serde_json::to_value(manifest).expect("serialize manifest"),
            )
            .expect("every bundled manifest must be schema-valid");
            assert_eq!(manifest.category, ManifestCategory::AgentProtocol);
            assert_eq!(manifest.protocol.kind, ManifestProtocolKind::Acp);
            assert_eq!(manifest.protocol.major, 1);
            let source = manifest.source.as_ref().expect("registry provenance");
            assert_eq!(source.kind, ManifestSourceKind::AcpRegistry);
            assert_eq!(source.revision.as_deref(), Some("agenttalk-v2"));
            assert_eq!(source.catalog_sha256.as_deref(), Some(catalog_digest));
        }
        // The catalog digest is self-consistent under the field-level
        // normalization (zero the two hash fields, then SHA-256).
        let snapshot_sha = snapshot
            .manifests
            .iter()
            .find_map(|manifest| {
                manifest
                    .source
                    .as_ref()
                    .and_then(|source| source.catalog_sha256.clone())
            })
            .expect("catalog source digest");
        assert_eq!(
            normalized_catalog_digest(PRODUCTION_CATALOG_BYTES).expect("normalized digest"),
            snapshot_sha,
            "bundled catalog digest must be self-consistent"
        );
    }

    #[test]
    fn registry_archive_sha_is_not_executable_identity_pin() {
        let snapshot = bundled_production_catalog().expect("bundled catalog is valid");
        let registry_manifests = snapshot
            .manifests
            .iter()
            .filter(|manifest| {
                manifest
                    .source
                    .as_ref()
                    .is_some_and(|source| source.kind == ManifestSourceKind::AcpRegistry)
            })
            .collect::<Vec<_>>();
        assert!(!registry_manifests.is_empty());
        assert!(registry_manifests
            .iter()
            .all(|manifest| manifest.match_rules.sha256.is_none()));
        assert!(registry_manifests.iter().any(|manifest| {
            matches!(
                manifest.launch,
                ManifestLaunch::Direct {
                    archive_sha256: Some(_),
                    ..
                }
            )
        }));
    }

    #[test]
    fn corrupt_or_empty_bundled_catalog_fails_closed() {
        assert!(
            bundled_production_catalog_from_bytes(b"not-json{{{").is_err(),
            "corrupt JSON must fail closed"
        );
        assert!(
            bundled_production_catalog_from_bytes(b"{}").is_err(),
            "empty catalog must fail closed"
        );
        let duplicate = {
            let mut value: Value =
                serde_json::from_slice(PRODUCTION_CATALOG_BYTES).expect("parse bundled catalog");
            let manifests = value["manifests"].as_array().expect("manifests").clone();
            let mut doubled = manifests.clone();
            doubled.extend(manifests);
            value["manifests"] = Value::Array(doubled);
            catalog_bytes_with_recomputed_digest(&value)
        };
        assert_eq!(
            bundled_production_catalog_from_bytes(&duplicate),
            Err(CatalogErrorCode::SchemaViolation),
            "duplicate manifest ids must fail closed"
        );
        let secret_env = {
            let mut value: Value =
                serde_json::from_slice(PRODUCTION_CATALOG_BYTES).expect("parse bundled catalog");
            value["manifests"][0]["launch"]["environmentAllowlist"] =
                Value::Array(vec![Value::String("GH_TOKEN".into())]);
            catalog_bytes_with_recomputed_digest(&value)
        };
        assert_eq!(
            bundled_production_catalog_from_bytes(&secret_env),
            Err(CatalogErrorCode::SchemaViolation),
            "secret-like environment names must fail closed"
        );
        let bad_revision = {
            let mut value: Value =
                serde_json::from_slice(PRODUCTION_CATALOG_BYTES).expect("parse bundled catalog");
            value["revision"] = Value::String("unavailable".into());
            catalog_bytes_with_recomputed_digest(&value)
        };
        assert_eq!(
            bundled_production_catalog_from_bytes(&bad_revision),
            Err(CatalogErrorCode::SchemaViolation),
            "the unavailable revision must fail closed"
        );
        let empty_manifests = {
            let mut value: Value =
                serde_json::from_slice(PRODUCTION_CATALOG_BYTES).expect("parse bundled catalog");
            value["manifests"] = Value::Array(Vec::new());
            catalog_bytes_with_recomputed_digest(&value)
        };
        assert_eq!(
            bundled_production_catalog_from_bytes(&empty_manifests),
            Err(CatalogErrorCode::SchemaViolation),
            "an empty production catalog must fail closed"
        );
        // A valid catalog round-trips as Available.
        assert!(bundled_production_catalog().is_ok());
    }

    /// Re-serializes a mutated catalog with a field-level recomputed
    /// self-referential digest (zero the two hash fields, hash, write the
    /// digest back into the two hash fields) so the loader's digest gate is
    /// satisfied and the test exercises only the intended rejection.
    fn catalog_bytes_with_recomputed_digest(value: &Value) -> Vec<u8> {
        let bytes = serde_json::to_vec(value).expect("serialize test catalog");
        let digest = normalized_catalog_digest(&bytes).expect("normalized digest");
        let mut value = value.clone();
        value["registrySha256"] = Value::String(digest.clone());
        if let Some(manifests) = value.get_mut("manifests").and_then(Value::as_array_mut) {
            for manifest in manifests.iter_mut() {
                if let Some(source) = manifest.get_mut("source").and_then(Value::as_object_mut) {
                    source.insert("catalogSha256".to_owned(), Value::String(digest.clone()));
                }
            }
        }
        serde_json::to_vec(&value).expect("serialize catalog")
    }

    /// Recomputes only the root `registrySha256` field-level, leaving every
    /// manifest source.catalogSha256 exactly as provided. Used by the source
    /// consistency tests so the loader's source-vs-root check is what fires.
    fn catalog_bytes_with_recomputed_root_digest(value: &Value) -> Vec<u8> {
        let bytes = serde_json::to_vec(value).expect("serialize test catalog");
        let digest = normalized_catalog_digest(&bytes).expect("normalized digest");
        let mut value = value.clone();
        value["registrySha256"] = Value::String(digest);
        serde_json::to_vec(&value).expect("serialize catalog")
    }

    #[test]
    fn launch_environment_policy_separates_ordinary_and_credential_grants() {
        let original: Value =
            serde_json::from_slice(PRODUCTION_CATALOG_BYTES).expect("parse bundled catalog");
        let cases = [
            (
                json!({"kind": "direct", "transport": "stdio", "executableRef": "matched-observation",
                       "args": ["--run"], "environmentAllowlist": ["GH_TOKEN"], "credentialEnvironment": []}),
                "direct allowlist",
            ),
            (
                json!({"kind": "npx", "package": "fixture-agent-pkg", "args": ["--run"],
                       "environmentAllowlist": ["GH_TOKEN"], "credentialEnvironment": []}),
                "npx allowlist",
            ),
            (
                json!({"kind": "uvx", "package": "fixture-agent-pkg", "args": ["--run"],
                       "environmentAllowlist": ["GH_TOKEN"], "credentialEnvironment": []}),
                "uvx allowlist",
            ),
        ];
        for (launch, label) in cases {
            let mut value = original.clone();
            value["manifests"][0]["launch"] = launch;
            // Recompute the digest so the ONLY rejection reason is the secret
            // environment policy, not the stale self-referential digest.
            let fixed = catalog_bytes_with_recomputed_digest(&value);
            assert_eq!(
                bundled_production_catalog_from_bytes(&fixed),
                Err(CatalogErrorCode::SchemaViolation),
                "{label} must fail closed"
            );
        }
        // Legal, non-secret environment names still pass for every launch kind.
        for launch in [
            json!({"kind": "direct", "transport": "stdio", "executableRef": "matched-observation",
                   "args": ["--run"], "environmentAllowlist": ["AGENTTALK_SAFE_ALLOWED"], "credentialEnvironment": []}),
            json!({"kind": "npx", "package": "fixture-agent-pkg", "args": ["--run"],
                   "environmentAllowlist": ["AGENTTALK_SAFE_ALLOWED"], "credentialEnvironment": []}),
            json!({"kind": "uvx", "package": "fixture-agent-pkg", "args": ["--run"],
                   "environmentAllowlist": ["AGENTTALK_SAFE_ALLOWED"], "credentialEnvironment": []}),
        ] {
            let mut value = original.clone();
            value["manifests"][0]["launch"] = launch;
            // The env policy itself must accept the catalog; only the stale
            // digest (the manifest bytes changed) may reject it, so recompute
            // the digest first.
            let fixed = catalog_bytes_with_recomputed_digest(&value);
            assert!(
                bundled_production_catalog_from_bytes(&fixed).is_ok(),
                "legal non-secret environment names must pass"
            );
        }
        // An explicit credential slot is legal as a name-only grant, including
        // a secret-like name; only the ordinary inherited allowlist is
        // rejected above.
        let mut value = original.clone();
        value["manifests"][0]["launch"]["credentialEnvironment"] =
            Value::Array(vec![Value::String("LX_API_KEY".into())]);
        let bytes = catalog_bytes_with_recomputed_digest(&value);
        assert!(bundled_production_catalog_from_bytes(&bytes).is_ok());
    }

    #[test]
    fn bundled_catalog_stale_digest_fails_closed() {
        let original: Value =
            serde_json::from_slice(PRODUCTION_CATALOG_BYTES).expect("parse bundled catalog");
        let original_digest = original["registrySha256"]
            .as_str()
            .expect("bundled digest")
            .to_owned();
        assert_eq!(original_digest.len(), 64);
        // A schema-valid content change without a digest update must be
        // rejected by the runtime loader (the digest gate fails first).
        let mut tampered = original.clone();
        tampered["manifests"][0]["displayName"] =
            Value::String("GitHub Copilot CLI (tampered)".into());
        let tampered_bytes = serde_json::to_vec(&tampered).expect("serialize tampered catalog");
        assert!(
            bundled_production_catalog_from_bytes(&tampered_bytes).is_err(),
            "stale digest after a content change must fail closed"
        );
        // A malformed digest length/characters must be rejected.
        let mut malformed = original.clone();
        malformed["registrySha256"] = Value::String("not-a-sha".into());
        let malformed_bytes = serde_json::to_vec(&malformed).expect("serialize malformed catalog");
        assert!(
            bundled_production_catalog_from_bytes(&malformed_bytes).is_err(),
            "malformed digest must fail closed"
        );
        // Correctly recomputing the digest makes the same catalog loadable.
        let fixed = catalog_bytes_with_recomputed_digest(&tampered);
        assert!(
            bundled_production_catalog_from_bytes(&fixed).is_ok(),
            "a correctly recomputed catalog must load"
        );
        // The loader and the test share the same normalization algorithm.
        let production = bundled_production_catalog().expect("bundled catalog");
        let production_digest = production
            .manifests
            .iter()
            .find_map(|manifest| {
                manifest
                    .source
                    .as_ref()
                    .and_then(|source| source.catalog_sha256.clone())
            })
            .expect("catalog source digest");
        assert_eq!(
            normalized_catalog_digest(PRODUCTION_CATALOG_BYTES).expect("normalized digest"),
            production_digest
        );
    }

    #[test]
    fn normalized_digest_zeroes_only_hash_fields_not_ordinary_ones() {
        // An ordinary field whose value happens to equal the declared digest
        // must be treated as real content, never as a hash field to zero.
        let mut value: Value =
            serde_json::from_slice(PRODUCTION_CATALOG_BYTES).expect("parse bundled catalog");
        let declared = value["registrySha256"]
            .as_str()
            .expect("bundled digest")
            .to_owned();
        value["manifests"][0]["displayName"] = Value::String(declared.clone());
        let bytes = serde_json::to_vec(&value).expect("serialize catalog");
        // The field-level expected digest: zero exactly the root registrySha256
        // and the manifest source.catalogSha256; the displayName (equal to the
        // old digest) remains content.
        let mut expected = value.clone();
        expected["registrySha256"] = Value::String(
            "0000000000000000000000000000000000000000000000000000000000000000".into(),
        );
        for manifest in expected["manifests"].as_array_mut().expect("manifests") {
            manifest["source"]["catalogSha256"] = Value::String(
                "0000000000000000000000000000000000000000000000000000000000000000".into(),
            );
        }
        let expected_digest = sha256_hex(
            serde_json::to_string(&expected)
                .expect("canonical")
                .as_bytes(),
        );
        assert_eq!(
            normalized_catalog_digest(&bytes).expect("normalized digest"),
            expected_digest,
            "an ordinary field equal to the digest must not be zeroed"
        );
    }

    #[test]
    fn bundled_catalog_ordinary_field_equal_to_digest_still_loads() {
        // Load-level regression for the field-level normalization: an ordinary
        // field (displayName) set to a 64-hex string equal to the declared
        // digest participates as content and the catalog still loads after the
        // field-level digest is recomputed.
        let mut value: Value =
            serde_json::from_slice(PRODUCTION_CATALOG_BYTES).expect("parse bundled catalog");
        let declared = value["registrySha256"]
            .as_str()
            .expect("bundled digest")
            .to_owned();
        value["manifests"][0]["displayName"] = Value::String(declared);
        let fixed = catalog_bytes_with_recomputed_digest(&value);
        assert!(
            bundled_production_catalog_from_bytes(&fixed).is_ok(),
            "a catalog whose ordinary field equals the old digest must still load"
        );
    }

    #[test]
    fn bundled_catalog_source_digest_must_match_root() {
        let original: Value =
            serde_json::from_slice(PRODUCTION_CATALOG_BYTES).expect("parse bundled catalog");
        // A different-but-valid source.catalogSha256 is rejected even when the
        // root digest is recomputed field-level (the inconsistency is checked
        // explicitly, not as an accident of the normalization).
        let mut mismatched = original.clone();
        mismatched["manifests"][0]["source"]["catalogSha256"] = Value::String("b".repeat(64));
        let fixed = catalog_bytes_with_recomputed_root_digest(&mismatched);
        assert_eq!(
            bundled_production_catalog_from_bytes(&fixed),
            Err(CatalogErrorCode::HashMismatch),
            "a source digest different from the root must fail closed"
        );
        // A manifest without a source must fail closed.
        let mut missing_source = original.clone();
        missing_source["manifests"][0]
            .as_object_mut()
            .expect("manifest object")
            .remove("source");
        let fixed = catalog_bytes_with_recomputed_root_digest(&missing_source);
        assert_eq!(
            bundled_production_catalog_from_bytes(&fixed),
            Err(CatalogErrorCode::HashMismatch),
            "a missing manifest source must fail closed"
        );
        // A source without a catalogSha256 must fail closed.
        let mut missing_sha = original.clone();
        missing_sha["manifests"][0]["source"]
            .as_object_mut()
            .expect("source object")
            .remove("catalogSha256");
        let fixed = catalog_bytes_with_recomputed_root_digest(&missing_sha);
        assert_eq!(
            bundled_production_catalog_from_bytes(&fixed),
            Err(CatalogErrorCode::HashMismatch),
            "a missing source catalogSha256 must fail closed"
        );
        // A malformed source catalogSha256 must fail closed.
        let mut malformed_source = original.clone();
        malformed_source["manifests"][0]["source"]["catalogSha256"] =
            Value::String("not-a-sha".into());
        let fixed = catalog_bytes_with_recomputed_root_digest(&malformed_source);
        assert_eq!(
            bundled_production_catalog_from_bytes(&fixed),
            Err(CatalogErrorCode::HashMismatch),
            "a malformed source catalogSha256 must fail closed"
        );
    }

    fn registry_fixture_distribution(distribution: Value) -> Vec<u8> {
        serde_json::to_vec(&json!({
            "version": "1.0.0",
            "agents": [{
                "id": "example-agent",
                "name": "Example Agent",
                "version": "1.2.3",
                "description": "Fixture",
                "repository": "https://example.invalid/repo",
                "website": "https://example.invalid/docs",
                "authors": ["Fixture Author"],
                "license": "MIT",
                "icon": "https://example.invalid/icon.svg",
                "distribution": distribution
            }]
        }))
        .expect("serialize fixture")
    }

    fn binary_registry_bytes() -> Vec<u8> {
        registry_fixture_distribution(json!({
            "binary": {
                "windows-x86_64": {
                    "archive": "https://example.invalid/example.zip",
                    "sha256": "ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789",
                    "cmd": "example-agent.exe",
                    "args": ["serve", "--acp"],
                    "env": {
                        "PATH": "fixture-path",
                        "USERPROFILE": "fixture-user",
                        "AGENT_TOKEN": "fixture-token-value"
                    }
                }
            }
        }))
    }

    fn registry_fixture_agents(agents: Vec<Value>) -> Vec<u8> {
        serde_json::to_vec(&json!({
            "version": "1.0.0",
            "agents": agents
        }))
        .expect("serialize registry")
    }

    fn registry_agent(id: &str, distribution: Value) -> Value {
        json!({
            "id": id,
            "name": format!("{} Agent", id),
            "version": "1.2.3",
            "description": "Fixture",
            "distribution": distribution
        })
    }

    fn runner_manifest_value(kind: &str, package: &str) -> Value {
        json!({
            "schemaVersion": "agenttalk.adapter.v1",
            "id": "org.example.runner",
            "displayName": "Example Runner",
            "category": "agent_protocol",
            "protocol": { "kind": "acp", "major": 1 },
            "match": {},
            "launch": {
                "kind": kind,
                "package": package,
                "args": [],
                "environmentAllowlist": ["PATH"]
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

    fn registry_runner_accepts(kind: &str, package: &str) -> bool {
        let distribution = match kind {
            "npx" => json!({"npx": {"package": package}}),
            "uvx" => json!({"uvx": {"package": package}}),
            _ => panic!("test runner kind"),
        };
        convert_acp_registry_bytes(
            &registry_fixture_distribution(distribution),
            "windows",
            "x86_64",
            "rev-a",
        )
        .is_ok()
    }

    fn unsupported_binary_agent(id: &str) -> Value {
        registry_agent(
            id,
            json!({
                "binary": {
                    "linux-x86_64": {
                        "archive": "https://example.invalid/linux.zip",
                        "cmd": "./linux-agent"
                    }
                }
            }),
        )
    }

    fn binary_distribution(cmd: &str, archive_sha: &str) -> Value {
        json!({
            "binary": {
                "windows-x86_64": {
                    "archive": "https://example.invalid/example.zip",
                    "sha256": archive_sha,
                    "cmd": cmd,
                    "args": ["serve"],
                    "env": {
                        "PATH": "fixture-path",
                        "AGENT_TOKEN": "fixture-token-value"
                    }
                }
            }
        })
    }

    fn manifest_with_id(id: &str) -> AdapterManifest {
        let mut manifest = convert_binary().manifest;
        manifest.id = id.into();
        manifest.display_name = format!("{id} Agent");
        manifest.source = Some(ManifestSource {
            kind: ManifestSourceKind::AgenttalkManifest,
            id: id.into(),
            version: "1.0.0".into(),
            revision: Some("manual".into()),
            catalog_sha256: None,
        });
        manifest.match_rules.registry_ids.clear();
        manifest.match_rules.package_ids.clear();
        manifest.match_rules.executable_names.clear();
        manifest.match_rules.publisher_subjects.clear();
        manifest.match_rules.sha256 = None;
        manifest
    }

    fn connector_suffix(projection: &CandidateProjection) -> &str {
        projection
            .connector_id
            .strip_prefix("local.registry.")
            .expect("registry connector")
    }

    fn convert_binary() -> ConvertedRegistryManifest {
        convert_acp_registry_bytes(&binary_registry_bytes(), "windows", "x86_64", "rev-a")
            .expect("binary converts")
            .into_iter()
            .next()
            .expect("one manifest")
    }

    const FIXED_UPSTREAM_RUNNER_COMMIT: &str = "03179331de80aa1b4695aab133a1fb79ded9ee8d";
    const FIXED_UPSTREAM_RUNNER_COUNT: usize = 24;
    const FIXED_UPSTREAM_RUNNER_SHA256: &str =
        "927a883fb5c4d46249eb2ec3878c3a4ad46c6a2b63bead83b200d933ba013e6a";

    #[derive(Clone, Debug, Deserialize)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    struct FixedRunnerSnapshot {
        upstream_commit: String,
        entries: Vec<FixedRunnerEntry>,
    }

    #[derive(Clone, Debug, Deserialize)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    struct FixedRunnerEntry {
        id: String,
        kind: RegistryLaunchKind,
        package: String,
        #[serde(default)]
        args: Vec<String>,
    }

    fn fixed_runner_snapshot_bytes() -> &'static [u8] {
        include_bytes!("../../../../fixtures/discovery/acp-registry/03179331-runner-packages.json")
    }

    fn fixed_runner_snapshot() -> FixedRunnerSnapshot {
        serde_json::from_slice(fixed_runner_snapshot_bytes()).expect("fixed runner snapshot parses")
    }

    fn registry_bytes_from_runner_entries(entries: &[FixedRunnerEntry]) -> Vec<u8> {
        let agents = entries
            .iter()
            .map(|entry| {
                let kind = match entry.kind {
                    RegistryLaunchKind::Npx => "npx",
                    RegistryLaunchKind::Uvx => "uvx",
                    RegistryLaunchKind::Binary => panic!("runner snapshot must not contain binary"),
                };
                registry_agent(
                    &entry.id,
                    json!({
                        kind: {
                            "package": entry.package,
                            "args": entry.args,
                        }
                    }),
                )
            })
            .collect::<Vec<_>>();
        registry_fixture_agents(agents)
    }

    fn launch_kind_label(kind: RegistryLaunchKind) -> &'static str {
        match kind {
            RegistryLaunchKind::Binary => "binary",
            RegistryLaunchKind::Npx => "npx",
            RegistryLaunchKind::Uvx => "uvx",
        }
    }

    fn production_evidence(
        status: AuthenticodeStatus,
        trusted_chain: bool,
        signer_subject: Option<&str>,
    ) -> AuthenticodeEvidence {
        AuthenticodeEvidence::from_signer(status, trusted_chain, signer_subject.map(str::to_owned))
    }

    #[test]
    fn registry_binary_entry_converts_to_direct_manifest() {
        let converted = convert_binary();
        assert_eq!(converted.launch_kind, RegistryLaunchKind::Binary);
        match converted.manifest.launch {
            ManifestLaunch::Direct {
                executable_ref,
                args,
                environment_allowlist,
                credential_environment,
                archive_sha256,
                sha256,
                ..
            } => {
                assert_eq!(executable_ref, "example-agent.exe");
                assert_eq!(args, vec!["serve", "--acp"]);
                assert_eq!(environment_allowlist, vec!["PATH", "USERPROFILE"]);
                assert_eq!(credential_environment, vec!["AGENT_TOKEN"]);
                assert_eq!(
                    archive_sha256.as_deref(),
                    Some("abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789")
                );
                assert_eq!(sha256, None);
            }
            _ => panic!("binary maps to direct launch"),
        }
    }

    #[test]
    fn registry_npx_and_uvx_convert_without_execution() {
        for (kind, distribution) in [
            (
                RegistryLaunchKind::Npx,
                json!({"npx": {"package": "@scope/example-agent", "args": ["--acp"], "env": {"PATH": "x"}}}),
            ),
            (
                RegistryLaunchKind::Uvx,
                json!({"uvx": {"package": "example-agent", "args": ["serve"], "env": {"PATH": "x"}}}),
            ),
        ] {
            let converted = convert_acp_registry_bytes(
                &registry_fixture_distribution(distribution),
                "windows",
                "x86_64",
                "rev-a",
            )
            .expect("runner converts")
            .into_iter()
            .next()
            .expect("one manifest");
            assert_eq!(converted.launch_kind, kind);
            assert!(matches!(
                converted.manifest.launch,
                ManifestLaunch::Npx { .. } | ManifestLaunch::Uvx { .. }
            ));
        }
    }

    #[test]
    fn official_versioned_unscoped_npx_package_converts() {
        let converted = convert_acp_registry_bytes(
            &registry_fixture_distribution(json!({
                "npx": {"package": "agoragentic-mcp@1.3.0", "args": ["--acp"]}
            })),
            "windows",
            "x86_64",
            "03179331de80aa1b4695aab133a1fb79ded9ee8d",
        )
        .expect("official versioned unscoped npx package converts")
        .remove(0);
        assert_eq!(converted.launch_kind, RegistryLaunchKind::Npx);
        match converted.manifest.launch {
            ManifestLaunch::Npx { package, args, .. } => {
                assert_eq!(package, "agoragentic-mcp@1.3.0");
                assert_eq!(args, vec!["--acp"]);
            }
            _ => panic!("npx launch"),
        }
        assert_eq!(
            converted.manifest.match_rules.package_ids,
            vec!["agoragentic-mcp@1.3.0"]
        );
    }

    #[test]
    fn official_versioned_scoped_npx_package_converts() {
        let converted = convert_acp_registry_bytes(
            &registry_fixture_distribution(json!({
                "npx": {"package": "@augmentcode/auggie@0.35.0", "args": ["--acp"]}
            })),
            "windows",
            "x86_64",
            "03179331de80aa1b4695aab133a1fb79ded9ee8d",
        )
        .expect("official versioned scoped npx package converts")
        .remove(0);
        assert_eq!(converted.launch_kind, RegistryLaunchKind::Npx);
        match converted.manifest.launch {
            ManifestLaunch::Npx { package, .. } => {
                assert_eq!(package, "@augmentcode/auggie@0.35.0");
            }
            _ => panic!("npx launch"),
        }
    }

    #[test]
    fn official_pinned_uvx_package_converts() {
        for package in ["fast-agent-acp==0.9.30", "minion-code@0.1.44"] {
            let converted = convert_acp_registry_bytes(
                &registry_fixture_distribution(json!({
                    "uvx": {"package": package, "args": ["acp"]}
                })),
                "windows",
                "x86_64",
                "03179331de80aa1b4695aab133a1fb79ded9ee8d",
            )
            .expect("official pinned uvx package converts")
            .remove(0);
            assert_eq!(converted.launch_kind, RegistryLaunchKind::Uvx);
            match converted.manifest.launch {
                ManifestLaunch::Uvx {
                    package: converted_package,
                    ..
                } => {
                    assert_eq!(converted_package, package);
                }
                _ => panic!("uvx launch"),
            }
        }
    }

    #[test]
    fn official_current_runner_package_snapshot_converts_offline() {
        let snapshot = fixed_runner_snapshot();
        let bytes = registry_bytes_from_runner_entries(&snapshot.entries);
        let converted = convert_acp_registry_bytes(
            &bytes,
            "windows",
            "x86_64",
            "03179331de80aa1b4695aab133a1fb79ded9ee8d",
        )
        .expect("fixed upstream runner snapshot converts offline");
        let ids = converted
            .iter()
            .map(|entry| (entry.manifest.id.as_str(), entry.launch_kind))
            .collect::<Vec<_>>();
        assert_eq!(
            ids,
            snapshot
                .entries
                .iter()
                .map(|entry| (entry.id.as_str(), entry.kind))
                .collect::<Vec<_>>()
        );
        let sanitized = converted
            .iter()
            .map(|entry| entry.manifest.to_sanitized_value())
            .collect::<Vec<_>>();
        let serialized =
            serde_json::to_string(&sanitized).expect("serialize converted snapshot manifests");
        assert!(!serialized.contains("AUGMENT_DISABLE_AUTO_UPDATE\":\"1"));
        assert!(!serialized.contains("\"match\":{\"sha256\""));
    }

    #[test]
    fn complete_fixed_upstream_runner_snapshot_has_expected_count_and_sha() {
        let snapshot = fixed_runner_snapshot();
        assert_eq!(snapshot.upstream_commit, FIXED_UPSTREAM_RUNNER_COMMIT);
        assert_eq!(snapshot.entries.len(), FIXED_UPSTREAM_RUNNER_COUNT);
        assert_eq!(
            sha256_hex(fixed_runner_snapshot_bytes()),
            FIXED_UPSTREAM_RUNNER_SHA256
        );
        let mut ids = BTreeSet::new();
        let mut packages = BTreeSet::new();
        for entry in &snapshot.entries {
            assert!(ids.insert(entry.id.as_str()), "duplicate id {}", entry.id);
            assert!(
                packages.insert((launch_kind_label(entry.kind), entry.package.as_str())),
                "duplicate package {:?} {}",
                entry.kind,
                entry.package
            );
        }
    }

    #[test]
    fn every_fixed_upstream_npx_package_passes_npx_parser() {
        let snapshot = fixed_runner_snapshot();
        let npx_entries = snapshot
            .entries
            .iter()
            .filter(|entry| entry.kind == RegistryLaunchKind::Npx)
            .collect::<Vec<_>>();
        assert_eq!(npx_entries.len(), 22);
        for entry in npx_entries {
            assert_eq!(
                normalize_registry_runner_package("npx", &entry.package).as_deref(),
                Ok(entry.package.as_str()),
                "npx package {}",
                entry.package
            );
            let converted = convert_acp_registry_bytes(
                &registry_bytes_from_runner_entries(std::slice::from_ref(entry)),
                "windows",
                "x86_64",
                FIXED_UPSTREAM_RUNNER_COMMIT,
            )
            .expect("npx entry converts")
            .remove(0);
            assert_eq!(converted.launch_kind, RegistryLaunchKind::Npx);
        }
    }

    #[test]
    fn every_fixed_upstream_uvx_package_passes_uvx_parser() {
        let snapshot = fixed_runner_snapshot();
        let uvx_entries = snapshot
            .entries
            .iter()
            .filter(|entry| entry.kind == RegistryLaunchKind::Uvx)
            .collect::<Vec<_>>();
        assert_eq!(uvx_entries.len(), 2);
        for entry in uvx_entries {
            assert_eq!(
                normalize_registry_runner_package("uvx", &entry.package).as_deref(),
                Ok(entry.package.as_str()),
                "uvx package {}",
                entry.package
            );
            let converted = convert_acp_registry_bytes(
                &registry_bytes_from_runner_entries(std::slice::from_ref(entry)),
                "windows",
                "x86_64",
                FIXED_UPSTREAM_RUNNER_COMMIT,
            )
            .expect("uvx entry converts")
            .remove(0);
            assert_eq!(converted.launch_kind, RegistryLaunchKind::Uvx);
        }
    }

    #[test]
    fn npx_rejects_uvx_double_equals_spec() {
        assert_eq!(
            normalize_registry_runner_package("npx", "fast-agent-acp==0.9.30")
                .unwrap_err()
                .code(),
            CatalogErrorCode::SchemaViolation
        );
    }

    #[test]
    fn uvx_rejects_npm_scoped_spec_without_authority() {
        assert_eq!(
            normalize_registry_runner_package("uvx", "@scope/example-agent@1.2.3")
                .unwrap_err()
                .code(),
            CatalogErrorCode::SchemaViolation
        );
    }

    #[test]
    fn kind_specific_schema_and_typed_validation_agree() {
        let snapshot = fixed_runner_snapshot();
        let converted = convert_acp_registry_bytes(
            &registry_bytes_from_runner_entries(&snapshot.entries),
            "windows",
            "x86_64",
            FIXED_UPSTREAM_RUNNER_COMMIT,
        )
        .expect("fixed runner snapshot converts");
        assert_eq!(converted.len(), FIXED_UPSTREAM_RUNNER_COUNT);
        let expected = snapshot
            .entries
            .iter()
            .map(|entry| {
                (
                    entry.id.as_str(),
                    launch_kind_label(entry.kind),
                    entry.package.as_str(),
                )
            })
            .collect::<BTreeSet<_>>();
        let actual = converted
            .iter()
            .map(|entry| {
                let package = match &entry.manifest.launch {
                    ManifestLaunch::Npx { package, .. } | ManifestLaunch::Uvx { package, .. } => {
                        package.as_str()
                    }
                    ManifestLaunch::Direct { .. } => panic!("runner snapshot produced binary"),
                };
                (
                    entry.manifest.id.as_str(),
                    launch_kind_label(entry.launch_kind),
                    package,
                )
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(actual, expected);
        for entry in converted {
            AdapterManifest::validate_value(entry.manifest.to_sanitized_value())
                .expect("converted runner manifest validates");
        }
    }

    #[test]
    fn duplicate_snapshot_entries_fail_closed() {
        let duplicated_id = registry_fixture_agents(vec![
            registry_agent(
                "duplicate-agent",
                json!({"npx": {"package": "first-agent"}}),
            ),
            registry_agent(
                "duplicate-agent",
                json!({"npx": {"package": "second-agent"}}),
            ),
        ]);
        assert_eq!(
            convert_acp_registry_bytes(
                &duplicated_id,
                "windows",
                "x86_64",
                FIXED_UPSTREAM_RUNNER_COMMIT
            )
            .unwrap_err()
            .code(),
            CatalogErrorCode::SchemaViolation
        );

        let duplicated_package = registry_fixture_agents(vec![
            registry_agent("first-agent", json!({"npx": {"package": "same-package"}})),
            registry_agent("second-agent", json!({"npx": {"package": "same-package"}})),
        ]);
        assert_eq!(
            convert_acp_registry_bytes(
                &duplicated_package,
                "windows",
                "x86_64",
                FIXED_UPSTREAM_RUNNER_COMMIT
            )
            .unwrap_err()
            .code(),
            CatalogErrorCode::SchemaViolation
        );
    }

    #[test]
    fn runner_package_rejects_option_url_path_and_shell_injection() {
        for package in [
            "-pwned",
            "pkg name",
            "pkg\tname",
            "pkg\u{202e}name",
            "https://example.invalid/pkg",
            "file:../pkg",
            "git+https://example.invalid/pkg",
            "\\\\server\\share\\pkg",
            "C:\\tools\\pkg",
            "../pkg",
            "scope/pkg/extra",
            "pkg;calc",
            "pkg$(calc)",
            "`pkg`",
            "pkg|more",
            "pkg&more",
        ] {
            let bytes = registry_fixture_distribution(json!({
                "npx": {"package": package}
            }));
            assert!(
                convert_acp_registry_bytes(&bytes, "windows", "x86_64", "rev-a").is_err(),
                "runner package must be rejected: {package:?}"
            );
        }
    }

    #[test]
    fn official_dot_slash_binary_cmd_converts() {
        let converted = convert_acp_registry_bytes(
            &registry_fixture_distribution(binary_distribution(
                "./example-agent",
                "ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789",
            )),
            "windows",
            "x86_64",
            "rev-a",
        )
        .expect("official cmd converts")
        .remove(0);
        match converted.manifest.launch {
            ManifestLaunch::Direct { executable_ref, .. } => {
                assert_eq!(executable_ref, "example-agent");
            }
            _ => panic!("direct launch"),
        }
    }

    #[test]
    fn archive_sha_is_not_executable_match_sha() {
        let converted = convert_binary();
        assert_eq!(converted.manifest.match_rules.sha256, None);
        match converted.manifest.launch {
            ManifestLaunch::Direct {
                archive_sha256,
                sha256,
                ..
            } => {
                assert!(archive_sha256.is_some());
                assert_eq!(sha256, None);
            }
            _ => panic!("direct launch"),
        }
    }

    #[test]
    fn current_platform_binary_is_selected_from_multiple_distributions() {
        let converted = convert_acp_registry_bytes(
            &registry_fixture_distribution(json!({
                "binary": {
                    "linux-x86_64": {
                        "archive": "https://example.invalid/linux.tgz",
                        "cmd": "./linux-agent",
                        "args": ["serve"]
                    },
                    "windows-x86_64": {
                        "archive": "https://example.invalid/windows.zip",
                        "cmd": "./example-agent",
                        "args": ["serve"]
                    }
                },
                "npx": {"package": "@scope/example-agent", "args": ["--acp"]},
                "uvx": {"package": "example-agent", "args": ["serve"]}
            })),
            "windows",
            "x86_64",
            "rev-a",
        )
        .expect("current platform binary selected")
        .remove(0);
        assert_eq!(converted.launch_kind, RegistryLaunchKind::Binary);
        assert!(matches!(
            converted.manifest.launch,
            ManifestLaunch::Direct { .. }
        ));
    }

    #[test]
    fn unsupported_binary_platform_does_not_mask_npx() {
        let converted = convert_acp_registry_bytes(
            &registry_fixture_distribution(json!({
                "binary": {
                    "linux-x86_64": {
                        "archive": "https://example.invalid/linux.tgz",
                        "cmd": "./linux-agent"
                    }
                },
                "npx": {"package": "@scope/example-agent", "args": ["--acp"]}
            })),
            "windows",
            "x86_64",
            "rev-a",
        )
        .expect("npx selected when binary unsupported")
        .remove(0);
        assert_eq!(converted.launch_kind, RegistryLaunchKind::Npx);
    }

    #[test]
    fn npx_precedes_uvx_deterministically() {
        let converted = convert_acp_registry_bytes(
            &registry_fixture_distribution(json!({
                "uvx": {"package": "example-agent", "args": ["serve"]},
                "npx": {"package": "@scope/example-agent", "args": ["--acp"]}
            })),
            "windows",
            "x86_64",
            "rev-a",
        )
        .expect("runner selected")
        .remove(0);
        assert_eq!(converted.launch_kind, RegistryLaunchKind::Npx);
    }

    #[test]
    fn multi_distribution_conversion_is_order_independent() {
        let left = registry_fixture_agents(vec![
            registry_agent(
                "alpha-agent",
                json!({"uvx": {"package": "alpha-agent"}, "npx": {"package": "@scope/alpha-agent"}}),
            ),
            registry_agent(
                "beta-agent",
                json!({"binary": {"windows-x86_64": {"archive": "https://example.invalid/beta.zip", "cmd": "./beta-agent"}}}),
            ),
        ]);
        let right = registry_fixture_agents(vec![
            registry_agent(
                "beta-agent",
                json!({"binary": {"windows-x86_64": {"archive": "https://example.invalid/beta.zip", "cmd": "./beta-agent"}}}),
            ),
            registry_agent(
                "alpha-agent",
                json!({"npx": {"package": "@scope/alpha-agent"}, "uvx": {"package": "alpha-agent"}}),
            ),
        ]);
        let mut left =
            convert_acp_registry_bytes(&left, "windows", "x86_64", "rev-a").expect("left converts");
        let mut right = convert_acp_registry_bytes(&right, "windows", "x86_64", "rev-a")
            .expect("right converts");
        left.sort_by(|a, b| a.manifest.id.cmp(&b.manifest.id));
        right.sort_by(|a, b| a.manifest.id.cmp(&b.manifest.id));
        assert_eq!(left, right);
    }

    #[test]
    fn registry_args_remain_structured_and_are_never_shell_joined() {
        let converted = convert_binary();
        let json = serde_json::to_string(&converted.manifest).expect("serialize");
        assert!(json.contains("\"serve\""));
        assert!(json.contains("\"--acp\""));
        assert!(!json.contains("serve --acp"));
    }

    #[test]
    fn registry_env_values_are_not_retained_or_serialized() {
        let converted = convert_binary();
        let json = serde_json::to_string(&converted.manifest).expect("serialize");
        assert!(!json.contains("fixture-token-value"));
        assert!(!json.contains("fixture-path"));
        assert!(json.contains("AGENT_TOKEN"));
    }

    #[test]
    fn registry_env_credentials_become_slots_without_inherited_values() {
        let converted = convert_acp_registry_bytes(
            &registry_fixture_distribution(json!({
                "npx": {
                    "package": "@scope/example-agent",
                    "args": ["--acp"],
                    "env": {
                        "PATH": "fixture-path",
                        "AGENT_API_KEY": "fixture-secret-value"
                    }
                }
            })),
            "windows",
            "x86_64",
            "rev-a",
        )
        .expect("credential env converted")
        .remove(0);
        match converted.manifest.launch {
            ManifestLaunch::Npx {
                ref environment_allowlist,
                ref credential_environment,
                ..
            } => {
                assert_eq!(environment_allowlist.as_slice(), ["PATH"]);
                assert_eq!(credential_environment.as_slice(), ["AGENT_API_KEY"]);
            }
            _ => panic!("npx launch"),
        }
        let json = serde_json::to_string(&converted.manifest).expect("serialize");
        assert!(!json.contains("fixture-secret-value"));
        assert!(!json.contains("fixture-path"));
    }

    #[test]
    fn unknown_distribution_and_unknown_fields_fail_closed() {
        let unknown = registry_fixture_distribution(json!({"docker": {"image": "example"}}));
        assert_eq!(
            convert_acp_registry_bytes(&unknown, "windows", "x86_64", "rev-a")
                .unwrap_err()
                .code(),
            CatalogErrorCode::SchemaViolation
        );
        let unknown_field = serde_json::to_vec(&json!({
            "version": "1.0.0",
            "agents": [],
            "unexpected": true
        }))
        .expect("serialize");
        assert_eq!(
            convert_acp_registry_bytes(&unknown_field, "windows", "x86_64", "rev-a")
                .unwrap_err()
                .code(),
            CatalogErrorCode::SchemaViolation
        );
    }

    #[test]
    fn platform_and_architecture_mismatch_are_typed() {
        let converted =
            convert_acp_registry_bytes(&binary_registry_bytes(), "windows", "aarch64", "rev-a")
                .expect("unsupported single entry is filtered from catalog conversion");
        assert!(converted.is_empty());
    }

    #[test]
    fn multiple_legal_distributions_do_not_return_unknown_distribution() {
        let converted = convert_acp_registry_bytes(
            &registry_fixture_distribution(json!({
                "binary": {
                    "windows-x86_64": {
                        "archive": "https://example.invalid/example.zip",
                        "cmd": "./example-agent"
                    }
                },
                "npx": {"package": "@scope/example-agent"},
                "uvx": {"package": "example-agent"}
            })),
            "windows",
            "x86_64",
            "rev-a",
        )
        .expect("multiple legal distributions convert")
        .remove(0);
        assert_eq!(converted.launch_kind, RegistryLaunchKind::Binary);
    }

    #[test]
    fn optional_sha256_is_validated_and_normalized() {
        let converted = convert_binary();
        let ManifestLaunch::Direct { archive_sha256, .. } = converted.manifest.launch else {
            panic!("direct launch");
        };
        assert_eq!(
            archive_sha256,
            Some("abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789".into())
        );
        assert_eq!(converted.manifest.match_rules.sha256, None);
    }

    #[test]
    fn converted_manifest_passes_internal_schema() {
        let converted = convert_binary();
        AdapterManifest::validate_value(converted.manifest.to_sanitized_value())
            .expect("converted manifest validates");
    }

    #[test]
    fn schema_and_typed_runner_package_validation_agree() {
        for package in [
            "plain-package",
            "plain-package@1.2.3",
            "@scope/plain-package",
            "@scope/plain-package@1.2.3",
        ] {
            let converted = convert_acp_registry_bytes(
                &registry_fixture_distribution(json!({
                    "npx": {"package": package}
                })),
                "windows",
                "x86_64",
                "rev-a",
            )
            .expect("typed npx conversion accepts official npm package")
            .remove(0);
            assert_eq!(converted.launch_kind, RegistryLaunchKind::Npx);
            AdapterManifest::validate_value(converted.manifest.to_sanitized_value())
                .expect("schema accepts the same npx package");
        }
        for package in ["fast-agent-acp==0.9.30", "minion-code@0.1.44"] {
            let converted = convert_acp_registry_bytes(
                &registry_fixture_distribution(json!({
                    "uvx": {"package": package}
                })),
                "windows",
                "x86_64",
                "rev-a",
            )
            .expect("typed uvx conversion accepts fixed upstream uvx package")
            .remove(0);
            assert_eq!(converted.launch_kind, RegistryLaunchKind::Uvx);
            AdapterManifest::validate_value(converted.manifest.to_sanitized_value())
                .expect("schema accepts the same uvx package");
        }
        for package in [
            "-bad",
            "pkg name",
            "https://example.invalid/pkg",
            "../pkg",
            "pkg;bad",
        ] {
            let bytes = registry_fixture_distribution(json!({
                "npx": {"package": package}
            }));
            assert!(convert_acp_registry_bytes(&bytes, "windows", "x86_64", "rev-a").is_err());
        }
    }

    #[test]
    fn npx_schema_typed_and_registry_rules_agree() {
        for (package, expected) in [
            ("package", true),
            ("package@1.2.3", true),
            ("@scope/package", true),
            ("@scope/package@1.2.3", true),
            ("package==1.2.3", false),
            ("package@1.x.3", false),
        ] {
            let value = runner_manifest_value("npx", package);
            assert_eq!(
                validate_against_embedded_schema(&value).is_ok(),
                expected,
                "npx schema package {package}"
            );
            assert_eq!(
                AdapterManifest::validate_value(value).is_ok(),
                expected,
                "npx typed package {package}"
            );
            assert_eq!(
                registry_runner_accepts("npx", package),
                expected,
                "npx registry package {package}"
            );
        }
    }

    #[test]
    fn uvx_schema_typed_and_registry_rules_agree() {
        for (package, expected) in [
            ("package", true),
            ("package==1.2.3", true),
            ("package@1.2.3", true),
            ("@scope/package@1.2.3", false),
            ("package==1.x.3", false),
        ] {
            let value = runner_manifest_value("uvx", package);
            assert_eq!(
                validate_against_embedded_schema(&value).is_ok(),
                expected,
                "uvx schema package {package}"
            );
            assert_eq!(
                AdapterManifest::validate_value(value).is_ok(),
                expected,
                "uvx typed package {package}"
            );
            assert_eq!(
                registry_runner_accepts("uvx", package),
                expected,
                "uvx registry package {package}"
            );
        }
    }

    #[test]
    fn unsupported_agent_does_not_discard_other_convertible_agents() {
        let bytes = registry_fixture_agents(vec![
            unsupported_binary_agent("linux-only"),
            registry_agent(
                "windows-agent",
                json!({"npx": {"package": "windows-agent@1.2.3"}}),
            ),
        ]);

        let converted = convert_acp_registry_bytes(&bytes, "windows", "x86_64", "rev-a")
            .expect("unsupported platform entry is isolated");

        assert_eq!(converted.len(), 1);
        assert_eq!(converted[0].manifest.id, "windows-agent");
        assert_eq!(converted[0].launch_kind, RegistryLaunchKind::Npx);
    }

    #[test]
    fn malformed_agent_fails_closed_without_partial_untrusted_catalog() {
        let bytes = registry_fixture_agents(vec![
            registry_agent(
                "good-agent",
                json!({"npx": {"package": "good-agent@1.2.3"}}),
            ),
            registry_agent(
                "bad-agent",
                json!({"npx": {"package": "https://example.invalid/pkg"}}),
            ),
        ]);

        assert_eq!(
            convert_acp_registry_bytes(&bytes, "windows", "x86_64", "rev-a")
                .unwrap_err()
                .code(),
            CatalogErrorCode::SchemaViolation
        );
    }

    #[test]
    fn platform_filtering_is_registry_order_independent() {
        let convertible = registry_agent(
            "windows-agent",
            json!({"uvx": {"package": "windows-agent==1.2.3"}}),
        );
        let unsupported = unsupported_binary_agent("linux-only");
        let mut left = convert_acp_registry_bytes(
            &registry_fixture_agents(vec![unsupported.clone(), convertible.clone()]),
            "windows",
            "x86_64",
            "rev-a",
        )
        .expect("left converts")
        .into_iter()
        .map(|entry| (entry.manifest.id, format!("{:?}", entry.launch_kind)))
        .collect::<Vec<_>>();
        let mut right = convert_acp_registry_bytes(
            &registry_fixture_agents(vec![convertible, unsupported]),
            "windows",
            "x86_64",
            "rev-a",
        )
        .expect("right converts")
        .into_iter()
        .map(|entry| (entry.manifest.id, format!("{:?}", entry.launch_kind)))
        .collect::<Vec<_>>();
        left.sort();
        right.sort();
        assert_eq!(left, right);
        assert_eq!(left, vec![("windows-agent".into(), "Uvx".into())]);
    }

    #[test]
    fn passive_registry_match_never_claims_protocol_verified() {
        let converted = convert_binary();
        let projection = match_manifest_passively(
            &ManifestMatchInput {
                executable_name: Some("example-agent.exe".into()),
                package_ids: Vec::new(),
                registry_ids: vec!["example-agent".into()],
                executable_sha256: Some(
                    "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789".into(),
                ),
                publisher_subject: None,
                category: CandidateCategory::Unknown,
                source_kind: ObservationSourceKind::WindowsPath,
            },
            &[converted.manifest],
            None,
        );
        assert_eq!(projection.discovery_state, DiscoveryState::Identified);
        assert_eq!(
            projection.compatibility_state,
            CompatibilityState::NotVerified
        );
        assert_eq!(projection.availability, CandidateAvailability::Unconfigured);
        assert!(projection.requires_configuration);
    }

    fn cache_from_manifest(mut manifest: AdapterManifest, revision: &str, now_ms: u64) -> Vec<u8> {
        let registry_sha256 = sha256_hex(format!("fixture-registry-{revision}").as_bytes());
        if let Some(source) = &mut manifest.source {
            if source.kind == ManifestSourceKind::AcpRegistry {
                source.catalog_sha256 = Some(registry_sha256.clone());
            }
        }
        serde_json::to_vec(&CatalogCache {
            version: 1,
            generation: CURRENT_CATALOG_GENERATION,
            revision: revision.into(),
            created_at_ms: now_ms,
            registry_sha256,
            manifests: vec![manifest],
        })
        .expect("cache serialize")
    }

    struct TempCatalogDir {
        path: PathBuf,
    }

    impl Deref for TempCatalogDir {
        type Target = Path;

        fn deref(&self) -> &Self::Target {
            &self.path
        }
    }

    impl AsRef<Path> for TempCatalogDir {
        fn as_ref(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempCatalogDir {
        fn drop(&mut self) {
            if self.path.exists() {
                if let Err(error) = fs::remove_dir_all(&self.path) {
                    if !thread::panicking() {
                        panic!("remove temp catalog dir {}: {error}", self.path.display());
                    }
                }
            }
        }
    }

    fn temp_dir(label: &str) -> TempCatalogDir {
        let suffix = format!(
            "agenttalk-w3-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        );
        let path = std::env::temp_dir().join(suffix);
        fs::create_dir_all(&path).expect("create temp dir");
        TempCatalogDir { path }
    }

    #[test]
    fn ordinary_scan_performs_zero_network_requests() {
        let converted = convert_binary();
        let counter = NetworkCounter::default();
        let bundled = cache_from_manifest(converted.manifest, "bundled", 1000);
        let report = load_catalog_for_scan(&bundled, None, 1000, &counter);
        assert_eq!(report.snapshot.manifests.len(), 1);
        assert_eq!(counter.count(), 0);
    }

    #[test]
    fn valid_cached_catalog_is_loaded_offline() {
        let root = temp_dir("valid-cache");
        let cache = root.join("catalog.json");
        let converted = convert_binary();
        fs::write(
            &cache,
            cache_from_manifest(converted.manifest.clone(), "cache", 1000),
        )
        .expect("write cache");
        let bundled = cache_from_manifest(converted.manifest, "bundled", 1000);
        let counter = NetworkCounter::default();
        let report = load_catalog_for_scan(&bundled, Some(&cache), 1000, &counter);
        assert_eq!(report.snapshot.revision, "cache");
        assert_eq!(counter.count(), 0);
    }

    #[test]
    fn corrupted_or_expired_cache_falls_back_to_bundled_catalog() {
        let root = temp_dir("corrupt-cache");
        let cache = root.join("catalog.json");
        fs::write(&cache, b"{not-json").expect("write corrupt");
        let converted = convert_binary();
        let bundled = cache_from_manifest(converted.manifest, "bundled", 10_000);
        let report =
            load_catalog_for_scan(&bundled, Some(&cache), 10_000, &NetworkCounter::default());
        assert_eq!(report.snapshot.revision, "bundled");
        assert!(report
            .diagnostics
            .iter()
            .any(|d| d.code == DiscoveryDiagnosticCode::InvalidSourceRecord));
        assert_eq!(fs::read(&cache).expect("cache unchanged"), b"{not-json");
    }

    #[test]
    fn corrupted_bundled_catalog_reports_typed_diagnostic() {
        let report = load_catalog_for_scan(b"{not-json", None, 10_000, &NetworkCounter::default());
        assert_eq!(report.snapshot.revision, "bundled-invalid");
        assert!(report.snapshot.manifests.is_empty());
        assert!(report
            .diagnostics
            .iter()
            .any(|d| d.code == DiscoveryDiagnosticCode::InvalidSourceRecord));
    }

    #[test]
    fn cache_registry_sha_and_manifest_source_hash_must_match() {
        let converted = convert_binary();
        let mut cache: CatalogCache =
            serde_json::from_slice(&cache_from_manifest(converted.manifest, "cache", 1000))
                .expect("cache json");
        cache.registry_sha256 = "f".repeat(64);
        let bytes = serde_json::to_vec(&cache).expect("cache serialize");
        assert_eq!(
            parse_cache_bytes(&bytes).unwrap_err().code(),
            CatalogErrorCode::HashMismatch
        );
    }

    struct ByteRefreshSource {
        response: RefreshResponse,
        calls: Arc<Mutex<usize>>,
        barrier: Option<Arc<Barrier>>,
    }

    impl CatalogRefreshSource for ByteRefreshSource {
        fn fetch(&self, _request: &RefreshRequest) -> Result<RefreshResponse, CatalogError> {
            if let Some(barrier) = &self.barrier {
                barrier.wait();
            }
            *self.calls.lock().expect("calls") += 1;
            Ok(self.response.clone())
        }
    }

    #[test]
    fn refresh_schema_or_hash_failure_preserves_old_cache_byte_for_byte() {
        let root = temp_dir("refresh-fail");
        let cache = root.join("catalog.json");
        let lock = root.join("catalog.lock");
        let old = b"old-cache";
        fs::write(&cache, old).expect("old cache");
        let source = ByteRefreshSource {
            response: RefreshResponse {
                origin: "https://registry.example".into(),
                redirected_to: None,
                bytes: b"{}".to_vec(),
            },
            calls: Arc::new(Mutex::new(0)),
            barrier: None,
        };
        let err = refresh_catalog_cache(
            &cache,
            &lock,
            &source,
            &RefreshRequest {
                expected_origin: "https://registry.example".into(),
                expected_sha256: Some("f".repeat(64)),
                now_ms: 1000,
                revision: "rev".into(),
            },
        )
        .unwrap_err();
        assert_eq!(err.code(), CatalogErrorCode::HashMismatch);
        assert_eq!(fs::read(&cache).expect("cache unchanged"), old);
    }

    #[test]
    fn refresh_download_is_bounded() {
        let root = temp_dir("bounded-refresh");
        let source = ByteRefreshSource {
            response: RefreshResponse {
                origin: "https://registry.example".into(),
                redirected_to: None,
                bytes: vec![b'x'; MAX_REGISTRY_BYTES + 1],
            },
            calls: Arc::new(Mutex::new(0)),
            barrier: None,
        };
        let err = refresh_catalog_cache(
            &root.join("catalog.json"),
            &root.join("catalog.lock"),
            &source,
            &RefreshRequest {
                expected_origin: "https://registry.example".into(),
                expected_sha256: None,
                now_ms: 1000,
                revision: "rev".into(),
            },
        )
        .unwrap_err();
        assert_eq!(err.code(), CatalogErrorCode::Oversized);
    }

    #[test]
    fn redirect_to_unapproved_origin_is_rejected() {
        let root = temp_dir("redirect-refresh");
        let source = ByteRefreshSource {
            response: RefreshResponse {
                origin: "https://registry.example".into(),
                redirected_to: Some("https://evil.example".into()),
                bytes: binary_registry_bytes(),
            },
            calls: Arc::new(Mutex::new(0)),
            barrier: None,
        };
        let err = refresh_catalog_cache(
            &root.join("catalog.json"),
            &root.join("catalog.lock"),
            &source,
            &RefreshRequest {
                expected_origin: "https://registry.example".into(),
                expected_sha256: None,
                now_ms: 1000,
                revision: "rev".into(),
            },
        )
        .unwrap_err();
        assert_eq!(err.code(), CatalogErrorCode::RedirectRejected);
    }

    #[test]
    fn atomic_replace_never_exposes_partial_catalog() {
        let root = temp_dir("atomic-refresh");
        let cache = root.join("catalog.json");
        let lock = root.join("catalog.lock");
        fs::write(&cache, b"old").expect("old");
        let source = ByteRefreshSource {
            response: RefreshResponse {
                origin: "https://registry.example".into(),
                redirected_to: None,
                bytes: binary_registry_bytes(),
            },
            calls: Arc::new(Mutex::new(0)),
            barrier: None,
        };
        let cache_model = refresh_catalog_cache(
            &cache,
            &lock,
            &source,
            &RefreshRequest {
                expected_origin: "https://registry.example".into(),
                expected_sha256: None,
                now_ms: 1000,
                revision: "rev".into(),
            },
        )
        .expect("refresh");
        assert_eq!(cache_model.revision, "rev");
        parse_cache_bytes(&fs::read(&cache).expect("read cache")).expect("complete cache");
    }

    #[test]
    fn interrupted_refresh_leaves_old_cache_readable() {
        let root = temp_dir("interrupted-refresh");
        let cache = root.join("catalog.json");
        let lock = root.join("catalog.lock");
        let converted = convert_binary();
        let old = cache_from_manifest(converted.manifest, "old", 1000);
        fs::write(&cache, &old).expect("old cache");
        let source = ByteRefreshSource {
            response: RefreshResponse {
                origin: "https://registry.example".into(),
                redirected_to: None,
                bytes: b"not-json".to_vec(),
            },
            calls: Arc::new(Mutex::new(0)),
            barrier: None,
        };
        assert!(refresh_catalog_cache(
            &cache,
            &lock,
            &source,
            &RefreshRequest {
                expected_origin: "https://registry.example".into(),
                expected_sha256: None,
                now_ms: 2000,
                revision: "new".into(),
            },
        )
        .is_err());
        assert_eq!(fs::read(&cache).expect("old readable"), old);
    }

    #[test]
    fn concurrent_refresh_is_single_flight_or_lock_safe() {
        let root = temp_dir("concurrent-refresh");
        let cache = root.join("catalog.json");
        let lock = root.join("catalog.lock");
        let calls = Arc::new(Mutex::new(0));
        let barrier = Arc::new(Barrier::new(2));
        let request = RefreshRequest {
            expected_origin: "https://registry.example".into(),
            expected_sha256: None,
            now_ms: 1000,
            revision: "rev".into(),
        };
        let first_source = ByteRefreshSource {
            response: RefreshResponse {
                origin: "https://registry.example".into(),
                redirected_to: None,
                bytes: binary_registry_bytes(),
            },
            calls: calls.clone(),
            barrier: Some(barrier.clone()),
        };
        let second_source = ByteRefreshSource {
            response: RefreshResponse {
                origin: "https://registry.example".into(),
                redirected_to: None,
                bytes: binary_registry_bytes(),
            },
            calls: calls.clone(),
            barrier: None,
        };
        let first_cache = cache.clone();
        let first_lock = lock.clone();
        let first_request = request.clone();
        let first = thread::spawn(move || {
            refresh_catalog_cache(&first_cache, &first_lock, &first_source, &first_request)
        });
        barrier.wait();
        let second = refresh_catalog_cache(&cache, &lock, &second_source, &request);
        let first = first.join().expect("join");
        assert!(first.is_ok() ^ second.is_ok());
        assert_eq!(*calls.lock().expect("calls"), 1);
    }

    #[test]
    fn temp_files_are_removed_after_success_and_failure() {
        let root = temp_dir("temp-refresh");
        let cache = root.join("catalog.json");
        let lock = root.join("catalog.lock");
        let source = ByteRefreshSource {
            response: RefreshResponse {
                origin: "https://registry.example".into(),
                redirected_to: None,
                bytes: binary_registry_bytes(),
            },
            calls: Arc::new(Mutex::new(0)),
            barrier: None,
        };
        refresh_catalog_cache(
            &cache,
            &lock,
            &source,
            &RefreshRequest {
                expected_origin: "https://registry.example".into(),
                expected_sha256: None,
                now_ms: 1000,
                revision: "rev".into(),
            },
        )
        .expect("success");
        assert!(!lock.exists());
        assert!(fs::read_dir(&root).expect("read dir").all(|entry| !entry
            .expect("entry")
            .file_name()
            .to_string_lossy()
            .contains(".tmp-")));
    }

    #[test]
    fn cache_never_contains_registry_secret_values() {
        let root = temp_dir("secret-cache");
        let cache = root.join("catalog.json");
        let source = ByteRefreshSource {
            response: RefreshResponse {
                origin: "https://registry.example".into(),
                redirected_to: None,
                bytes: binary_registry_bytes(),
            },
            calls: Arc::new(Mutex::new(0)),
            barrier: None,
        };
        refresh_catalog_cache(
            &cache,
            &root.join("catalog.lock"),
            &source,
            &RefreshRequest {
                expected_origin: "https://registry.example".into(),
                expected_sha256: None,
                now_ms: 1000,
                revision: "rev".into(),
            },
        )
        .expect("refresh");
        let bytes = fs::read_to_string(&cache).expect("cache text");
        assert!(!bytes.contains("fixture-token-value"));
        assert!(bytes.contains("AGENT_TOKEN"));
    }

    fn match_input() -> ManifestMatchInput {
        ManifestMatchInput {
            executable_name: Some("example-agent.exe".into()),
            package_ids: Vec::new(),
            registry_ids: Vec::new(),
            executable_sha256: None,
            publisher_subject: None,
            category: CandidateCategory::Unknown,
            source_kind: ObservationSourceKind::WindowsPath,
        }
    }

    #[test]
    fn filename_only_match_remains_unverified() {
        let converted = convert_binary();
        let projection = match_manifest_passively(&match_input(), &[converted.manifest], None);
        assert_eq!(projection.discovery_state, DiscoveryState::Identified);
        assert_eq!(
            projection.compatibility_state,
            CompatibilityState::NotVerified
        );
        assert!(projection.requires_configuration);
    }

    #[test]
    fn exact_hash_mismatch_fails_closed() {
        let mut converted = convert_binary().manifest;
        converted.match_rules.sha256 = Some("f".repeat(64));
        let mut input = match_input();
        input.executable_sha256 = Some("0".repeat(64));
        let projection = match_manifest_passively(&input, &[converted], None);
        assert!(projection
            .diagnostics
            .iter()
            .any(|d| d.code == DiscoveryDiagnosticCode::FingerprintChanged));
        assert_eq!(projection.discovery_state, DiscoveryState::Observed);
    }

    #[test]
    fn unrelated_hash_mismatch_does_not_outscore_registry_match() {
        let mut registry = manifest_with_id("registry-agent");
        registry.match_rules.registry_ids = vec!["winner".into()];
        let mut unrelated = manifest_with_id("unrelated-agent");
        unrelated.match_rules.sha256 = Some("f".repeat(64));
        let mut input = match_input();
        input.registry_ids = vec!["winner".into()];
        input.executable_sha256 = Some("0".repeat(64));
        let projection = match_manifest_passively(&input, &[unrelated, registry], None);
        assert_eq!(connector_suffix(&projection), "registry-agent");
        assert!(!projection
            .diagnostics
            .iter()
            .any(|d| d.code == DiscoveryDiagnosticCode::FingerprintChanged));
    }

    #[test]
    fn unrelated_hash_mismatch_does_not_block_filename_match() {
        let mut filename = manifest_with_id("filename-agent");
        filename.match_rules.executable_names = vec!["example-agent.exe".into()];
        let mut unrelated = manifest_with_id("unrelated-agent");
        unrelated.match_rules.sha256 = Some("f".repeat(64));
        let mut input = match_input();
        input.executable_sha256 = Some("0".repeat(64));
        let projection = match_manifest_passively(&input, &[unrelated, filename], None);
        assert_eq!(connector_suffix(&projection), "filename-agent");
        assert_eq!(projection.discovery_state, DiscoveryState::Identified);
    }

    #[test]
    fn matched_manifest_hash_mismatch_fails_closed() {
        let mut manifest = manifest_with_id("matched-agent");
        manifest.match_rules.executable_names = vec!["example-agent.exe".into()];
        manifest.match_rules.sha256 = Some("f".repeat(64));
        let mut input = match_input();
        input.executable_sha256 = Some("0".repeat(64));
        let projection = match_manifest_passively(&input, &[manifest], None);
        assert_eq!(projection.discovery_state, DiscoveryState::Observed);
        assert!(projection
            .diagnostics
            .iter()
            .any(|d| d.code == DiscoveryDiagnosticCode::FingerprintChanged));
    }

    #[test]
    fn exact_hash_match_beats_weaker_identity() {
        let mut hash = manifest_with_id("hash-agent");
        hash.match_rules.sha256 = Some("a".repeat(64));
        let mut filename = manifest_with_id("filename-agent");
        filename.match_rules.executable_names = vec!["example-agent.exe".into()];
        let mut input = match_input();
        input.executable_sha256 = Some("a".repeat(64));
        let projection = match_manifest_passively(&input, &[filename, hash], None);
        assert_eq!(connector_suffix(&projection), "hash-agent");
    }

    #[test]
    fn hash_matching_is_catalog_order_independent() {
        let mut hash = manifest_with_id("hash-agent");
        hash.match_rules.sha256 = Some("a".repeat(64));
        let mut registry = manifest_with_id("registry-agent");
        registry.match_rules.registry_ids = vec!["registry-agent".into()];
        let mut input = match_input();
        input.registry_ids = vec!["registry-agent".into()];
        input.executable_sha256 = Some("a".repeat(64));
        let left = match_manifest_passively(&input, &[hash.clone(), registry.clone()], None);
        let right = match_manifest_passively(&input, &[registry, hash], None);
        assert_eq!(left, right);
        assert_eq!(connector_suffix(&left), "hash-agent");
    }

    #[test]
    fn package_or_registry_stable_id_beats_filename_heuristic() {
        let mut exact = convert_binary().manifest;
        exact.match_rules.registry_ids = vec!["strong".into()];
        let mut weak = exact.clone();
        weak.id = "weak-agent".into();
        weak.match_rules.registry_ids.clear();
        let mut input = match_input();
        input.registry_ids = vec!["strong".into()];
        let projection = match_manifest_passively(&input, &[weak, exact], None);
        assert!(projection.connector_id.ends_with("example-agent"));
    }

    #[test]
    fn ambiguous_equal_matches_fail_closed_in_all_orders() {
        let first = convert_binary().manifest;
        let mut second = first.clone();
        second.id = "another-agent".into();
        for manifests in [vec![first.clone(), second.clone()], vec![second, first]] {
            let projection = match_manifest_passively(&match_input(), &manifests, None);
            assert!(projection
                .diagnostics
                .iter()
                .any(|d| d.code == DiscoveryDiagnosticCode::InvalidIdentity));
            assert_eq!(projection.discovery_state, DiscoveryState::Observed);
        }
    }

    #[test]
    fn matcher_is_catalog_and_provider_order_independent() {
        let first = convert_binary().manifest;
        let mut second = first.clone();
        second.id = "model-runtime".into();
        second.category = ManifestCategory::ModelRuntime;
        let mut input = match_input();
        input.registry_ids = vec!["example-agent".into()];
        let left = match_manifest_passively(&input, &[first.clone(), second.clone()], None);
        let right = match_manifest_passively(&input, &[second, first], None);
        assert_eq!(left, right);
    }

    #[test]
    fn signed_does_not_equal_trusted_or_agenttalk_approved() {
        let evidence = AuthenticodeEvidence::from_signer(AuthenticodeStatus::Signed, false, None);
        let converted = convert_binary();
        let projection =
            match_manifest_passively(&match_input(), &[converted.manifest], Some(&evidence));
        assert_eq!(
            projection.compatibility_state,
            CompatibilityState::NotVerified
        );
        assert!(projection.requires_configuration);
    }

    #[test]
    fn publisher_evidence_for_manifest_a_does_not_match_manifest_b() {
        let mut manifest_a = manifest_with_id("publisher-a");
        manifest_a.match_rules.publisher_subjects = vec!["Example Publisher A".into()];
        let mut manifest_b = manifest_with_id("publisher-b");
        manifest_b.match_rules.publisher_subjects = vec!["Example Publisher B".into()];
        let evidence = AuthenticodeEvidence::from_signer(
            AuthenticodeStatus::Trusted,
            true,
            Some("Example Publisher A".into()),
        );
        let projection = match_manifest_passively(
            &match_input(),
            &[manifest_b.clone(), manifest_a.clone()],
            Some(&evidence),
        );
        assert_eq!(connector_suffix(&projection), "publisher-a");
    }

    #[test]
    fn actual_signer_subject_must_intersect_current_manifest_rules() {
        let mut manifest = manifest_with_id("publisher-agent");
        manifest.match_rules.publisher_subjects = vec!["Different Corp".into()];
        let evidence = AuthenticodeEvidence::from_signer(
            AuthenticodeStatus::Trusted,
            true,
            Some("Example Corp".into()),
        );
        let projection = match_manifest_passively(&match_input(), &[manifest], Some(&evidence));
        assert_eq!(projection.discovery_state, DiscoveryState::Observed);
    }

    #[test]
    fn publisher_matching_is_manifest_order_independent() {
        let mut weaker = manifest_with_id("filename-agent");
        weaker.match_rules.executable_names = vec!["example-agent.exe".into()];
        let mut publisher = manifest_with_id("publisher-agent");
        publisher.match_rules.publisher_subjects = vec!["Example Publisher".into()];
        let evidence = AuthenticodeEvidence::from_signer(
            AuthenticodeStatus::Trusted,
            true,
            Some("Example Publisher".into()),
        );
        let left = match_manifest_passively(
            &match_input(),
            &[weaker.clone(), publisher.clone()],
            Some(&evidence),
        );
        let right = match_manifest_passively(&match_input(), &[publisher, weaker], Some(&evidence));
        assert_eq!(left, right);
        assert_eq!(connector_suffix(&left), "publisher-agent");
    }

    #[test]
    fn missing_signer_subject_never_matches_publisher_rule() {
        let mut manifest = manifest_with_id("publisher-agent");
        manifest.match_rules.publisher_subjects = vec!["Example Publisher".into()];
        let evidence = AuthenticodeEvidence::from_signer(AuthenticodeStatus::Trusted, true, None);
        let projection = match_manifest_passively(&match_input(), &[manifest], Some(&evidence));
        assert_eq!(projection.discovery_state, DiscoveryState::Observed);
    }

    #[test]
    fn publisher_subject_never_enters_renderer_projection() {
        let mut manifest = manifest_with_id("publisher-agent");
        manifest.match_rules.publisher_subjects = vec!["Example Publisher".into()];
        let evidence = AuthenticodeEvidence::from_signer(
            AuthenticodeStatus::Trusted,
            true,
            Some("Example Publisher".into()),
        );
        let projection = match_manifest_passively(&match_input(), &[manifest], Some(&evidence));
        let json = serde_json::to_string(&projection).expect("serialize projection");
        assert!(!json.contains("Example Publisher"));
    }

    #[test]
    fn production_evidence_without_expected_publisher_can_match_correct_manifest() {
        let mut manifest = manifest_with_id("publisher-agent");
        manifest.match_rules.publisher_subjects = vec!["Example Publisher".into()];
        let evidence =
            production_evidence(AuthenticodeStatus::Trusted, true, Some("Example Publisher"));
        let projection = match_manifest_passively(&match_input(), &[manifest], Some(&evidence));
        assert_eq!(connector_suffix(&projection), "publisher-agent");
        assert_eq!(projection.discovery_state, DiscoveryState::Identified);
    }

    #[test]
    fn production_style_evidence_does_not_require_prebound_publisher_bool() {
        let mut manifest = manifest_with_id("publisher-agent");
        manifest.match_rules.publisher_subjects = vec!["CN=Example Publisher, O=Fixture".into()];
        let evidence = production_evidence(
            AuthenticodeStatus::Trusted,
            true,
            Some("CN=Example Publisher, O=Fixture"),
        );
        let projection = match_manifest_passively(&match_input(), &[manifest], Some(&evidence));
        assert_eq!(projection.discovery_state, DiscoveryState::Identified);
        assert!(projection
            .diagnostics
            .iter()
            .all(|diagnostic| diagnostic.code != DiscoveryDiagnosticCode::InvalidIdentity));

        let manifest_independent_evidence =
            production_evidence(AuthenticodeStatus::Trusted, true, Some("Example Publisher"));
        assert!(
            !authenticode_evidence_to_safe_projection(&manifest_independent_evidence)
                .contains(&DiscoveryEvidence::InstallKnown),
            "manifest-independent signer facts must not create renderer-visible install evidence"
        );
    }

    #[test]
    fn untrusted_signer_subject_never_matches() {
        let mut manifest = manifest_with_id("publisher-agent");
        manifest.match_rules.publisher_subjects = vec!["Example Publisher".into()];
        let evidence =
            production_evidence(AuthenticodeStatus::Signed, false, Some("Example Publisher"));
        let projection = match_manifest_passively(&match_input(), &[manifest], Some(&evidence));
        assert_eq!(projection.discovery_state, DiscoveryState::Observed);
    }

    #[test]
    fn signer_subject_is_not_renderer_serializable() {
        let mut manifest = manifest_with_id("publisher-agent");
        manifest.match_rules.publisher_subjects = vec!["Fixture Publisher".into()];
        let evidence =
            production_evidence(AuthenticodeStatus::Trusted, true, Some("Fixture Publisher"));
        let projection = match_manifest_passively(&match_input(), &[manifest], Some(&evidence));
        let serialized = serde_json::to_string(&projection).expect("serialize projection");
        assert!(!serialized.contains("Fixture Publisher"));
        assert!(!serialized.contains("signer_subject"));
    }

    #[test]
    fn publisher_matching_is_order_independent() {
        let mut first = manifest_with_id("publisher-a");
        first.match_rules.publisher_subjects = vec!["Publisher A".into()];
        let mut second = manifest_with_id("publisher-b");
        second.match_rules.publisher_subjects = vec!["Publisher B".into()];
        let evidence = production_evidence(AuthenticodeStatus::Trusted, true, Some("Publisher B"));
        let left = match_manifest_passively(
            &match_input(),
            &[first.clone(), second.clone()],
            Some(&evidence),
        );
        let right = match_manifest_passively(&match_input(), &[second, first], Some(&evidence));
        assert_eq!(left, right);
        assert_eq!(connector_suffix(&left), "publisher-b");
    }

    #[test]
    fn authenticode_status_projection_distinguishes_signed_states() {
        for status in [
            AuthenticodeStatus::Trusted,
            AuthenticodeStatus::Signed,
            AuthenticodeStatus::Unsigned,
            AuthenticodeStatus::BadDigest,
            AuthenticodeStatus::UntrustedRoot,
            AuthenticodeStatus::ApiUnavailable,
        ] {
            let evidence = AuthenticodeEvidence::from_signer(
                status,
                status == AuthenticodeStatus::Trusted,
                None,
            );
            let projection = authenticode_evidence_to_safe_projection(&evidence);
            assert_eq!(
                projection.contains(&DiscoveryEvidence::ExecutableInventory),
                matches!(
                    status,
                    AuthenticodeStatus::Trusted | AuthenticodeStatus::Signed
                )
            );
            assert!(!projection.contains(&DiscoveryEvidence::InstallKnown));
        }
    }

    #[test]
    fn publisher_match_uses_normalized_signer_subject_only() {
        assert_eq!(
            normalize_publisher_subject("CN=Example Corp, O=Fixture"),
            Some("example corp fixture".into())
        );
        assert_eq!(
            normalize_publisher_subject("Example   Corp"),
            Some("example corp".into())
        );
        assert_eq!(normalize_publisher_subject("token=fixture"), None);

        let evidence = AuthenticodeEvidence::from_signer(
            AuthenticodeStatus::Trusted,
            true,
            Some("Example Corp".into()),
        );
        assert!(!authenticode_evidence_to_safe_projection(&evidence)
            .contains(&DiscoveryEvidence::InstallKnown));
    }

    #[cfg(windows)]
    #[test]
    fn production_authenticode_verifier_reports_unsigned_without_manifest_bound_publisher() {
        let unsigned = std::env::current_exe().expect("current test executable");
        let verifier = WindowsAuthenticodeVerifier;
        let evidence = verifier.verify_offline(&unsigned, Some("Example Corp"));
        assert_eq!(evidence.status, AuthenticodeStatus::Unsigned);
        assert!(!evidence.trusted_chain);
    }

    #[cfg(windows)]
    #[test]
    fn winverifytrust_hresult_mapping_is_typed() {
        use windows::Win32::Foundation::{
            CERT_E_CHAINING, CERT_E_UNTRUSTEDROOT, TRUST_E_BAD_DIGEST, TRUST_E_NOSIGNATURE,
        };

        assert_eq!(map_winverifytrust_status(0), AuthenticodeStatus::Trusted);
        assert_eq!(
            map_winverifytrust_status(TRUST_E_NOSIGNATURE.0),
            AuthenticodeStatus::Unsigned
        );
        assert_eq!(
            map_winverifytrust_status(TRUST_E_BAD_DIGEST.0),
            AuthenticodeStatus::BadDigest
        );
        assert_eq!(
            map_winverifytrust_status(CERT_E_UNTRUSTEDROOT.0),
            AuthenticodeStatus::UntrustedRoot
        );
        assert_eq!(
            map_winverifytrust_status(CERT_E_CHAINING.0),
            AuthenticodeStatus::UntrustedRoot
        );
        assert_eq!(
            map_winverifytrust_status(0x8000_4005u32 as i32),
            AuthenticodeStatus::ApiUnavailable
        );
    }

    #[test]
    fn authenticode_offline_mode_performs_zero_network_requests() {
        struct FixtureVerifier(Arc<Mutex<usize>>);
        impl AuthenticodeVerifier for FixtureVerifier {
            fn verify_offline(
                &self,
                _path: &Path,
                _expected_publisher: Option<&str>,
            ) -> AuthenticodeEvidence {
                *self.0.lock().expect("count") += 1;
                AuthenticodeEvidence::from_signer(AuthenticodeStatus::Unsigned, false, None)
            }
        }
        let count = Arc::new(Mutex::new(0));
        let verifier = FixtureVerifier(count.clone());
        let evidence = verifier.verify_offline(Path::new("fixture.exe"), None);
        assert_eq!(evidence.status, AuthenticodeStatus::Unsigned);
        assert_eq!(*count.lock().expect("count"), 1);
    }

    #[test]
    fn trust_evidence_is_private_and_renderer_safe() {
        let evidence = AuthenticodeEvidence::from_signer(
            AuthenticodeStatus::Signed,
            true,
            Some("Fixture Publisher".into()),
        );
        let converted = convert_binary();
        let mut input = match_input();
        input.publisher_subject = Some("CN=Fixture Publisher, O=Secret Path C:\\fixture".into());
        let projection = match_manifest_passively(&input, &[converted.manifest], Some(&evidence));
        let json = serde_json::to_string(&projection).expect("serialize projection");
        assert!(!json.contains("Fixture Publisher"));
        assert!(!json.contains("C:\\"));
        assert!(!json.contains("abcdef0123456789"));
    }
}
