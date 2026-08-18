use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, TryRecvError};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use agenttalk_domain::{
    AuthState, CandidateAvailability, CandidateCategory, CompatibilityState, DiscoveryState,
    ObservationSourceKind,
};
use serde::de::IgnoredAny;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};

use super::super::{
    catalog::ManifestMatchInput, manifest::*, ManagedChild, ManagedDirectStdioSpec, Observation,
};
use crate::{
    has_reparse_point, is_real_regular_file, is_windows_executable_file,
    open_verified_executable_guard, stable_file_fingerprint_with_deadline, VerifiedExecutableGuard,
};

const ACP_PROTOCOL_MAJOR: u16 = 1;
const MAX_ACP_REQUEST_BYTES: usize = 16 * 1024;
const MAX_ACP_RESPONSE_BYTES: usize = 64 * 1024;
const MAX_ACP_STDERR_BYTES: usize = 16 * 1024;
const ACP_CLEANUP_GRACE: Duration = Duration::from_millis(500);
const ACP_RESPONSE_EXIT_OBSERVATION_GRACE: Duration = Duration::from_millis(25);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AcpVerificationStatus {
    Verified,
    AuthRequired,
    Rejected,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AcpVerificationDiagnosticCode {
    ConsentRequired,
    IdentityMismatch,
    IdentityUnverified,
    Timeout,
    Cancelled,
    LaunchFailed,
    ProtocolMismatch,
    ProtocolViolation,
    OversizedFrame,
    NonUtf8Frame,
    StderrOutput,
    ProcessFailed,
    AuthenticationRequired,
    CleanupFailed,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AcpAgentInfo {
    pub name: String,
    pub title: Option<String>,
    pub version: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AcpCapabilitySummary {
    pub load_session: bool,
    pub prompt_image: bool,
    pub prompt_audio: bool,
    pub prompt_embedded_context: bool,
    pub mcp_http: bool,
    pub mcp_sse: bool,
    pub supports_logout: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AcpCompatibilityReport {
    pub candidate_id: String,
    pub status: AcpVerificationStatus,
    pub compatibility_state: CompatibilityState,
    pub auth_state: AuthState,
    pub requires_configuration: bool,
    pub protocol_major: Option<u16>,
    pub agent_info: Option<AcpAgentInfo>,
    pub capabilities: AcpCapabilitySummary,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diagnostic: Option<AcpVerificationDiagnosticCode>,
}

/// Renderer-safe metadata for W5's read-only import-plan preview. It is
/// derived only after the ACP target remains bound to the same local identity.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AcpImportPlanMetadata {
    pub candidate_id: String,
    pub adapter_kind: String,
    pub protocol_major: u16,
    pub manifest_id: String,
    pub manifest_sha256: String,
    pub candidate_binding_digest: String,
    pub auth_required: bool,
    pub capabilities: AcpCapabilitySummary,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AcpClassificationError {
    UnsupportedManifest,
    ObservationMismatch,
    UnsafeExecutable,
    FingerprintUnavailable,
}

/// Opaque Core-owned binding between a passively discovered executable and an
/// ACP manifest. It deliberately has neither a public raw-path constructor nor
/// a serialization implementation.
#[derive(Clone)]
pub struct AcpPassiveObservation {
    candidate_id: String,
    executable: PathBuf,
    executable_identity: String,
    executable_sha256: String,
    source_kind: ObservationSourceKind,
}

impl std::fmt::Debug for AcpPassiveObservation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AcpPassiveObservation")
            .field("source_kind", &self.source_kind)
            .finish()
    }
}

impl AcpPassiveObservation {
    /// Converts only Core-owned passive discovery evidence into an ACP launch
    /// target. This is crate-private so callers cannot supply an arbitrary
    /// path, working directory, or renderer payload.
    pub(crate) fn from_passive_observation(
        candidate_id: &str,
        observation: &Observation,
        deadline: Instant,
        cancelled: &AtomicBool,
    ) -> Result<Self, AcpClassificationError> {
        let observed_executable = observation
            .executable_locator()
            .ok_or(AcpClassificationError::ObservationMismatch)?;
        let executable = canonical_observed_executable(observed_executable)
            .ok_or(AcpClassificationError::UnsafeExecutable)?;
        let fingerprint = stable_file_fingerprint_with_deadline(&executable, deadline, cancelled)
            .map_err(|_| AcpClassificationError::FingerprintUnavailable)?;
        if !observation.matches_windows_executable_identity(&fingerprint.stable_identity) {
            return Err(AcpClassificationError::ObservationMismatch);
        }
        Ok(Self {
            candidate_id: candidate_id.to_owned(),
            executable,
            executable_identity: fingerprint.stable_identity,
            executable_sha256: fingerprint.content_sha256,
            source_kind: observation.source_kind,
        })
    }

    #[cfg(test)]
    pub(crate) fn from_observed_executable(
        observed_executable: &Path,
        source_kind: ObservationSourceKind,
        deadline: Instant,
        cancelled: &AtomicBool,
    ) -> Result<Self, AcpClassificationError> {
        let executable = canonical_observed_executable(observed_executable)
            .ok_or(AcpClassificationError::UnsafeExecutable)?;
        let fingerprint = stable_file_fingerprint_with_deadline(&executable, deadline, cancelled)
            .map_err(|_| AcpClassificationError::FingerprintUnavailable)?;
        Ok(Self {
            candidate_id: candidate_id_for_executable_identity(&fingerprint.stable_identity),
            executable,
            executable_identity: fingerprint.stable_identity,
            executable_sha256: fingerprint.content_sha256,
            source_kind,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AcpTargetBinding {
    candidate_id: String,
    binding_digest: String,
}

/// Private process and identity state paired with a renderer-safe passive
/// projection. This intentionally has no serialization implementation.
#[derive(Clone)]
pub struct AcpClassification {
    manifest: AdapterManifest,
    executable: PathBuf,
    executable_identity: String,
    executable_sha256: String,
    source_kind: ObservationSourceKind,
    projection: agenttalk_domain::CandidateProjection,
    binding: AcpTargetBinding,
}

impl std::fmt::Debug for AcpClassification {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AcpClassification")
            .field("candidate_id", &self.projection.candidate_id)
            .field("source_kind", &self.source_kind)
            .finish()
    }
}

impl AcpClassification {
    pub fn candidate_id(&self) -> &str {
        &self.projection.candidate_id
    }

    pub fn projection(&self) -> &agenttalk_domain::CandidateProjection {
        &self.projection
    }

    pub(crate) fn manifest_id(&self) -> &str {
        &self.manifest.id
    }

    pub(crate) fn binding(&self) -> &AcpTargetBinding {
        &self.binding
    }

    pub(crate) fn executable(&self) -> &Path {
        &self.executable
    }

    pub(crate) fn executable_identity(&self) -> &str {
        &self.executable_identity
    }

    pub(crate) fn executable_sha256(&self) -> &str {
        &self.executable_sha256
    }

    pub(crate) fn launch(&self) -> &ManifestLaunch {
        &self.manifest.launch
    }

    /// Whether the observed executable has an identity independent of its
    /// file name. A filename-only heuristic match (e.g. `copilot.exe` on
    /// PATH) proves nothing about the executable, so it must never reach an
    /// ACP child spawn or initialize. An independent identity exists only
    /// when the observation is an explicit user selection or the manifest
    /// pins an exact executable hash that matches the observed file.
    pub(crate) fn has_independent_identity(&self) -> bool {
        self.source_kind == ObservationSourceKind::UserSelected
            || self
                .manifest
                .match_rules
                .sha256
                .as_deref()
                .is_some_and(|expected| expected == self.executable_sha256)
    }
}

/// Structured consent is tied to the renderer-safe candidate ID. It carries
/// no path, manifest, credential, or arbitrary launch input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AcpVerificationConsent {
    candidate_id: String,
}

impl AcpVerificationConsent {
    pub fn for_candidate(candidate_id: &str) -> Self {
        Self {
            candidate_id: candidate_id.to_owned(),
        }
    }

    pub(crate) fn candidate_id(&self) -> &str {
        &self.candidate_id
    }
}

#[derive(Clone)]
pub struct AcpVerificationResult {
    report: AcpCompatibilityReport,
    binding: Option<AcpTargetBinding>,
}

impl std::fmt::Debug for AcpVerificationResult {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AcpVerificationResult")
            .field("report", &self.report)
            .finish()
    }
}

impl AcpVerificationResult {
    pub fn report(&self) -> &AcpCompatibilityReport {
        &self.report
    }

    pub(crate) fn binding(&self) -> Option<&AcpTargetBinding> {
        self.binding.as_ref()
    }
}

pub(crate) fn import_plan_metadata(
    classification: &AcpClassification,
    consent: &AcpVerificationConsent,
    verification: &AcpVerificationResult,
    deadline: Instant,
    cancelled: &AtomicBool,
) -> Result<AcpImportPlanMetadata, AcpClassificationError> {
    if consent.candidate_id != classification.candidate_id()
        || verification.report.candidate_id != classification.candidate_id()
        || !matches!(
            verification.report.status,
            AcpVerificationStatus::Verified | AcpVerificationStatus::AuthRequired
        )
    {
        return Err(AcpClassificationError::ObservationMismatch);
    }
    if !identity_is_current(classification, deadline, cancelled) {
        return Err(AcpClassificationError::ObservationMismatch);
    }
    if verification.report.status == AcpVerificationStatus::Verified
        && verification.binding.as_ref() != Some(&classification.binding)
    {
        return Err(AcpClassificationError::ObservationMismatch);
    }
    let manifest_bytes = serde_json::to_vec(&classification.manifest.to_sanitized_value())
        .map_err(|_| AcpClassificationError::ObservationMismatch)?;
    let manifest_sha256 = Sha256::digest(manifest_bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    Ok(AcpImportPlanMetadata {
        candidate_id: classification.candidate_id().to_owned(),
        adapter_kind: "acp".into(),
        protocol_major: classification.manifest.protocol.major,
        manifest_id: classification.manifest.id.clone(),
        manifest_sha256,
        candidate_binding_digest: classification.binding.binding_digest.clone(),
        auth_required: verification.report.status == AcpVerificationStatus::AuthRequired,
        capabilities: verification.report.capabilities.clone(),
    })
}

pub fn classify(
    manifest: &AdapterManifest,
    observation: AcpPassiveObservation,
) -> Result<AcpClassification, AcpClassificationError> {
    if !is_supported_acp_manifest(manifest) {
        return Err(AcpClassificationError::UnsupportedManifest);
    }
    if !manifest_launch_matches_observation(manifest, &observation.executable) {
        return Err(AcpClassificationError::ObservationMismatch);
    }
    let executable_name = observation
        .executable
        .file_name()
        .and_then(|name| name.to_str())
        .map(str::to_owned);
    let projection = crate::match_manifest_passively(
        &ManifestMatchInput {
            executable_name,
            package_ids: Vec::new(),
            registry_ids: Vec::new(),
            executable_sha256: Some(observation.executable_sha256.clone()),
            publisher_subject: None,
            category: CandidateCategory::Unknown,
            source_kind: observation.source_kind,
        },
        std::slice::from_ref(manifest),
        None,
    );
    if projection.discovery_state != DiscoveryState::Identified
        || projection.compatibility_state != CompatibilityState::NotVerified
        || projection.runtime_type != "acp"
        || projection.availability != CandidateAvailability::Unconfigured
        || !projection.requires_configuration
    {
        return Err(AcpClassificationError::ObservationMismatch);
    }
    let binding = AcpTargetBinding {
        candidate_id: observation.candidate_id.clone(),
        binding_digest: target_binding_digest(
            &observation.candidate_id,
            &manifest.id,
            &observation.executable_identity,
            &observation.executable_sha256,
        ),
    };
    let mut projection = projection;
    projection.candidate_id = observation.candidate_id.clone();
    Ok(AcpClassification {
        manifest: manifest.clone(),
        executable: observation.executable,
        executable_identity: observation.executable_identity,
        executable_sha256: observation.executable_sha256,
        source_kind: observation.source_kind,
        projection,
        binding,
    })
}

pub(crate) fn verify(
    classification: &AcpClassification,
    consent: Option<&AcpVerificationConsent>,
    deadline: Instant,
    cancelled: &AtomicBool,
) -> AcpVerificationResult {
    if consent.is_none_or(|consent| consent.candidate_id != classification.candidate_id()) {
        return rejected(
            classification,
            AcpVerificationDiagnosticCode::ConsentRequired,
            None,
        );
    }
    // A filename-only heuristic observation has no independent identity:
    // consent cannot upgrade trust, so the launch boundary fails closed
    // before any child process, initialize request, or workload.
    if !classification.has_independent_identity() {
        return rejected(
            classification,
            AcpVerificationDiagnosticCode::IdentityUnverified,
            None,
        );
    }
    if cancelled.load(Ordering::Acquire) {
        return rejected(
            classification,
            AcpVerificationDiagnosticCode::Cancelled,
            None,
        );
    }
    let manifest_deadline = Instant::now()
        + Duration::from_millis(u64::from(classification.manifest.verification.timeout_ms));
    let deadline = deadline.min(manifest_deadline);
    if Instant::now() >= deadline {
        return rejected(classification, AcpVerificationDiagnosticCode::Timeout, None);
    }
    // The final identity recheck opens the executable with a share mode that
    // denies concurrent write/delete and keeps that handle alive (the guard)
    // until the child is created, so the bytes the spawn loads cannot diverge
    // from the bytes that were fingerprinted.
    let guard =
        match open_verified_executable_guard(&classification.executable, deadline, cancelled) {
            Ok(guard) => guard,
            Err(_) => {
                return rejected(
                    classification,
                    if cancelled.load(Ordering::Acquire) {
                        AcpVerificationDiagnosticCode::Cancelled
                    } else {
                        AcpVerificationDiagnosticCode::IdentityMismatch
                    },
                    None,
                );
            }
        };
    if guard.fingerprint.stable_identity != classification.executable_identity
        || guard.fingerprint.content_sha256 != classification.executable_sha256
    {
        return rejected(
            classification,
            AcpVerificationDiagnosticCode::IdentityMismatch,
            None,
        );
    }
    // Re-check cancellation after the guard was acquired (and the file was
    // re-read) but before any child process is created.
    if cancelled.load(Ordering::Acquire) {
        return rejected(
            classification,
            AcpVerificationDiagnosticCode::Cancelled,
            None,
        );
    }

    let mut cwd = match AcpVerificationCwd::create() {
        Ok(cwd) => cwd,
        Err(()) => {
            return rejected(
                classification,
                AcpVerificationDiagnosticCode::LaunchFailed,
                None,
            )
        }
    };
    let outcome = verify_in_owned_process(classification, &guard, cwd.path(), deadline, cancelled);
    if cwd.cleanup().is_err() {
        return rejected(
            classification,
            AcpVerificationDiagnosticCode::CleanupFailed,
            None,
        );
    }
    match outcome {
        Ok(wire) => accepted(classification, wire),
        Err(code) => rejected(classification, code, None),
    }
}

fn is_supported_acp_manifest(manifest: &AdapterManifest) -> bool {
    matches!(manifest.category, ManifestCategory::AgentProtocol)
        && matches!(manifest.protocol.kind, ManifestProtocolKind::Acp)
        && manifest.protocol.major == ACP_PROTOCOL_MAJOR
        && matches!(
            manifest.launch,
            ManifestLaunch::Direct {
                transport: ManifestTransport::Stdio,
                ..
            }
        )
        && matches!(
            manifest.verification.kind,
            ManifestVerificationKind::AcpInitialize
        )
}

fn manifest_launch_matches_observation(manifest: &AdapterManifest, executable: &Path) -> bool {
    let ManifestLaunch::Direct { executable_ref, .. } = &manifest.launch else {
        return false;
    };
    if executable_ref == "matched-observation" {
        return true;
    }
    executable
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.eq_ignore_ascii_case(executable_ref))
}

fn canonical_observed_executable(path: &Path) -> Option<PathBuf> {
    if !path.is_absolute() || has_reparse_point(path) {
        return None;
    }
    let canonical = path.canonicalize().ok()?;
    (!has_reparse_point(&canonical)
        && is_real_regular_file(&canonical)
        && is_windows_executable_file(&canonical))
    .then_some(canonical)
}

/// Post-verify identity recheck used by import-plan generation: re-canonicalizes
/// the executable and re-computes its fingerprint to prove it is still the same
/// bytes that were verified. This is not the spawn boundary (that path uses
/// `open_verified_executable_guard`), so it may re-open by path.
fn identity_is_current(
    classification: &AcpClassification,
    deadline: Instant,
    cancelled: &AtomicBool,
) -> bool {
    canonical_observed_executable(&classification.executable)
        .is_some_and(|current| current == classification.executable)
        && stable_file_fingerprint_with_deadline(&classification.executable, deadline, cancelled)
            .is_ok_and(|fingerprint| {
                fingerprint.stable_identity == classification.executable_identity
                    && fingerprint.content_sha256 == classification.executable_sha256
            })
}

fn verify_in_owned_process(
    classification: &AcpClassification,
    _guard: &VerifiedExecutableGuard,
    current_dir: &Path,
    deadline: Instant,
    cancelled: &AtomicBool,
) -> Result<AcpInitializeResponse, AcpVerificationDiagnosticCode> {
    let ManifestLaunch::Direct {
        args,
        environment_allowlist,
        credential_environment,
        ..
    } = &classification.manifest.launch
    else {
        return Err(AcpVerificationDiagnosticCode::LaunchFailed);
    };
    // The verified-executable guard stays alive for the entire duration of
    // this function (it is borrowed from the caller), so its delete/write
    // denying handle keeps the executable non-replaceable through
    // `spawn_direct`'s CreateProcessW. `spawn_direct` opens the same canonical
    // path the guard fingerprinted.
    let mut child = ManagedChild::spawn_direct(&ManagedDirectStdioSpec {
        executable: classification.executable.clone(),
        args: args.clone(),
        current_dir: current_dir.to_owned(),
        environment_allowlist: environment_allowlist.clone(),
        credential_environment: credential_environment.clone(),
    })
    .map_err(|_| AcpVerificationDiagnosticCode::LaunchFailed)?;
    let stdin = child.take_stdin();
    let stdout = child.take_stdout();
    let stderr = child.take_stderr();
    let request = initialize_request()?;
    let (frame_sender, frame_receiver) = mpsc::sync_channel(1);
    thread::scope(|scope| {
        let writer = scope.spawn(move || {
            let mut stdin = stdin;
            stdin
                .write_all(&request)
                .and_then(|_| stdin.flush())
                .map_err(|_| ())
        });
        let stdout_reader = scope.spawn(move || {
            read_first_stdout_frame_and_drain(stdout, MAX_ACP_RESPONSE_BYTES, frame_sender)
        });
        let stderr_reader = scope.spawn(move || read_bounded_stream(stderr, MAX_ACP_STDERR_BYTES));

        let mut outcome = None;
        while outcome.is_none() {
            if cancelled.load(Ordering::Acquire) {
                outcome = Some(Err(AcpVerificationDiagnosticCode::Cancelled));
                break;
            }
            if Instant::now() >= deadline {
                outcome = Some(Err(AcpVerificationDiagnosticCode::Timeout));
                break;
            }
            match frame_receiver.try_recv() {
                Ok(Ok(frame)) => {
                    outcome = Some(parse_initialize_response(&frame).and_then(|response| {
                        observe_response_exit(&mut child, deadline, response)
                    }));
                    break;
                }
                Ok(Err(error)) => {
                    outcome = Some(Err(classify_stdout_error(&mut child, deadline, error)));
                    break;
                }
                Err(TryRecvError::Empty) => {}
                Err(TryRecvError::Disconnected) => {
                    outcome = Some(Err(AcpVerificationDiagnosticCode::ProtocolViolation));
                    break;
                }
            }
            match child.try_wait() {
                Ok(Some(status)) if !status.success() => {
                    outcome = Some(Err(AcpVerificationDiagnosticCode::ProcessFailed));
                }
                Ok(Some(_)) | Ok(None) => thread::sleep(Duration::from_millis(5)),
                Err(_) => {
                    outcome = Some(Err(AcpVerificationDiagnosticCode::ProcessFailed));
                }
            }
        }

        let cleanup_deadline = cleanup_deadline(deadline);
        let cleanup_succeeded = child.terminate(cleanup_deadline);
        let writer = writer
            .join()
            .map_err(|_| AcpVerificationDiagnosticCode::ProcessFailed)?;
        let stdout = stdout_reader
            .join()
            .map_err(|_| AcpVerificationDiagnosticCode::ProcessFailed)?;
        let stderr = stderr_reader
            .join()
            .map_err(|_| AcpVerificationDiagnosticCode::ProcessFailed)?;

        let outcome = outcome.expect("ACP verifier loop produces an outcome");
        outcome.as_ref().map_err(|error| *error)?;
        if !cleanup_succeeded {
            return Err(AcpVerificationDiagnosticCode::CleanupFailed);
        }
        if writer.is_err() {
            return Err(AcpVerificationDiagnosticCode::ProcessFailed);
        }
        let stdout = stdout.map_err(StdoutFrameError::diagnostic)?;
        if stdout.had_trailing_bytes {
            return Err(AcpVerificationDiagnosticCode::ProtocolViolation);
        }
        let stderr = stderr.map_err(|error| match error {
            BoundedStreamError::Io => AcpVerificationDiagnosticCode::ProcessFailed,
            BoundedStreamError::Oversized => AcpVerificationDiagnosticCode::StderrOutput,
        })?;
        if !stderr.iter().all(u8::is_ascii_whitespace) {
            return Err(AcpVerificationDiagnosticCode::StderrOutput);
        }
        outcome
    })
}

fn observe_response_exit(
    child: &mut ManagedChild,
    deadline: Instant,
    response: AcpInitializeResponse,
) -> Result<AcpInitializeResponse, AcpVerificationDiagnosticCode> {
    let observation_deadline = deadline.min(Instant::now() + ACP_RESPONSE_EXIT_OBSERVATION_GRACE);
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                return status
                    .success()
                    .then_some(response)
                    .ok_or(AcpVerificationDiagnosticCode::ProcessFailed);
            }
            Ok(None) if Instant::now() >= observation_deadline => return Ok(response),
            Ok(None) => thread::sleep(Duration::from_millis(2)),
            Err(_) => return Err(AcpVerificationDiagnosticCode::ProcessFailed),
        }
    }
}

fn classify_stdout_error(
    child: &mut ManagedChild,
    deadline: Instant,
    error: StdoutFrameError,
) -> AcpVerificationDiagnosticCode {
    let observation_deadline = deadline.min(Instant::now() + ACP_RESPONSE_EXIT_OBSERVATION_GRACE);
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                return if status.success() {
                    error.diagnostic()
                } else {
                    AcpVerificationDiagnosticCode::ProcessFailed
                };
            }
            Ok(None) if Instant::now() >= observation_deadline => return error.diagnostic(),
            Ok(None) => thread::sleep(Duration::from_millis(2)),
            Err(_) => return AcpVerificationDiagnosticCode::ProcessFailed,
        }
    }
}

fn initialize_request() -> Result<Vec<u8>, AcpVerificationDiagnosticCode> {
    let request = json!({
        "jsonrpc": "2.0",
        "id": 0,
            "method": "initialize",
            "params": {
                "protocolVersion": ACP_PROTOCOL_MAJOR,
                "clientCapabilities": {
                    "fs": {
                        "readTextFile": false,
                        "writeTextFile": false
                    },
                    "terminal": false
                },
            "clientInfo": {
                "name": "agenttalk",
                "title": "AgentTalk",
                "version": env!("CARGO_PKG_VERSION")
            }
        }
    });
    let mut bytes = serde_json::to_vec(&request)
        .map_err(|_| AcpVerificationDiagnosticCode::ProtocolViolation)?;
    bytes.push(b'\n');
    (bytes.len() <= MAX_ACP_REQUEST_BYTES)
        .then_some(bytes)
        .ok_or(AcpVerificationDiagnosticCode::ProtocolViolation)
}

fn cleanup_deadline(deadline: Instant) -> Instant {
    deadline.max(Instant::now()) + ACP_CLEANUP_GRACE
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BoundedStreamError {
    Io,
    Oversized,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StdoutFrameError {
    Missing,
    Io,
    Oversized,
}

impl StdoutFrameError {
    fn diagnostic(self) -> AcpVerificationDiagnosticCode {
        match self {
            Self::Missing => AcpVerificationDiagnosticCode::ProtocolViolation,
            Self::Io => AcpVerificationDiagnosticCode::ProcessFailed,
            Self::Oversized => AcpVerificationDiagnosticCode::OversizedFrame,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct StdoutDrain {
    had_trailing_bytes: bool,
}

fn read_first_stdout_frame_and_drain<R: Read>(
    mut reader: R,
    limit: usize,
    sender: mpsc::SyncSender<Result<Vec<u8>, StdoutFrameError>>,
) -> Result<StdoutDrain, StdoutFrameError> {
    let mut frame = Vec::new();
    let mut buffer = [0u8; 8192];
    let mut sent_frame = false;
    let mut had_trailing_bytes = false;
    let mut trailing_bytes = 0usize;
    loop {
        let read = reader.read(&mut buffer).map_err(|_| StdoutFrameError::Io)?;
        if read == 0 {
            if !sent_frame {
                let _ = sender.send(Err(StdoutFrameError::Missing));
                return Err(StdoutFrameError::Missing);
            }
            return Ok(StdoutDrain { had_trailing_bytes });
        }
        if frame.len().saturating_add(read) > limit {
            let _ = sender.send(Err(StdoutFrameError::Oversized));
            return Err(StdoutFrameError::Oversized);
        }
        let bytes = &buffer[..read];
        if sent_frame {
            trailing_bytes = trailing_bytes.saturating_add(read);
            if trailing_bytes > limit {
                return Err(StdoutFrameError::Oversized);
            }
            had_trailing_bytes = true;
            continue;
        }
        let newline = bytes.iter().position(|byte| *byte == b'\n');
        let frame_bytes = newline.map_or(bytes, |index| &bytes[..index]);
        if frame.len().saturating_add(frame_bytes.len()) > limit {
            let _ = sender.send(Err(StdoutFrameError::Oversized));
            return Err(StdoutFrameError::Oversized);
        }
        frame.extend_from_slice(frame_bytes);
        let Some(newline) = newline else {
            continue;
        };
        let completed_frame = std::mem::take(&mut frame);
        let mut completed_frame = completed_frame;
        completed_frame.push(b'\n');
        if sender.send(Ok(completed_frame)).is_err() {
            return Err(StdoutFrameError::Io);
        }
        sent_frame = true;
        let remainder = bytes.len().saturating_sub(newline + 1);
        trailing_bytes = trailing_bytes.saturating_add(remainder);
        if trailing_bytes > limit {
            return Err(StdoutFrameError::Oversized);
        }
        had_trailing_bytes = remainder != 0;
    }
}

fn read_bounded_stream<R: Read>(
    mut reader: R,
    limit: usize,
) -> Result<Vec<u8>, BoundedStreamError> {
    let mut output = Vec::new();
    let mut buffer = [0u8; 8192];
    loop {
        let read = reader
            .read(&mut buffer)
            .map_err(|_| BoundedStreamError::Io)?;
        if read == 0 {
            return Ok(output);
        }
        if output.len().saturating_add(read) > limit {
            return Err(BoundedStreamError::Oversized);
        }
        output.extend_from_slice(&buffer[..read]);
    }
}

fn parse_initialize_response(
    bytes: &[u8],
) -> Result<AcpInitializeResponse, AcpVerificationDiagnosticCode> {
    if bytes.is_empty() || !bytes.ends_with(b"\n") {
        return Err(AcpVerificationDiagnosticCode::ProtocolViolation);
    }
    let frame = &bytes[..bytes.len() - 1];
    if frame.is_empty() || frame.contains(&b'\n') || frame.contains(&b'\r') {
        return Err(AcpVerificationDiagnosticCode::ProtocolViolation);
    }
    let text =
        std::str::from_utf8(frame).map_err(|_| AcpVerificationDiagnosticCode::NonUtf8Frame)?;
    let response: AcpJsonRpcResponse =
        serde_json::from_str(text).map_err(|_| AcpVerificationDiagnosticCode::ProtocolViolation)?;
    if response.jsonrpc != "2.0" || response.id != 0 {
        return Err(AcpVerificationDiagnosticCode::ProtocolViolation);
    }
    response.result.validate()
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AcpJsonRpcResponse {
    #[serde(default, rename = "_meta")]
    _meta: Option<IgnoredAny>,
    jsonrpc: String,
    id: u64,
    result: AcpInitializeResponse,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AcpInitializeResponse {
    #[serde(default, rename = "_meta")]
    _meta: Option<IgnoredAny>,
    protocol_version: u16,
    #[serde(default)]
    agent_capabilities: AcpAgentCapabilities,
    agent_info: Option<AcpWireAgentInfo>,
    #[serde(default)]
    auth_methods: Vec<AcpWireAuthMethod>,
}

impl AcpInitializeResponse {
    fn validate(self) -> Result<Self, AcpVerificationDiagnosticCode> {
        if self.protocol_version == 0 {
            return Err(AcpVerificationDiagnosticCode::ProtocolViolation);
        }
        if self.auth_methods.len() > 16
            || self
                .auth_methods
                .iter()
                .any(|method| !method.is_safe_and_supported())
        {
            return Err(AcpVerificationDiagnosticCode::ProtocolViolation);
        }
        if self.agent_info.as_ref().is_some_and(|info| !info.is_safe()) {
            return Err(AcpVerificationDiagnosticCode::ProtocolViolation);
        }
        Ok(self)
    }

    fn safe_agent_info(&self) -> Option<AcpAgentInfo> {
        self.agent_info.as_ref().map(|info| AcpAgentInfo {
            name: info.name.clone(),
            title: info.title.clone(),
            version: info.version.clone(),
        })
    }

    fn capability_summary(&self) -> AcpCapabilitySummary {
        AcpCapabilitySummary {
            load_session: self.agent_capabilities.load_session,
            prompt_image: self.agent_capabilities.prompt_capabilities.image,
            prompt_audio: self.agent_capabilities.prompt_capabilities.audio,
            prompt_embedded_context: self.agent_capabilities.prompt_capabilities.embedded_context,
            mcp_http: self.agent_capabilities.mcp_capabilities.http,
            mcp_sse: self.agent_capabilities.mcp_capabilities.sse,
            supports_logout: self
                .agent_capabilities
                .auth
                .as_ref()
                .is_some_and(|auth| auth.logout.is_some()),
        }
    }
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AcpAgentCapabilities {
    #[serde(default, rename = "_meta")]
    _meta: Option<IgnoredAny>,
    #[serde(default)]
    load_session: bool,
    #[serde(default)]
    prompt_capabilities: AcpPromptCapabilities,
    #[serde(default)]
    mcp_capabilities: AcpMcpCapabilities,
    #[serde(default, rename = "sessionCapabilities")]
    _session_capabilities: AcpSessionCapabilities,
    #[serde(default)]
    auth: Option<AcpAuthCapabilities>,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AcpPromptCapabilities {
    #[serde(default, rename = "_meta")]
    _meta: Option<IgnoredAny>,
    #[serde(default)]
    image: bool,
    #[serde(default)]
    audio: bool,
    #[serde(default)]
    embedded_context: bool,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AcpMcpCapabilities {
    #[serde(default, rename = "_meta")]
    _meta: Option<IgnoredAny>,
    #[serde(default)]
    http: bool,
    #[serde(default)]
    sse: bool,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AcpSessionCapabilities {
    #[serde(default, rename = "_meta")]
    _meta: Option<IgnoredAny>,
    #[serde(default, rename = "additionalDirectories")]
    _additional_directories: Option<AcpEmptyObject>,
    #[serde(default, rename = "close")]
    _close: Option<AcpEmptyObject>,
    #[serde(default, rename = "delete")]
    _delete: Option<AcpEmptyObject>,
    #[serde(default, rename = "list")]
    _list: Option<AcpEmptyObject>,
    #[serde(default, rename = "resume")]
    _resume: Option<AcpEmptyObject>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AcpAuthCapabilities {
    #[serde(default, rename = "_meta")]
    _meta: Option<IgnoredAny>,
    #[serde(default)]
    logout: Option<AcpEmptyObject>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AcpEmptyObject {
    #[serde(default, rename = "_meta")]
    _meta: Option<IgnoredAny>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AcpWireAgentInfo {
    #[serde(default, rename = "_meta")]
    _meta: Option<IgnoredAny>,
    name: String,
    #[serde(default)]
    title: Option<String>,
    version: String,
}

impl AcpWireAgentInfo {
    fn is_safe(&self) -> bool {
        safe_protocol_text(&self.name, 128)
            && self
                .title
                .as_deref()
                .is_none_or(|title| safe_protocol_text(title, 160))
            && safe_protocol_text(&self.version, 128)
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AcpWireAuthMethod {
    #[serde(default, rename = "_meta")]
    _meta: Option<IgnoredAny>,
    id: String,
    name: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(default, rename = "type")]
    kind: Option<AcpAuthMethodKind>,
}

impl AcpWireAuthMethod {
    fn is_safe_and_supported(&self) -> bool {
        self.kind
            .is_none_or(|kind| kind == AcpAuthMethodKind::Agent)
            && safe_protocol_text(&self.id, 128)
            && safe_protocol_text(&self.name, 160)
            && self
                .description
                .as_deref()
                .is_none_or(|description| safe_protocol_text(description, 256))
    }
}

#[derive(Clone, Copy, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum AcpAuthMethodKind {
    Agent,
}

fn safe_protocol_text(value: &str, max_len: usize) -> bool {
    let value = value.trim();
    !value.is_empty()
        && value.len() <= max_len
        && !looks_like_private_text(value)
        && value.chars().all(|ch| {
            !ch.is_control()
                && !matches!(
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
        })
}

fn looks_like_private_text(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    value.contains('\\')
        || value.contains("://")
        || lower.contains("authorization")
        || lower.contains("cookie")
        || lower.contains("bearer ")
        || lower.contains("token=")
        || lower.contains("api_key")
        || lower.contains("apikey")
        || lower.contains("secret=")
}

fn accepted(
    classification: &AcpClassification,
    wire: AcpInitializeResponse,
) -> AcpVerificationResult {
    let protocol_matches = wire.protocol_version == classification.manifest.protocol.major;
    if !protocol_matches {
        return rejected(
            classification,
            AcpVerificationDiagnosticCode::ProtocolMismatch,
            Some(wire.protocol_version),
        );
    }
    let auth_required = !wire.auth_methods.is_empty();
    AcpVerificationResult {
        report: AcpCompatibilityReport {
            candidate_id: classification.candidate_id().to_owned(),
            status: if auth_required {
                AcpVerificationStatus::AuthRequired
            } else {
                AcpVerificationStatus::Verified
            },
            compatibility_state: CompatibilityState::Compatible,
            auth_state: if auth_required {
                AuthState::Required
            } else {
                AuthState::NotRequired
            },
            requires_configuration: auth_required,
            protocol_major: Some(wire.protocol_version),
            agent_info: wire.safe_agent_info(),
            capabilities: wire.capability_summary(),
            diagnostic: auth_required
                .then_some(AcpVerificationDiagnosticCode::AuthenticationRequired),
        },
        binding: (!auth_required).then(|| classification.binding.clone()),
    }
}

fn rejected(
    classification: &AcpClassification,
    diagnostic: AcpVerificationDiagnosticCode,
    protocol_major: Option<u16>,
) -> AcpVerificationResult {
    AcpVerificationResult {
        report: AcpCompatibilityReport {
            candidate_id: classification.candidate_id().to_owned(),
            status: AcpVerificationStatus::Rejected,
            compatibility_state: CompatibilityState::Incompatible,
            auth_state: AuthState::Unknown,
            requires_configuration: true,
            protocol_major,
            agent_info: None,
            capabilities: AcpCapabilitySummary::default(),
            diagnostic: Some(diagnostic),
        },
        binding: None,
    }
}

fn target_binding_digest(
    candidate_id: &str,
    manifest_id: &str,
    executable_identity: &str,
    executable_sha256: &str,
) -> String {
    let mut hasher = Sha256::new();
    for value in [
        candidate_id,
        manifest_id,
        executable_identity,
        executable_sha256,
    ] {
        hasher.update(value.as_bytes());
        hasher.update([0xff]);
    }
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
fn candidate_id_for_executable_identity(executable_identity: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"agenttalk-acp-passive-observation-v1");
    hasher.update([0xff]);
    hasher.update(executable_identity.as_bytes());
    format!(
        "candidate-{}",
        hasher
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    )
}

struct AcpVerificationCwd {
    path: Option<PathBuf>,
}

impl AcpVerificationCwd {
    fn create() -> Result<Self, ()> {
        static NEXT_NONCE: AtomicU64 = AtomicU64::new(0);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| ())?
            .as_nanos();
        for attempt in 0..64u64 {
            let nonce = NEXT_NONCE.fetch_add(1, Ordering::AcqRel);
            let path = std::env::temp_dir().join(format!(
                "agenttalk-w4-acp-cwd-{}-{now:x}-{nonce:x}-{attempt:x}",
                std::process::id()
            ));
            match fs::create_dir(&path) {
                Ok(()) => return Ok(Self { path: Some(path) }),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(_) => return Err(()),
            }
        }
        Err(())
    }

    fn path(&self) -> &Path {
        self.path.as_deref().expect("owned ACP cwd exists")
    }

    fn cleanup(&mut self) -> Result<(), ()> {
        let Some(path) = self.path.take() else {
            return Ok(());
        };
        fs::remove_dir_all(path).map_err(|_| ())
    }
}

impl Drop for AcpVerificationCwd {
    fn drop(&mut self) {
        if let Some(path) = self.path.take() {
            let _ = fs::remove_dir_all(path);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self, Read};
    use std::sync::mpsc;

    use super::{read_first_stdout_frame_and_drain, StdoutFrameError};

    struct ChunkedReader {
        chunks: Vec<Vec<u8>>,
        current: usize,
    }

    impl ChunkedReader {
        fn new(chunks: impl IntoIterator<Item = impl Into<Vec<u8>>>) -> Self {
            Self {
                chunks: chunks.into_iter().map(Into::into).collect(),
                current: 0,
            }
        }
    }

    impl Read for ChunkedReader {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            let Some(chunk) = self.chunks.get(self.current) else {
                return Ok(0);
            };
            assert!(
                chunk.len() <= buffer.len(),
                "fixture chunk must fit the verifier buffer"
            );
            buffer[..chunk.len()].copy_from_slice(chunk);
            self.current += 1;
            Ok(chunk.len())
        }
    }

    #[test]
    fn stdout_drain_is_bounded_after_valid_frame() {
        let (sender, receiver) = mpsc::sync_channel(1);
        let result = read_first_stdout_frame_and_drain(
            ChunkedReader::new([
                b"{}\n".as_slice(),
                b"1234567".as_slice(),
                b"abcdefg".as_slice(),
            ]),
            8,
            sender,
        );

        assert_eq!(
            receiver.recv().expect("first frame is forwarded"),
            Ok(b"{}\n".to_vec())
        );
        assert_eq!(result, Err(StdoutFrameError::Oversized));
    }
}
