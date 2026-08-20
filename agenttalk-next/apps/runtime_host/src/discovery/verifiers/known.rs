use std::collections::BTreeMap;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use agenttalk_domain::{
    AuthState, CandidateCategory, CandidateProjection, CompatibilityState, ObservationSourceKind,
};
use sha2::{Digest, Sha256};

use super::super::{ManagedChild, ManagedDirectStdioSpec, Observation};
use super::acp::{
    AcpAgentInfo, AcpCapabilitySummary, AcpCompatibilityReport, AcpImportPlanMetadata,
    AcpVerificationDiagnosticCode, AcpVerificationStatus,
};
use crate::{
    has_reparse_point, is_real_regular_file, is_windows_executable_file,
    open_verified_executable_guard, stable_file_fingerprint_with_deadline,
};

const CODEX_CONNECTOR_ID: &str = "local.codex";
const CODEX_RUNTIME_TYPE: &str = "codex";
const CODEX_PROTOCOL_MAJOR: u16 = 1;
const CODEX_MANIFEST_ID: &str = "builtin.codex-app-server";
const CODEX_MANIFEST_BYTES: &[u8] = b"agenttalk.builtin.codex-app-server.v1";
const MAX_PROBE_OUTPUT_BYTES: usize = 64 * 1024;
const PROBE_CLEANUP_GRACE: Duration = Duration::from_millis(500);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KnownConnectorClassificationError {
    ObservationMismatch,
    UnsafeExecutable,
    FingerprintUnavailable,
}

#[derive(Clone)]
struct KnownConnectorClassification {
    candidate_id: String,
    executable: PathBuf,
    executable_identity: String,
    executable_sha256: String,
    binding_digest: String,
}

#[derive(Clone, Default)]
pub struct KnownConnectorDiscoverySession {
    classifications: BTreeMap<String, KnownConnectorClassification>,
}

impl std::fmt::Debug for KnownConnectorDiscoverySession {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("KnownConnectorDiscoverySession")
            .field("candidate_count", &self.classifications.len())
            .finish()
    }
}

#[derive(Clone)]
pub struct KnownConnectorVerificationResult {
    report: AcpCompatibilityReport,
    binding_digest: String,
}

impl std::fmt::Debug for KnownConnectorVerificationResult {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("KnownConnectorVerificationResult")
            .field("report", &self.report)
            .finish()
    }
}

impl KnownConnectorVerificationResult {
    pub fn report(&self) -> &AcpCompatibilityReport {
        &self.report
    }
}

impl KnownConnectorDiscoverySession {
    pub(crate) fn classify(
        observations: &BTreeMap<String, Vec<Observation>>,
        projections: &[CandidateProjection],
        deadline: Instant,
        cancelled: &AtomicBool,
    ) -> Self {
        let mut classifications = BTreeMap::new();
        for projection in projections {
            if cancelled.load(Ordering::Acquire) || Instant::now() >= deadline {
                break;
            }
            if projection.connector_id != CODEX_CONNECTOR_ID
                || projection.runtime_type != CODEX_RUNTIME_TYPE
                || projection.category != CandidateCategory::AgentRuntime
                || projection.source_kind != ObservationSourceKind::ExecutableInventory
            {
                continue;
            }
            let Some(candidate_observations) = observations.get(&projection.candidate_id) else {
                continue;
            };
            let Some(observation) = candidate_observations.iter().find(|observation| {
                observation.connector_id == CODEX_CONNECTOR_ID
                    && observation.runtime_type == CODEX_RUNTIME_TYPE
                    && observation.source_kind == ObservationSourceKind::ExecutableInventory
            }) else {
                continue;
            };
            if let Ok(classification) =
                classify_codex(&projection.candidate_id, observation, deadline, cancelled)
            {
                classifications.insert(projection.candidate_id.clone(), classification);
            }
        }
        Self { classifications }
    }

    pub fn contains(&self, candidate_id: &str) -> bool {
        self.classifications.contains_key(candidate_id)
    }

    pub fn verify(
        &self,
        candidate_id: &str,
        deadline: Instant,
        cancelled: &AtomicBool,
    ) -> Result<KnownConnectorVerificationResult, KnownConnectorClassificationError> {
        let classification = self
            .classifications
            .get(candidate_id)
            .ok_or(KnownConnectorClassificationError::ObservationMismatch)?;
        Ok(verify_codex(classification, deadline, cancelled))
    }

    pub fn import_plan_metadata(
        &self,
        candidate_id: &str,
        verification: &KnownConnectorVerificationResult,
        deadline: Instant,
        cancelled: &AtomicBool,
    ) -> Result<AcpImportPlanMetadata, KnownConnectorClassificationError> {
        let classification = self
            .classifications
            .get(candidate_id)
            .ok_or(KnownConnectorClassificationError::ObservationMismatch)?;
        if verification.binding_digest != classification.binding_digest
            || !matches!(
                verification.report.status,
                AcpVerificationStatus::Verified | AcpVerificationStatus::AuthRequired
            )
            || !identity_is_current(classification, deadline, cancelled)
        {
            return Err(KnownConnectorClassificationError::ObservationMismatch);
        }
        Ok(AcpImportPlanMetadata {
            candidate_id: classification.candidate_id.clone(),
            adapter_kind: CODEX_RUNTIME_TYPE.into(),
            protocol_major: CODEX_PROTOCOL_MAJOR,
            manifest_id: CODEX_MANIFEST_ID.into(),
            manifest_sha256: sha256_hex(CODEX_MANIFEST_BYTES),
            candidate_binding_digest: classification.binding_digest.clone(),
            auth_required: true,
            capabilities: codex_capabilities(),
        })
    }
}

fn classify_codex(
    candidate_id: &str,
    observation: &Observation,
    deadline: Instant,
    cancelled: &AtomicBool,
) -> Result<KnownConnectorClassification, KnownConnectorClassificationError> {
    let observed = observation
        .executable_locator()
        .ok_or(KnownConnectorClassificationError::ObservationMismatch)?;
    let executable = canonical_known_executable(observed)
        .ok_or(KnownConnectorClassificationError::UnsafeExecutable)?;
    let fingerprint = stable_file_fingerprint_with_deadline(&executable, deadline, cancelled)
        .map_err(|_| KnownConnectorClassificationError::FingerprintUnavailable)?;
    let binding_digest = known_binding_digest(
        candidate_id,
        &fingerprint.stable_identity,
        &fingerprint.content_sha256,
    );
    Ok(KnownConnectorClassification {
        candidate_id: candidate_id.into(),
        executable,
        executable_identity: fingerprint.stable_identity,
        executable_sha256: fingerprint.content_sha256,
        binding_digest,
    })
}

fn verify_codex(
    classification: &KnownConnectorClassification,
    deadline: Instant,
    cancelled: &AtomicBool,
) -> KnownConnectorVerificationResult {
    let rejected = |diagnostic| KnownConnectorVerificationResult {
        report: AcpCompatibilityReport {
            candidate_id: classification.candidate_id.clone(),
            status: AcpVerificationStatus::Rejected,
            compatibility_state: CompatibilityState::Incompatible,
            auth_state: AuthState::Unknown,
            requires_configuration: true,
            protocol_major: None,
            agent_info: None,
            capabilities: AcpCapabilitySummary::default(),
            diagnostic: Some(diagnostic),
        },
        binding_digest: classification.binding_digest.clone(),
    };
    if cancelled.load(Ordering::Acquire) {
        return rejected(AcpVerificationDiagnosticCode::Cancelled);
    }
    let guard =
        match open_verified_executable_guard(&classification.executable, deadline, cancelled) {
            Ok(guard)
                if guard.fingerprint.stable_identity == classification.executable_identity
                    && guard.fingerprint.content_sha256 == classification.executable_sha256 =>
            {
                guard
            }
            _ => return rejected(AcpVerificationDiagnosticCode::IdentityMismatch),
        };
    let mut cwd = match KnownVerificationCwd::create() {
        Ok(cwd) => cwd,
        Err(()) => return rejected(AcpVerificationDiagnosticCode::LaunchFailed),
    };
    let version = run_probe(
        &classification.executable,
        &["--version"],
        cwd.path(),
        deadline,
        cancelled,
    )
    .and_then(|output| parse_codex_version(&output));
    let help = version.and_then(|version| {
        run_probe(
            &classification.executable,
            &["app-server", "--help"],
            cwd.path(),
            deadline,
            cancelled,
        )
        .and_then(|output| {
            output_contains_ascii(&output, "app-server")
                .then_some(version)
                .ok_or(AcpVerificationDiagnosticCode::ProtocolMismatch)
        })
    });
    drop(guard);
    if cwd.cleanup().is_err() {
        return rejected(AcpVerificationDiagnosticCode::CleanupFailed);
    }
    let version = match help {
        Ok(version) => version,
        Err(diagnostic) => return rejected(diagnostic),
    };
    KnownConnectorVerificationResult {
        report: AcpCompatibilityReport {
            candidate_id: classification.candidate_id.clone(),
            // The local app-server surface is present, but verification does
            // not read Codex account state or call a model provider.
            status: AcpVerificationStatus::AuthRequired,
            compatibility_state: CompatibilityState::Compatible,
            auth_state: AuthState::Required,
            requires_configuration: true,
            protocol_major: Some(CODEX_PROTOCOL_MAJOR),
            agent_info: Some(AcpAgentInfo {
                name: "Codex".into(),
                title: Some("Codex local app-server".into()),
                version,
            }),
            capabilities: codex_capabilities(),
            diagnostic: Some(AcpVerificationDiagnosticCode::AuthenticationRequired),
        },
        binding_digest: classification.binding_digest.clone(),
    }
}

fn identity_is_current(
    classification: &KnownConnectorClassification,
    deadline: Instant,
    cancelled: &AtomicBool,
) -> bool {
    canonical_known_executable(&classification.executable)
        .is_some_and(|current| current == classification.executable)
        && stable_file_fingerprint_with_deadline(&classification.executable, deadline, cancelled)
            .is_ok_and(|fingerprint| {
                fingerprint.stable_identity == classification.executable_identity
                    && fingerprint.content_sha256 == classification.executable_sha256
            })
}

fn canonical_known_executable(path: &Path) -> Option<PathBuf> {
    if !path.is_absolute() || has_reparse_point(path) {
        return None;
    }
    let canonical = path.canonicalize().ok()?;
    (!has_reparse_point(&canonical)
        && is_real_regular_file(&canonical)
        && is_windows_executable_file(&canonical))
    .then_some(canonical)
}

fn run_probe(
    executable: &Path,
    args: &[&str],
    current_dir: &Path,
    deadline: Instant,
    cancelled: &AtomicBool,
) -> Result<Vec<u8>, AcpVerificationDiagnosticCode> {
    if Instant::now() >= deadline {
        return Err(AcpVerificationDiagnosticCode::Timeout);
    }
    let mut child = ManagedChild::spawn_direct(&ManagedDirectStdioSpec {
        executable: executable.to_owned(),
        args: args.iter().map(|arg| (*arg).into()).collect(),
        current_dir: current_dir.to_owned(),
        environment_allowlist: vec!["LOCALAPPDATA".into(), "PATH".into(), "USERPROFILE".into()],
        credential_environment: Vec::new(),
    })
    .map_err(|_| AcpVerificationDiagnosticCode::LaunchFailed)?;
    drop(child.take_stdin());
    let stdout = child.take_stdout();
    let stderr = child.take_stderr();
    let stdout_reader = thread::spawn(move || read_bounded(stdout));
    let stderr_reader = thread::spawn(move || read_bounded(stderr));
    let status = loop {
        if cancelled.load(Ordering::Acquire) {
            let _ = child.terminate(Instant::now() + PROBE_CLEANUP_GRACE);
            break Err(AcpVerificationDiagnosticCode::Cancelled);
        }
        if Instant::now() >= deadline {
            let _ = child.terminate(Instant::now() + PROBE_CLEANUP_GRACE);
            break Err(AcpVerificationDiagnosticCode::Timeout);
        }
        match child.try_wait() {
            Ok(Some(status)) => break Ok(status),
            Ok(None) => thread::sleep(Duration::from_millis(5)),
            Err(()) => {
                let _ = child.terminate(Instant::now() + PROBE_CLEANUP_GRACE);
                break Err(AcpVerificationDiagnosticCode::ProcessFailed);
            }
        }
    };
    let stdout = stdout_reader
        .join()
        .map_err(|_| AcpVerificationDiagnosticCode::ProcessFailed)?
        .map_err(|_| AcpVerificationDiagnosticCode::OversizedFrame)?;
    let stderr = stderr_reader
        .join()
        .map_err(|_| AcpVerificationDiagnosticCode::ProcessFailed)?
        .map_err(|_| AcpVerificationDiagnosticCode::OversizedFrame)?;
    let status = status?;
    if !status.success() {
        return Err(AcpVerificationDiagnosticCode::ProcessFailed);
    }
    let mut output = stdout;
    if !stderr.is_empty() {
        output.push(b'\n');
        output.extend(stderr);
    }
    Ok(output)
}

fn read_bounded(mut reader: impl Read) -> Result<Vec<u8>, ()> {
    let mut output = Vec::new();
    let mut buffer = [0u8; 8192];
    loop {
        let read = reader.read(&mut buffer).map_err(|_| ())?;
        if read == 0 {
            return Ok(output);
        }
        if output.len().saturating_add(read) > MAX_PROBE_OUTPUT_BYTES {
            return Err(());
        }
        output.extend_from_slice(&buffer[..read]);
    }
}

fn parse_codex_version(output: &[u8]) -> Result<String, AcpVerificationDiagnosticCode> {
    let text =
        std::str::from_utf8(output).map_err(|_| AcpVerificationDiagnosticCode::NonUtf8Frame)?;
    let line = text
        .lines()
        .find(|line| !line.trim().is_empty())
        .unwrap_or("");
    if !line.to_ascii_lowercase().contains("codex") {
        return Err(AcpVerificationDiagnosticCode::ProtocolMismatch);
    }
    line.split_whitespace()
        .rev()
        .find(|part| {
            !part.is_empty()
                && part.len() <= 64
                && part
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b".-+_".contains(&byte))
        })
        .map(str::to_owned)
        .ok_or(AcpVerificationDiagnosticCode::ProtocolMismatch)
}

fn output_contains_ascii(output: &[u8], expected: &str) -> bool {
    std::str::from_utf8(output)
        .ok()
        .is_some_and(|text| text.to_ascii_lowercase().contains(expected))
}

fn codex_capabilities() -> AcpCapabilitySummary {
    AcpCapabilitySummary {
        load_session: true,
        ..AcpCapabilitySummary::default()
    }
}

fn known_binding_digest(candidate_id: &str, identity: &str, sha256: &str) -> String {
    let mut hasher = Sha256::new();
    for part in [
        candidate_id,
        CODEX_MANIFEST_ID,
        identity,
        sha256,
        std::str::from_utf8(CODEX_MANIFEST_BYTES).unwrap_or_default(),
    ] {
        hasher.update(part.as_bytes());
        hasher.update([0xff]);
    }
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

struct KnownVerificationCwd {
    path: PathBuf,
    cleaned: bool,
}

impl KnownVerificationCwd {
    fn create() -> Result<Self, ()> {
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        for _ in 0..8 {
            let id = NEXT_ID.fetch_add(1, Ordering::AcqRel);
            let path = std::env::temp_dir().join(format!(
                "agenttalk-known-verify-{}-{timestamp}-{id}",
                std::process::id()
            ));
            match fs::create_dir(&path) {
                Ok(()) => {
                    return Ok(Self {
                        path,
                        cleaned: false,
                    });
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(_) => return Err(()),
            }
        }
        Err(())
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn cleanup(&mut self) -> Result<(), ()> {
        if self.cleaned {
            return Ok(());
        }
        fs::remove_dir(&self.path).map_err(|_| ())?;
        self.cleaned = true;
        Ok(())
    }
}

impl Drop for KnownVerificationCwd {
    fn drop(&mut self) {
        if !self.cleaned {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

#[cfg(all(test, windows))]
mod tests {
    use super::*;
    use crate::{discover_local_connectors_report_with_config, LocalConnectorDiscoveryConfig};
    use std::process::Command;

    #[test]
    fn codex_known_connector_verifies_only_local_cli_surface_and_rechecks_import_identity() {
        let fixture = compile_fixture();
        let config = LocalConnectorDiscoveryConfig {
            codex_binary_paths: vec![fixture.executable.clone()],
            kun_data_dirs: Vec::new(),
            kun_install_dirs: Vec::new(),
            kun_expected_service_version: "fixture".into(),
            request_timeout: Duration::from_secs(2),
        };
        let report = discover_local_connectors_report_with_config(&config);
        let projection = report
            .projections
            .iter()
            .find(|projection| projection.connector_id == CODEX_CONNECTOR_ID)
            .expect("fixture Codex is passively discovered");
        let cancelled = AtomicBool::new(false);
        let session =
            report.classify_known_connectors(Instant::now() + Duration::from_secs(2), &cancelled);
        assert!(session.contains(&projection.candidate_id));

        let verification = session
            .verify(
                &projection.candidate_id,
                Instant::now() + Duration::from_secs(2),
                &cancelled,
            )
            .expect("known binding remains present");
        assert_eq!(
            verification.report().status,
            AcpVerificationStatus::AuthRequired
        );
        assert_eq!(
            verification
                .report()
                .agent_info
                .as_ref()
                .map(|info| info.version.as_str()),
            Some("1.2.3")
        );
        let metadata = session
            .import_plan_metadata(
                &projection.candidate_id,
                &verification,
                Instant::now() + Duration::from_secs(2),
                &cancelled,
            )
            .expect("unchanged verified fixture can produce import metadata");
        assert_eq!(metadata.adapter_kind, CODEX_RUNTIME_TYPE);
        assert_eq!(metadata.manifest_id, CODEX_MANIFEST_ID);
        assert!(metadata.auth_required);

        fs::write(&fixture.executable, b"replaced after verification").unwrap();
        assert!(session
            .import_plan_metadata(
                &projection.candidate_id,
                &verification,
                Instant::now() + Duration::from_secs(2),
                &cancelled,
            )
            .is_err());
    }

    struct FixtureExecutable {
        root: PathBuf,
        executable: PathBuf,
    }

    impl Drop for FixtureExecutable {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    fn compile_fixture() -> FixtureExecutable {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "agenttalk-known-codex-fixture-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir_all(&root).unwrap();
        let source = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join("known_codex_fixture.rs");
        let executable = root.join("codex.exe");
        let rustc = std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into());
        let output = Command::new(rustc)
            .arg(&source)
            .arg("-O")
            .arg("-o")
            .arg(&executable)
            .output()
            .expect("compile known Codex fixture");
        assert!(
            output.status.success(),
            "fixture compile failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        FixtureExecutable { root, executable }
    }
}
