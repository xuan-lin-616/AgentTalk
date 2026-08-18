use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::MutexGuard;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::adapters::acp::AcpProtocolAdapterFactory;
use crate::{
    discover_windows_passive_report_with_config, AdapterManifest, ExplicitDiscoverySource,
    ManifestLaunch, RuntimeRequest, WindowsPassiveDiscoveryConfig, WorkspaceAccess,
};
use agenttalk_domain::{
    AuthState, CompatibilityState, DiscoveryState, ObservationSourceKind, ObservationTrustLevel,
};
use serde_json::json;
use sha2::{Digest, Sha256};

use super::acp::{
    AcpClassification, AcpClassificationError, AcpPassiveObservation, AcpVerificationConsent,
    AcpVerificationDiagnosticCode, AcpVerificationResult, AcpVerificationStatus,
};

const ACP_TIMEOUT: Duration = Duration::from_millis(600);

fn suite_guard() -> MutexGuard<'static, ()> {
    super::super::managed_process_fixture_guard_for_tests()
}

fn unique_temp_path(name: &str) -> PathBuf {
    static NEXT_NONCE: AtomicUsize = AtomicUsize::new(0);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos();
    let nonce = NEXT_NONCE.fetch_add(1, Ordering::AcqRel);
    std::env::temp_dir().join(format!(
        "agenttalk-w4-{name}-{}-{now:x}-{nonce:x}",
        std::process::id()
    ))
}

struct TestTempDir {
    path: PathBuf,
}

impl TestTempDir {
    fn create(name: &str) -> Self {
        let path = unique_temp_path(name);
        std::fs::create_dir(&path).expect("create owned W4 test directory");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

/// Transient Windows delete errors that are safe to retry: access denied (5),
/// sharing violation (32), or a plain PermissionDenied. These mean another
/// handle still references the directory tree; retrying after a brief backoff
/// lets the child process finish releasing it. Non-transient errors fail
/// immediately and are never swallowed.
fn is_transient_delete_error(error: &std::io::Error) -> bool {
    matches!(error.raw_os_error(), Some(5) | Some(32))
        || error.kind() == std::io::ErrorKind::PermissionDenied
}

/// Removes an owned test directory tree, retrying transient Windows delete
/// errors within a bounded monotonic deadline. `remover`, `now`, and `sleep`
/// are injectable so the retry policy is unit-testable without depending on a
/// real file-lock race. `path` no longer existing (or NotFound) is success.
fn remove_owned_test_dir_with_retry(
    path: &Path,
    deadline: Duration,
    backoff: Duration,
    remover: &mut dyn FnMut(&Path) -> std::io::Result<()>,
    now: &mut dyn FnMut() -> Instant,
    sleep: &mut dyn FnMut(Duration),
) -> std::io::Result<()> {
    let started = now();
    loop {
        if !path.exists() {
            return Ok(());
        }
        match remover(path) {
            Ok(()) => return Ok(()),
            Err(error) => {
                if !is_transient_delete_error(&error) || now() >= started + deadline {
                    return Err(error);
                }
                sleep(backoff);
            }
        }
    }
}

impl Drop for TestTempDir {
    fn drop(&mut self) {
        if let Err(error) = remove_owned_test_dir_with_retry(
            &self.path,
            Duration::from_secs(2),
            Duration::from_millis(50),
            &mut |path: &Path| std::fs::remove_dir_all(path),
            &mut Instant::now,
            &mut |delay: Duration| std::thread::sleep(delay),
        ) {
            let message = format!("remove owned W4 test directory failed: {error}");
            if std::thread::panicking() {
                eprintln!("{message}");
            } else {
                panic!("{message}");
            }
        }
    }
}

struct EnvVarGuard {
    key: &'static str,
    original: Option<OsString>,
}

fn access_denied_io_error() -> std::io::Error {
    std::io::Error::from_raw_os_error(5) // ERROR_ACCESS_DENIED
}

#[test]
fn cleanup_retries_transient_delete_then_succeeds() {
    let dir = TestTempDir::create("cleanup-retry-success");
    let path = dir.path().to_path_buf();
    std::mem::forget(dir); // exercise the helper directly, not the real Drop

    let attempts = AtomicUsize::new(0);
    let mut remover = |p: &Path| {
        if attempts.fetch_add(1, Ordering::SeqCst) < 2 {
            Err(access_denied_io_error())
        } else {
            std::fs::remove_dir_all(p)
        }
    };
    let elapsed = std::cell::Cell::new(Duration::ZERO);
    let mut now = || Instant::now() + elapsed.get();
    let mut sleep = |delay: Duration| elapsed.set(elapsed.get() + delay);

    let result = remove_owned_test_dir_with_retry(
        &path,
        Duration::from_secs(2),
        Duration::from_millis(50),
        &mut remover,
        &mut now,
        &mut sleep,
    );
    assert!(result.is_ok(), "transient retries must succeed: {result:?}");
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        3,
        "must retry the two denied attempts then succeed"
    );
    assert!(!path.exists(), "the directory must be removed");
}

#[test]
fn cleanup_persistent_access_denied_fails_at_bounded_deadline() {
    let dir = TestTempDir::create("cleanup-retry-deadline");
    let path = dir.path().to_path_buf();
    std::mem::forget(dir);

    let attempts = AtomicUsize::new(0);
    let mut remover = |_p: &Path| {
        attempts.fetch_add(1, Ordering::SeqCst);
        Err(access_denied_io_error())
    };
    let elapsed = std::cell::Cell::new(Duration::ZERO);
    let mut now = || Instant::now() + elapsed.get();
    let mut sleep = |delay: Duration| elapsed.set(elapsed.get() + delay);

    let result = remove_owned_test_dir_with_retry(
        &path,
        Duration::from_secs(2),
        Duration::from_millis(50),
        &mut remover,
        &mut now,
        &mut sleep,
    );
    assert!(
        result.is_err(),
        "persistent denial must fail, not fake success"
    );
    let attempts = attempts.load(Ordering::SeqCst);
    assert!(attempts > 1, "must retry at least once before the deadline");
    assert!(
        attempts <= 50,
        "must stop at the bounded deadline, got {attempts} attempts"
    );
    // Leave the directory for the test's own cleanup; this path is a manual
    // helper exercise, not the real Drop, so remove it now.
    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn cleanup_missing_directory_is_success() {
    let path = unique_temp_path("cleanup-missing");
    let mut remover = |_p: &Path| panic!("remover must not run for a missing path");
    let mut now = Instant::now;
    let mut sleep = |_delay: Duration| {};
    let result = remove_owned_test_dir_with_retry(
        &path,
        Duration::from_secs(2),
        Duration::from_millis(50),
        &mut remover,
        &mut now,
        &mut sleep,
    );
    assert!(result.is_ok(), "a missing directory is success: {result:?}");
}

#[test]
fn cleanup_non_transient_error_fails_immediately_without_retry() {
    let dir = TestTempDir::create("cleanup-other-error");
    let path = dir.path().to_path_buf();
    std::mem::forget(dir);

    let attempts = AtomicUsize::new(0);
    let mut remover = |_p: &Path| {
        attempts.fetch_add(1, Ordering::SeqCst);
        Err(std::io::Error::other("not transient"))
    };
    let mut now = Instant::now;
    let mut sleep = |_delay: Duration| {};
    let result = remove_owned_test_dir_with_retry(
        &path,
        Duration::from_secs(2),
        Duration::from_millis(50),
        &mut remover,
        &mut now,
        &mut sleep,
    );
    assert!(result.is_err(), "non-transient errors must fail");
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        1,
        "non-transient errors must not retry"
    );
    let _ = std::fs::remove_dir_all(&path);
}

impl EnvVarGuard {
    fn set(key: &'static str, value: impl AsRef<OsStr>) -> Self {
        let guard = Self {
            key,
            original: std::env::var_os(key),
        };
        std::env::set_var(key, value);
        guard
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        match &self.original {
            Some(value) => std::env::set_var(self.key, value),
            None => std::env::remove_var(self.key),
        }
    }
}

struct AcpFixture {
    root: TestTempDir,
    executable: PathBuf,
}

impl AcpFixture {
    fn compile(name: &str) -> Self {
        let root = TestTempDir::create(name);
        let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        let source = manifest_dir
            .join("tests")
            .join("fixtures")
            .join("acp_stdio_fixture.rs");
        assert!(source.is_file(), "ACP fixture source must be test-only");
        assert!(
            !manifest_dir
                .join("src")
                .join("bin")
                .join("agenttalk-acp-stdio-fixture.rs")
                .exists(),
            "ACP fixture must not be a release binary target"
        );
        let executable = root.path().join(if cfg!(windows) {
            "agenttalk-acp-stdio-fixture.exe"
        } else {
            "agenttalk-acp-stdio-fixture"
        });
        let output = std::process::Command::new("rustc")
            .args(["--edition=2021"])
            .arg(source)
            .arg("-o")
            .arg(&executable)
            .output()
            .expect("compile ACP stdio fixture");
        assert!(
            output.status.success(),
            "compile ACP fixture failed: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        Self { root, executable }
    }

    fn executable(&self) -> &Path {
        &self.executable
    }

    fn executable_name(&self) -> String {
        self.executable
            .file_name()
            .and_then(|name| name.to_str())
            .expect("fixture executable name")
            .to_owned()
    }
}

fn manifest_for(fixture: &AcpFixture, mode: &str) -> AdapterManifest {
    AdapterManifest::validate_value(json!({
        "schemaVersion": "agenttalk.adapter.v1",
        "id": "fixture.thirdparty.acp",
        "displayName": "Third Party ACP Fixture",
        "category": "agent_protocol",
        "protocol": { "kind": "acp", "major": 1 },
        "match": { "executableNames": [fixture.executable_name()] },
        "launch": {
            "kind": "direct",
            "transport": "stdio",
            "executableRef": "matched-observation",
            "args": [mode],
            "environmentAllowlist": []
        },
        "verification": { "kind": "acp_initialize", "timeoutMs": 3000 },
        "capabilityPolicy": {
            "filesystem": "forbidden",
            "shell": "forbidden",
            "streaming": "negotiate",
            "cancel": "negotiate"
        }
    }))
    .expect("valid ACP fixture manifest")
}

fn classify(
    factory: &AcpProtocolAdapterFactory,
    manifest: &AdapterManifest,
    executable: &Path,
) -> AcpClassification {
    // The test-only fixture is observed through the UserSelected authority:
    // the tests explicitly select the executable they compiled, which is the
    // legitimate way to exercise the ACP protocol chain. A filename-only
    // heuristic observation is covered separately by the spoof-rejection
    // test and must never launch.
    let observation = AcpPassiveObservation::from_observed_executable(
        executable,
        ObservationSourceKind::UserSelected,
        Instant::now() + ACP_TIMEOUT,
        &AtomicBool::new(false),
    )
    .expect("passively observe legal ACP fixture");
    factory
        .classify(manifest, observation)
        .expect("classify legal ACP fixture")
}

fn verify(
    factory: &AcpProtocolAdapterFactory,
    classification: &AcpClassification,
    timeout: Duration,
    cancelled: &AtomicBool,
) -> AcpVerificationResult {
    let consent = AcpVerificationConsent::for_candidate(classification.candidate_id());
    factory.verify(
        classification,
        Some(&consent),
        Instant::now() + timeout,
        cancelled,
    )
}

fn execution_request(fixture: &AcpFixture, id: &str) -> RuntimeRequest {
    RuntimeRequest {
        execution_run_id: id.to_owned(),
        agent_identity_id: "agent-acp-fixture".into(),
        connector_id: "fixture.thirdparty.acp".into(),
        model_id: None,
        context_manifest_id: "context-fixture".into(),
        rendered_context: "write a hello world Rust function and run tests".into(),
        canonical_cwd: Some(fixture.root.path().display().to_string()),
        workspace_access: WorkspaceAccess::WorkspaceWrite,
        timeout_ms: 5_000,
        thread_policy: "new".into(),
        signed_scope: "fixture-scope".into(),
    }
}

fn process_id_from_marker(path: &Path) -> u32 {
    std::fs::read_to_string(path)
        .expect("read owned ACP fixture process marker")
        .trim()
        .parse()
        .expect("owned ACP fixture process marker is a PID")
}

fn wait_for_process_id_from_marker(path: &Path, label: &str) -> u32 {
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        if path.is_file() {
            return process_id_from_marker(path);
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    panic!("owned ACP fixture never recorded {label}");
}

fn owned_fixture_pids(fixture: &AcpFixture) -> (u32, u32) {
    let root = wait_for_process_id_from_marker(&fixture.root.path().join("root.pid"), "root PID");
    let descendant = wait_for_process_id_from_marker(
        &fixture.root.path().join("descendant.pid"),
        "descendant PID",
    );
    assert_ne!(root, descendant, "fixture must create a real descendant");
    (root, descendant)
}

fn assert_fixture_never_started(fixture: &AcpFixture) {
    assert!(!fixture.root.path().join("root.pid").exists());
    assert!(!fixture.root.path().join("descendant.pid").exists());
}

fn sha256_of_file(path: &Path) -> String {
    // This must match the Core fingerprint's content_sha256 exactly:
    // SHA-256 of the u64 little-endian byte length followed by the file bytes.
    let bytes = std::fs::read(path).expect("read fixture executable");
    let mut hasher = Sha256::new();
    hasher.update((bytes.len() as u64).to_le_bytes());
    hasher.update(&bytes);
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn passive_windows_report_for_fixture(
    fixture: &AcpFixture,
) -> crate::LocalConnectorDiscoveryReport {
    discover_windows_passive_report_with_config(&WindowsPassiveDiscoveryConfig {
        path_env: Some(fixture.root.path().display().to_string()),
        // The test-only fixture is also an explicit UserSelected source: the
        // legitimate authority that lets a test exercise the ACP protocol
        // chain. A filename-only PATH heuristic is never launchable.
        explicit_sources: vec![ExplicitDiscoverySource::Executable(
            fixture.executable().to_path_buf(),
        )],
        use_real_app_paths: false,
        use_real_packages: false,
        use_real_loopback: false,
        request_timeout: Duration::from_secs(2),
        ..WindowsPassiveDiscoveryConfig::default()
    })
}

#[cfg(windows)]
fn process_exists(pid: u32) -> bool {
    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE};
    use windows_sys::Win32::System::Threading::{
        GetExitCodeProcess, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
    };
    const STILL_ACTIVE: u32 = 259;

    let process: HANDLE = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
    if process.is_null() {
        return false;
    }
    let mut exit_code = 0u32;
    let queried = unsafe { GetExitCodeProcess(process, &mut exit_code) };
    unsafe {
        CloseHandle(process);
    }
    queried != 0 && exit_code == STILL_ACTIVE
}

#[cfg(windows)]
fn wait_for_owned_pids_to_exit(root: u32, descendant: u32) {
    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline {
        if !process_exists(root) && !process_exists(descendant) {
            return;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    assert!(!process_exists(root), "owned ACP root process remains");
    assert!(
        !process_exists(descendant),
        "owned ACP descendant process remains"
    );
}

#[cfg(windows)]
fn wait_for_owned_pid_to_exit(pid: u32) {
    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline {
        if !process_exists(pid) {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("owned ACP process {pid} survived cleanup");
}

#[test]
fn third_party_acp_manifest_is_identified_then_initialize_only_after_consent() {
    let _guard = suite_guard();
    let fixture = AcpFixture::compile("acp-success");
    let factory = AcpProtocolAdapterFactory;
    let manifest = manifest_for(&fixture, "success");
    let classification = classify(&factory, &manifest, fixture.executable());

    assert_eq!(
        classification.projection().discovery_state,
        DiscoveryState::Identified
    );
    assert_eq!(
        classification.projection().compatibility_state,
        CompatibilityState::NotVerified
    );
    assert!(classification.projection().requires_configuration);

    let result = verify(
        &factory,
        &classification,
        ACP_TIMEOUT,
        &AtomicBool::new(false),
    );
    assert_eq!(result.report().status, AcpVerificationStatus::Verified);
    assert_eq!(
        result.report().compatibility_state,
        CompatibilityState::Compatible
    );
    assert_eq!(result.report().auth_state, AuthState::NotRequired);
    assert_eq!(
        result
            .report()
            .agent_info
            .as_ref()
            .map(|info| info.version.as_str()),
        Some("init1-other0-envabsent")
    );
    let adapter = factory
        .instantiate(&classification, &result)
        .expect("same classified target and verification may instantiate deferred adapter");
    assert!(adapter
        .execute(&RuntimeRequest {
            execution_run_id: "fixture-run".into(),
            agent_identity_id: "fixture-agent".into(),
            connector_id: "fixture.thirdparty.acp".into(),
            model_id: None,
            context_manifest_id: "fixture-context".into(),
            rendered_context: "must never be sent".into(),
            canonical_cwd: None,
            workspace_access: WorkspaceAccess::None,
            timeout_ms: 1,
            thread_policy: "fixture".into(),
            signed_scope: "fixture".into(),
        })
        .is_err());
}

#[test]
fn production_passive_observation_classifies_and_verifies_after_candidate_consent() {
    let _guard = suite_guard();
    let fixture = AcpFixture::compile("acp-production-entry");
    let manifest = manifest_for(&fixture, "success");
    let passive = passive_windows_report_for_fixture(&fixture);

    let discovery = passive.classify_acp(
        &[manifest],
        Instant::now() + Duration::from_secs(2),
        &AtomicBool::new(false),
    );
    let projection = discovery
        .projections()
        .first()
        .expect("production passive report creates one ACP classification");
    assert_eq!(projection.discovery_state, DiscoveryState::Identified);
    assert_eq!(
        projection.compatibility_state,
        CompatibilityState::NotVerified
    );
    assert!(projection.requires_configuration);

    let consent = AcpVerificationConsent::for_candidate(&projection.candidate_id);
    let result = discovery
        .verify(
            &consent,
            Instant::now() + ACP_TIMEOUT,
            &AtomicBool::new(false),
        )
        .expect("structured candidate consent verifies the production target");
    assert_eq!(result.report().status, AcpVerificationStatus::Verified);
    assert_eq!(
        result.report().compatibility_state,
        CompatibilityState::Compatible
    );
    let adapter = discovery
        .instantiate(&consent, &result)
        .expect("only the verified production target may create an owned adapter");
    assert!(matches!(
        adapter.execute(&RuntimeRequest {
            execution_run_id: "production-fixture-run".into(),
            agent_identity_id: "production-fixture-agent".into(),
            connector_id: "fixture.thirdparty.acp".into(),
            model_id: None,
            context_manifest_id: "production-fixture-context".into(),
            rendered_context: "must never be sent".into(),
            canonical_cwd: None,
            workspace_access: WorkspaceAccess::None,
            timeout_ms: 1,
            thread_policy: "fixture".into(),
            signed_scope: "fixture".into(),
        }),
        Err(crate::RuntimeError::Permission)
    ));
}

#[test]
fn production_passive_entrypoint_rejects_unmatched_or_unconsented_targets_without_launching() {
    let _guard = suite_guard();
    let fixture = AcpFixture::compile("acp-production-reject");
    let mut unmatched = manifest_for(&fixture, "unknown-start-marker");
    unmatched.launch = match unmatched.launch {
        ManifestLaunch::Direct {
            transport,
            args,
            environment_allowlist,
            credential_environment,
            archive_sha256,
            sha256,
            ..
        } => ManifestLaunch::Direct {
            transport,
            executable_ref: "unmatched-fixture.exe".into(),
            args,
            environment_allowlist,
            credential_environment,
            archive_sha256,
            sha256,
        },
        _ => panic!("fixture manifest launch"),
    };
    let passive = passive_windows_report_for_fixture(&fixture);

    let mismatch = passive.classify_acp(
        &[unmatched.clone()],
        Instant::now() + Duration::from_secs(2),
        &AtomicBool::new(false),
    );
    assert!(mismatch.projections().is_empty());
    assert_fixture_never_started(&fixture);

    let matched = passive.classify_acp(
        &[manifest_for(&fixture, "unknown-start-marker")],
        Instant::now() + Duration::from_secs(2),
        &AtomicBool::new(false),
    );
    assert_eq!(matched.projections().len(), 1);
    let wrong_consent = AcpVerificationConsent::for_candidate("candidate-not-in-session");
    assert!(matched
        .verify(
            &wrong_consent,
            Instant::now() + ACP_TIMEOUT,
            &AtomicBool::new(false),
        )
        .is_err());
    assert_fixture_never_started(&fixture);
}

#[test]
fn production_acp_compatibility_report_keeps_passive_identity_private() {
    let _guard = suite_guard();
    let fixture = AcpFixture::compile("acp-production-safe-report");
    let manifest = manifest_for(&fixture, "success");
    let passive = passive_windows_report_for_fixture(&fixture);
    let discovery = passive.classify_acp(
        &[manifest],
        Instant::now() + Duration::from_secs(2),
        &AtomicBool::new(false),
    );
    let candidate_id = discovery
        .projections()
        .first()
        .expect("production target")
        .candidate_id
        .clone();
    let result = discovery
        .verify(
            &AcpVerificationConsent::for_candidate(&candidate_id),
            Instant::now() + ACP_TIMEOUT,
            &AtomicBool::new(false),
        )
        .expect("verify production target");
    let serialized = serde_json::to_string(result.report()).expect("serialize safe report");
    let passive_serialized =
        serde_json::to_string(&passive).expect("serialize renderer-safe passive report");
    let passive_debug = format!("{passive:?}");
    let discovery_debug = format!("{discovery:?}");
    for forbidden in [
        fixture.root.path().display().to_string(),
        fixture.executable().display().to_string(),
        "stable_identity".to_owned(),
        "content_sha256".to_owned(),
        "locator".to_owned(),
        "fingerprint".to_owned(),
    ] {
        assert!(!serialized.contains(&forbidden));
        assert!(!passive_serialized.contains(&forbidden));
        assert!(!passive_debug.contains(&forbidden));
        assert!(!discovery_debug.contains(&forbidden));
    }
}

#[test]
fn production_acp_session_rechecks_identity_before_launch_and_cannot_reuse_binding() {
    let _guard = suite_guard();
    let first = AcpFixture::compile("acp-production-first");
    let second = AcpFixture::compile("acp-production-second");
    let changed = AcpFixture::compile("acp-production-changed");
    let first_manifest = manifest_for(&first, "success");
    let second_manifest = manifest_for(&second, "success");
    let changed_manifest = manifest_for(&changed, "unknown-start-marker");
    let first_passive = passive_windows_report_for_fixture(&first);
    let second_passive = passive_windows_report_for_fixture(&second);
    let changed_passive = passive_windows_report_for_fixture(&changed);
    let first_session = first_passive.classify_acp(
        &[first_manifest],
        Instant::now() + Duration::from_secs(2),
        &AtomicBool::new(false),
    );
    let second_session = second_passive.classify_acp(
        &[second_manifest],
        Instant::now() + Duration::from_secs(2),
        &AtomicBool::new(false),
    );
    let changed_session = changed_passive.classify_acp(
        &[changed_manifest],
        Instant::now() + Duration::from_secs(2),
        &AtomicBool::new(false),
    );
    let first_consent = AcpVerificationConsent::for_candidate(
        &first_session
            .projections()
            .first()
            .expect("first production target")
            .candidate_id,
    );
    let second_consent = AcpVerificationConsent::for_candidate(
        &second_session
            .projections()
            .first()
            .expect("second production target")
            .candidate_id,
    );
    let verified = first_session
        .verify(
            &first_consent,
            Instant::now() + ACP_TIMEOUT,
            &AtomicBool::new(false),
        )
        .expect("verify first production target");
    assert!(second_session
        .instantiate(&second_consent, &verified)
        .is_err());

    let replacement = AcpFixture::compile("acp-production-replacement");
    std::fs::copy(replacement.executable(), changed.executable())
        .expect("replace passive executable after classification");
    let changed_consent = AcpVerificationConsent::for_candidate(
        &changed_session
            .projections()
            .first()
            .expect("changed production target")
            .candidate_id,
    );
    let changed_result = changed_session
        .verify(
            &changed_consent,
            Instant::now() + ACP_TIMEOUT,
            &AtomicBool::new(false),
        )
        .expect("classified target returns a typed verification report");
    assert_eq!(
        changed_result.report().status,
        AcpVerificationStatus::Rejected
    );
    assert_eq!(
        changed_result.report().diagnostic,
        Some(AcpVerificationDiagnosticCode::IdentityMismatch)
    );
    assert_fixture_never_started(&changed);
}

#[test]
fn verification_requires_structured_consent_and_never_starts_without_it() {
    let _guard = suite_guard();
    let fixture = AcpFixture::compile("acp-no-consent");
    let factory = AcpProtocolAdapterFactory;
    let manifest = manifest_for(&fixture, "unknown-start-marker");
    let classification = classify(&factory, &manifest, fixture.executable());

    let result = factory.verify(
        &classification,
        None,
        Instant::now() + ACP_TIMEOUT,
        &AtomicBool::new(false),
    );
    assert_eq!(result.report().status, AcpVerificationStatus::Rejected);
    assert_eq!(
        result.report().diagnostic,
        Some(AcpVerificationDiagnosticCode::ConsentRequired)
    );
    assert_fixture_never_started(&fixture);
}

#[test]
fn unknown_executable_never_classifies_or_starts() {
    let _guard = suite_guard();
    let fixture = AcpFixture::compile("acp-unknown");
    let factory = AcpProtocolAdapterFactory;
    let mut manifest = manifest_for(&fixture, "unknown-start-marker");
    manifest.match_rules.executable_names = vec!["not-this-fixture.exe".into()];
    let observation = AcpPassiveObservation::from_observed_executable(
        fixture.executable(),
        ObservationSourceKind::WindowsPath,
        Instant::now() + ACP_TIMEOUT,
        &AtomicBool::new(false),
    )
    .expect("passively observe unknown test fixture");
    let result = factory.classify(&manifest, observation);
    assert!(result.is_err());
    assert_fixture_never_started(&fixture);
}

#[test]
fn manifest_observation_identity_mismatch_never_starts() {
    let _guard = suite_guard();
    let fixture = AcpFixture::compile("acp-identity-mismatch");
    let factory = AcpProtocolAdapterFactory;
    let manifest = manifest_for(&fixture, "success");
    let classification = classify(&factory, &manifest, fixture.executable());
    let original = fixture.root.path().join("original-fixture.exe");
    std::fs::rename(fixture.executable(), &original).expect("move observed fixture aside");
    std::fs::write(fixture.executable(), b"identity mismatch replacement")
        .expect("replace observed fixture");

    let result = verify(
        &factory,
        &classification,
        ACP_TIMEOUT,
        &AtomicBool::new(false),
    );
    assert_eq!(result.report().status, AcpVerificationStatus::Rejected);
    assert_eq!(
        result.report().diagnostic,
        Some(AcpVerificationDiagnosticCode::IdentityMismatch)
    );
    assert_fixture_never_started(&fixture);
}

#[test]
fn unsupported_major_and_auth_required_are_typed_and_safe() {
    let _guard = suite_guard();
    let factory = AcpProtocolAdapterFactory;

    let major_fixture = AcpFixture::compile("acp-major");
    let major_classification = classify(
        &factory,
        &manifest_for(&major_fixture, "unsupported-major"),
        major_fixture.executable(),
    );
    let major = verify(
        &factory,
        &major_classification,
        ACP_TIMEOUT,
        &AtomicBool::new(false),
    );
    assert_eq!(major.report().status, AcpVerificationStatus::Rejected);
    assert_eq!(
        major.report().diagnostic,
        Some(AcpVerificationDiagnosticCode::ProtocolMismatch)
    );

    let auth_fixture = AcpFixture::compile("acp-auth");
    let auth_classification = classify(
        &factory,
        &manifest_for(&auth_fixture, "auth-required"),
        auth_fixture.executable(),
    );
    let auth = verify(
        &factory,
        &auth_classification,
        ACP_TIMEOUT,
        &AtomicBool::new(false),
    );
    assert_eq!(auth.report().status, AcpVerificationStatus::AuthRequired);
    assert_eq!(auth.report().auth_state, AuthState::Required);
    assert_eq!(
        auth.report().diagnostic,
        Some(AcpVerificationDiagnosticCode::AuthenticationRequired)
    );
    assert!(
        factory.instantiate(&auth_classification, &auth).is_err(),
        "auth-required ACP candidate must not instantiate a deferred adapter"
    );
}

#[test]
fn verified_acp_adapter_runs_one_new_session_and_one_prompt_in_owned_process() {
    let _guard = suite_guard();
    let fixture = AcpFixture::compile("acp-execute-success");
    let factory = AcpProtocolAdapterFactory;
    let classification = classify(
        &factory,
        &manifest_for(&fixture, "execute-success"),
        fixture.executable(),
    );
    let verification = verify(
        &factory,
        &classification,
        Duration::from_secs(2),
        &AtomicBool::new(false),
    );
    assert_eq!(
        verification.report().status,
        AcpVerificationStatus::Verified
    );
    std::fs::remove_file(fixture.root.path().join("root.pid")).expect("reset root marker");
    let adapter = factory
        .instantiate(&classification, &verification)
        .expect("instantiate verified ACP adapter");
    let events = adapter
        .execute(&execution_request(&fixture, "acp-execution-success"))
        .expect("execute one-shot ACP turn");
    assert_eq!(
        events.first().map(|event| event.event_type.as_str()),
        Some("runtime.started")
    );
    assert_eq!(
        events.last().map(|event| event.event_type.as_str()),
        Some("execution.completed")
    );
    assert!(events.iter().any(|event| {
        event.event_type == "output.delta"
            && event
                .payload
                .get("delta")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|text| text.contains("hello_world"))
    }));
    assert_eq!(
        std::fs::read_to_string(fixture.root.path().join("session-new.invocations"))
            .expect("session/new marker"),
        "1"
    );
    assert_eq!(
        std::fs::read_to_string(fixture.root.path().join("session-prompt.invocations"))
            .expect("session/prompt marker"),
        "1"
    );
    let pid = process_id_from_marker(&fixture.root.path().join("root.pid"));
    #[cfg(windows)]
    wait_for_owned_pid_to_exit(pid);
    #[cfg(not(windows))]
    let _ = pid;
}

#[test]
fn acp_stream_cancel_sends_session_cancel_and_reaps_owned_process() {
    let _guard = suite_guard();
    let fixture = AcpFixture::compile("acp-execute-cancel");
    let factory = AcpProtocolAdapterFactory;
    let classification = classify(
        &factory,
        &manifest_for(&fixture, "execute-cancel"),
        fixture.executable(),
    );
    let verification = verify(
        &factory,
        &classification,
        Duration::from_secs(2),
        &AtomicBool::new(false),
    );
    std::fs::remove_file(fixture.root.path().join("root.pid")).expect("reset root marker");
    let adapter = factory
        .instantiate(&classification, &verification)
        .expect("instantiate verified ACP adapter");
    let stream = adapter
        .stream_events(&execution_request(&fixture, "acp-execution-cancel"))
        .expect("start ACP stream");
    wait_for_process_id_from_marker(
        &fixture.root.path().join("session-prompt.invocations"),
        "session/prompt marker",
    );
    let pid = process_id_from_marker(&fixture.root.path().join("root.pid"));
    stream.cancel().expect("cancel ACP stream");
    let cancel_marker = fixture.root.path().join("session-cancel.invocations");
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline && !cancel_marker.is_file() {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(
        cancel_marker.is_file(),
        "session/cancel must reach the ACP agent"
    );
    drop(stream);
    #[cfg(windows)]
    wait_for_owned_pid_to_exit(pid);
    #[cfg(not(windows))]
    let _ = pid;
}

#[test]
fn official_acp_metadata_and_session_capabilities_are_accepted_without_retention() {
    let _guard = suite_guard();
    let fixture = AcpFixture::compile("acp-official-capabilities");
    let factory = AcpProtocolAdapterFactory;
    let classification = classify(
        &factory,
        &manifest_for(&fixture, "official-capabilities"),
        fixture.executable(),
    );
    let result = verify(
        &factory,
        &classification,
        ACP_TIMEOUT,
        &AtomicBool::new(false),
    );

    assert_eq!(result.report().status, AcpVerificationStatus::AuthRequired);
    assert!(result.report().capabilities.load_session);
    assert!(result.report().capabilities.prompt_image);
    assert!(result.report().capabilities.prompt_audio);
    assert!(result.report().capabilities.prompt_embedded_context);
    assert!(result.report().capabilities.mcp_http);
    assert!(result.report().capabilities.mcp_sse);
    assert!(result.report().capabilities.supports_logout);
    let serialized = serde_json::to_string(result.report()).expect("serialize safe ACP report");
    assert!(!serialized.contains("_meta"));
    assert!(!serialized.contains("ignore"));
}

#[test]
fn timeout_and_external_cancel_reap_only_owned_acp_processes() {
    let _guard = suite_guard();
    let factory = AcpProtocolAdapterFactory;

    let timeout_fixture = AcpFixture::compile("acp-timeout");
    let timeout_classification = classify(
        &factory,
        &manifest_for(&timeout_fixture, "timeout"),
        timeout_fixture.executable(),
    );
    let started = Instant::now();
    let timeout = verify(
        &factory,
        &timeout_classification,
        Duration::from_millis(100),
        &AtomicBool::new(false),
    );
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "ACP timeout must have a bounded return"
    );
    assert_eq!(
        timeout.report().diagnostic,
        Some(AcpVerificationDiagnosticCode::Timeout)
    );

    let cancel_fixture = AcpFixture::compile("acp-cancel");
    let cancel_classification = classify(
        &factory,
        &manifest_for(&cancel_fixture, "timeout"),
        cancel_fixture.executable(),
    );
    let cancelled = std::sync::Arc::new(AtomicBool::new(false));
    let cancelled_for_thread = std::sync::Arc::clone(&cancelled);
    let canceller = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(40));
        cancelled_for_thread.store(true, Ordering::Release);
    });
    let cancelled_result = verify(
        &factory,
        &cancel_classification,
        ACP_TIMEOUT,
        cancelled.as_ref(),
    );
    canceller.join().expect("cancel thread joins");
    assert_eq!(
        cancelled_result.report().diagnostic,
        Some(AcpVerificationDiagnosticCode::Cancelled)
    );
}

#[test]
fn malformed_stdout_stderr_and_crash_are_fail_closed() {
    let _guard = suite_guard();
    let factory = AcpProtocolAdapterFactory;
    for mode in [
        "stdout-pollution",
        "oversized",
        "truncated",
        "duplicate-response",
        "trailing-frame",
        "empty-frame",
        "stderr",
        "crash",
        "response-then-crash",
        "invalid-agent-info",
        "invalid-capabilities",
        "invalid-auth-method",
    ] {
        let fixture = AcpFixture::compile(&format!("acp-{mode}"));
        let classification = classify(
            &factory,
            &manifest_for(&fixture, mode),
            fixture.executable(),
        );
        let result = verify(
            &factory,
            &classification,
            ACP_TIMEOUT,
            &AtomicBool::new(false),
        );
        assert_eq!(
            result.report().status,
            AcpVerificationStatus::Rejected,
            "mode {mode} must fail closed"
        );
    }
}

#[test]
fn manifest_sha_mismatch_never_classifies_or_starts() {
    let _guard = suite_guard();
    let fixture = AcpFixture::compile("acp-manifest-sha-mismatch");
    let factory = AcpProtocolAdapterFactory;
    let mut manifest = manifest_for(&fixture, "unknown-start-marker");
    manifest.match_rules.sha256 = Some("00".repeat(32));

    let observation = AcpPassiveObservation::from_observed_executable(
        fixture.executable(),
        ObservationSourceKind::WindowsPath,
        Instant::now() + ACP_TIMEOUT,
        &AtomicBool::new(false),
    )
    .expect("passively observe SHA mismatch fixture");
    let result = factory.classify(&manifest, observation);

    assert!(matches!(
        result,
        Err(AcpClassificationError::ObservationMismatch)
    ));
    assert_fixture_never_started(&fixture);
}

#[test]
fn whitespace_only_stderr_and_explicit_safe_allowlist_are_accepted() {
    let _guard = suite_guard();
    let fixture = AcpFixture::compile("acp-whitespace-stderr");
    let factory = AcpProtocolAdapterFactory;
    let mut manifest = manifest_for(&fixture, "environment-allowlist");
    manifest.launch = match manifest.launch {
        ManifestLaunch::Direct {
            transport,
            executable_ref,
            args,
            credential_environment,
            archive_sha256,
            sha256,
            ..
        } => ManifestLaunch::Direct {
            transport,
            executable_ref,
            args,
            environment_allowlist: vec!["AGENTTALK_W4_SAFE_ALLOWED".into()],
            credential_environment,
            archive_sha256,
            sha256,
        },
        _ => panic!("fixture manifest launch"),
    };
    let _allowed = EnvVarGuard::set("AGENTTALK_W4_SAFE_ALLOWED", "allowed");
    let classification = classify(&factory, &manifest, fixture.executable());
    let result = verify(
        &factory,
        &classification,
        ACP_TIMEOUT,
        &AtomicBool::new(false),
    );
    assert_eq!(result.report().status, AcpVerificationStatus::Verified);
    assert_eq!(
        result
            .report()
            .agent_info
            .as_ref()
            .map(|info| info.version.as_str()),
        Some("init1-other0-envabsent-allowed")
    );

    let whitespace_fixture = AcpFixture::compile("acp-whitespace-only-stderr");
    let whitespace = classify(
        &factory,
        &manifest_for(&whitespace_fixture, "whitespace-stderr"),
        whitespace_fixture.executable(),
    );
    assert_eq!(
        verify(&factory, &whitespace, ACP_TIMEOUT, &AtomicBool::new(false),)
            .report()
            .status,
        AcpVerificationStatus::Verified
    );
}

#[test]
fn owned_child_is_proven_and_reaped_and_environment_is_minimal() {
    let _guard = suite_guard();
    let fixture = AcpFixture::compile("acp-child");
    let fixture_root = fixture.root.path().to_owned();
    let _sentinel = EnvVarGuard::set(
        "AGENTTALK_W4_UNRELATED_CREDENTIAL",
        "must-not-reach-acp-fixture",
    );
    let factory = AcpProtocolAdapterFactory;
    let classification = classify(
        &factory,
        &manifest_for(&fixture, "spawn-child"),
        fixture.executable(),
    );
    let result = verify(
        &factory,
        &classification,
        ACP_TIMEOUT,
        &AtomicBool::new(false),
    );
    assert_eq!(result.report().status, AcpVerificationStatus::Verified);
    let (root_pid, descendant_pid) = owned_fixture_pids(&fixture);
    assert_eq!(
        result
            .report()
            .agent_info
            .as_ref()
            .map(|info| info.version.as_str()),
        Some("init1-other0-envabsent-child"),
        "fixture only reports child creation after a real descendant spawn"
    );
    #[cfg(windows)]
    {
        wait_for_owned_pids_to_exit(root_pid, descendant_pid);
    }
    drop(fixture);
    assert!(
        !fixture_root.exists(),
        "owned ACP fixture directory must be removed after child cleanup"
    );
}

#[test]
fn valid_initialize_response_from_live_owned_tree_is_reaped() {
    let _guard = suite_guard();
    let fixture = AcpFixture::compile("acp-live-tree");
    let factory = AcpProtocolAdapterFactory;
    let classification = classify(
        &factory,
        &manifest_for(&fixture, "spawn-child-keepalive"),
        fixture.executable(),
    );

    let (result, root_pid, descendant_pid) = std::thread::scope(|scope| {
        let verification = scope.spawn(|| {
            verify(
                &factory,
                &classification,
                Duration::from_secs(3),
                &AtomicBool::new(false),
            )
        });
        let (root_pid, descendant_pid) = owned_fixture_pids(&fixture);
        let result = verification.join().expect("ACP verification thread joins");
        (result, root_pid, descendant_pid)
    });

    assert_eq!(result.report().status, AcpVerificationStatus::Verified);
    #[cfg(windows)]
    {
        wait_for_owned_pids_to_exit(root_pid, descendant_pid);
    }
    #[cfg(not(windows))]
    let _ = (root_pid, descendant_pid);
}

#[test]
fn timeout_cancel_and_crash_reap_recorded_owned_descendants() {
    let _guard = suite_guard();
    let factory = AcpProtocolAdapterFactory;

    for (name, mode, timeout, should_cancel, expected_diagnostic) in [
        (
            "acp-descendant-timeout",
            "spawn-child-timeout",
            Duration::from_secs(1),
            false,
            AcpVerificationDiagnosticCode::Timeout,
        ),
        (
            "acp-descendant-cancel",
            "spawn-child-timeout",
            Duration::from_secs(3),
            true,
            AcpVerificationDiagnosticCode::Cancelled,
        ),
        (
            "acp-descendant-crash",
            "spawn-child-crash",
            Duration::from_secs(3),
            false,
            AcpVerificationDiagnosticCode::ProcessFailed,
        ),
    ] {
        let fixture = AcpFixture::compile(name);
        let classification = classify(
            &factory,
            &manifest_for(&fixture, mode),
            fixture.executable(),
        );
        let cancelled = std::sync::Arc::new(AtomicBool::new(false));
        let (result, root_pid, descendant_pid) = std::thread::scope(|scope| {
            let verification =
                scope.spawn(|| verify(&factory, &classification, timeout, cancelled.as_ref()));
            let (root_pid, descendant_pid) = owned_fixture_pids(&fixture);
            if should_cancel {
                cancelled.store(true, Ordering::Release);
            } else if mode == "spawn-child-crash" {
                std::fs::write(fixture.root.path().join("allow-crash"), b"release")
                    .expect("release owned ACP crash fixture");
            }
            let result = verification.join().expect("ACP verification thread joins");
            (result, root_pid, descendant_pid)
        });
        assert_eq!(
            result.report().status,
            AcpVerificationStatus::Rejected,
            "{mode} must fail closed"
        );
        assert_eq!(result.report().diagnostic, Some(expected_diagnostic));
        #[cfg(windows)]
        wait_for_owned_pids_to_exit(root_pid, descendant_pid);
        #[cfg(not(windows))]
        let _ = (root_pid, descendant_pid);
    }
}

#[test]
fn compatibility_report_is_renderer_safe_and_cannot_be_reused_for_a_different_target() {
    let _guard = suite_guard();
    let factory = AcpProtocolAdapterFactory;
    let first = AcpFixture::compile("acp-safe-first");
    let second = AcpFixture::compile("acp-safe-second");
    let first_classification = classify(
        &factory,
        &manifest_for(&first, "success"),
        first.executable(),
    );
    let second_classification = classify(
        &factory,
        &manifest_for(&second, "success"),
        second.executable(),
    );
    let result = verify(
        &factory,
        &first_classification,
        ACP_TIMEOUT,
        &AtomicBool::new(false),
    );
    assert!(factory
        .instantiate(&second_classification, &result)
        .is_err());
    let serialized = serde_json::to_string(result.report()).expect("serialize safe report");
    for forbidden in [
        first.root.path().display().to_string(),
        second.root.path().display().to_string(),
        "AGENTTALK_W4_UNRELATED_CREDENTIAL".to_owned(),
        "authorization".to_owned(),
        "cookie".to_owned(),
        "locator".to_owned(),
        "fingerprint".to_owned(),
    ] {
        assert!(
            !serialized
                .to_ascii_lowercase()
                .contains(&forbidden.to_ascii_lowercase()),
            "renderer-safe report leaked {forbidden}"
        );
    }
}

#[test]
fn production_catalog_classifies_direct_manifest_without_product_branch() {
    let _guard = suite_guard();
    let factory = AcpProtocolAdapterFactory;
    let fixture = AcpFixture::compile("w83-production-direct");

    // The bundled production catalog is pure data: it loads through the
    // generic parser/validator and is matched through the same generic
    // classifier used by fixture manifests, with no product-specific scanner
    // branch anywhere in the classification path.
    let snapshot = crate::bundled_production_catalog().expect("bundled catalog is valid");
    let mut manifest = snapshot
        .manifests
        .iter()
        .find(|manifest| matches!(manifest.launch, ManifestLaunch::Direct { .. }))
        .expect("bundled catalog includes a direct manifest")
        .clone();
    manifest.match_rules.executable_names = vec![fixture.executable_name()];
    if let ManifestLaunch::Direct { executable_ref, .. } = &mut manifest.launch {
        *executable_ref = "matched-observation".to_owned();
    }
    let observation = AcpPassiveObservation::from_observed_executable(
        fixture.executable(),
        ObservationSourceKind::UserSelected,
        Instant::now() + ACP_TIMEOUT,
        &AtomicBool::new(false),
    )
    .expect("observe the direct-manifest fixture");
    let classification = factory
        .classify(&manifest, observation)
        .expect("generic direct manifest classification");
    assert!(classification.has_independent_identity());
    assert_eq!(classification.manifest_id(), manifest.id);
}

#[test]
fn heuristic_windows_path_observation_cannot_verify_or_launch() {
    let _guard = suite_guard();
    let factory = AcpProtocolAdapterFactory;
    let fixture = AcpFixture::compile("w831-heuristic-spoof");
    let root = TestTempDir::create("w831-heuristic-spoof");
    let spoofed_exe = root.path().join("copilot.exe");
    std::fs::copy(fixture.executable(), &spoofed_exe).expect("spoof a copilot.exe-named file");
    // A name-only manifest (no exact SHA pin) exercises the filename-only
    // heuristic: a Windows PATH observation is classified as a suspected
    // target but must never reach the ACP child spawn without an independent
    // identity. The production Copilot manifest now pins an exact SHA, so
    // this regression uses a generic name-only manifest.
    let mut manifest = manifest_for(&fixture, "success");
    manifest.match_rules.executable_names = vec!["copilot.exe".to_owned()];
    let observation = AcpPassiveObservation::from_observed_executable(
        &spoofed_exe,
        ObservationSourceKind::WindowsPath,
        Instant::now() + ACP_TIMEOUT,
        &AtomicBool::new(false),
    )
    .expect("observe the spoofed copilot.exe");
    let classification = factory
        .classify(&manifest, observation)
        .expect("heuristic classification succeeds");
    assert_eq!(
        classification.projection().trust_level,
        ObservationTrustLevel::Heuristic
    );
    assert!(
        !classification.has_independent_identity(),
        "a filename-only observation must not grant independent identity"
    );
    let result = verify(
        &factory,
        &classification,
        Duration::from_millis(2_000),
        &AtomicBool::new(false),
    );
    assert_eq!(
        result.report().status,
        AcpVerificationStatus::Rejected,
        "heuristic-only observation must be rejected before any launch"
    );
    assert_eq!(
        result.report().diagnostic,
        Some(AcpVerificationDiagnosticCode::IdentityUnverified)
    );
    // No child was ever spawned: the fixture markers must be absent.
    assert!(
        !root.path().join("root.pid").exists(),
        "rejected verification must not spawn the executable"
    );
    assert!(
        !root.path().join("initialize.invocations").exists(),
        "rejected verification must never reach initialize"
    );
    // Consent does not upgrade the trust level.
    assert_eq!(
        classification.projection().trust_level,
        ObservationTrustLevel::Heuristic
    );
}

#[test]
fn verified_executable_guard_denies_replacement_until_dropped() {
    let _guard = suite_guard();
    let fixture = AcpFixture::compile("w9f2-guard-replacement");
    let executable = fixture.executable();
    let replacement = TestTempDir::create("w9f2-guard-replacement");
    let malicious = replacement.path().join("malicious.exe");
    std::fs::copy(executable, &malicious).expect("copy a malicious fixture binary");

    // While the guard holds its delete/write-denying handle, replacing the
    // executable (rename onto its path) must fail — this is the handle
    // continuity that closes the identity-recheck to CreateProcessW TOCTOU.
    let verified = crate::open_verified_executable_guard(
        executable,
        Instant::now() + ACP_TIMEOUT,
        &AtomicBool::new(false),
    )
    .expect("open the verified executable guard");
    assert!(
        std::fs::rename(&malicious, executable).is_err(),
        "replacement must be denied while the guard holds the handle"
    );

    // Once the guard is dropped, the path is replaceable again.
    drop(verified);
    assert!(
        std::fs::rename(&malicious, executable).is_ok(),
        "replacement must succeed after the guard is dropped"
    );
}

#[test]
fn exact_sha_pin_grants_independent_identity_without_user_selected() {
    let _guard = suite_guard();
    let factory = AcpProtocolAdapterFactory;
    let fixture = AcpFixture::compile("w84-exact-sha-positive");
    let sha = sha256_of_file(fixture.executable());
    let mut manifest = manifest_for(&fixture, "success");
    manifest.match_rules.sha256 = Some(sha);
    // Observed through WindowsPath (not UserSelected): the exact SHA is the
    // independent identity, not the observation source or a publisher name.
    let observation = AcpPassiveObservation::from_observed_executable(
        fixture.executable(),
        ObservationSourceKind::WindowsPath,
        Instant::now() + ACP_TIMEOUT,
        &AtomicBool::new(false),
    )
    .expect("observe the fixture through WindowsPath");
    let classification = factory
        .classify(&manifest, observation)
        .expect("exact-SHA classification succeeds");
    assert!(
        classification.has_independent_identity(),
        "an exact executable SHA match must grant independent identity without UserSelected"
    );
    let result = verify(
        &factory,
        &classification,
        Duration::from_millis(2_000),
        &AtomicBool::new(false),
    );
    assert_eq!(
        result.report().status,
        AcpVerificationStatus::Verified,
        "exact-SHA identity must verify and initialize"
    );
    assert!(
        fixture.root.path().join("root.pid").exists(),
        "exact-SHA identity must actually spawn the fixture"
    );
    assert!(
        fixture.root.path().join("initialize.invocations").exists(),
        "exact-SHA identity must reach initialize"
    );
}

#[test]
fn sha_mismatch_rejects_at_classification_without_spawn() {
    let _guard = suite_guard();
    let factory = AcpProtocolAdapterFactory;
    let fixture = AcpFixture::compile("w84-sha-mismatch");
    let mut manifest = manifest_for(&fixture, "unknown-start-marker");
    manifest.match_rules.sha256 = Some("00".repeat(32));
    let observation = AcpPassiveObservation::from_observed_executable(
        fixture.executable(),
        ObservationSourceKind::WindowsPath,
        Instant::now() + ACP_TIMEOUT,
        &AtomicBool::new(false),
    )
    .expect("observe the SHA-mismatch fixture");
    let classification = factory.classify(&manifest, observation);
    assert!(
        matches!(
            classification,
            Err(AcpClassificationError::ObservationMismatch)
        ),
        "a name match with a mismatched exact SHA must be rejected at classification"
    );
    // No child, no initialize, no verified event.
    assert_fixture_never_started(&fixture);
}

#[test]
fn fixture_style_scan_with_explicit_source_observes_user_selected() {
    let _guard = suite_guard();
    let fixture = AcpFixture::compile("w831-explicit-source");
    let report =
        crate::discover_windows_passive_report_with_config(&WindowsPassiveDiscoveryConfig {
            path_env: Some(
                fixture
                    .executable()
                    .parent()
                    .expect("fixture parent")
                    .display()
                    .to_string(),
            ),
            explicit_sources: vec![ExplicitDiscoverySource::Executable(
                fixture.executable().canonicalize().expect("canonicalize"),
            )],
            use_real_app_paths: false,
            use_real_packages: false,
            use_real_loopback: false,
            max_results: 16,
            max_path_entries: 1,
            max_candidates_per_path_entry: 8,
            request_timeout: Duration::from_secs(2),
            ..WindowsPassiveDiscoveryConfig::default()
        });
    assert!(
        report.projections.iter().any(|projection| projection
            .source_kinds
            .contains(&ObservationSourceKind::UserSelected)),
        "explicit source must be observed as UserSelected; projections: {:?} diagnostics: {:?}",
        report
            .projections
            .iter()
            .map(|projection| (&projection.source_kinds, &projection.display_name))
            .collect::<Vec<_>>(),
        report.diagnostics
    );
}

#[test]
fn explicit_source_serializes_with_verbatim_paths() {
    let source = ExplicitDiscoverySource::Executable(std::path::PathBuf::from(
        r"\\?\C:\fixture\fixture-agent.exe",
    ));
    let payload = serde_json::to_value(&source).expect("serialize explicit source");
    let decoded: ExplicitDiscoverySource =
        serde_json::from_value(payload).expect("deserialize explicit source");
    assert_eq!(decoded, source);
}

#[test]
fn explicit_source_collects_observation_with_verbatim_path() {
    let _guard = suite_guard();
    let fixture = AcpFixture::compile("w831-explicit-verbatim");
    let canonical = fixture
        .executable()
        .canonicalize()
        .expect("canonicalize fixture");
    let report =
        crate::discover_windows_passive_report_with_config(&WindowsPassiveDiscoveryConfig {
            path_env: None,
            explicit_sources: vec![ExplicitDiscoverySource::Executable(canonical)],
            use_real_app_paths: false,
            use_real_packages: false,
            use_real_loopback: false,
            max_results: 4,
            request_timeout: Duration::from_secs(2),
            ..WindowsPassiveDiscoveryConfig::default()
        });
    assert!(
        report.projections.iter().any(|projection| projection
            .source_kinds
            .contains(&ObservationSourceKind::UserSelected)),
        "explicit verbatim-path executable must be observed; projections: {:?} diagnostics: {:?}",
        report
            .projections
            .iter()
            .map(|projection| (&projection.source_kinds, &projection.display_name))
            .collect::<Vec<_>>(),
        report.diagnostics
    );
}
