use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use agenttalk_domain::{CompatibilityState, DiscoveryState};
use agenttalk_runtime_host::{
    discover_windows_passive_report_with_config, install_local_discovery_fixture_worker_for_tests,
    AcpVerificationConsent, AcpVerificationStatus, AdapterManifest, ExplicitDiscoverySource,
    WindowsPassiveDiscoveryConfig,
};
use serde_json::json;

const WORKER_TIMEOUT: Duration = Duration::from_secs(5);
const ACP_TIMEOUT: Duration = Duration::from_secs(2);

fn unique_temp_path(name: &str) -> PathBuf {
    static NEXT_NONCE: AtomicUsize = AtomicUsize::new(0);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos();
    let nonce = NEXT_NONCE.fetch_add(1, Ordering::AcqRel);
    std::env::temp_dir().join(format!(
        "agenttalk-w41-{name}-{}-{now:x}-{nonce:x}",
        std::process::id()
    ))
}

struct TestTempDir {
    path: PathBuf,
}

impl TestTempDir {
    fn create(name: &str) -> Self {
        let path = unique_temp_path(name);
        std::fs::create_dir(&path).expect("create owned ACP production entry test directory");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestTempDir {
    fn drop(&mut self) {
        if !self.path.exists() {
            return;
        }
        if let Err(error) = std::fs::remove_dir_all(&self.path) {
            let message = format!("remove owned ACP production entry directory failed: {error}");
            if std::thread::panicking() {
                eprintln!("{message}");
            } else {
                panic!("{message}");
            }
        }
    }
}

struct AcpFixture {
    root: TestTempDir,
    executable: PathBuf,
}

impl AcpFixture {
    fn compile() -> Self {
        let root = TestTempDir::create("acp-production-entry");
        let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        let source = manifest_dir
            .join("tests")
            .join("fixtures")
            .join("acp_stdio_fixture.rs");
        let executable = root.path().join(if cfg!(windows) {
            "agenttalk-acp-production-entry.exe"
        } else {
            "agenttalk-acp-production-entry"
        });
        let output = std::process::Command::new("rustc")
            .args(["--edition=2021"])
            .arg(source)
            .arg("-o")
            .arg(&executable)
            .output()
            .expect("compile test-only ACP fixture");
        assert!(
            output.status.success(),
            "compile ACP production fixture failed: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        Self { root, executable }
    }

    fn manifest(&self) -> AdapterManifest {
        let executable_name = self
            .executable
            .file_name()
            .and_then(|name| name.to_str())
            .expect("fixture executable name");
        AdapterManifest::validate_value(json!({
            "schemaVersion": "agenttalk.adapter.v1",
            "id": "fixture.production.acp",
            "displayName": "Production ACP Fixture",
            "category": "agent_protocol",
            "protocol": { "kind": "acp", "major": 1 },
            "match": { "executableNames": [executable_name] },
            "launch": {
                "kind": "direct",
                "transport": "stdio",
                "executableRef": "matched-observation",
                "args": ["success"],
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
        .expect("valid ACP manifest")
    }
}

#[test]
fn public_acp_session_verifies_a_managed_passive_worker_candidate_after_consent() {
    let fixture = AcpFixture::compile();
    let _worker = install_local_discovery_fixture_worker_for_tests(env!(
        "CARGO_BIN_EXE_agenttalk-local-discovery-worker"
    ))
    .expect("install fixture discovery worker");
    let report = discover_windows_passive_report_with_config(&WindowsPassiveDiscoveryConfig {
        manifest_executable_names: Vec::new(),
        path_env: Some(fixture.root.path().display().to_string()),
        // The test-only fixture is also an explicit UserSelected source, the
        // legitimate authority that lets a test exercise the ACP protocol
        // chain; a filename-only PATH heuristic is never launchable.
        explicit_sources: vec![ExplicitDiscoverySource::Executable(
            fixture.executable.clone(),
        )],
        use_real_app_paths: false,
        use_real_packages: false,
        use_real_loopback: false,
        request_timeout: WORKER_TIMEOUT,
        ..WindowsPassiveDiscoveryConfig::default()
    });
    let serialized_report = serde_json::to_string(&report).expect("serialize safe passive report");
    assert!(
        !serialized_report.contains(&fixture.root.path().display().to_string()),
        "passive report must not serialize the private discovery path"
    );

    let session = report.classify_acp(
        &[fixture.manifest()],
        Instant::now() + ACP_TIMEOUT,
        &AtomicBool::new(false),
    );
    let projection = session
        .projections()
        .first()
        .expect("managed passive worker creates one ACP target");
    assert_eq!(projection.discovery_state, DiscoveryState::Identified);
    assert_eq!(
        projection.compatibility_state,
        CompatibilityState::NotVerified
    );
    let wrong_consent = AcpVerificationConsent::for_candidate("candidate-not-in-session");
    assert!(session
        .verify(
            &wrong_consent,
            Instant::now() + ACP_TIMEOUT,
            &AtomicBool::new(false)
        )
        .is_err());
    assert!(
        !fixture.root.path().join("root.pid").exists(),
        "a non-matching candidate consent must not launch the fixture"
    );

    let consent = AcpVerificationConsent::for_candidate(&projection.candidate_id);
    let verification = session
        .verify(
            &consent,
            Instant::now() + ACP_TIMEOUT,
            &AtomicBool::new(false),
        )
        .expect("matching candidate consent verifies the managed passive target");
    assert_eq!(
        verification.report().status,
        AcpVerificationStatus::Verified
    );
    assert_eq!(
        verification.report().compatibility_state,
        CompatibilityState::Compatible
    );
    assert!(
        fixture.root.path().join("root.pid").is_file(),
        "the ACP fixture must receive exactly one initialize request after consent"
    );
}
