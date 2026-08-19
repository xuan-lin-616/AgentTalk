#![cfg(windows)]

use agenttalk_ipc::{FramedTransport, NamedPipeClient, NamedPipeConnection};
use agenttalk_protocols::{
    CommandEnvelope, ErrorEnvelope, ProtocolHandshake, ProtocolVersion, QueryEnvelope,
    ResponseEnvelope, PROTOCOL_MAJOR,
};
use jsonschema::Draft;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const IPC_TIMEOUT: Duration = Duration::from_secs(10);
const CORE_EXIT_TIMEOUT: Duration = Duration::from_secs(5);
const WORKER_FILE: &str = "agenttalk-local-discovery-worker.exe";
const CORE_FILE: &str = "agenttalk-core.exe";
const WORKER_ENV: &str = "AGENTTALK_LOCAL_DISCOVERY_WORKER_EXE";

fn suite_guard() -> std::sync::MutexGuard<'static, ()> {
    static GUARD: OnceLock<Mutex<()>> = OnceLock::new();
    GUARD
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn unique_nonce() -> u128 {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after the Unix epoch")
        .as_nanos();
    now ^ u128::from(NEXT.fetch_add(1, Ordering::AcqRel))
}

struct OwnedTempDir {
    path: PathBuf,
}

impl OwnedTempDir {
    fn create(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "agenttalk-w5-{label}-{}-{:x}",
            std::process::id(),
            unique_nonce()
        ));
        fs::create_dir(&path).expect("create owned W5 test directory");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for OwnedTempDir {
    fn drop(&mut self) {
        if !self.path.exists() {
            return;
        }
        if let Err(error) = fs::remove_dir_all(&self.path) {
            let message = format!("remove owned W5 test directory failed: {error}");
            if thread::panicking() {
                eprintln!("{message}");
            } else {
                panic!("{message}");
            }
        }
    }
}

struct OwnedReleaseCore {
    child: Option<Child>,
    stderr_path: PathBuf,
}

struct ReleaseCoreSpawn<'a> {
    executable: &'a Path,
    worker: &'a Path,
    pipe: &'a str,
    database: &'a Path,
    artifact_root: &'a Path,
    fixture_root: &'a Path,
    catalog: &'a Path,
    credential: &'a str,
    /// When true the release Core runs in production mode: no dev-mode flag
    /// and no fixture catalog environment variables. The isolated PATH still
    /// points at `fixture_root` so production discovery can observe a
    /// `copilot.exe`-named external fixture.
    production_mode: bool,
    retention_max_events: Option<usize>,
    discovery_stream_max_owners: Option<usize>,
    discovery_stream_retention_ms: Option<u64>,
    discovery_max_sessions_per_owner: Option<usize>,
    discovery_max_sessions_global: Option<usize>,
    discovery_max_running_scans_per_owner: Option<usize>,
    discovery_max_running_scans_global: Option<usize>,
    discovery_max_receipts_per_session: Option<usize>,
    discovery_max_receipts_per_owner: Option<usize>,
    discovery_max_receipts_global: Option<usize>,
    discovery_max_running_verifications_per_owner: Option<usize>,
    discovery_max_running_verifications_global: Option<usize>,
    discovery_max_inflight_import_plans_per_owner: Option<usize>,
    discovery_max_inflight_import_plans_global: Option<usize>,
    discovery_import_plan_hold_ms: Option<u64>,
}

#[derive(Default)]
struct DiscoveryLimitOverrides {
    max_sessions_per_owner: Option<usize>,
    max_sessions_global: Option<usize>,
    max_running_scans_per_owner: Option<usize>,
    max_running_scans_global: Option<usize>,
    max_receipts_per_session: Option<usize>,
    max_receipts_per_owner: Option<usize>,
    max_receipts_global: Option<usize>,
    max_running_verifications_per_owner: Option<usize>,
    max_running_verifications_global: Option<usize>,
    max_inflight_import_plans_per_owner: Option<usize>,
    max_inflight_import_plans_global: Option<usize>,
    import_plan_hold_ms: Option<u64>,
}

impl OwnedReleaseCore {
    fn spawn(spec: ReleaseCoreSpawn<'_>) -> Self {
        let stderr_path = spec.artifact_root.join("core.stderr.txt");
        let stdout_path = spec.artifact_root.join("core.stdout.txt");
        let mut command = Command::new(spec.executable);
        command
            .args([
                spec.pipe,
                &spec.database.to_string_lossy(),
                &spec.artifact_root.to_string_lossy(),
            ])
            .env("AGENTTALK_CORE_SESSION_CREDENTIAL", spec.credential);
        if spec.production_mode {
            // Production mode: the bundled catalog is compiled into the Core;
            // no dev-mode flag and no fixture catalog environment variables.
            command.env_remove("AGENTTALK_CORE_DEV_MODE");
            command.env_remove("AGENTTALK_LOCAL_DISCOVERY_FIXTURE_ROOT");
            command.env_remove("AGENTTALK_LOCAL_DISCOVERY_FIXTURE_CATALOG");
            command.env_remove("AGENTTALK_LOCAL_DISCOVERY_FIXTURE_EXPLICIT_SOURCES");
            command.env(WORKER_ENV, spec.worker);
        } else {
            command
                .env("AGENTTALK_CORE_DEV_MODE", "1")
                .env(WORKER_ENV, spec.worker)
                .env("AGENTTALK_LOCAL_DISCOVERY_FIXTURE_ROOT", spec.fixture_root)
                .env("AGENTTALK_LOCAL_DISCOVERY_FIXTURE_CATALOG", spec.catalog)
                // The dev-mode fixture executable becomes an explicit
                // UserSelected observation, the legitimate test authority for
                // the ACP protocol chain; a filename-only heuristic match is
                // never launchable.
                .env(
                    "AGENTTALK_LOCAL_DISCOVERY_FIXTURE_EXPLICIT_SOURCES",
                    "fixture-agent.exe",
                );
        }
        command
            .env(
                "AGENTTALK_CODEX_BINARY",
                spec.fixture_root.join("missing-codex.exe"),
            )
            .env("KUN_DATA_DIR", spec.artifact_root.join("empty-kun-data"))
            .env(
                "LOCALAPPDATA",
                spec.artifact_root.join("empty-local-app-data"),
            )
            .env(
                "PATH",
                if spec.production_mode {
                    production_core_process_path(spec.fixture_root)
                } else {
                    core_process_path(spec.fixture_root)
                },
            )
            .env_remove("AGENTTALK_CORE_RUNTIME")
            .env_remove("AGENTTALK_CORE_RUNTIMES")
            .stdout(fs::File::create(&stdout_path).expect("create owned Core stdout capture"))
            .stderr(fs::File::create(&stderr_path).expect("create owned Core stderr capture"));
        if let Some(max_events) = spec.retention_max_events {
            command.env(
                "AGENTTALK_CORE_TEST_EVENT_RETENTION_MAX_EVENTS",
                max_events.to_string(),
            );
        }
        if let Some(max_owners) = spec.discovery_stream_max_owners {
            command.env(
                "AGENTTALK_CORE_TEST_DISCOVERY_STREAM_MAX_OWNERS",
                max_owners.to_string(),
            );
        }
        if let Some(retention_ms) = spec.discovery_stream_retention_ms {
            command.env(
                "AGENTTALK_CORE_TEST_DISCOVERY_STREAM_RETENTION_MS",
                retention_ms.to_string(),
            );
        }
        if let Some(limit) = spec.discovery_max_sessions_per_owner {
            command.env(
                "AGENTTALK_CORE_TEST_DISCOVERY_MAX_SESSIONS_PER_OWNER",
                limit.to_string(),
            );
        }
        if let Some(limit) = spec.discovery_max_sessions_global {
            command.env(
                "AGENTTALK_CORE_TEST_DISCOVERY_MAX_SESSIONS_GLOBAL",
                limit.to_string(),
            );
        }
        if let Some(limit) = spec.discovery_max_running_scans_per_owner {
            command.env(
                "AGENTTALK_CORE_TEST_DISCOVERY_MAX_RUNNING_SCANS_PER_OWNER",
                limit.to_string(),
            );
        }
        if let Some(limit) = spec.discovery_max_running_scans_global {
            command.env(
                "AGENTTALK_CORE_TEST_DISCOVERY_MAX_RUNNING_SCANS_GLOBAL",
                limit.to_string(),
            );
        }
        if let Some(limit) = spec.discovery_max_receipts_per_session {
            command.env(
                "AGENTTALK_CORE_TEST_DISCOVERY_MAX_RECEIPTS_PER_SESSION",
                limit.to_string(),
            );
        }
        if let Some(limit) = spec.discovery_max_receipts_per_owner {
            command.env(
                "AGENTTALK_CORE_TEST_DISCOVERY_MAX_RECEIPTS_PER_OWNER",
                limit.to_string(),
            );
        }
        if let Some(limit) = spec.discovery_max_receipts_global {
            command.env(
                "AGENTTALK_CORE_TEST_DISCOVERY_MAX_RECEIPTS_GLOBAL",
                limit.to_string(),
            );
        }
        if let Some(limit) = spec.discovery_max_running_verifications_per_owner {
            command.env(
                "AGENTTALK_CORE_TEST_DISCOVERY_MAX_RUNNING_VERIFICATIONS_PER_OWNER",
                limit.to_string(),
            );
        }
        if let Some(limit) = spec.discovery_max_running_verifications_global {
            command.env(
                "AGENTTALK_CORE_TEST_DISCOVERY_MAX_RUNNING_VERIFICATIONS_GLOBAL",
                limit.to_string(),
            );
        }
        if let Some(limit) = spec.discovery_max_inflight_import_plans_per_owner {
            command.env(
                "AGENTTALK_CORE_TEST_DISCOVERY_MAX_INFLIGHT_IMPORT_PLANS_PER_OWNER",
                limit.to_string(),
            );
        }
        if let Some(limit) = spec.discovery_max_inflight_import_plans_global {
            command.env(
                "AGENTTALK_CORE_TEST_DISCOVERY_MAX_INFLIGHT_IMPORT_PLANS_GLOBAL",
                limit.to_string(),
            );
        }
        if let Some(hold_ms) = spec.discovery_import_plan_hold_ms {
            command.env(
                "AGENTTALK_CORE_TEST_DISCOVERY_IMPORT_PLAN_HOLD_MS",
                hold_ms.to_string(),
            );
        }
        let child = command.spawn().expect("spawn isolated release Core binary");
        Self {
            child: Some(child),
            stderr_path,
        }
    }

    fn wait_for_clean_exit(&mut self) -> ExitStatus {
        let deadline = Instant::now() + CORE_EXIT_TIMEOUT;
        loop {
            let child = self.child.as_mut().expect("Core child is owned");
            match child.try_wait().expect("poll Core exit") {
                Some(status) => {
                    assert!(
                        status.success(),
                        "release Core exited unsuccessfully: {status}"
                    );
                    self.child.take();
                    return status;
                }
                None if Instant::now() < deadline => thread::sleep(Duration::from_millis(20)),
                None => panic!("release Core did not exit within {CORE_EXIT_TIMEOUT:?}"),
            }
        }
    }

    fn bounded_startup_diagnostic(&mut self) -> String {
        let exit = self
            .child
            .as_mut()
            .and_then(|child| child.try_wait().ok().flatten())
            .map(|status| status.to_string())
            .unwrap_or_else(|| "still_running".into());
        let mut stderr = fs::read_to_string(&self.stderr_path).unwrap_or_default();
        if stderr.len() > 512 {
            stderr.truncate(512);
            stderr.push_str("...[truncated]");
        }
        format!("exit={exit}; stderr={stderr:?}")
    }
}

impl Drop for OwnedReleaseCore {
    fn drop(&mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };
        if child.try_wait().ok().flatten().is_none() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

struct ReleaseFixture {
    _root: OwnedTempDir,
    fixture_root: PathBuf,
    catalog: PathBuf,
    database: PathBuf,
    artifact_root: PathBuf,
    core: PathBuf,
    worker: PathBuf,
    credential: String,
    pipe: String,
}

impl ReleaseFixture {
    fn create(mode: &str, include_unknown: bool) -> Self {
        let root = OwnedTempDir::create("release-named-pipe");
        let fixture_root = root.path().join("fixture-bin");
        fs::create_dir(&fixture_root).expect("create fixture executable directory");
        let fixture = compile_acp_fixture(&fixture_root);
        if include_unknown {
            fs::copy(&fixture, fixture_root.join("unknown-agent.exe"))
                .expect("create distinct unknown executable fixture");
        }
        let catalog = write_fixture_catalog(&fixture_root, mode);
        let core = release_binary(CORE_FILE);
        let worker = release_binary(WORKER_FILE);
        let nonce = unique_nonce();
        let pipe = format!(
            "\\\\.\\pipe\\agenttalk-w5-discovery-{}-{nonce:x}",
            std::process::id()
        );
        let database = root.path().join("agenttalk-core.sqlite3");
        let artifact_root = root.path().join("artifacts");
        fs::create_dir(&artifact_root).expect("create isolated artifact root");
        let credential = format!("w5-fixture-session-{}-{}", nonce, "x".repeat(48));
        Self {
            _root: root,
            fixture_root,
            catalog,
            database,
            artifact_root,
            core,
            worker,
            credential,
            pipe,
        }
    }

    fn spawn_core(&self, retention_max_events: Option<usize>) -> OwnedReleaseCore {
        self.spawn_core_with_discovery_stream_limits(retention_max_events, None, None)
    }

    fn spawn_core_with_discovery_stream_limits(
        &self,
        retention_max_events: Option<usize>,
        discovery_stream_max_owners: Option<usize>,
        discovery_stream_retention_ms: Option<u64>,
    ) -> OwnedReleaseCore {
        self.spawn_core_with_discovery_limits(
            retention_max_events,
            discovery_stream_max_owners,
            discovery_stream_retention_ms,
            DiscoveryLimitOverrides::default(),
        )
    }

    fn spawn_core_with_discovery_limits(
        &self,
        retention_max_events: Option<usize>,
        discovery_stream_max_owners: Option<usize>,
        discovery_stream_retention_ms: Option<u64>,
        discovery_limits: DiscoveryLimitOverrides,
    ) -> OwnedReleaseCore {
        OwnedReleaseCore::spawn(ReleaseCoreSpawn {
            executable: &self.core,
            worker: &self.worker,
            pipe: &self.pipe,
            database: &self.database,
            artifact_root: &self.artifact_root,
            fixture_root: &self.fixture_root,
            catalog: &self.catalog,
            credential: &self.credential,
            production_mode: false,
            retention_max_events,
            discovery_stream_max_owners,
            discovery_stream_retention_ms,
            discovery_max_sessions_per_owner: discovery_limits.max_sessions_per_owner,
            discovery_max_sessions_global: discovery_limits.max_sessions_global,
            discovery_max_running_scans_per_owner: discovery_limits.max_running_scans_per_owner,
            discovery_max_running_scans_global: discovery_limits.max_running_scans_global,
            discovery_max_receipts_per_session: discovery_limits.max_receipts_per_session,
            discovery_max_receipts_per_owner: discovery_limits.max_receipts_per_owner,
            discovery_max_receipts_global: discovery_limits.max_receipts_global,
            discovery_max_running_verifications_per_owner: discovery_limits
                .max_running_verifications_per_owner,
            discovery_max_running_verifications_global: discovery_limits
                .max_running_verifications_global,
            discovery_max_inflight_import_plans_per_owner: discovery_limits
                .max_inflight_import_plans_per_owner,
            discovery_max_inflight_import_plans_global: discovery_limits
                .max_inflight_import_plans_global,
            discovery_import_plan_hold_ms: discovery_limits.import_plan_hold_ms,
        })
    }

    fn connect(&self, session_id: &str, owned_core: &mut OwnedReleaseCore) -> NamedPipeConnection {
        self.connect_as("w5-release-named-pipe-test", session_id, owned_core)
    }

    fn connect_as(
        &self,
        client_id: &str,
        session_id: &str,
        owned_core: &mut OwnedReleaseCore,
    ) -> NamedPipeConnection {
        connect_authenticated(
            &self.pipe,
            &self.credential,
            client_id,
            session_id,
            owned_core,
        )
    }
}

fn core_process_path(fixture_root: &Path) -> std::ffi::OsString {
    let system_root = std::env::var_os("SystemRoot").expect("Windows SystemRoot is available");
    std::env::join_paths([
        fixture_root,
        Path::new(&system_root).join("System32").as_path(),
        Path::new(&system_root),
    ])
    .expect("build isolated Core process PATH")
}

/// Production-catalog discovery only needs to observe the isolated fixture
/// directory. It must NOT scan System32 or the Windows root inside the global
/// discovery budget, so the production child PATH contains exactly the fixture
/// root. Windows DLL loading is unaffected: the OS resolves system DLLs via
/// the standard system search order, independent of this process PATH.
fn production_core_process_path(fixture_root: &Path) -> std::ffi::OsString {
    std::env::join_paths([fixture_root]).expect("build production Core process PATH")
}

#[test]
fn production_catalog_fixture_path_excludes_system_directories() {
    let root = OwnedTempDir::create("prod-path-probe");
    let path = production_core_process_path(root.path());
    let components: Vec<PathBuf> = std::env::split_paths(&path).collect();
    assert_eq!(
        components.len(),
        1,
        "production PATH must contain only the fixture root, got {path:?}"
    );
    assert_eq!(
        components[0].as_path(),
        root.path(),
        "the fixture root must be the sole production PATH entry"
    );
    let lowered = path.to_string_lossy().to_ascii_lowercase();
    let system_root = std::env::var_os("SystemRoot")
        .expect("SystemRoot")
        .to_string_lossy()
        .to_ascii_lowercase();
    assert!(
        !lowered.contains(&format!("{system_root}\\system32")),
        "System32 must not be in the production PATH"
    );
    assert!(
        !lowered.contains(&system_root),
        "the Windows root must not be in the production PATH"
    );
}

/// Production-mode fixture: a test-only ACP fixture copied and named
/// `copilot.exe` in an isolated directory, driven by the release Core's
/// bundled production catalog. No dev-mode flag and no fixture catalog.
struct ProductionCatalogFixture {
    _root: OwnedTempDir,
    path_root: PathBuf,
    database: PathBuf,
    artifact_root: PathBuf,
    core: PathBuf,
    worker: PathBuf,
    credential: String,
    pipe: String,
}

impl ProductionCatalogFixture {
    fn create() -> Self {
        let root = OwnedTempDir::create("production-catalog");
        let path_root = root.path().join("bin");
        fs::create_dir(&path_root).expect("create production PATH directory");
        let fixture = compile_acp_fixture(&path_root);
        let copilot_exe = path_root.join("copilot.exe");
        fs::copy(&fixture, &copilot_exe).expect("name the external fixture copilot.exe");
        fs::copy(&fixture, path_root.join("random-tool.exe"))
            .expect("name an unrelated executable");
        let core = release_binary(CORE_FILE);
        let worker = release_binary(WORKER_FILE);
        let nonce = unique_nonce();
        let pipe = format!(
            "\\\\.\\pipe\\agenttalk-w83-prod-{}-{nonce:x}",
            std::process::id()
        );
        let database = root.path().join("agenttalk-core.sqlite3");
        let artifact_root = root.path().join("artifacts");
        fs::create_dir(&artifact_root).expect("create isolated artifact root");
        let credential = format!("w83-prod-session-{}-{}", nonce, "x".repeat(48));
        Self {
            _root: root,
            path_root,
            database,
            artifact_root,
            core,
            worker,
            credential,
            pipe,
        }
    }

    fn spawn_core(&self) -> OwnedReleaseCore {
        OwnedReleaseCore::spawn(ReleaseCoreSpawn {
            executable: &self.core,
            worker: &self.worker,
            pipe: &self.pipe,
            database: &self.database,
            artifact_root: &self.artifact_root,
            fixture_root: &self.path_root,
            catalog: &self.database,
            credential: &self.credential,
            production_mode: true,
            retention_max_events: None,
            discovery_stream_max_owners: None,
            discovery_stream_retention_ms: None,
            discovery_max_sessions_per_owner: None,
            discovery_max_sessions_global: None,
            discovery_max_running_scans_per_owner: None,
            discovery_max_running_scans_global: None,
            discovery_max_receipts_per_session: None,
            discovery_max_receipts_per_owner: None,
            discovery_max_receipts_global: None,
            discovery_max_running_verifications_per_owner: None,
            discovery_max_running_verifications_global: None,
            discovery_max_inflight_import_plans_per_owner: None,
            discovery_max_inflight_import_plans_global: None,
            discovery_import_plan_hold_ms: None,
        })
    }

    fn connect(&self, session_id: &str, owned_core: &mut OwnedReleaseCore) -> NamedPipeConnection {
        connect_authenticated(
            &self.pipe,
            &self.credential,
            "w83-production-test",
            session_id,
            owned_core,
        )
    }
}

fn release_binary(name: &str) -> PathBuf {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root");
    let binary = workspace.join("target").join("release").join(name);
    assert!(
        binary.is_file(),
        "required release binary is missing: {}",
        binary.display()
    );
    binary
}

fn compile_acp_fixture(root: &Path) -> PathBuf {
    let source = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../runtime_host/tests/fixtures/acp_stdio_fixture.rs");
    let executable = root.join("fixture-agent.exe");
    let output = Command::new("rustc")
        .args(["--edition=2021"])
        .arg(source)
        .arg("-o")
        .arg(&executable)
        .output()
        .expect("compile test-only ACP fixture");
    assert!(
        output.status.success(),
        "compile ACP fixture failed: {}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    executable
}

fn write_fixture_catalog(root: &Path, mode: &str) -> PathBuf {
    let catalog = root.join("fixture-catalog.json");
    let registry_sha256 = "d94f9df787f6779f618569e0d0d2f6f4b2f1d1e2f81c496de6c63c7f5c3a8a46";
    let value = json!({
        "version": 1,
        "generation": 1,
        "revision": "w5-fixture",
        "createdAtMs": 0,
        "registrySha256": registry_sha256,
        "manifests": [{
            "schemaVersion": "agenttalk.adapter.v1",
            "id": "org.fixture.w5.acp",
            "displayName": "W5 ACP Fixture",
            "category": "agent_protocol",
            "protocol": {"kind": "acp", "major": 1},
            "match": {"executableNames": ["fixture-agent.exe"]},
            "launch": {
                "kind": "direct",
                "transport": "stdio",
                "executableRef": "matched-observation",
                "args": [mode],
                "environmentAllowlist": []
            },
            "verification": {"kind": "acp_initialize", "timeoutMs": 3000},
            "capabilityPolicy": {
                "filesystem": "forbidden",
                "shell": "forbidden",
                "streaming": "negotiate",
                "cancel": "negotiate"
            },
            "source": {
                "kind": "agenttalk_manifest",
                "id": "org.fixture.w5.acp",
                "version": "1",
                "revision": "w5-fixture",
                "catalogSha256": registry_sha256
            }
        }]
    });
    fs::write(
        &catalog,
        serde_json::to_vec(&value).expect("serialize W5 fixture catalog"),
    )
    .expect("write W5 fixture catalog");
    catalog
}

fn connect_authenticated(
    pipe: &str,
    credential: &str,
    client_id: &str,
    session_id: &str,
    owned_core: &mut OwnedReleaseCore,
) -> NamedPipeConnection {
    let deadline = Instant::now() + IPC_TIMEOUT;
    let mut client = None;
    while Instant::now() < deadline {
        if let Ok(connection) = NamedPipeClient::connect(pipe) {
            client = Some(connection);
            break;
        }
        thread::sleep(Duration::from_millis(25));
    }
    let mut client = client.unwrap_or_else(|| {
        panic!(
            "release Core did not create its Named Pipe: {}",
            owned_core.bounded_startup_diagnostic()
        )
    });
    client
        .write_json(&ProtocolHandshake {
            kind: "handshake".into(),
            protocol: ProtocolVersion {
                major: PROTOCOL_MAJOR,
                minor: 0,
            },
            client_id: client_id.into(),
            session_id: session_id.into(),
            session_credential: credential.into(),
            max_message_bytes: 1024 * 1024,
            last_seen: None,
        })
        .expect("send handshake");
    let response: ResponseEnvelope =
        serde_json::from_slice(&client.read_json().expect("read handshake response"))
            .expect("decode handshake response");
    assert!(response.ok, "release Core handshake must succeed");
    assert_eq!(response.payload["eventStreamId"], "core-events");
    client
}

fn send_command(
    client: &mut NamedPipeConnection,
    session_id: &str,
    request_id: &str,
    command: &str,
    payload: Value,
    deadline_ms: Option<u64>,
) {
    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: request_id.into(),
            session_id: session_id.into(),
            command: command.into(),
            payload,
            deadline_ms,
        })
        .expect("send W5 command");
}

fn send_query(
    client: &mut NamedPipeConnection,
    session_id: &str,
    request_id: &str,
    query: &str,
    payload: Value,
) {
    client
        .write_json(&QueryEnvelope {
            kind: "query".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: request_id.into(),
            session_id: session_id.into(),
            query: query.into(),
            payload,
        })
        .expect("send W5 query");
}

fn read_value(client: &mut NamedPipeConnection, context: &str) -> Value {
    let deadline = Instant::now() + IPC_TIMEOUT;
    loop {
        match client.try_read_json() {
            Ok(Some(bytes)) => {
                return serde_json::from_slice(&bytes)
                    .unwrap_or_else(|error| panic!("decode {context} JSON: {error}"));
            }
            Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(5)),
            Ok(None) => panic!("timed out waiting for {context}"),
            Err(error) => panic!("read {context}: {error}"),
        }
    }
}

fn read_response(client: &mut NamedPipeConnection, context: &str) -> ResponseEnvelope {
    let value = read_value(client, context);
    if value.get("kind").and_then(Value::as_str) == Some("error") {
        let error: ErrorEnvelope =
            serde_json::from_value(value).expect("decode unexpected W5 error");
        panic!("{context} unexpectedly failed with {}", error.code);
    }
    serde_json::from_value(value).unwrap_or_else(|error| panic!("decode {context}: {error}"))
}

fn read_error(client: &mut NamedPipeConnection, context: &str) -> ErrorEnvelope {
    let value = read_value(client, context);
    if value.get("kind").and_then(Value::as_str) == Some("response") {
        panic!("{context} unexpectedly returned a response");
    }
    serde_json::from_value(value).unwrap_or_else(|error| panic!("decode {context}: {error}"))
}

fn start_scan(
    client: &mut NamedPipeConnection,
    session_id: &str,
    request_id: &str,
) -> ResponseEnvelope {
    send_command(
        client,
        session_id,
        request_id,
        "agent.discovery.start",
        json!({}),
        Some(5_000),
    );
    let response = read_response(client, "agent.discovery.start");
    assert_eq!(response.request_id, request_id);
    assert_eq!(response.payload["accepted"], true);
    assert_eq!(response.payload["state"], "running");
    assert!(response.payload["scanId"].is_string());
    assert_eq!(
        response.payload["eventStream"]["streamId"],
        "local-discovery-events"
    );
    assert!(
        response.payload["eventStream"]["epoch"]
            .as_str()
            .is_some_and(|value| !value.is_empty()),
        "discovery start receipt must expose the current in-memory event stream epoch"
    );
    response
}

fn snapshot(
    client: &mut NamedPipeConnection,
    session_id: &str,
    request_id: &str,
    scan_id: &str,
) -> ResponseEnvelope {
    send_query(
        client,
        session_id,
        request_id,
        "agent.discovery.snapshot",
        json!({"scanId": scan_id}),
    );
    read_response(client, "agent.discovery.snapshot")
}

fn wait_for_completed_snapshot(
    client: &mut NamedPipeConnection,
    session_id: &str,
    scan_id: &str,
) -> Value {
    let deadline = Instant::now() + IPC_TIMEOUT;
    let mut request_number = 0u64;
    while Instant::now() < deadline {
        request_number += 1;
        let response = snapshot(
            client,
            session_id,
            &format!("snapshot-wait-{request_number}"),
            scan_id,
        );
        if response.payload["state"] != "running" {
            return response.payload;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("local discovery scan did not complete");
}

fn wait_for_candidate_lifecycle(
    client: &mut NamedPipeConnection,
    session_id: &str,
    scan_id: &str,
    candidate_id: &str,
    expected_lifecycle: &str,
) -> Value {
    let deadline = Instant::now() + IPC_TIMEOUT;
    let mut request_number = 0u64;
    loop {
        request_number += 1;
        let value = snapshot(
            client,
            session_id,
            &format!("snapshot-candidate-{expected_lifecycle}-{request_number}"),
            scan_id,
        )
        .payload;
        let candidate = value["candidates"]
            .as_array()
            .expect("snapshot candidates")
            .iter()
            .find(|candidate| candidate["candidateId"] == candidate_id)
            .unwrap_or_else(|| panic!("candidate {candidate_id} disappeared unexpectedly"));
        if candidate["lifecycleState"] == expected_lifecycle {
            return value;
        }
        assert!(
            Instant::now() < deadline,
            "candidate {candidate_id} did not reach {expected_lifecycle}; latest snapshot: {value}"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

fn classified_candidate(snapshot: &Value) -> &Value {
    snapshot["candidates"]
        .as_array()
        .expect("snapshot candidates array")
        .iter()
        .find(|candidate| candidate["candidate"]["runtimeType"] == "acp")
        .expect("fixture candidate must be ACP classified")
}

fn candidate_id(candidate: &Value) -> String {
    candidate["candidateId"]
        .as_str()
        .expect("candidate id")
        .to_owned()
}

fn verify_candidate(
    client: &mut NamedPipeConnection,
    session_id: &str,
    request_id: &str,
    scan_id: &str,
    candidate_id: &str,
    deadline_ms: u64,
) -> ResponseEnvelope {
    send_command(
        client,
        session_id,
        request_id,
        "agent.discovery.verify",
        json!({
            "scanId": scan_id,
            "candidateId": candidate_id,
            "consent": true,
            "deadlineMs": deadline_ms,
        }),
        Some(deadline_ms),
    );
    read_response(client, "agent.discovery.verify")
}

fn process_exists(pid: u32) -> bool {
    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE};
    use windows_sys::Win32::System::Threading::{
        GetExitCodeProcess, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
    };

    // SAFETY: process handle is queried only and closed exactly once.
    unsafe {
        let handle: HANDLE = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
        if handle.is_null() {
            return false;
        }
        let mut exit_code = 0u32;
        let queried = GetExitCodeProcess(handle, &mut exit_code);
        CloseHandle(handle);
        queried != 0 && exit_code == 259
    }
}

fn sqlite_state_digest(database: &Path) -> Vec<(String, Option<String>)> {
    ["", "-wal", "-shm"]
        .into_iter()
        .map(|suffix| {
            let path = PathBuf::from(format!("{}{}", database.display(), suffix));
            let digest = match fs::read(&path) {
                Ok(bytes) => Some(
                    Sha256::digest(bytes)
                        .iter()
                        .map(|byte| format!("{byte:02x}"))
                        .collect(),
                ),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => panic!("read isolated SQLite state {}: {error}", path.display()),
            };
            (suffix.into(), digest)
        })
        .collect()
}

fn wait_for_marker_pid_to_exit(path: &Path) {
    let pid: u32 = fs::read_to_string(path)
        .expect("read fixture owned PID marker")
        .trim()
        .parse()
        .expect("parse fixture owned PID marker");
    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline {
        if !process_exists(pid) {
            return;
        }
        thread::sleep(Duration::from_millis(25));
    }
    assert!(
        !process_exists(pid),
        "owned fixture process remains after cleanup"
    );
}

fn assert_safe_renderer_value(value: &Value, fixture: &ReleaseFixture) {
    let serialized = serde_json::to_string(value)
        .expect("serialize renderer-safe Named Pipe response")
        .to_ascii_lowercase();
    for forbidden in [
        fixture.fixture_root.to_string_lossy().to_ascii_lowercase(),
        fixture.credential.to_ascii_lowercase(),
        "authorization".into(),
        "cookie".into(),
        "runtime.json".into(),
        "fingerprint".into(),
        "locator".into(),
        "root.pid".into(),
        "descendant.pid".into(),
    ] {
        assert!(
            !serialized.contains(&forbidden),
            "renderer response included forbidden local-discovery value: {forbidden}"
        );
    }
}

fn shutdown(
    mut client: NamedPipeConnection,
    session_id: &str,
    request_id: &str,
    owned_core: &mut OwnedReleaseCore,
) {
    send_command(
        &mut client,
        session_id,
        request_id,
        "shutdown_owned",
        json!({}),
        None,
    );
    let response = read_response(&mut client, "shutdown_owned");
    assert_eq!(response.payload["shutdownAccepted"], true);
    drop(client);
    owned_core.wait_for_clean_exit();
}

fn create_import_project(
    client: &mut NamedPipeConnection,
    session_id: &str,
    request_id: &str,
    project_id: &str,
) {
    send_command(
        client,
        session_id,
        request_id,
        "project.create",
        json!({"projectId": project_id, "name": "W6 import fixture"}),
        None,
    );
    read_response(client, "project.create for local import");
}

fn import_local_agent(
    client: &mut NamedPipeConnection,
    session_id: &str,
    request_id: &str,
    scan_id: &str,
    candidate_id: &str,
    project_id: &str,
) -> ResponseEnvelope {
    send_command(
        client,
        session_id,
        request_id,
        "agent.import_local",
        json!({
            "scanId": scan_id,
            "candidateId": candidate_id,
            "projectId": project_id,
            "modelSelection": null,
        }),
        Some(5_000),
    );
    read_response(client, "agent.import_local")
}

#[test]
fn release_core_named_pipe_runs_scan_verify_plan_with_core_private_binding() {
    let _guard = suite_guard();
    let fixture = ReleaseFixture::create("success", true);
    let mut owned_core = fixture.spawn_core(None);
    let session_id = "session-w5-release-lifecycle-123456";
    let mut client = fixture.connect(session_id, &mut owned_core);
    let sqlite_before = sqlite_state_digest(&fixture.database);

    let start = start_scan(&mut client, session_id, "w5-start-lifecycle");
    let scan_id = start.payload["scanId"].as_str().expect("scanId").to_owned();
    let completed = wait_for_completed_snapshot(&mut client, session_id, &scan_id);
    assert_eq!(completed["state"], "completed");
    assert_safe_renderer_value(&completed, &fixture);
    let candidate = classified_candidate(&completed);
    assert_eq!(candidate["lifecycleState"], "identified");
    assert!(
        candidate.get("candidateBinding").is_none(),
        "candidate binding must remain Core-private"
    );

    let candidate_id = candidate["candidateId"]
        .as_str()
        .expect("classified candidate id")
        .to_owned();
    let verify = verify_candidate(
        &mut client,
        session_id,
        "w5-verify-lifecycle",
        &scan_id,
        &candidate_id,
        2_000,
    );
    assert_eq!(verify.payload["accepted"], true);
    let duplicate = verify_candidate(
        &mut client,
        session_id,
        "w5-verify-lifecycle",
        &scan_id,
        &candidate_id,
        2_000,
    );
    assert_eq!(duplicate.payload, verify.payload);
    let verified =
        wait_for_candidate_lifecycle(&mut client, session_id, &scan_id, &candidate_id, "verified");
    let verified_candidate = classified_candidate(&verified);
    assert_eq!(verified_candidate["lifecycleState"], "verified");
    assert_eq!(
        verified_candidate["verification"]["status"], "verified",
        "fixture accepts exactly one initialize and rejects session/prompt/tool calls"
    );
    assert_safe_renderer_value(&verified, &fixture);

    send_query(
        &mut client,
        session_id,
        "w5-import-plan",
        "agent.import.plan",
        json!({
            "scanId": scan_id,
            "candidateId": candidate_id,
            "projectId": "project-w5",
            "modelSelection": null,
        }),
    );
    let plan = read_response(&mut client, "agent.import.plan");
    assert_eq!(plan.payload["readOnly"], true);
    assert_eq!(plan.payload["modelPolicy"], "connector_default");
    assert_eq!(plan.payload["authRequired"], false);
    assert_safe_renderer_value(&plan.payload, &fixture);
    assert!(fixture.fixture_root.join("root.pid").is_file());
    wait_for_marker_pid_to_exit(&fixture.fixture_root.join("root.pid"));
    let invocation_count = fs::read_to_string(fixture.fixture_root.join("initialize.invocations"))
        .expect("read ACP fixture invocation ledger")
        .lines()
        .count();
    assert_eq!(
        invocation_count, 1,
        "reused verify requestId must not launch a duplicate ACP child"
    );

    send_query(
        &mut client,
        session_id,
        "w5-legacy-agent-scan",
        "agent.scan_local",
        json!({}),
    );
    let legacy = read_response(&mut client, "agent.scan_local");
    assert!(
        legacy.payload["discoveries"].is_array(),
        "legacy agent.scan_local must remain a compatible query"
    );
    assert!(legacy.payload.get("scanId").is_none());
    assert_eq!(
        sqlite_state_digest(&fixture.database),
        sqlite_before,
        "W5 passive scan, initialize-only verify, and import plan must not write SQLite"
    );

    shutdown(client, session_id, "w5-shutdown-lifecycle", &mut owned_core);
}

#[test]
fn release_core_named_pipe_imports_verified_local_agent_atomically_and_replays() {
    let _guard = suite_guard();
    let fixture = ReleaseFixture::create("success", false);
    let mut owned_core = fixture.spawn_core(None);
    let session_id = "session-w6-local-import-123456";
    let mut client = fixture.connect(session_id, &mut owned_core);
    create_import_project(
        &mut client,
        session_id,
        "w6-create-project",
        "project-w6-import",
    );
    let start = start_scan(&mut client, session_id, "w6-start");
    let scan_id = start.payload["scanId"]
        .as_str()
        .expect("scan id")
        .to_owned();
    let snapshot = wait_for_completed_snapshot(&mut client, session_id, &scan_id);
    let candidate_id = candidate_id(classified_candidate(&snapshot));
    verify_candidate(
        &mut client,
        session_id,
        "w6-verify",
        &scan_id,
        &candidate_id,
        2_000,
    );
    wait_for_candidate_lifecycle(&mut client, session_id, &scan_id, &candidate_id, "verified");
    wait_for_marker_pid_to_exit(&fixture.fixture_root.join("root.pid"));

    let imported = import_local_agent(
        &mut client,
        session_id,
        "w6-import",
        &scan_id,
        &candidate_id,
        "project-w6-import",
    );
    assert_eq!(imported.payload["reused"], false);
    assert!(imported.payload["agentId"].is_string());
    assert_eq!(imported.payload["projectId"], "project-w6-import");
    assert_safe_renderer_value(&imported.payload, &fixture);
    let replay = import_local_agent(
        &mut client,
        session_id,
        "w6-import",
        &scan_id,
        &candidate_id,
        "project-w6-import",
    );
    assert_eq!(replay.payload["reused"], true);
    assert_eq!(replay.payload["importId"], imported.payload["importId"]);

    let connection =
        rusqlite::Connection::open(&fixture.database).expect("open isolated fixture DB");
    for table in [
        "connector_adapter_bindings",
        "local_agent_imports",
        "project_agents",
    ] {
        let count: i64 = connection
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .expect("query import rows");
        assert_eq!(count, 1, "{table} must contain exactly one imported row");
    }
    let schema: String = connection
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'connector_adapter_bindings'",
            [],
            |row| row.get(0),
        )
        .expect("binding schema");
    let lower = schema.to_ascii_lowercase();
    for forbidden in [
        "token",
        "authorization",
        "cookie",
        "runtime_json",
        "pid",
        "port",
        "path",
        "environment",
    ] {
        assert!(
            !lower.contains(forbidden),
            "binding schema must not retain {forbidden}"
        );
    }
    shutdown(client, session_id, "w6-shutdown", &mut owned_core);
}

#[test]
fn release_core_named_pipe_rejects_unknown_missing_consent_dismissed_and_reused_requests() {
    let _guard = suite_guard();
    let fixture = ReleaseFixture::create("success", true);
    let mut owned_core = fixture.spawn_core(None);
    let session_id = "session-w5-release-negative-123456";
    let mut client = fixture.connect(session_id, &mut owned_core);
    let start = start_scan(&mut client, session_id, "w5-start-negative");
    let scan_id = start.payload["scanId"].as_str().expect("scanId").to_owned();
    let snapshot = wait_for_completed_snapshot(&mut client, session_id, &scan_id);
    let acp_candidate = classified_candidate(&snapshot);
    let acp_id = candidate_id(acp_candidate);
    let unknown = snapshot["candidates"]
        .as_array()
        .expect("snapshot candidates")
        .iter()
        .find(|candidate| candidate["lifecycleState"] == "adapter_required")
        .expect("unknown executable becomes adapter_required");
    let unknown_id = candidate_id(unknown);

    send_command(
        &mut client,
        session_id,
        "w5-unknown-verify",
        "agent.discovery.verify",
        json!({
            "scanId": scan_id,
            "candidateId": unknown_id,
            "consent": true
        }),
        None,
    );
    assert_eq!(
        read_error(&mut client, "unknown candidate verify").code,
        "DISCOVERY_ADAPTER_REQUIRED"
    );
    assert!(
        !fixture.fixture_root.join("root.pid").exists(),
        "unknown candidate must never start the ACP fixture"
    );

    send_command(
        &mut client,
        session_id,
        "w5-wrong-candidate",
        "agent.discovery.verify",
        json!({
            "scanId": scan_id,
            "candidateId": "candidate-does-not-exist",
            "consent": true,
        }),
        None,
    );
    assert_eq!(
        read_error(&mut client, "wrong candidate verify").code,
        "DISCOVERY_CANDIDATE_NOT_FOUND"
    );
    assert!(!fixture.fixture_root.join("root.pid").exists());

    send_command(
        &mut client,
        session_id,
        "w5-consent-required",
        "agent.discovery.verify",
        json!({
            "scanId": scan_id,
            "candidateId": acp_id,
            "consent": false
        }),
        None,
    );
    assert_eq!(
        read_error(&mut client, "missing consent").code,
        "DISCOVERY_CONSENT_REQUIRED"
    );
    assert!(!fixture.fixture_root.join("root.pid").exists());

    send_command(
        &mut client,
        session_id,
        "w5-dismiss",
        "agent.discovery.dismiss",
        json!({"scanId": scan_id, "candidateId": acp_id}),
        None,
    );
    assert!(read_response(&mut client, "dismiss candidate").payload["dismissed"] == true);

    send_command(
        &mut client,
        session_id,
        "w5-dismissed-verify",
        "agent.discovery.verify",
        json!({
            "scanId": scan_id,
            "candidateId": acp_id,
            "consent": true
        }),
        None,
    );
    assert_eq!(
        read_error(&mut client, "dismissed candidate verify").code,
        "DISCOVERY_CANDIDATE_DISMISSED"
    );

    send_command(
        &mut client,
        session_id,
        "w5-dismiss-reuse",
        "agent.discovery.dismiss",
        json!({"scanId": scan_id, "candidateId": unknown_id}),
        None,
    );
    let dismissed_unknown = read_response(&mut client, "dismiss unknown candidate");
    assert_eq!(dismissed_unknown.payload["dismissed"], true);
    send_command(
        &mut client,
        session_id,
        "w5-dismiss-reuse",
        "agent.discovery.dismiss",
        json!({"scanId": scan_id, "candidateId": acp_id}),
        None,
    );
    assert_eq!(
        read_error(&mut client, "request id reuse").code,
        "REQUEST_ID_REUSE"
    );

    send_query(
        &mut client,
        session_id,
        "w5-wrong-scan",
        "agent.discovery.snapshot",
        json!({"scanId": "scan-unknown"}),
    );
    assert_eq!(
        read_error(&mut client, "unknown scan snapshot").code,
        "DISCOVERY_SCAN_NOT_FOUND"
    );
    shutdown(client, session_id, "w5-shutdown-negative", &mut owned_core);
}

#[test]
fn release_core_named_pipe_rejects_cross_owner_discovery_access_and_event_replay() {
    let _guard = suite_guard();
    let fixture = ReleaseFixture::create("success", false);
    let mut owned_core = fixture.spawn_core(Some(4));
    let owner_a_client_id = "w51-owner-a";
    let owner_a_session_id = "session-w51-owner-a-123456";
    let owner_b_client_id = "w51-owner-b";
    let owner_b_session_id = "session-w51-owner-b-123456";
    let owner_c_session_id = "session-w51-owner-a-other-123456";
    let mut owner_a = fixture.connect_as(owner_a_client_id, owner_a_session_id, &mut owned_core);
    let mut owner_b = fixture.connect_as(owner_b_client_id, owner_b_session_id, &mut owned_core);
    let mut owner_c = fixture.connect_as(owner_a_client_id, owner_c_session_id, &mut owned_core);

    let started = start_scan(&mut owner_a, owner_a_session_id, "w51-owner-a-start");
    let scan_id = started.payload["scanId"]
        .as_str()
        .expect("owner A scan id")
        .to_owned();
    let owner_a_snapshot = wait_for_completed_snapshot(&mut owner_a, owner_a_session_id, &scan_id);
    let candidate_id = candidate_id(classified_candidate(&owner_a_snapshot));
    let owner_a_epoch = started.payload["eventStream"]["epoch"]
        .as_str()
        .expect("owner A event epoch")
        .to_owned();

    assert_cross_owner_operations_are_rejected(
        &mut owner_b,
        owner_b_session_id,
        "different-client",
        &scan_id,
        &candidate_id,
    );
    assert_cross_owner_operations_are_rejected(
        &mut owner_c,
        owner_c_session_id,
        "same-client-different-session",
        &scan_id,
        &candidate_id,
    );
    assert!(
        !fixture.fixture_root.join("root.pid").exists(),
        "cross-owner verification must fail before launching the retained ACP executable"
    );
    assert_eq!(
        snapshot(
            &mut owner_a,
            owner_a_session_id,
            "w51-owner-a-unchanged",
            &scan_id,
        )
        .payload,
        owner_a_snapshot,
        "cross-owner commands must not change the scan owner's state"
    );

    send_query(
        &mut owner_b,
        owner_b_session_id,
        "w51-owner-b-empty-events",
        "events.replay",
        json!({
            "streamId": "local-discovery-events",
            "afterSequence": 0,
            "limit": 16,
        }),
    );
    assert_eq!(
        read_error(&mut owner_b, "owner B empty event replay").code,
        "DISCOVERY_STREAM_NOT_FOUND"
    );
    send_query(
        &mut owner_b,
        owner_b_session_id,
        "w51-owner-b-a-epoch",
        "events.replay",
        json!({
            "streamId": "local-discovery-events",
            "epoch": owner_a_epoch,
            "afterSequence": 0,
            "limit": 16,
        }),
    );
    assert_eq!(
        read_error(&mut owner_b, "owner B using owner A event epoch").code,
        "DISCOVERY_STREAM_NOT_FOUND"
    );

    let mut owner_a_reconnect =
        fixture.connect_as(owner_a_client_id, owner_a_session_id, &mut owned_core);
    send_query(
        &mut owner_a_reconnect,
        owner_a_session_id,
        "w51-owner-a-wrong-epoch",
        "events.replay",
        json!({
            "streamId": "local-discovery-events",
            "epoch": "local-discovery-wrong-epoch",
            "afterSequence": 0,
            "limit": 16,
        }),
    );
    assert_eq!(
        read_error(&mut owner_a_reconnect, "owner A wrong discovery epoch").code,
        "INVALID_QUERY"
    );
    send_query(
        &mut owner_a_reconnect,
        owner_a_session_id,
        "w51-owner-a-reconnect-events",
        "events.replay",
        json!({
            "streamId": "local-discovery-events",
            "epoch": owner_a_epoch,
            "afterSequence": 0,
            "limit": 16,
        }),
    );
    let owner_a_replay = read_response(&mut owner_a_reconnect, "owner A reconnect event replay");
    let replay_json = owner_a_replay.payload.to_string();
    assert!(replay_json.contains(&scan_id));
    assert!(replay_json.contains(&candidate_id));
    assert!(!replay_json.contains(&fixture.credential));

    assert_eq!(
        start_scan(
            &mut owner_a_reconnect,
            owner_a_session_id,
            "w51-owner-a-start"
        )
        .payload["scanId"],
        scan_id,
        "same owner and requestId must replay the original scan receipt"
    );
    let accepted = verify_candidate(
        &mut owner_a_reconnect,
        owner_a_session_id,
        "w51-owner-a-verify",
        &scan_id,
        &candidate_id,
        1_000,
    );
    assert_eq!(accepted.payload["accepted"], true);
    send_command(
        &mut owner_a_reconnect,
        owner_a_session_id,
        "w51-owner-a-verify",
        "agent.discovery.verify",
        json!({
            "scanId": scan_id,
            "candidateId": candidate_id,
            // A business-intent change (consent) is what distinguishes the
            // requestId, not the per-attempt deadline budget.
            "consent": false,
            "deadlineMs": 900,
        }),
        Some(900),
    );
    assert_eq!(
        read_error(&mut owner_a_reconnect, "same-owner request ID reuse").code,
        "REQUEST_ID_REUSE"
    );
    let _ = wait_for_candidate_lifecycle(
        &mut owner_a_reconnect,
        owner_a_session_id,
        &scan_id,
        &candidate_id,
        "verified",
    );
    send_query(
        &mut owner_a_reconnect,
        owner_a_session_id,
        "w51-owner-a-gap",
        "events.replay",
        json!({
            "streamId": "local-discovery-events",
            "afterSequence": 0,
            "limit": 16,
        }),
    );
    assert_eq!(
        read_error(&mut owner_a_reconnect, "owner A replay gap").code,
        "REPLAY_GAP"
    );
    let owner_a_recovered = snapshot(
        &mut owner_a_reconnect,
        owner_a_session_id,
        "w51-owner-a-recovery-snapshot",
        &scan_id,
    );
    assert_eq!(
        classified_candidate(&owner_a_recovered.payload)["lifecycleState"],
        "verified"
    );
    send_query(
        &mut owner_a_reconnect,
        owner_a_session_id,
        "w51-owner-a-head-before-b-overflow",
        "events.replay",
        json!({
            "streamId": "local-discovery-events",
            "afterSequence": 1,
            "limit": 16,
        }),
    );
    let owner_a_head = read_response(&mut owner_a_reconnect, "owner A current stream head").payload
        ["headSequence"]
        .as_u64()
        .expect("owner A stream head");
    assert_ne!(
        start_scan(&mut owner_c, owner_c_session_id, "w51-owner-a-start").payload["scanId"],
        scan_id,
        "the same clientId on another authenticated session must own an independent receipt scope"
    );

    let owner_b_started = start_scan(&mut owner_b, owner_b_session_id, "w51-owner-a-start");
    assert_ne!(
        owner_b_started.payload["scanId"], scan_id,
        "a non-owner copying A's requestId must create its own receipt and never replay A's scan"
    );
    let owner_b_epoch = owner_b_started.payload["eventStream"]["epoch"]
        .as_str()
        .expect("owner B event epoch")
        .to_owned();
    let owner_b_scan_id = owner_b_started.payload["scanId"]
        .as_str()
        .expect("owner B scan id")
        .to_owned();
    let _ = wait_for_completed_snapshot(&mut owner_b, owner_b_session_id, &owner_b_scan_id);
    let mut owner_b_subscription =
        fixture.connect_as(owner_b_client_id, owner_b_session_id, &mut owned_core);
    send_command(
        &mut owner_b_subscription,
        owner_b_session_id,
        "w51-owner-b-subscribe",
        "events.subscribe",
        json!({
            "afterCursor": {
                "streamId": "local-discovery-events",
                "sequence": 0,
                "epoch": owner_b_epoch,
            },
            "maxInFlightEvents": 1,
            "maxInFlightBytes": 262_144,
        }),
        None,
    );
    let subscription = read_response(&mut owner_b_subscription, "owner B discovery subscription");
    let subscription_id = subscription.payload["subscriptionId"]
        .as_str()
        .expect("owner B subscription ID")
        .to_owned();
    let owner_b_event: agenttalk_protocols::EventEnvelope =
        serde_json::from_value(read_value(&mut owner_b_subscription, "owner B event"))
            .expect("decode owner B event");
    let owner_b_event_json = serde_json::to_value(owner_b_event)
        .expect("serialize owner B renderer-safe event")
        .to_string();
    assert!(!owner_b_event_json.contains(&scan_id));
    assert!(!owner_b_event_json.contains(&candidate_id));
    assert!(!owner_b_event_json.contains(owner_a_client_id));
    assert!(!owner_b_event_json.contains(owner_a_session_id));
    assert!(!owner_b_event_json.contains(&fixture.credential));
    send_command(
        &mut owner_b_subscription,
        owner_b_session_id,
        "w51-owner-b-ack-a-cursor",
        "events.ack",
        json!({
            "subscriptionId": subscription_id,
            "cursor": {
                "streamId": "local-discovery-events",
                "sequence": 1,
                "epoch": owner_a_epoch,
            },
        }),
        None,
    );
    assert_eq!(
        read_error(
            &mut owner_b_subscription,
            "owner B ACK using owner A cursor"
        )
        .code,
        "INVALID_ACK"
    );
    drop(owner_b_subscription);
    for number in 1..=2 {
        let started = start_scan(
            &mut owner_b,
            owner_b_session_id,
            &format!("w51-owner-b-start-{number}"),
        );
        let scan_id = started.payload["scanId"]
            .as_str()
            .expect("owner B additional scan ID")
            .to_owned();
        let _ = wait_for_completed_snapshot(&mut owner_b, owner_b_session_id, &scan_id);
    }
    send_query(
        &mut owner_b,
        owner_b_session_id,
        "w51-owner-b-gap",
        "events.replay",
        json!({
            "streamId": "local-discovery-events",
            "afterSequence": 0,
            "limit": 16,
        }),
    );
    assert_eq!(
        read_error(&mut owner_b, "owner B independent replay gap").code,
        "REPLAY_GAP"
    );
    let owner_b_recovered = snapshot(
        &mut owner_b,
        owner_b_session_id,
        "w51-owner-b-recovery-snapshot",
        &owner_b_scan_id,
    );
    assert_eq!(owner_b_recovered.payload["state"], "completed");
    assert_safe_renderer_value(&owner_b_recovered.payload, &fixture);
    send_query(
        &mut owner_a_reconnect,
        owner_a_session_id,
        "w51-owner-a-still-replays",
        "events.replay",
        json!({
            "streamId": "local-discovery-events",
            "afterSequence": owner_a_head,
            "limit": 16,
        }),
    );
    let owner_a_after_b_overflow =
        read_response(&mut owner_a_reconnect, "owner A independent replay").payload;
    assert_eq!(owner_a_after_b_overflow["headSequence"], owner_a_head);
    assert_eq!(owner_a_after_b_overflow["events"], json!([]));

    drop(owner_b);
    drop(owner_c);
    drop(owner_a_reconnect);
    shutdown(
        owner_a,
        owner_a_session_id,
        "w51-owner-a-shutdown",
        &mut owned_core,
    );
}

#[test]
fn release_core_named_pipe_bounds_discovery_event_stream_owners() {
    let _guard = suite_guard();
    let fixture = ReleaseFixture::create("success", false);
    let mut owned_core =
        fixture.spawn_core_with_discovery_stream_limits(Some(4), Some(2), Some(600_000));
    let owner_a_client_id = "w52-owner-a";
    let owner_a_session_id = "session-w52-owner-a-123456";
    let flood_b_session_id = "session-w52-flood-b-123456";
    let flood_c_session_id = "session-w52-flood-c-123456";
    let flood_d_session_id = "session-w52-flood-d-123456";
    let mut owner_a = fixture.connect_as(owner_a_client_id, owner_a_session_id, &mut owned_core);
    let mut flood_b = fixture.connect_as("w52-flood-b", flood_b_session_id, &mut owned_core);
    let mut flood_c = fixture.connect_as("w52-flood-c", flood_c_session_id, &mut owned_core);
    let mut flood_d = fixture.connect_as("w52-flood-d", flood_d_session_id, &mut owned_core);

    send_query(
        &mut flood_b,
        flood_b_session_id,
        "w52-empty-replay",
        "events.replay",
        json!({
            "streamId": "local-discovery-events",
            "afterSequence": 0,
            "limit": 4,
        }),
    );
    assert_eq!(
        read_error(&mut flood_b, "empty owner discovery replay").code,
        "DISCOVERY_STREAM_NOT_FOUND"
    );
    send_command(
        &mut flood_b,
        flood_b_session_id,
        "w52-empty-subscribe",
        "events.subscribe",
        json!({
            "streamId": "local-discovery-events",
            "afterSequence": 0,
            "maxInFlightEvents": 1,
            "maxInFlightBytes": 262_144,
        }),
        None,
    );
    assert_eq!(
        read_error(&mut flood_b, "empty owner discovery subscribe").code,
        "DISCOVERY_STREAM_NOT_FOUND"
    );

    let owner_a_start = start_scan(&mut owner_a, owner_a_session_id, "w52-owner-a-start");
    let owner_a_scan_id = owner_a_start.payload["scanId"]
        .as_str()
        .expect("owner A scan id")
        .to_owned();
    let owner_a_epoch = owner_a_start.payload["eventStream"]["epoch"]
        .as_str()
        .expect("owner A discovery epoch")
        .to_owned();
    let _ = wait_for_completed_snapshot(&mut owner_a, owner_a_session_id, &owner_a_scan_id);

    let flood_c_start = start_scan(&mut flood_c, flood_c_session_id, "w52-flood-c-start");
    let flood_c_scan_id = flood_c_start.payload["scanId"]
        .as_str()
        .expect("flood C scan id")
        .to_owned();
    let _ = wait_for_completed_snapshot(&mut flood_c, flood_c_session_id, &flood_c_scan_id);

    send_command(
        &mut flood_d,
        flood_d_session_id,
        "w52-flood-d-start",
        "agent.discovery.start",
        json!({}),
        Some(5_000),
    );
    assert_eq!(
        read_error(&mut flood_d, "discovery owner stream capacity").code,
        "DISCOVERY_STREAM_CAPACITY_EXHAUSTED"
    );

    send_query(
        &mut owner_a,
        owner_a_session_id,
        "w52-owner-a-replay-after-flood",
        "events.replay",
        json!({
            "streamId": "local-discovery-events",
            "epoch": owner_a_epoch,
            "afterSequence": 0,
            "limit": 16,
        }),
    );
    let owner_a_replay = read_response(&mut owner_a, "owner A replay after flood");
    let replay_json = owner_a_replay.payload.to_string();
    assert!(replay_json.contains(&owner_a_scan_id));
    assert!(!replay_json.contains(flood_b_session_id));
    assert!(!replay_json.contains(flood_c_session_id));
    assert!(!replay_json.contains(flood_d_session_id));
    assert!(!replay_json.contains(&fixture.credential));

    drop(flood_b);
    drop(flood_c);
    drop(flood_d);
    shutdown(
        owner_a,
        owner_a_session_id,
        "w52-owner-a-shutdown",
        &mut owned_core,
    );
}

#[test]
fn release_core_named_pipe_bounds_same_owner_discovery_scans() {
    let _guard = suite_guard();
    let fixture = ReleaseFixture::create("success", false);
    let mut owned_core = fixture.spawn_core_with_discovery_limits(
        Some(16),
        Some(8),
        Some(600_000),
        DiscoveryLimitOverrides {
            max_sessions_per_owner: Some(2),
            max_sessions_global: Some(4),
            max_running_scans_per_owner: Some(2),
            max_running_scans_global: Some(4),
            ..DiscoveryLimitOverrides::default()
        },
    );
    let owner_session_id = "session-w53-owner-123456";
    let other_session_id = "session-w53-other-123456";
    let mut owner = fixture.connect_as("w53-owner", owner_session_id, &mut owned_core);
    let mut other = fixture.connect_as("w53-other", other_session_id, &mut owned_core);

    let first = start_scan(&mut owner, owner_session_id, "w53-owner-start-1");
    let first_scan_id = first.payload["scanId"]
        .as_str()
        .expect("first scan")
        .to_owned();
    let discovery_epoch = first.payload["eventStream"]["epoch"]
        .as_str()
        .expect("discovery epoch")
        .to_owned();
    let _ = wait_for_completed_snapshot(&mut owner, owner_session_id, &first_scan_id);
    let second = start_scan(&mut owner, owner_session_id, "w53-owner-start-2");
    let second_scan_id = second.payload["scanId"]
        .as_str()
        .expect("second scan")
        .to_owned();
    let _ = wait_for_completed_snapshot(&mut owner, owner_session_id, &second_scan_id);

    // Terminal sessions are retained for replay only until the per-owner
    // retention boundary. Starting a third scan must evict the oldest
    // completed session instead of reporting capacity exhaustion.
    let third = start_scan(&mut owner, owner_session_id, "w53-owner-start-3");
    let third_scan_id = third.payload["scanId"]
        .as_str()
        .expect("third scan")
        .to_owned();
    let _ = wait_for_completed_snapshot(&mut owner, owner_session_id, &third_scan_id);

    send_query(
        &mut owner,
        owner_session_id,
        "w53-owner-replay-after-eviction",
        "events.replay",
        json!({
            "streamId": "local-discovery-events",
            "epoch": discovery_epoch,
            "afterSequence": 0,
            "limit": 16,
        }),
    );
    let replay = read_response(&mut owner, "owner replay after terminal eviction");
    let replay_json = replay.payload.to_string();
    assert!(replay_json.contains(&first_scan_id));
    assert!(replay_json.contains(&second_scan_id));
    assert!(replay_json.contains(&third_scan_id));
    assert!(!replay_json.contains(&fixture.credential));

    send_query(
        &mut owner,
        owner_session_id,
        "w53-owner-evicted-snapshot",
        "agent.discovery.snapshot",
        json!({"scanId": first_scan_id}),
    );
    let evicted = read_error(&mut owner, "evicted terminal session snapshot");
    assert_eq!(evicted.code, "DISCOVERY_SCAN_NOT_FOUND");
    assert!(!evicted.retryable);
    assert_safe_renderer_value(
        &serde_json::to_value(&evicted).expect("error json"),
        &fixture,
    );

    let replacement = start_scan(&mut owner, owner_session_id, "w53-owner-start-1");
    assert_ne!(replacement.payload["scanId"], first_scan_id);

    send_command(
        &mut owner,
        owner_session_id,
        "w53-owner-start-1",
        "agent.discovery.start",
        json!({"unexpected": true}),
        Some(5_000),
    );
    assert_eq!(
        read_error(
            &mut owner,
            "same owner requestId reused with different payload"
        )
        .code,
        "REQUEST_ID_REUSE"
    );

    let other_start = start_scan(&mut other, other_session_id, "w53-other-start");
    let other_scan_id = other_start.payload["scanId"]
        .as_str()
        .expect("other owner scan")
        .to_owned();
    let other_snapshot = wait_for_completed_snapshot(&mut other, other_session_id, &other_scan_id);
    assert_eq!(other_snapshot["state"], "completed");

    shutdown(
        owner,
        owner_session_id,
        "w53-owner-shutdown",
        &mut owned_core,
    );
}

fn assert_cross_owner_operations_are_rejected(
    client: &mut NamedPipeConnection,
    session_id: &str,
    label: &str,
    scan_id: &str,
    candidate_id: &str,
) {
    send_query(
        client,
        session_id,
        &format!("w51-{label}-snapshot"),
        "agent.discovery.snapshot",
        json!({"scanId": scan_id}),
    );
    assert_eq!(
        read_error(client, "cross-owner discovery snapshot").code,
        "DISCOVERY_SCAN_NOT_FOUND",
        "{label} must not learn another owner's retained scan"
    );
    send_command(
        client,
        session_id,
        &format!("w51-{label}-verify"),
        "agent.discovery.verify",
        json!({
            "scanId": scan_id,
            "candidateId": candidate_id,
            "consent": true,
            "deadlineMs": 1_000,
        }),
        Some(1_000),
    );
    assert_eq!(
        read_error(client, "cross-owner verification").code,
        "DISCOVERY_SCAN_NOT_FOUND"
    );
    send_command(
        client,
        session_id,
        &format!("w51-{label}-dismiss"),
        "agent.discovery.dismiss",
        json!({"scanId": scan_id, "candidateId": candidate_id}),
        None,
    );
    assert_eq!(
        read_error(client, "cross-owner dismissal").code,
        "DISCOVERY_SCAN_NOT_FOUND"
    );
    send_query(
        client,
        session_id,
        &format!("w51-{label}-plan"),
        "agent.import.plan",
        json!({
            "scanId": scan_id,
            "candidateId": candidate_id,
            "projectId": "project-w51-isolation",
        }),
    );
    assert_eq!(
        read_error(client, "cross-owner import plan").code,
        "DISCOVERY_SCAN_NOT_FOUND"
    );
}

#[test]
fn release_core_named_pipe_projects_typed_acp_outcomes_and_cleans_owned_children() {
    let _guard = suite_guard();
    for (mode, deadline_ms, lifecycle, diagnostic) in [
        (
            "unsupported-major",
            1_000,
            "protocol_mismatch",
            "protocol_mismatch",
        ),
        (
            "auth-required",
            1_000,
            "auth_required",
            "authentication_required",
        ),
        (
            "stdout-pollution",
            1_000,
            "not_verified",
            "protocol_violation",
        ),
        ("oversized", 1_000, "not_verified", "oversized_frame"),
        ("truncated", 1_000, "not_verified", "non_utf8_frame"),
        (
            "duplicate-response",
            1_000,
            "not_verified",
            "protocol_violation",
        ),
        ("stderr", 1_000, "not_verified", "stderr_output"),
        ("crash", 1_000, "not_verified", "process_failed"),
        ("timeout", 250, "timeout", "timeout"),
    ] {
        let fixture = ReleaseFixture::create(mode, false);
        let mut owned_core = fixture.spawn_core(None);
        let session_id = "session-w5-release-typed-outcomes-123456";
        let mut client = fixture.connect(session_id, &mut owned_core);
        let started = start_scan(&mut client, session_id, &format!("w5-start-{mode}"));
        let scan_id = started.payload["scanId"]
            .as_str()
            .expect("scan id")
            .to_owned();
        let discovered = wait_for_completed_snapshot(&mut client, session_id, &scan_id);
        let candidate_id = candidate_id(classified_candidate(&discovered));
        let accepted = verify_candidate(
            &mut client,
            session_id,
            &format!("w5-verify-{mode}"),
            &scan_id,
            &candidate_id,
            deadline_ms,
        );
        assert_eq!(accepted.payload["accepted"], true);
        let completed = wait_for_candidate_lifecycle(
            &mut client,
            session_id,
            &scan_id,
            &candidate_id,
            lifecycle,
        );
        let candidate = completed["candidates"]
            .as_array()
            .expect("candidate list")
            .iter()
            .find(|candidate| candidate["candidateId"] == candidate_id)
            .expect("classified candidate remains in snapshot");
        assert_eq!(candidate["verification"]["diagnostic"], diagnostic);
        assert_safe_renderer_value(&completed, &fixture);
        send_query(
            &mut client,
            session_id,
            &format!("w5-events-{mode}"),
            "events.replay",
            json!({
                "streamId": "local-discovery-events",
                "afterSequence": 0,
                "limit": 32,
            }),
        );
        let replay = read_response(&mut client, "typed ACP lifecycle event replay");
        assert!(
            replay.payload["events"]
                .as_array()
                .expect("replayed event array")
                .iter()
                .any(|event| {
                    event["event"] == "agent.discovery.candidate_verified"
                        && event["payload"]["candidateId"] == candidate_id
                        && event["payload"]["status"] == lifecycle
                        && event["payload"]["diagnostic"] == diagnostic
                }),
            "typed ACP outcome must be visible in the retained safe event stream"
        );
        if mode == "auth-required" {
            send_query(
                &mut client,
                session_id,
                "w5-auth-required-plan",
                "agent.import.plan",
                json!({
                    "scanId": scan_id,
                    "candidateId": candidate_id,
                    "projectId": "project-w5-auth",
                }),
            );
            let plan = read_response(&mut client, "auth-required import plan");
            assert_eq!(plan.payload["readOnly"], true);
            assert_eq!(plan.payload["authRequired"], true);
        }
        wait_for_marker_pid_to_exit(&fixture.fixture_root.join("root.pid"));
        shutdown(
            client,
            session_id,
            &format!("w5-shutdown-{mode}"),
            &mut owned_core,
        );
    }
}

#[test]
fn release_core_named_pipe_rechecks_identity_cancels_owned_tree_and_invalidates_restarts() {
    let _guard = suite_guard();
    let fixture = ReleaseFixture::create("success", false);
    let session_id = "session-w5-release-identity-cancel-123456";
    let mut first_core = fixture.spawn_core(None);
    let mut first_client = fixture.connect(session_id, &mut first_core);
    let start = start_scan(&mut first_client, session_id, "w5-identity-start");
    let first_discovery_epoch = start.payload["eventStream"]["epoch"]
        .as_str()
        .expect("first Core discovery epoch")
        .to_owned();
    let scan_id = start.payload["scanId"]
        .as_str()
        .expect("scan id")
        .to_owned();
    let scan = wait_for_completed_snapshot(&mut first_client, session_id, &scan_id);
    let identity_candidate_id = candidate_id(classified_candidate(&scan));

    let mut executable = OpenOptions::new()
        .append(true)
        .open(fixture.fixture_root.join("fixture-agent.exe"))
        .expect("open fixture for controlled identity replacement");
    executable
        .write_all(b"w5 controlled identity replacement")
        .expect("change fixture content after passive scan");
    executable.flush().expect("flush replacement");
    let accepted = verify_candidate(
        &mut first_client,
        session_id,
        "w5-identity-verify",
        &scan_id,
        &identity_candidate_id,
        1_000,
    );
    assert_eq!(accepted.payload["accepted"], true);
    let identity_changed = wait_for_candidate_lifecycle(
        &mut first_client,
        session_id,
        &scan_id,
        &identity_candidate_id,
        "identity_changed",
    );
    assert_eq!(
        classified_candidate(&identity_changed)["verification"]["diagnostic"],
        "identity_mismatch"
    );
    assert!(
        !fixture.fixture_root.join("root.pid").exists(),
        "identity mismatch must be rejected before launch"
    );

    shutdown(
        first_client,
        session_id,
        "w5-identity-shutdown",
        &mut first_core,
    );
    let mut restarted_core = fixture.spawn_core(None);
    let mut restarted_client = fixture.connect(session_id, &mut restarted_core);
    send_query(
        &mut restarted_client,
        session_id,
        "w5-restart-old-scan",
        "agent.discovery.snapshot",
        json!({"scanId": scan_id}),
    );
    assert_eq!(
        read_error(&mut restarted_client, "old scan after Core restart").code,
        "DISCOVERY_SCAN_NOT_FOUND"
    );
    send_query(
        &mut restarted_client,
        session_id,
        "w5-restart-old-discovery-epoch",
        "events.replay",
        json!({
            "streamId": "local-discovery-events",
            "epoch": first_discovery_epoch,
            "afterSequence": 0,
            "limit": 16,
        }),
    );
    assert_eq!(
        read_error(
            &mut restarted_client,
            "old discovery epoch after Core restart"
        )
        .code,
        "DISCOVERY_STREAM_NOT_FOUND"
    );
    shutdown(
        restarted_client,
        session_id,
        "w5-restart-shutdown",
        &mut restarted_core,
    );

    let cancellation_fixture = ReleaseFixture::create("spawn-child-timeout", false);
    let mut cancellation_core = cancellation_fixture.spawn_core(None);
    let mut cancellation_client = cancellation_fixture.connect(session_id, &mut cancellation_core);
    let start = start_scan(&mut cancellation_client, session_id, "w5-cancel-start");
    let cancel_scan_id = start.payload["scanId"]
        .as_str()
        .expect("scan id")
        .to_owned();
    let scan = wait_for_completed_snapshot(&mut cancellation_client, session_id, &cancel_scan_id);
    let cancel_candidate_id = candidate_id(classified_candidate(&scan));
    let accepted = verify_candidate(
        &mut cancellation_client,
        session_id,
        "w5-cancel-verify",
        &cancel_scan_id,
        &cancel_candidate_id,
        2_000,
    );
    assert_eq!(accepted.payload["accepted"], true);
    let root_marker = cancellation_fixture.fixture_root.join("root.pid");
    let descendant_marker = cancellation_fixture.fixture_root.join("descendant.pid");
    let start_deadline = Instant::now() + IPC_TIMEOUT;
    while (!root_marker.is_file() || !descendant_marker.is_file())
        && Instant::now() < start_deadline
    {
        thread::sleep(Duration::from_millis(10));
    }
    assert!(root_marker.is_file(), "cancellable fixture root must start");
    assert!(
        descendant_marker.is_file(),
        "cancellable fixture must prove it created a real descendant"
    );
    send_command(
        &mut cancellation_client,
        session_id,
        "w5-cancel-dismiss",
        "agent.discovery.dismiss",
        json!({"scanId": cancel_scan_id, "candidateId": cancel_candidate_id}),
        None,
    );
    assert_eq!(
        read_response(&mut cancellation_client, "dismiss cancelling candidate").payload
            ["dismissed"],
        true
    );
    let after_dismiss = snapshot(
        &mut cancellation_client,
        session_id,
        "w5-cancel-snapshot",
        &cancel_scan_id,
    );
    assert!(
        after_dismiss.payload["candidates"]
            .as_array()
            .expect("candidate array")
            .iter()
            .all(|candidate| candidate["candidateId"] != cancel_candidate_id),
        "dismissal must hide the session-local candidate"
    );
    wait_for_marker_pid_to_exit(&root_marker);
    wait_for_marker_pid_to_exit(&descendant_marker);
    shutdown(
        cancellation_client,
        session_id,
        "w5-cancel-shutdown",
        &mut cancellation_core,
    );

    let shutdown_fixture = ReleaseFixture::create("spawn-child-timeout", false);
    let mut shutdown_core = shutdown_fixture.spawn_core(None);
    let mut shutdown_client = shutdown_fixture.connect(session_id, &mut shutdown_core);
    let start = start_scan(&mut shutdown_client, session_id, "w5-shutdown-cancel-start");
    let shutdown_scan_id = start.payload["scanId"]
        .as_str()
        .expect("scan id")
        .to_owned();
    let scan = wait_for_completed_snapshot(&mut shutdown_client, session_id, &shutdown_scan_id);
    let shutdown_candidate_id = candidate_id(classified_candidate(&scan));
    let accepted = verify_candidate(
        &mut shutdown_client,
        session_id,
        "w5-shutdown-cancel-verify",
        &shutdown_scan_id,
        &shutdown_candidate_id,
        2_000,
    );
    assert_eq!(accepted.payload["accepted"], true);
    let shutdown_root = shutdown_fixture.fixture_root.join("root.pid");
    let shutdown_descendant = shutdown_fixture.fixture_root.join("descendant.pid");
    let start_deadline = Instant::now() + IPC_TIMEOUT;
    while (!shutdown_root.is_file() || !shutdown_descendant.is_file())
        && Instant::now() < start_deadline
    {
        thread::sleep(Duration::from_millis(10));
    }
    assert!(shutdown_root.is_file(), "shutdown fixture root must start");
    assert!(
        shutdown_descendant.is_file(),
        "shutdown fixture must prove it created a real descendant"
    );
    shutdown(
        shutdown_client,
        session_id,
        "w5-shutdown-cancel",
        &mut shutdown_core,
    );
    wait_for_marker_pid_to_exit(&shutdown_root);
    wait_for_marker_pid_to_exit(&shutdown_descendant);
}

#[test]
fn release_core_named_pipe_rejects_import_plan_after_verified_identity_changes() {
    let _guard = suite_guard();
    let fixture = ReleaseFixture::create("success", false);
    let session_id = "session-w5-release-import-recheck-123456";
    let mut owned_core = fixture.spawn_core(None);
    let mut client = fixture.connect(session_id, &mut owned_core);
    let start = start_scan(&mut client, session_id, "w5-import-recheck-start");
    let scan_id = start.payload["scanId"]
        .as_str()
        .expect("scan id")
        .to_owned();
    let scan = wait_for_completed_snapshot(&mut client, session_id, &scan_id);
    let candidate_id = candidate_id(classified_candidate(&scan));
    let accepted = verify_candidate(
        &mut client,
        session_id,
        "w5-import-recheck-verify",
        &scan_id,
        &candidate_id,
        1_000,
    );
    assert_eq!(accepted.payload["accepted"], true);
    let verified =
        wait_for_candidate_lifecycle(&mut client, session_id, &scan_id, &candidate_id, "verified");
    assert_eq!(
        classified_candidate(&verified)["verification"]["status"],
        "verified"
    );

    let mut executable = OpenOptions::new()
        .append(true)
        .open(fixture.fixture_root.join("fixture-agent.exe"))
        .expect("open fixture for controlled post-verification identity change");
    executable
        .write_all(b"w5 post-verification identity change")
        .expect("change fixture after initialize-only verification");
    executable.flush().expect("flush changed fixture");

    send_query(
        &mut client,
        session_id,
        "w5-import-recheck-plan",
        "agent.import.plan",
        json!({
            "scanId": scan_id,
            "candidateId": candidate_id,
            "projectId": "project-w5-stale-plan",
        }),
    );
    assert_eq!(
        read_error(&mut client, "stale import plan").code,
        "DISCOVERY_IDENTITY_CHANGED"
    );
    shutdown(
        client,
        session_id,
        "w5-import-recheck-shutdown",
        &mut owned_core,
    );
}

#[test]
fn release_core_named_pipe_rolls_back_local_import_after_verified_identity_changes() {
    let _guard = suite_guard();
    let fixture = ReleaseFixture::create("success", false);
    let session_id = "session-w6-release-import-recheck-123456";
    let mut owned_core = fixture.spawn_core(None);
    let mut client = fixture.connect(session_id, &mut owned_core);
    create_import_project(
        &mut client,
        session_id,
        "w6-import-recheck-project",
        "project-w6-stale-import",
    );
    let start = start_scan(&mut client, session_id, "w6-import-recheck-start");
    let scan_id = start.payload["scanId"]
        .as_str()
        .expect("scan id")
        .to_owned();
    let scan = wait_for_completed_snapshot(&mut client, session_id, &scan_id);
    let candidate_id = candidate_id(classified_candidate(&scan));
    verify_candidate(
        &mut client,
        session_id,
        "w6-import-recheck-verify",
        &scan_id,
        &candidate_id,
        1_000,
    );
    wait_for_candidate_lifecycle(&mut client, session_id, &scan_id, &candidate_id, "verified");

    let mut executable = OpenOptions::new()
        .append(true)
        .open(fixture.fixture_root.join("fixture-agent.exe"))
        .expect("open fixture for controlled post-verification identity change");
    executable
        .write_all(b"w6 post-verification identity change")
        .expect("change fixture after verification");
    executable.flush().expect("flush changed fixture");
    send_command(
        &mut client,
        session_id,
        "w6-import-recheck-command",
        "agent.import_local",
        json!({
            "scanId": scan_id,
            "candidateId": candidate_id,
            "projectId": "project-w6-stale-import",
        }),
        Some(5_000),
    );
    assert_eq!(
        read_error(&mut client, "stale local import").code,
        "DISCOVERY_IDENTITY_CHANGED"
    );
    let connection =
        rusqlite::Connection::open(&fixture.database).expect("open isolated fixture DB");
    for table in [
        "connector_adapter_bindings",
        "local_agent_imports",
        "agents",
        "project_agents",
    ] {
        let count: i64 = connection
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .expect("query rollback rows");
        assert_eq!(
            count, 0,
            "{table} must remain empty after identity rejection"
        );
    }
    shutdown(
        client,
        session_id,
        "w6-import-recheck-shutdown",
        &mut owned_core,
    );
}

#[test]
fn release_core_named_pipe_import_conflicts_and_failures_are_typed() {
    let _guard = suite_guard();
    let fixture = ReleaseFixture::create("success", false);
    let mut owned_core = fixture.spawn_core(None);
    let session_id = "session-w61-import-errors-123456";
    let mut client = fixture.connect(session_id, &mut owned_core);
    create_import_project(
        &mut client,
        session_id,
        "w61-create-project",
        "project-w61-import",
    );
    let start = start_scan(&mut client, session_id, "w61-import-errors-start");
    let scan_id = start.payload["scanId"]
        .as_str()
        .expect("scan id")
        .to_owned();
    let snapshot = wait_for_completed_snapshot(&mut client, session_id, &scan_id);
    let candidate_id = candidate_id(classified_candidate(&snapshot));
    verify_candidate(
        &mut client,
        session_id,
        "w61-import-errors-verify",
        &scan_id,
        &candidate_id,
        2_000,
    );
    wait_for_candidate_lifecycle(&mut client, session_id, &scan_id, &candidate_id, "verified");
    wait_for_marker_pid_to_exit(&fixture.fixture_root.join("root.pid"));

    let imported = import_local_agent(
        &mut client,
        session_id,
        "w61-import",
        &scan_id,
        &candidate_id,
        "project-w61-import",
    );
    assert_eq!(imported.payload["reused"], false);

    // Same requestId with a DIFFERENT business payload is a stable conflict,
    // not an identity change.
    send_command(
        &mut client,
        session_id,
        "w61-import",
        "agent.import_local",
        json!({
            "scanId": scan_id,
            "candidateId": candidate_id,
            "projectId": "project-w61-other",
        }),
        Some(5_000),
    );
    let conflict = read_error(&mut client, "import request conflict");
    assert_eq!(
        conflict.code, "IMPORT_CONFLICT",
        "a requestId conflict must map to IMPORT_CONFLICT, not identity changed"
    );
    assert!(!conflict.retryable);
    assert_safe_renderer_value(
        &serde_json::to_value(&conflict).expect("error json"),
        &fixture,
    );

    // A fresh import into a missing project is a stable persistence failure
    // with zero partial rows, and must not leak SQLite details.
    send_command(
        &mut client,
        session_id,
        "w61-import-missing-project",
        "agent.import_local",
        json!({
            "scanId": scan_id,
            "candidateId": candidate_id,
            "projectId": "project-w61-does-not-exist",
        }),
        Some(5_000),
    );
    let failed = read_error(&mut client, "import persistence failure");
    assert_eq!(
        failed.code, "IMPORT_PERSISTENCE_FAILED",
        "an uncategorized persistence failure must not masquerade as identity changed"
    );
    assert!(!failed.retryable);
    assert_safe_renderer_value(
        &serde_json::to_value(&failed).expect("error json"),
        &fixture,
    );

    let connection =
        rusqlite::Connection::open(&fixture.database).expect("open isolated fixture DB");
    for table in [
        "connector_adapter_bindings",
        "local_agent_imports",
        "agents",
        "project_agents",
    ] {
        let count: i64 = connection
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .expect("query import rows");
        assert_eq!(
            count, 1,
            "{table} must keep exactly the original import rows after typed failures"
        );
    }
    shutdown(
        client,
        session_id,
        "w61-import-errors-shutdown",
        &mut owned_core,
    );
}

#[test]
fn release_core_named_pipe_import_replay_is_stable_across_envelope_deadline() {
    let _guard = suite_guard();
    let fixture = ReleaseFixture::create("success", false);
    let mut owned_core = fixture.spawn_core(None);
    let session_id = "session-w61-import-deadline-123456";
    let mut client = fixture.connect(session_id, &mut owned_core);
    create_import_project(
        &mut client,
        session_id,
        "w61-deadline-project",
        "project-w61-deadline",
    );
    let start = start_scan(&mut client, session_id, "w61-deadline-start");
    let scan_id = start.payload["scanId"]
        .as_str()
        .expect("scan id")
        .to_owned();
    let snapshot = wait_for_completed_snapshot(&mut client, session_id, &scan_id);
    let candidate_id = candidate_id(classified_candidate(&snapshot));
    verify_candidate(
        &mut client,
        session_id,
        "w61-deadline-verify",
        &scan_id,
        &candidate_id,
        2_000,
    );
    wait_for_candidate_lifecycle(&mut client, session_id, &scan_id, &candidate_id, "verified");
    wait_for_marker_pid_to_exit(&fixture.fixture_root.join("root.pid"));

    let payload = json!({
        "scanId": scan_id,
        "candidateId": candidate_id,
        "projectId": "project-w61-deadline",
        "modelSelection": null,
    });
    // First import with a short envelope deadline.
    send_command(
        &mut client,
        session_id,
        "w61-deadline-import",
        "agent.import_local",
        payload.clone(),
        Some(1_000),
    );
    let imported = read_response(&mut client, "first import with short deadline");
    assert_eq!(imported.payload["reused"], false);
    let original_sequence = imported.payload["eventSequence"]
        .as_u64()
        .expect("import event sequence");
    assert!(original_sequence > 0);

    // Same requestId + same business payload + DIFFERENT envelope deadlineMs
    // must replay (the idempotency hash must not include deadlineMs).
    send_command(
        &mut client,
        session_id,
        "w61-deadline-import",
        "agent.import_local",
        payload.clone(),
        Some(5_000),
    );
    let replay = read_response(&mut client, "import replay with different deadline");
    assert_eq!(replay.payload["reused"], true);
    assert_eq!(replay.payload["importId"], imported.payload["importId"]);
    assert_eq!(
        replay.payload["eventSequence"], imported.payload["eventSequence"],
        "replay across deadlineMs must return the original event sequence"
    );

    // Changing a business payload field with the same requestId still fails
    // closed as a conflict and writes nothing.
    let mut changed = payload;
    changed["projectId"] = json!("project-w61-deadline-other");
    send_command(
        &mut client,
        session_id,
        "w61-deadline-import",
        "agent.import_local",
        changed,
        Some(2_000),
    );
    let conflict = read_error(&mut client, "import payload change under same requestId");
    assert_eq!(
        conflict.code, "IMPORT_CONFLICT",
        "a changed business payload under the same requestId must fail closed"
    );
    let connection =
        rusqlite::Connection::open(&fixture.database).expect("open isolated fixture DB");
    for table in [
        "connector_adapter_bindings",
        "local_agent_imports",
        "agents",
        "project_agents",
    ] {
        let count: i64 = connection
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .expect("query import rows");
        assert_eq!(
            count, 1,
            "{table} must not grow after a business payload conflict"
        );
    }
    shutdown(
        client,
        session_id,
        "w61-import-deadline-shutdown",
        &mut owned_core,
    );
}

#[test]
fn release_core_named_pipe_import_model_selection_conflict_is_fail_closed() {
    let _guard = suite_guard();
    let fixture = ReleaseFixture::create("success", false);
    let mut owned_core = fixture.spawn_core(None);
    let session_id = "session-w62-import-model-selection-123456";
    let mut client = fixture.connect(session_id, &mut owned_core);
    create_import_project(
        &mut client,
        session_id,
        "w62-model-selection-project",
        "project-w62-import",
    );
    let start = start_scan(&mut client, session_id, "w62-model-selection-start");
    let scan_id = start.payload["scanId"]
        .as_str()
        .expect("scan id")
        .to_owned();
    let snapshot = wait_for_completed_snapshot(&mut client, session_id, &scan_id);
    let candidate_id = candidate_id(classified_candidate(&snapshot));
    verify_candidate(
        &mut client,
        session_id,
        "w62-model-selection-verify",
        &scan_id,
        &candidate_id,
        2_000,
    );
    wait_for_candidate_lifecycle(&mut client, session_id, &scan_id, &candidate_id, "verified");
    wait_for_marker_pid_to_exit(&fixture.fixture_root.join("root.pid"));

    // First import with modelSelection null (connector-default assignment).
    let imported = import_local_agent(
        &mut client,
        session_id,
        "w62-import-default",
        &scan_id,
        &candidate_id,
        "project-w62-import",
    );
    assert_eq!(imported.payload["reused"], false);
    let original_sequence = imported.payload["eventSequence"]
        .as_u64()
        .expect("import event sequence");
    assert!(original_sequence > 0);

    // New requestId + same scan/candidate/project + a DIFFERENT normalized
    // model selection must be a fail-closed IMPORT_CONFLICT, never a silent
    // reuse of the old connector-default assignment.
    send_command(
        &mut client,
        session_id,
        "w62-import-pinned",
        "agent.import_local",
        json!({
            "scanId": scan_id,
            "candidateId": candidate_id,
            "projectId": "project-w62-import",
            "modelSelection": "fixture-model",
        }),
        Some(5_000),
    );
    let conflict = read_error(&mut client, "model selection import conflict");
    assert_eq!(
        conflict.code, "IMPORT_CONFLICT",
        "same binding+project with a different model selection must conflict, not reuse"
    );
    assert!(!conflict.retryable);
    let conflict_json = serde_json::to_string(&conflict)
        .expect("serialize conflict error")
        .to_ascii_lowercase();
    assert!(
        !conflict_json.contains("fixture-model"),
        "the renderer error must not expose the model id"
    );
    assert!(!conflict_json.contains(&fixture.credential.to_ascii_lowercase()));
    assert_safe_renderer_value(
        &serde_json::to_value(&conflict).expect("error json"),
        &fixture,
    );

    // Fixture DB: the four durable tables keep exactly one row, the
    // assignment stays connector-default/null, and exactly one imported
    // event exists.
    let connection =
        rusqlite::Connection::open(&fixture.database).expect("open isolated fixture DB");
    for table in [
        "connector_adapter_bindings",
        "local_agent_imports",
        "agents",
        "project_agents",
    ] {
        let count: i64 = connection
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .expect("query import rows");
        assert_eq!(
            count, 1,
            "{table} must keep exactly the original import rows after the model-selection conflict"
        );
    }
    let (mode, model_id): (String, Option<String>) = connection
        .query_row(
            "SELECT model_selection_mode, model_id FROM project_agents",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("query assignment model selection");
    assert_eq!(mode, "connector_default");
    assert_eq!(
        model_id, None,
        "the existing assignment must remain connector-default/null"
    );
    let event_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM event_store WHERE event_type = 'local_agent.imported'",
            [],
            |row| row.get(0),
        )
        .expect("query imported event count");
    assert_eq!(event_count, 1);
    drop(connection);

    // The same normalized model selection across requestIds still reuses
    // with the original sequence.
    send_command(
        &mut client,
        session_id,
        "w62-import-default-2",
        "agent.import_local",
        json!({
            "scanId": scan_id,
            "candidateId": candidate_id,
            "projectId": "project-w62-import",
            "modelSelection": null,
        }),
        Some(5_000),
    );
    let reuse = read_response(&mut client, "same model selection reuse");
    assert_eq!(reuse.payload["reused"], true);
    assert_eq!(reuse.payload["importId"], imported.payload["importId"]);
    assert_eq!(
        reuse.payload["eventSequence"], imported.payload["eventSequence"],
        "same model selection reuse must return the original event sequence"
    );
    shutdown(
        client,
        session_id,
        "w62-model-selection-shutdown",
        &mut owned_core,
    );
}

#[test]
fn ipc_v1_schema_validates_additive_w6_routes_without_changing_legacy_queries() {
    let schema: Value =
        serde_json::from_str(include_str!("../../../schemas/ipc/v1/protocol.schema.json"))
            .expect("parse IPC v1 schema");
    let validator = jsonschema::options()
        .with_draft(Draft::Draft202012)
        .build(&schema)
        .expect("compile Draft 2020-12 IPC schema");
    let legacy_query = json!({
        "kind": "query",
        "protocol": {"major": 1, "minor": 0},
        "requestId": "w5-schema-legacy",
        "sessionId": "session-w5-schema-123456",
        "query": "agent.scan_local",
        "payload": {},
    });
    assert!(
        validator.is_valid(&legacy_query),
        "legacy agent.scan_local envelope remains valid under additive v1"
    );
    let verify = json!({
        "kind": "command",
        "protocol": {"major": 1, "minor": 0},
        "requestId": "w5-schema-verify",
        "sessionId": "session-w5-schema-123456",
        "command": "agent.discovery.verify",
        "payload": {
            "scanId": "scan-abc",
            "candidateId": "candidate-abc",
            "consent": true,
            "deadlineMs": 1000,
        },
        "deadlineMs": 1000,
    });
    assert!(validator.is_valid(&verify));
    let import = json!({
        "kind": "command",
        "protocol": {"major": 1, "minor": 0},
        "requestId": "w6-schema-import",
        "sessionId": "session-w6-schema-123456",
        "command": "agent.import_local",
        "payload": {
            "scanId": "scan-abc",
            "candidateId": "candidate-abc",
            "projectId": "project-abc",
            "modelSelection": null,
        },
    });
    assert!(validator.is_valid(&import));
    let mut import_with_private_binding = import;
    import_with_private_binding["payload"]["candidateBinding"] = json!("binding-not-public");
    assert!(
        !validator.is_valid(&import_with_private_binding),
        "private ACP binding material must not be accepted by local import IPC"
    );
    let mut forbidden_binding = verify;
    forbidden_binding["payload"]["candidateBinding"] = json!("binding-not-public");
    assert!(
        !validator.is_valid(&forbidden_binding),
        "private ACP binding material must not be accepted by IPC v1"
    );
}

#[test]
fn release_core_named_pipe_replays_acknowledges_and_recovers_after_gap() {
    let _guard = suite_guard();
    let fixture = ReleaseFixture::create("success", false);
    let mut owned_core = fixture.spawn_core(Some(4));
    let control_session = "session-w5-release-events-control-123456";
    let subscription_session = control_session;
    let mut control = fixture.connect(control_session, &mut owned_core);
    let mut subscriber = fixture.connect(subscription_session, &mut owned_core);

    send_command(
        &mut control,
        control_session,
        "w5-events-start-1",
        "agent.discovery.start",
        json!({}),
        Some(5_000),
    );
    let start = read_response(&mut control, "discovery lifecycle start");
    let scan_id = start.payload["scanId"]
        .as_str()
        .expect("scan id")
        .to_owned();
    let _ = wait_for_completed_snapshot(&mut control, control_session, &scan_id);
    send_query(
        &mut control,
        control_session,
        "w5-legacy-core-events",
        "events.replay",
        json!({"afterSequence": 0, "limit": 16}),
    );
    let legacy_replay = read_response(&mut control, "legacy core event replay");
    assert!(
        legacy_replay.payload["events"]
            .as_array()
            .expect("legacy core event array")
            .iter()
            .all(|event| {
                event["event"]
                    .as_str()
                    .is_none_or(|name| !name.starts_with("agent.discovery."))
            }),
        "W5 lifecycle events must not alter the legacy core-events stream"
    );
    send_query(
        &mut control,
        control_session,
        "w5-discovery-events-prime",
        "events.replay",
        json!({
            "streamId": "local-discovery-events",
            "afterSequence": 0,
            "limit": 1,
        }),
    );
    let discovery_prime = read_response(&mut control, "local discovery stream prime");
    let discovery_epoch = discovery_prime.payload["events"]
        .as_array()
        .expect("discovery event array")
        .first()
        .and_then(|event| event["cursor"]["epoch"].as_str())
        .expect("discovery stream epoch")
        .to_owned();
    send_command(
        &mut subscriber,
        subscription_session,
        "w5-events-subscribe",
        "events.subscribe",
        json!({
            "afterCursor": {
                "streamId": "local-discovery-events",
                "sequence": 0,
                "epoch": discovery_epoch,
            },
            "maxInFlightEvents": 1,
            "maxInFlightBytes": 262_144,
        }),
        None,
    );
    let subscription = read_response(&mut subscriber, "initial event subscription");
    let subscription_id = subscription.payload["subscriptionId"]
        .as_str()
        .expect("subscription id")
        .to_owned();
    let event: agenttalk_protocols::EventEnvelope =
        serde_json::from_value(read_value(&mut subscriber, "discovery subscription event"))
            .expect("decode discovery event");
    assert_eq!(event.event, "agent.discovery.started");
    assert_eq!(event.cursor.stream_id, "local-discovery-events");
    send_command(
        &mut subscriber,
        subscription_session,
        "w5-events-ack",
        "events.ack",
        json!({"subscriptionId": subscription_id, "cursor": event.cursor}),
        None,
    );
    assert_eq!(
        read_response(&mut subscriber, "discovery event ACK").payload["acknowledged"],
        true
    );
    // The subscription can already have another lifecycle event in flight
    // after its ACK. Closing this owned test connection is the intended
    // bounded teardown path and does not consume or ignore that event.
    drop(subscriber);

    send_query(
        &mut control,
        control_session,
        "w5-events-replay",
        "events.replay",
        json!({
            "streamId": "local-discovery-events",
            "afterSequence": 0,
            "limit": 16,
        }),
    );
    let replay = read_response(&mut control, "event replay");
    let events = replay.payload["events"]
        .as_array()
        .expect("replayed events");
    assert!(
        events
            .iter()
            .any(|event| event["event"] == "agent.discovery.started")
            && events
                .iter()
                .any(|event| event["event"] == "agent.discovery.completed"),
        "replay must contain the discovery lifecycle"
    );

    for number in 0..2 {
        let start = start_scan(
            &mut control,
            control_session,
            &format!("w5-events-overflow-{number}"),
        );
        let scan_id = start.payload["scanId"].as_str().expect("scanId").to_owned();
        let _ = wait_for_completed_snapshot(&mut control, control_session, &scan_id);
    }
    send_query(
        &mut control,
        control_session,
        "w5-events-gap",
        "events.replay",
        json!({
            "streamId": "local-discovery-events",
            "afterSequence": 0,
            "limit": 16,
        }),
    );
    let gap = read_error(&mut control, "event replay gap");
    assert_eq!(gap.code, "REPLAY_GAP");
    assert_eq!(
        gap.details
            .as_ref()
            .and_then(|details| details["requiresSnapshot"].as_bool()),
        Some(true)
    );

    let recovery = snapshot(
        &mut control,
        control_session,
        "w5-events-recovery-snapshot",
        &scan_id,
    );
    assert_eq!(recovery.payload["state"], "completed");
    assert_safe_renderer_value(&recovery.payload, &fixture);

    shutdown(
        control,
        control_session,
        "w5-shutdown-events",
        &mut owned_core,
    );
}

#[test]
fn release_core_named_pipe_repeated_verify_reuses_valid_result_without_new_acp_child() {
    let _guard = suite_guard();
    let fixture = ReleaseFixture::create("success", false);
    let mut owned_core = fixture.spawn_core(None);
    let session_id = "session-w58-verify-reuse-123456";
    let mut client = fixture.connect(session_id, &mut owned_core);
    let started = start_scan(&mut client, session_id, "w58-reuse-start");
    let scan_id = started.payload["scanId"]
        .as_str()
        .expect("scan id")
        .to_owned();
    let discovery_epoch = started.payload["eventStream"]["epoch"]
        .as_str()
        .expect("discovery epoch")
        .to_owned();
    let snapshot = wait_for_completed_snapshot(&mut client, session_id, &scan_id);
    let candidate_id = candidate_id(classified_candidate(&snapshot));

    let first = verify_candidate(
        &mut client,
        session_id,
        "w58-reuse-verify-1",
        &scan_id,
        &candidate_id,
        2_000,
    );
    assert_eq!(first.payload["accepted"], true);
    let _ =
        wait_for_candidate_lifecycle(&mut client, session_id, &scan_id, &candidate_id, "verified");

    // A NEW requestId for the same candidate must reuse the still-valid
    // verification without starting a new ACP child or emitting an event.
    let second = verify_candidate(
        &mut client,
        session_id,
        "w58-reuse-verify-2",
        &scan_id,
        &candidate_id,
        2_000,
    );
    assert_eq!(second.payload["accepted"], true);
    assert_eq!(second.payload["state"], "verified");
    assert_eq!(second.payload["reused"], true);

    send_query(
        &mut client,
        session_id,
        "w58-reuse-replay",
        "events.replay",
        json!({
            "streamId": "local-discovery-events",
            "epoch": discovery_epoch,
            "afterSequence": 0,
            "limit": 16,
        }),
    );
    let replay = read_response(&mut client, "verify reuse event replay");
    let verified_events = replay.payload["events"]
        .as_array()
        .expect("replayed event array")
        .iter()
        .filter(|event| {
            event["event"] == "agent.discovery.candidate_verified"
                && event["payload"]["candidateId"] == candidate_id
        })
        .count();
    assert_eq!(
        verified_events, 1,
        "the reused verification must not emit a second candidate_verified event"
    );

    let invocation_count = fs::read_to_string(fixture.fixture_root.join("initialize.invocations"))
        .expect("read ACP fixture invocation ledger")
        .lines()
        .count();
    assert_eq!(
        invocation_count, 1,
        "repeated ordinary verify must not launch a new ACP child"
    );
    wait_for_marker_pid_to_exit(&fixture.fixture_root.join("root.pid"));
    shutdown(client, session_id, "w58-reuse-shutdown", &mut owned_core);
}

#[test]
fn release_core_named_pipe_dismiss_is_business_idempotent_with_single_event_and_bounded_receipts() {
    let _guard = suite_guard();
    let fixture = ReleaseFixture::create("success", true);
    let mut owned_core = fixture.spawn_core_with_discovery_limits(
        Some(16),
        Some(8),
        Some(600_000),
        DiscoveryLimitOverrides {
            max_receipts_per_session: Some(3),
            ..DiscoveryLimitOverrides::default()
        },
    );
    let session_id = "session-w58-dismiss-idem-123456";
    let mut client = fixture.connect(session_id, &mut owned_core);
    let started = start_scan(&mut client, session_id, "w58-dismiss-idem-start");
    let scan_id = started.payload["scanId"]
        .as_str()
        .expect("scan id")
        .to_owned();
    let discovery_epoch = started.payload["eventStream"]["epoch"]
        .as_str()
        .expect("discovery epoch")
        .to_owned();
    let snapshot = wait_for_completed_snapshot(&mut client, session_id, &scan_id);
    let acp_id = candidate_id(classified_candidate(&snapshot));
    let unknown_id = snapshot["candidates"]
        .as_array()
        .expect("snapshot candidates")
        .iter()
        .find(|candidate| candidate["lifecycleState"] == "adapter_required")
        .expect("unknown executable becomes adapter_required");
    let unknown_id = candidate_id(unknown_id);

    // Receipt 2 of 3 (start was receipt 1): first dismissal commits.
    send_command(
        &mut client,
        session_id,
        "w58-dismiss-1",
        "agent.discovery.dismiss",
        json!({"scanId": scan_id, "candidateId": unknown_id}),
        None,
    );
    assert_eq!(
        read_response(&mut client, "first dismissal").payload["dismissed"],
        true
    );
    // Repeat dismissal with a NEW requestId: business no-op, no receipt/event.
    send_command(
        &mut client,
        session_id,
        "w58-dismiss-2",
        "agent.discovery.dismiss",
        json!({"scanId": scan_id, "candidateId": unknown_id}),
        None,
    );
    let repeated = read_response(&mut client, "repeated dismissal");
    assert_eq!(repeated.payload["dismissed"], true);
    assert_eq!(repeated.payload["alreadyDismissed"], true);

    // Receipt 3 of 3: verify the ACP candidate commits.
    let verify = verify_candidate(
        &mut client,
        session_id,
        "w58-dismiss-idem-verify",
        &scan_id,
        &acp_id,
        2_000,
    );
    assert_eq!(verify.payload["accepted"], true);
    let _ = wait_for_candidate_lifecycle(&mut client, session_id, &scan_id, &acp_id, "verified");

    // Re-verification reuses the valid result and needs no receipt quota even
    // though the session receipt cap is now exhausted.
    let reused = verify_candidate(
        &mut client,
        session_id,
        "w58-dismiss-idem-verify-2",
        &scan_id,
        &acp_id,
        2_000,
    );
    assert_eq!(reused.payload["reused"], true);

    // Any operation that would write a NEW receipt is rejected at the cap.
    send_command(
        &mut client,
        session_id,
        "w58-dismiss-overflow",
        "agent.discovery.dismiss",
        json!({"scanId": scan_id, "candidateId": acp_id}),
        None,
    );
    let overflow = read_error(&mut client, "session receipt capacity");
    assert_eq!(
        overflow.code, "DISCOVERY_OWNER_RECEIPT_CAPACITY_EXHAUSTED",
        "receipts beyond the session cap must be rejected with a typed error"
    );
    assert!(!overflow.retryable);
    assert_safe_renderer_value(
        &serde_json::to_value(&overflow).expect("error json"),
        &fixture,
    );
    let _ = wait_for_candidate_lifecycle(&mut client, session_id, &scan_id, &acp_id, "verified");

    // Exactly one dismiss event and one verify event were emitted.
    send_query(
        &mut client,
        session_id,
        "w58-dismiss-idem-replay",
        "events.replay",
        json!({
            "streamId": "local-discovery-events",
            "epoch": discovery_epoch,
            "afterSequence": 0,
            "limit": 16,
        }),
    );
    let replay = read_response(&mut client, "dismiss idempotency event replay");
    let verified_events = replay.payload["events"]
        .as_array()
        .expect("replayed event array")
        .iter()
        .filter(|event| event["event"] == "agent.discovery.candidate_verified")
        .count();
    assert_eq!(
        verified_events, 2,
        "repeated dismiss must not emit a second event: one dismissal plus one verification"
    );
    shutdown(
        client,
        session_id,
        "w58-dismiss-idem-shutdown",
        &mut owned_core,
    );
}

#[test]
fn release_core_named_pipe_bounds_concurrent_verifications() {
    let _guard = suite_guard();
    let fixture = ReleaseFixture::create("timeout", false);
    let mut owned_core = fixture.spawn_core_with_discovery_limits(
        Some(16),
        Some(8),
        Some(600_000),
        DiscoveryLimitOverrides {
            max_running_verifications_per_owner: Some(1),
            max_running_verifications_global: Some(2),
            ..DiscoveryLimitOverrides::default()
        },
    );
    let session_id = "session-w58-verify-flood-123456";
    let mut client = fixture.connect(session_id, &mut owned_core);
    let started_a = start_scan(&mut client, session_id, "w58-verify-flood-start-a");
    let scan_a = started_a.payload["scanId"]
        .as_str()
        .expect("scan A id")
        .to_owned();
    let snapshot_a = wait_for_completed_snapshot(&mut client, session_id, &scan_a);
    let candidate_a = candidate_id(classified_candidate(&snapshot_a));
    let started_b = start_scan(&mut client, session_id, "w58-verify-flood-start-b");
    let scan_b = started_b.payload["scanId"]
        .as_str()
        .expect("scan B id")
        .to_owned();
    let snapshot_b = wait_for_completed_snapshot(&mut client, session_id, &scan_b);
    let candidate_b = candidate_id(classified_candidate(&snapshot_b));

    // The timeout-mode fixture never answers initialize, so the verification
    // stays in flight (and its ACP child stays alive) until the deadline.
    let first = verify_candidate(
        &mut client,
        session_id,
        "w58-verify-flood-1",
        &scan_a,
        &candidate_a,
        3_000,
    );
    assert_eq!(first.payload["accepted"], true);
    let root_marker = fixture.fixture_root.join("root.pid");
    let start_deadline = Instant::now() + IPC_TIMEOUT;
    while !root_marker.is_file() {
        assert!(
            Instant::now() < start_deadline,
            "timeout fixture child did not start"
        );
        thread::sleep(Duration::from_millis(20));
    }
    let ledger_path = fixture.fixture_root.join("initialize.invocations");
    let ledger_deadline = Instant::now() + IPC_TIMEOUT;
    loop {
        let lines = fs::read_to_string(&ledger_path)
            .map(|contents| contents.lines().count())
            .unwrap_or(0);
        if lines >= 1 {
            break;
        }
        assert!(
            Instant::now() < ledger_deadline,
            "timeout fixture child did not append its initialize ledger"
        );
        thread::sleep(Duration::from_millis(10));
    }

    // A concurrent verification of a DIFFERENT candidate (new requestId) in
    // the same owner must be rejected at the per-owner running ceiling,
    // without a second ACP child.
    send_command(
        &mut client,
        session_id,
        "w58-verify-flood-2",
        "agent.discovery.verify",
        json!({
            "scanId": scan_b,
            "candidateId": candidate_b,
            "consent": true,
        }),
        Some(3_000),
    );
    let overflow = read_error(&mut client, "owner verification capacity");
    assert_eq!(
        overflow.code, "DISCOVERY_OWNER_VERIFICATION_CAPACITY_EXHAUSTED",
        "concurrent verifications beyond the per-owner ceiling must be rejected"
    );
    assert!(!overflow.retryable);
    assert_safe_renderer_value(
        &serde_json::to_value(&overflow).expect("error json"),
        &fixture,
    );
    let invocation_count = fs::read_to_string(&ledger_path)
        .expect("read ACP fixture invocation ledger")
        .lines()
        .count();
    assert_eq!(
        invocation_count, 1,
        "the rejected verification must not launch an ACP child"
    );

    // The first verification times out and its running slot must be released.
    let _ = wait_for_candidate_lifecycle(&mut client, session_id, &scan_a, &candidate_a, "timeout");
    wait_for_marker_pid_to_exit(&fixture.fixture_root.join("root.pid"));

    // Once the slot is free, a new requestId may verify candidate B.
    let retried = verify_candidate(
        &mut client,
        session_id,
        "w58-verify-flood-3",
        &scan_b,
        &candidate_b,
        1_000,
    );
    assert_eq!(retried.payload["accepted"], true);
    let _ = wait_for_candidate_lifecycle(&mut client, session_id, &scan_b, &candidate_b, "timeout");
    shutdown(
        client,
        session_id,
        "w58-verify-flood-shutdown",
        &mut owned_core,
    );
}

#[test]
fn release_core_named_pipe_bounds_concurrent_import_plans() {
    let _guard = suite_guard();
    let fixture = ReleaseFixture::create("success", false);
    let mut owned_core = fixture.spawn_core_with_discovery_limits(
        Some(16),
        Some(8),
        Some(600_000),
        DiscoveryLimitOverrides {
            max_inflight_import_plans_per_owner: Some(1),
            max_inflight_import_plans_global: Some(2),
            import_plan_hold_ms: Some(1_500),
            ..DiscoveryLimitOverrides::default()
        },
    );
    let session_id = "session-w58-plan-flood-123456";
    // Both connections use the same clientId+sessionId so they share one
    // DiscoveryOwnerScope (the owner of the scan below).
    let mut client_a = fixture.connect_as("w58-plan-a", session_id, &mut owned_core);
    let mut client_b = fixture.connect_as("w58-plan-a", session_id, &mut owned_core);
    let started = start_scan(&mut client_a, session_id, "w58-plan-flood-start");
    let scan_id = started.payload["scanId"]
        .as_str()
        .expect("scan id")
        .to_owned();
    let snapshot = wait_for_completed_snapshot(&mut client_a, session_id, &scan_id);
    let candidate_id = candidate_id(classified_candidate(&snapshot));
    let verify = verify_candidate(
        &mut client_a,
        session_id,
        "w58-plan-flood-verify",
        &scan_id,
        &candidate_id,
        2_000,
    );
    assert_eq!(verify.payload["accepted"], true);
    let _ = wait_for_candidate_lifecycle(
        &mut client_a,
        session_id,
        &scan_id,
        &candidate_id,
        "verified",
    );
    wait_for_marker_pid_to_exit(&fixture.fixture_root.join("root.pid"));

    // Connection A starts a plan whose work is held open by the bounded
    // dev-mode hold, keeping the per-owner in-flight lease occupied.
    let plan_scan_id = scan_id.clone();
    let plan_candidate_id = candidate_id.clone();
    let plan_handle = thread::spawn(move || {
        send_query(
            &mut client_a,
            session_id,
            "w58-plan-flood-a",
            "agent.import.plan",
            json!({
                "scanId": plan_scan_id,
                "candidateId": plan_candidate_id,
                "projectId": "project-w58-a",
            }),
        );
        read_response(&mut client_a, "in-flight import plan A")
    });
    thread::sleep(Duration::from_millis(200));

    // A concurrent plan (different project, so not the single-flight key) for
    // the same owner is rejected at the per-owner in-flight ceiling.
    send_query(
        &mut client_b,
        session_id,
        "w58-plan-flood-b",
        "agent.import.plan",
        json!({
            "scanId": scan_id,
            "candidateId": candidate_id,
            "projectId": "project-w58-b",
        }),
    );
    let overflow = read_error(&mut client_b, "owner import-plan capacity");
    assert_eq!(
        overflow.code, "DISCOVERY_OWNER_IMPORT_PLAN_CAPACITY_EXHAUSTED",
        "concurrent import plans beyond the per-owner in-flight ceiling must be rejected"
    );
    assert!(!overflow.retryable);
    assert_safe_renderer_value(
        &serde_json::to_value(&overflow).expect("error json"),
        &fixture,
    );

    let plan_a = plan_handle
        .join()
        .expect("import plan A thread must not panic")
        .payload;
    assert_eq!(plan_a["readOnly"], true);

    // After A completes, its lease is released and a new plan is accepted.
    send_query(
        &mut client_b,
        session_id,
        "w58-plan-flood-c",
        "agent.import.plan",
        json!({
            "scanId": scan_id,
            "candidateId": candidate_id,
            "projectId": "project-w58-c",
        }),
    );
    let plan_c = read_response(&mut client_b, "import plan after lease release");
    assert_eq!(plan_c.payload["readOnly"], true);
    shutdown(
        client_b,
        session_id,
        "w58-plan-flood-shutdown",
        &mut owned_core,
    );
}

/// Strict structural RFC3339 UTC validator for event `occurredAt` values:
/// YYYY-MM-DDTHH:MM:SS.mmmZ with valid ranges and exactly three digits.
fn is_schema_valid_utc_rfc3339(value: &str) -> bool {
    let Some(rest) = value.strip_suffix('Z') else {
        return false;
    };
    let Some((date, time)) = rest.split_once('T') else {
        return false;
    };
    let mut date_parts = date.split('-');
    let (Some(year_s), Some(month_s), Some(day_s)) =
        (date_parts.next(), date_parts.next(), date_parts.next())
    else {
        return false;
    };
    if date_parts.next().is_some() {
        return false;
    }
    // RFC3339 full-date requires exactly four digit years.
    if year_s.len() != 4 || !year_s.chars().all(|c| c.is_ascii_digit()) {
        return false;
    }
    let (Ok(year), Ok(month), Ok(day)) = (
        year_s.parse::<i32>(),
        month_s.parse::<u32>(),
        day_s.parse::<u32>(),
    ) else {
        return false;
    };
    if year < 0 || !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return false;
    }
    let days_in_month = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if (year % 400 == 0) || (year % 4 == 0 && year % 100 != 0) {
                29
            } else {
                28
            }
        }
        _ => return false,
    };
    if day > days_in_month {
        return false;
    }
    let mut time_parts = time.splitn(2, '.');
    let (Some(hms), Some(fraction)) = (time_parts.next(), time_parts.next()) else {
        return false;
    };
    if time_parts.next().is_some() {
        return false;
    }
    let mut hms_parts = hms.split(':');
    let (Some(hh), Some(mm), Some(ss)) = (hms_parts.next(), hms_parts.next(), hms_parts.next())
    else {
        return false;
    };
    if hms_parts.next().is_some() {
        return false;
    }
    let (Ok(hh), Ok(mm), Ok(ss)) = (hh.parse::<u32>(), mm.parse::<u32>(), ss.parse::<u32>()) else {
        return false;
    };
    if hh > 23 || mm > 59 || ss > 59 {
        return false;
    }
    fraction.len() == 3 && fraction.chars().all(|c| c.is_ascii_digit())
}

#[test]
fn release_core_events_carry_schema_valid_utc_rfc3339_occurred_at() {
    let _guard = suite_guard();
    let fixture = ReleaseFixture::create("success", false);
    let mut owned_core = fixture.spawn_core(None);
    let session_id = "session-w81-occurred-at-123456";
    // The subscription lives on a dedicated connection with the same owner
    // client id so the discovery stream is reachable while the main client
    // stays free to shut the owned Core down afterwards.
    let mut client = fixture.connect(session_id, &mut owned_core);
    let mut subscription =
        fixture.connect_as("w5-release-named-pipe-test", session_id, &mut owned_core);
    let started = start_scan(&mut client, session_id, "w81-occurred-at-start");
    let epoch = started.payload["eventStream"]["epoch"]
        .as_str()
        .expect("discovery epoch")
        .to_owned();
    send_command(
        &mut subscription,
        session_id,
        "w81-occurred-at-subscribe",
        "events.subscribe",
        json!({
            "streamId": "local-discovery-events",
            "afterCursor": {
                "streamId": "local-discovery-events",
                "sequence": 0,
                "epoch": epoch,
            },
            "maxInFlightEvents": 64,
            "maxInFlightBytes": 262144,
        }),
        None,
    );
    let _receipt = read_response(&mut subscription, "discovery subscription receipt");
    let mut validated = 0usize;
    loop {
        let event: agenttalk_protocols::EventEnvelope =
            serde_json::from_value(read_value(&mut subscription, "discovery event"))
                .expect("decode discovery event envelope");
        assert!(
            is_schema_valid_utc_rfc3339(&event.occurred_at),
            "event {} carried non-schema-valid occurredAt: {}",
            event.event,
            event.occurred_at
        );
        validated += 1;
        if event.event == "agent.discovery.completed" {
            break;
        }
    }
    assert!(
        validated >= 4,
        "expected the started/observed/classified/completed events"
    );
    drop(subscription);
    shutdown(
        client,
        session_id,
        "w81-occurred-at-shutdown",
        &mut owned_core,
    );
}

#[test]
fn production_bundled_catalog_rejects_spoofed_copilot_sha_mismatch() {
    let _guard = suite_guard();
    let fixture = ProductionCatalogFixture::create();
    let mut owned_core = fixture.spawn_core();
    let session_id = "session-w84-prod-copilot-123456";
    let mut client = fixture.connect(session_id, &mut owned_core);
    let start = start_scan(&mut client, session_id, "w84-prod-start");
    let scan_id = start.payload["scanId"].as_str().expect("scanId").to_owned();
    let completed = wait_for_completed_snapshot(&mut client, session_id, &scan_id);
    assert_eq!(completed["state"], "completed");

    // The production Copilot manifest pins the real copilot.exe content SHA.
    // A same-named file with a different SHA is rejected at the classification
    // boundary (fingerprint changed -> observed) and never becomes an
    // identified or verified ACP target.
    let copilot = completed["candidates"]
        .as_array()
        .expect("snapshot candidates")
        .iter()
        .find(|candidate| candidate["candidate"]["displayName"] == "copilot.exe")
        .unwrap_or_else(|| panic!("spoofed copilot.exe must be observed; snapshot: {completed}"));
    assert_eq!(copilot["candidate"]["runtimeType"], "unknown");
    assert_eq!(copilot["candidate"]["discoveryState"], "observed");
    assert_eq!(copilot["lifecycleState"], "adapter_required");
    assert_eq!(
        copilot["candidate"]["trustLevel"], "heuristic",
        "a mismatched-SHA name match must never upgrade trust"
    );

    // An unrelated executable in the same directory stays unknown and is
    // never eligible for direct use.
    let other = completed["candidates"]
        .as_array()
        .expect("snapshot candidates")
        .iter()
        .find(|candidate| candidate["candidate"]["displayName"] == "random-tool.exe")
        .expect("unrelated executable must be observed");
    assert_eq!(other["candidate"]["runtimeType"], "unknown");
    assert_eq!(other["lifecycleState"], "adapter_required");

    // No child, no initialize: the pinned-SHA mismatch is rejected before any
    // ACP spawn.
    assert!(
        !fixture.path_root.join("root.pid").exists(),
        "a mismatched-SHA copilot.exe must not spawn"
    );
    assert!(
        !fixture.path_root.join("initialize.invocations").exists(),
        "a mismatched-SHA copilot.exe must never reach initialize"
    );

    shutdown(client, session_id, "w84-prod-shutdown", &mut owned_core);
}

#[test]
fn user_selected_fixture_proves_acp_initialize_protocol_chain() {
    let _guard = suite_guard();
    // The dev-mode fixture catalog plus an explicit UserSelected source is the
    // legitimate test authority: this test proves the ACP initialize protocol
    // chain can work, NOT that a filename-only heuristic match is a trusted
    // production identity.
    let fixture = ReleaseFixture::create("success", false);
    let mut owned_core = fixture.spawn_core(None);
    let session_id = "session-w831-user-selected-123456";
    let mut client = fixture.connect(session_id, &mut owned_core);
    let start = start_scan(&mut client, session_id, "w831-user-start");
    let scan_id = start.payload["scanId"].as_str().expect("scanId").to_owned();
    let epoch = start.payload["eventStream"]["epoch"]
        .as_str()
        .expect("epoch")
        .to_owned();
    let completed = wait_for_completed_snapshot(&mut client, session_id, &scan_id);
    let candidate = classified_candidate(&completed);
    assert_eq!(candidate["candidate"]["sourceKind"], "user_selected");
    let candidate_id = candidate_id(candidate);
    let verify = verify_candidate(
        &mut client,
        session_id,
        "w831-user-verify",
        &scan_id,
        &candidate_id,
        5_000,
    );
    assert_eq!(verify.payload["accepted"], true);
    let verified =
        wait_for_candidate_lifecycle(&mut client, session_id, &scan_id, &candidate_id, "verified");
    let verified_candidate = classified_candidate(&verified);
    assert_eq!(verified_candidate["verification"]["status"], "verified");
    assert_eq!(verified_candidate["verification"]["protocolMajor"], 1);
    let invocations = fs::read_to_string(fixture.fixture_root.join("initialize.invocations"))
        .expect("read ACP fixture invocation ledger");
    assert_eq!(
        invocations.lines().count(),
        1,
        "UserSelected fixture must run exactly one initialize; zero session/prompt/tool"
    );

    // Count candidate_verified events for this candidate (the committed verify
    // receipt's public event evidence).
    let verified_event_count = |client: &mut NamedPipeConnection| -> usize {
        send_query(
            client,
            session_id,
            "w831-user-event-count",
            "events.replay",
            json!({
                "streamId": "local-discovery-events",
                "epoch": epoch,
                "afterSequence": 0,
                "limit": 32,
            }),
        );
        let replay = read_response(client, "verify replay event count");
        replay.payload["events"]
            .as_array()
            .expect("replayed event array")
            .iter()
            .filter(|event| {
                event["event"] == "agent.discovery.candidate_verified"
                    && event["payload"]["candidateId"] == candidate_id
            })
            .count()
    };
    let events_after_first_verify = verified_event_count(&mut client);
    assert_eq!(
        events_after_first_verify, 1,
        "the first verify must emit exactly one candidate_verified event"
    );

    // A retry with the same requestId and business intent but a different
    // deadline must replay the committed receipt exactly and must NOT run a
    // second ACP child/initialize or emit a second event.
    let replay = verify_candidate(
        &mut client,
        session_id,
        "w831-user-verify",
        &scan_id,
        &candidate_id,
        2_000,
    );
    assert_eq!(
        replay.payload, verify.payload,
        "the deadline-only replay must return the committed receipt payload verbatim"
    );
    let events_after_replay = verified_event_count(&mut client);
    assert_eq!(
        events_after_replay, events_after_first_verify,
        "a deadline-only replay must not grow the candidate_verified event count"
    );
    let invocations_after = fs::read_to_string(fixture.fixture_root.join("initialize.invocations"))
        .expect("read ACP fixture invocation ledger");
    assert_eq!(
        invocations_after.lines().count(),
        1,
        "a deadline-only replay must not run a second initialize"
    );

    shutdown(client, session_id, "w831-user-shutdown", &mut owned_core);
}
