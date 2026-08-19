use std::collections::BTreeSet;
use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use agenttalk_domain::{CompatibilityState, ObservationSourceKind};
use agenttalk_runtime_host::{
    discover_local_connectors_report_with_config, discover_windows_passive_report_with_config,
    install_local_discovery_fixture_worker_for_tests, ExplicitDiscoverySource,
    LocalConnectorDiscoveryConfig, LocalConnectorDiscoveryReport, WindowsAppPathRecord,
    WindowsLoopbackListenerRecord, WindowsPackageRecord, WindowsPassiveDiscoveryConfig,
    WindowsRegistryHive, WindowsRegistryView,
};
use serde_json::json;

const WORKER_ENV: &str = "AGENTTALK_LOCAL_DISCOVERY_WORKER_EXE";
const WORKER_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(5);
const SUITE_GUARD_HANDOFF_TIMEOUT: Duration = Duration::from_secs(90);

fn env_guard() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    match LOCK.get_or_init(|| Mutex::new(())).lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn unique_temp_path(name: &str) -> PathBuf {
    static NEXT_NONCE: AtomicUsize = AtomicUsize::new(0);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let nonce = NEXT_NONCE.fetch_add(1, Ordering::AcqRel);
    std::env::temp_dir().join(format!(
        "agenttalk-{name}-{}-{now:x}-{nonce:x}",
        std::process::id()
    ))
}

struct EnvVarGuard {
    key: &'static str,
    original: Option<OsString>,
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

    fn remove(key: &'static str) -> Self {
        let guard = Self {
            key,
            original: std::env::var_os(key),
        };
        std::env::remove_var(key);
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

struct TestTempDir {
    path: PathBuf,
}

impl TestTempDir {
    fn create(name: &str) -> Self {
        let path = unique_temp_path(name);
        std::fs::create_dir(&path).expect("create isolated test temp dir");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn join(&self, child: impl AsRef<Path>) -> PathBuf {
        self.path.join(child)
    }
}

impl Drop for TestTempDir {
    fn drop(&mut self) {
        if !self.path.exists() {
            return;
        }
        if let Err(error) = std::fs::remove_dir_all(&self.path) {
            let message = format!("remove owned local discovery worker test dir failed: {error}");
            if std::thread::panicking() {
                eprintln!("{message}");
            } else {
                panic!("{message}");
            }
        }
    }
}

fn temp_dirs_with_prefix(prefix: &str) -> BTreeSet<PathBuf> {
    std::fs::read_dir(std::env::temp_dir())
        .expect("read temp dir")
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let path = entry.path();
            let name = path.file_name()?.to_str()?;
            name.starts_with(prefix).then_some(path)
        })
        .collect()
}

fn assert_no_new_temp_dirs(prefix: &str, baseline: &BTreeSet<PathBuf>) {
    let current = temp_dirs_with_prefix(prefix);
    let added = current.difference(baseline).collect::<Vec<_>>();
    assert!(
        added.is_empty(),
        "new owned temp directories remain for {prefix}: {added:?}"
    );
    for path in baseline {
        assert!(
            path.exists(),
            "pre-existing temp baseline was touched: {}",
            path.display()
        );
    }
}

fn safe_report_evidence(report: &LocalConnectorDiscoveryReport) -> String {
    let candidates = report
        .candidates
        .iter()
        .map(|candidate| {
            json!({
                "connectorId": candidate.connector_id,
                "runtimeType": candidate.runtime_type,
                "displayName": candidate.display_name,
                "availability": candidate.availability,
                "models": candidate.models,
                "catalogRevision": candidate.catalog_revision,
                "source": candidate.source,
                "requiresConfiguration": candidate.requires_configuration,
            })
        })
        .collect::<Vec<_>>();
    let projections = report
        .projections
        .iter()
        .map(|candidate| {
            json!({
                "candidateId": candidate.candidate_id,
                "category": candidate.category,
                "connectorId": candidate.connector_id,
                "runtimeType": candidate.runtime_type,
                "displayName": candidate.display_name,
                "availability": candidate.availability,
                "sourceKind": candidate.source_kind,
                "sourceKinds": candidate.source_kinds,
                "requiresConfiguration": candidate.requires_configuration,
                "compatibilityState": candidate.compatibility_state,
                "authState": candidate.auth_state,
                "healthState": candidate.health_state,
                "discoveryState": candidate.discovery_state,
                "diagnostics": candidate.diagnostics,
            })
        })
        .collect::<Vec<_>>();
    serde_json::to_string_pretty(&json!({
        "candidates": candidates,
        "projections": projections,
        "diagnostics": report.diagnostics,
    }))
    .expect("serialize safe discovery report evidence")
}

fn assert_connector_ids(report: &LocalConnectorDiscoveryReport, expected: &[&str], context: &str) {
    let connector_ids = report
        .candidates
        .iter()
        .map(|candidate| candidate.connector_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        connector_ids,
        expected,
        "{context}:\n{}",
        safe_report_evidence(report)
    );
}

fn install_fixture_worker() -> agenttalk_runtime_host::LocalDiscoveryFixtureWorkerGuard {
    install_local_discovery_fixture_worker_for_tests(env!(
        "CARGO_BIN_EXE_agenttalk-local-discovery-worker"
    ))
    .expect("install fixture discovery worker")
}

fn create_local_worker_fixture(name: &str) -> (TestTempDir, PathBuf, PathBuf) {
    let root = TestTempDir::create(name);
    let data_dir = root.join("kun-data");
    let codex_binary = root.join("codex.exe");
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::write(&codex_binary, b"fixture codex executable").unwrap();
    std::fs::write(
        data_dir.join("runtime.json"),
        serde_json::to_vec(&json!({
            "version": 2,
            "pid": 1111,
            "host": "127.0.0.1",
            "port": 41111,
            "instanceId": "worker-kun-instance",
            "runtimeToken": "fixture-token-must-not-leak",
            "serviceVersion": "0.2.37",
            "buildId": "worker-build",
            "launchMode": "shared"
        }))
        .unwrap(),
    )
    .unwrap();
    (root, data_dir, codex_binary)
}

fn run_local_worker_fixture(name: &str) -> LocalConnectorDiscoveryReport {
    let (_root, data_dir, codex_binary) = create_local_worker_fixture(name);
    discover_local_connectors_report_with_config(&LocalConnectorDiscoveryConfig {
        codex_binary_paths: vec![codex_binary],
        kun_data_dirs: vec![data_dir],
        kun_install_dirs: Vec::new(),
        kun_expected_service_version: "0.2.34".into(),
        request_timeout: WORKER_DISCOVERY_TIMEOUT,
    })
}

fn restore_env_after_unwind<F>(after_snapshot: F)
where
    F: FnOnce(),
{
    let _guard = env_guard();
    let original_path = std::env::var_os("PATH");
    let original_worker = std::env::var_os(WORKER_ENV);
    after_snapshot();
    let result = std::panic::catch_unwind(|| {
        let _path = EnvVarGuard::set("PATH", "agenttalk-w37-mutated-path");
        let _worker = EnvVarGuard::set(WORKER_ENV, "agenttalk-w37-mutated-worker");
        panic!("intentional env unwind");
    });
    assert!(result.is_err());
    assert_eq!(std::env::var_os("PATH"), original_path);
    assert_eq!(std::env::var_os(WORKER_ENV), original_worker);
}

fn run_concurrent_env_restore(
    attempting_snapshot: Sender<()>,
    snapshot_ready: Sender<()>,
    continue_after_snapshot: Receiver<()>,
) {
    attempting_snapshot
        .send(())
        .expect("environment restore attempt receiver");
    restore_env_after_unwind(|| {
        snapshot_ready
            .send(())
            .expect("environment restore snapshot receiver");
        continue_after_snapshot
            .recv()
            .expect("environment restore continuation");
    });
}

#[test]
fn poisoned_suite_guard_can_be_reacquired() {
    let result = std::panic::catch_unwind(|| {
        let _guard = env_guard();
        panic!("intentional suite guard poison");
    });
    assert!(result.is_err());
    drop(env_guard());
}

#[test]
fn env_vars_are_restored_after_unwind() {
    restore_env_after_unwind(|| {});
}

#[test]
fn concurrent_env_restore_waits_for_suite_guard_before_snapshot() {
    let (original_path, original_worker) = {
        let _guard = env_guard();
        (std::env::var_os("PATH"), std::env::var_os(WORKER_ENV))
    };
    let (a_ready_tx, a_ready_rx) = mpsc::channel();
    let (a_release_tx, a_release_rx) = mpsc::channel();
    let thread_a = std::thread::spawn(move || {
        let _guard = env_guard();
        let _path = EnvVarGuard::set("PATH", "agenttalk-w37-thread-a-path");
        let _worker = EnvVarGuard::set(WORKER_ENV, "agenttalk-w37-thread-a-worker");
        a_ready_tx.send(()).expect("thread A ready receiver");
        a_release_rx.recv().expect("thread A release");
    });
    a_ready_rx
        .recv()
        .expect("thread A did not acquire suite guard");

    let (snapshot_tx, snapshot_rx) = mpsc::channel();
    let (continue_tx, continue_rx) = mpsc::channel();
    let (attempt_tx, attempt_rx) = mpsc::channel();
    let thread_b = std::thread::spawn(move || {
        run_concurrent_env_restore(attempt_tx, snapshot_tx, continue_rx)
    });

    attempt_rx
        .recv()
        .expect("thread B did not begin the environment snapshot flow");
    let early_snapshot = snapshot_rx.recv_timeout(Duration::from_millis(200));

    a_release_tx.send(()).expect("release thread A");
    thread_a.join().expect("thread A panicked");

    let late_snapshot = snapshot_rx.recv_timeout(SUITE_GUARD_HANDOFF_TIMEOUT);
    continue_tx
        .send(())
        .expect("release thread B snapshot gate");
    let thread_b_result = thread_b.join();

    {
        let _guard = env_guard();
        assert!(
            std::env::var_os("PATH") == original_path,
            "PATH was not restored to the pre-test value after both workers joined"
        );
        assert!(
            std::env::var_os(WORKER_ENV) == original_worker,
            "worker environment value was not restored to the pre-test value after both workers joined"
        );
    }

    assert!(
        early_snapshot.is_err(),
        "thread B completed its environment snapshot while thread A held the suite guard"
    );
    assert!(
        late_snapshot.is_ok(),
        "thread B did not complete its snapshot after thread A released the suite guard"
    );
    thread_b_result.expect("thread B environment restoration panicked");
}

#[test]
fn temp_root_is_removed_after_unwind() {
    let owned_root = Arc::new(Mutex::new(None::<std::path::PathBuf>));
    let root_for_unwind = Arc::clone(&owned_root);
    let result = std::panic::catch_unwind(move || {
        let root = TestTempDir::create("w36-unwind-temp");
        *root_for_unwind.lock().unwrap() = Some(root.path().to_owned());
        panic!("intentional temp root unwind");
    });
    assert!(result.is_err());
    let root = owned_root
        .lock()
        .unwrap()
        .clone()
        .expect("root recorded before unwind");
    assert!(
        !root.exists(),
        "temp root remained after unwind: {}",
        root.display()
    );
}

#[test]
fn temp_root_normal_run_removes_owned_directory() {
    let root = {
        let root = TestTempDir::create("w36-normal-temp");
        assert!(root.path().is_dir());
        root.path().to_owned()
    };
    assert!(
        !root.exists(),
        "temp root remained after normal run: {}",
        root.display()
    );
}

#[test]
fn repeated_and_concurrent_temp_roots_are_unique_and_cleaned() {
    let baseline = temp_dirs_with_prefix("agenttalk-w36-concurrent-temp-");
    let mut roots = BTreeSet::new();
    for _ in 0..3 {
        let root = {
            let root = TestTempDir::create("w36-concurrent-temp");
            assert!(root.path().is_dir());
            root.path().to_owned()
        };
        assert!(roots.insert(root.clone()));
        assert!(
            !root.exists(),
            "repeated temp root remained: {}",
            root.display()
        );
    }

    let handles = (0..4)
        .map(|_| {
            std::thread::spawn(|| {
                let root = TestTempDir::create("w36-concurrent-temp");
                let path = root.path().to_owned();
                assert!(path.is_dir());
                path
            })
        })
        .collect::<Vec<_>>();
    for handle in handles {
        let root = handle.join().expect("join temp root thread");
        assert!(roots.insert(root.clone()));
        assert!(
            !root.exists(),
            "concurrent temp root remained: {}",
            root.display()
        );
    }
    assert_no_new_temp_dirs("agenttalk-w36-concurrent-temp-", &baseline);
}

#[test]
fn release_build_waits_for_discovery_suite_guard() {
    let root = TestTempDir::create("w36-fake-cargo");
    let fake_cargo = root.join(if cfg!(windows) {
        "fake-cargo.cmd"
    } else {
        "fake-cargo.sh"
    });
    if cfg!(windows) {
        std::fs::write(
            &fake_cargo,
            b"@echo off\r\nmkdir \"%CARGO_TARGET_DIR%\\release\"\r\necho worker>\"%CARGO_TARGET_DIR%\\release\\agenttalk-local-discovery-worker.exe\"\r\n",
        )
        .unwrap();
    } else {
        std::fs::write(
            &fake_cargo,
            b"#!/bin/sh\nmkdir -p \"$CARGO_TARGET_DIR/release\"\nprintf worker > \"$CARGO_TARGET_DIR/release/agenttalk-local-discovery-worker\"\n",
        )
        .unwrap();
    }
    let suite = env_guard();
    let started = Arc::new(AtomicBool::new(false));
    let finished = Arc::new(AtomicBool::new(false));
    let started_for_thread = Arc::clone(&started);
    let finished_for_thread = Arc::clone(&finished);
    let cargo_for_thread = fake_cargo.clone();
    let handle = std::thread::spawn(move || {
        started_for_thread.store(true, Ordering::Release);
        run_serialized_release_build_has_no_handle_probe_binary_target(
            TestTempDir::create("w36-release-guard-probe"),
            cargo_for_thread.into_os_string(),
        );
        finished_for_thread.store(true, Ordering::Release);
    });

    let start = Instant::now();
    while !started.load(Ordering::Acquire) && start.elapsed() < Duration::from_millis(500) {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        started.load(Ordering::Acquire),
        "release thread did not start"
    );
    std::thread::sleep(Duration::from_millis(200));
    assert!(
        !finished.load(Ordering::Acquire),
        "release build completed while discovery suite guard was held"
    );
    drop(suite);

    handle.join().expect("join release build guard probe");
}

#[test]
fn release_build_failure_cleans_owned_target_dir() {
    let _guard = env_guard();
    let root = TestTempDir::create("w36-fake-cargo-failure");
    let target = TestTempDir::create("w36-release-failure-target");
    let target_dir = target.path().to_owned();
    let fake_cargo = root.join(if cfg!(windows) {
        "fake-cargo-failure.cmd"
    } else {
        "fake-cargo-failure.sh"
    });
    if cfg!(windows) {
        std::fs::write(
            &fake_cargo,
            b"@echo off\r\nmkdir \"%CARGO_TARGET_DIR%\\release\"\r\necho worker>\"%CARGO_TARGET_DIR%\\release\\agenttalk-local-discovery-worker.exe\"\r\nexit /b 9\r\n",
        )
        .unwrap();
    } else {
        std::fs::write(
            &fake_cargo,
            b"#!/bin/sh\nmkdir -p \"$CARGO_TARGET_DIR/release\"\nprintf worker > \"$CARGO_TARGET_DIR/release/agenttalk-local-discovery-worker\"\nexit 9\n",
        )
        .unwrap();
    }
    let result = std::panic::catch_unwind(move || {
        run_release_build_has_no_handle_probe_binary_target(target, fake_cargo.into_os_string());
    });
    assert!(result.is_err());
    assert!(
        !target_dir.exists(),
        "failed release build target dir should be cleaned: {}",
        target_dir.display()
    );
}

#[test]
fn public_local_discovery_runs_codex_and_kun_providers_inside_real_worker() {
    let _guard = env_guard();
    let _worker_env = EnvVarGuard::remove(WORKER_ENV);
    let _worker_guard = install_fixture_worker();

    let report = run_local_worker_fixture("local-discovery-worker");

    assert_connector_ids(
        &report,
        &["local.codex", "local.kun.shared-runtime"],
        "local worker discovery must return Codex and Kun",
    );
    assert!(
        report.diagnostics.is_empty(),
        "unexpected safe diagnostics:\n{}",
        safe_report_evidence(&report)
    );
    assert!(report
        .candidates
        .iter()
        .all(|candidate| candidate.availability == "unconfigured"));
    let serialized = serde_json::to_string(&report).unwrap();
    assert!(!serialized.contains("fixture-token-must-not-leak"));
    assert!(!serialized.contains("runtime.json"));
    assert!(!serialized.contains("41111"));
}

#[test]
fn first_discovery_assertion_failure_does_not_pollute_following_fixture() {
    let _guard = env_guard();
    let baseline = temp_dirs_with_prefix("agenttalk-local-discovery-worker-");
    let result = std::panic::catch_unwind(|| {
        let _worker_env = EnvVarGuard::remove(WORKER_ENV);
        let _worker_guard = install_fixture_worker();
        let report = run_local_worker_fixture("local-discovery-worker");
        assert_connector_ids(
            &report,
            &["intentionally.wrong"],
            "intentional discovery assertion failure",
        );
    });
    assert!(result.is_err());

    {
        let _worker_env = EnvVarGuard::remove(WORKER_ENV);
        let _worker_guard = install_fixture_worker();
        let report = run_local_worker_fixture("local-discovery-worker");
        assert_connector_ids(
            &report,
            &["local.codex", "local.kun.shared-runtime"],
            "following discovery after panic must still run",
        );
    }
    assert_no_new_temp_dirs("agenttalk-local-discovery-worker-", &baseline);
}

#[test]
fn repeated_worker_fixture_runs_leave_zero_new_temp_directories() {
    let _guard = env_guard();
    let _worker_env = EnvVarGuard::remove(WORKER_ENV);
    let _worker_guard = install_fixture_worker();
    let baseline = temp_dirs_with_prefix("agenttalk-local-discovery-worker-");
    for _ in 0..3 {
        let report = run_local_worker_fixture("local-discovery-worker");
        assert_connector_ids(
            &report,
            &["local.codex", "local.kun.shared-runtime"],
            "repeated local discovery worker fixture",
        );
        assert_no_new_temp_dirs("agenttalk-local-discovery-worker-", &baseline);
    }
}

#[test]
fn public_windows_passive_discovery_runs_sources_inside_real_worker() {
    let _guard = env_guard();
    let _worker_guard = install_fixture_worker();

    let root = TestTempDir::create("windows-passive-worker");
    let path_dir = root.join("bin");
    let package_root = root.join("package");
    std::fs::create_dir_all(&path_dir).unwrap();
    std::fs::create_dir_all(package_root.join("app")).unwrap();
    let executable = path_dir.join("worker-passive-agent.exe");
    let package_executable = package_root.join("app").join("worker-passive-agent.exe");
    std::fs::write(&executable, b"worker passive executable").unwrap();
    std::fs::copy(&executable, &package_executable).unwrap();

    let report = discover_windows_passive_report_with_config(&WindowsPassiveDiscoveryConfig {
        path_env: Some(path_dir.display().to_string()),
        app_path_records: vec![WindowsAppPathRecord {
            key_name: "worker-passive-agent.exe".into(),
            executable_path: executable.clone(),
            hive: WindowsRegistryHive::CurrentUser,
            view: WindowsRegistryView::Native,
        }],
        package_records: vec![WindowsPackageRecord {
            package_family_name: "Worker.Package_fixture".into(),
            package_full_name: "Worker.Package_fixture_1.0.0.0_x64__fixture".into(),
            version: "1.0.0.0".into(),
            installed_location: package_root,
            executable_relative_path: std::path::PathBuf::from("app")
                .join("worker-passive-agent.exe"),
        }],
        loopback_records: vec![
            WindowsLoopbackListenerRecord {
                address: "127.0.0.1".into(),
                port: 48001,
                owner_pid: 7777,
                owner_executable: Some(executable.clone()),
                owner_identity: Some("worker".into()),
            },
            WindowsLoopbackListenerRecord {
                address: "10.0.0.4".into(),
                port: 48002,
                owner_pid: 8888,
                owner_executable: Some(executable.clone()),
                owner_identity: Some("lan".into()),
            },
        ],
        loopback_recheck_records: None,
        explicit_sources: vec![
            ExplicitDiscoverySource::Executable(executable.clone()),
            ExplicitDiscoverySource::Endpoint("http://192.168.1.2:48003".into()),
        ],
        use_real_app_paths: false,
        use_real_packages: false,
        use_real_loopback: false,
        max_results: 8,
        max_path_entries: 16,
        max_candidates_per_path_entry: 16,
        request_timeout: WORKER_DISCOVERY_TIMEOUT,
    });

    assert_eq!(
        report.projections.len(),
        2,
        "windows passive worker discovery projection count:\n{}",
        safe_report_evidence(&report)
    );
    let merged = report
        .projections
        .iter()
        .find(|candidate| candidate.source_kinds != vec![ObservationSourceKind::WindowsPackage])
        .unwrap_or_else(|| {
            panic!(
                "merged executable candidate:\n{}",
                safe_report_evidence(&report)
            )
        });
    assert_eq!(
        merged.source_kinds,
        vec![
            ObservationSourceKind::WindowsPath,
            ObservationSourceKind::WindowsAppPath,
            ObservationSourceKind::LoopbackListener,
            ObservationSourceKind::UserSelected,
        ]
    );
    assert_eq!(merged.compatibility_state, CompatibilityState::NotVerified);
    assert!(merged.requires_configuration);
    let package = report
        .projections
        .iter()
        .find(|candidate| candidate.source_kinds == vec![ObservationSourceKind::WindowsPackage])
        .unwrap_or_else(|| panic!("package candidate:\n{}", safe_report_evidence(&report)));
    assert_eq!(package.compatibility_state, CompatibilityState::NotVerified);
    assert!(package.requires_configuration);
    assert!(report
        .diagnostics
        .iter()
        .any(|diagnostic| { diagnostic.source_kind == ObservationSourceKind::LoopbackListener }));
    assert!(report
        .diagnostics
        .iter()
        .any(|diagnostic| { diagnostic.source_kind == ObservationSourceKind::UserSelected }));
    let serialized = serde_json::to_string(&report).unwrap();
    for forbidden in [
        &root.path().display().to_string(),
        "48001",
        "48002",
        "48003",
        "7777",
        "8888",
        "Worker.Package",
    ] {
        assert!(!serialized.contains(forbidden));
    }
}

#[test]
fn production_discovery_does_not_accept_environment_worker_override() {
    let _guard = env_guard();
    let _worker_env = EnvVarGuard::set(
        WORKER_ENV,
        env!("CARGO_BIN_EXE_agenttalk-local-discovery-worker"),
    );

    let root = TestTempDir::create("local-discovery-env-override");
    let data_dir = root.join("kun-data");
    let codex_binary = root.join("codex.exe");
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::write(&codex_binary, b"fixture codex executable").unwrap();
    std::fs::write(
        data_dir.join("runtime.json"),
        serde_json::to_vec(&json!({
            "version": 2,
            "pid": 1111,
            "host": "127.0.0.1",
            "port": 41111,
            "instanceId": "env-override-kun-instance",
            "serviceVersion": "0.2.37",
            "buildId": "worker-build",
            "launchMode": "shared"
        }))
        .unwrap(),
    )
    .unwrap();

    let report = discover_local_connectors_report_with_config(&LocalConnectorDiscoveryConfig {
        codex_binary_paths: vec![codex_binary],
        kun_data_dirs: vec![data_dir],
        kun_install_dirs: Vec::new(),
        kun_expected_service_version: "0.2.34".into(),
        request_timeout: Duration::from_secs(1),
    });

    assert!(
        report.candidates.is_empty(),
        "production discovery must ignore arbitrary worker env override:\n{}",
        safe_report_evidence(&report)
    );
    assert!(
        !report.diagnostics.is_empty(),
        "expected safe diagnostic for ignored env override"
    );
}

#[test]
fn production_discovery_does_not_search_path_for_worker() {
    let _guard = env_guard();
    let root = TestTempDir::create("local-discovery-path-worker");
    let fake_path_dir = root.join("path");
    let data_dir = root.join("kun-data");
    let codex_binary = root.join("codex.exe");
    std::fs::create_dir_all(&fake_path_dir).unwrap();
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::copy(
        env!("CARGO_BIN_EXE_agenttalk-local-discovery-worker"),
        fake_path_dir.join(if cfg!(windows) {
            "agenttalk-local-discovery-worker.exe"
        } else {
            "agenttalk-local-discovery-worker"
        }),
    )
    .unwrap();
    std::fs::write(&codex_binary, b"fixture codex executable").unwrap();
    let _path = EnvVarGuard::set("PATH", &fake_path_dir);

    let report = discover_local_connectors_report_with_config(&LocalConnectorDiscoveryConfig {
        codex_binary_paths: vec![codex_binary],
        kun_data_dirs: vec![data_dir],
        kun_install_dirs: Vec::new(),
        kun_expected_service_version: "0.2.34".into(),
        request_timeout: Duration::from_secs(1),
    });

    assert!(
        report.candidates.is_empty(),
        "production discovery must not PATH-search worker executable:\n{}",
        safe_report_evidence(&report)
    );
    assert!(
        !report.diagnostics.is_empty(),
        "expected safe diagnostic for missing production worker"
    );
}

#[test]
fn release_build_has_no_handle_probe_binary_target() {
    let _guard = env_guard();
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    run_release_build_has_no_handle_probe_binary_target(
        TestTempDir::create("w3-release-target"),
        cargo,
    );
}

fn run_serialized_release_build_has_no_handle_probe_binary_target(
    target: TestTempDir,
    cargo: OsString,
) {
    let _guard = env_guard();
    run_release_build_has_no_handle_probe_binary_target(target, cargo);
}

fn run_release_build_has_no_handle_probe_binary_target(target: TestTempDir, cargo: OsString) {
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir
        .parent()
        .and_then(std::path::Path::parent)
        .expect("workspace root");
    let target_dir = target.path().to_owned();
    let output = std::process::Command::new(cargo)
        .current_dir(workspace_root)
        .env("CARGO_TARGET_DIR", &target_dir)
        .args(["build", "--workspace", "--release", "--locked"])
        .output()
        .expect("run isolated release build");
    assert!(
        output.status.success(),
        "isolated release build failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let release_dir = target_dir.join("release");
    let release_worker = release_dir.join(if cfg!(windows) {
        "agenttalk-local-discovery-worker.exe"
    } else {
        "agenttalk-local-discovery-worker"
    });
    assert!(
        release_worker.is_file(),
        "release local discovery worker must exist in isolated target dir"
    );
    let probe_name = if cfg!(windows) {
        "agenttalk-handle-inheritance-probe.exe"
    } else {
        "agenttalk-handle-inheritance-probe"
    };
    assert!(!release_dir.join(probe_name).exists());
    assert!(!release_dir
        .join("agenttalk-handle-inheritance-probe.pdb")
        .exists());
    let probe_artifacts = paths_containing(&target_dir, "agenttalk-handle-inheritance-probe");
    assert!(
        probe_artifacts.is_empty(),
        "probe artifacts must not be generated by release build: {probe_artifacts:?}"
    );

    let bytes = std::fs::read(&release_worker).expect("read release worker binary");
    assert!(!bytes
        .windows(b"HandleProbe".len())
        .any(|window| window == b"HandleProbe"));
    assert!(!bytes
        .windows(b"handle_probe".len())
        .any(|window| window == b"handle_probe"));
}

fn paths_containing(root: &std::path::Path, needle: &str) -> Vec<std::path::PathBuf> {
    let mut matches = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(path) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&path) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.contains(needle))
            {
                matches.push(path.clone());
            }
            if path.is_dir() {
                stack.push(path);
            }
        }
    }
    matches
}

#[test]
#[ignore]
fn real_windows_passive_scan_prints_local_manifest_candidates() {
    let _guard = env_guard();
    let _worker_guard = install_fixture_worker();
    let report = discover_windows_passive_report_with_config(&WindowsPassiveDiscoveryConfig {
        path_env: std::env::var("PATH").ok(),
        use_real_app_paths: false,
        use_real_packages: false,
        use_real_loopback: false,
        request_timeout: Duration::from_secs(10),
        max_path_entries: 25,
        max_candidates_per_path_entry: 8,
        max_results: 128,
        ..WindowsPassiveDiscoveryConfig::default()
    });
    let manifests = agenttalk_runtime_host::load_local_manifest_directory(
        agenttalk_runtime_host::default_local_manifest_directory()
            .as_deref()
            .expect("LOCALAPPDATA is available"),
    )
    .snapshot
    .manifests;
    let session = report.classify_acp(
        &manifests,
        Instant::now() + Duration::from_secs(5),
        &AtomicBool::new(false),
    );
    let ids: Vec<String> = session
        .projections()
        .iter()
        .map(|p| p.connector_id.clone())
        .collect();
    eprintln!("passive projections: {:?}", report.projections.len());
    for p in &report.projections {
        eprintln!(
            " projection {} {} {:?}",
            p.connector_id, p.display_name, p.verification_authority
        );
    }
    eprintln!("passive diagnostics: {:?}", report.diagnostics);
    eprintln!("manifest count: {}", manifests.len());
    eprintln!(
        "manifest ids: {:?}",
        manifests
            .iter()
            .map(|m| (m.id.clone(), format!("{:?}", m.launch), m.protocol.clone()))
            .collect::<Vec<_>>()
    );
    eprintln!("ACP projection ids: {ids:?}");
    assert!(
        ids.contains(&"local.dsh-acp".to_string()),
        "missing dsh in {ids:?}"
    );
    for p in session.projections() {
        eprintln!(
            "{} verification={:?}",
            p.connector_id, p.verification_authority
        );
    }
}
