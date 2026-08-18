use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, BufReader, Cursor, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
#[cfg(test)]
use std::sync::{Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};

use agenttalk_domain::{
    AuthState, CandidateAvailability, CandidateCategory, CandidateProjection, CompatibilityState,
    DiscoveryDiagnostic, DiscoveryDiagnosticCode, DiscoveryEvidence, DiscoveryPolicy,
    DiscoveryState, HealthState, ObservationSourceKind, ObservationTrustLevel,
    VerificationAuthority,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

#[path = "discovery/catalog.rs"]
pub(crate) mod catalog;
#[path = "discovery/manifest.rs"]
pub(crate) mod manifest;
#[path = "discovery/verifiers.rs"]
pub(crate) mod verifiers;

const CONFLICT_CONNECTOR_ID: &str = "local.discovery.conflict";
const UNKNOWN_RUNTIME_TYPE: &str = "unknown";
const OBSERVATION_CAP_MULTIPLIER: usize = 32;
const OBSERVATION_CAP_MINIMUM: usize = 128;
const WORKER_PROTOCOL_VERSION: u16 = 1;
const WORKER_FRAME_MAGIC: &str = "AGENTTALK_LOCAL_DISCOVERY_WORKER_V1";
const WORKER_PROTOCOL_ID: &str = "agenttalk.local-discovery-worker.v1";
const WORKER_BUILD_ID: &str = concat!(
    env!("CARGO_PKG_NAME"),
    ":",
    env!("CARGO_PKG_VERSION"),
    ":local-discovery-worker"
);
const MAX_WORKER_REQUEST_BYTES: usize = 64 * 1024;
const MAX_WORKER_RESPONSE_BYTES: usize = 1024 * 1024;
const MAX_WORKER_STDERR_BYTES: usize = 16 * 1024;
const MANAGED_CLEANUP_GRACE: Duration = Duration::from_millis(500);

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) enum ObservationLocator {
    Executable(PathBuf),
    Endpoint { endpoint_ref: String },
    RuntimeRecord { runtime_json: PathBuf },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ObservationFingerprint {
    stable_id: String,
}

impl ObservationFingerprint {
    pub(crate) fn from_parts(parts: &[String]) -> Self {
        let mut hasher = Sha256::new();
        for part in parts {
            hasher.update(part.as_bytes());
            hasher.update([0xff]);
        }
        let digest = hasher.finalize();
        let stable_id = digest
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        Self { stable_id }
    }

    pub(crate) fn candidate_id(&self) -> String {
        format!("candidate-{}", self.stable_id)
    }

    fn from_stable_id(stable_id: String) -> Self {
        Self { stable_id }
    }

    fn identity_key(&self) -> String {
        self.stable_id.clone()
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct Observation {
    pub(crate) locator: ObservationLocator,
    pub(crate) fingerprint: ObservationFingerprint,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) association_fingerprints: Vec<ObservationFingerprint>,
    pub(crate) source_kind: ObservationSourceKind,
    pub(crate) category: CandidateCategory,
    pub(crate) trust_level: ObservationTrustLevel,
    pub(crate) verification_authority: VerificationAuthority,
    pub(crate) availability_authority: VerificationAuthority,
    pub(crate) discovery_authority: VerificationAuthority,
    pub(crate) compatibility_authority: VerificationAuthority,
    pub(crate) auth_authority: VerificationAuthority,
    pub(crate) health_authority: VerificationAuthority,
    pub(crate) connector_id: String,
    pub(crate) runtime_type: String,
    pub(crate) display_name: String,
    pub(crate) availability: CandidateAvailability,
    pub(crate) models: Vec<String>,
    pub(crate) catalog_revision: Option<String>,
    pub(crate) requires_configuration: bool,
    pub(crate) discovery_state: DiscoveryState,
    pub(crate) compatibility_state: CompatibilityState,
    pub(crate) auth_state: AuthState,
    pub(crate) health_state: HealthState,
    pub(crate) evidence_summary: Vec<DiscoveryEvidence>,
    pub(crate) diagnostics: Vec<DiscoveryDiagnostic>,
}

impl Observation {
    pub(crate) fn candidate_id(&self) -> String {
        self.fingerprint.candidate_id()
    }

    fn primary_identity_key(&self) -> String {
        self.fingerprint.identity_key()
    }

    fn identity_keys(&self) -> BTreeSet<String> {
        let mut keys = BTreeSet::from([self.primary_identity_key()]);
        keys.extend(
            self.association_fingerprints
                .iter()
                .map(ObservationFingerprint::identity_key),
        );
        keys
    }

    fn package_primary_identity_key(&self) -> Option<String> {
        (self.source_kind == ObservationSourceKind::WindowsPackage)
            .then(|| self.primary_identity_key())
    }

    pub(crate) fn executable_locator(&self) -> Option<&Path> {
        match &self.locator {
            ObservationLocator::Executable(path) => Some(path),
            ObservationLocator::Endpoint { .. } | ObservationLocator::RuntimeRecord { .. } => None,
        }
    }

    pub(crate) fn matches_windows_executable_identity(&self, stable_identity: &str) -> bool {
        let expected = ObservationFingerprint::from_parts(&[
            "windows-executable".to_owned(),
            stable_identity.to_owned(),
        ]);
        self.identity_keys().contains(&expected.identity_key())
    }

    #[cfg(test)]
    pub(crate) fn project(&self) -> CandidateProjection {
        self.project_with_candidate_id(self.candidate_id())
    }

    fn project_with_candidate_id(&self, candidate_id: String) -> CandidateProjection {
        let mut diagnostics = self.diagnostics.clone();
        let connector_id = project_identifier(&self.connector_id);
        let runtime_type = project_identifier(&self.runtime_type);
        let display_name = project_display_name(&self.display_name);
        let mut availability = self.availability;
        let mut compatibility_state = self.compatibility_state;
        let mut auth_state = self.auth_state;
        let mut health_state = self.health_state;
        let mut requires_configuration = self.requires_configuration;
        if self.trust_level == ObservationTrustLevel::Untrusted {
            availability = CandidateAvailability::Unavailable;
            requires_configuration = true;
        }
        if connector_id.is_none() || runtime_type.is_none() || display_name.is_none() {
            availability = CandidateAvailability::Unavailable;
            compatibility_state = CompatibilityState::Incompatible;
            auth_state = AuthState::Unknown;
            health_state = HealthState::Unavailable;
            requires_configuration = true;
            push_diagnostic(
                &mut diagnostics,
                self.source_kind,
                DiscoveryDiagnosticCode::InvalidIdentity,
            );
        }
        let models = project_model_ids(&self.models);
        let catalog_revision = self
            .catalog_revision
            .as_deref()
            .and_then(project_catalog_revision);
        let has_catalog = catalog_revision.is_some() || !models.is_empty();
        CandidateProjection {
            candidate_id,
            category: self.category,
            connector_id: connector_id.unwrap_or_else(|| CONFLICT_CONNECTOR_ID.to_owned()),
            runtime_type: runtime_type.unwrap_or_else(|| UNKNOWN_RUNTIME_TYPE.to_owned()),
            display_name: display_name.unwrap_or_else(|| "Local Agent".to_owned()),
            availability,
            models,
            catalog_revision,
            requires_configuration,
            source_kind: self.source_kind,
            source_kinds: vec![self.source_kind],
            trust_level: self.trust_level,
            verification_authority: self.verification_authority,
            availability_authority: self.availability_authority,
            discovery_authority: self.discovery_authority,
            compatibility_authority: self.compatibility_authority,
            auth_authority: self.auth_authority,
            health_authority: self.health_authority,
            catalog_source_kind: has_catalog.then_some(self.source_kind),
            catalog_trust_level: has_catalog.then_some(self.trust_level),
            catalog_authority: has_catalog.then_some(self.verification_authority),
            discovery_state: self.discovery_state,
            compatibility_state,
            auth_state,
            health_state,
            evidence_summary: self.evidence_summary.clone(),
            diagnostics,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DiscoveryReport {
    pub(crate) candidates: Vec<CandidateProjection>,
    pub(crate) diagnostics: Vec<DiscoveryDiagnostic>,
    pub(crate) candidate_observations: BTreeMap<String, Vec<Observation>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DiscoveryProviderError {
    pub(crate) source_kind: ObservationSourceKind,
    pub(crate) code: DiscoveryDiagnosticCode,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DiscoveryProviderExecution {
    ManagedWorkerRequired,
    #[cfg(test)]
    InlineAllowedForTests,
}

#[derive(Default)]
pub(crate) struct DiscoveryCoordinator {
    providers: Vec<Arc<dyn DiscoveryProvider>>,
}

impl DiscoveryCoordinator {
    pub(crate) fn new(providers: Vec<Box<dyn DiscoveryProvider>>) -> Self {
        Self {
            providers: providers.into_iter().map(Arc::from).collect(),
        }
    }

    #[cfg(test)]
    pub(crate) fn discover(
        &self,
        policy: &DiscoveryPolicy,
        cancelled: &AtomicBool,
    ) -> Vec<CandidateProjection> {
        self.discover_report(policy, cancelled).candidates
    }

    pub(crate) fn discover_report(
        &self,
        policy: &DiscoveryPolicy,
        cancelled: &AtomicBool,
    ) -> DiscoveryReport {
        if policy.max_results == 0 {
            return DiscoveryReport {
                candidates: Vec::new(),
                diagnostics: Vec::new(),
                candidate_observations: BTreeMap::new(),
            };
        }
        let deadline = Instant::now() + Duration::from_millis(policy.timeout_ms);
        let mut merged = CandidateMergeSet::default();
        let mut diagnostics = Vec::new();
        for provider in &self.providers {
            if cancelled.load(Ordering::Acquire) || Instant::now() >= deadline {
                break;
            }
            let context = DiscoveryContext {
                _policy: policy.clone(),
                deadline,
                cancelled,
                observation_budget: DiscoveryBudget::new(observation_cap(policy.max_results)),
            };
            let provider_source = provider.source_kind();
            let (result, observations, already_budgeted, stop_on_error) =
                if let Some(spec) = provider.managed_process(&context) {
                    match collect_managed_process(provider_source, spec, &context) {
                        Ok(outcome) => {
                            diagnostics.extend(outcome.diagnostics);
                            (Ok(()), outcome.observations, false, false)
                        }
                        Err(error) => (Err(error), Vec::new(), false, true),
                    }
                } else if !provider.execution().allows_inline() {
                    (
                        Err(DiscoveryProviderError {
                            source_kind: provider_source,
                            code: DiscoveryDiagnosticCode::ProviderFailed,
                        }),
                        Vec::new(),
                        false,
                        true,
                    )
                } else {
                    let mut observations = Vec::new();
                    let mut accepted_identities = merged.acceptance_snapshot();
                    let mut accepted_unique_candidates = merged.len();
                    let mut emit = |observation: Observation| -> bool {
                        if cancelled.load(Ordering::Acquire) {
                            return false;
                        }
                        if context.should_stop() || !context.try_take_observation() {
                            return false;
                        }
                        let observation_keys = observation.identity_keys();
                        if accepted_identities
                            .iter()
                            .any(|identity| observation_keys.contains(identity))
                        {
                            accepted_identities.extend(observation_keys);
                            observations.push(observation);
                            return true;
                        }
                        if accepted_unique_candidates < policy.max_results {
                            accepted_unique_candidates += 1;
                            accepted_identities.extend(observation_keys);
                            observations.push(observation);
                            return true;
                        }
                        false
                    };
                    (
                        provider.collect(&context, &mut emit),
                        observations,
                        true,
                        false,
                    )
                };
            if cancelled.load(Ordering::Acquire) {
                break;
            }
            if let Err(error) = result {
                diagnostics.push(DiscoveryDiagnostic {
                    source_kind: error.source_kind,
                    code: error.code,
                });
                if stop_on_error || error.code == DiscoveryDiagnosticCode::ProviderTimeout {
                    break;
                }
                continue;
            }
            if Instant::now() >= deadline {
                diagnostics.push(DiscoveryDiagnostic {
                    source_kind: provider_source,
                    code: DiscoveryDiagnosticCode::ProviderTimeout,
                });
                break;
            }
            let publishable = if already_budgeted {
                observations
            } else {
                let mut accepted_identities = merged.acceptance_snapshot();
                let mut accepted_unique_candidates = merged.len();
                let mut publishable = Vec::new();
                for observation in observations {
                    if !accept_observation(
                        policy,
                        &context,
                        &mut accepted_identities,
                        &mut accepted_unique_candidates,
                        &mut publishable,
                        observation,
                    ) {
                        break;
                    }
                }
                publishable
            };
            for observation in publishable {
                merged.merge_observation(policy, observation);
            }
            if cancelled.load(Ordering::Acquire) || Instant::now() >= deadline {
                break;
            }
        }
        let (candidates, candidate_observations) = merged.into_candidates_and_observations();
        DiscoveryReport {
            candidates,
            diagnostics,
            candidate_observations,
        }
    }
}

impl DiscoveryProviderExecution {
    fn allows_inline(self) -> bool {
        match self {
            DiscoveryProviderExecution::ManagedWorkerRequired => false,
            #[cfg(test)]
            DiscoveryProviderExecution::InlineAllowedForTests => true,
        }
    }
}

fn accept_observation(
    policy: &DiscoveryPolicy,
    context: &DiscoveryContext<'_>,
    accepted_identities: &mut BTreeSet<String>,
    accepted_unique_candidates: &mut usize,
    observations: &mut Vec<Observation>,
    observation: Observation,
) -> bool {
    if context.cancelled.load(Ordering::Acquire) {
        return false;
    }
    if context.should_stop() || !context.try_take_observation() {
        return false;
    }
    let observation_keys = observation.identity_keys();
    if accepted_identities
        .iter()
        .any(|identity| observation_keys.contains(identity))
    {
        accepted_identities.extend(observation_keys);
        observations.push(observation);
        return true;
    }
    if *accepted_unique_candidates < policy.max_results {
        *accepted_unique_candidates += 1;
        accepted_identities.extend(observation_keys);
        observations.push(observation);
        return true;
    }
    false
}

fn collect_managed_process(
    source_kind: ObservationSourceKind,
    spec: ManagedProviderProcessSpec,
    context: &DiscoveryContext<'_>,
) -> Result<ManagedProviderOutcome, DiscoveryProviderError> {
    if context.cancelled.load(Ordering::Acquire) || Instant::now() >= context.deadline {
        return Err(DiscoveryProviderError {
            source_kind,
            code: DiscoveryDiagnosticCode::ProviderTimeout,
        });
    }
    let mut child = ManagedChild::spawn(&spec).map_err(|_| DiscoveryProviderError {
        source_kind,
        code: DiscoveryDiagnosticCode::ProviderFailed,
    })?;
    #[cfg(test)]
    if let Some(records) = &spec.started_processes {
        if spec.capture_descendants {
            thread::sleep(Duration::from_millis(100));
        }
        records.lock().unwrap().push(ManagedProviderProcessRecord {
            root_pid: child.id(),
            descendant_pids: if spec.capture_descendants {
                test_process_descendants(child.id())
            } else {
                Vec::new()
            },
        });
    }
    let stdin = child.take_stdin();
    let stdout = child.take_stdout();
    let stderr = child.take_stderr();
    let request = spec.request;
    let mut writer = Some(spawn_managed_io_thread(move || {
        let mut stdin = stdin;
        write_worker_frame(&mut stdin, &request, MAX_WORKER_REQUEST_BYTES)
    }));
    let stdout_reader =
        spawn_managed_io_thread(move || read_bounded_stream(stdout, MAX_WORKER_RESPONSE_BYTES));
    let stderr_reader =
        spawn_managed_io_thread(move || read_bounded_stream(stderr, MAX_WORKER_STDERR_BYTES));
    let mut writer_result = None;
    loop {
        if context.cancelled.load(Ordering::Acquire) || Instant::now() >= context.deadline {
            let cleanup_deadline = managed_cleanup_deadline(context);
            let _ = child.terminate(cleanup_deadline);
            let _ = join_optional_managed_io_thread(writer.take(), cleanup_deadline);
            let _ = join_managed_io_thread(stdout_reader, cleanup_deadline);
            let _ = join_managed_io_thread(stderr_reader, cleanup_deadline);
            return Err(DiscoveryProviderError {
                source_kind,
                code: DiscoveryDiagnosticCode::ProviderTimeout,
            });
        }
        if writer_result.is_none()
            && writer
                .as_ref()
                .is_some_and(std::thread::JoinHandle::is_finished)
        {
            writer_result = Some(
                join_optional_managed_io_thread(writer.take(), Instant::now()).unwrap_or(Err(())),
            );
            if writer_result == Some(Err(())) {
                let cleanup_deadline = managed_cleanup_deadline(context);
                let _ = child.terminate(cleanup_deadline);
                let _ = join_managed_io_thread(stdout_reader, cleanup_deadline);
                let _ = join_managed_io_thread(stderr_reader, cleanup_deadline);
                return Err(DiscoveryProviderError {
                    source_kind,
                    code: DiscoveryDiagnosticCode::ProviderFailed,
                });
            }
        }
        match child.try_wait() {
            Ok(Some(status)) => {
                child.close_owned_job();
                let cleanup_deadline = managed_cleanup_deadline(context);
                let writer = match writer_result {
                    Some(result) => result,
                    None => join_optional_managed_io_thread(writer.take(), cleanup_deadline)
                        .unwrap_or(Err(())),
                };
                let stdout =
                    join_managed_io_thread(stdout_reader, cleanup_deadline).unwrap_or(Err(()));
                let stderr =
                    join_managed_io_thread(stderr_reader, cleanup_deadline).unwrap_or(Err(()));
                return if status.success()
                    && writer.is_ok()
                    && stderr
                        .as_ref()
                        .is_ok_and(|bytes| bytes.iter().all(u8::is_ascii_whitespace))
                {
                    let outcome = stdout
                        .ok()
                        .and_then(|bytes| read_worker_response_frame(&bytes).ok());
                    outcome.ok_or(DiscoveryProviderError {
                        source_kind,
                        code: DiscoveryDiagnosticCode::ProviderFailed,
                    })
                } else {
                    Err(DiscoveryProviderError {
                        source_kind,
                        code: DiscoveryDiagnosticCode::ProviderFailed,
                    })
                };
            }
            Ok(None) => thread::sleep(Duration::from_millis(5)),
            Err(_) => {
                let cleanup_deadline = managed_cleanup_deadline(context);
                let _ = child.terminate(cleanup_deadline);
                let _ = join_optional_managed_io_thread(writer.take(), cleanup_deadline);
                let _ = join_managed_io_thread(stdout_reader, cleanup_deadline);
                let _ = join_managed_io_thread(stderr_reader, cleanup_deadline);
                return Err(DiscoveryProviderError {
                    source_kind,
                    code: DiscoveryDiagnosticCode::ProviderFailed,
                });
            }
        }
    }
}

fn managed_cleanup_deadline(context: &DiscoveryContext<'_>) -> Instant {
    let now = Instant::now();
    if context.deadline > now {
        context.deadline + MANAGED_CLEANUP_GRACE
    } else {
        now + MANAGED_CLEANUP_GRACE
    }
}

fn spawn_managed_io_thread<T: Send + 'static>(
    work: impl FnOnce() -> T + Send + 'static,
) -> thread::JoinHandle<T> {
    thread::spawn(move || {
        let _guard = ManagedIoThreadGuard::new();
        work()
    })
}

fn join_optional_managed_io_thread<T>(
    handle: Option<thread::JoinHandle<T>>,
    deadline: Instant,
) -> Result<T, ()> {
    match handle {
        Some(handle) => join_managed_io_thread(handle, deadline),
        None => Err(()),
    }
}

fn join_managed_io_thread<T>(handle: thread::JoinHandle<T>, deadline: Instant) -> Result<T, ()> {
    while !handle.is_finished() {
        if Instant::now() >= deadline {
            return Err(());
        }
        thread::sleep(Duration::from_millis(2));
    }
    handle.join().map_err(|_| ())
}

fn remaining_deadline_ms(deadline: Instant) -> u32 {
    deadline
        .saturating_duration_since(Instant::now())
        .as_millis()
        .min(u128::from(u32::MAX)) as u32
}

struct ManagedIoThreadGuard;

impl ManagedIoThreadGuard {
    fn new() -> Self {
        #[cfg(test)]
        {
            ACTIVE_MANAGED_IO_THREADS.fetch_add(1, Ordering::AcqRel);
        }
        Self
    }
}

impl Drop for ManagedIoThreadGuard {
    fn drop(&mut self) {
        #[cfg(test)]
        {
            ACTIVE_MANAGED_IO_THREADS.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

#[cfg(test)]
static ACTIVE_MANAGED_IO_THREADS: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
fn active_managed_io_threads_for_tests() -> usize {
    ACTIVE_MANAGED_IO_THREADS.load(Ordering::Acquire)
}

#[cfg(test)]
pub(crate) fn managed_process_fixture_guard_for_tests() -> MutexGuard<'static, ()> {
    static LOCK: std::sync::OnceLock<Mutex<()>> = std::sync::OnceLock::new();
    match LOCK.get_or_init(|| Mutex::new(())).lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

struct ManagedExitStatus {
    success: bool,
}

impl ManagedExitStatus {
    fn success(&self) -> bool {
        self.success
    }
}

#[cfg(not(windows))]
struct ManagedChild {
    child: std::process::Child,
    job: Option<OwnedJob>,
}

#[cfg(not(windows))]
impl ManagedChild {
    fn spawn(spec: &ManagedProviderProcessSpec) -> Result<Self, ()> {
        Self::spawn_parts(&spec.executable, &spec.args, None, &[], &[])
    }

    fn spawn_direct(spec: &ManagedDirectStdioSpec) -> Result<Self, ()> {
        Self::spawn_parts(
            &spec.executable,
            &spec.args,
            Some(&spec.current_dir),
            &spec.environment_allowlist,
            &spec.credential_environment,
        )
    }

    fn spawn_parts(
        executable: &Path,
        args: &[String],
        current_dir: Option<&Path>,
        environment_allowlist: &[String],
        credential_environment: &[String],
    ) -> Result<Self, ()> {
        let job = OwnedJob::new_kill_on_close()?;
        let mut command = std::process::Command::new(executable);
        command
            .args(args)
            .env_clear()
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        for variable in environment_allowlist
            .iter()
            .chain(credential_environment.iter())
        {
            if let Some(value) = std::env::var_os(variable) {
                command.env(variable, value);
            }
        }
        if let Some(current_dir) = current_dir {
            command.current_dir(current_dir);
        }
        let child = command.spawn().map_err(|_| ())?;
        job.assign(&child)?;
        Ok(Self {
            child,
            job: Some(job),
        })
    }

    fn id(&self) -> u32 {
        self.child.id()
    }

    fn take_stdin(&mut self) -> std::process::ChildStdin {
        self.child.stdin.take().expect("managed child stdin")
    }

    fn take_stdout(&mut self) -> std::process::ChildStdout {
        self.child.stdout.take().expect("managed child stdout")
    }

    fn take_stderr(&mut self) -> std::process::ChildStderr {
        self.child.stderr.take().expect("managed child stderr")
    }

    fn try_wait(&mut self) -> Result<Option<ManagedExitStatus>, ()> {
        self.child
            .try_wait()
            .map(|status| {
                status.map(|status| ManagedExitStatus {
                    success: status.success(),
                })
            })
            .map_err(|_| ())
    }

    fn terminate(&mut self, deadline: Instant) -> bool {
        let _ = self.child.kill();
        self.close_owned_job();
        loop {
            match self.child.try_wait() {
                Ok(Some(_)) => return true,
                Ok(None) => {
                    if Instant::now() >= deadline {
                        return false;
                    }
                    thread::sleep(Duration::from_millis(5));
                }
                Err(_) => return false,
            }
        }
    }

    fn close_owned_job(&mut self) {
        drop(self.job.take());
    }
}

#[cfg(windows)]
struct ManagedChild {
    process: windows_sys::Win32::Foundation::HANDLE,
    #[cfg(test)]
    pid: u32,
    stdin: Option<std::fs::File>,
    stdout: Option<std::fs::File>,
    stderr: Option<std::fs::File>,
    job: Option<OwnedJob>,
}

#[cfg(windows)]
impl ManagedChild {
    fn spawn(spec: &ManagedProviderProcessSpec) -> Result<Self, ()> {
        Self::spawn_parts(
            &spec.executable,
            &spec.args,
            None,
            spec.force_attribute_list_failure(),
            &[],
            &[],
        )
    }

    fn spawn_direct(spec: &ManagedDirectStdioSpec) -> Result<Self, ()> {
        Self::spawn_parts(
            &spec.executable,
            &spec.args,
            Some(&spec.current_dir),
            false,
            &spec.environment_allowlist,
            &spec.credential_environment,
        )
    }

    fn spawn_parts(
        executable: &Path,
        args: &[String],
        current_dir: Option<&Path>,
        force_attribute_list_failure: bool,
        environment_allowlist: &[String],
        credential_environment: &[String],
    ) -> Result<Self, ()> {
        let _ = force_attribute_list_failure;
        use std::mem::zeroed;
        use std::os::windows::io::FromRawHandle;
        use windows_sys::Win32::Foundation::{
            CloseHandle, SetHandleInformation, HANDLE, HANDLE_FLAG_INHERIT,
        };
        use windows_sys::Win32::Security::SECURITY_ATTRIBUTES;
        use windows_sys::Win32::System::Pipes::CreatePipe;
        use windows_sys::Win32::System::Threading::{
            CreateProcessW, ResumeThread, CREATE_NO_WINDOW, CREATE_SUSPENDED,
            CREATE_UNICODE_ENVIRONMENT, EXTENDED_STARTUPINFO_PRESENT, PROCESS_INFORMATION,
            STARTF_USESTDHANDLES, STARTUPINFOEXW,
        };

        unsafe fn close_if_valid(handle: HANDLE) {
            if !handle.is_null() {
                unsafe {
                    CloseHandle(handle);
                }
            }
        }

        fn pipe_pair() -> Result<(HANDLE, HANDLE), ()> {
            use windows_sys::Win32::Foundation::HANDLE;
            let mut read: HANDLE = std::ptr::null_mut();
            let mut write: HANDLE = std::ptr::null_mut();
            let security = SECURITY_ATTRIBUTES {
                nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
                lpSecurityDescriptor: std::ptr::null_mut(),
                bInheritHandle: 1,
            };
            let ok = unsafe { CreatePipe(&mut read, &mut write, &security, 0) };
            if ok == 0 {
                return Err(());
            }
            Ok((read, write))
        }

        fn make_not_inheritable(handle: HANDLE) -> Result<(), ()> {
            let ok = unsafe { SetHandleInformation(handle, HANDLE_FLAG_INHERIT, 0) };
            if ok == 0 {
                Err(())
            } else {
                Ok(())
            }
        }

        let job = OwnedJob::new_kill_on_close()?;
        let (stdin_read, stdin_write) = pipe_pair()?;
        let (stdout_read, stdout_write) = pipe_pair()?;
        let (stderr_read, stderr_write) = pipe_pair()?;
        if make_not_inheritable(stdin_write)
            .and_then(|_| make_not_inheritable(stdout_read))
            .and_then(|_| make_not_inheritable(stderr_read))
            .is_err()
        {
            unsafe {
                close_if_valid(stdin_read);
                close_if_valid(stdin_write);
                close_if_valid(stdout_read);
                close_if_valid(stdout_write);
                close_if_valid(stderr_read);
                close_if_valid(stderr_write);
            }
            return Err(());
        }
        #[cfg(test)]
        if force_attribute_list_failure {
            unsafe {
                close_if_valid(stdin_read);
                close_if_valid(stdin_write);
                close_if_valid(stdout_read);
                close_if_valid(stdout_write);
                close_if_valid(stderr_read);
                close_if_valid(stderr_write);
            }
            return Err(());
        }

        let mut inherited_handles = [stdin_read, stdout_write, stderr_write];
        let mut attributes = match ProcThreadAttributeList::new(&mut inherited_handles) {
            Ok(attributes) => attributes,
            Err(()) => {
                unsafe {
                    close_if_valid(stdin_read);
                    close_if_valid(stdin_write);
                    close_if_valid(stdout_read);
                    close_if_valid(stdout_write);
                    close_if_valid(stderr_read);
                    close_if_valid(stderr_write);
                }
                return Err(());
            }
        };

        let mut startup: STARTUPINFOEXW = unsafe { zeroed() };
        startup.StartupInfo.cb = std::mem::size_of::<STARTUPINFOEXW>() as u32;
        startup.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
        startup.StartupInfo.hStdInput = stdin_read;
        startup.StartupInfo.hStdOutput = stdout_write;
        startup.StartupInfo.hStdError = stderr_write;
        startup.lpAttributeList = attributes.as_mut_ptr();
        let mut process_info: PROCESS_INFORMATION = unsafe { zeroed() };
        let mut application = wide_null(executable.as_os_str());
        let command_line_text = windows_command_line(executable, args);
        let mut command_line = wide_null(std::ffi::OsStr::new(&command_line_text));
        let mut environment =
            minimal_windows_environment_block(environment_allowlist, credential_environment);
        let mut current_directory = current_dir.map(|path| wide_null(path.as_os_str()));
        let env_ptr = if environment.is_empty() {
            std::ptr::null_mut()
        } else {
            environment.as_mut_ptr().cast()
        };
        let current_directory_ptr = current_directory
            .as_mut()
            .map_or(std::ptr::null_mut(), |path| path.as_mut_ptr());
        let creation_flags = CREATE_SUSPENDED
            | CREATE_NO_WINDOW
            | CREATE_UNICODE_ENVIRONMENT
            | EXTENDED_STARTUPINFO_PRESENT;
        let created = unsafe {
            CreateProcessW(
                application.as_mut_ptr(),
                command_line.as_mut_ptr(),
                std::ptr::null(),
                std::ptr::null(),
                1,
                creation_flags,
                env_ptr,
                current_directory_ptr,
                &startup.StartupInfo,
                &mut process_info,
            )
        };
        unsafe {
            close_if_valid(stdin_read);
            close_if_valid(stdout_write);
            close_if_valid(stderr_write);
        }
        if created == 0 {
            unsafe {
                close_if_valid(stdin_write);
                close_if_valid(stdout_read);
                close_if_valid(stderr_read);
            }
            return Err(());
        }
        if job.assign_process_handle(process_info.hProcess).is_err() {
            unsafe {
                windows_sys::Win32::System::Threading::TerminateProcess(process_info.hProcess, 1);
                close_if_valid(process_info.hThread);
                close_if_valid(process_info.hProcess);
                close_if_valid(stdin_write);
                close_if_valid(stdout_read);
                close_if_valid(stderr_read);
            }
            return Err(());
        }
        let resumed = unsafe { ResumeThread(process_info.hThread) };
        unsafe {
            close_if_valid(process_info.hThread);
        }
        if resumed == u32::MAX {
            unsafe {
                windows_sys::Win32::System::Threading::TerminateProcess(process_info.hProcess, 1);
                close_if_valid(process_info.hProcess);
                close_if_valid(stdin_write);
                close_if_valid(stdout_read);
                close_if_valid(stderr_read);
            }
            return Err(());
        }
        Ok(Self {
            process: process_info.hProcess,
            #[cfg(test)]
            pid: process_info.dwProcessId,
            stdin: Some(unsafe { std::fs::File::from_raw_handle(stdin_write.cast()) }),
            stdout: Some(unsafe { std::fs::File::from_raw_handle(stdout_read.cast()) }),
            stderr: Some(unsafe { std::fs::File::from_raw_handle(stderr_read.cast()) }),
            job: Some(job),
        })
    }

    #[cfg(test)]
    fn id(&self) -> u32 {
        self.pid
    }

    fn take_stdin(&mut self) -> std::fs::File {
        self.stdin.take().expect("managed child stdin")
    }

    fn take_stdout(&mut self) -> std::fs::File {
        self.stdout.take().expect("managed child stdout")
    }

    fn take_stderr(&mut self) -> std::fs::File {
        self.stderr.take().expect("managed child stderr")
    }

    fn try_wait(&mut self) -> Result<Option<ManagedExitStatus>, ()> {
        use windows_sys::Win32::Foundation::WAIT_TIMEOUT;
        use windows_sys::Win32::System::Threading::{GetExitCodeProcess, WaitForSingleObject};
        let wait = unsafe { WaitForSingleObject(self.process, 0) };
        if wait == WAIT_TIMEOUT {
            return Ok(None);
        }
        let mut code = 1u32;
        let ok = unsafe { GetExitCodeProcess(self.process, &mut code) };
        if ok == 0 {
            return Err(());
        }
        Ok(Some(ManagedExitStatus { success: code == 0 }))
    }

    fn terminate(&mut self, deadline: Instant) -> bool {
        unsafe {
            windows_sys::Win32::System::Threading::TerminateProcess(self.process, 1);
        }
        self.close_owned_job();
        use windows_sys::Win32::Foundation::WAIT_OBJECT_0;
        let wait = unsafe {
            windows_sys::Win32::System::Threading::WaitForSingleObject(
                self.process,
                remaining_deadline_ms(deadline),
            )
        };
        wait == WAIT_OBJECT_0
    }

    fn close_owned_job(&mut self) {
        drop(self.job.take());
    }
}

#[cfg(windows)]
impl Drop for ManagedChild {
    fn drop(&mut self) {
        self.close_owned_job();
        unsafe {
            windows_sys::Win32::Foundation::CloseHandle(self.process);
        }
    }
}

#[cfg(windows)]
struct ProcThreadAttributeList {
    storage: Vec<u8>,
}

#[cfg(windows)]
impl ProcThreadAttributeList {
    fn new(inherited_handles: &mut [windows_sys::Win32::Foundation::HANDLE]) -> Result<Self, ()> {
        use windows_sys::Win32::System::Threading::{
            DeleteProcThreadAttributeList, InitializeProcThreadAttributeList,
            UpdateProcThreadAttribute, PROC_THREAD_ATTRIBUTE_HANDLE_LIST,
        };

        let mut bytes = 0usize;
        unsafe {
            InitializeProcThreadAttributeList(std::ptr::null_mut(), 1, 0, &mut bytes);
        }
        if bytes == 0 {
            return Err(());
        }
        let mut storage = vec![0u8; bytes];
        let list = storage.as_mut_ptr().cast();
        let initialized = unsafe { InitializeProcThreadAttributeList(list, 1, 0, &mut bytes) };
        if initialized == 0 {
            return Err(());
        }
        let updated = unsafe {
            UpdateProcThreadAttribute(
                list,
                0,
                PROC_THREAD_ATTRIBUTE_HANDLE_LIST as usize,
                inherited_handles.as_mut_ptr().cast(),
                std::mem::size_of_val(inherited_handles),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };
        if updated == 0 {
            unsafe {
                DeleteProcThreadAttributeList(list);
            }
            return Err(());
        }
        Ok(Self { storage })
    }

    fn as_mut_ptr(
        &mut self,
    ) -> windows_sys::Win32::System::Threading::LPPROC_THREAD_ATTRIBUTE_LIST {
        self.storage.as_mut_ptr().cast()
    }
}

#[cfg(windows)]
impl Drop for ProcThreadAttributeList {
    fn drop(&mut self) {
        unsafe {
            windows_sys::Win32::System::Threading::DeleteProcThreadAttributeList(
                self.storage.as_mut_ptr().cast(),
            );
        }
    }
}

#[cfg(windows)]
fn wide_null(value: &std::ffi::OsStr) -> Vec<u16> {
    use std::os::windows::ffi::OsStrExt;
    value.encode_wide().chain(std::iter::once(0)).collect()
}

#[cfg(windows)]
fn windows_command_line(executable: &Path, args: &[String]) -> String {
    std::iter::once(quote_windows_arg(&executable.display().to_string()))
        .chain(args.iter().map(|arg| quote_windows_arg(arg)))
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(windows)]
fn quote_windows_arg(value: &str) -> String {
    if value.is_empty() {
        return "\"\"".into();
    }
    let needs_quotes = value
        .chars()
        .any(|ch| ch.is_whitespace() || matches!(ch, '"' | '\\'));
    if !needs_quotes {
        return value.into();
    }
    let mut quoted = String::from("\"");
    let mut backslashes = 0usize;
    for ch in value.chars() {
        match ch {
            '\\' => backslashes += 1,
            '"' => {
                quoted.extend(std::iter::repeat_n('\\', backslashes * 2 + 1));
                quoted.push('"');
                backslashes = 0;
            }
            _ => {
                quoted.extend(std::iter::repeat_n('\\', backslashes));
                backslashes = 0;
                quoted.push(ch);
            }
        }
    }
    quoted.extend(std::iter::repeat_n('\\', backslashes * 2));
    quoted.push('"');
    quoted
}

#[cfg(windows)]
fn minimal_windows_environment_block(
    environment_allowlist: &[String],
    credential_environment: &[String],
) -> Vec<u16> {
    minimal_windows_environment_block_with_lookup(
        environment_allowlist,
        credential_environment,
        |name| std::env::var_os(name),
    )
}

#[cfg(windows)]
fn minimal_windows_environment_block_with_lookup(
    environment_allowlist: &[String],
    credential_environment: &[String],
    lookup: impl Fn(&str) -> Option<std::ffi::OsString>,
) -> Vec<u16> {
    let mut names = ["SystemRoot", "WINDIR"]
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    names.extend(environment_allowlist.iter().cloned());
    names.extend(credential_environment.iter().cloned());
    names.sort();
    names.dedup();
    let mut entries = names
        .into_iter()
        .filter_map(|name| lookup(&name).map(|value| (name, value)))
        .map(|(name, value)| format!("{}={}", name, value.to_string_lossy()))
        .collect::<Vec<_>>();
    entries.sort();
    entries.dedup();
    let mut block = Vec::new();
    for entry in entries {
        block.extend(wide_null(std::ffi::OsStr::new(&entry)));
    }
    // Each entry already carries its own NUL; the block must end with an
    // empty entry (a second NUL) to terminate. When no entry exists at all
    // (SystemRoot/WINDIR both absent and an empty allowlist) the block must
    // still be two NULs, never a single one, so CreateProcessW with
    // CREATE_UNICODE_ENVIRONMENT treats it as an empty environment rather
    // than a malformed block.
    block.push(0);
    if block.len() == 1 {
        block.push(0);
    }
    block
}

fn read_bounded_stream<R: Read>(mut reader: R, limit: usize) -> Result<Vec<u8>, ()> {
    let mut output = Vec::new();
    let mut buffer = [0u8; 8192];
    loop {
        let read = reader.read(&mut buffer).map_err(|_| ())?;
        if read == 0 {
            return Ok(output);
        }
        if output.len().saturating_add(read) > limit {
            return Err(());
        }
        output.extend_from_slice(&buffer[..read]);
    }
}

pub(crate) fn write_worker_frame<W: Write, T: Serialize>(
    writer: &mut W,
    payload: &T,
    max_bytes: usize,
) -> Result<(), ()> {
    let encoded = serde_json::to_vec(payload).map_err(|_| ())?;
    if encoded.len() > max_bytes {
        return Err(());
    }
    writer
        .write_all(format!("{WORKER_FRAME_MAGIC} {}\n", encoded.len()).as_bytes())
        .map_err(|_| ())?;
    writer.write_all(&encoded).map_err(|_| ())?;
    writer.flush().map_err(|_| ())
}

pub(crate) fn read_worker_request_frame<R: Read>(
    reader: R,
) -> Result<ManagedProviderWorkerRequest, DiscoveryDiagnosticCode> {
    let bytes = read_worker_frame_bytes(reader, MAX_WORKER_REQUEST_BYTES)?;
    let request: ManagedProviderWorkerRequest =
        serde_json::from_slice(&bytes).map_err(|_| DiscoveryDiagnosticCode::ProviderFailed)?;
    if !worker_identity_matches(
        request.version,
        &request.protocol_identity,
        &request.build_identity,
    ) {
        return Err(DiscoveryDiagnosticCode::ProviderFailed);
    }
    Ok(request)
}

fn read_worker_response_frame(
    bytes: &[u8],
) -> Result<ManagedProviderOutcome, DiscoveryDiagnosticCode> {
    let payload = read_worker_frame_bytes(Cursor::new(bytes), MAX_WORKER_RESPONSE_BYTES)?;
    let outcome: ManagedProviderOutcome =
        serde_json::from_slice(&payload).map_err(|_| DiscoveryDiagnosticCode::ProviderFailed)?;
    if !worker_identity_matches(
        outcome.version,
        &outcome.protocol_identity,
        &outcome.build_identity,
    ) {
        return Err(DiscoveryDiagnosticCode::ProviderFailed);
    }
    Ok(outcome)
}

fn worker_identity_matches(version: u16, protocol_identity: &str, build_identity: &str) -> bool {
    version == WORKER_PROTOCOL_VERSION
        && protocol_identity == WORKER_PROTOCOL_ID
        && build_identity == WORKER_BUILD_ID
}

fn read_worker_frame_bytes<R: Read>(
    reader: R,
    max_bytes: usize,
) -> Result<Vec<u8>, DiscoveryDiagnosticCode> {
    let mut reader = BufReader::new(reader);
    let mut header = String::new();
    let read = reader
        .read_line(&mut header)
        .map_err(|_| DiscoveryDiagnosticCode::ProviderFailed)?;
    if read == 0 || header.len() > 128 {
        return Err(DiscoveryDiagnosticCode::ProviderFailed);
    }
    let header = header.trim_end_matches(['\r', '\n']);
    let Some(length_text) = header
        .strip_prefix(WORKER_FRAME_MAGIC)
        .and_then(|tail| tail.strip_prefix(' '))
    else {
        return Err(DiscoveryDiagnosticCode::ProviderFailed);
    };
    let length = length_text
        .parse::<usize>()
        .map_err(|_| DiscoveryDiagnosticCode::ProviderFailed)?;
    if length > max_bytes {
        return Err(DiscoveryDiagnosticCode::OversizedInput);
    }
    let mut payload = vec![0u8; length];
    reader
        .read_exact(&mut payload)
        .map_err(|_| DiscoveryDiagnosticCode::ShortRead)?;
    let mut trailing = Vec::new();
    reader
        .read_to_end(&mut trailing)
        .map_err(|_| DiscoveryDiagnosticCode::ProviderFailed)?;
    if trailing.iter().any(|byte| !byte.is_ascii_whitespace()) {
        return Err(DiscoveryDiagnosticCode::ProviderFailed);
    }
    Ok(payload)
}

#[cfg(not(windows))]
fn configure_managed_command(
    _command: &mut std::process::Command,
    _spec: &ManagedProviderProcessSpec,
) {
}

#[cfg(windows)]
struct OwnedJob {
    handle: windows_sys::Win32::Foundation::HANDLE,
}

#[cfg(windows)]
impl OwnedJob {
    fn new_kill_on_close() -> Result<Self, ()> {
        use std::mem::zeroed;
        use windows_sys::Win32::Foundation::CloseHandle;
        use windows_sys::Win32::System::JobObjects::{
            CreateJobObjectW, JobObjectExtendedLimitInformation, SetInformationJobObject,
            JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
        };
        let handle = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
        if handle.is_null() {
            return Err(());
        }
        let mut limits: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { zeroed() };
        limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        let ok = unsafe {
            SetInformationJobObject(
                handle,
                JobObjectExtendedLimitInformation,
                &limits as *const _ as *const std::ffi::c_void,
                std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            )
        };
        if ok == 0 {
            unsafe {
                CloseHandle(handle);
            }
            return Err(());
        }
        Ok(Self { handle })
    }

    fn assign_process_handle(
        &self,
        process: windows_sys::Win32::Foundation::HANDLE,
    ) -> Result<(), ()> {
        use windows_sys::Win32::System::JobObjects::AssignProcessToJobObject;
        let ok = unsafe { AssignProcessToJobObject(self.handle, process) };
        if ok == 0 {
            Err(())
        } else {
            Ok(())
        }
    }
}

#[cfg(windows)]
impl Drop for OwnedJob {
    fn drop(&mut self) {
        unsafe {
            windows_sys::Win32::Foundation::CloseHandle(self.handle);
        }
    }
}

#[cfg(not(windows))]
struct OwnedJob;

#[cfg(not(windows))]
impl OwnedJob {
    fn new_kill_on_close() -> Result<Self, ()> {
        Ok(Self)
    }

    fn assign(&self, _child: &std::process::Child) -> Result<(), ()> {
        Ok(())
    }
}

#[cfg(test)]
fn test_process_descendants(_pid: u32) -> Vec<u32> {
    #[cfg(windows)]
    {
        let script = format!(
            "Get-CimInstance Win32_Process -Filter \"ParentProcessId = {_pid}\" | ForEach-Object {{ $_.ProcessId }}"
        );
        let output = std::process::Command::new("powershell.exe")
            .args(["-NoProfile", "-Command", &script])
            .output()
            .expect("query test process descendants");
        assert!(
            output.status.success(),
            "test process descendant inventory failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .filter_map(|line| line.trim().parse::<u32>().ok())
            .collect()
    }
    #[cfg(not(windows))]
    {
        Vec::new()
    }
}

fn observation_cap(max_results: usize) -> usize {
    max_results
        .saturating_mul(OBSERVATION_CAP_MULTIPLIER)
        .max(OBSERVATION_CAP_MINIMUM)
}

#[derive(Clone)]
pub(crate) struct DiscoveryBudget {
    remaining: Arc<AtomicUsize>,
}

impl DiscoveryBudget {
    fn new(max_results: usize) -> Self {
        Self {
            remaining: Arc::new(AtomicUsize::new(max_results)),
        }
    }

    fn remaining(&self) -> usize {
        self.remaining.load(Ordering::Acquire)
    }

    fn try_take(&self) -> bool {
        let mut current = self.remaining.load(Ordering::Acquire);
        while current > 0 {
            match self.remaining.compare_exchange(
                current,
                current - 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(next) => current = next,
            }
        }
        false
    }
}

#[derive(Clone)]
pub(crate) struct DiscoveryContext<'a> {
    pub(crate) _policy: DiscoveryPolicy,
    pub(crate) deadline: Instant,
    pub(crate) cancelled: &'a AtomicBool,
    observation_budget: DiscoveryBudget,
}

impl DiscoveryContext<'_> {
    pub(crate) fn should_stop(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
            || Instant::now() >= self.deadline
            || self.observation_budget.remaining() == 0
    }

    pub(crate) fn remaining_results(&self) -> usize {
        self.observation_budget.remaining()
    }

    pub(crate) fn try_take_observation(&self) -> bool {
        self.observation_budget.try_take()
    }
}

pub(crate) trait DiscoveryProvider: Send + Sync {
    fn source_kind(&self) -> ObservationSourceKind;

    fn execution(&self) -> DiscoveryProviderExecution {
        DiscoveryProviderExecution::ManagedWorkerRequired
    }

    fn managed_process(
        &self,
        _context: &DiscoveryContext<'_>,
    ) -> Option<ManagedProviderProcessSpec> {
        None
    }

    fn collect(
        &self,
        context: &DiscoveryContext<'_>,
        emit: &mut dyn FnMut(Observation) -> bool,
    ) -> Result<(), DiscoveryProviderError>;
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ManagedProviderWorkerKind {
    Codex,
    Kun,
    WindowsPath,
    WindowsAppPaths,
    WindowsPackages,
    WindowsLoopbackListeners,
    ExplicitSources,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ManagedProviderWorkerRequest {
    pub(crate) version: u16,
    pub(crate) protocol_identity: String,
    pub(crate) build_identity: String,
    pub(crate) kind: ManagedProviderWorkerKind,
    pub(crate) timeout_ms: u64,
    pub(crate) max_results: usize,
    pub(crate) allow_active_verification: bool,
    pub(crate) payload: Value,
}

impl ManagedProviderWorkerRequest {
    pub(crate) fn new(
        kind: ManagedProviderWorkerKind,
        timeout_ms: u64,
        max_results: usize,
        allow_active_verification: bool,
        payload: Value,
    ) -> Self {
        Self {
            version: WORKER_PROTOCOL_VERSION,
            protocol_identity: WORKER_PROTOCOL_ID.to_owned(),
            build_identity: WORKER_BUILD_ID.to_owned(),
            kind,
            timeout_ms,
            max_results,
            allow_active_verification,
            payload,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ManagedProviderOutcome {
    pub(crate) version: u16,
    pub(crate) protocol_identity: String,
    pub(crate) build_identity: String,
    pub(crate) observations: Vec<Observation>,
    pub(crate) diagnostics: Vec<DiscoveryDiagnostic>,
}

impl ManagedProviderOutcome {
    pub(crate) fn success(observations: Vec<Observation>) -> Self {
        Self {
            version: WORKER_PROTOCOL_VERSION,
            protocol_identity: WORKER_PROTOCOL_ID.to_owned(),
            build_identity: WORKER_BUILD_ID.to_owned(),
            observations,
            diagnostics: Vec::new(),
        }
    }
}

pub(crate) struct ManagedProviderProcessSpec {
    pub(crate) executable: PathBuf,
    pub(crate) args: Vec<String>,
    pub(crate) request: ManagedProviderWorkerRequest,
    #[cfg(test)]
    pub(crate) started_processes: Option<Arc<std::sync::Mutex<Vec<ManagedProviderProcessRecord>>>>,
    #[cfg(test)]
    pub(crate) capture_descendants: bool,
    #[cfg(test)]
    pub(crate) force_attribute_list_failure: bool,
}

impl ManagedProviderProcessSpec {
    #[cfg(test)]
    fn force_attribute_list_failure(&self) -> bool {
        self.force_attribute_list_failure
    }

    #[cfg(not(test))]
    fn force_attribute_list_failure(&self) -> bool {
        false
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ManagedDirectStdioSpec {
    pub(crate) executable: PathBuf,
    pub(crate) args: Vec<String>,
    pub(crate) current_dir: PathBuf,
    pub(crate) environment_allowlist: Vec<String>,
    pub(crate) credential_environment: Vec<String>,
}

#[cfg(test)]
#[derive(Clone, Debug)]
pub(crate) struct ManagedProviderProcessRecord {
    pub(crate) root_pid: u32,
    pub(crate) descendant_pids: Vec<u32>,
}

#[derive(Default)]
struct CandidateMergeSet {
    candidates: BTreeMap<String, CandidateAggregate>,
    identity_index: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CandidateAggregate {
    candidate: CandidateProjection,
    identities: BTreeSet<String>,
    package_primary_identities: BTreeSet<String>,
    observations: Vec<Observation>,
}

impl CandidateMergeSet {
    fn len(&self) -> usize {
        self.candidates.len()
    }

    fn acceptance_snapshot(&self) -> BTreeSet<String> {
        self.identity_index.keys().cloned().collect()
    }

    fn merge_observation(&mut self, policy: &DiscoveryPolicy, observation: Observation) {
        let observation_identities = observation.identity_keys();
        let mut target_ids = observation_identities
            .iter()
            .filter_map(|identity| self.identity_index.get(identity).cloned())
            .collect::<BTreeSet<_>>();
        let new_primary_id = select_candidate_id_from_observation(&observation);
        if self.candidates.contains_key(&new_primary_id) {
            target_ids.insert(new_primary_id.clone());
        }
        if target_ids.is_empty() && self.candidates.len() >= policy.max_results {
            return;
        }

        if target_ids.is_empty() {
            let mut aggregate = CandidateAggregate {
                candidate: observation.project_with_candidate_id(new_primary_id.clone()),
                identities: observation_identities,
                package_primary_identities: BTreeSet::new(),
                observations: vec![observation.clone()],
            };
            if let Some(package_identity) = observation.package_primary_identity_key() {
                aggregate
                    .package_primary_identities
                    .insert(package_identity);
            }
            self.insert_aggregate(new_primary_id, aggregate);
            return;
        }

        let mut aggregate = self.remove_target_aggregate(target_ids);
        aggregate.identities.extend(observation_identities);
        aggregate.observations.push(observation.clone());
        if let Some(package_identity) = observation.package_primary_identity_key() {
            aggregate
                .package_primary_identities
                .insert(package_identity);
        }
        let selected_id = aggregate.selected_candidate_id();
        let mut candidate = observation.project_with_candidate_id(selected_id.clone());
        apply_package_identity_conflict_if_needed(
            &aggregate.package_primary_identities,
            &mut candidate,
        );
        let mut merge_map = BTreeMap::new();
        let existing_id = aggregate.candidate.candidate_id.clone();
        candidate.candidate_id = existing_id.clone();
        merge_map.insert(existing_id, aggregate.candidate);
        merge_candidate_projection(&mut merge_map, candidate);
        let mut candidate = merge_map
            .into_values()
            .next()
            .expect("candidate merge map has one candidate");
        candidate.candidate_id = selected_id.clone();
        apply_package_identity_conflict_if_needed(
            &aggregate.package_primary_identities,
            &mut candidate,
        );
        aggregate.candidate = candidate;
        self.insert_aggregate(selected_id, aggregate);
    }

    fn remove_target_aggregate(&mut self, target_ids: BTreeSet<String>) -> CandidateAggregate {
        let mut targets = target_ids.into_iter();
        let first_id = targets
            .next()
            .expect("remove_target_aggregate requires at least one target");
        let mut aggregate = self
            .candidates
            .remove(&first_id)
            .expect("identity index points to existing candidate");
        for identity in &aggregate.identities {
            self.identity_index.remove(identity);
        }
        for target_id in targets {
            if let Some(other) = self.candidates.remove(&target_id) {
                for identity in &other.identities {
                    self.identity_index.remove(identity);
                }
                let existing_id = aggregate.candidate.candidate_id.clone();
                let mut other_candidate = other.candidate;
                other_candidate.candidate_id = existing_id.clone();
                let mut merge_map = BTreeMap::new();
                merge_map.insert(existing_id, aggregate.candidate);
                merge_candidate_projection(&mut merge_map, other_candidate);
                aggregate.candidate = merge_map
                    .into_values()
                    .next()
                    .expect("candidate merge map has one candidate");
                aggregate.identities.extend(other.identities);
                aggregate
                    .package_primary_identities
                    .extend(other.package_primary_identities);
                aggregate.observations.extend(other.observations);
            }
        }
        aggregate
    }

    fn insert_aggregate(&mut self, candidate_id: String, mut aggregate: CandidateAggregate) {
        aggregate.candidate.candidate_id = candidate_id.clone();
        for identity in &aggregate.identities {
            self.identity_index
                .insert(identity.clone(), candidate_id.clone());
        }
        self.candidates.insert(candidate_id, aggregate);
    }

    fn into_candidates_and_observations(
        self,
    ) -> (Vec<CandidateProjection>, BTreeMap<String, Vec<Observation>>) {
        let mut candidates = Vec::with_capacity(self.candidates.len());
        let mut candidate_observations = BTreeMap::new();
        for (candidate_id, aggregate) in self.candidates {
            candidates.push(aggregate.candidate);
            candidate_observations.insert(candidate_id, aggregate.observations);
        }
        (candidates, candidate_observations)
    }
}

impl CandidateAggregate {
    fn selected_candidate_id(&self) -> String {
        self.package_primary_identities
            .iter()
            .next()
            .cloned()
            .or_else(|| self.identities.iter().next().cloned())
            .map(|identity| ObservationFingerprint::from_stable_id(identity).candidate_id())
            .unwrap_or_else(|| {
                ObservationFingerprint::from_parts(&["empty-candidate".to_owned()]).candidate_id()
            })
    }
}

fn select_candidate_id_from_observation(observation: &Observation) -> String {
    observation
        .package_primary_identity_key()
        .map(|identity| ObservationFingerprint::from_stable_id(identity).candidate_id())
        .unwrap_or_else(|| observation.candidate_id())
}

fn apply_package_identity_conflict_if_needed(
    package_primary_identities: &BTreeSet<String>,
    candidate: &mut CandidateProjection,
) {
    if package_primary_identities.len() <= 1 {
        return;
    }
    candidate.availability = CandidateAvailability::Unavailable;
    candidate.compatibility_state = CompatibilityState::Incompatible;
    candidate.requires_configuration = true;
    push_diagnostic(
        &mut candidate.diagnostics,
        ObservationSourceKind::WindowsPackage,
        DiscoveryDiagnosticCode::InvalidIdentity,
    );
}

fn merge_candidate_projection(
    merged: &mut BTreeMap<String, CandidateProjection>,
    candidate: CandidateProjection,
) {
    use std::collections::btree_map::Entry;

    match merged.entry(candidate.candidate_id.clone()) {
        Entry::Vacant(entry) => {
            entry.insert(candidate);
        }
        Entry::Occupied(mut entry) => {
            let existing = entry.get_mut();
            let mut identity_conflict = false;
            if existing.connector_id != candidate.connector_id {
                existing.connector_id = CONFLICT_CONNECTOR_ID.to_owned();
                existing.availability = CandidateAvailability::Unavailable;
                existing.availability_authority = merge_authority(
                    existing.availability_authority,
                    candidate.availability_authority,
                );
                existing.compatibility_state = CompatibilityState::Incompatible;
                existing.compatibility_authority = merge_authority(
                    existing.compatibility_authority,
                    candidate.compatibility_authority,
                );
                existing.requires_configuration = true;
                identity_conflict = true;
                push_diagnostic(
                    &mut existing.diagnostics,
                    existing.source_kind,
                    DiscoveryDiagnosticCode::ConnectorConflict,
                );
            }
            if existing.runtime_type != candidate.runtime_type {
                existing.runtime_type = UNKNOWN_RUNTIME_TYPE.to_owned();
                existing.availability = CandidateAvailability::Unavailable;
                existing.availability_authority = merge_authority(
                    existing.availability_authority,
                    candidate.availability_authority,
                );
                existing.compatibility_state = CompatibilityState::Incompatible;
                existing.compatibility_authority = merge_authority(
                    existing.compatibility_authority,
                    candidate.compatibility_authority,
                );
                existing.requires_configuration = true;
                identity_conflict = true;
                push_diagnostic(
                    &mut existing.diagnostics,
                    existing.source_kind,
                    DiscoveryDiagnosticCode::RuntimeTypeConflict,
                );
            }
            existing.display_name =
                merge_display_name(&existing.display_name, candidate.display_name.clone());
            existing.source_kinds = merge_source_set(
                std::mem::take(&mut existing.source_kinds),
                candidate.source_kinds.clone(),
            );
            existing.source_kind = select_source_kind(&existing.source_kinds);
            if existing.category != candidate.category {
                existing.category = CandidateCategory::Unknown;
                existing.availability = CandidateAvailability::Unavailable;
                existing.requires_configuration = true;
                push_diagnostic(
                    &mut existing.diagnostics,
                    existing.source_kind,
                    DiscoveryDiagnosticCode::CategoryConflict,
                );
            }
            if existing.discovery_state != candidate.discovery_state {
                existing.discovery_state = DiscoveryState::Observed;
                existing.availability = CandidateAvailability::Unavailable;
                existing.requires_configuration = true;
                push_diagnostic(
                    &mut existing.diagnostics,
                    existing.source_kind,
                    DiscoveryDiagnosticCode::DiscoveryStateConflict,
                );
            }
            let authority = merge_authority(
                existing.verification_authority,
                candidate.verification_authority,
            );
            (existing.availability, existing.availability_authority) = merge_availability(
                existing.availability,
                existing.availability_authority,
                candidate.availability,
                candidate.availability_authority,
                &mut existing.diagnostics,
                existing.source_kind,
            );
            (
                existing.compatibility_state,
                existing.compatibility_authority,
            ) = merge_compatibility_state(
                existing.compatibility_state,
                existing.compatibility_authority,
                candidate.compatibility_state,
                candidate.compatibility_authority,
                &mut existing.diagnostics,
                existing.source_kind,
            );
            (existing.auth_state, existing.auth_authority) = merge_auth_state(
                existing.auth_state,
                existing.auth_authority,
                candidate.auth_state,
                candidate.auth_authority,
                &mut existing.diagnostics,
                existing.source_kind,
            );
            (existing.health_state, existing.health_authority) = merge_health_state(
                existing.health_state,
                existing.health_authority,
                candidate.health_state,
                candidate.health_authority,
                &mut existing.diagnostics,
                existing.source_kind,
            );
            existing.discovery_authority =
                merge_authority(existing.discovery_authority, candidate.discovery_authority);
            existing.verification_authority = authority;
            existing.trust_level = merge_trust(existing.trust_level, candidate.trust_level);
            merge_catalog(existing, &candidate);
            existing.evidence_summary = merge_evidence_set(
                std::mem::take(&mut existing.evidence_summary),
                candidate.evidence_summary,
            );
            existing.diagnostics = merge_diagnostic_set(
                std::mem::take(&mut existing.diagnostics),
                candidate.diagnostics,
            );
            if identity_conflict || has_fail_closed_diagnostic(existing) {
                existing.availability = CandidateAvailability::Unavailable;
                existing.compatibility_state = CompatibilityState::Incompatible;
                existing.auth_state = AuthState::Unknown;
                existing.health_state = HealthState::Unavailable;
            }
            existing.requires_configuration = derive_requires_configuration(existing);
        }
    }
}

fn merge_display_name(existing: &str, candidate: String) -> String {
    if existing.is_empty() {
        return candidate;
    }
    if candidate.is_empty() {
        return existing.to_owned();
    }
    existing.min(candidate.as_str()).to_owned()
}

fn merge_source_set(
    existing: Vec<ObservationSourceKind>,
    candidate: Vec<ObservationSourceKind>,
) -> Vec<ObservationSourceKind> {
    let mut set = existing.into_iter().collect::<BTreeSet<_>>();
    set.extend(candidate);
    set.into_iter().collect()
}

fn select_source_kind(sources: &[ObservationSourceKind]) -> ObservationSourceKind {
    if sources.contains(&ObservationSourceKind::RuntimeRecord) {
        ObservationSourceKind::RuntimeRecord
    } else if sources.contains(&ObservationSourceKind::UserSelected) {
        ObservationSourceKind::UserSelected
    } else if sources.contains(&ObservationSourceKind::WindowsPackage) {
        ObservationSourceKind::WindowsPackage
    } else if sources.contains(&ObservationSourceKind::WindowsAppPath) {
        ObservationSourceKind::WindowsAppPath
    } else if sources.contains(&ObservationSourceKind::LoopbackListener) {
        ObservationSourceKind::LoopbackListener
    } else if sources.contains(&ObservationSourceKind::WindowsPath) {
        ObservationSourceKind::WindowsPath
    } else {
        ObservationSourceKind::ExecutableInventory
    }
}

fn merge_evidence_set(
    existing: Vec<DiscoveryEvidence>,
    candidate: Vec<DiscoveryEvidence>,
) -> Vec<DiscoveryEvidence> {
    let mut set = existing.into_iter().collect::<BTreeSet<_>>();
    set.extend(candidate);
    set.into_iter().collect()
}

fn merge_diagnostic_set(
    existing: Vec<DiscoveryDiagnostic>,
    candidate: Vec<DiscoveryDiagnostic>,
) -> Vec<DiscoveryDiagnostic> {
    let mut set = existing.into_iter().collect::<BTreeSet<_>>();
    set.extend(candidate);
    set.into_iter().collect()
}

fn merge_authority(
    existing: VerificationAuthority,
    candidate: VerificationAuthority,
) -> VerificationAuthority {
    if authority_rank(candidate) > authority_rank(existing) {
        candidate
    } else {
        existing
    }
}

fn merge_trust(
    existing: ObservationTrustLevel,
    candidate: ObservationTrustLevel,
) -> ObservationTrustLevel {
    if existing == ObservationTrustLevel::Untrusted || candidate == ObservationTrustLevel::Untrusted
    {
        return ObservationTrustLevel::Untrusted;
    }
    if trust_rank(candidate) > trust_rank(existing) {
        candidate
    } else {
        existing
    }
}

fn merge_availability(
    existing: CandidateAvailability,
    existing_authority: VerificationAuthority,
    candidate: CandidateAvailability,
    candidate_authority: VerificationAuthority,
    diagnostics: &mut Vec<DiscoveryDiagnostic>,
    source_kind: ObservationSourceKind,
) -> (CandidateAvailability, VerificationAuthority) {
    merge_state(
        existing,
        existing_authority,
        candidate,
        candidate_authority,
        CandidateAvailability::Unavailable,
        diagnostics,
        source_kind,
    )
}

fn merge_compatibility_state(
    existing: CompatibilityState,
    existing_authority: VerificationAuthority,
    candidate: CompatibilityState,
    candidate_authority: VerificationAuthority,
    diagnostics: &mut Vec<DiscoveryDiagnostic>,
    source_kind: ObservationSourceKind,
) -> (CompatibilityState, VerificationAuthority) {
    merge_state(
        existing,
        existing_authority,
        candidate,
        candidate_authority,
        CompatibilityState::NotVerified,
        diagnostics,
        source_kind,
    )
}

fn merge_auth_state(
    existing: AuthState,
    existing_authority: VerificationAuthority,
    candidate: AuthState,
    candidate_authority: VerificationAuthority,
    diagnostics: &mut Vec<DiscoveryDiagnostic>,
    source_kind: ObservationSourceKind,
) -> (AuthState, VerificationAuthority) {
    merge_state(
        existing,
        existing_authority,
        candidate,
        candidate_authority,
        AuthState::Unknown,
        diagnostics,
        source_kind,
    )
}

fn merge_health_state(
    existing: HealthState,
    existing_authority: VerificationAuthority,
    candidate: HealthState,
    candidate_authority: VerificationAuthority,
    diagnostics: &mut Vec<DiscoveryDiagnostic>,
    source_kind: ObservationSourceKind,
) -> (HealthState, VerificationAuthority) {
    merge_state(
        existing,
        existing_authority,
        candidate,
        candidate_authority,
        HealthState::Unavailable,
        diagnostics,
        source_kind,
    )
}

fn merge_state<T: Copy + Eq>(
    existing: T,
    existing_authority: VerificationAuthority,
    candidate: T,
    candidate_authority: VerificationAuthority,
    conflict: T,
    diagnostics: &mut Vec<DiscoveryDiagnostic>,
    source_kind: ObservationSourceKind,
) -> (T, VerificationAuthority) {
    if existing == candidate {
        return (
            existing,
            merge_authority(existing_authority, candidate_authority),
        );
    }
    let existing_rank = authority_rank(existing_authority);
    let candidate_rank = authority_rank(candidate_authority);
    if candidate_rank > existing_rank {
        (candidate, candidate_authority)
    } else if candidate_rank < existing_rank {
        (existing, existing_authority)
    } else {
        push_diagnostic(
            diagnostics,
            source_kind,
            DiscoveryDiagnosticCode::StateConflict,
        );
        (conflict, existing_authority)
    }
}

fn merge_catalog(existing: &mut CandidateProjection, candidate: &CandidateProjection) {
    if existing
        .diagnostics
        .iter()
        .any(|diagnostic| diagnostic.code == DiscoveryDiagnosticCode::CatalogConflict)
    {
        existing.catalog_revision = None;
        existing.models.clear();
        existing.catalog_source_kind = None;
        existing.catalog_trust_level = None;
        existing.catalog_authority = None;
        return;
    }
    let candidate_has_catalog =
        candidate.catalog_revision.is_some() || !candidate.models.is_empty();
    if !candidate_has_catalog {
        return;
    }
    if existing.catalog_revision.is_none() && existing.models.is_empty() {
        existing.catalog_revision = candidate.catalog_revision.clone();
        existing.models = candidate.models.clone();
        existing.catalog_source_kind = candidate.catalog_source_kind;
        existing.catalog_trust_level = candidate.catalog_trust_level;
        existing.catalog_authority = candidate.catalog_authority;
        return;
    }
    let existing_authority = existing
        .catalog_authority
        .unwrap_or(VerificationAuthority::Unverified);
    let candidate_authority = candidate
        .catalog_authority
        .unwrap_or(VerificationAuthority::Unverified);
    let existing_trust = existing
        .catalog_trust_level
        .unwrap_or(ObservationTrustLevel::Untrusted);
    let candidate_trust = candidate
        .catalog_trust_level
        .unwrap_or(ObservationTrustLevel::Untrusted);
    let revision_conflict = existing.catalog_revision != candidate.catalog_revision;
    if revision_conflict {
        catalog_conflict(existing);
        return;
    }
    let existing_model_set = existing.models.iter().cloned().collect::<BTreeSet<_>>();
    let candidate_model_set = candidate.models.iter().cloned().collect::<BTreeSet<_>>();
    if existing_model_set == candidate_model_set {
        merge_catalog_metadata(
            existing,
            candidate,
            existing_authority,
            candidate_authority,
            existing_trust,
            candidate_trust,
        );
        return;
    }
    let candidate_dominates = authority_rank(candidate_authority)
        >= authority_rank(existing_authority)
        && trust_rank(candidate_trust) >= trust_rank(existing_trust)
        && (authority_rank(candidate_authority) > authority_rank(existing_authority)
            || trust_rank(candidate_trust) > trust_rank(existing_trust));
    let existing_dominates = authority_rank(existing_authority)
        >= authority_rank(candidate_authority)
        && trust_rank(existing_trust) >= trust_rank(candidate_trust)
        && (authority_rank(existing_authority) > authority_rank(candidate_authority)
            || trust_rank(existing_trust) > trust_rank(candidate_trust));
    if existing_dominates {
        return;
    }
    if candidate_dominates {
        existing.models = candidate.models.clone();
        existing.catalog_revision = candidate.catalog_revision.clone();
        existing.catalog_source_kind = candidate.catalog_source_kind;
        existing.catalog_trust_level = candidate.catalog_trust_level;
        existing.catalog_authority = candidate.catalog_authority;
        return;
    }
    catalog_conflict(existing);
}

fn merge_catalog_metadata(
    existing: &mut CandidateProjection,
    candidate: &CandidateProjection,
    existing_authority: VerificationAuthority,
    candidate_authority: VerificationAuthority,
    existing_trust: ObservationTrustLevel,
    candidate_trust: ObservationTrustLevel,
) {
    existing.catalog_authority = Some(merge_authority(existing_authority, candidate_authority));
    existing.catalog_trust_level = Some(merge_trust(existing_trust, candidate_trust));
    existing.catalog_source_kind = Some(select_source_kind(&[
        existing.catalog_source_kind.unwrap_or(existing.source_kind),
        candidate
            .catalog_source_kind
            .unwrap_or(candidate.source_kind),
    ]));
}

fn catalog_conflict(existing: &mut CandidateProjection) {
    existing.catalog_revision = None;
    existing.models.clear();
    existing.catalog_source_kind = None;
    existing.catalog_trust_level = None;
    existing.catalog_authority = None;
    push_diagnostic(
        &mut existing.diagnostics,
        existing.source_kind,
        DiscoveryDiagnosticCode::CatalogConflict,
    );
}

fn derive_requires_configuration(candidate: &CandidateProjection) -> bool {
    candidate.availability != CandidateAvailability::Available
        || candidate.compatibility_state != CompatibilityState::Compatible
        || !matches!(
            candidate.auth_state,
            AuthState::Ready | AuthState::NotRequired
        )
        || candidate.health_state != HealthState::Ready
        || candidate.trust_level == ObservationTrustLevel::Untrusted
        || candidate.category == CandidateCategory::Unknown
        || candidate.diagnostics.iter().any(|diagnostic| {
            matches!(
                diagnostic.code,
                DiscoveryDiagnosticCode::ConnectorConflict
                    | DiscoveryDiagnosticCode::RuntimeTypeConflict
                    | DiscoveryDiagnosticCode::InvalidIdentity
                    | DiscoveryDiagnosticCode::CategoryConflict
                    | DiscoveryDiagnosticCode::DiscoveryStateConflict
                    | DiscoveryDiagnosticCode::CatalogConflict
            )
        })
}

fn has_fail_closed_diagnostic(candidate: &CandidateProjection) -> bool {
    candidate.diagnostics.iter().any(|diagnostic| {
        matches!(
            diagnostic.code,
            DiscoveryDiagnosticCode::ConnectorConflict
                | DiscoveryDiagnosticCode::RuntimeTypeConflict
                | DiscoveryDiagnosticCode::InvalidIdentity
                | DiscoveryDiagnosticCode::CategoryConflict
                | DiscoveryDiagnosticCode::DiscoveryStateConflict
        )
    })
}

fn authority_rank(value: VerificationAuthority) -> u8 {
    match value {
        VerificationAuthority::Authoritative => 3,
        VerificationAuthority::Heuristic => 2,
        VerificationAuthority::Unverified => 1,
    }
}

fn trust_rank(value: ObservationTrustLevel) -> u8 {
    match value {
        ObservationTrustLevel::FirstParty => 3,
        ObservationTrustLevel::UserSelected => 3,
        ObservationTrustLevel::Heuristic => 2,
        ObservationTrustLevel::Untrusted => 1,
    }
}

fn push_diagnostic(
    diagnostics: &mut Vec<DiscoveryDiagnostic>,
    source_kind: ObservationSourceKind,
    code: DiscoveryDiagnosticCode,
) {
    let diagnostic = DiscoveryDiagnostic { source_kind, code };
    if !diagnostics.contains(&diagnostic) {
        diagnostics.push(diagnostic);
    }
}

fn project_identifier(value: &str) -> Option<String> {
    let value = value.trim();
    let valid = !value.is_empty()
        && value.len() <= 128
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-'));
    valid.then(|| value.to_owned())
}

fn project_display_name(value: &str) -> Option<String> {
    let value = value.trim();
    let valid = !value.is_empty()
        && value.len() <= 96
        && !contains_sensitive_marker(value)
        && !looks_like_path_or_uri(value)
        && value.chars().all(|ch| {
            !is_forbidden_text_control(ch)
                && (ch.is_alphanumeric()
                    || ch.is_whitespace()
                    || matches!(ch, '(' | ')' | '[' | ']' | '.' | '_' | '-' | '+')
                    || ('\u{4e00}'..='\u{9fff}').contains(&ch))
        });
    valid.then(|| value.to_owned())
}

fn project_model_ids(values: &[String]) -> Vec<String> {
    values
        .iter()
        .filter_map(|value| project_model_id(value))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn project_model_id(value: &str) -> Option<String> {
    let value = value.trim();
    let valid = !value.is_empty()
        && value.len() <= 160
        && !value.contains("//")
        && !value.contains("../")
        && !value.contains("..\\")
        && !value.starts_with('/')
        && !looks_like_windows_absolute_path(value)
        && !looks_like_path_or_uri(value)
        && !contains_sensitive_marker(value)
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-' | '/'));
    valid.then(|| value.to_owned())
}

fn contains_sensitive_marker(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    [
        "authorization",
        "cookie",
        "bearer",
        "runtime_token",
        "runtimetoken",
        "api_key",
        "apikey",
        "secret",
        "credential",
    ]
    .iter()
    .any(|marker| lower.contains(marker))
}

fn looks_like_path_or_uri(value: &str) -> bool {
    value.contains("://") || value.contains('\\') || looks_like_windows_absolute_path(value)
}

fn looks_like_windows_absolute_path(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() >= 3
        && bytes[1] == b':'
        && (bytes[2] == b'\\' || bytes[2] == b'/')
        && bytes[0].is_ascii_alphabetic()
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

fn project_catalog_revision(value: &str) -> Option<String> {
    let value = value.trim();
    let valid = !value.is_empty()
        && value.len() <= 64
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-'));
    valid.then(|| value.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::windows::process::CommandExt;
    use std::process::Stdio;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::{SystemTime, UNIX_EPOCH};

    const CREATE_NO_WINDOW: u32 = 0x0800_0000;

    #[test]
    fn empty_windows_environment_block_is_double_nul_and_never_inherits() {
        // With no SystemRoot/WINDIR and an empty allowlist, the block must be
        // a valid double-NUL empty environment, never a single NUL (which
        // would be a malformed block).
        let block = minimal_windows_environment_block_with_lookup(&[], &[], |_| None);
        assert_eq!(
            block,
            vec![0u16, 0u16],
            "empty environment must be two NULs"
        );
        // A non-empty block still ends with the double-NUL terminator and
        // contains only the explicitly allowed name/value pair.
        let block = minimal_windows_environment_block_with_lookup(&[], &[], |name| {
            (name == "SystemRoot").then(|| std::ffi::OsString::from(r"C:\Windows"))
        });
        let text = String::from_utf16_lossy(&block);
        assert!(
            text.starts_with("SystemRoot=C:\\Windows\0"),
            "unexpected block: {text:?}"
        );
        assert!(
            block.ends_with(&[0u16, 0u16]),
            "block must end with two NULs"
        );

        let block = minimal_windows_environment_block_with_lookup(
            &[],
            &["LX_API_KEY".to_owned()],
            |name| match name {
                "LX_API_KEY" => Some(std::ffi::OsString::from("redacted-test-value")),
                _ => None,
            },
        );
        let text = String::from_utf16_lossy(&block);
        assert!(text.starts_with("LX_API_KEY=redacted-test-value\0"));
    }

    #[test]
    fn nested_job_assignment_succeeds_on_modern_windows() {
        let exe = std::env::current_exe().expect("test binary");
        let output = std::process::Command::new(exe)
            .args([
                "--exact",
                "discovery::tests::nested_job_probe_child",
                "--nocapture",
            ])
            .output()
            .expect("spawn nested job probe child");
        assert!(
            output.status.success(),
            "nested job probe child failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn nested_job_probe_child() {
        // Runs only as the helper process spawned above. It assigns itself to
        // a parent job and then attempts the same nested assignment the
        // managed spawn performs (a second, independent job), to mechanically
        // answer whether the host-in-a-Job scenario breaks spawn.
        use windows_sys::Win32::System::JobObjects::{AssignProcessToJobObject, CreateJobObjectW};
        use windows_sys::Win32::System::Threading::GetCurrentProcess;
        let parent = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
        assert!(!parent.is_null(), "create parent job");
        let self_proc = unsafe { GetCurrentProcess() };
        let assigned_self = unsafe { AssignProcessToJobObject(parent, self_proc) };
        assert_ne!(assigned_self, 0, "assign self to parent job");
        let child = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
        assert!(!child.is_null(), "create child job");
        let nested = unsafe { AssignProcessToJobObject(child, self_proc) };
        assert_ne!(
            nested, 0,
            "nested job assignment must succeed on Windows 8+; the host-in-a-job scenario does not break managed spawn"
        );
    }

    #[derive(Clone)]
    struct FixedProvider {
        observations: Vec<Observation>,
        error: Option<&'static str>,
        call_count: Arc<Mutex<usize>>,
        remaining_budget_seen: Arc<Mutex<Vec<usize>>>,
    }

    #[derive(Clone)]
    struct OwnedBlockingWorkloadProvider {
        marker: String,
        owned_processes: Arc<Mutex<Vec<OwnedProcessTreeRecord>>>,
        capture_descendants: bool,
    }

    #[derive(Clone)]
    struct NonCooperativeManagedProvider {
        marker: String,
        started_processes: Arc<Mutex<Vec<ManagedProviderProcessRecord>>>,
        capture_descendants: bool,
    }

    #[derive(Clone)]
    struct UnmanagedBlockingProvider;

    #[derive(Clone)]
    struct PrefabSuccessManagedProvider;

    #[derive(Clone)]
    struct LargeRequestNonReadingManagedProvider {
        marker: String,
        started_processes: Arc<Mutex<Vec<ManagedProviderProcessRecord>>>,
        request_payload_bytes: usize,
    }

    #[derive(Clone)]
    struct CrashingNonReadingManagedProvider {
        marker: String,
        started_processes: Arc<Mutex<Vec<ManagedProviderProcessRecord>>>,
        request_payload_bytes: usize,
    }

    #[derive(Clone)]
    struct HandleInheritanceProbeManagedProvider {
        sentinel_handle: usize,
        probe: Arc<HandleProbeFixture>,
    }

    #[derive(Clone)]
    struct AttributeListFailureManagedProvider {
        marker: String,
        started_processes: Arc<Mutex<Vec<ManagedProviderProcessRecord>>>,
    }

    #[derive(Clone, Debug)]
    struct OwnedProcessTreeRecord {
        root_pid: u32,
        descendant_pids: Vec<u32>,
    }

    #[derive(Clone)]
    struct EmitThenErrorProvider {
        observations: Vec<Observation>,
    }

    #[derive(Clone)]
    struct ObservationFixtureContext {
        locator: ObservationLocator,
        stable_key: Vec<String>,
        display_name: String,
        availability: CandidateAvailability,
        compatibility_state: CompatibilityState,
        auth_state: AuthState,
        health_state: HealthState,
        evidence_summary: Vec<DiscoveryEvidence>,
    }

    impl DiscoveryProvider for FixedProvider {
        fn source_kind(&self) -> ObservationSourceKind {
            ObservationSourceKind::ExecutableInventory
        }

        fn execution(&self) -> DiscoveryProviderExecution {
            DiscoveryProviderExecution::InlineAllowedForTests
        }

        fn collect(
            &self,
            context: &DiscoveryContext<'_>,
            emit: &mut dyn FnMut(Observation) -> bool,
        ) -> Result<(), DiscoveryProviderError> {
            *self.call_count.lock().unwrap() += 1;
            if let Some(error) = self.error {
                let _ = error;
                return Err(DiscoveryProviderError {
                    source_kind: ObservationSourceKind::ExecutableInventory,
                    code: DiscoveryDiagnosticCode::ProviderFailed,
                });
            }
            for observation in &self.observations {
                self.remaining_budget_seen
                    .lock()
                    .unwrap()
                    .push(context.remaining_results());
                if !emit(observation.clone()) {
                    break;
                }
            }
            Ok(())
        }
    }

    impl DiscoveryProvider for OwnedBlockingWorkloadProvider {
        fn source_kind(&self) -> ObservationSourceKind {
            ObservationSourceKind::RuntimeRecord
        }

        fn execution(&self) -> DiscoveryProviderExecution {
            DiscoveryProviderExecution::InlineAllowedForTests
        }

        fn collect(
            &self,
            context: &DiscoveryContext<'_>,
            _emit: &mut dyn FnMut(Observation) -> bool,
        ) -> Result<(), DiscoveryProviderError> {
            let mut child = std::process::Command::new("waitfor.exe")
                .arg(&self.marker)
                .creation_flags(CREATE_NO_WINDOW)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("start owned blocking workload fixture");
            let root_pid = child.id();
            let descendant_pids = if self.capture_descendants {
                thread::sleep(Duration::from_millis(100));
                process_descendants(root_pid)
            } else {
                Vec::new()
            };
            self.owned_processes
                .lock()
                .unwrap()
                .push(OwnedProcessTreeRecord {
                    root_pid,
                    descendant_pids,
                });
            while !context.should_stop() {
                match child.try_wait() {
                    Ok(Some(_)) => break,
                    Ok(None) => thread::sleep(Duration::from_millis(5)),
                    Err(_) => break,
                }
            }
            let _ = child.kill();
            let _ = child.wait();
            Err(DiscoveryProviderError {
                source_kind: ObservationSourceKind::RuntimeRecord,
                code: DiscoveryDiagnosticCode::ProviderTimeout,
            })
        }
    }

    impl DiscoveryProvider for NonCooperativeManagedProvider {
        fn source_kind(&self) -> ObservationSourceKind {
            ObservationSourceKind::RuntimeRecord
        }

        fn managed_process(
            &self,
            _context: &DiscoveryContext<'_>,
        ) -> Option<ManagedProviderProcessSpec> {
            let (executable, args) = if self.capture_descendants {
                let marker = self.marker.replace('\'', "''");
                let waitfor = system_executable("waitfor.exe").display().to_string();
                (
                    system_executable("WindowsPowerShell\\v1.0\\powershell.exe"),
                    vec![
                        "-NoProfile".into(),
                        "-ExecutionPolicy".into(),
                        "Bypass".into(),
                        "-Command".into(),
                        format!(
                            "$m = '{marker}'; $w = '{waitfor}'; Start-Process -FilePath $w -ArgumentList ($m + 'child') -WindowStyle Hidden; & $w ($m + 'root')"
                        ),
                    ],
                )
            } else {
                (system_executable("waitfor.exe"), vec![self.marker.clone()])
            };
            Some(ManagedProviderProcessSpec {
                executable,
                args,
                request: dummy_worker_request(),
                started_processes: Some(Arc::clone(&self.started_processes)),
                capture_descendants: self.capture_descendants,
                force_attribute_list_failure: false,
            })
        }

        fn collect(
            &self,
            _context: &DiscoveryContext<'_>,
            _emit: &mut dyn FnMut(Observation) -> bool,
        ) -> Result<(), DiscoveryProviderError> {
            panic!("managed non-cooperative provider must not use synchronous collect")
        }
    }

    impl DiscoveryProvider for UnmanagedBlockingProvider {
        fn source_kind(&self) -> ObservationSourceKind {
            ObservationSourceKind::RuntimeRecord
        }

        fn collect(
            &self,
            _context: &DiscoveryContext<'_>,
            _emit: &mut dyn FnMut(Observation) -> bool,
        ) -> Result<(), DiscoveryProviderError> {
            thread::sleep(Duration::from_millis(250));
            Ok(())
        }
    }

    impl DiscoveryProvider for PrefabSuccessManagedProvider {
        fn source_kind(&self) -> ObservationSourceKind {
            ObservationSourceKind::RuntimeRecord
        }

        fn managed_process(
            &self,
            _context: &DiscoveryContext<'_>,
        ) -> Option<ManagedProviderProcessSpec> {
            Some(ManagedProviderProcessSpec {
                executable: system_executable("WindowsPowerShell\\v1.0\\powershell.exe"),
                args: vec![
                    "-NoProfile".into(),
                    "-ExecutionPolicy".into(),
                    "Bypass".into(),
                    "-Command".into(),
                    "exit 0".into(),
                ],
                request: dummy_worker_request(),
                started_processes: None,
                capture_descendants: false,
                force_attribute_list_failure: false,
            })
        }

        fn collect(
            &self,
            _context: &DiscoveryContext<'_>,
            _emit: &mut dyn FnMut(Observation) -> bool,
        ) -> Result<(), DiscoveryProviderError> {
            panic!("prefab success provider must not use synchronous collect")
        }
    }

    impl DiscoveryProvider for LargeRequestNonReadingManagedProvider {
        fn source_kind(&self) -> ObservationSourceKind {
            ObservationSourceKind::RuntimeRecord
        }

        fn managed_process(
            &self,
            _context: &DiscoveryContext<'_>,
        ) -> Option<ManagedProviderProcessSpec> {
            Some(ManagedProviderProcessSpec {
                executable: system_executable("waitfor.exe"),
                args: vec![self.marker.clone()],
                request: worker_request_with_payload(self.request_payload_bytes),
                started_processes: Some(Arc::clone(&self.started_processes)),
                capture_descendants: false,
                force_attribute_list_failure: false,
            })
        }

        fn collect(
            &self,
            _context: &DiscoveryContext<'_>,
            _emit: &mut dyn FnMut(Observation) -> bool,
        ) -> Result<(), DiscoveryProviderError> {
            panic!("large non-reading provider must use managed process")
        }
    }

    impl DiscoveryProvider for CrashingNonReadingManagedProvider {
        fn source_kind(&self) -> ObservationSourceKind {
            ObservationSourceKind::RuntimeRecord
        }

        fn managed_process(
            &self,
            _context: &DiscoveryContext<'_>,
        ) -> Option<ManagedProviderProcessSpec> {
            let marker = self.marker.replace('\'', "''");
            Some(ManagedProviderProcessSpec {
                executable: system_executable("WindowsPowerShell\\v1.0\\powershell.exe"),
                args: vec![
                    "-NoProfile".into(),
                    "-ExecutionPolicy".into(),
                    "Bypass".into(),
                    "-Command".into(),
                    format!("$m = '{marker}'; Start-Sleep -Milliseconds 80; exit 9"),
                ],
                request: worker_request_with_payload(self.request_payload_bytes),
                started_processes: Some(Arc::clone(&self.started_processes)),
                capture_descendants: false,
                force_attribute_list_failure: false,
            })
        }

        fn collect(
            &self,
            _context: &DiscoveryContext<'_>,
            _emit: &mut dyn FnMut(Observation) -> bool,
        ) -> Result<(), DiscoveryProviderError> {
            panic!("crashing non-reading provider must use managed process")
        }
    }

    impl DiscoveryProvider for HandleInheritanceProbeManagedProvider {
        fn source_kind(&self) -> ObservationSourceKind {
            ObservationSourceKind::RuntimeRecord
        }

        fn managed_process(
            &self,
            _context: &DiscoveryContext<'_>,
        ) -> Option<ManagedProviderProcessSpec> {
            Some(ManagedProviderProcessSpec {
                executable: self.probe.executable().to_owned(),
                args: vec![self.sentinel_handle.to_string()],
                request: dummy_worker_request(),
                started_processes: None,
                capture_descendants: false,
                force_attribute_list_failure: false,
            })
        }

        fn collect(
            &self,
            _context: &DiscoveryContext<'_>,
            _emit: &mut dyn FnMut(Observation) -> bool,
        ) -> Result<(), DiscoveryProviderError> {
            panic!("handle inheritance probe provider must use managed process")
        }
    }

    impl DiscoveryProvider for AttributeListFailureManagedProvider {
        fn source_kind(&self) -> ObservationSourceKind {
            ObservationSourceKind::RuntimeRecord
        }

        fn managed_process(
            &self,
            _context: &DiscoveryContext<'_>,
        ) -> Option<ManagedProviderProcessSpec> {
            Some(ManagedProviderProcessSpec {
                executable: system_executable("waitfor.exe"),
                args: vec![self.marker.clone()],
                request: dummy_worker_request(),
                started_processes: Some(Arc::clone(&self.started_processes)),
                capture_descendants: false,
                force_attribute_list_failure: true,
            })
        }

        fn collect(
            &self,
            _context: &DiscoveryContext<'_>,
            _emit: &mut dyn FnMut(Observation) -> bool,
        ) -> Result<(), DiscoveryProviderError> {
            panic!("attribute failure provider must use managed process")
        }
    }

    impl DiscoveryProvider for EmitThenErrorProvider {
        fn source_kind(&self) -> ObservationSourceKind {
            ObservationSourceKind::RuntimeRecord
        }

        fn execution(&self) -> DiscoveryProviderExecution {
            DiscoveryProviderExecution::InlineAllowedForTests
        }

        fn collect(
            &self,
            _context: &DiscoveryContext<'_>,
            emit: &mut dyn FnMut(Observation) -> bool,
        ) -> Result<(), DiscoveryProviderError> {
            for observation in &self.observations {
                let _ = emit(observation.clone());
            }
            Err(DiscoveryProviderError {
                source_kind: ObservationSourceKind::RuntimeRecord,
                code: DiscoveryDiagnosticCode::ProviderFailed,
            })
        }
    }

    fn observation(
        stable_key: &[&str],
        display_name: &str,
        availability: &str,
        compatibility_state: CompatibilityState,
        auth_state: AuthState,
        health_state: HealthState,
        evidence_summary: Vec<DiscoveryEvidence>,
    ) -> Observation {
        observation_from_context(ObservationFixtureContext {
            locator: ObservationLocator::Executable(PathBuf::from(
                r"C:\fixture\absolute\path\codex-token-authorization-cookie.exe",
            )),
            stable_key: stable_key
                .iter()
                .map(|value| (*value).to_owned())
                .collect::<Vec<_>>(),
            display_name: display_name.into(),
            availability: availability_from_str(availability),
            compatibility_state,
            auth_state,
            health_state,
            evidence_summary,
        })
    }

    fn dummy_worker_request() -> ManagedProviderWorkerRequest {
        worker_request_with_payload(0)
    }

    fn worker_request_with_payload(payload_bytes: usize) -> ManagedProviderWorkerRequest {
        ManagedProviderWorkerRequest::new(
            ManagedProviderWorkerKind::Codex,
            40,
            32,
            false,
            serde_json::json!({"fixture": true, "payload": "x".repeat(payload_bytes)}),
        )
    }

    struct HandleProbeFixture {
        root: PathBuf,
        executable: PathBuf,
    }

    impl HandleProbeFixture {
        fn compile() -> Self {
            let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
            let source = manifest_dir
                .join("tests")
                .join("fixtures")
                .join("handle_inheritance_probe.rs");
            assert!(
                source.is_file(),
                "handle inheritance probe fixture source missing at {}",
                source.display()
            );
            let out_dir = unique_handle_probe_fixture_dir();
            std::fs::create_dir(&out_dir).expect("create probe fixture output directory");
            let executable = out_dir.join(if cfg!(windows) {
                "agenttalk-handle-inheritance-probe-fixture.exe"
            } else {
                "agenttalk-handle-inheritance-probe-fixture"
            });
            let fixture = Self {
                root: out_dir,
                executable,
            };
            let output = std::process::Command::new("rustc")
                .arg("--edition=2021")
                .arg(&source)
                .arg("-o")
                .arg(&fixture.executable)
                .output()
                .expect("run rustc for handle inheritance probe fixture");
            assert!(
                output.status.success(),
                "compile handle inheritance probe fixture failed: {}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            fixture
        }

        fn executable(&self) -> &Path {
            &self.executable
        }

        fn root(&self) -> &Path {
            &self.root
        }
    }

    impl Drop for HandleProbeFixture {
        fn drop(&mut self) {
            if !self.root.exists() {
                return;
            }
            if let Err(error) = std::fs::remove_dir_all(&self.root) {
                let message = format!(
                    "remove handle probe fixture directory {} failed: {error}",
                    self.root.display()
                );
                if std::thread::panicking() {
                    eprintln!("{message}");
                } else {
                    panic!("{message}");
                }
            }
        }
    }

    fn unique_handle_probe_fixture_dir() -> PathBuf {
        static NEXT_FIXTURE_NONCE: AtomicUsize = AtomicUsize::new(0);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        for attempt in 0..64usize {
            let nonce = NEXT_FIXTURE_NONCE.fetch_add(1, Ordering::AcqRel);
            let path = std::env::temp_dir().join(format!(
                "agenttalk-handle-probe-fixture-{}-{nanos:x}-{nonce:x}-{attempt:x}",
                std::process::id()
            ));
            if !path.exists() {
                return path;
            }
        }
        panic!("unable to allocate unique handle probe fixture directory")
    }

    fn handle_inheritance_probe_executable_for_tests() -> HandleProbeFixture {
        HandleProbeFixture::compile()
    }

    fn handle_probe_fixture_temp_dirs() -> BTreeSet<PathBuf> {
        std::fs::read_dir(std::env::temp_dir())
            .unwrap()
            .filter_map(Result::ok)
            .filter_map(|entry| {
                let path = entry.path();
                let name = path.file_name()?.to_str()?;
                (name.starts_with("agenttalk-handle-probe-fixture-")).then_some(path)
            })
            .collect()
    }

    fn assert_no_new_handle_probe_fixture_dirs(baseline: &BTreeSet<PathBuf>) {
        let current = handle_probe_fixture_temp_dirs();
        let added = current.difference(baseline).collect::<Vec<_>>();
        assert!(
            added.is_empty(),
            "new handle probe fixture directories remain: {added:?}"
        );
        for path in baseline {
            assert!(
                path.exists(),
                "pre-existing handle probe fixture baseline was touched: {}",
                path.display()
            );
        }
    }

    fn assert_owned_handle_probe_fixture_dir_removed(
        baseline: &BTreeSet<PathBuf>,
        owned_root: &Path,
    ) {
        assert!(
            !owned_root.exists(),
            "owned handle probe fixture directory remains: {}",
            owned_root.display()
        );
        assert_no_new_handle_probe_fixture_dirs(baseline);
    }

    fn observation_from_context(context: ObservationFixtureContext) -> Observation {
        Observation {
            locator: context.locator,
            fingerprint: ObservationFingerprint::from_parts(&context.stable_key),
            association_fingerprints: Vec::new(),
            source_kind: if context
                .evidence_summary
                .contains(&DiscoveryEvidence::RuntimeRecord)
            {
                ObservationSourceKind::RuntimeRecord
            } else {
                ObservationSourceKind::ExecutableInventory
            },
            category: CandidateCategory::AgentRuntime,
            trust_level: if context
                .evidence_summary
                .contains(&DiscoveryEvidence::RuntimeRecord)
            {
                ObservationTrustLevel::FirstParty
            } else {
                ObservationTrustLevel::Heuristic
            },
            verification_authority: if context
                .evidence_summary
                .contains(&DiscoveryEvidence::IdentityMismatch)
            {
                VerificationAuthority::Authoritative
            } else if context
                .evidence_summary
                .contains(&DiscoveryEvidence::Available)
            {
                VerificationAuthority::Heuristic
            } else {
                VerificationAuthority::Unverified
            },
            availability_authority: if context
                .evidence_summary
                .contains(&DiscoveryEvidence::IdentityMismatch)
            {
                VerificationAuthority::Authoritative
            } else if context
                .evidence_summary
                .contains(&DiscoveryEvidence::Available)
            {
                VerificationAuthority::Heuristic
            } else {
                VerificationAuthority::Unverified
            },
            discovery_authority: if context
                .evidence_summary
                .contains(&DiscoveryEvidence::IdentityMismatch)
            {
                VerificationAuthority::Authoritative
            } else if context
                .evidence_summary
                .contains(&DiscoveryEvidence::Available)
            {
                VerificationAuthority::Heuristic
            } else {
                VerificationAuthority::Unverified
            },
            compatibility_authority: if context
                .evidence_summary
                .contains(&DiscoveryEvidence::IdentityMismatch)
            {
                VerificationAuthority::Authoritative
            } else if context
                .evidence_summary
                .contains(&DiscoveryEvidence::Available)
            {
                VerificationAuthority::Heuristic
            } else {
                VerificationAuthority::Unverified
            },
            auth_authority: if context
                .evidence_summary
                .contains(&DiscoveryEvidence::IdentityMismatch)
            {
                VerificationAuthority::Authoritative
            } else if context
                .evidence_summary
                .contains(&DiscoveryEvidence::Available)
            {
                VerificationAuthority::Heuristic
            } else {
                VerificationAuthority::Unverified
            },
            health_authority: if context
                .evidence_summary
                .contains(&DiscoveryEvidence::IdentityMismatch)
            {
                VerificationAuthority::Authoritative
            } else if context
                .evidence_summary
                .contains(&DiscoveryEvidence::Available)
            {
                VerificationAuthority::Heuristic
            } else {
                VerificationAuthority::Unverified
            },
            connector_id: "local.fixture".into(),
            runtime_type: "fixture".into(),
            display_name: context.display_name,
            availability: context.availability,
            models: vec!["model-a".into()],
            catalog_revision: Some("7".into()),
            requires_configuration: false,
            discovery_state: DiscoveryState::Identified,
            compatibility_state: context.compatibility_state,
            auth_state: context.auth_state,
            health_state: context.health_state,
            evidence_summary: context.evidence_summary,
            diagnostics: Vec::new(),
        }
    }

    fn availability_from_str(value: &str) -> CandidateAvailability {
        match value {
            "available" => CandidateAvailability::Available,
            "authentication_required" => CandidateAvailability::AuthenticationRequired,
            "unconfigured" => CandidateAvailability::Unconfigured,
            _ => CandidateAvailability::Unavailable,
        }
    }

    fn unique_process_marker(name: &str) -> String {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock after Unix epoch")
            .as_nanos();
        format!("agenttalkw14{name}{}{}", std::process::id(), now)
    }

    fn process_fixture_guard() -> std::sync::MutexGuard<'static, ()> {
        super::managed_process_fixture_guard_for_tests()
    }

    fn handle_probe_fixture_guard() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: std::sync::OnceLock<Mutex<()>> = std::sync::OnceLock::new();
        match LOCK.get_or_init(|| Mutex::new(())).lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn system_executable(name: &str) -> PathBuf {
        std::env::var_os("SystemRoot")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(r"C:\Windows"))
            .join("System32")
            .join(name)
    }

    fn fixture_process_inventory(marker: &str) -> Vec<u32> {
        let escaped = marker.replace('\'', "''");
        let script = format!(
            "$m = '{escaped}'; Get-CimInstance Win32_Process -Filter \"Name = 'waitfor.exe'\" | Where-Object {{ $_.CommandLine -like \"*$m*\" }} | ForEach-Object {{ $_.ProcessId }}"
        );
        let output = std::process::Command::new("powershell.exe")
            .args(["-NoProfile", "-Command", &script])
            .output()
            .expect("query fixture process inventory");
        assert!(
            output.status.success(),
            "fixture process inventory failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .filter_map(|line| line.trim().parse::<u32>().ok())
            .collect()
    }

    fn process_descendants(pid: u32) -> Vec<u32> {
        let script = format!(
            "Get-CimInstance Win32_Process -Filter \"ParentProcessId = {pid}\" | ForEach-Object {{ $_.ProcessId }}"
        );
        let output = std::process::Command::new("powershell.exe")
            .args(["-NoProfile", "-Command", &script])
            .output()
            .expect("query process descendants");
        assert!(
            output.status.success(),
            "process descendant inventory failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .filter_map(|line| line.trim().parse::<u32>().ok())
            .collect()
    }

    fn wait_for_no_fixture_processes(marker: &str) -> bool {
        let deadline = Instant::now() + Duration::from_millis(500);
        while Instant::now() < deadline {
            if fixture_process_inventory(marker).is_empty() {
                return true;
            }
            thread::sleep(Duration::from_millis(20));
        }
        fixture_process_inventory(marker).is_empty()
    }

    fn wait_for_marked_process_gone(pid: u32, marker: &str) -> bool {
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            if !process_with_marker_exists(pid, marker) {
                return true;
            }
            thread::sleep(Duration::from_millis(20));
        }
        !process_with_marker_exists(pid, marker)
    }

    fn assert_marked_process_tree_reaped(record: &ManagedProviderProcessRecord, marker: &str) {
        assert!(
            wait_for_marked_process_gone(record.root_pid, marker),
            "owned root pid {} with fixture marker must exit",
            record.root_pid
        );
        for pid in &record.descendant_pids {
            assert!(
                wait_for_marked_process_gone(*pid, marker),
                "owned descendant pid {pid} with fixture marker must exit"
            );
        }
    }

    fn assert_owned_process_tree_reaped(record: &OwnedProcessTreeRecord, marker: &str) {
        assert!(
            wait_for_marked_process_gone(record.root_pid, marker),
            "owned root pid {} with fixture marker must exit",
            record.root_pid
        );
        for pid in &record.descendant_pids {
            assert!(
                wait_for_marked_process_gone(*pid, marker),
                "owned descendant pid {pid} with fixture marker must exit"
            );
        }
    }

    fn process_with_marker_exists(pid: u32, marker: &str) -> bool {
        let escaped = marker.replace('\'', "''");
        let script = format!(
            "$m = '{escaped}'; if (Get-CimInstance Win32_Process -Filter \"ProcessId = {pid}\" | Where-Object {{ $_.CommandLine -like \"*$m*\" }}) {{ '1' }}"
        );
        let output = std::process::Command::new("powershell.exe")
            .args(["-NoProfile", "-Command", &script])
            .output()
            .expect("query marked process by pid");
        output.status.success() && String::from_utf8_lossy(&output.stdout).contains('1')
    }

    #[test]
    fn deduplicates_same_fingerprint_and_is_stable_against_display_changes() {
        let first_calls = Arc::new(Mutex::new(0usize));
        let second_calls = Arc::new(Mutex::new(0usize));
        let first_budget = Arc::new(Mutex::new(Vec::new()));
        let second_budget = Arc::new(Mutex::new(Vec::new()));
        let coordinator = DiscoveryCoordinator::new(vec![
            Box::new(FixedProvider {
                observations: vec![observation(
                    &["stable-id"],
                    "First name",
                    "available",
                    CompatibilityState::Compatible,
                    AuthState::Ready,
                    HealthState::Ready,
                    vec![
                        DiscoveryEvidence::ExecutableInventory,
                        DiscoveryEvidence::VersionMatched,
                        DiscoveryEvidence::BuildMatched,
                    ],
                )],
                error: None,
                call_count: Arc::clone(&first_calls),
                remaining_budget_seen: Arc::clone(&first_budget),
            }),
            Box::new(FixedProvider {
                observations: vec![observation(
                    &["stable-id"],
                    "Second name",
                    "available",
                    CompatibilityState::Compatible,
                    AuthState::Ready,
                    HealthState::Ready,
                    vec![
                        DiscoveryEvidence::ExecutableInventory,
                        DiscoveryEvidence::RuntimeRecord,
                        DiscoveryEvidence::Available,
                    ],
                )],
                error: None,
                call_count: Arc::clone(&second_calls),
                remaining_budget_seen: Arc::clone(&second_budget),
            }),
        ]);
        let policy = DiscoveryPolicy::default();
        let cancelled = AtomicBool::new(false);
        let candidates = coordinator.discover(&policy, &cancelled);
        assert_eq!(candidates.len(), 1);
        assert_eq!(
            candidates[0].candidate_id,
            ObservationFingerprint::from_parts(&["stable-id".to_owned()]).candidate_id()
        );
        assert_eq!(candidates[0].display_name, "First name");
        assert_eq!(*first_calls.lock().unwrap(), 1);
        assert_eq!(*second_calls.lock().unwrap(), 1);
        assert_eq!(*first_budget.lock().unwrap(), vec![1024]);
        assert_eq!(*second_budget.lock().unwrap(), vec![1024]);
    }

    #[test]
    fn projection_is_renderer_safe() {
        let hidden_locator = ObservationLocator::Executable(PathBuf::from(
            r"C:\fixture\absolute\pid-101-port-5555-token-authorization-cookie.exe",
        ));
        let projection = observation_from_context(ObservationFixtureContext {
            locator: hidden_locator,
            stable_key: vec!["stable-id".into()],
            display_name: "Visible name".into(),
            availability: CandidateAvailability::Available,
            compatibility_state: CompatibilityState::Compatible,
            auth_state: AuthState::Ready,
            health_state: HealthState::Ready,
            evidence_summary: vec![
                DiscoveryEvidence::ExecutableInventory,
                DiscoveryEvidence::RuntimeRecord,
                DiscoveryEvidence::AuthenticationRequired,
            ],
        })
        .project();
        let json = serde_json::to_string(&projection).unwrap();
        let debug = format!("{projection:?}");
        let legacy = crate::legacy_local_connector_candidate(projection.clone());
        let legacy_json = serde_json::to_string(&legacy).unwrap();
        for forbidden in [
            r"C:\fixture\absolute\pid-101-port-5555-token-authorization-cookie.exe",
            "token",
            "authorization",
            "cookie",
        ] {
            assert!(!json.contains(forbidden));
            assert!(!debug.contains(forbidden));
            assert!(!legacy_json.contains(forbidden));
        }
        assert!(json.contains("candidateId"));
        assert!(json.contains("evidenceSummary"));
    }

    #[test]
    fn provider_failure_does_not_stop_following_candidates() {
        let failure_calls = Arc::new(Mutex::new(0usize));
        let success_calls = Arc::new(Mutex::new(0usize));
        let coordinator = DiscoveryCoordinator::new(vec![
            Box::new(FixedProvider {
                observations: Vec::new(),
                error: Some("boom"),
                call_count: Arc::clone(&failure_calls),
                remaining_budget_seen: Arc::new(Mutex::new(Vec::new())),
            }),
            Box::new(FixedProvider {
                observations: vec![observation(
                    &["stable-id-2"],
                    "Visible name",
                    "available",
                    CompatibilityState::Compatible,
                    AuthState::Ready,
                    HealthState::Ready,
                    vec![DiscoveryEvidence::RuntimeRecord],
                )],
                error: None,
                call_count: Arc::clone(&success_calls),
                remaining_budget_seen: Arc::new(Mutex::new(Vec::new())),
            }),
        ]);
        let policy = DiscoveryPolicy::default();
        let cancelled = AtomicBool::new(false);
        let candidates = coordinator.discover(&policy, &cancelled);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].display_name, "Visible name");
        assert_eq!(*failure_calls.lock().unwrap(), 1);
        assert_eq!(*success_calls.lock().unwrap(), 1);
    }

    #[test]
    fn provider_budget_caps_overflow_and_exposes_remaining_budget() {
        let first_calls = Arc::new(Mutex::new(0usize));
        let first_budget = Arc::new(Mutex::new(Vec::new()));
        let coordinator = DiscoveryCoordinator::new(vec![Box::new(FixedProvider {
            observations: vec![
                observation(
                    &["budget-1"],
                    "Budget One",
                    "available",
                    CompatibilityState::Compatible,
                    AuthState::Ready,
                    HealthState::Ready,
                    vec![DiscoveryEvidence::ExecutableInventory],
                ),
                observation(
                    &["budget-2"],
                    "Budget Two",
                    "available",
                    CompatibilityState::Compatible,
                    AuthState::Ready,
                    HealthState::Ready,
                    vec![DiscoveryEvidence::RuntimeRecord],
                ),
            ],
            error: None,
            call_count: Arc::clone(&first_calls),
            remaining_budget_seen: Arc::clone(&first_budget),
        })]);
        let policy = DiscoveryPolicy {
            max_results: 1,
            ..DiscoveryPolicy::default()
        };
        let cancelled = AtomicBool::new(false);
        let candidates = coordinator.discover(&policy, &cancelled);
        assert_eq!(candidates.len(), 1);
        assert_eq!(
            candidates[0].candidate_id,
            ObservationFingerprint::from_parts(&["budget-1".to_owned()]).candidate_id()
        );
        assert_eq!(*first_calls.lock().unwrap(), 1);
        assert_eq!(*first_budget.lock().unwrap(), vec![128, 127]);
    }

    #[test]
    fn merge_is_order_independent_for_state_dimensions() {
        let base_a = observation(
            &["stable-state"],
            "Zulu name",
            "unconfigured",
            CompatibilityState::NotVerified,
            AuthState::Required,
            HealthState::NotChecked,
            vec![DiscoveryEvidence::ExecutableInventory],
        );
        let base_b = observation(
            &["stable-state"],
            "Alpha name",
            "available",
            CompatibilityState::Compatible,
            AuthState::Ready,
            HealthState::Ready,
            vec![
                DiscoveryEvidence::ExecutableInventory,
                DiscoveryEvidence::Available,
            ],
        );
        let policy = DiscoveryPolicy::default();
        let cancelled = AtomicBool::new(false);

        let forward = DiscoveryCoordinator::new(vec![
            Box::new(FixedProvider {
                observations: vec![base_a.clone()],
                error: None,
                call_count: Arc::new(Mutex::new(0)),
                remaining_budget_seen: Arc::new(Mutex::new(Vec::new())),
            }),
            Box::new(FixedProvider {
                observations: vec![base_b.clone()],
                error: None,
                call_count: Arc::new(Mutex::new(0)),
                remaining_budget_seen: Arc::new(Mutex::new(Vec::new())),
            }),
        ])
        .discover(&policy, &cancelled);
        let reverse = DiscoveryCoordinator::new(vec![
            Box::new(FixedProvider {
                observations: vec![base_b],
                error: None,
                call_count: Arc::new(Mutex::new(0)),
                remaining_budget_seen: Arc::new(Mutex::new(Vec::new())),
            }),
            Box::new(FixedProvider {
                observations: vec![base_a],
                error: None,
                call_count: Arc::new(Mutex::new(0)),
                remaining_budget_seen: Arc::new(Mutex::new(Vec::new())),
            }),
        ])
        .discover(&policy, &cancelled);

        assert_eq!(forward, reverse);
        let candidate = &forward[0];
        assert_eq!(candidate.availability, CandidateAvailability::Available);
        assert_eq!(
            candidate.compatibility_state,
            CompatibilityState::Compatible
        );
        assert_eq!(candidate.auth_state, AuthState::Ready);
        assert_eq!(candidate.health_state, HealthState::Ready);
    }

    #[test]
    fn max_results_limits_unique_candidates_not_duplicate_observations() {
        let calls = Arc::new(Mutex::new(0usize));
        let budget = Arc::new(Mutex::new(Vec::new()));
        let coordinator = DiscoveryCoordinator::new(vec![Box::new(FixedProvider {
            observations: vec![
                observation(
                    &["x"],
                    "X first",
                    "unconfigured",
                    CompatibilityState::NotVerified,
                    AuthState::Unknown,
                    HealthState::NotChecked,
                    vec![DiscoveryEvidence::ExecutableInventory],
                ),
                observation(
                    &["x"],
                    "X second",
                    "unavailable",
                    CompatibilityState::Incompatible,
                    AuthState::Unknown,
                    HealthState::IdentityMismatch,
                    vec![DiscoveryEvidence::IdentityMismatch],
                ),
                observation(
                    &["y"],
                    "Y",
                    "unconfigured",
                    CompatibilityState::NotVerified,
                    AuthState::Unknown,
                    HealthState::NotChecked,
                    vec![DiscoveryEvidence::RuntimeRecord],
                ),
            ],
            error: None,
            call_count: calls,
            remaining_budget_seen: budget,
        })]);
        let policy = DiscoveryPolicy {
            max_results: 2,
            ..DiscoveryPolicy::default()
        };
        let cancelled = AtomicBool::new(false);
        let candidates = coordinator.discover(&policy, &cancelled);

        assert_eq!(candidates.len(), 2);
        assert!(candidates
            .iter()
            .any(|candidate| candidate.display_name == "X first"));
        assert!(candidates
            .iter()
            .any(|candidate| candidate.display_name == "Y"));
    }

    #[test]
    fn authoritative_identity_mismatch_wins_over_heuristic_available_in_both_orders() {
        let available = observation(
            &["same"],
            "Available",
            "available",
            CompatibilityState::Compatible,
            AuthState::Ready,
            HealthState::Ready,
            vec![
                DiscoveryEvidence::ExecutableInventory,
                DiscoveryEvidence::Available,
            ],
        );
        let mismatch = observation(
            &["same"],
            "Mismatch",
            "unavailable",
            CompatibilityState::Incompatible,
            AuthState::Ready,
            HealthState::IdentityMismatch,
            vec![
                DiscoveryEvidence::RuntimeRecord,
                DiscoveryEvidence::IdentityMismatch,
            ],
        );
        let policy = DiscoveryPolicy::default();
        let cancelled = AtomicBool::new(false);
        let forward = DiscoveryCoordinator::new(vec![Box::new(FixedProvider {
            observations: vec![available.clone(), mismatch.clone()],
            error: None,
            call_count: Arc::new(Mutex::new(0)),
            remaining_budget_seen: Arc::new(Mutex::new(Vec::new())),
        })])
        .discover(&policy, &cancelled);
        let reverse = DiscoveryCoordinator::new(vec![Box::new(FixedProvider {
            observations: vec![mismatch, available],
            error: None,
            call_count: Arc::new(Mutex::new(0)),
            remaining_budget_seen: Arc::new(Mutex::new(Vec::new())),
        })])
        .discover(&policy, &cancelled);

        assert_eq!(forward, reverse);
        assert_eq!(forward[0].availability, CandidateAvailability::Unavailable);
        assert_eq!(
            forward[0].compatibility_state,
            CompatibilityState::Incompatible
        );
        assert_eq!(forward[0].health_state, HealthState::IdentityMismatch);
    }

    #[test]
    fn connector_id_or_runtime_type_conflict_fails_closed() {
        let mut first = observation(
            &["same-conflict"],
            "First",
            "unconfigured",
            CompatibilityState::NotVerified,
            AuthState::Unknown,
            HealthState::NotChecked,
            vec![DiscoveryEvidence::ExecutableInventory],
        );
        first.connector_id = "local.first".into();
        first.runtime_type = "first".into();
        let mut second = first.clone();
        second.connector_id = "local.second".into();
        second.runtime_type = "second".into();

        let policy = DiscoveryPolicy::default();
        let cancelled = AtomicBool::new(false);
        let candidates = DiscoveryCoordinator::new(vec![Box::new(FixedProvider {
            observations: vec![first, second],
            error: None,
            call_count: Arc::new(Mutex::new(0)),
            remaining_budget_seen: Arc::new(Mutex::new(Vec::new())),
        })])
        .discover(&policy, &cancelled);

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].connector_id, "local.discovery.conflict");
        assert_eq!(candidates[0].runtime_type, "unknown");
        assert_eq!(
            candidates[0].compatibility_state,
            CompatibilityState::Incompatible
        );
        assert!(candidates[0]
            .diagnostics
            .iter()
            .any(|diagnostic| { diagnostic.code == DiscoveryDiagnosticCode::ConnectorConflict }));
    }

    #[test]
    fn owned_blocking_workload_returns_near_deadline_and_cleans_up() {
        let _process_fixture_guard = process_fixture_guard();
        let marker = unique_process_marker("timeout");
        assert!(fixture_process_inventory(&marker).is_empty());
        let owned_processes = Arc::new(Mutex::new(Vec::new()));
        let coordinator =
            DiscoveryCoordinator::new(vec![Box::new(OwnedBlockingWorkloadProvider {
                marker: marker.clone(),
                owned_processes: Arc::clone(&owned_processes),
                capture_descendants: false,
            })]);
        let policy = DiscoveryPolicy {
            timeout_ms: 40,
            ..DiscoveryPolicy::default()
        };
        let cancelled = AtomicBool::new(false);
        let started = Instant::now();
        let candidates = coordinator.discover(&policy, &cancelled);

        assert!(candidates.is_empty());
        assert!(started.elapsed() < Duration::from_millis(120));
        assert!(!owned_processes.lock().unwrap().is_empty());
        for process in owned_processes.lock().unwrap().iter() {
            assert_owned_process_tree_reaped(process, &marker);
        }
        assert!(
            wait_for_no_fixture_processes(&marker),
            "owned timeout process must be gone"
        );
    }

    #[test]
    fn external_cancellation_returns_within_bound() {
        let _process_fixture_guard = process_fixture_guard();
        let marker = unique_process_marker("cancel");
        assert!(fixture_process_inventory(&marker).is_empty());
        let owned_processes = Arc::new(Mutex::new(Vec::new()));
        let coordinator =
            DiscoveryCoordinator::new(vec![Box::new(OwnedBlockingWorkloadProvider {
                marker: marker.clone(),
                owned_processes: Arc::clone(&owned_processes),
                capture_descendants: false,
            })]);
        let policy = DiscoveryPolicy {
            timeout_ms: 1_000,
            ..DiscoveryPolicy::default()
        };
        let cancelled = Arc::new(AtomicBool::new(false));
        let cancel_signal = Arc::clone(&cancelled);
        let cancel_thread = thread::spawn(move || {
            thread::sleep(Duration::from_millis(30));
            cancel_signal.store(true, Ordering::Release);
        });

        let started = Instant::now();
        let candidates = coordinator.discover(&policy, &cancelled);

        assert!(candidates.is_empty());
        assert!(started.elapsed() < Duration::from_millis(140));
        cancel_thread.join().expect("join cancel thread");
        assert!(!owned_processes.lock().unwrap().is_empty());
        for process in owned_processes.lock().unwrap().iter() {
            assert_owned_process_tree_reaped(process, &marker);
        }
        assert!(
            wait_for_no_fixture_processes(&marker),
            "owned cancelled process must be gone"
        );
    }

    #[test]
    fn permanent_blocking_provider_repeated_timeouts_do_not_accumulate_workers() {
        let _process_fixture_guard = process_fixture_guard();
        let marker = unique_process_marker("repeat");
        assert!(fixture_process_inventory(&marker).is_empty());
        let owned_processes = Arc::new(Mutex::new(Vec::new()));
        let coordinator =
            DiscoveryCoordinator::new(vec![Box::new(OwnedBlockingWorkloadProvider {
                marker: marker.clone(),
                owned_processes: Arc::clone(&owned_processes),
                capture_descendants: false,
            })]);
        let policy = DiscoveryPolicy {
            timeout_ms: 35,
            ..DiscoveryPolicy::default()
        };
        let cancelled = AtomicBool::new(false);
        for _ in 0..5 {
            let started = Instant::now();
            let report = coordinator.discover_report(&policy, &cancelled);
            assert!(report.candidates.is_empty());
            assert!(started.elapsed() < Duration::from_millis(160));
            assert!(
                wait_for_no_fixture_processes(&marker),
                "owned process from repeated timeout must be gone"
            );
        }
        drop(coordinator);
        assert!(owned_processes.lock().unwrap().len() >= 5);
        for process in owned_processes.lock().unwrap().iter() {
            assert_owned_process_tree_reaped(process, &marker);
        }
        assert!(wait_for_no_fixture_processes(&marker));
    }

    #[test]
    fn owned_blocking_workload_records_process_tree_and_reaps_it() {
        let _process_fixture_guard = process_fixture_guard();
        let marker = unique_process_marker("tree");
        assert!(fixture_process_inventory(&marker).is_empty());
        let owned_processes = Arc::new(Mutex::new(Vec::new()));
        let coordinator =
            DiscoveryCoordinator::new(vec![Box::new(OwnedBlockingWorkloadProvider {
                marker: marker.clone(),
                owned_processes: Arc::clone(&owned_processes),
                capture_descendants: true,
            })]);
        let policy = DiscoveryPolicy {
            timeout_ms: 500,
            max_results: 4,
            ..DiscoveryPolicy::default()
        };
        let cancelled = AtomicBool::new(false);

        let report = coordinator.discover_report(&policy, &cancelled);

        assert!(report.candidates.is_empty());
        let records = owned_processes.lock().unwrap().clone();
        assert_eq!(records.len(), 1);
        assert!(records[0].root_pid > 0);
        assert_owned_process_tree_reaped(&records[0], &marker);
        assert!(wait_for_no_fixture_processes(&marker));
    }

    #[test]
    fn non_cooperative_managed_provider_total_timeout_discards_emitted_observations_and_reaps_tree()
    {
        let _process_fixture_guard = process_fixture_guard();
        let marker = unique_process_marker("hardtimeout");
        assert!(fixture_process_inventory(&marker).is_empty());
        let started_processes = Arc::new(Mutex::new(Vec::new()));
        let coordinator =
            DiscoveryCoordinator::new(vec![Box::new(NonCooperativeManagedProvider {
                marker: marker.clone(),
                started_processes: Arc::clone(&started_processes),
                capture_descendants: false,
            })]);
        let policy = DiscoveryPolicy {
            timeout_ms: 40,
            ..DiscoveryPolicy::default()
        };
        let cancelled = AtomicBool::new(false);

        let started = Instant::now();
        let report = coordinator.discover_report(&policy, &cancelled);

        assert!(started.elapsed() < Duration::from_millis(140));
        assert!(report.candidates.is_empty());
        assert!(report
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == DiscoveryDiagnosticCode::ProviderTimeout));
        let records = started_processes.lock().unwrap().clone();
        assert_eq!(records.len(), 1);
        assert_marked_process_tree_reaped(&records[0], &marker);
        assert!(wait_for_no_fixture_processes(&marker));
    }

    #[test]
    fn non_cooperative_managed_provider_external_cancel_returns_and_reaps_tree() {
        let _process_fixture_guard = process_fixture_guard();
        let marker = unique_process_marker("hardcancel");
        assert!(fixture_process_inventory(&marker).is_empty());
        let started_processes = Arc::new(Mutex::new(Vec::new()));
        let coordinator =
            DiscoveryCoordinator::new(vec![Box::new(NonCooperativeManagedProvider {
                marker: marker.clone(),
                started_processes: Arc::clone(&started_processes),
                capture_descendants: false,
            })]);
        let policy = DiscoveryPolicy {
            timeout_ms: 1_000,
            ..DiscoveryPolicy::default()
        };
        let cancelled = Arc::new(AtomicBool::new(false));
        let cancel_signal = Arc::clone(&cancelled);
        let cancel_thread = thread::spawn(move || {
            thread::sleep(Duration::from_millis(30));
            cancel_signal.store(true, Ordering::Release);
        });

        let started = Instant::now();
        let report = coordinator.discover_report(&policy, &cancelled);
        cancel_thread.join().expect("join cancel thread");

        assert!(started.elapsed() < Duration::from_millis(180));
        assert!(report.candidates.is_empty());
        assert!(wait_for_no_fixture_processes(&marker));
        let records = started_processes.lock().unwrap().clone();
        assert_eq!(records.len(), 1);
        assert_marked_process_tree_reaped(&records[0], &marker);
    }

    #[test]
    fn non_cooperative_managed_provider_repeated_timeouts_do_not_accumulate_owned_processes() {
        let _process_fixture_guard = process_fixture_guard();
        let marker = unique_process_marker("hardrepeat");
        assert!(fixture_process_inventory(&marker).is_empty());
        let started_processes = Arc::new(Mutex::new(Vec::new()));
        let coordinator =
            DiscoveryCoordinator::new(vec![Box::new(NonCooperativeManagedProvider {
                marker: marker.clone(),
                started_processes: Arc::clone(&started_processes),
                capture_descendants: false,
            })]);
        let policy = DiscoveryPolicy {
            timeout_ms: 40,
            ..DiscoveryPolicy::default()
        };
        let cancelled = AtomicBool::new(false);

        for _ in 0..5 {
            let started = Instant::now();
            let report = coordinator.discover_report(&policy, &cancelled);
            assert!(started.elapsed() < Duration::from_millis(160));
            assert!(report.candidates.is_empty());
            assert!(wait_for_no_fixture_processes(&marker));
        }

        let records = started_processes.lock().unwrap().clone();
        assert_eq!(records.len(), 5);
        for record in records {
            assert_marked_process_tree_reaped(&record, &marker);
        }
    }

    #[test]
    fn managed_worker_large_stdin_to_permanent_non_reader_times_out_and_reclaims_io_threads() {
        let _process_fixture_guard = process_fixture_guard();
        let marker = unique_process_marker("stdinblock");
        assert!(fixture_process_inventory(&marker).is_empty());
        assert_eq!(active_managed_io_threads_for_tests(), 0);
        let started_processes = Arc::new(Mutex::new(Vec::new()));
        let coordinator =
            DiscoveryCoordinator::new(vec![Box::new(LargeRequestNonReadingManagedProvider {
                marker: marker.clone(),
                started_processes: Arc::clone(&started_processes),
                request_payload_bytes: 48 * 1024,
            })]);
        let policy = DiscoveryPolicy {
            timeout_ms: 40,
            ..DiscoveryPolicy::default()
        };
        let cancelled = AtomicBool::new(false);

        let started = Instant::now();
        let report = coordinator.discover_report(&policy, &cancelled);

        assert!(started.elapsed() < Duration::from_millis(220));
        assert!(report.candidates.is_empty());
        assert!(report
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == DiscoveryDiagnosticCode::ProviderTimeout));
        assert_eq!(started_processes.lock().unwrap().len(), 1);
        assert!(wait_for_no_fixture_processes(&marker));
        assert_eq!(active_managed_io_threads_for_tests(), 0);
    }

    #[test]
    fn managed_worker_large_stdin_external_cancel_reclaims_io_threads() {
        let _process_fixture_guard = process_fixture_guard();
        let marker = unique_process_marker("stdincancel");
        assert!(fixture_process_inventory(&marker).is_empty());
        assert_eq!(active_managed_io_threads_for_tests(), 0);
        let started_processes = Arc::new(Mutex::new(Vec::new()));
        let coordinator =
            DiscoveryCoordinator::new(vec![Box::new(LargeRequestNonReadingManagedProvider {
                marker: marker.clone(),
                started_processes: Arc::clone(&started_processes),
                request_payload_bytes: 48 * 1024,
            })]);
        let policy = DiscoveryPolicy {
            timeout_ms: 1_000,
            ..DiscoveryPolicy::default()
        };
        let cancelled = Arc::new(AtomicBool::new(false));
        let cancel_signal = Arc::clone(&cancelled);
        let cancel_thread = thread::spawn(move || {
            thread::sleep(Duration::from_millis(30));
            cancel_signal.store(true, Ordering::Release);
        });

        let started = Instant::now();
        let report = coordinator.discover_report(&policy, &cancelled);
        cancel_thread.join().expect("join cancel thread");

        assert!(started.elapsed() < Duration::from_millis(220));
        assert!(report.candidates.is_empty());
        assert_eq!(started_processes.lock().unwrap().len(), 1);
        assert!(wait_for_no_fixture_processes(&marker));
        assert_eq!(active_managed_io_threads_for_tests(), 0);
    }

    #[test]
    fn managed_worker_large_stdin_repeated_timeouts_do_not_accumulate_io_threads() {
        let _process_fixture_guard = process_fixture_guard();
        let marker = unique_process_marker("stdinrepeat");
        assert!(fixture_process_inventory(&marker).is_empty());
        assert_eq!(active_managed_io_threads_for_tests(), 0);
        let started_processes = Arc::new(Mutex::new(Vec::new()));
        let coordinator =
            DiscoveryCoordinator::new(vec![Box::new(LargeRequestNonReadingManagedProvider {
                marker: marker.clone(),
                started_processes: Arc::clone(&started_processes),
                request_payload_bytes: 48 * 1024,
            })]);
        let policy = DiscoveryPolicy {
            timeout_ms: 40,
            ..DiscoveryPolicy::default()
        };
        let cancelled = AtomicBool::new(false);

        for _ in 0..5 {
            let started = Instant::now();
            let report = coordinator.discover_report(&policy, &cancelled);
            assert!(started.elapsed() < Duration::from_millis(220));
            assert!(report.candidates.is_empty());
            assert!(wait_for_no_fixture_processes(&marker));
            assert_eq!(active_managed_io_threads_for_tests(), 0);
        }

        assert_eq!(started_processes.lock().unwrap().len(), 5);
    }

    #[test]
    fn managed_worker_partial_stdin_write_then_crash_discards_provider_and_stops_following() {
        let _process_fixture_guard = process_fixture_guard();
        let marker = unique_process_marker("stdincrash");
        let started_processes = Arc::new(Mutex::new(Vec::new()));
        let following_calls = Arc::new(Mutex::new(0usize));
        let coordinator = DiscoveryCoordinator::new(vec![
            Box::new(CrashingNonReadingManagedProvider {
                marker: marker.clone(),
                started_processes: Arc::clone(&started_processes),
                request_payload_bytes: 48 * 1024,
            }),
            Box::new(FixedProvider {
                observations: vec![observation(
                    &["following-after-crash"],
                    "Following",
                    "available",
                    CompatibilityState::Compatible,
                    AuthState::Ready,
                    HealthState::Ready,
                    vec![DiscoveryEvidence::Available],
                )],
                error: None,
                call_count: Arc::clone(&following_calls),
                remaining_budget_seen: Arc::new(Mutex::new(Vec::new())),
            }),
        ]);
        let policy = DiscoveryPolicy {
            timeout_ms: 1_000,
            ..DiscoveryPolicy::default()
        };
        let cancelled = AtomicBool::new(false);

        let report = coordinator.discover_report(&policy, &cancelled);

        assert!(report.candidates.is_empty());
        assert_eq!(*following_calls.lock().unwrap(), 0);
        assert_eq!(started_processes.lock().unwrap().len(), 1);
        assert_eq!(active_managed_io_threads_for_tests(), 0);
    }

    #[test]
    fn expired_deadline_does_not_start_managed_worker() {
        let _process_fixture_guard = process_fixture_guard();
        let marker = unique_process_marker("expired");
        let started_processes = Arc::new(Mutex::new(Vec::new()));
        let coordinator =
            DiscoveryCoordinator::new(vec![Box::new(LargeRequestNonReadingManagedProvider {
                marker: marker.clone(),
                started_processes: Arc::clone(&started_processes),
                request_payload_bytes: 48 * 1024,
            })]);
        let policy = DiscoveryPolicy {
            timeout_ms: 0,
            ..DiscoveryPolicy::default()
        };
        let cancelled = AtomicBool::new(false);

        let report = coordinator.discover_report(&policy, &cancelled);

        assert!(report.candidates.is_empty());
        assert!(started_processes.lock().unwrap().is_empty());
        assert!(fixture_process_inventory(&marker).is_empty());
        assert_eq!(active_managed_io_threads_for_tests(), 0);
    }

    #[test]
    fn non_cooperative_managed_provider_records_real_descendant_and_reaps_tree() {
        let _process_fixture_guard = process_fixture_guard();
        let marker = unique_process_marker("hardtree");
        assert!(fixture_process_inventory(&marker).is_empty());
        let started_processes = Arc::new(Mutex::new(Vec::new()));
        let coordinator =
            DiscoveryCoordinator::new(vec![Box::new(NonCooperativeManagedProvider {
                marker: marker.clone(),
                started_processes: Arc::clone(&started_processes),
                capture_descendants: true,
            })]);
        let policy = DiscoveryPolicy {
            timeout_ms: 500,
            ..DiscoveryPolicy::default()
        };
        let cancelled = AtomicBool::new(false);

        let report = coordinator.discover_report(&policy, &cancelled);

        assert!(report.candidates.is_empty());
        let records = started_processes.lock().unwrap().clone();
        assert_eq!(records.len(), 1);
        assert!(!records[0].descendant_pids.is_empty());
        assert_marked_process_tree_reaped(&records[0], &marker);
        assert!(wait_for_no_fixture_processes(&marker));
    }

    #[test]
    fn provider_after_total_timeout_is_not_started() {
        let _process_fixture_guard = process_fixture_guard();
        let marker = unique_process_marker("hardstop");
        assert!(fixture_process_inventory(&marker).is_empty());
        let started_processes = Arc::new(Mutex::new(Vec::new()));
        let following_calls = Arc::new(Mutex::new(0usize));
        let coordinator = DiscoveryCoordinator::new(vec![
            Box::new(NonCooperativeManagedProvider {
                marker: marker.clone(),
                started_processes: Arc::clone(&started_processes),
                capture_descendants: false,
            }),
            Box::new(FixedProvider {
                observations: vec![observation(
                    &["following"],
                    "Following",
                    "available",
                    CompatibilityState::Compatible,
                    AuthState::Ready,
                    HealthState::Ready,
                    vec![DiscoveryEvidence::Available],
                )],
                error: None,
                call_count: Arc::clone(&following_calls),
                remaining_budget_seen: Arc::new(Mutex::new(Vec::new())),
            }),
        ]);
        let policy = DiscoveryPolicy {
            timeout_ms: 40,
            ..DiscoveryPolicy::default()
        };
        let cancelled = AtomicBool::new(false);

        let report = coordinator.discover_report(&policy, &cancelled);

        assert!(report.candidates.is_empty());
        assert_eq!(*following_calls.lock().unwrap(), 0);
        assert!(report
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == DiscoveryDiagnosticCode::ProviderTimeout));
        assert!(wait_for_no_fixture_processes(&marker));
    }

    #[test]
    fn unmanaged_provider_without_worker_is_rejected_without_synchronous_blocking() {
        let coordinator = DiscoveryCoordinator::new(vec![Box::new(UnmanagedBlockingProvider)]);
        let policy = DiscoveryPolicy {
            timeout_ms: 40,
            ..DiscoveryPolicy::default()
        };
        let cancelled = AtomicBool::new(false);

        let started = Instant::now();
        let report = coordinator.discover_report(&policy, &cancelled);

        assert!(
            started.elapsed() < Duration::from_millis(140),
            "unmanaged provider must not synchronously block the coordinator"
        );
        assert!(report.candidates.is_empty());
        assert!(report
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == DiscoveryDiagnosticCode::ProviderFailed));
    }

    #[test]
    fn managed_provider_success_requires_worker_protocol_result_not_parent_prefab_observations() {
        let coordinator = DiscoveryCoordinator::new(vec![Box::new(PrefabSuccessManagedProvider)]);
        let policy = DiscoveryPolicy {
            timeout_ms: 1_000,
            ..DiscoveryPolicy::default()
        };
        let cancelled = AtomicBool::new(false);

        let report = coordinator.discover_report(&policy, &cancelled);

        assert!(report.candidates.is_empty());
        assert!(report
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == DiscoveryDiagnosticCode::ProviderFailed));
    }

    #[test]
    fn managed_provider_protocol_error_stops_following_provider() {
        let following_calls = Arc::new(Mutex::new(0usize));
        let coordinator = DiscoveryCoordinator::new(vec![
            Box::new(PrefabSuccessManagedProvider),
            Box::new(FixedProvider {
                observations: vec![observation(
                    &["following-after-protocol-error"],
                    "Following",
                    "available",
                    CompatibilityState::Compatible,
                    AuthState::Ready,
                    HealthState::Ready,
                    vec![DiscoveryEvidence::Available],
                )],
                error: None,
                call_count: Arc::clone(&following_calls),
                remaining_budget_seen: Arc::new(Mutex::new(Vec::new())),
            }),
        ]);
        let policy = DiscoveryPolicy {
            timeout_ms: 1_000,
            ..DiscoveryPolicy::default()
        };
        let cancelled = AtomicBool::new(false);

        let report = coordinator.discover_report(&policy, &cancelled);

        assert!(report.candidates.is_empty());
        assert_eq!(*following_calls.lock().unwrap(), 0);
        assert!(report
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == DiscoveryDiagnosticCode::ProviderFailed));
    }

    #[cfg(windows)]
    #[test]
    fn managed_worker_inherits_only_standard_pipe_handles() {
        let _handle_probe_fixture_guard = handle_probe_fixture_guard();
        let _process_fixture_guard = process_fixture_guard();
        let root = std::env::temp_dir().join(unique_process_marker("handleprobe"));
        std::fs::create_dir_all(&root).unwrap();
        let sentinel_path = root.join("sentinel.txt");
        let sentinel = create_inheritable_sentinel_file(&sentinel_path);
        let sentinel_handle = {
            use std::os::windows::io::AsRawHandle;
            sentinel.as_raw_handle() as usize
        };
        let probe = Arc::new(handle_inheritance_probe_executable_for_tests());
        let probe_root = probe.root().to_owned();
        let coordinator =
            DiscoveryCoordinator::new(vec![Box::new(HandleInheritanceProbeManagedProvider {
                sentinel_handle,
                probe,
            })]);
        let policy = DiscoveryPolicy {
            timeout_ms: 1_000,
            ..DiscoveryPolicy::default()
        };
        let cancelled = AtomicBool::new(false);

        let report = coordinator.discover_report(&policy, &cancelled);

        assert!(report.candidates.is_empty());
        assert!(
            report.diagnostics.is_empty(),
            "sentinel handle must not be inherited while stdio pipes remain usable: {:?}",
            report.diagnostics
        );
        drop(sentinel);
        let _ = std::fs::remove_dir_all(root);
        drop(coordinator);
        assert!(
            !probe_root.exists(),
            "handle probe fixture directory must be removed after provider drops"
        );
    }

    #[cfg(windows)]
    #[test]
    fn handle_probe_fixture_is_test_only_and_mechanical() {
        let _handle_probe_fixture_guard = handle_probe_fixture_guard();
        let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        assert!(
            manifest_dir
                .join("tests")
                .join("fixtures")
                .join("handle_inheritance_probe.rs")
                .is_file(),
            "handle inheritance probe must live under integration-test fixtures"
        );
        assert!(
            !manifest_dir
                .join("src")
                .join("bin")
                .join("agenttalk-handle-inheritance-probe.rs")
                .exists(),
            "handle inheritance probe must not be a default Cargo binary target"
        );
        let fixture = handle_inheritance_probe_executable_for_tests();
        assert!(fixture.executable().is_file());
        let root = fixture.root().to_owned();
        drop(fixture);
        assert!(
            !root.exists(),
            "test-only handle probe fixture directory should be cleaned on drop"
        );
    }

    #[cfg(windows)]
    #[test]
    fn handle_probe_fixture_normal_run_removes_owned_directory() {
        let _handle_probe_fixture_guard = handle_probe_fixture_guard();
        let baseline = handle_probe_fixture_temp_dirs();
        let root = {
            let fixture = Arc::new(handle_inheritance_probe_executable_for_tests());
            let root = fixture.root().to_owned();
            let sentinel_root =
                std::env::temp_dir().join(unique_process_marker("handleprobe-normal"));
            std::fs::create_dir_all(&sentinel_root).unwrap();
            let sentinel_path = sentinel_root.join("sentinel.txt");
            let sentinel = create_inheritable_sentinel_file(&sentinel_path);
            let sentinel_handle = {
                use std::os::windows::io::AsRawHandle;
                sentinel.as_raw_handle() as usize
            };
            let coordinator =
                DiscoveryCoordinator::new(vec![Box::new(HandleInheritanceProbeManagedProvider {
                    sentinel_handle,
                    probe: Arc::clone(&fixture),
                })]);
            let cancelled = AtomicBool::new(false);
            let report = coordinator.discover_report(&DiscoveryPolicy::default(), &cancelled);
            assert!(
                report.diagnostics.is_empty(),
                "handle probe fixture should run without diagnostics: {:?}",
                report.diagnostics
            );
            drop(coordinator);
            drop(sentinel);
            let _ = std::fs::remove_dir_all(sentinel_root);
            root
        };
        assert_owned_handle_probe_fixture_dir_removed(&baseline, &root);
    }

    #[cfg(windows)]
    #[test]
    fn handle_probe_fixture_failure_path_removes_owned_directory() {
        let _handle_probe_fixture_guard = handle_probe_fixture_guard();
        let baseline = handle_probe_fixture_temp_dirs();
        let owned_root = Arc::new(Mutex::new(None::<PathBuf>));
        let root_for_unwind = Arc::clone(&owned_root);
        let result = std::panic::catch_unwind(move || {
            let fixture = handle_inheritance_probe_executable_for_tests();
            *root_for_unwind.lock().unwrap() = Some(fixture.root().to_owned());
            panic!("intentional handle probe fixture unwind");
        });
        assert!(result.is_err());
        let root = owned_root
            .lock()
            .unwrap()
            .clone()
            .expect("fixture root recorded before unwind");
        assert_owned_handle_probe_fixture_dir_removed(&baseline, &root);
    }

    #[cfg(windows)]
    #[test]
    fn handle_probe_fixture_uses_unique_directory_per_owner() {
        let _handle_probe_fixture_guard = handle_probe_fixture_guard();
        let baseline = handle_probe_fixture_temp_dirs();
        let first = handle_inheritance_probe_executable_for_tests();
        let second = handle_inheritance_probe_executable_for_tests();
        assert_ne!(first.root(), second.root());
        assert!(first.root().starts_with(std::env::temp_dir()));
        assert!(second.root().starts_with(std::env::temp_dir()));
        assert!(first
            .root()
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| {
                name.starts_with(&format!(
                    "agenttalk-handle-probe-fixture-{}-",
                    std::process::id()
                ))
            }));
        let first_root = first.root().to_owned();
        let second_root = second.root().to_owned();
        drop(first);
        drop(second);
        assert_owned_handle_probe_fixture_dir_removed(&baseline, &first_root);
        assert!(
            !second_root.exists(),
            "second owned handle probe fixture directory remains: {}",
            second_root.display()
        );
    }

    #[cfg(windows)]
    #[test]
    fn repeated_handle_probe_tests_leave_zero_new_temp_directories() {
        let _handle_probe_fixture_guard = handle_probe_fixture_guard();
        let baseline = handle_probe_fixture_temp_dirs();
        for _ in 0..3 {
            let root = {
                let fixture = handle_inheritance_probe_executable_for_tests();
                assert!(fixture.executable().is_file());
                fixture.root().to_owned()
            };
            assert!(
                !root.exists(),
                "repeated handle probe fixture directory remains: {}",
                root.display()
            );
            assert_no_new_handle_probe_fixture_dirs(&baseline);
        }
    }

    #[cfg(windows)]
    #[test]
    fn concurrent_handle_probe_fixtures_do_not_share_directory() {
        let _handle_probe_fixture_guard = handle_probe_fixture_guard();
        let baseline = handle_probe_fixture_temp_dirs();
        let handles = (0..4)
            .map(|_| {
                thread::spawn(|| {
                    let fixture = handle_inheritance_probe_executable_for_tests();
                    assert!(fixture.executable().is_file());
                    let root = fixture.root().to_owned();
                    drop(fixture);
                    root
                })
            })
            .collect::<Vec<_>>();
        let roots = handles
            .into_iter()
            .map(|handle| handle.join().expect("join handle probe fixture thread"))
            .collect::<BTreeSet<_>>();
        assert_eq!(roots.len(), 4);
        for root in &roots {
            assert!(
                !root.exists(),
                "concurrent handle probe fixture directory remains: {}",
                root.display()
            );
        }
        assert_no_new_handle_probe_fixture_dirs(&baseline);
    }

    #[cfg(windows)]
    #[test]
    fn managed_worker_attribute_list_failure_does_not_start_process_or_io_threads() {
        let _process_fixture_guard = process_fixture_guard();
        let marker = unique_process_marker("attrfail");
        assert!(fixture_process_inventory(&marker).is_empty());
        assert_eq!(active_managed_io_threads_for_tests(), 0);
        let started_processes = Arc::new(Mutex::new(Vec::new()));
        let coordinator =
            DiscoveryCoordinator::new(vec![Box::new(AttributeListFailureManagedProvider {
                marker: marker.clone(),
                started_processes: Arc::clone(&started_processes),
            })]);
        let policy = DiscoveryPolicy {
            timeout_ms: 1_000,
            ..DiscoveryPolicy::default()
        };
        let cancelled = AtomicBool::new(false);

        let report = coordinator.discover_report(&policy, &cancelled);

        assert!(report.candidates.is_empty());
        assert!(report
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == DiscoveryDiagnosticCode::ProviderFailed));
        assert!(started_processes.lock().unwrap().is_empty());
        assert!(fixture_process_inventory(&marker).is_empty());
        assert_eq!(active_managed_io_threads_for_tests(), 0);
    }

    #[test]
    fn worker_request_rejects_unknown_fields() {
        let payload = format!(
            r#"{{"version":1,"protocolIdentity":"{WORKER_PROTOCOL_ID}","buildIdentity":"{WORKER_BUILD_ID}","kind":"codex","timeoutMs":40,"maxResults":1,"allowActiveVerification":false,"payload":{{}},"extra":true}}"#
        );
        let frame = worker_frame(payload.as_bytes());

        assert_eq!(
            read_worker_request_frame(Cursor::new(frame)),
            Err(DiscoveryDiagnosticCode::ProviderFailed)
        );
    }

    #[test]
    fn production_worker_rejects_unknown_handle_probe_kind() {
        let payload = format!(
            r#"{{"version":1,"protocolIdentity":"{WORKER_PROTOCOL_ID}","buildIdentity":"{WORKER_BUILD_ID}","kind":"handle_probe","timeoutMs":40,"maxResults":1,"allowActiveVerification":false,"payload":{{}}}}"#
        );
        let frame = worker_frame(payload.as_bytes());

        assert_eq!(
            read_worker_request_frame(Cursor::new(frame)),
            Err(DiscoveryDiagnosticCode::ProviderFailed)
        );
    }

    #[test]
    fn worker_response_rejects_oversized_truncated_non_utf8_and_duplicate_terminal() {
        let oversized = format!("{WORKER_FRAME_MAGIC} {}\n", MAX_WORKER_RESPONSE_BYTES + 1);
        assert_eq!(
            read_worker_response_frame(oversized.as_bytes()),
            Err(DiscoveryDiagnosticCode::OversizedInput)
        );

        let truncated = format!("{WORKER_FRAME_MAGIC} 64\n{{");
        assert_eq!(
            read_worker_response_frame(truncated.as_bytes()),
            Err(DiscoveryDiagnosticCode::ShortRead)
        );

        let mut invalid_utf8_json = format!(
            r#"{{"version":1,"protocolIdentity":"{WORKER_PROTOCOL_ID}","buildIdentity":"{WORKER_BUILD_ID}","observations":[],"diagnostics":[{{"sourceKind":"runtime_record","code":"provider_failed","bad":""#
        )
        .into_bytes();
        invalid_utf8_json.push(0xff);
        invalid_utf8_json.extend_from_slice(br#""}]}"#);
        let invalid_utf8 = worker_frame(&invalid_utf8_json);
        assert_eq!(
            read_worker_response_frame(&invalid_utf8),
            Err(DiscoveryDiagnosticCode::ProviderFailed)
        );

        let one = worker_frame(
            serde_json::to_string(&ManagedProviderOutcome::success(Vec::new()))
                .unwrap()
                .as_bytes(),
        );
        let mut duplicate_terminal = one.clone();
        duplicate_terminal.extend_from_slice(&one);
        assert_eq!(
            read_worker_response_frame(&duplicate_terminal),
            Err(DiscoveryDiagnosticCode::ProviderFailed)
        );
    }

    #[test]
    fn worker_response_rejects_protocol_or_build_identity_mismatch() {
        let mut wrong_protocol = ManagedProviderOutcome::success(Vec::new());
        wrong_protocol.protocol_identity = "agenttalk.local-discovery-worker.v0".into();
        let mut wrong_build = ManagedProviderOutcome::success(Vec::new());
        wrong_build.build_identity = "agenttalk-runtime-host:0.0.0:other-worker".into();

        for outcome in [wrong_protocol, wrong_build] {
            let payload = serde_json::to_vec(&outcome).unwrap();
            assert_eq!(
                read_worker_response_frame(&worker_frame(&payload)),
                Err(DiscoveryDiagnosticCode::ProviderFailed)
            );
        }
    }

    fn worker_frame(payload: &[u8]) -> Vec<u8> {
        let mut frame = format!("{WORKER_FRAME_MAGIC} {}\n", payload.len()).into_bytes();
        frame.extend_from_slice(payload);
        frame
    }

    #[cfg(windows)]
    fn create_inheritable_sentinel_file(path: &Path) -> std::fs::File {
        use std::os::windows::io::FromRawHandle;
        use windows_sys::Win32::Foundation::{GENERIC_WRITE, INVALID_HANDLE_VALUE};
        use windows_sys::Win32::Security::SECURITY_ATTRIBUTES;
        use windows_sys::Win32::Storage::FileSystem::{
            CreateFileW, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, FILE_SHARE_DELETE, FILE_SHARE_READ,
            FILE_SHARE_WRITE,
        };

        let security = SECURITY_ATTRIBUTES {
            nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: std::ptr::null_mut(),
            bInheritHandle: 1,
        };
        let mut path = wide_null(path.as_os_str());
        let handle = unsafe {
            CreateFileW(
                path.as_mut_ptr(),
                GENERIC_WRITE,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                &security,
                CREATE_ALWAYS,
                FILE_ATTRIBUTE_NORMAL,
                std::ptr::null_mut(),
            )
        };
        assert_ne!(handle, INVALID_HANDLE_VALUE, "create sentinel handle");
        unsafe { std::fs::File::from_raw_handle(handle.cast()) }
    }

    #[test]
    fn provider_failure_returns_typed_renderer_safe_diagnostic() {
        let coordinator = DiscoveryCoordinator::new(vec![Box::new(FixedProvider {
            observations: Vec::new(),
            error: Some(r#"C:\secret\agent.exe Authorization: Bearer token Cookie=x pid=7 port=9"#),
            call_count: Arc::new(Mutex::new(0)),
            remaining_budget_seen: Arc::new(Mutex::new(Vec::new())),
        })]);
        let policy = DiscoveryPolicy::default();
        let cancelled = AtomicBool::new(false);
        let report = coordinator.discover_report(&policy, &cancelled);
        let json = serde_json::to_string(&report.diagnostics).unwrap();

        assert!(report.candidates.is_empty());
        assert_eq!(report.diagnostics.len(), 1);
        assert_eq!(
            report.diagnostics[0].code,
            DiscoveryDiagnosticCode::ProviderFailed
        );
        for forbidden in ["secret", "Authorization", "Bearer", "Cookie", "pid", "port"] {
            assert!(!json.contains(forbidden));
        }
    }

    #[test]
    fn renderer_visible_string_fields_are_projected_through_allowlists() {
        let mut observation = observation_from_context(ObservationFixtureContext {
            locator: ObservationLocator::RuntimeRecord {
                runtime_json: PathBuf::from(r"C:\secret\runtime.json"),
            },
            stable_key: vec!["unsafe-visible-fields".into()],
            display_name: "Authorization: Bearer token\r\nCookie=x".into(),
            availability: CandidateAvailability::Available,
            compatibility_state: CompatibilityState::Compatible,
            auth_state: AuthState::Ready,
            health_state: HealthState::Ready,
            evidence_summary: vec![DiscoveryEvidence::RuntimeRecord],
        });
        observation.connector_id = r"C:\secret\connector".into();
        observation.runtime_type = "https://127.0.0.1:9999".into();
        observation.models = vec![
            r"C:\secret\model".into(),
            "https://127.0.0.1/model".into(),
            "Authorization:Bearer".into(),
        ];
        observation.catalog_revision = Some("9\r\nCookie=x".into());

        let projection = observation.project();
        let json = serde_json::to_string(&projection).unwrap();

        assert_eq!(projection.connector_id, CONFLICT_CONNECTOR_ID);
        assert_eq!(projection.runtime_type, UNKNOWN_RUNTIME_TYPE);
        assert_eq!(projection.display_name, "Local Agent");
        assert!(projection.models.is_empty());
        assert_eq!(projection.catalog_revision, None);
        for forbidden in [
            r"C:\secret",
            "127.0.0.1",
            "Authorization",
            "Bearer",
            "Cookie",
            "token",
        ] {
            assert!(!json.contains(forbidden));
        }
    }

    #[test]
    fn conflicting_catalog_revisions_fail_closed_without_string_max() {
        let mut revision_9 = observation(
            &["catalog"],
            "Catalog",
            "available",
            CompatibilityState::Compatible,
            AuthState::Ready,
            HealthState::Ready,
            vec![
                DiscoveryEvidence::RuntimeRecord,
                DiscoveryEvidence::Available,
            ],
        );
        revision_9.verification_authority = VerificationAuthority::Authoritative;
        revision_9.catalog_revision = Some("9".into());
        revision_9.models = vec!["model-a".into()];
        let mut revision_10 = revision_9.clone();
        revision_10.catalog_revision = Some("10".into());
        revision_10.models = vec!["model-b".into()];

        let policy = DiscoveryPolicy::default();
        let cancelled = AtomicBool::new(false);
        let candidates = DiscoveryCoordinator::new(vec![Box::new(FixedProvider {
            observations: vec![revision_9, revision_10],
            error: None,
            call_count: Arc::new(Mutex::new(0)),
            remaining_budget_seen: Arc::new(Mutex::new(Vec::new())),
        })])
        .discover(&policy, &cancelled);

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].catalog_revision, None);
        assert!(candidates[0].models.is_empty());
        assert!(candidates[0]
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == DiscoveryDiagnosticCode::CatalogConflict));
    }

    #[test]
    fn category_and_discovery_state_conflicts_are_order_independent_fail_closed() {
        let mut agent = observation(
            &["category-conflict"],
            "Agent",
            "available",
            CompatibilityState::Compatible,
            AuthState::Ready,
            HealthState::Ready,
            vec![DiscoveryEvidence::Available],
        );
        agent.category = CandidateCategory::AgentRuntime;
        agent.discovery_state = DiscoveryState::Identified;
        agent.verification_authority = VerificationAuthority::Authoritative;
        let mut model = agent.clone();
        model.category = CandidateCategory::ModelRuntime;
        model.discovery_state = DiscoveryState::Observed;

        let policy = DiscoveryPolicy::default();
        let cancelled = AtomicBool::new(false);
        let forward = DiscoveryCoordinator::new(vec![Box::new(FixedProvider {
            observations: vec![agent.clone(), model.clone()],
            error: None,
            call_count: Arc::new(Mutex::new(0)),
            remaining_budget_seen: Arc::new(Mutex::new(Vec::new())),
        })])
        .discover(&policy, &cancelled);
        let reverse = DiscoveryCoordinator::new(vec![Box::new(FixedProvider {
            observations: vec![model, agent],
            error: None,
            call_count: Arc::new(Mutex::new(0)),
            remaining_budget_seen: Arc::new(Mutex::new(Vec::new())),
        })])
        .discover(&policy, &cancelled);

        assert_eq!(forward, reverse);
        assert_eq!(forward[0].category, CandidateCategory::Unknown);
        assert_eq!(forward[0].discovery_state, DiscoveryState::Observed);
        assert_eq!(forward[0].availability, CandidateAvailability::Unavailable);
        assert!(forward[0].requires_configuration);
        assert!(forward[0]
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == DiscoveryDiagnosticCode::CategoryConflict));
        assert!(forward[0].diagnostics.iter().any(|diagnostic| {
            diagnostic.code == DiscoveryDiagnosticCode::DiscoveryStateConflict
        }));
    }

    #[test]
    fn state_dimensions_keep_independent_authority_and_untrusted_blocks_first_party() {
        let mut untrusted = observation(
            &["trust-cross"],
            "Untrusted",
            "unavailable",
            CompatibilityState::Incompatible,
            AuthState::Required,
            HealthState::IdentityMismatch,
            vec![DiscoveryEvidence::IdentityMismatch],
        );
        untrusted.trust_level = ObservationTrustLevel::Untrusted;
        untrusted.verification_authority = VerificationAuthority::Authoritative;

        let mut first_party = observation(
            &["trust-cross"],
            "FirstParty",
            "available",
            CompatibilityState::Compatible,
            AuthState::Ready,
            HealthState::Ready,
            vec![DiscoveryEvidence::Available],
        );
        first_party.trust_level = ObservationTrustLevel::FirstParty;
        first_party.verification_authority = VerificationAuthority::Heuristic;

        let policy = DiscoveryPolicy::default();
        let cancelled = AtomicBool::new(false);
        let forward = DiscoveryCoordinator::new(vec![Box::new(FixedProvider {
            observations: vec![untrusted.clone(), first_party.clone()],
            error: None,
            call_count: Arc::new(Mutex::new(0)),
            remaining_budget_seen: Arc::new(Mutex::new(Vec::new())),
        })])
        .discover(&policy, &cancelled);
        let reverse = DiscoveryCoordinator::new(vec![Box::new(FixedProvider {
            observations: vec![first_party, untrusted],
            error: None,
            call_count: Arc::new(Mutex::new(0)),
            remaining_budget_seen: Arc::new(Mutex::new(Vec::new())),
        })])
        .discover(&policy, &cancelled);

        assert_eq!(forward, reverse);
        let candidate = &forward[0];
        assert_eq!(candidate.trust_level, ObservationTrustLevel::Untrusted);
        assert_eq!(
            candidate.availability_authority,
            VerificationAuthority::Authoritative
        );
        assert_eq!(
            candidate.compatibility_authority,
            VerificationAuthority::Authoritative
        );
        assert_eq!(
            candidate.auth_authority,
            VerificationAuthority::Authoritative
        );
        assert_eq!(
            candidate.health_authority,
            VerificationAuthority::Authoritative
        );
        assert_eq!(candidate.availability, CandidateAvailability::Unavailable);
    }

    #[test]
    fn untrusted_available_ready_candidate_remains_not_importable_in_both_orders() {
        let mut untrusted = observation(
            &["untrusted-ready"],
            "Untrusted Ready",
            "available",
            CompatibilityState::Compatible,
            AuthState::Ready,
            HealthState::Ready,
            vec![DiscoveryEvidence::Available],
        );
        untrusted.trust_level = ObservationTrustLevel::Untrusted;
        untrusted.verification_authority = VerificationAuthority::Authoritative;
        untrusted.availability_authority = VerificationAuthority::Authoritative;
        untrusted.compatibility_authority = VerificationAuthority::Authoritative;
        untrusted.auth_authority = VerificationAuthority::Authoritative;
        untrusted.health_authority = VerificationAuthority::Authoritative;

        let mut heuristic = untrusted.clone();
        heuristic.trust_level = ObservationTrustLevel::Heuristic;
        heuristic.verification_authority = VerificationAuthority::Heuristic;
        heuristic.availability_authority = VerificationAuthority::Heuristic;
        heuristic.compatibility_authority = VerificationAuthority::Heuristic;
        heuristic.auth_authority = VerificationAuthority::Heuristic;
        heuristic.health_authority = VerificationAuthority::Heuristic;

        let policy = DiscoveryPolicy::default();
        let cancelled = AtomicBool::new(false);
        for observations in [
            vec![untrusted.clone(), heuristic.clone()],
            vec![heuristic, untrusted],
        ] {
            let candidates = DiscoveryCoordinator::new(vec![Box::new(FixedProvider {
                observations,
                error: None,
                call_count: Arc::new(Mutex::new(0)),
                remaining_budget_seen: Arc::new(Mutex::new(Vec::new())),
            })])
            .discover(&policy, &cancelled);
            assert_eq!(candidates[0].trust_level, ObservationTrustLevel::Untrusted);
            assert_eq!(
                candidates[0].availability,
                CandidateAvailability::Unavailable
            );
            assert!(candidates[0].requires_configuration);
        }
    }

    #[test]
    fn each_state_dimension_uses_its_own_authority() {
        let mut base = observation(
            &["per-dimension-authority"],
            "Per Dimension",
            "available",
            CompatibilityState::Incompatible,
            AuthState::Required,
            HealthState::Ready,
            vec![DiscoveryEvidence::RuntimeRecord],
        );
        base.availability_authority = VerificationAuthority::Heuristic;
        base.compatibility_authority = VerificationAuthority::Authoritative;
        base.auth_authority = VerificationAuthority::Authoritative;
        base.health_authority = VerificationAuthority::Heuristic;

        let mut other = base.clone();
        other.availability = CandidateAvailability::Unavailable;
        other.compatibility_state = CompatibilityState::Compatible;
        other.auth_state = AuthState::Ready;
        other.health_state = HealthState::IdentityMismatch;
        other.availability_authority = VerificationAuthority::Authoritative;
        other.compatibility_authority = VerificationAuthority::Heuristic;
        other.auth_authority = VerificationAuthority::Heuristic;
        other.health_authority = VerificationAuthority::Authoritative;

        let policy = DiscoveryPolicy::default();
        let cancelled = AtomicBool::new(false);
        let forward = DiscoveryCoordinator::new(vec![Box::new(FixedProvider {
            observations: vec![base.clone(), other.clone()],
            error: None,
            call_count: Arc::new(Mutex::new(0)),
            remaining_budget_seen: Arc::new(Mutex::new(Vec::new())),
        })])
        .discover(&policy, &cancelled);
        let reverse = DiscoveryCoordinator::new(vec![Box::new(FixedProvider {
            observations: vec![other, base],
            error: None,
            call_count: Arc::new(Mutex::new(0)),
            remaining_budget_seen: Arc::new(Mutex::new(Vec::new())),
        })])
        .discover(&policy, &cancelled);

        assert_eq!(forward, reverse);
        let candidate = &forward[0];
        assert_eq!(candidate.availability, CandidateAvailability::Unavailable);
        assert_eq!(
            candidate.availability_authority,
            VerificationAuthority::Authoritative
        );
        assert_eq!(
            candidate.compatibility_state,
            CompatibilityState::Incompatible
        );
        assert_eq!(
            candidate.compatibility_authority,
            VerificationAuthority::Authoritative
        );
        assert_eq!(candidate.auth_state, AuthState::Required);
        assert_eq!(
            candidate.auth_authority,
            VerificationAuthority::Authoritative
        );
        assert_eq!(candidate.health_state, HealthState::IdentityMismatch);
        assert_eq!(
            candidate.health_authority,
            VerificationAuthority::Authoritative
        );
    }

    #[test]
    fn repeated_observations_do_not_starve_later_provider_or_same_candidate_conflict() {
        let repeated_x = (0..128)
            .map(|index| {
                observation(
                    &["quota-x"],
                    &format!("X {index:03}"),
                    "unconfigured",
                    CompatibilityState::NotVerified,
                    AuthState::Unknown,
                    HealthState::NotChecked,
                    vec![DiscoveryEvidence::ExecutableInventory],
                )
            })
            .collect::<Vec<_>>();
        let mut conflict_x = observation(
            &["quota-x"],
            "X conflict",
            "unavailable",
            CompatibilityState::Incompatible,
            AuthState::Unknown,
            HealthState::IdentityMismatch,
            vec![DiscoveryEvidence::IdentityMismatch],
        );
        conflict_x.verification_authority = VerificationAuthority::Authoritative;
        let y = observation(
            &["quota-y"],
            "Y",
            "unconfigured",
            CompatibilityState::NotVerified,
            AuthState::Unknown,
            HealthState::NotChecked,
            vec![DiscoveryEvidence::RuntimeRecord],
        );
        let policy = DiscoveryPolicy {
            max_results: 2,
            ..DiscoveryPolicy::default()
        };
        let cancelled = AtomicBool::new(false);
        let candidates = DiscoveryCoordinator::new(vec![
            Box::new(FixedProvider {
                observations: repeated_x,
                error: None,
                call_count: Arc::new(Mutex::new(0)),
                remaining_budget_seen: Arc::new(Mutex::new(Vec::new())),
            }),
            Box::new(FixedProvider {
                observations: vec![y, conflict_x],
                error: None,
                call_count: Arc::new(Mutex::new(0)),
                remaining_budget_seen: Arc::new(Mutex::new(Vec::new())),
            }),
        ])
        .discover(&policy, &cancelled);

        assert_eq!(candidates.len(), 2);
        assert!(candidates
            .iter()
            .any(|candidate| candidate.display_name == "Y"));
        let x = candidates
            .iter()
            .find(|candidate| candidate.display_name.starts_with('X'))
            .unwrap();
        assert_eq!(x.availability, CandidateAvailability::Unavailable);
        assert_eq!(x.health_state, HealthState::IdentityMismatch);
    }

    #[test]
    fn provider_emit_then_error_does_not_publish_available_candidate() {
        let policy = DiscoveryPolicy::default();
        let cancelled = AtomicBool::new(false);
        let report = DiscoveryCoordinator::new(vec![Box::new(EmitThenErrorProvider {
            observations: vec![observation(
                &["emit-error"],
                "Should Not Publish",
                "available",
                CompatibilityState::Compatible,
                AuthState::Ready,
                HealthState::Ready,
                vec![DiscoveryEvidence::Available],
            )],
        })])
        .discover_report(&policy, &cancelled);

        assert!(report.candidates.is_empty());
        assert!(report
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == DiscoveryDiagnosticCode::ProviderFailed));
    }

    #[test]
    fn catalog_conflict_is_absorbing_and_revisionless_models_do_not_merge() {
        let mut rev_a = observation(
            &["catalog-aba"],
            "Catalog A",
            "available",
            CompatibilityState::Compatible,
            AuthState::Ready,
            HealthState::Ready,
            vec![DiscoveryEvidence::Available],
        );
        rev_a.verification_authority = VerificationAuthority::Authoritative;
        rev_a.catalog_revision = Some("A".into());
        rev_a.models = vec!["provider/model-a".into()];
        let mut rev_b = rev_a.clone();
        rev_b.catalog_revision = Some("B".into());
        rev_b.models = vec!["provider/model-b".into()];
        let mut revless = rev_a.clone();
        revless.catalog_revision = None;
        revless.models = vec!["provider/model-revisionless".into()];
        let policy = DiscoveryPolicy::default();
        let cancelled = AtomicBool::new(false);

        for observations in [
            vec![rev_a.clone(), rev_b.clone(), rev_a.clone()],
            vec![rev_a.clone(), rev_b.clone(), rev_b.clone()],
            vec![rev_a.clone(), revless.clone()],
            vec![revless, rev_a],
        ] {
            let candidates = DiscoveryCoordinator::new(vec![Box::new(FixedProvider {
                observations,
                error: None,
                call_count: Arc::new(Mutex::new(0)),
                remaining_budget_seen: Arc::new(Mutex::new(Vec::new())),
            })])
            .discover(&policy, &cancelled);
            assert_eq!(candidates[0].catalog_revision, None);
            assert!(candidates[0].models.is_empty());
            assert!(candidates[0]
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == DiscoveryDiagnosticCode::CatalogConflict));
        }
    }

    #[test]
    fn same_revision_different_model_sets_are_catalog_conflict_in_all_orders() {
        let mut a = observation(
            &["same-revision-catalog"],
            "Catalog",
            "available",
            CompatibilityState::Compatible,
            AuthState::Ready,
            HealthState::Ready,
            vec![DiscoveryEvidence::Available],
        );
        a.verification_authority = VerificationAuthority::Authoritative;
        a.catalog_revision = Some("same".into());
        a.models = vec!["provider/model-a".into()];
        let mut b = a.clone();
        b.models = vec!["provider/model-b".into()];
        let mut c = a.clone();
        c.models = vec!["provider/model-a".into(), "provider/model-c".into()];
        let policy = DiscoveryPolicy::default();
        let cancelled = AtomicBool::new(false);
        for observations in [
            vec![a.clone(), b.clone()],
            vec![b.clone(), a.clone()],
            vec![a.clone(), b.clone(), c.clone()],
            vec![c, b, a],
        ] {
            let candidates = DiscoveryCoordinator::new(vec![Box::new(FixedProvider {
                observations,
                error: None,
                call_count: Arc::new(Mutex::new(0)),
                remaining_budget_seen: Arc::new(Mutex::new(Vec::new())),
            })])
            .discover(&policy, &cancelled);
            assert_eq!(candidates[0].catalog_revision, None);
            assert!(candidates[0].models.is_empty());
            assert!(candidates[0]
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == DiscoveryDiagnosticCode::CatalogConflict));
        }
    }

    #[test]
    fn catalog_conflict_blocks_ready_first_party_candidate_in_both_orders() {
        let mut a = observation(
            &["catalog-ready-conflict"],
            "Catalog Ready",
            "available",
            CompatibilityState::Compatible,
            AuthState::Ready,
            HealthState::Ready,
            vec![DiscoveryEvidence::Available],
        );
        a.trust_level = ObservationTrustLevel::FirstParty;
        a.verification_authority = VerificationAuthority::Authoritative;
        a.catalog_revision = Some("same".into());
        a.models = vec!["provider/model-a".into()];
        let mut b = a.clone();
        b.models = vec!["provider/model-b".into()];
        let policy = DiscoveryPolicy::default();
        let cancelled = AtomicBool::new(false);

        let forward = DiscoveryCoordinator::new(vec![Box::new(FixedProvider {
            observations: vec![a.clone(), b.clone()],
            error: None,
            call_count: Arc::new(Mutex::new(0)),
            remaining_budget_seen: Arc::new(Mutex::new(Vec::new())),
        })])
        .discover(&policy, &cancelled);
        let reverse = DiscoveryCoordinator::new(vec![Box::new(FixedProvider {
            observations: vec![b, a],
            error: None,
            call_count: Arc::new(Mutex::new(0)),
            remaining_budget_seen: Arc::new(Mutex::new(Vec::new())),
        })])
        .discover(&policy, &cancelled);

        assert_eq!(forward, reverse);
        let candidate = &forward[0];
        assert!(candidate.models.is_empty());
        assert_eq!(candidate.catalog_revision, None);
        assert!(candidate.requires_configuration);
        assert_eq!(candidate.availability, CandidateAvailability::Available);
        assert_eq!(
            candidate.compatibility_state,
            CompatibilityState::Compatible
        );
        assert_eq!(candidate.auth_state, AuthState::Ready);
        assert_eq!(candidate.health_state, HealthState::Ready);
        assert!(candidate
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == DiscoveryDiagnosticCode::CatalogConflict));
    }

    #[test]
    fn catalog_incomparable_authority_and_trust_conflicts_are_order_independent() {
        let mut higher_authority = observation(
            &["catalog-incomparable"],
            "Catalog",
            "available",
            CompatibilityState::Compatible,
            AuthState::Ready,
            HealthState::Ready,
            vec![DiscoveryEvidence::Available],
        );
        higher_authority.verification_authority = VerificationAuthority::Authoritative;
        higher_authority.trust_level = ObservationTrustLevel::Heuristic;
        higher_authority.catalog_revision = Some("same".into());
        higher_authority.models = vec!["provider/model-authority".into()];
        let mut higher_trust = higher_authority.clone();
        higher_trust.verification_authority = VerificationAuthority::Heuristic;
        higher_trust.trust_level = ObservationTrustLevel::FirstParty;
        higher_trust.models = vec!["provider/model-trust".into()];
        let policy = DiscoveryPolicy::default();
        let cancelled = AtomicBool::new(false);

        let forward = DiscoveryCoordinator::new(vec![Box::new(FixedProvider {
            observations: vec![higher_authority.clone(), higher_trust.clone()],
            error: None,
            call_count: Arc::new(Mutex::new(0)),
            remaining_budget_seen: Arc::new(Mutex::new(Vec::new())),
        })])
        .discover(&policy, &cancelled);
        let reverse = DiscoveryCoordinator::new(vec![Box::new(FixedProvider {
            observations: vec![higher_trust, higher_authority],
            error: None,
            call_count: Arc::new(Mutex::new(0)),
            remaining_budget_seen: Arc::new(Mutex::new(Vec::new())),
        })])
        .discover(&policy, &cancelled);

        assert_eq!(forward, reverse);
        assert!(forward[0].models.is_empty());
        assert_eq!(forward[0].catalog_revision, None);
        assert!(forward[0]
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == DiscoveryDiagnosticCode::CatalogConflict));
    }

    #[test]
    fn same_revision_catalog_merge_is_order_independent_for_authority_trust_matrix() {
        fn catalog_fixture(
            model: &str,
            authority: VerificationAuthority,
            trust: ObservationTrustLevel,
        ) -> Observation {
            let mut observation = observation(
                &["catalog-matrix"],
                "Catalog Matrix",
                "available",
                CompatibilityState::Compatible,
                AuthState::Ready,
                HealthState::Ready,
                vec![DiscoveryEvidence::Available],
            );
            observation.verification_authority = authority;
            observation.availability_authority = authority;
            observation.compatibility_authority = authority;
            observation.auth_authority = authority;
            observation.health_authority = authority;
            observation.trust_level = trust;
            observation.catalog_revision = Some("same".into());
            observation.models = vec![model.into()];
            observation
        }

        fn projection_for(observations: Vec<Observation>) -> CandidateProjection {
            DiscoveryCoordinator::new(vec![Box::new(FixedProvider {
                observations,
                error: None,
                call_count: Arc::new(Mutex::new(0)),
                remaining_budget_seen: Arc::new(Mutex::new(Vec::new())),
            })])
            .discover(&DiscoveryPolicy::default(), &AtomicBool::new(false))
            .remove(0)
        }

        let authorities = [
            VerificationAuthority::Authoritative,
            VerificationAuthority::Heuristic,
            VerificationAuthority::Unverified,
        ];
        let trusts = [
            ObservationTrustLevel::FirstParty,
            ObservationTrustLevel::Heuristic,
            ObservationTrustLevel::Untrusted,
        ];

        for left_authority in authorities {
            for left_trust in trusts {
                for right_authority in authorities {
                    for right_trust in trusts {
                        let left =
                            catalog_fixture("provider/model-left", left_authority, left_trust);
                        let right =
                            catalog_fixture("provider/model-right", right_authority, right_trust);
                        let forward = projection_for(vec![left.clone(), right.clone()]);
                        let reverse = projection_for(vec![right, left]);

                        assert_eq!(
                            forward, reverse,
                            "catalog merge must be order-independent for {left_authority:?}/{left_trust:?} vs {right_authority:?}/{right_trust:?}"
                        );

                        let left_rank = (authority_rank(left_authority), trust_rank(left_trust));
                        let right_rank = (authority_rank(right_authority), trust_rank(right_trust));
                        let left_dominates = left_rank.0 >= right_rank.0
                            && left_rank.1 >= right_rank.1
                            && (left_rank.0 > right_rank.0 || left_rank.1 > right_rank.1);
                        let right_dominates = right_rank.0 >= left_rank.0
                            && right_rank.1 >= left_rank.1
                            && (right_rank.0 > left_rank.0 || right_rank.1 > left_rank.1);

                        if left_dominates || right_dominates {
                            let expected_model = if left_dominates {
                                "provider/model-left"
                            } else {
                                "provider/model-right"
                            };
                            assert_eq!(forward.models, vec![expected_model.to_owned()]);
                            assert_eq!(forward.catalog_revision, Some("same".to_owned()));
                            assert!(!forward.diagnostics.iter().any(|diagnostic| {
                                diagnostic.code == DiscoveryDiagnosticCode::CatalogConflict
                            }));
                        } else {
                            assert!(forward.models.is_empty());
                            assert_eq!(forward.catalog_revision, None);
                            assert!(forward.diagnostics.iter().any(|diagnostic| {
                                diagnostic.code == DiscoveryDiagnosticCode::CatalogConflict
                            }));
                            assert!(forward.requires_configuration);
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn low_trust_catalog_models_do_not_pollute_authoritative_snapshot() {
        let mut authoritative = observation(
            &["catalog-low-trust"],
            "Catalog",
            "available",
            CompatibilityState::Compatible,
            AuthState::Ready,
            HealthState::Ready,
            vec![DiscoveryEvidence::Available],
        );
        authoritative.verification_authority = VerificationAuthority::Authoritative;
        authoritative.trust_level = ObservationTrustLevel::FirstParty;
        authoritative.catalog_revision = Some("stable".into());
        authoritative.models = vec!["provider/model-a".into()];

        let mut low_trust = authoritative.clone();
        low_trust.verification_authority = VerificationAuthority::Unverified;
        low_trust.trust_level = ObservationTrustLevel::Untrusted;
        low_trust.models = vec!["provider/model-a".into(), "provider/model-injected".into()];

        let policy = DiscoveryPolicy::default();
        let cancelled = AtomicBool::new(false);
        for observations in [
            vec![authoritative.clone(), low_trust.clone()],
            vec![low_trust, authoritative],
        ] {
            let candidates = DiscoveryCoordinator::new(vec![Box::new(FixedProvider {
                observations,
                error: None,
                call_count: Arc::new(Mutex::new(0)),
                remaining_budget_seen: Arc::new(Mutex::new(Vec::new())),
            })])
            .discover(&policy, &cancelled);
            assert_eq!(candidates[0].catalog_revision.as_deref(), Some("stable"));
            assert_eq!(candidates[0].models, vec!["provider/model-a".to_owned()]);
            assert_eq!(
                candidates[0].catalog_authority,
                Some(VerificationAuthority::Authoritative)
            );
        }
    }

    #[test]
    fn invalid_identifier_projection_fails_closed_and_unicode_display_name_is_preserved() {
        let mut safe = observation(
            &["unicode-display"],
            "昆 本地运行时",
            "unconfigured",
            CompatibilityState::NotVerified,
            AuthState::Unknown,
            HealthState::NotChecked,
            vec![DiscoveryEvidence::RuntimeRecord],
        );
        safe.models = vec!["provider/model-1".into(), "gpt-4.1-mini".into()];
        let safe_projection = safe.project();
        assert_eq!(safe_projection.display_name, "昆 本地运行时");
        assert_eq!(
            safe_projection.models,
            vec!["gpt-4.1-mini".to_owned(), "provider/model-1".to_owned()]
        );

        let mut invalid = safe.clone();
        invalid.connector_id = "local.connector Authorization Bearer abcdef".into();
        invalid.runtime_type = "kun\u{202e}codex".into();
        invalid.display_name = "Kun\nRuntime".into();
        invalid.availability = CandidateAvailability::Available;
        invalid.compatibility_state = CompatibilityState::Compatible;
        invalid.auth_state = AuthState::Ready;
        invalid.health_state = HealthState::Ready;
        invalid.models = vec![
            "../escape".into(),
            "model/AuthorizationBearerCredential".into(),
            "provider/model-ok".into(),
        ];
        let projection = invalid.project();
        assert_eq!(projection.connector_id, CONFLICT_CONNECTOR_ID);
        assert_eq!(projection.runtime_type, UNKNOWN_RUNTIME_TYPE);
        assert_eq!(projection.availability, CandidateAvailability::Unavailable);
        assert_eq!(
            projection.compatibility_state,
            CompatibilityState::Incompatible
        );
        assert!(projection.requires_configuration);
        assert_eq!(projection.models, vec!["provider/model-ok".to_owned()]);
        assert!(projection
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == DiscoveryDiagnosticCode::InvalidIdentity));
    }

    #[test]
    fn legitimate_product_name_containing_token_is_not_rejected() {
        let mut observation = observation(
            &["token-name"],
            "Token Studio 本地运行时",
            "unconfigured",
            CompatibilityState::NotVerified,
            AuthState::Unknown,
            HealthState::NotChecked,
            vec![DiscoveryEvidence::RuntimeRecord],
        );
        observation.models = vec!["provider/tokenizer-model".into()];
        let projection = observation.project();
        assert_eq!(projection.display_name, "Token Studio 本地运行时");
        assert_eq!(
            projection.models,
            vec!["provider/tokenizer-model".to_owned()]
        );
        assert!(!projection
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == DiscoveryDiagnosticCode::InvalidIdentity));
    }
}
