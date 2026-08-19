mod adapters;
mod discovery;

pub use adapters::acp::{AcpDiscoverySession, AcpFactoryError, AcpProtocolAdapterFactory};
pub use discovery::verifiers::acp::{
    AcpAgentInfo, AcpCapabilitySummary, AcpClassification, AcpClassificationError,
    AcpCompatibilityReport, AcpImportPlanMetadata, AcpPassiveObservation, AcpVerificationConsent,
    AcpVerificationDiagnosticCode, AcpVerificationResult, AcpVerificationStatus,
};

#[cfg(windows)]
pub use discovery::catalog::WindowsAuthenticodeVerifier;
pub use discovery::catalog::{
    authenticode_evidence_to_safe_projection, bundled_production_catalog,
    convert_acp_registry_bytes, default_local_manifest_directory, load_catalog_for_scan,
    load_local_manifest_directory, match_manifest_passively, normalized_catalog_digest,
    refresh_catalog_cache, AuthenticodeEvidence, AuthenticodeStatus, AuthenticodeVerifier,
    CatalogCache, CatalogError, CatalogErrorCode, CatalogLoadReport, CatalogRefreshSource,
    CatalogSnapshot, ConvertedRegistryManifest, ManifestMatchInput, NetworkCounter, RefreshRequest,
    RefreshResponse, RegistryLaunchKind, PRODUCTION_CATALOG_BYTES,
};
pub use discovery::manifest::{
    validate_against_embedded_schema, AdapterManifest, CapabilityRequirement,
    ManifestCapabilityPolicy, ManifestCategory, ManifestLaunch, ManifestMatch, ManifestProtocol,
    ManifestProtocolKind, ManifestSource, ManifestSourceKind, ManifestTransport,
    ManifestValidationError, ManifestValidationErrorCode, ManifestVerification,
    ManifestVerificationKind, ADAPTER_MANIFEST_SCHEMA,
};

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::ffi::OsString;
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{IpAddr, SocketAddr, TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use agenttalk_domain::{
    AuthState, CandidateAvailability, CandidateCategory, CandidateProjection, CompatibilityState,
    DiscoveryDiagnostic, DiscoveryDiagnosticCode, DiscoveryEvidence, DiscoveryPolicy,
    DiscoveryState, HealthState, ObservationSourceKind, ObservationTrustLevel,
    VerificationAuthority, WorkspaceAccess,
};
use agenttalk_events::RuntimeEvent;
use discovery::{
    DiscoveryContext, DiscoveryCoordinator, DiscoveryProvider, DiscoveryProviderError,
    DiscoveryProviderExecution, ManagedProviderOutcome, ManagedProviderProcessSpec,
    ManagedProviderWorkerKind, ManagedProviderWorkerRequest, Observation, ObservationFingerprint,
    ObservationLocator, RunnerInstallationMetadata,
};
use serde::de::{IgnoredAny, MapAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use thiserror::Error;

pub const DEFAULT_RUNTIME_STREAM_CAPACITY: usize = 16;
pub const DEFAULT_RUNTIME_TIMEOUT_MS: u64 = 120_000;
pub const MAX_RUNTIME_TIMEOUT_MS: u64 = 3_600_000;

#[derive(Clone, Debug)]
pub struct RuntimeRequest {
    pub execution_run_id: String,
    pub agent_identity_id: String,
    pub connector_id: String,
    pub model_id: Option<String>,
    pub context_manifest_id: String,
    pub rendered_context: String,
    pub canonical_cwd: Option<String>,
    pub workspace_access: WorkspaceAccess,
    pub timeout_ms: u64,
    pub thread_policy: String,
    /// A deterministic local scope digest, not a cryptographic external signature.
    pub signed_scope: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeCapabilities {
    pub streaming: bool,
    pub cancel: bool,
    pub filesystem: bool,
    pub shell: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeDiscovery {
    pub runtime_id: String,
    pub version: Option<String>,
    pub owned: bool,
}

const LOCAL_DISCOVERY_CODEX_CONNECTOR_ID: &str = "local.codex";
const LOCAL_DISCOVERY_KUN_CONNECTOR_ID: &str = "local.kun.shared-runtime";
const LOCAL_DISCOVERY_REQUEST_TIMEOUT: Duration = Duration::from_secs(2);

/// One read-only local Connector candidate. This intentionally excludes every
/// credential, endpoint authorization detail, process id, and Runtime body.
/// Core turns it into the strict IPC allowlist used by local discovery.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct LocalConnectorCandidate {
    pub connector_id: String,
    pub runtime_type: String,
    pub display_name: String,
    pub availability: String,
    pub models: Vec<String>,
    pub catalog_revision: Option<String>,
    pub source: DiscoverySource,
    pub requires_configuration: bool,
}

#[derive(Clone, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LocalConnectorDiscoveryReport {
    pub candidates: Vec<LocalConnectorCandidate>,
    pub projections: Vec<CandidateProjection>,
    pub diagnostics: Vec<DiscoveryDiagnostic>,
    #[serde(skip)]
    acp_passive_observations: std::collections::BTreeMap<String, Vec<Observation>>,
}

impl std::fmt::Debug for LocalConnectorDiscoveryReport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LocalConnectorDiscoveryReport")
            .field("candidates", &self.candidates)
            .field("projections", &self.projections)
            .field("diagnostics", &self.diagnostics)
            .finish()
    }
}

impl LocalConnectorDiscoveryReport {
    /// Produces Core-owned ACP targets only from passive scan evidence retained
    /// in this report. It never accepts a caller-provided path or locator.
    pub fn classify_acp(
        &self,
        manifests: &[AdapterManifest],
        deadline: Instant,
        cancelled: &AtomicBool,
    ) -> AcpDiscoverySession {
        AcpProtocolAdapterFactory.classify_passive_observations(
            &self.acp_passive_observations,
            manifests,
            deadline,
            cancelled,
        )
    }
}

/// Closed local-discovery source labels. These are renderer-safe display data,
/// not free-form transport text.
pub type DiscoverySource = ObservationSourceKind;

/// Explicit discovery roots make tests isolated and keep production scanning
/// bounded to known Windows locations. No root is recursively traversed.
#[derive(Clone, Debug)]
pub struct LocalConnectorDiscoveryConfig {
    pub codex_binary_paths: Vec<PathBuf>,
    pub kun_data_dirs: Vec<PathBuf>,
    pub kun_install_dirs: Vec<PathBuf>,
    pub kun_expected_service_version: String,
    pub request_timeout: Duration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WindowsPassiveDiscoveryConfig {
    pub path_env: Option<String>,
    pub app_path_records: Vec<WindowsAppPathRecord>,
    pub package_records: Vec<WindowsPackageRecord>,
    pub loopback_records: Vec<WindowsLoopbackListenerRecord>,
    pub loopback_recheck_records: Option<Vec<WindowsLoopbackListenerRecord>>,
    pub explicit_sources: Vec<ExplicitDiscoverySource>,
    pub use_real_app_paths: bool,
    pub use_real_packages: bool,
    pub use_real_loopback: bool,
    pub max_results: usize,
    pub max_path_entries: usize,
    pub max_candidates_per_path_entry: usize,
    pub request_timeout: Duration,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WindowsAppPathRecord {
    pub key_name: String,
    pub executable_path: PathBuf,
    pub hive: WindowsRegistryHive,
    pub view: WindowsRegistryView,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WindowsRegistryHive {
    CurrentUser,
    LocalMachine,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WindowsRegistryView {
    Native,
    Wow6432,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WindowsPackageRecord {
    pub package_family_name: String,
    pub package_full_name: String,
    pub version: String,
    pub installed_location: PathBuf,
    pub executable_relative_path: PathBuf,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WindowsLoopbackListenerRecord {
    pub address: String,
    pub port: u16,
    pub owner_pid: u32,
    pub owner_executable: Option<PathBuf>,
    pub owner_identity: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub enum ExplicitDiscoverySource {
    Executable(PathBuf),
    Endpoint(String),
}

impl Default for WindowsPassiveDiscoveryConfig {
    fn default() -> Self {
        Self {
            path_env: std::env::var_os("PATH").map(|value| value.to_string_lossy().into_owned()),
            app_path_records: Vec::new(),
            package_records: Vec::new(),
            loopback_records: Vec::new(),
            loopback_recheck_records: None,
            explicit_sources: Vec::new(),
            use_real_app_paths: cfg!(windows),
            use_real_packages: cfg!(windows),
            use_real_loopback: cfg!(windows),
            max_results: 32,
            max_path_entries: 256,
            max_candidates_per_path_entry: 64,
            request_timeout: LOCAL_DISCOVERY_REQUEST_TIMEOUT,
        }
    }
}

impl Default for LocalConnectorDiscoveryConfig {
    fn default() -> Self {
        let mut codex_binary_paths = Vec::new();
        let explicit = [
            "AGENTTALK_CODEX_BINARY",
            "CODEX_BINARY_PATH",
            "CODEX_BINARY",
        ]
        .into_iter()
        .find_map(std::env::var_os);
        if let Some(explicit) = explicit {
            // An explicit integration path is authoritative. Do not fall back
            // to a user installation when a test or managed deployment has
            // deliberately supplied an isolated path.
            codex_binary_paths.push(PathBuf::from(explicit));
        } else {
            if let Some(path) = find_codex_on_process_path() {
                codex_binary_paths.push(path);
            }
            #[cfg(windows)]
            if let Some(local_app_data) = std::env::var_os("LOCALAPPDATA") {
                if let Some(path) = find_codex_desktop_binary(&PathBuf::from(local_app_data)) {
                    push_unique_local_path(&mut codex_binary_paths, path);
                }
            }
        }

        let kun_data_dirs = if let Some(data_dir) = std::env::var_os("KUN_DATA_DIR") {
            vec![PathBuf::from(data_dir)]
        } else if let Some(home) =
            std::env::var_os("USERPROFILE").or_else(|| std::env::var_os("HOME"))
        {
            vec![PathBuf::from(home).join(".kun").join("data")]
        } else {
            vec![PathBuf::from(".kun").join("data")]
        };
        let kun_install_dirs = std::env::var_os("KUN_INSTALL_DIR")
            .map(PathBuf::from)
            .into_iter()
            .collect();

        Self {
            codex_binary_paths,
            kun_data_dirs,
            kun_install_dirs,
            kun_expected_service_version: "0.2.34".into(),
            request_timeout: LOCAL_DISCOVERY_REQUEST_TIMEOUT,
        }
    }
}

#[derive(Clone, Debug)]
struct LocalKunRecord {
    runtime_json: PathBuf,
    _port: Option<u16>,
    instance_id: Option<String>,
    service_version: Option<String>,
    build_id: Option<String>,
    parsed: bool,
    diagnostics: Vec<DiscoveryDiagnostic>,
}

/// Discovers the known local desktop Connectors without mutating storage or
/// process state. Codex is identified from an executable only: querying its
/// app-server would launch a child process and is therefore forbidden here.
/// Kun uses its existing read-only authenticated loopback health/catalog path.
pub fn discover_local_connectors() -> Vec<LocalConnectorCandidate> {
    discover_local_connectors_with_config(&LocalConnectorDiscoveryConfig::default())
}

pub fn discover_local_connectors_report() -> LocalConnectorDiscoveryReport {
    discover_local_connectors_report_with_config(&LocalConnectorDiscoveryConfig::default())
}

/// The configuration variant is deliberately public for isolated fixture
/// tests. Production Core calls [`discover_local_connectors`] only.
pub fn discover_local_connectors_with_config(
    config: &LocalConnectorDiscoveryConfig,
) -> Vec<LocalConnectorCandidate> {
    discover_local_connectors_report_with_config(config).candidates
}

/// Public runtime_host API for local discovery diagnostics. This is not exposed
/// over the frozen IPC contract in W1.
pub fn discover_local_connectors_report_with_config(
    config: &LocalConnectorDiscoveryConfig,
) -> LocalConnectorDiscoveryReport {
    let coordinator = DiscoveryCoordinator::new(vec![
        Box::new(CodexDiscoveryProvider {
            config: config.clone(),
        }),
        Box::new(KunDiscoveryProvider {
            config: config.clone(),
        }),
    ]);
    let policy = DiscoveryPolicy {
        max_results: 32,
        timeout_ms: config.request_timeout.as_millis().min(u128::from(u64::MAX)) as u64,
        allow_active_verification: false,
        allow_lan: false,
    };
    let cancelled = std::sync::atomic::AtomicBool::new(false);
    let report = coordinator.discover_report(&policy, &cancelled);
    let projections = report.candidates;
    let mut discoveries = projections
        .clone()
        .into_iter()
        .map(legacy_local_connector_candidate)
        .collect::<Vec<_>>();
    discoveries.sort_by(|left, right| left.connector_id.cmp(&right.connector_id));
    LocalConnectorDiscoveryReport {
        candidates: discoveries,
        projections,
        diagnostics: report.diagnostics,
        acp_passive_observations: report.candidate_observations,
    }
}

pub fn discover_windows_passive_report_with_config(
    config: &WindowsPassiveDiscoveryConfig,
) -> LocalConnectorDiscoveryReport {
    let cancelled = AtomicBool::new(false);
    discover_windows_passive_report_with_config_and_cancelled(config, &cancelled)
}

/// Runs the W2 passive provider set while honoring a Core-owned cancellation
/// token. The token is never serialized into a worker request and carries no
/// caller-provided locator or credential data.
pub fn discover_windows_passive_report_with_config_and_cancelled(
    config: &WindowsPassiveDiscoveryConfig,
    cancelled: &AtomicBool,
) -> LocalConnectorDiscoveryReport {
    let coordinator = DiscoveryCoordinator::new(vec![
        Box::new(WindowsPassiveDiscoveryProvider {
            kind: ManagedProviderWorkerKind::WindowsPath,
            source_kind: ObservationSourceKind::WindowsPath,
            config: config.clone(),
        }),
        Box::new(WindowsPassiveDiscoveryProvider {
            kind: ManagedProviderWorkerKind::WindowsAppPaths,
            source_kind: ObservationSourceKind::WindowsAppPath,
            config: config.clone(),
        }),
        Box::new(WindowsPassiveDiscoveryProvider {
            kind: ManagedProviderWorkerKind::WindowsPackages,
            source_kind: ObservationSourceKind::WindowsPackage,
            config: config.clone(),
        }),
        Box::new(WindowsPassiveDiscoveryProvider {
            kind: ManagedProviderWorkerKind::WindowsRunnerInstallations,
            source_kind: ObservationSourceKind::ExecutableInventory,
            config: config.clone(),
        }),
        Box::new(WindowsPassiveDiscoveryProvider {
            kind: ManagedProviderWorkerKind::WindowsLoopbackListeners,
            source_kind: ObservationSourceKind::LoopbackListener,
            config: config.clone(),
        }),
        Box::new(WindowsPassiveDiscoveryProvider {
            kind: ManagedProviderWorkerKind::ExplicitSources,
            source_kind: ObservationSourceKind::UserSelected,
            config: config.clone(),
        }),
    ]);
    let policy = DiscoveryPolicy {
        max_results: config.max_results,
        timeout_ms: config.request_timeout.as_millis().min(u128::from(u64::MAX)) as u64,
        allow_active_verification: false,
        allow_lan: false,
    };
    let report = coordinator.discover_report(&policy, cancelled);
    let projections = report.candidates;
    let mut candidates = projections
        .clone()
        .into_iter()
        .map(legacy_local_connector_candidate)
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        left.connector_id
            .cmp(&right.connector_id)
            .then_with(|| left.display_name.cmp(&right.display_name))
    });
    LocalConnectorDiscoveryReport {
        candidates,
        projections,
        diagnostics: report.diagnostics,
        acp_passive_observations: report.candidate_observations,
    }
}

pub fn run_local_discovery_worker_from_stdio() -> i32 {
    match run_local_discovery_worker() {
        Ok(()) => 0,
        Err(_) => 2,
    }
}

fn run_local_discovery_worker() -> Result<(), DiscoveryDiagnosticCode> {
    let request = discovery::read_worker_request_frame(std::io::stdin().lock())?;
    let deadline = Instant::now() + Duration::from_millis(request.timeout_ms);
    let cancelled = AtomicBool::new(false);
    let outcome = match request.kind {
        ManagedProviderWorkerKind::Codex => {
            let config: WorkerDiscoveryConfig = serde_json::from_value(request.payload)
                .map_err(|_| DiscoveryDiagnosticCode::ProviderFailed)?;
            let config: LocalConnectorDiscoveryConfig = config.into();
            ManagedProviderOutcome::success(collect_codex_observations(
                &config,
                deadline,
                &cancelled,
                request.max_results,
            ))
        }
        ManagedProviderWorkerKind::Kun => {
            let config: WorkerDiscoveryConfig = serde_json::from_value(request.payload)
                .map_err(|_| DiscoveryDiagnosticCode::ProviderFailed)?;
            let config: LocalConnectorDiscoveryConfig = config.into();
            ManagedProviderOutcome::success(collect_kun_observations(
                &config,
                request.allow_active_verification,
                request.max_results,
            ))
        }
        ManagedProviderWorkerKind::WindowsPath
        | ManagedProviderWorkerKind::WindowsAppPaths
        | ManagedProviderWorkerKind::WindowsPackages
        | ManagedProviderWorkerKind::WindowsRunnerInstallations
        | ManagedProviderWorkerKind::WindowsLoopbackListeners
        | ManagedProviderWorkerKind::ExplicitSources => {
            let config: WindowsPassiveWorkerConfig = serde_json::from_value(request.payload)
                .map_err(|_| DiscoveryDiagnosticCode::ProviderFailed)?;
            collect_windows_passive_worker(
                request.kind,
                config,
                deadline,
                &cancelled,
                request.max_results,
            )
        }
    };
    discovery::write_worker_frame(&mut std::io::stdout().lock(), &outcome, 1024 * 1024)
        .map_err(|_| DiscoveryDiagnosticCode::ProviderFailed)
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WorkerDiscoveryConfig {
    codex_binary_paths: Vec<PathBuf>,
    kun_data_dirs: Vec<PathBuf>,
    kun_install_dirs: Vec<PathBuf>,
    kun_expected_service_version: String,
    request_timeout_ms: u64,
}

impl From<&LocalConnectorDiscoveryConfig> for WorkerDiscoveryConfig {
    fn from(config: &LocalConnectorDiscoveryConfig) -> Self {
        Self {
            codex_binary_paths: config.codex_binary_paths.clone(),
            kun_data_dirs: config.kun_data_dirs.clone(),
            kun_install_dirs: config.kun_install_dirs.clone(),
            kun_expected_service_version: config.kun_expected_service_version.clone(),
            request_timeout_ms: config.request_timeout.as_millis().min(u128::from(u64::MAX)) as u64,
        }
    }
}

impl From<WorkerDiscoveryConfig> for LocalConnectorDiscoveryConfig {
    fn from(config: WorkerDiscoveryConfig) -> Self {
        Self {
            codex_binary_paths: config.codex_binary_paths,
            kun_data_dirs: config.kun_data_dirs,
            kun_install_dirs: config.kun_install_dirs,
            kun_expected_service_version: config.kun_expected_service_version,
            request_timeout: Duration::from_millis(config.request_timeout_ms),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WindowsPassiveWorkerConfig {
    path_env: Option<String>,
    app_path_records: Vec<WindowsAppPathRecord>,
    package_records: Vec<WindowsPackageRecord>,
    loopback_records: Vec<WindowsLoopbackListenerRecord>,
    loopback_recheck_records: Option<Vec<WindowsLoopbackListenerRecord>>,
    explicit_sources: Vec<ExplicitDiscoverySource>,
    use_real_app_paths: bool,
    use_real_packages: bool,
    use_real_loopback: bool,
    max_path_entries: usize,
    max_candidates_per_path_entry: usize,
}

impl From<&WindowsPassiveDiscoveryConfig> for WindowsPassiveWorkerConfig {
    fn from(config: &WindowsPassiveDiscoveryConfig) -> Self {
        Self {
            path_env: config.path_env.clone(),
            app_path_records: config.app_path_records.clone(),
            package_records: config.package_records.clone(),
            loopback_records: config.loopback_records.clone(),
            loopback_recheck_records: config.loopback_recheck_records.clone(),
            explicit_sources: config.explicit_sources.clone(),
            use_real_app_paths: config.use_real_app_paths,
            use_real_packages: config.use_real_packages,
            use_real_loopback: config.use_real_loopback,
            max_path_entries: config.max_path_entries,
            max_candidates_per_path_entry: config.max_candidates_per_path_entry,
        }
    }
}

#[derive(Default)]
struct WindowsProviderCollection {
    observations: Vec<Observation>,
    diagnostics: Vec<DiscoveryDiagnostic>,
}

impl WindowsProviderCollection {
    fn push_diagnostic(
        &mut self,
        source_kind: ObservationSourceKind,
        code: DiscoveryDiagnosticCode,
    ) {
        let diagnostic = DiscoveryDiagnostic { source_kind, code };
        if !self.diagnostics.contains(&diagnostic) {
            self.diagnostics.push(diagnostic);
        }
    }

    fn push_observation(&mut self, observation: Observation, max_observations: usize) -> bool {
        if self.observations.len() >= max_observations {
            return false;
        }
        self.observations.push(observation);
        true
    }
}

fn collect_windows_passive_worker(
    kind: ManagedProviderWorkerKind,
    config: WindowsPassiveWorkerConfig,
    deadline: Instant,
    cancelled: &AtomicBool,
    max_observations: usize,
) -> ManagedProviderOutcome {
    let collection =
        collect_windows_passive_provider(kind, &config, deadline, cancelled, max_observations);
    let mut outcome = ManagedProviderOutcome::success(collection.observations);
    outcome.diagnostics = collection.diagnostics;
    outcome
}

fn collect_windows_passive_provider(
    kind: ManagedProviderWorkerKind,
    config: &WindowsPassiveWorkerConfig,
    deadline: Instant,
    cancelled: &AtomicBool,
    max_observations: usize,
) -> WindowsProviderCollection {
    match kind {
        ManagedProviderWorkerKind::WindowsPath => {
            collect_windows_path_observations(config, deadline, cancelled, max_observations)
        }
        ManagedProviderWorkerKind::WindowsAppPaths => {
            collect_windows_app_path_observations(config, deadline, cancelled, max_observations)
        }
        ManagedProviderWorkerKind::WindowsPackages => {
            collect_windows_package_observations(config, deadline, cancelled, max_observations)
        }
        ManagedProviderWorkerKind::WindowsRunnerInstallations => {
            collect_windows_runner_installations(config, deadline, cancelled, max_observations)
        }
        ManagedProviderWorkerKind::WindowsLoopbackListeners => {
            collect_windows_loopback_observations(config, deadline, cancelled, max_observations)
        }
        ManagedProviderWorkerKind::ExplicitSources => {
            collect_explicit_source_observations(config, deadline, cancelled, max_observations)
        }
        ManagedProviderWorkerKind::Codex | ManagedProviderWorkerKind::Kun => {
            WindowsProviderCollection::default()
        }
    }
}

#[derive(Clone)]
struct CodexDiscoveryProvider {
    config: LocalConnectorDiscoveryConfig,
}

impl DiscoveryProvider for CodexDiscoveryProvider {
    fn source_kind(&self) -> ObservationSourceKind {
        ObservationSourceKind::ExecutableInventory
    }

    fn execution(&self) -> DiscoveryProviderExecution {
        local_discovery_provider_execution()
    }

    fn managed_process(
        &self,
        context: &DiscoveryContext<'_>,
    ) -> Option<ManagedProviderProcessSpec> {
        local_discovery_worker_executable().map(|executable| ManagedProviderProcessSpec {
            executable,
            args: Vec::new(),
            request: local_discovery_worker_request(
                ManagedProviderWorkerKind::Codex,
                &self.config,
                context,
            ),
            #[cfg(test)]
            started_processes: None,
            #[cfg(test)]
            capture_descendants: false,
            #[cfg(test)]
            force_attribute_list_failure: false,
        })
    }

    fn collect(
        &self,
        context: &DiscoveryContext<'_>,
        emit: &mut dyn FnMut(Observation) -> bool,
    ) -> Result<(), DiscoveryProviderError> {
        #[cfg(not(test))]
        {
            let _ = (context, emit);
            Err(DiscoveryProviderError {
                source_kind: ObservationSourceKind::ExecutableInventory,
                code: DiscoveryDiagnosticCode::ProviderFailed,
            })
        }
        #[cfg(test)]
        {
            if context.should_stop() {
                return Ok(());
            }
            if context.remaining_results() == 0 {
                return Ok(());
            }
            let Some(binary) = self
                .config
                .codex_binary_paths
                .iter()
                .find(|path| is_real_regular_file(path))
                .cloned()
            else {
                return Ok(());
            };
            let _ = emit(codex_observation(
                context.deadline,
                context.cancelled,
                &binary,
            ));
            Ok(())
        }
    }
}

#[derive(Clone)]
struct KunDiscoveryProvider {
    config: LocalConnectorDiscoveryConfig,
}

impl DiscoveryProvider for KunDiscoveryProvider {
    fn source_kind(&self) -> ObservationSourceKind {
        ObservationSourceKind::RuntimeRecord
    }

    fn execution(&self) -> DiscoveryProviderExecution {
        local_discovery_provider_execution()
    }

    fn managed_process(
        &self,
        context: &DiscoveryContext<'_>,
    ) -> Option<ManagedProviderProcessSpec> {
        local_discovery_worker_executable().map(|executable| ManagedProviderProcessSpec {
            executable,
            args: Vec::new(),
            request: local_discovery_worker_request(
                ManagedProviderWorkerKind::Kun,
                &self.config,
                context,
            ),
            #[cfg(test)]
            started_processes: None,
            #[cfg(test)]
            capture_descendants: false,
            #[cfg(test)]
            force_attribute_list_failure: false,
        })
    }

    fn collect(
        &self,
        context: &DiscoveryContext<'_>,
        emit: &mut dyn FnMut(Observation) -> bool,
    ) -> Result<(), DiscoveryProviderError> {
        #[cfg(not(test))]
        {
            let _ = (context, emit);
            Err(DiscoveryProviderError {
                source_kind: ObservationSourceKind::RuntimeRecord,
                code: DiscoveryDiagnosticCode::ProviderFailed,
            })
        }
        #[cfg(test)]
        {
            if context.should_stop() {
                return Ok(());
            }
            if context.remaining_results() == 0 {
                return Ok(());
            }
            for data_dir in &self.config.kun_data_dirs {
                if context.should_stop() {
                    break;
                }
                let Some(record) = read_local_kun_record(data_dir) else {
                    continue;
                };
                if !emit(kun_observation(
                    &self.config,
                    record,
                    context._policy.allow_active_verification,
                )) {
                    break;
                }
            }
            Ok(())
        }
    }
}

#[derive(Clone)]
struct WindowsPassiveDiscoveryProvider {
    kind: ManagedProviderWorkerKind,
    source_kind: ObservationSourceKind,
    config: WindowsPassiveDiscoveryConfig,
}

impl DiscoveryProvider for WindowsPassiveDiscoveryProvider {
    fn source_kind(&self) -> ObservationSourceKind {
        self.source_kind
    }

    fn execution(&self) -> DiscoveryProviderExecution {
        local_discovery_provider_execution()
    }

    fn managed_process(
        &self,
        context: &DiscoveryContext<'_>,
    ) -> Option<ManagedProviderProcessSpec> {
        local_discovery_worker_executable().map(|executable| ManagedProviderProcessSpec {
            executable,
            args: Vec::new(),
            request: local_discovery_worker_request_with_payload(
                self.kind.clone(),
                serde_json::to_value(WindowsPassiveWorkerConfig::from(&self.config))
                    .expect("windows passive worker config is serializable"),
                context,
            ),
            #[cfg(test)]
            started_processes: None,
            #[cfg(test)]
            capture_descendants: false,
            #[cfg(test)]
            force_attribute_list_failure: false,
        })
    }

    fn collect(
        &self,
        context: &DiscoveryContext<'_>,
        emit: &mut dyn FnMut(Observation) -> bool,
    ) -> Result<(), DiscoveryProviderError> {
        #[cfg(not(test))]
        {
            let _ = (context, emit);
            Err(DiscoveryProviderError {
                source_kind: self.source_kind,
                code: DiscoveryDiagnosticCode::ProviderFailed,
            })
        }
        #[cfg(test)]
        {
            let collection = collect_windows_passive_provider(
                self.kind.clone(),
                &WindowsPassiveWorkerConfig::from(&self.config),
                context.deadline,
                context.cancelled,
                context.remaining_results(),
            );
            for observation in collection.observations {
                if !emit(observation) {
                    break;
                }
            }
            if collection.diagnostics.is_empty() {
                Ok(())
            } else {
                Err(DiscoveryProviderError {
                    source_kind: self.source_kind,
                    code: collection.diagnostics[0].code,
                })
            }
        }
    }
}

fn local_discovery_worker_request(
    kind: ManagedProviderWorkerKind,
    config: &LocalConnectorDiscoveryConfig,
    context: &DiscoveryContext<'_>,
) -> ManagedProviderWorkerRequest {
    local_discovery_worker_request_with_payload(
        kind,
        serde_json::to_value(WorkerDiscoveryConfig::from(config))
            .expect("worker config contains only serializable values"),
        context,
    )
}

fn local_discovery_worker_request_with_payload(
    kind: ManagedProviderWorkerKind,
    payload: Value,
    context: &DiscoveryContext<'_>,
) -> ManagedProviderWorkerRequest {
    ManagedProviderWorkerRequest::new(
        kind,
        context
            .deadline
            .saturating_duration_since(Instant::now())
            .as_millis()
            .min(u128::from(u64::MAX)) as u64,
        context.remaining_results(),
        context._policy.allow_active_verification,
        payload,
    )
}

fn local_discovery_worker_executable() -> Option<PathBuf> {
    #[cfg(any(test, debug_assertions))]
    if let Some(path) = local_discovery_fixture_worker_executable() {
        return validate_fixture_worker_executable(&path);
    }
    production_local_discovery_worker_executable()
}

fn production_local_discovery_worker_executable() -> Option<PathBuf> {
    let current = std::env::current_exe().ok()?;
    resolve_production_worker_for_current_exe(&current)
}

fn resolve_production_worker_for_current_exe(current_exe: &Path) -> Option<PathBuf> {
    if !current_exe.is_absolute() {
        return None;
    }
    if has_reparse_point(current_exe) {
        return None;
    }
    let current = current_exe.canonicalize().ok()?;
    if !is_real_regular_file(&current) {
        return None;
    }
    let directory = current.parent()?.canonicalize().ok()?;
    let candidate = directory.join(local_discovery_worker_file_name());
    validate_production_worker_candidate(&directory, &candidate)
}

fn validate_production_worker_candidate(directory: &Path, candidate: &Path) -> Option<PathBuf> {
    if !candidate.is_absolute()
        || candidate.file_name()? != std::ffi::OsStr::new(local_discovery_worker_file_name())
        || has_reparse_point(candidate)
    {
        return None;
    }
    let canonical = candidate.canonicalize().ok()?;
    if !is_real_regular_file(&canonical) {
        return None;
    }
    let candidate_directory = canonical.parent()?.canonicalize().ok()?;
    (candidate_directory == directory).then_some(canonical)
}

fn local_discovery_worker_file_name() -> &'static str {
    if cfg!(windows) {
        "agenttalk-local-discovery-worker.exe"
    } else {
        "agenttalk-local-discovery-worker"
    }
}

#[cfg(any(test, debug_assertions))]
fn validate_fixture_worker_executable(path: &Path) -> Option<PathBuf> {
    if !path.is_absolute() || has_reparse_point(path) {
        return None;
    }
    let canonical = path.canonicalize().ok()?;
    is_real_regular_file(&canonical).then_some(canonical)
}

#[cfg(any(test, debug_assertions))]
fn local_discovery_fixture_worker_slot() -> &'static Mutex<Option<PathBuf>> {
    static SLOT: std::sync::OnceLock<Mutex<Option<PathBuf>>> = std::sync::OnceLock::new();
    SLOT.get_or_init(|| Mutex::new(None))
}

#[cfg(any(test, debug_assertions))]
fn local_discovery_fixture_worker_executable() -> Option<PathBuf> {
    local_discovery_fixture_worker_slot()
        .lock()
        .unwrap()
        .clone()
}

#[cfg(any(test, debug_assertions))]
pub struct LocalDiscoveryFixtureWorkerGuard {
    previous: Option<PathBuf>,
}

#[cfg(any(test, debug_assertions))]
impl Drop for LocalDiscoveryFixtureWorkerGuard {
    fn drop(&mut self) {
        *local_discovery_fixture_worker_slot().lock().unwrap() = self.previous.take();
    }
}

#[cfg(any(test, debug_assertions))]
pub fn install_local_discovery_fixture_worker_for_tests(
    path: impl Into<PathBuf>,
) -> Result<LocalDiscoveryFixtureWorkerGuard, &'static str> {
    let path = path.into();
    let canonical = validate_fixture_worker_executable(&path).ok_or("invalid fixture worker")?;
    let previous = {
        let mut slot = local_discovery_fixture_worker_slot().lock().unwrap();
        slot.replace(canonical)
    };
    Ok(LocalDiscoveryFixtureWorkerGuard { previous })
}

#[cfg(windows)]
fn has_reparse_point(path: &Path) -> bool {
    use std::os::windows::fs::MetadataExt;
    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0400;
    std::fs::symlink_metadata(path)
        .map(|metadata| metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0)
        .unwrap_or(false)
}

#[cfg(not(windows))]
fn has_reparse_point(path: &Path) -> bool {
    std::fs::symlink_metadata(path)
        .map(|metadata| metadata.file_type().is_symlink())
        .unwrap_or(false)
}

fn local_discovery_provider_execution() -> DiscoveryProviderExecution {
    if local_discovery_worker_executable().is_some() {
        DiscoveryProviderExecution::ManagedWorkerRequired
    } else {
        #[cfg(test)]
        {
            DiscoveryProviderExecution::InlineAllowedForTests
        }
        #[cfg(not(test))]
        {
            DiscoveryProviderExecution::ManagedWorkerRequired
        }
    }
}

fn collect_codex_observations(
    config: &LocalConnectorDiscoveryConfig,
    deadline: Instant,
    cancelled: &AtomicBool,
    max_results: usize,
) -> Vec<Observation> {
    if max_results == 0 || cancelled.load(Ordering::Acquire) || Instant::now() >= deadline {
        return Vec::new();
    }
    config
        .codex_binary_paths
        .iter()
        .find(|path| is_real_regular_file(path))
        .map(|binary| codex_observation(deadline, cancelled, binary))
        .into_iter()
        .collect()
}

fn collect_kun_observations(
    config: &LocalConnectorDiscoveryConfig,
    allow_active_verification: bool,
    max_results: usize,
) -> Vec<Observation> {
    let mut observations = Vec::new();
    for data_dir in &config.kun_data_dirs {
        if observations.len() >= max_results {
            break;
        }
        let Some(record) = read_local_kun_record(data_dir) else {
            continue;
        };
        observations.push(kun_observation(config, record, allow_active_verification));
    }
    observations
}

const LOCAL_DISCOVERY_UNKNOWN_CONNECTOR_ID: &str = "local.discovery.unknown";
const LOCAL_DISCOVERY_UNKNOWN_RUNTIME_TYPE: &str = "unknown";
const MAX_PACKAGE_MANIFEST_BYTES: usize = 256 * 1024;
const MAX_REAL_APP_PATH_RECORDS: usize = 512;
const MAX_REAL_PACKAGE_RECORDS: usize = 512;
const MAX_REAL_LOOPBACK_RECORDS: usize = 2048;
const MAX_RUNNER_COMMAND_OUTPUT_BYTES: usize = 256 * 1024;
const MAX_RUNNER_INSTALLATIONS: usize = 128;
const MAX_RUNNER_PACKAGE_TREE_FILES: usize = 512;
const MAX_RUNNER_PACKAGE_TREE_BYTES: u64 = 4 * 1024 * 1024;

fn collect_windows_path_observations(
    config: &WindowsPassiveWorkerConfig,
    deadline: Instant,
    cancelled: &AtomicBool,
    max_observations: usize,
) -> WindowsProviderCollection {
    let mut collection = WindowsProviderCollection::default();
    let Some(path_env) = &config.path_env else {
        return collection;
    };
    let mut seen_dirs = BTreeSet::new();
    for directory in std::env::split_paths(path_env).take(config.max_path_entries) {
        if cancelled.load(Ordering::Acquire) || Instant::now() >= deadline {
            break;
        }
        if !directory.is_absolute() {
            collection.push_diagnostic(
                ObservationSourceKind::WindowsPath,
                DiscoveryDiagnosticCode::InvalidSourceRecord,
            );
            continue;
        }
        if has_reparse_point(&directory) {
            collection.push_diagnostic(
                ObservationSourceKind::WindowsPath,
                DiscoveryDiagnosticCode::ReparsePointRejected,
            );
            continue;
        }
        let canonical = match directory.canonicalize() {
            Ok(path) => path,
            Err(error) => {
                collection.push_diagnostic(
                    ObservationSourceKind::WindowsPath,
                    io_diagnostic_code(&error),
                );
                continue;
            }
        };
        if !seen_dirs.insert(normalized_path_key(&canonical)) {
            continue;
        }
        let entries = match fs::read_dir(&canonical) {
            Ok(entries) => entries,
            Err(error) => {
                collection.push_diagnostic(
                    ObservationSourceKind::WindowsPath,
                    io_diagnostic_code(&error),
                );
                continue;
            }
        };
        let mut accepted_in_dir = 0usize;
        for entry in entries {
            if cancelled.load(Ordering::Acquire) || Instant::now() >= deadline {
                return collection;
            }
            if accepted_in_dir >= config.max_candidates_per_path_entry {
                break;
            }
            let entry = match entry {
                Ok(entry) => entry,
                Err(error) => {
                    collection.push_diagnostic(
                        ObservationSourceKind::WindowsPath,
                        io_diagnostic_code(&error),
                    );
                    continue;
                }
            };
            let path = entry.path();
            if has_reparse_point(&path) {
                collection.push_diagnostic(
                    ObservationSourceKind::WindowsPath,
                    DiscoveryDiagnosticCode::ReparsePointRejected,
                );
                continue;
            }
            if !is_windows_executable_file(&path) || !is_real_regular_file(&path) {
                continue;
            }
            match unknown_executable_observation(
                ObservationSourceKind::WindowsPath,
                DiscoveryEvidence::WindowsPathEntry,
                ObservationTrustLevel::Heuristic,
                &path,
                deadline,
                cancelled,
            ) {
                Ok(observation) => {
                    accepted_in_dir += 1;
                    if !collection.push_observation(observation, max_observations) {
                        return collection;
                    }
                }
                Err(code) => {
                    collection.push_diagnostic(ObservationSourceKind::WindowsPath, code);
                }
            }
        }
    }
    collection
}

fn collect_windows_app_path_observations(
    config: &WindowsPassiveWorkerConfig,
    deadline: Instant,
    cancelled: &AtomicBool,
    max_observations: usize,
) -> WindowsProviderCollection {
    let mut collection = WindowsProviderCollection::default();
    let mut records = config.app_path_records.clone();
    if config.use_real_app_paths {
        records.extend(read_windows_app_path_records(&mut collection));
    }
    let mut seen_records = BTreeSet::new();
    for record in records {
        if cancelled.load(Ordering::Acquire) || Instant::now() >= deadline {
            break;
        }
        let key = format!(
            "{:?}:{:?}:{}",
            record.hive,
            record.view,
            normalized_path_key(&record.executable_path)
        );
        if !seen_records.insert(key) {
            continue;
        }
        match unknown_executable_observation(
            ObservationSourceKind::WindowsAppPath,
            DiscoveryEvidence::WindowsAppPathRegistry,
            ObservationTrustLevel::Heuristic,
            &record.executable_path,
            deadline,
            cancelled,
        ) {
            Ok(observation) => {
                if !collection.push_observation(observation, max_observations) {
                    break;
                }
            }
            Err(code) => {
                collection.push_diagnostic(ObservationSourceKind::WindowsAppPath, code);
            }
        }
    }
    collection
}

fn collect_windows_package_observations(
    config: &WindowsPassiveWorkerConfig,
    deadline: Instant,
    cancelled: &AtomicBool,
    max_observations: usize,
) -> WindowsProviderCollection {
    let mut collection = WindowsProviderCollection::default();
    let mut records = config.package_records.clone();
    if config.use_real_packages {
        records.extend(read_windows_package_records(&mut collection));
    }
    for record in records {
        if cancelled.load(Ordering::Acquire) || Instant::now() >= deadline {
            break;
        }
        let executable = match package_executable_path(&record) {
            Ok(path) => path,
            Err(code) => {
                collection.push_diagnostic(ObservationSourceKind::WindowsPackage, code);
                continue;
            }
        };
        match unknown_package_observation(&record, &executable, deadline, cancelled) {
            Ok(observation) => {
                if !collection.push_observation(observation, max_observations) {
                    break;
                }
            }
            Err(code) => {
                collection.push_diagnostic(ObservationSourceKind::WindowsPackage, code);
            }
        }
    }
    collection
}

/// Enumerates globally installed npm packages and uv tools without launching a
/// package.  The resulting package ID is classification metadata only: it is
/// intentionally not an executable identity and cannot authorize ACP verify.
fn collect_windows_runner_installations(
    config: &WindowsPassiveWorkerConfig,
    deadline: Instant,
    cancelled: &AtomicBool,
    max_observations: usize,
) -> WindowsProviderCollection {
    let mut collection = WindowsProviderCollection::default();
    if cancelled.load(Ordering::Acquire) || Instant::now() >= deadline {
        return collection;
    }

    collect_npm_runner_installations(
        config.path_env.as_deref(),
        deadline,
        cancelled,
        max_observations,
        &mut collection,
    );
    if collection.observations.len() >= max_observations
        || cancelled.load(Ordering::Acquire)
        || Instant::now() >= deadline
    {
        return collection;
    }
    collect_uvx_runner_installations(
        config.path_env.as_deref(),
        deadline,
        cancelled,
        max_observations,
        &mut collection,
    );
    collection
}

fn collect_npm_runner_installations(
    path_env: Option<&str>,
    deadline: Instant,
    cancelled: &AtomicBool,
    max_observations: usize,
    collection: &mut WindowsProviderCollection,
) {
    let Some(runner) = find_runner_executable(path_env, "npx") else {
        return;
    };
    let fingerprint = match stable_file_fingerprint_with_deadline(&runner, deadline, cancelled) {
        Ok(fingerprint) => fingerprint,
        Err(code) => {
            collection.push_diagnostic(ObservationSourceKind::ExecutableInventory, code);
            return;
        }
    };
    let output = match run_readonly_runner_command(
        "npm",
        &["ls", "-g", "--depth=0", "--json", "--offline"],
        deadline,
        cancelled,
    ) {
        Ok(output) => output,
        Err(code) => {
            collection.push_diagnostic(ObservationSourceKind::ExecutableInventory, code);
            return;
        }
    };
    let records = match parse_npm_global_list(&output) {
        Ok(records) => records,
        Err(code) => {
            collection.push_diagnostic(ObservationSourceKind::ExecutableInventory, code);
            return;
        }
    };
    for record in records.into_iter().take(MAX_RUNNER_INSTALLATIONS) {
        if collection.observations.len() >= max_observations
            || cancelled.load(Ordering::Acquire)
            || Instant::now() >= deadline
        {
            break;
        }
        match runner_installation_observation(
            "npx",
            &runner,
            &fingerprint,
            record,
            deadline,
            cancelled,
        ) {
            Ok(observation) => {
                let _ = collection.push_observation(observation, max_observations);
            }
            Err(code) => {
                collection.push_diagnostic(ObservationSourceKind::ExecutableInventory, code)
            }
        }
    }
}

fn collect_uvx_runner_installations(
    path_env: Option<&str>,
    deadline: Instant,
    cancelled: &AtomicBool,
    max_observations: usize,
    collection: &mut WindowsProviderCollection,
) {
    let Some(runner) = find_runner_executable(path_env, "uvx") else {
        return;
    };
    let fingerprint = match stable_file_fingerprint_with_deadline(&runner, deadline, cancelled) {
        Ok(fingerprint) => fingerprint,
        Err(code) => {
            collection.push_diagnostic(ObservationSourceKind::ExecutableInventory, code);
            return;
        }
    };
    let output = match run_readonly_runner_command(
        "uv",
        &["--offline", "tool", "list"],
        deadline,
        cancelled,
    ) {
        Ok(output) => output,
        Err(code) => {
            collection.push_diagnostic(ObservationSourceKind::ExecutableInventory, code);
            return;
        }
    };
    let install_root =
        match run_readonly_runner_command("uv", &["--offline", "tool", "dir"], deadline, cancelled)
        {
            Ok(root) => match parse_runner_install_root(&root) {
                Some(root) => root,
                None => {
                    collection.push_diagnostic(
                        ObservationSourceKind::ExecutableInventory,
                        DiscoveryDiagnosticCode::InvalidSourceRecord,
                    );
                    return;
                }
            },
            Err(code) => {
                collection.push_diagnostic(ObservationSourceKind::ExecutableInventory, code);
                return;
            }
        };
    for (name, version) in parse_uv_tool_list(&output)
        .into_iter()
        .take(MAX_RUNNER_INSTALLATIONS)
    {
        if collection.observations.len() >= max_observations
            || cancelled.load(Ordering::Acquire)
            || Instant::now() >= deadline
        {
            break;
        }
        let record = RunnerPackageRecord {
            package_name: name,
            resolved_version: version,
            install_root: install_root.clone(),
            package_integrity: None,
        };
        match runner_installation_observation(
            "uvx",
            &runner,
            &fingerprint,
            record,
            deadline,
            cancelled,
        ) {
            Ok(observation) => {
                let _ = collection.push_observation(observation, max_observations);
            }
            Err(code) => {
                collection.push_diagnostic(ObservationSourceKind::ExecutableInventory, code)
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RunnerPackageRecord {
    package_name: String,
    resolved_version: String,
    install_root: PathBuf,
    package_integrity: Option<String>,
}

fn runner_installation_observation(
    runner_kind: &str,
    runner: &Path,
    runner_fingerprint: &StableFileFingerprint,
    record: RunnerPackageRecord,
    deadline: Instant,
    cancelled: &AtomicBool,
) -> Result<Observation, DiscoveryDiagnosticCode> {
    let package_id =
        canonical_runner_package_id(runner_kind, &record.package_name, &record.resolved_version)
            .ok_or(DiscoveryDiagnosticCode::InvalidSourceRecord)?;
    let package_directory =
        runner_package_directory(runner_kind, &record.install_root, &record.package_name)
            .ok_or(DiscoveryDiagnosticCode::InvalidSourceRecord)?;
    let package_tree_digest = bounded_package_tree_digest(&package_directory, deadline, cancelled)?;
    let runner_identity =
        windows_executable_identity_fingerprint(&runner_fingerprint.stable_identity);
    let fingerprint = ObservationFingerprint::from_parts(&[
        "runner-installation".into(),
        runner_kind.into(),
        runner_fingerprint.stable_identity.clone(),
        runner_fingerprint.content_sha256.clone(),
        package_tree_digest.clone(),
    ]);
    Ok(Observation {
        locator: ObservationLocator::Executable(runner.to_path_buf()),
        fingerprint,
        association_fingerprints: vec![runner_identity],
        package_ids: vec![package_id],
        runner_installation: Some(RunnerInstallationMetadata {
            runner_kind: runner_kind.into(),
            package_name: record.package_name.clone(),
            resolved_version: record.resolved_version.clone(),
            install_root: record.install_root,
            package_integrity: record.package_integrity,
            package_tree_digest,
            runner_executable_identity: runner_fingerprint.stable_identity.clone(),
            runner_executable_sha256: runner_fingerprint.content_sha256.clone(),
        }),
        source_kind: ObservationSourceKind::ExecutableInventory,
        category: CandidateCategory::AgentRuntime,
        trust_level: ObservationTrustLevel::Heuristic,
        verification_authority: VerificationAuthority::Unverified,
        availability_authority: VerificationAuthority::Unverified,
        discovery_authority: VerificationAuthority::Unverified,
        compatibility_authority: VerificationAuthority::Unverified,
        auth_authority: VerificationAuthority::Unverified,
        health_authority: VerificationAuthority::Unverified,
        connector_id: LOCAL_DISCOVERY_UNKNOWN_CONNECTOR_ID.into(),
        runtime_type: LOCAL_DISCOVERY_UNKNOWN_RUNTIME_TYPE.into(),
        display_name: format!(
            "{runner_kind} {}@{}",
            record.package_name, record.resolved_version
        ),
        availability: CandidateAvailability::Unconfigured,
        models: Vec::new(),
        catalog_revision: None,
        requires_configuration: true,
        discovery_state: DiscoveryState::Observed,
        compatibility_state: CompatibilityState::NotVerified,
        auth_state: AuthState::Unknown,
        health_state: HealthState::NotChecked,
        evidence_summary: vec![
            DiscoveryEvidence::ExecutableInventory,
            DiscoveryEvidence::InstallKnown,
        ],
        diagnostics: Vec::new(),
    })
}

fn find_runner_executable(path_env: Option<&str>, runner: &str) -> Option<PathBuf> {
    let path_env = path_env?;
    let names: &[&str] = if cfg!(windows) {
        &["{runner}.exe", "{runner}.cmd", "{runner}.bat"]
    } else {
        &[runner]
    };
    for directory in std::env::split_paths(path_env).take(256) {
        if !directory.is_absolute() || has_reparse_point(&directory) {
            continue;
        }
        for name in names {
            let name = name.replace("{runner}", runner);
            let path = directory.join(name);
            if has_reparse_point(&path) || !is_real_regular_file(&path) {
                continue;
            }
            if let Ok(canonical) = path.canonicalize() {
                if !has_reparse_point(&canonical) && is_real_regular_file(&canonical) {
                    return Some(canonical);
                }
            }
        }
    }
    None
}

fn parse_npm_global_list(
    bytes: &[u8],
) -> Result<Vec<RunnerPackageRecord>, DiscoveryDiagnosticCode> {
    #[derive(Deserialize)]
    struct NpmDependency {
        version: Option<String>,
        integrity: Option<String>,
    }
    #[derive(Deserialize)]
    struct NpmList {
        path: Option<String>,
        dependencies: Option<BTreeMap<String, NpmDependency>>,
    }

    let parsed: NpmList =
        serde_json::from_slice(bytes).map_err(|_| DiscoveryDiagnosticCode::InvalidSourceRecord)?;
    let install_root = parsed
        .path
        .and_then(|path| bounded_absolute_path(&path))
        .ok_or(DiscoveryDiagnosticCode::InvalidSourceRecord)?;
    let mut records = Vec::new();
    for (package_name, dependency) in parsed.dependencies.unwrap_or_default() {
        let Some(version) = dependency.version else {
            continue;
        };
        if canonical_runner_package_id("npx", &package_name, &version).is_none() {
            continue;
        }
        let integrity = dependency
            .integrity
            .filter(|value| safe_runner_text(value, 512));
        records.push(RunnerPackageRecord {
            package_name,
            resolved_version: version,
            install_root: install_root.clone(),
            package_integrity: integrity,
        });
    }
    Ok(records)
}

fn parse_uv_tool_list(bytes: &[u8]) -> Vec<(String, String)> {
    let Ok(text) = std::str::from_utf8(bytes) else {
        return Vec::new();
    };
    let mut tools = BTreeSet::new();
    for line in text.lines().take(MAX_RUNNER_INSTALLATIONS * 2) {
        let mut parts = line.split_ascii_whitespace();
        let Some(name) = parts.next() else { continue };
        let Some(version) = parts.next() else {
            continue;
        };
        let version = version.strip_prefix('v').unwrap_or(version);
        if parts.next().is_some() || canonical_runner_package_id("uvx", name, version).is_none() {
            continue;
        }
        tools.insert((name.to_ascii_lowercase(), version.to_owned()));
    }
    tools.into_iter().collect()
}

fn parse_runner_install_root(bytes: &[u8]) -> Option<PathBuf> {
    let text = std::str::from_utf8(bytes).ok()?.trim();
    bounded_absolute_path(text)
}

fn bounded_absolute_path(value: &str) -> Option<PathBuf> {
    safe_runner_text(value, 1024)
        .then(|| PathBuf::from(value))
        .filter(|path| path.is_absolute())
}

fn canonical_runner_package_id(runner_kind: &str, name: &str, version: &str) -> Option<String> {
    let valid_name = match runner_kind {
        "npx" => valid_npm_runner_package_name(name),
        "uvx" => valid_uvx_runner_package_name(name),
        _ => false,
    };
    (valid_name && valid_runner_version(version))
        .then(|| format!("{}@{}", name.to_ascii_lowercase(), version))
}

fn valid_npm_runner_package_name(value: &str) -> bool {
    let name = value.strip_prefix('@').unwrap_or(value);
    let mut parts = name.split('/');
    let first = parts.next();
    let second = parts.next();
    parts.next().is_none()
        && match (value.starts_with('@'), first, second) {
            (true, Some(scope), Some(package)) => {
                valid_runner_name_part(scope) && valid_runner_name_part(package)
            }
            (false, Some(package), None) => valid_runner_name_part(package),
            _ => false,
        }
}

fn valid_uvx_runner_package_name(value: &str) -> bool {
    !value.contains('/') && valid_runner_name_part(value)
}

fn valid_runner_name_part(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-'))
}

fn valid_runner_version(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '+'))
}

fn safe_runner_text(value: &str, max_len: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_len
        && !value.chars().any(|ch| {
            ch.is_control() || matches!(ch, '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}')
        })
}

fn runner_package_directory(
    runner_kind: &str,
    install_root: &Path,
    package_name: &str,
) -> Option<PathBuf> {
    let root = install_root.canonicalize().ok()?;
    if has_reparse_point(&root)
        || !valid_npm_runner_package_name(package_name)
            && !valid_uvx_runner_package_name(package_name)
    {
        return None;
    }
    let base = if runner_kind == "npx" {
        root.join("node_modules")
    } else {
        root.clone()
    };
    let candidate = package_name
        .split('/')
        .fold(base, |path, part| path.join(part));
    if has_reparse_point(&candidate) {
        return None;
    }
    let canonical = candidate.canonicalize().ok()?;
    (canonical.starts_with(&root) && canonical.is_dir() && !has_reparse_point(&canonical))
        .then_some(canonical)
}

fn bounded_package_tree_digest(
    package_directory: &Path,
    deadline: Instant,
    cancelled: &AtomicBool,
) -> Result<String, DiscoveryDiagnosticCode> {
    let root = package_directory
        .canonicalize()
        .map_err(|error| io_diagnostic_code(&error))?;
    if has_reparse_point(&root) {
        return Err(DiscoveryDiagnosticCode::ReparsePointRejected);
    }
    let mut pending = vec![root.clone()];
    let mut files = Vec::new();
    let mut total_bytes = 0u64;
    while let Some(directory) = pending.pop() {
        if cancelled.load(Ordering::Acquire) || Instant::now() >= deadline {
            return Err(DiscoveryDiagnosticCode::ProviderTimeout);
        }
        let mut entries = fs::read_dir(&directory)
            .map_err(|error| io_diagnostic_code(&error))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| io_diagnostic_code(&error))?;
        entries.sort_by_key(|entry| normalized_path_key(&entry.path()));
        for entry in entries {
            let path = entry.path();
            if has_reparse_point(&path) {
                return Err(DiscoveryDiagnosticCode::ReparsePointRejected);
            }
            let metadata = entry
                .metadata()
                .map_err(|error| io_diagnostic_code(&error))?;
            if metadata.is_dir() {
                let canonical = path
                    .canonicalize()
                    .map_err(|error| io_diagnostic_code(&error))?;
                if !canonical.starts_with(&root) {
                    return Err(DiscoveryDiagnosticCode::InvalidSourceRecord);
                }
                pending.push(canonical);
            } else if metadata.is_file() {
                total_bytes = total_bytes.saturating_add(metadata.len());
                if files.len() >= MAX_RUNNER_PACKAGE_TREE_FILES
                    || total_bytes > MAX_RUNNER_PACKAGE_TREE_BYTES
                {
                    return Err(DiscoveryDiagnosticCode::OversizedInput);
                }
                let canonical = path
                    .canonicalize()
                    .map_err(|error| io_diagnostic_code(&error))?;
                if !canonical.starts_with(&root) {
                    return Err(DiscoveryDiagnosticCode::InvalidSourceRecord);
                }
                files.push((canonical, metadata.len()));
            }
        }
    }
    files.sort_by_key(|left| normalized_path_key(&left.0));
    let mut hasher = Sha256::new();
    hasher.update(b"agenttalk-runner-package-tree-v1");
    for (path, len) in files {
        if cancelled.load(Ordering::Acquire) || Instant::now() >= deadline {
            return Err(DiscoveryDiagnosticCode::ProviderTimeout);
        }
        let relative = path
            .strip_prefix(&root)
            .map_err(|_| DiscoveryDiagnosticCode::InvalidSourceRecord)?;
        hasher.update(normalized_relative_identity(relative)?.as_bytes());
        hasher.update([0]);
        hasher.update(len.to_le_bytes());
        let mut file = fs::File::open(&path).map_err(|error| io_diagnostic_code(&error))?;
        let mut remaining = len;
        let mut buffer = [0u8; 8192];
        while remaining > 0 {
            if cancelled.load(Ordering::Acquire) || Instant::now() >= deadline {
                return Err(DiscoveryDiagnosticCode::ProviderTimeout);
            }
            let to_read = remaining.min(buffer.len() as u64) as usize;
            let read = file
                .read(&mut buffer[..to_read])
                .map_err(|_| DiscoveryDiagnosticCode::InvalidSourceRecord)?;
            if read == 0 {
                return Err(DiscoveryDiagnosticCode::FingerprintChanged);
            }
            hasher.update(&buffer[..read]);
            remaining -= read as u64;
        }
    }
    Ok(sha256_hex(&hasher.finalize()))
}

fn run_readonly_runner_command(
    executable: &str,
    args: &[&str],
    deadline: Instant,
    cancelled: &AtomicBool,
) -> Result<Vec<u8>, DiscoveryDiagnosticCode> {
    let mut command = Command::new(executable);
    command
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env("npm_config_offline", "true")
        .env("npm_config_audit", "false")
        .env("npm_config_fund", "false")
        .env("UV_OFFLINE", "1")
        .env("UV_NO_PROGRESS", "1");
    let mut child = command
        .spawn()
        .map_err(|_| DiscoveryDiagnosticCode::ProviderFailed)?;
    let stdout = child
        .stdout
        .take()
        .ok_or(DiscoveryDiagnosticCode::ProviderFailed)?;
    let stderr = child
        .stderr
        .take()
        .ok_or(DiscoveryDiagnosticCode::ProviderFailed)?;
    let stdout_reader =
        thread::spawn(move || read_limited_stream(stdout, MAX_RUNNER_COMMAND_OUTPUT_BYTES));
    let stderr_reader =
        thread::spawn(move || read_limited_stream(stderr, MAX_RUNNER_COMMAND_OUTPUT_BYTES));
    loop {
        if cancelled.load(Ordering::Acquire) || Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            let _ = stdout_reader.join();
            let _ = stderr_reader.join();
            return Err(DiscoveryDiagnosticCode::ProviderTimeout);
        }
        match child.try_wait() {
            Ok(Some(status)) => {
                let stdout = stdout_reader
                    .join()
                    .map_err(|_| DiscoveryDiagnosticCode::ProviderFailed)??;
                let _ = stderr_reader
                    .join()
                    .map_err(|_| DiscoveryDiagnosticCode::ProviderFailed)??;
                return status
                    .success()
                    .then_some(stdout)
                    .ok_or(DiscoveryDiagnosticCode::ProviderFailed);
            }
            Ok(None) => thread::sleep(Duration::from_millis(10)),
            Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
                let _ = stdout_reader.join();
                let _ = stderr_reader.join();
                return Err(DiscoveryDiagnosticCode::ProviderFailed);
            }
        }
    }
}

#[cfg(test)]
mod runner_installation_tests {
    use super::*;

    #[test]
    fn npm_global_json_projects_version_integrity_and_root() {
        let bytes = br#"{"path":"C:\\npm","dependencies":{"@scope/agent":{"version":"1.2.3","integrity":"sha512-abc"},"bad":{"version":"not a version"}}}"#;
        let records = parse_npm_global_list(bytes).expect("npm list parses");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].package_name, "@scope/agent");
        assert_eq!(records[0].resolved_version, "1.2.3");
        assert_eq!(records[0].package_integrity.as_deref(), Some("sha512-abc"));
    }

    #[test]
    fn uv_tool_list_projects_only_safe_name_version_pairs() {
        let records = parse_uv_tool_list(b"agent 1.2.3\ninvalid/name 1.0.0\nother v2.0.0\n");
        assert_eq!(
            records,
            vec![
                ("agent".into(), "1.2.3".into()),
                ("other".into(), "2.0.0".into())
            ]
        );
    }

    #[test]
    fn package_id_is_match_key_and_not_identity_authority() {
        assert_eq!(
            canonical_runner_package_id("npx", "@scope/agent", "1.2.3").as_deref(),
            Some("@scope/agent@1.2.3")
        );
        assert_eq!(
            canonical_runner_package_id("uvx", "agent", "1.2.3").as_deref(),
            Some("agent@1.2.3")
        );
    }
}

fn read_limited_stream<R: Read>(
    reader: R,
    max_bytes: usize,
) -> Result<Vec<u8>, DiscoveryDiagnosticCode> {
    let mut bytes = Vec::new();
    reader
        .take((max_bytes + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| DiscoveryDiagnosticCode::ProviderFailed)?;
    (bytes.len() <= max_bytes)
        .then_some(bytes)
        .ok_or(DiscoveryDiagnosticCode::OversizedInput)
}

fn collect_windows_loopback_observations(
    config: &WindowsPassiveWorkerConfig,
    deadline: Instant,
    cancelled: &AtomicBool,
    max_observations: usize,
) -> WindowsProviderCollection {
    let mut collection = WindowsProviderCollection::default();
    let mut records = config.loopback_records.clone();
    if config.use_real_loopback {
        records.extend(read_windows_loopback_listener_records(&mut collection));
    }
    let recheck_records = config.loopback_recheck_records.as_deref();
    let mut seen_records = BTreeSet::new();
    for record in records {
        if cancelled.load(Ordering::Acquire) || Instant::now() >= deadline {
            break;
        }
        let address = match record.address.parse::<IpAddr>() {
            Ok(address) => address,
            Err(_) => {
                collection.push_diagnostic(
                    ObservationSourceKind::LoopbackListener,
                    DiscoveryDiagnosticCode::InvalidSourceRecord,
                );
                continue;
            }
        };
        if !is_allowed_loopback_ip(&address) {
            collection.push_diagnostic(
                ObservationSourceKind::LoopbackListener,
                DiscoveryDiagnosticCode::NonLoopbackRejected,
            );
            continue;
        }
        let Some(executable) = record.owner_executable.as_deref() else {
            collection.push_diagnostic(
                ObservationSourceKind::LoopbackListener,
                DiscoveryDiagnosticCode::SourceDisappeared,
            );
            continue;
        };
        if let Some(recheck_records) = recheck_records {
            if !loopback_record_survives_recheck(&record, recheck_records) {
                collection.push_diagnostic(
                    ObservationSourceKind::LoopbackListener,
                    DiscoveryDiagnosticCode::SourceDisappeared,
                );
                continue;
            }
        }
        if !seen_records.insert(normalized_path_key(executable)) {
            continue;
        }
        match unknown_executable_observation(
            ObservationSourceKind::LoopbackListener,
            DiscoveryEvidence::LoopbackListener,
            ObservationTrustLevel::Heuristic,
            executable,
            deadline,
            cancelled,
        ) {
            Ok(mut observation) => {
                observation.display_name = "Loopback listener".into();
                if !collection.push_observation(observation, max_observations) {
                    break;
                }
            }
            Err(code) => {
                collection.push_diagnostic(ObservationSourceKind::LoopbackListener, code);
            }
        }
    }
    collection
}

fn collect_explicit_source_observations(
    config: &WindowsPassiveWorkerConfig,
    deadline: Instant,
    cancelled: &AtomicBool,
    max_observations: usize,
) -> WindowsProviderCollection {
    let mut collection = WindowsProviderCollection::default();
    for source in &config.explicit_sources {
        if cancelled.load(Ordering::Acquire) || Instant::now() >= deadline {
            break;
        }
        match source {
            ExplicitDiscoverySource::Executable(path) => match unknown_executable_observation(
                ObservationSourceKind::UserSelected,
                DiscoveryEvidence::UserSelected,
                ObservationTrustLevel::UserSelected,
                path,
                deadline,
                cancelled,
            ) {
                Ok(observation) => {
                    if !collection.push_observation(observation, max_observations) {
                        break;
                    }
                }
                Err(code) => collection.push_diagnostic(ObservationSourceKind::UserSelected, code),
            },
            ExplicitDiscoverySource::Endpoint(endpoint) => {
                match normalize_explicit_loopback_endpoint(endpoint) {
                    Ok(endpoint_ref) => {
                        let observation = unknown_endpoint_observation(&endpoint_ref);
                        if !collection.push_observation(observation, max_observations) {
                            break;
                        }
                    }
                    Err(code) => {
                        collection.push_diagnostic(ObservationSourceKind::UserSelected, code);
                    }
                }
            }
        }
    }
    collection
}

fn unknown_executable_observation(
    source_kind: ObservationSourceKind,
    evidence: DiscoveryEvidence,
    trust_level: ObservationTrustLevel,
    executable: &Path,
    deadline: Instant,
    cancelled: &AtomicBool,
) -> Result<Observation, DiscoveryDiagnosticCode> {
    if !executable.is_absolute() {
        return Err(DiscoveryDiagnosticCode::InvalidSourceRecord);
    }
    if has_reparse_point(executable) {
        return Err(DiscoveryDiagnosticCode::ReparsePointRejected);
    }
    let canonical = executable
        .canonicalize()
        .map_err(|error| io_diagnostic_code(&error))?;
    if has_reparse_point(&canonical) {
        return Err(DiscoveryDiagnosticCode::ReparsePointRejected);
    }
    if !is_windows_executable_file(&canonical) || !is_real_regular_file(&canonical) {
        return Err(DiscoveryDiagnosticCode::InvalidSourceRecord);
    }
    let file_identity = stable_file_identity_with_deadline(&canonical, deadline, cancelled)?;
    let executable_identity = windows_executable_identity_fingerprint(&file_identity);
    let display_name = canonical
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or("Local executable")
        .to_owned();
    Ok(Observation {
        locator: ObservationLocator::Executable(canonical),
        fingerprint: executable_identity.clone(),
        association_fingerprints: vec![executable_identity],
        package_ids: Vec::new(),
        runner_installation: None,
        source_kind,
        category: CandidateCategory::Unknown,
        trust_level,
        verification_authority: VerificationAuthority::Unverified,
        availability_authority: VerificationAuthority::Unverified,
        discovery_authority: VerificationAuthority::Unverified,
        compatibility_authority: VerificationAuthority::Unverified,
        auth_authority: VerificationAuthority::Unverified,
        health_authority: VerificationAuthority::Unverified,
        connector_id: LOCAL_DISCOVERY_UNKNOWN_CONNECTOR_ID.into(),
        runtime_type: LOCAL_DISCOVERY_UNKNOWN_RUNTIME_TYPE.into(),
        display_name,
        availability: CandidateAvailability::Unconfigured,
        models: Vec::new(),
        catalog_revision: None,
        requires_configuration: true,
        discovery_state: DiscoveryState::Observed,
        compatibility_state: CompatibilityState::NotVerified,
        auth_state: AuthState::Unknown,
        health_state: HealthState::NotChecked,
        evidence_summary: vec![evidence],
        diagnostics: Vec::new(),
    })
}

fn unknown_endpoint_observation(endpoint_ref: &str) -> Observation {
    Observation {
        locator: ObservationLocator::Endpoint {
            endpoint_ref: endpoint_ref.to_owned(),
        },
        fingerprint: ObservationFingerprint::from_parts(&[
            "windows-loopback-endpoint".into(),
            endpoint_ref.to_owned(),
        ]),
        association_fingerprints: Vec::new(),
        package_ids: Vec::new(),
        runner_installation: None,
        source_kind: ObservationSourceKind::UserSelected,
        category: CandidateCategory::Unknown,
        trust_level: ObservationTrustLevel::UserSelected,
        verification_authority: VerificationAuthority::Unverified,
        availability_authority: VerificationAuthority::Unverified,
        discovery_authority: VerificationAuthority::Unverified,
        compatibility_authority: VerificationAuthority::Unverified,
        auth_authority: VerificationAuthority::Unverified,
        health_authority: VerificationAuthority::Unverified,
        connector_id: LOCAL_DISCOVERY_UNKNOWN_CONNECTOR_ID.into(),
        runtime_type: LOCAL_DISCOVERY_UNKNOWN_RUNTIME_TYPE.into(),
        display_name: "Loopback endpoint".into(),
        availability: CandidateAvailability::Unconfigured,
        models: Vec::new(),
        catalog_revision: None,
        requires_configuration: true,
        discovery_state: DiscoveryState::Observed,
        compatibility_state: CompatibilityState::NotVerified,
        auth_state: AuthState::Unknown,
        health_state: HealthState::NotChecked,
        evidence_summary: vec![DiscoveryEvidence::UserSelected],
        diagnostics: Vec::new(),
    }
}

fn unknown_package_observation(
    record: &WindowsPackageRecord,
    executable: &Path,
    deadline: Instant,
    cancelled: &AtomicBool,
) -> Result<Observation, DiscoveryDiagnosticCode> {
    let mut observation = unknown_executable_observation(
        ObservationSourceKind::WindowsPackage,
        DiscoveryEvidence::WindowsPackageInventory,
        ObservationTrustLevel::Heuristic,
        executable,
        deadline,
        cancelled,
    )?;
    let file_identity = stable_file_identity_with_deadline(executable, deadline, cancelled)?;
    observation.fingerprint = package_stable_identity(record)?;
    observation
        .association_fingerprints
        .push(windows_executable_identity_fingerprint(&file_identity));
    Ok(observation)
}

fn package_executable_path(
    record: &WindowsPackageRecord,
) -> Result<PathBuf, DiscoveryDiagnosticCode> {
    if !record.installed_location.is_absolute()
        || record.executable_relative_path.is_absolute()
        || record
            .executable_relative_path
            .components()
            .any(|component| {
                matches!(
                    component,
                    std::path::Component::ParentDir
                        | std::path::Component::Prefix(_)
                        | std::path::Component::RootDir
                )
            })
    {
        return Err(DiscoveryDiagnosticCode::InvalidSourceRecord);
    }
    if has_reparse_point(&record.installed_location) {
        return Err(DiscoveryDiagnosticCode::ReparsePointRejected);
    }
    let root = record
        .installed_location
        .canonicalize()
        .map_err(|error| io_diagnostic_code(&error))?;
    let executable = root.join(&record.executable_relative_path);
    if has_reparse_point(&executable) {
        return Err(DiscoveryDiagnosticCode::ReparsePointRejected);
    }
    let canonical = executable
        .canonicalize()
        .map_err(|error| io_diagnostic_code(&error))?;
    if !canonical.starts_with(&root) {
        return Err(DiscoveryDiagnosticCode::InvalidSourceRecord);
    }
    Ok(canonical)
}

fn package_stable_identity(
    record: &WindowsPackageRecord,
) -> Result<ObservationFingerprint, DiscoveryDiagnosticCode> {
    Ok(ObservationFingerprint::from_parts(&[
        "package-stable-id".into(),
        project_package_identity_part(&record.package_family_name)
            .ok_or(DiscoveryDiagnosticCode::InvalidSourceRecord)?,
        normalized_relative_identity(&record.executable_relative_path)?,
    ]))
}

fn windows_executable_identity_fingerprint(file_identity: &str) -> ObservationFingerprint {
    ObservationFingerprint::from_parts(&["windows-executable".into(), file_identity.to_owned()])
}

fn project_package_identity_part(value: &str) -> Option<String> {
    let value = value.trim();
    let valid = !value.is_empty()
        && value.len() <= 256
        && !value.contains(['\\', '/', ':'])
        && value.chars().all(|ch| {
            !ch.is_control() && !matches!(ch, '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}')
        });
    valid.then(|| value.to_ascii_lowercase())
}

fn normalized_relative_identity(path: &Path) -> Result<String, DiscoveryDiagnosticCode> {
    if path.is_absolute() {
        return Err(DiscoveryDiagnosticCode::InvalidSourceRecord);
    }
    let mut parts = Vec::new();
    for component in path.components() {
        match component {
            std::path::Component::Normal(part) => {
                let Some(part) = part.to_str() else {
                    return Err(DiscoveryDiagnosticCode::InvalidSourceRecord);
                };
                if part.is_empty()
                    || part.contains(['\\', '/', ':'])
                    || part.chars().any(|ch| {
                        ch.is_control()
                            || matches!(ch, '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}')
                    })
                {
                    return Err(DiscoveryDiagnosticCode::InvalidSourceRecord);
                }
                parts.push(part.to_ascii_lowercase());
            }
            _ => return Err(DiscoveryDiagnosticCode::InvalidSourceRecord),
        }
    }
    if parts.is_empty() {
        return Err(DiscoveryDiagnosticCode::InvalidSourceRecord);
    }
    Ok(parts.join("/"))
}

fn loopback_record_survives_recheck(
    record: &WindowsLoopbackListenerRecord,
    recheck_records: &[WindowsLoopbackListenerRecord],
) -> bool {
    recheck_records.iter().any(|candidate| {
        candidate.address == record.address
            && candidate.port == record.port
            && candidate.owner_pid == record.owner_pid
            && candidate.owner_identity == record.owner_identity
            && candidate.owner_executable == record.owner_executable
    })
}

fn is_windows_executable_file(path: &Path) -> bool {
    if !cfg!(windows) {
        return true;
    }
    path.extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| extension.eq_ignore_ascii_case("exe"))
}

fn is_allowed_loopback_ip(address: &IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => address.octets()[0] == 127,
        IpAddr::V6(address) => address.is_loopback(),
    }
}

fn normalize_explicit_loopback_endpoint(endpoint: &str) -> Result<String, DiscoveryDiagnosticCode> {
    if endpoint.trim() != endpoint
        || endpoint.chars().any(|ch| {
            ch.is_control() || matches!(ch, '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}')
        })
    {
        return Err(DiscoveryDiagnosticCode::InvalidSourceRecord);
    }
    let Some((scheme, rest)) = endpoint.split_once("://") else {
        return Err(DiscoveryDiagnosticCode::InvalidSourceRecord);
    };
    let scheme = scheme.to_ascii_lowercase();
    if scheme != "http" && scheme != "https" {
        return Err(DiscoveryDiagnosticCode::InvalidSourceRecord);
    }
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    if authority.is_empty() || authority.contains('@') || authority.contains('\\') {
        return Err(DiscoveryDiagnosticCode::InvalidSourceRecord);
    }
    let (host, normalized_host, port) = if let Some(after_bracket) = authority.strip_prefix('[') {
        let Some((host, tail)) = after_bracket.split_once(']') else {
            return Err(DiscoveryDiagnosticCode::InvalidSourceRecord);
        };
        if !tail.starts_with(':') || tail.len() == 1 {
            return Err(DiscoveryDiagnosticCode::InvalidSourceRecord);
        }
        (
            host,
            format!("[{}]", host.to_ascii_lowercase()),
            parse_endpoint_port(&tail[1..])?,
        )
    } else {
        let Some((host, port)) = authority.split_once(':') else {
            return Err(DiscoveryDiagnosticCode::InvalidSourceRecord);
        };
        if host.is_empty() || port.is_empty() || port.contains(':') {
            return Err(DiscoveryDiagnosticCode::InvalidSourceRecord);
        }
        (host, host.to_ascii_lowercase(), parse_endpoint_port(port)?)
    };
    let loopback = normalized_host == "localhost"
        || host
            .parse::<IpAddr>()
            .map(|address| is_allowed_loopback_ip(&address))
            .unwrap_or(false);
    if !loopback {
        return Err(DiscoveryDiagnosticCode::NonLoopbackRejected);
    }
    if host.contains(':') && !normalized_host.starts_with('[') {
        return Err(DiscoveryDiagnosticCode::InvalidSourceRecord);
    }
    Ok(format!("{scheme}://{normalized_host}:{port}"))
}

fn parse_endpoint_port(value: &str) -> Result<u16, DiscoveryDiagnosticCode> {
    if value.is_empty() || !value.chars().all(|ch| ch.is_ascii_digit()) {
        return Err(DiscoveryDiagnosticCode::InvalidSourceRecord);
    }
    let port = value
        .parse::<u16>()
        .map_err(|_| DiscoveryDiagnosticCode::InvalidSourceRecord)?;
    if port == 0 {
        return Err(DiscoveryDiagnosticCode::InvalidSourceRecord);
    }
    Ok(port)
}

fn normalized_path_key(path: &Path) -> String {
    let value = path.to_string_lossy().replace('\\', "/");
    if cfg!(windows) {
        value.to_ascii_lowercase()
    } else {
        value
    }
}

fn io_diagnostic_code(error: &std::io::Error) -> DiscoveryDiagnosticCode {
    match error.kind() {
        std::io::ErrorKind::NotFound => DiscoveryDiagnosticCode::SourceDisappeared,
        std::io::ErrorKind::PermissionDenied => DiscoveryDiagnosticCode::AccessDenied,
        _ => DiscoveryDiagnosticCode::InvalidSourceRecord,
    }
}

#[cfg(windows)]
fn read_windows_app_path_records(
    collection: &mut WindowsProviderCollection,
) -> Vec<WindowsAppPathRecord> {
    use windows_sys::Win32::Foundation::{
        ERROR_ACCESS_DENIED, ERROR_FILE_NOT_FOUND, ERROR_NO_MORE_ITEMS, ERROR_SUCCESS,
    };
    use windows_sys::Win32::System::Registry::{
        RegCloseKey, RegEnumKeyExW, RegGetValueW, RegOpenKeyExW, HKEY, HKEY_CURRENT_USER,
        HKEY_LOCAL_MACHINE, KEY_READ, KEY_WOW64_32KEY, REG_SZ, RRF_RT_REG_SZ,
    };

    struct RegistryKey(HKEY);
    impl Drop for RegistryKey {
        fn drop(&mut self) {
            unsafe {
                RegCloseKey(self.0);
            }
        }
    }

    fn utf16_z_to_string(bytes: &[u16]) -> Option<String> {
        let end = bytes
            .iter()
            .position(|value| *value == 0)
            .unwrap_or(bytes.len());
        String::from_utf16(&bytes[..end]).ok()
    }

    fn default_string(key: HKEY) -> Result<Option<String>, DiscoveryDiagnosticCode> {
        let mut value_type = 0u32;
        let mut bytes = 0u32;
        let status = unsafe {
            RegGetValueW(
                key,
                std::ptr::null(),
                std::ptr::null(),
                RRF_RT_REG_SZ,
                &mut value_type,
                std::ptr::null_mut(),
                &mut bytes,
            )
        };
        if status == ERROR_FILE_NOT_FOUND {
            return Ok(None);
        }
        if status != ERROR_SUCCESS {
            return Err(if status == ERROR_ACCESS_DENIED {
                DiscoveryDiagnosticCode::AccessDenied
            } else {
                DiscoveryDiagnosticCode::InvalidSourceRecord
            });
        }
        if value_type != REG_SZ || bytes == 0 {
            return Err(DiscoveryDiagnosticCode::InvalidSourceRecord);
        }
        let mut buffer = vec![0u16; (bytes as usize / 2).saturating_add(1)];
        let mut bytes = (buffer.len() * 2) as u32;
        let status = unsafe {
            RegGetValueW(
                key,
                std::ptr::null(),
                std::ptr::null(),
                RRF_RT_REG_SZ,
                &mut value_type,
                buffer.as_mut_ptr().cast(),
                &mut bytes,
            )
        };
        if status != ERROR_SUCCESS {
            return Err(if status == ERROR_ACCESS_DENIED {
                DiscoveryDiagnosticCode::AccessDenied
            } else {
                DiscoveryDiagnosticCode::InvalidSourceRecord
            });
        }
        Ok(utf16_z_to_string(&buffer).filter(|value| !value.trim().is_empty()))
    }

    let roots = [
        (HKEY_CURRENT_USER, WindowsRegistryHive::CurrentUser),
        (HKEY_LOCAL_MACHINE, WindowsRegistryHive::LocalMachine),
    ];
    let views = [
        (0, WindowsRegistryView::Native),
        (KEY_WOW64_32KEY, WindowsRegistryView::Wow6432),
    ];
    let subkey = wide_null(std::ffi::OsStr::new(
        "Software\\Microsoft\\Windows\\CurrentVersion\\App Paths",
    ));
    let mut records = Vec::new();
    for (root, hive) in roots {
        for (view_flag, view) in views {
            if records.len() >= MAX_REAL_APP_PATH_RECORDS {
                return records;
            }
            let mut key = std::ptr::null_mut();
            let status =
                unsafe { RegOpenKeyExW(root, subkey.as_ptr(), 0, KEY_READ | view_flag, &mut key) };
            if status == ERROR_FILE_NOT_FOUND {
                continue;
            }
            if status != ERROR_SUCCESS {
                collection.push_diagnostic(
                    ObservationSourceKind::WindowsAppPath,
                    if status == ERROR_ACCESS_DENIED {
                        DiscoveryDiagnosticCode::AccessDenied
                    } else {
                        DiscoveryDiagnosticCode::ProviderFailed
                    },
                );
                continue;
            }
            let key = RegistryKey(key);
            let mut index = 0u32;
            loop {
                if records.len() >= MAX_REAL_APP_PATH_RECORDS {
                    break;
                }
                let mut name = vec![0u16; 512];
                let mut name_len = name.len() as u32;
                let status = unsafe {
                    RegEnumKeyExW(
                        key.0,
                        index,
                        name.as_mut_ptr(),
                        &mut name_len,
                        std::ptr::null(),
                        std::ptr::null_mut(),
                        std::ptr::null_mut(),
                        std::ptr::null_mut(),
                    )
                };
                if status == ERROR_NO_MORE_ITEMS {
                    break;
                }
                index += 1;
                if status != ERROR_SUCCESS {
                    collection.push_diagnostic(
                        ObservationSourceKind::WindowsAppPath,
                        DiscoveryDiagnosticCode::InvalidSourceRecord,
                    );
                    continue;
                }
                let Some(key_name) = utf16_z_to_string(&name[..name_len as usize]) else {
                    collection.push_diagnostic(
                        ObservationSourceKind::WindowsAppPath,
                        DiscoveryDiagnosticCode::InvalidSourceRecord,
                    );
                    continue;
                };
                let child_subkey = wide_null(std::ffi::OsStr::new(&format!(
                    "Software\\Microsoft\\Windows\\CurrentVersion\\App Paths\\{key_name}"
                )));
                let mut child = std::ptr::null_mut();
                let status = unsafe {
                    RegOpenKeyExW(
                        root,
                        child_subkey.as_ptr(),
                        0,
                        KEY_READ | view_flag,
                        &mut child,
                    )
                };
                if status == ERROR_FILE_NOT_FOUND {
                    collection.push_diagnostic(
                        ObservationSourceKind::WindowsAppPath,
                        DiscoveryDiagnosticCode::SourceDisappeared,
                    );
                    continue;
                }
                if status != ERROR_SUCCESS {
                    collection.push_diagnostic(
                        ObservationSourceKind::WindowsAppPath,
                        if status == ERROR_ACCESS_DENIED {
                            DiscoveryDiagnosticCode::AccessDenied
                        } else {
                            DiscoveryDiagnosticCode::InvalidSourceRecord
                        },
                    );
                    continue;
                }
                let child = RegistryKey(child);
                match default_string(child.0) {
                    Ok(Some(executable)) => records.push(WindowsAppPathRecord {
                        key_name,
                        executable_path: PathBuf::from(executable),
                        hive,
                        view,
                    }),
                    Ok(None) => {}
                    Err(code) => {
                        collection.push_diagnostic(ObservationSourceKind::WindowsAppPath, code)
                    }
                }
            }
        }
    }
    records
}

#[cfg(not(windows))]
fn read_windows_app_path_records(
    _collection: &mut WindowsProviderCollection,
) -> Vec<WindowsAppPathRecord> {
    Vec::new()
}

#[cfg(windows)]
fn read_windows_package_records(
    collection: &mut WindowsProviderCollection,
) -> Vec<WindowsPackageRecord> {
    use windows::Management::Deployment::PackageManager;

    let manager = match PackageManager::new() {
        Ok(manager) => manager,
        Err(_) => {
            collection.push_diagnostic(
                ObservationSourceKind::WindowsPackage,
                DiscoveryDiagnosticCode::ProviderFailed,
            );
            return Vec::new();
        }
    };
    let packages = match manager.FindPackagesByUserSecurityId(&windows::core::HSTRING::new()) {
        Ok(packages) => packages,
        Err(_) => {
            collection.push_diagnostic(
                ObservationSourceKind::WindowsPackage,
                DiscoveryDiagnosticCode::ProviderFailed,
            );
            return Vec::new();
        }
    };
    let iterator = match packages.First() {
        Ok(iterator) => iterator,
        Err(_) => {
            collection.push_diagnostic(
                ObservationSourceKind::WindowsPackage,
                DiscoveryDiagnosticCode::ProviderFailed,
            );
            return Vec::new();
        }
    };
    let mut records = Vec::new();
    while records.len() < MAX_REAL_PACKAGE_RECORDS {
        let has_current = match iterator.HasCurrent() {
            Ok(value) => value,
            Err(_) => {
                collection.push_diagnostic(
                    ObservationSourceKind::WindowsPackage,
                    DiscoveryDiagnosticCode::SourceDisappeared,
                );
                break;
            }
        };
        if !has_current {
            break;
        }
        let package = match iterator.Current() {
            Ok(package) => package,
            Err(_) => {
                collection.push_diagnostic(
                    ObservationSourceKind::WindowsPackage,
                    DiscoveryDiagnosticCode::SourceDisappeared,
                );
                let _ = iterator.MoveNext();
                continue;
            }
        };
        let package_id = match package.Id() {
            Ok(package_id) => package_id,
            Err(_) => {
                collection.push_diagnostic(
                    ObservationSourceKind::WindowsPackage,
                    DiscoveryDiagnosticCode::InvalidSourceRecord,
                );
                let _ = iterator.MoveNext();
                continue;
            }
        };
        let family_name = package_id
            .FamilyName()
            .map(|value| value.to_string_lossy())
            .unwrap_or_default();
        let full_name = package_id
            .FullName()
            .map(|value| value.to_string_lossy())
            .unwrap_or_default();
        let version = package_id
            .Version()
            .map(|version| {
                format!(
                    "{}.{}.{}.{}",
                    version.Major, version.Minor, version.Build, version.Revision
                )
            })
            .unwrap_or_else(|_| "0.0.0.0".into());
        let installed_location = match package.InstalledPath() {
            Ok(path) => PathBuf::from(path.to_string_lossy()),
            Err(_) => {
                collection.push_diagnostic(
                    ObservationSourceKind::WindowsPackage,
                    DiscoveryDiagnosticCode::AccessDenied,
                );
                let _ = iterator.MoveNext();
                continue;
            }
        };
        let manifest = installed_location.join("AppxManifest.xml");
        match parse_appx_manifest_executables(&manifest) {
            Ok(executables) => {
                for executable_relative_path in executables {
                    if records.len() >= MAX_REAL_PACKAGE_RECORDS {
                        break;
                    }
                    records.push(WindowsPackageRecord {
                        package_family_name: family_name.clone(),
                        package_full_name: full_name.clone(),
                        version: version.clone(),
                        installed_location: installed_location.clone(),
                        executable_relative_path,
                    });
                }
            }
            Err(code) => collection.push_diagnostic(ObservationSourceKind::WindowsPackage, code),
        }
        if !iterator.MoveNext().unwrap_or(false) {
            break;
        }
    }
    records
}

#[cfg(not(windows))]
fn read_windows_package_records(
    _collection: &mut WindowsProviderCollection,
) -> Vec<WindowsPackageRecord> {
    Vec::new()
}

fn parse_appx_manifest_executables(
    manifest: &Path,
) -> Result<Vec<PathBuf>, DiscoveryDiagnosticCode> {
    use quick_xml::events::Event;
    let file = fs::File::open(manifest).map_err(|error| io_diagnostic_code(&error))?;
    let mut limited = file.take((MAX_PACKAGE_MANIFEST_BYTES + 1) as u64);
    let mut bytes = Vec::new();
    limited
        .read_to_end(&mut bytes)
        .map_err(|_| DiscoveryDiagnosticCode::InvalidSourceRecord)?;
    if bytes.len() > MAX_PACKAGE_MANIFEST_BYTES {
        return Err(DiscoveryDiagnosticCode::OversizedInput);
    }
    let mut reader = quick_xml::Reader::from_reader(std::io::Cursor::new(bytes));
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    let mut executables = Vec::new();
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(event)) | Ok(Event::Empty(event)) => {
                if xml_local_name(event.name().as_ref()) == b"Application" {
                    for attribute in event.attributes().with_checks(true) {
                        let attribute =
                            attribute.map_err(|_| DiscoveryDiagnosticCode::InvalidSourceRecord)?;
                        if xml_local_name(attribute.key.as_ref()) == b"Executable" {
                            let value = attribute
                                .decoded_and_normalized_value(
                                    quick_xml::XmlVersion::Implicit1_0,
                                    reader.decoder(),
                                )
                                .map_err(|_| DiscoveryDiagnosticCode::InvalidSourceRecord)?
                                .into_owned();
                            if !value.trim().is_empty() {
                                executables.push(PathBuf::from(value));
                            }
                        }
                    }
                }
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(_) => return Err(DiscoveryDiagnosticCode::InvalidSourceRecord),
        }
        buf.clear();
    }
    Ok(executables)
}

fn xml_local_name(name: &[u8]) -> &[u8] {
    name.rsplit(|byte| *byte == b':').next().unwrap_or(name)
}

#[cfg(windows)]
fn read_windows_loopback_listener_records(
    collection: &mut WindowsProviderCollection,
) -> Vec<WindowsLoopbackListenerRecord> {
    let mut records = Vec::new();
    read_windows_loopback_listener_records_v4(collection, &mut records);
    read_windows_loopback_listener_records_v6(collection, &mut records);
    records.truncate(MAX_REAL_LOOPBACK_RECORDS);
    records
}

#[cfg(not(windows))]
fn read_windows_loopback_listener_records(
    _collection: &mut WindowsProviderCollection,
) -> Vec<WindowsLoopbackListenerRecord> {
    Vec::new()
}

#[cfg(windows)]
fn read_windows_loopback_listener_records_v4(
    collection: &mut WindowsProviderCollection,
    records: &mut Vec<WindowsLoopbackListenerRecord>,
) {
    use windows_sys::Win32::Foundation::{ERROR_INSUFFICIENT_BUFFER, ERROR_SUCCESS};
    use windows_sys::Win32::NetworkManagement::IpHelper::{
        GetExtendedTcpTable, MIB_TCPTABLE_OWNER_PID, MIB_TCP_STATE_LISTEN,
        TCP_TABLE_OWNER_PID_LISTENER,
    };
    use windows_sys::Win32::Networking::WinSock::AF_INET;

    let mut size = 0u32;
    let status = unsafe {
        GetExtendedTcpTable(
            std::ptr::null_mut(),
            &mut size,
            0,
            AF_INET as u32,
            TCP_TABLE_OWNER_PID_LISTENER,
            0,
        )
    };
    if status != ERROR_INSUFFICIENT_BUFFER || size == 0 {
        if status != ERROR_SUCCESS {
            collection.push_diagnostic(
                ObservationSourceKind::LoopbackListener,
                DiscoveryDiagnosticCode::ProviderFailed,
            );
        }
        return;
    }
    let mut buffer = vec![0u8; size as usize];
    let status = unsafe {
        GetExtendedTcpTable(
            buffer.as_mut_ptr().cast(),
            &mut size,
            0,
            AF_INET as u32,
            TCP_TABLE_OWNER_PID_LISTENER,
            0,
        )
    };
    if status != ERROR_SUCCESS {
        collection.push_diagnostic(
            ObservationSourceKind::LoopbackListener,
            DiscoveryDiagnosticCode::ProviderFailed,
        );
        return;
    }
    let table = unsafe { &*(buffer.as_ptr().cast::<MIB_TCPTABLE_OWNER_PID>()) };
    let rows =
        unsafe { std::slice::from_raw_parts(table.table.as_ptr(), table.dwNumEntries as usize) };
    for row in rows {
        if records.len() >= MAX_REAL_LOOPBACK_RECORDS {
            break;
        }
        if row.dwState != MIB_TCP_STATE_LISTEN as u32 {
            continue;
        }
        let row_address = std::net::Ipv4Addr::from(row.dwLocalAddr.to_ne_bytes());
        if !is_allowed_loopback_ip(&IpAddr::V4(row_address)) {
            continue;
        }
        let result = verify_windows_loopback_listener_row_v4(
            row_address,
            u16::from_be(row.dwLocalPort as u16),
            row.dwOwningPid,
        );
        push_loopback_verification_result(collection, records, result);
    }
}

#[cfg(windows)]
fn read_windows_loopback_listener_records_v6(
    collection: &mut WindowsProviderCollection,
    records: &mut Vec<WindowsLoopbackListenerRecord>,
) {
    use windows_sys::Win32::Foundation::{ERROR_INSUFFICIENT_BUFFER, ERROR_SUCCESS};
    use windows_sys::Win32::NetworkManagement::IpHelper::{
        GetExtendedTcpTable, MIB_TCP6TABLE_OWNER_PID, MIB_TCP_STATE_LISTEN,
        TCP_TABLE_OWNER_PID_LISTENER,
    };
    use windows_sys::Win32::Networking::WinSock::AF_INET6;

    let mut size = 0u32;
    let status = unsafe {
        GetExtendedTcpTable(
            std::ptr::null_mut(),
            &mut size,
            0,
            AF_INET6 as u32,
            TCP_TABLE_OWNER_PID_LISTENER,
            0,
        )
    };
    if status != ERROR_INSUFFICIENT_BUFFER || size == 0 {
        if status != ERROR_SUCCESS {
            collection.push_diagnostic(
                ObservationSourceKind::LoopbackListener,
                DiscoveryDiagnosticCode::ProviderFailed,
            );
        }
        return;
    }
    let mut buffer = vec![0u8; size as usize];
    let status = unsafe {
        GetExtendedTcpTable(
            buffer.as_mut_ptr().cast(),
            &mut size,
            0,
            AF_INET6 as u32,
            TCP_TABLE_OWNER_PID_LISTENER,
            0,
        )
    };
    if status != ERROR_SUCCESS {
        collection.push_diagnostic(
            ObservationSourceKind::LoopbackListener,
            DiscoveryDiagnosticCode::ProviderFailed,
        );
        return;
    }
    let table = unsafe { &*(buffer.as_ptr().cast::<MIB_TCP6TABLE_OWNER_PID>()) };
    let rows =
        unsafe { std::slice::from_raw_parts(table.table.as_ptr(), table.dwNumEntries as usize) };
    for row in rows {
        if records.len() >= MAX_REAL_LOOPBACK_RECORDS {
            break;
        }
        if row.dwState != MIB_TCP_STATE_LISTEN as u32 {
            continue;
        }
        let row_address = std::net::Ipv6Addr::from(row.ucLocalAddr);
        if !is_allowed_loopback_ip(&IpAddr::V6(row_address)) {
            continue;
        }
        let result = verify_windows_loopback_listener_row_v6(
            row_address,
            u16::from_be(row.dwLocalPort as u16),
            row.dwOwningPid,
        );
        push_loopback_verification_result(collection, records, result);
    }
}

#[cfg(windows)]
fn push_loopback_verification_result(
    collection: &mut WindowsProviderCollection,
    records: &mut Vec<WindowsLoopbackListenerRecord>,
    result: Result<Option<WindowsLoopbackListenerRecord>, DiscoveryDiagnosticCode>,
) {
    match result {
        Ok(Some(record)) => records.push(record),
        Ok(None) => collection.push_diagnostic(
            ObservationSourceKind::LoopbackListener,
            DiscoveryDiagnosticCode::SourceDisappeared,
        ),
        Err(code) => collection.push_diagnostic(ObservationSourceKind::LoopbackListener, code),
    }
}

#[cfg(windows)]
fn verify_windows_loopback_listener_row_v4(
    address: std::net::Ipv4Addr,
    port: u16,
    pid: u32,
) -> Result<Option<WindowsLoopbackListenerRecord>, DiscoveryDiagnosticCode> {
    let (owner_identity, owner_executable) = process_identity_for_pid(pid)?;
    if !loopback_row_survives_recheck_v4(address, port, pid, &owner_identity, &owner_executable)? {
        return Ok(None);
    }
    Ok(Some(WindowsLoopbackListenerRecord {
        address: address.to_string(),
        port,
        owner_pid: pid,
        owner_executable: Some(owner_executable),
        owner_identity: Some(owner_identity),
    }))
}

#[cfg(windows)]
fn verify_windows_loopback_listener_row_v6(
    address: std::net::Ipv6Addr,
    port: u16,
    pid: u32,
) -> Result<Option<WindowsLoopbackListenerRecord>, DiscoveryDiagnosticCode> {
    let (owner_identity, owner_executable) = process_identity_for_pid(pid)?;
    if !loopback_row_survives_recheck_v6(address, port, pid, &owner_identity, &owner_executable)? {
        return Ok(None);
    }
    Ok(Some(WindowsLoopbackListenerRecord {
        address: address.to_string(),
        port,
        owner_pid: pid,
        owner_executable: Some(owner_executable),
        owner_identity: Some(owner_identity),
    }))
}

#[cfg(windows)]
fn process_identity_for_pid(pid: u32) -> Result<(String, PathBuf), DiscoveryDiagnosticCode> {
    use std::ffi::OsString;
    use std::mem::zeroed;
    use std::os::windows::ffi::OsStringExt;
    use windows_sys::Win32::Foundation::{CloseHandle, FILETIME};
    use windows_sys::Win32::System::Threading::{
        GetProcessTimes, OpenProcess, QueryFullProcessImageNameW, PROCESS_QUERY_LIMITED_INFORMATION,
    };

    let process = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
    if process.is_null() {
        return Err(DiscoveryDiagnosticCode::AccessDenied);
    }
    struct ProcessHandle(windows_sys::Win32::Foundation::HANDLE);
    impl Drop for ProcessHandle {
        fn drop(&mut self) {
            unsafe {
                CloseHandle(self.0);
            }
        }
    }
    let process = ProcessHandle(process);
    let mut creation: FILETIME = unsafe { zeroed() };
    let mut exit_time: FILETIME = unsafe { zeroed() };
    let mut kernel_time: FILETIME = unsafe { zeroed() };
    let mut user_time: FILETIME = unsafe { zeroed() };
    if unsafe {
        GetProcessTimes(
            process.0,
            &mut creation,
            &mut exit_time,
            &mut kernel_time,
            &mut user_time,
        )
    } == 0
    {
        return Err(DiscoveryDiagnosticCode::SourceDisappeared);
    }
    let mut buffer = vec![0u16; 32768];
    let mut chars = buffer.len() as u32;
    if unsafe { QueryFullProcessImageNameW(process.0, 0, buffer.as_mut_ptr(), &mut chars) } == 0 {
        return Err(DiscoveryDiagnosticCode::AccessDenied);
    }
    let mut creation_after: FILETIME = unsafe { zeroed() };
    if unsafe {
        GetProcessTimes(
            process.0,
            &mut creation_after,
            &mut exit_time,
            &mut kernel_time,
            &mut user_time,
        )
    } == 0
        || creation_after.dwLowDateTime != creation.dwLowDateTime
        || creation_after.dwHighDateTime != creation.dwHighDateTime
    {
        return Err(DiscoveryDiagnosticCode::SourceDisappeared);
    }
    let identity = format!(
        "{:08x}:{:08x}:{:08x}",
        creation.dwHighDateTime, creation.dwLowDateTime, pid
    );
    Ok((
        identity,
        PathBuf::from(OsString::from_wide(&buffer[..chars as usize])),
    ))
}

#[cfg(windows)]
fn loopback_row_survives_recheck_v4(
    address: std::net::Ipv4Addr,
    port: u16,
    pid: u32,
    owner_identity: &str,
    owner_executable: &Path,
) -> Result<bool, DiscoveryDiagnosticCode> {
    use windows_sys::Win32::Foundation::{ERROR_INSUFFICIENT_BUFFER, ERROR_SUCCESS};
    use windows_sys::Win32::NetworkManagement::IpHelper::{
        GetExtendedTcpTable, MIB_TCPTABLE_OWNER_PID, MIB_TCP_STATE_LISTEN,
        TCP_TABLE_OWNER_PID_LISTENER,
    };
    use windows_sys::Win32::Networking::WinSock::AF_INET;

    let mut size = 0u32;
    let status = unsafe {
        GetExtendedTcpTable(
            std::ptr::null_mut(),
            &mut size,
            0,
            AF_INET as u32,
            TCP_TABLE_OWNER_PID_LISTENER,
            0,
        )
    };
    if status != ERROR_INSUFFICIENT_BUFFER || size == 0 {
        return Ok(false);
    }
    let mut buffer = vec![0u8; size as usize];
    let status = unsafe {
        GetExtendedTcpTable(
            buffer.as_mut_ptr().cast(),
            &mut size,
            0,
            AF_INET as u32,
            TCP_TABLE_OWNER_PID_LISTENER,
            0,
        )
    };
    if status != ERROR_SUCCESS {
        return Ok(false);
    }
    let table = unsafe { &*(buffer.as_ptr().cast::<MIB_TCPTABLE_OWNER_PID>()) };
    let rows =
        unsafe { std::slice::from_raw_parts(table.table.as_ptr(), table.dwNumEntries as usize) };
    let (current_identity, current_executable) = process_identity_for_pid(pid)?;
    Ok(rows.iter().any(|row| {
        row.dwState == MIB_TCP_STATE_LISTEN as u32
            && row.dwOwningPid == pid
            && u16::from_be(row.dwLocalPort as u16) == port
            && std::net::Ipv4Addr::from(row.dwLocalAddr.to_ne_bytes()) == address
            && current_identity == owner_identity
            && current_executable == owner_executable
    }))
}

#[cfg(windows)]
fn loopback_row_survives_recheck_v6(
    address: std::net::Ipv6Addr,
    port: u16,
    pid: u32,
    owner_identity: &str,
    owner_executable: &Path,
) -> Result<bool, DiscoveryDiagnosticCode> {
    use windows_sys::Win32::Foundation::{ERROR_INSUFFICIENT_BUFFER, ERROR_SUCCESS};
    use windows_sys::Win32::NetworkManagement::IpHelper::{
        GetExtendedTcpTable, MIB_TCP6TABLE_OWNER_PID, MIB_TCP_STATE_LISTEN,
        TCP_TABLE_OWNER_PID_LISTENER,
    };
    use windows_sys::Win32::Networking::WinSock::AF_INET6;

    let mut size = 0u32;
    let status = unsafe {
        GetExtendedTcpTable(
            std::ptr::null_mut(),
            &mut size,
            0,
            AF_INET6 as u32,
            TCP_TABLE_OWNER_PID_LISTENER,
            0,
        )
    };
    if status != ERROR_INSUFFICIENT_BUFFER || size == 0 {
        return Ok(false);
    }
    let mut buffer = vec![0u8; size as usize];
    let status = unsafe {
        GetExtendedTcpTable(
            buffer.as_mut_ptr().cast(),
            &mut size,
            0,
            AF_INET6 as u32,
            TCP_TABLE_OWNER_PID_LISTENER,
            0,
        )
    };
    if status != ERROR_SUCCESS {
        return Ok(false);
    }
    let table = unsafe { &*(buffer.as_ptr().cast::<MIB_TCP6TABLE_OWNER_PID>()) };
    let rows =
        unsafe { std::slice::from_raw_parts(table.table.as_ptr(), table.dwNumEntries as usize) };
    let (current_identity, current_executable) = process_identity_for_pid(pid)?;
    Ok(rows.iter().any(|row| {
        row.dwState == MIB_TCP_STATE_LISTEN as u32
            && row.dwOwningPid == pid
            && u16::from_be(row.dwLocalPort as u16) == port
            && std::net::Ipv6Addr::from(row.ucLocalAddr) == address
            && current_identity == owner_identity
            && current_executable == owner_executable
    }))
}

fn codex_observation(deadline: Instant, cancelled: &AtomicBool, binary: &Path) -> Observation {
    let evidence_summary = vec![
        DiscoveryEvidence::ExecutableInventory,
        DiscoveryEvidence::InstallKnown,
    ];
    let (file_identity, diagnostics) =
        match stable_file_identity_with_deadline(binary, deadline, cancelled) {
            Ok(identity) => (identity, Vec::new()),
            Err(code) => (
                stable_path_identity(binary),
                vec![DiscoveryDiagnostic {
                    source_kind: ObservationSourceKind::ExecutableInventory,
                    code,
                }],
            ),
        };
    Observation {
        locator: ObservationLocator::Executable(binary.to_path_buf()),
        fingerprint: ObservationFingerprint::from_parts(&[
            "codex-executable".into(),
            file_identity,
        ]),
        association_fingerprints: Vec::new(),
        package_ids: Vec::new(),
        runner_installation: None,
        source_kind: ObservationSourceKind::ExecutableInventory,
        category: CandidateCategory::AgentRuntime,
        trust_level: ObservationTrustLevel::Heuristic,
        verification_authority: VerificationAuthority::Unverified,
        availability_authority: VerificationAuthority::Unverified,
        discovery_authority: VerificationAuthority::Unverified,
        compatibility_authority: VerificationAuthority::Unverified,
        auth_authority: VerificationAuthority::Unverified,
        health_authority: VerificationAuthority::Unverified,
        connector_id: LOCAL_DISCOVERY_CODEX_CONNECTOR_ID.into(),
        runtime_type: "codex".into(),
        display_name: "Codex (local executable)".into(),
        availability: CandidateAvailability::Unconfigured,
        models: Vec::new(),
        catalog_revision: None,
        requires_configuration: true,
        discovery_state: DiscoveryState::Identified,
        compatibility_state: CompatibilityState::NotVerified,
        auth_state: AuthState::Unknown,
        health_state: HealthState::NotChecked,
        evidence_summary,
        diagnostics,
    }
}

fn kun_observation(
    config: &LocalConnectorDiscoveryConfig,
    record: LocalKunRecord,
    allow_active_verification: bool,
) -> Observation {
    let evidence_summary = kun_evidence_summary(config, &record);
    let unavailable = |availability: CandidateAvailability,
                       discovery_state: DiscoveryState,
                       compatibility_state: CompatibilityState,
                       auth_state: AuthState,
                       health_state: HealthState,
                       extra: DiscoveryEvidence| {
        let authority = if matches!(health_state, HealthState::IdentityMismatch)
            || matches!(compatibility_state, CompatibilityState::Incompatible)
        {
            VerificationAuthority::Authoritative
        } else {
            VerificationAuthority::Unverified
        };
        let trust_level = if matches!(authority, VerificationAuthority::Authoritative) {
            ObservationTrustLevel::FirstParty
        } else {
            ObservationTrustLevel::Heuristic
        };
        Observation {
            locator: ObservationLocator::RuntimeRecord {
                runtime_json: record.runtime_json.clone(),
            },
            fingerprint: kun_fingerprint(&record),
            association_fingerprints: Vec::new(),
            package_ids: Vec::new(),
            runner_installation: None,
            source_kind: ObservationSourceKind::RuntimeRecord,
            category: CandidateCategory::AgentRuntime,
            trust_level,
            verification_authority: authority,
            availability_authority: authority,
            discovery_authority: authority,
            compatibility_authority: authority,
            auth_authority: authority,
            health_authority: authority,
            connector_id: LOCAL_DISCOVERY_KUN_CONNECTOR_ID.into(),
            runtime_type: "kun".into(),
            display_name: "Kun Shared Runtime".into(),
            availability,
            models: Vec::new(),
            catalog_revision: None,
            requires_configuration: true,
            discovery_state,
            compatibility_state,
            auth_state,
            health_state,
            evidence_summary: {
                let mut evidence = evidence_summary.clone();
                evidence.push(extra);
                evidence
            },
            diagnostics: record.diagnostics.clone(),
        }
    };

    if !record.parsed {
        return unavailable(
            CandidateAvailability::Unavailable,
            DiscoveryState::Observed,
            CompatibilityState::NotVerified,
            AuthState::Unknown,
            HealthState::Unavailable,
            DiscoveryEvidence::CatalogUnavailable,
        );
    }
    if record.instance_id.is_none() {
        return unavailable(
            CandidateAvailability::Unavailable,
            DiscoveryState::Identified,
            CompatibilityState::Incompatible,
            AuthState::Unknown,
            HealthState::Unavailable,
            DiscoveryEvidence::IdentityMismatch,
        );
    }
    if !allow_active_verification {
        return unavailable(
            CandidateAvailability::Unconfigured,
            DiscoveryState::Identified,
            CompatibilityState::NotVerified,
            AuthState::Unknown,
            HealthState::NotChecked,
            DiscoveryEvidence::Unconfigured,
        );
    }

    let runtime = KunSharedRuntime::with_config(KunSharedRuntimeConfig {
        data_dir: record.runtime_json.parent().map(Path::to_path_buf),
        install_dir: config.kun_install_dirs.first().cloned(),
        default_model: None,
        expected_service_version: record
            .service_version
            .clone()
            .unwrap_or_else(|| config.kun_expected_service_version.clone()),
        expected_build_id: None,
        request_timeout: config.request_timeout.min(MAX_TRANSPORT_SETUP_TIMEOUT),
    });
    match runtime.list_models_checked() {
        Ok(models) => Observation {
            locator: ObservationLocator::RuntimeRecord {
                runtime_json: record.runtime_json.clone(),
            },
            fingerprint: kun_fingerprint(&record),
            association_fingerprints: Vec::new(),
            package_ids: Vec::new(),
            runner_installation: None,
            source_kind: ObservationSourceKind::RuntimeRecord,
            category: CandidateCategory::AgentRuntime,
            trust_level: ObservationTrustLevel::FirstParty,
            verification_authority: VerificationAuthority::Authoritative,
            availability_authority: VerificationAuthority::Authoritative,
            discovery_authority: VerificationAuthority::Authoritative,
            compatibility_authority: VerificationAuthority::Authoritative,
            auth_authority: VerificationAuthority::Authoritative,
            health_authority: VerificationAuthority::Authoritative,
            connector_id: LOCAL_DISCOVERY_KUN_CONNECTOR_ID.into(),
            runtime_type: "kun".into(),
            display_name: "Kun Shared Runtime".into(),
            availability: CandidateAvailability::Available,
            models,
            catalog_revision: runtime
                .catalog_revision()
                .and_then(|revision| safe_model_identifier(&revision)),
            requires_configuration: false,
            discovery_state: DiscoveryState::Identified,
            compatibility_state: CompatibilityState::Compatible,
            auth_state: AuthState::Ready,
            health_state: HealthState::Ready,
            evidence_summary: {
                let mut evidence = evidence_summary.clone();
                evidence.push(DiscoveryEvidence::Available);
                evidence
            },
            diagnostics: record.diagnostics.clone(),
        },
        Err(error) => match connector_runtime_failure(&error) {
            Some(ConnectorRuntimeFailure::RuntimeAuthenticationFailed) => unavailable(
                CandidateAvailability::AuthenticationRequired,
                DiscoveryState::Identified,
                CompatibilityState::NotVerified,
                AuthState::Required,
                HealthState::Unavailable,
                DiscoveryEvidence::AuthenticationRequired,
            ),
            Some(ConnectorRuntimeFailure::CatalogUnavailable) => unavailable(
                CandidateAvailability::Unconfigured,
                DiscoveryState::Identified,
                CompatibilityState::NotVerified,
                AuthState::Unknown,
                HealthState::Unavailable,
                DiscoveryEvidence::Unconfigured,
            ),
            Some(ConnectorRuntimeFailure::RuntimeIdentityMismatch) => unavailable(
                CandidateAvailability::Unavailable,
                DiscoveryState::Identified,
                CompatibilityState::Incompatible,
                AuthState::Ready,
                HealthState::IdentityMismatch,
                DiscoveryEvidence::IdentityMismatch,
            ),
            Some(ConnectorRuntimeFailure::RuntimeUnavailable)
            | Some(ConnectorRuntimeFailure::SharedRuntimeUnavailable)
            | Some(ConnectorRuntimeFailure::ProviderAuthenticationFailed)
            | None => unavailable(
                CandidateAvailability::Unavailable,
                DiscoveryState::Identified,
                CompatibilityState::NotVerified,
                AuthState::Unknown,
                HealthState::Unavailable,
                DiscoveryEvidence::CatalogUnavailable,
            ),
        },
    }
}

fn kun_fingerprint(record: &LocalKunRecord) -> ObservationFingerprint {
    let instance_identity = record.instance_id.clone().unwrap_or_else(|| {
        stable_path_identity(
            record
                .runtime_json
                .parent()
                .unwrap_or(record.runtime_json.as_path()),
        )
    });
    ObservationFingerprint::from_parts(&[
        "kun-shared-runtime".into(),
        instance_identity,
        record
            .service_version
            .clone()
            .unwrap_or_else(|| "unknown".into()),
        record.build_id.clone().unwrap_or_else(|| "unknown".into()),
    ])
}

#[cfg(test)]
fn stable_file_identity(path: &Path) -> Result<String, DiscoveryDiagnosticCode> {
    stable_file_identity_with_deadline(
        path,
        Instant::now() + Duration::from_secs(2),
        &AtomicBool::new(false),
    )
}

fn stable_file_identity_with_deadline(
    path: &Path,
    deadline: Instant,
    cancelled: &AtomicBool,
) -> Result<String, DiscoveryDiagnosticCode> {
    stable_file_fingerprint_with_deadline(path, deadline, cancelled)
        .map(|fingerprint| fingerprint.stable_identity)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StableFileFingerprint {
    pub(crate) stable_identity: String,
    pub(crate) content_sha256: String,
}

pub(crate) fn stable_file_fingerprint_with_deadline(
    path: &Path,
    deadline: Instant,
    cancelled: &AtomicBool,
) -> Result<StableFileFingerprint, DiscoveryDiagnosticCode> {
    stable_file_fingerprint_with_read_hook(path, deadline, cancelled, &mut |_| {})
}

#[cfg(test)]
fn stable_file_identity_with_read_hook(
    path: &Path,
    deadline: Instant,
    cancelled: &AtomicBool,
    on_read: &mut dyn FnMut(u64),
) -> Result<String, DiscoveryDiagnosticCode> {
    stable_file_fingerprint_with_read_hook(path, deadline, cancelled, on_read)
        .map(|fingerprint| fingerprint.stable_identity)
}

fn stable_file_fingerprint_with_read_hook(
    path: &Path,
    deadline: Instant,
    cancelled: &AtomicBool,
    on_read: &mut dyn FnMut(u64),
) -> Result<StableFileFingerprint, DiscoveryDiagnosticCode> {
    let mut file = open_fingerprint_snapshot_file(path)?;
    fingerprint_from_open_file(&mut file, deadline, cancelled, on_read)
}

/// Computes the stable identity + content SHA from an already-open snapshot
/// handle without taking ownership, so a caller can keep the handle alive
/// (and thereby deny concurrent write/delete/replace) after the fingerprint
/// is computed.
fn fingerprint_from_open_file(
    file: &mut fs::File,
    deadline: Instant,
    cancelled: &AtomicBool,
    on_read: &mut dyn FnMut(u64),
) -> Result<StableFileFingerprint, DiscoveryDiagnosticCode> {
    let before_snapshot = FingerprintMetadataSnapshot::from_file(file)?;
    let mut hasher = Sha256::new();
    hasher.update(before_snapshot.len.to_le_bytes());
    let read = match hash_reader_exact_until_with_hook(
        file,
        before_snapshot.len,
        &mut hasher,
        deadline,
        cancelled,
        on_read,
    ) {
        Ok(read) => read,
        Err(code) => {
            if code == DiscoveryDiagnosticCode::FingerprintUnavailable {
                return Err(DiscoveryDiagnosticCode::FingerprintChanged);
            }
            if let Ok(after_snapshot) = FingerprintMetadataSnapshot::from_file(file) {
                if Some(after_snapshot)
                    .map(|after_snapshot| after_snapshot != before_snapshot)
                    .unwrap_or(false)
                {
                    return Err(DiscoveryDiagnosticCode::FingerprintChanged);
                }
            }
            return Err(code);
        }
    };
    if read != before_snapshot.len {
        return Err(DiscoveryDiagnosticCode::FingerprintChanged);
    }
    let after_snapshot = FingerprintMetadataSnapshot::from_file(file)?;
    if after_snapshot != before_snapshot {
        return Err(DiscoveryDiagnosticCode::FingerprintChanged);
    }
    let content_sha256 = sha256_hex(&hasher.finalize());
    let mut identity_hasher = Sha256::new();
    identity_hasher.update(b"agenttalk-windows-executable-identity-v2");
    before_snapshot.update_private_identity_hash(&mut identity_hasher);
    identity_hasher.update(content_sha256.as_bytes());
    Ok(StableFileFingerprint {
        stable_identity: sha256_hex(&identity_hasher.finalize()),
        content_sha256,
    })
}

/// Holds the snapshot file handle open with a share mode that denies
/// concurrent write/delete, so the executable cannot be replaced between the
/// identity recheck and `CreateProcessW`. Dropping the guard releases the
/// handle; it must outlive the suspended child creation.
pub(crate) struct VerifiedExecutableGuard {
    _file: fs::File,
    pub(crate) fingerprint: StableFileFingerprint,
}

/// Opens the executable with a delete/write-denying share mode and computes
/// its fingerprint from that same handle, returning a guard that keeps the
/// handle open. The guard must stay alive until the child process is created.
pub(crate) fn open_verified_executable_guard(
    path: &Path,
    deadline: Instant,
    cancelled: &AtomicBool,
) -> Result<VerifiedExecutableGuard, DiscoveryDiagnosticCode> {
    let mut file = open_fingerprint_snapshot_file(path)?;
    let fingerprint = fingerprint_from_open_file(&mut file, deadline, cancelled, &mut |_| {})?;
    Ok(VerifiedExecutableGuard {
        _file: file,
        fingerprint,
    })
}

#[cfg(windows)]
fn open_fingerprint_snapshot_file(path: &Path) -> Result<fs::File, DiscoveryDiagnosticCode> {
    use std::os::windows::io::FromRawHandle;
    use windows_sys::Win32::Foundation::{GENERIC_READ, INVALID_HANDLE_VALUE};
    use windows_sys::Win32::Storage::FileSystem::{
        CreateFileW, FILE_ATTRIBUTE_NORMAL, FILE_FLAG_SEQUENTIAL_SCAN, FILE_SHARE_READ,
        OPEN_EXISTING,
    };

    let mut path = wide_null(path.as_os_str());
    let handle = unsafe {
        CreateFileW(
            path.as_mut_ptr(),
            GENERIC_READ,
            FILE_SHARE_READ,
            std::ptr::null(),
            OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL | FILE_FLAG_SEQUENTIAL_SCAN,
            std::ptr::null_mut(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(DiscoveryDiagnosticCode::FingerprintUnavailable);
    }
    Ok(unsafe { fs::File::from_raw_handle(handle.cast()) })
}

#[cfg(not(windows))]
fn open_fingerprint_snapshot_file(path: &Path) -> Result<fs::File, DiscoveryDiagnosticCode> {
    fs::File::open(path).map_err(|_| DiscoveryDiagnosticCode::FingerprintUnavailable)
}

#[cfg(windows)]
fn wide_null(value: &std::ffi::OsStr) -> Vec<u16> {
    use std::os::windows::ffi::OsStrExt;
    value.encode_wide().chain(std::iter::once(0)).collect()
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FingerprintMetadataSnapshot {
    len: u64,
    modified: std::time::SystemTime,
    #[cfg(windows)]
    file_identity: WindowsFileIdentity,
}

impl FingerprintMetadataSnapshot {
    fn from_file(file: &std::fs::File) -> Result<Self, DiscoveryDiagnosticCode> {
        let metadata = file
            .metadata()
            .map_err(|_| DiscoveryDiagnosticCode::FingerprintUnavailable)?;
        Ok(Self {
            len: metadata.len(),
            modified: metadata
                .modified()
                .map_err(|_| DiscoveryDiagnosticCode::FingerprintUnavailable)?,
            #[cfg(windows)]
            file_identity: WindowsFileIdentity::from_file(file)?,
        })
    }

    fn update_private_identity_hash(&self, hasher: &mut Sha256) {
        hasher.update(self.len.to_le_bytes());
        #[cfg(windows)]
        self.file_identity.update_hash(hasher);
    }
}

#[cfg(windows)]
#[derive(Clone, Debug, Eq, PartialEq)]
struct WindowsFileIdentity {
    volume_serial_number: u32,
    file_index_high: u32,
    file_index_low: u32,
    creation_time_high: u32,
    creation_time_low: u32,
}

#[cfg(windows)]
impl WindowsFileIdentity {
    fn from_file(file: &std::fs::File) -> Result<Self, DiscoveryDiagnosticCode> {
        use std::mem::zeroed;
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Storage::FileSystem::{
            GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION,
        };

        let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { zeroed() };
        let ok = unsafe { GetFileInformationByHandle(file.as_raw_handle(), &mut info) };
        if ok == 0 {
            return Err(DiscoveryDiagnosticCode::FingerprintUnavailable);
        }
        Ok(Self {
            volume_serial_number: info.dwVolumeSerialNumber,
            file_index_high: info.nFileIndexHigh,
            file_index_low: info.nFileIndexLow,
            creation_time_high: info.ftCreationTime.dwHighDateTime,
            creation_time_low: info.ftCreationTime.dwLowDateTime,
        })
    }

    fn update_hash(&self, hasher: &mut Sha256) {
        hasher.update(self.volume_serial_number.to_le_bytes());
        hasher.update(self.file_index_high.to_le_bytes());
        hasher.update(self.file_index_low.to_le_bytes());
        hasher.update(self.creation_time_high.to_le_bytes());
        hasher.update(self.creation_time_low.to_le_bytes());
    }
}

#[cfg(test)]
fn stable_file_identity_from_reader<R: Read>(
    mut reader: R,
    len: u64,
    _modified: u128,
    _max_bytes: u64,
    max_time: Duration,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(len.to_le_bytes());
    let cancelled = AtomicBool::new(false);
    let _ = hash_reader_exact_until_with_hook(
        &mut reader,
        len,
        &mut hasher,
        Instant::now() + max_time,
        &cancelled,
        &mut |_| {},
    );
    sha256_hex(&hasher.finalize())
}

fn hash_reader_exact_until_with_hook<R: Read>(
    reader: &mut R,
    expected_bytes: u64,
    hasher: &mut Sha256,
    deadline: Instant,
    cancelled: &AtomicBool,
    on_read: &mut dyn FnMut(u64),
) -> Result<u64, DiscoveryDiagnosticCode> {
    let mut total = 0u64;
    let mut buffer = [0u8; 8192];
    while total < expected_bytes {
        if cancelled.load(Ordering::Acquire) || Instant::now() >= deadline {
            return Err(DiscoveryDiagnosticCode::ProviderTimeout);
        }
        let remaining = (expected_bytes - total).min(buffer.len() as u64) as usize;
        let read = reader
            .read(&mut buffer[..remaining])
            .map_err(|_| DiscoveryDiagnosticCode::FingerprintUnavailable)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        total += read as u64;
        on_read(total);
    }
    Ok(total)
}

fn stable_path_identity(path: &Path) -> String {
    let normalized = path
        .to_string_lossy()
        .replace('\\', "/")
        .to_ascii_lowercase();
    let mut hasher = Sha256::new();
    hasher.update(normalized.as_bytes());
    sha256_hex(&hasher.finalize())
}

fn sha256_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn legacy_local_connector_candidate(candidate: CandidateProjection) -> LocalConnectorCandidate {
    LocalConnectorCandidate {
        connector_id: candidate.connector_id,
        runtime_type: candidate.runtime_type,
        display_name: candidate.display_name,
        availability: candidate.availability.as_str().into(),
        models: candidate.models,
        catalog_revision: candidate.catalog_revision,
        source: candidate.source_kind,
        requires_configuration: candidate.requires_configuration,
    }
}

#[derive(Default)]
struct PassiveKunRuntimeRecord {
    port: Option<u16>,
    instance_id: Option<String>,
    service_version: Option<String>,
    build_id: Option<String>,
}

impl<'de> Deserialize<'de> for PassiveKunRuntimeRecord {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(PassiveKunRuntimeRecordVisitor)
    }
}

struct PassiveKunRuntimeRecordVisitor;

impl<'de> Visitor<'de> for PassiveKunRuntimeRecordVisitor {
    type Value = PassiveKunRuntimeRecord;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a Kun passive runtime record object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut record = PassiveKunRuntimeRecord::default();
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "port" => {
                    let value = map.next_value::<Option<u64>>()?;
                    record.port = value
                        .and_then(|port| u16::try_from(port).ok())
                        .filter(|p| *p > 0);
                }
                "instanceId" => {
                    let value = map.next_value::<Option<String>>()?;
                    record.instance_id = value.as_deref().and_then(safe_model_identifier);
                }
                "serviceVersion" => {
                    let value = map.next_value::<Option<String>>()?;
                    record.service_version = value.as_deref().and_then(safe_model_identifier);
                }
                "buildId" => {
                    let value = map.next_value::<Option<String>>()?;
                    record.build_id = value.as_deref().and_then(safe_model_identifier);
                }
                "runtimeToken" | "apiKey" | "Authorization" | "authorization" | "Cookie"
                | "cookie" => {
                    let _ = map.next_value::<IgnoredAny>()?;
                }
                _ => {
                    let _ = map.next_value::<IgnoredAny>()?;
                }
            }
        }
        Ok(record)
    }
}

struct BoundedJsonReader<R> {
    inner: R,
    limit: usize,
    read: usize,
    oversized: bool,
}

impl<R: Read> BoundedJsonReader<R> {
    fn new(inner: R, limit: usize) -> Self {
        Self {
            inner,
            limit,
            read: 0,
            oversized: false,
        }
    }

    fn oversized(&self) -> bool {
        self.oversized
    }
}

impl<R: Read> Read for BoundedJsonReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.read > self.limit {
            self.oversized = true;
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "bounded json input exceeded",
            ));
        }
        let remaining = self.limit + 1 - self.read;
        let max = remaining.min(buf.len());
        let read = self.inner.read(&mut buf[..max])?;
        self.read += read;
        if self.read > self.limit {
            self.oversized = true;
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "bounded json input exceeded",
            ));
        }
        Ok(read)
    }
}

fn parse_passive_kun_runtime_record_from_reader<R: Read>(
    reader: R,
    limit: usize,
) -> Result<PassiveKunRuntimeRecord, DiscoveryDiagnosticCode> {
    let mut bounded = BoundedJsonReader::new(reader, limit);
    let parsed = {
        let mut deserializer = serde_json::Deserializer::from_reader(&mut bounded);
        let record = PassiveKunRuntimeRecord::deserialize(&mut deserializer);
        record.and_then(|record| {
            deserializer.end()?;
            Ok(record)
        })
    };
    if bounded.oversized() {
        Err(DiscoveryDiagnosticCode::OversizedInput)
    } else {
        parsed.map_err(|_| DiscoveryDiagnosticCode::ProviderFailed)
    }
}

fn read_local_kun_record(data_dir: &Path) -> Option<LocalKunRecord> {
    let runtime_json = data_dir.join("runtime.json");
    let file = match fs::File::open(&runtime_json) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return None,
        Err(_) => {
            return Some(LocalKunRecord {
                runtime_json,
                _port: None,
                instance_id: None,
                service_version: None,
                build_id: None,
                parsed: false,
                diagnostics: vec![DiscoveryDiagnostic {
                    source_kind: ObservationSourceKind::RuntimeRecord,
                    code: DiscoveryDiagnosticCode::FingerprintUnavailable,
                }],
            });
        }
    };
    if file
        .metadata()
        .map(|metadata| metadata.len() > MAX_TRANSPORT_BODY_BYTES as u64)
        .unwrap_or(false)
    {
        return Some(LocalKunRecord {
            runtime_json,
            _port: None,
            instance_id: None,
            service_version: None,
            build_id: None,
            parsed: false,
            diagnostics: vec![DiscoveryDiagnostic {
                source_kind: ObservationSourceKind::RuntimeRecord,
                code: DiscoveryDiagnosticCode::OversizedInput,
            }],
        });
    }
    let reader = std::io::BufReader::new(file);
    let parsed_record =
        match parse_passive_kun_runtime_record_from_reader(reader, MAX_TRANSPORT_BODY_BYTES) {
            Ok(record) => record,
            Err(code) => {
                return Some(LocalKunRecord {
                    runtime_json,
                    _port: None,
                    instance_id: None,
                    service_version: None,
                    build_id: None,
                    parsed: false,
                    diagnostics: vec![DiscoveryDiagnostic {
                        source_kind: ObservationSourceKind::RuntimeRecord,
                        code,
                    }],
                })
            }
        };
    let instance_id = parsed_record.instance_id;
    let mut diagnostics = Vec::new();
    if instance_id.is_none() {
        diagnostics.push(DiscoveryDiagnostic {
            source_kind: ObservationSourceKind::RuntimeRecord,
            code: DiscoveryDiagnosticCode::InvalidIdentity,
        });
    }
    Some(LocalKunRecord {
        runtime_json,
        _port: parsed_record.port,
        instance_id,
        service_version: parsed_record.service_version,
        build_id: parsed_record.build_id,
        parsed: true,
        diagnostics,
    })
}

fn kun_evidence_summary(
    _config: &LocalConnectorDiscoveryConfig,
    _record: &LocalKunRecord,
) -> Vec<DiscoveryEvidence> {
    vec![DiscoveryEvidence::RuntimeRecord]
}

fn push_unique_local_path(paths: &mut Vec<PathBuf>, candidate: PathBuf) {
    if !paths
        .iter()
        .any(|existing| same_local_path(existing, &candidate))
    {
        paths.push(candidate);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeHealth {
    pub runtime_id: String,
    pub status: String,
    pub detail: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderUsage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub cost_micros: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeErrorClass {
    Authentication,
    Permission,
    Timeout,
    Cancelled,
    Transport,
    Provider,
    Protocol,
    Unknown,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum RuntimeError {
    #[error("runtime is not configured")]
    NotConfigured,
    #[error("workspace access and cwd are inconsistent")]
    InvalidWorkspace,
    #[error("runtime is cancelled")]
    Cancelled,
    #[error("runtime deadline elapsed")]
    Timeout,
    #[error("runtime permission does not allow this operation")]
    Permission,
    #[error("runtime protocol error: {0}")]
    Protocol(String),
    #[error("runtime stream capacity must be greater than zero")]
    InvalidStreamCapacity,
    #[error("runtime stream buffer is full (capacity {capacity})")]
    StreamBufferFull { capacity: usize },
    #[error("runtime stream already has a terminal event")]
    StreamTerminal,
    #[error("runtime stream ended without a terminal event")]
    StreamTerminalMissing,
    #[error("runtime transport closed before a terminal event")]
    TransportClosed,
    #[error("runtime transport error: {0}")]
    Transport(String),
    #[error("runtime authentication failed")]
    Authentication,
    #[error("provider rejected the runtime request: {0}")]
    Provider(String),
    #[error("runtime adapter operation is not implemented")]
    Unsupported,
}

/// Stable, credential-free classifications for failures that can cross a
/// Connector/Core IPC boundary. The transport keeps raw bodies and credentials
/// private; callers expose only these fixed values.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorRuntimeFailure {
    RuntimeUnavailable,
    SharedRuntimeUnavailable,
    RuntimeAuthenticationFailed,
    RuntimeIdentityMismatch,
    CatalogUnavailable,
    ProviderAuthenticationFailed,
}

impl ConnectorRuntimeFailure {
    pub fn ipc_code(self) -> &'static str {
        match self {
            Self::RuntimeUnavailable => "CONNECTOR_RUNTIME_UNAVAILABLE",
            Self::SharedRuntimeUnavailable => "CONNECTOR_SHARED_RUNTIME_UNAVAILABLE",
            Self::RuntimeAuthenticationFailed => "CONNECTOR_RUNTIME_AUTHENTICATION_FAILED",
            Self::RuntimeIdentityMismatch => "CONNECTOR_RUNTIME_IDENTITY_MISMATCH",
            Self::CatalogUnavailable => "CONNECTOR_CATALOG_UNAVAILABLE",
            Self::ProviderAuthenticationFailed => "CONNECTOR_PROVIDER_AUTHENTICATION_FAILED",
        }
    }

    pub fn category(self) -> &'static str {
        match self {
            Self::RuntimeUnavailable => "connector_runtime_unavailable",
            Self::SharedRuntimeUnavailable => "shared_runtime_unavailable",
            Self::RuntimeAuthenticationFailed => "runtime_authentication_failed",
            Self::RuntimeIdentityMismatch => "runtime_identity_mismatch",
            Self::CatalogUnavailable => "connector_catalog_unavailable",
            Self::ProviderAuthenticationFailed => "provider_authentication_failed",
        }
    }

    pub fn message(self) -> &'static str {
        match self {
            Self::RuntimeUnavailable => "connector Runtime is unavailable",
            Self::SharedRuntimeUnavailable => "shared Runtime is unavailable",
            Self::RuntimeAuthenticationFailed => "connector Runtime authentication failed",
            Self::RuntimeIdentityMismatch => "connector Runtime identity does not match",
            Self::CatalogUnavailable => "connector Runtime catalog is unavailable",
            Self::ProviderAuthenticationFailed => "provider authentication failed",
        }
    }

    pub fn event_reason(self) -> &'static str {
        self.category()
    }
}

/// Converts a transport error to the narrow public Connector classification
/// only when it is safe and meaningful to do so. All free-form diagnostics are
/// intentionally left out of the result.
pub fn connector_runtime_failure(error: &RuntimeError) -> Option<ConnectorRuntimeFailure> {
    match error {
        RuntimeError::Authentication => Some(ConnectorRuntimeFailure::RuntimeAuthenticationFailed),
        RuntimeError::Transport(code) if code == KUN_SHARED_RUNTIME_UNAVAILABLE => {
            Some(ConnectorRuntimeFailure::SharedRuntimeUnavailable)
        }
        RuntimeError::Transport(code) if code == CODEX_RUNTIME_UNAVAILABLE => {
            Some(ConnectorRuntimeFailure::RuntimeUnavailable)
        }
        RuntimeError::Transport(code)
            if code == KUN_CATALOG_UNAVAILABLE || code == CODEX_CATALOG_UNAVAILABLE =>
        {
            Some(ConnectorRuntimeFailure::CatalogUnavailable)
        }
        RuntimeError::Protocol(code) if code == KUN_RUNTIME_IDENTITY_MISMATCH => {
            Some(ConnectorRuntimeFailure::RuntimeIdentityMismatch)
        }
        RuntimeError::Provider(code) if code == KUN_PROVIDER_AUTHENTICATION_FAILED => {
            Some(ConnectorRuntimeFailure::ProviderAuthenticationFailed)
        }
        _ => None,
    }
}

#[derive(Debug)]
enum StreamClose {
    Terminal,
    Error(RuntimeError),
}

#[derive(Debug)]
struct RuntimeEventStreamState {
    capacity: usize,
    queue: VecDeque<RuntimeEvent>,
    cancelled: bool,
    closed: Option<StreamClose>,
}

impl RuntimeEventStreamState {
    fn closed_error(&self) -> Option<RuntimeError> {
        match &self.closed {
            Some(StreamClose::Terminal) | None => None,
            Some(StreamClose::Error(error)) => Some(error.clone()),
        }
    }

    fn terminal_is_closed(&self) -> bool {
        matches!(self.closed, Some(StreamClose::Terminal))
    }
}

#[derive(Debug)]
pub struct RuntimeEventProducer {
    shared: Arc<(Mutex<RuntimeEventStreamState>, Condvar)>,
}

impl RuntimeEventProducer {
    fn lock_state(&self) -> MutexGuard<'_, RuntimeEventStreamState> {
        match (self.shared.0).lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn push_locked(
        &self,
        state: &mut RuntimeEventStreamState,
        event: RuntimeEvent,
    ) -> Result<(), RuntimeError> {
        if state.cancelled {
            return Err(RuntimeError::Cancelled);
        }
        if state.terminal_is_closed() {
            return Err(RuntimeError::StreamTerminal);
        }
        if let Some(error) = state.closed_error() {
            return Err(error);
        }
        if state.queue.len() == state.capacity {
            return Err(RuntimeError::StreamBufferFull {
                capacity: state.capacity,
            });
        }

        let is_terminal = is_terminal_event(&event);
        state.queue.push_back(event);
        if is_terminal {
            state.closed = Some(StreamClose::Terminal);
        }
        self.shared.1.notify_all();
        Ok(())
    }

    /// Pushes an event, waiting for the consumer when the bounded queue is full.
    pub fn push(&self, event: RuntimeEvent) -> Result<(), RuntimeError> {
        let mut state = self.lock_state();
        while state.queue.len() == state.capacity && !state.cancelled && state.closed.is_none() {
            state = match self.shared.1.wait(state) {
                Ok(state) => state,
                Err(poisoned) => poisoned.into_inner(),
            };
        }
        self.push_locked(&mut state, event)
    }

    /// Attempts to push without waiting. `StreamBufferFull` is the producer
    /// over-limit signal and leaves the event unconsumed.
    pub fn try_push(&self, event: RuntimeEvent) -> Result<(), RuntimeError> {
        let mut state = self.lock_state();
        self.push_locked(&mut state, event)
    }

    /// Marks producer completion. A stream can complete only after accepting
    /// exactly one terminal event.
    pub fn finish(&self) -> Result<(), RuntimeError> {
        let mut state = self.lock_state();
        if state.cancelled {
            return Err(RuntimeError::Cancelled);
        }
        if state.terminal_is_closed() {
            return Ok(());
        }
        if let Some(error) = state.closed_error() {
            return Err(error);
        }
        state.closed = Some(StreamClose::Error(RuntimeError::StreamTerminalMissing));
        self.shared.1.notify_all();
        Err(RuntimeError::StreamTerminalMissing)
    }

    /// Closes the transport without manufacturing a terminal event. Consumers
    /// receive `TransportClosed` after draining already-buffered events.
    pub fn close_transport(&self) -> Result<(), RuntimeError> {
        let mut state = self.lock_state();
        if state.cancelled {
            return Err(RuntimeError::Cancelled);
        }
        if state.terminal_is_closed() {
            return Ok(());
        }
        if let Some(error) = state.closed_error() {
            return Err(error);
        }
        state.closed = Some(StreamClose::Error(RuntimeError::TransportClosed));
        self.shared.1.notify_all();
        Ok(())
    }

    fn fail(&self, error: RuntimeError) -> Result<(), RuntimeError> {
        let mut state = self.lock_state();
        if state.cancelled {
            return Err(RuntimeError::Cancelled);
        }
        if state.terminal_is_closed() {
            return Err(RuntimeError::StreamTerminal);
        }
        if let Some(existing) = state.closed_error() {
            return Err(existing);
        }
        state.closed = Some(StreamClose::Error(error.clone()));
        self.shared.1.notify_all();
        Err(error)
    }
}

pub struct RuntimeEventStream {
    shared: Arc<(Mutex<RuntimeEventStreamState>, Condvar)>,
    producer_thread: Option<JoinHandle<()>>,
    /// Optional adapter-owned cancellation signal. This is intentionally
    /// separate from the bounded queue state so a Core timeout can interrupt
    /// a blocking transport read without fabricating a terminal event.
    cancel_callback: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl RuntimeEventStream {
    pub fn with_capacity(capacity: usize) -> Result<Self, RuntimeError> {
        if capacity == 0 {
            return Err(RuntimeError::InvalidStreamCapacity);
        }
        Ok(Self {
            shared: Arc::new((
                Mutex::new(RuntimeEventStreamState {
                    capacity,
                    queue: VecDeque::with_capacity(capacity),
                    cancelled: false,
                    closed: None,
                }),
                Condvar::new(),
            )),
            producer_thread: None,
            cancel_callback: None,
        })
    }

    pub fn channel(capacity: usize) -> Result<(Self, RuntimeEventProducer), RuntimeError> {
        let stream = Self::with_capacity(capacity)?;
        let producer = stream.producer();
        Ok((stream, producer))
    }

    pub fn spawn<F>(capacity: usize, produce: F) -> Result<Self, RuntimeError>
    where
        F: FnOnce(&RuntimeEventProducer) -> Result<(), RuntimeError> + Send + 'static,
    {
        let mut stream = Self::with_capacity(capacity)?;
        let producer = stream.producer();
        stream.producer_thread = Some(thread::spawn(move || {
            if let Err(error) = produce(&producer) {
                let _ = producer.fail(error);
            } else {
                let _ = producer.finish();
            }
        }));
        Ok(stream)
    }

    /// Like [`Self::spawn`], but invokes `on_cancel` exactly once when the
    /// consumer cancels the stream. The callback must be bounded and must not
    /// manufacture a Runtime terminal event: Core remains the source of truth
    /// for timeout and cancellation persistence.
    pub fn spawn_with_cancel<F, C>(
        capacity: usize,
        on_cancel: C,
        produce: F,
    ) -> Result<Self, RuntimeError>
    where
        F: FnOnce(&RuntimeEventProducer) -> Result<(), RuntimeError> + Send + 'static,
        C: Fn() + Send + Sync + 'static,
    {
        let mut stream = Self::with_capacity(capacity)?;
        stream.cancel_callback = Some(Arc::new(on_cancel));
        let producer = stream.producer();
        stream.producer_thread = Some(thread::spawn(move || {
            if let Err(error) = produce(&producer) {
                let _ = producer.fail(error);
            } else {
                let _ = producer.finish();
            }
        }));
        Ok(stream)
    }

    pub fn producer(&self) -> RuntimeEventProducer {
        RuntimeEventProducer {
            shared: Arc::clone(&self.shared),
        }
    }

    fn lock_state(&self) -> MutexGuard<'_, RuntimeEventStreamState> {
        match (self.shared.0).lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn closed_result(
        state: &RuntimeEventStreamState,
    ) -> Result<Option<RuntimeEvent>, RuntimeError> {
        if state.cancelled {
            return Err(RuntimeError::Cancelled);
        }
        if let Some(error) = state.closed_error() {
            return Err(error);
        }
        if state.terminal_is_closed() {
            return Ok(None);
        }
        Ok(None)
    }

    pub fn try_next(&self) -> Result<Option<RuntimeEvent>, RuntimeError> {
        let mut state = self.lock_state();
        if let Some(event) = state.queue.pop_front() {
            self.shared.1.notify_all();
            return Ok(Some(event));
        }
        Self::closed_result(&state)
    }

    pub fn next(&self) -> Result<Option<RuntimeEvent>, RuntimeError> {
        let mut state = self.lock_state();
        while state.queue.is_empty() && !state.cancelled && state.closed.is_none() {
            state = match self.shared.1.wait(state) {
                Ok(state) => state,
                Err(poisoned) => poisoned.into_inner(),
            };
        }
        if let Some(event) = state.queue.pop_front() {
            self.shared.1.notify_all();
            return Ok(Some(event));
        }
        Self::closed_result(&state)
    }

    /// Waits for one event until the supplied remaining deadline. A timeout
    /// never manufactures a terminal Runtime event; Core remains responsible
    /// for persisting the authoritative failed terminal state.
    pub fn next_timeout(&self, timeout: Duration) -> Result<Option<RuntimeEvent>, RuntimeError> {
        let state = self.lock_state();
        let (mut state, wait_result) =
            match self.shared.1.wait_timeout_while(state, timeout, |state| {
                state.queue.is_empty() && !state.cancelled && state.closed.is_none()
            }) {
                Ok(result) => result,
                Err(poisoned) => poisoned.into_inner(),
            };
        if let Some(event) = state.queue.pop_front() {
            self.shared.1.notify_all();
            return Ok(Some(event));
        }
        if state.cancelled || state.closed.is_some() {
            return Self::closed_result(&state);
        }
        if wait_result.timed_out() {
            return Err(RuntimeError::Timeout);
        }
        Err(RuntimeError::Timeout)
    }

    pub fn cancel(&self) -> Result<(), RuntimeError> {
        let mut state = self.lock_state();
        if state.terminal_is_closed() {
            return Err(RuntimeError::StreamTerminal);
        }
        if let Some(error) = state.closed_error() {
            return Err(error);
        }
        if !state.cancelled {
            state.cancelled = true;
            state.queue.clear();
            self.shared.1.notify_all();
            let callback = self.cancel_callback.clone();
            drop(state);
            if let Some(callback) = callback {
                callback();
            }
        }
        Ok(())
    }

    pub fn capacity(&self) -> usize {
        self.lock_state().capacity
    }

    pub fn buffered_len(&self) -> usize {
        self.lock_state().queue.len()
    }
}

impl Drop for RuntimeEventStream {
    fn drop(&mut self) {
        let _ = self.cancel();
        if let Some(thread) = self.producer_thread.take() {
            let _ = thread.join();
        }
    }
}

fn is_terminal_event(event: &RuntimeEvent) -> bool {
    matches!(
        event.event_type.as_str(),
        "execution.completed"
            | "execution.failed"
            | "execution.cancelled"
            | "execution.interrupted"
    )
}

pub trait RuntimeAdapter: Send {
    fn id(&self) -> &str;
    fn capabilities(&self) -> RuntimeCapabilities;
    fn discover(&self) -> RuntimeDiscovery {
        RuntimeDiscovery {
            runtime_id: self.id().into(),
            version: None,
            owned: false,
        }
    }
    fn health(&self) -> RuntimeHealth {
        RuntimeHealth {
            runtime_id: self.id().into(),
            status: "unknown".into(),
            detail: None,
        }
    }
    /// Performs the availability probe required before a Connector-bound
    /// catalog or execution. Legacy adapters inherit the health projection;
    /// transport-backed adapters override this to preserve a classified
    /// failure instead of reducing it to an unavailable status string.
    fn ensure_available(&self) -> Result<(), RuntimeError> {
        if matches!(
            self.health().status.as_str(),
            "available" | "ready" | "healthy"
        ) {
            Ok(())
        } else {
            Err(RuntimeError::Transport("runtime_unavailable".into()))
        }
    }
    fn list_models(&self) -> Vec<String> {
        Vec::new()
    }
    /// Credential-free catalog access for additive Connector routes. The
    /// legacy list keeps its historical empty-on-error surface, while this
    /// method preserves a safe classified transport error for Core/IPC.
    fn list_models_checked(&self) -> Result<Vec<String>, RuntimeError> {
        Ok(self.list_models())
    }
    /// Optional connector-supplied catalog revision. Returning `None` keeps
    /// the historical Core-derived revision behavior for legacy adapters.
    fn catalog_revision(&self) -> Option<u64> {
        None
    }
    /// Runtime-declared catalog default.  This deliberately has no
    /// sorted-first fallback: callers must not silently reinterpret catalog
    /// ordering as an authority decision.
    fn catalog_default_model_id(&self) -> Option<String> {
        None
    }
    /// Credential-free per-model metadata from the Runtime-owned catalog.
    /// Legacy adapters can omit it; Connector transports retain it exactly
    /// after safe normalization.
    fn catalog_model_metadata(&self, _model_id: &str) -> Option<RuntimeModelMetadata> {
        None
    }
    fn execute(&self, request: &RuntimeRequest) -> Result<Vec<RuntimeEvent>, RuntimeError>;
    /// Returns a bounded pull stream. The legacy `execute` method remains the
    /// batch/fixture contract; adapters must opt into this method to expose a
    /// producer that observes cancellation and backpressure.
    fn stream_events(&self, request: &RuntimeRequest) -> Result<RuntimeEventStream, RuntimeError> {
        self.stream_events_with_capacity(request, DEFAULT_RUNTIME_STREAM_CAPACITY)
    }
    fn stream_events_with_capacity(
        &self,
        _request: &RuntimeRequest,
        _capacity: usize,
    ) -> Result<RuntimeEventStream, RuntimeError> {
        Err(RuntimeError::Unsupported)
    }
    fn cancel(&self, request: &RuntimeRequest) -> Result<RuntimeEvent, RuntimeError>;
    fn shutdown_owned(&self) -> Result<(), RuntimeError> {
        Ok(())
    }
    fn version(&self) -> Option<String> {
        self.discover().version
    }
    fn usage(&self) -> ProviderUsage {
        ProviderUsage {
            input_tokens: None,
            output_tokens: None,
            cost_micros: None,
        }
    }
    fn classify_error(&self, error: &RuntimeError) -> RuntimeErrorClass {
        match error {
            RuntimeError::Authentication => RuntimeErrorClass::Authentication,
            RuntimeError::InvalidWorkspace | RuntimeError::Permission => {
                RuntimeErrorClass::Permission
            }
            RuntimeError::Timeout => RuntimeErrorClass::Timeout,
            RuntimeError::Cancelled => RuntimeErrorClass::Cancelled,
            RuntimeError::Protocol(_) => RuntimeErrorClass::Protocol,
            RuntimeError::TransportClosed | RuntimeError::Transport(_) => {
                RuntimeErrorClass::Transport
            }
            RuntimeError::Provider(_) => RuntimeErrorClass::Provider,
            RuntimeError::InvalidStreamCapacity
            | RuntimeError::StreamBufferFull { .. }
            | RuntimeError::StreamTerminal
            | RuntimeError::StreamTerminalMissing => RuntimeErrorClass::Protocol,
            RuntimeError::NotConfigured | RuntimeError::Unsupported => RuntimeErrorClass::Unknown,
        }
    }
}

/// The production default when no Runtime/Connector has been configured.
/// It deliberately has no model catalog and cannot execute or cancel a turn;
/// callers must choose an explicit RuntimeAdapter or fail closed.
#[derive(Clone, Debug, Default)]
pub struct UnconfiguredRuntime;

impl RuntimeAdapter for UnconfiguredRuntime {
    fn id(&self) -> &str {
        "unconfigured"
    }

    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities {
            streaming: false,
            cancel: false,
            filesystem: false,
            shell: false,
        }
    }

    fn discover(&self) -> RuntimeDiscovery {
        RuntimeDiscovery {
            runtime_id: self.id().into(),
            version: Some("unconfigured-v1".into()),
            owned: false,
        }
    }

    fn health(&self) -> RuntimeHealth {
        RuntimeHealth {
            runtime_id: self.id().into(),
            status: "unavailable".into(),
            detail: Some("no RuntimeAdapter is configured".into()),
        }
    }

    fn execute(&self, _request: &RuntimeRequest) -> Result<Vec<RuntimeEvent>, RuntimeError> {
        Err(RuntimeError::NotConfigured)
    }

    fn cancel(&self, _request: &RuntimeRequest) -> Result<RuntimeEvent, RuntimeError> {
        Err(RuntimeError::NotConfigured)
    }
}

#[derive(Clone, Debug)]
pub struct MockRuntime {
    pub chunks: Vec<String>,
}

impl Default for MockRuntime {
    fn default() -> Self {
        Self {
            chunks: vec!["mock ".into(), "output".into()],
        }
    }
}

impl RuntimeAdapter for MockRuntime {
    fn id(&self) -> &str {
        "mock"
    }
    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities {
            streaming: true,
            cancel: true,
            filesystem: false,
            shell: false,
        }
    }
    fn discover(&self) -> RuntimeDiscovery {
        RuntimeDiscovery {
            runtime_id: self.id().into(),
            version: Some("mock-1".into()),
            owned: true,
        }
    }
    fn health(&self) -> RuntimeHealth {
        RuntimeHealth {
            runtime_id: self.id().into(),
            status: "ready".into(),
            detail: None,
        }
    }
    fn list_models(&self) -> Vec<String> {
        vec!["mock-default".into()]
    }
    fn catalog_default_model_id(&self) -> Option<String> {
        Some("mock-default".into())
    }
    fn execute(&self, request: &RuntimeRequest) -> Result<Vec<RuntimeEvent>, RuntimeError> {
        if matches!(request.workspace_access, WorkspaceAccess::None)
            && request.canonical_cwd.is_some()
        {
            return Err(RuntimeError::InvalidWorkspace);
        }
        let mut events = vec![RuntimeEvent {
            event_id: format!("runtime-started-{}", request.execution_run_id),
            execution_run_id: request.execution_run_id.clone(),
            runtime_id: self.id().into(),
            thread_id: Some(format!("mock-thread-{}", request.execution_run_id)),
            turn_id: Some("turn-1".into()),
            sequence: 0,
            event_type: "runtime.started".into(),
            timestamp_ms: 0,
            payload: json!({"manifestId": request.context_manifest_id}),
        }];
        for (index, chunk) in self.chunks.iter().enumerate() {
            events.push(RuntimeEvent {
                event_id: format!("output-{}-{index}", request.execution_run_id),
                execution_run_id: request.execution_run_id.clone(),
                runtime_id: self.id().into(),
                thread_id: Some(format!("mock-thread-{}", request.execution_run_id)),
                turn_id: Some("turn-1".into()),
                sequence: 0,
                event_type: "output.delta".into(),
                timestamp_ms: index as i64 + 1,
                payload: json!({"delta": chunk}),
            });
        }
        events.push(RuntimeEvent {
            event_id: format!("completed-{}", request.execution_run_id),
            execution_run_id: request.execution_run_id.clone(),
            runtime_id: self.id().into(),
            thread_id: Some(format!("mock-thread-{}", request.execution_run_id)),
            turn_id: Some("turn-1".into()),
            sequence: 0,
            event_type: "execution.completed".into(),
            timestamp_ms: self.chunks.len() as i64 + 1,
            payload: json!({"output": self.chunks.join("")}),
        });
        Ok(events)
    }
    fn stream_events_with_capacity(
        &self,
        request: &RuntimeRequest,
        capacity: usize,
    ) -> Result<RuntimeEventStream, RuntimeError> {
        if matches!(request.workspace_access, WorkspaceAccess::None)
            && request.canonical_cwd.is_some()
        {
            return Err(RuntimeError::InvalidWorkspace);
        }
        let chunks = self.chunks.clone();
        let request = request.clone();
        RuntimeEventStream::spawn(capacity, move |producer| {
            producer.push(RuntimeEvent {
                event_id: format!("runtime-started-{}", request.execution_run_id),
                execution_run_id: request.execution_run_id.clone(),
                runtime_id: "mock".into(),
                thread_id: Some(format!("mock-thread-{}", request.execution_run_id)),
                turn_id: Some("turn-1".into()),
                sequence: 0,
                event_type: "runtime.started".into(),
                timestamp_ms: 0,
                payload: json!({"manifestId": request.context_manifest_id}),
            })?;
            for (index, chunk) in chunks.iter().enumerate() {
                producer.push(RuntimeEvent {
                    event_id: format!("output-{}-{index}", request.execution_run_id),
                    execution_run_id: request.execution_run_id.clone(),
                    runtime_id: "mock".into(),
                    thread_id: Some(format!("mock-thread-{}", request.execution_run_id)),
                    turn_id: Some("turn-1".into()),
                    sequence: 0,
                    event_type: "output.delta".into(),
                    timestamp_ms: index as i64 + 1,
                    payload: json!({"delta": chunk}),
                })?;
            }
            producer.push(RuntimeEvent {
                event_id: format!("completed-{}", request.execution_run_id),
                execution_run_id: request.execution_run_id.clone(),
                runtime_id: "mock".into(),
                thread_id: Some(format!("mock-thread-{}", request.execution_run_id)),
                turn_id: Some("turn-1".into()),
                sequence: 0,
                event_type: "execution.completed".into(),
                timestamp_ms: chunks.len() as i64 + 1,
                payload: json!({"source": "mock-stream"}),
            })?;
            Ok(())
        })
    }
    fn cancel(&self, request: &RuntimeRequest) -> Result<RuntimeEvent, RuntimeError> {
        Ok(RuntimeEvent {
            event_id: format!("cancelled-{}", request.execution_run_id),
            execution_run_id: request.execution_run_id.clone(),
            runtime_id: self.id().into(),
            thread_id: None,
            turn_id: None,
            sequence: 0,
            event_type: "execution.cancelled".into(),
            timestamp_ms: 0,
            payload: json!({"reason":"user_cancelled"}),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SseFrame {
    pub data: String,
}

#[derive(Default)]
pub struct SseParser {
    buffer: String,
}

/// Preserves an incomplete UTF-8 suffix between arbitrary transport reads.
/// SSE framing is text-based, but TCP and HTTP chunk boundaries are byte
/// boundaries and have no relationship to Unicode scalar boundaries.
#[derive(Default)]
struct IncrementalUtf8Decoder {
    pending: Vec<u8>,
}

impl IncrementalUtf8Decoder {
    fn push(&mut self, bytes: &[u8]) -> Result<String, RuntimeError> {
        self.pending.extend_from_slice(bytes);
        match std::str::from_utf8(&self.pending) {
            Ok(text) => {
                let text = text.to_owned();
                self.pending.clear();
                Ok(text)
            }
            Err(error) if error.error_len().is_none() => {
                let valid = error.valid_up_to();
                let text = std::str::from_utf8(&self.pending[..valid])
                    .expect("Utf8Error valid prefix must be valid UTF-8")
                    .to_owned();
                let suffix = self.pending.split_off(valid);
                self.pending = suffix;
                Ok(text)
            }
            Err(_) => {
                self.pending.clear();
                Err(RuntimeError::Protocol("kun_protocol_error".into()))
            }
        }
    }

    fn finish(&self) -> Result<(), RuntimeError> {
        if self.pending.is_empty() {
            Ok(())
        } else {
            Err(RuntimeError::Protocol("kun_protocol_error".into()))
        }
    }
}

impl SseParser {
    pub fn push(&mut self, chunk: &str) -> Vec<SseFrame> {
        self.buffer.push_str(chunk);
        let mut frames = Vec::new();
        while let Some(index) = self
            .buffer
            .find("\n\n")
            .or_else(|| self.buffer.find("\r\n\r\n"))
        {
            let separator = if self.buffer[index..].starts_with("\r\n") {
                4
            } else {
                2
            };
            let block = self.buffer[..index].to_owned();
            self.buffer.drain(..index + separator);
            if let Some(frame) = parse_sse_block(&block) {
                frames.push(frame);
            }
        }
        frames
    }

    fn push_bounded(
        &mut self,
        chunk: &str,
        max_buffer_bytes: usize,
    ) -> Result<Vec<SseFrame>, RuntimeError> {
        if self.buffer.len().saturating_add(chunk.len()) > max_buffer_bytes {
            self.buffer.clear();
            return Err(RuntimeError::Protocol("sse_frame_too_large".into()));
        }
        Ok(self.push(chunk))
    }

    pub fn finish(&mut self) -> Vec<SseFrame> {
        let block = std::mem::take(&mut self.buffer);
        parse_sse_block(&block).into_iter().collect()
    }
}

fn parse_sse_block(block: &str) -> Option<SseFrame> {
    let values: Vec<String> = block
        .lines()
        .filter_map(|line| {
            line.strip_prefix("data:")
                .map(|value| value.trim_start().to_owned())
        })
        .collect();
    if values.is_empty() {
        None
    } else {
        Some(SseFrame {
            data: values.join("\n"),
        })
    }
}

#[derive(Clone, Debug)]
pub struct OpenAiCompatibleRuntime {
    pub model_id: String,
}

impl OpenAiCompatibleRuntime {
    pub fn new(model_id: impl Into<String>) -> Self {
        Self {
            model_id: model_id.into(),
        }
    }

    pub fn execute_from_sse(
        &self,
        request: &RuntimeRequest,
        chunks: &[&str],
    ) -> Result<Vec<RuntimeEvent>, RuntimeError> {
        if request.workspace_access == WorkspaceAccess::WorkspaceWrite {
            return Err(RuntimeError::Permission);
        }
        let mut parser = SseParser::default();
        let mut events = vec![runtime_started(self.id(), request, "openai-compatible")];
        let mut output = String::new();
        let mut saw_done = false;
        for chunk in chunks.iter().copied() {
            for frame in parser.push(chunk) {
                saw_done |= append_sse_event(&mut events, &mut output, request, frame)?;
            }
        }
        for frame in parser.finish() {
            saw_done |= append_sse_event(&mut events, &mut output, request, frame)?;
        }
        if !saw_done {
            return Err(RuntimeError::Protocol(
                "SSE stream ended without [DONE]".into(),
            ));
        }
        events.push(RuntimeEvent {
            event_id: format!("completed-{}", request.execution_run_id),
            execution_run_id: request.execution_run_id.clone(),
            runtime_id: self.id().into(),
            thread_id: None,
            turn_id: Some("turn-1".into()),
            sequence: 0,
            event_type: "execution.completed".into(),
            timestamp_ms: events.len() as i64,
            payload: json!({"output": output, "modelId": self.model_id}),
        });
        Ok(events)
    }
}

fn runtime_started(runtime_id: &str, request: &RuntimeRequest, source: &str) -> RuntimeEvent {
    RuntimeEvent {
        event_id: format!("runtime-started-{}", request.execution_run_id),
        execution_run_id: request.execution_run_id.clone(),
        runtime_id: runtime_id.into(),
        thread_id: None,
        turn_id: Some("turn-1".into()),
        sequence: 0,
        event_type: "runtime.started".into(),
        timestamp_ms: 0,
        payload: json!({"source": source, "manifestId": request.context_manifest_id}),
    }
}

fn append_sse_event(
    events: &mut Vec<RuntimeEvent>,
    output: &mut String,
    request: &RuntimeRequest,
    frame: SseFrame,
) -> Result<bool, RuntimeError> {
    if frame.data == "[DONE]" {
        return Ok(true);
    }
    let value: Value = serde_json::from_str(&frame.data)
        .map_err(|error| RuntimeError::Protocol(error.to_string()))?;
    let delta = value
        .pointer("/choices/0/delta/content")
        .and_then(Value::as_str)
        .or_else(|| value.get("output_text").and_then(Value::as_str))
        .unwrap_or("");
    if !delta.is_empty() {
        output.push_str(delta);
        events.push(RuntimeEvent {
            event_id: format!("output-{}-{}", request.execution_run_id, events.len()),
            execution_run_id: request.execution_run_id.clone(),
            runtime_id: "openai-compatible".into(),
            thread_id: None,
            turn_id: Some("turn-1".into()),
            sequence: 0,
            event_type: "output.delta".into(),
            timestamp_ms: events.len() as i64,
            payload: json!({"delta": delta}),
        });
    }
    Ok(false)
}

impl RuntimeAdapter for OpenAiCompatibleRuntime {
    fn id(&self) -> &str {
        "openai-compatible"
    }
    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities {
            streaming: true,
            cancel: true,
            filesystem: false,
            shell: false,
        }
    }
    fn discover(&self) -> RuntimeDiscovery {
        RuntimeDiscovery {
            runtime_id: self.id().into(),
            version: Some("sse-adapter-v1".into()),
            owned: false,
        }
    }
    fn health(&self) -> RuntimeHealth {
        RuntimeHealth {
            runtime_id: self.id().into(),
            status: "configured".into(),
            detail: None,
        }
    }
    fn list_models(&self) -> Vec<String> {
        vec![self.model_id.clone()]
    }
    fn catalog_default_model_id(&self) -> Option<String> {
        safe_model_identifier(&self.model_id)
    }
    fn execute(&self, _request: &RuntimeRequest) -> Result<Vec<RuntimeEvent>, RuntimeError> {
        Err(RuntimeError::Unsupported)
    }
    fn cancel(&self, _request: &RuntimeRequest) -> Result<RuntimeEvent, RuntimeError> {
        // This adapter currently exposes only the bounded fixture parser;
        // without a live transport it must not manufacture a cancellation
        // event that looks like a Provider acknowledgement.
        Err(RuntimeError::Unsupported)
    }
}

#[derive(Clone, Debug)]
pub struct HttpCustomRuntime {
    pub model_id: String,
}

impl HttpCustomRuntime {
    pub fn new(model_id: impl Into<String>) -> Self {
        Self {
            model_id: model_id.into(),
        }
    }

    /// Parses a provider-neutral fixture. Live HTTP transport is deliberately
    /// outside this contract and remains an Owner Gate.
    pub fn execute_from_json(
        &self,
        request: &RuntimeRequest,
        body: &str,
    ) -> Result<Vec<RuntimeEvent>, RuntimeError> {
        if request.workspace_access == WorkspaceAccess::WorkspaceWrite {
            return Err(RuntimeError::Permission);
        }
        let value: Value = serde_json::from_str(body)
            .map_err(|error| RuntimeError::Protocol(error.to_string()))?;
        let mut events = vec![runtime_started(self.id(), request, "http-custom-fixture")];
        let mut output = String::new();
        let mut terminal = false;
        if let Some(items) = value.get("events").and_then(Value::as_array) {
            for item in items {
                let event_type = item.get("type").and_then(Value::as_str).ok_or_else(|| {
                    RuntimeError::Protocol("fixture event type is missing".into())
                })?;
                match event_type {
                    "output.delta" => {
                        let delta = item.get("delta").and_then(Value::as_str).ok_or_else(|| {
                            RuntimeError::Protocol("fixture delta is missing".into())
                        })?;
                        output.push_str(delta);
                        events.push(RuntimeEvent {
                            event_id: format!(
                                "http-output-{}-{}",
                                request.execution_run_id,
                                events.len()
                            ),
                            execution_run_id: request.execution_run_id.clone(),
                            runtime_id: self.id().into(),
                            thread_id: None,
                            turn_id: Some("turn-1".into()),
                            sequence: 0,
                            event_type: "output.delta".into(),
                            timestamp_ms: events.len() as i64,
                            payload: json!({"delta": delta}),
                        });
                    }
                    "execution.completed" => terminal = true,
                    "execution.failed" => {
                        terminal = true;
                        events.push(RuntimeEvent {
                            event_id: format!("http-failed-{}", request.execution_run_id),
                            execution_run_id: request.execution_run_id.clone(),
                            runtime_id: self.id().into(),
                            thread_id: None,
                            turn_id: Some("turn-1".into()),
                            sequence: 0,
                            event_type: "execution.failed".into(),
                            timestamp_ms: events.len() as i64,
                            payload: item.clone(),
                        });
                    }
                    other => {
                        return Err(RuntimeError::Protocol(format!(
                            "unsupported HTTP/Custom fixture event {other}"
                        )))
                    }
                }
            }
        } else if let Some(text) = value
            .get("outputText")
            .and_then(Value::as_str)
            .or_else(|| value.get("content").and_then(Value::as_str))
        {
            output.push_str(text);
            events.push(RuntimeEvent {
                event_id: format!("http-output-{}-1", request.execution_run_id),
                execution_run_id: request.execution_run_id.clone(),
                runtime_id: self.id().into(),
                thread_id: None,
                turn_id: Some("turn-1".into()),
                sequence: 0,
                event_type: "output.delta".into(),
                timestamp_ms: 1,
                payload: json!({"delta": text}),
            });
            terminal = value
                .get("completed")
                .and_then(Value::as_bool)
                .unwrap_or(false)
                || value.get("status").and_then(Value::as_str) == Some("completed");
        }
        if !terminal {
            return Err(RuntimeError::Protocol(
                "HTTP/Custom fixture is missing an explicit terminal event".into(),
            ));
        }
        if !events
            .iter()
            .any(|event| event.event_type == "execution.failed")
        {
            events.push(RuntimeEvent {
                event_id: format!("http-completed-{}", request.execution_run_id),
                execution_run_id: request.execution_run_id.clone(),
                runtime_id: self.id().into(),
                thread_id: None,
                turn_id: Some("turn-1".into()),
                sequence: 0,
                event_type: "execution.completed".into(),
                timestamp_ms: events.len() as i64,
                payload: json!({"output": output, "modelId": self.model_id}),
            });
        }
        Ok(events)
    }
}

impl RuntimeAdapter for HttpCustomRuntime {
    fn id(&self) -> &str {
        "http-custom"
    }

    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities {
            streaming: true,
            cancel: false,
            filesystem: false,
            shell: false,
        }
    }

    fn discover(&self) -> RuntimeDiscovery {
        RuntimeDiscovery {
            runtime_id: self.id().into(),
            version: Some("fixture-contract-v1".into()),
            owned: false,
        }
    }

    fn health(&self) -> RuntimeHealth {
        RuntimeHealth {
            runtime_id: self.id().into(),
            status: "fixture-ready".into(),
            detail: Some("live HTTP transport remains an Owner Gate".into()),
        }
    }

    fn list_models(&self) -> Vec<String> {
        vec![self.model_id.clone()]
    }

    fn execute(&self, _request: &RuntimeRequest) -> Result<Vec<RuntimeEvent>, RuntimeError> {
        Err(RuntimeError::Unsupported)
    }

    fn cancel(&self, _request: &RuntimeRequest) -> Result<RuntimeEvent, RuntimeError> {
        Err(RuntimeError::Unsupported)
    }
}

#[derive(Clone, Debug, Default)]
pub struct AcpMockRuntime {
    pub chunks: Vec<String>,
}

impl AcpMockRuntime {
    pub fn new(chunks: Vec<String>) -> Self {
        Self { chunks }
    }

    pub fn execute_from_fixture(
        &self,
        request: &RuntimeRequest,
        fixture: &str,
    ) -> Result<Vec<RuntimeEvent>, RuntimeError> {
        let values: Vec<Value> = serde_json::from_str(fixture)
            .map_err(|error| RuntimeError::Protocol(error.to_string()))?;
        let mut events = vec![runtime_started(self.id(), request, "acp-fixture")];
        let mut terminal = false;
        for (index, value) in values.into_iter().enumerate() {
            let kind = value
                .get("kind")
                .and_then(Value::as_str)
                .ok_or_else(|| RuntimeError::Protocol("ACP fixture kind is missing".into()))?;
            match kind {
                "session.started" => {}
                "output.delta" => {
                    let delta = value.get("delta").and_then(Value::as_str).ok_or_else(|| {
                        RuntimeError::Protocol("ACP fixture delta is missing".into())
                    })?;
                    events.push(RuntimeEvent {
                        event_id: format!("acp-output-{}-{index}", request.execution_run_id),
                        execution_run_id: request.execution_run_id.clone(),
                        runtime_id: self.id().into(),
                        thread_id: Some(format!("acp-session-{}", request.execution_run_id)),
                        turn_id: Some("turn-1".into()),
                        sequence: 0,
                        event_type: "output.delta".into(),
                        timestamp_ms: events.len() as i64,
                        payload: json!({"delta": delta}),
                    });
                }
                "turn.completed" | "execution.completed" => terminal = true,
                other => {
                    return Err(RuntimeError::Protocol(format!(
                        "unsupported ACP fixture event {other}"
                    )))
                }
            }
        }
        if !terminal {
            return Err(RuntimeError::Protocol(
                "ACP fixture is missing an explicit terminal event".into(),
            ));
        }
        events.push(RuntimeEvent {
            event_id: format!("acp-completed-{}", request.execution_run_id),
            execution_run_id: request.execution_run_id.clone(),
            runtime_id: self.id().into(),
            thread_id: Some(format!("acp-session-{}", request.execution_run_id)),
            turn_id: Some("turn-1".into()),
            sequence: 0,
            event_type: "execution.completed".into(),
            timestamp_ms: events.len() as i64,
            payload: json!({"source": "acp-fixture"}),
        });
        Ok(events)
    }
}

impl RuntimeAdapter for AcpMockRuntime {
    fn id(&self) -> &str {
        "acp-mock"
    }

    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities {
            streaming: true,
            cancel: true,
            filesystem: false,
            shell: false,
        }
    }

    fn discover(&self) -> RuntimeDiscovery {
        RuntimeDiscovery {
            runtime_id: self.id().into(),
            version: Some("mock-contract-v1".into()),
            owned: true,
        }
    }

    fn health(&self) -> RuntimeHealth {
        RuntimeHealth {
            runtime_id: self.id().into(),
            status: "ready".into(),
            detail: Some("ACP mock fixture only".into()),
        }
    }

    fn execute(&self, request: &RuntimeRequest) -> Result<Vec<RuntimeEvent>, RuntimeError> {
        let chunks = if self.chunks.is_empty() {
            vec!["acp mock output".into()]
        } else {
            self.chunks.clone()
        };
        let fixture = format!(
            "[{},{},{{\"kind\":\"turn.completed\"}}]",
            "{\"kind\":\"session.started\"}",
            chunks
                .iter()
                .map(|chunk| format!(
                    "{{\"kind\":\"output.delta\",\"delta\":{}}}",
                    serde_json::to_string(chunk).unwrap()
                ))
                .collect::<Vec<_>>()
                .join(",")
        );
        self.execute_from_fixture(request, &fixture)
    }

    fn stream_events_with_capacity(
        &self,
        request: &RuntimeRequest,
        capacity: usize,
    ) -> Result<RuntimeEventStream, RuntimeError> {
        let chunks = if self.chunks.is_empty() {
            vec!["acp mock output".into()]
        } else {
            self.chunks.clone()
        };
        let request = request.clone();
        RuntimeEventStream::spawn(capacity, move |producer| {
            producer.push(runtime_started("acp-mock", &request, "acp-fixture"))?;
            for (index, chunk) in chunks.iter().enumerate() {
                producer.push(RuntimeEvent {
                    event_id: format!("acp-output-{}-{index}", request.execution_run_id),
                    execution_run_id: request.execution_run_id.clone(),
                    runtime_id: "acp-mock".into(),
                    thread_id: Some(format!("acp-session-{}", request.execution_run_id)),
                    turn_id: Some("turn-1".into()),
                    sequence: 0,
                    event_type: "output.delta".into(),
                    timestamp_ms: index as i64 + 1,
                    payload: json!({"delta": chunk}),
                })?;
            }
            producer.push(RuntimeEvent {
                event_id: format!("acp-completed-{}", request.execution_run_id),
                execution_run_id: request.execution_run_id.clone(),
                runtime_id: "acp-mock".into(),
                thread_id: Some(format!("acp-session-{}", request.execution_run_id)),
                turn_id: Some("turn-1".into()),
                sequence: 0,
                event_type: "execution.completed".into(),
                timestamp_ms: chunks.len() as i64 + 1,
                payload: json!({"source": "acp-fixture"}),
            })?;
            Ok(())
        })
    }

    fn cancel(&self, request: &RuntimeRequest) -> Result<RuntimeEvent, RuntimeError> {
        Ok(RuntimeEvent {
            event_id: format!("acp-cancelled-{}", request.execution_run_id),
            execution_run_id: request.execution_run_id.clone(),
            runtime_id: self.id().into(),
            thread_id: Some(format!("acp-session-{}", request.execution_run_id)),
            turn_id: Some("turn-1".into()),
            sequence: 0,
            event_type: "execution.cancelled".into(),
            timestamp_ms: 0,
            payload: json!({"reason": "user_cancelled"}),
        })
    }
}

#[derive(Clone, Debug)]
struct FixtureConnectorRuntime {
    runtime_id: String,
    runtime_version: String,
    model_ids: Vec<String>,
    provider_type: String,
    fixture: Option<String>,
    capabilities: RuntimeCapabilities,
    inter_event_delay: Duration,
}

impl FixtureConnectorRuntime {
    fn new(
        runtime_id: &str,
        runtime_version: &str,
        provider_type: &str,
        model_id: impl Into<String>,
        capabilities: RuntimeCapabilities,
    ) -> Self {
        Self {
            runtime_id: runtime_id.into(),
            runtime_version: runtime_version.into(),
            model_ids: vec![model_id.into()],
            provider_type: provider_type.into(),
            fixture: None,
            capabilities,
            inter_event_delay: Duration::ZERO,
        }
    }

    fn with_fixture(mut self, fixture: impl Into<String>) -> Self {
        self.fixture = Some(fixture.into());
        self
    }

    fn with_models(mut self, model_ids: Vec<String>) -> Self {
        let model_ids = model_ids
            .into_iter()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
            .collect::<Vec<_>>();
        if !model_ids.is_empty() {
            self.model_ids = model_ids;
        }
        self
    }

    fn with_inter_event_delay(mut self, delay: Duration) -> Self {
        self.inter_event_delay = delay;
        self
    }

    fn fixture_values(fixture: &str) -> Result<Vec<Value>, RuntimeError> {
        let fixture = fixture.trim();
        if fixture.is_empty() {
            return Err(RuntimeError::Protocol("connector fixture is empty".into()));
        }
        if fixture.starts_with('[') {
            return serde_json::from_str(fixture)
                .map_err(|error| RuntimeError::Protocol(error.to_string()));
        }
        fixture
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| {
                serde_json::from_str(line)
                    .map_err(|error| RuntimeError::Protocol(error.to_string()))
            })
            .collect()
    }

    fn execute_fixture(
        &self,
        request: &RuntimeRequest,
        fixture: &str,
    ) -> Result<Vec<RuntimeEvent>, RuntimeError> {
        if request.workspace_access == WorkspaceAccess::WorkspaceWrite
            && !self.capabilities.filesystem
        {
            return Err(RuntimeError::Permission);
        }
        let selected_model_id = request.model_id.as_deref().unwrap_or_else(|| {
            self.model_ids
                .first()
                .map(String::as_str)
                .unwrap_or_default()
        });
        if selected_model_id.is_empty()
            || !self
                .model_ids
                .iter()
                .any(|model_id| model_id == selected_model_id)
        {
            return Err(RuntimeError::Protocol(
                "requested model is not in this connector fixture catalog".into(),
            ));
        }
        let values = Self::fixture_values(fixture)?;
        let thread_id = format!("{}-thread-{}", self.runtime_id, request.execution_run_id);
        let mut events = vec![
            RuntimeEvent {
                event_id: format!("connector-started-{}", request.execution_run_id),
                execution_run_id: request.execution_run_id.clone(),
                runtime_id: self.runtime_id.clone(),
                thread_id: Some(thread_id.clone()),
                turn_id: Some("turn-1".into()),
                sequence: 0,
                event_type: "connector.started".into(),
                timestamp_ms: 0,
                payload: json!({
                    "connectorId": request.connector_id,
                    "modelId": selected_model_id,
                }),
            },
            RuntimeEvent {
                event_id: format!("runtime-started-{}", request.execution_run_id),
                execution_run_id: request.execution_run_id.clone(),
                runtime_id: self.runtime_id.clone(),
                thread_id: Some(thread_id.clone()),
                turn_id: Some("turn-1".into()),
                sequence: 0,
                event_type: "runtime.started".into(),
                timestamp_ms: 0,
                payload: json!({
                    "source": self.provider_type,
                    "manifestId": request.context_manifest_id,
                    "connectorId": request.connector_id,
                    "modelId": selected_model_id,
                }),
            },
        ];
        let mut terminal = false;
        for value in values {
            if terminal {
                return Err(RuntimeError::Protocol(
                    "connector fixture emitted data after terminal event".into(),
                ));
            }
            let kind = value
                .get("type")
                .and_then(Value::as_str)
                .or_else(|| value.get("kind").and_then(Value::as_str))
                .or_else(|| value.get("method").and_then(Value::as_str))
                .ok_or_else(|| {
                    RuntimeError::Protocol("connector fixture kind is missing".into())
                })?;
            let normalized = kind.to_ascii_lowercase();
            if matches!(
                normalized.as_str(),
                "session.started"
                    | "thread.started"
                    | "turn.started"
                    | "response.created"
                    | "response.in_progress"
            ) {
                continue;
            }
            if matches!(
                normalized.as_str(),
                "output.delta"
                    | "content.delta"
                    | "item/agentmessage/delta"
                    | "response.output_text.delta"
            ) {
                let delta = value
                    .get("delta")
                    .and_then(Value::as_str)
                    .or_else(|| value.pointer("/params/delta").and_then(Value::as_str))
                    .or_else(|| value.get("text").and_then(Value::as_str))
                    .or_else(|| value.pointer("/params/text").and_then(Value::as_str))
                    .ok_or_else(|| RuntimeError::Protocol("connector delta is missing".into()))?
                    .replace("{modelId}", selected_model_id);
                events.push(RuntimeEvent {
                    event_id: format!("output-{}-{}", request.execution_run_id, events.len()),
                    execution_run_id: request.execution_run_id.clone(),
                    runtime_id: self.runtime_id.clone(),
                    thread_id: Some(thread_id.clone()),
                    turn_id: Some("turn-1".into()),
                    sequence: 0,
                    event_type: "output.delta".into(),
                    timestamp_ms: events.len() as i64,
                    payload: json!({"delta": delta}),
                });
                continue;
            }
            if matches!(
                normalized.as_str(),
                "execution.completed"
                    | "turn.completed"
                    | "response.completed"
                    | "result.completed"
            ) {
                terminal = true;
                events.push(RuntimeEvent {
                    event_id: format!("completed-{}", request.execution_run_id),
                    execution_run_id: request.execution_run_id.clone(),
                    runtime_id: self.runtime_id.clone(),
                    thread_id: Some(thread_id.clone()),
                    turn_id: Some("turn-1".into()),
                    sequence: 0,
                    event_type: "execution.completed".into(),
                    timestamp_ms: events.len() as i64,
                    payload: json!({
                        "connectorId": request.connector_id,
                        "modelId": selected_model_id,
                    }),
                });
                continue;
            }
            if matches!(
                normalized.as_str(),
                "execution.failed" | "turn.failed" | "response.failed" | "error" | "provider.error"
            ) {
                terminal = true;
                events.push(RuntimeEvent {
                    event_id: format!("failed-{}", request.execution_run_id),
                    execution_run_id: request.execution_run_id.clone(),
                    runtime_id: self.runtime_id.clone(),
                    thread_id: Some(thread_id.clone()),
                    turn_id: Some("turn-1".into()),
                    sequence: 0,
                    event_type: "execution.failed".into(),
                    timestamp_ms: events.len() as i64,
                    payload: json!({"reason": "provider_error"}),
                });
                continue;
            }
            return Err(RuntimeError::Protocol(format!(
                "unsupported connector fixture event {kind}"
            )));
        }
        if !terminal {
            return Err(RuntimeError::Protocol(
                "connector fixture is missing an explicit terminal event".into(),
            ));
        }
        Ok(events)
    }

    fn execute_configured(
        &self,
        request: &RuntimeRequest,
    ) -> Result<Vec<RuntimeEvent>, RuntimeError> {
        self.execute_fixture(
            request,
            self.fixture.as_deref().ok_or(RuntimeError::Unsupported)?,
        )
    }
}

// -------------------------------------------------------------------------
// Transport-backed connector runtimes
// -------------------------------------------------------------------------
//
// The two desktop connectors below deliberately keep RuntimeAdapter free of
// concrete process and HTTP plumbing.  A production transport is constructed
// lazily, while fixture transports are injected only by development/test
// constructors.  This prevents registering a connector from starting a
// provider process, opening a user data directory, or touching credentials.

const MAX_TRANSPORT_BODY_BYTES: usize = 512 * 1024;
const MAX_TRANSPORT_LINE_BYTES: usize = 128 * 1024;
const MAX_REDACTED_DIAGNOSTIC_BYTES: usize = 1024;
const MAX_TRANSPORT_SETUP_TIMEOUT: Duration = Duration::from_secs(5);
const KUN_SHARED_RUNTIME_STARTUP_RETRY_WINDOW: Duration = Duration::from_millis(250);
const CODEX_RUNTIME_UNAVAILABLE: &str = "codex_runtime_unavailable";
const CODEX_PROTOCOL_ERROR: &str = "codex_protocol_error";
const CODEX_CATALOG_UNAVAILABLE: &str = "codex_catalog_unavailable";
const CODEX_MODEL_UNAVAILABLE: &str = "codex_model_unavailable";
const KUN_SHARED_RUNTIME_UNAVAILABLE: &str = "kun_shared_runtime_unavailable";
const KUN_RUNTIME_IDENTITY_MISMATCH: &str = "kun_runtime_identity_mismatch";
const KUN_CATALOG_UNAVAILABLE: &str = "kun_catalog_unavailable";
const KUN_MODEL_UNAVAILABLE: &str = "kun_model_unavailable";
const KUN_PROVIDER_AUTHENTICATION_FAILED: &str = "kun_provider_authentication_failed";

#[derive(Clone, Copy)]
struct TransportDeadline {
    ends_at: Instant,
}

impl TransportDeadline {
    fn after(timeout: Duration) -> Self {
        Self {
            ends_at: Instant::now() + timeout,
        }
    }

    fn remaining(self) -> Result<Duration, RuntimeError> {
        self.ends_at
            .checked_duration_since(Instant::now())
            .filter(|remaining| !remaining.is_zero())
            .ok_or(RuntimeError::Timeout)
    }

    fn capped(self, cap: Duration) -> Self {
        Self {
            ends_at: self.ends_at.min(Instant::now() + cap),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeModelCatalog {
    pub models: Vec<String>,
    /// The Runtime-owned revision is retained in memory only. Core computes
    /// its IPC revision independently, but callers may use this for local
    /// diagnostics without exposing a Runtime response body.
    pub revision: Option<String>,
    /// The Runtime-declared default, if it names one of `models`.  `None` is
    /// meaningful and must never be replaced by a sorted-first fallback.
    pub default_model_id: Option<String>,
    /// Availability and capability facts supplied by the Runtime.  They are
    /// normalized before being exposed through `RuntimeAdapter`.
    pub model_metadata: Vec<RuntimeModelMetadata>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeModelMetadata {
    pub model_id: String,
    pub available: bool,
    pub enabled: bool,
    pub status: Option<String>,
    pub capabilities: RuntimeCapabilities,
}

/// Narrow I/O seam for a connector Runtime.
///
/// Implementations must return stable, non-sensitive RuntimeError values. In
/// particular, raw provider response bodies, HTTP Authorization values, and
/// child stderr must not cross this trait boundary.
pub trait ConnectorRuntimeTransport: Send + Sync {
    fn runtime_id(&self) -> &'static str;
    fn owned(&self) -> bool;
    fn discover(&self) -> Result<RuntimeDiscovery, RuntimeError>;
    fn list_models(&self) -> Result<RuntimeModelCatalog, RuntimeError>;
    fn stream(
        &self,
        request: &RuntimeRequest,
        cancellation: Arc<AtomicBool>,
        producer: &RuntimeEventProducer,
    ) -> Result<(), RuntimeError>;
    /// Cancellation is best effort at the remote boundary. The adapter emits
    /// the authoritative local execution.cancelled terminal event only after
    /// this method returns successfully.
    fn cancel(&self, request: &RuntimeRequest) -> Result<(), RuntimeError>;
    /// Only resources created by this transport may be released here.
    fn shutdown_owned(&self) -> Result<(), RuntimeError>;
}

#[derive(Clone, Debug)]
struct ConnectorAdapterState {
    models: Vec<String>,
    catalog_revision: Option<String>,
    catalog_default_model_id: Option<String>,
    catalog_model_metadata: BTreeMap<String, RuntimeModelMetadata>,
    last_health: RuntimeHealth,
    active_cancellations: HashMap<String, Arc<AtomicBool>>,
}

#[derive(Clone)]
struct TransportBackedRuntime {
    runtime_id: &'static str,
    capabilities: RuntimeCapabilities,
    transport: Arc<dyn ConnectorRuntimeTransport>,
    state: Arc<Mutex<ConnectorAdapterState>>,
}

impl std::fmt::Debug for TransportBackedRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TransportBackedRuntime")
            .field("runtime_id", &self.runtime_id)
            .field("capabilities", &self.capabilities)
            .finish_non_exhaustive()
    }
}

impl TransportBackedRuntime {
    fn new(
        runtime_id: &'static str,
        capabilities: RuntimeCapabilities,
        transport: Arc<dyn ConnectorRuntimeTransport>,
    ) -> Self {
        Self {
            runtime_id,
            capabilities,
            transport,
            state: Arc::new(Mutex::new(ConnectorAdapterState {
                models: Vec::new(),
                catalog_revision: None,
                catalog_default_model_id: None,
                catalog_model_metadata: BTreeMap::new(),
                last_health: RuntimeHealth {
                    runtime_id: runtime_id.into(),
                    status: "unavailable".into(),
                    detail: Some("not yet probed".into()),
                },
                active_cancellations: HashMap::new(),
            })),
        }
    }

    fn lock_state(&self) -> MutexGuard<'_, ConnectorAdapterState> {
        match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn ensure_route(&self, request: &RuntimeRequest) -> Result<(), RuntimeError> {
        // Connector IDs are persisted profile identifiers (often UUIDs), not
        // runtime types. Core resolves a profile's frozen runtimeType before
        // selecting an adapter, so this layer must never reinterpret a
        // valid profile ID as the literal `codex` or `kun` adapter name.
        if request.connector_id.trim().is_empty() {
            return Err(RuntimeError::Protocol("connector_id_required".into()));
        }
        if matches!(request.workspace_access, WorkspaceAccess::None)
            && request.canonical_cwd.is_some()
        {
            return Err(RuntimeError::InvalidWorkspace);
        }
        Ok(())
    }

    fn update_health(&self, health: RuntimeHealth) {
        self.lock_state().last_health = health;
    }

    fn record_error(&self, error: &RuntimeError) {
        let detail = safe_runtime_error_code(error);
        self.update_health(RuntimeHealth {
            runtime_id: self.runtime_id.into(),
            status: "unavailable".into(),
            detail: Some(detail.into()),
        });
    }

    fn cached_catalog_revision(&self) -> Option<String> {
        self.lock_state().catalog_revision.clone()
    }

    fn cached_catalog_default_model_id(&self) -> Option<String> {
        self.lock_state().catalog_default_model_id.clone()
    }

    fn cached_catalog_model_metadata(&self, model_id: &str) -> Option<RuntimeModelMetadata> {
        self.lock_state()
            .catalog_model_metadata
            .get(model_id)
            .cloned()
    }

    fn refresh_models(&self) -> Result<Vec<String>, RuntimeError> {
        match self.transport.list_models() {
            Ok(catalog) => {
                let models = catalog
                    .models
                    .into_iter()
                    .filter_map(|model| safe_model_identifier(&model))
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>();
                // A catalog is authoritative only when it contains at least
                // one usable model. Empty success envelopes are fail-closed
                // and must remain distinguishable from a healthy empty list.
                if models.is_empty() {
                    let code = if self.runtime_id == "codex" {
                        CODEX_CATALOG_UNAVAILABLE
                    } else if self.runtime_id == "kun" {
                        KUN_CATALOG_UNAVAILABLE
                    } else {
                        "runtime_catalog_unavailable"
                    };
                    let error = RuntimeError::Transport(code.into());
                    self.record_error(&error);
                    let mut state = self.lock_state();
                    state.models.clear();
                    state.catalog_revision = None;
                    state.catalog_default_model_id = None;
                    state.catalog_model_metadata.clear();
                    return Err(error);
                }
                let known_models = models.iter().cloned().collect::<BTreeSet<_>>();
                let declared_default_model_id = catalog
                    .default_model_id
                    .as_deref()
                    .and_then(safe_model_identifier)
                    .filter(|model_id| known_models.contains(model_id));
                let model_metadata = catalog
                    .model_metadata
                    .into_iter()
                    .filter_map(|metadata| {
                        let model_id = safe_model_identifier(&metadata.model_id)?;
                        if !known_models.contains(&model_id) {
                            return None;
                        }
                        Some((
                            model_id.clone(),
                            RuntimeModelMetadata {
                                model_id,
                                available: metadata.available,
                                enabled: metadata.enabled,
                                status: metadata.status.as_deref().and_then(safe_model_identifier),
                                capabilities: metadata.capabilities,
                            },
                        ))
                    })
                    .collect::<BTreeMap<_, _>>();
                let default_model_id = declared_default_model_id.filter(|model_id| {
                    model_metadata
                        .get(model_id)
                        .map(|metadata| metadata.available && metadata.enabled)
                        .unwrap_or(true)
                });
                let mut state = self.lock_state();
                state.models = models.clone();
                state.catalog_revision = catalog.revision;
                state.catalog_default_model_id = default_model_id;
                state.catalog_model_metadata = model_metadata;
                Ok(models)
            }
            Err(error) => {
                self.record_error(&error);
                let mut state = self.lock_state();
                state.models.clear();
                state.catalog_revision = None;
                state.catalog_default_model_id = None;
                state.catalog_model_metadata.clear();
                Err(error)
            }
        }
    }

    fn execute_collect(&self, request: &RuntimeRequest) -> Result<Vec<RuntimeEvent>, RuntimeError> {
        let stream = self.stream_with_capacity(request, DEFAULT_RUNTIME_STREAM_CAPACITY)?;
        let mut events = Vec::new();
        while let Some(event) = stream.next()? {
            events.push(event);
        }
        Ok(events)
    }

    fn stream_with_capacity(
        &self,
        request: &RuntimeRequest,
        capacity: usize,
    ) -> Result<RuntimeEventStream, RuntimeError> {
        self.ensure_route(request)?;
        if capacity == 0 {
            return Err(RuntimeError::InvalidStreamCapacity);
        }

        let cancellation = Arc::new(AtomicBool::new(false));
        self.lock_state()
            .active_cancellations
            .insert(request.execution_run_id.clone(), Arc::clone(&cancellation));

        let transport = Arc::clone(&self.transport);
        let cancel_transport = Arc::clone(&self.transport);
        let state = Arc::clone(&self.state);
        let run_id = request.execution_run_id.clone();
        let stream_request = request.clone();
        let cancel_request = request.clone();
        let producer_cancellation = Arc::clone(&cancellation);

        RuntimeEventStream::spawn_with_cancel(
            capacity,
            move || {
                // Do not turn a failed remote interrupt into a local
                // `Cancelled` result. The callback cannot return an error,
                // so leave the worker running on failure; direct Core cancel
                // commands receive the classified transport error below.
                if cancel_transport.cancel(&cancel_request).is_ok() {
                    cancellation.store(true, Ordering::Release);
                }
            },
            move |producer| {
                let result = transport.stream(&stream_request, producer_cancellation, producer);
                match state.lock() {
                    Ok(mut state) => {
                        state.active_cancellations.remove(&run_id);
                    }
                    Err(poisoned) => {
                        poisoned.into_inner().active_cancellations.remove(&run_id);
                    }
                }
                result
            },
        )
    }

    fn cancel_run(&self, request: &RuntimeRequest) -> Result<RuntimeEvent, RuntimeError> {
        self.ensure_route(request)?;
        self.transport.cancel(request)?;
        if let Some(cancellation) = self
            .lock_state()
            .active_cancellations
            .get(&request.execution_run_id)
            .cloned()
        {
            cancellation.store(true, Ordering::Release);
        }
        Ok(cancelled_event(self.runtime_id, request, None, None))
    }

    fn shutdown_owned(&self) -> Result<(), RuntimeError> {
        let cancellations = self
            .lock_state()
            .active_cancellations
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for cancellation in cancellations {
            cancellation.store(true, Ordering::Release);
        }
        self.transport.shutdown_owned()
    }
}

impl RuntimeAdapter for TransportBackedRuntime {
    fn id(&self) -> &str {
        self.runtime_id
    }

    fn capabilities(&self) -> RuntimeCapabilities {
        self.capabilities.clone()
    }

    fn discover(&self) -> RuntimeDiscovery {
        match self.transport.discover() {
            Ok(discovery) => {
                self.update_health(RuntimeHealth {
                    runtime_id: self.runtime_id.into(),
                    status: "available".into(),
                    detail: None,
                });
                discovery
            }
            Err(error) => {
                self.record_error(&error);
                RuntimeDiscovery {
                    runtime_id: self.runtime_id.into(),
                    version: None,
                    owned: self.transport.owned(),
                }
            }
        }
    }

    fn health(&self) -> RuntimeHealth {
        match self.transport.discover() {
            Ok(_) => {
                let health = RuntimeHealth {
                    runtime_id: self.runtime_id.into(),
                    status: "available".into(),
                    detail: None,
                };
                self.update_health(health.clone());
                health
            }
            Err(error) => {
                self.record_error(&error);
                self.lock_state().last_health.clone()
            }
        }
    }

    fn ensure_available(&self) -> Result<(), RuntimeError> {
        match self.transport.discover() {
            Ok(_) => {
                self.update_health(RuntimeHealth {
                    runtime_id: self.runtime_id.into(),
                    status: "available".into(),
                    detail: None,
                });
                Ok(())
            }
            Err(error) => {
                self.record_error(&error);
                Err(error)
            }
        }
    }

    fn list_models(&self) -> Vec<String> {
        self.refresh_models().unwrap_or_default()
    }

    fn list_models_checked(&self) -> Result<Vec<String>, RuntimeError> {
        self.refresh_models()
    }

    fn catalog_revision(&self) -> Option<u64> {
        self.cached_catalog_revision()
            .and_then(|revision| revision.parse::<u64>().ok())
            .filter(|revision| *revision > 0)
    }

    fn catalog_default_model_id(&self) -> Option<String> {
        self.cached_catalog_default_model_id()
    }

    fn catalog_model_metadata(&self, model_id: &str) -> Option<RuntimeModelMetadata> {
        self.cached_catalog_model_metadata(model_id)
    }

    fn execute(&self, request: &RuntimeRequest) -> Result<Vec<RuntimeEvent>, RuntimeError> {
        self.execute_collect(request)
    }

    fn stream_events_with_capacity(
        &self,
        request: &RuntimeRequest,
        capacity: usize,
    ) -> Result<RuntimeEventStream, RuntimeError> {
        self.stream_with_capacity(request, capacity)
    }

    fn cancel(&self, request: &RuntimeRequest) -> Result<RuntimeEvent, RuntimeError> {
        self.cancel_run(request)
    }

    fn shutdown_owned(&self) -> Result<(), RuntimeError> {
        self.shutdown_owned()
    }
}

fn safe_model_identifier(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty() || value.len() > 256 || value.chars().any(char::is_control) {
        None
    } else {
        Some(value.into())
    }
}

fn safe_runtime_error_code(error: &RuntimeError) -> &'static str {
    match error {
        RuntimeError::Authentication => "runtime_authentication_failed",
        RuntimeError::Permission => "runtime_permission_denied",
        RuntimeError::Timeout => "runtime_timeout",
        RuntimeError::Cancelled => "runtime_cancelled",
        RuntimeError::Transport(code) if code == KUN_SHARED_RUNTIME_UNAVAILABLE => {
            KUN_SHARED_RUNTIME_UNAVAILABLE
        }
        RuntimeError::Transport(code) if code == KUN_CATALOG_UNAVAILABLE => KUN_CATALOG_UNAVAILABLE,
        RuntimeError::Transport(code) if code == CODEX_CATALOG_UNAVAILABLE => {
            CODEX_CATALOG_UNAVAILABLE
        }
        RuntimeError::Transport(code) if code == CODEX_RUNTIME_UNAVAILABLE => {
            CODEX_RUNTIME_UNAVAILABLE
        }
        RuntimeError::Protocol(code) if code == KUN_RUNTIME_IDENTITY_MISMATCH => {
            KUN_RUNTIME_IDENTITY_MISMATCH
        }
        RuntimeError::Protocol(code) if code == KUN_MODEL_UNAVAILABLE => KUN_MODEL_UNAVAILABLE,
        RuntimeError::Protocol(code) if code == CODEX_MODEL_UNAVAILABLE => CODEX_MODEL_UNAVAILABLE,
        RuntimeError::Provider(code) if code == KUN_PROVIDER_AUTHENTICATION_FAILED => {
            KUN_PROVIDER_AUTHENTICATION_FAILED
        }
        RuntimeError::Provider(_) => "provider_rejected",
        RuntimeError::Protocol(_) => "runtime_protocol_error",
        RuntimeError::TransportClosed => "runtime_transport_closed",
        RuntimeError::NotConfigured => "runtime_not_configured",
        RuntimeError::InvalidWorkspace => "invalid_workspace",
        RuntimeError::InvalidStreamCapacity
        | RuntimeError::StreamBufferFull { .. }
        | RuntimeError::StreamTerminal
        | RuntimeError::StreamTerminalMissing
        | RuntimeError::Unsupported => "runtime_unavailable",
        RuntimeError::Transport(_) => "runtime_transport_error",
    }
}

fn connector_started_event(
    runtime_id: &str,
    request: &RuntimeRequest,
    thread_id: Option<String>,
    turn_id: Option<String>,
) -> RuntimeEvent {
    RuntimeEvent {
        event_id: format!("connector-started-{}", request.execution_run_id),
        execution_run_id: request.execution_run_id.clone(),
        runtime_id: runtime_id.into(),
        thread_id,
        turn_id,
        sequence: 0,
        event_type: "connector.started".into(),
        timestamp_ms: 0,
        payload: json!({
            "connectorId": request.connector_id,
            "modelId": request.model_id,
        }),
    }
}

fn runtime_started_event(
    runtime_id: &str,
    source: &str,
    request: &RuntimeRequest,
    thread_id: Option<String>,
    turn_id: Option<String>,
) -> RuntimeEvent {
    RuntimeEvent {
        event_id: format!("runtime-started-{}", request.execution_run_id),
        execution_run_id: request.execution_run_id.clone(),
        runtime_id: runtime_id.into(),
        thread_id,
        turn_id,
        sequence: 0,
        event_type: "runtime.started".into(),
        timestamp_ms: 0,
        payload: json!({
            "source": source,
            "manifestId": request.context_manifest_id,
            "connectorId": request.connector_id,
            "modelId": request.model_id,
        }),
    }
}

fn output_delta_event(
    runtime_id: &str,
    request: &RuntimeRequest,
    thread_id: Option<String>,
    turn_id: Option<String>,
    index: u64,
    delta: &str,
) -> RuntimeEvent {
    RuntimeEvent {
        event_id: format!("output-{}-{index}", request.execution_run_id),
        execution_run_id: request.execution_run_id.clone(),
        runtime_id: runtime_id.into(),
        thread_id,
        turn_id,
        sequence: 0,
        event_type: "output.delta".into(),
        timestamp_ms: index as i64,
        payload: json!({"delta": redact_external_text(delta)}),
    }
}

fn terminal_event(
    runtime_id: &str,
    event_type: &str,
    request: &RuntimeRequest,
    thread_id: Option<String>,
    turn_id: Option<String>,
    index: u64,
    reason: Option<&str>,
) -> RuntimeEvent {
    let mut payload = serde_json::Map::new();
    if let Some(reason) = reason {
        payload.insert("reason".into(), Value::String(reason.into()));
    }
    payload.insert(
        "connectorId".into(),
        Value::String(request.connector_id.clone()),
    );
    payload.insert(
        "modelId".into(),
        request
            .model_id
            .clone()
            .map(Value::String)
            .unwrap_or(Value::Null),
    );
    RuntimeEvent {
        event_id: format!(
            "{}-{}",
            event_type.replace('.', "-"),
            request.execution_run_id
        ),
        execution_run_id: request.execution_run_id.clone(),
        runtime_id: runtime_id.into(),
        thread_id,
        turn_id,
        sequence: 0,
        event_type: event_type.into(),
        timestamp_ms: index as i64,
        payload: Value::Object(payload),
    }
}

fn cancelled_event(
    runtime_id: &str,
    request: &RuntimeRequest,
    thread_id: Option<String>,
    turn_id: Option<String>,
) -> RuntimeEvent {
    terminal_event(
        runtime_id,
        "execution.cancelled",
        request,
        thread_id,
        turn_id,
        0,
        Some("user_cancelled"),
    )
}

fn redact_external_text(value: &str) -> String {
    // Keep ordinary model text readable while eliminating values in both
    // header-style and JSON/key-value credential forms before they can reach
    // an event, stderr tail, health detail, or error envelope.
    let mut redacted = value.to_owned();
    for (marker, to_line_end) in [
        ("authorization:", true),
        ("proxy-authorization:", true),
        ("cookie:", true),
        ("set-cookie:", true),
        ("\"authorization\"", false),
        ("\"runtimetoken\"", false),
        ("\"runtime_token\"", false),
        ("\"token\"", false),
        ("\"apikey\"", false),
        ("\"api_key\"", false),
        ("bearer ", false),
        ("api_key=", false),
        ("api-key=", false),
        ("apikey=", false),
        ("token=", false),
        ("runtime_token=", false),
        ("cookie=", false),
    ] {
        redacted = redact_marker_value(redacted, marker, to_line_end);
    }
    if redacted.len() > MAX_TRANSPORT_LINE_BYTES {
        redacted = utf8_prefix(&redacted, MAX_TRANSPORT_LINE_BYTES).to_owned();
        redacted.push_str("...[truncated]");
    }
    redacted
}

fn redact_marker_value(mut value: String, marker: &str, to_line_end: bool) -> String {
    let marker = marker.to_ascii_lowercase();
    let mut cursor = 0usize;
    loop {
        let lowered = value.to_ascii_lowercase();
        let Some(found) = lowered[cursor..].find(&marker) else {
            break;
        };
        let marker_end = cursor + found + marker.len();
        let mut value_start = marker_end;
        while let Some(character) = value[value_start..].chars().next() {
            if character.is_whitespace() || matches!(character, ':' | '=') {
                value_start += character.len_utf8();
            } else {
                break;
            }
        }

        let mut quoted = None;
        if let Some(character @ ('\'' | '"')) = value[value_start..].chars().next() {
            quoted = Some(character);
            value_start += character.len_utf8();
        }
        let value_end = if to_line_end {
            value[value_start..]
                .find(['\r', '\n'])
                .map(|length| value_start + length)
                .unwrap_or(value.len())
        } else if let Some(quote) = quoted {
            value[value_start..]
                .find(quote)
                .map(|length| value_start + length)
                .unwrap_or(value.len())
        } else {
            value[value_start..]
                .find(|character: char| {
                    character.is_whitespace()
                        || matches!(character, ',' | ';' | '&' | '\r' | '\n' | '}' | ']')
                })
                .map(|length| value_start + length)
                .unwrap_or(value.len())
        };
        if value_start < value_end {
            value.replace_range(value_start..value_end, "<redacted>");
            cursor = value_start + "<redacted>".len();
        } else {
            cursor = marker_end;
        }
    }
    value
}

#[derive(Clone, Debug)]
struct FixtureConnectorTransport {
    inner: FixtureConnectorRuntime,
}

impl ConnectorRuntimeTransport for FixtureConnectorTransport {
    fn runtime_id(&self) -> &'static str {
        // The fixture is constructed only for the two fixed connector types.
        if self.inner.runtime_id == "codex" {
            "codex"
        } else {
            "kun"
        }
    }

    fn owned(&self) -> bool {
        true
    }

    fn discover(&self) -> Result<RuntimeDiscovery, RuntimeError> {
        Ok(RuntimeDiscovery {
            runtime_id: self.inner.runtime_id.clone(),
            version: Some(self.inner.runtime_version.clone()),
            owned: true,
        })
    }

    fn list_models(&self) -> Result<RuntimeModelCatalog, RuntimeError> {
        Ok(RuntimeModelCatalog {
            models: self.inner.model_ids.clone(),
            revision: Some("fixture".into()),
            default_model_id: self.inner.model_ids.first().cloned(),
            model_metadata: self
                .inner
                .model_ids
                .iter()
                .cloned()
                .map(|model_id| RuntimeModelMetadata {
                    model_id,
                    available: true,
                    enabled: true,
                    status: Some("available".into()),
                    capabilities: self.inner.capabilities.clone(),
                })
                .collect(),
        })
    }

    fn stream(
        &self,
        request: &RuntimeRequest,
        cancellation: Arc<AtomicBool>,
        producer: &RuntimeEventProducer,
    ) -> Result<(), RuntimeError> {
        let events = self.inner.execute_configured(request)?;
        for event in events {
            if cancellation.load(Ordering::Acquire) {
                return Err(RuntimeError::Cancelled);
            }
            producer.push(event)?;
            if !self.inner.inter_event_delay.is_zero() {
                thread::sleep(self.inner.inter_event_delay);
            }
        }
        Ok(())
    }

    fn cancel(&self, _request: &RuntimeRequest) -> Result<(), RuntimeError> {
        if self.inner.fixture.is_some() {
            Ok(())
        } else {
            Err(RuntimeError::Unsupported)
        }
    }

    fn shutdown_owned(&self) -> Result<(), RuntimeError> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct CodexAppServerConfig {
    /// An explicit binary wins over environment discovery. It is not opened or
    /// checked until a health/catalog/execution operation is requested.
    pub binary_path: Option<PathBuf>,
    /// Prefix arguments allow a local fixture executable to host the same
    /// JSON-RPC contract without making fixture transport a production path.
    pub command_args: Vec<String>,
    pub default_model: Option<String>,
    pub request_timeout: Duration,
}

impl Default for CodexAppServerConfig {
    fn default() -> Self {
        Self {
            binary_path: None,
            command_args: Vec::new(),
            default_model: None,
            request_timeout: Duration::from_millis(DEFAULT_RUNTIME_TIMEOUT_MS),
        }
    }
}

#[derive(Clone, Debug)]
pub struct CodexAppServerRuntime {
    inner: TransportBackedRuntime,
}

impl CodexAppServerRuntime {
    /// Production constructor. It records configuration only; no process is
    /// started and no Codex account/provider material is read here.
    pub fn new(model_id: impl Into<String>) -> Self {
        let model_id = model_id.into();
        Self::with_config(CodexAppServerConfig {
            default_model: safe_model_identifier(&model_id),
            ..CodexAppServerConfig::default()
        })
    }

    pub fn with_config(config: CodexAppServerConfig) -> Self {
        Self::with_transport(Arc::new(CodexAppServerTransport::new(config)))
    }

    /// Test/development injection seam. Normal desktop construction must use
    /// [`Self::with_config`] or [`Self::new`].
    pub fn with_transport(transport: Arc<dyn ConnectorRuntimeTransport>) -> Self {
        Self {
            inner: TransportBackedRuntime::new(
                "codex",
                RuntimeCapabilities {
                    streaming: true,
                    cancel: true,
                    filesystem: true,
                    shell: true,
                },
                transport,
            ),
        }
    }

    /// Development/test-only deterministic transport. It remains public so
    /// cross-crate integration tests can exercise the frozen IPC contract,
    /// but production registration never calls it.
    #[doc(hidden)]
    pub fn from_fixture(model_id: impl Into<String>, fixture: impl Into<String>) -> Self {
        Self::from_fixture_models(vec![model_id.into()], fixture)
    }

    #[doc(hidden)]
    pub fn from_fixture_models(model_ids: Vec<String>, fixture: impl Into<String>) -> Self {
        Self::from_fixture_models_with_delay(model_ids, fixture, Duration::ZERO)
    }

    #[doc(hidden)]
    pub fn from_fixture_models_with_delay(
        model_ids: Vec<String>,
        fixture: impl Into<String>,
        inter_event_delay: Duration,
    ) -> Self {
        let fixture = FixtureConnectorRuntime::new(
            "codex",
            "app-server-fixture-v1",
            "codex-fixture",
            "fixture-default",
            RuntimeCapabilities {
                streaming: true,
                cancel: true,
                filesystem: true,
                shell: true,
            },
        )
        .with_models(model_ids)
        .with_fixture(fixture)
        .with_inter_event_delay(inter_event_delay);
        Self::with_transport(Arc::new(FixtureConnectorTransport { inner: fixture }))
    }

    #[doc(hidden)]
    pub fn execute_from_fixture(
        &self,
        request: &RuntimeRequest,
        fixture: &str,
    ) -> Result<Vec<RuntimeEvent>, RuntimeError> {
        let fixture = FixtureConnectorRuntime::new(
            "codex",
            "app-server-fixture-v1",
            "codex-fixture",
            request
                .model_id
                .clone()
                .unwrap_or_else(|| "fixture-default".into()),
            RuntimeCapabilities {
                streaming: true,
                cancel: true,
                filesystem: true,
                shell: true,
            },
        )
        .with_fixture(fixture);
        fixture.execute_fixture(request, fixture.fixture.as_deref().unwrap_or_default())
    }

    pub fn catalog_revision(&self) -> Option<String> {
        self.inner.cached_catalog_revision()
    }
}

impl RuntimeAdapter for CodexAppServerRuntime {
    fn id(&self) -> &str {
        self.inner.id()
    }
    fn capabilities(&self) -> RuntimeCapabilities {
        self.inner.capabilities()
    }
    fn discover(&self) -> RuntimeDiscovery {
        self.inner.discover()
    }
    fn health(&self) -> RuntimeHealth {
        self.inner.health()
    }
    fn ensure_available(&self) -> Result<(), RuntimeError> {
        self.inner.ensure_available()
    }
    fn list_models(&self) -> Vec<String> {
        self.inner.list_models()
    }
    fn list_models_checked(&self) -> Result<Vec<String>, RuntimeError> {
        self.inner.list_models_checked()
    }
    fn catalog_revision(&self) -> Option<u64> {
        RuntimeAdapter::catalog_revision(&self.inner)
    }
    fn catalog_default_model_id(&self) -> Option<String> {
        RuntimeAdapter::catalog_default_model_id(&self.inner)
    }
    fn catalog_model_metadata(&self, model_id: &str) -> Option<RuntimeModelMetadata> {
        RuntimeAdapter::catalog_model_metadata(&self.inner, model_id)
    }
    fn execute(&self, request: &RuntimeRequest) -> Result<Vec<RuntimeEvent>, RuntimeError> {
        self.inner.execute(request)
    }
    fn stream_events_with_capacity(
        &self,
        request: &RuntimeRequest,
        capacity: usize,
    ) -> Result<RuntimeEventStream, RuntimeError> {
        self.inner.stream_events_with_capacity(request, capacity)
    }
    fn cancel(&self, request: &RuntimeRequest) -> Result<RuntimeEvent, RuntimeError> {
        self.inner.cancel(request)
    }
    fn shutdown_owned(&self) -> Result<(), RuntimeError> {
        self.inner.shutdown_owned()
    }
}

#[derive(Clone)]
struct CodexAppServerTransport {
    config: CodexAppServerConfig,
    state: Arc<Mutex<CodexTransportState>>,
}

#[derive(Default)]
struct CodexTransportState {
    sessions: HashMap<u32, Arc<AppServerSession>>,
    active_runs: HashMap<String, CodexActiveRun>,
}

#[derive(Clone)]
struct CodexActiveRun {
    session: Arc<AppServerSession>,
    thread_id: String,
    turn_id: String,
}

impl CodexAppServerTransport {
    fn new(config: CodexAppServerConfig) -> Self {
        Self {
            config,
            state: Arc::new(Mutex::new(CodexTransportState::default())),
        }
    }

    fn lock_state(&self) -> MutexGuard<'_, CodexTransportState> {
        match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn binary_path(&self) -> Result<PathBuf, RuntimeError> {
        if let Some(path) = self.config.binary_path.as_deref() {
            if is_real_regular_file(path) {
                return Ok(path.to_path_buf());
            }
        }
        for variable in [
            "AGENTTALK_CODEX_BINARY",
            "CODEX_BINARY_PATH",
            "CODEX_BINARY",
        ] {
            if let Some(value) = std::env::var_os(variable) {
                let path = PathBuf::from(value);
                if is_real_regular_file(&path) {
                    return Ok(path);
                }
            }
        }
        if let Some(path) = find_codex_on_process_path() {
            return Ok(path);
        }
        #[cfg(windows)]
        if let Some(local_app_data) = std::env::var_os("LOCALAPPDATA") {
            if let Some(path) = find_codex_desktop_binary(&PathBuf::from(local_app_data)) {
                return Ok(path);
            }
        }
        Err(RuntimeError::Transport(CODEX_RUNTIME_UNAVAILABLE.into()))
    }

    fn open_session(&self) -> Result<(Arc<AppServerSession>, Option<String>), RuntimeError> {
        self.open_session_until(TransportDeadline::after(self.config.request_timeout))
    }

    fn open_session_until(
        &self,
        deadline: TransportDeadline,
    ) -> Result<(Arc<AppServerSession>, Option<String>), RuntimeError> {
        let binary = self.binary_path()?;
        let mut command = Command::new(binary);
        command
            .args(&self.config.command_args)
            .arg("app-server")
            .arg("--listen")
            .arg("stdio://")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        configure_codex_child_environment(&mut command);
        let mut child = command
            .spawn()
            .map_err(|_| RuntimeError::Transport(CODEX_RUNTIME_UNAVAILABLE.into()))?;
        let pid = child.id();
        let Some(stdin) = child.stdin.take() else {
            terminate_spawned_child(&mut child);
            return Err(RuntimeError::Transport(CODEX_RUNTIME_UNAVAILABLE.into()));
        };
        let Some(stdout) = child.stdout.take() else {
            terminate_spawned_child(&mut child);
            return Err(RuntimeError::Transport(CODEX_RUNTIME_UNAVAILABLE.into()));
        };
        let Some(stderr) = child.stderr.take() else {
            terminate_spawned_child(&mut child);
            return Err(RuntimeError::Transport(CODEX_RUNTIME_UNAVAILABLE.into()));
        };
        let session = Arc::new(AppServerSession::new(pid, child, stdin, stdout, stderr));
        self.lock_state().sessions.insert(pid, Arc::clone(&session));

        let remaining = match deadline.remaining() {
            Ok(remaining) => remaining,
            Err(error) => {
                self.close_session(&session);
                return Err(error);
            }
        };
        let initialization = self.rpc_request(
            &session,
            "initialize",
            json!({
                "clientInfo": {"name": "AgentTalk Core", "version": "1"},
                "capabilities": {"experimentalApi": true},
            }),
            remaining,
        );
        let initialization = match initialization {
            Ok(value) => value,
            Err(error) => {
                self.close_session(&session);
                return Err(error);
            }
        };
        let initialized_timeout = match deadline.remaining() {
            Ok(remaining) => remaining,
            Err(error) => {
                self.close_session(&session);
                return Err(error);
            }
        };
        if let Err(error) = session.send_notification("initialized", json!({}), initialized_timeout)
        {
            // `initialized` is part of the ownership handshake. A broken
            // stdin must not leave a newly spawned helper in our session map.
            self.close_session(&session);
            return Err(error);
        }
        let version = initialization
            .pointer("/serverInfo/version")
            .and_then(Value::as_str)
            .or_else(|| initialization.get("version").and_then(Value::as_str))
            .and_then(safe_model_identifier);
        Ok((session, version))
    }

    fn close_session(&self, session: &Arc<AppServerSession>) {
        self.lock_state().sessions.remove(&session.pid);
        session.terminate();
    }

    fn rpc_request(
        &self,
        session: &Arc<AppServerSession>,
        method: &str,
        params: Value,
        timeout: Duration,
    ) -> Result<Value, RuntimeError> {
        let deadline = TransportDeadline::after(timeout);
        let id = session.next_request_id.fetch_add(1, Ordering::Relaxed);
        session.send_value(
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": method,
                "params": params,
            }),
            deadline.remaining()?,
        )?;
        loop {
            if let Some(value) = session.take_response_from_backlog(id) {
                return rpc_response_result(&value);
            }
            let remaining = deadline.remaining()?;
            let value = session.recv_raw(remaining)?;
            if jsonrpc_response_id(&value) == Some(id) {
                return rpc_response_result(&value);
            }
            if is_jsonrpc_server_request(&value) {
                session.respond_to_server_request(&value)?;
            } else {
                session.push_back(value);
            }
        }
    }

    fn interrupt(
        &self,
        session: &Arc<AppServerSession>,
        thread_id: &str,
        turn_id: &str,
        timeout: Duration,
    ) -> Result<(), RuntimeError> {
        if timeout.is_zero() {
            return Err(RuntimeError::Timeout);
        }
        let id = session.next_request_id.fetch_add(1, Ordering::Relaxed);
        // App-server notifications and the stream reader share one stdio
        // receiver, so waiting for the acknowledgement here can deadlock a
        // concurrent blocking stream. The write itself is authoritative for
        // this best-effort remote interrupt: broken stdin returns an error and
        // is never reported as a successful send.
        session.send_value(
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": "turn/interrupt",
                "params": {"threadId": thread_id, "turnId": turn_id},
            }),
            timeout,
        )
    }

    fn model_page(
        &self,
        value: &Value,
    ) -> (
        Vec<RuntimeModelMetadata>,
        Option<String>,
        Option<String>,
        Option<String>,
    ) {
        let rows = ["models", "items", "data", "results"]
            .iter()
            .find_map(|key| value.get(*key).and_then(Value::as_array))
            .cloned()
            .unwrap_or_default();
        let mut row_default = None;
        let models = rows
            .iter()
            .filter_map(|row| {
                let id = row
                    .as_str()
                    .or_else(|| row.get("id").and_then(Value::as_str))
                    .or_else(|| row.get("model").and_then(Value::as_str))
                    .or_else(|| row.get("name").and_then(Value::as_str))
                    .and_then(safe_model_identifier)?;
                let status = row
                    .get("status")
                    .and_then(Value::as_str)
                    .and_then(safe_model_identifier);
                let available = row
                    .get("available")
                    .and_then(Value::as_bool)
                    .unwrap_or(!matches!(status.as_deref(), Some("unavailable")));
                let enabled = row.get("enabled").and_then(Value::as_bool).unwrap_or(true);
                let is_default = row
                    .get("isDefault")
                    .and_then(Value::as_bool)
                    .or_else(|| row.get("default").and_then(Value::as_bool))
                    .or_else(|| row.get("configuredDefault").and_then(Value::as_bool))
                    .unwrap_or(false);
                if is_default && row_default.is_none() {
                    row_default = Some(id.clone());
                }
                Some(RuntimeModelMetadata {
                    model_id: id,
                    available,
                    enabled,
                    status,
                    capabilities: runtime_capabilities_from_value(
                        row.get("capabilities"),
                        RuntimeCapabilities {
                            streaming: true,
                            cancel: true,
                            filesystem: true,
                            shell: true,
                        },
                    ),
                })
            })
            .collect::<Vec<_>>();
        let cursor = value
            .get("nextCursor")
            .and_then(Value::as_str)
            .or_else(|| value.get("next_cursor").and_then(Value::as_str))
            .or_else(|| value.get("cursor").and_then(Value::as_str))
            .and_then(safe_model_identifier);
        let root_default = ["defaultModelId", "defaultModel", "configuredDefault"]
            .iter()
            .find_map(|key| value.get(*key).and_then(Value::as_str))
            .and_then(safe_model_identifier)
            .or(row_default);
        let revision = value
            .get("catalogRevision")
            .or_else(|| value.get("revision"))
            .and_then(|value| match value {
                Value::String(value) => safe_model_identifier(value),
                Value::Number(value) => safe_model_identifier(&value.to_string()),
                _ => None,
            });
        (models, cursor, root_default, revision)
    }

    fn list_models_with_timeout(
        &self,
        timeout: Duration,
    ) -> Result<RuntimeModelCatalog, RuntimeError> {
        self.list_models_until(TransportDeadline::after(timeout))
    }

    fn list_models_until(
        &self,
        deadline: TransportDeadline,
    ) -> Result<RuntimeModelCatalog, RuntimeError> {
        let (session, _version) = self.open_session_until(deadline)?;
        let result = (|| {
            let mut cursor: Option<String> = None;
            let mut seen = BTreeMap::new();
            let mut cursors = BTreeSet::new();
            let mut default_model_id = None;
            let mut revision = None;
            for _ in 0..25 {
                let params = cursor
                    .as_ref()
                    .map(|cursor| json!({"cursor": cursor}))
                    .unwrap_or_else(|| json!({}));
                let page =
                    self.rpc_request(&session, "model/list", params, deadline.remaining()?)?;
                let (models, next_cursor, page_default, page_revision) = self.model_page(&page);
                for metadata in models {
                    seen.insert(metadata.model_id.clone(), metadata);
                }
                default_model_id = default_model_id.or(page_default);
                revision = revision.or(page_revision);
                let Some(next_cursor) = next_cursor else {
                    break;
                };
                if !cursors.insert(next_cursor.clone()) {
                    break;
                }
                cursor = Some(next_cursor);
            }
            if seen.is_empty() {
                return Err(RuntimeError::Transport(CODEX_CATALOG_UNAVAILABLE.into()));
            }
            Ok(RuntimeModelCatalog {
                models: seen.keys().cloned().collect(),
                revision,
                default_model_id: default_model_id
                    .or_else(|| self.config.default_model.clone())
                    .filter(|model_id| seen.contains_key(model_id)),
                model_metadata: seen.into_values().collect(),
            })
        })();
        self.close_session(&session);
        result
    }
}

impl ConnectorRuntimeTransport for CodexAppServerTransport {
    fn runtime_id(&self) -> &'static str {
        "codex"
    }

    fn owned(&self) -> bool {
        true
    }

    fn discover(&self) -> Result<RuntimeDiscovery, RuntimeError> {
        let (session, version) = self.open_session()?;
        self.close_session(&session);
        Ok(RuntimeDiscovery {
            runtime_id: "codex".into(),
            version,
            owned: true,
        })
    }

    fn list_models(&self) -> Result<RuntimeModelCatalog, RuntimeError> {
        self.list_models_with_timeout(self.config.request_timeout)
    }

    fn stream(
        &self,
        request: &RuntimeRequest,
        cancellation: Arc<AtomicBool>,
        producer: &RuntimeEventProducer,
    ) -> Result<(), RuntimeError> {
        if cancellation.load(Ordering::Acquire) {
            return Err(RuntimeError::Cancelled);
        }
        let cwd = request
            .canonical_cwd
            .as_deref()
            .filter(|cwd| !cwd.trim().is_empty())
            .ok_or(RuntimeError::InvalidWorkspace)?;
        let model = request
            .model_id
            .as_deref()
            .filter(|model| !model.trim().is_empty())
            .ok_or_else(|| RuntimeError::Protocol(CODEX_MODEL_UNAVAILABLE.into()))?;
        let total_deadline = TransportDeadline::after(
            bounded_request_timeout(request.timeout_ms).min(self.config.request_timeout),
        );
        let setup_deadline = total_deadline.capped(MAX_TRANSPORT_SETUP_TIMEOUT);
        let catalog = self.list_models_until(setup_deadline)?;
        if cancellation.load(Ordering::Acquire) {
            return Err(RuntimeError::Cancelled);
        }
        if !catalog.models.iter().any(|candidate| candidate == model) {
            return Err(RuntimeError::Protocol(CODEX_MODEL_UNAVAILABLE.into()));
        }
        let (session, _version) = self.open_session_until(setup_deadline)?;
        let result = (|| {
            if cancellation.load(Ordering::Acquire) {
                return Err(RuntimeError::Cancelled);
            }
            let sandbox = match request.workspace_access {
                WorkspaceAccess::WorkspaceWrite => "workspace-write",
                WorkspaceAccess::ReadOnly | WorkspaceAccess::None => "read-only",
            };
            let thread = self.rpc_request(
                &session,
                "thread/start",
                json!({
                    "cwd": cwd,
                    "approvalPolicy": "never",
                    "sandbox": sandbox,
                    "ephemeral": true,
                    "model": model,
                }),
                setup_deadline.remaining()?,
            )?;
            let thread_id = json_string_at(&thread, &["id", "threadId", "thread_id", "thread.id"])
                .ok_or_else(|| RuntimeError::Protocol(CODEX_PROTOCOL_ERROR.into()))?;
            if cancellation.load(Ordering::Acquire) {
                return Err(RuntimeError::Cancelled);
            }
            let turn = self.rpc_request(
                &session,
                "turn/start",
                json!({
                    "threadId": thread_id,
                    "input": [{"type": "text", "text": request.rendered_context}],
                }),
                setup_deadline.remaining()?,
            )?;
            let turn_id = json_string_at(&turn, &["id", "turnId", "turn_id", "turn.id"])
                .ok_or_else(|| RuntimeError::Protocol(CODEX_PROTOCOL_ERROR.into()))?;
            self.lock_state().active_runs.insert(
                request.execution_run_id.clone(),
                CodexActiveRun {
                    session: Arc::clone(&session),
                    thread_id: thread_id.clone(),
                    turn_id: turn_id.clone(),
                },
            );

            producer.push(connector_started_event(
                "codex",
                request,
                Some(thread_id.clone()),
                Some(turn_id.clone()),
            ))?;
            producer.push(runtime_started_event(
                "codex",
                "codex-app-server",
                request,
                Some(thread_id.clone()),
                Some(turn_id.clone()),
            ))?;

            let mut event_index = 2u64;
            loop {
                if cancellation.load(Ordering::Acquire) {
                    let _ = self.interrupt(
                        &session,
                        &thread_id,
                        &turn_id,
                        total_deadline
                            .remaining()
                            .unwrap_or(Duration::from_millis(1)),
                    );
                    return Err(RuntimeError::Cancelled);
                }
                let remaining = match total_deadline.remaining() {
                    Ok(remaining) => remaining,
                    Err(error) => {
                        let _ = self.interrupt(
                            &session,
                            &thread_id,
                            &turn_id,
                            Duration::from_millis(1),
                        );
                        return Err(error);
                    }
                };
                let Some(value) =
                    session.recv_optional(remaining.min(Duration::from_millis(100)))?
                else {
                    continue;
                };
                if is_jsonrpc_server_request(&value) {
                    session.respond_to_server_request(&value)?;
                    continue;
                }
                let Some(method) = value.get("method").and_then(Value::as_str) else {
                    continue;
                };
                let params = value.get("params").cloned().unwrap_or(Value::Null);
                // Error notifications in current app-server builds may carry
                // their turn envelope under `error` or omit it entirely. A
                // retryable notification is non-terminal, so never discard it
                // merely because a server changed that optional envelope.
                if method != "error" && !notification_matches_turn(&params, &thread_id, &turn_id) {
                    continue;
                }
                match method {
                    "item/agentMessage/delta"
                    | "response.output_text.delta"
                    | "output.delta"
                    | "content.delta" => {
                        if let Some(delta) = notification_delta(&params) {
                            event_index += 1;
                            producer.push(output_delta_event(
                                "codex",
                                request,
                                Some(thread_id.clone()),
                                Some(turn_id.clone()),
                                event_index,
                                delta,
                            ))?;
                        }
                    }
                    "turn/completed" | "response.completed" | "execution.completed" => {
                        let status = params
                            .pointer("/turn/status")
                            .and_then(Value::as_str)
                            .or_else(|| params.get("status").and_then(Value::as_str));
                        let event_type = match status {
                            Some("failed") | Some("error") => "execution.failed",
                            Some("cancelled") | Some("canceled") => "execution.cancelled",
                            Some("interrupted") => "execution.interrupted",
                            _ => "execution.completed",
                        };
                        let reason = if event_type == "execution.failed" {
                            Some("provider_error")
                        } else {
                            None
                        };
                        event_index += 1;
                        producer.push(terminal_event(
                            "codex",
                            event_type,
                            request,
                            Some(thread_id.clone()),
                            Some(turn_id.clone()),
                            event_index,
                            reason,
                        ))?;
                        return Ok(());
                    }
                    "error" => {
                        if codex_error_will_retry(&params) {
                            // `willRetry=true` is advisory, not a terminal
                            // execution failure. Keep the session alive for
                            // the subsequent delta/completion notifications.
                            continue;
                        }
                        event_index += 1;
                        producer.push(terminal_event(
                            "codex",
                            "execution.failed",
                            request,
                            Some(thread_id.clone()),
                            Some(turn_id.clone()),
                            event_index,
                            Some("provider_error"),
                        ))?;
                        return Ok(());
                    }
                    _ => {}
                }
            }
        })();
        self.lock_state()
            .active_runs
            .remove(&request.execution_run_id);
        self.close_session(&session);
        result
    }

    fn cancel(&self, request: &RuntimeRequest) -> Result<(), RuntimeError> {
        if let Some(active) = self
            .lock_state()
            .active_runs
            .get(&request.execution_run_id)
            .cloned()
        {
            self.interrupt(
                &active.session,
                &active.thread_id,
                &active.turn_id,
                bounded_request_timeout(request.timeout_ms).min(MAX_TRANSPORT_SETUP_TIMEOUT),
            )?;
        }
        Ok(())
    }

    fn shutdown_owned(&self) -> Result<(), RuntimeError> {
        let sessions = {
            let mut state = self.lock_state();
            state.active_runs.clear();
            state
                .sessions
                .drain()
                .map(|(_, session)| session)
                .collect::<Vec<_>>()
        };
        let deadline = TransportDeadline::after(MAX_TRANSPORT_SETUP_TIMEOUT);
        for session in sessions {
            session.terminate_until(deadline);
        }
        Ok(())
    }
}

fn codex_error_will_retry(params: &Value) -> bool {
    params
        .get("willRetry")
        .and_then(Value::as_bool)
        .or_else(|| params.get("will_retry").and_then(Value::as_bool))
        .or_else(|| params.pointer("/error/willRetry").and_then(Value::as_bool))
        .unwrap_or(false)
}

struct AppServerSession {
    pid: u32,
    child: Mutex<Child>,
    stdin: Mutex<ChildStdin>,
    receiver: Mutex<Receiver<StdioInbound>>,
    backlog: Mutex<VecDeque<Value>>,
    next_request_id: AtomicU64,
    stderr_tail: Arc<Mutex<String>>,
}

enum StdioInbound {
    Value(Value),
    Error(RuntimeError),
}

impl AppServerSession {
    fn new(
        pid: u32,
        child: Child,
        stdin: ChildStdin,
        stdout: impl Read + Send + 'static,
        stderr: impl Read + Send + 'static,
    ) -> Self {
        let (sender, receiver) = mpsc::sync_channel(128);
        let stderr_tail = Arc::new(Mutex::new(String::new()));
        spawn_jsonrpc_reader(stdout, sender);
        spawn_bounded_stderr_reader(stderr, Arc::clone(&stderr_tail));
        Self {
            pid,
            child: Mutex::new(child),
            stdin: Mutex::new(stdin),
            receiver: Mutex::new(receiver),
            backlog: Mutex::new(VecDeque::new()),
            next_request_id: AtomicU64::new(1),
            stderr_tail,
        }
    }

    fn send_value(self: &Arc<Self>, value: Value, timeout: Duration) -> Result<(), RuntimeError> {
        if timeout.is_zero() {
            return Err(RuntimeError::Timeout);
        }
        let encoded = serde_json::to_string(&value)
            .map_err(|_| RuntimeError::Protocol(CODEX_PROTOCOL_ERROR.into()))?;
        // A helper that stops draining stdin can otherwise block write_all
        // forever. A timeout watcher owns only this session's child: on
        // expiry it tears down that owned helper, unblocking the writer and
        // preventing an unbounded cancel or shutdown path.
        let completed = Arc::new(AtomicBool::new(false));
        let expired = Arc::new(AtomicBool::new(false));
        let watcher_session = Arc::clone(self);
        let watcher_completed = Arc::clone(&completed);
        let watcher_expired = Arc::clone(&expired);
        thread::spawn(move || {
            thread::sleep(timeout);
            if !watcher_completed.load(Ordering::Acquire) {
                watcher_expired.store(true, Ordering::Release);
                watcher_session.terminate();
            }
        });
        let mut stdin = match self.stdin.lock() {
            Ok(stdin) => stdin,
            Err(poisoned) => poisoned.into_inner(),
        };
        let result = stdin
            .write_all(encoded.as_bytes())
            .and_then(|()| stdin.write_all(b"\n"))
            .and_then(|()| stdin.flush())
            .map_err(|_| RuntimeError::Transport(CODEX_RUNTIME_UNAVAILABLE.into()));
        completed.store(true, Ordering::Release);
        if expired.load(Ordering::Acquire) {
            Err(RuntimeError::Timeout)
        } else {
            result
        }
    }

    fn send_notification(
        self: &Arc<Self>,
        method: &str,
        params: Value,
        timeout: Duration,
    ) -> Result<(), RuntimeError> {
        self.send_value(
            json!({"jsonrpc": "2.0", "method": method, "params": params}),
            timeout,
        )
    }

    fn recv_raw(&self, timeout: Duration) -> Result<Value, RuntimeError> {
        let receiver = match self.receiver.lock() {
            Ok(receiver) => receiver,
            Err(poisoned) => poisoned.into_inner(),
        };
        match receiver.recv_timeout(timeout) {
            Ok(StdioInbound::Value(value)) => Ok(value),
            Ok(StdioInbound::Error(error)) => Err(error),
            Err(mpsc::RecvTimeoutError::Timeout) => Err(RuntimeError::Timeout),
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(RuntimeError::TransportClosed),
        }
    }

    fn recv_optional(&self, timeout: Duration) -> Result<Option<Value>, RuntimeError> {
        if let Some(value) = self.pop_back() {
            return Ok(Some(value));
        }
        let receiver = match self.receiver.lock() {
            Ok(receiver) => receiver,
            Err(poisoned) => poisoned.into_inner(),
        };
        match receiver.recv_timeout(timeout) {
            Ok(StdioInbound::Value(value)) => Ok(Some(value)),
            Ok(StdioInbound::Error(error)) => Err(error),
            Err(mpsc::RecvTimeoutError::Timeout) => Ok(None),
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(RuntimeError::TransportClosed),
        }
    }

    fn take_response_from_backlog(&self, request_id: u64) -> Option<Value> {
        let mut backlog = match self.backlog.lock() {
            Ok(backlog) => backlog,
            Err(poisoned) => poisoned.into_inner(),
        };
        let index = backlog
            .iter()
            .position(|value| jsonrpc_response_id(value) == Some(request_id))?;
        backlog.remove(index)
    }

    fn push_back(&self, value: Value) {
        let mut backlog = match self.backlog.lock() {
            Ok(backlog) => backlog,
            Err(poisoned) => poisoned.into_inner(),
        };
        if backlog.len() < 128 {
            backlog.push_back(value);
        }
    }

    fn pop_back(&self) -> Option<Value> {
        match self.backlog.lock() {
            Ok(mut backlog) => backlog.pop_front(),
            Err(poisoned) => poisoned.into_inner().pop_front(),
        }
    }

    fn respond_to_server_request(self: &Arc<Self>, request: &Value) -> Result<(), RuntimeError> {
        let id = request.get("id").cloned().unwrap_or(Value::Null);
        let method = request
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        // These are the current Codex App Server reverse-RPC methods.  Each
        // known request gets a protocol-shaped, fail-closed answer instead of
        // the old catch-all {decision:"deny"}, which was invalid for several
        // generated request types.  Anything else is explicitly JSON-RPC
        // method-not-supported; we never forge a successful response for an
        // unknown capability.
        let response = match codex_server_request_response(method) {
            CodexServerRequestResponse::Result(result) => {
                json!({"jsonrpc": "2.0", "id": id, "result": result})
            }
            CodexServerRequestResponse::Error { code, message } => {
                json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
            }
        };
        self.send_value(response, MAX_TRANSPORT_SETUP_TIMEOUT)
    }

    fn terminate(&self) {
        self.terminate_until(TransportDeadline::after(Duration::from_millis(1_500)));
    }

    fn terminate_until(&self, deadline: TransportDeadline) {
        // Retain only a bounded redacted diagnostic internally. It is never
        // surfaced in RuntimeError, events, persistence, or IPC.
        let _ = match self.stderr_tail.lock() {
            Ok(tail) => tail.len(),
            Err(poisoned) => poisoned.into_inner().len(),
        };
        let mut child = match self.child.lock() {
            Ok(child) => child,
            Err(poisoned) => poisoned.into_inner(),
        };
        let _ = child.kill();
        // Child::wait can block forever if a broken helper ignores a close
        // request. This is an owned process, so bounded polling is enough to
        // reap the normal path without making Core shutdown unbounded.
        while deadline.remaining().is_ok() {
            match child.try_wait() {
                Ok(Some(_)) | Err(_) => return,
                Ok(None) => thread::sleep(Duration::from_millis(20)),
            }
        }
    }
}

enum CodexServerRequestResponse {
    Result(Value),
    Error { code: i64, message: &'static str },
}

fn codex_server_request_response(method: &str) -> CodexServerRequestResponse {
    match method {
        // The generated approval response uses `decline`, not the old
        // `deny`. Core never grants a provider-originated escalation.
        "item/commandExecution/requestApproval" | "item/fileChange/requestApproval" => {
            CodexServerRequestResponse::Result(json!({"decision": "decline"}))
        }
        // Permission review uses a distinct generated result shape. An empty
        // permission map with strict automatic review disabled is the
        // protocol-valid, fail-closed response; it is not an approval.
        "item/permissions/requestApproval" => CodexServerRequestResponse::Result(json!({
            "permissions": {},
            "scope": "turn",
            "strictAutoReview": false,
        })),
        // Legacy/generated review requests use ReviewDecision rather than the
        // newer approval envelope. Abort is the only local fail-closed result.
        "applyPatchApproval" | "execCommandApproval" => {
            CodexServerRequestResponse::Result(json!({"decision": "abort"}))
        }
        // No interactive UI is delegated to an untrusted app-server. An empty
        // answer map is the fail-closed requestUserInput response shape.
        "item/tool/requestUserInput" => CodexServerRequestResponse::Result(json!({"answers": {}})),
        // MCP's elicitation schema defines `cancel` as an explicit outcome.
        "mcpServer/elicitation/request" => {
            CodexServerRequestResponse::Result(json!({"action": "cancel"}))
        }
        // These known but disabled capability requests receive distinct,
        // standard JSON-RPC errors. That is fail closed and avoids inventing
        // token/attestation/tool output schemas as a success result.
        "item/tool/call" => CodexServerRequestResponse::Error {
            code: -32031,
            message: "dynamic tools are disabled by AgentTalk Core",
        },
        "account/chatgptAuthTokens/refresh" => CodexServerRequestResponse::Error {
            code: -32032,
            message: "authentication refresh is unavailable to AgentTalk Core",
        },
        "attestation/generate" => CodexServerRequestResponse::Error {
            code: -32033,
            message: "attestation is unavailable to AgentTalk Core",
        },
        _ => CodexServerRequestResponse::Error {
            code: -32601,
            message: "Method not found",
        },
    }
}

fn terminate_spawned_child(child: &mut Child) {
    let _ = child.kill();
    let deadline = Instant::now() + Duration::from_millis(1_500);
    while Instant::now() < deadline {
        match child.try_wait() {
            Ok(Some(_)) | Err(_) => return,
            Ok(None) => thread::sleep(Duration::from_millis(20)),
        }
    }
}

fn is_real_regular_file(path: &Path) -> bool {
    fs::metadata(path)
        .map(|metadata| metadata.is_file())
        .unwrap_or(false)
}

fn find_codex_on_process_path() -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    let executable_names: &[&str] = if cfg!(windows) {
        &["codex.exe", "codex"]
    } else {
        &["codex"]
    };
    std::env::split_paths(&path)
        .flat_map(|directory| {
            executable_names
                .iter()
                .map(move |name| directory.join(name))
        })
        .find(|candidate| is_real_regular_file(candidate))
}

#[cfg(windows)]
fn find_codex_desktop_binary(local_app_data: &Path) -> Option<PathBuf> {
    // The official Desktop install uses one immediate version directory under
    // this fixed location.  Do not recursively search disks or user profiles.
    let bin_root = local_app_data.join("OpenAI").join("Codex").join("bin");
    let mut candidates = fs::read_dir(bin_root)
        .ok()?
        .flatten()
        .filter_map(|entry| {
            let file_type = entry.file_type().ok()?;
            if !file_type.is_dir() {
                return None;
            }
            let version = entry.file_name().to_string_lossy().to_string();
            let binary = entry.path().join("codex.exe");
            is_real_regular_file(&binary).then_some((version, binary))
        })
        .collect::<Vec<_>>();
    // Highest numeric version tuple wins; names then break ties in a stable,
    // testable way.  This intentionally does not attempt semver coercion or a
    // broader file-system scan.
    candidates.sort_by(|left, right| {
        codex_version_key(&left.0)
            .cmp(&codex_version_key(&right.0))
            .then_with(|| left.0.cmp(&right.0))
    });
    candidates.pop().map(|(_, binary)| binary)
}

#[cfg(windows)]
fn codex_version_key(version: &str) -> Vec<u64> {
    version
        .split(|character: char| !character.is_ascii_digit())
        .filter(|component| !component.is_empty())
        .filter_map(|component| component.parse::<u64>().ok())
        .collect()
}

const CODEX_CHILD_ENV_WHITELIST: &[&str] = &[
    "PATH",
    "PATHEXT",
    "SystemRoot",
    "WINDIR",
    "ComSpec",
    "SYSTEMDRIVE",
    "TEMP",
    "TMP",
    "USERPROFILE",
    "HOME",
    "HOMEDRIVE",
    "HOMEPATH",
    "USERNAME",
    "USERDOMAIN",
    "APPDATA",
    "LOCALAPPDATA",
    "ProgramData",
    "ProgramFiles",
    "ProgramFiles(x86)",
    "OS",
    "PROCESSOR_ARCHITECTURE",
    "PROCESSOR_ARCHITEW6432",
    "NUMBER_OF_PROCESSORS",
    "LANG",
    "LC_ALL",
    "PSModulePath",
    "CODEX_HOME",
    "CODEX_ACCESS_TOKEN",
    "CODEX_API_KEY",
    "OPENAI_API_KEY",
    "OPENAI_BASE_URL",
    "OPENAI_ORG_ID",
    "OPENAI_PROJECT_ID",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "NO_PROXY",
    "SSL_CERT_FILE",
    "CODEX_CA_CERTIFICATE",
    "NODE_EXTRA_CA_CERTS",
];

fn codex_child_environment_values(
    lookup: impl Fn(&str) -> Option<OsString>,
) -> Vec<(&'static str, OsString)> {
    CODEX_CHILD_ENV_WHITELIST
        .iter()
        .filter_map(|key| lookup(key).map(|value| (*key, value)))
        .collect()
}

fn configure_codex_child_environment(command: &mut Command) {
    command.env_clear();
    for (key, value) in codex_child_environment_values(|key| std::env::var_os(key)) {
        command.env(key, value);
    }
}

fn spawn_jsonrpc_reader(stdout: impl Read + Send + 'static, sender: SyncSender<StdioInbound>) {
    thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) => {
                    let _ = sender.send(StdioInbound::Error(RuntimeError::TransportClosed));
                    return;
                }
                Ok(_) if line.len() > MAX_TRANSPORT_LINE_BYTES => {
                    let _ = sender.send(StdioInbound::Error(RuntimeError::Protocol(
                        CODEX_PROTOCOL_ERROR.into(),
                    )));
                    return;
                }
                Ok(_) => {
                    let line = line.trim();
                    if line.is_empty() {
                        continue;
                    }
                    match serde_json::from_str::<Value>(line) {
                        Ok(value) => {
                            if sender.send(StdioInbound::Value(value)).is_err() {
                                return;
                            }
                        }
                        Err(_) => {
                            let _ = sender.send(StdioInbound::Error(RuntimeError::Protocol(
                                CODEX_PROTOCOL_ERROR.into(),
                            )));
                            return;
                        }
                    }
                }
                Err(_) => {
                    let _ = sender.send(StdioInbound::Error(RuntimeError::TransportClosed));
                    return;
                }
            }
        }
    });
}

fn spawn_bounded_stderr_reader(stderr: impl Read + Send + 'static, tail: Arc<Mutex<String>>) {
    thread::spawn(move || {
        let mut reader = BufReader::new(stderr);
        let mut buffer = [0u8; 1024];
        loop {
            match reader.read(&mut buffer) {
                Ok(0) | Err(_) => return,
                Ok(length) => {
                    let text = redact_external_text(&String::from_utf8_lossy(&buffer[..length]));
                    let mut current = match tail.lock() {
                        Ok(current) => current,
                        Err(poisoned) => poisoned.into_inner(),
                    };
                    current.push_str(&text);
                    if current.len() > MAX_REDACTED_DIAGNOSTIC_BYTES {
                        *current = utf8_tail(&current, MAX_REDACTED_DIAGNOSTIC_BYTES);
                    }
                }
            }
        }
    });
}

fn utf8_tail(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_owned();
    }
    let mut start = value.len().saturating_sub(max_bytes);
    while start < value.len() && !value.is_char_boundary(start) {
        start += 1;
    }
    value[start..].to_owned()
}

fn utf8_prefix(value: &str, max_bytes: usize) -> &str {
    if value.len() <= max_bytes {
        return value;
    }
    let mut end = max_bytes;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

fn jsonrpc_response_id(value: &Value) -> Option<u64> {
    if !value.get("result").is_some() && !value.get("error").is_some() {
        return None;
    }
    value
        .get("id")
        .and_then(Value::as_u64)
        .or_else(|| value.get("id").and_then(Value::as_str)?.parse().ok())
}

fn rpc_response_result(value: &Value) -> Result<Value, RuntimeError> {
    if let Some(error) = value.get("error") {
        let category = error
            .get("message")
            .and_then(Value::as_str)
            .map(str::to_ascii_lowercase)
            .unwrap_or_default();
        if ["auth", "login", "api key", "unauthorized"]
            .iter()
            .any(|marker| category.contains(marker))
        {
            return Err(RuntimeError::Authentication);
        }
        return Err(RuntimeError::Provider("codex_provider_rejected".into()));
    }
    value
        .get("result")
        .cloned()
        .ok_or_else(|| RuntimeError::Protocol(CODEX_PROTOCOL_ERROR.into()))
}

fn is_jsonrpc_server_request(value: &Value) -> bool {
    value.get("id").is_some()
        && value.get("method").and_then(Value::as_str).is_some()
        && value.get("result").is_none()
        && value.get("error").is_none()
}

fn json_string_at(value: &Value, candidates: &[&str]) -> Option<String> {
    candidates.iter().find_map(|candidate| {
        let item = if candidate.contains('.') {
            let pointer = format!("/{}", candidate.replace('.', "/"));
            value.pointer(&pointer)
        } else {
            value.get(*candidate)
        };
        item.and_then(Value::as_str).and_then(safe_model_identifier)
    })
}

fn notification_matches_turn(value: &Value, thread_id: &str, turn_id: &str) -> bool {
    let event_thread_id = value
        .get("threadId")
        .and_then(Value::as_str)
        .or_else(|| value.get("thread_id").and_then(Value::as_str));
    let event_turn_id = value
        .get("turnId")
        .and_then(Value::as_str)
        .or_else(|| value.get("turn_id").and_then(Value::as_str))
        .or_else(|| value.pointer("/turn/id").and_then(Value::as_str));
    matches!(event_thread_id, Some(value) if value == thread_id)
        && matches!(event_turn_id, Some(value) if value == turn_id)
}

fn notification_delta(value: &Value) -> Option<&str> {
    value
        .get("delta")
        .and_then(Value::as_str)
        .or_else(|| value.get("text").and_then(Value::as_str))
        .or_else(|| value.pointer("/item/text").and_then(Value::as_str))
        .or_else(|| value.pointer("/content/text").and_then(Value::as_str))
}

fn runtime_capabilities_from_value(
    value: Option<&Value>,
    fallback: RuntimeCapabilities,
) -> RuntimeCapabilities {
    let Some(value) = value.and_then(Value::as_object) else {
        return fallback;
    };
    RuntimeCapabilities {
        streaming: value
            .get("streaming")
            .and_then(Value::as_bool)
            .unwrap_or(fallback.streaming),
        cancel: value
            .get("cancel")
            .and_then(Value::as_bool)
            .unwrap_or(fallback.cancel),
        filesystem: value
            .get("filesystem")
            .and_then(Value::as_bool)
            .unwrap_or(fallback.filesystem),
        shell: value
            .get("shell")
            .and_then(Value::as_bool)
            .unwrap_or(fallback.shell),
    }
}

fn bounded_request_timeout(timeout_ms: u64) -> Duration {
    Duration::from_millis(timeout_ms.clamp(1, MAX_RUNTIME_TIMEOUT_MS))
}

#[derive(Clone, Debug)]
pub struct KunSharedRuntimeConfig {
    /// Official Kun data directory containing the runtime.json rendezvous
    /// record. The record and its token are read only on use.
    pub data_dir: Option<PathBuf>,
    /// Official Kun install root. Build metadata is read only from
    /// `resources/app.asar.unpacked/kun/dist/runtime-build.json` below this
    /// directory; it is deliberately never read from the mutable data dir.
    pub install_dir: Option<PathBuf>,
    pub default_model: Option<String>,
    pub expected_service_version: String,
    pub expected_build_id: Option<String>,
    pub request_timeout: Duration,
}

impl Default for KunSharedRuntimeConfig {
    fn default() -> Self {
        Self {
            data_dir: None,
            install_dir: None,
            default_model: None,
            expected_service_version: "0.2.34".into(),
            expected_build_id: None,
            request_timeout: Duration::from_millis(DEFAULT_RUNTIME_TIMEOUT_MS),
        }
    }
}

#[derive(Clone, Debug)]
pub struct KunSharedRuntime {
    inner: TransportBackedRuntime,
}

impl KunSharedRuntime {
    /// Production constructor. It only records a possible default model; the
    /// official Shared Runtime is never started, stopped, or probed here.
    pub fn new(model_id: impl Into<String>) -> Self {
        let model_id = model_id.into();
        Self::with_config(KunSharedRuntimeConfig {
            default_model: safe_model_identifier(&model_id),
            ..KunSharedRuntimeConfig::default()
        })
    }

    pub fn with_config(config: KunSharedRuntimeConfig) -> Self {
        Self::with_transport(Arc::new(KunSharedRuntimeTransport::new(config)))
    }

    #[cfg(test)]
    fn with_config_and_before_turn_start_hook(
        config: KunSharedRuntimeConfig,
        hook: Arc<dyn Fn() + Send + Sync>,
    ) -> Self {
        Self::with_transport(Arc::new(
            KunSharedRuntimeTransport::with_before_turn_start_hook(config, hook),
        ))
    }

    /// Test/development injection seam. It has no authority to turn a shared
    /// Runtime into an owned process.
    pub fn with_transport(transport: Arc<dyn ConnectorRuntimeTransport>) -> Self {
        Self {
            inner: TransportBackedRuntime::new(
                "kun",
                RuntimeCapabilities {
                    streaming: true,
                    cancel: true,
                    filesystem: true,
                    shell: true,
                },
                transport,
            ),
        }
    }

    #[doc(hidden)]
    pub fn from_fixture(model_id: impl Into<String>, fixture: impl Into<String>) -> Self {
        Self::from_fixture_models(vec![model_id.into()], fixture)
    }

    #[doc(hidden)]
    pub fn from_fixture_models(model_ids: Vec<String>, fixture: impl Into<String>) -> Self {
        Self::from_fixture_models_with_delay(model_ids, fixture, Duration::ZERO)
    }

    #[doc(hidden)]
    pub fn from_fixture_models_with_delay(
        model_ids: Vec<String>,
        fixture: impl Into<String>,
        inter_event_delay: Duration,
    ) -> Self {
        let fixture = FixtureConnectorRuntime::new(
            "kun",
            "shared-runtime-fixture-v0.2.34",
            "kun-fixture",
            "fixture-default",
            RuntimeCapabilities {
                streaming: true,
                cancel: true,
                filesystem: true,
                shell: true,
            },
        )
        .with_models(model_ids)
        .with_fixture(fixture)
        .with_inter_event_delay(inter_event_delay);
        Self::with_transport(Arc::new(FixtureConnectorTransport { inner: fixture }))
    }

    #[doc(hidden)]
    pub fn execute_from_fixture(
        &self,
        request: &RuntimeRequest,
        fixture: &str,
    ) -> Result<Vec<RuntimeEvent>, RuntimeError> {
        let fixture = FixtureConnectorRuntime::new(
            "kun",
            "shared-runtime-fixture-v0.2.34",
            "kun-fixture",
            request
                .model_id
                .clone()
                .unwrap_or_else(|| "fixture-default".into()),
            RuntimeCapabilities {
                streaming: true,
                cancel: true,
                filesystem: true,
                shell: true,
            },
        )
        .with_fixture(fixture);
        fixture.execute_fixture(request, fixture.fixture.as_deref().unwrap_or_default())
    }

    pub fn catalog_revision(&self) -> Option<String> {
        self.inner.cached_catalog_revision()
    }
}

impl RuntimeAdapter for KunSharedRuntime {
    fn id(&self) -> &str {
        self.inner.id()
    }
    fn capabilities(&self) -> RuntimeCapabilities {
        self.inner.capabilities()
    }
    fn discover(&self) -> RuntimeDiscovery {
        self.inner.discover()
    }
    fn health(&self) -> RuntimeHealth {
        self.inner.health()
    }
    fn ensure_available(&self) -> Result<(), RuntimeError> {
        self.inner.ensure_available()
    }
    fn list_models(&self) -> Vec<String> {
        self.inner.list_models()
    }
    fn list_models_checked(&self) -> Result<Vec<String>, RuntimeError> {
        self.inner.list_models_checked()
    }
    fn catalog_revision(&self) -> Option<u64> {
        RuntimeAdapter::catalog_revision(&self.inner)
    }
    fn catalog_default_model_id(&self) -> Option<String> {
        RuntimeAdapter::catalog_default_model_id(&self.inner)
    }
    fn catalog_model_metadata(&self, model_id: &str) -> Option<RuntimeModelMetadata> {
        RuntimeAdapter::catalog_model_metadata(&self.inner, model_id)
    }
    fn execute(&self, request: &RuntimeRequest) -> Result<Vec<RuntimeEvent>, RuntimeError> {
        self.inner.execute(request)
    }
    fn stream_events_with_capacity(
        &self,
        request: &RuntimeRequest,
        capacity: usize,
    ) -> Result<RuntimeEventStream, RuntimeError> {
        self.inner.stream_events_with_capacity(request, capacity)
    }
    fn cancel(&self, request: &RuntimeRequest) -> Result<RuntimeEvent, RuntimeError> {
        self.inner.cancel(request)
    }
    fn shutdown_owned(&self) -> Result<(), RuntimeError> {
        self.inner.shutdown_owned()
    }
}

#[derive(Clone)]
struct KunSharedRuntimeTransport {
    config: KunSharedRuntimeConfig,
    state: Arc<Mutex<KunTransportState>>,
    #[cfg(test)]
    before_turn_start_hook: Option<Arc<dyn Fn() + Send + Sync>>,
}

#[derive(Default)]
struct KunTransportState {
    active_runs: HashMap<String, KunActiveRun>,
    turn_starting: HashSet<String>,
    cancellations: HashMap<String, KunCancellationState>,
}

#[derive(Clone)]
enum KunCancellationState {
    PendingSetup,
    InterruptInFlight,
    RemoteInterruptAccepted,
    Failed(RuntimeError),
}

#[derive(Clone)]
struct KunActiveRun {
    connection: KunConnection,
    thread_id: String,
    turn_id: String,
}

#[derive(Clone)]
struct KunConnection {
    endpoint: LocalHttpEndpoint,
    runtime_token: String,
    data_dir: PathBuf,
    instance_id: String,
    pid: u64,
    started_at: String,
    service_version: String,
    build_id: String,
    launch_mode: String,
}

#[derive(Clone, Debug)]
struct KunRuntimeBuildMetadata {
    build_id: String,
    service_version: String,
}

impl KunSharedRuntimeTransport {
    fn new(config: KunSharedRuntimeConfig) -> Self {
        Self {
            config,
            state: Arc::new(Mutex::new(KunTransportState::default())),
            #[cfg(test)]
            before_turn_start_hook: None,
        }
    }

    #[cfg(test)]
    fn with_before_turn_start_hook(
        config: KunSharedRuntimeConfig,
        hook: Arc<dyn Fn() + Send + Sync>,
    ) -> Self {
        Self {
            config,
            state: Arc::new(Mutex::new(KunTransportState::default())),
            before_turn_start_hook: Some(hook),
        }
    }

    fn lock_state(&self) -> MutexGuard<'_, KunTransportState> {
        match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn data_dir(&self) -> PathBuf {
        if let Some(data_dir) = &self.config.data_dir {
            return data_dir.clone();
        }
        if let Some(data_dir) = std::env::var_os("KUN_DATA_DIR") {
            return PathBuf::from(data_dir);
        }
        if let Some(home) = std::env::var_os("USERPROFILE").or_else(|| std::env::var_os("HOME")) {
            return PathBuf::from(home).join(".kun").join("data");
        }
        PathBuf::from(".kun").join("data")
    }

    fn resolve_with_startup_retry(
        &self,
        deadline: TransportDeadline,
    ) -> Result<KunConnection, RuntimeError> {
        let retry_deadline = deadline.capped(KUN_SHARED_RUNTIME_STARTUP_RETRY_WINDOW);
        loop {
            match self.resolve(retry_deadline) {
                Err(RuntimeError::Transport(code)) if code == KUN_SHARED_RUNTIME_UNAVAILABLE => {
                    let Ok(remaining) = retry_deadline.remaining() else {
                        return Err(RuntimeError::Transport(code));
                    };
                    thread::sleep(remaining.min(Duration::from_millis(25)));
                }
                result => return result,
            }
        }
    }

    fn resolve(&self, deadline: TransportDeadline) -> Result<KunConnection, RuntimeError> {
        let data_dir = canonical_or_absolute(&self.data_dir());
        let record = fs::read_to_string(data_dir.join("runtime.json"))
            .map_err(|_| RuntimeError::Transport(KUN_SHARED_RUNTIME_UNAVAILABLE.into()))?;
        if record.len() > MAX_TRANSPORT_BODY_BYTES {
            return Err(RuntimeError::Protocol(KUN_RUNTIME_IDENTITY_MISMATCH.into()));
        }
        let record: Value = serde_json::from_str(&record)
            .map_err(|_| RuntimeError::Protocol(KUN_RUNTIME_IDENTITY_MISMATCH.into()))?;
        let object = record
            .as_object()
            .ok_or_else(|| RuntimeError::Protocol(KUN_RUNTIME_IDENTITY_MISMATCH.into()))?;
        if object.get("version").and_then(Value::as_u64) != Some(2)
            || object.get("insecure").and_then(Value::as_bool) != Some(false)
        {
            return Err(RuntimeError::Protocol(KUN_RUNTIME_IDENTITY_MISMATCH.into()));
        }
        let instance_id = required_safe_string(object, "instanceId")?;
        let pid = object
            .get("pid")
            .and_then(Value::as_u64)
            .filter(|pid| *pid > 0)
            .ok_or_else(|| RuntimeError::Protocol(KUN_RUNTIME_IDENTITY_MISMATCH.into()))?;
        let started_at = required_safe_string(object, "startedAt")?;
        let host = required_safe_string(object, "host")?;
        let port = object
            .get("port")
            .and_then(Value::as_u64)
            .and_then(|port| u16::try_from(port).ok())
            .filter(|port| *port > 0)
            .ok_or_else(|| RuntimeError::Protocol(KUN_RUNTIME_IDENTITY_MISMATCH.into()))?;
        let base_url = required_safe_string(object, "baseUrl")?;
        let runtime_token = required_nonempty_string(object, "runtimeToken")?;
        let service_version = required_safe_string(object, "serviceVersion")?;
        let build_id = required_safe_string(object, "buildId")?;
        let launch_mode = required_safe_string(object, "launchMode")?;
        if !valid_kun_build_id(&build_id) || !process_is_alive(pid) {
            return Err(RuntimeError::Protocol(KUN_RUNTIME_IDENTITY_MISMATCH.into()));
        }
        if service_version != self.config.expected_service_version || launch_mode != "shared" {
            return Err(RuntimeError::Protocol(KUN_RUNTIME_IDENTITY_MISMATCH.into()));
        }
        let endpoint = LocalHttpEndpoint::parse(&base_url)?;
        if !endpoint.matches_host_port(&host, port) {
            return Err(RuntimeError::Protocol(KUN_RUNTIME_IDENTITY_MISMATCH.into()));
        }
        verify_loopback_listener_owner(&endpoint, pid)?;
        // Resolve installation metadata only after the rendezvous PID and its
        // listener have both been verified. This permits a bounded process
        // executable inference without trusting an arbitrary data-dir path.
        let build_metadata = self.runtime_build_metadata(pid, &build_id, &service_version)?;
        let connection = KunConnection {
            endpoint,
            runtime_token,
            data_dir,
            instance_id,
            pid,
            started_at,
            service_version,
            build_id,
            launch_mode,
        };
        let info = self.request_json(
            &connection,
            "GET",
            "/v1/runtime/info",
            None,
            HttpFailureKind::Runtime,
            deadline,
        )?;
        self.verify_runtime_info(&connection, &build_metadata, &info)?;
        Ok(connection)
    }

    fn runtime_build_metadata(
        &self,
        pid: u64,
        runtime_build_id: &str,
        runtime_service_version: &str,
    ) -> Result<KunRuntimeBuildMetadata, RuntimeError> {
        let candidates = self.runtime_install_dir_candidates(pid);
        for (install_dir, explicit) in candidates {
            let metadata_path = kun_runtime_build_metadata_path(&install_dir);
            let metadata = match fs::read_to_string(&metadata_path) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound && !explicit => continue,
                Err(_) => return Err(RuntimeError::Protocol(KUN_RUNTIME_IDENTITY_MISMATCH.into())),
            };
            if metadata.len() > MAX_TRANSPORT_BODY_BYTES {
                return Err(RuntimeError::Protocol(KUN_RUNTIME_IDENTITY_MISMATCH.into()));
            }
            let object = serde_json::from_str::<Value>(&metadata)
                .ok()
                .and_then(|value| value.as_object().cloned())
                .ok_or_else(|| RuntimeError::Protocol(KUN_RUNTIME_IDENTITY_MISMATCH.into()))?;
            let build_id = required_safe_string(&object, "buildId")?;
            let service_version = required_safe_string(&object, "serviceVersion")?;
            if !valid_kun_build_id(&build_id)
                || build_id != runtime_build_id
                || service_version != runtime_service_version
                || self
                    .config
                    .expected_build_id
                    .as_deref()
                    .is_some_and(|expected| expected != build_id)
            {
                return Err(RuntimeError::Protocol(KUN_RUNTIME_IDENTITY_MISMATCH.into()));
            }
            return Ok(KunRuntimeBuildMetadata {
                build_id,
                service_version,
            });
        }

        Err(RuntimeError::Protocol(KUN_RUNTIME_IDENTITY_MISMATCH.into()))
    }

    fn runtime_install_dir_candidates(&self, pid: u64) -> Vec<(PathBuf, bool)> {
        let mut candidates = Vec::new();
        let mut push = |path: PathBuf, explicit: bool| {
            let path = canonical_or_absolute(&path);
            if !candidates
                .iter()
                .any(|(existing, _)| same_local_path(existing, &path))
            {
                candidates.push((path, explicit));
            }
        };
        if let Some(install_dir) = self.config.install_dir.clone() {
            push(install_dir, true);
        }
        if let Some(install_dir) = std::env::var_os("KUN_INSTALL_DIR").map(PathBuf::from) {
            push(install_dir, true);
        }
        if let Some(executable) = process_executable_path(pid) {
            // Do not scan: an official binary can be in a versioned nested
            // directory, so at most four parents are considered as install
            // roots in nearest-first order.
            for parent in executable.ancestors().skip(1).take(5) {
                push(parent.to_path_buf(), false);
            }
        }
        #[cfg(windows)]
        {
            if let Some(local_app_data) = std::env::var_os("LOCALAPPDATA") {
                push(
                    PathBuf::from(local_app_data).join("Programs").join("Kun"),
                    false,
                );
            }
            for variable in ["ProgramFiles", "ProgramFiles(x86)"] {
                if let Some(program_files) = std::env::var_os(variable) {
                    push(PathBuf::from(program_files).join("Kun"), false);
                }
            }
        }
        candidates
    }

    fn verify_runtime_info(
        &self,
        connection: &KunConnection,
        build_metadata: &KunRuntimeBuildMetadata,
        info: &Value,
    ) -> Result<(), RuntimeError> {
        let object = info
            .as_object()
            .ok_or_else(|| RuntimeError::Protocol(KUN_RUNTIME_IDENTITY_MISMATCH.into()))?;
        let data_dir =
            canonical_or_absolute(&PathBuf::from(required_safe_string(object, "dataDir")?));
        let valid = same_local_path(&data_dir, &connection.data_dir)
            && required_safe_string(object, "instanceId")? == connection.instance_id
            && object.get("pid").and_then(Value::as_u64) == Some(connection.pid)
            && required_safe_string(object, "startedAt")? == connection.started_at
            && required_safe_string(object, "serviceVersion")? == connection.service_version
            && required_safe_string(object, "buildId")? == connection.build_id
            && build_metadata.service_version == connection.service_version
            && build_metadata.build_id == connection.build_id
            && required_safe_string(object, "launchMode")? == connection.launch_mode;
        if valid {
            Ok(())
        } else {
            Err(RuntimeError::Protocol(KUN_RUNTIME_IDENTITY_MISMATCH.into()))
        }
    }

    fn request_json(
        &self,
        connection: &KunConnection,
        method: &str,
        path: &str,
        body: Option<Value>,
        failure_kind: HttpFailureKind,
        deadline: TransportDeadline,
    ) -> Result<Value, RuntimeError> {
        let body = body
            .map(|body| {
                serde_json::to_vec(&body)
                    .map_err(|_| RuntimeError::Protocol("transport_json".into()))
            })
            .transpose()?;
        let response = local_http_request(
            &connection.endpoint,
            method,
            path,
            Some(&connection.runtime_token),
            body.as_deref(),
            "application/json",
            deadline,
        )?;
        if !(200..300).contains(&response.status) {
            return Err(map_kun_http_failure(
                response.status,
                failure_kind,
                &response.body,
            ));
        }
        serde_json::from_slice(&response.body)
            .map_err(|_| RuntimeError::Protocol("kun_protocol_error".into()))
    }

    fn catalog(
        &self,
        connection: &KunConnection,
        deadline: TransportDeadline,
    ) -> Result<RuntimeModelCatalog, RuntimeError> {
        let response = self.request_json(
            connection,
            "GET",
            "/v1/model-connections",
            None,
            HttpFailureKind::Catalog,
            deadline,
        )?;
        let object = response
            .as_object()
            .ok_or_else(|| RuntimeError::Transport(KUN_CATALOG_UNAVAILABLE.into()))?;
        let providers = object
            .get("providers")
            .and_then(Value::as_array)
            .ok_or_else(|| RuntimeError::Transport(KUN_CATALOG_UNAVAILABLE.into()))?;
        let root_default_model = ["defaultModelId", "defaultModel"]
            .iter()
            .find_map(|key| object.get(*key).and_then(Value::as_str))
            .and_then(safe_model_identifier);
        let default_provider_id = object
            .get("defaultProviderId")
            .and_then(Value::as_str)
            .and_then(safe_model_identifier);
        let mut models = BTreeMap::new();
        let mut provider_defaults = Vec::new();
        for provider in providers {
            let Some(provider) = provider.as_object() else {
                continue;
            };
            let provider_id = provider
                .get("id")
                .and_then(Value::as_str)
                .and_then(safe_model_identifier);
            let configured = provider.get("configured").and_then(Value::as_bool) != Some(false);
            let provider_enabled = provider.get("enabled").and_then(Value::as_bool) != Some(false);
            let provider_status = provider
                .get("status")
                .and_then(Value::as_str)
                .and_then(safe_model_identifier);
            let provider_default = ["selectedModel", "defaultModelId", "defaultModel"]
                .iter()
                .find_map(|key| provider.get(*key).and_then(Value::as_str))
                .and_then(safe_model_identifier);
            let mut provider_declared_default = provider_default.clone();
            for model in ["models", "availableModels", "available_models", "items"]
                .iter()
                .find_map(|key| provider.get(*key).and_then(Value::as_array))
                .into_iter()
                .flatten()
            {
                let id = model
                    .as_str()
                    .or_else(|| model.get("id").and_then(Value::as_str))
                    .or_else(|| model.get("modelId").and_then(Value::as_str))
                    .or_else(|| model.get("model_id").and_then(Value::as_str))
                    .or_else(|| model.get("name").and_then(Value::as_str));
                let Some(id) = id.and_then(safe_model_identifier) else {
                    continue;
                };
                let status = model
                    .get("status")
                    .and_then(Value::as_str)
                    .and_then(safe_model_identifier)
                    .or_else(|| provider_status.clone());
                let available = model
                    .get("available")
                    .and_then(Value::as_bool)
                    .unwrap_or(configured && !matches!(status.as_deref(), Some("unavailable")));
                let enabled = provider_enabled
                    && model.get("enabled").and_then(Value::as_bool) != Some(false);
                let model_capabilities = provider
                    .get("modelCapabilities")
                    .and_then(Value::as_object)
                    .and_then(|capabilities| capabilities.get(&id))
                    .cloned();
                let is_default = model
                    .get("isDefault")
                    .and_then(Value::as_bool)
                    .or_else(|| model.get("default").and_then(Value::as_bool))
                    .or_else(|| model.get("selected").and_then(Value::as_bool))
                    .unwrap_or(false)
                    || provider_default.as_deref() == Some(id.as_str())
                    || (default_provider_id.as_ref() == provider_id.as_ref()
                        && provider_default.as_deref() == Some(id.as_str()));
                if is_default && provider_declared_default.is_none() {
                    provider_declared_default = Some(id.clone());
                }
                models
                    .entry(id.clone())
                    .or_insert_with(|| RuntimeModelMetadata {
                        model_id: id,
                        available,
                        enabled,
                        status,
                        capabilities: runtime_capabilities_from_value(
                            model
                                .get("capabilities")
                                .or(model_capabilities.as_ref())
                                .or_else(|| provider.get("capabilities")),
                            RuntimeCapabilities {
                                streaming: true,
                                cancel: true,
                                filesystem: true,
                                shell: true,
                            },
                        ),
                    });
            }
            provider_defaults.push((provider_id, provider_declared_default));
        }
        if models.is_empty() {
            return Err(RuntimeError::Transport(KUN_CATALOG_UNAVAILABLE.into()));
        }
        let revision = object
            .get("revision")
            .and_then(|value| match value {
                Value::String(value) => safe_model_identifier(value),
                Value::Number(value) => safe_model_identifier(&value.to_string()),
                _ => None,
            })
            .or_else(|| {
                object
                    .get("catalogRevision")
                    .and_then(Value::as_str)
                    .and_then(safe_model_identifier)
            });
        let selectable = |model_id: &str| {
            models
                .get(model_id)
                .is_some_and(|metadata| metadata.available && metadata.enabled)
        };
        let declared_default = root_default_model
            .filter(|model_id| selectable(model_id))
            .or_else(|| {
                default_provider_id.as_ref().and_then(|default_provider| {
                    provider_defaults
                        .iter()
                        .find(|(provider_id, _)| provider_id.as_ref() == Some(default_provider))
                        .and_then(|(_, model_id)| model_id.clone())
                        .filter(|model_id| selectable(model_id))
                })
            })
            .or_else(|| {
                default_provider_id
                    .is_none()
                    .then(|| {
                        provider_defaults
                            .iter()
                            .filter_map(|(_, model_id)| model_id.clone())
                            .find(|model_id| selectable(model_id))
                    })
                    .flatten()
            })
            .or_else(|| {
                self.config
                    .default_model
                    .clone()
                    .filter(|model_id| selectable(model_id))
            });
        Ok(RuntimeModelCatalog {
            models: models.keys().cloned().collect(),
            revision,
            default_model_id: declared_default,
            model_metadata: models.into_values().collect(),
        })
    }

    fn interrupt(
        &self,
        active: &KunActiveRun,
        deadline: TransportDeadline,
    ) -> Result<(), RuntimeError> {
        let body = serde_json::to_vec(&json!({"discard": false})).unwrap_or_default();
        let response = local_http_request(
            &active.connection.endpoint,
            "POST",
            &format!(
                "/v1/threads/{}/turns/{}/interrupt",
                encode_path_component(&active.thread_id),
                encode_path_component(&active.turn_id)
            ),
            Some(&active.connection.runtime_token),
            Some(&body),
            "application/json",
            deadline,
        )?;
        if !(200..300).contains(&response.status) {
            return Err(map_kun_http_failure(
                response.status,
                HttpFailureKind::Runtime,
                &response.body,
            ));
        }
        Ok(())
    }

    /// Coordinates the single remote interrupt result shared by the command
    /// path and the streaming worker. A `PendingSetup` request is deliberately
    /// local until a turn exists; once an active turn is visible, both paths
    /// wait for the same bounded interrupt result instead of overwriting a
    /// 401/timeout with `Cancelled`.
    fn interrupt_active_run(
        &self,
        run_id: &str,
        active: &KunActiveRun,
        deadline: TransportDeadline,
    ) -> Result<(), RuntimeError> {
        loop {
            enum Action {
                Invoke,
                Wait,
            }
            let action = {
                let mut state = self.lock_state();
                match state.cancellations.get(run_id) {
                    Some(KunCancellationState::RemoteInterruptAccepted) => return Ok(()),
                    Some(KunCancellationState::Failed(error)) => return Err(error.clone()),
                    Some(KunCancellationState::InterruptInFlight) => Action::Wait,
                    Some(KunCancellationState::PendingSetup) | None => {
                        state
                            .cancellations
                            .insert(run_id.to_owned(), KunCancellationState::InterruptInFlight);
                        Action::Invoke
                    }
                }
            };
            match action {
                Action::Invoke => {
                    let result = self.interrupt(active, deadline);
                    let mut state = self.lock_state();
                    match &result {
                        Ok(()) => {
                            state.cancellations.insert(
                                run_id.to_owned(),
                                KunCancellationState::RemoteInterruptAccepted,
                            );
                        }
                        Err(error) => {
                            state.cancellations.insert(
                                run_id.to_owned(),
                                KunCancellationState::Failed(error.clone()),
                            );
                        }
                    }
                    return result;
                }
                Action::Wait => {
                    let remaining = deadline.remaining()?;
                    thread::sleep(remaining.min(Duration::from_millis(5)));
                }
            }
        }
    }

    fn request_cancel(&self, request: &RuntimeRequest) -> Result<(), RuntimeError> {
        let deadline = TransportDeadline::after(
            bounded_request_timeout(request.timeout_ms).min(MAX_TRANSPORT_SETUP_TIMEOUT),
        );
        loop {
            let (active, turn_starting) = {
                let mut state = self.lock_state();
                match state.cancellations.get(&request.execution_run_id) {
                    Some(KunCancellationState::RemoteInterruptAccepted) => return Ok(()),
                    Some(KunCancellationState::Failed(error)) => return Err(error.clone()),
                    _ => {}
                }
                let active = state.active_runs.get(&request.execution_run_id).cloned();
                let turn_starting = state.turn_starting.contains(&request.execution_run_id);
                if active.is_none() {
                    // If a remote turn is already being created, do not claim
                    // local cancellation yet. Wait for the worker to publish
                    // its id and then share the exact interrupt outcome.
                    state
                        .cancellations
                        .entry(request.execution_run_id.clone())
                        .or_insert(KunCancellationState::PendingSetup);
                }
                (active, turn_starting)
            };
            if let Some(active) = active {
                return self.interrupt_active_run(&request.execution_run_id, &active, deadline);
            }
            if !turn_starting {
                // No remote turn exists or is in flight. A local pre-turn
                // cancellation is permitted and intentionally makes no
                // assertion about a remote interrupt acknowledgement.
                return Ok(());
            }
            let remaining = deadline.remaining()?;
            thread::sleep(remaining.min(Duration::from_millis(5)));
        }
    }

    fn clear_run_state(&self, run_id: &str) {
        let mut state = self.lock_state();
        state.active_runs.remove(run_id);
        state.turn_starting.remove(run_id);
        // The remote interrupt result is the shared outcome for this run. The
        // streaming worker can return before Core has persisted it, so dropping
        // either result here would let a racing repeat cancel re-enter the
        // pre-turn path (and potentially issue a second interrupt or
        // manufacture execution.cancelled after a 401). Keep the outcome for
        // the lifetime of this process-local adapter; run ids are immutable
        // execution ids and no active connection/token is retained here.
        if !matches!(
            state.cancellations.get(run_id),
            Some(KunCancellationState::RemoteInterruptAccepted | KunCancellationState::Failed(_))
        ) {
            state.cancellations.remove(run_id);
        }
    }

    fn cancellation_failure(&self, run_id: &str) -> Option<RuntimeError> {
        match self.lock_state().cancellations.get(run_id) {
            Some(KunCancellationState::Failed(error)) => Some(error.clone()),
            _ => None,
        }
    }

    fn cancellation_requested(&self, run_id: &str) -> bool {
        matches!(
            self.lock_state().cancellations.get(run_id),
            Some(
                KunCancellationState::PendingSetup
                    | KunCancellationState::InterruptInFlight
                    | KunCancellationState::RemoteInterruptAccepted
            )
        )
    }

    fn recover_assistant_text(
        &self,
        connection: &KunConnection,
        thread_id: &str,
        turn_id: &str,
        deadline: TransportDeadline,
    ) -> Result<Option<String>, RuntimeError> {
        let turn = self.request_json(
            connection,
            "GET",
            &format!(
                "/v1/threads/{}/turns/{}",
                encode_path_component(thread_id),
                encode_path_component(turn_id)
            ),
            None,
            HttpFailureKind::Runtime,
            deadline,
        )?;
        let items = turn
            .get("items")
            .and_then(Value::as_array)
            .or_else(|| turn.pointer("/turn/items").and_then(Value::as_array))
            .or_else(|| turn.get("messages").and_then(Value::as_array))
            .into_iter()
            .flatten();
        let mut text = String::new();
        for item in items {
            let role = item
                .get("role")
                .and_then(Value::as_str)
                .or_else(|| item.get("author").and_then(Value::as_str));
            let kind = item
                .get("kind")
                .and_then(Value::as_str)
                .or_else(|| item.get("type").and_then(Value::as_str));
            if role != Some("assistant") && kind != Some("assistant_text") {
                continue;
            }
            let item_text = item
                .get("text")
                .and_then(Value::as_str)
                .or_else(|| item.pointer("/content/text").and_then(Value::as_str))
                .or_else(|| item.pointer("/item/text").and_then(Value::as_str));
            if let Some(item_text) = item_text {
                if !text.is_empty() {
                    text.push('\n');
                }
                text.push_str(item_text);
                if text.len() > MAX_TRANSPORT_BODY_BYTES {
                    text = utf8_prefix(&text, MAX_TRANSPORT_BODY_BYTES).to_owned();
                    break;
                }
            }
        }
        if text.is_empty() {
            Err(RuntimeError::Protocol("kun_protocol_error".into()))
        } else {
            Ok(Some(text))
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn finish_completed_turn(
        &self,
        connection: &KunConnection,
        request: &RuntimeRequest,
        thread_id: &str,
        turn_id: &str,
        event_index: &mut u64,
        saw_assistant_delta: bool,
        producer: &RuntimeEventProducer,
        deadline: TransportDeadline,
    ) -> Result<(), RuntimeError> {
        if !saw_assistant_delta {
            if let Some(text) =
                self.recover_assistant_text(connection, thread_id, turn_id, deadline)?
            {
                *event_index += 1;
                producer.push(output_delta_event(
                    "kun",
                    request,
                    Some(thread_id.into()),
                    Some(turn_id.into()),
                    *event_index,
                    &text,
                ))?;
            }
        }
        *event_index += 1;
        producer.push(terminal_event(
            "kun",
            "execution.completed",
            request,
            Some(thread_id.into()),
            Some(turn_id.into()),
            *event_index,
            None,
        ))
    }
}

impl ConnectorRuntimeTransport for KunSharedRuntimeTransport {
    fn runtime_id(&self) -> &'static str {
        "kun"
    }

    fn owned(&self) -> bool {
        // Kun's official Shared Runtime belongs to Kun's Manager/GUI. This
        // adapter is strictly a client and must never advertise ownership.
        false
    }

    fn discover(&self) -> Result<RuntimeDiscovery, RuntimeError> {
        let connection = self.resolve_with_startup_retry(TransportDeadline::after(
            self.config.request_timeout.min(MAX_TRANSPORT_SETUP_TIMEOUT),
        ))?;
        Ok(RuntimeDiscovery {
            runtime_id: "kun".into(),
            version: Some(format!(
                "{}:{}",
                connection.service_version, connection.build_id
            )),
            owned: false,
        })
    }

    fn list_models(&self) -> Result<RuntimeModelCatalog, RuntimeError> {
        // The shared Runtime publishes its rendezvous record and accepts its
        // loopback listener independently. A just-published record can win a
        // short race with listener readiness. Retry only that one classified
        // local-unavailable result, under one shared deadline. Authentication,
        // identity, and catalog failures stay exact and are never retried as
        // another type.
        let deadline =
            TransportDeadline::after(self.config.request_timeout.min(MAX_TRANSPORT_SETUP_TIMEOUT));
        loop {
            match self
                .resolve(deadline)
                .and_then(|connection| self.catalog(&connection, deadline))
            {
                Err(RuntimeError::Transport(code)) if code == KUN_SHARED_RUNTIME_UNAVAILABLE => {
                    let Ok(remaining) = deadline.remaining() else {
                        return Err(RuntimeError::Transport(code));
                    };
                    thread::sleep(remaining.min(Duration::from_millis(25)));
                }
                result => return result,
            }
        }
    }

    fn stream(
        &self,
        request: &RuntimeRequest,
        cancellation: Arc<AtomicBool>,
        producer: &RuntimeEventProducer,
    ) -> Result<(), RuntimeError> {
        let total_deadline = TransportDeadline::after(
            bounded_request_timeout(request.timeout_ms).min(self.config.request_timeout),
        );
        if cancellation.load(Ordering::Acquire)
            || self.cancellation_requested(&request.execution_run_id)
        {
            self.clear_run_state(&request.execution_run_id);
            return Err(RuntimeError::Cancelled);
        }
        let workspace = request
            .canonical_cwd
            .as_deref()
            .filter(|workspace| !workspace.trim().is_empty())
            .ok_or(RuntimeError::InvalidWorkspace)?;
        let connection = self.resolve_with_startup_retry(total_deadline)?;
        if cancellation.load(Ordering::Acquire) {
            self.clear_run_state(&request.execution_run_id);
            return Err(RuntimeError::Cancelled);
        }
        let catalog = self.catalog(&connection, total_deadline)?;
        if cancellation.load(Ordering::Acquire) {
            self.clear_run_state(&request.execution_run_id);
            return Err(RuntimeError::Cancelled);
        }
        let selected_model = request
            .model_id
            .as_deref()
            .filter(|model| !model.trim().is_empty())
            .ok_or_else(|| RuntimeError::Protocol(KUN_MODEL_UNAVAILABLE.into()))?;
        if !catalog.models.iter().any(|model| model == selected_model)
            || catalog
                .model_metadata
                .iter()
                .find(|metadata| metadata.model_id == selected_model)
                .is_some_and(|metadata| !metadata.available || !metadata.enabled)
        {
            return Err(RuntimeError::Protocol(KUN_MODEL_UNAVAILABLE.into()));
        }
        let sandbox = match request.workspace_access {
            WorkspaceAccess::WorkspaceWrite => "workspace-write",
            WorkspaceAccess::ReadOnly | WorkspaceAccess::None => "read-only",
        };
        let thread = self.request_json(
            &connection,
            "POST",
            "/v1/threads",
            Some(json!({
                "title": "AgentTalk",
                "titleAuto": true,
                "workspace": workspace,
                "model": selected_model,
                "mode": "agent",
                "approvalPolicy": "auto",
                "sandboxMode": sandbox,
            })),
            HttpFailureKind::Provider,
            total_deadline,
        )?;
        total_deadline.remaining()?;
        let thread_id = json_string_at(&thread, &["id", "threadId", "thread_id"])
            .ok_or_else(|| RuntimeError::Protocol("kun_protocol_error".into()))?;
        #[cfg(test)]
        if let Some(hook) = self.before_turn_start_hook.as_ref() {
            hook();
        }
        // A cancel can land after the preceding atomic check but before a
        // remote turn exists. Decide that cancellation and publish
        // `turn_starting` under the same state lock: a recorded local cancel
        // must prevent this worker from POSTing a new remote turn, while a
        // cancel that loses this transaction observes `turn_starting` and
        // waits for the one shared interrupt outcome.
        enum BeforeTurnStart {
            Start,
            Cancelled,
            Failed(RuntimeError),
        }
        let before_turn_start = {
            let mut state = self.lock_state();
            match state.cancellations.get(&request.execution_run_id) {
                Some(KunCancellationState::Failed(error)) => BeforeTurnStart::Failed(error.clone()),
                Some(
                    KunCancellationState::PendingSetup
                    | KunCancellationState::InterruptInFlight
                    | KunCancellationState::RemoteInterruptAccepted,
                ) => BeforeTurnStart::Cancelled,
                None if cancellation.load(Ordering::Acquire) => BeforeTurnStart::Cancelled,
                None => {
                    state.turn_starting.insert(request.execution_run_id.clone());
                    BeforeTurnStart::Start
                }
            }
        };
        match before_turn_start {
            BeforeTurnStart::Start => {}
            BeforeTurnStart::Cancelled => {
                self.clear_run_state(&request.execution_run_id);
                return Err(RuntimeError::Cancelled);
            }
            BeforeTurnStart::Failed(error) => {
                self.clear_run_state(&request.execution_run_id);
                return Err(error);
            }
        }
        let turn_result = self.request_json(
            &connection,
            "POST",
            &format!("/v1/threads/{}/turns", encode_path_component(&thread_id)),
            Some(json!({
                "prompt": request.rendered_context,
                "model": selected_model,
                "mode": "agent",
                "disableUserInput": true,
            })),
            HttpFailureKind::Provider,
            total_deadline,
        );
        let turn = match turn_result {
            Ok(turn) => turn,
            Err(error) => {
                self.clear_run_state(&request.execution_run_id);
                return Err(error);
            }
        };
        let turn_id = (|| {
            total_deadline.remaining()?;
            json_string_at(&turn, &["turnId", "turn_id", "turn.id", "id"])
                .ok_or_else(|| RuntimeError::Protocol("kun_protocol_error".into()))
        })();
        let turn_id = match turn_id {
            Ok(turn_id) => turn_id,
            Err(error) => {
                self.clear_run_state(&request.execution_run_id);
                return Err(error);
            }
        };
        {
            let mut state = self.lock_state();
            state.turn_starting.remove(&request.execution_run_id);
            state.active_runs.insert(
                request.execution_run_id.clone(),
                KunActiveRun {
                    connection: connection.clone(),
                    thread_id: thread_id.clone(),
                    turn_id: turn_id.clone(),
                },
            );
        }
        // A Core-side cancellation can close the bounded event stream while
        // the POST /turns response is still in flight. Resolve the recorded
        // cancellation before attempting the first producer push so that a
        // successfully created remote turn is never stranded merely because
        // `connector.started` can no longer be queued.
        if let Some(error) = self.cancellation_failure(&request.execution_run_id) {
            self.clear_run_state(&request.execution_run_id);
            return Err(error);
        }
        if cancellation.load(Ordering::Acquire)
            || self.cancellation_requested(&request.execution_run_id)
        {
            let active = self
                .lock_state()
                .active_runs
                .get(&request.execution_run_id)
                .cloned()
                .ok_or_else(|| RuntimeError::Transport(KUN_SHARED_RUNTIME_UNAVAILABLE.into()))?;
            let result =
                self.interrupt_active_run(&request.execution_run_id, &active, total_deadline);
            self.clear_run_state(&request.execution_run_id);
            result?;
            return Err(RuntimeError::Cancelled);
        }
        let result = (|| {
            producer.push(connector_started_event(
                "kun",
                request,
                Some(thread_id.clone()),
                Some(turn_id.clone()),
            ))?;
            producer.push(runtime_started_event(
                "kun",
                "kun-shared-runtime",
                request,
                Some(thread_id.clone()),
                Some(turn_id.clone()),
            ))?;
            let mut stream = local_http_sse(
                &connection.endpoint,
                &format!(
                    "/v1/threads/{}/events?since_seq=0",
                    encode_path_component(&thread_id)
                ),
                &connection.runtime_token,
                total_deadline,
            )?;
            let mut parser = SseParser::default();
            let mut utf8 = IncrementalUtf8Decoder::default();
            let mut buffer = [0u8; 4096];
            let mut event_index = 2u64;
            let mut saw_assistant_delta = false;
            loop {
                if let Some(error) = self.cancellation_failure(&request.execution_run_id) {
                    return Err(error);
                }
                if cancellation.load(Ordering::Acquire)
                    || self.cancellation_requested(&request.execution_run_id)
                {
                    let active = self
                        .lock_state()
                        .active_runs
                        .get(&request.execution_run_id)
                        .cloned()
                        .ok_or_else(|| {
                            RuntimeError::Transport(KUN_SHARED_RUNTIME_UNAVAILABLE.into())
                        })?;
                    self.interrupt_active_run(&request.execution_run_id, &active, total_deadline)?;
                    return Err(RuntimeError::Cancelled);
                }
                if total_deadline.remaining().is_err() {
                    return Err(RuntimeError::Timeout);
                }
                match stream.read(&mut buffer) {
                    Ok(0) => break,
                    Ok(length) => {
                        let text = utf8.push(&buffer[..length])?;
                        for frame in parser.push_bounded(&text, MAX_TRANSPORT_BODY_BYTES)? {
                            if let Some(terminal) = emit_kun_sse_frame(
                                &frame,
                                request,
                                &thread_id,
                                &turn_id,
                                &mut event_index,
                                &mut saw_assistant_delta,
                                producer,
                            )? {
                                match terminal {
                                    KunSseTerminal::Completed => {
                                        self.finish_completed_turn(
                                            &connection,
                                            request,
                                            &thread_id,
                                            &turn_id,
                                            &mut event_index,
                                            saw_assistant_delta,
                                            producer,
                                            total_deadline,
                                        )?;
                                        return Ok(());
                                    }
                                    KunSseTerminal::Cancelled | KunSseTerminal::Failed => {
                                        return Ok(())
                                    }
                                }
                            }
                        }
                    }
                    Err(error)
                        if error.kind() == std::io::ErrorKind::TimedOut
                            || error.kind() == std::io::ErrorKind::WouldBlock => {}
                    Err(_) => {
                        return Err(RuntimeError::Transport(
                            KUN_SHARED_RUNTIME_UNAVAILABLE.into(),
                        ))
                    }
                }
            }
            utf8.finish()?;
            for frame in parser.finish() {
                if let Some(terminal) = emit_kun_sse_frame(
                    &frame,
                    request,
                    &thread_id,
                    &turn_id,
                    &mut event_index,
                    &mut saw_assistant_delta,
                    producer,
                )? {
                    match terminal {
                        KunSseTerminal::Completed => {
                            self.finish_completed_turn(
                                &connection,
                                request,
                                &thread_id,
                                &turn_id,
                                &mut event_index,
                                saw_assistant_delta,
                                producer,
                                total_deadline,
                            )?;
                            return Ok(());
                        }
                        KunSseTerminal::Cancelled | KunSseTerminal::Failed => return Ok(()),
                    }
                }
            }
            Err(RuntimeError::StreamTerminalMissing)
        })();
        self.clear_run_state(&request.execution_run_id);
        result
    }

    fn cancel(&self, request: &RuntimeRequest) -> Result<(), RuntimeError> {
        self.request_cancel(request)
    }

    fn shutdown_owned(&self) -> Result<(), RuntimeError> {
        // Do not stop, restart, delete, or otherwise mutate an external Kun
        // Shared Runtime. We may only best-effort interrupt turns that this
        // adapter itself created, then discard local bookkeeping.
        let active_runs = {
            let mut state = self.lock_state();
            state.turn_starting.clear();
            state.cancellations.clear();
            state
                .active_runs
                .drain()
                .map(|(_, active)| active)
                .collect::<Vec<_>>()
        };
        let deadline = TransportDeadline::after(MAX_TRANSPORT_SETUP_TIMEOUT);
        let mut first_error = None;
        for active in &active_runs {
            if deadline.remaining().is_err() {
                first_error.get_or_insert(RuntimeError::Timeout);
                break;
            }
            if let Err(error) = self.interrupt(active, deadline) {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum KunSseTerminal {
    Completed,
    Cancelled,
    Failed,
}

fn emit_kun_sse_frame(
    frame: &SseFrame,
    request: &RuntimeRequest,
    thread_id: &str,
    turn_id: &str,
    event_index: &mut u64,
    saw_assistant_delta: &mut bool,
    producer: &RuntimeEventProducer,
) -> Result<Option<KunSseTerminal>, RuntimeError> {
    let value: Value = serde_json::from_str(&frame.data)
        .map_err(|_| RuntimeError::Protocol("kun_protocol_error".into()))?;
    let kind = value
        .get("kind")
        .and_then(Value::as_str)
        .or_else(|| value.get("type").and_then(Value::as_str))
        .unwrap_or_default();
    let event_turn_id = value
        .get("turnId")
        .and_then(Value::as_str)
        .or_else(|| value.get("turn_id").and_then(Value::as_str));
    if event_turn_id.map(|value| value != turn_id).unwrap_or(false) {
        return Ok(None);
    }
    match kind {
        "assistant_text_delta" | "output.delta" | "content.delta" => {
            let delta = value
                .pointer("/item/text")
                .and_then(Value::as_str)
                .or_else(|| value.get("delta").and_then(Value::as_str))
                .or_else(|| value.get("text").and_then(Value::as_str));
            if let Some(delta) = delta {
                *saw_assistant_delta = true;
                *event_index += 1;
                producer.push(output_delta_event(
                    "kun",
                    request,
                    Some(thread_id.into()),
                    Some(turn_id.into()),
                    *event_index,
                    delta,
                ))?;
            }
            Ok(None)
        }
        "turn_completed" | "execution.completed" => {
            // The caller may need to recover output before it emits the sole
            // terminal event, so completion itself carries no side effect.
            Ok(Some(KunSseTerminal::Completed))
        }
        "turn_cancelled" | "turn_canceled" | "execution.cancelled" => {
            *event_index += 1;
            producer.push(terminal_event(
                "kun",
                "execution.cancelled",
                request,
                Some(thread_id.into()),
                Some(turn_id.into()),
                *event_index,
                Some("provider_cancelled"),
            ))?;
            Ok(Some(KunSseTerminal::Cancelled))
        }
        "turn_failed" | "error" | "execution.failed" => {
            // Kun may surface a Provider error as an SSE terminal instead of
            // an HTTP response. Only the narrow, body-free allowlist is
            // promoted to the provider-auth category; every other payload
            // remains the generic provider failure terminal.
            if kun_provider_authentication_value(&value) {
                return Err(RuntimeError::Provider(
                    KUN_PROVIDER_AUTHENTICATION_FAILED.into(),
                ));
            }
            *event_index += 1;
            producer.push(terminal_event(
                "kun",
                "execution.failed",
                request,
                Some(thread_id.into()),
                Some(turn_id.into()),
                *event_index,
                Some("provider_error"),
            ))?;
            Ok(Some(KunSseTerminal::Failed))
        }
        _ => Ok(None),
    }
}

fn required_safe_string(
    object: &serde_json::Map<String, Value>,
    key: &str,
) -> Result<String, RuntimeError> {
    object
        .get(key)
        .and_then(Value::as_str)
        .and_then(safe_model_identifier)
        .ok_or_else(|| RuntimeError::Protocol(KUN_RUNTIME_IDENTITY_MISMATCH.into()))
}

fn required_nonempty_string(
    object: &serde_json::Map<String, Value>,
    key: &str,
) -> Result<String, RuntimeError> {
    object
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| {
            !value.is_empty() && value.len() <= 16_384 && !value.chars().any(char::is_control)
        })
        .map(str::to_owned)
        .ok_or(RuntimeError::Authentication)
}

fn canonical_or_absolute(path: &PathBuf) -> PathBuf {
    fs::canonicalize(path).unwrap_or_else(|_| path.clone())
}

fn same_local_path(left: &PathBuf, right: &PathBuf) -> bool {
    if cfg!(windows) {
        left.to_string_lossy()
            .eq_ignore_ascii_case(&right.to_string_lossy())
    } else {
        left == right
    }
}

fn valid_kun_build_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn kun_runtime_build_metadata_path(install_dir: &Path) -> PathBuf {
    install_dir
        .join("resources")
        .join("app.asar.unpacked")
        .join("kun")
        .join("dist")
        .join("runtime-build.json")
}

#[cfg(windows)]
fn process_executable_path(pid: u64) -> Option<PathBuf> {
    use std::ffi::{c_void, OsString};
    use std::os::windows::ffi::OsStringExt;

    const PROCESS_QUERY_LIMITED_INFORMATION: u32 = 0x1000;
    unsafe extern "system" {
        fn OpenProcess(desired_access: u32, inherit_handle: i32, process_id: u32) -> *mut c_void;
        fn QueryFullProcessImageNameW(
            process: *mut c_void,
            flags: u32,
            executable_path: *mut u16,
            size: *mut u32,
        ) -> i32;
        fn CloseHandle(handle: *mut c_void) -> i32;
    }

    let pid = u32::try_from(pid).ok()?;
    // SAFETY: this uses a verified numeric PID, a bounded stack-owned UTF-16
    // buffer, and closes the query-only process handle on every open path.
    unsafe {
        let handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
        if handle.is_null() {
            return None;
        }
        let mut path = vec![0u16; 32_768];
        let mut length = path.len() as u32;
        let success = QueryFullProcessImageNameW(handle, 0, path.as_mut_ptr(), &mut length) != 0;
        let _ = CloseHandle(handle);
        if !success || length == 0 || length as usize > path.len() {
            return None;
        }
        Some(PathBuf::from(OsString::from_wide(&path[..length as usize])))
    }
}

#[cfg(not(windows))]
fn process_executable_path(_pid: u64) -> Option<PathBuf> {
    None
}

#[cfg(windows)]
fn process_is_alive(pid: u64) -> bool {
    use std::ffi::c_void;

    const PROCESS_QUERY_LIMITED_INFORMATION: u32 = 0x1000;
    const STILL_ACTIVE: u32 = 259;
    unsafe extern "system" {
        fn OpenProcess(desired_access: u32, inherit_handle: i32, process_id: u32) -> *mut c_void;
        fn GetExitCodeProcess(process: *mut c_void, exit_code: *mut u32) -> i32;
        fn CloseHandle(handle: *mut c_void) -> i32;
    }

    let Ok(pid) = u32::try_from(pid) else {
        return false;
    };
    // SAFETY: the Win32 calls use only the numeric PID and a stack-owned
    // output value. The handle is closed on every successful open path.
    unsafe {
        let handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
        if handle.is_null() {
            return false;
        }
        let mut exit_code = 0u32;
        let result = GetExitCodeProcess(handle, &mut exit_code) != 0 && exit_code == STILL_ACTIVE;
        let _ = CloseHandle(handle);
        result
    }
}

#[cfg(not(windows))]
fn process_is_alive(pid: u64) -> bool {
    PathBuf::from("/proc").join(pid.to_string()).is_dir()
}

fn verify_loopback_listener_owner(
    endpoint: &LocalHttpEndpoint,
    expected_pid: u64,
) -> Result<(), RuntimeError> {
    #[cfg(windows)]
    {
        if !windows_listener_owned_by(endpoint.selected_socket, expected_pid) {
            return Err(RuntimeError::Protocol(KUN_RUNTIME_IDENTITY_MISMATCH.into()));
        }
    }
    #[cfg(not(windows))]
    {
        let _ = (endpoint, expected_pid);
    }
    Ok(())
}

#[cfg(windows)]
fn windows_listener_owned_by(socket: SocketAddr, expected_pid: u64) -> bool {
    use std::ffi::c_void;
    use std::mem::size_of;
    use std::net::Ipv4Addr;

    const AF_INET: u32 = 2;
    const TCP_TABLE_OWNER_PID_LISTENER: u32 = 3;
    const ERROR_INSUFFICIENT_BUFFER: u32 = 122;
    #[repr(C)]
    #[derive(Clone, Copy)]
    struct MibTcpRowOwnerPid {
        state: u32,
        local_addr: u32,
        local_port: u32,
        remote_addr: u32,
        remote_port: u32,
        owning_pid: u32,
    }
    #[link(name = "iphlpapi")]
    unsafe extern "system" {
        fn GetExtendedTcpTable(
            tcp_table: *mut c_void,
            table_size: *mut u32,
            order: i32,
            address_family: u32,
            table_class: u32,
            reserved: u32,
        ) -> u32;
    }

    let SocketAddr::V4(socket) = socket else {
        // The current Windows v0.2.34 Runtime rendezvous is IPv4 loopback. An
        // IPv6 endpoint is rejected until the official owner-table variant is
        // available, rather than silently skipping the PID check.
        return false;
    };
    let Ok(expected_pid) = u32::try_from(expected_pid) else {
        return false;
    };
    // SAFETY: first call obtains the bounded buffer length; second call writes
    // exactly that buffer. Rows are read unaligned from the documented table.
    unsafe {
        let mut length = 0u32;
        if GetExtendedTcpTable(
            std::ptr::null_mut(),
            &mut length,
            0,
            AF_INET,
            TCP_TABLE_OWNER_PID_LISTENER,
            0,
        ) != ERROR_INSUFFICIENT_BUFFER
            || length < size_of::<u32>() as u32
        {
            return false;
        }
        let mut bytes = vec![0u8; length as usize];
        if GetExtendedTcpTable(
            bytes.as_mut_ptr().cast::<c_void>(),
            &mut length,
            0,
            AF_INET,
            TCP_TABLE_OWNER_PID_LISTENER,
            0,
        ) != 0
        {
            return false;
        }
        let count = std::ptr::read_unaligned(bytes.as_ptr().cast::<u32>()) as usize;
        let rows_offset = size_of::<u32>();
        let available = bytes.len().saturating_sub(rows_offset) / size_of::<MibTcpRowOwnerPid>();
        let target_ip = *socket.ip();
        let target_port = socket.port();
        (0..count.min(available)).any(|index| {
            let row = std::ptr::read_unaligned(
                bytes
                    .as_ptr()
                    .add(rows_offset + index * size_of::<MibTcpRowOwnerPid>())
                    .cast::<MibTcpRowOwnerPid>(),
            );
            let local_ip = Ipv4Addr::from(row.local_addr.to_ne_bytes());
            let local_port = u16::from_be(row.local_port as u16);
            row.owning_pid == expected_pid && local_ip == target_ip && local_port == target_port
        })
    }
}

#[derive(Clone)]
struct LocalHttpEndpoint {
    selected_socket: SocketAddr,
}

impl LocalHttpEndpoint {
    fn parse(value: &str) -> Result<Self, RuntimeError> {
        let value = value.trim();
        let authority = value
            .strip_prefix("http://")
            .and_then(|value| value.strip_suffix('/').or(Some(value)))
            .filter(|value| !value.contains('/') && !value.contains('@'))
            .ok_or_else(|| RuntimeError::Protocol(KUN_RUNTIME_IDENTITY_MISMATCH.into()))?;
        let (host, port) = if let Some(value) = authority.strip_prefix('[') {
            let (host, port) = value
                .split_once("]:")
                .ok_or_else(|| RuntimeError::Protocol(KUN_RUNTIME_IDENTITY_MISMATCH.into()))?;
            (host.to_owned(), port)
        } else {
            authority
                .rsplit_once(':')
                .map(|(host, port)| (host.to_owned(), port))
                .ok_or_else(|| RuntimeError::Protocol(KUN_RUNTIME_IDENTITY_MISMATCH.into()))?
        };
        let port = port
            .parse::<u16>()
            .ok()
            .filter(|port| *port > 0)
            .ok_or_else(|| RuntimeError::Protocol(KUN_RUNTIME_IDENTITY_MISMATCH.into()))?;
        let selected_socket = resolve_loopback_socket(&host, port)?;
        Ok(Self { selected_socket })
    }

    fn matches_host_port(&self, host: &str, port: u16) -> bool {
        resolve_loopback_socket(host, port)
            .map(|socket| socket == self.selected_socket)
            .unwrap_or(false)
    }

    fn authority(&self) -> String {
        self.selected_socket.to_string()
    }
}

fn resolve_loopback_socket(host: &str, port: u16) -> Result<SocketAddr, RuntimeError> {
    let host = host.trim();
    if host.is_empty() || host.contains('@') || host.contains('/') || host.contains('\\') {
        return Err(RuntimeError::Protocol(KUN_RUNTIME_IDENTITY_MISMATCH.into()));
    }
    if let Ok(ip) = host.parse::<IpAddr>() {
        return ip
            .is_loopback()
            .then_some(SocketAddr::new(ip, port))
            .ok_or_else(|| RuntimeError::Protocol(KUN_RUNTIME_IDENTITY_MISMATCH.into()));
    }
    // Do not resolve arbitrary DNS names before the Runtime token boundary.
    // `localhost` is the only hostname form accepted by the official local
    // rendezvous contract, and every selected address must still be loopback.
    if !host.eq_ignore_ascii_case("localhost") {
        return Err(RuntimeError::Protocol(KUN_RUNTIME_IDENTITY_MISMATCH.into()));
    }
    let mut addresses = ("localhost", port)
        .to_socket_addrs()
        .map_err(|_| RuntimeError::Protocol(KUN_RUNTIME_IDENTITY_MISMATCH.into()))?
        .filter(|address| address.ip().is_loopback())
        .collect::<Vec<_>>();
    addresses.sort_by_key(|address| (!address.is_ipv4(), address.to_string()));
    addresses
        .into_iter()
        .next()
        .ok_or_else(|| RuntimeError::Protocol(KUN_RUNTIME_IDENTITY_MISMATCH.into()))
}

struct LocalHttpResponse {
    status: u16,
    body: Vec<u8>,
}

#[derive(Clone, Copy)]
enum HttpFailureKind {
    Runtime,
    Catalog,
    Provider,
}

fn map_kun_http_failure(status: u16, kind: HttpFailureKind, body: &[u8]) -> RuntimeError {
    match (status, kind) {
        // Every local Shared Runtime endpoint is protected by the Runtime
        // bearer credential. A 401/403 can therefore never be re-labelled as
        // a Provider error merely because the request was a thread/turn.
        (401 | 403, _) => RuntimeError::Authentication,
        (_, HttpFailureKind::Catalog) => RuntimeError::Transport(KUN_CATALOG_UNAVAILABLE.into()),
        (_, HttpFailureKind::Runtime) => {
            RuntimeError::Transport(KUN_SHARED_RUNTIME_UNAVAILABLE.into())
        }
        (_, HttpFailureKind::Provider) if kun_provider_authentication_code(body) => {
            RuntimeError::Provider(KUN_PROVIDER_AUTHENTICATION_FAILED.into())
        }
        _ => RuntimeError::Provider("kun_provider_rejected".into()),
    }
}

fn kun_provider_authentication_code(body: &[u8]) -> bool {
    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        return false;
    };
    kun_provider_authentication_value(&value)
}

fn kun_provider_authentication_value(value: &Value) -> bool {
    let code = value
        .pointer("/error/code")
        .and_then(Value::as_str)
        .or_else(|| value.pointer("/error/type").and_then(Value::as_str))
        .or_else(|| value.get("code").and_then(Value::as_str))
        .map(str::to_ascii_lowercase);
    matches!(
        code.as_deref(),
        Some(
            "provider_authentication_failed"
                | "provider_unauthorized"
                | "invalid_api_key"
                | "invalid_api_key_error"
                | "authentication_error"
        )
    )
}

fn local_http_request(
    endpoint: &LocalHttpEndpoint,
    method: &str,
    path: &str,
    authorization: Option<&str>,
    body: Option<&[u8]>,
    accept: &str,
    deadline: TransportDeadline,
) -> Result<LocalHttpResponse, RuntimeError> {
    let reader = open_local_http(
        endpoint,
        method,
        path,
        authorization,
        body,
        accept,
        deadline,
    )?;
    let status = reader.status;
    let body = read_local_http_body(reader.body, deadline)?;
    Ok(LocalHttpResponse { status, body })
}

fn read_local_http_body(
    mut body: HttpBody,
    deadline: TransportDeadline,
) -> Result<Vec<u8>, RuntimeError> {
    let mut response = Vec::new();
    let mut buffer = [0u8; 4096];
    loop {
        let read = match body.read(&mut buffer) {
            Ok(read) => read,
            Err(error)
                if (error.kind() == std::io::ErrorKind::TimedOut
                    || error.kind() == std::io::ErrorKind::WouldBlock)
                    && deadline.remaining().is_ok() =>
            {
                continue;
            }
            Err(error)
                if error.kind() == std::io::ErrorKind::TimedOut
                    || error.kind() == std::io::ErrorKind::WouldBlock =>
            {
                return Err(RuntimeError::Timeout);
            }
            Err(_) => {
                return Err(RuntimeError::Transport(
                    KUN_SHARED_RUNTIME_UNAVAILABLE.into(),
                ));
            }
        };
        if read == 0 {
            break;
        }
        if response.len() + read > MAX_TRANSPORT_BODY_BYTES {
            return Err(RuntimeError::Protocol("kun_protocol_error".into()));
        }
        response.extend_from_slice(&buffer[..read]);
    }
    Ok(response)
}

struct LocalHttpSse {
    body: HttpBody,
}

impl Read for LocalHttpSse {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        self.body.read(buffer)
    }
}

fn local_http_sse(
    endpoint: &LocalHttpEndpoint,
    path: &str,
    authorization: &str,
    deadline: TransportDeadline,
) -> Result<LocalHttpSse, RuntimeError> {
    let response = open_local_http(
        endpoint,
        "GET",
        path,
        Some(authorization),
        None,
        "text/event-stream",
        deadline,
    )?;
    if !(200..300).contains(&response.status) {
        let body = read_local_http_body(response.body, deadline)?;
        return Err(map_kun_http_failure(
            response.status,
            HttpFailureKind::Provider,
            &body,
        ));
    }
    Ok(LocalHttpSse {
        body: response.body,
    })
}

struct OpenLocalHttpResponse {
    status: u16,
    body: HttpBody,
}

fn open_local_http(
    endpoint: &LocalHttpEndpoint,
    method: &str,
    path: &str,
    authorization: Option<&str>,
    body: Option<&[u8]>,
    accept: &str,
    deadline: TransportDeadline,
) -> Result<OpenLocalHttpResponse, RuntimeError> {
    let connect_timeout = deadline.remaining()?.min(Duration::from_secs(5));
    let address = endpoint.selected_socket;
    // Re-check the final selected socket immediately before the first write.
    // This prevents a hostile runtime.json hostname from ever receiving the
    // Runtime bearer token, workspace path, or rendered context.
    if !address.ip().is_loopback() {
        return Err(RuntimeError::Protocol(KUN_RUNTIME_IDENTITY_MISMATCH.into()));
    }
    let stream = TcpStream::connect_timeout(&address, connect_timeout)
        .map_err(|_| RuntimeError::Transport(KUN_SHARED_RUNTIME_UNAVAILABLE.into()))?;
    stream
        .set_read_timeout(Some(Duration::from_millis(100)))
        .map_err(|_| RuntimeError::Transport(KUN_SHARED_RUNTIME_UNAVAILABLE.into()))?;
    stream
        .set_write_timeout(Some(connect_timeout))
        .map_err(|_| RuntimeError::Transport(KUN_SHARED_RUNTIME_UNAVAILABLE.into()))?;
    let mut writer = stream
        .try_clone()
        .map_err(|_| RuntimeError::Transport(KUN_SHARED_RUNTIME_UNAVAILABLE.into()))?;
    let mut request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {}\r\nAccept: {accept}\r\nConnection: close\r\n",
        endpoint.authority()
    );
    if let Some(authorization) = authorization {
        request.push_str("Authorization: Bearer ");
        request.push_str(authorization);
        request.push_str("\r\n");
    }
    if let Some(body) = body {
        request.push_str("Content-Type: application/json\r\n");
        request.push_str(&format!("Content-Length: {}\r\n", body.len()));
    }
    request.push_str("\r\n");
    writer
        .write_all(request.as_bytes())
        .and_then(|()| match body {
            Some(body) => writer.write_all(body),
            None => Ok(()),
        })
        .and_then(|()| writer.flush())
        .map_err(|_| RuntimeError::Transport(KUN_SHARED_RUNTIME_UNAVAILABLE.into()))?;
    let mut reader = BufReader::new(stream);
    let status = read_http_status(&mut reader, deadline)?;
    let headers = read_http_headers(&mut reader, deadline)?;
    let chunked = headers
        .get("transfer-encoding")
        .map(|value| value.to_ascii_lowercase().contains("chunked"))
        .unwrap_or(false);
    let body = if chunked {
        HttpBody::Chunked(ChunkedBody::new(reader))
    } else {
        HttpBody::Plain(reader)
    };
    Ok(OpenLocalHttpResponse { status, body })
}

fn read_http_status(
    reader: &mut BufReader<TcpStream>,
    deadline: TransportDeadline,
) -> Result<u16, RuntimeError> {
    let mut line = String::new();
    let mut bytes = 0usize;
    loop {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) => {
                return Err(RuntimeError::Transport(
                    KUN_SHARED_RUNTIME_UNAVAILABLE.into(),
                ))
            }
            Ok(_) if line.len() > 8192 => {
                return Err(RuntimeError::Protocol("kun_protocol_error".into()))
            }
            Ok(length) => {
                bytes += length;
                if bytes > 16 * 1024 {
                    return Err(RuntimeError::Protocol("kun_protocol_error".into()));
                }
                if deadline.remaining().is_err() {
                    return Err(RuntimeError::Timeout);
                }
                if line.trim().is_empty() {
                    continue;
                }
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::TimedOut => {
                if deadline.remaining().is_err() {
                    return Err(RuntimeError::Timeout);
                }
            }
            Err(_) => {
                return Err(RuntimeError::Transport(
                    KUN_SHARED_RUNTIME_UNAVAILABLE.into(),
                ))
            }
        }
    }
    line.split_whitespace()
        .nth(1)
        .and_then(|status| status.parse::<u16>().ok())
        .ok_or_else(|| RuntimeError::Protocol("kun_protocol_error".into()))
}

fn read_http_headers(
    reader: &mut BufReader<TcpStream>,
    deadline: TransportDeadline,
) -> Result<HashMap<String, String>, RuntimeError> {
    let mut headers = HashMap::new();
    let mut line = String::new();
    let mut bytes = 0usize;
    loop {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) => {
                return Err(RuntimeError::Transport(
                    KUN_SHARED_RUNTIME_UNAVAILABLE.into(),
                ))
            }
            Ok(length) => {
                bytes += length;
                if bytes > 16 * 1024 {
                    return Err(RuntimeError::Protocol("kun_protocol_error".into()));
                }
                if line == "\r\n" || line == "\n" {
                    return Ok(headers);
                }
                if deadline.remaining().is_err() {
                    return Err(RuntimeError::Timeout);
                }
                if let Some((key, value)) = line.split_once(':') {
                    headers.insert(key.trim().to_ascii_lowercase(), value.trim().to_owned());
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::TimedOut => {
                if deadline.remaining().is_err() {
                    return Err(RuntimeError::Timeout);
                }
            }
            Err(_) => {
                return Err(RuntimeError::Transport(
                    KUN_SHARED_RUNTIME_UNAVAILABLE.into(),
                ))
            }
        }
    }
}

enum HttpBody {
    Plain(BufReader<TcpStream>),
    Chunked(ChunkedBody),
}

impl Read for HttpBody {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(reader) => reader.read(buffer),
            Self::Chunked(reader) => reader.read(buffer),
        }
    }
}

struct ChunkedBody {
    reader: BufReader<TcpStream>,
    remaining: usize,
    finished: bool,
}

impl ChunkedBody {
    fn new(reader: BufReader<TcpStream>) -> Self {
        Self {
            reader,
            remaining: 0,
            finished: false,
        }
    }

    fn next_chunk(&mut self) -> std::io::Result<()> {
        let mut line = String::new();
        self.reader.read_line(&mut line)?;
        let length = line.split(';').next().unwrap_or_default().trim();
        let length = usize::from_str_radix(length, 16)
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "chunk"))?;
        if length == 0 {
            self.finished = true;
            // Consume optional trailers.
            loop {
                line.clear();
                self.reader.read_line(&mut line)?;
                if line == "\r\n" || line == "\n" || line.is_empty() {
                    break;
                }
            }
        } else {
            self.remaining = length;
        }
        Ok(())
    }
}

impl Read for ChunkedBody {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if self.finished {
            return Ok(0);
        }
        if self.remaining == 0 {
            self.next_chunk()?;
            if self.finished {
                return Ok(0);
            }
        }
        let limit = buffer.len().min(self.remaining);
        let read = self.reader.read(&mut buffer[..limit])?;
        if read == 0 {
            return Ok(0);
        }
        self.remaining -= read;
        if self.remaining == 0 {
            let mut ending = [0u8; 2];
            self.reader.read_exact(&mut ending)?;
            if ending != *b"\r\n" {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "chunk",
                ));
            }
        }
        Ok(read)
    }
}

fn encode_path_component(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(byte as char);
        } else {
            encoded.push('%');
            encoded.push_str(&format!("{byte:02X}"));
        }
    }
    encoded
}

#[derive(Clone, Debug)]
pub struct ConfiguredAdapter {
    pub runtime_id: String,
    pub runtime_version: Option<String>,
    pub adapter_capabilities: RuntimeCapabilities,
}

impl ConfiguredAdapter {
    pub fn kun() -> Self {
        Self {
            runtime_id: "kun".into(),
            runtime_version: None,
            adapter_capabilities: RuntimeCapabilities {
                streaming: true,
                cancel: true,
                filesystem: true,
                shell: true,
            },
        }
    }
    pub fn codex() -> Self {
        Self {
            runtime_id: "codex".into(),
            runtime_version: None,
            adapter_capabilities: RuntimeCapabilities {
                streaming: true,
                cancel: true,
                filesystem: true,
                shell: true,
            },
        }
    }
    pub fn http_custom() -> Self {
        Self {
            runtime_id: "http-custom".into(),
            runtime_version: None,
            adapter_capabilities: RuntimeCapabilities {
                streaming: true,
                cancel: false,
                filesystem: false,
                shell: false,
            },
        }
    }
    pub fn openai_compatible() -> Self {
        Self {
            runtime_id: "openai-compatible".into(),
            runtime_version: Some("sse-adapter-v1".into()),
            adapter_capabilities: RuntimeCapabilities {
                streaming: true,
                cancel: true,
                filesystem: false,
                shell: false,
            },
        }
    }
    pub fn acp_mock() -> Self {
        Self {
            runtime_id: "acp-mock".into(),
            runtime_version: Some("mock-contract-v1".into()),
            adapter_capabilities: RuntimeCapabilities {
                streaming: true,
                cancel: true,
                filesystem: false,
                shell: false,
            },
        }
    }
}

impl RuntimeAdapter for ConfiguredAdapter {
    fn id(&self) -> &str {
        &self.runtime_id
    }
    fn capabilities(&self) -> RuntimeCapabilities {
        self.adapter_capabilities.clone()
    }
    fn discover(&self) -> RuntimeDiscovery {
        RuntimeDiscovery {
            runtime_id: self.runtime_id.clone(),
            version: self.runtime_version.clone(),
            owned: self.runtime_id == "codex",
        }
    }
    fn health(&self) -> RuntimeHealth {
        RuntimeHealth {
            runtime_id: self.runtime_id.clone(),
            status: "unverified".into(),
            detail: Some("live adapter contract reserved; no provider call performed".into()),
        }
    }
    fn execute(&self, request: &RuntimeRequest) -> Result<Vec<RuntimeEvent>, RuntimeError> {
        if request.workspace_access == WorkspaceAccess::WorkspaceWrite
            && !self.adapter_capabilities.filesystem
        {
            return Err(RuntimeError::Permission);
        }
        Err(RuntimeError::Unsupported)
    }
    fn cancel(&self, _request: &RuntimeRequest) -> Result<RuntimeEvent, RuntimeError> {
        // Configured adapters reserve live cancellation for the real
        // Provider bridge; no synthetic terminal event is emitted here.
        Err(RuntimeError::Unsupported)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn windows_worker_config_round_trips_explicit_sources() {
        let config = WindowsPassiveDiscoveryConfig {
            path_env: Some("C:\\fixture".into()),
            explicit_sources: vec![ExplicitDiscoverySource::Executable(
                std::path::PathBuf::from("\\\\?\\C:\\fixture\\fixture-agent.exe"),
            )],
            ..WindowsPassiveDiscoveryConfig::default()
        };
        let worker: WindowsPassiveWorkerConfig = (&config).into();
        let payload = serde_json::to_value(&worker).expect("serialize worker config");
        let decoded: WindowsPassiveWorkerConfig =
            serde_json::from_value(payload.clone()).expect("deserialize worker config");
        assert_eq!(decoded.explicit_sources, worker.explicit_sources);
        // The full request round-trips too.
        let request = crate::discovery::ManagedProviderWorkerRequest::new(
            crate::discovery::ManagedProviderWorkerKind::ExplicitSources,
            3000,
            8,
            false,
            payload,
        );
        let serialized = serde_json::to_vec(&request).expect("serialize request");
        let parsed: crate::discovery::ManagedProviderWorkerRequest =
            serde_json::from_slice(&serialized).expect("parse request");
        assert_eq!(parsed.payload, request.payload);
    }

    fn local_transport_fixture_guard() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: std::sync::OnceLock<Mutex<()>> = std::sync::OnceLock::new();
        match LOCK.get_or_init(|| Mutex::new(())).lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn request(access: WorkspaceAccess, cwd: Option<&str>) -> RuntimeRequest {
        request_with_model(access, cwd, "mock-default")
    }

    fn request_with_model(
        access: WorkspaceAccess,
        cwd: Option<&str>,
        model_id: &str,
    ) -> RuntimeRequest {
        RuntimeRequest {
            execution_run_id: "run-1".into(),
            agent_identity_id: "agent-1".into(),
            connector_id: "mock".into(),
            model_id: Some(model_id.into()),
            context_manifest_id: "manifest-1".into(),
            rendered_context: "fixture context".into(),
            canonical_cwd: cwd.map(str::to_owned),
            workspace_access: access,
            timeout_ms: DEFAULT_RUNTIME_TIMEOUT_MS,
            thread_policy: "per-run".into(),
            signed_scope: "local-fixture-scope".into(),
        }
    }

    #[test]
    fn unconfigured_runtime_is_unavailable_and_never_executes() {
        let runtime = UnconfiguredRuntime;
        assert_eq!(runtime.health().status, "unavailable");
        assert!(runtime.list_models().is_empty());
        assert_eq!(
            runtime.execute(&request(WorkspaceAccess::None, None)),
            Err(RuntimeError::NotConfigured)
        );
        assert_eq!(
            runtime.classify_error(&RuntimeError::Authentication),
            RuntimeErrorClass::Authentication
        );
        assert_eq!(
            runtime.classify_error(&RuntimeError::Provider("provider_error".into())),
            RuntimeErrorClass::Provider
        );
    }

    #[test]
    fn codex_app_server_fixture_supports_ndjson_stream_and_safe_provider_failure() {
        let fixture = r#"
{"method":"thread.started"}
{"method":"item/agentMessage/delta","params":{"delta":"hello"}}
{"method":"turn.completed"}
"#;
        let runtime = CodexAppServerRuntime::from_fixture("gpt-test", fixture);
        assert_eq!(runtime.health().status, "available");
        let events = runtime
            .execute(&request_with_model(WorkspaceAccess::None, None, "gpt-test"))
            .unwrap();
        assert_eq!(events[2].payload["delta"], "hello");
        assert_eq!(events.last().unwrap().event_type, "execution.completed");

        let stream = runtime
            .stream_events_with_capacity(
                &request_with_model(WorkspaceAccess::None, None, "gpt-test"),
                1,
            )
            .unwrap();
        let mut event_types = Vec::new();
        while let Some(event) = stream.next().unwrap() {
            event_types.push(event.event_type);
        }
        assert_eq!(
            event_types,
            vec![
                "connector.started",
                "runtime.started",
                "output.delta",
                "execution.completed"
            ]
        );
        assert_eq!(
            runtime
                .cancel(&request_with_model(WorkspaceAccess::None, None, "gpt-test"))
                .unwrap()
                .event_type,
            "execution.cancelled"
        );

        let failed = runtime
            .execute_from_fixture(
                &request_with_model(WorkspaceAccess::None, None, "gpt-test"),
                r#"[{"type":"provider.error","message":"secret must not persist"}]"#,
            )
            .unwrap();
        assert_eq!(failed.last().unwrap().event_type, "execution.failed");
        assert_eq!(
            failed.last().unwrap().payload,
            json!({"reason":"provider_error"})
        );
        assert!(!serde_json::to_string(&failed).unwrap().contains("secret"));
    }

    #[test]
    fn kun_shared_runtime_rejects_missing_terminal_and_write_scope_is_explicit() {
        let runtime = KunSharedRuntime::from_fixture(
            "kun-model",
            r#"[{"kind":"output.delta","delta":"partial"}]"#,
        );
        assert!(matches!(
            runtime.execute(&request_with_model(WorkspaceAccess::None, None, "kun-model")),
            Err(RuntimeError::Protocol(message)) if message.contains("terminal")
        ));
        let runtime = KunSharedRuntime::from_fixture(
            "kun-model",
            r#"[{"kind":"output.delta","delta":"write"},{"kind":"turn.completed"}]"#,
        );
        assert!(runtime
            .execute(&request_with_model(
                WorkspaceAccess::WorkspaceWrite,
                Some("C:\\workspace"),
                "kun-model",
            ))
            .is_ok());
    }

    fn stream_event(event_type: &str) -> RuntimeEvent {
        RuntimeEvent {
            event_id: format!("event-{event_type}"),
            execution_run_id: "run-stream".into(),
            runtime_id: "stream-test".into(),
            thread_id: None,
            turn_id: Some("turn-1".into()),
            sequence: 0,
            event_type: event_type.into(),
            timestamp_ms: 0,
            payload: json!({}),
        }
    }

    #[test]
    fn mock_runtime_is_deterministic_and_streams_terminal_event() {
        let events = MockRuntime::default()
            .execute(&request(WorkspaceAccess::None, None))
            .unwrap();
        assert_eq!(events.first().unwrap().event_type, "runtime.started");
        assert_eq!(events.last().unwrap().event_type, "execution.completed");
        assert_eq!(
            events
                .iter()
                .filter(|event| event.event_type == "output.delta")
                .count(),
            2
        );
    }

    #[test]
    fn mock_runtime_rejects_cwd_when_permission_is_none() {
        assert_eq!(
            MockRuntime::default().execute(&request(WorkspaceAccess::None, Some("C:\\forbidden"))),
            Err(RuntimeError::InvalidWorkspace)
        );
    }

    #[test]
    fn bounded_stream_rejects_over_limit_without_consuming_the_event() {
        assert!(matches!(
            RuntimeEventStream::with_capacity(0),
            Err(RuntimeError::InvalidStreamCapacity)
        ));

        let (stream, producer) = RuntimeEventStream::channel(1).unwrap();
        producer.try_push(stream_event("runtime.started")).unwrap();
        assert_eq!(stream.buffered_len(), 1);
        assert_eq!(
            producer.try_push(stream_event("output.delta")),
            Err(RuntimeError::StreamBufferFull { capacity: 1 })
        );
        assert_eq!(
            stream.try_next().unwrap().unwrap().event_type,
            "runtime.started"
        );
        producer.try_push(stream_event("output.delta")).unwrap();
        assert_eq!(stream.buffered_len(), 1);
    }

    #[test]
    fn bounded_stream_timeout_is_explicit_and_does_not_fabricate_terminal_state() {
        let stream = RuntimeEventStream::with_capacity(1).unwrap();
        assert_eq!(
            stream.next_timeout(Duration::from_millis(1)),
            Err(RuntimeError::Timeout)
        );
        assert_eq!(stream.buffered_len(), 0);
        assert_eq!(
            MockRuntime::default().classify_error(&RuntimeError::Timeout),
            RuntimeErrorClass::Timeout
        );

        let (stream, producer) = RuntimeEventStream::channel(1).unwrap();
        producer.try_push(stream_event("runtime.started")).unwrap();
        assert_eq!(
            stream
                .next_timeout(Duration::from_millis(1))
                .unwrap()
                .unwrap()
                .event_type,
            "runtime.started"
        );
        stream.cancel().unwrap();
    }

    #[test]
    fn bounded_stream_terminal_is_single_and_drains_before_eof() {
        let (stream, producer) = RuntimeEventStream::channel(2).unwrap();
        producer.try_push(stream_event("runtime.started")).unwrap();
        producer
            .try_push(stream_event("execution.completed"))
            .unwrap();
        assert_eq!(
            producer.try_push(stream_event("output.delta")),
            Err(RuntimeError::StreamTerminal)
        );
        assert_eq!(
            stream.next().unwrap().unwrap().event_type,
            "runtime.started"
        );
        assert_eq!(
            stream.next().unwrap().unwrap().event_type,
            "execution.completed"
        );
        assert_eq!(stream.next().unwrap(), None);
        assert_eq!(stream.cancel(), Err(RuntimeError::StreamTerminal));
    }

    #[test]
    fn bounded_stream_cancel_clears_buffer_and_stops_producer() {
        let (stream, producer) = RuntimeEventStream::channel(2).unwrap();
        producer.try_push(stream_event("runtime.started")).unwrap();
        producer.try_push(stream_event("output.delta")).unwrap();
        stream.cancel().unwrap();
        assert_eq!(stream.buffered_len(), 0);
        assert_eq!(stream.try_next(), Err(RuntimeError::Cancelled));
        assert_eq!(
            producer.try_push(stream_event("execution.completed")),
            Err(RuntimeError::Cancelled)
        );
        stream.cancel().unwrap();
    }

    #[test]
    fn bounded_stream_distinguishes_missing_terminal_from_transport_close() {
        let (stream, producer) = RuntimeEventStream::channel(2).unwrap();
        producer.try_push(stream_event("runtime.started")).unwrap();
        assert_eq!(producer.finish(), Err(RuntimeError::StreamTerminalMissing));
        assert_eq!(
            stream.next().unwrap().unwrap().event_type,
            "runtime.started"
        );
        assert_eq!(stream.next(), Err(RuntimeError::StreamTerminalMissing));

        let (transport_stream, transport_producer) = RuntimeEventStream::channel(2).unwrap();
        transport_producer
            .try_push(stream_event("runtime.started"))
            .unwrap();
        transport_producer.close_transport().unwrap();
        assert_eq!(
            transport_stream.next().unwrap().unwrap().event_type,
            "runtime.started"
        );
        assert_eq!(transport_stream.next(), Err(RuntimeError::TransportClosed));
        assert_eq!(
            transport_producer.try_push(stream_event("output.delta")),
            Err(RuntimeError::TransportClosed)
        );
    }

    #[test]
    fn mock_stream_is_deterministic_bounded_and_terminal() {
        let runtime = MockRuntime {
            chunks: vec!["a".into(), "b".into(), "c".into()],
        };
        let stream = runtime
            .stream_events_with_capacity(&request(WorkspaceAccess::None, None), 1)
            .unwrap();
        let mut events = Vec::new();
        while let Some(event) = stream.next().unwrap() {
            assert!(stream.buffered_len() <= stream.capacity());
            events.push(event.event_type);
        }
        assert_eq!(
            events,
            vec![
                "runtime.started",
                "output.delta",
                "output.delta",
                "output.delta",
                "execution.completed"
            ]
        );
    }

    #[test]
    fn fixture_adapters_do_not_claim_live_stream_transport() {
        let request = request(WorkspaceAccess::ReadOnly, None);
        assert!(matches!(
            OpenAiCompatibleRuntime::new("model-a").stream_events(&request),
            Err(RuntimeError::Unsupported)
        ));
        assert!(matches!(
            HttpCustomRuntime::new("model-a").stream_events(&request),
            Err(RuntimeError::Unsupported)
        ));
    }

    #[test]
    fn openai_sse_parser_keeps_split_frames_and_flushes_eof_tail() {
        let mut parser = SseParser::default();
        assert!(parser
            .push("data: {\"choices\":[{\"delta\":{\"content\":\"hel")
            .is_empty());
        let frames = parser.push("lo\"}}]}\n\ndata: [DONE]");
        assert_eq!(
            frames,
            vec![SseFrame {
                data: r#"{"choices":[{"delta":{"content":"hello"}}]}"#.into()
            }]
        );
        assert_eq!(
            parser.finish(),
            vec![SseFrame {
                data: "[DONE]".into()
            }]
        );
    }

    #[test]
    fn bounded_sse_parser_rejects_an_unterminated_oversized_frame() {
        let mut parser = SseParser::default();
        let oversized = "x".repeat(65);
        assert!(matches!(
            parser.push_bounded(&oversized, 64),
            Err(RuntimeError::Protocol(code)) if code == "sse_frame_too_large"
        ));
        assert_eq!(
            parser.push_bounded("data: {}\n\n", 64).unwrap(),
            vec![SseFrame { data: "{}".into() }]
        );
    }

    #[test]
    fn incremental_utf8_sse_preserves_chinese_across_every_multibyte_split() {
        let payload = concat!(
            "data: {\"kind\":\"assistant_text_delta\",\"delta\":\"你好\"}\n\n",
            "data: {\"kind\":\"assistant_text_delta\",\"delta\":\"，世界\"}\n\n"
        )
        .as_bytes()
        .to_vec();
        let mut boundaries = Vec::new();
        for (index, byte) in payload.iter().enumerate() {
            if (*byte & 0b1111_0000) == 0b1110_0000 {
                boundaries.push(index + 1);
                boundaries.push(index + 2);
            }
        }
        boundaries.sort_unstable();
        boundaries.dedup();

        // Both Content-Length and chunked bodies eventually expose arbitrary
        // byte reads to the same decoder. Feed every legal 1/2-byte Chinese
        // split deterministically, including an SSE frame boundary.
        for _body_kind in ["content-length", "chunked"] {
            let mut decoder = IncrementalUtf8Decoder::default();
            let mut parser = SseParser::default();
            let mut start = 0usize;
            let mut frames = Vec::new();
            for end in boundaries
                .iter()
                .copied()
                .chain(std::iter::once(payload.len()))
            {
                if end <= start {
                    continue;
                }
                let text = decoder.push(&payload[start..end]).unwrap();
                frames.extend(
                    parser
                        .push_bounded(&text, MAX_TRANSPORT_BODY_BYTES)
                        .unwrap(),
                );
                start = end;
            }
            decoder.finish().unwrap();
            frames.extend(parser.finish());
            let deltas = frames
                .iter()
                .map(|frame| {
                    serde_json::from_str::<Value>(&frame.data).unwrap()["delta"]
                        .as_str()
                        .unwrap()
                        .to_owned()
                })
                .collect::<Vec<_>>();
            assert_eq!(deltas.concat(), "你好，世界");
        }
    }

    #[test]
    fn external_transport_redaction_covers_json_headers_cookies_and_events() {
        let marker = "fixture-private-runtime-material";
        let input = format!(
            "{{\"runtimeToken\":\"{marker}\",\"apiKey\":\"{marker}\"}}\nAuthorization:Bearer {marker}\nCookie: session={marker}\napi_key={marker}\ntoken={marker}"
        );
        let redacted = redact_external_text(&input);
        assert!(!redacted.contains(marker));
        assert!(redacted.contains("<redacted>"));

        let event = output_delta_event(
            "kun",
            &request_with_model(WorkspaceAccess::ReadOnly, None, "kun-model-a"),
            Some("thread-redaction".into()),
            Some("turn-redaction".into()),
            1,
            &input,
        );
        assert!(!serde_json::to_string(&event)
            .expect("serialize redacted event")
            .contains(marker));
    }

    #[test]
    fn openai_compatible_fixture_stream_emits_deltas_and_rejects_write_scope() {
        let runtime = OpenAiCompatibleRuntime::new("model-a");
        let read_only_request = request(WorkspaceAccess::ReadOnly, None);
        let events = runtime
            .execute_from_sse(
                &read_only_request,
                &[
                    "data: {\"choices\":[{\"delta\":{\"content\":\"A\"}}]}\n\n",
                    "data: {\"output_text\":\"B\"}\n\ndata: [DONE]\n\n",
                ],
            )
            .unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| event.event_type == "output.delta")
                .count(),
            2
        );
        assert_eq!(events.last().unwrap().payload["output"], "AB");
        assert_eq!(
            runtime.execute_from_sse(&request(WorkspaceAccess::WorkspaceWrite, None), &[]),
            Err(RuntimeError::Permission)
        );
    }

    #[test]
    fn openai_compatible_stream_requires_done_terminal_event() {
        let runtime = OpenAiCompatibleRuntime::new("gpt-test");
        let request = RuntimeRequest {
            execution_run_id: "run-openai-truncated".into(),
            agent_identity_id: "agent-1".into(),
            connector_id: "openai-compatible".into(),
            model_id: Some("gpt-test".into()),
            context_manifest_id: "manifest-1".into(),
            rendered_context: "hello".into(),
            canonical_cwd: None,
            workspace_access: WorkspaceAccess::None,
            timeout_ms: DEFAULT_RUNTIME_TIMEOUT_MS,
            thread_policy: "per-run".into(),
            signed_scope: "local-fixture-scope".into(),
        };
        let result = runtime.execute_from_sse(
            &request,
            &[
                r#"data: {"choices":[{"delta":{"content":"partial"}}]}"#,
                "\n\n",
            ],
        );
        assert!(matches!(
            result,
            Err(RuntimeError::Protocol(message)) if message.contains("without [DONE]")
        ));
    }

    #[test]
    fn deferred_runtime_adapters_report_unverified_without_silent_capability_escalation() {
        let request = request(WorkspaceAccess::WorkspaceWrite, Some("C:\\workspace"));
        let kun = ConfiguredAdapter::kun();
        assert_eq!(kun.health().status, "unverified");
        assert!(kun.capabilities().filesystem);
        assert_eq!(kun.cancel(&request), Err(RuntimeError::Unsupported));
        assert_eq!(
            ConfiguredAdapter::http_custom().execute(&request),
            Err(RuntimeError::Permission)
        );
        assert_eq!(
            OpenAiCompatibleRuntime::new("gpt-test").cancel(&request),
            Err(RuntimeError::Unsupported)
        );
        assert_eq!(
            ConfiguredAdapter::acp_mock().discover().version.as_deref(),
            Some("mock-contract-v1")
        );
    }

    #[test]
    fn http_custom_fixture_requires_terminal_and_preserves_permission_boundary() {
        let runtime = HttpCustomRuntime::new("custom-model");
        let events = runtime
            .execute_from_json(
                &request(WorkspaceAccess::ReadOnly, None),
                r#"{"events":[{"type":"output.delta","delta":"A"},{"type":"execution.completed"}]}"#,
            )
            .unwrap();
        assert_eq!(events.last().unwrap().event_type, "execution.completed");
        assert_eq!(events[1].payload["delta"], "A");
        assert!(matches!(
            runtime.execute_from_json(
                &request(WorkspaceAccess::ReadOnly, None),
                r#"{"outputText":"partial"}"#,
            ),
            Err(RuntimeError::Protocol(message)) if message.contains("terminal")
        ));
        assert_eq!(
            runtime.execute_from_json(
                &request(WorkspaceAccess::WorkspaceWrite, None),
                r#"{"completed":true}"#,
            ),
            Err(RuntimeError::Permission)
        );
        assert_eq!(
            runtime.cancel(&request(WorkspaceAccess::ReadOnly, None)),
            Err(RuntimeError::Unsupported)
        );
    }

    #[test]
    fn acp_mock_fixture_maps_stream_and_terminal_contract() {
        let runtime = AcpMockRuntime::new(vec!["hello ".into(), "acp".into()]);
        let events = runtime
            .execute(&request(WorkspaceAccess::None, None))
            .unwrap();
        assert_eq!(events[0].event_type, "runtime.started");
        assert_eq!(events.last().unwrap().event_type, "execution.completed");
        assert_eq!(
            events
                .iter()
                .filter(|event| event.event_type == "output.delta")
                .count(),
            2
        );
        assert!(matches!(
            runtime.execute_from_fixture(
                &request(WorkspaceAccess::None, None),
                r#"[{"kind":"session.started"},{"kind":"output.delta","delta":"partial"}]"#,
            ),
            Err(RuntimeError::Protocol(message)) if message.contains("terminal")
        ));
        assert_eq!(
            runtime
                .cancel(&request(WorkspaceAccess::None, None))
                .unwrap()
                .event_type,
            "execution.cancelled"
        );
    }

    #[derive(Clone)]
    struct RecordingTransport {
        runtime_id: &'static str,
        calls: Arc<Mutex<Vec<&'static str>>>,
        owned: bool,
    }

    impl RecordingTransport {
        fn new(runtime_id: &'static str, owned: bool) -> Self {
            Self {
                runtime_id,
                calls: Arc::new(Mutex::new(Vec::new())),
                owned,
            }
        }

        fn calls(&self) -> Vec<&'static str> {
            match self.calls.lock() {
                Ok(calls) => calls.clone(),
                Err(poisoned) => poisoned.into_inner().clone(),
            }
        }

        fn record(&self, operation: &'static str) {
            match self.calls.lock() {
                Ok(mut calls) => calls.push(operation),
                Err(poisoned) => poisoned.into_inner().push(operation),
            }
        }
    }

    impl ConnectorRuntimeTransport for RecordingTransport {
        fn runtime_id(&self) -> &'static str {
            self.runtime_id
        }

        fn owned(&self) -> bool {
            self.owned
        }

        fn discover(&self) -> Result<RuntimeDiscovery, RuntimeError> {
            self.record("discover");
            Ok(RuntimeDiscovery {
                runtime_id: self.runtime_id.into(),
                version: Some("fixture-transport-v1".into()),
                owned: self.owned,
            })
        }

        fn list_models(&self) -> Result<RuntimeModelCatalog, RuntimeError> {
            self.record("list_models");
            Ok(RuntimeModelCatalog {
                models: vec![format!("{}-model", self.runtime_id)],
                revision: Some("7".into()),
                default_model_id: Some(format!("{}-model", self.runtime_id)),
                model_metadata: vec![RuntimeModelMetadata {
                    model_id: format!("{}-model", self.runtime_id),
                    available: true,
                    enabled: true,
                    status: Some("available".into()),
                    capabilities: RuntimeCapabilities {
                        streaming: true,
                        cancel: true,
                        filesystem: false,
                        shell: false,
                    },
                }],
            })
        }

        fn stream(
            &self,
            request: &RuntimeRequest,
            cancellation: Arc<AtomicBool>,
            producer: &RuntimeEventProducer,
        ) -> Result<(), RuntimeError> {
            self.record("stream");
            producer.push(connector_started_event(
                self.runtime_id,
                request,
                Some("thread-fixture".into()),
                Some("turn-fixture".into()),
            ))?;
            producer.push(runtime_started_event(
                self.runtime_id,
                "recording-transport",
                request,
                Some("thread-fixture".into()),
                Some("turn-fixture".into()),
            ))?;
            for _ in 0..100 {
                if cancellation.load(Ordering::Acquire) {
                    return Err(RuntimeError::Cancelled);
                }
                thread::sleep(Duration::from_millis(2));
            }
            producer.push(terminal_event(
                self.runtime_id,
                "execution.completed",
                request,
                Some("thread-fixture".into()),
                Some("turn-fixture".into()),
                3,
                None,
            ))?;
            Ok(())
        }

        fn cancel(&self, _request: &RuntimeRequest) -> Result<(), RuntimeError> {
            self.record("cancel");
            Ok(())
        }

        fn shutdown_owned(&self) -> Result<(), RuntimeError> {
            self.record("shutdown_owned");
            Ok(())
        }
    }

    #[test]
    fn transport_backed_constructor_is_lazy_and_accepts_profile_connector_ids() {
        let transport = RecordingTransport::new("codex", true);
        let runtime = CodexAppServerRuntime::with_transport(Arc::new(transport.clone()));
        assert!(
            transport.calls().is_empty(),
            "construction must not probe I/O"
        );

        assert_eq!(runtime.list_models(), vec!["codex-model"]);
        assert_eq!(runtime.catalog_revision().as_deref(), Some("7"));
        assert_eq!(transport.calls(), vec!["list_models"]);

        let mut request = request_with_model(WorkspaceAccess::ReadOnly, None, "codex-model");
        request.connector_id = "profile-codex-uuid".into();
        let stream = runtime
            .stream_events(&request)
            .expect("Core-selected adapter must accept a non-literal profile id");
        assert_eq!(
            stream
                .next()
                .expect("fixture event")
                .expect("connector started")
                .event_type,
            "connector.started"
        );
        stream.cancel().expect("cancel bounded fixture stream");
        let calls = transport.calls();
        assert!(calls.contains(&"stream"));
        assert!(calls.contains(&"cancel"));
    }

    #[test]
    fn owned_transport_cancel_and_shutdown_are_bounded_and_local() {
        let transport = RecordingTransport::new("codex", true);
        let runtime = CodexAppServerRuntime::with_transport(Arc::new(transport.clone()));
        let mut request = request_with_model(WorkspaceAccess::ReadOnly, None, "codex-model");
        request.connector_id = "profile-codex-uuid".into();
        let stream = runtime.stream_events_with_capacity(&request, 4).unwrap();
        assert_eq!(
            stream.next().unwrap().unwrap().event_type,
            "connector.started"
        );
        assert_eq!(
            stream.next().unwrap().unwrap().event_type,
            "runtime.started"
        );
        assert_eq!(
            runtime.cancel(&request).unwrap().event_type,
            "execution.cancelled"
        );
        assert_eq!(stream.next(), Err(RuntimeError::Cancelled));
        runtime.shutdown_owned().unwrap();
        let calls = transport.calls();
        assert!(calls.contains(&"stream"));
        assert!(calls.contains(&"cancel"));
        assert!(calls.contains(&"shutdown_owned"));
    }

    #[derive(Clone, Copy)]
    enum KunFixtureMode {
        Healthy,
        RuntimeUnauthorized,
        ThreadRuntimeUnauthorized,
        TurnRuntimeUnauthorized,
        SseRuntimeUnauthorized,
        SseRuntimeForbidden,
        SseProviderUnauthorized,
        InterruptRuntimeUnauthorized,
        InterruptSuccess,
        TurnPostDelayed,
        IdentityMismatch,
        CatalogUnavailable,
        DefaultProviderPriority,
        ProviderUnauthorized,
        NoDeltaCompletion,
        HostileHostname,
    }

    struct LocalKunFixture {
        stop: Arc<AtomicBool>,
        requests: Arc<Mutex<Vec<KunFixtureRequest>>>,
        thread: Option<JoinHandle<()>>,
        token: String,
        install_dir: std::path::PathBuf,
    }

    #[derive(Clone)]
    struct KunFixtureMetadata {
        instance_id: String,
        service_version: String,
        build_id: String,
    }

    #[derive(Clone, Debug)]
    struct KunFixtureRequest {
        path: String,
        authenticated: bool,
        body: Vec<u8>,
    }

    #[derive(Clone)]
    struct KunFixtureContext {
        data_dir: std::path::PathBuf,
        token: String,
        instance_id: String,
        service_version: String,
        build_id: String,
        pid: u64,
        mode: KunFixtureMode,
        requests: Arc<Mutex<Vec<KunFixtureRequest>>>,
    }

    impl LocalKunFixture {
        fn start(data_dir: &std::path::Path, mode: KunFixtureMode) -> Self {
            Self::start_with_metadata(
                data_dir,
                mode,
                KunFixtureMetadata {
                    instance_id: "fixture-instance".into(),
                    service_version: "0.2.34".into(),
                    build_id: "fixture-build".into(),
                },
            )
        }

        fn start_with_metadata(
            data_dir: &std::path::Path,
            mode: KunFixtureMode,
            metadata: KunFixtureMetadata,
        ) -> Self {
            let KunFixtureMetadata {
                instance_id,
                service_version,
                build_id,
            } = metadata;
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let port = listener.local_addr().unwrap().port();
            let token = "fixture-runtime-token".to_owned();
            let pid = std::process::id() as u64;
            let host = if matches!(mode, KunFixtureMode::HostileHostname) {
                "127.example.com"
            } else {
                "127.0.0.1"
            };
            let base_url = format!("http://{host}:{port}");
            let runtime_record = json!({
                "version": 2,
                "instanceId": instance_id.clone(),
                "pid": pid,
                "startedAt": "2026-08-09T00:00:00.000Z",
                "host": host,
                "port": port,
                "baseUrl": base_url,
                "runtimeToken": token,
                "insecure": false,
                "serviceVersion": service_version.clone(),
                "buildId": build_id.clone(),
                "launchMode": "shared",
            });
            std::fs::write(
                data_dir.join("runtime.json"),
                serde_json::to_vec(&runtime_record).unwrap(),
            )
            .unwrap();
            let install_dir = data_dir.with_file_name(format!(
                "{}-install",
                data_dir
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("kun-fixture")
            ));
            let build_metadata = kun_runtime_build_metadata_path(&install_dir);
            std::fs::create_dir_all(
                build_metadata
                    .parent()
                    .expect("official metadata path has a parent"),
            )
            .unwrap();
            std::fs::write(
                build_metadata,
                serde_json::to_vec(&json!({
                    "buildId": build_id.clone(),
                    "serviceVersion": service_version.clone(),
                }))
                .unwrap(),
            )
            .unwrap();
            assert!(
                !data_dir.join("runtime-build.json").exists(),
                "fixture dataDir must contain only the rendezvous record"
            );

            let stop = Arc::new(AtomicBool::new(false));
            let requests = Arc::new(Mutex::new(Vec::new()));
            let thread_stop = Arc::clone(&stop);
            let context = KunFixtureContext {
                data_dir: data_dir.to_path_buf(),
                token: token.clone(),
                instance_id,
                service_version,
                build_id,
                pid,
                mode,
                requests: Arc::clone(&requests),
            };
            let thread = thread::spawn(move || {
                while !thread_stop.load(Ordering::Acquire) {
                    match listener.accept() {
                        Ok((mut stream, _)) => {
                            let request_context = context.clone();
                            let _ = thread::spawn(move || {
                                let _ = handle_kun_fixture_request(&mut stream, &request_context);
                            });
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(5));
                        }
                        Err(_) => return,
                    }
                }
            });
            Self {
                stop,
                requests,
                thread: Some(thread),
                token,
                install_dir,
            }
        }

        fn all_requests_authenticated(&self) -> bool {
            match self.requests.lock() {
                Ok(requests) => requests.iter().all(|request| request.authenticated),
                Err(poisoned) => poisoned
                    .into_inner()
                    .iter()
                    .all(|request| request.authenticated),
            }
        }

        fn saw_path(&self, expected: &str) -> bool {
            match self.requests.lock() {
                Ok(requests) => requests.iter().any(|request| request.path == expected),
                Err(poisoned) => poisoned
                    .into_inner()
                    .iter()
                    .any(|request| request.path == expected),
            }
        }

        fn request_count(&self, expected: &str) -> usize {
            match self.requests.lock() {
                Ok(requests) => requests
                    .iter()
                    .filter(|request| request.path == expected)
                    .count(),
                Err(poisoned) => poisoned
                    .into_inner()
                    .iter()
                    .filter(|request| request.path == expected)
                    .count(),
            }
        }

        fn wait_for_path(&self, expected: &str, timeout: Duration) {
            let deadline = Instant::now() + timeout;
            while !self.saw_path(expected) {
                assert!(
                    Instant::now() < deadline,
                    "fixture did not receive {expected} before timeout"
                );
                thread::sleep(Duration::from_millis(5));
            }
        }

        fn saw_json_body(&self, path: &str, expected: &Value) -> bool {
            let requests = match self.requests.lock() {
                Ok(requests) => requests.clone(),
                Err(poisoned) => poisoned.into_inner().clone(),
            };
            requests.iter().any(|request| {
                request.path == path
                    && serde_json::from_slice::<Value>(&request.body)
                        .ok()
                        .as_ref()
                        .is_some_and(|body| body == expected)
            })
        }
    }

    impl Drop for LocalKunFixture {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Release);
            if let Some(thread) = self.thread.take() {
                let _ = thread.join();
            }
            let _ = std::fs::remove_dir_all(&self.install_dir);
        }
    }

    fn handle_kun_fixture_request(
        stream: &mut std::net::TcpStream,
        context: &KunFixtureContext,
    ) -> std::io::Result<()> {
        stream.set_read_timeout(Some(Duration::from_secs(1)))?;
        let mut request = Vec::new();
        let mut buffer = [0u8; 1024];
        while !request.windows(4).any(|window| window == b"\r\n\r\n") {
            let read = stream.read(&mut buffer)?;
            if read == 0 {
                return Ok(());
            }
            request.extend_from_slice(&buffer[..read]);
            if request.len() > 32 * 1024 {
                return Ok(());
            }
        }
        let header_end = request
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .map(|index| index + 4)
            .unwrap_or(request.len());
        let header = String::from_utf8_lossy(&request[..header_end]);
        let mut lines = header.lines();
        let first = lines.next().unwrap_or_default();
        let mut parts = first.split_whitespace();
        let method = parts.next().unwrap_or_default();
        let path = parts
            .next()
            .unwrap_or_default()
            .split('?')
            .next()
            .unwrap_or_default();
        let content_length = lines
            .clone()
            .find_map(|line| {
                line.strip_prefix("Content-Length:")
                    .or_else(|| line.strip_prefix("content-length:"))
                    .and_then(|value| value.trim().parse::<usize>().ok())
            })
            .unwrap_or(0);
        let authenticated = lines.any(|line| {
            line.eq_ignore_ascii_case(&format!("Authorization: Bearer {}", context.token))
        });
        let mut body = request[header_end..].to_vec();
        while body.len() < content_length {
            let read = stream.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            body.extend_from_slice(&buffer[..read]);
        }
        body.truncate(content_length);
        match context.requests.lock() {
            Ok(mut requests) => requests.push(KunFixtureRequest {
                path: path.into(),
                authenticated,
                body,
            }),
            Err(poisoned) => poisoned.into_inner().push(KunFixtureRequest {
                path: path.into(),
                authenticated,
                body,
            }),
        }
        let runtime_unauthorized = matches!(context.mode, KunFixtureMode::RuntimeUnauthorized)
            && path == "/v1/runtime/info"
            || matches!(context.mode, KunFixtureMode::ThreadRuntimeUnauthorized)
                && method == "POST"
                && path == "/v1/threads"
            || matches!(context.mode, KunFixtureMode::TurnRuntimeUnauthorized)
                && method == "POST"
                && path.ends_with("/turns")
            || matches!(context.mode, KunFixtureMode::SseRuntimeUnauthorized)
                && method == "GET"
                && path.ends_with("/events")
            || matches!(context.mode, KunFixtureMode::SseRuntimeForbidden)
                && method == "GET"
                && path.ends_with("/events")
            || matches!(context.mode, KunFixtureMode::InterruptRuntimeUnauthorized)
                && method == "POST"
                && path.ends_with("/interrupt");
        if !authenticated || runtime_unauthorized {
            let status = if matches!(context.mode, KunFixtureMode::SseRuntimeForbidden) {
                403
            } else {
                401
            };
            return write_fixture_http(stream, status, "application/json", b"{}");
        }
        if matches!(context.mode, KunFixtureMode::CatalogUnavailable)
            && path == "/v1/model-connections"
        {
            return write_fixture_http(stream, 503, "application/json", b"{}");
        }
        if matches!(context.mode, KunFixtureMode::ProviderUnauthorized)
            && method == "POST"
            && (path == "/v1/threads" || path.ends_with("/turns"))
        {
            return write_fixture_http(
                stream,
                400,
                "application/json",
                br#"{"error":{"code":"provider_authentication_failed"}}"#,
            );
        }
        if matches!(context.mode, KunFixtureMode::SseProviderUnauthorized)
            && method == "GET"
            && path.ends_with("/events")
        {
            return write_fixture_http(
                stream,
                400,
                "application/json",
                br#"{"error":{"code":"provider_authentication_failed"}}"#,
            );
        }
        let body = match (method, path) {
            ("GET", "/v1/runtime/info") => serde_json::to_vec(&json!({
                "dataDir": context.data_dir,
                "instanceId": if matches!(context.mode, KunFixtureMode::IdentityMismatch) { "wrong-instance" } else { context.instance_id.as_str() },
                "pid": context.pid,
                "startedAt": "2026-08-09T00:00:00.000Z",
                "serviceVersion": context.service_version,
                "buildId": context.build_id,
                "launchMode": "shared",
            }))
            .unwrap(),
            ("GET", "/v1/model-connections") => {
                let catalog = if matches!(context.mode, KunFixtureMode::DefaultProviderPriority) {
                    json!({
                        "revision": 10,
                        "defaultProviderId": "provider-b",
                        "providers": [
                            {"id": "provider-a", "configured": true, "selectedModel": "kun-model-a", "models": [{"id": "kun-model-a", "available": true, "enabled": true}]},
                            {"id": "provider-b", "configured": true, "selectedModel": "kun-model-b", "modelCapabilities": {"kun-model-b": {"streaming": true, "cancel": true, "filesystem": false, "shell": false}}, "models": [{"id": "kun-model-b", "available": true, "enabled": true}]},
                        ],
                    })
                } else {
                    json!({
                        "revision": 9,
                        "defaultModelId": "kun-model-b",
                        "defaultProviderId": "provider-b",
                        "providers": [
                            {"id": "provider-a", "configured": true, "models": [{"id": "kun-model-a", "available": true, "enabled": true, "capabilities": {"streaming": true, "cancel": true, "filesystem": false, "shell": false}}]},
                            {"id": "provider-b", "configured": true, "selectedModel": "kun-model-b", "models": [{"id": "kun-model-b", "status": "available", "enabled": true}]},
                        ],
                    })
                };
                serde_json::to_vec(&catalog).unwrap()
            }
            ("POST", "/v1/threads") => {
                serde_json::to_vec(&json!({"id": "thread-fixture"})).unwrap()
            }
            ("POST", path) if path.ends_with("/turns") => {
                if matches!(context.mode, KunFixtureMode::TurnPostDelayed) {
                    thread::sleep(Duration::from_millis(400));
                }
                serde_json::to_vec(&json!({"turnId": "turn-fixture"})).unwrap()
            }
            ("POST", path) if path.ends_with("/interrupt") => {
                serde_json::to_vec(&json!({"ok": true})).unwrap()
            }
            ("GET", path) if path.ends_with("/turns/turn-fixture") => {
                serde_json::to_vec(&json!({
                    "items": [{"role": "assistant", "kind": "assistant_text", "text": "recovered assistant fixture"}]
                }))
                .unwrap()
            }
            ("GET", path) if path.ends_with("/events") => {
                if matches!(
                    context.mode,
                    KunFixtureMode::InterruptRuntimeUnauthorized | KunFixtureMode::InterruptSuccess
                ) {
                    stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n")?;
                    stream.flush()?;
                    thread::sleep(Duration::from_millis(500));
                    return Ok(());
                }
                let body = concat!(
                    "data: {\"kind\":\"assistant_text_delta\",\"turnId\":\"turn-fixture\",\"item\":{\"kind\":\"assistant_text\",\"text\":\"hello from fixture\"}}\n\n",
                    "data: {\"kind\":\"turn_completed\",\"turnId\":\"turn-fixture\"}\n\n"
                );
                if matches!(context.mode, KunFixtureMode::NoDeltaCompletion) {
                    return write_fixture_http(
                        stream,
                        200,
                        "text/event-stream",
                        b"data: {\"kind\":\"turn_completed\",\"turnId\":\"turn-fixture\"}\n\n",
                    );
                }
                return write_fixture_http(stream, 200, "text/event-stream", body.as_bytes());
            }
            _ => b"{}".to_vec(),
        };
        write_fixture_http(stream, 200, "application/json", &body)
    }

    fn write_fixture_http(
        stream: &mut std::net::TcpStream,
        status: u16,
        content_type: &str,
        body: &[u8],
    ) -> std::io::Result<()> {
        let reason = if status == 200 {
            "OK"
        } else if status == 401 {
            "Unauthorized"
        } else {
            "Unavailable"
        };
        let header = format!(
            "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        stream.write_all(header.as_bytes())?;
        stream.write_all(body)?;
        stream.flush()
    }

    fn temporary_runtime_dir(label: &str) -> std::path::PathBuf {
        let suffix = format!(
            "{}-{}-{}",
            std::process::id(),
            label,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let path = std::env::temp_dir().join(format!("agenttalk-runtime-host-{suffix}"));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn production_worker_identity_requires_canonical_sibling_regular_file() {
        let root = temporary_runtime_dir("worker-identity");
        let core_dir = root.join("core");
        let other_dir = root.join("other");
        std::fs::create_dir_all(&core_dir).unwrap();
        std::fs::create_dir_all(&other_dir).unwrap();
        let core_exe = core_dir.join("agenttalk-core.exe");
        let worker = core_dir.join(local_discovery_worker_file_name());
        let other_worker = other_dir.join(local_discovery_worker_file_name());
        std::fs::write(&core_exe, b"core").unwrap();
        std::fs::write(&worker, b"worker").unwrap();
        std::fs::write(&other_worker, b"other-worker").unwrap();

        assert_eq!(
            resolve_production_worker_for_current_exe(&core_exe),
            Some(worker.canonicalize().unwrap())
        );
        assert!(validate_production_worker_candidate(
            &core_dir,
            Path::new("agenttalk-local-discovery-worker.exe")
        )
        .is_none());
        assert!(validate_production_worker_candidate(&core_dir, &other_worker).is_none());
        assert!(
            validate_production_worker_candidate(&core_dir, &core_dir.join("missing.exe"))
                .is_none()
        );
        std::fs::remove_file(&worker).unwrap();
        std::fs::create_dir(&worker).unwrap();
        assert!(validate_production_worker_candidate(&core_dir, &worker).is_none());

        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(windows)]
    #[test]
    fn production_worker_identity_rejects_symlink_escape() {
        let root = temporary_runtime_dir("worker-symlink-escape");
        let core_dir = root.join("core");
        let other_dir = root.join("other");
        std::fs::create_dir_all(&core_dir).unwrap();
        std::fs::create_dir_all(&other_dir).unwrap();
        let core_exe = core_dir.join("agenttalk-core.exe");
        let real_worker = other_dir.join(local_discovery_worker_file_name());
        let link_worker = core_dir.join(local_discovery_worker_file_name());
        std::fs::write(&core_exe, b"core").unwrap();
        std::fs::write(&real_worker, b"worker").unwrap();
        std::os::windows::fs::symlink_file(&real_worker, &link_worker)
            .expect("create worker symlink fixture");

        assert!(resolve_production_worker_for_current_exe(&core_exe).is_none());

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn kun_shared_runtime_uses_dynamic_authenticated_fixture_http_without_ownership() {
        let _fixture_guard = local_transport_fixture_guard();
        let data_dir = temporary_runtime_dir("kun-healthy");
        let fixture = LocalKunFixture::start(&data_dir, KunFixtureMode::Healthy);
        let runtime = KunSharedRuntime::with_config(KunSharedRuntimeConfig {
            data_dir: Some(data_dir.clone()),
            install_dir: Some(fixture.install_dir.clone()),
            default_model: None,
            expected_service_version: "0.2.34".into(),
            expected_build_id: None,
            request_timeout: Duration::from_secs(2),
        });
        let models = runtime.list_models();
        let recorded_requests = fixture
            .requests
            .lock()
            .expect("fixture request log should unlock")
            .clone();
        assert_eq!(
            models,
            vec!["kun-model-a", "kun-model-b"],
            "safe Kun health after catalog failure: {:?}; requests: {:?}",
            runtime.health().detail,
            recorded_requests
        );
        assert_eq!(runtime.catalog_revision().as_deref(), Some("9"));
        assert_eq!(
            runtime.inner.catalog_default_model_id().as_deref(),
            Some("kun-model-b")
        );
        assert_eq!(
            runtime
                .inner
                .catalog_model_metadata("kun-model-a")
                .map(|metadata| metadata.capabilities.filesystem),
            Some(false)
        );
        assert!(!runtime.discover().owned);

        let mut request = request_with_model(
            WorkspaceAccess::ReadOnly,
            Some(data_dir.to_string_lossy().as_ref()),
            "kun-model-a",
        );
        request.connector_id = "desktop-kun-profile".into();
        let mut missing_frozen_model = request.clone();
        missing_frozen_model.model_id = None;
        assert_eq!(
            runtime.execute(&missing_frozen_model),
            Err(RuntimeError::Protocol(KUN_MODEL_UNAVAILABLE.into()))
        );
        let events = runtime.execute(&request).unwrap();
        assert_eq!(events.last().unwrap().event_type, "execution.completed");
        assert!(events
            .iter()
            .any(|event| event.payload["delta"] == "hello from fixture"));
        let serialized = serde_json::to_string(&events).unwrap();
        assert!(!serialized.contains(&fixture.token));
        assert!(fixture.saw_path("/v1/model-connections"));
        assert!(fixture.saw_path("/v1/runtime/info"));
        assert!(fixture.all_requests_authenticated());
        assert!(fixture.saw_json_body(
            "/v1/threads",
            &json!({
                "title": "AgentTalk",
                "titleAuto": true,
                "workspace": data_dir.to_string_lossy(),
                "model": "kun-model-a",
                "mode": "agent",
                "approvalPolicy": "auto",
                "sandboxMode": "read-only",
            })
        ));
        assert!(fixture.saw_json_body(
            "/v1/threads/thread-fixture/turns",
            &json!({
                "prompt": "fixture context",
                "model": "kun-model-a",
                "mode": "agent",
                "disableUserInput": true,
            })
        ));

        runtime.shutdown_owned().unwrap();
        // shutdown_owned only drops local client state. The independently
        // owned fixture is still reachable after it returns.
        assert_eq!(runtime.health().status, "available");
        drop(fixture);
        let _ = std::fs::remove_dir_all(data_dir);
    }

    #[test]
    fn kun_default_provider_beats_first_provider_and_uses_provider_model_capabilities() {
        let _fixture_guard = local_transport_fixture_guard();
        let data_dir = temporary_runtime_dir("default-provider-priority");
        let fixture = LocalKunFixture::start(&data_dir, KunFixtureMode::DefaultProviderPriority);
        let runtime = KunSharedRuntime::with_config(KunSharedRuntimeConfig {
            data_dir: Some(data_dir.clone()),
            install_dir: Some(fixture.install_dir.clone()),
            request_timeout: Duration::from_secs(2),
            ..KunSharedRuntimeConfig::default()
        });

        assert_eq!(
            runtime.list_models_checked().unwrap(),
            vec!["kun-model-a", "kun-model-b"]
        );
        assert_eq!(
            runtime.catalog_default_model_id().as_deref(),
            Some("kun-model-b")
        );
        let metadata = runtime
            .catalog_model_metadata("kun-model-b")
            .expect("provider modelCapabilities must be retained internally");
        assert!(!metadata.capabilities.filesystem);
        assert!(!metadata.capabilities.shell);
        drop(fixture);
        let _ = std::fs::remove_dir_all(data_dir);
    }

    #[test]
    fn kun_transport_classifies_runtime_catalog_and_provider_failures_without_body_leaks() {
        let _fixture_guard = local_transport_fixture_guard();
        let data_dir = temporary_runtime_dir("runtime-auth");
        let fixture = LocalKunFixture::start(&data_dir, KunFixtureMode::RuntimeUnauthorized);
        let runtime = KunSharedRuntime::with_config(KunSharedRuntimeConfig {
            data_dir: Some(data_dir.clone()),
            install_dir: Some(fixture.install_dir.clone()),
            request_timeout: Duration::from_secs(2),
            ..KunSharedRuntimeConfig::default()
        });
        assert_eq!(
            runtime.list_models_checked(),
            Err(RuntimeError::Authentication)
        );
        assert!(runtime.list_models().is_empty());
        assert_eq!(
            runtime.health().detail.as_deref(),
            Some("runtime_authentication_failed")
        );
        drop(fixture);
        let _ = std::fs::remove_dir_all(data_dir);

        let data_dir = temporary_runtime_dir("identity-mismatch");
        let fixture = LocalKunFixture::start(&data_dir, KunFixtureMode::IdentityMismatch);
        let runtime = KunSharedRuntime::with_config(KunSharedRuntimeConfig {
            data_dir: Some(data_dir.clone()),
            install_dir: Some(fixture.install_dir.clone()),
            request_timeout: Duration::from_secs(2),
            ..KunSharedRuntimeConfig::default()
        });
        assert_eq!(
            runtime.list_models_checked(),
            Err(RuntimeError::Protocol(KUN_RUNTIME_IDENTITY_MISMATCH.into()))
        );
        assert!(runtime.list_models().is_empty());
        assert_eq!(
            runtime.health().detail.as_deref(),
            Some(KUN_RUNTIME_IDENTITY_MISMATCH)
        );
        drop(fixture);
        let _ = std::fs::remove_dir_all(data_dir);

        let data_dir = temporary_runtime_dir("catalog");
        let fixture = LocalKunFixture::start(&data_dir, KunFixtureMode::CatalogUnavailable);
        let runtime = KunSharedRuntime::with_config(KunSharedRuntimeConfig {
            data_dir: Some(data_dir.clone()),
            install_dir: Some(fixture.install_dir.clone()),
            request_timeout: Duration::from_secs(2),
            ..KunSharedRuntimeConfig::default()
        });
        assert_eq!(
            runtime.list_models_checked(),
            Err(RuntimeError::Transport(KUN_CATALOG_UNAVAILABLE.into()))
        );
        drop(fixture);
        let _ = std::fs::remove_dir_all(data_dir);

        let data_dir = temporary_runtime_dir("provider-auth");
        let fixture = LocalKunFixture::start(&data_dir, KunFixtureMode::ProviderUnauthorized);
        let runtime = KunSharedRuntime::with_config(KunSharedRuntimeConfig {
            data_dir: Some(data_dir.clone()),
            install_dir: Some(fixture.install_dir.clone()),
            request_timeout: Duration::from_secs(2),
            ..KunSharedRuntimeConfig::default()
        });
        let mut request = request_with_model(
            WorkspaceAccess::ReadOnly,
            Some(data_dir.to_string_lossy().as_ref()),
            "kun-model-a",
        );
        request.connector_id = "desktop-kun-profile".into();
        let error = runtime
            .execute(&request)
            .expect_err("fixture Provider must reject turn");
        assert_eq!(
            error,
            RuntimeError::Provider(KUN_PROVIDER_AUTHENTICATION_FAILED.into())
        );
        assert_eq!(
            connector_runtime_failure(&error),
            Some(ConnectorRuntimeFailure::ProviderAuthenticationFailed)
        );
        drop(fixture);
        let _ = std::fs::remove_dir_all(data_dir);
    }

    #[test]
    fn kun_runtime_endpoint_401s_are_all_runtime_authentication_failures() {
        let _fixture_guard = local_transport_fixture_guard();
        for (label, mode) in [
            ("thread", KunFixtureMode::ThreadRuntimeUnauthorized),
            ("turn", KunFixtureMode::TurnRuntimeUnauthorized),
            ("sse", KunFixtureMode::SseRuntimeUnauthorized),
            ("sse-forbidden", KunFixtureMode::SseRuntimeForbidden),
        ] {
            let data_dir = temporary_runtime_dir(label);
            let fixture = LocalKunFixture::start(&data_dir, mode);
            let runtime = KunSharedRuntime::with_config(KunSharedRuntimeConfig {
                data_dir: Some(data_dir.clone()),
                install_dir: Some(fixture.install_dir.clone()),
                request_timeout: Duration::from_secs(2),
                ..KunSharedRuntimeConfig::default()
            });
            let mut request = request_with_model(
                WorkspaceAccess::ReadOnly,
                Some(data_dir.to_string_lossy().as_ref()),
                "kun-model-a",
            );
            request.connector_id = format!("fixture-{label}");
            assert_eq!(runtime.execute(&request), Err(RuntimeError::Authentication));
            assert!(fixture.all_requests_authenticated());
            drop(fixture);
            let _ = std::fs::remove_dir_all(data_dir);
        }
    }

    #[test]
    fn kun_sse_provider_authentication_is_allowlisted_without_body_leakage() {
        let _fixture_guard = local_transport_fixture_guard();
        let data_dir = temporary_runtime_dir("sse-provider-auth");
        let fixture = LocalKunFixture::start(&data_dir, KunFixtureMode::SseProviderUnauthorized);
        let runtime = KunSharedRuntime::with_config(KunSharedRuntimeConfig {
            data_dir: Some(data_dir.clone()),
            install_dir: Some(fixture.install_dir.clone()),
            request_timeout: Duration::from_secs(2),
            ..KunSharedRuntimeConfig::default()
        });
        let request = request_with_model(
            WorkspaceAccess::ReadOnly,
            Some(data_dir.to_string_lossy().as_ref()),
            "kun-model-a",
        );
        let error = runtime
            .execute(&request)
            .expect_err("SSE provider auth must not be reported as a completed turn");
        assert_eq!(
            error,
            RuntimeError::Provider(KUN_PROVIDER_AUTHENTICATION_FAILED.into())
        );
        assert_eq!(
            connector_runtime_failure(&error),
            Some(ConnectorRuntimeFailure::ProviderAuthenticationFailed)
        );
        assert!(!format!("{error:?}").contains(&fixture.token));
        drop(fixture);
        let _ = std::fs::remove_dir_all(data_dir);
    }

    #[test]
    fn kun_interrupt_401_fails_closed_instead_of_claiming_remote_cancel_success() {
        let _fixture_guard = local_transport_fixture_guard();
        let data_dir = temporary_runtime_dir("interrupt-runtime-auth");
        let fixture =
            LocalKunFixture::start(&data_dir, KunFixtureMode::InterruptRuntimeUnauthorized);
        let runtime = KunSharedRuntime::with_config(KunSharedRuntimeConfig {
            data_dir: Some(data_dir.clone()),
            install_dir: Some(fixture.install_dir.clone()),
            request_timeout: Duration::from_secs(2),
            ..KunSharedRuntimeConfig::default()
        });
        let mut request = request_with_model(
            WorkspaceAccess::ReadOnly,
            Some(data_dir.to_string_lossy().as_ref()),
            "kun-model-a",
        );
        request.execution_run_id = "kun-interrupt-runtime-auth".into();
        request.connector_id = "fixture-interrupt-auth".into();
        let stream = runtime.stream_events_with_capacity(&request, 4).unwrap();
        assert_eq!(
            stream
                .next_timeout(Duration::from_secs(2))
                .unwrap()
                .unwrap()
                .event_type,
            "connector.started"
        );
        assert_eq!(
            stream
                .next_timeout(Duration::from_secs(2))
                .unwrap()
                .unwrap()
                .event_type,
            "runtime.started"
        );
        assert_eq!(runtime.cancel(&request), Err(RuntimeError::Authentication));
        assert_eq!(
            stream.next_timeout(Duration::from_secs(2)),
            Err(RuntimeError::Authentication),
            "the stream must retain the shared interrupt failure instead of manufacturing cancelled"
        );
        assert_eq!(
            runtime.cancel(&request),
            Err(RuntimeError::Authentication),
            "a repeat cancel must reuse the failed remote interrupt outcome"
        );
        assert!(fixture.saw_path("/v1/threads/thread-fixture/turns/turn-fixture/interrupt"));
        drop(stream);
        drop(fixture);
        let _ = std::fs::remove_dir_all(data_dir);
    }

    #[test]
    fn kun_cancel_between_pre_turn_check_and_registration_never_posts_a_turn() {
        let _fixture_guard = local_transport_fixture_guard();
        let data_dir = temporary_runtime_dir("pre-turn-registration-cancel");
        let fixture = LocalKunFixture::start(&data_dir, KunFixtureMode::Healthy);
        let entered = Arc::new(std::sync::Barrier::new(2));
        let release = Arc::new(std::sync::Barrier::new(2));
        let hook_entered = Arc::clone(&entered);
        let hook_release = Arc::clone(&release);
        let runtime = KunSharedRuntime::with_config_and_before_turn_start_hook(
            KunSharedRuntimeConfig {
                data_dir: Some(data_dir.clone()),
                install_dir: Some(fixture.install_dir.clone()),
                request_timeout: Duration::from_secs(2),
                ..KunSharedRuntimeConfig::default()
            },
            Arc::new(move || {
                hook_entered.wait();
                hook_release.wait();
            }),
        );
        let mut request = request_with_model(
            WorkspaceAccess::ReadOnly,
            Some(data_dir.to_string_lossy().as_ref()),
            "kun-model-a",
        );
        request.execution_run_id = "kun-pre-turn-registration-cancel".into();
        request.connector_id = "fixture-pre-turn-registration-cancel".into();
        let stream = runtime.stream_events_with_capacity(&request, 8).unwrap();
        entered.wait();
        assert_eq!(
            runtime.cancel(&request).unwrap().event_type,
            "execution.cancelled"
        );
        release.wait();
        assert_eq!(
            stream.next_timeout(Duration::from_secs(2)),
            Err(RuntimeError::Cancelled)
        );
        assert_eq!(
            fixture.request_count("/v1/threads/thread-fixture/turns"),
            0,
            "a cancellation recorded before turn_starting must block the remote turn POST"
        );
        assert_eq!(
            fixture.request_count("/v1/threads/thread-fixture/turns/turn-fixture/interrupt"),
            0,
            "no remote turn means no remote interrupt may be claimed"
        );
        drop(stream);
        drop(fixture);
        let _ = std::fs::remove_dir_all(data_dir);
    }

    #[test]
    fn kun_successful_interrupt_is_shared_by_repeat_cancel_and_streams_one_cancelled_outcome() {
        let _fixture_guard = local_transport_fixture_guard();
        let data_dir = temporary_runtime_dir("interrupt-success");
        let fixture = LocalKunFixture::start(&data_dir, KunFixtureMode::InterruptSuccess);
        let runtime = KunSharedRuntime::with_config(KunSharedRuntimeConfig {
            data_dir: Some(data_dir.clone()),
            install_dir: Some(fixture.install_dir.clone()),
            request_timeout: Duration::from_secs(2),
            ..KunSharedRuntimeConfig::default()
        });
        let mut request = request_with_model(
            WorkspaceAccess::ReadOnly,
            Some(data_dir.to_string_lossy().as_ref()),
            "kun-model-a",
        );
        request.execution_run_id = "kun-interrupt-success".into();
        request.connector_id = "fixture-interrupt-success".into();
        let stream = runtime.stream_events_with_capacity(&request, 8).unwrap();
        assert_eq!(
            stream
                .next_timeout(Duration::from_secs(2))
                .unwrap()
                .unwrap()
                .event_type,
            "connector.started"
        );
        assert_eq!(
            stream
                .next_timeout(Duration::from_secs(2))
                .unwrap()
                .unwrap()
                .event_type,
            "runtime.started"
        );
        assert_eq!(
            runtime.cancel(&request).unwrap().event_type,
            "execution.cancelled"
        );
        assert_eq!(
            runtime.cancel(&request).unwrap().event_type,
            "execution.cancelled"
        );
        assert_eq!(
            stream.next_timeout(Duration::from_secs(2)),
            Err(RuntimeError::Cancelled)
        );
        let interrupt_path = "/v1/threads/thread-fixture/turns/turn-fixture/interrupt";
        assert_eq!(fixture.request_count(interrupt_path), 1);
        runtime.shutdown_owned().unwrap();
        assert_eq!(fixture.request_count(interrupt_path), 1);
        drop(fixture);
        let _ = std::fs::remove_dir_all(data_dir);
    }

    #[test]
    fn kun_cancel_during_turn_post_interrupts_once_after_turn_exists() {
        let _fixture_guard = local_transport_fixture_guard();
        let data_dir = temporary_runtime_dir("turn-post-cancel");
        let fixture = LocalKunFixture::start(&data_dir, KunFixtureMode::TurnPostDelayed);
        let runtime = KunSharedRuntime::with_config(KunSharedRuntimeConfig {
            data_dir: Some(data_dir.clone()),
            install_dir: Some(fixture.install_dir.clone()),
            request_timeout: Duration::from_secs(2),
            ..KunSharedRuntimeConfig::default()
        });
        let mut request = request_with_model(
            WorkspaceAccess::ReadOnly,
            Some(data_dir.to_string_lossy().as_ref()),
            "kun-model-a",
        );
        request.execution_run_id = "kun-turn-post-cancel".into();
        request.connector_id = "fixture-turn-post-cancel".into();
        let stream = runtime.stream_events_with_capacity(&request, 8).unwrap();
        fixture.wait_for_path("/v1/threads/thread-fixture/turns", Duration::from_secs(2));
        assert_eq!(
            runtime.cancel(&request).unwrap().event_type,
            "execution.cancelled"
        );
        assert_eq!(
            stream.next_timeout(Duration::from_secs(2)),
            Err(RuntimeError::Cancelled)
        );
        let interrupt_path = "/v1/threads/thread-fixture/turns/turn-fixture/interrupt";
        assert_eq!(fixture.request_count(interrupt_path), 1);
        runtime.shutdown_owned().unwrap();
        assert_eq!(fixture.request_count(interrupt_path), 1);
        drop(fixture);
        let _ = std::fs::remove_dir_all(data_dir);
    }

    #[test]
    fn kun_completion_without_delta_recovers_assistant_turn_text() {
        let _fixture_guard = local_transport_fixture_guard();
        let data_dir = temporary_runtime_dir("no-delta");
        let fixture = LocalKunFixture::start(&data_dir, KunFixtureMode::NoDeltaCompletion);
        let runtime = KunSharedRuntime::with_config(KunSharedRuntimeConfig {
            data_dir: Some(data_dir.clone()),
            install_dir: Some(fixture.install_dir.clone()),
            request_timeout: Duration::from_secs(2),
            ..KunSharedRuntimeConfig::default()
        });
        let mut request = request_with_model(
            WorkspaceAccess::ReadOnly,
            Some(data_dir.to_string_lossy().as_ref()),
            "kun-model-a",
        );
        request.connector_id = "fixture-no-delta".into();
        let events = runtime.execute(&request).unwrap();
        assert!(events
            .iter()
            .any(|event| event.payload["delta"] == "recovered assistant fixture"));
        assert_eq!(
            events.last().map(|event| event.event_type.as_str()),
            Some("execution.completed")
        );
        assert!(fixture.saw_path("/v1/threads/thread-fixture/turns/turn-fixture"));
        drop(fixture);
        let _ = std::fs::remove_dir_all(data_dir);
    }

    #[test]
    fn kun_rejects_hostile_hostname_before_any_runtime_token_is_sent() {
        let _fixture_guard = local_transport_fixture_guard();
        let data_dir = temporary_runtime_dir("hostile-host");
        let fixture = LocalKunFixture::start(&data_dir, KunFixtureMode::HostileHostname);
        let runtime = KunSharedRuntime::with_config(KunSharedRuntimeConfig {
            data_dir: Some(data_dir.clone()),
            install_dir: Some(fixture.install_dir.clone()),
            request_timeout: Duration::from_secs(1),
            ..KunSharedRuntimeConfig::default()
        });
        assert_eq!(
            runtime.list_models_checked(),
            Err(RuntimeError::Protocol(KUN_RUNTIME_IDENTITY_MISMATCH.into()))
        );
        assert!(
            fixture.requests.lock().expect("fixture log should unlock").is_empty(),
            "host validation must fail before an HTTP request can carry token, workspace, or context"
        );
        drop(fixture);
        let _ = std::fs::remove_dir_all(data_dir);
    }

    #[test]
    fn kun_runtime_build_metadata_must_match_live_rendezvous_identity() {
        let _fixture_guard = local_transport_fixture_guard();
        let data_dir = temporary_runtime_dir("build-mismatch");
        let fixture = LocalKunFixture::start(&data_dir, KunFixtureMode::Healthy);
        assert!(
            !data_dir.join("runtime-build.json").exists(),
            "mutable dataDir must not carry build metadata"
        );
        let metadata_path = kun_runtime_build_metadata_path(&fixture.install_dir);
        std::fs::write(
            &metadata_path,
            serde_json::to_vec(&json!({"buildId": "different-build", "serviceVersion": "0.2.34"}))
                .unwrap(),
        )
        .unwrap();
        let runtime = KunSharedRuntime::with_config(KunSharedRuntimeConfig {
            data_dir: Some(data_dir.clone()),
            install_dir: Some(fixture.install_dir.clone()),
            request_timeout: Duration::from_secs(1),
            ..KunSharedRuntimeConfig::default()
        });
        assert_eq!(
            runtime.list_models_checked(),
            Err(RuntimeError::Protocol(KUN_RUNTIME_IDENTITY_MISMATCH.into()))
        );
        std::fs::write(
            &metadata_path,
            serde_json::to_vec(&json!({"buildId": "fixture-build", "serviceVersion": "0.2.33"}))
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            runtime.list_models_checked(),
            Err(RuntimeError::Protocol(KUN_RUNTIME_IDENTITY_MISMATCH.into()))
        );
        drop(fixture);
        let _ = std::fs::remove_dir_all(data_dir);
    }

    #[cfg(windows)]
    #[test]
    fn codex_windows_binary_discovery_is_ordered_real_file_only_and_version_deterministic() {
        let _fixture_guard = local_transport_fixture_guard();
        let root = temporary_runtime_dir("codex-discovery");
        let local_app_data = root.join("local-app-data");
        let desktop_bin = local_app_data.join("OpenAI").join("Codex").join("bin");
        let old_version = desktop_bin.join("0.9.0").join("codex.exe");
        let new_version = desktop_bin.join("0.10.0").join("codex.exe");
        std::fs::create_dir_all(old_version.parent().unwrap()).unwrap();
        std::fs::create_dir_all(new_version.parent().unwrap()).unwrap();
        std::fs::write(&old_version, b"fixture-old").unwrap();
        std::fs::write(&new_version, b"fixture-new").unwrap();
        let path_dir = root.join("fixture-path");
        std::fs::create_dir_all(&path_dir).unwrap();

        let keys = [
            "AGENTTALK_CODEX_BINARY",
            "CODEX_BINARY_PATH",
            "CODEX_BINARY",
            "PATH",
            "LOCALAPPDATA",
        ];
        let saved = keys
            .iter()
            .map(|key| (*key, std::env::var_os(key)))
            .collect::<Vec<_>>();
        for key in [
            "AGENTTALK_CODEX_BINARY",
            "CODEX_BINARY_PATH",
            "CODEX_BINARY",
        ] {
            std::env::remove_var(key);
        }
        std::env::set_var("LOCALAPPDATA", &local_app_data);
        std::env::set_var("PATH", std::env::join_paths([&path_dir]).unwrap());

        let transport = CodexAppServerTransport::new(CodexAppServerConfig::default());
        assert_eq!(transport.binary_path().unwrap(), new_version);

        let path_binary = path_dir.join("codex.exe");
        std::fs::write(&path_binary, b"fixture-path").unwrap();
        assert_eq!(transport.binary_path().unwrap(), path_binary);

        std::env::set_var("AGENTTALK_CODEX_BINARY", root.join("not-a-file"));
        assert_eq!(transport.binary_path().unwrap(), path_dir.join("codex.exe"));
        let explicit = root.join("explicit-codex.exe");
        std::fs::write(&explicit, b"fixture-explicit").unwrap();
        std::env::set_var("AGENTTALK_CODEX_BINARY", &explicit);
        assert_eq!(transport.binary_path().unwrap(), explicit);

        for (key, value) in saved {
            if let Some(value) = value {
                std::env::set_var(key, value);
            } else {
                std::env::remove_var(key);
            }
        }
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn codex_child_environment_whitelist_keeps_required_values_and_excludes_unrelated_credentials()
    {
        let values = HashMap::from([
            ("CODEX_HOME", OsString::from("fixture-codex-home")),
            ("CODEX_ACCESS_TOKEN", OsString::from("fixture-codex-access")),
            ("CODEX_API_KEY", OsString::from("fixture-codex-api-key")),
            ("OPENAI_API_KEY", OsString::from("fixture-openai-key")),
            (
                "OPENAI_BASE_URL",
                OsString::from("https://fixture.invalid/v1"),
            ),
            ("OPENAI_ORG_ID", OsString::from("fixture-org")),
            ("OPENAI_PROJECT_ID", OsString::from("fixture-project")),
            ("HTTP_PROXY", OsString::from("http://fixture-proxy")),
            ("HTTPS_PROXY", OsString::from("fixture-proxy")),
            ("ALL_PROXY", OsString::from("socks5://fixture-proxy")),
            ("NO_PROXY", OsString::from("localhost,127.0.0.1")),
            ("SSL_CERT_FILE", OsString::from("fixture-ca")),
            ("CODEX_CA_CERTIFICATE", OsString::from("fixture-codex-ca")),
            ("NODE_EXTRA_CA_CERTS", OsString::from("fixture-node-ca")),
            ("SystemRoot", OsString::from("C:\\Windows")),
            ("PATH", OsString::from("C:\\fixture-bin")),
            ("DATABASE_URL", OsString::from("fixture-database-secret")),
        ]);
        let child = codex_child_environment_values(|key| values.get(key).cloned());
        let child = child.into_iter().collect::<HashMap<_, _>>();
        for key in [
            "CODEX_HOME",
            "CODEX_ACCESS_TOKEN",
            "CODEX_API_KEY",
            "OPENAI_API_KEY",
            "OPENAI_BASE_URL",
            "OPENAI_ORG_ID",
            "OPENAI_PROJECT_ID",
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "ALL_PROXY",
            "NO_PROXY",
            "SSL_CERT_FILE",
            "CODEX_CA_CERTIFICATE",
            "NODE_EXTRA_CA_CERTS",
            "SystemRoot",
            "PATH",
        ] {
            assert!(child.contains_key(key), "required key {key} is missing");
        }
        assert!(!child.contains_key("DATABASE_URL"));
        assert!(!child
            .values()
            .any(|value| value == &OsString::from("fixture-database-secret")));
    }

    #[test]
    fn codex_reverse_rpc_responses_are_type_specific_and_unknown_methods_are_not_successes() {
        for method in [
            "item/commandExecution/requestApproval",
            "item/fileChange/requestApproval",
        ] {
            assert!(matches!(
                codex_server_request_response(method),
                CodexServerRequestResponse::Result(value) if value["decision"] == "decline"
            ));
        }
        assert!(matches!(
            codex_server_request_response("item/permissions/requestApproval"),
            CodexServerRequestResponse::Result(value)
                if value["permissions"] == json!({})
                    && value["scope"] == "turn"
                    && value["strictAutoReview"] == false
        ));
        for method in ["applyPatchApproval", "execCommandApproval"] {
            assert!(matches!(
                codex_server_request_response(method),
                CodexServerRequestResponse::Result(value) if value["decision"] == "abort"
            ));
        }
        assert!(matches!(
            codex_server_request_response("item/tool/requestUserInput"),
            CodexServerRequestResponse::Result(value) if value["answers"] == json!({})
        ));
        assert!(matches!(
            codex_server_request_response("mcpServer/elicitation/request"),
            CodexServerRequestResponse::Result(value) if value["action"] == "cancel"
        ));
        for method in [
            "item/tool/call",
            "account/chatgptAuthTokens/refresh",
            "attestation/generate",
        ] {
            assert!(matches!(
                codex_server_request_response(method),
                CodexServerRequestResponse::Error {
                    code: -32033..=-32031,
                    ..
                }
            ));
        }
        assert!(matches!(
            codex_server_request_response("unknown/request"),
            CodexServerRequestResponse::Error { code: -32601, .. }
        ));
    }

    #[test]
    fn utf8_bounded_external_tail_and_prefix_never_split_multibyte_characters() {
        let value = "prefix-中文🙂-suffix";
        let tail = utf8_tail(value, 8);
        assert!(std::str::from_utf8(tail.as_bytes()).is_ok());
        assert!(tail.len() <= 8);
        let prefix = utf8_prefix(value, 10);
        assert!(std::str::from_utf8(prefix.as_bytes()).is_ok());
        assert!(prefix.len() <= 10);
        assert!(value.starts_with(prefix));

        let oversized = format!("{}中", "x".repeat(MAX_TRANSPORT_LINE_BYTES - 1));
        let redacted = redact_external_text(&oversized);
        assert!(std::str::from_utf8(redacted.as_bytes()).is_ok());
        assert!(redacted.ends_with("...[truncated]"));
        assert!(redacted.len() <= MAX_TRANSPORT_LINE_BYTES + "...[truncated]".len());
    }

    #[cfg(windows)]
    #[test]
    fn codex_expired_initialization_deadline_reaps_owned_session() {
        let _fixture_guard = local_transport_fixture_guard();
        let data_dir = temporary_runtime_dir("codex-expired-initialize");
        let script = data_dir.join("blocked-app-server.ps1");
        std::fs::write(&script, "Start-Sleep -Seconds 30\n").unwrap();
        let binary = std::env::var_os("SystemRoot")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("C:\\Windows"))
            .join("System32")
            .join("WindowsPowerShell")
            .join("v1.0")
            .join("powershell.exe");
        let transport = CodexAppServerTransport::new(CodexAppServerConfig {
            binary_path: Some(binary),
            command_args: vec![
                "-NoProfile".into(),
                "-ExecutionPolicy".into(),
                "Bypass".into(),
                "-File".into(),
                script.to_string_lossy().into_owned(),
            ],
            default_model: None,
            request_timeout: Duration::from_secs(2),
        });
        assert!(matches!(
            transport.open_session_until(TransportDeadline::after(Duration::ZERO)),
            Err(RuntimeError::Timeout)
        ));
        assert!(
            transport.lock_state().sessions.is_empty(),
            "an expired initialize deadline must not retain an owned child session"
        );
        let _ = std::fs::remove_dir_all(data_dir);
    }

    #[cfg(windows)]
    #[test]
    fn codex_production_transport_uses_a_local_stdio_fixture_executable() {
        let _fixture_guard = local_transport_fixture_guard();
        let data_dir = temporary_runtime_dir("codex-stdio");
        let script = data_dir.join("codex-app-server-fixture.ps1");
        std::fs::write(
            &script,
            r#"
 param([string]$InterruptMarker, [string]$ReverseMarker)
 function Send-Json([object]$Value) {
   [Console]::Out.WriteLine(($Value | ConvertTo-Json -Compress -Depth 10))
   [Console]::Out.Flush()
 }
 function Assert-ReverseRequest([string]$Method) {
   $script:reverseId += 1
   Send-Json @{ jsonrpc = '2.0'; id = "server-$($script:reverseId)"; method = $Method; params = @{} }
   $replyLine = [Console]::In.ReadLine()
   if ($null -eq $replyLine) { [System.IO.File]::WriteAllText($ReverseMarker, 'missing'); return }
   $reply = $replyLine | ConvertFrom-Json
   $valid = switch ($Method) {
     'item/commandExecution/requestApproval' { $reply.result.decision -eq 'decline' }
     'item/fileChange/requestApproval' { $reply.result.decision -eq 'decline' }
     'item/permissions/requestApproval' { $null -ne $reply.result.permissions -and $reply.result.scope -eq 'turn' -and $reply.result.strictAutoReview -eq $false }
     'applyPatchApproval' { $reply.result.decision -eq 'abort' }
     'execCommandApproval' { $reply.result.decision -eq 'abort' }
     'item/tool/requestUserInput' { $null -ne $reply.result.answers }
     'mcpServer/elicitation/request' { $reply.result.action -eq 'cancel' }
     'item/tool/call' { $reply.error.code -eq -32031 }
     'account/chatgptAuthTokens/refresh' { $reply.error.code -eq -32032 }
     'attestation/generate' { $reply.error.code -eq -32033 }
     default { $reply.error.code -eq -32601 }
   }
   if (-not $valid) { [System.IO.File]::WriteAllText($ReverseMarker, "bad:$Method"); return }
 }
while (($line = [Console]::In.ReadLine()) -ne $null) {
  $message = $line | ConvertFrom-Json
  switch ($message.method) {
    'initialize' {
      Send-Json @{ jsonrpc = '2.0'; id = $message.id; result = @{ serverInfo = @{ version = 'fixture-app-server-v1' } } }
    }
    'model/list' {
      if ($message.params -and $message.params.cursor -eq 'page-2') {
        Send-Json @{ jsonrpc = '2.0'; id = $message.id; result = @{ models = @(@{ id = 'codex-model-b' }) } }
      } else {
        Send-Json @{ jsonrpc = '2.0'; id = $message.id; result = @{ models = @(@{ id = 'codex-model-a'; isDefault = $true }, @{ id = 'codex-model-block' }, @{ id = 'codex-model-timeout' }, @{ id = 'codex-model-final-error' }, @{ id = 'codex-model-close' }); nextCursor = 'page-2'; catalogRevision = 'fixture-codex-r2' } }
      }
    }
    'thread/start' {
      if ($message.params.model -notin @('codex-model-a', 'codex-model-block', 'codex-model-timeout', 'codex-model-final-error', 'codex-model-close')) {
        Send-Json @{ jsonrpc = '2.0'; id = $message.id; error = @{ code = -32602; message = 'model mismatch' } }
      } else {
        $script:activeModel = $message.params.model
        Send-Json @{ jsonrpc = '2.0'; id = $message.id; result = @{ id = 'thread-fixture' } }
      }
    }
    'turn/start' {
      Send-Json @{ jsonrpc = '2.0'; id = $message.id; result = @{ turn = @{ id = 'turn-fixture' } } }
      if ($script:activeModel -ne 'codex-model-block') {
        foreach ($reverseMethod in @(
          'item/commandExecution/requestApproval',
          'item/fileChange/requestApproval',
          'item/permissions/requestApproval',
          'applyPatchApproval',
          'execCommandApproval',
          'item/tool/requestUserInput',
          'mcpServer/elicitation/request',
          'item/tool/call',
          'account/chatgptAuthTokens/refresh',
          'attestation/generate',
          'unknown/request'
        )) { Assert-ReverseRequest $reverseMethod }
        if ($ReverseMarker -and -not (Test-Path $ReverseMarker)) { [System.IO.File]::WriteAllText($ReverseMarker, 'ok') }
        if ($script:activeModel -eq 'codex-model-close') { exit 0 }
        if ($script:activeModel -eq 'codex-model-final-error') {
          Send-Json @{ jsonrpc = '2.0'; method = 'error'; params = @{ threadId = 'thread-fixture'; turnId = 'turn-fixture'; willRetry = $false; error = @{ message = 'final fixture failure'; codexErrorInfo = $null; additionalDetails = $null } } }
        } else {
          Send-Json @{ jsonrpc = '2.0'; method = 'error'; params = @{ threadId = 'thread-fixture'; turnId = 'turn-fixture'; willRetry = $true; error = @{ message = 'retryable fixture failure'; codexErrorInfo = $null; additionalDetails = $null } } }
          if ($script:activeModel -ne 'codex-model-timeout') {
            Send-Json @{ jsonrpc = '2.0'; method = 'item/agentMessage/delta'; params = @{ threadId = 'thread-fixture'; turnId = 'turn-fixture'; delta = 'hello from codex fixture' } }
            Send-Json @{ jsonrpc = '2.0'; method = 'turn/completed'; params = @{ threadId = 'thread-fixture'; turnId = 'turn-fixture'; turn = @{ id = 'turn-fixture'; status = 'completed' } } }
          }
        }
      }
    }
    'turn/interrupt' {
      if ($InterruptMarker) { [System.IO.File]::WriteAllText($InterruptMarker, 'interrupted') }
      Send-Json @{ jsonrpc = '2.0'; id = $message.id; result = @{} }
      Send-Json @{ jsonrpc = '2.0'; method = 'turn/completed'; params = @{ threadId = 'thread-fixture'; turnId = 'turn-fixture'; turn = @{ id = 'turn-fixture'; status = 'cancelled' } } }
    }
  }
}
"#,
        )
        .unwrap();
        let interrupt_marker = data_dir.join("codex-interrupt.marker");
        let reverse_marker = data_dir.join("codex-reverse.marker");
        let binary = std::env::var_os("SystemRoot")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("C:\\Windows"))
            .join("System32")
            .join("WindowsPowerShell")
            .join("v1.0")
            .join("powershell.exe");
        assert!(
            binary.is_file(),
            "PowerShell fixture host must be installed"
        );
        let runtime = CodexAppServerRuntime::with_config(CodexAppServerConfig {
            binary_path: Some(binary),
            command_args: vec![
                "-NoProfile".into(),
                "-ExecutionPolicy".into(),
                "Bypass".into(),
                "-File".into(),
                script.to_string_lossy().into_owned(),
                "-InterruptMarker".into(),
                interrupt_marker.to_string_lossy().into_owned(),
                "-ReverseMarker".into(),
                reverse_marker.to_string_lossy().into_owned(),
            ],
            default_model: None,
            request_timeout: Duration::from_secs(3),
        });
        assert_eq!(
            runtime.list_models(),
            vec![
                "codex-model-a",
                "codex-model-b",
                "codex-model-block",
                "codex-model-close",
                "codex-model-final-error",
                "codex-model-timeout",
            ]
        );
        assert_eq!(
            runtime.catalog_revision().as_deref(),
            Some("fixture-codex-r2")
        );
        assert_eq!(
            runtime.inner.catalog_default_model_id().as_deref(),
            Some("codex-model-a")
        );
        let mut request = request_with_model(
            WorkspaceAccess::ReadOnly,
            Some(data_dir.to_string_lossy().as_ref()),
            "codex-model-a",
        );
        request.connector_id = "desktop-codex-profile".into();
        let mut missing_frozen_model = request.clone();
        missing_frozen_model.model_id = None;
        assert_eq!(
            runtime.execute(&missing_frozen_model),
            Err(RuntimeError::Protocol(CODEX_MODEL_UNAVAILABLE.into()))
        );
        let events = runtime.execute(&request).unwrap();
        assert_eq!(events.last().unwrap().event_type, "execution.completed");
        assert!(events
            .iter()
            .any(|event| event.payload["delta"] == "hello from codex fixture"));
        assert!(!events
            .iter()
            .any(|event| event.event_type == "execution.failed"));
        assert_eq!(
            std::fs::read_to_string(&reverse_marker)
                .expect("strict reverse-RPC fixture marker must exist"),
            "ok",
            "strict fixture must validate every current reverse-RPC response"
        );

        let mut final_error = request.clone();
        final_error.execution_run_id = "codex-final-error".into();
        final_error.model_id = Some("codex-model-final-error".into());
        let final_error_events = runtime.execute(&final_error).unwrap();
        assert_eq!(
            final_error_events
                .last()
                .map(|event| event.event_type.as_str()),
            Some("execution.failed")
        );

        let mut timed_out = request.clone();
        timed_out.execution_run_id = "codex-timeout".into();
        timed_out.model_id = Some("codex-model-timeout".into());
        timed_out.timeout_ms = 75;
        assert_eq!(runtime.execute(&timed_out), Err(RuntimeError::Timeout));

        let mut closed = request.clone();
        closed.execution_run_id = "codex-transport-close".into();
        closed.model_id = Some("codex-model-close".into());
        assert_eq!(runtime.execute(&closed), Err(RuntimeError::TransportClosed));

        let mut blocking = request.clone();
        blocking.execution_run_id = "codex-blocking-turn".into();
        blocking.model_id = Some("codex-model-block".into());
        let stream = runtime
            .stream_events_with_capacity(&blocking, 4)
            .expect("start bounded Codex blocking fixture turn");
        assert_eq!(
            stream
                .next_timeout(Duration::from_secs(2))
                .expect("read connector started")
                .expect("connector started event")
                .event_type,
            "connector.started"
        );
        assert_eq!(
            stream
                .next_timeout(Duration::from_secs(2))
                .expect("read runtime started")
                .expect("runtime started event")
                .event_type,
            "runtime.started"
        );
        assert_eq!(
            runtime
                .cancel(&blocking)
                .expect("interrupt owned Codex child")
                .event_type,
            "execution.cancelled"
        );
        let interrupt_deadline = Instant::now() + Duration::from_secs(2);
        while !interrupt_marker.is_file() && Instant::now() < interrupt_deadline {
            thread::sleep(Duration::from_millis(20));
        }
        assert!(
            interrupt_marker.is_file(),
            "owned Codex fixture must receive the bounded turn/interrupt request"
        );
        let mut terminal = None;
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            match stream.next_timeout(Duration::from_millis(100)) {
                Ok(Some(event)) if event.event_type.starts_with("execution.") => {
                    terminal = Some(event.event_type);
                    break;
                }
                Ok(Some(_)) | Ok(None) | Err(RuntimeError::Timeout) => {}
                Err(RuntimeError::Cancelled) => {
                    terminal = Some("execution.cancelled".into());
                    break;
                }
                Err(error) => {
                    panic!("Codex blocking fixture stream failed after interrupt: {error}")
                }
            }
        }
        assert_eq!(terminal.as_deref(), Some("execution.cancelled"));
        runtime.shutdown_owned().unwrap();
        let _ = std::fs::remove_dir_all(data_dir);
    }

    #[test]
    fn local_connector_discovery_is_read_only_idempotent_and_credential_free() {
        let _fixture_guard = local_transport_fixture_guard();
        let root = temporary_runtime_dir("local-discovery-healthy");
        let data_dir = root.join("kun-data");
        let codex_binary = root.join("codex.exe");
        std::fs::create_dir_all(&data_dir).expect("create isolated Kun data directory");
        std::fs::write(&codex_binary, b"fixture codex executable")
            .expect("write isolated Codex executable fixture");
        let fixture = LocalKunFixture::start(&data_dir, KunFixtureMode::Healthy);
        let runtime_json = data_dir.join("runtime.json");
        let before = std::fs::read(&runtime_json).expect("read isolated runtime record");
        let mut record: Value = serde_json::from_slice(&before).expect("parse runtime record");
        let object = record
            .as_object_mut()
            .expect("fixture record must be an object");
        object.insert("apiKey".into(), json!("fixture-api-key-must-not-leak"));
        object.insert(
            "authorization".into(),
            json!(format!(
                "{}{}",
                "Bearer ", "fixture-authorization-must-not-leak"
            )),
        );
        object.insert("cookie".into(), json!("fixture-cookie-must-not-leak"));
        std::fs::write(
            &runtime_json,
            serde_json::to_vec(&record).expect("serialize runtime record"),
        )
        .expect("write isolated runtime record");
        let expected_record = std::fs::read(&runtime_json).expect("read rewritten runtime record");

        let config = LocalConnectorDiscoveryConfig {
            codex_binary_paths: vec![codex_binary.clone()],
            kun_data_dirs: vec![data_dir.clone()],
            kun_install_dirs: vec![fixture.install_dir.clone()],
            kun_expected_service_version: "0.2.34".into(),
            request_timeout: Duration::from_secs(2),
        };
        let first = discover_local_connectors_with_config(&config);
        let second = discover_local_connectors_with_config(&config);

        assert_eq!(first, second, "repeated discovery must be stable");
        assert_eq!(
            first
                .iter()
                .map(|entry| entry.connector_id.as_str())
                .collect::<Vec<_>>(),
            vec![
                LOCAL_DISCOVERY_CODEX_CONNECTOR_ID,
                LOCAL_DISCOVERY_KUN_CONNECTOR_ID
            ]
        );
        let codex = first
            .iter()
            .find(|entry| entry.connector_id == LOCAL_DISCOVERY_CODEX_CONNECTOR_ID)
            .expect("Codex fixture should be discovered");
        assert_eq!(codex.runtime_type, "codex");
        assert_eq!(codex.availability, "unconfigured");
        assert!(codex.models.is_empty());
        assert!(codex.requires_configuration);
        assert_eq!(codex.source, DiscoverySource::ExecutableInventory);
        let codex_json = serde_json::to_string(codex).unwrap();
        assert!(!codex_json.contains(&codex_binary.display().to_string()));
        assert!(!codex_json.contains(&std::process::id().to_string()));
        assert!(!codex_json.contains("fixture-runtime-token"));
        assert!(!codex_json.contains("authorization"));
        assert!(!codex_json.contains("cookie"));

        let kun = first
            .iter()
            .find(|entry| entry.connector_id == LOCAL_DISCOVERY_KUN_CONNECTOR_ID)
            .expect("Kun fixture should be discovered");
        assert_eq!(kun.runtime_type, "kun");
        assert_eq!(kun.availability, "unconfigured");
        assert!(kun.models.is_empty());
        assert_eq!(kun.catalog_revision.as_deref(), None);
        assert!(kun.requires_configuration);
        assert_eq!(kun.source, DiscoverySource::RuntimeRecord);
        let kun_json = serde_json::to_string(kun).unwrap();
        assert!(!kun_json.contains(&runtime_json.display().to_string()));
        assert!(!kun_json.contains(&record["pid"].to_string()));
        assert!(!kun_json.contains(&record["port"].to_string()));
        assert!(!kun_json.contains("runtimeJson"));
        assert!(!kun_json.contains("fixture-runtime-token"));
        assert!(!kun_json.contains("authorization"));
        assert!(!kun_json.contains("cookie"));

        let serialized = format!("{first:?}").to_ascii_lowercase();
        for forbidden in [
            "fixture-runtime-token",
            "fixture-api-key-must-not-leak",
            "fixture-authorization-must-not-leak",
            "fixture-cookie-must-not-leak",
            "authorization",
            "cookie",
        ] {
            assert!(
                !serialized.contains(forbidden),
                "local discovery leaked credential-like material: {forbidden}"
            );
        }
        assert_eq!(
            std::fs::read(&runtime_json).expect("read runtime record after scan"),
            expected_record,
            "discovery must not write runtime.json"
        );
        drop(fixture);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn local_connector_discovery_is_passive_and_does_not_call_kun_transport() {
        let _fixture_guard = local_transport_fixture_guard();
        let root = temporary_runtime_dir("local-discovery-passive");
        let data_dir = root.join("kun-data");
        std::fs::create_dir_all(&data_dir).expect("create isolated Kun data directory");
        let fixture = LocalKunFixture::start(&data_dir, KunFixtureMode::Healthy);

        let discoveries = discover_local_connectors_with_config(&LocalConnectorDiscoveryConfig {
            codex_binary_paths: Vec::new(),
            kun_data_dirs: vec![data_dir],
            kun_install_dirs: vec![fixture.install_dir.clone()],
            kun_expected_service_version: "0.2.34".into(),
            request_timeout: Duration::from_secs(2),
        });

        assert_eq!(discoveries.len(), 1);
        assert_eq!(discoveries[0].availability, "unconfigured");
        assert!(discoveries[0].models.is_empty());
        assert_eq!(
            fixture.requests.lock().expect("fixture request lock").len(),
            0,
            "passive discovery must not call Kun transport"
        );

        drop(fixture);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn local_connector_discovery_reports_absent_unconfigured_and_untrusted_runtimes() {
        let _fixture_guard = local_transport_fixture_guard();
        let root = temporary_runtime_dir("local-discovery-statuses");
        let absent = LocalConnectorDiscoveryConfig {
            codex_binary_paths: vec![root.join("missing-codex.exe")],
            kun_data_dirs: vec![root.join("missing-kun")],
            kun_install_dirs: Vec::new(),
            kun_expected_service_version: "0.2.34".into(),
            request_timeout: Duration::from_secs(2),
        };
        assert!(
            discover_local_connectors_with_config(&absent).is_empty(),
            "an isolated empty fixture must not discover user runtimes"
        );

        let unconfigured_data = root.join("unconfigured-kun");
        std::fs::create_dir_all(&unconfigured_data)
            .expect("create unconfigured Kun data directory");
        let unconfigured_fixture =
            LocalKunFixture::start(&unconfigured_data, KunFixtureMode::CatalogUnavailable);
        let unconfigured = discover_local_connectors_with_config(&LocalConnectorDiscoveryConfig {
            codex_binary_paths: Vec::new(),
            kun_data_dirs: vec![unconfigured_data.clone()],
            kun_install_dirs: vec![unconfigured_fixture.install_dir.clone()],
            kun_expected_service_version: "0.2.34".into(),
            request_timeout: Duration::from_secs(2),
        });
        assert_eq!(unconfigured.len(), 1);
        assert_eq!(unconfigured[0].availability, "unconfigured");
        assert!(unconfigured[0].requires_configuration);
        assert!(unconfigured[0].models.is_empty());
        drop(unconfigured_fixture);

        let mismatched_data = root.join("identity-mismatch-kun");
        std::fs::create_dir_all(&mismatched_data)
            .expect("create identity-mismatch Kun data directory");
        let mismatched_fixture =
            LocalKunFixture::start(&mismatched_data, KunFixtureMode::IdentityMismatch);
        let mismatched = discover_local_connectors_with_config(&LocalConnectorDiscoveryConfig {
            codex_binary_paths: Vec::new(),
            kun_data_dirs: vec![mismatched_data.clone()],
            kun_install_dirs: vec![mismatched_fixture.install_dir.clone()],
            kun_expected_service_version: "0.2.34".into(),
            request_timeout: Duration::from_secs(2),
        });
        assert_eq!(mismatched.len(), 1);
        assert_eq!(mismatched[0].availability, "unconfigured");
        assert!(mismatched[0].requires_configuration);
        assert!(mismatched[0].models.is_empty());
        drop(mismatched_fixture);

        let authentication_data = root.join("authentication-required-kun");
        std::fs::create_dir_all(&authentication_data)
            .expect("create authentication Kun data directory");
        let authentication_fixture =
            LocalKunFixture::start(&authentication_data, KunFixtureMode::Healthy);
        let runtime_json = authentication_data.join("runtime.json");
        let mut record: Value = serde_json::from_slice(
            &std::fs::read(&runtime_json).expect("read authentication fixture record"),
        )
        .expect("parse authentication fixture record");
        record
            .as_object_mut()
            .expect("fixture record must be an object")
            .remove("runtimeToken");
        std::fs::write(
            &runtime_json,
            serde_json::to_vec(&record).expect("serialize missing-token record"),
        )
        .expect("write missing-token record");
        let request_count_before = authentication_fixture
            .requests
            .lock()
            .expect("fixture request lock")
            .len();
        let authentication_required =
            discover_local_connectors_with_config(&LocalConnectorDiscoveryConfig {
                codex_binary_paths: Vec::new(),
                kun_data_dirs: vec![authentication_data.clone()],
                kun_install_dirs: vec![authentication_fixture.install_dir.clone()],
                kun_expected_service_version: "0.2.34".into(),
                request_timeout: Duration::from_secs(2),
            });
        assert_eq!(authentication_required.len(), 1);
        assert_eq!(authentication_required[0].availability, "unconfigured");
        assert!(authentication_required[0].requires_configuration);
        assert_eq!(
            authentication_fixture
                .requests
                .lock()
                .expect("fixture request lock")
                .len(),
            request_count_before,
            "a token-less record must not cause a health request"
        );
        drop(authentication_fixture);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn local_connector_discovery_accepts_kun_service_version_from_record() {
        let _fixture_guard = local_transport_fixture_guard();
        let root = temporary_runtime_dir("local-discovery-kun-version");
        let data_dir = root.join("kun-data");
        std::fs::create_dir_all(&data_dir).expect("create isolated Kun data directory");
        let fixture = LocalKunFixture::start_with_metadata(
            &data_dir,
            KunFixtureMode::Healthy,
            KunFixtureMetadata {
                instance_id: "fixture-instance-v37".into(),
                service_version: "0.2.37".into(),
                build_id: "fixture-build".into(),
            },
        );
        let runtime_json = data_dir.join("runtime.json");
        let record: Value = serde_json::from_slice(
            &std::fs::read(&runtime_json).expect("read versioned Kun fixture record"),
        )
        .expect("parse versioned Kun fixture record");

        let discoveries = discover_local_connectors_with_config(&LocalConnectorDiscoveryConfig {
            codex_binary_paths: Vec::new(),
            kun_data_dirs: vec![data_dir.clone()],
            kun_install_dirs: vec![fixture.install_dir.clone()],
            kun_expected_service_version: "0.2.34".into(),
            request_timeout: Duration::from_secs(2),
        });

        let kun = discoveries
            .iter()
            .find(|entry| entry.connector_id == LOCAL_DISCOVERY_KUN_CONNECTOR_ID)
            .expect("Kun fixture should still be discovered");
        assert_eq!(kun.source, DiscoverySource::RuntimeRecord);
        assert_eq!(kun.availability, "unconfigured");
        assert!(kun.models.is_empty());
        assert_eq!(
            record.get("serviceVersion").and_then(Value::as_str),
            Some("0.2.37")
        );
        drop(fixture);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn local_connector_discovery_separates_same_version_different_kun_instances() {
        let _fixture_guard = local_transport_fixture_guard();
        let root = temporary_runtime_dir("local-discovery-kun-instances");

        let data_a = root.join("kun-a");
        std::fs::create_dir_all(&data_a).unwrap();
        let fixture_a = LocalKunFixture::start_with_metadata(
            &data_a,
            KunFixtureMode::Healthy,
            KunFixtureMetadata {
                instance_id: "kun-instance-a".into(),
                service_version: "0.2.34".into(),
                build_id: "fixture-build".into(),
            },
        );

        let data_b = root.join("kun-b");
        std::fs::create_dir_all(&data_b).unwrap();
        let fixture_b = LocalKunFixture::start_with_metadata(
            &data_b,
            KunFixtureMode::Healthy,
            KunFixtureMetadata {
                instance_id: "kun-instance-b".into(),
                service_version: "0.2.34".into(),
                build_id: "fixture-build".into(),
            },
        );

        let discovery_a = discover_local_connectors_with_config(&LocalConnectorDiscoveryConfig {
            codex_binary_paths: Vec::new(),
            kun_data_dirs: vec![data_a.clone()],
            kun_install_dirs: vec![fixture_a.install_dir.clone()],
            kun_expected_service_version: "0.2.34".into(),
            request_timeout: Duration::from_secs(2),
        });
        let discovery_b = discover_local_connectors_with_config(&LocalConnectorDiscoveryConfig {
            codex_binary_paths: Vec::new(),
            kun_data_dirs: vec![data_b.clone()],
            kun_install_dirs: vec![fixture_b.install_dir.clone()],
            kun_expected_service_version: "0.2.34".into(),
            request_timeout: Duration::from_secs(2),
        });

        let kun_a = discovery_a
            .iter()
            .find(|entry| entry.connector_id == LOCAL_DISCOVERY_KUN_CONNECTOR_ID)
            .expect("first Kun fixture should be discovered");
        let kun_b = discovery_b
            .iter()
            .find(|entry| entry.connector_id == LOCAL_DISCOVERY_KUN_CONNECTOR_ID)
            .expect("second Kun fixture should be discovered");

        assert_eq!(kun_a.availability, "unconfigured");
        assert_eq!(kun_b.availability, "unconfigured");
        assert_eq!(kun_a.source, DiscoverySource::RuntimeRecord);
        assert_eq!(kun_b.source, DiscoverySource::RuntimeRecord);
        assert!(kun_a.models.is_empty());
        assert!(kun_b.models.is_empty());

        drop(fixture_a);
        drop(fixture_b);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn local_connector_discovery_scans_all_kun_data_dirs_and_invalid_first_does_not_shadow_next() {
        let _fixture_guard = local_transport_fixture_guard();
        let root = temporary_runtime_dir("local-discovery-kun-many");
        let invalid_data = root.join("invalid-kun");
        std::fs::create_dir_all(&invalid_data).unwrap();
        std::fs::write(invalid_data.join("runtime.json"), b"{ not json").unwrap();

        let healthy_data = root.join("healthy-kun");
        std::fs::create_dir_all(&healthy_data).unwrap();
        let fixture = LocalKunFixture::start_with_metadata(
            &healthy_data,
            KunFixtureMode::Healthy,
            KunFixtureMetadata {
                instance_id: "healthy-instance".into(),
                service_version: "0.2.34".into(),
                build_id: "fixture-build".into(),
            },
        );

        let discoveries = discover_local_connectors_with_config(&LocalConnectorDiscoveryConfig {
            codex_binary_paths: Vec::new(),
            kun_data_dirs: vec![invalid_data, healthy_data],
            kun_install_dirs: vec![fixture.install_dir.clone()],
            kun_expected_service_version: "0.2.34".into(),
            request_timeout: Duration::from_secs(2),
        });

        assert_eq!(discoveries.len(), 2);
        assert!(discoveries
            .iter()
            .any(|candidate| candidate.availability == "unavailable"));
        assert!(discoveries
            .iter()
            .any(|candidate| candidate.availability == "unconfigured"));

        drop(fixture);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn kun_fingerprint_stays_stable_for_same_instance_across_pid_and_port_changes() {
        let record_a = LocalKunRecord {
            runtime_json: std::path::PathBuf::from(r"C:\fixture\a\runtime.json"),
            _port: Some(1111),
            instance_id: Some("stable-instance".into()),
            service_version: Some("0.2.37".into()),
            build_id: Some("fixture-build".into()),
            parsed: true,
            diagnostics: Vec::new(),
        };
        let record_b = LocalKunRecord {
            runtime_json: std::path::PathBuf::from(r"C:\fixture\b\runtime.json"),
            _port: Some(2222),
            instance_id: Some("stable-instance".into()),
            service_version: Some("0.2.37".into()),
            build_id: Some("fixture-build".into()),
            parsed: true,
            diagnostics: Vec::new(),
        };

        assert_eq!(kun_fingerprint(&record_a), kun_fingerprint(&record_b));
    }

    #[test]
    fn kun_fingerprint_distinguishes_same_version_different_instances() {
        let record_a = LocalKunRecord {
            runtime_json: std::path::PathBuf::from(r"C:\fixture\a\runtime.json"),
            _port: Some(1111),
            instance_id: Some("instance-a".into()),
            service_version: Some("0.2.37".into()),
            build_id: Some("fixture-build".into()),
            parsed: true,
            diagnostics: Vec::new(),
        };
        let record_b = LocalKunRecord {
            runtime_json: std::path::PathBuf::from(r"C:\fixture\b\runtime.json"),
            _port: Some(2222),
            instance_id: Some("instance-b".into()),
            service_version: Some("0.2.37".into()),
            build_id: Some("fixture-build".into()),
            parsed: true,
            diagnostics: Vec::new(),
        };

        assert_ne!(kun_fingerprint(&record_a), kun_fingerprint(&record_b));
    }

    #[test]
    fn kun_missing_instance_id_is_stable_across_pid_port_token_and_fails_closed() {
        let root = temporary_runtime_dir("local-discovery-kun-missing-instance");
        let data_dir = root.join("kun-data");
        std::fs::create_dir_all(&data_dir).unwrap();
        let runtime_json = data_dir.join("runtime.json");
        let write_record = |pid: u64, port: u16, token: &str| {
            std::fs::write(
                &runtime_json,
                serde_json::to_vec(&json!({
                    "version": 2,
                    "pid": pid,
                    "host": "127.0.0.1",
                    "port": port,
                    "runtimeToken": token,
                    "serviceVersion": "0.2.34",
                    "buildId": "fixture-build",
                    "launchMode": "shared",
                }))
                .unwrap(),
            )
            .unwrap();
        };
        write_record(111, 1111, "token-a");
        let first = read_local_kun_record(&data_dir).expect("first record");
        let first_fingerprint = kun_fingerprint(&first);
        let first_observation = kun_observation(
            &LocalConnectorDiscoveryConfig {
                codex_binary_paths: Vec::new(),
                kun_data_dirs: vec![data_dir.clone()],
                kun_install_dirs: Vec::new(),
                kun_expected_service_version: "0.2.34".into(),
                request_timeout: Duration::from_millis(50),
            },
            first,
            false,
        );

        write_record(222, 2222, "token-b");
        let second = read_local_kun_record(&data_dir).expect("second record");
        let second_fingerprint = kun_fingerprint(&second);

        assert_eq!(first_fingerprint, second_fingerprint);
        assert_eq!(
            first_observation.availability,
            CandidateAvailability::Unavailable
        );
        assert_eq!(
            first_observation.compatibility_state,
            CompatibilityState::Incompatible
        );
        assert!(first_observation
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == DiscoveryDiagnosticCode::InvalidIdentity));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn kun_passive_projection_is_identical_for_absent_empty_and_rotated_tokens() {
        let root = temporary_runtime_dir("local-discovery-kun-token-invariance");
        let data_dir = root.join("kun-data");
        std::fs::create_dir_all(&data_dir).unwrap();
        let runtime_json = data_dir.join("runtime.json");
        let config = LocalConnectorDiscoveryConfig {
            codex_binary_paths: Vec::new(),
            kun_data_dirs: vec![data_dir.clone()],
            kun_install_dirs: Vec::new(),
            kun_expected_service_version: "0.2.34".into(),
            request_timeout: Duration::from_millis(50),
        };
        let write_record = |token: Option<&str>| {
            let mut record = serde_json::Map::new();
            record.insert("version".into(), json!(2));
            record.insert("pid".into(), json!(1111));
            record.insert("host".into(), json!("127.0.0.1"));
            record.insert("port".into(), json!(41111));
            record.insert("instanceId".into(), json!("token-invariant-instance"));
            record.insert("serviceVersion".into(), json!("0.2.34"));
            record.insert("buildId".into(), json!("fixture-build"));
            record.insert("launchMode".into(), json!("shared"));
            record.insert("apiKey".into(), json!("fixture-api-key-must-be-ignored"));
            record.insert(
                "Authorization".into(),
                json!(format!(
                    "{}{}",
                    "Bearer ", "fixture-authorization-must-be-ignored"
                )),
            );
            record.insert("Cookie".into(), json!("fixture-cookie-must-be-ignored"));
            if let Some(token) = token {
                record.insert("runtimeToken".into(), json!(token));
            }
            std::fs::write(&runtime_json, serde_json::to_vec(&record).unwrap()).unwrap();
            let record = read_local_kun_record(&data_dir).expect("passive Kun record");
            kun_observation(&config, record, false).project()
        };

        let absent = write_record(None);
        let empty = write_record(Some(""));
        let token_a = write_record(Some("rotated-token-a"));
        let token_b = write_record(Some("rotated-token-b"));

        assert_eq!(absent, empty);
        assert_eq!(absent, token_a);
        assert_eq!(absent, token_b);
        assert_eq!(absent.auth_state, AuthState::Unknown);
        assert_eq!(absent.availability, CandidateAvailability::Unconfigured);
        assert!(absent.requires_configuration);
        let serialized = serde_json::to_string(&absent).unwrap();
        for forbidden in [
            "rotated-token",
            "fixture-api-key",
            "fixture-authorization",
            "fixture-cookie",
        ] {
            assert!(!serialized.contains(forbidden));
        }
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn passive_runtime_record_does_not_emit_unproven_version_build_or_install_evidence() {
        let root = temporary_runtime_dir("local-discovery-kun-evidence");
        let data_dir = root.join("kun-data");
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::write(
            data_dir.join("runtime.json"),
            serde_json::to_vec(&json!({
                "version": 2,
                "pid": 1111,
                "host": "127.0.0.1",
                "port": 41111,
                "instanceId": "evidence-instance",
                "runtimeToken": "fixture-token",
                "launchMode": "shared"
            }))
            .unwrap(),
        )
        .unwrap();
        let record = read_local_kun_record(&data_dir).expect("passive Kun record");
        let evidence = kun_evidence_summary(
            &LocalConnectorDiscoveryConfig {
                codex_binary_paths: Vec::new(),
                kun_data_dirs: vec![data_dir.clone()],
                kun_install_dirs: vec![root.join("install")],
                kun_expected_service_version: "0.2.34".into(),
                request_timeout: Duration::from_millis(50),
            },
            &record,
        );

        assert_eq!(evidence, vec![DiscoveryEvidence::RuntimeRecord]);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn oversized_kun_runtime_json_is_bounded_and_fail_closed() {
        let root = temporary_runtime_dir("local-discovery-kun-oversized");
        let data_dir = root.join("kun-data");
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::write(
            data_dir.join("runtime.json"),
            vec![b'a'; MAX_TRANSPORT_BODY_BYTES + 1],
        )
        .unwrap();

        let record = read_local_kun_record(&data_dir).expect("oversized record");
        let observation = kun_observation(
            &LocalConnectorDiscoveryConfig {
                codex_binary_paths: Vec::new(),
                kun_data_dirs: vec![data_dir.clone()],
                kun_install_dirs: Vec::new(),
                kun_expected_service_version: "0.2.34".into(),
                request_timeout: Duration::from_millis(50),
            },
            record,
            false,
        );

        assert_eq!(observation.availability, CandidateAvailability::Unavailable);
        assert!(observation
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == DiscoveryDiagnosticCode::OversizedInput));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn passive_kun_runtime_record_streaming_parser_rejects_oversized_unknown_secret_field() {
        let oversized = format!(
            "{{\"instanceId\":\"streaming-instance\",\"runtimeToken\":\"{}\"}}",
            "x".repeat(MAX_TRANSPORT_BODY_BYTES + 32)
        );
        let result = parse_passive_kun_runtime_record_from_reader(
            std::io::Cursor::new(oversized.into_bytes()),
            MAX_TRANSPORT_BODY_BYTES,
        );
        assert!(matches!(
            result,
            Err(DiscoveryDiagnosticCode::OversizedInput)
        ));
    }

    #[test]
    fn public_local_discovery_report_exposes_runtime_host_diagnostics_without_ipc() {
        let root = temporary_runtime_dir("local-discovery-public-report");
        let data_dir = root.join("kun-data");
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::write(
            data_dir.join("runtime.json"),
            vec![b'a'; MAX_TRANSPORT_BODY_BYTES + 1],
        )
        .unwrap();

        let report = discover_local_connectors_report_with_config(&LocalConnectorDiscoveryConfig {
            codex_binary_paths: Vec::new(),
            kun_data_dirs: vec![data_dir],
            kun_install_dirs: Vec::new(),
            kun_expected_service_version: "0.2.34".into(),
            request_timeout: Duration::from_millis(50),
        });

        assert_eq!(report.candidates.len(), 1);
        assert_eq!(report.projections.len(), 1);
        assert_eq!(report.candidates[0].availability, "unavailable");
        assert!(report.diagnostics.is_empty());
        assert!(report.projections[0]
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == DiscoveryDiagnosticCode::OversizedInput));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn windows_passive_path_discovers_direct_children_only_and_keeps_unknown_unverified() {
        let root = temporary_runtime_dir("w2-path-provider");
        let path_dir = root.join("bin");
        let nested_dir = path_dir.join("nested");
        std::fs::create_dir_all(&nested_dir).unwrap();
        let direct = path_dir.join("agent.exe");
        let nested = nested_dir.join("nested-agent.exe");
        std::fs::write(&direct, b"not executed direct fixture").unwrap();
        std::fs::write(&nested, b"not executed nested fixture").unwrap();
        let path_env = std::env::join_paths([&path_dir, &path_dir, &nested_dir])
            .unwrap()
            .to_string_lossy()
            .into_owned();

        let report = discover_windows_passive_report_with_config(&WindowsPassiveDiscoveryConfig {
            path_env: Some(path_env),
            use_real_app_paths: false,
            use_real_packages: false,
            use_real_loopback: false,
            max_results: 8,
            request_timeout: Duration::from_secs(2),
            ..WindowsPassiveDiscoveryConfig::default()
        });

        assert_eq!(report.projections.len(), 2);
        let names = report
            .projections
            .iter()
            .map(|candidate| candidate.display_name.as_str())
            .collect::<BTreeSet<_>>();
        assert!(names.contains("agent.exe"));
        assert!(names.contains("nested-agent.exe"));
        let direct_candidate = report
            .projections
            .iter()
            .find(|candidate| candidate.display_name == "agent.exe")
            .unwrap();
        assert_eq!(direct_candidate.category, CandidateCategory::Unknown);
        assert_eq!(
            direct_candidate.compatibility_state,
            CompatibilityState::NotVerified
        );
        assert_eq!(direct_candidate.auth_state, AuthState::Unknown);
        assert_eq!(direct_candidate.health_state, HealthState::NotChecked);
        assert_eq!(
            direct_candidate.source_kinds,
            vec![ObservationSourceKind::WindowsPath]
        );
        assert!(direct_candidate.requires_configuration);
        let serialized = serde_json::to_string(&report.projections).unwrap();
        assert!(!serialized.contains(&root.display().to_string()));
        assert!(!serialized.contains("nested\\nested-agent"));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn windows_passive_merges_all_sources_for_same_executable_without_path_pid_or_port_projection()
    {
        let root = temporary_runtime_dir("w2-source-merge");
        let path_dir = root.join("bin");
        std::fs::create_dir_all(&path_dir).unwrap();
        let executable = path_dir.join("merged-agent.exe");
        std::fs::write(&executable, b"same executable content").unwrap();

        let report = discover_windows_passive_report_with_config(&WindowsPassiveDiscoveryConfig {
            path_env: Some(path_dir.display().to_string()),
            app_path_records: vec![WindowsAppPathRecord {
                key_name: "merged-agent.exe".into(),
                executable_path: executable.clone(),
                hive: WindowsRegistryHive::CurrentUser,
                view: WindowsRegistryView::Native,
            }],
            loopback_records: vec![WindowsLoopbackListenerRecord {
                address: "127.1.2.3".into(),
                port: 49152,
                owner_pid: 4242,
                owner_executable: Some(executable.clone()),
                owner_identity: Some("created-at-fixture".into()),
            }],
            explicit_sources: vec![ExplicitDiscoverySource::Executable(executable.clone())],
            use_real_app_paths: false,
            use_real_packages: false,
            use_real_loopback: false,
            max_results: 8,
            request_timeout: Duration::from_secs(2),
            ..WindowsPassiveDiscoveryConfig::default()
        });

        assert_eq!(report.projections.len(), 1);
        let candidate = &report.projections[0];
        assert_eq!(
            candidate.source_kinds,
            vec![
                ObservationSourceKind::WindowsPath,
                ObservationSourceKind::WindowsAppPath,
                ObservationSourceKind::LoopbackListener,
                ObservationSourceKind::UserSelected,
            ]
        );
        assert_eq!(candidate.trust_level, ObservationTrustLevel::UserSelected);
        assert_eq!(candidate.discovery_state, DiscoveryState::Observed);
        assert_eq!(
            candidate.compatibility_state,
            CompatibilityState::NotVerified
        );
        assert!(candidate.requires_configuration);
        let serialized = serde_json::to_string(candidate).unwrap();
        assert!(!serialized.contains(&root.display().to_string()));
        assert!(!serialized.contains("49152"));
        assert!(!serialized.contains("4242"));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn distinct_file_ids_with_identical_bytes_do_not_merge() {
        let root = temporary_runtime_dir("w21-distinct-file-ids");
        let first_dir = root.join("first");
        let second_dir = root.join("second");
        std::fs::create_dir_all(&first_dir).unwrap();
        std::fs::create_dir_all(&second_dir).unwrap();
        let first = first_dir.join("same.exe");
        let second = second_dir.join("same.exe");
        std::fs::write(&first, b"identical executable bytes").unwrap();
        std::fs::write(&second, b"identical executable bytes").unwrap();

        let report = discover_windows_passive_report_with_config(&WindowsPassiveDiscoveryConfig {
            path_env: Some(
                std::env::join_paths([&first_dir, &second_dir])
                    .unwrap()
                    .to_string_lossy()
                    .into_owned(),
            ),
            use_real_app_paths: false,
            use_real_packages: false,
            use_real_loopback: false,
            max_results: 8,
            request_timeout: Duration::from_secs(2),
            ..WindowsPassiveDiscoveryConfig::default()
        });

        assert_eq!(
            report.projections.len(),
            2,
            "separate files with identical bytes must keep separate private identities"
        );
        let ids = report
            .projections
            .iter()
            .map(|candidate| candidate.candidate_id.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(ids.len(), 2);
        let serialized = serde_json::to_string(&report).unwrap();
        assert!(!serialized.contains(&root.display().to_string()));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn same_real_file_across_path_app_path_loopback_and_explicit_merges() {
        let root = temporary_runtime_dir("w21-same-real-file-merge");
        let path_dir = root.join("bin");
        std::fs::create_dir_all(&path_dir).unwrap();
        let executable = path_dir.join("shared.exe");
        std::fs::write(&executable, b"one physical executable").unwrap();

        let report = discover_windows_passive_report_with_config(&WindowsPassiveDiscoveryConfig {
            path_env: Some(path_dir.display().to_string()),
            app_path_records: vec![WindowsAppPathRecord {
                key_name: "shared.exe".into(),
                executable_path: executable.clone(),
                hive: WindowsRegistryHive::CurrentUser,
                view: WindowsRegistryView::Native,
            }],
            loopback_records: vec![WindowsLoopbackListenerRecord {
                address: "127.0.0.1".into(),
                port: 49154,
                owner_pid: 7777,
                owner_executable: Some(executable.clone()),
                owner_identity: Some("fixture-creation-a".into()),
            }],
            loopback_recheck_records: Some(vec![WindowsLoopbackListenerRecord {
                address: "127.0.0.1".into(),
                port: 49154,
                owner_pid: 7777,
                owner_executable: Some(executable.clone()),
                owner_identity: Some("fixture-creation-a".into()),
            }]),
            explicit_sources: vec![ExplicitDiscoverySource::Executable(executable.clone())],
            use_real_app_paths: false,
            use_real_packages: false,
            use_real_loopback: false,
            max_results: 8,
            request_timeout: Duration::from_secs(2),
            ..WindowsPassiveDiscoveryConfig::default()
        });

        assert_eq!(report.projections.len(), 1);
        assert_eq!(
            report.projections[0].source_kinds,
            vec![
                ObservationSourceKind::WindowsPath,
                ObservationSourceKind::WindowsAppPath,
                ObservationSourceKind::LoopbackListener,
                ObservationSourceKind::UserSelected,
            ]
        );
        let serialized = serde_json::to_string(&report).unwrap();
        assert!(!serialized.contains(&root.display().to_string()));
        assert!(!serialized.contains("49154"));
        assert!(!serialized.contains("7777"));
        assert!(!serialized.contains("fixture-creation-a"));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn same_physical_executable_across_path_and_package_merges() {
        let root = temporary_runtime_dir("w22-path-package-merge");
        let package_root = root.join("package-current");
        let bin_dir = package_root.join("bin");
        std::fs::create_dir_all(&bin_dir).unwrap();
        let executable = bin_dir.join("shared-package-agent.exe");
        std::fs::write(&executable, b"same physical package executable").unwrap();

        let report = discover_windows_passive_report_with_config(&WindowsPassiveDiscoveryConfig {
            path_env: Some(bin_dir.display().to_string()),
            package_records: vec![WindowsPackageRecord {
                package_family_name: "Shared.Package_fixture".into(),
                package_full_name: "Shared.Package_fixture_1.0.0.0_x64__fixture".into(),
                version: "1.0.0.0".into(),
                installed_location: package_root.clone(),
                executable_relative_path: PathBuf::from("bin").join("shared-package-agent.exe"),
            }],
            use_real_app_paths: false,
            use_real_packages: false,
            use_real_loopback: false,
            max_results: 8,
            request_timeout: Duration::from_secs(2),
            ..WindowsPassiveDiscoveryConfig::default()
        });

        assert_eq!(report.projections.len(), 1);
        assert_eq!(
            report.projections[0].source_kinds,
            vec![
                ObservationSourceKind::WindowsPath,
                ObservationSourceKind::WindowsPackage,
            ]
        );
        let serialized = serde_json::to_string(&report).unwrap();
        for forbidden in [
            &root.display().to_string(),
            "Shared.Package",
            "1.0.0.0",
            "x64__fixture",
        ] {
            assert!(!serialized.contains(forbidden), "leaked {forbidden}");
        }
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn same_physical_executable_across_all_windows_sources_merges() {
        let root = temporary_runtime_dir("w22-all-source-package-merge");
        let package_root = root.join("package-current");
        let bin_dir = package_root.join("bin");
        std::fs::create_dir_all(&bin_dir).unwrap();
        let executable = bin_dir.join("all-source-agent.exe");
        std::fs::write(&executable, b"all passive windows sources").unwrap();

        let report = discover_windows_passive_report_with_config(&WindowsPassiveDiscoveryConfig {
            path_env: Some(bin_dir.display().to_string()),
            app_path_records: vec![WindowsAppPathRecord {
                key_name: "all-source-agent.exe".into(),
                executable_path: executable.clone(),
                hive: WindowsRegistryHive::CurrentUser,
                view: WindowsRegistryView::Native,
            }],
            package_records: vec![WindowsPackageRecord {
                package_family_name: "AllSources.Package_fixture".into(),
                package_full_name: "AllSources.Package_fixture_1.0.0.0_x64__fixture".into(),
                version: "1.0.0.0".into(),
                installed_location: package_root.clone(),
                executable_relative_path: PathBuf::from("bin").join("all-source-agent.exe"),
            }],
            loopback_records: vec![WindowsLoopbackListenerRecord {
                address: "127.0.0.1".into(),
                port: 49220,
                owner_pid: 9220,
                owner_executable: Some(executable.clone()),
                owner_identity: Some("fixture-owner-all-sources".into()),
            }],
            loopback_recheck_records: Some(vec![WindowsLoopbackListenerRecord {
                address: "127.0.0.1".into(),
                port: 49220,
                owner_pid: 9220,
                owner_executable: Some(executable.clone()),
                owner_identity: Some("fixture-owner-all-sources".into()),
            }]),
            explicit_sources: vec![ExplicitDiscoverySource::Executable(executable.clone())],
            use_real_app_paths: false,
            use_real_packages: false,
            use_real_loopback: false,
            max_results: 8,
            request_timeout: Duration::from_secs(2),
            ..WindowsPassiveDiscoveryConfig::default()
        });

        assert_eq!(report.projections.len(), 1);
        assert_eq!(
            report.projections[0].source_kinds,
            vec![
                ObservationSourceKind::WindowsPath,
                ObservationSourceKind::WindowsAppPath,
                ObservationSourceKind::WindowsPackage,
                ObservationSourceKind::LoopbackListener,
                ObservationSourceKind::UserSelected,
            ]
        );
        let serialized = serde_json::to_string(&report).unwrap();
        for forbidden in [
            &root.display().to_string(),
            "AllSources.Package",
            "49220",
            "9220",
            "fixture-owner-all-sources",
        ] {
            assert!(!serialized.contains(forbidden), "leaked {forbidden}");
        }
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn package_candidate_id_survives_version_and_full_name_upgrade() {
        let root = temporary_runtime_dir("w22-package-upgrade-stable-id");
        let first_root = root.join("package-v1");
        let second_root = root.join("package-v2");
        std::fs::create_dir_all(first_root.join("bin")).unwrap();
        std::fs::create_dir_all(second_root.join("bin")).unwrap();
        std::fs::write(first_root.join("bin").join("agent.exe"), b"version one").unwrap();
        std::fs::write(second_root.join("bin").join("agent.exe"), b"version two").unwrap();

        let report_v1 =
            discover_windows_passive_report_with_config(&WindowsPassiveDiscoveryConfig {
                path_env: None,
                package_records: vec![WindowsPackageRecord {
                    package_family_name: "Upgrade.Package_fixture".into(),
                    package_full_name: "Upgrade.Package_fixture_1.0.0.0_x64__fixture".into(),
                    version: "1.0.0.0".into(),
                    installed_location: first_root.clone(),
                    executable_relative_path: PathBuf::from("bin").join("agent.exe"),
                }],
                use_real_app_paths: false,
                use_real_packages: false,
                use_real_loopback: false,
                max_results: 8,
                request_timeout: Duration::from_secs(2),
                ..WindowsPassiveDiscoveryConfig::default()
            });
        let report_v2 =
            discover_windows_passive_report_with_config(&WindowsPassiveDiscoveryConfig {
                path_env: None,
                package_records: vec![WindowsPackageRecord {
                    package_family_name: "Upgrade.Package_fixture".into(),
                    package_full_name: "Upgrade.Package_fixture_2.0.0.0_x64__fixture".into(),
                    version: "2.0.0.0".into(),
                    installed_location: second_root.clone(),
                    executable_relative_path: PathBuf::from("bin").join("agent.exe"),
                }],
                use_real_app_paths: false,
                use_real_packages: false,
                use_real_loopback: false,
                max_results: 8,
                request_timeout: Duration::from_secs(2),
                ..WindowsPassiveDiscoveryConfig::default()
            });

        assert_eq!(report_v1.projections.len(), 1);
        assert_eq!(report_v2.projections.len(), 1);
        assert_eq!(
            report_v1.projections[0].candidate_id, report_v2.projections[0].candidate_id,
            "package candidate identity must survive full-name, version, path, file-id, and content changes"
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn different_package_families_with_identical_bytes_remain_distinct() {
        let root = temporary_runtime_dir("w22-different-package-families");
        let package_a = root.join("package-a");
        let package_b = root.join("package-b");
        std::fs::create_dir_all(package_a.join("bin")).unwrap();
        std::fs::create_dir_all(package_b.join("bin")).unwrap();
        std::fs::write(package_a.join("bin").join("agent.exe"), b"same bytes").unwrap();
        std::fs::write(package_b.join("bin").join("agent.exe"), b"same bytes").unwrap();

        let report = discover_windows_passive_report_with_config(&WindowsPassiveDiscoveryConfig {
            path_env: None,
            package_records: vec![
                WindowsPackageRecord {
                    package_family_name: "Family.A_fixture".into(),
                    package_full_name: "Family.A_fixture_1.0.0.0_x64__fixture".into(),
                    version: "1.0.0.0".into(),
                    installed_location: package_a,
                    executable_relative_path: PathBuf::from("bin").join("agent.exe"),
                },
                WindowsPackageRecord {
                    package_family_name: "Family.B_fixture".into(),
                    package_full_name: "Family.B_fixture_1.0.0.0_x64__fixture".into(),
                    version: "1.0.0.0".into(),
                    installed_location: package_b,
                    executable_relative_path: PathBuf::from("bin").join("agent.exe"),
                },
            ],
            use_real_app_paths: false,
            use_real_packages: false,
            use_real_loopback: false,
            max_results: 8,
            request_timeout: Duration::from_secs(2),
            ..WindowsPassiveDiscoveryConfig::default()
        });

        assert_eq!(report.projections.len(), 2);
        let ids = report
            .projections
            .iter()
            .map(|candidate| candidate.candidate_id.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(ids.len(), 2);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn identical_bytes_with_different_file_ids_remain_distinct() {
        let root = temporary_runtime_dir("w22-identical-bytes-distinct-file-id");
        let first_dir = root.join("first");
        let second_dir = root.join("second");
        std::fs::create_dir_all(&first_dir).unwrap();
        std::fs::create_dir_all(&second_dir).unwrap();
        std::fs::write(first_dir.join("same.exe"), b"identical bytes").unwrap();
        std::fs::write(second_dir.join("same.exe"), b"identical bytes").unwrap();

        let report = discover_windows_passive_report_with_config(&WindowsPassiveDiscoveryConfig {
            path_env: Some(
                std::env::join_paths([&first_dir, &second_dir])
                    .unwrap()
                    .to_string_lossy()
                    .into_owned(),
            ),
            use_real_app_paths: false,
            use_real_packages: false,
            use_real_loopback: false,
            max_results: 8,
            request_timeout: Duration::from_secs(2),
            ..WindowsPassiveDiscoveryConfig::default()
        });

        assert_eq!(report.projections.len(), 2);
        let ids = report
            .projections
            .iter()
            .map(|candidate| candidate.candidate_id.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(ids.len(), 2);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn conflicting_package_stable_ids_claiming_one_executable_fail_closed_in_both_orders() {
        let root = temporary_runtime_dir("w22-package-association-conflict");
        let package_root = root.join("shared-root");
        std::fs::create_dir_all(package_root.join("bin")).unwrap();
        std::fs::write(
            package_root.join("bin").join("agent.exe"),
            b"same executable",
        )
        .unwrap();
        let first = WindowsPackageRecord {
            package_family_name: "Conflict.A_fixture".into(),
            package_full_name: "Conflict.A_fixture_1.0.0.0_x64__fixture".into(),
            version: "1.0.0.0".into(),
            installed_location: package_root.clone(),
            executable_relative_path: PathBuf::from("bin").join("agent.exe"),
        };
        let second = WindowsPackageRecord {
            package_family_name: "Conflict.B_fixture".into(),
            package_full_name: "Conflict.B_fixture_1.0.0.0_x64__fixture".into(),
            version: "1.0.0.0".into(),
            installed_location: package_root.clone(),
            executable_relative_path: PathBuf::from("bin").join("agent.exe"),
        };

        let projection_for = |records: Vec<WindowsPackageRecord>| {
            let report =
                discover_windows_passive_report_with_config(&WindowsPassiveDiscoveryConfig {
                    path_env: None,
                    package_records: records,
                    use_real_app_paths: false,
                    use_real_packages: false,
                    use_real_loopback: false,
                    max_results: 8,
                    request_timeout: Duration::from_secs(2),
                    ..WindowsPassiveDiscoveryConfig::default()
                });
            assert_eq!(report.projections.len(), 1);
            report.projections[0].clone()
        };

        let forward = projection_for(vec![first.clone(), second.clone()]);
        let reverse = projection_for(vec![second, first]);
        assert_eq!(forward, reverse);
        assert!(forward.requires_configuration);
        assert_eq!(forward.availability, CandidateAvailability::Unavailable);
        assert_eq!(
            forward.compatibility_state,
            CompatibilityState::Incompatible
        );
        assert!(forward.diagnostics.contains(&DiscoveryDiagnostic {
            source_kind: ObservationSourceKind::WindowsPackage,
            code: DiscoveryDiagnosticCode::InvalidIdentity,
        }));
        let serialized = serde_json::to_string(&forward).unwrap();
        assert!(!serialized.contains("Conflict.A"));
        assert!(!serialized.contains("Conflict.B"));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn association_merge_is_provider_order_independent() {
        #[derive(Clone)]
        struct InlineProvider {
            source_kind: ObservationSourceKind,
            observations: Vec<Observation>,
        }

        impl DiscoveryProvider for InlineProvider {
            fn source_kind(&self) -> ObservationSourceKind {
                self.source_kind
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
                    if !emit(observation.clone()) {
                        break;
                    }
                }
                Ok(())
            }
        }

        let root = temporary_runtime_dir("w22-association-provider-order");
        let package_root = root.join("package");
        let bin_dir = package_root.join("bin");
        std::fs::create_dir_all(&bin_dir).unwrap();
        let executable = bin_dir.join("ordered.exe");
        std::fs::write(&executable, b"provider order independent").unwrap();
        let config = WindowsPassiveWorkerConfig {
            path_env: Some(bin_dir.display().to_string()),
            app_path_records: Vec::new(),
            package_records: vec![WindowsPackageRecord {
                package_family_name: "Order.Package_fixture".into(),
                package_full_name: "Order.Package_fixture_1.0.0.0_x64__fixture".into(),
                version: "1.0.0.0".into(),
                installed_location: package_root,
                executable_relative_path: PathBuf::from("bin").join("ordered.exe"),
            }],
            loopback_records: Vec::new(),
            loopback_recheck_records: None,
            explicit_sources: Vec::new(),
            use_real_app_paths: false,
            use_real_packages: false,
            use_real_loopback: false,
            max_path_entries: 16,
            max_candidates_per_path_entry: 16,
        };
        let deadline = Instant::now() + Duration::from_secs(2);
        let cancelled = AtomicBool::new(false);
        let path_observations = collect_windows_passive_provider(
            ManagedProviderWorkerKind::WindowsPath,
            &config,
            deadline,
            &cancelled,
            8,
        )
        .observations;
        let package_observations = collect_windows_passive_provider(
            ManagedProviderWorkerKind::WindowsPackages,
            &config,
            deadline,
            &cancelled,
            8,
        )
        .observations;

        let projection_for = |providers: Vec<Box<dyn DiscoveryProvider>>| {
            DiscoveryCoordinator::new(providers)
                .discover(&DiscoveryPolicy::default(), &AtomicBool::new(false))
        };
        let forward = projection_for(vec![
            Box::new(InlineProvider {
                source_kind: ObservationSourceKind::WindowsPath,
                observations: path_observations.clone(),
            }),
            Box::new(InlineProvider {
                source_kind: ObservationSourceKind::WindowsPackage,
                observations: package_observations.clone(),
            }),
        ]);
        let reverse = projection_for(vec![
            Box::new(InlineProvider {
                source_kind: ObservationSourceKind::WindowsPackage,
                observations: package_observations,
            }),
            Box::new(InlineProvider {
                source_kind: ObservationSourceKind::WindowsPath,
                observations: path_observations,
            }),
        ]);

        assert_eq!(forward, reverse);
        assert_eq!(forward.len(), 1);
        assert_eq!(
            forward[0].source_kinds,
            vec![
                ObservationSourceKind::WindowsPath,
                ObservationSourceKind::WindowsPackage,
            ]
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn different_packages_with_identical_executable_bytes_do_not_merge() {
        let root = temporary_runtime_dir("w21-package-identity");
        let package_a = root.join("package-a");
        let package_b = root.join("package-b");
        std::fs::create_dir_all(package_a.join("bin")).unwrap();
        std::fs::create_dir_all(package_b.join("bin")).unwrap();
        std::fs::write(
            package_a.join("bin").join("agent.exe"),
            b"same package bytes",
        )
        .unwrap();
        std::fs::write(
            package_b.join("bin").join("agent.exe"),
            b"same package bytes",
        )
        .unwrap();

        let report = discover_windows_passive_report_with_config(&WindowsPassiveDiscoveryConfig {
            path_env: None,
            package_records: vec![
                WindowsPackageRecord {
                    package_family_name: "Example.PackageA_fixture".into(),
                    package_full_name: "Example.PackageA_fixture_1.0.0.0_x64__fixture".into(),
                    version: "1.0.0.0".into(),
                    installed_location: package_a.clone(),
                    executable_relative_path: PathBuf::from("bin").join("agent.exe"),
                },
                WindowsPackageRecord {
                    package_family_name: "Example.PackageB_fixture".into(),
                    package_full_name: "Example.PackageB_fixture_1.0.0.0_x64__fixture".into(),
                    version: "1.0.0.0".into(),
                    installed_location: package_b.clone(),
                    executable_relative_path: PathBuf::from("bin").join("agent.exe"),
                },
            ],
            use_real_app_paths: false,
            use_real_packages: false,
            use_real_loopback: false,
            max_results: 8,
            request_timeout: Duration::from_secs(2),
            ..WindowsPassiveDiscoveryConfig::default()
        });

        assert_eq!(report.projections.len(), 2);
        let ids = report
            .projections
            .iter()
            .map(|candidate| candidate.candidate_id.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(ids.len(), 2);
        let serialized = serde_json::to_string(&report).unwrap();
        assert!(!serialized.contains("Example.PackageA"));
        assert!(!serialized.contains("Example.PackageB"));
        assert!(!serialized.contains(&root.display().to_string()));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn package_stable_identity_is_private_and_not_renderer_projected() {
        let root = temporary_runtime_dir("w21-package-private-id");
        let package_root = root.join("package");
        std::fs::create_dir_all(package_root.join("bin")).unwrap();
        std::fs::write(
            package_root.join("bin").join("agent.exe"),
            b"package private",
        )
        .unwrap();

        let report = discover_windows_passive_report_with_config(&WindowsPassiveDiscoveryConfig {
            path_env: None,
            package_records: vec![WindowsPackageRecord {
                package_family_name: "Private.Package_fixture".into(),
                package_full_name: "Private.Package_fixture_2.0.0.0_x64__fixture".into(),
                version: "2.0.0.0".into(),
                installed_location: package_root.clone(),
                executable_relative_path: PathBuf::from("bin").join("agent.exe"),
            }],
            use_real_app_paths: false,
            use_real_packages: false,
            use_real_loopback: false,
            max_results: 8,
            request_timeout: Duration::from_secs(2),
            ..WindowsPassiveDiscoveryConfig::default()
        });

        assert_eq!(report.projections.len(), 1);
        assert_eq!(
            report.projections[0].source_kinds,
            vec![ObservationSourceKind::WindowsPackage]
        );
        let serialized = serde_json::to_string(&report).unwrap();
        for forbidden in [
            "Private.Package",
            "2.0.0.0",
            "bin",
            "agent.exe:",
            &root.display().to_string(),
        ] {
            assert!(!serialized.contains(forbidden), "leaked {forbidden}");
        }
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn windows_passive_rejects_invalid_sources_with_typed_diagnostics() {
        let root = temporary_runtime_dir("w2-invalid-sources");
        let outside = root.join("outside.exe");
        let package_root = root.join("package");
        std::fs::create_dir_all(&package_root).unwrap();
        std::fs::write(&outside, b"outside").unwrap();

        let report = discover_windows_passive_report_with_config(&WindowsPassiveDiscoveryConfig {
            path_env: None,
            app_path_records: vec![WindowsAppPathRecord {
                key_name: "relative.exe".into(),
                executable_path: PathBuf::from("relative.exe"),
                hive: WindowsRegistryHive::LocalMachine,
                view: WindowsRegistryView::Wow6432,
            }],
            package_records: vec![WindowsPackageRecord {
                package_family_name: "Bad.Package_fixture".into(),
                package_full_name: "Bad.Package_fixture_1.0.0.0_x64__fixture".into(),
                version: "1.0.0.0".into(),
                installed_location: package_root,
                executable_relative_path: PathBuf::from("..").join("outside.exe"),
            }],
            loopback_records: vec![WindowsLoopbackListenerRecord {
                address: "192.168.1.20".into(),
                port: 49153,
                owner_pid: 5252,
                owner_executable: Some(outside.clone()),
                owner_identity: Some("fixture".into()),
            }],
            explicit_sources: vec![ExplicitDiscoverySource::Endpoint(
                "http://192.168.1.20:49153".into(),
            )],
            use_real_app_paths: false,
            use_real_packages: false,
            use_real_loopback: false,
            request_timeout: Duration::from_secs(2),
            ..WindowsPassiveDiscoveryConfig::default()
        });

        assert!(report.projections.is_empty());
        assert!(report.diagnostics.contains(&DiscoveryDiagnostic {
            source_kind: ObservationSourceKind::WindowsAppPath,
            code: DiscoveryDiagnosticCode::InvalidSourceRecord,
        }));
        assert!(report.diagnostics.contains(&DiscoveryDiagnostic {
            source_kind: ObservationSourceKind::WindowsPackage,
            code: DiscoveryDiagnosticCode::InvalidSourceRecord,
        }));
        assert!(report.diagnostics.contains(&DiscoveryDiagnostic {
            source_kind: ObservationSourceKind::LoopbackListener,
            code: DiscoveryDiagnosticCode::NonLoopbackRejected,
        }));
        assert!(report.diagnostics.contains(&DiscoveryDiagnostic {
            source_kind: ObservationSourceKind::UserSelected,
            code: DiscoveryDiagnosticCode::NonLoopbackRejected,
        }));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn windows_passive_max_results_counts_unique_candidates_not_duplicate_observations() {
        let root = temporary_runtime_dir("w2-max-results");
        let path_a = root.join("a");
        let path_b = root.join("b");
        std::fs::create_dir_all(&path_a).unwrap();
        std::fs::create_dir_all(&path_b).unwrap();
        let exe_x = path_a.join("x.exe");
        let exe_y = path_b.join("y.exe");
        std::fs::write(&exe_x, b"x").unwrap();
        std::fs::write(&exe_y, b"y").unwrap();
        let mut path_entries = vec![path_a.clone(); 128];
        path_entries.push(path_b.clone());
        let path_env = std::env::join_paths(path_entries)
            .unwrap()
            .to_string_lossy()
            .into_owned();

        let report = discover_windows_passive_report_with_config(&WindowsPassiveDiscoveryConfig {
            path_env: Some(path_env),
            use_real_app_paths: false,
            use_real_packages: false,
            use_real_loopback: false,
            max_results: 2,
            request_timeout: Duration::from_secs(2),
            ..WindowsPassiveDiscoveryConfig::default()
        });

        assert_eq!(report.projections.len(), 2);
        let names = report
            .projections
            .iter()
            .map(|candidate| candidate.display_name.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            names,
            BTreeSet::from(["x.exe", "y.exe"]),
            "duplicate X observations must not starve later unique Y"
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn windows_passive_path_does_not_recurse_into_child_directories() {
        let root = temporary_runtime_dir("w2-path-no-recursion");
        let path_dir = root.join("bin");
        let child_dir = path_dir.join("child");
        std::fs::create_dir_all(&child_dir).unwrap();
        std::fs::write(path_dir.join("direct.exe"), b"direct").unwrap();
        std::fs::write(child_dir.join("nested.exe"), b"nested").unwrap();

        let report = discover_windows_passive_report_with_config(&WindowsPassiveDiscoveryConfig {
            path_env: Some(path_dir.display().to_string()),
            use_real_app_paths: false,
            use_real_packages: false,
            use_real_loopback: false,
            max_results: 8,
            request_timeout: Duration::from_secs(2),
            ..WindowsPassiveDiscoveryConfig::default()
        });

        let names = report
            .projections
            .iter()
            .map(|candidate| candidate.display_name.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(names, BTreeSet::from(["direct.exe"]));
        let serialized = serde_json::to_string(&report).unwrap();
        assert!(!serialized.contains("child"));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn windows_passive_app_paths_dedupes_hives_and_registry_views() {
        let root = temporary_runtime_dir("w2-app-paths-dedupe");
        let executable = root.join("app-path-agent.exe");
        std::fs::write(&executable, b"app paths").unwrap();

        let records = [
            (
                WindowsRegistryHive::CurrentUser,
                WindowsRegistryView::Native,
            ),
            (
                WindowsRegistryHive::CurrentUser,
                WindowsRegistryView::Wow6432,
            ),
            (
                WindowsRegistryHive::LocalMachine,
                WindowsRegistryView::Native,
            ),
            (
                WindowsRegistryHive::LocalMachine,
                WindowsRegistryView::Wow6432,
            ),
        ]
        .into_iter()
        .map(|(hive, view)| WindowsAppPathRecord {
            key_name: "app-path-agent.exe".into(),
            executable_path: executable.clone(),
            hive,
            view,
        })
        .collect::<Vec<_>>();

        let report = discover_windows_passive_report_with_config(&WindowsPassiveDiscoveryConfig {
            path_env: None,
            app_path_records: records,
            use_real_app_paths: false,
            use_real_packages: false,
            use_real_loopback: false,
            max_results: 8,
            request_timeout: Duration::from_secs(2),
            ..WindowsPassiveDiscoveryConfig::default()
        });

        assert_eq!(report.projections.len(), 1);
        assert_eq!(report.projections[0].display_name, "app-path-agent.exe");
        assert_eq!(
            report.projections[0].source_kinds,
            vec![ObservationSourceKind::WindowsAppPath]
        );
        let serialized = serde_json::to_string(&report).unwrap();
        assert!(!serialized.contains("App Paths"));
        assert!(!serialized.contains(&root.display().to_string()));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn windows_passive_appx_package_manifest_and_containment_are_bounded() {
        let root = temporary_runtime_dir("w2-appx-manifest");
        let package_root = root.join("package");
        std::fs::create_dir_all(package_root.join("bin")).unwrap();
        std::fs::write(
            package_root.join("bin").join("package-agent.exe"),
            b"package",
        )
        .unwrap();
        std::fs::write(
            package_root.join("AppxManifest.xml"),
            br#"
            <Package>
              <Applications>
                <Application Id="App" Executable="bin\package-agent.exe" />
              </Applications>
            </Package>
            "#,
        )
        .unwrap();

        let executables = parse_appx_manifest_executables(&package_root.join("AppxManifest.xml"))
            .expect("manifest executables");
        assert_eq!(executables, vec![PathBuf::from("bin\\package-agent.exe")]);

        let report = discover_windows_passive_report_with_config(&WindowsPassiveDiscoveryConfig {
            path_env: None,
            package_records: vec![WindowsPackageRecord {
                package_family_name: "Package.Family_fixture".into(),
                package_full_name: "Package.Full_1.0.0.0_x64__fixture".into(),
                version: "1.0.0.0".into(),
                installed_location: package_root.clone(),
                executable_relative_path: executables[0].clone(),
            }],
            use_real_app_paths: false,
            use_real_packages: false,
            use_real_loopback: false,
            max_results: 8,
            request_timeout: Duration::from_secs(2),
            ..WindowsPassiveDiscoveryConfig::default()
        });

        assert_eq!(report.projections.len(), 1);
        assert_eq!(report.projections[0].display_name, "package-agent.exe");
        assert_eq!(
            report.projections[0].source_kinds,
            vec![ObservationSourceKind::WindowsPackage]
        );
        assert!(report.projections[0].requires_configuration);
        let serialized = serde_json::to_string(&report).unwrap();
        assert!(!serialized.contains("Package.Family"));
        assert!(!serialized.contains(&package_root.display().to_string()));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn valid_namespaced_application_executable_is_accepted() {
        let root = temporary_runtime_dir("w21-appx-namespaced");
        let manifest = root.join("AppxManifest.xml");
        std::fs::write(
            &manifest,
            br#"
            <pkg:Package xmlns:pkg="urn:fixture">
              <pkg:Applications>
                <pkg:Application pkg:Id="App" pkg:Executable="bin\namespaced.exe" />
              </pkg:Applications>
            </pkg:Package>
            "#,
        )
        .unwrap();

        let executables = parse_appx_manifest_executables(&manifest).unwrap();
        assert_eq!(executables, vec![PathBuf::from("bin\\namespaced.exe")]);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn suffix_spoofed_application_and_executable_are_ignored() {
        let root = temporary_runtime_dir("w21-appx-suffix-spoof");
        let manifest = root.join("AppxManifest.xml");
        std::fs::write(
            &manifest,
            br#"
            <Package>
              <Applications>
                <NotApplication Executable="bin\wrong-element.exe" />
                <FakeApplication Executable="bin\wrong-fake-element.exe" />
                <Application FakeExecutable="bin\wrong-attr.exe" ExecutableSuffix="bin\wrong-suffix.exe" />
              </Applications>
            </Package>
            "#,
        )
        .unwrap();

        let executables = parse_appx_manifest_executables(&manifest).unwrap();
        assert!(
            executables.is_empty(),
            "suffix-spoofed Application/Executable names must not be accepted"
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn malformed_or_oversized_manifest_fails_closed() {
        let root = temporary_runtime_dir("w21-appx-malformed-oversized");
        let malformed = root.join("malformed.xml");
        let oversized = root.join("oversized.xml");
        std::fs::write(&malformed, b"<Package><Applications><Application").unwrap();
        std::fs::write(&oversized, vec![b'a'; MAX_PACKAGE_MANIFEST_BYTES + 1]).unwrap();

        assert!(matches!(
            parse_appx_manifest_executables(&malformed),
            Err(DiscoveryDiagnosticCode::InvalidSourceRecord)
        ));
        assert!(matches!(
            parse_appx_manifest_executables(&oversized),
            Err(DiscoveryDiagnosticCode::OversizedInput)
        ));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn traversal_and_reparse_escape_remain_rejected() {
        let root = temporary_runtime_dir("w21-appx-containment");
        let package_root = root.join("package");
        std::fs::create_dir_all(&package_root).unwrap();
        let traversal = WindowsPackageRecord {
            package_family_name: "Traversal.Package_fixture".into(),
            package_full_name: "Traversal.Package_fixture_1.0.0.0_x64__fixture".into(),
            version: "1.0.0.0".into(),
            installed_location: package_root,
            executable_relative_path: PathBuf::from("..").join("escape.exe"),
        };

        assert!(matches!(
            package_executable_path(&traversal),
            Err(DiscoveryDiagnosticCode::InvalidSourceRecord)
        ));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn windows_passive_loopback_accepts_loopback_only_and_dedupes_ports() {
        let root = temporary_runtime_dir("w2-loopback");
        let executable = root.join("listener-agent.exe");
        std::fs::write(&executable, b"listener").unwrap();
        let records = vec![
            WindowsLoopbackListenerRecord {
                address: "127.0.0.1".into(),
                port: 41001,
                owner_pid: 1111,
                owner_executable: Some(executable.clone()),
                owner_identity: Some("first".into()),
            },
            WindowsLoopbackListenerRecord {
                address: "127.12.34.56".into(),
                port: 41002,
                owner_pid: 2222,
                owner_executable: Some(executable.clone()),
                owner_identity: Some("second".into()),
            },
            WindowsLoopbackListenerRecord {
                address: "::1".into(),
                port: 41003,
                owner_pid: 3333,
                owner_executable: Some(executable.clone()),
                owner_identity: Some("third".into()),
            },
        ];

        let report = discover_windows_passive_report_with_config(&WindowsPassiveDiscoveryConfig {
            path_env: None,
            loopback_records: records,
            use_real_app_paths: false,
            use_real_packages: false,
            use_real_loopback: false,
            max_results: 8,
            request_timeout: Duration::from_secs(2),
            ..WindowsPassiveDiscoveryConfig::default()
        });

        assert_eq!(report.projections.len(), 1);
        assert_eq!(report.projections[0].display_name, "Loopback listener");
        assert_eq!(
            report.projections[0].source_kinds,
            vec![ObservationSourceKind::LoopbackListener]
        );
        let serialized = serde_json::to_string(&report).unwrap();
        for forbidden in [
            "listener-agent.exe",
            "41001",
            "41002",
            "41003",
            "1111",
            "2222",
            "3333",
        ] {
            assert!(!serialized.contains(forbidden));
        }
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn listener_disappears_before_owner_recheck_fails_closed() {
        let root = temporary_runtime_dir("w21-loopback-disappears");
        let executable = root.join("listener.exe");
        std::fs::write(&executable, b"listener").unwrap();

        let report = discover_windows_passive_report_with_config(&WindowsPassiveDiscoveryConfig {
            path_env: None,
            loopback_records: vec![WindowsLoopbackListenerRecord {
                address: "127.0.0.1".into(),
                port: 42001,
                owner_pid: 9001,
                owner_executable: Some(executable),
                owner_identity: Some("creation-a".into()),
            }],
            loopback_recheck_records: Some(Vec::new()),
            use_real_app_paths: false,
            use_real_packages: false,
            use_real_loopback: false,
            max_results: 8,
            request_timeout: Duration::from_secs(2),
            ..WindowsPassiveDiscoveryConfig::default()
        });

        assert!(report.projections.is_empty());
        assert!(report.diagnostics.contains(&DiscoveryDiagnostic {
            source_kind: ObservationSourceKind::LoopbackListener,
            code: DiscoveryDiagnosticCode::SourceDisappeared,
        }));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn reused_pid_with_different_creation_identity_fails_closed() {
        let root = temporary_runtime_dir("w21-loopback-pid-reuse");
        let executable = root.join("listener.exe");
        std::fs::write(&executable, b"listener").unwrap();

        let report = discover_windows_passive_report_with_config(&WindowsPassiveDiscoveryConfig {
            path_env: None,
            loopback_records: vec![WindowsLoopbackListenerRecord {
                address: "127.0.0.1".into(),
                port: 42002,
                owner_pid: 9002,
                owner_executable: Some(executable.clone()),
                owner_identity: Some("creation-a".into()),
            }],
            loopback_recheck_records: Some(vec![WindowsLoopbackListenerRecord {
                address: "127.0.0.1".into(),
                port: 42002,
                owner_pid: 9002,
                owner_executable: Some(executable),
                owner_identity: Some("creation-b".into()),
            }]),
            use_real_app_paths: false,
            use_real_packages: false,
            use_real_loopback: false,
            max_results: 8,
            request_timeout: Duration::from_secs(2),
            ..WindowsPassiveDiscoveryConfig::default()
        });

        assert!(report.projections.is_empty());
        assert!(report.diagnostics.contains(&DiscoveryDiagnostic {
            source_kind: ObservationSourceKind::LoopbackListener,
            code: DiscoveryDiagnosticCode::SourceDisappeared,
        }));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn listener_owner_changes_between_snapshots_fails_closed() {
        let root = temporary_runtime_dir("w21-loopback-owner-change");
        let first = root.join("first.exe");
        let second = root.join("second.exe");
        std::fs::write(&first, b"first").unwrap();
        std::fs::write(&second, b"second").unwrap();

        let report = discover_windows_passive_report_with_config(&WindowsPassiveDiscoveryConfig {
            path_env: None,
            loopback_records: vec![WindowsLoopbackListenerRecord {
                address: "::1".into(),
                port: 42003,
                owner_pid: 9003,
                owner_executable: Some(first),
                owner_identity: Some("creation-a".into()),
            }],
            loopback_recheck_records: Some(vec![WindowsLoopbackListenerRecord {
                address: "::1".into(),
                port: 42003,
                owner_pid: 9004,
                owner_executable: Some(second),
                owner_identity: Some("creation-b".into()),
            }]),
            use_real_app_paths: false,
            use_real_packages: false,
            use_real_loopback: false,
            max_results: 8,
            request_timeout: Duration::from_secs(2),
            ..WindowsPassiveDiscoveryConfig::default()
        });

        assert!(report.projections.is_empty());
        assert!(report.diagnostics.contains(&DiscoveryDiagnostic {
            source_kind: ObservationSourceKind::LoopbackListener,
            code: DiscoveryDiagnosticCode::SourceDisappeared,
        }));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn stable_ipv4_listener_owner_is_accepted() {
        let root = temporary_runtime_dir("w21-loopback-stable-v4");
        let executable = root.join("listener-v4.exe");
        std::fs::write(&executable, b"listener v4").unwrap();

        let report = discover_windows_passive_report_with_config(&WindowsPassiveDiscoveryConfig {
            path_env: None,
            loopback_records: vec![WindowsLoopbackListenerRecord {
                address: "127.0.0.2".into(),
                port: 42004,
                owner_pid: 9004,
                owner_executable: Some(executable.clone()),
                owner_identity: Some("creation-v4".into()),
            }],
            loopback_recheck_records: Some(vec![WindowsLoopbackListenerRecord {
                address: "127.0.0.2".into(),
                port: 42004,
                owner_pid: 9004,
                owner_executable: Some(executable),
                owner_identity: Some("creation-v4".into()),
            }]),
            use_real_app_paths: false,
            use_real_packages: false,
            use_real_loopback: false,
            max_results: 8,
            request_timeout: Duration::from_secs(2),
            ..WindowsPassiveDiscoveryConfig::default()
        });

        assert_eq!(report.projections.len(), 1);
        assert_eq!(
            report.projections[0].source_kinds,
            vec![ObservationSourceKind::LoopbackListener]
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn stable_ipv6_listener_owner_is_accepted() {
        let root = temporary_runtime_dir("w21-loopback-stable-v6");
        let executable = root.join("listener-v6.exe");
        std::fs::write(&executable, b"listener v6").unwrap();

        let report = discover_windows_passive_report_with_config(&WindowsPassiveDiscoveryConfig {
            path_env: None,
            loopback_records: vec![WindowsLoopbackListenerRecord {
                address: "::1".into(),
                port: 42005,
                owner_pid: 9005,
                owner_executable: Some(executable.clone()),
                owner_identity: Some("creation-v6".into()),
            }],
            loopback_recheck_records: Some(vec![WindowsLoopbackListenerRecord {
                address: "::1".into(),
                port: 42005,
                owner_pid: 9005,
                owner_executable: Some(executable),
                owner_identity: Some("creation-v6".into()),
            }]),
            use_real_app_paths: false,
            use_real_packages: false,
            use_real_loopback: false,
            max_results: 8,
            request_timeout: Duration::from_secs(2),
            ..WindowsPassiveDiscoveryConfig::default()
        });

        assert_eq!(report.projections.len(), 1);
        assert_eq!(
            report.projections[0].source_kinds,
            vec![ObservationSourceKind::LoopbackListener]
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn owner_identity_pid_port_and_path_are_not_projected() {
        let root = temporary_runtime_dir("w21-loopback-private-owner");
        let executable = root.join("private-listener.exe");
        std::fs::write(&executable, b"private listener").unwrap();

        let report = discover_windows_passive_report_with_config(&WindowsPassiveDiscoveryConfig {
            path_env: None,
            loopback_records: vec![WindowsLoopbackListenerRecord {
                address: "127.0.0.1".into(),
                port: 42006,
                owner_pid: 9006,
                owner_executable: Some(executable.clone()),
                owner_identity: Some("private-owner-creation".into()),
            }],
            loopback_recheck_records: Some(vec![WindowsLoopbackListenerRecord {
                address: "127.0.0.1".into(),
                port: 42006,
                owner_pid: 9006,
                owner_executable: Some(executable),
                owner_identity: Some("private-owner-creation".into()),
            }]),
            use_real_app_paths: false,
            use_real_packages: false,
            use_real_loopback: false,
            max_results: 8,
            request_timeout: Duration::from_secs(2),
            ..WindowsPassiveDiscoveryConfig::default()
        });

        assert_eq!(report.projections.len(), 1);
        let serialized = serde_json::to_string(&report).unwrap();
        let value: serde_json::Value = serde_json::from_str(&serialized).unwrap();
        let rendered = value.to_string();
        let root_text = root.display().to_string();
        for forbidden in [
            root_text.as_str(),
            "private-listener.exe",
            "private-owner-creation",
            "ownerPid",
            "ownerIdentity",
            "ownerExecutable",
            "locator",
            "port",
            "endpoint",
        ] {
            assert!(!rendered.contains(forbidden), "leaked {forbidden}");
        }
        for forbidden in ["42006", "9006"] {
            assert!(
                !rendered.contains(&format!(":{forbidden}"))
                    && !rendered.contains(&format!("\"{forbidden}\"")),
                "leaked structured numeric value {forbidden}"
            );
        }
        for projection in &report.projections {
            assert!(!projection.display_name.contains("private-owner-creation"));
            assert!(!projection.display_name.contains("42006"));
            assert!(!projection.display_name.contains("9006"));
            assert!(!projection
                .display_name
                .contains(&root.display().to_string()));
        }
        for diagnostic in &report.diagnostics {
            let diagnostic_json = serde_json::to_string(diagnostic).unwrap();
            for forbidden in [
                root_text.as_str(),
                "private-listener.exe",
                "private-owner-creation",
                "ownerPid",
                "ownerIdentity",
                "ownerExecutable",
                "locator",
                "port",
                "endpoint",
            ] {
                assert!(
                    !diagnostic_json.contains(forbidden),
                    "diagnostic leaked {forbidden}"
                );
            }
            assert!(!diagnostic_json.contains("42006"));
            assert!(!diagnostic_json.contains("9006"));
        }
        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(windows)]
    #[test]
    fn real_loopback_verification_none_emits_source_local_diagnostic_v4() {
        let mut collection = WindowsProviderCollection::default();
        let mut records = Vec::new();
        push_loopback_verification_result(&mut collection, &mut records, Ok(None));

        assert!(records.is_empty());
        assert_eq!(
            collection.diagnostics,
            vec![DiscoveryDiagnostic {
                source_kind: ObservationSourceKind::LoopbackListener,
                code: DiscoveryDiagnosticCode::SourceDisappeared,
            }]
        );
    }

    #[cfg(windows)]
    #[test]
    fn real_loopback_verification_none_emits_source_local_diagnostic_v6() {
        let mut collection = WindowsProviderCollection::default();
        let mut records = Vec::new();
        push_loopback_verification_result(&mut collection, &mut records, Ok(None));

        assert!(records.is_empty());
        assert_eq!(
            collection.diagnostics,
            vec![DiscoveryDiagnostic {
                source_kind: ObservationSourceKind::LoopbackListener,
                code: DiscoveryDiagnosticCode::SourceDisappeared,
            }]
        );
    }

    #[cfg(windows)]
    #[test]
    fn fixture_and_real_recheck_failure_have_equivalent_safe_projection() {
        let root = temporary_runtime_dir("w22-loopback-fixture-real-equivalence");
        let executable = root.join("listener.exe");
        std::fs::write(&executable, b"listener").unwrap();
        let fixture_report =
            discover_windows_passive_report_with_config(&WindowsPassiveDiscoveryConfig {
                path_env: None,
                loopback_records: vec![WindowsLoopbackListenerRecord {
                    address: "127.0.0.1".into(),
                    port: 49221,
                    owner_pid: 9221,
                    owner_executable: Some(executable),
                    owner_identity: Some("owner-before".into()),
                }],
                loopback_recheck_records: Some(Vec::new()),
                use_real_app_paths: false,
                use_real_packages: false,
                use_real_loopback: false,
                max_results: 8,
                request_timeout: Duration::from_secs(2),
                ..WindowsPassiveDiscoveryConfig::default()
            });
        let mut real_collection = WindowsProviderCollection::default();
        let mut records = Vec::new();
        push_loopback_verification_result(&mut real_collection, &mut records, Ok(None));

        assert_eq!(fixture_report.projections.len(), 0);
        assert_eq!(records.len(), 0);
        assert_eq!(fixture_report.diagnostics, real_collection.diagnostics);
        let serialized = serde_json::to_string(&fixture_report).unwrap();
        for forbidden in [&root.display().to_string(), "49221", "9221", "owner-before"] {
            assert!(!serialized.contains(forbidden), "leaked {forbidden}");
        }
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn one_disappearing_listener_does_not_drop_other_valid_listener() {
        let root = temporary_runtime_dir("w22-loopback-one-disappears");
        let first = root.join("first.exe");
        let second = root.join("second.exe");
        std::fs::write(&first, b"first").unwrap();
        std::fs::write(&second, b"second").unwrap();

        let config = WindowsPassiveWorkerConfig {
            path_env: None,
            app_path_records: Vec::new(),
            package_records: Vec::new(),
            loopback_records: vec![
                WindowsLoopbackListenerRecord {
                    address: "127.0.0.1".into(),
                    port: 49222,
                    owner_pid: 9222,
                    owner_executable: Some(first),
                    owner_identity: Some("gone-owner".into()),
                },
                WindowsLoopbackListenerRecord {
                    address: "::1".into(),
                    port: 49223,
                    owner_pid: 9223,
                    owner_executable: Some(second.clone()),
                    owner_identity: Some("stable-owner".into()),
                },
            ],
            loopback_recheck_records: Some(vec![WindowsLoopbackListenerRecord {
                address: "::1".into(),
                port: 49223,
                owner_pid: 9223,
                owner_executable: Some(second),
                owner_identity: Some("stable-owner".into()),
            }]),
            explicit_sources: Vec::new(),
            use_real_app_paths: false,
            use_real_packages: false,
            use_real_loopback: false,
            max_path_entries: 16,
            max_candidates_per_path_entry: 16,
        };

        let collection = collect_windows_passive_provider(
            ManagedProviderWorkerKind::WindowsLoopbackListeners,
            &config,
            Instant::now() + Duration::from_secs(2),
            &AtomicBool::new(false),
            8,
        );

        assert_eq!(collection.observations.len(), 1);
        assert_eq!(
            collection.observations[0].source_kind,
            ObservationSourceKind::LoopbackListener
        );
        assert!(collection.diagnostics.contains(&DiscoveryDiagnostic {
            source_kind: ObservationSourceKind::LoopbackListener,
            code: DiscoveryDiagnosticCode::SourceDisappeared,
        }));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn windows_passive_explicit_endpoint_is_loopback_only_and_not_verified() {
        let loopback =
            discover_windows_passive_report_with_config(&WindowsPassiveDiscoveryConfig {
                path_env: None,
                explicit_sources: vec![ExplicitDiscoverySource::Endpoint(
                    "http://127.0.0.1:47777".into(),
                )],
                use_real_app_paths: false,
                use_real_packages: false,
                use_real_loopback: false,
                max_results: 8,
                request_timeout: Duration::from_secs(2),
                ..WindowsPassiveDiscoveryConfig::default()
            });
        assert_eq!(loopback.projections.len(), 1);
        assert_eq!(loopback.projections[0].display_name, "Loopback endpoint");
        assert_eq!(
            loopback.projections[0].compatibility_state,
            CompatibilityState::NotVerified
        );
        assert!(loopback.projections[0].requires_configuration);
        let serialized = serde_json::to_string(&loopback).unwrap();
        assert!(!serialized.contains("47777"));

        let non_loopback =
            discover_windows_passive_report_with_config(&WindowsPassiveDiscoveryConfig {
                path_env: None,
                explicit_sources: vec![ExplicitDiscoverySource::Endpoint(
                    "http://192.168.1.25:47777".into(),
                )],
                use_real_app_paths: false,
                use_real_packages: false,
                use_real_loopback: false,
                max_results: 8,
                request_timeout: Duration::from_secs(2),
                ..WindowsPassiveDiscoveryConfig::default()
            });
        assert!(non_loopback.projections.is_empty());
        assert!(non_loopback.diagnostics.contains(&DiscoveryDiagnostic {
            source_kind: ObservationSourceKind::UserSelected,
            code: DiscoveryDiagnosticCode::NonLoopbackRejected,
        }));
    }

    #[test]
    fn explicit_loopback_endpoint_strictly_validates_authority_and_origin() {
        for accepted in [
            (
                "HTTP://LOCALHOST:48000/path?q=1#frag",
                "http://localhost:48000",
            ),
            ("https://127.12.0.1:443/card", "https://127.12.0.1:443"),
            ("http://[::1]:48001/agent", "http://[::1]:48001"),
        ] {
            assert_eq!(
                normalize_explicit_loopback_endpoint(accepted.0).unwrap(),
                accepted.1
            );
        }

        for rejected in [
            "http://localhost",
            "http://localhost:",
            "http://localhost:0",
            "http://localhost:65536",
            "http://localhost:not-a-port",
            "http://localhost:48000:extra",
            "http://user@localhost:48000",
            "ftp://localhost:48000",
            "http://example.local:48000",
            "http://192.168.1.10:48000",
            "http://localhost:48000/\u{202e}",
            "http://localhost:48000/\n",
        ] {
            assert!(
                normalize_explicit_loopback_endpoint(rejected).is_err(),
                "endpoint should be rejected: {rejected:?}"
            );
        }
    }

    #[test]
    fn windows_passive_sensitive_file_name_fails_closed_without_leaking() {
        let root = temporary_runtime_dir("w2-sensitive-name");
        let path_dir = root.join("bin");
        std::fs::create_dir_all(&path_dir).unwrap();
        std::fs::write(
            path_dir.join("authorization-cookie-runtime_token.exe"),
            b"sensitive name",
        )
        .unwrap();

        let report = discover_windows_passive_report_with_config(&WindowsPassiveDiscoveryConfig {
            path_env: Some(path_dir.display().to_string()),
            use_real_app_paths: false,
            use_real_packages: false,
            use_real_loopback: false,
            max_results: 8,
            request_timeout: Duration::from_secs(2),
            ..WindowsPassiveDiscoveryConfig::default()
        });

        assert_eq!(report.projections.len(), 1);
        let candidate = &report.projections[0];
        assert_eq!(candidate.display_name, "Local Agent");
        assert_eq!(candidate.availability, CandidateAvailability::Unavailable);
        assert!(candidate.requires_configuration);
        assert!(candidate.diagnostics.contains(&DiscoveryDiagnostic {
            source_kind: ObservationSourceKind::WindowsPath,
            code: DiscoveryDiagnosticCode::InvalidIdentity,
        }));
        let serialized = serde_json::to_string(&report).unwrap();
        assert!(!serialized.contains("authorization"));
        assert!(!serialized.contains("cookie"));
        assert!(!serialized.contains("runtime_token"));
        assert!(!serialized.contains(&root.display().to_string()));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn stable_file_identity_changes_when_large_file_middle_byte_changes() {
        let root = temporary_runtime_dir("local-discovery-file-middle-fingerprint");
        let executable = root.join("agent.exe");
        let mut contents = vec![0x11; 1_200_000];
        contents[600_000] = 0x22;
        std::fs::write(&executable, &contents).unwrap();
        let first = stable_file_identity(&executable).expect("first identity");
        contents[600_000] = 0x33;
        std::fs::write(&executable, &contents).unwrap();
        let changed_middle = stable_file_identity(&executable).expect("changed identity");

        assert_ne!(first, changed_middle);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn stable_file_identity_succeeds_for_stable_complete_file() {
        let root = temporary_runtime_dir("local-discovery-stable-file-fingerprint");
        let executable = root.join("agent.exe");
        std::fs::write(&executable, vec![0x51; 1_000_000]).unwrap();

        let first = stable_file_identity(&executable).expect("first identity");
        let second = stable_file_identity(&executable).expect("second identity");

        assert_eq!(first, second);
        assert!(!first.is_empty());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn stable_file_identity_detects_same_length_replacement_during_snapshot() {
        let root = temporary_runtime_dir("local-discovery-file-replacement");
        let executable = root.join("agent.exe");
        let original = vec![0x41; 12_000_000];
        let replacement = vec![0x42; 12_000_000];
        std::fs::write(&executable, &original).unwrap();
        let path_for_replacement = executable.clone();
        let mut attempted = false;
        let mut write_result = None;
        let result = stable_file_identity_with_read_hook(
            &executable,
            Instant::now() + Duration::from_secs(2),
            &AtomicBool::new(false),
            &mut |read| {
                if read > 0 && !attempted {
                    write_result = Some(std::fs::write(&path_for_replacement, &replacement));
                    attempted = true;
                }
            },
        );
        assert!(attempted);
        #[cfg(windows)]
        {
            assert!(write_result.expect("write attempted").is_err());
            assert!(result.is_ok());
        }
        #[cfg(not(windows))]
        {
            assert!(write_result.expect("write attempted").is_ok());
            assert!(matches!(
                result,
                Err(DiscoveryDiagnosticCode::FingerprintChanged)
            ));
        }
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn stable_file_identity_detects_truncate_and_growth_during_snapshot() {
        for (name, replacement_len) in [("truncate", 1_000_000usize), ("growth", 14_000_000usize)] {
            let root = temporary_runtime_dir(&format!("local-discovery-file-{name}"));
            let executable = root.join("agent.exe");
            std::fs::write(&executable, vec![0x61; 12_000_000]).unwrap();
            let path_for_replacement = executable.clone();
            let mut attempted = false;
            let mut write_result = None;

            let result = stable_file_identity_with_read_hook(
                &executable,
                Instant::now() + Duration::from_secs(2),
                &AtomicBool::new(false),
                &mut |read| {
                    if read > 0 && !attempted {
                        write_result = Some(std::fs::write(
                            &path_for_replacement,
                            vec![0x62; replacement_len],
                        ));
                        attempted = true;
                    }
                },
            );

            assert!(attempted);
            #[cfg(windows)]
            {
                assert!(
                    write_result.expect("write attempted").is_err(),
                    "{name} write must be denied by snapshot sharing mode"
                );
                assert!(result.is_ok(), "{name} denied write leaves snapshot stable");
            }
            #[cfg(not(windows))]
            {
                assert!(write_result.expect("write attempted").is_ok());
                assert!(
                    matches!(result, Err(DiscoveryDiagnosticCode::FingerprintChanged)),
                    "{name} must fail closed, got {result:?}"
                );
            }
            let _ = std::fs::remove_dir_all(root);
        }
    }

    #[test]
    fn stable_file_identity_respects_deadline_and_cancel() {
        let root = temporary_runtime_dir("local-discovery-file-timeout-cancel");
        let executable = root.join("agent.exe");
        std::fs::write(&executable, vec![0x71; 1_000_000]).unwrap();

        let cancelled = AtomicBool::new(false);
        let deadline_result =
            stable_file_identity_with_deadline(&executable, Instant::now(), &cancelled);
        assert!(matches!(
            deadline_result,
            Err(DiscoveryDiagnosticCode::ProviderTimeout)
        ));

        cancelled.store(true, Ordering::Release);
        let cancel_result = stable_file_identity_with_deadline(
            &executable,
            Instant::now() + Duration::from_secs(1),
            &cancelled,
        );
        assert!(matches!(
            cancel_result,
            Err(DiscoveryDiagnosticCode::ProviderTimeout)
        ));
        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(windows)]
    #[test]
    fn stable_file_identity_snapshot_rejects_concurrent_same_length_write_with_restored_mtime() {
        let root = temporary_runtime_dir("local-discovery-file-concurrent-mtime-write");
        let executable = root.join("agent.exe");
        let original = vec![0x41; 12_000_000];
        let replacement = vec![0x42; 12_000_000];
        std::fs::write(&executable, &original).unwrap();
        let original_mtime = std::fs::metadata(&executable).unwrap().modified().unwrap();
        let path_for_write = executable.clone();
        let mut attempted = false;
        let mut write_result = None;

        let result = stable_file_identity_with_read_hook(
            &executable,
            Instant::now() + Duration::from_secs(2),
            &AtomicBool::new(false),
            &mut |read| {
                if read > 0 && !attempted {
                    attempted = true;
                    write_result = Some(
                        std::fs::OpenOptions::new()
                            .write(true)
                            .open(&path_for_write)
                            .and_then(|mut file| {
                                use std::io::Write;
                                file.write_all(&replacement)?;
                                file.flush()
                            }),
                    );
                    if write_result.as_ref().is_some_and(Result::is_ok) {
                        restore_last_write_time(&path_for_write, original_mtime);
                    }
                }
            },
        );

        assert!(attempted);
        assert!(
            write_result.expect("write attempted").is_err(),
            "fingerprint snapshot handle must deny concurrent write access"
        );
        assert!(
            result.is_ok(),
            "unchanged snapshot must still hash successfully"
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(windows)]
    #[test]
    fn stable_file_identity_snapshot_rejects_concurrent_replace_and_releases_handle_afterwards() {
        let root = temporary_runtime_dir("local-discovery-file-concurrent-replace");
        let executable = root.join("agent.exe");
        let replacement_path = root.join("replacement.exe");
        std::fs::write(&executable, vec![0x31; 12_000_000]).unwrap();
        std::fs::write(&replacement_path, vec![0x32; 12_000_000]).unwrap();
        let path_for_replace = executable.clone();
        let replacement_for_replace = replacement_path.clone();
        let mut attempted = false;
        let mut replace_result = None;

        let result = stable_file_identity_with_read_hook(
            &executable,
            Instant::now() + Duration::from_secs(2),
            &AtomicBool::new(false),
            &mut |read| {
                if read > 0 && !attempted {
                    attempted = true;
                    replace_result =
                        Some(std::fs::rename(&replacement_for_replace, &path_for_replace));
                }
            },
        );

        assert!(attempted);
        assert!(
            replace_result.expect("replace attempted").is_err(),
            "snapshot handle must deny concurrent replace/delete access"
        );
        assert!(result.is_ok());
        std::fs::write(&executable, vec![0x33; 12_000_000]).unwrap();
        std::fs::write(&replacement_path, vec![0x34; 12_000_000]).unwrap();
        std::fs::remove_file(&executable).unwrap();
        std::fs::rename(&replacement_path, &executable).unwrap();
        assert!(stable_file_identity(&executable).is_ok());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn stable_file_identity_from_reader_streams_complete_content() {
        struct CountingReader {
            remaining: usize,
            read_total: Arc<Mutex<usize>>,
        }

        impl std::io::Read for CountingReader {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                if self.remaining == 0 {
                    return Ok(0);
                }
                let take = self.remaining.min(buf.len()).min(8192);
                buf[..take].fill(0x5a);
                self.remaining -= take;
                *self.read_total.lock().unwrap() += take;
                Ok(take)
            }
        }

        let read_total = Arc::new(Mutex::new(0usize));
        let reader = CountingReader {
            remaining: 2 * 1024 * 1024,
            read_total: Arc::clone(&read_total),
        };
        let fingerprint = super::stable_file_identity_from_reader(
            reader,
            2 * 1024 * 1024,
            0,
            64 * 1024,
            std::time::Duration::from_secs(1),
        );

        assert!(!fingerprint.is_empty());
        assert_eq!(*read_total.lock().unwrap(), 2 * 1024 * 1024);
    }

    #[test]
    fn stable_file_identity_ignores_mtime_and_covers_tail_changes() {
        let unchanged_content_old_mtime = super::stable_file_identity_from_reader(
            b"same-content".as_slice(),
            12,
            1,
            64,
            std::time::Duration::from_secs(1),
        );
        let unchanged_content_new_mtime = super::stable_file_identity_from_reader(
            b"same-content".as_slice(),
            12,
            2,
            64,
            std::time::Duration::from_secs(1),
        );
        let first = b"same-prefix-tail-a".as_slice();
        let second = b"same-prefix-tail-b".as_slice();
        let changed_tail_a = super::stable_file_identity_from_reader(
            first,
            first.len() as u64,
            1,
            64,
            std::time::Duration::from_secs(1),
        );
        let changed_tail_b = super::stable_file_identity_from_reader(
            second,
            second.len() as u64,
            1,
            64,
            std::time::Duration::from_secs(1),
        );

        assert_eq!(unchanged_content_old_mtime, unchanged_content_new_mtime);
        assert_ne!(changed_tail_a, changed_tail_b);
    }

    #[test]
    fn stable_file_identity_for_large_file_includes_tail_without_mtime() {
        let root = temporary_runtime_dir("local-discovery-file-fingerprint");
        let executable = root.join("agent.exe");
        let mut contents = vec![0x11; 1_200_000];
        contents.extend_from_slice(b"tail-a");
        std::fs::write(&executable, &contents).unwrap();
        let first = stable_file_identity(&executable).expect("first identity");
        std::fs::write(&executable, &contents).unwrap();
        let same_contents = stable_file_identity(&executable).expect("same identity");
        contents.truncate(1_200_000);
        contents.extend_from_slice(b"tail-b");
        std::fs::write(&executable, &contents).unwrap();
        let changed_tail = stable_file_identity(&executable).expect("changed identity");

        assert_eq!(first, same_contents);
        assert_ne!(first, changed_tail);
        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(windows)]
    fn restore_last_write_time(path: &std::path::Path, time: std::time::SystemTime) {
        use std::time::UNIX_EPOCH;
        let escaped = path.display().to_string().replace('\'', "''");
        let millis = time.duration_since(UNIX_EPOCH).unwrap().as_millis();
        let script = format!(
            "$p = '{escaped}'; (Get-Item -LiteralPath $p).LastWriteTimeUtc = [DateTimeOffset]::FromUnixTimeMilliseconds({millis}).UtcDateTime"
        );
        let output = std::process::Command::new("powershell.exe")
            .args(["-NoProfile", "-Command", &script])
            .output()
            .expect("restore last write time");
        assert!(
            output.status.success(),
            "restore last write time failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[cfg(not(windows))]
    fn restore_last_write_time(_path: &std::path::Path, _time: std::time::SystemTime) {}
}
