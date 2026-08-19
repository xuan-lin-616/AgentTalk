use agenttalk_domain::{
    AuthState, CandidateAvailability, CandidateProjection, CompatibilityState, DiscoveryDiagnostic,
    DiscoveryDiagnosticCode, DiscoveryState, HealthState, ObservationSourceKind,
};
#[cfg(test)]
use agenttalk_domain::{CandidateCategory, ObservationTrustLevel, VerificationAuthority};
use agenttalk_events::RuntimeEvent;
use agenttalk_runtime_host::{
    bundled_production_catalog, default_local_manifest_directory,
    discover_windows_passive_report_with_config_and_cancelled, load_catalog_for_scan,
    load_local_manifest_directory, AcpCompatibilityReport, AcpDiscoverySession,
    AcpImportPlanMetadata, AcpVerificationConsent, AcpVerificationDiagnosticCode,
    AcpVerificationResult, AcpVerificationStatus, CatalogSnapshot, ExplicitDiscoverySource,
    NetworkCounter, WindowsPassiveDiscoveryConfig,
};
#[cfg(test)]
use agenttalk_runtime_host::{AdapterManifest, CatalogLoadReport, ManifestLaunch};
use getrandom::fill as fill_random;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const DEFAULT_SCAN_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_VERIFY_TIMEOUT: Duration = Duration::from_secs(5);
const SESSION_LIFETIME: Duration = Duration::from_secs(10 * 60);
const DISCOVERY_MAX_SESSIONS_PER_OWNER: usize = 16;
const DISCOVERY_MAX_SESSIONS_GLOBAL: usize = 256;
const DISCOVERY_MAX_RUNNING_SCANS_PER_OWNER: usize = 2;
const DISCOVERY_MAX_RUNNING_SCANS_GLOBAL: usize = 32;
// W5.8 in-memory receipt bounds. A session holds at most 32 candidates, so
// 128 receipts cover a start plus several verify/dismiss receipts per
// candidate with headroom; owner/global bounds keep the retained map bounded
// across the 10-minute session lifetime.
const DISCOVERY_MAX_RECEIPTS_PER_SESSION: usize = 128;
const DISCOVERY_MAX_RECEIPTS_PER_OWNER: usize = 512;
const DISCOVERY_MAX_RECEIPTS_GLOBAL: usize = 2048;
// W5.8 running verification bounds. Each verification owns an ACP child
// process, so per-owner/global ceilings are deliberately small.
const DISCOVERY_MAX_RUNNING_VERIFICATIONS_PER_OWNER: usize = 4;
const DISCOVERY_MAX_RUNNING_VERIFICATIONS_GLOBAL: usize = 32;
// W5.8 import-plan in-flight bounds. A plan performs a private ACP identity
// recheck on the Named Pipe connection thread.
const DISCOVERY_MAX_INFLIGHT_IMPORT_PLANS_PER_OWNER: usize = 2;
const DISCOVERY_MAX_INFLIGHT_IMPORT_PLANS_GLOBAL: usize = 8;
const MAX_FIXTURE_CATALOG_BYTES: usize = 512 * 1024;
const WORKER_LEASE_ACK_TIMEOUT: Duration = Duration::from_secs(2);
// Bounded dev-mode-only hold that makes the import-plan in-flight window
// deterministic in Named Pipe flood tests; ignored outside dev mode.
const MAX_IMPORT_PLAN_HOLD_MS: u64 = 2_000;

pub(crate) type EventSink = Arc<dyn Fn(RuntimeEvent) + Send + Sync + 'static>;

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct DiscoveryOwnerScope {
    client_id: String,
    session_id: String,
}

impl DiscoveryOwnerScope {
    pub(crate) fn from_authenticated_session(client_id: &str, session_id: &str) -> Self {
        Self {
            client_id: client_id.to_owned(),
            session_id: session_id.to_owned(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct LocalDiscoveryService {
    state: Arc<Mutex<LocalDiscoveryState>>,
    publication_lock: Arc<Mutex<()>>,
    #[cfg(test)]
    worker_before_lease_hook: Arc<Mutex<Option<Arc<WorkerPauseHook>>>>,
    #[cfg(test)]
    worker_after_lease_hook: Arc<Mutex<Option<Arc<WorkerPauseHook>>>>,
    #[cfg(test)]
    verify_before_state_hook: Arc<Mutex<Option<Arc<WorkerPauseHook>>>>,
    #[cfg(test)]
    dismiss_before_state_hook: Arc<Mutex<Option<Arc<WorkerPauseHook>>>>,
    #[cfg(test)]
    import_plan_before_state_hook: Arc<Mutex<Option<Arc<WorkerPauseHook>>>>,
    #[cfg(test)]
    active_running_leases: Arc<AtomicUsize>,
    #[cfg(test)]
    scan_workloads_started: Arc<AtomicUsize>,
    #[cfg(test)]
    verify_private_state_attempts: Arc<AtomicUsize>,
    #[cfg(test)]
    import_plan_preflight_attempts: Arc<AtomicUsize>,
    configuration: LocalDiscoveryConfiguration,
    limits: LocalDiscoveryLimits,
    next_id: Arc<AtomicU64>,
}

struct LocalDiscoveryState {
    sessions: BTreeMap<String, DiscoverySession>,
    requests: BTreeMap<DiscoveryRequestKey, DiscoveryRequestReceipt>,
    running_scans: BTreeMap<String, DiscoveryOwnerScope>,
    lease_waiters: BTreeMap<String, mpsc::SyncSender<WorkerLeaseAck>>,
    running_verifications: BTreeMap<VerificationKey, DiscoveryOwnerScope>,
    verification_waiters: BTreeMap<VerificationKey, mpsc::SyncSender<VerificationLeaseAck>>,
    inflight_import_plans: BTreeMap<ImportPlanKey, DiscoveryOwnerScope>,
    accepting_starts: bool,
    shutdown_generation: u64,
}

struct DiscoverySession {
    owner: DiscoveryOwnerScope,
    expires_at: Instant,
    terminal_at: Option<Instant>,
    scan_cancelled: Arc<AtomicBool>,
    verification_cancellations: BTreeMap<String, Arc<AtomicBool>>,
    status: SessionStatus,
    candidates: BTreeMap<String, CandidateState>,
    diagnostics: Vec<DiscoveryDiagnostic>,
    acp_session: Option<AcpDiscoverySession>,
}

#[derive(Clone)]
struct CandidateState {
    projection: CandidateProjection,
    has_acp_binding: bool,
    verification: Option<AcpVerificationResult>,
    dismissed: bool,
    verifying: bool,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum SessionStatus {
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl SessionStatus {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
struct DiscoveryRequestKey {
    owner: DiscoveryOwnerScope,
    command: String,
    request_id: String,
}

#[derive(Clone)]
struct DiscoveryRequestReceipt {
    payload_hash: String,
    state: DiscoveryRequestReceiptState,
}

#[derive(Clone)]
enum DiscoveryRequestReceiptState {
    PendingStart {
        scan_id: String,
    },
    WorkerReady {
        scan_id: String,
        response: Value,
    },
    PendingVerification {
        scan_id: String,
        candidate_id: String,
    },
    VerificationWorkerReady {
        scan_id: String,
        candidate_id: String,
        response: Value,
    },
    Committed {
        response: Value,
    },
}

pub(crate) enum StartScanOutcome {
    Replayed(Value),
    Reserved(Box<StartScanReservation>),
}

pub(crate) enum VerifyStartOutcome {
    Replayed(Value),
    AlreadyVerified(Value),
    Reserved(Box<VerifyReservation>),
}

pub(crate) struct StartScanReservation {
    service: LocalDiscoveryService,
    request_key: DiscoveryRequestKey,
    scan_id: String,
    payload_hash: String,
    response: Value,
    shutdown_generation: u64,
    explicit_sources: Option<Vec<ExplicitDiscoverySource>>,
    launched: bool,
}

pub(crate) struct WorkerReadyStart {
    reservation: StartScanReservation,
    gate: Arc<WorkerStartGate>,
    lease_receiver: mpsc::Receiver<WorkerLeaseAck>,
    publication_latch: Arc<WorkerPublicationLatch>,
    event_sink: EventSink,
    worker_started: bool,
    lease_acquired: bool,
    published: bool,
}

pub(crate) struct VerifyReservation {
    service: LocalDiscoveryService,
    request_key: DiscoveryRequestKey,
    scan_id: String,
    candidate_id: String,
    payload_hash: String,
    response: Value,
    shutdown_generation: u64,
    work: VerificationWork,
    launched: bool,
}

pub(crate) struct WorkerReadyVerify {
    reservation: VerifyReservation,
    gate: Arc<WorkerStartGate>,
    lease_receiver: mpsc::Receiver<VerificationLeaseAck>,
    publication_latch: Arc<WorkerPublicationLatch>,
    worker_started: bool,
    lease_acquired: bool,
    published: bool,
}

pub(crate) struct ImportPlanWork {
    scan_id: String,
    candidate_id: String,
    project_id: String,
    model_selection: Option<String>,
    acp_session: AcpDiscoverySession,
    verification: AcpVerificationResult,
    cancelled: Arc<AtomicBool>,
    projection: CandidateProjection,
    // RAII in-flight lease; held for the whole lifetime of the work so every
    // completion path releases the import-plan slot. Never read directly.
    _lease: ImportPlanLease,
}

/// Core-private, revalidated input for one durable import transaction.  This
/// carries renderer-safe manifest metadata only; executable paths, process
/// identities, environment values, and ACP session handles stay private.
pub(crate) struct LocalImportWork {
    pub(crate) scan_id: String,
    pub(crate) candidate_id: String,
    pub(crate) project_id: String,
    pub(crate) model_selection: Option<String>,
    pub(crate) projection: CandidateProjection,
    pub(crate) metadata: AcpImportPlanMetadata,
    _lease: ImportPlanLease,
}

pub(crate) struct DiscoveryStartPublicationGuard<'a> {
    service: &'a LocalDiscoveryService,
    _guard: std::sync::MutexGuard<'a, ()>,
}

struct RunningScanLease {
    service: LocalDiscoveryService,
    scan_id: String,
    #[cfg(test)]
    active_running_leases: Arc<AtomicUsize>,
}

struct RunningVerificationLease {
    service: LocalDiscoveryService,
    key: VerificationKey,
}

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
struct VerificationKey {
    scan_id: String,
    candidate_id: String,
}

/// Stable business-operation identity for one import-plan request. `scan_id`
/// is unique per owner/session, so this key is owner-unique as well.
#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
struct ImportPlanKey {
    scan_id: String,
    candidate_id: String,
    project_id: String,
    model_selection: Option<String>,
}

/// RAII in-flight lease for one import-plan request. The slot is reserved
/// inside the state lock before any private ACP read and released on every
/// completion path: success, error, shutdown, panic unwind, or IPC disconnect.
struct ImportPlanLease {
    service: LocalDiscoveryService,
    key: ImportPlanKey,
}

#[cfg(test)]
#[derive(Clone, Copy, Eq, PartialEq)]
enum StartLaunchMode {
    FailSpawn,
    FailReadiness,
    ShutdownAfterReadyBeforeCommit,
    ShutdownAfterCommitBeforePublish,
}

struct WorkerStartGate {
    state: Mutex<WorkerStartGateState>,
    changed: Condvar,
}

#[cfg(test)]
pub(crate) struct WorkerPauseHook {
    state: Mutex<WorkerLeasePauseState>,
    changed: Condvar,
}

#[cfg(test)]
struct WorkerLeasePauseState {
    entered: bool,
    released: bool,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum WorkerStartGateState {
    Waiting,
    Start,
    Abort,
}

enum WorkerLeaseAck {
    Acquired,
    Unavailable,
}

enum VerificationLeaseAck {
    Acquired,
    Unavailable,
}

struct WorkerPublicationLatch {
    state: Mutex<WorkerPublicationState>,
    changed: Condvar,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum WorkerPublicationState {
    Waiting,
    Published,
    Abort,
}

pub(crate) struct VerifyRequest<'a> {
    pub(crate) owner: &'a DiscoveryOwnerScope,
    pub(crate) request_id: &'a str,
    pub(crate) scan_id: &'a str,
    pub(crate) candidate_id: &'a str,
    pub(crate) consent: bool,
    pub(crate) deadline: Duration,
    pub(crate) event_sink: EventSink,
}

#[derive(Clone)]
struct VerificationWork {
    scan_id: String,
    owner: DiscoveryOwnerScope,
    candidate_id: String,
    acp_session: AcpDiscoverySession,
    cancelled: Arc<AtomicBool>,
    deadline: Duration,
    event_sink: EventSink,
}

#[derive(Clone)]
struct LocalDiscoveryConfiguration {
    scan: WindowsPassiveDiscoveryConfig,
    catalog: CatalogConfiguration,
    catalog_diagnostics: Vec<DiscoveryDiagnostic>,
    import_plan_hold: Duration,
}

#[derive(Clone, Copy)]
struct LocalDiscoveryLimits {
    max_sessions_per_owner: usize,
    max_sessions_global: usize,
    max_running_scans_per_owner: usize,
    max_running_scans_global: usize,
    max_receipts_per_session: usize,
    max_receipts_per_owner: usize,
    max_receipts_global: usize,
    max_running_verifications_per_owner: usize,
    max_running_verifications_global: usize,
    max_inflight_import_plans_per_owner: usize,
    max_inflight_import_plans_global: usize,
}

#[cfg(test)]
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct LocalDiscoveryCounts {
    pub(crate) sessions: usize,
    pub(crate) owner_sessions: usize,
    pub(crate) requests: usize,
    pub(crate) owner_requests: usize,
    pub(crate) running_scans: usize,
    pub(crate) owner_running_scans: usize,
    pub(crate) lease_waiters: usize,
    pub(crate) owner_lease_waiters: usize,
    pub(crate) running_verifications: usize,
    pub(crate) owner_running_verifications: usize,
    pub(crate) verification_waiters: usize,
    pub(crate) owner_verification_waiters: usize,
    pub(crate) inflight_import_plans: usize,
    pub(crate) owner_inflight_import_plans: usize,
}

#[derive(Clone)]
enum CatalogConfiguration {
    Available(CatalogSnapshot),
    Unavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LocalDiscoveryServiceError {
    InvalidPayload,
    EntropyUnavailable,
    RequestIdReuse,
    StartInProgress,
    ScanNotFound,
    ScanExpired,
    CandidateNotFound,
    CandidateDismissed,
    ConsentRequired,
    CandidateNotVerified,
    VerificationInProgress,
    AdapterRequired,
    IdentityChanged,
    OwnerScanCapacityExhausted,
    GlobalScanCapacityExhausted,
    OwnerReceiptCapacityExhausted,
    GlobalReceiptCapacityExhausted,
    OwnerVerificationCapacityExhausted,
    GlobalVerificationCapacityExhausted,
    OwnerImportPlanCapacityExhausted,
    GlobalImportPlanCapacityExhausted,
    ImportPlanInFlight,
    ImportConflict,
    ImportPersistenceFailed,
    ScanWorkerUnavailable,
    ShuttingDown,
}

impl LocalDiscoveryServiceError {
    pub(crate) const fn code(self) -> &'static str {
        match self {
            Self::InvalidPayload => "INVALID_DISCOVERY_REQUEST",
            Self::EntropyUnavailable => "DISCOVERY_ENTROPY_UNAVAILABLE",
            Self::RequestIdReuse => "REQUEST_ID_REUSE",
            Self::StartInProgress => "DISCOVERY_START_IN_PROGRESS",
            Self::ScanNotFound => "DISCOVERY_SCAN_NOT_FOUND",
            Self::ScanExpired => "DISCOVERY_SCAN_EXPIRED",
            Self::CandidateNotFound => "DISCOVERY_CANDIDATE_NOT_FOUND",
            Self::CandidateDismissed => "DISCOVERY_CANDIDATE_DISMISSED",
            Self::ConsentRequired => "DISCOVERY_CONSENT_REQUIRED",
            Self::CandidateNotVerified => "DISCOVERY_CANDIDATE_NOT_VERIFIED",
            Self::VerificationInProgress => "DISCOVERY_VERIFICATION_IN_PROGRESS",
            Self::AdapterRequired => "DISCOVERY_ADAPTER_REQUIRED",
            Self::IdentityChanged => "DISCOVERY_IDENTITY_CHANGED",
            Self::OwnerScanCapacityExhausted => "DISCOVERY_OWNER_SCAN_CAPACITY_EXHAUSTED",
            Self::GlobalScanCapacityExhausted => "DISCOVERY_GLOBAL_SCAN_CAPACITY_EXHAUSTED",
            Self::OwnerReceiptCapacityExhausted => "DISCOVERY_OWNER_RECEIPT_CAPACITY_EXHAUSTED",
            Self::GlobalReceiptCapacityExhausted => "DISCOVERY_GLOBAL_RECEIPT_CAPACITY_EXHAUSTED",
            Self::OwnerVerificationCapacityExhausted => {
                "DISCOVERY_OWNER_VERIFICATION_CAPACITY_EXHAUSTED"
            }
            Self::GlobalVerificationCapacityExhausted => {
                "DISCOVERY_GLOBAL_VERIFICATION_CAPACITY_EXHAUSTED"
            }
            Self::OwnerImportPlanCapacityExhausted => {
                "DISCOVERY_OWNER_IMPORT_PLAN_CAPACITY_EXHAUSTED"
            }
            Self::GlobalImportPlanCapacityExhausted => {
                "DISCOVERY_GLOBAL_IMPORT_PLAN_CAPACITY_EXHAUSTED"
            }
            Self::ImportPlanInFlight => "DISCOVERY_IMPORT_PLAN_IN_PROGRESS",
            Self::ImportConflict => "IMPORT_CONFLICT",
            Self::ImportPersistenceFailed => "IMPORT_PERSISTENCE_FAILED",
            Self::ScanWorkerUnavailable => "DISCOVERY_SCAN_WORKER_UNAVAILABLE",
            Self::ShuttingDown => "DISCOVERY_SERVICE_SHUTTING_DOWN",
        }
    }

    pub(crate) const fn message(self) -> &'static str {
        match self {
            Self::InvalidPayload => "the discovery request payload is invalid",
            Self::EntropyUnavailable => "a discovery session could not be created securely",
            Self::RequestIdReuse => "requestId is already bound to a different discovery request",
            Self::StartInProgress => "the discovery start request is still committing",
            Self::ScanNotFound => "the requested discovery scan does not exist",
            Self::ScanExpired => "the requested discovery scan has expired",
            Self::CandidateNotFound => "the requested discovery candidate does not exist",
            Self::CandidateDismissed => "the requested discovery candidate was dismissed",
            Self::ConsentRequired => "explicit verification consent is required",
            Self::CandidateNotVerified => {
                "the candidate must be verified before an import plan can be created"
            }
            Self::VerificationInProgress => "candidate verification is already in progress",
            Self::AdapterRequired => "the candidate has no retained ACP adapter binding",
            Self::IdentityChanged => "the candidate identity changed and must be scanned again",
            Self::OwnerScanCapacityExhausted => {
                "discovery scan capacity for the authenticated owner is exhausted"
            }
            Self::GlobalScanCapacityExhausted => "global discovery scan capacity is exhausted",
            Self::OwnerReceiptCapacityExhausted => {
                "discovery receipt capacity for the authenticated owner is exhausted"
            }
            Self::GlobalReceiptCapacityExhausted => {
                "global discovery receipt capacity is exhausted"
            }
            Self::OwnerVerificationCapacityExhausted => {
                "discovery verification capacity for the authenticated owner is exhausted"
            }
            Self::GlobalVerificationCapacityExhausted => {
                "global discovery verification capacity is exhausted"
            }
            Self::OwnerImportPlanCapacityExhausted => {
                "import-plan capacity for the authenticated owner is exhausted"
            }
            Self::GlobalImportPlanCapacityExhausted => "global import-plan capacity is exhausted",
            Self::ImportPlanInFlight => {
                "an import plan for the same discovery operation is already in progress"
            }
            Self::ImportConflict => "the local Agent import conflicts with an existing record",
            Self::ImportPersistenceFailed => "the local Agent import could not be persisted",
            Self::ScanWorkerUnavailable => "a discovery scan worker could not be started",
            Self::ShuttingDown => "local discovery is shutting down",
        }
    }
}

impl LocalDiscoveryService {
    pub(crate) fn from_environment() -> Self {
        Self {
            state: Arc::new(Mutex::new(LocalDiscoveryState {
                sessions: BTreeMap::new(),
                requests: BTreeMap::new(),
                running_scans: BTreeMap::new(),
                lease_waiters: BTreeMap::new(),
                running_verifications: BTreeMap::new(),
                verification_waiters: BTreeMap::new(),
                inflight_import_plans: BTreeMap::new(),
                accepting_starts: true,
                shutdown_generation: 0,
            })),
            publication_lock: Arc::new(Mutex::new(())),
            #[cfg(test)]
            worker_before_lease_hook: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            worker_after_lease_hook: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            verify_before_state_hook: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            dismiss_before_state_hook: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            import_plan_before_state_hook: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            active_running_leases: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            scan_workloads_started: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            verify_private_state_attempts: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            import_plan_preflight_attempts: Arc::new(AtomicUsize::new(0)),
            configuration: LocalDiscoveryConfiguration::from_environment(),
            limits: LocalDiscoveryLimits::from_environment(),
            next_id: Arc::new(AtomicU64::new(0)),
        }
    }

    pub(crate) fn begin_start(
        &self,
        owner: &DiscoveryOwnerScope,
        request_id: &str,
        payload: &Value,
    ) -> Result<StartScanOutcome, LocalDiscoveryServiceError> {
        let request_key = DiscoveryRequestKey {
            owner: owner.clone(),
            command: "agent.discovery.start".into(),
            request_id: request_id.to_owned(),
        };
        let payload_hash = request_hash(payload);
        let mut state = lock_state(&self.state);
        prune_expired(&mut state);
        if !state.accepting_starts {
            return Err(LocalDiscoveryServiceError::ShuttingDown);
        }
        if let Some(existing) = state.requests.get(&request_key) {
            return replay_or_reject(existing, &payload_hash).map(StartScanOutcome::Replayed);
        }
        let explicit_sources = parse_start_explicit_sources(payload)?;
        ensure_start_capacity(&mut state, owner, &self.limits)?;
        let scan_id = self.new_scan_id()?;
        ensure_receipt_capacity(&state, &scan_id, owner, &self.limits)?;
        let response = json!({
            "scanId": scan_id,
            "accepted": true,
            "state": "running",
        });
        state.sessions.insert(
            scan_id.clone(),
            DiscoverySession {
                owner: owner.clone(),
                expires_at: Instant::now() + SESSION_LIFETIME,
                terminal_at: None,
                scan_cancelled: Arc::new(AtomicBool::new(false)),
                verification_cancellations: BTreeMap::new(),
                status: SessionStatus::Running,
                candidates: BTreeMap::new(),
                diagnostics: Vec::new(),
                acp_session: None,
            },
        );
        state.running_scans.insert(scan_id.clone(), owner.clone());
        state.requests.insert(
            request_key.clone(),
            DiscoveryRequestReceipt {
                payload_hash: payload_hash.clone(),
                state: DiscoveryRequestReceiptState::PendingStart {
                    scan_id: scan_id.clone(),
                },
            },
        );
        Ok(StartScanOutcome::Reserved(Box::new(StartScanReservation {
            service: self.clone(),
            request_key,
            scan_id,
            payload_hash,
            response,
            shutdown_generation: state.shutdown_generation,
            explicit_sources,
            launched: false,
        })))
    }

    pub(crate) fn begin_verify_with_publication(
        &self,
        _publication: &DiscoveryStartPublicationGuard<'_>,
        request: VerifyRequest<'_>,
    ) -> Result<VerifyStartOutcome, LocalDiscoveryServiceError> {
        if !is_safe_id(request.scan_id) || !is_safe_id(request.candidate_id) {
            return Err(LocalDiscoveryServiceError::InvalidPayload);
        }
        // The idempotency hash binds only business intent (scan/candidate/
        // consent), never the per-attempt timeout budget. Two retries of the
        // same requestId with different deadlineMs must replay, not conflict.
        let payload = json!({
            "scanId": request.scan_id,
            "candidateId": request.candidate_id,
            "consent": request.consent,
        });
        let request_key = DiscoveryRequestKey {
            owner: request.owner.clone(),
            command: "agent.discovery.verify".into(),
            request_id: request.request_id.to_owned(),
        };
        let payload_hash = request_hash(&payload);
        let response = json!({
            "scanId": request.scan_id,
            "candidateId": request.candidate_id,
            "accepted": true,
            "state": "verifying",
        });
        let key = VerificationKey {
            scan_id: request.scan_id.to_owned(),
            candidate_id: request.candidate_id.to_owned(),
        };
        let mut state = lock_state(&self.state);
        prune_expired(&mut state);
        let accepting_starts = state.accepting_starts;
        let verification_is_running = state.running_verifications.contains_key(&key);
        {
            let session = state
                .sessions
                .get(request.scan_id)
                .ok_or(LocalDiscoveryServiceError::ScanNotFound)?;
            ensure_owner(session, request.owner)?;
            if !accepting_starts
                || session.status == SessionStatus::Cancelled
                || session.scan_cancelled.load(Ordering::Acquire)
            {
                return Err(LocalDiscoveryServiceError::ShuttingDown);
            }
        }
        if let Some(existing) = state.requests.get(&request_key) {
            return replay_or_reject(existing, &payload_hash).map(VerifyStartOutcome::Replayed);
        }
        // W5.8 validation phase. The borrows end before the capacity checks,
        // and a still-valid verification is reused without any new receipt,
        // event, thread, cancellation, or ACP child (re-verification with a
        // new requestId must not restart the ACP workload).
        {
            let session = state
                .sessions
                .get_mut(request.scan_id)
                .ok_or(LocalDiscoveryServiceError::ScanNotFound)?;
            if session.expires_at <= Instant::now() {
                session.scan_cancelled.store(true, Ordering::Release);
                return Err(LocalDiscoveryServiceError::ScanExpired);
            }
            if !request.consent {
                return Err(LocalDiscoveryServiceError::ConsentRequired);
            }
            let candidate = session
                .candidates
                .get_mut(request.candidate_id)
                .ok_or(LocalDiscoveryServiceError::CandidateNotFound)?;
            if candidate.dismissed {
                return Err(LocalDiscoveryServiceError::CandidateDismissed);
            }
            if !candidate.has_acp_binding {
                return Err(LocalDiscoveryServiceError::AdapterRequired);
            }
            if candidate.verifying || verification_is_running {
                return Err(LocalDiscoveryServiceError::VerificationInProgress);
            }
            if let Some(verification) = &candidate.verification {
                if matches!(
                    verification.report().status,
                    AcpVerificationStatus::Verified | AcpVerificationStatus::AuthRequired
                ) {
                    return Ok(VerifyStartOutcome::AlreadyVerified(json!({
                        "scanId": request.scan_id,
                        "candidateId": request.candidate_id,
                        "accepted": true,
                        "state": candidate_lifecycle_state(candidate),
                        "reused": true,
                    })));
                }
            }
        }
        // W5.8: atomic capacity checks inside the same state lock, before any
        // mutation, thread spawn, event, or private ACP read. A rejected
        // request creates no receipt, verifying flag, cancellation, running
        // slot, waiter, thread, or ACP child.
        ensure_receipt_capacity(&state, request.scan_id, request.owner, &self.limits)?;
        ensure_verification_capacity(&state, request.owner, &self.limits)?;
        let session = state
            .sessions
            .get_mut(request.scan_id)
            .ok_or(LocalDiscoveryServiceError::ScanNotFound)?;
        #[cfg(test)]
        self.verify_private_state_attempts
            .fetch_add(1, Ordering::AcqRel);
        let acp_session = session
            .acp_session
            .clone()
            .ok_or(LocalDiscoveryServiceError::AdapterRequired)?;
        let cancellation = Arc::new(AtomicBool::new(false));
        let candidate = session
            .candidates
            .get_mut(request.candidate_id)
            .ok_or(LocalDiscoveryServiceError::CandidateNotFound)?;
        candidate.verifying = true;
        session
            .verification_cancellations
            .insert(request.candidate_id.to_owned(), Arc::clone(&cancellation));
        state
            .running_verifications
            .insert(key, request.owner.clone());
        state.requests.insert(
            request_key.clone(),
            DiscoveryRequestReceipt {
                payload_hash: payload_hash.clone(),
                state: DiscoveryRequestReceiptState::PendingVerification {
                    scan_id: request.scan_id.to_owned(),
                    candidate_id: request.candidate_id.to_owned(),
                },
            },
        );
        Ok(VerifyStartOutcome::Reserved(Box::new(VerifyReservation {
            service: self.clone(),
            request_key,
            scan_id: request.scan_id.to_owned(),
            candidate_id: request.candidate_id.to_owned(),
            payload_hash,
            response,
            shutdown_generation: state.shutdown_generation,
            work: VerificationWork {
                scan_id: request.scan_id.to_owned(),
                owner: request.owner.clone(),
                candidate_id: request.candidate_id.to_owned(),
                acp_session,
                cancelled: cancellation,
                deadline: request.deadline,
                event_sink: request.event_sink,
            },
            launched: false,
        })))
    }

    #[cfg(test)]
    fn start(
        &self,
        owner: &DiscoveryOwnerScope,
        request_id: &str,
        payload: &Value,
        event_sink: EventSink,
    ) -> Result<Value, LocalDiscoveryServiceError> {
        match self.begin_start(owner, request_id, payload)? {
            StartScanOutcome::Replayed(response) => Ok(response),
            StartScanOutcome::Reserved(reservation) => reservation.launch(event_sink),
        }
    }

    fn rollback_start_reservation(
        &self,
        scan_id: &str,
        request_key: &DiscoveryRequestKey,
        payload_hash: &str,
    ) {
        let mut state = lock_state(&self.state);
        let request_belongs_to_reservation = match state.requests.get(request_key) {
            Some(receipt) => {
                receipt.payload_hash == payload_hash
                    && matches!(
                        &receipt.state,
                        DiscoveryRequestReceiptState::PendingStart {
                            scan_id: pending_scan_id
                        } | DiscoveryRequestReceiptState::WorkerReady {
                            scan_id: pending_scan_id,
                            ..
                        } if pending_scan_id == scan_id
                    )
            }
            None => true,
        };
        if request_belongs_to_reservation {
            state.sessions.remove(scan_id);
            state.running_scans.remove(scan_id);
            let waiter = state.lease_waiters.remove(scan_id);
            if state.requests.get(request_key).is_some_and(|receipt| {
                receipt.payload_hash == payload_hash
                    && matches!(
                        &receipt.state,
                        DiscoveryRequestReceiptState::PendingStart {
                            scan_id: pending_scan_id
                        } | DiscoveryRequestReceiptState::WorkerReady {
                            scan_id: pending_scan_id,
                            ..
                        } if pending_scan_id == scan_id
                    )
            }) {
                state.requests.remove(request_key);
            }
            drop(state);
            if let Some(waiter) = waiter {
                let _ = waiter.try_send(WorkerLeaseAck::Unavailable);
            }
        }
    }

    fn rollback_verify_reservation(
        &self,
        scan_id: &str,
        candidate_id: &str,
        request_key: &DiscoveryRequestKey,
        payload_hash: &str,
    ) {
        let key = VerificationKey {
            scan_id: scan_id.to_owned(),
            candidate_id: candidate_id.to_owned(),
        };
        let mut state = lock_state(&self.state);
        let request_belongs_to_reservation = match state.requests.get(request_key) {
            Some(receipt) => {
                receipt.payload_hash == payload_hash
                    && matches!(
                        &receipt.state,
                        DiscoveryRequestReceiptState::PendingVerification {
                            scan_id: pending_scan_id,
                            candidate_id: pending_candidate_id,
                        }
                        | DiscoveryRequestReceiptState::VerificationWorkerReady {
                            scan_id: pending_scan_id,
                            candidate_id: pending_candidate_id,
                            ..
                        } if pending_scan_id == scan_id && pending_candidate_id == candidate_id
                    )
            }
            None => true,
        };
        if !request_belongs_to_reservation {
            return;
        }
        if let Some(session) = state.sessions.get_mut(scan_id) {
            if let Some(candidate) = session.candidates.get_mut(candidate_id) {
                candidate.verifying = false;
            }
            if let Some(cancellation) = session.verification_cancellations.remove(candidate_id) {
                cancellation.store(true, Ordering::Release);
            }
        }
        state.running_verifications.remove(&key);
        let waiter = state.verification_waiters.remove(&key);
        if state.requests.get(request_key).is_some_and(|receipt| {
            receipt.payload_hash == payload_hash
                && matches!(
                    &receipt.state,
                    DiscoveryRequestReceiptState::PendingVerification {
                        scan_id: pending_scan_id,
                        candidate_id: pending_candidate_id,
                    }
                    | DiscoveryRequestReceiptState::VerificationWorkerReady {
                        scan_id: pending_scan_id,
                        candidate_id: pending_candidate_id,
                        ..
                    } if pending_scan_id == scan_id && pending_candidate_id == candidate_id
                )
        }) {
            state.requests.remove(request_key);
        }
        drop(state);
        if let Some(waiter) = waiter {
            let _ = waiter.try_send(VerificationLeaseAck::Unavailable);
        }
    }

    fn mark_start_worker_ready(
        &self,
        scan_id: &str,
        request_key: &DiscoveryRequestKey,
        payload_hash: &str,
        response: &Value,
        shutdown_generation: u64,
        lease_sender: mpsc::SyncSender<WorkerLeaseAck>,
    ) -> Result<(), LocalDiscoveryServiceError> {
        let mut state = lock_state(&self.state);
        if !state.accepting_starts || state.shutdown_generation != shutdown_generation {
            return Err(LocalDiscoveryServiceError::ShuttingDown);
        }
        if state.running_scans.get(scan_id) != Some(&request_key.owner) {
            return Err(LocalDiscoveryServiceError::ShuttingDown);
        }
        let session_owner = state.sessions.get(scan_id).map(|session| &session.owner);
        if session_owner != Some(&request_key.owner) {
            return Err(LocalDiscoveryServiceError::ShuttingDown);
        }
        let Some(receipt) = state.requests.get_mut(request_key) else {
            return Err(LocalDiscoveryServiceError::ShuttingDown);
        };
        if receipt.payload_hash != payload_hash {
            return Err(LocalDiscoveryServiceError::RequestIdReuse);
        }
        match &receipt.state {
            DiscoveryRequestReceiptState::PendingStart {
                scan_id: pending_scan_id,
            } if pending_scan_id == scan_id => {
                receipt.state = DiscoveryRequestReceiptState::WorkerReady {
                    scan_id: scan_id.to_owned(),
                    response: response.clone(),
                };
                state.lease_waiters.insert(scan_id.to_owned(), lease_sender);
                Ok(())
            }
            DiscoveryRequestReceiptState::Committed { .. }
            | DiscoveryRequestReceiptState::WorkerReady { .. }
            | DiscoveryRequestReceiptState::PendingVerification { .. }
            | DiscoveryRequestReceiptState::VerificationWorkerReady { .. } => {
                Err(LocalDiscoveryServiceError::StartInProgress)
            }
            DiscoveryRequestReceiptState::PendingStart { .. } => {
                Err(LocalDiscoveryServiceError::ShuttingDown)
            }
        }
    }

    fn mark_verify_worker_ready(
        &self,
        reservation: &VerifyReservation,
        lease_sender: mpsc::SyncSender<VerificationLeaseAck>,
    ) -> Result<(), LocalDiscoveryServiceError> {
        let key = VerificationKey {
            scan_id: reservation.scan_id.clone(),
            candidate_id: reservation.candidate_id.clone(),
        };
        let mut state = lock_state(&self.state);
        if !state.accepting_starts || state.shutdown_generation != reservation.shutdown_generation {
            return Err(LocalDiscoveryServiceError::ShuttingDown);
        }
        let session = state
            .sessions
            .get(&reservation.scan_id)
            .ok_or(LocalDiscoveryServiceError::ShuttingDown)?;
        if session.owner != reservation.request_key.owner
            || session.status == SessionStatus::Cancelled
            || session.scan_cancelled.load(Ordering::Acquire)
            || state.running_verifications.get(&key) != Some(&reservation.request_key.owner)
        {
            return Err(LocalDiscoveryServiceError::ShuttingDown);
        }
        let Some(receipt) = state.requests.get_mut(&reservation.request_key) else {
            return Err(LocalDiscoveryServiceError::ShuttingDown);
        };
        if receipt.payload_hash != reservation.payload_hash {
            return Err(LocalDiscoveryServiceError::RequestIdReuse);
        }
        match &receipt.state {
            DiscoveryRequestReceiptState::PendingVerification {
                scan_id,
                candidate_id,
            } if scan_id == &reservation.scan_id && candidate_id == &reservation.candidate_id => {
                receipt.state = DiscoveryRequestReceiptState::VerificationWorkerReady {
                    scan_id: reservation.scan_id.clone(),
                    candidate_id: reservation.candidate_id.clone(),
                    response: reservation.response.clone(),
                };
                state.verification_waiters.insert(key, lease_sender);
                Ok(())
            }
            _ => Err(LocalDiscoveryServiceError::ShuttingDown),
        }
    }

    pub(crate) fn ensure_start_replay_publishable(
        &self,
        scan_id: &str,
        owner: &DiscoveryOwnerScope,
    ) -> Result<(), LocalDiscoveryServiceError> {
        let mut state = lock_state(&self.state);
        prune_expired(&mut state);
        if !state.accepting_starts {
            return Err(LocalDiscoveryServiceError::ShuttingDown);
        }
        let Some(session) = state.sessions.get(scan_id) else {
            return Err(LocalDiscoveryServiceError::ShuttingDown);
        };
        if &session.owner != owner {
            return Err(LocalDiscoveryServiceError::ShuttingDown);
        }
        Ok(())
    }

    fn ensure_worker_ready_publishable_locked(
        &self,
        scan_id: &str,
        owner: &DiscoveryOwnerScope,
    ) -> Result<(), LocalDiscoveryServiceError> {
        let mut state = lock_state(&self.state);
        prune_expired(&mut state);
        if !state.accepting_starts {
            return Err(LocalDiscoveryServiceError::ShuttingDown);
        }
        if state.running_scans.get(scan_id) != Some(owner) {
            return Err(LocalDiscoveryServiceError::ShuttingDown);
        }
        let session_owner = state.sessions.get(scan_id).map(|session| &session.owner);
        if session_owner != Some(owner) {
            return Err(LocalDiscoveryServiceError::ShuttingDown);
        }
        let has_worker_ready_receipt = state.requests.iter().any(|(key, receipt)| {
            &key.owner == owner
                && matches!(
                    &receipt.state,
                    DiscoveryRequestReceiptState::WorkerReady {
                        scan_id: ready_scan_id,
                        ..
                    } if ready_scan_id == scan_id
                )
        });
        if !has_worker_ready_receipt {
            return Err(LocalDiscoveryServiceError::ShuttingDown);
        }
        Ok(())
    }

    fn ensure_verify_worker_ready_publishable_locked(
        &self,
        scan_id: &str,
        candidate_id: &str,
        owner: &DiscoveryOwnerScope,
    ) -> Result<(), LocalDiscoveryServiceError> {
        let key = VerificationKey {
            scan_id: scan_id.to_owned(),
            candidate_id: candidate_id.to_owned(),
        };
        let mut state = lock_state(&self.state);
        prune_expired(&mut state);
        let session = state
            .sessions
            .get(scan_id)
            .ok_or(LocalDiscoveryServiceError::ScanNotFound)?;
        ensure_owner(session, owner)?;
        if !state.accepting_starts
            || session.status == SessionStatus::Cancelled
            || session.scan_cancelled.load(Ordering::Acquire)
            || state.running_verifications.get(&key) != Some(owner)
        {
            return Err(LocalDiscoveryServiceError::ShuttingDown);
        }
        let has_worker_ready_receipt = state.requests.iter().any(|(request_key, receipt)| {
            &request_key.owner == owner
                && matches!(
                    &receipt.state,
                    DiscoveryRequestReceiptState::VerificationWorkerReady {
                        scan_id: ready_scan_id,
                        candidate_id: ready_candidate_id,
                        ..
                    } if ready_scan_id == scan_id && ready_candidate_id == candidate_id
                )
        });
        has_worker_ready_receipt
            .then_some(())
            .ok_or(LocalDiscoveryServiceError::ShuttingDown)
    }

    pub(crate) fn start_publication_guard(&self) -> DiscoveryStartPublicationGuard<'_> {
        DiscoveryStartPublicationGuard {
            service: self,
            _guard: self
                .publication_lock
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        }
    }

    fn publish_start_locked(
        &self,
        scan_id: &str,
        owner: &DiscoveryOwnerScope,
        event_sink: &EventSink,
    ) -> Result<(), LocalDiscoveryServiceError> {
        let response = {
            let mut state = lock_state(&self.state);
            if !state.accepting_starts {
                return Err(LocalDiscoveryServiceError::ShuttingDown);
            }
            if state.running_scans.get(scan_id) != Some(owner) {
                return Err(LocalDiscoveryServiceError::ShuttingDown);
            }
            let session_owner = state.sessions.get(scan_id).map(|session| &session.owner);
            if session_owner != Some(owner) {
                return Err(LocalDiscoveryServiceError::ShuttingDown);
            }
            let Some((_, receipt)) = state.requests.iter_mut().find(|(_, receipt)| {
                matches!(
                    &receipt.state,
                    DiscoveryRequestReceiptState::WorkerReady {
                        scan_id: ready_scan_id,
                        ..
                    } if ready_scan_id == scan_id
                )
            }) else {
                return Err(LocalDiscoveryServiceError::ShuttingDown);
            };
            let DiscoveryRequestReceiptState::WorkerReady { response, .. } = &receipt.state else {
                return Err(LocalDiscoveryServiceError::ShuttingDown);
            };
            let response = response.clone();
            receipt.state = DiscoveryRequestReceiptState::Committed {
                response: response.clone(),
            };
            state.lease_waiters.remove(scan_id);
            response
        };
        self.emit(
            event_sink,
            "agent.discovery.started",
            scan_id,
            json!({"scanId": scan_id}),
        );
        debug_assert_eq!(response["accepted"], true);
        Ok(())
    }

    fn publish_verify_locked(
        &self,
        scan_id: &str,
        candidate_id: &str,
        owner: &DiscoveryOwnerScope,
    ) -> Result<(), LocalDiscoveryServiceError> {
        let key = VerificationKey {
            scan_id: scan_id.to_owned(),
            candidate_id: candidate_id.to_owned(),
        };
        let mut state = lock_state(&self.state);
        let session = state
            .sessions
            .get(scan_id)
            .ok_or(LocalDiscoveryServiceError::ScanNotFound)?;
        ensure_owner(session, owner)?;
        if !state.accepting_starts
            || session.status == SessionStatus::Cancelled
            || session.scan_cancelled.load(Ordering::Acquire)
            || state.running_verifications.get(&key) != Some(owner)
        {
            return Err(LocalDiscoveryServiceError::ShuttingDown);
        }
        let Some((_, receipt)) = state.requests.iter_mut().find(|(request_key, receipt)| {
            &request_key.owner == owner
                && matches!(
                    &receipt.state,
                    DiscoveryRequestReceiptState::VerificationWorkerReady {
                        scan_id: ready_scan_id,
                        candidate_id: ready_candidate_id,
                        ..
                    } if ready_scan_id == scan_id && ready_candidate_id == candidate_id
                )
        }) else {
            return Err(LocalDiscoveryServiceError::ShuttingDown);
        };
        let DiscoveryRequestReceiptState::VerificationWorkerReady { response, .. } = &receipt.state
        else {
            return Err(LocalDiscoveryServiceError::ShuttingDown);
        };
        let response = response.clone();
        receipt.state = DiscoveryRequestReceiptState::Committed { response };
        state.verification_waiters.remove(&key);
        Ok(())
    }

    fn acquire_running_scan_lease(
        &self,
        scan_id: &str,
        owner: &DiscoveryOwnerScope,
    ) -> Option<RunningScanLease> {
        let state = lock_state(&self.state);
        let running_owner = state.running_scans.get(scan_id)?;
        if running_owner != owner {
            return None;
        }
        #[cfg(test)]
        self.active_running_leases.fetch_add(1, Ordering::AcqRel);
        Some(RunningScanLease {
            service: self.clone(),
            scan_id: scan_id.to_owned(),
            #[cfg(test)]
            active_running_leases: Arc::clone(&self.active_running_leases),
        })
    }

    fn acquire_running_verification_lease(
        &self,
        scan_id: &str,
        candidate_id: &str,
        owner: &DiscoveryOwnerScope,
    ) -> Option<RunningVerificationLease> {
        let key = VerificationKey {
            scan_id: scan_id.to_owned(),
            candidate_id: candidate_id.to_owned(),
        };
        let state = lock_state(&self.state);
        if state.running_verifications.get(&key) != Some(owner) {
            return None;
        }
        Some(RunningVerificationLease {
            service: self.clone(),
            key,
        })
    }

    fn release_running_scan(&self, scan_id: &str) {
        let mut state = lock_state(&self.state);
        state.running_scans.remove(scan_id);
    }

    #[cfg(test)]
    pub(crate) fn counts_for_tests(&self, owner: &DiscoveryOwnerScope) -> LocalDiscoveryCounts {
        let state = lock_state(&self.state);
        LocalDiscoveryCounts {
            sessions: state.sessions.len(),
            owner_sessions: state
                .sessions
                .values()
                .filter(|session| &session.owner == owner)
                .count(),
            requests: state.requests.len(),
            owner_requests: state
                .requests
                .keys()
                .filter(|request| &request.owner == owner)
                .count(),
            running_scans: state.running_scans.len(),
            owner_running_scans: state
                .running_scans
                .values()
                .filter(|running_owner| *running_owner == owner)
                .count(),
            lease_waiters: state.lease_waiters.len(),
            owner_lease_waiters: state
                .lease_waiters
                .keys()
                .filter(|scan_id| {
                    state
                        .sessions
                        .get(*scan_id)
                        .is_some_and(|session| &session.owner == owner)
                        || state.running_scans.get(*scan_id) == Some(owner)
                })
                .count(),
            running_verifications: state.running_verifications.len(),
            owner_running_verifications: state
                .running_verifications
                .values()
                .filter(|running_owner| *running_owner == owner)
                .count(),
            verification_waiters: state.verification_waiters.len(),
            owner_verification_waiters: state
                .verification_waiters
                .keys()
                .filter(|key| state.running_verifications.get(*key) == Some(owner))
                .count(),
            inflight_import_plans: state.inflight_import_plans.len(),
            owner_inflight_import_plans: state
                .inflight_import_plans
                .values()
                .filter(|inflight_owner| *inflight_owner == owner)
                .count(),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_state_lock_held_for_tests<R>(&self, f: impl FnOnce() -> R) -> R {
        let _guard = lock_state(&self.state);
        f()
    }

    #[cfg(test)]
    pub(crate) fn set_worker_before_lease_hook_for_tests(&self, hook: Arc<WorkerPauseHook>) {
        *self
            .worker_before_lease_hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(hook);
    }

    #[cfg(test)]
    fn pause_before_running_lease_for_tests(&self) {
        let hook = self
            .worker_before_lease_hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        if let Some(hook) = hook {
            hook.pause();
        }
    }

    #[cfg(test)]
    pub(crate) fn set_worker_after_lease_hook_for_tests(&self, hook: Arc<WorkerPauseHook>) {
        *self
            .worker_after_lease_hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(hook);
    }

    #[cfg(test)]
    fn pause_after_running_lease_for_tests(&self) {
        let hook = self
            .worker_after_lease_hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        if let Some(hook) = hook {
            hook.pause();
        }
    }

    #[cfg(test)]
    pub(crate) fn set_verify_before_state_hook_for_tests(&self, hook: Arc<WorkerPauseHook>) {
        *self
            .verify_before_state_hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(hook);
    }

    #[cfg(test)]
    pub(crate) fn pause_before_verify_publication_for_tests(&self) {
        self.pause_test_hook(&self.verify_before_state_hook);
    }

    #[cfg(test)]
    pub(crate) fn set_dismiss_before_state_hook_for_tests(&self, hook: Arc<WorkerPauseHook>) {
        *self
            .dismiss_before_state_hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(hook);
    }

    #[cfg(test)]
    pub(crate) fn pause_before_dismiss_publication_for_tests(&self) {
        self.pause_test_hook(&self.dismiss_before_state_hook);
    }

    #[cfg(test)]
    pub(crate) fn set_import_plan_before_state_hook_for_tests(&self, hook: Arc<WorkerPauseHook>) {
        *self
            .import_plan_before_state_hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(hook);
    }

    #[cfg(test)]
    pub(crate) fn pause_before_import_plan_publication_for_tests(&self) {
        self.pause_test_hook(&self.import_plan_before_state_hook);
    }

    #[cfg(test)]
    fn pause_test_hook(&self, hook: &Arc<Mutex<Option<Arc<WorkerPauseHook>>>>) {
        let hook = hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        if let Some(hook) = hook {
            hook.pause();
        }
    }

    #[cfg(test)]
    pub(crate) fn active_running_leases_for_tests(&self) -> usize {
        self.active_running_leases.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn scan_workloads_started_for_tests(&self) -> usize {
        self.scan_workloads_started.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn verify_private_state_attempts_for_tests(&self) -> usize {
        self.verify_private_state_attempts.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn import_plan_preflight_attempts_for_tests(&self) -> usize {
        self.import_plan_preflight_attempts.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn seed_completed_candidate_for_shutdown_tests(
        &self,
        owner: &DiscoveryOwnerScope,
        scan_id: &str,
        candidate_id: &str,
    ) {
        let projection = CandidateProjection {
            candidate_id: candidate_id.to_owned(),
            category: CandidateCategory::AgentRuntime,
            connector_id: "fixture-acp".into(),
            runtime_type: "acp".into(),
            display_name: "Fixture ACP".into(),
            availability: CandidateAvailability::Unconfigured,
            models: Vec::new(),
            catalog_revision: None,
            requires_configuration: true,
            source_kind: ObservationSourceKind::ExecutableInventory,
            source_kinds: vec![ObservationSourceKind::ExecutableInventory],
            trust_level: ObservationTrustLevel::FirstParty,
            verification_authority: VerificationAuthority::Authoritative,
            availability_authority: VerificationAuthority::Authoritative,
            discovery_authority: VerificationAuthority::Authoritative,
            compatibility_authority: VerificationAuthority::Authoritative,
            auth_authority: VerificationAuthority::Authoritative,
            health_authority: VerificationAuthority::Authoritative,
            catalog_source_kind: None,
            catalog_trust_level: None,
            catalog_authority: None,
            discovery_state: DiscoveryState::Identified,
            compatibility_state: CompatibilityState::NotVerified,
            auth_state: AuthState::Unknown,
            health_state: HealthState::NotChecked,
            evidence_summary: Vec::new(),
            diagnostics: Vec::new(),
        };
        let mut state = lock_state(&self.state);
        state.sessions.insert(
            scan_id.to_owned(),
            DiscoverySession {
                owner: owner.clone(),
                expires_at: Instant::now() + SESSION_LIFETIME,
                terminal_at: Some(Instant::now()),
                scan_cancelled: Arc::new(AtomicBool::new(false)),
                verification_cancellations: BTreeMap::new(),
                status: SessionStatus::Completed,
                candidates: BTreeMap::from([(
                    candidate_id.to_owned(),
                    CandidateState {
                        projection,
                        has_acp_binding: true,
                        verification: None,
                        dismissed: false,
                        verifying: false,
                    },
                )]),
                diagnostics: Vec::new(),
                acp_session: None,
            },
        );
    }

    #[cfg(test)]
    pub(crate) fn candidate_flags_for_tests(
        &self,
        owner: &DiscoveryOwnerScope,
        scan_id: &str,
        candidate_id: &str,
    ) -> Option<(bool, bool, bool)> {
        let state = lock_state(&self.state);
        let session = state.sessions.get(scan_id)?;
        if &session.owner != owner {
            return None;
        }
        let candidate = session.candidates.get(candidate_id)?;
        Some((
            candidate.dismissed,
            candidate.verifying,
            session
                .verification_cancellations
                .contains_key(candidate_id),
        ))
    }

    #[cfg(test)]
    fn expire_all_sessions_for_tests(&self) {
        let mut state = lock_state(&self.state);
        let expired_at = Instant::now() - Duration::from_millis(1);
        for session in state.sessions.values_mut() {
            session.expires_at = expired_at;
        }
        prune_expired(&mut state);
    }

    pub(crate) fn dismiss_with_publication(
        &self,
        _publication: &DiscoveryStartPublicationGuard<'_>,
        owner: &DiscoveryOwnerScope,
        request_id: &str,
        scan_id: &str,
        candidate_id: &str,
        event_sink: EventSink,
    ) -> Result<Value, LocalDiscoveryServiceError> {
        if !is_safe_id(scan_id) || !is_safe_id(candidate_id) {
            return Err(LocalDiscoveryServiceError::InvalidPayload);
        }
        let payload = json!({"scanId": scan_id, "candidateId": candidate_id});
        let request_key = DiscoveryRequestKey {
            owner: owner.clone(),
            command: "agent.discovery.dismiss".into(),
            request_id: request_id.to_owned(),
        };
        let payload_hash = request_hash(&payload);
        let response = json!({
            "scanId": scan_id,
            "candidateId": candidate_id,
            "dismissed": true,
        });
        {
            let mut state = lock_state(&self.state);
            prune_expired(&mut state);
            let accepting_starts = state.accepting_starts;
            {
                let session = state
                    .sessions
                    .get(scan_id)
                    .ok_or(LocalDiscoveryServiceError::ScanNotFound)?;
                ensure_owner(session, owner)?;
                if !accepting_starts
                    || session.status == SessionStatus::Cancelled
                    || session.scan_cancelled.load(Ordering::Acquire)
                {
                    return Err(LocalDiscoveryServiceError::ShuttingDown);
                }
            }
            if let Some(existing) = state.requests.get(&request_key) {
                return replay_or_reject(existing, &payload_hash);
            }
            // W5.8 validation phase: a candidate that is already dismissed is a
            // business no-op that must not write another receipt or emit
            // another event, regardless of requestId. The borrows end before
            // the capacity check below.
            {
                let session = state
                    .sessions
                    .get_mut(scan_id)
                    .ok_or(LocalDiscoveryServiceError::ScanNotFound)?;
                let candidate = session
                    .candidates
                    .get_mut(candidate_id)
                    .ok_or(LocalDiscoveryServiceError::CandidateNotFound)?;
                if candidate.dismissed {
                    let mut already = response;
                    already["alreadyDismissed"] = json!(true);
                    return Ok(already);
                }
            }
            ensure_receipt_capacity(&state, scan_id, owner, &self.limits)?;
            let session = state
                .sessions
                .get_mut(scan_id)
                .ok_or(LocalDiscoveryServiceError::ScanNotFound)?;
            let candidate = session
                .candidates
                .get_mut(candidate_id)
                .ok_or(LocalDiscoveryServiceError::CandidateNotFound)?;
            candidate.dismissed = true;
            if let Some(cancelled) = session.verification_cancellations.get(candidate_id) {
                cancelled.store(true, Ordering::Release);
            }
            state.requests.insert(
                request_key,
                DiscoveryRequestReceipt {
                    payload_hash,
                    state: DiscoveryRequestReceiptState::Committed {
                        response: response.clone(),
                    },
                },
            );
        }
        self.emit(
            &event_sink,
            "agent.discovery.candidate_verified",
            scan_id,
            json!({
                "scanId": scan_id,
                "candidateId": candidate_id,
                "dismissed": true,
                "status": "dismissed",
                "reason": "cancelled",
            }),
        );
        Ok(response)
    }

    pub(crate) fn snapshot(
        &self,
        owner: &DiscoveryOwnerScope,
        scan_id: &str,
    ) -> Result<Value, LocalDiscoveryServiceError> {
        if !is_safe_id(scan_id) {
            return Err(LocalDiscoveryServiceError::InvalidPayload);
        }
        let mut state = lock_state(&self.state);
        prune_expired(&mut state);
        let session = state
            .sessions
            .get(scan_id)
            .ok_or(LocalDiscoveryServiceError::ScanNotFound)?;
        ensure_owner(session, owner)?;
        Ok(snapshot_value(scan_id, session))
    }

    pub(crate) fn begin_import_plan_with_publication(
        &self,
        _publication: &DiscoveryStartPublicationGuard<'_>,
        owner: &DiscoveryOwnerScope,
        scan_id: &str,
        candidate_id: &str,
        project_id: &str,
        model_selection: Option<&str>,
    ) -> Result<ImportPlanWork, LocalDiscoveryServiceError> {
        if !is_safe_id(scan_id)
            || !is_safe_id(candidate_id)
            || !is_safe_id(project_id)
            || model_selection.is_some_and(|model| !is_safe_id(model))
        {
            return Err(LocalDiscoveryServiceError::InvalidPayload);
        }
        let key = ImportPlanKey {
            scan_id: scan_id.to_owned(),
            candidate_id: candidate_id.to_owned(),
            project_id: project_id.to_owned(),
            model_selection: model_selection.map(str::to_owned),
        };
        // W5.8: the in-flight lease is acquired atomically inside the state
        // lock, before any private ACP read or metadata/identity recheck. The
        // guard is released before the fallible private reads below so that a
        // rejection drops the RAII lease without re-locking the same guard.
        let lease = {
            let mut state = lock_state(&self.state);
            prune_expired(&mut state);
            let accepting_starts = state.accepting_starts;
            {
                let session = state
                    .sessions
                    .get(scan_id)
                    .ok_or(LocalDiscoveryServiceError::ScanNotFound)?;
                ensure_owner(session, owner)?;
                if !accepting_starts
                    || session.status == SessionStatus::Cancelled
                    || session.scan_cancelled.load(Ordering::Acquire)
                {
                    return Err(LocalDiscoveryServiceError::ShuttingDown);
                }
            }
            ensure_import_plan_capacity(&state, &key, owner, &self.limits)?;
            state
                .inflight_import_plans
                .insert(key.clone(), owner.clone());
            ImportPlanLease {
                service: self.clone(),
                key: key.clone(),
            }
        };
        let (acp_session, verification, cancelled, projection) = {
            let state = lock_state(&self.state);
            #[cfg(test)]
            self.import_plan_preflight_attempts
                .fetch_add(1, Ordering::AcqRel);
            let session = state
                .sessions
                .get(scan_id)
                .ok_or(LocalDiscoveryServiceError::ScanNotFound)?;
            if !state.accepting_starts
                || session.status == SessionStatus::Cancelled
                || session.scan_cancelled.load(Ordering::Acquire)
            {
                return Err(LocalDiscoveryServiceError::ShuttingDown);
            }
            let candidate = session
                .candidates
                .get(candidate_id)
                .ok_or(LocalDiscoveryServiceError::CandidateNotFound)?;
            if candidate.dismissed {
                return Err(LocalDiscoveryServiceError::CandidateDismissed);
            }
            let verification = candidate
                .verification
                .clone()
                .ok_or(LocalDiscoveryServiceError::CandidateNotVerified)?;
            if !matches!(
                verification.report().status,
                AcpVerificationStatus::Verified | AcpVerificationStatus::AuthRequired
            ) {
                return Err(LocalDiscoveryServiceError::CandidateNotVerified);
            }
            (
                session
                    .acp_session
                    .clone()
                    .ok_or(LocalDiscoveryServiceError::AdapterRequired)?,
                verification,
                Arc::clone(&session.scan_cancelled),
                candidate.projection.clone(),
            )
        };
        Ok(ImportPlanWork {
            scan_id: scan_id.to_owned(),
            candidate_id: candidate_id.to_owned(),
            project_id: project_id.to_owned(),
            model_selection: model_selection.map(str::to_owned),
            acp_session,
            verification,
            cancelled,
            projection,
            _lease: lease,
        })
    }

    pub(crate) fn execute_import_plan(
        &self,
        work: ImportPlanWork,
    ) -> Result<Value, LocalDiscoveryServiceError> {
        // Bounded dev-mode-only hold that keeps the in-flight lease occupied
        // long enough for Named Pipe flood tests to observe the per-owner and
        // global ceilings deterministically. It is zero in production and
        // cannot be enabled without AGENTTALK_CORE_DEV_MODE=1.
        if !self.configuration.import_plan_hold.is_zero() {
            thread::sleep(self.configuration.import_plan_hold);
        }
        let consent = AcpVerificationConsent::for_candidate(&work.candidate_id);
        let metadata = work.acp_session.import_plan_metadata(
            &consent,
            &work.verification,
            Instant::now() + DEFAULT_VERIFY_TIMEOUT,
            &work.cancelled,
        );
        let metadata = metadata.map_err(|_| LocalDiscoveryServiceError::IdentityChanged)?;
        Ok(import_plan_value(
            &work.scan_id,
            &work.project_id,
            work.model_selection.as_deref(),
            &work.projection,
            &metadata,
        ))
    }

    /// Re-checks the same private ACP identity immediately before W6 durable
    /// writes. A previous read-only plan is never accepted as a write token.
    pub(crate) fn execute_local_import(
        &self,
        work: ImportPlanWork,
    ) -> Result<LocalImportWork, LocalDiscoveryServiceError> {
        let consent = AcpVerificationConsent::for_candidate(&work.candidate_id);
        let metadata = work
            .acp_session
            .import_plan_metadata(
                &consent,
                &work.verification,
                Instant::now() + DEFAULT_VERIFY_TIMEOUT,
                &work.cancelled,
            )
            .map_err(|_| LocalDiscoveryServiceError::IdentityChanged)?;
        Ok(LocalImportWork {
            scan_id: work.scan_id,
            candidate_id: work.candidate_id,
            project_id: work.project_id,
            model_selection: work.model_selection,
            projection: work.projection,
            metadata,
            _lease: work._lease,
        })
    }

    pub(crate) fn ensure_import_plan_publishable(
        &self,
        publication: &DiscoveryStartPublicationGuard<'_>,
        owner: &DiscoveryOwnerScope,
        scan_id: &str,
    ) -> Result<(), LocalDiscoveryServiceError> {
        publication.ensure_mutation_publishable(scan_id, owner)
    }

    pub(crate) fn cancel_all(&self) {
        let publication = self.start_publication_guard();
        publication.cancel_all();
    }

    fn cancel_all_locked(&self) {
        let mut state = lock_state(&self.state);
        state.accepting_starts = false;
        state.shutdown_generation = state.shutdown_generation.saturating_add(1);
        let waiters = std::mem::take(&mut state.lease_waiters);
        let verification_waiters = std::mem::take(&mut state.verification_waiters);
        let unpublished_scan_ids = state
            .requests
            .values()
            .filter_map(|receipt| match &receipt.state {
                DiscoveryRequestReceiptState::PendingStart { scan_id }
                | DiscoveryRequestReceiptState::WorkerReady { scan_id, .. } => {
                    Some(scan_id.clone())
                }
                DiscoveryRequestReceiptState::PendingVerification { .. }
                | DiscoveryRequestReceiptState::VerificationWorkerReady { .. }
                | DiscoveryRequestReceiptState::Committed { .. } => None,
            })
            .collect::<Vec<_>>();
        for scan_id in unpublished_scan_ids {
            if let Some(session) = state.sessions.remove(&scan_id) {
                session.scan_cancelled.store(true, Ordering::Release);
                for cancellation in session.verification_cancellations.values() {
                    cancellation.store(true, Ordering::Release);
                }
            }
            state.running_scans.remove(&scan_id);
        }
        let unpublished_verifications = state
            .requests
            .iter()
            .filter_map(|(key, receipt)| {
                matches!(
                    receipt.state,
                    DiscoveryRequestReceiptState::PendingVerification { .. }
                        | DiscoveryRequestReceiptState::VerificationWorkerReady { .. }
                )
                .then_some(key.clone())
            })
            .collect::<Vec<_>>();
        for request_key in unpublished_verifications {
            if let Some(receipt) = state.requests.remove(&request_key) {
                let (scan_id, candidate_id) = match receipt.state {
                    DiscoveryRequestReceiptState::PendingVerification {
                        scan_id,
                        candidate_id,
                    }
                    | DiscoveryRequestReceiptState::VerificationWorkerReady {
                        scan_id,
                        candidate_id,
                        ..
                    } => (scan_id, candidate_id),
                    _ => continue,
                };
                let key = VerificationKey {
                    scan_id: scan_id.clone(),
                    candidate_id: candidate_id.clone(),
                };
                state.running_verifications.remove(&key);
                if let Some(session) = state.sessions.get_mut(&scan_id) {
                    if let Some(candidate) = session.candidates.get_mut(&candidate_id) {
                        candidate.verifying = false;
                    }
                    if let Some(cancellation) =
                        session.verification_cancellations.remove(&candidate_id)
                    {
                        cancellation.store(true, Ordering::Release);
                    }
                }
            }
        }
        state.running_scans.clear();
        state.requests.retain(|_, receipt| {
            !matches!(
                receipt.state,
                DiscoveryRequestReceiptState::PendingStart { .. }
                    | DiscoveryRequestReceiptState::WorkerReady { .. }
            )
        });
        for session in state.sessions.values_mut() {
            session.scan_cancelled.store(true, Ordering::Release);
            for cancellation in session.verification_cancellations.values() {
                cancellation.store(true, Ordering::Release);
            }
            for candidate in session.candidates.values_mut() {
                candidate.verifying = false;
            }
            session.status = SessionStatus::Cancelled;
            session.terminal_at = Some(Instant::now());
        }
        state.running_verifications.clear();
        // In-flight import-plan leases are cancelled together with the
        // sessions they belong to; running rechecks observe the cancelled
        // flag and the RAII leases no-op on an already-cleared map.
        state.inflight_import_plans.clear();
        drop(state);
        for waiter in waiters.into_values() {
            let _ = waiter.try_send(WorkerLeaseAck::Unavailable);
        }
        for waiter in verification_waiters.into_values() {
            let _ = waiter.try_send(VerificationLeaseAck::Unavailable);
        }
    }

    pub(crate) fn recoverable_owners(&self) -> BTreeSet<DiscoveryOwnerScope> {
        let mut state = lock_state(&self.state);
        prune_expired(&mut state);
        state
            .sessions
            .values()
            .map(|session| session.owner.clone())
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn mark_owner_active_for_tests(&self, owner: &DiscoveryOwnerScope) {
        let mut state = lock_state(&self.state);
        let scan_id = format!("test-active-owner-{}", state.sessions.len());
        state.sessions.insert(
            scan_id,
            DiscoverySession {
                owner: owner.clone(),
                expires_at: Instant::now() + SESSION_LIFETIME,
                terminal_at: None,
                scan_cancelled: Arc::new(AtomicBool::new(false)),
                verification_cancellations: BTreeMap::new(),
                status: SessionStatus::Running,
                candidates: BTreeMap::new(),
                diagnostics: Vec::new(),
                acp_session: None,
            },
        );
    }

    #[cfg(test)]
    pub(crate) fn clear_owner_activity_for_tests(&self, owner: &DiscoveryOwnerScope) {
        let mut state = lock_state(&self.state);
        state.sessions.retain(|_, session| &session.owner != owner);
        state.requests.retain(|key, _| &key.owner != owner);
    }

    fn run_scan_with_lease(
        &self,
        scan_id: String,
        owner: DiscoveryOwnerScope,
        event_sink: EventSink,
        _running_scan: RunningScanLease,
        explicit_sources: Option<Vec<ExplicitDiscoverySource>>,
    ) {
        let cancelled = {
            let state = lock_state(&self.state);
            let Some(session) = state.sessions.get(&scan_id) else {
                return;
            };
            if session.owner != owner {
                return;
            }
            Arc::clone(&session.scan_cancelled)
        };
        if cancelled.load(Ordering::Acquire) {
            return;
        }
        #[cfg(test)]
        self.scan_workloads_started.fetch_add(1, Ordering::AcqRel);
        if matches!(
            self.configuration.catalog,
            CatalogConfiguration::Unavailable
        ) {
            self.finish_scan_failure(
                &scan_id,
                &owner,
                &event_sink,
                DiscoveryDiagnosticCode::InvalidSourceRecord,
            );
            return;
        }
        let scan_config = explicit_sources.map_or_else(
            || self.configuration.scan.clone(),
            |explicit_sources| {
                let mut config = self.configuration.scan.clone();
                config.path_env = None;
                config.app_path_records.clear();
                config.package_records.clear();
                config.loopback_records.clear();
                config.loopback_recheck_records = None;
                config.use_real_app_paths = false;
                config.use_real_packages = false;
                config.use_real_loopback = false;
                config.explicit_sources = explicit_sources;
                config
            },
        );
        let report =
            discover_windows_passive_report_with_config_and_cancelled(&scan_config, &cancelled);
        if cancelled.load(Ordering::Acquire) {
            self.finish_scan_cancelled(&scan_id, &owner, &event_sink);
            return;
        }
        let manifests = match &self.configuration.catalog {
            CatalogConfiguration::Available(snapshot) => snapshot.manifests.clone(),
            CatalogConfiguration::Unavailable => Vec::new(),
        };
        let acp_session = report.classify_acp(
            &manifests,
            Instant::now() + DEFAULT_SCAN_TIMEOUT,
            &cancelled,
        );
        if cancelled.load(Ordering::Acquire) {
            self.finish_scan_cancelled(&scan_id, &owner, &event_sink);
            return;
        }
        let mut candidates = report
            .projections
            .into_iter()
            .map(|projection| (projection.candidate_id.clone(), projection))
            .collect::<BTreeMap<_, _>>();
        let acp_candidate_ids = acp_session
            .projections()
            .iter()
            .map(|projection| projection.candidate_id.clone())
            .collect::<BTreeSet<_>>();
        for projection in acp_session.projections() {
            candidates.insert(projection.candidate_id.clone(), projection.clone());
        }
        let mut diagnostics = report.diagnostics;
        diagnostics.extend(self.configuration.catalog_diagnostics.iter().cloned());
        diagnostics.extend(acp_session.diagnostics().iter().cloned());
        sort_diagnostics(&mut diagnostics);
        let mut candidate_states = BTreeMap::new();
        for (candidate_id, projection) in candidates {
            candidate_states.insert(
                candidate_id.clone(),
                CandidateState {
                    projection,
                    has_acp_binding: acp_candidate_ids.contains(&candidate_id),
                    verification: None,
                    dismissed: false,
                    verifying: false,
                },
            );
        }
        {
            let mut state = lock_state(&self.state);
            let Some(session) = state.sessions.get_mut(&scan_id) else {
                return;
            };
            if session.owner != owner {
                return;
            }
            if session.scan_cancelled.load(Ordering::Acquire)
                || session.expires_at <= Instant::now()
            {
                session.status = SessionStatus::Cancelled;
                session.terminal_at = Some(Instant::now());
                return;
            }
            session.status = SessionStatus::Completed;
            session.terminal_at = Some(Instant::now());
            session.candidates = candidate_states;
            session.diagnostics = diagnostics;
            session.acp_session = Some(acp_session);
        };
        let snapshot = match self.snapshot(&owner, &scan_id) {
            Ok(snapshot) => snapshot,
            Err(_) => return,
        };
        let candidates = snapshot["candidates"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        for candidate in candidates {
            self.emit(
                &event_sink,
                "agent.discovery.candidate_observed",
                &scan_id,
                json!({
                    "scanId": scan_id,
                    "candidateId": candidate["candidateId"],
                }),
            );
        }
        for candidate in snapshot["candidates"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|candidate| candidate["candidate"]["runtimeType"] == "acp")
        {
            self.emit(
                &event_sink,
                "agent.discovery.candidate_classified",
                &scan_id,
                json!({
                    "scanId": scan_id,
                    "candidateId": candidate["candidateId"],
                }),
            );
        }
        self.emit(
            &event_sink,
            "agent.discovery.completed",
            &scan_id,
            json!({
                "scanId": scan_id,
                "candidateCount": snapshot["candidates"].as_array().map_or(0, Vec::len),
            }),
        );
    }

    fn run_verify_with_lease(
        &self,
        work: VerificationWork,
        _running_verification: RunningVerificationLease,
    ) {
        if work.cancelled.load(Ordering::Acquire) {
            self.finish_verify_cancelled(&work);
            return;
        }
        let consent = AcpVerificationConsent::for_candidate(&work.candidate_id);
        let verification =
            match work
                .acp_session
                .verify(&consent, Instant::now() + work.deadline, &work.cancelled)
            {
                Ok(verification) => verification,
                Err(_) => {
                    self.finish_verify_binding_failure(
                        &work.scan_id,
                        &work.owner,
                        &work.candidate_id,
                        &work.event_sink,
                    );
                    return;
                }
            };
        let report = verification.report().clone();
        let (lifecycle_state, diagnostic) = {
            let mut state = lock_state(&self.state);
            let Some(session) = state.sessions.get_mut(&work.scan_id) else {
                return;
            };
            if session.owner != work.owner
                || session.status == SessionStatus::Cancelled
                || session.scan_cancelled.load(Ordering::Acquire)
            {
                return;
            }
            session
                .verification_cancellations
                .remove(&work.candidate_id);
            let Some(candidate) = session.candidates.get_mut(&work.candidate_id) else {
                return;
            };
            candidate.verifying = false;
            if candidate.dismissed {
                return;
            }
            apply_verification_projection(&mut candidate.projection, &report);
            candidate.verification = Some(verification);
            (
                candidate_lifecycle_state(candidate),
                report.diagnostic.map(verification_diagnostic_code),
            )
        };
        self.emit(
            &work.event_sink,
            "agent.discovery.candidate_verified",
            &work.scan_id,
            json!({
                "scanId": work.scan_id,
                "candidateId": work.candidate_id,
                "status": lifecycle_state,
                "diagnostic": diagnostic,
            }),
        );
    }

    fn finish_scan_failure(
        &self,
        scan_id: &str,
        owner: &DiscoveryOwnerScope,
        event_sink: &EventSink,
        code: DiscoveryDiagnosticCode,
    ) {
        {
            let mut state = lock_state(&self.state);
            let Some(session) = state.sessions.get_mut(scan_id) else {
                return;
            };
            if &session.owner != owner
                || session.status == SessionStatus::Cancelled
                || session.scan_cancelled.load(Ordering::Acquire)
            {
                return;
            }
            session.status = SessionStatus::Failed;
            session.terminal_at = Some(Instant::now());
            session.diagnostics.push(DiscoveryDiagnostic {
                source_kind: ObservationSourceKind::ExecutableInventory,
                code,
            });
            sort_diagnostics(&mut session.diagnostics);
        }
        self.emit(
            event_sink,
            "agent.discovery.failed",
            scan_id,
            json!({"scanId": scan_id, "code": "catalog_unavailable"}),
        );
    }

    fn finish_verify_binding_failure(
        &self,
        scan_id: &str,
        owner: &DiscoveryOwnerScope,
        candidate_id: &str,
        event_sink: &EventSink,
    ) {
        {
            let mut state = lock_state(&self.state);
            let Some(session) = state.sessions.get_mut(scan_id) else {
                return;
            };
            if &session.owner != owner
                || session.status == SessionStatus::Cancelled
                || session.scan_cancelled.load(Ordering::Acquire)
            {
                return;
            }
            session.verification_cancellations.remove(candidate_id);
            let Some(candidate) = session.candidates.get_mut(candidate_id) else {
                return;
            };
            candidate.verifying = false;
            candidate.projection.availability = CandidateAvailability::Unavailable;
            candidate.projection.compatibility_state = CompatibilityState::Incompatible;
            candidate.projection.auth_state = AuthState::Unknown;
            candidate.projection.health_state = HealthState::IdentityMismatch;
            candidate.projection.requires_configuration = true;
            candidate.projection.diagnostics.push(DiscoveryDiagnostic {
                source_kind: candidate.projection.source_kind,
                code: DiscoveryDiagnosticCode::InvalidIdentity,
            });
            sort_diagnostics(&mut candidate.projection.diagnostics);
        }
        self.emit(
            event_sink,
            "agent.discovery.candidate_verified",
            scan_id,
            json!({
                "scanId": scan_id,
                "candidateId": candidate_id,
                "status": "identity_changed",
            }),
        );
    }

    fn finish_verify_cancelled(&self, work: &VerificationWork) {
        let mut state = lock_state(&self.state);
        let Some(session) = state.sessions.get_mut(&work.scan_id) else {
            return;
        };
        if session.owner != work.owner {
            return;
        }
        session
            .verification_cancellations
            .remove(&work.candidate_id);
        if let Some(candidate) = session.candidates.get_mut(&work.candidate_id) {
            candidate.verifying = false;
        }
    }

    fn finish_scan_cancelled(
        &self,
        scan_id: &str,
        owner: &DiscoveryOwnerScope,
        event_sink: &EventSink,
    ) {
        {
            let mut state = lock_state(&self.state);
            let Some(session) = state.sessions.get_mut(scan_id) else {
                return;
            };
            if &session.owner != owner {
                return;
            }
            if session.status == SessionStatus::Cancelled {
                return;
            }
            session.status = SessionStatus::Cancelled;
            session.terminal_at = Some(Instant::now());
        }
        self.emit(
            event_sink,
            "agent.discovery.failed",
            scan_id,
            json!({"scanId": scan_id, "code": "cancelled"}),
        );
    }

    fn emit(&self, event_sink: &EventSink, event_type: &str, scan_id: &str, payload: Value) {
        let sequence = self.next_id.fetch_add(1, Ordering::AcqRel);
        event_sink(RuntimeEvent {
            event_id: format!("discovery-{sequence}"),
            execution_run_id: format!("discovery-{scan_id}"),
            runtime_id: "local-discovery".into(),
            thread_id: None,
            turn_id: None,
            sequence: 0,
            event_type: event_type.into(),
            timestamp_ms: unix_time_ms(),
            payload,
        });
    }

    fn new_scan_id(&self) -> Result<String, LocalDiscoveryServiceError> {
        let mut random = [0u8; 32];
        fill_random(&mut random).map_err(|_| LocalDiscoveryServiceError::EntropyUnavailable)?;
        Ok(format!(
            "scan-{}",
            random
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        ))
    }
}

fn parse_start_explicit_sources(
    payload: &Value,
) -> Result<Option<Vec<ExplicitDiscoverySource>>, LocalDiscoveryServiceError> {
    let Some(object) = payload.as_object() else {
        return Err(LocalDiscoveryServiceError::InvalidPayload);
    };
    if object.is_empty() {
        return Ok(None);
    }
    if object.len() != 1 {
        return Err(LocalDiscoveryServiceError::InvalidPayload);
    }
    let Some(value) = object.get("explicitExecutablePath") else {
        return Err(LocalDiscoveryServiceError::InvalidPayload);
    };
    let Some(path) = value.as_str() else {
        return Err(LocalDiscoveryServiceError::InvalidPayload);
    };
    if path.is_empty()
        || path.len() > 32 * 1024
        || path.contains('\0')
        || !Path::new(path).is_absolute()
    {
        return Err(LocalDiscoveryServiceError::InvalidPayload);
    }
    Ok(Some(vec![ExplicitDiscoverySource::Executable(
        PathBuf::from(path),
    )]))
}

impl StartScanReservation {
    #[cfg(test)]
    fn launch(self, event_sink: EventSink) -> Result<Value, LocalDiscoveryServiceError> {
        let mut ready =
            self.launch_worker_until_ready_with_hooks(event_sink, false, false, false)?;
        let owner = ready.reservation.request_key.owner.clone();
        let service = ready.reservation.service.clone();
        let publication = service.start_publication_guard();
        ready.publish_with(&publication, &owner)
    }

    #[cfg(test)]
    fn launch_with_mode_for_tests(
        self,
        event_sink: EventSink,
        mode: StartLaunchMode,
    ) -> Result<Value, LocalDiscoveryServiceError> {
        self.launch_with_hooks(
            event_sink,
            mode == StartLaunchMode::FailSpawn,
            mode == StartLaunchMode::FailReadiness,
            mode == StartLaunchMode::ShutdownAfterReadyBeforeCommit,
            mode == StartLaunchMode::ShutdownAfterCommitBeforePublish,
        )
    }

    #[cfg(test)]
    fn launch_with_hooks(
        self,
        event_sink: EventSink,
        fail_spawn: bool,
        fail_readiness: bool,
        shutdown_after_ready_before_commit: bool,
        shutdown_after_commit_before_publish: bool,
    ) -> Result<Value, LocalDiscoveryServiceError> {
        let mut ready = self.launch_worker_until_ready_with_hooks(
            event_sink,
            fail_spawn,
            fail_readiness,
            shutdown_after_ready_before_commit,
        )?;
        let owner = ready.reservation.request_key.owner.clone();
        let service = ready.reservation.service.clone();
        if shutdown_after_commit_before_publish {
            service.cancel_all();
        }
        let publication = service.start_publication_guard();
        ready.publish_with(&publication, &owner)
    }

    pub(crate) fn launch_worker_until_ready(
        self,
        event_sink: EventSink,
    ) -> Result<WorkerReadyStart, LocalDiscoveryServiceError> {
        self.launch_worker_until_ready_with_hooks(event_sink, false, false, false)
    }

    fn launch_worker_until_ready_with_hooks(
        self,
        event_sink: EventSink,
        fail_spawn: bool,
        fail_readiness: bool,
        shutdown_after_ready_before_commit: bool,
    ) -> Result<WorkerReadyStart, LocalDiscoveryServiceError> {
        let scan_id = self.scan_id.clone();
        let owner = self.request_key.owner.clone();
        let service = self.service.clone();
        if fail_spawn {
            service.rollback_start_reservation(&scan_id, &self.request_key, &self.payload_hash);
            return Err(LocalDiscoveryServiceError::ScanWorkerUnavailable);
        }
        let worker_service = self.service.clone();
        let worker_scan_id = scan_id.clone();
        let worker_owner = owner.clone();
        let worker_event_sink = Arc::clone(&event_sink);
        let worker_explicit_sources = self.explicit_sources.clone();
        let gate = Arc::new(WorkerStartGate::new());
        let worker_gate = Arc::clone(&gate);
        let publication_latch = Arc::new(WorkerPublicationLatch::new());
        let worker_publication_latch = Arc::clone(&publication_latch);
        let (ready_sender, ready_receiver) = mpsc::sync_channel(0);
        let (lease_sender, lease_receiver) = mpsc::sync_channel(1);
        let worker_lease_sender = lease_sender.clone();
        let worker = thread::Builder::new()
            .name("agenttalk-local-discovery-scan".into())
            .spawn(move || {
                let _ = ready_sender.send(());
                if worker_gate.wait_to_start() {
                    #[cfg(test)]
                    worker_service.pause_before_running_lease_for_tests();
                    let Some(running_scan) =
                        worker_service.acquire_running_scan_lease(&worker_scan_id, &worker_owner)
                    else {
                        let _ = worker_lease_sender.try_send(WorkerLeaseAck::Unavailable);
                        return;
                    };
                    if worker_lease_sender
                        .try_send(WorkerLeaseAck::Acquired)
                        .is_err()
                    {
                        return;
                    }
                    #[cfg(test)]
                    worker_service.pause_after_running_lease_for_tests();
                    if worker_publication_latch.wait_to_run() {
                        worker_service.run_scan_with_lease(
                            worker_scan_id,
                            worker_owner,
                            worker_event_sink,
                            running_scan,
                            worker_explicit_sources,
                        );
                    }
                }
            });
        if worker.is_err() {
            service.rollback_start_reservation(&scan_id, &self.request_key, &self.payload_hash);
            return Err(LocalDiscoveryServiceError::ScanWorkerUnavailable);
        }
        if fail_readiness {
            gate.abort();
            service.rollback_start_reservation(&scan_id, &self.request_key, &self.payload_hash);
            return Err(LocalDiscoveryServiceError::ScanWorkerUnavailable);
        }
        if ready_receiver.recv_timeout(Duration::from_secs(2)).is_err() {
            gate.abort();
            service.rollback_start_reservation(&scan_id, &self.request_key, &self.payload_hash);
            return Err(LocalDiscoveryServiceError::ScanWorkerUnavailable);
        }
        if shutdown_after_ready_before_commit {
            service.cancel_all();
        }
        if let Err(error) = service.mark_start_worker_ready(
            &scan_id,
            &self.request_key,
            &self.payload_hash,
            &self.response,
            self.shutdown_generation,
            lease_sender.clone(),
        ) {
            gate.abort();
            service.rollback_start_reservation(&scan_id, &self.request_key, &self.payload_hash);
            return Err(error);
        }
        Ok(WorkerReadyStart {
            reservation: self,
            gate,
            lease_receiver,
            publication_latch,
            event_sink,
            worker_started: false,
            lease_acquired: false,
            published: false,
        })
    }
}

impl VerifyReservation {
    pub(crate) fn launch_worker_until_ready(
        self,
    ) -> Result<WorkerReadyVerify, LocalDiscoveryServiceError> {
        let scan_id = self.scan_id.clone();
        let candidate_id = self.candidate_id.clone();
        let owner = self.request_key.owner.clone();
        let service = self.service.clone();
        let worker_service = self.service.clone();
        let worker_work = self.work.clone();
        let worker_scan_id = scan_id.clone();
        let worker_candidate_id = candidate_id.clone();
        let worker_owner = owner.clone();
        let gate = Arc::new(WorkerStartGate::new());
        let worker_gate = Arc::clone(&gate);
        let publication_latch = Arc::new(WorkerPublicationLatch::new());
        let worker_publication_latch = Arc::clone(&publication_latch);
        let (ready_sender, ready_receiver) = mpsc::sync_channel(0);
        let (lease_sender, lease_receiver) = mpsc::sync_channel(1);
        let worker_lease_sender = lease_sender.clone();
        let worker = thread::Builder::new()
            .name("agenttalk-local-discovery-verify".into())
            .spawn(move || {
                let _ = ready_sender.send(());
                if !worker_gate.wait_to_start() {
                    return;
                }
                let Some(running_verification) = worker_service.acquire_running_verification_lease(
                    &worker_scan_id,
                    &worker_candidate_id,
                    &worker_owner,
                ) else {
                    let _ = worker_lease_sender.try_send(VerificationLeaseAck::Unavailable);
                    return;
                };
                if worker_lease_sender
                    .try_send(VerificationLeaseAck::Acquired)
                    .is_err()
                {
                    return;
                }
                if worker_publication_latch.wait_to_run() {
                    worker_service.run_verify_with_lease(worker_work, running_verification);
                }
            });
        if worker.is_err() {
            service.rollback_verify_reservation(
                &scan_id,
                &candidate_id,
                &self.request_key,
                &self.payload_hash,
            );
            return Err(LocalDiscoveryServiceError::ScanWorkerUnavailable);
        }
        if ready_receiver.recv_timeout(Duration::from_secs(2)).is_err() {
            gate.abort();
            service.rollback_verify_reservation(
                &scan_id,
                &candidate_id,
                &self.request_key,
                &self.payload_hash,
            );
            return Err(LocalDiscoveryServiceError::ScanWorkerUnavailable);
        }
        if let Err(error) = service.mark_verify_worker_ready(&self, lease_sender) {
            gate.abort();
            service.rollback_verify_reservation(
                &scan_id,
                &candidate_id,
                &self.request_key,
                &self.payload_hash,
            );
            return Err(error);
        }
        Ok(WorkerReadyVerify {
            reservation: self,
            gate,
            lease_receiver,
            publication_latch,
            worker_started: false,
            lease_acquired: false,
            published: false,
        })
    }
}

impl WorkerReadyStart {
    pub(crate) fn scan_id(&self) -> &str {
        &self.reservation.scan_id
    }

    pub(crate) fn response(&self) -> Value {
        self.reservation.response.clone()
    }

    #[cfg(test)]
    pub(crate) fn publish_with(
        &mut self,
        publication: &DiscoveryStartPublicationGuard<'_>,
        owner: &DiscoveryOwnerScope,
    ) -> Result<Value, LocalDiscoveryServiceError> {
        self.publish_with_timeout(publication, owner, WORKER_LEASE_ACK_TIMEOUT)
    }

    #[cfg(test)]
    fn publish_with_timeout(
        &mut self,
        publication: &DiscoveryStartPublicationGuard<'_>,
        owner: &DiscoveryOwnerScope,
        lease_ack_timeout: Duration,
    ) -> Result<Value, LocalDiscoveryServiceError> {
        publication.ensure_worker_ready_publishable(&self.reservation.scan_id, owner)?;
        self.start_worker();
        self.wait_for_running_lease_with_timeout(lease_ack_timeout)?;
        self.publish_after_running_lease_with(publication, owner)
    }

    pub(crate) fn start_worker(&mut self) {
        if !self.worker_started {
            self.worker_started = true;
            self.gate.start();
        }
    }

    pub(crate) fn wait_for_running_lease(&mut self) -> Result<(), LocalDiscoveryServiceError> {
        self.wait_for_running_lease_with_timeout(WORKER_LEASE_ACK_TIMEOUT)
    }

    fn wait_for_running_lease_with_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<(), LocalDiscoveryServiceError> {
        self.start_worker();
        if self.lease_acquired {
            return Ok(());
        }
        match self.lease_receiver.recv_timeout(timeout) {
            Ok(WorkerLeaseAck::Acquired) => {
                self.lease_acquired = true;
                Ok(())
            }
            Ok(WorkerLeaseAck::Unavailable) => {
                self.abort_and_rollback();
                Err(LocalDiscoveryServiceError::ShuttingDown)
            }
            Err(_) => {
                self.abort_and_rollback();
                Err(LocalDiscoveryServiceError::ScanWorkerUnavailable)
            }
        }
    }

    pub(crate) fn publish_after_running_lease_with(
        &mut self,
        publication: &DiscoveryStartPublicationGuard<'_>,
        owner: &DiscoveryOwnerScope,
    ) -> Result<Value, LocalDiscoveryServiceError> {
        if !self.lease_acquired {
            self.abort_and_rollback();
            return Err(LocalDiscoveryServiceError::ScanWorkerUnavailable);
        }
        if let Err(error) =
            publication.publish_start(&self.reservation.scan_id, owner, &self.event_sink)
        {
            self.publication_latch.abort();
            self.reservation.service.rollback_start_reservation(
                &self.reservation.scan_id,
                &self.reservation.request_key,
                &self.reservation.payload_hash,
            );
            return Err(error);
        }
        self.reservation.launched = true;
        self.published = true;
        self.publication_latch.publish();
        Ok(self.reservation.response.clone())
    }

    pub(crate) fn abort_and_rollback(&mut self) {
        self.gate.abort();
        self.publication_latch.abort();
        self.reservation.service.rollback_start_reservation(
            &self.reservation.scan_id,
            &self.reservation.request_key,
            &self.reservation.payload_hash,
        );
    }
}

impl WorkerReadyVerify {
    pub(crate) fn scan_id(&self) -> &str {
        &self.reservation.scan_id
    }

    pub(crate) fn candidate_id(&self) -> &str {
        &self.reservation.candidate_id
    }

    pub(crate) fn start_worker(&mut self) {
        if !self.worker_started {
            self.worker_started = true;
            self.gate.start();
        }
    }

    pub(crate) fn wait_for_running_lease(&mut self) -> Result<(), LocalDiscoveryServiceError> {
        self.start_worker();
        if self.lease_acquired {
            return Ok(());
        }
        match self.lease_receiver.recv_timeout(WORKER_LEASE_ACK_TIMEOUT) {
            Ok(VerificationLeaseAck::Acquired) => {
                self.lease_acquired = true;
                Ok(())
            }
            Ok(VerificationLeaseAck::Unavailable) => {
                self.abort_and_rollback();
                Err(LocalDiscoveryServiceError::ShuttingDown)
            }
            Err(_) => {
                self.abort_and_rollback();
                Err(LocalDiscoveryServiceError::ScanWorkerUnavailable)
            }
        }
    }

    pub(crate) fn publish_after_running_lease_with(
        &mut self,
        publication: &DiscoveryStartPublicationGuard<'_>,
        owner: &DiscoveryOwnerScope,
    ) -> Result<Value, LocalDiscoveryServiceError> {
        if !self.lease_acquired {
            self.abort_and_rollback();
            return Err(LocalDiscoveryServiceError::ScanWorkerUnavailable);
        }
        if let Err(error) = publication.publish_verify(
            &self.reservation.scan_id,
            &self.reservation.candidate_id,
            owner,
        ) {
            self.publication_latch.abort();
            self.reservation.service.rollback_verify_reservation(
                &self.reservation.scan_id,
                &self.reservation.candidate_id,
                &self.reservation.request_key,
                &self.reservation.payload_hash,
            );
            return Err(error);
        }
        self.reservation.launched = true;
        self.published = true;
        self.publication_latch.publish();
        Ok(self.reservation.response.clone())
    }

    pub(crate) fn abort_and_rollback(&mut self) {
        self.gate.abort();
        self.publication_latch.abort();
        self.reservation.service.rollback_verify_reservation(
            &self.reservation.scan_id,
            &self.reservation.candidate_id,
            &self.reservation.request_key,
            &self.reservation.payload_hash,
        );
    }
}

impl Drop for WorkerReadyStart {
    fn drop(&mut self) {
        if !self.published {
            self.abort_and_rollback();
        }
    }
}

impl Drop for WorkerReadyVerify {
    fn drop(&mut self) {
        if !self.published {
            self.abort_and_rollback();
        }
    }
}

impl DiscoveryStartPublicationGuard<'_> {
    pub(crate) fn ensure_start_replay_publishable(
        &self,
        scan_id: &str,
        owner: &DiscoveryOwnerScope,
    ) -> Result<(), LocalDiscoveryServiceError> {
        self.service.ensure_start_replay_publishable(scan_id, owner)
    }

    pub(crate) fn ensure_worker_ready_publishable(
        &self,
        scan_id: &str,
        owner: &DiscoveryOwnerScope,
    ) -> Result<(), LocalDiscoveryServiceError> {
        self.service
            .ensure_worker_ready_publishable_locked(scan_id, owner)
    }

    pub(crate) fn ensure_verify_worker_ready_publishable(
        &self,
        scan_id: &str,
        candidate_id: &str,
        owner: &DiscoveryOwnerScope,
    ) -> Result<(), LocalDiscoveryServiceError> {
        self.service
            .ensure_verify_worker_ready_publishable_locked(scan_id, candidate_id, owner)
    }

    pub(crate) fn ensure_mutation_publishable(
        &self,
        scan_id: &str,
        owner: &DiscoveryOwnerScope,
    ) -> Result<(), LocalDiscoveryServiceError> {
        let mut state = lock_state(&self.service.state);
        prune_expired(&mut state);
        let session = state
            .sessions
            .get(scan_id)
            .ok_or(LocalDiscoveryServiceError::ScanNotFound)?;
        ensure_owner(session, owner)?;
        if !state.accepting_starts
            || session.status == SessionStatus::Cancelled
            || session.scan_cancelled.load(Ordering::Acquire)
        {
            return Err(LocalDiscoveryServiceError::ShuttingDown);
        }
        Ok(())
    }

    pub(crate) fn publish_start(
        &self,
        scan_id: &str,
        owner: &DiscoveryOwnerScope,
        event_sink: &EventSink,
    ) -> Result<(), LocalDiscoveryServiceError> {
        self.service
            .publish_start_locked(scan_id, owner, event_sink)
    }

    pub(crate) fn publish_verify(
        &self,
        scan_id: &str,
        candidate_id: &str,
        owner: &DiscoveryOwnerScope,
    ) -> Result<(), LocalDiscoveryServiceError> {
        self.service
            .publish_verify_locked(scan_id, candidate_id, owner)
    }

    pub(crate) fn cancel_all(&self) {
        self.service.cancel_all_locked();
    }
}

impl Drop for StartScanReservation {
    fn drop(&mut self) {
        if !self.launched {
            self.service.rollback_start_reservation(
                &self.scan_id,
                &self.request_key,
                &self.payload_hash,
            );
        }
    }
}

impl Drop for VerifyReservation {
    fn drop(&mut self) {
        if !self.launched {
            self.service.rollback_verify_reservation(
                &self.scan_id,
                &self.candidate_id,
                &self.request_key,
                &self.payload_hash,
            );
        }
    }
}

impl WorkerStartGate {
    fn new() -> Self {
        Self {
            state: Mutex::new(WorkerStartGateState::Waiting),
            changed: Condvar::new(),
        }
    }

    fn wait_to_start(&self) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while *state == WorkerStartGateState::Waiting {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        *state == WorkerStartGateState::Start
    }

    fn start(&self) {
        self.transition(WorkerStartGateState::Start);
    }

    fn abort(&self) {
        self.transition(WorkerStartGateState::Abort);
    }

    fn transition(&self, next: WorkerStartGateState) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *state = next;
        self.changed.notify_all();
    }
}

impl WorkerPublicationLatch {
    fn new() -> Self {
        Self {
            state: Mutex::new(WorkerPublicationState::Waiting),
            changed: Condvar::new(),
        }
    }

    fn wait_to_run(&self) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while *state == WorkerPublicationState::Waiting {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        *state == WorkerPublicationState::Published
    }

    fn publish(&self) {
        self.transition(WorkerPublicationState::Published);
    }

    fn abort(&self) {
        self.transition(WorkerPublicationState::Abort);
    }

    fn transition(&self, next: WorkerPublicationState) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *state = next;
        self.changed.notify_all();
    }
}

#[cfg(test)]
impl WorkerPauseHook {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(WorkerLeasePauseState {
                entered: false,
                released: false,
            }),
            changed: Condvar::new(),
        }
    }

    pub(crate) fn wait_until_entered(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while !state.entered {
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let remaining = deadline.saturating_duration_since(now);
            let (next_state, wait_result) = self
                .changed
                .wait_timeout(state, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state = next_state;
            if wait_result.timed_out() && !state.entered {
                return false;
            }
        }
        true
    }

    pub(crate) fn release(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.released = true;
        self.changed.notify_all();
    }

    pub(crate) fn pause_for_tests(&self) {
        self.pause();
    }

    fn pause(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.entered = true;
        self.changed.notify_all();
        while !state.released {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }
}

impl Drop for RunningScanLease {
    fn drop(&mut self) {
        self.service.release_running_scan(&self.scan_id);
        #[cfg(test)]
        self.active_running_leases.fetch_sub(1, Ordering::AcqRel);
    }
}

impl Drop for RunningVerificationLease {
    fn drop(&mut self) {
        let mut state = lock_state(&self.service.state);
        state.running_verifications.remove(&self.key);
    }
}

impl Drop for ImportPlanLease {
    fn drop(&mut self) {
        let mut state = lock_state(&self.service.state);
        state.inflight_import_plans.remove(&self.key);
    }
}

impl LocalDiscoveryLimits {
    fn from_environment() -> Self {
        if std::env::var("AGENTTALK_CORE_DEV_MODE").as_deref() != Ok("1") {
            return Self::production();
        }
        Self {
            max_sessions_per_owner: read_limit_override(
                "AGENTTALK_CORE_TEST_DISCOVERY_MAX_SESSIONS_PER_OWNER",
                DISCOVERY_MAX_SESSIONS_PER_OWNER,
            ),
            max_sessions_global: read_limit_override(
                "AGENTTALK_CORE_TEST_DISCOVERY_MAX_SESSIONS_GLOBAL",
                DISCOVERY_MAX_SESSIONS_GLOBAL,
            ),
            max_running_scans_per_owner: read_limit_override(
                "AGENTTALK_CORE_TEST_DISCOVERY_MAX_RUNNING_SCANS_PER_OWNER",
                DISCOVERY_MAX_RUNNING_SCANS_PER_OWNER,
            ),
            max_running_scans_global: read_limit_override(
                "AGENTTALK_CORE_TEST_DISCOVERY_MAX_RUNNING_SCANS_GLOBAL",
                DISCOVERY_MAX_RUNNING_SCANS_GLOBAL,
            ),
            max_receipts_per_session: read_limit_override(
                "AGENTTALK_CORE_TEST_DISCOVERY_MAX_RECEIPTS_PER_SESSION",
                DISCOVERY_MAX_RECEIPTS_PER_SESSION,
            ),
            max_receipts_per_owner: read_limit_override(
                "AGENTTALK_CORE_TEST_DISCOVERY_MAX_RECEIPTS_PER_OWNER",
                DISCOVERY_MAX_RECEIPTS_PER_OWNER,
            ),
            max_receipts_global: read_limit_override(
                "AGENTTALK_CORE_TEST_DISCOVERY_MAX_RECEIPTS_GLOBAL",
                DISCOVERY_MAX_RECEIPTS_GLOBAL,
            ),
            max_running_verifications_per_owner: read_limit_override(
                "AGENTTALK_CORE_TEST_DISCOVERY_MAX_RUNNING_VERIFICATIONS_PER_OWNER",
                DISCOVERY_MAX_RUNNING_VERIFICATIONS_PER_OWNER,
            ),
            max_running_verifications_global: read_limit_override(
                "AGENTTALK_CORE_TEST_DISCOVERY_MAX_RUNNING_VERIFICATIONS_GLOBAL",
                DISCOVERY_MAX_RUNNING_VERIFICATIONS_GLOBAL,
            ),
            max_inflight_import_plans_per_owner: read_limit_override(
                "AGENTTALK_CORE_TEST_DISCOVERY_MAX_INFLIGHT_IMPORT_PLANS_PER_OWNER",
                DISCOVERY_MAX_INFLIGHT_IMPORT_PLANS_PER_OWNER,
            ),
            max_inflight_import_plans_global: read_limit_override(
                "AGENTTALK_CORE_TEST_DISCOVERY_MAX_INFLIGHT_IMPORT_PLANS_GLOBAL",
                DISCOVERY_MAX_INFLIGHT_IMPORT_PLANS_GLOBAL,
            ),
        }
        .normalized()
    }

    const fn production() -> Self {
        Self {
            max_sessions_per_owner: DISCOVERY_MAX_SESSIONS_PER_OWNER,
            max_sessions_global: DISCOVERY_MAX_SESSIONS_GLOBAL,
            max_running_scans_per_owner: DISCOVERY_MAX_RUNNING_SCANS_PER_OWNER,
            max_running_scans_global: DISCOVERY_MAX_RUNNING_SCANS_GLOBAL,
            max_receipts_per_session: DISCOVERY_MAX_RECEIPTS_PER_SESSION,
            max_receipts_per_owner: DISCOVERY_MAX_RECEIPTS_PER_OWNER,
            max_receipts_global: DISCOVERY_MAX_RECEIPTS_GLOBAL,
            max_running_verifications_per_owner: DISCOVERY_MAX_RUNNING_VERIFICATIONS_PER_OWNER,
            max_running_verifications_global: DISCOVERY_MAX_RUNNING_VERIFICATIONS_GLOBAL,
            max_inflight_import_plans_per_owner: DISCOVERY_MAX_INFLIGHT_IMPORT_PLANS_PER_OWNER,
            max_inflight_import_plans_global: DISCOVERY_MAX_INFLIGHT_IMPORT_PLANS_GLOBAL,
        }
    }

    const fn normalized(self) -> Self {
        Self {
            max_sessions_per_owner: self.max_sessions_per_owner,
            max_sessions_global: self.max_sessions_global,
            max_running_scans_per_owner: if self.max_running_scans_per_owner
                > self.max_sessions_per_owner
            {
                self.max_sessions_per_owner
            } else {
                self.max_running_scans_per_owner
            },
            max_running_scans_global: if self.max_running_scans_global > self.max_sessions_global {
                self.max_sessions_global
            } else {
                self.max_running_scans_global
            },
            max_receipts_per_session: if self.max_receipts_per_session > self.max_receipts_per_owner
            {
                self.max_receipts_per_owner
            } else {
                self.max_receipts_per_session
            },
            max_receipts_per_owner: if self.max_receipts_per_owner > self.max_receipts_global {
                self.max_receipts_global
            } else {
                self.max_receipts_per_owner
            },
            max_receipts_global: self.max_receipts_global,
            max_running_verifications_per_owner: if self.max_running_verifications_per_owner
                > self.max_running_verifications_global
            {
                self.max_running_verifications_global
            } else {
                self.max_running_verifications_per_owner
            },
            max_running_verifications_global: self.max_running_verifications_global,
            max_inflight_import_plans_per_owner: if self.max_inflight_import_plans_per_owner
                > self.max_inflight_import_plans_global
            {
                self.max_inflight_import_plans_global
            } else {
                self.max_inflight_import_plans_per_owner
            },
            max_inflight_import_plans_global: self.max_inflight_import_plans_global,
        }
    }
}

fn read_limit_override(name: &str, production: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| (1..=production).contains(value))
        .unwrap_or(production)
}

impl LocalDiscoveryConfiguration {
    fn from_environment() -> Self {
        let development_mode = std::env::var("AGENTTALK_CORE_DEV_MODE").as_deref() == Ok("1");
        let import_plan_hold = if development_mode {
            std::env::var("AGENTTALK_CORE_TEST_DISCOVERY_IMPORT_PLAN_HOLD_MS")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .filter(|value| (50..=MAX_IMPORT_PLAN_HOLD_MS).contains(value))
                .map(Duration::from_millis)
                .unwrap_or_default()
        } else {
            Duration::ZERO
        };
        let fixture_root =
            std::env::var_os("AGENTTALK_LOCAL_DISCOVERY_FIXTURE_ROOT").map(PathBuf::from);
        let fixture_catalog =
            std::env::var_os("AGENTTALK_LOCAL_DISCOVERY_FIXTURE_CATALOG").map(PathBuf::from);
        if development_mode {
            if let (Some(root), Some(catalog)) = (fixture_root, fixture_catalog) {
                if let Some((root, catalog)) = canonical_fixture_paths(&root, &catalog) {
                    let catalog = load_fixture_catalog(&catalog);
                    return Self {
                        scan: WindowsPassiveDiscoveryConfig {
                            path_env: Some(root.display().to_string()),
                            use_real_app_paths: false,
                            use_real_packages: false,
                            use_real_loopback: false,
                            // Keep headroom above the isolated PATH noise so the
                            // explicit UserSelected source still fits the
                            // observation budget after the PATH provider runs.
                            max_results: 16,
                            max_path_entries: 1,
                            max_candidates_per_path_entry: 8,
                            request_timeout: DEFAULT_SCAN_TIMEOUT,
                            explicit_sources: fixture_explicit_sources(&root),
                            ..WindowsPassiveDiscoveryConfig::default()
                        },
                        catalog,
                        catalog_diagnostics: Vec::new(),
                        import_plan_hold,
                    };
                }
                return Self {
                    scan: inert_scan_configuration(),
                    catalog: CatalogConfiguration::Unavailable,
                    catalog_diagnostics: Vec::new(),
                    import_plan_hold,
                };
            }
        }
        let (catalog, catalog_diagnostics) = match bundled_production_catalog() {
            Ok(mut snapshot) => {
                let mut diagnostics = Vec::new();
                if let Some(directory) =
                    default_local_manifest_directory().filter(|directory| directory.exists())
                {
                    let local_report = load_local_manifest_directory(&directory);
                    diagnostics.extend(merge_local_manifest_report(&mut snapshot, local_report));
                }
                (CatalogConfiguration::Available(snapshot), diagnostics)
            }
            Err(_) => (CatalogConfiguration::Unavailable, Vec::new()),
        };
        Self {
            scan: WindowsPassiveDiscoveryConfig::default(),
            // W8.3: the production Core ships a bundled, offline ACP catalog
            // compiled into the binary. It loads through the existing cache
            // parser and schema validator and fails closed (Unavailable) on
            // any corrupt, empty, duplicate, or secret-like content, so the
            // discovery scan then surfaces a typed failure instead of running
            // silently without a catalog.
            catalog,
            catalog_diagnostics,
            import_plan_hold,
        }
    }
}

fn merge_local_manifest_report(
    bundled: &mut CatalogSnapshot,
    local: agenttalk_runtime_host::CatalogLoadReport,
) -> Vec<DiscoveryDiagnostic> {
    let mut diagnostics = local.diagnostics;
    let mut ids = bundled
        .manifests
        .iter()
        .map(|manifest| manifest.id.clone())
        .collect::<BTreeSet<_>>();
    for manifest in local.snapshot.manifests {
        if ids.insert(manifest.id.clone()) {
            bundled.manifests.push(manifest);
        } else {
            diagnostics.push(DiscoveryDiagnostic {
                source_kind: ObservationSourceKind::ExecutableInventory,
                code: DiscoveryDiagnosticCode::CatalogConflict,
            });
        }
    }
    diagnostics
}

fn inert_scan_configuration() -> WindowsPassiveDiscoveryConfig {
    WindowsPassiveDiscoveryConfig {
        path_env: None,
        use_real_app_paths: false,
        use_real_packages: false,
        use_real_loopback: false,
        max_results: 1,
        max_path_entries: 0,
        max_candidates_per_path_entry: 0,
        request_timeout: Duration::from_millis(100),
        ..WindowsPassiveDiscoveryConfig::default()
    }
}

/// Dev/test-only explicit sources: the fixture executable(s) become
/// `UserSelected` observations with an independent, user-visible authority,
/// so test-only fixtures can exercise the ACP protocol chain without ever
/// implying that a filename-only heuristic match is trusted. The names come
/// from a dev-mode environment variable and are resolved inside the
/// canonical fixture root; nothing is read from cwd, PATH, or any global
/// install.
fn fixture_explicit_sources(root: &Path) -> Vec<ExplicitDiscoverySource> {
    std::env::var("AGENTTALK_LOCAL_DISCOVERY_FIXTURE_EXPLICIT_SOURCES")
        .ok()
        .map(|value| {
            value
                .split(',')
                .filter(|name| !name.trim().is_empty())
                .map(|name| {
                    let path = root.join(name.trim());
                    ExplicitDiscoverySource::Executable(path)
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn canonical_fixture_paths(root: &Path, catalog: &Path) -> Option<(PathBuf, PathBuf)> {
    if !root.is_absolute() || !catalog.is_absolute() {
        return None;
    }
    let root = root.canonicalize().ok()?;
    let catalog = catalog.canonicalize().ok()?;
    (catalog.starts_with(&root) || catalog.parent().is_some_and(|parent| parent == root))
        .then_some((root, catalog))
}

fn load_fixture_catalog(path: &Path) -> CatalogConfiguration {
    let mut bytes = Vec::with_capacity(MAX_FIXTURE_CATALOG_BYTES.min(16 * 1024));
    let mut file = match File::open(path) {
        Ok(file) => file.take((MAX_FIXTURE_CATALOG_BYTES + 1) as u64),
        Err(_) => return CatalogConfiguration::Unavailable,
    };
    if file.read_to_end(&mut bytes).is_err() || bytes.len() > MAX_FIXTURE_CATALOG_BYTES {
        return CatalogConfiguration::Unavailable;
    }
    let network_counter = NetworkCounter::default();
    let report =
        load_catalog_for_scan(&bytes, None, unix_time_ms().max(0) as u64, &network_counter);
    if network_counter.count() != 0
        || !report.diagnostics.is_empty()
        || report.snapshot.manifests.is_empty()
    {
        return CatalogConfiguration::Unavailable;
    }
    CatalogConfiguration::Available(report.snapshot)
}

fn replay_or_reject(
    receipt: &DiscoveryRequestReceipt,
    payload_hash: &str,
) -> Result<Value, LocalDiscoveryServiceError> {
    if receipt.payload_hash != payload_hash {
        return Err(LocalDiscoveryServiceError::RequestIdReuse);
    }
    match &receipt.state {
        DiscoveryRequestReceiptState::Committed { response } => Ok(response.clone()),
        DiscoveryRequestReceiptState::PendingStart { .. }
        | DiscoveryRequestReceiptState::WorkerReady { .. }
        | DiscoveryRequestReceiptState::PendingVerification { .. }
        | DiscoveryRequestReceiptState::VerificationWorkerReady { .. } => {
            Err(LocalDiscoveryServiceError::StartInProgress)
        }
    }
}

fn ensure_owner(
    session: &DiscoverySession,
    owner: &DiscoveryOwnerScope,
) -> Result<(), LocalDiscoveryServiceError> {
    (session.owner == *owner)
        .then_some(())
        .ok_or(LocalDiscoveryServiceError::ScanNotFound)
}

fn ensure_start_capacity(
    state: &mut LocalDiscoveryState,
    owner: &DiscoveryOwnerScope,
    limits: &LocalDiscoveryLimits,
) -> Result<(), LocalDiscoveryServiceError> {
    let owner_running = state
        .running_scans
        .values()
        .filter(|running_owner| *running_owner == owner)
        .count();
    if owner_running >= limits.max_running_scans_per_owner {
        return Err(LocalDiscoveryServiceError::OwnerScanCapacityExhausted);
    }
    if state.running_scans.len() >= limits.max_running_scans_global {
        return Err(LocalDiscoveryServiceError::GlobalScanCapacityExhausted);
    }

    // Completed/failed/cancelled sessions are retained for a bounded period
    // so clients can read their snapshot, but they must not turn normal
    // repeated scans into a capacity failure. Evict the oldest terminal
    // sessions only when no operation still references them. A client that
    // races this boundary receives the stable ScanNotFound error from the
    // existing snapshot/mutation APIs; running sessions are never evicted.
    while state
        .sessions
        .values()
        .filter(|session| &session.owner == owner)
        .count()
        >= limits.max_sessions_per_owner
    {
        let Some(scan_id) = oldest_evictable_terminal_session(state, Some(owner)) else {
            return Err(LocalDiscoveryServiceError::OwnerScanCapacityExhausted);
        };
        evict_terminal_session(state, &scan_id);
    }
    while state.sessions.len() >= limits.max_sessions_global {
        let Some(scan_id) = oldest_evictable_terminal_session(state, None) else {
            return Err(LocalDiscoveryServiceError::GlobalScanCapacityExhausted);
        };
        evict_terminal_session(state, &scan_id);
    }
    Ok(())
}

fn oldest_evictable_terminal_session(
    state: &LocalDiscoveryState,
    owner: Option<&DiscoveryOwnerScope>,
) -> Option<String> {
    state
        .sessions
        .iter()
        .filter(|(scan_id, session)| {
            let scan_id = (*scan_id).as_str();
            owner.is_none_or(|expected| &session.owner == expected)
                && session.status != SessionStatus::Running
                && session.terminal_at.is_some()
                && session.verification_cancellations.is_empty()
                && !state.running_scans.contains_key(scan_id)
                && !state.lease_waiters.contains_key(scan_id)
                && !state
                    .running_verifications
                    .keys()
                    .any(|key| key.scan_id == scan_id)
                && !state
                    .inflight_import_plans
                    .keys()
                    .any(|key| key.scan_id == scan_id)
        })
        .min_by(|(left_id, left), (right_id, right)| {
            left.terminal_at
                .cmp(&right.terminal_at)
                .then_with(|| left_id.cmp(right_id))
        })
        .map(|(scan_id, _)| scan_id.clone())
}

fn evict_terminal_session(state: &mut LocalDiscoveryState, scan_id: &str) {
    let Some(session) = state.sessions.remove(scan_id) else {
        return;
    };
    session.scan_cancelled.store(true, Ordering::Release);
    for cancellation in session.verification_cancellations.values() {
        cancellation.store(true, Ordering::Release);
    }
    state.running_scans.remove(scan_id);
    state.lease_waiters.remove(scan_id);
    state
        .running_verifications
        .retain(|key, _| key.scan_id != scan_id);
    state
        .verification_waiters
        .retain(|key, _| key.scan_id != scan_id);
    state
        .inflight_import_plans
        .retain(|key, _| key.scan_id != scan_id);
    state.requests.retain(|_, receipt| {
        receipt_scan_id(receipt).is_none_or(|receipt_scan_id| receipt_scan_id != scan_id)
    });
}

/// The session a receipt belongs to. Committed receipts carry the scan id in
/// the response; all other states carry it structurally.
fn receipt_scan_id(receipt: &DiscoveryRequestReceipt) -> Option<&str> {
    match &receipt.state {
        DiscoveryRequestReceiptState::PendingStart { scan_id }
        | DiscoveryRequestReceiptState::WorkerReady { scan_id, .. }
        | DiscoveryRequestReceiptState::PendingVerification { scan_id, .. }
        | DiscoveryRequestReceiptState::VerificationWorkerReady { scan_id, .. } => Some(scan_id),
        DiscoveryRequestReceiptState::Committed { response } => response["scanId"].as_str(),
    }
}

/// W5.8 receipt hard bounds. Called while the state lock is held, before any
/// mutation, spawn, event, or private read. Never exposes another owner's
/// usage; the typed errors carry only a static renderer-safe message.
fn ensure_receipt_capacity(
    state: &LocalDiscoveryState,
    scan_id: &str,
    owner: &DiscoveryOwnerScope,
    limits: &LocalDiscoveryLimits,
) -> Result<(), LocalDiscoveryServiceError> {
    let session_receipts = state
        .requests
        .values()
        .filter(|receipt| receipt_scan_id(receipt) == Some(scan_id))
        .count();
    let owner_receipts = state
        .requests
        .keys()
        .filter(|request_key| &request_key.owner == owner)
        .count();
    if session_receipts >= limits.max_receipts_per_session
        || owner_receipts >= limits.max_receipts_per_owner
    {
        return Err(LocalDiscoveryServiceError::OwnerReceiptCapacityExhausted);
    }
    if state.requests.len() >= limits.max_receipts_global {
        return Err(LocalDiscoveryServiceError::GlobalReceiptCapacityExhausted);
    }
    Ok(())
}

/// W5.8 running-verification bounds (each in-flight verification owns an ACP
/// child). Called while the state lock is held, before any reservation.
fn ensure_verification_capacity(
    state: &LocalDiscoveryState,
    owner: &DiscoveryOwnerScope,
    limits: &LocalDiscoveryLimits,
) -> Result<(), LocalDiscoveryServiceError> {
    let owner_running = state
        .running_verifications
        .values()
        .filter(|running_owner| *running_owner == owner)
        .count();
    if owner_running >= limits.max_running_verifications_per_owner {
        return Err(LocalDiscoveryServiceError::OwnerVerificationCapacityExhausted);
    }
    if state.running_verifications.len() >= limits.max_running_verifications_global {
        return Err(LocalDiscoveryServiceError::GlobalVerificationCapacityExhausted);
    }
    Ok(())
}

/// W5.8 import-plan in-flight bounds plus per-operation single-flight: an
/// identical plan already in flight is stably rejected. Called while the
/// state lock is held, before any private ACP read or metadata recheck.
fn ensure_import_plan_capacity(
    state: &LocalDiscoveryState,
    key: &ImportPlanKey,
    owner: &DiscoveryOwnerScope,
    limits: &LocalDiscoveryLimits,
) -> Result<(), LocalDiscoveryServiceError> {
    if state.inflight_import_plans.contains_key(key) {
        return Err(LocalDiscoveryServiceError::ImportPlanInFlight);
    }
    let owner_inflight = state
        .inflight_import_plans
        .values()
        .filter(|inflight_owner| *inflight_owner == owner)
        .count();
    if owner_inflight >= limits.max_inflight_import_plans_per_owner {
        return Err(LocalDiscoveryServiceError::OwnerImportPlanCapacityExhausted);
    }
    if state.inflight_import_plans.len() >= limits.max_inflight_import_plans_global {
        return Err(LocalDiscoveryServiceError::GlobalImportPlanCapacityExhausted);
    }
    Ok(())
}

fn lock_state(
    state: &Arc<Mutex<LocalDiscoveryState>>,
) -> std::sync::MutexGuard<'_, LocalDiscoveryState> {
    state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn prune_expired(state: &mut LocalDiscoveryState) {
    let now = Instant::now();
    let expired = state
        .sessions
        .iter()
        .filter_map(|(scan_id, session)| (session.expires_at <= now).then_some(scan_id.clone()))
        .collect::<Vec<_>>();
    let mut expired_waiters = Vec::new();
    let mut expired_verification_waiters = Vec::new();
    for scan_id in expired {
        if let Some(session) = state.sessions.remove(&scan_id) {
            session.scan_cancelled.store(true, Ordering::Release);
            for cancellation in session.verification_cancellations.values() {
                cancellation.store(true, Ordering::Release);
            }
            state.running_scans.remove(&scan_id);
            if let Some(waiter) = state.lease_waiters.remove(&scan_id) {
                expired_waiters.push(waiter);
            }
            let verification_keys = state
                .running_verifications
                .keys()
                .filter(|key| key.scan_id == scan_id)
                .cloned()
                .collect::<Vec<_>>();
            for key in verification_keys {
                state.running_verifications.remove(&key);
                if let Some(waiter) = state.verification_waiters.remove(&key) {
                    expired_verification_waiters.push(waiter);
                }
            }
        }
    }
    state.requests.retain(|_, receipt| match &receipt.state {
        DiscoveryRequestReceiptState::Committed { response } => response["scanId"]
            .as_str()
            .is_none_or(|id| state.sessions.contains_key(id)),
        DiscoveryRequestReceiptState::PendingStart { scan_id }
        | DiscoveryRequestReceiptState::WorkerReady { scan_id, .. } => {
            state.sessions.contains_key(scan_id)
        }
        DiscoveryRequestReceiptState::PendingVerification { scan_id, .. }
        | DiscoveryRequestReceiptState::VerificationWorkerReady { scan_id, .. } => {
            state.sessions.contains_key(scan_id)
        }
    });
    state
        .inflight_import_plans
        .retain(|key, _| state.sessions.contains_key(&key.scan_id));
    for waiter in expired_waiters {
        let _ = waiter.try_send(WorkerLeaseAck::Unavailable);
    }
    for waiter in expired_verification_waiters {
        let _ = waiter.try_send(VerificationLeaseAck::Unavailable);
    }
}

fn snapshot_value(scan_id: &str, session: &DiscoverySession) -> Value {
    let candidates = session
        .candidates
        .iter()
        .filter(|(_, candidate)| !candidate.dismissed)
        .map(|(candidate_id, candidate)| candidate_snapshot_value(candidate_id, candidate))
        .collect::<Vec<_>>();
    json!({
        "schemaVersion": "agent.discovery.snapshot.v1",
        "scanId": scan_id,
        "state": session.status.as_str(),
        "candidates": candidates,
        "diagnostics": session.diagnostics,
    })
}

fn candidate_snapshot_value(candidate_id: &str, candidate: &CandidateState) -> Value {
    json!({
        "candidate": candidate.projection,
        "verification": candidate.verification.as_ref().map(|result| result.report()),
        "lifecycleState": candidate_lifecycle_state(candidate),
        "candidateId": candidate_id,
    })
}

fn import_plan_value(
    scan_id: &str,
    project_id: &str,
    model_selection: Option<&str>,
    projection: &CandidateProjection,
    metadata: &AcpImportPlanMetadata,
) -> Value {
    let plan_id = stable_plan_id(
        scan_id,
        &metadata.candidate_id,
        &metadata.candidate_binding_digest,
        project_id,
        model_selection,
    );
    json!({
        "schemaVersion": "agent.import.plan.v1",
        "planId": plan_id,
        "scanId": scan_id,
        "candidateId": metadata.candidate_id,
        "targetProjectId": project_id,
        "modelSelection": model_selection,
        "actions": [
            "create_connector_profile",
            "store_adapter_binding",
            "create_agent_identity",
            "set_model_selection",
            "assign_project_agent"
        ],
        "connector": {
            "id": projection.connector_id,
            "displayName": projection.display_name,
        },
        "adapter": {
            "kind": metadata.adapter_kind,
            "protocolMajor": metadata.protocol_major,
            "manifestId": metadata.manifest_id,
            "manifestSha256": metadata.manifest_sha256,
            "candidateBindingDigest": metadata.candidate_binding_digest,
        },
        "capabilities": metadata.capabilities,
        "authRequired": metadata.auth_required,
        "modelPolicy": "connector_default",
        "readOnly": true,
    })
}

fn stable_plan_id(
    scan_id: &str,
    candidate_id: &str,
    binding: &str,
    project_id: &str,
    model_selection: Option<&str>,
) -> String {
    let mut hasher = Sha256::new();
    for part in [
        scan_id,
        candidate_id,
        binding,
        project_id,
        model_selection.unwrap_or(""),
    ] {
        hasher.update(part.as_bytes());
        hasher.update([0xff]);
    }
    format!(
        "plan-{}",
        hasher
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    )
}

fn apply_verification_projection(
    projection: &mut CandidateProjection,
    report: &AcpCompatibilityReport,
) {
    projection.compatibility_state = report.compatibility_state;
    projection.auth_state = report.auth_state;
    projection.requires_configuration = report.requires_configuration;
    match report.status {
        AcpVerificationStatus::Verified => {
            projection.availability = CandidateAvailability::Available;
            projection.health_state = HealthState::Ready;
        }
        AcpVerificationStatus::AuthRequired => {
            projection.availability = CandidateAvailability::AuthenticationRequired;
            projection.health_state = HealthState::Ready;
        }
        AcpVerificationStatus::Rejected => {
            projection.availability = CandidateAvailability::Unavailable;
            projection.health_state =
                if report.diagnostic == Some(AcpVerificationDiagnosticCode::IdentityMismatch) {
                    HealthState::IdentityMismatch
                } else {
                    HealthState::Unavailable
                };
            projection.compatibility_state = CompatibilityState::Incompatible;
            projection.auth_state = AuthState::Unknown;
            projection.requires_configuration = true;
        }
    }
    if report.diagnostic == Some(AcpVerificationDiagnosticCode::IdentityMismatch) {
        projection.discovery_state = DiscoveryState::Disappeared;
    }
}

fn sort_diagnostics(diagnostics: &mut Vec<DiscoveryDiagnostic>) {
    let unique = std::mem::take(diagnostics)
        .into_iter()
        .collect::<BTreeSet<_>>();
    *diagnostics = unique.into_iter().collect();
}

fn request_hash(payload: &Value) -> String {
    let bytes = serde_json::to_vec(payload).unwrap_or_default();
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn is_safe_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._:-".contains(&byte))
}

fn candidate_lifecycle_state(candidate: &CandidateState) -> &'static str {
    if candidate.verifying {
        return "verifying";
    }
    if let Some(verification) = &candidate.verification {
        return match verification.report().status {
            AcpVerificationStatus::Verified => "verified",
            AcpVerificationStatus::AuthRequired => "auth_required",
            AcpVerificationStatus::Rejected => match verification.report().diagnostic {
                Some(AcpVerificationDiagnosticCode::ProtocolMismatch) => "protocol_mismatch",
                Some(AcpVerificationDiagnosticCode::IdentityMismatch) => "identity_changed",
                Some(AcpVerificationDiagnosticCode::IdentityUnverified) => "identity_unverified",
                Some(AcpVerificationDiagnosticCode::Timeout) => "timeout",
                Some(AcpVerificationDiagnosticCode::Cancelled) => "cancelled",
                _ => "not_verified",
            },
        };
    }
    if candidate.projection.health_state == HealthState::IdentityMismatch {
        return "identity_changed";
    }
    if !candidate.has_acp_binding
        || candidate.projection.compatibility_state == CompatibilityState::AdapterRequired
    {
        return "adapter_required";
    }
    match candidate.projection.discovery_state {
        DiscoveryState::Observed => "observed",
        DiscoveryState::Identified => "identified",
        DiscoveryState::Disappeared => "identity_changed",
    }
}

fn verification_diagnostic_code(code: AcpVerificationDiagnosticCode) -> &'static str {
    match code {
        AcpVerificationDiagnosticCode::ConsentRequired => "consent_required",
        AcpVerificationDiagnosticCode::IdentityMismatch => "identity_mismatch",
        AcpVerificationDiagnosticCode::IdentityUnverified => "identity_unverified",
        AcpVerificationDiagnosticCode::Timeout => "timeout",
        AcpVerificationDiagnosticCode::Cancelled => "cancelled",
        AcpVerificationDiagnosticCode::LaunchFailed => "launch_failed",
        AcpVerificationDiagnosticCode::ProtocolMismatch => "protocol_mismatch",
        AcpVerificationDiagnosticCode::ProtocolViolation => "protocol_violation",
        AcpVerificationDiagnosticCode::OversizedFrame => "oversized_frame",
        AcpVerificationDiagnosticCode::NonUtf8Frame => "non_utf8_frame",
        AcpVerificationDiagnosticCode::StderrOutput => "stderr_output",
        AcpVerificationDiagnosticCode::ProcessFailed => "process_failed",
        AcpVerificationDiagnosticCode::AuthenticationRequired => "authentication_required",
        AcpVerificationDiagnosticCode::CleanupFailed => "cleanup_failed",
    }
}

fn unix_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn explicit_start_payload_accepts_only_one_absolute_executable_path() {
        let path = if cfg!(windows) {
            r"C:\\Agents\\fixture.exe"
        } else {
            "/tmp/agents/fixture"
        };
        let parsed = parse_start_explicit_sources(&json!({
            "explicitExecutablePath": path
        }))
        .expect("explicit path payload");
        assert!(matches!(
            parsed,
            Some(ref sources) if sources.len() == 1
                && matches!(&sources[0], ExplicitDiscoverySource::Executable(candidate) if candidate == Path::new(path))
        ));
        assert!(parse_start_explicit_sources(&json!({})).unwrap().is_none());
        for payload in [
            json!({"explicitExecutablePath": "relative.exe"}),
            json!({"explicitExecutablePath": ""}),
            json!({"unknown": path}),
            json!({"explicitExecutablePath": path, "extra": true}),
        ] {
            assert!(
                matches!(
                    parse_start_explicit_sources(&payload),
                    Err(LocalDiscoveryServiceError::InvalidPayload)
                ),
                "payload must be rejected: {payload}"
            );
        }
    }

    fn test_service() -> LocalDiscoveryService {
        LocalDiscoveryService {
            state: Arc::new(Mutex::new(LocalDiscoveryState {
                sessions: BTreeMap::new(),
                requests: BTreeMap::new(),
                running_scans: BTreeMap::new(),
                lease_waiters: BTreeMap::new(),
                running_verifications: BTreeMap::new(),
                verification_waiters: BTreeMap::new(),
                inflight_import_plans: BTreeMap::new(),
                accepting_starts: true,
                shutdown_generation: 0,
            })),
            publication_lock: Arc::new(Mutex::new(())),
            worker_before_lease_hook: Arc::new(Mutex::new(None)),
            worker_after_lease_hook: Arc::new(Mutex::new(None)),
            verify_before_state_hook: Arc::new(Mutex::new(None)),
            dismiss_before_state_hook: Arc::new(Mutex::new(None)),
            import_plan_before_state_hook: Arc::new(Mutex::new(None)),
            active_running_leases: Arc::new(AtomicUsize::new(0)),
            scan_workloads_started: Arc::new(AtomicUsize::new(0)),
            verify_private_state_attempts: Arc::new(AtomicUsize::new(0)),
            import_plan_preflight_attempts: Arc::new(AtomicUsize::new(0)),
            configuration: LocalDiscoveryConfiguration {
                scan: inert_scan_configuration(),
                catalog: CatalogConfiguration::Available(CatalogSnapshot {
                    revision: "test".into(),
                    manifests: Vec::new(),
                }),
                catalog_diagnostics: Vec::new(),
                import_plan_hold: Duration::ZERO,
            },
            limits: LocalDiscoveryLimits {
                max_sessions_per_owner: 2,
                max_sessions_global: 2,
                max_running_scans_per_owner: 2,
                max_running_scans_global: 2,
                max_receipts_per_session: 8,
                max_receipts_per_owner: 8,
                max_receipts_global: 8,
                max_running_verifications_per_owner: 2,
                max_running_verifications_global: 2,
                max_inflight_import_plans_per_owner: 2,
                max_inflight_import_plans_global: 2,
            },
            next_id: Arc::new(AtomicU64::new(0)),
        }
    }

    fn test_owner(label: &str) -> DiscoveryOwnerScope {
        DiscoveryOwnerScope::from_authenticated_session(
            &format!("client-{label}"),
            &format!("session-{label}"),
        )
    }

    #[test]
    fn fixture_explicit_sources_require_dev_mode_and_resolve_inside_fixture_root() {
        // Without the environment variable no explicit source is granted.
        std::env::remove_var("AGENTTALK_LOCAL_DISCOVERY_FIXTURE_EXPLICIT_SOURCES");
        let root = PathBuf::from("C:\\fixture-root");
        assert!(fixture_explicit_sources(&root).is_empty());
        // With the variable, the names resolve strictly inside the root.
        std::env::set_var(
            "AGENTTALK_LOCAL_DISCOVERY_FIXTURE_EXPLICIT_SOURCES",
            "fixture-agent.exe, other-tool.exe",
        );
        let sources = fixture_explicit_sources(&root);
        assert_eq!(sources.len(), 2);
        match &sources[0] {
            ExplicitDiscoverySource::Executable(path) => {
                assert_eq!(path, &root.join("fixture-agent.exe"));
            }
            ExplicitDiscoverySource::Endpoint(_) => panic!("fixture names are executables"),
        }
        std::env::remove_var("AGENTTALK_LOCAL_DISCOVERY_FIXTURE_EXPLICIT_SOURCES");
    }

    fn event_counter() -> (EventSink, Arc<AtomicUsize>) {
        let count = Arc::new(AtomicUsize::new(0));
        let captured = Arc::clone(&count);
        (
            Arc::new(move |_event| {
                captured.fetch_add(1, Ordering::AcqRel);
            }),
            count,
        )
    }

    #[test]
    fn production_default_catalog_is_non_empty_and_offline() {
        // W8.3: the production Core must ship a bundled, offline ACP catalog.
        // This test must run without dev-mode or fixture catalog environment
        // variables so it exercises the real production configuration path.
        assert!(
            std::env::var("AGENTTALK_CORE_DEV_MODE").as_deref() != Ok("1"),
            "test must run outside dev mode"
        );
        assert!(
            std::env::var_os("AGENTTALK_LOCAL_DISCOVERY_FIXTURE_ROOT").is_none()
                && std::env::var_os("AGENTTALK_LOCAL_DISCOVERY_FIXTURE_CATALOG").is_none(),
            "test must not depend on fixture environment variables"
        );
        let configuration = LocalDiscoveryConfiguration::from_environment();
        match configuration.catalog {
            CatalogConfiguration::Available(snapshot) => {
                assert!(
                    !snapshot.manifests.is_empty(),
                    "production catalog must not be empty"
                );
                assert_ne!(
                    snapshot.revision, "unavailable",
                    "production catalog revision must be stable and meaningful"
                );
                assert!(
                    snapshot
                        .manifests
                        .iter()
                        .any(|manifest| matches!(manifest.launch, ManifestLaunch::Direct { .. })),
                    "production catalog must include at least one direct manifest"
                );
            }
            CatalogConfiguration::Unavailable => {
                panic!("production bundled catalog must be available");
            }
        }
    }

    #[test]
    fn local_manifest_report_merges_with_bundled_catalog_and_conflicts_fail_closed() {
        let local_manifest = AdapterManifest::validate_json_bytes(include_bytes!(
            "../../../examples/local-agent-manifests/claude-code.agenttalk-agent.json"
        ))
        .expect("example local manifest");
        let mut bundled = bundled_production_catalog().expect("bundled catalog");
        let before = bundled.manifests.len();
        let diagnostics = merge_local_manifest_report(
            &mut bundled,
            CatalogLoadReport {
                snapshot: CatalogSnapshot {
                    revision: "local-test".into(),
                    manifests: vec![local_manifest.clone()],
                },
                diagnostics: Vec::new(),
            },
        );
        assert!(diagnostics.is_empty());
        assert_eq!(bundled.manifests.len(), before + 1);
        assert!(bundled
            .manifests
            .iter()
            .any(|manifest| manifest.id == local_manifest.id));

        let after_first_merge = bundled.manifests.len();
        let conflict = merge_local_manifest_report(
            &mut bundled,
            CatalogLoadReport {
                snapshot: CatalogSnapshot {
                    revision: "local-conflict".into(),
                    manifests: vec![local_manifest],
                },
                diagnostics: Vec::new(),
            },
        );
        assert_eq!(bundled.manifests.len(), after_first_merge);
        assert!(conflict
            .iter()
            .any(|diagnostic| { diagnostic.code == DiscoveryDiagnosticCode::CatalogConflict }));
    }

    #[test]
    fn same_owner_scan_flood_is_bounded_without_side_effects() {
        let service = test_service();
        let owner = test_owner("same-owner-flood");
        let (_sink, events) = event_counter();

        let mut reservations = Vec::new();
        for index in 0..2 {
            let outcome = service
                .begin_start(&owner, &format!("request-{index}"), &json!({}))
                .expect("initial scans fit the owner quota");
            match outcome {
                StartScanOutcome::Reserved(reservation) => reservations.push(reservation),
                StartScanOutcome::Replayed(_) => panic!("new request must reserve a scan"),
            }
        }
        let before = service.counts_for_tests(&owner);
        assert_eq!(before.owner_sessions, 2);
        assert_eq!(before.owner_requests, 2);
        assert_eq!(before.owner_running_scans, 2);
        let rejected = service.begin_start(&owner, "request-overflow", &json!({}));
        assert!(matches!(
            rejected,
            Err(LocalDiscoveryServiceError::OwnerScanCapacityExhausted)
        ));
        assert_eq!(
            service.counts_for_tests(&owner),
            before,
            "rejected start must not create a session or request receipt"
        );
        assert_eq!(
            events.load(Ordering::Acquire),
            0,
            "reserved or rejected starts must not emit discovery lifecycle events before launch"
        );
        drop(reservations);
        assert_eq!(service.counts_for_tests(&owner).owner_sessions, 0);
        assert_eq!(service.counts_for_tests(&owner).owner_requests, 0);
        assert_eq!(service.counts_for_tests(&owner).owner_running_scans, 0);
    }

    #[test]
    fn repeated_completed_scans_evict_oldest_without_capacity_failure() {
        let mut service = test_service();
        service.limits.max_sessions_per_owner = 16;
        service.limits.max_sessions_global = 16;
        let owner = test_owner("repeated-completed");
        let first_scan = "scan-completed-00".to_owned();
        let mut retained_scan_ids = Vec::new();

        for index in 0..20 {
            let scan_id = format!("scan-completed-{index:02}");
            service.seed_completed_candidate_for_shutdown_tests(
                &owner,
                &scan_id,
                "candidate-empty",
            );
            let outcome =
                service.begin_start(&owner, &format!("repeat-request-{index:02}"), &json!({}));
            match outcome {
                Ok(StartScanOutcome::Reserved(reservation)) => drop(reservation),
                Ok(StartScanOutcome::Replayed(_)) | Err(_) => {
                    panic!("completed scan {index} must not exhaust capacity")
                }
            }
            retained_scan_ids.push(scan_id);
        }

        let counts = service.counts_for_tests(&owner);
        assert!(counts.owner_sessions <= 16);
        assert_eq!(counts.owner_running_scans, 0);
        assert!(
            matches!(
                service.snapshot(&owner, &first_scan),
                Err(LocalDiscoveryServiceError::ScanNotFound)
            ),
            "the oldest terminal session must be evicted at the retention boundary"
        );
        let newest = retained_scan_ids.last().expect("newest scan");
        assert!(service.snapshot(&owner, newest).is_ok());
        assert!(matches!(
            service.snapshot(&owner, &first_scan),
            Err(LocalDiscoveryServiceError::ScanNotFound)
        ));
    }

    #[test]
    fn pending_start_receipt_is_not_replayed_before_commit() {
        let service = test_service();
        let owner = test_owner("pending-replay");
        let reservation = match service
            .begin_start(&owner, "same-request", &json!({}))
            .expect("reserve pending start")
        {
            StartScanOutcome::Reserved(reservation) => reservation,
            StartScanOutcome::Replayed(_) => panic!("first start must reserve"),
        };

        let duplicate = service.begin_start(&owner, "same-request", &json!({}));
        assert!(
            !matches!(duplicate, Ok(StartScanOutcome::Replayed(_))),
            "same request must not replay a success before the original start commits"
        );
        drop(reservation);
    }

    #[test]
    fn worker_ready_start_receipt_is_not_replayed_before_publication() {
        let service = test_service();
        let owner = test_owner("worker-ready-replay");
        let (sink, _) = event_counter();
        let reservation = match service
            .begin_start(&owner, "same-request", &json!({}))
            .expect("reserve pending start")
        {
            StartScanOutcome::Reserved(reservation) => reservation,
            StartScanOutcome::Replayed(_) => panic!("first start must reserve"),
        };
        let ready = reservation
            .launch_worker_until_ready(Arc::clone(&sink))
            .expect("worker reaches ready state");

        assert!(matches!(
            service.begin_start(&owner, "same-request", &json!({})),
            Err(LocalDiscoveryServiceError::StartInProgress)
        ));
        drop(ready);
        let counts = service.counts_for_tests(&owner);
        assert_eq!(counts.owner_sessions, 0);
        assert_eq!(counts.owner_requests, 0);
        assert_eq!(counts.owner_running_scans, 0);
    }

    #[test]
    fn duplicate_start_replays_only_after_single_successful_commit() {
        let service = test_service();
        let owner = test_owner("duplicate-commit");
        let events = Arc::new(Mutex::new(Vec::<String>::new()));
        let captured_events = Arc::clone(&events);
        let sink: EventSink = Arc::new(move |event| {
            captured_events
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(event.event_type);
        });
        let reservation = match service
            .begin_start(&owner, "same-request", &json!({}))
            .expect("reserve pending start")
        {
            StartScanOutcome::Reserved(reservation) => reservation,
            StartScanOutcome::Replayed(_) => panic!("first start must reserve"),
        };
        assert!(matches!(
            service.begin_start(&owner, "same-request", &json!({})),
            Err(LocalDiscoveryServiceError::StartInProgress)
        ));

        let committed = reservation
            .launch(Arc::clone(&sink))
            .expect("original start commits");
        wait_for_running_to_drain(&service, &owner);
        let replay = service
            .start(&owner, "same-request", &json!({}), sink)
            .expect("committed duplicate replays");
        assert_eq!(replay, committed);
        let started_events = events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .filter(|event_type| event_type.as_str() == "agent.discovery.started")
            .count();
        assert_eq!(started_events, 1);
        let counts = service.counts_for_tests(&owner);
        assert_eq!(counts.owner_sessions, 1);
        assert_eq!(counts.owner_requests, 1);
        assert_eq!(counts.owner_running_scans, 0);
        assert_eq!(counts.owner_lease_waiters, 0);
    }

    #[test]
    fn shutdown_blocks_old_start_receipt_replay() {
        let service = test_service();
        let owner = test_owner("shutdown-replay");
        let (sink, _) = event_counter();
        let first = service
            .start(&owner, "same-request", &json!({}), Arc::clone(&sink))
            .expect("initial start commits");
        assert_eq!(first["accepted"], true);
        wait_for_running_to_drain(&service, &owner);

        service.cancel_all();
        assert!(matches!(
            service.start(&owner, "same-request", &json!({}), sink),
            Err(LocalDiscoveryServiceError::ShuttingDown)
        ));
    }

    #[test]
    fn shutdown_between_reserve_and_launch_rejects_without_started_event() {
        let service = test_service();
        let owner = test_owner("shutdown-before-launch");
        let (sink, events) = event_counter();
        let reservation = match service
            .begin_start(&owner, "request", &json!({}))
            .expect("reserve pending start")
        {
            StartScanOutcome::Reserved(reservation) => reservation,
            StartScanOutcome::Replayed(_) => panic!("first start must reserve"),
        };

        service.cancel_all();
        assert!(matches!(
            reservation.launch(sink),
            Err(LocalDiscoveryServiceError::ShuttingDown)
        ));
        assert_eq!(
            events.load(Ordering::Acquire),
            0,
            "shutdown before commit must not publish agent.discovery.started"
        );
        let counts = service.counts_for_tests(&owner);
        assert_eq!(counts.owner_sessions, 0);
        assert_eq!(counts.owner_requests, 0);
        assert_eq!(counts.owner_running_scans, 0);
        assert_eq!(counts.owner_lease_waiters, 0);
    }

    #[test]
    fn start_spawn_readiness_and_commit_failures_have_zero_side_effects() {
        for (label, mode, expected_error) in [
            (
                "spawn",
                StartLaunchMode::FailSpawn,
                LocalDiscoveryServiceError::ScanWorkerUnavailable,
            ),
            (
                "readiness",
                StartLaunchMode::FailReadiness,
                LocalDiscoveryServiceError::ScanWorkerUnavailable,
            ),
            (
                "commit-shutdown",
                StartLaunchMode::ShutdownAfterReadyBeforeCommit,
                LocalDiscoveryServiceError::ShuttingDown,
            ),
            (
                "post-commit-shutdown",
                StartLaunchMode::ShutdownAfterCommitBeforePublish,
                LocalDiscoveryServiceError::ShuttingDown,
            ),
        ] {
            let service = test_service();
            let owner = test_owner(label);
            let (sink, events) = event_counter();
            let reservation = match service
                .begin_start(&owner, "request", &json!({}))
                .expect("reserve pending start")
            {
                StartScanOutcome::Reserved(reservation) => reservation,
                StartScanOutcome::Replayed(_) => panic!("first start must reserve"),
            };
            assert_eq!(service.counts_for_tests(&owner).owner_sessions, 1);
            assert!(matches!(
                reservation.launch_with_mode_for_tests(sink, mode),
                Err(error) if error == expected_error
            ));
            assert_eq!(
                events.load(Ordering::Acquire),
                0,
                "{label} failure must not publish started"
            );
            let counts = service.counts_for_tests(&owner);
            assert_eq!(counts.owner_sessions, 0, "{label} leaked session");
            assert_eq!(counts.owner_requests, 0, "{label} leaked request receipt");
            assert_eq!(counts.owner_running_scans, 0, "{label} leaked running slot");
            assert_eq!(counts.owner_lease_waiters, 0, "{label} leaked lease waiter");
        }
    }

    #[test]
    fn shutdown_after_receipt_commit_before_publication_does_not_replay_success() {
        let service = test_service();
        let owner = test_owner("shutdown-after-commit");
        let (sink, events) = event_counter();
        let reservation = match service
            .begin_start(&owner, "request", &json!({}))
            .expect("reserve pending start")
        {
            StartScanOutcome::Reserved(reservation) => reservation,
            StartScanOutcome::Replayed(_) => panic!("new request must reserve"),
        };

        assert!(matches!(
            reservation.launch_with_mode_for_tests(
                Arc::clone(&sink),
                StartLaunchMode::ShutdownAfterCommitBeforePublish
            ),
            Err(LocalDiscoveryServiceError::ShuttingDown)
        ));
        assert_eq!(
            events.load(Ordering::Acquire),
            0,
            "shutdown after receipt commit but before publication must not emit started"
        );
        assert!(matches!(
            service.start(&owner, "request", &json!({}), sink),
            Err(LocalDiscoveryServiceError::ShuttingDown)
        ));
        let counts = service.counts_for_tests(&owner);
        assert_eq!(counts.owner_sessions, 0);
        assert_eq!(counts.owner_requests, 0);
        assert_eq!(counts.owner_running_scans, 0);
        assert_eq!(counts.owner_lease_waiters, 0);
    }

    #[test]
    fn shutdown_clears_worker_ready_start_before_publication() {
        let service = test_service();
        let owner = test_owner("shutdown-worker-ready");
        let (sink, events) = event_counter();
        let reservation = match service
            .begin_start(&owner, "request", &json!({}))
            .expect("reserve pending start")
        {
            StartScanOutcome::Reserved(reservation) => reservation,
            StartScanOutcome::Replayed(_) => panic!("new request must reserve"),
        };
        let mut ready = reservation
            .launch_worker_until_ready(Arc::clone(&sink))
            .expect("worker reaches ready state");

        service.cancel_all();

        let publication = service.start_publication_guard();
        assert!(matches!(
            ready.publish_with(&publication, &owner),
            Err(LocalDiscoveryServiceError::ShuttingDown)
        ));
        assert_eq!(events.load(Ordering::Acquire), 0);
        drop(publication);
        drop(ready);
        let counts = service.counts_for_tests(&owner);
        assert_eq!(counts.owner_sessions, 0);
        assert_eq!(counts.owner_requests, 0);
        assert_eq!(counts.owner_running_scans, 0);
        assert_eq!(counts.owner_lease_waiters, 0);
    }

    #[test]
    fn worker_lease_ack_timeout_rolls_back_without_started_or_committed_receipt() {
        let service = test_service();
        let owner = test_owner("lease-ack-timeout");
        let hook = Arc::new(WorkerPauseHook::new());
        service.set_worker_before_lease_hook_for_tests(Arc::clone(&hook));
        let (sink, events) = event_counter();
        let reservation = match service
            .begin_start(&owner, "request", &json!({}))
            .expect("reserve pending start")
        {
            StartScanOutcome::Reserved(reservation) => reservation,
            StartScanOutcome::Replayed(_) => panic!("new request must reserve"),
        };
        let mut ready = reservation
            .launch_worker_until_ready(Arc::clone(&sink))
            .expect("worker reaches ready state");
        let publication = service.start_publication_guard();
        assert!(matches!(
            ready.publish_with_timeout(&publication, &owner, Duration::from_millis(10)),
            Err(LocalDiscoveryServiceError::ScanWorkerUnavailable)
        ));
        assert!(
            hook.wait_until_entered(Duration::from_secs(1)),
            "worker must be paused before the running lease until test releases it"
        );
        hook.release();
        drop(publication);
        drop(ready);
        assert_eq!(events.load(Ordering::Acquire), 0);
        let counts = service.counts_for_tests(&owner);
        assert_eq!(counts.owner_sessions, 0);
        assert_eq!(counts.owner_requests, 0);
        assert_eq!(counts.owner_running_scans, 0);
    }

    #[test]
    fn concurrent_same_owner_start_is_atomically_bounded() {
        let service = test_service();
        let owner = test_owner("concurrent-owner-flood");
        let mut threads = Vec::new();
        let barrier = Arc::new(std::sync::Barrier::new(8));
        for index in 0..8 {
            let service = service.clone();
            let owner = owner.clone();
            let barrier = Arc::clone(&barrier);
            threads.push(thread::spawn(move || {
                barrier.wait();
                service.begin_start(&owner, &format!("request-{index}"), &json!({}))
            }));
        }
        let outcomes = threads
            .into_iter()
            .map(|thread| thread.join().expect("start thread must not panic"))
            .collect::<Vec<_>>();
        let accepted = outcomes.iter().filter(|outcome| outcome.is_ok()).count();
        assert!(
            accepted <= 2,
            "concurrent starts exceeded the owner quota: accepted={accepted}"
        );
        assert!(
            service.counts_for_tests(&owner).owner_sessions <= 2,
            "state retained too many sessions for one owner: {:?}",
            service.counts_for_tests(&owner)
        );
        assert!(
            service.counts_for_tests(&owner).owner_running_scans <= 2,
            "state retained too many running scans for one owner: {:?}",
            service.counts_for_tests(&owner)
        );
        drop(outcomes);
    }

    #[test]
    fn start_request_id_reuse_with_different_payload_stays_request_reuse() {
        let service = test_service();
        let owner = test_owner("request-reuse");
        let (sink, _) = event_counter();
        let first = service
            .start(&owner, "same-request", &json!({}), Arc::clone(&sink))
            .expect("initial start");
        wait_for_running_to_drain(&service, &owner);
        let replay = service
            .start(&owner, "same-request", &json!({}), Arc::clone(&sink))
            .expect("same request and payload replays");
        assert_eq!(replay, first);
        assert_eq!(service.counts_for_tests(&owner).owner_sessions, 1);
        assert_eq!(service.counts_for_tests(&owner).owner_requests, 1);

        assert!(matches!(
            service.start(
                &owner,
                "same-request",
                &json!({"unexpected": true}),
                Arc::clone(&sink)
            ),
            Err(LocalDiscoveryServiceError::RequestIdReuse)
        ));
        assert_eq!(
            service.counts_for_tests(&owner).owner_sessions,
            1,
            "requestId reuse rejection must not consume another quota slot"
        );
    }

    #[test]
    fn global_scan_flood_is_bounded_without_eviction() {
        let service = test_service();
        let owner_a = test_owner("global-a");
        let owner_b = test_owner("global-b");
        let owner_c = test_owner("global-c");
        let reservation_a = service
            .begin_start(&owner_a, "request-a", &json!({}))
            .expect("first global scan");
        let reservation_b = service
            .begin_start(&owner_b, "request-b", &json!({}))
            .expect("second global scan");
        let owner_a_scan = {
            let state = lock_state(&service.state);
            state
                .sessions
                .iter()
                .find(|(_, session)| session.owner == owner_a)
                .map(|(scan_id, _)| scan_id.clone())
                .expect("owner A scan remains retained")
        };

        let rejected = service.begin_start(&owner_c, "request-c", &json!({}));
        assert!(matches!(
            rejected,
            Err(LocalDiscoveryServiceError::GlobalScanCapacityExhausted)
        ));
        assert!(
            service.snapshot(&owner_a, &owner_a_scan).is_ok(),
            "global flood must not evict another owner's retained scan"
        );
        assert_eq!(service.counts_for_tests(&owner_c).owner_sessions, 0);
        assert_eq!(service.counts_for_tests(&owner_c).owner_requests, 0);
        drop(reservation_a);
        drop(reservation_b);
    }

    #[test]
    fn expired_sessions_release_recoverable_capacity_and_receipts() {
        let service = test_service();
        let owner = test_owner("expiry");
        let (sink, _) = event_counter();
        for index in 0..2 {
            service
                .start(
                    &owner,
                    &format!("request-{index}"),
                    &json!({}),
                    Arc::clone(&sink),
                )
                .expect("start within owner quota");
        }
        wait_for_running_to_drain(&service, &owner);
        assert_eq!(service.counts_for_tests(&owner).owner_sessions, 2);
        assert_eq!(service.counts_for_tests(&owner).owner_requests, 2);
        service
            .start(&owner, "request-overflow", &json!({}), Arc::clone(&sink))
            .expect("completed retained sessions are evictable before capacity failure");
        assert!(service.counts_for_tests(&owner).owner_sessions <= 2);

        service.expire_all_sessions_for_tests();
        assert_eq!(service.counts_for_tests(&owner).owner_sessions, 0);
        assert_eq!(service.counts_for_tests(&owner).owner_requests, 0);
        service
            .start(
                &owner,
                "request-after-expiry",
                &json!({}),
                Arc::clone(&sink),
            )
            .expect("expired retained sessions release capacity");
        wait_for_running_to_drain(&service, &owner);
    }

    #[test]
    fn running_scan_slot_releases_on_failure_cancel_and_reservation_drop() {
        let owner = test_owner("running-release");
        let (sink, _) = event_counter();
        let failure_service = LocalDiscoveryService {
            state: Arc::new(Mutex::new(LocalDiscoveryState {
                sessions: BTreeMap::new(),
                requests: BTreeMap::new(),
                running_scans: BTreeMap::new(),
                lease_waiters: BTreeMap::new(),
                running_verifications: BTreeMap::new(),
                verification_waiters: BTreeMap::new(),
                inflight_import_plans: BTreeMap::new(),
                accepting_starts: true,
                shutdown_generation: 0,
            })),
            publication_lock: Arc::new(Mutex::new(())),
            worker_before_lease_hook: Arc::new(Mutex::new(None)),
            worker_after_lease_hook: Arc::new(Mutex::new(None)),
            verify_before_state_hook: Arc::new(Mutex::new(None)),
            dismiss_before_state_hook: Arc::new(Mutex::new(None)),
            import_plan_before_state_hook: Arc::new(Mutex::new(None)),
            active_running_leases: Arc::new(AtomicUsize::new(0)),
            scan_workloads_started: Arc::new(AtomicUsize::new(0)),
            verify_private_state_attempts: Arc::new(AtomicUsize::new(0)),
            import_plan_preflight_attempts: Arc::new(AtomicUsize::new(0)),
            configuration: LocalDiscoveryConfiguration {
                scan: inert_scan_configuration(),
                catalog: CatalogConfiguration::Unavailable,
                catalog_diagnostics: Vec::new(),
                import_plan_hold: Duration::ZERO,
            },
            limits: LocalDiscoveryLimits {
                max_sessions_per_owner: 2,
                max_sessions_global: 2,
                max_running_scans_per_owner: 1,
                max_running_scans_global: 1,
                max_receipts_per_session: 8,
                max_receipts_per_owner: 8,
                max_receipts_global: 8,
                max_running_verifications_per_owner: 2,
                max_running_verifications_global: 2,
                max_inflight_import_plans_per_owner: 2,
                max_inflight_import_plans_global: 2,
            },
            next_id: Arc::new(AtomicU64::new(0)),
        };
        failure_service
            .start(&owner, "request-failure", &json!({}), Arc::clone(&sink))
            .expect("failure scan starts");
        wait_for_running_to_drain(&failure_service, &owner);
        assert_eq!(
            failure_service.counts_for_tests(&owner).owner_running_scans,
            0
        );

        let cancel_service = test_service();
        cancel_service
            .start(&owner, "request-cancel", &json!({}), Arc::clone(&sink))
            .expect("cancel scan starts");
        cancel_service.cancel_all();
        wait_for_running_to_drain(&cancel_service, &owner);
        assert!(matches!(
            cancel_service.start(
                &owner,
                "request-after-shutdown",
                &json!({}),
                Arc::clone(&sink)
            ),
            Err(LocalDiscoveryServiceError::ShuttingDown)
        ));

        let rollback_service = test_service();
        let reservation = match rollback_service
            .begin_start(&owner, "request-reserved", &json!({}))
            .expect("reserve scan without launch")
        {
            StartScanOutcome::Reserved(reservation) => reservation,
            StartScanOutcome::Replayed(_) => panic!("new request must reserve"),
        };
        assert_eq!(
            rollback_service
                .counts_for_tests(&owner)
                .owner_running_scans,
            1
        );
        drop(reservation);
        assert_eq!(
            rollback_service
                .counts_for_tests(&owner)
                .owner_running_scans,
            0
        );
        assert_eq!(rollback_service.counts_for_tests(&owner).owner_sessions, 0);
        assert_eq!(rollback_service.counts_for_tests(&owner).owner_requests, 0);
    }

    #[test]
    fn shutdown_wins_before_verify_state_access_without_receipt_or_event() {
        let service = Arc::new(test_service());
        let owner = test_owner("w57-verify-shutdown");
        let scan_id = "scan-w57-verify";
        let candidate_id = "candidate-w57-verify";
        service.seed_completed_candidate_for_shutdown_tests(&owner, scan_id, candidate_id);
        let hook = Arc::new(WorkerPauseHook::new());
        service.set_verify_before_state_hook_for_tests(Arc::clone(&hook));
        let (sink, events) = event_counter();
        let (sender, receiver) = mpsc::channel();
        let worker_service = Arc::clone(&service);
        let worker_owner = owner.clone();
        thread::spawn(move || {
            worker_service.pause_before_verify_publication_for_tests();
            let publication = worker_service.start_publication_guard();
            let result = worker_service.begin_verify_with_publication(
                &publication,
                VerifyRequest {
                    owner: &worker_owner,
                    request_id: "verify-request",
                    scan_id,
                    candidate_id,
                    consent: true,
                    deadline: Duration::from_millis(50),
                    event_sink: sink,
                },
            );
            sender
                .send(result)
                .expect("result receiver remains available");
        });

        assert!(
            hook.wait_until_entered(Duration::from_secs(1)),
            "verify must pause before it can read private ACP state"
        );
        service.cancel_all();
        hook.release();

        let result = receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("verify request finishes after the test releases it");
        assert!(matches!(
            result,
            Err(LocalDiscoveryServiceError::ShuttingDown)
        ));
        assert_eq!(service.verify_private_state_attempts_for_tests(), 0);
        assert_eq!(
            service.candidate_flags_for_tests(&owner, scan_id, candidate_id),
            Some((false, false, false))
        );
        assert_eq!(events.load(Ordering::Acquire), 0);
        assert_eq!(service.counts_for_tests(&owner).owner_requests, 0);
    }

    #[test]
    fn shutdown_wins_before_dismiss_mutation_without_receipt_or_event() {
        let service = Arc::new(test_service());
        let owner = test_owner("w57-dismiss-shutdown");
        let scan_id = "scan-w57-dismiss";
        let candidate_id = "candidate-w57-dismiss";
        service.seed_completed_candidate_for_shutdown_tests(&owner, scan_id, candidate_id);
        let hook = Arc::new(WorkerPauseHook::new());
        service.set_dismiss_before_state_hook_for_tests(Arc::clone(&hook));
        let (sink, events) = event_counter();
        let (sender, receiver) = mpsc::channel();
        let worker_service = Arc::clone(&service);
        let worker_owner = owner.clone();
        thread::spawn(move || {
            worker_service.pause_before_dismiss_publication_for_tests();
            let publication = worker_service.start_publication_guard();
            let result = worker_service.dismiss_with_publication(
                &publication,
                &worker_owner,
                "dismiss-request",
                scan_id,
                candidate_id,
                sink,
            );
            sender
                .send(result)
                .expect("result receiver remains available");
        });

        assert!(
            hook.wait_until_entered(Duration::from_secs(1)),
            "dismiss must pause before it can mutate candidate state"
        );
        service.cancel_all();
        hook.release();

        let result = receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("dismiss request finishes after the test releases it");
        assert!(matches!(
            result,
            Err(LocalDiscoveryServiceError::ShuttingDown)
        ));
        assert_eq!(
            service.candidate_flags_for_tests(&owner, scan_id, candidate_id),
            Some((false, false, false))
        );
        assert_eq!(events.load(Ordering::Acquire), 0);
        assert_eq!(service.counts_for_tests(&owner).owner_requests, 0);
    }

    #[test]
    fn shutdown_wins_before_import_plan_preflight_without_private_work() {
        let service = Arc::new(test_service());
        let owner = test_owner("w57-import-shutdown");
        let scan_id = "scan-w57-import";
        let candidate_id = "candidate-w57-import";
        service.seed_completed_candidate_for_shutdown_tests(&owner, scan_id, candidate_id);
        let hook = Arc::new(WorkerPauseHook::new());
        service.set_import_plan_before_state_hook_for_tests(Arc::clone(&hook));
        let (sender, receiver) = mpsc::channel();
        let worker_service = Arc::clone(&service);
        let worker_owner = owner.clone();
        thread::spawn(move || {
            worker_service.pause_before_import_plan_publication_for_tests();
            let publication = worker_service.start_publication_guard();
            let result = worker_service.begin_import_plan_with_publication(
                &publication,
                &worker_owner,
                scan_id,
                candidate_id,
                "project-w57-import",
                None,
            );
            sender
                .send(result)
                .expect("result receiver remains available");
        });

        assert!(
            hook.wait_until_entered(Duration::from_secs(1)),
            "import planning must pause before it can read private ACP state"
        );
        service.cancel_all();
        hook.release();

        let result = receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("import-plan request finishes after the test releases it");
        assert!(matches!(
            result,
            Err(LocalDiscoveryServiceError::ShuttingDown)
        ));
        assert_eq!(service.import_plan_preflight_attempts_for_tests(), 0);
        assert_eq!(service.counts_for_tests(&owner).owner_requests, 0);
    }

    // ------------------------------------------------------------------
    // W5.8: receipt bounds, operation idempotency, verification running
    // quotas, and import-plan in-flight quotas.
    // ------------------------------------------------------------------

    fn insert_receipt_for_tests(
        service: &LocalDiscoveryService,
        owner: &DiscoveryOwnerScope,
        command: &str,
        request_id: &str,
        scan_id: &str,
    ) {
        let mut state = lock_state(&service.state);
        let payload = json!({"scanId": scan_id});
        state.requests.insert(
            DiscoveryRequestKey {
                owner: owner.clone(),
                command: command.to_owned(),
                request_id: request_id.to_owned(),
            },
            DiscoveryRequestReceipt {
                payload_hash: request_hash(&payload),
                state: DiscoveryRequestReceiptState::Committed { response: payload },
            },
        );
    }

    fn insert_running_verification_for_tests(
        service: &LocalDiscoveryService,
        owner: &DiscoveryOwnerScope,
        scan_id: &str,
        candidate_id: &str,
    ) {
        let mut state = lock_state(&service.state);
        state.running_verifications.insert(
            VerificationKey {
                scan_id: scan_id.to_owned(),
                candidate_id: candidate_id.to_owned(),
            },
            owner.clone(),
        );
    }

    fn insert_inflight_import_plan_for_tests(
        service: &LocalDiscoveryService,
        owner: &DiscoveryOwnerScope,
        scan_id: &str,
        candidate_id: &str,
        project_id: &str,
    ) {
        let mut state = lock_state(&service.state);
        state.inflight_import_plans.insert(
            ImportPlanKey {
                scan_id: scan_id.to_owned(),
                candidate_id: candidate_id.to_owned(),
                project_id: project_id.to_owned(),
                model_selection: None,
            },
            owner.clone(),
        );
    }

    fn dismiss_candidate_for_tests(
        service: &LocalDiscoveryService,
        owner: &DiscoveryOwnerScope,
        request_id: &str,
        scan_id: &str,
        candidate_id: &str,
        sink: &EventSink,
    ) -> Result<Value, LocalDiscoveryServiceError> {
        let publication = service.start_publication_guard();
        service.dismiss_with_publication(
            &publication,
            owner,
            request_id,
            scan_id,
            candidate_id,
            Arc::clone(sink),
        )
    }

    #[test]
    fn repeated_dismiss_with_new_request_ids_creates_single_receipt_and_event() {
        let service = test_service();
        let owner = test_owner("dismiss-idempotent");
        let scan_id = "scan-dismiss-idem";
        let candidate_id = "candidate-dismiss-idem";
        service.seed_completed_candidate_for_shutdown_tests(&owner, scan_id, candidate_id);
        let (sink, events) = event_counter();

        let first = dismiss_candidate_for_tests(
            &service,
            &owner,
            "dismiss-1",
            scan_id,
            candidate_id,
            &sink,
        )
        .expect("first dismiss commits");
        assert_eq!(first["dismissed"], true);
        assert_eq!(first.get("alreadyDismissed"), None);

        for index in 2..=4 {
            let repeated = dismiss_candidate_for_tests(
                &service,
                &owner,
                &format!("dismiss-{index}"),
                scan_id,
                candidate_id,
                &sink,
            )
            .expect("repeated dismiss must not fail");
            assert_eq!(repeated["dismissed"], true);
            assert_eq!(
                repeated["alreadyDismissed"], true,
                "repeated dismiss must be reported as already dismissed"
            );
        }
        let counts = service.counts_for_tests(&owner);
        assert_eq!(
            counts.owner_requests, 1,
            "only the first dismiss may write a receipt"
        );
        assert_eq!(
            events.load(Ordering::Acquire),
            1,
            "only the first dismiss may emit an event"
        );
        assert_eq!(
            service.candidate_flags_for_tests(&owner, scan_id, candidate_id),
            Some((true, false, false))
        );
    }

    #[test]
    fn concurrent_dismiss_of_same_candidate_is_business_idempotent() {
        let service = Arc::new(test_service());
        let owner = test_owner("dismiss-concurrent");
        let scan_id = "scan-dismiss-concurrent";
        let candidate_id = "candidate-dismiss-concurrent";
        service.seed_completed_candidate_for_shutdown_tests(&owner, scan_id, candidate_id);
        let (sink, events) = event_counter();
        let barrier = Arc::new(std::sync::Barrier::new(8));
        let mut threads = Vec::new();
        for index in 0..8 {
            let service = Arc::clone(&service);
            let owner = owner.clone();
            let sink = Arc::clone(&sink);
            let barrier = Arc::clone(&barrier);
            threads.push(thread::spawn(move || {
                barrier.wait();
                let publication = service.start_publication_guard();
                service.dismiss_with_publication(
                    &publication,
                    &owner,
                    &format!("dismiss-{index}"),
                    scan_id,
                    candidate_id,
                    sink,
                )
            }));
        }
        let results = threads
            .into_iter()
            .map(|thread| thread.join().expect("dismiss thread must not panic"))
            .collect::<Vec<_>>();
        let firsts = results
            .iter()
            .filter(|result| {
                result
                    .as_ref()
                    .is_ok_and(|value| value.get("alreadyDismissed").is_none())
            })
            .count();
        assert_eq!(
            firsts, 1,
            "exactly one concurrent dismiss may be the first mutation"
        );
        assert_eq!(service.counts_for_tests(&owner).owner_requests, 1);
        assert_eq!(events.load(Ordering::Acquire), 1);
    }

    #[test]
    fn dismiss_receipt_flood_is_rejected_at_owner_and_global_caps_with_zero_side_effects() {
        let service = test_service();
        let owner = test_owner("dismiss-receipt-owner");
        let scan_id = "scan-dismiss-receipt-owner";
        let candidate_id = "candidate-dismiss-receipt-owner";
        service.seed_completed_candidate_for_shutdown_tests(&owner, scan_id, candidate_id);
        for index in 0..8 {
            insert_receipt_for_tests(
                &service,
                &owner,
                "agent.discovery.dismiss",
                &format!("prefill-{index}"),
                scan_id,
            );
        }
        let (sink, events) = event_counter();
        let rejected = dismiss_candidate_for_tests(
            &service,
            &owner,
            "dismiss-overflow",
            scan_id,
            candidate_id,
            &sink,
        );
        assert!(matches!(
            rejected,
            Err(LocalDiscoveryServiceError::OwnerReceiptCapacityExhausted)
        ));
        assert_eq!(events.load(Ordering::Acquire), 0);
        assert_eq!(
            service.candidate_flags_for_tests(&owner, scan_id, candidate_id),
            Some((false, false, false)),
            "capacity rejection must not mutate the candidate"
        );
        assert_eq!(service.counts_for_tests(&owner).owner_requests, 8);

        let global_service = test_service();
        let global_owner = test_owner("dismiss-receipt-global");
        let global_scan = "scan-dismiss-receipt-global";
        let global_candidate = "candidate-dismiss-receipt-global";
        global_service.seed_completed_candidate_for_shutdown_tests(
            &global_owner,
            global_scan,
            global_candidate,
        );
        for index in 0..8 {
            let other = test_owner(&format!("other-{index}"));
            let other_scan = format!("scan-other-{index}");
            global_service.seed_completed_candidate_for_shutdown_tests(
                &other,
                &other_scan,
                &format!("candidate-other-{index}"),
            );
            insert_receipt_for_tests(
                &global_service,
                &other,
                "agent.discovery.dismiss",
                &format!("prefill-{index}"),
                &other_scan,
            );
        }
        let (sink, events) = event_counter();
        let rejected = dismiss_candidate_for_tests(
            &global_service,
            &global_owner,
            "dismiss-overflow",
            global_scan,
            global_candidate,
            &sink,
        );
        assert!(matches!(
            rejected,
            Err(LocalDiscoveryServiceError::GlobalReceiptCapacityExhausted)
        ));
        assert_eq!(events.load(Ordering::Acquire), 0);
        assert_eq!(
            global_service.candidate_flags_for_tests(&global_owner, global_scan, global_candidate),
            Some((false, false, false))
        );
    }

    #[test]
    fn verify_receipt_flood_is_rejected_before_private_state_read() {
        let service = test_service();
        let owner = test_owner("verify-receipt-owner");
        let scan_id = "scan-verify-receipt-owner";
        let candidate_id = "candidate-verify-receipt-owner";
        service.seed_completed_candidate_for_shutdown_tests(&owner, scan_id, candidate_id);
        for index in 0..8 {
            insert_receipt_for_tests(
                &service,
                &owner,
                "agent.discovery.verify",
                &format!("prefill-{index}"),
                scan_id,
            );
        }
        let publication = service.start_publication_guard();
        let outcome = service.begin_verify_with_publication(
            &publication,
            VerifyRequest {
                owner: &owner,
                request_id: "verify-overflow",
                scan_id,
                candidate_id,
                consent: true,
                deadline: Duration::from_millis(100),
                event_sink: event_counter().0,
            },
        );
        assert!(matches!(
            outcome,
            Err(LocalDiscoveryServiceError::OwnerReceiptCapacityExhausted)
        ));
        assert_eq!(
            service.verify_private_state_attempts_for_tests(),
            0,
            "capacity rejection must not read private ACP state"
        );
        assert_eq!(
            service.candidate_flags_for_tests(&owner, scan_id, candidate_id),
            Some((false, false, false))
        );
        assert_eq!(
            service.counts_for_tests(&owner).owner_running_verifications,
            0
        );
        assert_eq!(
            service.counts_for_tests(&owner).owner_verification_waiters,
            0
        );
    }

    #[test]
    fn verification_running_capacity_is_checked_atomically_before_private_state_read() {
        let service = test_service();
        let owner = test_owner("verify-running-owner");
        let scan_id = "scan-verify-running-owner";
        let candidate_id = "candidate-verify-running-owner";
        service.seed_completed_candidate_for_shutdown_tests(&owner, scan_id, candidate_id);
        for index in 0..2 {
            insert_running_verification_for_tests(
                &service,
                &owner,
                scan_id,
                &format!("candidate-other-{index}"),
            );
        }
        let publication = service.start_publication_guard();
        let outcome = service.begin_verify_with_publication(
            &publication,
            VerifyRequest {
                owner: &owner,
                request_id: "verify-overflow",
                scan_id,
                candidate_id,
                consent: true,
                deadline: Duration::from_millis(100),
                event_sink: event_counter().0,
            },
        );
        assert!(matches!(
            outcome,
            Err(LocalDiscoveryServiceError::OwnerVerificationCapacityExhausted)
        ));
        assert_eq!(service.verify_private_state_attempts_for_tests(), 0);
        assert_eq!(
            service.candidate_flags_for_tests(&owner, scan_id, candidate_id),
            Some((false, false, false))
        );
        assert_eq!(service.counts_for_tests(&owner).owner_requests, 0);

        let global_service = test_service();
        let global_owner = test_owner("verify-running-global");
        let global_scan = "scan-verify-running-global";
        let global_candidate = "candidate-verify-running-global";
        global_service.seed_completed_candidate_for_shutdown_tests(
            &global_owner,
            global_scan,
            global_candidate,
        );
        for index in 0..2 {
            insert_running_verification_for_tests(
                &global_service,
                &test_owner(&format!("other-{index}")),
                &format!("scan-other-{index}"),
                "candidate-other",
            );
        }
        let publication = global_service.start_publication_guard();
        let outcome = global_service.begin_verify_with_publication(
            &publication,
            VerifyRequest {
                owner: &global_owner,
                request_id: "verify-overflow",
                scan_id: global_scan,
                candidate_id: global_candidate,
                consent: true,
                deadline: Duration::from_millis(100),
                event_sink: event_counter().0,
            },
        );
        assert!(matches!(
            outcome,
            Err(LocalDiscoveryServiceError::GlobalVerificationCapacityExhausted)
        ));
        assert_eq!(global_service.verify_private_state_attempts_for_tests(), 0);
    }

    #[test]
    fn verify_replay_is_stable_across_deadline_variation() {
        let service = test_service();
        let owner = test_owner("verify-replay-deadline");
        let scan_id = "scan-verify-replay-deadline";
        let candidate_id = "candidate-verify-replay-deadline";
        service.seed_completed_candidate_for_shutdown_tests(&owner, scan_id, candidate_id);

        // Seed a committed verify receipt whose hash binds only scanId/candidateId/
        // consent (no deadline), exactly as begin_verify_with_publication computes.
        let committed = json!({"scanId": scan_id, "candidateId": candidate_id, "accepted": true, "state": "verified", "reused": false});
        {
            let mut state = lock_state(&service.state);
            let payload = json!({"scanId": scan_id, "candidateId": candidate_id, "consent": true});
            state.requests.insert(
                DiscoveryRequestKey {
                    owner: owner.clone(),
                    command: "agent.discovery.verify".into(),
                    request_id: "verify-replay".into(),
                },
                DiscoveryRequestReceipt {
                    payload_hash: request_hash(&payload),
                    state: DiscoveryRequestReceiptState::Committed {
                        response: committed.clone(),
                    },
                },
            );
        }
        let attempts_before = service.verify_private_state_attempts_for_tests();
        let requests_before = service.counts_for_tests(&owner).owner_requests;

        // A retry with the same requestId but a different deadline is the same
        // business intent: it must replay the committed receipt exactly, with
        // no new receipt, thread, private read, event, or child.
        let (sink, events) = event_counter();
        let publication = service.start_publication_guard();
        let replay = service.begin_verify_with_publication(
            &publication,
            VerifyRequest {
                owner: &owner,
                request_id: "verify-replay",
                scan_id,
                candidate_id,
                consent: true,
                deadline: Duration::from_millis(800),
                event_sink: sink,
            },
        );
        assert!(
            matches!(replay, Ok(VerifyStartOutcome::Replayed(response)) if response == committed),
            "a deadline-only retry must replay the committed receipt exactly"
        );
        drop(publication);
        assert_eq!(
            service.verify_private_state_attempts_for_tests(),
            attempts_before,
            "replay must not read private ACP state"
        );
        assert_eq!(
            service.counts_for_tests(&owner).owner_requests,
            requests_before,
            "replay must not allocate a new receipt"
        );
        assert_eq!(
            events.load(Ordering::SeqCst),
            0,
            "replay must publish no event"
        );

        // A real business-intent change (consent) still conflicts.
        let publication = service.start_publication_guard();
        let conflict = service.begin_verify_with_publication(
            &publication,
            VerifyRequest {
                owner: &owner,
                request_id: "verify-replay",
                scan_id,
                candidate_id,
                consent: false,
                deadline: Duration::from_millis(100),
                event_sink: event_counter().0,
            },
        );
        assert!(
            matches!(conflict, Err(LocalDiscoveryServiceError::RequestIdReuse)),
            "a consent change must remain REQUEST_ID_REUSE"
        );
        drop(publication);
    }

    #[test]
    fn import_plan_flood_is_rejected_at_caps_with_zero_private_work() {
        let service = test_service();
        let owner = test_owner("import-plan-owner");
        let scan_id = "scan-import-plan-owner";
        let candidate_id = "candidate-import-plan-owner";
        service.seed_completed_candidate_for_shutdown_tests(&owner, scan_id, candidate_id);
        for index in 0..2 {
            insert_inflight_import_plan_for_tests(
                &service,
                &owner,
                scan_id,
                &format!("candidate-other-{index}"),
                &format!("project-{index}"),
            );
        }
        let publication = service.start_publication_guard();
        let rejected = service.begin_import_plan_with_publication(
            &publication,
            &owner,
            scan_id,
            candidate_id,
            "project-w58",
            None,
        );
        assert!(matches!(
            rejected,
            Err(LocalDiscoveryServiceError::OwnerImportPlanCapacityExhausted)
        ));
        assert_eq!(
            service.import_plan_preflight_attempts_for_tests(),
            0,
            "capacity rejection must not read private ACP state or run metadata work"
        );

        let dedup_service = test_service();
        let dedup_owner = test_owner("import-plan-dedup");
        let dedup_scan = "scan-import-plan-dedup";
        let dedup_candidate = "candidate-import-plan-dedup";
        dedup_service.seed_completed_candidate_for_shutdown_tests(
            &dedup_owner,
            dedup_scan,
            dedup_candidate,
        );
        insert_inflight_import_plan_for_tests(
            &dedup_service,
            &dedup_owner,
            dedup_scan,
            dedup_candidate,
            "project-w58",
        );
        let publication = dedup_service.start_publication_guard();
        let rejected = dedup_service.begin_import_plan_with_publication(
            &publication,
            &dedup_owner,
            dedup_scan,
            dedup_candidate,
            "project-w58",
            None,
        );
        assert!(matches!(
            rejected,
            Err(LocalDiscoveryServiceError::ImportPlanInFlight)
        ));
        assert_eq!(dedup_service.import_plan_preflight_attempts_for_tests(), 0);

        let global_service = test_service();
        let global_owner = test_owner("import-plan-global");
        let global_scan = "scan-import-plan-global";
        let global_candidate = "candidate-import-plan-global";
        global_service.seed_completed_candidate_for_shutdown_tests(
            &global_owner,
            global_scan,
            global_candidate,
        );
        for index in 0..2 {
            let other = test_owner(&format!("other-{index}"));
            let other_scan = format!("scan-other-{index}");
            global_service.seed_completed_candidate_for_shutdown_tests(
                &other,
                &other_scan,
                "candidate-x",
            );
            insert_inflight_import_plan_for_tests(
                &global_service,
                &other,
                &other_scan,
                "candidate-x",
                "project-x",
            );
        }
        let publication = global_service.start_publication_guard();
        let rejected = global_service.begin_import_plan_with_publication(
            &publication,
            &global_owner,
            global_scan,
            global_candidate,
            "project-w58",
            None,
        );
        assert!(matches!(
            rejected,
            Err(LocalDiscoveryServiceError::GlobalImportPlanCapacityExhausted)
        ));
        assert_eq!(global_service.import_plan_preflight_attempts_for_tests(), 0);
    }

    #[test]
    fn import_plan_lease_is_released_on_error_paths_and_shutdown() {
        let service = test_service();
        let owner = test_owner("import-plan-lease");
        let scan_id = "scan-import-plan-lease";
        let candidate_id = "candidate-import-plan-lease";
        service.seed_completed_candidate_for_shutdown_tests(&owner, scan_id, candidate_id);
        let rejected = {
            let publication = service.start_publication_guard();
            service.begin_import_plan_with_publication(
                &publication,
                &owner,
                scan_id,
                candidate_id,
                "project-w58",
                None,
            )
        };
        // The seeded candidate has no verification: the lease is acquired and
        // then released on the CandidateNotVerified error path.
        assert!(matches!(
            rejected,
            Err(LocalDiscoveryServiceError::CandidateNotVerified)
        ));
        assert_eq!(
            service.counts_for_tests(&owner).owner_inflight_import_plans,
            0,
            "error path must release the in-flight lease"
        );

        insert_inflight_import_plan_for_tests(
            &service,
            &owner,
            scan_id,
            candidate_id,
            "project-w58",
        );
        insert_running_verification_for_tests(&service, &owner, scan_id, "candidate-other");
        service.cancel_all();
        let counts = service.counts_for_tests(&owner);
        assert_eq!(counts.owner_inflight_import_plans, 0);
        assert_eq!(counts.owner_running_verifications, 0);
        assert_eq!(counts.owner_verification_waiters, 0);
        assert_eq!(counts.owner_lease_waiters, 0);

        let publication = service.start_publication_guard();
        assert!(matches!(
            service.begin_verify_with_publication(
                &publication,
                VerifyRequest {
                    owner: &owner,
                    request_id: "verify-after-shutdown",
                    scan_id,
                    candidate_id,
                    consent: true,
                    deadline: Duration::from_millis(100),
                    event_sink: event_counter().0,
                },
            ),
            Err(LocalDiscoveryServiceError::ShuttingDown)
        ));
        drop(publication);
        assert!(matches!(
            dismiss_candidate_for_tests(
                &service,
                &owner,
                "dismiss-after-shutdown",
                scan_id,
                candidate_id,
                &event_counter().0,
            ),
            Err(LocalDiscoveryServiceError::ShuttingDown)
        ));
        let publication = service.start_publication_guard();
        assert!(matches!(
            service.begin_import_plan_with_publication(
                &publication,
                &owner,
                scan_id,
                candidate_id,
                "project-w58",
                None,
            ),
            Err(LocalDiscoveryServiceError::ShuttingDown)
        ));
        drop(publication);
    }

    #[test]
    fn dismiss_and_verify_receipts_are_pruned_on_session_expiry() {
        let service = test_service();
        let owner = test_owner("receipt-expiry");
        let scan_id = "scan-receipt-expiry";
        let candidate_id = "candidate-receipt-expiry";
        service.seed_completed_candidate_for_shutdown_tests(&owner, scan_id, candidate_id);
        let (sink, _) = event_counter();
        dismiss_candidate_for_tests(&service, &owner, "dismiss-1", scan_id, candidate_id, &sink)
            .expect("dismiss commits a receipt");
        assert_eq!(service.counts_for_tests(&owner).owner_requests, 1);
        service.expire_all_sessions_for_tests();
        assert_eq!(service.counts_for_tests(&owner).owner_requests, 0);
        assert_eq!(service.counts_for_tests(&owner).owner_sessions, 0);
    }

    #[test]
    fn request_id_replay_does_not_consume_receipt_quota_and_stays_request_reuse() {
        let service = test_service();
        let owner = test_owner("receipt-replay");
        let scan_id = "scan-receipt-replay";
        let candidate_id = "candidate-receipt-replay";
        service.seed_completed_candidate_for_shutdown_tests(&owner, scan_id, candidate_id);
        let (sink, events) = event_counter();
        let first = dismiss_candidate_for_tests(
            &service,
            &owner,
            "dismiss-1",
            scan_id,
            candidate_id,
            &sink,
        )
        .expect("first dismiss");
        assert_eq!(service.counts_for_tests(&owner).owner_requests, 1);
        let replay = dismiss_candidate_for_tests(
            &service,
            &owner,
            "dismiss-1",
            scan_id,
            candidate_id,
            &sink,
        )
        .expect("same requestId replays");
        assert_eq!(replay, first);
        assert_eq!(
            service.counts_for_tests(&owner).owner_requests,
            1,
            "replay must not write another receipt"
        );
        assert_eq!(
            events.load(Ordering::Acquire),
            1,
            "replay must not emit another event"
        );

        let reused = dismiss_candidate_for_tests(
            &service,
            &owner,
            "dismiss-1",
            scan_id,
            "candidate-other",
            &event_counter().0,
        );
        assert!(matches!(
            reused,
            Err(LocalDiscoveryServiceError::RequestIdReuse)
        ));
    }

    fn wait_for_running_to_drain(service: &LocalDiscoveryService, owner: &DiscoveryOwnerScope) {
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            if service.counts_for_tests(owner).owner_running_scans == 0 {
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
        panic!(
            "running scan slot did not drain: {:?}",
            service.counts_for_tests(owner)
        );
    }
}
