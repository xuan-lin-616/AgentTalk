use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ExecutionStatus {
    Pending,
    Assembling,
    AwaitingApproval,
    Running,
    Verifying,
    Completed,
    Failed,
    Cancelled,
    Interrupted,
}

impl ExecutionStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::Interrupted
        )
    }

    pub fn can_transition_to(&self, next: &Self) -> bool {
        use ExecutionStatus::*;
        match self {
            Pending => matches!(next, Assembling | Failed | Cancelled | Interrupted),
            Assembling => matches!(
                next,
                AwaitingApproval | Running | Failed | Cancelled | Interrupted
            ),
            AwaitingApproval => matches!(next, Running | Failed | Cancelled | Interrupted),
            Running => matches!(next, Verifying | Failed | Cancelled | Interrupted),
            Verifying => matches!(next, Completed | Failed | Cancelled | Interrupted),
            Completed | Failed | Cancelled | Interrupted => false,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum WorkspaceAccess {
    None,
    ReadOnly,
    WorkspaceWrite,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScopeSnapshot {
    pub project_id: String,
    pub conversation_id: String,
    pub agent_id: String,
    pub workspace_access: WorkspaceAccess,
    pub canonical_cwd: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Project {
    pub id: String,
    pub name: String,
    pub root_path: Option<String>,
    pub archived: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Conversation {
    pub id: String,
    pub project_id: String,
    pub title: String,
    pub scope_revision: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AgentIdentity {
    pub id: String,
    pub name: String,
    pub role: String,
    pub specialty: String,
    pub system_prompt: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RuntimeProfile {
    pub id: String,
    pub connector_id: String,
    pub runtime_type: String,
}

/// The connector profile registry is intentionally global to the local Core
/// host.  It stores only non-secret identity metadata; provider calls and
/// credential resolution remain outside this contract.
pub const CONNECTOR_PROFILE_SCOPE: &str = "desktop";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConnectorProfile {
    pub scope_id: String,
    pub connector_id: String,
    pub display_name: String,
    pub provider_type: String,
    pub runtime_type: String,
    pub enabled: bool,
    /// Environment variable name only.  The value is never part of this
    /// struct, persistence schema, IPC response, or projection.
    pub auth_env_key: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscoveryState {
    Observed,
    Identified,
    Disappeared,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompatibilityState {
    NotVerified,
    Compatible,
    Incompatible,
    AdapterRequired,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthState {
    Unknown,
    NotRequired,
    Required,
    Ready,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthState {
    NotChecked,
    Ready,
    Unavailable,
    IdentityMismatch,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateAvailability {
    Unavailable,
    Unconfigured,
    AuthenticationRequired,
    Available,
}

impl CandidateAvailability {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unavailable => "unavailable",
            Self::Unconfigured => "unconfigured",
            Self::AuthenticationRequired => "authentication_required",
            Self::Available => "available",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateCategory {
    AgentRuntime,
    ModelRuntime,
    ToolService,
    Unknown,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservationSourceKind {
    ExecutableInventory,
    WindowsPath,
    WindowsAppPath,
    WindowsPackage,
    LoopbackListener,
    UserSelected,
    RuntimeRecord,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservationTrustLevel {
    FirstParty,
    UserSelected,
    Heuristic,
    Untrusted,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationAuthority {
    Authoritative,
    Heuristic,
    Unverified,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscoveryDiagnosticCode {
    ProviderFailed,
    ProviderTimeout,
    AccessDenied,
    SourceDisappeared,
    InvalidSourceRecord,
    ReparsePointRejected,
    NonLoopbackRejected,
    ConnectorConflict,
    RuntimeTypeConflict,
    StateConflict,
    CategoryConflict,
    DiscoveryStateConflict,
    CatalogConflict,
    InvalidIdentity,
    FingerprintUnavailable,
    FingerprintChanged,
    ShortRead,
    OversizedInput,
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DiscoveryDiagnostic {
    pub source_kind: ObservationSourceKind,
    pub code: DiscoveryDiagnosticCode,
}

/// Discovery policy remains credential-free and renderer-safe. The runtime
/// can thread it into provider and coordinator gates without exposing any
/// locator or process data.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DiscoveryPolicy {
    pub max_results: usize,
    pub timeout_ms: u64,
    pub allow_active_verification: bool,
    pub allow_lan: bool,
}

impl Default for DiscoveryPolicy {
    fn default() -> Self {
        Self {
            max_results: 32,
            timeout_ms: 2_000,
            allow_active_verification: false,
            allow_lan: false,
        }
    }
}

/// A renderer-safe candidate projection. It may be displayed and serialized,
/// but it intentionally omits locators, credentials, pids, ports, and raw
/// source material.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CandidateProjection {
    pub candidate_id: String,
    pub category: CandidateCategory,
    pub connector_id: String,
    pub runtime_type: String,
    pub display_name: String,
    pub availability: CandidateAvailability,
    pub models: Vec<String>,
    pub catalog_revision: Option<String>,
    pub requires_configuration: bool,
    pub source_kind: ObservationSourceKind,
    pub source_kinds: Vec<ObservationSourceKind>,
    pub trust_level: ObservationTrustLevel,
    pub verification_authority: VerificationAuthority,
    pub availability_authority: VerificationAuthority,
    pub discovery_authority: VerificationAuthority,
    pub compatibility_authority: VerificationAuthority,
    pub auth_authority: VerificationAuthority,
    pub health_authority: VerificationAuthority,
    pub catalog_source_kind: Option<ObservationSourceKind>,
    pub catalog_trust_level: Option<ObservationTrustLevel>,
    pub catalog_authority: Option<VerificationAuthority>,
    pub discovery_state: DiscoveryState,
    pub compatibility_state: CompatibilityState,
    pub auth_state: AuthState,
    pub health_state: HealthState,
    pub evidence_summary: Vec<DiscoveryEvidence>,
    pub diagnostics: Vec<DiscoveryDiagnostic>,
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscoveryEvidence {
    ExecutableInventory,
    WindowsPathEntry,
    WindowsAppPathRegistry,
    WindowsPackageInventory,
    LoopbackListener,
    UserSelected,
    RuntimeRecord,
    VersionMatched,
    BuildMatched,
    InstallKnown,
    Available,
    AuthenticationRequired,
    Unconfigured,
    IdentityMismatch,
    CatalogUnavailable,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ModelCandidate {
    pub id: String,
    pub connector_id: String,
    pub model_id: String,
    pub available: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ModelCandidateList {
    pub identity_id: String,
    pub revision: String,
    pub options: Vec<ModelCandidate>,
    pub default_model_id: Option<String>,
}

/// The model/connector catalog identity frozen for one ExecutionRun.
///
/// Legacy migrations may leave the optional fields empty. New Core-created
/// runs always write a connector identity and a deterministic catalog
/// revision, while `model_id` remains optional for runtimes that resolve their
/// default model internally.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelSnapshot {
    pub run_id: String,
    pub connector_id: Option<String>,
    pub model_id: Option<String>,
    pub revision: Option<u64>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelSelectionMode {
    Inherit,
    ConnectorDefault,
    Pinned,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ModelSelection {
    pub mode: ModelSelectionMode,
    pub model_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProjectAgentAssignment {
    pub project_id: String,
    pub agent_id: String,
    pub enabled: bool,
    pub workspace_access: WorkspaceAccess,
    pub model_selection: ModelSelection,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ConversationAgentAssignment {
    pub conversation_id: String,
    pub agent_id: String,
    pub enabled: bool,
    pub model_selection: Option<ModelSelection>,
}

/// Scope of a persisted identity-owned candidate model list.  These values
/// mirror the legacy AgentTalk contract and are intentionally independent of
/// the Connector's discovered catalog.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityModelListScope {
    BaseAgent,
    ProjectAgent,
    ConversationAgent,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityModelListMode {
    Own,
    Inherit,
    Override,
    LegacyCompatibility,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelOptionSource {
    Runtime,
    Config,
    Manual,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelAvailability {
    Available,
    Unverified,
    Unavailable,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelSelectionSource {
    Conversation,
    Project,
    BaseAgent,
    ConnectorDefault,
    IdentityDefault,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IdentityModelOption {
    pub id: String,
    pub scope: IdentityModelListScope,
    pub agent_id: String,
    pub project_id: Option<String>,
    pub conversation_id: Option<String>,
    pub model_id: String,
    pub display_name: String,
    pub connector_id: String,
    pub source: ModelOptionSource,
    pub availability: ModelAvailability,
    pub is_default: bool,
    pub sort_order: u64,
    pub catalog_revision: Option<String>,
    pub context_window: Option<u64>,
    pub reasoning_efforts: Vec<String>,
    pub service_tiers: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IdentityModelListTarget {
    pub scope: IdentityModelListScope,
    pub agent_id: String,
    pub project_id: Option<String>,
    pub conversation_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IdentityModelListSnapshot {
    pub scope: IdentityModelListScope,
    pub mode: IdentityModelListMode,
    pub revision: u64,
    pub hash: String,
    pub option_count: u64,
}

/// Full resolved model selection frozen alongside the existing connector
/// ModelSnapshot.  Keeping this as an additive record preserves old nullable
/// `model_snapshots` rows while allowing normal Retry to retain the complete
/// layered selection decision.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ModelSelectionSnapshot {
    pub run_id: String,
    pub version: u8,
    pub runtime_type: String,
    pub provider_type: String,
    pub connector_id: String,
    pub effective_model_id: Option<String>,
    pub selection_source: ModelSelectionSource,
    pub selection_mode: ModelSelectionMode,
    pub availability: ModelAvailability,
    pub catalog_revision: Option<String>,
    pub context_window: Option<u64>,
    pub reasoning_efforts: Vec<String>,
    pub service_tiers: Vec<String>,
    pub candidate_model_list: Option<IdentityModelListSnapshot>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkspaceAuthorization {
    pub project_id: String,
    pub canonical_root: String,
    pub revision: u64,
    pub validation_status: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkspacePermission {
    pub project_id: String,
    pub agent_id: String,
    pub access: WorkspaceAccess,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Message {
    pub id: String,
    pub conversation_id: String,
    pub sender_id: String,
    pub sequence: u64,
    pub content: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Attachment {
    pub id: String,
    pub message_id: String,
    pub artifact_id: String,
    pub file_name: String,
    pub sha256: String,
    pub size: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkflowTemplate {
    pub id: String,
    pub name: String,
    pub kind: String,
    pub steps: Vec<WorkflowStep>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowStep {
    pub id: String,
    pub order: u32,
    pub agent_id: String,
    pub prompt_supplement: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StructuredHandoffDetails {
    pub parent_execution_run_id: Option<String>,
    pub child_execution_run_id: Option<String>,
    pub source_message_id: Option<String>,
    pub from_agent_id: Option<String>,
    pub to_agent_id: Option<String>,
    pub kind: Option<String>,
    pub dispatch_mode: Option<String>,
    pub batch_id: Option<String>,
    pub sequence_index: Option<u64>,
    pub detected_by: Option<String>,
    pub task: Option<String>,
    pub reason: Option<String>,
    pub decisions: Option<Vec<String>>,
    pub constraints: Option<Vec<String>>,
    pub artifacts: Option<Vec<String>>,
    pub expected_output: Option<String>,
    pub context_scope: Option<String>,
    /// Core-derived ancestor agent path used for cycle diagnostics. Clients
    /// may omit it; Core must never trust a client-supplied path as authority.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_path: Option<Vec<String>>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Handoff {
    pub id: String,
    pub collaboration_run_id: String,
    pub from_execution_run_id: String,
    pub to_agent_id: String,
    pub status: String,
    #[serde(default)]
    pub details: Option<StructuredHandoffDetails>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContextBundle {
    pub current_task: String,
    pub rendered_context: String,
    pub source_ids: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContextManifest {
    pub id: String,
    pub execution_run_id: String,
    pub schema_version: String,
    pub source_ids: Vec<String>,
    pub workspace_access: WorkspaceAccess,
    pub canonical_cwd: Option<String>,
    #[serde(default)]
    pub connector_id: Option<String>,
    #[serde(default)]
    pub model_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Summary {
    pub id: String,
    pub scope_id: String,
    pub version: u64,
    pub content_hash: String,
    pub artifact_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MemoryItem {
    pub id: String,
    pub scope_id: String,
    pub agent_id: Option<String>,
    pub content_hash: String,
    pub confirmed: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RetrievalSource {
    pub id: String,
    pub scope_id: String,
    pub citation: String,
    pub sha256: String,
    pub token_count: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RetrievalSelectionScope {
    Project,
    Conversation,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RetrievalMatchMethod {
    ExactPhrase,
    ExactTerms,
    PathExact,
    RgFixedStrings,
    BoundedFileScan,
    ExplicitSelection,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RetrievalSelectionReason {
    ExactPhrase,
    ExactTerms,
    PathExact,
    ExplicitUserChoice,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RetrievalLineRange {
    pub start: Option<u32>,
    pub end: Option<u32>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RetrievalSelectionItem {
    pub source_id: String,
    pub source_hash: String,
    pub rank: u32,
    pub score_milli: u16,
    pub match_method: RetrievalMatchMethod,
    pub reason: RetrievalSelectionReason,
    pub range: Option<RetrievalLineRange>,
}

/// A durable, exact source selection. It contains only source metadata and
/// immutable scope/query hashes; source bodies and prompts are intentionally
/// outside this contract.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RetrievalSelection {
    pub id: String,
    pub scope: RetrievalSelectionScope,
    pub scope_id: String,
    pub project_id: String,
    pub conversation_id: Option<String>,
    pub scope_revision: u64,
    pub workspace_revision: Option<u64>,
    pub retrieval_version: String,
    pub query_hash: String,
    pub items: Vec<RetrievalSelectionItem>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RetrievalFeedbackLabel {
    Helpful,
    NotHelpful,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RetrievalFeedbackReason {
    ExactMatch,
    Irrelevant,
    StaleSource,
    WrongScope,
    Duplicate,
    Permission,
}

/// Structured feedback for one explicitly selected source. Free-form text is
/// deliberately absent so feedback cannot become a prompt/secret sink.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RetrievalFeedback {
    pub id: String,
    pub selection_id: String,
    pub scope_id: String,
    pub source_id: String,
    pub label: RetrievalFeedbackLabel,
    pub reason: RetrievalFeedbackReason,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Artifact {
    pub id: String,
    pub sha256: String,
    pub size: u64,
    pub mime: String,
    pub relative_path: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Approval {
    pub id: String,
    pub execution_run_id: String,
    pub kind: String,
    pub decision: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RuntimeEvent {
    pub id: String,
    pub execution_run_id: String,
    pub event_type: String,
    pub sequence: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AuditEvent {
    pub id: String,
    pub scope_id: Option<String>,
    pub action: String,
    pub occurred_at_ms: i64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionRun {
    pub id: String,
    pub collaboration_run_id: String,
    pub project_id: String,
    pub conversation_id: String,
    pub agent_id: String,
    pub status: ExecutionStatus,
    pub version: u64,
    pub scope: ScopeSnapshot,
    pub terminal_reason: Option<String>,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum StateTransitionError {
    #[error("execution run version conflict: expected {expected}, actual {actual}")]
    VersionConflict { expected: u64, actual: u64 },
    #[error("terminal execution run cannot transition from {from:?} to {to:?}")]
    TerminalImmutable {
        from: ExecutionStatus,
        to: ExecutionStatus,
    },
    #[error("invalid execution transition from {from:?} to {to:?}")]
    InvalidTransition {
        from: ExecutionStatus,
        to: ExecutionStatus,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransitionOutcome {
    Applied,
    Idempotent,
}

impl ExecutionRun {
    pub fn transition(
        &mut self,
        next: ExecutionStatus,
        expected_version: u64,
        reason: Option<String>,
    ) -> Result<TransitionOutcome, StateTransitionError> {
        if expected_version != self.version {
            return Err(StateTransitionError::VersionConflict {
                expected: expected_version,
                actual: self.version,
            });
        }
        if self.status == next {
            return Ok(TransitionOutcome::Idempotent);
        }
        if self.status.is_terminal() {
            return Err(StateTransitionError::TerminalImmutable {
                from: self.status.clone(),
                to: next,
            });
        }
        if !self.status.can_transition_to(&next) {
            return Err(StateTransitionError::InvalidTransition {
                from: self.status.clone(),
                to: next,
            });
        }
        self.status = next;
        self.version += 1;
        if self.status.is_terminal() {
            self.terminal_reason = reason;
        }
        Ok(TransitionOutcome::Applied)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum CollaborationStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
    Interrupted,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CollaborationRun {
    pub id: String,
    pub root_agent_ids: Vec<String>,
    pub call_count: u32,
    pub max_calls: u32,
    pub depth: u32,
    pub max_depth: u32,
    pub status: CollaborationStatus,
    pub stop_reason: Option<String>,
    #[serde(default)]
    pub auto_dispatch_handoffs: bool,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum CollaborationError {
    #[error("collaboration call limit reached")]
    MaxCalls,
    #[error("collaboration depth limit reached")]
    MaxDepth,
    #[error("collaboration is terminal")]
    Terminal,
}

impl CollaborationRun {
    pub fn record_call(&mut self, depth: u32) -> Result<(), CollaborationError> {
        if self.status != CollaborationStatus::Pending
            && self.status != CollaborationStatus::Running
        {
            return Err(CollaborationError::Terminal);
        }
        if self.call_count >= self.max_calls {
            return Err(CollaborationError::MaxCalls);
        }
        if depth > self.max_depth {
            return Err(CollaborationError::MaxDepth);
        }
        self.call_count += 1;
        self.depth = self.depth.max(depth);
        self.status = CollaborationStatus::Running;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run() -> ExecutionRun {
        ExecutionRun {
            id: "run-1".into(),
            collaboration_run_id: "collab-1".into(),
            project_id: "project-1".into(),
            conversation_id: "conversation-1".into(),
            agent_id: "agent-1".into(),
            status: ExecutionStatus::Pending,
            version: 0,
            scope: ScopeSnapshot {
                project_id: "project-1".into(),
                conversation_id: "conversation-1".into(),
                agent_id: "agent-1".into(),
                workspace_access: WorkspaceAccess::ReadOnly,
                canonical_cwd: None,
            },
            terminal_reason: None,
        }
    }

    #[test]
    fn execution_state_machine_is_one_way_and_terminal_is_immutable() {
        let mut value = run();
        assert_eq!(
            value.transition(ExecutionStatus::Assembling, 0, None),
            Ok(TransitionOutcome::Applied)
        );
        assert_eq!(
            value.transition(ExecutionStatus::Running, 1, None),
            Ok(TransitionOutcome::Applied)
        );
        assert_eq!(
            value.transition(ExecutionStatus::Verifying, 2, None),
            Ok(TransitionOutcome::Applied)
        );
        assert_eq!(
            value.transition(ExecutionStatus::Completed, 3, None),
            Ok(TransitionOutcome::Applied)
        );
        assert!(matches!(
            value.transition(ExecutionStatus::Failed, 4, Some("late".into())),
            Err(StateTransitionError::TerminalImmutable { .. })
        ));
    }

    #[test]
    fn command_is_idempotent_at_same_state() {
        let mut value = run();
        assert_eq!(
            value.transition(ExecutionStatus::Pending, 0, None),
            Ok(TransitionOutcome::Idempotent)
        );
        assert_eq!(value.version, 0);
    }

    #[test]
    fn collaboration_enforces_call_and_depth_limits() {
        let mut value = CollaborationRun {
            id: "collab".into(),
            root_agent_ids: vec!["a".into()],
            call_count: 0,
            max_calls: 1,
            depth: 0,
            max_depth: 2,
            status: CollaborationStatus::Pending,
            stop_reason: None,
            auto_dispatch_handoffs: false,
        };
        assert_eq!(value.record_call(2), Ok(()));
        assert_eq!(value.record_call(1), Err(CollaborationError::MaxCalls));
    }

    #[test]
    fn structured_handoff_and_auto_dispatch_are_legacy_json_compatible() {
        let handoff: Handoff = serde_json::from_str(
            r#"{
                "id":"handoff-legacy",
                "collaboration_run_id":"collab-legacy",
                "from_execution_run_id":"run-legacy",
                "to_agent_id":"agent-legacy",
                "status":"proposed"
            }"#,
        )
        .unwrap();
        assert_eq!(handoff.details, None);

        let run: CollaborationRun = serde_json::from_str(
            r#"{
                "id":"collab-legacy",
                "rootAgentIds":[],
                "callCount":0,
                "maxCalls":1,
                "depth":0,
                "maxDepth":1,
                "status":"Pending",
                "stopReason":null
            }"#,
        )
        .unwrap();
        assert!(!run.auto_dispatch_handoffs);
    }
}
