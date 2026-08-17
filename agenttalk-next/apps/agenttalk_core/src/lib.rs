use agenttalk_brief_sealer::{BriefSealer, PreparedBriefSeal};
use agenttalk_context::{
    AssembledContext, AttachmentContextSource, ContextAssembler, ContextInput,
};
use agenttalk_domain::{
    AgentIdentity, Artifact, Attachment, CollaborationRun, ConnectorProfile, Conversation,
    ExecutionRun, ExecutionStatus, Handoff, IdentityModelListMode, IdentityModelListScope,
    IdentityModelListSnapshot, IdentityModelListTarget, IdentityModelOption, MemoryItem, Message,
    ModelAvailability, ModelOptionSource, ModelSelection, ModelSelectionMode,
    ModelSelectionSnapshot, ModelSelectionSource, ModelSnapshot, Project, RetrievalFeedback,
    RetrievalSelection, RetrievalSource, ScopeSnapshot, StateTransitionError,
    StructuredHandoffDetails, Summary, TransitionOutcome, WorkflowStep, WorkflowTemplate,
    WorkspaceAccess, WorkspaceAuthorization,
};
use agenttalk_events::{EventStore, EventStoreError, InMemoryEventStore, RuntimeEvent};
use agenttalk_orchestration_contracts::registry::SchemaRegistry;
use agenttalk_permissions::FileReadGrant;
use agenttalk_runtime_host::{
    connector_runtime_failure, LocalConnectorCandidate, RuntimeAdapter, RuntimeCapabilities,
    RuntimeError, RuntimeEventStream, RuntimeRequest,
};
use agenttalk_storage::{
    AgentModelBinding, AgentModelBindingPatch, ArtifactBodyChunk, CommandReceipt,
    CommandReceiptKey, LocalAgentImportOutcome, LocalAgentImportRequest, OrchestrationRunRecord,
    RetrievalEmbeddingProvider, RetrievalPreviewRequest, SqliteStore, StorageError,
    StoredModelSelection,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use thiserror::Error;

#[cfg(test)]
use agenttalk_runtime_host::{
    ConfiguredAdapter, RuntimeDiscovery, RuntimeHealth, RuntimeModelMetadata,
};
#[cfg(test)]
use agenttalk_storage::CommandReceiptState;

#[derive(Debug, Error)]
pub enum CoreError {
    #[error(transparent)]
    BriefSeal(#[from] agenttalk_brief_sealer::BriefSealError),
    #[error("agent is not assigned to the current Project")]
    AgentNotAssigned,
    #[error("execution run id already exists")]
    RunAlreadyExists,
    #[error("requested workspace access exceeds the Project-Agent assignment")]
    WorkspaceAccessDenied,
    #[error("Project workspace authorization is missing")]
    WorkspaceAuthorizationMissing,
    #[error("requested cwd is outside the authorized Project root")]
    WorkspacePathDenied,
    #[error("authorized workspace root is invalid: {0}")]
    InvalidWorkspaceRoot(String),
    #[error("invalid persisted workspace access: {0}")]
    InvalidWorkspaceAccess(String),
    #[error("execution run not found")]
    RunNotFound,
    #[error("execution retry source must be terminal")]
    RetrySourceNotTerminal,
    #[error("execution retry requires a non-empty current task")]
    RetryTaskMissing,
    #[error("execution retry requires a persisted model snapshot")]
    ModelSnapshotMissing,
    #[error("execution model snapshot conflicts with the persisted source snapshot")]
    ModelSnapshotConflict,
    #[error("execution retry requires a persisted full model selection snapshot")]
    ModelSelectionSnapshotMissing,
    #[error("execution model selection conflicts with the persisted or requested binding")]
    ModelSelectionSnapshotConflict,
    #[error("projection entity not found")]
    ProjectionEntityNotFound,
    #[error("connector profile does not exist")]
    ConnectorNotFound,
    #[error("connector profile is disabled")]
    ConnectorDisabled,
    #[error("connector profile runtime does not match the active RuntimeAdapter")]
    ConnectorRuntimeMismatch,
    #[error("connector Runtime is unavailable")]
    ConnectorRuntimeUnavailable,
    #[error("connector Runtime is not verified for execution")]
    ConnectorUnverified,
    #[error("connector model is unavailable from the selected Runtime")]
    ConnectorModelUnavailable,
    #[error("connector Runtime catalog is unavailable")]
    ConnectorCatalogUnavailable,
    #[error("a connector binding is required when a model is specified")]
    ConnectorBindingRequired,
    #[error("runtime adapter registration is invalid: {0}")]
    RuntimeAdapterRegistrationInvalid(String),
    #[error("runtime timeout must be between 1 and 3600000 milliseconds")]
    RuntimeTimeoutInvalid,
    #[error("memory scope does not exist")]
    MemoryScopeNotFound,
    #[error("memory agent does not exist")]
    MemoryAgentNotFound,
    #[error("summary scope does not exist")]
    SummaryScopeNotFound,
    #[error("summary metadata conflicts with an existing id")]
    SummaryConflict,
    #[error("summary content is unavailable")]
    SummaryContentUnavailable,
    #[error("artifact metadata is invalid")]
    ArtifactInvalid,
    #[error("artifact metadata conflicts with an existing id")]
    ArtifactConflict,
    #[error("artifact source file is invalid or unavailable")]
    ArtifactSourceInvalid,
    #[error("attachment metadata is invalid")]
    AttachmentInvalid,
    #[error("attachment metadata conflicts with an existing id or ordinal")]
    AttachmentConflict,
    #[error("attachment message does not exist")]
    AttachmentMessageNotFound,
    #[error("attachment artifact does not exist")]
    AttachmentArtifactNotFound,
    #[error("attachment metadata does not match its artifact")]
    AttachmentArtifactMismatch,
    #[error("retrieval scope does not exist")]
    RetrievalScopeNotFound,
    #[error("retrieval source id already exists with different data")]
    RetrievalConflict,
    #[error("retrieval selection scope is invalid")]
    RetrievalSelectionScopeInvalid,
    #[error("retrieval selection source is not available in the selected scope")]
    RetrievalSelectionSourceRejected,
    #[error("retrieval selection id already exists with different data")]
    RetrievalSelectionConflict,
    #[error("retrieval feedback selection or source is invalid")]
    RetrievalFeedbackRejected,
    #[error("retrieval feedback id already exists with different data")]
    RetrievalFeedbackConflict,
    #[error("retrieval preview rejected")]
    RetrievalPreviewRejected,
    #[error("configuration transfer project does not exist")]
    ConfigTransferProjectNotFound,
    #[error("configuration transfer payload is too large")]
    ConfigTransferTooLarge,
    #[error("invalid configuration transfer: {0}")]
    ConfigTransferInvalid(String),
    #[error("workflow project does not exist")]
    WorkflowProjectNotFound,
    #[error("workflow step agent is not in the Project roster")]
    WorkflowAgentNotInProject,
    #[error("workflow id already exists with different data")]
    WorkflowConflict,
    #[error("workflow does not exist")]
    WorkflowNotFound,
    #[error("workflow must contain at least one step")]
    WorkflowEmpty,
    #[error("workflow kind is unsupported: {0}")]
    WorkflowKindInvalid(String),
    #[error("workflow does not match the parent execution Project")]
    WorkflowProjectMismatch,
    #[error("collaboration Project does not exist")]
    CollaborationProjectNotFound,
    #[error("collaboration root Agent is not in the Project roster")]
    CollaborationAgentNotInProject,
    #[error("collaboration id already exists with different data")]
    CollaborationConflict,
    #[error("handoff collaboration run does not exist")]
    HandoffCollaborationNotFound,
    #[error("handoff source execution run does not exist")]
    HandoffExecutionNotFound,
    #[error("handoff target Agent is not in the Project roster")]
    HandoffAgentNotInProject,
    #[error("handoff id already exists with different data")]
    HandoffConflict,
    #[error("handoff does not exist")]
    HandoffNotFound,
    #[error("handoff cannot start Runtime without a non-empty structured task")]
    HandoffTaskMissing,
    #[error("handoff requires a structured proposal with source message and edge metadata")]
    HandoffStructuredDetailsMissing,
    #[error("handoff source message is missing or outside the source Conversation")]
    HandoffSourceMessageMissing,
    #[error("handoff would create an Agent cycle")]
    HandoffCycleDetected,
    #[error("handoff depth limit reached")]
    HandoffDepthLimit,
    #[error("invalid handoff status transition")]
    HandoffInvalidTransition,
    #[error(transparent)]
    State(#[from] StateTransitionError),
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
    #[error(transparent)]
    Context(#[from] agenttalk_context::ContextError),
    #[error(transparent)]
    Event(#[from] EventStoreError),
    #[error(transparent)]
    Storage(#[from] StorageError),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreMemoryCommand {
    pub memory: MemoryItem,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreSummaryCommand {
    pub summary: Summary,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GenerateSummaryCommand {
    pub scope_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImportLocalAgentCommand {
    pub request: LocalAgentImportRequest,
}

/// Core-owned input for the ADR-001 brief seal → journal Run boundary.
/// `project_root` is an explicit filesystem authority selected by the host;
/// the sealer never reads a mutable authoring path after this call returns.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateOrchestrationRunFromBriefCommand {
    pub project_id: String,
    pub run_id: String,
    pub project_root: PathBuf,
    pub dag_snapshot_digest: String,
    pub role_binding_snapshot_digest: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatedOrchestrationRun {
    pub run: OrchestrationRunRecord,
    pub seal: PreparedBriefSeal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreArtifactCommand {
    pub artifact: Artifact,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreArtifactBodyCommand {
    pub artifact_id: String,
    pub body: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreAttachmentCommand {
    pub attachment: Attachment,
    pub ordinal: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImportAttachmentFileCommand {
    pub attachment_id: String,
    pub artifact_id: String,
    pub message_id: String,
    pub source_path: PathBuf,
    pub mime: String,
    pub ordinal: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreRetrievalSourceCommand {
    pub source: RetrievalSource,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreRetrievalSelectionCommand {
    pub selection: RetrievalSelection,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreRetrievalFeedbackCommand {
    pub feedback: RetrievalFeedback,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateWorkflowCommand {
    pub project_id: String,
    pub workflow: WorkflowTemplate,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateCollaborationCommand {
    pub project_id: String,
    pub collaboration: CollaborationRun,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateHandoffCommand {
    pub handoff: Handoff,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigImportResult {
    pub new_project_id: String,
    pub imported_agents: u32,
    pub imported_conversations: u32,
    pub imported_workflows: u32,
    pub workspace_rebind_required: bool,
}

pub const CONFIG_TRANSFER_SCHEMA_VERSION: &str = "config.transfer.v1";
const MAX_CONFIG_TRANSFER_BYTES: usize = 512 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemoryWriteOutcome {
    Created,
    AlreadyPresent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SummaryWriteOutcome {
    Created,
    AlreadyPresent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SummaryGenerationOutcome {
    pub summary: Summary,
    pub generator: String,
    pub message_count: u64,
}

pub const SUMMARY_GENERATOR_VERSION: &str = "local-deterministic-v1";
const SUMMARY_CONTENT_MAX_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArtifactWriteOutcome {
    Created,
    AlreadyPresent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttachmentWriteOutcome {
    Created,
    AlreadyPresent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttachmentFileImportOutcome {
    pub artifact: Artifact,
    pub attachment: Attachment,
    pub artifact_outcome: ArtifactWriteOutcome,
    pub attachment_outcome: AttachmentWriteOutcome,
    pub body_stored: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetrievalWriteOutcome {
    Created,
    AlreadyPresent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetrievalSelectionWriteOutcome {
    Created,
    AlreadyPresent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetrievalFeedbackWriteOutcome {
    Created,
    AlreadyPresent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CollaborationWriteOutcome {
    Created,
    AlreadyPresent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HandoffWriteOutcome {
    Created,
    AlreadyPresent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HandoffTransitionOutcome {
    Changed,
    AlreadyAtTarget,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HandoffDispatchResult {
    pub child_run: ExecutionRun,
    pub created: bool,
    pub event_sequence: u64,
    pub handoff_status: String,
    pub runtime_started: bool,
    pub runtime_dispatch: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LegacyMentionCandidate {
    pub id: String,
    pub name: String,
}

/// Resolves a legacy `@name` mention only when it maps to exactly one known
/// roster candidate. It deliberately returns no task text: legacy output is
/// not an authority for persisted prompts or secrets.
pub fn parse_legacy_handoff_mention(
    output: &str,
    candidates: &[LegacyMentionCandidate],
) -> Option<String> {
    let mut matches = parse_legacy_handoff_mentions(output, candidates)?;
    (matches.len() == 1).then(|| matches.remove(0))
}

/// Resolves every exact `@name` mention in a legacy output. Each mention must
/// map to exactly one roster candidate and no candidate may be repeated. The
/// returned order is the mention order so callers can persist a deterministic
/// parallel batch sequence.
pub fn parse_legacy_handoff_mentions(
    output: &str,
    candidates: &[LegacyMentionCandidate],
) -> Option<Vec<String>> {
    let mentioned: Vec<&str> = output
        .split_whitespace()
        .filter_map(|token| token.strip_prefix('@'))
        .filter(|token| {
            !token.is_empty()
                && token.chars().all(|character| {
                    character.is_ascii_alphanumeric() || "._:-".contains(character)
                })
        })
        .collect();
    if mentioned.is_empty() {
        return None;
    }
    let mut resolved = Vec::with_capacity(mentioned.len());
    for mention in mentioned {
        let mut matches = candidates
            .iter()
            .filter(|candidate| mention == candidate.id || mention == candidate.name)
            .map(|candidate| candidate.id.clone())
            .collect::<Vec<_>>();
        matches.sort();
        matches.dedup();
        if matches.len() != 1 {
            return None;
        }
        resolved.push(matches.remove(0));
    }
    let mut unique = resolved.clone();
    unique.sort();
    unique.dedup();
    (unique.len() == resolved.len()).then_some(resolved)
}

/// Parses the explicit structured proposal carried by a Runtime output or
/// completion event. This function does not consult storage and therefore
/// cannot establish roster or policy authority; callers must do that before
/// creating the Handoff.
pub fn parse_runtime_handoff_proposal(event: &RuntimeEvent) -> Result<Option<Handoff>, CoreError> {
    let mut proposals = parse_runtime_handoff_proposals(event)?;
    match proposals.as_mut() {
        None => Ok(None),
        Some(proposals) if proposals.len() == 1 => Ok(proposals.pop()),
        Some(_) => Err(CoreError::HandoffStructuredDetailsMissing),
    }
}

/// Parses one or more structured proposals from a Runtime completion event.
/// A multi-proposal event is a parallel batch: Core assigns a deterministic
/// batch id and sequence indices after parsing, while roster/scope/cycle
/// authority remains in the caller.
pub fn parse_runtime_handoff_proposals(
    event: &RuntimeEvent,
) -> Result<Option<Vec<Handoff>>, CoreError> {
    if !matches!(
        event.event_type.as_str(),
        "execution.completed" | "output.delta" | "output.completed"
    ) {
        return Ok(None);
    }
    if event.execution_run_id.trim().is_empty() {
        return Err(CoreError::HandoffStructuredDetailsMissing);
    }

    let proposal_values = runtime_handoff_proposal_values(&event.payload);
    if proposal_values.is_empty() {
        return Ok(None);
    }
    let mut proposals = proposal_values
        .into_iter()
        .map(|proposal| parse_runtime_handoff_proposal_value(event, &proposal))
        .collect::<Result<Vec<_>, _>>()?;
    if proposals.len() > 1 {
        let dispatch_mode_is_parallel = proposals.iter().all(|handoff| {
            handoff
                .details
                .as_ref()
                .and_then(|details| details.dispatch_mode.as_deref())
                == Some("parallel")
        });
        let unique_targets = proposals
            .iter()
            .map(|handoff| handoff.to_agent_id.as_str())
            .collect::<HashSet<_>>();
        let unique_ids = proposals
            .iter()
            .map(|handoff| handoff.id.as_str())
            .collect::<HashSet<_>>();
        let batch_scope = proposals.first().and_then(|handoff| {
            handoff.details.as_ref().map(|details| {
                (
                    handoff.collaboration_run_id.as_str(),
                    details.source_message_id.as_deref(),
                    details.from_agent_id.as_deref(),
                )
            })
        });
        let same_scope = proposals.iter().all(|handoff| {
            handoff.details.as_ref().map(|details| {
                (
                    handoff.collaboration_run_id.as_str(),
                    details.source_message_id.as_deref(),
                    details.from_agent_id.as_deref(),
                )
            }) == batch_scope
        });
        if !dispatch_mode_is_parallel
            || unique_targets.len() != proposals.len()
            || unique_ids.len() != proposals.len()
            || !same_scope
        {
            return Err(CoreError::HandoffStructuredDetailsMissing);
        }
        let batch_id = format!(
            "runtime-handoff-batch-{}",
            sha256_hex(
                &proposals
                    .iter()
                    .map(|handoff| handoff.id.as_str())
                    .collect::<Vec<_>>()
                    .join(":"),
            )
        );
        for (sequence_index, handoff) in proposals.iter_mut().enumerate() {
            let details = handoff
                .details
                .as_mut()
                .ok_or(CoreError::HandoffStructuredDetailsMissing)?;
            details.batch_id = Some(batch_id.clone());
            details.sequence_index = Some(sequence_index as u64);
        }
    }
    Ok(Some(proposals))
}

const MAX_STRUCTURED_HANDOFF_TEXT_BYTES: usize = 64 * 1024;

fn runtime_handoff_proposal_values(payload: &serde_json::Value) -> Vec<serde_json::Value> {
    let mut values = Vec::new();
    let mut containers = vec![payload];
    if let Some(output) = payload.get("output") {
        containers.push(output);
    }

    for container in containers {
        collect_runtime_handoff_proposals(container, &mut values);
        for key in ["output", "content", "text", "delta"] {
            let Some(text) = container.get(key).and_then(serde_json::Value::as_str) else {
                continue;
            };
            collect_embedded_runtime_handoff_proposals(text, &mut values);
        }
    }
    values
}

fn collect_runtime_handoff_proposals(
    value: &serde_json::Value,
    values: &mut Vec<serde_json::Value>,
) {
    match value {
        serde_json::Value::Array(items) => {
            for item in items {
                collect_runtime_handoff_proposals(item, values);
            }
        }
        serde_json::Value::Object(object) => {
            if object.contains_key("handoffId")
                && object.contains_key("details")
                && !values.iter().any(|existing| existing == value)
            {
                values.push(value.clone());
            }
            for key in ["handoffProposals", "handoffProposal"] {
                if let Some(proposals) = object.get(key) {
                    collect_runtime_handoff_proposals(proposals, values);
                }
            }
            // Runtime providers commonly wrap text in nested content/delta
            // blocks. Traverse only the protocol-owned output keys so that
            // arbitrary metadata is never interpreted as a handoff proposal.
            for key in [
                "output",
                "content",
                "text",
                "delta",
                "choices",
                "message",
                "item",
                "items",
                "parts",
                "response",
                "candidates",
            ] {
                let Some(value) = object.get(key) else {
                    continue;
                };
                if let Some(text) = value.as_str() {
                    collect_embedded_runtime_handoff_proposals(text, values);
                } else {
                    collect_runtime_handoff_proposals(value, values);
                }
            }
        }
        _ => {}
    }
}

fn collect_embedded_runtime_handoff_proposals(text: &str, values: &mut Vec<serde_json::Value>) {
    if text.len() > MAX_STRUCTURED_HANDOFF_TEXT_BYTES {
        return;
    }
    let trimmed = text.trim();
    let json_text = if let Some(fenced) = trimmed.strip_prefix("```") {
        let Some(newline) = fenced.find('\n') else {
            return;
        };
        let Some(body) = fenced[newline + 1..].strip_suffix("```") else {
            return;
        };
        body.trim()
    } else {
        trimmed
    };
    let Ok(parsed) = serde_json::from_str::<serde_json::Value>(json_text) else {
        return;
    };
    if let Some(nested_json) = parsed.as_str() {
        if nested_json.len() <= MAX_STRUCTURED_HANDOFF_TEXT_BYTES {
            if let Ok(nested) = serde_json::from_str::<serde_json::Value>(nested_json) {
                collect_runtime_handoff_proposals(&nested, values);
            }
        }
    } else {
        collect_runtime_handoff_proposals(&parsed, values);
    }
}

fn parse_runtime_handoff_proposal_value(
    event: &RuntimeEvent,
    proposal_value: &serde_json::Value,
) -> Result<Handoff, CoreError> {
    let proposal = proposal_value
        .as_object()
        .ok_or(CoreError::HandoffStructuredDetailsMissing)?;
    let details_value = proposal
        .get("details")
        .ok_or(CoreError::HandoffStructuredDetailsMissing)?;
    let mut details: StructuredHandoffDetails = serde_json::from_value(details_value.clone())
        .map_err(|_| CoreError::HandoffStructuredDetailsMissing)?;

    let text = |key: &str| {
        proposal
            .get(key)
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .map(str::to_owned)
            .ok_or(CoreError::HandoffStructuredDetailsMissing)
    };
    let handoff_id = text("handoffId")?;
    let collaboration_run_id = text("collaborationRunId")?;
    let to_agent_id = text("toAgentId")?;
    if proposal
        .get("status")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|status| status != "proposed")
    {
        return Err(CoreError::HandoffStructuredDetailsMissing);
    }
    if proposal
        .get("fromExecutionRunId")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|run_id| run_id != event.execution_run_id)
    {
        return Err(CoreError::HandoffStructuredDetailsMissing);
    }
    if details.child_execution_run_id.is_some()
        || details.parent_execution_run_id.as_deref() != Some(event.execution_run_id.as_str())
        || details.to_agent_id.as_deref() != Some(to_agent_id.as_str())
        || details
            .context_scope
            .as_deref()
            .is_some_and(|scope| scope != "conversation")
    {
        return Err(CoreError::HandoffStructuredDetailsMissing);
    }

    // The path is Core-derived and must never be imported from Runtime output.
    details.agent_path = None;
    let handoff = Handoff {
        id: handoff_id,
        collaboration_run_id,
        from_execution_run_id: event.execution_run_id.clone(),
        to_agent_id,
        status: "proposed".into(),
        details: Some(details),
    };
    validate_handoff_shape(&handoff)?;
    Ok(handoff)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkflowWriteOutcome {
    Created,
    AlreadyPresent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowDispatchCommand {
    pub workflow_id: String,
    pub collaboration_run_id: String,
    pub parent_execution_run_id: String,
    pub source_message_id: String,
    pub task: String,
    pub start_runtime: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowDispatchStep {
    pub step_id: String,
    pub order: u32,
    pub agent_id: String,
    pub handoff_id: String,
    pub child_execution_run_id: Option<String>,
    pub handoff_status: String,
    pub child_status: Option<String>,
    pub runtime_started: bool,
    pub runtime_dispatch: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowDispatchResult {
    pub workflow_id: String,
    pub collaboration_run_id: String,
    pub mode: String,
    pub steps: Vec<WorkflowDispatchStep>,
    pub completed: bool,
    pub failed: bool,
}

pub struct PersistentCore {
    state: CoreState,
    storage: SqliteStore,
    persisted_event_cursor: u64,
    event_stream_epoch: String,
    runtimes: RuntimeRegistry,
    runtime_timeout_ms: u64,
    execution_timeouts: HashMap<String, u64>,
    contexts: HashMap<String, AssembledContext>,
    execution_bindings: HashMap<String, ExecutionRuntimeBinding>,
    model_snapshots: HashMap<String, ModelSnapshot>,
    model_selection_snapshots: HashMap<String, ModelSelectionSnapshot>,
}

#[derive(Clone, Debug)]
struct ExecutionRuntimeBinding {
    connector_id: String,
    runtime_type: Option<String>,
    model_id: Option<String>,
    catalog_revision: Option<u64>,
    validate_profile: bool,
}

#[derive(Clone, Debug)]
struct RestartFrozenRoute {
    connector_id: Option<String>,
    runtime_type: Option<String>,
    model_id: Option<String>,
    catalog_revision: Value,
}

#[derive(Clone, Debug)]
struct ResolvedModelSelection {
    snapshot: ModelSelectionSnapshot,
    binding: ExecutionRuntimeBinding,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ExecutionRuntimeOptions {
    pub connector_id: Option<String>,
    pub model_id: Option<String>,
    pub timeout_ms: Option<u64>,
}

/// An owned Runtime stream prepared while Core state is locked. Hosts may
/// drive it outside the Core mutex and re-enter only to persist each event,
/// allowing independent connector runs to interleave safely.
pub struct RuntimeDispatch {
    pub run_id: String,
    pub stream: RuntimeEventStream,
    pub timeout: Duration,
}

/// Core-owned registry for the adapters available to this Core process.
///
/// The first adapter is the legacy active Runtime used by `runtime.models`.
/// Connector-bound work must resolve through the connector profile's
/// `runtimeType`; it must never fall back to this active Runtime.
pub struct RuntimeRegistry {
    default_runtime_id: String,
    adapters: HashMap<String, Box<dyn RuntimeAdapter>>,
}

impl RuntimeRegistry {
    pub fn from_adapters(adapters: Vec<Box<dyn RuntimeAdapter>>) -> Result<Self, CoreError> {
        let mut adapters = adapters.into_iter();
        let default = adapters.next().ok_or_else(|| {
            CoreError::RuntimeAdapterRegistrationInvalid(
                "at least one RuntimeAdapter is required".into(),
            )
        })?;
        let default_runtime_id = default.id().trim().to_owned();
        if default_runtime_id.is_empty() {
            return Err(CoreError::RuntimeAdapterRegistrationInvalid(
                "RuntimeAdapter id must not be empty".into(),
            ));
        }

        let mut registered = HashMap::new();
        registered.insert(default_runtime_id.clone(), default);
        for adapter in adapters {
            let id = adapter.id().trim().to_owned();
            if id.is_empty() || registered.contains_key(&id) {
                return Err(CoreError::RuntimeAdapterRegistrationInvalid(id));
            }
            registered.insert(id, adapter);
        }
        Ok(Self {
            default_runtime_id,
            adapters: registered,
        })
    }

    fn default_runtime(&self) -> &dyn RuntimeAdapter {
        self.adapters
            .get(&self.default_runtime_id)
            .map(Box::as_ref)
            .expect("RuntimeRegistry always retains its default adapter")
    }

    /// The legacy active Runtime identifier used by unscoped `runtime.models`.
    /// Connector-bound requests must resolve a persisted profile instead.
    pub fn default_runtime_id(&self) -> &str {
        &self.default_runtime_id
    }

    /// Returns whether this Core process registered an adapter for the exact
    /// persisted connector `runtimeType`. This does not discover, connect to,
    /// or otherwise activate that adapter.
    pub fn has_runtime_type(&self, runtime_type: &str) -> bool {
        self.adapters.contains_key(runtime_type)
    }

    fn has_multiple_adapters(&self) -> bool {
        self.adapters.len() > 1
    }

    fn adapter(&self, runtime_type: &str) -> Option<&dyn RuntimeAdapter> {
        self.adapters.get(runtime_type).map(Box::as_ref)
    }

    fn shutdown_owned(&self) -> Result<(), CoreError> {
        let mut failures = 0usize;
        for adapter in self.adapters.values() {
            // Every adapter receives its own shutdown opportunity. External
            // adapters must make this a no-op; owned adapters may release only
            // resources they created. Never surface raw transport diagnostics
            // here because shutdown errors are reported over IPC.
            if adapter.shutdown_owned().is_err() {
                failures += 1;
            }
        }
        if failures == 0 {
            Ok(())
        } else {
            Err(CoreError::Runtime(RuntimeError::Transport(format!(
                "owned Runtime shutdown failed for {failures} adapter(s)"
            ))))
        }
    }
}

fn recover_orchestration_on_startup(storage: &mut SqliteStore) -> Result<(), CoreError> {
    for (run_id, status) in storage.orchestration_run_ids()? {
        if matches!(status.as_str(), "completed" | "failed" | "cancelled") {
            continue;
        }
        // The generation bump is the first durable recovery write. It fences
        // every active lease before any attempt is interpreted or retried.
        storage.bump_coordinator_generation(&run_id)?;
        for (node_id, node_status, _) in storage.orchestration_recovery_state(&run_id)? {
            if matches!(node_status.as_str(), "running" | "sealing") {
                storage.recover_active_attempt_interrupted(&node_id)?;
            }
        }
    }
    Ok(())
}

pub const RUNTIME_MODELS_SCHEMA_VERSION: &str = "runtime.models.v1";
pub const CONNECTOR_MODELS_SCHEMA_VERSION: &str = "connector.models.v1";
pub const RUNTIME_HEALTH_SCHEMA_VERSION: &str = "runtime.health.v1";
pub const CONNECTOR_HEALTH_SCHEMA_VERSION: &str = "connector.health.v1";

#[cfg(test)]
fn default_runtime() -> Box<dyn RuntimeAdapter> {
    // Unit tests must opt into a deterministic local runtime; this branch is
    // never compiled into the production Core binary.
    Box::new(agenttalk_runtime_host::MockRuntime::default())
}

#[cfg(not(test))]
fn default_runtime() -> Box<dyn RuntimeAdapter> {
    Box::new(agenttalk_runtime_host::UnconfiguredRuntime)
}

impl PersistentCore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, CoreError> {
        Self::open_with_runtime(path, default_runtime())
    }

    /// Seal the mutable brief first, then create the immutable journal Run
    /// binding in the final step of ADR-001 §2.  CAS publication belongs to
    /// the sealer; the SQLite writer stores only the sealed snapshot refs and
    /// digests.  If journal creation fails, the published objects remain
    /// orphan candidates and no partial Run is returned.
    pub fn create_orchestration_run_from_brief(
        &mut self,
        command: CreateOrchestrationRunFromBriefCommand,
        schema_registry: &dyn SchemaRegistry,
    ) -> Result<CreatedOrchestrationRun, CoreError> {
        let seal = BriefSealer::new(command.project_root).seal(schema_registry)?;
        self.storage
            .create_orchestration_run_from_prepared_brief_seal(
                &command.project_id,
                &command.run_id,
                &seal,
                &command.dag_snapshot_digest,
                &command.role_binding_snapshot_digest,
            )?;
        let run = self.storage.orchestration_run(&command.run_id)?;
        Ok(CreatedOrchestrationRun { run, seal })
    }

    /// Read-only orchestration projection for IPC/UI consumers.  Core keeps
    /// the journal and CAS authoritative; callers receive metadata and
    /// digest/object references, never sealed bytes or SQLite access.
    pub fn orchestration_projection(&self, run_id: &str) -> Result<Value, CoreError> {
        Ok(self.storage.orchestration_projection(run_id)?)
    }

    pub fn orchestration_recovery_state(&self, run_id: &str) -> Result<Value, CoreError> {
        let run = self.storage.orchestration_run(run_id)?;
        let nodes = self
            .storage
            .orchestration_recovery_state(run_id)?
            .into_iter()
            .map(|(node_id, status, attempt_count)| {
                json!({
                    "nodeId": node_id,
                    "status": status,
                    "attemptCount": attempt_count,
                })
            })
            .collect::<Vec<_>>();
        Ok(json!({
            "runId": run.run_id,
            "coordinatorGeneration": run.coordinator_generation,
            "nodes": nodes,
        }))
    }

    pub fn open_with_runtime(
        path: impl AsRef<Path>,
        runtime: Box<dyn RuntimeAdapter>,
    ) -> Result<Self, CoreError> {
        Self::open_with_runtime_registry(path, RuntimeRegistry::from_adapters(vec![runtime])?)
    }

    pub fn open_with_runtime_registry(
        path: impl AsRef<Path>,
        runtimes: RuntimeRegistry,
    ) -> Result<Self, CoreError> {
        Self::open_with_runtime_registry_and_artifact_root(path, runtimes, None)
    }

    pub fn open_with_artifact_root(
        path: impl AsRef<Path>,
        artifact_root: impl AsRef<Path>,
    ) -> Result<Self, CoreError> {
        Self::open_with_runtime_and_artifact_root(
            path,
            default_runtime(),
            Some(artifact_root.as_ref()),
        )
    }

    pub fn open_with_runtime_and_artifact_root(
        path: impl AsRef<Path>,
        runtime: Box<dyn RuntimeAdapter>,
        artifact_root: Option<&Path>,
    ) -> Result<Self, CoreError> {
        Self::open_with_runtime_registry_and_artifact_root(
            path,
            RuntimeRegistry::from_adapters(vec![runtime])?,
            artifact_root,
        )
    }

    pub fn open_with_runtime_registry_and_artifact_root(
        path: impl AsRef<Path>,
        runtimes: RuntimeRegistry,
        artifact_root: Option<&Path>,
    ) -> Result<Self, CoreError> {
        Self::open_with_runtime_configuration(
            path,
            runtimes,
            artifact_root,
            agenttalk_runtime_host::DEFAULT_RUNTIME_TIMEOUT_MS,
        )
    }

    fn open_with_runtime_configuration(
        path: impl AsRef<Path>,
        runtimes: RuntimeRegistry,
        artifact_root: Option<&Path>,
        runtime_timeout_ms: u64,
    ) -> Result<Self, CoreError> {
        let mut storage = SqliteStore::open_with_artifact_root(path, artifact_root)?;
        recover_orchestration_on_startup(&mut storage)?;
        let event_stream_epoch = storage.event_stream_epoch()?;
        let persisted_assignments = storage.load_project_agent_assignments()?;
        let persisted_conversation_assignments = storage.load_conversation_agent_assignments()?;
        let persisted_workspaces = storage.load_workspace_authorizations()?;
        let persisted_model_snapshots = storage.load_model_snapshots()?;
        let persisted_model_selection_snapshots = storage.load_model_selection_snapshots()?;
        let restart_routes = restart_frozen_routes(
            &persisted_model_snapshots,
            &persisted_model_selection_snapshots,
        );
        let mut recovered_runs = storage.load_execution_runs()?;
        for run in &mut recovered_runs {
            if run.status.is_terminal() {
                continue;
            }
            let expected_version = run.version;
            run.transition(
                ExecutionStatus::Interrupted,
                expected_version,
                Some("core_restarted_before_completion".into()),
            )?;
            let event = RuntimeEvent {
                event_id: format!("core-restart-interrupted-{}", run.id),
                execution_run_id: run.id.clone(),
                runtime_id: "core".into(),
                thread_id: None,
                turn_id: None,
                sequence: 0,
                event_type: "execution.interrupted".into(),
                timestamp_ms: 0,
                payload: restart_routes.get(&run.id).map_or_else(
                    || json!({"reason": "core_restarted_before_completion"}),
                    |route| {
                        json!({
                            "reason": "core_restarted_before_completion",
                            "connectorId": route.connector_id,
                            "runtimeType": route.runtime_type,
                            "modelId": route.model_id,
                            "catalogRevision": route.catalog_revision,
                        })
                    },
                ),
            };
            storage.persist_execution_run_and_events(run, &[event])?;
        }
        let recovered_events = storage.replay_after(0)?;
        let persisted_event_cursor = recovered_events
            .last()
            .map(|event| event.sequence)
            .unwrap_or(0);
        let mut state = CoreState::default();
        let default_runtime_id = runtimes.default_runtime_id().to_owned();
        let mut execution_bindings = HashMap::new();
        let mut model_snapshots = HashMap::new();
        let mut model_selection_snapshots = HashMap::new();
        for (project_id, agent_id, access, enabled) in persisted_assignments {
            let access = if enabled {
                parse_workspace_access(&access)?
            } else {
                WorkspaceAccess::None
            };
            state.restore_project_roster_entry(project_id, agent_id, enabled, access);
        }
        for (conversation_id, agent_id, enabled) in persisted_conversation_assignments {
            state.restore_conversation_assignment(conversation_id, agent_id, enabled);
        }
        for authorization in persisted_workspaces {
            state.restore_workspace_authorization(authorization);
        }
        for snapshot in persisted_model_snapshots {
            if let Some(connector_id) = snapshot.connector_id.clone().or_else(|| {
                snapshot
                    .model_id
                    .as_ref()
                    .map(|_| default_runtime_id.clone())
            }) {
                let validate_profile = connector_id != default_runtime_id
                    || !storage
                        .query_connector_profiles("desktop", Some(&connector_id), 1)?
                        .is_empty();
                let runtime_type = storage
                    .query_connector_profiles("desktop", Some(&connector_id), 1)?
                    .into_iter()
                    .next()
                    .map(|profile| profile.runtime_type);
                execution_bindings.insert(
                    snapshot.run_id.clone(),
                    ExecutionRuntimeBinding {
                        connector_id,
                        runtime_type,
                        model_id: snapshot.model_id.clone(),
                        catalog_revision: snapshot.revision,
                        validate_profile,
                    },
                );
            }
            model_snapshots.insert(snapshot.run_id.clone(), snapshot);
        }
        for snapshot in persisted_model_selection_snapshots {
            model_selection_snapshots.insert(snapshot.run_id.clone(), snapshot);
        }
        for run in recovered_runs {
            state.restore_run(run);
        }
        for event in recovered_events {
            state.restore_event(event)?;
        }
        Ok(Self {
            state,
            storage,
            persisted_event_cursor,
            event_stream_epoch,
            runtimes,
            runtime_timeout_ms: runtime_timeout_ms.max(1),
            execution_timeouts: HashMap::new(),
            contexts: HashMap::new(),
            execution_bindings,
            model_snapshots,
            model_selection_snapshots,
        })
    }

    fn default_runtime(&self) -> &dyn RuntimeAdapter {
        self.runtimes.default_runtime()
    }

    fn default_runtime_id(&self) -> &str {
        self.runtimes.default_runtime_id()
    }

    fn connector_profile(&self, connector_id: &str) -> Result<ConnectorProfile, CoreError> {
        self.storage
            .query_connector_profiles("desktop", Some(connector_id), 1)?
            .into_iter()
            .next()
            .ok_or(CoreError::ConnectorNotFound)
    }

    fn runtime_for_profile(
        &self,
        profile: &ConnectorProfile,
    ) -> Result<&dyn RuntimeAdapter, CoreError> {
        if !profile.enabled {
            return Err(CoreError::ConnectorDisabled);
        }
        let runtime = self
            .runtimes
            .adapter(&profile.runtime_type)
            .ok_or(CoreError::ConnectorRuntimeUnavailable)?;
        if runtime.id() != profile.runtime_type {
            return Err(CoreError::ConnectorRuntimeMismatch);
        }
        // Connector-bound work must retain a transport's known safe failure
        // class (for example authentication or identity mismatch), not
        // collapse it into the old health projection before IPC can classify
        // it. Legacy adapters with an unclassified unavailable status retain
        // the historical generic Connector-unverified outcome.
        if let Err(error) = runtime.ensure_available() {
            return if connector_runtime_failure(&error).is_some() {
                Err(error.into())
            } else {
                Err(CoreError::ConnectorUnverified)
            };
        }
        Ok(runtime)
    }

    fn runtime_for_binding(
        &self,
        binding: &ExecutionRuntimeBinding,
    ) -> Result<&dyn RuntimeAdapter, CoreError> {
        if binding.validate_profile {
            let profile = self.connector_profile(&binding.connector_id)?;
            if binding
                .runtime_type
                .as_deref()
                .is_some_and(|runtime_type| runtime_type != profile.runtime_type)
            {
                return Err(CoreError::ConnectorRuntimeMismatch);
            }
            let runtime = self.runtime_for_profile(&profile)?;
            if let Some(model_id) = binding.model_id.as_deref() {
                if !runtime.list_models_checked()?.iter().any(|candidate| {
                    candidate == model_id && runtime_catalog_model_is_selectable(runtime, candidate)
                }) {
                    return Err(CoreError::ConnectorModelUnavailable);
                }
            }
            return Ok(runtime);
        }

        let runtime = self.default_runtime();
        if binding.connector_id != runtime.id()
            || binding.runtime_type.as_deref().is_some_and(|runtime_type| {
                runtime_type != runtime.id() && runtime_type != runtime.discover().runtime_id
            })
        {
            return Err(CoreError::ConnectorRuntimeMismatch);
        }
        // Legacy default-runtime executions intentionally defer health and
        // transport failures until dispatch. That preserves the durable
        // execution.failed record for an unavailable default adapter instead
        // of rejecting the command before a Run exists. Connector profiles
        // still use `runtime_for_profile` above and remain fail-closed at
        // resolution time.
        // The legacy/default adapter may execute a persisted manual or
        // unverified identity option that was created before catalog routing
        // existed. Connector-profile bindings above always enforce catalog
        // membership; this compatibility branch must not turn those old
        // defaults into an orphaned execution before dispatch.
        Ok(runtime)
    }

    fn runtime_for_run(&self, run_id: &str) -> Result<&dyn RuntimeAdapter, CoreError> {
        match self.execution_bindings.get(run_id) {
            Some(binding) => self.runtime_for_binding(binding),
            None => Ok(self.default_runtime()),
        }
    }

    fn requested_runtime_binding(
        &self,
        connector_id: Option<String>,
        model_id: Option<String>,
    ) -> Result<Option<ExecutionRuntimeBinding>, CoreError> {
        match (connector_id, model_id) {
            (None, None) => Ok(None),
            (None, Some(_)) => Err(CoreError::ConnectorBindingRequired),
            (Some(connector_id), model_id) => Ok(Some(ExecutionRuntimeBinding {
                connector_id,
                runtime_type: None,
                model_id,
                catalog_revision: None,
                validate_profile: true,
            })),
        }
    }

    pub fn assign_agent(&mut self, agent_id: impl Into<String>) {
        self.state.assign_agent(agent_id);
    }

    pub fn start_execution(&mut self, input: ExecutionStart) -> Result<ExecutionRun, CoreError> {
        self.start_execution_internal(input, None, None, None, None)
    }

    pub fn retry_execution(
        &mut self,
        new_run_id: impl Into<String>,
        source_run_id: &str,
        current_task: String,
        connector_id: Option<String>,
        model_id: Option<String>,
    ) -> Result<ExecutionRun, CoreError> {
        self.retry_execution_internal(
            new_run_id.into(),
            source_run_id,
            current_task,
            None,
            ExecutionRuntimeOptions {
                connector_id,
                model_id,
                timeout_ms: None,
            },
            true,
        )
    }

    /// Re-run a terminal Run against the current Runtime/Connector settings.
    /// This is deliberately separate from ordinary Retry, which must retain
    /// the source Run's frozen ModelSnapshot.
    pub fn rerun_current_execution_with_receipt(
        &mut self,
        new_run_id: impl Into<String>,
        source_run_id: &str,
        current_task: String,
        receipt: &CommandReceipt,
        options: ExecutionRuntimeOptions,
    ) -> Result<ExecutionRun, CoreError> {
        self.rerun_current_execution_with_receipt_mode(
            new_run_id.into(),
            source_run_id,
            current_task,
            receipt,
            options,
            true,
        )
    }

    /// Deferred counterpart of [`Self::rerun_current_execution_with_receipt`]
    /// for a multi-adapter Core. It freezes a fresh current selection, but
    /// leaves Runtime I/O to the host worker after the command response has
    /// been published.
    pub fn rerun_current_execution_with_receipt_deferred(
        &mut self,
        new_run_id: impl Into<String>,
        source_run_id: &str,
        current_task: String,
        receipt: &CommandReceipt,
        options: ExecutionRuntimeOptions,
    ) -> Result<ExecutionRun, CoreError> {
        self.rerun_current_execution_with_receipt_mode(
            new_run_id.into(),
            source_run_id,
            current_task,
            receipt,
            options,
            false,
        )
    }

    fn rerun_current_execution_with_receipt_mode(
        &mut self,
        new_run_id: String,
        source_run_id: &str,
        current_task: String,
        receipt: &CommandReceipt,
        options: ExecutionRuntimeOptions,
        drive_runtime: bool,
    ) -> Result<ExecutionRun, CoreError> {
        if current_task.trim().is_empty() {
            return Err(CoreError::RetryTaskMissing);
        }
        let source = self
            .state
            .run(source_run_id)
            .cloned()
            .ok_or(CoreError::RunNotFound)?;
        if !source.status.is_terminal() {
            return Err(CoreError::RetrySourceNotTerminal);
        }
        let ExecutionRuntimeOptions {
            connector_id,
            model_id,
            timeout_ms,
        } = options;
        let binding = self.requested_runtime_binding(connector_id, model_id)?;
        self.start_execution_internal_with_selection(
            ExecutionStart {
                run_id: new_run_id,
                collaboration_run_id: source.collaboration_run_id,
                project_id: source.project_id,
                conversation_id: source.conversation_id,
                agent_id: source.agent_id,
                workspace_access: source.scope.workspace_access,
                canonical_cwd: source.scope.canonical_cwd,
            },
            Some(current_task),
            Some(receipt),
            binding,
            timeout_ms,
            None,
            drive_runtime,
        )
    }

    pub fn start_execution_with_receipt(
        &mut self,
        input: ExecutionStart,
        receipt: &CommandReceipt,
    ) -> Result<ExecutionRun, CoreError> {
        self.start_execution_internal(input, None, Some(receipt), None, None)
    }

    pub fn start_execution_with_task_and_receipt(
        &mut self,
        input: ExecutionStart,
        current_task: String,
        receipt: &CommandReceipt,
    ) -> Result<ExecutionRun, CoreError> {
        self.start_execution_internal(input, Some(current_task), Some(receipt), None, None)
    }

    pub fn start_execution_with_connector_and_receipt(
        &mut self,
        input: ExecutionStart,
        current_task: Option<String>,
        receipt: &CommandReceipt,
        connector_id: Option<String>,
        model_id: Option<String>,
        runtime_timeout_ms: Option<u64>,
    ) -> Result<ExecutionRun, CoreError> {
        self.start_execution_with_connector_and_receipt_mode(
            input,
            current_task,
            receipt,
            connector_id,
            model_id,
            runtime_timeout_ms,
            true,
        )
    }

    pub fn start_execution_with_connector_and_receipt_deferred(
        &mut self,
        input: ExecutionStart,
        current_task: Option<String>,
        receipt: &CommandReceipt,
        connector_id: Option<String>,
        model_id: Option<String>,
        runtime_timeout_ms: Option<u64>,
    ) -> Result<ExecutionRun, CoreError> {
        self.start_execution_with_connector_and_receipt_mode(
            input,
            current_task,
            receipt,
            connector_id,
            model_id,
            runtime_timeout_ms,
            false,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn start_execution_with_connector_and_receipt_mode(
        &mut self,
        input: ExecutionStart,
        current_task: Option<String>,
        receipt: &CommandReceipt,
        connector_id: Option<String>,
        model_id: Option<String>,
        runtime_timeout_ms: Option<u64>,
        drive_runtime: bool,
    ) -> Result<ExecutionRun, CoreError> {
        let binding = self.requested_runtime_binding(connector_id, model_id)?;
        self.start_execution_internal_with_selection(
            input,
            current_task,
            Some(receipt),
            binding,
            runtime_timeout_ms,
            None,
            drive_runtime,
        )
    }

    pub fn retry_execution_with_receipt(
        &mut self,
        new_run_id: impl Into<String>,
        source_run_id: &str,
        current_task: String,
        receipt: &CommandReceipt,
        options: ExecutionRuntimeOptions,
    ) -> Result<ExecutionRun, CoreError> {
        self.retry_execution_internal(
            new_run_id.into(),
            source_run_id,
            current_task,
            Some(receipt),
            options,
            true,
        )
    }

    /// Deferred counterpart of [`Self::retry_execution_with_receipt`] for a
    /// multi-adapter Core. Retry retains the source Run's frozen selection;
    /// Runtime I/O is dispatched by the host worker after the response.
    pub fn retry_execution_with_receipt_deferred(
        &mut self,
        new_run_id: impl Into<String>,
        source_run_id: &str,
        current_task: String,
        receipt: &CommandReceipt,
        options: ExecutionRuntimeOptions,
    ) -> Result<ExecutionRun, CoreError> {
        self.retry_execution_internal(
            new_run_id.into(),
            source_run_id,
            current_task,
            Some(receipt),
            options,
            false,
        )
    }

    fn retry_execution_internal(
        &mut self,
        new_run_id: String,
        source_run_id: &str,
        current_task: String,
        receipt: Option<&CommandReceipt>,
        options: ExecutionRuntimeOptions,
        drive_runtime: bool,
    ) -> Result<ExecutionRun, CoreError> {
        if current_task.trim().is_empty() {
            return Err(CoreError::RetryTaskMissing);
        }
        let source = self
            .state
            .run(source_run_id)
            .cloned()
            .ok_or(CoreError::RunNotFound)?;
        if !source.status.is_terminal() {
            return Err(CoreError::RetrySourceNotTerminal);
        }
        let ExecutionRuntimeOptions {
            connector_id,
            model_id,
            timeout_ms,
        } = options;
        let persisted_snapshot = self
            .model_snapshots
            .get(source_run_id)
            .cloned()
            .or(self.storage.load_model_snapshot(source_run_id)?);
        let snapshot = persisted_snapshot.ok_or(CoreError::ModelSnapshotMissing)?;
        if connector_id
            .as_deref()
            .is_some_and(|value| snapshot.connector_id.as_deref() != Some(value))
            || model_id
                .as_ref()
                .is_some_and(|value| snapshot.model_id.as_ref() != Some(value))
        {
            return Err(CoreError::ModelSnapshotConflict);
        }
        let mut selection_snapshot = match self
            .model_selection_snapshots
            .get(source_run_id)
            .cloned()
            .or(self.storage.load_model_selection_snapshot(source_run_id)?)
        {
            Some(snapshot) => snapshot,
            None => self.legacy_selection_snapshot(&snapshot)?,
        };
        if selection_snapshot.connector_id != snapshot.connector_id.as_deref().unwrap_or("")
            || selection_snapshot.effective_model_id != snapshot.model_id
        {
            return Err(CoreError::ModelSelectionSnapshotConflict);
        }
        selection_snapshot.run_id = new_run_id.clone();
        let binding = if let Some(memory_binding) = self.execution_bindings.get(source_run_id) {
            if snapshot.connector_id.as_deref() != Some(memory_binding.connector_id.as_str())
                || snapshot.model_id.as_ref() != memory_binding.model_id.as_ref()
                || snapshot.revision != memory_binding.catalog_revision
            {
                return Err(CoreError::ModelSnapshotConflict);
            }
            Some(memory_binding.clone())
        } else {
            let connector_id = snapshot
                .connector_id
                .clone()
                .ok_or(CoreError::ModelSnapshotMissing)?;
            Some(ExecutionRuntimeBinding {
                validate_profile: self.snapshot_requires_profile(&connector_id)?,
                connector_id,
                runtime_type: Some(selection_snapshot.runtime_type.clone()),
                model_id: snapshot.model_id.clone(),
                catalog_revision: snapshot.revision,
            })
        };
        let runtime_timeout_ms =
            timeout_ms.or_else(|| self.execution_timeouts.get(source_run_id).copied());
        self.start_execution_internal_with_selection(
            ExecutionStart {
                run_id: new_run_id,
                collaboration_run_id: source.collaboration_run_id,
                project_id: source.project_id,
                conversation_id: source.conversation_id,
                agent_id: source.agent_id,
                workspace_access: source.scope.workspace_access,
                canonical_cwd: source.scope.canonical_cwd,
            },
            Some(current_task),
            receipt,
            binding,
            runtime_timeout_ms,
            Some(selection_snapshot),
            drive_runtime,
        )
    }

    fn start_execution_internal(
        &mut self,
        input: ExecutionStart,
        current_task: Option<String>,
        receipt: Option<&CommandReceipt>,
        binding: Option<ExecutionRuntimeBinding>,
        runtime_timeout_ms: Option<u64>,
    ) -> Result<ExecutionRun, CoreError> {
        self.start_execution_internal_with_selection(
            input,
            current_task,
            receipt,
            binding,
            runtime_timeout_ms,
            None,
            true,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn start_execution_internal_with_selection(
        &mut self,
        input: ExecutionStart,
        current_task: Option<String>,
        receipt: Option<&CommandReceipt>,
        requested_binding: Option<ExecutionRuntimeBinding>,
        runtime_timeout_ms: Option<u64>,
        frozen_selection: Option<ModelSelectionSnapshot>,
        drive_runtime: bool,
    ) -> Result<ExecutionRun, CoreError> {
        let run_id = input.run_id.clone();
        if self.state.run(&run_id).is_some() || self.recover_run(&run_id)?.is_some() {
            return Err(CoreError::RunAlreadyExists);
        }
        self.state.validate_workspace_request(&input)?;
        if runtime_timeout_ms.is_some_and(|timeout| {
            timeout == 0 || timeout > agenttalk_runtime_host::MAX_RUNTIME_TIMEOUT_MS
        }) {
            return Err(CoreError::RuntimeTimeoutInvalid);
        }
        let pending_run = self.state.build_pending_execution(input)?;
        let resolved = if let Some(snapshot) = frozen_selection {
            self.resolved_frozen_model_selection(snapshot, requested_binding.as_ref())?
        } else {
            self.resolve_current_model_selection(&pending_run, requested_binding.as_ref(), None)?
        };
        if resolved.binding.validate_profile {
            self.validate_connector_binding(&resolved.binding)?;
        }
        let mut context = current_task
            .as_deref()
            .map(|task| self.assemble_context(&pending_run, task))
            .transpose()?;
        if let Some(context) = context.as_mut() {
            context.manifest.connector_id = Some(resolved.snapshot.connector_id.clone());
            context.manifest.model_id = resolved.snapshot.effective_model_id.clone();
        }
        let run = self.state.insert_pending_execution(pending_run)?.clone();
        let model_snapshot =
            self.model_snapshot_for(&run, Some(&resolved.binding), context.as_ref())?;
        if model_snapshot.connector_id.as_deref() != Some(resolved.snapshot.connector_id.as_str())
            || model_snapshot.model_id != resolved.snapshot.effective_model_id
        {
            return Err(CoreError::ModelSelectionSnapshotConflict);
        }
        self.state.emit(
            &run.id,
            "scope.frozen",
            json!({
                "projectId": run.scope.project_id,
                "conversationId": run.scope.conversation_id,
                "agentId": run.scope.agent_id,
                "workspaceAccess": run.scope.workspace_access,
                "canonicalCwd": run.scope.canonical_cwd,
                "connectorId": resolved.snapshot.connector_id,
                "runtimeType": resolved.snapshot.runtime_type,
                "modelId": resolved.snapshot.effective_model_id,
                // Route events use the same authoritative Runtime catalog
                // revision as the execution binding. Identity option metadata
                // may carry its own historical revision string, but must not
                // make scope.frozen disagree with connector/runtime events.
                "catalogRevision": resolved.binding.catalog_revision,
            }),
        )?;
        if let Some(context) = context.as_ref() {
            let manifest_id = context.manifest.id.clone();
            let source_count = context.source_ledger.len();
            let bundle_hash = sha256_hex(&context.bundle.rendered_context);
            self.state.emit(
                &run.id,
                "context.assembled",
                json!({
                    "manifestId": manifest_id.clone(),
                    "sourceCount": source_count,
                    "budget": 4096,
                    "connectorId": &model_snapshot.connector_id,
                    "modelId": &model_snapshot.model_id,
                }),
            )?;
            self.state.emit(
                &run.id,
                "context.sealed",
                json!({
                    "manifestId": manifest_id,
                    "bundleHash": bundle_hash,
                    "metadataOnly": true,
                    "connectorId": &model_snapshot.connector_id,
                    "modelId": &model_snapshot.model_id,
                }),
            )?;
        }
        self.persist_initial_run_boundary(
            receipt,
            &run,
            &model_snapshot,
            &resolved.snapshot,
            context.as_ref(),
        )?;
        if let Some(context) = context {
            self.contexts.insert(run.id.clone(), context);
        }
        self.model_snapshots
            .insert(run.id.clone(), model_snapshot.clone());
        self.model_selection_snapshots
            .insert(run.id.clone(), resolved.snapshot);
        self.execution_bindings
            .insert(run.id.clone(), resolved.binding);
        if let Some(runtime_timeout_ms) = runtime_timeout_ms {
            self.execution_timeouts
                .insert(run.id.clone(), runtime_timeout_ms);
        }
        if drive_runtime {
            self.drive_runtime(&run.id)?;
        }
        self.state
            .run(&run.id)
            .cloned()
            .ok_or(CoreError::RunNotFound)
    }

    fn validate_connector_binding(
        &self,
        binding: &ExecutionRuntimeBinding,
    ) -> Result<(), CoreError> {
        self.runtime_for_binding(binding).map(|_| ())
    }

    fn snapshot_requires_profile(&self, connector_id: &str) -> Result<bool, CoreError> {
        if connector_id != self.default_runtime_id() {
            return Ok(true);
        }
        Ok(!self
            .storage
            .query_connector_profiles("desktop", Some(connector_id), 1)?
            .is_empty())
    }

    fn resolve_current_model_selection(
        &self,
        run: &ExecutionRun,
        requested_binding: Option<&ExecutionRuntimeBinding>,
        context: Option<&AssembledContext>,
    ) -> Result<ResolvedModelSelection, CoreError> {
        let persisted_binding = self.storage.load_agent_model_binding(&run.agent_id)?;
        if let (Some(requested), Some(persisted)) = (requested_binding, persisted_binding.as_ref())
        {
            if persisted
                .connector_id
                .as_deref()
                .is_some_and(|value| value != requested.connector_id)
                || persisted
                    .model_id
                    .as_deref()
                    .is_some_and(|value| requested.model_id.as_deref() != Some(value))
            {
                return Err(CoreError::ModelSelectionSnapshotConflict);
            }
        }

        let explicit_connector_id = persisted_binding
            .as_ref()
            .and_then(|value| value.connector_id.clone())
            .or_else(|| requested_binding.map(|value| value.connector_id.clone()));
        let requested_connector_id = requested_binding.map(|value| value.connector_id.as_str());
        let base_model_id = persisted_binding
            .as_ref()
            .and_then(|value| value.model_id.clone())
            .or_else(|| requested_binding.and_then(|value| value.model_id.clone()))
            .or_else(|| context.and_then(|value| value.manifest.model_id.clone()));
        if explicit_connector_id.is_none() && base_model_id.is_some() {
            return Err(CoreError::ConnectorBindingRequired);
        }
        let connector_id = explicit_connector_id
            .clone()
            .unwrap_or_else(|| self.default_runtime_id().to_owned());
        let profile = self
            .storage
            .query_connector_profiles("desktop", Some(&connector_id), 1)?
            .into_iter()
            .next();
        // A command-level Connector selection always requires a persisted
        // profile, including when its id happens to equal the default adapter
        // id. Legacy persisted bindings that point at the default adapter may
        // still execute without a profile for IPC compatibility; non-default
        // persisted bindings remain fail-closed as well.
        let requires_profile = requested_connector_id.is_some()
            || explicit_connector_id
                .as_deref()
                .is_some_and(|value| value != self.default_runtime_id());
        if requires_profile && profile.is_none() {
            return Err(CoreError::ConnectorNotFound);
        }
        let (runtime_type, provider_type, runtime, validate_profile) =
            if let Some(profile) = profile.as_ref() {
                let runtime = self.runtime_for_profile(profile)?;
                (
                    profile.runtime_type.clone(),
                    profile.provider_type.clone(),
                    runtime,
                    true,
                )
            } else {
                let runtime = self.default_runtime();
                (
                    runtime.id().to_owned(),
                    connector_id.clone(),
                    runtime,
                    false,
                )
            };
        if requested_binding
            .and_then(|value| value.runtime_type.as_deref())
            .is_some_and(|value| value != runtime_type)
        {
            return Err(CoreError::ConnectorRuntimeMismatch);
        }
        // Fetch a Connector profile's catalog once through the typed path.
        // This preserves catalog/auth/identity failures for start, retry, and
        // rerun-current instead of letting a legacy empty vector pick a model
        // or turn the failure into a generic model mismatch.
        let validated_catalog = if validate_profile {
            Some(runtime.list_models_checked()?)
        } else {
            None
        };
        let runtime_default_model_id = validated_catalog.as_deref().and_then(|models| {
            runtime.catalog_default_model_id().filter(|model_id| {
                models.iter().any(|candidate| candidate == model_id)
                    && runtime_catalog_model_is_selectable(runtime, model_id)
            })
        });
        let runtime_default_catalog_revision = validated_catalog
            .as_deref()
            .map(|models| runtime_model_catalog_revision_for_models(runtime, models).to_string());

        let base_target = IdentityModelListTarget {
            scope: IdentityModelListScope::BaseAgent,
            agent_id: run.agent_id.clone(),
            project_id: None,
            conversation_id: None,
        };
        let project_target = IdentityModelListTarget {
            scope: IdentityModelListScope::ProjectAgent,
            agent_id: run.agent_id.clone(),
            project_id: Some(run.project_id.clone()),
            conversation_id: None,
        };
        let conversation_target = IdentityModelListTarget {
            scope: IdentityModelListScope::ConversationAgent,
            agent_id: run.agent_id.clone(),
            project_id: None,
            conversation_id: Some(run.conversation_id.clone()),
        };
        let base_options = self
            .storage
            .query_identity_model_options(&base_target, Some(&connector_id))?;
        let project_options = self
            .storage
            .query_identity_model_options(&project_target, Some(&connector_id))?;
        let conversation_options = self
            .storage
            .query_identity_model_options(&conversation_target, Some(&connector_id))?;
        let project_selection = self
            .storage
            .load_project_agent_model_selection(&run.project_id, &run.agent_id)?;
        let conversation_selection = self
            .storage
            .load_conversation_agent_model_selection(&run.conversation_id, &run.agent_id)?;
        let identity_lists_configured = persisted_binding
            .as_ref()
            .is_some_and(|value| value.model_id.is_some())
            || !base_options.is_empty()
            || !project_options.is_empty()
            || !conversation_options.is_empty()
            || project_selection
                .as_ref()
                .is_some_and(stored_selection_configured)
            || conversation_selection
                .as_ref()
                .is_some_and(stored_selection_configured);

        let snapshot = if identity_lists_configured {
            resolve_identity_model_selection(
                run.id.clone(),
                runtime_type.clone(),
                provider_type.clone(),
                connector_id.clone(),
                base_model_id,
                runtime_default_model_id,
                runtime_default_catalog_revision,
                persisted_binding
                    .as_ref()
                    .map(|value| value.candidate_model_list_revision)
                    .unwrap_or(0),
                project_selection,
                conversation_selection,
                base_options,
                project_options,
                conversation_options,
            )?
        } else {
            resolve_legacy_model_selection(
                run.id.clone(),
                runtime_type,
                provider_type,
                connector_id.clone(),
                base_model_id,
                runtime,
                validated_catalog.as_deref(),
            )?
        };
        if validate_profile {
            let models = validated_catalog
                .as_deref()
                .expect("Connector profile catalog was fetched before selection");
            let Some(model_id) = snapshot.effective_model_id.as_deref() else {
                return Err(CoreError::ConnectorModelUnavailable);
            };
            if !models.iter().any(|candidate| {
                candidate == model_id && runtime_catalog_model_is_selectable(runtime, candidate)
            }) {
                return Err(CoreError::ConnectorModelUnavailable);
            }
        }
        let binding = ExecutionRuntimeBinding {
            connector_id,
            runtime_type: Some(snapshot.runtime_type.clone()),
            model_id: snapshot.effective_model_id.clone(),
            catalog_revision: requested_binding
                .and_then(|value| value.catalog_revision)
                .or_else(|| {
                    validated_catalog
                        .as_deref()
                        .map(|models| runtime_model_catalog_revision_for_models(runtime, models))
                })
                .or_else(|| Some(runtime_model_catalog_revision(runtime))),
            validate_profile,
        };
        Ok(ResolvedModelSelection { snapshot, binding })
    }

    fn resolved_frozen_model_selection(
        &self,
        snapshot: ModelSelectionSnapshot,
        requested_binding: Option<&ExecutionRuntimeBinding>,
    ) -> Result<ResolvedModelSelection, CoreError> {
        if let Some(requested) = requested_binding {
            if requested.connector_id != snapshot.connector_id
                || requested.model_id != snapshot.effective_model_id
            {
                return Err(CoreError::ModelSelectionSnapshotConflict);
            }
        }
        let connector_id = snapshot.connector_id.clone();
        let binding = ExecutionRuntimeBinding {
            connector_id: connector_id.clone(),
            runtime_type: Some(snapshot.runtime_type.clone()),
            model_id: snapshot.effective_model_id.clone(),
            catalog_revision: requested_binding.and_then(|value| value.catalog_revision),
            validate_profile: requested_binding
                .map(|value| value.validate_profile)
                .unwrap_or(self.snapshot_requires_profile(&connector_id)?),
        };
        self.runtime_for_binding(&binding)?;
        Ok(ResolvedModelSelection { snapshot, binding })
    }

    fn legacy_selection_snapshot(
        &self,
        snapshot: &ModelSnapshot,
    ) -> Result<ModelSelectionSnapshot, CoreError> {
        let connector_id = snapshot
            .connector_id
            .clone()
            .unwrap_or_else(|| self.default_runtime_id().to_owned());
        let profile = self
            .storage
            .query_connector_profiles("desktop", Some(&connector_id), 1)?
            .into_iter()
            .next();
        if connector_id != self.default_runtime_id() && profile.is_none() {
            return Err(CoreError::ConnectorNotFound);
        }
        let (runtime, runtime_type, provider_type, validate_profile) =
            if let Some(profile) = profile.as_ref() {
                (
                    self.runtime_for_profile(profile)?,
                    profile.runtime_type.clone(),
                    profile.provider_type.clone(),
                    true,
                )
            } else {
                let runtime = self.default_runtime();
                (
                    runtime,
                    runtime.id().to_owned(),
                    connector_id.clone(),
                    false,
                )
            };
        let model_id = snapshot.model_id.clone();
        let validated_catalog = if validate_profile {
            Some(runtime.list_models_checked()?)
        } else {
            None
        };
        let availability = if let Some(models) = validated_catalog.as_deref() {
            if model_id.as_deref().is_some_and(|model| {
                models.iter().any(|candidate| {
                    candidate == model && runtime_catalog_model_is_selectable(runtime, candidate)
                })
            }) {
                ModelAvailability::Available
            } else {
                ModelAvailability::Unavailable
            }
        } else {
            runtime_model_availability(runtime, model_id.as_deref())
        };
        Ok(ModelSelectionSnapshot {
            run_id: snapshot.run_id.clone(),
            version: 1,
            runtime_type,
            provider_type,
            connector_id,
            effective_model_id: model_id.clone(),
            selection_source: if model_id.is_some() {
                ModelSelectionSource::BaseAgent
            } else {
                ModelSelectionSource::ConnectorDefault
            },
            selection_mode: if model_id.is_some() {
                ModelSelectionMode::Pinned
            } else {
                ModelSelectionMode::ConnectorDefault
            },
            availability,
            catalog_revision: Some(
                validated_catalog
                    .as_deref()
                    .map(|models| runtime_model_catalog_revision_for_models(runtime, models))
                    .unwrap_or_else(|| runtime_model_catalog_revision(runtime))
                    .to_string(),
            ),
            context_window: None,
            reasoning_efforts: Vec::new(),
            service_tiers: Vec::new(),
            candidate_model_list: None,
        })
    }

    fn model_snapshot_for(
        &self,
        run: &ExecutionRun,
        binding: Option<&ExecutionRuntimeBinding>,
        context: Option<&AssembledContext>,
    ) -> Result<ModelSnapshot, CoreError> {
        let connector_id = binding
            .map(|value| value.connector_id.clone())
            .unwrap_or_else(|| self.default_runtime_id().to_owned());
        let model_id = binding
            .and_then(|value| value.model_id.clone())
            .or_else(|| context.and_then(|value| value.manifest.model_id.clone()));
        let revision = binding
            .and_then(|value| value.catalog_revision)
            .or_else(|| Some(runtime_model_catalog_revision(self.default_runtime())));
        Ok(ModelSnapshot {
            run_id: run.id.clone(),
            connector_id: Some(connector_id),
            model_id,
            revision,
        })
    }

    fn assemble_context(
        &self,
        run: &ExecutionRun,
        current_task: &str,
    ) -> Result<AssembledContext, CoreError> {
        let history = self
            .storage
            .load_recent_message_contents(&run.conversation_id, 20)?;
        let summary = self
            .storage
            .load_recent_summary_metadata(&run.conversation_id, 1)?
            .into_iter()
            .next()
            .map(|metadata| {
                self.storage
                    .load_summary_content(&metadata.id)
                    .unwrap_or_else(|_| {
                        json!({
                            "summaryId": metadata.id,
                            "version": metadata.version,
                            "contentHash": metadata.content_hash,
                            "artifactId": metadata.artifact_id,
                            "body": "summary_body_unavailable",
                        })
                        .to_string()
                    })
            });
        let memories = self
            .storage
            .load_recent_memory_metadata(&run.project_id, &run.agent_id, 16)?
            .into_iter()
            .map(|memory| {
                json!({
                    "memoryId": memory.id,
                    "scopeId": memory.scope_id,
                    "agentId": memory.agent_id,
                    "contentHash": memory.content_hash,
                    "confirmed": memory.confirmed,
                })
                .to_string()
            })
            .collect::<Vec<_>>();
        let mut retrieval = Vec::new();
        let mut retrieval_ids = BTreeSet::new();
        for scope_id in [&run.project_id, &run.conversation_id] {
            if !self.storage.memory_scope_exists(scope_id)? {
                continue;
            }
            for source in self.storage.query_retrieval_sources(scope_id, None, 16)? {
                let Some(source_id) = source.get("id").and_then(Value::as_str) else {
                    continue;
                };
                if retrieval_ids.insert(source_id.to_owned()) {
                    retrieval.push(json!({"retrievalMetadata": source}).to_string());
                }
            }
        }
        let attachments = self
            .storage
            .load_attachment_context_records(&run.conversation_id, 16)?
            .into_iter()
            .map(|record| {
                let source_id = record.attachment_id.clone().unwrap_or_else(|| {
                    format!(
                        "legacy-attachment-{}-{}",
                        &sha256_hex(&record.message_id)[..16],
                        record.ordinal
                    )
                });
                let resolution = if record.artifact_id.is_some() {
                    "artifact_store_reference"
                } else {
                    "legacy_metadata_only"
                };
                AttachmentContextSource {
                    source_id,
                    metadata: format!(
                        "[attachment_metadata_untrusted]\n{}",
                        json!({
                            "attachmentId": record.attachment_id,
                            "artifactId": record.artifact_id,
                            "messageId": record.message_id,
                            "messageSequence": record.message_sequence,
                            "ordinal": record.ordinal,
                            "fileName": record.file_name,
                            "sha256": record.sha256,
                            "size": record.size,
                            "mime": record.mime,
                            "permission": "read_only",
                            "resolution": resolution,
                        })
                    ),
                }
            })
            .collect();
        Ok(ContextAssembler { token_budget: 4096 }.assemble_for_run(
            run.id.clone(),
            ContextInput {
                scope: run.scope.clone(),
                current_task: current_task.to_owned(),
                history,
                summary,
                memories,
                retrieval,
                attachments,
            },
        )?)
    }

    fn persist_context(
        &mut self,
        run: &ExecutionRun,
        context: AssembledContext,
    ) -> Result<(), CoreError> {
        if context.manifest.execution_run_id != run.id {
            return Err(CoreError::Context(
                agenttalk_context::ContextError::ManifestRunMismatch,
            ));
        }
        let (bundle_hash, source_ledger_json) = context_manifest_storage_values(&context);
        self.storage.store_context_manifest_with_ledger(
            &context.manifest,
            &bundle_hash,
            &source_ledger_json,
        )?;
        self.contexts.insert(run.id.clone(), context);
        Ok(())
    }

    fn runtime_request_for(&self, run: &ExecutionRun) -> RuntimeRequest {
        let context = self.contexts.get(&run.id);
        let context_manifest_id = context
            .map(|value| value.manifest.id.clone())
            .unwrap_or_else(|| format!("manifest-{}", run.id));
        let binding = self.execution_bindings.get(&run.id);
        let model_id = binding
            .and_then(|value| value.model_id.clone())
            .or_else(|| context.and_then(|value| value.manifest.model_id.clone()));
        RuntimeRequest {
            execution_run_id: run.id.clone(),
            agent_identity_id: run.agent_id.clone(),
            connector_id: binding
                .map(|value| value.connector_id.clone())
                .unwrap_or_else(|| self.default_runtime_id().to_owned()),
            model_id,
            context_manifest_id: context_manifest_id.clone(),
            rendered_context: context
                .map(|value| value.bundle.rendered_context.clone())
                .unwrap_or_default(),
            canonical_cwd: run.scope.canonical_cwd.clone(),
            workspace_access: run.scope.workspace_access.clone(),
            timeout_ms: self
                .execution_timeouts
                .get(&run.id)
                .copied()
                .unwrap_or(self.runtime_timeout_ms),
            thread_policy: "per-run".into(),
            signed_scope: runtime_scope_digest(run, &context_manifest_id, binding),
        }
    }

    pub fn cancel_execution(&mut self, run_id: &str) -> Result<ExecutionRun, CoreError> {
        let run = self
            .state
            .run(run_id)
            .cloned()
            .ok_or(CoreError::RunNotFound)?;
        if run.status == ExecutionStatus::Cancelled {
            // Explicit cancellation is idempotent. A repeated client command
            // must not call the provider again or append a second terminal
            // event.
            return Ok(run);
        }
        if run.status.is_terminal() {
            return Err(CoreError::State(StateTransitionError::TerminalImmutable {
                from: run.status,
                to: ExecutionStatus::Cancelled,
            }));
        }
        let request = self.runtime_request_for(&run);
        let event = self.runtime_for_run(run_id)?.cancel(&request)?;
        if event.execution_run_id != run_id || event.event_type != "execution.cancelled" {
            return Err(CoreError::Runtime(RuntimeError::Protocol(
                "runtime cancel returned an invalid terminal event".into(),
            )));
        }
        self.apply_runtime_event(event)?;
        self.state
            .run(run_id)
            .cloned()
            .ok_or(CoreError::RunNotFound)
    }

    pub fn begin_runtime_dispatch(&mut self, run_id: &str) -> Result<RuntimeDispatch, CoreError> {
        let run = self
            .state
            .run(run_id)
            .cloned()
            .ok_or(CoreError::RunNotFound)?;
        if run.status.is_terminal() {
            return Err(CoreError::State(StateTransitionError::TerminalImmutable {
                from: run.status,
                to: ExecutionStatus::Assembling,
            }));
        }
        let assembling_version = run.version;
        self.transition_and_persist(
            run_id,
            ExecutionStatus::Assembling,
            assembling_version,
            None,
        )?;

        let request = self.runtime_request_for(&run);
        let stream = self.runtime_for_run(run_id)?.stream_events(&request)?;
        Ok(RuntimeDispatch {
            run_id: run_id.to_owned(),
            stream,
            timeout: Duration::from_millis(request.timeout_ms.max(1)),
        })
    }

    pub fn apply_runtime_dispatch_event(
        &mut self,
        run_id: &str,
        event: RuntimeEvent,
    ) -> Result<bool, CoreError> {
        if event.execution_run_id != run_id {
            return Err(CoreError::Runtime(RuntimeError::Protocol(
                "Runtime stream emitted an event for a different execution run".into(),
            )));
        }
        let terminal = is_terminal_event(&event.event_type);
        self.apply_runtime_event(event)?;
        Ok(terminal)
    }

    pub fn fail_runtime_dispatch(
        &mut self,
        run_id: &str,
        error: &RuntimeError,
    ) -> Result<(), CoreError> {
        self.persist_runtime_failure(run_id, error)
    }

    pub fn execution_is_terminal(&self, run_id: &str) -> Result<bool, CoreError> {
        Ok(self
            .state
            .run(run_id)
            .ok_or(CoreError::RunNotFound)?
            .status
            .is_terminal())
    }

    fn drive_runtime(&mut self, run_id: &str) -> Result<(), CoreError> {
        let dispatch = match self.begin_runtime_dispatch(run_id) {
            Ok(dispatch) => dispatch,
            Err(CoreError::Runtime(error)) => {
                self.persist_runtime_failure(run_id, &error)?;
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let stream = dispatch.stream;
        let timeout = dispatch.timeout;
        let started = Instant::now();

        loop {
            let Some(remaining) = timeout.checked_sub(started.elapsed()) else {
                let _ = stream.cancel();
                self.persist_runtime_failure(run_id, &RuntimeError::Timeout)?;
                return Ok(());
            };
            match stream.next_timeout(remaining) {
                Ok(Some(event)) => {
                    let terminal = self.apply_runtime_dispatch_event(run_id, event)?;
                    if terminal {
                        return Ok(());
                    }
                }
                Ok(None) => {
                    self.persist_runtime_failure(run_id, &RuntimeError::StreamTerminalMissing)?;
                    return Ok(());
                }
                Err(error) => {
                    if error == RuntimeError::Timeout {
                        let _ = stream.cancel();
                    }
                    self.persist_runtime_failure(run_id, &error)?;
                    return Ok(());
                }
            }
        }
    }

    fn apply_runtime_event(&mut self, mut event: RuntimeEvent) -> Result<(), CoreError> {
        self.decorate_runtime_event(&mut event)?;
        let run_id = event.execution_run_id.clone();
        // Dispatch and an explicit cancel command can race on separate
        // connections. A terminal winner is immutable; discard the losing
        // late event before appending it so replay never contains two
        // conflicting terminal records (or post-terminal deltas).
        if self.execution_is_terminal(&run_id)? {
            return Ok(());
        }
        self.state.append_runtime_event(event.clone())?;
        match event.event_type.as_str() {
            "runtime.started" => {
                let version = self.current_run_version(&run_id)?;
                self.state
                    .transition(&run_id, ExecutionStatus::Running, version, None)?;
            }
            "execution.completed" => {
                let version = self.current_run_version(&run_id)?;
                self.state
                    .transition(&run_id, ExecutionStatus::Verifying, version, None)?;
                let version = self.current_run_version(&run_id)?;
                self.state.transition(
                    &run_id,
                    ExecutionStatus::Completed,
                    version,
                    Some("runtime_completed".into()),
                )?;
            }
            "execution.failed" => {
                let version = self.current_run_version(&run_id)?;
                self.state.transition(
                    &run_id,
                    ExecutionStatus::Failed,
                    version,
                    Some("runtime_failed".into()),
                )?;
            }
            "execution.cancelled" => {
                let version = self.current_run_version(&run_id)?;
                self.state.transition(
                    &run_id,
                    ExecutionStatus::Cancelled,
                    version,
                    Some("runtime_cancelled".into()),
                )?;
            }
            "execution.interrupted" => {
                let version = self.current_run_version(&run_id)?;
                self.state.transition(
                    &run_id,
                    ExecutionStatus::Interrupted,
                    version,
                    Some("runtime_interrupted".into()),
                )?;
            }
            _ => {}
        }
        let run = self
            .state
            .run(&run_id)
            .cloned()
            .ok_or(CoreError::RunNotFound)?;
        self.persist_run_and_events(&run)?;
        self.accept_runtime_handoff_proposal(&event)
    }

    fn decorate_runtime_event(&self, event: &mut RuntimeEvent) -> Result<(), CoreError> {
        // Older persisted or direct-Core runs may predate the model snapshot
        // table. Preserve their default-runtime cancellation/event behavior
        // while all newly created runs retain an explicit frozen binding.
        let legacy_binding = ExecutionRuntimeBinding {
            connector_id: self.default_runtime_id().to_owned(),
            runtime_type: Some(self.default_runtime_id().to_owned()),
            model_id: None,
            catalog_revision: Some(runtime_model_catalog_revision(self.default_runtime())),
            validate_profile: false,
        };
        let binding = self
            .execution_bindings
            .get(&event.execution_run_id)
            .unwrap_or(&legacy_binding);
        let runtime = self.runtime_for_binding(binding)?;
        if event.runtime_id != runtime.id() {
            return Err(CoreError::Runtime(RuntimeError::Protocol(
                "runtime event adapter identity does not match the frozen connector route".into(),
            )));
        }
        let runtime_type = binding
            .runtime_type
            .clone()
            .unwrap_or_else(|| runtime.id().to_owned());
        let route = [
            ("connectorId", Value::String(binding.connector_id.clone())),
            ("runtimeType", Value::String(runtime_type)),
            (
                "modelId",
                binding
                    .model_id
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Null),
            ),
            (
                "catalogRevision",
                binding
                    .catalog_revision
                    .map(serde_json::Number::from)
                    .map(Value::Number)
                    .unwrap_or(Value::Null),
            ),
        ];
        let payload = event.payload.as_object_mut().ok_or_else(|| {
            CoreError::Runtime(RuntimeError::Protocol(
                "runtime event payload must be an object".into(),
            ))
        })?;
        for (field, value) in route {
            if let Some(existing) = payload.get(field) {
                if existing != &value {
                    return Err(CoreError::Runtime(RuntimeError::Protocol(format!(
                        "runtime event {field} does not match the frozen connector route"
                    ))));
                }
            } else {
                payload.insert(field.into(), value);
            }
        }
        Ok(())
    }

    fn accept_runtime_handoff_proposal(&mut self, event: &RuntimeEvent) -> Result<(), CoreError> {
        let handoffs = match parse_runtime_handoff_proposals(event) {
            Ok(Some(handoffs)) => handoffs,
            Ok(None) => self
                .parse_legacy_runtime_handoff_proposals(event)?
                .unwrap_or_default(),
            // Runtime proposals are advisory. A malformed structured payload
            // must not turn an otherwise valid execution into a Core failure.
            Err(_) => Vec::new(),
        };
        if handoffs.is_empty() {
            return Ok(());
        }

        // Validate the complete batch before writing any proposal. A malformed
        // or out-of-scope member must never leave a partial batch in storage.
        for handoff in &handoffs {
            if let Err(error) = self.validate_runtime_handoff_proposal(handoff) {
                return if is_safe_handoff_rejection(&error) {
                    Ok(())
                } else {
                    Err(error)
                };
            }
        }
        for handoff in handoffs {
            match self.create_handoff(CreateHandoffCommand { handoff }) {
                Ok(_) => {}
                Err(error) if is_safe_handoff_rejection(&error) => return Ok(()),
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    fn parse_legacy_runtime_handoff_proposals(
        &self,
        event: &RuntimeEvent,
    ) -> Result<Option<Vec<Handoff>>, CoreError> {
        if !matches!(
            event.event_type.as_str(),
            "execution.completed" | "output.completed"
        ) {
            return Ok(None);
        }
        let Some(parent_run) = self.state.run(&event.execution_run_id) else {
            return Ok(None);
        };
        let Some(source_message_id) = runtime_payload_string(&event.payload, "sourceMessageId")
        else {
            // Legacy output has no authoritative source message unless the
            // adapter carries the binding explicitly. Never guess it from the
            // latest conversation message.
            return Ok(None);
        };
        let output = self.runtime_output_for_run(&event.execution_run_id, event);
        if output.trim().is_empty() {
            return Ok(None);
        }
        let candidates = self.legacy_handoff_candidates(parent_run)?;
        let Some(to_agent_ids) = parse_legacy_handoff_mentions(&output, &candidates) else {
            return Ok(None);
        };
        if to_agent_ids
            .iter()
            .any(|agent_id| agent_id == &parent_run.agent_id)
        {
            return Ok(None);
        }
        let batch_id = (to_agent_ids.len() > 1).then(|| {
            format!(
                "legacy-handoff-batch-{}",
                sha256_hex(&format!(
                    "{}:{}:{}",
                    event.execution_run_id,
                    source_message_id,
                    to_agent_ids.join(":"),
                ))
            )
        });
        let dispatch_mode = if batch_id.is_some() {
            "parallel"
        } else {
            "sequential"
        };
        let handoffs = to_agent_ids
            .into_iter()
            .enumerate()
            .map(|(sequence_index, to_agent_id)| {
                let handoff_id = if let Some(batch_id) = &batch_id {
                    format!(
                        "legacy-handoff-{}",
                        sha256_hex(&format!("{}:{}", batch_id, to_agent_id))
                    )
                } else {
                    format!(
                        "legacy-handoff-{}",
                        sha256_hex(&format!(
                            "{}:{}:{}",
                            event.execution_run_id, source_message_id, to_agent_id
                        ))
                    )
                };
                Handoff {
                    id: handoff_id,
                    collaboration_run_id: parent_run.collaboration_run_id.clone(),
                    from_execution_run_id: event.execution_run_id.clone(),
                    to_agent_id: to_agent_id.clone(),
                    status: "proposed".into(),
                    details: Some(StructuredHandoffDetails {
                        parent_execution_run_id: Some(event.execution_run_id.clone()),
                        child_execution_run_id: None,
                        source_message_id: Some(source_message_id.clone()),
                        from_agent_id: Some(parent_run.agent_id.clone()),
                        to_agent_id: Some(to_agent_id),
                        kind: Some("task".into()),
                        dispatch_mode: Some(dispatch_mode.into()),
                        batch_id: batch_id.clone(),
                        sequence_index: batch_id.as_ref().map(|_| sequence_index as u64),
                        detected_by: Some("legacy_mention".into()),
                        task: None,
                        reason: Some("legacy @ mention".into()),
                        decisions: None,
                        constraints: None,
                        artifacts: None,
                        expected_output: None,
                        context_scope: Some("conversation".into()),
                        agent_path: None,
                    }),
                }
            })
            .collect::<Vec<_>>();
        for handoff in &handoffs {
            validate_handoff_shape(handoff)?;
        }
        Ok(Some(handoffs))
    }

    fn runtime_output_for_run(&self, run_id: &str, terminal_event: &RuntimeEvent) -> String {
        let mut deltas = String::new();
        let mut terminal_output = None;
        for event in self.state.replay_events(0) {
            if event.execution_run_id != run_id {
                continue;
            }
            if event.event_type == "output.delta" {
                if let Some(text) = runtime_payload_text(&event.payload) {
                    deltas.push_str(&text);
                }
            } else if matches!(
                event.event_type.as_str(),
                "execution.completed" | "output.completed"
            ) {
                terminal_output = runtime_payload_text(&event.payload);
            }
        }
        if let Some(text) = runtime_payload_text(&terminal_event.payload) {
            terminal_output = Some(text);
        }
        terminal_output
            .filter(|text| !text.trim().is_empty())
            .unwrap_or(deltas)
    }

    fn legacy_handoff_candidates(
        &self,
        run: &ExecutionRun,
    ) -> Result<Vec<LegacyMentionCandidate>, CoreError> {
        let projection = self.storage.projection_snapshot()?;
        let names = projection
            .get("agents")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|agent| {
                Some((
                    agent.get("id")?.as_str()?.to_owned(),
                    agent.get("name")?.as_str()?.to_owned(),
                ))
            })
            .collect::<HashMap<_, _>>();
        let conversation_roster = projection
            .get("conversationAgents")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter(|assignment| {
                assignment
                    .get("conversationId")
                    .and_then(serde_json::Value::as_str)
                    == Some(run.conversation_id.as_str())
                    && assignment
                        .get("enabled")
                        .and_then(serde_json::Value::as_bool)
                        == Some(true)
            })
            .filter_map(|assignment| {
                assignment
                    .get("agentId")
                    .and_then(serde_json::Value::as_str)
            })
            .map(str::to_owned)
            .collect::<HashSet<_>>();
        let candidates = projection
            .get("assignments")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter(|assignment| {
                assignment
                    .get("projectId")
                    .and_then(serde_json::Value::as_str)
                    == Some(run.project_id.as_str())
                    && assignment
                        .get("enabled")
                        .and_then(serde_json::Value::as_bool)
                        == Some(true)
            })
            .filter_map(|assignment| {
                assignment
                    .get("agentId")
                    .and_then(serde_json::Value::as_str)
            })
            .filter(|agent_id| {
                conversation_roster.is_empty() || conversation_roster.contains(*agent_id)
            })
            .filter_map(|agent_id| {
                Some(LegacyMentionCandidate {
                    id: agent_id.to_owned(),
                    name: names.get(agent_id)?.clone(),
                })
            })
            .collect();
        Ok(candidates)
    }

    fn validate_runtime_handoff_proposal(&self, handoff: &Handoff) -> Result<(), CoreError> {
        validate_handoff_shape(handoff)?;
        let projection = self.storage.projection_snapshot()?;
        let parent_run = self
            .state
            .run(&handoff.from_execution_run_id)
            .cloned()
            .or(self
                .storage
                .load_execution_run(&handoff.from_execution_run_id)?)
            .ok_or(CoreError::HandoffExecutionNotFound)?;
        if parent_run.collaboration_run_id != handoff.collaboration_run_id {
            return Err(CoreError::HandoffStructuredDetailsMissing);
        }
        let project_id = parent_run.project_id.as_str();
        let collaboration = projection
            .get("collaborationRuns")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .find(|run| {
                run.get("id").and_then(serde_json::Value::as_str)
                    == Some(handoff.collaboration_run_id.as_str())
            })
            .ok_or(CoreError::HandoffCollaborationNotFound)?;
        if collaboration
            .get("projectId")
            .and_then(serde_json::Value::as_str)
            != Some(project_id)
        {
            return Err(CoreError::HandoffStructuredDetailsMissing);
        }
        let target_rostered = projection
            .get("assignments")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .any(|assignment| {
                assignment
                    .get("projectId")
                    .and_then(serde_json::Value::as_str)
                    == Some(project_id)
                    && assignment
                        .get("agentId")
                        .and_then(serde_json::Value::as_str)
                        == Some(handoff.to_agent_id.as_str())
                    && assignment
                        .get("enabled")
                        .and_then(serde_json::Value::as_bool)
                        == Some(true)
            });
        if !target_rostered {
            return Err(CoreError::HandoffAgentNotInProject);
        }
        self.validate_handoff_dispatch_policy(handoff)?;
        if self.collaboration_auto_dispatches(&handoff.collaboration_run_id)? {
            required_handoff_task(handoff.details.as_ref())?;
        }
        Ok(())
    }

    fn persist_runtime_failure(
        &mut self,
        run_id: &str,
        error: &RuntimeError,
    ) -> Result<(), CoreError> {
        // An explicit cancel/completed event may have won while a transport
        // worker was unwinding. Do not append a second synthetic terminal.
        if self.execution_is_terminal(run_id)? {
            return Ok(());
        }
        let next = match error {
            RuntimeError::Cancelled => ExecutionStatus::Cancelled,
            RuntimeError::TransportClosed | RuntimeError::StreamTerminalMissing => {
                ExecutionStatus::Interrupted
            }
            _ => ExecutionStatus::Failed,
        };
        let event_type = match next {
            ExecutionStatus::Cancelled => "execution.cancelled",
            ExecutionStatus::Interrupted => "execution.interrupted",
            _ => "execution.failed",
        };
        let binding = self.execution_bindings.get(run_id);
        let runtime_id = binding
            .and_then(|value| value.runtime_type.clone())
            .unwrap_or_else(|| self.default_runtime_id().to_owned());
        let payload = serde_json::json!({
            "reason": runtime_error_reason(error),
            "connectorId": binding.map(|value| value.connector_id.clone()),
            "runtimeType": binding.and_then(|value| value.runtime_type.clone()),
            "modelId": binding.and_then(|value| value.model_id.clone()),
            "catalogRevision": binding.and_then(|value| value.catalog_revision),
        });
        self.state.append_runtime_event(RuntimeEvent {
            event_id: format!("{event_type}-{run_id}-runtime-error"),
            execution_run_id: run_id.into(),
            runtime_id,
            thread_id: None,
            turn_id: None,
            sequence: 0,
            event_type: event_type.into(),
            timestamp_ms: 0,
            payload,
        })?;
        let version = self.current_run_version(run_id)?;
        self.state.transition(
            run_id,
            next,
            version,
            Some(runtime_error_reason(error).into()),
        )?;
        let run = self
            .state
            .run(run_id)
            .cloned()
            .ok_or(CoreError::RunNotFound)?;
        self.persist_run_and_events(&run)
    }

    fn current_run_version(&self, run_id: &str) -> Result<u64, CoreError> {
        self.state
            .run(run_id)
            .map(|run| run.version)
            .ok_or(CoreError::RunNotFound)
    }

    fn transition_and_persist(
        &mut self,
        run_id: &str,
        next: ExecutionStatus,
        expected_version: u64,
        reason: Option<String>,
    ) -> Result<(), CoreError> {
        self.state
            .transition(run_id, next, expected_version, reason)?;
        let run = self
            .state
            .run(run_id)
            .cloned()
            .ok_or(CoreError::RunNotFound)?;
        self.persist_run_and_events(&run)
    }

    pub fn transition(
        &mut self,
        run_id: &str,
        next: ExecutionStatus,
        expected_version: u64,
        reason: Option<String>,
    ) -> Result<TransitionOutcome, CoreError> {
        let outcome = self
            .state
            .transition(run_id, next, expected_version, reason)?;
        if outcome == TransitionOutcome::Idempotent {
            return Ok(outcome);
        }
        let run = self
            .state
            .run(run_id)
            .ok_or(CoreError::RunNotFound)?
            .clone();
        self.persist_run_and_events(&run)?;
        Ok(outcome)
    }

    pub fn recover_run(&self, run_id: &str) -> Result<Option<ExecutionRun>, CoreError> {
        Ok(self.storage.load_execution_run(run_id)?)
    }

    pub fn model_snapshot(&self, run_id: &str) -> Result<Option<ModelSnapshot>, CoreError> {
        Ok(self
            .model_snapshots
            .get(run_id)
            .cloned()
            .or(self.storage.load_model_snapshot(run_id)?))
    }

    pub fn model_selection_snapshot(
        &self,
        run_id: &str,
    ) -> Result<Option<ModelSelectionSnapshot>, CoreError> {
        Ok(self
            .model_selection_snapshots
            .get(run_id)
            .cloned()
            .or(self.storage.load_model_selection_snapshot(run_id)?))
    }

    pub fn migration_checksum(&self) -> String {
        self.storage.migration_checksum()
    }

    pub fn load_command_receipt(
        &self,
        key: &CommandReceiptKey,
    ) -> Result<Option<CommandReceipt>, CoreError> {
        Ok(self.storage.load_command_receipt(key)?)
    }

    pub fn save_command_receipt(&mut self, receipt: &CommandReceipt) -> Result<(), CoreError> {
        self.storage.upsert_command_receipt(receipt)?;
        Ok(())
    }

    pub fn event_cursor(&self) -> u64 {
        self.persisted_event_cursor
    }

    pub fn event_stream_epoch(&self) -> &str {
        &self.event_stream_epoch
    }

    pub fn shutdown_owned_runtimes(&self) -> Result<(), CoreError> {
        self.runtimes.shutdown_owned()
    }

    /// A single legacy/default Runtime retains the synchronous v1 command
    /// behavior. A registry with multiple adapters needs worker dispatch so
    /// independently accepted Connector runs can make progress concurrently.
    pub fn uses_deferred_runtime_dispatch(&self) -> bool {
        self.runtimes.has_multiple_adapters()
    }

    /// Returns the credential-free, Core-owned runtime model catalog projection.
    ///
    /// The adapter contributes model IDs only. Core derives the rest of the
    /// response from the adapter's identity, discovery, health, and fixed
    /// capability surface; provider configuration and health detail never
    /// cross this boundary.
    pub fn runtime_models(&self) -> serde_json::Value {
        runtime_models_payload(self.default_runtime())
    }

    /// Returns a credential-free runtime/Connector health projection. The
    /// adapter's free-form detail is reduced to a boolean so authorization,
    /// endpoint and provider diagnostics cannot cross the IPC boundary.
    pub fn runtime_health(&self) -> serde_json::Value {
        runtime_health_payload(self.default_runtime())
    }

    /// Returns the current read-only local Connector candidates. Discovery does
    /// not create a Connector profile or Agent and never reads/writes SQLite.
    pub fn discover_local_connectors(&self) -> serde_json::Value {
        local_connector_discovery_payload(agenttalk_runtime_host::discover_local_connectors())
    }

    /// `agent.scan_local` is a presentation alias for the same safe candidate
    /// snapshot. The UI decides whether to create an Agent later through its
    /// existing explicit mutation workflow.
    pub fn scan_local_agents(&self) -> serde_json::Value {
        self.discover_local_connectors()
    }

    /// Returns the health projection for one persisted Connector profile.
    ///
    /// Profile metadata is used only to select and label the active adapter.
    /// This method never resolves `auth_env_key`, performs a live Provider
    /// call, or exposes free-form Runtime health detail.
    pub fn connector_health(
        &self,
        scope_id: &str,
        connector_id: &str,
    ) -> Result<serde_json::Value, CoreError> {
        let profile = self
            .storage
            .query_connector_profiles(scope_id, Some(connector_id), 1)?
            .into_iter()
            .next()
            .ok_or(CoreError::ConnectorNotFound)?;
        Ok(connector_health_payload(
            &profile,
            self.runtimes.adapter(&profile.runtime_type),
        ))
    }

    /// Returns a credential-free model catalog for one persisted Connector.
    /// This is intentionally distinct from `runtime.models`, which remains a
    /// legacy query for the active/default Runtime only.
    pub fn connector_models(
        &self,
        scope_id: &str,
        connector_id: &str,
    ) -> Result<serde_json::Value, CoreError> {
        let profile = self
            .storage
            .query_connector_profiles(scope_id, Some(connector_id), 1)?
            .into_iter()
            .next()
            .ok_or(CoreError::ConnectorNotFound)?;
        let runtime = self.runtime_for_profile(&profile)?;
        let models = runtime.list_models_checked()?;
        connector_models_payload(&profile, runtime, models)
    }

    pub fn projection_snapshot(&self) -> Result<serde_json::Value, CoreError> {
        Ok(self.storage.projection_snapshot()?)
    }

    /// Exports only the project-scoped configuration that the new local Core
    /// can safely reproduce. Workspace roots, runtime credentials, health
    /// details, execution history and source bodies are intentionally absent.
    pub fn export_project_config(&self, project_id: &str) -> Result<Value, CoreError> {
        let snapshot = self.storage.projection_snapshot()?;
        let project = snapshot
            .get("projects")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .find(|project| project.get("id").and_then(Value::as_str) == Some(project_id))
            .cloned()
            .ok_or(CoreError::ConfigTransferProjectNotFound)?;

        let assignments = snapshot
            .get("assignments")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter(|assignment| {
                assignment.get("projectId").and_then(Value::as_str) == Some(project_id)
            })
            .cloned()
            .collect::<Vec<_>>();
        let assigned_agent_ids = assignments
            .iter()
            .filter_map(|assignment| assignment.get("agentId").and_then(Value::as_str))
            .collect::<HashSet<_>>();
        let agents = snapshot
            .get("agents")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter(|agent| {
                agent
                    .get("id")
                    .and_then(Value::as_str)
                    .is_some_and(|id| assigned_agent_ids.contains(id))
            })
            .map(|agent| {
                json!({
                    "id": agent.get("id"),
                    "name": agent.get("name"),
                    "role": agent.get("role"),
                    "specialty": agent.get("specialty"),
                    "systemPrompt": agent.get("systemPrompt"),
                })
            })
            .collect::<Vec<_>>();
        let project_agents = assignments
            .iter()
            .map(|assignment| {
                json!({
                    "projectId": project_id,
                    "agentId": assignment.get("agentId"),
                    "enabled": assignment.get("enabled").and_then(Value::as_bool).unwrap_or(false),
                    "workspaceAccess": assignment
                        .get("workspaceAccess")
                        .and_then(Value::as_str)
                        .unwrap_or("none"),
                })
            })
            .collect::<Vec<_>>();
        let conversations = snapshot
            .get("conversations")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter(|conversation| {
                conversation.get("projectId").and_then(Value::as_str) == Some(project_id)
            })
            .map(|conversation| {
                json!({
                    "id": conversation.get("id"),
                    "projectId": project_id,
                    "title": conversation.get("title"),
                })
            })
            .collect::<Vec<_>>();
        let workflow_templates = snapshot
            .get("workflows")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter(|workflow| {
                workflow.get("projectId").and_then(Value::as_str) == Some(project_id)
            })
            .map(|workflow| {
                let steps = workflow
                    .get("stepsJson")
                    .and_then(Value::as_str)
                    .and_then(|steps| serde_json::from_str::<Value>(steps).ok())
                    .filter(Value::is_array)
                    .and_then(|steps| {
                        Some(
                            steps
                                .as_array()?
                                .iter()
                                .map(|step| {
                                    json!({
                                        "id": step.get("id"),
                                        "order": step.get("order"),
                                        "agentId": step
                                            .get("agent_id")
                                            .or_else(|| step.get("agentId")),
                                        "promptSupplement": step
                                            .get("prompt_supplement")
                                            .or_else(|| step.get("promptSupplement")),
                                    })
                                })
                                .collect::<Vec<_>>(),
                        )
                    })
                    .map_or_else(|| json!([]), |steps| json!(steps));
                json!({
                    "id": workflow.get("id"),
                    "projectId": project_id,
                    "name": workflow.get("name"),
                    "kind": workflow.get("kind"),
                    "steps": steps,
                })
            })
            .collect::<Vec<_>>();

        Ok(json!({
            "schemaVersion": CONFIG_TRANSFER_SCHEMA_VERSION,
            "version": "1.0",
            "exportedAt": format!("unix-ms:{}", unix_time_millis()),
            "project": {
                "id": project.get("id"),
                "name": project.get("name"),
                "description": Value::Null,
                "rootPath": Value::Null,
                "archived": project.get("archived").and_then(Value::as_bool).unwrap_or(false),
            },
            "agents": agents,
            "projectAgents": project_agents,
            "conversations": conversations,
            "workflowTemplates": workflow_templates,
        }))
    }

    /// Imports a bounded, safe configuration snapshot. All destination IDs
    /// are regenerated, and the source workspace root is never bound without
    /// a separate local authorization action.
    pub fn import_project_config(
        &mut self,
        config: Value,
    ) -> Result<ConfigImportResult, CoreError> {
        let encoded = serde_json::to_vec(&config)
            .map_err(|error| CoreError::ConfigTransferInvalid(error.to_string()))?;
        if encoded.len() > MAX_CONFIG_TRANSFER_BYTES {
            return Err(CoreError::ConfigTransferTooLarge);
        }
        let root = config
            .as_object()
            .ok_or_else(|| ConfigTransferError::message("payload must be an object"))?;
        ensure_allowed_keys(
            root,
            &[
                "schemaVersion",
                "version",
                "exportedAt",
                "project",
                "agents",
                "projectAgents",
                "conversations",
                "workflowTemplates",
            ],
            "root",
        )?;
        if root
            .get("schemaVersion")
            .and_then(Value::as_str)
            .is_some_and(|version| version != CONFIG_TRANSFER_SCHEMA_VERSION)
        {
            return Err(ConfigTransferError::message("unsupported schemaVersion"));
        }
        let project = required_object(root.get("project"), "project")?;
        ensure_allowed_keys(
            project,
            &[
                "id",
                "name",
                "description",
                "rootPath",
                "archived",
                "workspaceRevision",
                "workspaceValidationStatus",
                "workspaceValidationMessage",
                "sortOrder",
                "createdAt",
            ],
            "project",
        )?;
        let source_project_id = required_text(project.get("id"), "project.id")?;
        let project_name = required_text(project.get("name"), "project.name")?;
        let project_archived = project
            .get("archived")
            .map(|value| {
                value
                    .as_bool()
                    .ok_or_else(|| ConfigTransferError::message("project.archived must be boolean"))
            })
            .transpose()?
            .unwrap_or(false);
        let source_root_present = project
            .get("rootPath")
            .and_then(Value::as_str)
            .is_some_and(|root| !root.trim().is_empty());

        let agents_value = root.get("agents").cloned().unwrap_or_else(|| json!([]));
        let agents = bounded_array(&agents_value, "agents", 100)?;
        let mut source_agent_ids = HashSet::new();
        let mut imported_agents = Vec::with_capacity(agents.len());
        for (index, value) in agents.iter().enumerate() {
            let object = value.as_object().ok_or_else(|| {
                ConfigTransferError::message(format!("agents[{index}] must be an object"))
            })?;
            ensure_allowed_keys(
                object,
                &[
                    "id",
                    "name",
                    "role",
                    "specialty",
                    "systemPrompt",
                    "avatarKey",
                    "avatarUrl",
                    "accentColor",
                    "sortOrder",
                    "runtimeType",
                    "providerType",
                    "enabled",
                    "connectorId",
                    "modelId",
                    "apiFormat",
                    "authEnvKey",
                    "capabilities",
                    "isVerified",
                    "verifiedAt",
                    "lastError",
                    "lastTestStatus",
                ],
                &format!("agents[{index}]"),
            )?;
            let source_id = required_text(object.get("id"), &format!("agents[{index}].id"))?;
            if !source_agent_ids.insert(source_id.clone()) {
                return Err(ConfigTransferError::message("duplicate agent id"));
            }
            let identity = AgentIdentity {
                id: String::new(),
                name: required_text(object.get("name"), &format!("agents[{index}].name"))?,
                role: required_text(object.get("role"), &format!("agents[{index}].role"))?,
                specialty: required_text(
                    object.get("specialty"),
                    &format!("agents[{index}].specialty"),
                )?,
                system_prompt: object
                    .get("systemPrompt")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
            };
            imported_agents.push((source_id, identity));
        }

        let assignments_value = root
            .get("projectAgents")
            .cloned()
            .unwrap_or_else(|| json!([]));
        let assignments = bounded_array(&assignments_value, "projectAgents", 100)?;
        let mut assigned_ids = HashSet::new();
        let mut assignment_access = Vec::with_capacity(assignments.len());
        for (index, value) in assignments.iter().enumerate() {
            let object = value.as_object().ok_or_else(|| {
                ConfigTransferError::message(format!("projectAgents[{index}] must be an object"))
            })?;
            ensure_allowed_keys(
                object,
                &[
                    "projectId",
                    "agentId",
                    "enabled",
                    "workspaceAccess",
                    "role",
                    "specialty",
                    "systemPrompt",
                    "sortOrder",
                ],
                &format!("projectAgents[{index}]"),
            )?;
            if object
                .get("projectId")
                .and_then(Value::as_str)
                .is_some_and(|id| id != source_project_id)
            {
                return Err(ConfigTransferError::message(
                    "project assignment crosses project scope",
                ));
            }
            let agent_id = required_text(
                object.get("agentId"),
                &format!("projectAgents[{index}].agentId"),
            )?;
            if !source_agent_ids.contains(&agent_id) || !assigned_ids.insert(agent_id.clone()) {
                return Err(ConfigTransferError::message(
                    "project assignment references an invalid or duplicate agent",
                ));
            }
            let enabled = object
                .get("enabled")
                .map(|value| {
                    value.as_bool().ok_or_else(|| {
                        ConfigTransferError::message("projectAgents.enabled must be boolean")
                    })
                })
                .transpose()?
                .unwrap_or(true);
            let access = parse_config_workspace_access(
                object
                    .get("workspaceAccess")
                    .and_then(Value::as_str)
                    .unwrap_or("none"),
            )?;
            assignment_access.push((agent_id, enabled, access));
        }

        let conversations_value = root
            .get("conversations")
            .cloned()
            .unwrap_or_else(|| json!([]));
        let conversations = bounded_array(&conversations_value, "conversations", 100)?;
        let mut imported_conversation_titles = Vec::with_capacity(conversations.len());
        let mut source_conversation_ids = HashSet::new();
        for (index, value) in conversations.iter().enumerate() {
            let object = value.as_object().ok_or_else(|| {
                ConfigTransferError::message(format!("conversations[{index}] must be an object"))
            })?;
            ensure_allowed_keys(
                object,
                &[
                    "id",
                    "projectId",
                    "title",
                    "description",
                    "isArchived",
                    "sortOrder",
                    "scopeRevision",
                    "createdAt",
                ],
                &format!("conversations[{index}]"),
            )?;
            let source_id = required_text(object.get("id"), &format!("conversations[{index}].id"))?;
            if !source_conversation_ids.insert(source_id) {
                return Err(ConfigTransferError::message("duplicate conversation id"));
            }
            if object
                .get("projectId")
                .and_then(Value::as_str)
                .is_some_and(|id| id != source_project_id)
            {
                return Err(ConfigTransferError::message(
                    "conversation crosses project scope",
                ));
            }
            imported_conversation_titles.push(required_text(
                object.get("title"),
                &format!("conversations[{index}].title"),
            )?);
        }

        let workflows_value = root
            .get("workflowTemplates")
            .cloned()
            .unwrap_or_else(|| json!([]));
        let workflows = bounded_array(&workflows_value, "workflowTemplates", 100)?;
        let source_to_new_agent = imported_agents
            .iter()
            .enumerate()
            .map(|(index, (source_id, _))| {
                (
                    source_id.clone(),
                    format!("imported-agent-{}-{}", unix_time_nanos(), index),
                )
            })
            .collect::<HashMap<_, _>>();
        let mut imported_workflows = Vec::with_capacity(workflows.len());
        for (index, value) in workflows.iter().enumerate() {
            let object = value.as_object().ok_or_else(|| {
                ConfigTransferError::message(format!(
                    "workflowTemplates[{index}] must be an object"
                ))
            })?;
            ensure_allowed_keys(
                object,
                &[
                    "id",
                    "projectId",
                    "name",
                    "type",
                    "kind",
                    "steps",
                    "createdAt",
                ],
                &format!("workflowTemplates[{index}]"),
            )?;
            if object
                .get("projectId")
                .and_then(Value::as_str)
                .is_some_and(|id| id != source_project_id)
            {
                return Err(ConfigTransferError::message(
                    "workflow crosses project scope",
                ));
            }
            let kind = object
                .get("kind")
                .or_else(|| object.get("type"))
                .and_then(Value::as_str)
                .filter(|kind| matches!(*kind, "sequential" | "parallel" | "reviewer"))
                .ok_or_else(|| {
                    ConfigTransferError::message(
                        "workflow kind must be sequential, parallel, or reviewer",
                    )
                })?;
            let name = required_text(
                object.get("name"),
                &format!("workflowTemplates[{index}].name"),
            )?;
            let steps_value = object.get("steps").cloned().unwrap_or_else(|| json!([]));
            let steps = bounded_array(
                &steps_value,
                &format!("workflowTemplates[{index}].steps"),
                100,
            )?;
            let mut imported_steps = Vec::with_capacity(steps.len());
            for (step_index, step) in steps.iter().enumerate() {
                let step = step.as_object().ok_or_else(|| {
                    ConfigTransferError::message(format!(
                        "workflow step {index}:{step_index} must be an object"
                    ))
                })?;
                ensure_allowed_keys(
                    step,
                    &["id", "agentId", "order", "role", "promptSupplement"],
                    &format!("workflowTemplates[{index}].steps[{step_index}]"),
                )?;
                let source_agent_id = required_text(
                    step.get("agentId"),
                    &format!("workflowTemplates[{index}].steps[{step_index}].agentId"),
                )?;
                let agent_id = source_to_new_agent.get(&source_agent_id).ok_or_else(|| {
                    ConfigTransferError::message(
                        "workflow step references an agent outside the exported roster",
                    )
                })?;
                let order = step.get("order").and_then(Value::as_u64).ok_or_else(|| {
                    ConfigTransferError::message("workflow step order must be an integer")
                })?;
                let order = u32::try_from(order).map_err(|_| {
                    ConfigTransferError::message("workflow step order is out of range")
                })?;
                imported_steps.push(WorkflowStep {
                    id: String::new(),
                    order,
                    agent_id: agent_id.clone(),
                    prompt_supplement: step
                        .get("promptSupplement")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                });
            }
            imported_workflows.push((name, kind.to_owned(), imported_steps));
        }

        let nonce = unix_time_nanos();
        let config_hash = sha256_hex(&String::from_utf8_lossy(&encoded));
        let new_project_id = format!("imported-project-{nonce}-{}", &config_hash[..12]);
        self.create_project(Project {
            id: new_project_id.clone(),
            name: format!("{project_name} (导入)"),
            root_path: None,
            archived: project_archived,
        })?;
        for (index, (source_id, mut identity)) in imported_agents.into_iter().enumerate() {
            let new_id = source_to_new_agent
                .get(&source_id)
                .cloned()
                .ok_or_else(|| ConfigTransferError::message("agent remap missing"))?;
            identity.id = new_id.clone();
            self.create_agent(identity)?;
            let (enabled, access) = assignment_access
                .iter()
                .find(|(agent_id, _, _)| agent_id == &source_id)
                .map(|(_, enabled, access)| (*enabled, access.clone()))
                .unwrap_or((false, WorkspaceAccess::None));
            self.set_project_agent_assignment(&new_project_id, &new_id, enabled, access)?;
            let _ = index;
        }
        for (index, title) in imported_conversation_titles.into_iter().enumerate() {
            self.create_conversation(Conversation {
                id: format!("imported-conversation-{nonce}-{index}"),
                project_id: new_project_id.clone(),
                title,
                scope_revision: 0,
            })?;
        }
        for (workflow_index, (name, kind, mut steps)) in imported_workflows.into_iter().enumerate()
        {
            let workflow_id = format!("imported-workflow-{nonce}-{workflow_index}");
            for (step_index, step) in steps.iter_mut().enumerate() {
                step.id = format!("{workflow_id}-step-{step_index}");
            }
            self.create_workflow(CreateWorkflowCommand {
                project_id: new_project_id.clone(),
                workflow: WorkflowTemplate {
                    id: workflow_id,
                    name,
                    kind,
                    steps,
                },
            })?;
        }
        Ok(ConfigImportResult {
            new_project_id,
            imported_agents: source_to_new_agent.len() as u32,
            imported_conversations: conversations.len() as u32,
            imported_workflows: workflows.len() as u32,
            workspace_rebind_required: source_root_present,
        })
    }

    pub fn replay_events(&self, after_sequence: u64) -> Result<Vec<RuntimeEvent>, CoreError> {
        Ok(self.storage.replay_after(after_sequence)?)
    }

    pub fn replay_events_limited(
        &self,
        after_sequence: u64,
        limit: u64,
    ) -> Result<Vec<RuntimeEvent>, CoreError> {
        Ok(self.storage.replay_after_limited(after_sequence, limit)?)
    }

    pub fn search_messages(
        &self,
        query: &str,
        conversation_id: Option<&str>,
        limit: u64,
    ) -> Result<Vec<serde_json::Value>, CoreError> {
        Ok(self
            .storage
            .search_messages(query, conversation_id, limit)?)
    }

    pub fn preview_retrieval(
        &self,
        request: RetrievalPreviewRequest,
    ) -> Result<serde_json::Value, CoreError> {
        self.preview_retrieval_with_storage(request, false)
    }

    pub fn preview_retrieval_vector(
        &self,
        request: RetrievalPreviewRequest,
    ) -> Result<serde_json::Value, CoreError> {
        self.preview_retrieval_with_storage(request, true)
    }

    /// Internal adapter boundary for a future real embedding provider. IPC
    /// deliberately exposes only the local fixture mode until an Owner-Gated
    /// provider is verified; Core still maps all provider failures to the
    /// generic retrieval rejection without leaking diagnostics.
    pub fn preview_retrieval_vector_with_provider(
        &self,
        request: RetrievalPreviewRequest,
        provider: &dyn RetrievalEmbeddingProvider,
    ) -> Result<serde_json::Value, CoreError> {
        match self
            .storage
            .preview_retrieval_vector_with_provider(&request, provider)
        {
            Ok(result) => Ok(result),
            Err(StorageError::RetrievalPreviewInvalid { .. }) => {
                Err(CoreError::RetrievalPreviewRejected)
            }
            Err(error) => Err(CoreError::Storage(error)),
        }
    }

    fn preview_retrieval_with_storage(
        &self,
        request: RetrievalPreviewRequest,
        vector_fixture: bool,
    ) -> Result<serde_json::Value, CoreError> {
        let result = if vector_fixture {
            self.storage.preview_retrieval_vector(&request)
        } else {
            self.storage.preview_retrieval(&request)
        };
        match result {
            Ok(result) => Ok(result),
            Err(StorageError::RetrievalPreviewInvalid { .. }) => {
                Err(CoreError::RetrievalPreviewRejected)
            }
            Err(error) => Err(CoreError::Storage(error)),
        }
    }

    pub fn record_projection_changed(&mut self, action: &str) -> Result<(), CoreError> {
        self.state.emit_projection_changed(action)?;
        let event = self
            .state
            .replay_events(self.persisted_event_cursor)
            .last()
            .cloned()
            .ok_or(CoreError::RunNotFound)?;
        self.persisted_event_cursor = self.storage.append_event(&event)?;
        Ok(())
    }

    pub fn create_project(&mut self, project: Project) -> Result<(), CoreError> {
        self.storage
            .create_project(&project.id, &project.name, project.root_path.as_deref())?;
        Ok(())
    }

    pub fn update_project(&mut self, project: Project) -> Result<(), CoreError> {
        if !self.storage.update_project(
            &project.id,
            &project.name,
            project.root_path.as_deref(),
            project.archived,
        )? {
            return Err(CoreError::ProjectionEntityNotFound);
        }
        Ok(())
    }

    pub fn create_agent(&mut self, agent: AgentIdentity) -> Result<(), CoreError> {
        self.storage.create_agent(
            &agent.id,
            &agent.name,
            &agent.role,
            &agent.specialty,
            &agent.system_prompt,
        )?;
        Ok(())
    }

    pub fn update_agent(&mut self, agent: AgentIdentity) -> Result<(), CoreError> {
        if !self.storage.update_agent(
            &agent.id,
            &agent.name,
            &agent.role,
            &agent.specialty,
            &agent.system_prompt,
        )? {
            return Err(CoreError::ProjectionEntityNotFound);
        }
        Ok(())
    }

    pub fn create_conversation(&mut self, conversation: Conversation) -> Result<(), CoreError> {
        self.storage.create_conversation(
            &conversation.id,
            &conversation.project_id,
            &conversation.title,
        )?;
        Ok(())
    }

    pub fn update_conversation(&mut self, conversation: Conversation) -> Result<(), CoreError> {
        if !self
            .storage
            .update_conversation(&conversation.id, &conversation.title)?
        {
            return Err(CoreError::ProjectionEntityNotFound);
        }
        Ok(())
    }

    pub fn create_connector_profile(
        &mut self,
        profile: ConnectorProfile,
    ) -> Result<bool, CoreError> {
        Ok(self.storage.create_connector_profile(&profile)?)
    }

    pub fn import_local_agent(
        &mut self,
        command: ImportLocalAgentCommand,
    ) -> Result<LocalAgentImportOutcome, CoreError> {
        let outcome = self.storage.import_local_agent(&command.request)?;
        if !outcome.reused {
            self.state.restore_project_roster_entry(
                outcome.project_id.clone(),
                outcome.agent_id.clone(),
                true,
                WorkspaceAccess::None,
            );
            self.persisted_event_cursor = outcome.event_sequence;
        }
        Ok(outcome)
    }

    pub fn update_connector_profile(
        &mut self,
        profile: ConnectorProfile,
    ) -> Result<bool, CoreError> {
        Ok(self.storage.update_connector_profile(&profile)?)
    }

    pub fn remove_connector_profile(
        &mut self,
        scope_id: &str,
        connector_id: &str,
    ) -> Result<bool, CoreError> {
        Ok(self
            .storage
            .remove_connector_profile(scope_id, connector_id)?)
    }

    pub fn query_connector_profiles(
        &self,
        scope_id: &str,
        connector_id: Option<&str>,
        limit: u64,
    ) -> Result<Vec<ConnectorProfile>, CoreError> {
        Ok(self
            .storage
            .query_connector_profiles(scope_id, connector_id, limit)?)
    }

    pub fn create_message(&mut self, message: Message) -> Result<(), CoreError> {
        self.storage.create_message(&message)?;
        Ok(())
    }

    pub fn store_memory(
        &mut self,
        command: StoreMemoryCommand,
    ) -> Result<MemoryWriteOutcome, CoreError> {
        if !self.storage.memory_scope_exists(&command.memory.scope_id)? {
            return Err(CoreError::MemoryScopeNotFound);
        }
        if let Some(agent_id) = command.memory.agent_id.as_deref() {
            if !self.storage.agent_exists(agent_id)? {
                return Err(CoreError::MemoryAgentNotFound);
            }
        }
        Ok(if self.storage.store_memory(&command.memory)? {
            MemoryWriteOutcome::Created
        } else {
            MemoryWriteOutcome::AlreadyPresent
        })
    }

    pub fn store_summary(
        &mut self,
        command: StoreSummaryCommand,
    ) -> Result<SummaryWriteOutcome, CoreError> {
        match self.storage.store_summary(&command.summary) {
            Ok(true) => Ok(SummaryWriteOutcome::Created),
            Ok(false) => Ok(SummaryWriteOutcome::AlreadyPresent),
            Err(StorageError::SummaryScopeNotFound { .. }) => Err(CoreError::SummaryScopeNotFound),
            Err(StorageError::SummaryConflict { .. }) => Err(CoreError::SummaryConflict),
            Err(error) => Err(CoreError::Storage(error)),
        }
    }

    pub fn generate_summary(
        &mut self,
        command: GenerateSummaryCommand,
    ) -> Result<SummaryGenerationOutcome, CoreError> {
        if !self.storage.memory_scope_exists(&command.scope_id)? {
            return Err(CoreError::SummaryScopeNotFound);
        }
        let messages = self
            .storage
            .load_recent_message_contents(&command.scope_id, 20)?;
        let mut content = format!(
            "AgentTalk summary ({SUMMARY_GENERATOR_VERSION})\nMessages: {}\n",
            messages.len()
        );
        for (index, message) in messages.iter().enumerate() {
            let trimmed = message.trim();
            if trimmed.is_empty() {
                continue;
            }
            let remaining = SUMMARY_CONTENT_MAX_BYTES.saturating_sub(content.len());
            if remaining == 0 {
                break;
            }
            let prefix = format!("{}. ", index + 1);
            let available = remaining.saturating_sub(prefix.len() + 1);
            let excerpt = if trimmed.len() > available {
                &trimmed[..trimmed
                    .char_indices()
                    .take_while(|(offset, _)| *offset < available)
                    .last()
                    .map(|(offset, character)| offset + character.len_utf8())
                    .unwrap_or(0)]
            } else {
                trimmed
            };
            content.push_str(&prefix);
            content.push_str(excerpt);
            content.push('\n');
        }
        if content.len() > SUMMARY_CONTENT_MAX_BYTES {
            let mut end = SUMMARY_CONTENT_MAX_BYTES;
            while end > 0 && !content.is_char_boundary(end) {
                end -= 1;
            }
            content.truncate(end);
        }
        let content_hash = sha256_hex(&content);
        let artifact_id = format!("summary-artifact-{}", &content_hash[..40]);
        let artifact = Artifact {
            id: artifact_id.clone(),
            sha256: content_hash.clone(),
            size: content.len() as u64,
            mime: "text/plain; charset=utf-8".into(),
            relative_path: None,
        };
        self.store_artifact(StoreArtifactCommand { artifact })?;
        self.store_artifact_body(StoreArtifactBodyCommand {
            artifact_id: artifact_id.clone(),
            body: content.into_bytes(),
        })?;
        let version = self.storage.next_summary_version(&command.scope_id)?;
        let summary = Summary {
            id: format!(
                "summary-{}",
                sha256_hex(&format!("{}:{version}", command.scope_id))
            ),
            scope_id: command.scope_id,
            version,
            content_hash,
            artifact_id: Some(artifact_id),
        };
        self.store_summary(StoreSummaryCommand {
            summary: summary.clone(),
        })?;
        Ok(SummaryGenerationOutcome {
            summary,
            generator: SUMMARY_GENERATOR_VERSION.into(),
            message_count: messages.len() as u64,
        })
    }

    pub fn load_summary_content(&self, summary_id: &str) -> Result<String, CoreError> {
        self.storage
            .load_summary_content(summary_id)
            .map_err(|error| match error {
                StorageError::SummaryContentUnavailable { .. }
                | StorageError::ArtifactBodyNotFound { .. }
                | StorageError::ArtifactBodyMismatch
                | StorageError::ArtifactBodyStoreUnavailable
                | StorageError::ArtifactBodyIo => CoreError::SummaryContentUnavailable,
                other => CoreError::Storage(other),
            })
    }

    pub fn store_artifact(
        &mut self,
        command: StoreArtifactCommand,
    ) -> Result<ArtifactWriteOutcome, CoreError> {
        match self.storage.store_artifact(&command.artifact) {
            Ok(true) => Ok(ArtifactWriteOutcome::Created),
            Ok(false) => Ok(ArtifactWriteOutcome::AlreadyPresent),
            Err(StorageError::ArtifactInvalid { .. }) => Err(CoreError::ArtifactInvalid),
            Err(StorageError::ArtifactConflict { .. }) => Err(CoreError::ArtifactConflict),
            Err(error) => Err(CoreError::Storage(error)),
        }
    }

    pub fn store_artifact_body(
        &self,
        command: StoreArtifactBodyCommand,
    ) -> Result<bool, CoreError> {
        Ok(self
            .storage
            .store_artifact_body(&command.artifact_id, &command.body)?)
    }

    pub fn load_artifact_body(&self, artifact_id: &str) -> Result<Vec<u8>, CoreError> {
        Ok(self.storage.load_artifact_body(artifact_id)?)
    }

    /// Reads a bounded, digest-verified range from the Core-owned Artifact
    /// Store. This is the only Core API intended for large-body IPC callers;
    /// it never materializes or returns the complete blob.
    pub fn read_artifact_body_chunk(
        &self,
        artifact_id: &str,
        offset: u64,
        limit: u64,
    ) -> Result<ArtifactBodyChunk, CoreError> {
        Ok(self
            .storage
            .read_artifact_body_chunk(artifact_id, offset, limit)?)
    }

    pub fn store_attachment(
        &mut self,
        command: StoreAttachmentCommand,
    ) -> Result<AttachmentWriteOutcome, CoreError> {
        match self
            .storage
            .store_attachment(&command.attachment, command.ordinal)
        {
            Ok(true) => Ok(AttachmentWriteOutcome::Created),
            Ok(false) => Ok(AttachmentWriteOutcome::AlreadyPresent),
            Err(StorageError::AttachmentInvalid { .. }) => Err(CoreError::AttachmentInvalid),
            Err(StorageError::AttachmentConflict { .. }) => Err(CoreError::AttachmentConflict),
            Err(StorageError::AttachmentMessageNotFound { .. }) => {
                Err(CoreError::AttachmentMessageNotFound)
            }
            Err(StorageError::AttachmentArtifactNotFound { .. }) => {
                Err(CoreError::AttachmentArtifactNotFound)
            }
            Err(StorageError::AttachmentArtifactMismatch { .. }) => {
                Err(CoreError::AttachmentArtifactMismatch)
            }
            Err(error) => Err(CoreError::Storage(error)),
        }
    }

    /// Imports one explicitly selected file into the Core-owned Artifact Store
    /// and associates its verified metadata with an existing Message. The
    /// source path is a one-time read grant and never appears in the returned
    /// outcome or projection; recovery uses the copied digest-addressed blob.
    pub fn import_attachment_file(
        &mut self,
        command: ImportAttachmentFileCommand,
    ) -> Result<AttachmentFileImportOutcome, CoreError> {
        for value in [
            command.attachment_id.as_str(),
            command.artifact_id.as_str(),
            command.message_id.as_str(),
        ] {
            if value.trim().is_empty() || value.len() > 128 {
                return Err(CoreError::AttachmentInvalid);
            }
        }
        if command.mime.trim().is_empty() || command.mime.len() > 256 || command.ordinal > 1_000_000
        {
            return Err(CoreError::AttachmentInvalid);
        }
        if !command.source_path.is_absolute() {
            return Err(CoreError::ArtifactSourceInvalid);
        }
        if !self.storage.message_exists(&command.message_id)? {
            return Err(CoreError::AttachmentMessageNotFound);
        }

        let mut grant = FileReadGrant::issue(&command.source_path)
            .map_err(|_| CoreError::ArtifactSourceInvalid)?;
        let imported = self
            .storage
            .import_artifact_file_with_grant(&grant)
            .map_err(|error| match error {
                StorageError::ArtifactSourceInvalid => CoreError::ArtifactSourceInvalid,
                other => CoreError::Storage(other),
            })?;
        grant.revoke();
        let artifact = Artifact {
            id: command.artifact_id,
            sha256: imported.sha256.clone(),
            size: imported.size,
            mime: command.mime,
            relative_path: None,
        };
        let artifact_outcome = self.store_artifact(StoreArtifactCommand {
            artifact: artifact.clone(),
        })?;
        let attachment = Attachment {
            id: command.attachment_id,
            message_id: command.message_id,
            artifact_id: artifact.id.clone(),
            file_name: imported.file_name,
            sha256: artifact.sha256.clone(),
            size: artifact.size,
        };
        let attachment_outcome = self.store_attachment(StoreAttachmentCommand {
            attachment: attachment.clone(),
            ordinal: command.ordinal,
        })?;
        Ok(AttachmentFileImportOutcome {
            artifact,
            attachment,
            artifact_outcome,
            attachment_outcome,
            body_stored: imported.body_stored,
        })
    }

    pub fn store_retrieval_source(
        &mut self,
        command: StoreRetrievalSourceCommand,
    ) -> Result<RetrievalWriteOutcome, CoreError> {
        match self.storage.store_retrieval_source(&command.source) {
            Ok(true) => Ok(RetrievalWriteOutcome::Created),
            Ok(false) => Ok(RetrievalWriteOutcome::AlreadyPresent),
            Err(StorageError::RetrievalScopeNotFound { .. }) => {
                Err(CoreError::RetrievalScopeNotFound)
            }
            Err(StorageError::RetrievalConflict { .. }) => Err(CoreError::RetrievalConflict),
            Err(error) => Err(CoreError::Storage(error)),
        }
    }

    pub fn query_retrieval_sources(
        &self,
        scope_id: &str,
        source_ids: Option<&[String]>,
        limit: u64,
    ) -> Result<Vec<serde_json::Value>, CoreError> {
        match self
            .storage
            .query_retrieval_sources(scope_id, source_ids, limit)
        {
            Ok(sources) => Ok(sources),
            Err(StorageError::RetrievalScopeNotFound { .. }) => {
                Err(CoreError::RetrievalScopeNotFound)
            }
            Err(error) => Err(CoreError::Storage(error)),
        }
    }

    pub fn store_retrieval_selection(
        &mut self,
        command: StoreRetrievalSelectionCommand,
    ) -> Result<RetrievalSelectionWriteOutcome, CoreError> {
        match self.storage.store_retrieval_selection(&command.selection) {
            Ok(true) => Ok(RetrievalSelectionWriteOutcome::Created),
            Ok(false) => Ok(RetrievalSelectionWriteOutcome::AlreadyPresent),
            Err(StorageError::RetrievalSelectionScopeInvalid { .. })
            | Err(StorageError::RetrievalSelectionInvalid { .. }) => {
                Err(CoreError::RetrievalSelectionScopeInvalid)
            }
            Err(StorageError::RetrievalSelectionSourceNotFound { .. })
            | Err(StorageError::RetrievalSelectionSourceOutOfScope { .. })
            | Err(StorageError::RetrievalSelectionSourceChanged { .. }) => {
                Err(CoreError::RetrievalSelectionSourceRejected)
            }
            Err(StorageError::RetrievalSelectionConflict { .. }) => {
                Err(CoreError::RetrievalSelectionConflict)
            }
            Err(error) => Err(CoreError::Storage(error)),
        }
    }

    pub fn query_retrieval_selections(
        &self,
        scope_id: &str,
        selection_ids: Option<&[String]>,
        limit: u64,
    ) -> Result<Vec<serde_json::Value>, CoreError> {
        match self
            .storage
            .query_retrieval_selections(scope_id, selection_ids, limit)
        {
            Ok(selections) => Ok(selections),
            Err(StorageError::RetrievalScopeNotFound { .. }) => {
                Err(CoreError::RetrievalScopeNotFound)
            }
            Err(error) => Err(CoreError::Storage(error)),
        }
    }

    pub fn store_retrieval_feedback(
        &mut self,
        command: StoreRetrievalFeedbackCommand,
    ) -> Result<RetrievalFeedbackWriteOutcome, CoreError> {
        match self.storage.store_retrieval_feedback(&command.feedback) {
            Ok(true) => Ok(RetrievalFeedbackWriteOutcome::Created),
            Ok(false) => Ok(RetrievalFeedbackWriteOutcome::AlreadyPresent),
            Err(StorageError::RetrievalFeedbackSelectionNotFound { .. })
            | Err(StorageError::RetrievalFeedbackSourceNotSelected { .. })
            | Err(StorageError::RetrievalFeedbackScopeMismatch { .. })
            | Err(StorageError::RetrievalFeedbackInvalid { .. }) => {
                Err(CoreError::RetrievalFeedbackRejected)
            }
            Err(StorageError::RetrievalFeedbackConflict { .. }) => {
                Err(CoreError::RetrievalFeedbackConflict)
            }
            Err(error) => Err(CoreError::Storage(error)),
        }
    }

    pub fn query_retrieval_feedback(
        &self,
        scope_id: &str,
        selection_id: Option<&str>,
        limit: u64,
    ) -> Result<Vec<serde_json::Value>, CoreError> {
        match self
            .storage
            .query_retrieval_feedback(scope_id, selection_id, limit)
        {
            Ok(feedback) => Ok(feedback),
            Err(StorageError::RetrievalScopeNotFound { .. }) => {
                Err(CoreError::RetrievalScopeNotFound)
            }
            Err(error) => Err(CoreError::Storage(error)),
        }
    }

    pub fn create_workflow(
        &mut self,
        command: CreateWorkflowCommand,
    ) -> Result<WorkflowWriteOutcome, CoreError> {
        match self
            .storage
            .create_workflow(&command.project_id, &command.workflow)
        {
            Ok(true) => Ok(WorkflowWriteOutcome::Created),
            Ok(false) => Ok(WorkflowWriteOutcome::AlreadyPresent),
            Err(StorageError::ProjectNotFound { .. }) => Err(CoreError::WorkflowProjectNotFound),
            Err(StorageError::WorkflowAgentNotInProject { .. }) => {
                Err(CoreError::WorkflowAgentNotInProject)
            }
            Err(StorageError::WorkflowConflict { .. }) => Err(CoreError::WorkflowConflict),
            Err(error) => Err(CoreError::Storage(error)),
        }
    }

    pub fn dispatch_workflow(
        &mut self,
        command: WorkflowDispatchCommand,
    ) -> Result<WorkflowDispatchResult, CoreError> {
        if command.task.trim().is_empty() {
            return Err(CoreError::HandoffTaskMissing);
        }
        if command.source_message_id.trim().is_empty() {
            return Err(CoreError::HandoffSourceMessageMissing);
        }
        let (workflow_project_id, workflow) = self
            .storage
            .load_workflow(&command.workflow_id)?
            .ok_or(CoreError::WorkflowNotFound)?;
        let initial_parent = self
            .state
            .run(&command.parent_execution_run_id)
            .cloned()
            .or(self
                .storage
                .load_execution_run(&command.parent_execution_run_id)?)
            .ok_or(CoreError::HandoffExecutionNotFound)?;
        if initial_parent.project_id != workflow_project_id
            || initial_parent.collaboration_run_id != command.collaboration_run_id
        {
            return Err(CoreError::WorkflowProjectMismatch);
        }
        if workflow.steps.is_empty() {
            return Err(CoreError::WorkflowEmpty);
        }
        let mode = match workflow.kind.as_str() {
            "sequential" | "linear" | "reviewer" => "sequential",
            "parallel" => "parallel",
            other => return Err(CoreError::WorkflowKindInvalid(other.to_owned())),
        };
        let mut steps = workflow.steps.clone();
        steps.sort_by_key(|step| (step.order, step.id.clone()));
        let batch_id = format!(
            "workflow-batch-{}-{}",
            command.workflow_id, command.collaboration_run_id
        );
        let frozen_parent_id = initial_parent.id.clone();
        let mut parent_id = frozen_parent_id.clone();
        let mut dispatches = Vec::with_capacity(steps.len());

        for (index, step) in steps.into_iter().enumerate() {
            let parent = self
                .state
                .run(&parent_id)
                .cloned()
                .or(self.storage.load_execution_run(&parent_id)?)
                .ok_or(CoreError::HandoffExecutionNotFound)?;
            let handoff_id = format!("workflow-handoff-{}-{}", command.workflow_id, step.id);
            let step_task = match step.prompt_supplement.as_deref() {
                Some(supplement) if !supplement.trim().is_empty() => {
                    format!(
                        "{}\n\nWorkflow step requirement: {}",
                        command.task, supplement
                    )
                }
                _ => command.task.clone(),
            };
            let handoff = Handoff {
                id: handoff_id.clone(),
                collaboration_run_id: command.collaboration_run_id.clone(),
                from_execution_run_id: parent.id.clone(),
                to_agent_id: step.agent_id.clone(),
                status: "proposed".into(),
                details: Some(StructuredHandoffDetails {
                    parent_execution_run_id: Some(parent.id.clone()),
                    child_execution_run_id: None,
                    source_message_id: Some(command.source_message_id.clone()),
                    from_agent_id: Some(parent.agent_id.clone()),
                    to_agent_id: Some(step.agent_id.clone()),
                    kind: Some(
                        if workflow.kind == "reviewer" && index + 1 == workflow.steps.len() {
                            "review_feedback".into()
                        } else {
                            "task".into()
                        },
                    ),
                    dispatch_mode: Some(mode.into()),
                    batch_id: Some(batch_id.clone()),
                    sequence_index: Some(index as u64),
                    detected_by: Some("ui_explicit".into()),
                    task: Some(step_task),
                    reason: Some(format!("workflow:{}", workflow.id)),
                    decisions: None,
                    constraints: None,
                    artifacts: None,
                    expected_output: None,
                    context_scope: Some(format!("workflow:{}:step:{}", workflow.id, step.id)),
                    agent_path: None,
                }),
            };
            validate_handoff_shape(&handoff)?;
            if let Some(existing) = self.storage.load_handoff(&handoff_id)? {
                let existing_task = existing
                    .details
                    .as_ref()
                    .and_then(|details| details.task.as_deref());
                let requested_task = handoff
                    .details
                    .as_ref()
                    .and_then(|details| details.task.as_deref());
                if existing.collaboration_run_id != handoff.collaboration_run_id
                    || existing.from_execution_run_id != handoff.from_execution_run_id
                    || existing.to_agent_id != handoff.to_agent_id
                    || existing_task != requested_task
                {
                    return Err(CoreError::HandoffConflict);
                }
            } else {
                self.create_handoff(CreateHandoffCommand { handoff })?;
            }
            let persisted = self
                .storage
                .load_handoff(&handoff_id)?
                .ok_or(CoreError::HandoffNotFound)?;
            if persisted.status == "proposed" {
                self.transition_handoff(&handoff_id, "approved")?;
            }
            let persisted = self
                .storage
                .load_handoff(&handoff_id)?
                .ok_or(CoreError::HandoffNotFound)?;
            let dispatch = if persisted.status == "approved" {
                self.dispatch_handoff_with_runtime(&handoff_id, command.start_runtime)?
            } else if persisted.status == "dispatched" {
                let child_id = persisted
                    .details
                    .as_ref()
                    .and_then(|details| details.child_execution_run_id.as_deref())
                    .ok_or(CoreError::HandoffInvalidTransition)?;
                let child = self
                    .state
                    .run(child_id)
                    .cloned()
                    .or(self.storage.load_execution_run(child_id)?)
                    .ok_or(CoreError::RunNotFound)?;
                HandoffDispatchResult {
                    child_run: child,
                    created: false,
                    event_sequence: self.event_cursor(),
                    handoff_status: persisted.status,
                    runtime_started: false,
                    runtime_dispatch: "deferred".into(),
                }
            } else {
                let child_id = persisted
                    .details
                    .as_ref()
                    .and_then(|details| details.child_execution_run_id.as_deref())
                    .ok_or(CoreError::HandoffInvalidTransition)?;
                let child = self
                    .state
                    .run(child_id)
                    .cloned()
                    .or(self.storage.load_execution_run(child_id)?)
                    .ok_or(CoreError::RunNotFound)?;
                HandoffDispatchResult {
                    child_run: child,
                    created: false,
                    event_sequence: self.event_cursor(),
                    handoff_status: persisted.status,
                    runtime_started: false,
                    runtime_dispatch: "already-terminal".into(),
                }
            };
            let child_status = format!("{:?}", dispatch.child_run.status).to_lowercase();
            let child_id = dispatch.child_run.id.clone();
            dispatches.push(WorkflowDispatchStep {
                step_id: step.id,
                order: step.order,
                agent_id: step.agent_id,
                handoff_id,
                child_execution_run_id: Some(child_id.clone()),
                handoff_status: dispatch.handoff_status,
                child_status: Some(child_status.clone()),
                runtime_started: dispatch.runtime_started,
                runtime_dispatch: dispatch.runtime_dispatch,
            });
            if mode == "sequential" {
                if child_status != "completed" {
                    break;
                }
                parent_id = child_id;
            }
        }
        let failed = dispatches.iter().any(|step| {
            matches!(
                step.child_status.as_deref(),
                Some("failed" | "cancelled" | "interrupted")
            )
        });
        Ok(WorkflowDispatchResult {
            workflow_id: command.workflow_id,
            collaboration_run_id: command.collaboration_run_id,
            mode: mode.into(),
            completed: !failed && dispatches.len() == workflow.steps.len(),
            failed,
            steps: dispatches,
        })
    }

    pub fn create_collaboration(
        &mut self,
        command: CreateCollaborationCommand,
    ) -> Result<CollaborationWriteOutcome, CoreError> {
        match self
            .storage
            .create_collaboration_run(&command.project_id, &command.collaboration)
        {
            Ok(true) => Ok(CollaborationWriteOutcome::Created),
            Ok(false) => Ok(CollaborationWriteOutcome::AlreadyPresent),
            Err(StorageError::CollaborationProjectNotFound { .. }) => {
                Err(CoreError::CollaborationProjectNotFound)
            }
            Err(StorageError::CollaborationAgentNotInProject { .. }) => {
                Err(CoreError::CollaborationAgentNotInProject)
            }
            Err(StorageError::CollaborationConflict { .. }) => {
                Err(CoreError::CollaborationConflict)
            }
            Err(error) => Err(CoreError::Storage(error)),
        }
    }

    pub fn create_handoff(
        &mut self,
        command: CreateHandoffCommand,
    ) -> Result<HandoffWriteOutcome, CoreError> {
        validate_handoff_shape(&command.handoff)?;
        match self.storage.create_handoff(&command.handoff) {
            Ok(true) => {
                if self.collaboration_auto_dispatches(&command.handoff.collaboration_run_id)? {
                    self.transition_handoff(&command.handoff.id, "approved")?;
                    self.dispatch_handoff_with_runtime(&command.handoff.id, true)?;
                }
                Ok(HandoffWriteOutcome::Created)
            }
            Ok(false) => Ok(HandoffWriteOutcome::AlreadyPresent),
            Err(StorageError::HandoffCollaborationNotFound { .. }) => {
                Err(CoreError::HandoffCollaborationNotFound)
            }
            Err(StorageError::HandoffExecutionNotFound { .. }) => {
                Err(CoreError::HandoffExecutionNotFound)
            }
            Err(StorageError::HandoffAgentNotInProject { .. }) => {
                Err(CoreError::HandoffAgentNotInProject)
            }
            Err(StorageError::HandoffConflict { .. }) => Err(CoreError::HandoffConflict),
            Err(error) => Err(CoreError::Storage(error)),
        }
    }

    pub fn dispatch_handoff(
        &mut self,
        handoff_id: &str,
    ) -> Result<HandoffDispatchResult, CoreError> {
        self.dispatch_handoff_with_runtime(handoff_id, false)
    }

    pub fn dispatch_handoff_with_runtime(
        &mut self,
        handoff_id: &str,
        start_runtime: bool,
    ) -> Result<HandoffDispatchResult, CoreError> {
        let handoff = self
            .storage
            .load_handoff(handoff_id)?
            .ok_or(CoreError::HandoffNotFound)?;

        if handoff.status == "dispatched" {
            let child_id = handoff
                .details
                .as_ref()
                .and_then(|details| details.child_execution_run_id.as_deref())
                .ok_or(CoreError::HandoffInvalidTransition)?;
            let child_run = self
                .state
                .run(child_id)
                .cloned()
                .or(self.storage.load_execution_run(child_id)?)
                .ok_or(CoreError::RunNotFound)?;
            let should_start_runtime = start_runtime && !child_run.status.is_terminal();
            let child_run = if should_start_runtime {
                let task = required_handoff_task(handoff.details.as_ref())?;
                self.run_handoff_child_runtime(&handoff, &child_run, task)?
            } else {
                child_run
            };
            let mut handoff_status = handoff.status.clone();
            if let Some(target_status) = handoff_status_for_child(&child_run) {
                self.transition_handoff(handoff_id, target_status)?;
                handoff_status = target_status.into();
            }
            let runtime_dispatch = if should_start_runtime {
                runtime_dispatch_for_child(&child_run).into()
            } else {
                "deferred".into()
            };
            return Ok(HandoffDispatchResult {
                child_run,
                created: false,
                event_sequence: self.event_cursor(),
                handoff_status,
                runtime_started: should_start_runtime,
                runtime_dispatch,
            });
        }
        if handoff.status != "approved" {
            return Err(CoreError::HandoffInvalidTransition);
        }
        let handoff_task = handoff
            .details
            .as_ref()
            .and_then(|details| details.task.as_deref())
            .filter(|task| !task.trim().is_empty())
            .map(str::to_owned);
        if start_runtime && handoff_task.is_none() {
            return Err(CoreError::HandoffTaskMissing);
        }

        self.validate_handoff_dispatch_policy(&handoff)?;

        let parent = self
            .state
            .run(&handoff.from_execution_run_id)
            .cloned()
            .or(self
                .storage
                .load_execution_run(&handoff.from_execution_run_id)?)
            .ok_or(CoreError::HandoffExecutionNotFound)?;
        let child_run_id = format!("handoff-child-{handoff_id}");
        let target_access = self
            .state
            .project_assignments
            .get(&(parent.project_id.clone(), handoff.to_agent_id.clone()))
            .map(|granted| downgrade_workspace_access(granted, &parent.scope.workspace_access))
            .ok_or(CoreError::AgentNotAssigned)?;
        let child_run = self.state.build_pending_execution(ExecutionStart {
            run_id: child_run_id,
            collaboration_run_id: handoff.collaboration_run_id.clone(),
            project_id: parent.project_id.clone(),
            conversation_id: parent.conversation_id.clone(),
            agent_id: handoff.to_agent_id.clone(),
            workspace_access: target_access,
            canonical_cwd: parent.scope.canonical_cwd.clone(),
        })?;
        let event = RuntimeEvent {
            event_id: format!("handoff-child-created-{}", child_run.id),
            execution_run_id: child_run.id.clone(),
            runtime_id: "core".into(),
            thread_id: None,
            turn_id: None,
            sequence: 0,
            event_type: "execution.created".into(),
            timestamp_ms: 0,
            payload: json!({
                "status": "pending",
                "handoffId": handoff_id,
                "runtimeStart": "deferred",
            }),
        };
        let existing_snapshot = self.model_snapshots.get(&child_run.id).cloned().or(self
            .storage
            .load_model_snapshot(
            &child_run.id,
        )?);
        let existing_selection = self
            .model_selection_snapshots
            .get(&child_run.id)
            .cloned()
            .or(self.storage.load_model_selection_snapshot(&child_run.id)?);
        let resolved = match (existing_snapshot.as_ref(), existing_selection) {
            (Some(snapshot), Some(selection)) => {
                if snapshot.connector_id.as_deref() != Some(selection.connector_id.as_str())
                    || snapshot.model_id != selection.effective_model_id
                {
                    return Err(CoreError::ModelSelectionSnapshotConflict);
                }
                self.resolved_frozen_model_selection(selection, None)?
            }
            (Some(snapshot), None) => self
                .resolved_frozen_model_selection(self.legacy_selection_snapshot(snapshot)?, None)?,
            (None, Some(_)) => return Err(CoreError::ModelSnapshotMissing),
            (None, None) => self.resolve_current_model_selection(&child_run, None, None)?,
        };
        if resolved.binding.validate_profile {
            self.validate_connector_binding(&resolved.binding)?;
        }
        let child_snapshot = match existing_snapshot {
            Some(snapshot) => snapshot,
            None => self.model_snapshot_for(&child_run, Some(&resolved.binding), None)?,
        };
        let mut initial_context = handoff_task
            .as_deref()
            .map(|task| self.assemble_context(&child_run, task))
            .transpose()?;
        if let Some(context) = initial_context.as_mut() {
            apply_frozen_context_manifest_route(
                context,
                &child_run,
                &child_snapshot,
                &resolved.snapshot,
                Some(&resolved.binding),
            )?;
        }
        let mut initial_events = vec![event];
        if let Some(context) = initial_context.as_ref() {
            initial_events.extend(handoff_context_initial_events(
                &child_run,
                &child_snapshot,
                &resolved.snapshot,
                context,
            ));
        }
        let (created, event_sequence) = match initial_context.as_ref() {
            Some(context) => {
                let (bundle_hash, source_ledger_json) = context_manifest_storage_values(context);
                self.storage
                    .dispatch_handoff_and_persist_child_with_selection_context_and_events(
                        handoff_id,
                        &child_run,
                        &child_snapshot,
                        &resolved.snapshot,
                        &context.manifest,
                        &bundle_hash,
                        &source_ledger_json,
                        &initial_events,
                    )?
            }
            None => self
                .storage
                .dispatch_handoff_and_persist_child_with_selection(
                    handoff_id,
                    &child_run,
                    &child_snapshot,
                    &resolved.snapshot,
                    initial_events
                        .first()
                        .expect("handoff child always has execution.created"),
                )?,
        };
        self.model_snapshots
            .insert(child_run.id.clone(), child_snapshot.clone());
        self.model_selection_snapshots
            .insert(child_run.id.clone(), resolved.snapshot);
        self.execution_bindings
            .insert(child_run.id.clone(), resolved.binding);
        if created {
            self.state.restore_run(child_run.clone());
            for event in initial_events {
                self.state.restore_event(event)?;
            }
            self.persisted_event_cursor = event_sequence;
        } else {
            let child_id = handoff
                .details
                .as_ref()
                .and_then(|details| details.child_execution_run_id.as_deref())
                .unwrap_or(&child_run.id);
            if child_id != child_run.id {
                return Err(CoreError::HandoffConflict);
            }
        }
        if let Some(context) = initial_context {
            self.contexts.insert(child_run.id.clone(), context);
        }
        let should_start_runtime = start_runtime;
        let child_run = if should_start_runtime {
            self.run_handoff_child_runtime(
                &handoff,
                &child_run,
                handoff_task
                    .as_deref()
                    .expect("validated handoff runtime task must be present"),
            )?
        } else {
            child_run
        };
        let mut handoff_status = "dispatched".to_owned();
        if let Some(target_status) = handoff_status_for_child(&child_run) {
            self.transition_handoff(handoff_id, target_status)?;
            handoff_status = target_status.into();
        }
        let runtime_dispatch = if should_start_runtime {
            runtime_dispatch_for_child(&child_run).into()
        } else {
            "deferred".into()
        };
        Ok(HandoffDispatchResult {
            child_run,
            created,
            event_sequence: self.event_cursor().max(event_sequence),
            handoff_status,
            runtime_started: should_start_runtime,
            runtime_dispatch,
        })
    }

    fn run_handoff_child_runtime(
        &mut self,
        handoff: &Handoff,
        child_run: &ExecutionRun,
        task: &str,
    ) -> Result<ExecutionRun, CoreError> {
        if !self.contexts.contains_key(&child_run.id) {
            // Legacy/deferred children can predate the atomic startup boundary.
            // Assemble their Context only when they are actually started; new
            // start-runtime dispatches insert it before this method is called.
            let mut context = self.assemble_context(child_run, task)?;
            let frozen_selection = self
                .model_selection_snapshots
                .get(&child_run.id)
                .cloned()
                .or(self.storage.load_model_selection_snapshot(&child_run.id)?)
                .ok_or(CoreError::ModelSnapshotMissing)?;
            let frozen_snapshot = self
                .model_snapshots
                .get(&child_run.id)
                .cloned()
                .or(self.storage.load_model_snapshot(&child_run.id)?)
                .ok_or(CoreError::ModelSnapshotMissing)?;
            apply_frozen_context_manifest_route(
                &mut context,
                child_run,
                &frozen_snapshot,
                &frozen_selection,
                self.execution_bindings.get(&child_run.id),
            )?;
            self.persist_context(child_run, context)?;
        }
        self.drive_runtime(&child_run.id)?;
        let child_run = self
            .state
            .run(&child_run.id)
            .cloned()
            .ok_or(CoreError::RunNotFound)?;
        if let Some(target_status) = handoff_status_for_child(&child_run) {
            self.transition_handoff(&handoff.id, target_status)?;
        }
        Ok(child_run)
    }

    pub fn transition_handoff(
        &mut self,
        handoff_id: &str,
        target_status: &str,
    ) -> Result<HandoffTransitionOutcome, CoreError> {
        match self.storage.transition_handoff(handoff_id, target_status) {
            Ok(true) => Ok(HandoffTransitionOutcome::Changed),
            Ok(false) => Ok(HandoffTransitionOutcome::AlreadyAtTarget),
            Err(StorageError::HandoffNotFound { .. }) => Err(CoreError::HandoffNotFound),
            Err(StorageError::HandoffInvalidTransition { .. }) => {
                Err(CoreError::HandoffInvalidTransition)
            }
            Err(error) => Err(CoreError::Storage(error)),
        }
    }

    pub fn authorize_workspace(
        &mut self,
        project_id: &str,
        root_path: &str,
    ) -> Result<WorkspaceAuthorization, CoreError> {
        let root = std::fs::canonicalize(root_path)
            .map_err(|error| CoreError::InvalidWorkspaceRoot(error.to_string()))?;
        if !root.is_dir() {
            return Err(CoreError::InvalidWorkspaceRoot(
                "workspace root is not a directory".into(),
            ));
        }
        let authorization = WorkspaceAuthorization {
            project_id: project_id.into(),
            canonical_root: root.to_string_lossy().into_owned(),
            revision: 1,
            validation_status: "valid".into(),
        };
        self.storage.set_workspace_authorization(&authorization)?;
        self.state
            .restore_workspace_authorization(authorization.clone());
        Ok(authorization)
    }

    pub fn set_project_agent_assignment(
        &mut self,
        project_id: &str,
        agent_id: &str,
        enabled: bool,
        workspace_access: WorkspaceAccess,
    ) -> Result<(), CoreError> {
        self.storage.set_project_agent_assignment(
            project_id,
            agent_id,
            enabled,
            &workspace_access,
        )?;
        self.state.restore_project_roster_entry(
            project_id.to_owned(),
            agent_id.to_owned(),
            enabled,
            workspace_access,
        );
        Ok(())
    }

    pub fn set_agent_model_binding(
        &mut self,
        agent_id: &str,
        connector_id: Option<String>,
        model_id: Option<String>,
        candidate_model_list_revision: u64,
    ) -> Result<(), CoreError> {
        self.storage.set_agent_model_binding(
            agent_id,
            &AgentModelBinding {
                connector_id,
                model_id,
                candidate_model_list_revision,
            },
        )?;
        Ok(())
    }

    /// Applies a field-presence-aware binding change. This is intentionally
    /// separate from the legacy replace-style setter so existing IPC callers
    /// remain wire compatible while new clients can preserve, clear, or set
    /// each field without a silent default Runtime fallback.
    pub fn patch_agent_model_binding(
        &mut self,
        agent_id: &str,
        patch: &AgentModelBindingPatch,
    ) -> Result<AgentModelBinding, CoreError> {
        Ok(self.storage.patch_agent_model_binding(agent_id, patch)?)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn set_project_agent_assignment_with_model_selection(
        &mut self,
        project_id: &str,
        agent_id: &str,
        enabled: bool,
        workspace_access: WorkspaceAccess,
        selection: ModelSelection,
        candidate_model_list_mode: IdentityModelListMode,
        candidate_model_list_revision: u64,
    ) -> Result<(), CoreError> {
        self.storage
            .set_project_agent_assignment_with_model_selection(
                project_id,
                agent_id,
                enabled,
                &workspace_access,
                &selection,
                candidate_model_list_mode,
                candidate_model_list_revision,
            )?;
        self.state.restore_project_roster_entry(
            project_id.to_owned(),
            agent_id.to_owned(),
            enabled,
            workspace_access,
        );
        Ok(())
    }

    pub fn remove_project_agent_assignment(
        &mut self,
        project_id: &str,
        agent_id: &str,
    ) -> Result<(), CoreError> {
        if !self
            .storage
            .remove_project_agent_assignment(project_id, agent_id)?
        {
            return Err(CoreError::ProjectionEntityNotFound);
        }
        self.state.remove_project_assignment(project_id, agent_id);
        Ok(())
    }

    pub fn set_conversation_agent_assignment(
        &mut self,
        conversation_id: &str,
        agent_id: &str,
        enabled: bool,
    ) -> Result<(), CoreError> {
        if !self
            .storage
            .conversation_agent_has_project_assignment(conversation_id, agent_id)?
        {
            return Err(CoreError::AgentNotAssigned);
        }
        self.storage
            .set_conversation_agent_assignment(conversation_id, agent_id, enabled)?;
        self.state.restore_conversation_assignment(
            conversation_id.to_owned(),
            agent_id.to_owned(),
            enabled,
        );
        Ok(())
    }

    pub fn set_conversation_agent_assignment_with_model_selection(
        &mut self,
        conversation_id: &str,
        agent_id: &str,
        enabled: bool,
        selection: ModelSelection,
        candidate_model_list_mode: IdentityModelListMode,
        candidate_model_list_revision: u64,
    ) -> Result<(), CoreError> {
        if !self
            .storage
            .conversation_agent_has_project_assignment(conversation_id, agent_id)?
        {
            return Err(CoreError::AgentNotAssigned);
        }
        self.storage
            .set_conversation_agent_assignment_with_model_selection(
                conversation_id,
                agent_id,
                enabled,
                &selection,
                candidate_model_list_mode,
                candidate_model_list_revision,
            )?;
        self.state.restore_conversation_assignment(
            conversation_id.to_owned(),
            agent_id.to_owned(),
            enabled,
        );
        Ok(())
    }

    pub fn upsert_identity_model_option(
        &mut self,
        option: &IdentityModelOption,
    ) -> Result<(), CoreError> {
        self.storage.upsert_identity_model_option(option)?;
        Ok(())
    }

    pub fn set_identity_model_option_default(
        &mut self,
        target: &IdentityModelListTarget,
        connector_id: &str,
        model_id: &str,
    ) -> Result<(), CoreError> {
        self.storage
            .set_identity_model_option_default(target, connector_id, model_id)?;
        Ok(())
    }

    pub fn identity_model_options(
        &self,
        target: &IdentityModelListTarget,
        connector_id: Option<&str>,
    ) -> Result<Vec<IdentityModelOption>, CoreError> {
        Ok(self
            .storage
            .query_identity_model_options(target, connector_id)?)
    }

    pub fn remove_conversation_agent_assignment(
        &mut self,
        conversation_id: &str,
        agent_id: &str,
    ) -> Result<(), CoreError> {
        if !self
            .storage
            .remove_conversation_agent_assignment(conversation_id, agent_id)?
        {
            return Err(CoreError::ProjectionEntityNotFound);
        }
        self.state
            .remove_conversation_assignment(conversation_id, agent_id);
        Ok(())
    }

    fn persist_run_and_events(&mut self, run: &ExecutionRun) -> Result<(), CoreError> {
        let events = self.state.replay_events(self.persisted_event_cursor);
        self.persisted_event_cursor = self
            .storage
            .persist_execution_run_and_events(run, &events)?;
        Ok(())
    }

    fn persist_run_and_model_snapshots_and_events(
        &mut self,
        run: &ExecutionRun,
        snapshot: &ModelSnapshot,
        selection_snapshot: &ModelSelectionSnapshot,
    ) -> Result<(), CoreError> {
        let events = self.state.replay_events(self.persisted_event_cursor);
        self.persisted_event_cursor = self
            .storage
            .persist_execution_run_and_model_snapshots_and_events(
                run,
                snapshot,
                selection_snapshot,
                &events,
            )?;
        Ok(())
    }

    fn persist_receipt_run_and_model_snapshots_and_events(
        &mut self,
        receipt: &CommandReceipt,
        run: &ExecutionRun,
        snapshot: &ModelSnapshot,
        selection_snapshot: &ModelSelectionSnapshot,
    ) -> Result<(), CoreError> {
        let events = self.state.replay_events(self.persisted_event_cursor);
        self.persisted_event_cursor = self
            .storage
            .persist_command_receipt_and_execution_run_and_model_snapshots_and_events(
                receipt,
                run,
                snapshot,
                selection_snapshot,
                &events,
            )?;
        Ok(())
    }

    /// Commits every durable piece of an initial Run together. In the context
    /// case this deliberately performs no prior Run/snapshot write: a crash
    /// cannot leave a recoverable Run without its frozen Context Manifest or
    /// vice versa.
    fn persist_initial_run_boundary(
        &mut self,
        receipt: Option<&CommandReceipt>,
        run: &ExecutionRun,
        snapshot: &ModelSnapshot,
        selection_snapshot: &ModelSelectionSnapshot,
        context: Option<&AssembledContext>,
    ) -> Result<(), CoreError> {
        let Some(context) = context else {
            return match receipt {
                Some(receipt) => self.persist_receipt_run_and_model_snapshots_and_events(
                    receipt,
                    run,
                    snapshot,
                    selection_snapshot,
                ),
                None => self.persist_run_and_model_snapshots_and_events(
                    run,
                    snapshot,
                    selection_snapshot,
                ),
            };
        };
        if context.manifest.execution_run_id != run.id {
            return Err(CoreError::Context(
                agenttalk_context::ContextError::ManifestRunMismatch,
            ));
        }
        let (bundle_hash, source_ledger_json) = context_manifest_storage_values(context);
        let events = self.state.replay_events(self.persisted_event_cursor);
        self.persisted_event_cursor = match receipt {
            Some(receipt) => self
                .storage
                .persist_command_receipt_and_execution_run_and_model_snapshots_context_manifest_and_events(
                    receipt,
                    run,
                    snapshot,
                    selection_snapshot,
                    &context.manifest,
                    &bundle_hash,
                    &source_ledger_json,
                    &events,
                )?,
            None => self
                .storage
                .persist_execution_run_and_model_snapshots_context_manifest_and_events(
                    run,
                    snapshot,
                    selection_snapshot,
                    &context.manifest,
                    &bundle_hash,
                    &source_ledger_json,
                    &events,
                )?,
        };
        Ok(())
    }
}

fn validate_handoff_shape(handoff: &Handoff) -> Result<(), CoreError> {
    let Some(details) = handoff.details.as_ref() else {
        return Err(CoreError::HandoffStructuredDetailsMissing);
    };
    let has_text = |value: Option<&String>| value.is_some_and(|value| !value.trim().is_empty());
    if !has_text(details.source_message_id.as_ref())
        || !has_text(details.parent_execution_run_id.as_ref())
        || !has_text(details.from_agent_id.as_ref())
        || !has_text(details.to_agent_id.as_ref())
    {
        return Err(CoreError::HandoffStructuredDetailsMissing);
    }
    if !matches!(
        details.kind.as_deref(),
        Some("task" | "review_feedback" | "revision_request")
    ) || !matches!(
        details.dispatch_mode.as_deref(),
        Some("sequential" | "parallel")
    ) || !matches!(
        details.detected_by.as_deref(),
        Some("ui_explicit" | "structured_output" | "legacy_mention")
    ) {
        return Err(CoreError::HandoffStructuredDetailsMissing);
    }
    if details
        .parent_execution_run_id
        .as_deref()
        .is_some_and(|value| value != handoff.from_execution_run_id)
        || details
            .to_agent_id
            .as_deref()
            .is_some_and(|value| value != handoff.to_agent_id)
    {
        return Err(CoreError::HandoffStructuredDetailsMissing);
    }
    Ok(())
}

impl PersistentCore {
    fn collaboration_auto_dispatches(&self, collaboration_id: &str) -> Result<bool, CoreError> {
        let projection = self.storage.projection_snapshot()?;
        Ok(projection
            .get("collaborationRuns")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .find(|run| run.get("id").and_then(serde_json::Value::as_str) == Some(collaboration_id))
            .and_then(|run| run.get("autoDispatchHandoffs"))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false))
    }

    fn validate_handoff_dispatch_policy(&self, handoff: &Handoff) -> Result<(), CoreError> {
        let projection = self.storage.projection_snapshot()?;
        let runs = projection
            .get("runs")
            .and_then(serde_json::Value::as_array)
            .ok_or(CoreError::HandoffSourceMessageMissing)?;
        let parent = runs
            .iter()
            .find(|run| {
                run.get("id").and_then(serde_json::Value::as_str)
                    == Some(handoff.from_execution_run_id.as_str())
            })
            .ok_or(CoreError::HandoffExecutionNotFound)?;
        let conversation_id = parent
            .get("conversationId")
            .and_then(serde_json::Value::as_str)
            .ok_or(CoreError::HandoffSourceMessageMissing)?;
        let source_agent_id = parent
            .get("agentId")
            .and_then(serde_json::Value::as_str)
            .ok_or(CoreError::HandoffStructuredDetailsMissing)?;
        if handoff
            .details
            .as_ref()
            .and_then(|details| details.from_agent_id.as_deref())
            != Some(source_agent_id)
        {
            return Err(CoreError::HandoffStructuredDetailsMissing);
        }
        let source_message_id = handoff
            .details
            .as_ref()
            .and_then(|details| details.source_message_id.as_deref())
            .ok_or(CoreError::HandoffStructuredDetailsMissing)?;
        let source_message_exists = projection
            .get("messages")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .any(|message| {
                message.get("id").and_then(serde_json::Value::as_str) == Some(source_message_id)
                    && message
                        .get("conversationId")
                        .and_then(serde_json::Value::as_str)
                        == Some(conversation_id)
            });
        if !source_message_exists {
            return Err(CoreError::HandoffSourceMessageMissing);
        }

        let handoffs = self.storage.load_handoffs()?;
        let mut current_run = handoff.from_execution_run_id.clone();
        let mut visited_runs = HashSet::new();
        let mut agent_path = vec![parent
            .get("agentId")
            .and_then(serde_json::Value::as_str)
            .ok_or(CoreError::HandoffCycleDetected)?
            .to_owned()];
        let mut ancestor_depth = 0_u64;
        loop {
            if !visited_runs.insert(current_run.clone()) {
                return Err(CoreError::HandoffCycleDetected);
            }
            let Some(ancestor) = handoffs.iter().find(|candidate| {
                candidate
                    .details
                    .as_ref()
                    .and_then(|details| details.child_execution_run_id.as_deref())
                    == Some(current_run.as_str())
                    && candidate.collaboration_run_id == handoff.collaboration_run_id
            }) else {
                break;
            };
            ancestor_depth = ancestor_depth.saturating_add(1);
            agent_path.push(
                ancestor
                    .details
                    .as_ref()
                    .and_then(|details| details.from_agent_id.clone())
                    .ok_or(CoreError::HandoffCycleDetected)?,
            );
            current_run = ancestor.from_execution_run_id.clone();
        }
        if agent_path.iter().any(|agent| agent == &handoff.to_agent_id) {
            return Err(CoreError::HandoffCycleDetected);
        }
        let max_depth = projection
            .get("collaborationRuns")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .find(|run| {
                run.get("id").and_then(serde_json::Value::as_str)
                    == Some(handoff.collaboration_run_id.as_str())
            })
            .and_then(|run| run.get("maxDepth"))
            .and_then(serde_json::Value::as_u64)
            .ok_or(CoreError::HandoffDepthLimit)?;
        if ancestor_depth.saturating_add(1) > max_depth {
            return Err(CoreError::HandoffDepthLimit);
        }
        Ok(())
    }
}

fn required_handoff_task(details: Option<&StructuredHandoffDetails>) -> Result<&str, CoreError> {
    let task = details
        .and_then(|details| details.task.as_deref())
        .filter(|task| !task.trim().is_empty())
        .ok_or(CoreError::HandoffTaskMissing)?;
    Ok(task)
}

fn handoff_status_for_child(child_run: &ExecutionRun) -> Option<&'static str> {
    match child_run.status {
        ExecutionStatus::Completed => Some("completed"),
        ExecutionStatus::Failed | ExecutionStatus::Interrupted => Some("failed"),
        ExecutionStatus::Cancelled => Some("cancelled"),
        _ => None,
    }
}

fn runtime_dispatch_for_child(child_run: &ExecutionRun) -> &'static str {
    match child_run.status {
        ExecutionStatus::Completed => "completed",
        ExecutionStatus::Failed | ExecutionStatus::Interrupted => "failed",
        ExecutionStatus::Cancelled => "cancelled",
        _ => "started",
    }
}

fn is_safe_handoff_rejection(error: &CoreError) -> bool {
    matches!(
        error,
        CoreError::AgentNotAssigned
            | CoreError::HandoffCollaborationNotFound
            | CoreError::HandoffExecutionNotFound
            | CoreError::HandoffAgentNotInProject
            | CoreError::HandoffConflict
            | CoreError::HandoffNotFound
            | CoreError::HandoffTaskMissing
            | CoreError::HandoffStructuredDetailsMissing
            | CoreError::HandoffSourceMessageMissing
            | CoreError::HandoffCycleDetected
            | CoreError::HandoffDepthLimit
            | CoreError::HandoffInvalidTransition
            | CoreError::WorkspaceAccessDenied
    )
}

#[derive(Default)]
pub struct CoreState {
    assigned_agents: HashSet<String>,
    project_roster: HashSet<(String, String)>,
    project_assignments: HashMap<(String, String), WorkspaceAccess>,
    conversation_assignments: HashMap<String, HashSet<String>>,
    workspace_authorizations: HashMap<String, String>,
    runs: HashMap<String, ExecutionRun>,
    events: InMemoryEventStore,
}

#[derive(Clone, Debug)]
pub struct ExecutionStart {
    pub run_id: String,
    pub collaboration_run_id: String,
    pub project_id: String,
    pub conversation_id: String,
    pub agent_id: String,
    pub workspace_access: WorkspaceAccess,
    pub canonical_cwd: Option<String>,
}

impl CoreState {
    pub fn assign_agent(&mut self, agent_id: impl Into<String>) {
        self.assigned_agents.insert(agent_id.into());
    }

    fn restore_project_assignment(
        &mut self,
        project_id: String,
        agent_id: String,
        access: WorkspaceAccess,
    ) {
        self.project_roster
            .insert((project_id.clone(), agent_id.clone()));
        self.project_assignments
            .insert((project_id, agent_id), access);
    }

    fn restore_project_roster_entry(
        &mut self,
        project_id: String,
        agent_id: String,
        enabled: bool,
        access: WorkspaceAccess,
    ) {
        let key = (project_id, agent_id);
        self.project_roster.insert(key.clone());
        if enabled {
            self.restore_project_assignment(key.0, key.1, access);
        } else {
            self.project_assignments.remove(&key);
        }
    }

    fn restore_conversation_assignment(
        &mut self,
        conversation_id: String,
        agent_id: String,
        enabled: bool,
    ) {
        if enabled {
            self.conversation_assignments
                .entry(conversation_id)
                .or_default()
                .insert(agent_id);
        } else if let Some(agents) = self.conversation_assignments.get_mut(&conversation_id) {
            agents.remove(&agent_id);
            if agents.is_empty() {
                self.conversation_assignments.remove(&conversation_id);
            }
        }
    }

    fn restore_workspace_authorization(
        &mut self,
        authorization: agenttalk_domain::WorkspaceAuthorization,
    ) {
        self.workspace_authorizations
            .insert(authorization.project_id, authorization.canonical_root);
    }

    fn validate_workspace_request(&self, input: &ExecutionStart) -> Result<(), CoreError> {
        let Some(requested_cwd) = input.canonical_cwd.as_deref() else {
            return Ok(());
        };
        let root = self
            .workspace_authorizations
            .get(&input.project_id)
            .ok_or(CoreError::WorkspaceAuthorizationMissing)?;
        let root =
            std::fs::canonicalize(root).map_err(|_| CoreError::WorkspaceAuthorizationMissing)?;
        let requested =
            std::fs::canonicalize(requested_cwd).map_err(|_| CoreError::WorkspacePathDenied)?;
        if requested.starts_with(root) {
            Ok(())
        } else {
            Err(CoreError::WorkspacePathDenied)
        }
    }

    fn remove_project_assignment(&mut self, project_id: &str, agent_id: &str) {
        let key = (project_id.to_owned(), agent_id.to_owned());
        self.project_roster.remove(&key);
        self.project_assignments.remove(&key);
    }

    fn remove_conversation_assignment(&mut self, conversation_id: &str, agent_id: &str) {
        if let Some(agents) = self.conversation_assignments.get_mut(conversation_id) {
            agents.remove(agent_id);
            if agents.is_empty() {
                self.conversation_assignments.remove(conversation_id);
            }
        }
    }

    pub fn start_execution(&mut self, input: ExecutionStart) -> Result<&ExecutionRun, CoreError> {
        let run = self.build_pending_execution(input)?;
        self.insert_pending_execution(run)
    }

    fn insert_pending_execution(&mut self, run: ExecutionRun) -> Result<&ExecutionRun, CoreError> {
        let run_id = run.id.clone();
        self.runs.insert(run_id.clone(), run);
        self.emit(&run_id, "execution.created", json!({"status":"pending"}))?;
        Ok(self.runs.get(&run_id).expect("inserted run"))
    }

    fn build_pending_execution(&self, input: ExecutionStart) -> Result<ExecutionRun, CoreError> {
        if self.runs.contains_key(&input.run_id) {
            return Err(CoreError::RunAlreadyExists);
        }
        let agent_id = input.agent_id;
        let project_key = (input.project_id.clone(), agent_id.clone());
        let project_has_roster = self
            .project_roster
            .iter()
            .any(|(project_id, _)| project_id == &input.project_id);
        let project_assignment = self.project_assignments.get(&project_key);
        let conversation_roster = self
            .conversation_assignments
            .get(&input.conversation_id)
            .filter(|agents| !agents.is_empty());
        let workspace_access = if let Some(conversation_roster) = conversation_roster {
            if !conversation_roster.contains(&agent_id) {
                return Err(CoreError::AgentNotAssigned);
            }
            let granted = project_assignment.ok_or(CoreError::AgentNotAssigned)?;
            if workspace_access_allows(granted, &input.workspace_access) {
                input.workspace_access
            } else {
                return Err(CoreError::WorkspaceAccessDenied);
            }
        } else if let Some(granted) = project_assignment {
            if workspace_access_allows(granted, &input.workspace_access) {
                input.workspace_access
            } else {
                return Err(CoreError::WorkspaceAccessDenied);
            }
        } else if self.assigned_agents.contains(&agent_id) && !project_has_roster {
            input.workspace_access
        } else {
            return Err(CoreError::AgentNotAssigned);
        };
        let project_id = input.project_id;
        let conversation_id = input.conversation_id;
        Ok(ExecutionRun {
            id: input.run_id,
            collaboration_run_id: input.collaboration_run_id,
            project_id: project_id.clone(),
            conversation_id: conversation_id.clone(),
            agent_id: agent_id.clone(),
            status: ExecutionStatus::Pending,
            version: 0,
            scope: ScopeSnapshot {
                project_id,
                conversation_id,
                agent_id,
                workspace_access,
                canonical_cwd: input.canonical_cwd,
            },
            terminal_reason: None,
        })
    }

    pub fn transition(
        &mut self,
        run_id: &str,
        next: ExecutionStatus,
        expected_version: u64,
        reason: Option<String>,
    ) -> Result<TransitionOutcome, CoreError> {
        let run = self.runs.get_mut(run_id).ok_or(CoreError::RunNotFound)?;
        let outcome = run.transition(next.clone(), expected_version, reason.clone())?;
        if outcome == TransitionOutcome::Applied {
            self.emit(
                run_id,
                "execution.status_changed",
                json!({"status": format!("{next:?}"), "reason": reason}),
            )?;
        }
        Ok(outcome)
    }

    pub fn retry(
        &mut self,
        new_run_id: impl Into<String>,
        source: &ExecutionRun,
    ) -> Result<&ExecutionRun, CoreError> {
        self.start_execution(ExecutionStart {
            run_id: new_run_id.into(),
            collaboration_run_id: source.collaboration_run_id.clone(),
            project_id: source.project_id.clone(),
            conversation_id: source.conversation_id.clone(),
            agent_id: source.agent_id.clone(),
            workspace_access: source.scope.workspace_access.clone(),
            canonical_cwd: source.scope.canonical_cwd.clone(),
        })
    }

    pub fn run(&self, id: &str) -> Option<&ExecutionRun> {
        self.runs.get(id)
    }

    fn restore_run(&mut self, run: ExecutionRun) {
        self.runs.insert(run.id.clone(), run);
    }

    fn restore_event(&mut self, event: RuntimeEvent) -> Result<(), CoreError> {
        self.events.append(event)?;
        Ok(())
    }

    fn append_runtime_event(&mut self, event: RuntimeEvent) -> Result<(), CoreError> {
        self.events.append(event)?;
        Ok(())
    }
    pub fn replay_events(&self, cursor: u64) -> Vec<RuntimeEvent> {
        self.events.replay_after(cursor)
    }

    fn emit(
        &mut self,
        run_id: &str,
        event_type: &str,
        payload: serde_json::Value,
    ) -> Result<(), CoreError> {
        self.events
            .append(RuntimeEvent {
                event_id: format!("{event_type}-{run_id}-{}", self.events.len() + 1),
                execution_run_id: run_id.into(),
                runtime_id: "core".into(),
                thread_id: None,
                turn_id: None,
                sequence: 0,
                event_type: event_type.into(),
                timestamp_ms: 0,
                payload,
            })
            .map(|_| ())
            .map_err(CoreError::from)
    }

    fn emit_projection_changed(&mut self, action: &str) -> Result<(), CoreError> {
        self.emit(
            "projection",
            "projection.changed",
            json!({"action": action}),
        )
    }
}

fn parse_workspace_access(value: &str) -> Result<WorkspaceAccess, CoreError> {
    match value {
        "none" => Ok(WorkspaceAccess::None),
        "read_only" => Ok(WorkspaceAccess::ReadOnly),
        "workspace_write" => Ok(WorkspaceAccess::WorkspaceWrite),
        other => Err(CoreError::InvalidWorkspaceAccess(other.into())),
    }
}

fn workspace_access_allows(granted: &WorkspaceAccess, requested: &WorkspaceAccess) -> bool {
    matches!(
        (granted, requested),
        (WorkspaceAccess::WorkspaceWrite, _)
            | (
                WorkspaceAccess::ReadOnly,
                WorkspaceAccess::None | WorkspaceAccess::ReadOnly
            )
            | (WorkspaceAccess::None, WorkspaceAccess::None)
    )
}

fn restart_frozen_routes(
    snapshots: &[ModelSnapshot],
    selection_snapshots: &[ModelSelectionSnapshot],
) -> HashMap<String, RestartFrozenRoute> {
    let selections = selection_snapshots
        .iter()
        .map(|snapshot| (snapshot.run_id.as_str(), snapshot))
        .collect::<HashMap<_, _>>();
    let mut routes = HashMap::new();
    for snapshot in snapshots {
        let selection = selections.get(snapshot.run_id.as_str()).copied();
        routes.insert(
            snapshot.run_id.clone(),
            RestartFrozenRoute {
                connector_id: snapshot
                    .connector_id
                    .clone()
                    .or_else(|| selection.map(|value| value.connector_id.clone())),
                runtime_type: selection.map(|value| value.runtime_type.clone()),
                model_id: snapshot
                    .model_id
                    .clone()
                    .or_else(|| selection.and_then(|value| value.effective_model_id.clone())),
                catalog_revision: snapshot.revision.map_or_else(
                    || {
                        selection
                            .and_then(|value| value.catalog_revision.clone())
                            .map_or(Value::Null, Value::String)
                    },
                    |revision| json!(revision),
                ),
            },
        );
    }
    for selection in selection_snapshots {
        routes
            .entry(selection.run_id.clone())
            .or_insert_with(|| RestartFrozenRoute {
                connector_id: Some(selection.connector_id.clone()),
                runtime_type: Some(selection.runtime_type.clone()),
                model_id: selection.effective_model_id.clone(),
                catalog_revision: selection
                    .catalog_revision
                    .clone()
                    .map_or(Value::Null, Value::String),
            });
    }
    routes
}

fn downgrade_workspace_access(
    granted: &WorkspaceAccess,
    requested: &WorkspaceAccess,
) -> WorkspaceAccess {
    match (granted, requested) {
        (WorkspaceAccess::None, _) => WorkspaceAccess::None,
        (WorkspaceAccess::ReadOnly, WorkspaceAccess::WorkspaceWrite) => WorkspaceAccess::ReadOnly,
        (_, requested) => requested.clone(),
    }
}

fn runtime_scope_digest(
    run: &ExecutionRun,
    context_manifest_id: &str,
    binding: Option<&ExecutionRuntimeBinding>,
) -> String {
    let canonical_scope = json!({
        "executionRunId": run.id,
        "projectId": run.project_id,
        "conversationId": run.conversation_id,
        "agentId": run.agent_id,
        "contextManifestId": context_manifest_id,
        "canonicalCwd": run.scope.canonical_cwd,
        "workspaceAccess": format!("{:?}", run.scope.workspace_access),
        "connectorId": binding.map(|value| value.connector_id.as_str()),
        "runtimeType": binding.and_then(|value| value.runtime_type.as_deref()),
        "modelId": binding.and_then(|value| value.model_id.as_deref()),
        "catalogRevision": binding.and_then(|value| value.catalog_revision),
    })
    .to_string();
    format!("local-sha256:{}", sha256_hex(&canonical_scope))
}

fn context_manifest_storage_values(context: &AssembledContext) -> (String, String) {
    let bundle_hash = sha256_hex(&context.bundle.rendered_context);
    let source_ledger_json = json!(context
        .source_ledger
        .iter()
        .map(|entry| json!({
            "sourceId": entry.source_id,
            "kind": entry.kind,
            "sha256": entry.sha256,
            "tokenCount": entry.token_count,
            "included": entry.included,
        }))
        .collect::<Vec<_>>())
    .to_string();
    (bundle_hash, source_ledger_json)
}

fn apply_frozen_context_manifest_route(
    context: &mut AssembledContext,
    run: &ExecutionRun,
    snapshot: &ModelSnapshot,
    selection: &ModelSelectionSnapshot,
    binding: Option<&ExecutionRuntimeBinding>,
) -> Result<(), CoreError> {
    if context.manifest.execution_run_id != run.id {
        return Err(CoreError::Context(
            agenttalk_context::ContextError::ManifestRunMismatch,
        ));
    }
    if snapshot.connector_id.as_deref() != Some(selection.connector_id.as_str())
        || snapshot.model_id != selection.effective_model_id
    {
        return Err(CoreError::ModelSelectionSnapshotConflict);
    }
    if binding.is_some_and(|binding| {
        binding.connector_id != selection.connector_id
            || binding.model_id != selection.effective_model_id
    }) {
        return Err(CoreError::ModelSelectionSnapshotConflict);
    }
    context.manifest.connector_id = Some(selection.connector_id.clone());
    context.manifest.model_id = selection.effective_model_id.clone();
    Ok(())
}

fn handoff_context_initial_events(
    run: &ExecutionRun,
    snapshot: &ModelSnapshot,
    selection: &ModelSelectionSnapshot,
    context: &AssembledContext,
) -> [RuntimeEvent; 3] {
    let bundle_hash = sha256_hex(&context.bundle.rendered_context);
    [
        RuntimeEvent {
            event_id: format!("scope.frozen-{}-handoff", run.id),
            execution_run_id: run.id.clone(),
            runtime_id: "core".into(),
            thread_id: None,
            turn_id: None,
            sequence: 0,
            event_type: "scope.frozen".into(),
            timestamp_ms: 0,
            payload: json!({
                "projectId": run.scope.project_id,
                "conversationId": run.scope.conversation_id,
                "agentId": run.scope.agent_id,
                "workspaceAccess": run.scope.workspace_access,
                "canonicalCwd": run.scope.canonical_cwd,
                "connectorId": selection.connector_id,
                "runtimeType": selection.runtime_type,
                "modelId": selection.effective_model_id,
                "catalogRevision": selection.catalog_revision,
            }),
        },
        RuntimeEvent {
            event_id: format!("context.assembled-{}-handoff", run.id),
            execution_run_id: run.id.clone(),
            runtime_id: "core".into(),
            thread_id: None,
            turn_id: None,
            sequence: 0,
            event_type: "context.assembled".into(),
            timestamp_ms: 0,
            payload: json!({
                "manifestId": context.manifest.id,
                "sourceCount": context.source_ledger.len(),
                "budget": 4096,
                "connectorId": snapshot.connector_id,
                "modelId": snapshot.model_id,
            }),
        },
        RuntimeEvent {
            event_id: format!("context.sealed-{}-handoff", run.id),
            execution_run_id: run.id.clone(),
            runtime_id: "core".into(),
            thread_id: None,
            turn_id: None,
            sequence: 0,
            event_type: "context.sealed".into(),
            timestamp_ms: 0,
            payload: json!({
                "manifestId": context.manifest.id,
                "bundleHash": bundle_hash,
                "metadataOnly": true,
                "connectorId": snapshot.connector_id,
                "modelId": snapshot.model_id,
            }),
        },
    ]
}

fn sha256_hex(value: &str) -> String {
    Sha256::digest(value.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

struct ConfigTransferError;

impl ConfigTransferError {
    fn message(message: impl Into<String>) -> CoreError {
        CoreError::ConfigTransferInvalid(message.into())
    }
}

fn ensure_allowed_keys(
    object: &serde_json::Map<String, Value>,
    allowed: &[&str],
    context: &str,
) -> Result<(), CoreError> {
    if let Some(key) = object.keys().find(|key| !allowed.contains(&key.as_str())) {
        return Err(ConfigTransferError::message(format!(
            "{context} contains unsupported field {key}"
        )));
    }
    Ok(())
}

fn required_object<'a>(
    value: Option<&'a Value>,
    context: &str,
) -> Result<&'a serde_json::Map<String, Value>, CoreError> {
    value
        .and_then(Value::as_object)
        .ok_or_else(|| ConfigTransferError::message(format!("{context} must be an object")))
}

fn required_text(value: Option<&Value>, context: &str) -> Result<String, CoreError> {
    let text = value
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
        .ok_or_else(|| {
            ConfigTransferError::message(format!("{context} must be a non-empty string"))
        })?;
    if text.len() > 4096 {
        return Err(ConfigTransferError::message(format!(
            "{context} is too long"
        )));
    }
    Ok(text.to_owned())
}

fn bounded_array<'a>(
    value: &'a Value,
    context: &str,
    max_items: usize,
) -> Result<&'a Vec<Value>, CoreError> {
    let array = value
        .as_array()
        .ok_or_else(|| ConfigTransferError::message(format!("{context} must be an array")))?;
    if array.len() > max_items {
        return Err(ConfigTransferError::message(format!(
            "{context} exceeds the maximum of {max_items} items"
        )));
    }
    Ok(array)
}

fn parse_config_workspace_access(value: &str) -> Result<WorkspaceAccess, CoreError> {
    match value {
        "none" => Ok(WorkspaceAccess::None),
        "read_only" => Ok(WorkspaceAccess::ReadOnly),
        "workspace_write" => Ok(WorkspaceAccess::WorkspaceWrite),
        _ => Err(ConfigTransferError::message(
            "projectAgents.workspaceAccess is invalid",
        )),
    }
}

fn unix_time_millis() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}

fn unix_time_nanos() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default()
}

fn runtime_payload_string(payload: &serde_json::Value, key: &str) -> Option<String> {
    payload
        .get(key)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .or_else(|| {
            payload
                .get("output")
                .and_then(serde_json::Value::as_object)
                .and_then(|output| output.get(key))
                .and_then(serde_json::Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .map(str::to_owned)
        })
}

fn runtime_payload_text(payload: &serde_json::Value) -> Option<String> {
    payload
        .get("delta")
        .and_then(serde_json::Value::as_str)
        .or_else(|| payload.get("content").and_then(serde_json::Value::as_str))
        .or_else(|| payload.get("output").and_then(serde_json::Value::as_str))
        .map(str::to_owned)
        .or_else(|| {
            payload
                .get("output")
                .and_then(serde_json::Value::as_object)
                .and_then(|output| {
                    output
                        .get("content")
                        .or_else(|| output.get("text"))
                        .and_then(serde_json::Value::as_str)
                })
                .map(str::to_owned)
        })
}

fn is_terminal_event(event_type: &str) -> bool {
    matches!(
        event_type,
        "execution.completed"
            | "execution.failed"
            | "execution.cancelled"
            | "execution.interrupted"
    )
}

fn runtime_error_reason(error: &RuntimeError) -> &'static str {
    if let Some(classification) = connector_runtime_failure(error) {
        return classification.event_reason();
    }
    match error {
        RuntimeError::NotConfigured => "runtime_not_configured",
        RuntimeError::Cancelled => "cancelled",
        RuntimeError::Timeout => "timeout",
        RuntimeError::TransportClosed => "transport_closed",
        RuntimeError::StreamTerminalMissing => "terminal_missing",
        RuntimeError::InvalidWorkspace => "invalid_workspace",
        RuntimeError::Permission => "permission_denied",
        RuntimeError::Protocol(_) => "protocol_error",
        RuntimeError::InvalidStreamCapacity => "invalid_stream_capacity",
        RuntimeError::StreamBufferFull { .. } => "stream_buffer_full",
        RuntimeError::StreamTerminal => "stream_terminal",
        RuntimeError::Transport(_) => "transport_error",
        RuntimeError::Authentication => "authentication_failed",
        RuntimeError::Provider(_) => "provider_error",
        RuntimeError::Unsupported => "unsupported",
    }
}

const MAX_RUNTIME_CATALOG_IDENTIFIER_BYTES: usize = 128;
const MAX_RUNTIME_MODEL_ID_BYTES: usize = 256;

fn stored_selection_configured(value: &StoredModelSelection) -> bool {
    value.selection.mode != ModelSelectionMode::Inherit
        || value.selection.model_id.is_some()
        || value.candidate_model_list_mode != IdentityModelListMode::Inherit
        || value.candidate_model_list_revision != 0
}

#[allow(clippy::too_many_arguments)]
fn resolve_identity_model_selection(
    run_id: String,
    runtime_type: String,
    provider_type: String,
    connector_id: String,
    base_model_id: Option<String>,
    runtime_default_model_id: Option<String>,
    runtime_default_catalog_revision: Option<String>,
    base_revision: u64,
    project_selection: Option<StoredModelSelection>,
    conversation_selection: Option<StoredModelSelection>,
    base_options: Vec<IdentityModelOption>,
    project_options: Vec<IdentityModelOption>,
    conversation_options: Vec<IdentityModelOption>,
) -> Result<ModelSelectionSnapshot, CoreError> {
    let conversation_pinned = validated_pinned_model(conversation_selection.as_ref())?;
    let project_pinned = validated_pinned_model(project_selection.as_ref())?;
    let conversation_overrides = conversation_selection
        .as_ref()
        .is_some_and(|value| value.candidate_model_list_mode == IdentityModelListMode::Override);
    let project_overrides = project_selection
        .as_ref()
        .is_some_and(|value| value.candidate_model_list_mode == IdentityModelListMode::Override);
    let (scope, mut mode, revision, mut options) = if conversation_overrides {
        (
            IdentityModelListScope::ConversationAgent,
            IdentityModelListMode::Override,
            conversation_selection
                .as_ref()
                .map(|value| value.candidate_model_list_revision)
                .unwrap_or(0),
            conversation_options,
        )
    } else if project_overrides {
        (
            IdentityModelListScope::ProjectAgent,
            IdentityModelListMode::Override,
            project_selection
                .as_ref()
                .map(|value| value.candidate_model_list_revision)
                .unwrap_or(0),
            project_options,
        )
    } else {
        (
            IdentityModelListScope::BaseAgent,
            IdentityModelListMode::Own,
            base_revision,
            base_options,
        )
    };
    options.retain(|option| option.connector_id == connector_id);
    if options.is_empty() && scope == IdentityModelListScope::BaseAgent {
        if let Some(model_id) = base_model_id.as_ref() {
            options.push(IdentityModelOption {
                id: format!("legacy:{model_id}"),
                scope: IdentityModelListScope::BaseAgent,
                agent_id: String::new(),
                project_id: None,
                conversation_id: None,
                model_id: model_id.clone(),
                display_name: model_id.clone(),
                connector_id: connector_id.clone(),
                source: ModelOptionSource::Manual,
                availability: ModelAvailability::Unverified,
                is_default: true,
                sort_order: 0,
                catalog_revision: None,
                context_window: None,
                reasoning_efforts: Vec::new(),
                service_tiers: Vec::new(),
            });
            mode = IdentityModelListMode::LegacyCompatibility;
        }
    }
    options.sort_by(|left, right| {
        left.sort_order
            .cmp(&right.sort_order)
            .then_with(|| left.model_id.cmp(&right.model_id))
    });
    let default_model_id = options
        .iter()
        .find(|option| option.is_default)
        .map(|option| option.model_id.clone());
    let identity_or_runtime_default = || {
        default_model_id
            .clone()
            .or_else(|| runtime_default_model_id.clone())
    };
    let default_source = || {
        if default_model_id.is_some() {
            ModelSelectionSource::IdentityDefault
        } else {
            ModelSelectionSource::ConnectorDefault
        }
    };
    let (effective_model_id, selection_source, selection_mode) =
        if let Some(model_id) = conversation_pinned {
            (
                Some(model_id),
                ModelSelectionSource::Conversation,
                ModelSelectionMode::Pinned,
            )
        } else if scope == IdentityModelListScope::ConversationAgent {
            (
                identity_or_runtime_default(),
                default_source(),
                ModelSelectionMode::ConnectorDefault,
            )
        } else if let Some(model_id) = project_pinned {
            (
                Some(model_id),
                ModelSelectionSource::Project,
                ModelSelectionMode::Pinned,
            )
        } else if scope == IdentityModelListScope::ProjectAgent {
            (
                identity_or_runtime_default(),
                default_source(),
                ModelSelectionMode::ConnectorDefault,
            )
        } else if let Some(model_id) = base_model_id {
            (
                Some(model_id),
                ModelSelectionSource::BaseAgent,
                ModelSelectionMode::Pinned,
            )
        } else {
            (
                identity_or_runtime_default(),
                default_source(),
                ModelSelectionMode::ConnectorDefault,
            )
        };
    let selected = effective_model_id
        .as_ref()
        .and_then(|model_id| options.iter().find(|option| option.model_id == *model_id));
    let uses_runtime_default = effective_model_id
        .as_deref()
        .is_some_and(|model_id| runtime_default_model_id.as_deref() == Some(model_id));
    let availability = selected
        .map(|option| option.availability)
        .or_else(|| uses_runtime_default.then_some(ModelAvailability::Available))
        .unwrap_or(ModelAvailability::Unavailable);
    let catalog_revision = selected
        .and_then(|option| option.catalog_revision.clone())
        .or_else(|| {
            uses_runtime_default
                .then(|| runtime_default_catalog_revision.clone())
                .flatten()
        });
    let candidate_model_list = IdentityModelListSnapshot {
        scope,
        mode,
        revision,
        hash: identity_model_options_hash(&options),
        option_count: options.len() as u64,
    };
    Ok(ModelSelectionSnapshot {
        run_id,
        version: 2,
        runtime_type,
        provider_type,
        connector_id,
        effective_model_id,
        selection_source,
        selection_mode,
        availability,
        catalog_revision,
        context_window: selected.and_then(|option| option.context_window),
        reasoning_efforts: selected
            .map(|option| normalized_string_list(&option.reasoning_efforts))
            .unwrap_or_default(),
        service_tiers: selected
            .map(|option| normalized_string_list(&option.service_tiers))
            .unwrap_or_default(),
        candidate_model_list: Some(candidate_model_list),
    })
}

fn resolve_legacy_model_selection(
    run_id: String,
    runtime_type: String,
    provider_type: String,
    connector_id: String,
    base_model_id: Option<String>,
    runtime: &dyn RuntimeAdapter,
    resolved_catalog: Option<&[String]>,
) -> Result<ModelSelectionSnapshot, CoreError> {
    let runtime_models = resolved_catalog
        .map(|models| models.to_vec())
        .unwrap_or_else(|| runtime.list_models())
        .into_iter()
        .filter_map(|model_id| safe_runtime_model_id(&model_id))
        .collect::<Vec<_>>();
    let runtime_default_model_id = runtime.catalog_default_model_id().filter(|model_id| {
        runtime_models.iter().any(|candidate| candidate == model_id)
            && runtime_catalog_model_is_selectable(runtime, model_id)
    });
    let (effective_model_id, selection_source, selection_mode) =
        if let Some(model_id) = base_model_id {
            (
                Some(model_id),
                ModelSelectionSource::BaseAgent,
                ModelSelectionMode::Pinned,
            )
        } else {
            (
                runtime_default_model_id,
                ModelSelectionSource::ConnectorDefault,
                ModelSelectionMode::ConnectorDefault,
            )
        };
    let availability = if resolved_catalog.is_some() {
        if effective_model_id
            .as_deref()
            .is_some_and(|model_id| runtime_models.iter().any(|model| model == model_id))
        {
            ModelAvailability::Available
        } else {
            ModelAvailability::Unavailable
        }
    } else {
        runtime_model_availability(runtime, effective_model_id.as_deref())
    };
    Ok(ModelSelectionSnapshot {
        run_id,
        version: 1,
        runtime_type,
        provider_type,
        connector_id,
        effective_model_id,
        selection_source,
        selection_mode,
        availability,
        catalog_revision: Some(
            resolved_catalog
                .map(|models| runtime_model_catalog_revision_for_models(runtime, models))
                .unwrap_or_else(|| runtime_model_catalog_revision(runtime))
                .to_string(),
        ),
        context_window: None,
        reasoning_efforts: Vec::new(),
        service_tiers: Vec::new(),
        candidate_model_list: None,
    })
}

fn validated_pinned_model(
    stored: Option<&StoredModelSelection>,
) -> Result<Option<String>, CoreError> {
    let Some(stored) = stored else {
        return Ok(None);
    };
    let model_id = stored
        .selection
        .model_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    match stored.selection.mode {
        ModelSelectionMode::Pinned => model_id
            .map(str::to_owned)
            .map(Some)
            .ok_or(CoreError::ModelSelectionSnapshotConflict),
        ModelSelectionMode::Inherit | ModelSelectionMode::ConnectorDefault => {
            if model_id.is_some() {
                Err(CoreError::ModelSelectionSnapshotConflict)
            } else {
                Ok(None)
            }
        }
    }
}

fn runtime_model_availability(
    runtime: &dyn RuntimeAdapter,
    model_id: Option<&str>,
) -> ModelAvailability {
    let Some(model_id) = model_id else {
        return ModelAvailability::Unavailable;
    };
    let known = runtime
        .list_models()
        .into_iter()
        .filter_map(|value| safe_runtime_model_id(&value))
        .any(|value| value == model_id);
    if !known {
        return ModelAvailability::Unverified;
    }
    match runtime_availability(&runtime.health().status) {
        "available" => ModelAvailability::Available,
        "unavailable" => ModelAvailability::Unavailable,
        _ => ModelAvailability::Unverified,
    }
}

fn normalized_string_list(values: &[String]) -> Vec<String> {
    let mut seen = HashSet::new();
    values
        .iter()
        .filter_map(|value| {
            let value = value.trim();
            (!value.is_empty() && seen.insert(value.to_owned())).then(|| value.to_owned())
        })
        .collect()
}

fn identity_model_options_hash(options: &[IdentityModelOption]) -> String {
    let mut options = options.iter().collect::<Vec<_>>();
    options.sort_by(|left, right| {
        left.sort_order
            .cmp(&right.sort_order)
            .then_with(|| left.model_id.cmp(&right.model_id))
    });
    let canonical = options
        .into_iter()
        .map(|option| {
            format!(
                concat!(
                    "{{\"modelId\":{},\"connectorId\":{},\"source\":{},",
                    "\"availability\":{},\"isDefault\":{},\"sortOrder\":{},",
                    "\"catalogRevision\":{},\"contextWindow\":{},",
                    "\"reasoningEfforts\":{},\"serviceTiers\":{}}}"
                ),
                serde_json::to_string(&option.model_id)
                    .expect("String serialization is infallible"),
                serde_json::to_string(&option.connector_id)
                    .expect("String serialization is infallible"),
                serde_json::to_string(model_option_source_name(option.source))
                    .expect("String serialization is infallible"),
                serde_json::to_string(model_availability_name(option.availability))
                    .expect("String serialization is infallible"),
                option.is_default,
                option.sort_order,
                serde_json::to_string(&option.catalog_revision)
                    .expect("optional String serialization is infallible"),
                serde_json::to_string(&option.context_window)
                    .expect("optional integer serialization is infallible"),
                serde_json::to_string(&normalized_string_list(&option.reasoning_efforts))
                    .expect("String list serialization is infallible"),
                serde_json::to_string(&normalized_string_list(&option.service_tiers))
                    .expect("String list serialization is infallible"),
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    sha256_hex(&format!("[{canonical}]"))
}

fn model_option_source_name(source: ModelOptionSource) -> &'static str {
    match source {
        ModelOptionSource::Runtime => "runtime",
        ModelOptionSource::Config => "config",
        ModelOptionSource::Manual => "manual",
    }
}

fn model_availability_name(availability: ModelAvailability) -> &'static str {
    match availability {
        ModelAvailability::Available => "available",
        ModelAvailability::Unverified => "unverified",
        ModelAvailability::Unavailable => "unavailable",
    }
}

fn runtime_model_catalog_revision(runtime: &dyn RuntimeAdapter) -> u64 {
    let models = runtime
        .list_models()
        .into_iter()
        .filter_map(|model_id| safe_runtime_model_id(&model_id))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    runtime_model_catalog_revision_for_models(runtime, &models)
}

fn runtime_model_catalog_revision_for_models(
    runtime: &dyn RuntimeAdapter,
    models: &[String],
) -> u64 {
    if let Some(revision) = runtime.catalog_revision() {
        return revision.max(1);
    }
    let discovery = runtime.discover();
    let canonical = json!({
        "connectorId": safe_runtime_catalog_identifier(runtime.id()),
        "runtimeId": safe_runtime_catalog_identifier(&discovery.runtime_id),
        "runtimeVersion": discovery.version.as_deref().map(safe_runtime_catalog_identifier),
        "models": models,
    });
    let digest = sha256_hex(&canonical.to_string());
    (u64::from_str_radix(&digest[..16], 16).unwrap_or(1) & i64::MAX as u64).max(1)
}

/// Connector catalogs retain richer Runtime-owned availability metadata than
/// the frozen IPC schema can expose.  A missing metadata record is legacy
/// compatible; a record that explicitly disables or withdraws a model is not
/// selectable for a new Connector-bound run.
fn runtime_catalog_model_is_selectable(runtime: &dyn RuntimeAdapter, model_id: &str) -> bool {
    runtime
        .catalog_model_metadata(model_id)
        .map(|metadata| metadata.available && metadata.enabled)
        .unwrap_or(true)
}

fn local_connector_discovery_payload(
    discoveries: Vec<LocalConnectorCandidate>,
) -> serde_json::Value {
    json!({
        "discoveries": discoveries.into_iter().map(|discovery| json!({
            "connectorId": discovery.connector_id,
            "runtimeType": discovery.runtime_type,
            "displayName": discovery.display_name,
            "availability": discovery.availability,
            "models": discovery.models,
            "catalogRevision": discovery.catalog_revision,
            "source": discovery.source,
            "requiresConfiguration": discovery.requires_configuration,
        })).collect::<Vec<_>>(),
    })
}

fn runtime_models_payload(runtime: &dyn RuntimeAdapter) -> serde_json::Value {
    let discovery = runtime.discover();
    let health = runtime.health();
    let availability = runtime_availability(&health.status);
    let capabilities = runtime_capabilities_json(runtime.capabilities());
    let models = runtime
        .list_models()
        .into_iter()
        .filter_map(|model_id| safe_runtime_model_id(&model_id))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let model_metadata = models
        .iter()
        .map(|model_id| {
            json!({
                "modelId": model_id,
                "availability": availability,
                "capabilities": capabilities,
            })
        })
        .collect::<Vec<_>>();

    json!({
        "schemaVersion": RUNTIME_MODELS_SCHEMA_VERSION,
        "connectorId": safe_runtime_catalog_identifier(runtime.id()),
        "runtimeId": safe_runtime_catalog_identifier(&discovery.runtime_id),
        "runtimeVersion": discovery.version.as_deref().map(safe_runtime_catalog_identifier),
        "runtimeOwned": discovery.owned,
        "availability": availability,
        "capabilities": capabilities,
        // Keep the legacy string list stable for existing catalog consumers.
        "models": models,
        "modelMetadata": model_metadata,
    })
}

fn connector_models_payload(
    profile: &ConnectorProfile,
    runtime: &dyn RuntimeAdapter,
    source_models: Vec<String>,
) -> Result<serde_json::Value, CoreError> {
    let capabilities = runtime_capabilities_json(runtime.capabilities());
    // This projection is built only after `ensure_available` and the typed
    // catalog fetch succeed. Do not issue a second health/catalog probe here:
    // a concurrent failure must not turn a valid snapshot into an apparently
    // successful empty catalog or erase the classified failure from IPC.
    let availability = "available";
    let models = source_models
        .into_iter()
        .filter_map(|model_id| safe_runtime_model_id(&model_id))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if models.is_empty() {
        return Err(CoreError::ConnectorCatalogUnavailable);
    }
    let model_metadata = models
        .iter()
        .map(|model_id| {
            let metadata = runtime.catalog_model_metadata(model_id);
            let model_capabilities = metadata
                .as_ref()
                .map(|metadata| runtime_capabilities_json(metadata.capabilities.clone()))
                .unwrap_or_else(|| capabilities.clone());
            let model_availability = metadata
                .as_ref()
                .map(|metadata| {
                    if metadata.available && metadata.enabled {
                        "available"
                    } else {
                        "unavailable"
                    }
                })
                .unwrap_or(availability);
            json!({
                "modelId": model_id,
                "availability": model_availability,
                "capabilities": model_capabilities,
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "schemaVersion": CONNECTOR_MODELS_SCHEMA_VERSION,
        "scopeId": profile.scope_id,
        "connectorId": profile.connector_id,
        "runtimeType": profile.runtime_type,
        "catalogRevision": runtime_model_catalog_revision_for_models(runtime, &models),
        "defaultModelId": runtime
            .catalog_default_model_id()
            .filter(|model_id| {
                models.iter().any(|candidate| candidate == model_id)
                    && runtime_catalog_model_is_selectable(runtime, model_id)
            }),
        "models": models,
        "modelMetadata": model_metadata,
        "availability": availability,
    }))
}

fn runtime_health_payload(runtime: &dyn RuntimeAdapter) -> serde_json::Value {
    let discovery = runtime.discover();
    let health = runtime.health();
    let status = safe_runtime_status(&health.status);
    let availability = runtime_availability(status);
    let connector_id = safe_runtime_catalog_identifier(runtime.id());
    let runtime_id = safe_runtime_catalog_identifier(&discovery.runtime_id);
    let capabilities = runtime_capabilities_json(runtime.capabilities());
    json!({
        "schemaVersion": RUNTIME_HEALTH_SCHEMA_VERSION,
        "runtime": "core",
        "status": status,
        "availability": availability,
        "connectorId": connector_id,
        "runtimeId": runtime_id,
        "runtimeVersion": discovery.version.as_deref().map(safe_runtime_catalog_identifier),
        "runtimeOwned": discovery.owned,
        "healthDetailPresent": health.detail.is_some(),
        "healthDetailRedacted": true,
        "capabilities": capabilities,
        "connectors": [{
            "connectorId": connector_id,
            "runtimeId": runtime_id,
            "status": availability,
            "ok": availability == "available",
            "verified": false,
            "verification": "local_adapter_only",
            "capabilities": capabilities,
        }]
    })
}

fn connector_health_payload(
    profile: &ConnectorProfile,
    runtime: Option<&dyn RuntimeAdapter>,
) -> serde_json::Value {
    let Some(runtime) = runtime else {
        return json!({
            "schemaVersion": CONNECTOR_HEALTH_SCHEMA_VERSION,
            "scopeId": profile.scope_id,
            "connector": {
                "connectorId": profile.connector_id,
                "displayName": profile.display_name,
                "providerType": profile.provider_type,
                "runtimeType": profile.runtime_type,
                "enabled": profile.enabled,
                "status": if profile.enabled { "unavailable" } else { "disabled" },
                "availability": "unavailable",
                "ok": false,
                "verified": false,
                // A profile that names no registered adapter remains a
                // fail-closed mismatch in the legacy health projection. The
                // stricter `connector.models` resolver reports the additive
                // `connector_runtime_unavailable` category for this same
                // condition.
                "verification": if profile.enabled { "runtime_mismatch" } else { "profile_disabled" },
                "runtimeId": "unknown",
                "runtimeVersion": Value::Null,
                "runtimeOwned": false,
                "capabilities": runtime_capabilities_json(RuntimeCapabilities {
                    streaming: false,
                    cancel: false,
                    filesystem: false,
                    shell: false,
                }),
                "authReferencePresent": profile.auth_env_key.is_some(),
                "healthDetailPresent": false,
                "healthDetailRedacted": true,
            },
        });
    };
    let discovery = runtime.discover();
    let health = runtime.health();
    let runtime_id = safe_runtime_catalog_identifier(&discovery.runtime_id);
    let runtime_version = discovery
        .version
        .as_deref()
        .map(safe_runtime_catalog_identifier);
    let capabilities = runtime_capabilities_json(runtime.capabilities());
    let profile_runtime_matches = profile.runtime_type == runtime.id();
    let (status, availability, ok, verification) = if !profile.enabled {
        ("disabled", "unavailable", false, "profile_disabled")
    } else if !profile_runtime_matches {
        ("unavailable", "unavailable", false, "runtime_mismatch")
    } else {
        let status = safe_runtime_status(&health.status);
        let availability = runtime_availability(status);
        (
            status,
            availability,
            availability == "available",
            "local_adapter_only",
        )
    };

    json!({
        "schemaVersion": CONNECTOR_HEALTH_SCHEMA_VERSION,
        "scopeId": profile.scope_id,
        "connector": {
            "connectorId": profile.connector_id,
            "displayName": profile.display_name,
            "providerType": profile.provider_type,
            "runtimeType": profile.runtime_type,
            "enabled": profile.enabled,
            "status": status,
            "availability": availability,
            "ok": ok,
            "verified": false,
            "verification": verification,
            "runtimeId": runtime_id,
            "runtimeVersion": runtime_version,
            "runtimeOwned": discovery.owned,
            "capabilities": capabilities,
            "authReferencePresent": profile.auth_env_key.is_some(),
            "healthDetailPresent": health.detail.is_some(),
            "healthDetailRedacted": true,
        },
    })
}

fn runtime_capabilities_json(capabilities: RuntimeCapabilities) -> serde_json::Value {
    json!({
        "streaming": capabilities.streaming,
        "cancel": capabilities.cancel,
        "filesystem": capabilities.filesystem,
        "shell": capabilities.shell,
    })
}

fn runtime_availability(status: &str) -> &'static str {
    match status {
        "ready" | "healthy" | "ok" | "available" => "available",
        "degraded" => "degraded",
        "unavailable" | "offline" | "error" => "unavailable",
        _ => "unknown",
    }
}

fn safe_runtime_status(value: &str) -> &'static str {
    match value {
        "available" => "available",
        "ready" => "ready",
        "healthy" => "healthy",
        "ok" => "ok",
        "degraded" => "degraded",
        "unavailable" => "unavailable",
        "offline" => "offline",
        "error" => "error",
        _ => "unknown",
    }
}

fn safe_runtime_catalog_identifier(value: &str) -> String {
    if is_safe_runtime_identifier(value, MAX_RUNTIME_CATALOG_IDENTIFIER_BYTES) {
        value.to_owned()
    } else {
        "unknown".into()
    }
}

fn safe_runtime_model_id(value: &str) -> Option<String> {
    if !is_safe_runtime_identifier(value, MAX_RUNTIME_MODEL_ID_BYTES) {
        return None;
    }
    let lower = value.to_ascii_lowercase();
    if [
        "token",
        "secret",
        "authorization",
        "api-key",
        "api_key",
        "apikey",
        "provider-config",
        "provider_config",
    ]
    .iter()
    .any(|marker| lower.contains(marker))
    {
        return None;
    }
    Some(value.to_owned())
}

fn is_safe_runtime_identifier(value: &str, max_bytes: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_bytes
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._:/-".contains(&byte))
}

#[cfg(test)]
fn runtime_catalog_fixture_runtime() -> impl RuntimeAdapter {
    struct FixtureRuntime;

    impl RuntimeAdapter for FixtureRuntime {
        fn id(&self) -> &str {
            "fixture-connector"
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
                runtime_id: "fixture-runtime".into(),
                version: Some("fixture-1".into()),
                owned: true,
            }
        }

        fn health(&self) -> RuntimeHealth {
            RuntimeHealth {
                runtime_id: "fixture-runtime".into(),
                status: "ready".into(),
                detail: Some("Authorization: bearer should never be serialized".into()),
            }
        }

        fn list_models(&self) -> Vec<String> {
            vec![
                "safe-model".into(),
                "safe-model".into(),
                "token=secret".into(),
                "Authorization: bearer secret".into(),
                "provider-config".into(),
                "bad model".into(),
                String::new(),
            ]
        }

        fn catalog_revision(&self) -> Option<u64> {
            Some(9)
        }

        fn execute(&self, _request: &RuntimeRequest) -> Result<Vec<RuntimeEvent>, RuntimeError> {
            Err(RuntimeError::Unsupported)
        }

        fn cancel(&self, _request: &RuntimeRequest) -> Result<RuntimeEvent, RuntimeError> {
            Err(RuntimeError::Unsupported)
        }
    }

    FixtureRuntime
}

#[cfg(test)]
mod tests {
    use super::*;
    use agenttalk_domain::CollaborationStatus;
    use agenttalk_orchestration_contracts::registry::InMemorySchemaRegistry;
    use agenttalk_runtime_host::{CodexAppServerRuntime, RuntimeEventStream};
    use std::sync::{Arc, Mutex};

    #[test]
    fn core_seals_brief_before_creating_orchestration_run() {
        let base = std::env::temp_dir().join(format!(
            "agenttalk-core-orchestration-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let root = base.join("project");
        std::fs::create_dir_all(root.join("plan")).unwrap();
        let body = b"# sealed roadmap\n";
        std::fs::write(root.join("plan/roadmap.md"), body).unwrap();
        let manifest = json!({
            "schemaVersion": "agenttalk.brief.manifest.v1",
            "projectId": "project-1",
            "title": "Core orchestration fixture",
            "roles": [{"roleId": "owner", "displayName": "Owner"}],
            "files": [{
                "path": "plan/roadmap.md",
                "kind": "plan",
                "format": "markdown",
                "contentSchemaRef": null,
                "required": true,
                "sha256": agenttalk_brief_sealer::cas::sha256_hex(body),
                "size": body.len(),
                "context": {"layer": "shared", "roleIds": ["owner"], "retention": "run", "workspaceAccess": "read_only"},
                "declaredOwnerRoleId": "owner"
            }]
        });
        std::fs::write(
            root.join("agenttalk-brief.json"),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();

        let mut core = PersistentCore::open(":memory:").unwrap();
        let created = core
            .create_orchestration_run_from_brief(
                CreateOrchestrationRunFromBriefCommand {
                    project_id: "project-1".into(),
                    run_id: "orchestration-run-1".into(),
                    project_root: root.clone(),
                    dag_snapshot_digest: "a".repeat(64),
                    role_binding_snapshot_digest: "b".repeat(64),
                },
                &InMemorySchemaRegistry::new(),
            )
            .unwrap();
        assert_eq!(created.run.status, "pending");
        assert_eq!(
            created.run.brief_snapshot_id,
            created.seal.brief_snapshot_id()
        );
        assert_eq!(
            created.run.brief_tree_digest,
            created.seal.brief_tree_digest()
        );

        // The snapshot remains readable from CAS after the mutable authoring
        // files are removed; the Run never re-reads those paths.
        std::fs::remove_file(root.join("agenttalk-brief.json")).unwrap();
        std::fs::remove_file(root.join("plan/roadmap.md")).unwrap();
        let descriptor = agenttalk_brief_sealer::BriefSealer::new(&root)
            .read_snapshot_descriptor(&created.run.brief_snapshot_id)
            .unwrap();
        assert_eq!(
            descriptor.brief_tree_digest(),
            created.run.brief_tree_digest
        );
        assert_eq!(descriptor.files().len(), 1);
        std::fs::remove_dir_all(&base).unwrap();
    }

    #[test]
    fn core_restart_fences_and_recovers_orchestration_attempts() {
        let base = std::env::temp_dir().join(format!(
            "agenttalk-core-orchestration-recovery-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let database = base.join("core.sqlite3");
        std::fs::create_dir_all(&base).unwrap();
        {
            let mut storage = SqliteStore::open(&database).unwrap();
            storage
                .create_orchestration_run(agenttalk_storage::OrchestrationRunSeed {
                    run_id: "orchestration-run-recovery".into(),
                    project_id: "project-1".into(),
                    brief_snapshot_id: format!("sha256:{}", "0".repeat(64)),
                    brief_tree_digest: "0".repeat(64),
                    dag_snapshot_digest: "1".repeat(64),
                    role_binding_snapshot_digest: "2".repeat(64),
                })
                .unwrap();
            storage
                .insert_orchestration_task_node(
                    "orchestration-run-recovery",
                    "node-1",
                    "node-key-1",
                )
                .unwrap();
            storage
                .mark_orchestration_task_ready("node-1", "input", "role-1", "contract-1")
                .unwrap();
            storage
                .transition_task_ready_to_running("node-1", "execution-run-1", "worker-a")
                .unwrap();
        }
        let core = PersistentCore::open(&database).unwrap();
        assert_eq!(
            core.storage
                .orchestration_run("orchestration-run-recovery")
                .unwrap()
                .coordinator_generation,
            2
        );
        let recovery = core
            .storage
            .orchestration_recovery_state("orchestration-run-recovery")
            .unwrap();
        assert_eq!(recovery, vec![("node-1".into(), "failed".into(), 1)]);
        drop(core);
        std::fs::remove_dir_all(&base).unwrap();
    }

    struct ShutdownProbeRuntime {
        id: &'static str,
        fail_shutdown: bool,
        calls: Arc<Mutex<Vec<&'static str>>>,
    }

    struct CatalogProbeRuntime {
        outcome: Result<Vec<String>, RuntimeError>,
    }

    struct DisabledDefaultCatalogRuntime;

    impl RuntimeAdapter for CatalogProbeRuntime {
        fn id(&self) -> &str {
            "kun"
        }

        fn capabilities(&self) -> RuntimeCapabilities {
            RuntimeCapabilities {
                streaming: true,
                cancel: true,
                filesystem: true,
                shell: true,
            }
        }

        fn health(&self) -> RuntimeHealth {
            RuntimeHealth {
                runtime_id: "kun".into(),
                status: "available".into(),
                detail: None,
            }
        }

        fn ensure_available(&self) -> Result<(), RuntimeError> {
            Ok(())
        }

        fn list_models(&self) -> Vec<String> {
            self.outcome.clone().unwrap_or_default()
        }

        fn list_models_checked(&self) -> Result<Vec<String>, RuntimeError> {
            self.outcome.clone()
        }

        fn catalog_default_model_id(&self) -> Option<String> {
            Some("kun-model-b".into())
        }

        fn execute(&self, _request: &RuntimeRequest) -> Result<Vec<RuntimeEvent>, RuntimeError> {
            Err(RuntimeError::Unsupported)
        }

        fn cancel(&self, _request: &RuntimeRequest) -> Result<RuntimeEvent, RuntimeError> {
            Err(RuntimeError::Unsupported)
        }
    }

    impl RuntimeAdapter for DisabledDefaultCatalogRuntime {
        fn id(&self) -> &str {
            "kun"
        }

        fn capabilities(&self) -> RuntimeCapabilities {
            RuntimeCapabilities {
                streaming: true,
                cancel: true,
                filesystem: true,
                shell: true,
            }
        }

        fn health(&self) -> RuntimeHealth {
            RuntimeHealth {
                runtime_id: "kun".into(),
                status: "available".into(),
                detail: None,
            }
        }

        fn ensure_available(&self) -> Result<(), RuntimeError> {
            Ok(())
        }

        fn list_models(&self) -> Vec<String> {
            vec!["kun-model-a".into(), "kun-model-b".into()]
        }

        fn list_models_checked(&self) -> Result<Vec<String>, RuntimeError> {
            Ok(self.list_models())
        }

        fn catalog_default_model_id(&self) -> Option<String> {
            Some("kun-model-b".into())
        }

        fn catalog_model_metadata(&self, model_id: &str) -> Option<RuntimeModelMetadata> {
            (model_id == "kun-model-b").then(|| RuntimeModelMetadata {
                model_id: model_id.into(),
                available: false,
                enabled: false,
                status: Some("disabled".into()),
                capabilities: self.capabilities(),
            })
        }

        fn execute(&self, _request: &RuntimeRequest) -> Result<Vec<RuntimeEvent>, RuntimeError> {
            Err(RuntimeError::Unsupported)
        }

        fn cancel(&self, _request: &RuntimeRequest) -> Result<RuntimeEvent, RuntimeError> {
            Err(RuntimeError::Unsupported)
        }
    }

    impl RuntimeAdapter for ShutdownProbeRuntime {
        fn id(&self) -> &str {
            self.id
        }

        fn capabilities(&self) -> RuntimeCapabilities {
            RuntimeCapabilities {
                streaming: false,
                cancel: false,
                filesystem: false,
                shell: false,
            }
        }

        fn execute(&self, _request: &RuntimeRequest) -> Result<Vec<RuntimeEvent>, RuntimeError> {
            Err(RuntimeError::Unsupported)
        }

        fn cancel(&self, _request: &RuntimeRequest) -> Result<RuntimeEvent, RuntimeError> {
            Err(RuntimeError::Unsupported)
        }

        fn shutdown_owned(&self) -> Result<(), RuntimeError> {
            self.calls.lock().unwrap().push(self.id);
            if self.fail_shutdown {
                Err(RuntimeError::Transport(
                    "fixture-token-must-not-leak".into(),
                ))
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn runtime_registry_shutdown_attempts_every_adapter_and_redacts_failures() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let registry = RuntimeRegistry::from_adapters(vec![
            Box::new(ShutdownProbeRuntime {
                id: "shutdown-probe-failing",
                fail_shutdown: true,
                calls: Arc::clone(&calls),
            }),
            Box::new(ShutdownProbeRuntime {
                id: "shutdown-probe-after-failure",
                fail_shutdown: false,
                calls: Arc::clone(&calls),
            }),
            Box::new(ShutdownProbeRuntime {
                id: "shutdown-probe-external-noop",
                fail_shutdown: false,
                calls: Arc::clone(&calls),
            }),
        ])
        .expect("test registry should accept unique RuntimeAdapter ids");

        let error = registry
            .shutdown_owned()
            .expect_err("one owned runtime shutdown failure must be reported");
        let mut attempted = calls.lock().expect("test call log should unlock").clone();
        attempted.sort_unstable();
        assert_eq!(
            attempted,
            vec![
                "shutdown-probe-after-failure",
                "shutdown-probe-external-noop",
                "shutdown-probe-failing",
            ]
        );

        let message = error.to_string();
        assert!(message.contains("1 adapter(s)"));
        assert!(!message.contains("fixture-token-must-not-leak"));
    }

    #[test]
    fn connector_models_preserves_typed_catalog_failures_and_rejects_empty_catalogs() {
        let failures = vec![
            RuntimeError::Authentication,
            RuntimeError::Protocol("kun_runtime_identity_mismatch".into()),
            RuntimeError::Transport("kun_catalog_unavailable".into()),
            RuntimeError::Transport("kun_shared_runtime_unavailable".into()),
        ];
        for failure in failures {
            let registry = RuntimeRegistry::from_adapters(vec![Box::new(CatalogProbeRuntime {
                outcome: Err(failure.clone()),
            })])
            .expect("typed catalog test registry");
            let mut core = PersistentCore::open_with_runtime_registry(":memory:", registry)
                .expect("open isolated Core");
            core.create_connector_profile(ConnectorProfile {
                scope_id: "desktop".into(),
                connector_id: "kun-profile".into(),
                display_name: "Kun fixture profile".into(),
                provider_type: "kun".into(),
                runtime_type: "kun".into(),
                enabled: true,
                auth_env_key: None,
            })
            .expect("persist profile");
            match core.connector_models("desktop", "kun-profile") {
                Err(CoreError::Runtime(actual)) => assert_eq!(actual, failure),
                other => panic!("typed Runtime failure was swallowed: {other:?}"),
            }
        }

        let registry = RuntimeRegistry::from_adapters(vec![Box::new(CatalogProbeRuntime {
            outcome: Ok(Vec::new()),
        })])
        .expect("empty catalog test registry");
        let mut core = PersistentCore::open_with_runtime_registry(":memory:", registry)
            .expect("open isolated Core");
        core.create_connector_profile(ConnectorProfile {
            scope_id: "desktop".into(),
            connector_id: "kun-empty-profile".into(),
            display_name: "Kun empty catalog fixture".into(),
            provider_type: "kun".into(),
            runtime_type: "kun".into(),
            enabled: true,
            auth_env_key: None,
        })
        .expect("persist profile");
        assert!(matches!(
            core.connector_models("desktop", "kun-empty-profile"),
            Err(CoreError::ConnectorCatalogUnavailable)
        ));
        assert_eq!(
            runtime_error_reason(&RuntimeError::Provider(
                "kun_provider_authentication_failed".into()
            )),
            "provider_authentication_failed"
        );
    }

    #[test]
    fn connector_catalog_uses_runtime_declared_default_not_sorted_first_model() {
        let registry = RuntimeRegistry::from_adapters(vec![Box::new(CatalogProbeRuntime {
            outcome: Ok(vec!["kun-model-a".into(), "kun-model-b".into()]),
        })])
        .expect("declared-default registry");
        let mut core = PersistentCore::open_with_runtime_registry(":memory:", registry)
            .expect("open isolated Core");
        core.create_connector_profile(ConnectorProfile {
            scope_id: "desktop".into(),
            connector_id: "kun-default-profile".into(),
            display_name: "Kun declared default fixture".into(),
            provider_type: "kun".into(),
            runtime_type: "kun".into(),
            enabled: true,
            auth_env_key: None,
        })
        .expect("persist profile");

        let payload = core
            .connector_models("desktop", "kun-default-profile")
            .expect("safe catalog projection");
        assert_eq!(payload["models"], json!(["kun-model-a", "kun-model-b"]));
        assert_eq!(payload["defaultModelId"], "kun-model-b");
    }

    #[test]
    fn connector_catalog_does_not_publish_an_explicitly_disabled_runtime_default() {
        let registry =
            RuntimeRegistry::from_adapters(vec![Box::new(DisabledDefaultCatalogRuntime)])
                .expect("disabled-default registry");
        let mut core = PersistentCore::open_with_runtime_registry(":memory:", registry)
            .expect("open isolated Core");
        core.create_connector_profile(ConnectorProfile {
            scope_id: "desktop".into(),
            connector_id: "kun-disabled-default-profile".into(),
            display_name: "Kun disabled default fixture".into(),
            provider_type: "kun".into(),
            runtime_type: "kun".into(),
            enabled: true,
            auth_env_key: None,
        })
        .expect("persist profile");

        let payload = core
            .connector_models("desktop", "kun-disabled-default-profile")
            .expect("safe catalog projection");
        assert_eq!(payload["defaultModelId"], Value::Null);
        let metadata = payload["modelMetadata"]
            .as_array()
            .expect("model metadata array")
            .iter()
            .find(|metadata| metadata["modelId"] == "kun-model-b")
            .expect("disabled runtime model metadata");
        assert_eq!(metadata["availability"], "unavailable");
        assert!(metadata.get("enabled").is_none());
        assert!(metadata.get("status").is_none());
    }

    struct SilentRuntime;

    impl RuntimeAdapter for SilentRuntime {
        fn id(&self) -> &str {
            "silent-timeout-fixture"
        }

        fn capabilities(&self) -> RuntimeCapabilities {
            RuntimeCapabilities {
                streaming: true,
                cancel: true,
                filesystem: false,
                shell: false,
            }
        }

        fn execute(&self, _request: &RuntimeRequest) -> Result<Vec<RuntimeEvent>, RuntimeError> {
            Err(RuntimeError::Unsupported)
        }

        fn stream_events_with_capacity(
            &self,
            _request: &RuntimeRequest,
            capacity: usize,
        ) -> Result<RuntimeEventStream, RuntimeError> {
            RuntimeEventStream::with_capacity(capacity)
        }

        fn cancel(&self, _request: &RuntimeRequest) -> Result<RuntimeEvent, RuntimeError> {
            Err(RuntimeError::Unsupported)
        }
    }

    #[test]
    fn runtime_deadline_persists_one_authoritative_timeout_terminal_state() {
        let mut core =
            PersistentCore::open_with_runtime(":memory:", Box::new(SilentRuntime)).unwrap();
        core.assign_agent("agent-timeout");
        let run = core
            .start_execution_internal(
                ExecutionStart {
                    run_id: "run-timeout".into(),
                    collaboration_run_id: "collaboration-timeout".into(),
                    project_id: "project-timeout".into(),
                    conversation_id: "conversation-timeout".into(),
                    agent_id: "agent-timeout".into(),
                    workspace_access: WorkspaceAccess::None,
                    canonical_cwd: None,
                },
                None,
                None,
                None,
                Some(5),
            )
            .unwrap();
        assert_eq!(run.status, ExecutionStatus::Failed);
        assert_eq!(run.terminal_reason.as_deref(), Some("timeout"));
        let timeout_events = core
            .replay_events(0)
            .unwrap()
            .into_iter()
            .filter(|event| {
                event.execution_run_id == "run-timeout"
                    && event.event_type == "execution.failed"
                    && event.payload["reason"] == "timeout"
            })
            .count();
        assert_eq!(timeout_events, 1);

        let invalid = core.start_execution_internal(
            ExecutionStart {
                run_id: "run-timeout-invalid".into(),
                collaboration_run_id: "collaboration-timeout".into(),
                project_id: "project-timeout".into(),
                conversation_id: "conversation-timeout".into(),
                agent_id: "agent-timeout".into(),
                workspace_access: WorkspaceAccess::None,
                canonical_cwd: None,
            },
            None,
            None,
            None,
            Some(agenttalk_runtime_host::MAX_RUNTIME_TIMEOUT_MS + 1),
        );
        assert!(matches!(invalid, Err(CoreError::RuntimeTimeoutInvalid)));
        assert!(core.recover_run("run-timeout-invalid").unwrap().is_none());
    }

    #[test]
    fn runtime_models_are_versioned_stable_and_fail_closed() {
        let core = PersistentCore::open_with_runtime(
            ":memory:",
            Box::new(runtime_catalog_fixture_runtime()),
        )
        .unwrap();

        let payload = core.runtime_models();
        assert_eq!(payload["schemaVersion"], RUNTIME_MODELS_SCHEMA_VERSION);
        assert_eq!(payload["connectorId"], "fixture-connector");
        assert_eq!(payload["runtimeId"], "fixture-runtime");
        assert_eq!(payload["runtimeVersion"], "fixture-1");
        assert_eq!(payload["availability"], "available");
        assert_eq!(payload["models"], json!(["safe-model"]));
        assert_eq!(
            payload["modelMetadata"][0],
            json!({
                "modelId": "safe-model",
                "availability": "available",
                "capabilities": {
                    "streaming": true,
                    "cancel": false,
                    "filesystem": false,
                    "shell": false,
                },
            })
        );
        let serialized = serde_json::to_string(&payload)
            .unwrap()
            .to_ascii_lowercase();
        for forbidden in ["token", "secret", "authorization", "provider-config"] {
            assert!(!serialized.contains(forbidden));
        }
        assert!(payload.get("detail").is_none());

        let health = core.runtime_health();
        assert_eq!(health["schemaVersion"], RUNTIME_HEALTH_SCHEMA_VERSION);
        assert_eq!(health["status"], "ready");
        assert_eq!(health["availability"], "available");
        assert_eq!(health["connectorId"], "fixture-connector");
        assert_eq!(
            health["connectors"][0]["verification"],
            "local_adapter_only"
        );
        assert_eq!(health["healthDetailPresent"], true);
        assert_eq!(health["healthDetailRedacted"], true);
        let serialized_health = serde_json::to_string(&health).unwrap().to_ascii_lowercase();
        for forbidden in [
            "token",
            "secret",
            "authorization",
            "bearer",
            "provider-config",
        ] {
            assert!(!serialized_health.contains(forbidden));
        }
    }

    #[test]
    fn connector_catalog_prefers_a_safe_runtime_supplied_revision() {
        let runtime = runtime_catalog_fixture_runtime();
        assert_eq!(runtime_model_catalog_revision(&runtime), 9);
    }

    #[test]
    fn connector_health_is_profile_specific_and_never_verifies_credentials() {
        let mut core = PersistentCore::open_with_runtime(
            ":memory:",
            Box::new(agenttalk_runtime_host::MockRuntime::default()),
        )
        .unwrap();
        core.create_connector_profile(ConnectorProfile {
            scope_id: "desktop".into(),
            connector_id: "mock-profile".into(),
            display_name: "Local Mock".into(),
            provider_type: "mock".into(),
            runtime_type: "mock".into(),
            enabled: true,
            auth_env_key: Some("AGENTTALK_FIXTURE_KEY".into()),
        })
        .unwrap();

        let health = core
            .connector_health("desktop", "mock-profile")
            .expect("profile health");
        assert_eq!(health["schemaVersion"], CONNECTOR_HEALTH_SCHEMA_VERSION);
        assert_eq!(health["scopeId"], "desktop");
        assert_eq!(health["connector"]["connectorId"], "mock-profile");
        assert_eq!(health["connector"]["availability"], "available");
        assert_eq!(health["connector"]["ok"], true);
        assert_eq!(health["connector"]["verified"], false);
        assert_eq!(health["connector"]["verification"], "local_adapter_only");
        assert_eq!(health["connector"]["authReferencePresent"], true);
        let serialized = serde_json::to_string(&health).unwrap().to_ascii_lowercase();
        for forbidden in ["agenttalk_fixture_key", "token", "secret", "authorization"] {
            assert!(!serialized.contains(forbidden));
        }

        let missing = core.connector_health("desktop", "missing");
        assert!(matches!(missing, Err(CoreError::ConnectorNotFound)));
    }

    #[test]
    fn connector_health_fails_closed_for_disabled_or_mismatched_profiles() {
        let mut core = PersistentCore::open_with_runtime(
            ":memory:",
            Box::new(agenttalk_runtime_host::MockRuntime::default()),
        )
        .unwrap();
        for (connector_id, runtime_type, enabled) in [
            ("disabled-profile", "mock", false),
            ("mismatched-profile", "codex", true),
        ] {
            core.create_connector_profile(ConnectorProfile {
                scope_id: "desktop".into(),
                connector_id: connector_id.into(),
                display_name: connector_id.into(),
                provider_type: "fixture".into(),
                runtime_type: runtime_type.into(),
                enabled,
                auth_env_key: None,
            })
            .unwrap();
        }

        let disabled = core
            .connector_health("desktop", "disabled-profile")
            .unwrap();
        assert_eq!(disabled["connector"]["status"], "disabled");
        assert_eq!(disabled["connector"]["ok"], false);
        assert_eq!(disabled["connector"]["verification"], "profile_disabled");

        let mismatched = core
            .connector_health("desktop", "mismatched-profile")
            .unwrap();
        assert_eq!(mismatched["connector"]["status"], "unavailable");
        assert_eq!(mismatched["connector"]["ok"], false);
        assert_eq!(mismatched["connector"]["verification"], "runtime_mismatch");
    }

    #[test]
    fn explicit_connector_execution_binding_allows_mock_and_rejects_unverified_runtime() {
        let mut mock_core = PersistentCore::open_with_runtime(
            ":memory:",
            Box::new(agenttalk_runtime_host::MockRuntime::default()),
        )
        .unwrap();
        mock_core
            .create_connector_profile(ConnectorProfile {
                scope_id: "desktop".into(),
                connector_id: "mock-profile".into(),
                display_name: "Local Mock".into(),
                provider_type: "mock".into(),
                runtime_type: "mock".into(),
                enabled: true,
                auth_env_key: None,
            })
            .unwrap();
        assert!(mock_core
            .validate_connector_binding(&ExecutionRuntimeBinding {
                connector_id: "mock-profile".into(),
                runtime_type: Some("mock".into()),
                model_id: Some("mock-default".into()),
                catalog_revision: None,
                validate_profile: true,
            })
            .is_ok());

        let mut kun_core =
            PersistentCore::open_with_runtime(":memory:", Box::new(ConfiguredAdapter::kun()))
                .unwrap();
        kun_core
            .create_connector_profile(ConnectorProfile {
                scope_id: "desktop".into(),
                connector_id: "kun-profile".into(),
                display_name: "Kun".into(),
                provider_type: "kun".into(),
                runtime_type: "kun".into(),
                enabled: true,
                auth_env_key: Some("KUN_AUTH".into()),
            })
            .unwrap();
        assert!(matches!(
            kun_core.validate_connector_binding(&ExecutionRuntimeBinding {
                connector_id: "kun-profile".into(),
                runtime_type: Some("kun".into()),
                model_id: None,
                catalog_revision: None,
                validate_profile: true,
            }),
            Err(CoreError::ConnectorUnverified)
        ));
    }

    #[test]
    fn core_rejects_unassigned_agents_and_preserves_frozen_scope() {
        let mut core = CoreState::default();
        assert!(matches!(
            core.start_execution(ExecutionStart {
                run_id: "r0".into(),
                collaboration_run_id: "c0".into(),
                project_id: "p0".into(),
                conversation_id: "v0".into(),
                agent_id: "unassigned".into(),
                workspace_access: WorkspaceAccess::None,
                canonical_cwd: None,
            }),
            Err(CoreError::AgentNotAssigned)
        ));
        core.assign_agent("agent-1");
        let run = core
            .start_execution(ExecutionStart {
                run_id: "r1".into(),
                collaboration_run_id: "c1".into(),
                project_id: "project-1".into(),
                conversation_id: "conversation-1".into(),
                agent_id: "agent-1".into(),
                workspace_access: WorkspaceAccess::ReadOnly,
                canonical_cwd: Some("C:\\workspace".into()),
            })
            .unwrap();
        assert_eq!(run.scope.agent_id, "agent-1");
        assert_eq!(run.scope.workspace_access, WorkspaceAccess::ReadOnly);
        assert_eq!(run.scope.canonical_cwd.as_deref(), Some("C:\\workspace"));
    }

    #[test]
    fn duplicate_execution_run_ids_are_rejected_without_replacing_scope() {
        let mut core = CoreState::default();
        core.assign_agent("agent-1");
        let first = core
            .start_execution(ExecutionStart {
                run_id: "run-1".into(),
                collaboration_run_id: "collab-1".into(),
                project_id: "project-1".into(),
                conversation_id: "conversation-1".into(),
                agent_id: "agent-1".into(),
                workspace_access: WorkspaceAccess::ReadOnly,
                canonical_cwd: None,
            })
            .unwrap()
            .clone();
        let result = core.start_execution(ExecutionStart {
            run_id: "run-1".into(),
            collaboration_run_id: "collab-2".into(),
            project_id: "project-2".into(),
            conversation_id: "conversation-2".into(),
            agent_id: "agent-1".into(),
            workspace_access: WorkspaceAccess::WorkspaceWrite,
            canonical_cwd: Some("C:\\other".into()),
        });
        assert!(matches!(result, Err(CoreError::RunAlreadyExists)));
        assert_eq!(core.run("run-1").unwrap(), &first);
    }

    #[test]
    fn retry_creates_a_distinct_run() {
        let mut core = CoreState::default();
        core.assign_agent("agent-1");
        let source = core
            .start_execution(ExecutionStart {
                run_id: "r1".into(),
                collaboration_run_id: "c1".into(),
                project_id: "p1".into(),
                conversation_id: "v1".into(),
                agent_id: "agent-1".into(),
                workspace_access: WorkspaceAccess::None,
                canonical_cwd: None,
            })
            .unwrap()
            .clone();
        let retry = core.retry("r2", &source).unwrap();
        assert_ne!(source.id, retry.id);
        assert_eq!(retry.status, ExecutionStatus::Pending);
    }

    #[test]
    fn persistent_retry_creates_a_new_run_without_reviving_the_source() {
        let mut core = PersistentCore::open(":memory:").unwrap();
        core.assign_agent("agent-1");
        let source = core
            .start_execution(ExecutionStart {
                run_id: "persistent-retry-source".into(),
                collaboration_run_id: "collaboration-retry".into(),
                project_id: "project-retry".into(),
                conversation_id: "conversation-retry".into(),
                agent_id: "agent-1".into(),
                workspace_access: WorkspaceAccess::None,
                canonical_cwd: None,
            })
            .unwrap();
        assert_eq!(source.status, ExecutionStatus::Completed);

        let retry = core
            .retry_execution(
                "persistent-retry-child",
                &source.id,
                "retry task".into(),
                None,
                None,
            )
            .unwrap();
        assert_eq!(retry.id, "persistent-retry-child");
        assert_eq!(retry.project_id, source.project_id);
        assert_eq!(retry.conversation_id, source.conversation_id);
        assert_eq!(retry.agent_id, source.agent_id);
        assert_eq!(retry.scope.workspace_access, source.scope.workspace_access);
        assert_eq!(retry.status, ExecutionStatus::Completed);
        assert_eq!(
            core.recover_run("persistent-retry-source")
                .unwrap()
                .unwrap()
                .status,
            ExecutionStatus::Completed
        );
        assert!(core
            .recover_run("persistent-retry-child")
            .unwrap()
            .is_some());
    }

    #[test]
    fn model_snapshot_is_frozen_across_retry_and_core_restart() {
        let path = std::env::temp_dir().join(format!(
            "agenttalk-model-snapshot-{}.sqlite3",
            unix_time_nanos()
        ));
        let _ = std::fs::remove_file(&path);
        let (source_id, source_snapshot) = {
            let mut core = PersistentCore::open(&path).unwrap();
            core.create_connector_profile(ConnectorProfile {
                scope_id: "desktop".into(),
                connector_id: "mock-profile".into(),
                display_name: "Local Mock".into(),
                provider_type: "mock".into(),
                runtime_type: "mock".into(),
                enabled: true,
                auth_env_key: None,
            })
            .unwrap();
            core.assign_agent("agent-model-snapshot");
            let source = core
                .start_execution_internal(
                    ExecutionStart {
                        run_id: "model-snapshot-source".into(),
                        collaboration_run_id: "model-snapshot-collaboration".into(),
                        project_id: "model-snapshot-project".into(),
                        conversation_id: "model-snapshot-conversation".into(),
                        agent_id: "agent-model-snapshot".into(),
                        workspace_access: WorkspaceAccess::None,
                        canonical_cwd: None,
                    },
                    Some("freeze this model".into()),
                    None,
                    Some(ExecutionRuntimeBinding {
                        connector_id: "mock-profile".into(),
                        runtime_type: Some("mock".into()),
                        model_id: Some("mock-default".into()),
                        catalog_revision: None,
                        validate_profile: true,
                    }),
                    None,
                )
                .unwrap();
            assert_eq!(source.status, ExecutionStatus::Completed);
            let snapshot = core.model_snapshot(&source.id).unwrap().unwrap();
            let retry = core
                .retry_execution(
                    "model-snapshot-retry",
                    &source.id,
                    "retry with the frozen model".into(),
                    None,
                    None,
                )
                .unwrap();
            assert_eq!(
                core.model_snapshot(&retry.id).unwrap().unwrap(),
                ModelSnapshot {
                    run_id: retry.id.clone(),
                    ..snapshot.clone()
                }
            );
            (source.id, snapshot)
        };

        let mut reopened = PersistentCore::open(&path).unwrap();
        assert_eq!(
            reopened.model_snapshot(&source_id).unwrap(),
            Some(source_snapshot.clone())
        );
        // This test isolates snapshot recovery; the lower-level fixture uses
        // CoreState.assign_agent, so restore that in-memory roster explicitly.
        reopened.assign_agent("agent-model-snapshot");
        let restarted_retry = reopened
            .retry_execution(
                "model-snapshot-retry-after-restart",
                &source_id,
                "retry after restart".into(),
                None,
                None,
            )
            .unwrap();
        assert_eq!(
            reopened
                .model_snapshot(&restarted_retry.id)
                .unwrap()
                .unwrap()
                .revision,
            source_snapshot.revision
        );

        assert!(matches!(
            reopened.retry_execution(
                "model-snapshot-explicit-connector-conflict",
                &source_id,
                "ordinary retry cannot change connector".into(),
                Some("different-profile".into()),
                source_snapshot.model_id.clone(),
            ),
            Err(CoreError::ModelSnapshotConflict)
        ));
        assert!(matches!(
            reopened.retry_execution(
                "model-snapshot-explicit-model-conflict",
                &source_id,
                "ordinary retry cannot change model".into(),
                source_snapshot.connector_id.clone(),
                Some("different-model".into()),
            ),
            Err(CoreError::ModelSnapshotConflict)
        ));
        let explicit_same = reopened
            .retry_execution(
                "model-snapshot-explicit-assertion",
                &source_id,
                "matching explicit values are assertions only".into(),
                source_snapshot.connector_id.clone(),
                source_snapshot.model_id.clone(),
            )
            .unwrap();
        assert_eq!(
            reopened.model_snapshot(&explicit_same.id).unwrap().unwrap(),
            ModelSnapshot {
                run_id: explicit_same.id,
                ..source_snapshot.clone()
            }
        );

        reopened.execution_bindings.insert(
            source_id.clone(),
            ExecutionRuntimeBinding {
                connector_id: "different-profile".into(),
                runtime_type: Some("mock".into()),
                model_id: source_snapshot.model_id.clone(),
                catalog_revision: source_snapshot.revision,
                validate_profile: true,
            },
        );
        assert!(matches!(
            reopened.retry_execution(
                "model-snapshot-conflict",
                &source_id,
                "must fail closed".into(),
                None,
                None,
            ),
            Err(CoreError::ModelSnapshotConflict)
        ));

        drop(reopened);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite3-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite3-shm"));
    }

    #[test]
    fn model_snapshot_restart_keeps_same_name_connector_profile_validation() {
        let path = std::env::temp_dir().join(format!(
            "agenttalk-model-snapshot-same-name-profile-{}.sqlite3",
            unix_time_nanos()
        ));
        let _ = std::fs::remove_file(&path);
        let source_id = {
            let mut core = PersistentCore::open(&path).unwrap();
            core.create_connector_profile(ConnectorProfile {
                scope_id: "desktop".into(),
                connector_id: "mock".into(),
                display_name: "Explicit same-name Mock profile".into(),
                provider_type: "mock".into(),
                runtime_type: "mock".into(),
                enabled: true,
                auth_env_key: None,
            })
            .unwrap();
            core.assign_agent("agent-same-name-profile");
            let source = core
                .start_execution_internal(
                    ExecutionStart {
                        run_id: "model-snapshot-same-name-source".into(),
                        collaboration_run_id: "model-snapshot-same-name-collaboration".into(),
                        project_id: "model-snapshot-same-name-project".into(),
                        conversation_id: "model-snapshot-same-name-conversation".into(),
                        agent_id: "agent-same-name-profile".into(),
                        workspace_access: WorkspaceAccess::None,
                        canonical_cwd: None,
                    },
                    Some("freeze explicit same-name profile".into()),
                    None,
                    Some(ExecutionRuntimeBinding {
                        connector_id: "mock".into(),
                        runtime_type: Some("mock".into()),
                        model_id: Some("mock-default".into()),
                        catalog_revision: None,
                        validate_profile: true,
                    }),
                    None,
                )
                .unwrap();
            source.id
        };

        let reopened = PersistentCore::open(&path).unwrap();
        let binding = reopened.execution_bindings.get(&source_id).unwrap();
        assert_eq!(binding.connector_id, "mock");
        assert!(binding.validate_profile);

        drop(reopened);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite3-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite3-shm"));
    }

    #[test]
    fn ordinary_retry_fails_closed_when_source_snapshot_is_missing() {
        let mut core = PersistentCore::open(":memory:").unwrap();
        core.assign_agent("legacy-agent");
        let legacy_run = ExecutionRun {
            id: "legacy-run-without-snapshot".into(),
            collaboration_run_id: "legacy-collaboration".into(),
            project_id: "legacy-project".into(),
            conversation_id: "legacy-conversation".into(),
            agent_id: "legacy-agent".into(),
            status: ExecutionStatus::Completed,
            version: 1,
            scope: ScopeSnapshot {
                project_id: "legacy-project".into(),
                conversation_id: "legacy-conversation".into(),
                agent_id: "legacy-agent".into(),
                workspace_access: WorkspaceAccess::None,
                canonical_cwd: None,
            },
            terminal_reason: None,
        };
        core.storage
            .persist_execution_run_and_events(&legacy_run, &[])
            .unwrap();
        core.state.restore_run(legacy_run);

        assert!(matches!(
            core.retry_execution(
                "legacy-run-retry",
                "legacy-run-without-snapshot",
                "retry must not silently resolve the current model".into(),
                None,
                None,
            ),
            Err(CoreError::ModelSnapshotMissing)
        ));
    }

    #[test]
    fn persisted_project_assignment_limits_requested_workspace_access() {
        let mut core = CoreState::default();
        core.restore_project_assignment(
            "project-1".into(),
            "agent-1".into(),
            WorkspaceAccess::ReadOnly,
        );
        assert!(matches!(
            core.start_execution(ExecutionStart {
                run_id: "run-denied".into(),
                collaboration_run_id: "collab-1".into(),
                project_id: "project-1".into(),
                conversation_id: "conversation-1".into(),
                agent_id: "agent-1".into(),
                workspace_access: WorkspaceAccess::WorkspaceWrite,
                canonical_cwd: Some("C:\\workspace".into()),
            }),
            Err(CoreError::WorkspaceAccessDenied)
        ));
        let run = core
            .start_execution(ExecutionStart {
                run_id: "run-read-only".into(),
                collaboration_run_id: "collab-1".into(),
                project_id: "project-1".into(),
                conversation_id: "conversation-1".into(),
                agent_id: "agent-1".into(),
                workspace_access: WorkspaceAccess::ReadOnly,
                canonical_cwd: Some("C:\\workspace".into()),
            })
            .unwrap();
        assert_eq!(run.scope.workspace_access, WorkspaceAccess::ReadOnly);
    }

    #[test]
    fn conversation_roster_can_only_shrink_project_roster_and_empty_roster_inherits() {
        let mut core = CoreState::default();
        core.restore_project_assignment(
            "project-1".into(),
            "agent-1".into(),
            WorkspaceAccess::ReadOnly,
        );
        core.restore_project_assignment(
            "project-1".into(),
            "agent-2".into(),
            WorkspaceAccess::None,
        );
        core.restore_conversation_assignment("conversation-1".into(), "agent-1".into(), true);

        assert!(matches!(
            core.start_execution(ExecutionStart {
                run_id: "run-expanded".into(),
                collaboration_run_id: "collab-1".into(),
                project_id: "project-1".into(),
                conversation_id: "conversation-1".into(),
                agent_id: "agent-2".into(),
                workspace_access: WorkspaceAccess::None,
                canonical_cwd: None,
            }),
            Err(CoreError::AgentNotAssigned)
        ));
        assert!(matches!(
            core.start_execution(ExecutionStart {
                run_id: "run-ceiling".into(),
                collaboration_run_id: "collab-1".into(),
                project_id: "project-1".into(),
                conversation_id: "conversation-1".into(),
                agent_id: "agent-1".into(),
                workspace_access: WorkspaceAccess::WorkspaceWrite,
                canonical_cwd: None,
            }),
            Err(CoreError::WorkspaceAccessDenied)
        ));

        core.remove_conversation_assignment("conversation-1", "agent-1");
        let inherited = core
            .start_execution(ExecutionStart {
                run_id: "run-inherited".into(),
                collaboration_run_id: "collab-1".into(),
                project_id: "project-1".into(),
                conversation_id: "conversation-1".into(),
                agent_id: "agent-2".into(),
                workspace_access: WorkspaceAccess::None,
                canonical_cwd: None,
            })
            .unwrap();
        assert_eq!(inherited.scope.agent_id, "agent-2");
    }

    #[test]
    fn persistent_core_recovers_projection_and_event_store_after_restart() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock is before the Unix epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "agenttalk-persistent-core-{}-{}.db",
            std::process::id(),
            nonce
        ));
        {
            let mut core = PersistentCore::open(&path).unwrap();
            core.assign_agent("agent-1");
            let started = core
                .start_execution(ExecutionStart {
                    run_id: "run-persisted".into(),
                    collaboration_run_id: "collab-1".into(),
                    project_id: "project-1".into(),
                    conversation_id: "conversation-1".into(),
                    agent_id: "agent-1".into(),
                    workspace_access: WorkspaceAccess::None,
                    canonical_cwd: None,
                })
                .unwrap();
            assert_eq!(started.status, ExecutionStatus::Completed);
            assert_eq!(started.version, 4);
            assert!(core
                .replay_events(0)
                .unwrap()
                .iter()
                .any(|event| event.event_type == "output.delta"));
            assert!(matches!(
                core.start_execution(ExecutionStart {
                    run_id: "run-persisted".into(),
                    collaboration_run_id: "collab-duplicate".into(),
                    project_id: "project-duplicate".into(),
                    conversation_id: "conversation-duplicate".into(),
                    agent_id: "agent-1".into(),
                    workspace_access: WorkspaceAccess::WorkspaceWrite,
                    canonical_cwd: Some("C:\\duplicate".into()),
                }),
                Err(CoreError::RunAlreadyExists)
            ));
        }
        let mut core = PersistentCore::open(&path).unwrap();
        let recovered = core.recover_run("run-persisted").unwrap().unwrap();
        assert_eq!(recovered.status, ExecutionStatus::Completed);
        assert_eq!(recovered.version, 4);
        assert!(matches!(
            core.transition(
                "run-persisted",
                ExecutionStatus::Failed,
                recovered.version,
                Some("late".into())
            ),
            Err(CoreError::State(
                StateTransitionError::TerminalImmutable { .. }
            ))
        ));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("db-wal"));
        let _ = std::fs::remove_file(path.with_extension("db-shm"));
    }

    #[test]
    fn persistent_core_marks_non_terminal_runs_interrupted_on_restart() {
        let path = std::env::temp_dir().join(format!(
            "agenttalk-persistent-core-interrupted-{}.db",
            std::process::id()
        ));
        {
            let mut storage = agenttalk_storage::SqliteStore::open(&path).unwrap();
            storage
                .upsert_execution_run(&ExecutionRun {
                    id: "run-interrupted".into(),
                    collaboration_run_id: "collab-interrupted".into(),
                    project_id: "project-interrupted".into(),
                    conversation_id: "conversation-interrupted".into(),
                    agent_id: "agent-interrupted".into(),
                    status: ExecutionStatus::Running,
                    version: 1,
                    scope: ScopeSnapshot {
                        project_id: "project-interrupted".into(),
                        conversation_id: "conversation-interrupted".into(),
                        agent_id: "agent-interrupted".into(),
                        workspace_access: WorkspaceAccess::None,
                        canonical_cwd: None,
                    },
                    terminal_reason: None,
                })
                .unwrap();
        }
        let core = PersistentCore::open(&path).unwrap();
        let recovered = core.recover_run("run-interrupted").unwrap().unwrap();
        assert_eq!(recovered.status, ExecutionStatus::Interrupted);
        assert_eq!(
            recovered.terminal_reason.as_deref(),
            Some("core_restarted_before_completion")
        );
        let events = core.replay_events(0).unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| event.event_id == "core-restart-interrupted-run-interrupted")
                .count(),
            1
        );
        drop(core);
        let core = PersistentCore::open(&path).unwrap();
        assert_eq!(
            core.replay_events(0)
                .unwrap()
                .iter()
                .filter(|event| event.event_id == "core-restart-interrupted-run-interrupted")
                .count(),
            1
        );
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("db-wal"));
        let _ = std::fs::remove_file(path.with_extension("db-shm"));
    }

    #[test]
    fn restart_interruption_event_preserves_the_frozen_connector_route() {
        let path = std::env::temp_dir().join(format!(
            "agenttalk-persistent-core-interrupted-route-{}.db",
            std::process::id()
        ));
        let run = ExecutionRun {
            id: "run-interrupted-route".into(),
            collaboration_run_id: "collab-interrupted-route".into(),
            project_id: "project-interrupted-route".into(),
            conversation_id: "conversation-interrupted-route".into(),
            agent_id: "agent-interrupted-route".into(),
            status: ExecutionStatus::Running,
            version: 1,
            scope: ScopeSnapshot {
                project_id: "project-interrupted-route".into(),
                conversation_id: "conversation-interrupted-route".into(),
                agent_id: "agent-interrupted-route".into(),
                workspace_access: WorkspaceAccess::None,
                canonical_cwd: None,
            },
            terminal_reason: None,
        };
        let snapshot = ModelSnapshot {
            run_id: run.id.clone(),
            connector_id: Some("codex-fixture".into()),
            model_id: Some("codex-model-a".into()),
            revision: Some(7),
        };
        let selection = ModelSelectionSnapshot {
            run_id: run.id.clone(),
            version: 2,
            runtime_type: "codex".into(),
            provider_type: "codex".into(),
            connector_id: "codex-fixture".into(),
            effective_model_id: Some("codex-model-a".into()),
            selection_source: ModelSelectionSource::IdentityDefault,
            selection_mode: ModelSelectionMode::Pinned,
            availability: ModelAvailability::Unverified,
            catalog_revision: Some("fixture-catalog-revision".into()),
            context_window: Some(128_000),
            reasoning_efforts: vec!["medium".into()],
            service_tiers: Vec::new(),
            candidate_model_list: None,
        };
        {
            let mut storage = agenttalk_storage::SqliteStore::open(&path).unwrap();
            storage
                .persist_execution_run_and_model_snapshots_and_events(
                    &run,
                    &snapshot,
                    &selection,
                    &[],
                )
                .unwrap();
        }

        let core = PersistentCore::open(&path).unwrap();
        let event = core
            .replay_events(0)
            .unwrap()
            .into_iter()
            .find(|event| event.event_id == "core-restart-interrupted-run-interrupted-route")
            .expect("restart must append one interruption event");
        assert_eq!(
            event.payload,
            json!({
                "reason": "core_restarted_before_completion",
                "connectorId": "codex-fixture",
                "runtimeType": "codex",
                "modelId": "codex-model-a",
                "catalogRevision": 7,
            })
        );
        drop(core);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("db-wal"));
        let _ = std::fs::remove_file(path.with_extension("db-shm"));
    }

    #[test]
    fn persistent_core_fails_closed_when_runtime_adapter_is_unavailable() {
        let mut core = PersistentCore::open_with_runtime(
            ":memory:",
            Box::new(agenttalk_runtime_host::ConfiguredAdapter::http_custom()),
        )
        .unwrap();
        core.assign_agent("agent-runtime-error");
        let run = core
            .start_execution(ExecutionStart {
                run_id: "run-runtime-error".into(),
                collaboration_run_id: "collab-runtime-error".into(),
                project_id: "project-runtime-error".into(),
                conversation_id: "conversation-runtime-error".into(),
                agent_id: "agent-runtime-error".into(),
                workspace_access: WorkspaceAccess::None,
                canonical_cwd: None,
            })
            .unwrap();
        assert_eq!(run.status, ExecutionStatus::Failed);
        assert_eq!(run.terminal_reason.as_deref(), Some("unsupported"));
        assert!(core.replay_events(0).unwrap().iter().any(|event| {
            event.execution_run_id == "run-runtime-error"
                && event.event_type == "execution.failed"
                && event.payload["reason"] == "unsupported"
        }));
    }

    #[test]
    fn identical_context_is_frozen_as_distinct_manifests_per_run() {
        let mut core = PersistentCore::open(":memory:").unwrap();
        core.assign_agent("agent-context");
        let execution = |run_id: &str| ExecutionStart {
            run_id: run_id.into(),
            collaboration_run_id: format!("collaboration-{run_id}"),
            project_id: "project-context".into(),
            conversation_id: "conversation-context".into(),
            agent_id: "agent-context".into(),
            workspace_access: WorkspaceAccess::None,
            canonical_cwd: None,
        };

        core.start_execution_internal(
            execution("run-context-1"),
            Some("same task".into()),
            None,
            None,
            None,
        )
        .unwrap();
        core.start_execution_internal(
            execution("run-context-2"),
            Some("same task".into()),
            None,
            None,
            None,
        )
        .unwrap();

        let snapshot = core.projection_snapshot().unwrap();
        let manifests = snapshot["contextManifests"].as_array().unwrap().clone();
        assert_eq!(manifests.len(), 2);
        assert_ne!(manifests[0]["id"], manifests[1]["id"]);
        let manifest_run_ids: Vec<_> = manifests
            .iter()
            .map(|manifest| manifest["executionRunId"].as_str().unwrap())
            .collect();
        assert!(manifest_run_ids.contains(&"run-context-1"));
        assert!(manifest_run_ids.contains(&"run-context-2"));
        assert!(manifests.iter().all(|manifest| manifest["sourceLedger"]
            .as_array()
            .is_some_and(|ledger| !ledger.is_empty())));
        let events = core
            .replay_events(0)
            .unwrap()
            .into_iter()
            .filter(|event| event.execution_run_id == "run-context-1")
            .map(|event| event.event_type)
            .collect::<Vec<_>>();
        let scope_index = events.iter().position(|event| event == "scope.frozen");
        let assembled_index = events.iter().position(|event| event == "context.assembled");
        let sealed_index = events.iter().position(|event| event == "context.sealed");
        let runtime_index = events.iter().position(|event| event == "runtime.started");
        assert!(scope_index < assembled_index && assembled_index < sealed_index);
        assert!(sealed_index < runtime_index);
    }

    #[test]
    fn persistent_core_persists_product_projection_and_assignment_commands() {
        let mut core = PersistentCore::open(":memory:").unwrap();
        core.create_project(Project {
            id: "project-1".into(),
            name: "Project".into(),
            root_path: Some("C:\\workspace".into()),
            archived: false,
        })
        .unwrap();
        core.create_agent(AgentIdentity {
            id: "agent-1".into(),
            name: "Agent".into(),
            role: "builder".into(),
            specialty: "code".into(),
            system_prompt: "system".into(),
        })
        .unwrap();
        core.create_conversation(Conversation {
            id: "conversation-1".into(),
            project_id: "project-1".into(),
            title: "Conversation".into(),
            scope_revision: 0,
        })
        .unwrap();
        core.create_message(Message {
            id: "message-1".into(),
            conversation_id: "conversation-1".into(),
            sender_id: "user".into(),
            sequence: 1,
            content: "Hello Core".into(),
        })
        .unwrap();
        core.set_project_agent_assignment("project-1", "agent-1", true, WorkspaceAccess::ReadOnly)
            .unwrap();
        let snapshot = core.projection_snapshot().unwrap();
        assert_eq!(snapshot["projects"][0]["id"], "project-1");
        assert_eq!(snapshot["conversations"][0]["projectId"], "project-1");
        assert_eq!(snapshot["messages"][0]["content"], "Hello Core");
        assert_eq!(snapshot["assignments"][0]["workspaceAccess"], "read_only");
        core.remove_project_agent_assignment("project-1", "agent-1")
            .unwrap();
        assert!(core.projection_snapshot().unwrap()["assignments"]
            .as_array()
            .unwrap()
            .is_empty());
    }

    #[test]
    fn memory_command_is_scope_checked_idempotent_and_survives_reopen() {
        let path = std::env::temp_dir().join(format!(
            "agenttalk-memory-command-{}.sqlite3",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let memory = MemoryItem {
            id: "memory-command-1".into(),
            scope_id: "project-memory-command".into(),
            agent_id: Some("agent-memory-command".into()),
            content_hash: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into(),
            confirmed: false,
        };
        {
            let mut core = PersistentCore::open(&path).unwrap();
            core.create_project(Project {
                id: "project-memory-command".into(),
                name: "Memory Project".into(),
                root_path: None,
                archived: false,
            })
            .unwrap();
            core.create_agent(AgentIdentity {
                id: "agent-memory-command".into(),
                name: "Memory Agent".into(),
                role: "role".into(),
                specialty: "specialty".into(),
                system_prompt: "system".into(),
            })
            .unwrap();
            assert_eq!(
                core.store_memory(StoreMemoryCommand {
                    memory: memory.clone(),
                })
                .unwrap(),
                MemoryWriteOutcome::Created
            );
            assert_eq!(
                core.store_memory(StoreMemoryCommand {
                    memory: memory.clone(),
                })
                .unwrap(),
                MemoryWriteOutcome::AlreadyPresent
            );
            assert!(matches!(
                core.store_memory(StoreMemoryCommand {
                    memory: MemoryItem {
                        scope_id: "missing-scope".into(),
                        ..memory.clone()
                    },
                }),
                Err(CoreError::MemoryScopeNotFound)
            ));
            assert!(matches!(
                core.store_memory(StoreMemoryCommand {
                    memory: MemoryItem {
                        agent_id: Some("missing-agent".into()),
                        ..memory.clone()
                    },
                }),
                Err(CoreError::MemoryAgentNotFound)
            ));
        }

        let core = PersistentCore::open(&path).unwrap();
        assert_eq!(
            core.projection_snapshot().unwrap()["memories"],
            json!([{
                "id": "memory-command-1",
                "scopeId": "project-memory-command",
                "agentId": "agent-memory-command",
                "contentHash": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "confirmed": false,
            }])
        );
        drop(core);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite3-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite3-shm"));
    }

    #[test]
    fn workflow_command_is_project_scoped_idempotent_and_survives_reopen() {
        let path = std::env::temp_dir().join(format!(
            "agenttalk-workflow-command-{}.sqlite3",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let workflow = WorkflowTemplate {
            id: "workflow-command-1".into(),
            name: "Build and review".into(),
            kind: "linear".into(),
            steps: vec![agenttalk_domain::WorkflowStep {
                id: "step-1".into(),
                order: 1,
                agent_id: "agent-workflow-command".into(),
                prompt_supplement: Some("review the change".into()),
            }],
        };

        {
            let mut core = PersistentCore::open(&path).unwrap();
            core.create_project(Project {
                id: "project-workflow-command".into(),
                name: "Workflow Project".into(),
                root_path: None,
                archived: false,
            })
            .unwrap();
            core.create_agent(AgentIdentity {
                id: "agent-workflow-command".into(),
                name: "Workflow Agent".into(),
                role: "builder".into(),
                specialty: "code".into(),
                system_prompt: "system".into(),
            })
            .unwrap();
            core.create_agent(AgentIdentity {
                id: "agent-not-rostered".into(),
                name: "Unassigned Agent".into(),
                role: "reviewer".into(),
                specialty: "review".into(),
                system_prompt: "system".into(),
            })
            .unwrap();
            assert!(matches!(
                core.create_workflow(CreateWorkflowCommand {
                    project_id: "missing-project".into(),
                    workflow: workflow.clone(),
                }),
                Err(CoreError::WorkflowProjectNotFound)
            ));
            assert!(matches!(
                core.create_workflow(CreateWorkflowCommand {
                    project_id: "project-workflow-command".into(),
                    workflow: WorkflowTemplate {
                        steps: vec![agenttalk_domain::WorkflowStep {
                            agent_id: "agent-not-rostered".into(),
                            ..workflow.steps[0].clone()
                        }],
                        ..workflow.clone()
                    },
                }),
                Err(CoreError::WorkflowAgentNotInProject)
            ));
            core.set_project_agent_assignment(
                "project-workflow-command",
                "agent-workflow-command",
                true,
                WorkspaceAccess::None,
            )
            .unwrap();
            assert_eq!(
                core.create_workflow(CreateWorkflowCommand {
                    project_id: "project-workflow-command".into(),
                    workflow: workflow.clone(),
                })
                .unwrap(),
                WorkflowWriteOutcome::Created
            );
            assert_eq!(
                core.create_workflow(CreateWorkflowCommand {
                    project_id: "project-workflow-command".into(),
                    workflow: workflow.clone(),
                })
                .unwrap(),
                WorkflowWriteOutcome::AlreadyPresent
            );
            let mut conflicting = workflow.clone();
            conflicting.kind = "fan-out".into();
            assert!(matches!(
                core.create_workflow(CreateWorkflowCommand {
                    project_id: "project-workflow-command".into(),
                    workflow: conflicting,
                }),
                Err(CoreError::WorkflowConflict)
            ));
            assert_eq!(
                core.projection_snapshot().unwrap()["workflows"]
                    .as_array()
                    .unwrap()
                    .len(),
                1
            );
        }

        let mut core = PersistentCore::open(&path).unwrap();
        let snapshot = core.projection_snapshot().unwrap();
        assert_eq!(snapshot["workflows"][0]["id"], "workflow-command-1");
        assert_eq!(
            snapshot["workflows"][0]["projectId"],
            "project-workflow-command"
        );
        assert_eq!(
            core.create_workflow(CreateWorkflowCommand {
                project_id: "project-workflow-command".into(),
                workflow,
            })
            .unwrap(),
            WorkflowWriteOutcome::AlreadyPresent
        );
        drop(core);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite3-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite3-shm"));
    }

    #[test]
    fn persistent_core_recovers_conversation_assignment_without_prompt_projection() {
        let path = std::env::temp_dir().join(format!(
            "agenttalk-conversation-assignment-{}.db",
            std::process::id()
        ));
        {
            let mut core = PersistentCore::open(&path).unwrap();
            core.create_project(Project {
                id: "project-conversation".into(),
                name: "Project".into(),
                root_path: None,
                archived: false,
            })
            .unwrap();
            for agent_id in ["agent-conversation-1", "agent-conversation-2"] {
                core.create_agent(AgentIdentity {
                    id: agent_id.into(),
                    name: "Agent".into(),
                    role: "builder".into(),
                    specialty: "code".into(),
                    system_prompt: "system".into(),
                })
                .unwrap();
            }
            core.create_conversation(Conversation {
                id: "conversation-assignment".into(),
                project_id: "project-conversation".into(),
                title: "Conversation".into(),
                scope_revision: 0,
            })
            .unwrap();
            core.set_project_agent_assignment(
                "project-conversation",
                "agent-conversation-1",
                true,
                WorkspaceAccess::ReadOnly,
            )
            .unwrap();
            core.set_project_agent_assignment(
                "project-conversation",
                "agent-conversation-2",
                true,
                WorkspaceAccess::None,
            )
            .unwrap();
            assert!(matches!(
                core.set_conversation_agent_assignment(
                    "conversation-assignment",
                    "agent-not-in-project",
                    true,
                ),
                Err(CoreError::AgentNotAssigned)
            ));
            core.set_conversation_agent_assignment(
                "conversation-assignment",
                "agent-conversation-1",
                true,
            )
            .unwrap();
            let snapshot = core.projection_snapshot().unwrap();
            let assignment = &snapshot["conversationAgents"][0];
            assert_eq!(assignment["conversationId"], "conversation-assignment");
            assert_eq!(assignment["agentId"], "agent-conversation-1");
            assert_eq!(assignment["enabled"], true);
            assert!(assignment.get("systemPrompt").is_none());
            assert!(matches!(
                core.start_execution(ExecutionStart {
                    run_id: "run-conversation-expanded".into(),
                    collaboration_run_id: "collab-conversation".into(),
                    project_id: "project-conversation".into(),
                    conversation_id: "conversation-assignment".into(),
                    agent_id: "agent-conversation-2".into(),
                    workspace_access: WorkspaceAccess::None,
                    canonical_cwd: None,
                }),
                Err(CoreError::AgentNotAssigned)
            ));
            assert!(matches!(
                core.start_execution(ExecutionStart {
                    run_id: "run-conversation-ceiling".into(),
                    collaboration_run_id: "collab-conversation".into(),
                    project_id: "project-conversation".into(),
                    conversation_id: "conversation-assignment".into(),
                    agent_id: "agent-conversation-1".into(),
                    workspace_access: WorkspaceAccess::WorkspaceWrite,
                    canonical_cwd: None,
                }),
                Err(CoreError::WorkspaceAccessDenied)
            ));
        }

        let mut core = PersistentCore::open(&path).unwrap();
        assert_eq!(
            core.projection_snapshot().unwrap()["conversationAgents"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert!(core
            .start_execution(ExecutionStart {
                run_id: "run-conversation-restored".into(),
                collaboration_run_id: "collab-conversation".into(),
                project_id: "project-conversation".into(),
                conversation_id: "conversation-assignment".into(),
                agent_id: "agent-conversation-1".into(),
                workspace_access: WorkspaceAccess::None,
                canonical_cwd: None,
            })
            .is_ok());
        assert!(matches!(
            core.start_execution(ExecutionStart {
                run_id: "run-conversation-restored-expanded".into(),
                collaboration_run_id: "collab-conversation".into(),
                project_id: "project-conversation".into(),
                conversation_id: "conversation-assignment".into(),
                agent_id: "agent-conversation-2".into(),
                workspace_access: WorkspaceAccess::None,
                canonical_cwd: None,
            }),
            Err(CoreError::AgentNotAssigned)
        ));
        core.remove_conversation_agent_assignment(
            "conversation-assignment",
            "agent-conversation-1",
        )
        .unwrap();
        assert!(core
            .start_execution(ExecutionStart {
                run_id: "run-conversation-inherited".into(),
                collaboration_run_id: "collab-conversation".into(),
                project_id: "project-conversation".into(),
                conversation_id: "conversation-assignment".into(),
                agent_id: "agent-conversation-2".into(),
                workspace_access: WorkspaceAccess::None,
                canonical_cwd: None,
            })
            .is_ok());
        drop(core);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("db-wal"));
        let _ = std::fs::remove_file(path.with_extension("db-shm"));
    }

    #[test]
    fn summary_and_artifact_metadata_cross_core_boundary_without_bodies() {
        let mut core = PersistentCore::open(":memory:").unwrap();
        core.create_project(Project {
            id: "summary-artifact-project".into(),
            name: "Summary Artifact".into(),
            root_path: None,
            archived: false,
        })
        .unwrap();
        core.create_conversation(Conversation {
            id: "summary-artifact-conversation".into(),
            project_id: "summary-artifact-project".into(),
            title: "Conversation".into(),
            scope_revision: 0,
        })
        .unwrap();

        assert_eq!(
            core.store_summary(StoreSummaryCommand {
                summary: Summary {
                    id: "summary-core-1".into(),
                    scope_id: "summary-artifact-conversation".into(),
                    version: 1,
                    content_hash: "a".repeat(64),
                    artifact_id: None,
                },
            })
            .unwrap(),
            SummaryWriteOutcome::Created
        );
        assert_eq!(
            core.store_artifact(StoreArtifactCommand {
                artifact: Artifact {
                    id: "artifact-core-1".into(),
                    sha256: "b".repeat(64),
                    size: 7,
                    mime: "text/plain".into(),
                    relative_path: Some("notes/core.txt".into()),
                },
            })
            .unwrap(),
            ArtifactWriteOutcome::Created
        );
        let snapshot = core.projection_snapshot().unwrap();
        assert_eq!(snapshot["summaries"][0]["id"], "summary-core-1");
        assert_eq!(snapshot["artifacts"][0]["id"], "artifact-core-1");
        assert!(!serde_json::to_string(&snapshot).unwrap().contains("body"));
    }

    #[test]
    fn attachment_association_crosses_core_boundary_without_body_projection() {
        let mut core = PersistentCore::open(":memory:").unwrap();
        core.create_project(Project {
            id: "attachment-core-project".into(),
            name: "Attachments".into(),
            root_path: None,
            archived: false,
        })
        .unwrap();
        core.create_conversation(Conversation {
            id: "attachment-core-conversation".into(),
            project_id: "attachment-core-project".into(),
            title: "Conversation".into(),
            scope_revision: 0,
        })
        .unwrap();
        core.create_message(Message {
            id: "attachment-core-message".into(),
            conversation_id: "attachment-core-conversation".into(),
            sender_id: "user".into(),
            sequence: 1,
            content: "file".into(),
        })
        .unwrap();
        let artifact = Artifact {
            id: "attachment-core-artifact".into(),
            sha256: "c".repeat(64),
            size: 3,
            mime: "text/plain".into(),
            relative_path: None,
        };
        assert_eq!(
            core.store_artifact(StoreArtifactCommand { artifact })
                .unwrap(),
            ArtifactWriteOutcome::Created
        );
        let attachment = Attachment {
            id: "attachment-core-1".into(),
            message_id: "attachment-core-message".into(),
            artifact_id: "attachment-core-artifact".into(),
            file_name: "file.txt".into(),
            sha256: "c".repeat(64),
            size: 3,
        };
        assert_eq!(
            core.store_attachment(StoreAttachmentCommand {
                attachment: attachment.clone(),
                ordinal: 0,
            })
            .unwrap(),
            AttachmentWriteOutcome::Created
        );
        assert_eq!(
            core.store_attachment(StoreAttachmentCommand {
                attachment,
                ordinal: 0,
            })
            .unwrap(),
            AttachmentWriteOutcome::AlreadyPresent
        );
        let snapshot = core.projection_snapshot().unwrap();
        assert_eq!(
            snapshot["attachments"][0]["attachmentId"],
            "attachment-core-1"
        );
        assert_eq!(
            snapshot["attachments"][0]["artifactId"],
            "attachment-core-artifact"
        );
        assert!(!serde_json::to_string(&snapshot)
            .unwrap()
            .contains("file contents"));
    }

    #[test]
    fn selected_attachment_file_import_is_body_free_and_restart_recoverable() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let base = std::env::temp_dir().join(format!(
            "agenttalk-core-attachment-import-{}-{nonce}",
            std::process::id()
        ));
        let database = base.join("core.sqlite3");
        let root = base.join("artifacts");
        let source = base.join("selected-large.txt");
        let body = vec![b'a'; 600 * 1024];
        std::fs::create_dir_all(&base).unwrap();
        std::fs::write(&source, &body).unwrap();

        let mut core = PersistentCore::open_with_artifact_root(&database, &root).unwrap();
        core.create_project(Project {
            id: "attachment-import-project".into(),
            name: "Attachment import".into(),
            root_path: None,
            archived: false,
        })
        .unwrap();
        core.create_conversation(Conversation {
            id: "attachment-import-conversation".into(),
            project_id: "attachment-import-project".into(),
            title: "Conversation".into(),
            scope_revision: 0,
        })
        .unwrap();
        core.create_message(Message {
            id: "attachment-import-message".into(),
            conversation_id: "attachment-import-conversation".into(),
            sender_id: "user".into(),
            sequence: 1,
            content: "with selected file".into(),
        })
        .unwrap();

        let command = ImportAttachmentFileCommand {
            attachment_id: "attachment-import-1".into(),
            artifact_id: "artifact-import-1".into(),
            message_id: "attachment-import-message".into(),
            source_path: source.clone(),
            mime: "text/plain".into(),
            ordinal: 0,
        };
        let outcome = core.import_attachment_file(command.clone()).unwrap();
        assert_eq!(outcome.artifact_outcome, ArtifactWriteOutcome::Created);
        assert_eq!(outcome.attachment_outcome, AttachmentWriteOutcome::Created);
        assert!(outcome.body_stored);
        assert_eq!(outcome.artifact.size, body.len() as u64);
        assert_eq!(outcome.attachment.file_name, "selected-large.txt");
        let replay = core.import_attachment_file(command).unwrap();
        assert_eq!(
            replay.artifact_outcome,
            ArtifactWriteOutcome::AlreadyPresent
        );
        assert_eq!(
            replay.attachment_outcome,
            AttachmentWriteOutcome::AlreadyPresent
        );
        assert!(!replay.body_stored);
        let projection = core.projection_snapshot().unwrap();
        let serialized = serde_json::to_string(&projection).unwrap();
        assert!(!serialized.contains(&source.to_string_lossy().to_string()));
        assert!(!serialized.contains(&"a".repeat(256)));
        assert_eq!(core.load_artifact_body(&outcome.artifact.id).unwrap(), body);

        drop(core);
        std::fs::remove_file(&source).unwrap();
        let reopened = PersistentCore::open_with_artifact_root(&database, &root).unwrap();
        assert_eq!(
            reopened.load_artifact_body(&outcome.artifact.id).unwrap(),
            body
        );
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn artifact_body_crosses_core_boundary_only_with_explicit_store_root() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let database = std::env::temp_dir().join(format!(
            "agenttalk-core-artifact-body-{}-{}/core.sqlite3",
            std::process::id(),
            nonce
        ));
        let root = database.parent().unwrap().join("artifacts");
        let body = "core artifact body";
        let artifact = Artifact {
            id: "artifact-core-body".into(),
            sha256: sha256_hex(body),
            size: body.len() as u64,
            mime: "text/plain".into(),
            relative_path: Some("notes/core.txt".into()),
        };
        std::fs::create_dir_all(database.parent().unwrap()).unwrap();
        {
            let mut core = PersistentCore::open_with_artifact_root(&database, &root).unwrap();
            assert_eq!(
                core.store_artifact(StoreArtifactCommand {
                    artifact: artifact.clone(),
                })
                .unwrap(),
                ArtifactWriteOutcome::Created
            );
            assert!(core
                .store_artifact_body(StoreArtifactBodyCommand {
                    artifact_id: artifact.id.clone(),
                    body: body.as_bytes().to_vec(),
                })
                .unwrap());
            assert_eq!(
                core.load_artifact_body(&artifact.id).unwrap(),
                body.as_bytes()
            );
        }
        let core = PersistentCore::open_with_artifact_root(&database, &root).unwrap();
        assert_eq!(
            core.load_artifact_body(&artifact.id).unwrap(),
            body.as_bytes()
        );
        let snapshot = serde_json::to_string(&core.projection_snapshot().unwrap()).unwrap();
        assert!(!snapshot.contains(body));
        let _ = std::fs::remove_dir_all(database.parent().unwrap());
    }

    #[test]
    fn artifact_body_chunk_reads_are_bounded_at_the_core_boundary() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let database = std::env::temp_dir().join(format!(
            "agenttalk-core-artifact-chunk-{}-{nonce}/core.sqlite3",
            std::process::id()
        ));
        let root = database.parent().unwrap().join("artifacts");
        let body: Vec<u8> = (0..(agenttalk_storage::ARTIFACT_CONTENT_CHUNK_MAX_BYTES as usize + 5))
            .map(|index| (index % 239) as u8)
            .collect();
        let digest = Sha256::digest(&body);
        let sha256 = digest
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let artifact = Artifact {
            id: "artifact-core-chunk".into(),
            sha256,
            size: body.len() as u64,
            mime: "application/octet-stream".into(),
            relative_path: None,
        };
        std::fs::create_dir_all(database.parent().unwrap()).unwrap();
        let mut core = PersistentCore::open_with_artifact_root(&database, &root).unwrap();
        assert_eq!(
            core.store_artifact(StoreArtifactCommand {
                artifact: artifact.clone(),
            })
            .unwrap(),
            ArtifactWriteOutcome::Created
        );
        assert!(core
            .store_artifact_body(StoreArtifactBodyCommand {
                artifact_id: artifact.id.clone(),
                body: body.clone(),
            })
            .unwrap());
        let first = core
            .read_artifact_body_chunk(
                &artifact.id,
                0,
                agenttalk_storage::ARTIFACT_CONTENT_CHUNK_MAX_BYTES,
            )
            .unwrap();
        assert_eq!(
            first.bytes.len() as u64,
            agenttalk_storage::ARTIFACT_CONTENT_CHUNK_MAX_BYTES
        );
        assert!(!first.eof);
        let last = core
            .read_artifact_body_chunk(
                &artifact.id,
                first.bytes.len() as u64,
                agenttalk_storage::ARTIFACT_CONTENT_CHUNK_MAX_BYTES,
            )
            .unwrap();
        assert_eq!(last.bytes, body[body.len() - 5..]);
        assert!(last.eof);
        let _ = std::fs::remove_dir_all(database.parent().unwrap());
    }

    #[test]
    fn generated_summary_is_local_versioned_artifact_content_and_body_free_in_projection() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let database = std::env::temp_dir().join(format!(
            "agenttalk-core-summary-generation-{}-{nonce}/core.sqlite3",
            std::process::id()
        ));
        let root = database.parent().unwrap().join("artifacts");
        std::fs::create_dir_all(database.parent().unwrap()).unwrap();
        let mut core = PersistentCore::open_with_artifact_root(&database, &root).unwrap();
        core.create_project(Project {
            id: "summary-generation-project".into(),
            name: "Summary".into(),
            root_path: None,
            archived: false,
        })
        .unwrap();
        core.create_conversation(Conversation {
            id: "summary-generation-conversation".into(),
            project_id: "summary-generation-project".into(),
            title: "Summary".into(),
            scope_revision: 0,
        })
        .unwrap();
        core.create_message(Message {
            id: "summary-generation-message-1".into(),
            conversation_id: "summary-generation-conversation".into(),
            sender_id: "user".into(),
            sequence: 1,
            content: "First durable message".into(),
        })
        .unwrap();
        core.create_message(Message {
            id: "summary-generation-message-2".into(),
            conversation_id: "summary-generation-conversation".into(),
            sender_id: "user".into(),
            sequence: 2,
            content: "Second durable message".into(),
        })
        .unwrap();

        let first = core
            .generate_summary(GenerateSummaryCommand {
                scope_id: "summary-generation-conversation".into(),
            })
            .unwrap();
        assert_eq!(first.generator, SUMMARY_GENERATOR_VERSION);
        assert_eq!(first.message_count, 2);
        assert_eq!(first.summary.version, 1);
        assert!(first.summary.artifact_id.is_some());
        let content = core.load_summary_content(&first.summary.id).unwrap();
        assert!(content.contains("First durable message"));
        assert!(content.contains("Second durable message"));

        let second = core
            .generate_summary(GenerateSummaryCommand {
                scope_id: "summary-generation-conversation".into(),
            })
            .unwrap();
        assert_eq!(second.summary.version, 2);
        assert_ne!(first.summary.id, second.summary.id);
        let projection = core.projection_snapshot().unwrap();
        let summaries = serde_json::to_string(&projection["summaries"]).unwrap();
        assert!(!summaries.contains("First durable message"));
        assert!(!summaries.contains("Second durable message"));
        drop(core);
        let reopened = PersistentCore::open_with_artifact_root(&database, &root).unwrap();
        assert!(reopened
            .load_summary_content(&first.summary.id)
            .unwrap()
            .contains("First durable message"));
        let _ = std::fs::remove_dir_all(database.parent().unwrap());
    }

    #[test]
    fn projection_mutation_emits_a_replayable_cursor_event() {
        let mut core = PersistentCore::open(":memory:").unwrap();
        core.record_projection_changed("created").unwrap();
        let events = core.replay_events(0).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "projection.changed");
        assert_eq!(events[0].sequence, 1);
        assert_eq!(core.replay_events(1).unwrap().len(), 0);
    }

    #[test]
    fn retrieval_preview_is_scoped_through_core_and_rejects_cross_project_requests() {
        let mut core = PersistentCore::open(":memory:").unwrap();
        core.create_project(Project {
            id: "preview-core-project".into(),
            name: "Preview Core".into(),
            root_path: None,
            archived: false,
        })
        .unwrap();
        core.create_agent(AgentIdentity {
            id: "preview-core-agent".into(),
            name: "Preview Agent".into(),
            role: "reviewer".into(),
            specialty: "retrieval".into(),
            system_prompt: "do not persist this prompt".into(),
        })
        .unwrap();
        core.create_conversation(agenttalk_domain::Conversation {
            id: "preview-core-conversation".into(),
            project_id: "preview-core-project".into(),
            title: "Preview".into(),
            scope_revision: 0,
        })
        .unwrap();
        core.set_project_agent_assignment(
            "preview-core-project",
            "preview-core-agent",
            true,
            WorkspaceAccess::ReadOnly,
        )
        .unwrap();
        core.create_message(Message {
            id: "preview-core-message".into(),
            conversation_id: "preview-core-conversation".into(),
            sender_id: "preview-core-agent".into(),
            sequence: 1,
            content: "Core exact retrieval phrase".into(),
        })
        .unwrap();

        let result = core
            .preview_retrieval(RetrievalPreviewRequest {
                expected_project_id: "preview-core-project".into(),
                conversation_id: "preview-core-conversation".into(),
                agent_id: "preview-core-agent".into(),
                query: "retrieval phrase".into(),
                scope: "conversation".into(),
                source_types: vec!["message".into()],
                limit: 10,
            })
            .unwrap();
        assert_eq!(result["retrievalVersion"], "exact-retrieval-v1");
        assert_eq!(result["hits"].as_array().unwrap().len(), 1);
        assert_eq!(result["hits"][0]["permissionDecision"], "not_applicable");

        struct UnavailableEmbeddingProvider;
        impl agenttalk_storage::RetrievalEmbeddingProvider for UnavailableEmbeddingProvider {
            fn descriptor(&self) -> agenttalk_storage::RetrievalEmbeddingDescriptor {
                agenttalk_storage::RetrievalEmbeddingDescriptor {
                    provider_id: "core-fixture-provider".into(),
                    retrieval_version: "core-fixture-vector-v1".into(),
                    dimension: 2,
                    verification:
                        agenttalk_storage::RetrievalEmbeddingVerification::VerifiedProvider,
                }
            }

            fn embed(
                &self,
                _text: &str,
            ) -> Result<Vec<f64>, agenttalk_storage::RetrievalEmbeddingError> {
                Err(agenttalk_storage::RetrievalEmbeddingError::Unavailable)
            }
        }
        assert!(matches!(
            core.preview_retrieval_vector_with_provider(
                RetrievalPreviewRequest {
                    expected_project_id: "preview-core-project".into(),
                    conversation_id: "preview-core-conversation".into(),
                    agent_id: "preview-core-agent".into(),
                    query: "retrieval phrase".into(),
                    scope: "conversation".into(),
                    source_types: vec!["message".into()],
                    limit: 10,
                },
                &UnavailableEmbeddingProvider,
            ),
            Err(CoreError::RetrievalPreviewRejected)
        ));
        assert!(matches!(
            core.preview_retrieval(RetrievalPreviewRequest {
                expected_project_id: "wrong-project".into(),
                conversation_id: "preview-core-conversation".into(),
                agent_id: "preview-core-agent".into(),
                query: "retrieval".into(),
                scope: "conversation".into(),
                source_types: vec!["message".into()],
                limit: 10,
            }),
            Err(CoreError::RetrievalPreviewRejected)
        ));
    }

    #[test]
    fn retrieval_selection_and_feedback_are_consumable_through_core_scope_boundary() {
        let mut core = PersistentCore::open(":memory:").unwrap();
        core.create_project(Project {
            id: "retrieval-core-project".into(),
            name: "Retrieval Core".into(),
            root_path: None,
            archived: false,
        })
        .unwrap();
        core.create_conversation(agenttalk_domain::Conversation {
            id: "retrieval-core-conversation".into(),
            project_id: "retrieval-core-project".into(),
            title: "Retrieval Core".into(),
            scope_revision: 0,
        })
        .unwrap();
        let source = RetrievalSource {
            id: "retrieval-core-source".into(),
            scope_id: "retrieval-core-conversation".into(),
            citation: "docs/exact.md#selection".into(),
            sha256: "e".repeat(64),
            token_count: 8,
        };
        core.store_retrieval_source(StoreRetrievalSourceCommand {
            source: source.clone(),
        })
        .unwrap();
        let selection = RetrievalSelection {
            id: "retrieval-core-selection".into(),
            scope: agenttalk_domain::RetrievalSelectionScope::Conversation,
            scope_id: "retrieval-core-conversation".into(),
            project_id: "retrieval-core-project".into(),
            conversation_id: Some("retrieval-core-conversation".into()),
            scope_revision: 0,
            workspace_revision: None,
            retrieval_version: "exact-retrieval-v1".into(),
            query_hash: "f".repeat(64),
            items: vec![agenttalk_domain::RetrievalSelectionItem {
                source_id: source.id.clone(),
                source_hash: source.sha256.clone(),
                rank: 1,
                score_milli: 900,
                match_method: agenttalk_domain::RetrievalMatchMethod::ExactTerms,
                reason: agenttalk_domain::RetrievalSelectionReason::ExactTerms,
                range: None,
            }],
        };
        assert_eq!(
            core.store_retrieval_selection(StoreRetrievalSelectionCommand {
                selection: selection.clone(),
            })
            .unwrap(),
            RetrievalSelectionWriteOutcome::Created
        );
        assert_eq!(
            core.query_retrieval_selections("retrieval-core-conversation", None, 10)
                .unwrap()
                .len(),
            1
        );
        assert!(core
            .query_retrieval_selections("unscoped-retrieval-query", None, 10)
            .is_err());
        assert_eq!(
            core.store_retrieval_feedback(StoreRetrievalFeedbackCommand {
                feedback: RetrievalFeedback {
                    id: "retrieval-core-feedback".into(),
                    selection_id: selection.id,
                    scope_id: "retrieval-core-conversation".into(),
                    source_id: source.id,
                    label: agenttalk_domain::RetrievalFeedbackLabel::Helpful,
                    reason: agenttalk_domain::RetrievalFeedbackReason::ExactMatch,
                    created_at_ms: 1,
                },
            })
            .unwrap(),
            RetrievalFeedbackWriteOutcome::Created
        );
    }

    #[test]
    fn handoff_transition_is_project_scoped_idempotent_and_terminally_immutable() {
        let mut core = PersistentCore::open(":memory:").unwrap();
        core.create_project(Project {
            id: "handoff-project".into(),
            name: "Handoff Project".into(),
            root_path: None,
            archived: false,
        })
        .unwrap();
        core.create_agent(AgentIdentity {
            id: "handoff-agent".into(),
            name: "Handoff Agent".into(),
            role: "reviewer".into(),
            specialty: "handoff".into(),
            system_prompt: "handoff".into(),
        })
        .unwrap();
        core.create_agent(AgentIdentity {
            id: "handoff-target-agent".into(),
            name: "Handoff Target".into(),
            role: "reviewer".into(),
            specialty: "handoff".into(),
            system_prompt: "handoff".into(),
        })
        .unwrap();
        core.create_conversation(Conversation {
            id: "handoff-conversation".into(),
            project_id: "handoff-project".into(),
            title: "Handoff".into(),
            scope_revision: 0,
        })
        .unwrap();
        core.set_project_agent_assignment(
            "handoff-project",
            "handoff-agent",
            true,
            WorkspaceAccess::None,
        )
        .unwrap();
        core.set_project_agent_assignment(
            "handoff-project",
            "handoff-target-agent",
            true,
            WorkspaceAccess::None,
        )
        .unwrap();
        core.create_collaboration(CreateCollaborationCommand {
            project_id: "handoff-project".into(),
            collaboration: CollaborationRun {
                id: "handoff-collaboration".into(),
                root_agent_ids: vec!["handoff-agent".into()],
                call_count: 0,
                max_calls: 8,
                depth: 0,
                max_depth: 5,
                status: CollaborationStatus::Pending,
                stop_reason: None,
                auto_dispatch_handoffs: false,
            },
        })
        .unwrap();
        core.start_execution(ExecutionStart {
            run_id: "handoff-execution".into(),
            collaboration_run_id: "handoff-collaboration".into(),
            project_id: "handoff-project".into(),
            conversation_id: "handoff-conversation".into(),
            agent_id: "handoff-agent".into(),
            workspace_access: WorkspaceAccess::None,
            canonical_cwd: None,
        })
        .unwrap();
        core.create_message(Message {
            id: "handoff-source-message".into(),
            conversation_id: "handoff-conversation".into(),
            sender_id: "user".into(),
            sequence: 1,
            content: "handoff source".into(),
        })
        .unwrap();
        core.create_handoff(CreateHandoffCommand {
            handoff: Handoff {
                id: "handoff-record".into(),
                collaboration_run_id: "handoff-collaboration".into(),
                from_execution_run_id: "handoff-execution".into(),
                to_agent_id: "handoff-target-agent".into(),
                status: "proposed".into(),
                details: Some(StructuredHandoffDetails {
                    parent_execution_run_id: Some("handoff-execution".into()),
                    child_execution_run_id: None,
                    source_message_id: Some("handoff-source-message".into()),
                    from_agent_id: Some("handoff-agent".into()),
                    to_agent_id: Some("handoff-target-agent".into()),
                    kind: Some("task".into()),
                    dispatch_mode: Some("sequential".into()),
                    batch_id: None,
                    sequence_index: None,
                    detected_by: Some("ui_explicit".into()),
                    task: Some("handoff task".into()),
                    reason: None,
                    decisions: None,
                    constraints: None,
                    artifacts: None,
                    expected_output: None,
                    context_scope: Some("conversation".into()),
                    agent_path: None,
                }),
            },
        })
        .unwrap();

        assert!(matches!(
            core.transition_handoff("handoff-record", "approved"),
            Ok(HandoffTransitionOutcome::Changed)
        ));
        assert!(matches!(
            core.transition_handoff("handoff-record", "approved"),
            Ok(HandoffTransitionOutcome::AlreadyAtTarget)
        ));
        let dispatched = core.dispatch_handoff("handoff-record").unwrap();
        assert!(dispatched.created);
        assert_eq!(dispatched.child_run.id, "handoff-child-handoff-record");
        let child_snapshot = core
            .model_snapshot(&dispatched.child_run.id)
            .unwrap()
            .expect("handoff child must freeze a model snapshot");
        assert_eq!(child_snapshot.run_id, dispatched.child_run.id);
        assert_eq!(child_snapshot.connector_id.as_deref(), Some("mock"));
        let child_selection = core
            .model_selection_snapshot(&dispatched.child_run.id)
            .unwrap()
            .expect("handoff child must freeze a full model selection");
        let projection = core.projection_snapshot().unwrap();
        let manifest = projection["contextManifests"]
            .as_array()
            .unwrap()
            .iter()
            .find(|manifest| manifest["executionRunId"] == dispatched.child_run.id)
            .expect("deferred handoff child task must atomically persist a Context Manifest");
        assert_eq!(
            manifest["connectorId"],
            child_selection.connector_id.as_str()
        );
        assert_eq!(
            manifest["modelId"],
            json!(child_selection.effective_model_id)
        );
        assert_eq!(
            core.replay_events(0)
                .unwrap()
                .into_iter()
                .filter(|event| event.execution_run_id == dispatched.child_run.id)
                .map(|event| event.event_type)
                .collect::<Vec<_>>(),
            vec![
                "execution.created",
                "scope.frozen",
                "context.assembled",
                "context.sealed",
            ]
        );
        let replayed = core.dispatch_handoff("handoff-record").unwrap();
        assert!(!replayed.created);
        assert_eq!(replayed.child_run.id, dispatched.child_run.id);
        let details = core
            .storage
            .load_handoff("handoff-record")
            .unwrap()
            .unwrap()
            .details
            .unwrap();
        assert_eq!(
            details.parent_execution_run_id.as_deref(),
            Some("handoff-execution")
        );
        assert_eq!(
            details.child_execution_run_id.as_deref(),
            Some("handoff-child-handoff-record")
        );
        core.create_handoff(CreateHandoffCommand {
            handoff: Handoff {
                id: "handoff-cycle-record".into(),
                collaboration_run_id: "handoff-collaboration".into(),
                from_execution_run_id: "handoff-child-handoff-record".into(),
                to_agent_id: "handoff-agent".into(),
                status: "proposed".into(),
                details: Some(StructuredHandoffDetails {
                    parent_execution_run_id: Some("handoff-child-handoff-record".into()),
                    child_execution_run_id: None,
                    source_message_id: Some("handoff-source-message".into()),
                    from_agent_id: Some("handoff-target-agent".into()),
                    to_agent_id: Some("handoff-agent".into()),
                    kind: Some("review_feedback".into()),
                    dispatch_mode: Some("sequential".into()),
                    batch_id: None,
                    sequence_index: None,
                    detected_by: Some("structured_output".into()),
                    task: Some("cycle task".into()),
                    reason: None,
                    decisions: None,
                    constraints: None,
                    artifacts: None,
                    expected_output: None,
                    context_scope: Some("conversation".into()),
                    agent_path: None,
                }),
            },
        })
        .unwrap();
        core.transition_handoff("handoff-cycle-record", "approved")
            .unwrap();
        let cycle_result = core.dispatch_handoff("handoff-cycle-record");
        assert!(matches!(cycle_result, Err(CoreError::HandoffCycleDetected)));
        assert!(matches!(
            core.transition_handoff("handoff-record", "cancelled"),
            Ok(HandoffTransitionOutcome::Changed)
        ));
        assert!(matches!(
            core.transition_handoff("handoff-record", "approved"),
            Err(CoreError::HandoffInvalidTransition)
        ));
    }

    #[test]
    fn persistent_core_opens_a_writable_copy_of_the_real_r4_clone() {
        let state_root = std::env::var_os("AGENTTALK_STATE_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../../AgentTalk-local-state")
            });
        let source =
            state_root.join("artifacts/artifacts/migration/postgres-clone-20260805-r4.sqlite3");
        if !source.exists() {
            return;
        }
        let target = std::env::temp_dir().join(format!(
            "agenttalk-core-r4-smoke-{}.sqlite3",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&target);
        std::fs::copy(&source, &target).unwrap();
        let core = PersistentCore::open(&target).unwrap();
        let snapshot = core.projection_snapshot().unwrap();
        assert_eq!(snapshot["runs"].as_array().unwrap().len(), 58);
        assert_eq!(snapshot["projects"].as_array().unwrap().len(), 4);
        assert_eq!(snapshot["workflows"].as_array().unwrap().len(), 1);
        assert_eq!(snapshot["modelSnapshots"].as_array().unwrap().len(), 17);
        assert_eq!(snapshot["auditTimestamps"].as_array().unwrap().len(), 222);
        drop(core);
        let _ = std::fs::remove_file(&target);
        let _ = std::fs::remove_file(target.with_extension("sqlite3-wal"));
        let _ = std::fs::remove_file(target.with_extension("sqlite3-shm"));
    }

    #[test]
    fn persistent_core_rejects_cwd_outside_authorized_workspace_root() {
        let root = std::env::temp_dir().join(format!("agenttalk-core-root-{}", std::process::id()));
        let child = root.join("child");
        let outside =
            std::env::temp_dir().join(format!("agenttalk-core-outside-{}", std::process::id()));
        let database = std::env::temp_dir().join(format!(
            "agenttalk-core-workspace-{}.db",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(&outside);
        let _ = std::fs::remove_file(&database);
        std::fs::create_dir_all(&child).unwrap();
        std::fs::create_dir(&outside).unwrap();
        let mut core = PersistentCore::open(&database).unwrap();
        core.create_project(Project {
            id: "p-workspace".into(),
            name: "Workspace".into(),
            root_path: None,
            archived: false,
        })
        .unwrap();
        core.create_agent(AgentIdentity {
            id: "a-workspace".into(),
            name: "Agent".into(),
            role: "builder".into(),
            specialty: "code".into(),
            system_prompt: "system".into(),
        })
        .unwrap();
        core.set_project_agent_assignment(
            "p-workspace",
            "a-workspace",
            true,
            WorkspaceAccess::ReadOnly,
        )
        .unwrap();
        core.authorize_workspace("p-workspace", root.to_str().unwrap())
            .unwrap();
        core.start_execution(ExecutionStart {
            run_id: "run-inside".into(),
            collaboration_run_id: "collab-inside".into(),
            project_id: "p-workspace".into(),
            conversation_id: "conversation".into(),
            agent_id: "a-workspace".into(),
            workspace_access: WorkspaceAccess::ReadOnly,
            canonical_cwd: Some(child.to_string_lossy().into_owned()),
        })
        .unwrap();
        assert!(matches!(
            core.start_execution(ExecutionStart {
                run_id: "run-outside".into(),
                collaboration_run_id: "collab-outside".into(),
                project_id: "p-workspace".into(),
                conversation_id: "conversation".into(),
                agent_id: "a-workspace".into(),
                workspace_access: WorkspaceAccess::ReadOnly,
                canonical_cwd: Some(outside.to_string_lossy().into_owned()),
            }),
            Err(CoreError::WorkspacePathDenied)
        ));
        drop(core);
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(&outside);
        let _ = std::fs::remove_file(&database);
        let _ = std::fs::remove_file(database.with_extension("db-wal"));
        let _ = std::fs::remove_file(database.with_extension("db-shm"));
    }

    #[test]
    fn handoff_dispatch_can_start_a_mock_child_runtime_from_structured_task() {
        let mut core = PersistentCore::open(":memory:").unwrap();
        core.create_project(Project {
            id: "handoff-runtime-project".into(),
            name: "Handoff Runtime".into(),
            root_path: None,
            archived: false,
        })
        .unwrap();
        for (id, role) in [
            ("handoff-runtime-source", "builder"),
            ("handoff-runtime-target", "reviewer"),
        ] {
            core.create_agent(AgentIdentity {
                id: id.into(),
                name: id.into(),
                role: role.into(),
                specialty: "handoff".into(),
                system_prompt: "fixture".into(),
            })
            .unwrap();
            core.set_project_agent_assignment(
                "handoff-runtime-project",
                id,
                true,
                WorkspaceAccess::None,
            )
            .unwrap();
        }
        core.create_conversation(Conversation {
            id: "handoff-runtime-conversation".into(),
            project_id: "handoff-runtime-project".into(),
            title: "Handoff Runtime".into(),
            scope_revision: 0,
        })
        .unwrap();
        core.create_collaboration(CreateCollaborationCommand {
            project_id: "handoff-runtime-project".into(),
            collaboration: CollaborationRun {
                id: "handoff-runtime-collaboration".into(),
                root_agent_ids: vec!["handoff-runtime-source".into()],
                call_count: 0,
                max_calls: 4,
                depth: 0,
                max_depth: 3,
                status: agenttalk_domain::CollaborationStatus::Pending,
                stop_reason: None,
                auto_dispatch_handoffs: false,
            },
        })
        .unwrap();
        core.start_execution(ExecutionStart {
            run_id: "handoff-runtime-parent".into(),
            collaboration_run_id: "handoff-runtime-collaboration".into(),
            project_id: "handoff-runtime-project".into(),
            conversation_id: "handoff-runtime-conversation".into(),
            agent_id: "handoff-runtime-source".into(),
            workspace_access: WorkspaceAccess::None,
            canonical_cwd: None,
        })
        .unwrap();
        core.create_message(Message {
            id: "handoff-runtime-source-message".into(),
            conversation_id: "handoff-runtime-conversation".into(),
            sender_id: "user".into(),
            sequence: 1,
            content: "runtime handoff source".into(),
        })
        .unwrap();
        core.create_handoff(CreateHandoffCommand {
            handoff: Handoff {
                id: "handoff-runtime-record".into(),
                collaboration_run_id: "handoff-runtime-collaboration".into(),
                from_execution_run_id: "handoff-runtime-parent".into(),
                to_agent_id: "handoff-runtime-target".into(),
                status: "approved".into(),
                details: Some(StructuredHandoffDetails {
                    parent_execution_run_id: Some("handoff-runtime-parent".into()),
                    child_execution_run_id: None,
                    source_message_id: Some("handoff-runtime-source-message".into()),
                    from_agent_id: Some("handoff-runtime-source".into()),
                    to_agent_id: Some("handoff-runtime-target".into()),
                    kind: Some("task".into()),
                    dispatch_mode: Some("sequential".into()),
                    batch_id: None,
                    sequence_index: None,
                    detected_by: Some("ui_explicit".into()),
                    task: Some("run the child task".into()),
                    reason: Some("structured fixture".into()),
                    decisions: None,
                    constraints: None,
                    artifacts: None,
                    expected_output: Some("completed output".into()),
                    context_scope: None,
                    agent_path: None,
                }),
            },
        })
        .unwrap();

        let result = core
            .dispatch_handoff_with_runtime("handoff-runtime-record", true)
            .unwrap();
        assert!(result.created);
        assert!(result.runtime_started);
        assert_eq!(result.runtime_dispatch, "completed");
        assert_eq!(result.handoff_status, "completed");
        assert_eq!(result.child_run.status, ExecutionStatus::Completed);
        let child_selection = core
            .model_selection_snapshot(&result.child_run.id)
            .unwrap()
            .unwrap();
        let child_connector_snapshot = core.model_snapshot(&result.child_run.id).unwrap().unwrap();
        assert_eq!(child_selection.run_id, result.child_run.id);
        assert_eq!(
            child_selection.effective_model_id,
            child_connector_snapshot.model_id
        );
        assert_eq!(
            child_selection.connector_id,
            child_connector_snapshot
                .connector_id
                .as_deref()
                .expect("handoff child connector must be frozen")
        );
        let snapshot = core.projection_snapshot().unwrap();
        let manifests = snapshot["contextManifests"].as_array().unwrap();
        let manifest = manifests
            .iter()
            .find(|manifest| manifest["executionRunId"] == "handoff-child-handoff-runtime-record")
            .expect("handoff child must persist its context manifest");
        assert_eq!(
            manifest["connectorId"],
            child_connector_snapshot
                .connector_id
                .as_deref()
                .expect("handoff child connector must be frozen")
        );
        assert_eq!(
            manifest["modelId"],
            child_connector_snapshot
                .model_id
                .as_deref()
                .expect("handoff child model must be frozen")
        );
        let initial_event_types = core
            .replay_events(0)
            .unwrap()
            .into_iter()
            .filter(|event| event.execution_run_id == result.child_run.id)
            .map(|event| event.event_type)
            .take(4)
            .collect::<Vec<_>>();
        assert_eq!(
            initial_event_types,
            vec![
                "execution.created",
                "scope.frozen",
                "context.assembled",
                "context.sealed",
            ]
        );

        let missing_task = Handoff {
            id: "handoff-runtime-missing-task".into(),
            collaboration_run_id: "handoff-runtime-collaboration".into(),
            from_execution_run_id: "handoff-runtime-parent".into(),
            to_agent_id: "handoff-runtime-target".into(),
            status: "approved".into(),
            details: Some(StructuredHandoffDetails {
                parent_execution_run_id: Some("handoff-runtime-parent".into()),
                child_execution_run_id: None,
                source_message_id: Some("handoff-runtime-source-message".into()),
                from_agent_id: Some("handoff-runtime-source".into()),
                to_agent_id: Some("handoff-runtime-target".into()),
                kind: Some("task".into()),
                dispatch_mode: Some("sequential".into()),
                batch_id: None,
                sequence_index: None,
                detected_by: Some("ui_explicit".into()),
                task: None,
                reason: None,
                decisions: None,
                constraints: None,
                artifacts: None,
                expected_output: None,
                context_scope: Some("conversation".into()),
                agent_path: None,
            }),
        };
        core.create_handoff(CreateHandoffCommand {
            handoff: missing_task,
        })
        .unwrap();
        assert!(matches!(
            core.dispatch_handoff_with_runtime("handoff-runtime-missing-task", true),
            Err(CoreError::HandoffTaskMissing)
        ));
    }

    #[test]
    fn auto_dispatch_handoff_approves_and_starts_child_runtime_in_core() {
        let mut core = PersistentCore::open(":memory:").unwrap();
        core.create_project(Project {
            id: "handoff-auto-project".into(),
            name: "Auto Handoff".into(),
            root_path: None,
            archived: false,
        })
        .unwrap();
        for (id, role) in [
            ("handoff-auto-source", "builder"),
            ("handoff-auto-target", "reviewer"),
        ] {
            core.create_agent(AgentIdentity {
                id: id.into(),
                name: id.into(),
                role: role.into(),
                specialty: "handoff".into(),
                system_prompt: "fixture".into(),
            })
            .unwrap();
            core.set_project_agent_assignment(
                "handoff-auto-project",
                id,
                true,
                WorkspaceAccess::None,
            )
            .unwrap();
        }
        core.create_conversation(Conversation {
            id: "handoff-auto-conversation".into(),
            project_id: "handoff-auto-project".into(),
            title: "Auto Handoff".into(),
            scope_revision: 0,
        })
        .unwrap();
        core.create_collaboration(CreateCollaborationCommand {
            project_id: "handoff-auto-project".into(),
            collaboration: CollaborationRun {
                id: "handoff-auto-collaboration".into(),
                root_agent_ids: vec!["handoff-auto-source".into()],
                call_count: 0,
                max_calls: 2,
                depth: 0,
                max_depth: 2,
                status: CollaborationStatus::Pending,
                stop_reason: None,
                auto_dispatch_handoffs: true,
            },
        })
        .unwrap();
        core.start_execution(ExecutionStart {
            run_id: "handoff-auto-parent".into(),
            collaboration_run_id: "handoff-auto-collaboration".into(),
            project_id: "handoff-auto-project".into(),
            conversation_id: "handoff-auto-conversation".into(),
            agent_id: "handoff-auto-source".into(),
            workspace_access: WorkspaceAccess::None,
            canonical_cwd: None,
        })
        .unwrap();
        core.create_message(Message {
            id: "handoff-auto-message".into(),
            conversation_id: "handoff-auto-conversation".into(),
            sender_id: "user".into(),
            sequence: 1,
            content: "auto handoff source".into(),
        })
        .unwrap();
        core.create_handoff(CreateHandoffCommand {
            handoff: Handoff {
                id: "handoff-auto-record".into(),
                collaboration_run_id: "handoff-auto-collaboration".into(),
                from_execution_run_id: "handoff-auto-parent".into(),
                to_agent_id: "handoff-auto-target".into(),
                status: "proposed".into(),
                details: Some(StructuredHandoffDetails {
                    parent_execution_run_id: Some("handoff-auto-parent".into()),
                    child_execution_run_id: None,
                    source_message_id: Some("handoff-auto-message".into()),
                    from_agent_id: Some("handoff-auto-source".into()),
                    to_agent_id: Some("handoff-auto-target".into()),
                    kind: Some("task".into()),
                    dispatch_mode: Some("sequential".into()),
                    batch_id: None,
                    sequence_index: None,
                    detected_by: Some("structured_output".into()),
                    task: Some("auto dispatch child task".into()),
                    reason: Some("automatic handoff policy".into()),
                    decisions: None,
                    constraints: None,
                    artifacts: None,
                    expected_output: Some("child output".into()),
                    context_scope: Some("conversation".into()),
                    agent_path: None,
                }),
            },
        })
        .unwrap();
        let snapshot = core.projection_snapshot().unwrap();
        assert_eq!(snapshot["handoffs"][0]["status"], "completed");
        assert_eq!(
            core.recover_run("handoff-child-handoff-auto-record")
                .unwrap()
                .unwrap()
                .status,
            ExecutionStatus::Completed
        );
        assert_eq!(snapshot["collaborationRuns"][0]["callCount"], 1);
    }

    #[test]
    fn structured_handoff_parser_accepts_explicit_envelopes_and_bounded_json_text() {
        let proposal = json!({
            "handoffId": "handoff-parser",
            "collaborationRunId": "collaboration-parser",
            "fromExecutionRunId": "parent-parser",
            "toAgentId": "target-parser",
            "status": "proposed",
            "details": {
                "parentExecutionRunId": "parent-parser",
                "sourceMessageId": "message-parser",
                "fromAgentId": "source-parser",
                "toAgentId": "target-parser",
                "kind": "task",
                "dispatchMode": "sequential",
                "detectedBy": "structured_output",
                "contextScope": "conversation"
            }
        });
        let nested = json!({ "handoffProposal": proposal.clone() });
        let nested_text = serde_json::to_string(&nested).unwrap();
        let fenced_text = format!("```json\n{nested_text}\n```");
        let payloads = vec![
            json!({ "handoffProposal": proposal.clone() }),
            json!({ "output": { "handoffProposals": [proposal.clone()] } }),
            json!({ "output": nested_text }),
            json!({ "content": fenced_text }),
            json!({
                "response": {
                    "output": [{
                        "type": "message",
                        "content": [{"type": "output_text", "text": nested_text}]
                    }]
                }
            }),
            json!({
                "candidates": [{
                    "content": {"parts": [{"text": nested_text}]}
                }]
            }),
            proposal.clone(),
        ];

        for payload in payloads {
            let event = RuntimeEvent {
                event_id: "event-parser".into(),
                execution_run_id: "parent-parser".into(),
                runtime_id: "mock".into(),
                thread_id: None,
                turn_id: None,
                sequence: 1,
                event_type: "execution.completed".into(),
                timestamp_ms: 1,
                payload,
            };
            let parsed = parse_runtime_handoff_proposals(&event).unwrap().unwrap();
            assert_eq!(parsed.len(), 1);
            assert_eq!(parsed[0].id, "handoff-parser");
        }

        let oversized = format!(
            "{{\"handoffProposal\":{},\"padding\":\"{}\"}}",
            proposal,
            "x".repeat(MAX_STRUCTURED_HANDOFF_TEXT_BYTES)
        );
        let event = RuntimeEvent {
            event_id: "event-parser-oversized".into(),
            execution_run_id: "parent-parser".into(),
            runtime_id: "mock".into(),
            thread_id: None,
            turn_id: None,
            sequence: 1,
            event_type: "execution.completed".into(),
            timestamp_ms: 1,
            payload: json!({ "output": oversized }),
        };
        assert!(parse_runtime_handoff_proposals(&event).unwrap().is_none());
    }

    #[test]
    fn layered_identity_model_resolver_matches_legacy_v2_precedence() {
        let option = |id: &str,
                      scope: IdentityModelListScope,
                      model_id: &str,
                      is_default: bool,
                      sort_order: u64| IdentityModelOption {
            id: id.into(),
            scope,
            agent_id: "resolver-agent".into(),
            project_id: (scope == IdentityModelListScope::ProjectAgent)
                .then(|| "resolver-project".into()),
            conversation_id: (scope == IdentityModelListScope::ConversationAgent)
                .then(|| "resolver-conversation".into()),
            model_id: model_id.into(),
            display_name: format!("Display {model_id}"),
            connector_id: "mock".into(),
            source: ModelOptionSource::Runtime,
            availability: ModelAvailability::Available,
            is_default,
            sort_order,
            catalog_revision: Some("catalog-r1".into()),
            context_window: Some(128_000),
            reasoning_efforts: vec!["medium".into(), "medium".into()],
            service_tiers: vec!["priority".into()],
        };
        let project = StoredModelSelection {
            selection: ModelSelection {
                mode: ModelSelectionMode::Pinned,
                model_id: Some("project-pinned".into()),
            },
            candidate_model_list_mode: IdentityModelListMode::Override,
            candidate_model_list_revision: 6,
        };
        let conversation = StoredModelSelection {
            selection: ModelSelection {
                mode: ModelSelectionMode::Pinned,
                model_id: Some("conversation-pinned".into()),
            },
            candidate_model_list_mode: IdentityModelListMode::Override,
            candidate_model_list_revision: 8,
        };
        let base_options = vec![option(
            "base-default",
            IdentityModelListScope::BaseAgent,
            "base-list-default",
            true,
            0,
        )];
        let project_options = vec![option(
            "project-default",
            IdentityModelListScope::ProjectAgent,
            "project-list-default",
            true,
            0,
        )];
        let conversation_options = vec![option(
            "conversation-default",
            IdentityModelListScope::ConversationAgent,
            "conversation-list-default",
            true,
            0,
        )];

        let conversation_pinned = resolve_identity_model_selection(
            "resolver-run-conversation-pinned".into(),
            "mock".into(),
            "mock".into(),
            "mock".into(),
            Some("base-pinned".into()),
            None,
            None,
            3,
            Some(project.clone()),
            Some(conversation.clone()),
            base_options.clone(),
            project_options.clone(),
            conversation_options.clone(),
        )
        .unwrap();
        assert_eq!(
            conversation_pinned.effective_model_id.as_deref(),
            Some("conversation-pinned")
        );
        assert_eq!(
            conversation_pinned.selection_source,
            ModelSelectionSource::Conversation
        );

        let conversation_default = resolve_identity_model_selection(
            "resolver-run-conversation-default".into(),
            "mock".into(),
            "mock".into(),
            "mock".into(),
            Some("base-pinned".into()),
            None,
            None,
            3,
            Some(project.clone()),
            Some(StoredModelSelection {
                selection: ModelSelection {
                    mode: ModelSelectionMode::Inherit,
                    model_id: None,
                },
                ..conversation
            }),
            base_options.clone(),
            project_options.clone(),
            conversation_options,
        )
        .unwrap();
        assert_eq!(
            conversation_default.effective_model_id.as_deref(),
            Some("conversation-list-default")
        );
        assert_eq!(
            conversation_default.selection_source,
            ModelSelectionSource::IdentityDefault
        );
        assert_eq!(
            conversation_default
                .candidate_model_list
                .as_ref()
                .unwrap()
                .scope,
            IdentityModelListScope::ConversationAgent
        );

        let project_default = resolve_identity_model_selection(
            "resolver-run-project-default".into(),
            "mock".into(),
            "mock".into(),
            "mock".into(),
            Some("base-pinned".into()),
            None,
            None,
            3,
            Some(StoredModelSelection {
                selection: ModelSelection {
                    mode: ModelSelectionMode::Inherit,
                    model_id: None,
                },
                ..project
            }),
            None,
            base_options,
            project_options,
            Vec::new(),
        )
        .unwrap();
        assert_eq!(
            project_default.effective_model_id.as_deref(),
            Some("project-list-default")
        );
        assert_eq!(
            project_default
                .candidate_model_list
                .as_ref()
                .unwrap()
                .revision,
            6
        );
        assert_eq!(project_default.reasoning_efforts, vec!["medium"]);
    }

    #[test]
    fn identity_resolver_falls_back_to_the_same_runtime_declared_default() {
        let selection = resolve_identity_model_selection(
            "connector-only-default".into(),
            "kun".into(),
            "kun".into(),
            "kun-profile".into(),
            None,
            Some("kun-model-b".into()),
            Some("42".into()),
            0,
            Some(StoredModelSelection {
                selection: ModelSelection {
                    mode: ModelSelectionMode::Inherit,
                    model_id: None,
                },
                candidate_model_list_mode: IdentityModelListMode::Override,
                candidate_model_list_revision: 9,
            }),
            None,
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .expect("an empty current Identity layer must use this connector Runtime default");
        assert_eq!(selection.effective_model_id.as_deref(), Some("kun-model-b"));
        assert_eq!(
            selection.selection_source,
            ModelSelectionSource::ConnectorDefault
        );
        assert_eq!(selection.availability, ModelAvailability::Available);
        assert_eq!(selection.catalog_revision.as_deref(), Some("42"));
    }

    #[test]
    fn full_selection_snapshot_survives_restart_retry_and_rerun_current() {
        let path = std::env::temp_dir().join(format!(
            "agenttalk-selection-lifecycle-{}.sqlite3",
            unix_time_nanos()
        ));
        let _ = std::fs::remove_file(&path);
        let source_id = "selection-lifecycle-source";
        let target = IdentityModelListTarget {
            scope: IdentityModelListScope::ConversationAgent,
            agent_id: "selection-agent".into(),
            project_id: None,
            conversation_id: Some("selection-conversation".into()),
        };
        {
            let mut core = PersistentCore::open_with_runtime(
                &path,
                Box::new(CodexAppServerRuntime::from_fixture_models(
                    vec!["model-a".into(), "model-b".into()],
                    r#"{"type":"response.completed"}"#,
                )),
            )
            .unwrap();
            core.create_project(Project {
                id: "selection-project".into(),
                name: "Selection project".into(),
                root_path: None,
                archived: false,
            })
            .unwrap();
            core.create_agent(AgentIdentity {
                id: "selection-agent".into(),
                name: "Selection agent".into(),
                role: "worker".into(),
                specialty: "selection".into(),
                system_prompt: "fixture".into(),
            })
            .unwrap();
            core.create_conversation(Conversation {
                id: "selection-conversation".into(),
                project_id: "selection-project".into(),
                title: "Selection conversation".into(),
                scope_revision: 0,
            })
            .unwrap();
            core.create_connector_profile(ConnectorProfile {
                scope_id: "desktop".into(),
                connector_id: "selection-fixture".into(),
                display_name: "Selection lifecycle fixture".into(),
                provider_type: "codex".into(),
                runtime_type: "codex".into(),
                enabled: true,
                auth_env_key: None,
            })
            .unwrap();
            core.set_agent_model_binding(
                "selection-agent",
                Some("selection-fixture".into()),
                None,
                1,
            )
            .unwrap();
            core.set_project_agent_assignment_with_model_selection(
                "selection-project",
                "selection-agent",
                true,
                WorkspaceAccess::None,
                ModelSelection {
                    mode: ModelSelectionMode::Inherit,
                    model_id: None,
                },
                IdentityModelListMode::Inherit,
                0,
            )
            .unwrap();
            core.set_conversation_agent_assignment_with_model_selection(
                "selection-conversation",
                "selection-agent",
                true,
                ModelSelection {
                    mode: ModelSelectionMode::Inherit,
                    model_id: None,
                },
                IdentityModelListMode::Override,
                2,
            )
            .unwrap();
            for (id, model_id, is_default, order) in [
                ("option-a", "model-a", true, 0),
                ("option-b", "model-b", false, 1),
            ] {
                core.upsert_identity_model_option(&IdentityModelOption {
                    id: id.into(),
                    scope: IdentityModelListScope::ConversationAgent,
                    agent_id: "selection-agent".into(),
                    project_id: None,
                    conversation_id: Some("selection-conversation".into()),
                    model_id: model_id.into(),
                    display_name: model_id.into(),
                    connector_id: "selection-fixture".into(),
                    source: ModelOptionSource::Manual,
                    availability: ModelAvailability::Unverified,
                    is_default,
                    sort_order: order,
                    catalog_revision: Some("identity-r1".into()),
                    context_window: None,
                    reasoning_efforts: Vec::new(),
                    service_tiers: Vec::new(),
                })
                .unwrap();
            }
            let source = core
                .start_execution_with_task_and_receipt(
                    ExecutionStart {
                        run_id: source_id.into(),
                        collaboration_run_id: "selection-collaboration".into(),
                        project_id: "selection-project".into(),
                        conversation_id: "selection-conversation".into(),
                        agent_id: "selection-agent".into(),
                        workspace_access: WorkspaceAccess::None,
                        canonical_cwd: None,
                    },
                    "freeze model A".into(),
                    &CommandReceipt {
                        key: CommandReceiptKey {
                            scope_id: "selection-project".into(),
                            client_id: "test-client".into(),
                            request_id: "start-selection-source".into(),
                        },
                        command: "execution.start".into(),
                        payload_hash: "a".repeat(64),
                        operation_key: source_id.into(),
                        state: CommandReceiptState::InProgress,
                        result_json: None,
                        error_json: None,
                        created_at: 1,
                        updated_at: 1,
                    },
                )
                .unwrap();
            assert_eq!(source.status, ExecutionStatus::Completed);
            let frozen = core.model_selection_snapshot(source_id).unwrap().unwrap();
            assert_eq!(frozen.version, 2);
            assert_eq!(frozen.effective_model_id.as_deref(), Some("model-a"));
            assert_eq!(
                frozen.candidate_model_list.as_ref().unwrap().scope,
                IdentityModelListScope::ConversationAgent
            );
            let projection = core.projection_snapshot().unwrap();
            assert!(projection["modelSelectionSnapshots"]
                .as_array()
                .unwrap()
                .iter()
                .any(|snapshot| {
                    snapshot["runId"] == source_id && snapshot["effectiveModelId"] == "model-a"
                }));
            assert_eq!(
                projection["identityModelOptions"].as_array().unwrap().len(),
                2
            );
            assert_eq!(
                projection["conversationAgents"][0]["candidateModelListMode"],
                "override"
            );
            assert!(projection["contextManifests"]
                .as_array()
                .unwrap()
                .iter()
                .any(|manifest| {
                    manifest["executionRunId"] == source_id && manifest["modelId"] == "model-a"
                }));
            core.set_identity_model_option_default(&target, "selection-fixture", "model-b")
                .unwrap();
        }

        let mut reopened = PersistentCore::open_with_runtime(
            &path,
            Box::new(CodexAppServerRuntime::from_fixture_models(
                vec!["model-a".into(), "model-b".into()],
                r#"{"type":"response.completed"}"#,
            )),
        )
        .unwrap();
        let retry = reopened
            .retry_execution(
                "selection-lifecycle-retry",
                source_id,
                "ordinary retry".into(),
                None,
                None,
            )
            .unwrap();
        let retry_snapshot = reopened
            .model_selection_snapshot(&retry.id)
            .unwrap()
            .unwrap();
        assert_eq!(
            retry_snapshot.effective_model_id.as_deref(),
            Some("model-a")
        );

        let rerun = reopened
            .rerun_current_execution_with_receipt(
                "selection-lifecycle-current",
                source_id,
                "rerun current".into(),
                &CommandReceipt {
                    key: CommandReceiptKey {
                        scope_id: "selection-project".into(),
                        client_id: "test-client".into(),
                        request_id: "rerun-selection-current".into(),
                    },
                    command: "execution.rerun_current".into(),
                    payload_hash: "b".repeat(64),
                    operation_key: "selection-lifecycle-current".into(),
                    state: CommandReceiptState::InProgress,
                    result_json: None,
                    error_json: None,
                    created_at: 2,
                    updated_at: 2,
                },
                ExecutionRuntimeOptions::default(),
            )
            .unwrap();
        let current_snapshot = reopened
            .model_selection_snapshot(&rerun.id)
            .unwrap()
            .unwrap();
        assert_eq!(
            current_snapshot.effective_model_id.as_deref(),
            Some("model-b")
        );
        assert_ne!(retry_snapshot, current_snapshot);

        drop(reopened);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite3-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite3-shm"));
    }

    #[test]
    fn explicit_cancel_is_idempotent_without_duplicate_terminal_events() {
        let mut core = PersistentCore::open(":memory:").unwrap();
        core.assign_agent("cancel-agent");
        let pending = core
            .state
            .build_pending_execution(ExecutionStart {
                run_id: "cancel-run".into(),
                collaboration_run_id: "cancel-collaboration".into(),
                project_id: "cancel-project".into(),
                conversation_id: "cancel-conversation".into(),
                agent_id: "cancel-agent".into(),
                workspace_access: WorkspaceAccess::None,
                canonical_cwd: None,
            })
            .unwrap();
        core.state.insert_pending_execution(pending).unwrap();

        let first = core.cancel_execution("cancel-run").unwrap();
        let second = core.cancel_execution("cancel-run").unwrap();
        assert_eq!(first.status, ExecutionStatus::Cancelled);
        assert_eq!(second, first);
        assert_eq!(
            core.replay_events(0)
                .unwrap()
                .iter()
                .filter(|event| event.execution_run_id == "cancel-run"
                    && event.event_type == "execution.cancelled")
                .count(),
            1
        );
    }

    #[test]
    fn profile_bound_cancel_is_allowed_after_connector_started() {
        let registry = RuntimeRegistry::from_adapters(vec![Box::new(
            CodexAppServerRuntime::from_fixture_models(
                vec!["codex-model-a".into()],
                r#"{"type":"response.completed"}"#,
            ),
        )])
        .unwrap();
        let mut core = PersistentCore::open_with_runtime_registry(":memory:", registry).unwrap();
        core.assign_agent("cancel-profile-agent");
        core.create_connector_profile(ConnectorProfile {
            scope_id: "desktop".into(),
            connector_id: "cancel-profile".into(),
            display_name: "Offline Codex".into(),
            provider_type: "codex".into(),
            runtime_type: "codex".into(),
            enabled: true,
            auth_env_key: None,
        })
        .unwrap();
        let run = core
            .start_execution_internal_with_selection(
                ExecutionStart {
                    run_id: "cancel-profile-run".into(),
                    collaboration_run_id: "cancel-profile-collaboration".into(),
                    project_id: "cancel-profile-project".into(),
                    conversation_id: "cancel-profile-conversation".into(),
                    agent_id: "cancel-profile-agent".into(),
                    workspace_access: WorkspaceAccess::None,
                    canonical_cwd: None,
                },
                None,
                None,
                Some(ExecutionRuntimeBinding {
                    connector_id: "cancel-profile".into(),
                    runtime_type: None,
                    model_id: Some("codex-model-a".into()),
                    catalog_revision: None,
                    validate_profile: true,
                }),
                None,
                None,
                false,
            )
            .unwrap();
        let dispatch = core.begin_runtime_dispatch(&run.id).unwrap();
        let connector_started = dispatch
            .stream
            .next_timeout(Duration::from_secs(1))
            .unwrap()
            .expect("fixture must start its Connector before terminal output");
        assert_eq!(connector_started.event_type, "connector.started");
        core.apply_runtime_dispatch_event(&run.id, connector_started)
            .unwrap();

        let cancelled = core.cancel_execution(&run.id).unwrap();
        assert_eq!(cancelled.status, ExecutionStatus::Cancelled);
    }
}
