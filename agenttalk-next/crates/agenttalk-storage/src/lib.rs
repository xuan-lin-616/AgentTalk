use agenttalk_domain::{
    Artifact, Attachment, CollaborationRun, CollaborationStatus, ConnectorProfile, ExecutionRun,
    ExecutionStatus, Handoff, IdentityModelListMode, IdentityModelListScope,
    IdentityModelListTarget, IdentityModelOption, MemoryItem, Message, ModelAvailability,
    ModelOptionSource, ModelSelection, ModelSelectionMode, ModelSelectionSnapshot, ModelSnapshot,
    RetrievalFeedback, RetrievalFeedbackLabel, RetrievalFeedbackReason, RetrievalSelection,
    RetrievalSelectionItem, RetrievalSelectionScope, RetrievalSource, StructuredHandoffDetails,
    Summary, WorkflowTemplate, WorkspaceAccess, WorkspaceAuthorization, CONNECTOR_PROFILE_SCOPE,
};
use agenttalk_events::RuntimeEvent;
use agenttalk_permissions::FileReadGrant;
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use sha2::{Digest, Sha256};
use std::collections::VecDeque;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use thiserror::Error;

mod orchestration;
pub use orchestration::*;

const V11_SCHEMA_VERSION: i64 = 11;
const V12_SCHEMA_VERSION: i64 = 12;
const V13_SCHEMA_VERSION: i64 = 13;
const V14_SCHEMA_VERSION: i64 = 14;
const V15_SCHEMA_VERSION: i64 = 15;
const V16_SCHEMA_VERSION: i64 = 16;
pub const SCHEMA_VERSION: i64 = 16;
const HISTORICAL_V11_MIGRATION_CHECKSUM: &str =
    "f5a0e07a7de1f53b86aeee16e4908321abf637bae8b5372e019a37a39f6a38c7";
const MUTATED_V11_MIGRATION_CHECKSUM: &str =
    "ddb3843c80aabee4b1b0356fb8592f113ed1bb4b4652b1626666096ee5613ef5";
pub const CONNECTOR_PROFILE_QUERY_LIMIT_MAX: u64 = 256;
pub const EXACT_RETRIEVAL_VERSION: &str = "exact-retrieval-v1";
pub const LOCAL_VECTOR_RETRIEVAL_VERSION: &str = "local-vector-fixture-v1";
pub const RETRIEVAL_PREVIEW_LIMIT_MAX: u64 = 100;
const RETRIEVAL_EMBEDDING_INPUT_MAX_CHARS: usize = 16 * 1024;
const RETRIEVAL_SNIPPET_MAX_CHARS: usize = 240;
const RETRIEVAL_FILE_MAX_SIZE: u64 = 512 * 1024;
const RETRIEVAL_FILE_READ_MAX_BYTES: usize = 256 * 1024;
const RETRIEVAL_FILE_MAX_ENTRIES: usize = 500;
const RETRIEVAL_FILE_MAX_DIRECTORIES: usize = 200;
const RETRIEVAL_FILE_MAX_FILES: usize = 2_000;
const RETRIEVAL_FILE_MAX_DEPTH: usize = 8;
const RETRIEVAL_FILE_SCAN_TIMEOUT: Duration = Duration::from_secs(2);
pub const ARTIFACT_BODY_MAX_BYTES: u64 = 64 * 1024 * 1024;
/// Maximum body bytes returned by one versioned Artifact content query. The
/// limit keeps base64 JSON responses comfortably below the 1 MiB IPC frame
/// budget while allowing callers to reconstruct larger blobs incrementally.
pub const ARTIFACT_CONTENT_CHUNK_MAX_BYTES: u64 = 64 * 1024;

/// Describes the verification state of a vector provider without exposing any
/// endpoint, credential reference, request body, or provider error detail.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetrievalEmbeddingVerification {
    LocalFixture,
    VerifiedProvider,
}

impl RetrievalEmbeddingVerification {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LocalFixture => "local_fixture",
            Self::VerifiedProvider => "verified_provider",
        }
    }
}

/// Allowlisted metadata recorded in a retrieval preview response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetrievalEmbeddingDescriptor {
    pub provider_id: String,
    pub retrieval_version: String,
    pub dimension: usize,
    pub verification: RetrievalEmbeddingVerification,
}

/// Provider-neutral boundary for semantic retrieval. Implementations must not
/// return endpoint, token, Authorization, or provider diagnostic text through
/// this contract.
pub trait RetrievalEmbeddingProvider: Send + Sync {
    fn descriptor(&self) -> RetrievalEmbeddingDescriptor;
    fn embed(&self, text: &str) -> Result<Vec<f64>, RetrievalEmbeddingError>;
}

#[derive(Debug, Error)]
pub enum RetrievalEmbeddingError {
    #[error("embedding provider unavailable")]
    Unavailable,
    #[error("embedding provider returned an invalid response")]
    InvalidResponse,
}

/// Deterministic offline provider used by unit tests and local fixtures. It is
/// deliberately the default until a real provider has passed its Owner Gate.
#[derive(Default)]
pub struct LocalFixtureEmbeddingProvider;

impl RetrievalEmbeddingProvider for LocalFixtureEmbeddingProvider {
    fn descriptor(&self) -> RetrievalEmbeddingDescriptor {
        RetrievalEmbeddingDescriptor {
            provider_id: "local_fixture".into(),
            retrieval_version: LOCAL_VECTOR_RETRIEVAL_VERSION.into(),
            dimension: LOCAL_VECTOR_DIMENSION,
            verification: RetrievalEmbeddingVerification::LocalFixture,
        }
    }

    fn embed(&self, text: &str) -> Result<Vec<f64>, RetrievalEmbeddingError> {
        Ok(local_fixture_embedding(text).to_vec())
    }
}
const MIGRATION_V11_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS schema_migrations (
  version INTEGER PRIMARY KEY,
  checksum TEXT NOT NULL,
  applied_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS command_receipts (
  scope_id TEXT NOT NULL,
  client_id TEXT NOT NULL,
  request_id TEXT NOT NULL,
  command TEXT NOT NULL,
  payload_hash TEXT NOT NULL,
  operation_key TEXT NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('pending', 'in_progress', 'completed', 'failed', 'interrupted')),
  result_json TEXT,
  error_json TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY(scope_id, client_id, request_id)
);
CREATE TABLE IF NOT EXISTS stream_metadata (
  stream_id TEXT PRIMARY KEY,
  epoch TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS event_store (
  sequence INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id TEXT NOT NULL UNIQUE,
  execution_run_id TEXT NOT NULL,
  runtime_id TEXT NOT NULL,
  thread_id TEXT,
  turn_id TEXT,
  event_type TEXT NOT NULL,
  timestamp_ms INTEGER NOT NULL,
  payload_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_event_store_run_sequence ON event_store(execution_run_id, sequence);
CREATE TABLE IF NOT EXISTS execution_runs (
  id TEXT PRIMARY KEY,
  collaboration_run_id TEXT NOT NULL,
  project_id TEXT NOT NULL,
  conversation_id TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  status TEXT NOT NULL,
  version INTEGER NOT NULL,
  scope_json TEXT NOT NULL,
  terminal_reason TEXT,
  terminal INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS projects (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  root_path TEXT,
  archived INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS agents (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  role TEXT NOT NULL,
  specialty TEXT NOT NULL,
  system_prompt TEXT NOT NULL,
  connector_id TEXT,
  model_id TEXT,
  candidate_model_list_revision INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS conversations (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL,
  title TEXT NOT NULL,
  scope_revision INTEGER NOT NULL,
  FOREIGN KEY(project_id) REFERENCES projects(id)
);
CREATE TABLE IF NOT EXISTS messages (
  id TEXT PRIMARY KEY,
  conversation_id TEXT NOT NULL,
  sender_id TEXT NOT NULL,
  sequence INTEGER NOT NULL,
  content TEXT NOT NULL,
  FOREIGN KEY(conversation_id) REFERENCES conversations(id)
);
CREATE TABLE IF NOT EXISTS conversation_agents (
  conversation_id TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  role TEXT,
  specialty TEXT,
  system_prompt TEXT,
  enabled INTEGER NOT NULL,
  model_selection_mode TEXT NOT NULL DEFAULT 'inherit',
  model_id TEXT,
  candidate_model_list_mode TEXT NOT NULL DEFAULT 'inherit',
  candidate_model_list_revision INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY(conversation_id, agent_id),
  FOREIGN KEY(conversation_id) REFERENCES conversations(id),
  FOREIGN KEY(agent_id) REFERENCES agents(id)
);
CREATE INDEX IF NOT EXISTS idx_conversation_agents_conversation
  ON conversation_agents(conversation_id, agent_id);
CREATE TABLE IF NOT EXISTS workflows (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL,
  name TEXT NOT NULL,
  kind TEXT NOT NULL,
  steps_json TEXT NOT NULL,
  FOREIGN KEY(project_id) REFERENCES projects(id)
);
CREATE TABLE IF NOT EXISTS model_snapshots (
  run_id TEXT PRIMARY KEY,
  connector_id TEXT,
  model_id TEXT,
  revision INTEGER,
  FOREIGN KEY(run_id) REFERENCES execution_runs(id)
);
CREATE TABLE IF NOT EXISTS model_selection_snapshots (
  run_id TEXT PRIMARY KEY,
  snapshot_json TEXT NOT NULL CHECK (json_valid(snapshot_json)),
  snapshot_hash TEXT NOT NULL,
  FOREIGN KEY(run_id) REFERENCES execution_runs(id)
);
CREATE TABLE IF NOT EXISTS model_candidates (
  id TEXT PRIMARY KEY,
  agent_id TEXT NOT NULL,
  connector_id TEXT NOT NULL,
  model_id TEXT NOT NULL,
  available INTEGER NOT NULL,
  FOREIGN KEY(agent_id) REFERENCES agents(id)
);
CREATE TABLE IF NOT EXISTS connector_profiles (
  scope_id TEXT NOT NULL CHECK(scope_id = 'desktop'),
  connector_id TEXT NOT NULL,
  display_name TEXT NOT NULL,
  provider_type TEXT NOT NULL,
  runtime_type TEXT NOT NULL,
  enabled INTEGER NOT NULL CHECK(enabled IN (0, 1)),
  auth_env_key TEXT,
  PRIMARY KEY(scope_id, connector_id)
);
CREATE INDEX IF NOT EXISTS idx_connector_profiles_scope
  ON connector_profiles(scope_id, connector_id);
CREATE TABLE IF NOT EXISTS retrieval_sources (
  id TEXT PRIMARY KEY,
  scope_id TEXT NOT NULL,
  citation TEXT NOT NULL,
  sha256 TEXT NOT NULL,
  token_count INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS retrieval_selections (
  id TEXT PRIMARY KEY,
  scope_kind TEXT NOT NULL CHECK (scope_kind IN ('project', 'conversation')),
  scope_id TEXT NOT NULL,
  project_id TEXT NOT NULL,
  conversation_id TEXT,
  scope_revision INTEGER NOT NULL,
  workspace_revision INTEGER,
  retrieval_version TEXT NOT NULL,
  query_hash TEXT NOT NULL CHECK (length(query_hash) = 64),
  items_json TEXT NOT NULL CHECK (json_valid(items_json)),
  FOREIGN KEY(project_id) REFERENCES projects(id),
  FOREIGN KEY(conversation_id) REFERENCES conversations(id)
);
CREATE INDEX IF NOT EXISTS idx_retrieval_selections_scope
  ON retrieval_selections(scope_id, id);
CREATE TABLE IF NOT EXISTS retrieval_feedback (
  id TEXT PRIMARY KEY,
  selection_id TEXT NOT NULL,
  scope_id TEXT NOT NULL,
  source_id TEXT NOT NULL,
  label TEXT NOT NULL CHECK (label IN ('helpful', 'not_helpful')),
  reason TEXT NOT NULL CHECK (reason IN ('exact_match', 'irrelevant', 'stale_source', 'wrong_scope', 'duplicate', 'permission')),
  created_at_ms INTEGER NOT NULL,
  FOREIGN KEY(selection_id) REFERENCES retrieval_selections(id),
  FOREIGN KEY(source_id) REFERENCES retrieval_sources(id)
);
CREATE INDEX IF NOT EXISTS idx_retrieval_feedback_scope_selection
  ON retrieval_feedback(scope_id, selection_id, created_at_ms, id);
CREATE TABLE IF NOT EXISTS summaries (
  id TEXT PRIMARY KEY,
  scope_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  content_hash TEXT NOT NULL,
  artifact_id TEXT,
  FOREIGN KEY(scope_id) REFERENCES conversations(id)
);
CREATE TABLE IF NOT EXISTS memories (
  id TEXT PRIMARY KEY,
  scope_id TEXT NOT NULL,
  agent_id TEXT,
  content_hash TEXT NOT NULL,
  confirmed INTEGER NOT NULL,
  FOREIGN KEY(agent_id) REFERENCES agents(id)
);
CREATE TABLE IF NOT EXISTS artifacts (
  id TEXT PRIMARY KEY,
  sha256 TEXT NOT NULL,
  size INTEGER NOT NULL,
  mime TEXT NOT NULL,
  relative_path TEXT
);
CREATE TABLE IF NOT EXISTS context_manifests (
  id TEXT PRIMARY KEY,
  execution_run_id TEXT NOT NULL,
  schema_version TEXT NOT NULL,
  bundle_hash TEXT NOT NULL,
  source_ledger_json TEXT NOT NULL DEFAULT '[]',
  model_id TEXT,
  FOREIGN KEY(execution_run_id) REFERENCES execution_runs(id)
);
CREATE TABLE IF NOT EXISTS attachments (
  attachment_id TEXT,
  artifact_id TEXT,
  message_id TEXT NOT NULL,
  ordinal INTEGER NOT NULL,
  file_name TEXT NOT NULL,
  sha256 TEXT NOT NULL,
  size INTEGER NOT NULL,
  PRIMARY KEY(message_id, ordinal),
  FOREIGN KEY(message_id) REFERENCES messages(id),
  FOREIGN KEY(artifact_id) REFERENCES artifacts(id)
);
CREATE TABLE IF NOT EXISTS audit_timestamps (
  entity_type TEXT NOT NULL,
  entity_id TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY(entity_type, entity_id)
);
CREATE TABLE IF NOT EXISTS project_agents (
  project_id TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  role TEXT,
  specialty TEXT,
  system_prompt TEXT,
  enabled INTEGER NOT NULL,
  workspace_access TEXT NOT NULL,
  model_selection_mode TEXT NOT NULL DEFAULT 'inherit',
  model_id TEXT,
  candidate_model_list_mode TEXT NOT NULL DEFAULT 'inherit',
  candidate_model_list_revision INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY(project_id, agent_id),
  FOREIGN KEY(project_id) REFERENCES projects(id),
  FOREIGN KEY(agent_id) REFERENCES agents(id)
);
CREATE TABLE IF NOT EXISTS identity_model_options (
  id TEXT PRIMARY KEY,
  identity_scope TEXT NOT NULL CHECK(identity_scope IN ('base_agent', 'project_agent', 'conversation_agent')),
  agent_id TEXT NOT NULL,
  project_id TEXT,
  conversation_id TEXT,
  connector_id TEXT NOT NULL,
  model_id TEXT NOT NULL,
  display_name TEXT NOT NULL,
  source TEXT NOT NULL CHECK(source IN ('runtime', 'config', 'manual')),
  availability TEXT NOT NULL CHECK(availability IN ('available', 'unverified', 'unavailable')),
  is_default INTEGER NOT NULL DEFAULT 0 CHECK(is_default IN (0, 1)),
  sort_order INTEGER NOT NULL DEFAULT 0,
  catalog_revision TEXT,
  context_window INTEGER,
  reasoning_efforts_json TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(reasoning_efforts_json)),
  service_tiers_json TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(service_tiers_json)),
  FOREIGN KEY(agent_id) REFERENCES agents(id),
  FOREIGN KEY(project_id) REFERENCES projects(id),
  FOREIGN KEY(conversation_id) REFERENCES conversations(id),
  CHECK(
    (identity_scope = 'base_agent' AND project_id IS NULL AND conversation_id IS NULL)
    OR (identity_scope = 'project_agent' AND project_id IS NOT NULL AND conversation_id IS NULL)
    OR (identity_scope = 'conversation_agent' AND project_id IS NULL AND conversation_id IS NOT NULL)
  )
);
CREATE INDEX IF NOT EXISTS idx_identity_model_options_target
  ON identity_model_options(identity_scope, agent_id, project_id, conversation_id, sort_order, model_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_identity_model_options_target_model
  ON identity_model_options(identity_scope, agent_id, COALESCE(project_id, ''), COALESCE(conversation_id, ''), connector_id, model_id);
CREATE TABLE IF NOT EXISTS collaboration_runs (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL,
  root_agent_ids_json TEXT NOT NULL,
  call_count INTEGER NOT NULL,
  max_calls INTEGER NOT NULL,
  depth INTEGER NOT NULL,
  max_depth INTEGER NOT NULL,
  status TEXT NOT NULL,
  stop_reason TEXT,
  auto_dispatch_handoffs INTEGER NOT NULL DEFAULT 0,
  FOREIGN KEY(project_id) REFERENCES projects(id)
);
CREATE TABLE IF NOT EXISTS handoffs (
  id TEXT PRIMARY KEY,
  collaboration_run_id TEXT NOT NULL,
  from_execution_run_id TEXT NOT NULL,
  to_agent_id TEXT NOT NULL,
  status TEXT NOT NULL,
  details_json TEXT,
  FOREIGN KEY(collaboration_run_id) REFERENCES collaboration_runs(id),
  FOREIGN KEY(from_execution_run_id) REFERENCES execution_runs(id),
  FOREIGN KEY(to_agent_id) REFERENCES agents(id)
);
CREATE TABLE IF NOT EXISTS workspace_authorizations (
  project_id TEXT PRIMARY KEY,
  canonical_root TEXT NOT NULL,
  revision INTEGER NOT NULL,
  validation_status TEXT NOT NULL,
  FOREIGN KEY(project_id) REFERENCES projects(id)
);
CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
  content,
  content='messages',
  content_rowid='rowid'
);
CREATE TRIGGER IF NOT EXISTS messages_ai AFTER INSERT ON messages BEGIN
  INSERT INTO messages_fts(rowid, content) VALUES (new.rowid, new.content);
END;
CREATE TRIGGER IF NOT EXISTS messages_ad AFTER DELETE ON messages BEGIN
  INSERT INTO messages_fts(messages_fts, rowid, content) VALUES ('delete', old.rowid, old.content);
END;
CREATE TRIGGER IF NOT EXISTS messages_au AFTER UPDATE OF content ON messages BEGIN
  INSERT INTO messages_fts(messages_fts, rowid, content) VALUES ('delete', old.rowid, old.content);
  INSERT INTO messages_fts(rowid, content) VALUES (new.rowid, new.content);
END;
INSERT INTO messages_fts(messages_fts) VALUES ('rebuild');
"#;

// v12 records the post-v11 bookkeeping and scope change as a new, additive
// migration. The statements are applied conditionally because v11 databases
// can already contain one or more of these objects after an interrupted
// startup attempt.
const MIGRATION_V12_SQL: &str = r#"
ALTER TABLE schema_migrations ADD COLUMN dirty INTEGER NOT NULL DEFAULT 0;
CREATE TABLE IF NOT EXISTS migration_lock (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  owner TEXT NOT NULL,
  acquired_at INTEGER NOT NULL
);
ALTER TABLE summaries RENAME TO summaries_v12_legacy;
CREATE TABLE summaries (
  id TEXT PRIMARY KEY,
  scope_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  content_hash TEXT NOT NULL,
  artifact_id TEXT
);
INSERT INTO summaries(id, scope_id, version, content_hash, artifact_id)
  SELECT id, scope_id, version, content_hash, artifact_id FROM summaries_v12_legacy;
DROP TABLE summaries_v12_legacy;
"#;

// v13 extends a context manifest with the Connector identity that was frozen
// for its execution. v11/v12 text stays immutable because existing databases
// verify those historical checksums before any newer migration can run.
const MIGRATION_V13_SQL: &str = r#"
ALTER TABLE context_manifests ADD COLUMN connector_id TEXT;
"#;

// v14 adds the durable, non-secret record for one locally verified Agent
// import. Historical migration text is immutable: a v14 database still
// verifies the v11-v13 checksums before this transaction is applied.
const MIGRATION_V14_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS connector_adapter_bindings (
  scope_id TEXT NOT NULL CHECK(scope_id = 'desktop'),
  connector_id TEXT NOT NULL,
  adapter_kind TEXT NOT NULL,
  protocol_major INTEGER NOT NULL,
  manifest_id TEXT NOT NULL,
  manifest_sha256 TEXT NOT NULL,
  candidate_binding_digest TEXT NOT NULL,
  capabilities_json TEXT NOT NULL CHECK(json_valid(capabilities_json)),
  auth_required INTEGER NOT NULL CHECK(auth_required IN (0, 1)),
  PRIMARY KEY(scope_id, connector_id),
  FOREIGN KEY(scope_id, connector_id) REFERENCES connector_profiles(scope_id, connector_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_connector_adapter_bindings_candidate
  ON connector_adapter_bindings(candidate_binding_digest);
CREATE TABLE IF NOT EXISTS local_agent_imports (
  import_id TEXT PRIMARY KEY,
  scope_id TEXT NOT NULL CHECK(scope_id = 'desktop'),
  client_id TEXT NOT NULL,
  request_id TEXT NOT NULL,
  payload_hash TEXT NOT NULL,
  candidate_binding_digest TEXT NOT NULL,
  project_id TEXT NOT NULL,
  connector_id TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  result_json TEXT NOT NULL CHECK(json_valid(result_json)),
  created_at INTEGER NOT NULL,
  UNIQUE(scope_id, client_id, request_id),
  UNIQUE(candidate_binding_digest, project_id),
  FOREIGN KEY(project_id) REFERENCES projects(id),
  FOREIGN KEY(agent_id) REFERENCES agents(id),
  FOREIGN KEY(scope_id, connector_id) REFERENCES connector_profiles(scope_id, connector_id)
);
"#;

// v15 adds the C2-B orchestration journal facts. Historical migration text
// remains immutable: v11-v14 checksums are still verified first.
const MIGRATION_V15_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS orchestration_runs (
  run_id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('pending','running','awaiting_approval','cancelling','completed','failed','cancelled')),
  version INTEGER NOT NULL,
  brief_snapshot_id TEXT NOT NULL,
  brief_tree_digest TEXT NOT NULL,
  dag_snapshot_digest TEXT NOT NULL,
  role_binding_snapshot_digest TEXT NOT NULL,
  coordinator_generation INTEGER NOT NULL DEFAULT 1,
  terminal_reason TEXT
);
CREATE TABLE IF NOT EXISTS orchestration_task_nodes (
  node_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  node_key TEXT NOT NULL,
  required INTEGER NOT NULL CHECK(required IN (0, 1)),
  status TEXT NOT NULL CHECK(status IN ('pending','ready','blocked','leased','running','completed','failed','cancelled')),
  version INTEGER NOT NULL,
  active_attempt_id TEXT,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 1,
  input_artifact_set_digest TEXT,
  role_id TEXT,
  acceptance_contract_ref TEXT,
  terminal_reason TEXT,
  UNIQUE(run_id, node_key)
);
CREATE TABLE IF NOT EXISTS orchestration_task_attempts (
  attempt_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  node_id TEXT NOT NULL,
  attempt_no INTEGER NOT NULL,
  from_execution_run_id TEXT,
  status TEXT NOT NULL CHECK(status IN ('leased','running','completed','failed','cancelled')),
  lease_epoch INTEGER NOT NULL DEFAULT 0,
  artifact_set_digest TEXT,
  acceptance_evidence_digest TEXT,
  terminal_reason TEXT,
  terminal_identity_json TEXT,
  UNIQUE(node_id, attempt_no)
);
CREATE TABLE IF NOT EXISTS orchestration_milestones (
  milestone_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  milestone_key TEXT NOT NULL,
  required INTEGER NOT NULL CHECK(required IN (0, 1)),
  status TEXT NOT NULL CHECK(status IN ('awaiting_approval','approved','rejected')),
  version INTEGER NOT NULL,
  brief_tree_digest TEXT,
  presented_artifact_set_digest TEXT,
  acceptance_evidence_digest TEXT,
  terminal_reason TEXT,
  UNIQUE(run_id, milestone_key)
);
CREATE TABLE IF NOT EXISTS orchestration_edges (
  edge_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  from_node_id TEXT NOT NULL,
  to_node_id TEXT NOT NULL,
  dag_snapshot_digest TEXT NOT NULL,
  allowed_consumer_json TEXT NOT NULL CHECK(json_valid(allowed_consumer_json)),
  UNIQUE(run_id, from_node_id, to_node_id)
);
CREATE TABLE IF NOT EXISTS orchestration_edge_ports (
  edge_port_id TEXT PRIMARY KEY,
  edge_id TEXT NOT NULL,
  source_output_port_id TEXT NOT NULL,
  target_input_port_id TEXT NOT NULL,
  port_policy_json TEXT NOT NULL CHECK(json_valid(port_policy_json)),
  UNIQUE(edge_id, source_output_port_id, target_input_port_id)
);
CREATE TABLE IF NOT EXISTS orchestration_artifact_bindings (
  binding_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  delivery_id TEXT NOT NULL,
  edge_port_id TEXT NOT NULL,
  object_ref TEXT NOT NULL,
  sha256 TEXT NOT NULL,
  size INTEGER NOT NULL CHECK(size >= 0),
  normalized_content_type TEXT,
  normalized_content_type_policy_version TEXT,
  content_schema_ref_json TEXT,
  UNIQUE(delivery_id, edge_port_id)
);
CREATE TABLE IF NOT EXISTS orchestration_role_binding_snapshots (
  role_binding_snapshot_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  digest TEXT NOT NULL,
  sealed_at INTEGER NOT NULL,
  role_id TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  workspace_access TEXT NOT NULL,
  UNIQUE(run_id, role_id, agent_id)
);
CREATE TABLE IF NOT EXISTS orchestration_human_receipts (
  receipt_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  milestone_id TEXT NOT NULL,
  request_id TEXT NOT NULL,
  semantic_payload_hash TEXT NOT NULL,
  decision TEXT NOT NULL CHECK(decision IN ('approve','reject')),
  expected_version INTEGER NOT NULL,
  brief_tree_digest TEXT NOT NULL,
  presented_artifact_set_digest TEXT NOT NULL,
  acceptance_evidence_digest TEXT NOT NULL,
  authenticated_principal TEXT NOT NULL,
  core_timestamp INTEGER NOT NULL,
  UNIQUE(milestone_id, request_id)
);
CREATE TABLE IF NOT EXISTS orchestration_leases (
  attempt_id TEXT NOT NULL,
  run_id TEXT NOT NULL,
  node_id TEXT NOT NULL,
  lease_epoch INTEGER NOT NULL,
  lease_owner TEXT NOT NULL,
  heartbeat_at INTEGER NOT NULL,
  deadline INTEGER NOT NULL,
  coordinator_generation INTEGER NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('active','expired','released','fenced')),
  PRIMARY KEY(attempt_id, lease_epoch, coordinator_generation)
);
CREATE TABLE IF NOT EXISTS orchestration_handoff_deliveries (
  delivery_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  attempt_id TEXT NOT NULL,
  edge_id TEXT NOT NULL,
  lease_epoch INTEGER NOT NULL,
  declaration_digest TEXT NOT NULL,
  artifact_transfer_set_digest TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  delivery_payload_digest TEXT NOT NULL,
  envelope_object_ref TEXT NOT NULL,
  envelope_raw_sha256 TEXT NOT NULL,
  envelope_sha256_jcs TEXT NOT NULL,
  acceptance_contract_ref TEXT NOT NULL,
  acceptance_contract_digest TEXT NOT NULL,
  acceptance_evidence_ref TEXT NOT NULL,
  acceptance_evidence_digest TEXT NOT NULL,
  producer_context_manifest_digest TEXT NOT NULL,
  replay_receipt_json TEXT,
  UNIQUE(attempt_id, edge_id, lease_epoch)
);
CREATE TABLE IF NOT EXISTS orchestration_context_manifest_authorities (
  context_manifest_ref_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  attempt_id TEXT NOT NULL,
  producer_context_manifest_digest TEXT NOT NULL,
  sealed_at INTEGER NOT NULL,
  UNIQUE(attempt_id, producer_context_manifest_digest)
);
"#;

// v16 adds the ADR-002 orchestration audit sink and corrects the v15
// orchestrator status sets. v15 text is preserved; v16 rebuilds only the
// v15 orchestrator tables that carried provisional status names.
const MIGRATION_V16_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS orchestration_audit_events (
  event_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  sequence INTEGER NOT NULL CHECK(sequence >= 0),
  event_type TEXT NOT NULL,
  schema_version TEXT NOT NULL,
  subject_kind TEXT NOT NULL,
  subject_id TEXT NOT NULL,
  payload_json TEXT NOT NULL CHECK(json_valid(payload_json)),
  payload_sha256 TEXT NOT NULL CHECK(length(payload_sha256) = 64 AND payload_sha256 = lower(payload_sha256)),
  idempotency_key TEXT NOT NULL,
  coordinator_generation INTEGER NOT NULL CHECK(coordinator_generation >= 0),
  core_timestamp INTEGER NOT NULL CHECK(core_timestamp >= 0),
  UNIQUE(run_id, sequence),
  UNIQUE(run_id, idempotency_key)
);
CREATE TRIGGER IF NOT EXISTS orchestration_audit_events_no_update
BEFORE UPDATE ON orchestration_audit_events
BEGIN
  SELECT RAISE(ABORT, 'orchestration_audit_events is append-only');
END;
CREATE TRIGGER IF NOT EXISTS orchestration_audit_events_no_delete
BEFORE DELETE ON orchestration_audit_events
BEGIN
  SELECT RAISE(ABORT, 'orchestration_audit_events is append-only');
END;

ALTER TABLE orchestration_runs RENAME TO orchestration_runs_v15;
CREATE TABLE orchestration_runs (
  run_id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('pending','running','awaiting_approval','cancelling','completed','failed','cancelled')),
  version INTEGER NOT NULL,
  brief_snapshot_id TEXT NOT NULL,
  brief_tree_digest TEXT NOT NULL,
  dag_snapshot_digest TEXT NOT NULL,
  role_binding_snapshot_digest TEXT NOT NULL,
  coordinator_generation INTEGER NOT NULL DEFAULT 1,
  terminal_reason TEXT
);
INSERT INTO orchestration_runs(
  run_id, project_id, status, version, brief_snapshot_id,
  brief_tree_digest, dag_snapshot_digest, role_binding_snapshot_digest,
  coordinator_generation, terminal_reason
)
SELECT run_id, project_id, status, version, brief_snapshot_id,
  brief_tree_digest, dag_snapshot_digest, role_binding_snapshot_digest,
  coordinator_generation, terminal_reason
FROM orchestration_runs_v15;
DROP TABLE orchestration_runs_v15;

ALTER TABLE orchestration_role_binding_snapshots RENAME TO orchestration_role_binding_snapshots_v15;
CREATE TABLE orchestration_role_binding_snapshots (
  role_binding_snapshot_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  digest TEXT NOT NULL,
  sealed_at INTEGER NOT NULL,
  role_id TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  workspace_access TEXT NOT NULL,
  UNIQUE(run_id, role_id, agent_id)
);
INSERT INTO orchestration_role_binding_snapshots(
  role_binding_snapshot_id, run_id, digest, sealed_at,
  role_id, agent_id, workspace_access
)
SELECT role_binding_snapshot_id, run_id, digest, sealed_at,
  role_id, agent_id, workspace_access
FROM orchestration_role_binding_snapshots_v15;
DROP TABLE orchestration_role_binding_snapshots_v15;

ALTER TABLE orchestration_task_nodes RENAME TO orchestration_task_nodes_v15;
CREATE TABLE orchestration_task_nodes (
  node_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  node_key TEXT NOT NULL,
  required INTEGER NOT NULL CHECK(required IN (0, 1)),
  status TEXT NOT NULL CHECK(status IN ('pending','ready','running','sealing','completed','failed','blocked','cancelled')),
  version INTEGER NOT NULL,
  active_attempt_id TEXT,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 1,
  input_artifact_set_digest TEXT,
  role_id TEXT,
  acceptance_contract_ref TEXT,
  terminal_reason TEXT,
  UNIQUE(run_id, node_key)
);
CREATE TRIGGER orchestration_task_nodes_v15_guard
BEFORE INSERT ON orchestration_task_nodes_v15
WHEN NEW.status NOT IN ('pending','ready','running','sealing','completed','failed','blocked','cancelled')
BEGIN
  SELECT RAISE(ABORT, 'v15 orchestration_task_nodes contains a status that v16 cannot map; manual recovery required');
END;
INSERT INTO orchestration_task_nodes(
  node_id, run_id, node_key, required, status, version, active_attempt_id,
  attempt_count, max_attempts, input_artifact_set_digest, role_id,
  acceptance_contract_ref, terminal_reason
)
SELECT
  node_id, run_id, node_key, required, status, version, active_attempt_id,
  attempt_count, max_attempts, input_artifact_set_digest, role_id,
  acceptance_contract_ref, terminal_reason
FROM orchestration_task_nodes_v15;
DROP TRIGGER orchestration_task_nodes_v15_guard;
DROP TABLE orchestration_task_nodes_v15;

ALTER TABLE orchestration_task_attempts RENAME TO orchestration_task_attempts_v15;
CREATE TABLE orchestration_task_attempts (
  attempt_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  node_id TEXT NOT NULL,
  attempt_no INTEGER NOT NULL,
  from_execution_run_id TEXT,
  status TEXT NOT NULL CHECK(status IN ('leased','running','sealing','completed','failed','cancelled','interrupted')),
  lease_epoch INTEGER NOT NULL DEFAULT 0,
  artifact_set_digest TEXT,
  acceptance_evidence_digest TEXT,
  terminal_reason TEXT,
  terminal_identity_json TEXT,
  UNIQUE(node_id, attempt_no)
);
CREATE TRIGGER orchestration_task_attempts_v15_guard
BEFORE INSERT ON orchestration_task_attempts_v15
WHEN NEW.status NOT IN ('leased','running','sealing','completed','failed','cancelled','interrupted')
BEGIN
  SELECT RAISE(ABORT, 'v15 orchestration_task_attempts contains a status that v16 cannot map; manual recovery required');
END;
INSERT INTO orchestration_task_attempts(
  attempt_id, run_id, node_id, attempt_no, from_execution_run_id, status,
  lease_epoch, artifact_set_digest, acceptance_evidence_digest,
  terminal_reason, terminal_identity_json
)
SELECT
  attempt_id, run_id, node_id, attempt_no, from_execution_run_id, status,
  lease_epoch, artifact_set_digest, acceptance_evidence_digest,
  terminal_reason, terminal_identity_json
FROM orchestration_task_attempts_v15;
DROP TRIGGER orchestration_task_attempts_v15_guard;
DROP TABLE orchestration_task_attempts_v15;

ALTER TABLE orchestration_milestones RENAME TO orchestration_milestones_v15;
CREATE TABLE orchestration_milestones (
  milestone_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  milestone_key TEXT NOT NULL,
  required INTEGER NOT NULL CHECK(required IN (0, 1)),
  status TEXT NOT NULL CHECK(status IN ('pending','awaiting_approval','approved','rejected','cancelled')),
  version INTEGER NOT NULL,
  brief_tree_digest TEXT NOT NULL,
  presented_artifact_set_digest TEXT NOT NULL,
  acceptance_evidence_digest TEXT NOT NULL,
  terminal_reason TEXT,
  UNIQUE(run_id, milestone_key)
);
CREATE TRIGGER orchestration_milestones_v15_guard
BEFORE INSERT ON orchestration_milestones_v15
WHEN NEW.status NOT IN ('pending','awaiting_approval','approved','rejected','cancelled')
   OR NEW.brief_tree_digest IS NULL
   OR NEW.presented_artifact_set_digest IS NULL
   OR NEW.acceptance_evidence_digest IS NULL
BEGIN
  SELECT RAISE(ABORT, 'v15 orchestration_milestones contains unmappable status or NULL sealed digest; manual recovery required');
END;
INSERT INTO orchestration_milestones(
  milestone_id, run_id, milestone_key, required, status, version,
  brief_tree_digest, presented_artifact_set_digest, acceptance_evidence_digest,
  terminal_reason
)
SELECT
  milestone_id, run_id, milestone_key, required, status, version,
  brief_tree_digest, presented_artifact_set_digest, acceptance_evidence_digest,
  terminal_reason
FROM orchestration_milestones_v15;
DROP TRIGGER orchestration_milestones_v15_guard;
DROP TABLE orchestration_milestones_v15;

ALTER TABLE orchestration_handoff_deliveries RENAME TO orchestration_handoff_deliveries_v15;
CREATE TABLE orchestration_handoff_deliveries (
  delivery_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  attempt_id TEXT NOT NULL,
  edge_id TEXT NOT NULL,
  lease_epoch INTEGER NOT NULL,
  envelope_handoff_id TEXT NOT NULL,
  from_task_node_id TEXT NOT NULL,
  from_execution_run_id TEXT NOT NULL,
  to_task_node_id TEXT NOT NULL,
  lease_owner TEXT NOT NULL,
  coordinator_generation INTEGER NOT NULL,
  dag_snapshot_digest TEXT NOT NULL,
  role_binding_snapshot_digest TEXT NOT NULL,
  declaration_digest TEXT NOT NULL,
  artifact_transfer_set_digest TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  delivery_payload_digest TEXT NOT NULL,
  envelope_object_ref TEXT NOT NULL,
  envelope_raw_sha256 TEXT NOT NULL,
  envelope_sha256_jcs TEXT NOT NULL,
  acceptance_contract_ref TEXT NOT NULL,
  acceptance_contract_digest TEXT NOT NULL,
  acceptance_evidence_ref TEXT NOT NULL,
  acceptance_evidence_digest TEXT NOT NULL,
  producer_context_manifest_digest TEXT NOT NULL,
  replay_receipt_json TEXT,
  UNIQUE(attempt_id, edge_id, lease_epoch)
);
DROP TABLE orchestration_handoff_deliveries_v15;
ALTER TABLE orchestration_artifact_bindings RENAME TO orchestration_artifact_bindings_v15;
CREATE TABLE orchestration_artifact_bindings (
  binding_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  delivery_id TEXT NOT NULL,
  edge_port_id TEXT NOT NULL,
  source_output_port_id TEXT NOT NULL,
  target_input_port_id TEXT NOT NULL,
  object_ref TEXT NOT NULL,
  sha256 TEXT NOT NULL,
  size INTEGER NOT NULL CHECK(size >= 0),
  content_schema_id TEXT NOT NULL,
  content_schema_version TEXT NOT NULL,
  content_schema_digest TEXT NOT NULL,
  normalized_content_type TEXT NOT NULL,
  normalized_content_type_policy_version TEXT NOT NULL,
  content_schema_ref_json TEXT NOT NULL CHECK(json_valid(content_schema_ref_json)),
  UNIQUE(delivery_id, edge_port_id)
);
DROP TABLE orchestration_artifact_bindings_v15;
"#;

/// Non-secret durable ACP binding metadata. It deliberately has no path,
/// endpoint, process, environment, credential, or raw runtime JSON field.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalAgentAdapterBinding {
    pub adapter_kind: String,
    pub protocol_major: u16,
    pub manifest_id: String,
    pub manifest_sha256: String,
    pub candidate_binding_digest: String,
    pub capabilities_json: String,
    pub auth_required: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalAgentImportRequest {
    pub import_id: String,
    pub scope_id: String,
    pub client_id: String,
    pub request_id: String,
    pub payload_hash: String,
    pub project_id: String,
    pub connector: ConnectorProfile,
    pub agent_id: String,
    pub agent_name: String,
    pub binding: LocalAgentAdapterBinding,
    pub model_selection: ModelSelection,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalAgentImportOutcome {
    pub import_id: String,
    pub connector_id: String,
    pub agent_id: String,
    pub project_id: String,
    pub reused: bool,
    pub event_sequence: u64,
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("migration checksum mismatch for version {version}")]
    MigrationChecksumMismatch { version: i64 },
    #[error("schema migration version {version} is dirty and requires recovery")]
    MigrationDirty { version: i64 },
    #[error(
        "execution projection rejected because an older or terminal row cannot be overwritten"
    )]
    ProjectionRejected,
    #[error("model snapshot is invalid: {reason}")]
    ModelSnapshotInvalid { reason: String },
    #[error("model snapshot conflicts with existing run: {id}")]
    ModelSnapshotConflict { id: String },
    #[error("model snapshot run does not exist: {id}")]
    ModelSnapshotRunNotFound { id: String },
    #[error("model selection is invalid: {reason}")]
    ModelSelectionInvalid { reason: String },
    #[error("model selection snapshot conflicts with existing run: {id}")]
    ModelSelectionSnapshotConflict { id: String },
    #[error("model selection snapshot run does not exist: {id}")]
    ModelSelectionSnapshotRunNotFound { id: String },
    #[error("identity model target does not exist: {id}")]
    IdentityModelTargetNotFound { id: String },
    #[error("identity model option conflicts with existing data: {id}")]
    IdentityModelOptionConflict { id: String },
    #[error("invalid command receipt state: {state}")]
    InvalidCommandReceiptState { state: String },
    #[error("memory id already exists with different data: {id}")]
    MemoryConflict { id: String },
    #[error("summary scope does not exist: {id}")]
    SummaryScopeNotFound { id: String },
    #[error("summary id already exists with different data: {id}")]
    SummaryConflict { id: String },
    #[error("summary artifact does not exist: {id}")]
    SummaryArtifactNotFound { id: String },
    #[error("summary artifact metadata does not match its content hash: {id}")]
    SummaryArtifactMismatch { id: String },
    #[error("summary content is not available: {id}")]
    SummaryContentUnavailable { id: String },
    #[error("artifact metadata is invalid: {reason}")]
    ArtifactInvalid { reason: String },
    #[error("artifact id already exists with different data: {id}")]
    ArtifactConflict { id: String },
    #[error("attachment metadata is invalid: {reason}")]
    AttachmentInvalid { reason: String },
    #[error("attachment id already exists with different data: {id}")]
    AttachmentConflict { id: String },
    #[error("attachment message does not exist: {id}")]
    AttachmentMessageNotFound { id: String },
    #[error("attachment artifact does not exist: {id}")]
    AttachmentArtifactNotFound { id: String },
    #[error("attachment metadata does not match its artifact: {id}")]
    AttachmentArtifactMismatch { id: String },
    #[error("artifact body store is unavailable")]
    ArtifactBodyStoreUnavailable,
    #[error("orchestration run already exists with different brief snapshot: {run_id}")]
    OrchestrationRunConflict { run_id: String },
    #[error("orchestration run does not exist: {run_id}")]
    OrchestrationRunNotFound { run_id: String },
    #[error("stale lease epoch for attempt {attempt_id}")]
    StaleLease { attempt_id: String },
    #[error("orchestration task is terminal: {node_id}")]
    OrchestrationTaskTerminal { node_id: String },
    #[error("orchestration task does not exist: {node_id}")]
    OrchestrationTaskNotFound { node_id: String },
    #[error("orchestration milestone does not exist: {milestone_id}")]
    OrchestrationMilestoneNotFound { milestone_id: String },
    #[error(
        "orchestration human receipt conflict for milestone {milestone_id} request {request_id}"
    )]
    HumanReceiptConflict {
        milestone_id: String,
        request_id: String,
    },
    #[error(
        "orchestration handoff delivery conflict for attempt {attempt_id} edge {edge_id} epoch {lease_epoch}"
    )]
    HandoffDeliveryConflict {
        attempt_id: String,
        edge_id: String,
        lease_epoch: i64,
    },
    #[error("orchestration run status {status} is invalid for run {run_id}")]
    OrchestrationRunStatusInvalid { run_id: String, status: String },
    #[error("orchestration milestone {milestone_id} has invalid state {status}")]
    OrchestrationMilestoneStateInvalid {
        milestone_id: String,
        status: String,
    },
    #[error("orchestration run has active attempt: {run_id}")]
    OrchestrationActiveAttemptExists { run_id: String },
    #[error("orchestration task {node_id} is not ready (status {status})")]
    OrchestrationTaskNotReady { node_id: String, status: String },
    #[error("orchestration artifact binding is invalid: {reason}")]
    OrchestrationArtifactBindingInvalid { reason: String },
    #[error("orchestration audit payload canonicalization failed: {reason}")]
    AuditPayloadCanonicalization { reason: String },
    #[error("v15 orchestrator state cannot be mapped to v16: {detail}")]
    MigrationInvalidV15State { detail: String },
    #[error("artifact body is not registered: {id}")]
    ArtifactBodyNotFound { id: String },
    #[error("artifact body does not match its registered metadata")]
    ArtifactBodyMismatch,
    #[error("artifact body exceeds the configured size limit")]
    ArtifactBodyTooLarge,
    #[error("artifact body content range is invalid")]
    ArtifactBodyRangeInvalid,
    #[error("artifact body store I/O failed")]
    ArtifactBodyIo,
    #[error("artifact source file is invalid or unavailable")]
    ArtifactSourceInvalid,
    #[error("retrieval source scope does not exist: {id}")]
    RetrievalScopeNotFound { id: String },
    #[error("retrieval source id already exists with different data: {id}")]
    RetrievalConflict { id: String },
    #[error("retrieval selection scope is invalid: {id}")]
    RetrievalSelectionScopeInvalid { id: String },
    #[error("retrieval selection rejected for {id}: {reason}")]
    RetrievalSelectionInvalid { id: String, reason: String },
    #[error("retrieval selection source does not exist: {id}")]
    RetrievalSelectionSourceNotFound { id: String },
    #[error("retrieval selection source is outside its scope: {id}")]
    RetrievalSelectionSourceOutOfScope { id: String },
    #[error("retrieval selection source hash changed: {id}")]
    RetrievalSelectionSourceChanged { id: String },
    #[error("retrieval selection id already exists with different data: {id}")]
    RetrievalSelectionConflict { id: String },
    #[error("retrieval feedback selection does not exist: {id}")]
    RetrievalFeedbackSelectionNotFound { id: String },
    #[error("retrieval feedback source was not selected: {id}")]
    RetrievalFeedbackSourceNotSelected { id: String },
    #[error("retrieval feedback scope does not match selection: {id}")]
    RetrievalFeedbackScopeMismatch { id: String },
    #[error("retrieval feedback rejected for {id}: {reason}")]
    RetrievalFeedbackInvalid { id: String, reason: String },
    #[error("retrieval feedback id already exists with different data: {id}")]
    RetrievalFeedbackConflict { id: String },
    #[error("retrieval preview rejected: {reason}")]
    RetrievalPreviewInvalid { reason: String },
    #[error("connector profile scope is invalid: {scope}")]
    ConnectorProfileScopeInvalid { scope: String },
    #[error("connector profile field {field} is invalid: {reason}")]
    ConnectorProfileInvalid { field: String, reason: String },
    #[error("connector profile already exists with different metadata: {id}")]
    ConnectorProfileConflict { id: String },
    #[error("connector profile does not exist: {id}")]
    ConnectorProfileNotFound { id: String },
    #[error("local Agent import is invalid: {field}")]
    LocalAgentImportInvalid { field: String },
    #[error("local Agent import request id conflicts with a different payload")]
    LocalAgentImportRequestConflict,
    #[error("local Agent import model selection conflicts with the existing assignment")]
    LocalAgentImportModelSelectionConflict,
    #[error("local Agent import binding conflicts with existing metadata")]
    LocalAgentImportBindingConflict,
    #[error("context manifest id already exists with different data: {id}")]
    ContextManifestConflict { id: String },
    #[error("project does not exist: {id}")]
    ProjectNotFound { id: String },
    #[error("workflow step agent is not in the Project roster: workflow {workflow_id}, project {project_id}, agent {agent_id}")]
    WorkflowAgentNotInProject {
        workflow_id: String,
        project_id: String,
        agent_id: String,
    },
    #[error("workflow id already exists with different data: {id}")]
    WorkflowConflict { id: String },
    #[error("collaboration Project does not exist: {id}")]
    CollaborationProjectNotFound { id: String },
    #[error(
        "collaboration root Agent is not in the enabled Project roster: project {project_id}, agent {agent_id}"
    )]
    CollaborationAgentNotInProject {
        project_id: String,
        agent_id: String,
    },
    #[error("collaboration id already exists with different data: {id}")]
    CollaborationConflict { id: String },
    #[error("handoff id already exists with different data: {id}")]
    HandoffConflict { id: String },
    #[error("handoff contract rejected for {id}: {reason}")]
    HandoffContractRejected { id: String, reason: String },
    #[error("handoff dispatch rejected for {id}: {reason}")]
    HandoffDispatchRejected { id: String, reason: String },
    #[error("handoff collaboration run does not exist: {id}")]
    HandoffCollaborationNotFound { id: String },
    #[error("handoff source execution run does not exist: {id}")]
    HandoffExecutionNotFound { id: String },
    #[error(
        "handoff target Agent is not in the enabled Project roster: project {project_id}, agent {agent_id}"
    )]
    HandoffAgentNotInProject {
        project_id: String,
        agent_id: String,
    },
    #[error("handoff does not exist: {id}")]
    HandoffNotFound { id: String },
    #[error("invalid handoff transition for {id}: {from_status} -> {target_status}")]
    HandoffInvalidTransition {
        id: String,
        from_status: String,
        target_status: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandReceiptKey {
    pub scope_id: String,
    pub client_id: String,
    pub request_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandReceiptState {
    Pending,
    InProgress,
    Completed,
    Failed,
    Interrupted,
}

impl CommandReceiptState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::InProgress => "in_progress",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Interrupted => "interrupted",
        }
    }
}

impl FromStr for CommandReceiptState {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "pending" => Ok(Self::Pending),
            "in_progress" => Ok(Self::InProgress),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "interrupted" => Ok(Self::Interrupted),
            _ => Err(value.to_owned()),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CommandReceipt {
    pub key: CommandReceiptKey,
    pub command: String,
    pub payload_hash: String,
    pub operation_key: String,
    pub state: CommandReceiptState,
    pub result_json: Option<serde_json::Value>,
    pub error_json: Option<serde_json::Value>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AgentModelBinding {
    pub connector_id: Option<String>,
    pub model_id: Option<String>,
    pub candidate_model_list_revision: u64,
}

/// Presence-aware update intent for model binding fields. The IPC layer uses
/// this to distinguish omitted fields from explicit clears without inventing
/// a default Runtime or silently writing NULL.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BindingFieldPatch<T> {
    Preserve,
    Clear,
    Set(T),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AgentModelBindingPatch {
    pub connector_id: BindingFieldPatch<String>,
    pub model_id: BindingFieldPatch<String>,
    pub candidate_model_list_revision: BindingFieldPatch<u64>,
}

type IdentityModelOptionKey = (
    String,
    String,
    String,
    String,
    Option<String>,
    Option<String>,
);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredModelSelection {
    pub selection: ModelSelection,
    pub candidate_model_list_mode: IdentityModelListMode,
    pub candidate_model_list_revision: u64,
}

struct CommandReceiptRow {
    scope_id: String,
    client_id: String,
    request_id: String,
    command: String,
    payload_hash: String,
    operation_key: String,
    state: String,
    result_json: Option<String>,
    error_json: Option<String>,
    created_at: i64,
    updated_at: i64,
}

pub struct SqliteStore {
    pub(crate) connection: Connection,
    artifact_root: Option<PathBuf>,
}

/// Metadata returned after an explicitly selected file has been streamed into
/// the content-addressed Artifact Store. The source path is intentionally not
/// retained or exposed so restart recovery depends only on the verified blob.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImportedArtifactFile {
    pub sha256: String,
    pub size: u64,
    pub file_name: String,
    pub body_stored: bool,
}

/// One bounded read from a digest-addressed Artifact Store blob. The whole
/// body is intentionally not materialized in this value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactBodyChunk {
    pub artifact_id: String,
    pub sha256: String,
    pub offset: u64,
    pub size: u64,
    pub bytes: Vec<u8>,
    pub eof: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttachmentContextRecord {
    pub attachment_id: Option<String>,
    pub artifact_id: Option<String>,
    pub message_id: String,
    pub message_sequence: u64,
    pub ordinal: u64,
    pub file_name: String,
    pub sha256: String,
    pub size: u64,
    pub mime: Option<String>,
}

/// The bounded, exact retrieval request shared by Core and Storage. The
/// request deliberately carries the complete scope instead of accepting an
/// implicit global search.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetrievalPreviewRequest {
    pub expected_project_id: String,
    pub conversation_id: String,
    pub agent_id: String,
    pub query: String,
    pub scope: String,
    pub source_types: Vec<String>,
    pub limit: u64,
}

impl SqliteStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        Self::open_with_artifact_root(path, None)
    }

    /// Opens SQLite with an explicit, absolute root for Artifact Store blobs.
    /// `None` is intentional for in-memory/unit-test stores and makes body
    /// writes fail closed instead of falling back to cwd or the user profile.
    pub fn open_with_artifact_root(
        path: impl AsRef<Path>,
        artifact_root: Option<&Path>,
    ) -> Result<Self, StorageError> {
        if artifact_root.is_some_and(|root| !root.is_absolute()) {
            return Err(StorageError::ArtifactBodyStoreUnavailable);
        }
        let connection = Connection::open(path)?;
        let mut store = Self {
            connection,
            artifact_root: artifact_root.map(Path::to_path_buf),
        };
        store.configure()?;
        store.migrate()?;
        Ok(store)
    }

    pub fn open_in_memory() -> Result<Self, StorageError> {
        Self::open(":memory:")
    }

    fn configure(&mut self) -> Result<(), StorageError> {
        self.connection.pragma_update(None, "foreign_keys", "ON")?;
        self.connection.pragma_update(None, "journal_mode", "WAL")?;
        self.connection
            .busy_timeout(std::time::Duration::from_millis(5000))?;
        Ok(())
    }

    fn migrate(&mut self) -> Result<(), StorageError> {
        let v11_checksum = hex_digest(MIGRATION_V11_SQL.as_bytes());
        debug_assert_eq!(v11_checksum, HISTORICAL_V11_MIGRATION_CHECKSUM);

        // The bookkeeping table itself was part of the immutable v11
        // migration. Do not create or alter v12 state until the recorded v11
        // checksum has been verified.
        self.connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS schema_migrations(
                 version INTEGER PRIMARY KEY,
                 checksum TEXT NOT NULL,
                 applied_at INTEGER NOT NULL
             );",
        )?;
        let existing_v11: Option<String> = self
            .connection
            .query_row(
                "SELECT checksum FROM schema_migrations WHERE version = ?1",
                [V11_SCHEMA_VERSION],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(value) = existing_v11.as_deref() {
            if value != v11_checksum && value != MUTATED_V11_MIGRATION_CHECKSUM {
                return Err(StorageError::MigrationChecksumMismatch {
                    version: V11_SCHEMA_VERSION,
                });
            }
        }

        // v11 is kept byte-for-byte compatible with the version that Git
        // proves was applied to existing databases. Its additive ensure_column
        // operations were already part of the v11 implementation and remain
        // idempotent here.
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute_batch(MIGRATION_V11_SQL)?;
        ensure_column(
            &tx,
            "execution_runs",
            "scope_json",
            "ALTER TABLE execution_runs ADD COLUMN scope_json TEXT NOT NULL DEFAULT '{}'",
        )?;
        ensure_column(
            &tx,
            "execution_runs",
            "terminal",
            "ALTER TABLE execution_runs ADD COLUMN terminal INTEGER NOT NULL DEFAULT 0",
        )?;
        ensure_column(
            &tx,
            "execution_runs",
            "terminal_reason",
            "ALTER TABLE execution_runs ADD COLUMN terminal_reason TEXT",
        )?;
        ensure_column(
            &tx,
            "conversation_agents",
            "role",
            "ALTER TABLE conversation_agents ADD COLUMN role TEXT",
        )?;
        ensure_column(
            &tx,
            "conversation_agents",
            "specialty",
            "ALTER TABLE conversation_agents ADD COLUMN specialty TEXT",
        )?;
        ensure_column(
            &tx,
            "conversation_agents",
            "system_prompt",
            "ALTER TABLE conversation_agents ADD COLUMN system_prompt TEXT",
        )?;
        ensure_column(
            &tx,
            "conversation_agents",
            "enabled",
            "ALTER TABLE conversation_agents ADD COLUMN enabled INTEGER NOT NULL DEFAULT 1",
        )?;
        ensure_column(
            &tx,
            "conversation_agents",
            "model_selection_mode",
            "ALTER TABLE conversation_agents ADD COLUMN model_selection_mode TEXT NOT NULL DEFAULT 'inherit'",
        )?;
        ensure_column(
            &tx,
            "conversation_agents",
            "model_id",
            "ALTER TABLE conversation_agents ADD COLUMN model_id TEXT",
        )?;
        ensure_column(
            &tx,
            "conversation_agents",
            "candidate_model_list_mode",
            "ALTER TABLE conversation_agents ADD COLUMN candidate_model_list_mode TEXT NOT NULL DEFAULT 'inherit'",
        )?;
        ensure_column(
            &tx,
            "conversation_agents",
            "candidate_model_list_revision",
            "ALTER TABLE conversation_agents ADD COLUMN candidate_model_list_revision INTEGER NOT NULL DEFAULT 0",
        )?;
        ensure_column(
            &tx,
            "project_agents",
            "model_selection_mode",
            "ALTER TABLE project_agents ADD COLUMN model_selection_mode TEXT NOT NULL DEFAULT 'inherit'",
        )?;
        ensure_column(
            &tx,
            "project_agents",
            "model_id",
            "ALTER TABLE project_agents ADD COLUMN model_id TEXT",
        )?;
        ensure_column(
            &tx,
            "project_agents",
            "candidate_model_list_mode",
            "ALTER TABLE project_agents ADD COLUMN candidate_model_list_mode TEXT NOT NULL DEFAULT 'inherit'",
        )?;
        ensure_column(
            &tx,
            "project_agents",
            "candidate_model_list_revision",
            "ALTER TABLE project_agents ADD COLUMN candidate_model_list_revision INTEGER NOT NULL DEFAULT 0",
        )?;
        ensure_column(
            &tx,
            "agents",
            "connector_id",
            "ALTER TABLE agents ADD COLUMN connector_id TEXT",
        )?;
        ensure_column(
            &tx,
            "agents",
            "model_id",
            "ALTER TABLE agents ADD COLUMN model_id TEXT",
        )?;
        ensure_column(
            &tx,
            "agents",
            "candidate_model_list_revision",
            "ALTER TABLE agents ADD COLUMN candidate_model_list_revision INTEGER NOT NULL DEFAULT 0",
        )?;
        ensure_column(
            &tx,
            "collaboration_runs",
            "project_id",
            "ALTER TABLE collaboration_runs ADD COLUMN project_id TEXT NOT NULL DEFAULT ''",
        )?;
        ensure_column(
            &tx,
            "collaboration_runs",
            "root_agent_ids_json",
            "ALTER TABLE collaboration_runs ADD COLUMN root_agent_ids_json TEXT NOT NULL DEFAULT '[]'",
        )?;
        ensure_column(
            &tx,
            "collaboration_runs",
            "depth",
            "ALTER TABLE collaboration_runs ADD COLUMN depth INTEGER NOT NULL DEFAULT 0",
        )?;
        ensure_column(
            &tx,
            "collaboration_runs",
            "stop_reason",
            "ALTER TABLE collaboration_runs ADD COLUMN stop_reason TEXT",
        )?;
        ensure_column(
            &tx,
            "collaboration_runs",
            "auto_dispatch_handoffs",
            "ALTER TABLE collaboration_runs ADD COLUMN auto_dispatch_handoffs INTEGER NOT NULL DEFAULT 0",
        )?;
        ensure_column(
            &tx,
            "handoffs",
            "details_json",
            "ALTER TABLE handoffs ADD COLUMN details_json TEXT",
        )?;
        ensure_column(
            &tx,
            "context_manifests",
            "source_ledger_json",
            "ALTER TABLE context_manifests ADD COLUMN source_ledger_json TEXT NOT NULL DEFAULT '[]'",
        )?;
        ensure_column(
            &tx,
            "context_manifests",
            "model_id",
            "ALTER TABLE context_manifests ADD COLUMN model_id TEXT",
        )?;
        ensure_column(
            &tx,
            "summaries",
            "artifact_id",
            "ALTER TABLE summaries ADD COLUMN artifact_id TEXT",
        )?;
        ensure_column(
            &tx,
            "attachments",
            "attachment_id",
            "ALTER TABLE attachments ADD COLUMN attachment_id TEXT",
        )?;
        ensure_column(
            &tx,
            "attachments",
            "artifact_id",
            "ALTER TABLE attachments ADD COLUMN artifact_id TEXT",
        )?;
        tx.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_attachments_attachment_id
             ON attachments(attachment_id) WHERE attachment_id IS NOT NULL",
            [],
        )?;
        tx.execute(
            "CREATE INDEX IF NOT EXISTS idx_attachments_artifact_id ON attachments(artifact_id)",
            [],
        )?;
        tx.execute(
            "UPDATE execution_runs SET terminal = CASE WHEN lower(replace(status, '\"', '')) IN ('completed', 'failed', 'cancelled', 'interrupted') THEN 1 ELSE 0 END",
            [],
        )?;
        hydrate_missing_execution_scopes(&tx)?;
        let existing: Option<String> = tx
            .query_row(
                "SELECT checksum FROM schema_migrations WHERE version = ?1",
                [V11_SCHEMA_VERSION],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(value) = existing {
            if value != v11_checksum && value != MUTATED_V11_MIGRATION_CHECKSUM {
                return Err(StorageError::MigrationChecksumMismatch {
                    version: V11_SCHEMA_VERSION,
                });
            }
        } else {
            tx.execute(
                "INSERT INTO schema_migrations(version, checksum, applied_at) VALUES(?1, ?2, strftime('%s','now'))",
                params![V11_SCHEMA_VERSION, v11_checksum],
            )?;
        }
        tx.commit()?;

        self.migrate_v12()?;
        self.migrate_v13()?;
        self.migrate_v14()?;
        self.migrate_v15()?;
        self.migrate_v16()
    }

    fn migrate_v12(&mut self) -> Result<(), StorageError> {
        let checksum = hex_digest(MIGRATION_V12_SQL.as_bytes());
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;

        // v12 adds the durable dirty marker. Keeping this inside the same
        // transaction means a failed v12 preflight does not leave a new
        // bookkeeping column behind on an otherwise untouched v11 database.
        ensure_column(
            &tx,
            "schema_migrations",
            "dirty",
            "ALTER TABLE schema_migrations ADD COLUMN dirty INTEGER NOT NULL DEFAULT 0",
        )?;
        let dirty_v11: Option<i64> = tx
            .query_row(
                "SELECT dirty FROM schema_migrations WHERE version = ?1",
                [V11_SCHEMA_VERSION],
                |row| row.get(0),
            )
            .optional()?;
        if dirty_v11 == Some(1) {
            return Err(StorageError::MigrationDirty {
                version: V11_SCHEMA_VERSION,
            });
        }
        let dirty_v12: Option<i64> = tx
            .query_row(
                "SELECT dirty FROM schema_migrations WHERE version = ?1",
                [V12_SCHEMA_VERSION],
                |row| row.get(0),
            )
            .optional()?;
        if dirty_v12 == Some(1) {
            return Err(StorageError::MigrationDirty {
                version: V12_SCHEMA_VERSION,
            });
        }
        let existing_v12: Option<String> = tx
            .query_row(
                "SELECT checksum FROM schema_migrations WHERE version = ?1",
                [V12_SCHEMA_VERSION],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(value) = existing_v12 {
            if value != checksum {
                return Err(StorageError::MigrationChecksumMismatch {
                    version: V12_SCHEMA_VERSION,
                });
            }
            if summaries_has_scope_foreign_key(&tx)? {
                rebuild_summaries_without_scope_foreign_key(&tx)?;
            }
            tx.commit()?;
            return Ok(());
        }

        tx.execute_batch(
            "CREATE TABLE IF NOT EXISTS migration_lock(
                 id INTEGER PRIMARY KEY CHECK (id = 1),
                 owner TEXT NOT NULL,
                 acquired_at INTEGER NOT NULL
             );",
        )?;
        tx.execute(
            "INSERT INTO schema_migrations(version, checksum, applied_at, dirty)
             VALUES(?1, ?2, strftime('%s','now'), 1)",
            params![V12_SCHEMA_VERSION, checksum],
        )?;
        if summaries_has_scope_foreign_key(&tx)? {
            rebuild_summaries_without_scope_foreign_key(&tx)?;
        }
        tx.execute(
            "UPDATE schema_migrations SET dirty = 0 WHERE version = ?1",
            [V12_SCHEMA_VERSION],
        )?;
        tx.execute("DELETE FROM migration_lock WHERE id = 1", [])?;
        tx.commit()?;
        Ok(())
    }

    fn migrate_v13(&mut self) -> Result<(), StorageError> {
        let checksum = hex_digest(MIGRATION_V13_SQL.as_bytes());
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let dirty_v13: Option<i64> = tx
            .query_row(
                "SELECT dirty FROM schema_migrations WHERE version = ?1",
                [V13_SCHEMA_VERSION],
                |row| row.get(0),
            )
            .optional()?;
        if dirty_v13 == Some(1) {
            return Err(StorageError::MigrationDirty {
                version: V13_SCHEMA_VERSION,
            });
        }
        let existing: Option<String> = tx
            .query_row(
                "SELECT checksum FROM schema_migrations WHERE version = ?1",
                [V13_SCHEMA_VERSION],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(value) = existing {
            if value != checksum {
                return Err(StorageError::MigrationChecksumMismatch {
                    version: V13_SCHEMA_VERSION,
                });
            }
            tx.commit()?;
            return Ok(());
        }

        tx.execute(
            "INSERT INTO schema_migrations(version, checksum, applied_at, dirty)
             VALUES(?1, ?2, strftime('%s','now'), 1)",
            params![V13_SCHEMA_VERSION, checksum],
        )?;
        ensure_column(
            &tx,
            "context_manifests",
            "connector_id",
            "ALTER TABLE context_manifests ADD COLUMN connector_id TEXT",
        )?;
        tx.execute(
            "UPDATE schema_migrations SET dirty = 0 WHERE version = ?1",
            [V13_SCHEMA_VERSION],
        )?;
        tx.commit()?;
        Ok(())
    }

    fn migrate_v14(&mut self) -> Result<(), StorageError> {
        let checksum = hex_digest(MIGRATION_V14_SQL.as_bytes());
        let version = V14_SCHEMA_VERSION;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let dirty: Option<i64> = tx
            .query_row(
                "SELECT dirty FROM schema_migrations WHERE version = ?1",
                [version],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(dirty) = dirty {
            if dirty != 0 {
                return Err(StorageError::MigrationDirty { version });
            }
            let existing: String = tx.query_row(
                "SELECT checksum FROM schema_migrations WHERE version = ?1",
                [version],
                |row| row.get(0),
            )?;
            if existing != checksum {
                return Err(StorageError::MigrationChecksumMismatch { version });
            }
            tx.commit()?;
            return Ok(());
        }
        tx.execute(
            "INSERT INTO schema_migrations(version, checksum, applied_at, dirty)
             VALUES(?1, ?2, strftime('%s','now'), 1)",
            params![version, checksum],
        )?;
        tx.execute_batch(MIGRATION_V14_SQL)?;
        tx.execute(
            "UPDATE schema_migrations SET dirty = 0 WHERE version = ?1",
            [version],
        )?;
        tx.commit()?;
        Ok(())
    }

    fn migrate_v15(&mut self) -> Result<(), StorageError> {
        let checksum = hex_digest(MIGRATION_V15_SQL.as_bytes());
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let dirty: Option<i64> = tx
            .query_row(
                "SELECT dirty FROM schema_migrations WHERE version = ?1",
                [V15_SCHEMA_VERSION],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(dirty) = dirty {
            if dirty != 0 {
                return Err(StorageError::MigrationDirty {
                    version: V15_SCHEMA_VERSION,
                });
            }
            let existing: String = tx.query_row(
                "SELECT checksum FROM schema_migrations WHERE version = ?1",
                [V15_SCHEMA_VERSION],
                |row| row.get(0),
            )?;
            if existing != checksum {
                return Err(StorageError::MigrationChecksumMismatch {
                    version: V15_SCHEMA_VERSION,
                });
            }
            tx.commit()?;
            return Ok(());
        }
        tx.execute(
            "INSERT INTO schema_migrations(version, checksum, applied_at, dirty)
             VALUES(?1, ?2, strftime('%s','now'), 1)",
            params![V15_SCHEMA_VERSION, checksum],
        )?;
        tx.execute_batch(MIGRATION_V15_SQL)?;
        tx.execute(
            "UPDATE schema_migrations SET dirty = 0 WHERE version = ?1",
            [V15_SCHEMA_VERSION],
        )?;
        tx.commit()?;
        Ok(())
    }

    fn validate_v15_to_v16_state(tx: &rusqlite::Transaction<'_>) -> Result<(), StorageError> {
        let bad_node: Option<String> = tx
        .query_row(
            "SELECT node_id FROM orchestration_task_nodes
             WHERE status NOT IN ('pending','ready','running','sealing','completed','failed','blocked','cancelled')
             LIMIT 1",
            [],
            |row| row.get(0),
        )
        .optional()?;
        if let Some(node_id) = bad_node {
            return Err(StorageError::MigrationInvalidV15State {
                detail: format!("task node {node_id} has an unmappable v15 status"),
            });
        }
        let bad_attempt: Option<String> = tx
        .query_row(
            "SELECT attempt_id FROM orchestration_task_attempts
             WHERE status NOT IN ('leased','running','sealing','completed','failed','cancelled','interrupted')
             LIMIT 1",
            [],
            |row| row.get(0),
        )
        .optional()?;
        if let Some(attempt_id) = bad_attempt {
            return Err(StorageError::MigrationInvalidV15State {
                detail: format!("task attempt {attempt_id} has an unmappable v15 status"),
            });
        }
        let bad_milestone: Option<String> = tx
            .query_row(
                "SELECT milestone_id FROM orchestration_milestones
             WHERE status NOT IN ('pending','awaiting_approval','approved','rejected','cancelled')
                OR brief_tree_digest IS NULL
                OR presented_artifact_set_digest IS NULL
                OR acceptance_evidence_digest IS NULL
             LIMIT 1",
                [],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(milestone_id) = bad_milestone {
            return Err(StorageError::MigrationInvalidV15State {
                detail: format!(
                    "milestone {milestone_id} has an unmappable v15 status or NULL sealed digest"
                ),
            });
        }
        let handoff_rows: i64 = tx.query_row(
            "SELECT count(*) FROM orchestration_handoff_deliveries",
            [],
            |row| row.get(0),
        )?;
        if handoff_rows > 0 {
            return Err(StorageError::MigrationInvalidV15State {
                detail:
                    "v15 handoff deliveries exist and cannot derive authority columns automatically"
                        .into(),
            });
        }
        let artifact_rows: i64 = tx.query_row(
            "SELECT count(*) FROM orchestration_artifact_bindings",
            [],
            |row| row.get(0),
        )?;
        if artifact_rows > 0 {
            return Err(StorageError::MigrationInvalidV15State {
                detail: "v15 artifact bindings exist and cannot derive sealed binding columns automatically"
                    .into(),
            });
        }
        Ok(())
    }

    fn migrate_v16(&mut self) -> Result<(), StorageError> {
        let checksum = hex_digest(MIGRATION_V16_SQL.as_bytes());
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let dirty: Option<i64> = tx
            .query_row(
                "SELECT dirty FROM schema_migrations WHERE version = ?1",
                [V16_SCHEMA_VERSION],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(dirty) = dirty {
            if dirty != 0 {
                return Err(StorageError::MigrationDirty {
                    version: V16_SCHEMA_VERSION,
                });
            }
            let existing: String = tx.query_row(
                "SELECT checksum FROM schema_migrations WHERE version = ?1",
                [V16_SCHEMA_VERSION],
                |row| row.get(0),
            )?;
            if existing != checksum {
                return Err(StorageError::MigrationChecksumMismatch {
                    version: V16_SCHEMA_VERSION,
                });
            }
            tx.commit()?;
            return Ok(());
        }
        Self::validate_v15_to_v16_state(&tx)?;
        tx.execute(
            "INSERT INTO schema_migrations(version, checksum, applied_at, dirty)
             VALUES(?1, ?2, strftime('%s','now'), 1)",
            params![V16_SCHEMA_VERSION, checksum],
        )?;
        tx.execute_batch(MIGRATION_V16_SQL)?;
        tx.execute(
            "UPDATE schema_migrations SET dirty = 0 WHERE version = ?1",
            [V16_SCHEMA_VERSION],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn migration_checksum(&self) -> String {
        hex_digest(MIGRATION_V16_SQL.as_bytes())
    }

    pub fn event_stream_epoch(&mut self) -> Result<String, StorageError> {
        let candidate = format!(
            "core-stream-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_nanos())
                .unwrap_or_default()
        );
        self.connection.execute(
            "INSERT OR IGNORE INTO stream_metadata(stream_id, epoch) VALUES('core-events', ?1)",
            [&candidate],
        )?;
        Ok(self.connection.query_row(
            "SELECT epoch FROM stream_metadata WHERE stream_id = 'core-events'",
            [],
            |row| row.get(0),
        )?)
    }

    pub fn upsert_command_receipt(&mut self, receipt: &CommandReceipt) -> Result<(), StorageError> {
        let result_json = receipt
            .result_json
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;
        let error_json = receipt
            .error_json
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;
        self.connection.execute(
            "INSERT INTO command_receipts(
                scope_id, client_id, request_id, command, payload_hash,
                operation_key, state, result_json, error_json, created_at, updated_at
             ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
             ON CONFLICT(scope_id, client_id, request_id) DO UPDATE SET
                command = excluded.command,
                payload_hash = excluded.payload_hash,
                operation_key = excluded.operation_key,
                state = excluded.state,
                result_json = excluded.result_json,
                error_json = excluded.error_json,
                updated_at = excluded.updated_at",
            params![
                &receipt.key.scope_id,
                &receipt.key.client_id,
                &receipt.key.request_id,
                &receipt.command,
                &receipt.payload_hash,
                &receipt.operation_key,
                receipt.state.as_str(),
                result_json,
                error_json,
                receipt.created_at,
                receipt.updated_at,
            ],
        )?;
        Ok(())
    }

    pub fn load_command_receipt(
        &self,
        key: &CommandReceiptKey,
    ) -> Result<Option<CommandReceipt>, StorageError> {
        let row: Option<CommandReceiptRow> = self
            .connection
            .query_row(
                "SELECT scope_id, client_id, request_id, command, payload_hash,
                        operation_key, state, result_json, error_json, created_at, updated_at
                 FROM command_receipts
                 WHERE scope_id = ?1 AND client_id = ?2 AND request_id = ?3",
                params![key.scope_id, key.client_id, key.request_id],
                |row| {
                    Ok(CommandReceiptRow {
                        scope_id: row.get(0)?,
                        client_id: row.get(1)?,
                        request_id: row.get(2)?,
                        command: row.get(3)?,
                        payload_hash: row.get(4)?,
                        operation_key: row.get(5)?,
                        state: row.get(6)?,
                        result_json: row.get(7)?,
                        error_json: row.get(8)?,
                        created_at: row.get(9)?,
                        updated_at: row.get(10)?,
                    })
                },
            )
            .optional()?;

        row.map(|row| {
            let CommandReceiptRow {
                scope_id,
                client_id,
                request_id,
                command,
                payload_hash,
                operation_key,
                state,
                result_json,
                error_json,
                created_at,
                updated_at,
            } = row;
            let state = state
                .parse()
                .map_err(|state| StorageError::InvalidCommandReceiptState { state })?;
            Ok(CommandReceipt {
                key: CommandReceiptKey {
                    scope_id,
                    client_id,
                    request_id,
                },
                command,
                payload_hash,
                operation_key,
                state,
                result_json: decode_optional_json(result_json)?,
                error_json: decode_optional_json(error_json)?,
                created_at,
                updated_at,
            })
        })
        .transpose()
    }

    pub fn create_project(
        &mut self,
        id: &str,
        name: &str,
        root_path: Option<&str>,
    ) -> Result<(), StorageError> {
        self.connection.execute(
            "INSERT INTO projects(id, name, root_path, archived) VALUES(?1, ?2, ?3, 0)",
            params![id, name, root_path],
        )?;
        Ok(())
    }

    pub fn update_project(
        &mut self,
        id: &str,
        name: &str,
        root_path: Option<&str>,
        archived: bool,
    ) -> Result<bool, StorageError> {
        let changed = self.connection.execute(
            "UPDATE projects SET name = ?2, root_path = ?3, archived = ?4 WHERE id = ?1",
            params![id, name, root_path, archived as i64],
        )?;
        Ok(changed != 0)
    }

    pub fn create_agent(
        &mut self,
        id: &str,
        name: &str,
        role: &str,
        specialty: &str,
        system_prompt: &str,
    ) -> Result<(), StorageError> {
        self.connection.execute(
            "INSERT INTO agents(id, name, role, specialty, system_prompt) VALUES(?1, ?2, ?3, ?4, ?5)",
            params![id, name, role, specialty, system_prompt],
        )?;
        Ok(())
    }

    pub fn update_agent(
        &mut self,
        id: &str,
        name: &str,
        role: &str,
        specialty: &str,
        system_prompt: &str,
    ) -> Result<bool, StorageError> {
        let changed = self.connection.execute(
            "UPDATE agents SET name = ?2, role = ?3, specialty = ?4, system_prompt = ?5 WHERE id = ?1",
            params![id, name, role, specialty, system_prompt],
        )?;
        Ok(changed != 0)
    }

    /// Creates a connector profile by stable `(scopeId, connectorId)`.
    /// Repeating the exact same metadata is idempotent; changing an existing
    /// profile through create is a conflict.
    pub fn create_connector_profile(
        &mut self,
        profile: &ConnectorProfile,
    ) -> Result<bool, StorageError> {
        validate_connector_profile(profile)?;
        if let Some(existing) = self
            .query_connector_profiles(&profile.scope_id, Some(&profile.connector_id), 1)?
            .into_iter()
            .next()
        {
            if existing == *profile {
                return Ok(false);
            }
            return Err(StorageError::ConnectorProfileConflict {
                id: profile.connector_id.clone(),
            });
        }
        self.connection.execute(
            "INSERT INTO connector_profiles(
                scope_id, connector_id, display_name, provider_type,
                runtime_type, enabled, auth_env_key
             ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                &profile.scope_id,
                &profile.connector_id,
                &profile.display_name,
                &profile.provider_type,
                &profile.runtime_type,
                profile.enabled as i64,
                &profile.auth_env_key,
            ],
        )?;
        Ok(true)
    }

    /// Atomically persists a locally verified Agent import.  This is the only
    /// W6 write path: durable Connector/adapter metadata, Agent identity,
    /// model selection, Project assignment, import receipt, and event are
    /// committed together under an IMMEDIATE transaction.
    pub fn import_local_agent(
        &mut self,
        request: &LocalAgentImportRequest,
    ) -> Result<LocalAgentImportOutcome, StorageError> {
        validate_local_agent_import_request(request)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;

        if let Some(existing) = tx
            .query_row(
                "SELECT payload_hash, import_id, connector_id, agent_id, project_id
                 FROM local_agent_imports
                 WHERE scope_id = ?1 AND client_id = ?2 AND request_id = ?3",
                params![&request.scope_id, &request.client_id, &request.request_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                },
            )
            .optional()?
        {
            if existing.0 != request.payload_hash {
                return Err(StorageError::LocalAgentImportRequestConflict);
            }
            let event_sequence = tx.query_row(
                "SELECT COALESCE(MAX(sequence), 0) FROM event_store
                 WHERE event_type = 'local_agent.imported'
                   AND json_extract(payload_json, '$.importId') = ?1",
                [&existing.1],
                |row| row.get::<_, u64>(0),
            )?;
            tx.commit()?;
            return Ok(LocalAgentImportOutcome {
                import_id: existing.1,
                connector_id: existing.2,
                agent_id: existing.3,
                project_id: existing.4,
                reused: true,
                event_sequence,
            });
        }

        if let Some(existing) = tx
            .query_row(
                "SELECT import_id, connector_id, agent_id, project_id
                 FROM local_agent_imports
                 WHERE candidate_binding_digest = ?1 AND project_id = ?2",
                params![
                    &request.binding.candidate_binding_digest,
                    &request.project_id
                ],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                },
            )
            .optional()?
        {
            // Intentional business reuse: same binding + project with a
            // different requestId returns the ORIGINAL non-zero event
            // sequence and never appends a second event. The reuse is only
            // valid when the normalized model selection matches the existing
            // Project-Agent assignment; a different model selection fails
            // closed instead of silently reusing the old model choice.
            let existing_selection: Option<(String, Option<String>)> = tx
                .query_row(
                    "SELECT model_selection_mode, model_id FROM project_agents
                     WHERE project_id = ?1 AND agent_id = ?2",
                    params![&existing.3, &existing.2],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;
            let selection_matches = existing_selection.is_some_and(|(mode, model_id)| {
                mode == model_selection_mode_to_storage(request.model_selection.mode)
                    && model_id == request.model_selection.model_id
            });
            if !selection_matches {
                // A missing assignment is equally an integrity inconsistency:
                // without the original assignment the reuse precondition
                // cannot be verified, so the request fails closed.
                return Err(StorageError::LocalAgentImportModelSelectionConflict);
            }
            let event_sequence = tx.query_row(
                "SELECT COALESCE(MAX(sequence), 0) FROM event_store
                 WHERE event_type = 'local_agent.imported'
                   AND json_extract(payload_json, '$.importId') = ?1",
                [&existing.0],
                |row| row.get::<_, u64>(0),
            )?;
            tx.commit()?;
            return Ok(LocalAgentImportOutcome {
                import_id: existing.0,
                connector_id: existing.1,
                agent_id: existing.2,
                project_id: existing.3,
                reused: true,
                event_sequence,
            });
        }

        upsert_local_import_connector(&tx, &request.connector)?;
        upsert_local_agent_adapter_binding(&tx, &request.connector, &request.binding)?;
        tx.execute(
            "INSERT INTO agents(id, name, role, specialty, system_prompt, connector_id, model_id, candidate_model_list_revision)
             VALUES(?1, ?2, 'local_agent', 'acp', '', ?3, ?4, 0)",
            params![
                &request.agent_id,
                &request.agent_name,
                &request.connector.connector_id,
                request.model_selection.model_id.as_deref(),
            ],
        )?;
        tx.execute(
            "INSERT INTO project_agents(project_id, agent_id, role, specialty, system_prompt, enabled, workspace_access, model_selection_mode, model_id, candidate_model_list_mode, candidate_model_list_revision)
             VALUES(?1, ?2, NULL, NULL, NULL, 1, 'none', ?3, ?4, 'inherit', 0)",
            params![
                &request.project_id,
                &request.agent_id,
                model_selection_mode_to_storage(request.model_selection.mode),
                request.model_selection.model_id.as_deref(),
            ],
        )?;
        let event = RuntimeEvent {
            event_id: format!("local-agent-import-{}", request.import_id),
            execution_run_id: format!("local-agent-import-{}", request.import_id),
            runtime_id: "local-discovery".into(),
            thread_id: None,
            turn_id: None,
            sequence: 0,
            event_type: "local_agent.imported".into(),
            timestamp_ms: unix_time_ms(),
            payload: serde_json::json!({
                "importId": request.import_id,
                "connectorId": request.connector.connector_id,
                "agentId": request.agent_id,
                "projectId": request.project_id,
                "adapterKind": request.binding.adapter_kind,
                "manifestSha256": request.binding.manifest_sha256,
            }),
        };
        let event_sequence = persist_runtime_events(&tx, &[event])?;
        let result_json = serde_json::to_string(&serde_json::json!({
            "importId": request.import_id,
            "connectorId": request.connector.connector_id,
            "agentId": request.agent_id,
            "projectId": request.project_id,
            "reused": false,
            "eventSequence": event_sequence,
        }))?;
        tx.execute(
            "INSERT INTO local_agent_imports(
                import_id, scope_id, client_id, request_id, payload_hash,
                candidate_binding_digest, project_id, connector_id, agent_id,
                result_json, created_at
             ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, strftime('%s','now'))",
            params![
                &request.import_id,
                &request.scope_id,
                &request.client_id,
                &request.request_id,
                &request.payload_hash,
                &request.binding.candidate_binding_digest,
                &request.project_id,
                &request.connector.connector_id,
                &request.agent_id,
                result_json,
            ],
        )?;
        tx.commit()?;
        Ok(LocalAgentImportOutcome {
            import_id: request.import_id.clone(),
            connector_id: request.connector.connector_id.clone(),
            agent_id: request.agent_id.clone(),
            project_id: request.project_id.clone(),
            reused: false,
            event_sequence,
        })
    }

    /// Updates a complete profile metadata record.  A repeated identical
    /// update is idempotent and returns `false`; partial field updates are
    /// deliberately not supported by this slice.
    pub fn update_connector_profile(
        &mut self,
        profile: &ConnectorProfile,
    ) -> Result<bool, StorageError> {
        validate_connector_profile(profile)?;
        let existing = self
            .query_connector_profiles(&profile.scope_id, Some(&profile.connector_id), 1)?
            .into_iter()
            .next()
            .ok_or_else(|| StorageError::ConnectorProfileNotFound {
                id: profile.connector_id.clone(),
            })?;
        if existing == *profile {
            return Ok(false);
        }
        let changed = self.connection.execute(
            "UPDATE connector_profiles
                SET display_name = ?3,
                    provider_type = ?4,
                    runtime_type = ?5,
                    enabled = ?6,
                    auth_env_key = ?7
              WHERE scope_id = ?1 AND connector_id = ?2",
            params![
                &profile.scope_id,
                &profile.connector_id,
                &profile.display_name,
                &profile.provider_type,
                &profile.runtime_type,
                profile.enabled as i64,
                &profile.auth_env_key,
            ],
        )?;
        Ok(changed != 0)
    }

    /// Removes a profile by its explicit global scope.  Removing an absent
    /// profile is idempotent and returns `false`.
    pub fn remove_connector_profile(
        &mut self,
        scope_id: &str,
        connector_id: &str,
    ) -> Result<bool, StorageError> {
        validate_connector_scope(scope_id)?;
        validate_connector_identifier("connectorId", connector_id)?;
        let changed = self.connection.execute(
            "DELETE FROM connector_profiles WHERE scope_id = ?1 AND connector_id = ?2",
            params![scope_id, connector_id],
        )?;
        Ok(changed != 0)
    }

    pub fn query_connector_profiles(
        &self,
        scope_id: &str,
        connector_id: Option<&str>,
        limit: u64,
    ) -> Result<Vec<ConnectorProfile>, StorageError> {
        validate_connector_scope(scope_id)?;
        if limit == 0 || limit > CONNECTOR_PROFILE_QUERY_LIMIT_MAX {
            return Err(StorageError::ConnectorProfileInvalid {
                field: "limit".into(),
                reason: format!("must be between 1 and {CONNECTOR_PROFILE_QUERY_LIMIT_MAX}"),
            });
        }
        if let Some(connector_id) = connector_id {
            validate_connector_identifier("connectorId", connector_id)?;
            let mut statement = self.connection.prepare(
                "SELECT scope_id, connector_id, display_name, provider_type,
                        runtime_type, enabled, auth_env_key
                   FROM connector_profiles
                  WHERE scope_id = ?1 AND connector_id = ?2
                  ORDER BY connector_id
                  LIMIT ?3",
            )?;
            let rows = statement.query_map(
                params![scope_id, connector_id, limit],
                map_connector_profile,
            )?;
            return Ok(rows.collect::<Result<Vec<_>, _>>()?);
        }
        let mut statement = self.connection.prepare(
            "SELECT scope_id, connector_id, display_name, provider_type,
                    runtime_type, enabled, auth_env_key
               FROM connector_profiles
              WHERE scope_id = ?1
              ORDER BY connector_id
              LIMIT ?2",
        )?;
        let rows = statement.query_map(params![scope_id, limit], map_connector_profile)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn create_conversation(
        &mut self,
        id: &str,
        project_id: &str,
        title: &str,
    ) -> Result<(), StorageError> {
        self.connection.execute(
            "INSERT INTO conversations(id, project_id, title, scope_revision) VALUES(?1, ?2, ?3, 0)",
            params![id, project_id, title],
        )?;
        Ok(())
    }

    pub fn update_conversation(&mut self, id: &str, title: &str) -> Result<bool, StorageError> {
        let changed = self.connection.execute(
            "UPDATE conversations SET title = ?2, scope_revision = scope_revision + 1 WHERE id = ?1",
            params![id, title],
        )?;
        Ok(changed != 0)
    }

    pub fn create_message(&mut self, message: &Message) -> Result<(), StorageError> {
        self.connection.execute(
            "INSERT INTO messages(id, conversation_id, sender_id, sequence, content) VALUES(?1, ?2, ?3, ?4, ?5)",
            params![
                message.id,
                message.conversation_id,
                message.sender_id,
                message.sequence,
                message.content,
            ],
        )?;
        Ok(())
    }

    pub fn message_exists(&self, message_id: &str) -> Result<bool, StorageError> {
        Ok(self.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM messages WHERE id = ?1)",
            [message_id],
            |row| row.get(0),
        )?)
    }

    pub fn load_recent_message_contents(
        &self,
        conversation_id: &str,
        limit: u64,
    ) -> Result<Vec<String>, StorageError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let limit = limit.min(i64::MAX as u64) as i64;
        let mut statement = self.connection.prepare(
            "SELECT content
             FROM (
                 SELECT sequence, content
                 FROM messages
                 WHERE conversation_id = ?1
                 ORDER BY sequence DESC
                 LIMIT ?2
             )
             ORDER BY sequence ASC",
        )?;
        let rows = statement.query_map(params![conversation_id, limit], |row| row.get(0))?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn load_attachment_context_records(
        &self,
        conversation_id: &str,
        limit: u64,
    ) -> Result<Vec<AttachmentContextRecord>, StorageError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let limit = limit.min(64) as i64;
        let mut statement = self.connection.prepare(
            "SELECT a.attachment_id, a.artifact_id, a.message_id, m.sequence,
                    a.ordinal, a.file_name, a.sha256, a.size, r.mime
               FROM attachments a
               JOIN messages m ON m.id = a.message_id
               LEFT JOIN artifacts r ON r.id = a.artifact_id
              WHERE m.conversation_id = ?1
              ORDER BY m.sequence DESC, a.ordinal DESC
              LIMIT ?2",
        )?;
        let rows = statement.query_map(params![conversation_id, limit], |row| {
            Ok(AttachmentContextRecord {
                attachment_id: row.get(0)?,
                artifact_id: row.get(1)?,
                message_id: row.get(2)?,
                message_sequence: row.get(3)?,
                ordinal: row.get(4)?,
                file_name: row.get(5)?,
                sha256: row.get(6)?,
                size: row.get(7)?,
                mime: row.get(8)?,
            })
        })?;
        let mut records = rows.collect::<Result<Vec<_>, _>>()?;
        records.reverse();
        Ok(records)
    }

    pub fn store_context_manifest(
        &mut self,
        manifest: &agenttalk_domain::ContextManifest,
        bundle_hash: &str,
    ) -> Result<bool, StorageError> {
        self.store_context_manifest_with_ledger(manifest, bundle_hash, "[]")
    }

    pub fn store_context_manifest_with_ledger(
        &mut self,
        manifest: &agenttalk_domain::ContextManifest,
        bundle_hash: &str,
        source_ledger_json: &str,
    ) -> Result<bool, StorageError> {
        let tx = self.connection.transaction()?;
        let stored =
            store_context_manifest_with_ledger_row(&tx, manifest, bundle_hash, source_ledger_json)?;
        tx.commit()?;
        Ok(stored)
    }

    pub fn search_messages(
        &self,
        query: &str,
        conversation_id: Option<&str>,
        limit: u64,
    ) -> Result<Vec<serde_json::Value>, StorageError> {
        let query = query.trim();
        if query.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let phrase = format!("\"{}\"", query.replace('"', " "));
        let limit = limit.min(100) as i64;
        let mut statement = self.connection.prepare(
            "SELECT m.id, m.conversation_id, m.sender_id, m.sequence, m.content
             FROM messages_fts f
             JOIN messages m ON m.rowid = f.rowid
             WHERE messages_fts MATCH ?1
               AND (?2 IS NULL OR m.conversation_id = ?2)
             ORDER BY bm25(messages_fts), m.conversation_id, m.sequence
             LIMIT ?3",
        )?;
        let rows = statement.query_map(params![phrase, conversation_id, limit], |row| {
            Ok(serde_json::json!({
                "id": row.get::<_, String>(0)?,
                "conversationId": row.get::<_, String>(1)?,
                "senderId": row.get::<_, String>(2)?,
                "sequence": row.get::<_, u64>(3)?,
                "content": row.get::<_, String>(4)?,
            }))
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// Performs the bounded exact retrieval preview. Candidate bodies are
    /// read from existing projections and scored in memory; this method never
    /// persists a source body, prompt, query, or snippet.
    pub fn preview_retrieval(
        &self,
        request: &RetrievalPreviewRequest,
    ) -> Result<serde_json::Value, StorageError> {
        self.preview_retrieval_with_provider(request, None)
    }

    /// Performs the same bounded, scope-checked preview using a deterministic
    /// local vector fixture. This is intentionally not a live embedding or
    /// Provider path; it exists to verify vector ranking, permissions and
    /// source-hash boundaries without network or credential access.
    pub fn preview_retrieval_vector(
        &self,
        request: &RetrievalPreviewRequest,
    ) -> Result<serde_json::Value, StorageError> {
        self.preview_retrieval_vector_with_provider(request, &LocalFixtureEmbeddingProvider)
    }

    /// Runs a scope-checked vector preview against an injected provider. Core
    /// owns provider selection; Storage only enforces bounded input, descriptor
    /// validity, response dimensions and finite cosine scores.
    pub fn preview_retrieval_vector_with_provider(
        &self,
        request: &RetrievalPreviewRequest,
        provider: &dyn RetrievalEmbeddingProvider,
    ) -> Result<serde_json::Value, StorageError> {
        self.preview_retrieval_with_provider(request, Some(provider))
    }

    fn preview_retrieval_with_provider(
        &self,
        request: &RetrievalPreviewRequest,
        embedding_provider: Option<&dyn RetrievalEmbeddingProvider>,
    ) -> Result<serde_json::Value, StorageError> {
        validate_retrieval_preview_request(request)?;
        let embedding_descriptor = embedding_provider
            .map(validate_embedding_descriptor)
            .transpose()?;
        let vector_mode = embedding_descriptor.is_some();
        let mode = if vector_mode {
            "vector_fixture"
        } else {
            "exact"
        };

        let scope = request.scope.as_str();
        let metadata: Option<(String, u64, u64, i64, i64, i64)> = self
            .connection
            .query_row(
                "SELECT c.project_id,
                        c.scope_revision,
                        COALESCE((SELECT revision FROM workspace_authorizations
                                  WHERE project_id = c.project_id
                                    AND validation_status = 'valid'), 0),
                        EXISTS(SELECT 1 FROM agents WHERE id = ?2),
                        EXISTS(SELECT 1 FROM project_agents
                               WHERE project_id = c.project_id AND agent_id = ?2 AND enabled != 0),
                        (NOT EXISTS(SELECT 1 FROM conversation_agents
                                    WHERE conversation_id = c.id)
                         OR EXISTS(SELECT 1 FROM conversation_agents
                                   WHERE conversation_id = c.id AND agent_id = ?2 AND enabled != 0))
                 FROM conversations AS c
                 WHERE c.id = ?1",
                params![request.conversation_id, request.agent_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )
            .optional()?;
        let Some((
            project_id,
            conversation_revision,
            workspace_revision,
            agent_exists,
            project_agent_exists,
            conversation_agent_authorized,
        )) = metadata
        else {
            return Err(retrieval_preview_invalid("conversation does not exist"));
        };
        if project_id != request.expected_project_id {
            return Err(retrieval_preview_invalid(
                "conversation is outside expected project",
            ));
        }
        if agent_exists == 0 || project_agent_exists == 0 || conversation_agent_authorized == 0 {
            return Err(retrieval_preview_invalid(
                "agent is not authorized for project scope",
            ));
        }

        let mut candidates = Vec::new();
        let workspace = self
            .connection
            .query_row(
                "SELECT wa.canonical_root, pa.workspace_access
                 FROM workspace_authorizations AS wa
                 JOIN project_agents AS pa
                   ON pa.project_id = wa.project_id AND pa.agent_id = ?2
                 WHERE wa.project_id = ?1
                   AND wa.validation_status = 'valid'
                   AND pa.enabled != 0",
                params![request.expected_project_id, request.agent_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;
        let wants_project_files = request
            .source_types
            .iter()
            .any(|source_type| source_type == "project_file");
        let bounded_file_scan = workspace
            .as_ref()
            .is_some_and(|(_, access)| access == "read_only" || access == "workspace_write");
        let mut bounded_file_scan_unavailable_reason = (!bounded_file_scan)
            .then_some("workspace_authorization_or_read_permission_unavailable");
        if wants_project_files {
            if let Some((root, access)) = workspace.as_ref() {
                if matches!(access.as_str(), "read_only" | "workspace_write") {
                    candidates.extend(self.scan_project_files(
                        root,
                        &request.query,
                        &request.expected_project_id,
                        &request.conversation_id,
                        access,
                        vector_mode,
                    ));
                } else if request.source_types.len() == 1 {
                    return Err(retrieval_preview_invalid(
                        "project file retrieval requires valid workspace authorization and read permission",
                    ));
                }
            } else if request.source_types.len() == 1 {
                return Err(retrieval_preview_invalid(
                    "project file retrieval requires valid workspace authorization and read permission",
                ));
            }
        }

        let first_term = request
            .query
            .split_whitespace()
            .next()
            .ok_or_else(|| retrieval_preview_invalid("query must not be blank"))?;
        let first_term = escape_like_term(&first_term.to_lowercase());
        if request
            .source_types
            .iter()
            .any(|source_type| source_type == "message")
        {
            let mut statement = self.connection.prepare(
                "SELECT m.id, m.conversation_id, m.sender_id, m.sequence, m.content
                 FROM messages AS m
                 JOIN conversations AS c ON c.id = m.conversation_id
                 WHERE c.project_id = ?1
                   AND (?2 = 'project' OR m.conversation_id = ?3)
                   AND (?5 = 'vector_fixture' OR lower(m.content) LIKE '%' || ?4 || '%' ESCAPE '\\')
                 ORDER BY m.conversation_id, m.sequence, m.id
                 LIMIT 1000",
            )?;
            let rows = statement.query_map(
                params![
                    request.expected_project_id,
                    scope,
                    request.conversation_id,
                    first_term,
                    mode,
                ],
                |row| {
                    Ok(RetrievalPreviewCandidate {
                        source_type: "message",
                        source_object_id: row.get(0)?,
                        project_id: request.expected_project_id.clone(),
                        conversation_id: Some(row.get(1)?),
                        agent_id: Some(row.get(2)?),
                        body: row.get(4)?,
                        permission_decision: "not_applicable".into(),
                    })
                },
            )?;
            candidates.extend(rows.collect::<Result<Vec<_>, _>>()?);
        }

        if request
            .source_types
            .iter()
            .any(|source_type| source_type == "execution")
        {
            let mut statement = self.connection.prepare(
                "SELECT e.event_id, e.event_type, e.payload_json,
                        r.conversation_id, r.agent_id
                 FROM event_store AS e
                 JOIN execution_runs AS r ON r.id = e.execution_run_id
                 WHERE r.project_id = ?1
                   AND (?2 = 'project' OR r.conversation_id = ?3)
                   AND e.event_type IN ('output.delta', 'execution.completed')
                   AND (?5 = 'vector_fixture' OR lower(e.payload_json) LIKE '%' || ?4 || '%' ESCAPE '\\')
                 ORDER BY e.sequence, e.event_id
                 LIMIT 1000",
            )?;
            let rows = statement.query_map(
                params![
                    request.expected_project_id,
                    scope,
                    request.conversation_id,
                    first_term,
                    mode,
                ],
                |row| {
                    let event_type: String = row.get(1)?;
                    let payload_json: String = row.get(2)?;
                    let payload: serde_json::Value =
                        serde_json::from_str(&payload_json).map_err(|error| {
                            rusqlite::Error::FromSqlConversionFailure(
                                2,
                                rusqlite::types::Type::Text,
                                Box::new(error),
                            )
                        })?;
                    Ok((
                        row.get::<_, String>(0)?,
                        event_type,
                        payload,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )?;
            for row in rows {
                let (event_id, event_type, payload, conversation_id, agent_id) = row?;
                if let Some(body) = safe_execution_event_body(&event_type, &payload) {
                    candidates.push(RetrievalPreviewCandidate {
                        source_type: "execution",
                        source_object_id: event_id,
                        project_id: request.expected_project_id.clone(),
                        conversation_id: Some(conversation_id),
                        agent_id: Some(agent_id),
                        body,
                        permission_decision: "not_applicable".into(),
                    });
                }
            }
        }

        let query_embedding = match (embedding_provider, embedding_descriptor.as_ref()) {
            (Some(provider), Some(descriptor)) => {
                Some(embed_retrieval_text(provider, descriptor, &request.query)?)
            }
            _ => None,
        };
        let mut hits = Vec::new();
        for candidate in candidates {
            let hit = match (
                embedding_provider,
                embedding_descriptor.as_ref(),
                query_embedding.as_deref(),
            ) {
                (Some(provider), Some(descriptor), Some(query_embedding)) => {
                    build_vector_retrieval_hit(
                        &candidate,
                        request,
                        provider,
                        descriptor,
                        query_embedding,
                    )?
                }
                _ => build_retrieval_hit(&candidate, request),
            };
            if let Some(hit) = hit {
                hits.push(hit);
            }
        }
        hits.sort_by(|left, right| {
            right["score"]
                .as_f64()
                .unwrap_or_default()
                .partial_cmp(&left["score"].as_f64().unwrap_or_default())
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| {
                    left["sourceType"]
                        .as_str()
                        .cmp(&right["sourceType"].as_str())
                })
                .then_with(|| {
                    left["sourceObjectId"]
                        .as_str()
                        .cmp(&right["sourceObjectId"].as_str())
                })
        });
        hits.truncate(request.limit as usize);
        for (index, hit) in hits.iter_mut().enumerate() {
            hit["rank"] = serde_json::json!(index + 1);
        }

        let mut capabilities = serde_json::json!({
            "exactSearch": true,
            "pathSearch": true,
            "boundedFileScan": bounded_file_scan,
            "rgAvailable": false,
            "ftsAvailable": false,
            "semantic": vector_mode,
            "semanticUnavailableReason": if vector_mode {
                "local_deterministic_fixture_not_live_provider"
            } else {
                "not_evaluated"
            }
        });
        if let Some(descriptor) = embedding_descriptor.as_ref() {
            capabilities["embeddingProvider"] = serde_json::json!(descriptor.provider_id);
            capabilities["embeddingDimension"] = serde_json::json!(descriptor.dimension);
            capabilities["embeddingVerification"] =
                serde_json::json!(descriptor.verification.as_str());
            if descriptor.verification == RetrievalEmbeddingVerification::VerifiedProvider {
                capabilities["semanticUnavailableReason"] = serde_json::Value::Null;
            }
        }
        if let Some(reason) = bounded_file_scan_unavailable_reason.take() {
            capabilities["boundedFileScanUnavailableReason"] = serde_json::json!(reason);
        }
        Ok(serde_json::json!({
            "retrievalVersion": embedding_descriptor
                .as_ref()
                .map(|descriptor| descriptor.retrieval_version.as_str())
                .unwrap_or(EXACT_RETRIEVAL_VERSION),
            "projectId": request.expected_project_id,
            "conversationId": request.conversation_id,
            "scopeRevision": if scope == "conversation" { conversation_revision } else { 0 },
            "workspaceRevision": workspace_revision,
            "queryHash": hex_digest(request.query.trim().as_bytes()),
            "capabilities": capabilities,
            "hits": hits
        }))
    }

    fn scan_project_files(
        &self,
        canonical_root: &str,
        query: &str,
        project_id: &str,
        conversation_id: &str,
        permission_decision: &str,
        vector_mode: bool,
    ) -> Vec<RetrievalPreviewCandidate> {
        let Ok(root) = fs::canonicalize(canonical_root) else {
            return Vec::new();
        };
        let Ok(root_metadata) = fs::metadata(&root) else {
            return Vec::new();
        };
        if !root_metadata.is_dir() {
            return Vec::new();
        }
        let terms = query
            .split_whitespace()
            .map(|term| term.to_lowercase())
            .filter(|term| !term.is_empty())
            .collect::<Vec<_>>();
        if terms.is_empty() {
            return Vec::new();
        }
        let mut directories = VecDeque::from([(root.clone(), 0_usize)]);
        let started_at = Instant::now();
        let mut visited_directories = 0_usize;
        let mut visited_files = 0_usize;
        let mut candidates = Vec::new();
        while let Some((directory, depth)) = directories.pop_front() {
            if visited_directories >= RETRIEVAL_FILE_MAX_DIRECTORIES
                || visited_files >= RETRIEVAL_FILE_MAX_FILES
                || candidates.len() >= RETRIEVAL_FILE_MAX_ENTRIES
                || started_at.elapsed() >= RETRIEVAL_FILE_SCAN_TIMEOUT
            {
                break;
            }
            visited_directories += 1;
            let Ok(entries) = fs::read_dir(&directory) else {
                continue;
            };
            for entry in entries.flatten() {
                if candidates.len() >= RETRIEVAL_FILE_MAX_ENTRIES
                    || visited_files >= RETRIEVAL_FILE_MAX_FILES
                    || started_at.elapsed() >= RETRIEVAL_FILE_SCAN_TIMEOUT
                {
                    break;
                }
                let path = entry.path();
                let name = entry.file_name().to_string_lossy().into_owned();
                let is_directory = entry
                    .file_type()
                    .map(|value| value.is_dir())
                    .unwrap_or(false);
                if is_directory {
                    if depth >= RETRIEVAL_FILE_MAX_DEPTH || ignored_project_file_directory(&name) {
                        continue;
                    }
                    let Ok(canonical) = fs::canonicalize(&path) else {
                        continue;
                    };
                    if canonical.starts_with(&root) {
                        directories.push_back((canonical, depth + 1));
                    }
                    continue;
                }
                visited_files += 1;
                if ignored_project_file(&name) {
                    continue;
                }
                let Ok(canonical) = fs::canonicalize(&path) else {
                    continue;
                };
                if !canonical.starts_with(&root) {
                    continue;
                }
                let Ok(metadata) = fs::metadata(&canonical) else {
                    continue;
                };
                if !metadata.is_file() || metadata.len() > RETRIEVAL_FILE_MAX_SIZE {
                    continue;
                }
                let Some(body) = read_project_file_prefix(&canonical, metadata.len()) else {
                    continue;
                };
                let lower_body = body.to_lowercase();
                if !vector_mode && !terms.iter().any(|term| lower_body.contains(term)) {
                    continue;
                }
                let Ok(relative_path) = canonical.strip_prefix(&root) else {
                    continue;
                };
                candidates.push(RetrievalPreviewCandidate {
                    source_type: "project_file",
                    source_object_id: relative_path.to_string_lossy().replace('\\', "/"),
                    project_id: project_id.to_owned(),
                    conversation_id: Some(conversation_id.to_owned()),
                    agent_id: None,
                    body,
                    permission_decision: permission_decision.to_owned(),
                });
            }
        }
        candidates
    }

    /// Returns whether a scope accepted by the legacy memory contract exists.
    /// Memory scopes may be projects, conversations, or agents.
    pub fn memory_scope_exists(&self, scope_id: &str) -> Result<bool, StorageError> {
        Ok(self.connection.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM projects WHERE id = ?1
                UNION ALL
                SELECT 1 FROM conversations WHERE id = ?1
                UNION ALL
                SELECT 1 FROM agents WHERE id = ?1
            )",
            [scope_id],
            |row| row.get(0),
        )?)
    }

    /// Loads only confirmed memory metadata for context assembly. Memory
    /// bodies are intentionally absent from this storage contract.
    pub fn load_recent_memory_metadata(
        &self,
        scope_id: &str,
        agent_id: &str,
        limit: u64,
    ) -> Result<Vec<MemoryItem>, StorageError> {
        let mut statement = self.connection.prepare(
            "SELECT id, scope_id, agent_id, content_hash, confirmed
             FROM memories
             WHERE scope_id = ?1 AND confirmed != 0
               AND (agent_id IS NULL OR agent_id = ?2)
             ORDER BY id DESC LIMIT ?3",
        )?;
        let rows = statement.query_map(params![scope_id, agent_id, limit.min(64)], |row| {
            Ok(MemoryItem {
                id: row.get(0)?,
                scope_id: row.get(1)?,
                agent_id: row.get(2)?,
                content_hash: row.get(3)?,
                confirmed: row.get::<_, i64>(4)? != 0,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// Loads the newest summary metadata for a project or conversation. The
    /// caller may separately resolve an artifact body when that is explicitly
    /// available; the metadata query itself never materializes a body.
    pub fn load_recent_summary_metadata(
        &self,
        scope_id: &str,
        limit: u64,
    ) -> Result<Vec<Summary>, StorageError> {
        let mut statement = self.connection.prepare(
            "SELECT id, scope_id, version, content_hash, artifact_id
             FROM summaries WHERE scope_id = ?1
             ORDER BY version DESC, id DESC LIMIT ?2",
        )?;
        let rows = statement.query_map(params![scope_id, limit.min(16)], |row| {
            Ok(Summary {
                id: row.get(0)?,
                scope_id: row.get(1)?,
                version: row.get(2)?,
                content_hash: row.get(3)?,
                artifact_id: row.get(4)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn agent_exists(&self, agent_id: &str) -> Result<bool, StorageError> {
        Ok(self.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM agents WHERE id = ?1)",
            [agent_id],
            |row| row.get(0),
        )?)
    }

    pub fn create_workflow(
        &mut self,
        project_id: &str,
        workflow: &WorkflowTemplate,
    ) -> Result<bool, StorageError> {
        let steps_json = serde_json::to_string(&workflow.steps)?;
        let existing = self
            .connection
            .query_row(
                "SELECT project_id, name, kind, steps_json FROM workflows WHERE id = ?1",
                [&workflow.id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                },
            )
            .optional()?;
        if let Some(existing) = existing {
            if existing
                == (
                    project_id.to_owned(),
                    workflow.name.clone(),
                    workflow.kind.clone(),
                    steps_json,
                )
            {
                return Ok(false);
            }
            return Err(StorageError::WorkflowConflict {
                id: workflow.id.clone(),
            });
        }

        if !self.project_exists(project_id)? {
            return Err(StorageError::ProjectNotFound {
                id: project_id.to_owned(),
            });
        }
        for step in &workflow.steps {
            if !self.project_agent_is_rostered(project_id, &step.agent_id)? {
                return Err(StorageError::WorkflowAgentNotInProject {
                    workflow_id: workflow.id.clone(),
                    project_id: project_id.to_owned(),
                    agent_id: step.agent_id.clone(),
                });
            }
        }

        self.connection.execute(
            "INSERT INTO workflows(id, project_id, name, kind, steps_json) VALUES(?1, ?2, ?3, ?4, ?5)",
            params![workflow.id, project_id, workflow.name, workflow.kind, steps_json],
        )?;
        Ok(true)
    }

    pub fn load_workflow(
        &self,
        workflow_id: &str,
    ) -> Result<Option<(String, WorkflowTemplate)>, StorageError> {
        let row = self
            .connection
            .query_row(
                "SELECT project_id, name, kind, steps_json FROM workflows WHERE id = ?1",
                [workflow_id],
                |row| {
                    let project_id = row.get::<_, String>(0)?;
                    let workflow = WorkflowTemplate {
                        id: workflow_id.to_owned(),
                        name: row.get(1)?,
                        kind: row.get(2)?,
                        steps: serde_json::from_str(&row.get::<_, String>(3)?).map_err(
                            |error| {
                                rusqlite::Error::FromSqlConversionFailure(
                                    3,
                                    rusqlite::types::Type::Text,
                                    Box::new(error),
                                )
                            },
                        )?,
                    };
                    Ok((project_id, workflow))
                },
            )
            .optional()?;
        Ok(row)
    }

    pub fn create_collaboration_run(
        &mut self,
        project_id: &str,
        run: &CollaborationRun,
    ) -> Result<bool, StorageError> {
        let root_agent_ids_json = serde_json::to_string(&run.root_agent_ids)?;
        let status = serde_json::to_string(&run.status)?;
        let existing = self
            .connection
            .query_row(
                "SELECT project_id, root_agent_ids_json, call_count, max_calls,
                        depth, max_depth, status, stop_reason, auto_dispatch_handoffs
                 FROM collaboration_runs WHERE id = ?1",
                [&run.id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, u32>(2)?,
                        row.get::<_, u32>(3)?,
                        row.get::<_, u32>(4)?,
                        row.get::<_, u32>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, Option<String>>(7)?,
                        row.get::<_, i64>(8)? != 0,
                    ))
                },
            )
            .optional()?;
        if let Some(existing) = existing {
            if existing
                == (
                    project_id.to_owned(),
                    root_agent_ids_json,
                    run.call_count,
                    run.max_calls,
                    run.depth,
                    run.max_depth,
                    status,
                    run.stop_reason.clone(),
                    run.auto_dispatch_handoffs,
                )
            {
                return Ok(false);
            }
            return Err(StorageError::CollaborationConflict { id: run.id.clone() });
        }

        if !self.project_exists(project_id)? {
            return Err(StorageError::CollaborationProjectNotFound {
                id: project_id.to_owned(),
            });
        }
        for agent_id in &run.root_agent_ids {
            if !self.project_agent_is_rostered(project_id, agent_id)? {
                return Err(StorageError::CollaborationAgentNotInProject {
                    project_id: project_id.to_owned(),
                    agent_id: agent_id.clone(),
                });
            }
        }

        self.connection.execute(
            "INSERT INTO collaboration_runs(
                id, project_id, root_agent_ids_json, call_count, max_calls,
                depth, max_depth, status, stop_reason, auto_dispatch_handoffs
             ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                &run.id,
                project_id,
                root_agent_ids_json,
                run.call_count,
                run.max_calls,
                run.depth,
                run.max_depth,
                status,
                &run.stop_reason,
                run.auto_dispatch_handoffs as i64,
            ],
        )?;
        Ok(true)
    }

    pub fn create_handoff(&mut self, handoff: &Handoff) -> Result<bool, StorageError> {
        let existing = self
            .connection
            .query_row(
                "SELECT collaboration_run_id, from_execution_run_id, to_agent_id, status, details_json
                 FROM handoffs WHERE id = ?1",
                [&handoff.id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, Option<String>>(4)?,
                    ))
                },
            )
            .optional()?;
        if let Some(existing) = existing {
            if existing
                == (
                    handoff.collaboration_run_id.clone(),
                    handoff.from_execution_run_id.clone(),
                    handoff.to_agent_id.clone(),
                    handoff.status.clone(),
                    handoff
                        .details
                        .as_ref()
                        .map(serde_json::to_string)
                        .transpose()?,
                )
            {
                return Ok(false);
            }
            return Err(StorageError::HandoffConflict {
                id: handoff.id.clone(),
            });
        }

        if !is_known_handoff_status(&handoff.status) {
            return Err(StorageError::HandoffContractRejected {
                id: handoff.id.clone(),
                reason: format!("unknown status {}", handoff.status),
            });
        }

        let collaboration_project_id: Option<String> = self
            .connection
            .query_row(
                "SELECT project_id FROM collaboration_runs WHERE id = ?1",
                [&handoff.collaboration_run_id],
                |row| row.get(0),
            )
            .optional()?;
        if collaboration_project_id.is_none() {
            return Err(StorageError::HandoffCollaborationNotFound {
                id: handoff.collaboration_run_id.clone(),
            });
        }

        let source_execution: Option<(String, String, String)> = self
            .connection
            .query_row(
                "SELECT collaboration_run_id, project_id, agent_id
                 FROM execution_runs WHERE id = ?1",
                [&handoff.from_execution_run_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let (source_collaboration_id, project_id, source_agent_id) =
            source_execution.ok_or_else(|| StorageError::HandoffExecutionNotFound {
                id: handoff.from_execution_run_id.clone(),
            })?;
        if source_collaboration_id != handoff.collaboration_run_id {
            return Err(StorageError::HandoffContractRejected {
                id: handoff.id.clone(),
                reason: "source execution belongs to a different collaboration".into(),
            });
        }
        if collaboration_project_id.as_deref() != Some(project_id.as_str()) {
            return Err(StorageError::HandoffContractRejected {
                id: handoff.id.clone(),
                reason: "source execution belongs to a different Project".into(),
            });
        }
        if !self.project_agent_is_rostered(&project_id, &handoff.to_agent_id)? {
            return Err(StorageError::HandoffAgentNotInProject {
                project_id,
                agent_id: handoff.to_agent_id.clone(),
            });
        }
        if let Some(details) = handoff.details.as_ref() {
            if details
                .parent_execution_run_id
                .as_deref()
                .is_some_and(|value| value != handoff.from_execution_run_id)
                || details
                    .from_agent_id
                    .as_deref()
                    .is_some_and(|value| value != source_agent_id)
                || details
                    .to_agent_id
                    .as_deref()
                    .is_some_and(|value| value != handoff.to_agent_id)
            {
                return Err(StorageError::HandoffContractRejected {
                    id: handoff.id.clone(),
                    reason: "structured details do not match the authoritative handoff edge".into(),
                });
            }
        }

        self.connection.execute(
            "INSERT INTO handoffs(
                id, collaboration_run_id, from_execution_run_id, to_agent_id, status, details_json
             ) VALUES(?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                &handoff.id,
                &handoff.collaboration_run_id,
                &handoff.from_execution_run_id,
                &handoff.to_agent_id,
                &handoff.status,
                handoff
                    .details
                    .as_ref()
                    .map(serde_json::to_string)
                    .transpose()?,
            ],
        )?;
        Ok(true)
    }

    pub fn load_handoff(&self, handoff_id: &str) -> Result<Option<Handoff>, StorageError> {
        let row = self
            .connection
            .query_row(
                "SELECT id, collaboration_run_id, from_execution_run_id, to_agent_id,
                        status, details_json
                 FROM handoffs WHERE id = ?1",
                [handoff_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, Option<String>>(5)?,
                    ))
                },
            )
            .optional()?;
        row.map(
            |(
                id,
                collaboration_run_id,
                from_execution_run_id,
                to_agent_id,
                status,
                details_json,
            )| {
                Ok(Handoff {
                    id,
                    collaboration_run_id,
                    from_execution_run_id,
                    to_agent_id,
                    status,
                    details: decode_handoff_details(details_json.as_deref())?,
                })
            },
        )
        .transpose()
    }

    pub fn load_handoffs(&self) -> Result<Vec<Handoff>, StorageError> {
        let mut statement = self.connection.prepare(
            "SELECT id, collaboration_run_id, from_execution_run_id, to_agent_id,
                    status, details_json
             FROM handoffs ORDER BY id",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, Option<String>>(5)?,
            ))
        })?;
        rows.map(|row| {
            let (id, collaboration_run_id, from_execution_run_id, to_agent_id, status, details) =
                row?;
            Ok(Handoff {
                id,
                collaboration_run_id,
                from_execution_run_id,
                to_agent_id,
                status,
                details: decode_handoff_details(details.as_deref())?,
            })
        })
        .collect()
    }

    pub fn dispatch_handoff_and_persist_child(
        &mut self,
        handoff_id: &str,
        child: &ExecutionRun,
        child_snapshot: &ModelSnapshot,
        event: &RuntimeEvent,
    ) -> Result<(bool, u64), StorageError> {
        self.dispatch_handoff_and_persist_child_internal(
            handoff_id,
            child,
            child_snapshot,
            None,
            None,
            std::slice::from_ref(event),
        )
    }

    pub fn dispatch_handoff_and_persist_child_with_selection(
        &mut self,
        handoff_id: &str,
        child: &ExecutionRun,
        child_snapshot: &ModelSnapshot,
        child_selection_snapshot: &ModelSelectionSnapshot,
        event: &RuntimeEvent,
    ) -> Result<(bool, u64), StorageError> {
        self.dispatch_handoff_and_persist_child_internal(
            handoff_id,
            child,
            child_snapshot,
            Some(child_selection_snapshot),
            None,
            std::slice::from_ref(event),
        )
    }

    /// Atomically records a handoff's dispatched child together with its
    /// frozen connector/model snapshots, Context Manifest, and the full
    /// initial event sequence. Runtime dispatch happens only after this
    /// boundary commits, so a crash cannot expose a Connector-bound child
    /// without its frozen Context Manifest.
    #[allow(clippy::too_many_arguments)]
    pub fn dispatch_handoff_and_persist_child_with_selection_context_and_events(
        &mut self,
        handoff_id: &str,
        child: &ExecutionRun,
        child_snapshot: &ModelSnapshot,
        child_selection_snapshot: &ModelSelectionSnapshot,
        manifest: &agenttalk_domain::ContextManifest,
        bundle_hash: &str,
        source_ledger_json: &str,
        events: &[RuntimeEvent],
    ) -> Result<(bool, u64), StorageError> {
        self.dispatch_handoff_and_persist_child_internal(
            handoff_id,
            child,
            child_snapshot,
            Some(child_selection_snapshot),
            Some((manifest, bundle_hash, source_ledger_json)),
            events,
        )
    }

    fn dispatch_handoff_and_persist_child_internal(
        &mut self,
        handoff_id: &str,
        child: &ExecutionRun,
        child_snapshot: &ModelSnapshot,
        child_selection_snapshot: Option<&ModelSelectionSnapshot>,
        context_manifest: Option<(&agenttalk_domain::ContextManifest, &str, &str)>,
        events: &[RuntimeEvent],
    ) -> Result<(bool, u64), StorageError> {
        validate_execution_run_model_snapshot_binding(child, child_snapshot)?;
        if let Some(selection_snapshot) = child_selection_snapshot {
            validate_execution_run_model_selection_snapshot_binding(child, selection_snapshot)?;
            validate_model_snapshot_selection_pair(child_snapshot, selection_snapshot)?;
        }
        if events.is_empty()
            || events
                .iter()
                .any(|event| event.execution_run_id != child.id)
        {
            return Err(StorageError::HandoffDispatchRejected {
                id: handoff_id.to_owned(),
                reason:
                    "handoff child initial events must be non-empty and belong to the child Run"
                        .into(),
            });
        }
        if let Some((manifest, _, _)) = context_manifest {
            let selection_snapshot =
                child_selection_snapshot.ok_or_else(|| StorageError::ModelSelectionInvalid {
                    reason: "handoff Context Manifest requires a frozen selection snapshot".into(),
                })?;
            validate_context_manifest_snapshot_route(
                child,
                child_snapshot,
                selection_snapshot,
                manifest,
            )?;
        }
        let tx = self.connection.transaction()?;
        let handoff = tx
            .query_row(
                "SELECT collaboration_run_id, from_execution_run_id, to_agent_id,
                        status, details_json
                 FROM handoffs WHERE id = ?1",
                [handoff_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, Option<String>>(4)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| StorageError::HandoffNotFound {
                id: handoff_id.to_owned(),
            })?;
        let stored_details = decode_handoff_details(handoff.4.as_deref())?;

        if handoff.3 == "dispatched" {
            if stored_details
                .as_ref()
                .and_then(|details| details.child_execution_run_id.as_deref())
                == Some(child.id.as_str())
            {
                let sequence = event_sequence_or_max(&tx, &child.id)?;
                if sequence == 0 {
                    return Err(StorageError::HandoffDispatchRejected {
                        id: handoff_id.to_owned(),
                        reason: "dispatched handoff has no persisted child event".into(),
                    });
                }
                persist_model_snapshot_row(&tx, child_snapshot)?;
                if let Some(selection_snapshot) = child_selection_snapshot {
                    persist_model_selection_snapshot_row(&tx, selection_snapshot)?;
                }
                if let Some((manifest, bundle_hash, source_ledger_json)) = context_manifest {
                    store_context_manifest_with_ledger_row(
                        &tx,
                        manifest,
                        bundle_hash,
                        source_ledger_json,
                    )?;
                }
                tx.commit()?;
                return Ok((false, sequence));
            }
            return Err(StorageError::HandoffDispatchRejected {
                id: handoff_id.to_owned(),
                reason: "handoff is already dispatched to a different or unknown child".into(),
            });
        }
        if handoff.3 != "approved" {
            return Err(StorageError::HandoffDispatchRejected {
                id: handoff_id.to_owned(),
                reason: format!("current status is {}", handoff.3),
            });
        }
        if child.collaboration_run_id != handoff.0 {
            return Err(StorageError::HandoffDispatchRejected {
                id: handoff_id.to_owned(),
                reason: "child collaboration_run_id does not match handoff".into(),
            });
        }
        if child.agent_id != handoff.2 {
            return Err(StorageError::HandoffDispatchRejected {
                id: handoff_id.to_owned(),
                reason: "child agent_id does not match handoff target".into(),
            });
        }

        let source = tx
            .query_row(
                "SELECT collaboration_run_id, project_id, conversation_id, agent_id
                 FROM execution_runs WHERE id = ?1",
                [&handoff.1],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| StorageError::HandoffExecutionNotFound {
                id: handoff.1.clone(),
            })?;
        if source.0 != handoff.0 {
            return Err(StorageError::HandoffDispatchRejected {
                id: handoff_id.to_owned(),
                reason: "handoff source execution belongs to a different collaboration".into(),
            });
        }
        if child.project_id != source.1
            || child.conversation_id != source.2
            || child.scope.project_id != child.project_id
            || child.scope.conversation_id != child.conversation_id
            || child.scope.agent_id != child.agent_id
        {
            return Err(StorageError::HandoffDispatchRejected {
                id: handoff_id.to_owned(),
                reason: "child scope does not match source execution".into(),
            });
        }
        if tx.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM project_agents
                WHERE project_id = ?1 AND agent_id = ?2 AND enabled != 0
            )",
            params![child.project_id, handoff.2],
            |row| row.get::<_, i64>(0),
        )? == 0
        {
            return Err(StorageError::HandoffDispatchRejected {
                id: handoff_id.to_owned(),
                reason: "child agent is not in the enabled target Project roster".into(),
            });
        }

        let collaboration = tx
            .query_row(
                "SELECT project_id, status, call_count, max_calls
                 FROM collaboration_runs WHERE id = ?1",
                [&handoff.0],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, u32>(2)?,
                        row.get::<_, u32>(3)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| StorageError::HandoffCollaborationNotFound {
                id: handoff.0.clone(),
            })?;
        if collaboration.0 != source.1
            || !is_running_collaboration_status(&collaboration.1)
            || collaboration.2 >= collaboration.3
        {
            return Err(StorageError::HandoffDispatchRejected {
                id: handoff_id.to_owned(),
                reason: "collaboration is not running or has exhausted its call budget".into(),
            });
        }

        let expected_status = serde_json::to_string(&child.status)?;
        let expected_scope = serde_json::to_string(&child.scope)?;
        let existing_child = tx
            .query_row(
                "SELECT collaboration_run_id, project_id, conversation_id, agent_id,
                        status, version, scope_json, terminal_reason, terminal
                 FROM execution_runs WHERE id = ?1",
                [&child.id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, u64>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, Option<String>>(7)?,
                        row.get::<_, i64>(8)?,
                    ))
                },
            )
            .optional()?;
        if let Some(ref existing) = existing_child {
            if existing
                != &(
                    child.collaboration_run_id.clone(),
                    child.project_id.clone(),
                    child.conversation_id.clone(),
                    child.agent_id.clone(),
                    expected_status.clone(),
                    child.version,
                    expected_scope.clone(),
                    child.terminal_reason.clone(),
                    child.status.is_terminal() as i64,
                )
            {
                return Err(StorageError::HandoffDispatchRejected {
                    id: handoff_id.to_owned(),
                    reason: "child execution id already belongs to a different Run".into(),
                });
            }
        }

        let running_status = serde_json::to_string(&CollaborationStatus::Running)?;
        let changed = tx.execute(
            "UPDATE collaboration_runs
             SET call_count = call_count + 1, status = ?2
             WHERE id = ?1 AND status = ?3 AND call_count = ?4 AND call_count < max_calls",
            params![handoff.0, running_status, collaboration.1, collaboration.2],
        )?;
        if changed != 1 {
            return Err(StorageError::HandoffDispatchRejected {
                id: handoff_id.to_owned(),
                reason: "collaboration budget changed before dispatch".into(),
            });
        }

        if existing_child.is_none() {
            tx.execute(
                "INSERT INTO execution_runs(
                    id, collaboration_run_id, project_id, conversation_id, agent_id,
                    status, version, scope_json, terminal_reason, terminal
                 ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                params![
                    &child.id,
                    &child.collaboration_run_id,
                    &child.project_id,
                    &child.conversation_id,
                    &child.agent_id,
                    expected_status,
                    child.version,
                    expected_scope,
                    &child.terminal_reason,
                    child.status.is_terminal() as i64,
                ],
            )?;
        }
        persist_model_snapshot_row(&tx, child_snapshot)?;
        if let Some(selection_snapshot) = child_selection_snapshot {
            persist_model_selection_snapshot_row(&tx, selection_snapshot)?;
        }
        if let Some((manifest, bundle_hash, source_ledger_json)) = context_manifest {
            store_context_manifest_with_ledger_row(&tx, manifest, bundle_hash, source_ledger_json)?;
        }
        let sequence = persist_runtime_events(&tx, events)?;

        let mut details = stored_details.unwrap_or_else(empty_handoff_details);
        details.parent_execution_run_id = Some(handoff.1.clone());
        details.child_execution_run_id = Some(child.id.clone());
        details.from_agent_id = Some(source.3);
        details.to_agent_id = Some(handoff.2.clone());
        let details_json = serde_json::to_string(&details)?;
        if tx.execute(
            "UPDATE handoffs
             SET status = 'dispatched', details_json = ?2
             WHERE id = ?1 AND status = 'approved'",
            params![handoff_id, details_json],
        )? != 1
        {
            return Err(StorageError::HandoffDispatchRejected {
                id: handoff_id.to_owned(),
                reason: "handoff status changed before dispatch commit".into(),
            });
        }

        tx.commit()?;
        Ok((true, sequence))
    }

    /// Transitions a handoff through the explicit allowlist. Returns `true`
    /// when the status changed and `false` for an exact same-status retry.
    pub fn transition_handoff(
        &mut self,
        handoff_id: &str,
        target_status: &str,
    ) -> Result<bool, StorageError> {
        let current_status: Option<String> = self
            .connection
            .query_row(
                "SELECT status FROM handoffs WHERE id = ?1",
                [handoff_id],
                |row| row.get(0),
            )
            .optional()?;
        let current_status = current_status.ok_or_else(|| StorageError::HandoffNotFound {
            id: handoff_id.to_owned(),
        })?;

        if current_status == target_status && is_known_handoff_status(&current_status) {
            return Ok(false);
        }
        if !is_valid_handoff_transition(&current_status, target_status) {
            return Err(StorageError::HandoffInvalidTransition {
                id: handoff_id.to_owned(),
                from_status: current_status,
                target_status: target_status.to_owned(),
            });
        }

        self.connection.execute(
            "UPDATE handoffs SET status = ?2 WHERE id = ?1 AND status = ?3",
            params![handoff_id, target_status, current_status],
        )?;
        Ok(true)
    }

    fn project_exists(&self, project_id: &str) -> Result<bool, StorageError> {
        Ok(self.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM projects WHERE id = ?1)",
            [project_id],
            |row| row.get(0),
        )?)
    }

    fn project_agent_is_rostered(
        &self,
        project_id: &str,
        agent_id: &str,
    ) -> Result<bool, StorageError> {
        Ok(self.connection.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM project_agents
                WHERE project_id = ?1 AND agent_id = ?2 AND enabled != 0
            )",
            params![project_id, agent_id],
            |row| row.get(0),
        )?)
    }

    /// Stores only the existing memory metadata contract. Repeating the exact
    /// same item is idempotent; reusing an id for different data is rejected.
    pub fn store_memory(&mut self, memory: &MemoryItem) -> Result<bool, StorageError> {
        let inserted = self.connection.execute(
            "INSERT INTO memories(id, scope_id, agent_id, content_hash, confirmed)
             VALUES(?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(id) DO NOTHING",
            params![
                memory.id,
                memory.scope_id,
                memory.agent_id,
                memory.content_hash,
                memory.confirmed as i64,
            ],
        )?;
        if inserted != 0 {
            return Ok(true);
        }

        let existing = self
            .connection
            .query_row(
                "SELECT scope_id, agent_id, content_hash, confirmed
                 FROM memories WHERE id = ?1",
                [&memory.id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(3)? != 0,
                    ))
                },
            )
            .optional()?;
        if existing
            == Some((
                memory.scope_id.clone(),
                memory.agent_id.clone(),
                memory.content_hash.clone(),
                memory.confirmed,
            ))
        {
            Ok(false)
        } else {
            Err(StorageError::MemoryConflict {
                id: memory.id.clone(),
            })
        }
    }

    /// Stores summary metadata only. Summary bodies are intentionally not part
    /// of the local projection contract; the hash/version identify an
    /// externally managed or future Artifact Store payload.
    pub fn store_summary(&mut self, summary: &Summary) -> Result<bool, StorageError> {
        if !self.memory_scope_exists(&summary.scope_id)? {
            return Err(StorageError::SummaryScopeNotFound {
                id: summary.scope_id.clone(),
            });
        }
        if summary.id.trim().is_empty() || summary.id.len() > 128 {
            return Err(StorageError::SummaryConflict {
                id: summary.id.clone(),
            });
        }
        if !is_sha256_hex(&summary.content_hash) {
            return Err(StorageError::SummaryConflict {
                id: summary.id.clone(),
            });
        }
        if let Some(artifact_id) = &summary.artifact_id {
            let artifact = self
                .connection
                .query_row(
                    "SELECT sha256 FROM artifacts WHERE id = ?1",
                    [artifact_id],
                    |row| row.get::<_, String>(0),
                )
                .optional()?
                .ok_or_else(|| StorageError::SummaryArtifactNotFound {
                    id: artifact_id.clone(),
                })?;
            if !artifact.eq_ignore_ascii_case(&summary.content_hash) {
                return Err(StorageError::SummaryArtifactMismatch {
                    id: artifact_id.clone(),
                });
            }
        }

        let inserted = self.connection.execute(
            "INSERT INTO summaries(id, scope_id, version, content_hash, artifact_id)
             VALUES(?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(id) DO NOTHING",
            params![
                summary.id,
                summary.scope_id,
                summary.version,
                summary.content_hash,
                summary.artifact_id,
            ],
        )?;
        if inserted != 0 {
            return Ok(true);
        }

        let existing = self
            .connection
            .query_row(
                "SELECT scope_id, version, content_hash, artifact_id FROM summaries WHERE id = ?1",
                [&summary.id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, u64>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                },
            )
            .optional()?;
        if existing
            == Some((
                summary.scope_id.clone(),
                summary.version,
                summary.content_hash.clone(),
                summary.artifact_id.clone(),
            ))
        {
            Ok(false)
        } else {
            Err(StorageError::SummaryConflict {
                id: summary.id.clone(),
            })
        }
    }

    pub fn next_summary_version(&self, scope_id: &str) -> Result<u64, StorageError> {
        let version = self.connection.query_row(
            "SELECT COALESCE(MAX(version), 0) + 1 FROM summaries WHERE scope_id = ?1",
            [scope_id],
            |row| row.get::<_, u64>(0),
        )?;
        Ok(version)
    }

    pub fn load_summary_content(&self, summary_id: &str) -> Result<String, StorageError> {
        let artifact_id = self
            .connection
            .query_row(
                "SELECT artifact_id FROM summaries WHERE id = ?1",
                [summary_id],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()?
            .flatten()
            .ok_or_else(|| StorageError::SummaryContentUnavailable {
                id: summary_id.to_owned(),
            })?;
        let body = self.load_artifact_body(&artifact_id)?;
        if body.len() > 64 * 1024 {
            return Err(StorageError::SummaryContentUnavailable {
                id: summary_id.to_owned(),
            });
        }
        String::from_utf8(body).map_err(|_| StorageError::SummaryContentUnavailable {
            id: summary_id.to_owned(),
        })
    }

    /// Stores artifact identity metadata only. The file body is never copied
    /// or read by this method.
    pub fn store_artifact(&mut self, artifact: &Artifact) -> Result<bool, StorageError> {
        validate_artifact_metadata(artifact)?;
        let inserted = self.connection.execute(
            "INSERT INTO artifacts(id, sha256, size, mime, relative_path)
             VALUES(?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(id) DO NOTHING",
            params![
                artifact.id,
                artifact.sha256,
                artifact.size,
                artifact.mime,
                artifact.relative_path,
            ],
        )?;
        if inserted != 0 {
            return Ok(true);
        }

        let existing = self
            .connection
            .query_row(
                "SELECT sha256, size, mime, relative_path FROM artifacts WHERE id = ?1",
                [&artifact.id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, u64>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                },
            )
            .optional()?;
        if existing
            == Some((
                artifact.sha256.clone(),
                artifact.size,
                artifact.mime.clone(),
                artifact.relative_path.clone(),
            ))
        {
            Ok(false)
        } else {
            Err(StorageError::ArtifactConflict {
                id: artifact.id.clone(),
            })
        }
    }

    /// Stores a bounded artifact body outside SQLite. The database metadata
    /// must already exist and its declared size/hash must match exactly.
    /// Blobs are addressed by their verified digest, written through a unique
    /// temporary file, and never exposed through projection snapshots.
    pub fn store_artifact_body(
        &self,
        artifact_id: &str,
        body: &[u8],
    ) -> Result<bool, StorageError> {
        if body.len() as u64 > ARTIFACT_BODY_MAX_BYTES {
            return Err(StorageError::ArtifactBodyTooLarge);
        }
        let metadata = self
            .connection
            .query_row(
                "SELECT sha256, size FROM artifacts WHERE id = ?1",
                [artifact_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?)),
            )
            .optional()?
            .ok_or_else(|| StorageError::ArtifactBodyNotFound {
                id: artifact_id.to_owned(),
            })?;
        if metadata.1 != body.len() as u64 || metadata.0 != hex_digest(body) {
            return Err(StorageError::ArtifactBodyMismatch);
        }

        let root = self
            .artifact_root
            .as_ref()
            .ok_or(StorageError::ArtifactBodyStoreUnavailable)?;
        fs::create_dir_all(root).map_err(|_| StorageError::ArtifactBodyIo)?;
        let destination = root.join(format!("{}.blob", metadata.0));
        if destination.exists() {
            let existing = fs::read(&destination).map_err(|_| StorageError::ArtifactBodyIo)?;
            if existing == body {
                return Ok(false);
            }
            return Err(StorageError::ArtifactBodyMismatch);
        }

        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| StorageError::ArtifactBodyIo)?
            .as_nanos();
        let temporary = root.join(format!(
            ".{}.{}.{}.tmp",
            metadata.0,
            std::process::id(),
            nonce
        ));
        let write_result = (|| -> Result<(), StorageError> {
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)
                .map_err(|_| StorageError::ArtifactBodyIo)?;
            file.write_all(body)
                .map_err(|_| StorageError::ArtifactBodyIo)?;
            file.sync_all().map_err(|_| StorageError::ArtifactBodyIo)?;
            Ok(())
        })();
        if let Err(error) = write_result {
            let _ = fs::remove_file(&temporary);
            return Err(error);
        }

        match fs::rename(&temporary, &destination) {
            Ok(()) => Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let _ = fs::remove_file(&temporary);
                let existing = fs::read(&destination).map_err(|_| StorageError::ArtifactBodyIo)?;
                if existing == body {
                    Ok(false)
                } else {
                    Err(StorageError::ArtifactBodyMismatch)
                }
            }
            Err(_) => {
                let _ = fs::remove_file(&temporary);
                Err(StorageError::ArtifactBodyIo)
            }
        }
    }

    /// Streams one explicitly selected absolute file into the Artifact Store
    /// without crossing the bounded IPC frame as base64. The source must be a
    /// regular, non-symlink file outside the Artifact Store. A digest-addressed
    /// blob survives source removal and process restart; the absolute source
    /// path is never persisted by this method.
    pub fn import_artifact_file(
        &self,
        source_path: &Path,
    ) -> Result<ImportedArtifactFile, StorageError> {
        let grant =
            FileReadGrant::issue(source_path).map_err(|_| StorageError::ArtifactSourceInvalid)?;
        self.import_artifact_file_with_grant(&grant)
    }

    /// Imports through a short-lived source receipt that was issued after an
    /// explicit file selection. The receipt is revalidated immediately before
    /// copying; it is not persisted and does not claim to be an OS capability.
    pub fn import_artifact_file_with_grant(
        &self,
        grant: &FileReadGrant,
    ) -> Result<ImportedArtifactFile, StorageError> {
        grant
            .validate()
            .map_err(|_| StorageError::ArtifactSourceInvalid)?;
        let source_path = grant.source_path();
        if !source_path.is_absolute() {
            return Err(StorageError::ArtifactSourceInvalid);
        }
        let source_link_metadata =
            fs::symlink_metadata(source_path).map_err(|_| StorageError::ArtifactSourceInvalid)?;
        if source_link_metadata.file_type().is_symlink() || !source_link_metadata.is_file() {
            return Err(StorageError::ArtifactSourceInvalid);
        }
        if source_link_metadata.len() > ARTIFACT_BODY_MAX_BYTES {
            return Err(StorageError::ArtifactBodyTooLarge);
        }
        let canonical_source =
            fs::canonicalize(source_path).map_err(|_| StorageError::ArtifactSourceInvalid)?;
        let source_metadata =
            fs::metadata(&canonical_source).map_err(|_| StorageError::ArtifactSourceInvalid)?;
        if !source_metadata.is_file() || source_metadata.len() > ARTIFACT_BODY_MAX_BYTES {
            return Err(if source_metadata.len() > ARTIFACT_BODY_MAX_BYTES {
                StorageError::ArtifactBodyTooLarge
            } else {
                StorageError::ArtifactSourceInvalid
            });
        }
        let file_name = canonical_source
            .file_name()
            .and_then(|value| value.to_str())
            .filter(|value| !value.is_empty())
            .ok_or(StorageError::ArtifactSourceInvalid)?
            .to_owned();

        let root = self
            .artifact_root
            .as_ref()
            .ok_or(StorageError::ArtifactBodyStoreUnavailable)?;
        fs::create_dir_all(root).map_err(|_| StorageError::ArtifactBodyIo)?;
        let canonical_root = fs::canonicalize(root).map_err(|_| StorageError::ArtifactBodyIo)?;
        if canonical_source.starts_with(&canonical_root) {
            return Err(StorageError::ArtifactSourceInvalid);
        }

        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| StorageError::ArtifactBodyIo)?
            .as_nanos();
        let temporary =
            canonical_root.join(format!(".import.{}.{}.tmp", std::process::id(), nonce));
        let staged = (|| -> Result<(String, u64), StorageError> {
            let mut source =
                File::open(&canonical_source).map_err(|_| StorageError::ArtifactSourceInvalid)?;
            let mut target = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)
                .map_err(|_| StorageError::ArtifactBodyIo)?;
            let mut hasher = Sha256::new();
            let mut total = 0_u64;
            let mut buffer = [0_u8; 64 * 1024];
            loop {
                let read = source
                    .read(&mut buffer)
                    .map_err(|_| StorageError::ArtifactBodyIo)?;
                if read == 0 {
                    break;
                }
                total = total
                    .checked_add(read as u64)
                    .ok_or(StorageError::ArtifactBodyTooLarge)?;
                if total > ARTIFACT_BODY_MAX_BYTES {
                    return Err(StorageError::ArtifactBodyTooLarge);
                }
                hasher.update(&buffer[..read]);
                target
                    .write_all(&buffer[..read])
                    .map_err(|_| StorageError::ArtifactBodyIo)?;
            }
            if total != source_metadata.len() {
                return Err(StorageError::ArtifactBodyMismatch);
            }
            target
                .sync_all()
                .map_err(|_| StorageError::ArtifactBodyIo)?;
            let digest = hasher.finalize();
            let sha256 = digest.iter().map(|byte| format!("{byte:02x}")).collect();
            Ok((sha256, total))
        })();
        let (sha256, size) = match staged {
            Ok(value) => value,
            Err(error) => {
                let _ = fs::remove_file(&temporary);
                return Err(error);
            }
        };

        let destination = canonical_root.join(format!("{sha256}.blob"));
        let body_stored = if destination.exists() {
            let _ = fs::remove_file(&temporary);
            verify_artifact_blob(&destination, &sha256, size)?;
            false
        } else {
            match fs::rename(&temporary, &destination) {
                Ok(()) => true,
                Err(_) if destination.exists() => {
                    let _ = fs::remove_file(&temporary);
                    verify_artifact_blob(&destination, &sha256, size)?;
                    false
                }
                Err(_) => {
                    let _ = fs::remove_file(&temporary);
                    return Err(StorageError::ArtifactBodyIo);
                }
            }
        };

        Ok(ImportedArtifactFile {
            sha256,
            size,
            file_name,
            body_stored,
        })
    }

    /// Loads a verified artifact body from the explicit Artifact Store root.
    /// The returned bytes are bounded by `ARTIFACT_BODY_MAX_BYTES`.
    pub fn load_artifact_body(&self, artifact_id: &str) -> Result<Vec<u8>, StorageError> {
        let metadata = self
            .connection
            .query_row(
                "SELECT sha256, size FROM artifacts WHERE id = ?1",
                [artifact_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?)),
            )
            .optional()?
            .ok_or_else(|| StorageError::ArtifactBodyNotFound {
                id: artifact_id.to_owned(),
            })?;
        if metadata.1 > ARTIFACT_BODY_MAX_BYTES {
            return Err(StorageError::ArtifactBodyTooLarge);
        }
        let root = self
            .artifact_root
            .as_ref()
            .ok_or(StorageError::ArtifactBodyStoreUnavailable)?;
        let path = root.join(format!("{}.blob", metadata.0));
        let body = fs::read(path).map_err(|_| StorageError::ArtifactBodyIo)?;
        if body.len() as u64 != metadata.1 || hex_digest(&body) != metadata.0 {
            return Err(StorageError::ArtifactBodyMismatch);
        }
        Ok(body)
    }

    /// Reads one bounded range from a verified Artifact Store blob. The range
    /// API is the large-content IPC boundary: callers may request successive
    /// chunks without putting the complete body into one JSON frame or one
    /// in-memory Core result.
    pub fn read_artifact_body_chunk(
        &self,
        artifact_id: &str,
        offset: u64,
        limit: u64,
    ) -> Result<ArtifactBodyChunk, StorageError> {
        if limit == 0 || limit > ARTIFACT_CONTENT_CHUNK_MAX_BYTES {
            return Err(StorageError::ArtifactBodyRangeInvalid);
        }
        let (sha256, size) = self
            .connection
            .query_row(
                "SELECT sha256, size FROM artifacts WHERE id = ?1",
                [artifact_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?)),
            )
            .optional()?
            .ok_or_else(|| StorageError::ArtifactBodyNotFound {
                id: artifact_id.to_owned(),
            })?;
        if size > ARTIFACT_BODY_MAX_BYTES {
            return Err(StorageError::ArtifactBodyTooLarge);
        }
        if offset > size {
            return Err(StorageError::ArtifactBodyRangeInvalid);
        }
        let root = self
            .artifact_root
            .as_ref()
            .ok_or(StorageError::ArtifactBodyStoreUnavailable)?;
        let path = root.join(format!("{sha256}.blob"));
        // Content-addressed blobs are expected to be immutable. Verify the
        // digest before exposing any range so a corrupted file cannot be
        // reassembled successfully by a client.
        verify_artifact_blob(&path, &sha256, size)?;

        let length = size.saturating_sub(offset).min(limit) as usize;
        let mut bytes = vec![0_u8; length];
        if length > 0 {
            let mut file = File::open(&path).map_err(|_| StorageError::ArtifactBodyIo)?;
            file.seek(SeekFrom::Start(offset))
                .map_err(|_| StorageError::ArtifactBodyIo)?;
            file.read_exact(&mut bytes)
                .map_err(|_| StorageError::ArtifactBodyIo)?;
        }
        let eof = offset.saturating_add(bytes.len() as u64) >= size;
        Ok(ArtifactBodyChunk {
            artifact_id: artifact_id.to_owned(),
            sha256,
            offset,
            size,
            bytes,
            eof,
        })
    }

    /// Associates one registered Artifact with one existing Message. The
    /// association stores only metadata and the stable message ordinal; body
    /// bytes remain in the Artifact Store addressed by the artifact digest.
    pub fn store_attachment(
        &mut self,
        attachment: &Attachment,
        ordinal: u64,
    ) -> Result<bool, StorageError> {
        validate_attachment_metadata(attachment, ordinal)?;
        let message_exists: bool = self.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM messages WHERE id = ?1)",
            [&attachment.message_id],
            |row| row.get(0),
        )?;
        if !message_exists {
            return Err(StorageError::AttachmentMessageNotFound {
                id: attachment.message_id.clone(),
            });
        }
        let artifact_metadata = self
            .connection
            .query_row(
                "SELECT sha256, size FROM artifacts WHERE id = ?1",
                [&attachment.artifact_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?)),
            )
            .optional()?
            .ok_or_else(|| StorageError::AttachmentArtifactNotFound {
                id: attachment.artifact_id.clone(),
            })?;
        if artifact_metadata.1 != attachment.size
            || !artifact_metadata.0.eq_ignore_ascii_case(&attachment.sha256)
        {
            return Err(StorageError::AttachmentArtifactMismatch {
                id: attachment.artifact_id.clone(),
            });
        }

        let existing_by_id = self
            .connection
            .query_row(
                "SELECT artifact_id, message_id, ordinal, file_name, sha256, size
                   FROM attachments WHERE attachment_id = ?1",
                [&attachment.id],
                |row| {
                    Ok((
                        row.get::<_, Option<String>>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, u64>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, u64>(5)?,
                    ))
                },
            )
            .optional()?;
        if let Some(existing) = existing_by_id {
            let same = existing.0.as_deref() == Some(attachment.artifact_id.as_str())
                && existing.1 == attachment.message_id
                && existing.2 == ordinal
                && existing.3 == attachment.file_name
                && existing.4.eq_ignore_ascii_case(&attachment.sha256)
                && existing.5 == attachment.size;
            return if same {
                Ok(false)
            } else {
                Err(StorageError::AttachmentConflict {
                    id: attachment.id.clone(),
                })
            };
        }

        let inserted = self.connection.execute(
            "INSERT INTO attachments(attachment_id, artifact_id, message_id, ordinal, file_name, sha256, size)
             VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(message_id, ordinal) DO NOTHING",
            params![
                attachment.id,
                attachment.artifact_id,
                attachment.message_id,
                ordinal,
                attachment.file_name,
                attachment.sha256,
                attachment.size,
            ],
        )?;
        if inserted != 0 {
            return Ok(true);
        }
        let existing = self
            .connection
            .query_row(
                "SELECT attachment_id, artifact_id, file_name, sha256, size
                   FROM attachments WHERE message_id = ?1 AND ordinal = ?2",
                params![&attachment.message_id, ordinal],
                |row| {
                    Ok((
                        row.get::<_, Option<String>>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, u64>(4)?,
                    ))
                },
            )
            .optional()?;
        if let Some(existing) = existing {
            if existing.0.as_deref() == Some(attachment.id.as_str())
                && existing.1.as_deref() == Some(attachment.artifact_id.as_str())
                && existing.2 == attachment.file_name
                && existing.3.eq_ignore_ascii_case(&attachment.sha256)
                && existing.4 == attachment.size
            {
                return Ok(false);
            }
        }
        Err(StorageError::AttachmentConflict {
            id: attachment.id.clone(),
        })
    }

    /// Stores retrieval source metadata for an existing project, conversation,
    /// or agent scope. Repeating the exact same source is idempotent; reusing
    /// an id for different data is rejected.
    pub fn store_retrieval_source(
        &mut self,
        source: &RetrievalSource,
    ) -> Result<bool, StorageError> {
        if !self.memory_scope_exists(&source.scope_id)? {
            return Err(StorageError::RetrievalScopeNotFound {
                id: source.scope_id.clone(),
            });
        }

        let inserted = self.connection.execute(
            "INSERT INTO retrieval_sources(id, scope_id, citation, sha256, token_count)
             VALUES(?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(id) DO NOTHING",
            params![
                source.id,
                source.scope_id,
                source.citation,
                source.sha256,
                source.token_count,
            ],
        )?;
        if inserted != 0 {
            return Ok(true);
        }

        let existing = self
            .connection
            .query_row(
                "SELECT scope_id, citation, sha256, token_count
                 FROM retrieval_sources WHERE id = ?1",
                [&source.id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, u64>(3)?,
                    ))
                },
            )
            .optional()?;
        if existing
            == Some((
                source.scope_id.clone(),
                source.citation.clone(),
                source.sha256.clone(),
                source.token_count,
            ))
        {
            Ok(false)
        } else {
            Err(StorageError::RetrievalConflict {
                id: source.id.clone(),
            })
        }
    }

    /// Returns only metadata sources explicitly scoped by the caller. When
    /// source ids are supplied, selection is exact and deterministic; this
    /// method does not read or reconstruct source bodies.
    pub fn query_retrieval_sources(
        &self,
        scope_id: &str,
        source_ids: Option<&[String]>,
        limit: u64,
    ) -> Result<Vec<serde_json::Value>, StorageError> {
        if !self.memory_scope_exists(scope_id)? {
            return Err(StorageError::RetrievalScopeNotFound {
                id: scope_id.to_owned(),
            });
        }
        let mut statement = self.connection.prepare(
            "SELECT id, scope_id, citation, sha256, token_count
             FROM retrieval_sources WHERE scope_id = ?1 ORDER BY id",
        )?;
        let rows = statement.query_map([scope_id], |row| {
            Ok(serde_json::json!({
                "id": row.get::<_, String>(0)?,
                "scopeId": row.get::<_, String>(1)?,
                "citation": row.get::<_, String>(2)?,
                "sha256": row.get::<_, String>(3)?,
                "tokenCount": row.get::<_, u64>(4)?,
            }))
        })?;
        let mut sources = rows.collect::<Result<Vec<_>, _>>()?;
        if let Some(source_ids) = source_ids {
            sources.retain(|source| {
                source
                    .get("id")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|id| source_ids.iter().any(|requested| requested == id))
            });
        }
        sources.truncate(limit.min(100) as usize);
        Ok(sources)
    }

    /// Persists an exact, caller-selected set of retrieval source metadata.
    /// The selection is bound to one project or conversation scope and stores
    /// hashes/ranges/reasons only; it never accepts source bodies or queries.
    pub fn store_retrieval_selection(
        &mut self,
        selection: &RetrievalSelection,
    ) -> Result<bool, StorageError> {
        validate_retrieval_selection_shape(selection)?;
        self.validate_retrieval_selection_scope(selection)?;
        for item in &selection.items {
            let source = self
                .connection
                .query_row(
                    "SELECT scope_id, sha256 FROM retrieval_sources WHERE id = ?1",
                    [&item.source_id],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                )
                .optional()?;
            let Some((source_scope_id, source_hash)) = source else {
                return Err(StorageError::RetrievalSelectionSourceNotFound {
                    id: item.source_id.clone(),
                });
            };
            if source_scope_id != selection.scope_id {
                return Err(StorageError::RetrievalSelectionSourceOutOfScope {
                    id: item.source_id.clone(),
                });
            }
            if source_hash != item.source_hash {
                return Err(StorageError::RetrievalSelectionSourceChanged {
                    id: item.source_id.clone(),
                });
            }
        }

        let items_json = serde_json::to_string(&selection.items)?;
        let inserted = self.connection.execute(
            "INSERT INTO retrieval_selections(
                id, scope_kind, scope_id, project_id, conversation_id,
                scope_revision, workspace_revision, retrieval_version, query_hash, items_json
             ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
             ON CONFLICT(id) DO NOTHING",
            params![
                selection.id,
                retrieval_selection_scope_sql(&selection.scope),
                selection.scope_id,
                selection.project_id,
                selection.conversation_id,
                selection.scope_revision,
                selection.workspace_revision,
                selection.retrieval_version,
                selection.query_hash,
                items_json,
            ],
        )?;
        if inserted != 0 {
            return Ok(true);
        }

        let existing = self
            .connection
            .query_row(
                "SELECT scope_kind, scope_id, project_id, conversation_id,
                        scope_revision, workspace_revision, retrieval_version, query_hash, items_json
                 FROM retrieval_selections WHERE id = ?1",
                [&selection.id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, u64>(4)?,
                        row.get::<_, Option<u64>>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, String>(7)?,
                        row.get::<_, String>(8)?,
                    ))
                },
            )
            .optional()?;
        let expected = (
            retrieval_selection_scope_sql(&selection.scope).to_owned(),
            selection.scope_id.clone(),
            selection.project_id.clone(),
            selection.conversation_id.clone(),
            selection.scope_revision,
            selection.workspace_revision,
            selection.retrieval_version.clone(),
            selection.query_hash.clone(),
            items_json,
        );
        if existing == Some(expected) {
            Ok(false)
        } else {
            Err(StorageError::RetrievalSelectionConflict {
                id: selection.id.clone(),
            })
        }
    }

    /// Returns only selections owned by the requested project/conversation
    /// scope. There is intentionally no unscoped retrieval query.
    pub fn query_retrieval_selections(
        &self,
        scope_id: &str,
        selection_ids: Option<&[String]>,
        limit: u64,
    ) -> Result<Vec<serde_json::Value>, StorageError> {
        if !self.retrieval_project_or_conversation_exists(scope_id)? {
            return Err(StorageError::RetrievalScopeNotFound {
                id: scope_id.to_owned(),
            });
        }
        let mut statement = self.connection.prepare(
            "SELECT id, scope_kind, scope_id, project_id, conversation_id,
                    scope_revision, workspace_revision, retrieval_version, query_hash, items_json
             FROM retrieval_selections WHERE scope_id = ?1 ORDER BY id",
        )?;
        let rows = statement.query_map([scope_id], |row| {
            let items_json = row.get::<_, String>(9)?;
            let items =
                serde_json::from_str::<serde_json::Value>(&items_json).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        9,
                        rusqlite::types::Type::Text,
                        Box::new(error),
                    )
                })?;
            Ok(serde_json::json!({
                "id": row.get::<_, String>(0)?,
                "scope": row.get::<_, String>(1)?,
                "scopeId": row.get::<_, String>(2)?,
                "projectId": row.get::<_, String>(3)?,
                "conversationId": row.get::<_, Option<String>>(4)?,
                "scopeRevision": row.get::<_, u64>(5)?,
                "workspaceRevision": row.get::<_, Option<u64>>(6)?,
                "retrievalVersion": row.get::<_, String>(7)?,
                "queryHash": row.get::<_, String>(8)?,
                "items": items,
            }))
        })?;
        let mut selections = rows.collect::<Result<Vec<_>, _>>()?;
        if let Some(selection_ids) = selection_ids {
            selections.retain(|selection| {
                selection
                    .get("id")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|id| selection_ids.iter().any(|requested| requested == id))
            });
        }
        selections.truncate(limit.min(100) as usize);
        Ok(selections)
    }

    /// Persists structured feedback for one source in one exact selection.
    /// Feedback is enum-only and therefore cannot become a prompt/secret sink.
    pub fn store_retrieval_feedback(
        &mut self,
        feedback: &RetrievalFeedback,
    ) -> Result<bool, StorageError> {
        validate_retrieval_feedback_shape(feedback)?;
        let selection_scope = self
            .connection
            .query_row(
                "SELECT scope_id, items_json FROM retrieval_selections WHERE id = ?1",
                [&feedback.selection_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;
        let Some((selection_scope_id, items_json)) = selection_scope else {
            return Err(StorageError::RetrievalFeedbackSelectionNotFound {
                id: feedback.selection_id.clone(),
            });
        };
        if selection_scope_id != feedback.scope_id {
            return Err(StorageError::RetrievalFeedbackScopeMismatch {
                id: feedback.id.clone(),
            });
        }
        let items = serde_json::from_str::<Vec<RetrievalSelectionItem>>(&items_json)?;
        if !items
            .iter()
            .any(|item| item.source_id == feedback.source_id)
        {
            return Err(StorageError::RetrievalFeedbackSourceNotSelected {
                id: feedback.source_id.clone(),
            });
        }

        let inserted = self.connection.execute(
            "INSERT INTO retrieval_feedback(
                id, selection_id, scope_id, source_id, label, reason, created_at_ms
             ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(id) DO NOTHING",
            params![
                feedback.id,
                feedback.selection_id,
                feedback.scope_id,
                feedback.source_id,
                retrieval_feedback_label_sql(&feedback.label),
                retrieval_feedback_reason_sql(&feedback.reason),
                feedback.created_at_ms,
            ],
        )?;
        if inserted != 0 {
            return Ok(true);
        }

        let existing = self
            .connection
            .query_row(
                "SELECT selection_id, scope_id, source_id, label, reason, created_at_ms
                 FROM retrieval_feedback WHERE id = ?1",
                [&feedback.id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, i64>(5)?,
                    ))
                },
            )
            .optional()?;
        let expected = (
            feedback.selection_id.clone(),
            feedback.scope_id.clone(),
            feedback.source_id.clone(),
            retrieval_feedback_label_sql(&feedback.label).to_owned(),
            retrieval_feedback_reason_sql(&feedback.reason).to_owned(),
            feedback.created_at_ms,
        );
        if existing == Some(expected) {
            Ok(false)
        } else {
            Err(StorageError::RetrievalFeedbackConflict {
                id: feedback.id.clone(),
            })
        }
    }

    pub fn query_retrieval_feedback(
        &self,
        scope_id: &str,
        selection_id: Option<&str>,
        limit: u64,
    ) -> Result<Vec<serde_json::Value>, StorageError> {
        if !self.retrieval_project_or_conversation_exists(scope_id)? {
            return Err(StorageError::RetrievalScopeNotFound {
                id: scope_id.to_owned(),
            });
        }
        let mut statement = self.connection.prepare(
            "SELECT id, selection_id, scope_id, source_id, label, reason, created_at_ms
             FROM retrieval_feedback
             WHERE scope_id = ?1 AND (?2 IS NULL OR selection_id = ?2)
             ORDER BY created_at_ms, id",
        )?;
        let rows = statement.query_map(params![scope_id, selection_id], |row| {
            Ok(serde_json::json!({
                "id": row.get::<_, String>(0)?,
                "selectionId": row.get::<_, String>(1)?,
                "scopeId": row.get::<_, String>(2)?,
                "sourceId": row.get::<_, String>(3)?,
                "label": row.get::<_, String>(4)?,
                "reason": row.get::<_, String>(5)?,
                "createdAtMs": row.get::<_, i64>(6)?,
            }))
        })?;
        let mut feedback = rows.collect::<Result<Vec<_>, _>>()?;
        feedback.truncate(limit.min(100) as usize);
        Ok(feedback)
    }

    fn validate_retrieval_selection_scope(
        &self,
        selection: &RetrievalSelection,
    ) -> Result<(), StorageError> {
        let valid = match selection.scope {
            RetrievalSelectionScope::Project => {
                selection.scope_id == selection.project_id
                    && selection.conversation_id.is_none()
                    && self.project_exists(&selection.project_id)?
            }
            RetrievalSelectionScope::Conversation => {
                selection.conversation_id.as_deref() == Some(selection.scope_id.as_str())
                    && self.connection.query_row(
                        "SELECT EXISTS(
                                SELECT 1 FROM conversations
                                WHERE id = ?1 AND project_id = ?2
                            )",
                        params![selection.scope_id, selection.project_id],
                        |row| row.get::<_, i64>(0),
                    )? != 0
            }
        };
        if valid {
            Ok(())
        } else {
            Err(StorageError::RetrievalSelectionScopeInvalid {
                id: selection.id.clone(),
            })
        }
    }

    fn retrieval_project_or_conversation_exists(
        &self,
        scope_id: &str,
    ) -> Result<bool, StorageError> {
        Ok(self.connection.query_row(
            "SELECT EXISTS(
                    SELECT 1 FROM projects WHERE id = ?1
                ) OR EXISTS(
                    SELECT 1 FROM conversations WHERE id = ?1
                )",
            [scope_id],
            |row| row.get::<_, i64>(0),
        )? != 0)
    }

    pub fn set_project_agent_assignment(
        &mut self,
        project_id: &str,
        agent_id: &str,
        enabled: bool,
        workspace_access: &WorkspaceAccess,
    ) -> Result<(), StorageError> {
        let existing = self.load_project_agent_model_selection(project_id, agent_id)?;
        let selection = existing
            .as_ref()
            .map(|value| value.selection.clone())
            .unwrap_or(ModelSelection {
                mode: ModelSelectionMode::Inherit,
                model_id: None,
            });
        let list_mode = existing
            .as_ref()
            .map(|value| value.candidate_model_list_mode)
            .unwrap_or(IdentityModelListMode::Inherit);
        let list_revision = existing
            .as_ref()
            .map(|value| value.candidate_model_list_revision)
            .unwrap_or(0);
        self.set_project_agent_assignment_with_model_selection(
            project_id,
            agent_id,
            enabled,
            workspace_access,
            &selection,
            list_mode,
            list_revision,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn set_project_agent_assignment_with_model_selection(
        &mut self,
        project_id: &str,
        agent_id: &str,
        enabled: bool,
        workspace_access: &WorkspaceAccess,
        selection: &ModelSelection,
        candidate_model_list_mode: IdentityModelListMode,
        candidate_model_list_revision: u64,
    ) -> Result<(), StorageError> {
        validate_model_selection(selection)?;
        validate_identity_model_list_mode(candidate_model_list_mode, false)?;
        validate_revision(candidate_model_list_revision)?;
        self.connection.execute(
            "INSERT INTO project_agents(project_id, agent_id, role, specialty, system_prompt, enabled, workspace_access, model_selection_mode, model_id, candidate_model_list_mode, candidate_model_list_revision)
             VALUES(?1, ?2, NULL, NULL, NULL, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(project_id, agent_id) DO UPDATE SET
               enabled = excluded.enabled,
               workspace_access = excluded.workspace_access,
               model_selection_mode = excluded.model_selection_mode,
               model_id = excluded.model_id,
               candidate_model_list_mode = excluded.candidate_model_list_mode,
               candidate_model_list_revision = excluded.candidate_model_list_revision",
            params![
                project_id,
                agent_id,
                enabled as i64,
                workspace_access_to_storage(workspace_access),
                model_selection_mode_to_storage(selection.mode),
                selection.model_id.as_deref(),
                identity_model_list_mode_to_storage(candidate_model_list_mode),
                candidate_model_list_revision as i64,
            ],
        )?;
        Ok(())
    }

    pub fn set_workspace_authorization(
        &mut self,
        authorization: &WorkspaceAuthorization,
    ) -> Result<(), StorageError> {
        self.connection.execute(
            "INSERT INTO workspace_authorizations(project_id, canonical_root, revision, validation_status) VALUES(?1, ?2, ?3, ?4) ON CONFLICT(project_id) DO UPDATE SET canonical_root = excluded.canonical_root, revision = excluded.revision, validation_status = excluded.validation_status",
            params![
                authorization.project_id,
                authorization.canonical_root,
                authorization.revision,
                authorization.validation_status,
            ],
        )?;
        Ok(())
    }

    pub fn load_workspace_authorizations(
        &self,
    ) -> Result<Vec<WorkspaceAuthorization>, StorageError> {
        let mut statement = self.connection.prepare(
            "SELECT project_id, canonical_root, revision, validation_status FROM workspace_authorizations ORDER BY project_id",
        )?;
        let rows = statement.query_map([], |row| {
            Ok(WorkspaceAuthorization {
                project_id: row.get(0)?,
                canonical_root: row.get(1)?,
                revision: row.get(2)?,
                validation_status: row.get(3)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn remove_project_agent_assignment(
        &mut self,
        project_id: &str,
        agent_id: &str,
    ) -> Result<bool, StorageError> {
        let changed = self.connection.execute(
            "DELETE FROM project_agents WHERE project_id = ?1 AND agent_id = ?2",
            params![project_id, agent_id],
        )?;
        Ok(changed != 0)
    }

    pub fn set_conversation_agent_assignment(
        &mut self,
        conversation_id: &str,
        agent_id: &str,
        enabled: bool,
    ) -> Result<(), StorageError> {
        let existing = self.load_conversation_agent_model_selection(conversation_id, agent_id)?;
        let selection = existing
            .as_ref()
            .map(|value| value.selection.clone())
            .unwrap_or(ModelSelection {
                mode: ModelSelectionMode::Inherit,
                model_id: None,
            });
        let list_mode = existing
            .as_ref()
            .map(|value| value.candidate_model_list_mode)
            .unwrap_or(IdentityModelListMode::Inherit);
        let list_revision = existing
            .as_ref()
            .map(|value| value.candidate_model_list_revision)
            .unwrap_or(0);
        self.set_conversation_agent_assignment_with_model_selection(
            conversation_id,
            agent_id,
            enabled,
            &selection,
            list_mode,
            list_revision,
        )
    }

    pub fn set_conversation_agent_assignment_with_model_selection(
        &mut self,
        conversation_id: &str,
        agent_id: &str,
        enabled: bool,
        selection: &ModelSelection,
        candidate_model_list_mode: IdentityModelListMode,
        candidate_model_list_revision: u64,
    ) -> Result<(), StorageError> {
        validate_model_selection(selection)?;
        validate_identity_model_list_mode(candidate_model_list_mode, false)?;
        validate_revision(candidate_model_list_revision)?;
        self.connection.execute(
            "INSERT INTO conversation_agents(conversation_id, agent_id, role, specialty, system_prompt, enabled, model_selection_mode, model_id, candidate_model_list_mode, candidate_model_list_revision)
             VALUES(?1, ?2, NULL, NULL, NULL, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(conversation_id, agent_id) DO UPDATE SET
               role = NULL,
               specialty = NULL,
               system_prompt = NULL,
               enabled = excluded.enabled,
               model_selection_mode = excluded.model_selection_mode,
               model_id = excluded.model_id,
               candidate_model_list_mode = excluded.candidate_model_list_mode,
               candidate_model_list_revision = excluded.candidate_model_list_revision",
            params![
                conversation_id,
                agent_id,
                enabled as i64,
                model_selection_mode_to_storage(selection.mode),
                selection.model_id.as_deref(),
                identity_model_list_mode_to_storage(candidate_model_list_mode),
                candidate_model_list_revision as i64,
            ],
        )?;
        Ok(())
    }

    pub fn remove_conversation_agent_assignment(
        &mut self,
        conversation_id: &str,
        agent_id: &str,
    ) -> Result<bool, StorageError> {
        let changed = self.connection.execute(
            "DELETE FROM conversation_agents WHERE conversation_id = ?1 AND agent_id = ?2",
            params![conversation_id, agent_id],
        )?;
        Ok(changed != 0)
    }

    pub fn set_agent_model_binding(
        &mut self,
        agent_id: &str,
        binding: &AgentModelBinding,
    ) -> Result<(), StorageError> {
        validate_agent_model_binding(binding)?;
        let changed = self.connection.execute(
            "UPDATE agents
             SET connector_id = ?2, model_id = ?3, candidate_model_list_revision = ?4
             WHERE id = ?1",
            params![
                agent_id,
                binding.connector_id.as_deref(),
                binding.model_id.as_deref(),
                binding.candidate_model_list_revision as i64,
            ],
        )?;
        if changed == 0 {
            return Err(StorageError::IdentityModelTargetNotFound {
                id: agent_id.to_owned(),
            });
        }
        Ok(())
    }

    /// Applies a presence-aware binding update in one SQLite transaction.
    /// Missing fields preserve their values, null/clear fields remove their
    /// values, and explicit strings replace them. Clearing a connector also
    /// clears the model because a pinned model without a connector is unsafe.
    pub fn patch_agent_model_binding(
        &mut self,
        agent_id: &str,
        patch: &AgentModelBindingPatch,
    ) -> Result<AgentModelBinding, StorageError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing = tx
            .query_row(
                "SELECT connector_id, model_id, candidate_model_list_revision
                 FROM agents WHERE id = ?1",
                [agent_id],
                |row| {
                    Ok(AgentModelBinding {
                        connector_id: row.get(0)?,
                        model_id: row.get(1)?,
                        candidate_model_list_revision: row.get::<_, i64>(2)?.max(0) as u64,
                    })
                },
            )
            .optional()?
            .ok_or_else(|| StorageError::IdentityModelTargetNotFound {
                id: agent_id.to_owned(),
            })?;
        let connector_id = match &patch.connector_id {
            BindingFieldPatch::Preserve => existing.connector_id.clone(),
            BindingFieldPatch::Clear => None,
            BindingFieldPatch::Set(value) => Some(value.clone()),
        };
        let model_id = match (&patch.connector_id, &patch.model_id) {
            (BindingFieldPatch::Clear, BindingFieldPatch::Set(_)) => {
                return Err(StorageError::ModelSelectionInvalid {
                    reason: "connector clear cannot be combined with a pinned model".into(),
                });
            }
            (BindingFieldPatch::Clear, _) => None,
            (_, BindingFieldPatch::Preserve) => existing.model_id.clone(),
            (_, BindingFieldPatch::Clear) => None,
            (_, BindingFieldPatch::Set(value)) => Some(value.clone()),
        };
        if connector_id.is_none() && model_id.is_some() {
            return Err(StorageError::ModelSelectionInvalid {
                reason: "a model binding requires a connector binding".into(),
            });
        }
        let candidate_model_list_revision = match patch.candidate_model_list_revision {
            BindingFieldPatch::Preserve => existing.candidate_model_list_revision,
            BindingFieldPatch::Clear => 0,
            BindingFieldPatch::Set(value) => value,
        };
        let updated = AgentModelBinding {
            connector_id,
            model_id,
            candidate_model_list_revision,
        };
        validate_agent_model_binding(&updated)?;
        tx.execute(
            "UPDATE agents
             SET connector_id = ?2, model_id = ?3, candidate_model_list_revision = ?4
             WHERE id = ?1",
            params![
                agent_id,
                updated.connector_id.as_deref(),
                updated.model_id.as_deref(),
                updated.candidate_model_list_revision as i64,
            ],
        )?;
        tx.commit()?;
        Ok(updated)
    }

    pub fn load_agent_model_binding(
        &self,
        agent_id: &str,
    ) -> Result<Option<AgentModelBinding>, StorageError> {
        Ok(self
            .connection
            .query_row(
                "SELECT connector_id, model_id, candidate_model_list_revision
                 FROM agents WHERE id = ?1",
                [agent_id],
                |row| {
                    Ok(AgentModelBinding {
                        connector_id: row.get(0)?,
                        model_id: row.get(1)?,
                        candidate_model_list_revision: row.get::<_, i64>(2)?.max(0) as u64,
                    })
                },
            )
            .optional()?)
    }

    pub fn load_project_agent_model_selection(
        &self,
        project_id: &str,
        agent_id: &str,
    ) -> Result<Option<StoredModelSelection>, StorageError> {
        Ok(self
            .connection
            .query_row(
                "SELECT model_selection_mode, model_id, candidate_model_list_mode,
                        candidate_model_list_revision
                 FROM project_agents WHERE project_id = ?1 AND agent_id = ?2",
                params![project_id, agent_id],
                map_stored_model_selection,
            )
            .optional()?)
    }

    pub fn load_conversation_agent_model_selection(
        &self,
        conversation_id: &str,
        agent_id: &str,
    ) -> Result<Option<StoredModelSelection>, StorageError> {
        Ok(self
            .connection
            .query_row(
                "SELECT model_selection_mode, model_id, candidate_model_list_mode,
                        candidate_model_list_revision
                 FROM conversation_agents WHERE conversation_id = ?1 AND agent_id = ?2",
                params![conversation_id, agent_id],
                map_stored_model_selection,
            )
            .optional()?)
    }

    pub fn upsert_identity_model_option(
        &mut self,
        option: &IdentityModelOption,
    ) -> Result<(), StorageError> {
        validate_identity_model_option(option)?;
        if !self.identity_model_target_exists(option)? {
            return Err(StorageError::IdentityModelTargetNotFound {
                id: option.id.clone(),
            });
        }
        let reasoning = serde_json::to_string(&option.reasoning_efforts)?;
        let tiers = serde_json::to_string(&option.service_tiers)?;
        // The default reset and the following insert/update must be atomic.
        // Otherwise a failed upsert could clear an existing default, and two
        // writers could momentarily persist more than one default for the same
        // identity target and Connector.
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing: Option<IdentityModelOptionKey> = tx.query_row(
                "SELECT identity_scope, agent_id, model_id, connector_id, project_id, conversation_id
                 FROM identity_model_options WHERE id = ?1",
                [&option.id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )
            .optional()?;
        if option.is_default {
            tx.execute(
                "UPDATE identity_model_options SET is_default = 0
                 WHERE identity_scope = ?1 AND agent_id = ?2
                   AND project_id IS ?3 AND conversation_id IS ?4 AND connector_id = ?5
                   AND id != ?6",
                params![
                    identity_model_scope_to_storage(option.scope),
                    &option.agent_id,
                    option.project_id.as_deref(),
                    option.conversation_id.as_deref(),
                    &option.connector_id,
                    &option.id,
                ],
            )?;
        }
        if let Some((scope, agent, model, connector, project, conversation)) = existing {
            if scope != identity_model_scope_to_storage(option.scope)
                || agent != option.agent_id
                || model != option.model_id
                || connector != option.connector_id
                || project != option.project_id
                || conversation != option.conversation_id
            {
                return Err(StorageError::IdentityModelOptionConflict {
                    id: option.id.clone(),
                });
            }
            tx.execute(
                "UPDATE identity_model_options SET project_id = ?2, conversation_id = ?3,
                    display_name = ?4, source = ?5, availability = ?6, is_default = ?7,
                    sort_order = ?8, catalog_revision = ?9, context_window = ?10,
                    reasoning_efforts_json = ?11, service_tiers_json = ?12
                 WHERE id = ?1",
                params![
                    &option.id,
                    option.project_id.as_deref(),
                    option.conversation_id.as_deref(),
                    &option.display_name,
                    model_option_source_to_storage(option.source),
                    model_availability_to_storage(option.availability),
                    option.is_default as i64,
                    option.sort_order as i64,
                    option.catalog_revision.as_deref(),
                    option.context_window.map(|value| value as i64),
                    reasoning,
                    tiers,
                ],
            )?;
            tx.commit()?;
            return Ok(());
        }
        tx.execute(
            "INSERT INTO identity_model_options(
                id, identity_scope, agent_id, project_id, conversation_id,
                connector_id, model_id, display_name, source, availability,
                is_default, sort_order, catalog_revision, context_window,
                reasoning_efforts_json, service_tiers_json
             ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)",
            params![
                &option.id,
                identity_model_scope_to_storage(option.scope),
                &option.agent_id,
                option.project_id.as_deref(),
                option.conversation_id.as_deref(),
                &option.connector_id,
                &option.model_id,
                &option.display_name,
                model_option_source_to_storage(option.source),
                model_availability_to_storage(option.availability),
                option.is_default as i64,
                option.sort_order as i64,
                option.catalog_revision.as_deref(),
                option.context_window.map(|value| value as i64),
                reasoning,
                tiers,
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn set_identity_model_option_default(
        &mut self,
        target: &IdentityModelListTarget,
        connector_id: &str,
        model_id: &str,
    ) -> Result<(), StorageError> {
        let options = self.query_identity_model_options(target, Some(connector_id))?;
        let selected = options
            .iter()
            .find(|option| option.model_id == model_id)
            .ok_or_else(|| StorageError::IdentityModelTargetNotFound {
                id: model_id.to_owned(),
            })?;
        let tx = self.connection.transaction()?;
        tx.execute(
            "UPDATE identity_model_options SET is_default = 0
             WHERE identity_scope = ?1 AND agent_id = ?2
               AND project_id IS ?3 AND conversation_id IS ?4 AND connector_id = ?5",
            params![
                identity_model_scope_to_storage(target.scope),
                &target.agent_id,
                target.project_id.as_deref(),
                target.conversation_id.as_deref(),
                connector_id,
            ],
        )?;
        tx.execute(
            "UPDATE identity_model_options SET is_default = 1
             WHERE id = ?1",
            [&selected.id],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn query_identity_model_options(
        &self,
        target: &IdentityModelListTarget,
        connector_id: Option<&str>,
    ) -> Result<Vec<IdentityModelOption>, StorageError> {
        validate_identity_model_target(target)?;
        let mut statement = self.connection.prepare(
            "SELECT id, identity_scope, agent_id, project_id, conversation_id,
                    connector_id, model_id, display_name, source, availability,
                    is_default, sort_order, catalog_revision, context_window,
                    reasoning_efforts_json, service_tiers_json
             FROM identity_model_options
             WHERE identity_scope = ?1 AND agent_id = ?2
               AND project_id IS ?3 AND conversation_id IS ?4
               AND (?5 IS NULL OR connector_id = ?5)
             ORDER BY sort_order, model_id",
        )?;
        let rows = statement.query_map(
            params![
                identity_model_scope_to_storage(target.scope),
                &target.agent_id,
                target.project_id.as_deref(),
                target.conversation_id.as_deref(),
                connector_id,
            ],
            map_identity_model_option,
        )?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    fn identity_model_target_exists(
        &self,
        option: &IdentityModelOption,
    ) -> Result<bool, StorageError> {
        let exists = match option.scope {
            IdentityModelListScope::BaseAgent => self.connection.query_row(
                "SELECT EXISTS(SELECT 1 FROM agents WHERE id = ?1)",
                [&option.agent_id],
                |row| row.get::<_, i64>(0),
            )?,
            IdentityModelListScope::ProjectAgent => self.connection.query_row(
                "SELECT EXISTS(SELECT 1 FROM project_agents
                 WHERE project_id = ?1 AND agent_id = ?2)",
                params![option.project_id.as_deref(), &option.agent_id],
                |row| row.get::<_, i64>(0),
            )?,
            IdentityModelListScope::ConversationAgent => self.connection.query_row(
                "SELECT EXISTS(SELECT 1 FROM conversation_agents
                 WHERE conversation_id = ?1 AND agent_id = ?2)",
                params![option.conversation_id.as_deref(), &option.agent_id],
                |row| row.get::<_, i64>(0),
            )?,
        };
        Ok(exists != 0)
    }

    pub fn upsert_model_selection_snapshot(
        &mut self,
        snapshot: &ModelSelectionSnapshot,
    ) -> Result<(), StorageError> {
        validate_model_selection_snapshot(snapshot)?;
        let run_exists: bool = self.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM execution_runs WHERE id = ?1)",
            [&snapshot.run_id],
            |row| row.get::<_, i64>(0),
        )? != 0;
        if !run_exists {
            return Err(StorageError::ModelSelectionSnapshotRunNotFound {
                id: snapshot.run_id.clone(),
            });
        }
        let json = serde_json::to_string(snapshot)?;
        let hash = hex_digest(json.as_bytes());
        let existing: Option<(String, String)> = self
            .connection
            .query_row(
                "SELECT snapshot_json, snapshot_hash FROM model_selection_snapshots WHERE run_id = ?1",
                [&snapshot.run_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if let Some((existing_json, existing_hash)) = existing {
            if existing_json == json && existing_hash == hash {
                return Ok(());
            }
            return Err(StorageError::ModelSelectionSnapshotConflict {
                id: snapshot.run_id.clone(),
            });
        }
        self.connection.execute(
            "INSERT INTO model_selection_snapshots(run_id, snapshot_json, snapshot_hash)
             VALUES(?1, ?2, ?3)",
            params![&snapshot.run_id, json, hash],
        )?;
        Ok(())
    }

    pub fn load_model_selection_snapshot(
        &self,
        run_id: &str,
    ) -> Result<Option<ModelSelectionSnapshot>, StorageError> {
        Ok(self
            .connection
            .query_row(
                "SELECT snapshot_json FROM model_selection_snapshots WHERE run_id = ?1",
                [run_id],
                |row| {
                    let value: String = row.get(0)?;
                    serde_json::from_str(&value).map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            0,
                            rusqlite::types::Type::Text,
                            Box::new(error),
                        )
                    })
                },
            )
            .optional()?)
    }

    pub fn load_model_selection_snapshots(
        &self,
    ) -> Result<Vec<ModelSelectionSnapshot>, StorageError> {
        let mut statement = self
            .connection
            .prepare("SELECT snapshot_json FROM model_selection_snapshots ORDER BY run_id")?;
        let rows = statement.query_map([], |row| {
            let value: String = row.get(0)?;
            serde_json::from_str(&value).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn conversation_agent_has_project_assignment(
        &self,
        conversation_id: &str,
        agent_id: &str,
    ) -> Result<bool, StorageError> {
        let assigned = self.connection.query_row(
            "SELECT EXISTS(
                SELECT 1
                FROM conversations AS c
                JOIN project_agents AS pa
                  ON pa.project_id = c.project_id
                 AND pa.agent_id = ?2
                 AND pa.enabled != 0
                WHERE c.id = ?1
            )",
            params![conversation_id, agent_id],
            |row| row.get::<_, i64>(0),
        )?;
        Ok(assigned != 0)
    }

    pub fn upsert_execution_run(&mut self, run: &ExecutionRun) -> Result<(), StorageError> {
        let changed = self.connection.execute(
            "INSERT INTO execution_runs(id, collaboration_run_id, project_id, conversation_id, agent_id, status, version, scope_json, terminal_reason, terminal) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10) ON CONFLICT(id) DO UPDATE SET collaboration_run_id=excluded.collaboration_run_id, project_id=excluded.project_id, conversation_id=excluded.conversation_id, agent_id=excluded.agent_id, status=excluded.status, version=excluded.version, scope_json=excluded.scope_json, terminal_reason=excluded.terminal_reason, terminal=excluded.terminal WHERE execution_runs.terminal = 0 AND excluded.version > execution_runs.version",
            params![
                run.id,
                run.collaboration_run_id,
                run.project_id,
                run.conversation_id,
                run.agent_id,
                serde_json::to_string(&run.status)?,
                run.version,
                serde_json::to_string(&run.scope)?,
                run.terminal_reason,
                run.status.is_terminal() as i64,
            ],
        )?;
        if changed == 0 {
            return Err(StorageError::ProjectionRejected);
        }
        Ok(())
    }

    pub fn persist_execution_run_and_events(
        &mut self,
        run: &ExecutionRun,
        events: &[RuntimeEvent],
    ) -> Result<u64, StorageError> {
        let tx = self.connection.transaction()?;
        let existing = tx
            .query_row(
                "SELECT terminal, version FROM execution_runs WHERE id = ?1",
                [&run.id],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, u64>(1)?)),
            )
            .optional()?;
        if let Some((terminal, version)) = existing {
            if terminal != 0 || run.version < version {
                return Err(StorageError::ProjectionRejected);
            }
        }
        tx.execute(
            "INSERT INTO execution_runs(id, collaboration_run_id, project_id, conversation_id, agent_id, status, version, scope_json, terminal_reason, terminal) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10) ON CONFLICT(id) DO UPDATE SET collaboration_run_id=excluded.collaboration_run_id, project_id=excluded.project_id, conversation_id=excluded.conversation_id, agent_id=excluded.agent_id, status=excluded.status, version=excluded.version, scope_json=excluded.scope_json, terminal_reason=excluded.terminal_reason, terminal=excluded.terminal WHERE execution_runs.terminal = 0 AND excluded.version > execution_runs.version",
            params![
                run.id,
                run.collaboration_run_id,
                run.project_id,
                run.conversation_id,
                run.agent_id,
                serde_json::to_string(&run.status)?,
                run.version,
                serde_json::to_string(&run.scope)?,
                run.terminal_reason,
                run.status.is_terminal() as i64,
            ],
        )?;
        let mut last_sequence = 0;
        for event in events {
            tx.execute(
                "INSERT INTO event_store(event_id, execution_run_id, runtime_id, thread_id, turn_id, event_type, timestamp_ms, payload_json) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
                params![
                    event.event_id,
                    event.execution_run_id,
                    event.runtime_id,
                    event.thread_id,
                    event.turn_id,
                    event.event_type,
                    event.timestamp_ms,
                    serde_json::to_string(&event.payload)?,
                ],
            )?;
            last_sequence = tx.last_insert_rowid() as u64;
        }
        if last_sequence == 0 {
            last_sequence = tx.query_row(
                "SELECT COALESCE(MAX(sequence), 0) FROM event_store",
                [],
                |row| row.get(0),
            )?;
        }
        tx.commit()?;
        Ok(last_sequence)
    }

    /// Persist a newly-created Run, its frozen model snapshot and its events
    /// in one SQLite transaction. The snapshot is immutable for a given Run;
    /// retries must create a new Run row instead of rewriting the source row.
    pub fn persist_execution_run_and_model_snapshot_and_events(
        &mut self,
        run: &ExecutionRun,
        snapshot: &ModelSnapshot,
        events: &[RuntimeEvent],
    ) -> Result<u64, StorageError> {
        validate_execution_run_model_snapshot_binding(run, snapshot)?;
        let tx = self.connection.transaction()?;
        persist_execution_run_row(&tx, run)?;
        persist_model_snapshot_row(&tx, snapshot)?;
        let last_sequence = persist_runtime_events(&tx, events)?;
        tx.commit()?;
        Ok(last_sequence)
    }

    /// Persist a new Run with both the connector-level snapshot and the full
    /// layered selection snapshot in the same transaction.
    pub fn persist_execution_run_and_model_snapshots_and_events(
        &mut self,
        run: &ExecutionRun,
        snapshot: &ModelSnapshot,
        selection_snapshot: &ModelSelectionSnapshot,
        events: &[RuntimeEvent],
    ) -> Result<u64, StorageError> {
        validate_execution_run_model_snapshot_binding(run, snapshot)?;
        validate_execution_run_model_selection_snapshot_binding(run, selection_snapshot)?;
        validate_model_snapshot_selection_pair(snapshot, selection_snapshot)?;
        let tx = self.connection.transaction()?;
        persist_execution_run_row(&tx, run)?;
        persist_model_snapshot_row(&tx, snapshot)?;
        persist_model_selection_snapshot_row(&tx, selection_snapshot)?;
        let last_sequence = persist_runtime_events(&tx, events)?;
        tx.commit()?;
        Ok(last_sequence)
    }

    /// Persists the complete initial execution boundary in one transaction:
    /// Run projection, both immutable selection snapshots, Context Manifest,
    /// and every initial event. A failed event or manifest write rolls back
    /// the Run and both snapshots as well, so recovery cannot observe a
    /// partially frozen Connector route.
    #[allow(clippy::too_many_arguments)]
    pub fn persist_execution_run_and_model_snapshots_context_manifest_and_events(
        &mut self,
        run: &ExecutionRun,
        snapshot: &ModelSnapshot,
        selection_snapshot: &ModelSelectionSnapshot,
        manifest: &agenttalk_domain::ContextManifest,
        bundle_hash: &str,
        source_ledger_json: &str,
        events: &[RuntimeEvent],
    ) -> Result<u64, StorageError> {
        validate_execution_run_model_snapshot_binding(run, snapshot)?;
        validate_execution_run_model_selection_snapshot_binding(run, selection_snapshot)?;
        validate_model_snapshot_selection_pair(snapshot, selection_snapshot)?;
        validate_context_manifest_snapshot_route(run, snapshot, selection_snapshot, manifest)?;
        let tx = self.connection.transaction()?;
        persist_execution_run_row(&tx, run)?;
        persist_model_snapshot_row(&tx, snapshot)?;
        persist_model_selection_snapshot_row(&tx, selection_snapshot)?;
        store_context_manifest_with_ledger_row(&tx, manifest, bundle_hash, source_ledger_json)?;
        let last_sequence = persist_runtime_events(&tx, events)?;
        tx.commit()?;
        Ok(last_sequence)
    }

    pub fn persist_command_receipt_and_execution_run_and_events(
        &mut self,
        receipt: &CommandReceipt,
        run: &ExecutionRun,
        events: &[RuntimeEvent],
    ) -> Result<u64, StorageError> {
        let result_json = receipt
            .result_json
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;
        let error_json = receipt
            .error_json
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;
        let tx = self.connection.transaction()?;
        tx.execute(
            "INSERT INTO command_receipts(
                scope_id, client_id, request_id, command, payload_hash,
                operation_key, state, result_json, error_json, created_at, updated_at
             ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
             ON CONFLICT(scope_id, client_id, request_id) DO UPDATE SET
                command = excluded.command,
                payload_hash = excluded.payload_hash,
                operation_key = excluded.operation_key,
                state = excluded.state,
                result_json = excluded.result_json,
                error_json = excluded.error_json,
                updated_at = excluded.updated_at",
            params![
                &receipt.key.scope_id,
                &receipt.key.client_id,
                &receipt.key.request_id,
                &receipt.command,
                &receipt.payload_hash,
                &receipt.operation_key,
                receipt.state.as_str(),
                result_json,
                error_json,
                receipt.created_at,
                receipt.updated_at,
            ],
        )?;
        let existing = tx
            .query_row(
                "SELECT terminal, version FROM execution_runs WHERE id = ?1",
                [&run.id],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, u64>(1)?)),
            )
            .optional()?;
        if let Some((terminal, version)) = existing {
            if terminal != 0 || run.version < version {
                return Err(StorageError::ProjectionRejected);
            }
        }
        tx.execute(
            "INSERT INTO execution_runs(id, collaboration_run_id, project_id, conversation_id, agent_id, status, version, scope_json, terminal_reason, terminal) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10) ON CONFLICT(id) DO UPDATE SET collaboration_run_id=excluded.collaboration_run_id, project_id=excluded.project_id, conversation_id=excluded.conversation_id, agent_id=excluded.agent_id, status=excluded.status, version=excluded.version, scope_json=excluded.scope_json, terminal_reason=excluded.terminal_reason, terminal=excluded.terminal WHERE execution_runs.terminal = 0 AND excluded.version > execution_runs.version",
            params![
                run.id,
                run.collaboration_run_id,
                run.project_id,
                run.conversation_id,
                run.agent_id,
                serde_json::to_string(&run.status)?,
                run.version,
                serde_json::to_string(&run.scope)?,
                run.terminal_reason,
                run.status.is_terminal() as i64,
            ],
        )?;
        let mut last_sequence = 0;
        for event in events {
            tx.execute(
                "INSERT INTO event_store(event_id, execution_run_id, runtime_id, thread_id, turn_id, event_type, timestamp_ms, payload_json) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
                params![
                    event.event_id,
                    event.execution_run_id,
                    event.runtime_id,
                    event.thread_id,
                    event.turn_id,
                    event.event_type,
                    event.timestamp_ms,
                    serde_json::to_string(&event.payload)?,
                ],
            )?;
            last_sequence = tx.last_insert_rowid() as u64;
        }
        if last_sequence == 0 {
            last_sequence = tx.query_row(
                "SELECT COALESCE(MAX(sequence), 0) FROM event_store",
                [],
                |row| row.get(0),
            )?;
        }
        tx.commit()?;
        Ok(last_sequence)
    }

    /// Receipt-aware variant of
    /// [`Self::persist_execution_run_and_model_snapshot_and_events`]. The
    /// command receipt, Run projection, frozen model snapshot and newly
    /// emitted events either commit together or remain absent after a crash.
    pub fn persist_command_receipt_and_execution_run_and_model_snapshot_and_events(
        &mut self,
        receipt: &CommandReceipt,
        run: &ExecutionRun,
        snapshot: &ModelSnapshot,
        events: &[RuntimeEvent],
    ) -> Result<u64, StorageError> {
        validate_execution_run_model_snapshot_binding(run, snapshot)?;
        let result_json = receipt
            .result_json
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;
        let error_json = receipt
            .error_json
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;
        let tx = self.connection.transaction()?;
        tx.execute(
            "INSERT INTO command_receipts(
                scope_id, client_id, request_id, command, payload_hash,
                operation_key, state, result_json, error_json, created_at, updated_at
             ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
             ON CONFLICT(scope_id, client_id, request_id) DO UPDATE SET
                command = excluded.command,
                payload_hash = excluded.payload_hash,
                operation_key = excluded.operation_key,
                state = excluded.state,
                result_json = excluded.result_json,
                error_json = excluded.error_json,
                updated_at = excluded.updated_at",
            params![
                &receipt.key.scope_id,
                &receipt.key.client_id,
                &receipt.key.request_id,
                &receipt.command,
                &receipt.payload_hash,
                &receipt.operation_key,
                receipt.state.as_str(),
                result_json,
                error_json,
                receipt.created_at,
                receipt.updated_at,
            ],
        )?;
        persist_execution_run_row(&tx, run)?;
        persist_model_snapshot_row(&tx, snapshot)?;
        let last_sequence = persist_runtime_events(&tx, events)?;
        tx.commit()?;
        Ok(last_sequence)
    }

    pub fn persist_command_receipt_and_execution_run_and_model_snapshots_and_events(
        &mut self,
        receipt: &CommandReceipt,
        run: &ExecutionRun,
        snapshot: &ModelSnapshot,
        selection_snapshot: &ModelSelectionSnapshot,
        events: &[RuntimeEvent],
    ) -> Result<u64, StorageError> {
        validate_execution_run_model_snapshot_binding(run, snapshot)?;
        validate_execution_run_model_selection_snapshot_binding(run, selection_snapshot)?;
        validate_model_snapshot_selection_pair(snapshot, selection_snapshot)?;
        let result_json = receipt
            .result_json
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;
        let error_json = receipt
            .error_json
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;
        let tx = self.connection.transaction()?;
        tx.execute(
            "INSERT INTO command_receipts(
                scope_id, client_id, request_id, command, payload_hash,
                operation_key, state, result_json, error_json, created_at, updated_at
             ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
             ON CONFLICT(scope_id, client_id, request_id) DO UPDATE SET
                command = excluded.command,
                payload_hash = excluded.payload_hash,
                operation_key = excluded.operation_key,
                state = excluded.state,
                result_json = excluded.result_json,
                error_json = excluded.error_json,
                updated_at = excluded.updated_at",
            params![
                &receipt.key.scope_id,
                &receipt.key.client_id,
                &receipt.key.request_id,
                &receipt.command,
                &receipt.payload_hash,
                &receipt.operation_key,
                receipt.state.as_str(),
                result_json,
                error_json,
                receipt.created_at,
                receipt.updated_at,
            ],
        )?;
        persist_execution_run_row(&tx, run)?;
        persist_model_snapshot_row(&tx, snapshot)?;
        persist_model_selection_snapshot_row(&tx, selection_snapshot)?;
        let last_sequence = persist_runtime_events(&tx, events)?;
        tx.commit()?;
        Ok(last_sequence)
    }

    /// Receipt-aware variant of
    /// [`Self::persist_execution_run_and_model_snapshots_context_manifest_and_events`].
    /// The idempotency receipt is part of the same transaction as the initial
    /// execution boundary, preventing a crash from replaying a response for a
    /// Run whose frozen route was never committed.
    #[allow(clippy::too_many_arguments)]
    pub fn persist_command_receipt_and_execution_run_and_model_snapshots_context_manifest_and_events(
        &mut self,
        receipt: &CommandReceipt,
        run: &ExecutionRun,
        snapshot: &ModelSnapshot,
        selection_snapshot: &ModelSelectionSnapshot,
        manifest: &agenttalk_domain::ContextManifest,
        bundle_hash: &str,
        source_ledger_json: &str,
        events: &[RuntimeEvent],
    ) -> Result<u64, StorageError> {
        validate_execution_run_model_snapshot_binding(run, snapshot)?;
        validate_execution_run_model_selection_snapshot_binding(run, selection_snapshot)?;
        validate_model_snapshot_selection_pair(snapshot, selection_snapshot)?;
        validate_context_manifest_snapshot_route(run, snapshot, selection_snapshot, manifest)?;
        let result_json = receipt
            .result_json
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;
        let error_json = receipt
            .error_json
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;
        let tx = self.connection.transaction()?;
        tx.execute(
            "INSERT INTO command_receipts(
                scope_id, client_id, request_id, command, payload_hash,
                operation_key, state, result_json, error_json, created_at, updated_at
             ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
             ON CONFLICT(scope_id, client_id, request_id) DO UPDATE SET
                command = excluded.command,
                payload_hash = excluded.payload_hash,
                operation_key = excluded.operation_key,
                state = excluded.state,
                result_json = excluded.result_json,
                error_json = excluded.error_json,
                updated_at = excluded.updated_at",
            params![
                &receipt.key.scope_id,
                &receipt.key.client_id,
                &receipt.key.request_id,
                &receipt.command,
                &receipt.payload_hash,
                &receipt.operation_key,
                receipt.state.as_str(),
                result_json,
                error_json,
                receipt.created_at,
                receipt.updated_at,
            ],
        )?;
        persist_execution_run_row(&tx, run)?;
        persist_model_snapshot_row(&tx, snapshot)?;
        persist_model_selection_snapshot_row(&tx, selection_snapshot)?;
        store_context_manifest_with_ledger_row(&tx, manifest, bundle_hash, source_ledger_json)?;
        let last_sequence = persist_runtime_events(&tx, events)?;
        tx.commit()?;
        Ok(last_sequence)
    }

    pub fn load_execution_run(&self, id: &str) -> Result<Option<ExecutionRun>, StorageError> {
        let value = self
            .connection
            .query_row(
                "SELECT id, collaboration_run_id, project_id, conversation_id, agent_id, status, version, scope_json, terminal_reason FROM execution_runs WHERE id = ?1",
                [id],
                map_execution_run,
            )
            .optional()?;
        Ok(value)
    }

    pub fn load_execution_runs(&self) -> Result<Vec<ExecutionRun>, StorageError> {
        let mut statement = self.connection.prepare(
            "SELECT id, collaboration_run_id, project_id, conversation_id, agent_id, status, version, scope_json, terminal_reason FROM execution_runs ORDER BY id",
        )?;
        let rows = statement.query_map([], map_execution_run)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn upsert_model_snapshot(&mut self, snapshot: &ModelSnapshot) -> Result<(), StorageError> {
        validate_model_snapshot(snapshot)?;
        let run_exists: bool = self.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM execution_runs WHERE id = ?1)",
            [&snapshot.run_id],
            |row| row.get::<_, i64>(0),
        )? != 0;
        if !run_exists {
            return Err(StorageError::ModelSnapshotRunNotFound {
                id: snapshot.run_id.clone(),
            });
        }
        let existing = self.load_model_snapshot(&snapshot.run_id)?;
        if let Some(existing) = existing {
            return if existing == *snapshot {
                Ok(())
            } else {
                Err(StorageError::ModelSnapshotConflict {
                    id: snapshot.run_id.clone(),
                })
            };
        }
        self.connection.execute(
            "INSERT INTO model_snapshots(run_id, connector_id, model_id, revision) VALUES(?1, ?2, ?3, ?4)",
            params![
                &snapshot.run_id,
                &snapshot.connector_id,
                &snapshot.model_id,
                snapshot.revision.map(|value| value as i64),
            ],
        )?;
        Ok(())
    }

    pub fn load_model_snapshot(&self, run_id: &str) -> Result<Option<ModelSnapshot>, StorageError> {
        Ok(self
            .connection
            .query_row(
                "SELECT run_id, connector_id, model_id, revision FROM model_snapshots WHERE run_id = ?1",
                [run_id],
                map_model_snapshot,
            )
            .optional()?)
    }

    pub fn load_model_snapshots(&self) -> Result<Vec<ModelSnapshot>, StorageError> {
        let mut statement = self.connection.prepare(
            "SELECT run_id, connector_id, model_id, revision FROM model_snapshots ORDER BY run_id",
        )?;
        let rows = statement.query_map([], map_model_snapshot)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn load_project_agent_assignments(
        &self,
    ) -> Result<Vec<(String, String, String, bool)>, StorageError> {
        let mut statement = self.connection.prepare(
            "SELECT project_id, agent_id, workspace_access, enabled FROM project_agents ORDER BY project_id, agent_id",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get::<_, i64>(3)? != 0,
            ))
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn load_conversation_agent_assignments(
        &self,
    ) -> Result<Vec<(String, String, bool)>, StorageError> {
        let mut statement = self.connection.prepare(
            "SELECT conversation_id, agent_id, enabled FROM conversation_agents ORDER BY conversation_id, agent_id",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get::<_, i64>(2)? != 0))
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn projection_snapshot(&self) -> Result<serde_json::Value, StorageError> {
        let projects = self.read_projection_rows(
            "SELECT id, name, root_path, archived FROM projects ORDER BY id",
            |row| {
                Ok(serde_json::json!({
                    "id": row.get::<_, String>(0)?,
                    "name": row.get::<_, String>(1)?,
                    "rootPath": row.get::<_, Option<String>>(2)?,
                    "archived": row.get::<_, i64>(3)? != 0,
                }))
            },
        )?;
        let agents = self.read_projection_rows(
            "SELECT id, name, role, specialty, system_prompt, connector_id, model_id,
                    candidate_model_list_revision
             FROM agents ORDER BY id",
            |row| {
                Ok(serde_json::json!({
                    "id": row.get::<_, String>(0)?,
                    "name": row.get::<_, String>(1)?,
                    "role": row.get::<_, String>(2)?,
                    "specialty": row.get::<_, String>(3)?,
                    "systemPrompt": row.get::<_, String>(4)?,
                    "connectorId": row.get::<_, Option<String>>(5)?,
                    "modelId": row.get::<_, Option<String>>(6)?,
                    "candidateModelListRevision": row.get::<_, u64>(7)?,
                }))
            },
        )?;
        let conversations = self.read_projection_rows(
            "SELECT id, project_id, title, scope_revision FROM conversations ORDER BY id",
            |row| {
                Ok(serde_json::json!({
                    "id": row.get::<_, String>(0)?,
                    "projectId": row.get::<_, String>(1)?,
                    "title": row.get::<_, String>(2)?,
                    "scopeRevision": row.get::<_, u64>(3)?,
                }))
            },
        )?;
        let assignments = self.read_projection_rows(
            "SELECT project_id, agent_id, enabled, workspace_access,
                    model_selection_mode, model_id, candidate_model_list_mode,
                    candidate_model_list_revision
             FROM project_agents ORDER BY project_id, agent_id",
            |row| {
                Ok(serde_json::json!({
                    "projectId": row.get::<_, String>(0)?,
                    "agentId": row.get::<_, String>(1)?,
                    "enabled": row.get::<_, i64>(2)? != 0,
                    "workspaceAccess": row.get::<_, String>(3)?,
                    "modelSelectionMode": row.get::<_, String>(4)?,
                    "modelId": row.get::<_, Option<String>>(5)?,
                    "candidateModelListMode": row.get::<_, String>(6)?,
                    "candidateModelListRevision": row.get::<_, u64>(7)?,
                }))
            },
        )?;
        let messages = self.read_projection_rows(
            "SELECT id, conversation_id, sender_id, sequence, content FROM messages ORDER BY conversation_id, sequence",
            |row| {
                Ok(serde_json::json!({
                    "id": row.get::<_, String>(0)?,
                    "conversationId": row.get::<_, String>(1)?,
                    "senderId": row.get::<_, String>(2)?,
                    "sequence": row.get::<_, u64>(3)?,
                    "content": row.get::<_, String>(4)?,
                }))
            },
        )?;
        let conversation_agents = self.read_projection_rows(
            "SELECT conversation_id, agent_id, role, specialty, enabled,
                    model_selection_mode, model_id, candidate_model_list_mode,
                    candidate_model_list_revision
             FROM conversation_agents ORDER BY conversation_id, agent_id",
            |row| {
                Ok(serde_json::json!({
                    "conversationId": row.get::<_, String>(0)?,
                    "agentId": row.get::<_, String>(1)?,
                    "role": row.get::<_, Option<String>>(2)?,
                    "specialty": row.get::<_, Option<String>>(3)?,
                    "enabled": row.get::<_, i64>(4)? != 0,
                    "modelSelectionMode": row.get::<_, String>(5)?,
                    "modelId": row.get::<_, Option<String>>(6)?,
                    "candidateModelListMode": row.get::<_, String>(7)?,
                    "candidateModelListRevision": row.get::<_, u64>(8)?,
                }))
            },
        )?;
        let workflows = self.read_projection_rows(
            "SELECT id, project_id, name, kind, steps_json FROM workflows ORDER BY id",
            |row| {
                Ok(serde_json::json!({
                    "id": row.get::<_, String>(0)?,
                    "projectId": row.get::<_, String>(1)?,
                    "name": row.get::<_, String>(2)?,
                    "kind": row.get::<_, String>(3)?,
                    "stepsJson": row.get::<_, String>(4)?,
                }))
            },
        )?;
        let collaboration_runs = self.read_projection_rows(
            "SELECT id, project_id, root_agent_ids_json, call_count, max_calls,
                    depth, max_depth, status, stop_reason, auto_dispatch_handoffs
             FROM collaboration_runs ORDER BY id",
            |row| {
                Ok(serde_json::json!({
                    "id": row.get::<_, String>(0)?,
                    "projectId": row.get::<_, String>(1)?,
                    "rootAgentIdsJson": row.get::<_, String>(2)?,
                    "callCount": row.get::<_, u32>(3)?,
                    "maxCalls": row.get::<_, u32>(4)?,
                    "depth": row.get::<_, u32>(5)?,
                    "maxDepth": row.get::<_, u32>(6)?,
                    "status": row.get::<_, String>(7)?,
                    "stopReason": row.get::<_, Option<String>>(8)?,
                    "autoDispatchHandoffs": row.get::<_, i64>(9)? != 0,
                }))
            },
        )?;
        let handoffs = self.read_projection_rows(
            "SELECT id, collaboration_run_id, from_execution_run_id, to_agent_id, status, details_json
             FROM handoffs ORDER BY id",
            |row| {
                Ok(serde_json::json!({
                    "id": row.get::<_, String>(0)?,
                    "collaborationRunId": row.get::<_, String>(1)?,
                    "fromExecutionRunId": row.get::<_, String>(2)?,
                    "toAgentId": row.get::<_, String>(3)?,
                    "status": row.get::<_, String>(4)?,
                    "details": row
                        .get::<_, Option<String>>(5)?
                        .and_then(|value| serde_json::from_str::<serde_json::Value>(&value).ok()),
                }))
            },
        )?;
        let model_snapshots = self.read_projection_rows(
            "SELECT run_id, connector_id, model_id, revision FROM model_snapshots ORDER BY run_id",
            |row| {
                Ok(serde_json::json!({
                    "runId": row.get::<_, String>(0)?,
                    "connectorId": row.get::<_, Option<String>>(1)?,
                    "modelId": row.get::<_, Option<String>>(2)?,
                    "revision": row.get::<_, Option<i64>>(3)?.map(|value| value as u64),
                }))
            },
        )?;
        let model_selection_snapshots = self.read_projection_rows(
            "SELECT snapshot_json FROM model_selection_snapshots ORDER BY run_id",
            |row| {
                let value = row.get::<_, String>(0)?;
                serde_json::from_str::<serde_json::Value>(&value).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Text,
                        Box::new(error),
                    )
                })
            },
        )?;
        let identity_model_options = self.read_projection_rows(
            "SELECT id, identity_scope, agent_id, project_id, conversation_id,
                    connector_id, model_id, display_name, source, availability,
                    is_default, sort_order, catalog_revision, context_window,
                    reasoning_efforts_json, service_tiers_json
             FROM identity_model_options
             ORDER BY identity_scope, agent_id, project_id, conversation_id, sort_order, model_id",
            |row| {
                let option = map_identity_model_option(row)?;
                serde_json::to_value(option).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Text,
                        Box::new(error),
                    )
                })
            },
        )?;
        let model_candidates = self.read_projection_rows(
            "SELECT id, agent_id, connector_id, model_id, available FROM model_candidates ORDER BY id",
            |row| {
                Ok(serde_json::json!({
                    "id": row.get::<_, String>(0)?,
                    "agentId": row.get::<_, String>(1)?,
                    "connectorId": row.get::<_, String>(2)?,
                    "modelId": row.get::<_, String>(3)?,
                    "available": row.get::<_, i64>(4)? != 0,
                }))
            },
        )?;
        let connector_profiles = self
            .query_connector_profiles(
                CONNECTOR_PROFILE_SCOPE,
                None,
                CONNECTOR_PROFILE_QUERY_LIMIT_MAX,
            )?
            .into_iter()
            .map(serde_json::to_value)
            .collect::<Result<Vec<_>, _>>()?;
        let retrieval_sources = self.read_projection_rows(
            "SELECT id, scope_id, citation, sha256, token_count FROM retrieval_sources ORDER BY id",
            |row| {
                Ok(serde_json::json!({
                    "id": row.get::<_, String>(0)?,
                    "scopeId": row.get::<_, String>(1)?,
                    "citation": row.get::<_, String>(2)?,
                    "sha256": row.get::<_, String>(3)?,
                    "tokenCount": row.get::<_, u64>(4)?,
                }))
            },
        )?;
        let retrieval_selections = self.read_projection_rows(
            "SELECT id, scope_kind, scope_id, project_id, conversation_id,
                    scope_revision, workspace_revision, retrieval_version, query_hash, items_json
             FROM retrieval_selections ORDER BY id",
            |row| {
                let items_json = row.get::<_, String>(9)?;
                let items =
                    serde_json::from_str::<serde_json::Value>(&items_json).map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            9,
                            rusqlite::types::Type::Text,
                            Box::new(error),
                        )
                    })?;
                Ok(serde_json::json!({
                    "id": row.get::<_, String>(0)?,
                    "scope": row.get::<_, String>(1)?,
                    "scopeId": row.get::<_, String>(2)?,
                    "projectId": row.get::<_, String>(3)?,
                    "conversationId": row.get::<_, Option<String>>(4)?,
                    "scopeRevision": row.get::<_, u64>(5)?,
                    "workspaceRevision": row.get::<_, Option<u64>>(6)?,
                    "retrievalVersion": row.get::<_, String>(7)?,
                    "queryHash": row.get::<_, String>(8)?,
                    "items": items,
                }))
            },
        )?;
        let retrieval_feedback = self.read_projection_rows(
            "SELECT id, selection_id, scope_id, source_id, label, reason, created_at_ms
             FROM retrieval_feedback ORDER BY created_at_ms, id",
            |row| {
                Ok(serde_json::json!({
                    "id": row.get::<_, String>(0)?,
                    "selectionId": row.get::<_, String>(1)?,
                    "scopeId": row.get::<_, String>(2)?,
                    "sourceId": row.get::<_, String>(3)?,
                    "label": row.get::<_, String>(4)?,
                    "reason": row.get::<_, String>(5)?,
                    "createdAtMs": row.get::<_, i64>(6)?,
                }))
            },
        )?;
        let summaries = self.read_projection_rows(
            "SELECT id, scope_id, version, content_hash, artifact_id FROM summaries ORDER BY id",
            |row| {
                let mut value = serde_json::json!({
                    "id": row.get::<_, String>(0)?,
                    "scopeId": row.get::<_, String>(1)?,
                    "version": row.get::<_, u64>(2)?,
                    "contentHash": row.get::<_, String>(3)?,
                });
                if let Some(artifact_id) = row.get::<_, Option<String>>(4)? {
                    value["artifactId"] = serde_json::Value::String(artifact_id);
                }
                Ok(value)
            },
        )?;
        let memories = self.read_projection_rows(
            "SELECT id, scope_id, agent_id, content_hash, confirmed FROM memories ORDER BY id",
            |row| {
                Ok(serde_json::json!({
                    "id": row.get::<_, String>(0)?,
                    "scopeId": row.get::<_, String>(1)?,
                    "agentId": row.get::<_, Option<String>>(2)?,
                    "contentHash": row.get::<_, String>(3)?,
                    "confirmed": row.get::<_, i64>(4)? != 0,
                }))
            },
        )?;
        let artifacts = self.read_projection_rows(
            "SELECT id, sha256, size, mime, relative_path FROM artifacts ORDER BY id",
            |row| {
                Ok(serde_json::json!({
                    "id": row.get::<_, String>(0)?,
                    "sha256": row.get::<_, String>(1)?,
                    "size": row.get::<_, u64>(2)?,
                    "mime": row.get::<_, String>(3)?,
                    "relativePath": row.get::<_, Option<String>>(4)?,
                }))
            },
        )?;
        let context_manifests = self.read_projection_rows(
            "SELECT id, execution_run_id, schema_version, bundle_hash, source_ledger_json,
                    connector_id, model_id
             FROM context_manifests ORDER BY id",
            |row| {
                let source_ledger_json = row.get::<_, String>(4)?;
                let source_ledger = serde_json::from_str::<serde_json::Value>(&source_ledger_json)
                    .map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            4,
                            rusqlite::types::Type::Text,
                            Box::new(error),
                        )
                    })?;
                let mut value = serde_json::json!({
                    "id": row.get::<_, String>(0)?,
                    "executionRunId": row.get::<_, String>(1)?,
                    "schemaVersion": row.get::<_, String>(2)?,
                    "bundleHash": row.get::<_, String>(3)?,
                    "connectorId": row.get::<_, Option<String>>(5)?,
                    "modelId": row.get::<_, Option<String>>(6)?,
                });
                if source_ledger != serde_json::json!([]) {
                    value["sourceLedger"] = source_ledger;
                }
                Ok(value)
            },
        )?;
        let attachments = self.read_projection_rows(
            "SELECT attachment_id, artifact_id, message_id, ordinal, file_name, sha256, size
               FROM attachments ORDER BY message_id, ordinal",
            |row| {
                Ok(serde_json::json!({
                    "attachmentId": row.get::<_, Option<String>>(0)?,
                    "artifactId": row.get::<_, Option<String>>(1)?,
                    "messageId": row.get::<_, String>(2)?,
                    "ordinal": row.get::<_, u64>(3)?,
                    "fileName": row.get::<_, String>(4)?,
                    "sha256": row.get::<_, String>(5)?,
                    "size": row.get::<_, u64>(6)?,
                }))
            },
        )?;
        let audit_timestamps = self.read_projection_rows(
            "SELECT entity_type, entity_id, created_at, updated_at FROM audit_timestamps ORDER BY entity_type, entity_id",
            |row| {
                Ok(serde_json::json!({
                    "entityType": row.get::<_, String>(0)?,
                    "entityId": row.get::<_, String>(1)?,
                    "createdAt": row.get::<_, String>(2)?,
                    "updatedAt": row.get::<_, String>(3)?,
                }))
            },
        )?;
        let workspace_authorizations = self.read_projection_rows(
            "SELECT project_id, canonical_root, revision, validation_status FROM workspace_authorizations ORDER BY project_id",
            |row| {
                Ok(serde_json::json!({
                    "projectId": row.get::<_, String>(0)?,
                    "canonicalRoot": row.get::<_, String>(1)?,
                    "revision": row.get::<_, u64>(2)?,
                    "validationStatus": row.get::<_, String>(3)?,
                }))
            },
        )?;
        let runs = self.read_projection_rows(
            "SELECT id, project_id, conversation_id, agent_id, status, version FROM execution_runs ORDER BY id",
            |row| {
                Ok(serde_json::json!({
                    "id": row.get::<_, String>(0)?,
                    "projectId": row.get::<_, String>(1)?,
                    "conversationId": row.get::<_, String>(2)?,
                    "agentId": row.get::<_, String>(3)?,
                    "status": row.get::<_, String>(4)?,
                    "version": row.get::<_, u64>(5)?,
                }))
            },
        )?;
        Ok(serde_json::json!({
            "projects": projects,
            "agents": agents,
            "conversations": conversations,
            "assignments": assignments,
            "messages": messages,
            "conversationAgents": conversation_agents,
            "workflows": workflows,
            "collaborationRuns": collaboration_runs,
            "handoffs": handoffs,
            "modelSnapshots": model_snapshots,
            "modelSelectionSnapshots": model_selection_snapshots,
            "identityModelOptions": identity_model_options,
            "modelCandidates": model_candidates,
            "connectorProfiles": connector_profiles,
            "retrievalSources": retrieval_sources,
            "retrievalSelections": retrieval_selections,
            "retrievalFeedback": retrieval_feedback,
            "summaries": summaries,
            "memories": memories,
            "artifacts": artifacts,
            "contextManifests": context_manifests,
            "attachments": attachments,
            "auditTimestamps": audit_timestamps,
            "workspaceAuthorizations": workspace_authorizations,
            "runs": runs,
        }))
    }

    fn read_projection_rows<T, F>(&self, sql: &str, mapper: F) -> Result<Vec<T>, StorageError>
    where
        F: FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    {
        let mut statement = self.connection.prepare(sql)?;
        let rows = statement.query_map([], mapper)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn append_event(&mut self, event: &RuntimeEvent) -> Result<u64, StorageError> {
        self.connection.execute("INSERT INTO event_store(event_id, execution_run_id, runtime_id, thread_id, turn_id, event_type, timestamp_ms, payload_json) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)", params![event.event_id, event.execution_run_id, event.runtime_id, event.thread_id, event.turn_id, event.event_type, event.timestamp_ms, serde_json::to_string(&event.payload)?])?;
        Ok(self.connection.last_insert_rowid() as u64)
    }

    pub fn replay_after(&self, sequence: u64) -> Result<Vec<RuntimeEvent>, StorageError> {
        self.replay_after_with_limit(sequence, None)
    }

    pub fn replay_after_limited(
        &self,
        sequence: u64,
        limit: u64,
    ) -> Result<Vec<RuntimeEvent>, StorageError> {
        self.replay_after_with_limit(sequence, Some(limit))
    }

    fn replay_after_with_limit(
        &self,
        sequence: u64,
        limit: Option<u64>,
    ) -> Result<Vec<RuntimeEvent>, StorageError> {
        if let Some(limit) = limit {
            let mut statement = self.connection.prepare(
                "SELECT event_id, execution_run_id, runtime_id, thread_id, turn_id, sequence, event_type, timestamp_ms, payload_json FROM event_store WHERE sequence > ?1 ORDER BY sequence LIMIT ?2",
            )?;
            let rows = statement.query_map(
                params![sequence, limit.min(i64::MAX as u64) as i64],
                map_runtime_event,
            )?;
            return Ok(rows.collect::<Result<Vec<_>, _>>()?);
        }
        let mut statement = self.connection.prepare(
            "SELECT event_id, execution_run_id, runtime_id, thread_id, turn_id, sequence, event_type, timestamp_ms, payload_json FROM event_store WHERE sequence > ?1 ORDER BY sequence",
        )?;
        let rows = statement.query_map([sequence], map_runtime_event)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }
}

fn map_connector_profile(row: &rusqlite::Row<'_>) -> rusqlite::Result<ConnectorProfile> {
    Ok(ConnectorProfile {
        scope_id: row.get(0)?,
        connector_id: row.get(1)?,
        display_name: row.get(2)?,
        provider_type: row.get(3)?,
        runtime_type: row.get(4)?,
        enabled: row.get::<_, i64>(5)? != 0,
        auth_env_key: row.get(6)?,
    })
}

fn validate_connector_scope(scope_id: &str) -> Result<(), StorageError> {
    if scope_id == CONNECTOR_PROFILE_SCOPE {
        Ok(())
    } else {
        Err(StorageError::ConnectorProfileScopeInvalid {
            scope: scope_id.to_owned(),
        })
    }
}

fn validate_connector_identifier(field: &str, value: &str) -> Result<(), StorageError> {
    if value.is_empty() || value.len() > 128 {
        return Err(StorageError::ConnectorProfileInvalid {
            field: field.to_owned(),
            reason: "must be 1..=128 bytes".into(),
        });
    }
    let valid = value.bytes().enumerate().all(|(index, byte)| {
        byte.is_ascii_alphanumeric() || (index > 0 && b"._:/-".contains(&byte))
    });
    if !valid {
        return Err(StorageError::ConnectorProfileInvalid {
            field: field.to_owned(),
            reason: "must use ASCII identifier characters".into(),
        });
    }
    Ok(())
}

fn validate_connector_display_name(value: &str) -> Result<(), StorageError> {
    if value.trim().is_empty() || value.chars().count() > 200 || value.chars().any(char::is_control)
    {
        return Err(StorageError::ConnectorProfileInvalid {
            field: "displayName".into(),
            reason: "must be 1..=200 characters without control characters".into(),
        });
    }
    Ok(())
}

fn validate_auth_env_key(value: Option<&str>) -> Result<(), StorageError> {
    let Some(value) = value else {
        return Ok(());
    };
    let valid = !value.is_empty()
        && value.len() <= 200
        && value.bytes().enumerate().all(|(index, byte)| {
            (index == 0 && (byte.is_ascii_uppercase() || byte == b'_'))
                || (index > 0
                    && (byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_'))
        });
    if !valid {
        return Err(StorageError::ConnectorProfileInvalid {
            field: "authEnvKey".into(),
            reason: "must be an environment variable name, not an auth value".into(),
        });
    }
    Ok(())
}

fn validate_connector_profile(profile: &ConnectorProfile) -> Result<(), StorageError> {
    validate_connector_scope(&profile.scope_id)?;
    validate_connector_identifier("connectorId", &profile.connector_id)?;
    validate_connector_display_name(&profile.display_name)?;
    validate_connector_identifier("providerType", &profile.provider_type)?;
    validate_connector_identifier("runtimeType", &profile.runtime_type)?;
    validate_auth_env_key(profile.auth_env_key.as_deref())?;
    Ok(())
}

fn validate_local_agent_import_request(
    request: &LocalAgentImportRequest,
) -> Result<(), StorageError> {
    validate_connector_profile(&request.connector)?;
    validate_connector_scope(&request.scope_id)?;
    validate_connector_identifier("clientId", &request.client_id)?;
    validate_connector_identifier("requestId", &request.request_id)?;
    validate_connector_identifier("importId", &request.import_id)?;
    validate_connector_identifier("projectId", &request.project_id)?;
    validate_connector_identifier("agentId", &request.agent_id)?;
    validate_connector_display_name(&request.agent_name)?;
    validate_connector_identifier("adapterKind", &request.binding.adapter_kind)?;
    validate_connector_identifier("manifestId", &request.binding.manifest_id)?;
    validate_fixed_hex("manifestSha256", &request.binding.manifest_sha256)?;
    validate_fixed_hex(
        "candidateBindingDigest",
        &request.binding.candidate_binding_digest,
    )?;
    if request.binding.protocol_major == 0 {
        return Err(StorageError::LocalAgentImportInvalid {
            field: "protocolMajor".into(),
        });
    }
    if serde_json::from_str::<serde_json::Value>(&request.binding.capabilities_json)
        .ok()
        .filter(serde_json::Value::is_object)
        .is_none()
    {
        return Err(StorageError::LocalAgentImportInvalid {
            field: "capabilities".into(),
        });
    }
    validate_model_selection(&request.model_selection)?;
    Ok(())
}

fn validate_fixed_hex(field: &str, value: &str) -> Result<(), StorageError> {
    if value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(StorageError::LocalAgentImportInvalid {
            field: field.to_owned(),
        })
    }
}

fn upsert_local_import_connector(
    tx: &rusqlite::Transaction<'_>,
    profile: &ConnectorProfile,
) -> Result<(), StorageError> {
    let existing: Option<ConnectorProfile> = tx
        .query_row(
            "SELECT scope_id, connector_id, display_name, provider_type, runtime_type, enabled, auth_env_key
             FROM connector_profiles WHERE scope_id = ?1 AND connector_id = ?2",
            params![&profile.scope_id, &profile.connector_id],
            map_connector_profile,
        )
        .optional()?;
    match existing {
        Some(existing) if existing == *profile => Ok(()),
        Some(_) => Err(StorageError::ConnectorProfileConflict {
            id: profile.connector_id.clone(),
        }),
        None => {
            tx.execute(
                "INSERT INTO connector_profiles(scope_id, connector_id, display_name, provider_type, runtime_type, enabled, auth_env_key)
                 VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    &profile.scope_id,
                    &profile.connector_id,
                    &profile.display_name,
                    &profile.provider_type,
                    &profile.runtime_type,
                    profile.enabled as i64,
                    &profile.auth_env_key,
                ],
            )?;
            Ok(())
        }
    }
}

fn upsert_local_agent_adapter_binding(
    tx: &rusqlite::Transaction<'_>,
    profile: &ConnectorProfile,
    binding: &LocalAgentAdapterBinding,
) -> Result<(), StorageError> {
    let existing: Option<LocalAgentAdapterBinding> = tx
        .query_row(
            "SELECT adapter_kind, protocol_major, manifest_id, manifest_sha256,
                    candidate_binding_digest, capabilities_json, auth_required
             FROM connector_adapter_bindings WHERE scope_id = ?1 AND connector_id = ?2",
            params![&profile.scope_id, &profile.connector_id],
            |row| {
                Ok(LocalAgentAdapterBinding {
                    adapter_kind: row.get(0)?,
                    protocol_major: row.get::<_, u16>(1)?,
                    manifest_id: row.get(2)?,
                    manifest_sha256: row.get(3)?,
                    candidate_binding_digest: row.get(4)?,
                    capabilities_json: row.get(5)?,
                    auth_required: row.get::<_, i64>(6)? != 0,
                })
            },
        )
        .optional()?;
    match existing {
        Some(existing) if existing == *binding => Ok(()),
        Some(_) => Err(StorageError::LocalAgentImportBindingConflict),
        None => {
            tx.execute(
                "INSERT INTO connector_adapter_bindings(
                    scope_id, connector_id, adapter_kind, protocol_major, manifest_id,
                    manifest_sha256, candidate_binding_digest, capabilities_json, auth_required
                 ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    &profile.scope_id,
                    &profile.connector_id,
                    &binding.adapter_kind,
                    binding.protocol_major,
                    &binding.manifest_id,
                    &binding.manifest_sha256,
                    &binding.candidate_binding_digest,
                    &binding.capabilities_json,
                    binding.auth_required as i64,
                ],
            )?;
            Ok(())
        }
    }
}

fn unix_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or_default()
}

struct RetrievalPreviewCandidate {
    source_type: &'static str,
    source_object_id: String,
    project_id: String,
    conversation_id: Option<String>,
    agent_id: Option<String>,
    body: String,
    permission_decision: String,
}

const LOCAL_VECTOR_DIMENSION: usize = 32;

fn retrieval_preview_invalid(reason: &str) -> StorageError {
    StorageError::RetrievalPreviewInvalid {
        reason: reason.to_owned(),
    }
}

fn validate_retrieval_preview_request(
    request: &RetrievalPreviewRequest,
) -> Result<(), StorageError> {
    if request.expected_project_id.trim().is_empty()
        || request.conversation_id.trim().is_empty()
        || request.agent_id.trim().is_empty()
    {
        return Err(retrieval_preview_invalid("scope identifiers are required"));
    }
    if !matches!(request.scope.as_str(), "conversation" | "project") {
        return Err(retrieval_preview_invalid(
            "scope must be conversation or project",
        ));
    }
    if request.query.trim().is_empty() {
        return Err(retrieval_preview_invalid("query must not be blank"));
    }
    if request.query.len() > 4096 {
        return Err(retrieval_preview_invalid(
            "query exceeds the bounded length",
        ));
    }
    if request.source_types.is_empty() {
        return Err(retrieval_preview_invalid("sourceTypes must not be empty"));
    }
    let mut seen = std::collections::BTreeSet::new();
    for source_type in &request.source_types {
        if !matches!(
            source_type.as_str(),
            "message" | "execution" | "project_file"
        ) {
            return Err(retrieval_preview_invalid(
                "sourceTypes contains an unknown value",
            ));
        }
        if !seen.insert(source_type) {
            return Err(retrieval_preview_invalid("sourceTypes contains duplicates"));
        }
    }
    if request.limit == 0 || request.limit > RETRIEVAL_PREVIEW_LIMIT_MAX {
        return Err(retrieval_preview_invalid(
            "limit is outside the bounded range",
        ));
    }
    Ok(())
}

fn escape_like_term(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

fn safe_execution_event_body(event_type: &str, payload: &serde_json::Value) -> Option<String> {
    let key = match event_type {
        "output.delta" => "delta",
        "execution.completed" => "output",
        _ => return None,
    };
    payload
        .get(key)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn ignored_project_file_directory(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "node_modules"
            | "dist"
            | "release"
            | ".git"
            | "logs"
            | ".next"
            | "__pycache__"
            | ".cache"
            | "build"
            | "out"
            | ".output"
            | ".nuxt"
            | "target"
            | "bin"
            | "obj"
            | ".vscode"
            | ".idea"
            | ".ssh"
            | ".aws"
            | ".azure"
            | ".kube"
            | ".gnupg"
    )
}

fn ignored_project_file(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    if matches!(
        lower.as_str(),
        ".env"
            | ".env.local"
            | ".env.production"
            | ".envrc"
            | ".npmrc"
            | ".pypirc"
            | "id_rsa"
            | "id_ed25519"
            | "credentials"
            | "credentials.json"
            | "secrets.json"
            | "service-account.json"
    ) {
        return true;
    }
    if lower.ends_with(".pem")
        || lower.ends_with(".key")
        || lower.ends_with(".p12")
        || lower.ends_with(".pfx")
        || lower.ends_with(".kdbx")
    {
        return true;
    }
    matches!(
        lower.rsplit_once('.').map(|(_, extension)| extension),
        Some(
            "exe"
                | "dll"
                | "so"
                | "dylib"
                | "bin"
                | "dat"
                | "png"
                | "jpg"
                | "jpeg"
                | "gif"
                | "bmp"
                | "ico"
                | "webp"
                | "mp3"
                | "mp4"
                | "avi"
                | "mov"
                | "wav"
                | "flv"
                | "zip"
                | "tar"
                | "gz"
                | "rar"
                | "7z"
                | "bz2"
                | "pdf"
                | "doc"
                | "docx"
                | "xls"
                | "xlsx"
                | "ppt"
                | "pptx"
                | "woff"
                | "woff2"
                | "ttf"
                | "eot"
                | "otf"
                | "lock"
                | "sum"
        )
    ) || lower.ends_with(".min.js")
        || lower.ends_with(".min.css")
}

fn read_project_file_prefix(path: &Path, size: u64) -> Option<String> {
    let mut file = File::open(path).ok()?;
    let bytes_to_read = size.min(RETRIEVAL_FILE_READ_MAX_BYTES as u64) as usize;
    let mut buffer = Vec::with_capacity(bytes_to_read);
    std::io::Read::by_ref(&mut file)
        .take(bytes_to_read as u64)
        .read_to_end(&mut buffer)
        .ok()?;
    if buffer.contains(&0) {
        return None;
    }
    String::from_utf8(buffer).ok()
}

fn build_retrieval_hit(
    candidate: &RetrievalPreviewCandidate,
    request: &RetrievalPreviewRequest,
) -> Option<serde_json::Value> {
    let normalized_body = candidate.body.to_lowercase();
    let normalized_query = request.query.trim().to_lowercase();
    let terms = normalized_query.split_whitespace().collect::<Vec<_>>();
    let matched_terms = terms
        .iter()
        .filter(|term| normalized_body.contains(**term))
        .count();
    if matched_terms == 0 {
        return None;
    }
    let phrase = normalized_body.contains(&normalized_query);
    let ratio = matched_terms as f64 / terms.len() as f64;
    let (score, match_reason, match_method) = if phrase {
        (1.0, "exact_phrase", "exact_phrase")
    } else if matched_terms == terms.len() {
        (0.95, "exact_terms", "exact_terms")
    } else {
        (0.4 + ratio * 0.4, "exact_terms", "exact_terms")
    };
    let snippet = retrieval_snippet(&candidate.body, terms[0]);
    let conversation_id = candidate.conversation_id.clone();
    Some(serde_json::json!({
        "hitId": format!("{}:{}", candidate.source_type, candidate.source_object_id),
        "projectId": candidate.project_id,
        "conversationId": conversation_id,
        "agentId": candidate.agent_id,
        "sourceType": candidate.source_type,
        "sourceObjectId": candidate.source_object_id,
        "sourceHash": hex_digest(candidate.body.as_bytes()),
        "selectedProjectId": request.expected_project_id,
        "selectedConversationId": request.conversation_id,
        "snippet": snippet,
        "matchReason": match_reason,
        "matchMethod": match_method,
        "score": score,
        "estimatedTokens": (candidate.body.chars().count() as u64).div_ceil(4),
        "trustLevel": "untrusted",
        "permissionDecision": candidate.permission_decision
    }))
}

fn build_vector_retrieval_hit(
    candidate: &RetrievalPreviewCandidate,
    request: &RetrievalPreviewRequest,
    provider: &dyn RetrievalEmbeddingProvider,
    descriptor: &RetrievalEmbeddingDescriptor,
    query_embedding: &[f64],
) -> Result<Option<serde_json::Value>, StorageError> {
    let body_embedding = embed_retrieval_text(provider, descriptor, &candidate.body)?;
    let Some(score) = cosine_similarity(query_embedding, &body_embedding) else {
        return Err(retrieval_preview_invalid(
            "embedding response is not comparable",
        ));
    };
    if score < 0.05 {
        return Ok(None);
    }
    let first_term = request.query.split_whitespace().next().unwrap_or_default();
    let (match_reason, match_method) = match descriptor.verification {
        RetrievalEmbeddingVerification::LocalFixture => {
            ("local_vector_similarity", "local_vector_fixture")
        }
        RetrievalEmbeddingVerification::VerifiedProvider => {
            ("semantic_similarity", "provider_vector")
        }
    };
    Ok(Some(serde_json::json!({
        "hitId": format!("{}:{}", candidate.source_type, candidate.source_object_id),
        "projectId": candidate.project_id,
        "conversationId": candidate.conversation_id,
        "agentId": candidate.agent_id,
        "sourceType": candidate.source_type,
        "sourceObjectId": candidate.source_object_id,
        "sourceHash": hex_digest(candidate.body.as_bytes()),
        "selectedProjectId": request.expected_project_id,
        "selectedConversationId": request.conversation_id,
        "snippet": retrieval_snippet(&candidate.body, first_term),
        "matchReason": match_reason,
        "matchMethod": match_method,
        "score": score,
        "estimatedTokens": (candidate.body.chars().count() as u64).div_ceil(4),
        "trustLevel": "untrusted",
        "permissionDecision": candidate.permission_decision
    })))
}

fn validate_embedding_descriptor(
    provider: &dyn RetrievalEmbeddingProvider,
) -> Result<RetrievalEmbeddingDescriptor, StorageError> {
    let descriptor = provider.descriptor();
    let is_identifier = |value: &str| {
        !value.is_empty()
            && value.len() <= 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    };
    if !is_identifier(&descriptor.provider_id) || !is_identifier(&descriptor.retrieval_version) {
        return Err(retrieval_preview_invalid("embedding descriptor is invalid"));
    }
    if descriptor.dimension == 0 || descriptor.dimension > 4096 {
        return Err(retrieval_preview_invalid("embedding dimension is invalid"));
    }
    Ok(descriptor)
}

fn embed_retrieval_text(
    provider: &dyn RetrievalEmbeddingProvider,
    descriptor: &RetrievalEmbeddingDescriptor,
    text: &str,
) -> Result<Vec<f64>, StorageError> {
    let bounded = text
        .chars()
        .take(RETRIEVAL_EMBEDDING_INPUT_MAX_CHARS)
        .collect::<String>();
    let embedding = provider
        .embed(&bounded)
        .map_err(|_| retrieval_preview_invalid("embedding provider unavailable"))?;
    if embedding.len() != descriptor.dimension || embedding.iter().any(|value| !value.is_finite()) {
        return Err(retrieval_preview_invalid("embedding response is invalid"));
    }
    Ok(embedding)
}

fn local_fixture_embedding(text: &str) -> [f64; LOCAL_VECTOR_DIMENSION] {
    let mut embedding = [0.0_f64; LOCAL_VECTOR_DIMENSION];
    for token in text
        .to_lowercase()
        .split(|character: char| !character.is_alphanumeric())
        .filter(|token| !token.is_empty())
    {
        let mut hash = 2_166_136_261_u32;
        for byte in token.as_bytes() {
            hash ^= u32::from(*byte);
            hash = hash.wrapping_mul(16_777_619);
        }
        let index = (hash as usize) % LOCAL_VECTOR_DIMENSION;
        let sign = if hash & 0x8000_0000 == 0 { 1.0 } else { -1.0 };
        embedding[index] += sign;
    }
    embedding
}

fn cosine_similarity(left: &[f64], right: &[f64]) -> Option<f64> {
    if left.is_empty() || left.len() != right.len() {
        return None;
    }
    let dot = left
        .iter()
        .zip(right.iter())
        .map(|(left, right)| left * right)
        .sum::<f64>();
    let left_norm = left.iter().map(|value| value * value).sum::<f64>().sqrt();
    let right_norm = right.iter().map(|value| value * value).sum::<f64>().sqrt();
    if left_norm == 0.0 || right_norm == 0.0 {
        return Some(0.0);
    }
    Some(((dot / (left_norm * right_norm)) + 1.0) / 2.0)
}

fn retrieval_snippet(body: &str, first_term: &str) -> String {
    let lower_body = body.to_lowercase();
    let lower_term = first_term.to_lowercase();
    let char_start = lower_body
        .find(&lower_term)
        .map(|byte_index| lower_body[..byte_index].chars().count())
        .unwrap_or(0);
    let chars = body.chars().collect::<Vec<_>>();
    let start = char_start.saturating_sub(80);
    let end = (start + RETRIEVAL_SNIPPET_MAX_CHARS).min(chars.len());
    chars[start..end]
        .iter()
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn validate_retrieval_selection_shape(selection: &RetrievalSelection) -> Result<(), StorageError> {
    let invalid = |reason: &str| {
        Err(StorageError::RetrievalSelectionInvalid {
            id: selection.id.clone(),
            reason: reason.to_owned(),
        })
    };
    if selection.id.trim().is_empty() || selection.id.len() > 160 {
        return invalid("id must be a bounded non-empty value");
    }
    if selection.project_id.trim().is_empty() || selection.project_id.len() > 160 {
        return invalid("project_id must be a bounded non-empty value");
    }
    if selection.scope_id.trim().is_empty() || selection.scope_id.len() > 160 {
        return invalid("scope_id must be a bounded non-empty value");
    }
    if selection.retrieval_version.trim().is_empty() || selection.retrieval_version.len() > 64 {
        return invalid("retrieval_version must be bounded and non-empty");
    }
    if !is_sha256_hex(&selection.query_hash) {
        return invalid("query_hash must be a sha256 hex digest");
    }
    if selection.items.is_empty() || selection.items.len() > 20 {
        return invalid("items must contain 1-20 exact sources");
    }
    let mut source_ids = std::collections::BTreeSet::new();
    let mut ranks = std::collections::BTreeSet::new();
    for item in &selection.items {
        if item.source_id.trim().is_empty() || item.source_id.len() > 160 {
            return invalid("source_id must be a bounded non-empty value");
        }
        if !is_sha256_hex(&item.source_hash) {
            return invalid("source_hash must be a sha256 hex digest");
        }
        if item.rank == 0 || !ranks.insert(item.rank) {
            return invalid("ranks must be positive and unique");
        }
        if item.score_milli > 1000 {
            return invalid("score_milli must be between 0 and 1000");
        }
        if !source_ids.insert(item.source_id.clone()) {
            return invalid("source ids must be unique");
        }
        if let Some(range) = &item.range {
            if range.start == Some(0)
                || range.end == Some(0)
                || (range.end.is_some() && range.start.is_none())
                || matches!((range.start, range.end), (Some(start), Some(end)) if end < start)
            {
                return invalid("ranges must use positive ascending line numbers");
            }
        }
    }
    Ok(())
}

fn validate_retrieval_feedback_shape(feedback: &RetrievalFeedback) -> Result<(), StorageError> {
    if feedback.id.trim().is_empty() || feedback.id.len() > 160 {
        return Err(StorageError::RetrievalFeedbackInvalid {
            id: feedback.id.clone(),
            reason: "id must be a bounded non-empty value".into(),
        });
    }
    if feedback.selection_id.trim().is_empty() || feedback.selection_id.len() > 160 {
        return Err(StorageError::RetrievalFeedbackInvalid {
            id: feedback.id.clone(),
            reason: "selection_id must be a bounded non-empty value".into(),
        });
    }
    if feedback.scope_id.trim().is_empty() || feedback.scope_id.len() > 160 {
        return Err(StorageError::RetrievalFeedbackInvalid {
            id: feedback.id.clone(),
            reason: "scope_id must be a bounded non-empty value".into(),
        });
    }
    if feedback.source_id.trim().is_empty() || feedback.source_id.len() > 160 {
        return Err(StorageError::RetrievalFeedbackInvalid {
            id: feedback.id.clone(),
            reason: "source_id must be a bounded non-empty value".into(),
        });
    }
    if feedback.created_at_ms < 0 {
        return Err(StorageError::RetrievalFeedbackInvalid {
            id: feedback.id.clone(),
            reason: "created_at_ms must be non-negative".into(),
        });
    }
    Ok(())
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn validate_artifact_metadata(artifact: &Artifact) -> Result<(), StorageError> {
    if artifact.id.trim().is_empty() || artifact.id.len() > 128 {
        return Err(StorageError::ArtifactInvalid {
            reason: "id must be 1..=128 bytes".into(),
        });
    }
    if !is_sha256_hex(&artifact.sha256) {
        return Err(StorageError::ArtifactInvalid {
            reason: "sha256 must be a 64-character hexadecimal digest".into(),
        });
    }
    if artifact.mime.trim().is_empty() || artifact.mime.len() > 256 {
        return Err(StorageError::ArtifactInvalid {
            reason: "mime must be 1..=256 bytes".into(),
        });
    }
    if let Some(relative_path) = &artifact.relative_path {
        let invalid = relative_path.is_empty()
            || relative_path.len() > 1024
            || relative_path.starts_with(['/', '\\'])
            || relative_path.contains(':')
            || relative_path
                .split(['/', '\\'])
                .any(|component| component == ".." || component.is_empty());
        if invalid {
            return Err(StorageError::ArtifactInvalid {
                reason: "relative_path must be a bounded, relative path without traversal".into(),
            });
        }
    }
    Ok(())
}

fn validate_attachment_metadata(attachment: &Attachment, ordinal: u64) -> Result<(), StorageError> {
    for (field, value) in [
        ("id", attachment.id.as_str()),
        ("message_id", attachment.message_id.as_str()),
        ("artifact_id", attachment.artifact_id.as_str()),
    ] {
        if value.trim().is_empty() || value.len() > 128 {
            return Err(StorageError::AttachmentInvalid {
                reason: format!("{field} must be 1..=128 bytes"),
            });
        }
    }
    if ordinal > 1_000_000 {
        return Err(StorageError::AttachmentInvalid {
            reason: "ordinal must be in the bounded range 0..=1000000".into(),
        });
    }
    if attachment.file_name.is_empty()
        || attachment.file_name.len() > 1024
        || attachment.file_name.contains(['/', '\\', '\0'])
        || attachment.file_name == "."
        || attachment.file_name == ".."
        || attachment.file_name.chars().any(char::is_control)
    {
        return Err(StorageError::AttachmentInvalid {
            reason: "file_name must be a bounded basename without traversal or control characters"
                .into(),
        });
    }
    if !is_sha256_hex(&attachment.sha256) {
        return Err(StorageError::AttachmentInvalid {
            reason: "sha256 must be a 64-character hexadecimal digest".into(),
        });
    }
    if attachment.size > ARTIFACT_BODY_MAX_BYTES {
        return Err(StorageError::AttachmentInvalid {
            reason: "size exceeds the configured artifact body limit".into(),
        });
    }
    Ok(())
}

fn retrieval_selection_scope_sql(value: &RetrievalSelectionScope) -> &'static str {
    match value {
        RetrievalSelectionScope::Project => "project",
        RetrievalSelectionScope::Conversation => "conversation",
    }
}

fn retrieval_feedback_label_sql(value: &RetrievalFeedbackLabel) -> &'static str {
    match value {
        RetrievalFeedbackLabel::Helpful => "helpful",
        RetrievalFeedbackLabel::NotHelpful => "not_helpful",
    }
}

fn retrieval_feedback_reason_sql(value: &RetrievalFeedbackReason) -> &'static str {
    match value {
        RetrievalFeedbackReason::ExactMatch => "exact_match",
        RetrievalFeedbackReason::Irrelevant => "irrelevant",
        RetrievalFeedbackReason::StaleSource => "stale_source",
        RetrievalFeedbackReason::WrongScope => "wrong_scope",
        RetrievalFeedbackReason::Duplicate => "duplicate",
        RetrievalFeedbackReason::Permission => "permission",
    }
}

fn map_runtime_event(row: &rusqlite::Row<'_>) -> rusqlite::Result<RuntimeEvent> {
    let payload: String = row.get(8)?;
    Ok(RuntimeEvent {
        event_id: row.get(0)?,
        execution_run_id: row.get(1)?,
        runtime_id: row.get(2)?,
        thread_id: row.get(3)?,
        turn_id: row.get(4)?,
        sequence: row.get(5)?,
        event_type: row.get(6)?,
        timestamp_ms: row.get(7)?,
        payload: serde_json::from_str(&payload).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                8,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?,
    })
}

fn map_execution_run(row: &rusqlite::Row<'_>) -> rusqlite::Result<ExecutionRun> {
    let status: String = row.get(5)?;
    let scope: String = row.get(7)?;
    Ok(ExecutionRun {
        id: row.get(0)?,
        collaboration_run_id: row.get(1)?,
        project_id: row.get(2)?,
        conversation_id: row.get(3)?,
        agent_id: row.get(4)?,
        status: parse_execution_status(&status).ok_or_else(|| {
            rusqlite::Error::InvalidColumnType(5, "status".into(), rusqlite::types::Type::Text)
        })?,
        version: row.get(6)?,
        scope: serde_json::from_str(&scope).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                7,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?,
        terminal_reason: row.get(8)?,
    })
}

fn map_model_snapshot(row: &rusqlite::Row<'_>) -> rusqlite::Result<ModelSnapshot> {
    Ok(ModelSnapshot {
        run_id: row.get(0)?,
        connector_id: row.get(1)?,
        model_id: row.get(2)?,
        revision: row.get(3)?,
    })
}

fn map_stored_model_selection(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredModelSelection> {
    let mode: String = row.get(0)?;
    let list_mode: String = row.get(2)?;
    Ok(StoredModelSelection {
        selection: ModelSelection {
            mode: parse_model_selection_mode(&mode).ok_or_else(|| {
                rusqlite::Error::InvalidColumnType(
                    0,
                    "model_selection_mode".into(),
                    rusqlite::types::Type::Text,
                )
            })?,
            model_id: row.get(1)?,
        },
        candidate_model_list_mode: parse_identity_model_list_mode(&list_mode).ok_or_else(|| {
            rusqlite::Error::InvalidColumnType(
                2,
                "candidate_model_list_mode".into(),
                rusqlite::types::Type::Text,
            )
        })?,
        candidate_model_list_revision: row.get::<_, i64>(3)?.max(0) as u64,
    })
}

fn map_identity_model_option(row: &rusqlite::Row<'_>) -> rusqlite::Result<IdentityModelOption> {
    let scope: String = row.get(1)?;
    let source: String = row.get(8)?;
    let availability: String = row.get(9)?;
    let reasoning: String = row.get(14)?;
    let tiers: String = row.get(15)?;
    Ok(IdentityModelOption {
        id: row.get(0)?,
        scope: parse_identity_model_scope(&scope).ok_or_else(|| {
            rusqlite::Error::InvalidColumnType(
                1,
                "identity_scope".into(),
                rusqlite::types::Type::Text,
            )
        })?,
        agent_id: row.get(2)?,
        project_id: row.get(3)?,
        conversation_id: row.get(4)?,
        connector_id: row.get(5)?,
        model_id: row.get(6)?,
        display_name: row.get(7)?,
        source: parse_model_option_source(&source).ok_or_else(|| {
            rusqlite::Error::InvalidColumnType(8, "source".into(), rusqlite::types::Type::Text)
        })?,
        availability: parse_model_availability(&availability).ok_or_else(|| {
            rusqlite::Error::InvalidColumnType(
                9,
                "availability".into(),
                rusqlite::types::Type::Text,
            )
        })?,
        is_default: row.get::<_, i64>(10)? != 0,
        sort_order: row.get::<_, i64>(11)?.max(0) as u64,
        catalog_revision: row.get(12)?,
        context_window: row
            .get::<_, Option<i64>>(13)?
            .map(|value| value.max(0) as u64),
        reasoning_efforts: serde_json::from_str(&reasoning).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                14,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?,
        service_tiers: serde_json::from_str(&tiers).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                15,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?,
    })
}

fn validate_model_selection(selection: &ModelSelection) -> Result<(), StorageError> {
    let model_id = selection
        .model_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    match selection.mode {
        ModelSelectionMode::Pinned if model_id.is_none() => {
            Err(StorageError::ModelSelectionInvalid {
                reason: "pinned selection requires a non-empty model id".into(),
            })
        }
        ModelSelectionMode::Inherit | ModelSelectionMode::ConnectorDefault
            if model_id.is_some() =>
        {
            Err(StorageError::ModelSelectionInvalid {
                reason: "inherit and connector_default selections cannot carry a model id".into(),
            })
        }
        _ if selection
            .model_id
            .as_deref()
            .is_some_and(|value| value.len() > 256) =>
        {
            Err(StorageError::ModelSelectionInvalid {
                reason: "model id exceeds 256 bytes".into(),
            })
        }
        _ => Ok(()),
    }
}

fn validate_identity_model_list_mode(
    mode: IdentityModelListMode,
    base_agent: bool,
) -> Result<(), StorageError> {
    if base_agent
        && !matches!(
            mode,
            IdentityModelListMode::Own | IdentityModelListMode::LegacyCompatibility
        )
    {
        return Err(StorageError::ModelSelectionInvalid {
            reason: "base Agent candidate list must be own or legacy_compatibility".into(),
        });
    }
    Ok(())
}

fn validate_revision(value: u64) -> Result<(), StorageError> {
    if value > i64::MAX as u64 {
        return Err(StorageError::ModelSelectionInvalid {
            reason: "candidate list revision exceeds SQLite integer range".into(),
        });
    }
    Ok(())
}

fn validate_agent_model_binding(binding: &AgentModelBinding) -> Result<(), StorageError> {
    validate_revision(binding.candidate_model_list_revision)?;
    for (name, value) in [
        ("connector_id", binding.connector_id.as_deref()),
        ("model_id", binding.model_id.as_deref()),
    ] {
        if value.is_some_and(|value| value.trim().is_empty() || value.len() > 256) {
            return Err(StorageError::ModelSelectionInvalid {
                reason: format!("{name} must be non-empty and at most 256 bytes when present"),
            });
        }
    }
    Ok(())
}

fn validate_identity_model_target(target: &IdentityModelListTarget) -> Result<(), StorageError> {
    if target.agent_id.trim().is_empty() || target.agent_id.len() > 128 {
        return Err(StorageError::ModelSelectionInvalid {
            reason: "agent id must be 1..=128 bytes".into(),
        });
    }
    let has_project = target
        .project_id
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty());
    let has_conversation = target
        .conversation_id
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty());
    let valid = match target.scope {
        IdentityModelListScope::BaseAgent => !has_project && !has_conversation,
        IdentityModelListScope::ProjectAgent => has_project && !has_conversation,
        IdentityModelListScope::ConversationAgent => !has_project && has_conversation,
    };
    if !valid {
        return Err(StorageError::ModelSelectionInvalid {
            reason: "identity model target scope and ids do not match".into(),
        });
    }
    Ok(())
}

fn validate_identity_model_option(option: &IdentityModelOption) -> Result<(), StorageError> {
    let target = IdentityModelListTarget {
        scope: option.scope,
        agent_id: option.agent_id.clone(),
        project_id: option.project_id.clone(),
        conversation_id: option.conversation_id.clone(),
    };
    validate_identity_model_target(&target)?;
    for (name, value, max) in [
        ("option id", option.id.as_str(), 128_usize),
        ("model id", option.model_id.as_str(), 256_usize),
        ("connector id", option.connector_id.as_str(), 256_usize),
        ("display name", option.display_name.as_str(), 256_usize),
    ] {
        if value.trim().is_empty() || value.len() > max {
            return Err(StorageError::ModelSelectionInvalid {
                reason: format!("{name} must be non-empty and at most {max} bytes"),
            });
        }
    }
    if option
        .catalog_revision
        .as_deref()
        .is_some_and(|value| value.len() > 256)
    {
        return Err(StorageError::ModelSelectionInvalid {
            reason: "catalog revision exceeds 256 bytes".into(),
        });
    }
    if option
        .context_window
        .is_some_and(|value| value == 0 || value > 10_000_000)
    {
        return Err(StorageError::ModelSelectionInvalid {
            reason: "context window is outside the supported bound".into(),
        });
    }
    if option.reasoning_efforts.len() > 32 || option.service_tiers.len() > 32 {
        return Err(StorageError::ModelSelectionInvalid {
            reason: "model capability list is too large".into(),
        });
    }
    Ok(())
}

fn validate_model_selection_snapshot(
    snapshot: &ModelSelectionSnapshot,
) -> Result<(), StorageError> {
    if snapshot.run_id.trim().is_empty() || snapshot.run_id.len() > 128 {
        return Err(StorageError::ModelSelectionInvalid {
            reason: "selection snapshot run id must be 1..=128 bytes".into(),
        });
    }
    if snapshot.version != 1 && snapshot.version != 2 {
        return Err(StorageError::ModelSelectionInvalid {
            reason: "selection snapshot version must be 1 or 2".into(),
        });
    }
    for (name, value) in [
        ("runtime type", snapshot.runtime_type.as_str()),
        ("provider type", snapshot.provider_type.as_str()),
        ("connector id", snapshot.connector_id.as_str()),
    ] {
        if value.trim().is_empty() || value.len() > 256 {
            return Err(StorageError::ModelSelectionInvalid {
                reason: format!("{name} is empty or too long"),
            });
        }
    }
    if snapshot
        .effective_model_id
        .as_deref()
        .is_some_and(|value| value.trim().is_empty() || value.len() > 256)
    {
        return Err(StorageError::ModelSelectionInvalid {
            reason: "effective model id is empty or too long".into(),
        });
    }
    if snapshot
        .context_window
        .is_some_and(|value| value == 0 || value > 10_000_000)
    {
        return Err(StorageError::ModelSelectionInvalid {
            reason: "selection snapshot context window is outside the supported bound".into(),
        });
    }
    if let Some(list) = &snapshot.candidate_model_list {
        validate_revision(list.revision)?;
        if list.hash.len() != 64 || list.option_count > 10000 {
            return Err(StorageError::ModelSelectionInvalid {
                reason: "candidate model list snapshot hash/count is invalid".into(),
            });
        }
    }
    Ok(())
}

fn model_selection_mode_to_storage(value: ModelSelectionMode) -> &'static str {
    match value {
        ModelSelectionMode::Inherit => "inherit",
        ModelSelectionMode::ConnectorDefault => "connector_default",
        ModelSelectionMode::Pinned => "pinned",
    }
}

fn parse_model_selection_mode(value: &str) -> Option<ModelSelectionMode> {
    match value.trim().trim_matches('"') {
        "inherit" => Some(ModelSelectionMode::Inherit),
        "connector_default" => Some(ModelSelectionMode::ConnectorDefault),
        "pinned" => Some(ModelSelectionMode::Pinned),
        _ => None,
    }
}

fn identity_model_list_mode_to_storage(value: IdentityModelListMode) -> &'static str {
    match value {
        IdentityModelListMode::Own => "own",
        IdentityModelListMode::Inherit => "inherit",
        IdentityModelListMode::Override => "override",
        IdentityModelListMode::LegacyCompatibility => "legacy_compatibility",
    }
}

fn parse_identity_model_list_mode(value: &str) -> Option<IdentityModelListMode> {
    match value.trim().trim_matches('"') {
        "own" => Some(IdentityModelListMode::Own),
        "inherit" => Some(IdentityModelListMode::Inherit),
        "override" => Some(IdentityModelListMode::Override),
        "legacy_compatibility" => Some(IdentityModelListMode::LegacyCompatibility),
        _ => None,
    }
}

fn identity_model_scope_to_storage(value: IdentityModelListScope) -> &'static str {
    match value {
        IdentityModelListScope::BaseAgent => "base_agent",
        IdentityModelListScope::ProjectAgent => "project_agent",
        IdentityModelListScope::ConversationAgent => "conversation_agent",
    }
}

fn parse_identity_model_scope(value: &str) -> Option<IdentityModelListScope> {
    match value.trim().trim_matches('"') {
        "base_agent" => Some(IdentityModelListScope::BaseAgent),
        "project_agent" => Some(IdentityModelListScope::ProjectAgent),
        "conversation_agent" => Some(IdentityModelListScope::ConversationAgent),
        _ => None,
    }
}

fn model_option_source_to_storage(value: ModelOptionSource) -> &'static str {
    match value {
        ModelOptionSource::Runtime => "runtime",
        ModelOptionSource::Config => "config",
        ModelOptionSource::Manual => "manual",
    }
}

fn parse_model_option_source(value: &str) -> Option<ModelOptionSource> {
    match value.trim().trim_matches('"') {
        "runtime" => Some(ModelOptionSource::Runtime),
        "config" => Some(ModelOptionSource::Config),
        "manual" => Some(ModelOptionSource::Manual),
        _ => None,
    }
}

fn model_availability_to_storage(value: ModelAvailability) -> &'static str {
    match value {
        ModelAvailability::Available => "available",
        ModelAvailability::Unverified => "unverified",
        ModelAvailability::Unavailable => "unavailable",
    }
}

fn parse_model_availability(value: &str) -> Option<ModelAvailability> {
    match value.trim().trim_matches('"') {
        "available" => Some(ModelAvailability::Available),
        "unverified" => Some(ModelAvailability::Unverified),
        "unavailable" => Some(ModelAvailability::Unavailable),
        _ => None,
    }
}

fn validate_execution_run_model_snapshot_binding(
    run: &ExecutionRun,
    snapshot: &ModelSnapshot,
) -> Result<(), StorageError> {
    if run.id != snapshot.run_id {
        return Err(StorageError::ModelSnapshotInvalid {
            reason: "execution run id must match model snapshot run_id".into(),
        });
    }
    Ok(())
}

fn validate_execution_run_model_selection_snapshot_binding(
    run: &ExecutionRun,
    snapshot: &ModelSelectionSnapshot,
) -> Result<(), StorageError> {
    if run.id != snapshot.run_id {
        return Err(StorageError::ModelSelectionInvalid {
            reason: "execution run id must match selection snapshot run_id".into(),
        });
    }
    Ok(())
}

fn validate_model_snapshot_selection_pair(
    snapshot: &ModelSnapshot,
    selection_snapshot: &ModelSelectionSnapshot,
) -> Result<(), StorageError> {
    if snapshot.connector_id.as_deref() != Some(selection_snapshot.connector_id.as_str())
        || snapshot.model_id != selection_snapshot.effective_model_id
    {
        return Err(StorageError::ModelSelectionInvalid {
            reason: "model snapshot must match the frozen selection connector and model".into(),
        });
    }
    Ok(())
}

fn validate_context_manifest_snapshot_route(
    run: &ExecutionRun,
    snapshot: &ModelSnapshot,
    selection_snapshot: &ModelSelectionSnapshot,
    manifest: &agenttalk_domain::ContextManifest,
) -> Result<(), StorageError> {
    if manifest.execution_run_id != run.id {
        return Err(StorageError::ModelSelectionInvalid {
            reason: "context manifest execution run id must match the frozen selection".into(),
        });
    }
    if manifest.connector_id != snapshot.connector_id
        || manifest.model_id != snapshot.model_id
        || manifest.connector_id.as_deref() != Some(selection_snapshot.connector_id.as_str())
        || manifest.model_id != selection_snapshot.effective_model_id
    {
        return Err(StorageError::ModelSelectionInvalid {
            reason: "context manifest must match the frozen connector and model".into(),
        });
    }
    Ok(())
}

fn validate_model_snapshot(snapshot: &ModelSnapshot) -> Result<(), StorageError> {
    if snapshot.run_id.trim().is_empty() || snapshot.run_id.len() > 128 {
        return Err(StorageError::ModelSnapshotInvalid {
            reason: "run_id must be 1..=128 bytes".into(),
        });
    }
    for (field, value) in [
        ("connector_id", snapshot.connector_id.as_deref()),
        ("model_id", snapshot.model_id.as_deref()),
    ] {
        if value.is_some_and(|value| value.trim().is_empty() || value.len() > 256) {
            return Err(StorageError::ModelSnapshotInvalid {
                reason: format!("{field} must be non-empty and at most 256 bytes when present"),
            });
        }
    }
    if snapshot
        .revision
        .is_some_and(|value| value == 0 || value > i64::MAX as u64)
    {
        return Err(StorageError::ModelSnapshotInvalid {
            reason: "revision must be between 1 and i64::MAX when present".into(),
        });
    }
    Ok(())
}

fn persist_execution_run_row(
    tx: &rusqlite::Transaction<'_>,
    run: &ExecutionRun,
) -> Result<(), StorageError> {
    let existing = tx
        .query_row(
            "SELECT terminal, version FROM execution_runs WHERE id = ?1",
            [&run.id],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, u64>(1)?)),
        )
        .optional()?;
    if let Some((terminal, version)) = existing {
        if terminal != 0 || run.version < version {
            return Err(StorageError::ProjectionRejected);
        }
    }
    tx.execute(
        "INSERT INTO execution_runs(id, collaboration_run_id, project_id, conversation_id, agent_id, status, version, scope_json, terminal_reason, terminal) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10) ON CONFLICT(id) DO UPDATE SET collaboration_run_id=excluded.collaboration_run_id, project_id=excluded.project_id, conversation_id=excluded.conversation_id, agent_id=excluded.agent_id, status=excluded.status, version=excluded.version, scope_json=excluded.scope_json, terminal_reason=excluded.terminal_reason, terminal=excluded.terminal WHERE execution_runs.terminal = 0 AND excluded.version > execution_runs.version",
        params![
            &run.id,
            &run.collaboration_run_id,
            &run.project_id,
            &run.conversation_id,
            &run.agent_id,
            serde_json::to_string(&run.status)?,
            run.version,
            serde_json::to_string(&run.scope)?,
            &run.terminal_reason,
            run.status.is_terminal() as i64,
        ],
    )?;
    Ok(())
}

fn persist_model_snapshot_row(
    tx: &rusqlite::Transaction<'_>,
    snapshot: &ModelSnapshot,
) -> Result<(), StorageError> {
    validate_model_snapshot(snapshot)?;
    let run_exists: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM execution_runs WHERE id = ?1)",
        [&snapshot.run_id],
        |row| row.get::<_, i64>(0),
    )? != 0;
    if !run_exists {
        return Err(StorageError::ModelSnapshotRunNotFound {
            id: snapshot.run_id.clone(),
        });
    }
    let existing = tx
        .query_row(
            "SELECT run_id, connector_id, model_id, revision FROM model_snapshots WHERE run_id = ?1",
            [&snapshot.run_id],
            map_model_snapshot,
        )
        .optional()?;
    if let Some(existing) = existing {
        return if existing == *snapshot {
            Ok(())
        } else {
            Err(StorageError::ModelSnapshotConflict {
                id: snapshot.run_id.clone(),
            })
        };
    }
    tx.execute(
        "INSERT INTO model_snapshots(run_id, connector_id, model_id, revision) VALUES(?1, ?2, ?3, ?4)",
        params![
            &snapshot.run_id,
            &snapshot.connector_id,
            &snapshot.model_id,
            snapshot.revision.map(|value| value as i64),
        ],
    )?;
    Ok(())
}

fn persist_model_selection_snapshot_row(
    tx: &rusqlite::Transaction<'_>,
    snapshot: &ModelSelectionSnapshot,
) -> Result<(), StorageError> {
    validate_model_selection_snapshot(snapshot)?;
    let run_exists: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM execution_runs WHERE id = ?1)",
        [&snapshot.run_id],
        |row| row.get::<_, i64>(0),
    )? != 0;
    if !run_exists {
        return Err(StorageError::ModelSelectionSnapshotRunNotFound {
            id: snapshot.run_id.clone(),
        });
    }
    let json = serde_json::to_string(snapshot)?;
    let hash = hex_digest(json.as_bytes());
    let existing: Option<(String, String)> = tx
        .query_row(
            "SELECT snapshot_json, snapshot_hash FROM model_selection_snapshots WHERE run_id = ?1",
            [&snapshot.run_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if let Some((existing_json, existing_hash)) = existing {
        if existing_json == json && existing_hash == hash {
            return Ok(());
        }
        return Err(StorageError::ModelSelectionSnapshotConflict {
            id: snapshot.run_id.clone(),
        });
    }
    tx.execute(
        "INSERT INTO model_selection_snapshots(run_id, snapshot_json, snapshot_hash)
         VALUES(?1, ?2, ?3)",
        params![&snapshot.run_id, json, hash],
    )?;
    Ok(())
}

fn store_context_manifest_with_ledger_row(
    tx: &rusqlite::Transaction<'_>,
    manifest: &agenttalk_domain::ContextManifest,
    bundle_hash: &str,
    source_ledger_json: &str,
) -> Result<bool, StorageError> {
    let _: serde_json::Value = serde_json::from_str(source_ledger_json).map_err(|error| {
        StorageError::Sqlite(rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
    })?;
    let existing = tx
        .query_row(
            "SELECT execution_run_id, schema_version, bundle_hash, source_ledger_json,
                    connector_id, model_id
             FROM context_manifests
             WHERE id = ?1",
            [&manifest.id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                ))
            },
        )
        .optional()?;

    if let Some((
        execution_run_id,
        schema_version,
        existing_bundle_hash,
        existing_source_ledger_json,
        existing_connector_id,
        existing_model_id,
    )) = existing
    {
        if execution_run_id == manifest.execution_run_id
            && schema_version == manifest.schema_version
            && existing_bundle_hash == bundle_hash
            && existing_source_ledger_json == source_ledger_json
            && existing_connector_id == manifest.connector_id
            && existing_model_id == manifest.model_id
        {
            return Ok(false);
        }
        return Err(StorageError::ContextManifestConflict {
            id: manifest.id.clone(),
        });
    }

    tx.execute(
        "INSERT INTO context_manifests(
            id, execution_run_id, schema_version, bundle_hash, source_ledger_json,
            connector_id, model_id
         ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            &manifest.id,
            &manifest.execution_run_id,
            &manifest.schema_version,
            bundle_hash,
            source_ledger_json,
            &manifest.connector_id,
            &manifest.model_id,
        ],
    )?;
    Ok(true)
}

fn persist_runtime_events(
    tx: &rusqlite::Transaction<'_>,
    events: &[RuntimeEvent],
) -> Result<u64, StorageError> {
    let mut last_sequence = 0;
    for event in events {
        tx.execute(
            "INSERT INTO event_store(event_id, execution_run_id, runtime_id, thread_id, turn_id, event_type, timestamp_ms, payload_json) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
            params![
                &event.event_id,
                &event.execution_run_id,
                &event.runtime_id,
                &event.thread_id,
                &event.turn_id,
                &event.event_type,
                event.timestamp_ms,
                serde_json::to_string(&event.payload)?,
            ],
        )?;
        last_sequence = tx.last_insert_rowid() as u64;
    }
    if last_sequence == 0 {
        last_sequence = tx.query_row(
            "SELECT COALESCE(MAX(sequence), 0) FROM event_store",
            [],
            |row| row.get(0),
        )?;
    }
    Ok(last_sequence)
}

fn ensure_column(
    tx: &rusqlite::Transaction<'_>,
    table: &str,
    column: &str,
    alter_sql: &str,
) -> Result<(), StorageError> {
    let exists = tx
        .prepare(&format!("PRAGMA table_info({table})"))?
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .any(|name| name == column);
    if !exists {
        tx.execute(alter_sql, [])?;
    }
    Ok(())
}

fn summaries_has_scope_foreign_key(tx: &rusqlite::Transaction<'_>) -> Result<bool, StorageError> {
    let mut statement = tx.prepare("PRAGMA foreign_key_list(summaries)")?;
    let foreign_keys = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(2)?, row.get::<_, String>(3)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(foreign_keys
        .iter()
        .any(|(table, column)| table == "conversations" && column == "scope_id"))
}

fn rebuild_summaries_without_scope_foreign_key(
    tx: &rusqlite::Transaction<'_>,
) -> Result<(), StorageError> {
    tx.execute_batch(
        "ALTER TABLE summaries RENAME TO summaries_v12_legacy;
         CREATE TABLE summaries(
           id TEXT PRIMARY KEY,
           scope_id TEXT NOT NULL,
           version INTEGER NOT NULL,
           content_hash TEXT NOT NULL,
           artifact_id TEXT
         );
         INSERT INTO summaries(id, scope_id, version, content_hash, artifact_id)
           SELECT id, scope_id, version, content_hash, artifact_id
           FROM summaries_v12_legacy;
         DROP TABLE summaries_v12_legacy;",
    )?;
    Ok(())
}

fn decode_optional_json(value: Option<String>) -> Result<Option<serde_json::Value>, StorageError> {
    value
        .map(|value| serde_json::from_str(&value))
        .transpose()
        .map_err(StorageError::from)
}

fn decode_handoff_details(
    value: Option<&str>,
) -> Result<Option<StructuredHandoffDetails>, StorageError> {
    value
        .map(serde_json::from_str::<Option<StructuredHandoffDetails>>)
        .transpose()
        .map(|details| details.flatten())
        .map_err(StorageError::from)
}

fn empty_handoff_details() -> StructuredHandoffDetails {
    StructuredHandoffDetails {
        parent_execution_run_id: None,
        child_execution_run_id: None,
        source_message_id: None,
        from_agent_id: None,
        to_agent_id: None,
        kind: None,
        dispatch_mode: None,
        batch_id: None,
        sequence_index: None,
        detected_by: None,
        task: None,
        reason: None,
        decisions: None,
        constraints: None,
        artifacts: None,
        expected_output: None,
        context_scope: None,
        agent_path: None,
    }
}

fn event_sequence_or_max(
    tx: &rusqlite::Transaction<'_>,
    execution_run_id: &str,
) -> Result<u64, StorageError> {
    Ok(tx.query_row(
        "SELECT COALESCE(MAX(sequence), 0)
         FROM event_store WHERE execution_run_id = ?1",
        [execution_run_id],
        |row| row.get(0),
    )?)
}

fn is_running_collaboration_status(value: &str) -> bool {
    serde_json::from_str::<CollaborationStatus>(value)
        .map(|status| {
            matches!(
                status,
                CollaborationStatus::Pending | CollaborationStatus::Running
            )
        })
        .unwrap_or_else(|_| {
            matches!(
                value.trim().trim_matches('"').to_ascii_lowercase().as_str(),
                "pending" | "running"
            )
        })
}

fn hydrate_missing_execution_scopes(tx: &rusqlite::Transaction<'_>) -> Result<(), StorageError> {
    let mut statement = tx.prepare(
        "SELECT id, project_id, conversation_id, agent_id FROM execution_runs WHERE scope_json IS NULL OR scope_json = '{}'",
    )?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    drop(statement);
    for (id, project_id, conversation_id, agent_id) in rows {
        let scope = serde_json::json!({
            "projectId": project_id,
            "conversationId": conversation_id,
            "agentId": agent_id,
            "workspaceAccess": "None",
            "canonicalCwd": null,
        });
        tx.execute(
            "UPDATE execution_runs SET scope_json = ?2 WHERE id = ?1",
            params![id, serde_json::to_string(&scope)?],
        )?;
    }
    Ok(())
}

fn parse_execution_status(value: &str) -> Option<ExecutionStatus> {
    if let Ok(status) = serde_json::from_str(value) {
        return Some(status);
    }
    match value.trim_matches('"').to_ascii_lowercase().as_str() {
        "pending" => Some(ExecutionStatus::Pending),
        "assembling" => Some(ExecutionStatus::Assembling),
        "awaitingapproval" | "awaiting_approval" => Some(ExecutionStatus::AwaitingApproval),
        "running" => Some(ExecutionStatus::Running),
        "verifying" => Some(ExecutionStatus::Verifying),
        "completed" => Some(ExecutionStatus::Completed),
        "failed" => Some(ExecutionStatus::Failed),
        "cancelled" | "canceled" => Some(ExecutionStatus::Cancelled),
        "interrupted" => Some(ExecutionStatus::Interrupted),
        _ => None,
    }
}

fn is_known_handoff_status(value: &str) -> bool {
    matches!(
        value,
        "proposed" | "approved" | "rejected" | "cancelled" | "dispatched" | "completed" | "failed"
    )
}

fn is_valid_handoff_transition(from: &str, to: &str) -> bool {
    matches!(
        (from, to),
        ("proposed", "approved" | "rejected" | "cancelled")
            | ("approved", "dispatched" | "cancelled")
            | ("dispatched", "completed" | "failed" | "cancelled")
    )
}

fn workspace_access_to_storage(value: &WorkspaceAccess) -> &'static str {
    match value {
        WorkspaceAccess::None => "none",
        WorkspaceAccess::ReadOnly => "read_only",
        WorkspaceAccess::WorkspaceWrite => "workspace_write",
    }
}

fn hex_digest(value: &[u8]) -> String {
    let digest = Sha256::digest(value);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn verify_artifact_blob(
    path: &Path,
    expected_sha256: &str,
    expected_size: u64,
) -> Result<(), StorageError> {
    let metadata = fs::metadata(path).map_err(|_| StorageError::ArtifactBodyIo)?;
    if !metadata.is_file()
        || metadata.len() != expected_size
        || metadata.len() > ARTIFACT_BODY_MAX_BYTES
    {
        return Err(StorageError::ArtifactBodyMismatch);
    }
    let mut file = File::open(path).map_err(|_| StorageError::ArtifactBodyIo)?;
    let mut hasher = Sha256::new();
    let mut total = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|_| StorageError::ArtifactBodyIo)?;
        if read == 0 {
            break;
        }
        total = total
            .checked_add(read as u64)
            .ok_or(StorageError::ArtifactBodyMismatch)?;
        if total > ARTIFACT_BODY_MAX_BYTES {
            return Err(StorageError::ArtifactBodyMismatch);
        }
        hasher.update(&buffer[..read]);
    }
    let digest = hasher.finalize();
    let actual_sha256: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
    if total != expected_size || !actual_sha256.eq_ignore_ascii_case(expected_sha256) {
        return Err(StorageError::ArtifactBodyMismatch);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use agenttalk_domain::{
        Artifact, Attachment, CollaborationRun, CollaborationStatus, ConnectorProfile,
        ExecutionRun, ExecutionStatus, Handoff, IdentityModelListMode, IdentityModelListScope,
        IdentityModelListTarget, IdentityModelOption, ModelAvailability, ModelOptionSource,
        ModelSelection, ModelSelectionMode, ModelSelectionSnapshot, ModelSelectionSource,
        ModelSnapshot, ScopeSnapshot, StructuredHandoffDetails, Summary, WorkflowStep,
        WorkspaceAccess, WorkspaceAuthorization,
    };
    use serde_json::json;
    use std::fs;

    #[test]
    fn sqlite_is_wal_foreign_key_enabled_and_event_replayable() {
        let path =
            std::env::temp_dir().join(format!("agenttalk-core-test-{}.db", std::process::id()));
        let mut store = SqliteStore::open(&path).unwrap();
        assert_eq!(store.migration_checksum().len(), 64);
        let event = RuntimeEvent {
            event_id: "e1".into(),
            execution_run_id: "r1".into(),
            runtime_id: "mock".into(),
            thread_id: None,
            turn_id: None,
            sequence: 0,
            event_type: "output.delta".into(),
            timestamp_ms: 1,
            payload: json!({"delta":"ok"}),
        };
        assert_eq!(store.append_event(&event).unwrap(), 1);
        assert_eq!(store.replay_after(0).unwrap().len(), 1);
        let journal: String = store
            .connection
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .unwrap();
        assert_eq!(journal.to_lowercase(), "wal");
        let foreign_keys: i64 = store
            .connection
            .query_row("PRAGMA foreign_keys", [], |row| row.get(0))
            .unwrap();
        assert_eq!(foreign_keys, 1);
        drop(store);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("db-wal"));
        let _ = std::fs::remove_file(path.with_extension("db-shm"));
    }

    #[test]
    fn message_search_uses_fts_and_respects_conversation_scope() {
        let mut store = SqliteStore::open_in_memory().unwrap();
        store
            .create_project("project-search", "Search", None)
            .unwrap();
        store
            .create_conversation("conversation-a", "project-search", "A")
            .unwrap();
        store
            .create_conversation("conversation-b", "project-search", "B")
            .unwrap();
        for (id, conversation_id, content) in [
            ("message-a", "conversation-a", "Rust event replay works"),
            ("message-b", "conversation-b", "Rust workspace search"),
        ] {
            store
                .create_message(&Message {
                    id: id.into(),
                    conversation_id: conversation_id.into(),
                    sender_id: "user".into(),
                    sequence: 1,
                    content: content.into(),
                })
                .unwrap();
        }
        let all = store.search_messages("Rust", None, 10).unwrap();
        assert_eq!(all.len(), 2);
        let scoped = store
            .search_messages("Rust", Some("conversation-a"), 10)
            .unwrap();
        assert_eq!(scoped.len(), 1);
        assert_eq!(scoped[0]["id"], "message-a");
        assert!(store.search_messages("", None, 10).unwrap().is_empty());
    }

    #[test]
    fn retrieval_preview_is_exact_scoped_metadata_only_and_file_scan_is_explicitly_bounded() {
        let mut store = SqliteStore::open_in_memory().unwrap();
        store
            .create_project("project-preview", "Preview", None)
            .unwrap();
        store
            .create_agent(
                "agent-preview",
                "Preview Agent",
                "role",
                "specialty",
                "prompt",
            )
            .unwrap();
        store
            .set_project_agent_assignment(
                "project-preview",
                "agent-preview",
                true,
                &WorkspaceAccess::ReadOnly,
            )
            .unwrap();
        store
            .create_conversation("conversation-preview", "project-preview", "Preview")
            .unwrap();
        store
            .create_message(&Message {
                id: "message-preview".into(),
                conversation_id: "conversation-preview".into(),
                sender_id: "agent-preview".into(),
                sequence: 1,
                content: "Exact retrieval phrase from a message".into(),
            })
            .unwrap();
        store
            .connection
            .execute(
                "INSERT INTO execution_runs(
                    id, collaboration_run_id, project_id, conversation_id, agent_id,
                    status, version, scope_json, terminal
                 ) VALUES('run-preview', 'collaboration-preview', 'project-preview',
                          'conversation-preview', 'agent-preview', 'Completed', 1, '{}', 1)",
                [],
            )
            .unwrap();
        store
            .append_event(&RuntimeEvent {
                event_id: "event-preview".into(),
                execution_run_id: "run-preview".into(),
                runtime_id: "mock".into(),
                thread_id: None,
                turn_id: None,
                sequence: 0,
                event_type: "execution.completed".into(),
                timestamp_ms: 1,
                payload: json!({
                    "output": "Exact retrieval phrase from an execution event",
                    "prompt": "secret-like prompt must not become a hit"
                }),
            })
            .unwrap();

        let result = store
            .preview_retrieval(&RetrievalPreviewRequest {
                expected_project_id: "project-preview".into(),
                conversation_id: "conversation-preview".into(),
                agent_id: "agent-preview".into(),
                query: "retrieval phrase".into(),
                scope: "conversation".into(),
                source_types: vec!["message".into(), "execution".into(), "project_file".into()],
                limit: 10,
            })
            .unwrap();
        assert_eq!(result["retrievalVersion"], EXACT_RETRIEVAL_VERSION);
        assert_eq!(result["capabilities"]["boundedFileScan"], false);
        assert!(result["capabilities"]["boundedFileScanUnavailableReason"]
            .as_str()
            .unwrap()
            .contains("workspace_authorization"));
        let hits = result["hits"].as_array().unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0]["matchReason"], "exact_phrase");
        assert!(hits.iter().all(|hit| {
            hit.get("sourceHash").is_some()
                && hit.get("estimatedTokens").is_some()
                && hit.get("permissionDecision").is_some()
                && hit.get("prompt").is_none()
                && hit.get("content").is_none()
        }));
        assert!(serde_json::to_string(&result)
            .unwrap()
            .contains("Exact retrieval phrase"));
        assert!(!serde_json::to_string(&result)
            .unwrap()
            .contains("secret-like prompt"));

        let vector_result = store
            .preview_retrieval_vector(&RetrievalPreviewRequest {
                expected_project_id: "project-preview".into(),
                conversation_id: "conversation-preview".into(),
                agent_id: "agent-preview".into(),
                query: "retrieval event".into(),
                scope: "conversation".into(),
                source_types: vec!["execution".into()],
                limit: 10,
            })
            .unwrap();
        assert_eq!(
            vector_result["retrievalVersion"],
            LOCAL_VECTOR_RETRIEVAL_VERSION
        );
        assert_eq!(vector_result["capabilities"]["semantic"], true);
        assert_eq!(
            vector_result["capabilities"]["embeddingProvider"],
            "local_fixture"
        );
        assert_eq!(vector_result["capabilities"]["embeddingDimension"], 32);
        assert_eq!(
            vector_result["capabilities"]["embeddingVerification"],
            "local_fixture"
        );
        assert_eq!(
            vector_result["hits"][0]["matchMethod"],
            "local_vector_fixture"
        );
        assert_eq!(
            vector_result["hits"][0]["matchReason"],
            "local_vector_similarity"
        );

        struct UnavailableEmbeddingProvider;
        impl RetrievalEmbeddingProvider for UnavailableEmbeddingProvider {
            fn descriptor(&self) -> RetrievalEmbeddingDescriptor {
                RetrievalEmbeddingDescriptor {
                    provider_id: "fixture_unavailable".into(),
                    retrieval_version: "fixture-vector-v1".into(),
                    dimension: 2,
                    verification: RetrievalEmbeddingVerification::VerifiedProvider,
                }
            }

            fn embed(&self, _text: &str) -> Result<Vec<f64>, RetrievalEmbeddingError> {
                Err(RetrievalEmbeddingError::Unavailable)
            }
        }
        assert!(matches!(
            store.preview_retrieval_vector_with_provider(
                &RetrievalPreviewRequest {
                    expected_project_id: "project-preview".into(),
                    conversation_id: "conversation-preview".into(),
                    agent_id: "agent-preview".into(),
                    query: "retrieval event".into(),
                    scope: "conversation".into(),
                    source_types: vec!["execution".into()],
                    limit: 10,
                },
                &UnavailableEmbeddingProvider,
            ),
            Err(StorageError::RetrievalPreviewInvalid { .. })
        ));

        struct FixtureEmbeddingProvider;
        impl RetrievalEmbeddingProvider for FixtureEmbeddingProvider {
            fn descriptor(&self) -> RetrievalEmbeddingDescriptor {
                RetrievalEmbeddingDescriptor {
                    provider_id: "fixture_provider".into(),
                    retrieval_version: "fixture-vector-v1".into(),
                    dimension: 2,
                    verification: RetrievalEmbeddingVerification::VerifiedProvider,
                }
            }

            fn embed(&self, text: &str) -> Result<Vec<f64>, RetrievalEmbeddingError> {
                Ok(if text.contains("event") {
                    vec![1.0, 0.0]
                } else {
                    vec![0.8, 0.2]
                })
            }
        }
        let provider_result = store
            .preview_retrieval_vector_with_provider(
                &RetrievalPreviewRequest {
                    expected_project_id: "project-preview".into(),
                    conversation_id: "conversation-preview".into(),
                    agent_id: "agent-preview".into(),
                    query: "retrieval event".into(),
                    scope: "conversation".into(),
                    source_types: vec!["execution".into()],
                    limit: 10,
                },
                &FixtureEmbeddingProvider,
            )
            .unwrap();
        assert_eq!(provider_result["retrievalVersion"], "fixture-vector-v1");
        assert_eq!(
            provider_result["capabilities"]["embeddingProvider"],
            "fixture_provider"
        );
        assert_eq!(
            provider_result["capabilities"]["embeddingVerification"],
            "verified_provider"
        );
        assert!(provider_result["capabilities"]["semanticUnavailableReason"].is_null());
        assert_eq!(provider_result["hits"][0]["matchMethod"], "provider_vector");

        let mut unauthorized = RetrievalPreviewRequest {
            agent_id: "missing-agent".into(),
            ..RetrievalPreviewRequest {
                expected_project_id: "project-preview".into(),
                conversation_id: "conversation-preview".into(),
                agent_id: "agent-preview".into(),
                query: "retrieval".into(),
                scope: "conversation".into(),
                source_types: vec!["message".into()],
                limit: 10,
            }
        };
        assert!(matches!(
            store.preview_retrieval(&unauthorized),
            Err(StorageError::RetrievalPreviewInvalid { .. })
        ));
        unauthorized.agent_id = "agent-preview".into();
        unauthorized.query = " ".into();
        assert!(matches!(
            store.preview_retrieval(&unauthorized),
            Err(StorageError::RetrievalPreviewInvalid { .. })
        ));
    }

    #[test]
    fn retrieval_preview_scans_only_authorized_text_files_within_canonical_root() {
        let root = std::env::temp_dir().join(format!(
            "agenttalk-retrieval-files-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(root.join(".git")).unwrap();
        fs::write(
            root.join("notes.txt"),
            "bounded project file retrieval phrase",
        )
        .unwrap();
        fs::write(
            root.join(".env"),
            "retrieval phrase SECRET_TOKEN=do-not-read",
        )
        .unwrap();
        fs::write(root.join(".git").join("ignored.txt"), "retrieval phrase").unwrap();

        let mut store = SqliteStore::open_in_memory().unwrap();
        let canonical_root = fs::canonicalize(&root).unwrap();
        store
            .create_project("project-file", "Project File", None)
            .unwrap();
        store
            .create_agent("agent-file", "File Agent", "role", "specialty", "prompt")
            .unwrap();
        store
            .set_project_agent_assignment(
                "project-file",
                "agent-file",
                true,
                &WorkspaceAccess::ReadOnly,
            )
            .unwrap();
        store
            .set_workspace_authorization(&WorkspaceAuthorization {
                project_id: "project-file".into(),
                canonical_root: canonical_root.to_string_lossy().into_owned(),
                revision: 1,
                validation_status: "valid".into(),
            })
            .unwrap();
        store
            .create_conversation("conversation-file", "project-file", "File")
            .unwrap();

        let result = store
            .preview_retrieval(&RetrievalPreviewRequest {
                expected_project_id: "project-file".into(),
                conversation_id: "conversation-file".into(),
                agent_id: "agent-file".into(),
                query: "retrieval phrase".into(),
                scope: "project".into(),
                source_types: vec!["project_file".into()],
                limit: 10,
            })
            .unwrap();
        assert_eq!(result["capabilities"]["boundedFileScan"], true);
        assert!(result["capabilities"]
            .get("boundedFileScanUnavailableReason")
            .is_none());
        let hits = result["hits"].as_array().unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0]["sourceType"], "project_file");
        assert_eq!(hits[0]["sourceObjectId"], "notes.txt");
        assert_eq!(hits[0]["permissionDecision"], "read_only");
        let serialized = serde_json::to_string(&result).unwrap();
        assert!(!serialized.contains("SECRET_TOKEN"));
        assert!(!serialized.contains("ignored.txt"));

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn recent_message_contents_are_ordered_bounded_and_empty_safe() {
        let mut store = SqliteStore::open_in_memory().unwrap();
        store
            .create_project("project-context", "Context", None)
            .unwrap();
        store
            .create_conversation("conversation-context", "project-context", "Context")
            .unwrap();
        for (sequence, content) in [(3, "third"), (1, "first"), (4, "fourth"), (2, "second")] {
            store
                .create_message(&Message {
                    id: format!("message-context-{sequence}"),
                    conversation_id: "conversation-context".into(),
                    sender_id: "agent".into(),
                    sequence,
                    content: content.into(),
                })
                .unwrap();
        }

        assert_eq!(
            store
                .load_recent_message_contents("conversation-context", 2)
                .unwrap(),
            vec!["third", "fourth"]
        );
        assert_eq!(
            store
                .load_recent_message_contents("conversation-context", 10)
                .unwrap(),
            vec!["first", "second", "third", "fourth"]
        );
        assert!(store
            .load_recent_message_contents("conversation-context", 0)
            .unwrap()
            .is_empty());
        assert!(store
            .load_recent_message_contents("missing-conversation", 10)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn context_manifest_write_is_idempotent_conflict_safe_and_fk_fail_closed() {
        let mut store = SqliteStore::open_in_memory().unwrap();
        store
            .connection
            .execute(
                "INSERT INTO execution_runs(
                    id, collaboration_run_id, project_id, conversation_id, agent_id,
                    status, version, scope_json, terminal
                 ) VALUES(?1, 'collaboration', 'project', 'conversation', 'agent',
                           'pending', 0, '{}', 0)",
                ["execution-context"],
            )
            .unwrap();
        let manifest = agenttalk_domain::ContextManifest {
            id: "manifest-context".into(),
            execution_run_id: "execution-context".into(),
            schema_version: "1".into(),
            source_ids: Vec::new(),
            workspace_access: WorkspaceAccess::ReadOnly,
            canonical_cwd: None,
            connector_id: None,
            model_id: None,
        };

        assert!(store.store_context_manifest(&manifest, "bundle-a").unwrap());
        assert!(!store.store_context_manifest(&manifest, "bundle-a").unwrap());
        assert!(matches!(
            store.store_context_manifest(&manifest, "bundle-b"),
            Err(StorageError::ContextManifestConflict { id }) if id == "manifest-context"
        ));
        assert!(matches!(
            store.store_context_manifest(
                &agenttalk_domain::ContextManifest {
                    id: "manifest-missing".into(),
                    execution_run_id: "missing-execution".into(),
                    ..manifest.clone()
                },
                "bundle-missing"
            ),
            Err(StorageError::Sqlite(_))
        ));
    }

    #[test]
    fn context_manifest_survives_store_reopen_and_remains_idempotent() {
        let path = std::env::temp_dir().join(format!(
            "agenttalk-context-manifest-{}-{}.sqlite3",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let manifest = agenttalk_domain::ContextManifest {
            id: "manifest-reopen".into(),
            execution_run_id: "execution-reopen".into(),
            schema_version: "1".into(),
            source_ids: vec!["source-1".into()],
            workspace_access: WorkspaceAccess::None,
            canonical_cwd: None,
            connector_id: Some("connector".into()),
            model_id: Some("model".into()),
        };

        {
            let mut store = SqliteStore::open(&path).unwrap();
            store
                .connection
                .execute(
                    "INSERT INTO execution_runs(
                        id, collaboration_run_id, project_id, conversation_id, agent_id,
                        status, version, scope_json, terminal
                     ) VALUES(?1, 'collaboration', 'project', 'conversation', 'agent',
                               'pending', 0, '{}', 0)",
                    [&manifest.execution_run_id],
                )
                .unwrap();
            assert!(store
                .store_context_manifest_with_ledger(
                    &manifest,
                    "bundle-reopen",
                    r#"[{"sourceId":"source-1","sha256":"abc","tokenCount":2,"included":true}]"#,
                )
                .unwrap());
        }

        let mut reopened = SqliteStore::open(&path).unwrap();
        assert_eq!(
            reopened.projection_snapshot().unwrap()["contextManifests"],
            json!([{
                "id": "manifest-reopen",
                "executionRunId": "execution-reopen",
                "schemaVersion": "1",
                "bundleHash": "bundle-reopen",
                "connectorId": "connector",
                "modelId": "model",
                "sourceLedger": [{
                    "sourceId": "source-1",
                    "sha256": "abc",
                    "tokenCount": 2,
                    "included": true,
                }],
            }])
        );
        assert!(!reopened
            .store_context_manifest_with_ledger(
                &manifest,
                "bundle-reopen",
                r#"[{"sourceId":"source-1","sha256":"abc","tokenCount":2,"included":true}]"#,
            )
            .unwrap());

        drop(reopened);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite3-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite3-shm"));
    }

    #[test]
    fn memory_write_is_persistent_and_idempotent_by_existing_id() {
        let mut store = SqliteStore::open_in_memory().unwrap();
        store
            .create_project("project-memory", "Memory", None)
            .unwrap();
        store
            .create_agent("agent-memory", "Agent", "role", "specialty", "system")
            .unwrap();
        let memory = MemoryItem {
            id: "memory-1".into(),
            scope_id: "project-memory".into(),
            agent_id: Some("agent-memory".into()),
            content_hash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            confirmed: true,
        };

        assert!(store.memory_scope_exists("project-memory").unwrap());
        assert!(store.agent_exists("agent-memory").unwrap());
        assert!(store.store_memory(&memory).unwrap());
        assert!(!store.store_memory(&memory).unwrap());
        assert_eq!(
            store.projection_snapshot().unwrap()["memories"][0],
            json!({
                "id": "memory-1",
                "scopeId": "project-memory",
                "agentId": "agent-memory",
                "contentHash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "confirmed": true,
            })
        );

        let mut conflicting = memory.clone();
        conflicting.confirmed = false;
        assert!(matches!(
            store.store_memory(&conflicting),
            Err(StorageError::MemoryConflict { id }) if id == "memory-1"
        ));
    }

    #[test]
    fn summary_and_artifact_metadata_are_scoped_idempotent_and_body_free() {
        let mut store = SqliteStore::open_in_memory().unwrap();
        store
            .create_project("project-summary-artifact", "Summary", None)
            .unwrap();
        store
            .create_conversation(
                "conversation-summary-artifact",
                "project-summary-artifact",
                "Summary",
            )
            .unwrap();

        let summary = Summary {
            id: "summary-1".into(),
            scope_id: "conversation-summary-artifact".into(),
            version: 1,
            content_hash: "a".repeat(64),
            artifact_id: None,
        };
        assert!(store.store_summary(&summary).unwrap());
        assert!(!store.store_summary(&summary).unwrap());
        let mut conflicting_summary = summary.clone();
        conflicting_summary.version = 2;
        assert!(matches!(
            store.store_summary(&conflicting_summary),
            Err(StorageError::SummaryConflict { id }) if id == "summary-1"
        ));

        let artifact = Artifact {
            id: "artifact-1".into(),
            sha256: "b".repeat(64),
            size: 12,
            mime: "text/plain".into(),
            relative_path: Some("notes/example.txt".into()),
        };
        assert!(store.store_artifact(&artifact).unwrap());
        assert!(!store.store_artifact(&artifact).unwrap());
        let mut conflicting_artifact = artifact.clone();
        conflicting_artifact.size = 13;
        assert!(matches!(
            store.store_artifact(&conflicting_artifact),
            Err(StorageError::ArtifactConflict { id }) if id == "artifact-1"
        ));
        assert!(matches!(
            store.store_artifact(&Artifact {
                id: "artifact-escape".into(),
                sha256: "c".repeat(64),
                size: 1,
                mime: "text/plain".into(),
                relative_path: Some("../secret.txt".into()),
            }),
            Err(StorageError::ArtifactInvalid { .. })
        ));

        let projection = store.projection_snapshot().unwrap();
        assert_eq!(projection["summaries"][0]["id"], "summary-1");
        assert_eq!(projection["artifacts"][0]["id"], "artifact-1");
        let serialized = serde_json::to_string(&projection).unwrap();
        assert!(!serialized.contains("body"));
        assert!(!serialized.contains("example contents"));
    }

    #[test]
    fn attachments_require_existing_message_and_artifact_and_are_idempotent() {
        let mut store = SqliteStore::open_in_memory().unwrap();
        store
            .create_project("project-attachment", "Attachments", None)
            .unwrap();
        store
            .create_conversation(
                "conversation-attachment",
                "project-attachment",
                "Attachments",
            )
            .unwrap();
        store
            .create_message(&Message {
                id: "message-attachment".into(),
                conversation_id: "conversation-attachment".into(),
                sender_id: "user".into(),
                sequence: 1,
                content: "with file".into(),
            })
            .unwrap();
        store
            .store_artifact(&Artifact {
                id: "artifact-attachment".into(),
                sha256: "a".repeat(64),
                size: 5,
                mime: "text/plain".into(),
                relative_path: Some("notes/file.txt".into()),
            })
            .unwrap();

        let attachment = Attachment {
            id: "attachment-1".into(),
            message_id: "message-attachment".into(),
            artifact_id: "artifact-attachment".into(),
            file_name: "file.txt".into(),
            sha256: "a".repeat(64),
            size: 5,
        };
        assert!(store.store_attachment(&attachment, 0).unwrap());
        assert!(!store.store_attachment(&attachment, 0).unwrap());

        let mut id_conflict = attachment.clone();
        id_conflict.file_name = "other.txt".into();
        assert!(matches!(
            store.store_attachment(&id_conflict, 0),
            Err(StorageError::AttachmentConflict { id }) if id == "attachment-1"
        ));

        let mut ordinal_conflict = attachment.clone();
        ordinal_conflict.id = "attachment-2".into();
        assert!(matches!(
            store.store_attachment(&ordinal_conflict, 0),
            Err(StorageError::AttachmentConflict { id }) if id == "attachment-2"
        ));

        let context_records = store
            .load_attachment_context_records("conversation-attachment", 64)
            .unwrap();
        assert_eq!(context_records.len(), 1);
        assert_eq!(
            context_records[0].attachment_id.as_deref(),
            Some("attachment-1")
        );
        assert_eq!(
            context_records[0].artifact_id.as_deref(),
            Some("artifact-attachment")
        );
        assert_eq!(context_records[0].message_sequence, 1);
        assert_eq!(context_records[0].mime.as_deref(), Some("text/plain"));

        let projection = store.projection_snapshot().unwrap();
        assert_eq!(projection["attachments"][0]["attachmentId"], "attachment-1");
        assert_eq!(
            projection["attachments"][0]["artifactId"],
            "artifact-attachment"
        );
        assert!(!serde_json::to_string(&projection)
            .unwrap()
            .contains("example contents"));
    }

    #[test]
    fn attachments_fail_closed_for_references_metadata_and_ordinals() {
        let mut store = SqliteStore::open_in_memory().unwrap();
        store
            .create_project("project-attachment-invalid", "Attachments", None)
            .unwrap();
        store
            .create_conversation(
                "conversation-attachment-invalid",
                "project-attachment-invalid",
                "Attachments",
            )
            .unwrap();
        store
            .create_message(&Message {
                id: "message-attachment-invalid".into(),
                conversation_id: "conversation-attachment-invalid".into(),
                sender_id: "user".into(),
                sequence: 1,
                content: "with file".into(),
            })
            .unwrap();
        store
            .store_artifact(&Artifact {
                id: "artifact-attachment-invalid".into(),
                sha256: "b".repeat(64),
                size: 4,
                mime: "text/plain".into(),
                relative_path: None,
            })
            .unwrap();
        let base = Attachment {
            id: "attachment-invalid".into(),
            message_id: "message-attachment-invalid".into(),
            artifact_id: "artifact-attachment-invalid".into(),
            file_name: "file.txt".into(),
            sha256: "b".repeat(64),
            size: 4,
        };

        let mut missing_message = base.clone();
        missing_message.message_id = "message-missing".into();
        assert!(matches!(
            store.store_attachment(&missing_message, 0),
            Err(StorageError::AttachmentMessageNotFound { id }) if id == "message-missing"
        ));
        let mut missing_artifact = base.clone();
        missing_artifact.artifact_id = "artifact-missing".into();
        assert!(matches!(
            store.store_attachment(&missing_artifact, 0),
            Err(StorageError::AttachmentArtifactNotFound { id }) if id == "artifact-missing"
        ));
        let mut metadata_mismatch = base.clone();
        metadata_mismatch.size = 5;
        assert!(matches!(
            store.store_attachment(&metadata_mismatch, 0),
            Err(StorageError::AttachmentArtifactMismatch { id })
                if id == "artifact-attachment-invalid"
        ));
        let mut invalid_name = base.clone();
        invalid_name.id = "attachment-invalid-name".into();
        invalid_name.file_name = "../file.txt".into();
        assert!(matches!(
            store.store_attachment(&invalid_name, 0),
            Err(StorageError::AttachmentInvalid { .. })
        ));
        let mut invalid_hash = base.clone();
        invalid_hash.id = "attachment-invalid-hash".into();
        invalid_hash.sha256 = "not-a-sha".into();
        assert!(matches!(
            store.store_attachment(&invalid_hash, 0),
            Err(StorageError::AttachmentInvalid { .. })
        ));
        assert!(matches!(
            store.store_attachment(&base, 1_000_001),
            Err(StorageError::AttachmentInvalid { .. })
        ));
    }

    #[test]
    fn artifact_body_store_is_explicit_hashed_atomic_and_recoverable() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let database = std::env::temp_dir().join(format!(
            "agenttalk-artifact-body-{}-{nonce}/core.sqlite3",
            std::process::id()
        ));
        let root = database.parent().unwrap().join("artifact-store");
        let body = b"example contents";
        let artifact = Artifact {
            id: "artifact-body".into(),
            sha256: hex_digest(body),
            size: body.len() as u64,
            mime: "text/plain".into(),
            relative_path: Some("notes/example.txt".into()),
        };
        fs::create_dir_all(database.parent().unwrap()).unwrap();

        {
            let mut store = SqliteStore::open_with_artifact_root(&database, Some(&root)).unwrap();
            assert!(store.store_artifact(&artifact).unwrap());
            assert!(store.store_artifact_body(&artifact.id, body).unwrap());
            assert!(!store.store_artifact_body(&artifact.id, body).unwrap());
            assert_eq!(store.load_artifact_body(&artifact.id).unwrap(), body);
            let projection = serde_json::to_string(&store.projection_snapshot().unwrap()).unwrap();
            assert!(!projection.contains("example contents"));
        }

        {
            let store = SqliteStore::open_with_artifact_root(&database, Some(&root)).unwrap();
            assert_eq!(store.load_artifact_body(&artifact.id).unwrap(), body);
            assert!(matches!(
                store.store_artifact_body(&artifact.id, b"wrong"),
                Err(StorageError::ArtifactBodyMismatch)
            ));
        }

        let mut in_memory = SqliteStore::open_in_memory().unwrap();
        in_memory.store_artifact(&artifact).unwrap();
        assert!(matches!(
            in_memory.store_artifact_body(&artifact.id, body),
            Err(StorageError::ArtifactBodyStoreUnavailable)
        ));

        let _ = fs::remove_dir_all(database.parent().unwrap());
    }

    #[test]
    fn artifact_body_chunk_reads_are_bounded_ordered_and_restart_safe() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let database = std::env::temp_dir().join(format!(
            "agenttalk-artifact-body-chunk-{}-{nonce}/core.sqlite3",
            std::process::id()
        ));
        let root = database.parent().unwrap().join("artifact-store");
        let body: Vec<u8> = (0..(ARTIFACT_CONTENT_CHUNK_MAX_BYTES as usize * 2 + 17))
            .map(|index| (index % 251) as u8)
            .collect();
        let artifact = Artifact {
            id: "artifact-body-chunk".into(),
            sha256: hex_digest(&body),
            size: body.len() as u64,
            mime: "application/octet-stream".into(),
            relative_path: None,
        };
        fs::create_dir_all(database.parent().unwrap()).unwrap();
        {
            let mut store = SqliteStore::open_with_artifact_root(&database, Some(&root)).unwrap();
            assert!(store.store_artifact(&artifact).unwrap());
            assert!(store.store_artifact_body(&artifact.id, &body).unwrap());
            assert!(matches!(
                store.read_artifact_body_chunk(
                    &artifact.id,
                    0,
                    ARTIFACT_CONTENT_CHUNK_MAX_BYTES + 1
                ),
                Err(StorageError::ArtifactBodyRangeInvalid)
            ));
            assert!(matches!(
                store.read_artifact_body_chunk(&artifact.id, artifact.size + 1, 1),
                Err(StorageError::ArtifactBodyRangeInvalid)
            ));

            let mut reconstructed = Vec::new();
            let mut offset = 0_u64;
            loop {
                let chunk = store
                    .read_artifact_body_chunk(
                        &artifact.id,
                        offset,
                        ARTIFACT_CONTENT_CHUNK_MAX_BYTES,
                    )
                    .unwrap();
                assert_eq!(chunk.artifact_id, artifact.id);
                assert_eq!(chunk.sha256, artifact.sha256);
                assert_eq!(chunk.offset, offset);
                assert!(chunk.bytes.len() as u64 <= ARTIFACT_CONTENT_CHUNK_MAX_BYTES);
                reconstructed.extend_from_slice(&chunk.bytes);
                offset += chunk.bytes.len() as u64;
                if chunk.eof {
                    break;
                }
            }
            assert_eq!(reconstructed, body);
        }
        {
            let store = SqliteStore::open_with_artifact_root(&database, Some(&root)).unwrap();
            let tail = store
                .read_artifact_body_chunk(&artifact.id, artifact.size - 17, 64)
                .unwrap();
            assert_eq!(tail.bytes, body[body.len() - 17..]);
            assert!(tail.eof);
        }
        let _ = fs::remove_dir_all(database.parent().unwrap());
    }

    #[test]
    fn selected_file_import_is_streamed_bounded_and_restart_recoverable() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let base = std::env::temp_dir().join(format!(
            "agenttalk-artifact-file-import-{}-{nonce}",
            std::process::id()
        ));
        let database = base.join("core.sqlite3");
        let root = base.join("artifact-store");
        let source = base.join("用户选择-large.bin");
        let body = vec![0x5a; 600 * 1024];
        fs::create_dir_all(&base).unwrap();
        fs::write(&source, &body).unwrap();

        let artifact = {
            let mut store = SqliteStore::open_with_artifact_root(&database, Some(&root)).unwrap();
            let imported = store.import_artifact_file(&source).unwrap();
            assert!(imported.body_stored);
            assert_eq!(imported.file_name, "用户选择-large.bin");
            assert_eq!(imported.size, body.len() as u64);
            assert_eq!(imported.sha256, hex_digest(&body));
            let replay = store.import_artifact_file(&source).unwrap();
            assert!(!replay.body_stored);
            assert_eq!(replay.sha256, imported.sha256);
            let artifact = Artifact {
                id: "artifact-file-import".into(),
                sha256: imported.sha256,
                size: imported.size,
                mime: "application/octet-stream".into(),
                relative_path: None,
            };
            assert!(store.store_artifact(&artifact).unwrap());
            assert_eq!(store.load_artifact_body(&artifact.id).unwrap(), body);
            artifact
        };

        fs::remove_file(&source).unwrap();
        let reopened = SqliteStore::open_with_artifact_root(&database, Some(&root)).unwrap();
        assert_eq!(reopened.load_artifact_body(&artifact.id).unwrap(), body);
        let internal_blob = root.join(format!("{}.blob", artifact.sha256));
        assert!(matches!(
            reopened.import_artifact_file(&internal_blob),
            Err(StorageError::ArtifactSourceInvalid)
        ));
        assert!(matches!(
            reopened.import_artifact_file(Path::new("relative.bin")),
            Err(StorageError::ArtifactSourceInvalid)
        ));

        let oversized = base.join("oversized.bin");
        File::create(&oversized)
            .unwrap()
            .set_len(ARTIFACT_BODY_MAX_BYTES + 1)
            .unwrap();
        assert!(matches!(
            reopened.import_artifact_file(&oversized),
            Err(StorageError::ArtifactBodyTooLarge)
        ));

        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn summary_content_is_artifact_backed_versioned_and_explicitly_readable() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let database = std::env::temp_dir().join(format!(
            "agenttalk-summary-content-{}-{nonce}/core.sqlite3",
            std::process::id()
        ));
        let root = database.parent().unwrap().join("artifacts");
        let body = b"deterministic summary body";
        fs::create_dir_all(database.parent().unwrap()).unwrap();
        let artifact = Artifact {
            id: "summary-artifact-content".into(),
            sha256: hex_digest(body),
            size: body.len() as u64,
            mime: "text/plain; charset=utf-8".into(),
            relative_path: None,
        };
        {
            let mut store = SqliteStore::open_with_artifact_root(&database, Some(&root)).unwrap();
            store
                .create_project("summary-content-project", "Summary", None)
                .unwrap();
            store
                .create_conversation(
                    "summary-content-conversation",
                    "summary-content-project",
                    "Summary",
                )
                .unwrap();
            store.store_artifact(&artifact).unwrap();
            store.store_artifact_body(&artifact.id, body).unwrap();
            let summary = Summary {
                id: "summary-content-1".into(),
                scope_id: "summary-content-conversation".into(),
                version: 1,
                content_hash: artifact.sha256.clone(),
                artifact_id: Some(artifact.id.clone()),
            };
            assert!(store.store_summary(&summary).unwrap());
            assert_eq!(store.next_summary_version(&summary.scope_id).unwrap(), 2);
            assert_eq!(
                store.load_summary_content(&summary.id).unwrap(),
                "deterministic summary body"
            );
            let projection = store.projection_snapshot().unwrap();
            assert_eq!(projection["summaries"][0]["artifactId"], artifact.id);
            assert!(!serde_json::to_string(&projection)
                .unwrap()
                .contains("deterministic summary body"));
            let mut mismatch = summary.clone();
            mismatch.id = "summary-content-mismatch".into();
            mismatch.content_hash = "f".repeat(64);
            assert!(matches!(
                store.store_summary(&mismatch),
                Err(StorageError::SummaryArtifactMismatch { id })
                    if id == "summary-artifact-content"
            ));
        }
        let reopened = SqliteStore::open_with_artifact_root(&database, Some(&root)).unwrap();
        assert_eq!(
            reopened.load_summary_content("summary-content-1").unwrap(),
            "deterministic summary body"
        );
        let _ = fs::remove_dir_all(database.parent().unwrap());
    }

    #[test]
    fn retrieval_source_write_validates_scope_is_idempotent_projected_and_persistent() {
        let path = std::env::temp_dir().join(format!(
            "agenttalk-retrieval-source-{}-{}.sqlite3",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let source = RetrievalSource {
            id: "retrieval-1".into(),
            scope_id: "conversation-retrieval".into(),
            citation: "docs/guide.md#intro".into(),
            sha256: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into(),
            token_count: 42,
        };

        {
            let mut store = SqliteStore::open(&path).unwrap();
            assert!(matches!(
                store.store_retrieval_source(&RetrievalSource {
                    scope_id: "missing-scope".into(),
                    ..source.clone()
                }),
                Err(StorageError::RetrievalScopeNotFound { id })
                    if id == "missing-scope"
            ));

            store
                .create_project("project-retrieval", "Retrieval", None)
                .unwrap();
            store
                .create_conversation("conversation-retrieval", "project-retrieval", "Retrieval")
                .unwrap();
            assert!(store.store_retrieval_source(&source).unwrap());
            assert!(!store.store_retrieval_source(&source).unwrap());
            assert_eq!(
                store
                    .query_retrieval_sources("conversation-retrieval", None, 10)
                    .unwrap(),
                vec![json!({
                    "id": "retrieval-1",
                    "scopeId": "conversation-retrieval",
                    "citation": "docs/guide.md#intro",
                    "sha256": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    "tokenCount": 42,
                })]
            );
            let selected = vec!["retrieval-1".to_owned(), "missing-source".to_owned()];
            assert_eq!(
                store
                    .query_retrieval_sources("conversation-retrieval", Some(&selected), 10)
                    .unwrap()
                    .len(),
                1
            );
            assert_eq!(
                store.projection_snapshot().unwrap()["retrievalSources"][0],
                json!({
                    "id": "retrieval-1",
                    "scopeId": "conversation-retrieval",
                    "citation": "docs/guide.md#intro",
                    "sha256": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    "tokenCount": 42,
                })
            );

            let mut conflicting = source.clone();
            conflicting.token_count = 43;
            assert!(matches!(
                store.store_retrieval_source(&conflicting),
                Err(StorageError::RetrievalConflict { id }) if id == "retrieval-1"
            ));
        }

        let mut reopened = SqliteStore::open(&path).unwrap();
        assert_eq!(
            reopened.projection_snapshot().unwrap()["retrievalSources"][0],
            json!({
                "id": "retrieval-1",
                "scopeId": "conversation-retrieval",
                "citation": "docs/guide.md#intro",
                "sha256": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "tokenCount": 42,
            })
        );
        assert!(!reopened.store_retrieval_source(&source).unwrap());
        drop(reopened);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite3-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite3-shm"));
    }

    #[test]
    fn retrieval_selection_and_feedback_are_exact_scoped_and_metadata_only() {
        use agenttalk_domain::{
            RetrievalFeedback, RetrievalFeedbackLabel, RetrievalFeedbackReason,
            RetrievalMatchMethod, RetrievalSelection, RetrievalSelectionItem,
            RetrievalSelectionReason, RetrievalSelectionScope,
        };

        let mut store = SqliteStore::open_in_memory().unwrap();
        store
            .create_project("project-selection", "Selection", None)
            .unwrap();
        store
            .create_conversation("conversation-selection", "project-selection", "Selection")
            .unwrap();
        let source = RetrievalSource {
            id: "source-selection".into(),
            scope_id: "conversation-selection".into(),
            citation: "docs/retrieval.md#exact".into(),
            sha256: "c".repeat(64),
            token_count: 12,
        };
        store.store_retrieval_source(&source).unwrap();
        let selection = RetrievalSelection {
            id: "selection-1".into(),
            scope: RetrievalSelectionScope::Conversation,
            scope_id: "conversation-selection".into(),
            project_id: "project-selection".into(),
            conversation_id: Some("conversation-selection".into()),
            scope_revision: 0,
            workspace_revision: Some(3),
            retrieval_version: "exact-retrieval-v1".into(),
            query_hash: "d".repeat(64),
            items: vec![RetrievalSelectionItem {
                source_id: source.id.clone(),
                source_hash: source.sha256.clone(),
                rank: 1,
                score_milli: 1000,
                match_method: RetrievalMatchMethod::ExactPhrase,
                reason: RetrievalSelectionReason::ExactPhrase,
                range: Some(agenttalk_domain::RetrievalLineRange {
                    start: Some(4),
                    end: Some(5),
                }),
            }],
        };

        assert!(store.store_retrieval_selection(&selection).unwrap());
        assert!(!store.store_retrieval_selection(&selection).unwrap());
        let selections = store
            .query_retrieval_selections("conversation-selection", None, 10)
            .unwrap();
        assert_eq!(selections.len(), 1);
        assert_eq!(selections[0]["items"][0]["sourceId"], "source-selection");
        assert!(selections[0].get("query").is_none());
        assert!(selections[0].get("prompt").is_none());
        assert!(selections[0].get("content").is_none());
        assert!(store
            .query_retrieval_selections("missing-scope", None, 10)
            .is_err());

        let feedback = RetrievalFeedback {
            id: "feedback-1".into(),
            selection_id: selection.id.clone(),
            scope_id: selection.scope_id.clone(),
            source_id: source.id.clone(),
            label: RetrievalFeedbackLabel::Helpful,
            reason: RetrievalFeedbackReason::ExactMatch,
            created_at_ms: 100,
        };
        assert!(store.store_retrieval_feedback(&feedback).unwrap());
        assert!(!store.store_retrieval_feedback(&feedback).unwrap());
        assert_eq!(
            store
                .query_retrieval_feedback("conversation-selection", Some("selection-1"), 10)
                .unwrap(),
            vec![json!({
                "id": "feedback-1",
                "selectionId": "selection-1",
                "scopeId": "conversation-selection",
                "sourceId": "source-selection",
                "label": "helpful",
                "reason": "exact_match",
                "createdAtMs": 100,
            })]
        );

        let mut wrong_scope = selection.clone();
        wrong_scope.id = "selection-wrong-scope".into();
        wrong_scope.scope_id = "project-selection".into();
        wrong_scope.scope = RetrievalSelectionScope::Project;
        wrong_scope.conversation_id = None;
        assert!(matches!(
            store.store_retrieval_selection(&wrong_scope),
            Err(StorageError::RetrievalSelectionSourceOutOfScope { id })
                if id == "source-selection"
        ));
        assert!(matches!(
            store.store_retrieval_feedback(&RetrievalFeedback {
                id: "feedback-unselected".into(),
                source_id: "missing-source".into(),
                ..feedback
            }),
            Err(StorageError::RetrievalFeedbackSourceNotSelected { id })
                if id == "missing-source"
        ));
    }

    #[test]
    fn collaboration_and_handoff_projection_is_empty_after_v9_open() {
        let store = SqliteStore::open_in_memory().unwrap();
        assert_eq!(
            store.projection_snapshot().unwrap()["collaborationRuns"],
            json!([])
        );
        assert_eq!(store.projection_snapshot().unwrap()["handoffs"], json!([]));
        let version: i64 = store
            .connection
            .query_row(
                "SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
    }

    #[test]
    fn schema_v7_upgrade_adds_collaboration_and_handoff_tables() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "agenttalk-storage-v7-collaboration-upgrade-{}-{nonce}.sqlite3",
            std::process::id()
        ));
        {
            let connection = Connection::open(&path).unwrap();
            connection
                .execute_batch(
                    "PRAGMA foreign_keys = ON;
                     CREATE TABLE schema_migrations(
                       version INTEGER PRIMARY KEY,
                       checksum TEXT NOT NULL,
                       applied_at INTEGER NOT NULL
                     );
                     INSERT INTO schema_migrations(version, checksum, applied_at)
                       VALUES(7, 'legacy-v7-checksum', 1);
                     CREATE TABLE projects(
                       id TEXT PRIMARY KEY,
                       name TEXT NOT NULL,
                       root_path TEXT,
                       archived INTEGER NOT NULL DEFAULT 0
                     );
                     INSERT INTO projects(id, name, root_path, archived)
                       VALUES('project-preserved', 'Preserved', NULL, 0);",
                )
                .unwrap();
        }

        let store = SqliteStore::open(&path).unwrap();
        let versions: Vec<i64> = store
            .connection
            .prepare("SELECT version FROM schema_migrations ORDER BY version")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(versions, vec![7, 11, 12, 13, 14, 15, 16]);
        let checksums: Vec<(i64, String)> = store
            .connection
            .prepare(
                "SELECT version, checksum FROM schema_migrations
                 WHERE version IN (?1, ?2, ?3, ?4, ?5, ?6) ORDER BY version",
            )
            .unwrap()
            .query_map(
                params![
                    V11_SCHEMA_VERSION,
                    V12_SCHEMA_VERSION,
                    V13_SCHEMA_VERSION,
                    V14_SCHEMA_VERSION,
                    V15_SCHEMA_VERSION,
                    SCHEMA_VERSION
                ],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            checksums,
            vec![
                (V11_SCHEMA_VERSION, HISTORICAL_V11_MIGRATION_CHECKSUM.into()),
                (V12_SCHEMA_VERSION, hex_digest(MIGRATION_V12_SQL.as_bytes())),
                (V13_SCHEMA_VERSION, hex_digest(MIGRATION_V13_SQL.as_bytes())),
                (V14_SCHEMA_VERSION, hex_digest(MIGRATION_V14_SQL.as_bytes())),
                (V15_SCHEMA_VERSION, hex_digest(MIGRATION_V15_SQL.as_bytes())),
                (SCHEMA_VERSION, store.migration_checksum()),
            ]
        );
        assert_eq!(
            store
                .connection
                .query_row(
                    "SELECT name FROM projects WHERE id = 'project-preserved'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "Preserved"
        );
        for (table, expected_columns) in [
            (
                "collaboration_runs",
                vec![
                    "id",
                    "project_id",
                    "root_agent_ids_json",
                    "call_count",
                    "max_calls",
                    "depth",
                    "max_depth",
                    "status",
                    "stop_reason",
                    "auto_dispatch_handoffs",
                ],
            ),
            (
                "handoffs",
                vec![
                    "id",
                    "collaboration_run_id",
                    "from_execution_run_id",
                    "to_agent_id",
                    "status",
                    "details_json",
                ],
            ),
        ] {
            let actual_columns = store
                .connection
                .prepare(&format!("PRAGMA table_info({table})"))
                .unwrap()
                .query_map([], |row| row.get::<_, String>(1))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(actual_columns, expected_columns);
        }

        drop(store);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite3-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite3-shm"));
    }

    #[test]
    fn v11_checksum_is_immutable_and_v12_v13_v14_are_recorded_separately() {
        let path = std::env::temp_dir().join(format!(
            "agenttalk-storage-v11-immutability-{}.sqlite3",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        {
            let connection = Connection::open(&path).unwrap();
            connection
                .execute_batch("PRAGMA foreign_keys = ON;")
                .unwrap();
            connection.execute_batch(MIGRATION_V11_SQL).unwrap();
            connection
                .execute(
                    "INSERT INTO schema_migrations(version, checksum, applied_at)
                     VALUES(?1, ?2, 1)",
                    params![V11_SCHEMA_VERSION, HISTORICAL_V11_MIGRATION_CHECKSUM],
                )
                .unwrap();
        }

        let mut store = SqliteStore::open(&path).unwrap();
        let checksums: Vec<(i64, String)> = store
            .connection
            .prepare("SELECT version, checksum FROM schema_migrations ORDER BY version")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            checksums,
            vec![
                (V11_SCHEMA_VERSION, HISTORICAL_V11_MIGRATION_CHECKSUM.into()),
                (V12_SCHEMA_VERSION, hex_digest(MIGRATION_V12_SQL.as_bytes())),
                (V13_SCHEMA_VERSION, hex_digest(MIGRATION_V13_SQL.as_bytes())),
                (V14_SCHEMA_VERSION, hex_digest(MIGRATION_V14_SQL.as_bytes())),
                (V15_SCHEMA_VERSION, hex_digest(MIGRATION_V15_SQL.as_bytes())),
                (SCHEMA_VERSION, store.migration_checksum()),
            ]
        );
        let transaction = store.connection.transaction().unwrap();
        assert!(!summaries_has_scope_foreign_key(&transaction).unwrap());
        transaction.rollback().unwrap();
        drop(store);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite3-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite3-shm"));
    }

    #[test]
    fn mutated_v11_checksum_is_accepted_without_rewriting_history() {
        let path = std::env::temp_dir().join(format!(
            "agenttalk-storage-v11-mutated-{}.sqlite3",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        {
            let connection = Connection::open(&path).unwrap();
            connection.execute_batch(MIGRATION_V11_SQL).unwrap();
            connection
                .execute(
                    "INSERT INTO schema_migrations(version, checksum, applied_at)
                     VALUES(?1, ?2, 1)",
                    params![V11_SCHEMA_VERSION, MUTATED_V11_MIGRATION_CHECKSUM],
                )
                .unwrap();
        }
        let store = SqliteStore::open(&path).unwrap();
        let checksums: Vec<(i64, String)> = store
            .connection
            .prepare(
                "SELECT version, checksum FROM schema_migrations
                 WHERE version IN (?1, ?2, ?3, ?4, ?5, ?6) ORDER BY version",
            )
            .unwrap()
            .query_map(
                params![
                    V11_SCHEMA_VERSION,
                    V12_SCHEMA_VERSION,
                    V13_SCHEMA_VERSION,
                    V14_SCHEMA_VERSION,
                    V15_SCHEMA_VERSION,
                    SCHEMA_VERSION
                ],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            checksums,
            vec![
                (V11_SCHEMA_VERSION, MUTATED_V11_MIGRATION_CHECKSUM.into()),
                (V12_SCHEMA_VERSION, hex_digest(MIGRATION_V12_SQL.as_bytes())),
                (V13_SCHEMA_VERSION, hex_digest(MIGRATION_V13_SQL.as_bytes())),
                (V14_SCHEMA_VERSION, hex_digest(MIGRATION_V14_SQL.as_bytes())),
                (V15_SCHEMA_VERSION, hex_digest(MIGRATION_V15_SQL.as_bytes())),
                (SCHEMA_VERSION, store.migration_checksum()),
            ]
        );
        drop(store);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite3-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite3-shm"));
    }

    #[test]
    fn unknown_v11_checksum_fails_before_v12_changes() {
        let path = std::env::temp_dir().join(format!(
            "agenttalk-storage-v11-unknown-{}.sqlite3",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        {
            let connection = Connection::open(&path).unwrap();
            connection
                .execute_batch(
                    "CREATE TABLE schema_migrations(
                       version INTEGER PRIMARY KEY,
                       checksum TEXT NOT NULL,
                       applied_at INTEGER NOT NULL
                     );
                     INSERT INTO schema_migrations(version, checksum, applied_at)
                       VALUES(11, 'unknown-v11-checksum', 1);",
                )
                .unwrap();
        }
        assert!(matches!(
            SqliteStore::open(&path),
            Err(StorageError::MigrationChecksumMismatch { version: 11 })
        ));
        let connection = Connection::open(&path).unwrap();
        assert_eq!(
            connection
                .query_row(
                    "SELECT checksum FROM schema_migrations WHERE version = 11",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "unknown-v11-checksum"
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM pragma_table_info('schema_migrations') WHERE name = 'dirty'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite3-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite3-shm"));
    }

    #[test]
    fn collaboration_and_handoff_writes_validate_roster_fks_are_idempotent_and_reopen() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "agenttalk-storage-collaboration-{}-{nonce}.sqlite3",
            std::process::id()
        ));
        let collaboration = CollaborationRun {
            id: "collaboration-1".into(),
            root_agent_ids: vec!["agent-root".into()],
            call_count: 1,
            max_calls: 8,
            depth: 2,
            max_depth: 5,
            status: CollaborationStatus::Running,
            stop_reason: None,
            auto_dispatch_handoffs: true,
        };
        let handoff = Handoff {
            id: "handoff-1".into(),
            collaboration_run_id: collaboration.id.clone(),
            from_execution_run_id: "execution-1".into(),
            to_agent_id: "agent-target".into(),
            status: "proposed".into(),
            details: Some(StructuredHandoffDetails {
                parent_execution_run_id: Some("execution-1".into()),
                child_execution_run_id: None,
                source_message_id: Some("message-1".into()),
                from_agent_id: Some("agent-root".into()),
                to_agent_id: Some("agent-target".into()),
                kind: Some("review".into()),
                dispatch_mode: Some("manual".into()),
                batch_id: Some("batch-1".into()),
                sequence_index: Some(0),
                detected_by: Some("parser".into()),
                task: Some("review changes".into()),
                reason: Some("handoff requested".into()),
                decisions: Some(vec!["preserve scope".into()]),
                constraints: Some(vec!["no child run".into()]),
                artifacts: Some(vec!["diff".into()]),
                expected_output: Some("review result".into()),
                context_scope: Some("conversation".into()),
                agent_path: None,
            }),
        };

        {
            let mut store = SqliteStore::open(&path).unwrap();
            assert!(matches!(
                store.create_collaboration_run("missing-project", &collaboration),
                Err(StorageError::CollaborationProjectNotFound { id })
                    if id == "missing-project"
            ));

            store
                .create_project("project-collaboration", "Collaboration", None)
                .unwrap();
            for agent_id in ["agent-root", "agent-target", "agent-disabled"] {
                store
                    .create_agent(agent_id, agent_id, "role", "specialty", "system")
                    .unwrap();
            }
            store
                .set_project_agent_assignment(
                    "project-collaboration",
                    "agent-root",
                    true,
                    &WorkspaceAccess::None,
                )
                .unwrap();
            store
                .set_project_agent_assignment(
                    "project-collaboration",
                    "agent-target",
                    true,
                    &WorkspaceAccess::None,
                )
                .unwrap();
            store
                .set_project_agent_assignment(
                    "project-collaboration",
                    "agent-disabled",
                    false,
                    &WorkspaceAccess::None,
                )
                .unwrap();

            assert!(matches!(
                store.create_collaboration_run(
                    "project-collaboration",
                    &CollaborationRun {
                        id: "collaboration-roster".into(),
                        root_agent_ids: vec!["agent-disabled".into()],
                        ..collaboration.clone()
                    }
                ),
                Err(StorageError::CollaborationAgentNotInProject { agent_id, .. })
                    if agent_id == "agent-disabled"
            ));
            assert!(store
                .create_collaboration_run("project-collaboration", &collaboration)
                .is_ok());
            assert!(!store
                .create_collaboration_run("project-collaboration", &collaboration)
                .unwrap());

            let mut conflicting_collaboration = collaboration.clone();
            conflicting_collaboration.call_count = 2;
            assert!(matches!(
                store.create_collaboration_run(
                    "project-collaboration",
                    &conflicting_collaboration
                ),
                Err(StorageError::CollaborationConflict { id }) if id == "collaboration-1"
            ));

            let execution = ExecutionRun {
                id: "execution-1".into(),
                collaboration_run_id: collaboration.id.clone(),
                project_id: "project-collaboration".into(),
                conversation_id: "conversation-1".into(),
                agent_id: "agent-root".into(),
                status: ExecutionStatus::Pending,
                version: 0,
                scope: ScopeSnapshot {
                    project_id: "project-collaboration".into(),
                    conversation_id: "conversation-1".into(),
                    agent_id: "agent-root".into(),
                    workspace_access: WorkspaceAccess::None,
                    canonical_cwd: None,
                },
                terminal_reason: None,
            };
            store.upsert_execution_run(&execution).unwrap();

            assert!(matches!(
                store.create_handoff(&Handoff {
                    collaboration_run_id: "missing-collaboration".into(),
                    ..handoff.clone()
                }),
                Err(StorageError::HandoffCollaborationNotFound { id })
                    if id == "missing-collaboration"
            ));
            assert!(matches!(
                store.create_handoff(&Handoff {
                    from_execution_run_id: "missing-execution".into(),
                    ..handoff.clone()
                }),
                Err(StorageError::HandoffExecutionNotFound { id })
                    if id == "missing-execution"
            ));
            assert!(matches!(
                store.create_handoff(&Handoff {
                    id: "handoff-roster".into(),
                    to_agent_id: "agent-disabled".into(),
                    ..handoff.clone()
                }),
                Err(StorageError::HandoffAgentNotInProject { agent_id, .. })
                    if agent_id == "agent-disabled"
            ));

            assert!(store.create_handoff(&handoff).unwrap());
            assert!(!store.create_handoff(&handoff).unwrap());
            assert!(!store.transition_handoff("handoff-1", "proposed").unwrap());
            assert!(store.transition_handoff("handoff-1", "approved").unwrap());
            assert!(store.transition_handoff("handoff-1", "dispatched").unwrap());
            assert!(store.transition_handoff("handoff-1", "completed").unwrap());
            assert!(!store.transition_handoff("handoff-1", "completed").unwrap());
            assert!(matches!(
                store.transition_handoff("handoff-1", "failed"),
                Err(StorageError::HandoffInvalidTransition {
                    id,
                    from_status,
                    target_status,
                }) if id == "handoff-1"
                    && from_status == "completed"
                    && target_status == "failed"
            ));
            assert!(matches!(
                store.transition_handoff("handoff-1", "unknown"),
                Err(StorageError::HandoffInvalidTransition { target_status, .. })
                    if target_status == "unknown"
            ));
            let mut conflicting_handoff = handoff.clone();
            conflicting_handoff.status = "proposed".into();
            assert!(matches!(
                store.create_handoff(&conflicting_handoff),
                Err(StorageError::HandoffConflict { id }) if id == "handoff-1"
            ));

            assert_eq!(
                store.projection_snapshot().unwrap()["collaborationRuns"][0]["projectId"],
                "project-collaboration"
            );
            assert_eq!(
                store.projection_snapshot().unwrap()["handoffs"][0]["toAgentId"],
                "agent-target"
            );

            assert!(store
                .connection
                .execute(
                    "INSERT INTO collaboration_runs(
                        id, project_id, root_agent_ids_json, call_count, max_calls,
                        depth, max_depth, status, stop_reason
                     ) VALUES('collaboration-fk-project', 'missing-project', '[]', 0, 1, 0, 1, 'pending', NULL)",
                    [],
                )
                .is_err());
            assert!(store
                .connection
                .execute(
                    "INSERT INTO handoffs(
                        id, collaboration_run_id, from_execution_run_id, to_agent_id, status
                     ) VALUES('handoff-fk-collaboration', 'missing-collaboration', 'execution-1', 'agent-target', 'pending')",
                    [],
                )
                .is_err());
            assert!(store
                .connection
                .execute(
                    "INSERT INTO handoffs(
                        id, collaboration_run_id, from_execution_run_id, to_agent_id, status
                     ) VALUES('handoff-fk-execution', 'collaboration-1', 'missing-execution', 'agent-target', 'pending')",
                    [],
                )
                .is_err());
        }

        let reopened = SqliteStore::open(&path).unwrap();
        let projection = reopened.projection_snapshot().unwrap();
        assert_eq!(projection["collaborationRuns"].as_array().unwrap().len(), 1);
        assert_eq!(projection["handoffs"].as_array().unwrap().len(), 1);
        assert_eq!(
            projection["collaborationRuns"][0]["rootAgentIdsJson"],
            serde_json::to_string(&collaboration.root_agent_ids).unwrap()
        );
        assert_eq!(
            projection["collaborationRuns"][0]["callCount"],
            collaboration.call_count
        );
        assert_eq!(
            projection["collaborationRuns"][0]["autoDispatchHandoffs"],
            true
        );
        assert_eq!(
            projection["handoffs"][0]["collaborationRunId"],
            collaboration.id
        );
        assert_eq!(projection["handoffs"][0]["status"], "completed");
        assert_eq!(
            projection["handoffs"][0]["details"],
            serde_json::to_value(handoff.details.clone()).unwrap()
        );
        let details_json: Option<String> = reopened
            .connection
            .query_row(
                "SELECT details_json FROM handoffs WHERE id = 'handoff-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            serde_json::from_str::<StructuredHandoffDetails>(details_json.as_deref().unwrap())
                .unwrap(),
            handoff.details.clone().unwrap()
        );

        drop(reopened);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite3-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite3-shm"));
    }

    #[test]
    fn handoff_transition_allowlist_covers_all_branches_and_unknown_states() {
        let mut store = SqliteStore::open_in_memory().unwrap();
        store
            .create_project("project-transition", "Transition", None)
            .unwrap();
        store
            .create_agent("agent-transition", "Agent", "role", "specialty", "system")
            .unwrap();
        store
            .set_project_agent_assignment(
                "project-transition",
                "agent-transition",
                true,
                &WorkspaceAccess::None,
            )
            .unwrap();
        store
            .connection
            .execute(
                "INSERT INTO collaboration_runs(
                    id, project_id, root_agent_ids_json, call_count, max_calls,
                    depth, max_depth, status, stop_reason
                 ) VALUES('collaboration-transition', 'project-transition', '[\"agent-transition\"]', 0, 8, 0, 5, 'pending', NULL)",
                [],
            )
            .unwrap();
        store
            .connection
            .execute(
                "INSERT INTO execution_runs(
                    id, collaboration_run_id, project_id, conversation_id, agent_id,
                    status, version, scope_json, terminal_reason, terminal
                 ) VALUES('execution-transition', 'collaboration-transition', 'project-transition', 'conversation-transition', 'agent-transition', 'Pending', 0, '{}', NULL, 0)",
                [],
            )
            .unwrap();

        for (id, status) in [
            ("handoff-transition-path", "proposed"),
            ("handoff-transition-rejected", "proposed"),
            ("handoff-transition-cancelled", "proposed"),
            ("handoff-approved-cancelled", "approved"),
            ("handoff-dispatched-completed", "dispatched"),
            ("handoff-dispatched-cancelled", "dispatched"),
            ("handoff-unknown", "mystery"),
        ] {
            store
                .connection
                .execute(
                    "INSERT INTO handoffs(
                        id, collaboration_run_id, from_execution_run_id, to_agent_id, status
                     ) VALUES(?1, 'collaboration-transition', 'execution-transition', 'agent-transition', ?2)",
                    params![id, status],
                )
                .unwrap();
        }

        assert!(store
            .transition_handoff("handoff-transition-path", "approved")
            .unwrap());
        assert!(store
            .transition_handoff("handoff-transition-path", "dispatched")
            .unwrap());
        assert!(store
            .transition_handoff("handoff-transition-path", "failed")
            .unwrap());
        assert!(!store
            .transition_handoff("handoff-transition-path", "failed")
            .unwrap());
        assert!(matches!(
            store.transition_handoff("handoff-transition-path", "cancelled"),
            Err(StorageError::HandoffInvalidTransition { .. })
        ));

        assert!(store
            .transition_handoff("handoff-transition-rejected", "rejected")
            .unwrap());
        assert!(!store
            .transition_handoff("handoff-transition-rejected", "rejected")
            .unwrap());
        assert!(matches!(
            store.transition_handoff("handoff-transition-rejected", "approved"),
            Err(StorageError::HandoffInvalidTransition { .. })
        ));
        assert!(store
            .transition_handoff("handoff-transition-cancelled", "cancelled")
            .unwrap());
        assert!(store
            .transition_handoff("handoff-approved-cancelled", "cancelled")
            .unwrap());
        assert!(store
            .transition_handoff("handoff-dispatched-completed", "completed")
            .unwrap());
        assert!(store
            .transition_handoff("handoff-dispatched-cancelled", "cancelled")
            .unwrap());
        assert!(matches!(
            store.transition_handoff("handoff-unknown", "mystery"),
            Err(StorageError::HandoffInvalidTransition { .. })
        ));
        assert!(matches!(
            store.transition_handoff("missing-handoff", "approved"),
            Err(StorageError::HandoffNotFound { id }) if id == "missing-handoff"
        ));
    }

    #[test]
    fn workflow_write_is_project_scoped_roster_checked_idempotent_and_projected() {
        let mut store = SqliteStore::open_in_memory().unwrap();
        let workflow = WorkflowTemplate {
            id: "workflow-1".into(),
            name: "Build and review".into(),
            kind: "linear".into(),
            steps: vec![WorkflowStep {
                id: "step-1".into(),
                order: 1,
                agent_id: "agent-workflow".into(),
                prompt_supplement: Some("review the change".into()),
            }],
        };

        assert!(matches!(
            store.create_workflow("missing-project", &workflow),
            Err(StorageError::ProjectNotFound { id }) if id == "missing-project"
        ));

        store
            .create_project("project-workflow", "Workflow", None)
            .unwrap();
        store
            .create_agent("agent-workflow", "Agent", "role", "specialty", "system")
            .unwrap();
        assert!(matches!(
            store.create_workflow("project-workflow", &workflow),
            Err(StorageError::WorkflowAgentNotInProject { agent_id, .. })
                if agent_id == "agent-workflow"
        ));
        store
            .set_project_agent_assignment(
                "project-workflow",
                "agent-workflow",
                true,
                &WorkspaceAccess::None,
            )
            .unwrap();

        assert!(store
            .create_workflow("project-workflow", &workflow)
            .unwrap());
        assert!(!store
            .create_workflow("project-workflow", &workflow)
            .unwrap());

        let mut conflicting = workflow.clone();
        conflicting.name = "Changed name".into();
        assert!(matches!(
            store.create_workflow("project-workflow", &conflicting),
            Err(StorageError::WorkflowConflict { id }) if id == "workflow-1"
        ));

        assert_eq!(
            store.projection_snapshot().unwrap()["workflows"][0],
            json!({
                "id": "workflow-1",
                "projectId": "project-workflow",
                "name": "Build and review",
                "kind": "linear",
                "stepsJson": serde_json::to_string(&workflow.steps).unwrap(),
            })
        );
    }

    #[test]
    fn conversation_assignment_persists_without_system_prompt_data() {
        let mut store = SqliteStore::open_in_memory().unwrap();
        store
            .create_project("project-assignment", "Project", None)
            .unwrap();
        store
            .create_agent("agent-assignment", "Agent", "role", "specialty", "system")
            .unwrap();
        store
            .create_conversation(
                "conversation-assignment",
                "project-assignment",
                "Conversation",
            )
            .unwrap();
        store
            .set_project_agent_assignment(
                "project-assignment",
                "agent-assignment",
                true,
                &WorkspaceAccess::ReadOnly,
            )
            .unwrap();
        store
            .set_conversation_agent_assignment("conversation-assignment", "agent-assignment", true)
            .unwrap();

        assert_eq!(
            store.load_conversation_agent_assignments().unwrap(),
            vec![(
                "conversation-assignment".into(),
                "agent-assignment".into(),
                true,
            )]
        );
        let prompt: Option<String> = store
            .connection
            .query_row(
                "SELECT system_prompt FROM conversation_agents WHERE conversation_id = ?1 AND agent_id = ?2",
                ["conversation-assignment", "agent-assignment"],
                |row| row.get(0),
            )
            .unwrap();
        assert!(prompt.is_none());
        let projection = store.projection_snapshot().unwrap();
        let assignment = &projection["conversationAgents"][0];
        assert_eq!(assignment["enabled"], true);
        assert!(assignment.get("systemPrompt").is_none());
    }

    #[test]
    fn execution_projection_round_trips_and_rejects_stale_terminal_overwrite() {
        let mut store = SqliteStore::open_in_memory().unwrap();
        let mut run = ExecutionRun {
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
        };
        store.upsert_execution_run(&run).unwrap();
        assert!(matches!(
            store.upsert_execution_run(&run),
            Err(StorageError::ProjectionRejected)
        ));
        run.transition(ExecutionStatus::Assembling, 0, None)
            .unwrap();
        run.transition(ExecutionStatus::Running, 1, None).unwrap();
        run.transition(ExecutionStatus::Verifying, 2, None).unwrap();
        run.transition(ExecutionStatus::Completed, 3, Some("done".into()))
            .unwrap();
        store.upsert_execution_run(&run).unwrap();
        assert_eq!(
            store.load_execution_run("run-1").unwrap().unwrap().status,
            ExecutionStatus::Completed
        );
        let mut stale = run.clone();
        stale.status = ExecutionStatus::Pending;
        stale.version = 0;
        assert!(matches!(
            store.upsert_execution_run(&stale),
            Err(StorageError::ProjectionRejected)
        ));
    }

    #[test]
    fn model_snapshot_is_idempotent_immutable_and_atomic_with_run_creation() {
        let mut store = SqliteStore::open_in_memory().unwrap();
        let run = ExecutionRun {
            id: "model-snapshot-run".into(),
            collaboration_run_id: "model-snapshot-collaboration".into(),
            project_id: "model-snapshot-project".into(),
            conversation_id: "model-snapshot-conversation".into(),
            agent_id: "model-snapshot-agent".into(),
            status: ExecutionStatus::Pending,
            version: 0,
            scope: ScopeSnapshot {
                project_id: "model-snapshot-project".into(),
                conversation_id: "model-snapshot-conversation".into(),
                agent_id: "model-snapshot-agent".into(),
                workspace_access: WorkspaceAccess::None,
                canonical_cwd: None,
            },
            terminal_reason: None,
        };
        let snapshot = ModelSnapshot {
            run_id: run.id.clone(),
            connector_id: Some("mock".into()),
            model_id: Some("mock-default".into()),
            revision: Some(7),
        };
        store
            .persist_execution_run_and_model_snapshot_and_events(&run, &snapshot, &[])
            .unwrap();
        store.upsert_model_snapshot(&snapshot).unwrap();
        assert_eq!(
            store.load_model_snapshot(&run.id).unwrap(),
            Some(snapshot.clone())
        );
        assert_eq!(
            store.load_model_snapshots().unwrap(),
            vec![snapshot.clone()]
        );
        assert_eq!(
            store.projection_snapshot().unwrap()["modelSnapshots"][0],
            serde_json::json!({
                "runId": run.id,
                "connectorId": "mock",
                "modelId": "mock-default",
                "revision": 7,
            })
        );

        let conflict = ModelSnapshot {
            model_id: Some("different-model".into()),
            ..snapshot.clone()
        };
        assert!(matches!(
            store.upsert_model_snapshot(&conflict),
            Err(StorageError::ModelSnapshotConflict { .. })
        ));

        let invalid_run = ExecutionRun {
            id: "model-snapshot-atomic-invalid".into(),
            ..run
        };
        let invalid_snapshot = ModelSnapshot {
            run_id: invalid_run.id.clone(),
            connector_id: Some(String::new()),
            model_id: None,
            revision: Some(1),
        };
        assert!(matches!(
            store.persist_execution_run_and_model_snapshot_and_events(
                &invalid_run,
                &invalid_snapshot,
                &[]
            ),
            Err(StorageError::ModelSnapshotInvalid { .. })
        ));
        assert!(store.load_execution_run(&invalid_run.id).unwrap().is_none());
        assert!(store
            .load_model_snapshot(&invalid_run.id)
            .unwrap()
            .is_none());
    }

    #[test]
    fn atomic_run_snapshot_apis_reject_cross_run_bindings_before_writes() {
        let mut store = SqliteStore::open_in_memory().unwrap();
        let snapshot_run = ExecutionRun {
            id: "model-snapshot-existing-run".into(),
            collaboration_run_id: "model-snapshot-collaboration".into(),
            project_id: "model-snapshot-project".into(),
            conversation_id: "model-snapshot-conversation".into(),
            agent_id: "model-snapshot-agent".into(),
            status: ExecutionStatus::Pending,
            version: 0,
            scope: ScopeSnapshot {
                project_id: "model-snapshot-project".into(),
                conversation_id: "model-snapshot-conversation".into(),
                agent_id: "model-snapshot-agent".into(),
                workspace_access: WorkspaceAccess::None,
                canonical_cwd: None,
            },
            terminal_reason: None,
        };
        let snapshot = ModelSnapshot {
            run_id: snapshot_run.id.clone(),
            connector_id: Some("mock".into()),
            model_id: Some("mock-default".into()),
            revision: Some(11),
        };
        store
            .persist_execution_run_and_model_snapshot_and_events(&snapshot_run, &snapshot, &[])
            .unwrap();

        let mismatched_run = ExecutionRun {
            id: "model-snapshot-mismatched-run".into(),
            ..snapshot_run.clone()
        };
        assert!(matches!(
            store.persist_execution_run_and_model_snapshot_and_events(
                &mismatched_run,
                &snapshot,
                &[]
            ),
            Err(StorageError::ModelSnapshotInvalid { .. })
        ));
        assert!(store
            .load_execution_run(&mismatched_run.id)
            .unwrap()
            .is_none());

        let receipt_run = ExecutionRun {
            id: "model-snapshot-receipt-mismatched-run".into(),
            ..snapshot_run
        };
        let receipt = CommandReceipt {
            key: CommandReceiptKey {
                scope_id: "model-snapshot-scope".into(),
                client_id: "model-snapshot-client".into(),
                request_id: "model-snapshot-request".into(),
            },
            command: "execution.start".into(),
            payload_hash: "model-snapshot-payload".into(),
            operation_key: "execution.start:model-snapshot-receipt-mismatched-run".into(),
            state: CommandReceiptState::InProgress,
            result_json: None,
            error_json: None,
            created_at: 1,
            updated_at: 1,
        };
        assert!(matches!(
            store.persist_command_receipt_and_execution_run_and_model_snapshot_and_events(
                &receipt,
                &receipt_run,
                &snapshot,
                &[]
            ),
            Err(StorageError::ModelSnapshotInvalid { .. })
        ));
        assert!(store.load_execution_run(&receipt_run.id).unwrap().is_none());
        assert!(store.load_command_receipt(&receipt.key).unwrap().is_none());
        assert_eq!(
            store.load_model_snapshots().unwrap(),
            vec![snapshot.clone()]
        );
    }

    #[test]
    fn migration_clone_schema_is_opened_with_fail_closed_scope_and_status_compatibility() {
        let path = std::env::temp_dir().join(format!(
            "agenttalk-migration-clone-{}.sqlite3",
            std::process::id()
        ));
        {
            let connection = Connection::open(&path).unwrap();
            connection
                .execute_batch(
                    "
                    PRAGMA foreign_keys = ON;
                    CREATE TABLE projects(id TEXT PRIMARY KEY, name TEXT NOT NULL, root_path TEXT, archived INTEGER NOT NULL);
                    CREATE TABLE conversations(id TEXT PRIMARY KEY, project_id TEXT NOT NULL, title TEXT NOT NULL, scope_revision INTEGER NOT NULL);
                    CREATE TABLE agents(id TEXT PRIMARY KEY, name TEXT NOT NULL, role TEXT NOT NULL, specialty TEXT NOT NULL, system_prompt TEXT NOT NULL);
                    CREATE TABLE collaboration_runs(id TEXT PRIMARY KEY, status TEXT NOT NULL, call_count INTEGER NOT NULL, max_calls INTEGER NOT NULL, max_depth INTEGER NOT NULL);
                    CREATE TABLE execution_runs(id TEXT PRIMARY KEY, collaboration_run_id TEXT NOT NULL, project_id TEXT NOT NULL, conversation_id TEXT NOT NULL, agent_id TEXT NOT NULL, status TEXT NOT NULL, version INTEGER NOT NULL, legacy INTEGER NOT NULL);
                    INSERT INTO projects VALUES('p1', 'Project', NULL, 0);
                    INSERT INTO conversations VALUES('c1', 'p1', 'Conversation', 0);
                    INSERT INTO agents VALUES('a1', 'Agent', 'role', 'specialty', 'prompt');
                    INSERT INTO collaboration_runs VALUES('collab1', 'completed', 0, 8, 5);
                    INSERT INTO execution_runs VALUES('r1', 'collab1', 'p1', 'c1', 'a1', 'completed', 0, 0);
                    ",
                )
                .unwrap();
        }
        let store = SqliteStore::open(&path).unwrap();
        let run = store.load_execution_run("r1").unwrap().unwrap();
        assert_eq!(run.status, ExecutionStatus::Completed);
        assert_eq!(run.scope.workspace_access, WorkspaceAccess::None);
        assert_eq!(run.scope.project_id, "p1");
        assert_eq!(store.replay_after(0).unwrap(), Vec::<RuntimeEvent>::new());
        drop(store);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite3-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite3-shm"));
    }

    #[test]
    fn execution_projection_and_event_append_roll_back_together() {
        let mut store = SqliteStore::open_in_memory().unwrap();
        let mut run = ExecutionRun {
            id: "run-atomic".into(),
            collaboration_run_id: "collab-atomic".into(),
            project_id: "project-atomic".into(),
            conversation_id: "conversation-atomic".into(),
            agent_id: "agent-atomic".into(),
            status: ExecutionStatus::Pending,
            version: 0,
            scope: ScopeSnapshot {
                project_id: "project-atomic".into(),
                conversation_id: "conversation-atomic".into(),
                agent_id: "agent-atomic".into(),
                workspace_access: WorkspaceAccess::None,
                canonical_cwd: None,
            },
            terminal_reason: None,
        };
        let event = RuntimeEvent {
            event_id: "event-atomic".into(),
            execution_run_id: run.id.clone(),
            runtime_id: "core".into(),
            thread_id: None,
            turn_id: None,
            sequence: 0,
            event_type: "execution.created".into(),
            timestamp_ms: 0,
            payload: json!({}),
        };
        store
            .persist_execution_run_and_events(&run, std::slice::from_ref(&event))
            .unwrap();
        run.transition(ExecutionStatus::Assembling, 0, None)
            .unwrap();
        assert!(matches!(
            store.persist_execution_run_and_events(&run, std::slice::from_ref(&event)),
            Err(StorageError::Sqlite(_))
        ));
        assert_eq!(
            store
                .load_execution_run("run-atomic")
                .unwrap()
                .unwrap()
                .status,
            ExecutionStatus::Pending
        );
        assert_eq!(store.replay_after(0).unwrap().len(), 1);
    }

    #[test]
    fn execution_event_append_allows_same_version_projection() {
        let mut store = SqliteStore::open_in_memory().unwrap();
        let run = ExecutionRun {
            id: "run-stream".into(),
            collaboration_run_id: "collab-stream".into(),
            project_id: "project-stream".into(),
            conversation_id: "conversation-stream".into(),
            agent_id: "agent-stream".into(),
            status: ExecutionStatus::Pending,
            version: 0,
            scope: ScopeSnapshot {
                project_id: "project-stream".into(),
                conversation_id: "conversation-stream".into(),
                agent_id: "agent-stream".into(),
                workspace_access: WorkspaceAccess::None,
                canonical_cwd: None,
            },
            terminal_reason: None,
        };
        let event = |id: &str, event_type: &str| RuntimeEvent {
            event_id: id.into(),
            execution_run_id: run.id.clone(),
            runtime_id: "mock".into(),
            thread_id: None,
            turn_id: None,
            sequence: 0,
            event_type: event_type.into(),
            timestamp_ms: 0,
            payload: json!({}),
        };
        store
            .persist_execution_run_and_events(
                &run,
                std::slice::from_ref(&event("created", "execution.created")),
            )
            .unwrap();
        store
            .persist_execution_run_and_events(
                &run,
                std::slice::from_ref(&event("delta", "output.delta")),
            )
            .unwrap();
        assert_eq!(store.replay_after(0).unwrap().len(), 2);
        assert_eq!(store.replay_after_limited(0, 1).unwrap().len(), 1);
        assert_eq!(
            store
                .load_execution_run("run-stream")
                .unwrap()
                .unwrap()
                .status,
            ExecutionStatus::Pending
        );
    }

    #[test]
    fn command_receipt_round_trip_uses_composite_primary_key() {
        let mut store = SqliteStore::open_in_memory().unwrap();
        let key = CommandReceiptKey {
            scope_id: "scope-test".into(),
            client_id: "client-test".into(),
            request_id: "request-test".into(),
        };
        let receipt = CommandReceipt {
            key: key.clone(),
            command: "execution.start".into(),
            payload_hash: "hash-a".into(),
            operation_key: "operation-a".into(),
            state: CommandReceiptState::InProgress,
            result_json: None,
            error_json: None,
            created_at: 100,
            updated_at: 100,
        };

        store.upsert_command_receipt(&receipt).unwrap();
        assert_eq!(store.load_command_receipt(&key).unwrap(), Some(receipt));

        let completed = CommandReceipt {
            key: key.clone(),
            command: "execution.start".into(),
            payload_hash: "hash-a".into(),
            operation_key: "operation-a".into(),
            state: CommandReceiptState::Completed,
            result_json: Some(json!({"runId": "run-test"})),
            error_json: None,
            created_at: 100,
            updated_at: 200,
        };
        store.upsert_command_receipt(&completed).unwrap();
        assert_eq!(store.load_command_receipt(&key).unwrap(), Some(completed));
    }

    #[test]
    fn initial_command_receipt_run_and_event_commit_as_one_transaction() {
        let mut store = SqliteStore::open_in_memory().unwrap();
        let run = ExecutionRun {
            id: "run-receipt-atomic".into(),
            collaboration_run_id: "collab-receipt-atomic".into(),
            project_id: "project-receipt-atomic".into(),
            conversation_id: "conversation-receipt-atomic".into(),
            agent_id: "agent-receipt-atomic".into(),
            status: ExecutionStatus::Pending,
            version: 0,
            scope: ScopeSnapshot {
                project_id: "project-receipt-atomic".into(),
                conversation_id: "conversation-receipt-atomic".into(),
                agent_id: "agent-receipt-atomic".into(),
                workspace_access: WorkspaceAccess::None,
                canonical_cwd: None,
            },
            terminal_reason: None,
        };
        let event = RuntimeEvent {
            event_id: "event-receipt-atomic".into(),
            execution_run_id: run.id.clone(),
            runtime_id: "core".into(),
            thread_id: None,
            turn_id: None,
            sequence: 0,
            event_type: "execution.created".into(),
            timestamp_ms: 0,
            payload: json!({}),
        };
        let receipt = CommandReceipt {
            key: CommandReceiptKey {
                scope_id: "desktop-command-v1".into(),
                client_id: "client-receipt-atomic".into(),
                request_id: "request-receipt-atomic".into(),
            },
            command: "execution.start".into(),
            payload_hash: "hash-receipt-atomic".into(),
            operation_key: "execution.start:run-receipt-atomic".into(),
            state: CommandReceiptState::InProgress,
            result_json: None,
            error_json: None,
            created_at: 1,
            updated_at: 1,
        };
        store
            .persist_command_receipt_and_execution_run_and_events(
                &receipt,
                &run,
                std::slice::from_ref(&event),
            )
            .unwrap();
        assert_eq!(store.load_execution_run(&run.id).unwrap(), Some(run));
        assert_eq!(
            store.load_command_receipt(&receipt.key).unwrap(),
            Some(receipt)
        );
        assert_eq!(store.replay_after(0).unwrap()[0].event_id, event.event_id);
    }

    #[test]
    fn schema_v5_upgrade_adds_receipts_and_preserves_foreign_keys() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "agenttalk-storage-v5-upgrade-{}-{nonce}.sqlite3",
            std::process::id()
        ));
        {
            let connection = Connection::open(&path).unwrap();
            connection
                .execute_batch(
                    "PRAGMA foreign_keys = ON;
                     CREATE TABLE schema_migrations(
                       version INTEGER PRIMARY KEY,
                       checksum TEXT NOT NULL,
                       applied_at INTEGER NOT NULL
                     );
                     INSERT INTO schema_migrations(version, checksum, applied_at)
                       VALUES(5, 'legacy-v5-checksum', 1);",
                )
                .unwrap();
        }

        let store = SqliteStore::open(&path).unwrap();
        let receipt_columns = store
            .connection
            .prepare("PRAGMA table_info(command_receipts)")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            receipt_columns,
            vec![
                "scope_id",
                "client_id",
                "request_id",
                "command",
                "payload_hash",
                "operation_key",
                "state",
                "result_json",
                "error_json",
                "created_at",
                "updated_at",
            ]
        );
        let version: i64 = store
            .connection
            .query_row(
                "SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
        let foreign_keys: i64 = store
            .connection
            .query_row("PRAGMA foreign_keys", [], |row| row.get(0))
            .unwrap();
        assert_eq!(foreign_keys, 1);
        assert!(store
            .connection
            .execute(
                "INSERT INTO messages(id, conversation_id, sender_id, sequence, content)
                 VALUES('message-orphan', 'missing-conversation', 'sender', 1, 'content')",
                [],
            )
            .is_err());

        drop(store);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite3-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite3-shm"));
    }

    fn dispatch_fixture(
        max_calls: u32,
        handoff_status: &str,
    ) -> (
        SqliteStore,
        ExecutionRun,
        ExecutionRun,
        ModelSnapshot,
        RuntimeEvent,
    ) {
        let mut store = SqliteStore::open_in_memory().unwrap();
        store
            .create_project("project-dispatch", "Dispatch", None)
            .unwrap();
        for agent_id in ["agent-source", "agent-target"] {
            store
                .create_agent(agent_id, agent_id, "role", "specialty", "system")
                .unwrap();
            store
                .set_project_agent_assignment(
                    "project-dispatch",
                    agent_id,
                    true,
                    &WorkspaceAccess::None,
                )
                .unwrap();
        }
        store
            .create_conversation("conversation-dispatch", "project-dispatch", "Dispatch")
            .unwrap();
        let collaboration = CollaborationRun {
            id: "collaboration-dispatch".into(),
            root_agent_ids: vec!["agent-source".into()],
            call_count: 0,
            max_calls,
            depth: 0,
            max_depth: 4,
            status: CollaborationStatus::Running,
            stop_reason: None,
            auto_dispatch_handoffs: true,
        };
        store
            .create_collaboration_run("project-dispatch", &collaboration)
            .unwrap();

        let source = ExecutionRun {
            id: "execution-source".into(),
            collaboration_run_id: collaboration.id.clone(),
            project_id: "project-dispatch".into(),
            conversation_id: "conversation-dispatch".into(),
            agent_id: "agent-source".into(),
            status: ExecutionStatus::Running,
            version: 1,
            scope: ScopeSnapshot {
                project_id: "project-dispatch".into(),
                conversation_id: "conversation-dispatch".into(),
                agent_id: "agent-source".into(),
                workspace_access: WorkspaceAccess::None,
                canonical_cwd: None,
            },
            terminal_reason: None,
        };
        store.upsert_execution_run(&source).unwrap();

        let mut details = empty_handoff_details();
        details.task = Some("preserve this task".into());
        details.reason = Some("user supplied reason".into());
        let handoff = Handoff {
            id: "handoff-dispatch".into(),
            collaboration_run_id: collaboration.id,
            from_execution_run_id: source.id.clone(),
            to_agent_id: "agent-target".into(),
            status: handoff_status.into(),
            details: Some(details),
        };
        store.create_handoff(&handoff).unwrap();

        let child = ExecutionRun {
            id: "execution-child".into(),
            collaboration_run_id: handoff.collaboration_run_id.clone(),
            project_id: source.project_id.clone(),
            conversation_id: source.conversation_id.clone(),
            agent_id: handoff.to_agent_id.clone(),
            status: ExecutionStatus::Pending,
            version: 0,
            scope: ScopeSnapshot {
                project_id: source.project_id.clone(),
                conversation_id: source.conversation_id.clone(),
                agent_id: handoff.to_agent_id,
                workspace_access: WorkspaceAccess::None,
                canonical_cwd: None,
            },
            terminal_reason: None,
        };
        let child_snapshot = ModelSnapshot {
            run_id: child.id.clone(),
            connector_id: Some("mock".into()),
            model_id: Some("mock-default".into()),
            revision: Some(1),
        };
        let event = RuntimeEvent {
            event_id: "event-child-created".into(),
            execution_run_id: child.id.clone(),
            runtime_id: "test-runtime".into(),
            thread_id: None,
            turn_id: None,
            sequence: 0,
            event_type: "execution.created".into(),
            timestamp_ms: 1,
            payload: json!({"handoffId": "handoff-dispatch"}),
        };
        (store, source, child, child_snapshot, event)
    }

    fn assert_dispatch_rollback(store: &SqliteStore) {
        assert!(store
            .load_execution_run("execution-child")
            .unwrap()
            .is_none());
        assert!(store
            .load_model_snapshot("execution-child")
            .unwrap()
            .is_none());
        assert!(store.replay_after(0).unwrap().is_empty());
        assert_eq!(
            store
                .connection
                .query_row(
                    "SELECT call_count FROM collaboration_runs WHERE id = 'collaboration-dispatch'",
                    [],
                    |row| row.get::<_, u32>(0),
                )
                .unwrap(),
            0
        );
        assert_eq!(
            store
                .load_handoff("handoff-dispatch")
                .unwrap()
                .unwrap()
                .status,
            "approved"
        );
    }

    #[test]
    fn handoff_dispatch_persists_child_snapshot_event_and_budget_atomically() {
        let (mut store, source, child, child_snapshot, event) = dispatch_fixture(2, "approved");
        let child_selection_snapshot = ModelSelectionSnapshot {
            run_id: child.id.clone(),
            version: 1,
            runtime_type: "mock".into(),
            provider_type: "mock".into(),
            connector_id: "mock".into(),
            effective_model_id: Some("mock-default".into()),
            selection_source: ModelSelectionSource::ConnectorDefault,
            selection_mode: ModelSelectionMode::ConnectorDefault,
            availability: ModelAvailability::Available,
            catalog_revision: Some("mock-r1".into()),
            context_window: None,
            reasoning_efforts: Vec::new(),
            service_tiers: Vec::new(),
            candidate_model_list: None,
        };
        let manifest = agenttalk_domain::ContextManifest {
            id: "manifest-child".into(),
            execution_run_id: child.id.clone(),
            schema_version: "context-v2".into(),
            source_ids: Vec::new(),
            workspace_access: WorkspaceAccess::None,
            canonical_cwd: None,
            connector_id: child_snapshot.connector_id.clone(),
            model_id: child_snapshot.model_id.clone(),
        };
        let scope_event = RuntimeEvent {
            event_id: "event-child-scope".into(),
            event_type: "scope.frozen".into(),
            payload: json!({"connectorId":"mock","modelId":"mock-default"}),
            ..event.clone()
        };
        let assembled_event = RuntimeEvent {
            event_id: "event-child-context-assembled".into(),
            event_type: "context.assembled".into(),
            payload: json!({"manifestId":"manifest-child"}),
            ..event.clone()
        };
        let sealed_event = RuntimeEvent {
            event_id: "event-child-context-sealed".into(),
            event_type: "context.sealed".into(),
            payload: json!({"manifestId":"manifest-child","bundleHash":"bundle-child"}),
            ..event.clone()
        };
        let initial_events = vec![event.clone(), scope_event, assembled_event, sealed_event];
        let before = store.load_handoff("handoff-dispatch").unwrap().unwrap();
        assert_eq!(
            before
                .details
                .as_ref()
                .and_then(|details| details.task.as_deref()),
            Some("preserve this task")
        );

        assert_eq!(
            store
                .dispatch_handoff_and_persist_child_with_selection_context_and_events(
                    "handoff-dispatch",
                    &child,
                    &child_snapshot,
                    &child_selection_snapshot,
                    &manifest,
                    "bundle-child",
                    "[]",
                    &initial_events,
                )
                .unwrap(),
            (true, 4)
        );
        assert_eq!(
            store.load_execution_run(&child.id).unwrap(),
            Some(child.clone())
        );
        assert_eq!(
            store.load_model_snapshot(&child.id).unwrap(),
            Some(child_snapshot.clone())
        );
        assert_eq!(
            store.load_model_selection_snapshot(&child.id).unwrap(),
            Some(child_selection_snapshot)
        );
        let projection = store.projection_snapshot().unwrap();
        assert!(projection["runs"]
            .as_array()
            .unwrap()
            .iter()
            .any(|run| run["id"] == child.id));
        assert_eq!(
            projection["modelSnapshots"][0],
            json!({
                "runId": "execution-child",
                "connectorId": "mock",
                "modelId": "mock-default",
                "revision": 1,
            })
        );
        assert_eq!(
            store
                .replay_after(0)
                .unwrap()
                .iter()
                .map(|event| event.event_type.as_str())
                .collect::<Vec<_>>(),
            vec![
                "execution.created",
                "scope.frozen",
                "context.assembled",
                "context.sealed",
            ]
        );
        let persisted_manifest = projection["contextManifests"]
            .as_array()
            .unwrap()
            .iter()
            .find(|value| value["id"] == "manifest-child")
            .expect("handoff child Context Manifest must commit with its Run");
        assert_eq!(persisted_manifest["executionRunId"], child.id.as_str());
        assert_eq!(persisted_manifest["connectorId"], "mock");
        assert_eq!(persisted_manifest["modelId"], "mock-default");
        let dispatched = store.load_handoff("handoff-dispatch").unwrap().unwrap();
        assert_eq!(dispatched.status, "dispatched");
        let details = dispatched.details.unwrap();
        assert_eq!(details.task.as_deref(), Some("preserve this task"));
        assert_eq!(
            details.parent_execution_run_id.as_deref(),
            Some(source.id.as_str())
        );
        assert_eq!(
            details.child_execution_run_id.as_deref(),
            Some(child.id.as_str())
        );
        assert_eq!(details.from_agent_id.as_deref(), Some("agent-source"));
        assert_eq!(details.to_agent_id.as_deref(), Some("agent-target"));
        assert_eq!(
            store
                .connection
                .query_row(
                    "SELECT call_count FROM collaboration_runs WHERE id = 'collaboration-dispatch'",
                    [],
                    |row| row.get::<_, u32>(0),
                )
                .unwrap(),
            1
        );
    }

    #[test]
    fn handoff_context_boundary_rolls_back_when_an_initial_event_conflicts() {
        let (mut store, _, child, child_snapshot, event) = dispatch_fixture(2, "approved");
        let child_selection_snapshot = ModelSelectionSnapshot {
            run_id: child.id.clone(),
            version: 1,
            runtime_type: "mock".into(),
            provider_type: "mock".into(),
            connector_id: "mock".into(),
            effective_model_id: Some("mock-default".into()),
            selection_source: ModelSelectionSource::ConnectorDefault,
            selection_mode: ModelSelectionMode::ConnectorDefault,
            availability: ModelAvailability::Available,
            catalog_revision: Some("mock-r1".into()),
            context_window: None,
            reasoning_efforts: Vec::new(),
            service_tiers: Vec::new(),
            candidate_model_list: None,
        };
        let manifest = agenttalk_domain::ContextManifest {
            id: "manifest-child-conflict".into(),
            execution_run_id: child.id.clone(),
            schema_version: "context-v2".into(),
            source_ids: Vec::new(),
            workspace_access: WorkspaceAccess::None,
            canonical_cwd: None,
            connector_id: child_snapshot.connector_id.clone(),
            model_id: child_snapshot.model_id.clone(),
        };
        store
            .append_event(&RuntimeEvent {
                execution_run_id: "seed-run".into(),
                ..event.clone()
            })
            .unwrap();

        assert!(matches!(
            store.dispatch_handoff_and_persist_child_with_selection_context_and_events(
                "handoff-dispatch",
                &child,
                &child_snapshot,
                &child_selection_snapshot,
                &manifest,
                "bundle-child-conflict",
                "[]",
                &[event],
            ),
            Err(StorageError::Sqlite(_))
        ));
        assert!(store.load_execution_run(&child.id).unwrap().is_none());
        assert!(store.load_model_snapshot(&child.id).unwrap().is_none());
        assert!(store
            .load_model_selection_snapshot(&child.id)
            .unwrap()
            .is_none());
        assert_eq!(
            store
                .connection
                .query_row(
                    "SELECT COUNT(*) FROM context_manifests WHERE id = ?1",
                    [&manifest.id],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
        assert_eq!(store.replay_after(0).unwrap().len(), 1);
        assert_eq!(
            store
                .connection
                .query_row(
                    "SELECT call_count FROM collaboration_runs WHERE id = 'collaboration-dispatch'",
                    [],
                    |row| row.get::<_, u32>(0),
                )
                .unwrap(),
            0
        );
        assert_eq!(
            store
                .load_handoff("handoff-dispatch")
                .unwrap()
                .unwrap()
                .status,
            "approved"
        );
    }

    #[test]
    fn repeated_handoff_dispatch_is_idempotent_without_duplicate_child_event_or_budget() {
        let (mut store, _, child, child_snapshot, event) = dispatch_fixture(2, "approved");
        let first = store
            .dispatch_handoff_and_persist_child("handoff-dispatch", &child, &child_snapshot, &event)
            .unwrap();
        let second = store
            .dispatch_handoff_and_persist_child("handoff-dispatch", &child, &child_snapshot, &event)
            .unwrap();
        assert_eq!(first, (true, 1));
        assert_eq!(second, (false, 1));
        assert_eq!(store.load_execution_runs().unwrap().len(), 2);
        assert_eq!(store.load_model_snapshots().unwrap(), vec![child_snapshot]);
        assert_eq!(store.replay_after(0).unwrap().len(), 1);
        assert_eq!(
            store
                .connection
                .query_row(
                    "SELECT call_count FROM collaboration_runs WHERE id = 'collaboration-dispatch'",
                    [],
                    |row| row.get::<_, u32>(0),
                )
                .unwrap(),
            1
        );
    }

    #[test]
    fn max_calls_and_non_approved_handoff_dispatch_roll_back_every_write() {
        let (mut maxed_store, _, child, child_snapshot, event) = dispatch_fixture(0, "approved");
        assert!(matches!(
            maxed_store.dispatch_handoff_and_persist_child(
                "handoff-dispatch",
                &child,
                &child_snapshot,
                &event,
            ),
            Err(StorageError::HandoffDispatchRejected { .. })
        ));
        assert_dispatch_rollback(&maxed_store);

        let (mut proposed_store, _, child, child_snapshot, event) = dispatch_fixture(2, "proposed");
        assert!(matches!(
            proposed_store.dispatch_handoff_and_persist_child(
                "handoff-dispatch",
                &child,
                &child_snapshot,
                &event,
            ),
            Err(StorageError::HandoffDispatchRejected { .. })
        ));
        assert_eq!(
            proposed_store
                .load_handoff("handoff-dispatch")
                .unwrap()
                .unwrap()
                .status,
            "proposed"
        );
        assert!(proposed_store
            .load_execution_run(&child.id)
            .unwrap()
            .is_none());
        assert!(proposed_store
            .load_model_snapshot(&child.id)
            .unwrap()
            .is_none());
        assert!(proposed_store.replay_after(0).unwrap().is_empty());
    }

    #[test]
    fn handoff_dispatch_rejects_scope_and_roster_mismatches_without_writes() {
        let (mut scope_store, _, mut child, child_snapshot, event) =
            dispatch_fixture(2, "approved");
        child.conversation_id = "conversation-other".into();
        child.scope.conversation_id = "conversation-other".into();
        assert!(matches!(
            scope_store.dispatch_handoff_and_persist_child(
                "handoff-dispatch",
                &child,
                &child_snapshot,
                &event,
            ),
            Err(StorageError::HandoffDispatchRejected { .. })
        ));
        assert_dispatch_rollback(&scope_store);

        let (mut roster_store, _, child, child_snapshot, event) = dispatch_fixture(2, "approved");
        roster_store
            .set_project_agent_assignment(
                "project-dispatch",
                "agent-target",
                false,
                &WorkspaceAccess::None,
            )
            .unwrap();
        assert!(matches!(
            roster_store.dispatch_handoff_and_persist_child(
                "handoff-dispatch",
                &child,
                &child_snapshot,
                &event,
            ),
            Err(StorageError::HandoffDispatchRejected { .. })
        ));
        assert_dispatch_rollback(&roster_store);
    }

    #[test]
    fn handoff_dispatch_rejects_a_conflicting_existing_child_run() {
        let (mut store, _, child, child_snapshot, event) = dispatch_fixture(2, "approved");
        let mut conflicting = child.clone();
        conflicting.agent_id = "agent-source".into();
        conflicting.scope.agent_id = "agent-source".into();
        store.upsert_execution_run(&conflicting).unwrap();

        assert!(matches!(
            store.dispatch_handoff_and_persist_child(
                "handoff-dispatch",
                &child,
                &child_snapshot,
                &event,
            ),
            Err(StorageError::HandoffDispatchRejected { .. })
        ));
        assert_eq!(store.replay_after(0).unwrap().len(), 0);
        assert_eq!(
            store
                .connection
                .query_row(
                    "SELECT call_count FROM collaboration_runs WHERE id = 'collaboration-dispatch'",
                    [],
                    |row| row.get::<_, u32>(0),
                )
                .unwrap(),
            0
        );
        assert_eq!(store.load_execution_runs().unwrap().len(), 2);
        assert!(store
            .load_model_snapshot("execution-child")
            .unwrap()
            .is_none());
    }

    #[test]
    fn handoff_dispatch_rejects_a_conflicting_child_snapshot_without_writes() {
        let (mut store, _, child, child_snapshot, event) = dispatch_fixture(2, "approved");
        store.upsert_execution_run(&child).unwrap();
        let existing_snapshot = ModelSnapshot {
            model_id: Some("existing-model".into()),
            ..child_snapshot.clone()
        };
        store.upsert_model_snapshot(&existing_snapshot).unwrap();

        assert!(matches!(
            store.dispatch_handoff_and_persist_child(
                "handoff-dispatch",
                &child,
                &child_snapshot,
                &event,
            ),
            Err(StorageError::ModelSnapshotConflict { .. })
        ));
        assert_eq!(
            store.load_model_snapshot(&child.id).unwrap(),
            Some(existing_snapshot)
        );
        assert!(store.replay_after(0).unwrap().is_empty());
        assert_eq!(
            store
                .connection
                .query_row(
                    "SELECT call_count FROM collaboration_runs WHERE id = 'collaboration-dispatch'",
                    [],
                    |row| row.get::<_, u32>(0),
                )
                .unwrap(),
            0
        );
        assert_eq!(
            store
                .load_handoff("handoff-dispatch")
                .unwrap()
                .unwrap()
                .status,
            "approved"
        );
    }

    #[test]
    fn repeated_handoff_dispatch_rejects_a_conflicting_snapshot_without_mutation() {
        let (mut store, _, child, child_snapshot, event) = dispatch_fixture(2, "approved");
        store
            .dispatch_handoff_and_persist_child("handoff-dispatch", &child, &child_snapshot, &event)
            .unwrap();
        let conflicting_snapshot = ModelSnapshot {
            revision: Some(2),
            ..child_snapshot.clone()
        };

        assert!(matches!(
            store.dispatch_handoff_and_persist_child(
                "handoff-dispatch",
                &child,
                &conflicting_snapshot,
                &event,
            ),
            Err(StorageError::ModelSnapshotConflict { .. })
        ));
        assert_eq!(
            store.load_model_snapshot(&child.id).unwrap(),
            Some(child_snapshot)
        );
        assert_eq!(store.replay_after(0).unwrap().len(), 1);
        assert_eq!(
            store
                .connection
                .query_row(
                    "SELECT call_count FROM collaboration_runs WHERE id = 'collaboration-dispatch'",
                    [],
                    |row| row.get::<_, u32>(0),
                )
                .unwrap(),
            1
        );
        assert_eq!(
            store
                .load_handoff("handoff-dispatch")
                .unwrap()
                .unwrap()
                .status,
            "dispatched"
        );
    }

    #[test]
    fn handoff_dispatch_rejects_cross_run_snapshot_binding_before_writes() {
        let (mut store, source, child, _, event) = dispatch_fixture(2, "approved");
        let mismatched_snapshot = ModelSnapshot {
            run_id: source.id,
            connector_id: Some("mock".into()),
            model_id: Some("mock-default".into()),
            revision: Some(1),
        };

        assert!(matches!(
            store.dispatch_handoff_and_persist_child(
                "handoff-dispatch",
                &child,
                &mismatched_snapshot,
                &event,
            ),
            Err(StorageError::ModelSnapshotInvalid { .. })
        ));
        assert_dispatch_rollback(&store);
    }

    #[test]
    fn handoff_creation_rejects_cross_collaboration_unknown_status_and_bad_details() {
        let (mut store, source, _, _, _) = dispatch_fixture(2, "approved");
        store
            .create_collaboration_run(
                "project-dispatch",
                &CollaborationRun {
                    id: "collaboration-other".into(),
                    root_agent_ids: vec!["agent-source".into()],
                    call_count: 0,
                    max_calls: 2,
                    depth: 0,
                    max_depth: 4,
                    status: CollaborationStatus::Pending,
                    stop_reason: None,
                    auto_dispatch_handoffs: false,
                },
            )
            .unwrap();
        let cross_collaboration = Handoff {
            id: "handoff-cross-collaboration".into(),
            collaboration_run_id: "collaboration-other".into(),
            from_execution_run_id: source.id.clone(),
            to_agent_id: "agent-target".into(),
            status: "proposed".into(),
            details: None,
        };
        assert!(matches!(
            store.create_handoff(&cross_collaboration),
            Err(StorageError::HandoffContractRejected { .. })
        ));

        let mut bad_details = empty_handoff_details();
        bad_details.from_agent_id = Some("agent-target".into());
        let mismatched_details = Handoff {
            id: "handoff-bad-details".into(),
            collaboration_run_id: "collaboration-dispatch".into(),
            from_execution_run_id: source.id,
            to_agent_id: "agent-target".into(),
            status: "proposed".into(),
            details: Some(bad_details),
        };
        assert!(matches!(
            store.create_handoff(&mismatched_details),
            Err(StorageError::HandoffContractRejected { .. })
        ));

        let unknown_status = Handoff {
            id: "handoff-unknown-status".into(),
            collaboration_run_id: "collaboration-dispatch".into(),
            from_execution_run_id: "execution-source".into(),
            to_agent_id: "agent-target".into(),
            status: "unknown".into(),
            details: None,
        };
        assert!(matches!(
            store.create_handoff(&unknown_status),
            Err(StorageError::HandoffContractRejected { .. })
        ));
    }

    #[test]
    fn event_stream_epoch_survives_store_reopen() {
        let path = std::env::temp_dir().join(format!(
            "agenttalk-stream-epoch-{}.sqlite3",
            std::process::id()
        ));
        let first = {
            let mut store = SqliteStore::open(&path).unwrap();
            store.event_stream_epoch().unwrap()
        };
        let second = {
            let mut store = SqliteStore::open(&path).unwrap();
            store.event_stream_epoch().unwrap()
        };
        assert_eq!(first, second);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite3-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite3-shm"));
    }

    #[test]
    fn identity_model_lists_and_assignment_selection_persist_with_scope_isolation() {
        let mut store = SqliteStore::open_in_memory().unwrap();
        store
            .create_project("model-project", "Model project", None)
            .unwrap();
        store
            .create_agent("model-agent", "Model agent", "role", "specialty", "prompt")
            .unwrap();
        store
            .create_conversation("model-conversation", "model-project", "Conversation")
            .unwrap();

        let project_selection = ModelSelection {
            mode: ModelSelectionMode::Pinned,
            model_id: Some("project-pinned".into()),
        };
        store
            .set_project_agent_assignment_with_model_selection(
                "model-project",
                "model-agent",
                true,
                &WorkspaceAccess::ReadOnly,
                &project_selection,
                IdentityModelListMode::Override,
                4,
            )
            .unwrap();
        let stored = store
            .load_project_agent_model_selection("model-project", "model-agent")
            .unwrap()
            .unwrap();
        assert_eq!(stored.selection, project_selection);
        assert_eq!(
            stored.candidate_model_list_mode,
            IdentityModelListMode::Override
        );
        assert_eq!(stored.candidate_model_list_revision, 4);
        store
            .set_project_agent_assignment(
                "model-project",
                "model-agent",
                false,
                &WorkspaceAccess::None,
            )
            .unwrap();
        let preserved = store
            .load_project_agent_model_selection("model-project", "model-agent")
            .unwrap()
            .unwrap();
        assert_eq!(preserved, stored);

        let target = IdentityModelListTarget {
            scope: IdentityModelListScope::BaseAgent,
            agent_id: "model-agent".into(),
            project_id: None,
            conversation_id: None,
        };
        store
            .upsert_identity_model_option(&IdentityModelOption {
                id: "model-option-a".into(),
                scope: IdentityModelListScope::BaseAgent,
                agent_id: "model-agent".into(),
                project_id: None,
                conversation_id: None,
                model_id: "base-default".into(),
                display_name: "Base default".into(),
                connector_id: "codex".into(),
                source: ModelOptionSource::Manual,
                availability: ModelAvailability::Unverified,
                is_default: true,
                sort_order: 0,
                catalog_revision: None,
                context_window: None,
                reasoning_efforts: vec![],
                service_tiers: vec![],
            })
            .unwrap();
        store
            .upsert_identity_model_option(&IdentityModelOption {
                id: "model-option-foreign".into(),
                scope: IdentityModelListScope::BaseAgent,
                agent_id: "model-agent".into(),
                project_id: None,
                conversation_id: None,
                model_id: "kun-model".into(),
                display_name: "Foreign".into(),
                connector_id: "kun".into(),
                source: ModelOptionSource::Runtime,
                availability: ModelAvailability::Available,
                is_default: false,
                sort_order: 1,
                catalog_revision: Some("kun-1".into()),
                context_window: None,
                reasoning_efforts: vec![],
                service_tiers: vec![],
            })
            .unwrap();
        let codex = store
            .query_identity_model_options(&target, Some("codex"))
            .unwrap();
        assert_eq!(codex.len(), 1);
        assert_eq!(codex[0].model_id, "base-default");
        store
            .set_identity_model_option_default(&target, "codex", "base-default")
            .unwrap();
        assert!(
            store
                .query_identity_model_options(&target, Some("codex"))
                .unwrap()[0]
                .is_default
        );
    }

    #[test]
    fn identity_model_options_file_backed_scopes_defaults_and_revisions_are_isolated() {
        let path = std::env::temp_dir().join(format!(
            "agenttalk-identity-options-{}-{}.sqlite3",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = fs::remove_file(&path);
        let base = IdentityModelListTarget {
            scope: IdentityModelListScope::BaseAgent,
            agent_id: "identity-agent".into(),
            project_id: None,
            conversation_id: None,
        };
        let project = IdentityModelListTarget {
            scope: IdentityModelListScope::ProjectAgent,
            agent_id: "identity-agent".into(),
            project_id: Some("identity-project".into()),
            conversation_id: None,
        };
        let conversation = IdentityModelListTarget {
            scope: IdentityModelListScope::ConversationAgent,
            agent_id: "identity-agent".into(),
            project_id: None,
            conversation_id: Some("identity-conversation".into()),
        };
        {
            let mut store = SqliteStore::open(&path).unwrap();
            store
                .create_project("identity-project", "Identity Project", None)
                .unwrap();
            store
                .create_agent(
                    "identity-agent",
                    "Identity Agent",
                    "role",
                    "specialty",
                    "prompt",
                )
                .unwrap();
            store
                .create_conversation(
                    "identity-conversation",
                    "identity-project",
                    "Identity Conversation",
                )
                .unwrap();
            store
                .set_project_agent_assignment(
                    "identity-project",
                    "identity-agent",
                    true,
                    &WorkspaceAccess::None,
                )
                .unwrap();
            store
                .set_conversation_agent_assignment("identity-conversation", "identity-agent", true)
                .unwrap();
            for (connector_id, runtime_type) in
                [("connector.codex", "codex"), ("connector.kun", "kun")]
            {
                store
                    .create_connector_profile(&ConnectorProfile {
                        scope_id: "desktop".into(),
                        connector_id: connector_id.into(),
                        display_name: connector_id.into(),
                        provider_type: runtime_type.into(),
                        runtime_type: runtime_type.into(),
                        enabled: true,
                        auth_env_key: None,
                    })
                    .unwrap();
            }

            let option = |id: &str,
                          target: &IdentityModelListTarget,
                          connector: &str,
                          model: &str,
                          default: bool,
                          revision: &str| IdentityModelOption {
                id: id.into(),
                scope: target.scope,
                agent_id: target.agent_id.clone(),
                project_id: target.project_id.clone(),
                conversation_id: target.conversation_id.clone(),
                model_id: model.into(),
                display_name: model.into(),
                connector_id: connector.into(),
                source: ModelOptionSource::Manual,
                availability: ModelAvailability::Unverified,
                is_default: default,
                sort_order: 0,
                catalog_revision: Some(revision.into()),
                context_window: Some(128_000),
                reasoning_efforts: vec!["medium".into()],
                service_tiers: vec![],
            };
            store
                .upsert_identity_model_option(&option(
                    "base-codex-a",
                    &base,
                    "connector.codex",
                    "codex-model-a",
                    true,
                    "catalog-1",
                ))
                .unwrap();
            store
                .upsert_identity_model_option(&option(
                    "base-codex-b",
                    &base,
                    "connector.codex",
                    "codex-model-b",
                    true,
                    "catalog-2",
                ))
                .unwrap();
            store
                .upsert_identity_model_option(&option(
                    "base-kun-a",
                    &base,
                    "connector.kun",
                    "kun-model-a",
                    true,
                    "catalog-kun-1",
                ))
                .unwrap();
            store
                .upsert_identity_model_option(&option(
                    "project-codex-a",
                    &project,
                    "connector.codex",
                    "codex-model-a",
                    true,
                    "catalog-project-1",
                ))
                .unwrap();
            store
                .upsert_identity_model_option(&option(
                    "conversation-codex-a",
                    &conversation,
                    "connector.codex",
                    "codex-model-a",
                    true,
                    "catalog-conversation-1",
                ))
                .unwrap();

            let codex_base = store
                .query_identity_model_options(&base, Some("connector.codex"))
                .unwrap();
            assert_eq!(codex_base.len(), 2);
            assert_eq!(
                codex_base.iter().filter(|option| option.is_default).count(),
                1
            );
            assert_eq!(
                codex_base
                    .iter()
                    .find(|option| option.is_default)
                    .unwrap()
                    .model_id,
                "codex-model-b"
            );
            assert_eq!(
                store
                    .query_identity_model_options(&base, Some("connector.kun"))
                    .unwrap()
                    .len(),
                1
            );
            assert_eq!(
                store
                    .query_identity_model_options(&project, Some("connector.codex"))
                    .unwrap()
                    .len(),
                1
            );
            assert_eq!(
                store
                    .query_identity_model_options(&conversation, Some("connector.codex"))
                    .unwrap()
                    .len(),
                1
            );

            let mut revised = option(
                "base-codex-b",
                &base,
                "connector.codex",
                "codex-model-b",
                true,
                "catalog-3",
            );
            revised.reasoning_efforts = vec!["high".into()];
            store.upsert_identity_model_option(&revised).unwrap();
            assert_eq!(
                store
                    .query_identity_model_options(&base, Some("connector.codex"))
                    .unwrap()
                    .iter()
                    .find(|option| option.id == "base-codex-b")
                    .unwrap()
                    .catalog_revision
                    .as_deref(),
                Some("catalog-3")
            );

            let invalid = IdentityModelOption {
                id: "invalid-scope".into(),
                scope: IdentityModelListScope::ProjectAgent,
                agent_id: "identity-agent".into(),
                project_id: None,
                conversation_id: None,
                model_id: "codex-model-a".into(),
                display_name: "invalid".into(),
                connector_id: "connector.codex".into(),
                source: ModelOptionSource::Manual,
                availability: ModelAvailability::Unverified,
                is_default: false,
                sort_order: 0,
                catalog_revision: None,
                context_window: None,
                reasoning_efforts: vec![],
                service_tiers: vec![],
            };
            assert!(matches!(
                store.upsert_identity_model_option(&invalid),
                Err(StorageError::ModelSelectionInvalid { .. })
            ));
        }

        let reopened = SqliteStore::open(&path).unwrap();
        let persisted = reopened
            .query_identity_model_options(&base, Some("connector.codex"))
            .unwrap();
        assert_eq!(persisted.len(), 2);
        let default = persisted.iter().find(|option| option.is_default).unwrap();
        assert_eq!(default.model_id, "codex-model-b");
        assert_eq!(default.catalog_revision.as_deref(), Some("catalog-3"));
        assert_eq!(default.source, ModelOptionSource::Manual);
        assert_eq!(default.availability, ModelAvailability::Unverified);
        drop(reopened);
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(path.with_extension("sqlite3-wal"));
        let _ = fs::remove_file(path.with_extension("sqlite3-shm"));
    }

    #[test]
    fn agent_model_binding_patch_is_presence_aware_and_survives_reopen() {
        let path = std::env::temp_dir().join(format!(
            "agenttalk-binding-patch-{}-{}.sqlite3",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = fs::remove_file(&path);
        {
            let mut store = SqliteStore::open(&path).unwrap();
            store
                .create_agent(
                    "binding-agent",
                    "Binding Agent",
                    "role",
                    "specialty",
                    "prompt",
                )
                .unwrap();
            store
                .set_agent_model_binding(
                    "binding-agent",
                    &AgentModelBinding {
                        connector_id: Some("connector.codex".into()),
                        model_id: Some("codex-model-a".into()),
                        candidate_model_list_revision: 7,
                    },
                )
                .unwrap();
            let preserved = store
                .patch_agent_model_binding(
                    "binding-agent",
                    &AgentModelBindingPatch {
                        connector_id: BindingFieldPatch::Preserve,
                        model_id: BindingFieldPatch::Preserve,
                        candidate_model_list_revision: BindingFieldPatch::Preserve,
                    },
                )
                .unwrap();
            assert_eq!(preserved.model_id.as_deref(), Some("codex-model-a"));
            assert_eq!(preserved.candidate_model_list_revision, 7);

            let cleared_model = store
                .patch_agent_model_binding(
                    "binding-agent",
                    &AgentModelBindingPatch {
                        connector_id: BindingFieldPatch::Preserve,
                        model_id: BindingFieldPatch::Clear,
                        candidate_model_list_revision: BindingFieldPatch::Preserve,
                    },
                )
                .unwrap();
            assert_eq!(
                cleared_model.connector_id.as_deref(),
                Some("connector.codex")
            );
            assert!(cleared_model.model_id.is_none());
            assert_eq!(cleared_model.candidate_model_list_revision, 7);

            let updated = store
                .patch_agent_model_binding(
                    "binding-agent",
                    &AgentModelBindingPatch {
                        connector_id: BindingFieldPatch::Set("connector.kun".into()),
                        model_id: BindingFieldPatch::Set("kun-model-a".into()),
                        candidate_model_list_revision: BindingFieldPatch::Set(9),
                    },
                )
                .unwrap();
            assert_eq!(updated.connector_id.as_deref(), Some("connector.kun"));
            assert_eq!(updated.model_id.as_deref(), Some("kun-model-a"));
            assert_eq!(updated.candidate_model_list_revision, 9);

            let cleared = store
                .patch_agent_model_binding(
                    "binding-agent",
                    &AgentModelBindingPatch {
                        connector_id: BindingFieldPatch::Clear,
                        model_id: BindingFieldPatch::Preserve,
                        candidate_model_list_revision: BindingFieldPatch::Clear,
                    },
                )
                .unwrap();
            assert!(cleared.connector_id.is_none());
            assert!(cleared.model_id.is_none());
            assert_eq!(cleared.candidate_model_list_revision, 0);
            assert!(matches!(
                store.patch_agent_model_binding(
                    "binding-agent",
                    &AgentModelBindingPatch {
                        connector_id: BindingFieldPatch::Preserve,
                        model_id: BindingFieldPatch::Set("orphan-model".into()),
                        candidate_model_list_revision: BindingFieldPatch::Preserve,
                    },
                ),
                Err(StorageError::ModelSelectionInvalid { .. })
            ));
        }
        let reopened = SqliteStore::open(&path).unwrap();
        assert_eq!(
            reopened.load_agent_model_binding("binding-agent").unwrap(),
            Some(AgentModelBinding {
                connector_id: None,
                model_id: None,
                candidate_model_list_revision: 0,
            })
        );
        drop(reopened);
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(path.with_extension("sqlite3-wal"));
        let _ = fs::remove_file(path.with_extension("sqlite3-shm"));
    }

    #[test]
    fn model_selection_snapshot_is_atomic_idempotent_and_conflict_safe() {
        let mut store = SqliteStore::open_in_memory().unwrap();
        let run = ExecutionRun {
            id: "selection-run".into(),
            collaboration_run_id: "selection-collab".into(),
            project_id: "selection-project".into(),
            conversation_id: "selection-conversation".into(),
            agent_id: "selection-agent".into(),
            status: ExecutionStatus::Pending,
            version: 0,
            scope: ScopeSnapshot {
                project_id: "selection-project".into(),
                conversation_id: "selection-conversation".into(),
                agent_id: "selection-agent".into(),
                workspace_access: WorkspaceAccess::None,
                canonical_cwd: None,
            },
            terminal_reason: None,
        };
        let connector_snapshot = ModelSnapshot {
            run_id: run.id.clone(),
            connector_id: Some("codex".into()),
            model_id: Some("base-default".into()),
            revision: Some(1),
        };
        let selection_snapshot = ModelSelectionSnapshot {
            run_id: run.id.clone(),
            version: 2,
            runtime_type: "local_gateway".into(),
            provider_type: "codex".into(),
            connector_id: "codex".into(),
            effective_model_id: Some("base-default".into()),
            selection_source: ModelSelectionSource::IdentityDefault,
            selection_mode: ModelSelectionMode::ConnectorDefault,
            availability: ModelAvailability::Unverified,
            catalog_revision: Some("catalog-1".into()),
            context_window: Some(128000),
            reasoning_efforts: vec!["medium".into()],
            service_tiers: vec![],
            candidate_model_list: Some(agenttalk_domain::IdentityModelListSnapshot {
                scope: IdentityModelListScope::BaseAgent,
                mode: IdentityModelListMode::Own,
                revision: 3,
                hash: "a".repeat(64),
                option_count: 1,
            }),
        };
        store
            .persist_execution_run_and_model_snapshots_and_events(
                &run,
                &connector_snapshot,
                &selection_snapshot,
                &[],
            )
            .unwrap();
        assert_eq!(
            store.load_model_selection_snapshot(&run.id).unwrap(),
            Some(selection_snapshot.clone())
        );
        store
            .upsert_model_selection_snapshot(&selection_snapshot)
            .unwrap();
        let mut conflict = selection_snapshot.clone();
        conflict.effective_model_id = Some("other-model".into());
        assert!(matches!(
            store.upsert_model_selection_snapshot(&conflict),
            Err(StorageError::ModelSelectionSnapshotConflict { .. })
        ));
        let mut mismatched = selection_snapshot;
        mismatched.run_id = "other-run".into();
        let mismatched_connector = ModelSnapshot {
            run_id: "mismatched-run".into(),
            ..connector_snapshot.clone()
        };
        assert!(matches!(
            store.persist_execution_run_and_model_snapshots_and_events(
                &ExecutionRun {
                    id: "mismatched-run".into(),
                    ..run
                },
                &mismatched_connector,
                &mismatched,
                &[],
            ),
            Err(StorageError::ModelSelectionInvalid { .. })
        ));
        assert!(store
            .load_execution_run("mismatched-run")
            .unwrap()
            .is_none());
    }

    #[test]
    fn initial_execution_boundary_rolls_back_run_snapshots_manifest_and_events_together() {
        let mut store = SqliteStore::open_in_memory().unwrap();
        store
            .append_event(&RuntimeEvent {
                event_id: "initial-boundary-duplicate-event".into(),
                execution_run_id: "seed-run".into(),
                runtime_id: "core".into(),
                thread_id: None,
                turn_id: None,
                sequence: 0,
                event_type: "seed".into(),
                timestamp_ms: 1,
                payload: json!({}),
            })
            .unwrap();
        let run = ExecutionRun {
            id: "initial-boundary-run".into(),
            collaboration_run_id: "initial-boundary-collaboration".into(),
            project_id: "initial-boundary-project".into(),
            conversation_id: "initial-boundary-conversation".into(),
            agent_id: "initial-boundary-agent".into(),
            status: ExecutionStatus::Pending,
            version: 0,
            scope: ScopeSnapshot {
                project_id: "initial-boundary-project".into(),
                conversation_id: "initial-boundary-conversation".into(),
                agent_id: "initial-boundary-agent".into(),
                workspace_access: WorkspaceAccess::None,
                canonical_cwd: None,
            },
            terminal_reason: None,
        };
        let snapshot = ModelSnapshot {
            run_id: run.id.clone(),
            connector_id: Some("connector-codex".into()),
            model_id: Some("codex-model-a".into()),
            revision: Some(1),
        };
        let selection_snapshot = ModelSelectionSnapshot {
            run_id: run.id.clone(),
            version: 2,
            runtime_type: "codex".into(),
            provider_type: "codex".into(),
            connector_id: "connector-codex".into(),
            effective_model_id: Some("codex-model-a".into()),
            selection_source: ModelSelectionSource::IdentityDefault,
            selection_mode: ModelSelectionMode::ConnectorDefault,
            availability: ModelAvailability::Available,
            catalog_revision: Some("fixture-catalog-1".into()),
            context_window: Some(128_000),
            reasoning_efforts: vec!["medium".into()],
            service_tiers: Vec::new(),
            candidate_model_list: None,
        };
        let manifest = agenttalk_domain::ContextManifest {
            id: "initial-boundary-manifest".into(),
            execution_run_id: run.id.clone(),
            schema_version: "context-v2".into(),
            source_ids: Vec::new(),
            workspace_access: WorkspaceAccess::None,
            canonical_cwd: None,
            connector_id: snapshot.connector_id.clone(),
            model_id: snapshot.model_id.clone(),
        };
        let duplicate_event = RuntimeEvent {
            event_id: "initial-boundary-duplicate-event".into(),
            execution_run_id: run.id.clone(),
            runtime_id: "core".into(),
            thread_id: None,
            turn_id: None,
            sequence: 0,
            event_type: "execution.created".into(),
            timestamp_ms: 2,
            payload: json!({"status":"pending"}),
        };

        assert!(matches!(
            store.persist_execution_run_and_model_snapshots_context_manifest_and_events(
                &run,
                &snapshot,
                &selection_snapshot,
                &manifest,
                "bundle-boundary",
                "[]",
                &[duplicate_event],
            ),
            Err(StorageError::Sqlite(_))
        ));
        assert!(store.load_execution_run(&run.id).unwrap().is_none());
        assert!(store.load_model_snapshot(&run.id).unwrap().is_none());
        assert!(store
            .load_model_selection_snapshot(&run.id)
            .unwrap()
            .is_none());
        assert_eq!(
            store
                .connection
                .query_row(
                    "SELECT COUNT(*) FROM context_manifests WHERE id = ?1",
                    [&manifest.id],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
        assert_eq!(store.replay_after(0).unwrap().len(), 1);
    }

    #[test]
    fn project_summary_scope_and_context_metadata_queries_are_supported() {
        let mut store = SqliteStore::open_in_memory().unwrap();
        store
            .create_project("summary-project-scope", "Summary project", None)
            .unwrap();
        store
            .create_agent(
                "summary-agent-scope",
                "Summary agent",
                "role",
                "specialty",
                "prompt",
            )
            .unwrap();
        let summary = Summary {
            id: "summary-project-scope-1".into(),
            scope_id: "summary-project-scope".into(),
            version: 1,
            content_hash: "a".repeat(64),
            artifact_id: None,
        };
        assert!(store.store_summary(&summary).unwrap());
        assert_eq!(
            store
                .load_recent_summary_metadata("summary-project-scope", 4)
                .unwrap(),
            vec![summary]
        );

        let memory = MemoryItem {
            id: "summary-memory-1".into(),
            scope_id: "summary-project-scope".into(),
            agent_id: Some("summary-agent-scope".into()),
            content_hash: "b".repeat(64),
            confirmed: true,
        };
        assert!(store.store_memory(&memory).unwrap());
        assert_eq!(
            store
                .load_recent_memory_metadata("summary-project-scope", "summary-agent-scope", 4,)
                .unwrap(),
            vec![memory]
        );
    }

    #[test]
    fn dirty_schema_migration_fails_closed_before_opening_the_store() {
        let path = std::env::temp_dir().join(format!(
            "agenttalk-storage-dirty-migration-{}-{}.sqlite3",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_file(&path);
        {
            let store = SqliteStore::open(&path).unwrap();
            drop(store);
            let connection = Connection::open(&path).unwrap();
            connection
                .execute(
                    "UPDATE schema_migrations SET dirty = 1 WHERE version = ?1",
                    [SCHEMA_VERSION],
                )
                .unwrap();
        }
        assert!(matches!(
            SqliteStore::open(&path),
            Err(StorageError::MigrationDirty {
                version: SCHEMA_VERSION
            })
        ));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite3-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite3-shm"));
    }

    fn local_agent_import_request(request_id: &str) -> LocalAgentImportRequest {
        LocalAgentImportRequest {
            import_id: format!("import-{request_id}"),
            scope_id: CONNECTOR_PROFILE_SCOPE.into(),
            client_id: "fixture-client".into(),
            request_id: request_id.into(),
            payload_hash: "a".repeat(64),
            project_id: "project-import".into(),
            connector: ConnectorProfile {
                scope_id: CONNECTOR_PROFILE_SCOPE.into(),
                connector_id: "local-acp-fixture".into(),
                display_name: "Local ACP Fixture".into(),
                provider_type: "local_agent".into(),
                runtime_type: "acp".into(),
                enabled: true,
                auth_env_key: None,
            },
            agent_id: "agent-local-import".into(),
            agent_name: "Local ACP Fixture".into(),
            binding: LocalAgentAdapterBinding {
                adapter_kind: "acp".into(),
                protocol_major: 1,
                manifest_id: "org.fixture.acp".into(),
                manifest_sha256: "b".repeat(64),
                candidate_binding_digest: "c".repeat(64),
                capabilities_json: r#"{"streaming":true}"#.into(),
                auth_required: false,
            },
            model_selection: ModelSelection {
                mode: ModelSelectionMode::ConnectorDefault,
                model_id: None,
            },
        }
    }

    #[test]
    fn local_agent_import_is_atomic_idempotent_and_secret_free() {
        let mut store = SqliteStore::open_in_memory().unwrap();
        store
            .create_project("project-import", "Import target", None)
            .unwrap();
        let request = local_agent_import_request("request-import");
        let outcome = store.import_local_agent(&request).unwrap();
        assert!(!outcome.reused);
        assert!(outcome.event_sequence > 0);
        let replay = store.import_local_agent(&request).unwrap();
        assert!(replay.reused);
        assert_eq!(replay.import_id, outcome.import_id);
        assert_eq!(
            store
                .connection
                .query_row(
                    "SELECT COUNT(*) FROM connector_adapter_bindings",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            1
        );
        assert_eq!(
            store
                .connection
                .query_row("SELECT COUNT(*) FROM local_agent_imports", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert_eq!(
            store
                .replay_after(0)
                .unwrap()
                .iter()
                .filter(|event| event.event_type == "local_agent.imported")
                .count(),
            1
        );
        let schema: String = store.connection.query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'connector_adapter_bindings'",
            [],
            |row| row.get(0),
        ).unwrap();
        assert!(!schema.to_ascii_lowercase().contains("token"));
        assert!(!schema.to_ascii_lowercase().contains("authorization"));
        assert!(!schema.to_ascii_lowercase().contains("cookie"));
        assert!(!schema.to_ascii_lowercase().contains("path"));
    }

    #[test]
    fn local_agent_import_rolls_back_every_row_when_project_is_missing() {
        let mut store = SqliteStore::open_in_memory().unwrap();
        let request = local_agent_import_request("request-missing-project");
        assert!(matches!(
            store.import_local_agent(&request),
            Err(StorageError::Sqlite(_))
        ));
        for table in [
            "connector_profiles",
            "connector_adapter_bindings",
            "agents",
            "project_agents",
            "local_agent_imports",
            "event_store",
        ] {
            let count: i64 = store
                .connection
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(count, 0, "{table} must be empty after rollback");
        }
    }

    #[test]
    fn local_agent_import_binding_and_project_reuse_returns_original_event_sequence() {
        let mut store = SqliteStore::open_in_memory().unwrap();
        store
            .create_project("project-import", "Import target", None)
            .unwrap();
        let first = store
            .import_local_agent(&local_agent_import_request("request-first"))
            .unwrap();
        assert!(!first.reused);
        assert!(
            first.event_sequence > 0,
            "fresh import must persist a non-zero local_agent.imported event"
        );

        // A different requestId with the same binding+project is an intentional
        // business reuse: it must return the SAME real, non-zero original event
        // sequence and must not append a second event.
        let reused = store
            .import_local_agent(&local_agent_import_request("request-second"))
            .unwrap();
        assert!(reused.reused);
        assert_eq!(reused.import_id, first.import_id);
        assert_eq!(
            reused.event_sequence, first.event_sequence,
            "binding+project reuse must return the original event sequence, not 0"
        );
        assert_eq!(
            store
                .replay_after(0)
                .unwrap()
                .iter()
                .filter(|event| event.event_type == "local_agent.imported")
                .count(),
            1,
            "business reuse must not emit a second local_agent.imported event"
        );

        // Same-requestId replay keeps the same sequence as well.
        let replay = store
            .import_local_agent(&local_agent_import_request("request-first"))
            .unwrap();
        assert!(replay.reused);
        assert_eq!(replay.event_sequence, first.event_sequence);
    }

    #[test]
    fn local_agent_import_conflicts_are_distinct_storage_errors() {
        let mut store = SqliteStore::open_in_memory().unwrap();
        store
            .create_project("project-import", "Import target", None)
            .unwrap();
        let request = local_agent_import_request("request-conflict");
        store.import_local_agent(&request).unwrap();

        // Same requestId with a different payload hash is a request conflict.
        let mut request_conflict = request.clone();
        request_conflict.payload_hash = "d".repeat(64);
        assert!(matches!(
            store.import_local_agent(&request_conflict),
            Err(StorageError::LocalAgentImportRequestConflict)
        ));

        // A different connector profile under the same connector id conflicts.
        let mut profile_conflict = request.clone();
        profile_conflict.request_id = "request-profile-conflict".into();
        profile_conflict.import_id = "import-profile-conflict".into();
        profile_conflict.agent_id = "agent-profile-other".into();
        profile_conflict.binding.candidate_binding_digest = "e".repeat(64);
        profile_conflict.connector.display_name = "Different Name".into();
        assert!(matches!(
            store.import_local_agent(&profile_conflict),
            Err(StorageError::ConnectorProfileConflict { .. })
        ));

        // A different binding under the same connector id conflicts.
        let mut binding_conflict = request.clone();
        binding_conflict.request_id = "request-binding-conflict".into();
        binding_conflict.import_id = "import-binding-conflict".into();
        binding_conflict.agent_id = "agent-binding-other".into();
        binding_conflict.binding.candidate_binding_digest = "e".repeat(64);
        binding_conflict.binding.manifest_sha256 = "f".repeat(64);
        assert!(matches!(
            store.import_local_agent(&binding_conflict),
            Err(StorageError::LocalAgentImportBindingConflict)
        ));

        // No rows beyond the original import were created by the conflicts.
        for table in [
            "connector_adapter_bindings",
            "local_agent_imports",
            "agents",
            "project_agents",
        ] {
            let count: i64 = store
                .connection
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(count, 1, "{table} must keep only the original import rows");
        }
        assert_eq!(
            store
                .replay_after(0)
                .unwrap()
                .iter()
                .filter(|event| event.event_type == "local_agent.imported")
                .count(),
            1
        );
    }

    #[test]
    fn local_agent_import_model_selection_conflict_is_fail_closed() {
        // First import: ConnectorDefault/None (modelSelection null).
        let mut store = SqliteStore::open_in_memory().unwrap();
        store
            .create_project("project-import", "Import target", None)
            .unwrap();
        let first = store
            .import_local_agent(&local_agent_import_request("request-default"))
            .unwrap();
        assert!(!first.reused);
        assert!(first.event_sequence > 0);

        // Different requestId, same binding + project, but a pinned model:
        // must fail closed as a model-selection conflict, never silently
        // reuse the old connector-default assignment.
        let mut pinned = local_agent_import_request("request-pinned");
        pinned.model_selection = ModelSelection {
            mode: ModelSelectionMode::Pinned,
            model_id: Some("fixture-model".into()),
        };
        assert!(matches!(
            store.import_local_agent(&pinned),
            Err(StorageError::LocalAgentImportModelSelectionConflict)
        ));

        // No new rows anywhere; the existing assignment stays untouched.
        for table in [
            "connector_adapter_bindings",
            "local_agent_imports",
            "agents",
            "project_agents",
        ] {
            let count: i64 = store
                .connection
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(count, 1, "{table} must keep only the original import rows");
        }
        let (mode, model_id): (String, Option<String>) = store
            .connection
            .query_row(
                "SELECT model_selection_mode, model_id FROM project_agents",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(mode, "connector_default");
        assert_eq!(
            model_id, None,
            "existing assignment must remain connector-default/null"
        );
        assert_eq!(
            store
                .replay_after(0)
                .unwrap()
                .iter()
                .filter(|event| event.event_type == "local_agent.imported")
                .count(),
            1
        );

        // Reverse direction: pinned model first, then connector-default or a
        // different pinned model both conflict.
        let mut store2 = SqliteStore::open_in_memory().unwrap();
        store2
            .create_project("project-import", "Import target", None)
            .unwrap();
        let mut pinned_a = local_agent_import_request("request-pinned-a");
        pinned_a.model_selection = ModelSelection {
            mode: ModelSelectionMode::Pinned,
            model_id: Some("model-a".into()),
        };
        let first_a = store2.import_local_agent(&pinned_a).unwrap();
        assert!(!first_a.reused);

        let default_again = local_agent_import_request("request-pinned-then-default");
        assert!(matches!(
            store2.import_local_agent(&default_again),
            Err(StorageError::LocalAgentImportModelSelectionConflict)
        ));
        let mut pinned_b = local_agent_import_request("request-pinned-b");
        pinned_b.model_selection = ModelSelection {
            mode: ModelSelectionMode::Pinned,
            model_id: Some("model-b".into()),
        };
        assert!(matches!(
            store2.import_local_agent(&pinned_b),
            Err(StorageError::LocalAgentImportModelSelectionConflict)
        ));

        // The SAME normalized selection across requestIds still reuses with
        // the original non-zero sequence and no second event.
        let mut pinned_a_reuse = local_agent_import_request("request-pinned-a-reuse");
        pinned_a_reuse.model_selection = ModelSelection {
            mode: ModelSelectionMode::Pinned,
            model_id: Some("model-a".into()),
        };
        let reuse = store2.import_local_agent(&pinned_a_reuse).unwrap();
        assert!(reuse.reused);
        assert_eq!(reuse.import_id, first_a.import_id);
        assert_eq!(reuse.event_sequence, first_a.event_sequence);
        assert!(reuse.event_sequence > 0);
        assert_eq!(
            store2
                .replay_after(0)
                .unwrap()
                .iter()
                .filter(|event| event.event_type == "local_agent.imported")
                .count(),
            1
        );
    }
}
