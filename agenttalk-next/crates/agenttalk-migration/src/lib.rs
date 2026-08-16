use agenttalk_domain::{ModelSelectionSnapshot, StructuredHandoffDetails};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use thiserror::Error;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LegacyExport {
    pub source_schema_checksum: String,
    pub projects: Vec<ProjectExport>,
    pub agents: Vec<AgentExport>,
    pub project_agents: Vec<ProjectAgentExport>,
    pub conversations: Vec<ConversationExport>,
    pub conversation_agents: Vec<ConversationAgentExport>,
    pub messages: Vec<MessageExport>,
    pub execution_runs: Vec<ExecutionRunExport>,
    pub collaboration_runs: Vec<CollaborationRunExport>,
    pub handoffs: Vec<HandoffExport>,
    pub context_manifests: Vec<ContextManifestExport>,
    pub summaries: Vec<SummaryExport>,
    pub memories: Vec<MemoryExport>,
    #[serde(default)]
    pub attachments: Vec<AttachmentExport>,
    #[serde(default)]
    pub workflows: Vec<WorkflowTemplateExport>,
    #[serde(default)]
    pub model_snapshots: Vec<ModelSnapshotExport>,
    #[serde(default)]
    pub model_selection_snapshots: Vec<ModelSelectionSnapshotExport>,
    pub retrieval_sources: Vec<RetrievalSourceExport>,
    #[serde(default)]
    pub audit_timestamps: Vec<AuditTimestampExport>,
    pub model_candidates: Vec<ModelCandidateExport>,
    #[serde(default)]
    pub identity_model_options: Vec<IdentityModelOptionExport>,
    pub workspace_authorizations: Vec<WorkspaceAuthorizationExport>,
}

fn default_inherit() -> String {
    "inherit".into()
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectExport {
    pub id: String,
    pub name: String,
    pub root_path: Option<String>,
    pub archived: bool,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AgentExport {
    pub id: String,
    pub name: String,
    pub role: String,
    pub specialty: String,
    pub system_prompt: String,
    #[serde(default)]
    pub connector_id: Option<String>,
    #[serde(default)]
    pub model_id: Option<String>,
    #[serde(default)]
    pub candidate_model_list_revision: u64,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectAgentExport {
    pub project_id: String,
    pub agent_id: String,
    #[serde(default)]
    pub role: Option<String>,
    #[serde(default)]
    pub specialty: Option<String>,
    #[serde(default)]
    pub system_prompt: Option<String>,
    pub enabled: bool,
    pub workspace_access: String,
    #[serde(default = "default_inherit")]
    pub model_selection_mode: String,
    #[serde(default)]
    pub model_id: Option<String>,
    #[serde(default = "default_inherit")]
    pub candidate_model_list_mode: String,
    #[serde(default)]
    pub candidate_model_list_revision: u64,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConversationExport {
    pub id: String,
    pub project_id: String,
    pub title: String,
    pub scope_revision: u64,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConversationAgentExport {
    pub conversation_id: String,
    pub agent_id: String,
    #[serde(default)]
    pub role: Option<String>,
    #[serde(default)]
    pub specialty: Option<String>,
    #[serde(default)]
    pub system_prompt: Option<String>,
    pub enabled: bool,
    #[serde(default = "default_inherit")]
    pub model_selection_mode: String,
    #[serde(default)]
    pub model_id: Option<String>,
    #[serde(default = "default_inherit")]
    pub candidate_model_list_mode: String,
    #[serde(default)]
    pub candidate_model_list_revision: u64,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MessageExport {
    pub id: String,
    pub conversation_id: String,
    pub sender_id: String,
    pub sequence: u64,
    pub content: String,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionRunExport {
    pub id: String,
    pub collaboration_run_id: String,
    pub project_id: String,
    pub conversation_id: String,
    pub agent_id: String,
    pub status: String,
    pub version: u64,
    pub legacy: bool,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CollaborationRunExport {
    pub id: String,
    pub status: String,
    pub call_count: u32,
    pub max_calls: u32,
    pub max_depth: u32,
    #[serde(default)]
    pub project_id: Option<String>,
    #[serde(default)]
    pub root_agent_ids_json: Option<String>,
    #[serde(default)]
    pub depth: u32,
    #[serde(default)]
    pub stop_reason: Option<String>,
    #[serde(default)]
    pub auto_dispatch_handoffs: bool,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HandoffExport {
    pub id: String,
    pub collaboration_run_id: String,
    pub from_execution_run_id: String,
    pub to_agent_id: String,
    pub status: String,
    #[serde(default)]
    pub details: Option<StructuredHandoffDetails>,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ContextManifestExport {
    pub id: String,
    pub execution_run_id: String,
    pub schema_version: String,
    pub bundle_hash: String,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SummaryExport {
    pub id: String,
    pub scope_id: String,
    pub version: u64,
    pub content_hash: String,
    #[serde(default)]
    pub artifact_id: Option<String>,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryExport {
    pub id: String,
    pub scope_id: String,
    pub agent_id: Option<String>,
    pub content_hash: String,
    pub confirmed: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AttachmentExport {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub artifact_id: Option<String>,
    pub message_id: String,
    pub ordinal: u64,
    pub file_name: String,
    pub sha256: String,
    pub size: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowTemplateExport {
    pub id: String,
    pub project_id: String,
    pub name: String,
    pub kind: String,
    pub steps_json: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelSnapshotExport {
    pub run_id: String,
    #[serde(default)]
    pub connector_id: Option<String>,
    #[serde(default)]
    pub model_id: Option<String>,
    #[serde(default)]
    pub revision: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelSelectionSnapshotExport {
    pub run_id: String,
    pub snapshot_json: serde_json::Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityModelOptionExport {
    pub id: String,
    pub identity_scope: String,
    pub agent_id: String,
    #[serde(default)]
    pub project_id: Option<String>,
    #[serde(default)]
    pub conversation_id: Option<String>,
    pub connector_id: String,
    pub model_id: String,
    pub display_name: String,
    pub source: String,
    pub availability: String,
    #[serde(default)]
    pub is_default: bool,
    #[serde(default)]
    pub sort_order: u64,
    #[serde(default)]
    pub catalog_revision: Option<String>,
    #[serde(default)]
    pub context_window: Option<u64>,
    #[serde(default)]
    pub reasoning_efforts: Vec<String>,
    #[serde(default)]
    pub service_tiers: Vec<String>,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RetrievalSourceExport {
    pub id: String,
    #[serde(alias = "scope")]
    pub scope_id: String,
    pub citation: String,
    pub sha256: String,
    pub token_count: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AuditTimestampExport {
    pub entity_type: String,
    pub entity_id: String,
    pub created_at: String,
    pub updated_at: String,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelCandidateExport {
    pub id: String,
    pub agent_id: String,
    pub connector_id: String,
    pub model_id: String,
    pub available: bool,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceAuthorizationExport {
    pub project_id: String,
    pub canonical_root: String,
    pub revision: u64,
    pub validation_status: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MigrationReport {
    pub source_schema_checksum: String,
    pub export_sha256: String,
    pub row_counts: BTreeMap<String, u64>,
    pub legacy_run_count: u64,
    pub warnings: Vec<String>,
    pub dry_run: bool,
}

#[derive(Debug, Error)]
pub enum MigrationError {
    #[error("invalid migration export: {0}")]
    Invalid(String),
    #[error("SQLite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("migration export contains a secret-like field")]
    SecretField,
}

pub fn dry_run(export: &LegacyExport) -> Result<MigrationReport, MigrationError> {
    validate(export)?;
    Ok(report(export, true))
}

pub struct MigrationStore {
    connection: Connection,
}

impl MigrationStore {
    pub fn open_in_memory() -> Result<Self, MigrationError> {
        Self::open(":memory:")
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self, MigrationError> {
        let connection = Connection::open(path)?;
        let store = Self { connection };
        store.connection.pragma_update(None, "foreign_keys", "ON")?;
        store
            .connection
            .pragma_update(None, "journal_mode", "WAL")?;
        store
            .connection
            .busy_timeout(std::time::Duration::from_millis(5000))?;
        store.connection.execute_batch(SCHEMA_SQL)?;
        ensure_column(
            &store.connection,
            "collaboration_runs",
            "auto_dispatch_handoffs",
            "ALTER TABLE collaboration_runs ADD COLUMN auto_dispatch_handoffs INTEGER NOT NULL DEFAULT 0",
        )?;
        ensure_column(
            &store.connection,
            "handoffs",
            "details_json",
            "ALTER TABLE handoffs ADD COLUMN details_json TEXT",
        )?;
        for (table, column, alter_sql) in [
            (
                "agents",
                "connector_id",
                "ALTER TABLE agents ADD COLUMN connector_id TEXT",
            ),
            (
                "agents",
                "model_id",
                "ALTER TABLE agents ADD COLUMN model_id TEXT",
            ),
            (
                "agents",
                "candidate_model_list_revision",
                "ALTER TABLE agents ADD COLUMN candidate_model_list_revision INTEGER NOT NULL DEFAULT 0",
            ),
            (
                "project_agents",
                "model_selection_mode",
                "ALTER TABLE project_agents ADD COLUMN model_selection_mode TEXT NOT NULL DEFAULT 'inherit'",
            ),
            (
                "project_agents",
                "model_id",
                "ALTER TABLE project_agents ADD COLUMN model_id TEXT",
            ),
            (
                "project_agents",
                "candidate_model_list_mode",
                "ALTER TABLE project_agents ADD COLUMN candidate_model_list_mode TEXT NOT NULL DEFAULT 'inherit'",
            ),
            (
                "project_agents",
                "candidate_model_list_revision",
                "ALTER TABLE project_agents ADD COLUMN candidate_model_list_revision INTEGER NOT NULL DEFAULT 0",
            ),
            (
                "conversation_agents",
                "model_selection_mode",
                "ALTER TABLE conversation_agents ADD COLUMN model_selection_mode TEXT NOT NULL DEFAULT 'inherit'",
            ),
            (
                "conversation_agents",
                "model_id",
                "ALTER TABLE conversation_agents ADD COLUMN model_id TEXT",
            ),
            (
                "conversation_agents",
                "candidate_model_list_mode",
                "ALTER TABLE conversation_agents ADD COLUMN candidate_model_list_mode TEXT NOT NULL DEFAULT 'inherit'",
            ),
            (
                "conversation_agents",
                "candidate_model_list_revision",
                "ALTER TABLE conversation_agents ADD COLUMN candidate_model_list_revision INTEGER NOT NULL DEFAULT 0",
            ),
        ] {
            ensure_column(&store.connection, table, column, alter_sql)?;
        }
        Ok(store)
    }

    pub fn apply(&mut self, export: &LegacyExport) -> Result<MigrationReport, MigrationError> {
        validate(export)?;
        let serialized = serde_json::to_string(export)?;
        let export_sha256 = digest(&serialized);
        let tx = self.connection.transaction()?;
        if let Some(existing_sha256) = tx
            .query_row(
                "SELECT export_sha256 FROM migration_meta WHERE source_schema_checksum = ?1",
                params![export.source_schema_checksum],
                |row| row.get::<_, String>(0),
            )
            .optional()?
        {
            if existing_sha256 == export_sha256 {
                let mut report = report(export, false);
                report
                    .warnings
                    .push("identical export was already applied; apply was a no-op".into());
                return Ok(report);
            }
            return Err(MigrationError::Invalid(
                "source schema checksum already has a different export; refusing to overwrite"
                    .into(),
            ));
        }
        tx.execute("INSERT INTO migration_meta(source_schema_checksum, export_sha256, applied_at) VALUES(?1,?2,strftime('%s','now'))", params![export.source_schema_checksum, export_sha256])?;
        for row in &export.projects {
            tx.execute(
                "INSERT INTO projects(id,name,root_path,archived) VALUES(?1,?2,?3,?4)",
                params![row.id, row.name, row.root_path, row.archived],
            )?;
        }
        for row in &export.agents {
            tx.execute(
                "INSERT INTO agents(
                    id,name,role,specialty,system_prompt,connector_id,model_id,
                    candidate_model_list_revision
                 ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
                params![
                    row.id,
                    row.name,
                    row.role,
                    row.specialty,
                    row.system_prompt,
                    row.connector_id,
                    row.model_id,
                    row.candidate_model_list_revision,
                ],
            )?;
        }
        for row in &export.project_agents {
            tx.execute(
                "INSERT INTO project_agents(
                    project_id,agent_id,role,specialty,system_prompt,enabled,
                    workspace_access,model_selection_mode,model_id,
                    candidate_model_list_mode,candidate_model_list_revision
                 ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
                params![
                    row.project_id,
                    row.agent_id,
                    row.role,
                    row.specialty,
                    row.system_prompt,
                    row.enabled,
                    row.workspace_access,
                    row.model_selection_mode,
                    row.model_id,
                    row.candidate_model_list_mode,
                    row.candidate_model_list_revision,
                ],
            )?;
        }
        for row in &export.conversations {
            tx.execute(
                "INSERT INTO conversations(id,project_id,title,scope_revision) VALUES(?1,?2,?3,?4)",
                params![row.id, row.project_id, row.title, row.scope_revision],
            )?;
        }
        for row in &export.conversation_agents {
            tx.execute(
                "INSERT INTO conversation_agents(
                    conversation_id,agent_id,role,specialty,system_prompt,enabled,
                    model_selection_mode,model_id,candidate_model_list_mode,
                    candidate_model_list_revision
                 ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
                params![
                    row.conversation_id,
                    row.agent_id,
                    row.role,
                    row.specialty,
                    row.system_prompt,
                    row.enabled,
                    row.model_selection_mode,
                    row.model_id,
                    row.candidate_model_list_mode,
                    row.candidate_model_list_revision,
                ],
            )?;
        }
        for row in &export.messages {
            tx.execute("INSERT INTO messages(id,conversation_id,sender_id,sequence,content) VALUES(?1,?2,?3,?4,?5)", params![row.id,row.conversation_id,row.sender_id,row.sequence,row.content])?;
        }
        for row in &export.collaboration_runs {
            let project_id = collaboration_project_id(export, row)?;
            let (root_agent_ids_json, _) = collaboration_root_agent_ids(row)?;
            tx.execute(
                "INSERT INTO collaboration_runs(
                    id, project_id, root_agent_ids_json, call_count, max_calls,
                    depth, max_depth, status, stop_reason, auto_dispatch_handoffs
                 ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
                params![
                    row.id,
                    project_id,
                    root_agent_ids_json,
                    row.call_count,
                    row.max_calls,
                    row.depth,
                    row.max_depth,
                    row.status,
                    row.stop_reason,
                    row.auto_dispatch_handoffs,
                ],
            )?;
        }
        for row in &export.execution_runs {
            tx.execute("INSERT INTO execution_runs(id,collaboration_run_id,project_id,conversation_id,agent_id,status,version,legacy) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)", params![row.id,row.collaboration_run_id,row.project_id,row.conversation_id,row.agent_id,row.status,row.version,row.legacy])?;
        }
        for row in &export.handoffs {
            tx.execute("INSERT INTO handoffs(id,collaboration_run_id,from_execution_run_id,to_agent_id,status,details_json) VALUES(?1,?2,?3,?4,?5,?6)", params![row.id,row.collaboration_run_id,row.from_execution_run_id,row.to_agent_id,row.status,row.details.as_ref().map(serde_json::to_string).transpose()?])?;
        }
        for row in &export.context_manifests {
            tx.execute("INSERT INTO context_manifests(id,execution_run_id,schema_version,bundle_hash) VALUES(?1,?2,?3,?4)", params![row.id,row.execution_run_id,row.schema_version,row.bundle_hash])?;
        }
        for row in &export.summaries {
            tx.execute(
                "INSERT INTO summaries(id,scope_id,version,content_hash,artifact_id) VALUES(?1,?2,?3,?4,?5)",
                params![
                    row.id,
                    row.scope_id,
                    row.version,
                    row.content_hash,
                    row.artifact_id,
                ],
            )?;
        }
        for row in &export.memories {
            tx.execute("INSERT INTO memories(id,scope_id,agent_id,content_hash,confirmed) VALUES(?1,?2,?3,?4,?5)", params![row.id,row.scope_id,row.agent_id,row.content_hash,row.confirmed])?;
        }
        for row in &export.attachments {
            tx.execute(
                "INSERT INTO attachments(attachment_id,artifact_id,message_id,ordinal,file_name,sha256,size)
                 VALUES(?1,?2,?3,?4,?5,?6,?7)",
                params![
                    row.id,
                    row.artifact_id,
                    row.message_id,
                    row.ordinal,
                    row.file_name,
                    row.sha256,
                    row.size,
                ],
            )?;
        }
        for row in &export.workflows {
            tx.execute(
                "INSERT INTO workflows(id,project_id,name,kind,steps_json) VALUES(?1,?2,?3,?4,?5)",
                params![row.id, row.project_id, row.name, row.kind, row.steps_json],
            )?;
        }
        for row in &export.model_snapshots {
            tx.execute(
                "INSERT INTO model_snapshots(run_id,connector_id,model_id,revision) VALUES(?1,?2,?3,?4)",
                params![row.run_id, row.connector_id, row.model_id, row.revision],
            )?;
        }
        for row in &export.model_selection_snapshots {
            let snapshot: ModelSelectionSnapshot =
                serde_json::from_value(row.snapshot_json.clone())?;
            let snapshot_json = serde_json::to_string(&snapshot)?;
            tx.execute(
                "INSERT INTO model_selection_snapshots(run_id,snapshot_json,snapshot_hash)
                 VALUES(?1,?2,?3)",
                params![row.run_id, snapshot_json, digest(&snapshot_json)],
            )?;
        }
        for row in &export.retrieval_sources {
            tx.execute(
                "INSERT INTO retrieval_sources(id,scope_id,citation,sha256,token_count) VALUES(?1,?2,?3,?4,?5)",
                params![row.id, row.scope_id, row.citation, row.sha256, row.token_count],
            )?;
        }
        for row in &export.audit_timestamps {
            tx.execute(
                "INSERT INTO audit_timestamps(entity_type,entity_id,created_at,updated_at) VALUES(?1,?2,?3,?4)",
                params![row.entity_type, row.entity_id, row.created_at, row.updated_at],
            )?;
        }
        for row in &export.model_candidates {
            tx.execute("INSERT INTO model_candidates(id,agent_id,connector_id,model_id,available) VALUES(?1,?2,?3,?4,?5)", params![row.id,row.agent_id,row.connector_id,row.model_id,row.available])?;
        }
        for row in &export.identity_model_options {
            tx.execute(
                "INSERT INTO identity_model_options(
                    id,identity_scope,agent_id,project_id,conversation_id,
                    connector_id,model_id,display_name,source,availability,
                    is_default,sort_order,catalog_revision,context_window,
                    reasoning_efforts_json,service_tiers_json
                 ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)",
                params![
                    row.id,
                    row.identity_scope,
                    row.agent_id,
                    row.project_id,
                    row.conversation_id,
                    row.connector_id,
                    row.model_id,
                    row.display_name,
                    row.source,
                    row.availability,
                    row.is_default,
                    row.sort_order,
                    row.catalog_revision,
                    row.context_window,
                    serde_json::to_string(&row.reasoning_efforts)?,
                    serde_json::to_string(&row.service_tiers)?,
                ],
            )?;
        }
        for row in &export.workspace_authorizations {
            tx.execute("INSERT INTO workspace_authorizations(project_id,canonical_root,revision,validation_status) VALUES(?1,?2,?3,?4)", params![row.project_id,row.canonical_root,row.revision,row.validation_status])?;
        }
        tx.commit()?;
        Ok(report(export, false))
    }

    pub fn count(&self, table: &str) -> Result<u64, MigrationError> {
        let allowed = [
            "projects",
            "agents",
            "project_agents",
            "conversations",
            "conversation_agents",
            "messages",
            "execution_runs",
            "collaboration_runs",
            "handoffs",
            "context_manifests",
            "summaries",
            "memories",
            "attachments",
            "workflows",
            "model_snapshots",
            "model_selection_snapshots",
            "retrieval_sources",
            "audit_timestamps",
            "model_candidates",
            "identity_model_options",
            "workspace_authorizations",
        ];
        if !allowed.contains(&table) {
            return Err(MigrationError::Invalid(
                "table name is not in the migration allowlist".into(),
            ));
        }
        Ok(self
            .connection
            .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                row.get(0)
            })?)
    }
}

fn collaboration_project_id(
    export: &LegacyExport,
    row: &CollaborationRunExport,
) -> Result<String, MigrationError> {
    if let Some(project_id) = row.project_id.as_deref() {
        if project_id.is_empty() {
            return Err(MigrationError::Invalid(format!(
                "collaboration run {} has an empty project id",
                row.id
            )));
        }
        return Ok(project_id.to_owned());
    }

    let project_ids: BTreeSet<&str> = export
        .execution_runs
        .iter()
        .filter(|execution| execution.collaboration_run_id == row.id)
        .map(|execution| execution.project_id.as_str())
        .collect();
    match project_ids.len() {
        1 => Ok((*project_ids.iter().next().expect("set length is one")).to_owned()),
        0 => Err(MigrationError::Invalid(format!(
            "collaboration run {} has no project id and no execution run to derive it from",
            row.id
        ))),
        _ => Err(MigrationError::Invalid(format!(
            "collaboration run {} maps to multiple projects",
            row.id
        ))),
    }
}

fn collaboration_root_agent_ids(
    row: &CollaborationRunExport,
) -> Result<(String, Vec<String>), MigrationError> {
    let serialized = row
        .root_agent_ids_json
        .as_deref()
        .unwrap_or("[]")
        .to_owned();
    let agent_ids: Vec<String> = serde_json::from_str(&serialized)?;
    if agent_ids.iter().any(String::is_empty) {
        return Err(MigrationError::Invalid(format!(
            "collaboration run {} contains an empty root agent id",
            row.id
        )));
    }
    Ok((serialized, agent_ids))
}

fn ensure_column(
    connection: &Connection,
    table: &str,
    column: &str,
    alter_sql: &str,
) -> Result<(), MigrationError> {
    let exists = connection
        .prepare(&format!("PRAGMA table_info({table})"))?
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .any(|name| name == column);
    if !exists {
        connection.execute(alter_sql, [])?;
    }
    Ok(())
}

fn validate(export: &LegacyExport) -> Result<(), MigrationError> {
    if export.source_schema_checksum.len() != 64
        || !export
            .source_schema_checksum
            .chars()
            .all(|character| character.is_ascii_hexdigit())
    {
        return Err(MigrationError::Invalid(
            "source schema checksum must be a 64-character hexadecimal digest".into(),
        ));
    }
    let serialized = serde_json::to_string(export)?;
    reject_secret_like(&serialized)?;

    ensure_unique(export.projects.iter().map(|row| row.id.clone()), "project")?;
    ensure_unique(export.agents.iter().map(|row| row.id.clone()), "agent")?;
    ensure_unique(
        export.conversations.iter().map(|row| row.id.clone()),
        "conversation",
    )?;
    ensure_unique(export.messages.iter().map(|row| row.id.clone()), "message")?;
    ensure_unique(
        export.collaboration_runs.iter().map(|row| row.id.clone()),
        "collaboration run",
    )?;
    ensure_unique(
        export.execution_runs.iter().map(|row| row.id.clone()),
        "execution run",
    )?;
    ensure_unique(export.handoffs.iter().map(|row| row.id.clone()), "handoff")?;
    ensure_unique(
        export.context_manifests.iter().map(|row| row.id.clone()),
        "context manifest",
    )?;
    ensure_unique(export.summaries.iter().map(|row| row.id.clone()), "summary")?;
    ensure_unique(export.memories.iter().map(|row| row.id.clone()), "memory")?;
    ensure_unique(
        export
            .attachments
            .iter()
            .map(|row| format!("{}:{}", row.message_id, row.ordinal)),
        "attachment",
    )?;
    ensure_unique(
        export.workflows.iter().map(|row| row.id.clone()),
        "workflow",
    )?;
    ensure_unique(
        export.model_snapshots.iter().map(|row| row.run_id.clone()),
        "model snapshot",
    )?;
    ensure_unique(
        export
            .model_selection_snapshots
            .iter()
            .map(|row| row.run_id.clone()),
        "model selection snapshot",
    )?;
    ensure_unique(
        export.retrieval_sources.iter().map(|row| row.id.clone()),
        "retrieval source",
    )?;
    ensure_unique(
        export
            .audit_timestamps
            .iter()
            .map(|row| format!("{}:{}", row.entity_type, row.entity_id)),
        "audit timestamp",
    )?;
    ensure_unique(
        export.model_candidates.iter().map(|row| row.id.clone()),
        "model candidate",
    )?;
    ensure_unique(
        export
            .identity_model_options
            .iter()
            .map(|row| row.id.clone()),
        "identity model option",
    )?;
    ensure_unique(
        export.identity_model_options.iter().map(|row| {
            format!(
                "{}:{}:{}:{}:{}:{}",
                row.identity_scope,
                row.agent_id,
                row.project_id.as_deref().unwrap_or(""),
                row.conversation_id.as_deref().unwrap_or(""),
                row.connector_id,
                row.model_id
            )
        }),
        "identity model target/model",
    )?;
    ensure_unique(
        export
            .workspace_authorizations
            .iter()
            .map(|row| row.project_id.clone()),
        "workspace authorization",
    )?;

    let projects: BTreeSet<&str> = export.projects.iter().map(|row| row.id.as_str()).collect();
    let agents: BTreeSet<&str> = export.agents.iter().map(|row| row.id.as_str()).collect();
    let collaboration_runs: BTreeSet<&str> = export
        .collaboration_runs
        .iter()
        .map(|row| row.id.as_str())
        .collect();
    let conversations: BTreeSet<&str> = export
        .conversations
        .iter()
        .map(|row| row.id.as_str())
        .collect();
    let messages: BTreeSet<&str> = export.messages.iter().map(|row| row.id.as_str()).collect();
    let execution_runs: BTreeSet<&str> = export
        .execution_runs
        .iter()
        .map(|row| row.id.as_str())
        .collect();
    let collaboration_run_ids = collaboration_runs.clone();
    let summary_ids: BTreeSet<&str> = export.summaries.iter().map(|row| row.id.as_str()).collect();
    let memory_ids: BTreeSet<&str> = export.memories.iter().map(|row| row.id.as_str()).collect();
    let workflow_ids: BTreeSet<&str> = export.workflows.iter().map(|row| row.id.as_str()).collect();
    let valid_scope_ids: BTreeSet<&str> = projects
        .iter()
        .chain(conversations.iter())
        .chain(agents.iter())
        .copied()
        .collect();
    let valid_retrieval_scope_ids: BTreeSet<&str> = projects
        .iter()
        .chain(conversations.iter())
        .copied()
        .collect();

    for row in &export.collaboration_runs {
        let project_id = collaboration_project_id(export, row)?;
        if !projects.contains(project_id.as_str()) {
            return Err(MigrationError::Invalid(format!(
                "collaboration run {} references missing project {}",
                row.id, project_id
            )));
        }
        let (_, root_agent_ids) = collaboration_root_agent_ids(row)?;
        for agent_id in root_agent_ids {
            if !export.project_agents.iter().any(|assignment| {
                assignment.project_id == project_id
                    && assignment.agent_id == agent_id
                    && assignment.enabled
            }) {
                return Err(MigrationError::Invalid(format!(
                    "collaboration run {} root agent {} is not in the enabled Project roster",
                    row.id, agent_id
                )));
            }
        }
    }

    for row in &export.conversations {
        if !projects.contains(row.project_id.as_str()) {
            return Err(MigrationError::Invalid(format!(
                "conversation {} references missing project",
                row.id
            )));
        }
    }
    for row in &export.agents {
        if row.connector_id.as_deref().is_some_and(str::is_empty)
            || row.model_id.as_deref().is_some_and(str::is_empty)
        {
            return Err(MigrationError::Invalid(format!(
                "agent {} has an empty connector/model binding",
                row.id
            )));
        }
    }
    for row in &export.project_agents {
        if !projects.contains(row.project_id.as_str()) || !agents.contains(row.agent_id.as_str()) {
            return Err(MigrationError::Invalid(format!(
                "project assignment {}:{} has a missing foreign key",
                row.project_id, row.agent_id
            )));
        }
        validate_assignment_model_fields(
            &row.model_selection_mode,
            row.model_id.as_deref(),
            &row.candidate_model_list_mode,
        )?;
    }
    for row in &export.conversation_agents {
        if !conversations.contains(row.conversation_id.as_str())
            || !agents.contains(row.agent_id.as_str())
        {
            return Err(MigrationError::Invalid(format!(
                "conversation assignment {}:{} has a missing foreign key",
                row.conversation_id, row.agent_id
            )));
        }
        validate_assignment_model_fields(
            &row.model_selection_mode,
            row.model_id.as_deref(),
            &row.candidate_model_list_mode,
        )?;
    }
    for row in &export.messages {
        if !conversations.contains(row.conversation_id.as_str()) {
            return Err(MigrationError::Invalid(format!(
                "message {} references missing conversation",
                row.id
            )));
        }
        reject_secret_like(&row.content)?;
    }
    for row in &export.execution_runs {
        if !collaboration_runs.contains(row.collaboration_run_id.as_str()) {
            return Err(MigrationError::Invalid(format!(
                "execution run {} references missing collaboration run",
                row.id
            )));
        }
        let collaboration = export
            .collaboration_runs
            .iter()
            .find(|collaboration| collaboration.id == row.collaboration_run_id)
            .ok_or_else(|| {
                MigrationError::Invalid(format!(
                    "execution run {} references missing collaboration run",
                    row.id
                ))
            })?;
        if !row.legacy && collaboration_project_id(export, collaboration)? != row.project_id {
            return Err(MigrationError::Invalid(format!(
                "execution run {} has a collaboration run outside its project",
                row.id
            )));
        }
        if !projects.contains(row.project_id.as_str())
            || !conversations.contains(row.conversation_id.as_str())
        {
            return Err(MigrationError::Invalid(format!(
                "execution run {} references a missing scope",
                row.id
            )));
        }
        if !row.legacy
            && export
                .conversations
                .iter()
                .find(|conversation| conversation.id == row.conversation_id)
                .is_some_and(|conversation| conversation.project_id != row.project_id)
        {
            return Err(MigrationError::Invalid(format!(
                "execution run {} has a conversation outside its project",
                row.id
            )));
        }
        if !agents.contains(row.agent_id.as_str()) {
            return Err(MigrationError::Invalid(format!(
                "execution run {} references missing agent",
                row.id
            )));
        }
    }
    for row in &export.handoffs {
        if !collaboration_runs.contains(row.collaboration_run_id.as_str())
            || !execution_runs.contains(row.from_execution_run_id.as_str())
            || !agents.contains(row.to_agent_id.as_str())
        {
            return Err(MigrationError::Invalid(format!(
                "handoff {} has a missing foreign key",
                row.id
            )));
        }
        let project_id = export
            .execution_runs
            .iter()
            .find(|execution| execution.id == row.from_execution_run_id)
            .map(|execution| execution.project_id.as_str())
            .expect("execution run set was checked above");
        if !export.project_agents.iter().any(|assignment| {
            assignment.project_id == project_id
                && assignment.agent_id == row.to_agent_id
                && assignment.enabled
        }) {
            return Err(MigrationError::Invalid(format!(
                "handoff {} target agent is not in the enabled Project roster",
                row.id
            )));
        }
    }
    for row in &export.context_manifests {
        if !execution_runs.contains(row.execution_run_id.as_str()) {
            return Err(MigrationError::Invalid(format!(
                "context manifest {} references missing execution run",
                row.id
            )));
        }
    }
    for row in &export.attachments {
        if !messages.contains(row.message_id.as_str()) {
            return Err(MigrationError::Invalid(format!(
                "attachment {}:{} references missing message",
                row.message_id, row.ordinal
            )));
        }
        if row.file_name.is_empty()
            || row.file_name.contains(['/', '\\'])
            || row.file_name.contains('\0')
        {
            return Err(MigrationError::Invalid(
                "attachment file_name must be a non-empty basename".into(),
            ));
        }
        if !is_sha256(&row.sha256) {
            return Err(MigrationError::Invalid(
                "attachment sha256 must be a 64-character hexadecimal digest".into(),
            ));
        }
        for (field, value) in [
            ("id", row.id.as_deref()),
            ("artifact_id", row.artifact_id.as_deref()),
        ] {
            if let Some(value) = value {
                if value.trim().is_empty() || value.len() > 128 {
                    return Err(MigrationError::Invalid(format!(
                        "attachment {field} must be 1..=128 bytes when present"
                    )));
                }
            }
        }
    }
    for row in &export.workflows {
        if !projects.contains(row.project_id.as_str()) {
            return Err(MigrationError::Invalid(format!(
                "workflow {} references missing project",
                row.id
            )));
        }
        if row.name.is_empty() || row.kind.is_empty() {
            return Err(MigrationError::Invalid(
                "workflow name and kind must be non-empty".into(),
            ));
        }
        serde_json::from_str::<serde_json::Value>(&row.steps_json).map_err(|_| {
            MigrationError::Invalid(format!("workflow {} has invalid steps_json", row.id))
        })?;
    }
    for row in &export.model_snapshots {
        if !execution_runs.contains(row.run_id.as_str()) {
            return Err(MigrationError::Invalid(format!(
                "model snapshot references missing execution run {}",
                row.run_id
            )));
        }
        for value in [&row.connector_id, &row.model_id] {
            if value.as_deref().is_some_and(str::is_empty) {
                return Err(MigrationError::Invalid(
                    "model snapshot identifiers must be non-empty when present".into(),
                ));
            }
        }
    }
    for row in &export.model_selection_snapshots {
        if !execution_runs.contains(row.run_id.as_str()) {
            return Err(MigrationError::Invalid(format!(
                "model selection snapshot references missing execution run {}",
                row.run_id
            )));
        }
        let snapshot: ModelSelectionSnapshot = serde_json::from_value(row.snapshot_json.clone())
            .map_err(|error| {
                MigrationError::Invalid(format!(
                    "model selection snapshot {} is invalid: {error}",
                    row.run_id
                ))
            })?;
        if snapshot.run_id != row.run_id {
            return Err(MigrationError::Invalid(format!(
                "model selection snapshot {} has a mismatched runId",
                row.run_id
            )));
        }
    }
    for row in &export.memories {
        if !valid_scope_ids.contains(row.scope_id.as_str()) {
            return Err(MigrationError::Invalid(format!(
                "memory {} references missing scope",
                row.id
            )));
        }
        if let Some(agent_id) = row.agent_id.as_deref() {
            if !agents.contains(agent_id) {
                return Err(MigrationError::Invalid(format!(
                    "memory {} references missing agent",
                    row.id
                )));
            }
        }
    }
    for row in &export.retrieval_sources {
        if !valid_retrieval_scope_ids.contains(row.scope_id.as_str()) {
            return Err(MigrationError::Invalid(format!(
                "retrieval source {} references missing scope",
                row.id
            )));
        }
        if row.citation.is_empty() || !is_sha256(&row.sha256) {
            return Err(MigrationError::Invalid(
                "retrieval metadata requires citation and a 64-character sha256".into(),
            ));
        }
    }
    for row in &export.audit_timestamps {
        let exists = match row.entity_type.as_str() {
            "project" => projects.contains(row.entity_id.as_str()),
            "agent" => agents.contains(row.entity_id.as_str()),
            "conversation" => conversations.contains(row.entity_id.as_str()),
            "message" => messages.contains(row.entity_id.as_str()),
            "execution_run" => execution_runs.contains(row.entity_id.as_str()),
            "collaboration_run" => collaboration_run_ids.contains(row.entity_id.as_str()),
            "workflow" => workflow_ids.contains(row.entity_id.as_str()),
            "summary" => summary_ids.contains(row.entity_id.as_str()),
            "memory" => memory_ids.contains(row.entity_id.as_str()),
            _ => false,
        };
        if !exists || row.created_at.is_empty() || row.updated_at.is_empty() {
            return Err(MigrationError::Invalid(format!(
                "audit timestamp {}:{} has an invalid entity reference or timestamp",
                row.entity_type, row.entity_id
            )));
        }
    }
    for row in &export.workspace_authorizations {
        if !projects.contains(row.project_id.as_str()) {
            return Err(MigrationError::Invalid(format!(
                "workspace authorization references missing project {}",
                row.project_id
            )));
        }
    }
    for row in &export.model_candidates {
        if !agents.contains(row.agent_id.as_str()) {
            return Err(MigrationError::Invalid(format!(
                "model candidate {} references missing agent",
                row.id
            )));
        }
    }
    let mut default_targets = BTreeSet::new();
    for row in &export.identity_model_options {
        if !agents.contains(row.agent_id.as_str()) {
            return Err(MigrationError::Invalid(format!(
                "identity model option {} references missing agent",
                row.id
            )));
        }
        let target_exists = match row.identity_scope.as_str() {
            "base_agent" => row.project_id.is_none() && row.conversation_id.is_none(),
            "project_agent" => {
                row.conversation_id.is_none()
                    && row.project_id.as_deref().is_some_and(|project_id| {
                        export.project_agents.iter().any(|assignment| {
                            assignment.project_id == project_id
                                && assignment.agent_id == row.agent_id
                        })
                    })
            }
            "conversation_agent" => {
                row.project_id.is_none()
                    && row
                        .conversation_id
                        .as_deref()
                        .is_some_and(|conversation_id| {
                            export.conversation_agents.iter().any(|assignment| {
                                assignment.conversation_id == conversation_id
                                    && assignment.agent_id == row.agent_id
                            })
                        })
            }
            _ => false,
        };
        if !target_exists {
            return Err(MigrationError::Invalid(format!(
                "identity model option {} has an invalid identity target",
                row.id
            )));
        }
        if row.connector_id.is_empty()
            || row.model_id.is_empty()
            || row.display_name.is_empty()
            || !matches!(row.source.as_str(), "runtime" | "config" | "manual")
            || !matches!(
                row.availability.as_str(),
                "available" | "unverified" | "unavailable"
            )
            || row.catalog_revision.as_deref().is_some_and(str::is_empty)
            || row.context_window.is_some_and(|window| window == 0)
            || row.reasoning_efforts.iter().any(String::is_empty)
            || row.service_tiers.iter().any(String::is_empty)
        {
            return Err(MigrationError::Invalid(format!(
                "identity model option {} has invalid metadata",
                row.id
            )));
        }
        if row.is_default {
            let target = format!(
                "{}:{}:{}:{}:{}",
                row.identity_scope,
                row.agent_id,
                row.project_id.as_deref().unwrap_or(""),
                row.conversation_id.as_deref().unwrap_or(""),
                row.connector_id
            );
            if !default_targets.insert(target) {
                return Err(MigrationError::Invalid(
                    "identity model target has more than one default option".into(),
                ));
            }
        }
    }
    Ok(())
}

fn validate_assignment_model_fields(
    selection_mode: &str,
    model_id: Option<&str>,
    candidate_list_mode: &str,
) -> Result<(), MigrationError> {
    if !matches!(selection_mode, "inherit" | "connector_default" | "pinned") {
        return Err(MigrationError::Invalid(
            "assignment model selection mode is invalid".into(),
        ));
    }
    if selection_mode == "pinned" {
        if model_id.is_none_or(str::is_empty) {
            return Err(MigrationError::Invalid(
                "pinned assignment model selection requires a model id".into(),
            ));
        }
    } else if model_id.is_some() {
        return Err(MigrationError::Invalid(
            "non-pinned assignment model selection must not include a model id".into(),
        ));
    }
    if !matches!(candidate_list_mode, "inherit" | "override") {
        return Err(MigrationError::Invalid(
            "assignment candidate model list mode is invalid".into(),
        ));
    }
    Ok(())
}

fn ensure_unique<I, S>(ids: I, label: &str) -> Result<(), MigrationError>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let mut seen = BTreeSet::new();
    for id in ids {
        let id = id.into();
        if !seen.insert(id) {
            return Err(MigrationError::Invalid(format!(
                "duplicate {label} identifier in export"
            )));
        }
    }
    Ok(())
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64 && value.chars().all(|character| character.is_ascii_hexdigit())
}

fn reject_secret_like(value: &str) -> Result<(), MigrationError> {
    let lower = value.to_ascii_lowercase();
    let markers = [
        "authorization: bearer ",
        "authorization= bearer ",
        "api_key=",
        "api_key:",
        "api-key=",
        "api-key:",
        "api key=",
        "api key:",
        "password=",
        "password:",
        "secret=",
        "secret:",
        "token=",
        "token:",
        "credential=",
        "credential:",
        "client_secret=",
        "client_secret:",
        "private_key=",
        "private_key:",
        "bearer ",
        "sk-",
        "akia",
    ];
    if markers.iter().any(|marker| lower.contains(marker)) {
        return Err(MigrationError::SecretField);
    }
    Ok(())
}

fn report(export: &LegacyExport, dry_run: bool) -> MigrationReport {
    let mut row_counts = BTreeMap::new();
    row_counts.insert("projects".into(), export.projects.len() as u64);
    row_counts.insert("agents".into(), export.agents.len() as u64);
    row_counts.insert("project_agents".into(), export.project_agents.len() as u64);
    row_counts.insert("conversations".into(), export.conversations.len() as u64);
    row_counts.insert(
        "conversation_agents".into(),
        export.conversation_agents.len() as u64,
    );
    row_counts.insert("messages".into(), export.messages.len() as u64);
    row_counts.insert("execution_runs".into(), export.execution_runs.len() as u64);
    row_counts.insert(
        "collaboration_runs".into(),
        export.collaboration_runs.len() as u64,
    );
    row_counts.insert("handoffs".into(), export.handoffs.len() as u64);
    row_counts.insert(
        "context_manifests".into(),
        export.context_manifests.len() as u64,
    );
    row_counts.insert("summaries".into(), export.summaries.len() as u64);
    row_counts.insert("memories".into(), export.memories.len() as u64);
    row_counts.insert("attachments".into(), export.attachments.len() as u64);
    row_counts.insert("workflows".into(), export.workflows.len() as u64);
    row_counts.insert(
        "model_snapshots".into(),
        export.model_snapshots.len() as u64,
    );
    row_counts.insert(
        "model_selection_snapshots".into(),
        export.model_selection_snapshots.len() as u64,
    );
    row_counts.insert(
        "retrieval_sources".into(),
        export.retrieval_sources.len() as u64,
    );
    row_counts.insert(
        "audit_timestamps".into(),
        export.audit_timestamps.len() as u64,
    );
    row_counts.insert(
        "model_candidates".into(),
        export.model_candidates.len() as u64,
    );
    row_counts.insert(
        "identity_model_options".into(),
        export.identity_model_options.len() as u64,
    );
    row_counts.insert(
        "workspace_authorizations".into(),
        export.workspace_authorizations.len() as u64,
    );
    let serialized = serde_json::to_string(export).unwrap_or_default();
    MigrationReport {
        source_schema_checksum: export.source_schema_checksum.clone(),
        export_sha256: digest(&serialized),
        row_counts,
        legacy_run_count: export
            .execution_runs
            .iter()
            .filter(|row| row.legacy)
            .count() as u64,
        warnings: vec![
            "Secrets are excluded by the typed export contract; PostgreSQL remains untouched."
                .into(),
        ],
        dry_run,
    }
}

fn digest(value: &str) -> String {
    Sha256::digest(value.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

const SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS migration_meta(
    source_schema_checksum TEXT PRIMARY KEY,
    export_sha256 TEXT NOT NULL,
    applied_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS projects(
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    root_path TEXT,
    archived INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS agents(
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    role TEXT NOT NULL,
    specialty TEXT NOT NULL,
    system_prompt TEXT NOT NULL,
    connector_id TEXT,
    model_id TEXT,
    candidate_model_list_revision INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS project_agents(
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
CREATE TABLE IF NOT EXISTS conversations(
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    title TEXT NOT NULL,
    scope_revision INTEGER NOT NULL,
    FOREIGN KEY(project_id) REFERENCES projects(id)
);
CREATE TABLE IF NOT EXISTS conversation_agents(
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
CREATE TABLE IF NOT EXISTS messages(
    id TEXT PRIMARY KEY,
    conversation_id TEXT NOT NULL,
    sender_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    content TEXT NOT NULL,
    FOREIGN KEY(conversation_id) REFERENCES conversations(id)
);
CREATE TABLE IF NOT EXISTS collaboration_runs(
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
CREATE TABLE IF NOT EXISTS execution_runs(
    id TEXT PRIMARY KEY,
    collaboration_run_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    status TEXT NOT NULL,
    version INTEGER NOT NULL,
    legacy INTEGER NOT NULL,
    FOREIGN KEY(collaboration_run_id) REFERENCES collaboration_runs(id),
    FOREIGN KEY(project_id) REFERENCES projects(id),
    FOREIGN KEY(conversation_id) REFERENCES conversations(id),
    FOREIGN KEY(agent_id) REFERENCES agents(id)
);
CREATE TABLE IF NOT EXISTS handoffs(
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
CREATE TABLE IF NOT EXISTS context_manifests(
    id TEXT PRIMARY KEY,
    execution_run_id TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    bundle_hash TEXT NOT NULL,
    source_ledger_json TEXT NOT NULL DEFAULT '[]',
    FOREIGN KEY(execution_run_id) REFERENCES execution_runs(id)
);
CREATE TABLE IF NOT EXISTS summaries(
    id TEXT PRIMARY KEY,
    scope_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    content_hash TEXT NOT NULL,
    artifact_id TEXT,
    FOREIGN KEY(scope_id) REFERENCES conversations(id)
);
CREATE TABLE IF NOT EXISTS memories(
    id TEXT PRIMARY KEY,
    scope_id TEXT NOT NULL,
    agent_id TEXT,
    content_hash TEXT NOT NULL,
    confirmed INTEGER NOT NULL,
    FOREIGN KEY(agent_id) REFERENCES agents(id)
);
CREATE TABLE IF NOT EXISTS attachments(
    attachment_id TEXT,
    artifact_id TEXT,
    message_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    file_name TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    size INTEGER NOT NULL,
    PRIMARY KEY(message_id, ordinal),
    FOREIGN KEY(message_id) REFERENCES messages(id)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_attachments_attachment_id
    ON attachments(attachment_id) WHERE attachment_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_attachments_artifact_id ON attachments(artifact_id);
CREATE TABLE IF NOT EXISTS workflows(
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    name TEXT NOT NULL,
    kind TEXT NOT NULL,
    steps_json TEXT NOT NULL,
    FOREIGN KEY(project_id) REFERENCES projects(id)
);
CREATE TABLE IF NOT EXISTS model_snapshots(
    run_id TEXT PRIMARY KEY,
    connector_id TEXT,
    model_id TEXT,
    revision INTEGER,
    FOREIGN KEY(run_id) REFERENCES execution_runs(id)
);
CREATE TABLE IF NOT EXISTS model_selection_snapshots(
    run_id TEXT PRIMARY KEY,
    snapshot_json TEXT NOT NULL CHECK(json_valid(snapshot_json)),
    snapshot_hash TEXT NOT NULL,
    FOREIGN KEY(run_id) REFERENCES execution_runs(id)
);
CREATE TABLE IF NOT EXISTS retrieval_sources(
    id TEXT PRIMARY KEY,
    scope_id TEXT NOT NULL,
    citation TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    token_count INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS retrieval_selections(
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
CREATE TABLE IF NOT EXISTS retrieval_feedback(
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
CREATE TABLE IF NOT EXISTS audit_timestamps(
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(entity_type, entity_id)
);
CREATE TABLE IF NOT EXISTS model_candidates(
    id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    connector_id TEXT NOT NULL,
    model_id TEXT NOT NULL,
    available INTEGER NOT NULL,
    FOREIGN KEY(agent_id) REFERENCES agents(id)
);
CREATE TABLE IF NOT EXISTS identity_model_options(
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
CREATE UNIQUE INDEX IF NOT EXISTS idx_identity_model_options_target_model
    ON identity_model_options(identity_scope, agent_id, COALESCE(project_id, ''), COALESCE(conversation_id, ''), connector_id, model_id);
CREATE TABLE IF NOT EXISTS workspace_authorizations(
    project_id TEXT PRIMARY KEY,
    canonical_root TEXT NOT NULL,
    revision INTEGER NOT NULL,
    validation_status TEXT NOT NULL,
    FOREIGN KEY(project_id) REFERENCES projects(id)
);
"#;

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> LegacyExport {
        LegacyExport {
            source_schema_checksum:
                "1111111111111111111111111111111111111111111111111111111111111111".into(),
            projects: vec![ProjectExport {
                id: "project-1".into(),
                name: "Migration fixture".into(),
                root_path: None,
                archived: false,
            }],
            agents: vec![AgentExport {
                id: "agent-1".into(),
                name: "Fixture agent".into(),
                role: "builder".into(),
                specialty: "migration".into(),
                system_prompt: "fixture prompt".into(),
                connector_id: None,
                model_id: None,
                candidate_model_list_revision: 0,
            }],
            project_agents: vec![ProjectAgentExport {
                project_id: "project-1".into(),
                agent_id: "agent-1".into(),
                role: Some("builder".into()),
                specialty: Some("migration".into()),
                system_prompt: Some("fixture assignment".into()),
                enabled: true,
                workspace_access: "read_only".into(),
                model_selection_mode: "inherit".into(),
                model_id: None,
                candidate_model_list_mode: "inherit".into(),
                candidate_model_list_revision: 0,
            }],
            conversations: vec![ConversationExport {
                id: "conversation-1".into(),
                project_id: "project-1".into(),
                title: "Migration conversation".into(),
                scope_revision: 1,
            }],
            conversation_agents: vec![ConversationAgentExport {
                conversation_id: "conversation-1".into(),
                agent_id: "agent-1".into(),
                role: Some("builder".into()),
                specialty: Some("migration".into()),
                system_prompt: Some("fixture conversation assignment".into()),
                enabled: true,
                model_selection_mode: "inherit".into(),
                model_id: None,
                candidate_model_list_mode: "inherit".into(),
                candidate_model_list_revision: 0,
            }],
            messages: vec![MessageExport {
                id: "message-1".into(),
                conversation_id: "conversation-1".into(),
                sender_id: "agent-1".into(),
                sequence: 1,
                content: "migration fixture message".into(),
            }],
            execution_runs: vec![ExecutionRunExport {
                id: "run-1".into(),
                collaboration_run_id: "collab-1".into(),
                project_id: "project-1".into(),
                conversation_id: "conversation-1".into(),
                agent_id: "agent-1".into(),
                status: "completed".into(),
                version: 3,
                legacy: true,
            }],
            collaboration_runs: vec![CollaborationRunExport {
                id: "collab-1".into(),
                status: "completed".into(),
                call_count: 1,
                max_calls: 8,
                max_depth: 5,
                project_id: None,
                root_agent_ids_json: None,
                depth: 0,
                stop_reason: None,
                auto_dispatch_handoffs: false,
            }],
            handoffs: Vec::new(),
            context_manifests: vec![ContextManifestExport {
                id: "manifest-1".into(),
                execution_run_id: "run-1".into(),
                schema_version: "context.v2".into(),
                bundle_hash: "2222222222222222222222222222222222222222222222222222222222222222"
                    .into(),
            }],
            summaries: vec![SummaryExport {
                id: "summary-1".into(),
                scope_id: "conversation-1".into(),
                version: 1,
                content_hash: "3333333333333333333333333333333333333333333333333333333333333333"
                    .into(),
                artifact_id: None,
            }],
            memories: vec![MemoryExport {
                id: "memory-1".into(),
                scope_id: "project-1".into(),
                agent_id: Some("agent-1".into()),
                content_hash: "4444444444444444444444444444444444444444444444444444444444444444"
                    .into(),
                confirmed: true,
            }],
            attachments: vec![AttachmentExport {
                id: Some("attachment-1".into()),
                artifact_id: None,
                message_id: "message-1".into(),
                ordinal: 0,
                file_name: "fixture.txt".into(),
                sha256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                size: 12,
            }],
            workflows: vec![WorkflowTemplateExport {
                id: "workflow-1".into(),
                project_id: "project-1".into(),
                name: "Migration workflow".into(),
                kind: "sequential".into(),
                steps_json: "[]".into(),
            }],
            model_snapshots: vec![ModelSnapshotExport {
                run_id: "run-1".into(),
                connector_id: None,
                model_id: None,
                revision: Some(1),
            }],
            model_selection_snapshots: Vec::new(),
            retrieval_sources: vec![RetrievalSourceExport {
                id: "retrieval-1".into(),
                scope_id: "project-1".into(),
                citation: "fixture citation".into(),
                sha256: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into(),
                token_count: 4,
            }],
            audit_timestamps: vec![
                AuditTimestampExport {
                    entity_type: "project".into(),
                    entity_id: "project-1".into(),
                    created_at: "2026-08-07T00:00:00Z".into(),
                    updated_at: "2026-08-07T00:00:00Z".into(),
                },
                AuditTimestampExport {
                    entity_type: "message".into(),
                    entity_id: "message-1".into(),
                    created_at: "2026-08-07T00:00:00Z".into(),
                    updated_at: "2026-08-07T00:00:00Z".into(),
                },
            ],
            model_candidates: vec![ModelCandidateExport {
                id: "candidate-1".into(),
                agent_id: "agent-1".into(),
                connector_id: "fixture-connector".into(),
                model_id: "fixture-model".into(),
                available: true,
            }],
            identity_model_options: Vec::new(),
            workspace_authorizations: vec![WorkspaceAuthorizationExport {
                project_id: "project-1".into(),
                canonical_root: r"C:\agenttalk-migration-fixture".into(),
                revision: 1,
                validation_status: "validated".into(),
            }],
        }
    }

    fn model_selection_fixture() -> LegacyExport {
        let mut export = fixture();
        export.agents[0].connector_id = Some("connector-1".into());
        export.agents[0].model_id = Some("base-model".into());
        export.agents[0].candidate_model_list_revision = 7;
        export.project_agents[0].model_selection_mode = "pinned".into();
        export.project_agents[0].model_id = Some("project-model".into());
        export.project_agents[0].candidate_model_list_mode = "override".into();
        export.project_agents[0].candidate_model_list_revision = 9;
        export.conversation_agents[0].candidate_model_list_mode = "override".into();
        export.conversation_agents[0].candidate_model_list_revision = 11;
        export
            .identity_model_options
            .push(IdentityModelOptionExport {
                id: "option-1".into(),
                identity_scope: "project_agent".into(),
                agent_id: "agent-1".into(),
                project_id: Some("project-1".into()),
                conversation_id: None,
                connector_id: "connector-1".into(),
                model_id: "project-model".into(),
                display_name: "Project model".into(),
                source: "manual".into(),
                availability: "available".into(),
                is_default: true,
                sort_order: 3,
                catalog_revision: Some("catalog-beta-2026-08".into()),
                context_window: Some(128_000),
                reasoning_efforts: vec!["high".into()],
                service_tiers: vec!["priority".into()],
            });
        export
            .model_selection_snapshots
            .push(ModelSelectionSnapshotExport {
                run_id: "run-1".into(),
                snapshot_json: serde_json::json!({
                    "runId": "run-1",
                    "version": 2,
                    "runtimeType": "local_gateway",
                    "providerType": "codex",
                    "connectorId": "connector-1",
                    "effectiveModelId": "project-model",
                    "selectionSource": "project",
                    "selectionMode": "pinned",
                    "availability": "available",
                    "catalogRevision": "catalog-beta-2026-08",
                    "contextWindow": 128000,
                    "reasoningEfforts": ["high"],
                    "serviceTiers": ["priority"],
                    "candidateModelList": {
                        "scope": "project_agent",
                        "mode": "override",
                        "revision": 9,
                        "hash": "3333333333333333333333333333333333333333333333333333333333333333",
                        "optionCount": 1
                    }
                }),
            });
        export
    }

    #[test]
    fn dry_run_reports_counts_and_legacy_runs_without_writing() {
        let report = dry_run(&fixture()).unwrap();
        assert!(report.dry_run);
        assert_eq!(report.row_counts["projects"], 1);
        assert_eq!(report.legacy_run_count, 1);
    }

    #[test]
    fn apply_is_id_preserving_and_repeatable() {
        let export = fixture();
        let mut store = MigrationStore::open_in_memory().unwrap();
        store.apply(&export).unwrap();
        let report = store.apply(&export).unwrap();
        assert!(report
            .warnings
            .iter()
            .any(|warning| warning.contains("no-op")));
        assert_eq!(store.count("projects").unwrap(), 1);
        assert_eq!(store.count("messages").unwrap(), 1);
        assert_eq!(store.count("collaboration_runs").unwrap(), 1);
        assert_eq!(store.count("execution_runs").unwrap(), 1);
        assert_eq!(store.count("handoffs").unwrap(), 0);
        assert_eq!(store.count("attachments").unwrap(), 1);
        assert_eq!(store.count("workflows").unwrap(), 1);
        assert_eq!(store.count("model_snapshots").unwrap(), 1);
        assert_eq!(store.count("retrieval_sources").unwrap(), 1);
        assert_eq!(store.count("audit_timestamps").unwrap(), 2);
        assert_eq!(store.count("model_candidates").unwrap(), 1);
        assert_eq!(store.count("model_selection_snapshots").unwrap(), 0);
        assert_eq!(store.count("identity_model_options").unwrap(), 0);
    }

    #[test]
    fn model_selection_metadata_round_trips_and_repeat_apply_is_a_no_op() {
        let export = model_selection_fixture();
        let mut store = MigrationStore::open_in_memory().unwrap();
        store.apply(&export).unwrap();

        let agent: (Option<String>, Option<String>, u64) = store
            .connection
            .query_row(
                "SELECT connector_id, model_id, candidate_model_list_revision
                 FROM agents WHERE id = 'agent-1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(
            agent,
            (Some("connector-1".into()), Some("base-model".into()), 7)
        );

        let project: (String, Option<String>, String, u64) = store
            .connection
            .query_row(
                "SELECT model_selection_mode, model_id, candidate_model_list_mode,
                        candidate_model_list_revision
                 FROM project_agents WHERE project_id = 'project-1' AND agent_id = 'agent-1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(
            project,
            (
                "pinned".into(),
                Some("project-model".into()),
                "override".into(),
                9
            )
        );

        let conversation: (String, Option<String>, String, u64) = store
            .connection
            .query_row(
                "SELECT model_selection_mode, model_id, candidate_model_list_mode,
                        candidate_model_list_revision
                 FROM conversation_agents
                 WHERE conversation_id = 'conversation-1' AND agent_id = 'agent-1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(
            conversation,
            ("inherit".into(), None, "override".into(), 11)
        );

        let option: (String, String, String, String, u64, String, String) = store
            .connection
            .query_row(
                "SELECT identity_scope, connector_id, model_id, catalog_revision,
                        context_window, reasoning_efforts_json, service_tiers_json
                 FROM identity_model_options WHERE id = 'option-1'",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(option.0, "project_agent");
        assert_eq!(option.1, "connector-1");
        assert_eq!(option.2, "project-model");
        assert_eq!(option.3, "catalog-beta-2026-08");
        assert_eq!(option.4, 128_000);
        assert_eq!(option.5, r#"["high"]"#);
        assert_eq!(option.6, r#"["priority"]"#);

        let (snapshot_json, snapshot_hash): (String, String) = store
            .connection
            .query_row(
                "SELECT snapshot_json, snapshot_hash FROM model_selection_snapshots
                 WHERE run_id = 'run-1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        let snapshot: ModelSelectionSnapshot = serde_json::from_str(&snapshot_json).unwrap();
        assert_eq!(
            snapshot.catalog_revision.as_deref(),
            Some("catalog-beta-2026-08")
        );
        assert_eq!(snapshot.candidate_model_list.unwrap().revision, 9);
        assert_eq!(snapshot_hash, digest(&snapshot_json));

        let repeat = store.apply(&export).unwrap();
        assert!(repeat
            .warnings
            .iter()
            .any(|warning| warning.contains("no-op")));
        assert_eq!(store.count("identity_model_options").unwrap(), 1);
        assert_eq!(store.count("model_selection_snapshots").unwrap(), 1);
    }

    #[test]
    fn changed_selection_metadata_is_rejected_without_overwriting() {
        let export = model_selection_fixture();
        let mut conflicting = export.clone();
        conflicting.project_agents[0].candidate_model_list_revision = 10;
        let mut store = MigrationStore::open_in_memory().unwrap();
        store.apply(&export).unwrap();
        assert!(matches!(
            store.apply(&conflicting),
            Err(MigrationError::Invalid(message)) if message.contains("different export")
        ));
        let revision: u64 = store
            .connection
            .query_row(
                "SELECT candidate_model_list_revision FROM project_agents
                 WHERE project_id = 'project-1' AND agent_id = 'agent-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(revision, 9);
        assert_eq!(store.count("identity_model_options").unwrap(), 1);
        assert_eq!(store.count("model_selection_snapshots").unwrap(), 1);
    }

    #[test]
    fn apply_round_trips_current_collaboration_and_handoff_fields() {
        let mut export = fixture();
        export.collaboration_runs[0].project_id = Some("project-1".into());
        export.collaboration_runs[0].root_agent_ids_json = Some(r#"["agent-1"]"#.into());
        export.collaboration_runs[0].depth = 2;
        export.collaboration_runs[0].stop_reason = Some("owner-cancelled".into());
        export.collaboration_runs[0].auto_dispatch_handoffs = true;
        export.handoffs.push(HandoffExport {
            id: "handoff-round-trip".into(),
            collaboration_run_id: "collab-1".into(),
            from_execution_run_id: "run-1".into(),
            to_agent_id: "agent-1".into(),
            status: "proposed".into(),
            details: Some(StructuredHandoffDetails {
                parent_execution_run_id: Some("run-1".into()),
                child_execution_run_id: None,
                source_message_id: Some("message-1".into()),
                from_agent_id: Some("agent-1".into()),
                to_agent_id: Some("agent-1".into()),
                kind: Some("review".into()),
                dispatch_mode: Some("manual".into()),
                batch_id: Some("batch-1".into()),
                sequence_index: Some(0),
                detected_by: Some("test".into()),
                task: Some("review".into()),
                reason: Some("test".into()),
                decisions: Some(vec!["keep".into()]),
                constraints: Some(vec!["bounded".into()]),
                artifacts: Some(vec!["diff".into()]),
                expected_output: Some("result".into()),
                context_scope: Some("conversation".into()),
                agent_path: None,
            }),
        });

        let mut store = MigrationStore::open_in_memory().unwrap();
        store.apply(&export).unwrap();
        assert_eq!(store.count("collaboration_runs").unwrap(), 1);
        assert_eq!(store.count("handoffs").unwrap(), 1);

        let collaboration: (
            String,
            String,
            u32,
            u32,
            u32,
            u32,
            String,
            Option<String>,
            bool,
        ) = store
            .connection
            .query_row(
                "SELECT project_id, root_agent_ids_json, call_count, max_calls,
                        depth, max_depth, status, stop_reason, auto_dispatch_handoffs
                 FROM collaboration_runs WHERE id = 'collab-1'",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                        row.get(7)?,
                        row.get::<_, i64>(8)? != 0,
                    ))
                },
            )
            .unwrap();
        assert_eq!(
            collaboration,
            (
                "project-1".into(),
                r#"["agent-1"]"#.into(),
                1,
                8,
                2,
                5,
                "completed".into(),
                Some("owner-cancelled".into()),
                true,
            )
        );
        let handoff: (String, String, String, String, Option<String>) = store
            .connection
            .query_row(
                "SELECT collaboration_run_id, from_execution_run_id, to_agent_id, status, details_json
                 FROM handoffs WHERE id = 'handoff-round-trip'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
            )
            .unwrap();
        assert_eq!(
            handoff,
            (
                "collab-1".into(),
                "run-1".into(),
                "agent-1".into(),
                "proposed".into(),
                Some(serde_json::to_string(&export.handoffs[0].details).unwrap()),
            )
        );
        let repeat = store.apply(&export).unwrap();
        assert!(repeat
            .warnings
            .iter()
            .any(|warning| warning.contains("no-op")));
        assert_eq!(store.count("collaboration_runs").unwrap(), 1);
        assert_eq!(store.count("handoffs").unwrap(), 1);
    }

    #[test]
    fn apply_rejects_conflicting_export_without_overwriting() {
        let export = fixture();
        let mut conflicting = export.clone();
        conflicting.projects[0].name = "changed-after-export".into();
        let mut store = MigrationStore::open_in_memory().unwrap();
        store.apply(&export).unwrap();
        assert!(matches!(
            store.apply(&conflicting),
            Err(MigrationError::Invalid(message)) if message.contains("different export")
        ));
        assert_eq!(store.count("projects").unwrap(), 1);
    }

    #[test]
    fn dry_run_rejects_orphan_foreign_keys() {
        let mut export = fixture();
        export.conversations[0].project_id = "missing".into();
        assert!(matches!(dry_run(&export), Err(MigrationError::Invalid(_))));
    }

    #[test]
    fn dry_run_rejects_handoff_target_outside_enabled_project_roster() {
        let mut export = fixture();
        export.project_agents[0].enabled = false;
        export.handoffs.push(HandoffExport {
            id: "handoff-disabled-target".into(),
            collaboration_run_id: "collab-1".into(),
            from_execution_run_id: "run-1".into(),
            to_agent_id: "agent-1".into(),
            status: "pending".into(),
            details: None,
        });
        assert!(matches!(
            dry_run(&export),
            Err(MigrationError::Invalid(message))
                if message.contains("enabled Project roster")
        ));
    }

    #[test]
    fn old_json_without_new_fields_remains_backward_compatible() {
        let mut value = serde_json::to_value(fixture()).unwrap();
        let object = value.as_object_mut().unwrap();
        for field in [
            "attachments",
            "workflows",
            "model_snapshots",
            "model_selection_snapshots",
            "identity_model_options",
            "audit_timestamps",
        ] {
            object.remove(field);
        }
        for field in ["role", "specialty", "system_prompt"] {
            object["project_agents"][0]
                .as_object_mut()
                .unwrap()
                .remove(field);
            object["conversation_agents"][0]
                .as_object_mut()
                .unwrap()
                .remove(field);
        }
        for field in ["connector_id", "model_id", "candidate_model_list_revision"] {
            object["agents"][0].as_object_mut().unwrap().remove(field);
        }
        for field in [
            "model_selection_mode",
            "model_id",
            "candidate_model_list_mode",
            "candidate_model_list_revision",
        ] {
            object["project_agents"][0]
                .as_object_mut()
                .unwrap()
                .remove(field);
            object["conversation_agents"][0]
                .as_object_mut()
                .unwrap()
                .remove(field);
        }
        for field in [
            "project_id",
            "root_agent_ids_json",
            "depth",
            "stop_reason",
            "auto_dispatch_handoffs",
        ] {
            object["collaboration_runs"][0]
                .as_object_mut()
                .unwrap()
                .remove(field);
        }
        let legacy: LegacyExport = serde_json::from_value(value).unwrap();
        assert!(legacy.attachments.is_empty());
        assert!(legacy.workflows.is_empty());
        assert!(legacy.model_snapshots.is_empty());
        assert!(legacy.model_selection_snapshots.is_empty());
        assert!(legacy.identity_model_options.is_empty());
        assert!(legacy.audit_timestamps.is_empty());
        assert!(legacy.project_agents[0].role.is_none());
        assert!(legacy.conversation_agents[0].system_prompt.is_none());
        assert_eq!(legacy.project_agents[0].model_selection_mode, "inherit");
        assert_eq!(
            legacy.conversation_agents[0].candidate_model_list_mode,
            "inherit"
        );
        assert!(legacy.collaboration_runs[0].project_id.is_none());
        assert!(legacy.collaboration_runs[0].root_agent_ids_json.is_none());
        assert_eq!(legacy.collaboration_runs[0].depth, 0);
        assert!(legacy.collaboration_runs[0].stop_reason.is_none());
    }

    #[test]
    fn new_metadata_is_foreign_key_checked_and_secret_rejected() {
        let mut orphan = fixture();
        orphan.attachments[0].message_id = "missing-message".into();
        assert!(matches!(dry_run(&orphan), Err(MigrationError::Invalid(_))));

        let mut secret = fixture();
        secret.workflows[0].steps_json = r#"[{"token":"sk-test-secret-value"}]"#.into();
        assert!(matches!(dry_run(&secret), Err(MigrationError::SecretField)));
    }

    #[test]
    fn interrupted_file_backed_transaction_is_rolled_back_and_can_resume() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "agenttalk-migration-interrupted-{}-{nonce}.sqlite3",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite3-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite3-shm"));

        {
            let store = MigrationStore::open(&path).unwrap();
            store
                .connection
                .execute_batch(
                    "BEGIN IMMEDIATE;
                     INSERT INTO projects(id, name, root_path, archived)
                     VALUES('interrupted-project', 'partial', NULL, 0);",
                )
                .unwrap();
            // Dropping the connection before COMMIT simulates a process interruption.
        }

        {
            let mut reopened = MigrationStore::open(&path).unwrap();
            assert_eq!(reopened.count("projects").unwrap(), 0);
            reopened.apply(&fixture()).unwrap();
            assert_eq!(reopened.count("projects").unwrap(), 1);
        }

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite3-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite3-shm"));
    }

    #[test]
    fn apply_rolls_back_when_insert_hits_existing_primary_key() {
        let export = fixture();
        let mut store = MigrationStore::open_in_memory().unwrap();
        store.apply(&export).unwrap();

        let mut conflicting_source = export.clone();
        conflicting_source.source_schema_checksum =
            "0000000000000000000000000000000000000000000000000000000000000002".into();
        assert!(matches!(
            store.apply(&conflicting_source),
            Err(MigrationError::Sqlite(_))
        ));
        assert_eq!(store.count("projects").unwrap(), 1);
        assert_eq!(store.count("workflows").unwrap(), 1);
        assert_eq!(store.count("identity_model_options").unwrap(), 0);
        assert_eq!(store.count("model_selection_snapshots").unwrap(), 0);
    }
}
