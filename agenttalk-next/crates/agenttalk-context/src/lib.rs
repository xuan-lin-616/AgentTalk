use agenttalk_domain::{ContextBundle, ContextManifest, ScopeSnapshot};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttachmentContextSource {
    pub source_id: String,
    pub metadata: String,
}

#[derive(Clone, Debug)]
pub struct ContextInput {
    pub scope: ScopeSnapshot,
    pub current_task: String,
    pub history: Vec<String>,
    pub summary: Option<String>,
    pub memories: Vec<String>,
    pub retrieval: Vec<String>,
    pub attachments: Vec<AttachmentContextSource>,
}

/// One stable, serializable record for a source considered during assembly.
///
/// The field names intentionally match the persisted source-ledger JSON
/// contract consumed by the core (`sourceId`, `tokenCount`, and so on).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceLedgerEntry {
    pub source_id: String,
    pub kind: String,
    pub sha256: String,
    pub token_count: u64,
    pub included: bool,
}

/// The source-ledger contract exposed by this crate to its callers.
pub type SourceLedger = Vec<SourceLedgerEntry>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AssembledContext {
    pub bundle: ContextBundle,
    pub manifest: ContextManifest,
    pub source_ledger: SourceLedger,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum ContextError {
    #[error("current task cannot be empty")]
    EmptyTask,
    #[error("execution run id cannot be empty")]
    EmptyExecutionRunId,
    #[error("context manifest is bound to a different execution run")]
    ManifestRunMismatch,
    #[error("context exceeds the frozen token budget")]
    BudgetExceeded,
}

pub struct ContextAssembler {
    pub token_budget: u64,
}

impl ContextAssembler {
    /// Assemble using the legacy synthetic run id required by existing
    /// callers. New execution paths should call [`Self::assemble_for_run`]
    /// so that the manifest is explicitly bound to the real execution run.
    pub fn assemble(&self, input: ContextInput) -> Result<AssembledContext, ContextError> {
        let legacy_execution_run_id = format!("run-for-{}", input.scope.agent_id);
        self.assemble_for_run(legacy_execution_run_id, input)
    }

    /// Assemble a context whose manifest identity is bound to one execution
    /// run. Identical rendered contexts from different runs therefore receive
    /// different manifest IDs, while repeating the same run/context pair is
    /// deterministic and idempotent.
    pub fn assemble_for_run(
        &self,
        execution_run_id: impl Into<String>,
        input: ContextInput,
    ) -> Result<AssembledContext, ContextError> {
        let execution_run_id = execution_run_id.into();
        if execution_run_id.trim().is_empty() {
            return Err(ContextError::EmptyExecutionRunId);
        }

        self.assemble_internal(execution_run_id, input)
    }

    /// Parameter-ordering companion for callers that naturally construct the
    /// input before selecting the execution run.
    pub fn assemble_with_execution_run_id(
        &self,
        input: ContextInput,
        execution_run_id: impl Into<String>,
    ) -> Result<AssembledContext, ContextError> {
        self.assemble_for_run(execution_run_id, input)
    }

    fn assemble_internal(
        &self,
        execution_run_id: String,
        input: ContextInput,
    ) -> Result<AssembledContext, ContextError> {
        let current_task = input.current_task;
        if current_task.trim().is_empty() {
            return Err(ContextError::EmptyTask);
        }
        let mut ledger = Vec::new();
        let mut sections = Vec::new();
        let mut add = |source_id: String, kind: &str, text: String| {
            let token_count = estimate_tokens(&text);
            ledger.push(SourceLedgerEntry {
                source_id,
                kind: kind.into(),
                sha256: sha256(&text),
                token_count,
                included: true,
            });
            sections.push(text);
        };
        if let Some(summary) = input.summary {
            add("summary".into(), "summary", summary);
        }
        for (index, value) in input.memories.into_iter().enumerate() {
            add(format!("memory-{index}"), "memory", value);
        }
        for (index, value) in input.retrieval.into_iter().enumerate() {
            add(format!("retrieval-{index}"), "retrieval", value);
        }
        for attachment in input.attachments {
            add(attachment.source_id, "attachment", attachment.metadata);
        }
        for (index, value) in input.history.into_iter().enumerate() {
            add(format!("message-{index}"), "message", value);
        }
        let task = format!("[current_task]\n{current_task}");
        add("current-task".into(), "current_task", task);
        let rendered_context = sections.join("\n\n");
        let total_tokens = estimate_tokens(&rendered_context);
        if total_tokens > self.token_budget {
            return Err(ContextError::BudgetExceeded);
        }
        let source_ids = ledger.iter().map(|entry| entry.source_id.clone()).collect();
        let manifest = ContextManifest {
            id: manifest_id_for_run(&execution_run_id, &rendered_context)?,
            execution_run_id,
            schema_version: "context-v2".into(),
            source_ids,
            workspace_access: input.scope.workspace_access,
            canonical_cwd: input.scope.canonical_cwd,
            connector_id: None,
            model_id: None,
        };
        Ok(AssembledContext {
            bundle: ContextBundle {
                current_task,
                rendered_context,
                source_ids: manifest.source_ids.clone(),
            },
            manifest,
            source_ledger: ledger,
        })
    }
}

/// Return the deterministic manifest ID for a run/context pair.
///
/// The execution run ID is part of the identity. The rendered-context hash is
/// retained so a single run cannot silently reuse a manifest for changed
/// context, while identical context across different runs cannot collide.
pub fn manifest_id_for_run(
    execution_run_id: &str,
    rendered_context: &str,
) -> Result<String, ContextError> {
    let execution_run_id = execution_run_id.trim();
    if execution_run_id.is_empty() {
        return Err(ContextError::EmptyExecutionRunId);
    }
    let context_hash = sha256(rendered_context);
    Ok(format!(
        "manifest-{execution_run_id}-{}",
        &context_hash[..16]
    ))
}

pub fn estimate_tokens(value: &str) -> u64 {
    value.split_whitespace().count() as u64
}
fn sha256(value: &str) -> String {
    Sha256::digest(value.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use agenttalk_domain::WorkspaceAccess;

    fn input(task: &str) -> ContextInput {
        ContextInput {
            scope: ScopeSnapshot {
                project_id: "p".into(),
                conversation_id: "c".into(),
                agent_id: "a".into(),
                workspace_access: WorkspaceAccess::ReadOnly,
                canonical_cwd: None,
            },
            current_task: task.into(),
            history: vec!["prior message".into()],
            summary: None,
            memories: vec!["confirmed memory".into()],
            retrieval: vec!["exact source".into()],
            attachments: vec![],
        }
    }

    #[test]
    fn current_task_is_rendered_once_and_manifest_does_not_store_full_prompt() {
        let assembled = ContextAssembler { token_budget: 100 }
            .assemble(input("unique current task"))
            .unwrap();
        assert_eq!(
            assembled
                .bundle
                .rendered_context
                .matches("unique current task")
                .count(),
            1
        );
        assert!(!assembled
            .manifest
            .source_ids
            .iter()
            .any(|source| source == "unique current task"));
        assert_eq!(assembled.manifest.schema_version, "context-v2");

        let ledger = serde_json::to_value(&assembled.source_ledger).unwrap();
        assert_eq!(ledger[0]["sha256"].as_str().unwrap().len(), 64);
        assert!(ledger[0]["tokenCount"].as_u64().unwrap() > 0);
        assert!(ledger[0]["included"].as_bool().unwrap());
    }

    #[test]
    fn identical_rendered_contexts_are_bound_to_different_execution_runs() {
        let assembler = ContextAssembler { token_budget: 100 };
        let first = assembler
            .assemble_for_run("run-one", input("same task"))
            .unwrap();
        let second = assembler
            .assemble_for_run("run-two", input("same task"))
            .unwrap();

        assert_eq!(
            first.bundle.rendered_context,
            second.bundle.rendered_context
        );
        assert_eq!(first.manifest.execution_run_id, "run-one");
        assert_eq!(second.manifest.execution_run_id, "run-two");
        assert_ne!(first.manifest.id, second.manifest.id);
        assert_eq!(
            first.manifest.id,
            manifest_id_for_run("run-one", &first.bundle.rendered_context).unwrap()
        );
    }

    #[test]
    fn attachment_sources_keep_stable_identity_and_metadata_only_content() {
        let mut context_input = input("inspect attachment metadata");
        context_input.attachments = vec![AttachmentContextSource {
            source_id: "attachment-stable-1".into(),
            metadata:
                "[attachment]\nartifact_id=artifact-1\nsha256=abc\nsize=12\npermission=read_only"
                    .into(),
        }];
        let assembled = ContextAssembler { token_budget: 200 }
            .assemble_for_run("run-with-attachment", context_input)
            .unwrap();
        let entry = assembled
            .source_ledger
            .iter()
            .find(|entry| entry.kind == "attachment")
            .unwrap();
        assert_eq!(entry.source_id, "attachment-stable-1");
        assert!(assembled
            .bundle
            .rendered_context
            .contains("artifact_id=artifact-1"));
        assert!(!assembled.bundle.rendered_context.contains("sourcePath"));
    }

    #[test]
    fn token_budget_fails_closed() {
        assert!(matches!(
            ContextAssembler { token_budget: 1 }.assemble(input("a task with many words")),
            Err(ContextError::BudgetExceeded)
        ));
    }

    #[test]
    fn empty_execution_run_id_fails_closed() {
        assert!(matches!(
            ContextAssembler { token_budget: 100 }.assemble_for_run("  ", input("a task")),
            Err(ContextError::EmptyExecutionRunId)
        ));
    }
}
