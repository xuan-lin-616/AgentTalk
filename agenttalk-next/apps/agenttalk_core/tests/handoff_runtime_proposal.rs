use agenttalk_core::{
    parse_legacy_handoff_mention, parse_runtime_handoff_proposal, CreateCollaborationCommand,
    ExecutionStart, LegacyMentionCandidate, PersistentCore,
};
use agenttalk_domain::{
    AgentIdentity, CollaborationRun, CollaborationStatus, Conversation, ExecutionStatus, Message,
    Project, WorkspaceAccess,
};
use agenttalk_events::RuntimeEvent;
use agenttalk_runtime_host::{
    RuntimeAdapter, RuntimeCapabilities, RuntimeError, RuntimeEventStream, RuntimeRequest,
};
use serde_json::{json, Value};

struct ProposalRuntime {
    payload: Value,
}

impl RuntimeAdapter for ProposalRuntime {
    fn id(&self) -> &str {
        "proposal-fixture"
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
        request: &RuntimeRequest,
        capacity: usize,
    ) -> Result<RuntimeEventStream, RuntimeError> {
        let run_id = request.execution_run_id.clone();
        let payload = self.payload.clone();
        RuntimeEventStream::spawn(capacity, move |producer| {
            producer.push(RuntimeEvent {
                event_id: format!("started-{run_id}"),
                execution_run_id: run_id.clone(),
                runtime_id: "proposal-fixture".into(),
                thread_id: None,
                turn_id: Some("turn-1".into()),
                sequence: 0,
                event_type: "runtime.started".into(),
                timestamp_ms: 0,
                payload: json!({}),
            })?;
            producer.push(RuntimeEvent {
                event_id: format!("completed-{run_id}"),
                execution_run_id: run_id,
                runtime_id: "proposal-fixture".into(),
                thread_id: None,
                turn_id: Some("turn-1".into()),
                sequence: 0,
                event_type: "execution.completed".into(),
                timestamp_ms: 1,
                payload,
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
            payload: json!({}),
        })
    }
}

fn proposal_payload(target: &str, source_message_id: &str, dispatch_mode: &str) -> Value {
    json!({
        "handoffProposal": {
            "handoffId": "runtime-proposal-1",
            "collaborationRunId": "collaboration-1",
            "toAgentId": target,
            "details": {
                "parentExecutionRunId": "parent-1",
                "sourceMessageId": source_message_id,
                "fromAgentId": "source-agent",
                "toAgentId": target,
                "kind": "task",
                "dispatchMode": dispatch_mode,
                "detectedBy": "structured_output",
                "task": "review the completed output",
                "contextScope": "conversation"
            }
        }
    })
}

fn runtime_event(payload: Value) -> RuntimeEvent {
    RuntimeEvent {
        event_id: "completed-parent-1".into(),
        execution_run_id: "parent-1".into(),
        runtime_id: "fixture".into(),
        thread_id: None,
        turn_id: Some("turn-1".into()),
        sequence: 0,
        event_type: "execution.completed".into(),
        timestamp_ms: 1,
        payload,
    }
}

fn configured_core(payload: Value) -> PersistentCore {
    let mut core =
        PersistentCore::open_with_runtime(":memory:", Box::new(ProposalRuntime { payload }))
            .unwrap();
    core.create_project(Project {
        id: "project-1".into(),
        name: "Project".into(),
        root_path: None,
        archived: false,
    })
    .unwrap();
    for (id, role) in [("source-agent", "builder"), ("target-agent", "reviewer")] {
        core.create_agent(AgentIdentity {
            id: id.into(),
            name: id.into(),
            role: role.into(),
            specialty: "handoff".into(),
            system_prompt: "fixture".into(),
        })
        .unwrap();
        core.set_project_agent_assignment("project-1", id, true, WorkspaceAccess::None)
            .unwrap();
    }
    core.create_conversation(Conversation {
        id: "conversation-1".into(),
        project_id: "project-1".into(),
        title: "Conversation".into(),
        scope_revision: 0,
    })
    .unwrap();
    core.create_collaboration(CreateCollaborationCommand {
        project_id: "project-1".into(),
        collaboration: CollaborationRun {
            id: "collaboration-1".into(),
            root_agent_ids: vec!["source-agent".into()],
            call_count: 0,
            max_calls: 4,
            depth: 0,
            max_depth: 3,
            status: CollaborationStatus::Pending,
            stop_reason: None,
            auto_dispatch_handoffs: false,
        },
    })
    .unwrap();
    core.create_message(Message {
        id: "source-message-1".into(),
        conversation_id: "conversation-1".into(),
        sender_id: "user".into(),
        sequence: 1,
        content: "source message".into(),
    })
    .unwrap();
    core
}

#[test]
fn completed_runtime_event_creates_only_a_policy_checked_structured_handoff() {
    let mut core = configured_core(proposal_payload(
        "target-agent",
        "source-message-1",
        "sequential",
    ));
    let run = core
        .start_execution(ExecutionStart {
            run_id: "parent-1".into(),
            collaboration_run_id: "collaboration-1".into(),
            project_id: "project-1".into(),
            conversation_id: "conversation-1".into(),
            agent_id: "source-agent".into(),
            workspace_access: WorkspaceAccess::None,
            canonical_cwd: None,
        })
        .unwrap();
    assert_eq!(run.status, ExecutionStatus::Completed);
    let handoffs = core.projection_snapshot().unwrap()["handoffs"]
        .as_array()
        .unwrap()
        .clone();
    assert_eq!(handoffs.len(), 1);
    assert_eq!(handoffs[0]["toAgentId"], "target-agent");
    assert_eq!(
        handoffs[0]["details"]["sourceMessageId"],
        "source-message-1"
    );
}

#[test]
fn completed_runtime_json_string_envelope_creates_a_scoped_structured_handoff() {
    let structured = proposal_payload("target-agent", "source-message-1", "sequential");
    let mut core = configured_core(json!({
        "output": serde_json::to_string(&structured).unwrap()
    }));
    let run = core
        .start_execution(ExecutionStart {
            run_id: "parent-1".into(),
            collaboration_run_id: "collaboration-1".into(),
            project_id: "project-1".into(),
            conversation_id: "conversation-1".into(),
            agent_id: "source-agent".into(),
            workspace_access: WorkspaceAccess::None,
            canonical_cwd: None,
        })
        .unwrap();
    assert_eq!(run.status, ExecutionStatus::Completed);
    let snapshot = core.projection_snapshot().unwrap();
    let handoffs = snapshot["handoffs"].as_array().unwrap();
    assert_eq!(handoffs.len(), 1);
    assert_eq!(handoffs[0]["id"], "runtime-proposal-1");
    assert_eq!(handoffs[0]["details"]["detectedBy"], "structured_output");
}

#[test]
fn completed_runtime_legacy_mention_creates_a_scoped_proposed_handoff() {
    let mut core = configured_core(json!({
        "output": "Please ask @target-agent to review the result",
        "sourceMessageId": "source-message-1"
    }));
    let run = core
        .start_execution(ExecutionStart {
            run_id: "parent-1".into(),
            collaboration_run_id: "collaboration-1".into(),
            project_id: "project-1".into(),
            conversation_id: "conversation-1".into(),
            agent_id: "source-agent".into(),
            workspace_access: WorkspaceAccess::None,
            canonical_cwd: None,
        })
        .unwrap();
    assert_eq!(run.status, ExecutionStatus::Completed);
    let handoff = &core.projection_snapshot().unwrap()["handoffs"][0];
    assert_eq!(handoff["status"], "proposed");
    assert_eq!(handoff["toAgentId"], "target-agent");
    assert_eq!(handoff["details"]["detectedBy"], "legacy_mention");
    assert_eq!(handoff["details"]["sourceMessageId"], "source-message-1");
    assert!(handoff["details"]["task"].is_null());
}

#[test]
fn legacy_mention_auto_trigger_fails_closed_without_an_authoritative_source() {
    let mut core = configured_core(json!({
        "output": "Please ask @target-agent to review the result"
    }));
    core.start_execution(ExecutionStart {
        run_id: "parent-1".into(),
        collaboration_run_id: "collaboration-1".into(),
        project_id: "project-1".into(),
        conversation_id: "conversation-1".into(),
        agent_id: "source-agent".into(),
        workspace_access: WorkspaceAccess::None,
        canonical_cwd: None,
    })
    .unwrap();
    assert!(core.projection_snapshot().unwrap()["handoffs"]
        .as_array()
        .unwrap()
        .is_empty());
}

#[test]
fn unknown_target_and_invalid_scope_are_rejected_before_handoff_write() {
    let mut unknown_target = configured_core(proposal_payload(
        "not-rostered",
        "source-message-1",
        "sequential",
    ));
    unknown_target
        .start_execution(ExecutionStart {
            run_id: "parent-1".into(),
            collaboration_run_id: "collaboration-1".into(),
            project_id: "project-1".into(),
            conversation_id: "conversation-1".into(),
            agent_id: "source-agent".into(),
            workspace_access: WorkspaceAccess::None,
            canonical_cwd: None,
        })
        .unwrap();
    assert!(unknown_target.projection_snapshot().unwrap()["handoffs"]
        .as_array()
        .unwrap()
        .is_empty());

    let mut missing_source = configured_core(proposal_payload(
        "target-agent",
        "missing-message",
        "sequential",
    ));
    missing_source
        .start_execution(ExecutionStart {
            run_id: "parent-1".into(),
            collaboration_run_id: "collaboration-1".into(),
            project_id: "project-1".into(),
            conversation_id: "conversation-1".into(),
            agent_id: "source-agent".into(),
            workspace_access: WorkspaceAccess::None,
            canonical_cwd: None,
        })
        .unwrap();
    assert!(missing_source.projection_snapshot().unwrap()["handoffs"]
        .as_array()
        .unwrap()
        .is_empty());
}

#[test]
fn structured_parser_fails_closed_for_missing_source_and_invalid_dispatch_mode() {
    let missing_source = proposal_payload("target-agent", "", "sequential");
    assert!(parse_runtime_handoff_proposal(&runtime_event(missing_source)).is_err());

    let invalid_mode = proposal_payload("target-agent", "source-message-1", "broadcast");
    assert!(parse_runtime_handoff_proposal(&runtime_event(invalid_mode)).is_err());
}

#[test]
fn structured_parser_accepts_nested_content_blocks_and_deduplicates_text() {
    let proposal = proposal_payload("target-agent", "source-message-1", "sequential");
    let encoded = serde_json::to_string(&proposal).unwrap();
    let payload = json!({
            "output": {
            "content": [
                {"type": "output_text", "text": encoded},
                {"delta": {"content": [{"text": encoded}]}},
                {"choices": [{"message": {"content": encoded}}]}
            ]
        }
    });

    let parsed = agenttalk_core::parse_runtime_handoff_proposals(&runtime_event(payload))
        .unwrap()
        .unwrap();
    assert_eq!(parsed.len(), 1);
    assert_eq!(parsed[0].id, "runtime-proposal-1");
}

#[test]
fn legacy_mention_parser_requires_one_exact_roster_match() {
    let candidates = vec![
        LegacyMentionCandidate {
            id: "agent-reviewer".into(),
            name: "reviewer".into(),
        },
        LegacyMentionCandidate {
            id: "agent-builder".into(),
            name: "builder".into(),
        },
    ];
    assert_eq!(
        parse_legacy_handoff_mention("Please ask @reviewer to check this", &candidates),
        Some("agent-reviewer".into())
    );
    assert_eq!(
        parse_legacy_handoff_mention("@unknown or @reviewer", &candidates),
        None
    );
    assert_eq!(
        parse_legacy_handoff_mention("@reviewer @builder", &candidates),
        None
    );
    assert_eq!(
        parse_legacy_handoff_mention("Please review this", &candidates),
        None
    );
}
