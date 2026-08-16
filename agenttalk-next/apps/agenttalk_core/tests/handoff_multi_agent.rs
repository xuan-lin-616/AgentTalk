use agenttalk_core::{CreateCollaborationCommand, ExecutionStart, PersistentCore};
use agenttalk_domain::{
    AgentIdentity, CollaborationRun, CollaborationStatus, Conversation, ExecutionStatus, Message,
    Project, WorkspaceAccess,
};
use agenttalk_events::RuntimeEvent;
use agenttalk_runtime_host::{
    RuntimeAdapter, RuntimeCapabilities, RuntimeError, RuntimeEventStream, RuntimeRequest,
};
use serde_json::{json, Value};

struct LegacyMentionRuntime {
    payload: Value,
}

impl RuntimeAdapter for LegacyMentionRuntime {
    fn id(&self) -> &str {
        "multi-agent-mention-fixture"
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
                runtime_id: "multi-agent-mention-fixture".into(),
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
                runtime_id: "multi-agent-mention-fixture".into(),
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

fn configured_core(agent_names: &[(&str, &str)], payload: Value) -> PersistentCore {
    let mut core =
        PersistentCore::open_with_runtime(":memory:", Box::new(LegacyMentionRuntime { payload }))
            .unwrap();
    core.create_project(Project {
        id: "project-1".into(),
        name: "Project".into(),
        root_path: None,
        archived: false,
    })
    .unwrap();

    for (id, name) in agent_names {
        core.create_agent(AgentIdentity {
            id: (*id).into(),
            name: (*name).into(),
            role: "fixture".into(),
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
            max_calls: 8,
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

fn completion_payload(output: &str, source_message_id: Option<&str>) -> Value {
    let mut payload = json!({"output": output});
    if let Some(source_message_id) = source_message_id {
        payload["sourceMessageId"] = json!(source_message_id);
    }
    payload
}

fn run_completion(core: &mut PersistentCore) -> ExecutionStatus {
    core.start_execution(ExecutionStart {
        run_id: "parent-1".into(),
        collaboration_run_id: "collaboration-1".into(),
        project_id: "project-1".into(),
        conversation_id: "conversation-1".into(),
        agent_id: "source-agent".into(),
        workspace_access: WorkspaceAccess::None,
        canonical_cwd: None,
    })
    .unwrap()
    .status
}

fn handoffs(core: &PersistentCore) -> Vec<Value> {
    core.projection_snapshot().unwrap()["handoffs"]
        .as_array()
        .unwrap()
        .clone()
}

#[test]
fn one_completion_with_two_exact_roster_mentions_creates_a_parallel_proposal_batch() {
    let mut core = configured_core(
        &[
            ("source-agent", "source-agent"),
            ("reviewer-a", "reviewer-a"),
            ("reviewer-b", "reviewer-b"),
        ],
        completion_payload(
            "Please ask @reviewer-a and @reviewer-b to review the result",
            Some("source-message-1"),
        ),
    );

    assert_eq!(run_completion(&mut core), ExecutionStatus::Completed);

    let mut proposals = handoffs(&core);
    proposals.sort_by_key(|handoff| handoff["details"]["sequenceIndex"].as_u64());
    assert_eq!(proposals.len(), 2);
    assert_eq!(
        proposals
            .iter()
            .map(|handoff| handoff["status"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["proposed", "proposed"]
    );
    assert_eq!(
        proposals
            .iter()
            .map(|handoff| handoff["toAgentId"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["reviewer-a", "reviewer-b"]
    );
    assert_eq!(
        proposals
            .iter()
            .map(|handoff| handoff["details"]["dispatchMode"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["parallel", "parallel"]
    );
    assert_eq!(
        proposals
            .iter()
            .map(|handoff| handoff["details"]["sequenceIndex"].as_u64())
            .collect::<Vec<_>>(),
        vec![Some(0), Some(1)]
    );
    let batch_ids = proposals
        .iter()
        .map(|handoff| handoff["details"]["batchId"].as_str())
        .collect::<Vec<_>>();
    assert!(batch_ids[0].is_some_and(|batch_id| !batch_id.is_empty()));
    assert_eq!(batch_ids[0], batch_ids[1]);
}

#[test]
fn unknown_target_is_fail_closed() {
    let mut core = configured_core(
        &[
            ("source-agent", "source-agent"),
            ("reviewer-a", "reviewer-a"),
        ],
        completion_payload(
            "Please ask @not-rostered to review",
            Some("source-message-1"),
        ),
    );

    assert_eq!(run_completion(&mut core), ExecutionStatus::Completed);
    assert!(handoffs(&core).is_empty());
}

#[test]
fn ambiguous_name_is_fail_closed_even_when_multiple_roster_agents_match() {
    let mut core = configured_core(
        &[
            ("source-agent", "source-agent"),
            ("reviewer-a", "reviewer"),
            ("reviewer-b", "reviewer"),
        ],
        completion_payload("Please ask @reviewer to review", Some("source-message-1")),
    );

    assert_eq!(run_completion(&mut core), ExecutionStatus::Completed);
    assert!(handoffs(&core).is_empty());
}

#[test]
fn self_loop_target_is_fail_closed() {
    let mut core = configured_core(
        &[
            ("source-agent", "source-agent"),
            ("reviewer-a", "reviewer-a"),
        ],
        completion_payload(
            "Please ask @source-agent to review",
            Some("source-message-1"),
        ),
    );

    assert_eq!(run_completion(&mut core), ExecutionStatus::Completed);
    assert!(handoffs(&core).is_empty());
}

#[test]
fn missing_source_message_is_fail_closed() {
    let mut core = configured_core(
        &[
            ("source-agent", "source-agent"),
            ("reviewer-a", "reviewer-a"),
        ],
        completion_payload("Please ask @reviewer-a to review", None),
    );

    assert_eq!(run_completion(&mut core), ExecutionStatus::Completed);
    assert!(handoffs(&core).is_empty());
}
