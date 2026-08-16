use agenttalk_core::{
    CreateCollaborationCommand, CreateWorkflowCommand, ExecutionStart, PersistentCore,
    WorkflowDispatchCommand,
};
use agenttalk_domain::{
    AgentIdentity, CollaborationRun, CollaborationStatus, Conversation, ExecutionStatus, Message,
    Project, WorkflowStep, WorkflowTemplate, WorkspaceAccess,
};
use agenttalk_runtime_host::MockRuntime;

fn configured_core() -> PersistentCore {
    let mut core = PersistentCore::open_with_runtime(":memory:", Box::new(MockRuntime::default()))
        .expect("open core");
    core.create_project(Project {
        id: "workflow-project".into(),
        name: "Workflow Project".into(),
        root_path: None,
        archived: false,
    })
    .expect("create project");
    for agent_id in ["root-agent", "step-agent-a", "step-agent-b"] {
        core.create_agent(AgentIdentity {
            id: agent_id.into(),
            name: agent_id.into(),
            role: "fixture".into(),
            specialty: "workflow".into(),
            system_prompt: "fixture".into(),
        })
        .expect("create agent");
        core.set_project_agent_assignment(
            "workflow-project",
            agent_id,
            true,
            WorkspaceAccess::None,
        )
        .expect("assign agent");
    }
    core.create_conversation(Conversation {
        id: "workflow-conversation".into(),
        project_id: "workflow-project".into(),
        title: "Workflow Conversation".into(),
        scope_revision: 0,
    })
    .expect("create conversation");
    core.create_message(Message {
        id: "workflow-source-message".into(),
        conversation_id: "workflow-conversation".into(),
        sender_id: "user".into(),
        sequence: 1,
        content: "workflow source".into(),
    })
    .expect("create source message");
    core.create_collaboration(CreateCollaborationCommand {
        project_id: "workflow-project".into(),
        collaboration: CollaborationRun {
            id: "workflow-collaboration".into(),
            root_agent_ids: vec!["root-agent".into()],
            call_count: 0,
            max_calls: 8,
            depth: 0,
            max_depth: 5,
            status: CollaborationStatus::Pending,
            stop_reason: None,
            auto_dispatch_handoffs: false,
        },
    })
    .expect("create collaboration");
    core.start_execution(ExecutionStart {
        run_id: "workflow-root-run".into(),
        collaboration_run_id: "workflow-collaboration".into(),
        project_id: "workflow-project".into(),
        conversation_id: "workflow-conversation".into(),
        agent_id: "root-agent".into(),
        workspace_access: WorkspaceAccess::None,
        canonical_cwd: None,
    })
    .expect("start root run");
    core
}

fn workflow(id: &str, kind: &str) -> WorkflowTemplate {
    WorkflowTemplate {
        id: id.into(),
        name: id.into(),
        kind: kind.into(),
        steps: vec![
            WorkflowStep {
                id: format!("{id}-step-a"),
                order: 1,
                agent_id: "step-agent-a".into(),
                prompt_supplement: Some("inspect the input".into()),
            },
            WorkflowStep {
                id: format!("{id}-step-b"),
                order: 2,
                agent_id: "step-agent-b".into(),
                prompt_supplement: Some("produce the next result".into()),
            },
        ],
    }
}

fn dispatch(workflow_id: &str) -> WorkflowDispatchCommand {
    WorkflowDispatchCommand {
        workflow_id: workflow_id.into(),
        collaboration_run_id: "workflow-collaboration".into(),
        parent_execution_run_id: "workflow-root-run".into(),
        source_message_id: "workflow-source-message".into(),
        task: "complete the workflow fixture".into(),
        start_runtime: true,
    }
}

#[test]
fn sequential_workflow_uses_the_previous_child_as_the_next_parent() {
    let mut core = configured_core();
    core.create_workflow(CreateWorkflowCommand {
        project_id: "workflow-project".into(),
        workflow: workflow("workflow-sequential", "sequential"),
    })
    .expect("create workflow");

    let result = core
        .dispatch_workflow(dispatch("workflow-sequential"))
        .expect("dispatch sequential workflow");
    assert_eq!(result.mode, "sequential");
    assert!(result.completed);
    assert!(!result.failed);
    assert_eq!(result.steps.len(), 2);
    assert!(result.steps.iter().all(|step| {
        step.handoff_status == "completed"
            && step.child_status.as_deref() == Some("completed")
            && step.runtime_started
    }));

    let handoffs = core.projection_snapshot().expect("projection")["handoffs"]
        .as_array()
        .expect("handoffs")
        .to_vec();
    let first = handoffs
        .iter()
        .find(|handoff| handoff["details"]["sequenceIndex"] == 0)
        .expect("first handoff");
    let second = handoffs
        .iter()
        .find(|handoff| handoff["details"]["sequenceIndex"] == 1)
        .expect("second handoff");
    assert_eq!(
        second["fromExecutionRunId"],
        first["details"]["childExecutionRunId"]
    );
}

#[test]
fn parallel_workflow_keeps_siblings_on_the_frozen_parent_and_is_idempotent() {
    let mut core = configured_core();
    core.create_workflow(CreateWorkflowCommand {
        project_id: "workflow-project".into(),
        workflow: workflow("workflow-parallel", "parallel"),
    })
    .expect("create workflow");

    let command = dispatch("workflow-parallel");
    let result = core
        .dispatch_workflow(command.clone())
        .expect("dispatch parallel workflow");
    assert_eq!(result.mode, "parallel");
    assert!(result.completed);
    assert_eq!(result.steps.len(), 2);
    assert!(result.steps.iter().all(|step| {
        step.handoff_status == "completed" && step.child_status.as_deref() == Some("completed")
    }));

    let replay = core
        .dispatch_workflow(command)
        .expect("replay parallel workflow");
    assert_eq!(replay.steps.len(), 2);
    assert_eq!(
        replay
            .steps
            .iter()
            .map(|step| step.child_execution_run_id.clone())
            .collect::<Vec<_>>(),
        result
            .steps
            .iter()
            .map(|step| step.child_execution_run_id.clone())
            .collect::<Vec<_>>()
    );
    assert!(replay
        .steps
        .iter()
        .all(|step| step.child_status == Some("completed".into())));

    let snapshot = core.projection_snapshot().expect("projection");
    let runs = snapshot["runs"].as_array().expect("runs");
    assert!(runs.iter().any(|run| run["id"] == "workflow-root-run"));
    let handoffs = snapshot["handoffs"].as_array().expect("handoffs");
    for step in &result.steps {
        let child_id = step.child_execution_run_id.as_ref().expect("child id");
        let child = runs
            .iter()
            .find(|run| run["id"] == child_id.as_str())
            .expect("child run");
        assert_eq!(child["id"], child_id.as_str());
        let handoff = handoffs
            .iter()
            .find(|handoff| handoff["id"] == step.handoff_id)
            .expect("handoff");
        assert_eq!(
            handoff["details"]["parentExecutionRunId"],
            "workflow-root-run"
        );
    }
}

#[test]
fn workflow_dispatch_fails_closed_for_unknown_kind_and_missing_source() {
    let mut core = configured_core();
    core.create_workflow(CreateWorkflowCommand {
        project_id: "workflow-project".into(),
        workflow: workflow("workflow-invalid", "fan-out"),
    })
    .expect("create workflow");
    let error = core
        .dispatch_workflow(dispatch("workflow-invalid"))
        .expect_err("unknown kind must fail");
    assert!(error.to_string().contains("unsupported"));

    let mut core = configured_core();
    core.create_workflow(CreateWorkflowCommand {
        project_id: "workflow-project".into(),
        workflow: workflow("workflow-missing-source", "sequential"),
    })
    .expect("create workflow");
    let mut missing_source = dispatch("workflow-missing-source");
    missing_source.source_message_id.clear();
    assert!(matches!(
        core.dispatch_workflow(missing_source),
        Err(agenttalk_core::CoreError::HandoffSourceMessageMissing)
    ));
}

#[allow(dead_code)]
fn _status_is_terminal(status: &ExecutionStatus) -> bool {
    status.is_terminal()
}
