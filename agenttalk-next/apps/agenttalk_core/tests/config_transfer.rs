//! Configuration transfer acceptance contract.
//!
//! This test is written against the current Core public API:
//!
//! * `PersistentCore::export_project_config(project_id) -> Result<Value, CoreError>`
//! * `PersistentCore::import_project_config(Value) -> Result<ConfigImportResult, CoreError>`
//!
//! The JSON shape follows the legacy `IExportConfig`/`IImportResult` contract
//! (`project`, `agents`, `projectAgents`, `conversations`, optional
//! `workflowTemplates`, and camelCase result fields).  It is a contract test,
//! not a production implementation.  Do not replace the target calls with
//! direct SQLite access: import/export must remain Core-owned and atomic.

use agenttalk_core::{CreateWorkflowCommand, PersistentCore};
use agenttalk_domain::{
    AgentIdentity, Conversation, Message, Project, WorkflowStep, WorkflowTemplate, WorkspaceAccess,
};
use agenttalk_runtime_host::MockRuntime;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};

const SOURCE_PROJECT_ID: &str = "config-transfer-project-source";
const SOURCE_PROJECT_ROOT: &str = r"C:\Users\owner\Private\AgentTalkWorkspace";
const SOURCE_AGENT_IDS: [&str; 2] = [
    "config-transfer-agent-builder",
    "config-transfer-agent-reviewer",
];
const SOURCE_CONVERSATION_IDS: [&str; 2] = [
    "config-transfer-conversation-main",
    "config-transfer-conversation-review",
];
const SOURCE_WORKFLOW_ID: &str = "config-transfer-workflow-source";

fn fixture_core() -> PersistentCore {
    let mut core = PersistentCore::open_with_runtime(":memory:", Box::new(MockRuntime::default()))
        .expect("fixture Core should open");

    core.create_project(Project {
        id: SOURCE_PROJECT_ID.into(),
        name: "Config transfer fixture".into(),
        // The export must redact this path even though it is present in the
        // in-memory source project.
        root_path: Some(SOURCE_PROJECT_ROOT.into()),
        archived: false,
    })
    .unwrap();

    for (id, name, role, specialty, prompt) in [
        (
            SOURCE_AGENT_IDS[0],
            "Builder",
            "builder",
            "implementation",
            "Build only within the approved project scope.",
        ),
        (
            SOURCE_AGENT_IDS[1],
            "Reviewer",
            "reviewer",
            "verification",
            "Review the bounded project result.",
        ),
    ] {
        core.create_agent(AgentIdentity {
            id: id.into(),
            name: name.into(),
            role: role.into(),
            specialty: specialty.into(),
            system_prompt: prompt.into(),
        })
        .unwrap();
    }

    core.set_project_agent_assignment(
        SOURCE_PROJECT_ID,
        SOURCE_AGENT_IDS[0],
        true,
        WorkspaceAccess::WorkspaceWrite,
    )
    .unwrap();
    core.set_project_agent_assignment(
        SOURCE_PROJECT_ID,
        SOURCE_AGENT_IDS[1],
        true,
        WorkspaceAccess::ReadOnly,
    )
    .unwrap();

    for (id, title) in [
        (SOURCE_CONVERSATION_IDS[0], "Main discussion"),
        (SOURCE_CONVERSATION_IDS[1], "Review discussion"),
    ] {
        core.create_conversation(Conversation {
            id: id.into(),
            project_id: SOURCE_PROJECT_ID.into(),
            title: title.into(),
            scope_revision: 7,
        })
        .unwrap();
    }

    core.create_message(Message {
        id: "config-transfer-message-1".into(),
        conversation_id: SOURCE_CONVERSATION_IDS[0].into(),
        sender_id: "user".into(),
        sequence: 1,
        content: "fixture conversation metadata".into(),
    })
    .unwrap();

    core.create_workflow(CreateWorkflowCommand {
        project_id: SOURCE_PROJECT_ID.into(),
        workflow: WorkflowTemplate {
            id: SOURCE_WORKFLOW_ID.into(),
            name: "Build then review".into(),
            kind: "sequential".into(),
            steps: vec![
                WorkflowStep {
                    id: "config-transfer-step-builder".into(),
                    order: 0,
                    agent_id: SOURCE_AGENT_IDS[0].into(),
                    prompt_supplement: Some("Implement the requested change.".into()),
                },
                WorkflowStep {
                    id: "config-transfer-step-reviewer".into(),
                    order: 1,
                    agent_id: SOURCE_AGENT_IDS[1].into(),
                    prompt_supplement: Some("Check the result and report findings.".into()),
                },
            ],
        },
    })
    .unwrap();

    core
}

fn snapshot(core: &PersistentCore) -> Value {
    core.projection_snapshot()
        .expect("projection should be readable")
}

fn rows_for<'a>(snapshot: &'a Value, key: &str) -> &'a [Value] {
    snapshot[key]
        .as_array()
        .unwrap_or_else(|| panic!("projection field {key} must be an array"))
}

fn row_by<'a>(rows: &'a [Value], key: &str, expected: &str) -> &'a Value {
    rows.iter()
        .find(|row| row[key].as_str() == Some(expected))
        .unwrap_or_else(|| panic!("no {key}={expected} row"))
}

fn ids(rows: &[Value], key: &str) -> HashSet<String> {
    rows.iter()
        .map(|row| row[key].as_str().expect("id field must be a string").into())
        .collect()
}

fn assert_no_unsafe_export_material(value: &Value) {
    match value {
        Value::Array(items) => items.iter().for_each(assert_no_unsafe_export_material),
        Value::Object(object) => {
            for (key, child) in object {
                let normalized = key.to_ascii_lowercase();
                assert!(
                    ![
                        "secret",
                        "token",
                        "auth",
                        "password",
                        "apikey",
                        "credential"
                    ]
                    .iter()
                    .any(|needle| normalized.contains(needle)),
                    "export contains sensitive field name: {key}"
                );
                assert_no_unsafe_export_material(child);
            }
        }
        Value::String(text) => {
            // This catches both the fixture path and any other Windows/UNC
            // path accidentally carried into the portable configuration.
            assert!(!text.contains(SOURCE_PROJECT_ROOT));
            assert!(
                !looks_like_absolute_windows_path(text),
                "export contains an absolute workspace path: {text}"
            );
        }
        _ => {}
    }
}

fn looks_like_absolute_windows_path(value: &str) -> bool {
    let bytes = value.as_bytes();
    (bytes.len() >= 3
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && (bytes[2] == b'\\' || bytes[2] == b'/'))
        || value.starts_with(r"\\")
}

#[test]
fn export_is_safe_metadata_only_and_round_trips_project_roster_conversations_and_workflows() {
    let source = fixture_core();
    let source_snapshot = snapshot(&source);

    // Target API: the export must be a JSON representation of IExportConfig,
    // with workspace roots and all secret/token/auth material removed.
    let exported = source
        .export_project_config(SOURCE_PROJECT_ID)
        .expect("export_project_config should export the selected project");
    assert_eq!(exported["schemaVersion"], json!("config.transfer.v1"));
    assert_eq!(exported["version"], json!("1.0"));
    assert!(exported["exportedAt"].is_string());
    assert_no_unsafe_export_material(&exported);
    assert!(exported["project"]["rootPath"].is_null());
    assert!(exported.get("messages").is_none());
    assert_eq!(exported["agents"].as_array().unwrap().len(), 2);
    assert_eq!(exported["projectAgents"].as_array().unwrap().len(), 2);
    assert_eq!(exported["conversations"].as_array().unwrap().len(), 2);
    assert_eq!(exported["workflowTemplates"].as_array().unwrap().len(), 1);

    let mut imported_core =
        PersistentCore::open_with_runtime(":memory:", Box::new(MockRuntime::default()))
            .expect("target Core should open");
    let result = imported_core
        .import_project_config(exported)
        .expect("import_project_config should import safe metadata");

    // Current Core returns a typed result rather than the legacy
    // IImportResult envelope; these fields are its direct mapping.
    assert_eq!(result.imported_agents, 2);
    assert_eq!(result.imported_conversations, 2);
    assert_eq!(result.imported_workflows, 1);
    assert!(!result.workspace_rebind_required);
    let new_project_id = result.new_project_id.as_str();
    assert_ne!(new_project_id, SOURCE_PROJECT_ID);

    let imported_snapshot = snapshot(&imported_core);
    let imported_project = row_by(
        rows_for(&imported_snapshot, "projects"),
        "id",
        new_project_id,
    );
    assert_eq!(
        imported_project["name"],
        json!("Config transfer fixture (导入)")
    );
    assert!(imported_project["rootPath"].is_null());

    let source_agents = rows_for(&source_snapshot, "agents");
    let imported_agents = rows_for(&imported_snapshot, "agents");
    assert_eq!(imported_agents.len(), source_agents.len());
    let source_agent_ids = ids(source_agents, "id");
    let imported_agent_ids = ids(imported_agents, "id");
    assert!(source_agent_ids.is_disjoint(&imported_agent_ids));

    let agent_id_by_name: HashMap<String, String> = imported_agents
        .iter()
        .map(|agent| {
            (
                agent["name"].as_str().unwrap().into(),
                agent["id"].as_str().unwrap().into(),
            )
        })
        .collect();
    let imported_builder_id = agent_id_by_name.get("Builder").unwrap();
    let imported_reviewer_id = agent_id_by_name.get("Reviewer").unwrap();

    let imported_assignments = rows_for(&imported_snapshot, "assignments");
    assert_eq!(imported_assignments.len(), 2);
    assert!(imported_assignments.iter().all(|assignment| {
        assignment["projectId"].as_str() == Some(new_project_id)
            && imported_agent_ids.contains(assignment["agentId"].as_str().unwrap())
    }));
    assert_eq!(
        row_by(imported_assignments, "agentId", imported_builder_id)["workspaceAccess"],
        json!("workspace_write")
    );
    assert_eq!(
        row_by(imported_assignments, "agentId", imported_reviewer_id)["workspaceAccess"],
        json!("read_only")
    );

    let imported_conversations = rows_for(&imported_snapshot, "conversations");
    assert_eq!(imported_conversations.len(), 2);
    assert!(imported_conversations
        .iter()
        .all(|conversation| { conversation["projectId"].as_str() == Some(new_project_id) }));
    let source_conversation_ids = ids(rows_for(&source_snapshot, "conversations"), "id");
    let imported_conversation_ids = ids(imported_conversations, "id");
    assert!(source_conversation_ids.is_disjoint(&imported_conversation_ids));

    let imported_workflows = rows_for(&imported_snapshot, "workflows");
    assert_eq!(imported_workflows.len(), 1);
    let imported_workflow = &imported_workflows[0];
    assert_ne!(imported_workflow["id"], json!(SOURCE_WORKFLOW_ID));
    assert_eq!(imported_workflow["projectId"], json!(new_project_id));
    let steps: Value = serde_json::from_str(imported_workflow["stepsJson"].as_str().unwrap())
        .expect("workflow steps must remain valid JSON");
    let step_agent_ids: HashSet<&str> = steps
        .as_array()
        .unwrap()
        .iter()
        .map(|step| step["agentId"].as_str().unwrap())
        .collect();
    assert_eq!(
        step_agent_ids,
        HashSet::from([imported_builder_id.as_str(), imported_reviewer_id.as_str()])
    );
}

#[test]
fn import_rejects_unknown_fields_before_mutating_the_target_core() {
    let source = fixture_core();
    let mut exported = source.export_project_config(SOURCE_PROJECT_ID).unwrap();
    exported["unexpectedFutureField"] = json!(true);

    let mut target =
        PersistentCore::open_with_runtime(":memory:", Box::new(MockRuntime::default())).unwrap();
    let error = target
        .import_project_config(exported)
        .expect_err("unknown fields must fail closed");
    assert!(error.to_string().contains("unsupported field"));
    assert!(rows_for(&snapshot(&target), "projects").is_empty());
    assert!(rows_for(&snapshot(&target), "agents").is_empty());
}

#[test]
fn import_rejects_an_oversized_configuration_before_mutating_the_target_core() {
    let source = fixture_core();
    let mut exported = source.export_project_config(SOURCE_PROJECT_ID).unwrap();
    // The target API must enforce a bounded serialized import size.  8 MiB is
    // deliberately beyond a reasonable portable metadata envelope.
    exported["oversizedMetadata"] = Value::String("x".repeat(8 * 1024 * 1024));

    let mut target =
        PersistentCore::open_with_runtime(":memory:", Box::new(MockRuntime::default())).unwrap();
    let error = target
        .import_project_config(exported)
        .expect_err("oversized configurations must fail closed");
    assert!(error.to_string().contains("size") || error.to_string().contains("large"));
    assert!(rows_for(&snapshot(&target), "projects").is_empty());
}

#[test]
fn import_rejects_a_missing_project_before_mutating_the_target_core() {
    let source = fixture_core();
    let mut exported = source.export_project_config(SOURCE_PROJECT_ID).unwrap();
    exported.as_object_mut().unwrap().remove("project");

    let mut target =
        PersistentCore::open_with_runtime(":memory:", Box::new(MockRuntime::default())).unwrap();
    let error = target
        .import_project_config(exported)
        .expect_err("project is required by IExportConfig");
    assert!(error.to_string().contains("project"));
    assert!(rows_for(&snapshot(&target), "projects").is_empty());
    assert!(rows_for(&snapshot(&target), "agents").is_empty());
}
