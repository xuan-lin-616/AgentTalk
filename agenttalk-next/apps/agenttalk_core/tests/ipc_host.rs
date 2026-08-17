#![cfg(windows)]

use agenttalk_brief_sealer::BriefSealer;
use agenttalk_ipc::{FramedTransport, NamedPipeClient};
use agenttalk_orchestration_contracts::registry::InMemorySchemaRegistry;
use agenttalk_protocols::{
    CommandEnvelope, ErrorEnvelope, EventEnvelope, ProtocolHandshake, ProtocolVersion,
    QueryEnvelope, ResponseEnvelope, PROTOCOL_MAJOR,
};
use agenttalk_storage::{OrchestrationRunSeed, SqliteStore};
use serde_json::json;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

struct ChildGuard(std::process::Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn assert_error_envelope(
    client: &mut agenttalk_ipc::NamedPipeConnection,
    request_id: &str,
    code: &str,
) {
    let error: ErrorEnvelope = serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(error.kind, "error");
    assert_eq!(error.protocol, ProtocolVersion { major: 1, minor: 0 });
    assert_eq!(error.request_id, request_id);
    assert_eq!(error.code, code);
    assert!(!error.message.is_empty());
    assert!(!error.retryable);
    assert!(error.details.is_none());
}

fn connect_authenticated(
    pipe: &str,
    credential: &str,
    client_id: &str,
    session_id: &str,
) -> agenttalk_ipc::NamedPipeConnection {
    let mut client = None;
    for _ in 0..100 {
        match NamedPipeClient::connect(pipe) {
            Ok(value) => {
                client = Some(value);
                break;
            }
            Err(_) => thread::sleep(Duration::from_millis(25)),
        }
    }
    let mut client = client.expect("Core host did not accept the authenticated client");
    client
        .write_json(&ProtocolHandshake {
            kind: "handshake".into(),
            protocol: ProtocolVersion {
                major: PROTOCOL_MAJOR,
                minor: 0,
            },
            client_id: client_id.into(),
            session_id: session_id.into(),
            session_credential: credential.into(),
            max_message_bytes: 1024 * 1024,
            last_seen: None,
        })
        .unwrap();
    let handshake: ResponseEnvelope = serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert!(handshake.ok);
    client
}

fn send_command(
    client: &mut agenttalk_ipc::NamedPipeConnection,
    request_id: &str,
    session_id: &str,
    command: &str,
    payload: serde_json::Value,
) -> ResponseEnvelope {
    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: request_id.into(),
            session_id: session_id.into(),
            command: command.into(),
            payload,
            deadline_ms: None,
        })
        .unwrap();
    let raw = client.read_json().unwrap();
    serde_json::from_slice(&raw).unwrap_or_else(|error| {
        panic!(
            "expected response envelope, got {}: {error}",
            String::from_utf8_lossy(&raw)
        )
    })
}

fn send_command_error(
    client: &mut agenttalk_ipc::NamedPipeConnection,
    request_id: &str,
    session_id: &str,
    command: &str,
    payload: serde_json::Value,
) -> ErrorEnvelope {
    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: request_id.into(),
            session_id: session_id.into(),
            command: command.into(),
            payload,
            deadline_ms: None,
        })
        .unwrap();
    serde_json::from_slice(&client.read_json().unwrap()).unwrap()
}

fn send_query(
    client: &mut agenttalk_ipc::NamedPipeConnection,
    request_id: &str,
    session_id: &str,
    query: &str,
    payload: serde_json::Value,
) -> ResponseEnvelope {
    client
        .write_json(&QueryEnvelope {
            kind: "query".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: request_id.into(),
            session_id: session_id.into(),
            query: query.into(),
            payload,
        })
        .unwrap();
    let raw = client.read_json().unwrap();
    serde_json::from_slice(&raw).unwrap_or_else(|error| {
        panic!(
            "expected query response envelope, got {}: {error}",
            String::from_utf8_lossy(&raw)
        )
    })
}

#[test]
fn core_host_accepts_handshake_command_and_query_over_named_pipe() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after Unix epoch")
        .as_nanos();
    let pipe = format!(
        "\\\\.\\pipe\\agenttalk-core-host-test-{}-{}",
        std::process::id(),
        nonce
    );
    let database = std::env::temp_dir().join(format!(
        "agenttalk-core-host-test-{}-{}.db",
        std::process::id(),
        nonce
    ));
    let artifact_root = std::env::temp_dir().join(format!(
        "agenttalk-core-host-artifacts-{}-{}",
        std::process::id(),
        nonce
    ));
    let selected_file = std::env::temp_dir().join(format!(
        "agenttalk-core-host-selected-{}-{}-large.bin",
        std::process::id(),
        nonce
    ));
    let _ = std::fs::remove_file(&database);
    let _ = std::fs::remove_file(database.with_extension("db-wal"));
    let _ = std::fs::remove_file(database.with_extension("db-shm"));
    let credential = format!("test-credential-{}", "x".repeat(40));
    {
        let mut storage = SqliteStore::open(&database).unwrap();
        storage
            .create_orchestration_run(OrchestrationRunSeed {
                run_id: "orchestration-host-run".into(),
                project_id: "project-host-1".into(),
                brief_snapshot_id: format!("sha256:{}", "0".repeat(64)),
                brief_tree_digest: "0".repeat(64),
                dag_snapshot_digest: "1".repeat(64),
                role_binding_snapshot_digest: "2".repeat(64),
            })
            .unwrap();
    }
    let executable = env!("CARGO_BIN_EXE_agenttalk-core");
    let child = Command::new(executable)
        .args([
            pipe.clone(),
            database.to_string_lossy().into_owned(),
            artifact_root.to_string_lossy().into_owned(),
        ])
        .env("AGENTTALK_CORE_SESSION_CREDENTIAL", &credential)
        .env("AGENTTALK_CORE_RUNTIME", "mock")
        .env("AGENTTALK_CORE_DEV_MODE", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let _child = ChildGuard(child);
    let mut rejected_client = None;
    for _ in 0..100 {
        match NamedPipeClient::connect(&pipe) {
            Ok(value) => {
                rejected_client = Some(value);
                break;
            }
            Err(_) => thread::sleep(Duration::from_millis(25)),
        }
    }
    let mut rejected_client = rejected_client.expect("Core host did not create its Named Pipe");
    rejected_client
        .write_json(&ProtocolHandshake {
            kind: "handshake".into(),
            protocol: ProtocolVersion {
                major: PROTOCOL_MAJOR,
                minor: 0,
            },
            client_id: "flutter-test".into(),
            session_id: "session-host-test-123456".into(),
            session_credential: format!("bad-credential-{}", "y".repeat(40)),
            max_message_bytes: 1024 * 1024,
            last_seen: None,
        })
        .unwrap();
    let rejected: ErrorEnvelope =
        serde_json::from_slice(&rejected_client.read_json().unwrap()).unwrap();
    assert_eq!(rejected.code, "INVALID_HANDSHAKE");
    drop(rejected_client);

    let mut client = None;
    for _ in 0..100 {
        match NamedPipeClient::connect(&pipe) {
            Ok(value) => {
                client = Some(value);
                break;
            }
            Err(_) => thread::sleep(Duration::from_millis(25)),
        }
    }
    let mut client = client.expect("Core host did not accept the authenticated client");
    client
        .write_json(&ProtocolHandshake {
            kind: "handshake".into(),
            protocol: ProtocolVersion {
                major: PROTOCOL_MAJOR,
                minor: 0,
            },
            client_id: "flutter-test".into(),
            session_id: "session-host-test-123456".into(),
            session_credential: credential.clone(),
            max_message_bytes: 1024 * 1024,
            last_seen: None,
        })
        .unwrap();
    let handshake: ResponseEnvelope = serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert!(handshake.ok);
    assert!(handshake.payload["serverEpoch"]
        .as_str()
        .unwrap()
        .starts_with("core-"));
    assert_eq!(handshake.payload["eventStreamId"], "core-events");

    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "assign-1".into(),
            session_id: "session-host-test-123456".into(),
            command: "roster.assign".into(),
            payload: json!({"agentId":"agent-fixture"}),
            deadline_ms: None,
        })
        .unwrap();
    assert_error_envelope(&mut client, "assign-1", "UNSUPPORTED_COMMAND");

    client
        .write_json(&json!({
            "kind": "command",
            "protocol": {"major": 1, "minor": 0},
            "requestId": "malformed-command-1",
            "sessionId": "session-host-test-123456",
            "command": "project.create"
        }))
        .unwrap();
    assert_error_envelope(&mut client, "malformed-command-1", "INVALID_COMMAND");

    client
        .write_json(&json!({
            "kind": "command",
            "protocol": {"major": 1, "minor": 0},
            "requestId": "array-command-1",
            "sessionId": "session-host-test-123456",
            "command": "project.create",
            "payload": []
        }))
        .unwrap();
    assert_error_envelope(&mut client, "array-command-1", "INVALID_COMMAND");

    client
        .write_json(&QueryEnvelope {
            kind: "query".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "unknown-query-1".into(),
            session_id: "session-host-test-123456".into(),
            query: "query.not-implemented".into(),
            payload: json!({}),
        })
        .unwrap();
    assert_error_envelope(&mut client, "unknown-query-1", "UNSUPPORTED_QUERY");

    client
        .write_json(&json!({
            "kind": "query",
            "protocol": {"major": 1, "minor": 0},
            "requestId": "array-query-1",
            "sessionId": "session-host-test-123456",
            "query": "runtime.health",
            "payload": []
        }))
        .unwrap();
    assert_error_envelope(&mut client, "array-query-1", "INVALID_QUERY");

    client
        .write_json(&QueryEnvelope {
            kind: "query".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "mismatch-1".into(),
            session_id: "session-other-123456".into(),
            query: "runtime.health".into(),
            payload: json!({}),
        })
        .unwrap();
    let mismatch: ErrorEnvelope = serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(mismatch.code, "SESSION_MISMATCH");

    client
        .write_json(&QueryEnvelope {
            kind: "query".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "health-1".into(),
            session_id: "session-host-test-123456".into(),
            query: "runtime.health".into(),
            payload: json!({}),
        })
        .unwrap();
    let health: ResponseEnvelope = serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(health.payload["status"], "ready");
    assert_eq!(health.payload["schemaVersion"], "runtime.health.v1");
    assert_eq!(health.payload["connectorId"], "mock");
    assert_eq!(health.payload["availability"], "available");
    assert_eq!(
        health.payload["connectors"][0]["verification"],
        "local_adapter_only"
    );
    assert_eq!(health.payload["healthDetailRedacted"], true);
    let health_serialized = serde_json::to_string(&health.payload)
        .unwrap()
        .to_ascii_lowercase();
    for forbidden in ["token", "secret", "authorization", "bearer"] {
        assert!(!health_serialized.contains(forbidden));
    }

    client
        .write_json(&QueryEnvelope {
            kind: "query".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "snapshot-1".into(),
            session_id: "session-host-test-123456".into(),
            query: "projection.snapshot".into(),
            payload: json!({}),
        })
        .unwrap();
    let snapshot: ResponseEnvelope = serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert!(snapshot.payload["projects"].is_array());
    assert!(snapshot.payload["assignments"].is_array());

    client
        .write_json(&QueryEnvelope {
            kind: "query".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "orchestration-snapshot-1".into(),
            session_id: "session-host-test-123456".into(),
            query: "orchestration.run.snapshot".into(),
            payload: json!({"runId":"orchestration-host-run"}),
        })
        .unwrap();
    let orchestration: ResponseEnvelope =
        serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(
        orchestration.payload["run"]["runId"],
        "orchestration-host-run"
    );
    assert_eq!(orchestration.payload["run"]["projectId"], "project-host-1");
    assert!(orchestration.payload["nodes"].is_array());
    assert!(orchestration.payload["machineAcceptances"].is_array());
    assert!(!serde_json::to_string(&orchestration.payload)
        .unwrap()
        .contains("artifact sealed bytes"));

    client
        .write_json(&QueryEnvelope {
            kind: "query".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "orchestration-recovery-1".into(),
            session_id: "session-host-test-123456".into(),
            query: "orchestration.run.recovery_state".into(),
            payload: json!({"runId":"orchestration-host-run"}),
        })
        .unwrap();
    let recovery: ResponseEnvelope = serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(recovery.payload["runId"], "orchestration-host-run");
    assert_eq!(recovery.payload["coordinatorGeneration"], 2);
    assert!(recovery.payload["nodes"].as_array().unwrap().is_empty());

    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "project-create-1".into(),
            session_id: "session-host-test-123456".into(),
            command: "project.create".into(),
            payload: json!({"projectId":"project-host-1", "name":"Host Project"}),
            deadline_ms: None,
        })
        .unwrap();
    let project: ResponseEnvelope = serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(project.payload["changed"], true);

    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "artifact-store-body-1".into(),
            session_id: "session-host-test-123456".into(),
            command: "artifact.store".into(),
            payload: json!({
                "artifactId": "artifact-host-body",
                "sha256": "3c92e11f7dc655e18f5b934fb173611bf6f20a337f054b9f4e7c325272b365d7",
                "size": 18,
                "mime": "text/plain",
                "relativePath": "notes/host.txt",
                "bodyBase64": "cGlwZSBhcnRpZmFjdCBib2R5"
            }),
            deadline_ms: None,
        })
        .unwrap();
    let artifact: ResponseEnvelope = serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(artifact.payload["created"], true);
    assert_eq!(artifact.payload["bodyStored"], true);
    assert_eq!(
        artifact.payload["projection"]["artifacts"][0]["id"],
        "artifact-host-body"
    );
    assert!(!serde_json::to_string(&artifact.payload)
        .unwrap()
        .contains("pipe artifact body"));
    assert_eq!(
        std::fs::read(
            artifact_root
                .join("3c92e11f7dc655e18f5b934fb173611bf6f20a337f054b9f4e7c325272b365d7.blob")
        )
        .unwrap(),
        b"pipe artifact body"
    );

    client
        .write_json(&QueryEnvelope {
            kind: "query".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "artifact-content-first-chunk-1".into(),
            session_id: "session-host-test-123456".into(),
            query: "artifact.content".into(),
            payload: json!({
                "artifactId": "artifact-host-body",
                "offset": 5,
                "limit": 8
            }),
        })
        .unwrap();
    let first_chunk: ResponseEnvelope =
        serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(first_chunk.payload["artifactId"], "artifact-host-body");
    assert_eq!(first_chunk.payload["offset"], 5);
    assert_eq!(first_chunk.payload["size"], 18);
    assert_eq!(first_chunk.payload["chunkBase64"], "YXJ0aWZhY3Q=");
    assert_eq!(first_chunk.payload["chunkBytes"], 8);
    assert_eq!(first_chunk.payload["eof"], false);
    assert!(!serde_json::to_string(&first_chunk.payload)
        .unwrap()
        .contains("pipe artifact body"));

    client
        .write_json(&QueryEnvelope {
            kind: "query".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "artifact-content-last-chunk-1".into(),
            session_id: "session-host-test-123456".into(),
            query: "artifact.content".into(),
            payload: json!({
                "artifactId": "artifact-host-body",
                "offset": 13,
                "limit": 65536
            }),
        })
        .unwrap();
    let last_chunk: ResponseEnvelope =
        serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(last_chunk.payload["offset"], 13);
    assert_eq!(last_chunk.payload["chunkBase64"], "IGJvZHk=");
    assert_eq!(last_chunk.payload["chunkBytes"], 5);
    assert_eq!(last_chunk.payload["eof"], true);

    client
        .write_json(&QueryEnvelope {
            kind: "query".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "artifact-content-invalid-limit-1".into(),
            session_id: "session-host-test-123456".into(),
            query: "artifact.content".into(),
            payload: json!({
                "artifactId": "artifact-host-body",
                "offset": 0,
                "limit": 65537
            }),
        })
        .unwrap();
    assert_error_envelope(
        &mut client,
        "artifact-content-invalid-limit-1",
        "INVALID_QUERY",
    );

    client
        .write_json(&QueryEnvelope {
            kind: "query".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "replay-invalid-cursor-1".into(),
            session_id: "session-host-test-123456".into(),
            query: "events.replay".into(),
            payload: json!({"afterSequence": -1}),
        })
        .unwrap();
    assert_error_envelope(&mut client, "replay-invalid-cursor-1", "INVALID_QUERY");

    client
        .write_json(&QueryEnvelope {
            kind: "query".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "replay-max-cursor-1".into(),
            session_id: "session-host-test-123456".into(),
            query: "events.replay".into(),
            payload: json!({"afterSequence": u64::MAX}),
        })
        .unwrap();
    let replay: ResponseEnvelope = serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert!(replay.payload["events"].as_array().unwrap().is_empty());
    assert_eq!(replay.payload["nextSequence"], json!(u64::MAX));

    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "agent-create-1".into(),
            session_id: "session-host-test-123456".into(),
            command: "agent.create".into(),
            payload: json!({
                "agentId":"agent-host-1",
                "name":"Host Agent",
                "role":"builder",
                "specialty":"code",
                "systemPrompt":"system"
            }),
            deadline_ms: None,
        })
        .unwrap();
    let agent: ResponseEnvelope = serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(agent.payload["changed"], true);
    assert_eq!(
        agent.payload["projection"]["agents"][0]["systemPrompt"],
        "system"
    );

    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "agent-create-2".into(),
            session_id: "session-host-test-123456".into(),
            command: "agent.create".into(),
            payload: json!({
                "agentId":"agent-host-2",
                "name":"Host Agent Two",
                "role":"reviewer",
                "specialty":"tests",
                "systemPrompt":"system-two"
            }),
            deadline_ms: None,
        })
        .unwrap();
    let agent_two: ResponseEnvelope = serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(agent_two.payload["changed"], true);

    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "conversation-create-1".into(),
            session_id: "session-host-test-123456".into(),
            command: "conversation.create".into(),
            payload: json!({
                "conversationId":"conversation-host-1",
                "projectId":"project-host-1",
                "title":"Host Conversation"
            }),
            deadline_ms: None,
        })
        .unwrap();
    let conversation: ResponseEnvelope =
        serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(conversation.payload["changed"], true);

    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "project-update-1".into(),
            session_id: "session-host-test-123456".into(),
            command: "project.update".into(),
            payload: json!({
                "projectId":"project-host-1",
                "name":"Renamed Project",
                "rootPath":null,
                "archived":false
            }),
            deadline_ms: None,
        })
        .unwrap();
    let project_update: ResponseEnvelope =
        serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(project_update.payload["changed"], true);

    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "conversation-update-1".into(),
            session_id: "session-host-test-123456".into(),
            command: "conversation.update".into(),
            payload: json!({
                "conversationId":"conversation-host-1",
                "title":"Renamed Conversation"
            }),
            deadline_ms: None,
        })
        .unwrap();
    let conversation_update: ResponseEnvelope =
        serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(conversation_update.payload["changed"], true);

    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "assignment-set-1".into(),
            session_id: "session-host-test-123456".into(),
            command: "project_agent.set".into(),
            payload: json!({
                "projectId":"project-host-1",
                "agentId":"agent-host-1",
                "enabled":true,
                "workspaceAccess":"read_only"
            }),
            deadline_ms: None,
        })
        .unwrap();
    let assignment: ResponseEnvelope =
        serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(assignment.payload["changed"], true);

    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "assignment-set-2".into(),
            session_id: "session-host-test-123456".into(),
            command: "project_agent.set".into(),
            payload: json!({
                "projectId":"project-host-1",
                "agentId":"agent-host-2",
                "enabled":true,
                "workspaceAccess":"none"
            }),
            deadline_ms: None,
        })
        .unwrap();
    let assignment_two: ResponseEnvelope =
        serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(assignment_two.payload["changed"], true);

    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "conversation-assignment-unassigned".into(),
            session_id: "session-host-test-123456".into(),
            command: "conversation_agent.set".into(),
            payload: json!({
                "conversationId":"conversation-host-1",
                "agentId":"agent-not-assigned"
            }),
            deadline_ms: None,
        })
        .unwrap();
    assert_error_envelope(
        &mut client,
        "conversation-assignment-unassigned",
        "COMMAND_REJECTED",
    );

    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "conversation-assignment-set-1".into(),
            session_id: "session-host-test-123456".into(),
            command: "conversation_agent.set".into(),
            payload: json!({
                "conversationId":"conversation-host-1",
                "agentId":"agent-host-1",
                "enabled":true
            }),
            deadline_ms: None,
        })
        .unwrap();
    let conversation_assignment: ResponseEnvelope =
        serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(conversation_assignment.payload["changed"], true);
    let conversation_assignments = conversation_assignment.payload["projection"]
        ["conversationAgents"]
        .as_array()
        .unwrap();
    assert_eq!(conversation_assignments.len(), 1);
    assert_eq!(conversation_assignments[0]["agentId"], "agent-host-1");
    assert_eq!(conversation_assignments[0]["enabled"], true);
    assert!(conversation_assignments[0].get("systemPrompt").is_none());

    let binding = send_command(
        &mut client,
        "model-binding-set-1",
        "session-host-test-123456",
        "agent.model_binding.set",
        json!({
            "agentId": "agent-host-1",
            "connectorId": "mock",
            "modelId": null,
            "candidateModelListRevision": 1
        }),
    );
    assert_eq!(binding.payload["changed"], true);
    let selection_assignment = send_command(
        &mut client,
        "conversation-selection-set-1",
        "session-host-test-123456",
        "conversation_agent.set",
        json!({
            "conversationId": "conversation-host-1",
            "agentId": "agent-host-1",
            "enabled": true,
            "modelSelectionMode": "inherit",
            "modelId": null,
            "candidateModelListMode": "override",
            "candidateModelListRevision": 2
        }),
    );
    assert_eq!(selection_assignment.payload["changed"], true);
    for (request_id, option_id, model_id, is_default, sort_order) in [
        (
            "model-option-upsert-a",
            "option-host-a",
            "model-host-a",
            true,
            0,
        ),
        (
            "model-option-upsert-b",
            "option-host-b",
            "model-host-b",
            false,
            1,
        ),
    ] {
        let option = send_command(
            &mut client,
            request_id,
            "session-host-test-123456",
            "identity_model_option.upsert",
            json!({
                "id": option_id,
                "identityScope": "conversation_agent",
                "agentId": "agent-host-1",
                "projectId": null,
                "conversationId": "conversation-host-1",
                "modelId": model_id,
                "displayName": model_id,
                "connectorId": "mock",
                "source": "manual",
                "availability": "unverified",
                "isDefault": is_default,
                "sortOrder": sort_order,
                "catalogRevision": "host-r1",
                "contextWindow": null,
                "reasoningEfforts": [],
                "serviceTiers": []
            }),
        );
        assert_eq!(option.payload["changed"], true);
    }
    client
        .write_json(&QueryEnvelope {
            kind: "query".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "model-options-list-1".into(),
            session_id: "session-host-test-123456".into(),
            query: "identity_model_options.list".into(),
            payload: json!({
                "identityScope": "conversation_agent",
                "agentId": "agent-host-1",
                "conversationId": "conversation-host-1",
                "connectorId": "mock"
            }),
        })
        .unwrap();
    let model_options: ResponseEnvelope =
        serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(
        model_options.payload["options"].as_array().unwrap().len(),
        2
    );

    let model_start = send_command(
        &mut client,
        "model-selection-start-1",
        "session-host-test-123456",
        "execution.start",
        json!({
            "executionRunId": "run-host-model-selection",
            "collaborationRunId": "collab-host-model-selection",
            "projectId": "project-host-1",
            "conversationId": "conversation-host-1",
            "agentId": "agent-host-1",
            "workspaceAccess": "none",
            "currentTask": "freeze selected model"
        }),
    );
    assert_eq!(model_start.payload["run"]["status"], "Completed");
    client
        .write_json(&QueryEnvelope {
            kind: "query".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "model-selection-snapshot-1".into(),
            session_id: "session-host-test-123456".into(),
            query: "model_selection.snapshot".into(),
            payload: json!({"executionRunId": "run-host-model-selection"}),
        })
        .unwrap();
    let model_snapshot: ResponseEnvelope =
        serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(
        model_snapshot.payload["selectionSnapshot"]["effectiveModelId"],
        "model-host-a"
    );
    assert_eq!(
        model_snapshot.payload["modelSnapshot"]["modelId"],
        "model-host-a"
    );

    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "conversation-roster-expansion".into(),
            session_id: "session-host-test-123456".into(),
            command: "execution.start".into(),
            payload: json!({
                "executionRunId":"run-host-conversation-expansion",
                "collaborationRunId":"collab-host-1",
                "projectId":"project-host-1",
                "conversationId":"conversation-host-1",
                "agentId":"agent-host-2",
                "workspaceAccess":"none"
            }),
            deadline_ms: None,
        })
        .unwrap();
    assert_error_envelope(
        &mut client,
        "conversation-roster-expansion",
        "COMMAND_REJECTED",
    );

    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "conversation-assignment-remove-1".into(),
            session_id: "session-host-test-123456".into(),
            command: "conversation_agent.remove".into(),
            payload: json!({
                "conversationId":"conversation-host-1",
                "agentId":"agent-host-1"
            }),
            deadline_ms: None,
        })
        .unwrap();
    let conversation_assignment_remove: ResponseEnvelope =
        serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(conversation_assignment_remove.payload["changed"], true);
    assert!(
        conversation_assignment_remove.payload["projection"]["conversationAgents"]
            .as_array()
            .unwrap()
            .is_empty()
    );

    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "conversation-roster-inherited".into(),
            session_id: "session-host-test-123456".into(),
            command: "execution.start".into(),
            payload: json!({
                "executionRunId":"run-host-conversation-inherited",
                "collaborationRunId":"collab-host-1",
                "projectId":"project-host-1",
                "conversationId":"conversation-host-1",
                "agentId":"agent-host-2",
                "workspaceAccess":"none"
            }),
            deadline_ms: None,
        })
        .unwrap();
    let inherited: ResponseEnvelope = serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(inherited.payload["run"]["agentId"], "agent-host-2");
    assert_eq!(inherited.payload["run"]["status"], "Completed");

    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "conversation-roster-inherited".into(),
            session_id: "session-host-test-123456".into(),
            command: "execution.start".into(),
            payload: json!({
                "executionRunId":"run-host-conversation-inherited",
                "collaborationRunId":"collab-host-1",
                "projectId":"project-host-1",
                "conversationId":"conversation-host-1",
                "agentId":"agent-host-2",
                "workspaceAccess":"none"
            }),
            deadline_ms: None,
        })
        .unwrap();
    let replayed: ResponseEnvelope = serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(
        replayed.payload["run"]["id"],
        "run-host-conversation-inherited"
    );

    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "execution-retry-1".into(),
            session_id: "session-host-test-123456".into(),
            command: "execution.retry".into(),
            payload: json!({
                "executionRunId":"run-host-conversation-retry",
                "sourceExecutionRunId":"run-host-conversation-inherited",
                "currentTask":"retry the inherited run"
            }),
            deadline_ms: None,
        })
        .unwrap();
    let retried: ResponseEnvelope = serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(retried.payload["run"]["id"], "run-host-conversation-retry");
    assert_eq!(
        retried.payload["sourceExecutionRunId"],
        "run-host-conversation-inherited"
    );
    assert_eq!(retried.payload["run"]["status"], "Completed");

    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "execution-rerun-current-1".into(),
            session_id: "session-host-test-123456".into(),
            command: "execution.rerun_current".into(),
            payload: json!({
                "executionRunId":"run-host-conversation-rerun-current",
                "sourceExecutionRunId":"run-host-conversation-inherited",
                "currentTask":"resolve current runtime settings"
            }),
            deadline_ms: None,
        })
        .unwrap();
    let rerun_current: ResponseEnvelope =
        serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(
        rerun_current.payload["run"]["id"],
        "run-host-conversation-rerun-current"
    );
    assert_eq!(
        rerun_current.payload["sourceExecutionRunId"],
        "run-host-conversation-inherited"
    );
    assert_eq!(rerun_current.payload["run"]["status"], "Completed");

    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "conversation-roster-inherited".into(),
            session_id: "session-host-test-123456".into(),
            command: "execution.start".into(),
            payload: json!({
                "executionRunId":"run-host-conversation-inherited-reused",
                "collaborationRunId":"collab-host-1",
                "projectId":"project-host-1",
                "conversationId":"conversation-host-1",
                "agentId":"agent-host-2",
                "workspaceAccess":"none"
            }),
            deadline_ms: None,
        })
        .unwrap();
    assert_error_envelope(
        &mut client,
        "conversation-roster-inherited",
        "REQUEST_ID_REUSE",
    );

    client
        .write_json(&QueryEnvelope {
            kind: "query".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "events-replay-runtime-1".into(),
            session_id: "session-host-test-123456".into(),
            query: "events.replay".into(),
            payload: json!({"afterSequence": 0}),
        })
        .unwrap();
    let runtime_replay: ResponseEnvelope =
        serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    let runtime_events = runtime_replay.payload["events"].as_array().unwrap();
    assert!(runtime_events.iter().any(|event| {
        event["executionRunId"] == "run-host-conversation-inherited"
            && event["event"] == "runtime.started"
    }));
    assert!(runtime_events.iter().any(|event| {
        event["executionRunId"] == "run-host-conversation-inherited"
            && event["event"] == "output.delta"
    }));
    assert!(runtime_events.iter().any(|event| {
        event["executionRunId"] == "run-host-conversation-inherited"
            && event["event"] == "execution.completed"
    }));

    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "message-create-1".into(),
            session_id: "session-host-test-123456".into(),
            command: "message.create".into(),
            payload: json!({
                "messageId":"message-host-1",
                "conversationId":"conversation-host-1",
                "senderId":"user",
                "sequence":1,
                "content":"hello"
            }),
            deadline_ms: None,
        })
        .unwrap();
    let message: ResponseEnvelope = serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(
        message.payload["projection"]["messages"][0]["content"],
        "hello"
    );

    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "attachment-store-1".into(),
            session_id: "session-host-test-123456".into(),
            command: "attachment.store".into(),
            payload: json!({
                "attachmentId": "attachment-host-1",
                "artifactId": "artifact-host-body",
                "messageId": "message-host-1",
                "ordinal": 0,
                "fileName": "host.txt",
                "sha256": "3c92e11f7dc655e18f5b934fb173611bf6f20a337f054b9f4e7c325272b365d7",
                "size": 18
            }),
            deadline_ms: None,
        })
        .unwrap();
    let attachment: ResponseEnvelope =
        serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(attachment.payload["created"], true);
    assert_eq!(
        attachment.payload["projection"]["attachments"][0]["attachmentId"],
        "attachment-host-1"
    );
    assert_eq!(
        attachment.payload["projection"]["attachments"][0]["artifactId"],
        "artifact-host-body"
    );
    assert!(!serde_json::to_string(&attachment.payload)
        .unwrap()
        .contains("pipe artifact body"));

    let selected_body = vec![0x5a; 600 * 1024];
    std::fs::write(&selected_file, &selected_body).unwrap();
    let import_payload = json!({
        "attachmentId": "attachment-host-file-1",
        "artifactId": "artifact-host-file-1",
        "messageId": "message-host-1",
        "sourcePath": selected_file.to_string_lossy(),
        "mime": "application/octet-stream",
        "ordinal": 1
    });
    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "attachment-import-file-1".into(),
            session_id: "session-host-test-123456".into(),
            command: "attachment.import_file".into(),
            payload: import_payload.clone(),
            deadline_ms: None,
        })
        .unwrap();
    let imported: ResponseEnvelope = serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(imported.payload["created"], true);
    assert_eq!(imported.payload["artifactCreated"], true);
    assert_eq!(imported.payload["bodyStored"], true);
    assert_eq!(imported.payload["artifact"]["size"], 600 * 1024);
    assert_eq!(
        imported.payload["attachment"]["fileName"],
        selected_file
            .file_name()
            .unwrap()
            .to_string_lossy()
            .as_ref()
    );
    let imported_serialized = serde_json::to_string(&imported.payload).unwrap();
    assert!(!imported_serialized.contains(&selected_file.to_string_lossy().to_string()));
    assert!(!imported_serialized.contains(&"Z".repeat(256)));

    std::fs::remove_file(&selected_file).unwrap();
    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "attachment-import-file-1".into(),
            session_id: "session-host-test-123456".into(),
            command: "attachment.import_file".into(),
            payload: import_payload,
            deadline_ms: None,
        })
        .unwrap();
    let replayed_import: ResponseEnvelope =
        serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(replayed_import.payload, imported.payload);

    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "summary-generate-1".into(),
            session_id: "session-host-test-123456".into(),
            command: "summary.generate".into(),
            payload: json!({"scopeId": "conversation-host-1"}),
            deadline_ms: None,
        })
        .unwrap();
    let generated_summary: ResponseEnvelope =
        serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(
        generated_summary.payload["generator"],
        "local-deterministic-v1"
    );
    assert_eq!(generated_summary.payload["messageCount"], 1);
    assert!(generated_summary.payload["summary"]["artifactId"].is_string());
    assert!(!serde_json::to_string(&generated_summary.payload)
        .unwrap()
        .contains("AgentTalk summary (local-deterministic-v1)"));

    let summary_id = generated_summary.payload["summary"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    client
        .write_json(&QueryEnvelope {
            kind: "query".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "summary-content-1".into(),
            session_id: "session-host-test-123456".into(),
            query: "summary.content".into(),
            payload: json!({"summaryId": summary_id}),
        })
        .unwrap();
    let summary_content: ResponseEnvelope =
        serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert!(summary_content.payload["content"]
        .as_str()
        .unwrap()
        .contains("hello"));

    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "memory-store-1".into(),
            session_id: "session-host-test-123456".into(),
            command: "memory.store".into(),
            payload: json!({
                "memoryId": "memory-host-1",
                "scopeId": "project-host-1",
                "agentId": "agent-host-1",
                "contentHash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "confirmed": true
            }),
            deadline_ms: None,
        })
        .unwrap();
    let stored_memory: ResponseEnvelope =
        serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(stored_memory.payload["created"], true);
    assert_eq!(
        stored_memory.payload["projection"]["memories"][0]["id"],
        "memory-host-1"
    );

    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "memory-store-2".into(),
            session_id: "session-host-test-123456".into(),
            command: "memory.store".into(),
            payload: json!({
                "memoryId": "memory-host-1",
                "scopeId": "project-host-1",
                "agentId": "agent-host-1",
                "contentHash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "confirmed": true
            }),
            deadline_ms: None,
        })
        .unwrap();
    let repeated_memory: ResponseEnvelope =
        serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(repeated_memory.payload["alreadyPresent"], true);

    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "memory-store-conflict".into(),
            session_id: "session-host-test-123456".into(),
            command: "memory.store".into(),
            payload: json!({
                "memoryId": "memory-host-1",
                "scopeId": "project-host-1",
                "agentId": "agent-host-1",
                "contentHash": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "confirmed": true
            }),
            deadline_ms: None,
        })
        .unwrap();
    assert_error_envelope(&mut client, "memory-store-conflict", "COMMAND_REJECTED");

    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "retrieval-store-1".into(),
            session_id: "session-host-test-123456".into(),
            command: "retrieval.store".into(),
            payload: json!({
                "retrievalSourceId": "retrieval-host-1",
                "scopeId": "project-host-1",
                "citation": "docs/README.md#scope",
                "sha256": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                "tokenCount": 42
            }),
            deadline_ms: None,
        })
        .unwrap();
    let retrieval: ResponseEnvelope = serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(retrieval.payload["created"], true);
    assert_eq!(
        retrieval.payload["projection"]["retrievalSources"][0]["id"],
        "retrieval-host-1"
    );

    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "retrieval-store-1".into(),
            session_id: "session-host-test-123456".into(),
            command: "retrieval.store".into(),
            payload: json!({
                "retrievalSourceId": "retrieval-host-1",
                "scopeId": "project-host-1",
                "citation": "docs/README.md#scope",
                "sha256": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                "tokenCount": 42
            }),
            deadline_ms: None,
        })
        .unwrap();
    let retrieval_replay: ResponseEnvelope =
        serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(retrieval_replay.payload["created"], true);

    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "retrieval-store-conflict".into(),
            session_id: "session-host-test-123456".into(),
            command: "retrieval.store".into(),
            payload: json!({
                "retrievalSourceId": "retrieval-host-1",
                "scopeId": "project-host-1",
                "citation": "docs/README.md#changed",
                "sha256": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                "tokenCount": 42
            }),
            deadline_ms: None,
        })
        .unwrap();
    assert_error_envelope(&mut client, "retrieval-store-conflict", "COMMAND_REJECTED");

    client
        .write_json(&QueryEnvelope {
            kind: "query".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "retrieval-query-1".into(),
            session_id: "session-host-test-123456".into(),
            query: "retrieval.query".into(),
            payload: json!({
                "scopeId": "project-host-1",
                "sourceIds": ["retrieval-host-1"],
                "limit": 10
            }),
        })
        .unwrap();
    let retrieval_query: ResponseEnvelope =
        serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(
        retrieval_query.payload["retrievalSources"][0]["id"],
        "retrieval-host-1"
    );

    client
        .write_json(&QueryEnvelope {
            kind: "query".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "retrieval-query-invalid-scope".into(),
            session_id: "session-host-test-123456".into(),
            query: "retrieval.query".into(),
            payload: json!({"scopeId": "missing-scope"}),
        })
        .unwrap();
    assert_error_envelope(
        &mut client,
        "retrieval-query-invalid-scope",
        "QUERY_REJECTED",
    );

    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "retrieval-select-1".into(),
            session_id: "session-host-test-123456".into(),
            command: "retrieval.select".into(),
            payload: json!({
                "selectionId": "selection-host-1",
                "scope": "project",
                "scopeId": "project-host-1",
                "projectId": "project-host-1",
                "conversationId": null,
                "scopeRevision": 0,
                "workspaceRevision": null,
                "retrievalVersion": "v1",
                "queryHash": "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
                "items": [{
                    "sourceId": "retrieval-host-1",
                    "sourceHash": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                    "rank": 1,
                    "scoreMilli": 950,
                    "matchMethod": "explicit_selection",
                    "reason": "explicit_user_choice",
                    "range": null
                }]
            }),
            deadline_ms: None,
        })
        .unwrap();
    let selection: ResponseEnvelope = serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(selection.payload["created"], true);
    assert_eq!(
        selection.payload["projection"]["retrievalSelections"][0]["id"],
        "selection-host-1"
    );

    client
        .write_json(&QueryEnvelope {
            kind: "query".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "retrieval-selections-query-1".into(),
            session_id: "session-host-test-123456".into(),
            query: "retrieval.selections".into(),
            payload: json!({
                "scopeId": "project-host-1",
                "selectionIds": ["selection-host-1"],
                "limit": 10
            }),
        })
        .unwrap();
    let selections: ResponseEnvelope =
        serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(
        selections.payload["retrievalSelections"][0]["id"],
        "selection-host-1"
    );

    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "retrieval-feedback-1".into(),
            session_id: "session-host-test-123456".into(),
            command: "retrieval.feedback".into(),
            payload: json!({
                "feedbackId": "feedback-host-1",
                "selectionId": "selection-host-1",
                "scopeId": "project-host-1",
                "sourceId": "retrieval-host-1",
                "label": "helpful",
                "reason": "exact_match",
                "createdAtMs": 1
            }),
            deadline_ms: None,
        })
        .unwrap();
    let feedback: ResponseEnvelope = serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(feedback.payload["created"], true);
    assert_eq!(
        feedback.payload["projection"]["retrievalFeedback"][0]["id"],
        "feedback-host-1"
    );

    client
        .write_json(&QueryEnvelope {
            kind: "query".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "retrieval-feedback-query-1".into(),
            session_id: "session-host-test-123456".into(),
            query: "retrieval.feedback".into(),
            payload: json!({
                "scopeId": "project-host-1",
                "selectionId": "selection-host-1",
                "limit": 10
            }),
        })
        .unwrap();
    let feedback_query: ResponseEnvelope =
        serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(
        feedback_query.payload["retrievalFeedback"][0]["sourceId"],
        "retrieval-host-1"
    );

    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "workflow-create-1".into(),
            session_id: "session-host-test-123456".into(),
            command: "workflow.create".into(),
            payload: json!({
                "projectId": "project-host-1",
                "workflowId": "workflow-host-1",
                "name": "Review workflow",
                "kind": "sequential",
                "steps": [{
                    "id": "step-1",
                    "order": 1,
                    "agentId": "agent-host-1",
                    "promptSupplement": "Review the change"
                }]
            }),
            deadline_ms: None,
        })
        .unwrap();
    let workflow: ResponseEnvelope = serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(workflow.payload["created"], true);
    assert_eq!(
        workflow.payload["projection"]["workflows"][0]["id"],
        "workflow-host-1"
    );

    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "workflow-create-1".into(),
            session_id: "session-host-test-123456".into(),
            command: "workflow.create".into(),
            payload: json!({
                "projectId": "project-host-1",
                "workflowId": "workflow-host-1",
                "name": "Review workflow",
                "kind": "sequential",
                "steps": [{
                    "id": "step-1",
                    "order": 1,
                    "agentId": "agent-host-1",
                    "promptSupplement": "Review the change"
                }]
            }),
            deadline_ms: None,
        })
        .unwrap();
    let workflow_replay: ResponseEnvelope =
        serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(workflow_replay.payload["created"], true);
    assert_eq!(
        workflow_replay.payload["projection"]["workflows"]
            .as_array()
            .unwrap()
            .len(),
        1
    );

    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "workflow-create-conflict".into(),
            session_id: "session-host-test-123456".into(),
            command: "workflow.create".into(),
            payload: json!({
                "projectId": "project-host-1",
                "workflowId": "workflow-host-1",
                "name": "Conflicting workflow",
                "kind": "sequential",
                "steps": [{"id":"step-1", "order":1, "agentId":"agent-host-1"}]
            }),
            deadline_ms: None,
        })
        .unwrap();
    assert_error_envelope(&mut client, "workflow-create-conflict", "COMMAND_REJECTED");

    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "workflow-create-invalid-agent".into(),
            session_id: "session-host-test-123456".into(),
            command: "workflow.create".into(),
            payload: json!({
                "projectId": "project-host-1",
                "workflowId": "workflow-host-invalid",
                "name": "Invalid workflow",
                "kind": "parallel",
                "steps": [{"id":"step-1", "order":1, "agentId":"agent-not-rostered"}]
            }),
            deadline_ms: None,
        })
        .unwrap();
    assert_error_envelope(
        &mut client,
        "workflow-create-invalid-agent",
        "COMMAND_REJECTED",
    );

    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "collaboration-create-handoff-1".into(),
            session_id: "session-host-test-123456".into(),
            command: "collaboration.create".into(),
            payload: json!({
                "projectId": "project-host-1",
                "collaborationRunId": "collab-host-1",
                "rootAgentIds": ["agent-host-1"],
                "callCount": 0,
                "maxCalls": 8,
                "depth": 0,
                "maxDepth": 5,
                "status": "pending",
                "autoDispatchHandoffs": false
            }),
            deadline_ms: None,
        })
        .unwrap();
    let collaboration: ResponseEnvelope =
        serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(collaboration.payload["created"], true);
    assert_eq!(
        collaboration.payload["projection"]["collaborationRuns"][0]["id"],
        "collab-host-1"
    );

    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "workflow-dispatch-1".into(),
            session_id: "session-host-test-123456".into(),
            command: "workflow.dispatch".into(),
            payload: json!({
                "workflowId": "workflow-host-1",
                "collaborationRunId": "collab-host-1",
                "parentExecutionRunId": "run-host-conversation-inherited",
                "sourceMessageId": "message-host-1",
                "task": "dispatch the workflow over IPC",
                "startRuntime": true
            }),
            deadline_ms: None,
        })
        .unwrap();
    let workflow_dispatch_raw = client.read_json().unwrap();
    let workflow_dispatch_value: serde_json::Value =
        serde_json::from_slice(&workflow_dispatch_raw).unwrap();
    assert_eq!(
        workflow_dispatch_value["ok"], true,
        "workflow.dispatch response: {workflow_dispatch_value}"
    );
    let workflow_dispatch: ResponseEnvelope =
        serde_json::from_value(workflow_dispatch_value).unwrap();
    assert_eq!(workflow_dispatch.payload["workflowId"], "workflow-host-1");
    assert_eq!(workflow_dispatch.payload["mode"], "sequential");
    assert_eq!(workflow_dispatch.payload["completed"], true);
    assert_eq!(workflow_dispatch.payload["failed"], false);
    assert_eq!(
        workflow_dispatch.payload["steps"].as_array().unwrap().len(),
        1
    );
    assert_eq!(
        workflow_dispatch.payload["steps"][0]["runtimeStarted"],
        true
    );
    assert_eq!(
        workflow_dispatch.payload["steps"][0]["childStatus"],
        "completed"
    );
    assert!(workflow_dispatch.payload["projection"]["handoffs"]
        .as_array()
        .unwrap()
        .iter()
        .any(|handoff| {
            handoff["id"] == "workflow-handoff-workflow-host-1-step-1"
                && handoff["status"] == "completed"
                && handoff["details"]["batchId"] == "workflow-batch-workflow-host-1-collab-host-1"
        }));

    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "handoff-create-1".into(),
            session_id: "session-host-test-123456".into(),
            command: "handoff.create".into(),
            payload: json!({
                "handoffId": "handoff-host-1",
                "collaborationRunId": "collab-host-1",
                "fromExecutionRunId": "run-host-conversation-inherited",
                "toAgentId": "agent-host-1",
                "status": "proposed",
                "details": {
                    "parentExecutionRunId": "run-host-conversation-inherited",
                    "sourceMessageId": "message-host-1",
                    "fromAgentId": "agent-host-2",
                    "toAgentId": "agent-host-1",
                    "task": "dispatch child task",
                    "reason": "structured handoff test",
                    "kind": "task",
                    "dispatchMode": "sequential",
                    "detectedBy": "ui_explicit",
                    "contextScope": "conversation"
                }
            }),
            deadline_ms: None,
        })
        .unwrap();
    let handoff: ResponseEnvelope = serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(handoff.payload["created"], true);
    assert_eq!(
        handoff.payload["projection"]["handoffs"][0]["status"],
        "proposed"
    );

    for (request_id, command, status) in [
        ("handoff-approve-1", "handoff.approve", "approved"),
        ("handoff-dispatch-1", "handoff.dispatch", "dispatched"),
        ("handoff-cancel-1", "handoff.cancel", "cancelled"),
    ] {
        client
            .write_json(&CommandEnvelope {
                kind: "command".into(),
                protocol: ProtocolVersion { major: 1, minor: 0 },
                request_id: request_id.into(),
                session_id: "session-host-test-123456".into(),
                command: command.into(),
                payload: json!({"handoffId": "handoff-host-1"}),
                deadline_ms: None,
            })
            .unwrap();
        let response: ResponseEnvelope =
            serde_json::from_slice(&client.read_json().unwrap()).unwrap();
        assert_eq!(response.payload["status"], status);
        assert_eq!(response.payload["changed"], true);
        assert_eq!(
            response.payload["projection"]["handoffs"][0]["status"],
            status
        );
        if command == "handoff.dispatch" {
            assert_eq!(response.payload["runtimeStarted"], false);
            assert_eq!(response.payload["runtimeDispatch"], "deferred");
            assert_eq!(
                response.payload["childExecutionRunId"],
                "handoff-child-handoff-host-1"
            );
        }
    }

    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "handoff-cancel-replay-1".into(),
            session_id: "session-host-test-123456".into(),
            command: "handoff.cancel".into(),
            payload: json!({"handoffId": "handoff-host-1"}),
            deadline_ms: None,
        })
        .unwrap();
    let handoff_replay: ResponseEnvelope =
        serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(handoff_replay.payload["alreadyAtTarget"], true);

    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "handoff-invalid-late-approve-1".into(),
            session_id: "session-host-test-123456".into(),
            command: "handoff.approve".into(),
            payload: json!({"handoffId": "handoff-host-1"}),
            deadline_ms: None,
        })
        .unwrap();
    assert_error_envelope(
        &mut client,
        "handoff-invalid-late-approve-1",
        "COMMAND_REJECTED",
    );

    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "handoff-runtime-create-1".into(),
            session_id: "session-host-test-123456".into(),
            command: "handoff.create".into(),
            payload: json!({
                "handoffId": "handoff-host-runtime-1",
                "collaborationRunId": "collab-host-1",
                "fromExecutionRunId": "run-host-conversation-inherited",
                "toAgentId": "agent-host-1",
                "status": "proposed",
                "details": {
                    "parentExecutionRunId": "run-host-conversation-inherited",
                    "sourceMessageId": "message-host-1",
                    "fromAgentId": "agent-host-2",
                    "toAgentId": "agent-host-1",
                    "task": "run the child Runtime task",
                    "reason": "explicit runtime integration test",
                    "kind": "task",
                    "dispatchMode": "sequential",
                    "detectedBy": "ui_explicit",
                    "contextScope": "conversation"
                }
            }),
            deadline_ms: None,
        })
        .unwrap();
    let runtime_handoff: ResponseEnvelope =
        serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(runtime_handoff.payload["created"], true);

    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "handoff-runtime-approve-1".into(),
            session_id: "session-host-test-123456".into(),
            command: "handoff.approve".into(),
            payload: json!({"handoffId": "handoff-host-runtime-1"}),
            deadline_ms: None,
        })
        .unwrap();
    let runtime_approved: ResponseEnvelope =
        serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(runtime_approved.payload["status"], "approved");

    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "handoff-runtime-dispatch-1".into(),
            session_id: "session-host-test-123456".into(),
            command: "handoff.dispatch".into(),
            payload: json!({
                "handoffId": "handoff-host-runtime-1",
                "startRuntime": true
            }),
            deadline_ms: None,
        })
        .unwrap();
    let runtime_dispatch: ResponseEnvelope =
        serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(runtime_dispatch.payload["status"], "completed");
    assert_eq!(runtime_dispatch.payload["changed"], true);
    assert_eq!(runtime_dispatch.payload["runtimeStarted"], true);
    assert_eq!(runtime_dispatch.payload["runtimeDispatch"], "completed");
    assert_eq!(runtime_dispatch.payload["childRun"]["status"], "Completed");
    assert!(runtime_dispatch.payload["projection"]["contextManifests"]
        .as_array()
        .unwrap()
        .iter()
        .any(|manifest| { manifest["executionRunId"] == "handoff-child-handoff-host-runtime-1" }));

    client
        .write_json(&QueryEnvelope {
            kind: "query".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "messages-search-1".into(),
            session_id: "session-host-test-123456".into(),
            query: "messages.search".into(),
            payload: json!({
                "query": "hello",
                "conversationId": "conversation-host-1",
                "limit": 10
            }),
        })
        .unwrap();
    let search: ResponseEnvelope = serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(search.payload["messages"].as_array().unwrap().len(), 1);
    assert_eq!(search.payload["messages"][0]["content"], "hello");

    client
        .write_json(&QueryEnvelope {
            kind: "query".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "runtime-models-1".into(),
            session_id: "session-host-test-123456".into(),
            query: "runtime.models".into(),
            payload: json!({}),
        })
        .unwrap();
    let models: ResponseEnvelope = serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(models.payload["schemaVersion"], "runtime.models.v1");
    assert_eq!(models.payload["connectorId"], "mock");
    assert_eq!(models.payload["runtimeId"], "mock");
    assert_eq!(models.payload["models"], json!(["mock-default"]));
    assert_eq!(
        models.payload["modelMetadata"][0]["availability"],
        "available"
    );

    client
        .write_json(&QueryEnvelope {
            kind: "query".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "runtime-models-invalid-1".into(),
            session_id: "session-host-test-123456".into(),
            query: "runtime.models".into(),
            payload: json!({"unexpected": true}),
        })
        .unwrap();
    assert_error_envelope(&mut client, "runtime-models-invalid-1", "INVALID_QUERY");

    client
        .write_json(&QueryEnvelope {
            kind: "query".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "messages-search-invalid-1".into(),
            session_id: "session-host-test-123456".into(),
            query: "messages.search".into(),
            payload: json!({"query": "hello", "limit": 0}),
        })
        .unwrap();
    assert_error_envelope(&mut client, "messages-search-invalid-1", "INVALID_QUERY");

    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "start-unassigned".into(),
            session_id: "session-host-test-123456".into(),
            command: "execution.start".into(),
            payload: json!({
                "executionRunId": "run-host-unassigned",
                "collaborationRunId": "collab-host-1",
                "projectId": "project-host-1",
                "conversationId": "conversation-host-1",
                "agentId": "agent-not-assigned",
                "workspaceAccess": "none"
            }),
            deadline_ms: None,
        })
        .unwrap();
    let rejected_start: ErrorEnvelope =
        serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(rejected_start.code, "COMMAND_REJECTED");

    let mut subscription_client = None;
    for _ in 0..100 {
        match NamedPipeClient::connect(&pipe) {
            Ok(value) => {
                subscription_client = Some(value);
                break;
            }
            Err(_) => thread::sleep(Duration::from_millis(25)),
        }
    }
    let mut subscription_client = subscription_client.expect("subscription pipe unavailable");
    subscription_client
        .write_json(&ProtocolHandshake {
            kind: "handshake".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            client_id: "flutter-subscription-test".into(),
            session_id: "session-subscription-test-123456".into(),
            session_credential: credential,
            max_message_bytes: 1024 * 1024,
            last_seen: None,
        })
        .unwrap();
    let subscription_handshake: ResponseEnvelope =
        serde_json::from_slice(&subscription_client.read_json().unwrap()).unwrap();
    let epoch = subscription_handshake.payload["serverEpoch"]
        .as_str()
        .unwrap()
        .to_owned();
    subscription_client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "subscription-start-1".into(),
            session_id: "session-subscription-test-123456".into(),
            command: "events.subscribe".into(),
            payload: json!({
                "afterCursor": {"streamId":"core-events", "sequence":0, "epoch":epoch},
                "maxInFlightEvents": 1,
                "maxInFlightBytes": 262144
            }),
            deadline_ms: None,
        })
        .unwrap();
    let subscription: ResponseEnvelope =
        serde_json::from_slice(&subscription_client.read_json().unwrap()).unwrap();
    let subscription_id = subscription.payload["subscriptionId"]
        .as_str()
        .unwrap()
        .to_owned();
    let first_event: EventEnvelope =
        serde_json::from_slice(&subscription_client.read_json().unwrap()).unwrap();
    assert_eq!(
        first_event.subscription_id.as_deref(),
        Some(subscription_id.as_str())
    );
    subscription_client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "subscription-ack-1".into(),
            session_id: "session-subscription-test-123456".into(),
            command: "events.ack".into(),
            payload: json!({
                "subscriptionId": subscription_id,
                "cursor": first_event.cursor,
            }),
            deadline_ms: None,
        })
        .unwrap();
    let ack: ResponseEnvelope =
        serde_json::from_slice(&subscription_client.read_json().unwrap()).unwrap();
    assert_eq!(ack.payload["acknowledged"], true);
    let _second_event: EventEnvelope =
        serde_json::from_slice(&subscription_client.read_json().unwrap()).unwrap();
    subscription_client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "subscription-stop-1".into(),
            session_id: "session-subscription-test-123456".into(),
            command: "events.unsubscribe".into(),
            payload: json!({"subscriptionId": subscription_id}),
            deadline_ms: None,
        })
        .unwrap();
    let unsubscribed: ResponseEnvelope =
        serde_json::from_slice(&subscription_client.read_json().unwrap()).unwrap();
    assert_eq!(unsubscribed.payload["unsubscribed"], true);
    drop(subscription_client);

    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "shutdown-1".into(),
            session_id: "session-host-test-123456".into(),
            command: "shutdown_owned".into(),
            payload: json!({}),
            deadline_ms: None,
        })
        .unwrap();
    let shutdown: ResponseEnvelope = serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(shutdown.payload["shutdownAccepted"], true);

    drop(client);
    let _ = std::fs::remove_file(&database);
    let _ = std::fs::remove_file(database.with_extension("db-wal"));
    let _ = std::fs::remove_file(database.with_extension("db-shm"));
    let _ = std::fs::remove_file(selected_file);
    let _ = std::fs::remove_dir_all(artifact_root);
}

#[test]
fn core_host_creates_orchestration_run_from_sealed_snapshot_and_replays() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after Unix epoch")
        .as_nanos();
    let pipe = format!(
        "\\\\.\\pipe\\agenttalk-orchestration-create-test-{}-{}",
        std::process::id(),
        nonce
    );
    let database = std::env::temp_dir().join(format!(
        "agenttalk-orchestration-create-test-{}-{}.db",
        std::process::id(),
        nonce
    ));
    let artifact_root = std::env::temp_dir().join(format!(
        "agenttalk-orchestration-create-artifacts-{}-{}",
        std::process::id(),
        nonce
    ));
    let project_root = std::env::temp_dir().join(format!(
        "agenttalk-orchestration-create-project-{}-{}",
        std::process::id(),
        nonce
    ));
    std::fs::create_dir_all(project_root.join("plan")).unwrap();
    let roadmap = b"# sealed orchestration brief\n";
    let manifest = json!({
        "schemaVersion": "agenttalk.brief.manifest.v1",
        "projectId": "orchestration-create-project",
        "title": "Orchestration Create",
        "roles": [{"roleId": "owner", "displayName": "Owner"}],
        "files": [{
            "path": "plan/roadmap.md",
            "kind": "plan",
            "format": "markdown",
            "contentSchemaRef": null,
            "required": true,
            "sha256": agenttalk_brief_sealer::cas::sha256_hex(roadmap),
            "size": roadmap.len(),
            "context": {"layer": "shared", "roleIds": ["owner"], "retention": "run", "workspaceAccess": "read_only"},
            "declaredOwnerRoleId": "owner"
        }]
    });
    std::fs::write(
        project_root.join("agenttalk-brief.json"),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
    std::fs::write(project_root.join("plan/roadmap.md"), roadmap).unwrap();
    let seal = BriefSealer::new(&project_root)
        .seal(&InMemorySchemaRegistry::new())
        .unwrap();
    let project_root_string = project_root.to_string_lossy().into_owned();
    {
        let mut storage = SqliteStore::open(&database).unwrap();
        storage
            .create_project(
                "orchestration-create-project",
                "Orchestration Create",
                Some(&project_root_string),
            )
            .unwrap();
    }
    let credential = format!("test-credential-{}", "x".repeat(40));
    let executable = env!("CARGO_BIN_EXE_agenttalk-core");
    let child = Command::new(executable)
        .args([
            pipe.clone(),
            database.to_string_lossy().into_owned(),
            artifact_root.to_string_lossy().into_owned(),
        ])
        .env("AGENTTALK_CORE_SESSION_CREDENTIAL", &credential)
        .env("AGENTTALK_CORE_RUNTIME", "mock")
        .env("AGENTTALK_CORE_DEV_MODE", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let _child = ChildGuard(child);
    let mut client = connect_authenticated(
        &pipe,
        &credential,
        "orchestration-create-test-client",
        "orchestration-create-test-session",
    );
    let payload = json!({
        "projectId": "orchestration-create-project",
        "runId": "orchestration-create-run",
        "briefSnapshotId": seal.brief_snapshot_id(),
        "briefTreeDigest": seal.brief_tree_digest(),
        "dagSnapshotDigest": "1".repeat(64),
        "roleBindingSnapshotDigest": "2".repeat(64)
    });
    let created = send_command(
        &mut client,
        "orchestration-run-create-1",
        "orchestration-create-test-session",
        "orchestration.run.create",
        payload.clone(),
    );
    assert!(created.ok);
    assert_eq!(created.payload["created"], true);
    assert_eq!(created.payload["run"]["runId"], "orchestration-create-run");
    assert_eq!(
        created.payload["run"]["briefSnapshotId"],
        seal.brief_snapshot_id()
    );
    assert_eq!(
        created.payload["projection"]["run"]["runId"],
        "orchestration-create-run"
    );

    let replayed = send_command(
        &mut client,
        "orchestration-run-create-1",
        "orchestration-create-test-session",
        "orchestration.run.create",
        payload,
    );
    assert!(replayed.ok);
    assert_eq!(replayed.payload["run"], created.payload["run"]);
    assert_eq!(
        replayed.payload["projection"],
        created.payload["projection"]
    );

    let snapshot = send_query(
        &mut client,
        "orchestration-run-snapshot-1",
        "orchestration-create-test-session",
        "orchestration.run.snapshot",
        json!({"runId": "orchestration-create-run"}),
    );
    assert!(snapshot.ok);
    assert_eq!(
        snapshot.payload["run"]["briefTreeDigest"],
        seal.brief_tree_digest()
    );
    assert!(snapshot.payload["machineAcceptances"].is_array());

    let recovery = send_query(
        &mut client,
        "orchestration-run-recovery-1",
        "orchestration-create-test-session",
        "orchestration.run.recovery_state",
        json!({"runId": "orchestration-create-run"}),
    );
    assert!(recovery.ok);
    assert_eq!(recovery.payload["runId"], "orchestration-create-run");
    assert_eq!(recovery.payload["coordinatorGeneration"], 1);

    let inserted = send_command(
        &mut client,
        "orchestration-task-insert-1",
        "orchestration-create-test-session",
        "orchestration.task.insert",
        json!({
            "runId": "orchestration-create-run",
            "nodeId": "orchestration-node-1",
            "nodeKey": "architect"
        }),
    );
    assert!(inserted.ok);
    assert_eq!(inserted.payload["status"], "pending");

    let graph = send_command(
        &mut client,
        "orchestration-graph-bind-1",
        "orchestration-create-test-session",
        "orchestration.graph.bind",
        json!({
            "runId": "orchestration-create-run",
            "edges": [{
                "edgeId": "orchestration-edge-1",
                "runId": "orchestration-create-run",
                "fromNodeId": "orchestration-node-1",
                "toNodeId": "orchestration-node-1",
                "dagSnapshotDigest": "1".repeat(64),
                "allowedConsumerJson": "[]"
            }],
            "edgePorts": [{
                "edgePortId": "orchestration-edge-port-1",
                "edgeId": "orchestration-edge-1",
                "sourceOutputPortId": "out",
                "targetInputPortId": "in",
                "portPolicyJson": "{}"
            }],
            "roleBindings": [{
                "roleBindingSnapshotId": "orchestration-role-binding-1",
                "runId": "orchestration-create-run",
                "digest": "2".repeat(64),
                "roleId": "architect",
                "agentId": "agent-fixture",
                "workspaceAccess": "read_only"
            }],
            "contextAuthorities": []
        }),
    );
    assert!(graph.ok);
    assert_eq!(graph.payload["edges"], 1);
    assert_eq!(graph.payload["edgePorts"], 1);

    let ready = send_command(
        &mut client,
        "orchestration-task-ready-1",
        "orchestration-create-test-session",
        "orchestration.task.ready",
        json!({
            "nodeId": "orchestration-node-1",
            "inputArtifactSetDigest": "3".repeat(64),
            "roleId": "architect",
            "acceptanceContractRef": format!("sha256:{}", "4".repeat(64))
        }),
    );
    assert!(ready.ok);
    assert_eq!(ready.payload["status"], "ready");

    let started = send_command(
        &mut client,
        "orchestration-task-start-1",
        "orchestration-create-test-session",
        "orchestration.task.start",
        json!({
            "nodeId": "orchestration-node-1",
            "fromExecutionRunId": "execution-1",
            "leaseOwner": "core-instance-1"
        }),
    );
    assert!(started.ok);
    assert_eq!(
        started.payload["outcome"]["attemptId"],
        "orchestration-node-1:attempt:1"
    );
    assert_eq!(started.payload["outcome"]["leaseEpoch"], 1);

    let context_graph = send_command(
        &mut client,
        "orchestration-context-bind-1",
        "orchestration-create-test-session",
        "orchestration.graph.bind",
        json!({
            "runId": "orchestration-create-run",
            "edges": [],
            "edgePorts": [],
            "roleBindings": [],
            "contextAuthorities": [{
                "contextManifestRefId": "orchestration-context-1",
                "runId": "orchestration-create-run",
                "attemptId": "orchestration-node-1:attempt:1",
                "producerContextManifestDigest": "3".repeat(64)
            }]
        }),
    );
    assert!(context_graph.ok);
    assert_eq!(context_graph.payload["contextAuthorities"], 1);

    let running_snapshot = send_query(
        &mut client,
        "orchestration-run-snapshot-running",
        "orchestration-create-test-session",
        "orchestration.run.snapshot",
        json!({"runId": "orchestration-create-run"}),
    );
    assert_eq!(running_snapshot.payload["nodes"][0]["status"], "running");
    assert_eq!(
        running_snapshot.payload["attempts"][0]["fromExecutionRunId"],
        "execution-1"
    );
    let sealed = send_command(
        &mut client,
        "orchestration-task-seal-1",
        "orchestration-create-test-session",
        "orchestration.task.seal",
        json!({"nodeId": "orchestration-node-1"}),
    );
    assert!(sealed.ok);
    assert_eq!(sealed.payload["status"], "sealing");

    let approval_run = send_command(
        &mut client,
        "orchestration-approval-run-create",
        "orchestration-create-test-session",
        "orchestration.run.create",
        json!({
            "projectId": "orchestration-create-project",
            "runId": "orchestration-approval-run",
            "briefSnapshotId": seal.brief_snapshot_id(),
            "briefTreeDigest": seal.brief_tree_digest(),
            "dagSnapshotDigest": "5".repeat(64),
            "roleBindingSnapshotDigest": "6".repeat(64)
        }),
    );
    assert!(approval_run.ok);
    let milestone = send_command(
        &mut client,
        "orchestration-milestone-ensure-1",
        "orchestration-create-test-session",
        "orchestration.milestone.ensure",
        json!({
            "runId": "orchestration-approval-run",
            "milestoneId": "orchestration-milestone-1",
            "milestoneKey": "review",
            "briefTreeDigest": seal.brief_tree_digest(),
            "presentedArtifactSetDigest": "7".repeat(64),
            "acceptanceEvidenceDigest": "8".repeat(64)
        }),
    );
    assert!(milestone.ok);
    assert_eq!(milestone.payload["status"], "awaiting_approval");
    let receipt = send_command(
        &mut client,
        "orchestration-receipt-record-1",
        "orchestration-create-test-session",
        "orchestration.receipt.record",
        json!({
            "receiptId": "orchestration-receipt-1",
            "runId": "orchestration-approval-run",
            "milestoneId": "orchestration-milestone-1",
            "requestId": "human-request-1",
            "semanticPayloadHash": "9".repeat(64),
            "decision": "approve",
            "expectedVersion": 1,
            "briefTreeDigest": seal.brief_tree_digest(),
            "presentedArtifactSetDigest": "7".repeat(64),
            "acceptanceEvidenceDigest": "8".repeat(64)
        }),
    );
    assert!(receipt.ok);
    assert_eq!(receipt.payload["decision"], "approve");
    let approval_snapshot = send_query(
        &mut client,
        "orchestration-approval-snapshot-1",
        "orchestration-create-test-session",
        "orchestration.run.snapshot",
        json!({"runId": "orchestration-approval-run"}),
    );
    assert_eq!(approval_snapshot.payload["run"]["status"], "completed");

    let wrong_digest = json!({
        "projectId": "orchestration-create-project",
        "runId": "orchestration-create-run-wrong",
        "briefSnapshotId": seal.brief_snapshot_id(),
        "briefTreeDigest": "f".repeat(64),
        "dagSnapshotDigest": "1".repeat(64),
        "roleBindingSnapshotDigest": "2".repeat(64)
    });
    let wrong = send_command_error(
        &mut client,
        "orchestration-run-create-wrong-digest",
        "orchestration-create-test-session",
        "orchestration.run.create",
        wrong_digest,
    );
    assert_eq!(wrong.code, "COMMAND_REJECTED");

    let missing = send_command_error(
        &mut client,
        "orchestration-run-create-missing",
        "orchestration-create-test-session",
        "orchestration.run.create",
        json!({"projectId": "orchestration-create-project"}),
    );
    assert_eq!(missing.code, "INVALID_COMMAND");

    let unknown_snapshot = send_command_error(
        &mut client,
        "orchestration-run-create-unknown",
        "orchestration-create-test-session",
        "orchestration.run.create",
        json!({
            "projectId": "orchestration-create-project",
            "runId": "orchestration-create-run-unknown",
            "briefSnapshotId": format!("sha256:{}", "a".repeat(64)),
            "briefTreeDigest": seal.brief_tree_digest(),
            "dagSnapshotDigest": "1".repeat(64),
            "roleBindingSnapshotDigest": "2".repeat(64)
        }),
    );
    assert_eq!(unknown_snapshot.code, "COMMAND_REJECTED");

    drop(client);
    drop(_child);
    let _ = std::fs::remove_file(&database);
    let _ = std::fs::remove_file(database.with_extension("db-wal"));
    let _ = std::fs::remove_file(database.with_extension("db-shm"));
    let _ = std::fs::remove_dir_all(artifact_root);
    let _ = std::fs::remove_dir_all(project_root);
}

#[test]
fn attachment_import_receipt_replays_after_core_restart_without_source_path() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after Unix epoch")
        .as_nanos();
    let pipe_before = format!(
        "\\\\.\\pipe\\agenttalk-core-attachment-restart-before-{}-{}",
        std::process::id(),
        nonce
    );
    let pipe_after = format!(
        "\\\\.\\pipe\\agenttalk-core-attachment-restart-after-{}-{}",
        std::process::id(),
        nonce
    );
    let database = std::env::temp_dir().join(format!(
        "agenttalk-core-attachment-restart-{}-{}.db",
        std::process::id(),
        nonce
    ));
    let artifact_root = std::env::temp_dir().join(format!(
        "agenttalk-core-attachment-restart-artifacts-{}-{}",
        std::process::id(),
        nonce
    ));
    let selected_file = std::env::temp_dir().join(format!(
        "agenttalk-core-attachment-restart-selected-{}-{}.bin",
        std::process::id(),
        nonce
    ));
    let _ = std::fs::remove_file(&database);
    let _ = std::fs::remove_file(database.with_extension("db-wal"));
    let _ = std::fs::remove_file(database.with_extension("db-shm"));
    let _ = std::fs::remove_dir_all(&artifact_root);
    let _ = std::fs::remove_file(&selected_file);

    let credential = format!("attachment-restart-credential-{}", "x".repeat(40));
    let client_id = "flutter-attachment-restart";
    let session_id = "session-attachment-restart-123456";
    let executable = env!("CARGO_BIN_EXE_agenttalk-core");
    let first_child = Command::new(executable)
        .args([
            pipe_before.clone(),
            database.to_string_lossy().into_owned(),
            artifact_root.to_string_lossy().into_owned(),
        ])
        .env("AGENTTALK_CORE_SESSION_CREDENTIAL", &credential)
        .env("AGENTTALK_CORE_RUNTIME", "mock")
        .env("AGENTTALK_CORE_DEV_MODE", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let first_guard = ChildGuard(first_child);
    let mut first_client = connect_authenticated(&pipe_before, &credential, client_id, session_id);

    assert!(
        send_command(
            &mut first_client,
            "attachment-restart-project-create",
            session_id,
            "project.create",
            json!({"projectId":"project-attachment-restart", "name":"Attachment Restart"}),
        )
        .ok
    );
    assert!(
        send_command(
            &mut first_client,
            "attachment-restart-conversation-create",
            session_id,
            "conversation.create",
            json!({
                "conversationId":"conversation-attachment-restart",
                "projectId":"project-attachment-restart",
                "title":"Attachment Restart"
            }),
        )
        .ok
    );
    assert!(
        send_command(
            &mut first_client,
            "attachment-restart-message-create",
            session_id,
            "message.create",
            json!({
                "messageId":"message-attachment-restart",
                "conversationId":"conversation-attachment-restart",
                "senderId":"user",
                "sequence":1,
                "content":"attachment recovery fixture"
            }),
        )
        .ok
    );

    std::fs::write(&selected_file, vec![0x5a; 600 * 1024]).unwrap();
    let import_payload = json!({
        "attachmentId": "attachment-restart-1",
        "artifactId": "artifact-restart-1",
        "messageId": "message-attachment-restart",
        "sourcePath": selected_file.to_string_lossy(),
        "mime": "application/octet-stream",
        "ordinal": 0
    });
    let imported = send_command(
        &mut first_client,
        "attachment-import-restart-1",
        session_id,
        "attachment.import_file",
        import_payload.clone(),
    );
    assert!(imported.ok);
    assert_eq!(imported.payload["created"], true);
    assert_eq!(imported.payload["bodyStored"], true);
    assert!(!serde_json::to_string(&imported.payload)
        .unwrap()
        .contains(&selected_file.to_string_lossy().to_string()));
    drop(first_client);
    std::fs::remove_file(&selected_file).unwrap();
    drop(first_guard);

    let second_child = Command::new(executable)
        .args([
            pipe_after.clone(),
            database.to_string_lossy().into_owned(),
            artifact_root.to_string_lossy().into_owned(),
        ])
        .env("AGENTTALK_CORE_SESSION_CREDENTIAL", &credential)
        .env("AGENTTALK_CORE_RUNTIME", "mock")
        .env("AGENTTALK_CORE_DEV_MODE", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let second_guard = ChildGuard(second_child);
    let mut restarted_client =
        connect_authenticated(&pipe_after, &credential, client_id, session_id);

    let replayed = send_command(
        &mut restarted_client,
        "attachment-import-restart-1",
        session_id,
        "attachment.import_file",
        import_payload.clone(),
    );
    assert!(replayed.ok);
    assert_eq!(replayed.payload, imported.payload);
    let replayed_serialized = serde_json::to_string(&replayed.payload).unwrap();
    assert!(!replayed_serialized.contains(&selected_file.to_string_lossy().to_string()));
    assert!(!replayed_serialized.contains(&"Z".repeat(256)));
    assert_eq!(
        replayed.payload["projection"]["attachments"][0]["attachmentId"],
        "attachment-restart-1"
    );

    let mut changed_payload = import_payload;
    changed_payload["mime"] = json!("text/plain");
    restarted_client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "attachment-import-restart-1".into(),
            session_id: session_id.into(),
            command: "attachment.import_file".into(),
            payload: changed_payload,
            deadline_ms: None,
        })
        .unwrap();
    assert_error_envelope(
        &mut restarted_client,
        "attachment-import-restart-1",
        "REQUEST_ID_REUSE",
    );

    drop(restarted_client);
    drop(second_guard);
    let _ = std::fs::remove_file(&database);
    let _ = std::fs::remove_file(database.with_extension("db-wal"));
    let _ = std::fs::remove_file(database.with_extension("db-shm"));
    let _ = std::fs::remove_dir_all(artifact_root);
}
