#![cfg(windows)]

use agenttalk_ipc::{FramedTransport, NamedPipeClient};
use agenttalk_protocols::{
    CommandEnvelope, ErrorEnvelope, ProtocolHandshake, ProtocolVersion, QueryEnvelope,
    ResponseEnvelope, PROTOCOL_MAJOR,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
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

fn connect(pipe: &str) -> agenttalk_ipc::NamedPipeConnection {
    for _ in 0..100 {
        if let Ok(client) = NamedPipeClient::connect(pipe) {
            return client;
        }
        thread::sleep(Duration::from_millis(25));
    }
    panic!("Core host did not create its Named Pipe");
}

fn handshake(client: &mut agenttalk_ipc::NamedPipeConnection, session_id: &str, credential: &str) {
    client
        .write_json(&ProtocolHandshake {
            kind: "handshake".into(),
            protocol: ProtocolVersion {
                major: PROTOCOL_MAJOR,
                minor: 0,
            },
            client_id: "context-manifest-ipc-test".into(),
            session_id: session_id.into(),
            session_credential: credential.into(),
            max_message_bytes: 1024 * 1024,
            last_seen: None,
        })
        .unwrap();
    let response: ResponseEnvelope = serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert!(response.ok);
}

fn command(
    client: &mut agenttalk_ipc::NamedPipeConnection,
    session_id: &str,
    request_id: &str,
    name: &str,
    payload: Value,
) -> ResponseEnvelope {
    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: request_id.into(),
            session_id: session_id.into(),
            command: name.into(),
            payload,
            deadline_ms: None,
        })
        .unwrap();
    let response: ResponseEnvelope = serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert!(response.ok, "{name} failed: {:?}", response.payload);
    response
}

fn rejected_command(
    client: &mut agenttalk_ipc::NamedPipeConnection,
    session_id: &str,
    request_id: &str,
    payload: Value,
) -> ErrorEnvelope {
    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: request_id.into(),
            session_id: session_id.into(),
            command: "execution.start".into(),
            payload,
            deadline_ms: None,
        })
        .unwrap();
    serde_json::from_slice(&client.read_json().unwrap()).unwrap()
}

fn execution_payload(run_id: Option<&str>) -> Value {
    let mut payload = json!({
        "collaborationRunId": "collaboration-context-manifest-ipc",
        "projectId": "project-context-manifest-ipc",
        "conversationId": "conversation-in-scope",
        "agentId": "agent-context-manifest-ipc",
        "workspaceAccess": "none",
        "currentTask": "context manifest IPC task"
    });
    if let Some(run_id) = run_id {
        payload["executionRunId"] = json!(run_id);
    }
    payload
}

fn sha256_hex(value: &str) -> String {
    Sha256::digest(value.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[test]
fn execution_start_exposes_run_bound_context_manifest_and_scoped_source_ledger() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let pipe = format!(
        r#"\\.\pipe\agenttalk-core-context-manifest-{}-{nonce}"#,
        std::process::id()
    );
    let database = std::env::temp_dir().join(format!(
        "agenttalk-core-context-manifest-{}-{nonce}.db",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&database);
    let _ = std::fs::remove_file(database.with_extension("db-wal"));
    let _ = std::fs::remove_file(database.with_extension("db-shm"));
    let credential = format!("test-credential-{}", "x".repeat(40));
    let executable = env!("CARGO_BIN_EXE_agenttalk-core");
    let child = Command::new(executable)
        .args([pipe.clone(), database.to_string_lossy().into_owned()])
        .env("AGENTTALK_CORE_SESSION_CREDENTIAL", &credential)
        .env("AGENTTALK_CORE_RUNTIME", "mock")
        .env("AGENTTALK_CORE_DEV_MODE", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let _child = ChildGuard(child);
    let session_id = "session-context-manifest-ipc-123456";
    let mut client = connect(&pipe);
    handshake(&mut client, session_id, &credential);

    command(
        &mut client,
        session_id,
        "context-manifest-project-create",
        "project.create",
        json!({
            "projectId": "project-context-manifest-ipc",
            "name": "Context Manifest IPC"
        }),
    );
    command(
        &mut client,
        session_id,
        "context-manifest-agent-create",
        "agent.create",
        json!({
            "agentId": "agent-context-manifest-ipc",
            "name": "Context Manifest Agent",
            "role": "builder",
            "specialty": "testing",
            "systemPrompt": "test"
        }),
    );
    for (request_id, conversation_id) in [
        (
            "context-manifest-conversation-in-create",
            "conversation-in-scope",
        ),
        (
            "context-manifest-conversation-out-create",
            "conversation-out-of-scope",
        ),
    ] {
        command(
            &mut client,
            session_id,
            request_id,
            "conversation.create",
            json!({
                "conversationId": conversation_id,
                "projectId": "project-context-manifest-ipc",
                "title": conversation_id
            }),
        );
    }
    command(
        &mut client,
        session_id,
        "context-manifest-in-message",
        "message.create",
        json!({
            "messageId": "message-in-scope",
            "conversationId": "conversation-in-scope",
            "senderId": "user",
            "sequence": 1,
            "content": "in-scope-history-token"
        }),
    );
    command(
        &mut client,
        session_id,
        "context-manifest-out-message",
        "message.create",
        json!({
            "messageId": "message-out-of-scope",
            "conversationId": "conversation-out-of-scope",
            "senderId": "user",
            "sequence": 1,
            "content": "out-of-scope-history-token"
        }),
    );
    command(
        &mut client,
        session_id,
        "context-manifest-artifact",
        "artifact.store",
        json!({
            "artifactId": "artifact-context-manifest-ipc",
            "sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "size": 12,
            "mime": "text/plain"
        }),
    );
    command(
        &mut client,
        session_id,
        "context-manifest-attachment",
        "attachment.store",
        json!({
            "attachmentId": "attachment-context-manifest-ipc",
            "artifactId": "artifact-context-manifest-ipc",
            "messageId": "message-in-scope",
            "ordinal": 0,
            "fileName": "context.txt",
            "sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "size": 12
        }),
    );
    command(
        &mut client,
        session_id,
        "context-manifest-assignment",
        "project_agent.set",
        json!({
            "projectId": "project-context-manifest-ipc",
            "agentId": "agent-context-manifest-ipc",
            "enabled": true,
            "workspaceAccess": "none"
        }),
    );

    let run_id = "run-context-manifest-ipc";
    let started = command(
        &mut client,
        session_id,
        "context-manifest-execution-start",
        "execution.start",
        execution_payload(Some(run_id)),
    );
    assert_eq!(started.payload["run"]["id"], run_id);

    let snapshot = {
        client
            .write_json(&QueryEnvelope {
                kind: "query".into(),
                protocol: ProtocolVersion { major: 1, minor: 0 },
                request_id: "context-manifest-snapshot".into(),
                session_id: session_id.into(),
                query: "projection.snapshot".into(),
                payload: json!({}),
            })
            .unwrap();
        let response: ResponseEnvelope =
            serde_json::from_slice(&client.read_json().unwrap()).unwrap();
        assert!(response.ok);
        response.payload
    };
    let run = snapshot["runs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|run| run["id"] == run_id)
        .expect("execution.start run missing from projection");
    assert_eq!(run["projectId"], "project-context-manifest-ipc");
    assert_eq!(run["conversationId"], "conversation-in-scope");
    assert_eq!(run["agentId"], "agent-context-manifest-ipc");

    let manifest = snapshot["contextManifests"]
        .as_array()
        .unwrap()
        .iter()
        .find(|manifest| manifest["executionRunId"] == run_id)
        .expect("run-bound ContextManifest missing from IPC projection");
    assert_eq!(manifest["executionRunId"], run["id"]);
    assert!(manifest["id"].as_str().unwrap().contains(run_id));
    let ledger = manifest["sourceLedger"]
        .as_array()
        .expect("sourceLedger must be present as an array");
    assert!(!ledger.is_empty());
    assert!(ledger.iter().any(|entry| {
        entry["sourceId"] == "current-task"
            && entry["kind"] == "current_task"
            && entry["included"] == true
            && entry["tokenCount"].as_u64().is_some_and(|count| count > 0)
    }));
    assert!(ledger.iter().any(|entry| {
        entry["sourceId"] == "message-0" && entry["sha256"] == sha256_hex("in-scope-history-token")
    }));
    assert!(ledger.iter().any(|entry| {
        entry["sourceId"] == "attachment-context-manifest-ipc"
            && entry["kind"] == "attachment"
            && entry["included"] == true
    }));
    assert!(!ledger
        .iter()
        .any(|entry| entry["sha256"] == sha256_hex("out-of-scope-history-token")));

    for (request_id, payload) in [
        ("context-manifest-missing-run-id", execution_payload(None)),
        ("context-manifest-empty-run-id", execution_payload(Some(""))),
    ] {
        let error = rejected_command(&mut client, session_id, request_id, payload);
        assert_eq!(error.kind, "error");
        assert_eq!(error.code, "INVALID_COMMAND");
        assert!(error.message.contains("executionRunId"));
    }

    let shutdown = command(
        &mut client,
        session_id,
        "context-manifest-shutdown",
        "shutdown_owned",
        json!({}),
    );
    assert_eq!(shutdown.payload["shutdownAccepted"], true);
    drop(client);
    let _ = std::fs::remove_file(&database);
    let _ = std::fs::remove_file(database.with_extension("db-wal"));
    let _ = std::fs::remove_file(database.with_extension("db-shm"));
}
