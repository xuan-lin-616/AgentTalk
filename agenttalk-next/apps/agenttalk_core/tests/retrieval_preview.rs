#![cfg(windows)]

use agenttalk_ipc::{FramedTransport, NamedPipeClient};
use agenttalk_protocols::{
    CommandEnvelope, ErrorEnvelope, ProtocolHandshake, ProtocolVersion, QueryEnvelope,
    ResponseEnvelope, PROTOCOL_MAJOR,
};
use serde_json::{json, Value};
use std::fs;
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

struct ChildGuard(std::process::Child);

impl ChildGuard {
    /// Explicitly terminates the owned Core child and waits for it to exit.
    /// This is the success-path teardown: the child must be fully reaped
    /// before the nonce-owned SQLite fixture files can be removed on Windows.
    /// A forcibly terminated process reports a non-success exit code by
    /// design, so only the fact of exit matters here.
    fn stop(&mut self) {
        let _ = self.0.kill();
        self.0
            .wait()
            .expect("owned Core child must exit after termination");
    }

    /// Whether the owned child has already exited (used to make the Drop
    /// fallback idempotent after an explicit `stop`).
    fn exited(&mut self) -> bool {
        self.0.try_wait().ok().flatten().is_some()
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        // Panic/unwind fallback: if the test aborts mid-way, the owned child
        // must still be reaped. The guard is idempotent after an explicit
        // `stop()` on the success path.
        if !self.exited() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

fn command(
    client: &mut agenttalk_ipc::NamedPipeConnection,
    request_id: &str,
    command: &str,
    payload: Value,
) -> ResponseEnvelope {
    client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion {
                major: PROTOCOL_MAJOR,
                minor: 0,
            },
            request_id: request_id.into(),
            session_id: "session-retrieval-preview-123".into(),
            command: command.into(),
            payload,
            deadline_ms: None,
        })
        .unwrap();
    serde_json::from_slice(&client.read_json().unwrap()).unwrap()
}

fn assert_query_error(
    client: &mut agenttalk_ipc::NamedPipeConnection,
    request_id: &str,
    payload: Value,
    code: &str,
) {
    client
        .write_json(&QueryEnvelope {
            kind: "query".into(),
            protocol: ProtocolVersion {
                major: PROTOCOL_MAJOR,
                minor: 0,
            },
            request_id: request_id.into(),
            session_id: "session-retrieval-preview-123".into(),
            query: "retrieval.preview".into(),
            payload,
        })
        .unwrap();
    let error: ErrorEnvelope = serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert_eq!(error.code, code);
    assert!(!error.message.is_empty());
}

#[test]
fn retrieval_preview_named_pipe_host_is_scoped_exact_and_file_scan_is_not_faked() {
    let run_nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let pipe = format!(
        "\\\\.\\pipe\\agenttalk-retrieval-preview-{}-{run_nonce}",
        std::process::id()
    );
    let database = std::env::temp_dir().join(format!(
        "agenttalk-retrieval-preview-{}-{run_nonce}.db",
        std::process::id()
    ));
    let workspace_root = std::env::temp_dir().join(format!(
        "agenttalk-retrieval-preview-workspace-{}-{run_nonce}",
        std::process::id()
    ));
    fs::create_dir_all(workspace_root.join(".git")).unwrap();
    fs::write(
        workspace_root.join("notes.txt"),
        "bounded retrieval phrase over Named Pipe",
    )
    .unwrap();
    fs::write(workspace_root.join(".env"), "retrieval phrase SECRET_TOKEN").unwrap();
    fs::write(
        workspace_root.join(".git").join("ignored.txt"),
        "retrieval phrase",
    )
    .unwrap();
    let credential = format!("retrieval-preview-credential-{}", "x".repeat(32));
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
    let mut child_guard = ChildGuard(child);

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
    let mut client = client.expect("Core host did not create its Named Pipe");
    client
        .write_json(&ProtocolHandshake {
            kind: "handshake".into(),
            protocol: ProtocolVersion {
                major: PROTOCOL_MAJOR,
                minor: 0,
            },
            client_id: "retrieval-preview-test".into(),
            session_id: "session-retrieval-preview-123".into(),
            session_credential: credential,
            max_message_bytes: 1024 * 1024,
            last_seen: None,
        })
        .unwrap();
    let handshake: ResponseEnvelope = serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert!(handshake.ok);

    assert!(
        command(
            &mut client,
            "project-create",
            "project.create",
            json!({"projectId":"preview-project","name":"Preview Project","rootPath":null})
        )
        .ok
    );
    assert!(
        command(
            &mut client,
            "agent-create",
            "agent.create",
            json!({
                "agentId":"preview-agent",
                "name":"Preview Agent",
                "role":"reviewer",
                "specialty":"retrieval",
                "systemPrompt":"secret-like prompt must not be stored in retrieval results"
            })
        )
        .ok
    );
    assert!(command(
        &mut client,
        "agent-assign",
        "project_agent.set",
        json!({"projectId":"preview-project","agentId":"preview-agent","enabled":true,"workspaceAccess":"read_only"})
    )
    .ok);
    assert!(
        command(
            &mut client,
            "workspace-authorize",
            "workspace.authorize",
            json!({
                "projectId":"preview-project",
                "rootPath":workspace_root.to_string_lossy()
            })
        )
        .ok
    );
    assert!(command(
        &mut client,
        "conversation-create",
        "conversation.create",
        json!({"conversationId":"preview-conversation","projectId":"preview-project","title":"Preview"})
    )
    .ok);
    assert!(
        command(
            &mut client,
            "message-create",
            "message.create",
            json!({
                "messageId":"preview-message",
                "conversationId":"preview-conversation",
                "senderId":"preview-agent",
                "sequence":1,
                "content":"Exact retrieval phrase over Named Pipe"
            })
        )
        .ok
    );

    client
        .write_json(&QueryEnvelope {
            kind: "query".into(),
            protocol: ProtocolVersion {
                major: PROTOCOL_MAJOR,
                minor: 0,
            },
            request_id: "preview-query".into(),
            session_id: "session-retrieval-preview-123".into(),
            query: "retrieval.preview".into(),
            payload: json!({
                "expectedProjectId":"preview-project",
                "conversationId":"preview-conversation",
                "agentId":"preview-agent",
                "query":"retrieval phrase",
                "scope":"conversation",
                "sourceTypes":["message","execution","project_file"],
                "limit":10
            }),
        })
        .unwrap();
    let response: ResponseEnvelope = serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert!(response.ok);
    assert_eq!(response.payload["retrievalVersion"], "exact-retrieval-v1");
    assert_eq!(response.payload["capabilities"]["boundedFileScan"], true);
    assert!(response.payload["hits"]
        .as_array()
        .unwrap()
        .iter()
        .any(|hit| hit["sourceType"] == "message"));
    assert!(response.payload["hits"]
        .as_array()
        .unwrap()
        .iter()
        .any(|hit| {
            hit["sourceType"] == "project_file"
                && hit["sourceObjectId"] == "notes.txt"
                && hit["permissionDecision"] == "read_only"
        }));
    assert!(!serde_json::to_string(&response.payload)
        .unwrap()
        .contains("secret-like prompt"));
    assert!(!serde_json::to_string(&response.payload)
        .unwrap()
        .contains("SECRET_TOKEN"));

    client
        .write_json(&QueryEnvelope {
            kind: "query".into(),
            protocol: ProtocolVersion {
                major: PROTOCOL_MAJOR,
                minor: 0,
            },
            request_id: "preview-vector-query".into(),
            session_id: "session-retrieval-preview-123".into(),
            query: "retrieval.preview".into(),
            payload: json!({
                "expectedProjectId":"preview-project",
                "conversationId":"preview-conversation",
                "agentId":"preview-agent",
                "query":"retrieval phrase",
                "scope":"conversation",
                "sourceTypes":["message","project_file"],
                "limit":10,
                "mode":"vector_fixture"
            }),
        })
        .unwrap();
    let vector_response: ResponseEnvelope =
        serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert!(vector_response.ok);
    assert_eq!(
        vector_response.payload["retrievalVersion"],
        "local-vector-fixture-v1"
    );
    assert_eq!(vector_response.payload["capabilities"]["semantic"], true);
    assert_eq!(
        vector_response.payload["capabilities"]["semanticUnavailableReason"],
        "local_deterministic_fixture_not_live_provider"
    );
    assert_eq!(
        vector_response.payload["capabilities"]["embeddingProvider"],
        "local_fixture"
    );
    assert!(vector_response.payload["hits"]
        .as_array()
        .unwrap()
        .iter()
        .all(|hit| hit["matchMethod"] == "local_vector_fixture"));

    assert_query_error(
        &mut client,
        "preview-unknown-field",
        json!({
            "expectedProjectId":"preview-project",
            "conversationId":"preview-conversation",
            "agentId":"preview-agent",
            "query":"retrieval",
            "scope":"conversation",
            "sourceTypes":["message"],
            "limit":10,
            "prompt":"must be rejected"
        }),
        "INVALID_QUERY",
    );
    assert_query_error(
        &mut client,
        "preview-cross-project",
        json!({
            "expectedProjectId":"wrong-project",
            "conversationId":"preview-conversation",
            "agentId":"preview-agent",
            "query":"retrieval",
            "scope":"conversation",
            "sourceTypes":["message"],
            "limit":10
        }),
        "QUERY_REJECTED",
    );
    // W6.3 teardown with the correct resource order. Only the paths created
    // by this run's unique nonce are touched; historical residue and any
    // other process files are never referenced.
    let database_wal = database.with_extension("db-wal");
    let database_shm = database.with_extension("db-shm");

    // 1. Release the Named Pipe client so the Core connection handler sees
    //    the disconnect before the child is stopped.
    drop(client);

    // 2. Explicitly terminate and reap the owned Core child. This must
    //    happen before the SQLite fixture files are removed: on Windows the
    //    running Core process holds them open, and deleting while it is
    //    alive fails silently.
    child_guard.stop();
    assert!(
        child_guard.exited(),
        "owned Core child must be fully exited before fixture cleanup"
    );

    // 3. Remove every nonce-owned fixture path and verify the removal. A
    //    failure is a test failure, never a silently ignored error.
    if workspace_root.exists() {
        fs::remove_dir_all(&workspace_root).expect("remove nonce-owned retrieval workspace");
    }
    for path in [&database, &database_wal, &database_shm] {
        if path.exists() {
            fs::remove_file(path).expect("remove nonce-owned retrieval fixture file");
        }
    }
    assert!(
        !workspace_root.exists(),
        "nonce-owned workspace must be removed after the success path"
    );
    assert!(
        !database.exists(),
        "nonce-owned fixture database must be removed after the success path"
    );
    assert!(
        !database_wal.exists(),
        "nonce-owned fixture WAL must be removed after the success path"
    );
    assert!(
        !database_shm.exists(),
        "nonce-owned fixture SHM must be removed after the success path"
    );
}
