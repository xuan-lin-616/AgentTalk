#![cfg(windows)]

use agenttalk_ipc::{FramedTransport, NamedPipeClient, NamedPipeConnection};
use agenttalk_protocols::{CommandEnvelope, ProtocolHandshake, ProtocolVersion, ResponseEnvelope};
use serde_json::json;
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

struct ChildGuard(std::process::Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn connect(pipe: &str, database: &str, credential: &str) -> (ChildGuard, NamedPipeConnection) {
    let executable = env!("CARGO_BIN_EXE_agenttalk-core");
    let child = Command::new(executable)
        .args([pipe, database])
        .env("AGENTTALK_CORE_SESSION_CREDENTIAL", credential)
        .env("AGENTTALK_CORE_RUNTIME", "mock")
        .env("AGENTTALK_CORE_DEV_MODE", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let guard = ChildGuard(child);
    let mut client = None;
    for _ in 0..120 {
        match NamedPipeClient::connect(pipe) {
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
            protocol: ProtocolVersion { major: 1, minor: 0 },
            client_id: "config-transfer-test".into(),
            session_id: "session-config-transfer-123456".into(),
            session_credential: credential.into(),
            max_message_bytes: 1024 * 1024,
            last_seen: None,
        })
        .unwrap();
    let handshake: ResponseEnvelope = serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert!(handshake.ok);
    (guard, client)
}

fn command(request_id: &str, name: &str, payload: serde_json::Value) -> CommandEnvelope {
    CommandEnvelope {
        kind: "command".into(),
        protocol: ProtocolVersion { major: 1, minor: 0 },
        request_id: request_id.into(),
        session_id: "session-config-transfer-123456".into(),
        command: name.into(),
        payload,
        deadline_ms: None,
    }
}

fn read_response(client: &mut NamedPipeConnection) -> ResponseEnvelope {
    serde_json::from_slice(&client.read_json().unwrap()).unwrap()
}

#[test]
fn config_transfer_commands_round_trip_over_named_pipe_without_workspace_binding() {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let pipe = format!(r"\\.\pipe\agenttalk-config-transfer-{nonce}");
    let database = std::env::temp_dir()
        .join(format!("agenttalk-config-transfer-{nonce}.db"))
        .to_string_lossy()
        .into_owned();
    let credential = format!("config-transfer-credential-{}", "x".repeat(40));
    let (_guard, mut client) = connect(&pipe, &database, &credential);

    client
        .write_json(&command(
            "project-create",
            "project.create",
            json!({
                "projectId": "config-project",
                "name": "Config Project",
                "rootPath": r"C:\private\workspace"
            }),
        ))
        .unwrap();
    assert!(read_response(&mut client).payload["changed"] == true);

    for (request_id, agent_id, name) in [
        ("agent-create-a", "config-agent-a", "Builder"),
        ("agent-create-b", "config-agent-b", "Reviewer"),
    ] {
        client
            .write_json(&command(
                request_id,
                "agent.create",
                json!({
                    "agentId": agent_id,
                    "name": name,
                    "role": "fixture",
                    "specialty": "config",
                    "systemPrompt": "safe fixture prompt"
                }),
            ))
            .unwrap();
        assert!(read_response(&mut client).payload["changed"] == true);
    }
    client
        .write_json(&command(
            "assignment-a",
            "project_agent.set",
            json!({
                "projectId": "config-project",
                "agentId": "config-agent-a",
                "enabled": true,
                "workspaceAccess": "read_only"
            }),
        ))
        .unwrap();
    assert!(read_response(&mut client).payload["changed"] == true);
    client
        .write_json(&command(
            "assignment-b",
            "project_agent.set",
            json!({
                "projectId": "config-project",
                "agentId": "config-agent-b",
                "enabled": true,
                "workspaceAccess": "none"
            }),
        ))
        .unwrap();
    assert!(read_response(&mut client).payload["changed"] == true);

    client
        .write_json(&command(
            "conversation-create",
            "conversation.create",
            json!({
                "conversationId": "config-conversation",
                "projectId": "config-project",
                "title": "Config conversation"
            }),
        ))
        .unwrap();
    assert!(read_response(&mut client).payload["changed"] == true);

    client
        .write_json(&command(
            "workflow-create",
            "workflow.create",
            json!({
                "projectId": "config-project",
                "workflowId": "config-workflow",
                "name": "Build and review",
                "kind": "sequential",
                "steps": [{
                    "id": "config-step-a",
                    "order": 0,
                    "agentId": "config-agent-a",
                    "promptSupplement": "Review safely"
                }]
            }),
        ))
        .unwrap();
    let workflow_response = read_response(&mut client);
    assert!(
        workflow_response.payload["created"] == true,
        "workflow response: {:?}",
        workflow_response
    );

    client
        .write_json(&command(
            "config-export",
            "config.export",
            json!({"projectId": "config-project"}),
        ))
        .unwrap();
    let exported = read_response(&mut client);
    let config = exported.payload["config"].clone();
    assert!(config["project"]["rootPath"].is_null());
    let encoded = serde_json::to_string(&config).unwrap().to_ascii_lowercase();
    assert!(!encoded.contains("token"));
    assert!(!encoded.contains("authorization"));
    assert!(!encoded.contains(r"c:\private\workspace"));

    client
        .write_json(&command(
            "config-import",
            "config.import",
            json!({"config": config}),
        ))
        .unwrap();
    let imported = read_response(&mut client);
    assert!(imported.payload["success"] == true);
    assert_eq!(imported.payload["importedAgents"], 2);
    assert_eq!(imported.payload["importedConversations"], 1);
    assert_eq!(imported.payload["importedWorkflows"], 1);
    assert!(imported.payload["workspaceRebindRequired"] == false);
    assert_ne!(imported.payload["newProjectId"], "config-project");
}
