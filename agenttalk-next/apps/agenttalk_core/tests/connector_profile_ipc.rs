#![cfg(windows)]

use agenttalk_ipc::{FramedTransport, NamedPipeClient, NamedPipeConnection};
use agenttalk_protocols::{
    CommandEnvelope, ErrorEnvelope, ProtocolHandshake, ProtocolVersion, QueryEnvelope,
    ResponseEnvelope,
};
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
        .expect("spawn Core host");
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
            client_id: "connector-profile-test".into(),
            session_id: "session-connector-profile-123456".into(),
            session_credential: credential.into(),
            max_message_bytes: 1024 * 1024,
            last_seen: None,
        })
        .expect("write handshake");
    let handshake: ResponseEnvelope =
        serde_json::from_slice(&client.read_json().expect("read handshake"))
            .expect("decode handshake");
    assert!(handshake.ok);
    (guard, client)
}

fn command(request_id: &str, name: &str, payload: serde_json::Value) -> CommandEnvelope {
    CommandEnvelope {
        kind: "command".into(),
        protocol: ProtocolVersion { major: 1, minor: 0 },
        request_id: request_id.into(),
        session_id: "session-connector-profile-123456".into(),
        command: name.into(),
        payload,
        deadline_ms: None,
    }
}

fn query(request_id: &str, name: &str, payload: serde_json::Value) -> QueryEnvelope {
    QueryEnvelope {
        kind: "query".into(),
        protocol: ProtocolVersion { major: 1, minor: 0 },
        request_id: request_id.into(),
        session_id: "session-connector-profile-123456".into(),
        query: name.into(),
        payload,
    }
}

fn read_response(client: &mut NamedPipeConnection) -> ResponseEnvelope {
    serde_json::from_slice(&client.read_json().expect("read response")).expect("decode response")
}

fn read_error(client: &mut NamedPipeConnection) -> ErrorEnvelope {
    serde_json::from_slice(&client.read_json().expect("read error")).expect("decode error")
}

#[test]
fn connector_profile_lifecycle_is_receipted_projected_and_fail_closed_over_named_pipe() {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock before epoch")
        .as_nanos();
    let pipe = format!(r"\\.\pipe\agenttalk-connector-profile-{nonce}");
    let database = std::env::temp_dir()
        .join(format!("agenttalk-connector-profile-{nonce}.sqlite3"))
        .to_string_lossy()
        .into_owned();
    let credential = format!("connector-profile-credential-{}", "x".repeat(40));
    let (guard, mut client) = connect(&pipe, &database, &credential);
    let metadata = json!({
        "scopeId": "desktop",
        "connectorId": "fixture.connector",
        "displayName": "Fixture Connector",
        "providerType": "openai-compatible",
        "runtimeType": "local_gateway",
        "enabled": true,
        "authEnvKey": "AGENTTALK_AUTH_KEY"
    });

    client
        .write_json(&command(
            "connector-create",
            "connector.create",
            metadata.clone(),
        ))
        .expect("write create");
    let created = read_response(&mut client);
    assert!(created.ok);
    assert!(created.payload["created"] == true);
    assert_eq!(
        created.payload["projection"]["connectorProfiles"][0]["connectorId"],
        "fixture.connector"
    );
    assert_eq!(
        created.payload["connectorProfile"]["authEnvKey"],
        "AGENTTALK_AUTH_KEY"
    );

    client
        .write_json(&command(
            "connector-create",
            "connector.create",
            metadata.clone(),
        ))
        .expect("write replay");
    let replay = read_response(&mut client);
    assert_eq!(replay.payload, created.payload);

    client
        .write_json(&query(
            "connector-query",
            "connector.query",
            json!({"scopeId": "desktop"}),
        ))
        .expect("write query");
    let queried = read_response(&mut client);
    assert_eq!(
        queried.payload["connectorProfiles"]
            .as_array()
            .unwrap()
            .len(),
        1
    );

    let mut updated_metadata = metadata.clone();
    updated_metadata["displayName"] = json!("Fixture Connector Updated");
    updated_metadata["enabled"] = json!(false);
    client
        .write_json(&command(
            "connector-update",
            "connector.update",
            updated_metadata,
        ))
        .expect("write update");
    let updated = read_response(&mut client);
    assert!(updated.payload["updated"] == true);
    assert!(updated.payload["projection"]["connectorProfiles"][0]["enabled"] == false);

    let mut conflict_metadata = metadata.clone();
    conflict_metadata["displayName"] = json!("Conflicting Connector");
    client
        .write_json(&command(
            "connector-conflict",
            "connector.create",
            conflict_metadata,
        ))
        .expect("write conflict");
    let conflict = read_error(&mut client);
    assert_eq!(conflict.code, "COMMAND_REJECTED");

    let mut unknown_metadata = metadata;
    unknown_metadata["unexpected"] = json!("rejected");
    client
        .write_json(&command(
            "connector-unknown",
            "connector.create",
            unknown_metadata,
        ))
        .expect("write unknown field");
    let unknown = read_error(&mut client);
    assert_eq!(unknown.code, "INVALID_COMMAND");

    client
        .write_json(&command(
            "connector-remove",
            "connector.remove",
            json!({
                "scopeId": "desktop",
                "connectorId": "fixture.connector"
            }),
        ))
        .expect("write remove");
    let removed = read_response(&mut client);
    assert!(removed.payload["removed"] == true);

    client
        .write_json(&query(
            "connector-query-empty",
            "connector.query",
            json!({"scopeId": "desktop"}),
        ))
        .expect("write empty query");
    let empty = read_response(&mut client);
    assert!(empty.payload["connectorProfiles"]
        .as_array()
        .unwrap()
        .is_empty());

    drop(client);
    drop(guard);
    let path = std::path::Path::new(&database);
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}-wal", path.display()));
    let _ = std::fs::remove_file(format!("{}-shm", path.display()));
}

#[test]
fn connector_health_query_is_profile_specific_and_secret_safe_over_named_pipe() {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock before epoch")
        .as_nanos();
    let pipe = format!(r"\\.\pipe\agenttalk-connector-health-{nonce}");
    let database = std::env::temp_dir()
        .join(format!("agenttalk-connector-health-{nonce}.sqlite3"))
        .to_string_lossy()
        .into_owned();
    let credential = format!("connector-health-credential-{}", "x".repeat(40));
    let (guard, mut client) = connect(&pipe, &database, &credential);

    client
        .write_json(&command(
            "health-create",
            "connector.create",
            json!({
                "scopeId": "desktop",
                "connectorId": "mock-profile",
                "displayName": "Local Mock",
                "providerType": "mock",
                "runtimeType": "mock",
                "enabled": true,
                "authEnvKey": "AGENTTALK_FIXTURE_KEY"
            }),
        ))
        .expect("write health profile");
    assert!(read_response(&mut client).ok);

    client
        .write_json(&query(
            "health-query",
            "connector.health",
            json!({"scopeId": "desktop", "connectorId": "mock-profile"}),
        ))
        .expect("write connector health query");
    let health = read_response(&mut client);
    assert!(health.ok);
    assert_eq!(health.payload["schemaVersion"], "connector.health.v1");
    assert_eq!(health.payload["scopeId"], "desktop");
    assert_eq!(health.payload["connector"]["connectorId"], "mock-profile");
    assert_eq!(health.payload["connector"]["availability"], "available");
    assert_eq!(health.payload["connector"]["ok"], true);
    assert_eq!(health.payload["connector"]["verified"], false);
    assert_eq!(
        health.payload["connector"]["verification"],
        "local_adapter_only"
    );
    assert_eq!(health.payload["connector"]["authReferencePresent"], true);
    let serialized = serde_json::to_string(&health.payload)
        .expect("serialize health")
        .to_ascii_lowercase();
    for forbidden in ["agenttalk_fixture_key", "token", "secret", "authorization"] {
        assert!(!serialized.contains(forbidden));
    }

    client
        .write_json(&query(
            "health-invalid",
            "connector.health",
            json!({"scopeId": "desktop", "connectorId": "mock-profile", "extra": true}),
        ))
        .expect("write invalid health query");
    assert_eq!(read_error(&mut client).code, "INVALID_QUERY");

    client
        .write_json(&query(
            "health-missing",
            "connector.health",
            json!({"scopeId": "desktop", "connectorId": "missing-profile"}),
        ))
        .expect("write missing health query");
    assert_eq!(read_error(&mut client).code, "QUERY_REJECTED");

    drop(client);
    drop(guard);
    let path = std::path::Path::new(&database);
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}-wal", path.display()));
    let _ = std::fs::remove_file(format!("{}-shm", path.display()));
}
