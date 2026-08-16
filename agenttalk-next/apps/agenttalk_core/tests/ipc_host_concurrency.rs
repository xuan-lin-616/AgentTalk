#![cfg(windows)]

use agenttalk_ipc::{FramedTransport, NamedPipeClient};
use agenttalk_protocols::{
    CommandEnvelope, ErrorEnvelope, EventEnvelope, ProtocolHandshake, ProtocolVersion,
    QueryEnvelope, ResponseEnvelope, PROTOCOL_MAJOR,
};
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

fn connect(pipe: &str) -> agenttalk_ipc::NamedPipeConnection {
    for _ in 0..100 {
        if let Ok(client) = NamedPipeClient::connect(pipe) {
            return client;
        }
        thread::sleep(Duration::from_millis(25));
    }
    panic!("Core host did not create its Named Pipe");
}

fn handshake(
    client: &mut agenttalk_ipc::NamedPipeConnection,
    session_id: &str,
    credential: &str,
) -> ResponseEnvelope {
    client
        .write_json(&ProtocolHandshake {
            kind: "handshake".into(),
            protocol: ProtocolVersion {
                major: PROTOCOL_MAJOR,
                minor: 0,
            },
            client_id: format!("client-{session_id}"),
            session_id: session_id.into(),
            session_credential: credential.into(),
            max_message_bytes: 1024 * 1024,
            last_seen: None,
        })
        .unwrap();
    let response: ResponseEnvelope = serde_json::from_slice(&client.read_json().unwrap()).unwrap();
    assert!(response.ok);
    response
}

#[test]
fn subscription_event_pump_does_not_block_another_connection_owner() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let pipe = format!(
        "\\\\.\\pipe\\agenttalk-core-concurrency-test-{}-{nonce}",
        std::process::id(),
    );
    let database = std::env::temp_dir().join(format!(
        "agenttalk-core-concurrency-test-{}-{nonce}.db",
        std::process::id(),
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

    let mut subscription_client = connect(&pipe);
    let subscription_session = "session-concurrency-subscription-123456";
    let handshake_response = handshake(&mut subscription_client, subscription_session, &credential);
    subscription_client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "seed-event-concurrency".into(),
            session_id: subscription_session.into(),
            command: "project.create".into(),
            payload: json!({
                "projectId": "project-concurrency-seed",
                "name": "Concurrency seed"
            }),
            deadline_ms: None,
        })
        .unwrap();
    let seeded: ResponseEnvelope =
        serde_json::from_slice(&subscription_client.read_json().unwrap()).unwrap();
    assert!(seeded.ok);
    let epoch = handshake_response.payload["serverEpoch"]
        .as_str()
        .unwrap()
        .to_owned();
    subscription_client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "subscription-start-concurrency".into(),
            session_id: subscription_session.into(),
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

    let mut ordinary_client = connect(&pipe);
    let ordinary_session = "session-concurrency-ordinary-123456";
    let _ = handshake(&mut ordinary_client, ordinary_session, &credential);
    ordinary_client
        .write_json(&QueryEnvelope {
            kind: "query".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "runtime-health-concurrency".into(),
            session_id: ordinary_session.into(),
            query: "runtime.health".into(),
            payload: json!({}),
        })
        .unwrap();
    let health: ResponseEnvelope =
        serde_json::from_slice(&ordinary_client.read_json().unwrap()).unwrap();
    assert_eq!(health.payload["status"], "ready");

    subscription_client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "subscription-stop-concurrency".into(),
            session_id: subscription_session.into(),
            command: "events.unsubscribe".into(),
            payload: json!({"subscriptionId": subscription_id}),
            deadline_ms: None,
        })
        .unwrap();
    let unsubscribed: ResponseEnvelope =
        serde_json::from_slice(&subscription_client.read_json().unwrap()).unwrap();
    assert_eq!(unsubscribed.payload["unsubscribed"], true);
    assert!(first_event.subscription_id.is_some());

    drop(subscription_client);
    ordinary_client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "shutdown-concurrency".into(),
            session_id: ordinary_session.into(),
            command: "shutdown_owned".into(),
            payload: json!({}),
            deadline_ms: None,
        })
        .unwrap();
    let shutdown: ResponseEnvelope =
        serde_json::from_slice(&ordinary_client.read_json().unwrap()).unwrap();
    assert_eq!(shutdown.payload["shutdownAccepted"], true);

    drop(ordinary_client);
    let _ = std::fs::remove_file(&database);
    let _ = std::fs::remove_file(database.with_extension("db-wal"));
    let _ = std::fs::remove_file(database.with_extension("db-shm"));
}

#[test]
fn slow_subscription_reports_replay_gap_after_bounded_retention_eviction() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let pipe = format!(
        "\\\\.\\pipe\\agenttalk-core-retention-test-{}-{nonce}",
        std::process::id(),
    );
    let database = std::env::temp_dir().join(format!(
        "agenttalk-core-retention-test-{}-{nonce}.db",
        std::process::id(),
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

    let mut subscription_client = connect(&pipe);
    let subscription_session = "session-retention-subscription-123456";
    let handshake_response = handshake(&mut subscription_client, subscription_session, &credential);
    let epoch = handshake_response.payload["serverEpoch"]
        .as_str()
        .unwrap()
        .to_owned();
    subscription_client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "retention-subscribe".into(),
            session_id: subscription_session.into(),
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

    let mut ordinary_client = connect(&pipe);
    let ordinary_session = "session-retention-ordinary-123456";
    let _ = handshake(&mut ordinary_client, ordinary_session, &credential);
    for index in 0..=257 {
        ordinary_client
            .write_json(&CommandEnvelope {
                kind: "command".into(),
                protocol: ProtocolVersion { major: 1, minor: 0 },
                request_id: format!("retention-project-{index}"),
                session_id: ordinary_session.into(),
                command: "project.create".into(),
                payload: json!({
                    "projectId": format!("retention-project-{index}"),
                    "name": format!("Retention project {index}")
                }),
                deadline_ms: None,
            })
            .unwrap();
        let response: ResponseEnvelope =
            serde_json::from_slice(&ordinary_client.read_json().unwrap()).unwrap();
        assert!(response.ok);
        if index == 0 {
            let first_event: EventEnvelope =
                serde_json::from_slice(&subscription_client.read_json().unwrap()).unwrap();
            assert_eq!(first_event.cursor.sequence, 1);
        }
    }

    ordinary_client
        .write_json(&QueryEnvelope {
            kind: "query".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "retention-replay-gap".into(),
            session_id: ordinary_session.into(),
            query: "events.replay".into(),
            payload: json!({"afterSequence": 0}),
        })
        .unwrap();
    let replay_gap: ErrorEnvelope =
        serde_json::from_slice(&ordinary_client.read_json().unwrap()).unwrap();
    assert_eq!(replay_gap.code, "REPLAY_GAP");
    assert!(replay_gap.retryable);

    ordinary_client
        .write_json(&QueryEnvelope {
            kind: "query".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "retention-health".into(),
            session_id: ordinary_session.into(),
            query: "runtime.health".into(),
            payload: json!({}),
        })
        .unwrap();
    let health: ResponseEnvelope =
        serde_json::from_slice(&ordinary_client.read_json().unwrap()).unwrap();
    assert_eq!(health.payload["status"], "ready");

    subscription_client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "retention-ack-old".into(),
            session_id: subscription_session.into(),
            command: "events.ack".into(),
            payload: json!({
                "subscriptionId": subscription_id,
                "cursor": {"streamId":"core-events", "sequence":1, "epoch":epoch}
            }),
            deadline_ms: None,
        })
        .unwrap();
    let ack: ResponseEnvelope =
        serde_json::from_slice(&subscription_client.read_json().unwrap()).unwrap();
    assert_eq!(ack.payload["acknowledged"], true);
    let gap: ErrorEnvelope =
        serde_json::from_slice(&subscription_client.read_json().unwrap()).unwrap();
    assert_eq!(gap.code, "REPLAY_GAP");
    assert!(gap.retryable);
    let details = gap.details.expect("REPLAY_GAP details");
    assert_eq!(details["requiresSnapshot"], true);
    assert_eq!(
        details["recovery"],
        "snapshot_then_subscribe_from_resume_cursor"
    );
    assert!(details["resumeCursor"]["sequence"].as_u64().unwrap() > 1);

    drop(subscription_client);
    ordinary_client
        .write_json(&CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "shutdown-retention".into(),
            session_id: ordinary_session.into(),
            command: "shutdown_owned".into(),
            payload: json!({}),
            deadline_ms: None,
        })
        .unwrap();
    let shutdown: ResponseEnvelope =
        serde_json::from_slice(&ordinary_client.read_json().unwrap()).unwrap();
    assert_eq!(shutdown.payload["shutdownAccepted"], true);

    drop(ordinary_client);
    let _ = std::fs::remove_file(&database);
    let _ = std::fs::remove_file(database.with_extension("db-wal"));
    let _ = std::fs::remove_file(database.with_extension("db-shm"));
}
