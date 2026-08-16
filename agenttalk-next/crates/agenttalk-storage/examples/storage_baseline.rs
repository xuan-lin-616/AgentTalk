//! Reproducible, isolated SQLite/Storage baseline.
//!
//! This intentionally measures only the local data layer. It does not start
//! the desktop app, contact a Provider, or read any existing AgentTalk data.

use agenttalk_domain::{ExecutionRun, ExecutionStatus, Message, ScopeSnapshot, WorkspaceAccess};
use agenttalk_events::RuntimeEvent;
use agenttalk_storage::SqliteStore;
use serde_json::json;
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

const MESSAGE_COUNT: usize = 10_000;
const RUN_COUNT: usize = 100;
const EVENT_COUNT: usize = 1_000;

fn elapsed_ms(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1_000.0
}

fn isolated_database_path() -> PathBuf {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "agenttalk-storage-baseline-{}-{nonce}.sqlite3",
        std::process::id()
    ))
}

fn remove_database(path: &PathBuf) {
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(path.with_extension("sqlite3-wal"));
    let _ = fs::remove_file(path.with_extension("sqlite3-shm"));
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = isolated_database_path();
    remove_database(&path);

    let open_start = Instant::now();
    let mut store = SqliteStore::open(&path)?;
    let open_ms = elapsed_ms(open_start);

    store.create_project("project-baseline", "Storage baseline", None)?;
    store.create_conversation(
        "conversation-baseline",
        "project-baseline",
        "Storage baseline",
    )?;

    let write_messages_start = Instant::now();
    for sequence in 0..MESSAGE_COUNT {
        store.create_message(&Message {
            id: format!("message-baseline-{sequence:05}"),
            conversation_id: "conversation-baseline".into(),
            sender_id: if sequence % 2 == 0 {
                "user".into()
            } else {
                "agent".into()
            },
            sequence: sequence as u64,
            content: format!(
                "baseline message {sequence:05} with searchable baseline-needle {}",
                sequence % 100
            ),
        })?;
    }
    let messages_write_ms = elapsed_ms(write_messages_start);

    let recent_load_start = Instant::now();
    let recent_messages = store.load_recent_message_contents("conversation-baseline", 50)?;
    let recent_load_ms = elapsed_ms(recent_load_start);

    let fts_search_start = Instant::now();
    let search_hits = store.search_messages("baseline-needle", None, 100)?;
    let fts_search_ms = elapsed_ms(fts_search_start);

    let runs_write_start = Instant::now();
    for index in 0..RUN_COUNT {
        store.upsert_execution_run(&ExecutionRun {
            id: format!("run-baseline-{index:03}"),
            collaboration_run_id: format!("collaboration-baseline-{index:03}"),
            project_id: "project-baseline".into(),
            conversation_id: "conversation-baseline".into(),
            agent_id: "agent-baseline".into(),
            status: ExecutionStatus::Completed,
            version: 1,
            scope: ScopeSnapshot {
                project_id: "project-baseline".into(),
                conversation_id: "conversation-baseline".into(),
                agent_id: "agent-baseline".into(),
                workspace_access: WorkspaceAccess::None,
                canonical_cwd: None,
            },
            terminal_reason: Some("baseline".into()),
        })?;
    }
    let runs_write_ms = elapsed_ms(runs_write_start);

    let runs_load_start = Instant::now();
    let loaded_runs = store.load_execution_runs()?;
    let runs_load_ms = elapsed_ms(runs_load_start);

    let event_append_start = Instant::now();
    for sequence in 0..EVENT_COUNT {
        let run_index = sequence % RUN_COUNT;
        store.append_event(&RuntimeEvent {
            event_id: format!("event-baseline-{sequence:04}"),
            execution_run_id: format!("run-baseline-{run_index:03}"),
            runtime_id: "local-baseline".into(),
            thread_id: None,
            turn_id: None,
            sequence: sequence as u64,
            event_type: "output.delta".into(),
            timestamp_ms: sequence as i64,
            payload: json!({"delta": format!("event-{sequence:04}")}),
        })?;
    }
    let event_append_ms = elapsed_ms(event_append_start);

    let event_replay_start = Instant::now();
    let replayed_events = store.replay_after(0)?;
    let event_replay_ms = elapsed_ms(event_replay_start);

    drop(store);

    let reopen_start = Instant::now();
    let reopened = SqliteStore::open(&path)?;
    let reopen_ms = elapsed_ms(reopen_start);
    let reopened_runs = reopened.load_execution_runs()?.len();
    let reopened_events = reopened.replay_after(0)?.len();
    drop(reopened);

    let db_bytes = fs::metadata(&path)
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    let report = json!({
        "scope": "isolated_sqlite_storage_only",
        "providerContacted": false,
        "desktopAppStarted": false,
        "messageCount": MESSAGE_COUNT,
        "runCount": RUN_COUNT,
        "eventCount": EVENT_COUNT,
        "metricsMs": {
            "initialOpen": open_ms,
            "messageWrite": messages_write_ms,
            "recentMessageLoad50": recent_load_ms,
            "ftsSearch100": fts_search_ms,
            "executionRunWrite100": runs_write_ms,
            "executionRunLoad100": runs_load_ms,
            "eventAppend1000": event_append_ms,
            "eventReplay1000": event_replay_ms,
            "reopen": reopen_ms,
        },
        "checks": {
            "recentMessagesLoaded": recent_messages.len(),
            "ftsHits": search_hits.len(),
            "runsLoaded": loaded_runs.len(),
            "eventsReplayed": replayed_events.len(),
            "reopenedRuns": reopened_runs,
            "reopenedEvents": reopened_events,
        },
        "databaseBytes": db_bytes,
    });
    println!("{}", serde_json::to_string_pretty(&report)?);

    remove_database(&path);
    Ok(())
}
