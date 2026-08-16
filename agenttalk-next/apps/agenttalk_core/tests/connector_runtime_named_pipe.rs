#![cfg(windows)]

use agenttalk_ipc::{FramedTransport, NamedPipeClient, NamedPipeConnection};
use agenttalk_protocols::{
    CommandEnvelope, ErrorEnvelope, EventEnvelope, ProtocolHandshake, ProtocolVersion,
    QueryEnvelope, ResponseEnvelope, PROTOCOL_MAJOR,
};
use agenttalk_storage::SqliteStore;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const IPC_TIMEOUT: Duration = Duration::from_secs(10);
const CORE_EXIT_TIMEOUT: Duration = Duration::from_secs(5);

/// Owns only the Core process launched by this test.  A failing assertion still
/// cleans up that child, while the normal path must explicitly prove a clean
/// `shutdown_owned` exit before this guard is dropped.
struct OwnedCore {
    child: Option<Child>,
}

impl OwnedCore {
    fn spawn(pipe: &str, database: &Path, artifact_root: &Path, credential: &str) -> Self {
        let executable = env!("CARGO_BIN_EXE_agenttalk-core");
        let child = Command::new(executable)
            .args([
                pipe,
                &database.to_string_lossy(),
                &artifact_root.to_string_lossy(),
            ])
            .env("AGENTTALK_CORE_SESSION_CREDENTIAL", credential)
            .env("AGENTTALK_CORE_RUNTIME", "fixture-dual")
            .env("AGENTTALK_CORE_DEV_MODE", "1")
            // A user-level multi-Runtime setting must never leak into this
            // credential-free fixture process.
            .env_remove("AGENTTALK_CORE_RUNTIMES")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn fixture-dual Core binary");
        Self { child: Some(child) }
    }

    /// Starts the binary with no Runtime-selection environment variable. The
    /// missing Codex binary and empty Kun data directory prove that built-in
    /// registration is lazy and credential-free rather than accidentally
    /// probing a user installation during this isolated acceptance test.
    fn spawn_inert_production_registry(
        pipe: &str,
        database: &Path,
        artifact_root: &Path,
        credential: &str,
    ) -> Self {
        let executable = env!("CARGO_BIN_EXE_agenttalk-core");
        let empty_kun_data = artifact_root.join("empty-kun-data");
        let empty_local_app_data = artifact_root.join("empty-localappdata");
        let empty_path = artifact_root.join("empty-path");
        fs::create_dir_all(&empty_kun_data).expect("create isolated empty Kun data directory");
        fs::create_dir_all(&empty_local_app_data)
            .expect("create isolated empty Codex local app data directory");
        fs::create_dir_all(&empty_path).expect("create isolated empty process PATH directory");
        let missing_codex_binary = artifact_root.join("missing-codex-app-server.exe");
        let child = Command::new(executable)
            .args([
                pipe,
                &database.to_string_lossy(),
                &artifact_root.to_string_lossy(),
            ])
            .env("AGENTTALK_CORE_SESSION_CREDENTIAL", credential)
            .env("AGENTTALK_CODEX_BINARY", &missing_codex_binary)
            .env("KUN_DATA_DIR", &empty_kun_data)
            .env_remove("KUN_INSTALL_DIR")
            // The test must not inherit the Codex task's actual Desktop
            // installation or PATH. Core itself is launched by absolute path.
            .env("LOCALAPPDATA", &empty_local_app_data)
            .env("PATH", &empty_path)
            .env_remove("AGENTTALK_CORE_RUNTIME")
            .env_remove("AGENTTALK_CORE_RUNTIMES")
            .env_remove("AGENTTALK_CORE_DEV_MODE")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn inert production-registry Core binary");
        Self { child: Some(child) }
    }

    fn spawn_production_kun_registry(
        pipe: &str,
        database: &Path,
        artifact_root: &Path,
        kun_data_dir: &Path,
        kun_install_dir: &Path,
        credential: &str,
    ) -> Self {
        let executable = env!("CARGO_BIN_EXE_agenttalk-core");
        let child = Command::new(executable)
            .args([
                pipe,
                &database.to_string_lossy(),
                &artifact_root.to_string_lossy(),
            ])
            .env("AGENTTALK_CORE_SESSION_CREDENTIAL", credential)
            .env("KUN_DATA_DIR", kun_data_dir)
            .env("KUN_INSTALL_DIR", kun_install_dir)
            .env(
                "AGENTTALK_CODEX_BINARY",
                artifact_root.join("missing-codex.exe"),
            )
            .env_remove("AGENTTALK_CORE_RUNTIME")
            .env_remove("AGENTTALK_CORE_RUNTIMES")
            .env_remove("AGENTTALK_CORE_DEV_MODE")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn production-Kun Core binary");
        Self { child: Some(child) }
    }

    fn spawn_local_discovery_registry(
        pipe: &str,
        database: &Path,
        artifact_root: &Path,
        codex_binary: &Path,
        kun_data_dir: &Path,
        kun_install_dir: &Path,
        credential: &str,
    ) -> Self {
        let executable = env!("CARGO_BIN_EXE_agenttalk-core");
        let empty_path = artifact_root.join("empty-path");
        let empty_local_app_data = artifact_root.join("empty-localappdata");
        fs::create_dir_all(&empty_path).expect("create isolated empty process PATH directory");
        fs::create_dir_all(&empty_local_app_data)
            .expect("create isolated empty Codex local app data directory");
        let child = Command::new(executable)
            .args([
                pipe,
                &database.to_string_lossy(),
                &artifact_root.to_string_lossy(),
            ])
            .env("AGENTTALK_CORE_SESSION_CREDENTIAL", credential)
            .env("AGENTTALK_CODEX_BINARY", codex_binary)
            .env("KUN_DATA_DIR", kun_data_dir)
            .env("KUN_INSTALL_DIR", kun_install_dir)
            .env("LOCALAPPDATA", empty_local_app_data)
            .env("PATH", empty_path)
            .env_remove("AGENTTALK_CORE_RUNTIME")
            .env_remove("AGENTTALK_CORE_RUNTIMES")
            .env_remove("AGENTTALK_CORE_DEV_MODE")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn isolated local-discovery Core binary");
        Self { child: Some(child) }
    }

    fn wait_for_clean_exit(&mut self) -> ExitStatus {
        let deadline = Instant::now() + CORE_EXIT_TIMEOUT;
        loop {
            let child = self.child.as_mut().expect("Core child must still be owned");
            match child.try_wait().expect("poll Core child exit") {
                Some(status) => {
                    assert!(status.success(), "Core exited unsuccessfully: {status}");
                    let _ = self.child.take();
                    return status;
                }
                None if Instant::now() < deadline => thread::sleep(Duration::from_millis(20)),
                None => {
                    panic!("Core did not exit after shutdown_owned within {CORE_EXIT_TIMEOUT:?}")
                }
            }
        }
    }
}

impl Drop for OwnedCore {
    fn drop(&mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };
        if child.try_wait().ok().flatten().is_none() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[derive(Clone, Debug)]
struct ProductionKunRequest {
    path: String,
    authenticated: bool,
    body: Value,
}

#[derive(Clone, Copy)]
enum ProductionKunSseMode {
    Default,
    ChineseSplitContentLength,
    ChineseSplitChunked,
    Hold,
    DelayedCompletion,
    NoDeltaCompletion,
}

#[derive(Clone, Copy)]
struct ProductionKunFixtureConfig {
    sse_mode: ProductionKunSseMode,
    interrupt_status: u16,
    delay_turn_post: bool,
    runtime_info_status: u16,
    provider_auth_on_turn: bool,
    runtime_info_identity_mismatch: bool,
}

impl Default for ProductionKunFixtureConfig {
    fn default() -> Self {
        Self {
            sse_mode: ProductionKunSseMode::Default,
            interrupt_status: 200,
            delay_turn_post: false,
            runtime_info_status: 200,
            provider_auth_on_turn: false,
            runtime_info_identity_mismatch: false,
        }
    }
}

/// Minimal local HTTP/SSE implementation used only by this integration test.
/// It is deliberately external to the Core process: the exercised path is
/// Core Named Pipe -> built-in production Registry -> Kun transport -> socket.
struct ProductionKunFixture {
    stop: Arc<AtomicBool>,
    requests: Arc<Mutex<Vec<ProductionKunRequest>>>,
    active_turns: Arc<Mutex<BTreeSet<String>>>,
    thread: Option<JoinHandle<()>>,
    token: String,
    config: Arc<Mutex<ProductionKunFixtureConfig>>,
    data_dir: PathBuf,
    port: u16,
}

impl ProductionKunFixture {
    fn start(data_dir: &Path, install_dir: &Path) -> Self {
        fs::create_dir_all(data_dir).expect("create isolated Kun fixture data directory");
        fs::create_dir_all(install_dir).expect("create isolated Kun fixture install directory");
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind local Kun fixture");
        listener
            .set_nonblocking(true)
            .expect("configure local Kun fixture listener");
        let port = listener
            .local_addr()
            .expect("fixture listener address")
            .port();
        let token = "pipe-fixture-runtime-token".to_owned();
        let data_dir = data_dir.to_path_buf();
        let install_dir = install_dir.to_path_buf();
        let build_metadata = install_dir
            .join("resources")
            .join("app.asar.unpacked")
            .join("kun")
            .join("dist")
            .join("runtime-build.json");
        fs::create_dir_all(
            build_metadata
                .parent()
                .expect("official build metadata path has a parent"),
        )
        .expect("create official Kun build metadata parent");
        fs::write(
            build_metadata,
            serde_json::to_vec(
                &json!({"buildId": "pipe-fixture-build", "serviceVersion": "0.2.34"}),
            )
            .expect("serialize fixture build metadata"),
        )
        .expect("write fixture build metadata");
        assert!(
            !data_dir.join("runtime-build.json").exists(),
            "fixture dataDir must contain runtime.json only"
        );
        fs::write(
            data_dir.join("runtime.json"),
            serde_json::to_vec(&json!({
                "version": 2,
                "instanceId": "pipe-fixture-instance",
                "pid": std::process::id(),
                "startedAt": "2026-08-09T00:00:00.000Z",
                "host": "127.0.0.1",
                "port": port,
                "baseUrl": format!("http://127.0.0.1:{port}"),
                "runtimeToken": token,
                "insecure": false,
                "serviceVersion": "0.2.34",
                "buildId": "pipe-fixture-build",
                "launchMode": "shared",
            }))
            .expect("serialize fixture runtime record"),
        )
        .expect("write fixture runtime record");

        let stop = Arc::new(AtomicBool::new(false));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let active_turns = Arc::new(Mutex::new(BTreeSet::new()));
        let config = Arc::new(Mutex::new(ProductionKunFixtureConfig::default()));
        let thread_stop = Arc::clone(&stop);
        let thread_requests = Arc::clone(&requests);
        let thread_active_turns = Arc::clone(&active_turns);
        let thread_config = Arc::clone(&config);
        let thread_data_dir = data_dir.clone();
        let thread_token = token.clone();
        let thread = thread::spawn(move || {
            while !thread_stop.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let request_data_dir = thread_data_dir.clone();
                        let request_token = thread_token.clone();
                        let request_log = Arc::clone(&thread_requests);
                        let request_active_turns = Arc::clone(&thread_active_turns);
                        let request_config = Arc::clone(&thread_config);
                        thread::spawn(move || {
                            let mut stream = stream;
                            let _ = handle_production_kun_fixture_request(
                                &mut stream,
                                &request_data_dir,
                                &request_token,
                                &request_log,
                                &request_active_turns,
                                &request_config,
                            );
                        });
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => return,
                }
            }
        });
        Self {
            stop,
            requests,
            active_turns,
            thread: Some(thread),
            token,
            config,
            data_dir,
            port,
        }
    }

    fn configure(&self, config: ProductionKunFixtureConfig) {
        *self
            .config
            .lock()
            .expect("fixture config lock should unlock") = config;
        // Each production scenario is an independent synthetic Runtime turn.
        // Reset only between scenarios, never while an assertion is observing
        // the active/interrupt transition inside one scenario.
        self.active_turns
            .lock()
            .expect("fixture turn state lock should unlock")
            .clear();
    }

    fn write_runtime_rendezvous(&self, host: &str) {
        fs::write(
            self.data_dir.join("runtime.json"),
            serde_json::to_vec(&json!({
                "version": 2,
                "instanceId": "pipe-fixture-instance",
                "pid": std::process::id(),
                "startedAt": "2026-08-09T00:00:00.000Z",
                "host": host,
                "port": self.port,
                "baseUrl": format!("http://{host}:{}", self.port),
                "runtimeToken": self.token,
                "insecure": false,
                "serviceVersion": "0.2.34",
                "buildId": "pipe-fixture-build",
                "launchMode": "shared",
            }))
            .expect("serialize fixture runtime rendezvous"),
        )
        .expect("write fixture runtime rendezvous");
    }

    fn saw_body(&self, path: &str, expected: &Value) -> bool {
        self.requests
            .lock()
            .expect("fixture request log should unlock")
            .iter()
            .any(|request| request.path == path && request.body == *expected)
    }

    fn all_authenticated(&self) -> bool {
        self.requests
            .lock()
            .expect("fixture request log should unlock")
            .iter()
            .all(|request| request.authenticated)
    }

    fn request_count(&self, expected: &str) -> usize {
        self.requests
            .lock()
            .expect("fixture request log should unlock")
            .iter()
            .filter(|request| request.path == expected)
            .count()
    }

    fn wait_for_request_count(&self, expected: &str, minimum: usize, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        while self.request_count(expected) < minimum {
            assert!(
                Instant::now() < deadline,
                "fixture received only {} of {minimum} request(s) for {expected} before timeout",
                self.request_count(expected)
            );
            thread::sleep(Duration::from_millis(5));
        }
    }

    fn turn_is_active(&self, turn_id: &str) -> bool {
        self.active_turns
            .lock()
            .expect("fixture turn state lock should unlock")
            .contains(turn_id)
    }
}

impl Drop for ProductionKunFixture {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn handle_production_kun_fixture_request(
    stream: &mut TcpStream,
    data_dir: &Path,
    token: &str,
    requests: &Arc<Mutex<Vec<ProductionKunRequest>>>,
    active_turns: &Arc<Mutex<BTreeSet<String>>>,
    config: &Arc<Mutex<ProductionKunFixtureConfig>>,
) -> std::io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    let mut request = Vec::new();
    let mut buffer = [0u8; 2048];
    while !request.windows(4).any(|window| window == b"\r\n\r\n") {
        let read = stream.read(&mut buffer)?;
        if read == 0 {
            return Ok(());
        }
        request.extend_from_slice(&buffer[..read]);
        if request.len() > 64 * 1024 {
            return Ok(());
        }
    }
    let header_end = request
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|index| index + 4)
        .expect("header terminator was checked");
    let header = String::from_utf8_lossy(&request[..header_end]);
    let mut lines = header.lines();
    let first = lines.next().unwrap_or_default();
    let mut parts = first.split_whitespace();
    let method = parts.next().unwrap_or_default();
    let path = parts
        .next()
        .unwrap_or_default()
        .split('?')
        .next()
        .unwrap_or_default()
        .to_owned();
    let header_lines = header.lines().skip(1).collect::<Vec<_>>();
    let content_length = header_lines
        .iter()
        .find_map(|line| {
            line.strip_prefix("Content-Length:")
                .or_else(|| line.strip_prefix("content-length:"))
                .and_then(|value| value.trim().parse::<usize>().ok())
        })
        .unwrap_or(0);
    let authenticated = header_lines
        .iter()
        .any(|line| line.eq_ignore_ascii_case(&format!("Authorization: Bearer {token}")));
    let mut body = request[header_end..].to_vec();
    while body.len() < content_length {
        let read = stream.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        body.extend_from_slice(&buffer[..read]);
    }
    body.truncate(content_length);
    let body_value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    requests
        .lock()
        .expect("fixture request log should unlock")
        .push(ProductionKunRequest {
            path: path.clone(),
            authenticated,
            body: body_value,
        });
    if !authenticated {
        return write_production_kun_http(stream, 401, "application/json", b"{}");
    }
    let config = *config.lock().expect("fixture config lock should unlock");
    if path == "/v1/runtime/info" && config.runtime_info_status != 200 {
        return write_production_kun_http(
            stream,
            config.runtime_info_status,
            "application/json",
            b"{}",
        );
    }
    if config.provider_auth_on_turn
        && method == "POST"
        && (path == "/v1/threads" || path.ends_with("/turns"))
    {
        return write_production_kun_http(
            stream,
            400,
            "application/json",
            br#"{"error":{"code":"provider_authentication_failed"}}"#,
        );
    }
    let response = match (method, path.as_str()) {
        ("GET", "/v1/runtime/info") => serde_json::to_vec(&json!({
            "dataDir": data_dir,
            "instanceId": if config.runtime_info_identity_mismatch { "wrong-pipe-fixture-instance" } else { "pipe-fixture-instance" },
            "pid": std::process::id(),
            "startedAt": "2026-08-09T00:00:00.000Z",
            "serviceVersion": "0.2.34",
            "buildId": "pipe-fixture-build",
            "launchMode": "shared",
        }))
        .expect("serialize fixture runtime info"),
        ("GET", "/v1/model-connections") => serde_json::to_vec(&json!({
            "revision": 42,
            "defaultProviderId": "pipe-provider-b",
            "providers": [
                {"id": "pipe-provider-a", "configured": true, "selectedModel": "kun-pipe-model-a", "models": [
                    {"id": "kun-pipe-model-a", "available": true, "enabled": true, "capabilities": {"streaming": true, "cancel": true, "filesystem": false, "shell": false}}
                ]},
                {"id": "pipe-provider-b", "configured": true, "selectedModel": "kun-pipe-model-b", "modelCapabilities": {"kun-pipe-model-b": {"streaming": true, "cancel": true, "filesystem": false, "shell": false}}, "models": [
                    {"id": "kun-pipe-model-b", "available": true, "enabled": true, "status": "available"}
                ]}
            ]
        }))
        .expect("serialize fixture catalog"),
        ("POST", "/v1/threads") => serde_json::to_vec(&json!({"id": "pipe-thread"}))
            .expect("serialize fixture thread"),
        ("POST", path) if path.ends_with("/turns") => {
            if config.delay_turn_post {
                thread::sleep(Duration::from_millis(400));
            }
            active_turns
                .lock()
                .expect("fixture turn state lock should unlock")
                .insert("pipe-turn".into());
            serde_json::to_vec(&json!({"turnId": "pipe-turn"})).expect("serialize fixture turn")
        }
        ("GET", path) if path.ends_with("/turns/pipe-turn") => {
            serde_json::to_vec(&json!({
                "items": [{"role": "assistant", "kind": "assistant_text", "text": "recovered production Kun delta"}]
            }))
            .expect("serialize fixture turn recovery")
        }
        ("GET", path) if path.ends_with("/events") => {
            let result = write_production_kun_sse(stream, config.sse_mode);
            if !matches!(config.sse_mode, ProductionKunSseMode::Hold) {
                active_turns
                    .lock()
                    .expect("fixture turn state lock should unlock")
                    .remove("pipe-turn");
            }
            return result;
        }
        ("POST", path) if path.ends_with("/interrupt") => {
            if config.interrupt_status != 200 {
                return write_production_kun_http(
                    stream,
                    config.interrupt_status,
                    "application/json",
                    b"{}",
                );
            }
            active_turns
                .lock()
                .expect("fixture turn state lock should unlock")
                .remove("pipe-turn");
            serde_json::to_vec(&json!({"ok": true})).expect("serialize fixture interrupt")
        }
        _ => b"{}".to_vec(),
    };
    write_production_kun_http(stream, 200, "application/json", &response)
}

fn production_kun_sse_body(mode: ProductionKunSseMode) -> &'static [u8] {
    match mode {
        ProductionKunSseMode::Default
        | ProductionKunSseMode::Hold
        | ProductionKunSseMode::DelayedCompletion => b"data: {\"kind\":\"assistant_text_delta\",\"turnId\":\"pipe-turn\",\"item\":{\"kind\":\"assistant_text\",\"text\":\"pipe production Kun delta\"}}\n\ndata: {\"kind\":\"turn_completed\",\"turnId\":\"pipe-turn\"}\n\n",
        ProductionKunSseMode::ChineseSplitContentLength | ProductionKunSseMode::ChineseSplitChunked => "data: {\"kind\":\"assistant_text_delta\",\"turnId\":\"pipe-turn\",\"item\":{\"kind\":\"assistant_text\",\"text\":\"你好，世界\"}}\n\ndata: {\"kind\":\"turn_completed\",\"turnId\":\"pipe-turn\"}\n\n".as_bytes(),
        ProductionKunSseMode::NoDeltaCompletion => {
            b"data: {\"kind\":\"turn_completed\",\"turnId\":\"pipe-turn\"}\n\n"
        }
    }
}

fn split_multibyte_utf8_body(body: &[u8]) -> Vec<&[u8]> {
    let mut ends = Vec::new();
    for (index, byte) in body.iter().enumerate() {
        if (*byte & 0b1111_0000) == 0b1110_0000 {
            ends.push(index + 1);
            ends.push(index + 2);
        }
    }
    ends.push(body.len());
    ends.sort_unstable();
    ends.dedup();
    let mut start = 0usize;
    let mut segments = Vec::new();
    for end in ends {
        if end > start {
            segments.push(&body[start..end]);
            start = end;
        }
    }
    segments
}

fn write_production_kun_sse(
    stream: &mut TcpStream,
    mode: ProductionKunSseMode,
) -> std::io::Result<()> {
    stream.set_nodelay(true)?;
    if matches!(mode, ProductionKunSseMode::Hold) {
        stream.write_all(
            b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n",
        )?;
        stream.flush()?;
        thread::sleep(Duration::from_millis(500));
        return Ok(());
    }
    if matches!(mode, ProductionKunSseMode::DelayedCompletion) {
        stream.write_all(
            b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n",
        )?;
        stream.flush()?;
        thread::sleep(Duration::from_millis(40));
        stream.write_all(production_kun_sse_body(mode))?;
        return stream.flush();
    }
    let body = production_kun_sse_body(mode);
    let split = matches!(
        mode,
        ProductionKunSseMode::ChineseSplitContentLength | ProductionKunSseMode::ChineseSplitChunked
    );
    let segments = if split {
        split_multibyte_utf8_body(body)
    } else {
        vec![body]
    };
    if matches!(mode, ProductionKunSseMode::ChineseSplitChunked) {
        stream.write_all(
            b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
        )?;
        for segment in segments {
            stream.write_all(format!("{:X}\r\n", segment.len()).as_bytes())?;
            stream.write_all(segment)?;
            stream.write_all(b"\r\n")?;
            stream.flush()?;
        }
        stream.write_all(b"0\r\n\r\n")?;
    } else {
        stream.write_all(
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            )
            .as_bytes(),
        )?;
        for segment in segments {
            stream.write_all(segment)?;
            stream.flush()?;
            if split {
                thread::sleep(Duration::from_millis(2));
            }
        }
    }
    stream.flush()
}

fn write_production_kun_http(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &[u8],
) -> std::io::Result<()> {
    let reason = match status {
        200 => "OK",
        401 => "Unauthorized",
        _ => "Fixture",
    };
    stream.write_all(
        format!(
            "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .as_bytes(),
    )?;
    stream.write_all(body)?;
    stream.flush()
}

struct PipeClient {
    connection: NamedPipeConnection,
    session_id: String,
    server_epoch: String,
    request_number: u64,
}

impl PipeClient {
    fn connect(pipe: &str, credential: &str, client_id: &str, session_id: &str) -> Self {
        let deadline = Instant::now() + IPC_TIMEOUT;
        let connection = loop {
            match NamedPipeClient::connect(pipe) {
                Ok(connection) => break connection,
                Err(error) => {
                    if Instant::now() >= deadline {
                        panic!(
                            "fixture-dual Core did not accept Named Pipe client before timeout: {}",
                            error
                        );
                    }
                    thread::sleep(Duration::from_millis(20));
                }
            }
        };
        let mut connection = connection;
        connection
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
            .expect("write fixture handshake");
        let handshake = read_response(&mut connection, "handshake");
        assert!(handshake.ok, "handshake must succeed");
        assert_eq!(handshake.payload["eventStreamId"], "core-events");
        let server_epoch = handshake.payload["serverEpoch"]
            .as_str()
            .expect("handshake must return a server epoch")
            .to_owned();
        Self {
            connection,
            session_id: session_id.into(),
            server_epoch,
            request_number: 0,
        }
    }

    fn next_request_id(&mut self, prefix: &str) -> String {
        self.request_number += 1;
        format!("dual-runtime-{prefix}-{}", self.request_number)
    }

    fn command(&mut self, command: &str, payload: Value) -> ResponseEnvelope {
        let request_id = self.next_request_id("command");
        self.connection
            .write_json(&CommandEnvelope {
                kind: "command".into(),
                protocol: ProtocolVersion {
                    major: PROTOCOL_MAJOR,
                    minor: 0,
                },
                request_id: request_id.clone(),
                session_id: self.session_id.clone(),
                command: command.into(),
                payload,
                deadline_ms: None,
            })
            .unwrap_or_else(|error| panic!("write command {command}: {error}"));
        let response = read_response(&mut self.connection, command);
        assert_eq!(
            response.request_id, request_id,
            "response request id for {command}"
        );
        assert!(response.ok, "response for {command} must be successful");
        response
    }

    fn command_error(&mut self, command: &str, payload: Value) -> ErrorEnvelope {
        let request_id = self.next_request_id("command-error");
        self.connection
            .write_json(&CommandEnvelope {
                kind: "command".into(),
                protocol: ProtocolVersion {
                    major: PROTOCOL_MAJOR,
                    minor: 0,
                },
                request_id: request_id.clone(),
                session_id: self.session_id.clone(),
                command: command.into(),
                payload,
                deadline_ms: None,
            })
            .unwrap_or_else(|error| panic!("write command {command}: {error}"));
        let error = read_error(&mut self.connection, command);
        assert_eq!(
            error.request_id, request_id,
            "error request id for {command}"
        );
        error
    }

    fn command_result(
        &mut self,
        command: &str,
        payload: Value,
    ) -> Result<ResponseEnvelope, Box<ErrorEnvelope>> {
        let request_id = self.next_request_id("command-result");
        self.connection
            .write_json(&CommandEnvelope {
                kind: "command".into(),
                protocol: ProtocolVersion {
                    major: PROTOCOL_MAJOR,
                    minor: 0,
                },
                request_id: request_id.clone(),
                session_id: self.session_id.clone(),
                command: command.into(),
                payload,
                deadline_ms: None,
            })
            .unwrap_or_else(|error| panic!("write command {command}: {error}"));
        let value = read_value(&mut self.connection, IPC_TIMEOUT, command);
        if value.get("kind").and_then(Value::as_str) == Some("response") {
            let response: ResponseEnvelope = serde_json::from_value(value)
                .unwrap_or_else(|error| panic!("decode {command} response: {error}"));
            assert_eq!(response.request_id, request_id);
            Ok(response)
        } else {
            let error: ErrorEnvelope = serde_json::from_value(value)
                .unwrap_or_else(|error| panic!("decode {command} error: {error}"));
            assert_eq!(error.request_id, request_id);
            Err(Box::new(error))
        }
    }

    fn query(&mut self, query: &str, payload: Value) -> ResponseEnvelope {
        let request_id = self.next_request_id("query");
        self.connection
            .write_json(&QueryEnvelope {
                kind: "query".into(),
                protocol: ProtocolVersion {
                    major: PROTOCOL_MAJOR,
                    minor: 0,
                },
                request_id: request_id.clone(),
                session_id: self.session_id.clone(),
                query: query.into(),
                payload,
            })
            .unwrap_or_else(|error| panic!("write query {query}: {error}"));
        let response = read_response(&mut self.connection, query);
        assert_eq!(
            response.request_id, request_id,
            "response request id for {query}"
        );
        assert!(response.ok, "response for {query} must be successful");
        response
    }

    fn query_error(&mut self, query: &str, payload: Value) -> ErrorEnvelope {
        let request_id = self.next_request_id("query-error");
        self.connection
            .write_json(&QueryEnvelope {
                kind: "query".into(),
                protocol: ProtocolVersion {
                    major: PROTOCOL_MAJOR,
                    minor: 0,
                },
                request_id: request_id.clone(),
                session_id: self.session_id.clone(),
                query: query.into(),
                payload,
            })
            .unwrap_or_else(|error| panic!("write query {query}: {error}"));
        let error = read_error(&mut self.connection, query);
        assert_eq!(error.request_id, request_id, "error request id for {query}");
        error
    }
}

struct EventSubscription {
    connection: NamedPipeConnection,
    session_id: String,
    server_epoch: String,
    subscription_id: String,
    request_number: u64,
    last_cursor: u64,
}

impl EventSubscription {
    fn start(mut client: PipeClient, after_sequence: u64) -> Self {
        let request_id = client.next_request_id("subscribe");
        client
            .connection
            .write_json(&CommandEnvelope {
                kind: "command".into(),
                protocol: ProtocolVersion {
                    major: PROTOCOL_MAJOR,
                    minor: 0,
                },
                request_id: request_id.clone(),
                session_id: client.session_id.clone(),
                command: "events.subscribe".into(),
                payload: json!({
                    "afterCursor": {
                        "streamId": "core-events",
                        "sequence": after_sequence,
                        "epoch": client.server_epoch,
                    },
                    "maxInFlightEvents": 1,
                    "maxInFlightBytes": 262144,
                }),
                deadline_ms: None,
            })
            .expect("write events.subscribe");
        let response = read_response(&mut client.connection, "events.subscribe");
        assert_eq!(response.request_id, request_id);
        assert!(response.ok);
        let subscription_id = response.payload["subscriptionId"]
            .as_str()
            .expect("events.subscribe must return subscriptionId")
            .to_owned();
        assert_eq!(response.payload["cursor"]["sequence"], after_sequence);
        assert_eq!(response.payload["cursor"]["epoch"], client.server_epoch);
        Self {
            connection: client.connection,
            session_id: client.session_id,
            server_epoch: client.server_epoch,
            subscription_id,
            request_number: 0,
            last_cursor: after_sequence,
        }
    }

    fn next_event(&mut self, timeout: Duration) -> EventEnvelope {
        let value = read_value(&mut self.connection, timeout, "event subscription");
        assert_eq!(
            value.get("kind").and_then(Value::as_str),
            Some("event"),
            "subscription emitted a non-event frame: {value}"
        );
        let event: EventEnvelope =
            serde_json::from_value(value).expect("decode subscribed event envelope");
        assert_eq!(event.protocol.major, PROTOCOL_MAJOR);
        assert_eq!(event.session_id, self.session_id);
        assert_eq!(
            event.subscription_id.as_deref(),
            Some(self.subscription_id.as_str())
        );
        assert_eq!(event.cursor.stream_id, "core-events");
        assert_eq!(
            event.cursor.epoch.as_deref(),
            Some(self.server_epoch.as_str())
        );
        assert!(
            event.cursor.sequence > self.last_cursor,
            "event cursor must advance: {} <= {}",
            event.cursor.sequence,
            self.last_cursor
        );
        self.ack(&event);
        self.last_cursor = event.cursor.sequence;
        event
    }

    fn ack(&mut self, event: &EventEnvelope) {
        self.request_number += 1;
        let request_id = format!("dual-runtime-event-ack-{}", self.request_number);
        self.connection
            .write_json(&CommandEnvelope {
                kind: "command".into(),
                protocol: ProtocolVersion {
                    major: PROTOCOL_MAJOR,
                    minor: 0,
                },
                request_id: request_id.clone(),
                session_id: self.session_id.clone(),
                command: "events.ack".into(),
                payload: json!({
                    "subscriptionId": self.subscription_id,
                    "cursor": event.cursor,
                }),
                deadline_ms: None,
            })
            .expect("write events.ack");
        let response = read_response(&mut self.connection, "events.ack");
        assert_eq!(response.request_id, request_id);
        assert_eq!(response.payload["acknowledged"], true);
        assert_eq!(
            response.payload["cursor"]["sequence"],
            event.cursor.sequence
        );
    }
}

fn read_value(connection: &mut NamedPipeConnection, timeout: Duration, context: &str) -> Value {
    let deadline = Instant::now() + timeout;
    loop {
        match connection.try_read_json() {
            Ok(Some(bytes)) => {
                return serde_json::from_slice(&bytes)
                    .unwrap_or_else(|error| panic!("decode {context} JSON: {error}"));
            }
            Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(5)),
            Ok(None) => panic!("timed out waiting for {context} after {timeout:?}"),
            Err(error) => panic!("read {context}: {error}"),
        }
    }
}

fn read_response(connection: &mut NamedPipeConnection, context: &str) -> ResponseEnvelope {
    let value = read_value(connection, IPC_TIMEOUT, context);
    if value.get("kind").and_then(Value::as_str) == Some("error") {
        let error: ErrorEnvelope = serde_json::from_value(value).expect("decode unexpected error");
        panic!(
            "{context} unexpectedly failed with {}: {}",
            error.code, error.message
        );
    }
    serde_json::from_value(value)
        .unwrap_or_else(|error| panic!("decode {context} response: {error}"))
}

fn read_error(connection: &mut NamedPipeConnection, context: &str) -> ErrorEnvelope {
    let value = read_value(connection, IPC_TIMEOUT, context);
    if value.get("kind").and_then(Value::as_str) == Some("response") {
        let response: ResponseEnvelope =
            serde_json::from_value(value).expect("decode unexpected response");
        panic!(
            "{context} unexpectedly succeeded with payload {}",
            response.payload
        );
    }
    serde_json::from_value(value).unwrap_or_else(|error| panic!("decode {context} error: {error}"))
}

fn connector_profile(
    connector_id: &str,
    display_name: &str,
    runtime_type: &str,
    enabled: bool,
) -> Value {
    json!({
        "scopeId": "desktop",
        "connectorId": connector_id,
        "displayName": display_name,
        "providerType": runtime_type,
        "runtimeType": runtime_type,
        "enabled": enabled,
    })
}

fn assert_classified_error(error: &ErrorEnvelope, code: &str, category: &str) {
    assert_eq!(error.code, code);
    assert!(!error.retryable);
    assert_eq!(
        error
            .details
            .as_ref()
            .and_then(|details| details.get("category"))
            .and_then(Value::as_str),
        Some(category),
        "{code} must retain its stable category"
    );
}

fn is_runtime_route_event(event: &EventEnvelope) -> bool {
    matches!(
        event.event.as_str(),
        "scope.frozen"
            | "connector.started"
            | "runtime.started"
            | "output.delta"
            | "execution.completed"
            | "execution.failed"
            | "execution.cancelled"
            | "execution.interrupted"
    )
}

fn is_terminal_event(event: &EventEnvelope) -> bool {
    matches!(
        event.event.as_str(),
        "execution.completed"
            | "execution.failed"
            | "execution.cancelled"
            | "execution.interrupted"
    )
}

fn assert_runtime_route(
    event: &EventEnvelope,
    connector_id: &str,
    runtime_type: &str,
    model_id: &str,
    catalog_revision: u64,
) {
    if !is_runtime_route_event(event) {
        return;
    }
    assert_eq!(
        event.payload["connectorId"], connector_id,
        "{} connector route",
        event.event
    );
    assert_eq!(
        event.payload["modelId"], model_id,
        "{} model route",
        event.event
    );
    assert_eq!(
        event.payload["runtimeType"], runtime_type,
        "{} runtime route",
        event.event
    );
    let actual_revision = event.payload["catalogRevision"].as_u64().or_else(|| {
        event.payload["catalogRevision"]
            .as_str()
            .and_then(|value| value.parse::<u64>().ok())
    });
    assert_eq!(
        actual_revision,
        Some(catalog_revision),
        "{} catalog revision route",
        event.event
    );
}

fn collect_routed_runs_until_terminal(
    subscription: &mut EventSubscription,
    expected_routes: &[(&str, &str, &str, &str, u64)],
) -> (BTreeMap<String, Vec<EventEnvelope>>, Vec<String>) {
    let routes = expected_routes
        .iter()
        .map(
            |(run_id, connector_id, runtime_type, model_id, catalog_revision)| {
                (
                    (*run_id).to_owned(),
                    (
                        (*connector_id).to_owned(),
                        (*runtime_type).to_owned(),
                        (*model_id).to_owned(),
                        *catalog_revision,
                    ),
                )
            },
        )
        .collect::<BTreeMap<_, _>>();
    let mut observed = BTreeMap::<String, Vec<EventEnvelope>>::new();
    let mut terminal = BTreeSet::<String>::new();
    let mut runtime_event_order = Vec::new();
    let deadline = Instant::now() + IPC_TIMEOUT;

    while terminal.len() != routes.len() {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or(Duration::ZERO);
        assert!(
            !remaining.is_zero(),
            "timed out before all expected Runtime streams reached a terminal event"
        );
        let event = subscription.next_event(remaining);
        let Some(run_id) = event.execution_run_id.as_deref() else {
            continue;
        };
        let Some((connector_id, runtime_type, model_id, catalog_revision)) = routes.get(run_id)
        else {
            continue;
        };
        assert_runtime_route(
            &event,
            connector_id,
            runtime_type,
            model_id,
            *catalog_revision,
        );
        if is_runtime_route_event(&event) {
            runtime_event_order.push(run_id.to_owned());
        }
        if is_terminal_event(&event) {
            terminal.insert(run_id.to_owned());
        }
        observed.entry(run_id.to_owned()).or_default().push(event);
    }
    (observed, runtime_event_order)
}

fn collect_one_routed_run_until_terminal(
    subscription: &mut EventSubscription,
    run_id: &str,
    connector_id: &str,
    runtime_type: &str,
    model_id: &str,
    catalog_revision: u64,
) -> Vec<EventEnvelope> {
    let (mut observed, _) = collect_routed_runs_until_terminal(
        subscription,
        &[(
            run_id,
            connector_id,
            runtime_type,
            model_id,
            catalog_revision,
        )],
    );
    observed
        .remove(run_id)
        .expect("expected run must have routed events")
}

fn wait_for_routed_event(
    subscription: &mut EventSubscription,
    run_id: &str,
    connector_id: &str,
    runtime_type: &str,
    model_id: &str,
    catalog_revision: u64,
    expected_event: &str,
) {
    let deadline = Instant::now() + IPC_TIMEOUT;
    loop {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or(Duration::ZERO);
        assert!(
            !remaining.is_zero(),
            "timed out waiting for {expected_event} for {run_id}"
        );
        let event = subscription.next_event(remaining);
        if event.execution_run_id.as_deref() != Some(run_id) {
            continue;
        }
        assert_runtime_route(
            &event,
            connector_id,
            runtime_type,
            model_id,
            catalog_revision,
        );
        if event.event == expected_event {
            return;
        }
    }
}

fn event_names(events: &[EventEnvelope]) -> Vec<&str> {
    events.iter().map(|event| event.event.as_str()).collect()
}

fn output_deltas(events: &[EventEnvelope]) -> Vec<&str> {
    events
        .iter()
        .filter(|event| event.event == "output.delta")
        .map(|event| {
            event.payload["delta"]
                .as_str()
                .expect("output.delta must contain text")
        })
        .collect()
}

fn replay_run_terminal_events(command: &mut PipeClient, run_id: &str) -> Vec<String> {
    let replay = command.query("events.replay", json!({"afterSequence": 0, "limit": 4096}));
    replay.payload["events"]
        .as_array()
        .expect("event replay must include events")
        .iter()
        .filter(|event| event["executionRunId"].as_str() == Some(run_id))
        .filter_map(|event| event["event"].as_str())
        .filter(|event| {
            matches!(
                *event,
                "execution.completed"
                    | "execution.failed"
                    | "execution.cancelled"
                    | "execution.interrupted"
            )
        })
        .map(str::to_owned)
        .collect()
}

fn assert_persisted_route(
    store: &SqliteStore,
    run_id: &str,
    connector_id: &str,
    runtime_type: &str,
    model_id: &str,
    catalog_revision: u64,
) {
    let snapshot = store
        .load_model_snapshot(run_id)
        .expect("read SQLite model snapshot")
        .expect("run must persist a model snapshot");
    assert_eq!(snapshot.connector_id.as_deref(), Some(connector_id));
    assert_eq!(snapshot.model_id.as_deref(), Some(model_id));
    assert_eq!(snapshot.revision, Some(catalog_revision));

    let selection = store
        .load_model_selection_snapshot(run_id)
        .expect("read SQLite selection snapshot")
        .expect("run must persist a full selection snapshot");
    assert_eq!(selection.connector_id, connector_id);
    assert_eq!(selection.runtime_type, runtime_type);
    assert_eq!(selection.effective_model_id.as_deref(), Some(model_id));
    if let Some(selection_revision) = selection.catalog_revision.as_deref() {
        assert_eq!(
            selection_revision.parse::<u64>().ok(),
            Some(catalog_revision),
            "when a selection carries a Runtime catalog revision it must match its model snapshot"
        );
    }
}

fn assert_context_manifest_route(
    projection: &Value,
    run_id: &str,
    connector_id: &str,
    model_id: &str,
) {
    let manifests = projection["contextManifests"]
        .as_array()
        .expect("SQLite projection context manifests");
    let manifest = manifests
        .iter()
        .find(|manifest| manifest["executionRunId"] == run_id)
        .unwrap_or_else(|| panic!("context manifest is missing for {run_id}"));
    assert_eq!(manifest["connectorId"], connector_id);
    assert_eq!(manifest["modelId"], model_id);
}

fn assert_projection_agent_binding(
    projection: &Value,
    agent_id: &str,
    connector_id: Option<&str>,
    model_id: Option<&str>,
    candidate_model_list_revision: u64,
) {
    let agent = projection["agents"]
        .as_array()
        .expect("projection agents")
        .iter()
        .find(|agent| agent["id"] == agent_id)
        .unwrap_or_else(|| panic!("projection is missing agent {agent_id}"));
    assert_eq!(agent["connectorId"].as_str(), connector_id);
    assert_eq!(agent["modelId"].as_str(), model_id);
    assert_eq!(
        agent["candidateModelListRevision"].as_u64(),
        Some(candidate_model_list_revision)
    );
}

fn assert_persisted_agent_binding(
    store: &SqliteStore,
    agent_id: &str,
    connector_id: Option<&str>,
    model_id: Option<&str>,
    candidate_model_list_revision: u64,
) {
    let binding = store
        .load_agent_model_binding(agent_id)
        .expect("read SQLite model binding")
        .unwrap_or_else(|| panic!("SQLite is missing agent model binding for {agent_id}"));
    assert_eq!(binding.connector_id.as_deref(), connector_id);
    assert_eq!(binding.model_id.as_deref(), model_id);
    assert_eq!(
        binding.candidate_model_list_revision,
        candidate_model_list_revision
    );
}

#[test]
fn real_named_pipe_fixture_dual_routes_connector_catalogs_and_execution_snapshots() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after Unix epoch")
        .as_nanos();
    let state_root = std::env::temp_dir().join(format!(
        "agenttalk-connector-runtime-pipe-{}-{nonce}",
        std::process::id()
    ));
    let database = state_root.join("core.sqlite3");
    let artifact_root = state_root.join("artifacts");
    let workspace_root = state_root.join("workspace");
    fs::create_dir_all(&artifact_root).expect("create isolated artifact root");
    fs::create_dir_all(&workspace_root).expect("create isolated workspace root");

    let pipe = format!(r"\\.\pipe\agenttalk-connector-runtime-{nonce}");
    let credential = format!("fixture-dual-session-credential-{}", "x".repeat(40));
    let session_id = format!("fixture-dual-session-{nonce}");
    let mut core = OwnedCore::spawn(&pipe, &database, &artifact_root, &credential);
    let mut command = PipeClient::connect(
        &pipe,
        &credential,
        "connector-runtime-command-client",
        &session_id,
    );

    // `runtime.models` remains the legacy default-adapter endpoint.  The
    // first fixture adapter is Codex, but connector-aware callers must use
    // the separate query tested below.
    let legacy_models = command.query("runtime.models", json!({}));
    assert_eq!(legacy_models.payload["schemaVersion"], "runtime.models.v1");
    assert_eq!(legacy_models.payload["connectorId"], "codex");
    assert_eq!(
        legacy_models.payload["models"],
        json!(["codex-model-a", "codex-model-b"])
    );

    for profile in [
        connector_profile("codex-fixture", "Offline Codex", "codex", true),
        connector_profile("kun-fixture", "Offline Kun", "kun", true),
        connector_profile("disabled-fixture", "Disabled fixture", "codex", false),
        connector_profile(
            "unavailable-fixture",
            "Unavailable fixture",
            "missing-runtime",
            true,
        ),
    ] {
        let created = command.command("connector.create", profile);
        assert_eq!(created.payload["created"], true);
    }

    let codex_catalog = command.query(
        "connector.models",
        json!({"scopeId":"desktop", "connectorId":"codex-fixture"}),
    );
    let kun_catalog = command.query(
        "connector.models",
        json!({"scopeId":"desktop", "connectorId":"kun-fixture"}),
    );
    let codex_catalog_revision = codex_catalog.payload["catalogRevision"]
        .as_u64()
        .expect("Codex catalog revision must be numeric");
    let kun_catalog_revision = kun_catalog.payload["catalogRevision"]
        .as_u64()
        .expect("Kun catalog revision must be numeric");
    for (catalog, connector_id, runtime_type, models, forbidden_models) in [
        (
            &codex_catalog.payload,
            "codex-fixture",
            "codex",
            json!(["codex-model-a", "codex-model-b"]),
            json!(["kun-model-a", "kun-model-b"]),
        ),
        (
            &kun_catalog.payload,
            "kun-fixture",
            "kun",
            json!(["kun-model-a", "kun-model-b"]),
            json!(["codex-model-a", "codex-model-b"]),
        ),
    ] {
        assert_eq!(catalog["schemaVersion"], "connector.models.v1");
        assert_eq!(catalog["scopeId"], "desktop");
        assert_eq!(catalog["connectorId"], connector_id);
        assert_eq!(catalog["runtimeType"], runtime_type);
        assert_eq!(catalog["availability"], "available");
        assert_eq!(catalog["models"], models);
        for model in forbidden_models.as_array().expect("fixture model list") {
            assert!(
                !catalog["models"]
                    .as_array()
                    .expect("catalog models")
                    .contains(model),
                "{connector_id} catalog leaked a foreign connector model: {model}"
            );
        }
        let serialized = catalog.to_string().to_ascii_lowercase();
        for forbidden in ["token", "secret", "authorization", "bearer", "api_key"] {
            assert!(
                !serialized.contains(forbidden),
                "connector catalog must remain credential-free: {forbidden}"
            );
        }
    }

    assert_classified_error(
        &command.query_error(
            "connector.models",
            json!({"scopeId":"desktop", "connectorId":"unknown-fixture"}),
        ),
        "CONNECTOR_NOT_FOUND",
        "connector_not_found",
    );
    assert_classified_error(
        &command.query_error(
            "connector.models",
            json!({"scopeId":"desktop", "connectorId":"disabled-fixture"}),
        ),
        "CONNECTOR_DISABLED",
        "connector_disabled",
    );
    assert_classified_error(
        &command.query_error(
            "connector.models",
            json!({"scopeId":"desktop", "connectorId":"unavailable-fixture"}),
        ),
        "CONNECTOR_RUNTIME_UNAVAILABLE",
        "connector_runtime_unavailable",
    );

    let project_id = "dual-runtime-project";
    let conversation_id = "dual-runtime-conversation";
    let codex_agent_id = "dual-runtime-codex-agent";
    let kun_agent_id = "dual-runtime-kun-agent";
    command.command(
        "project.create",
        json!({
            "projectId": project_id,
            "name": "Dual Runtime Pipe Acceptance",
            "rootPath": workspace_root,
        }),
    );
    command.command(
        "conversation.create",
        json!({
            "conversationId": conversation_id,
            "projectId": project_id,
            "title": "Offline dual-runtime conversation",
        }),
    );
    for (agent_id, name) in [(codex_agent_id, "Codex agent"), (kun_agent_id, "Kun agent")] {
        command.command(
            "agent.create",
            json!({
                "agentId": agent_id,
                "name": name,
                "role": "builder",
                "specialty": "connector-routing",
                "systemPrompt": "offline fixture only",
            }),
        );
        command.command(
            "project_agent.set",
            json!({
                "projectId": project_id,
                "agentId": agent_id,
                "enabled": true,
                "workspaceAccess": "none",
            }),
        );
    }
    // The registry's default adapter is Codex, but it is not itself a
    // persisted Connector profile. An explicit request for that ID must fail
    // closed rather than silently dispatch to the default adapter.
    let default_adapter_escape_run = "dual-runtime-default-adapter-without-profile";
    assert_classified_error(
        &command.command_error(
            "execution.start",
            json!({
                "executionRunId": default_adapter_escape_run,
                "collaborationRunId": "dual-runtime-collaboration",
                "projectId": project_id,
                "conversationId": conversation_id,
                "agentId": codex_agent_id,
                "workspaceAccess": "none",
                "currentTask": "must not silently route an unknown Connector to default Codex",
                "connectorId": "codex",
                "modelId": "codex-model-a",
            }),
        ),
        "CONNECTOR_NOT_FOUND",
        "connector_not_found",
    );
    let after_default_adapter_rejection = command.query("projection.snapshot", json!({}));
    assert!(
        !after_default_adapter_rejection.payload["runs"]
            .as_array()
            .expect("projection runs")
            .iter()
            .any(|run| run["id"] == default_adapter_escape_run),
        "unknown explicit Connector must not create a Run through the default adapter"
    );
    command.command(
        "agent.model_binding.patch",
        json!({
            "agentId": codex_agent_id,
            "connectorId": "codex-fixture",
            "modelId": "codex-model-a",
            "candidateModelListRevision": 1,
        }),
    );
    command.command(
        "agent.model_binding.patch",
        json!({
            "agentId": kun_agent_id,
            "connectorId": "kun-fixture",
            "modelId": "kun-model-a",
            "candidateModelListRevision": 1,
        }),
    );

    // Subscribe only to events created after setup.  This makes the event
    // ordering assertion about actual Runtime workers rather than projection
    // setup noise.
    let head = command
        .query("events.replay", json!({"afterSequence": 0, "limit": 1}))
        .payload["headSequence"]
        .as_u64()
        .expect("event replay must return a numeric head sequence");
    let event_client = PipeClient::connect(
        &pipe,
        &credential,
        "connector-runtime-event-client",
        &session_id,
    );
    let mut subscription = EventSubscription::start(event_client, head);

    let codex_run = "dual-runtime-codex-run";
    let kun_run = "dual-runtime-kun-run";
    for (run_id, agent_id, task) in [
        (
            codex_run,
            codex_agent_id,
            "stream exact Codex fixture output",
        ),
        (kun_run, kun_agent_id, "stream exact Kun fixture output"),
    ] {
        let started = command.command(
            "execution.start",
            json!({
                "executionRunId": run_id,
                "collaborationRunId": "dual-runtime-collaboration",
                "projectId": project_id,
                "conversationId": conversation_id,
                "agentId": agent_id,
                "workspaceAccess": "none",
                "currentTask": task,
            }),
        );
        assert_eq!(started.payload["run"]["id"], run_id);
    }
    let (dual_events, runtime_order) = collect_routed_runs_until_terminal(
        &mut subscription,
        &[
            (
                codex_run,
                "codex-fixture",
                "codex",
                "codex-model-a",
                codex_catalog_revision,
            ),
            (
                kun_run,
                "kun-fixture",
                "kun",
                "kun-model-a",
                kun_catalog_revision,
            ),
        ],
    );
    let codex_events = dual_events.get(codex_run).expect("Codex run events");
    let kun_events = dual_events.get(kun_run).expect("Kun run events");
    for events in [codex_events, kun_events] {
        let names = event_names(events);
        assert!(names.contains(&"connector.started"));
        assert!(names.contains(&"runtime.started"));
        assert!(names.contains(&"execution.completed"));
    }
    assert_eq!(
        output_deltas(codex_events),
        vec!["codex:codex-model-a:delta-1", "codex:codex-model-a:delta-2"]
    );
    assert_eq!(
        output_deltas(kun_events),
        vec!["kun:kun-model-a:delta-1", "kun:kun-model-a:delta-2"]
    );
    assert!(
        runtime_order.windows(2).any(|pair| pair[0] != pair[1]),
        "fixture dual Runtime streams must interleave rather than serialize by run"
    );

    let cancel_run = "dual-runtime-cancel-run";
    command.command(
        "execution.start",
        json!({
            "executionRunId": cancel_run,
            "collaborationRunId": "dual-runtime-collaboration",
            "projectId": project_id,
            "conversationId": conversation_id,
            "agentId": codex_agent_id,
            "workspaceAccess": "none",
            "currentTask": "cancel after connector startup",
        }),
    );
    loop {
        let event = subscription.next_event(IPC_TIMEOUT);
        if event.execution_run_id.as_deref() == Some(cancel_run)
            && event.event == "connector.started"
        {
            assert_runtime_route(
                &event,
                "codex-fixture",
                "codex",
                "codex-model-a",
                codex_catalog_revision,
            );
            break;
        }
    }
    let cancelled = command.command("execution.cancel", json!({"executionRunId": cancel_run}));
    assert_eq!(cancelled.payload["cancelled"], true);
    let cancel_events = collect_one_routed_run_until_terminal(
        &mut subscription,
        cancel_run,
        "codex-fixture",
        "codex",
        "codex-model-a",
        codex_catalog_revision,
    );
    assert!(event_names(&cancel_events).contains(&"execution.cancelled"));

    // A frozen Codex snapshot must fail closed if the persisted profile is
    // changed to point at Kun before a normal Retry.  The command boundary is
    // deliberately asserted here, not inferred from an internal unit test.
    command.command(
        "connector.update",
        connector_profile("codex-fixture", "Offline Codex", "kun", true),
    );
    assert_classified_error(
        &command.command_error(
            "execution.retry",
            json!({
                "executionRunId": "dual-runtime-mismatch-retry",
                "sourceExecutionRunId": codex_run,
                "currentTask": "must reject frozen Codex route after runtime mismatch",
            }),
        ),
        "CONNECTOR_RUNTIME_MISMATCH",
        "connector_runtime_mismatch",
    );
    command.command(
        "connector.update",
        connector_profile("codex-fixture", "Offline Codex", "codex", true),
    );

    let retry_run = "dual-runtime-codex-retry";
    let retry = command.command(
        "execution.retry",
        json!({
            "executionRunId": retry_run,
            "sourceExecutionRunId": codex_run,
            "currentTask": "retry must retain frozen Codex route",
        }),
    );
    assert_eq!(retry.payload["run"]["id"], retry_run);
    assert_eq!(
        retry.payload["run"]["status"], "Pending",
        "multi-runtime Retry must return before its worker drives Runtime I/O"
    );
    assert_eq!(retry.payload["sourceExecutionRunId"], codex_run);
    let retry_events = collect_one_routed_run_until_terminal(
        &mut subscription,
        retry_run,
        "codex-fixture",
        "codex",
        "codex-model-a",
        codex_catalog_revision,
    );
    assert_eq!(
        output_deltas(&retry_events),
        vec!["codex:codex-model-a:delta-1", "codex:codex-model-a:delta-2"]
    );

    // rerun-current deliberately resolves the current binding, unlike Retry.
    command.command(
        "agent.model_binding.patch",
        json!({
            "agentId": codex_agent_id,
            "modelId": "codex-model-b",
        }),
    );
    let rerun_current = "dual-runtime-codex-rerun-current";
    let rerun = command.command(
        "execution.rerun_current",
        json!({
            "executionRunId": rerun_current,
            "sourceExecutionRunId": codex_run,
            "currentTask": "rerun against current Codex model binding",
        }),
    );
    assert_eq!(rerun.payload["run"]["id"], rerun_current);
    assert_eq!(
        rerun.payload["run"]["status"], "Pending",
        "multi-runtime rerun-current must return before its worker drives Runtime I/O"
    );
    let rerun_events = collect_one_routed_run_until_terminal(
        &mut subscription,
        rerun_current,
        "codex-fixture",
        "codex",
        "codex-model-b",
        codex_catalog_revision,
    );
    assert_eq!(
        output_deltas(&rerun_events),
        vec!["codex:codex-model-b:delta-1", "codex:codex-model-b:delta-2"]
    );

    // Read the isolated SQLite state directly.  These assertions prove the
    // connector/model route is frozen atomically and survives beyond event
    // delivery, including Context Manifest connector persistence.
    {
        let store = SqliteStore::open(&database).expect("open isolated acceptance SQLite");
        for (run_id, connector_id, runtime_type, model_id, catalog_revision) in [
            (
                codex_run,
                "codex-fixture",
                "codex",
                "codex-model-a",
                codex_catalog_revision,
            ),
            (
                kun_run,
                "kun-fixture",
                "kun",
                "kun-model-a",
                kun_catalog_revision,
            ),
            (
                cancel_run,
                "codex-fixture",
                "codex",
                "codex-model-a",
                codex_catalog_revision,
            ),
            (
                retry_run,
                "codex-fixture",
                "codex",
                "codex-model-a",
                codex_catalog_revision,
            ),
            (
                rerun_current,
                "codex-fixture",
                "codex",
                "codex-model-b",
                codex_catalog_revision,
            ),
        ] {
            assert_persisted_route(
                &store,
                run_id,
                connector_id,
                runtime_type,
                model_id,
                catalog_revision,
            );
        }
        let projection = store
            .projection_snapshot()
            .expect("read isolated SQLite projection");
        assert_context_manifest_route(&projection, codex_run, "codex-fixture", "codex-model-a");
        assert_context_manifest_route(&projection, kun_run, "kun-fixture", "kun-model-a");
        assert_context_manifest_route(&projection, rerun_current, "codex-fixture", "codex-model-b");
    }

    // Dropping the subscription lets its connection close before the owner
    // sends the authenticated normal shutdown command.  No external process
    // is touched, and `wait_for_clean_exit` proves the owned Core exited.
    drop(subscription);
    let shutdown = command.command("shutdown_owned", json!({}));
    assert_eq!(shutdown.payload["shutdownAccepted"], true);
    drop(command);
    let exit_status = core.wait_for_clean_exit();
    assert!(
        exit_status.success(),
        "owned Core must exit cleanly after shutdown_owned: {exit_status}"
    );
    fs::remove_dir_all(&state_root)
        .expect("remove isolated acceptance state after clean Core exit");
}

#[test]
fn real_named_pipe_local_discovery_is_safe_idempotent_and_non_persistent() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after Unix epoch")
        .as_nanos();
    let state_root = std::env::temp_dir().join(format!(
        "agenttalk-local-discovery-pipe-{}-{nonce}",
        std::process::id()
    ));
    let database = state_root.join("core.sqlite3");
    let artifact_root = state_root.join("artifacts");
    let codex_binary = state_root.join("codex-install").join("codex.exe");
    let kun_data_dir = state_root.join("kun-runtime");
    let kun_install_dir = state_root.join("kun-install");
    fs::create_dir_all(&artifact_root).expect("create isolated discovery artifacts");
    fs::create_dir_all(
        codex_binary
            .parent()
            .expect("Codex fixture binary must have a parent"),
    )
    .expect("create isolated Codex install");
    fs::write(&codex_binary, b"local discovery Codex fixture")
        .expect("write isolated Codex executable");
    let fixture = ProductionKunFixture::start(&kun_data_dir, &kun_install_dir);

    let pipe = format!(r"\\.\pipe\agenttalk-local-discovery-{nonce}");
    let credential = format!("fixture-local-discovery-credential-{}", "x".repeat(40));
    let session_id = format!("fixture-local-discovery-session-{nonce}");
    let mut core = OwnedCore::spawn_local_discovery_registry(
        &pipe,
        &database,
        &artifact_root,
        &codex_binary,
        &kun_data_dir,
        &kun_install_dir,
        &credential,
    );
    let mut client = PipeClient::connect(&pipe, &credential, "local-discovery-client", &session_id);

    let before = client.query("projection.snapshot", json!({}));
    let connectors = client.query("connector.discover", json!({}));
    let agents = client.query("agent.scan_local", json!({}));
    assert_eq!(
        connectors.payload, agents.payload,
        "agent.scan_local must be a read-only presentation alias"
    );
    let discoveries = connectors.payload["discoveries"]
        .as_array()
        .expect("discovery payload must be an array");
    assert_eq!(discoveries.len(), 2);
    let expected_fields = BTreeSet::from([
        "availability",
        "catalogRevision",
        "connectorId",
        "displayName",
        "models",
        "requiresConfiguration",
        "runtimeType",
        "source",
    ]);
    for discovery in discoveries {
        assert_eq!(
            discovery
                .as_object()
                .expect("discovery entry must be an object")
                .keys()
                .map(String::as_str)
                .collect::<BTreeSet<_>>(),
            expected_fields,
            "discovery IPC entry must retain the strict safe allowlist"
        );
    }
    let codex = discoveries
        .iter()
        .find(|entry| entry["connectorId"] == "local.codex")
        .expect("Codex fixture discovery");
    assert_eq!(codex["runtimeType"], "codex");
    assert_eq!(codex["availability"], "unconfigured");
    assert_eq!(codex["models"], json!([]));
    assert_eq!(codex["requiresConfiguration"], true);
    assert_eq!(codex["source"], "executable_inventory");

    let kun = discoveries
        .iter()
        .find(|entry| entry["connectorId"] == "local.kun.shared-runtime")
        .expect("Kun fixture discovery");
    assert_eq!(kun["runtimeType"], "kun");
    assert_eq!(kun["availability"], "unconfigured");
    assert_eq!(kun["models"], json!([]));
    assert_eq!(kun["catalogRevision"], serde_json::Value::Null);
    assert_eq!(kun["requiresConfiguration"], true);
    assert_eq!(kun["source"], "runtime_record");

    let serialized = serde_json::to_string(&connectors.payload)
        .expect("serialize local discovery response")
        .to_ascii_lowercase();
    for forbidden in [
        fixture.token.as_str(),
        "authorization",
        "cookie",
        "api_key",
        "apikey",
        "secret",
        "bearer",
    ] {
        assert!(
            !serialized.contains(&forbidden.to_ascii_lowercase()),
            "local discovery response leaked credential-like material: {forbidden}"
        );
    }
    assert!(fixture.all_authenticated());
    assert_eq!(fixture.request_count("/v1/runtime/info"), 0);
    assert_eq!(fixture.request_count("/v1/model-connections"), 0);

    let invalid = client.query_error("connector.discover", json!({"unexpected": true}));
    assert_eq!(invalid.code, "INVALID_QUERY");
    let after = client.query("projection.snapshot", json!({}));
    assert_eq!(
        before.payload, after.payload,
        "scan must not persist a projection"
    );

    let shutdown = client.command("shutdown_owned", json!({}));
    assert_eq!(shutdown.payload["shutdownAccepted"], true);
    drop(client);
    let exit_status = core.wait_for_clean_exit();
    assert!(exit_status.success(), "owned test Core must exit cleanly");
    drop(fixture);
    fs::remove_dir_all(&state_root).expect("remove isolated local discovery state");
}

#[test]
fn real_named_pipe_default_production_registry_reaches_kun_http_sse_fixture() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after Unix epoch")
        .as_nanos();
    let state_root = std::env::temp_dir().join(format!(
        "agenttalk-production-kun-pipe-{}-{nonce}",
        std::process::id()
    ));
    let database = state_root.join("core.sqlite3");
    let artifact_root = state_root.join("artifacts");
    let workspace_root = state_root.join("workspace");
    let kun_data_dir = state_root.join("kun-runtime");
    let kun_install_dir = state_root.join("kun-install");
    fs::create_dir_all(&artifact_root).expect("create isolated production-Kun artifacts");
    fs::create_dir_all(&workspace_root).expect("create isolated production-Kun workspace");
    let fixture = ProductionKunFixture::start(&kun_data_dir, &kun_install_dir);
    assert!(
        !kun_data_dir.join("runtime-build.json").exists(),
        "production fixture must prove the dataDir-only layout no longer works"
    );

    let pipe = format!(r"\\.\pipe\agenttalk-production-kun-{nonce}");
    let credential = format!("fixture-production-kun-credential-{}", "x".repeat(40));
    let session_id = format!("fixture-production-kun-session-{nonce}");
    let mut core = OwnedCore::spawn_production_kun_registry(
        &pipe,
        &database,
        &artifact_root,
        &kun_data_dir,
        &kun_install_dir,
        &credential,
    );
    let mut command = PipeClient::connect(
        &pipe,
        &credential,
        "production-kun-command-client",
        &session_id,
    );

    let legacy_models = command.query("runtime.models", json!({}));
    assert_eq!(legacy_models.payload["runtimeId"], "unconfigured");
    assert_eq!(legacy_models.payload["models"], json!([]));

    command.command(
        "connector.create",
        connector_profile("kun-pipe-profile", "Production Kun fixture", "kun", true),
    );
    let catalog = command.query(
        "connector.models",
        json!({"scopeId":"desktop", "connectorId":"kun-pipe-profile"}),
    );
    assert_eq!(catalog.payload["runtimeType"], "kun");
    assert_eq!(catalog.payload["catalogRevision"], 42);
    assert_eq!(catalog.payload["defaultModelId"], "kun-pipe-model-b");
    assert_eq!(
        catalog.payload["models"],
        json!(["kun-pipe-model-a", "kun-pipe-model-b"])
    );
    let metadata_a = catalog.payload["modelMetadata"]
        .as_array()
        .expect("catalog model metadata")
        .iter()
        .find(|item| item["modelId"] == "kun-pipe-model-a")
        .expect("model a metadata");
    assert_eq!(
        metadata_a
            .as_object()
            .expect("catalog metadata must remain an object")
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["availability", "capabilities", "modelId"]),
        "connector.models must remain compatible with the frozen IPC v1 schema"
    );
    assert_eq!(metadata_a["availability"], "available");
    assert_eq!(metadata_a["capabilities"]["filesystem"], false);
    assert!(!serde_json::to_string(&catalog)
        .expect("serialize production-Kun catalog response")
        .contains(&fixture.token));

    let project_id = "production-kun-project";
    let conversation_id = "production-kun-conversation";
    let agent_id = "production-kun-agent";
    command.command(
        "project.create",
        json!({"projectId": project_id, "name": "Production Kun Pipe", "rootPath": workspace_root}),
    );
    command.command(
        "workspace.authorize",
        json!({"projectId": project_id, "rootPath": workspace_root}),
    );
    command.command(
        "conversation.create",
        json!({"conversationId": conversation_id, "projectId": project_id, "title": "Production Kun fixture"}),
    );
    command.command(
        "agent.create",
        json!({
            "agentId": agent_id,
            "name": "Production Kun agent",
            "role": "builder",
            "specialty": "production-transport-fixture",
            "systemPrompt": "local fixture only",
        }),
    );
    command.command(
        "project_agent.set",
        json!({"projectId": project_id, "agentId": agent_id, "enabled": true, "workspaceAccess": "read_only"}),
    );
    command.command(
        "agent.model_binding.patch",
        json!({
            "agentId": agent_id,
            "connectorId": "kun-pipe-profile",
        }),
    );

    let head = command
        .query("events.replay", json!({"afterSequence": 0, "limit": 1}))
        .payload["headSequence"]
        .as_u64()
        .expect("event replay head sequence");
    let event_client = PipeClient::connect(
        &pipe,
        &credential,
        "production-kun-event-client",
        &session_id,
    );
    let mut subscription = EventSubscription::start(event_client, head);
    let run_id = "production-kun-run";
    let started = command.command(
        "execution.start",
        json!({
            "executionRunId": run_id,
            "collaborationRunId": "production-kun-collaboration",
            "projectId": project_id,
            "conversationId": conversation_id,
            "agentId": agent_id,
            "workspaceAccess": "read_only",
            "canonicalCwd": workspace_root,
            "currentTask": "exercise default production Kun HTTP and SSE transport",
        }),
    );
    assert_eq!(started.payload["run"]["id"], run_id);
    let events = collect_one_routed_run_until_terminal(
        &mut subscription,
        run_id,
        "kun-pipe-profile",
        "kun",
        "kun-pipe-model-b",
        42,
    );
    assert_eq!(
        output_deltas(&events),
        vec!["pipe production Kun delta"],
        "production Kun routed events: {events:#?}"
    );
    assert!(event_names(&events).contains(&"execution.completed"));
    assert!(fixture.all_authenticated());
    assert!(fixture.saw_body(
        "/v1/threads",
        &json!({
            "title": "AgentTalk",
            "titleAuto": true,
            "workspace": workspace_root,
            "model": "kun-pipe-model-b",
            "mode": "agent",
            "approvalPolicy": "auto",
            "sandboxMode": "read-only",
        }),
    ));
    assert!(fixture.saw_body(
        "/v1/threads/pipe-thread/turns",
        &json!({
            "prompt": "[current_task]\nexercise default production Kun HTTP and SSE transport",
            "model": "kun-pipe-model-b",
            "mode": "agent",
            "disableUserInput": true,
        }),
    ));
    {
        let store = SqliteStore::open(&database).expect("open isolated production-Kun SQLite");
        assert_persisted_route(
            &store,
            run_id,
            "kun-pipe-profile",
            "kun",
            "kun-pipe-model-b",
            42,
        );
        let projection = store
            .projection_snapshot()
            .expect("read production-Kun projection");
        assert_context_manifest_route(&projection, run_id, "kun-pipe-profile", "kun-pipe-model-b");
    }

    for (sse_mode, split_run_id) in [
        (
            ProductionKunSseMode::ChineseSplitContentLength,
            "production-kun-utf8-content-length",
        ),
        (
            ProductionKunSseMode::ChineseSplitChunked,
            "production-kun-utf8-chunked",
        ),
    ] {
        fixture.configure(ProductionKunFixtureConfig {
            sse_mode,
            ..ProductionKunFixtureConfig::default()
        });
        command.command(
            "execution.start",
            json!({
                "executionRunId": split_run_id,
                "collaborationRunId": "production-kun-collaboration",
                "projectId": project_id,
                "conversationId": conversation_id,
                "agentId": agent_id,
                "workspaceAccess": "read_only",
                "canonicalCwd": workspace_root,
                "currentTask": "exercise UTF-8 split production Kun SSE",
            }),
        );
        let split_events = collect_one_routed_run_until_terminal(
            &mut subscription,
            split_run_id,
            "kun-pipe-profile",
            "kun",
            "kun-pipe-model-b",
            42,
        );
        assert_eq!(output_deltas(&split_events).concat(), "你好，世界");
        assert_eq!(
            split_events
                .iter()
                .filter(|event| event.event == "execution.completed")
                .count(),
            1,
            "{split_run_id} must have one completed terminal"
        );
    }

    fixture.configure(ProductionKunFixtureConfig {
        sse_mode: ProductionKunSseMode::NoDeltaCompletion,
        ..ProductionKunFixtureConfig::default()
    });
    let no_delta_run = "production-kun-no-delta";
    command.command(
        "execution.start",
        json!({
            "executionRunId": no_delta_run,
            "collaborationRunId": "production-kun-collaboration",
            "projectId": project_id,
            "conversationId": conversation_id,
            "agentId": agent_id,
            "workspaceAccess": "read_only",
            "canonicalCwd": workspace_root,
            "currentTask": "exercise production Kun no-delta recovery",
        }),
    );
    let no_delta_events = collect_one_routed_run_until_terminal(
        &mut subscription,
        no_delta_run,
        "kun-pipe-profile",
        "kun",
        "kun-pipe-model-b",
        42,
    );
    assert_eq!(
        output_deltas(&no_delta_events),
        vec!["recovered production Kun delta"]
    );

    fixture.configure(ProductionKunFixtureConfig {
        runtime_info_status: 401,
        ..ProductionKunFixtureConfig::default()
    });
    assert_classified_error(
        &command.query_error(
            "connector.models",
            json!({"scopeId":"desktop", "connectorId":"kun-pipe-profile"}),
        ),
        "CONNECTOR_RUNTIME_AUTHENTICATION_FAILED",
        "runtime_authentication_failed",
    );
    fixture.configure(ProductionKunFixtureConfig {
        runtime_info_identity_mismatch: true,
        ..ProductionKunFixtureConfig::default()
    });
    assert_classified_error(
        &command.query_error(
            "connector.models",
            json!({"scopeId":"desktop", "connectorId":"kun-pipe-profile"}),
        ),
        "CONNECTOR_RUNTIME_IDENTITY_MISMATCH",
        "runtime_identity_mismatch",
    );
    fixture.configure(ProductionKunFixtureConfig::default());

    let runtime_info_path = "/v1/runtime/info";
    let runtime_info_before = fixture.request_count(runtime_info_path);
    fixture.write_runtime_rendezvous("127.example.com");
    assert_classified_error(
        &command.query_error(
            "connector.models",
            json!({"scopeId":"desktop", "connectorId":"kun-pipe-profile"}),
        ),
        "CONNECTOR_RUNTIME_IDENTITY_MISMATCH",
        "runtime_identity_mismatch",
    );
    assert_eq!(
        fixture.request_count(runtime_info_path),
        runtime_info_before,
        "hostile rendezvous must be rejected before sending the runtime token"
    );
    fixture.write_runtime_rendezvous("127.0.0.1");

    fixture.configure(ProductionKunFixtureConfig {
        provider_auth_on_turn: true,
        ..ProductionKunFixtureConfig::default()
    });
    let provider_auth_run = "production-kun-provider-auth";
    command.command(
        "execution.start",
        json!({
            "executionRunId": provider_auth_run,
            "collaborationRunId": "production-kun-collaboration",
            "projectId": project_id,
            "conversationId": conversation_id,
            "agentId": agent_id,
            "workspaceAccess": "read_only",
            "canonicalCwd": workspace_root,
            "currentTask": "exercise production Kun Provider authentication failure",
        }),
    );
    let provider_auth_events = collect_one_routed_run_until_terminal(
        &mut subscription,
        provider_auth_run,
        "kun-pipe-profile",
        "kun",
        "kun-pipe-model-b",
        42,
    );
    assert_eq!(
        replay_run_terminal_events(&mut command, provider_auth_run),
        vec!["execution.failed"]
    );
    assert!(
        event_names(&provider_auth_events).contains(&"execution.failed"),
        "Provider auth must be a classified failed terminal"
    );
    fixture.configure(ProductionKunFixtureConfig::default());

    let interrupt_path = "/v1/threads/pipe-thread/turns/pipe-turn/interrupt";
    fixture.configure(ProductionKunFixtureConfig {
        sse_mode: ProductionKunSseMode::Hold,
        ..ProductionKunFixtureConfig::default()
    });
    let interrupt_success_run = "production-kun-interrupt-success";
    command.command(
        "execution.start",
        json!({
            "executionRunId": interrupt_success_run,
            "collaborationRunId": "production-kun-collaboration",
            "projectId": project_id,
            "conversationId": conversation_id,
            "agentId": agent_id,
            "workspaceAccess": "read_only",
            "canonicalCwd": workspace_root,
            "currentTask": "exercise production Kun interrupt success",
        }),
    );
    wait_for_routed_event(
        &mut subscription,
        interrupt_success_run,
        "kun-pipe-profile",
        "kun",
        "kun-pipe-model-b",
        42,
        "runtime.started",
    );
    let interrupt_before = fixture.request_count(interrupt_path);
    let cancelled = command.command(
        "execution.cancel",
        json!({"executionRunId": interrupt_success_run}),
    );
    assert_eq!(cancelled.payload["cancelled"], true);
    let interrupt_success_events = collect_one_routed_run_until_terminal(
        &mut subscription,
        interrupt_success_run,
        "kun-pipe-profile",
        "kun",
        "kun-pipe-model-b",
        42,
    );
    assert!(event_names(&interrupt_success_events).contains(&"execution.cancelled"));
    let repeated = command.command(
        "execution.cancel",
        json!({"executionRunId": interrupt_success_run}),
    );
    assert_eq!(repeated.payload["cancelled"], true);
    assert_eq!(fixture.request_count(interrupt_path), interrupt_before + 1);
    assert_eq!(
        replay_run_terminal_events(&mut command, interrupt_success_run),
        vec!["execution.cancelled"]
    );

    fixture.configure(ProductionKunFixtureConfig {
        sse_mode: ProductionKunSseMode::Hold,
        interrupt_status: 401,
        ..ProductionKunFixtureConfig::default()
    });
    let interrupt_401_run = "production-kun-interrupt-401";
    command.command(
        "execution.start",
        json!({
            "executionRunId": interrupt_401_run,
            "collaborationRunId": "production-kun-collaboration",
            "projectId": project_id,
            "conversationId": conversation_id,
            "agentId": agent_id,
            "workspaceAccess": "read_only",
            "canonicalCwd": workspace_root,
            "currentTask": "exercise production Kun interrupt authentication failure",
        }),
    );
    wait_for_routed_event(
        &mut subscription,
        interrupt_401_run,
        "kun-pipe-profile",
        "kun",
        "kun-pipe-model-b",
        42,
        "runtime.started",
    );
    let interrupt_before = fixture.request_count(interrupt_path);
    assert_classified_error(
        &command.command_error(
            "execution.cancel",
            json!({"executionRunId": interrupt_401_run}),
        ),
        "CONNECTOR_RUNTIME_AUTHENTICATION_FAILED",
        "runtime_authentication_failed",
    );
    let interrupt_401_events = collect_one_routed_run_until_terminal(
        &mut subscription,
        interrupt_401_run,
        "kun-pipe-profile",
        "kun",
        "kun-pipe-model-b",
        42,
    );
    assert!(event_names(&interrupt_401_events).contains(&"execution.failed"));
    assert!(!event_names(&interrupt_401_events).contains(&"execution.cancelled"));
    assert_eq!(fixture.request_count(interrupt_path), interrupt_before + 1);
    assert!(
        fixture.turn_is_active("pipe-turn"),
        "a rejected interrupt must not claim that the remote fixture turn was cancelled"
    );
    assert_eq!(
        replay_run_terminal_events(&mut command, interrupt_401_run),
        vec!["execution.failed"]
    );

    fixture.configure(ProductionKunFixtureConfig {
        delay_turn_post: true,
        ..ProductionKunFixtureConfig::default()
    });
    let turn_post_run = "production-kun-turn-post-cancel";
    let turn_path = "/v1/threads/pipe-thread/turns";
    let turns_before = fixture.request_count(turn_path);
    let interrupts_before = fixture.request_count(interrupt_path);
    command.command(
        "execution.start",
        json!({
            "executionRunId": turn_post_run,
            "collaborationRunId": "production-kun-collaboration",
            "projectId": project_id,
            "conversationId": conversation_id,
            "agentId": agent_id,
            "workspaceAccess": "read_only",
            "canonicalCwd": workspace_root,
            "currentTask": "cancel while production Kun turn POST is in flight",
        }),
    );
    fixture.wait_for_request_count(turn_path, turns_before + 1, IPC_TIMEOUT);
    let cancelled = command.command("execution.cancel", json!({"executionRunId": turn_post_run}));
    assert_eq!(cancelled.payload["cancelled"], true);
    let turn_post_events = collect_one_routed_run_until_terminal(
        &mut subscription,
        turn_post_run,
        "kun-pipe-profile",
        "kun",
        "kun-pipe-model-b",
        42,
    );
    assert!(event_names(&turn_post_events).contains(&"execution.cancelled"));
    fixture.wait_for_request_count(interrupt_path, interrupts_before + 1, IPC_TIMEOUT);
    assert_eq!(fixture.request_count(interrupt_path), interrupts_before + 1);
    assert!(
        !fixture.turn_is_active("pipe-turn"),
        "a successful post-in-flight interrupt must leave no active remote fixture turn"
    );
    assert_eq!(
        replay_run_terminal_events(&mut command, turn_post_run),
        vec!["execution.cancelled"]
    );

    fixture.configure(ProductionKunFixtureConfig {
        sse_mode: ProductionKunSseMode::DelayedCompletion,
        ..ProductionKunFixtureConfig::default()
    });
    let completion_race_run = "production-kun-completion-cancel-race";
    command.command(
        "execution.start",
        json!({
            "executionRunId": completion_race_run,
            "collaborationRunId": "production-kun-collaboration",
            "projectId": project_id,
            "conversationId": conversation_id,
            "agentId": agent_id,
            "workspaceAccess": "read_only",
            "canonicalCwd": workspace_root,
            "currentTask": "race production Kun completion against cancel",
        }),
    );
    wait_for_routed_event(
        &mut subscription,
        completion_race_run,
        "kun-pipe-profile",
        "kun",
        "kun-pipe-model-b",
        42,
        "runtime.started",
    );
    match command.command_result(
        "execution.cancel",
        json!({"executionRunId": completion_race_run}),
    ) {
        Ok(response) => assert_eq!(response.payload["cancelled"], true),
        Err(error) => assert_eq!(error.code, "COMMAND_REJECTED"),
    }
    let _ = collect_one_routed_run_until_terminal(
        &mut subscription,
        completion_race_run,
        "kun-pipe-profile",
        "kun",
        "kun-pipe-model-b",
        42,
    );
    let race_terminal = replay_run_terminal_events(&mut command, completion_race_run);
    assert_eq!(
        race_terminal.len(),
        1,
        "completion/cancel race must persist one terminal"
    );
    assert!(
        matches!(
            race_terminal[0].as_str(),
            "execution.completed" | "execution.cancelled"
        ),
        "completion/cancel race must retain a legal terminal: {race_terminal:?}"
    );

    drop(subscription);
    let shutdown = command.command("shutdown_owned", json!({}));
    assert_eq!(shutdown.payload["shutdownAccepted"], true);
    drop(command);
    let exit = core.wait_for_clean_exit();
    assert!(
        exit.success(),
        "production-Kun Core must exit cleanly: {exit}"
    );
    drop(fixture);
    fs::remove_dir_all(&state_root)
        .expect("remove isolated production-Kun Pipe state after clean exit");
}

#[test]
fn real_named_pipe_default_production_registry_is_fail_closed_but_recognizes_builtins() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after Unix epoch")
        .as_nanos();
    let state_root = std::env::temp_dir().join(format!(
        "agenttalk-production-registry-pipe-{}-{nonce}",
        std::process::id()
    ));
    let database = state_root.join("core.sqlite3");
    let artifact_root = state_root.join("artifacts");
    fs::create_dir_all(&artifact_root).expect("create isolated production-registry artifacts");

    let pipe = format!(r"\\.\pipe\agenttalk-production-registry-{nonce}");
    let credential = format!("fixture-production-registry-credential-{}", "x".repeat(40));
    let session_id = format!("fixture-production-registry-session-{nonce}");
    let mut core =
        OwnedCore::spawn_inert_production_registry(&pipe, &database, &artifact_root, &credential);
    let mut command = PipeClient::connect(
        &pipe,
        &credential,
        "production-registry-command-client",
        &session_id,
    );

    let legacy_models = command.query("runtime.models", json!({}));
    assert_eq!(legacy_models.payload["runtimeId"], "unconfigured");
    assert_eq!(legacy_models.payload["availability"], "unavailable");
    assert_eq!(legacy_models.payload["models"], json!([]));

    for profile in [
        connector_profile("desktop-codex-profile", "Codex", "codex", true),
        connector_profile("desktop-kun-profile", "Kun", "kun", true),
    ] {
        let created = command.command("connector.create", profile);
        assert_eq!(created.payload["created"], true);
    }

    // No real executable, shared runtime, provider, or credential is
    // available to this child. The additive query must preserve the exact
    // safe local failure class rather than fabricate an empty successful
    // catalog, fall back to `unconfigured`, or attempt an implicit Mock.
    for (connector_id, expected_code, expected_category) in [
        (
            "desktop-codex-profile",
            "CONNECTOR_RUNTIME_UNAVAILABLE",
            "connector_runtime_unavailable",
        ),
        (
            "desktop-kun-profile",
            "CONNECTOR_SHARED_RUNTIME_UNAVAILABLE",
            "shared_runtime_unavailable",
        ),
    ] {
        let error = command.query_error(
            "connector.models",
            json!({"scopeId":"desktop", "connectorId": connector_id}),
        );
        assert_classified_error(&error, expected_code, expected_category);
        let serialized = serde_json::to_string(&error)
            .expect("serialize classified inert-production error")
            .to_ascii_lowercase();
        for forbidden in ["credential", "token", "authorization", "cookie"] {
            assert!(
                !serialized.contains(forbidden),
                "inert production error leaked fixture material: {forbidden}"
            );
        }
    }

    let shutdown = command.command("shutdown_owned", json!({}));
    assert_eq!(shutdown.payload["shutdownAccepted"], true);
    drop(command);
    let exit = core.wait_for_clean_exit();
    assert!(
        exit.success(),
        "inert production-registry Core must exit cleanly: {exit}"
    );
    fs::remove_dir_all(&state_root)
        .expect("remove isolated production-registry state after clean Core exit");
}

#[test]
fn real_named_pipe_model_binding_set_and_patch_preserve_compatibility_after_restart() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after Unix epoch")
        .as_nanos();
    let state_root = std::env::temp_dir().join(format!(
        "agenttalk-model-binding-compat-pipe-{}-{nonce}",
        std::process::id()
    ));
    let database = state_root.join("core.sqlite3");
    let artifact_root = state_root.join("artifacts");
    fs::create_dir_all(&artifact_root).expect("create isolated artifact root");

    let credential = format!("fixture-binding-session-credential-{}", "x".repeat(40));
    let agent_id = "binding-compat-agent";
    let pipe = format!(r"\\.\pipe\agenttalk-model-binding-compat-{nonce}");
    let session_id = format!("fixture-binding-session-{nonce}");
    let mut core = OwnedCore::spawn(&pipe, &database, &artifact_root, &credential);
    let mut command = PipeClient::connect(
        &pipe,
        &credential,
        "model-binding-compat-command-client",
        &session_id,
    );
    command.command(
        "agent.create",
        json!({
            "agentId": agent_id,
            "name": "Binding compatibility agent",
            "role": "builder",
            "specialty": "ipc compatibility",
            "systemPrompt": "isolated fixture only",
        }),
    );

    // The legacy command always replaces the complete binding.
    let complete_binding = command.command(
        "agent.model_binding.set",
        json!({
            "agentId": agent_id,
            "connectorId": "codex-fixture",
            "modelId": "codex-model-a",
            "candidateModelListRevision": 7,
        }),
    );
    assert_projection_agent_binding(
        &complete_binding.payload["projection"],
        agent_id,
        Some("codex-fixture"),
        Some("codex-model-a"),
        7,
    );
    let omitted_replacement =
        command.command("agent.model_binding.set", json!({"agentId": agent_id}));
    assert_projection_agent_binding(
        &omitted_replacement.payload["projection"],
        agent_id,
        None,
        None,
        0,
    );

    let rebound = command.command(
        "agent.model_binding.set",
        json!({
            "agentId": agent_id,
            "connectorId": "codex-fixture",
            "modelId": "codex-model-a",
            "candidateModelListRevision": 7,
        }),
    );
    assert_projection_agent_binding(
        &rebound.payload["projection"],
        agent_id,
        Some("codex-fixture"),
        Some("codex-model-a"),
        7,
    );

    // The additive command alone retains omitted fields.
    let preserved = command.command(
        "agent.model_binding.patch",
        json!({
            "agentId": agent_id,
            "modelId": "codex-model-b",
        }),
    );
    assert_projection_agent_binding(
        &preserved.payload["projection"],
        agent_id,
        Some("codex-fixture"),
        Some("codex-model-b"),
        7,
    );
    let null_cleared = command.command(
        "agent.model_binding.patch",
        json!({
            "agentId": agent_id,
            "modelId": null,
            "candidateModelListRevision": null,
        }),
    );
    assert_projection_agent_binding(
        &null_cleared.payload["projection"],
        agent_id,
        Some("codex-fixture"),
        None,
        0,
    );
    command.command(
        "agent.model_binding.patch",
        json!({
            "agentId": agent_id,
            "modelId": "codex-model-a",
            "candidateModelListRevision": 8,
        }),
    );
    let connector_cleared = command.command(
        "agent.model_binding.patch",
        json!({
            "agentId": agent_id,
            "connectorId": null,
        }),
    );
    assert_projection_agent_binding(
        &connector_cleared.payload["projection"],
        agent_id,
        None,
        None,
        8,
    );

    let orphan_patch = command.command_error(
        "agent.model_binding.patch",
        json!({
            "agentId": agent_id,
            "modelId": "orphan-model",
        }),
    );
    assert_eq!(orphan_patch.code, "COMMAND_REJECTED");
    assert!(orphan_patch.message.contains("requires a connector"));
    let orphan_set = command.command_error(
        "agent.model_binding.set",
        json!({
            "agentId": agent_id,
            "modelId": "orphan-model",
        }),
    );
    assert_eq!(orphan_set.code, "INVALID_COMMAND");
    assert!(orphan_set.message.contains("modelId requires connectorId"));
    let after_orphan_rejection = command.query("projection.snapshot", json!({}));
    assert_projection_agent_binding(&after_orphan_rejection.payload, agent_id, None, None, 8);

    // `null` has the same clear meaning as an omitted legacy set field.
    command.command(
        "agent.model_binding.set",
        json!({
            "agentId": agent_id,
            "connectorId": "kun-fixture",
            "modelId": "kun-model-a",
            "candidateModelListRevision": 12,
        }),
    );
    let null_replacement = command.command(
        "agent.model_binding.set",
        json!({
            "agentId": agent_id,
            "connectorId": null,
            "modelId": null,
        }),
    );
    assert_projection_agent_binding(
        &null_replacement.payload["projection"],
        agent_id,
        None,
        None,
        0,
    );

    let final_binding = command.command(
        "agent.model_binding.set",
        json!({
            "agentId": agent_id,
            "connectorId": "kun-fixture",
            "modelId": "kun-model-b",
            "candidateModelListRevision": 13,
        }),
    );
    assert_projection_agent_binding(
        &final_binding.payload["projection"],
        agent_id,
        Some("kun-fixture"),
        Some("kun-model-b"),
        13,
    );

    let shutdown = command.command("shutdown_owned", json!({}));
    assert_eq!(shutdown.payload["shutdownAccepted"], true);
    drop(command);
    let first_exit = core.wait_for_clean_exit();
    assert!(
        first_exit.success(),
        "first owned Core must exit cleanly after shutdown_owned: {first_exit}"
    );

    // Open the isolated database only after the owned Core exits, then reopen
    // it to prove the complete binding was committed rather than just held in
    // the in-memory projection.
    {
        let store = SqliteStore::open(&database).expect("open isolated binding SQLite");
        assert_persisted_agent_binding(
            &store,
            agent_id,
            Some("kun-fixture"),
            Some("kun-model-b"),
            13,
        );
    }
    {
        let reopened = SqliteStore::open(&database).expect("reopen isolated binding SQLite");
        assert_persisted_agent_binding(
            &reopened,
            agent_id,
            Some("kun-fixture"),
            Some("kun-model-b"),
            13,
        );
    }

    let restart_pipe = format!(r"\\.\pipe\agenttalk-model-binding-compat-restart-{nonce}");
    let restart_session_id = format!("fixture-binding-restart-session-{nonce}");
    let mut restarted_core =
        OwnedCore::spawn(&restart_pipe, &database, &artifact_root, &credential);
    let mut restarted_command = PipeClient::connect(
        &restart_pipe,
        &credential,
        "model-binding-compat-restart-client",
        &restart_session_id,
    );
    let restarted_projection = restarted_command.query("projection.snapshot", json!({}));
    assert_projection_agent_binding(
        &restarted_projection.payload,
        agent_id,
        Some("kun-fixture"),
        Some("kun-model-b"),
        13,
    );
    let restarted_shutdown = restarted_command.command("shutdown_owned", json!({}));
    assert_eq!(restarted_shutdown.payload["shutdownAccepted"], true);
    drop(restarted_command);
    let restart_exit = restarted_core.wait_for_clean_exit();
    assert!(
        restart_exit.success(),
        "restarted owned Core must exit cleanly after shutdown_owned: {restart_exit}"
    );

    let final_reopen = SqliteStore::open(&database).expect("final reopen isolated binding SQLite");
    assert_persisted_agent_binding(
        &final_reopen,
        agent_id,
        Some("kun-fixture"),
        Some("kun-model-b"),
        13,
    );
    drop(final_reopen);
    fs::remove_dir_all(&state_root)
        .expect("remove isolated binding compatibility state after clean Core exits");
}
