use std::collections::HashMap;
use std::ffi::OsString;
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use agenttalk_domain::WorkspaceAccess;
use serde_json::{json, Value};

use crate::{
    bounded_request_timeout, connector_started_event, has_reparse_point, is_real_regular_file,
    jsonrpc_response_id, output_delta_event, rpc_response_result, runtime_started_event,
    terminal_event, RuntimeAdapter, RuntimeCapabilities, RuntimeDiscovery, RuntimeError,
    RuntimeEvent, RuntimeEventStream, RuntimeHealth, RuntimeRequest,
    DEFAULT_RUNTIME_STREAM_CAPACITY, MAX_RUNTIME_TIMEOUT_MS,
};

use super::{
    Integration, IntegrationConnectError, IntegrationDescriptor, IntegrationDetectOutcome,
    IntegrationInstalled, IntegrationLoginState, IntegrationVerification,
    IntegrationVerificationStatus, INTEGRATION_PROBE_TIMEOUT,
};

const CLAUDE_RUNTIME_UNAVAILABLE: &str = "claude_runtime_unavailable";
const CLAUDE_PROTOCOL_ERROR: &str = "claude_protocol_error";
const MAX_CLAUDE_LINE_BYTES: usize = 128 * 1024;
const MAX_CLAUDE_STDERR_BYTES: usize = 16 * 1024;
const CLAUDE_SETUP_TIMEOUT: Duration = Duration::from_secs(5);
const CLAUDE_CLEANUP_GRACE: Duration = Duration::from_millis(1_500);

pub struct ClaudeCodeIntegration;

fn descriptor() -> &'static IntegrationDescriptor {
    static DESCRIPTOR: OnceLock<IntegrationDescriptor> = OnceLock::new();
    DESCRIPTOR.get_or_init(|| IntegrationDescriptor {
        id: "local.claude-code".into(),
        display_name: "Claude Code".into(),
        category: "agent_runtime".into(),
        protocol: "acp".into(),
        runtime_type: "claude-code".into(),
        install_command: "npm install -g @anthropic-ai/claude-code".into(),
        needs_consent: true,
    })
}

impl Integration for ClaudeCodeIntegration {
    fn descriptor(&self) -> &IntegrationDescriptor {
        descriptor()
    }

    fn detect(&self) -> IntegrationDetectOutcome {
        let Some(executable) = find_claude_executable() else {
            return IntegrationDetectOutcome::NotInstalled;
        };
        let Some(version) = probe_claude_version(&executable) else {
            return IntegrationDetectOutcome::NotInstalled;
        };
        IntegrationDetectOutcome::Installed(IntegrationInstalled {
            version,
            login_state: probe_claude_login_state(&executable),
        })
    }

    fn verify(&self) -> IntegrationVerification {
        let installed = self.detect();
        let IntegrationDetectOutcome::Installed(installed) = installed else {
            return IntegrationVerification {
                integration_id: self.descriptor().id.clone(),
                status: IntegrationVerificationStatus::Rejected,
                login_state: IntegrationLoginState::Unknown,
                protocol_major: None,
                version: None,
                detail: Some("claude_cli_missing".into()),
            };
        };
        match self.connect() {
            Ok(runtime) => {
                let discovery = runtime.discover();
                IntegrationVerification {
                    integration_id: self.descriptor().id.clone(),
                    status: if installed.login_state == IntegrationLoginState::LoggedIn {
                        IntegrationVerificationStatus::Verified
                    } else {
                        IntegrationVerificationStatus::AuthRequired
                    },
                    login_state: installed.login_state,
                    protocol_major: Some(1),
                    version: discovery.version,
                    detail: Some("claude_acp_initialize_ok".into()),
                }
            }
            Err(_) => IntegrationVerification {
                integration_id: self.descriptor().id.clone(),
                status: IntegrationVerificationStatus::Rejected,
                login_state: installed.login_state,
                protocol_major: Some(1),
                version: None,
                detail: Some("claude_acp_initialize_failed".into()),
            },
        }
    }

    fn connect(&self) -> Result<Box<dyn RuntimeAdapter>, IntegrationConnectError> {
        let executable = find_claude_executable().ok_or(IntegrationConnectError::NotInstalled)?;
        let runtime = ClaudeCodeRuntime::with_config(ClaudeCodeConfig {
            binary_path: Some(executable),
            ..ClaudeCodeConfig::default()
        });
        runtime
            .ensure_available()
            .map_err(|_| IntegrationConnectError::ConnectFailed)?;
        Ok(Box::new(runtime))
    }
}

#[derive(Clone, Debug)]
pub struct ClaudeCodeConfig {
    pub binary_path: Option<PathBuf>,
    pub command_args: Vec<String>,
    pub isolated_cwd: Option<PathBuf>,
    pub request_timeout: Duration,
}

impl Default for ClaudeCodeConfig {
    fn default() -> Self {
        Self {
            binary_path: None,
            command_args: Vec::new(),
            isolated_cwd: None,
            request_timeout: Duration::from_secs(120),
        }
    }
}

impl ClaudeCodeConfig {
    fn effective_request_timeout(&self) -> Duration {
        if self.request_timeout.is_zero() {
            Duration::from_millis(120_000)
        } else {
            self.request_timeout
        }
    }
}

#[derive(Clone)]
pub struct ClaudeCodeRuntime {
    config: ClaudeCodeConfig,
    state: Arc<Mutex<ClaudeCodeState>>,
}

#[derive(Default)]
struct ClaudeCodeState {
    active_cancellations: HashMap<String, Arc<AtomicBool>>,
    active_runs: HashMap<String, ClaudeActiveRun>,
}

#[derive(Clone)]
struct ClaudeActiveRun {
    session: Arc<ClaudeAcpSession>,
    session_id: String,
}

impl ClaudeCodeRuntime {
    pub fn new() -> Self {
        Self::with_config(ClaudeCodeConfig::default())
    }

    pub fn with_config(config: ClaudeCodeConfig) -> Self {
        Self {
            config,
            state: Arc::new(Mutex::new(ClaudeCodeState::default())),
        }
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, ClaudeCodeState> {
        match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn begin_execution(&self, execution_run_id: &str) -> Result<Arc<AtomicBool>, RuntimeError> {
        let mut state = self.lock_state();
        if state.active_cancellations.contains_key(execution_run_id) {
            return Err(RuntimeError::Protocol(
                "claude_execution_already_active".into(),
            ));
        }
        let cancelled = Arc::new(AtomicBool::new(false));
        state
            .active_cancellations
            .insert(execution_run_id.to_owned(), Arc::clone(&cancelled));
        Ok(cancelled)
    }

    fn register_active_run(&self, execution_run_id: &str, active: ClaudeActiveRun) {
        self.lock_state()
            .active_runs
            .insert(execution_run_id.to_owned(), active);
    }

    fn finish_execution(&self, execution_run_id: &str) {
        let mut state = self.lock_state();
        state.active_cancellations.remove(execution_run_id);
        state.active_runs.remove(execution_run_id);
    }

    fn open_session_until(
        &self,
        deadline: crate::TransportDeadline,
        cwd: &Path,
    ) -> Result<(Arc<ClaudeAcpSession>, Option<String>), RuntimeError> {
        let executable = self.binary_path()?;
        open_claude_acp_session(&executable, &self.config.command_args, cwd, deadline)
    }

    fn binary_path(&self) -> Result<PathBuf, RuntimeError> {
        if let Some(path) = self.config.binary_path.as_deref() {
            if is_real_regular_file(path) {
                return Ok(path.to_path_buf());
            }
        }
        find_claude_executable()
            .ok_or_else(|| RuntimeError::Transport(CLAUDE_RUNTIME_UNAVAILABLE.into()))
    }

    fn execution_cwd(&self, request: &RuntimeRequest) -> Result<PathBuf, RuntimeError> {
        if !matches!(request.workspace_access, WorkspaceAccess::None) {
            let cwd = request
                .canonical_cwd
                .as_deref()
                .filter(|cwd| !cwd.trim().is_empty())
                .map(PathBuf::from)
                .ok_or(RuntimeError::InvalidWorkspace)?;
            if !cwd.is_absolute() || !cwd.is_dir() {
                return Err(RuntimeError::InvalidWorkspace);
            }
            return Ok(cwd);
        }
        if request.canonical_cwd.is_some() {
            return Err(RuntimeError::InvalidWorkspace);
        }
        let configured = self
            .config
            .isolated_cwd
            .as_deref()
            .filter(|path| path.is_absolute())
            .ok_or(RuntimeError::InvalidWorkspace)?;
        if has_reparse_point(configured) {
            return Err(RuntimeError::InvalidWorkspace);
        }
        fs::create_dir_all(configured).map_err(|_| RuntimeError::InvalidWorkspace)?;
        if has_reparse_point(configured) {
            return Err(RuntimeError::InvalidWorkspace);
        }
        let canonical = configured
            .canonicalize()
            .map_err(|_| RuntimeError::InvalidWorkspace)?;
        if !canonical.is_dir() || has_reparse_point(&canonical) {
            return Err(RuntimeError::InvalidWorkspace);
        }
        Ok(canonical)
    }
}

impl ClaudeCodeRuntime {
    fn run_stream(
        &self,
        request: &RuntimeRequest,
        cancelled: &AtomicBool,
        producer: &crate::RuntimeEventProducer,
    ) -> Result<(), RuntimeError> {
        if cancelled.load(Ordering::Acquire) {
            return Err(RuntimeError::Cancelled);
        }
        let cwd = self.execution_cwd(request)?;
        let total_deadline = crate::TransportDeadline::after(
            bounded_request_timeout(request.timeout_ms)
                .min(self.config.effective_request_timeout()),
        );
        let setup_deadline = total_deadline.capped(CLAUDE_SETUP_TIMEOUT);
        let (session, _version) = self.open_session_until(setup_deadline, &cwd)?;
        let result = (|| {
            if cancelled.load(Ordering::Acquire) {
                return Err(RuntimeError::Cancelled);
            }
            let new_session = claude_rpc_request(
                &session,
                "session/new",
                json!({"cwd": cwd.to_string_lossy(), "mcpServers": []}),
                setup_deadline.remaining()?,
            )?;
            let session_id = new_session
                .get("sessionId")
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty() && value.len() <= 160)
                .ok_or_else(|| RuntimeError::Protocol(CLAUDE_PROTOCOL_ERROR.into()))?
                .to_owned();
            self.register_active_run(
                &request.execution_run_id,
                ClaudeActiveRun {
                    session: Arc::clone(&session),
                    session_id: session_id.clone(),
                },
            );

            producer.push(connector_started_event(
                "claude-code",
                request,
                Some(session_id.clone()),
                None,
            ))?;
            producer.push(runtime_started_event(
                "claude-code",
                "claude-acp",
                request,
                Some(session_id.clone()),
                None,
            ))?;

            let prompt_id = session.next_request_id.fetch_add(1, Ordering::Relaxed);
            session.send_value(
                json!({
                    "jsonrpc": "2.0",
                    "id": prompt_id,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": session_id,
                        "prompt": [{"type": "text", "text": request.rendered_context}]
                    }
                }),
                setup_deadline.remaining()?,
            )?;

            let mut event_index = 2u64;
            loop {
                if cancelled.load(Ordering::Acquire) {
                    let _ = session.send_notification(
                        "session/cancel",
                        json!({"sessionId": session_id}),
                        Duration::from_millis(250),
                    );
                    return Err(RuntimeError::Cancelled);
                }
                let remaining = match total_deadline.remaining() {
                    Ok(remaining) => remaining,
                    Err(error) => return Err(error),
                };
                let Some(value) =
                    session.recv_optional(remaining.min(Duration::from_millis(100)))?
                else {
                    continue;
                };
                if crate::is_jsonrpc_server_request(&value) {
                    session.respond_to_server_request(&value)?;
                    continue;
                }
                if jsonrpc_response_id(&value) == Some(prompt_id) {
                    let prompt_result = rpc_response_result(&value)?;
                    let stop_reason = prompt_result
                        .get("stopReason")
                        .and_then(Value::as_str)
                        .ok_or_else(|| RuntimeError::Protocol(CLAUDE_PROTOCOL_ERROR.into()))?;
                    if stop_reason == "cancelled" {
                        return Err(RuntimeError::Cancelled);
                    }
                    if stop_reason != "end_turn" {
                        return Err(RuntimeError::Provider("claude_prompt_failed".into()));
                    }
                    event_index += 1;
                    producer.push(terminal_event(
                        "claude-code",
                        "execution.completed",
                        request,
                        Some(session_id.clone()),
                        None,
                        event_index,
                        None,
                    ))?;
                    return Ok(());
                }
                if let Some(delta) = claude_agent_message_chunk(&value) {
                    event_index += 1;
                    producer.push(output_delta_event(
                        "claude-code",
                        request,
                        Some(session_id.clone()),
                        None,
                        event_index,
                        delta,
                    ))?;
                }
            }
        })();
        self.finish_execution(&request.execution_run_id);
        session.terminate();
        result
    }
}

impl Default for ClaudeCodeRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl RuntimeAdapter for ClaudeCodeRuntime {
    fn id(&self) -> &str {
        "claude-code"
    }

    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities {
            streaming: true,
            cancel: true,
            filesystem: true,
            shell: true,
        }
    }

    fn discover(&self) -> RuntimeDiscovery {
        match self.ensure_available() {
            Ok(()) => RuntimeDiscovery {
                runtime_id: self.id().into(),
                version: None,
                owned: true,
            },
            Err(_) => RuntimeDiscovery {
                runtime_id: self.id().into(),
                version: None,
                owned: false,
            },
        }
    }

    fn health(&self) -> RuntimeHealth {
        match self.ensure_available() {
            Ok(()) => RuntimeHealth {
                runtime_id: self.id().into(),
                status: "available".into(),
                detail: None,
            },
            Err(_) => RuntimeHealth {
                runtime_id: self.id().into(),
                status: "unavailable".into(),
                detail: None,
            },
        }
    }

    fn ensure_available(&self) -> Result<(), RuntimeError> {
        let probe_cwd = std::env::current_dir().unwrap_or_else(|_| std::env::temp_dir());
        let deadline = crate::TransportDeadline::after(CLAUDE_SETUP_TIMEOUT);
        let (session, _version) = self.open_session_until(deadline, &probe_cwd)?;
        session.terminate();
        Ok(())
    }

    fn list_models(&self) -> Vec<String> {
        Vec::new()
    }

    fn execute(&self, request: &RuntimeRequest) -> Result<Vec<RuntimeEvent>, RuntimeError> {
        let stream = self.stream_events_with_capacity(request, DEFAULT_RUNTIME_STREAM_CAPACITY)?;
        let mut events = Vec::new();
        while let Some(event) = stream.next()? {
            events.push(event);
        }
        Ok(events)
    }

    fn stream_events_with_capacity(
        &self,
        request: &RuntimeRequest,
        capacity: usize,
    ) -> Result<RuntimeEventStream, RuntimeError> {
        validate_claude_execution_request(request)?;
        if capacity == 0 {
            return Err(RuntimeError::InvalidStreamCapacity);
        }
        let cancelled = self.begin_execution(&request.execution_run_id)?;
        let cancel_for_callback = Arc::clone(&cancelled);
        let state = Arc::clone(&self.state);
        let config = self.config.clone();
        let request = request.clone();
        RuntimeEventStream::spawn_with_cancel(
            capacity,
            move || cancel_for_callback.store(true, Ordering::Release),
            move |producer| {
                let runtime = ClaudeCodeRuntime {
                    config,
                    state: Arc::clone(&state),
                };
                let result = runtime.run_stream(&request, &cancelled, producer);
                runtime.finish_execution(&request.execution_run_id);
                result
            },
        )
    }

    fn cancel(&self, request: &RuntimeRequest) -> Result<RuntimeEvent, RuntimeError> {
        let cancelled = self
            .lock_state()
            .active_cancellations
            .get(&request.execution_run_id)
            .cloned()
            .ok_or(RuntimeError::Cancelled)?;
        cancelled.store(true, Ordering::Release);
        let active = self
            .lock_state()
            .active_runs
            .get(&request.execution_run_id)
            .cloned();
        if let Some(active) = active {
            let _ = active.session.send_notification(
                "session/cancel",
                json!({"sessionId": active.session_id}),
                Duration::from_millis(250),
            );
        }
        Ok(crate::cancelled_event("claude-code", request, None, None))
    }

    fn shutdown_owned(&self) -> Result<(), RuntimeError> {
        let mut state = self.lock_state();
        for cancelled in state.active_cancellations.values() {
            cancelled.store(true, Ordering::Release);
        }
        let active_runs = state.active_runs.drain().collect::<Vec<_>>();
        drop(state);
        for (_, active) in active_runs {
            active.session.terminate();
        }
        Ok(())
    }
}

fn validate_claude_execution_request(request: &RuntimeRequest) -> Result<(), RuntimeError> {
    if request.timeout_ms == 0 || request.timeout_ms > MAX_RUNTIME_TIMEOUT_MS {
        return Err(RuntimeError::InvalidWorkspace);
    }
    if request.rendered_context.trim().is_empty() {
        return Err(RuntimeError::InvalidWorkspace);
    }
    Ok(())
}

fn find_claude_executable() -> Option<PathBuf> {
    for variable in ["CLAUDE_BINARY_PATH", "CLAUDE_BINARY"] {
        if let Some(value) = std::env::var_os(variable) {
            let path = PathBuf::from(value);
            if is_real_regular_file(&path) {
                return Some(path);
            }
        }
    }
    let path = std::env::var_os("PATH")?;
    for directory in std::env::split_paths(&path) {
        #[cfg(windows)]
        {
            let exe = directory.join("claude.exe");
            if is_real_regular_file(&exe) {
                return Some(exe);
            }
            let cmd = directory.join("claude.cmd");
            if is_real_regular_file(&cmd) {
                if let Some(exe) = native_exe_from_cmd_script(&cmd) {
                    return Some(exe);
                }
            }
        }
        #[cfg(not(windows))]
        {
            let executable = directory.join("claude");
            if is_real_regular_file(&executable) {
                return Some(executable);
            }
        }
    }
    None
}

#[cfg(windows)]
fn native_exe_from_cmd_script(script: &Path) -> Option<PathBuf> {
    let text = fs::read_to_string(script).ok()?;
    let parent = script.parent()?;
    for line in text.lines().take(24) {
        let trimmed = line.trim();
        let Some(start) = trimmed.find('"') else {
            continue;
        };
        let rest = &trimmed[start + 1..];
        let end = rest.find('"')?;
        let raw = &rest[..end];
        let raw = if let Some(value) = raw.strip_prefix("%dp0%\\") {
            value
        } else if let Some(value) = raw.strip_prefix("%dp0%/") {
            value
        } else if raw.contains("%dp0%") {
            continue;
        } else {
            raw
        };
        let candidate = parent.join(raw);
        if is_real_regular_file(&candidate) {
            return Some(candidate);
        }
    }
    None
}

fn probe_claude_version(executable: &Path) -> Option<String> {
    let output = run_claude_probe(executable, &["--version"], true)?;
    parse_claude_version(&output)
}

fn parse_claude_version(output: &str) -> Option<String> {
    let line = output.lines().find(|line| !line.trim().is_empty())?;
    line.split_whitespace()
        .find(|part| {
            part.len() <= 64
                && part
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b".-+_".contains(&byte))
        })
        .map(str::to_owned)
}

fn probe_claude_login_state(executable: &Path) -> IntegrationLoginState {
    let Some(output) = run_claude_probe(executable, &["auth", "status"], false) else {
        return IntegrationLoginState::Unknown;
    };
    let Ok(value) = serde_json::from_str::<Value>(&output) else {
        return IntegrationLoginState::Unknown;
    };
    match value.get("loggedIn").and_then(Value::as_bool) {
        Some(true) => IntegrationLoginState::LoggedIn,
        Some(false) => IntegrationLoginState::LoginRequired,
        None => IntegrationLoginState::Unknown,
    }
}

fn run_claude_probe(executable: &Path, args: &[&str], require_success: bool) -> Option<String> {
    let mut child = Command::new(executable)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .ok()?;
    let mut stdout = child.stdout.take()?;
    let mut stderr = child.stderr.take()?;
    let stdout_reader = thread::spawn(move || {
        let mut output = Vec::new();
        let mut buffer = [0u8; 4096];
        loop {
            match stdout.read(&mut buffer) {
                Ok(0) | Err(_) => break,
                Ok(read) => {
                    if output.len().saturating_add(read) > 64 * 1024 {
                        break;
                    }
                    output.extend_from_slice(&buffer[..read]);
                }
            }
        }
        output
    });
    let stderr_reader = thread::spawn(move || {
        let mut output = Vec::new();
        let mut buffer = [0u8; 4096];
        loop {
            match stderr.read(&mut buffer) {
                Ok(0) | Err(_) => break,
                Ok(read) => {
                    if output.len().saturating_add(read) > MAX_CLAUDE_STDERR_BYTES {
                        break;
                    }
                    output.extend_from_slice(&buffer[..read]);
                }
            }
        }
        output
    });
    let deadline = Instant::now() + INTEGRATION_PROBE_TIMEOUT;
    let status = loop {
        if Instant::now() >= deadline {
            let _ = child.kill();
            return None;
        }
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) => thread::sleep(Duration::from_millis(5)),
            Err(_) => return None,
        }
    };
    let stdout = stdout_reader.join().ok()?;
    let stderr = stderr_reader.join().ok()?;
    let mut output = String::from_utf8_lossy(&stdout).into_owned();
    if output.trim().is_empty() {
        output = String::from_utf8_lossy(&stderr).into_owned();
    }
    if require_success && !status.success() {
        return None;
    }
    Some(output)
}

fn open_claude_acp_session(
    executable: &Path,
    command_args: &[String],
    cwd: &Path,
    deadline: crate::TransportDeadline,
) -> Result<(Arc<ClaudeAcpSession>, Option<String>), RuntimeError> {
    let mut command = Command::new(executable);
    command
        .args(command_args)
        .arg("--acp")
        .current_dir(cwd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    configure_claude_child_environment(&mut command);
    let mut child = command
        .spawn()
        .map_err(|_| RuntimeError::Transport(CLAUDE_RUNTIME_UNAVAILABLE.into()))?;
    let Some(stdin) = child.stdin.take() else {
        terminate_spawned_child(&mut child);
        return Err(RuntimeError::Transport(CLAUDE_RUNTIME_UNAVAILABLE.into()));
    };
    let Some(stdout) = child.stdout.take() else {
        terminate_spawned_child(&mut child);
        return Err(RuntimeError::Transport(CLAUDE_RUNTIME_UNAVAILABLE.into()));
    };
    let Some(stderr) = child.stderr.take() else {
        terminate_spawned_child(&mut child);
        return Err(RuntimeError::Transport(CLAUDE_RUNTIME_UNAVAILABLE.into()));
    };
    let session = Arc::new(ClaudeAcpSession::new(child, stdin, stdout, stderr));

    let remaining = deadline.remaining()?;
    let initialization = claude_rpc_request(
        &session,
        "initialize",
        json!({
            "protocolVersion": 1,
            "clientCapabilities": {},
            "clientInfo": {"name": "AgentTalk", "version": "1"}
        }),
        remaining,
    );
    let initialization = match initialization {
        Ok(value) => value,
        Err(error) => {
            session.terminate();
            return Err(error);
        }
    };
    if initialization
        .get("protocolVersion")
        .and_then(Value::as_u64)
        != Some(1)
    {
        session.terminate();
        return Err(RuntimeError::Protocol(CLAUDE_PROTOCOL_ERROR.into()));
    }
    let version = initialization
        .pointer("/serverInfo/version")
        .and_then(Value::as_str)
        .or_else(|| initialization.get("version").and_then(Value::as_str))
        .map(str::to_owned);
    Ok((session, version))
}

fn claude_rpc_request(
    session: &Arc<ClaudeAcpSession>,
    method: &str,
    params: Value,
    timeout: Duration,
) -> Result<Value, RuntimeError> {
    let deadline = crate::TransportDeadline::after(timeout);
    let id = session.next_request_id.fetch_add(1, Ordering::Relaxed);
    session.send_value(
        json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}),
        deadline.remaining()?,
    )?;
    loop {
        let remaining = deadline.remaining()?;
        let value = session.recv_raw(remaining)?;
        if jsonrpc_response_id(&value) == Some(id) {
            return rpc_response_result(&value);
        }
        if crate::is_jsonrpc_server_request(&value) {
            session.respond_to_server_request(&value)?;
        }
    }
}

struct ClaudeAcpSession {
    child: Mutex<Child>,
    stdin: Mutex<ChildStdin>,
    receiver: Mutex<mpsc::Receiver<ClaudeInbound>>,
    next_request_id: AtomicU64,
}

enum ClaudeInbound {
    Value(Value),
    Error(RuntimeError),
}

impl ClaudeAcpSession {
    fn new(
        child: Child,
        stdin: ChildStdin,
        stdout: ChildStdout,
        stderr: impl Read + Send + 'static,
    ) -> Self {
        let (sender, receiver) = mpsc::sync_channel(128);
        spawn_claude_reader(stdout, sender);
        spawn_claude_stderr_reader(stderr);
        Self {
            child: Mutex::new(child),
            stdin: Mutex::new(stdin),
            receiver: Mutex::new(receiver),
            next_request_id: AtomicU64::new(1),
        }
    }

    fn send_value(self: &Arc<Self>, value: Value, timeout: Duration) -> Result<(), RuntimeError> {
        if timeout.is_zero() {
            return Err(RuntimeError::Timeout);
        }
        let encoded = serde_json::to_string(&value)
            .map_err(|_| RuntimeError::Protocol(CLAUDE_PROTOCOL_ERROR.into()))?;
        let completed = Arc::new(AtomicBool::new(false));
        let expired = Arc::new(AtomicBool::new(false));
        let watcher_session = Arc::clone(self);
        let watcher_completed = Arc::clone(&completed);
        let watcher_expired = Arc::clone(&expired);
        thread::spawn(move || {
            thread::sleep(timeout);
            if !watcher_completed.load(Ordering::Acquire) {
                watcher_expired.store(true, Ordering::Release);
                watcher_session.terminate();
            }
        });
        let mut stdin = match self.stdin.lock() {
            Ok(stdin) => stdin,
            Err(poisoned) => poisoned.into_inner(),
        };
        let result = stdin
            .write_all(encoded.as_bytes())
            .and_then(|()| stdin.write_all(b"\n"))
            .and_then(|()| stdin.flush())
            .map_err(|_| RuntimeError::Transport(CLAUDE_RUNTIME_UNAVAILABLE.into()));
        completed.store(true, Ordering::Release);
        if expired.load(Ordering::Acquire) {
            Err(RuntimeError::Timeout)
        } else {
            result
        }
    }

    fn send_notification(
        self: &Arc<Self>,
        method: &str,
        params: Value,
        timeout: Duration,
    ) -> Result<(), RuntimeError> {
        self.send_value(
            json!({"jsonrpc": "2.0", "method": method, "params": params}),
            timeout,
        )
    }

    fn recv_raw(&self, timeout: Duration) -> Result<Value, RuntimeError> {
        let receiver = match self.receiver.lock() {
            Ok(receiver) => receiver,
            Err(poisoned) => poisoned.into_inner(),
        };
        match receiver.recv_timeout(timeout) {
            Ok(ClaudeInbound::Value(value)) => Ok(value),
            Ok(ClaudeInbound::Error(error)) => Err(error),
            Err(mpsc::RecvTimeoutError::Timeout) => Err(RuntimeError::Timeout),
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(RuntimeError::TransportClosed),
        }
    }

    fn recv_optional(&self, timeout: Duration) -> Result<Option<Value>, RuntimeError> {
        let receiver = match self.receiver.lock() {
            Ok(receiver) => receiver,
            Err(poisoned) => poisoned.into_inner(),
        };
        match receiver.recv_timeout(timeout) {
            Ok(ClaudeInbound::Value(value)) => Ok(Some(value)),
            Ok(ClaudeInbound::Error(error)) => Err(error),
            Err(mpsc::RecvTimeoutError::Timeout) => Ok(None),
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(RuntimeError::TransportClosed),
        }
    }

    fn respond_to_server_request(self: &Arc<Self>, request: &Value) -> Result<(), RuntimeError> {
        let id = request.get("id").cloned().unwrap_or(Value::Null);
        self.send_value(
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": {"code": -32601, "message": "Method not found"}
            }),
            CLAUDE_SETUP_TIMEOUT,
        )
    }

    fn terminate(&self) {
        self.terminate_until(crate::TransportDeadline::after(CLAUDE_CLEANUP_GRACE));
    }

    fn terminate_until(&self, deadline: crate::TransportDeadline) {
        let mut child = match self.child.lock() {
            Ok(child) => child,
            Err(poisoned) => poisoned.into_inner(),
        };
        let _ = child.kill();
        while deadline.remaining().is_ok() {
            match child.try_wait() {
                Ok(Some(_)) | Err(_) => return,
                Ok(None) => thread::sleep(Duration::from_millis(20)),
            }
        }
    }
}

fn spawn_claude_reader(
    stdout: impl Read + Send + 'static,
    sender: mpsc::SyncSender<ClaudeInbound>,
) {
    thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) => {
                    let _ = sender.send(ClaudeInbound::Error(RuntimeError::TransportClosed));
                    return;
                }
                Ok(_) if line.len() > MAX_CLAUDE_LINE_BYTES => {
                    let _ = sender.send(ClaudeInbound::Error(RuntimeError::Protocol(
                        CLAUDE_PROTOCOL_ERROR.into(),
                    )));
                    return;
                }
                Ok(_) => {
                    let line = line.trim();
                    if line.is_empty() {
                        continue;
                    }
                    match serde_json::from_str::<Value>(line) {
                        Ok(value) => {
                            if sender.send(ClaudeInbound::Value(value)).is_err() {
                                return;
                            }
                        }
                        Err(_) => {
                            let _ = sender.send(ClaudeInbound::Error(RuntimeError::Protocol(
                                CLAUDE_PROTOCOL_ERROR.into(),
                            )));
                            return;
                        }
                    }
                }
                Err(_) => {
                    let _ = sender.send(ClaudeInbound::Error(RuntimeError::TransportClosed));
                    return;
                }
            }
        }
    });
}

fn spawn_claude_stderr_reader(stderr: impl Read + Send + 'static) {
    thread::spawn(move || {
        let mut reader = BufReader::new(stderr);
        let mut buffer = [0u8; 1024];
        loop {
            match reader.read(&mut buffer) {
                Ok(0) | Err(_) => return,
                Ok(_) => {}
            }
        }
    });
}

fn terminate_spawned_child(child: &mut Child) {
    let _ = child.kill();
    let deadline = Instant::now() + CLAUDE_CLEANUP_GRACE;
    while Instant::now() < deadline {
        match child.try_wait() {
            Ok(Some(_)) | Err(_) => return,
            Ok(None) => thread::sleep(Duration::from_millis(20)),
        }
    }
}

fn configure_claude_child_environment(command: &mut Command) {
    command.env_clear();
    for (key, value) in claude_child_environment_values(|key| std::env::var_os(key)) {
        command.env(key, value);
    }
}

const CLAUDE_CHILD_ENV_WHITELIST: &[&str] = &[
    "PATH",
    "PATHEXT",
    "SystemRoot",
    "WINDIR",
    "ComSpec",
    "SYSTEMDRIVE",
    "TEMP",
    "TMP",
    "USERPROFILE",
    "HOME",
    "HOMEDRIVE",
    "HOMEPATH",
    "USERNAME",
    "USERDOMAIN",
    "APPDATA",
    "LOCALAPPDATA",
    "ProgramData",
    "ProgramFiles",
    "ProgramFiles(x86)",
    "OS",
    "PROCESSOR_ARCHITECTURE",
    "PROCESSOR_ARCHITEW6432",
    "NUMBER_OF_PROCESSORS",
    "LANG",
    "LC_ALL",
    "PSModulePath",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "NO_PROXY",
    "SSL_CERT_FILE",
    "NODE_EXTRA_CA_CERTS",
    "CLAUDE_CONFIG_DIR",
];

fn claude_child_environment_values(
    lookup: impl Fn(&str) -> Option<OsString>,
) -> Vec<(&'static str, OsString)> {
    CLAUDE_CHILD_ENV_WHITELIST
        .iter()
        .filter_map(|key| lookup(key).map(|value| (*key, value)))
        .collect()
}

fn claude_agent_message_chunk(frame: &Value) -> Option<&str> {
    if frame.get("method").and_then(Value::as_str) != Some("session/update") {
        return None;
    }
    let update = frame.get("params")?.get("update")?;
    if update.get("sessionUpdate").and_then(Value::as_str) != Some("agent_message_chunk")
        || update.get("content")?.get("type").and_then(Value::as_str) != Some("text")
    {
        return None;
    }
    update.get("content")?.get("text")?.as_str()
}
