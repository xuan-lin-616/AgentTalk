use std::collections::HashMap;
use std::ffi::OsString;
use std::fs;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStderr, ChildStdout, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use agenttalk_domain::WorkspaceAccess;
use serde_json::Value;

use crate::{
    bounded_request_timeout, connector_started_event, has_reparse_point, is_real_regular_file,
    output_delta_event, runtime_started_event, terminal_event, RuntimeAdapter, RuntimeCapabilities,
    RuntimeDiscovery, RuntimeError, RuntimeEvent, RuntimeEventStream, RuntimeHealth,
    RuntimeRequest, DEFAULT_RUNTIME_STREAM_CAPACITY, MAX_RUNTIME_TIMEOUT_MS,
};

use super::{
    Integration, IntegrationConnectError, IntegrationDescriptor, IntegrationDetectOutcome,
    IntegrationInstalled, IntegrationLoginState, IntegrationVerification,
    IntegrationVerificationStatus, INTEGRATION_PROBE_TIMEOUT,
};

const ANTIGRAVITY_RUNTIME_UNAVAILABLE: &str = "antigravity_runtime_unavailable";
const ANTIGRAVITY_PROTOCOL_ERROR: &str = "antigravity_protocol_error";
const MAX_ANTIGRAVITY_LINE_BYTES: usize = 4 * 1024 * 1024;
const ANTIGRAVITY_MODELS_TIMEOUT: Duration = Duration::from_secs(20);
const ANTIGRAVITY_CLEANUP_GRACE: Duration = Duration::from_millis(1_500);

pub struct AntigravityIntegration;

fn descriptor() -> &'static IntegrationDescriptor {
    static DESCRIPTOR: OnceLock<IntegrationDescriptor> = OnceLock::new();
    DESCRIPTOR.get_or_init(|| IntegrationDescriptor {
        id: "local.antigravity".into(),
        display_name: "Antigravity".into(),
        category: "agent_runtime".into(),
        protocol: "agy-stream-json".into(),
        runtime_type: "antigravity".into(),
        install_command: String::new(),
        needs_consent: true,
    })
}

impl Integration for AntigravityIntegration {
    fn descriptor(&self) -> &IntegrationDescriptor {
        descriptor()
    }

    fn detect(&self) -> IntegrationDetectOutcome {
        let Some(executable) = find_agy_executable() else {
            return IntegrationDetectOutcome::NotInstalled;
        };
        // agy is a Go CLI: only `--version` / `--help` are safe probes. Bare
        // positional args (including `agy version` and `agy models`) are
        // treated as a model prompt and may hang in a login/network wait, so
        // this code only ever passes `--`-prefixed flags.
        let Some(version) = probe_agy_version(&executable) else {
            return IntegrationDetectOutcome::NotInstalled;
        };
        IntegrationDetectOutcome::Installed(IntegrationInstalled {
            version,
            login_state: IntegrationLoginState::Unknown,
        })
    }

    fn verify(&self) -> IntegrationVerification {
        let installed = self.detect();
        let login_state = match &installed {
            IntegrationDetectOutcome::Installed(installed) => installed.login_state,
            IntegrationDetectOutcome::NotInstalled => IntegrationLoginState::Unknown,
        };
        match self.connect() {
            Ok(_) => IntegrationVerification {
                integration_id: self.descriptor().id.clone(),
                status: IntegrationVerificationStatus::Verified,
                login_state,
                protocol_major: Some(1),
                version: None,
                detail: Some("agy_stream_json_initialize_ok".into()),
            },
            Err(IntegrationConnectError::AuthenticationRequired) => IntegrationVerification {
                integration_id: self.descriptor().id.clone(),
                status: IntegrationVerificationStatus::AuthRequired,
                login_state: IntegrationLoginState::LoginRequired,
                protocol_major: Some(1),
                version: None,
                detail: Some(
                    "agy_headless_oauth_required_run_agy_interactively_to_complete_device_code"
                        .into(),
                ),
            },
            Err(_) => IntegrationVerification {
                integration_id: self.descriptor().id.clone(),
                status: IntegrationVerificationStatus::Rejected,
                login_state,
                protocol_major: Some(1),
                version: None,
                detail: Some("agy_stream_json_initialize_failed".into()),
            },
        }
    }

    fn connect(&self) -> Result<Box<dyn RuntimeAdapter>, IntegrationConnectError> {
        let executable = find_agy_executable().ok_or(IntegrationConnectError::NotInstalled)?;
        let runtime = AntigravityRuntime::with_config(AntigravityConfig {
            binary_path: Some(executable),
            ..AntigravityConfig::default()
        });
        // Establish one bounded NDJSON stream-json roundtrip now. The probe
        // sends one user message and only waits for the first stream event;
        // if agy is not signed in it reports AuthenticationRequired instead
        // of a fake protocol success.
        runtime
            .probe_stream_json_roundtrip()
            .map_err(|error| match error {
                RuntimeError::Authentication => IntegrationConnectError::AuthenticationRequired,
                _ => IntegrationConnectError::ConnectFailed,
            })?;
        Ok(Box::new(runtime))
    }
}

fn find_agy_executable() -> Option<PathBuf> {
    for variable in ["AGY_BINARY_PATH", "AGY_BINARY"] {
        if let Some(value) = std::env::var_os(variable) {
            let path = PathBuf::from(value);
            if is_real_regular_file(&path) {
                return Some(path);
            }
        }
    }
    #[cfg(windows)]
    if let Some(local_app_data) = std::env::var_os("LOCALAPPDATA") {
        let installed = PathBuf::from(local_app_data)
            .join("agy")
            .join("bin")
            .join("agy.exe");
        if is_real_regular_file(&installed) {
            return Some(installed);
        }
    }
    let path = std::env::var_os("PATH")?;
    for directory in std::env::split_paths(&path) {
        #[cfg(windows)]
        {
            let exe = directory.join("agy.exe");
            if is_real_regular_file(&exe) {
                return Some(exe);
            }
            let cmd = directory.join("agy.cmd");
            if is_real_regular_file(&cmd) {
                if let Some(exe) = native_exe_from_cmd_script(&cmd) {
                    return Some(exe);
                }
            }
        }
        #[cfg(not(windows))]
        {
            let executable = directory.join("agy");
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

fn probe_agy_version(executable: &Path) -> Option<String> {
    // Only --version and --help are safe flag probes. Never use `agy version`
    // (bare positional) because agy treats it as a model prompt.
    for args in [&["--version"][..], &["--help"][..]] {
        if let Some(output) = run_agy_probe(executable, args) {
            if let Some(version) = parse_agy_version(&output) {
                return Some(version);
            }
        }
    }
    None
}

fn spawn_agy_reader(
    mut stream: impl std::io::Read + Send + 'static,
) -> thread::JoinHandle<Vec<u8>> {
    thread::spawn(move || {
        let mut output = Vec::new();
        let mut buffer = [0u8; 4096];
        loop {
            match stream.read(&mut buffer) {
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
    })
}

fn run_agy_probe(executable: &Path, args: &[&str]) -> Option<String> {
    let mut child = Command::new(executable)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .ok()?;
    let stdout = child.stdout.take()?;
    let stderr = child.stderr.take()?;
    let stdout_reader = spawn_agy_reader(stdout);
    let stderr_reader = spawn_agy_reader(stderr);
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
    if !status.success() {
        return None;
    }
    let stdout = stdout_reader.join().ok()?;
    let stderr = stderr_reader.join().ok()?;
    let mut output = String::from_utf8_lossy(&stdout).into_owned();
    if output.trim().is_empty() {
        output = String::from_utf8_lossy(&stderr).into_owned();
    }
    Some(output)
}

fn parse_agy_version(output: &str) -> Option<String> {
    for line in output.lines().filter(|line| !line.trim().is_empty()) {
        if let Some(part) = line.split_whitespace().find(|part| {
            part.len() <= 64
                && part.bytes().any(|byte| byte.is_ascii_digit())
                && part
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b".-+_".contains(&byte))
        }) {
            return Some(part.to_owned());
        }
    }
    None
}

#[derive(Clone, Debug)]
pub struct AntigravityConfig {
    pub binary_path: Option<PathBuf>,
    pub command_args: Vec<String>,
    pub isolated_cwd: Option<PathBuf>,
    pub request_timeout: Duration,
}

impl Default for AntigravityConfig {
    fn default() -> Self {
        Self {
            binary_path: None,
            command_args: Vec::new(),
            isolated_cwd: None,
            request_timeout: Duration::from_secs(120),
        }
    }
}

impl AntigravityConfig {
    fn effective_request_timeout(&self) -> Duration {
        if self.request_timeout.is_zero() {
            Duration::from_secs(120)
        } else {
            self.request_timeout
        }
    }
}

#[derive(Clone)]
pub struct AntigravityRuntime {
    config: AntigravityConfig,
    state: Arc<Mutex<AntigravityState>>,
}

#[derive(Default)]
struct AntigravityState {
    active_cancellations: HashMap<String, Arc<AtomicBool>>,
    active_children: HashMap<String, Arc<Mutex<Child>>>,
}

impl AntigravityRuntime {
    pub fn new() -> Self {
        Self::with_config(AntigravityConfig::default())
    }

    pub fn with_config(config: AntigravityConfig) -> Self {
        Self {
            config,
            state: Arc::new(Mutex::new(AntigravityState::default())),
        }
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, AntigravityState> {
        match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn begin_execution(&self, execution_run_id: &str) -> Result<Arc<AtomicBool>, RuntimeError> {
        let mut state = self.lock_state();
        if state.active_cancellations.contains_key(execution_run_id) {
            return Err(RuntimeError::Protocol(
                "antigravity_execution_already_active".into(),
            ));
        }
        let cancelled = Arc::new(AtomicBool::new(false));
        state
            .active_cancellations
            .insert(execution_run_id.to_owned(), Arc::clone(&cancelled));
        Ok(cancelled)
    }

    fn register_active_child(&self, execution_run_id: &str, child: Arc<Mutex<Child>>) {
        self.lock_state()
            .active_children
            .insert(execution_run_id.to_owned(), child);
    }

    fn finish_execution(&self, execution_run_id: &str) {
        let mut state = self.lock_state();
        state.active_cancellations.remove(execution_run_id);
        state.active_children.remove(execution_run_id);
    }

    fn binary_path(&self) -> Result<PathBuf, RuntimeError> {
        if let Some(path) = self.config.binary_path.as_deref() {
            if is_real_regular_file(path) {
                return Ok(path.to_path_buf());
            }
        }
        find_agy_executable()
            .ok_or_else(|| RuntimeError::Transport(ANTIGRAVITY_RUNTIME_UNAVAILABLE.into()))
    }

    fn execution_workspace(
        &self,
        request: &RuntimeRequest,
    ) -> Result<(PathBuf, Option<PathBuf>), RuntimeError> {
        if let Some(cwd) = request
            .canonical_cwd
            .as_deref()
            .filter(|cwd| !cwd.trim().is_empty())
            .map(PathBuf::from)
        {
            if !cwd.is_absolute() || !cwd.is_dir() {
                return Err(RuntimeError::InvalidWorkspace);
            }
            let canonical = cwd
                .canonicalize()
                .map_err(|_| RuntimeError::InvalidWorkspace)?;
            if !canonical.is_dir() || has_reparse_point(&canonical) {
                return Err(RuntimeError::InvalidWorkspace);
            }
            // Pass the project root both as the child cwd and as an
            // explicit `--add-dir`, so agy tools can write to the project
            // instead of falling back to the agy scratch directory.
            // agy may reject Windows verbatim `\?\` prefixes for
            // `--add-dir`, so strip that prefix from the workspace path
            // while keeping the canonical path as the child cwd.
            let workspace_dir = cwd
                .to_string_lossy()
                .trim_start_matches("\\\\?\\")
                .to_owned();
            return Ok((canonical, Some(PathBuf::from(workspace_dir))));
        }
        if matches!(request.workspace_access, WorkspaceAccess::None) {
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
            return Ok((canonical, None));
        }
        Err(RuntimeError::InvalidWorkspace)
    }

    fn spawn_stream_child(
        &self,
        cwd: &Path,
        workspace_dir: Option<&Path>,
        print_timeout: Duration,
        prompt: &str,
    ) -> Result<(Child, ChildStdout, ChildStderr), RuntimeError> {
        let executable = self.binary_path()?;
        let mut command = Command::new(executable);
        // agy 1.1.x parses the print prompt as the first positional arg
        // after `--print`. Flags must come after the prompt; otherwise the
        // flags themselves are sent to the model as the prompt. Confirmed
        // against a real logged-in `agy`:
        //   agy --print <prompt> --output-format=stream-json --print-timeout=N
        command
            .args(&self.config.command_args)
            .arg("--print")
            .arg(prompt)
            .arg("--output-format=stream-json");
        if let Some(workspace_dir) = workspace_dir {
            command.arg("--add-dir").arg(workspace_dir);
        }
        command
            .arg(format!(
                "--print-timeout={}s",
                print_timeout.as_secs().max(1)
            ))
            .current_dir(cwd)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        configure_antigravity_child_environment(&mut command);
        let mut child = command
            .spawn()
            .map_err(|_| RuntimeError::Transport(ANTIGRAVITY_RUNTIME_UNAVAILABLE.into()))?;
        let Some(stdout) = child.stdout.take() else {
            terminate_spawned_child(&mut child);
            return Err(RuntimeError::Transport(
                ANTIGRAVITY_RUNTIME_UNAVAILABLE.into(),
            ));
        };
        let Some(stderr) = child.stderr.take() else {
            terminate_spawned_child(&mut child);
            return Err(RuntimeError::Transport(
                ANTIGRAVITY_RUNTIME_UNAVAILABLE.into(),
            ));
        };
        Ok((child, stdout, stderr))
    }
}

impl AntigravityRuntime {
    fn probe_stream_json_roundtrip(&self) -> Result<(), RuntimeError> {
        let cwd = std::env::current_dir().unwrap_or_else(|_| std::env::temp_dir());
        let probe_timeout = self
            .config
            .effective_request_timeout()
            .min(Duration::from_secs(30));
        let deadline = crate::TransportDeadline::after(probe_timeout + Duration::from_secs(2));
        let (mut child, stdout, stderr) =
            self.spawn_stream_child(&cwd, None, probe_timeout, "Reply with the single word ok.")?;
        let stderr_tail = Arc::new(Mutex::new(String::new()));
        crate::spawn_bounded_stderr_reader(stderr, Arc::clone(&stderr_tail));
        let (sender, receiver) = mpsc::sync_channel(8);
        spawn_antigravity_line_reader(stdout, sender);
        let result = loop {
            if deadline.remaining().is_err() {
                let stderr = stderr_tail
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clone();
                let _ = child.kill();
                if matches!(
                    antigravity_exit_error(&stderr),
                    RuntimeError::Authentication
                ) {
                    break Err(RuntimeError::Authentication);
                }
                break Err(RuntimeError::Timeout);
            }
            match receiver.recv_timeout(Duration::from_millis(100)) {
                Ok(AntigravityInbound::Value(value)) => {
                    if antigravity_event_type(&value).is_some() {
                        break Ok(());
                    }
                }
                Ok(AntigravityInbound::Error(RuntimeError::TransportClosed)) => {
                    // stdout closed before a stream event. Give the child a
                    // bounded moment to exit so stderr (auth/eligibility or
                    // transport failure) is available for classification.
                    let grace = Instant::now() + Duration::from_millis(500);
                    while Instant::now() < grace {
                        match child.try_wait() {
                            Ok(Some(_)) | Err(_) => break,
                            Ok(None) => thread::sleep(Duration::from_millis(20)),
                        }
                    }
                    let stderr = stderr_tail
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .clone();
                    break Err(antigravity_exit_error(&stderr));
                }
                Ok(AntigravityInbound::Error(error)) => break Err(error),
                Err(mpsc::RecvTimeoutError::Timeout) => match child.try_wait() {
                    Ok(Some(status)) if !status.success() => {
                        let stderr = stderr_tail
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .clone();
                        break Err(antigravity_exit_error(&stderr));
                    }
                    Ok(Some(_)) => break Err(RuntimeError::TransportClosed),
                    Ok(None) => continue,
                    Err(_) => {
                        break Err(RuntimeError::Transport(
                            ANTIGRAVITY_RUNTIME_UNAVAILABLE.into(),
                        ))
                    }
                },
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    let stderr = stderr_tail
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .clone();
                    break Err(antigravity_exit_error(&stderr));
                }
            }
        };
        terminate_spawned_child(&mut child);
        result
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AntigravityModelEntry {
    id: String,
    display_name: String,
}

impl AntigravityRuntime {
    fn run_agy_models(&self) -> Result<Vec<AntigravityModelEntry>, RuntimeError> {
        self.run_agy_models_with_cancelled(&AtomicBool::new(false))
    }

    fn run_agy_models_with_cancelled(
        &self,
        cancelled: &AtomicBool,
    ) -> Result<Vec<AntigravityModelEntry>, RuntimeError> {
        let executable = self.binary_path()?;
        let mut child = Command::new(executable)
            .arg("models")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|_| RuntimeError::Transport(ANTIGRAVITY_RUNTIME_UNAVAILABLE.into()))?;
        let stdout = child.stdout.take().ok_or_else(|| {
            terminate_spawned_child(&mut child);
            RuntimeError::Transport(ANTIGRAVITY_RUNTIME_UNAVAILABLE.into())
        })?;
        let stderr = child.stderr.take().ok_or_else(|| {
            terminate_spawned_child(&mut child);
            RuntimeError::Transport(ANTIGRAVITY_RUNTIME_UNAVAILABLE.into())
        })?;
        let stdout_reader = spawn_agy_reader(stdout);
        let stderr_reader = spawn_agy_reader(stderr);
        let timeout = self
            .config
            .effective_request_timeout()
            .min(ANTIGRAVITY_MODELS_TIMEOUT);
        let deadline = crate::TransportDeadline::after(timeout);
        let status = loop {
            if cancelled.load(Ordering::Acquire) {
                let _ = child.kill();
                let _ = child.wait();
                return Err(RuntimeError::Cancelled);
            }
            if deadline.remaining().is_err() {
                let _ = child.kill();
                let _ = child.wait();
                return Err(RuntimeError::Timeout);
            }
            match child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) => thread::sleep(Duration::from_millis(5)),
                Err(_) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(RuntimeError::Transport(
                        ANTIGRAVITY_RUNTIME_UNAVAILABLE.into(),
                    ));
                }
            }
        };
        let stdout = stdout_reader
            .join()
            .map_err(|_| RuntimeError::Transport(ANTIGRAVITY_RUNTIME_UNAVAILABLE.into()))?;
        let stderr = stderr_reader
            .join()
            .map_err(|_| RuntimeError::Transport(ANTIGRAVITY_RUNTIME_UNAVAILABLE.into()))?;
        if !status.success() {
            let stderr = String::from_utf8_lossy(&stderr);
            return Err(antigravity_exit_error(&stderr));
        }
        let stdout = String::from_utf8_lossy(&stdout);
        let models = parse_agy_models_output(&stdout);
        if models.is_empty() {
            return Err(RuntimeError::Transport(
                ANTIGRAVITY_RUNTIME_UNAVAILABLE.into(),
            ));
        }
        Ok(models)
    }
}

fn parse_agy_models_output(output: &str) -> Vec<AntigravityModelEntry> {
    output
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() || line.starts_with("Fetching") {
                return None;
            }
            let (id, display_name) = line
                .split_once('\t')
                .or_else(|| line.split_once(' '))
                .or_else(|| line.split_once("  "))?;
            let id = id.trim();
            let display_name = display_name.trim();
            if id.is_empty()
                || id.len() > 256
                || id.chars().any(char::is_control)
                || display_name.is_empty()
            {
                return None;
            }
            Some(AntigravityModelEntry {
                id: id.to_owned(),
                display_name: display_name.to_owned(),
            })
        })
        .collect()
}

impl Default for AntigravityRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl RuntimeAdapter for AntigravityRuntime {
    fn id(&self) -> &str {
        "antigravity"
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
        match self.binary_path().and_then(|executable| {
            probe_agy_version(&executable).ok_or(RuntimeError::Transport(
                ANTIGRAVITY_RUNTIME_UNAVAILABLE.into(),
            ))
        }) {
            Ok(version) => RuntimeDiscovery {
                runtime_id: self.id().into(),
                version: Some(version),
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
        match self.discover() {
            discovery if discovery.version.is_some() => RuntimeHealth {
                runtime_id: self.id().into(),
                status: "available".into(),
                detail: None,
            },
            _ => RuntimeHealth {
                runtime_id: self.id().into(),
                status: "unavailable".into(),
                detail: None,
            },
        }
    }

    fn ensure_available(&self) -> Result<(), RuntimeError> {
        let executable = self.binary_path()?;
        let version = probe_agy_version(&executable)
            .ok_or_else(|| RuntimeError::Transport(ANTIGRAVITY_RUNTIME_UNAVAILABLE.into()))?;
        if version.is_empty() {
            return Err(RuntimeError::Transport(
                ANTIGRAVITY_RUNTIME_UNAVAILABLE.into(),
            ));
        }
        Ok(())
    }

    fn list_models(&self) -> Vec<String> {
        self.list_models_checked().unwrap_or_default()
    }

    fn list_models_checked(&self) -> Result<Vec<String>, RuntimeError> {
        match self.run_agy_models() {
            Ok(models) => Ok(models.into_iter().map(|entry| entry.id).collect::<Vec<_>>()),
            // `agy models` returns auth/eligibility errors before the
            // catalog. Treat that as an empty catalog rather than hanging
            // the connector.models query on a login wait.
            Err(RuntimeError::Authentication) => Ok(Vec::new()),
            Err(error) => Err(error),
        }
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
        validate_antigravity_request(request)?;
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
                let runtime = AntigravityRuntime {
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
        if let Some(child) = self
            .lock_state()
            .active_children
            .get(&request.execution_run_id)
            .cloned()
        {
            if let Ok(mut child) = child.lock() {
                let _ = child.kill();
            }
        }
        Ok(crate::cancelled_event("antigravity", request, None, None))
    }

    fn shutdown_owned(&self) -> Result<(), RuntimeError> {
        let mut state = self.lock_state();
        for cancelled in state.active_cancellations.values() {
            cancelled.store(true, Ordering::Release);
        }
        let children = state.active_children.drain().collect::<Vec<_>>();
        drop(state);
        for (_, child) in children {
            if let Ok(mut child) = child.lock() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
        Ok(())
    }
}

impl AntigravityRuntime {
    fn run_stream(
        &self,
        request: &RuntimeRequest,
        cancelled: &AtomicBool,
        producer: &crate::RuntimeEventProducer,
    ) -> Result<(), RuntimeError> {
        if cancelled.load(Ordering::Acquire) {
            return Err(RuntimeError::Cancelled);
        }
        let (cwd, workspace_dir) = self.execution_workspace(request)?;
        let total_deadline = crate::TransportDeadline::after(
            bounded_request_timeout(request.timeout_ms)
                .min(self.config.effective_request_timeout()),
        );
        let print_timeout = bounded_request_timeout(request.timeout_ms)
            .min(self.config.effective_request_timeout());
        let (child, stdout, stderr) = self.spawn_stream_child(
            &cwd,
            workspace_dir.as_deref(),
            print_timeout,
            &request.rendered_context,
        )?;
        let child = Arc::new(Mutex::new(child));
        self.register_active_child(&request.execution_run_id, Arc::clone(&child));
        let stderr_tail = Arc::new(Mutex::new(String::new()));
        crate::spawn_bounded_stderr_reader(stderr, Arc::clone(&stderr_tail));
        let (sender, receiver) = mpsc::sync_channel(128);
        spawn_antigravity_line_reader(stdout, sender);

        let result = (|| {
            producer.push(connector_started_event("antigravity", request, None, None))?;
            let mut event_index = 1u64;
            let mut accumulated_text = String::new();
            let mut started = false;
            loop {
                if cancelled.load(Ordering::Acquire) {
                    kill_antigravity_child(&child);
                    return Err(RuntimeError::Cancelled);
                }
                let remaining = match total_deadline.remaining() {
                    Ok(remaining) => remaining,
                    Err(error) => {
                        kill_antigravity_child(&child);
                        return Err(error);
                    }
                };
                let inbound = match receiver.recv_timeout(remaining.min(Duration::from_millis(100)))
                {
                    Ok(inbound) => inbound,
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        match try_wait_antigravity_child(&child) {
                            Ok(Some(status)) if !status.success() => {
                                return Err(RuntimeError::Transport(
                                    ANTIGRAVITY_RUNTIME_UNAVAILABLE.into(),
                                ));
                            }
                            Ok(Some(_)) => return Err(RuntimeError::TransportClosed),
                            Ok(None) => continue,
                            Err(_) => {
                                kill_antigravity_child(&child);
                                return Err(RuntimeError::Transport(
                                    ANTIGRAVITY_RUNTIME_UNAVAILABLE.into(),
                                ));
                            }
                        }
                    }
                    Err(mpsc::RecvTimeoutError::Disconnected) => {
                        return Err(RuntimeError::TransportClosed);
                    }
                };
                let value = match inbound {
                    AntigravityInbound::Value(value) => value,
                    AntigravityInbound::Error(error) => {
                        kill_antigravity_child(&child);
                        return Err(error);
                    }
                };
                match antigravity_event_type(&value) {
                    Some("init") => {
                        if !started {
                            started = true;
                            event_index += 1;
                            producer.push(runtime_started_event(
                                "antigravity",
                                "agy-stream-json",
                                request,
                                None,
                                None,
                            ))?;
                        }
                    }
                    Some("step_update") => {
                        if !started {
                            started = true;
                            event_index += 1;
                            producer.push(runtime_started_event(
                                "antigravity",
                                "agy-stream-json",
                                request,
                                None,
                                None,
                            ))?;
                        }
                        if let Some(delta) = antigravity_delta_text(&value) {
                            if !delta.is_empty() {
                                event_index += 1;
                                accumulated_text.push_str(delta);
                                producer.push(output_delta_event(
                                    "antigravity",
                                    request,
                                    None,
                                    None,
                                    event_index,
                                    delta,
                                ))?;
                            }
                        }
                        if let Some(reason) = antigravity_tool_failure_reason(&value) {
                            event_index += 1;
                            producer.push(terminal_event(
                                "antigravity",
                                "execution.failed",
                                request,
                                None,
                                None,
                                event_index,
                                Some(reason),
                            ))?;
                            return Ok(());
                        }
                    }
                    Some("result") => {
                        if let Some(final_text) = antigravity_result_text(&value) {
                            if !final_text.is_empty() && final_text != accumulated_text {
                                event_index += 1;
                                producer.push(output_delta_event(
                                    "antigravity",
                                    request,
                                    None,
                                    None,
                                    event_index,
                                    &final_text,
                                ))?;
                            }
                        }
                        let stop_reason = antigravity_stop_reason(&value)
                            .unwrap_or_default()
                            .to_ascii_lowercase();
                        let event_type = match stop_reason.as_str() {
                            "failed" | "error" | "cancelled" => "execution.failed",
                            _ => "execution.completed",
                        };
                        event_index += 1;
                        producer.push(terminal_event(
                            "antigravity",
                            event_type,
                            request,
                            None,
                            None,
                            event_index,
                            if event_type == "execution.failed" {
                                Some("provider_error")
                            } else {
                                None
                            },
                        ))?;
                        return Ok(());
                    }
                    Some(_) | None => {}
                }
            }
        })();
        kill_antigravity_child(&child);
        result
    }
}

fn validate_antigravity_request(request: &RuntimeRequest) -> Result<(), RuntimeError> {
    if request.timeout_ms == 0 || request.timeout_ms > MAX_RUNTIME_TIMEOUT_MS {
        return Err(RuntimeError::InvalidWorkspace);
    }
    if request.rendered_context.trim().is_empty() {
        return Err(RuntimeError::InvalidWorkspace);
    }
    Ok(())
}

enum AntigravityInbound {
    Value(Value),
    Error(RuntimeError),
}

fn spawn_antigravity_line_reader(
    stdout: impl Read + Send + 'static,
    sender: mpsc::SyncSender<AntigravityInbound>,
) {
    thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) => {
                    let _ = sender.send(AntigravityInbound::Error(RuntimeError::TransportClosed));
                    return;
                }
                Ok(_) if line.len() > MAX_ANTIGRAVITY_LINE_BYTES => {
                    let _ = sender.send(AntigravityInbound::Error(RuntimeError::Protocol(
                        ANTIGRAVITY_PROTOCOL_ERROR.into(),
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
                            if sender.send(AntigravityInbound::Value(value)).is_err() {
                                return;
                            }
                        }
                        Err(_) => {
                            let _ = sender.send(AntigravityInbound::Error(RuntimeError::Protocol(
                                ANTIGRAVITY_PROTOCOL_ERROR.into(),
                            )));
                            return;
                        }
                    }
                }
                Err(_) => {
                    let _ = sender.send(AntigravityInbound::Error(RuntimeError::TransportClosed));
                    return;
                }
            }
        }
    });
}

fn antigravity_event_type(value: &Value) -> Option<&str> {
    value
        .get("event")
        .and_then(Value::as_str)
        .or_else(|| value.get("type").and_then(Value::as_str))
}

fn antigravity_delta_text(value: &Value) -> Option<&str> {
    let body = value
        .get("step_update")
        .or_else(|| value.get("stepUpdate"))
        .unwrap_or(value);
    // 1.1.17 stream-json emits agent_response chunks in `text_delta`.
    let text = body
        .get("text_delta")
        .and_then(Value::as_str)
        .or_else(|| body.get("text").and_then(Value::as_str));
    if let Some(text) = text {
        return Some(text);
    }
    // Older/build variants place the chunk under `message.content`.
    body.get("message")
        .and_then(|message| message.get("content"))
        .and_then(|content| {
            if let Some(text) = content.get("text").and_then(Value::as_str) {
                return Some(text);
            }
            content
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|part| part.get("text").and_then(Value::as_str))
                .next()
        })
}

fn antigravity_result_text(value: &Value) -> Option<String> {
    let body = value
        .get("result")
        .or_else(|| value.get("resultData"))
        .unwrap_or(value);
    let text = body
        .get("response")
        .and_then(Value::as_str)
        .or_else(|| body.get("output").and_then(Value::as_str))
        .or_else(|| body.get("text").and_then(Value::as_str))
        .or_else(|| {
            body.get("message")
                .and_then(|message| message.get("content"))
                .and_then(|content| {
                    content
                        .get("text")
                        .and_then(Value::as_str)
                        .or_else(|| content.get("output").and_then(Value::as_str))
                })
        });
    text.map(str::to_owned)
}

fn antigravity_stop_reason(value: &Value) -> Option<&str> {
    let body = value.get("result").unwrap_or(value);
    body.get("status")
        .and_then(Value::as_str)
        .or_else(|| body.get("stopReason").and_then(Value::as_str))
        .or_else(|| body.get("stop_reason").and_then(Value::as_str))
}

fn antigravity_tool_failure_reason(value: &Value) -> Option<&'static str> {
    let body = value
        .get("step_update")
        .or_else(|| value.get("stepUpdate"))
        .unwrap_or(value);
    if body.get("step_type").and_then(Value::as_str) != Some("tool") {
        return None;
    }
    if body.get("state").and_then(Value::as_str) != Some("ERROR") {
        return None;
    }
    let message = body
        .pointer("/tool_info/error/message")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if message.to_ascii_lowercase().contains("permission") {
        Some("permission_denied")
    } else {
        Some("tool_error")
    }
}

fn antigravity_exit_error(stderr: &str) -> RuntimeError {
    let lowered = stderr.to_ascii_lowercase();
    if lowered.contains("sign in")
        || lowered.contains("login")
        || lowered.contains("authentication")
        || lowered.contains("auth")
        || lowered.contains("eligibility")
        || lowered.contains("not signed in")
        || lowered.contains("device")
        || lowered.contains("oauth")
        || lowered.contains("paste")
        || lowered.contains("agent execution terminated due to error")
    {
        RuntimeError::Authentication
    } else {
        RuntimeError::Transport(ANTIGRAVITY_RUNTIME_UNAVAILABLE.into())
    }
}

fn kill_antigravity_child(child: &Arc<Mutex<Child>>) {
    if let Ok(mut child) = child.lock() {
        let _ = child.kill();
        let _ = child.wait();
    }
}

fn try_wait_antigravity_child(
    child: &Arc<Mutex<Child>>,
) -> Result<Option<std::process::ExitStatus>, ()> {
    match child.lock() {
        Ok(mut child) => child.try_wait().map_err(|_| ()),
        Err(_) => Err(()),
    }
}

fn terminate_spawned_child(child: &mut Child) {
    let _ = child.kill();
    let deadline = Instant::now() + ANTIGRAVITY_CLEANUP_GRACE;
    while Instant::now() < deadline {
        match child.try_wait() {
            Ok(Some(_)) | Err(_) => return,
            Ok(None) => thread::sleep(Duration::from_millis(20)),
        }
    }
}

fn configure_antigravity_child_environment(command: &mut Command) {
    command.env_clear();
    for (key, value) in antigravity_child_environment_values(|key| std::env::var_os(key)) {
        command.env(key, value);
    }
}

const ANTIGRAVITY_CHILD_ENV_WHITELIST: &[&str] = &[
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
];

fn antigravity_child_environment_values(
    lookup: impl Fn(&str) -> Option<OsString>,
) -> Vec<(&'static str, OsString)> {
    ANTIGRAVITY_CHILD_ENV_WHITELIST
        .iter()
        .filter_map(|key| lookup(key).map(|value| (*key, value)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn ndjson_stream_conversion_extracts_deltas_and_result() {
        let init = json!({
            "event": "init",
            "conversation_id": "conv-1",
            "init": {"cwd": "C:\tmp", "tools": []}
        });
        assert_eq!(antigravity_event_type(&init), Some("init"));

        let step = json!({
            "event": "step_update",
            "step_update": {
                "conversation_id": "conv-1",
                "step_index": 2,
                "state": "DONE",
                "step_type": "agent_response",
                "text_delta": "Hello "
            }
        });
        assert_eq!(antigravity_event_type(&step), Some("step_update"));
        assert_eq!(antigravity_delta_text(&step), Some("Hello "));

        let step_nested = json!({
            "event": "step_update",
            "step_update": {
                "step_type": "agent_message",
                "message": {"role": "assistant", "content": [{"type": "text", "text": "world"}]}
            }
        });
        assert_eq!(antigravity_delta_text(&step_nested), Some("world"));

        let result = json!({
            "event": "result",
            "result": {
                "conversation_id": "conv-1",
                "status": "SUCCESS",
                "response": "Hello world"
            }
        });
        assert_eq!(antigravity_event_type(&result), Some("result"));
        assert_eq!(antigravity_result_text(&result), Some("Hello world".into()));
        assert_eq!(antigravity_stop_reason(&result), Some("SUCCESS"));
    }

    #[test]
    fn tool_failures_are_detected_as_terminal_errors() {
        let denied = json!({
            "event": "step_update",
            "step_update": {
                "step_index": 3,
                "state": "ERROR",
                "step_type": "tool",
                "tool_name": "run_command",
                "tool_info": {
                    "name": "run_command",
                    "parameters": {"CommandLine": "exit 1"},
                    "error": {
                        "type": "TOOL_ERROR",
                        "message": "permission check failed for command \"exit 1\": user denied permission"
                    }
                }
            }
        });
        assert_eq!(
            antigravity_tool_failure_reason(&denied),
            Some("permission_denied")
        );

        let failed = json!({
            "event": "result",
            "result": {"status": "ERROR", "error": "tool failed"}
        });
        assert_eq!(antigravity_stop_reason(&failed), Some("ERROR"));
    }

    #[test]
    fn auth_failures_are_classified_as_authentication() {
        assert!(matches!(
            antigravity_exit_error("Error: Please sign in to continue."),
            RuntimeError::Authentication
        ));
        assert!(matches!(
            antigravity_exit_error("Eligibility check failed"),
            RuntimeError::Authentication
        ));
        assert!(matches!(
            antigravity_exit_error("Open browser to complete OAuth device-code flow"),
            RuntimeError::Authentication
        ));
        assert!(matches!(
            antigravity_exit_error("some unrelated stderr"),
            RuntimeError::Transport(_)
        ));
    }

    #[test]
    fn request_validation_is_fail_closed() {
        let mut request = RuntimeRequest {
            execution_run_id: "run-1".into(),
            agent_identity_id: "agent-1".into(),
            connector_id: "local.antigravity".into(),
            model_id: None,
            context_manifest_id: "ctx".into(),
            rendered_context: "  ".into(),
            canonical_cwd: None,
            workspace_access: WorkspaceAccess::ReadOnly,
            timeout_ms: 1000,
            thread_policy: "default".into(),
            signed_scope: "scope".into(),
        };
        assert!(matches!(
            validate_antigravity_request(&request),
            Err(RuntimeError::InvalidWorkspace)
        ));
        request.rendered_context = "hello".into();
        request.timeout_ms = 0;
        assert!(matches!(
            validate_antigravity_request(&request),
            Err(RuntimeError::InvalidWorkspace)
        ));
        request.timeout_ms = 1000;
        assert!(validate_antigravity_request(&request).is_ok());
    }

    #[test]
    fn parses_agy_models_tsv_output() {
        let sample = concat!(
            "Fetching available models...
",
            "gemini-3.7-flash-high	Gemini 3.7 Flash (High)
",
            "gemini-3.7-flash-medium	Gemini 3.7 Flash (Medium)
",
            "claude-sonnet-4-6	Claude Sonnet 4.6 (Thinking)
",
            "gpt-oss-120b-medium	GPT-OSS 120B (Medium)
",
        );
        let models = parse_agy_models_output(sample);
        assert_eq!(models.len(), 4);
        assert_eq!(models[0].id, "gemini-3.7-flash-high");
        assert_eq!(models[0].display_name, "Gemini 3.7 Flash (High)");
        assert_eq!(models[2].id, "claude-sonnet-4-6");
        assert_eq!(models[3].id, "gpt-oss-120b-medium");
    }

    #[test]
    fn version_parser_rejects_help_without_version() {
        assert!(parse_agy_version(
            "Usage of agy.exe:
  --version"
        )
        .is_none());
        assert_eq!(parse_agy_version("1.1.17"), Some("1.1.17".into()));
    }
}
