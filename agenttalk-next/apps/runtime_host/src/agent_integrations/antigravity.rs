use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::OnceLock;
use std::thread;
use std::time::{Duration, Instant};

use crate::{is_real_regular_file, RuntimeAdapter};

use super::{
    Integration, IntegrationConnectError, IntegrationDescriptor, IntegrationDetectOutcome,
    IntegrationInstalled, IntegrationLoginState, IntegrationVerification,
    IntegrationVerificationStatus, INTEGRATION_PROBE_TIMEOUT,
};

pub struct AntigravityIntegration;

fn descriptor() -> &'static IntegrationDescriptor {
    static DESCRIPTOR: OnceLock<IntegrationDescriptor> = OnceLock::new();
    DESCRIPTOR.get_or_init(|| IntegrationDescriptor {
        id: "local.antigravity".into(),
        display_name: "Antigravity".into(),
        category: "agent_runtime".into(),
        protocol: "needs-adapter".into(),
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
        // The public `agy` CLI shape was not verifiable on this machine and
        // no official protocol surface could be confirmed. Try both common
        // probe shapes and report only what the local binary proves.
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
        IntegrationVerification {
            integration_id: self.descriptor().id.clone(),
            status: IntegrationVerificationStatus::NeedsAdapter,
            login_state,
            protocol_major: None,
            version: None,
            detail: Some("agy_protocol_unconfirmed_needs_adapter".into()),
        }
    }

    fn connect(&self) -> Result<Box<dyn RuntimeAdapter>, IntegrationConnectError> {
        Err(IntegrationConnectError::NeedsAdapter)
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
    let text = std::fs::read_to_string(script).ok()?;
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
    // Prefer `--version`, fall back to `--help` for CLIs that do not expose a
    // version flag. The result is treated as a display-only version string.
    for args in [&["--version"][..], &["--help"][..], &["version"][..]] {
        if let Some(output) = run_agy_probe(executable, args) {
            if let Some(version) = parse_agy_version(&output) {
                return Some(version);
            }
        }
    }
    None
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
    let reader = |mut stream: std::process::ChildStdout| {
        thread::spawn(move || {
            use std::io::Read;
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
    };
    let _ = stderr;
    let stdout_reader = reader(stdout);
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
    Some(String::from_utf8_lossy(&stdout).into_owned())
}

fn parse_agy_version(output: &str) -> Option<String> {
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
