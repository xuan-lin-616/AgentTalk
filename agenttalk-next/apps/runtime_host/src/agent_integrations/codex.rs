use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crate::discovery::verifiers::known::probe_codex_cli_surface;
use crate::{
    find_codex_desktop_binary, find_codex_on_process_path, is_real_regular_file,
    CodexAppServerConfig, CodexAppServerRuntime, RuntimeAdapter,
};

use super::{
    Integration, IntegrationConnectError, IntegrationDescriptor, IntegrationDetectOutcome,
    IntegrationInstalled, IntegrationLoginState, IntegrationVerification,
    IntegrationVerificationStatus, INTEGRATION_PROBE_TIMEOUT,
};

pub struct CodexIntegration;

fn descriptor() -> &'static IntegrationDescriptor {
    static DESCRIPTOR: OnceLock<IntegrationDescriptor> = OnceLock::new();
    DESCRIPTOR.get_or_init(|| IntegrationDescriptor {
        id: "local.codex".into(),
        display_name: "Codex".into(),
        category: "agent_runtime".into(),
        protocol: "codex-app-server".into(),
        runtime_type: "codex".into(),
        install_command: "npm install -g @openai/codex".into(),
        needs_consent: true,
    })
}

impl Integration for CodexIntegration {
    fn descriptor(&self) -> &IntegrationDescriptor {
        descriptor()
    }

    fn detect(&self) -> IntegrationDetectOutcome {
        let Some(executable) = find_codex_executable() else {
            return IntegrationDetectOutcome::NotInstalled;
        };
        let deadline = Instant::now() + INTEGRATION_PROBE_TIMEOUT;
        let cancelled = AtomicBool::new(false);
        let mut cwd = match CodexProbeCwd::create() {
            Ok(cwd) => cwd,
            Err(()) => return IntegrationDetectOutcome::NotInstalled,
        };
        let result = probe_codex_cli_surface(&executable, cwd.path(), deadline, &cancelled);
        let _ = cwd.cleanup();
        match result {
            Ok(version) => IntegrationDetectOutcome::Installed(IntegrationInstalled {
                version,
                // The local app-server surface is present. Account state is
                // intentionally not read from disk or environment.
                login_state: IntegrationLoginState::Unknown,
            }),
            Err(_) => IntegrationDetectOutcome::NotInstalled,
        }
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
                detail: Some("codex_cli_surface_missing".into()),
            };
        };
        match self.connect() {
            Ok(runtime) => {
                let discovery = runtime.discover();
                IntegrationVerification {
                    integration_id: self.descriptor().id.clone(),
                    status: IntegrationVerificationStatus::Verified,
                    login_state: installed.login_state,
                    protocol_major: Some(1),
                    version: discovery.version,
                    detail: Some("codex_app_server_initialize_ok".into()),
                }
            }
            Err(_) => IntegrationVerification {
                integration_id: self.descriptor().id.clone(),
                status: IntegrationVerificationStatus::Rejected,
                login_state: installed.login_state,
                protocol_major: Some(1),
                version: None,
                detail: Some("codex_app_server_initialize_failed".into()),
            },
        }
    }

    fn connect(&self) -> Result<Box<dyn RuntimeAdapter>, IntegrationConnectError> {
        let executable = find_codex_executable().ok_or(IntegrationConnectError::NotInstalled)?;
        let runtime = CodexAppServerRuntime::with_config(CodexAppServerConfig {
            binary_path: Some(executable),
            ..CodexAppServerConfig::default()
        });
        // Establish and close one initialize-only handshake now; this proves
        // the spawned command is a live app-server before returning a
        // RuntimeAdapter. Runtime/profile registration remains inert.
        runtime
            .ensure_available()
            .map_err(|_| IntegrationConnectError::ConnectFailed)?;
        Ok(Box::new(runtime))
    }
}

fn find_codex_executable() -> Option<PathBuf> {
    for variable in [
        "AGENTTALK_CODEX_BINARY",
        "CODEX_BINARY_PATH",
        "CODEX_BINARY",
    ] {
        if let Some(value) = std::env::var_os(variable) {
            let path = PathBuf::from(value);
            if is_real_regular_file(&path) {
                return Some(path);
            }
        }
    }
    if let Some(path) = find_codex_on_process_path() {
        return Some(path);
    }
    #[cfg(windows)]
    if let Some(local_app_data) = std::env::var_os("LOCALAPPDATA") {
        if let Some(path) = find_codex_desktop_binary(&PathBuf::from(local_app_data)) {
            return Some(path);
        }
    }
    None
}

struct CodexProbeCwd {
    path: PathBuf,
    cleaned: bool,
}

impl CodexProbeCwd {
    fn create() -> Result<Self, ()> {
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        for _ in 0..8 {
            let id = NEXT_ID.fetch_add(1, Ordering::AcqRel);
            let path = std::env::temp_dir().join(format!(
                "agenttalk-codex-integration-probe-{}-{timestamp}-{id}",
                std::process::id()
            ));
            match fs::create_dir(&path) {
                Ok(()) => {
                    return Ok(Self {
                        path,
                        cleaned: false,
                    });
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(_) => return Err(()),
            }
        }
        Err(())
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn cleanup(&mut self) -> Result<(), ()> {
        if self.cleaned {
            return Ok(());
        }
        fs::remove_dir(&self.path).map_err(|_| ())?;
        self.cleaned = true;
        Ok(())
    }
}

impl Drop for CodexProbeCwd {
    fn drop(&mut self) {
        if !self.cleaned {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}
