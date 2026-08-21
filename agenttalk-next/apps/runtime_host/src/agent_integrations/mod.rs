//! Deterministic local agent integration catalog.
//!
//! This module is the default entry point for first-party local agents. Each
//! integration owns its detection, install recommendation, verification, and
//! connection adapter. The generic local Discovery scanner remains the
//! fallback for unknown/custom agents and is intentionally not reimplemented
//! here.

mod antigravity;
mod claude;
mod codex;

pub use antigravity::{AntigravityConfig, AntigravityIntegration, AntigravityRuntime};
pub use claude::{ClaudeCodeConfig, ClaudeCodeIntegration, ClaudeCodeRuntime};
pub use codex::CodexIntegration;

use std::time::Duration;

use agenttalk_domain::ObservationSourceKind;
use serde::Serialize;

use crate::{LocalConnectorCandidate, RuntimeAdapter};

pub const INTEGRATION_PROBE_TIMEOUT: Duration = Duration::from_secs(8);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrationLoginState {
    LoggedIn,
    LoginRequired,
    Unknown,
}

impl IntegrationLoginState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LoggedIn => "logged_in",
            Self::LoginRequired => "login_required",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IntegrationInstalled {
    pub version: String,
    pub login_state: IntegrationLoginState,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrationDetectOutcome {
    Installed(IntegrationInstalled),
    NotInstalled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrationVerificationStatus {
    Verified,
    AuthRequired,
    Rejected,
    NeedsAdapter,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IntegrationVerification {
    pub integration_id: String,
    pub status: IntegrationVerificationStatus,
    pub login_state: IntegrationLoginState,
    pub protocol_major: Option<u16>,
    pub version: Option<String>,
    pub detail: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IntegrationInstall {
    pub command: String,
    pub needs_consent: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IntegrationDescriptor {
    pub id: String,
    pub display_name: String,
    pub category: String,
    pub protocol: String,
    pub runtime_type: String,
    pub install_command: String,
    pub needs_consent: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IntegrationConnectError {
    NotInstalled,
    NeedsAdapter,
    AuthenticationRequired,
    ConnectFailed,
}

impl std::fmt::Display for IntegrationConnectError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::NotInstalled => "integration is not installed",
            Self::NeedsAdapter => "integration protocol adapter is not implemented yet",
            Self::AuthenticationRequired => "integration requires local CLI login",
            Self::ConnectFailed => "integration connection handshake failed",
        })
    }
}

impl std::error::Error for IntegrationConnectError {}

/// An integration entry owns every protocol-specific decision. Detection is
/// deterministic (`--version` / `--help` probes), installation is only a
/// consent-gated command recommendation, and `connect` returns a real
/// [`RuntimeAdapter`]; it must never fall back to generic executable scanning.
pub trait Integration: Send + Sync {
    fn descriptor(&self) -> &IntegrationDescriptor;

    fn detect(&self) -> IntegrationDetectOutcome;

    fn install(&self) -> IntegrationInstall {
        IntegrationInstall {
            command: self.descriptor().install_command.clone(),
            needs_consent: self.descriptor().needs_consent,
        }
    }

    fn verify(&self) -> IntegrationVerification;

    fn connect(&self) -> Result<Box<dyn RuntimeAdapter>, IntegrationConnectError>;
}

static INTEGRATIONS: &[&dyn Integration] = &[
    &CodexIntegration,
    &ClaudeCodeIntegration,
    &AntigravityIntegration,
];

pub fn list_integrations() -> Vec<IntegrationDescriptor> {
    INTEGRATIONS
        .iter()
        .map(|integration| integration.descriptor().clone())
        .collect()
}

pub fn integration(id: &str) -> Option<&'static dyn Integration> {
    INTEGRATIONS
        .iter()
        .find(|integration| integration.descriptor().id == id)
        .copied()
}

pub fn detect(integration: &dyn Integration) -> IntegrationDetectOutcome {
    integration.detect()
}

pub fn detect_by_id(id: &str) -> Option<IntegrationDetectOutcome> {
    integration(id).map(detect)
}

pub fn install(integration: &dyn Integration) -> IntegrationInstall {
    integration.install()
}

pub fn install_by_id(id: &str) -> Option<IntegrationInstall> {
    integration(id).map(install)
}

pub fn verify(integration: &dyn Integration) -> IntegrationVerification {
    integration.verify()
}

pub fn verify_by_id(id: &str) -> Option<IntegrationVerification> {
    integration(id).map(verify)
}

pub fn connect(
    integration: &dyn Integration,
) -> Result<Box<dyn RuntimeAdapter>, IntegrationConnectError> {
    integration.connect()
}

pub fn connect_by_id(id: &str) -> Option<Result<Box<dyn RuntimeAdapter>, IntegrationConnectError>> {
    integration(id).map(connect)
}

/// Maps the integration catalog onto the frozen `connector.discover` DTO.
///
/// This is deliberately a projection for the existing IPC schema: install
/// commands and rich login state remain in the integration module API, while
/// the legacy DTO keeps its stable fields. Not-installed entries are included
/// as `unavailable` so the renderer can show all three catalog rows without a
/// schema change.
pub fn discover_agent_integrations() -> Vec<LocalConnectorCandidate> {
    let mut candidates = INTEGRATIONS
        .iter()
        .map(|integration| {
            let descriptor = integration.descriptor();
            let (availability, models, catalog_revision) = match integration.detect() {
                IntegrationDetectOutcome::Installed(installed) => {
                    let availability = match installed.login_state {
                        IntegrationLoginState::LoggedIn => "available",
                        IntegrationLoginState::LoginRequired => "authentication_required",
                        IntegrationLoginState::Unknown => "unconfigured",
                    };
                    (availability, Vec::new(), Some(installed.version))
                }
                IntegrationDetectOutcome::NotInstalled => ("unavailable", Vec::new(), None),
            };
            LocalConnectorCandidate {
                connector_id: descriptor.id.clone(),
                runtime_type: descriptor.runtime_type.clone(),
                display_name: descriptor.display_name.clone(),
                availability: availability.into(),
                models,
                catalog_revision,
                source: ObservationSourceKind::ExecutableInventory,
                requires_configuration: true,
            }
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| left.connector_id.cmp(&right.connector_id));
    candidates
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integration_catalog_is_stable_and_ordered() {
        let ids = list_integrations()
            .into_iter()
            .map(|descriptor| descriptor.id)
            .collect::<Vec<_>>();
        assert_eq!(
            ids,
            vec![
                "local.codex".to_owned(),
                "local.claude-code".to_owned(),
                "local.antigravity".to_owned()
            ]
        );
    }

    #[test]
    fn discovery_projection_keeps_all_catalog_entries_visible() {
        let candidates = discover_agent_integrations();
        assert_eq!(candidates.len(), 3);
        let ids = candidates
            .into_iter()
            .map(|candidate| candidate.connector_id)
            .collect::<Vec<_>>();
        assert_eq!(
            ids,
            vec![
                "local.antigravity".to_owned(),
                "local.claude-code".to_owned(),
                "local.codex".to_owned()
            ]
        );
    }
}
