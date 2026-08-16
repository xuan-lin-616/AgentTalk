use std::collections::BTreeMap;
use std::sync::atomic::AtomicBool;
use std::time::Instant;

use agenttalk_domain::{
    CandidateProjection, DiscoveryDiagnostic, DiscoveryDiagnosticCode, WorkspaceAccess,
};

use crate::discovery::verifiers::acp::{
    self, AcpClassification, AcpClassificationError, AcpImportPlanMetadata, AcpPassiveObservation,
    AcpTargetBinding, AcpVerificationConsent, AcpVerificationResult,
};
use crate::discovery::Observation;
use crate::{
    AdapterManifest, RuntimeAdapter, RuntimeCapabilities, RuntimeDiscovery, RuntimeError,
    RuntimeEvent, RuntimeHealth, RuntimeRequest,
};

#[derive(Clone, Copy, Debug, Default)]
pub struct AcpProtocolAdapterFactory;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AcpFactoryError {
    BindingMismatch,
    NotVerified,
}

/// A non-serializable Core-owned ACP verification session. It is constructed
/// exclusively from passive discovery sidecar evidence and exposes only
/// renderer-safe projections and typed diagnostics.
#[derive(Clone)]
pub struct AcpDiscoverySession {
    classifications: BTreeMap<String, AcpClassification>,
    projections: Vec<CandidateProjection>,
    diagnostics: Vec<DiscoveryDiagnostic>,
}

impl std::fmt::Debug for AcpDiscoverySession {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AcpDiscoverySession")
            .field("projections", &self.projections)
            .field("diagnostics", &self.diagnostics)
            .finish()
    }
}

impl AcpDiscoverySession {
    pub fn projections(&self) -> &[CandidateProjection] {
        &self.projections
    }

    pub fn diagnostics(&self) -> &[DiscoveryDiagnostic] {
        &self.diagnostics
    }

    pub fn verify(
        &self,
        consent: &AcpVerificationConsent,
        deadline: Instant,
        cancelled: &AtomicBool,
    ) -> Result<AcpVerificationResult, AcpClassificationError> {
        let classification = self
            .classifications
            .get(consent.candidate_id())
            .ok_or(AcpClassificationError::ObservationMismatch)?;
        Ok(acp::verify(
            classification,
            Some(consent),
            deadline,
            cancelled,
        ))
    }

    pub fn instantiate(
        &self,
        consent: &AcpVerificationConsent,
        verification: &AcpVerificationResult,
    ) -> Result<Box<dyn RuntimeAdapter>, AcpFactoryError> {
        let classification = self
            .classifications
            .get(consent.candidate_id())
            .ok_or(AcpFactoryError::BindingMismatch)?;
        AcpProtocolAdapterFactory.instantiate(classification, verification)
    }

    /// Creates only a renderer-safe, read-only import-plan input. The
    /// underlying executable identity is checked again before metadata is
    /// returned, and remains private to the ACP session.
    pub fn import_plan_metadata(
        &self,
        consent: &AcpVerificationConsent,
        verification: &AcpVerificationResult,
        deadline: Instant,
        cancelled: &AtomicBool,
    ) -> Result<AcpImportPlanMetadata, AcpClassificationError> {
        let classification = self
            .classifications
            .get(consent.candidate_id())
            .ok_or(AcpClassificationError::ObservationMismatch)?;
        acp::import_plan_metadata(classification, consent, verification, deadline, cancelled)
    }
}

impl AcpProtocolAdapterFactory {
    pub fn classify(
        &self,
        manifest: &AdapterManifest,
        observation: AcpPassiveObservation,
    ) -> Result<AcpClassification, AcpClassificationError> {
        acp::classify(manifest, observation)
    }

    pub fn verify(
        &self,
        classification: &AcpClassification,
        consent: Option<&AcpVerificationConsent>,
        deadline: Instant,
        cancelled: &AtomicBool,
    ) -> AcpVerificationResult {
        acp::verify(classification, consent, deadline, cancelled)
    }

    pub fn instantiate(
        &self,
        classification: &AcpClassification,
        verification: &AcpVerificationResult,
    ) -> Result<Box<dyn RuntimeAdapter>, AcpFactoryError> {
        let Some(verified_binding) = verification.binding() else {
            return Err(AcpFactoryError::NotVerified);
        };
        if verified_binding != classification.binding() {
            return Err(AcpFactoryError::BindingMismatch);
        }
        Ok(Box::new(AcpDeferredAdapter::new(
            classification.binding().clone(),
            classification.candidate_id().to_owned(),
        )))
    }

    pub(crate) fn classify_passive_observations(
        &self,
        observations: &BTreeMap<String, Vec<Observation>>,
        manifests: &[AdapterManifest],
        deadline: Instant,
        cancelled: &AtomicBool,
    ) -> AcpDiscoverySession {
        let mut classifications = BTreeMap::new();
        let mut projections = Vec::new();
        let mut diagnostics = Vec::new();

        for (candidate_id, observations) in observations {
            if cancelled.load(std::sync::atomic::Ordering::Acquire) || Instant::now() >= deadline {
                break;
            }
            let mut candidate_classifications = Vec::new();
            for observation in observations {
                let passive = match AcpPassiveObservation::from_passive_observation(
                    candidate_id,
                    observation,
                    deadline,
                    cancelled,
                ) {
                    Ok(passive) => passive,
                    Err(_) => continue,
                };
                for manifest in manifests {
                    if cancelled.load(std::sync::atomic::Ordering::Acquire)
                        || Instant::now() >= deadline
                    {
                        break;
                    }
                    if let Ok(classification) = self.classify(manifest, passive.clone()) {
                        candidate_classifications.push(classification);
                    }
                }
            }
            // One classification per manifest, preferring the observation with
            // an independent identity (a real user selection or an exact
            // pinned hash) over a filename-only heuristic match from the same
            // candidate.
            let mut by_manifest: BTreeMap<String, AcpClassification> = BTreeMap::new();
            for classification in candidate_classifications {
                let manifest_id = classification.manifest_id().to_owned();
                if classification.has_independent_identity()
                    && by_manifest
                        .get(&manifest_id)
                        .is_some_and(|existing| !existing.has_independent_identity())
                {
                    by_manifest.insert(manifest_id, classification);
                } else {
                    by_manifest.entry(manifest_id).or_insert(classification);
                }
            }
            let mut candidate_classifications: Vec<_> = by_manifest.into_values().collect();
            match candidate_classifications.len() {
                0 => {}
                1 => {
                    let classification = candidate_classifications
                        .pop()
                        .expect("one ACP classification");
                    projections.push(classification.projection().clone());
                    classifications.insert(candidate_id.clone(), classification);
                }
                _ => diagnostics.push(DiscoveryDiagnostic {
                    source_kind: observations
                        .first()
                        .map(|observation| observation.source_kind)
                        .unwrap_or(agenttalk_domain::ObservationSourceKind::ExecutableInventory),
                    code: DiscoveryDiagnosticCode::InvalidIdentity,
                }),
            }
        }
        projections.sort_by(|left, right| left.candidate_id.cmp(&right.candidate_id));
        AcpDiscoverySession {
            classifications,
            projections,
            diagnostics,
        }
    }
}

struct AcpDeferredAdapter {
    binding: AcpTargetBinding,
    runtime_id: String,
}

impl AcpDeferredAdapter {
    fn new(binding: AcpTargetBinding, candidate_id: String) -> Self {
        Self {
            binding,
            runtime_id: format!("acp-deferred-{candidate_id}"),
        }
    }
}

impl RuntimeAdapter for AcpDeferredAdapter {
    fn id(&self) -> &str {
        &self.runtime_id
    }

    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities {
            streaming: false,
            cancel: false,
            filesystem: false,
            shell: false,
        }
    }

    fn discover(&self) -> RuntimeDiscovery {
        RuntimeDiscovery {
            runtime_id: self.runtime_id.clone(),
            version: Some("acp-initialize-only-v1".into()),
            owned: false,
        }
    }

    fn health(&self) -> RuntimeHealth {
        RuntimeHealth {
            runtime_id: self.runtime_id.clone(),
            status: "unverified".into(),
            detail: Some(
                "ACP execution is deferred until a later owner-gated runtime phase".into(),
            ),
        }
    }

    fn execute(&self, request: &RuntimeRequest) -> Result<Vec<RuntimeEvent>, RuntimeError> {
        if request.workspace_access == WorkspaceAccess::WorkspaceWrite {
            return Err(RuntimeError::Permission);
        }
        let _ = &self.binding;
        Err(RuntimeError::Unsupported)
    }

    fn cancel(&self, _request: &RuntimeRequest) -> Result<RuntimeEvent, RuntimeError> {
        Err(RuntimeError::Unsupported)
    }
}
