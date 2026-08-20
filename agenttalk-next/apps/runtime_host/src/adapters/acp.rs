use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use agenttalk_domain::{
    CandidateProjection, DiscoveryDiagnostic, DiscoveryDiagnosticCode, WorkspaceAccess,
};

use crate::discovery::verifiers::acp::{
    self, AcpClassification, AcpClassificationError, AcpImportPlanMetadata, AcpPassiveObservation,
    AcpVerificationConsent, AcpVerificationResult,
};
use crate::discovery::{ManagedChild, ManagedDirectStdioSpec, Observation};
use crate::{
    open_verified_executable_guard, AdapterManifest, ManifestLaunch, RuntimeAdapter,
    RuntimeCapabilities, RuntimeDiscovery, RuntimeError, RuntimeEvent, RuntimeEventStream,
    RuntimeHealth, RuntimeRequest, DEFAULT_RUNTIME_STREAM_CAPACITY,
};
use serde_json::{json, Value};

const MAX_ACP_EXECUTION_FRAME_BYTES: usize = 1024 * 1024;
const MAX_ACP_EXECUTION_OUTPUT_BYTES: usize = 1024 * 1024;
const ACP_EXECUTION_POLL: Duration = Duration::from_millis(5);
const ACP_EXECUTION_CLEANUP: Duration = Duration::from_secs(2);

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
        Ok(Box::new(AcpDeferredAdapter::new(classification.clone())))
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

        let manifest_executable_names = manifests
            .iter()
            .flat_map(|manifest| manifest.match_rules.executable_names.iter())
            .map(|name| name.to_lowercase())
            .collect::<std::collections::BTreeSet<_>>();
        let mut ordered_candidate_ids = observations.keys().cloned().collect::<Vec<_>>();
        ordered_candidate_ids.sort_by_key(|candidate_id| {
            let hits_manifest_name = observations
                .get(candidate_id)
                .into_iter()
                .flatten()
                .filter_map(|observation| {
                    observation.executable_locator().and_then(|path| {
                        path.file_name()
                            .and_then(|name| name.to_str())
                            .map(|name| name.to_lowercase())
                    })
                })
                .any(|name| manifest_executable_names.contains(&name));
            (u8::from(!hits_manifest_name), candidate_id.clone())
        });
        for candidate_id in ordered_candidate_ids {
            if cancelled.load(std::sync::atomic::Ordering::Acquire) || Instant::now() >= deadline {
                break;
            }
            let Some(observations) = observations.get(&candidate_id) else {
                continue;
            };
            let mut candidate_classifications = Vec::new();
            for observation in observations {
                let passive = match AcpPassiveObservation::from_passive_observation(
                    &candidate_id,
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
    classification: AcpClassification,
    runtime_id: String,
    active_cancellations: Arc<Mutex<BTreeMap<String, Arc<AtomicBool>>>>,
}

impl AcpDeferredAdapter {
    fn new(classification: AcpClassification) -> Self {
        Self {
            runtime_id: format!("acp-owned-{}", classification.candidate_id()),
            classification,
            active_cancellations: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    fn begin_execution(&self, execution_run_id: &str) -> Result<Arc<AtomicBool>, RuntimeError> {
        let mut active = self
            .active_cancellations
            .lock()
            .map_err(|_| RuntimeError::Transport("acp_execution_state_poisoned".into()))?;
        if active.contains_key(execution_run_id) {
            return Err(RuntimeError::Protocol(
                "acp_execution_already_active".into(),
            ));
        }
        let cancelled = Arc::new(AtomicBool::new(false));
        active.insert(execution_run_id.to_owned(), Arc::clone(&cancelled));
        Ok(cancelled)
    }

    fn finish_execution(&self, execution_run_id: &str) {
        if let Ok(mut active) = self.active_cancellations.lock() {
            active.remove(execution_run_id);
        }
    }
}

impl RuntimeAdapter for AcpDeferredAdapter {
    fn id(&self) -> &str {
        &self.runtime_id
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
        RuntimeDiscovery {
            runtime_id: self.runtime_id.clone(),
            version: Some("acp-owned-one-shot-v1".into()),
            owned: true,
        }
    }

    fn health(&self) -> RuntimeHealth {
        RuntimeHealth {
            runtime_id: self.runtime_id.clone(),
            status: "ready".into(),
            detail: None,
        }
    }

    fn execute(&self, request: &RuntimeRequest) -> Result<Vec<RuntimeEvent>, RuntimeError> {
        validate_acp_execution_request(request)?;
        let cancelled = self.begin_execution(&request.execution_run_id)?;
        let result = run_owned_acp_execution(
            &self.classification,
            &self.runtime_id,
            request,
            cancelled.as_ref(),
        );
        self.finish_execution(&request.execution_run_id);
        result
    }

    fn stream_events_with_capacity(
        &self,
        request: &RuntimeRequest,
        capacity: usize,
    ) -> Result<RuntimeEventStream, RuntimeError> {
        validate_acp_execution_request(request)?;
        let cancelled = self.begin_execution(&request.execution_run_id)?;
        let cancel_for_callback = Arc::clone(&cancelled);
        let active = Arc::clone(&self.active_cancellations);
        let classification = self.classification.clone();
        let runtime_id = self.runtime_id.clone();
        let request = request.clone();
        RuntimeEventStream::spawn_with_cancel(
            capacity,
            move || cancel_for_callback.store(true, Ordering::Release),
            move |producer| {
                let result = run_owned_acp_execution(
                    &classification,
                    &runtime_id,
                    &request,
                    cancelled.as_ref(),
                );
                if let Ok(mut active) = active.lock() {
                    active.remove(&request.execution_run_id);
                }
                for event in result? {
                    producer.push(event)?;
                }
                Ok(())
            },
        )
    }

    fn stream_events(&self, request: &RuntimeRequest) -> Result<RuntimeEventStream, RuntimeError> {
        self.stream_events_with_capacity(request, DEFAULT_RUNTIME_STREAM_CAPACITY)
    }

    fn cancel(&self, request: &RuntimeRequest) -> Result<RuntimeEvent, RuntimeError> {
        let active = self
            .active_cancellations
            .lock()
            .map_err(|_| RuntimeError::Transport("acp_execution_state_poisoned".into()))?;
        let cancelled = active
            .get(&request.execution_run_id)
            .ok_or(RuntimeError::Cancelled)?;
        cancelled.store(true, Ordering::Release);
        Ok(runtime_event(
            &self.runtime_id,
            request,
            None,
            0,
            "execution.cancelled",
            json!({"reason":"user"}),
        ))
    }
}

fn validate_acp_execution_request(request: &RuntimeRequest) -> Result<(), RuntimeError> {
    if request.workspace_access != WorkspaceAccess::WorkspaceWrite {
        return Err(RuntimeError::Permission);
    }
    let cwd = request
        .canonical_cwd
        .as_deref()
        .map(Path::new)
        .ok_or(RuntimeError::InvalidWorkspace)?;
    if !cwd.is_absolute() || !cwd.is_dir() || request.rendered_context.trim().is_empty() {
        return Err(RuntimeError::InvalidWorkspace);
    }
    if request.timeout_ms == 0 || request.rendered_context.len() > MAX_ACP_EXECUTION_OUTPUT_BYTES {
        return Err(RuntimeError::Protocol("acp_request_bounds".into()));
    }
    Ok(())
}

fn run_owned_acp_execution(
    classification: &AcpClassification,
    runtime_id: &str,
    request: &RuntimeRequest,
    cancelled: &AtomicBool,
) -> Result<Vec<RuntimeEvent>, RuntimeError> {
    let cwd = PathBuf::from(
        request
            .canonical_cwd
            .as_deref()
            .ok_or(RuntimeError::InvalidWorkspace)?,
    );
    let deadline = Instant::now()
        + Duration::from_millis(request.timeout_ms.min(crate::MAX_RUNTIME_TIMEOUT_MS));
    let guard = open_verified_executable_guard(classification.executable(), deadline, cancelled)
        .map_err(|_| RuntimeError::Transport("acp_identity_recheck_failed".into()))?;
    if guard.fingerprint.stable_identity != classification.executable_identity()
        || guard.fingerprint.content_sha256 != classification.executable_sha256()
    {
        return Err(RuntimeError::Transport("acp_identity_mismatch".into()));
    }
    let ManifestLaunch::Direct {
        args,
        environment_allowlist,
        credential_environment,
        ..
    } = classification.launch()
    else {
        return Err(RuntimeError::Unsupported);
    };
    let credential_values = credential_environment
        .iter()
        .filter_map(|name| std::env::var(name).ok())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    let mut child = ManagedChild::spawn_direct(&ManagedDirectStdioSpec {
        executable: classification.executable().to_owned(),
        args: args.clone(),
        current_dir: cwd,
        environment_allowlist: environment_allowlist.clone(),
        credential_environment: credential_environment.clone(),
    })
    .map_err(|_| RuntimeError::Transport("acp_launch_failed".into()))?;
    drop(guard);
    let mut stdin = child.take_stdin();
    let stdout = child.take_stdout();
    let stderr = child.take_stderr();
    let (frame_tx, frame_rx) = mpsc::sync_channel(128);

    thread::scope(|scope| {
        let stdout_thread = scope.spawn(move || read_acp_execution_frames(stdout, frame_tx));
        let stderr_thread = scope.spawn(move || read_bounded_acp_stderr(stderr));
        let protocol = AcpProtocolContext {
            runtime_id,
            request,
            cancelled,
            deadline,
            credential_values: &credential_values,
        };
        let outcome = run_acp_protocol(&mut child, &mut stdin, &frame_rx, &protocol);
        drop(stdin);
        let cleanup_ok = child.terminate(Instant::now() + ACP_EXECUTION_CLEANUP);
        let stdout_ok = stdout_thread.join().is_ok_and(|result| result.is_ok());
        let stderr_result = stderr_thread
            .join()
            .map_err(|_| RuntimeError::Transport("acp_stderr_join_failed".into()))?;
        if !cleanup_ok {
            return Err(RuntimeError::Transport("acp_cleanup_failed".into()));
        }
        if !stdout_ok {
            return Err(RuntimeError::Protocol("acp_stdout_invalid".into()));
        }
        let stderr_bytes = stderr_result?;
        if !stderr_bytes.iter().all(u8::is_ascii_whitespace) {
            return Err(RuntimeError::Transport("acp_stderr_output".into()));
        }
        outcome
    })
}

struct AcpProtocolContext<'a> {
    runtime_id: &'a str,
    request: &'a RuntimeRequest,
    cancelled: &'a AtomicBool,
    deadline: Instant,
    credential_values: &'a [String],
}

fn run_acp_protocol(
    child: &mut ManagedChild,
    stdin: &mut impl Write,
    frames: &mpsc::Receiver<Result<Value, RuntimeError>>,
    context: &AcpProtocolContext<'_>,
) -> Result<Vec<RuntimeEvent>, RuntimeError> {
    write_acp_frame(
        stdin,
        &json!({
            "jsonrpc":"2.0", "id":0, "method":"initialize",
            "params":{"protocolVersion":1,"clientCapabilities":{},"clientInfo":{"name":"AgentTalk","version":"1"}}
        }),
    )?;
    let initialized = wait_for_acp_response(
        child,
        stdin,
        frames,
        0,
        None,
        context.cancelled,
        context.deadline,
    )?;
    let result = response_result(&initialized)?;
    if result.get("protocolVersion").and_then(Value::as_u64) != Some(1)
        || result
            .get("authMethods")
            .and_then(Value::as_array)
            .is_none_or(|methods| !methods.is_empty())
    {
        return Err(RuntimeError::Protocol("acp_initialize_mismatch".into()));
    }

    write_acp_frame(
        stdin,
        &json!({
            "jsonrpc":"2.0", "id":1, "method":"session/new",
            "params":{"cwd":context.request.canonical_cwd,"mcpServers":[]}
        }),
    )?;
    let new_session = wait_for_acp_response(
        child,
        stdin,
        frames,
        1,
        None,
        context.cancelled,
        context.deadline,
    )?;
    let session_id = response_result(&new_session)?
        .get("sessionId")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty() && value.len() <= 160)
        .ok_or_else(|| RuntimeError::Protocol("acp_session_id_invalid".into()))?
        .to_owned();

    write_acp_frame(
        stdin,
        &json!({
            "jsonrpc":"2.0", "id":2, "method":"session/prompt",
            "params":{"sessionId":session_id,"prompt":[{"type":"text","text":context.request.rendered_context}]}
        }),
    )?;

    let started_at = now_ms();
    let mut events = vec![runtime_event(
        context.runtime_id,
        context.request,
        Some(&session_id),
        0,
        "runtime.started",
        json!({"transport":"acp","protocolMajor":1}),
    )];
    let mut output = String::new();
    let mut sequence = 1u64;
    loop {
        let frame = next_acp_frame(
            child,
            frames,
            context.cancelled,
            context.deadline,
            Some((&session_id, stdin)),
        )?;
        if frame.get("id").and_then(Value::as_u64) == Some(2) {
            let stop_reason = response_result(&frame)?
                .get("stopReason")
                .and_then(Value::as_str)
                .ok_or_else(|| RuntimeError::Protocol("acp_stop_reason_missing".into()))?;
            if stop_reason == "cancelled" {
                return Err(RuntimeError::Cancelled);
            }
            if stop_reason != "end_turn" {
                return Err(RuntimeError::Provider("acp_prompt_failed".into()));
            }
            events.push(runtime_event(
                context.runtime_id,
                context.request,
                Some(&session_id),
                sequence,
                "execution.completed",
                json!({"output":output,"stopReason":stop_reason,"elapsedMs":now_ms().saturating_sub(started_at)}),
            ));
            return Ok(events);
        }
        if let Some(chunk) = agent_message_chunk(&frame) {
            if context
                .credential_values
                .iter()
                .any(|credential| chunk.contains(credential))
            {
                return Err(RuntimeError::Protocol("acp_credential_leak".into()));
            }
            if output.len().saturating_add(chunk.len()) > MAX_ACP_EXECUTION_OUTPUT_BYTES {
                return Err(RuntimeError::Protocol("acp_output_oversized".into()));
            }
            output.push_str(chunk);
            events.push(runtime_event(
                context.runtime_id,
                context.request,
                Some(&session_id),
                sequence,
                "output.delta",
                json!({"delta":chunk}),
            ));
            sequence += 1;
        }
    }
}

fn wait_for_acp_response(
    child: &mut ManagedChild,
    stdin: &mut impl Write,
    frames: &mpsc::Receiver<Result<Value, RuntimeError>>,
    expected_id: u64,
    session_id: Option<&str>,
    cancelled: &AtomicBool,
    deadline: Instant,
) -> Result<Value, RuntimeError> {
    loop {
        let frame = next_acp_frame(
            child,
            frames,
            cancelled,
            deadline,
            session_id.map(|id| (id, &mut *stdin)),
        )?;
        if frame.get("id").and_then(Value::as_u64) == Some(expected_id) {
            return Ok(frame);
        }
    }
}

fn next_acp_frame<W: Write>(
    child: &mut ManagedChild,
    frames: &mpsc::Receiver<Result<Value, RuntimeError>>,
    cancelled: &AtomicBool,
    deadline: Instant,
    mut cancellation: Option<(&str, &mut W)>,
) -> Result<Value, RuntimeError> {
    loop {
        if cancelled.load(Ordering::Acquire) {
            if let Some((session_id, stdin)) = cancellation.as_mut() {
                let _ = write_acp_frame(
                    *stdin,
                    &json!({"jsonrpc":"2.0","method":"session/cancel","params":{"sessionId":*session_id}}),
                );
            }
            return Err(RuntimeError::Cancelled);
        }
        if Instant::now() >= deadline {
            return Err(RuntimeError::Timeout);
        }
        match frames.recv_timeout(ACP_EXECUTION_POLL) {
            Ok(frame) => return frame,
            Err(mpsc::RecvTimeoutError::Timeout) => match child.try_wait() {
                Ok(Some(status)) if !status.success() => {
                    return Err(RuntimeError::Transport("acp_process_failed".into()))
                }
                Ok(Some(_)) => return Err(RuntimeError::TransportClosed),
                Ok(None) => {}
                Err(_) => return Err(RuntimeError::Transport("acp_wait_failed".into())),
            },
            Err(mpsc::RecvTimeoutError::Disconnected) => return Err(RuntimeError::TransportClosed),
        }
    }
}

fn response_result(frame: &Value) -> Result<&Value, RuntimeError> {
    if frame.get("error").is_some() {
        return Err(RuntimeError::Provider("acp_request_failed".into()));
    }
    frame
        .get("result")
        .ok_or_else(|| RuntimeError::Protocol("acp_result_missing".into()))
}

fn agent_message_chunk(frame: &Value) -> Option<&str> {
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

fn write_acp_frame(writer: &mut impl Write, frame: &Value) -> Result<(), RuntimeError> {
    let mut bytes = serde_json::to_vec(frame)
        .map_err(|_| RuntimeError::Protocol("acp_frame_encode_failed".into()))?;
    bytes.push(b'\n');
    writer
        .write_all(&bytes)
        .and_then(|_| writer.flush())
        .map_err(|_| RuntimeError::Transport("acp_write_failed".into()))
}

fn read_acp_execution_frames(
    mut reader: impl Read,
    sender: mpsc::SyncSender<Result<Value, RuntimeError>>,
) -> Result<(), RuntimeError> {
    let mut pending = Vec::new();
    let mut buffer = [0u8; 4096];
    loop {
        let read = reader
            .read(&mut buffer)
            .map_err(|_| RuntimeError::Transport("acp_stdout_read_failed".into()))?;
        if read == 0 {
            if pending.iter().all(u8::is_ascii_whitespace) {
                return Ok(());
            }
            return Err(RuntimeError::Protocol("acp_stdout_truncated".into()));
        }
        pending.extend_from_slice(&buffer[..read]);
        if pending.len() > MAX_ACP_EXECUTION_FRAME_BYTES {
            return Err(RuntimeError::Protocol("acp_frame_oversized".into()));
        }
        while let Some(index) = pending.iter().position(|byte| *byte == b'\n') {
            let line = pending.drain(..=index).collect::<Vec<_>>();
            let line = &line[..line.len() - 1];
            if line.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            let value = serde_json::from_slice(line)
                .map_err(|_| RuntimeError::Protocol("acp_frame_invalid".into()));
            if sender.send(value).is_err() {
                return Ok(());
            }
        }
    }
}

fn read_bounded_acp_stderr(reader: impl Read) -> Result<Vec<u8>, RuntimeError> {
    let mut bytes = Vec::new();
    reader
        .take((MAX_ACP_EXECUTION_FRAME_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| RuntimeError::Transport("acp_stderr_read_failed".into()))?;
    if bytes.len() > MAX_ACP_EXECUTION_FRAME_BYTES {
        return Err(RuntimeError::Transport("acp_stderr_oversized".into()));
    }
    Ok(bytes)
}

fn runtime_event(
    runtime_id: &str,
    request: &RuntimeRequest,
    session_id: Option<&str>,
    sequence: u64,
    event_type: &str,
    payload: Value,
) -> RuntimeEvent {
    RuntimeEvent {
        event_id: format!("acp-{}-{sequence}", request.execution_run_id),
        execution_run_id: request.execution_run_id.clone(),
        runtime_id: runtime_id.to_owned(),
        thread_id: session_id.map(str::to_owned),
        turn_id: Some(format!("acp-turn-{}", request.execution_run_id)),
        sequence,
        event_type: event_type.to_owned(),
        timestamp_ms: now_ms() as i64,
        payload,
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}
