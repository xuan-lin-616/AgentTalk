#[cfg(windows)]
use agenttalk_core::{
    ArtifactWriteOutcome, AttachmentWriteOutcome, CollaborationWriteOutcome, CoreError,
    CreateCollaborationCommand, CreateHandoffCommand, CreateWorkflowCommand,
    ExecutionRuntimeOptions, ExecutionStart, GenerateSummaryCommand, HandoffTransitionOutcome,
    HandoffWriteOutcome, ImportAttachmentFileCommand, ImportLocalAgentCommand, MemoryWriteOutcome,
    PersistentCore, RetrievalFeedbackWriteOutcome, RetrievalSelectionWriteOutcome,
    RetrievalWriteOutcome, RuntimeRegistry, StoreArtifactBodyCommand, StoreArtifactCommand,
    StoreAttachmentCommand, StoreMemoryCommand, StoreRetrievalFeedbackCommand,
    StoreRetrievalSelectionCommand, StoreRetrievalSourceCommand, StoreSummaryCommand,
    SummaryWriteOutcome, WorkflowDispatchCommand, WorkflowWriteOutcome,
};
#[cfg(windows)]
use agenttalk_domain::{
    AgentIdentity, Artifact, Attachment, CollaborationRun, CollaborationStatus, ConnectorProfile,
    Conversation, Handoff, IdentityModelListMode, IdentityModelListScope, IdentityModelListTarget,
    IdentityModelOption, MemoryItem, Message, ModelAvailability, ModelOptionSource, ModelSelection,
    ModelSelectionMode, Project, RetrievalFeedback, RetrievalSelection, RetrievalSource,
    StructuredHandoffDetails, Summary, WorkflowStep, WorkflowTemplate, WorkspaceAccess,
    CONNECTOR_PROFILE_SCOPE,
};
#[cfg(windows)]
use agenttalk_events::RuntimeEvent;
#[cfg(windows)]
use agenttalk_ipc::{
    NamedPipeClient, NamedPipeConnection, NamedPipeListener, NamedPipeReader, NamedPipeWriter,
    TransportError,
};
#[cfg(windows)]
use agenttalk_protocols::{
    validate_protocol, CommandEnvelope, ErrorEnvelope, ProtocolHandshake, ProtocolVersion,
    QueryEnvelope, ResponseEnvelope, StreamCursor, PROTOCOL_MAJOR,
};
#[cfg(windows)]
use agenttalk_runtime_host::{
    connector_runtime_failure, CodexAppServerRuntime, HttpCustomRuntime, KunSharedRuntime,
    OpenAiCompatibleRuntime, RuntimeAdapter, RuntimeError, UnconfiguredRuntime,
};
#[cfg(windows)]
use agenttalk_storage::{
    AgentModelBindingPatch, ArtifactBindingInput, BindingFieldPatch, CommandReceipt,
    CommandReceiptKey, CommandReceiptState, HandoffDeliveryRecord, HumanReceiptRecord,
    LocalAgentAdapterBinding, LocalAgentImportRequest, MachineAcceptanceRecord,
    OrchestrationContextAuthorityInput, OrchestrationEdgeInput, OrchestrationEdgePortInput,
    OrchestrationRoleBindingInput, RetrievalPreviewRequest, StorageError, ARTIFACT_BODY_MAX_BYTES,
    ARTIFACT_CONTENT_CHUNK_MAX_BYTES, CONNECTOR_PROFILE_QUERY_LIMIT_MAX,
    RETRIEVAL_PREVIEW_LIMIT_MAX,
};
#[cfg(windows)]
use base64::{engine::general_purpose::STANDARD, Engine as _};
#[cfg(windows)]
use serde_json::{json, Value};
#[cfg(windows)]
use sha2::{Digest, Sha256};
#[cfg(windows)]
use std::collections::{BTreeMap, VecDeque};
#[cfg(windows)]
use std::error::Error;
#[cfg(windows)]
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
#[cfg(windows)]
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Condvar, Mutex};
#[cfg(windows)]
use std::thread;
#[cfg(windows)]
use std::time::{Duration, Instant};
#[cfg(windows)]
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(windows)]
mod local_discovery;
#[cfg(windows)]
use local_discovery::{
    DiscoveryOwnerScope, EventSink, LocalDiscoveryService, LocalDiscoveryServiceError,
    LocalImportWork, StartScanOutcome, VerifyRequest, VerifyStartOutcome,
};

#[cfg(windows)]
#[derive(Clone)]
struct SessionBinding {
    client_id: String,
    session_id: String,
}

#[cfg(windows)]
impl SessionBinding {
    fn discovery_owner_scope(&self) -> DiscoveryOwnerScope {
        DiscoveryOwnerScope::from_authenticated_session(&self.client_id, &self.session_id)
    }
}

#[cfg(windows)]
const EVENT_RETENTION_MAX_EVENTS: usize = 256;
#[cfg(windows)]
const EVENT_RETENTION_MAX_BYTES: usize = 4 * 1024 * 1024;
#[cfg(windows)]
const DISCOVERY_STREAM_MAX_OWNERS: usize = 128;
#[cfg(windows)]
const DISCOVERY_STREAM_RETENTION: Duration = Duration::from_secs(10 * 60);
#[cfg(windows)]
const CORE_EVENT_STREAM_ID: &str = "core-events";
#[cfg(windows)]
const DISCOVERY_EVENT_STREAM_ID: &str = "local-discovery-events";

#[cfg(windows)]
#[derive(Clone, Copy)]
struct EventRetentionLimits {
    max_events: usize,
    max_bytes: usize,
}

#[cfg(windows)]
impl EventRetentionLimits {
    const fn production() -> Self {
        Self {
            max_events: EVENT_RETENTION_MAX_EVENTS,
            max_bytes: EVENT_RETENTION_MAX_BYTES,
        }
    }

    fn test_fixture() -> Self {
        if std::env::var("AGENTTALK_CORE_DEV_MODE").as_deref() != Ok("1") {
            return Self::production();
        }
        let max_events = std::env::var("AGENTTALK_CORE_TEST_EVENT_RETENTION_MAX_EVENTS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| (4..=EVENT_RETENTION_MAX_EVENTS).contains(value))
            .unwrap_or(EVENT_RETENTION_MAX_EVENTS);
        Self {
            max_events,
            max_bytes: EVENT_RETENTION_MAX_BYTES,
        }
    }
}

#[cfg(windows)]
struct RetainedEvent {
    event: RuntimeEvent,
    bytes: usize,
}

#[cfg(windows)]
struct EventHubState {
    head: u64,
    retained: VecDeque<RetainedEvent>,
    retained_bytes: usize,
    retention: EventRetentionLimits,
}

#[cfg(windows)]
struct EventHub {
    state: Mutex<EventHubState>,
    changed: Condvar,
}

#[cfg(windows)]
impl EventHub {
    fn new(head: u64, initial_events: Vec<RuntimeEvent>, retention: EventRetentionLimits) -> Self {
        let mut state = EventHubState {
            head,
            retained: VecDeque::new(),
            retained_bytes: 0,
            retention,
        };
        for event in initial_events {
            let bytes = serde_json::to_vec(&event)
                .map(|value| value.len())
                .unwrap_or(usize::MAX);
            if bytes > state.retention.max_bytes {
                continue;
            }
            state.retained.push_back(RetainedEvent { event, bytes });
            state.retained_bytes = state.retained_bytes.saturating_add(bytes);
            while state.retained.len() > state.retention.max_events
                || state.retained_bytes > state.retention.max_bytes
            {
                if let Some(evicted) = state.retained.pop_front() {
                    state.retained_bytes = state.retained_bytes.saturating_sub(evicted.bytes);
                } else {
                    break;
                }
            }
        }
        Self {
            state: Mutex::new(EventHubState {
                head: state.head,
                retained: state.retained,
                retained_bytes: state.retained_bytes,
                retention: state.retention,
            }),
            changed: Condvar::new(),
        }
    }

    fn append_existing(&self, events: Vec<RuntimeEvent>) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        for event in events {
            state.head = state.head.max(event.sequence);
            let bytes = serde_json::to_vec(&event)
                .map(|value| value.len())
                .unwrap_or(usize::MAX);
            if bytes > state.retention.max_bytes {
                continue;
            }
            state.retained.push_back(RetainedEvent { event, bytes });
            state.retained_bytes = state.retained_bytes.saturating_add(bytes);
            while state.retained.len() > state.retention.max_events
                || state.retained_bytes > state.retention.max_bytes
            {
                if let Some(evicted) = state.retained.pop_front() {
                    state.retained_bytes = state.retained_bytes.saturating_sub(evicted.bytes);
                } else {
                    break;
                }
            }
        }
        self.changed.notify_all();
    }

    fn append_generated(&self, events: Vec<RuntimeEvent>) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        for mut event in events {
            state.head = state.head.saturating_add(1);
            event.sequence = state.head;
            let bytes = serde_json::to_vec(&event)
                .map(|value| value.len())
                .unwrap_or(usize::MAX);
            if bytes > state.retention.max_bytes {
                continue;
            }
            state.retained.push_back(RetainedEvent { event, bytes });
            state.retained_bytes = state.retained_bytes.saturating_add(bytes);
            while state.retained.len() > state.retention.max_events
                || state.retained_bytes > state.retention.max_bytes
            {
                if let Some(evicted) = state.retained.pop_front() {
                    state.retained_bytes = state.retained_bytes.saturating_sub(evicted.bytes);
                } else {
                    break;
                }
            }
        }
        self.changed.notify_all();
    }

    fn replay_after(
        &self,
        after_sequence: u64,
        limit: u64,
    ) -> Result<Vec<RuntimeEvent>, ReplayGap> {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let oldest_sequence = state
            .retained
            .front()
            .map(|event| event.event.sequence)
            .unwrap_or_else(|| state.head.saturating_add(1));
        if after_sequence < oldest_sequence.saturating_sub(1) {
            return Err(ReplayGap {
                oldest_sequence,
                head: state.head,
                retention: state.retention,
            });
        }
        Ok(state
            .retained
            .iter()
            .filter(|event| event.event.sequence > after_sequence)
            .take(limit as usize)
            .map(|event| event.event.clone())
            .collect())
    }

    fn gap(&self, after_sequence: u64) -> Option<ReplayGap> {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let oldest_sequence = state
            .retained
            .front()
            .map(|event| event.event.sequence)
            .unwrap_or_else(|| state.head.saturating_add(1));
        (after_sequence < oldest_sequence.saturating_sub(1)).then_some(ReplayGap {
            oldest_sequence,
            head: state.head,
            retention: state.retention,
        })
    }

    fn retention_window(&self) -> (u64, u64, EventRetentionLimits) {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        (
            state
                .retained
                .front()
                .map(|event| event.event.sequence)
                .unwrap_or_else(|| state.head.saturating_add(1)),
            state.head,
            state.retention,
        )
    }

    fn wait_for_change(&self, after_sequence: u64, timeout: Duration) -> bool {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.head > after_sequence {
            return true;
        }
        let result = self.changed.wait_timeout(state, timeout);
        match result {
            Ok((state, _)) => state.head > after_sequence,
            Err(error) => error.into_inner().0.head > after_sequence,
        }
    }
}

#[cfg(windows)]
struct ReplayGap {
    oldest_sequence: u64,
    head: u64,
    retention: EventRetentionLimits,
}

#[cfg(windows)]
impl ReplayGap {
    fn details(&self, stream_id: &str, stream_epoch: &str) -> Value {
        json!({
            "streamId": stream_id,
            "epoch": stream_epoch,
            "requestedCursorIsBefore": self.oldest_sequence,
            "oldestAvailableCursor": {
                "streamId": stream_id,
                "sequence": self.oldest_sequence,
                "epoch": stream_epoch,
            },
            "resumeCursor": {
                "streamId": stream_id,
                "sequence": self.oldest_sequence.saturating_sub(1),
                "epoch": stream_epoch,
            },
            "headCursor": {
                "streamId": stream_id,
                "sequence": self.head,
                "epoch": stream_epoch,
            },
            "requiresSnapshot": true,
            "recovery": "snapshot_then_subscribe_from_resume_cursor",
            "retention": {
                "maxEvents": self.retention.max_events,
                "maxBytes": self.retention.max_bytes,
            },
        })
    }
}

#[cfg(windows)]
impl EventHub {
    fn publish_from_core(
        &self,
        core: &PersistentCore,
        before: u64,
        after: u64,
    ) -> Result<(), Box<dyn Error>> {
        if after <= before {
            return Ok(());
        }
        let mut cursor = before;
        while cursor < after {
            let batch = core.replay_events_limited(cursor, EVENT_RETENTION_MAX_EVENTS as u64)?;
            if batch.is_empty() {
                return Err("event stream advanced without replayable events".into());
            }
            let next_cursor = batch.last().map(|event| event.sequence).unwrap_or(cursor);
            if next_cursor <= cursor {
                return Err("event stream replay did not advance".into());
            }
            self.append_existing(batch);
            cursor = next_cursor;
        }
        Ok(())
    }
}

#[cfg(windows)]
impl EventHub {
    fn initial_events(
        core: &PersistentCore,
        retention: EventRetentionLimits,
    ) -> Result<Vec<RuntimeEvent>, Box<dyn Error>> {
        let head = core.event_cursor();
        if head == 0 {
            return Ok(Vec::new());
        }
        Ok(core.replay_events_limited(
            head.saturating_sub(retention.max_events as u64),
            retention.max_events as u64,
        )?)
    }
}

#[cfg(windows)]
impl EventHub {
    fn new_from_core(
        core: &PersistentCore,
        retention: EventRetentionLimits,
    ) -> Result<Self, Box<dyn Error>> {
        Ok(Self::new(
            core.event_cursor(),
            Self::initial_events(core, retention)?,
            retention,
        ))
    }
}

/*
 * The host retention window is deliberately independent from the persistent
 * event store. Persistence remains the source of record; IPC replay is a
 * bounded, fail-closed delivery window for cursors held by live clients.
 */

#[cfg(windows)]
struct CoreHost {
    core: Mutex<PersistentCore>,
    events: Arc<EventHub>,
    stream_epoch: String,
    discovery_events: Mutex<DiscoveryEventStreams>,
    next_discovery_epoch: AtomicU64,
    shutdown_requested: AtomicBool,
    discovery: LocalDiscoveryService,
    #[cfg(test)]
    start_replay_before_response_hook: Mutex<Option<Arc<local_discovery::WorkerPauseHook>>>,
}

#[cfg(windows)]
struct DiscoveryEventStreams {
    streams: BTreeMap<DiscoveryOwnerScope, DiscoveryEventStream>,
    retention: Duration,
    max_owners: usize,
    clock_offset: Duration,
}

#[cfg(windows)]
struct DiscoveryEventStream {
    events: Arc<EventHub>,
    epoch: String,
    last_activity: Instant,
    active_subscriptions: usize,
}

#[cfg(windows)]
struct DiscoveryEventStreamReservation {
    host: Arc<CoreHost>,
    owner: DiscoveryOwnerScope,
    epoch: String,
    newly_created: bool,
    committed: bool,
}

#[cfg(windows)]
struct ImportPlanPublicationRequest<'a> {
    request_id: &'a str,
    owner: &'a DiscoveryOwnerScope,
    scan_id: &'a str,
    candidate_id: &'a str,
    project_id: &'a str,
    model_selection: Option<&'a str>,
}

#[cfg(windows)]
struct LocalImportPublicationRequest<'a> {
    request_id: &'a str,
    client_id: &'a str,
    owner: &'a DiscoveryOwnerScope,
    scan_id: &'a str,
    candidate_id: &'a str,
    project_id: &'a str,
    model_selection: Option<&'a str>,
    payload_hash: String,
}

#[cfg(windows)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DiscoveryStreamError {
    NotFound,
    CapacityExhausted,
}

#[cfg(windows)]
impl DiscoveryStreamError {
    const fn code(self) -> &'static str {
        match self {
            Self::NotFound => "DISCOVERY_STREAM_NOT_FOUND",
            Self::CapacityExhausted => "DISCOVERY_STREAM_CAPACITY_EXHAUSTED",
        }
    }

    const fn message(self) -> &'static str {
        match self {
            Self::NotFound => "the requested discovery event stream does not exist",
            Self::CapacityExhausted => "discovery event stream capacity is exhausted",
        }
    }
}

#[cfg(windows)]
impl std::fmt::Display for DiscoveryStreamError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.message())
    }
}

#[cfg(windows)]
impl Error for DiscoveryStreamError {}

#[cfg(windows)]
#[derive(Clone, Copy, Eq, PartialEq)]
enum EventStreamKind {
    Core,
    Discovery,
}

#[cfg(windows)]
impl EventStreamKind {
    const fn id(self) -> &'static str {
        match self {
            Self::Core => CORE_EVENT_STREAM_ID,
            Self::Discovery => DISCOVERY_EVENT_STREAM_ID,
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            CORE_EVENT_STREAM_ID => Some(Self::Core),
            DISCOVERY_EVENT_STREAM_ID => Some(Self::Discovery),
            _ => None,
        }
    }
}

#[cfg(windows)]
type WriterQueue = Sender<Value>;

#[cfg(windows)]
type ReaderQueue = Receiver<Result<Vec<u8>, TransportError>>;

#[cfg(windows)]
type ReaderRuntime = (ReaderQueue, Arc<AtomicBool>, thread::JoinHandle<()>);

#[cfg(windows)]
struct SubscriptionState {
    cursor: u64,
    last_acked: u64,
    in_flight: VecDeque<(u64, usize)>,
    in_flight_bytes: usize,
    closed: bool,
}

#[cfg(windows)]
struct SharedSubscription {
    state: Mutex<SubscriptionState>,
    changed: Condvar,
}

#[cfg(windows)]
struct SubscriptionEventPump {
    shared: Arc<SharedSubscription>,
    host: Arc<CoreHost>,
    event_hub: Arc<EventHub>,
    writer: WriterQueue,
    session_id: String,
    subscription_id: String,
    request_id: String,
    stream_kind: EventStreamKind,
    server_epoch: String,
    max_events: u64,
    max_bytes: usize,
    _discovery_subscription: Option<DiscoverySubscriptionLease>,
}

#[cfg(windows)]
impl SharedSubscription {
    fn new(cursor: u64) -> Self {
        Self {
            state: Mutex::new(SubscriptionState {
                cursor,
                last_acked: cursor,
                in_flight: VecDeque::new(),
                in_flight_bytes: 0,
                closed: false,
            }),
            changed: Condvar::new(),
        }
    }

    fn close(&self) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.closed = true;
        self.changed.notify_all();
    }
}

#[cfg(windows)]
impl CoreHost {
    fn new(core: PersistentCore, discovery: LocalDiscoveryService) -> Result<Self, Box<dyn Error>> {
        let events = EventHub::new_from_core(&core, EventRetentionLimits::production())?;
        let stream_epoch = core.event_stream_epoch().to_owned();
        let discovery_stream_limits = DiscoveryStreamLimits::from_environment();
        Ok(Self {
            core: Mutex::new(core),
            events: Arc::new(events),
            stream_epoch,
            discovery_events: Mutex::new(DiscoveryEventStreams {
                streams: BTreeMap::new(),
                retention: discovery_stream_limits.retention,
                max_owners: discovery_stream_limits.max_owners,
                clock_offset: Duration::ZERO,
            }),
            next_discovery_epoch: AtomicU64::new(0),
            shutdown_requested: AtomicBool::new(false),
            discovery,
            #[cfg(test)]
            start_replay_before_response_hook: Mutex::new(None),
        })
    }

    fn publish_if_changed(
        &self,
        core: &PersistentCore,
        before: u64,
        after: u64,
    ) -> Result<(), Box<dyn Error>> {
        if after > before {
            self.events.publish_from_core(core, before, after)?;
        }
        Ok(())
    }

    fn publish_discovery_event(&self, owner: &DiscoveryOwnerScope, event: RuntimeEvent) {
        let recoverable_owners = self.discovery.recoverable_owners();
        let mut streams = self
            .discovery_events
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        prune_discovery_streams(&mut streams, &recoverable_owners);
        let now = streams.now();
        if let Some(stream) = streams.streams.get_mut(owner) {
            stream.last_activity = now;
            stream.events.append_generated(vec![event]);
        }
    }

    fn cancel_discovery_sessions(&self) {
        let publication = self.discovery.start_publication_guard();
        publication.cancel_all();
        let mut streams = self
            .discovery_events
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        streams.streams.clear();
    }

    #[cfg(test)]
    fn set_start_replay_before_response_hook_for_tests(
        &self,
        hook: Arc<local_discovery::WorkerPauseHook>,
    ) {
        *self
            .start_replay_before_response_hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(hook);
    }

    #[cfg(test)]
    fn pause_start_replay_before_response_for_tests(&self) {
        let hook = self
            .start_replay_before_response_hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        if let Some(hook) = hook {
            hook.pause_for_tests();
        }
    }

    #[cfg(test)]
    fn create_discovery_event_stream_for_owner(
        &self,
        owner: &DiscoveryOwnerScope,
    ) -> Result<(Arc<EventHub>, String), DiscoveryStreamError> {
        self.create_discovery_event_stream_entry(owner)
            .map(|(events, epoch, _)| (events, epoch))
    }

    fn create_discovery_event_stream_entry(
        &self,
        owner: &DiscoveryOwnerScope,
    ) -> Result<(Arc<EventHub>, String, bool), DiscoveryStreamError> {
        let recoverable_owners = self.discovery.recoverable_owners();
        let mut streams = self
            .discovery_events
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        prune_discovery_streams(&mut streams, &recoverable_owners);
        if !streams.streams.contains_key(owner) && streams.streams.len() >= streams.max_owners {
            return Err(DiscoveryStreamError::CapacityExhausted);
        }
        let now = streams.now();
        let newly_created = !streams.streams.contains_key(owner);
        let stream = streams
            .streams
            .entry(owner.clone())
            .or_insert_with(|| DiscoveryEventStream {
                events: Arc::new(EventHub::new(
                    0,
                    Vec::new(),
                    EventRetentionLimits::test_fixture(),
                )),
                epoch: format!(
                    "local-discovery-{}-{}-{}",
                    std::process::id(),
                    unix_time_ms(),
                    self.next_discovery_epoch.fetch_add(1, Ordering::AcqRel)
                ),
                last_activity: now,
                active_subscriptions: 0,
            });
        stream.last_activity = now;
        Ok((
            Arc::clone(&stream.events),
            stream.epoch.clone(),
            newly_created,
        ))
    }

    fn reserve_discovery_event_stream_for_owner(
        self: &Arc<Self>,
        owner: &DiscoveryOwnerScope,
    ) -> Result<DiscoveryEventStreamReservation, DiscoveryStreamError> {
        let (_, epoch, newly_created) = self.create_discovery_event_stream_entry(owner)?;
        Ok(DiscoveryEventStreamReservation {
            host: Arc::clone(self),
            owner: owner.clone(),
            epoch,
            newly_created,
            committed: false,
        })
    }

    fn publish_discovery_start_response(
        &self,
        connection: &WriterQueue,
        request_id: &str,
        owner: &DiscoveryOwnerScope,
        mut worker_ready: local_discovery::WorkerReadyStart,
        stream_reservation: DiscoveryEventStreamReservation,
    ) -> Result<(), LocalDiscoveryRouteError> {
        {
            let publication = self.discovery.start_publication_guard();
            publication
                .ensure_worker_ready_publishable(worker_ready.scan_id(), owner)
                .map_err(LocalDiscoveryRouteError::from)?;
            let (_, discovery_epoch) = self
                .try_discovery_event_stream(owner)
                .map_err(LocalDiscoveryRouteError::from)?;
            if discovery_epoch != stream_reservation.epoch() {
                return Err(LocalDiscoveryServiceError::ShuttingDown.into());
            }
            worker_ready.start_worker();
        }
        worker_ready
            .wait_for_running_lease()
            .map_err(LocalDiscoveryRouteError::from)?;

        let publication = self.discovery.start_publication_guard();
        publication
            .ensure_worker_ready_publishable(worker_ready.scan_id(), owner)
            .map_err(LocalDiscoveryRouteError::from)?;
        let (_, discovery_epoch) = self
            .try_discovery_event_stream(owner)
            .map_err(LocalDiscoveryRouteError::from)?;
        if discovery_epoch != stream_reservation.epoch() {
            return Err(LocalDiscoveryServiceError::ShuttingDown.into());
        }
        let mut response = worker_ready.response();
        response["eventStream"] = json!({
            "streamId": DISCOVERY_EVENT_STREAM_ID,
            "epoch": discovery_epoch,
        });
        worker_ready
            .publish_after_running_lease_with(&publication, owner)
            .map_err(LocalDiscoveryRouteError::from)?;
        stream_reservation.commit();
        write_response(connection, request_id, response).map_err(|_| {
            LocalDiscoveryRouteError::Service(LocalDiscoveryServiceError::ShuttingDown)
        })
    }

    fn publish_discovery_verify_response(
        &self,
        connection: &WriterQueue,
        request_id: &str,
        owner: &DiscoveryOwnerScope,
        mut worker_ready: local_discovery::WorkerReadyVerify,
    ) -> Result<(), LocalDiscoveryRouteError> {
        {
            let publication = self.discovery.start_publication_guard();
            publication
                .ensure_verify_worker_ready_publishable(
                    worker_ready.scan_id(),
                    worker_ready.candidate_id(),
                    owner,
                )
                .map_err(LocalDiscoveryRouteError::from)?;
            worker_ready.start_worker();
        }
        worker_ready
            .wait_for_running_lease()
            .map_err(LocalDiscoveryRouteError::from)?;

        let publication = self.discovery.start_publication_guard();
        publication
            .ensure_verify_worker_ready_publishable(
                worker_ready.scan_id(),
                worker_ready.candidate_id(),
                owner,
            )
            .map_err(LocalDiscoveryRouteError::from)?;
        let response = worker_ready
            .publish_after_running_lease_with(&publication, owner)
            .map_err(LocalDiscoveryRouteError::from)?;
        write_response(connection, request_id, response).map_err(|_| {
            LocalDiscoveryRouteError::Service(LocalDiscoveryServiceError::ShuttingDown)
        })
    }

    fn publish_discovery_mutation_response(
        &self,
        connection: &WriterQueue,
        request_id: &str,
        owner: &DiscoveryOwnerScope,
        scan_id: &str,
        mutate: impl FnOnce(
            &local_discovery::DiscoveryStartPublicationGuard<'_>,
        ) -> Result<Value, LocalDiscoveryServiceError>,
    ) -> Result<(), LocalDiscoveryRouteError> {
        let publication = self.discovery.start_publication_guard();
        publication
            .ensure_mutation_publishable(scan_id, owner)
            .map_err(LocalDiscoveryRouteError::from)?;
        let response = mutate(&publication).map_err(LocalDiscoveryRouteError::from)?;
        write_response(connection, request_id, response).map_err(|_| {
            LocalDiscoveryRouteError::Service(LocalDiscoveryServiceError::ShuttingDown)
        })
    }

    fn publish_discovery_import_plan_response(
        &self,
        connection: &WriterQueue,
        request: ImportPlanPublicationRequest<'_>,
    ) -> Result<(), LocalDiscoveryRouteError> {
        let work = {
            let publication = self.discovery.start_publication_guard();
            self.discovery
                .begin_import_plan_with_publication(
                    &publication,
                    request.owner,
                    request.scan_id,
                    request.candidate_id,
                    request.project_id,
                    request.model_selection,
                )
                .map_err(LocalDiscoveryRouteError::from)?
        };
        let result = self.discovery.execute_import_plan(work);
        let publication = self.discovery.start_publication_guard();
        self.discovery
            .ensure_import_plan_publishable(&publication, request.owner, request.scan_id)
            .map_err(LocalDiscoveryRouteError::from)?;
        let response = result.map_err(LocalDiscoveryRouteError::from)?;
        write_response(connection, request.request_id, response).map_err(|_| {
            LocalDiscoveryRouteError::Service(LocalDiscoveryServiceError::ShuttingDown)
        })
    }

    fn publish_local_agent_import_response(
        &self,
        connection: &WriterQueue,
        request: LocalImportPublicationRequest<'_>,
    ) -> Result<(), LocalDiscoveryRouteError> {
        let plan_work = {
            let publication = self.discovery.start_publication_guard();
            self.discovery
                .begin_import_plan_with_publication(
                    &publication,
                    request.owner,
                    request.scan_id,
                    request.candidate_id,
                    request.project_id,
                    request.model_selection,
                )
                .map_err(LocalDiscoveryRouteError::from)?
        };
        let work = self
            .discovery
            .execute_local_import(plan_work)
            .map_err(LocalDiscoveryRouteError::from)?;
        let publication = self.discovery.start_publication_guard();
        self.discovery
            .ensure_import_plan_publishable(&publication, request.owner, request.scan_id)
            .map_err(LocalDiscoveryRouteError::from)?;
        let mut core = self.core.lock().unwrap_or_else(|error| error.into_inner());
        let before = core.event_cursor();
        let outcome = core
            .import_local_agent(ImportLocalAgentCommand {
                request: local_agent_import_request(&request, work),
            })
            .map_err(|error| {
                LocalDiscoveryRouteError::Service(local_import_outcome_error(&error))
            })?;
        let after = core.event_cursor();
        self.publish_if_changed(&core, before, after).map_err(|_| {
            LocalDiscoveryRouteError::Service(LocalDiscoveryServiceError::ShuttingDown)
        })?;
        let response = json!({
            "schemaVersion": "agent.import_local.v1",
            "importId": outcome.import_id,
            "connectorId": outcome.connector_id,
            "agentId": outcome.agent_id,
            "projectId": outcome.project_id,
            "reused": outcome.reused,
            "eventSequence": outcome.event_sequence,
        });
        drop(core);
        write_response(connection, request.request_id, response).map_err(|_| {
            LocalDiscoveryRouteError::Service(LocalDiscoveryServiceError::ShuttingDown)
        })
    }

    fn try_discovery_event_stream(
        &self,
        owner: &DiscoveryOwnerScope,
    ) -> Result<(Arc<EventHub>, String), DiscoveryStreamError> {
        let recoverable_owners = self.discovery.recoverable_owners();
        let mut streams = self
            .discovery_events
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        prune_discovery_streams(&mut streams, &recoverable_owners);
        let now = streams.now();
        let stream = streams
            .streams
            .get_mut(owner)
            .ok_or(DiscoveryStreamError::NotFound)?;
        stream.last_activity = now;
        Ok((Arc::clone(&stream.events), stream.epoch.clone()))
    }

    fn event_stream(
        &self,
        kind: EventStreamKind,
        owner: &DiscoveryOwnerScope,
    ) -> Result<(Arc<EventHub>, String), DiscoveryStreamError> {
        match kind {
            EventStreamKind::Core => Ok((Arc::clone(&self.events), self.stream_epoch.clone())),
            EventStreamKind::Discovery => self.try_discovery_event_stream(owner),
        }
    }

    fn begin_discovery_subscription(
        self: &Arc<Self>,
        owner: &DiscoveryOwnerScope,
    ) -> Result<DiscoverySubscriptionLease, DiscoveryStreamError> {
        let recoverable_owners = self.discovery.recoverable_owners();
        {
            let mut streams = self
                .discovery_events
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            prune_discovery_streams(&mut streams, &recoverable_owners);
            let now = streams.now();
            let stream = streams
                .streams
                .get_mut(owner)
                .ok_or(DiscoveryStreamError::NotFound)?;
            stream.last_activity = now;
            stream.active_subscriptions = stream.active_subscriptions.saturating_add(1);
        }
        Ok(DiscoverySubscriptionLease {
            host: Arc::clone(self),
            owner: owner.clone(),
        })
    }

    fn end_discovery_subscription(&self, owner: &DiscoveryOwnerScope) {
        let recoverable_owners = self.discovery.recoverable_owners();
        let mut streams = self
            .discovery_events
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(stream) = streams.streams.get_mut(owner) {
            stream.active_subscriptions = stream.active_subscriptions.saturating_sub(1);
        }
        prune_discovery_streams(&mut streams, &recoverable_owners);
    }

    #[cfg(test)]
    fn discovery_stream_count_for_tests(&self) -> usize {
        let streams = self
            .discovery_events
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        streams.streams.len()
    }

    #[cfg(test)]
    fn discovery_stream_epoch_for_tests(&self, owner: &DiscoveryOwnerScope) -> Option<String> {
        let streams = self
            .discovery_events
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        streams
            .streams
            .get(owner)
            .map(|stream| stream.epoch.clone())
    }

    #[cfg(test)]
    fn advance_discovery_stream_clock_for_tests(&self, duration: Duration) {
        let mut streams = self
            .discovery_events
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        streams.clock_offset = streams.clock_offset.saturating_add(duration);
    }

    #[cfg(test)]
    fn prune_discovery_streams_for_tests(&self) {
        let recoverable_owners = self.discovery.recoverable_owners();
        let mut streams = self
            .discovery_events
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        prune_discovery_streams(&mut streams, &recoverable_owners);
    }

    #[cfg(test)]
    fn discovery_stream_event_count_for_tests(&self, owner: &DiscoveryOwnerScope) -> Option<usize> {
        let streams = self
            .discovery_events
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        streams.streams.get(owner).map(|stream| {
            let state = stream
                .events
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            state.retained.len()
        })
    }

    #[cfg(test)]
    fn begin_discovery_subscription_for_tests(
        self: &Arc<Self>,
        owner: &DiscoveryOwnerScope,
    ) -> Result<DiscoverySubscriptionLease, DiscoveryStreamError> {
        let _ = self.create_discovery_event_stream_for_owner(owner)?;
        self.begin_discovery_subscription(owner)
    }
}

#[cfg(windows)]
struct DiscoveryStreamLimits {
    max_owners: usize,
    retention: Duration,
}

#[cfg(windows)]
impl DiscoveryStreamLimits {
    fn from_environment() -> Self {
        if std::env::var("AGENTTALK_CORE_DEV_MODE").as_deref() != Ok("1") {
            return Self::production();
        }
        let max_owners = std::env::var("AGENTTALK_CORE_TEST_DISCOVERY_STREAM_MAX_OWNERS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| (1..=DISCOVERY_STREAM_MAX_OWNERS).contains(value))
            .unwrap_or(DISCOVERY_STREAM_MAX_OWNERS);
        let retention = std::env::var("AGENTTALK_CORE_TEST_DISCOVERY_STREAM_RETENTION_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| (1..=600_000).contains(value))
            .map(Duration::from_millis)
            .unwrap_or(DISCOVERY_STREAM_RETENTION);
        Self {
            max_owners,
            retention,
        }
    }

    const fn production() -> Self {
        Self {
            max_owners: DISCOVERY_STREAM_MAX_OWNERS,
            retention: DISCOVERY_STREAM_RETENTION,
        }
    }
}

#[cfg(windows)]
struct DiscoverySubscriptionLease {
    host: Arc<CoreHost>,
    owner: DiscoveryOwnerScope,
}

#[cfg(windows)]
impl Drop for DiscoverySubscriptionLease {
    fn drop(&mut self) {
        self.host.end_discovery_subscription(&self.owner);
    }
}

#[cfg(windows)]
impl DiscoveryEventStreams {
    fn now(&self) -> Instant {
        Instant::now() + self.clock_offset
    }
}

#[cfg(windows)]
impl DiscoveryEventStreamReservation {
    fn event_sink(&self) -> EventSink {
        let owner = self.owner.clone();
        let host = Arc::clone(&self.host);
        Arc::new(move |event| host.publish_discovery_event(&owner, event))
    }

    fn epoch(&self) -> &str {
        &self.epoch
    }

    fn commit(mut self) {
        self.committed = true;
    }
}

#[cfg(windows)]
impl Drop for DiscoveryEventStreamReservation {
    fn drop(&mut self) {
        if self.committed || !self.newly_created {
            return;
        }
        let mut streams = self
            .host
            .discovery_events
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let can_remove = streams.streams.get(&self.owner).is_some_and(|stream| {
            let event_count = stream
                .events
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .retained
                .len();
            stream.epoch == self.epoch && stream.active_subscriptions == 0 && event_count == 0
        });
        if can_remove {
            streams.streams.remove(&self.owner);
        }
    }
}

#[cfg(windows)]
fn prune_discovery_streams(
    streams: &mut DiscoveryEventStreams,
    recoverable_owners: &std::collections::BTreeSet<DiscoveryOwnerScope>,
) {
    let now = streams.now();
    let retention = streams.retention;
    streams.streams.retain(|owner, stream| {
        stream.active_subscriptions > 0
            || recoverable_owners.contains(owner)
            || now.duration_since(stream.last_activity) <= retention
    });
}

#[cfg(windows)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CoreStartupCategory {
    DatabaseUnavailable,
    DatabaseSchemaIncompatible,
    DatabaseLocked,
    PermissionDenied,
    RuntimeConfigurationUnavailable,
    NamedPipeBindFailed,
    CoreStartupFailed,
}

#[cfg(windows)]
impl CoreStartupCategory {
    const fn as_str(self) -> &'static str {
        match self {
            Self::DatabaseUnavailable => "database_unavailable",
            Self::DatabaseSchemaIncompatible => "database_schema_incompatible",
            Self::DatabaseLocked => "database_locked",
            Self::PermissionDenied => "permission_denied",
            Self::RuntimeConfigurationUnavailable => "runtime_configuration_unavailable",
            Self::NamedPipeBindFailed => "named_pipe_bind_failed",
            Self::CoreStartupFailed => "core_startup_failed",
        }
    }
}

#[cfg(windows)]
#[derive(Debug)]
struct CoreStartupError {
    category: CoreStartupCategory,
    stage: &'static str,
    detail: String,
}

#[cfg(windows)]
impl CoreStartupError {
    fn new(category: CoreStartupCategory, stage: &'static str, detail: impl Into<String>) -> Self {
        Self {
            category,
            stage,
            detail: detail.into(),
        }
    }

    fn from_core(stage: &'static str, error: &CoreError, database_path: &std::path::Path) -> Self {
        let category = match error {
            CoreError::Storage(error) => storage_startup_category(error, database_path),
            _ => CoreStartupCategory::CoreStartupFailed,
        };
        Self::new(category, stage, error.to_string())
    }
}

#[cfg(windows)]
impl std::fmt::Display for CoreStartupError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.stage, self.detail)
    }
}

#[cfg(windows)]
impl Error for CoreStartupError {}

#[cfg(windows)]
fn storage_startup_category(
    error: &StorageError,
    database_path: &std::path::Path,
) -> CoreStartupCategory {
    match error {
        StorageError::MigrationChecksumMismatch { .. } | StorageError::MigrationDirty { .. } => {
            CoreStartupCategory::DatabaseSchemaIncompatible
        }
        StorageError::Sqlite(error) => {
            let detail = error.to_string().to_ascii_lowercase();
            if detail.contains("locked") || detail.contains("busy") {
                CoreStartupCategory::DatabaseLocked
            } else if detail.contains("permission denied")
                || detail.contains("access is denied")
                || detail.contains("readonly")
                || detail.contains("read-only")
            {
                CoreStartupCategory::PermissionDenied
            } else if detail.contains("not a database")
                || detail.contains("malformed")
                || detail.contains("no such table")
            {
                CoreStartupCategory::DatabaseSchemaIncompatible
            } else if detail.contains("unable to open database") || detail.contains("cannot open") {
                if database_path.exists() {
                    CoreStartupCategory::PermissionDenied
                } else {
                    CoreStartupCategory::DatabaseUnavailable
                }
            } else {
                CoreStartupCategory::CoreStartupFailed
            }
        }
        _ => CoreStartupCategory::CoreStartupFailed,
    }
}

#[cfg(windows)]
fn io_startup_error(stage: &'static str, error: &std::io::Error) -> CoreStartupError {
    let category = match error.kind() {
        std::io::ErrorKind::PermissionDenied => CoreStartupCategory::PermissionDenied,
        std::io::ErrorKind::NotFound | std::io::ErrorKind::AlreadyExists => {
            CoreStartupCategory::DatabaseUnavailable
        }
        _ => CoreStartupCategory::CoreStartupFailed,
    };
    CoreStartupError::new(category, stage, error.to_string())
}

#[cfg(windows)]
fn main() {
    if let Err(error) = run_core() {
        eprintln!(
            "AGENTTALK_CORE_STARTUP category={} stage={} detail={}",
            error.category.as_str(),
            error.stage,
            redact_startup_detail(&error.detail),
        );
        eprintln!(
            "AgentTalk Core startup failed: {}",
            redact_startup_detail(&error.detail)
        );
        std::process::exit(1);
    }
}

#[cfg(windows)]
fn run_core() -> Result<(), CoreStartupError> {
    let args: Vec<String> = std::env::args().collect();
    let pipe_name = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| format!("\\\\.\\pipe\\agenttalk-core-{}", std::process::id()));
    let db_path = args
        .get(2)
        .cloned()
        .unwrap_or_else(|| "agenttalk-core.sqlite3".into());
    let expected_session_credential = session_credential_from_environment().map_err(|error| {
        CoreStartupError::new(
            CoreStartupCategory::RuntimeConfigurationUnavailable,
            "environment_parameters",
            error.to_string(),
        )
    })?;
    let db_path_ref = std::path::Path::new(&db_path);
    if let Some(parent) = db_path_ref.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .map_err(|error| io_startup_error("database_open", &error))?;
        }
    }
    let artifact_root = args
        .get(3)
        .cloned()
        .or_else(|| std::env::var("AGENTTALK_CORE_ARTIFACT_ROOT").ok());
    let runtimes = runtime_registry_from_environment().map_err(|error| {
        CoreStartupError::new(
            CoreStartupCategory::RuntimeConfigurationUnavailable,
            "runtime_initialization",
            error.to_string(),
        )
    })?;
    let core = match artifact_root {
        Some(root) => PersistentCore::open_with_runtime_registry_and_artifact_root(
            &db_path,
            runtimes,
            Some(std::path::Path::new(&root)),
        )
        .map_err(|error| {
            CoreStartupError::from_core("schema_migration_preflight", &error, db_path_ref)
        })?,
        None => {
            PersistentCore::open_with_runtime_registry(&db_path, runtimes).map_err(|error| {
                CoreStartupError::from_core("schema_migration_preflight", &error, db_path_ref)
            })?
        }
    };
    let discovery = LocalDiscoveryService::from_environment();
    let host = Arc::new(CoreHost::new(core, discovery).map_err(|error| {
        CoreStartupError::new(
            CoreStartupCategory::CoreStartupFailed,
            "runtime_initialization",
            error.to_string(),
        )
    })?);
    let server_epoch = host.stream_epoch.clone();
    if let Some(delay_ms) = test_fixture_startup_delay_ms() {
        thread::sleep(Duration::from_millis(delay_ms));
    }
    let mut listener = NamedPipeListener::bind(pipe_name.clone()).map_err(|error| {
        CoreStartupError::new(
            CoreStartupCategory::NamedPipeBindFailed,
            "named_pipe_bind",
            error.to_string(),
        )
    })?;
    eprintln!("AgentTalk Core listening on {pipe_name}");
    loop {
        if host.shutdown_requested.load(Ordering::Acquire) {
            break Ok(());
        }
        match listener.accept() {
            Ok(connection) => {
                if host.shutdown_requested.load(Ordering::Acquire) {
                    drop(connection);
                    break Ok(());
                }
                let host = Arc::clone(&host);
                let expected_session_credential = expected_session_credential.clone();
                let server_epoch = server_epoch.clone();
                let wake_pipe_name = pipe_name.clone();
                thread::spawn(move || {
                    let result = handle_connection(
                        connection,
                        &host,
                        &expected_session_credential,
                        &server_epoch,
                    );
                    match result {
                        Ok(true) => {
                            // Connect once to the next listener instance so a
                            // blocking ConnectNamedPipe wakes after the
                            // authenticated owner requested shutdown. The
                            // main loop observes the flag and exits without
                            // accepting another client session.
                            let _ = NamedPipeClient::connect(&wake_pipe_name);
                        }
                        Ok(false) => {}
                        Err(error) => eprintln!("connection ended: {error}"),
                    }
                });
            }
            Err(error) => eprintln!("accept failed: {error}"),
        }
    }
}

#[cfg(windows)]
fn redact_startup_detail(detail: &str) -> String {
    let compact = detail.replace(['\r', '\n'], " ");
    let secret_keys = [
        "authorization",
        "cookie",
        "token",
        "api_key",
        "apikey",
        "password",
        "secret",
    ];
    let mut redacted = Vec::new();
    let mut redact_next = false;
    for token in compact.split_whitespace() {
        if redact_next {
            redacted.push("<redacted>".to_owned());
            redact_next = false;
            continue;
        }
        let lower = token.to_ascii_lowercase();
        let key = secret_keys.iter().find(|key| {
            lower == **key
                || lower.starts_with(&format!("{}=", key))
                || lower.starts_with(&format!("{}:", key))
        });
        if let Some(key) = key {
            if lower == *key {
                redacted.push(token.to_owned());
                redact_next = true;
            } else if let Some(separator) = token.find(['=', ':']) {
                if separator + 1 == token.len() {
                    redacted.push(token.to_owned());
                    redact_next = true;
                } else {
                    redacted.push(format!("{}<redacted>", &token[..=separator]));
                }
            } else {
                redacted.push("<redacted>".to_owned());
            }
        } else {
            redacted.push(token.to_owned());
        }
    }
    let mut compact = redacted.join(" ");
    if compact.len() > 512 {
        truncate_utf8_prefix_in_place(&mut compact, 512);
        compact.push_str("...[truncated]");
    }
    compact
}

fn truncate_utf8_prefix_in_place(value: &mut String, max_bytes: usize) {
    if value.len() <= max_bytes {
        return;
    }
    let mut end = max_bytes;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    value.truncate(end);
}

#[cfg(windows)]
fn test_fixture_startup_delay_ms() -> Option<u64> {
    if std::env::var("AGENTTALK_CORE_DEV_MODE").as_deref() != Ok("1") {
        return None;
    }
    std::env::var("AGENTTALK_CORE_STARTUP_DELAY_MS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|value| *value > 0)
}

#[cfg(windows)]
fn test_fixture_hangs(mode: &str) -> bool {
    if std::env::var("AGENTTALK_CORE_DEV_MODE").as_deref() != Ok("1") {
        return false;
    }
    std::env::var("AGENTTALK_CORE_TEST_BEHAVIOR")
        .map(|value| value.trim() == mode || value.trim() == "requests")
        .unwrap_or(false)
}

#[cfg(windows)]
fn test_fixture_wait_forever() -> ! {
    loop {
        thread::sleep(Duration::from_secs(60));
    }
}

#[cfg(windows)]
fn runtime_registry_from_environment() -> Result<RuntimeRegistry, Box<dyn Error>> {
    let configured = std::env::var("AGENTTALK_CORE_RUNTIMES")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| std::env::var("AGENTTALK_CORE_RUNTIME").unwrap_or_default());
    let development_mode = std::env::var("AGENTTALK_CORE_DEV_MODE").as_deref() == Ok("1");
    runtime_registry_from_configuration(&configured, development_mode)
}

/// Builds the Core Runtime registry without reading process environment. This
/// keeps tests deterministic and, more importantly, ensures construction is
/// inert: registered production adapters are discovered only when their
/// profile is actually queried or selected for work.
#[cfg(windows)]
fn runtime_registry_from_configuration(
    configured: &str,
    development_mode: bool,
) -> Result<RuntimeRegistry, Box<dyn Error>> {
    let requested = if configured.trim().is_empty() {
        // Preserve the legacy default Runtime projection while making the two
        // built-in desktop Connector types recognizable without an
        // environment-variable activation path. The registry reads only
        // adapter ids here; Codex/Kun discovery remains lazy.
        vec!["unconfigured", "codex", "kun"]
    } else {
        configured
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .collect::<Vec<_>>()
    };
    if requested == ["fixture-dual"] {
        if !development_mode {
            return Err(
                "fixture-dual requires AGENTTALK_CORE_DEV_MODE=1 and is unavailable in formal mode"
                    .into(),
            );
        }
        // Give the executable-level fixture enough deterministic spacing for
        // an authenticated cancellation to reach Core after
        // `connector.started`, while still keeping the test suite bounded.
        // This development-only adapter is never selected by production.
        let delay = Duration::from_millis(75);
        return RuntimeRegistry::from_adapters(vec![
            Box::new(CodexAppServerRuntime::from_fixture_models_with_delay(
                vec!["codex-model-a".into(), "codex-model-b".into()],
                r#"{"type":"item/agentMessage/delta","delta":"codex:{modelId}:delta-1"}
{"type":"item/agentMessage/delta","delta":"codex:{modelId}:delta-2"}
{"type":"response.completed"}"#,
                delay,
            )),
            Box::new(KunSharedRuntime::from_fixture_models_with_delay(
                vec!["kun-model-a".into(), "kun-model-b".into()],
                r#"{"method":"output.delta","params":{"delta":"kun:{modelId}:delta-1"}}
{"method":"output.delta","params":{"delta":"kun:{modelId}:delta-2"}}
{"type":"execution.completed"}"#,
                delay,
            )),
        ])
        .map_err(Into::into);
    }
    if requested.contains(&"fixture-dual") {
        return Err("fixture-dual must be the only configured Runtime".into());
    }

    let mut adapters: Vec<Box<dyn RuntimeAdapter>> = Vec::new();
    for runtime_id in requested {
        let adapter: Box<dyn RuntimeAdapter> = match runtime_id {
            "unconfigured" => Box::new(UnconfiguredRuntime),
            // These are production transports. Construction records no model
            // fixture and performs no CLI discovery, HTTP call, or credential
            // read; profile-bound operations trigger their bounded lazy use.
            "codex" => Box::new(CodexAppServerRuntime::with_config(Default::default())),
            "kun" => Box::new(KunSharedRuntime::with_config(Default::default())),
            "openai-compatible" => Box::new(OpenAiCompatibleRuntime::new("default")),
            "http-custom" => Box::new(HttpCustomRuntime::new("default")),
            "mock" if development_mode => Box::new(agenttalk_runtime_host::MockRuntime::default()),
            "mock" => return Err(
                "MockRuntime requires AGENTTALK_CORE_DEV_MODE=1 and is unavailable in formal mode"
                    .into(),
            ),
            other => {
                return Err(format!("unsupported AGENTTALK_CORE_RUNTIME value: {other}").into())
            }
        };
        adapters.push(adapter);
    }
    RuntimeRegistry::from_adapters(adapters).map_err(Into::into)
}

#[cfg(windows)]
fn spawn_runtime_dispatch(host: Arc<CoreHost>, run_id: String) {
    thread::spawn(move || {
        let dispatch = {
            let mut core = host.core.lock().unwrap_or_else(|error| error.into_inner());
            let before = core.event_cursor();
            let result = core.begin_runtime_dispatch(&run_id);
            let after = core.event_cursor();
            let _ = host.publish_if_changed(&core, before, after);
            result
        };
        let dispatch = match dispatch {
            Ok(dispatch) => dispatch,
            Err(CoreError::Runtime(error)) => {
                let mut core = host.core.lock().unwrap_or_else(|error| error.into_inner());
                let before = core.event_cursor();
                let _ = core.fail_runtime_dispatch(&run_id, &error);
                let after = core.event_cursor();
                let _ = host.publish_if_changed(&core, before, after);
                return;
            }
            Err(_) => {
                let mut core = host.core.lock().unwrap_or_else(|error| error.into_inner());
                let before = core.event_cursor();
                let _ = core.fail_runtime_dispatch(&run_id, &RuntimeError::NotConfigured);
                let after = core.event_cursor();
                let _ = host.publish_if_changed(&core, before, after);
                return;
            }
        };

        let started = Instant::now();
        loop {
            if host.shutdown_requested.load(Ordering::Acquire) {
                let _ = dispatch.stream.cancel();
                return;
            }
            let Some(remaining) = dispatch.timeout.checked_sub(started.elapsed()) else {
                let _ = dispatch.stream.cancel();
                let mut core = host.core.lock().unwrap_or_else(|error| error.into_inner());
                let before = core.event_cursor();
                let _ = core.fail_runtime_dispatch(&run_id, &RuntimeError::Timeout);
                let after = core.event_cursor();
                let _ = host.publish_if_changed(&core, before, after);
                return;
            };
            match dispatch.stream.next_timeout(remaining) {
                Ok(Some(event)) => {
                    let result = {
                        let mut core = host.core.lock().unwrap_or_else(|error| error.into_inner());
                        if core.execution_is_terminal(&run_id).unwrap_or(true) {
                            Ok(true)
                        } else {
                            let before = core.event_cursor();
                            let result = core.apply_runtime_dispatch_event(&run_id, event);
                            let after = core.event_cursor();
                            let _ = host.publish_if_changed(&core, before, after);
                            result
                        }
                    };
                    match result {
                        Ok(true) => return,
                        Ok(false) => {}
                        Err(_) => {
                            let _ = dispatch.stream.cancel();
                            let mut core =
                                host.core.lock().unwrap_or_else(|error| error.into_inner());
                            let before = core.event_cursor();
                            let _ = core.fail_runtime_dispatch(
                                &run_id,
                                &RuntimeError::Protocol("runtime event route rejected".into()),
                            );
                            let after = core.event_cursor();
                            let _ = host.publish_if_changed(&core, before, after);
                            return;
                        }
                    }
                }
                Ok(None) => {
                    let mut core = host.core.lock().unwrap_or_else(|error| error.into_inner());
                    if core.execution_is_terminal(&run_id).unwrap_or(true) {
                        return;
                    }
                    let before = core.event_cursor();
                    let _ =
                        core.fail_runtime_dispatch(&run_id, &RuntimeError::StreamTerminalMissing);
                    let after = core.event_cursor();
                    let _ = host.publish_if_changed(&core, before, after);
                    return;
                }
                Err(error) => {
                    if error == RuntimeError::Cancelled {
                        let core = host
                            .core
                            .lock()
                            .unwrap_or_else(|poison| poison.into_inner());
                        if core.execution_is_terminal(&run_id).unwrap_or(true) {
                            return;
                        }
                    }
                    let mut core = host
                        .core
                        .lock()
                        .unwrap_or_else(|poison| poison.into_inner());
                    let before = core.event_cursor();
                    let _ = core.fail_runtime_dispatch(&run_id, &error);
                    let after = core.event_cursor();
                    let _ = host.publish_if_changed(&core, before, after);
                    return;
                }
            }
        }
    });
}

#[cfg(not(windows))]
fn main() {
    eprintln!("AgentTalk Core Named Pipe host requires Windows");
}

#[cfg(windows)]
fn handle_connection(
    connection: NamedPipeConnection,
    host: &Arc<CoreHost>,
    expected_session_credential: &str,
    server_epoch: &str,
) -> Result<bool, Box<dyn Error>> {
    let (reader, writer) = connection.into_split()?;
    let (reader_rx, reader_stop, reader_thread) = spawn_reader(reader);
    let (writer_tx, writer_thread) = spawn_writer(writer);
    let result = handle_connection_owner(
        &reader_rx,
        &writer_tx,
        host,
        expected_session_credential,
        server_epoch,
    );
    reader_stop.store(true, Ordering::Release);
    drop(writer_tx);
    let _ = reader_thread.join();
    let _ = writer_thread.join();
    result
}

#[cfg(windows)]
fn spawn_reader(mut reader: NamedPipeReader) -> ReaderRuntime {
    let (tx, rx) = mpsc::channel();
    let stop = Arc::new(AtomicBool::new(false));
    let thread_stop = Arc::clone(&stop);
    let thread = thread::spawn(move || loop {
        if thread_stop.load(Ordering::Acquire) {
            break;
        }
        match reader.try_read_json() {
            Ok(Some(bytes)) => {
                if tx.send(Ok(bytes)).is_err() {
                    break;
                }
            }
            Ok(None) => thread::sleep(Duration::from_millis(5)),
            Err(error) => {
                let _ = tx.send(Err(error));
                break;
            }
        }
    });
    (rx, stop, thread)
}

#[cfg(windows)]
fn spawn_writer(mut writer: NamedPipeWriter) -> (WriterQueue, thread::JoinHandle<()>) {
    let (tx, rx) = mpsc::channel::<Value>();
    let thread = thread::spawn(move || {
        while let Ok(value) = rx.recv() {
            if writer.write_json(&value).is_err() {
                break;
            }
        }
    });
    (tx, thread)
}

#[cfg(windows)]
fn next_frame(
    reader_rx: &Receiver<Result<Vec<u8>, TransportError>>,
) -> Result<Vec<u8>, TransportError> {
    reader_rx.recv().unwrap_or(Err(TransportError::Closed))
}

#[cfg(windows)]
fn handle_connection_owner(
    reader_rx: &Receiver<Result<Vec<u8>, TransportError>>,
    writer: &WriterQueue,
    host: &Arc<CoreHost>,
    expected_session_credential: &str,
    server_epoch: &str,
) -> Result<bool, Box<dyn Error>> {
    let handshake: ProtocolHandshake = serde_json::from_slice(&next_frame(reader_rx)?)?;
    let session = match validate_handshake(&handshake, expected_session_credential, server_epoch) {
        Ok(session) => session,
        Err(code) => {
            write_error(writer, code, "IPC handshake rejected", false, "handshake")?;
            return Ok(false);
        }
    };
    if test_fixture_hangs("handshake") {
        test_fixture_wait_forever();
    }
    write_response(
        writer,
        "handshake",
        json!({
            "protocolMajor": PROTOCOL_MAJOR,
            "maxMessageBytes": handshake.max_message_bytes,
            "serverEpoch": server_epoch,
            "eventStreamId": "core-events",
        }),
    )?;
    loop {
        let bytes = match next_frame(reader_rx) {
            Ok(value) => value,
            Err(TransportError::Closed) => return Ok(false),
            Err(error) => return Err(error.into()),
        };
        let envelope: Value = match serde_json::from_slice(&bytes) {
            Ok(value) => value,
            Err(_) => {
                write_error(
                    writer,
                    "INVALID_ENVELOPE",
                    "IPC envelope is not valid JSON",
                    false,
                    "unknown",
                )?;
                continue;
            }
        };
        if !envelope.is_object() {
            write_error(
                writer,
                "INVALID_ENVELOPE",
                "IPC envelope must be a JSON object",
                false,
                "unknown",
            )?;
            continue;
        }
        let request_id = request_id_for_error(&envelope);
        match envelope.get("kind").and_then(Value::as_str) {
            Some("command") => {
                let command: CommandEnvelope = match serde_json::from_value(envelope) {
                    Ok(command) => command,
                    Err(_) => {
                        write_error(
                            writer,
                            "INVALID_COMMAND",
                            "IPC command envelope is malformed",
                            false,
                            &request_id,
                        )?;
                        continue;
                    }
                };
                if let Err(code) =
                    validate_bound_request(&session, &command.session_id, &command.protocol)
                {
                    write_error(
                        writer,
                        code,
                        "IPC session is not authenticated",
                        false,
                        &command.request_id,
                    )?;
                    continue;
                }
                if !command.payload.is_object() {
                    write_error(
                        writer,
                        "INVALID_COMMAND",
                        "command payload must be a JSON object",
                        false,
                        &command.request_id,
                    )?;
                    continue;
                }
                if test_fixture_hangs("requests") {
                    test_fixture_wait_forever();
                }
                if command.command == "events.subscribe" {
                    handle_subscription(reader_rx, writer, host, &session, command)?;
                    return Ok(false);
                }
                if matches!(
                    command.command.as_str(),
                    "agent.discovery.start"
                        | "agent.discovery.verify"
                        | "agent.discovery.dismiss"
                        | "agent.import_local"
                ) {
                    handle_local_discovery_command(writer, host, &session, command)?;
                    continue;
                }
                if command.command == "shutdown_owned" {
                    host.cancel_discovery_sessions();
                }
                let mut core = host.core.lock().unwrap_or_else(|error| error.into_inner());
                let before = core.event_cursor();
                let mut deferred_dispatches = Vec::new();
                let should_exit = handle_command(
                    writer,
                    &mut core,
                    &session,
                    command,
                    &mut deferred_dispatches,
                )?;
                let after = core.event_cursor();
                host.publish_if_changed(&core, before, after)?;
                drop(core);
                for run_id in deferred_dispatches {
                    spawn_runtime_dispatch(Arc::clone(host), run_id);
                }
                if should_exit {
                    host.shutdown_requested.store(true, Ordering::Release);
                    return Ok(true);
                }
            }
            Some("query") => {
                let query: QueryEnvelope = match serde_json::from_value(envelope) {
                    Ok(query) => query,
                    Err(_) => {
                        write_error(
                            writer,
                            "INVALID_QUERY",
                            "IPC query envelope is malformed",
                            false,
                            &request_id,
                        )?;
                        continue;
                    }
                };
                if let Err(code) =
                    validate_bound_request(&session, &query.session_id, &query.protocol)
                {
                    write_error(
                        writer,
                        code,
                        "IPC session is not authenticated",
                        false,
                        &query.request_id,
                    )?;
                    continue;
                }
                if !query.payload.is_object() {
                    write_error(
                        writer,
                        "INVALID_QUERY",
                        "query payload must be a JSON object",
                        false,
                        &query.request_id,
                    )?;
                    continue;
                }
                if query.query == "projection.snapshot" && test_fixture_hangs("snapshot") {
                    test_fixture_wait_forever();
                }
                if test_fixture_hangs("requests") {
                    test_fixture_wait_forever();
                }
                if query.query == "events.replay" {
                    handle_event_replay(writer, host, &session, query)?;
                } else if matches!(
                    query.query.as_str(),
                    "agent.discovery.snapshot" | "agent.import.plan"
                ) {
                    handle_local_discovery_query(writer, host, &session, query)?;
                } else {
                    let core = host.core.lock().unwrap_or_else(|error| error.into_inner());
                    handle_query(writer, &core, query)?;
                }
            }
            _ => write_error(
                writer,
                "INVALID_ENVELOPE",
                "IPC envelope kind is unsupported",
                false,
                &request_id,
            )?,
        }
    }
}

#[cfg(windows)]
fn handle_local_discovery_command(
    connection: &WriterQueue,
    host: &Arc<CoreHost>,
    session: &SessionBinding,
    command: CommandEnvelope,
) -> Result<(), Box<dyn Error>> {
    let owner = session.discovery_owner_scope();
    let event_owner = owner.clone();
    let event_host = Arc::clone(host);
    let event_sink = Arc::new(move |event| event_host.publish_discovery_event(&event_owner, event));
    let result = (|| -> Result<(), LocalDiscoveryRouteError> {
        match command.command.as_str() {
            "agent.discovery.start" => {
                match host
                    .discovery
                    .begin_start(&owner, &command.request_id, &command.payload)
                    .map_err(LocalDiscoveryRouteError::from)?
                {
                    StartScanOutcome::Replayed(mut response) => {
                        let publication = host.discovery.start_publication_guard();
                        let scan_id = response["scanId"]
                            .as_str()
                            .ok_or(LocalDiscoveryServiceError::ShuttingDown)?;
                        publication
                            .ensure_start_replay_publishable(scan_id, &owner)
                            .map_err(LocalDiscoveryRouteError::from)?;
                        let (_, discovery_epoch) = host
                            .try_discovery_event_stream(&owner)
                            .map_err(LocalDiscoveryRouteError::from)?;
                        response["eventStream"] = json!({
                            "streamId": DISCOVERY_EVENT_STREAM_ID,
                            "epoch": discovery_epoch,
                        });
                        #[cfg(test)]
                        host.pause_start_replay_before_response_for_tests();
                        write_response(connection, &command.request_id, response).map_err(|_| {
                            LocalDiscoveryRouteError::Service(
                                LocalDiscoveryServiceError::ShuttingDown,
                            )
                        })
                    }
                    StartScanOutcome::Reserved(reservation) => {
                        let stream_reservation = host
                            .reserve_discovery_event_stream_for_owner(&owner)
                            .map_err(LocalDiscoveryRouteError::from)?;
                        let worker_ready = reservation
                            .launch_worker_until_ready(stream_reservation.event_sink())
                            .map_err(LocalDiscoveryRouteError::from)?;
                        host.publish_discovery_start_response(
                            connection,
                            &command.request_id,
                            &owner,
                            worker_ready,
                            stream_reservation,
                        )
                    }
                }
            }
            "agent.discovery.verify" => {
                let (scan_id, candidate_id, consent, deadline) =
                    local_discovery_verify_parameters(&command.payload, command.deadline_ms)
                        .map_err(LocalDiscoveryRouteError::from)?;
                #[cfg(test)]
                host.discovery.pause_before_verify_publication_for_tests();
                let outcome = {
                    let publication = host.discovery.start_publication_guard();
                    host.discovery
                        .begin_verify_with_publication(
                            &publication,
                            VerifyRequest {
                                owner: &owner,
                                request_id: &command.request_id,
                                scan_id: &scan_id,
                                candidate_id: &candidate_id,
                                consent,
                                deadline,
                                event_sink,
                            },
                        )
                        .map_err(LocalDiscoveryRouteError::from)?
                };
                match outcome {
                    // A committed same-requestId replay and a W5.8
                    // business-idempotent reuse of a still-valid verification
                    // are both served without launching any new ACP work.
                    VerifyStartOutcome::Replayed(response)
                    | VerifyStartOutcome::AlreadyVerified(response) => {
                        let publication = host.discovery.start_publication_guard();
                        let scan_id = response["scanId"]
                            .as_str()
                            .ok_or(LocalDiscoveryServiceError::ShuttingDown)?;
                        host.discovery
                            .ensure_import_plan_publishable(&publication, &owner, scan_id)
                            .map_err(LocalDiscoveryRouteError::from)?;
                        write_response(connection, &command.request_id, response).map_err(|_| {
                            LocalDiscoveryRouteError::Service(
                                LocalDiscoveryServiceError::ShuttingDown,
                            )
                        })
                    }
                    VerifyStartOutcome::Reserved(reservation) => {
                        let worker_ready = reservation
                            .launch_worker_until_ready()
                            .map_err(LocalDiscoveryRouteError::from)?;
                        host.publish_discovery_verify_response(
                            connection,
                            &command.request_id,
                            &owner,
                            worker_ready,
                        )
                    }
                }
            }
            "agent.discovery.dismiss" => {
                let (scan_id, candidate_id) = local_discovery_dismiss_parameters(&command.payload)
                    .map_err(LocalDiscoveryRouteError::from)?;
                #[cfg(test)]
                host.discovery.pause_before_dismiss_publication_for_tests();
                host.publish_discovery_mutation_response(
                    connection,
                    &command.request_id,
                    &owner,
                    &scan_id,
                    |publication| {
                        host.discovery.dismiss_with_publication(
                            publication,
                            &owner,
                            &command.request_id,
                            &scan_id,
                            &candidate_id,
                            event_sink,
                        )
                    },
                )
            }
            "agent.import_local" => {
                let (scan_id, candidate_id, project_id, model_selection) =
                    local_discovery_import_plan_parameters(&command.payload)
                        .map_err(LocalDiscoveryRouteError::from)?;
                host.publish_local_agent_import_response(
                    connection,
                    LocalImportPublicationRequest {
                        request_id: &command.request_id,
                        client_id: &session.client_id,
                        owner: &owner,
                        scan_id: &scan_id,
                        candidate_id: &candidate_id,
                        project_id: &project_id,
                        model_selection: model_selection.as_deref(),
                        // The idempotency hash covers only the business import
                        // intent; the envelope deadlineMs must not change it.
                        payload_hash: local_import_payload_hash(
                            &scan_id,
                            &candidate_id,
                            &project_id,
                            model_selection.as_deref(),
                        ),
                    },
                )
            }
            _ => unreachable!("local discovery command route is explicit"),
        }
    })();
    match result {
        Ok(()) => {}
        Err(error) => write_local_discovery_route_error(connection, error, &command.request_id)?,
    }
    Ok(())
}

#[cfg(windows)]
#[derive(Debug)]
enum LocalDiscoveryRouteError {
    Service(LocalDiscoveryServiceError),
    Stream(DiscoveryStreamError),
}

#[cfg(windows)]
impl From<LocalDiscoveryServiceError> for LocalDiscoveryRouteError {
    fn from(value: LocalDiscoveryServiceError) -> Self {
        Self::Service(value)
    }
}

#[cfg(windows)]
impl From<DiscoveryStreamError> for LocalDiscoveryRouteError {
    fn from(value: DiscoveryStreamError) -> Self {
        Self::Stream(value)
    }
}

#[cfg(windows)]
fn handle_local_discovery_query(
    connection: &WriterQueue,
    host: &Arc<CoreHost>,
    session: &SessionBinding,
    query: QueryEnvelope,
) -> Result<(), Box<dyn Error>> {
    let owner = session.discovery_owner_scope();
    if query.query == "agent.import.plan" {
        let result = (|| -> Result<(), LocalDiscoveryRouteError> {
            let (scan_id, candidate_id, project_id, model_selection) =
                local_discovery_import_plan_parameters(&query.payload)
                    .map_err(LocalDiscoveryRouteError::from)?;
            #[cfg(test)]
            host.discovery
                .pause_before_import_plan_publication_for_tests();
            host.publish_discovery_import_plan_response(
                connection,
                ImportPlanPublicationRequest {
                    request_id: &query.request_id,
                    owner: &owner,
                    scan_id: &scan_id,
                    candidate_id: &candidate_id,
                    project_id: &project_id,
                    model_selection: model_selection.as_deref(),
                },
            )
        })();
        match result {
            Ok(()) => {}
            Err(error) => write_local_discovery_route_error(connection, error, &query.request_id)?,
        }
        return Ok(());
    }

    let result = match query.query.as_str() {
        "agent.discovery.snapshot" => local_discovery_snapshot_parameters(&query.payload)
            .and_then(|scan_id| host.discovery.snapshot(&owner, &scan_id)),
        _ => unreachable!("W5 query route is explicit"),
    };
    match result {
        Ok(payload) => write_response(connection, &query.request_id, payload)?,
        Err(error) => write_local_discovery_error(connection, error, &query.request_id)?,
    }
    Ok(())
}

#[cfg(windows)]
fn write_local_discovery_error(
    connection: &WriterQueue,
    error: LocalDiscoveryServiceError,
    request_id: &str,
) -> Result<(), TransportError> {
    write_error(
        connection,
        error.code(),
        error.message(),
        matches!(error, LocalDiscoveryServiceError::StartInProgress),
        request_id,
    )
}

#[cfg(windows)]
fn write_discovery_stream_error(
    connection: &WriterQueue,
    error: DiscoveryStreamError,
    request_id: &str,
) -> Result<(), TransportError> {
    write_error(connection, error.code(), error.message(), false, request_id)
}

#[cfg(windows)]
fn write_local_discovery_route_error(
    connection: &WriterQueue,
    error: LocalDiscoveryRouteError,
    request_id: &str,
) -> Result<(), TransportError> {
    match error {
        LocalDiscoveryRouteError::Service(error) => {
            write_local_discovery_error(connection, error, request_id)
        }
        LocalDiscoveryRouteError::Stream(error) => {
            write_discovery_stream_error(connection, error, request_id)
        }
    }
}

#[cfg(windows)]
fn local_discovery_verify_parameters(
    payload: &Value,
    command_deadline_ms: Option<u64>,
) -> Result<(String, String, bool, Duration), LocalDiscoveryServiceError> {
    reject_unknown_payload_fields(payload, &["scanId", "candidateId", "consent", "deadlineMs"])?;
    let scan_id = strict_discovery_id(payload, "scanId")?;
    let candidate_id = strict_discovery_id(payload, "candidateId")?;
    let consent = payload
        .get("consent")
        .and_then(Value::as_bool)
        .ok_or(LocalDiscoveryServiceError::InvalidPayload)?;
    let payload_deadline = match payload.get("deadlineMs") {
        None => None,
        Some(value) => Some(
            value
                .as_u64()
                .filter(|value| (100..=30_000).contains(value))
                .ok_or(LocalDiscoveryServiceError::InvalidPayload)?,
        ),
    };
    let command_deadline = command_deadline_ms
        .filter(|value| (100..=30_000).contains(value))
        .unwrap_or(5_000);
    let deadline_ms = payload_deadline
        .unwrap_or(command_deadline)
        .min(command_deadline);
    Ok((
        scan_id,
        candidate_id,
        consent,
        Duration::from_millis(deadline_ms),
    ))
}

#[cfg(windows)]
fn local_discovery_dismiss_parameters(
    payload: &Value,
) -> Result<(String, String), LocalDiscoveryServiceError> {
    reject_unknown_payload_fields(payload, &["scanId", "candidateId"])?;
    Ok((
        strict_discovery_id(payload, "scanId")?,
        strict_discovery_id(payload, "candidateId")?,
    ))
}

#[cfg(windows)]
fn local_discovery_snapshot_parameters(
    payload: &Value,
) -> Result<String, LocalDiscoveryServiceError> {
    reject_unknown_payload_fields(payload, &["scanId"])?;
    strict_discovery_id(payload, "scanId")
}

#[cfg(windows)]
fn local_discovery_import_plan_parameters(
    payload: &Value,
) -> Result<(String, String, String, Option<String>), LocalDiscoveryServiceError> {
    reject_unknown_payload_fields(
        payload,
        &["scanId", "candidateId", "projectId", "modelSelection"],
    )?;
    let model_selection = match payload.get("modelSelection") {
        None | Some(Value::Null) => None,
        Some(Value::String(value)) if is_safe_discovery_id(value) => Some(value.clone()),
        Some(_) => return Err(LocalDiscoveryServiceError::InvalidPayload),
    };
    Ok((
        strict_discovery_id(payload, "scanId")?,
        strict_discovery_id(payload, "candidateId")?,
        strict_discovery_id(payload, "projectId")?,
        model_selection,
    ))
}

#[cfg(windows)]
fn local_agent_import_request(
    request: &LocalImportPublicationRequest<'_>,
    work: LocalImportWork,
) -> LocalAgentImportRequest {
    let model_selection = match work.model_selection {
        Some(model_id) => ModelSelection {
            mode: ModelSelectionMode::Pinned,
            model_id: Some(model_id),
        },
        None => ModelSelection {
            mode: ModelSelectionMode::ConnectorDefault,
            model_id: None,
        },
    };
    let mut hasher = Sha256::new();
    for value in [
        work.scan_id.as_str(),
        work.candidate_id.as_str(),
        work.project_id.as_str(),
        work.metadata.candidate_binding_digest.as_str(),
    ] {
        hasher.update(value.as_bytes());
        hasher.update([0xff]);
    }
    let stable = hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    LocalAgentImportRequest {
        import_id: format!("local-import-{}", &stable[..32]),
        scope_id: CONNECTOR_PROFILE_SCOPE.into(),
        client_id: request.client_id.into(),
        request_id: request.request_id.into(),
        payload_hash: request.payload_hash.clone(),
        project_id: work.project_id,
        connector: ConnectorProfile {
            scope_id: CONNECTOR_PROFILE_SCOPE.into(),
            connector_id: work.projection.connector_id,
            display_name: work.projection.display_name.clone(),
            provider_type: "local_agent".into(),
            runtime_type: work.projection.runtime_type,
            enabled: true,
            auth_env_key: None,
        },
        agent_id: format!("local-agent-{}", &stable[..32]),
        agent_name: work.projection.display_name,
        binding: LocalAgentAdapterBinding {
            adapter_kind: work.metadata.adapter_kind,
            protocol_major: work.metadata.protocol_major,
            manifest_id: work.metadata.manifest_id,
            manifest_sha256: work.metadata.manifest_sha256,
            candidate_binding_digest: work.metadata.candidate_binding_digest,
            capabilities_json: serde_json::to_string(&work.metadata.capabilities)
                .expect("ACP capability metadata is serializable"),
            auth_required: work.metadata.auth_required,
        },
        model_selection,
    }
}

/// Stable idempotency hash for `agent.import_local`. It covers only the
/// business import intent: the fixed command name plus the allowlist-parsed
/// payload fields. Envelope metadata such as `deadlineMs` is deliberately
/// excluded so a retry with a different deadline replays instead of
/// conflicting.
#[cfg(windows)]
fn local_import_payload_hash(
    scan_id: &str,
    candidate_id: &str,
    project_id: &str,
    model_selection: Option<&str>,
) -> String {
    let bytes = serde_json::to_vec(&json!({
        "command": "agent.import_local",
        "scanId": scan_id,
        "candidateId": candidate_id,
        "projectId": project_id,
        "modelSelection": model_selection,
    }))
    .unwrap_or_default();
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Classifies a durable import failure into a stable, renderer-safe typed
/// error. Only the storage conflicts named by the W6 review become
/// IMPORT_CONFLICT; every other persistence failure becomes
/// IMPORT_PERSISTENCE_FAILED without leaking SQLite text, paths, tokens, or
/// database details.
#[cfg(windows)]
fn local_import_storage_error(error: &StorageError) -> LocalDiscoveryServiceError {
    match error {
        StorageError::LocalAgentImportBindingConflict
        | StorageError::ConnectorProfileConflict { .. }
        | StorageError::LocalAgentImportRequestConflict
        | StorageError::LocalAgentImportModelSelectionConflict => {
            LocalDiscoveryServiceError::ImportConflict
        }
        _ => LocalDiscoveryServiceError::ImportPersistenceFailed,
    }
}

/// Maps the Core-level import outcome error. The identity recheck itself is
/// performed earlier by `execute_local_import` and still surfaces as
/// `DISCOVERY_IDENTITY_CHANGED`; this layer only classifies durable-write
/// failures.
#[cfg(windows)]
fn local_import_outcome_error(error: &CoreError) -> LocalDiscoveryServiceError {
    match error {
        CoreError::Storage(storage_error) => local_import_storage_error(storage_error),
        _ => LocalDiscoveryServiceError::ImportPersistenceFailed,
    }
}

#[cfg(windows)]
fn reject_unknown_payload_fields(
    payload: &Value,
    allowed: &[&str],
) -> Result<(), LocalDiscoveryServiceError> {
    let object = payload
        .as_object()
        .ok_or(LocalDiscoveryServiceError::InvalidPayload)?;
    object
        .keys()
        .all(|key| allowed.contains(&key.as_str()))
        .then_some(())
        .ok_or(LocalDiscoveryServiceError::InvalidPayload)
}

#[cfg(windows)]
fn strict_discovery_id(payload: &Value, key: &str) -> Result<String, LocalDiscoveryServiceError> {
    payload
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| is_safe_discovery_id(value))
        .map(str::to_owned)
        .ok_or(LocalDiscoveryServiceError::InvalidPayload)
}

#[cfg(windows)]
fn is_safe_discovery_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._:-".contains(&byte))
}

#[cfg(windows)]
fn handle_subscription(
    reader_rx: &Receiver<Result<Vec<u8>, TransportError>>,
    writer: &WriterQueue,
    host: &Arc<CoreHost>,
    session: &SessionBinding,
    command: CommandEnvelope,
) -> Result<(), Box<dyn Error>> {
    let owner = session.discovery_owner_scope();
    let (stream_kind, after_sequence, requested_epoch) = match subscription_cursor(&command.payload)
    {
        Ok(cursor) => cursor,
        Err(code) => {
            write_error(
                writer,
                code,
                "subscription cursor is not valid for this Core epoch",
                false,
                &command.request_id,
            )?;
            return Ok(());
        }
    };
    let (events, server_epoch) = match host.event_stream(stream_kind, &owner) {
        Ok(stream) => stream,
        Err(error) => {
            write_discovery_stream_error(writer, error, &command.request_id)?;
            return Ok(());
        }
    };
    if requested_epoch
        .as_deref()
        .is_some_and(|epoch| epoch != server_epoch)
    {
        write_error(
            writer,
            "CURSOR_EPOCH_MISMATCH",
            "subscription cursor is not valid for this Core epoch",
            false,
            &command.request_id,
        )?;
        return Ok(());
    }
    let max_events = match bounded_subscription_events(&command.payload) {
        Ok(value) => value,
        Err(message) => {
            write_error(
                writer,
                "INVALID_COMMAND",
                message,
                false,
                &command.request_id,
            )?;
            return Ok(());
        }
    };
    let max_bytes = match bounded_subscription_bytes(&command.payload) {
        Ok(value) => value,
        Err(message) => {
            write_error(
                writer,
                "INVALID_COMMAND",
                message,
                false,
                &command.request_id,
            )?;
            return Ok(());
        }
    };
    if let Some(gap) = events.gap(after_sequence) {
        write_replay_gap(
            writer,
            &command.request_id,
            &gap,
            stream_kind.id(),
            &server_epoch,
        )?;
        return Ok(());
    }
    let subscription_id = format!(
        "sub-{}-{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis()
    );
    let discovery_subscription = if stream_kind == EventStreamKind::Discovery {
        match host.begin_discovery_subscription(&owner) {
            Ok(lease) => Some(lease),
            Err(error) => {
                write_discovery_stream_error(writer, error, &command.request_id)?;
                return Ok(());
            }
        }
    } else {
        None
    };
    let session_id = command.session_id.clone();
    write_response(
        writer,
        &command.request_id,
        json!({
            "subscriptionId": subscription_id,
            "streamId": stream_kind.id(),
            "cursor": {
                "streamId": stream_kind.id(),
                "sequence": after_sequence,
                "epoch": server_epoch,
            },
            "maxInFlightEvents": max_events,
            "maxInFlightBytes": max_bytes,
        }),
    )?;

    let shared = Arc::new(SharedSubscription::new(after_sequence));
    let event_shared = Arc::clone(&shared);
    let event_host = Arc::clone(host);
    let event_hub = Arc::clone(&events);
    let event_writer = writer.clone();
    let event_session_id = session_id.clone();
    let event_subscription_id = subscription_id.clone();
    let event_request_id = command.request_id.clone();
    let event_server_epoch = server_epoch.to_owned();
    let event_stream_kind = stream_kind;
    let event_thread = thread::spawn(move || {
        let pump = SubscriptionEventPump {
            shared: event_shared,
            host: event_host,
            event_hub,
            writer: event_writer,
            session_id: event_session_id,
            subscription_id: event_subscription_id,
            request_id: event_request_id,
            stream_kind: event_stream_kind,
            server_epoch: event_server_epoch,
            max_events,
            max_bytes,
            _discovery_subscription: discovery_subscription,
        };
        let request_id = pump.request_id.clone();
        let writer = pump.writer.clone();
        let shared = Arc::clone(&pump.shared);
        if let Err(message) = run_subscription_events(pump) {
            let _ = write_error(&writer, "QUERY_REJECTED", &message, false, &request_id);
            shared.close();
        }
    });

    let result = (|| -> Result<(), Box<dyn Error>> {
        loop {
            if host.shutdown_requested.load(Ordering::Acquire) {
                break Ok(());
            }
            if shared
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .closed
            {
                break Ok(());
            }
            match reader_rx.recv_timeout(Duration::from_millis(20)) {
                Ok(Ok(bytes)) => {
                    let value: Value = match serde_json::from_slice(&bytes) {
                        Ok(value) => value,
                        Err(_) => {
                            write_error(
                                writer,
                                "INVALID_COMMAND",
                                "subscription control frame is malformed",
                                false,
                                &command.request_id,
                            )?;
                            continue;
                        }
                    };
                    let control: CommandEnvelope = match serde_json::from_value(value) {
                        Ok(control) => control,
                        Err(_) => {
                            write_error(
                                writer,
                                "INVALID_COMMAND",
                                "subscription control frame is malformed",
                                false,
                                &command.request_id,
                            )?;
                            continue;
                        }
                    };
                    if let Err(code) =
                        validate_bound_request(session, &control.session_id, &control.protocol)
                    {
                        write_error(
                            writer,
                            code,
                            "IPC session is not authenticated",
                            false,
                            &control.request_id,
                        )?;
                        continue;
                    }
                    match control.command.as_str() {
                        "events.ack" => {
                            let requested_subscription = control
                                .payload
                                .get("subscriptionId")
                                .and_then(Value::as_str);
                            let ack_cursor = control
                                .payload
                                .get("cursor")
                                .cloned()
                                .ok_or("ack cursor is missing")
                                .and_then(|value| {
                                    serde_json::from_value::<StreamCursor>(value)
                                        .map_err(|_| "ack cursor is malformed")
                                });
                            let ack_cursor = match ack_cursor {
                                Ok(value) => value,
                                Err(message) => {
                                    write_error(
                                        writer,
                                        "INVALID_ACK",
                                        message,
                                        false,
                                        &control.request_id,
                                    )?;
                                    continue;
                                }
                            };
                            let mut state = shared
                                .state
                                .lock()
                                .unwrap_or_else(|error| error.into_inner());
                            if requested_subscription != Some(subscription_id.as_str())
                                || ack_cursor.stream_id != stream_kind.id()
                                || ack_cursor.epoch.as_deref() != Some(server_epoch.as_str())
                                || ack_cursor.sequence < state.last_acked
                                || ack_cursor.sequence > state.cursor
                            {
                                write_error(
                                    writer,
                                    "INVALID_ACK",
                                    "ack cursor is outside the subscription window",
                                    false,
                                    &control.request_id,
                                )?;
                                continue;
                            }
                            while let Some((sequence, bytes)) = state.in_flight.front().copied() {
                                if sequence > ack_cursor.sequence {
                                    break;
                                }
                                state.in_flight.pop_front();
                                state.in_flight_bytes = state.in_flight_bytes.saturating_sub(bytes);
                            }
                            state.last_acked = ack_cursor.sequence;
                            write_response(
                                writer,
                                &control.request_id,
                                json!({"acknowledged": true, "cursor": ack_cursor}),
                            )?;
                            shared.changed.notify_all();
                            drop(state);
                        }
                        "events.unsubscribe" => {
                            if control
                                .payload
                                .get("subscriptionId")
                                .and_then(Value::as_str)
                                != Some(subscription_id.as_str())
                            {
                                write_error(
                                    writer,
                                    "SUBSCRIPTION_NOT_FOUND",
                                    "subscriptionId is not active on this connection",
                                    false,
                                    &control.request_id,
                                )?;
                                continue;
                            }
                            write_response(
                                writer,
                                &control.request_id,
                                json!({"unsubscribed": true}),
                            )?;
                            break Ok(());
                        }
                        _ => {
                            write_error(
                            writer,
                            "INVALID_COMMAND",
                            "only events.ack or events.unsubscribe is allowed on a subscription",
                            false,
                            &control.request_id,
                        )?;
                        }
                    }
                }
                Ok(Err(TransportError::Closed)) | Err(RecvTimeoutError::Disconnected) => {
                    break Ok(())
                }
                Ok(Err(error)) => break Err(error.into()),
                Err(RecvTimeoutError::Timeout) => continue,
            }
        }
    })();
    shared.close();
    let _ = event_thread.join();
    result
}

#[cfg(windows)]
fn run_subscription_events(pump: SubscriptionEventPump) -> Result<(), String> {
    let shared = &pump.shared;
    let host = &pump.host;
    let writer = &pump.writer;
    let session_id = &pump.session_id;
    let subscription_id = &pump.subscription_id;
    let request_id = &pump.request_id;
    let stream_kind = pump.stream_kind;
    let server_epoch = &pump.server_epoch;
    let max_events = pump.max_events;
    let max_bytes = pump.max_bytes;
    loop {
        if host.shutdown_requested.load(Ordering::Acquire) {
            shared.close();
            return Ok(());
        }
        let cursor = {
            let state = shared
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if state.closed {
                return Ok(());
            }
            if state.in_flight.len() >= max_events as usize || state.in_flight_bytes >= max_bytes {
                let _ = shared
                    .changed
                    .wait_timeout(state, Duration::from_millis(20));
                continue;
            }
            state.cursor
        };
        let events = match pump
            .event_hub
            .replay_after(cursor, max_events.saturating_add(1))
        {
            Ok(events) => events,
            Err(gap) => {
                write_replay_gap(writer, request_id, &gap, stream_kind.id(), server_epoch)
                    .map_err(|error| error.to_string())?;
                shared.close();
                return Ok(());
            }
        };
        let mut sent = false;
        for event in events {
            let envelope = runtime_event_json(
                event.clone(),
                session_id,
                stream_kind.id(),
                server_epoch,
                Some(subscription_id),
            );
            let event_bytes = serde_json::to_vec(&envelope)
                .map_err(|error| error.to_string())?
                .len();
            let mut state = shared
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if state.closed {
                return Ok(());
            }
            if event_bytes > max_bytes {
                drop(state);
                write_error(
                    writer,
                    "SUBSCRIPTION_OVERFLOW",
                    "one event exceeds the subscription byte window",
                    false,
                    request_id,
                )
                .map_err(|error| error.to_string())?;
                shared.close();
                return Ok(());
            }
            if state.in_flight.len() >= max_events as usize
                || state.in_flight_bytes.saturating_add(event_bytes) > max_bytes
            {
                break;
            }
            state.cursor = event.sequence;
            state.in_flight.push_back((event.sequence, event_bytes));
            state.in_flight_bytes = state.in_flight_bytes.saturating_add(event_bytes);
            drop(state);
            if writer.send(envelope).is_err() {
                shared.close();
                return Ok(());
            }
            sent = true;
        }
        if !sent {
            pump.event_hub
                .wait_for_change(cursor, Duration::from_millis(20));
        }
    }
}

#[cfg(windows)]
fn handle_command(
    connection: &WriterQueue,
    core: &mut PersistentCore,
    session: &SessionBinding,
    command: CommandEnvelope,
    deferred_dispatches: &mut Vec<String>,
) -> Result<bool, Box<dyn Error>> {
    macro_rules! parse_or_error {
        ($value:expr) => {
            match $value {
                Ok(value) => value,
                Err(error) => {
                    write_error(
                        connection,
                        "INVALID_COMMAND",
                        &error.to_string(),
                        false,
                        &command.request_id,
                    )?;
                    return Ok(false);
                }
            }
        };
    }
    match command.command.as_str() {
        "orchestration.run.create" => {
            let result = (|| -> Result<Value, (&str, String, bool)> {
                reject_unknown_fields(
                    &command.payload,
                    &[
                        "projectId",
                        "runId",
                        "briefSnapshotId",
                        "briefTreeDigest",
                        "dagSnapshotDigest",
                        "roleBindingSnapshotDigest",
                    ],
                )
                .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                let project_id = required_string(&command.payload, "projectId")
                    .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                let run_id = required_string(&command.payload, "runId")
                    .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                let brief_snapshot_id = required_string(&command.payload, "briefSnapshotId")
                    .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                let brief_tree_digest =
                    required_orchestration_digest(&command.payload, "briefTreeDigest")
                        .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                let dag_snapshot_digest =
                    required_orchestration_digest(&command.payload, "dagSnapshotDigest")
                        .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                let role_binding_snapshot_digest =
                    required_orchestration_digest(&command.payload, "roleBindingSnapshotDigest")
                        .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                let run = core
                    .create_orchestration_run_from_prepared_facts(
                        &project_id,
                        &run_id,
                        &brief_snapshot_id,
                        &brief_tree_digest,
                        &dag_snapshot_digest,
                        &role_binding_snapshot_digest,
                    )
                    .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                let projection = core
                    .orchestration_projection(&run_id)
                    .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                Ok(json!({
                    "created": true,
                    "run": run,
                    "projection": projection,
                }))
            })();
            match result {
                Ok(payload) => write_response(connection, &command.request_id, payload)?,
                Err((code, message, retryable)) => {
                    write_error(connection, code, &message, retryable, &command.request_id)?
                }
            }
        }
        "orchestration.task.insert" => {
            let result = (|| -> Result<Value, (&str, String, bool)> {
                reject_unknown_fields(&command.payload, &["runId", "nodeId", "nodeKey"])
                    .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                let run_id = required_string(&command.payload, "runId")
                    .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                let node_id = required_string(&command.payload, "nodeId")
                    .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                let node_key = required_string(&command.payload, "nodeKey")
                    .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                core.insert_orchestration_task_node(&run_id, &node_id, &node_key)
                    .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                Ok(
                    json!({"created": true, "runId": run_id, "nodeId": node_id, "status": "pending"}),
                )
            })();
            match result {
                Ok(payload) => write_response(connection, &command.request_id, payload)?,
                Err((code, message, retryable)) => {
                    write_error(connection, code, &message, retryable, &command.request_id)?
                }
            }
        }
        "orchestration.task.ready" => {
            let result = (|| -> Result<Value, (&str, String, bool)> {
                reject_unknown_fields(
                    &command.payload,
                    &[
                        "nodeId",
                        "inputArtifactSetDigest",
                        "roleId",
                        "acceptanceContractRef",
                    ],
                )
                .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                let node_id = required_string(&command.payload, "nodeId")
                    .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                let input_digest =
                    required_orchestration_digest(&command.payload, "inputArtifactSetDigest")
                        .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                let role_id = required_string(&command.payload, "roleId")
                    .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                let contract_ref =
                    required_orchestration_object_ref(&command.payload, "acceptanceContractRef")
                        .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                core.mark_orchestration_task_ready(
                    &node_id,
                    &input_digest,
                    &role_id,
                    &contract_ref,
                )
                .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                Ok(json!({"changed": true, "nodeId": node_id, "status": "ready"}))
            })();
            match result {
                Ok(payload) => write_response(connection, &command.request_id, payload)?,
                Err((code, message, retryable)) => {
                    write_error(connection, code, &message, retryable, &command.request_id)?
                }
            }
        }
        "orchestration.task.start" => {
            let result = (|| -> Result<Value, (&str, String, bool)> {
                reject_unknown_fields(
                    &command.payload,
                    &["nodeId", "fromExecutionRunId", "leaseOwner"],
                )
                .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                let node_id = required_string(&command.payload, "nodeId")
                    .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                let from_execution_run_id = required_string(&command.payload, "fromExecutionRunId")
                    .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                let lease_owner = required_string(&command.payload, "leaseOwner")
                    .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                let outcome = core
                    .transition_orchestration_task_ready_to_running(
                        &node_id,
                        &from_execution_run_id,
                        &lease_owner,
                    )
                    .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                Ok(json!({"started": true, "outcome": outcome}))
            })();
            match result {
                Ok(payload) => write_response(connection, &command.request_id, payload)?,
                Err((code, message, retryable)) => {
                    write_error(connection, code, &message, retryable, &command.request_id)?
                }
            }
        }
        "orchestration.delivery.record" => {
            let result = (|| -> Result<Value, (&str, String, bool)> {
                reject_unknown_fields(&command.payload, &["delivery", "bindings"])
                    .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                let delivery_value = command.payload.get("delivery").cloned().ok_or_else(|| {
                    (
                        "INVALID_COMMAND",
                        "missing required field delivery".to_owned(),
                        false,
                    )
                })?;
                let bindings_value = command.payload.get("bindings").cloned().ok_or_else(|| {
                    (
                        "INVALID_COMMAND",
                        "missing required field bindings".to_owned(),
                        false,
                    )
                })?;
                let delivery: HandoffDeliveryRecord = serde_json::from_value(delivery_value)
                    .map_err(|error| {
                        (
                            "INVALID_COMMAND",
                            format!("invalid delivery: {error}"),
                            false,
                        )
                    })?;
                let bindings: Vec<ArtifactBindingInput> = serde_json::from_value(bindings_value)
                    .map_err(|error| {
                        (
                            "INVALID_COMMAND",
                            format!("invalid bindings: {error}"),
                            false,
                        )
                    })?;
                let delivery_id = delivery.delivery_id.clone();
                let replayed = core
                    .record_orchestration_handoff_delivery(delivery, &bindings)
                    .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                Ok(json!({
                    "deliveryId": delivery_id,
                    "replayed": replayed,
                    "journaled": true
                }))
            })();
            match result {
                Ok(payload) => write_response(connection, &command.request_id, payload)?,
                Err((code, message, retryable)) => {
                    write_error(connection, code, &message, retryable, &command.request_id)?
                }
            }
        }
        "orchestration.acceptance.record" => {
            let result = (|| -> Result<Value, (&str, String, bool)> {
                reject_unknown_fields(&command.payload, &["acceptance"])
                    .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                let acceptance_value =
                    command.payload.get("acceptance").cloned().ok_or_else(|| {
                        (
                            "INVALID_COMMAND",
                            "missing required field acceptance".to_owned(),
                            false,
                        )
                    })?;
                let acceptance: MachineAcceptanceRecord = serde_json::from_value(acceptance_value)
                    .map_err(|error| {
                        (
                            "INVALID_COMMAND",
                            format!("invalid acceptance: {error}"),
                            false,
                        )
                    })?;
                let acceptance_id = acceptance.acceptance_id.clone();
                let verdict = acceptance.verdict.clone();
                let replayed = core
                    .record_orchestration_machine_acceptance(acceptance)
                    .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                Ok(json!({
                    "acceptanceId": acceptance_id,
                    "verdict": verdict,
                    "replayed": replayed,
                    "recorded": true
                }))
            })();
            match result {
                Ok(payload) => write_response(connection, &command.request_id, payload)?,
                Err((code, message, retryable)) => {
                    write_error(connection, code, &message, retryable, &command.request_id)?
                }
            }
        }
        "orchestration.milestone.ensure" => {
            let result = (|| -> Result<Value, (&str, String, bool)> {
                reject_unknown_fields(
                    &command.payload,
                    &[
                        "runId",
                        "milestoneId",
                        "milestoneKey",
                        "briefTreeDigest",
                        "presentedArtifactSetDigest",
                        "acceptanceEvidenceDigest",
                    ],
                )
                .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                let run_id = required_string(&command.payload, "runId")
                    .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                let milestone_id = required_string(&command.payload, "milestoneId")
                    .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                let milestone_key = required_string(&command.payload, "milestoneKey")
                    .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                let brief_digest =
                    required_orchestration_digest(&command.payload, "briefTreeDigest")
                        .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                let artifact_digest =
                    required_orchestration_digest(&command.payload, "presentedArtifactSetDigest")
                        .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                let evidence_digest =
                    required_orchestration_digest(&command.payload, "acceptanceEvidenceDigest")
                        .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                core.ensure_orchestration_milestone(
                    &run_id,
                    &milestone_id,
                    &milestone_key,
                    &brief_digest,
                    &artifact_digest,
                    &evidence_digest,
                )
                .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                Ok(json!({
                    "milestoneId": milestone_id,
                    "status": "awaiting_approval",
                    "runId": run_id
                }))
            })();
            match result {
                Ok(payload) => write_response(connection, &command.request_id, payload)?,
                Err((code, message, retryable)) => {
                    write_error(connection, code, &message, retryable, &command.request_id)?
                }
            }
        }
        "orchestration.receipt.record" => {
            let result = (|| -> Result<Value, (&str, String, bool)> {
                reject_unknown_fields(
                    &command.payload,
                    &[
                        "receiptId",
                        "runId",
                        "milestoneId",
                        "requestId",
                        "semanticPayloadHash",
                        "decision",
                        "expectedVersion",
                        "briefTreeDigest",
                        "presentedArtifactSetDigest",
                        "acceptanceEvidenceDigest",
                    ],
                )
                .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                let decision = required_string(&command.payload, "decision")
                    .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                if !matches!(decision.as_str(), "approve" | "reject") {
                    return Err((
                        "INVALID_COMMAND",
                        "decision must be approve or reject".into(),
                        false,
                    ));
                }
                let semantic_hash =
                    required_orchestration_digest(&command.payload, "semanticPayloadHash")
                        .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                let receipt = HumanReceiptRecord {
                    receipt_id: required_string(&command.payload, "receiptId")
                        .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?,
                    run_id: required_string(&command.payload, "runId")
                        .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?,
                    milestone_id: required_string(&command.payload, "milestoneId")
                        .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?,
                    request_id: required_string(&command.payload, "requestId")
                        .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?,
                    semantic_payload_hash: semantic_hash,
                    decision: decision.clone(),
                    expected_version: required_i64(&command.payload, "expectedVersion")
                        .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?,
                    brief_tree_digest: required_orchestration_digest(
                        &command.payload,
                        "briefTreeDigest",
                    )
                    .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?,
                    presented_artifact_set_digest: required_orchestration_digest(
                        &command.payload,
                        "presentedArtifactSetDigest",
                    )
                    .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?,
                    acceptance_evidence_digest: required_orchestration_digest(
                        &command.payload,
                        "acceptanceEvidenceDigest",
                    )
                    .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?,
                    authenticated_principal: String::new(),
                    core_timestamp: 0,
                };
                let receipt_id = receipt.receipt_id.clone();
                let replayed = core
                    .record_orchestration_human_receipt(
                        receipt,
                        &format!("ipc-session:{}", command.session_id),
                    )
                    .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                Ok(json!({
                    "receiptId": receipt_id,
                    "decision": decision,
                    "replayed": replayed,
                    "recorded": true
                }))
            })();
            match result {
                Ok(payload) => write_response(connection, &command.request_id, payload)?,
                Err((code, message, retryable)) => {
                    write_error(connection, code, &message, retryable, &command.request_id)?
                }
            }
        }
        "orchestration.graph.bind" => {
            let result = (|| -> Result<Value, (&str, String, bool)> {
                reject_unknown_fields(
                    &command.payload,
                    &[
                        "runId",
                        "edges",
                        "edgePorts",
                        "roleBindings",
                        "contextAuthorities",
                    ],
                )
                .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                let run_id = required_string(&command.payload, "runId")
                    .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                let edges: Vec<OrchestrationEdgeInput> = serde_json::from_value(
                    command
                        .payload
                        .get("edges")
                        .cloned()
                        .unwrap_or_else(|| json!([])),
                )
                .map_err(|error| ("INVALID_COMMAND", format!("invalid edges: {error}"), false))?;
                let edge_ports: Vec<OrchestrationEdgePortInput> = serde_json::from_value(
                    command
                        .payload
                        .get("edgePorts")
                        .cloned()
                        .unwrap_or_else(|| json!([])),
                )
                .map_err(|error| {
                    (
                        "INVALID_COMMAND",
                        format!("invalid edgePorts: {error}"),
                        false,
                    )
                })?;
                let role_bindings: Vec<OrchestrationRoleBindingInput> = serde_json::from_value(
                    command
                        .payload
                        .get("roleBindings")
                        .cloned()
                        .unwrap_or_else(|| json!([])),
                )
                .map_err(|error| {
                    (
                        "INVALID_COMMAND",
                        format!("invalid roleBindings: {error}"),
                        false,
                    )
                })?;
                let context_authorities: Vec<OrchestrationContextAuthorityInput> =
                    serde_json::from_value(
                        command
                            .payload
                            .get("contextAuthorities")
                            .cloned()
                            .unwrap_or_else(|| json!([])),
                    )
                    .map_err(|error| {
                        (
                            "INVALID_COMMAND",
                            format!("invalid contextAuthorities: {error}"),
                            false,
                        )
                    })?;
                core.bind_orchestration_graph_facts(
                    &run_id,
                    &edges,
                    &edge_ports,
                    &role_bindings,
                    &context_authorities,
                )
                .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                Ok(json!({
                    "runId": run_id,
                    "edges": edges.len(),
                    "edgePorts": edge_ports.len(),
                    "roleBindings": role_bindings.len(),
                    "contextAuthorities": context_authorities.len()
                }))
            })();
            match result {
                Ok(payload) => write_response(connection, &command.request_id, payload)?,
                Err((code, message, retryable)) => {
                    write_error(connection, code, &message, retryable, &command.request_id)?
                }
            }
        }
        "project.create" => {
            let project = Project {
                id: parse_or_error!(required_string(&command.payload, "projectId")),
                name: parse_or_error!(required_string(&command.payload, "name")),
                root_path: parse_or_error!(optional_string(&command.payload, "rootPath")),
                archived: false,
            };
            match core.create_project(project) {
                Ok(()) => write_projection_mutation_response(
                    connection,
                    core,
                    &command.request_id,
                    "created",
                )?,
                Err(error) => write_error(
                    connection,
                    "COMMAND_REJECTED",
                    &error.to_string(),
                    false,
                    &command.request_id,
                )?,
            }
        }
        "project.update" => {
            let project = Project {
                id: parse_or_error!(required_string(&command.payload, "projectId")),
                name: parse_or_error!(required_string(&command.payload, "name")),
                root_path: parse_or_error!(optional_string(&command.payload, "rootPath")),
                archived: command
                    .payload
                    .get("archived")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            };
            match core.update_project(project) {
                Ok(()) => write_projection_mutation_response(
                    connection,
                    core,
                    &command.request_id,
                    "updated",
                )?,
                Err(error) => write_error(
                    connection,
                    "COMMAND_REJECTED",
                    &error.to_string(),
                    false,
                    &command.request_id,
                )?,
            }
        }
        "agent.create" => {
            let agent = parse_or_error!(agent_from_payload(&command.payload));
            match core.create_agent(agent) {
                Ok(()) => write_projection_mutation_response(
                    connection,
                    core,
                    &command.request_id,
                    "created",
                )?,
                Err(error) => write_error(
                    connection,
                    "COMMAND_REJECTED",
                    &error.to_string(),
                    false,
                    &command.request_id,
                )?,
            }
        }
        "agent.update" => {
            let agent = parse_or_error!(agent_from_payload(&command.payload));
            match core.update_agent(agent) {
                Ok(()) => write_projection_mutation_response(
                    connection,
                    core,
                    &command.request_id,
                    "updated",
                )?,
                Err(error) => write_error(
                    connection,
                    "COMMAND_REJECTED",
                    &error.to_string(),
                    false,
                    &command.request_id,
                )?,
            }
        }
        "conversation.create" => {
            let conversation = Conversation {
                id: parse_or_error!(required_string(&command.payload, "conversationId")),
                project_id: parse_or_error!(required_string(&command.payload, "projectId")),
                title: parse_or_error!(required_string(&command.payload, "title")),
                scope_revision: 0,
            };
            match core.create_conversation(conversation) {
                Ok(()) => write_projection_mutation_response(
                    connection,
                    core,
                    &command.request_id,
                    "created",
                )?,
                Err(error) => write_error(
                    connection,
                    "COMMAND_REJECTED",
                    &error.to_string(),
                    false,
                    &command.request_id,
                )?,
            }
        }
        "conversation.update" => {
            let conversation = Conversation {
                id: parse_or_error!(required_string(&command.payload, "conversationId")),
                project_id: String::new(),
                title: parse_or_error!(required_string(&command.payload, "title")),
                scope_revision: 0,
            };
            match core.update_conversation(conversation) {
                Ok(()) => write_projection_mutation_response(
                    connection,
                    core,
                    &command.request_id,
                    "updated",
                )?,
                Err(error) => write_error(
                    connection,
                    "COMMAND_REJECTED",
                    &error.to_string(),
                    false,
                    &command.request_id,
                )?,
            }
        }
        "connector.create" => match begin_command_receipt(core, session, &command)? {
            ReceiptDecision::ReplayResponse(payload) => {
                write_response(connection, &command.request_id, payload)?
            }
            ReceiptDecision::ReplayError {
                code,
                message,
                retryable,
            } => write_error(connection, &code, &message, retryable, &command.request_id)?,
            ReceiptDecision::New(mut receipt) => {
                core.save_command_receipt(&receipt)?;
                let result = (|| -> Result<Value, (&str, String, bool)> {
                    let profile = connector_profile_from_payload(&command.payload)
                        .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                    let created = core
                        .create_connector_profile(profile.clone())
                        .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                    if created {
                        core.record_projection_changed("connector-created")
                            .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                    }
                    Ok(json!({
                        "created": created,
                        "alreadyPresent": !created,
                        "connectorProfile": profile,
                        "projection": core.projection_snapshot()
                            .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?,
                    }))
                })();
                match result {
                    Ok(payload) => {
                        complete_command_receipt(core, &mut receipt, payload.clone())?;
                        write_response(connection, &command.request_id, payload)?;
                    }
                    Err((code, message, retryable)) => {
                        fail_command_receipt(core, &mut receipt, code, &message, retryable)?;
                        write_error(connection, code, &message, retryable, &command.request_id)?;
                    }
                }
            }
        },
        "connector.update" => match begin_command_receipt(core, session, &command)? {
            ReceiptDecision::ReplayResponse(payload) => {
                write_response(connection, &command.request_id, payload)?
            }
            ReceiptDecision::ReplayError {
                code,
                message,
                retryable,
            } => write_error(connection, &code, &message, retryable, &command.request_id)?,
            ReceiptDecision::New(mut receipt) => {
                core.save_command_receipt(&receipt)?;
                let result = (|| -> Result<Value, (&str, String, bool)> {
                    let profile = connector_profile_from_payload(&command.payload)
                        .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                    let changed = core
                        .update_connector_profile(profile.clone())
                        .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                    if changed {
                        core.record_projection_changed("connector-updated")
                            .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                    }
                    Ok(json!({
                        "updated": changed,
                        "alreadyCurrent": !changed,
                        "connectorProfile": profile,
                        "projection": core.projection_snapshot()
                            .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?,
                    }))
                })();
                match result {
                    Ok(payload) => {
                        complete_command_receipt(core, &mut receipt, payload.clone())?;
                        write_response(connection, &command.request_id, payload)?;
                    }
                    Err((code, message, retryable)) => {
                        fail_command_receipt(core, &mut receipt, code, &message, retryable)?;
                        write_error(connection, code, &message, retryable, &command.request_id)?;
                    }
                }
            }
        },
        "connector.remove" => match begin_command_receipt(core, session, &command)? {
            ReceiptDecision::ReplayResponse(payload) => {
                write_response(connection, &command.request_id, payload)?
            }
            ReceiptDecision::ReplayError {
                code,
                message,
                retryable,
            } => write_error(connection, &code, &message, retryable, &command.request_id)?,
            ReceiptDecision::New(mut receipt) => {
                core.save_command_receipt(&receipt)?;
                let result = (|| -> Result<Value, (&str, String, bool)> {
                    let (scope_id, connector_id) = connector_remove_from_payload(&command.payload)
                        .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                    let removed = core
                        .remove_connector_profile(&scope_id, &connector_id)
                        .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                    if removed {
                        core.record_projection_changed("connector-removed")
                            .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                    }
                    Ok(json!({
                        "removed": removed,
                        "alreadyAbsent": !removed,
                        "scopeId": scope_id,
                        "connectorId": connector_id,
                        "projection": core.projection_snapshot()
                            .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?,
                    }))
                })();
                match result {
                    Ok(payload) => {
                        complete_command_receipt(core, &mut receipt, payload.clone())?;
                        write_response(connection, &command.request_id, payload)?;
                    }
                    Err((code, message, retryable)) => {
                        fail_command_receipt(core, &mut receipt, code, &message, retryable)?;
                        write_error(connection, code, &message, retryable, &command.request_id)?;
                    }
                }
            }
        },
        "config.export" => {
            let project_id = parse_or_error!(required_string(&command.payload, "projectId"));
            match core.export_project_config(&project_id) {
                Ok(config) => {
                    write_response(connection, &command.request_id, json!({"config": config}))?
                }
                Err(error) => write_error(
                    connection,
                    "QUERY_REJECTED",
                    &error.to_string(),
                    false,
                    &command.request_id,
                )?,
            }
        }
        "config.import" => match begin_command_receipt(core, session, &command)? {
            ReceiptDecision::ReplayResponse(payload) => {
                write_response(connection, &command.request_id, payload)?
            }
            ReceiptDecision::ReplayError {
                code,
                message,
                retryable,
            } => write_error(connection, &code, &message, retryable, &command.request_id)?,
            ReceiptDecision::New(mut receipt) => {
                core.save_command_receipt(&receipt)?;
                let result = (|| -> Result<Value, (&str, String, bool)> {
                    let config = config_from_payload(&command.payload)
                        .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                    let result = core
                        .import_project_config(config)
                        .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                    core.record_projection_changed("config-imported")
                        .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                    Ok(json!({
                        "success": true,
                        "newProjectId": result.new_project_id,
                        "importedAgents": result.imported_agents,
                        "importedConversations": result.imported_conversations,
                        "importedWorkflows": result.imported_workflows,
                        "workspaceRebindRequired": result.workspace_rebind_required,
                        "projection": core.projection_snapshot()
                            .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?,
                    }))
                })();
                match result {
                    Ok(payload) => {
                        complete_command_receipt(core, &mut receipt, payload.clone())?;
                        write_response(connection, &command.request_id, payload)?;
                    }
                    Err((code, message, retryable)) => {
                        fail_command_receipt(core, &mut receipt, code, &message, retryable)?;
                        write_error(connection, code, &message, retryable, &command.request_id)?;
                    }
                }
            }
        },
        "message.create" => {
            let message = Message {
                id: parse_or_error!(required_string(&command.payload, "messageId")),
                conversation_id: parse_or_error!(required_string(
                    &command.payload,
                    "conversationId"
                )),
                sender_id: parse_or_error!(required_string(&command.payload, "senderId")),
                sequence: command
                    .payload
                    .get("sequence")
                    .and_then(Value::as_u64)
                    .unwrap_or(0),
                content: parse_or_error!(required_string(&command.payload, "content")),
            };
            if message.sequence == 0 {
                write_error(
                    connection,
                    "INVALID_COMMAND",
                    "message sequence must be greater than zero",
                    false,
                    &command.request_id,
                )?;
                return Ok(false);
            }
            match core.create_message(message) {
                Ok(()) => write_projection_mutation_response(
                    connection,
                    core,
                    &command.request_id,
                    "created",
                )?,
                Err(error) => write_error(
                    connection,
                    "COMMAND_REJECTED",
                    &error.to_string(),
                    false,
                    &command.request_id,
                )?,
            }
        }
        "memory.store" => match begin_command_receipt(core, session, &command)? {
            ReceiptDecision::ReplayResponse(payload) => {
                write_response(connection, &command.request_id, payload)?
            }
            ReceiptDecision::ReplayError {
                code,
                message,
                retryable,
            } => write_error(connection, &code, &message, retryable, &command.request_id)?,
            ReceiptDecision::New(mut receipt) => {
                core.save_command_receipt(&receipt)?;
                let result = (|| -> Result<Value, (&str, String, bool)> {
                    let memory = MemoryItem {
                        id: required_string(&command.payload, "memoryId")
                            .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?,
                        scope_id: required_string(&command.payload, "scopeId")
                            .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?,
                        agent_id: optional_string(&command.payload, "agentId")
                            .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?,
                        content_hash: required_string(&command.payload, "contentHash")
                            .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?,
                        confirmed: command
                            .payload
                            .get("confirmed")
                            .and_then(Value::as_bool)
                            .unwrap_or(false),
                    };
                    let outcome = core
                        .store_memory(StoreMemoryCommand { memory })
                        .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                    if outcome == MemoryWriteOutcome::Created {
                        core.record_projection_changed("memory-stored")
                            .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                    }
                    let payload = json!({
                        "created": outcome == MemoryWriteOutcome::Created,
                        "alreadyPresent": outcome == MemoryWriteOutcome::AlreadyPresent,
                        "projection": core.projection_snapshot()
                            .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?,
                    });
                    Ok(payload)
                })();
                match result {
                    Ok(payload) => {
                        complete_command_receipt(core, &mut receipt, payload.clone())?;
                        write_response(connection, &command.request_id, payload)?;
                    }
                    Err((code, message, retryable)) => {
                        fail_command_receipt(core, &mut receipt, code, &message, retryable)?;
                        write_error(connection, code, &message, retryable, &command.request_id)?;
                    }
                }
            }
        },
        "summary.store" => match begin_command_receipt(core, session, &command)? {
            ReceiptDecision::ReplayResponse(payload) => {
                write_response(connection, &command.request_id, payload)?
            }
            ReceiptDecision::ReplayError {
                code,
                message,
                retryable,
            } => write_error(connection, &code, &message, retryable, &command.request_id)?,
            ReceiptDecision::New(mut receipt) => {
                core.save_command_receipt(&receipt)?;
                let result = (|| -> Result<Value, (&str, String, bool)> {
                    let version = command
                        .payload
                        .get("version")
                        .and_then(Value::as_u64)
                        .ok_or_else(|| {
                            (
                                "INVALID_COMMAND",
                                "version must be a non-negative integer".to_owned(),
                                false,
                            )
                        })?;
                    let summary = Summary {
                        id: required_string(&command.payload, "summaryId")
                            .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?,
                        scope_id: required_string(&command.payload, "scopeId")
                            .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?,
                        version,
                        content_hash: required_string(&command.payload, "contentHash")
                            .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?,
                        artifact_id: optional_string(&command.payload, "artifactId")
                            .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?,
                    };
                    let outcome = core
                        .store_summary(StoreSummaryCommand { summary })
                        .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                    if outcome == SummaryWriteOutcome::Created {
                        core.record_projection_changed("summary-stored")
                            .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                    }
                    Ok(json!({
                        "created": outcome == SummaryWriteOutcome::Created,
                        "alreadyPresent": outcome == SummaryWriteOutcome::AlreadyPresent,
                        "projection": core.projection_snapshot()
                            .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?,
                    }))
                })();
                match result {
                    Ok(payload) => {
                        complete_command_receipt(core, &mut receipt, payload.clone())?;
                        write_response(connection, &command.request_id, payload)?;
                    }
                    Err((code, message, retryable)) => {
                        fail_command_receipt(core, &mut receipt, code, &message, retryable)?;
                        write_error(connection, code, &message, retryable, &command.request_id)?;
                    }
                }
            }
        },
        "summary.generate" => match begin_command_receipt(core, session, &command)? {
            ReceiptDecision::ReplayResponse(payload) => {
                write_response(connection, &command.request_id, payload)?
            }
            ReceiptDecision::ReplayError {
                code,
                message,
                retryable,
            } => write_error(connection, &code, &message, retryable, &command.request_id)?,
            ReceiptDecision::New(mut receipt) => {
                core.save_command_receipt(&receipt)?;
                let result = (|| -> Result<Value, (&str, String, bool)> {
                    let scope_id = required_string(&command.payload, "scopeId")
                        .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                    let outcome = core
                        .generate_summary(GenerateSummaryCommand { scope_id })
                        .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                    core.record_projection_changed("summary-generated")
                        .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                    Ok(json!({
                        "summary": {
                            "id": outcome.summary.id,
                            "scopeId": outcome.summary.scope_id,
                            "version": outcome.summary.version,
                            "contentHash": outcome.summary.content_hash,
                            "artifactId": outcome.summary.artifact_id,
                        },
                        "generator": outcome.generator,
                        "messageCount": outcome.message_count,
                        "projection": core.projection_snapshot()
                            .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?,
                    }))
                })();
                match result {
                    Ok(payload) => {
                        complete_command_receipt(core, &mut receipt, payload.clone())?;
                        write_response(connection, &command.request_id, payload)?;
                    }
                    Err((code, message, retryable)) => {
                        fail_command_receipt(core, &mut receipt, code, &message, retryable)?;
                        write_error(connection, code, &message, retryable, &command.request_id)?;
                    }
                }
            }
        },
        "artifact.store" => match begin_command_receipt(core, session, &command)? {
            ReceiptDecision::ReplayResponse(payload) => {
                write_response(connection, &command.request_id, payload)?
            }
            ReceiptDecision::ReplayError {
                code,
                message,
                retryable,
            } => write_error(connection, &code, &message, retryable, &command.request_id)?,
            ReceiptDecision::New(mut receipt) => {
                core.save_command_receipt(&receipt)?;
                let result = (|| -> Result<Value, (&str, String, bool)> {
                    let size = command
                        .payload
                        .get("size")
                        .and_then(Value::as_u64)
                        .ok_or_else(|| {
                            (
                                "INVALID_COMMAND",
                                "size must be a non-negative integer".to_owned(),
                                false,
                            )
                        })?;
                    let artifact = Artifact {
                        id: required_string(&command.payload, "artifactId")
                            .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?,
                        sha256: required_string(&command.payload, "sha256")
                            .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?,
                        size,
                        mime: required_string(&command.payload, "mime")
                            .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?,
                        relative_path: optional_string(&command.payload, "relativePath")
                            .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?,
                    };
                    let body = command
                        .payload
                        .get("bodyBase64")
                        .map(|value| {
                            let encoded = value.as_str().ok_or_else(|| {
                                (
                                    "INVALID_COMMAND",
                                    "bodyBase64 must be a string".to_owned(),
                                    false,
                                )
                            })?;
                            let maximum_encoded = ARTIFACT_BODY_MAX_BYTES
                                .div_ceil(3)
                                .saturating_mul(4)
                                .saturating_add(4)
                                as usize;
                            if encoded.len() > maximum_encoded {
                                return Err((
                                    "INVALID_COMMAND",
                                    "bodyBase64 exceeds the configured size limit".to_owned(),
                                    false,
                                ));
                            }
                            STANDARD.decode(encoded).map_err(|_| {
                                (
                                    "INVALID_COMMAND",
                                    "bodyBase64 is not valid base64".to_owned(),
                                    false,
                                )
                            })
                        })
                        .transpose()?;
                    let outcome = core
                        .store_artifact(StoreArtifactCommand { artifact })
                        .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                    let body_stored = match body {
                        Some(body) => core
                            .store_artifact_body(StoreArtifactBodyCommand {
                                artifact_id: required_string(&command.payload, "artifactId")
                                    .map_err(|error| {
                                        ("INVALID_COMMAND", error.to_string(), false)
                                    })?,
                                body,
                            })
                            .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?,
                        None => false,
                    };
                    if outcome == ArtifactWriteOutcome::Created {
                        core.record_projection_changed("artifact-stored")
                            .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                    }
                    Ok(json!({
                        "created": outcome == ArtifactWriteOutcome::Created,
                        "alreadyPresent": outcome == ArtifactWriteOutcome::AlreadyPresent,
                        "bodyStored": body_stored,
                        "projection": core.projection_snapshot()
                            .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?,
                    }))
                })();
                match result {
                    Ok(payload) => {
                        complete_command_receipt(core, &mut receipt, payload.clone())?;
                        write_response(connection, &command.request_id, payload)?;
                    }
                    Err((code, message, retryable)) => {
                        fail_command_receipt(core, &mut receipt, code, &message, retryable)?;
                        write_error(connection, code, &message, retryable, &command.request_id)?;
                    }
                }
            }
        },
        "attachment.import_file" => match begin_command_receipt(core, session, &command)? {
            ReceiptDecision::ReplayResponse(payload) => {
                write_response(connection, &command.request_id, payload)?
            }
            ReceiptDecision::ReplayError {
                code,
                message,
                retryable,
            } => write_error(connection, &code, &message, retryable, &command.request_id)?,
            ReceiptDecision::New(mut receipt) => {
                core.save_command_receipt(&receipt)?;
                let result = (|| -> Result<Value, (&str, String, bool)> {
                    let ordinal = command
                        .payload
                        .get("ordinal")
                        .and_then(Value::as_u64)
                        .ok_or_else(|| {
                            (
                                "INVALID_COMMAND",
                                "ordinal must be a non-negative integer".to_owned(),
                                false,
                            )
                        })?;
                    let outcome = core
                        .import_attachment_file(ImportAttachmentFileCommand {
                            attachment_id: required_string(&command.payload, "attachmentId")
                                .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?,
                            artifact_id: required_string(&command.payload, "artifactId")
                                .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?,
                            message_id: required_string(&command.payload, "messageId")
                                .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?,
                            source_path: required_string(&command.payload, "sourcePath")
                                .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?
                                .into(),
                            mime: required_string(&command.payload, "mime")
                                .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?,
                            ordinal,
                        })
                        .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                    if outcome.body_stored
                        || outcome.artifact_outcome == ArtifactWriteOutcome::Created
                        || outcome.attachment_outcome == AttachmentWriteOutcome::Created
                    {
                        core.record_projection_changed("attachment-file-imported")
                            .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                    }
                    Ok(json!({
                        "created": outcome.attachment_outcome == AttachmentWriteOutcome::Created,
                        "alreadyPresent": outcome.attachment_outcome == AttachmentWriteOutcome::AlreadyPresent,
                        "artifactCreated": outcome.artifact_outcome == ArtifactWriteOutcome::Created,
                        "artifactAlreadyPresent": outcome.artifact_outcome == ArtifactWriteOutcome::AlreadyPresent,
                        "bodyStored": outcome.body_stored,
                        "artifact": {
                            "id": outcome.artifact.id,
                            "sha256": outcome.artifact.sha256,
                            "size": outcome.artifact.size,
                            "mime": outcome.artifact.mime,
                            "relativePath": outcome.artifact.relative_path,
                        },
                        "attachment": {
                            "attachmentId": outcome.attachment.id,
                            "artifactId": outcome.attachment.artifact_id,
                            "messageId": outcome.attachment.message_id,
                            "fileName": outcome.attachment.file_name,
                            "sha256": outcome.attachment.sha256,
                            "size": outcome.attachment.size,
                            "ordinal": ordinal,
                        },
                        "projection": core.projection_snapshot()
                            .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?,
                    }))
                })();
                match result {
                    Ok(payload) => {
                        complete_command_receipt(core, &mut receipt, payload.clone())?;
                        write_response(connection, &command.request_id, payload)?;
                    }
                    Err((code, message, retryable)) => {
                        fail_command_receipt(core, &mut receipt, code, &message, retryable)?;
                        write_error(connection, code, &message, retryable, &command.request_id)?;
                    }
                }
            }
        },
        "attachment.store" => match begin_command_receipt(core, session, &command)? {
            ReceiptDecision::ReplayResponse(payload) => {
                write_response(connection, &command.request_id, payload)?
            }
            ReceiptDecision::ReplayError {
                code,
                message,
                retryable,
            } => write_error(connection, &code, &message, retryable, &command.request_id)?,
            ReceiptDecision::New(mut receipt) => {
                core.save_command_receipt(&receipt)?;
                let result = (|| -> Result<Value, (&str, String, bool)> {
                    let ordinal = command
                        .payload
                        .get("ordinal")
                        .and_then(Value::as_u64)
                        .ok_or_else(|| {
                            (
                                "INVALID_COMMAND",
                                "ordinal must be a non-negative integer".to_owned(),
                                false,
                            )
                        })?;
                    let size = command
                        .payload
                        .get("size")
                        .and_then(Value::as_u64)
                        .ok_or_else(|| {
                            (
                                "INVALID_COMMAND",
                                "size must be a non-negative integer".to_owned(),
                                false,
                            )
                        })?;
                    let attachment = Attachment {
                        id: required_string(&command.payload, "attachmentId")
                            .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?,
                        message_id: required_string(&command.payload, "messageId")
                            .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?,
                        artifact_id: required_string(&command.payload, "artifactId")
                            .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?,
                        file_name: required_string(&command.payload, "fileName")
                            .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?,
                        sha256: required_string(&command.payload, "sha256")
                            .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?,
                        size,
                    };
                    let outcome = core
                        .store_attachment(StoreAttachmentCommand {
                            attachment,
                            ordinal,
                        })
                        .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                    if outcome == AttachmentWriteOutcome::Created {
                        core.record_projection_changed("attachment-stored")
                            .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                    }
                    Ok(json!({
                        "created": outcome == AttachmentWriteOutcome::Created,
                        "alreadyPresent": outcome == AttachmentWriteOutcome::AlreadyPresent,
                        "projection": core.projection_snapshot()
                            .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?,
                    }))
                })();
                match result {
                    Ok(payload) => {
                        complete_command_receipt(core, &mut receipt, payload.clone())?;
                        write_response(connection, &command.request_id, payload)?;
                    }
                    Err((code, message, retryable)) => {
                        fail_command_receipt(core, &mut receipt, code, &message, retryable)?;
                        write_error(connection, code, &message, retryable, &command.request_id)?;
                    }
                }
            }
        },
        "retrieval.store" => match begin_command_receipt(core, session, &command)? {
            ReceiptDecision::ReplayResponse(payload) => {
                write_response(connection, &command.request_id, payload)?
            }
            ReceiptDecision::ReplayError {
                code,
                message,
                retryable,
            } => write_error(connection, &code, &message, retryable, &command.request_id)?,
            ReceiptDecision::New(mut receipt) => {
                core.save_command_receipt(&receipt)?;
                let result = (|| -> Result<Value, (&str, String, bool)> {
                    let token_count = command
                        .payload
                        .get("tokenCount")
                        .and_then(Value::as_u64)
                        .ok_or_else(|| {
                            (
                                "INVALID_COMMAND",
                                "tokenCount must be a non-negative integer".to_owned(),
                                false,
                            )
                        })?;
                    let source = RetrievalSource {
                        id: required_string(&command.payload, "retrievalSourceId")
                            .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?,
                        scope_id: required_string(&command.payload, "scopeId")
                            .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?,
                        citation: required_string(&command.payload, "citation")
                            .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?,
                        sha256: required_string(&command.payload, "sha256")
                            .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?,
                        token_count,
                    };
                    let outcome = core
                        .store_retrieval_source(StoreRetrievalSourceCommand { source })
                        .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                    if outcome == RetrievalWriteOutcome::Created {
                        core.record_projection_changed("retrieval-stored")
                            .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                    }
                    let payload = json!({
                        "created": outcome == RetrievalWriteOutcome::Created,
                        "alreadyPresent": outcome == RetrievalWriteOutcome::AlreadyPresent,
                        "projection": core.projection_snapshot()
                            .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?,
                    });
                    Ok(payload)
                })();
                match result {
                    Ok(payload) => {
                        complete_command_receipt(core, &mut receipt, payload.clone())?;
                        write_response(connection, &command.request_id, payload)?;
                    }
                    Err((code, message, retryable)) => {
                        fail_command_receipt(core, &mut receipt, code, &message, retryable)?;
                        write_error(connection, code, &message, retryable, &command.request_id)?;
                    }
                }
            }
        },
        "retrieval.select" => match begin_command_receipt(core, session, &command)? {
            ReceiptDecision::ReplayResponse(payload) => {
                write_response(connection, &command.request_id, payload)?
            }
            ReceiptDecision::ReplayError {
                code,
                message,
                retryable,
            } => write_error(connection, &code, &message, retryable, &command.request_id)?,
            ReceiptDecision::New(mut receipt) => {
                core.save_command_receipt(&receipt)?;
                let result = (|| -> Result<Value, (&str, String, bool)> {
                    let selection = retrieval_selection_from_payload(&command.payload)
                        .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                    let outcome = core
                        .store_retrieval_selection(StoreRetrievalSelectionCommand { selection })
                        .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                    if outcome == RetrievalSelectionWriteOutcome::Created {
                        core.record_projection_changed("retrieval-selection-stored")
                            .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                    }
                    Ok(json!({
                        "created": outcome == RetrievalSelectionWriteOutcome::Created,
                        "alreadyPresent": outcome == RetrievalSelectionWriteOutcome::AlreadyPresent,
                        "projection": core.projection_snapshot()
                            .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?,
                    }))
                })();
                match result {
                    Ok(payload) => {
                        complete_command_receipt(core, &mut receipt, payload.clone())?;
                        write_response(connection, &command.request_id, payload)?;
                    }
                    Err((code, message, retryable)) => {
                        fail_command_receipt(core, &mut receipt, code, &message, retryable)?;
                        write_error(connection, code, &message, retryable, &command.request_id)?;
                    }
                }
            }
        },
        "retrieval.feedback" => match begin_command_receipt(core, session, &command)? {
            ReceiptDecision::ReplayResponse(payload) => {
                write_response(connection, &command.request_id, payload)?
            }
            ReceiptDecision::ReplayError {
                code,
                message,
                retryable,
            } => write_error(connection, &code, &message, retryable, &command.request_id)?,
            ReceiptDecision::New(mut receipt) => {
                core.save_command_receipt(&receipt)?;
                let result = (|| -> Result<Value, (&str, String, bool)> {
                    let feedback = retrieval_feedback_from_payload(&command.payload)
                        .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                    let outcome = core
                        .store_retrieval_feedback(StoreRetrievalFeedbackCommand { feedback })
                        .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                    if outcome == RetrievalFeedbackWriteOutcome::Created {
                        core.record_projection_changed("retrieval-feedback-stored")
                            .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                    }
                    Ok(json!({
                        "created": outcome == RetrievalFeedbackWriteOutcome::Created,
                        "alreadyPresent": outcome == RetrievalFeedbackWriteOutcome::AlreadyPresent,
                        "projection": core.projection_snapshot()
                            .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?,
                    }))
                })();
                match result {
                    Ok(payload) => {
                        complete_command_receipt(core, &mut receipt, payload.clone())?;
                        write_response(connection, &command.request_id, payload)?;
                    }
                    Err((code, message, retryable)) => {
                        fail_command_receipt(core, &mut receipt, code, &message, retryable)?;
                        write_error(connection, code, &message, retryable, &command.request_id)?;
                    }
                }
            }
        },
        "workflow.create" => match begin_command_receipt(core, session, &command)? {
            ReceiptDecision::ReplayResponse(payload) => {
                write_response(connection, &command.request_id, payload)?
            }
            ReceiptDecision::ReplayError {
                code,
                message,
                retryable,
            } => write_error(connection, &code, &message, retryable, &command.request_id)?,
            ReceiptDecision::New(mut receipt) => {
                core.save_command_receipt(&receipt)?;
                let result = (|| -> Result<Value, (&str, String, bool)> {
                    let (project_id, workflow) = workflow_from_payload(&command.payload)
                        .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                    let outcome = core
                        .create_workflow(CreateWorkflowCommand {
                            project_id,
                            workflow,
                        })
                        .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                    if outcome == WorkflowWriteOutcome::Created {
                        core.record_projection_changed("workflow-created")
                            .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                    }
                    let payload = json!({
                        "created": outcome == WorkflowWriteOutcome::Created,
                        "alreadyPresent": outcome == WorkflowWriteOutcome::AlreadyPresent,
                        "projection": core.projection_snapshot()
                            .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?,
                    });
                    Ok(payload)
                })();
                match result {
                    Ok(payload) => {
                        complete_command_receipt(core, &mut receipt, payload.clone())?;
                        write_response(connection, &command.request_id, payload)?;
                    }
                    Err((code, message, retryable)) => {
                        fail_command_receipt(core, &mut receipt, code, &message, retryable)?;
                        write_error(connection, code, &message, retryable, &command.request_id)?;
                    }
                }
            }
        },
        "workflow.dispatch" => match begin_command_receipt(core, session, &command)? {
            ReceiptDecision::ReplayResponse(payload) => {
                write_response(connection, &command.request_id, payload)?
            }
            ReceiptDecision::ReplayError {
                code,
                message,
                retryable,
            } => write_error(connection, &code, &message, retryable, &command.request_id)?,
            ReceiptDecision::New(mut receipt) => {
                core.save_command_receipt(&receipt)?;
                let result = (|| -> Result<Value, (&str, String, bool)> {
                    let dispatch = workflow_dispatch_from_payload(&command.payload)
                        .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                    let outcome = core
                        .dispatch_workflow(dispatch)
                        .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                    core.record_projection_changed("workflow-dispatched")
                        .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                    Ok(json!({
                        "workflowId": outcome.workflow_id,
                        "collaborationRunId": outcome.collaboration_run_id,
                        "mode": outcome.mode,
                        "completed": outcome.completed,
                        "failed": outcome.failed,
                        "steps": outcome.steps.iter().map(|step| json!({
                            "stepId": step.step_id,
                            "order": step.order,
                            "agentId": step.agent_id,
                            "handoffId": step.handoff_id,
                            "childExecutionRunId": step.child_execution_run_id,
                            "handoffStatus": step.handoff_status,
                            "childStatus": step.child_status,
                            "runtimeStarted": step.runtime_started,
                            "runtimeDispatch": step.runtime_dispatch,
                        })).collect::<Vec<_>>(),
                        "projection": core.projection_snapshot()
                            .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?,
                    }))
                })();
                match result {
                    Ok(payload) => {
                        complete_command_receipt(core, &mut receipt, payload.clone())?;
                        write_response(connection, &command.request_id, payload)?;
                    }
                    Err((code, message, retryable)) => {
                        fail_command_receipt(core, &mut receipt, code, &message, retryable)?;
                        write_error(connection, code, &message, retryable, &command.request_id)?;
                    }
                }
            }
        },
        "collaboration.create" => match begin_command_receipt(core, session, &command)? {
            ReceiptDecision::ReplayResponse(payload) => {
                write_response(connection, &command.request_id, payload)?
            }
            ReceiptDecision::ReplayError {
                code,
                message,
                retryable,
            } => write_error(connection, &code, &message, retryable, &command.request_id)?,
            ReceiptDecision::New(mut receipt) => {
                core.save_command_receipt(&receipt)?;
                let result = (|| -> Result<Value, (&str, String, bool)> {
                    let (project_id, collaboration) = collaboration_from_payload(&command.payload)
                        .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                    let outcome = core
                        .create_collaboration(CreateCollaborationCommand {
                            project_id,
                            collaboration,
                        })
                        .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                    if outcome == CollaborationWriteOutcome::Created {
                        core.record_projection_changed("collaboration-created")
                            .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                    }
                    let payload = json!({
                        "created": outcome == CollaborationWriteOutcome::Created,
                        "alreadyPresent": outcome == CollaborationWriteOutcome::AlreadyPresent,
                        "projection": core.projection_snapshot()
                            .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?,
                    });
                    Ok(payload)
                })();
                match result {
                    Ok(payload) => {
                        complete_command_receipt(core, &mut receipt, payload.clone())?;
                        write_response(connection, &command.request_id, payload)?;
                    }
                    Err((code, message, retryable)) => {
                        fail_command_receipt(core, &mut receipt, code, &message, retryable)?;
                        write_error(connection, code, &message, retryable, &command.request_id)?;
                    }
                }
            }
        },
        "handoff.create" => match begin_command_receipt(core, session, &command)? {
            ReceiptDecision::ReplayResponse(payload) => {
                write_response(connection, &command.request_id, payload)?
            }
            ReceiptDecision::ReplayError {
                code,
                message,
                retryable,
            } => write_error(connection, &code, &message, retryable, &command.request_id)?,
            ReceiptDecision::New(mut receipt) => {
                core.save_command_receipt(&receipt)?;
                let result = (|| -> Result<Value, (&str, String, bool)> {
                    let handoff = handoff_from_payload(&command.payload)
                        .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                    let outcome = core
                        .create_handoff(CreateHandoffCommand { handoff })
                        .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                    if outcome == HandoffWriteOutcome::Created {
                        core.record_projection_changed("handoff-created")
                            .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                    }
                    let payload = json!({
                        "created": outcome == HandoffWriteOutcome::Created,
                        "alreadyPresent": outcome == HandoffWriteOutcome::AlreadyPresent,
                        "projection": core.projection_snapshot()
                            .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?,
                    });
                    Ok(payload)
                })();
                match result {
                    Ok(payload) => {
                        complete_command_receipt(core, &mut receipt, payload.clone())?;
                        write_response(connection, &command.request_id, payload)?;
                    }
                    Err((code, message, retryable)) => {
                        fail_command_receipt(core, &mut receipt, code, &message, retryable)?;
                        write_error(connection, code, &message, retryable, &command.request_id)?;
                    }
                }
            }
        },
        "handoff.approve" => {
            handle_handoff_transition(connection, core, session, &command, "approved")?;
        }
        "handoff.reject" => {
            handle_handoff_transition(connection, core, session, &command, "rejected")?;
        }
        "handoff.dispatch" => {
            handle_handoff_dispatch(connection, core, session, &command)?;
        }
        "handoff.cancel" => {
            handle_handoff_transition(connection, core, session, &command, "cancelled")?;
        }
        "workspace.authorize" => {
            let project_id = parse_or_error!(required_string(&command.payload, "projectId"));
            let root_path = parse_or_error!(required_string(&command.payload, "rootPath"));
            match core.authorize_workspace(&project_id, &root_path) {
                Ok(_) => write_projection_mutation_response(
                    connection,
                    core,
                    &command.request_id,
                    "authorized",
                )?,
                Err(error) => write_error(
                    connection,
                    "COMMAND_REJECTED",
                    &error.to_string(),
                    false,
                    &command.request_id,
                )?,
            }
        }
        "agent.model_binding.set" => {
            let (agent_id, connector_id, model_id, revision) =
                parse_or_error!(agent_model_binding_from_payload(&command.payload));
            match core.set_agent_model_binding(&agent_id, connector_id, model_id, revision) {
                Ok(()) => write_projection_mutation_response(
                    connection,
                    core,
                    &command.request_id,
                    "updated",
                )?,
                Err(error) => write_error(
                    connection,
                    "COMMAND_REJECTED",
                    &error.to_string(),
                    false,
                    &command.request_id,
                )?,
            }
        }
        "agent.model_binding.patch" => {
            let (agent_id, patch) =
                parse_or_error!(agent_model_binding_patch_from_payload(&command.payload));
            match core.patch_agent_model_binding(&agent_id, &patch) {
                Ok(_) => write_projection_mutation_response(
                    connection,
                    core,
                    &command.request_id,
                    "updated",
                )?,
                Err(error) => write_error(
                    connection,
                    "COMMAND_REJECTED",
                    &error.to_string(),
                    false,
                    &command.request_id,
                )?,
            }
        }
        "project_agent.set" => {
            let project_id = parse_or_error!(required_string(&command.payload, "projectId"));
            let agent_id = parse_or_error!(required_string(&command.payload, "agentId"));
            let enabled = command
                .payload
                .get("enabled")
                .and_then(Value::as_bool)
                .unwrap_or(true);
            let access = parse_or_error!(parse_access(
                command
                    .payload
                    .get("workspaceAccess")
                    .and_then(Value::as_str)
                    .unwrap_or("none"),
            ));
            let result = if has_model_selection_fields(&command.payload) {
                let (selection, list_mode, list_revision) =
                    parse_or_error!(model_selection_from_payload(&command.payload));
                core.set_project_agent_assignment_with_model_selection(
                    &project_id,
                    &agent_id,
                    enabled,
                    access,
                    selection,
                    list_mode,
                    list_revision,
                )
            } else {
                core.set_project_agent_assignment(&project_id, &agent_id, enabled, access)
            };
            match result {
                Ok(()) => write_projection_mutation_response(
                    connection,
                    core,
                    &command.request_id,
                    "updated",
                )?,
                Err(error) => write_error(
                    connection,
                    "COMMAND_REJECTED",
                    &error.to_string(),
                    false,
                    &command.request_id,
                )?,
            }
        }
        "project_agent.remove" => {
            let project_id = parse_or_error!(required_string(&command.payload, "projectId"));
            let agent_id = parse_or_error!(required_string(&command.payload, "agentId"));
            match core.remove_project_agent_assignment(&project_id, &agent_id) {
                Ok(()) => write_projection_mutation_response(
                    connection,
                    core,
                    &command.request_id,
                    "removed",
                )?,
                Err(error) => write_error(
                    connection,
                    "COMMAND_REJECTED",
                    &error.to_string(),
                    false,
                    &command.request_id,
                )?,
            }
        }
        "conversation_agent.set" => {
            let conversation_id =
                parse_or_error!(required_string(&command.payload, "conversationId"));
            let agent_id = parse_or_error!(required_string(&command.payload, "agentId"));
            let enabled = command
                .payload
                .get("enabled")
                .and_then(Value::as_bool)
                .unwrap_or(true);
            let result = if has_model_selection_fields(&command.payload) {
                let (selection, list_mode, list_revision) =
                    parse_or_error!(model_selection_from_payload(&command.payload));
                core.set_conversation_agent_assignment_with_model_selection(
                    &conversation_id,
                    &agent_id,
                    enabled,
                    selection,
                    list_mode,
                    list_revision,
                )
            } else {
                core.set_conversation_agent_assignment(&conversation_id, &agent_id, enabled)
            };
            match result {
                Ok(()) => write_projection_mutation_response(
                    connection,
                    core,
                    &command.request_id,
                    "updated",
                )?,
                Err(error) => write_error(
                    connection,
                    "COMMAND_REJECTED",
                    &error.to_string(),
                    false,
                    &command.request_id,
                )?,
            }
        }
        "conversation_agent.remove" => {
            let conversation_id =
                parse_or_error!(required_string(&command.payload, "conversationId"));
            let agent_id = parse_or_error!(required_string(&command.payload, "agentId"));
            match core.remove_conversation_agent_assignment(&conversation_id, &agent_id) {
                Ok(()) => write_projection_mutation_response(
                    connection,
                    core,
                    &command.request_id,
                    "removed",
                )?,
                Err(error) => write_error(
                    connection,
                    "COMMAND_REJECTED",
                    &error.to_string(),
                    false,
                    &command.request_id,
                )?,
            }
        }
        "identity_model_option.upsert" => {
            let option = parse_or_error!(identity_model_option_from_payload(&command.payload));
            match core.upsert_identity_model_option(&option) {
                Ok(()) => write_projection_mutation_response(
                    connection,
                    core,
                    &command.request_id,
                    "updated",
                )?,
                Err(error) => write_error(
                    connection,
                    "COMMAND_REJECTED",
                    &error.to_string(),
                    false,
                    &command.request_id,
                )?,
            }
        }
        "identity_model_option.default" => {
            let (target, connector_id, model_id) =
                parse_or_error!(identity_model_default_from_payload(&command.payload));
            match core.set_identity_model_option_default(&target, &connector_id, &model_id) {
                Ok(()) => write_projection_mutation_response(
                    connection,
                    core,
                    &command.request_id,
                    "updated",
                )?,
                Err(error) => write_error(
                    connection,
                    "COMMAND_REJECTED",
                    &error.to_string(),
                    false,
                    &command.request_id,
                )?,
            }
        }
        "execution.start" => match begin_command_receipt(core, session, &command)? {
            ReceiptDecision::ReplayResponse(payload) => {
                write_response(connection, &command.request_id, payload)?
            }
            ReceiptDecision::ReplayError {
                code,
                message,
                retryable,
            } => write_error(connection, &code, &message, retryable, &command.request_id)?,
            ReceiptDecision::New(mut receipt) => {
                match execution_start_from_payload(&command.payload) {
                    Ok((input, current_task, connector_id, model_id)) => {
                        let deferred = core.uses_deferred_runtime_dispatch();
                        let started = if deferred {
                            core.start_execution_with_connector_and_receipt_deferred(
                                input,
                                current_task,
                                &receipt,
                                connector_id,
                                model_id,
                                command.deadline_ms,
                            )
                        } else {
                            core.start_execution_with_connector_and_receipt(
                                input,
                                current_task,
                                &receipt,
                                connector_id,
                                model_id,
                                command.deadline_ms,
                            )
                        };
                        match started {
                            Ok(run) => {
                                if deferred {
                                    deferred_dispatches.push(run.id.clone());
                                }
                                let payload = json!({"run": run});
                                complete_command_receipt(core, &mut receipt, payload.clone())?;
                                write_response(connection, &command.request_id, payload)?;
                            }
                            Err(error) => fail_command_with_core_error(
                                core,
                                &mut receipt,
                                connection,
                                "COMMAND_REJECTED",
                                &error,
                                &command.request_id,
                            )?,
                        }
                    }
                    Err(error) => {
                        let message = error.to_string();
                        fail_command_receipt(
                            core,
                            &mut receipt,
                            "INVALID_COMMAND",
                            &message,
                            false,
                        )?;
                        write_error(
                            connection,
                            "INVALID_COMMAND",
                            &message,
                            false,
                            &command.request_id,
                        )?;
                    }
                }
            }
        },
        "execution.retry" | "execution.rerun_current" => {
            let rerun_current = command.command == "execution.rerun_current";
            match begin_command_receipt(core, session, &command)? {
                ReceiptDecision::ReplayResponse(payload) => {
                    write_response(connection, &command.request_id, payload)?
                }
                ReceiptDecision::ReplayError {
                    code,
                    message,
                    retryable,
                } => write_error(connection, &code, &message, retryable, &command.request_id)?,
                ReceiptDecision::New(mut receipt) => {
                    match execution_retry_from_payload(&command.payload) {
                        Ok((new_run_id, source_run_id, current_task, connector_id, model_id)) => {
                            let options = ExecutionRuntimeOptions {
                                connector_id,
                                model_id,
                                timeout_ms: command.deadline_ms,
                            };
                            let deferred = core.uses_deferred_runtime_dispatch();
                            let started = match (rerun_current, deferred) {
                                (true, true) => core.rerun_current_execution_with_receipt_deferred(
                                    new_run_id,
                                    &source_run_id,
                                    current_task,
                                    &receipt,
                                    options,
                                ),
                                (true, false) => core.rerun_current_execution_with_receipt(
                                    new_run_id,
                                    &source_run_id,
                                    current_task,
                                    &receipt,
                                    options,
                                ),
                                (false, true) => core.retry_execution_with_receipt_deferred(
                                    new_run_id,
                                    &source_run_id,
                                    current_task,
                                    &receipt,
                                    options,
                                ),
                                (false, false) => core.retry_execution_with_receipt(
                                    new_run_id,
                                    &source_run_id,
                                    current_task,
                                    &receipt,
                                    options,
                                ),
                            };
                            match started {
                                Ok(run) => {
                                    if deferred {
                                        deferred_dispatches.push(run.id.clone());
                                    }
                                    let payload = json!({
                                        "run": run,
                                        "sourceExecutionRunId": source_run_id,
                                    });
                                    complete_command_receipt(core, &mut receipt, payload.clone())?;
                                    write_response(connection, &command.request_id, payload)?;
                                }
                                Err(error) => fail_command_with_core_error(
                                    core,
                                    &mut receipt,
                                    connection,
                                    "COMMAND_REJECTED",
                                    &error,
                                    &command.request_id,
                                )?,
                            }
                        }
                        Err(error) => {
                            let message = error.to_string();
                            fail_command_receipt(
                                core,
                                &mut receipt,
                                "INVALID_COMMAND",
                                &message,
                                false,
                            )?;
                            write_error(
                                connection,
                                "INVALID_COMMAND",
                                &message,
                                false,
                                &command.request_id,
                            )?;
                        }
                    }
                }
            }
        }
        "execution.cancel" => match begin_command_receipt(core, session, &command)? {
            ReceiptDecision::ReplayResponse(payload) => {
                write_response(connection, &command.request_id, payload)?
            }
            ReceiptDecision::ReplayError {
                code,
                message,
                retryable,
            } => write_error(connection, &code, &message, retryable, &command.request_id)?,
            ReceiptDecision::New(mut receipt) => {
                core.save_command_receipt(&receipt)?;
                let result = match required_string(&command.payload, "executionRunId") {
                    Ok(run_id) => match core.cancel_execution(&run_id) {
                        Ok(_) => {
                            let payload = json!({"cancelled": true});
                            complete_command_receipt(core, &mut receipt, payload.clone())?;
                            write_response(connection, &command.request_id, payload)?;
                            Ok(())
                        }
                        Err(agenttalk_core::CoreError::RunNotFound) => {
                            Err(("RUN_NOT_FOUND", "execution run not found".into(), false))
                        }
                        Err(error) => {
                            let (code, message, category) =
                                core_error_details("COMMAND_REJECTED", &error);
                            fail_command_receipt(core, &mut receipt, code, message, false)?;
                            write_error_with_details(
                                connection,
                                code,
                                message,
                                false,
                                &command.request_id,
                                (code != "COMMAND_REJECTED").then(|| json!({"category": category})),
                            )?;
                            Ok(())
                        }
                    },
                    Err(error) => Err(("INVALID_COMMAND", error.to_string(), false)),
                };
                if let Err((code, message, retryable)) = result {
                    fail_command_receipt(core, &mut receipt, code, &message, retryable)?;
                    write_error(connection, code, &message, retryable, &command.request_id)?;
                }
            }
        },
        "shutdown_owned" => {
            if let Err(error) = core.shutdown_owned_runtimes() {
                write_core_error(connection, "COMMAND_REJECTED", &error, &command.request_id)?;
                return Ok(false);
            }
            write_response(
                connection,
                &command.request_id,
                json!({"shutdownAccepted": true}),
            )?;
            return Ok(true);
        }
        _ => write_error(
            connection,
            "UNSUPPORTED_COMMAND",
            "command is not implemented by this Core build",
            false,
            &command.request_id,
        )?,
    }
    Ok(false)
}

#[cfg(windows)]
enum ReceiptDecision {
    New(CommandReceipt),
    ReplayResponse(Value),
    ReplayError {
        code: String,
        message: String,
        retryable: bool,
    },
}

#[cfg(windows)]
fn handle_handoff_transition(
    connection: &WriterQueue,
    core: &mut PersistentCore,
    session: &SessionBinding,
    command: &CommandEnvelope,
    target_status: &str,
) -> Result<(), Box<dyn Error>> {
    match begin_command_receipt(core, session, command)? {
        ReceiptDecision::ReplayResponse(payload) => {
            write_response(connection, &command.request_id, payload)?;
        }
        ReceiptDecision::ReplayError {
            code,
            message,
            retryable,
        } => write_error(connection, &code, &message, retryable, &command.request_id)?,
        ReceiptDecision::New(mut receipt) => {
            core.save_command_receipt(&receipt)?;
            let result = (|| -> Result<Value, (&str, String, bool)> {
                let handoff_id = required_string(&command.payload, "handoffId")
                    .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                let outcome = core
                    .transition_handoff(&handoff_id, target_status)
                    .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                if outcome == HandoffTransitionOutcome::Changed {
                    core.record_projection_changed(&format!("handoff-{target_status}"))
                        .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                }
                Ok(json!({
                    "handoffId": handoff_id,
                    "status": target_status,
                    "changed": outcome == HandoffTransitionOutcome::Changed,
                    "alreadyAtTarget": outcome == HandoffTransitionOutcome::AlreadyAtTarget,
                    "projection": core.projection_snapshot()
                        .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?,
                }))
            })();
            match result {
                Ok(payload) => {
                    complete_command_receipt(core, &mut receipt, payload.clone())?;
                    write_response(connection, &command.request_id, payload)?;
                }
                Err((code, message, retryable)) => {
                    fail_command_receipt(core, &mut receipt, code, &message, retryable)?;
                    write_error(connection, code, &message, retryable, &command.request_id)?;
                }
            }
        }
    }
    Ok(())
}

#[cfg(windows)]
fn handle_handoff_dispatch(
    connection: &WriterQueue,
    core: &mut PersistentCore,
    session: &SessionBinding,
    command: &CommandEnvelope,
) -> Result<(), Box<dyn Error>> {
    match begin_command_receipt(core, session, command)? {
        ReceiptDecision::ReplayResponse(payload) => {
            write_response(connection, &command.request_id, payload)?;
        }
        ReceiptDecision::ReplayError {
            code,
            message,
            retryable,
        } => write_error(connection, &code, &message, retryable, &command.request_id)?,
        ReceiptDecision::New(mut receipt) => {
            core.save_command_receipt(&receipt)?;
            let result = (|| -> Result<Value, (&str, String, bool)> {
                let handoff_id = required_string(&command.payload, "handoffId")
                    .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                let start_runtime = optional_bool(&command.payload, "startRuntime")
                    .map_err(|error| ("INVALID_COMMAND", error.to_string(), false))?;
                let outcome = core
                    .dispatch_handoff_with_runtime(&handoff_id, start_runtime)
                    .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                if outcome.created || outcome.runtime_started {
                    core.record_projection_changed("handoff-dispatched")
                        .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?;
                }
                Ok(json!({
                    "handoffId": handoff_id,
                    "status": outcome.handoff_status,
                    "changed": outcome.created || outcome.runtime_started,
                    "alreadyAtTarget": !outcome.created && !outcome.runtime_started,
                    "childExecutionRunId": outcome.child_run.id,
                    "childRun": outcome.child_run,
                    "runtimeStarted": outcome.runtime_started,
                    "runtimeDispatch": outcome.runtime_dispatch,
                    "eventSequence": outcome.event_sequence,
                    "projection": core.projection_snapshot()
                        .map_err(|error| ("COMMAND_REJECTED", error.to_string(), false))?,
                }))
            })();
            match result {
                Ok(payload) => {
                    complete_command_receipt(core, &mut receipt, payload.clone())?;
                    write_response(connection, &command.request_id, payload)?;
                }
                Err((code, message, retryable)) => {
                    fail_command_receipt(core, &mut receipt, code, &message, retryable)?;
                    write_error(connection, code, &message, retryable, &command.request_id)?;
                }
            }
        }
    }
    Ok(())
}

#[cfg(windows)]
fn begin_command_receipt(
    core: &mut PersistentCore,
    session: &SessionBinding,
    command: &CommandEnvelope,
) -> Result<ReceiptDecision, Box<dyn Error>> {
    let payload_hash = command_payload_hash(command)?;
    let key = CommandReceiptKey {
        scope_id: "desktop-command-v1".into(),
        client_id: session.client_id.clone(),
        request_id: command.request_id.clone(),
    };
    if let Some(existing) = core.load_command_receipt(&key)? {
        if existing.command != command.command || existing.payload_hash != payload_hash {
            return Ok(ReceiptDecision::ReplayError {
                code: "REQUEST_ID_REUSE".into(),
                message: "requestId is already bound to a different command payload".into(),
                retryable: false,
            });
        }
        return Ok(match existing.state {
            CommandReceiptState::Completed => existing
                .result_json
                .map(ReceiptDecision::ReplayResponse)
                .unwrap_or_else(|| ReceiptDecision::ReplayError {
                    code: "COMMAND_RECEIPT_CORRUPT".into(),
                    message: "completed command receipt has no result".into(),
                    retryable: false,
                }),
            CommandReceiptState::Failed | CommandReceiptState::Interrupted => existing
                .error_json
                .map(|error| ReceiptDecision::ReplayError {
                    code: error
                        .get("code")
                        .and_then(Value::as_str)
                        .unwrap_or("COMMAND_REJECTED")
                        .into(),
                    message: error
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or("command failed")
                        .into(),
                    retryable: error
                        .get("retryable")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                })
                .unwrap_or(ReceiptDecision::ReplayError {
                    code: "COMMAND_REJECTED".into(),
                    message: "command failed without a persisted error".into(),
                    retryable: false,
                }),
            CommandReceiptState::Pending | CommandReceiptState::InProgress => {
                ReceiptDecision::ReplayError {
                    code: "COMMAND_IN_PROGRESS".into(),
                    message: "the command is already in progress".into(),
                    retryable: true,
                }
            }
        });
    }

    let operation_key = command
        .payload
        .get("executionRunId")
        .and_then(Value::as_str)
        .or_else(|| command.payload.get("runId").and_then(Value::as_str))
        .map(|run_id| format!("{}:{run_id}", command.command))
        .unwrap_or_else(|| format!("{}:{}", command.command, command.request_id));
    let now = unix_time_ms();
    let receipt = CommandReceipt {
        key,
        command: command.command.clone(),
        payload_hash,
        operation_key,
        state: CommandReceiptState::InProgress,
        result_json: None,
        error_json: None,
        created_at: now,
        updated_at: now,
    };
    Ok(ReceiptDecision::New(receipt))
}

#[cfg(windows)]
fn complete_command_receipt(
    core: &mut PersistentCore,
    receipt: &mut CommandReceipt,
    payload: Value,
) -> Result<(), Box<dyn Error>> {
    receipt.state = CommandReceiptState::Completed;
    receipt.result_json = Some(payload);
    receipt.updated_at = unix_time_ms();
    core.save_command_receipt(receipt)?;
    Ok(())
}

#[cfg(windows)]
fn fail_command_receipt(
    core: &mut PersistentCore,
    receipt: &mut CommandReceipt,
    code: &str,
    message: &str,
    retryable: bool,
) -> Result<(), Box<dyn Error>> {
    receipt.state = CommandReceiptState::Failed;
    receipt.error_json = Some(json!({
        "code": code,
        "message": message,
        "retryable": retryable,
    }));
    receipt.updated_at = unix_time_ms();
    core.save_command_receipt(receipt)?;
    Ok(())
}

#[cfg(windows)]
fn command_payload_hash(command: &CommandEnvelope) -> Result<String, Box<dyn Error>> {
    let bytes = serde_json::to_vec(&json!({
        "command": command.command,
        "payload": command.payload,
        "deadlineMs": command.deadline_ms,
    }))?;
    let digest = Sha256::digest(bytes);
    Ok(digest.iter().map(|byte| format!("{byte:02x}")).collect())
}

#[cfg(windows)]
fn unix_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

#[cfg(windows)]
fn handle_event_replay(
    connection: &WriterQueue,
    host: &Arc<CoreHost>,
    session: &SessionBinding,
    query: QueryEnvelope,
) -> Result<(), Box<dyn Error>> {
    let owner = session.discovery_owner_scope();
    let (stream_kind, after_sequence, requested_epoch) = match replay_cursor(&query.payload) {
        Ok(cursor) => cursor,
        Err(error) => {
            write_error(
                connection,
                "INVALID_QUERY",
                &error.to_string(),
                false,
                &query.request_id,
            )?;
            return Ok(());
        }
    };
    let (events, server_epoch) = match host.event_stream(stream_kind, &owner) {
        Ok(stream) => stream,
        Err(error) => {
            write_discovery_stream_error(connection, error, &query.request_id)?;
            return Ok(());
        }
    };
    if requested_epoch
        .as_deref()
        .is_some_and(|epoch| epoch != server_epoch)
    {
        write_error(
            connection,
            "INVALID_QUERY",
            "event cursor epoch does not match the selected stream",
            false,
            &query.request_id,
        )?;
        return Ok(());
    }
    let limit = match replay_limit(&query.payload) {
        Ok(limit) => limit,
        Err(error) => {
            write_error(
                connection,
                "INVALID_QUERY",
                &error.to_string(),
                false,
                &query.request_id,
            )?;
            return Ok(());
        }
    };
    let fetched_events = match events.replay_after(after_sequence, limit.saturating_add(1)) {
        Ok(events) => events,
        Err(gap) => {
            write_replay_gap(
                connection,
                &query.request_id,
                &gap,
                stream_kind.id(),
                &server_epoch,
            )?;
            return Ok(());
        }
    };
    let has_more = fetched_events.len() > limit as usize;
    let event_values = fetched_events
        .into_iter()
        .take(limit as usize)
        .map(|event| {
            runtime_event_json(
                event,
                &query.session_id,
                stream_kind.id(),
                &server_epoch,
                None,
            )
        })
        .collect::<Vec<_>>();
    let next_sequence = event_values
        .last()
        .and_then(|event| event.get("cursor"))
        .and_then(|cursor| cursor.get("sequence"))
        .and_then(Value::as_u64)
        .unwrap_or(after_sequence);
    let (oldest_sequence, head, retention) = events.retention_window();
    write_response(
        connection,
        &query.request_id,
        json!({
            "events": event_values,
            "nextSequence": next_sequence,
            "hasMore": has_more,
            "oldestAvailableSequence": oldest_sequence,
            "headSequence": head,
            "retention": {
                "maxEvents": retention.max_events,
                "maxBytes": retention.max_bytes,
            },
        }),
    )?;
    Ok(())
}

#[cfg(windows)]
fn handle_query(
    connection: &WriterQueue,
    core: &PersistentCore,
    query: QueryEnvelope,
) -> Result<(), Box<dyn Error>> {
    let payload = match query.query.as_str() {
        "runtime.health" => core.runtime_health(),
        "runtime.models" => {
            if let Err(message) = runtime_models_query_payload(&query.payload) {
                write_error(
                    connection,
                    "INVALID_QUERY",
                    message,
                    false,
                    &query.request_id,
                )?;
                return Ok(());
            }
            core.runtime_models()
        }
        "connector.discover" => {
            if let Err(message) = local_discovery_query_payload(&query.payload) {
                write_error(
                    connection,
                    "INVALID_QUERY",
                    message,
                    false,
                    &query.request_id,
                )?;
                return Ok(());
            }
            core.discover_local_connectors()
        }
        "agent.scan_local" => {
            if let Err(message) = local_discovery_query_payload(&query.payload) {
                write_error(
                    connection,
                    "INVALID_QUERY",
                    message,
                    false,
                    &query.request_id,
                )?;
                return Ok(());
            }
            core.scan_local_agents()
        }
        "connector.health" => {
            let (scope_id, connector_id) = match connector_health_parameters(&query.payload) {
                Ok(parameters) => parameters,
                Err(error) => {
                    write_error(
                        connection,
                        "INVALID_QUERY",
                        &error.to_string(),
                        false,
                        &query.request_id,
                    )?;
                    return Ok(());
                }
            };
            match core.connector_health(&scope_id, &connector_id) {
                Ok(payload) => payload,
                Err(error) => {
                    // `connector.health` predates connector-scoped catalog
                    // errors and remains a generic legacy health query.
                    // Keep its existing IPC failure code stable; the additive
                    // `connector.models` query below owns route categories.
                    write_error(
                        connection,
                        "QUERY_REJECTED",
                        &error.to_string(),
                        false,
                        &query.request_id,
                    )?;
                    return Ok(());
                }
            }
        }
        "connector.models" => {
            let (scope_id, connector_id) = match connector_models_parameters(&query.payload) {
                Ok(parameters) => parameters,
                Err(error) => {
                    write_error(
                        connection,
                        "INVALID_QUERY",
                        &error.to_string(),
                        false,
                        &query.request_id,
                    )?;
                    return Ok(());
                }
            };
            match core.connector_models(&scope_id, &connector_id) {
                Ok(payload) => payload,
                Err(error) => {
                    write_core_error(connection, "QUERY_REJECTED", &error, &query.request_id)?;
                    return Ok(());
                }
            }
        }
        "connector.query" => {
            let (scope_id, connector_id, limit) = match connector_query_parameters(&query.payload) {
                Ok(parameters) => parameters,
                Err(error) => {
                    write_error(
                        connection,
                        "INVALID_QUERY",
                        &error.to_string(),
                        false,
                        &query.request_id,
                    )?;
                    return Ok(());
                }
            };
            match core.query_connector_profiles(&scope_id, connector_id.as_deref(), limit) {
                Ok(profiles) => json!({
                    "scopeId": scope_id,
                    "connectorProfiles": profiles,
                }),
                Err(error) => {
                    write_error(
                        connection,
                        "QUERY_REJECTED",
                        &error.to_string(),
                        false,
                        &query.request_id,
                    )?;
                    return Ok(());
                }
            }
        }
        "projection.snapshot" => core.projection_snapshot()?,
        "orchestration.run.snapshot" => {
            if let Err(message) = reject_unknown_fields(&query.payload, &["runId"]) {
                write_error(
                    connection,
                    "INVALID_QUERY",
                    &message.to_string(),
                    false,
                    &query.request_id,
                )?;
                return Ok(());
            }
            let run_id = match required_string(&query.payload, "runId") {
                Ok(run_id) => run_id,
                Err(error) => {
                    write_error(
                        connection,
                        "INVALID_QUERY",
                        &error.to_string(),
                        false,
                        &query.request_id,
                    )?;
                    return Ok(());
                }
            };
            match core.orchestration_projection(&run_id) {
                Ok(payload) => payload,
                Err(error) => {
                    write_error(
                        connection,
                        "QUERY_REJECTED",
                        &error.to_string(),
                        false,
                        &query.request_id,
                    )?;
                    return Ok(());
                }
            }
        }
        "orchestration.run.recovery_state" => {
            if let Err(message) = reject_unknown_fields(&query.payload, &["runId"]) {
                write_error(
                    connection,
                    "INVALID_QUERY",
                    &message.to_string(),
                    false,
                    &query.request_id,
                )?;
                return Ok(());
            }
            let run_id = match required_string(&query.payload, "runId") {
                Ok(run_id) => run_id,
                Err(error) => {
                    write_error(
                        connection,
                        "INVALID_QUERY",
                        &error.to_string(),
                        false,
                        &query.request_id,
                    )?;
                    return Ok(());
                }
            };
            match core.orchestration_recovery_state(&run_id) {
                Ok(payload) => payload,
                Err(error) => {
                    write_error(
                        connection,
                        "QUERY_REJECTED",
                        &error.to_string(),
                        false,
                        &query.request_id,
                    )?;
                    return Ok(());
                }
            }
        }
        "model_selection.snapshot" => {
            let run_id = match required_string(&query.payload, "executionRunId") {
                Ok(run_id) => run_id,
                Err(error) => {
                    write_error(
                        connection,
                        "INVALID_QUERY",
                        &error.to_string(),
                        false,
                        &query.request_id,
                    )?;
                    return Ok(());
                }
            };
            match (
                core.model_snapshot(&run_id),
                core.model_selection_snapshot(&run_id),
            ) {
                (Ok(model_snapshot), Ok(selection_snapshot)) => json!({
                    "executionRunId": run_id,
                    "modelSnapshot": model_snapshot,
                    "selectionSnapshot": selection_snapshot,
                }),
                (Err(error), _) | (_, Err(error)) => {
                    write_error(
                        connection,
                        "QUERY_REJECTED",
                        &error.to_string(),
                        false,
                        &query.request_id,
                    )?;
                    return Ok(());
                }
            }
        }
        "identity_model_options.list" => {
            let (target, connector_id) = match identity_model_target_from_payload(&query.payload) {
                Ok(value) => value,
                Err(error) => {
                    write_error(
                        connection,
                        "INVALID_QUERY",
                        &error.to_string(),
                        false,
                        &query.request_id,
                    )?;
                    return Ok(());
                }
            };
            match core.identity_model_options(&target, connector_id.as_deref()) {
                Ok(options) => json!({
                    "target": target,
                    "connectorId": connector_id,
                    "options": options,
                }),
                Err(error) => {
                    write_error(
                        connection,
                        "QUERY_REJECTED",
                        &error.to_string(),
                        false,
                        &query.request_id,
                    )?;
                    return Ok(());
                }
            }
        }
        "execution.get" => {
            let run_id = match required_string(&query.payload, "executionRunId") {
                Ok(run_id) => run_id,
                Err(error) => {
                    write_error(
                        connection,
                        "INVALID_QUERY",
                        &error.to_string(),
                        false,
                        &query.request_id,
                    )?;
                    return Ok(());
                }
            };
            match core.recover_run(&run_id) {
                Ok(run) => json!({"run": run}),
                Err(error) => {
                    write_error(
                        connection,
                        "QUERY_REJECTED",
                        &error.to_string(),
                        false,
                        &query.request_id,
                    )?;
                    return Ok(());
                }
            }
        }
        "messages.search" => {
            let (search_query, conversation_id, limit) =
                match message_search_parameters(&query.payload) {
                    Ok(parameters) => parameters,
                    Err(error) => {
                        write_error(
                            connection,
                            "INVALID_QUERY",
                            &error.to_string(),
                            false,
                            &query.request_id,
                        )?;
                        return Ok(());
                    }
                };
            match core.search_messages(&search_query, conversation_id.as_deref(), limit) {
                Ok(messages) => json!({"messages": messages}),
                Err(error) => {
                    write_error(
                        connection,
                        "QUERY_REJECTED",
                        &error.to_string(),
                        false,
                        &query.request_id,
                    )?;
                    return Ok(());
                }
            }
        }
        "summary.content" => {
            let summary_id = match required_string(&query.payload, "summaryId") {
                Ok(summary_id) => summary_id,
                Err(error) => {
                    write_error(
                        connection,
                        "INVALID_QUERY",
                        &error.to_string(),
                        false,
                        &query.request_id,
                    )?;
                    return Ok(());
                }
            };
            match core.load_summary_content(&summary_id) {
                Ok(content) => json!({"summaryId": summary_id, "content": content}),
                Err(error) => {
                    write_error(
                        connection,
                        "QUERY_REJECTED",
                        &error.to_string(),
                        false,
                        &query.request_id,
                    )?;
                    return Ok(());
                }
            }
        }
        "artifact.content" => {
            let artifact_id = match required_string(&query.payload, "artifactId") {
                Ok(artifact_id) => artifact_id,
                Err(error) => {
                    write_error(
                        connection,
                        "INVALID_QUERY",
                        &error.to_string(),
                        false,
                        &query.request_id,
                    )?;
                    return Ok(());
                }
            };
            let offset = match query.payload.get("offset").and_then(Value::as_u64) {
                Some(offset) => offset,
                None => {
                    write_error(
                        connection,
                        "INVALID_QUERY",
                        "offset must be a non-negative integer",
                        false,
                        &query.request_id,
                    )?;
                    return Ok(());
                }
            };
            let limit = match query.payload.get("limit").and_then(Value::as_u64) {
                Some(limit) if (1..=ARTIFACT_CONTENT_CHUNK_MAX_BYTES).contains(&limit) => limit,
                _ => {
                    write_error(
                        connection,
                        "INVALID_QUERY",
                        &format!(
                            "limit must be between 1 and {ARTIFACT_CONTENT_CHUNK_MAX_BYTES} bytes"
                        ),
                        false,
                        &query.request_id,
                    )?;
                    return Ok(());
                }
            };
            match core.read_artifact_body_chunk(&artifact_id, offset, limit) {
                Ok(chunk) => json!({
                    "artifactId": chunk.artifact_id,
                    "sha256": chunk.sha256,
                    "offset": chunk.offset,
                    "size": chunk.size,
                    "chunkBase64": STANDARD.encode(&chunk.bytes),
                    "chunkBytes": chunk.bytes.len(),
                    "eof": chunk.eof,
                }),
                Err(error) => {
                    write_error(
                        connection,
                        "QUERY_REJECTED",
                        &error.to_string(),
                        false,
                        &query.request_id,
                    )?;
                    return Ok(());
                }
            }
        }
        "retrieval.preview" => {
            let (request, vector_fixture) = match retrieval_preview_parameters(&query.payload) {
                Ok(request) => request,
                Err(error) => {
                    write_error(
                        connection,
                        "INVALID_QUERY",
                        &error.to_string(),
                        false,
                        &query.request_id,
                    )?;
                    return Ok(());
                }
            };
            let result = if vector_fixture {
                core.preview_retrieval_vector(request)
            } else {
                core.preview_retrieval(request)
            };
            match result {
                Ok(result) => result,
                Err(error) => {
                    write_error(
                        connection,
                        "QUERY_REJECTED",
                        &error.to_string(),
                        false,
                        &query.request_id,
                    )?;
                    return Ok(());
                }
            }
        }
        "retrieval.query" => {
            let (scope_id, source_ids, limit) = match retrieval_query_parameters(&query.payload) {
                Ok(parameters) => parameters,
                Err(error) => {
                    write_error(
                        connection,
                        "INVALID_QUERY",
                        &error.to_string(),
                        false,
                        &query.request_id,
                    )?;
                    return Ok(());
                }
            };
            match core.query_retrieval_sources(&scope_id, source_ids.as_deref(), limit) {
                Ok(sources) => json!({"retrievalSources": sources}),
                Err(error) => {
                    write_error(
                        connection,
                        "QUERY_REJECTED",
                        &error.to_string(),
                        false,
                        &query.request_id,
                    )?;
                    return Ok(());
                }
            }
        }
        "retrieval.selections" => {
            let (scope_id, selection_ids, limit) =
                match retrieval_selection_query_parameters(&query.payload) {
                    Ok(parameters) => parameters,
                    Err(error) => {
                        write_error(
                            connection,
                            "INVALID_QUERY",
                            &error.to_string(),
                            false,
                            &query.request_id,
                        )?;
                        return Ok(());
                    }
                };
            match core.query_retrieval_selections(&scope_id, selection_ids.as_deref(), limit) {
                Ok(selections) => json!({"retrievalSelections": selections}),
                Err(error) => {
                    write_error(
                        connection,
                        "QUERY_REJECTED",
                        &error.to_string(),
                        false,
                        &query.request_id,
                    )?;
                    return Ok(());
                }
            }
        }
        "retrieval.feedback" => {
            let (scope_id, selection_id, limit) =
                match retrieval_feedback_query_parameters(&query.payload) {
                    Ok(parameters) => parameters,
                    Err(error) => {
                        write_error(
                            connection,
                            "INVALID_QUERY",
                            &error.to_string(),
                            false,
                            &query.request_id,
                        )?;
                        return Ok(());
                    }
                };
            match core.query_retrieval_feedback(&scope_id, selection_id.as_deref(), limit) {
                Ok(feedback) => json!({"retrievalFeedback": feedback}),
                Err(error) => {
                    write_error(
                        connection,
                        "QUERY_REJECTED",
                        &error.to_string(),
                        false,
                        &query.request_id,
                    )?;
                    return Ok(());
                }
            }
        }
        _ => {
            write_error(
                connection,
                "UNSUPPORTED_QUERY",
                "query is not implemented by this Core build",
                false,
                &query.request_id,
            )?;
            return Ok(());
        }
    };
    Ok(write_response(connection, &query.request_id, payload)?)
}

#[cfg(windows)]
fn runtime_models_query_payload(payload: &Value) -> Result<(), &'static str> {
    if payload.as_object().is_some_and(|object| object.is_empty()) {
        Ok(())
    } else {
        Err("runtime.models accepts an empty payload only")
    }
}

#[cfg(windows)]
fn local_discovery_query_payload(payload: &Value) -> Result<(), &'static str> {
    if payload.as_object().is_some_and(|object| object.is_empty()) {
        Ok(())
    } else {
        Err("local discovery accepts an empty payload only")
    }
}

#[cfg(windows)]
fn runtime_event_json(
    event: agenttalk_events::RuntimeEvent,
    session_id: &str,
    stream_id: &str,
    server_epoch: &str,
    subscription_id: Option<&str>,
) -> Value {
    json!({
        "kind": "event",
        "protocol": {"major": PROTOCOL_MAJOR, "minor": 0},
        "eventId": event.event_id,
        "sessionId": session_id,
        "cursor": StreamCursor {
            stream_id: stream_id.into(),
            sequence: event.sequence,
            epoch: Some(server_epoch.into()),
        },
        "subscriptionId": subscription_id,
        "executionRunId": event.execution_run_id,
        "event": event.event_type,
        "occurredAt": occurred_at(event.timestamp_ms),
        "payload": event.payload,
    })
}

#[cfg(windows)]
fn write_response(
    connection: &WriterQueue,
    request_id: &str,
    payload: Value,
) -> Result<(), TransportError> {
    let response = serde_json::to_value(ResponseEnvelope {
        kind: "response".into(),
        protocol: ProtocolVersion {
            major: PROTOCOL_MAJOR,
            minor: 0,
        },
        request_id: request_id.into(),
        ok: true,
        payload,
    })
    .map_err(|_| TransportError::Closed)?;
    connection
        .send(response)
        .map_err(|_| TransportError::Closed)
}

#[cfg(windows)]
fn write_error(
    connection: &WriterQueue,
    code: &str,
    message: &str,
    retryable: bool,
    request_id: &str,
) -> Result<(), TransportError> {
    write_error_with_details(connection, code, message, retryable, request_id, None)
}

#[cfg(windows)]
fn write_core_error(
    connection: &WriterQueue,
    fallback_code: &str,
    error: &CoreError,
    request_id: &str,
) -> Result<(), TransportError> {
    let (code, message, category) = core_error_details(fallback_code, error);
    write_error_with_details(
        connection,
        code,
        message,
        false,
        request_id,
        (code != fallback_code).then(|| json!({"category": category})),
    )
}

#[cfg(windows)]
fn core_error_details<'a>(
    fallback_code: &'a str,
    error: &CoreError,
) -> (&'a str, &'static str, &'static str) {
    match error {
        CoreError::ConnectorNotFound => (
            "CONNECTOR_NOT_FOUND",
            "connector profile does not exist",
            "connector_not_found",
        ),
        CoreError::ConnectorDisabled => (
            "CONNECTOR_DISABLED",
            "connector profile is disabled",
            "connector_disabled",
        ),
        CoreError::ConnectorRuntimeUnavailable | CoreError::ConnectorUnverified => (
            "CONNECTOR_RUNTIME_UNAVAILABLE",
            "connector Runtime is unavailable",
            "connector_runtime_unavailable",
        ),
        CoreError::ConnectorRuntimeMismatch => (
            "CONNECTOR_RUNTIME_MISMATCH",
            "connector Runtime type does not match the frozen route",
            "connector_runtime_mismatch",
        ),
        CoreError::ConnectorModelUnavailable => (
            "CONNECTOR_MODEL_UNAVAILABLE",
            "model is unavailable from the selected connector catalog",
            "connector_model_unavailable",
        ),
        CoreError::ConnectorCatalogUnavailable => (
            "CONNECTOR_CATALOG_UNAVAILABLE",
            "connector Runtime catalog is unavailable",
            "connector_catalog_unavailable",
        ),
        CoreError::ConnectorBindingRequired => (
            "CONNECTOR_BINDING_REQUIRED",
            "a connector is required when a model is specified",
            "connector_binding_required",
        ),
        CoreError::Runtime(runtime_error) => {
            if let Some(classification) = connector_runtime_failure(runtime_error) {
                (
                    classification.ipc_code(),
                    classification.message(),
                    classification.category(),
                )
            } else {
                (
                    fallback_code,
                    "Core rejected the requested operation",
                    "core_rejected",
                )
            }
        }
        _ => (
            fallback_code,
            "Core rejected the requested operation",
            "core_rejected",
        ),
    }
}

#[cfg(windows)]
fn fail_command_with_core_error(
    core: &mut PersistentCore,
    receipt: &mut CommandReceipt,
    connection: &WriterQueue,
    fallback_code: &str,
    error: &CoreError,
    request_id: &str,
) -> Result<(), Box<dyn Error>> {
    let (code, message, category) = core_error_details(fallback_code, error);
    fail_command_receipt(core, receipt, code, message, false)?;
    write_error_with_details(
        connection,
        code,
        message,
        false,
        request_id,
        (code != fallback_code).then(|| json!({"category": category})),
    )?;
    Ok(())
}

#[cfg(windows)]
fn write_error_with_details(
    connection: &WriterQueue,
    code: &str,
    message: &str,
    retryable: bool,
    request_id: &str,
    details: Option<Value>,
) -> Result<(), TransportError> {
    let error = serde_json::to_value(ErrorEnvelope {
        kind: "error".into(),
        protocol: ProtocolVersion {
            major: PROTOCOL_MAJOR,
            minor: 0,
        },
        request_id: request_id.into(),
        code: code.into(),
        message: message.into(),
        retryable,
        details,
    })
    .map_err(|_| TransportError::Closed)?;
    connection.send(error).map_err(|_| TransportError::Closed)
}

#[cfg(windows)]
fn write_replay_gap(
    connection: &WriterQueue,
    request_id: &str,
    gap: &ReplayGap,
    stream_id: &str,
    server_epoch: &str,
) -> Result<(), TransportError> {
    write_error_with_details(
        connection,
        "REPLAY_GAP",
        "the requested event cursor is outside the bounded IPC retention window; a snapshot is required before resuming",
        true,
        request_id,
        Some(gap.details(stream_id, server_epoch)),
    )
}

#[cfg(windows)]
fn write_projection_mutation_response(
    connection: &WriterQueue,
    core: &mut PersistentCore,
    request_id: &str,
    action: &str,
) -> Result<(), Box<dyn Error>> {
    core.record_projection_changed(action)?;
    write_response(
        connection,
        request_id,
        json!({"changed": true, "action": action, "projection": core.projection_snapshot()?}),
    )?;
    Ok(())
}

#[cfg(windows)]
fn request_id_for_error(envelope: &Value) -> String {
    envelope
        .get("requestId")
        .and_then(Value::as_str)
        .filter(|value| {
            !value.is_empty()
                && value.len() <= 128
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"._:-".contains(&byte))
        })
        .map(str::to_owned)
        .unwrap_or_else(|| "unknown".into())
}

#[cfg(windows)]
fn replay_cursor(
    payload: &Value,
) -> Result<(EventStreamKind, u64, Option<String>), Box<dyn Error>> {
    let stream_kind = match payload.get("streamId") {
        None => EventStreamKind::Core,
        Some(Value::String(value)) => EventStreamKind::parse(value)
            .ok_or_else(|| "streamId is not a supported event stream".to_owned())?,
        Some(_) => return Err("streamId must be a string".into()),
    };
    let requested_epoch = match payload.get("epoch") {
        None => None,
        Some(Value::String(value)) => Some(value.clone()),
        Some(_) => return Err("epoch must be a string".into()),
    };
    let after_sequence = match payload.get("afterSequence") {
        None => 0,
        Some(value) => value
            .as_u64()
            .ok_or_else(|| "afterSequence must be a non-negative integer".to_owned())?,
    };
    Ok((stream_kind, after_sequence, requested_epoch))
}

#[cfg(windows)]
fn replay_limit(payload: &Value) -> Result<u64, Box<dyn Error>> {
    let limit = match payload.get("limit") {
        None => 64,
        Some(value) => value
            .as_u64()
            .ok_or_else(|| "limit must be a positive integer".to_owned())?,
    };
    if limit == 0 {
        return Err("limit must be a positive integer".into());
    }
    Ok(limit.min(256))
}

#[cfg(windows)]
fn subscription_cursor(
    payload: &Value,
) -> Result<(EventStreamKind, u64, Option<String>), &'static str> {
    let cursor = payload.get("afterCursor");
    if let Some(cursor) = cursor {
        let object = cursor.as_object().ok_or("INVALID_COMMAND")?;
        let stream_kind = object
            .get("streamId")
            .and_then(Value::as_str)
            .and_then(EventStreamKind::parse)
            .ok_or("INVALID_COMMAND")?;
        let requested_epoch = object
            .get("epoch")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or("CURSOR_EPOCH_MISMATCH")?;
        let sequence = object
            .get("sequence")
            .and_then(Value::as_u64)
            .ok_or("INVALID_COMMAND")?;
        return Ok((stream_kind, sequence, Some(requested_epoch)));
    }
    let stream_kind = match payload.get("streamId") {
        None => EventStreamKind::Core,
        Some(Value::String(value)) => EventStreamKind::parse(value).ok_or("INVALID_COMMAND")?,
        Some(_) => return Err("INVALID_COMMAND"),
    };
    let requested_epoch = match payload.get("epoch") {
        None => None,
        Some(Value::String(value)) => Some(value.clone()),
        Some(_) => return Err("INVALID_COMMAND"),
    };
    let sequence = match payload.get("afterSequence") {
        None => 0,
        Some(value) => value.as_u64().ok_or("INVALID_COMMAND")?,
    };
    Ok((stream_kind, sequence, requested_epoch))
}

#[cfg(windows)]
fn bounded_subscription_events(payload: &Value) -> Result<u64, &'static str> {
    let value = payload
        .get("maxInFlightEvents")
        .and_then(Value::as_u64)
        .unwrap_or(64);
    if value == 0 {
        return Err("maxInFlightEvents must be a positive integer");
    }
    Ok(value.min(256))
}

#[cfg(windows)]
fn bounded_subscription_bytes(payload: &Value) -> Result<usize, &'static str> {
    let value = payload
        .get("maxInFlightBytes")
        .and_then(Value::as_u64)
        .unwrap_or(256 * 1024);
    if !(1024..=1024 * 1024).contains(&value) {
        return Err("maxInFlightBytes must be between 1024 and 1048576");
    }
    Ok(value as usize)
}

#[cfg(windows)]
fn occurred_at(timestamp_ms: i64) -> String {
    // Convert a Unix epoch millisecond value into a schema-valid UTC RFC3339
    // instant with millisecond precision. Negative inputs clamp to the epoch
    // so the function is total and stable. The civil date is computed with
    // Howard Hinnant's days-from-epoch algorithm, avoiding a new dependency.
    let millis = timestamp_ms.max(0);
    let total_seconds = millis / 1000;
    let sub_millis = millis % 1000;
    let days = total_seconds.div_euclid(86_400);
    let secs_of_day = total_seconds.rem_euclid(86_400);
    let hours = secs_of_day / 3600;
    let minutes = (secs_of_day % 3600) / 60;
    let seconds = secs_of_day % 60;
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { year + 1 } else { year };
    // RFC3339 full-date requires exactly four digit years; values beyond the
    // maximum schema-valid instant deterministically clamp to the upper bound
    // instead of emitting an extended year.
    if year > 9999 {
        return "9999-12-31T23:59:59.999Z".to_owned();
    }
    format!("{year:04}-{month:02}-{day:02}T{hours:02}:{minutes:02}:{seconds:02}.{sub_millis:03}Z")
}

#[cfg(windows)]
fn message_search_parameters(
    payload: &Value,
) -> Result<(String, Option<String>, u64), Box<dyn Error>> {
    let query = required_string(payload, "query")?;
    if query.trim().is_empty() {
        return Err("search query must not be blank".into());
    }
    let conversation_id = optional_string(payload, "conversationId")?;
    let limit = match payload.get("limit") {
        None => 20,
        Some(value) => {
            let limit = value
                .as_u64()
                .ok_or_else(|| "limit must be a positive integer".to_owned())?;
            if limit == 0 {
                return Err("limit must be a positive integer".into());
            }
            limit.min(100)
        }
    };
    Ok((query, conversation_id, limit))
}

#[cfg(windows)]
fn retrieval_preview_parameters(
    payload: &Value,
) -> Result<(RetrievalPreviewRequest, bool), Box<dyn Error>> {
    reject_unknown_fields(
        payload,
        &[
            "expectedProjectId",
            "conversationId",
            "agentId",
            "query",
            "scope",
            "sourceTypes",
            "limit",
            "mode",
        ],
    )?;
    let vector_fixture = match payload.get("mode") {
        None => false,
        Some(Value::String(mode)) if mode == "exact" => false,
        Some(Value::String(mode)) if mode == "vector_fixture" => true,
        Some(_) => return Err("mode must be exact or vector_fixture".into()),
    };
    let source_types = match payload.get("sourceTypes") {
        None => vec!["message".into(), "execution".into()],
        Some(Value::Array(values)) if !values.is_empty() => values
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .filter(|value| !value.is_empty())
                    .map(str::to_owned)
                    .ok_or_else(|| "sourceTypes must contain non-empty strings".to_owned())
            })
            .collect::<Result<Vec<_>, _>>()?,
        Some(_) => return Err("sourceTypes must be a non-empty array".into()),
    };
    let limit = match payload.get("limit") {
        None => 20,
        Some(value) => {
            let value = value
                .as_u64()
                .ok_or_else(|| "limit must be a positive integer".to_owned())?;
            if value == 0 {
                return Err("limit must be a positive integer".into());
            }
            value.min(RETRIEVAL_PREVIEW_LIMIT_MAX)
        }
    };
    let scope = required_string(payload, "scope")?;
    if !matches!(scope.as_str(), "conversation" | "project") {
        return Err("scope must be conversation or project".into());
    }
    if source_types.iter().any(|source_type| {
        !matches!(
            source_type.as_str(),
            "message" | "execution" | "project_file"
        )
    }) {
        return Err("sourceTypes contains an unknown value".into());
    }
    Ok((
        RetrievalPreviewRequest {
            expected_project_id: required_string(payload, "expectedProjectId")?,
            conversation_id: required_string(payload, "conversationId")?,
            agent_id: required_string(payload, "agentId")?,
            query: required_string(payload, "query")?,
            scope,
            source_types,
            limit,
        },
        vector_fixture,
    ))
}

#[cfg(windows)]
fn reject_unknown_fields(payload: &Value, allowed: &[&str]) -> Result<(), Box<dyn Error>> {
    let object = payload
        .as_object()
        .ok_or_else(|| "query payload must be a JSON object".to_owned())?;
    if object.keys().any(|key| !allowed.contains(&key.as_str())) {
        return Err("query payload contains an unknown field".into());
    }
    Ok(())
}

#[cfg(windows)]
type RetrievalQueryParameters = (String, Option<Vec<String>>, u64);

#[cfg(windows)]
fn retrieval_query_parameters(payload: &Value) -> Result<RetrievalQueryParameters, Box<dyn Error>> {
    let scope_id = required_string(payload, "scopeId")?;
    let source_ids = match payload.get("sourceIds") {
        None | Some(Value::Null) => None,
        Some(Value::Array(values)) => Some(
            values
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .filter(|id| !id.is_empty())
                        .map(str::to_owned)
                        .ok_or_else(|| "sourceIds must contain non-empty strings".to_owned())
                })
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Some(_) => return Err("sourceIds must be an array or null".into()),
    };
    let limit = match payload.get("limit") {
        None => 20,
        Some(value) => {
            let limit = value
                .as_u64()
                .ok_or_else(|| "limit must be a positive integer".to_owned())?;
            if limit == 0 {
                return Err("limit must be a positive integer".into());
            }
            limit.min(100)
        }
    };
    Ok((scope_id, source_ids, limit))
}

#[cfg(windows)]
fn retrieval_selection_query_parameters(
    payload: &Value,
) -> Result<RetrievalQueryParameters, Box<dyn Error>> {
    retrieval_id_query_parameters(payload, "selectionIds")
}

#[cfg(windows)]
fn retrieval_feedback_query_parameters(
    payload: &Value,
) -> Result<(String, Option<String>, u64), Box<dyn Error>> {
    let scope_id = required_string(payload, "scopeId")?;
    let selection_id = optional_string(payload, "selectionId")?;
    let limit = bounded_retrieval_limit(payload)?;
    Ok((scope_id, selection_id, limit))
}

#[cfg(windows)]
fn connector_profile_from_payload(payload: &Value) -> Result<ConnectorProfile, Box<dyn Error>> {
    reject_unknown_fields(
        payload,
        &[
            "scopeId",
            "connectorId",
            "displayName",
            "providerType",
            "runtimeType",
            "enabled",
            "authEnvKey",
        ],
    )?;
    let scope_id = required_string(payload, "scopeId")?;
    if scope_id != CONNECTOR_PROFILE_SCOPE {
        return Err("connector profile scopeId must be desktop".into());
    }
    Ok(ConnectorProfile {
        scope_id,
        connector_id: required_string(payload, "connectorId")?,
        display_name: required_string(payload, "displayName")?,
        provider_type: required_string(payload, "providerType")?,
        runtime_type: required_string(payload, "runtimeType")?,
        enabled: required_bool(payload, "enabled")?,
        auth_env_key: optional_string(payload, "authEnvKey")?,
    })
}

#[cfg(windows)]
fn connector_remove_from_payload(payload: &Value) -> Result<(String, String), Box<dyn Error>> {
    reject_unknown_fields(payload, &["scopeId", "connectorId"])?;
    let scope_id = required_string(payload, "scopeId")?;
    if scope_id != CONNECTOR_PROFILE_SCOPE {
        return Err("connector.remove scopeId must be desktop".into());
    }
    Ok((scope_id, required_string(payload, "connectorId")?))
}

#[cfg(windows)]
fn connector_query_parameters(
    payload: &Value,
) -> Result<(String, Option<String>, u64), Box<dyn Error>> {
    reject_unknown_fields(payload, &["scopeId", "connectorId", "limit"])?;
    let scope_id = required_string(payload, "scopeId")?;
    if scope_id != CONNECTOR_PROFILE_SCOPE {
        return Err("connector.query scopeId must be desktop".into());
    }
    let connector_id = optional_string(payload, "connectorId")?;
    let limit = match payload.get("limit") {
        None => 100,
        Some(value) => {
            let limit = value
                .as_u64()
                .ok_or_else(|| "connector.query limit must be a positive integer".to_owned())?;
            if limit == 0 {
                return Err("connector.query limit must be a positive integer".into());
            }
            if limit > CONNECTOR_PROFILE_QUERY_LIMIT_MAX {
                return Err(format!(
                    "connector.query limit must be at most {CONNECTOR_PROFILE_QUERY_LIMIT_MAX}"
                )
                .into());
            }
            limit
        }
    };
    Ok((scope_id, connector_id, limit))
}

#[cfg(windows)]
fn connector_health_parameters(payload: &Value) -> Result<(String, String), Box<dyn Error>> {
    reject_unknown_fields(payload, &["scopeId", "connectorId"])?;
    let scope_id = required_string(payload, "scopeId")?;
    if scope_id != CONNECTOR_PROFILE_SCOPE {
        return Err("connector.health scopeId must be desktop".into());
    }
    Ok((scope_id, required_string(payload, "connectorId")?))
}

#[cfg(windows)]
fn connector_models_parameters(payload: &Value) -> Result<(String, String), Box<dyn Error>> {
    reject_unknown_fields(payload, &["scopeId", "connectorId"])?;
    let scope_id = required_string(payload, "scopeId")?;
    if scope_id != CONNECTOR_PROFILE_SCOPE {
        return Err("connector.models scopeId must be desktop".into());
    }
    Ok((scope_id, required_string(payload, "connectorId")?))
}

#[cfg(windows)]
fn retrieval_id_query_parameters(
    payload: &Value,
    key: &str,
) -> Result<RetrievalQueryParameters, Box<dyn Error>> {
    let scope_id = required_string(payload, "scopeId")?;
    let ids = match payload.get(key) {
        None | Some(Value::Null) => None,
        Some(Value::Array(values)) => Some(
            values
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .filter(|id| !id.is_empty())
                        .map(str::to_owned)
                        .ok_or_else(|| format!("{key} must contain non-empty strings"))
                })
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Some(_) => return Err(format!("{key} must be an array or null").into()),
    };
    Ok((scope_id, ids, bounded_retrieval_limit(payload)?))
}

#[cfg(windows)]
fn bounded_retrieval_limit(payload: &Value) -> Result<u64, Box<dyn Error>> {
    let limit = match payload.get("limit") {
        None => 20,
        Some(value) => {
            let limit = value
                .as_u64()
                .ok_or_else(|| "limit must be a positive integer".to_owned())?;
            if limit == 0 {
                return Err("limit must be a positive integer".into());
            }
            limit
        }
    };
    Ok(limit.min(100))
}

#[cfg(windows)]
fn retrieval_selection_from_payload(payload: &Value) -> Result<RetrievalSelection, Box<dyn Error>> {
    let mut value = payload.clone();
    let object = value
        .as_object_mut()
        .ok_or_else(|| "retrieval selection payload must be an object".to_owned())?;
    if !object.contains_key("id") {
        let id = object
            .remove("selectionId")
            .ok_or_else(|| "missing required field selectionId".to_owned())?;
        object.insert("id".into(), id);
    }
    serde_json::from_value(value)
        .map_err(|error| format!("invalid retrieval selection: {error}").into())
}

#[cfg(windows)]
fn retrieval_feedback_from_payload(payload: &Value) -> Result<RetrievalFeedback, Box<dyn Error>> {
    let mut value = payload.clone();
    let object = value
        .as_object_mut()
        .ok_or_else(|| "retrieval feedback payload must be an object".to_owned())?;
    if !object.contains_key("id") {
        let id = object
            .remove("feedbackId")
            .ok_or_else(|| "missing required field feedbackId".to_owned())?;
        object.insert("id".into(), id);
    }
    serde_json::from_value(value)
        .map_err(|error| format!("invalid retrieval feedback: {error}").into())
}

#[cfg(windows)]
fn required_string(payload: &Value, key: &str) -> Result<String, Box<dyn Error>> {
    payload
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| format!("missing required field {key}").into())
}

#[cfg(windows)]
fn required_i64(payload: &Value, key: &str) -> Result<i64, Box<dyn Error>> {
    payload
        .get(key)
        .and_then(Value::as_i64)
        .filter(|value| *value >= 0)
        .ok_or_else(|| format!("missing or invalid non-negative integer field {key}").into())
}

#[cfg(windows)]
fn required_orchestration_digest(payload: &Value, key: &str) -> Result<String, Box<dyn Error>> {
    let value = required_string(payload, key)?;
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(format!("{key} must be lowercase hex64").into());
    }
    Ok(value)
}

#[cfg(windows)]
fn required_orchestration_object_ref(payload: &Value, key: &str) -> Result<String, Box<dyn Error>> {
    let value = required_string(payload, key)?;
    let digest = value
        .strip_prefix("sha256:")
        .ok_or_else(|| format!("{key} must be sha256:<lowercase hex64>"))?;
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(format!("{key} must be sha256:<lowercase hex64>").into());
    }
    Ok(value)
}

#[cfg(windows)]
fn optional_string(payload: &Value, key: &str) -> Result<Option<String>, Box<dyn Error>> {
    match payload.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if !value.is_empty() => Ok(Some(value.clone())),
        Some(_) => Err(format!("field {key} must be a non-empty string or null").into()),
    }
}

#[cfg(windows)]
fn required_bool(payload: &Value, key: &str) -> Result<bool, Box<dyn Error>> {
    payload
        .get(key)
        .and_then(Value::as_bool)
        .ok_or_else(|| format!("missing required boolean field {key}").into())
}

#[cfg(windows)]
fn optional_bool(payload: &Value, key: &str) -> Result<bool, Box<dyn Error>> {
    match payload.get(key) {
        None | Some(Value::Null) => Ok(false),
        Some(Value::Bool(value)) => Ok(*value),
        Some(_) => Err(format!("field {key} must be a boolean or null").into()),
    }
}

#[cfg(windows)]
fn collaboration_from_payload(
    payload: &Value,
) -> Result<(String, CollaborationRun), Box<dyn Error>> {
    let root_agent_ids = payload
        .get("rootAgentIds")
        .and_then(Value::as_array)
        .ok_or_else(|| "rootAgentIds must be an array".to_owned())?
        .iter()
        .map(|value| {
            value
                .as_str()
                .filter(|id| !id.is_empty())
                .map(str::to_owned)
                .ok_or_else(|| "rootAgentIds must contain non-empty strings".to_owned())
        })
        .collect::<Result<Vec<_>, _>>()?;
    if root_agent_ids.is_empty() {
        return Err("rootAgentIds must not be empty".into());
    }
    let max_calls = positive_u32(payload, "maxCalls")?;
    let max_depth = positive_u32(payload, "maxDepth")?;
    let auto_dispatch_handoffs = match payload.get("autoDispatchHandoffs") {
        None => false,
        Some(Value::Bool(value)) => *value,
        Some(_) => return Err("autoDispatchHandoffs must be a boolean".into()),
    };
    Ok((
        required_string(payload, "projectId")?,
        CollaborationRun {
            id: required_string(payload, "collaborationRunId")?,
            root_agent_ids,
            call_count: optional_u32(payload, "callCount")?,
            max_calls,
            depth: optional_u32(payload, "depth")?,
            max_depth,
            status: collaboration_status(
                payload
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or("pending"),
            )?,
            stop_reason: optional_string(payload, "stopReason")?,
            auto_dispatch_handoffs,
        },
    ))
}

#[cfg(windows)]
fn handoff_from_payload(payload: &Value) -> Result<Handoff, Box<dyn Error>> {
    let details = match payload.get("details") {
        None | Some(Value::Null) => None,
        Some(value) => Some(
            serde_json::from_value::<StructuredHandoffDetails>(value.clone())
                .map_err(|error| format!("details must match StructuredHandoffDetails: {error}"))?,
        ),
    };
    Ok(Handoff {
        id: required_string(payload, "handoffId")?,
        collaboration_run_id: required_string(payload, "collaborationRunId")?,
        from_execution_run_id: required_string(payload, "fromExecutionRunId")?,
        to_agent_id: required_string(payload, "toAgentId")?,
        status: required_string(payload, "status")?,
        details,
    })
}

#[cfg(windows)]
fn collaboration_status(value: &str) -> Result<CollaborationStatus, Box<dyn Error>> {
    match value.to_ascii_lowercase().as_str() {
        "pending" => Ok(CollaborationStatus::Pending),
        "running" => Ok(CollaborationStatus::Running),
        "completed" => Ok(CollaborationStatus::Completed),
        "failed" => Ok(CollaborationStatus::Failed),
        "cancelled" | "canceled" => Ok(CollaborationStatus::Cancelled),
        "interrupted" => Ok(CollaborationStatus::Interrupted),
        _ => Err(
            "status must be pending, running, completed, failed, cancelled, or interrupted".into(),
        ),
    }
}

#[cfg(windows)]
fn positive_u32(payload: &Value, key: &str) -> Result<u32, Box<dyn Error>> {
    let value = payload
        .get(key)
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("{key} must be a positive integer"))?;
    let value = u32::try_from(value).map_err(|_| format!("{key} is out of range"))?;
    if value == 0 {
        return Err(format!("{key} must be a positive integer").into());
    }
    Ok(value)
}

#[cfg(windows)]
fn optional_u32(payload: &Value, key: &str) -> Result<u32, Box<dyn Error>> {
    let Some(value) = payload.get(key) else {
        return Ok(0);
    };
    let value = value
        .as_u64()
        .ok_or_else(|| format!("{key} must be a non-negative integer"))?;
    u32::try_from(value).map_err(|_| format!("{key} is out of range").into())
}

#[cfg(windows)]
fn agent_from_payload(payload: &Value) -> Result<AgentIdentity, Box<dyn Error>> {
    Ok(AgentIdentity {
        id: required_string(payload, "agentId")?,
        name: required_string(payload, "name")?,
        role: required_string(payload, "role")?,
        specialty: required_string(payload, "specialty")?,
        system_prompt: required_string(payload, "systemPrompt")?,
    })
}

#[cfg(windows)]
fn config_from_payload(payload: &Value) -> Result<Value, Box<dyn Error>> {
    let object = payload
        .as_object()
        .ok_or_else(|| "config.import payload must be an object".to_owned())?;
    if object.len() != 1 || !object.contains_key("config") {
        return Err("config.import payload must contain only config".into());
    }
    let config = object
        .get("config")
        .cloned()
        .ok_or_else(|| "config.import config is missing".to_owned())?;
    if !config.is_object() {
        return Err("config.import config must be an object".into());
    }
    Ok(config)
}

#[cfg(windows)]
fn workflow_from_payload(payload: &Value) -> Result<(String, WorkflowTemplate), Box<dyn Error>> {
    let steps = payload
        .get("steps")
        .and_then(Value::as_array)
        .ok_or_else(|| "steps must be an array".to_owned())?
        .iter()
        .map(|step| {
            let order = step
                .get("order")
                .and_then(Value::as_u64)
                .ok_or_else(|| "workflow step order must be an integer".to_owned())?;
            Ok(WorkflowStep {
                id: step
                    .get("id")
                    .and_then(Value::as_str)
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| "workflow step id is required".to_owned())?
                    .to_owned(),
                order: u32::try_from(order)
                    .map_err(|_| "workflow step order is out of range".to_owned())?,
                agent_id: step
                    .get("agentId")
                    .and_then(Value::as_str)
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| "workflow step agentId is required".to_owned())?
                    .to_owned(),
                prompt_supplement: step
                    .get("promptSupplement")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            })
        })
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    Ok((
        required_string(payload, "projectId")?,
        WorkflowTemplate {
            id: required_string(payload, "workflowId")?,
            name: required_string(payload, "name")?,
            kind: required_string(payload, "kind")?,
            steps,
        },
    ))
}

#[cfg(windows)]
fn workflow_dispatch_from_payload(
    payload: &Value,
) -> Result<WorkflowDispatchCommand, Box<dyn Error>> {
    reject_unknown_fields(
        payload,
        &[
            "workflowId",
            "collaborationRunId",
            "parentExecutionRunId",
            "sourceMessageId",
            "task",
            "startRuntime",
        ],
    )?;
    let start_runtime = match payload.get("startRuntime") {
        None => true,
        Some(Value::Bool(value)) => *value,
        Some(_) => return Err("startRuntime must be a boolean".into()),
    };
    Ok(WorkflowDispatchCommand {
        workflow_id: required_string(payload, "workflowId")?,
        collaboration_run_id: required_string(payload, "collaborationRunId")?,
        parent_execution_run_id: required_string(payload, "parentExecutionRunId")?,
        source_message_id: required_string(payload, "sourceMessageId")?,
        task: required_string(payload, "task")?,
        start_runtime,
    })
}

#[cfg(windows)]
type AgentModelBindingPayload = (String, Option<String>, Option<String>, u64);

#[cfg(windows)]
fn agent_model_binding_from_payload(
    payload: &Value,
) -> Result<AgentModelBindingPayload, Box<dyn Error>> {
    reject_unknown_fields(
        payload,
        &[
            "agentId",
            "connectorId",
            "modelId",
            "candidateModelListRevision",
        ],
    )?;
    let agent_id = required_string(payload, "agentId")?;
    let connector_id = optional_string(payload, "connectorId")?;
    let model_id = optional_string(payload, "modelId")?;
    if connector_id.is_none() && model_id.is_some() {
        return Err("modelId requires connectorId".into());
    }
    Ok((
        agent_id,
        connector_id,
        model_id,
        optional_u64(payload, "candidateModelListRevision", 0)?,
    ))
}

#[cfg(windows)]
fn agent_model_binding_patch_from_payload(
    payload: &Value,
) -> Result<(String, AgentModelBindingPatch), Box<dyn Error>> {
    reject_unknown_fields(
        payload,
        &[
            "agentId",
            "connectorId",
            "modelId",
            "candidateModelListRevision",
        ],
    )?;
    Ok((
        required_string(payload, "agentId")?,
        AgentModelBindingPatch {
            connector_id: binding_string_patch(payload, "connectorId")?,
            model_id: binding_string_patch(payload, "modelId")?,
            candidate_model_list_revision: binding_revision_patch(
                payload,
                "candidateModelListRevision",
            )?,
        },
    ))
}

#[cfg(windows)]
fn binding_string_patch(
    payload: &Value,
    key: &str,
) -> Result<BindingFieldPatch<String>, Box<dyn Error>> {
    match payload.get(key) {
        None => Ok(BindingFieldPatch::Preserve),
        Some(Value::Null) => Ok(BindingFieldPatch::Clear),
        Some(Value::String(value)) if !value.trim().is_empty() => {
            Ok(BindingFieldPatch::Set(value.clone()))
        }
        Some(_) => Err(format!("field {key} must be a non-empty string or null").into()),
    }
}

#[cfg(windows)]
fn binding_revision_patch(
    payload: &Value,
    key: &str,
) -> Result<BindingFieldPatch<u64>, Box<dyn Error>> {
    match payload.get(key) {
        None => Ok(BindingFieldPatch::Preserve),
        Some(Value::Null) => Ok(BindingFieldPatch::Clear),
        Some(value) => value
            .as_u64()
            .map(BindingFieldPatch::Set)
            .ok_or_else(|| format!("field {key} must be a non-negative integer or null").into()),
    }
}

#[cfg(windows)]
fn has_model_selection_fields(payload: &Value) -> bool {
    [
        "modelSelectionMode",
        "modelId",
        "candidateModelListMode",
        "candidateModelListRevision",
    ]
    .iter()
    .any(|field| payload.get(field).is_some())
}

#[cfg(windows)]
fn model_selection_from_payload(
    payload: &Value,
) -> Result<(ModelSelection, IdentityModelListMode, u64), Box<dyn Error>> {
    let mode = match payload
        .get("modelSelectionMode")
        .and_then(Value::as_str)
        .unwrap_or("inherit")
    {
        "inherit" => ModelSelectionMode::Inherit,
        "connector_default" => ModelSelectionMode::ConnectorDefault,
        "pinned" => ModelSelectionMode::Pinned,
        _ => return Err("modelSelectionMode is invalid".into()),
    };
    let model_id = optional_string(payload, "modelId")?;
    if (mode == ModelSelectionMode::Pinned) != model_id.is_some() {
        return Err("pinned requires modelId; inherit and connector_default forbid modelId".into());
    }
    let list_mode = match payload
        .get("candidateModelListMode")
        .and_then(Value::as_str)
        .unwrap_or("inherit")
    {
        "inherit" => IdentityModelListMode::Inherit,
        "override" => IdentityModelListMode::Override,
        _ => return Err("assignment candidateModelListMode must be inherit or override".into()),
    };
    Ok((
        ModelSelection { mode, model_id },
        list_mode,
        optional_u64(payload, "candidateModelListRevision", 0)?,
    ))
}

#[cfg(windows)]
fn identity_model_scope(value: &str) -> Result<IdentityModelListScope, Box<dyn Error>> {
    match value {
        "base_agent" => Ok(IdentityModelListScope::BaseAgent),
        "project_agent" => Ok(IdentityModelListScope::ProjectAgent),
        "conversation_agent" => Ok(IdentityModelListScope::ConversationAgent),
        _ => Err("identityScope is invalid".into()),
    }
}

#[cfg(windows)]
fn identity_model_target_fields(
    payload: &Value,
) -> Result<IdentityModelListTarget, Box<dyn Error>> {
    let scope = identity_model_scope(&required_string(payload, "identityScope")?)?;
    let project_id = optional_string(payload, "projectId")?;
    let conversation_id = optional_string(payload, "conversationId")?;
    let valid = match scope {
        IdentityModelListScope::BaseAgent => project_id.is_none() && conversation_id.is_none(),
        IdentityModelListScope::ProjectAgent => project_id.is_some() && conversation_id.is_none(),
        IdentityModelListScope::ConversationAgent => {
            project_id.is_none() && conversation_id.is_some()
        }
    };
    if !valid {
        return Err("identityScope does not match projectId/conversationId".into());
    }
    Ok(IdentityModelListTarget {
        scope,
        agent_id: required_string(payload, "agentId")?,
        project_id,
        conversation_id,
    })
}

#[cfg(windows)]
fn identity_model_target_from_payload(
    payload: &Value,
) -> Result<(IdentityModelListTarget, Option<String>), Box<dyn Error>> {
    reject_unknown_fields(
        payload,
        &[
            "identityScope",
            "agentId",
            "projectId",
            "conversationId",
            "connectorId",
        ],
    )?;
    Ok((
        identity_model_target_fields(payload)?,
        optional_string(payload, "connectorId")?,
    ))
}

#[cfg(windows)]
fn identity_model_default_from_payload(
    payload: &Value,
) -> Result<(IdentityModelListTarget, String, String), Box<dyn Error>> {
    reject_unknown_fields(
        payload,
        &[
            "identityScope",
            "agentId",
            "projectId",
            "conversationId",
            "connectorId",
            "modelId",
        ],
    )?;
    Ok((
        identity_model_target_fields(payload)?,
        required_string(payload, "connectorId")?,
        required_string(payload, "modelId")?,
    ))
}

#[cfg(windows)]
fn identity_model_option_from_payload(
    payload: &Value,
) -> Result<IdentityModelOption, Box<dyn Error>> {
    reject_unknown_fields(
        payload,
        &[
            "id",
            "identityScope",
            "agentId",
            "projectId",
            "conversationId",
            "modelId",
            "displayName",
            "connectorId",
            "source",
            "availability",
            "isDefault",
            "sortOrder",
            "catalogRevision",
            "contextWindow",
            "reasoningEfforts",
            "serviceTiers",
        ],
    )?;
    let target = identity_model_target_fields(payload)?;
    let source = match required_string(payload, "source")?.as_str() {
        "runtime" => ModelOptionSource::Runtime,
        "config" => ModelOptionSource::Config,
        "manual" => ModelOptionSource::Manual,
        _ => return Err("source is invalid".into()),
    };
    let availability = match required_string(payload, "availability")?.as_str() {
        "available" => ModelAvailability::Available,
        "unverified" => ModelAvailability::Unverified,
        "unavailable" => ModelAvailability::Unavailable,
        _ => return Err("availability is invalid".into()),
    };
    Ok(IdentityModelOption {
        id: required_string(payload, "id")?,
        scope: target.scope,
        agent_id: target.agent_id,
        project_id: target.project_id,
        conversation_id: target.conversation_id,
        model_id: required_string(payload, "modelId")?,
        display_name: required_string(payload, "displayName")?,
        connector_id: required_string(payload, "connectorId")?,
        source,
        availability,
        is_default: required_bool(payload, "isDefault")?,
        sort_order: optional_u64(payload, "sortOrder", 0)?,
        catalog_revision: optional_string(payload, "catalogRevision")?,
        context_window: optional_nullable_u64(payload, "contextWindow")?,
        reasoning_efforts: optional_string_array(payload, "reasoningEfforts", 32)?,
        service_tiers: optional_string_array(payload, "serviceTiers", 32)?,
    })
}

#[cfg(windows)]
fn optional_u64(payload: &Value, key: &str, default: u64) -> Result<u64, Box<dyn Error>> {
    match payload.get(key) {
        None => Ok(default),
        Some(value) => value
            .as_u64()
            .ok_or_else(|| format!("field {key} must be a non-negative integer").into()),
    }
}

#[cfg(windows)]
fn optional_nullable_u64(payload: &Value, key: &str) -> Result<Option<u64>, Box<dyn Error>> {
    match payload.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value
            .as_u64()
            .map(Some)
            .ok_or_else(|| format!("field {key} must be a non-negative integer or null").into()),
    }
}

#[cfg(windows)]
fn optional_string_array(
    payload: &Value,
    key: &str,
    max_items: usize,
) -> Result<Vec<String>, Box<dyn Error>> {
    let Some(value) = payload.get(key) else {
        return Ok(Vec::new());
    };
    let values = value
        .as_array()
        .ok_or_else(|| format!("field {key} must be an array"))?;
    if values.len() > max_items {
        return Err(format!("field {key} exceeds {max_items} items").into());
    }
    values
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .ok_or_else(|| format!("field {key} must contain non-empty strings").into())
        })
        .collect()
}

#[cfg(windows)]
type ExecutionStartPayload = (
    ExecutionStart,
    Option<String>,
    Option<String>,
    Option<String>,
);

#[cfg(windows)]
type ExecutionRetryPayload = (String, String, String, Option<String>, Option<String>);

#[cfg(windows)]
fn execution_start_from_payload(payload: &Value) -> Result<ExecutionStartPayload, Box<dyn Error>> {
    reject_unknown_fields(
        payload,
        &[
            "executionRunId",
            "collaborationRunId",
            "projectId",
            "conversationId",
            "agentId",
            "workspaceAccess",
            "canonicalCwd",
            "currentTask",
            "connectorId",
            "modelId",
        ],
    )?;
    Ok((
        ExecutionStart {
            run_id: required_string(payload, "executionRunId")?,
            collaboration_run_id: required_string(payload, "collaborationRunId")?,
            project_id: required_string(payload, "projectId")?,
            conversation_id: required_string(payload, "conversationId")?,
            agent_id: required_string(payload, "agentId")?,
            workspace_access: parse_access(
                payload
                    .get("workspaceAccess")
                    .and_then(Value::as_str)
                    .unwrap_or("none"),
            )?,
            canonical_cwd: payload
                .get("canonicalCwd")
                .and_then(Value::as_str)
                .map(str::to_owned),
        },
        optional_string(payload, "currentTask")?,
        optional_string(payload, "connectorId")?,
        optional_string(payload, "modelId")?,
    ))
}

#[cfg(windows)]
fn execution_retry_from_payload(payload: &Value) -> Result<ExecutionRetryPayload, Box<dyn Error>> {
    reject_unknown_fields(
        payload,
        &[
            "executionRunId",
            "sourceExecutionRunId",
            "currentTask",
            "connectorId",
            "modelId",
        ],
    )?;
    Ok((
        required_string(payload, "executionRunId")?,
        required_string(payload, "sourceExecutionRunId")?,
        required_string(payload, "currentTask")?,
        optional_string(payload, "connectorId")?,
        optional_string(payload, "modelId")?,
    ))
}

#[cfg(windows)]
fn parse_access(value: &str) -> Result<WorkspaceAccess, Box<dyn Error>> {
    match value {
        "none" => Ok(WorkspaceAccess::None),
        "read_only" => Ok(WorkspaceAccess::ReadOnly),
        "workspace_write" => Ok(WorkspaceAccess::WorkspaceWrite),
        _ => Err(format!("invalid workspace access {value}").into()),
    }
}

#[cfg(windows)]
fn validate_handshake(
    handshake: &ProtocolHandshake,
    expected_session_credential: &str,
    server_epoch: &str,
) -> Result<SessionBinding, &'static str> {
    validate_protocol(&handshake.protocol).map_err(|_| "INVALID_HANDSHAKE")?;
    if handshake.kind != "handshake"
        || handshake.client_id.is_empty()
        || handshake.client_id.len() > 128
        || !(16..=128).contains(&handshake.session_id.len())
        || !(32..=256).contains(&handshake.session_credential.len())
        || !(1024..=16 * 1024 * 1024).contains(&handshake.max_message_bytes)
    {
        return Err("INVALID_HANDSHAKE");
    }
    if !constant_time_equal(
        handshake.session_credential.as_bytes(),
        expected_session_credential.as_bytes(),
    ) {
        return Err("INVALID_HANDSHAKE");
    }
    if let Some(last_seen) = &handshake.last_seen {
        if last_seen.stream_id != "core-events" || last_seen.epoch.as_deref() != Some(server_epoch)
        {
            return Err("CURSOR_EPOCH_MISMATCH");
        }
    }
    Ok(SessionBinding {
        client_id: handshake.client_id.clone(),
        session_id: handshake.session_id.clone(),
    })
}

#[cfg(all(test, windows))]
mod tests {
    use super::*;
    use agenttalk_protocols::StreamCursor;

    /// Strict structural RFC3339 UTC validator: YYYY-MM-DDTHH:MM:SS.mmmZ with
    /// valid field ranges and exactly three fractional digits.
    fn is_strict_rfc3339_utc(value: &str) -> bool {
        let Some(rest) = value.strip_suffix('Z') else {
            return false;
        };
        let Some((date, time)) = rest.split_once('T') else {
            return false;
        };
        let mut date_parts = date.split('-');
        let (Some(year_s), Some(month_s), Some(day_s)) =
            (date_parts.next(), date_parts.next(), date_parts.next())
        else {
            return false;
        };
        if date_parts.next().is_some() {
            return false;
        }
        // RFC3339 full-date requires exactly four digit years.
        if year_s.len() != 4 || !year_s.chars().all(|c| c.is_ascii_digit()) {
            return false;
        }
        let (Ok(year), Ok(month), Ok(day)) = (
            year_s.parse::<i32>(),
            month_s.parse::<u32>(),
            day_s.parse::<u32>(),
        ) else {
            return false;
        };
        if year < 0 || !(1..=12).contains(&month) || !(1..=31).contains(&day) {
            return false;
        }
        let days_in_month = match month {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            2 => {
                if (year % 400 == 0) || (year % 4 == 0 && year % 100 != 0) {
                    29
                } else {
                    28
                }
            }
            _ => return false,
        };
        if day > days_in_month {
            return false;
        }
        let mut time_parts = time.splitn(2, '.');
        let (Some(hms), Some(fraction)) = (time_parts.next(), time_parts.next()) else {
            return false;
        };
        if time_parts.next().is_some() {
            return false;
        }
        let mut hms_parts = hms.split(':');
        let (Some(hh), Some(mm), Some(ss)) = (hms_parts.next(), hms_parts.next(), hms_parts.next())
        else {
            return false;
        };
        if hms_parts.next().is_some() {
            return false;
        }
        let (Ok(hh), Ok(mm), Ok(ss)) = (hh.parse::<u32>(), mm.parse::<u32>(), ss.parse::<u32>())
        else {
            return false;
        };
        if hh > 23 || mm > 59 || ss > 59 {
            return false;
        }
        fraction.len() == 3 && fraction.chars().all(|c| c.is_ascii_digit())
    }

    #[test]
    fn occurred_at_emits_schema_valid_utc_rfc3339_at_fixed_boundaries() {
        assert_eq!(occurred_at(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(occurred_at(1), "1970-01-01T00:00:00.001Z");
        assert_eq!(occurred_at(999), "1970-01-01T00:00:00.999Z");
        assert_eq!(occurred_at(1000), "1970-01-01T00:00:01.000Z");
        assert_eq!(occurred_at(59_999), "1970-01-01T00:00:59.999Z");
        assert_eq!(occurred_at(60_000), "1970-01-01T00:01:00.000Z");
        assert_eq!(occurred_at(3_599_999), "1970-01-01T00:59:59.999Z");
        assert_eq!(occurred_at(3_600_000), "1970-01-01T01:00:00.000Z");
        assert_eq!(occurred_at(86_399_999), "1970-01-01T23:59:59.999Z");
        assert_eq!(occurred_at(86_400_000), "1970-01-02T00:00:00.000Z");
        assert_eq!(occurred_at(86_400_123), "1970-01-02T00:00:00.123Z");
        assert_eq!(occurred_at(1_700_000_000_123), "2023-11-14T22:13:20.123Z");
        assert_eq!(occurred_at(-5), "1970-01-01T00:00:00.000Z");
        // The maximum schema-valid instant and anything beyond it clamp to the
        // four-digit-year upper bound; extended years are never emitted.
        assert_eq!(occurred_at(253_402_300_799_999), "9999-12-31T23:59:59.999Z");
        assert_eq!(occurred_at(253_402_300_800_000), "9999-12-31T23:59:59.999Z");
        assert_eq!(occurred_at(i64::MAX), "9999-12-31T23:59:59.999Z");
        assert_eq!(occurred_at(i64::MAX / 1000), "9999-12-31T23:59:59.999Z");
    }

    #[test]
    fn occurred_at_outputs_are_strict_rfc3339_parseable() {
        for ms in [
            0i64,
            1,
            999,
            1000,
            59_999,
            60_000,
            3_599_999,
            3_600_000,
            86_399_999,
            86_400_000,
            86_400_123,
            1_700_000_000_123,
            2_534_021_856_000,
            253_402_300_799_999,
            253_402_300_800_000,
            i64::MAX,
            i64::MAX / 1000,
            i64::MIN,
        ] {
            let text = occurred_at(ms);
            assert!(
                is_strict_rfc3339_utc(&text),
                "occurred_at({ms}) produced non-RFC3339 output: {text}"
            );
        }
    }

    #[test]
    fn strict_validator_rejects_extended_years_and_impossible_dates() {
        for value in [
            "292278-01-01T00:00:00.000Z",
            "10000-01-01T00:00:00.000Z",
            "2023-02-29T00:00:00.000Z",
            "2024-02-30T00:00:00.000Z",
            "2023-04-31T00:00:00.000Z",
            "2023-06-31T00:00:00.000Z",
            "2023-11-31T00:00:00.000Z",
            "2021-02-29T00:00:00.000Z",
            "1900-02-29T00:00:00.000Z",
            "2023-00-15T00:00:00.000Z",
            "2023-13-01T00:00:00.000Z",
        ] {
            assert!(
                !is_strict_rfc3339_utc(value),
                "strict validator must reject {value}"
            );
        }
        for value in [
            "2024-02-29T00:00:00.000Z",
            "2000-02-29T00:00:00.000Z",
            "2023-04-30T00:00:00.000Z",
            "2023-12-31T23:59:59.999Z",
            "9999-12-31T23:59:59.999Z",
            "1970-01-01T00:00:00.000Z",
        ] {
            assert!(
                is_strict_rfc3339_utc(value),
                "strict validator must accept {value}"
            );
        }
    }

    fn handshake(last_seen: Option<StreamCursor>) -> ProtocolHandshake {
        ProtocolHandshake {
            kind: "handshake".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            client_id: "flutter-test".into(),
            session_id: "session-handshake-test".into(),
            session_credential: "credential-handshake-test-1234567890".into(),
            max_message_bytes: 1024 * 1024,
            last_seen,
        }
    }

    fn test_core_host_with_discovery_stream_limits(
        max_owners: usize,
        retention: Duration,
    ) -> Arc<CoreHost> {
        let registry = RuntimeRegistry::from_adapters(vec![Box::new(
            agenttalk_runtime_host::MockRuntime::default(),
        )])
        .expect("test runtime registry");
        let core = PersistentCore::open_with_runtime_registry(":memory:", registry)
            .expect("open in-memory test Core");
        let host = Arc::new(
            CoreHost::new(core, LocalDiscoveryService::from_environment())
                .expect("create test CoreHost"),
        );
        {
            let mut streams = host
                .discovery_events
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            streams.max_owners = max_owners;
            streams.retention = retention;
        }
        host
    }

    fn discovery_test_event(event_type: &str, scan_id: &str) -> RuntimeEvent {
        RuntimeEvent {
            event_id: format!("{event_type}-{scan_id}"),
            execution_run_id: scan_id.into(),
            runtime_id: "local-discovery".into(),
            thread_id: None,
            turn_id: None,
            sequence: 0,
            event_type: event_type.into(),
            timestamp_ms: 1,
            payload: json!({"scanId": scan_id}),
        }
    }

    fn recv_thread_result<T: Send + 'static>(
        receiver: mpsc::Receiver<T>,
        timeout: Duration,
        label: &str,
    ) -> T {
        receiver
            .recv_timeout(timeout)
            .unwrap_or_else(|_| panic!("{label} did not finish within {timeout:?}"))
    }

    fn wait_for_discovery_running_scans_to_drain(host: &CoreHost, owner: &DiscoveryOwnerScope) {
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            if host.discovery.counts_for_tests(owner).owner_running_scans == 0 {
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
        panic!(
            "discovery running scans did not drain: {:?}",
            host.discovery.counts_for_tests(owner)
        );
    }

    #[test]
    fn default_production_registry_keeps_legacy_runtime_fail_closed_and_registers_builtins() {
        let registry = runtime_registry_from_configuration("", false)
            .expect("empty production configuration should register inert built-ins");
        assert_eq!(registry.default_runtime_id(), "unconfigured");
        assert!(registry.has_runtime_type("unconfigured"));
        assert!(registry.has_runtime_type("codex"));
        assert!(registry.has_runtime_type("kun"));

        // Opening Core and reading the old unscoped projection must not probe
        // either transport or silently choose one of the connector adapters.
        let core = PersistentCore::open_with_runtime_registry(":memory:", registry)
            .expect("inert production registry should open Core");
        let models = core.runtime_models();
        assert_eq!(models["runtimeId"], "unconfigured");
        assert_eq!(models["availability"], "unavailable");
        assert_eq!(models["models"], json!([]));
    }

    #[test]
    fn local_discovery_queries_are_additive_schema_entries_with_empty_payloads() {
        assert!(local_discovery_query_payload(&json!({})).is_ok());
        assert!(local_discovery_query_payload(&json!({"unexpected": true})).is_err());
        let schema: Value =
            serde_json::from_str(include_str!("../../../schemas/ipc/v1/protocol.schema.json"))
                .expect("parse IPC v1 schema");
        let queries = schema["$defs"]["QueryEnvelope"]["properties"]["query"]["enum"]
            .as_array()
            .expect("query enum");
        for query in [
            "connector.discover",
            "agent.scan_local",
            "orchestration.run.snapshot",
            "orchestration.run.recovery_state",
        ] {
            assert!(
                queries.iter().any(|value| value == query),
                "schema must register {query} as an additive query"
            );
        }
        let entry = &schema["$defs"]["LocalDiscoveryEntry"];
        assert_eq!(entry["additionalProperties"], false);
        assert_eq!(
            entry["required"],
            json!([
                "connectorId",
                "runtimeType",
                "displayName",
                "availability",
                "models",
                "catalogRevision",
                "source",
                "requiresConfiguration",
            ])
        );
    }

    #[test]
    fn published_event_types_are_present_in_ipc_schema_event_enum() {
        let schema: Value =
            serde_json::from_str(include_str!("../../../schemas/ipc/v1/protocol.schema.json"))
                .expect("parse IPC v1 schema");
        let events = schema["$defs"]["EventEnvelope"]["properties"]["event"]["enum"]
            .as_array()
            .expect("event enum");
        // Every event type the Core publishes must be registered so strict
        // schema-validating clients can accept the envelope.
        for event in [
            "agent.discovery.started",
            "agent.discovery.candidate_observed",
            "agent.discovery.candidate_classified",
            "agent.discovery.candidate_verified",
            "agent.discovery.completed",
            "agent.discovery.failed",
            "local_agent.imported",
        ] {
            assert!(
                events.iter().any(|value| value == event),
                "schema event enum must register {event}"
            );
        }
    }

    #[test]
    fn explicit_runtime_configuration_remains_an_exact_override() {
        let registry = runtime_registry_from_configuration("unconfigured", false)
            .expect("explicit legacy override should be accepted");
        assert_eq!(registry.default_runtime_id(), "unconfigured");
        assert!(registry.has_runtime_type("unconfigured"));
        assert!(!registry.has_runtime_type("codex"));
        assert!(!registry.has_runtime_type("kun"));

        let fixture_error = runtime_registry_from_configuration("fixture-dual", false)
            .err()
            .expect("fixture-dual must remain development-only");
        assert!(fixture_error
            .to_string()
            .contains("fixture-dual requires AGENTTALK_CORE_DEV_MODE=1"));
    }

    #[test]
    fn connector_transport_failures_keep_safe_distinct_ipc_categories() {
        for (runtime_error, expected_code, expected_category) in [
            (
                RuntimeError::Transport("kun_shared_runtime_unavailable".into()),
                "CONNECTOR_SHARED_RUNTIME_UNAVAILABLE",
                "shared_runtime_unavailable",
            ),
            (
                RuntimeError::Authentication,
                "CONNECTOR_RUNTIME_AUTHENTICATION_FAILED",
                "runtime_authentication_failed",
            ),
            (
                RuntimeError::Protocol("kun_runtime_identity_mismatch".into()),
                "CONNECTOR_RUNTIME_IDENTITY_MISMATCH",
                "runtime_identity_mismatch",
            ),
            (
                RuntimeError::Transport("kun_catalog_unavailable".into()),
                "CONNECTOR_CATALOG_UNAVAILABLE",
                "connector_catalog_unavailable",
            ),
            (
                RuntimeError::Provider("kun_provider_authentication_failed".into()),
                "CONNECTOR_PROVIDER_AUTHENTICATION_FAILED",
                "provider_authentication_failed",
            ),
        ] {
            let (code, message, category) =
                core_error_details("QUERY_REJECTED", &CoreError::Runtime(runtime_error));
            assert_eq!(code, expected_code);
            assert_eq!(category, expected_category);
            assert!(!message.contains("fixture"));
            assert!(!message.contains("token"));
        }

        let (code, _, category) =
            core_error_details("QUERY_REJECTED", &CoreError::ConnectorCatalogUnavailable);
        assert_eq!(code, "CONNECTOR_CATALOG_UNAVAILABLE");
        assert_eq!(category, "connector_catalog_unavailable");
    }

    #[test]
    fn discovery_stream_replay_without_started_owner_does_not_create_stream() {
        let host = test_core_host_with_discovery_stream_limits(2, Duration::from_secs(60));
        let owner = DiscoveryOwnerScope::from_authenticated_session(
            "w52-random-client",
            "session-w52-random-owner-000001",
        );
        assert_eq!(host.discovery_stream_count_for_tests(), 0);
        assert!(matches!(
            host.event_stream(EventStreamKind::Discovery, &owner),
            Err(DiscoveryStreamError::NotFound)
        ));
        assert_eq!(host.discovery_stream_count_for_tests(), 0);
    }

    #[test]
    fn discovery_stream_owner_map_is_bounded_and_prunes_idle_expired_streams() {
        let host = test_core_host_with_discovery_stream_limits(2, Duration::from_millis(10));
        let owner_a = DiscoveryOwnerScope::from_authenticated_session(
            "w52-owner-a",
            "session-w52-owner-a-000001",
        );
        let owner_b = DiscoveryOwnerScope::from_authenticated_session(
            "w52-owner-b",
            "session-w52-owner-b-000001",
        );
        let owner_c = DiscoveryOwnerScope::from_authenticated_session(
            "w52-owner-c",
            "session-w52-owner-c-000001",
        );

        let _ = host
            .create_discovery_event_stream_for_owner(&owner_a)
            .unwrap();
        let _ = host
            .create_discovery_event_stream_for_owner(&owner_b)
            .unwrap();
        assert_eq!(host.discovery_stream_count_for_tests(), 2);
        assert!(matches!(
            host.create_discovery_event_stream_for_owner(&owner_c),
            Err(DiscoveryStreamError::CapacityExhausted)
        ));
        assert_eq!(host.discovery_stream_count_for_tests(), 2);

        host.advance_discovery_stream_clock_for_tests(Duration::from_millis(11));
        let (_, epoch_c) = host
            .create_discovery_event_stream_for_owner(&owner_c)
            .expect("expired idle owner should be pruned for a new owner");
        assert_eq!(host.discovery_stream_count_for_tests(), 1);
        assert!(host.discovery_stream_epoch_for_tests(&owner_c).is_some());
        assert!(host.discovery_stream_epoch_for_tests(&owner_a).is_none());
        assert!(!epoch_c.is_empty());
    }

    #[test]
    fn discovery_stream_capacity_does_not_evict_active_session_or_subscription() {
        let host = test_core_host_with_discovery_stream_limits(2, Duration::from_millis(10));
        let active_session_owner = DiscoveryOwnerScope::from_authenticated_session(
            "w52-active-session",
            "session-w52-active-session",
        );
        let active_subscription_owner = DiscoveryOwnerScope::from_authenticated_session(
            "w52-active-subscription",
            "session-w52-active-subscription",
        );
        let flood_owner = DiscoveryOwnerScope::from_authenticated_session(
            "w52-flood-owner",
            "session-w52-flood-owner",
        );
        let _ = host
            .create_discovery_event_stream_for_owner(&active_session_owner)
            .unwrap();
        let subscription = host
            .begin_discovery_subscription_for_tests(&active_subscription_owner)
            .expect("create test subscription lease");
        host.discovery
            .mark_owner_active_for_tests(&active_session_owner);
        host.advance_discovery_stream_clock_for_tests(Duration::from_millis(11));

        assert!(matches!(
            host.create_discovery_event_stream_for_owner(&flood_owner),
            Err(DiscoveryStreamError::CapacityExhausted)
        ));
        assert_eq!(host.discovery_stream_count_for_tests(), 2);
        assert!(host
            .discovery_stream_epoch_for_tests(&active_session_owner)
            .is_some());
        assert!(host
            .discovery_stream_epoch_for_tests(&active_subscription_owner)
            .is_some());

        drop(subscription);
        host.discovery
            .clear_owner_activity_for_tests(&active_session_owner);
        let _ = host
            .create_discovery_event_stream_for_owner(&flood_owner)
            .expect("idle expired owners can be pruned once work and subscriptions are gone");
        assert_eq!(host.discovery_stream_count_for_tests(), 1);
    }

    #[test]
    fn discovery_stream_reconnect_retention_and_expiry_rotate_epoch() {
        let host = test_core_host_with_discovery_stream_limits(4, Duration::from_millis(10));
        let owner = DiscoveryOwnerScope::from_authenticated_session(
            "w52-reconnect-owner",
            "session-w52-reconnect-owner",
        );

        let (_, first_epoch) = host
            .create_discovery_event_stream_for_owner(&owner)
            .expect("create owner stream");
        host.publish_discovery_event(
            &owner,
            discovery_test_event("agent.discovery.started", "scan-w52-reconnect"),
        );
        host.advance_discovery_stream_clock_for_tests(Duration::from_millis(5));
        let (_, retained_epoch) = host
            .event_stream(EventStreamKind::Discovery, &owner)
            .expect("same owner should reconnect inside retention");
        assert_eq!(retained_epoch, first_epoch);

        host.advance_discovery_stream_clock_for_tests(Duration::from_millis(11));
        host.prune_discovery_streams_for_tests();
        assert_eq!(host.discovery_stream_count_for_tests(), 0);
        assert!(matches!(
            host.event_stream(EventStreamKind::Discovery, &owner),
            Err(DiscoveryStreamError::NotFound)
        ));
        let (_, second_epoch) = host
            .create_discovery_event_stream_for_owner(&owner)
            .expect("new scan after expiry creates a fresh epoch");
        assert_ne!(second_epoch, first_epoch);
    }

    #[test]
    fn discovery_streams_are_cleared_on_core_shutdown() {
        let host = test_core_host_with_discovery_stream_limits(4, Duration::from_secs(60));
        let owner = DiscoveryOwnerScope::from_authenticated_session(
            "w52-shutdown-owner",
            "session-w52-shutdown-owner",
        );
        let _ = host
            .create_discovery_event_stream_for_owner(&owner)
            .expect("create owner stream");
        assert_eq!(host.discovery_stream_count_for_tests(), 1);
        host.cancel_discovery_sessions();
        assert_eq!(host.discovery_stream_count_for_tests(), 0);
        assert!(matches!(
            host.event_stream(EventStreamKind::Discovery, &owner),
            Err(DiscoveryStreamError::NotFound)
        ));
    }

    #[test]
    fn failed_start_rolls_back_only_new_empty_discovery_stream() {
        let host = test_core_host_with_discovery_stream_limits(2, Duration::from_secs(60));
        let owner = DiscoveryOwnerScope::from_authenticated_session(
            "w54-new-stream-owner",
            "session-w54-new-stream-owner",
        );
        let reservation = host
            .reserve_discovery_event_stream_for_owner(&owner)
            .expect("reserve new stream");
        assert_eq!(host.discovery_stream_count_for_tests(), 1);
        drop(reservation);
        assert_eq!(
            host.discovery_stream_count_for_tests(),
            0,
            "uncommitted failed start must remove the newly created empty stream"
        );

        let existing_owner = DiscoveryOwnerScope::from_authenticated_session(
            "w54-existing-stream-owner",
            "session-w54-existing-stream-owner",
        );
        let _ = host
            .create_discovery_event_stream_for_owner(&existing_owner)
            .expect("existing stream");
        let reservation = host
            .reserve_discovery_event_stream_for_owner(&existing_owner)
            .expect("reserve existing stream");
        drop(reservation);
        assert_eq!(
            host.discovery_stream_count_for_tests(),
            1,
            "rollback must not delete a pre-existing owner stream"
        );

        let event_owner = DiscoveryOwnerScope::from_authenticated_session(
            "w54-event-stream-owner",
            "session-w54-event-stream-owner",
        );
        let reservation = host
            .reserve_discovery_event_stream_for_owner(&event_owner)
            .expect("reserve new stream with event");
        host.publish_discovery_event(
            &event_owner,
            discovery_test_event("agent.discovery.started", "scan-w54-event"),
        );
        drop(reservation);
        assert_eq!(
            host.discovery_stream_event_count_for_tests(&event_owner),
            Some(1),
            "rollback must not delete a stream that already contains events"
        );
    }

    #[test]
    fn failed_start_stream_rollback_survives_local_discovery_state_lock_contention() {
        let host = test_core_host_with_discovery_stream_limits(2, Duration::from_secs(60));
        let owner = DiscoveryOwnerScope::from_authenticated_session(
            "w55-lock-contention-owner",
            "session-w55-lock-contention-owner",
        );
        let reservation = host
            .reserve_discovery_event_stream_for_owner(&owner)
            .expect("reserve new stream");
        assert_eq!(host.discovery_stream_count_for_tests(), 1);

        host.discovery.with_state_lock_held_for_tests(|| {
            drop(reservation);
        });

        assert_eq!(
            host.discovery_stream_count_for_tests(),
            0,
            "failed stream rollback must clean deterministically after LocalDiscoveryState lock contention"
        );
    }

    #[test]
    fn start_publication_rejects_worker_ready_after_shutdown_without_epoch_or_event() {
        let host = test_core_host_with_discovery_stream_limits(2, Duration::from_secs(60));
        let owner = DiscoveryOwnerScope::from_authenticated_session(
            "w55-shutdown-wins-owner",
            "session-w55-shutdown-wins-owner",
        );
        let reservation = match host
            .discovery
            .begin_start(&owner, "request", &json!({}))
            .expect("reserve local discovery start")
        {
            StartScanOutcome::Reserved(reservation) => reservation,
            StartScanOutcome::Replayed(_) => panic!("new request must reserve"),
        };
        let stream_reservation = host
            .reserve_discovery_event_stream_for_owner(&owner)
            .expect("reserve discovery event stream");
        let worker_ready = reservation
            .launch_worker_until_ready(stream_reservation.event_sink())
            .expect("worker reaches ready state before publication");
        assert_eq!(host.discovery_stream_count_for_tests(), 1);

        host.cancel_discovery_sessions();
        let (writer, reader) = mpsc::channel();
        let result = host.publish_discovery_start_response(
            &writer,
            "request",
            &owner,
            worker_ready,
            stream_reservation,
        );

        assert!(matches!(
            result,
            Err(LocalDiscoveryRouteError::Service(
                LocalDiscoveryServiceError::ShuttingDown
            ))
        ));
        assert!(
            reader.try_recv().is_err(),
            "shutdown winner must not write accepted start IPC response"
        );
        assert_eq!(host.discovery_stream_count_for_tests(), 0);
        assert_eq!(host.discovery.counts_for_tests(&owner).owner_sessions, 0);
        assert_eq!(host.discovery.counts_for_tests(&owner).owner_requests, 0);
        assert_eq!(
            host.discovery.counts_for_tests(&owner).owner_running_scans,
            0
        );
    }

    #[test]
    fn start_publication_winner_holds_running_lease_before_shutdown_and_cancels_cleanly() {
        let host = test_core_host_with_discovery_stream_limits(2, Duration::from_secs(60));
        let owner = DiscoveryOwnerScope::from_authenticated_session(
            "w55-start-wins-owner",
            "session-w55-start-wins-owner",
        );
        let after_lease_hook = Arc::new(local_discovery::WorkerPauseHook::new());
        host.discovery
            .set_worker_after_lease_hook_for_tests(Arc::clone(&after_lease_hook));
        let reservation = match host
            .discovery
            .begin_start(&owner, "request", &json!({}))
            .expect("reserve local discovery start")
        {
            StartScanOutcome::Reserved(reservation) => reservation,
            StartScanOutcome::Replayed(_) => panic!("new request must reserve"),
        };
        let stream_reservation = host
            .reserve_discovery_event_stream_for_owner(&owner)
            .expect("reserve discovery event stream");
        let worker_ready = reservation
            .launch_worker_until_ready(stream_reservation.event_sink())
            .expect("worker reaches ready state before publication");
        let (writer, reader) = mpsc::channel();

        host.publish_discovery_start_response(
            &writer,
            "request",
            &owner,
            worker_ready,
            stream_reservation,
        )
        .expect("start wins publication race");

        let response = reader
            .recv_timeout(Duration::from_secs(1))
            .expect("start response is written before shutdown can win");
        assert_eq!(response["kind"], "response");
        let scan_id = response["payload"]["scanId"]
            .as_str()
            .expect("scan id in response")
            .to_owned();
        assert!(response["payload"]["eventStream"]["epoch"]
            .as_str()
            .is_some_and(|epoch| !epoch.is_empty()));
        assert!(
            host.discovery_stream_event_count_for_tests(&owner)
                .is_some_and(|count| count >= 1),
            "started event must be retained on the live stream"
        );
        assert!(
            after_lease_hook.wait_until_entered(Duration::from_secs(1)),
            "start success must be preceded by a worker-held running lease"
        );
        assert_eq!(
            host.discovery.active_running_leases_for_tests(),
            1,
            "accepted start must retain a real running lease before shutdown"
        );
        assert_eq!(
            host.discovery.scan_workloads_started_for_tests(),
            0,
            "the test hook pauses before the first scan workload"
        );

        host.cancel_discovery_sessions();
        assert_eq!(host.discovery_stream_count_for_tests(), 0);
        after_lease_hook.release();
        let deadline = Instant::now() + Duration::from_secs(1);
        while Instant::now() < deadline && host.discovery.active_running_leases_for_tests() != 0 {
            thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(
            host.discovery.active_running_leases_for_tests(),
            0,
            "shutdown must release the real worker-held running lease"
        );
        assert_eq!(
            host.discovery.scan_workloads_started_for_tests(),
            0,
            "shutdown must prevent a paused worker from beginning its first scan workload"
        );
        assert_eq!(
            host.discovery
                .snapshot(&owner, &scan_id)
                .expect("published session remains observable after shutdown")["state"],
            "cancelled"
        );
    }

    #[test]
    fn start_publication_waits_for_worker_running_lease_before_accepted() {
        let host = test_core_host_with_discovery_stream_limits(2, Duration::from_secs(60));
        let owner = DiscoveryOwnerScope::from_authenticated_session(
            "w56-worker-lease-owner",
            "session-w56-worker-lease-owner",
        );
        let hook = Arc::new(local_discovery::WorkerPauseHook::new());
        host.discovery
            .set_worker_before_lease_hook_for_tests(Arc::clone(&hook));
        let reservation = match host
            .discovery
            .begin_start(&owner, "request", &json!({}))
            .expect("reserve local discovery start")
        {
            StartScanOutcome::Reserved(reservation) => reservation,
            StartScanOutcome::Replayed(_) => panic!("new request must reserve"),
        };
        let stream_reservation = host
            .reserve_discovery_event_stream_for_owner(&owner)
            .expect("reserve discovery event stream");
        let worker_ready = reservation
            .launch_worker_until_ready(stream_reservation.event_sink())
            .expect("worker reaches ready state before publication");
        let (writer, reader) = mpsc::channel();
        let publish_host = Arc::clone(&host);
        let publish_owner = owner.clone();
        let (result_sender, result_receiver) = mpsc::channel();
        thread::spawn(move || {
            let result = publish_host.publish_discovery_start_response(
                &writer,
                "request",
                &publish_owner,
                worker_ready,
                stream_reservation,
            );
            result_sender
                .send(result)
                .expect("test result receiver must remain open");
        });

        assert!(
            hook.wait_until_entered(Duration::from_secs(1)),
            "worker must be mechanically paused after gate open and before running lease"
        );
        let response_before_worker_lease = reader.try_recv().ok();
        host.cancel_discovery_sessions();
        let result = recv_thread_result(
            result_receiver,
            Duration::from_secs(1),
            "start publication waiting for worker lease after shutdown",
        );
        hook.release();

        assert!(
            response_before_worker_lease.is_none(),
            "start must not write accepted before the worker has acquired its running lease"
        );
        assert!(matches!(
            result,
            Err(LocalDiscoveryRouteError::Service(
                LocalDiscoveryServiceError::ShuttingDown
            ))
        ));
        assert!(
            reader.try_recv().is_err(),
            "shutdown winner must not leave a delayed accepted response"
        );
        assert_eq!(host.discovery_stream_count_for_tests(), 0);
        let counts = host.discovery.counts_for_tests(&owner);
        assert_eq!(counts.owner_sessions, 0);
        assert_eq!(counts.owner_requests, 0);
        assert_eq!(counts.owner_running_scans, 0);
        assert_eq!(counts.owner_lease_waiters, 0);
    }

    #[test]
    fn start_replay_waits_for_worker_lease_and_final_publication() {
        let host = test_core_host_with_discovery_stream_limits(2, Duration::from_secs(60));
        let owner = DiscoveryOwnerScope::from_authenticated_session(
            "w56-replay-publication-owner",
            "session-w56-replay-publication-owner",
        );
        let reservation = match host
            .discovery
            .begin_start(&owner, "request", &json!({}))
            .expect("reserve local discovery start")
        {
            StartScanOutcome::Reserved(reservation) => reservation,
            StartScanOutcome::Replayed(_) => panic!("new request must reserve"),
        };
        let stream_reservation = host
            .reserve_discovery_event_stream_for_owner(&owner)
            .expect("reserve discovery event stream");
        let worker_ready = reservation
            .launch_worker_until_ready(stream_reservation.event_sink())
            .expect("worker reaches ready state before publication");
        let mut worker_ready = worker_ready;
        {
            let publication = host.discovery.start_publication_guard();
            publication
                .ensure_worker_ready_publishable(worker_ready.scan_id(), &owner)
                .expect("worker-ready start should be publishable");
            worker_ready.start_worker();
        }
        worker_ready
            .wait_for_running_lease()
            .expect("worker must acquire its running lease before final publication");

        assert!(matches!(
            host.discovery.begin_start(&owner, "request", &json!({})),
            Err(LocalDiscoveryServiceError::StartInProgress)
        ));
        let publication = host.discovery.start_publication_guard();
        worker_ready
            .publish_after_running_lease_with(&publication, &owner)
            .expect("final publication should commit the start receipt");
        stream_reservation.commit();
        drop(publication);
        assert!(matches!(
            host.discovery.begin_start(&owner, "request", &json!({})),
            Ok(StartScanOutcome::Replayed(response)) if response["accepted"] == true
        ));
        host.cancel_discovery_sessions();
    }

    #[test]
    fn committed_start_replay_writes_inside_publication_before_shutdown_can_clear_epoch() {
        let host = test_core_host_with_discovery_stream_limits(2, Duration::from_secs(60));
        let owner = DiscoveryOwnerScope::from_authenticated_session(
            "w57-replay-start-wins-owner",
            "session-w57-replay-start-wins-owner",
        );
        let reservation = match host
            .discovery
            .begin_start(&owner, "request", &json!({}))
            .expect("reserve local discovery start")
        {
            StartScanOutcome::Reserved(reservation) => reservation,
            StartScanOutcome::Replayed(_) => panic!("new request must reserve"),
        };
        let stream_reservation = host
            .reserve_discovery_event_stream_for_owner(&owner)
            .expect("reserve discovery event stream");
        let worker_ready = reservation
            .launch_worker_until_ready(stream_reservation.event_sink())
            .expect("worker reaches ready state before publication");
        let (setup_writer, setup_reader) = mpsc::channel();
        host.publish_discovery_start_response(
            &setup_writer,
            "request",
            &owner,
            worker_ready,
            stream_reservation,
        )
        .expect("commit initial start");
        let initial = setup_reader
            .recv_timeout(Duration::from_secs(1))
            .expect("initial start response");
        let initial_epoch = initial["payload"]["eventStream"]["epoch"]
            .as_str()
            .expect("initial epoch")
            .to_owned();
        wait_for_discovery_running_scans_to_drain(&host, &owner);
        let events_before = host
            .discovery_stream_event_count_for_tests(&owner)
            .expect("live discovery stream");

        let hook = Arc::new(local_discovery::WorkerPauseHook::new());
        host.set_start_replay_before_response_hook_for_tests(Arc::clone(&hook));
        let (writer, reader) = mpsc::channel();
        let replay_host = Arc::clone(&host);
        let replay_session = SessionBinding {
            client_id: "w57-replay-start-wins-owner".into(),
            session_id: "session-w57-replay-start-wins-owner".into(),
        };
        let (replay_result_sender, replay_result_receiver) = mpsc::channel();
        thread::spawn(move || {
            let result = handle_local_discovery_command(
                &writer,
                &replay_host,
                &replay_session,
                CommandEnvelope {
                    kind: "command".into(),
                    protocol: ProtocolVersion { major: 1, minor: 0 },
                    request_id: "request".into(),
                    session_id: "session-w57-replay-start-wins-owner".into(),
                    command: "agent.discovery.start".into(),
                    payload: json!({}),
                    deadline_ms: None,
                },
            )
            .map(|_| ());
            replay_result_sender
                .send(result.map_err(|error| error.to_string()))
                .expect("replay result receiver remains available");
        });

        assert!(
            hook.wait_until_entered(Duration::from_secs(1)),
            "replay must pause after it validates the live epoch while publication is held"
        );
        let shutdown_host = Arc::clone(&host);
        let (shutdown_started_sender, shutdown_started_receiver) = mpsc::channel();
        let (shutdown_result_sender, shutdown_result_receiver) = mpsc::channel();
        thread::spawn(move || {
            shutdown_started_sender
                .send(())
                .expect("shutdown start receiver remains available");
            shutdown_host.cancel_discovery_sessions();
            shutdown_result_sender
                .send(())
                .expect("shutdown result receiver remains available");
        });
        shutdown_started_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("shutdown thread begins");
        assert!(
            shutdown_result_receiver.try_recv().is_err(),
            "shutdown must wait for the replay publication boundary"
        );

        hook.release();
        let replay = reader
            .recv_timeout(Duration::from_secs(1))
            .expect("replay response written before shutdown clears the stream");
        assert_eq!(replay["kind"], "response");
        assert_eq!(replay["payload"]["accepted"], true);
        assert_eq!(replay["payload"]["eventStream"]["epoch"], initial_epoch);
        assert_eq!(
            host.discovery_stream_event_count_for_tests(&owner),
            Some(events_before),
            "replay must not publish another lifecycle event"
        );
        recv_thread_result(
            replay_result_receiver,
            Duration::from_secs(1),
            "committed replay route",
        )
        .expect("replay route succeeds before shutdown");
        shutdown_result_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("shutdown finishes after replay response");
        assert_eq!(host.discovery_stream_count_for_tests(), 0);
    }

    #[test]
    fn shutdown_before_committed_start_replay_writes_only_safe_error() {
        let host = test_core_host_with_discovery_stream_limits(2, Duration::from_secs(60));
        let owner = DiscoveryOwnerScope::from_authenticated_session(
            "w57-replay-shutdown-wins-owner",
            "session-w57-replay-shutdown-wins-owner",
        );
        let reservation = match host
            .discovery
            .begin_start(&owner, "request", &json!({}))
            .expect("reserve local discovery start")
        {
            StartScanOutcome::Reserved(reservation) => reservation,
            StartScanOutcome::Replayed(_) => panic!("new request must reserve"),
        };
        let stream_reservation = host
            .reserve_discovery_event_stream_for_owner(&owner)
            .expect("reserve discovery event stream");
        let worker_ready = reservation
            .launch_worker_until_ready(stream_reservation.event_sink())
            .expect("worker reaches ready state before publication");
        let (setup_writer, setup_reader) = mpsc::channel();
        host.publish_discovery_start_response(
            &setup_writer,
            "request",
            &owner,
            worker_ready,
            stream_reservation,
        )
        .expect("commit initial start");
        let _ = setup_reader
            .recv_timeout(Duration::from_secs(1))
            .expect("initial start response");

        host.cancel_discovery_sessions();
        let (writer, reader) = mpsc::channel();
        let session = SessionBinding {
            client_id: "w57-replay-shutdown-wins-owner".into(),
            session_id: "session-w57-replay-shutdown-wins-owner".into(),
        };
        handle_local_discovery_command(
            &writer,
            &host,
            &session,
            CommandEnvelope {
                kind: "command".into(),
                protocol: ProtocolVersion { major: 1, minor: 0 },
                request_id: "request".into(),
                session_id: "session-w57-replay-shutdown-wins-owner".into(),
                command: "agent.discovery.start".into(),
                payload: json!({}),
                deadline_ms: None,
            },
        )
        .expect("route writes a typed error");
        let error = reader
            .recv_timeout(Duration::from_secs(1))
            .expect("shutdown winner error response");
        assert_eq!(error["kind"], "error");
        assert_eq!(error["code"], "DISCOVERY_SERVICE_SHUTTING_DOWN");
        assert!(error["payload"].is_null());
        assert_eq!(host.discovery_stream_count_for_tests(), 0);
    }

    #[test]
    fn failed_owner_stream_rollbacks_under_state_lock_contention_do_not_fill_capacity() {
        let host = test_core_host_with_discovery_stream_limits(2, Duration::from_secs(60));
        for index in 0..8 {
            let owner = DiscoveryOwnerScope::from_authenticated_session(
                &format!("w55-failed-owner-{index}"),
                &format!("session-w55-failed-owner-{index}"),
            );
            let reservation_host = Arc::clone(&host);
            let (sender, receiver) = mpsc::channel();
            host.discovery.with_state_lock_held_for_tests(|| {
                thread::spawn(move || {
                    sender
                        .send(reservation_host.reserve_discovery_event_stream_for_owner(&owner))
                        .expect("contention test receiver remains available");
                });
            });
            let reservation = recv_thread_result(
                receiver,
                Duration::from_secs(1),
                "stream reservation after LocalDiscoveryState contention",
            )
            .expect("failed start gets a temporary stream reservation");
            drop(reservation);
        }

        assert_eq!(
            host.discovery_stream_count_for_tests(),
            0,
            "failed owners under lock contention must not retain empty stream entries"
        );
        let valid_owner = DiscoveryOwnerScope::from_authenticated_session(
            "w55-valid-owner",
            "session-w55-valid-owner",
        );
        assert!(
            host.reserve_discovery_event_stream_for_owner(&valid_owner)
                .is_ok(),
            "failed-owner rollback must leave stream capacity available"
        );
    }

    #[test]
    fn stream_capacity_failure_rolls_back_local_discovery_start_reservation() {
        let host = test_core_host_with_discovery_stream_limits(1, Duration::from_secs(60));
        let existing_owner = DiscoveryOwnerScope::from_authenticated_session(
            "w54-existing-capacity-owner",
            "session-w54-existing-capacity-owner",
        );
        let flood_owner = DiscoveryOwnerScope::from_authenticated_session(
            "w54-flood-capacity-owner",
            "session-w54-flood-capacity-owner",
        );
        let _ = host
            .create_discovery_event_stream_for_owner(&existing_owner)
            .expect("fill stream owner capacity");
        let outcome = host
            .discovery
            .begin_start(&flood_owner, "request", &json!({}))
            .expect("local discovery reservation happens before stream allocation");
        let reservation = match outcome {
            StartScanOutcome::Reserved(reservation) => reservation,
            StartScanOutcome::Replayed(_) => panic!("new request must reserve"),
        };
        assert!(matches!(
            host.reserve_discovery_event_stream_for_owner(&flood_owner),
            Err(DiscoveryStreamError::CapacityExhausted)
        ));
        drop(reservation);
        let counts = host.discovery.counts_for_tests(&flood_owner);
        assert_eq!(counts.owner_sessions, 0);
        assert_eq!(counts.owner_requests, 0);
        assert_eq!(counts.owner_running_scans, 0);
        assert_eq!(host.discovery_stream_count_for_tests(), 1);
        assert!(host
            .discovery_stream_epoch_for_tests(&existing_owner)
            .is_some());
    }

    #[test]
    fn collaboration_payload_defaults_and_validates_auto_dispatch_type() {
        let payload = serde_json::json!({
            "projectId": "project-1",
            "collaborationRunId": "collaboration-1",
            "rootAgentIds": ["agent-1"],
            "maxCalls": 2,
            "maxDepth": 2
        });
        let (_, collaboration) = collaboration_from_payload(&payload).unwrap();
        assert!(!collaboration.auto_dispatch_handoffs);

        let mut malformed = payload;
        malformed["autoDispatchHandoffs"] = serde_json::json!("yes");
        assert!(collaboration_from_payload(&malformed)
            .unwrap_err()
            .to_string()
            .contains("autoDispatchHandoffs must be a boolean"));
    }

    #[test]
    fn workflow_dispatch_payload_defaults_runtime_and_rejects_unknown_fields() {
        let payload = serde_json::json!({
            "workflowId": "workflow-1",
            "collaborationRunId": "collaboration-1",
            "parentExecutionRunId": "run-1",
            "sourceMessageId": "message-1",
            "task": "run the workflow"
        });
        let command = workflow_dispatch_from_payload(&payload).unwrap();
        assert!(command.start_runtime);

        let mut malformed = payload;
        malformed["startRuntime"] = serde_json::json!("yes");
        assert!(workflow_dispatch_from_payload(&malformed)
            .unwrap_err()
            .to_string()
            .contains("startRuntime must be a boolean"));

        let mut unknown = serde_json::json!({
            "workflowId": "workflow-1",
            "collaborationRunId": "collaboration-1",
            "parentExecutionRunId": "run-1",
            "sourceMessageId": "message-1",
            "task": "run the workflow",
            "prompt": "must not cross IPC"
        });
        assert!(workflow_dispatch_from_payload(&unknown).is_err());
        unknown.as_object_mut().unwrap().remove("prompt");
        assert!(workflow_dispatch_from_payload(&unknown).is_ok());
    }

    #[test]
    fn handshake_rejects_a_cursor_from_another_server_epoch() {
        let value = handshake(Some(StreamCursor {
            stream_id: "core-events".into(),
            sequence: 8,
            epoch: Some("core-old-epoch".into()),
        }));
        assert!(matches!(
            validate_handshake(
                &value,
                "credential-handshake-test-1234567890",
                "core-new-epoch",
            ),
            Err("CURSOR_EPOCH_MISMATCH")
        ));
    }

    #[test]
    fn handshake_accepts_a_cursor_from_the_current_server_epoch() {
        let value = handshake(Some(StreamCursor {
            stream_id: "core-events".into(),
            sequence: 8,
            epoch: Some("core-current-epoch".into()),
        }));
        assert!(validate_handshake(
            &value,
            "credential-handshake-test-1234567890",
            "core-current-epoch",
        )
        .is_ok());
    }

    #[test]
    fn retrieval_preview_parameters_require_scope_and_reject_unknown_fields() {
        let base = serde_json::json!({
            "expectedProjectId": "project",
            "conversationId": "conversation",
            "agentId": "agent",
            "query": "needle",
            "scope": "conversation",
            "sourceTypes": ["message"],
            "limit": 10
        });
        assert!(retrieval_preview_parameters(&base).is_ok());

        let mut vector = base.clone();
        vector["mode"] = serde_json::json!("vector_fixture");
        let (_, vector_fixture) = retrieval_preview_parameters(&vector).unwrap();
        assert!(vector_fixture);

        let mut invalid_mode = base.clone();
        invalid_mode["mode"] = serde_json::json!("provider");
        assert!(retrieval_preview_parameters(&invalid_mode).is_err());

        let mut unknown = base.clone();
        unknown["prompt"] = serde_json::json!("secret-like");
        assert!(retrieval_preview_parameters(&unknown).is_err());

        let mut missing_scope = base;
        missing_scope.as_object_mut().unwrap().remove("scope");
        assert!(retrieval_preview_parameters(&missing_scope).is_err());
    }

    #[test]
    fn execution_retry_payload_requires_a_new_id_source_and_task() {
        let payload = serde_json::json!({
            "executionRunId": "retry-run",
            "sourceExecutionRunId": "source-run",
            "currentTask": "retry task"
        });
        let parsed = execution_retry_from_payload(&payload).unwrap();
        assert_eq!(parsed.0, "retry-run");
        assert_eq!(parsed.1, "source-run");
        assert_eq!(parsed.2, "retry task");
        assert!(parsed.3.is_none());
        assert!(parsed.4.is_none());

        let mut unknown = payload.clone();
        unknown["prompt"] = serde_json::json!("must not cross IPC");
        assert!(execution_retry_from_payload(&unknown).is_err());

        let mut missing_task = payload;
        missing_task.as_object_mut().unwrap().remove("currentTask");
        assert!(execution_retry_from_payload(&missing_task).is_err());
    }

    #[test]
    fn model_selection_ipc_payloads_are_scoped_and_fail_closed() {
        let assignment = serde_json::json!({
            "modelSelectionMode": "pinned",
            "modelId": "model-a",
            "candidateModelListMode": "override",
            "candidateModelListRevision": 3
        });
        let (selection, list_mode, revision) = model_selection_from_payload(&assignment).unwrap();
        assert_eq!(selection.mode, ModelSelectionMode::Pinned);
        assert_eq!(selection.model_id.as_deref(), Some("model-a"));
        assert_eq!(list_mode, IdentityModelListMode::Override);
        assert_eq!(revision, 3);

        let mut invalid_assignment = assignment.clone();
        invalid_assignment["modelSelectionMode"] = serde_json::json!("inherit");
        assert!(model_selection_from_payload(&invalid_assignment).is_err());

        let option = serde_json::json!({
            "id": "option-a",
            "identityScope": "conversation_agent",
            "agentId": "agent-a",
            "conversationId": "conversation-a",
            "modelId": "model-a",
            "displayName": "Model A",
            "connectorId": "mock",
            "source": "manual",
            "availability": "unverified",
            "isDefault": true,
            "sortOrder": 0,
            "catalogRevision": null,
            "contextWindow": null,
            "reasoningEfforts": ["medium"],
            "serviceTiers": []
        });
        let parsed = identity_model_option_from_payload(&option).unwrap();
        assert_eq!(parsed.scope, IdentityModelListScope::ConversationAgent);
        assert_eq!(parsed.conversation_id.as_deref(), Some("conversation-a"));
        assert_eq!(parsed.availability, ModelAvailability::Unverified);

        let mut cross_scope = option.clone();
        cross_scope["projectId"] = serde_json::json!("project-a");
        assert!(identity_model_option_from_payload(&cross_scope).is_err());
        let mut secret_like = option;
        secret_like["apiKey"] = serde_json::json!("must-not-cross-ipc");
        assert!(identity_model_option_from_payload(&secret_like).is_err());
    }

    #[test]
    fn command_receipt_hash_binds_the_runtime_deadline() {
        let command = |deadline_ms| CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "request-deadline".into(),
            session_id: "session-deadline-test".into(),
            command: "execution.start".into(),
            payload: json!({"executionRunId":"run-deadline"}),
            deadline_ms,
        };
        let default_hash = command_payload_hash(&command(None)).unwrap();
        let first_hash = command_payload_hash(&command(Some(1000))).unwrap();
        let second_hash = command_payload_hash(&command(Some(2000))).unwrap();
        assert_ne!(default_hash, first_hash);
        assert_ne!(first_hash, second_hash);
    }

    #[test]
    fn local_import_error_mapping_is_stable_typed_and_secret_free() {
        // The three storage conflicts surface as IMPORT_CONFLICT.
        assert_eq!(
            local_import_outcome_error(&CoreError::Storage(
                StorageError::LocalAgentImportRequestConflict
            ))
            .code(),
            "IMPORT_CONFLICT"
        );
        assert_eq!(
            local_import_outcome_error(&CoreError::Storage(
                StorageError::LocalAgentImportBindingConflict
            ))
            .code(),
            "IMPORT_CONFLICT"
        );
        assert_eq!(
            local_import_outcome_error(&CoreError::Storage(
                StorageError::ConnectorProfileConflict {
                    id: "connector-x".into()
                }
            ))
            .code(),
            "IMPORT_CONFLICT"
        );
        assert_eq!(
            local_import_outcome_error(&CoreError::Storage(
                StorageError::LocalAgentImportModelSelectionConflict
            ))
            .code(),
            "IMPORT_CONFLICT"
        );
        // SQLite and other uncategorized persistence failures use a stable
        // code and never leak SQLite text or database details.
        let sqlite_failure = local_import_outcome_error(&CoreError::Storage(StorageError::Sqlite(
            rusqlite::Error::InvalidQuery,
        )));
        assert_eq!(sqlite_failure.code(), "IMPORT_PERSISTENCE_FAILED");
        assert!(!sqlite_failure
            .message()
            .to_ascii_lowercase()
            .contains("sqlite"));
        assert_eq!(
            local_import_outcome_error(&CoreError::Storage(StorageError::ProjectNotFound {
                id: "project-x".into()
            }))
            .code(),
            "IMPORT_PERSISTENCE_FAILED"
        );
        // The real identity recheck outcome still maps to identity changed.
        assert_eq!(
            LocalDiscoveryServiceError::IdentityChanged.code(),
            "DISCOVERY_IDENTITY_CHANGED"
        );
    }

    #[test]
    fn local_import_payload_hash_ignores_deadline_and_binds_business_fields() {
        let base = local_import_payload_hash("scan-a", "candidate-a", "project-a", None);
        assert_eq!(
            local_import_payload_hash("scan-a", "candidate-a", "project-a", None),
            base,
            "the hash has no envelope inputs to vary"
        );
        assert_ne!(
            local_import_payload_hash("scan-a", "candidate-a", "project-a", Some("model-a")),
            base,
            "modelSelection is part of the business intent"
        );
        assert_ne!(
            local_import_payload_hash("scan-a", "candidate-a", "project-b", None),
            base,
            "projectId is part of the business intent"
        );
    }

    #[test]
    fn startup_classifies_schema_failures_without_changing_ipc() {
        assert_eq!(
            storage_startup_category(
                &StorageError::MigrationChecksumMismatch { version: 11 },
                std::path::Path::new("missing.sqlite3"),
            ),
            CoreStartupCategory::DatabaseSchemaIncompatible
        );
        assert_eq!(
            storage_startup_category(
                &StorageError::MigrationDirty { version: 11 },
                std::path::Path::new("missing.sqlite3"),
            ),
            CoreStartupCategory::DatabaseSchemaIncompatible
        );
        assert_eq!(
            CoreStartupCategory::DatabaseSchemaIncompatible.as_str(),
            "database_schema_incompatible"
        );
    }

    #[test]
    fn startup_diagnostic_redacts_secret_like_values_and_bounds_detail() {
        let detail = redact_startup_detail(
            format!(
                "{}=redacted-value {}: redacted-bearer\n",
                "token", "authorization"
            )
            .repeat(40)
            .as_str(),
        );
        assert!(!detail.contains("redacted-value"));
        assert!(!detail.contains("redacted-bearer"));
        assert!(detail.ends_with("...[truncated]"));
    }

    #[test]
    fn startup_diagnostic_truncation_keeps_utf8_boundaries() {
        let detail = redact_startup_detail(&"中文🙂".repeat(220));
        assert!(detail.ends_with("...[truncated]"));
        assert!(detail.len() <= 512 + "...[truncated]".len());
        assert!(std::str::from_utf8(detail.as_bytes()).is_ok());
    }
}

#[cfg(windows)]
fn session_credential_from_environment() -> Result<String, Box<dyn Error>> {
    let value = std::env::var("AGENTTALK_CORE_SESSION_CREDENTIAL")
        .map_err(|_| std::io::Error::other("AGENTTALK_CORE_SESSION_CREDENTIAL is required"))?;
    if !(32..=256).contains(&value.len()) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "AGENTTALK_CORE_SESSION_CREDENTIAL has an invalid length",
        )
        .into());
    }
    Ok(value)
}

#[cfg(windows)]
fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut difference = 0u8;
    for (left, right) in left.iter().zip(right) {
        difference |= left ^ right;
    }
    difference == 0
}

#[cfg(windows)]
fn validate_bound_request(
    session: &SessionBinding,
    session_id: &str,
    protocol: &ProtocolVersion,
) -> Result<(), &'static str> {
    if session_id != session.session_id {
        return Err("SESSION_MISMATCH");
    }
    validate_protocol(protocol).map_err(|_| "UNSUPPORTED_PROTOCOL")
}
