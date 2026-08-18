// ignore: unused_import
import 'package:intl/intl.dart' as intl;
import 'l10n.dart';

// ignore_for_file: type=lint

/// The translations for English (`en`).
class AppLocalizationsEn extends AppLocalizations {
  AppLocalizationsEn([String locale = 'en']) : super(locale);

  @override
  String get title => 'AgentTalk';

  @override
  String get selectProject => 'Select Project';

  @override
  String get selectConversation => 'Select Conversation';

  @override
  String get noProjectOrConversation =>
      'Please select a Project or Conversation first';

  @override
  String get storeMemorySuccess => 'Memory stored successfully';

  @override
  String get errorInvalidProject => 'Invalid Project';

  @override
  String get errorInvalidConversation => 'Invalid Conversation';

  @override
  String get pleaseSelectProjectOrConversation =>
      'Please select a Project or Conversation first';

  @override
  String get pleaseSelectProject => 'Please select a Project first';

  @override
  String get pleaseSelectConversationAddAttachment =>
      'Please select a Conversation first before adding attachment';

  @override
  String get cancel => 'Cancel';

  @override
  String get save => 'Save';

  @override
  String get confirm => 'Confirm';

  @override
  String get createAgent => 'New Agent';

  @override
  String get editAgent => 'Edit Agent';

  @override
  String get connectorCenter => 'Connector Center';

  @override
  String get connectorDiscovery => 'Connector discovery';

  @override
  String get localDiscovery => 'Local discovery';

  @override
  String get addAgent => 'Add Agent';

  @override
  String get scanLocalAgents => 'Scan local agents';

  @override
  String get manualAddAgent => 'Add manually';

  @override
  String get contextInspector => 'Context Inspector';

  @override
  String get eventRecovery => 'Event Stream Recovery';

  @override
  String get diagnostics => 'Diagnostics & Metadata';

  @override
  String get searchMessages => 'Search Messages';

  @override
  String get writeMemory => 'Write Memory';

  @override
  String get projectAgents => 'Project Agent Roster';

  @override
  String get projectionEntity => 'Projection Entity';

  @override
  String get retrievalSource => 'Write Retrieval Source';

  @override
  String get retrievalSelection => 'Retrieval Selection';

  @override
  String get retrievalPreview => 'Retrieval Preview';

  @override
  String get createWorkflow => 'Create Workflow';

  @override
  String get setAsDefaultModelSuccess => 'Set as default model: ';

  @override
  String get setAsDefaultModelFailed => 'Failed to set default: ';

  @override
  String get refresh => 'Refresh';

  @override
  String get catalogUnavailableOrLoadFailed =>
      'Catalog unavailable/load failed: ';

  @override
  String get availableModelsFromCore => 'Available models (from Core):';

  @override
  String get sourceLabel => 'Source: ';

  @override
  String get availabilityLabel => 'Availability: ';

  @override
  String get setAsDefault => 'Set Default';

  @override
  String get allFieldsCannotBeEmpty =>
      'All fields (including Connector ID and Model ID) cannot be empty';

  @override
  String get displayNameLabel => 'Display Name (Name)';

  @override
  String get displayNameHint => 'e.g. Architect / Codex';

  @override
  String get roleLabel => 'Role';

  @override
  String get roleHint => 'e.g. Full-stack Engineer / Architecture Evaluation';

  @override
  String get specialtyLabel => 'Specialty';

  @override
  String get specialtyHint => 'e.g. Flutter / Rust / Performance Optimization';

  @override
  String get systemPromptLabel => 'System Prompt';

  @override
  String get manuallySpecifiedUnverified => 'Manually specified (unverified)';

  @override
  String get scanLocalAgentsEmptyTitle => 'No local agents yet';

  @override
  String get scanLocalAgentsEmptySubtitle =>
      'Scan local agents or add one manually after you confirm a candidate.';

  @override
  String get scanLocalAgentsScanning => 'Scanning local agents…';

  @override
  String get scanLocalAgentsNoResults => 'No local agents were found.';

  @override
  String get scanLocalAgentsPartial =>
      'Some candidates need configuration or authentication.';

  @override
  String get scanLocalAgentsRequiresConfig => 'Needs configuration';

  @override
  String get scanLocalAgentsRequiresAuth => 'Needs authentication';

  @override
  String get scanLocalAgentsFailed => 'Local agent scan failed: ';

  @override
  String get scanLocalAgentsRetry => 'Retry';

  @override
  String get scanLocalAgentsRescan => 'Rescan';

  @override
  String get scanLocalAgentsUseCandidate => 'Use this candidate';

  @override
  String get scanLocalAgentsManualFallback => 'Add manually';

  @override
  String get discoveryConnectorIdLabel => 'connectorId';

  @override
  String get discoveryRuntimeTypeLabel => 'runtimeType';

  @override
  String get discoveryDisplayNameLabel => 'displayName';

  @override
  String get discoveryAvailabilityLabel => 'availability';

  @override
  String get discoveryModelsLabel => 'models';

  @override
  String get discoveryCatalogRevisionLabel => 'catalogRevision';

  @override
  String get discoverySourceLabel => 'source';

  @override
  String get discoveryRequiresConfigurationLabel => 'requiresConfiguration';

  @override
  String get availabilityAvailable => 'Available';

  @override
  String get availabilityUnavailable => 'Unavailable';

  @override
  String get availabilityUnconfigured => 'Needs configuration';

  @override
  String get availabilityAuthenticationRequired => 'Needs authentication';

  @override
  String get availabilityPartial => 'Partially available';

  @override
  String get availabilityUnknown => 'Unknown';

  @override
  String get localAgentScanDialogTitle => 'Scan & import local agents';

  @override
  String get localAgentScanDialogDescription =>
      'Passively scans local candidates, groups them by category, verifies with a bounded initialize-only handshake, and imports atomically.';

  @override
  String get localAgentRescan => 'Rescan';

  @override
  String get localAgentManualAdd => 'Add manually';

  @override
  String get localAgentSelectExecutable => 'Select file to verify';

  @override
  String get localAgentScanning => 'Scanning…';

  @override
  String get localAgentNoCandidates => 'No local candidates were found.';

  @override
  String get localAgentCategoryAgent => 'Agent';

  @override
  String get localAgentCategoryModelRuntime => 'Model Runtime';

  @override
  String get localAgentCategoryToolServer => 'Tool Server';

  @override
  String get localAgentCategoryUnknown => 'Unknown';

  @override
  String get localAgentGroupEmpty => '(no candidates)';

  @override
  String get localAgentErrorShuttingDown =>
      'The service is shutting down; please retry shortly.';

  @override
  String get localAgentErrorIdentityChanged =>
      'The candidate identity changed; rescan and try again.';

  @override
  String get localAgentErrorConflict =>
      'The import conflicts with an existing record and cannot continue.';

  @override
  String get localAgentErrorPersistence => 'The import could not be persisted.';

  @override
  String get localAgentErrorCapacity =>
      'Capacity is full right now; please retry later.';

  @override
  String get localAgentErrorScanMissing =>
      'The scan no longer exists or expired; rescan to continue.';

  @override
  String get localAgentErrorCandidateMissing =>
      'The candidate no longer exists; rescan to continue.';

  @override
  String get localAgentErrorCandidateDismissed => 'This candidate was hidden.';

  @override
  String get localAgentErrorConsentRequired =>
      'Verification consent is required first.';

  @override
  String get localAgentErrorVerificationInProgress =>
      'This candidate is being verified.';

  @override
  String get localAgentErrorAdapterRequired =>
      'This candidate needs an adapter.';

  @override
  String get localAgentErrorScanWorkerUnavailable =>
      'The scan service is unavailable; please retry.';

  @override
  String get localAgentErrorPlanMismatch =>
      'The import plan no longer matches the current selection; fetch it again.';

  @override
  String get localAgentErrorGeneric => 'The operation failed; please retry.';

  @override
  String get localAgentStatusDiscovery => 'Discovery';

  @override
  String get localAgentStatusCompatibility => 'Protocol';

  @override
  String get localAgentStatusAuth => 'Auth';

  @override
  String get localAgentStatusHealth => 'Health';

  @override
  String get localAgentDiscoveryObserved => 'Observed';

  @override
  String get localAgentDiscoveryIdentified => 'Identified';

  @override
  String get localAgentDiscoveryDisappeared => 'Disappeared';

  @override
  String get localAgentCompatibilityCompatible => 'Compatible';

  @override
  String get localAgentCompatibilityIncompatible => 'Incompatible';

  @override
  String get localAgentCompatibilityAdapterRequired => 'Adapter required';

  @override
  String get localAgentCompatibilityNotVerified => 'Not verified';

  @override
  String get localAgentAuthUnknown => 'Unknown';

  @override
  String get localAgentAuthNotRequired => 'Not required';

  @override
  String get localAgentAuthRequired => 'Sign-in required';

  @override
  String get localAgentAuthReady => 'Ready';

  @override
  String get localAgentHealthNotChecked => 'Not checked';

  @override
  String get localAgentHealthReady => 'Ready';

  @override
  String get localAgentHealthUnavailable => 'Unavailable';

  @override
  String get localAgentHealthIdentityMismatch => 'Identity mismatch';

  @override
  String get localAgentLifecycleObserved => 'Observed';

  @override
  String get localAgentLifecycleIdentified =>
      'Identified, awaiting verification';

  @override
  String get localAgentLifecycleVerifying => 'Verifying…';

  @override
  String get localAgentLifecycleVerified => 'Verified';

  @override
  String get localAgentLifecycleAuthRequired => 'Auth required';

  @override
  String get localAgentLifecycleIdentityChanged =>
      'Identity changed; rescan to refresh';

  @override
  String get localAgentLifecycleNotVerified => 'Not verified';

  @override
  String get localAgentVerifyConsentTitle => 'Verify compatibility';

  @override
  String get localAgentVerifyConsentBody =>
      'Verification runs one bounded protocol handshake (initialize only). No task, prompt, or tool call is sent; the verifier is isolated and time-boxed by Core.';

  @override
  String get localAgentVerifyConsentAgree => 'Agree & verify';

  @override
  String get localAgentVerify => 'Verify';

  @override
  String get localAgentImport => 'Import';

  @override
  String get localAgentDismiss => 'Hide';

  @override
  String get localAgentUnknownNeedsAdapter =>
      'This candidate needs an adapter or manifest to be selected before it can be used.';

  @override
  String get localAgentModelRuntimeNote =>
      'Model Runtime: this category needs a separate model-connector flow (not yet available).';

  @override
  String get localAgentToolServerNote =>
      'Tool Server: this category belongs in the tool center (not yet available).';

  @override
  String get localAgentImportReusedNotice =>
      'This agent was already imported; the existing record was reused.';

  @override
  String get localAgentEventReplayGapNotice =>
      'The event stream had a gap; the view fell back to snapshot refresh.';

  @override
  String get localAgentEventStreamNotice =>
      'Event subscription is unavailable; the view is using snapshot refresh.';

  @override
  String get localAgentProjectRequired =>
      'Select a project before importing an agent.';

  @override
  String get localAgentImportDialogTitle => 'Import agent';

  @override
  String localAgentImportTargetProject(String projectId) {
    return 'Target project: $projectId';
  }

  @override
  String get localAgentModelSelectionTitle => 'Model selection';

  @override
  String get localAgentModelConnectorDefault =>
      'Use the connector default model (no model ID)';

  @override
  String get localAgentModelConnectorDefaultHint =>
      'connector_default; omitting a model ID is a valid import option.';

  @override
  String get localAgentModelPinned => 'Specify a model';

  @override
  String get localAgentModelPinnedLabel => 'Model';

  @override
  String get localAgentModelPinnedUnavailable =>
      'This candidate has no model list; use the connector default.';

  @override
  String get localAgentImportPlanLoading =>
      'Generating the read-only import plan…';

  @override
  String get localAgentImportPlanMissing =>
      'The import plan is not available yet.';

  @override
  String get localAgentImportPlanSummary => 'Import plan summary';

  @override
  String get localAgentImportPlanReadOnly => 'Read-only';

  @override
  String get localAgentImportPlanConnector => 'Connector';

  @override
  String get localAgentImportPlanAdapter => 'Adapter';

  @override
  String get localAgentImportPlanProtocol => 'Protocol';

  @override
  String get localAgentImportPlanAuth => 'Auth';

  @override
  String get localAgentImportPlanAuthRequired => 'Auth required';

  @override
  String get localAgentImportPlanModel => 'Model';

  @override
  String get localAgentImportPlanActions => 'Plan actions: ';

  @override
  String get localAgentImportConfirm => 'Confirm import';

  @override
  String get localAgentImportDone => 'Done';

  @override
  String get localAgentImportSuccess => 'Import succeeded';

  @override
  String get localAgentImportSuccessReused => 'Already imported (reused)';

  @override
  String localAgentImportReceiptNote(String agentId, String connectorId) {
    return 'Created agent $agentId on connector $connectorId. A successful import does not mean a real agent turn has run.';
  }

  @override
  String get localAgentEvidenceExecutableInventory => 'Executable inventory';

  @override
  String get localAgentEvidenceWindowsPath => 'On PATH';

  @override
  String get localAgentEvidenceAppPaths => 'Registered in App Paths';

  @override
  String get localAgentEvidencePackage => 'Package inventory';

  @override
  String get localAgentEvidenceLoopback => 'Loopback service';

  @override
  String get localAgentEvidenceUserSelected => 'User selected';

  @override
  String get localAgentEvidenceRuntimeRecord => 'Runtime record';

  @override
  String get localAgentEvidenceVersionMatched => 'Version matched';

  @override
  String get localAgentEvidenceBuildMatched => 'Build matched';

  @override
  String get localAgentEvidenceInstallKnown => 'Known install';

  @override
  String get localAgentEvidenceAvailable => 'Available';

  @override
  String get localAgentEvidenceAuthRequired => 'Auth required';

  @override
  String get localAgentEvidenceUnconfigured => 'Needs configuration';

  @override
  String get localAgentEvidenceIdentityMismatch => 'Identity mismatch';

  @override
  String get localAgentEvidenceCatalogUnavailable => 'Catalog unavailable';

  @override
  String get connectorDiscoverEmptyTitle => 'No connectors discovered yet';

  @override
  String get connectorDiscoverEmptySubtitle =>
      'Refresh to run connector.discover and inspect local candidates.';

  @override
  String get connectorDiscoverScannning => 'Discovering connectors…';

  @override
  String get connectorDiscoverFailed => 'Connector discovery failed: ';

  @override
  String get connectorDiscoverRetry => 'Retry';

  @override
  String get connectorDiscoverRescan => 'Refresh';

  @override
  String get connectorDiscoverNotFound =>
      'No local connectors were discovered.';

  @override
  String get connectorDiscoverSupported =>
      'Discovered local connector candidates';

  @override
  String get connectorDiscoverManualFallback => 'Manage profiles';

  @override
  String get advancedDiagnosticsTitle => 'Advanced diagnostics';

  @override
  String get advancedDiagnosticsSubtitle =>
      'Runtime status and projection metadata';

  @override
  String get technicalDiagnosticsDetails => 'Technical diagnostics details';

  @override
  String get retryStartup => 'Retry startup';

  @override
  String get coreHealth => 'Core health';

  @override
  String get coreProjectionReady => 'Core projection ready';

  @override
  String get coreProjectionUnavailable => 'Core projection unavailable';

  @override
  String get coreEventStreamError => 'Core event stream error: ';

  @override
  String get coreProjectionReconnected => 'Core projection reconnected';

  @override
  String get coreEventStreamStopped =>
      'Event subscription failed; the app has stopped applying events.';

  @override
  String get coreEventRecoveryFailed =>
      'Event recovery failed; fail-closed remains in effect.';

  @override
  String get projectHasNoAgents => 'This project has no agents yet.';

  @override
  String get projectAgentEmptyHint =>
      'Add or scan an agent to make it appear here.';

  @override
  String get scanLocalAgentsTitle => 'Scan local agents';

  @override
  String get scanLocalAgentsDescription =>
      'This call runs agent.scan_local and never auto-creates an identity.';

  @override
  String get searchMessagesHint => 'Search the current conversation history';

  @override
  String get searchMessagesEmpty => 'Enter a keyword to search messages';

  @override
  String get searchMessagesFailed => 'Search failed: ';

  @override
  String get composerTools => 'Composer tools';

  @override
  String get send => 'Send';

  @override
  String get stopActiveRun => 'Stop active run';

  @override
  String get attachment => 'Attachment';

  @override
  String get memory => 'Memory';

  @override
  String get saveMemorySource => 'Save memory';

  @override
  String get retrieval => 'Retrieval';

  @override
  String get saveRetrievalSource => 'Save retrieval source';

  @override
  String get agentPicker => 'Select agent';

  @override
  String get agentPanel => 'Agent panel';

  @override
  String get workflowPanel => 'Workflow panel';

  @override
  String get toggleTheme => 'Toggle theme';

  @override
  String get project => 'Project';

  @override
  String get conversation => 'Conversation';
}
