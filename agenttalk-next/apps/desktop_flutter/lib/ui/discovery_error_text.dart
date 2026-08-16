import '../gen/l10n.dart';
import '../ipc/core_ipc_client.dart';

/// Maps a Core IPC error to a fixed, localized, renderer-safe message.
///
/// Only allowlisted error codes produce a distinct localized text; unknown
/// codes, errors without a code, non-[CoreIpcException] errors and malformed
/// content always fall back to a generic safe message. The raw wire message,
/// details, `toString()` output, JSON and stack traces are never surfaced.
String discoveryErrorText(AppLocalizations l10n, Object error) {
  final code = error is CoreIpcException ? error.code : null;
  switch (code) {
    case 'DISCOVERY_SERVICE_SHUTTING_DOWN':
    case 'CLIENT_CLOSED':
      return l10n.localAgentErrorShuttingDown;
    case 'DISCOVERY_IDENTITY_CHANGED':
      return l10n.localAgentErrorIdentityChanged;
    case 'IMPORT_CONFLICT':
      return l10n.localAgentErrorConflict;
    case 'IMPORT_PERSISTENCE_FAILED':
      return l10n.localAgentErrorPersistence;
    case 'DISCOVERY_OWNER_RECEIPT_CAPACITY_EXHAUSTED':
    case 'DISCOVERY_GLOBAL_RECEIPT_CAPACITY_EXHAUSTED':
    case 'DISCOVERY_OWNER_SCAN_CAPACITY_EXHAUSTED':
    case 'DISCOVERY_GLOBAL_SCAN_CAPACITY_EXHAUSTED':
    case 'DISCOVERY_OWNER_VERIFICATION_CAPACITY_EXHAUSTED':
    case 'DISCOVERY_GLOBAL_VERIFICATION_CAPACITY_EXHAUSTED':
    case 'DISCOVERY_OWNER_IMPORT_PLAN_CAPACITY_EXHAUSTED':
    case 'DISCOVERY_GLOBAL_IMPORT_PLAN_CAPACITY_EXHAUSTED':
    case 'DISCOVERY_IMPORT_PLAN_IN_PROGRESS':
      return l10n.localAgentErrorCapacity;
    case 'DISCOVERY_SCAN_NOT_FOUND':
    case 'DISCOVERY_SCAN_EXPIRED':
      return l10n.localAgentErrorScanMissing;
    case 'DISCOVERY_CANDIDATE_NOT_FOUND':
      return l10n.localAgentErrorCandidateMissing;
    case 'DISCOVERY_CANDIDATE_DISMISSED':
      return l10n.localAgentErrorCandidateDismissed;
    case 'DISCOVERY_CONSENT_REQUIRED':
      return l10n.localAgentErrorConsentRequired;
    case 'DISCOVERY_VERIFICATION_IN_PROGRESS':
      return l10n.localAgentErrorVerificationInProgress;
    case 'DISCOVERY_ADAPTER_REQUIRED':
      return l10n.localAgentErrorAdapterRequired;
    case 'DISCOVERY_SCAN_WORKER_UNAVAILABLE':
      return l10n.localAgentErrorScanWorkerUnavailable;
    case 'REPLAY_GAP':
      return l10n.localAgentEventReplayGapNotice;
    default:
      return l10n.localAgentErrorGeneric;
  }
}
