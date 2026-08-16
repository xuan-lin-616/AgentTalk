library;

import 'package:agenttalk_desktop/ipc/core_ipc_client.dart';
import 'package:agenttalk_desktop/ipc/protocol_v1.dart';

/// Renderer-safe typed DTOs for the W5/W6 local discovery & import IPC
/// surface (`agent.discovery.*`, `agent.import.*`).
///
/// every parser is fail-closed: missing fields, wrong types, unknown enum
/// values and unsafe content are rejected with [CoreIpcException] or
/// [FormatException]. No absolute path, PID, port, raw source, token,
/// Authorization/Cookie, runtime.json body, private candidate binding or
/// fingerprint ever enters these DTOs; the import command only ever submits
/// the allowlisted business fields scanId/candidateId/projectId/modelSelection.

const String localDiscoveryEventStreamId = 'local-discovery-events';

const Set<String> discoveryCategoryValues = {
  'agent_runtime',
  'model_runtime',
  'tool_service',
  'unknown',
};

const Set<String> discoveryStateValues = {
  'observed',
  'identified',
  'disappeared',
};

const Set<String> compatibilityStateValues = {
  'not_verified',
  'compatible',
  'incompatible',
  'adapter_required',
};

const Set<String> authStateValues = {
  'unknown',
  'not_required',
  'required',
  'ready',
};

const Set<String> healthStateValues = {
  'not_checked',
  'ready',
  'unavailable',
  'identity_mismatch',
};

const Set<String> availabilityValues = {
  'available',
  'unavailable',
  'unconfigured',
  'authentication_required',
};

const Set<String> verificationStatusValues = {
  'verified',
  'auth_required',
  'rejected',
};

const Set<String> snapshotStateValues = {
  'running',
  'completed',
  'failed',
  'cancelled',
};

const Set<String> discoveryEvidenceValues = {
  'executable_inventory',
  'windows_path_entry',
  'windows_app_path_registry',
  'windows_package_inventory',
  'loopback_listener',
  'user_selected',
  'runtime_record',
  'version_matched',
  'build_matched',
  'install_known',
  'available',
  'authentication_required',
  'unconfigured',
  'identity_mismatch',
  'catalog_unavailable',
};

const Set<String> discoveryEventTypeValues = {
  'agent.discovery.started',
  'agent.discovery.candidate_observed',
  'agent.discovery.candidate_classified',
  'agent.discovery.candidate_verified',
  'agent.discovery.completed',
  'agent.discovery.failed',
};

/// The fixed set of ACP verification diagnostics the Core may project. Any
/// other value fails closed.
const Set<String> _verificationDiagnosticValues = {
  'consent_required',
  'identity_mismatch',
  'identity_unverified',
  'timeout',
  'cancelled',
  'launch_failed',
  'protocol_mismatch',
  'protocol_violation',
  'oversized_frame',
  'non_utf8_frame',
  'stderr_output',
  'process_failed',
  'authentication_required',
  'cleanup_failed',
};

/// Keys that are never allowed to cross into renderer state. The discovery
/// snapshot/verify/dismiss surfaces must reject any of these outright; the
/// import-plan surface rejects the credential/locator subset and explicitly
/// drops (never stores) binding/fingerprint material.
///
/// Exact keys are matched verbatim (case-insensitively); substring keys are
/// matched against the lower-cased field name (e.g. `executablePath` matches
/// `executable`). `pid`/`port` are exact-only because containment would
/// false-positive on legitimate identifiers such as `importId` (which
/// contains "port").
const Set<String> _rendererForbiddenExactKeysLower = {
  'source',
  'path',
  'pid',
  'port',
};

/// Case-insensitive substring stems that identify a path / pid / port bearing
/// field regardless of casing or separators. A field carrying one of these
/// stems fails closed unless its whole lower-cased name is an explicit
/// public-contract field in [_pathPidPortSafePublicFieldsLower].
const Set<String> _rendererForbiddenPathPidPortSubstrings = {
  'path',
  'pid',
  'port',
};

/// Public renderer fields that happen to contain "port" as a substring but
/// are not a path / pid / port. This is an explicit allowlist of the public
/// contract, not a per-test-case patch.
const Set<String> _pathPidPortSafePublicFieldsLower = {
  'transport',
  'importid',
  'supportslogout',
};

/// Returns true when [lower] carries a path/pid/port stem that is not one of
/// the safe public fields. `source` remains exact-only (see
/// [_rendererForbiddenExactKeysLower]) so `sourceKind`/`sourceKinds` pass.
bool _hasForbiddenPathPidPortStem(String lower) {
  if (_pathPidPortSafePublicFieldsLower.contains(lower)) {
    return false;
  }
  return _rendererForbiddenPathPidPortSubstrings.any(lower.contains);
}

const Set<String> _rendererForbiddenSubstrings = {
  'locatorref',
  'executable',
  'absolute',
  'token',
  'authorization',
  'cookie',
  'runtimejson',
  'runtime.json',
  'credential',
  'secret',
  'binding',
  'fingerprint',
  'manifestsha256',
  'candidatebindingdigest',
};

/// The only keys the import-plan boundary may carry and then drop without
/// storing: the two Core-private adapter fields. Every other key containing a
/// forbidden substring (e.g. `candidateBinding`, a nested `binding` object,
/// or a top-level digest) still fails closed.
const Set<String> _planBoundaryDropKeysLower = {
  'manifestsha256',
  'candidatebindingdigest',
};

/// Recursively rejects forbidden content anywhere in a JSON value. When
/// [allowBindingFingerprint] is set, the two exact Core-private adapter keys
/// (`manifestSha256`/`candidateBindingDigest`) are permitted at the boundary
/// so the import-plan parser can drop them without storing them; every other
/// sensitive key is always rejected.
void _rejectSensitiveJson(
  Object? value, {
  required String context,
  required bool allowBindingFingerprint,
}) {
  if (value is Map<String, dynamic>) {
    for (final entry in value.entries) {
      final lower = entry.key.toLowerCase();
      if (_rendererForbiddenExactKeysLower.contains(lower)) {
        throw CoreIpcException('$context payload contains a forbidden field');
      }
      for (final substring in _rendererForbiddenSubstrings) {
        if (lower.contains(substring) &&
            !(allowBindingFingerprint &&
                _planBoundaryDropKeysLower.contains(lower))) {
          throw CoreIpcException('$context payload contains a forbidden field');
        }
      }
      if (_hasForbiddenPathPidPortStem(lower)) {
        throw CoreIpcException('$context payload contains a forbidden field');
      }
      _rejectSensitiveJson(
        entry.value,
        context: context,
        allowBindingFingerprint: allowBindingFingerprint,
      );
    }
  } else if (value is List) {
    for (final item in value) {
      _rejectSensitiveJson(
        item,
        context: context,
        allowBindingFingerprint: allowBindingFingerprint,
      );
    }
  }
}

void _requireType(Object? value, Type type, String context, String field) {
  if (value.runtimeType != type) {
    throw CoreIpcException('$context field $field has an invalid type');
  }
}

String _requireString(Map<String, dynamic> json, String field, String context) {
  final value = json[field];
  _requireType(value, String, context, field);
  if ((value as String).trim().isEmpty) {
    throw CoreIpcException('$context field $field must not be empty');
  }
  return value;
}

String _requireEnum(
  Map<String, dynamic> json,
  String field,
  Set<String> values,
  String context,
) {
  final value = _requireString(json, field, context);
  if (!values.contains(value)) {
    throw CoreIpcException('$context field $field has an unknown value');
  }
  return value;
}

bool _requireBool(Map<String, dynamic> json, String field, String context) {
  final value = json[field];
  _requireType(value, bool, context, field);
  return value as bool;
}

int _requireInt(Map<String, dynamic> json, String field, String context) {
  final value = json[field];
  _requireType(value, int, context, field);
  if (value as int < 0) {
    throw CoreIpcException('$context field $field must not be negative');
  }
  return value;
}

List<String> _requireStringList(
  Map<String, dynamic> json,
  String field,
  String context, {
  Set<String>? enumValues,
  int maxLength = 256,
}) {
  final value = json[field];
  if (value is! List || value.length > maxLength) {
    throw CoreIpcException('$context field $field has an invalid list');
  }
  final result = <String>[];
  for (final entry in value) {
    if (entry is! String || entry.trim().isEmpty) {
      throw CoreIpcException('$context field $field has an invalid entry');
    }
    if (enumValues != null && !enumValues.contains(entry)) {
      throw CoreIpcException('$context field $field has an unknown value');
    }
    result.add(entry);
  }
  return result;
}

/// Result of `agent.discovery.start`. The `eventStream` block carries the
/// discovery stream epoch used for subscribe/replay.
class DiscoveryStartResult {
  const DiscoveryStartResult({
    required this.scanId,
    required this.accepted,
    required this.state,
    required this.eventStreamId,
    required this.eventEpoch,
  });

  final String scanId;
  final bool accepted;
  final String state;
  final String eventStreamId;
  final String eventEpoch;

  factory DiscoveryStartResult.fromResponse(Map<String, dynamic> response) {
    final payload = response['payload'];
    if (payload is! Map<String, dynamic>) {
      throw const CoreIpcException(
        'agent.discovery.start response payload is invalid',
      );
    }
    _rejectSensitiveJson(
      payload,
      context: 'discovery.start',
      allowBindingFingerprint: false,
    );
    final scanId = _requireString(payload, 'scanId', 'discovery.start');
    final accepted = _requireBool(payload, 'accepted', 'discovery.start');
    final state = _requireString(payload, 'state', 'discovery.start');
    if (state != 'running') {
      throw const CoreIpcException(
        'agent.discovery.start response state is invalid',
      );
    }
    final eventStream = payload['eventStream'];
    if (eventStream is! Map<String, dynamic>) {
      throw const CoreIpcException(
        'agent.discovery.start response eventStream is invalid',
      );
    }
    _rejectSensitiveJson(
      eventStream,
      context: 'discovery.start eventStream',
      allowBindingFingerprint: false,
    );
    final eventStreamId = _requireString(
      eventStream,
      'streamId',
      'discovery.start eventStream',
    );
    if (eventStreamId != localDiscoveryEventStreamId) {
      throw const CoreIpcException(
        'agent.discovery.start eventStream id is invalid',
      );
    }
    final eventEpoch = _requireString(
      eventStream,
      'epoch',
      'discovery.start eventStream',
    );
    return DiscoveryStartResult(
      scanId: scanId,
      accepted: accepted,
      state: state,
      eventStreamId: eventStreamId,
      eventEpoch: eventEpoch,
    );
  }
}

/// Renderer-safe verification report projection of an ACP candidate.
class CandidateVerification {
  const CandidateVerification({
    required this.status,
    required this.compatibilityState,
    required this.authState,
    required this.requiresConfiguration,
    this.protocolMajor,
    this.agentInfoName,
    this.agentInfoVersion,
    this.diagnostic,
  });

  final String status;
  final String compatibilityState;
  final String authState;
  final bool requiresConfiguration;
  final int? protocolMajor;
  final String? agentInfoName;
  final String? agentInfoVersion;
  final String? diagnostic;

  factory CandidateVerification.fromJson(Map<String, dynamic> json) {
    _rejectSensitiveJson(
      json,
      context: 'candidate verification',
      allowBindingFingerprint: false,
    );
    final status = _requireEnum(
      json,
      'status',
      verificationStatusValues,
      'candidate verification',
    );
    final compatibilityState = _requireEnum(
      json,
      'compatibilityState',
      compatibilityStateValues,
      'candidate verification',
    );
    final authState = _requireEnum(
      json,
      'authState',
      authStateValues,
      'candidate verification',
    );
    final requiresConfiguration = _requireBool(
      json,
      'requiresConfiguration',
      'candidate verification',
    );
    final protocolMajor = json['protocolMajor'];
    if (protocolMajor != null && protocolMajor is! int) {
      throw const CoreIpcException(
        'candidate verification protocolMajor is invalid',
      );
    }
    final agentInfo = json['agentInfo'];
    if (agentInfo != null && agentInfo is! Map<String, dynamic>) {
      throw const CoreIpcException(
        'candidate verification agentInfo is invalid',
      );
    }
    String? agentInfoName;
    String? agentInfoVersion;
    if (agentInfo is Map<String, dynamic>) {
      // agentInfo carries only public identity fields; anything else fails
      // closed (the recursive sensitive check already ran on the whole
      // verification object).
      for (final key in agentInfo.keys) {
        if (key != 'name' && key != 'version' && key != 'title') {
          throw CoreIpcException(
            'candidate verification agentInfo has an unexpected field',
          );
        }
      }
      final name = agentInfo['name'];
      final version = agentInfo['version'];
      if (name != null && name is! String) {
        throw const CoreIpcException(
          'candidate verification agentInfo name is invalid',
        );
      }
      if (version != null && version is! String) {
        throw const CoreIpcException(
          'candidate verification agentInfo version is invalid',
        );
      }
      agentInfoName = name as String?;
      agentInfoVersion = version as String?;
    }
    final diagnostic = json['diagnostic'];
    if (diagnostic != null) {
      if (diagnostic is! String ||
          !_verificationDiagnosticValues.contains(diagnostic)) {
        throw CoreIpcException('candidate verification diagnostic is invalid');
      }
    }
    return CandidateVerification(
      status: status,
      compatibilityState: compatibilityState,
      authState: authState,
      requiresConfiguration: requiresConfiguration,
      protocolMajor: protocolMajor as int?,
      agentInfoName: agentInfoName,
      agentInfoVersion: agentInfoVersion,
      diagnostic: diagnostic as String?,
    );
  }
}

/// A renderer-safe candidate from `agent.discovery.snapshot`. Contains only
/// public display/status fields; locators, processes, ports, credentials and
/// raw source never appear here.
class DiscoveryCandidate {
  const DiscoveryCandidate({
    required this.candidateId,
    required this.category,
    required this.connectorId,
    required this.runtimeTypeName,
    required this.displayName,
    required this.availability,
    required this.models,
    required this.requiresConfiguration,
    required this.discoveryState,
    required this.compatibilityState,
    required this.authState,
    required this.healthState,
    required this.evidenceSummary,
    this.catalogRevision,
    this.verification,
  });

  final String candidateId;
  final String category;
  final String connectorId;
  final String runtimeTypeName;
  final String displayName;
  final String availability;
  final List<String> models;
  final String? catalogRevision;
  final bool requiresConfiguration;
  final String discoveryState;
  final String compatibilityState;
  final String authState;
  final String healthState;
  final List<String> evidenceSummary;
  final CandidateVerification? verification;

  bool get isAgent => category == 'agent_runtime';

  bool get isUnknown => category == 'unknown';

  bool get isVerified =>
      verification?.status == 'verified' ||
      verification?.status == 'auth_required';

  factory DiscoveryCandidate.fromJson(Map<String, dynamic> json) {
    _rejectSensitiveJson(
      json,
      context: 'discovery candidate',
      allowBindingFingerprint: false,
    );
    final candidateId = _requireString(json, 'candidateId', 'candidate');
    final category = _requireEnum(
      json,
      'category',
      discoveryCategoryValues,
      'candidate',
    );
    final connectorId = _requireString(json, 'connectorId', 'candidate');
    final runtimeType = _requireString(json, 'runtimeType', 'candidate');
    final displayName = _requireString(json, 'displayName', 'candidate');
    final availability = _requireEnum(
      json,
      'availability',
      availabilityValues,
      'candidate',
    );
    final models = _requireStringList(json, 'models', 'candidate');
    final catalogRevision = json['catalogRevision'];
    if (catalogRevision != null && catalogRevision is! String) {
      throw const CoreIpcException('candidate catalogRevision is invalid');
    }
    final requiresConfiguration = _requireBool(
      json,
      'requiresConfiguration',
      'candidate',
    );
    final discoveryState = _requireEnum(
      json,
      'discoveryState',
      discoveryStateValues,
      'candidate',
    );
    final compatibilityState = _requireEnum(
      json,
      'compatibilityState',
      compatibilityStateValues,
      'candidate',
    );
    final authState = _requireEnum(
      json,
      'authState',
      authStateValues,
      'candidate',
    );
    final healthState = _requireEnum(
      json,
      'healthState',
      healthStateValues,
      'candidate',
    );
    final evidenceSummary = _requireStringList(
      json,
      'evidenceSummary',
      'candidate',
      enumValues: discoveryEvidenceValues,
    );
    return DiscoveryCandidate(
      candidateId: candidateId,
      category: category,
      connectorId: connectorId,
      runtimeTypeName: runtimeType,
      displayName: displayName,
      availability: availability,
      models: models,
      catalogRevision: catalogRevision as String?,
      requiresConfiguration: requiresConfiguration,
      discoveryState: discoveryState,
      compatibilityState: compatibilityState,
      authState: authState,
      healthState: healthState,
      evidenceSummary: evidenceSummary,
      verification: null,
    );
  }
}

/// One snapshot candidate entry: the projection plus its snapshot-level
/// lifecycle state and verification report.
class SnapshotCandidate {
  const SnapshotCandidate({
    required this.candidate,
    required this.lifecycleState,
    this.verification,
  });

  final DiscoveryCandidate candidate;
  final String lifecycleState;
  final CandidateVerification? verification;

  factory SnapshotCandidate.fromJson(Map<String, dynamic> json) {
    final candidateJson = json['candidate'];
    if (candidateJson is! Map<String, dynamic>) {
      throw const CoreIpcException('snapshot candidate projection is invalid');
    }
    final candidate = DiscoveryCandidate.fromJson(candidateJson);
    final candidateId = json['candidateId'];
    if (candidateId is! String || candidateId != candidate.candidateId) {
      throw const CoreIpcException(
        'snapshot candidateId does not match its projection',
      );
    }
    final lifecycleState = _requireString(
      json,
      'lifecycleState',
      'snapshot candidate',
    );
    final verificationJson = json['verification'];
    if (verificationJson != null && verificationJson is! Map<String, dynamic>) {
      throw const CoreIpcException('snapshot verification is invalid');
    }
    return SnapshotCandidate(
      candidate: candidate,
      lifecycleState: lifecycleState,
      verification: verificationJson is Map<String, dynamic>
          ? CandidateVerification.fromJson(verificationJson)
          : null,
    );
  }
}

class DiscoverySnapshot {
  const DiscoverySnapshot({
    required this.scanId,
    required this.state,
    required this.candidates,
    required this.diagnostics,
  });

  final String scanId;
  final String state;
  final List<SnapshotCandidate> candidates;
  final List<Map<String, String>> diagnostics;

  factory DiscoverySnapshot.fromResponse(Map<String, dynamic> response) {
    final payload = response['payload'];
    if (payload is! Map<String, dynamic>) {
      throw const CoreIpcException(
        'agent.discovery.snapshot response payload is invalid',
      );
    }
    _rejectSensitiveJson(
      payload,
      context: 'discovery.snapshot',
      allowBindingFingerprint: false,
    );
    final schemaVersion = _requireString(
      payload,
      'schemaVersion',
      'discovery.snapshot',
    );
    if (schemaVersion != 'agent.discovery.snapshot.v1') {
      throw const CoreIpcException(
        'agent.discovery.snapshot schemaVersion is invalid',
      );
    }
    final scanId = _requireString(payload, 'scanId', 'discovery.snapshot');
    final state = _requireEnum(
      payload,
      'state',
      snapshotStateValues,
      'discovery.snapshot',
    );
    final candidatesJson = payload['candidates'];
    if (candidatesJson is! List || candidatesJson.length > 64) {
      throw const CoreIpcException(
        'agent.discovery.snapshot candidates are invalid',
      );
    }
    final candidates = <SnapshotCandidate>[];
    for (final entry in candidatesJson) {
      if (entry is! Map<String, dynamic>) {
        throw const CoreIpcException(
          'agent.discovery.snapshot candidate entry is invalid',
        );
      }
      candidates.add(SnapshotCandidate.fromJson(entry));
    }
    final diagnostics = <Map<String, String>>[];
    final diagnosticsJson = payload['diagnostics'];
    if (diagnosticsJson != null) {
      if (diagnosticsJson is! List || diagnosticsJson.length > 64) {
        throw const CoreIpcException(
          'agent.discovery.snapshot diagnostics are invalid',
        );
      }
      for (final entry in diagnosticsJson) {
        if (entry is! Map<String, dynamic>) {
          throw const CoreIpcException(
            'agent.discovery.snapshot diagnostic entry is invalid',
          );
        }
        diagnostics.add({
          'sourceKind': entry['sourceKind'] is String
              ? entry['sourceKind'] as String
              : 'unknown',
          'code': entry['code'] is String ? entry['code'] as String : 'unknown',
        });
      }
    }
    return DiscoverySnapshot(
      scanId: scanId,
      state: state,
      candidates: candidates,
      diagnostics: diagnostics,
    );
  }
}

class DiscoveryVerifyResult {
  const DiscoveryVerifyResult({
    required this.scanId,
    required this.candidateId,
    required this.accepted,
    required this.state,
    required this.reused,
  });

  final String scanId;
  final String candidateId;
  final bool accepted;
  final String state;
  final bool reused;

  factory DiscoveryVerifyResult.fromResponse(Map<String, dynamic> response) {
    final payload = response['payload'];
    if (payload is! Map<String, dynamic>) {
      throw const CoreIpcException(
        'agent.discovery.verify response payload is invalid',
      );
    }
    _rejectSensitiveJson(
      payload,
      context: 'discovery.verify',
      allowBindingFingerprint: false,
    );
    final scanId = _requireString(payload, 'scanId', 'discovery.verify');
    final candidateId = _requireString(
      payload,
      'candidateId',
      'discovery.verify',
    );
    final accepted = _requireBool(payload, 'accepted', 'discovery.verify');
    final state = _requireString(payload, 'state', 'discovery.verify');
    final reused = payload['reused'] is bool
        ? payload['reused'] as bool
        : false;
    return DiscoveryVerifyResult(
      scanId: scanId,
      candidateId: candidateId,
      accepted: accepted,
      state: state,
      reused: reused,
    );
  }
}

class DiscoveryDismissResult {
  const DiscoveryDismissResult({
    required this.scanId,
    required this.candidateId,
    required this.dismissed,
    required this.alreadyDismissed,
  });

  final String scanId;
  final String candidateId;
  final bool dismissed;
  final bool alreadyDismissed;

  factory DiscoveryDismissResult.fromResponse(Map<String, dynamic> response) {
    final payload = response['payload'];
    if (payload is! Map<String, dynamic>) {
      throw const CoreIpcException(
        'agent.discovery.dismiss response payload is invalid',
      );
    }
    _rejectSensitiveJson(
      payload,
      context: 'discovery.dismiss',
      allowBindingFingerprint: false,
    );
    final scanId = _requireString(payload, 'scanId', 'discovery.dismiss');
    final candidateId = _requireString(
      payload,
      'candidateId',
      'discovery.dismiss',
    );
    final dismissed = _requireBool(payload, 'dismissed', 'discovery.dismiss');
    final alreadyDismissed = payload['alreadyDismissed'] is bool
        ? payload['alreadyDismissed'] as bool
        : false;
    return DiscoveryDismissResult(
      scanId: scanId,
      candidateId: candidateId,
      dismissed: dismissed,
      alreadyDismissed: alreadyDismissed,
    );
  }
}

/// Strict typed projection of the Core ACP capability summary. Only these
/// seven boolean keys are accepted; unknown keys, non-boolean values, and
/// case/underscore variants fail closed. No raw map ever enters renderer
/// state.
class AcpCapabilitySummary {
  const AcpCapabilitySummary({
    required this.loadSession,
    required this.promptImage,
    required this.promptAudio,
    required this.promptEmbeddedContext,
    required this.mcpHttp,
    required this.mcpSse,
    required this.supportsLogout,
  });

  static const Set<String> _allowedKeys = {
    'loadSession',
    'promptImage',
    'promptAudio',
    'promptEmbeddedContext',
    'mcpHttp',
    'mcpSse',
    'supportsLogout',
  };

  final bool loadSession;
  final bool promptImage;
  final bool promptAudio;
  final bool promptEmbeddedContext;
  final bool mcpHttp;
  final bool mcpSse;
  final bool supportsLogout;

  factory AcpCapabilitySummary.fromJson(Map<String, dynamic> json) {
    if (json.keys.length != _allowedKeys.length) {
      throw const CoreIpcException(
        'capability summary must contain exactly the seven capability keys',
      );
    }
    for (final key in json.keys) {
      if (!_allowedKeys.contains(key)) {
        throw CoreIpcException(
          'capability summary contains an unknown key: $key',
        );
      }
      final value = json[key];
      if (value is! bool) {
        throw CoreIpcException('capability summary $key is not a boolean');
      }
    }
    return AcpCapabilitySummary(
      loadSession: json['loadSession'] as bool,
      promptImage: json['promptImage'] as bool,
      promptAudio: json['promptAudio'] as bool,
      promptEmbeddedContext: json['promptEmbeddedContext'] as bool,
      mcpHttp: json['mcpHttp'] as bool,
      mcpSse: json['mcpSse'] as bool,
      supportsLogout: json['supportsLogout'] as bool,
    );
  }
}

/// Read-only import-plan projection. Only public fields are retained;
/// manifestSha256 / candidateBindingDigest from the wire are deliberately
/// dropped and never enter renderer state or the import command.
class ImportPlan {
  const ImportPlan({
    required this.planId,
    required this.scanId,
    required this.candidateId,
    required this.targetProjectId,
    required this.actions,
    required this.connectorId,
    required this.connectorDisplayName,
    required this.adapterKind,
    required this.protocolMajor,
    required this.manifestId,
    required this.capabilities,
    required this.authRequired,
    required this.modelPolicy,
    required this.readOnly,
    this.modelSelection,
  });

  final String planId;
  final String scanId;
  final String candidateId;
  final String targetProjectId;
  final String? modelSelection;
  final List<String> actions;
  final String connectorId;
  final String connectorDisplayName;
  final String adapterKind;
  final int protocolMajor;
  final String manifestId;
  final AcpCapabilitySummary capabilities;
  final bool authRequired;
  final String modelPolicy;
  final bool readOnly;

  /// Public projection of the plan. This is the ONLY renderer state derived
  /// from an `agent.import.plan` response: manifestSha256 and
  /// candidateBindingDigest (Core-private) never appear here.
  Map<String, dynamic> toJsonPublic() => <String, dynamic>{
    'planId': planId,
    'scanId': scanId,
    'candidateId': candidateId,
    'targetProjectId': targetProjectId,
    'modelSelection': modelSelection,
    'actions': actions,
    'connectorId': connectorId,
    'connectorDisplayName': connectorDisplayName,
    'adapterKind': adapterKind,
    'protocolMajor': protocolMajor,
    'manifestId': manifestId,
    'authRequired': authRequired,
    'modelPolicy': modelPolicy,
    'readOnly': readOnly,
  };

  factory ImportPlan.fromResponse(Map<String, dynamic> response) {
    final payload = response['payload'];
    if (payload is! Map<String, dynamic>) {
      throw const CoreIpcException(
        'agent.import.plan response payload is invalid',
      );
    }
    // Exact top-level allowlist: any other field (future fields, case or
    // underscore variants, top-level private digest material) fails closed.
    // manifestSha256 / candidateBindingDigest are only permitted inside the
    // adapter object, where they are dropped at the wire boundary.
    const topLevelKeys = {
      'schemaVersion',
      'planId',
      'scanId',
      'candidateId',
      'targetProjectId',
      'modelSelection',
      'actions',
      'connector',
      'adapter',
      'capabilities',
      'authRequired',
      'modelPolicy',
      'readOnly',
    };
    for (final key in payload.keys) {
      if (!topLevelKeys.contains(key)) {
        throw CoreIpcException(
          'import.plan payload has an unexpected top-level field',
        );
      }
    }
    // The plan surface rejects credential/locator material; binding and
    // fingerprint fields, when present, are dropped by the parser below and
    // never exposed.
    _rejectSensitiveJson(
      payload,
      context: 'import.plan',
      allowBindingFingerprint: true,
    );
    final schemaVersion = _requireString(
      payload,
      'schemaVersion',
      'import.plan',
    );
    if (schemaVersion != 'agent.import.plan.v1') {
      throw const CoreIpcException(
        'agent.import.plan schemaVersion is invalid',
      );
    }
    final planId = _requireString(payload, 'planId', 'import.plan');
    final scanId = _requireString(payload, 'scanId', 'import.plan');
    final candidateId = _requireString(payload, 'candidateId', 'import.plan');
    final targetProjectId = _requireString(
      payload,
      'targetProjectId',
      'import.plan',
    );
    final modelSelection = payload['modelSelection'];
    if (modelSelection != null && modelSelection is! String) {
      throw const CoreIpcException('import.plan modelSelection is invalid');
    }
    final actions = _requireStringList(payload, 'actions', 'import.plan');
    final connector = payload['connector'];
    if (connector is! Map<String, dynamic>) {
      throw const CoreIpcException('import.plan connector is invalid');
    }
    for (final key in connector.keys) {
      if (key != 'id' && key != 'displayName') {
        throw CoreIpcException('import.plan connector has an unexpected field');
      }
    }
    final connectorId = _requireString(
      connector,
      'id',
      'import.plan connector',
    );
    final connectorDisplayName = _requireString(
      connector,
      'displayName',
      'import.plan connector',
    );
    final adapter = payload['adapter'];
    if (adapter is! Map<String, dynamic>) {
      throw const CoreIpcException('import.plan adapter is invalid');
    }
    for (final key in adapter.keys) {
      // manifestSha256 / candidateBindingDigest are Core-private fields that
      // the parser drops at the wire boundary; every other key must be one of
      // the public adapter fields.
      if (key != 'kind' &&
          key != 'protocolMajor' &&
          key != 'manifestId' &&
          key != 'manifestSha256' &&
          key != 'candidateBindingDigest') {
        throw CoreIpcException('import.plan adapter has an unexpected field');
      }
    }
    final adapterKind = _requireString(adapter, 'kind', 'import.plan adapter');
    final protocolMajor = _requireInt(
      adapter,
      'protocolMajor',
      'import.plan adapter',
    );
    final manifestId = _requireString(
      adapter,
      'manifestId',
      'import.plan adapter',
    );
    final capabilitiesJson = payload['capabilities'];
    if (capabilitiesJson == null) {
      throw const CoreIpcException('import.plan capabilities are missing');
    }
    if (capabilitiesJson is! Map<String, dynamic>) {
      throw const CoreIpcException('import.plan capabilities are invalid');
    }
    final capabilities = AcpCapabilitySummary.fromJson(capabilitiesJson);
    final authRequired = _requireBool(payload, 'authRequired', 'import.plan');
    final modelPolicy = _requireString(payload, 'modelPolicy', 'import.plan');
    final readOnly = _requireBool(payload, 'readOnly', 'import.plan');
    if (!readOnly) {
      throw const CoreIpcException('import.plan must be read-only');
    }
    return ImportPlan(
      planId: planId,
      scanId: scanId,
      candidateId: candidateId,
      targetProjectId: targetProjectId,
      modelSelection: modelSelection as String?,
      actions: actions,
      connectorId: connectorId,
      connectorDisplayName: connectorDisplayName,
      adapterKind: adapterKind,
      protocolMajor: protocolMajor,
      manifestId: manifestId,
      capabilities: capabilities,
      authRequired: authRequired,
      modelPolicy: modelPolicy,
      readOnly: readOnly,
    );
  }
}

/// Minimal renderer-safe receipt from `agent.import_local`.
class LocalAgentImportResult {
  const LocalAgentImportResult({
    required this.importId,
    required this.connectorId,
    required this.agentId,
    required this.projectId,
    required this.reused,
    required this.eventSequence,
  });

  final String importId;
  final String connectorId;
  final String agentId;
  final String projectId;
  final bool reused;
  final int eventSequence;

  factory LocalAgentImportResult.fromResponse(Map<String, dynamic> response) {
    final payload = response['payload'];
    if (payload is! Map<String, dynamic>) {
      throw const CoreIpcException(
        'agent.import_local response payload is invalid',
      );
    }
    _rejectSensitiveJson(
      payload,
      context: 'import_local',
      allowBindingFingerprint: false,
    );
    final schemaVersion = _requireString(
      payload,
      'schemaVersion',
      'import_local',
    );
    if (schemaVersion != 'agent.import_local.v1') {
      throw const CoreIpcException(
        'agent.import_local schemaVersion is invalid',
      );
    }
    final importId = _requireString(payload, 'importId', 'import_local');
    final connectorId = _requireString(payload, 'connectorId', 'import_local');
    final agentId = _requireString(payload, 'agentId', 'import_local');
    final projectId = _requireString(payload, 'projectId', 'import_local');
    final reused = _requireBool(payload, 'reused', 'import_local');
    final eventSequence = _requireInt(payload, 'eventSequence', 'import_local');
    return LocalAgentImportResult(
      importId: importId,
      connectorId: connectorId,
      agentId: agentId,
      projectId: projectId,
      reused: reused,
      eventSequence: eventSequence,
    );
  }
}

/// Typed, renderer-safe summary of one discovery event. Unknown discovery
/// event types are rejected by the parser (fail-closed).
class DiscoveryEventSummary {
  const DiscoveryEventSummary({
    required this.type,
    required this.scanId,
    this.candidateId,
    this.status,
    this.diagnostic,
    this.candidateCount,
  });

  final String type;
  final String scanId;
  final String? candidateId;
  final String? status;
  final String? diagnostic;
  final int? candidateCount;

  factory DiscoveryEventSummary.fromEnvelope(EventEnvelope envelope) {
    final type = envelope.event;
    if (!discoveryEventTypeValues.contains(type)) {
      throw CoreIpcException(
        'discovery event type is not a known discovery event',
      );
    }
    final payload = envelope.payload;
    _rejectSensitiveJson(
      payload,
      context: 'discovery event',
      allowBindingFingerprint: false,
    );
    final scanId = _requireString(payload, 'scanId', 'discovery event');
    final candidateId = payload['candidateId'];
    if (candidateId != null && candidateId is! String) {
      throw const CoreIpcException('discovery event candidateId is invalid');
    }
    final status = payload['status'];
    if (status != null && status is! String) {
      throw const CoreIpcException('discovery event status is invalid');
    }
    final diagnostic = payload['diagnostic'];
    if (diagnostic != null && diagnostic is! String) {
      throw const CoreIpcException('discovery event diagnostic is invalid');
    }
    final candidateCount = payload['candidateCount'];
    if (candidateCount != null && candidateCount is! int) {
      throw const CoreIpcException('discovery event candidateCount is invalid');
    }
    return DiscoveryEventSummary(
      type: type,
      scanId: scanId,
      candidateId: candidateId as String?,
      status: status as String?,
      diagnostic: diagnostic as String?,
      candidateCount: candidateCount as int?,
    );
  }
}
