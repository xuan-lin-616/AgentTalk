import 'dart:async';
import 'dart:convert';
import 'dart:ffi';
import 'dart:io';
import 'dart:isolate';
import 'dart:math';
import 'dart:typed_data';

import 'package:ffi/ffi.dart';

import 'local_discovery.dart';
import 'protocol_v1.dart';
import 'retrieval_preview.dart';

const _coreIpcCloseTotalTimeout = Duration(seconds: 8);
const _coreIpcClosedCode = 'CLIENT_CLOSED';
const _coreStartupCaptureMaxBytes = 8192;

const _coreStartupUserMessages = <String, String>{
  'database_unavailable': 'AgentTalk 数据库不可用，请检查数据目录后重试。',
  'database_schema_incompatible': 'AgentTalk 数据库版本不兼容，请打开诊断查看详情。',
  'database_locked': 'AgentTalk 数据库正被占用，请关闭其他实例后重试。',
  'permission_denied': 'AgentTalk 没有访问数据目录的权限，请检查目录权限后重试。',
  'runtime_configuration_unavailable': 'AgentTalk Core 启动配置不可用，请检查运行环境后重试。',
  'job_object_registration_failed': 'AgentTalk Core 进程托管失败，请重试启动。',
  'named_pipe_bind_failed': 'AgentTalk Core 通信通道创建失败，请重试启动。',
  'core_startup_failed': 'AgentTalk Core 启动失败，请打开诊断查看详情。',
};

class CoreIpcException implements Exception {
  const CoreIpcException(
    this.message, {
    this.code,
    this.retryable,
    this.details,
  });
  final String message;
  final String? code;
  final bool? retryable;
  final Map<String, dynamic>? details;

  bool get isReplayGap => code == 'REPLAY_GAP';

  ReplayGapDetails? get replayGap =>
      isReplayGap ? ReplayGapDetails.tryParse(details) : null;

  @override
  String toString() => 'CoreIpcException: $message';
}

class ReplayGapDetails {
  const ReplayGapDetails({
    required this.streamId,
    required this.epoch,
    required this.resumeCursor,
    required this.headCursor,
    required this.oldestAvailableCursor,
  });

  final String streamId;
  final String? epoch;
  final StreamCursor? resumeCursor;
  final StreamCursor? headCursor;
  final StreamCursor? oldestAvailableCursor;

  factory ReplayGapDetails.fallback({String? epoch}) => ReplayGapDetails(
    streamId: 'core-events',
    epoch: epoch,
    resumeCursor: null,
    headCursor: null,
    oldestAvailableCursor: null,
  );

  static ReplayGapDetails? tryParse(Map<String, dynamic>? json) {
    if (json == null) return null;
    final streamId = json['streamId'];
    final epoch = json['epoch'];
    if (streamId is! String ||
        streamId.isEmpty ||
        (epoch != null && (epoch is! String || epoch.isEmpty))) {
      return null;
    }
    StreamCursor? cursor(String key) {
      final value = json[key];
      if (value is! Map<String, dynamic>) return null;
      try {
        return StreamCursor.fromJson(value);
      } on FormatException {
        return null;
      }
    }

    return ReplayGapDetails(
      streamId: streamId,
      epoch: epoch as String?,
      resumeCursor: cursor('resumeCursor'),
      headCursor: cursor('headCursor'),
      oldestAvailableCursor: cursor('oldestAvailableCursor'),
    );
  }
}

class IdentityModelTarget {
  const IdentityModelTarget({
    required this.identityScope,
    required this.agentId,
    this.projectId,
    this.conversationId,
  });

  final String identityScope;
  final String agentId;
  final String? projectId;
  final String? conversationId;

  Map<String, dynamic> toJson({String? connectorId}) {
    const scopes = <String>{
      'base_agent',
      'project_agent',
      'conversation_agent',
    };
    if (!scopes.contains(identityScope) || agentId.trim().isEmpty) {
      throw const FormatException('Identity model target is invalid');
    }
    final valid = switch (identityScope) {
      'base_agent' => projectId == null && conversationId == null,
      'project_agent' => projectId != null && conversationId == null,
      'conversation_agent' => projectId == null && conversationId != null,
      _ => false,
    };
    if (!valid) {
      throw const FormatException('Identity model target scope is invalid');
    }
    return <String, dynamic>{
      'identityScope': identityScope,
      'agentId': agentId,
      'projectId': projectId,
      'conversationId': conversationId,
      ...?connectorId == null
          ? null
          : <String, dynamic>{'connectorId': connectorId},
    };
  }
}

class IdentityModelOptionMetadata {
  const IdentityModelOptionMetadata({
    required this.id,
    required this.target,
    required this.modelId,
    required this.displayName,
    required this.connectorId,
    required this.source,
    required this.availability,
    required this.isDefault,
    required this.sortOrder,
    this.catalogRevision,
    this.contextWindow,
    this.reasoningEfforts = const <String>[],
    this.serviceTiers = const <String>[],
  });

  final String id;
  final IdentityModelTarget target;
  final String modelId;
  final String displayName;
  final String connectorId;
  final String source;
  final String availability;
  final bool isDefault;
  final int sortOrder;
  final String? catalogRevision;
  final int? contextWindow;
  final List<String> reasoningEfforts;
  final List<String> serviceTiers;

  Map<String, dynamic> toJson() => <String, dynamic>{
    ...target.toJson(),
    'id': id,
    'modelId': modelId,
    'displayName': displayName,
    'connectorId': connectorId,
    'source': source,
    'availability': availability,
    'isDefault': isDefault,
    'sortOrder': sortOrder,
    'catalogRevision': catalogRevision,
    'contextWindow': contextWindow,
    'reasoningEfforts': reasoningEfforts,
    'serviceTiers': serviceTiers,
  };

  factory IdentityModelOptionMetadata.fromJson(Map<String, dynamic> json) {
    final scope = json['scope'] ?? json['identityScope'];
    if (json['id'] is! String ||
        scope is! String ||
        json['agentId'] is! String ||
        json['modelId'] is! String ||
        json['displayName'] is! String ||
        json['connectorId'] is! String ||
        json['source'] is! String ||
        json['availability'] is! String ||
        json['isDefault'] is! bool ||
        json['sortOrder'] is! int ||
        json['reasoningEfforts'] is! List ||
        json['serviceTiers'] is! List) {
      throw const FormatException('Identity model option payload is invalid');
    }
    return IdentityModelOptionMetadata(
      id: json['id'] as String,
      target: IdentityModelTarget(
        identityScope: scope,
        agentId: json['agentId'] as String,
        projectId: json['projectId'] as String?,
        conversationId: json['conversationId'] as String?,
      ),
      modelId: json['modelId'] as String,
      displayName: json['displayName'] as String,
      connectorId: json['connectorId'] as String,
      source: json['source'] as String,
      availability: json['availability'] as String,
      isDefault: json['isDefault'] as bool,
      sortOrder: json['sortOrder'] as int,
      catalogRevision: json['catalogRevision'] as String?,
      contextWindow: json['contextWindow'] as int?,
      reasoningEfforts: (json['reasoningEfforts'] as List)
          .whereType<String>()
          .toList(growable: false),
      serviceTiers: (json['serviceTiers'] as List).whereType<String>().toList(
        growable: false,
      ),
    );
  }
}

class ModelSelectionSnapshotResult {
  const ModelSelectionSnapshotResult({
    required this.executionRunId,
    required this.modelSnapshot,
    required this.selectionSnapshot,
  });

  final String executionRunId;
  final Map<String, dynamic>? modelSnapshot;
  final Map<String, dynamic>? selectionSnapshot;

  factory ModelSelectionSnapshotResult.fromResponse(
    Map<String, dynamic> response,
  ) {
    final payload = response['payload'];
    if (payload is! Map<String, dynamic> ||
        payload['executionRunId'] is! String ||
        (payload['modelSnapshot'] != null &&
            payload['modelSnapshot'] is! Map<String, dynamic>) ||
        (payload['selectionSnapshot'] != null &&
            payload['selectionSnapshot'] is! Map<String, dynamic>)) {
      throw const CoreIpcException(
        'model_selection.snapshot response payload is invalid',
      );
    }
    return ModelSelectionSnapshotResult(
      executionRunId: payload['executionRunId'] as String,
      modelSnapshot: payload['modelSnapshot'] as Map<String, dynamic>?,
      selectionSnapshot: payload['selectionSnapshot'] as Map<String, dynamic>?,
    );
  }
}

class CollaborationCreateResult {
  const CollaborationCreateResult({
    required this.created,
    required this.alreadyPresent,
    required this.projection,
  });

  final bool created;
  final bool alreadyPresent;
  final Map<String, dynamic> projection;

  factory CollaborationCreateResult.fromResponse(
    Map<String, dynamic> response,
  ) {
    final payload = _requireProjectionMutationPayload(
      response,
      'collaboration.create',
    );
    return CollaborationCreateResult(
      created: payload['created'] as bool,
      alreadyPresent: payload['alreadyPresent'] as bool,
      projection: payload['projection'] as Map<String, dynamic>,
    );
  }
}

class HandoffCreateResult {
  const HandoffCreateResult({
    required this.created,
    required this.alreadyPresent,
    required this.projection,
  });

  final bool created;
  final bool alreadyPresent;
  final Map<String, dynamic> projection;

  factory HandoffCreateResult.fromResponse(Map<String, dynamic> response) {
    final payload = _requireProjectionMutationPayload(
      response,
      'handoff.create',
    );
    return HandoffCreateResult(
      created: payload['created'] as bool,
      alreadyPresent: payload['alreadyPresent'] as bool,
      projection: payload['projection'] as Map<String, dynamic>,
    );
  }
}

class HandoffDispatchResult {
  const HandoffDispatchResult({
    required this.created,
    required this.alreadyAtTarget,
    required this.childExecutionRunId,
    required this.runtimeStarted,
    required this.runtimeDispatch,
    required this.projection,
  });

  final bool created;
  final bool alreadyAtTarget;
  final String childExecutionRunId;
  final bool runtimeStarted;
  final String runtimeDispatch;
  final Map<String, dynamic> projection;

  factory HandoffDispatchResult.fromResponse(Map<String, dynamic> response) {
    final payload = response['payload'];
    if (payload is! Map<String, dynamic> ||
        payload['created'] is! bool ||
        payload['alreadyAtTarget'] is! bool ||
        payload['childExecutionRunId'] is! String ||
        payload['runtimeStarted'] is! bool ||
        payload['runtimeDispatch'] is! String ||
        payload['projection'] is! Map<String, dynamic>) {
      throw const CoreIpcException(
        'handoff.dispatch response payload is invalid',
      );
    }
    return HandoffDispatchResult(
      created: payload['created'] as bool,
      alreadyAtTarget: payload['alreadyAtTarget'] as bool,
      childExecutionRunId: payload['childExecutionRunId'] as String,
      runtimeStarted: payload['runtimeStarted'] as bool,
      runtimeDispatch: payload['runtimeDispatch'] as String,
      projection: payload['projection'] as Map<String, dynamic>,
    );
  }
}

class HandoffTransitionResult {
  const HandoffTransitionResult({
    required this.handoffId,
    required this.status,
    required this.changed,
    required this.alreadyAtTarget,
    required this.projection,
  });

  final String handoffId;
  final String status;
  final bool changed;
  final bool alreadyAtTarget;
  final Map<String, dynamic> projection;

  factory HandoffTransitionResult.fromResponse(Map<String, dynamic> response) {
    final payload = response['payload'];
    if (payload is! Map<String, dynamic> ||
        payload['handoffId'] is! String ||
        payload['status'] is! String ||
        payload['changed'] is! bool ||
        payload['alreadyAtTarget'] is! bool ||
        payload['projection'] is! Map<String, dynamic>) {
      throw const CoreIpcException(
        'handoff transition response payload is invalid',
      );
    }
    return HandoffTransitionResult(
      handoffId: payload['handoffId'] as String,
      status: payload['status'] as String,
      changed: payload['changed'] as bool,
      alreadyAtTarget: payload['alreadyAtTarget'] as bool,
      projection: payload['projection'] as Map<String, dynamic>,
    );
  }
}

class ConfigImportResult {
  const ConfigImportResult({
    required this.success,
    required this.newProjectId,
    required this.importedAgents,
    required this.importedConversations,
    required this.importedWorkflows,
    required this.workspaceRebindRequired,
    required this.projection,
  });

  final bool success;
  final String newProjectId;
  final int importedAgents;
  final int importedConversations;
  final int importedWorkflows;
  final bool workspaceRebindRequired;
  final Map<String, dynamic> projection;

  factory ConfigImportResult.fromResponse(Map<String, dynamic> response) {
    final payload = response['payload'];
    if (payload is! Map<String, dynamic> ||
        payload['success'] is! bool ||
        payload['newProjectId'] is! String ||
        payload['importedAgents'] is! int ||
        payload['importedConversations'] is! int ||
        payload['importedWorkflows'] is! int ||
        payload['workspaceRebindRequired'] is! bool ||
        payload['projection'] is! Map<String, dynamic>) {
      throw const CoreIpcException('config.import response payload is invalid');
    }
    return ConfigImportResult(
      success: payload['success'] as bool,
      newProjectId: payload['newProjectId'] as String,
      importedAgents: payload['importedAgents'] as int,
      importedConversations: payload['importedConversations'] as int,
      importedWorkflows: payload['importedWorkflows'] as int,
      workspaceRebindRequired: payload['workspaceRebindRequired'] as bool,
      projection: payload['projection'] as Map<String, dynamic>,
    );
  }
}

class ArtifactContentChunk {
  const ArtifactContentChunk({
    required this.artifactId,
    required this.sha256,
    required this.offset,
    required this.size,
    required this.bytes,
    required this.eof,
  });

  final String artifactId;
  final String sha256;
  final int offset;
  final int size;
  final Uint8List bytes;
  final bool eof;

  factory ArtifactContentChunk.fromResponse(Map<String, dynamic> response) {
    final payload = response['payload'];
    if (payload is! Map<String, dynamic> ||
        payload['artifactId'] is! String ||
        payload['sha256'] is! String ||
        payload['offset'] is! int ||
        payload['size'] is! int ||
        payload['chunkBase64'] is! String ||
        payload['chunkBytes'] is! int ||
        payload['eof'] is! bool) {
      throw const CoreIpcException(
        'artifact.content response payload is invalid',
      );
    }
    final artifactId = payload['artifactId'] as String;
    final sha256 = payload['sha256'] as String;
    final offset = payload['offset'] as int;
    final size = payload['size'] as int;
    final encoded = payload['chunkBase64'] as String;
    final declaredBytes = payload['chunkBytes'] as int;
    final eof = payload['eof'] as bool;
    Uint8List bytes;
    try {
      bytes = Uint8List.fromList(base64Decode(encoded));
    } on FormatException {
      throw const CoreIpcException(
        'artifact.content response chunk is not valid base64',
      );
    }
    if (artifactId.trim().isEmpty ||
        sha256.length != 64 ||
        offset < 0 ||
        size < 0 ||
        size > 64 * 1024 * 1024 ||
        offset > size ||
        declaredBytes < 0 ||
        declaredBytes > 64 * 1024 ||
        declaredBytes != bytes.length ||
        bytes.length > 64 * 1024 ||
        offset + bytes.length > size ||
        (eof != (offset + bytes.length >= size))) {
      throw const CoreIpcException(
        'artifact.content response chunk metadata is invalid',
      );
    }
    return ArtifactContentChunk(
      artifactId: artifactId,
      sha256: sha256,
      offset: offset,
      size: size,
      bytes: bytes,
      eof: eof,
    );
  }
}

class ConnectorProfileMetadata {
  const ConnectorProfileMetadata({
    required this.scopeId,
    required this.connectorId,
    required this.displayName,
    required this.providerType,
    required this.runtimeTypeName,
    required this.enabled,
    this.authEnvKey,
  });

  final String scopeId;
  final String connectorId;
  final String displayName;
  final String providerType;
  final String runtimeTypeName;
  final bool enabled;
  final String? authEnvKey;

  factory ConnectorProfileMetadata.fromJson(Map<String, dynamic> json) {
    final scopeId = json['scopeId'];
    final connectorId = json['connectorId'];
    final displayName = json['displayName'];
    final providerType = json['providerType'];
    final runtimeType = json['runtimeType'];
    final enabled = json['enabled'];
    final authEnvKey = json['authEnvKey'];
    if (scopeId is! String ||
        scopeId.isEmpty ||
        connectorId is! String ||
        connectorId.isEmpty ||
        displayName is! String ||
        displayName.isEmpty ||
        providerType is! String ||
        providerType.isEmpty ||
        runtimeType is! String ||
        runtimeType.isEmpty ||
        enabled is! bool ||
        (authEnvKey != null && authEnvKey is! String)) {
      throw const CoreIpcException('Connector profile payload is invalid');
    }
    return ConnectorProfileMetadata(
      scopeId: scopeId,
      connectorId: connectorId,
      displayName: displayName,
      providerType: providerType,
      runtimeTypeName: runtimeType,
      enabled: enabled,
      authEnvKey: authEnvKey as String?,
    );
  }

  Map<String, dynamic> toJson() => <String, dynamic>{
    'scopeId': scopeId,
    'connectorId': connectorId,
    'displayName': displayName,
    'providerType': providerType,
    'runtimeType': runtimeTypeName,
    'enabled': enabled,
    if (authEnvKey != null && authEnvKey!.isNotEmpty) 'authEnvKey': authEnvKey,
  };
}

class ConnectorHealthCapabilities {
  const ConnectorHealthCapabilities({
    required this.streaming,
    required this.cancel,
    required this.filesystem,
    required this.shell,
  });

  final bool streaming;
  final bool cancel;
  final bool filesystem;
  final bool shell;

  factory ConnectorHealthCapabilities.fromJson(Map<String, dynamic> json) {
    _requireExactFields(json, const {
      'streaming',
      'cancel',
      'filesystem',
      'shell',
    }, 'connector.health capabilities payload is invalid');
    final streaming = json['streaming'];
    final cancel = json['cancel'];
    final filesystem = json['filesystem'];
    final shell = json['shell'];
    if (streaming is! bool ||
        cancel is! bool ||
        filesystem is! bool ||
        shell is! bool) {
      throw const CoreIpcException(
        'connector.health capabilities payload is invalid',
      );
    }
    return ConnectorHealthCapabilities(
      streaming: streaming,
      cancel: cancel,
      filesystem: filesystem,
      shell: shell,
    );
  }
}

class ConnectorHealth {
  const ConnectorHealth({
    required this.connectorId,
    required this.displayName,
    required this.providerType,
    required this.runtimeTypeName,
    required this.enabled,
    required this.status,
    required this.availability,
    required this.ok,
    required this.verified,
    required this.verification,
    required this.runtimeId,
    required this.runtimeVersion,
    required this.runtimeOwned,
    required this.capabilities,
    required this.authReferencePresent,
    required this.healthDetailPresent,
    required this.healthDetailRedacted,
  });

  final String connectorId;
  final String displayName;
  final String providerType;
  final String runtimeTypeName;
  final bool enabled;
  final String status;
  final String availability;
  final bool ok;
  final bool verified;
  final String verification;
  final String runtimeId;
  final String? runtimeVersion;
  final bool runtimeOwned;
  final ConnectorHealthCapabilities capabilities;
  final bool authReferencePresent;
  final bool healthDetailPresent;
  final bool healthDetailRedacted;

  factory ConnectorHealth.fromJson(Map<String, dynamic> json) {
    _requireExactFields(json, const {
      'connectorId',
      'displayName',
      'providerType',
      'runtimeType',
      'enabled',
      'status',
      'availability',
      'ok',
      'verified',
      'verification',
      'runtimeId',
      'runtimeVersion',
      'runtimeOwned',
      'capabilities',
      'authReferencePresent',
      'healthDetailPresent',
      'healthDetailRedacted',
    }, 'connector.health connector payload is invalid');
    final connectorId = json['connectorId'];
    final displayName = json['displayName'];
    final providerType = json['providerType'];
    final runtimeType = json['runtimeType'];
    final enabled = json['enabled'];
    final status = json['status'];
    final availability = json['availability'];
    final ok = json['ok'];
    final verified = json['verified'];
    final verification = json['verification'];
    final runtimeId = json['runtimeId'];
    final runtimeVersion = json['runtimeVersion'];
    final runtimeOwned = json['runtimeOwned'];
    final capabilities = json['capabilities'];
    final authReferencePresent = json['authReferencePresent'];
    final healthDetailPresent = json['healthDetailPresent'];
    final healthDetailRedacted = json['healthDetailRedacted'];
    if (connectorId is! String ||
        connectorId.isEmpty ||
        displayName is! String ||
        displayName.isEmpty ||
        providerType is! String ||
        providerType.isEmpty ||
        runtimeType is! String ||
        runtimeType.isEmpty ||
        enabled is! bool ||
        status is! String ||
        status.isEmpty ||
        availability is! String ||
        availability.isEmpty ||
        ok is! bool ||
        verified is! bool ||
        verification is! String ||
        verification.isEmpty ||
        runtimeId is! String ||
        runtimeId.isEmpty ||
        (runtimeVersion != null && runtimeVersion is! String) ||
        runtimeOwned is! bool ||
        capabilities is! Map<String, dynamic> ||
        authReferencePresent is! bool ||
        healthDetailPresent is! bool ||
        healthDetailRedacted is! bool) {
      throw const CoreIpcException(
        'connector.health connector payload is invalid',
      );
    }
    return ConnectorHealth(
      connectorId: connectorId,
      displayName: displayName,
      providerType: providerType,
      runtimeTypeName: runtimeType,
      enabled: enabled,
      status: status,
      availability: availability,
      ok: ok,
      verified: verified,
      verification: verification,
      runtimeId: runtimeId,
      runtimeVersion: runtimeVersion as String?,
      runtimeOwned: runtimeOwned,
      capabilities: ConnectorHealthCapabilities.fromJson(capabilities),
      authReferencePresent: authReferencePresent,
      healthDetailPresent: healthDetailPresent,
      healthDetailRedacted: healthDetailRedacted,
    );
  }
}

class ConnectorHealthResult {
  const ConnectorHealthResult({
    required this.schemaVersion,
    required this.scopeId,
    required this.connector,
  });

  final String schemaVersion;
  final String scopeId;
  final ConnectorHealth connector;

  factory ConnectorHealthResult.fromResponse(
    Map<String, dynamic> response, {
    String? expectedScopeId,
  }) {
    final payload = response['payload'];
    if (payload is! Map<String, dynamic>) {
      throw const CoreIpcException(
        'connector.health response payload is invalid',
      );
    }
    _requireExactFields(payload, const {
      'schemaVersion',
      'scopeId',
      'connector',
    }, 'connector.health response payload is invalid');
    final schemaVersion = payload['schemaVersion'];
    final scopeId = payload['scopeId'];
    final connector = payload['connector'];
    if (schemaVersion != 'connector.health.v1' ||
        scopeId is! String ||
        scopeId.isEmpty ||
        (expectedScopeId != null && scopeId != expectedScopeId) ||
        connector is! Map<String, dynamic>) {
      throw const CoreIpcException(
        'connector.health response payload is invalid',
      );
    }
    return ConnectorHealthResult(
      schemaVersion: schemaVersion as String,
      scopeId: scopeId,
      connector: ConnectorHealth.fromJson(connector),
    );
  }
}

/// A non-persisted local Connector candidate. The Core contract deliberately
/// has no credential or process-control fields, so the UI must still require
/// an explicit user action before creating an Agent or Connector profile.
class LocalConnectorDiscovery {
  const LocalConnectorDiscovery({
    required this.connectorId,
    required this.connectorRuntimeType,
    required this.displayName,
    required this.availability,
    required this.models,
    required this.catalogRevision,
    required this.source,
    required this.requiresConfiguration,
  });

  final String connectorId;
  final String connectorRuntimeType;
  final String displayName;
  final String availability;
  final List<String> models;
  final String? catalogRevision;
  final String source;
  final bool requiresConfiguration;

  factory LocalConnectorDiscovery.fromJson(Map<String, dynamic> json) {
    const fields = <String>{
      'connectorId',
      'runtimeType',
      'displayName',
      'availability',
      'models',
      'catalogRevision',
      'source',
      'requiresConfiguration',
    };
    _requireExactFields(json, fields, 'local discovery payload is invalid');
    final connectorId = json['connectorId'];
    final runtimeType = json['runtimeType'];
    final displayName = json['displayName'];
    final availability = json['availability'];
    final models = json['models'];
    final catalogRevision = json['catalogRevision'];
    final source = json['source'];
    final requiresConfiguration = json['requiresConfiguration'];
    const availabilityValues = <String>{
      'available',
      'unavailable',
      'unconfigured',
      'authentication_required',
    };
    if (connectorId is! String ||
        connectorId.isEmpty ||
        runtimeType is! String ||
        runtimeType.isEmpty ||
        displayName is! String ||
        displayName.isEmpty ||
        availability is! String ||
        !availabilityValues.contains(availability) ||
        models is! List ||
        models.any((model) => model is! String || model.isEmpty) ||
        (catalogRevision != null && catalogRevision is! String) ||
        source is! String ||
        source.isEmpty ||
        requiresConfiguration is! bool) {
      throw const CoreIpcException('local discovery payload is invalid');
    }
    return LocalConnectorDiscovery(
      connectorId: connectorId,
      connectorRuntimeType: runtimeType,
      displayName: displayName,
      availability: availability,
      models: models.cast<String>().toList(growable: false),
      catalogRevision: catalogRevision as String?,
      source: source,
      requiresConfiguration: requiresConfiguration,
    );
  }
}

class LocalConnectorDiscoveryResult {
  const LocalConnectorDiscoveryResult({required this.discoveries});

  final List<LocalConnectorDiscovery> discoveries;

  factory LocalConnectorDiscoveryResult.fromResponse(
    Map<String, dynamic> response,
    String query,
  ) {
    final payload = response['payload'];
    if (payload is! Map<String, dynamic>) {
      throw CoreIpcException('$query response payload is invalid');
    }
    _requireExactFields(payload, const {
      'discoveries',
    }, '$query response payload is invalid');
    final discoveries = payload['discoveries'];
    if (discoveries is! List || discoveries.length > 16) {
      throw CoreIpcException('$query response payload is invalid');
    }
    try {
      final parsed = discoveries
          .map((entry) {
            if (entry is! Map<String, dynamic>) {
              throw const FormatException('local discovery entry is invalid');
            }
            return LocalConnectorDiscovery.fromJson(entry);
          })
          .toList(growable: false);
      final ids = parsed.map((entry) => entry.connectorId).toList();
      final sortedIds = List<String>.from(ids)..sort();
      final stableOrder = ids.asMap().entries.every(
        (entry) => entry.value == sortedIds[entry.key],
      );
      if (ids.toSet().length != ids.length || !stableOrder) {
        throw const FormatException('local discovery entries are not stable');
      }
      return LocalConnectorDiscoveryResult(discoveries: parsed);
    } on FormatException catch (error) {
      throw CoreIpcException(error.message);
    }
  }
}

class ConnectorProfileMutationResult {
  const ConnectorProfileMutationResult({
    required this.changed,
    required this.alreadyAtTarget,
    required this.profile,
    required this.projection,
  });

  final bool changed;
  final bool alreadyAtTarget;
  final ConnectorProfileMetadata profile;
  final Map<String, dynamic> projection;

  factory ConnectorProfileMutationResult.fromResponse(
    Map<String, dynamic> response, {
    required String command,
    required String changedKey,
    required String alreadyKey,
  }) {
    final payload = response['payload'];
    if (payload is! Map<String, dynamic> ||
        payload[changedKey] is! bool ||
        payload[alreadyKey] is! bool ||
        payload['connectorProfile'] is! Map<String, dynamic> ||
        payload['projection'] is! Map<String, dynamic>) {
      throw CoreIpcException('$command response payload is invalid');
    }
    return ConnectorProfileMutationResult(
      changed: payload[changedKey] as bool,
      alreadyAtTarget: payload[alreadyKey] as bool,
      profile: ConnectorProfileMetadata.fromJson(
        payload['connectorProfile'] as Map<String, dynamic>,
      ),
      projection: payload['projection'] as Map<String, dynamic>,
    );
  }
}

class ConnectorProfileRemoveResult {
  const ConnectorProfileRemoveResult({
    required this.removed,
    required this.alreadyAbsent,
    required this.scopeId,
    required this.connectorId,
    required this.projection,
  });

  final bool removed;
  final bool alreadyAbsent;
  final String scopeId;
  final String connectorId;
  final Map<String, dynamic> projection;

  factory ConnectorProfileRemoveResult.fromResponse(
    Map<String, dynamic> response,
  ) {
    final payload = response['payload'];
    if (payload is! Map<String, dynamic> ||
        payload['removed'] is! bool ||
        payload['alreadyAbsent'] is! bool ||
        payload['scopeId'] is! String ||
        payload['connectorId'] is! String ||
        payload['projection'] is! Map<String, dynamic>) {
      throw const CoreIpcException(
        'connector.remove response payload is invalid',
      );
    }
    return ConnectorProfileRemoveResult(
      removed: payload['removed'] as bool,
      alreadyAbsent: payload['alreadyAbsent'] as bool,
      scopeId: payload['scopeId'] as String,
      connectorId: payload['connectorId'] as String,
      projection: payload['projection'] as Map<String, dynamic>,
    );
  }
}

class ExecutionStartResult {
  const ExecutionStartResult({required this.run});

  final Map<String, dynamic> run;

  factory ExecutionStartResult.fromResponse(Map<String, dynamic> response) {
    final payload = response['payload'];
    if (payload is! Map<String, dynamic> ||
        payload['run'] is! Map<String, dynamic>) {
      throw const CoreIpcException(
        'execution.start response payload is invalid',
      );
    }
    return ExecutionStartResult(run: payload['run'] as Map<String, dynamic>);
  }
}

class ExecutionRetryResult {
  const ExecutionRetryResult({
    required this.run,
    required this.sourceExecutionRunId,
  });

  final Map<String, dynamic> run;
  final String sourceExecutionRunId;

  factory ExecutionRetryResult.fromResponse(Map<String, dynamic> response) {
    final payload = response['payload'];
    if (payload is! Map<String, dynamic> ||
        payload['run'] is! Map<String, dynamic> ||
        payload['sourceExecutionRunId'] is! String ||
        (payload['sourceExecutionRunId'] as String).trim().isEmpty) {
      throw const CoreIpcException(
        'execution.retry response payload is invalid',
      );
    }
    return ExecutionRetryResult(
      run: payload['run'] as Map<String, dynamic>,
      sourceExecutionRunId: payload['sourceExecutionRunId'] as String,
    );
  }
}

Map<String, dynamic> _requireProjectionMutationPayload(
  Map<String, dynamic> response,
  String command,
) {
  final payload = response['payload'];
  if (payload is! Map<String, dynamic> ||
      payload['created'] is! bool ||
      payload['alreadyPresent'] is! bool ||
      payload['projection'] is! Map<String, dynamic>) {
    throw CoreIpcException('$command response payload is invalid');
  }
  return payload;
}

Map<String, dynamic> _requireModelMutationPayload(
  Map<String, dynamic> response,
  String command,
) {
  final payload = response['payload'];
  if (payload is! Map<String, dynamic> ||
      payload['changed'] is! bool ||
      payload['projection'] is! Map<String, dynamic>) {
    throw CoreIpcException('$command response payload is invalid');
  }
  return payload;
}

void _requireNonEmpty(String field, String value) {
  if (value.trim().isEmpty) {
    throw CoreIpcException('IPC command requires non-empty $field');
  }
}

void _requireExactFields(
  Map<String, dynamic> json,
  Set<String> fields,
  String message,
) {
  if (json.length != fields.length || !json.keys.every(fields.contains)) {
    throw CoreIpcException(message);
  }
}

class _Win32PipeException extends CoreIpcException {
  const _Win32PipeException({
    required this.operation,
    required this.errorCode,
    required String message,
  }) : super(message);

  final String operation;
  final int errorCode;
}

class CoreEventSubscription {
  CoreEventSubscription._(
    this._client,
    this.subscriptionId,
    this.streamId,
    this.cursor,
    this._controller,
  ) : _lastEventCursor = cursor;

  final CoreIpcClient _client;
  final StreamController<EventEnvelope> _controller;
  final String subscriptionId;
  final String streamId;
  final StreamCursor cursor;
  StreamCursor _lastEventCursor;
  StreamCursor? _lastAckedCursor;
  bool _active = true;

  Stream<EventEnvelope> get events => _controller.stream;

  bool get isActive => _active;

  StreamCursor get lastEventCursor => _lastEventCursor;

  StreamCursor? get lastAckedCursor => _lastAckedCursor;

  Future<Map<String, dynamic>> ack(StreamCursor cursor) {
    return _client.ackEvents(subscriptionId: subscriptionId, cursor: cursor);
  }

  Future<Map<String, dynamic>> unsubscribe() {
    return _client.unsubscribeEvents(subscriptionId: subscriptionId);
  }
}

typedef CoreIpcRead = FutureOr<Uint8List> Function(int length);
typedef CoreIpcWrite = FutureOr<void> Function(Uint8List data);
typedef CoreIpcClose = FutureOr<void> Function();

class CoreIpcClient {
  CoreIpcClient._(
    this._transport,
    this.maximumBytes,
    this._ownedProcess,
    this.sessionCredential,
    this._pipeName,
  );

  /// Creates a client with injected transport callbacks for deterministic Dart tests.
  factory CoreIpcClient.forTesting({
    required CoreIpcRead read,
    required CoreIpcWrite write,
    required CoreIpcClose close,
    int maximumBytes = defaultMaxMessageBytes,
    String? sessionCredential,
    String? serverEpoch,
    String? sessionId,
    Process? ownedProcess,
  }) {
    final client = CoreIpcClient._(
      _CallbackCoreIpcTransport(
        readCallback: read,
        writeCallback: write,
        closeCallback: close,
      ),
      maximumBytes,
      ownedProcess,
      sessionCredential,
      null,
    );
    client._serverEpoch = serverEpoch;
    client._sessionId = sessionId;
    return client;
  }

  _CoreIpcTransport _transport;
  final int maximumBytes;
  final Process? _ownedProcess;
  final String? sessionCredential;
  final String? _pipeName;
  Future<void> _requestQueue = Future<void>.value();
  Future<void>? _closeFuture;
  bool _closeRequested = false;
  bool _closed = false;
  String? _sessionId;
  String? _serverEpoch;
  CoreEventSubscription? _activeSubscription;
  bool _subscriptionStarting = false;
  final List<EventEnvelope> _queuedSubscriptionEvents = [];
  final Map<String, Completer<Map<String, dynamic>>> _pendingResponses = {};
  Future<void>? _readerFuture;
  CoreIpcException? _readerError;
  bool _readerStopRequested = false;

  String? get serverEpoch => _serverEpoch;

  bool get ownsCoreProcess => _ownedProcess != null;

  int? get ownedCoreProcessId => _ownedProcess?.pid;

  Future<bool> waitForOwnedCoreExit({
    Duration timeout = const Duration(seconds: 3),
  }) async {
    final process = _ownedProcess;
    if (process == null) return true;
    return _waitForProcessExit(process, timeout);
  }

  Future<CoreIpcClient> openSubscription({
    required String sessionId,
    StreamCursor? lastSeen,
  }) async {
    if (_transport is _CallbackCoreIpcTransport) {
      return this;
    }
    final pipeName = _pipeName;
    final credential = sessionCredential;
    if (pipeName == null || credential == null) {
      throw const CoreIpcException(
        'Core event subscription requires an owned Named Pipe client',
      );
    }
    final replacement = await CoreIpcClient.connect(
      pipeName: pipeName,
      maximumBytes: maximumBytes,
    );
    final subscription = CoreIpcClient._(
      replacement._transport,
      replacement.maximumBytes,
      null,
      credential,
      pipeName,
    );
    try {
      await subscription.handshake(sessionId: sessionId, lastSeen: lastSeen);
      return subscription;
    } catch (_) {
      await subscription.close();
      rethrow;
    }
  }

  Future<CoreEventSubscription> subscribeEvents({
    required String sessionId,
    required StreamCursor afterCursor,
    int maxInFlightEvents = 64,
    int maxInFlightBytes = 262144,
  }) async {
    if (_activeSubscription != null || _subscriptionStarting) {
      throw const CoreIpcException('Core event subscription is already active');
    }
    final currentSessionId = _sessionId;
    if (currentSessionId == null || currentSessionId != sessionId) {
      throw const CoreIpcException(
        'Core event subscription requires a matching completed handshake',
      );
    }
    if (maxInFlightEvents <= 0 || maxInFlightBytes <= 0) {
      throw const CoreIpcException(
        'Core event subscription windows must be positive',
      );
    }
    if (afterCursor.sequence < 0 || afterCursor.streamId.isEmpty) {
      throw const CoreIpcException('Core event subscription cursor is invalid');
    }
    _subscriptionStarting = true;
    try {
      final response = await request({
        'kind': 'command',
        'protocol': {'major': protocolMajor, 'minor': 0},
        'requestId':
            'events-subscribe-${DateTime.now().microsecondsSinceEpoch}',
        'sessionId': sessionId,
        'command': 'events.subscribe',
        'payload': {
          'afterCursor': afterCursor.toJson(),
          'maxInFlightEvents': maxInFlightEvents,
          'maxInFlightBytes': maxInFlightBytes,
        },
      });
      final payload = response['payload'];
      if (payload is! Map<String, dynamic>) {
        throw CoreIpcException(
          'Core event subscription response payload is invalid',
        );
      }
      final receipt = EventSubscriptionReceipt.fromJson(payload);
      _validateSubscriptionReceipt(receipt, afterCursor);
      final controller = StreamController<EventEnvelope>();
      final subscription = CoreEventSubscription._(
        this,
        receipt.subscriptionId,
        receipt.streamId,
        receipt.cursor,
        controller,
      );
      _activeSubscription = subscription;
      final queuedEvents = List<EventEnvelope>.from(_queuedSubscriptionEvents);
      _queuedSubscriptionEvents.clear();
      try {
        for (final event in queuedEvents) {
          _dispatchEvent(event);
        }
      } catch (_) {
        _activeSubscription = null;
        await controller.close();
        rethrow;
      }
      return subscription;
    } catch (_) {
      _queuedSubscriptionEvents.clear();
      rethrow;
    } finally {
      _subscriptionStarting = false;
    }
  }

  Future<Map<String, dynamic>> ackEvents({
    required String subscriptionId,
    required StreamCursor cursor,
  }) async {
    final subscription = _requireSubscription(subscriptionId);
    _validateSubscriptionCursor(subscription, cursor);
    final lastAcked = subscription._lastAckedCursor;
    if (lastAcked != null && cursor.sequence < lastAcked.sequence) {
      throw const CoreIpcException(
        'Core event acknowledgement cursor moved backwards',
      );
    }
    final response = await request(
      EventAckCommand(
        requestId: _requestId('events-ack'),
        sessionId: _requireSessionId(),
        subscriptionId: subscriptionId,
        cursor: cursor,
      ).toJson(),
    );
    subscription._lastAckedCursor = cursor;
    return response;
  }

  Future<Map<String, dynamic>> unsubscribeEvents({
    required String subscriptionId,
  }) async {
    final subscription = _requireSubscription(subscriptionId);
    final response = await request(
      EventUnsubscribeCommand(
        requestId: _requestId('events-unsubscribe'),
        sessionId: _requireSessionId(),
        subscriptionId: subscriptionId,
      ).toJson(),
    );
    if (identical(_activeSubscription, subscription)) {
      _activeSubscription = null;
      subscription._active = false;
      if (!subscription._controller.isClosed) {
        // A single-subscription StreamController without a listener never
        // completes its close future, so the teardown must not await it.
        unawaited(subscription._controller.close());
      }
    }
    return response;
  }

  CoreEventSubscription _requireSubscription(String subscriptionId) {
    final subscription = _activeSubscription;
    if (subscription == null || subscription.subscriptionId != subscriptionId) {
      throw const CoreIpcException('Core event subscriptionId is invalid');
    }
    return subscription;
  }

  String _requireSessionId() {
    final sessionId = _sessionId;
    if (sessionId == null || sessionId.isEmpty) {
      throw const CoreIpcException(
        'Core event subscription requires a completed handshake',
      );
    }
    return sessionId;
  }

  String _requestId(String prefix) =>
      '$prefix-${DateTime.now().microsecondsSinceEpoch}';

  void _validateSubscriptionReceipt(
    EventSubscriptionReceipt receipt,
    StreamCursor afterCursor,
  ) {
    if (receipt.streamId != afterCursor.streamId ||
        receipt.cursor.streamId != receipt.streamId ||
        receipt.cursor.sequence < afterCursor.sequence ||
        (afterCursor.epoch != null &&
            receipt.cursor.epoch != afterCursor.epoch) ||
        // The discovery stream has its own epoch obtained from the start
        // response; only the core-events stream is bound to the handshake
        // server epoch.
        (_serverEpoch != null &&
            afterCursor.streamId == 'core-events' &&
            receipt.cursor.epoch != _serverEpoch)) {
      throw const CoreIpcException(
        'Core event subscription receipt cursor is invalid',
      );
    }
  }

  void _validateSubscriptionCursor(
    CoreEventSubscription subscription,
    StreamCursor cursor,
  ) {
    if (cursor.streamId != subscription.streamId ||
        cursor.epoch != subscription.cursor.epoch ||
        cursor.sequence < subscription.cursor.sequence ||
        cursor.sequence > subscription._lastEventCursor.sequence) {
      throw const CoreIpcException('Core event subscription cursor is invalid');
    }
  }

  void _dispatchEvent(EventEnvelope event) {
    final subscription = _activeSubscription;
    if (subscription == null) return;
    if (event.subscriptionId != subscription.subscriptionId ||
        event.sessionId != _sessionId ||
        event.cursor.streamId != subscription.streamId ||
        event.cursor.epoch != subscription.cursor.epoch ||
        event.cursor.sequence <= subscription._lastEventCursor.sequence) {
      throw const CoreIpcException(
        'Core event envelope subscriptionId or cursor is invalid',
      );
    }
    subscription._lastEventCursor = event.cursor;
    subscription._controller.add(event);
  }

  void _handleEventFrame(Map<String, dynamic> frame) {
    final event = EventEnvelope.fromJson(frame);
    if (_activeSubscription != null) {
      _dispatchEvent(event);
      return;
    }
    if (_subscriptionStarting) {
      if (_queuedSubscriptionEvents.length >= 128) {
        throw CoreIpcException(
          'Core event subscription startup event buffer overflowed',
        );
      }
      _queuedSubscriptionEvents.add(event);
    }
    // A valid unsolicited event without a matching subscription is parsed and
    // ignored. It must never be treated as a response to a pending request.
  }

  static Future<CoreIpcClient> connect({
    required String pipeName,
    int maximumBytes = defaultMaxMessageBytes,
    int waitTimeoutMs = 250,
  }) async {
    if (!Platform.isWindows) {
      throw const CoreIpcException(
        'AgentTalk Core IPC currently requires Windows Named Pipes',
      );
    }
    if (waitTimeoutMs <= 0 || waitTimeoutMs > 5000) {
      throw const CoreIpcException('Core IPC connect timeout is invalid');
    }
    final result = await _runNativePipeOperation(
      operation: _NativePipeOperation.open,
      arguments: <Object>[pipeName, waitTimeoutMs],
      timeout: Duration(milliseconds: waitTimeoutMs + 750),
    );
    switch (result[0]) {
      case _NativePipeResult.opened:
        final readHandle = result[1];
        if (readHandle == 0 || readHandle == -1) {
          throw const CoreIpcException(
            'AgentTalk Core Named Pipe returned an invalid handle',
          );
        }
        final api = _Win32PipeApi();
        final int writeHandle;
        try {
          writeHandle = api.duplicateHandle(readHandle);
        } catch (_) {
          api.closeHandle(readHandle);
          rethrow;
        }
        return CoreIpcClient._(
          _Win32PipeTransport(api, readHandle, writeHandle),
          maximumBytes,
          null,
          null,
          pipeName,
        );
      case _NativePipeResult.waitFailed:
        final errorCode = result[1];
        throw _Win32PipeException(
          operation: 'WaitNamedPipeW',
          errorCode: errorCode,
          message:
              'AgentTalk Core Named Pipe is not available '
              '(Win32 $errorCode)',
        );
      case _NativePipeResult.openFailed:
        final errorCode = result[1];
        throw _Win32PipeException(
          operation: 'CreateFileW',
          errorCode: errorCode,
          message:
              'AgentTalk Core Named Pipe could not be opened '
              '(Win32 $errorCode)',
        );
      default:
        throw const CoreIpcException(
          'AgentTalk Core Named Pipe native operation timed out',
        );
    }
  }

  static Future<CoreIpcClient> startOwned({
    required String coreExecutable,
    required String pipeName,
    required String databasePath,
    String? artifactRoot,
    Map<String, String>? environmentOverrides,
    int maximumBytes = defaultMaxMessageBytes,
    bool Function()? isCancelled,
  }) async {
    _throwIfCancelled(isCancelled);
    final credential = _generateSessionCredential();
    final arguments = <String>[pipeName, databasePath];
    if (artifactRoot != null && artifactRoot.isNotEmpty) {
      arguments.add(artifactRoot);
    }
    final process = await Process.start(
      coreExecutable,
      arguments,
      runInShell: false,
      environment: {
        ...Platform.environment,
        ...?environmentOverrides,
        'AGENTTALK_CORE_SESSION_CREDENTIAL': credential,
      },
    );
    final stdoutCapture = _BoundedProcessOutput();
    final stderrCapture = _BoundedProcessOutput();
    final stdoutDrain = _drainProcessOutput(process.stdout, stdoutCapture);
    final stderrDrain = _drainProcessOutput(process.stderr, stderrCapture);
    CoreIpcClient? connected;
    try {
      _throwIfCancelled(isCancelled);
      final exitCodeFuture = process.exitCode;
      final deadline = Stopwatch()..start();
      Object? lastError;
      while (connected == null &&
          deadline.elapsed < const Duration(seconds: 10)) {
        _throwIfCancelled(isCancelled);
        final exitCode = await _pollProcessExit(exitCodeFuture);
        if (exitCode != null) {
          await _awaitProcessOutput(stdoutDrain, stderrDrain);
          throw _coreStartupException(
            exitCode: exitCode,
            stdout: stdoutCapture.text,
            stderr: stderrCapture.text,
          );
        }
        final remaining = const Duration(seconds: 10) - deadline.elapsed;
        final waitTimeoutMs = remaining.inMilliseconds.clamp(1, 250);
        try {
          connected = await connect(
            pipeName: pipeName,
            maximumBytes: maximumBytes,
            waitTimeoutMs: waitTimeoutMs,
          );
          _throwIfCancelled(isCancelled);
        } catch (error) {
          lastError = error;
          if (error is! _Win32PipeException ||
              !_isTransientPipeError(error.errorCode)) {
            rethrow;
          }
          final delay = remaining < const Duration(milliseconds: 25)
              ? remaining
              : const Duration(milliseconds: 25);
          if (delay > Duration.zero) await Future<void>.delayed(delay);
        }
      }
      if (connected == null) {
        throw lastError ?? const CoreIpcException('Core IPC did not start');
      }
      return CoreIpcClient._(
        connected._transport,
        connected.maximumBytes,
        process,
        credential,
        pipeName,
      );
    } catch (_) {
      await _awaitProcessOutput(stdoutDrain, stderrDrain);
      if (connected != null) await connected.close();
      await _terminateOwnedProcess(process);
      rethrow;
    }
  }

  static Future<CoreIpcClient> connectExternal({
    required String pipeName,
    required String sessionCredential,
    int maximumBytes = defaultMaxMessageBytes,
    bool Function()? isCancelled,
  }) async {
    if (sessionCredential.length < 32) {
      throw const CoreIpcException(
        'External Core session credential is invalid',
      );
    }
    final deadline = Stopwatch()..start();
    CoreIpcClient? connected;
    Object? lastError;
    try {
      while (connected == null &&
          deadline.elapsed < const Duration(seconds: 10)) {
        _throwIfCancelled(isCancelled);
        final remaining = const Duration(seconds: 10) - deadline.elapsed;
        try {
          connected = await connect(
            pipeName: pipeName,
            maximumBytes: maximumBytes,
            waitTimeoutMs: remaining.inMilliseconds.clamp(1, 250),
          );
          _throwIfCancelled(isCancelled);
        } catch (error) {
          lastError = error;
          if (error is! _Win32PipeException ||
              !_isTransientPipeError(error.errorCode)) {
            rethrow;
          }
          final delay = remaining < const Duration(milliseconds: 25)
              ? remaining
              : const Duration(milliseconds: 25);
          if (delay > Duration.zero) await Future<void>.delayed(delay);
        }
      }
      if (connected == null) {
        throw lastError ??
            const CoreIpcException('External Core IPC did not start');
      }
      return CoreIpcClient._(
        connected._transport,
        connected.maximumBytes,
        null,
        sessionCredential,
        pipeName,
      );
    } catch (_) {
      await connected?.close();
      rethrow;
    }
  }

  Future<Map<String, dynamic>> handshake({
    required String sessionId,
    String? sessionCredential,
    StreamCursor? lastSeen,
  }) async {
    final credential = sessionCredential ?? this.sessionCredential;
    if (credential == null || credential.isEmpty) {
      throw const CoreIpcException(
        'Core IPC handshake requires a session credential',
      );
    }
    final envelope = <String, dynamic>{
      'kind': 'handshake',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'clientId': 'flutter-desktop',
      'sessionId': sessionId,
      'sessionCredential': credential,
      'maxMessageBytes': maximumBytes,
    };
    if (lastSeen != null) envelope['lastSeen'] = lastSeen.toJson();
    final response = await request(envelope);
    _sessionId = sessionId;
    final payload = response['payload'];
    _serverEpoch = payload is Map<String, dynamic>
        ? payload['serverEpoch']?.toString()
        : null;
    return response;
  }

  Future<Map<String, dynamic>> request(Map<String, dynamic> envelope) async {
    if (_closeRequested) {
      throw const CoreIpcException(
        'Core IPC client is closed',
        code: _coreIpcClosedCode,
      );
    }
    final readerError = _readerError;
    if (readerError != null) throw readerError;
    final expectedRequestId = _expectedResponseRequestId(envelope);
    return _enqueue(() => _requestUnlocked(envelope, expectedRequestId));
  }

  Future<List<Map<String, dynamic>>> replayEvents({
    required String sessionId,
    required int afterSequence,
  }) async {
    final response = await request({
      'kind': 'query',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': 'events-${DateTime.now().microsecondsSinceEpoch}',
      'sessionId': sessionId,
      'query': 'events.replay',
      'payload': {'afterSequence': afterSequence, 'limit': 64},
    });
    final responsePayload = response['payload'];
    if (responsePayload is! Map<String, dynamic>) {
      throw const CoreIpcException('Core event replay payload is invalid');
    }
    final events = responsePayload['events'];
    if (events is! List) {
      throw const CoreIpcException('Core event replay list is invalid');
    }
    final typedEvents = events.whereType<Map<String, dynamic>>().toList(
      growable: false,
    );
    for (final event in typedEvents) {
      EventEnvelope.fromJson(event);
    }
    return typedEvents;
  }

  Future<List<Map<String, dynamic>>> searchMessages({
    required String sessionId,
    required String query,
    String? conversationId,
    int limit = 20,
  }) async {
    final payload = <String, dynamic>{'query': query, 'limit': limit};
    if (conversationId != null) {
      payload['conversationId'] = conversationId;
    }
    final response = await request({
      'kind': 'query',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': 'search-${DateTime.now().microsecondsSinceEpoch}',
      'sessionId': sessionId,
      'query': 'messages.search',
      'payload': payload,
    });
    final responsePayload = response['payload'];
    if (responsePayload is! Map<String, dynamic>) {
      throw const CoreIpcException('Core message search payload is invalid');
    }
    final results = responsePayload['messages'] ?? responsePayload['results'];
    if (results is! List) {
      throw const CoreIpcException('Core message search result is invalid');
    }
    return results.whereType<Map<String, dynamic>>().toList(growable: false);
  }

  Future<Map<String, dynamic>> generateSummary({
    required String sessionId,
    required String scopeId,
  }) async {
    _requireNonEmpty('scopeId', scopeId);
    final response = await request({
      'kind': 'command',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': _requestId('summary-generate'),
      'sessionId': sessionId,
      'command': 'summary.generate',
      'payload': {'scopeId': scopeId},
    });
    final payload = response['payload'];
    if (payload is! Map<String, dynamic> ||
        payload['summary'] is! Map<String, dynamic> ||
        payload['projection'] is! Map<String, dynamic> ||
        payload['generator'] is! String ||
        payload['messageCount'] is! int) {
      throw const CoreIpcException(
        'summary.generate response payload is invalid',
      );
    }
    return payload;
  }

  Future<Map<String, dynamic>> querySummaryContent({
    required String sessionId,
    required String summaryId,
  }) async {
    _requireNonEmpty('summaryId', summaryId);
    final response = await request({
      'kind': 'query',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': _requestId('summary-content'),
      'sessionId': sessionId,
      'query': 'summary.content',
      'payload': {'summaryId': summaryId},
    });
    final payload = response['payload'];
    if (payload is! Map<String, dynamic> ||
        payload['summaryId'] is! String ||
        payload['content'] is! String ||
        (payload['content'] as String).length > 64 * 1024) {
      throw const CoreIpcException(
        'summary.content response payload is invalid',
      );
    }
    return payload;
  }

  /// Reads one bounded Artifact Store range. Callers can advance by the
  /// returned byte count to reconstruct large content without sending or
  /// receiving a complete blob in one IPC frame.
  Future<ArtifactContentChunk> queryArtifactContent({
    required String sessionId,
    required String artifactId,
    required int offset,
    int limit = 64 * 1024,
  }) async {
    _requireNonEmpty('artifactId', artifactId);
    if (offset < 0 ||
        limit < 1 ||
        limit > 64 * 1024 ||
        offset > 64 * 1024 * 1024) {
      throw const CoreIpcException(
        'artifact.content range is outside the supported bound',
      );
    }
    final response = await request({
      'kind': 'query',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': _requestId('artifact-content'),
      'sessionId': sessionId,
      'query': 'artifact.content',
      'payload': {'artifactId': artifactId, 'offset': offset, 'limit': limit},
    });
    return ArtifactContentChunk.fromResponse(response);
  }

  /// Stores bounded attachment/summary bytes through the Core-owned Artifact
  /// Store. The body is never returned in the response or projection. Larger
  /// selected files use [importAttachmentFile] so bytes do not cross the
  /// intentionally bounded IPC frame as base64.
  Future<Map<String, dynamic>> storeArtifact({
    required String sessionId,
    required String artifactId,
    required String sha256,
    required int size,
    required String mime,
    String? relativePath,
    Uint8List? body,
  }) async {
    for (final entry in <String, String>{
      'artifactId': artifactId,
      'sha256': sha256,
      'mime': mime,
    }.entries) {
      _requireNonEmpty(entry.key, entry.value);
    }
    if (size < 0 || size > 64 * 1024 * 1024) {
      throw const CoreIpcException(
        'artifact size is outside the supported bound',
      );
    }
    if (body != null) {
      if (body.length > 512 * 1024) {
        throw const CoreIpcException(
          'artifact body exceeds the bounded IPC transfer limit',
        );
      }
      if (body.length != size) {
        throw const CoreIpcException(
          'artifact body size does not match metadata',
        );
      }
    }
    final payload = <String, dynamic>{
      'artifactId': artifactId,
      'sha256': sha256,
      'size': size,
      'mime': mime,
    };
    if (relativePath != null) payload['relativePath'] = relativePath;
    if (body != null) payload['bodyBase64'] = base64Encode(body);
    final response = await request({
      'kind': 'command',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': _requestId('artifact-store'),
      'sessionId': sessionId,
      'command': 'artifact.store',
      'payload': payload,
    });
    final responsePayload = response['payload'];
    if (responsePayload is! Map<String, dynamic> ||
        responsePayload['projection'] is! Map<String, dynamic> ||
        responsePayload['bodyStored'] is! bool) {
      throw const CoreIpcException(
        'artifact.store response payload is invalid',
      );
    }
    return responsePayload;
  }

  /// Imports one explicitly user-selected file through the Core-owned,
  /// file-backed path and associates it with an existing Message. The source
  /// path is one-time command input and must never be returned or projected.
  Future<Map<String, dynamic>> importAttachmentFile({
    required String sessionId,
    required String attachmentId,
    required String artifactId,
    required String messageId,
    required String sourcePath,
    required String mime,
    required int ordinal,
  }) async {
    for (final entry in <String, String>{
      'attachmentId': attachmentId,
      'artifactId': artifactId,
      'messageId': messageId,
      'sourcePath': sourcePath,
      'mime': mime,
    }.entries) {
      _requireNonEmpty(entry.key, entry.value);
    }
    if (sourcePath.length > 32767) {
      throw const CoreIpcException(
        'attachment source path exceeds the supported bound',
      );
    }
    if (ordinal < 0 || ordinal > 1000000) {
      throw const CoreIpcException(
        'attachment ordinal is outside the supported bound',
      );
    }
    final response = await request({
      'kind': 'command',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': _requestId('attachment-import-file'),
      'sessionId': sessionId,
      'command': 'attachment.import_file',
      'payload': {
        'attachmentId': attachmentId,
        'artifactId': artifactId,
        'messageId': messageId,
        'sourcePath': sourcePath,
        'mime': mime,
        'ordinal': ordinal,
      },
    });
    final payload = response['payload'];
    if (payload is! Map<String, dynamic> ||
        payload['projection'] is! Map<String, dynamic> ||
        payload['created'] is! bool ||
        payload['alreadyPresent'] is! bool ||
        payload['artifactCreated'] is! bool ||
        payload['artifactAlreadyPresent'] is! bool ||
        payload['bodyStored'] is! bool ||
        payload['artifact'] is! Map<String, dynamic> ||
        payload['attachment'] is! Map<String, dynamic> ||
        payload.containsKey('sourcePath')) {
      throw const CoreIpcException(
        'attachment.import_file response payload is invalid',
      );
    }
    final artifact = payload['artifact'] as Map<String, dynamic>;
    final attachment = payload['attachment'] as Map<String, dynamic>;
    final returnedSize = artifact['size'];
    final returnedHash = artifact['sha256'];
    final returnedName = attachment['fileName'];
    if (artifact['id'] != artifactId ||
        attachment['attachmentId'] != attachmentId ||
        attachment['artifactId'] != artifactId ||
        attachment['messageId'] != messageId ||
        attachment['ordinal'] != ordinal ||
        returnedSize is! int ||
        returnedSize < 0 ||
        returnedSize > 64 * 1024 * 1024 ||
        attachment['size'] != returnedSize ||
        returnedHash is! String ||
        returnedHash.length != 64 ||
        attachment['sha256'] != returnedHash ||
        returnedName is! String ||
        returnedName.isEmpty ||
        returnedName.contains('/') ||
        returnedName.contains('\\') ||
        artifact.containsKey('sourcePath') ||
        attachment.containsKey('sourcePath') ||
        artifact.containsKey('body') ||
        attachment.containsKey('body')) {
      throw const CoreIpcException(
        'attachment.import_file metadata is invalid',
      );
    }
    return payload;
  }

  /// Associates an existing Artifact with an existing Message. Only bounded
  /// metadata crosses this command; artifact bytes stay in the Core-owned
  /// Artifact Store and are never returned in the projection.
  Future<Map<String, dynamic>> storeAttachment({
    required String sessionId,
    required String attachmentId,
    required String artifactId,
    required String messageId,
    required int ordinal,
    required String fileName,
    required String sha256,
    required int size,
  }) async {
    for (final entry in <String, String>{
      'attachmentId': attachmentId,
      'artifactId': artifactId,
      'messageId': messageId,
      'fileName': fileName,
      'sha256': sha256,
    }.entries) {
      _requireNonEmpty(entry.key, entry.value);
    }
    if (ordinal < 0 || ordinal > 1000000) {
      throw const CoreIpcException(
        'attachment ordinal is outside the supported bound',
      );
    }
    if (size < 0 || size > 64 * 1024 * 1024) {
      throw const CoreIpcException(
        'attachment size is outside the supported bound',
      );
    }
    final response = await request({
      'kind': 'command',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': _requestId('attachment-store'),
      'sessionId': sessionId,
      'command': 'attachment.store',
      'payload': {
        'attachmentId': attachmentId,
        'artifactId': artifactId,
        'messageId': messageId,
        'ordinal': ordinal,
        'fileName': fileName,
        'sha256': sha256,
        'size': size,
      },
    });
    final responsePayload = response['payload'];
    if (responsePayload is! Map<String, dynamic> ||
        responsePayload['projection'] is! Map<String, dynamic> ||
        responsePayload['created'] is! bool ||
        responsePayload['alreadyPresent'] is! bool) {
      throw const CoreIpcException(
        'attachment.store response payload is invalid',
      );
    }
    return responsePayload;
  }

  Future<Map<String, dynamic>> queryRuntimeModels({
    required String sessionId,
  }) async {
    final response = await request({
      'kind': 'query',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': _requestId('runtime-models'),
      'sessionId': sessionId,
      'query': 'runtime.models',
      'payload': <String, dynamic>{},
    });
    final payload = response['payload'];
    if (payload is! Map<String, dynamic> ||
        payload['schemaVersion'] != 'runtime.models.v1' ||
        payload['models'] is! List ||
        payload['modelMetadata'] is! List) {
      throw const CoreIpcException(
        'Core runtime model catalog payload is invalid',
      );
    }
    return payload;
  }

  Future<Map<String, dynamic>> queryOrchestrationRunSnapshot({
    required String sessionId,
    required String runId,
  }) async {
    _requireNonEmpty('runId', runId);
    final response = await request({
      'kind': 'query',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': _requestId('orchestration-run-snapshot'),
      'sessionId': sessionId,
      'query': orchestrationRunSnapshotQuery,
      'payload': {'runId': runId},
    });
    final payload = response['payload'];
    if (payload is! Map<String, dynamic> ||
        payload['run'] is! Map<String, dynamic> ||
        payload['nodes'] is! List ||
        payload['attempts'] is! List ||
        payload['machineAcceptances'] is! List) {
      throw const CoreIpcException(
        'orchestration.run.snapshot payload is invalid',
      );
    }
    return payload;
  }

  Future<Map<String, dynamic>> queryOrchestrationRecoveryState({
    required String sessionId,
    required String runId,
  }) async {
    _requireNonEmpty('runId', runId);
    final response = await request({
      'kind': 'query',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': _requestId('orchestration-run-recovery'),
      'sessionId': sessionId,
      'query': orchestrationRunRecoveryStateQuery,
      'payload': {'runId': runId},
    });
    final payload = response['payload'];
    if (payload is! Map<String, dynamic> ||
        payload['runId'] != runId ||
        payload['coordinatorGeneration'] is! int ||
        payload['nodes'] is! List) {
      throw const CoreIpcException(
        'orchestration.run.recovery_state payload is invalid',
      );
    }
    return payload;
  }

  Future<Map<String, dynamic>> setAgentModelBinding({
    required String sessionId,
    required String agentId,
    String? connectorId,
    String? modelId,
    int candidateModelListRevision = 0,
  }) async {
    _requireNonEmpty('agentId', agentId);
    if (connectorId != null) _requireNonEmpty('connectorId', connectorId);
    if (modelId != null) _requireNonEmpty('modelId', modelId);
    if (candidateModelListRevision < 0) {
      throw const CoreIpcException(
        'candidateModelListRevision must be non-negative',
      );
    }
    final response = await request({
      'kind': 'command',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': _requestId('agent-model-binding-set'),
      'sessionId': sessionId,
      'command': 'agent.model_binding.set',
      'payload': {
        'agentId': agentId,
        'connectorId': connectorId,
        'modelId': modelId,
        'candidateModelListRevision': candidateModelListRevision,
      },
    });
    return _requireModelMutationPayload(response, 'agent.model_binding.set');
  }

  Future<Map<String, dynamic>> setProjectAgentModelSelection({
    required String sessionId,
    required String projectId,
    required String agentId,
    required bool enabled,
    required String workspaceAccess,
    required String modelSelectionMode,
    String? modelId,
    required String candidateModelListMode,
    required int candidateModelListRevision,
  }) => _setAgentModelSelection(
    sessionId: sessionId,
    command: 'project_agent.set',
    scope: <String, dynamic>{'projectId': projectId},
    agentId: agentId,
    enabled: enabled,
    workspaceAccess: workspaceAccess,
    modelSelectionMode: modelSelectionMode,
    modelId: modelId,
    candidateModelListMode: candidateModelListMode,
    candidateModelListRevision: candidateModelListRevision,
  );

  Future<Map<String, dynamic>> setConversationAgentModelSelection({
    required String sessionId,
    required String conversationId,
    required String agentId,
    required bool enabled,
    required String modelSelectionMode,
    String? modelId,
    required String candidateModelListMode,
    required int candidateModelListRevision,
  }) => _setAgentModelSelection(
    sessionId: sessionId,
    command: 'conversation_agent.set',
    scope: <String, dynamic>{'conversationId': conversationId},
    agentId: agentId,
    enabled: enabled,
    modelSelectionMode: modelSelectionMode,
    modelId: modelId,
    candidateModelListMode: candidateModelListMode,
    candidateModelListRevision: candidateModelListRevision,
  );

  Future<Map<String, dynamic>> _setAgentModelSelection({
    required String sessionId,
    required String command,
    required Map<String, dynamic> scope,
    required String agentId,
    required bool enabled,
    String? workspaceAccess,
    required String modelSelectionMode,
    String? modelId,
    required String candidateModelListMode,
    required int candidateModelListRevision,
  }) async {
    _requireNonEmpty('agentId', agentId);
    if (!const <String>{
      'inherit',
      'connector_default',
      'pinned',
    }.contains(modelSelectionMode)) {
      throw const CoreIpcException('modelSelectionMode is invalid');
    }
    if ((modelSelectionMode == 'pinned') != (modelId != null)) {
      throw const CoreIpcException(
        'pinned requires modelId; other selection modes forbid it',
      );
    }
    if (!const <String>{
      'inherit',
      'override',
    }.contains(candidateModelListMode)) {
      throw const CoreIpcException('candidateModelListMode is invalid');
    }
    if (candidateModelListRevision < 0) {
      throw const CoreIpcException(
        'candidateModelListRevision must be non-negative',
      );
    }
    final response = await request({
      'kind': 'command',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': _requestId('model-selection-set'),
      'sessionId': sessionId,
      'command': command,
      'payload': <String, dynamic>{
        ...scope,
        'agentId': agentId,
        'enabled': enabled,
        ...?workspaceAccess == null
            ? null
            : <String, dynamic>{'workspaceAccess': workspaceAccess},
        'modelSelectionMode': modelSelectionMode,
        'modelId': modelId,
        'candidateModelListMode': candidateModelListMode,
        'candidateModelListRevision': candidateModelListRevision,
      },
    });
    return _requireModelMutationPayload(response, command);
  }

  Future<Map<String, dynamic>> upsertIdentityModelOption({
    required String sessionId,
    required IdentityModelOptionMetadata option,
  }) async {
    final response = await request({
      'kind': 'command',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': _requestId('identity-model-option-upsert'),
      'sessionId': sessionId,
      'command': 'identity_model_option.upsert',
      'payload': option.toJson(),
    });
    return _requireModelMutationPayload(
      response,
      'identity_model_option.upsert',
    );
  }

  Future<Map<String, dynamic>> setIdentityModelOptionDefault({
    required String sessionId,
    required IdentityModelTarget target,
    required String connectorId,
    required String modelId,
  }) async {
    _requireNonEmpty('connectorId', connectorId);
    _requireNonEmpty('modelId', modelId);
    final response = await request({
      'kind': 'command',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': _requestId('identity-model-option-default'),
      'sessionId': sessionId,
      'command': 'identity_model_option.default',
      'payload': <String, dynamic>{
        ...target.toJson(),
        'connectorId': connectorId,
        'modelId': modelId,
      },
    });
    return _requireModelMutationPayload(
      response,
      'identity_model_option.default',
    );
  }

  Future<List<IdentityModelOptionMetadata>> queryIdentityModelOptions({
    required String sessionId,
    required IdentityModelTarget target,
    String? connectorId,
  }) async {
    final response = await request({
      'kind': 'query',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': _requestId('identity-model-options-list'),
      'sessionId': sessionId,
      'query': 'identity_model_options.list',
      'payload': target.toJson(connectorId: connectorId),
    });
    final payload = response['payload'];
    if (payload is! Map<String, dynamic> || payload['options'] is! List) {
      throw const CoreIpcException(
        'identity_model_options.list response payload is invalid',
      );
    }
    try {
      return (payload['options'] as List)
          .whereType<Map<String, dynamic>>()
          .map(IdentityModelOptionMetadata.fromJson)
          .toList(growable: false);
    } on FormatException catch (error) {
      throw CoreIpcException(error.message.toString());
    }
  }

  Future<ModelSelectionSnapshotResult> queryModelSelectionSnapshot({
    required String sessionId,
    required String executionRunId,
  }) async {
    _requireNonEmpty('executionRunId', executionRunId);
    final response = await request({
      'kind': 'query',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': _requestId('model-selection-snapshot'),
      'sessionId': sessionId,
      'query': 'model_selection.snapshot',
      'payload': {'executionRunId': executionRunId},
    });
    return ModelSelectionSnapshotResult.fromResponse(response);
  }

  Future<List<ConnectorProfileMetadata>> queryConnectorProfiles({
    required String sessionId,
    String scopeId = 'desktop',
    String? connectorId,
    int limit = 100,
  }) async {
    final payload = <String, dynamic>{'scopeId': scopeId, 'limit': limit};
    if (connectorId != null) payload['connectorId'] = connectorId;
    final response = await request({
      'kind': 'query',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': _requestId('connector-query'),
      'sessionId': sessionId,
      'query': 'connector.query',
      'payload': payload,
    });
    final responsePayload = response['payload'];
    if (responsePayload is! Map<String, dynamic> ||
        responsePayload['scopeId'] != scopeId ||
        responsePayload['connectorProfiles'] is! List) {
      throw const CoreIpcException(
        'connector.query response payload is invalid',
      );
    }
    return (responsePayload['connectorProfiles'] as List)
        .whereType<Map<String, dynamic>>()
        .map(ConnectorProfileMetadata.fromJson)
        .toList(growable: false);
  }

  Future<ConnectorHealthResult> queryConnectorHealth({
    required String sessionId,
    required String scopeId,
    required String connectorId,
  }) async {
    _requireNonEmpty('scopeId', scopeId);
    _requireNonEmpty('connectorId', connectorId);
    final response = await request({
      'kind': 'query',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': _requestId('connector-health'),
      'sessionId': sessionId,
      'query': 'connector.health',
      'payload': {'scopeId': scopeId, 'connectorId': connectorId},
    });
    return ConnectorHealthResult.fromResponse(
      response,
      expectedScopeId: scopeId,
    );
  }

  Future<LocalConnectorDiscoveryResult> discoverLocalConnectors({
    required String sessionId,
  }) =>
      _queryLocalDiscoveries(sessionId: sessionId, query: 'connector.discover');

  Future<LocalConnectorDiscoveryResult> scanLocalAgents({
    required String sessionId,
  }) => _queryLocalDiscoveries(sessionId: sessionId, query: 'agent.scan_local');

  Future<LocalConnectorDiscoveryResult> _queryLocalDiscoveries({
    required String sessionId,
    required String query,
  }) async {
    final response = await request({
      'kind': 'query',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': _requestId(query.replaceAll('.', '-')),
      'sessionId': sessionId,
      'query': query,
      'payload': <String, dynamic>{},
    });
    return LocalConnectorDiscoveryResult.fromResponse(response, query);
  }

  // ---- W5/W6 typed local discovery & import surface ------------------------

  /// Starts one passive local discovery scan and returns the scan id plus the
  /// `local-discovery-events` epoch used for subscribe/replay.
  Future<DiscoveryStartResult> discoveryStart({
    required String sessionId,
    required String requestId,
  }) async {
    final response = await request({
      'kind': 'command',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': requestId,
      'sessionId': sessionId,
      'command': 'agent.discovery.start',
      'payload': <String, dynamic>{},
    });
    return DiscoveryStartResult.fromResponse(response);
  }

  Future<DiscoverySnapshot> discoverySnapshot({
    required String sessionId,
    required String requestId,
    required String scanId,
  }) async {
    _requireNonEmpty('scanId', scanId);
    final response = await request({
      'kind': 'query',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': requestId,
      'sessionId': sessionId,
      'query': 'agent.discovery.snapshot',
      'payload': {'scanId': scanId},
    });
    return DiscoverySnapshot.fromResponse(response);
  }

  Future<DiscoveryVerifyResult> discoveryVerify({
    required String sessionId,
    required String requestId,
    required String scanId,
    required String candidateId,
    required bool consent,
    Duration? deadline,
  }) async {
    _requireNonEmpty('scanId', scanId);
    _requireNonEmpty('candidateId', candidateId);
    final response = await request({
      'kind': 'command',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': requestId,
      'sessionId': sessionId,
      'command': 'agent.discovery.verify',
      'payload': {
        'scanId': scanId,
        'candidateId': candidateId,
        'consent': consent,
        if (deadline != null)
          'deadlineMs': deadline.inMilliseconds.clamp(100, 30000),
      },
    });
    return DiscoveryVerifyResult.fromResponse(response);
  }

  Future<DiscoveryDismissResult> discoveryDismiss({
    required String sessionId,
    required String requestId,
    required String scanId,
    required String candidateId,
  }) async {
    _requireNonEmpty('scanId', scanId);
    _requireNonEmpty('candidateId', candidateId);
    final response = await request({
      'kind': 'command',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': requestId,
      'sessionId': sessionId,
      'command': 'agent.discovery.dismiss',
      'payload': {'scanId': scanId, 'candidateId': candidateId},
    });
    return DiscoveryDismissResult.fromResponse(response);
  }

  /// Read-only import plan. Only the allowlisted business fields are sent;
  /// no plan, binding, or fingerprint material is ever echoed back. The
  /// response is validated against the request intent: any mismatch of
  /// scanId/candidateId/targetProjectId/modelSelection fails closed.
  Future<ImportPlan> importPlan({
    required String sessionId,
    required String requestId,
    required String scanId,
    required String candidateId,
    required String projectId,
    String? modelSelection,
  }) async {
    _requireNonEmpty('scanId', scanId);
    _requireNonEmpty('candidateId', candidateId);
    _requireNonEmpty('projectId', projectId);
    final response = await request({
      'kind': 'query',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': requestId,
      'sessionId': sessionId,
      'query': 'agent.import.plan',
      'payload': {
        'scanId': scanId,
        'candidateId': candidateId,
        'projectId': projectId,
        'modelSelection': modelSelection,
      },
    });
    final plan = ImportPlan.fromResponse(response);
    if (plan.scanId != scanId ||
        plan.candidateId != candidateId ||
        plan.targetProjectId != projectId ||
        plan.modelSelection != modelSelection) {
      throw const CoreIpcException(
        'agent.import.plan response does not match the requested import intent',
      );
    }
    return plan;
  }

  /// Atomic local Agent import. Submits only scanId/candidateId/projectId/
  /// modelSelection; `modelSelection:null` is the legal connector-default.
  Future<LocalAgentImportResult> importLocal({
    required String sessionId,
    required String requestId,
    required String scanId,
    required String candidateId,
    required String projectId,
    String? modelSelection,
  }) async {
    _requireNonEmpty('scanId', scanId);
    _requireNonEmpty('candidateId', candidateId);
    _requireNonEmpty('projectId', projectId);
    final response = await request({
      'kind': 'command',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': requestId,
      'sessionId': sessionId,
      'command': 'agent.import_local',
      'payload': {
        'scanId': scanId,
        'candidateId': candidateId,
        'projectId': projectId,
        'modelSelection': modelSelection,
      },
    });
    return LocalAgentImportResult.fromResponse(response);
  }

  /// Replays `local-discovery-events` after a cursor; returns typed event
  /// envelopes validated against the discovery stream id.
  Future<List<EventEnvelope>> discoveryReplay({
    required String sessionId,
    required String requestId,
    required String epoch,
    int afterSequence = 0,
    int limit = 64,
  }) async {
    if (epoch.trim().isEmpty || afterSequence < 0 || limit <= 0) {
      throw const CoreIpcException(
        'local discovery event replay cursor is invalid',
      );
    }
    final response = await request({
      'kind': 'query',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': requestId,
      'sessionId': sessionId,
      'query': 'events.replay',
      'payload': {
        'streamId': localDiscoveryEventStreamId,
        'epoch': epoch,
        'afterSequence': afterSequence,
        'limit': limit.clamp(1, 256),
      },
    });
    final payload = response['payload'];
    if (payload is! Map<String, dynamic>) {
      throw const CoreIpcException(
        'local discovery event replay payload is invalid',
      );
    }
    final events = payload['events'];
    if (events is! List) {
      throw const CoreIpcException(
        'local discovery event replay list is invalid',
      );
    }
    final typed = <EventEnvelope>[];
    for (final event in events) {
      if (event is! Map<String, dynamic>) {
        throw const CoreIpcException(
          'local discovery event replay entry is invalid',
        );
      }
      final envelope = EventEnvelope.fromJson(event);
      if (envelope.cursor.streamId != localDiscoveryEventStreamId) {
        throw const CoreIpcException(
          'local discovery event replay stream mismatch',
        );
      }
      typed.add(envelope);
    }
    return typed;
  }

  /// Subscribes to `local-discovery-events` after the given discovery epoch
  /// (obtained from the `agent.discovery.start` response). ACK and unsubscribe
  /// use the regular `events.ack`/`events.unsubscribe` commands bound to the
  /// returned subscription.
  Future<CoreEventSubscription> subscribeDiscoveryEvents({
    required String sessionId,
    required String epoch,
    int maxInFlightEvents = 64,
    int maxInFlightBytes = 262144,
  }) {
    return subscribeEvents(
      sessionId: sessionId,
      afterCursor: StreamCursor(
        streamId: localDiscoveryEventStreamId,
        sequence: 0,
        epoch: epoch,
      ),
      maxInFlightEvents: maxInFlightEvents,
      maxInFlightBytes: maxInFlightBytes,
    );
  }

  Future<ConnectorProfileMutationResult> createConnectorProfile({
    required String sessionId,
    required ConnectorProfileMetadata profile,
  }) async {
    final response = await request({
      'kind': 'command',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': _requestId('connector-create'),
      'sessionId': sessionId,
      'command': 'connector.create',
      'payload': profile.toJson(),
    });
    return ConnectorProfileMutationResult.fromResponse(
      response,
      command: 'connector.create',
      changedKey: 'created',
      alreadyKey: 'alreadyPresent',
    );
  }

  Future<ConnectorProfileMutationResult> updateConnectorProfile({
    required String sessionId,
    required ConnectorProfileMetadata profile,
  }) async {
    final response = await request({
      'kind': 'command',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': _requestId('connector-update'),
      'sessionId': sessionId,
      'command': 'connector.update',
      'payload': profile.toJson(),
    });
    return ConnectorProfileMutationResult.fromResponse(
      response,
      command: 'connector.update',
      changedKey: 'updated',
      alreadyKey: 'alreadyCurrent',
    );
  }

  Future<ConnectorProfileRemoveResult> removeConnectorProfile({
    required String sessionId,
    required String connectorId,
    String scopeId = 'desktop',
  }) async {
    _requireNonEmpty('connectorId', connectorId);
    final response = await request({
      'kind': 'command',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': _requestId('connector-remove'),
      'sessionId': sessionId,
      'command': 'connector.remove',
      'payload': {'scopeId': scopeId, 'connectorId': connectorId},
    });
    return ConnectorProfileRemoveResult.fromResponse(response);
  }

  Future<List<Map<String, dynamic>>> queryRetrievalSources({
    required String sessionId,
    required String scopeId,
    List<String>? sourceIds,
    int limit = 20,
  }) async {
    final payload = <String, dynamic>{'scopeId': scopeId, 'limit': limit};
    if (sourceIds != null) payload['sourceIds'] = sourceIds;
    final response = await request({
      'kind': 'query',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': 'retrieval-query-${DateTime.now().microsecondsSinceEpoch}',
      'sessionId': sessionId,
      'query': 'retrieval.query',
      'payload': payload,
    });
    final responsePayload = response['payload'];
    if (responsePayload is! Map<String, dynamic>) {
      throw const CoreIpcException('Core retrieval query payload is invalid');
    }
    final results = responsePayload['retrievalSources'];
    if (results is! List) {
      throw const CoreIpcException('Core retrieval query result is invalid');
    }
    return results.whereType<Map<String, dynamic>>().toList(growable: false);
  }

  Future<RetrievalPreviewResult> queryRetrievalPreview({
    required String sessionId,
    required String? project,
    required String? conversation,
    required String? agent,
    required String query,
    required String scope,
    required List<String> sourceTypes,
    required int limit,
    String mode = 'exact',
  }) {
    return queryRetrievalPreviewRequest(
      sessionId: sessionId,
      previewRequest: RetrievalPreviewRequest(
        project: project,
        conversation: conversation,
        agent: agent,
        query: query,
        scope: scope,
        sourceTypes: sourceTypes,
        limit: limit,
        mode: mode,
      ),
    );
  }

  Future<RetrievalPreviewResult> queryRetrievalPreviewRequest({
    required String sessionId,
    required RetrievalPreviewRequest previewRequest,
  }) async {
    final Map<String, dynamic> payload;
    try {
      payload = previewRequest.toJson();
    } on FormatException catch (error) {
      throw CoreIpcException(error.message.toString());
    }
    final response = await request({
      'kind': 'query',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': _requestId('retrieval-preview'),
      'sessionId': sessionId,
      'query': 'retrieval.preview',
      'payload': payload,
    });
    try {
      return RetrievalPreviewResult.fromResponse(response);
    } on FormatException catch (error) {
      throw CoreIpcException(error.message.toString());
    }
  }

  Future<Map<String, dynamic>> storeRetrievalSelection({
    required String sessionId,
    required String selectionId,
    required String scope,
    required String scopeId,
    required String projectId,
    String? conversationId,
    int scopeRevision = 0,
    int? workspaceRevision,
    required String retrievalVersion,
    required String queryHash,
    required List<Map<String, dynamic>> items,
  }) async {
    for (final entry in <String, String>{
      'selectionId': selectionId,
      'scope': scope,
      'scopeId': scopeId,
      'projectId': projectId,
      'retrievalVersion': retrievalVersion,
      'queryHash': queryHash,
    }.entries) {
      _requireNonEmpty(entry.key, entry.value);
    }
    if (items.isEmpty) {
      throw const CoreIpcException(
        'retrieval.select requires at least one selected source',
      );
    }
    final payload = <String, dynamic>{
      'selectionId': selectionId,
      'scope': scope,
      'scopeId': scopeId,
      'projectId': projectId,
      'conversationId': conversationId,
      'scopeRevision': scopeRevision,
      'workspaceRevision': workspaceRevision,
      'retrievalVersion': retrievalVersion,
      'queryHash': queryHash,
      'items': items,
    };
    final response = await request({
      'kind': 'command',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': _requestId('retrieval-select'),
      'sessionId': sessionId,
      'command': 'retrieval.select',
      'payload': payload,
    });
    final responsePayload = response['payload'];
    if (responsePayload is! Map<String, dynamic> ||
        responsePayload['projection'] is! Map<String, dynamic>) {
      throw const CoreIpcException(
        'retrieval.select response payload is invalid',
      );
    }
    return responsePayload;
  }

  Future<List<Map<String, dynamic>>> queryRetrievalSelections({
    required String sessionId,
    required String scopeId,
    List<String>? selectionIds,
    int limit = 20,
  }) async {
    final payload = <String, dynamic>{'scopeId': scopeId, 'limit': limit};
    if (selectionIds != null) payload['selectionIds'] = selectionIds;
    final response = await request({
      'kind': 'query',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': _requestId('retrieval-selections-query'),
      'sessionId': sessionId,
      'query': 'retrieval.selections',
      'payload': payload,
    });
    final responsePayload = response['payload'];
    if (responsePayload is! Map<String, dynamic> ||
        responsePayload['retrievalSelections'] is! List) {
      throw const CoreIpcException(
        'retrieval.selections response payload is invalid',
      );
    }
    return (responsePayload['retrievalSelections'] as List)
        .whereType<Map<String, dynamic>>()
        .toList(growable: false);
  }

  Future<Map<String, dynamic>> storeRetrievalFeedback({
    required String sessionId,
    required String feedbackId,
    required String selectionId,
    required String scopeId,
    required String sourceId,
    required String label,
    required String reason,
    int createdAtMs = 0,
  }) async {
    for (final entry in <String, String>{
      'feedbackId': feedbackId,
      'selectionId': selectionId,
      'scopeId': scopeId,
      'sourceId': sourceId,
      'label': label,
      'reason': reason,
    }.entries) {
      _requireNonEmpty(entry.key, entry.value);
    }
    final response = await request({
      'kind': 'command',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': _requestId('retrieval-feedback'),
      'sessionId': sessionId,
      'command': 'retrieval.feedback',
      'payload': {
        'feedbackId': feedbackId,
        'selectionId': selectionId,
        'scopeId': scopeId,
        'sourceId': sourceId,
        'label': label,
        'reason': reason,
        'createdAtMs': createdAtMs,
      },
    });
    final responsePayload = response['payload'];
    if (responsePayload is! Map<String, dynamic> ||
        responsePayload['projection'] is! Map<String, dynamic>) {
      throw const CoreIpcException(
        'retrieval.feedback response payload is invalid',
      );
    }
    return responsePayload;
  }

  Future<List<Map<String, dynamic>>> queryRetrievalFeedback({
    required String sessionId,
    required String scopeId,
    String? selectionId,
    int limit = 20,
  }) async {
    final payload = <String, dynamic>{'scopeId': scopeId, 'limit': limit};
    if (selectionId != null) payload['selectionId'] = selectionId;
    final response = await request({
      'kind': 'query',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': _requestId('retrieval-feedback-query'),
      'sessionId': sessionId,
      'query': 'retrieval.feedback',
      'payload': payload,
    });
    final responsePayload = response['payload'];
    if (responsePayload is! Map<String, dynamic> ||
        responsePayload['retrievalFeedback'] is! List) {
      throw const CoreIpcException(
        'retrieval.feedback query response payload is invalid',
      );
    }
    return (responsePayload['retrievalFeedback'] as List)
        .whereType<Map<String, dynamic>>()
        .toList(growable: false);
  }

  Future<Map<String, dynamic>> exportProjectConfig({
    required String sessionId,
    required String projectId,
  }) async {
    _requireNonEmpty('projectId', projectId);
    final response = await request({
      'kind': 'command',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': _requestId('config-export'),
      'sessionId': sessionId,
      'command': 'config.export',
      'payload': {'projectId': projectId},
    });
    final payload = response['payload'];
    if (payload is! Map<String, dynamic> ||
        payload['config'] is! Map<String, dynamic>) {
      throw const CoreIpcException('config.export response payload is invalid');
    }
    return payload['config'] as Map<String, dynamic>;
  }

  Future<ConfigImportResult> importProjectConfig({
    required String sessionId,
    required Map<String, dynamic> config,
  }) async {
    if (config.isEmpty) {
      throw const CoreIpcException('config.import requires a non-empty config');
    }
    final response = await request({
      'kind': 'command',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': _requestId('config-import'),
      'sessionId': sessionId,
      'command': 'config.import',
      'payload': {'config': config},
    });
    return ConfigImportResult.fromResponse(response);
  }

  Future<CollaborationCreateResult> createCollaboration({
    required String sessionId,
    required String projectId,
    required String collaborationRunId,
    required List<String> rootAgentIds,
    int maxCalls = 1,
    int maxDepth = 1,
    String status = 'pending',
    String? stopReason,
    bool autoDispatchHandoffs = false,
  }) async {
    _requireNonEmpty('projectId', projectId);
    _requireNonEmpty('collaborationRunId', collaborationRunId);
    if (rootAgentIds.isEmpty || rootAgentIds.any((id) => id.isEmpty)) {
      throw const CoreIpcException(
        'collaboration.create requires non-empty rootAgentIds',
      );
    }
    if (maxCalls <= 0 || maxDepth <= 0) {
      throw const CoreIpcException(
        'collaboration.create maxCalls and maxDepth must be positive',
      );
    }
    _requireNonEmpty('status', status);
    final payload = <String, dynamic>{
      'projectId': projectId,
      'collaborationRunId': collaborationRunId,
      'rootAgentIds': rootAgentIds,
      'maxCalls': maxCalls,
      'maxDepth': maxDepth,
      'status': status,
      'autoDispatchHandoffs': autoDispatchHandoffs,
    };
    if (stopReason != null) payload['stopReason'] = stopReason;
    final response = await request({
      'kind': 'command',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': _requestId('collaboration-create'),
      'sessionId': sessionId,
      'command': 'collaboration.create',
      'payload': payload,
    });
    return CollaborationCreateResult.fromResponse(response);
  }

  Future<ExecutionStartResult> startExecution({
    required String sessionId,
    required String executionRunId,
    required String collaborationRunId,
    required String projectId,
    required String conversationId,
    required String agentId,
    required String currentTask,
    String workspaceAccess = 'none',
    String? canonicalCwd,
    String? connectorId,
    String? modelId,
    int? deadlineMs,
  }) async {
    for (final entry in <String, String>{
      'executionRunId': executionRunId,
      'collaborationRunId': collaborationRunId,
      'projectId': projectId,
      'conversationId': conversationId,
      'agentId': agentId,
      'currentTask': currentTask,
    }.entries) {
      _requireNonEmpty(entry.key, entry.value);
    }
    if (connectorId != null) _requireNonEmpty('connectorId', connectorId);
    if (modelId != null) _requireNonEmpty('modelId', modelId);
    if (deadlineMs != null && (deadlineMs < 1 || deadlineMs > 3600000)) {
      throw const CoreIpcException(
        'execution.start deadlineMs is outside the supported bound',
      );
    }
    final payload = <String, dynamic>{
      'executionRunId': executionRunId,
      'collaborationRunId': collaborationRunId,
      'projectId': projectId,
      'conversationId': conversationId,
      'agentId': agentId,
      'currentTask': currentTask,
      'workspaceAccess': workspaceAccess,
      ...?canonicalCwd == null
          ? null
          : <String, dynamic>{'canonicalCwd': canonicalCwd},
      ...?connectorId == null
          ? null
          : <String, dynamic>{'connectorId': connectorId},
      ...?modelId == null ? null : <String, dynamic>{'modelId': modelId},
    };
    final response = await request({
      'kind': 'command',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': _requestId('execution-start'),
      'sessionId': sessionId,
      'command': 'execution.start',
      'payload': payload,
      ...?deadlineMs == null
          ? null
          : <String, dynamic>{'deadlineMs': deadlineMs},
    });
    return ExecutionStartResult.fromResponse(response);
  }

  Future<ExecutionRetryResult> retryExecution({
    required String sessionId,
    required String executionRunId,
    required String sourceExecutionRunId,
    required String currentTask,
    String? connectorId,
    String? modelId,
    int? deadlineMs,
  }) async {
    for (final entry in <String, String>{
      'executionRunId': executionRunId,
      'sourceExecutionRunId': sourceExecutionRunId,
      'currentTask': currentTask,
    }.entries) {
      _requireNonEmpty(entry.key, entry.value);
    }
    if (connectorId != null) _requireNonEmpty('connectorId', connectorId);
    if (modelId != null) _requireNonEmpty('modelId', modelId);
    if (deadlineMs != null && (deadlineMs < 1 || deadlineMs > 3600000)) {
      throw const CoreIpcException(
        'execution.retry deadlineMs is outside the supported bound',
      );
    }
    final payload = <String, dynamic>{
      'executionRunId': executionRunId,
      'sourceExecutionRunId': sourceExecutionRunId,
      'currentTask': currentTask,
      ...?connectorId == null
          ? null
          : <String, dynamic>{'connectorId': connectorId},
      ...?modelId == null ? null : <String, dynamic>{'modelId': modelId},
    };
    final response = await request({
      'kind': 'command',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': _requestId('execution-retry'),
      'sessionId': sessionId,
      'command': 'execution.retry',
      'payload': payload,
      ...?deadlineMs == null
          ? null
          : <String, dynamic>{'deadlineMs': deadlineMs},
    });
    return ExecutionRetryResult.fromResponse(response);
  }

  Future<ExecutionRetryResult> rerunCurrentExecution({
    required String sessionId,
    required String executionRunId,
    required String sourceExecutionRunId,
    required String currentTask,
    String? connectorId,
    String? modelId,
    int? deadlineMs,
  }) async {
    for (final entry in <String, String>{
      'executionRunId': executionRunId,
      'sourceExecutionRunId': sourceExecutionRunId,
      'currentTask': currentTask,
    }.entries) {
      _requireNonEmpty(entry.key, entry.value);
    }
    if (connectorId != null) _requireNonEmpty('connectorId', connectorId);
    if (modelId != null) _requireNonEmpty('modelId', modelId);
    if (deadlineMs != null && (deadlineMs < 1 || deadlineMs > 3600000)) {
      throw const CoreIpcException(
        'execution.rerun_current deadlineMs is outside the supported bound',
      );
    }
    final payload = <String, dynamic>{
      'executionRunId': executionRunId,
      'sourceExecutionRunId': sourceExecutionRunId,
      'currentTask': currentTask,
      ...?connectorId == null
          ? null
          : <String, dynamic>{'connectorId': connectorId},
      ...?modelId == null ? null : <String, dynamic>{'modelId': modelId},
    };
    final response = await request({
      'kind': 'command',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': _requestId('execution-rerun-current'),
      'sessionId': sessionId,
      'command': 'execution.rerun_current',
      'payload': payload,
      ...?deadlineMs == null
          ? null
          : <String, dynamic>{'deadlineMs': deadlineMs},
    });
    return ExecutionRetryResult.fromResponse(response);
  }

  Future<HandoffCreateResult> createHandoff({
    required String sessionId,
    required String handoffId,
    required String collaborationRunId,
    required String fromExecutionRunId,
    required String sourceMessageId,
    required String fromAgentId,
    required String toAgentId,
    required String task,
    String kind = 'task',
    String dispatchMode = 'sequential',
    String detectedBy = 'ui_explicit',
    String? reason,
    String? contextScope,
    String status = 'proposed',
  }) async {
    _requireNonEmpty('handoffId', handoffId);
    _requireNonEmpty('collaborationRunId', collaborationRunId);
    _requireNonEmpty('fromExecutionRunId', fromExecutionRunId);
    _requireNonEmpty('sourceMessageId', sourceMessageId);
    _requireNonEmpty('fromAgentId', fromAgentId);
    _requireNonEmpty('toAgentId', toAgentId);
    _requireNonEmpty('task', task);
    _requireNonEmpty('status', status);
    final response = await request({
      'kind': 'command',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': _requestId('handoff-create'),
      'sessionId': sessionId,
      'command': 'handoff.create',
      'payload': {
        'handoffId': handoffId,
        'collaborationRunId': collaborationRunId,
        'fromExecutionRunId': fromExecutionRunId,
        'sourceMessageId': sourceMessageId,
        'fromAgentId': fromAgentId,
        'toAgentId': toAgentId,
        'status': status,
        'details': {
          'parentExecutionRunId': fromExecutionRunId,
          'sourceMessageId': sourceMessageId,
          'fromAgentId': fromAgentId,
          'toAgentId': toAgentId,
          'kind': kind,
          'dispatchMode': dispatchMode,
          'detectedBy': detectedBy,
          'task': task,
          if (reason != null && reason.trim().isNotEmpty) 'reason': reason,
          if (contextScope != null && contextScope.trim().isNotEmpty)
            'contextScope': contextScope,
        },
      },
    });
    return HandoffCreateResult.fromResponse(response);
  }

  Future<HandoffDispatchResult> dispatchHandoff({
    required String sessionId,
    required String handoffId,
    bool startRuntime = true,
  }) async {
    _requireNonEmpty('handoffId', handoffId);
    final response = await request({
      'kind': 'command',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': _requestId('handoff-dispatch'),
      'sessionId': sessionId,
      'command': 'handoff.dispatch',
      'payload': {'handoffId': handoffId, 'startRuntime': startRuntime},
    });
    return HandoffDispatchResult.fromResponse(response);
  }

  Future<HandoffTransitionResult> transitionHandoff({
    required String sessionId,
    required String handoffId,
    required String targetStatus,
  }) async {
    _requireNonEmpty('handoffId', handoffId);
    const allowedStatuses = {'approved', 'rejected', 'cancelled'};
    if (!allowedStatuses.contains(targetStatus)) {
      throw const CoreIpcException('unsupported Handoff transition status');
    }
    final commandName = switch (targetStatus) {
      'approved' => 'approve',
      'rejected' => 'reject',
      'cancelled' => 'cancel',
      _ => throw const CoreIpcException(
        'unsupported Handoff transition status',
      ),
    };
    final response = await request({
      'kind': 'command',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': _requestId('handoff-$commandName'),
      'sessionId': sessionId,
      'command': 'handoff.$commandName',
      'payload': {'handoffId': handoffId},
    });
    return HandoffTransitionResult.fromResponse(response);
  }

  Future<void> reconnect({
    required String sessionId,
    String? sessionCredential,
    StreamCursor? lastSeen,
  }) async {
    final pipeName = _pipeName;
    if (pipeName == null) {
      throw const CoreIpcException('Core IPC reconnect requires a Named Pipe');
    }
    if (_closeRequested) {
      throw const CoreIpcException('Core IPC client is closed');
    }
    if (_activeSubscription != null ||
        _subscriptionStarting ||
        _pendingResponses.isNotEmpty) {
      throw const CoreIpcException(
        'Core IPC reconnect requires an idle client without a subscription',
      );
    }
    final replacement = await CoreIpcClient.connect(
      pipeName: pipeName,
      maximumBytes: maximumBytes,
    );
    final previous = _transport;
    try {
      await replacement.handshake(
        sessionId: sessionId,
        sessionCredential: sessionCredential ?? this.sessionCredential,
        lastSeen: lastSeen,
      );
    } catch (_) {
      await replacement._transport.close();
      rethrow;
    }
    _readerStopRequested = true;
    await previous.close();
    _transport = replacement._transport;
    _readerFuture = replacement._readerFuture;
    _readerError = replacement._readerError;
    _readerStopRequested = replacement._readerStopRequested;
    _sessionId = replacement._sessionId;
    _serverEpoch = replacement._serverEpoch;
  }

  Future<Map<String, dynamic>> _requestUnlocked(
    Map<String, dynamic> envelope,
    String expectedRequestId, {
    bool allowDuringClose = false,
  }) async {
    if (_closeRequested && !allowDuringClose) {
      throw const CoreIpcException(
        'Core IPC client is closed',
        code: _coreIpcClosedCode,
      );
    }
    final frame = _encodeFrame(envelope);
    if (_pendingResponses.containsKey(expectedRequestId)) {
      throw CoreIpcException(
        'Core IPC requestId is already pending: $expectedRequestId',
      );
    }
    final pending = Completer<Map<String, dynamic>>();
    _pendingResponses[expectedRequestId] = pending;
    try {
      await _transport.write(frame);
      _ensureReader();
      final decoded = await pending.future;
      _validateResponse(decoded, expectedRequestId);
      if (decoded['kind'] == 'error') {
        final error = IpcErrorEnvelope.fromJson(decoded);
        throw CoreIpcException(
          '${error.code}: ${error.message}',
          code: error.code,
          retryable: error.retryable,
          details: error.details,
        );
      }
      return decoded;
    } catch (_) {
      final current = _pendingResponses[expectedRequestId];
      if (identical(current, pending)) {
        _pendingResponses.remove(expectedRequestId);
      }
      rethrow;
    }
  }

  String _expectedResponseRequestId(Map<String, dynamic> envelope) {
    if (envelope['kind'] == 'handshake') return 'handshake';
    final requestId = envelope['requestId'];
    if (requestId is! String || requestId.isEmpty) {
      throw const CoreIpcException('Core IPC request is missing requestId');
    }
    return requestId;
  }

  void _validateResponse(
    Map<String, dynamic> response,
    String expectedRequestId,
  ) {
    final protocol = response['protocol'];
    if (protocol is! Map<String, dynamic> ||
        protocol['major'] != protocolMajor) {
      throw const CoreIpcException(
        'Core IPC response has an unsupported protocol major',
      );
    }
    final requestId = response['requestId'];
    if (requestId is! String || requestId != expectedRequestId) {
      throw CoreIpcException(
        'Core IPC response requestId mismatch: expected $expectedRequestId, got $requestId',
      );
    }
    final kind = response['kind'];
    if (kind != 'response' && kind != 'error') {
      throw CoreIpcException('Core IPC response kind is unsupported: $kind');
    }
  }

  Uint8List _encodeFrame(Map<String, dynamic> value) {
    final payload = Uint8List.fromList(utf8.encode(jsonEncode(value)));
    if (payload.length > maximumBytes) {
      throw const CoreIpcException('Core IPC message exceeds maximum size');
    }
    final frame = Uint8List(payload.length + 4);
    final length = payload.length;
    frame[0] = (length >> 24) & 0xff;
    frame[1] = (length >> 16) & 0xff;
    frame[2] = (length >> 8) & 0xff;
    frame[3] = length & 0xff;
    frame.setRange(4, frame.length, payload);
    return frame;
  }

  void _ensureReader() {
    if (_readerFuture != null) return;
    if (_readerStopRequested) {
      throw const CoreIpcException('Core IPC reader is stopped');
    }
    final reader = _readLoop();
    _readerFuture = reader;
    unawaited(reader);
  }

  Future<void> _readLoop() async {
    try {
      while (!_readerStopRequested) {
        final payload = await _readFrame();
        final decoded = jsonDecode(utf8.decode(payload));
        if (decoded is! Map<String, dynamic>) {
          throw const CoreIpcException('Core IPC frame is not a JSON object');
        }
        final kind = decoded['kind'];
        if (kind == 'event') {
          _handleEventFrame(decoded);
          continue;
        }
        if (kind != 'response' && kind != 'error') {
          throw CoreIpcException('Core IPC frame kind is unsupported: $kind');
        }
        final requestId = decoded['requestId'];
        if (requestId is! String || requestId.isEmpty) {
          throw const CoreIpcException(
            'Core IPC response is missing requestId',
          );
        }
        var pending = _pendingResponses.remove(requestId);
        if (pending == null && _pendingResponses.length == 1) {
          // Preserve the requestId mismatch diagnostic for the sole in-flight
          // request. Event frames never enter this branch.
          final expectedRequestId = _pendingResponses.keys.single;
          pending = _pendingResponses.remove(expectedRequestId);
        }
        if (pending == null) {
          throw CoreIpcException(
            'Core IPC response has no pending request: $requestId',
          );
        }
        if (!pending.isCompleted) pending.complete(decoded);
      }
    } catch (error, stackTrace) {
      if (!_readerStopRequested) _failReader(error, stackTrace);
    }
  }

  void _failReader(Object error, StackTrace stackTrace) {
    final readerError = error is CoreIpcException
        ? error
        : CoreIpcException('Core IPC reader failed: $error');
    _readerError = readerError;
    for (final pending in _pendingResponses.values) {
      if (!pending.isCompleted) pending.completeError(readerError, stackTrace);
    }
    _pendingResponses.clear();
    final subscription = _activeSubscription;
    _activeSubscription = null;
    if (subscription != null && !subscription._controller.isClosed) {
      if (!_closeRequested) {
        subscription._controller.addError(readerError, stackTrace);
      }
      unawaited(subscription._controller.close());
      subscription._active = false;
    }
  }

  Future<Uint8List> _readFrame() async {
    final prefix = await _transport.read(4);
    if (prefix.length != 4) {
      throw const CoreIpcException(
        'Core IPC response length prefix is incomplete',
      );
    }
    final length =
        (prefix[0] << 24) | (prefix[1] << 16) | (prefix[2] << 8) | prefix[3];
    if (length < 0 || length > maximumBytes) {
      throw const CoreIpcException('Core IPC response length is invalid');
    }
    final payload = await _transport.read(length);
    if (payload.length != length) {
      throw const CoreIpcException('Core IPC response payload is incomplete');
    }
    return payload;
  }

  Future<T> _enqueue<T>(Future<T> Function() operation) {
    final result = _requestQueue.then<T>((_) => operation());
    _requestQueue = result.then<void>(
      (_) {},
      onError: (Object _, StackTrace _) {},
    );
    return result;
  }

  Future<void> close() {
    final existing = _closeFuture;
    if (existing != null) return existing;
    _closeRequested = true;
    final future = _closeImmediately().timeout(
      _coreIpcCloseTotalTimeout,
      onTimeout: () async {
        _readerStopRequested = true;
        try {
          await _transport.close();
        } catch (_) {}
        final process = _ownedProcess;
        if (process != null) {
          try {
            process.kill();
          } catch (_) {}
        }
      },
    );
    _closeFuture = future;
    return future;
  }

  Future<void> _closeImmediately() async {
    if (_closed) return;
    _closed = true;
    _cancelPendingResponses();
    final subscription = _activeSubscription;
    _activeSubscription = null;
    if (subscription != null) {
      subscription._active = false;
      if (!subscription._controller.isClosed) {
        unawaited(subscription._controller.close());
      }
    }
    final process = _ownedProcess;
    try {
      final sessionId = _sessionId;
      final credential = sessionCredential;
      if (process != null && sessionId != null && credential != null) {
        try {
          final shutdownRequestId =
              'shutdown-${DateTime.now().microsecondsSinceEpoch}';
          await _requestUnlocked(
            {
              'kind': 'command',
              'protocol': {'major': protocolMajor, 'minor': 0},
              'requestId': shutdownRequestId,
              'sessionId': sessionId,
              'command': 'shutdown_owned',
              'payload': <String, dynamic>{},
            },
            shutdownRequestId,
            allowDuringClose: true,
          ).timeout(const Duration(seconds: 2));
        } catch (_) {
          // The owned process is still terminated below if graceful shutdown fails.
        }
      }
    } finally {
      _readerStopRequested = true;
      try {
        await _transport.close();
      } catch (_) {}
      if (process != null) await _terminateOwnedProcess(process);
    }
  }

  void _cancelPendingResponses() {
    if (_pendingResponses.isEmpty) return;
    final error = const CoreIpcException(
      'Core IPC request cancelled because the client is closing',
      code: _coreIpcClosedCode,
    );
    final stackTrace = StackTrace.current;
    for (final pending in _pendingResponses.values) {
      if (!pending.isCompleted) pending.completeError(error, stackTrace);
    }
    _pendingResponses.clear();
  }
}

Future<int?> _pollProcessExit(Future<int> exitCode) {
  return Future.any<int?>([
    exitCode,
    Future<int?>.delayed(Duration.zero, () => null),
  ]);
}

void _throwIfCancelled(bool Function()? isCancelled) {
  if (isCancelled?.call() == true) {
    throw const CoreIpcException(
      'Core IPC startup cancelled because the application is closing',
      code: _coreIpcClosedCode,
    );
  }
}

class _BoundedProcessOutput {
  final StringBuffer _buffer = StringBuffer();
  var _byteCount = 0;
  var _truncated = false;

  void add(List<int> chunk) {
    if (_byteCount >= _coreStartupCaptureMaxBytes) {
      _truncated = true;
      return;
    }
    final remaining = _coreStartupCaptureMaxBytes - _byteCount;
    final accepted = chunk.length <= remaining
        ? chunk
        : chunk.sublist(0, remaining);
    _byteCount += accepted.length;
    _buffer.write(utf8.decode(accepted, allowMalformed: true));
    if (accepted.length != chunk.length) _truncated = true;
  }

  String get text {
    final value = _buffer.toString();
    return _truncated ? '$value...[truncated]' : value;
  }
}

Future<void> _drainProcessOutput(
  Stream<List<int>> stream,
  _BoundedProcessOutput capture,
) async {
  try {
    await for (final chunk in stream) {
      capture.add(chunk);
    }
  } catch (_) {
    // The owned process may close its stream while the Flutter client is
    // cleaning up. The bounded diagnostic already contains the useful bytes.
  }
}

Future<void> _awaitProcessOutput(
  Future<void> stdoutDrain,
  Future<void> stderrDrain,
) async {
  try {
    await Future.wait<void>(<Future<void>>[
      stdoutDrain,
      stderrDrain,
    ]).timeout(const Duration(milliseconds: 500));
  } catch (_) {
    // Diagnostics are best effort and must never extend Core shutdown.
  }
}

CoreIpcException _coreStartupException({
  required int exitCode,
  required String stdout,
  required String stderr,
}) {
  final marker = _parseCoreStartupMarker(stderr);
  final category = marker?.category ?? 'core_startup_failed';
  final message =
      _coreStartupUserMessages[category] ??
      _coreStartupUserMessages['core_startup_failed']!;
  final technical = <String>[
    if (stderr.trim().isNotEmpty) 'stderr: ${_redactProcessDiagnostic(stderr)}',
    if (stdout.trim().isNotEmpty) 'stdout: ${_redactProcessDiagnostic(stdout)}',
  ].join('\n');
  final details = <String, dynamic>{
    'category': category,
    'stage': marker?.stage ?? 'process_exit_before_named_pipe',
    'exitCode': exitCode,
    'technical': technical.isEmpty
        ? 'Core emitted no startup diagnostics.'
        : technical,
  };
  return CoreIpcException(
    message,
    code: category,
    retryable: category != 'database_schema_incompatible',
    details: details,
  );
}

class _CoreStartupMarker {
  const _CoreStartupMarker({required this.category, required this.stage});

  final String category;
  final String stage;
}

_CoreStartupMarker? _parseCoreStartupMarker(String output) {
  final pattern = RegExp(
    r'^AGENTTALK_CORE_STARTUP\s+category=([^\s]+)\s+stage=([^\s]+)\s+detail=.*$',
    multiLine: true,
  );
  final match = pattern.firstMatch(output);
  if (match == null) return null;
  final category = match.group(1)!;
  if (!_coreStartupUserMessages.containsKey(category)) return null;
  return _CoreStartupMarker(category: category, stage: match.group(2)!);
}

String _redactProcessDiagnostic(String value) {
  var result = value.replaceAll(RegExp(r'[\r\n]+'), ' ').trim();
  final localAppData = Platform.environment['LOCALAPPDATA'];
  if (localAppData != null && localAppData.isNotEmpty) {
    result = result.replaceAll(localAppData, r'%LOCALAPPDATA%');
  }
  final secretPattern = RegExp(
    r'(authorization|cookie|token|api[_-]?key|password|secret)\s*[:=]\s*\S+',
    caseSensitive: false,
  );
  result = result.replaceAllMapped(
    secretPattern,
    (match) => '${match.group(1)}=<redacted>',
  );
  if (result.length > 4096) {
    result = '${result.substring(0, 4096)}...[truncated]';
  }
  return result;
}

bool _isTransientPipeError(int errorCode) =>
    errorCode == 2 || // ERROR_FILE_NOT_FOUND
    errorCode == 121 || // ERROR_SEM_TIMEOUT
    errorCode == 231; // ERROR_PIPE_BUSY

abstract final class _NativePipeOperation {
  static const open = 0;
  static const read = 1;
  static const write = 2;
}

abstract final class _NativePipeResult {
  static const opened = 0;
  static const ok = 0;
  static const waitFailed = 1;
  static const openFailed = 2;
  static const timedOut = 3;
  static const failed = 4;
}

Future<List<int>> _runNativePipeOperation({
  required int operation,
  required List<Object> arguments,
  required Duration timeout,
}) async {
  final resultPort = ReceivePort();
  Isolate? worker;
  try {
    worker = await Isolate.spawn<List<Object?>>(
      _nativePipeOperationEntry,
      <Object?>[resultPort.sendPort, operation, ...arguments],
    );
    final result = await resultPort.first.timeout(timeout);
    if (result is! List) {
      throw const CoreIpcException(
        'AgentTalk Core Named Pipe native operation returned invalid data',
      );
    }
    return result.whereType<int>().toList(growable: false);
  } on TimeoutException {
    worker?.kill(priority: Isolate.immediate);
    return <int>[_NativePipeResult.timedOut, 258];
  } finally {
    resultPort.close();
  }
}

void _nativePipeOperationEntry(List<Object?> message) {
  final resultPort = message[0] as SendPort;
  final operation = message[1] as int;
  final api = _Win32PipeApi();
  if (operation == _NativePipeOperation.read) {
    final handle = message[2] as int;
    final length = message[3] as int;
    try {
      final bytes = _readExactNative(api, handle, length);
      resultPort.send(<int>[_NativePipeResult.ok, ...bytes]);
    } catch (error) {
      final errorCode = error is _Win32PipeException ? error.errorCode : 0;
      resultPort.send(<int>[_NativePipeResult.failed, errorCode]);
    }
    return;
  }
  if (operation == _NativePipeOperation.write) {
    final handle = message[2] as int;
    final bytes = (message[3] as List).cast<int>();
    try {
      _writeAllNative(api, handle, bytes);
      resultPort.send(<int>[_NativePipeResult.ok]);
    } catch (error) {
      final errorCode = error is _Win32PipeException ? error.errorCode : 0;
      resultPort.send(<int>[_NativePipeResult.failed, errorCode]);
    }
    return;
  }
  if (operation != _NativePipeOperation.open) {
    resultPort.send(<int>[_NativePipeResult.timedOut, 0]);
    return;
  }
  final pipeName = message[2] as String;
  final waitTimeoutMs = message[3] as int;
  final nativeName = pipeName.toNativeUtf16();
  try {
    if (api.waitNamedPipe(nativeName, waitTimeoutMs) == 0) {
      final errorCode = api.getLastError();
      resultPort.send(<int>[_NativePipeResult.waitFailed, errorCode]);
      return;
    }
    final handle = api.createFile(nativeName);
    if (handle == 0 || handle == -1) {
      final errorCode = api.getLastError();
      resultPort.send(<int>[_NativePipeResult.openFailed, errorCode]);
      return;
    }
    resultPort.send(<int>[_NativePipeResult.opened, handle]);
  } finally {
    calloc.free(nativeName);
  }
}

Uint8List _readExactNative(_Win32PipeApi api, int handle, int length) {
  final output = Uint8List(length);
  var offset = 0;
  while (offset < length) {
    final buffer = calloc<Uint8>(length - offset);
    final count = calloc<Uint32>();
    try {
      if (api.readFile(handle, buffer, length - offset, count, nullptr) == 0 ||
          count.value == 0) {
        throw const CoreIpcException('Core IPC read failed or pipe closed');
      }
      output.setRange(
        offset,
        offset + count.value,
        buffer.asTypedList(count.value),
      );
      offset += count.value;
    } finally {
      calloc.free(count);
      calloc.free(buffer);
    }
  }
  return output;
}

void _writeAllNative(_Win32PipeApi api, int handle, List<int> data) {
  var offset = 0;
  while (offset < data.length) {
    final buffer = calloc<Uint8>(data.length - offset);
    try {
      buffer.asTypedList(data.length - offset).setAll(0, data.sublist(offset));
      final count = _overlappedIo(
        api,
        handle,
        buffer,
        data.length - offset,
        write: true,
      );
      if (count == 0) throw const CoreIpcException('Core IPC pipe closed');
      offset += count;
    } finally {
      calloc.free(buffer);
    }
  }
}

int _overlappedIo(
  _Win32PipeApi api,
  int handle,
  Pointer<Uint8> buffer,
  int length, {
  required bool write,
}) {
  final event = api.createEvent();
  final overlapped = calloc<_Overlapped>();
  final transferred = calloc<Uint32>();
  try {
    overlapped.ref.hEvent = Pointer<Void>.fromAddress(event);
    final completedImmediately = write
        ? api.writeFile(handle, buffer, length, nullptr, overlapped.cast())
        : api.readFile(handle, buffer, length, nullptr, overlapped.cast());
    if (completedImmediately == 0) {
      final errorCode = api.getLastError();
      if (errorCode != 997) {
        throw _Win32PipeException(
          operation: write ? 'WriteFile' : 'ReadFile',
          errorCode: errorCode,
          message:
              'Core IPC ${write ? 'write' : 'read'} failed (Win32 $errorCode)',
        );
      }
    }
    final waitResult = api.waitForSingleObject(event, 5000);
    if (waitResult != 0) {
      api.cancelIo(handle, overlapped);
      api.waitForSingleObject(event, 5000);
      throw const CoreIpcException('Core IPC overlapped I/O timed out');
    }
    if (api.getOverlappedResult(handle, overlapped, transferred, 0) == 0) {
      final errorCode = api.getLastError();
      throw _Win32PipeException(
        operation: write
            ? 'GetOverlappedResult(WriteFile)'
            : 'GetOverlappedResult(ReadFile)',
        errorCode: errorCode,
        message: 'Core IPC overlapped I/O failed (Win32 $errorCode)',
      );
    }
    return transferred.value;
  } finally {
    calloc.free(transferred);
    calloc.free(overlapped);
    api.closeHandle(event);
  }
}

String _generateSessionCredential() {
  final random = Random.secure();
  final bytes = Uint8List.fromList(
    List<int>.generate(32, (_) => random.nextInt(256), growable: false),
  );
  return base64UrlEncode(bytes);
}

Future<bool> _waitForProcessExit(Process process, Duration timeout) async {
  try {
    await process.exitCode.timeout(timeout);
    return true;
  } on TimeoutException {
    return false;
  } catch (_) {
    return true;
  }
}

Future<void> _terminateOwnedProcess(Process process) async {
  if (await _waitForProcessExit(process, const Duration(seconds: 2))) return;
  // This is the final bounded fallback for a process owned by this client.
  // External clients never carry a Process here and therefore never reach it.
  process.kill();
  await _waitForProcessExit(process, const Duration(seconds: 2));
}

abstract class _CoreIpcTransport {
  Future<Uint8List> read(int length);
  Future<void> write(Uint8List data);
  Future<void> close();
}

class _CallbackCoreIpcTransport implements _CoreIpcTransport {
  _CallbackCoreIpcTransport({
    required this.readCallback,
    required this.writeCallback,
    required this.closeCallback,
  });

  final CoreIpcRead readCallback;
  final CoreIpcWrite writeCallback;
  final CoreIpcClose closeCallback;

  @override
  Future<Uint8List> read(int length) =>
      Future<Uint8List>.sync(() => readCallback(length));

  @override
  Future<void> write(Uint8List data) =>
      Future<void>.sync(() => writeCallback(data));

  @override
  Future<void> close() => Future<void>.sync(closeCallback);
}

class _Win32PipeTransport implements _CoreIpcTransport {
  _Win32PipeTransport(this._api, this._readHandle, this._writeHandle);

  final _Win32PipeApi _api;
  final int _readHandle;
  final int _writeHandle;
  bool _closed = false;

  @override
  Future<Uint8List> read(int length) async {
    final result = await _runNativePipeOperation(
      operation: _NativePipeOperation.read,
      arguments: <Object>[_readHandle, length],
      timeout: const Duration(seconds: 5),
    );
    if (result[0] == _NativePipeResult.ok) {
      return Uint8List.fromList(result.sublist(1));
    }
    final errorCode = result.length > 1 ? result[1] : 0;
    _abort();
    throw CoreIpcException(
      'Core IPC read failed or timed out (Win32 $errorCode)',
    );
  }

  @override
  Future<void> write(Uint8List data) {
    try {
      _writeAllNative(_api, _writeHandle, data.toList(growable: false));
      return Future<void>.value();
    } catch (_) {
      _abort();
      throw const CoreIpcException('Core IPC write failed');
    }
  }

  @override
  Future<void> close() async {
    if (_closed) return;
    _abort();
  }

  void _abort() {
    if (_closed) return;
    _closed = true;
    _api.closeHandle(_readHandle);
    _api.closeHandle(_writeHandle);
  }
}

final class _Overlapped extends Struct {
  @IntPtr()
  external int internalValue;

  @IntPtr()
  external int internalHigh;

  @Uint32()
  external int offset;

  @Uint32()
  external int offsetHigh;

  external Pointer<Void> hEvent;
}

typedef _CreateFileNative =
    IntPtr Function(
      Pointer<Utf16>,
      Uint32,
      Uint32,
      Pointer<Void>,
      Uint32,
      Uint32,
      IntPtr,
    );
typedef _CreateFileDart =
    int Function(Pointer<Utf16>, int, int, Pointer<Void>, int, int, int);
typedef _WaitNamedPipeNative = Int32 Function(Pointer<Utf16>, Uint32);
typedef _WaitNamedPipeDart = int Function(Pointer<Utf16>, int);
typedef _ReadWriteNative =
    Int32 Function(
      IntPtr,
      Pointer<Uint8>,
      Uint32,
      Pointer<Uint32>,
      Pointer<Void>,
    );
typedef _ReadWriteDart =
    int Function(int, Pointer<Uint8>, int, Pointer<Uint32>, Pointer<Void>);
typedef _CloseHandleNative = Int32 Function(IntPtr);
typedef _CloseHandleDart = int Function(int);
typedef _DuplicateHandleNative =
    Int32 Function(
      IntPtr,
      IntPtr,
      IntPtr,
      Pointer<IntPtr>,
      Uint32,
      Int32,
      Uint32,
    );
typedef _DuplicateHandleDart =
    int Function(int, int, int, Pointer<IntPtr>, int, int, int);
typedef _GetCurrentProcessNative = IntPtr Function();
typedef _GetCurrentProcessDart = int Function();
typedef _CreateEventNative =
    IntPtr Function(Pointer<Void>, Int32, Int32, Pointer<Utf16>);
typedef _CreateEventDart =
    int Function(Pointer<Void>, int, int, Pointer<Utf16>);
typedef _WaitForSingleObjectNative = Uint32 Function(IntPtr, Uint32);
typedef _WaitForSingleObjectDart = int Function(int, int);
typedef _GetOverlappedResultNative =
    Int32 Function(IntPtr, Pointer<_Overlapped>, Pointer<Uint32>, Int32);
typedef _GetOverlappedResultDart =
    int Function(int, Pointer<_Overlapped>, Pointer<Uint32>, int);
typedef _CancelIoExNative = Int32 Function(IntPtr, Pointer<_Overlapped>);
typedef _CancelIoExDart = int Function(int, Pointer<_Overlapped>);
typedef _GetLastErrorNative = Uint32 Function();
typedef _GetLastErrorDart = int Function();

class _Win32PipeApi {
  _Win32PipeApi() {
    final kernel32 = DynamicLibrary.open('kernel32.dll');
    _createFile = kernel32.lookupFunction<_CreateFileNative, _CreateFileDart>(
      'CreateFileW',
    );
    waitNamedPipe = kernel32
        .lookupFunction<_WaitNamedPipeNative, _WaitNamedPipeDart>(
          'WaitNamedPipeW',
        );
    readFile = kernel32.lookupFunction<_ReadWriteNative, _ReadWriteDart>(
      'ReadFile',
    );
    writeFile = kernel32.lookupFunction<_ReadWriteNative, _ReadWriteDart>(
      'WriteFile',
    );
    closeHandle = kernel32.lookupFunction<_CloseHandleNative, _CloseHandleDart>(
      'CloseHandle',
    );
    duplicateHandleNative = kernel32
        .lookupFunction<_DuplicateHandleNative, _DuplicateHandleDart>(
          'DuplicateHandle',
        );
    getCurrentProcess = kernel32
        .lookupFunction<_GetCurrentProcessNative, _GetCurrentProcessDart>(
          'GetCurrentProcess',
        );
    createEventNative = kernel32
        .lookupFunction<_CreateEventNative, _CreateEventDart>('CreateEventW');
    waitForSingleObject = kernel32
        .lookupFunction<_WaitForSingleObjectNative, _WaitForSingleObjectDart>(
          'WaitForSingleObject',
        );
    getOverlappedResult = kernel32
        .lookupFunction<_GetOverlappedResultNative, _GetOverlappedResultDart>(
          'GetOverlappedResult',
        );
    cancelIo = kernel32.lookupFunction<_CancelIoExNative, _CancelIoExDart>(
      'CancelIoEx',
    );
    getLastError = kernel32
        .lookupFunction<_GetLastErrorNative, _GetLastErrorDart>('GetLastError');
  }

  late final _WaitNamedPipeDart waitNamedPipe;
  late final _ReadWriteDart readFile;
  late final _ReadWriteDart writeFile;
  late final _CloseHandleDart closeHandle;
  late final _DuplicateHandleDart duplicateHandleNative;
  late final _GetCurrentProcessDart getCurrentProcess;
  late final _CreateEventDart createEventNative;
  late final _WaitForSingleObjectDart waitForSingleObject;
  late final _GetOverlappedResultDart getOverlappedResult;
  late final _CancelIoExDart cancelIo;
  late final _GetLastErrorDart getLastError;

  int createEvent() => createEventNative(nullptr, 1, 0, nullptr);

  int duplicateHandle(int source) {
    final target = calloc<IntPtr>();
    try {
      final process = getCurrentProcess();
      if (duplicateHandleNative(process, source, process, target, 0, 0, 2) ==
          0) {
        final errorCode = getLastError();
        throw _Win32PipeException(
          operation: 'DuplicateHandle',
          errorCode: errorCode,
          message:
              'AgentTalk Core Named Pipe handle duplication failed '
              '(Win32 $errorCode)',
        );
      }
      return target.value;
    } finally {
      calloc.free(target);
    }
  }

  int createFile(Pointer<Utf16> name) => _createFile(
    name,
    0x80000000 | 0x40000000,
    0x1 | 0x2,
    nullptr,
    3,
    0x40000000,
    0,
  );
  late final _CreateFileDart _createFile;
}
