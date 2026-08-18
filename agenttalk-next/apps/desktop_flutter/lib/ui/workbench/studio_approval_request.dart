import 'package:flutter/material.dart';

import '../../ipc/protocol_v1.dart';
import '../theme/studio_colors.dart';

enum StudioApprovalKind { tool, handoff }

/// Renderer-safe pending approval derived from real IPC events.
class StudioApprovalRequest {
  const StudioApprovalRequest({
    required this.id,
    required this.kind,
    required this.occurredAt,
    required this.message,
    this.handoffId,
    this.toolName,
    this.safeDetails = const {},
  });

  final String id;
  final StudioApprovalKind kind;
  final DateTime occurredAt;
  final String message;
  final String? handoffId;
  final String? toolName;
  final Map<String, dynamic> safeDetails;

  bool get hasApprovalCommand => handoffId != null && handoffId!.isNotEmpty;
}

StudioApprovalRequest? studioApprovalFromEventMap(Map<String, dynamic> event) {
  final eventType = event['event'];
  final payload = event['payload'];
  if (eventType is! String || payload is! Map<String, dynamic>) return null;
  final handoffId = payload['handoffId']?.toString();
  final toolName = payload['toolName']?.toString();
  final fromAgentId = payload['fromAgentId']?.toString();
  final toAgentId = payload['toAgentId']?.toString();
  final occurredAt = event['occurredAt']?.toString();
  final eventId = event['eventId']?.toString();
  final cursor = event['cursor'];
  final sequence = cursor is Map<String, dynamic> ? cursor['sequence'] : null;
  final fallbackId =
      '$eventType-${sequence ?? DateTime.now().microsecondsSinceEpoch}';

  switch (eventType) {
    case 'tool.requested':
      return StudioApprovalRequest(
        id: eventId?.isNotEmpty == true ? eventId! : fallbackId,
        kind: StudioApprovalKind.tool,
        occurredAt: occurredAt != null
            ? DateTime.tryParse(occurredAt) ?? DateTime.now()
            : DateTime.now(),
        message: toolName == null || toolName.isEmpty
            ? '工具审批请求'
            : '工具审批请求：$toolName',
        handoffId: handoffId,
        toolName: toolName,
        safeDetails: _safeApprovalDetails(payload),
      );
    case 'handoff.proposed':
      return StudioApprovalRequest(
        id: eventId?.isNotEmpty == true ? eventId! : fallbackId,
        kind: StudioApprovalKind.handoff,
        occurredAt: occurredAt != null
            ? DateTime.tryParse(occurredAt) ?? DateTime.now()
            : DateTime.now(),
        message: '交接待审批：${fromAgentId ?? '-'} → ${toAgentId ?? '-'}',
        handoffId: handoffId,
        safeDetails: _safeApprovalDetails(payload),
      );
    default:
      return null;
  }
}

StudioApprovalRequest? studioApprovalFromEnvelope(EventEnvelope event) {
  final eventType = event.event;
  final payload = event.payload;
  switch (eventType) {
    case 'tool.requested':
      return StudioApprovalRequest(
        id: event.eventId,
        kind: StudioApprovalKind.tool,
        occurredAt: event.occurredAt,
        message: '工具审批请求：${payload['toolName'] ?? '-'}',
        handoffId: payload['handoffId']?.toString(),
        toolName: payload['toolName']?.toString(),
        safeDetails: _safeApprovalDetails(payload),
      );
    case 'handoff.proposed':
      return StudioApprovalRequest(
        id: event.eventId,
        kind: StudioApprovalKind.handoff,
        occurredAt: event.occurredAt,
        message:
            '交接待审批：${payload['fromAgentId'] ?? '-'} → ${payload['toAgentId'] ?? '-'}',
        handoffId: payload['handoffId']?.toString(),
        safeDetails: _safeApprovalDetails(payload),
      );
    default:
      return null;
  }
}

Map<String, dynamic> _safeApprovalDetails(Map<String, dynamic> payload) {
  final safe = <String, dynamic>{};
  for (final key in const [
    'requestId',
    'handoffId',
    'toolName',
    'fromAgentId',
    'toAgentId',
    'fromTaskNodeId',
    'toTaskNodeId',
    'kind',
    'dispatchMode',
  ]) {
    final value = payload[key];
    if (value is String && value.isNotEmpty) safe[key] = value;
    if (value is num || value is bool) safe[key] = value;
  }
  return safe;
}

/// Color marker for the approval panel.
Color studioApprovalColor(StudioApprovalKind kind) {
  return switch (kind) {
    StudioApprovalKind.tool => StudioColors.warning,
    StudioApprovalKind.handoff => StudioColors.nodeAnalyzer,
  };
}
