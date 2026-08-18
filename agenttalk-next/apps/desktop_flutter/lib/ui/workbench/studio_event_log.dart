import 'package:flutter/material.dart';

import '../../ipc/protocol_v1.dart';
import '../theme/studio_colors.dart';

/// Renderer-safe UI log entries derived from real IPC events.
///
/// The shell owns the IPC event stream and appends entries here. This file
/// only transforms envelopes/maps into displayable entries; it never invents
/// events and never exposes raw payloads. Absolute paths, PIDs, ports and
/// credential-shaped substrings are redacted before a string reaches the UI.
enum StudioLogLevel { info, success, warning, error }

class StudioLogEntry {
  const StudioLogEntry({
    required this.id,
    required this.occurredAt,
    required this.eventType,
    required this.message,
    required this.level,
    this.safeDetails = const {},
  });

  final String id;
  final DateTime occurredAt;
  final String eventType;
  final String message;
  final StudioLogLevel level;
  final Map<String, dynamic> safeDetails;
}

/// Streaming token chunk derived from `output.delta` events only.
class StudioStreamingDelta {
  const StudioStreamingDelta({
    required this.id,
    required this.occurredAt,
    required this.delta,
    required this.isComplete,
    this.executionRunId,
    this.conversationId,
  });

  final String id;
  final DateTime occurredAt;
  final String delta;
  final bool isComplete;
  final String? executionRunId;
  final String? conversationId;
}

const int _maxSafeStringLength = 600;
const int _maxDetails = 6;

final RegExp _secretPattern = RegExp(
  r'(authorization|cookie|token|api[_-]?key|password|secret|credential)\s*[:=]\s*\S+',
  caseSensitive: false,
);

final RegExp _absoluteWindowsPathPattern = RegExp(r'[A-Za-z]:\\[^\s,;:]*');

final RegExp _absoluteUnixPathPattern = RegExp(r'(/[A-Za-z0-9_\-\.]+)+');

String studioSafeText(String value) => _redact(value);

String _redact(String value) {
  var result = value.replaceAll(_secretPattern, '<redacted>');
  result = result.replaceAll(_absoluteWindowsPathPattern, '<path-redacted>');
  result = result.replaceAll(_absoluteUnixPathPattern, '<path-redacted>');
  result = result.replaceAll(RegExp(r'[\r\n]+'), ' ');
  if (result.length > _maxSafeStringLength) {
    result = '${result.substring(0, _maxSafeStringLength)}…';
  }
  return result;
}

String _safeString(dynamic value) {
  if (value is String) return _redact(value);
  if (value is num || value is bool) return '$value';
  return '';
}

Map<String, dynamic> _safeDetails(Map<String, dynamic> payload) {
  final safe = <String, dynamic>{};
  final keys = payload.keys.take(_maxDetails);
  for (final key in keys) {
    final value = payload[key];
    if (value is String) {
      safe[key] = _safeString(value);
    } else if (value is num || value is bool) {
      safe[key] = value;
    } else if (value is Map<String, dynamic>) {
      safe[key] = _safeDetails(value);
    } else if (value is List) {
      safe[key] = value
          .map(
            (item) => item is Map<String, dynamic>
                ? _safeDetails(item)
                : _safeString(item),
          )
          .take(3)
          .toList(growable: false);
    }
  }
  return safe;
}

StudioLogLevel _levelForEvent(String eventType) {
  if (eventType.contains('failed') ||
      eventType.contains('denied') ||
      eventType.contains('rejected') ||
      eventType.contains('error')) {
    return StudioLogLevel.error;
  }
  if (eventType.contains('completed') ||
      eventType.contains('approved') ||
      eventType.contains('verified') ||
      eventType.contains('imported') ||
      eventType.contains('started')) {
    return StudioLogLevel.success;
  }
  if (eventType.contains('cancelled') ||
      eventType.contains('interrupted') ||
      eventType.contains('overflow')) {
    return StudioLogLevel.warning;
  }
  return StudioLogLevel.info;
}

String _summaryForEvent(String eventType, Map<String, dynamic> payload) {
  switch (eventType) {
    case 'output.delta':
      final delta = _safeString(payload['delta']);
      final complete = payload['isComplete'] == true;
      return delta.isEmpty ? '输出增量${complete ? '（完成）' : ''}' : delta;
    case 'tool.requested':
      final toolName = _safeString(payload['toolName']);
      return toolName.isEmpty ? '工具审批请求' : '工具审批请求：$toolName';
    case 'tool.approved':
      return '工具审批已批准：${_safeString(payload['toolName'])}';
    case 'tool.denied':
      return '工具审批已拒绝：${_safeString(payload['toolName'])}';
    case 'handoff.proposed':
      return '交接待审批：${_safeString(payload['fromAgentId'])} → ${_safeString(payload['toAgentId'])}';
    case 'handoff.approved':
      return '交接已批准：${_safeString(payload['handoffId'])}';
    case 'handoff.rejected':
      return '交接已拒绝：${_safeString(payload['handoffId'])}';
    case 'handoff.dispatched':
      return '交接已派发：${_safeString(payload['handoffId'])}';
    case 'handoff.completed':
      return '交接已完成：${_safeString(payload['handoffId'])}';
    case 'execution.status_changed':
      return '运行状态变更：${_safeString(payload['status'])}（${_safeString(payload['executionRunId'])}）';
    case 'execution.completed':
      return '运行已完成：${_safeString(payload['executionRunId'])}';
    case 'execution.failed':
      return '运行失败：${_safeString(payload['executionRunId'])} ${_safeString(payload['message'])}';
    case 'execution.cancelled':
      return '运行已取消：${_safeString(payload['executionRunId'])}';
    case 'execution.interrupted':
      return '运行已中断：${_safeString(payload['executionRunId'])}';
    case 'context.assembled':
      return '上下文已组装';
    case 'context.sealed':
      return '上下文已封板';
    case 'scope.frozen':
      return '作用域已冻结';
    case 'projection.changed':
      return 'Core 投影已更新';
    case 'connector.started':
      return 'Connector 已启动：${_safeString(payload['connectorId'])}';
    case 'runtime.started':
      return 'Runtime 已启动';
    case 'core.heartbeat':
      return 'Core 心跳';
    case 'core.restarted':
      return 'Core 已重启';
    case 'agent.discovery.started':
      return '本地发现已启动';
    case 'agent.discovery.candidate_observed':
      return '发现候选：${_safeString(payload['candidateId'])}';
    case 'agent.discovery.candidate_classified':
      return '候选已分类：${_safeString(payload['candidateId'])}';
    case 'agent.discovery.candidate_verified':
      return '候选已验证：${_safeString(payload['candidateId'])}';
    case 'agent.discovery.completed':
      return '本地发现已完成';
    case 'agent.discovery.failed':
      return '本地发现失败：${_safeString(payload['message'])}';
    case 'local_agent.imported':
      return '本地智能体已导入：${_safeString(payload['agentId'])}';
    default:
      return eventType;
  }
}

/// Builds a renderer-safe log entry from a raw `events.replay` map. Returns
/// null when the map is not recognizably an IPC event.
StudioLogEntry? studioLogEntryFromEventMap(Map<String, dynamic> event) {
  final eventType = event['event'];
  final payload = event['payload'];
  if (eventType is! String ||
      eventType.isEmpty ||
      payload is! Map<String, dynamic>) {
    return null;
  }
  final eventId = event['eventId']?.toString();
  final cursor = event['cursor'];
  final sequence = cursor is Map<String, dynamic> ? cursor['sequence'] : null;
  final occurredAt = event['occurredAt']?.toString();
  return StudioLogEntry(
    id: eventId?.isNotEmpty == true
        ? eventId!
        : '$eventType-${sequence ?? DateTime.now().microsecondsSinceEpoch}',
    occurredAt: occurredAt != null
        ? DateTime.tryParse(occurredAt) ?? DateTime.now()
        : DateTime.now(),
    eventType: eventType,
    message: _summaryForEvent(eventType, payload),
    level: _levelForEvent(eventType),
    safeDetails: _safeDetails(payload),
  );
}

/// Builds a renderer-safe log entry from a typed event envelope.
StudioLogEntry studioLogEntryFromEnvelope(EventEnvelope event) {
  return StudioLogEntry(
    id: event.eventId,
    occurredAt: event.occurredAt,
    eventType: event.event,
    message: _summaryForEvent(event.event, event.payload),
    level: _levelForEvent(event.event),
    safeDetails: _safeDetails(event.payload),
  );
}

/// Builds a streaming delta from an `output.delta` event, or returns null for
/// other event types.
StudioStreamingDelta? studioStreamingDeltaFromEventMap(
  Map<String, dynamic> event,
) {
  if (event['event'] != 'output.delta') return null;
  final payload = event['payload'];
  if (payload is! Map<String, dynamic>) return null;
  final delta = payload['delta'];
  if (delta is! String || delta.isEmpty) return null;
  final eventId = event['eventId']?.toString();
  final cursor = event['cursor'];
  final sequence = cursor is Map<String, dynamic> ? cursor['sequence'] : null;
  final occurredAt = event['occurredAt']?.toString();
  return StudioStreamingDelta(
    id: eventId?.isNotEmpty == true
        ? eventId!
        : 'delta-${sequence ?? DateTime.now().microsecondsSinceEpoch}',
    occurredAt: occurredAt != null
        ? DateTime.tryParse(occurredAt) ?? DateTime.now()
        : DateTime.now(),
    delta: _redact(delta),
    isComplete: payload['isComplete'] == true,
    executionRunId: event['executionRunId']?.toString(),
    conversationId: payload['conversationId']?.toString(),
  );
}

/// Builds a streaming delta from a typed event envelope, or returns null.
StudioStreamingDelta? studioStreamingDeltaFromEnvelope(EventEnvelope event) {
  if (event.event != 'output.delta') return null;
  final delta = event.payload['delta'];
  if (delta is! String || delta.isEmpty) return null;
  return StudioStreamingDelta(
    id: event.eventId,
    occurredAt: event.occurredAt,
    delta: _redact(delta),
    isComplete: event.payload['isComplete'] == true,
    executionRunId: event.executionRunId,
    conversationId: event.payload['conversationId']?.toString(),
  );
}

/// Log entry colors shared by the log panel.
Color studioLogLevelColor(StudioLogLevel level) {
  return switch (level) {
    StudioLogLevel.success => StudioColors.success,
    StudioLogLevel.warning => StudioColors.warning,
    StudioLogLevel.error => StudioColors.danger,
    StudioLogLevel.info => StudioColors.nodeCollector,
  };
}
