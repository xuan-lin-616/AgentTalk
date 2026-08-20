import 'package:agenttalk_desktop/ipc/protocol_v1.dart';
import 'package:agenttalk_desktop/ui/theme/studio_colors.dart';
import 'package:agenttalk_desktop/ui/workbench/execution_log_panel.dart';
import 'package:agenttalk_desktop/ui/workbench/studio_event_log.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  test('studioLogEntryFromEventMap redacts paths and credentials', () {
    final entry = studioLogEntryFromEventMap({
      'eventId': 'evt-1',
      'event': 'execution.failed',
      'occurredAt': '2026-08-19T10:00:00Z',
      'cursor': {'streamId': 'core-events', 'sequence': 1},
      'payload': {
        'executionRunId': 'run-1',
        'message': r'token=abc123 path C:\Users\me\secret.txt failed',
      },
    });
    expect(entry, isNotNull);
    expect(entry!.message, isNot(contains('abc123')));
    expect(entry.message, isNot(contains(r'C:\Users')));
    expect(entry.message, contains('运行失败'));
    expect(entry.safeDetails['executionRunId'], 'run-1');
  });

  test('execution failure uses the Core reason and envelope run id', () {
    final entry = studioLogEntryFromEventMap({
      'eventId': 'evt-reason',
      'event': 'execution.failed',
      'executionRunId': 'run-from-envelope',
      'occurredAt': '2026-08-19T10:00:00Z',
      'cursor': {'streamId': 'core-events', 'sequence': 2},
      'payload': {
        'reason': 'invalid_workspace',
        'message': 'legacy message must not win',
      },
    });
    expect(entry, isNotNull);
    expect(entry!.message, contains('run-from-envelope'));
    expect(entry.message, contains('invalid_workspace'));
    expect(entry.message, isNot(contains('legacy message')));
  });

  test('studioStreamingDeltaFromEventMap only accepts output.delta', () {
    final delta = studioStreamingDeltaFromEventMap({
      'eventId': 'evt-delta',
      'event': 'output.delta',
      'occurredAt': '2026-08-19T10:00:01Z',
      'cursor': {'streamId': 'core-events', 'sequence': 2},
      'executionRunId': 'exec-1',
      'payload': {
        'delta': 'hello agent',
        'isComplete': false,
        'conversationId': 'conversation-1',
      },
    });
    expect(delta, isNotNull);
    expect(delta!.delta, 'hello agent');
    expect(delta.executionRunId, 'exec-1');
    expect(delta.conversationId, 'conversation-1');
    expect(
      studioStreamingDeltaFromEventMap({'event': 'handoff.proposed'}),
      isNull,
    );
  });

  test('studioLogEntryFromEnvelope uses the typed event envelope', () {
    final entry = studioLogEntryFromEnvelope(
      EventEnvelope(
        eventId: 'evt-envelope',
        sessionId: 'session-1',
        cursor: const StreamCursor(streamId: 'core-events', sequence: 3),
        event: 'handoff.proposed',
        occurredAt: DateTime.parse('2026-08-19T10:00:02Z'),
        payload: const {'fromAgentId': 'agent-1', 'toAgentId': 'agent-2'},
      ),
    );
    expect(entry.id, 'evt-envelope');
    expect(entry.message, contains('agent-1'));
    expect(entry.message, contains('agent-2'));
  });

  testWidgets('ExecutionLogPanel shows empty state and event rows', (
    tester,
  ) async {
    await tester.pumpWidget(
      const MaterialApp(
        home: Scaffold(body: ExecutionLogPanel(entries: [])),
      ),
    );
    expect(find.text('暂无运行日志'), findsOneWidget);

    await tester.pumpWidget(
      MaterialApp(
        home: Scaffold(
          body: ExecutionLogPanel(
            entries: [
              StudioLogEntry(
                id: 'log-1',
                occurredAt: DateTime.parse('2026-08-19T10:00:03Z'),
                eventType: 'execution.completed',
                message: '运行已完成：run-1',
                level: StudioLogLevel.success,
              ),
            ],
          ),
        ),
      ),
    );
    await tester.pump();
    expect(find.text('运行日志'), findsOneWidget);
    expect(find.text('运行已完成：run-1'), findsOneWidget);
    expect(find.text('暂无运行日志'), findsNothing);
  });

  test('studio log level colors are stable semantic colors', () {
    expect(studioLogLevelColor(StudioLogLevel.success), StudioColors.success);
    expect(studioLogLevelColor(StudioLogLevel.warning), StudioColors.warning);
    expect(studioLogLevelColor(StudioLogLevel.error), StudioColors.danger);
    expect(
      studioLogLevelColor(StudioLogLevel.info),
      StudioColors.nodeCollector,
    );
  });
}
