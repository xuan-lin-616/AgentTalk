import 'dart:async';
import 'dart:typed_data';

import 'package:agenttalk_desktop/ipc/core_ipc_client.dart';
import 'package:agenttalk_desktop/ipc/protocol_v1.dart';
import 'package:agenttalk_desktop/ui/orchestration_panel.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  testWidgets(
    'orchestration panel routes approval and retry through Core IPC',
    (tester) async {
      final pipe = _PanelPipe();
      final client = CoreIpcClient.forTesting(
        read: pipe.read,
        write: pipe.write,
        close: pipe.close,
      );
      addTearDown(client.close);

      await tester.pumpWidget(
        MaterialApp(
          home: Scaffold(
            body: OrchestrationPanel(
              client: client,
              sessionId: 'session-test-123456',
            ),
          ),
        ),
      );

      await tester.tap(find.byKey(const Key('orchestration-run-picker')));
      await tester.pumpAndSettle();
      await tester.enterText(find.byType(TextField), 'run-1');
      await tester.tap(find.widgetWithText(FilledButton, '读取'));
      await tester.pumpAndSettle();

      expect(
        find.byKey(const Key('orchestration-milestone-approve-milestone-1')),
        findsOneWidget,
      );
      expect(
        find.byKey(const Key('orchestration-node-retry-node-1')),
        findsOneWidget,
      );

      await tester.tap(
        find.byKey(const Key('orchestration-milestone-approve-milestone-1')),
      );
      await tester.pumpAndSettle();
      await tester.tap(
        find.byKey(const Key('orchestration-node-retry-node-1')),
      );
      await tester.pumpAndSettle();

      expect(pipe.commands, contains('orchestration.receipt.record'));
      expect(pipe.commands, contains('orchestration.task.retry'));
      expect(pipe.receiptPayload?['decision'], 'approve');
      expect(pipe.retryPayload?['nodeId'], 'node-1');
    },
  );
}

class _PanelPipe {
  final IpcFrameCodec _codec = const IpcFrameCodec();
  final List<_QueuedFrame> _frames = [];
  final List<Completer<void>> _waiters = [];
  final List<String> commands = [];
  Map<String, dynamic>? receiptPayload;
  Map<String, dynamic>? retryPayload;
  bool _closed = false;

  Future<void> write(Uint8List bytes) async {
    final request = _codec.decodeJson(bytes);
    final requestId = request['requestId'] as String;
    final operation = request['command'] ?? request['query'];
    final payload = Map<String, dynamic>.from(
      (request['payload'] as Map?) ?? const <String, dynamic>{},
    );
    if (operation is String && operation.startsWith('orchestration.')) {
      if (request['command'] is String) commands.add(operation);
      if (operation == orchestrationReceiptRecordCommand) {
        receiptPayload = payload;
      } else if (operation == orchestrationTaskRetryCommand) {
        retryPayload = payload;
      }
    }
    final responsePayload = switch (operation) {
      orchestrationRunSnapshotQuery => <String, dynamic>{
        'run': <String, dynamic>{
          'runId': 'run-1',
          'status': 'awaiting_approval',
          'coordinatorGeneration': 1,
        },
        'nodes': <Map<String, dynamic>>[
          <String, dynamic>{
            'nodeId': 'node-1',
            'nodeKey': 'failed-node',
            'status': 'failed',
          },
        ],
        'attempts': <dynamic>[],
        'milestones': <Map<String, dynamic>>[
          <String, dynamic>{
            'milestoneId': 'milestone-1',
            'milestoneKey': 'review',
            'status': 'awaiting_approval',
            'version': 1,
            'briefTreeDigest': 'a' * 64,
            'presentedArtifactSetDigest': 'b' * 64,
            'acceptanceEvidenceDigest': 'c' * 64,
          },
        ],
        'deliveries': <dynamic>[],
        'machineAcceptances': <dynamic>[],
        'edges': <dynamic>[],
        'edgePorts': <dynamic>[],
        'roleBindings': <dynamic>[],
        'contextAuthorities': <dynamic>[],
      },
      orchestrationAuditEventsQuery => <String, dynamic>{
        'runId': 'run-1',
        'events': <dynamic>[],
      },
      orchestrationReceiptRecordCommand => <String, dynamic>{
        'receiptId': payload['receiptId'],
        'decision': payload['decision'],
        'replayed': false,
        'recorded': true,
      },
      orchestrationTaskRetryCommand => <String, dynamic>{
        'nodeId': payload['nodeId'],
        'ready': true,
        'replayed': false,
      },
      _ => <String, dynamic>{'status': 'ready'},
    };
    _enqueue(<String, dynamic>{
      'kind': 'response',
      'protocol': <String, dynamic>{'major': protocolMajor, 'minor': 0},
      'requestId': requestId,
      'ok': true,
      'payload': responsePayload,
    });
  }

  Future<Uint8List> read(int length) async {
    while (_frames.isEmpty) {
      if (_closed) throw StateError('pipe is closed');
      final waiter = Completer<void>();
      _waiters.add(waiter);
      await waiter.future;
    }
    final frame = _frames.first;
    final chunk = Uint8List.fromList(
      frame.bytes.sublist(frame.offset, frame.offset + length),
    );
    frame.offset += length;
    if (frame.offset == frame.bytes.length) _frames.removeAt(0);
    return chunk;
  }

  Future<void> close() async {
    _closed = true;
    for (final waiter in _waiters) {
      if (!waiter.isCompleted) waiter.complete();
    }
    _waiters.clear();
  }

  void _enqueue(Map<String, dynamic> value) {
    _frames.add(_QueuedFrame(_codec.encodeJson(value)));
    final waiters = List<Completer<void>>.from(_waiters);
    _waiters.clear();
    for (final waiter in waiters) {
      if (!waiter.isCompleted) waiter.complete();
    }
  }
}

class _QueuedFrame {
  _QueuedFrame(this.bytes);

  final Uint8List bytes;
  int offset = 0;
}
