import 'dart:async';
import 'dart:typed_data';

import 'package:agenttalk_desktop/ipc/core_ipc_client.dart';
import 'package:agenttalk_desktop/ipc/protocol_v1.dart';
import 'package:agenttalk_desktop/main.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';

CoreIpcClient _clientFor(_FakePipe pipe) => CoreIpcClient.forTesting(
  read: pipe.read,
  write: pipe.write,
  close: pipe.close,
  sessionCredential: 'test-credential',
  serverEpoch: 'epoch-1',
  sessionId: 'session-test',
);

class _FakePipe {
  _FakePipe();

  final IpcFrameCodec _codec = const IpcFrameCodec();
  final List<String> writtenCommands = [];
  final List<Map<String, dynamic>> writtenPayloads = [];
  final List<_QueuedFrame> _frames = [];
  final List<Completer<void>> _readWaiters = [];
  final List<Completer<void>> ackWaiters = [];
  bool _closed = false;
  bool failNextAck = false;
  String? failNextAckWithCode;
  bool failNextReplay = false;
  bool failNextSnapshot = false;

  Future<void> write(Uint8List frame) async {
    final request = _codec.decodeJson(frame);
    final command = request['command'] ?? request['query'];
    if (command is String) writtenCommands.add(command);
    final payload = request['payload'];
    if (payload is Map<String, dynamic>) writtenPayloads.add(payload);

    if (request['kind'] == 'handshake') {
      _enqueueResponse(request['requestId'], {'serverEpoch': 'epoch-1'});
      return;
    }

    if (command == 'events.ack') {
      final waiter = Completer<void>();
      ackWaiters.add(waiter);
      await waiter.future;
      if (failNextAck || failNextAckWithCode != null) {
        final code = failNextAckWithCode;
        failNextAck = false;
        failNextAckWithCode = null;
        _enqueueError(
          request['requestId'],
          'Simulated ACK failure',
          code: code,
        );
      } else {
        _enqueueResponse(request['requestId'], {});
      }
    } else if (command == 'events.subscribe') {
      _enqueueResponse(request['requestId'], {
        'subscriptionId': 'sub-1',
        'streamId': 'core-events',
        'cursor': {
          'streamId': 'core-events',
          'sequence': request['payload']?['afterCursor']?['sequence'] ?? 10,
          'epoch': 'epoch-1',
        },
      });
    } else if (command == 'events.replay') {
      if (failNextReplay) {
        failNextReplay = false;
        _enqueueError(
          request['requestId'],
          'Gap detected',
          code: 'REPLAY_GAP',
          details: {
            'streamId': 'core-events',
            'resumeCursor': {
              'streamId': 'core-events',
              'sequence': 10,
              'epoch': 'epoch-1',
            },
          },
        );
      } else {
        _enqueueResponse(request['requestId'], {'events': []});
      }
    } else if (command == 'projection.snapshot') {
      if (failNextSnapshot) {
        failNextSnapshot = false;
        _enqueueError(request['requestId'], 'Simulated snapshot failure');
      } else {
        _enqueueResponse(request['requestId'], {
          'projects': [],
          'agents': [],
          'assignments': [],
          'runs': [],
          'conversations': [],
        });
      }
    } else {
      _enqueueResponse(request['requestId'], {});
    }
  }

  void _enqueueResponse(String requestId, Map<String, dynamic> payload) {
    _frames.add(
      _QueuedFrame(
        _codec.encodeJson({
          'kind': 'response',
          'protocol': {'major': protocolMajor, 'minor': 0},
          'requestId': requestId,
          'ok': true,
          'payload': payload,
        }),
      ),
    );
    _wakeReaders();
  }

  void _enqueueError(
    String requestId,
    String message, {
    String? code,
    Map<String, dynamic>? details,
  }) {
    _frames.add(
      _QueuedFrame(
        _codec.encodeJson({
          'kind': 'error',
          'protocol': {'major': protocolMajor, 'minor': 0},
          'requestId': requestId,
          'ok': false,
          'code': code ?? 'GENERIC',
          'message': message,
          'retryable': false,
          if (details != null) ...{'details': details},
        }),
      ),
    );
    _wakeReaders();
  }

  void pushEvent(String eventName, int sequence) {
    _frames.add(
      _QueuedFrame(
        _codec.encodeJson({
          'kind': 'event',
          'protocol': {'major': protocolMajor, 'minor': 0},
          'eventId': 'evt-$sequence',
          'sessionId': 'session-test',
          'cursor': {
            'streamId': 'core-events',
            'sequence': sequence,
            'epoch': 'epoch-1',
          },
          'event': eventName,
          'occurredAt': DateTime.now().toUtc().toIso8601String(),
          'payload': {'data': {}},
          'subscriptionId': 'sub-1',
        }),
      ),
    );
    _wakeReaders();
  }

  Future<Uint8List> read(int length) async {
    while (_availableBytes < length) {
      if (_closed) throw StateError('pipe is closed');
      final waiter = Completer<void>();
      _readWaiters.add(waiter);
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
    _wakeReaders();
  }

  int get _availableBytes =>
      _frames.isEmpty ? 0 : _frames.first.bytes.length - _frames.first.offset;

  void _wakeReaders() {
    final waiters = List<Completer<void>>.from(_readWaiters);
    _readWaiters.clear();
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

void main() {
  testWidgets(
    'WorkspaceShell event loop processes events serially and pauses on slow ACKs',
    (tester) async {
      tester.view.physicalSize = const Size(1920, 1080);
      tester.view.devicePixelRatio = 1.0;
      addTearDown(() {
        tester.view.resetPhysicalSize();
        tester.view.resetDevicePixelRatio();
      });
      final pipe = _FakePipe();
      final client = _clientFor(pipe);
      addTearDown(() => unawaited(client.close()));

      final Map<String, dynamic> snapshot = {
        'projection': {
          'projects': [],
          'agents': [],
          'assignments': [],
          'runs': [],
          'conversations': [],
        },
        'headCursor': {'streamId': 'stream-1', 'sequence': 10},
        'serverEpoch': 'epoch-1',
      };

      await tester.pumpWidget(
        MaterialApp(
          home: WorkspaceShell(
            initialClient: client,
            initialSessionId: 'session-test',
            initialSnapshot: snapshot,
          ),
        ),
      );
      await tester.pumpAndSettle();

      // Trigger a REPLAY_GAP to force the UI to show the recovery banner
      pipe.failNextReplay = true;
      // Advance time to trigger polling
      await tester.pump(const Duration(seconds: 2));
      await tester.pumpAndSettle();

      // Click the "Refresh and Subscribe" button
      await tester.tap(find.byKey(const Key('event-recovery-subscribe')));
      await tester.pumpAndSettle();

      expect(pipe.writtenCommands, contains('events.subscribe'));

      final startAckCount = pipe.ackWaiters.length;

      pipe.pushEvent('agent.message', 11);
      pipe.pushEvent('agent.message', 12);

      await tester.pump();

      expect(pipe.ackWaiters.length, startAckCount + 1);

      await tester.pump(const Duration(milliseconds: 50));
      expect(
        pipe.ackWaiters.length,
        startAckCount + 1,
        reason: 'Second event should wait for first ACK to complete',
      );

      pipe.ackWaiters[startAckCount].complete();
      await tester.pump();

      expect(pipe.ackWaiters.length, startAckCount + 2);
      pipe.ackWaiters[startAckCount + 1].complete();
      await tester.pumpAndSettle();

      final commandsCountBeforeProjection = pipe.writtenCommands.length;
      final ackCountBeforeProjection = pipe.ackWaiters.length;

      // push projection.changed to verify projection.snapshot is called before events.ack
      pipe.pushEvent('projection.changed', 13);
      await tester.pump();

      // wait for snapshot to be called
      await tester.pump(const Duration(milliseconds: 50));
      expect(pipe.ackWaiters.length, ackCountBeforeProjection + 1);

      final newCommands = pipe.writtenCommands.sublist(
        commandsCountBeforeProjection,
      );
      expect(
        newCommands.indexOf('projection.snapshot') <
            newCommands.indexOf('events.ack'),
        isTrue,
      );

      pipe.ackWaiters[ackCountBeforeProjection].complete();
      await tester.pumpAndSettle();
    },
  );

  testWidgets(
    'WorkspaceShell closes subscription and enters fail-closed state on ACK failure',
    (tester) async {
      tester.view.physicalSize = const Size(1920, 1080);
      tester.view.devicePixelRatio = 1.0;
      addTearDown(() {
        tester.view.resetPhysicalSize();
        tester.view.resetDevicePixelRatio();
      });
      final pipe = _FakePipe();
      final client = _clientFor(pipe);
      addTearDown(() => unawaited(client.close()));

      final Map<String, dynamic> snapshot = {
        'projection': {
          'projects': [],
          'agents': [],
          'assignments': [],
          'runs': [],
          'conversations': [],
        },
        'headCursor': {'streamId': 'stream-1', 'sequence': 10},
        'serverEpoch': 'epoch-1',
      };

      pipe.failNextReplay = true;

      await tester.pumpWidget(
        MaterialApp(
          home: WorkspaceShell(
            initialClient: client,
            initialSessionId: 'session-test',
            initialSnapshot: snapshot,
          ),
        ),
      );
      await tester.pumpAndSettle();

      pipe.failNextReplay = true;
      await tester.pump(const Duration(seconds: 2));
      await tester.pumpAndSettle();

      await tester.tap(find.byKey(const Key('event-recovery-subscribe')));
      await tester.pumpAndSettle();

      final startAckCount = pipe.ackWaiters.length;

      pipe.failNextAck = true;
      pipe.pushEvent('agent.message', 13);

      await tester.pump();

      expect(pipe.ackWaiters.length, startAckCount + 1);
      pipe.ackWaiters[startAckCount].complete();
      await tester.pumpAndSettle();

      expect(find.textContaining('Simulated ACK failure'), findsWidgets);
    },
  );

  testWidgets('WorkspaceShell enters fail-closed state on refresh failure', (
    tester,
  ) async {
    tester.view.physicalSize = const Size(1920, 1080);
    tester.view.devicePixelRatio = 1.0;
    addTearDown(() {
      tester.view.resetPhysicalSize();
      tester.view.resetDevicePixelRatio();
    });
    final pipe = _FakePipe();
    final client = _clientFor(pipe);
    addTearDown(() => unawaited(client.close()));

    final Map<String, dynamic> snapshot = {
      'projection': {
        'projects': [],
        'agents': [],
        'assignments': [],
        'runs': [],
        'conversations': [],
      },
      'headCursor': {'streamId': 'stream-1', 'sequence': 10},
      'serverEpoch': 'epoch-1',
    };

    pipe.failNextReplay = true;

    await tester.pumpWidget(
      MaterialApp(
        home: WorkspaceShell(
          initialClient: client,
          initialSessionId: 'session-test',
          initialSnapshot: snapshot,
        ),
      ),
    );
    await tester.pumpAndSettle();

    pipe.failNextReplay = true;
    await tester.pump(const Duration(seconds: 2));
    await tester.pumpAndSettle();

    await tester.tap(find.byKey(const Key('event-recovery-subscribe')));
    await tester.pumpAndSettle();

    pipe.failNextSnapshot = true;
    pipe.pushEvent('projection.changed', 13);

    await tester.pump();
    await tester.pumpAndSettle();

    expect(find.textContaining('Simulated snapshot failure'), findsWidgets);
  });

  testWidgets('WorkspaceShell handles SUBSCRIPTION_OVERFLOW correctly', (
    tester,
  ) async {
    tester.view.physicalSize = const Size(1920, 1080);
    tester.view.devicePixelRatio = 1.0;
    addTearDown(() {
      tester.view.resetPhysicalSize();
      tester.view.resetDevicePixelRatio();
    });
    final pipe = _FakePipe();
    final client = _clientFor(pipe);
    addTearDown(() => unawaited(client.close()));

    final Map<String, dynamic> snapshot = {
      'projection': {
        'projects': [],
        'agents': [],
        'assignments': [],
        'runs': [],
        'conversations': [],
      },
      'headCursor': {'streamId': 'stream-1', 'sequence': 10},
      'serverEpoch': 'epoch-1',
    };

    await tester.pumpWidget(
      MaterialApp(
        home: WorkspaceShell(
          initialClient: client,
          initialSessionId: 'session-test',
          initialSnapshot: snapshot,
        ),
      ),
    );
    await tester.pumpAndSettle();

    pipe.failNextReplay = true;
    await tester.pump(const Duration(seconds: 2));
    await tester.pumpAndSettle();

    await tester.tap(find.byKey(const Key('event-recovery-subscribe')));
    await tester.pumpAndSettle();

    final startAckCount = pipe.ackWaiters.length;

    pipe.failNextAckWithCode = 'SUBSCRIPTION_OVERFLOW';
    pipe.pushEvent('agent.message', 13);

    await tester.pump();
    pipe.ackWaiters[startAckCount].complete();
    await tester.pumpAndSettle();

    expect(find.textContaining('Simulated ACK failure'), findsWidgets);
  });
  testWidgets('WorkspaceShell handles CURSOR_EPOCH_MISMATCH correctly', (
    tester,
  ) async {
    tester.view.physicalSize = const Size(1920, 1080);
    tester.view.devicePixelRatio = 1.0;
    addTearDown(() {
      tester.view.resetPhysicalSize();
      tester.view.resetDevicePixelRatio();
    });
    final pipe = _FakePipe();
    final client = _clientFor(pipe);
    addTearDown(() => unawaited(client.close()));

    final Map<String, dynamic> snapshot = {
      'projection': {
        'projects': [],
        'agents': [],
        'assignments': [],
        'runs': [],
        'conversations': [],
      },
      'headCursor': {'streamId': 'stream-1', 'sequence': 10},
      'serverEpoch': 'epoch-1',
    };

    await tester.pumpWidget(
      MaterialApp(
        home: WorkspaceShell(
          initialClient: client,
          initialSessionId: 'session-test',
          initialSnapshot: snapshot,
        ),
      ),
    );
    await tester.pumpAndSettle();

    pipe.failNextReplay = true;
    await tester.pump(const Duration(seconds: 2));
    await tester.pumpAndSettle();

    await tester.tap(find.byKey(const Key('event-recovery-subscribe')));
    await tester.pumpAndSettle();

    final startAckCount = pipe.ackWaiters.length;

    pipe.failNextAckWithCode = 'CURSOR_EPOCH_MISMATCH';
    pipe.pushEvent('agent.message', 13);

    await tester.pump();
    pipe.ackWaiters[startAckCount].complete();
    await tester.pumpAndSettle();

    expect(find.textContaining('Simulated ACK failure'), findsWidgets);
  });
}
