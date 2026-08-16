import 'dart:async';
import 'dart:typed_data';

import 'package:agenttalk_desktop/ipc/core_ipc_client.dart';
import 'package:agenttalk_desktop/ipc/protocol_v1.dart';
import 'package:agenttalk_desktop/main.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  testWidgets('Structured Handoff submits the selected roster agent and run', (
    tester,
  ) async {
    tester.view.physicalSize = const Size(1400, 900);
    tester.view.devicePixelRatio = 1;
    addTearDown(tester.view.resetPhysicalSize);
    addTearDown(tester.view.resetDevicePixelRatio);
    final pipe = _FakePipe(
      responsePayload: const {
        'created': true,
        'alreadyPresent': false,
        'projection': {
          'projects': [
            {'id': 'project-1', 'name': 'Project One'},
          ],
          'agents': [
            {'id': 'agent-1', 'name': 'Alpha'},
            {'id': 'agent-2', 'name': 'Beta'},
          ],
          'assignments': [
            {'projectId': 'project-1', 'agentId': 'agent-1', 'enabled': true},
            {'projectId': 'project-1', 'agentId': 'agent-2', 'enabled': true},
          ],
          'runs': [
            {
              'id': 'run-1',
              'projectId': 'project-1',
              'agentId': 'agent-1',
              'status': 'completed',
            },
          ],
        },
      },
    );
    final client = _clientFor(pipe);
    addTearDown(() => unawaited(client.close()));

    await tester.pumpWidget(
      MaterialApp(
        home: WorkspaceShell(
          initialClient: client,
          initialSessionId: 'session-test-123456',
          initialSnapshot: _snapshot,
        ),
      ),
    );
    await _settle(tester);

    await tester.tap(find.byTooltip('创建结构化交接'));
    await _settle(tester);
    await tester.tap(find.byKey(const Key('handoff-source-run')));
    await _settle(tester);
    await tester.tap(find.text('run-1 · 已完成').last);
    await _settle(tester);
    await tester.tap(find.byKey(const Key('handoff-target-agent')));
    await _settle(tester);
    await tester.tap(find.text('Beta').last);
    await _settle(tester);
    await tester.tap(find.byKey(const Key('handoff-source-message')));
    await _settle(tester);
    await tester.tap(find.text('message-1 · source message').last);
    await _settle(tester);
    await tester.enterText(
      find.byKey(const Key('handoff-task')),
      'Review the selected source',
    );
    await tester.tap(find.byKey(const Key('handoff-submit')));
    await _settle(tester);

    expect(pipe.writtenCommands, ['collaboration.create', 'handoff.create']);
    expect(find.text('结构化交接已创建'), findsOneWidget);
  });

  testWidgets('Structured Handoff rejects missing required selections', (
    tester,
  ) async {
    tester.view.physicalSize = const Size(1400, 900);
    tester.view.devicePixelRatio = 1;
    addTearDown(tester.view.resetPhysicalSize);
    addTearDown(tester.view.resetDevicePixelRatio);
    final pipe = _FakePipe();
    final client = _clientFor(pipe);
    addTearDown(() => unawaited(client.close()));

    await tester.pumpWidget(
      MaterialApp(
        home: WorkspaceShell(
          initialClient: client,
          initialSessionId: 'session-test-123456',
          initialSnapshot: _snapshot,
        ),
      ),
    );
    await _settle(tester);
    await tester.tap(find.byTooltip('创建结构化交接'));
    await _settle(tester);
    await tester.tap(find.byKey(const Key('handoff-submit')));
    await _settle(tester);

    expect(find.text('请选择来源运行'), findsOneWidget);
    expect(find.text('请选择目标智能体'), findsOneWidget);
    expect(pipe.writtenCommands, isEmpty);
  });

  testWidgets('Structured Handoff rejects an empty current Project roster', (
    tester,
  ) async {
    tester.view.physicalSize = const Size(1400, 900);
    tester.view.devicePixelRatio = 1;
    addTearDown(tester.view.resetPhysicalSize);
    addTearDown(tester.view.resetDevicePixelRatio);
    final pipe = _FakePipe();
    final client = _clientFor(pipe);
    addTearDown(() => unawaited(client.close()));
    final snapshot = <String, dynamic>{
      'projects': [
        {'id': 'project-1', 'name': 'Project One'},
      ],
      'agents': [
        {'id': 'agent-1', 'name': 'Alpha'},
      ],
      'assignments': const <Map<String, dynamic>>[],
      'runs': [
        {'id': 'run-1', 'projectId': 'project-1', 'status': 'completed'},
      ],
    };

    await tester.pumpWidget(
      MaterialApp(
        home: WorkspaceShell(
          initialClient: client,
          initialSessionId: 'session-test-123456',
          initialSnapshot: snapshot,
        ),
      ),
    );
    await _settle(tester);
    await tester.tap(find.byTooltip('创建结构化交接'));
    await _settle(tester);
    await tester.tap(find.byKey(const Key('handoff-source-run')));
    await _settle(tester);
    await tester.tap(find.text('run-1 · 已完成').last);
    await _settle(tester);
    await tester.tap(find.byKey(const Key('handoff-submit')));
    await _settle(tester);

    expect(find.text('当前项目没有可用智能体'), findsOneWidget);
    expect(pipe.writtenCommands, isEmpty);
  });

  testWidgets('failed Run exposes Retry and sends the explicit current task', (
    tester,
  ) async {
    tester.view.physicalSize = const Size(1400, 900);
    tester.view.devicePixelRatio = 1;
    addTearDown(tester.view.resetPhysicalSize);
    addTearDown(tester.view.resetDevicePixelRatio);
    final pipe = _FakePipe(
      responsePayload: const {
        'run': {'id': 'retry-run-1', 'status': 'Completed'},
        'sourceExecutionRunId': 'run-failed-1',
      },
    );
    final client = _clientFor(pipe);
    addTearDown(() => unawaited(client.close()));
    final snapshot = <String, dynamic>{
      ..._snapshot,
      'runs': [
        {
          'id': 'run-failed-1',
          'projectId': 'project-1',
          'agentId': 'agent-1',
          'status': 'failed',
        },
      ],
    };

    await tester.pumpWidget(
      MaterialApp(
        home: WorkspaceShell(
          initialClient: client,
          initialSessionId: 'session-test-123456',
          initialSnapshot: snapshot,
        ),
      ),
    );
    await _settle(tester);

    expect(find.byTooltip('重试'), findsOneWidget);
    expect(find.byTooltip('按当前设置重新运行'), findsOneWidget);
    await tester.tap(find.byTooltip('重试'));
    await _settle(tester);
    await tester.enterText(find.byType(TextField).last, 'retry this task');
    await tester.tap(find.text('重试').last);
    await _settle(tester);

    expect(pipe.writtenCommands, ['execution.retry']);
    final retryPayloads = pipe.writtenPayloads
        .where((payload) => payload.containsKey('sourceExecutionRunId'))
        .toList();
    expect(retryPayloads, hasLength(1));
    expect(retryPayloads.single['sourceExecutionRunId'], 'run-failed-1');
    expect(retryPayloads.single['currentTask'], 'retry this task');
    expect(find.text('已重试运行：retry-run-1'), findsWidgets);
  });

  testWidgets('Handoff review actions expose approve and reject controls', (
    tester,
  ) async {
    tester.view.physicalSize = const Size(1400, 900);
    tester.view.devicePixelRatio = 1;
    addTearDown(tester.view.resetPhysicalSize);
    addTearDown(tester.view.resetDevicePixelRatio);
    final pipe = _FakePipe(
      responsePayload: const {
        'handoffId': 'handoff-1',
        'status': 'approved',
        'changed': true,
        'alreadyAtTarget': false,
        'projection': {'handoffs': []},
      },
    );
    final client = _clientFor(pipe);
    addTearDown(() => unawaited(client.close()));

    await tester.pumpWidget(
      MaterialApp(
        home: WorkspaceShell(
          initialClient: client,
          initialSessionId: 'session-test-123456',
          initialSnapshot: {
            ..._snapshot,
            'handoffs': [
              {'id': 'handoff-1', 'status': 'proposed', 'toAgentId': 'agent-2'},
            ],
          },
        ),
      ),
    );
    await _settle(tester);

    expect(find.byTooltip('批准交接'), findsOneWidget);
    expect(find.byTooltip('拒绝交接'), findsOneWidget);
    await tester.tap(find.byTooltip('批准交接'));
    await _settle(tester);

    expect(pipe.writtenCommands, ['handoff.approve']);
    expect(find.text('交接已变更为已批准'), findsWidgets);
  });
}

Future<void> _settle(WidgetTester tester) =>
    tester.pump(const Duration(milliseconds: 500));

final _snapshot = <String, dynamic>{
  'projects': [
    {'id': 'project-1', 'name': 'Project One'},
  ],
  'agents': [
    {'id': 'agent-1', 'name': 'Alpha'},
    {'id': 'agent-2', 'name': 'Beta'},
  ],
  'assignments': [
    {'projectId': 'project-1', 'agentId': 'agent-1', 'enabled': true},
    {'projectId': 'project-1', 'agentId': 'agent-2', 'enabled': true},
  ],
  'runs': [
    {
      'id': 'run-1',
      'projectId': 'project-1',
      'agentId': 'agent-1',
      'status': 'completed',
    },
  ],
  'messages': [
    {
      'id': 'message-1',
      'conversationId': 'conversation-1',
      'content': 'source message',
    },
  ],
};

CoreIpcClient _clientFor(_FakePipe pipe) => CoreIpcClient.forTesting(
  read: pipe.read,
  write: pipe.write,
  close: pipe.close,
);

class _FakePipe {
  _FakePipe({this.responsePayload});

  final Map<String, dynamic>? responsePayload;
  final IpcFrameCodec _codec = const IpcFrameCodec();
  final List<String> writtenCommands = [];
  final List<Map<String, dynamic>> writtenPayloads = [];
  final List<_QueuedFrame> _frames = [];
  final List<Completer<void>> _readWaiters = [];
  bool _closed = false;

  Future<void> write(Uint8List frame) async {
    final request = _codec.decodeJson(frame);
    final command = request['command'];
    if (command is String) writtenCommands.add(command);
    final payload = request['payload'];
    if (payload is Map<String, dynamic>) writtenPayloads.add(payload);
    _frames.add(
      _QueuedFrame(
        _codec.encodeJson({
          'kind': 'response',
          'protocol': {'major': protocolMajor, 'minor': 0},
          'requestId': request['requestId'],
          'ok': true,
          'payload': responsePayload ?? <String, dynamic>{},
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
