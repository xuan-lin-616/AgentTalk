import 'dart:async';
import 'dart:typed_data';

import 'package:agenttalk_desktop/gen/l10n.dart';
import 'package:agenttalk_desktop/ipc/core_ipc_client.dart';
import 'package:agenttalk_desktop/ipc/local_discovery.dart';
import 'package:agenttalk_desktop/ipc/protocol_v1.dart';
import 'package:agenttalk_desktop/ui/local_agent_scan_dialog.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';

/// Scripted IPC transport that records every outgoing command/query and
/// answers each request from a responder. Events can be pushed afterwards.
class _ScriptedPipe {
  _ScriptedPipe();

  final IpcFrameCodec _codec = const IpcFrameCodec();
  final List<String> writtenRequestIds = [];
  final List<String> writtenCommands = [];
  final List<String> writtenQueries = [];
  final List<Map<String, dynamic>> writtenPayloads = [];
  final List<Map<String, dynamic>> writtenCommandPayloads = [];

  List<Map<String, dynamic>> payloadsForCommand(String command) => [
    for (var i = 0; i < writtenCommands.length; i++)
      if (writtenCommands[i] == command) writtenCommandPayloads[i],
  ];

  Map<String, dynamic>? Function(Map<String, dynamic> request)? responder;

  /// Optional responder that may return a [Future] to delay the response
  /// (used to simulate late-arriving plan requests).
  Object? Function(Map<String, dynamic> request)? asyncResponder;
  final List<_QueuedFrame> _frames = [];
  final List<Completer<void>> _readWaiters = [];
  bool _closed = false;
  bool _awaitingResponse = false;

  void emitEvent(Map<String, dynamic> frame) {
    _enqueueFrame(frame, isResponse: false);
  }

  Future<void> write(Uint8List frame) async {
    await Future<void>.delayed(const Duration(milliseconds: 1));
    if (_awaitingResponse) {
      throw StateError(
        'a second request was written before the first response was read',
      );
    }
    final request = _codec.decodeJson(frame);
    final requestId = request['requestId'] as String? ?? 'handshake';
    writtenRequestIds.add(requestId);
    final command = request['command'];
    if (command is String) writtenCommands.add(command);
    final query = request['query'];
    if (query is String) writtenQueries.add(query);
    final payload = request['payload'];
    if (payload is Map<String, dynamic>) writtenPayloads.add(payload);
    if (command is String && payload is Map<String, dynamic>) {
      writtenCommandPayloads.add(payload);
    }
    final async = asyncResponder;
    if (async != null) {
      final result = async(request);
      if (result is Future) {
        final awaited = await result;
        if (awaited is Map<String, dynamic>) {
          _enqueueFrame(awaited, isResponse: true);
        }
        _awaitingResponse = true;
        return;
      }
      if (result is Map<String, dynamic>) {
        _enqueueFrame(result, isResponse: true);
        _awaitingResponse = true;
        return;
      }
      _awaitingResponse = true;
      return;
    }
    final response = responder?.call(request);
    if (response == null) {
      _awaitingResponse = true;
      return;
    }
    _enqueueFrame(response, isResponse: true);
    _awaitingResponse = true;
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
    if (frame.offset == frame.bytes.length) {
      _frames.removeAt(0);
      if (frame.isResponse) {
        _awaitingResponse = false;
      }
    }
    return chunk;
  }

  Future<void> close() async {
    await Future<void>.delayed(const Duration(milliseconds: 1));
    _closed = true;
    _wakeReaders();
  }

  int get _availableBytes =>
      _frames.isEmpty ? 0 : _frames.first.bytes.length - _frames.first.offset;

  void _enqueueFrame(Map<String, dynamic> value, {required bool isResponse}) {
    _frames.add(_QueuedFrame(_codec.encodeJson(value), isResponse));
    _wakeReaders();
  }

  void _wakeReaders() {
    final waiters = List<Completer<void>>.from(_readWaiters);
    _readWaiters.clear();
    for (final waiter in waiters) {
      if (!waiter.isCompleted) waiter.complete();
    }
  }
}

class _QueuedFrame {
  _QueuedFrame(this.bytes, this.isResponse);

  final Uint8List bytes;
  final bool isResponse;
  int offset = 0;
}

Map<String, dynamic> _okResponse(
  Map<String, dynamic> request,
  Map<String, dynamic> payload,
) => {
  'kind': 'response',
  'protocol': {'major': protocolMajor, 'minor': 0},
  'requestId': request['requestId'],
  'ok': true,
  'payload': payload,
};

Map<String, dynamic> _errorResponse(
  Map<String, dynamic> request,
  String code,
  String message, {
  bool retryable = false,
  Map<String, dynamic>? details,
}) => {
  'kind': 'error',
  'protocol': {'major': protocolMajor, 'minor': 0},
  'requestId': request['requestId'],
  'code': code,
  'message': message,
  'retryable': retryable,
  'details': ?details,
};

const String _scanId = 'scan-w7';
const String _epoch = 'discovery-epoch-1';

Map<String, dynamic> _startResponse(Map<String, dynamic> request) =>
    _okResponse(request, {
      'scanId': _scanId,
      'accepted': true,
      'state': 'running',
      'eventStream': {'streamId': 'local-discovery-events', 'epoch': _epoch},
    });

Map<String, dynamic> _subscribeResponse(Map<String, dynamic> request) =>
    _okResponse(request, {
      'subscriptionId': 'sub-discovery-1',
      'streamId': 'local-discovery-events',
      'cursor': {
        'streamId': 'local-discovery-events',
        'sequence': 0,
        'epoch': _epoch,
      },
      'maxInFlightEvents': 64,
      'maxInFlightBytes': 262144,
    });

Map<String, dynamic> _snapshotResponse(
  Map<String, dynamic> request, {
  required List<Map<String, dynamic>> candidates,
  String state = 'completed',
}) => _okResponse(request, {
  'schemaVersion': 'agent.discovery.snapshot.v1',
  'scanId': _scanId,
  'state': state,
  'candidates': candidates,
  'diagnostics': <Map<String, dynamic>>[],
});

Map<String, dynamic> _candidateProjection({
  required String candidateId,
  required String category,
  required String displayName,
  required String compatibilityState,
  required String lifecycleState,
  String? verificationStatus,
  List<String> models = const <String>[],
}) => {
  'candidateId': candidateId,
  'candidate': {
    'candidateId': candidateId,
    'category': category,
    'connectorId': 'local-$candidateId',
    'runtimeType': category == 'agent_runtime' ? 'acp' : category,
    'displayName': displayName,
    'availability': 'unconfigured',
    'models': models,
    'catalogRevision': null,
    'requiresConfiguration': true,
    'sourceKind': 'executable_inventory',
    'sourceKinds': ['executable_inventory'],
    'trustLevel': 'first_party',
    'verificationAuthority': 'unverified',
    'availabilityAuthority': 'unverified',
    'discoveryAuthority': 'unverified',
    'compatibilityAuthority': 'unverified',
    'authAuthority': 'unverified',
    'healthAuthority': 'unverified',
    'catalogSourceKind': null,
    'catalogTrustLevel': null,
    'catalogAuthority': null,
    'discoveryState': 'identified',
    'compatibilityState': compatibilityState,
    'authState': 'unknown',
    'healthState': 'not_checked',
    'evidenceSummary': ['executable_inventory', 'windows_path_entry'],
    'diagnostics': <Map<String, dynamic>>[],
  },
  if (verificationStatus != null)
    'verification': {
      'candidateId': candidateId,
      'status': verificationStatus,
      'compatibilityState': 'compatible',
      'authState': verificationStatus == 'auth_required'
          ? 'required'
          : 'not_required',
      'requiresConfiguration': false,
      'protocolMajor': 1,
      'agentInfo': {'name': displayName, 'version': '1.0.0'},
      'capabilities': {'loadSession': true, 'promptImage': false},
    },
  'lifecycleState': lifecycleState,
};

Map<String, dynamic> _agentIdentified() => _candidateProjection(
  candidateId: 'candidate-agent',
  category: 'agent_runtime',
  displayName: 'Fixture Agent',
  compatibilityState: 'not_verified',
  lifecycleState: 'identified',
);

Map<String, dynamic> _agentVerified() => _candidateProjection(
  candidateId: 'candidate-agent-verified',
  category: 'agent_runtime',
  displayName: 'Verified Agent',
  compatibilityState: 'compatible',
  lifecycleState: 'verified',
  verificationStatus: 'verified',
  models: const ['model-a', 'model-b'],
);

Map<String, dynamic> _modelRuntime() => _candidateProjection(
  candidateId: 'candidate-model',
  category: 'model_runtime',
  displayName: 'Local Model Runtime',
  compatibilityState: 'compatible',
  lifecycleState: 'identified',
);

Map<String, dynamic> _unknown() => _candidateProjection(
  candidateId: 'candidate-unknown',
  category: 'unknown',
  displayName: 'Unknown Executable',
  compatibilityState: 'adapter_required',
  lifecycleState: 'identified',
);

Map<String, dynamic> _verifyResponse(Map<String, dynamic> request) =>
    _okResponse(request, {
      'scanId': _scanId,
      'candidateId': request['payload']['candidateId'],
      'accepted': true,
      'state': 'verifying',
    });

Map<String, dynamic> _dismissResponse(Map<String, dynamic> request) =>
    _okResponse(request, {
      'scanId': _scanId,
      'candidateId': request['payload']['candidateId'],
      'dismissed': true,
    });

Map<String, dynamic> _planResponse(Map<String, dynamic> request) {
  final candidateId = request['payload']['candidateId'] as String;
  return _okResponse(request, {
    'schemaVersion': 'agent.import.plan.v1',
    'planId': 'plan-$candidateId',
    'scanId': _scanId,
    'candidateId': candidateId,
    'targetProjectId': request['payload']['projectId'],
    'modelSelection': request['payload']['modelSelection'],
    'actions': ['create_connector_profile', 'create_agent_identity'],
    'connector': {'id': 'local-$candidateId', 'displayName': 'Plan Connector'},
    'adapter': {
      'kind': 'acp',
      'protocolMajor': 1,
      'manifestId': 'org.fixture.acp',
      // Core-private material that must never surface in renderer state.
      'manifestSha256': 'a' * 64,
      'candidateBindingDigest': 'b' * 64,
    },
    'capabilities': const {
      'loadSession': true,
      'promptImage': false,
      'promptAudio': false,
      'promptEmbeddedContext': false,
      'mcpHttp': false,
      'mcpSse': false,
      'supportsLogout': false,
    },
    'authRequired': false,
    'modelPolicy': 'connector_default',
    'readOnly': true,
  });
}

Map<String, dynamic> _importResponse(Map<String, dynamic> request) {
  final candidateId = request['payload']['candidateId'] as String;
  return _okResponse(request, {
    'schemaVersion': 'agent.import_local.v1',
    'importId': 'import-$candidateId',
    'connectorId': 'local-$candidateId',
    'agentId': 'agent-$candidateId',
    'projectId': request['payload']['projectId'],
    'reused': false,
    'eventSequence': 7,
  });
}

Widget _host(Widget child) => MaterialApp(
  locale: const Locale('zh'),
  localizationsDelegates: AppLocalizations.localizationsDelegates,
  supportedLocales: AppLocalizations.supportedLocales,
  home: Scaffold(body: child),
);

void _useDesktopSurface(WidgetTester tester) {
  tester.view.physicalSize = const Size(1920, 1080);
  tester.view.devicePixelRatio = 1;
  addTearDown(tester.view.resetPhysicalSize);
  addTearDown(tester.view.resetDevicePixelRatio);
}

CoreIpcClient _clientFor(_ScriptedPipe pipe) {
  return CoreIpcClient.forTesting(
    read: pipe.read,
    write: pipe.write,
    close: pipe.close,
    sessionId: 'session-w7-test',
    serverEpoch: 'core-epoch',
  );
}

void main() {
  testWidgets(
    'scan dialog drives agent.discovery.start with a typed scan and groups '
    'candidates into four categories',
    (tester) async {
      _useDesktopSurface(tester);
      final pipe = _ScriptedPipe();
      pipe.responder = (request) {
        final command = request['command'];
        final query = request['query'];
        if (command == 'agent.discovery.start') return _startResponse(request);
        if (command == 'events.subscribe') return _subscribeResponse(request);
        if (command == 'events.ack' || command == 'events.unsubscribe') {
          return _okResponse(request, <String, dynamic>{});
        }
        if (query == 'agent.discovery.snapshot') {
          return _snapshotResponse(
            request,
            candidates: [
              _agentIdentified(),
              _agentVerified(),
              _modelRuntime(),
              _unknown(),
            ],
          );
        }
        return _errorResponse(request, 'INVALID_COMMAND', 'unexpected request');
      };
      final client = _clientFor(pipe);
      addTearDown(() => unawaited(client.close()));

      var imported = 0;
      await tester.pumpWidget(
        _host(
          LocalAgentScanDialog(
            client: client,
            sessionId: 'session-w7-test',
            projectId: 'project-w7',
            onImported: (_) => imported += 1,
          ),
        ),
      );
      await tester.pumpAndSettle();

      expect(pipe.writtenCommands, contains('agent.discovery.start'));
      expect(
        pipe.writtenQueries,
        isNot(contains('agent.scan_local')),
        reason: 'the legacy flat scan must not be the discovery entry point',
      );
      for (final key in const [
        'discovery-group-agent_runtime',
        'discovery-group-model_runtime',
        'discovery-group-tool_service',
        'discovery-group-unknown',
      ]) {
        expect(
          find.byKey(Key(key)),
          findsOneWidget,
          reason: 'the scan dialog must render the $key group section',
        );
      }
      // The four status dimensions are visible without raw field names.
      expect(find.text('发现'), findsWidgets);
      expect(find.text('协议兼容'), findsWidgets);
      expect(find.text('认证'), findsWidgets);
      expect(find.text('健康'), findsWidgets);
      // Unknown candidates only offer an adapter note, never a direct use.
      expect(find.text('此候选需要选择适配器或清单后才能使用。'), findsWidgets);
      expect(
        find.byKey(const Key('local-agent-import-candidate-unknown')),
        findsNothing,
      );
      // A verified agent offers import; an identified agent offers verify.
      expect(
        find.byKey(const Key('local-agent-import-candidate-agent-verified')),
        findsOneWidget,
      );
      expect(
        find.byKey(const Key('local-agent-verify-candidate-agent')),
        findsOneWidget,
      );
      expect(imported, 0);
    },
  );

  testWidgets(
    'verify requires explicit consent before any agent.discovery.verify call',
    (tester) async {
      _useDesktopSurface(tester);
      final pipe = _ScriptedPipe();
      pipe.responder = (request) {
        final command = request['command'];
        final query = request['query'];
        if (command == 'agent.discovery.start') return _startResponse(request);
        if (command == 'events.subscribe') return _subscribeResponse(request);
        if (command == 'events.ack' || command == 'events.unsubscribe') {
          return _okResponse(request, <String, dynamic>{});
        }
        if (query == 'agent.discovery.snapshot') {
          return _snapshotResponse(request, candidates: [_agentIdentified()]);
        }
        if (command == 'agent.discovery.verify') {
          return _verifyResponse(request);
        }
        return _errorResponse(request, 'INVALID_COMMAND', 'unexpected request');
      };
      final client = _clientFor(pipe);
      addTearDown(() => unawaited(client.close()));

      await tester.pumpWidget(
        _host(
          LocalAgentScanDialog(
            client: client,
            sessionId: 'session-w7-test',
            projectId: 'project-w7',
            onImported: (_) {},
          ),
        ),
      );
      await tester.pumpAndSettle();

      expect(pipe.writtenCommands, isNot(contains('agent.discovery.verify')));
      await tester.tap(
        find.byKey(const Key('local-agent-verify-candidate-agent')),
      );
      await tester.pumpAndSettle();
      expect(find.text('验证兼容性'), findsWidgets);
      expect(pipe.writtenCommands, isNot(contains('agent.discovery.verify')));

      // Cancel the consent: no verify command is sent.
      await tester.tap(
        find.byKey(const Key('local-agent-verify-consent-cancel')),
      );
      await tester.pumpAndSettle();
      expect(pipe.writtenCommands, isNot(contains('agent.discovery.verify')));

      // Agree: exactly one verify command with explicit consent.
      await tester.tap(
        find.byKey(const Key('local-agent-verify-candidate-agent')),
      );
      await tester.pumpAndSettle();
      await tester.tap(
        find.byKey(const Key('local-agent-verify-consent-agree')),
      );
      await tester.pumpAndSettle();
      expect(pipe.writtenCommands, contains('agent.discovery.verify'));
      final verifyPayloads = pipe.writtenPayloads
          .where((payload) => payload['consent'] == true)
          .toList();
      expect(verifyPayloads, isNotEmpty);
    },
  );

  testWidgets(
    'connector-default import: plan -> confirm -> single atomic import with '
    'modelSelection null -> receipt -> projection refresh callback',
    (tester) async {
      _useDesktopSurface(tester);
      final pipe = _ScriptedPipe();
      pipe.responder = (request) {
        final command = request['command'];
        final query = request['query'];
        if (command == 'agent.discovery.start') return _startResponse(request);
        if (command == 'events.subscribe') return _subscribeResponse(request);
        if (command == 'events.ack' || command == 'events.unsubscribe') {
          return _okResponse(request, <String, dynamic>{});
        }
        if (query == 'agent.discovery.snapshot') {
          return _snapshotResponse(request, candidates: [_agentVerified()]);
        }
        if (query == 'agent.import.plan') return _planResponse(request);
        if (command == 'agent.import_local') return _importResponse(request);
        return _errorResponse(request, 'INVALID_COMMAND', 'unexpected request');
      };
      final client = _clientFor(pipe);
      addTearDown(() => unawaited(client.close()));

      var imported = 0;
      LocalAgentImportResult? importedResult;
      await tester.pumpWidget(
        _host(
          LocalAgentScanDialog(
            client: client,
            sessionId: 'session-w7-test',
            projectId: 'project-w7',
            onImported: (result) {
              imported += 1;
              importedResult = result;
            },
          ),
        ),
      );
      await tester.pumpAndSettle();

      await tester.tap(
        find.byKey(const Key('local-agent-import-candidate-agent-verified')),
      );
      await tester.pumpAndSettle();

      // The read-only plan is shown; connector-default is preselected.
      expect(find.byKey(const Key('local-agent-import-plan')), findsOneWidget);
      expect(find.text('只读计划'), findsOneWidget);
      expect(find.text('使用连接器默认模型（无需模型 ID）'), findsWidgets);

      await tester.tap(find.byKey(const Key('local-agent-import-confirm')));
      await tester.pumpAndSettle();

      // Exactly one import with modelSelection null (connector-default).
      final imports = pipe.payloadsForCommand('agent.import_local');
      expect(pipe.writtenCommands, contains('agent.import_local'));
      expect(imports.length, 1);
      expect(imports.single['modelSelection'], isNull);
      expect(
        imports.single.keys,
        containsAll(['scanId', 'candidateId', 'projectId', 'modelSelection']),
      );
      expect(imports.single['scanId'], _scanId);
      expect(imports.single['projectId'], 'project-w7');
      // No plan/binding/fingerprint is ever echoed back.
      expect(imports.single.keys, isNot(contains('planId')));
      expect(imports.single.keys, isNot(contains('candidateBindingDigest')));

      expect(
        find.byKey(const Key('local-agent-import-success')),
        findsOneWidget,
      );
      expect(find.text('导入成功'), findsOneWidget);

      await tester.tap(find.byKey(const Key('local-agent-import-done')));
      await tester.pumpAndSettle();
      expect(imported, 1);
      expect(importedResult?.agentId, 'agent-candidate-agent-verified');
      expect(importedResult?.eventSequence, 7);
    },
  );

  testWidgets('pinned model import submits exactly one normalized model id', (
    tester,
  ) async {
    _useDesktopSurface(tester);
    final pipe = _ScriptedPipe();
    pipe.responder = (request) {
      final command = request['command'];
      final query = request['query'];
      if (command == 'agent.discovery.start') return _startResponse(request);
      if (command == 'events.subscribe') return _subscribeResponse(request);
      if (command == 'events.ack' || command == 'events.unsubscribe') {
        return _okResponse(request, <String, dynamic>{});
      }
      if (query == 'agent.discovery.snapshot') {
        return _snapshotResponse(request, candidates: [_agentVerified()]);
      }
      if (query == 'agent.import.plan') return _planResponse(request);
      if (command == 'agent.import_local') return _importResponse(request);
      return _errorResponse(request, 'INVALID_COMMAND', 'unexpected request');
    };
    final client = _clientFor(pipe);
    addTearDown(() => unawaited(client.close()));

    await tester.pumpWidget(
      _host(
        LocalAgentScanDialog(
          client: client,
          sessionId: 'session-w7-test',
          projectId: 'project-w7',
          onImported: (_) {},
        ),
      ),
    );
    await tester.pumpAndSettle();

    await tester.tap(
      find.byKey(const Key('local-agent-import-candidate-agent-verified')),
    );
    await tester.pumpAndSettle();
    await tester.tap(find.byKey(const Key('local-agent-model-pinned')));
    await tester.pumpAndSettle();
    await tester.tap(
      find.byKey(const Key('local-agent-model-pinned-dropdown')),
    );
    await tester.pumpAndSettle();
    await tester.tap(find.text('model-b').last);
    await tester.pumpAndSettle();

    await tester.tap(find.byKey(const Key('local-agent-import-confirm')));
    await tester.pumpAndSettle();

    final imports = pipe.payloadsForCommand('agent.import_local');
    expect(imports.length, 1, reason: 'pinned model must be a single import');
    expect(imports.single['modelSelection'], 'model-b');
    expect(imports.single.keys, isNot(contains('planId')));
  });

  testWidgets(
    'verify failure keeps the candidate and allows retry; no state loss',
    (tester) async {
      _useDesktopSurface(tester);
      var verifyAttempts = 0;
      final pipe = _ScriptedPipe();
      pipe.responder = (request) {
        final command = request['command'];
        final query = request['query'];
        if (command == 'agent.discovery.start') return _startResponse(request);
        if (command == 'events.subscribe') return _subscribeResponse(request);
        if (command == 'events.ack' || command == 'events.unsubscribe') {
          return _okResponse(request, <String, dynamic>{});
        }
        if (query == 'agent.discovery.snapshot') {
          return _snapshotResponse(request, candidates: [_agentIdentified()]);
        }
        if (command == 'agent.discovery.verify') {
          verifyAttempts += 1;
          if (verifyAttempts == 1) {
            return _errorResponse(
              request,
              'DISCOVERY_SERVICE_SHUTTING_DOWN',
              'discovery is shutting down',
            );
          }
          return _verifyResponse(request);
        }
        return _errorResponse(request, 'INVALID_COMMAND', 'unexpected request');
      };
      final client = _clientFor(pipe);
      addTearDown(() => unawaited(client.close()));

      await tester.pumpWidget(
        _host(
          LocalAgentScanDialog(
            client: client,
            sessionId: 'session-w7-test',
            projectId: 'project-w7',
            onImported: (_) {},
          ),
        ),
      );
      await tester.pumpAndSettle();

      await tester.tap(
        find.byKey(const Key('local-agent-verify-candidate-agent')),
      );
      await tester.pumpAndSettle();
      await tester.tap(
        find.byKey(const Key('local-agent-verify-consent-agree')),
      );
      await tester.pumpAndSettle();

      expect(verifyAttempts, 1);
      // W7.1: the allowlisted code maps to a fixed localized message; the raw
      // Core message ('discovery is shutting down') must not surface.
      expect(find.text('服务正在关闭，请稍后重试。'), findsOneWidget);
      expect(find.textContaining('shutting down'), findsNothing);
      // The candidate card remains with its verify button for retry.
      expect(
        find.byKey(const Key('local-agent-verify-candidate-agent')),
        findsOneWidget,
      );

      await tester.tap(
        find.byKey(const Key('local-agent-verify-candidate-agent')),
      );
      await tester.pumpAndSettle();
      await tester.tap(
        find.byKey(const Key('local-agent-verify-consent-agree')),
      );
      await tester.pumpAndSettle();
      expect(verifyAttempts, 2);
      expect(tester.takeException(), isNull);
    },
  );

  testWidgets('repeated confirm taps produce exactly one import', (
    tester,
  ) async {
    _useDesktopSurface(tester);
    var importCalls = 0;
    final pipe = _ScriptedPipe();
    pipe.responder = (request) {
      final command = request['command'];
      final query = request['query'];
      if (command == 'agent.discovery.start') return _startResponse(request);
      if (command == 'events.subscribe') return _subscribeResponse(request);
      if (command == 'events.ack' || command == 'events.unsubscribe') {
        return _okResponse(request, <String, dynamic>{});
      }
      if (query == 'agent.discovery.snapshot') {
        return _snapshotResponse(request, candidates: [_agentVerified()]);
      }
      if (query == 'agent.import.plan') return _planResponse(request);
      if (command == 'agent.import_local') {
        importCalls += 1;
        return _importResponse(request);
      }
      return _errorResponse(request, 'INVALID_COMMAND', 'unexpected request');
    };
    final client = _clientFor(pipe);
    addTearDown(() => unawaited(client.close()));

    await tester.pumpWidget(
      _host(
        LocalAgentScanDialog(
          client: client,
          sessionId: 'session-w7-test',
          projectId: 'project-w7',
          onImported: (_) {},
        ),
      ),
    );
    await tester.pumpAndSettle();
    await tester.tap(
      find.byKey(const Key('local-agent-import-candidate-agent-verified')),
    );
    await tester.pumpAndSettle();
    final confirm = find.byKey(const Key('local-agent-import-confirm'));
    await tester.tap(confirm);
    await tester.pump();
    // A second tap while the first import is still in flight is ignored.
    await tester.tap(confirm, warnIfMissed: false);
    await tester.pumpAndSettle();
    expect(importCalls, 1);
    expect(tester.takeException(), isNull);
  });

  testWidgets('closing the dialog mid-verify does not setState after dispose', (
    tester,
  ) async {
    _useDesktopSurface(tester);
    final pipe = _ScriptedPipe();
    pipe.responder = (request) {
      final command = request['command'];
      final query = request['query'];
      if (command == 'agent.discovery.start') return _startResponse(request);
      if (command == 'events.subscribe') return _subscribeResponse(request);
      if (command == 'events.ack' || command == 'events.unsubscribe') {
        return _okResponse(request, <String, dynamic>{});
      }
      if (query == 'agent.discovery.snapshot') {
        return _snapshotResponse(request, candidates: [_agentIdentified()]);
      }
      // Verify is left unanswered: the request stays in flight while the
      // dialog is closed, then the client close fails it.
      if (command == 'agent.discovery.verify') return null;
      return _errorResponse(request, 'INVALID_COMMAND', 'unexpected request');
    };
    final client = _clientFor(pipe);
    addTearDown(() => unawaited(client.close()));

    await tester.pumpWidget(
      _host(
        LocalAgentScanDialog(
          client: client,
          sessionId: 'session-w7-test',
          projectId: 'project-w7',
          onImported: (_) {},
        ),
      ),
    );
    await tester.pumpAndSettle();
    await tester.tap(
      find.byKey(const Key('local-agent-verify-candidate-agent')),
    );
    await tester.pumpAndSettle();
    await tester.tap(find.byKey(const Key('local-agent-verify-consent-agree')));
    // Advance fake time so the verify request is written and in flight
    // before the dialog is closed. A bounded pump is used instead of
    // pumpAndSettle because the busy indicator animates forever.
    await tester.pump(const Duration(milliseconds: 20));
    // The dialog is closed while verification is still in flight.
    await tester.pumpWidget(const SizedBox.shrink());
    await tester.pump();
    // Closing the client fails the pending request; the late completion
    // must not call setState on the disposed dialog. Fake time is advanced
    // so the transport close timer can complete.
    final closeFuture = client.close();
    await tester.pump(const Duration(milliseconds: 20));
    await closeFuture;
    await tester.pumpAndSettle();
    expect(tester.takeException(), isNull);
  });

  testWidgets('replay gap on subscription falls back to snapshot refresh', (
    tester,
  ) async {
    _useDesktopSurface(tester);
    final pipe = _ScriptedPipe();
    pipe.responder = (request) {
      final command = request['command'];
      final query = request['query'];
      if (command == 'agent.discovery.start') return _startResponse(request);
      if (command == 'events.subscribe') {
        return _errorResponse(
          request,
          'REPLAY_GAP',
          'replay gap',
          details: {
            'streamId': 'local-discovery-events',
            'epoch': _epoch,
            'requiresSnapshot': true,
          },
        );
      }
      if (query == 'agent.discovery.snapshot') {
        return _snapshotResponse(request, candidates: [_agentIdentified()]);
      }
      return _errorResponse(request, 'INVALID_COMMAND', 'unexpected request');
    };
    final client = _clientFor(pipe);
    addTearDown(() => unawaited(client.close()));

    await tester.pumpWidget(
      _host(
        LocalAgentScanDialog(
          client: client,
          sessionId: 'session-w7-test',
          projectId: 'project-w7',
          onImported: (_) {},
        ),
      ),
    );
    await tester.pumpAndSettle();

    expect(find.textContaining('事件流出现缺口'), findsOneWidget);
    expect(
      find.byKey(const Key('local-agent-verify-candidate-agent')),
      findsOneWidget,
      reason: 'the snapshot fallback must still render the candidate',
    );
    expect(tester.takeException(), isNull);
  });

  testWidgets('dismiss hides the candidate', (tester) async {
    _useDesktopSurface(tester);
    var dismissed = false;
    final pipe = _ScriptedPipe();
    pipe.responder = (request) {
      final command = request['command'];
      final query = request['query'];
      if (command == 'agent.discovery.start') return _startResponse(request);
      if (command == 'events.subscribe') return _subscribeResponse(request);
      if (command == 'events.ack' || command == 'events.unsubscribe') {
        return _okResponse(request, <String, dynamic>{});
      }
      if (query == 'agent.discovery.snapshot') {
        return _snapshotResponse(
          request,
          candidates: dismissed
              ? <Map<String, dynamic>>[]
              : [_agentIdentified()],
        );
      }
      if (command == 'agent.discovery.dismiss') {
        dismissed = true;
        return _dismissResponse(request);
      }
      return _errorResponse(request, 'INVALID_COMMAND', 'unexpected request');
    };
    final client = _clientFor(pipe);
    addTearDown(() => unawaited(client.close()));

    await tester.pumpWidget(
      _host(
        LocalAgentScanDialog(
          client: client,
          sessionId: 'session-w7-test',
          projectId: 'project-w7',
          onImported: (_) {},
        ),
      ),
    );
    await tester.pumpAndSettle();
    expect(
      find.byKey(const Key('local-agent-verify-candidate-agent')),
      findsOneWidget,
    );
    await tester.tap(
      find.byKey(const Key('local-agent-dismiss-candidate-agent')),
    );
    await tester.pumpAndSettle();
    expect(pipe.writtenCommands, contains('agent.discovery.dismiss'));
    expect(find.text('没有发现本地候选。'), findsOneWidget);
  });

  testWidgets(
    'renderer text and semantics never expose fixture paths, pids, ports or '
    'credentials',
    (tester) async {
      _useDesktopSurface(tester);
      final pipe = _ScriptedPipe();
      pipe.responder = (request) {
        final command = request['command'];
        final query = request['query'];
        if (command == 'agent.discovery.start') return _startResponse(request);
        if (command == 'events.subscribe') return _subscribeResponse(request);
        if (command == 'events.ack' || command == 'events.unsubscribe') {
          return _okResponse(request, <String, dynamic>{});
        }
        if (query == 'agent.discovery.snapshot') {
          return _snapshotResponse(
            request,
            candidates: [
              _agentIdentified(),
              _agentVerified(),
              _modelRuntime(),
              _unknown(),
            ],
          );
        }
        if (query == 'agent.import.plan') return _planResponse(request);
        if (command == 'agent.import_local') return _importResponse(request);
        return _errorResponse(request, 'INVALID_COMMAND', 'unexpected request');
      };
      final client = _clientFor(pipe);
      addTearDown(() => unawaited(client.close()));

      await tester.pumpWidget(
        _host(
          LocalAgentScanDialog(
            client: client,
            sessionId: 'session-w7-test',
            projectId: 'project-w7',
            onImported: (_) {},
          ),
        ),
      );
      await tester.pumpAndSettle();

      // Open the import dialog so its texts are in the tree too.
      await tester.tap(
        find.byKey(const Key('local-agent-import-candidate-agent-verified')),
      );
      await tester.pumpAndSettle();

      final texts = tester
          .widgetList<Text>(find.byType(Text))
          .map((t) {
            final data = t.data ?? t.textSpan?.toPlainText() ?? '';
            return data;
          })
          .join('\n')
          .toLowerCase();
      for (final forbidden in const [
        'c:\\',
        '.exe',
        'root.pid',
        'descendant.pid',
        'fixture-bin',
        'fixture-model',
        'authorization',
        'cookie',
        'credential',
        'secret_token',
        'candidatebindingdigest',
        'manifestsha256',
        'locatorref',
        'runtime.json',
        'token',
      ]) {
        expect(
          texts.contains(forbidden),
          isFalse,
          reason: 'renderer text must not expose: $forbidden',
        );
      }
      expect(tester.takeException(), isNull);
    },
  );

  testWidgets(
    'scan dialog fits the three required window sizes in light and dark',
    (tester) async {
      _useDesktopSurface(tester);
      for (final size in const [
        Size(1366, 768),
        Size(1600, 900),
        Size(1920, 1080),
      ]) {
        for (final dark in const [false, true]) {
          final pipe = _ScriptedPipe();
          pipe.responder = (request) {
            final command = request['command'];
            final query = request['query'];
            if (command == 'agent.discovery.start') {
              return _startResponse(request);
            }
            if (command == 'events.subscribe') {
              return _subscribeResponse(request);
            }
            if (command == 'events.ack' || command == 'events.unsubscribe') {
              return _okResponse(request, <String, dynamic>{});
            }
            if (query == 'agent.discovery.snapshot') {
              return _snapshotResponse(
                request,
                candidates: [
                  _agentIdentified(),
                  _agentVerified(),
                  _modelRuntime(),
                  _unknown(),
                ],
              );
            }
            return _errorResponse(
              request,
              'INVALID_COMMAND',
              'unexpected request',
            );
          };
          final client = _clientFor(pipe);
          addTearDown(() => unawaited(client.close()));

          tester.view.physicalSize = size;
          tester.view.devicePixelRatio = 1;
          addTearDown(tester.view.resetPhysicalSize);
          addTearDown(tester.view.resetDevicePixelRatio);
          await tester.pumpWidget(
            MaterialApp(
              locale: const Locale('zh'),
              localizationsDelegates: AppLocalizations.localizationsDelegates,
              supportedLocales: AppLocalizations.supportedLocales,
              theme: dark ? ThemeData.dark() : ThemeData.light(),
              home: Scaffold(
                body: LocalAgentScanDialog(
                  client: client,
                  sessionId: 'session-w7-test',
                  projectId: 'project-w7',
                  onImported: (_) {},
                ),
              ),
            ),
          );
          await tester.pumpAndSettle();
          expect(
            tester.takeException(),
            isNull,
            reason: 'no overflow at $size dark=$dark',
          );
          expect(find.text('智能体'), findsWidgets);
        }
      }
    },
  );

  // ---- W7.1 P1-1: import plan is bound to the exact business intent ----

  testWidgets(
    'W7.1 red: plan failure after switching to pinned disables confirm and '
    'never imports',
    (tester) async {
      _useDesktopSurface(tester);
      var importCalls = 0;
      var failPlan = false;
      final pipe = _ScriptedPipe();
      pipe.asyncResponder = (request) {
        final command = request['command'];
        final query = request['query'];
        if (command == 'agent.discovery.start') return _startResponse(request);
        if (command == 'events.subscribe') return _subscribeResponse(request);
        if (command == 'events.ack' || command == 'events.unsubscribe') {
          return _okResponse(request, <String, dynamic>{});
        }
        if (query == 'agent.discovery.snapshot') {
          return _snapshotResponse(request, candidates: [_agentVerified()]);
        }
        if (query == 'agent.import.plan') {
          if (failPlan) {
            return _errorResponse(
              request,
              'IMPORT_PLAN_FAILED',
              'plan unavailable',
            );
          }
          return _planResponse(request);
        }
        if (command == 'agent.import_local') {
          importCalls += 1;
          return _importResponse(request);
        }
        return _errorResponse(request, 'INVALID_COMMAND', 'unexpected request');
      };
      await tester.pumpWidget(
        _host(
          LocalAgentScanDialog(
            client: _clientFor(pipe),
            sessionId: 'session-w7-test',
            projectId: 'project-w7',
            onImported: (_) {},
          ),
        ),
      );
      await tester.pumpAndSettle();
      await tester.tap(
        find.byKey(const Key('local-agent-import-candidate-agent-verified')),
      );
      await tester.pumpAndSettle();
      // The initial connector-default plan succeeded; switching to pinned now
      // fails, so the OLD default plan must not remain confirmable.
      failPlan = true;
      await tester.tap(find.byKey(const Key('local-agent-model-pinned')));
      await tester.pumpAndSettle();
      final confirm = find.byKey(const Key('local-agent-import-confirm'));
      expect(
        tester.widget<FilledButton>(confirm).onPressed,
        isNull,
        reason: 'W7.1: after the pinned plan failed, confirm must be disabled',
      );
      await tester.tap(confirm, warnIfMissed: false);
      await tester.pump();
      expect(importCalls, 0);
      expect(tester.takeException(), isNull);
    },
  );

  testWidgets(
    'W7.1 red: plan failure after switching back to default disables confirm',
    (tester) async {
      _useDesktopSurface(tester);
      var importCalls = 0;
      var failPlan = false;
      final pipe = _ScriptedPipe();
      pipe.asyncResponder = (request) {
        final command = request['command'];
        final query = request['query'];
        if (command == 'agent.discovery.start') return _startResponse(request);
        if (command == 'events.subscribe') return _subscribeResponse(request);
        if (command == 'events.ack' || command == 'events.unsubscribe') {
          return _okResponse(request, <String, dynamic>{});
        }
        if (query == 'agent.discovery.snapshot') {
          return _snapshotResponse(request, candidates: [_agentVerified()]);
        }
        if (query == 'agent.import.plan') {
          if (failPlan) {
            return _errorResponse(
              request,
              'IMPORT_PLAN_FAILED',
              'plan unavailable',
            );
          }
          return _planResponse(request);
        }
        if (command == 'agent.import_local') {
          importCalls += 1;
          return _importResponse(request);
        }
        return _errorResponse(request, 'INVALID_COMMAND', 'unexpected request');
      };
      await tester.pumpWidget(
        _host(
          LocalAgentScanDialog(
            client: _clientFor(pipe),
            sessionId: 'session-w7-test',
            projectId: 'project-w7',
            onImported: (_) {},
          ),
        ),
      );
      await tester.pumpAndSettle();
      await tester.tap(
        find.byKey(const Key('local-agent-import-candidate-agent-verified')),
      );
      await tester.pumpAndSettle();
      // Pinned plan succeeds first.
      await tester.tap(find.byKey(const Key('local-agent-model-pinned')));
      await tester.pumpAndSettle();
      // Switching back to default now fails.
      failPlan = true;
      await tester.tap(
        find.byKey(const Key('local-agent-model-connector-default')),
      );
      await tester.pumpAndSettle();
      final confirm = find.byKey(const Key('local-agent-import-confirm'));
      expect(
        tester.widget<FilledButton>(confirm).onPressed,
        isNull,
        reason:
            'W7.1: stale default plan must not remain confirmable after failure',
      );
      await tester.tap(confirm, warnIfMissed: false);
      await tester.pump();
      expect(importCalls, 0);
      expect(tester.takeException(), isNull);
    },
  );

  testWidgets(
    'W7.1 red: a late-arriving stale plan request cannot restore confirm',
    (tester) async {
      _useDesktopSurface(tester);
      var importCalls = 0;
      final pipe = _ScriptedPipe();
      final defaultPlanCompleter = Completer<Map<String, dynamic>>();
      pipe.asyncResponder = (request) {
        final command = request['command'];
        final query = request['query'];
        if (command == 'agent.discovery.start') return _startResponse(request);
        if (command == 'events.subscribe') return _subscribeResponse(request);
        if (command == 'events.ack' || command == 'events.unsubscribe') {
          return _okResponse(request, <String, dynamic>{});
        }
        if (query == 'agent.discovery.snapshot') {
          return _snapshotResponse(request, candidates: [_agentVerified()]);
        }
        if (query == 'agent.import.plan') {
          final selection = request['payload']['modelSelection'];
          if (selection == null) {
            // The default plan request hangs until the test releases it.
            return defaultPlanCompleter.future;
          }
          return _errorResponse(
            request,
            'IMPORT_PLAN_FAILED',
            'pinned plan unavailable',
          );
        }
        if (command == 'agent.import_local') {
          importCalls += 1;
          return _importResponse(request);
        }
        return _errorResponse(request, 'INVALID_COMMAND', 'unexpected request');
      };
      await tester.pumpWidget(
        _host(
          LocalAgentScanDialog(
            client: _clientFor(pipe),
            sessionId: 'session-w7-test',
            projectId: 'project-w7',
            onImported: (_) {},
          ),
        ),
      );
      await tester.pumpAndSettle();
      await tester.tap(
        find.byKey(const Key('local-agent-import-candidate-agent-verified')),
      );
      // The import dialog opens with the default plan request in flight and
      // hanging; the plan-loading spinner animates, so use bounded pumps.
      await tester.pump();
      await tester.pump(const Duration(milliseconds: 300));
      // Switch to pinned while the default request is still pending.
      await tester.tap(find.byKey(const Key('local-agent-model-pinned')));
      await tester.pump(const Duration(milliseconds: 50));
      final confirm = find.byKey(const Key('local-agent-import-confirm'));
      expect(tester.widget<FilledButton>(confirm).onPressed, isNull);
      // The STALE default plan now arrives late; it must not re-enable confirm.
      defaultPlanCompleter.complete(
        _planResponse({
          'requestId': 'late-default-plan',
          'payload': {
            'candidateId': 'candidate-agent-verified',
            'projectId': 'project-w7',
            'modelSelection': null,
          },
        }),
      );
      // Default response is delivered (discarded as stale), then the pinned
      // request writes and its error is delivered; each hop needs a pump.
      await tester.pump(const Duration(milliseconds: 50));
      await tester.pump(const Duration(milliseconds: 50));
      await tester.pumpAndSettle();
      expect(
        tester.widget<FilledButton>(confirm).onPressed,
        isNull,
        reason:
            'W7.1: a stale plan for a previous selection must not restore confirm',
      );
      await tester.tap(confirm, warnIfMissed: false);
      await tester.pump();
      expect(importCalls, 0);
      expect(tester.takeException(), isNull);
    },
  );

  testWidgets(
    'W7.1 red: a plan that does not match the request is rejected with zero imports',
    (tester) async {
      _useDesktopSurface(tester);
      var importCalls = 0;
      final pipe = _ScriptedPipe();
      pipe.asyncResponder = (request) {
        final command = request['command'];
        final query = request['query'];
        if (command == 'agent.discovery.start') return _startResponse(request);
        if (command == 'events.subscribe') return _subscribeResponse(request);
        if (command == 'events.ack' || command == 'events.unsubscribe') {
          return _okResponse(request, <String, dynamic>{});
        }
        if (query == 'agent.discovery.snapshot') {
          return _snapshotResponse(request, candidates: [_agentVerified()]);
        }
        if (query == 'agent.import.plan') {
          final payload = Map<String, dynamic>.from(
            _planResponse(request)['payload'] as Map<String, dynamic>,
          );
          // The plan response claims a different target project than requested.
          payload['targetProjectId'] = 'project-other';
          return _okResponse(request, payload);
        }
        if (command == 'agent.import_local') {
          importCalls += 1;
          return _importResponse(request);
        }
        return _errorResponse(request, 'INVALID_COMMAND', 'unexpected request');
      };
      await tester.pumpWidget(
        _host(
          LocalAgentScanDialog(
            client: _clientFor(pipe),
            sessionId: 'session-w7-test',
            projectId: 'project-w7',
            onImported: (_) {},
          ),
        ),
      );
      await tester.pumpAndSettle();
      await tester.tap(
        find.byKey(const Key('local-agent-import-candidate-agent-verified')),
      );
      await tester.pumpAndSettle();
      final confirm = find.byKey(const Key('local-agent-import-confirm'));
      expect(
        tester.widget<FilledButton>(confirm).onPressed,
        isNull,
        reason: 'W7.1: a mismatched plan must not be confirmable',
      );
      await tester.tap(confirm, warnIfMissed: false);
      await tester.pump();
      expect(importCalls, 0);
      expect(tester.takeException(), isNull);
    },
  );

  // ---- W7.1 P1-2: renderer-safe error mapping ----

  testWidgets('W7.1 red: scan dialog never surfaces raw Core error text', (
    tester,
  ) async {
    _useDesktopSurface(tester);
    final pipe = _ScriptedPipe();
    pipe.responder = (request) {
      final command = request['command'];
      if (command == 'agent.discovery.start') {
        return _errorResponse(
          request,
          'SCAN_FAILED',
          r'C:\Users\user\secret.db token=abc123 locator=C:\bin\agent.exe '
              r'Authorization: Bearer xyz Cookie: session=1 sqlite: disk I/O error',
        );
      }
      return _errorResponse(request, 'INVALID_COMMAND', 'unexpected request');
    };
    await tester.pumpWidget(
      _host(
        LocalAgentScanDialog(
          client: _clientFor(pipe),
          sessionId: 'session-w7-test',
          projectId: 'project-w7',
          onImported: (_) {},
        ),
      ),
    );
    await tester.pumpAndSettle();
    final texts = tester
        .widgetList<Text>(find.byType(Text))
        .map((t) => t.data ?? t.textSpan?.toPlainText() ?? '')
        .join('\n');
    for (final forbidden in const [
      r'C:\Users\user\secret.db',
      'token=abc123',
      r'locator=C:\bin\agent.exe',
      'Authorization: Bearer xyz',
      'Cookie: session=1',
      'sqlite: disk I/O error',
    ]) {
      expect(
        texts.contains(forbidden),
        isFalse,
        reason:
            'W7.1: the raw Core error text must never reach the UI: $forbidden',
      );
    }
    expect(tester.takeException(), isNull);
  });

  testWidgets(
    'W7.1 red: allowlisted code maps to localized text and unknown code maps '
    'to a generic safe message',
    (tester) async {
      for (final (code, expectedText, forbiddenSubstring) in [
        (
          'DISCOVERY_IDENTITY_CHANGED',
          '候选身份已变化，请重新扫描后再试。',
          r'C:\Users\user\secret.db',
        ),
        (
          'IMPORT_PERSISTENCE_FAILED',
          '持久化失败，未能完成导入。',
          'sqlite: disk I/O error',
        ),
        ('WEIRD_FUTURE_CODE', '操作失败，请重试。', 'some raw detail'),
      ]) {
        // Tear down the previous iteration's tree so the dialog State is
        // recreated (same widget type would otherwise be reused in place).
        await tester.pumpWidget(const SizedBox.shrink());
        await tester.pump();
        _useDesktopSurface(tester);
        final pipe = _ScriptedPipe();
        pipe.responder = (request) {
          final command = request['command'];
          if (command == 'agent.discovery.start') {
            return _errorResponse(
              request,
              code,
              'raw message with $forbiddenSubstring and token=zzz',
            );
          }
          return _errorResponse(
            request,
            'INVALID_COMMAND',
            'unexpected request',
          );
        };
        await tester.pumpWidget(
          _host(
            LocalAgentScanDialog(
              client: _clientFor(pipe),
              sessionId: 'session-w7-test',
              projectId: 'project-w7',
              onImported: (_) {},
            ),
          ),
        );
        await tester.pumpAndSettle();
        expect(
          find.text(expectedText),
          findsWidgets,
          reason: 'W7.1: code $code must map to the localized fixed text',
        );
        final texts = tester
            .widgetList<Text>(find.byType(Text))
            .map((t) => t.data ?? t.textSpan?.toPlainText() ?? '')
            .join('\n');
        expect(
          texts.contains(forbiddenSubstring),
          isFalse,
          reason: 'W7.1: raw error content must not appear for code $code',
        );
        expect(texts.contains('token=zzz'), isFalse);
        expect(tester.takeException(), isNull);
      }
    },
  );

  testWidgets(
    'W7.1 red: import dialog import failure shows a generic safe message',
    (tester) async {
      _useDesktopSurface(tester);
      final pipe = _ScriptedPipe();
      pipe.asyncResponder = (request) {
        final command = request['command'];
        final query = request['query'];
        if (command == 'agent.discovery.start') return _startResponse(request);
        if (command == 'events.subscribe') return _subscribeResponse(request);
        if (command == 'events.ack' || command == 'events.unsubscribe') {
          return _okResponse(request, <String, dynamic>{});
        }
        if (query == 'agent.discovery.snapshot') {
          return _snapshotResponse(request, candidates: [_agentVerified()]);
        }
        if (query == 'agent.import.plan') return _planResponse(request);
        if (command == 'agent.import_local') {
          return _errorResponse(
            request,
            'IMPORT_PERSISTENCE_FAILED',
            r'C:\Users\user\secret.db sqlite: disk I/O error',
          );
        }
        return _errorResponse(request, 'INVALID_COMMAND', 'unexpected request');
      };
      await tester.pumpWidget(
        _host(
          LocalAgentScanDialog(
            client: _clientFor(pipe),
            sessionId: 'session-w7-test',
            projectId: 'project-w7',
            onImported: (_) {},
          ),
        ),
      );
      await tester.pumpAndSettle();
      await tester.tap(
        find.byKey(const Key('local-agent-import-candidate-agent-verified')),
      );
      await tester.pumpAndSettle();
      await tester.tap(find.byKey(const Key('local-agent-import-confirm')));
      await tester.pumpAndSettle();
      expect(find.text('持久化失败，未能完成导入。'), findsWidgets);
      final texts = tester
          .widgetList<Text>(find.byType(Text))
          .map((t) => t.data ?? t.textSpan?.toPlainText() ?? '')
          .join('\n');
      expect(texts.contains(r'C:\Users\user\secret.db'), isFalse);
      expect(texts.contains('sqlite: disk I/O error'), isFalse);
      expect(tester.takeException(), isNull);
    },
  );

  // ---- W7.2 P1-2: no-model candidates must not downgrade pinned to
  // connector-default ----

  Map<String, dynamic> noModelVerifiedCandidate() => _candidateProjection(
    candidateId: 'candidate-no-model',
    category: 'agent_runtime',
    displayName: 'No Model Agent',
    compatibilityState: 'compatible',
    lifecycleState: 'verified',
    verificationStatus: 'verified',
    models: const <String>[],
  );

  Future<void> openImportDialogFor(
    WidgetTester tester,
    _ScriptedPipe pipe,
    Map<String, dynamic> candidate,
  ) async {
    pipe.asyncResponder = (request) {
      final command = request['command'];
      final query = request['query'];
      if (command == 'agent.discovery.start') return _startResponse(request);
      if (command == 'events.subscribe') return _subscribeResponse(request);
      if (command == 'events.ack' || command == 'events.unsubscribe') {
        return _okResponse(request, <String, dynamic>{});
      }
      if (query == 'agent.discovery.snapshot') {
        return _snapshotResponse(request, candidates: [candidate]);
      }
      if (query == 'agent.import.plan') return _planResponse(request);
      if (command == 'agent.import_local') return _importResponse(request);
      return _errorResponse(request, 'INVALID_COMMAND', 'unexpected request');
    };
    await tester.pumpWidget(
      _host(
        LocalAgentScanDialog(
          client: _clientFor(pipe),
          sessionId: 'session-w7-test',
          projectId: 'project-w7',
          onImported: (_) {},
        ),
      ),
    );
    await tester.pumpAndSettle();
    await tester.tap(
      find.byKey(const Key('local-agent-import-candidate-no-model')),
    );
    await tester.pumpAndSettle();
  }

  testWidgets(
    'W7.2 red: no-model candidate keeps pinned inert with zero plan and zero '
    'import side effects',
    (tester) async {
      _useDesktopSurface(tester);
      var importCalls = 0;
      final pipe = _ScriptedPipe();
      await openImportDialogFor(tester, pipe, noModelVerifiedCandidate());
      // The connector-default plan is already loaded; count its request.
      final planCallsBefore = pipe.writtenQueries
          .where((q) => q == 'agent.import.plan')
          .length;
      expect(planCallsBefore, 1);
      // Tapping pinned must not switch selection or fire any request.
      await tester.tap(
        find.byKey(const Key('local-agent-model-pinned')),
        warnIfMissed: false,
      );
      await tester.pumpAndSettle();
      expect(
        pipe.writtenQueries.where((q) => q == 'agent.import.plan').length,
        planCallsBefore,
        reason:
            'W7.2: tapping pinned on a no-model candidate must not issue '
            'a new plan request',
      );
      expect(importCalls, 0);
      expect(tester.takeException(), isNull);
    },
  );

  testWidgets(
    'W7.2 red: pinned without a model never imports (zero plan/import deltas)',
    (tester) async {
      _useDesktopSurface(tester);
      final pipe = _ScriptedPipe();
      await openImportDialogFor(tester, pipe, noModelVerifiedCandidate());
      final planCallsBefore = pipe.writtenQueries
          .where((q) => q == 'agent.import.plan')
          .length;
      int importCount() =>
          pipe.writtenCommands.where((c) => c == 'agent.import_local').length;
      // A stray state could still flip the selection; the pinned tap must be
      // inert: no new plan request, no import, and the connector-default plan
      // remains the only confirmable path.
      await tester.tap(
        find.byKey(const Key('local-agent-model-pinned')),
        warnIfMissed: false,
      );
      await tester.pumpAndSettle();
      expect(
        pipe.writtenQueries.where((q) => q == 'agent.import.plan').length,
        planCallsBefore,
        reason: 'W7.2: no additional plan request for an invalid pinned state',
      );
      expect(importCount(), 0);
      final confirm = find.byKey(const Key('local-agent-import-confirm'));
      expect(
        tester.widget<FilledButton>(confirm).onPressed,
        isNotNull,
        reason: 'W7.2: the connector-default plan stays confirmable',
      );
      // Confirming imports exactly once with modelSelection null (the legal
      // connector-default import; the pinned attempt added nothing).
      await tester.tap(confirm);
      await tester.pumpAndSettle();
      expect(importCount(), 1);
      final imports = pipe.payloadsForCommand('agent.import_local');
      expect(imports.length, 1);
      expect(imports.single['modelSelection'], isNull);
      expect(tester.takeException(), isNull);
    },
  );

  testWidgets(
    'W7.2 red: connector-default on a no-model candidate still plans and '
    'imports once with modelSelection null',
    (tester) async {
      _useDesktopSurface(tester);
      var importCalls = 0;
      final pipe = _ScriptedPipe();
      pipe.asyncResponder = (request) {
        final command = request['command'];
        final query = request['query'];
        if (command == 'agent.discovery.start') return _startResponse(request);
        if (command == 'events.subscribe') return _subscribeResponse(request);
        if (command == 'events.ack' || command == 'events.unsubscribe') {
          return _okResponse(request, <String, dynamic>{});
        }
        if (query == 'agent.discovery.snapshot') {
          return _snapshotResponse(
            request,
            candidates: [noModelVerifiedCandidate()],
          );
        }
        if (query == 'agent.import.plan') return _planResponse(request);
        if (command == 'agent.import_local') {
          importCalls += 1;
          return _importResponse(request);
        }
        return _errorResponse(request, 'INVALID_COMMAND', 'unexpected request');
      };
      await tester.pumpWidget(
        _host(
          LocalAgentScanDialog(
            client: _clientFor(pipe),
            sessionId: 'session-w7-test',
            projectId: 'project-w7',
            onImported: (_) {},
          ),
        ),
      );
      await tester.pumpAndSettle();
      await tester.tap(
        find.byKey(const Key('local-agent-import-candidate-no-model')),
      );
      await tester.pumpAndSettle();
      // The connector-default plan is loaded and confirmable; import once.
      await tester.tap(find.byKey(const Key('local-agent-import-confirm')));
      await tester.pumpAndSettle();
      final imports = pipe.payloadsForCommand('agent.import_local');
      expect(imports.length, 1);
      expect(imports.single['modelSelection'], isNull);
      expect(imports.single['projectId'], 'project-w7');
      expect(importCalls, 1);
      expect(tester.takeException(), isNull);
    },
  );

  testWidgets(
    'W7.2 red: a plan with an unexpected top-level field cannot import',
    (tester) async {
      _useDesktopSurface(tester);
      var importCalls = 0;
      final pipe = _ScriptedPipe();
      pipe.asyncResponder = (request) {
        final command = request['command'];
        final query = request['query'];
        if (command == 'agent.discovery.start') return _startResponse(request);
        if (command == 'events.subscribe') return _subscribeResponse(request);
        if (command == 'events.ack' || command == 'events.unsubscribe') {
          return _okResponse(request, <String, dynamic>{});
        }
        if (query == 'agent.discovery.snapshot') {
          return _snapshotResponse(request, candidates: [_agentVerified()]);
        }
        if (query == 'agent.import.plan') {
          final payload = Map<String, dynamic>.from(
            _planResponse(request)['payload'] as Map<String, dynamic>,
          );
          payload['futureField'] = false;
          return _okResponse(request, payload);
        }
        if (command == 'agent.import_local') {
          importCalls += 1;
          return _importResponse(request);
        }
        return _errorResponse(request, 'INVALID_COMMAND', 'unexpected request');
      };
      await tester.pumpWidget(
        _host(
          LocalAgentScanDialog(
            client: _clientFor(pipe),
            sessionId: 'session-w7-test',
            projectId: 'project-w7',
            onImported: (_) {},
          ),
        ),
      );
      await tester.pumpAndSettle();
      await tester.tap(
        find.byKey(const Key('local-agent-import-candidate-agent-verified')),
      );
      await tester.pumpAndSettle();
      final confirm = find.byKey(const Key('local-agent-import-confirm'));
      expect(
        tester.widget<FilledButton>(confirm).onPressed,
        isNull,
        reason:
            'W7.2: a plan with an unexpected top-level field must fail closed',
      );
      await tester.tap(confirm, warnIfMissed: false);
      await tester.pump();
      expect(importCalls, 0);
      expect(tester.takeException(), isNull);
    },
  );
}
