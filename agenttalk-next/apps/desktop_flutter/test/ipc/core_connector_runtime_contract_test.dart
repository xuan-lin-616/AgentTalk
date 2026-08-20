import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:agenttalk_desktop/ipc/core_ipc_client.dart';
import 'package:agenttalk_desktop/ipc/protocol_v1.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  test(
    'Flutter Core IPC returns isolated offline connector.models catalogs',
    () async {
      final harness = await _ReleaseCoreContractHarness.start();
      addTearDown(harness.close);

      await _createConnectorProfile(
        harness,
        connectorId: 'flutter-codex-fixture',
        displayName: 'Flutter offline Codex fixture',
        runtimeType: 'codex',
      );
      await _createConnectorProfile(
        harness,
        connectorId: 'flutter-kun-fixture',
        displayName: 'Flutter offline Kun fixture',
        runtimeType: 'kun',
      );

      final codexCatalog = _payload(
        await harness.query('connector.models', <String, dynamic>{
          'scopeId': 'desktop',
          'connectorId': 'flutter-codex-fixture',
        }),
      );
      final kunCatalog = _payload(
        await harness.query('connector.models', <String, dynamic>{
          'scopeId': 'desktop',
          'connectorId': 'flutter-kun-fixture',
        }),
      );

      _expectCatalog(
        codexCatalog,
        connectorId: 'flutter-codex-fixture',
        runtimeType: 'codex',
        expectedModels: const <String>['codex-model-a', 'codex-model-b'],
        foreignModels: const <String>['kun-model-a', 'kun-model-b'],
      );
      _expectCatalog(
        kunCatalog,
        connectorId: 'flutter-kun-fixture',
        runtimeType: 'kun',
        expectedModels: const <String>['kun-model-a', 'kun-model-b'],
        foreignModels: const <String>['codex-model-a', 'codex-model-b'],
      );

      _expectCredentialFree(codexCatalog);
      _expectCredentialFree(kunCatalog);
      expect(codexCatalog.toString(), isNot(contains('AUTH_REFERENCE')));
      expect(kunCatalog.toString(), isNot(contains('AUTH_REFERENCE')));

      await harness.close(verifyOwnedCoreExit: true);
    },
    timeout: const Timeout(Duration(minutes: 1)),
  );

  test(
    'Flutter Core IPC explicitly cancels an offline fixture run exactly once',
    () async {
      final harness = await _ReleaseCoreContractHarness.start();
      addTearDown(harness.close);

      const projectId = 'flutter-cancel-project';
      const conversationId = 'flutter-cancel-conversation';
      const agentId = 'flutter-cancel-agent';
      const connectorId = 'flutter-cancel-codex-fixture';
      const runId = 'flutter-cancel-run';

      await _createConnectorProfile(
        harness,
        connectorId: connectorId,
        displayName: 'Flutter cancellation fixture',
        runtimeType: 'codex',
      );
      await harness.command('project.create', <String, dynamic>{
        'projectId': projectId,
        'name': 'Flutter cancellation contract project',
        'rootPath': harness.workspace.path,
      });
      await harness.command('conversation.create', <String, dynamic>{
        'conversationId': conversationId,
        'projectId': projectId,
        'title': 'Offline cancellation contract',
      });
      await harness.command('agent.create', <String, dynamic>{
        'agentId': agentId,
        'name': 'Offline cancellation agent',
        'role': 'builder',
        'specialty': 'cross-layer acceptance',
        'systemPrompt': 'offline fixture only',
      });
      await harness.command('project_agent.set', <String, dynamic>{
        'projectId': projectId,
        'agentId': agentId,
        'enabled': true,
        'workspaceAccess': 'none',
      });
      await harness.command('agent.model_binding.patch', <String, dynamic>{
        'agentId': agentId,
        'connectorId': connectorId,
        'modelId': 'codex-model-a',
        'candidateModelListRevision': 1,
      });

      await harness.subscribeAfterCurrentHead();
      final started = _payload(
        await harness.command('execution.start', <String, dynamic>{
          'executionRunId': runId,
          'collaborationRunId': 'flutter-cancel-collaboration',
          'projectId': projectId,
          'conversationId': conversationId,
          'agentId': agentId,
          'workspaceAccess': 'none',
          'currentTask': 'cancel after the offline connector has started',
        }),
      );
      expect(started['run'], isA<Map<String, dynamic>>());
      expect((started['run'] as Map<String, dynamic>)['id'], runId);

      final observedEvents = <EventEnvelope>[];
      while (true) {
        final event = await harness.nextEvent();
        if (event.executionRunId != runId) continue;
        observedEvents.add(event);
        if (event.event == 'connector.started' ||
            event.event == 'runtime.started') {
          expect(event.payload['connectorId'], connectorId);
          expect(event.payload['modelId'], 'codex-model-a');
          expect(event.payload['runtimeType'], 'codex');
          break;
        }
      }

      expect(harness.client.ownsCoreProcess, isTrue);
      // Keep both Named Pipe clients open. This must exercise execution.cancel,
      // not rely on a client disconnect to stop the owned Core worker.
      final firstCancel = _payload(
        await harness.command('execution.cancel', <String, dynamic>{
          'executionRunId': runId,
        }),
      );
      expect(firstCancel['cancelled'], true);
      final firstCancelRequestId = harness.lastRequestId;
      expect(firstCancelRequestId, isNotNull);

      while (true) {
        final event = await harness.nextEvent();
        if (event.executionRunId != runId) continue;
        observedEvents.add(event);
        if (_isTerminalEvent(event.event)) break;
      }

      final terminalEvents = observedEvents
          .where((event) => _isTerminalEvent(event.event))
          .map((event) => event.event)
          .toList(growable: false);
      expect(terminalEvents, const <String>['execution.cancelled']);

      final projection = _payload(
        await harness.query('projection.snapshot', <String, dynamic>{}),
      );
      final runs = _asMapList(projection['runs'], 'projection runs');
      final persistedRun = runs.singleWhere((run) => run['id'] == runId);
      // The execution_runs column stores the serde JSON representation of the
      // enum. Assert both the persisted wire value and its semantic state.
      expect(persistedRun['status'], '"Cancelled"');
      expect(jsonDecode(persistedRun['status'] as String), 'Cancelled');

      // command() emits a fresh requestId every time; the second explicit
      // cancel must be accepted without appending another terminal event.
      final repeatedCancel = _payload(
        await harness.command('execution.cancel', <String, dynamic>{
          'executionRunId': runId,
        }),
      );
      expect(repeatedCancel['cancelled'], true);
      final repeatedCancelRequestId = harness.lastRequestId;
      expect(repeatedCancelRequestId, isNot(firstCancelRequestId));

      final replay = await harness.client.replayEvents(
        sessionId: harness.sessionId,
        afterSequence: 0,
      );
      final replayTerminals = replay
          .where((event) => event['executionRunId'] == runId)
          .map((event) => event['event'])
          .whereType<String>()
          .where(_isTerminalEvent)
          .toList(growable: false);
      expect(replayTerminals, const <String>['execution.cancelled']);

      await harness.close(verifyOwnedCoreExit: true);
    },
    timeout: const Timeout(Duration(minutes: 1)),
  );

  test(
    'Flutter Core IPC keeps local discovery isolated and non-persistent',
    () async {
      final harness = await _ReleaseCoreContractHarness.start(
        isolateLocalDiscovery: true,
      );
      addTearDown(harness.close);

      final before = _payload(
        await harness.query('projection.snapshot', <String, dynamic>{}),
      );
      final connectors = await harness.client.discoverLocalConnectors(
        sessionId: harness.sessionId,
      );
      final agents = await harness.client.scanLocalAgents(
        sessionId: harness.sessionId,
      );
      // The deterministic integration catalog always returns the three
      // first-party rows over the frozen connector.discover DTO. Under
      // isolation every row must be fail-closed  and no Kun
      // runtime record may leak into this projection.
      final expectedIds = <String>{
        'local.codex',
        'local.claude-code',
        'local.antigravity',
      };
      expect(
        connectors.discoveries.map((entry) => entry.connectorId).toSet(),
        expectedIds,
      );
      expect(
        connectors.discoveries.every(
          (entry) => entry.availability == 'unavailable',
        ),
        isTrue,
      );
      expect(
        agents.discoveries.map((entry) => entry.connectorId).toList(),
        connectors.discoveries.map((entry) => entry.connectorId).toList(),
      );
      final after = _payload(
        await harness.query('projection.snapshot', <String, dynamic>{}),
      );
      expect(after, before);

      await harness.close(verifyOwnedCoreExit: true);
    },
    timeout: const Timeout(Duration(minutes: 1)),
  );
}

Future<void> _createConnectorProfile(
  _ReleaseCoreContractHarness harness, {
  required String connectorId,
  required String displayName,
  required String runtimeType,
}) async {
  final response = _payload(
    await harness.command('connector.create', <String, dynamic>{
      'scopeId': 'desktop',
      'connectorId': connectorId,
      'displayName': displayName,
      'providerType': runtimeType,
      'runtimeType': runtimeType,
      'enabled': true,
      // This is an inert reference name, not a secret. connector.models must
      // not expose even the reference from a persisted connector profile.
      'authEnvKey': 'AGENTTALK_OFFLINE_FIXTURE_AUTH_REFERENCE',
    }),
  );
  expect(response['created'], true);
}

void _expectCatalog(
  Map<String, dynamic> catalog, {
  required String connectorId,
  required String runtimeType,
  required List<String> expectedModels,
  required List<String> foreignModels,
}) {
  expect(catalog['schemaVersion'], 'connector.models.v1');
  expect(catalog['scopeId'], 'desktop');
  expect(catalog['connectorId'], connectorId);
  expect(catalog['runtimeType'], runtimeType);
  expect(catalog['availability'], 'available');
  expect(catalog['catalogRevision'], isA<int>());
  expect(catalog['catalogRevision'] as int, greaterThan(0));

  final models = _asStringList(catalog['models'], '$connectorId models');
  expect(models, expectedModels);
  expect(catalog['defaultModelId'], expectedModels.first);
  for (final foreignModel in foreignModels) {
    expect(
      models,
      isNot(contains(foreignModel)),
      reason: '$connectorId catalog leaked $foreignModel from another fixture',
    );
  }

  final metadata = _asMapList(
    catalog['modelMetadata'],
    '$connectorId metadata',
  );
  expect(metadata, hasLength(expectedModels.length));
  for (final modelId in expectedModels) {
    final entry = metadata.singleWhere(
      (metadata) => metadata['modelId'] == modelId,
    );
    expect(entry['availability'], 'available');
    expect(entry['capabilities'], isA<Map<String, dynamic>>());
  }
}

Map<String, dynamic> _payload(Map<String, dynamic> response) {
  expect(response['ok'], true);
  expect(response['payload'], isA<Map<String, dynamic>>());
  return response['payload'] as Map<String, dynamic>;
}

List<String> _asStringList(Object? value, String label) {
  expect(value, isA<List<dynamic>>(), reason: '$label must be a string list');
  return (value as List<dynamic>)
      .map((item) {
        expect(
          item,
          isA<String>(),
          reason: '$label contains a non-string value',
        );
        return item as String;
      })
      .toList(growable: false);
}

List<Map<String, dynamic>> _asMapList(Object? value, String label) {
  expect(value, isA<List<dynamic>>(), reason: '$label must be a list');
  return (value as List<dynamic>)
      .map((item) {
        expect(
          item,
          isA<Map<String, dynamic>>(),
          reason: '$label contains a non-map value',
        );
        return item as Map<String, dynamic>;
      })
      .toList(growable: false);
}

bool _isTerminalEvent(String event) =>
    event == 'execution.completed' ||
    event == 'execution.failed' ||
    event == 'execution.cancelled' ||
    event == 'execution.interrupted';

void _expectCredentialFree(Object? value, [String path = r'$']) {
  const forbiddenMarkers = <String>[
    'token',
    'authorization',
    'cookie',
    'secret',
    'bearer',
    'apikey',
    'authenvkey',
    'credential',
    'password',
  ];
  if (value is Map<String, dynamic>) {
    for (final entry in value.entries) {
      final key = entry.key;
      final normalizedKey = key
          .toLowerCase()
          .replaceAll('_', '')
          .replaceAll('-', '');
      for (final marker in forbiddenMarkers) {
        expect(
          normalizedKey,
          isNot(contains(marker)),
          reason: '$path contains credential-like key $key',
        );
      }
      _expectCredentialFree(entry.value, '$path.$key');
    }
    return;
  }
  if (value is List<dynamic>) {
    for (var index = 0; index < value.length; index += 1) {
      _expectCredentialFree(value[index], '$path[$index]');
    }
    return;
  }
  if (value is String) {
    final normalizedValue = value.toLowerCase();
    for (final marker in forbiddenMarkers) {
      expect(
        normalizedValue,
        isNot(contains(marker)),
        reason: '$path contains credential-like value',
      );
    }
  }
}

class _ReleaseCoreContractHarness {
  _ReleaseCoreContractHarness._({
    required this.state,
    required this.workspace,
    required this.databasePath,
    required this.sessionId,
    required this.client,
  });

  final Directory state;
  final Directory workspace;
  final String databasePath;
  final String sessionId;
  final CoreIpcClient client;
  CoreIpcClient? _subscriptionClient;
  CoreEventSubscription? _subscription;
  StreamIterator<EventEnvelope>? _eventIterator;
  var _requestNumber = 0;
  var _closed = false;

  String? _lastRequestId;

  String? get lastRequestId => _lastRequestId;

  static Future<_ReleaseCoreContractHarness> start({
    bool isolateLocalDiscovery = false,
  }) async {
    if (!Platform.isWindows) {
      fail('The release Core contract requires Windows Named Pipes.');
    }
    final executable =
        Platform.environment['AGENTTALK_CORE_INTEGRATION_BINARY'];
    if (executable == null || !File(executable).existsSync()) {
      fail(
        'AGENTTALK_CORE_INTEGRATION_BINARY must point to the release '
        'agenttalk-core.exe; this contract must not skip without it.',
      );
    }

    final nonce = '$pid-${DateTime.now().microsecondsSinceEpoch}';
    final state = await Directory.systemTemp.createTemp(
      'agenttalk-flutter-connector-contract-$nonce-',
    );
    final workspace = await Directory(
      '${state.path}${Platform.pathSeparator}workspace',
    ).create();
    final artifactRoot = await Directory(
      '${state.path}${Platform.pathSeparator}artifacts',
    ).create();
    final localDiscoveryRoot = await Directory(
      '${state.path}${Platform.pathSeparator}local-discovery',
    ).create();
    final emptyKunData = await Directory(
      '${localDiscoveryRoot.path}${Platform.pathSeparator}kun-data',
    ).create();
    final emptyKunInstall = await Directory(
      '${localDiscoveryRoot.path}${Platform.pathSeparator}kun-install',
    ).create();
    final emptyLocalAppData = await Directory(
      '${localDiscoveryRoot.path}${Platform.pathSeparator}local-app-data',
    ).create();
    final emptyPath = await Directory(
      '${localDiscoveryRoot.path}${Platform.pathSeparator}path',
    ).create();
    final databasePath = '${state.path}${Platform.pathSeparator}core.sqlite3';
    final sessionId = 'flutter-connector-contract-$nonce';
    final pipeName = r'\\.\pipe\agenttalk-flutter-connector-contract-' + nonce;

    try {
      final environmentOverrides = <String, String>{
        // fixture-dual is compiled into the Core only for deterministic
        // development/test acceptance. It does not contact a Provider.
        'AGENTTALK_CORE_RUNTIMES': 'fixture-dual',
        'AGENTTALK_CORE_RUNTIME': 'fixture-dual',
        'AGENTTALK_CORE_DEV_MODE': '1',
        if (isolateLocalDiscovery) ...<String, String>{
          'AGENTTALK_CODEX_BINARY':
              '${localDiscoveryRoot.path}${Platform.pathSeparator}missing-codex.exe',
          'KUN_DATA_DIR': emptyKunData.path,
          'KUN_INSTALL_DIR': emptyKunInstall.path,
          'LOCALAPPDATA': emptyLocalAppData.path,
          'PATH': emptyPath.path,
        },
      };
      final client = await CoreIpcClient.startOwned(
        coreExecutable: executable,
        pipeName: pipeName,
        databasePath: databasePath,
        artifactRoot: artifactRoot.path,
        environmentOverrides: environmentOverrides,
      );
      final harness = _ReleaseCoreContractHarness._(
        state: state,
        workspace: workspace,
        databasePath: databasePath,
        sessionId: sessionId,
        client: client,
      );
      final handshake = await client.handshake(sessionId: sessionId);
      expect(handshake['ok'], true);
      return harness;
    } catch (_) {
      if (state.existsSync()) await state.delete(recursive: true);
      rethrow;
    }
  }

  Future<Map<String, dynamic>> command(
    String command,
    Map<String, dynamic> payload,
  ) {
    _requestNumber += 1;
    final requestId = 'flutter-connector-command-$_requestNumber';
    _lastRequestId = requestId;
    return client.request(<String, dynamic>{
      'kind': 'command',
      'protocol': <String, int>{'major': protocolMajor, 'minor': 0},
      'requestId': requestId,
      'sessionId': sessionId,
      'command': command,
      'payload': payload,
    });
  }

  Future<Map<String, dynamic>> query(
    String query,
    Map<String, dynamic> payload,
  ) {
    _requestNumber += 1;
    final requestId = 'flutter-connector-query-$_requestNumber';
    _lastRequestId = requestId;
    return client.request(<String, dynamic>{
      'kind': 'query',
      'protocol': <String, int>{'major': protocolMajor, 'minor': 0},
      'requestId': requestId,
      'sessionId': sessionId,
      'query': query,
      'payload': payload,
    });
  }

  Future<void> subscribeAfterCurrentHead() async {
    final replay = _payload(
      await query('events.replay', <String, dynamic>{
        'afterSequence': 0,
        'limit': 1,
      }),
    );
    expect(replay['headSequence'], isA<int>());
    final subscriptionClient = await client.openSubscription(
      sessionId: sessionId,
    );
    final subscription = await subscriptionClient.subscribeEvents(
      sessionId: sessionId,
      afterCursor: StreamCursor(
        streamId: 'core-events',
        sequence: replay['headSequence'] as int,
        epoch: subscriptionClient.serverEpoch,
      ),
    );
    _subscriptionClient = subscriptionClient;
    _subscription = subscription;
    _eventIterator = StreamIterator<EventEnvelope>(subscription.events);
  }

  Future<EventEnvelope> nextEvent() async {
    final iterator = _eventIterator;
    final subscription = _subscription;
    if (iterator == null || subscription == null) {
      throw StateError('event subscription is not initialized');
    }
    final hasEvent = await iterator.moveNext().timeout(
      const Duration(seconds: 10),
    );
    if (!hasEvent) {
      throw StateError('Core event stream ended before the expected event');
    }
    final event = iterator.current;
    final acknowledgement = await subscription.ack(event.cursor);
    expect(acknowledgement['ok'], true);
    expect(subscription.lastAckedCursor, event.cursor);
    return event;
  }

  Future<void> close({bool verifyOwnedCoreExit = false}) async {
    if (_closed) return;
    _closed = true;
    try {
      await _eventIterator?.cancel();
      await _subscriptionClient?.close().timeout(const Duration(seconds: 3));
      await client.close().timeout(const Duration(seconds: 5));
      if (verifyOwnedCoreExit) {
        expect(
          await client.waitForOwnedCoreExit(
            timeout: const Duration(seconds: 1),
          ),
          isTrue,
        );
      }
    } finally {
      if (state.existsSync()) await state.delete(recursive: true);
    }
  }
}
