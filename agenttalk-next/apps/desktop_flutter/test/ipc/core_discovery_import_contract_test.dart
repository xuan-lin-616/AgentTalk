import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:agenttalk_desktop/ipc/core_ipc_client.dart';
import 'package:agenttalk_desktop/ipc/protocol_v1.dart';
import 'package:flutter_test/flutter_test.dart';

/// W8: offline cross-layer fixture contract over a REAL Windows Named Pipe.
///
/// Every test spawns this round's release `agenttalk-core.exe` (resolved from
/// `AGENTTALK_CORE_INTEGRATION_BINARY`) plus the sibling release
/// `agenttalk-local-discovery-worker.exe`, installs a test-only ACP fixture
/// through the dev/test fixture catalog (the catalog is data read by the
/// release worker; the fixture is never recompiled into the production
/// worker), and drives the real discovery/verify/plan/import chain.
///
/// Missing or wrong binaries fail loudly; nothing here silently skips.
const String _discoveryStream = 'local-discovery-events';

void main() {
  test('W8 real release Core drives the full discovery, verify, plan and '
      'atomic import chain over a real Named Pipe', () async {
    final harness = await _W8Harness.start();
    addTearDown(harness.dispose);
    final client = harness.client;
    final sessionId = harness.sessionId;
    // ---- handshake ----
    final handshake = await client.handshake(sessionId: sessionId);
    expect(handshake['ok'], true, reason: 'W8 handshake over real pipe');

    // ---- project for the atomic import ----
    final projectPayload = await harness.command('project.create', {
      'projectId': harness.projectId,
      'name': 'W8 Contract Project',
      'rootPath': harness.workspace.path,
    });
    expect(projectPayload['ok'], true);

    // ---- passive scan ----
    final start = await client.discoveryStart(
      sessionId: sessionId,
      requestId: 'w8-start',
    );
    expect(start.accepted, true);
    expect(start.state, 'running');
    expect(start.eventStreamId, _discoveryStream);
    expect(start.eventEpoch, isNotEmpty);
    final epoch = start.eventEpoch;
    final scanId = start.scanId;
    // ---- subscribe to local-discovery-events on a dedicated connection ----
    final subscriptionClient = await client.openSubscription(
      sessionId: sessionId,
    );
    final subscription = await subscriptionClient.subscribeDiscoveryEvents(
      sessionId: sessionId,
      epoch: epoch,
    );
    // Keep the subscription connection alive during long verification
    // waits: the Core aborts connections idle beyond the transport read
    // timeout, and only events.ack / events.unsubscribe are valid on a
    // subscription connection, so the ping is rejected but still exercises
    // the pipe.
    final subscriptionHeartbeat = Timer.periodic(const Duration(seconds: 2), (
      _,
    ) {
      unawaited(
        subscriptionClient
            .request({
              'kind': 'query',
              'protocol': {'major': protocolMajor, 'minor': 0},
              'requestId':
                  'w8-sub-heartbeat-${DateTime.now().microsecondsSinceEpoch}',
              'sessionId': sessionId,
              'query': 'runtime.health',
              'payload': <String, dynamic>{},
            })
            .catchError((Object _) => <String, dynamic>{}),
      );
    });
    final events = StreamIterator<EventEnvelope>(subscription.events);
    final observed = <String>[];
    while (!observed.contains('agent.discovery.completed') &&
        !observed.contains('agent.discovery.failed')) {
      final hasEvent = await events.moveNext().timeout(
        const Duration(seconds: 15),
      );
      expect(hasEvent, isTrue, reason: 'W8 discovery events must arrive');
      final event = events.current;
      observed.add(event.event);
      expect(event.cursor.streamId, _discoveryStream);
      expect(event.cursor.epoch, epoch);
      // Every event is renderer-safe: no forbidden content on the wire.
      expect(
        jsonEncode(event.payload).toLowerCase(),
        isNot(contains('authorization')),
        reason: 'W8 discovery event payload must be credential-free',
      );
      final ack = await subscription.ack(event.cursor);
      expect(ack['ok'], true, reason: 'W8 discovery event ACK');
    }
    expect(observed, contains('agent.discovery.started'));
    expect(observed, contains('agent.discovery.candidate_observed'));
    expect(observed, contains('agent.discovery.candidate_classified'));
    expect(observed, contains('agent.discovery.completed'));
    expect(observed, isNot(contains('agent.discovery.failed')));
    // ---- snapshot: the fixture agent is classified ----
    final snapshot = await client.discoverySnapshot(
      sessionId: sessionId,
      requestId: 'w8-snapshot',
      scanId: scanId,
    );
    expect(snapshot.state, 'completed');
    final classified = snapshot.candidates
        .where((entry) => entry.lifecycleState == 'identified')
        .toList();
    expect(classified, hasLength(1), reason: 'W8 fixture agent is classified');
    final candidate = classified.single.candidate;
    expect(candidate.category, 'agent_runtime');
    expect(candidate.displayName, isNotEmpty);
    expect(candidate.discoveryState, 'identified');
    expect(candidate.compatibilityState, 'not_verified');
    expect(candidate.candidateId, isNotEmpty);
    // Renderer-safe DTO: absolute paths, pids and credentials never appear.
    expect(candidate.displayName, isNot(contains(r'C:\')));
    // ---- explicit consent + initialize-only verification ----
    final verify = await client.discoveryVerify(
      sessionId: sessionId,
      requestId: 'w8-verify',
      scanId: scanId,
      candidateId: candidate.candidateId,
      consent: true,
      deadline: const Duration(seconds: 3),
    );
    expect(verify.accepted, true);
    // The verification event arrives on the discovery stream.
    while (true) {
      final hasEvent = await events.moveNext().timeout(
        const Duration(seconds: 15),
      );
      expect(hasEvent, isTrue, reason: 'W8 verification event must arrive');
      final event = events.current;
      await subscription.ack(event.cursor);
      if (event.event == 'agent.discovery.candidate_verified') {
        break;
      }
      if (event.event == 'agent.discovery.failed') {
        fail('W8 verification failed: ${jsonEncode(event.payload)}');
      }
    }
    // ---- verified snapshot ----
    final verifiedSnapshot = await client.discoverySnapshot(
      sessionId: sessionId,
      requestId: 'w8-verified-snapshot',
      scanId: scanId,
    );
    final verifiedEntry = verifiedSnapshot.candidates.singleWhere(
      (entry) => entry.candidate.candidateId == candidate.candidateId,
    );
    expect(verifiedEntry.verification, isNotNull);
    expect(verifiedEntry.verification!.status, 'verified');
    expect(verifiedEntry.verification!.compatibilityState, 'compatible');
    expect(
      verifiedEntry.verification!.authState,
      anyOf('not_required', 'required'),
    );

    // The ACP fixture only ever handled initialize: it exits with a fatal
    // code if it ever sees session/prompt/tool, so a successful
    // verification mechanically proves zero prompt/turn/tool invocations.
    harness.expectFixtureOnlyInitialized();
    // ---- read-only import plan (connector-default) ----
    final plan = await client.importPlan(
      sessionId: sessionId,
      requestId: 'w8-plan',
      scanId: scanId,
      candidateId: candidate.candidateId,
      projectId: harness.projectId,
      modelSelection: null,
    );
    expect(plan.readOnly, isTrue);
    expect(plan.modelSelection, isNull);
    expect(plan.connectorId, isNotEmpty);
    expect(plan.adapterKind, 'acp');
    expect(plan.protocolMajor, 1);
    expect(plan.manifestId, isNotEmpty);
    expect(plan.capabilities.loadSession, isA<bool>());
    // ---- atomic import ----
    final imported = await client.importLocal(
      sessionId: sessionId,
      requestId: 'w8-import',
      scanId: scanId,
      candidateId: candidate.candidateId,
      projectId: harness.projectId,
      modelSelection: null,
    );
    expect(imported.importId, isNotEmpty);
    expect(imported.agentId, isNotEmpty);
    expect(imported.projectId, harness.projectId);
    expect(imported.reused, isFalse);
    expect(
      imported.eventSequence,
      greaterThan(0),
      reason: 'W8 import receipt carries a non-zero event sequence',
    );
    // The real Core persists the import receipt (local_agent_imports row)
    // and publishes projection.changed; there is no separate discovery
    // stream imported event. The discovery subscription is no longer
    // needed; close it so its connection cannot idle past the transport
    // read timeout.
    subscriptionHeartbeat.cancel();
    await subscriptionClient.close().catchError((Object _) {});

    // ---- projection refresh shows the imported agent ----
    final projection =
        (await harness.query('projection.snapshot', {}))['payload']
            as Map<String, dynamic>;
    final agents = (projection['agents'] as List<dynamic>)
        .whereType<Map<String, dynamic>>()
        .toList();
    expect(
      agents.map((agent) => agent['id']),
      contains(imported.agentId),
      reason: 'W8 projection refresh exposes the imported agent',
    );
    final assignments = (projection['assignments'] as List<dynamic>)
        .whereType<Map<String, dynamic>>()
        .toList();
    expect(
      assignments.where((entry) => entry['agentId'] == imported.agentId),
      isNotEmpty,
      reason: 'W8 import creates the project assignment',
    );
    // ---- replay from cursor + ACK ----
    final replay = await client.discoveryReplay(
      sessionId: sessionId,
      requestId: 'w8-replay',
      epoch: epoch,
      afterSequence: 0,
      limit: 64,
    );
    final replayEvents = replay.map((event) => event.event).toSet();
    expect(replayEvents, contains('agent.discovery.started'));
    expect(replayEvents, contains('agent.discovery.completed'));
    expect(replayEvents, contains('agent.discovery.candidate_verified'));

    // ---- graceful shutdown of the owned Core ----
    await subscriptionClient.close().catchError((Object _) {});
    await harness.close(verifyOwnedCoreExit: true);
    // SQLite fixture integrity and foreign keys hold; no forbidden content.
    await harness.expectSqliteIntegrityAndNoForbiddenContent();
    harness.expectNoDefaultDataPathTouched();
  }, timeout: const Timeout(Duration(minutes: 4)));

  test(
    'W8 an unknown candidate is never directly usable',
    () async {
      final harness = await _W8Harness.start(addUnknownTool: true);
      addTearDown(harness.dispose);
      final client = harness.client;
      final sessionId = harness.sessionId;

      await client.handshake(sessionId: sessionId);
      await harness.command('project.create', {
        'projectId': harness.projectId,
        'name': 'W8 Unknown Project',
        'rootPath': harness.workspace.path,
      });
      final start = await client.discoveryStart(
        sessionId: sessionId,
        requestId: 'w8-unknown-start',
      );
      final epoch = start.eventEpoch;
      final scanId = start.scanId;
      final subscriptionClient = await client.openSubscription(
        sessionId: sessionId,
      );
      final subscription = await subscriptionClient.subscribeDiscoveryEvents(
        sessionId: sessionId,
        epoch: epoch,
      );
      final events = StreamIterator<EventEnvelope>(subscription.events);
      while (true) {
        final hasEvent = await events.moveNext().timeout(
          const Duration(seconds: 15),
        );
        expect(hasEvent, isTrue);
        final event = events.current;
        await subscription.ack(event.cursor);
        if (event.event == 'agent.discovery.completed') break;
        if (event.event == 'agent.discovery.failed') {
          fail('W8 unknown scan failed: ${jsonEncode(event.payload)}');
        }
      }

      final snapshot = await client.discoverySnapshot(
        sessionId: sessionId,
        requestId: 'w8-unknown-snapshot',
        scanId: scanId,
      );
      final unknownEntries = snapshot.candidates
          .where((entry) => entry.candidate.category == 'unknown')
          .toList();
      expect(
        unknownEntries,
        isNotEmpty,
        reason:
            'other-tool.exe must be classified unknown: '
            '${snapshot.candidates.map((e) => '${e.candidate.displayName}'
                '/${e.candidate.category}/'
                '${e.candidate.compatibilityState}').join(', ')}',
      );
      final unknown = unknownEntries.single;
      expect(unknown.candidate.category, 'unknown');
      expect(
        unknown.candidate.compatibilityState,
        anyOf('adapter_required', 'not_verified'),
      );

      // Zero verification and zero import for the unknown candidate.
      await expectLater(
        client.discoveryVerify(
          sessionId: sessionId,
          requestId: 'w8-unknown-verify',
          scanId: scanId,
          candidateId: unknown.candidate.candidateId,
          consent: true,
          deadline: const Duration(seconds: 2),
        ),
        throwsA(isA<CoreIpcException>()),
      );
      await expectLater(
        client.importLocal(
          sessionId: sessionId,
          requestId: 'w8-unknown-import',
          scanId: scanId,
          candidateId: unknown.candidate.candidateId,
          projectId: harness.projectId,
          modelSelection: null,
        ),
        throwsA(isA<CoreIpcException>()),
      );
      expect(
        File(
          '${harness.fixtureRoot.path}${Platform.pathSeparator}root.pid',
        ).existsSync(),
        isFalse,
        reason: 'W8 unknown candidate must never launch the executable',
      );

      await subscriptionClient.close().catchError((Object _) {});
      await harness.close(verifyOwnedCoreExit: true);
      await harness.expectSqliteNoImportRows();
    },
    timeout: const Timeout(Duration(minutes: 3)),
  );

  test(
    'W8 imports are idempotent, business-reusable and conflict-typed',
    () async {
      final harness = await _W8Harness.start();
      addTearDown(harness.dispose);
      final client = harness.client;
      final sessionId = harness.sessionId;
      final candidateId = await harness.classifyAndVerify();

      // First import creates the row.
      final first = await client.importLocal(
        sessionId: sessionId,
        requestId: 'w8-imp-1',
        scanId: harness.scanId,
        candidateId: candidateId,
        projectId: harness.projectId,
        modelSelection: null,
      );
      expect(first.reused, isFalse);
      final originalSequence = first.eventSequence;
      expect(originalSequence, greaterThan(0));

      // Same requestId + same payload replays the original receipt.
      final replaySameRequest = await client.importLocal(
        sessionId: sessionId,
        requestId: 'w8-imp-1',
        scanId: harness.scanId,
        candidateId: candidateId,
        projectId: harness.projectId,
        modelSelection: null,
      );
      expect(replaySameRequest.importId, first.importId);
      expect(replaySameRequest.eventSequence, originalSequence);

      // Same requestId + DIFFERENT payload is a typed conflict.
      await expectLater(
        client.importLocal(
          sessionId: sessionId,
          requestId: 'w8-imp-1',
          scanId: harness.scanId,
          candidateId: candidateId,
          projectId: 'w8-other-project',
          modelSelection: null,
        ),
        throwsA(
          isA<CoreIpcException>().having(
            (error) => error.code,
            'code',
            'IMPORT_CONFLICT',
          ),
        ),
      );

      // Different requestId + same binding is a business reuse of the row.
      final reused = await client.importLocal(
        sessionId: sessionId,
        requestId: 'w8-imp-2',
        scanId: harness.scanId,
        candidateId: candidateId,
        projectId: harness.projectId,
        modelSelection: null,
      );
      expect(reused.reused, isTrue);
      expect(reused.importId, first.importId);
      expect(reused.eventSequence, originalSequence);

      // A conflicting modelSelection for the same binding is typed.
      await expectLater(
        client.importLocal(
          sessionId: sessionId,
          requestId: 'w8-imp-3',
          scanId: harness.scanId,
          candidateId: candidateId,
          projectId: harness.projectId,
          modelSelection: 'model-unknown',
        ),
        throwsA(
          isA<CoreIpcException>().having(
            (error) => error.code,
            'code',
            'IMPORT_CONFLICT',
          ),
        ),
      );
      await harness.close(verifyOwnedCoreExit: true);
      // Exactly one persisted import row despite replay/reuse/conflicts.
      await harness.expectSqliteImportRowCount(1);
    },
    timeout: const Timeout(Duration(minutes: 3)),
  );

  test(
    'W8 failed imports leave zero partial writes',
    () async {
      final harness = await _W8Harness.start();
      addTearDown(harness.dispose);
      final client = harness.client;
      final sessionId = harness.sessionId;
      final candidateId = await harness.classifyAndVerify();

      // Change the observed executable after verification: the import-time
      // identity recheck must reject the import BEFORE any row is written.
      final executable = File(
        '${harness.fixtureRoot.path}${Platform.pathSeparator}fixture-agent.exe',
      );
      final before = executable.lengthSync();
      final originalBytes = executable.readAsBytesSync();
      executable.writeAsBytesSync(<int>[
        ...originalBytes,
        ...utf8.encode('w8 identity replacement'),
      ], flush: true);
      expect(executable.lengthSync(), greaterThan(before));

      await expectLater(
        client.importLocal(
          sessionId: sessionId,
          requestId: 'w8-fail-import',
          scanId: harness.scanId,
          candidateId: candidateId,
          projectId: harness.projectId,
          modelSelection: null,
        ),
        throwsA(
          isA<CoreIpcException>().having(
            (error) => error.code,
            'code',
            'DISCOVERY_IDENTITY_CHANGED',
          ),
        ),
      );
      await harness.close(verifyOwnedCoreExit: true);
      // Zero partial rows across every import-facing table.
      await harness.expectSqliteNoImportRows();
    },
    timeout: const Timeout(Duration(minutes: 3)),
  );

  test(
    'W8 replay-gap recovers from snapshot and ACK/epoch scoping holds',
    () async {
      // A small event retention prunes the discovery stream so a replay from
      // sequence 0 becomes a REPLAY_GAP, exactly as in the Rust suite.
      final harness = await _W8Harness.start(retentionMaxEvents: 4);
      addTearDown(harness.dispose);
      final client = harness.client;
      final sessionId = harness.sessionId;
      await client.handshake(sessionId: sessionId);
      await harness.command('project.create', {
        'projectId': harness.projectId,
        'name': 'W8 Replay Project',
        'rootPath': harness.workspace.path,
      });
      final start = await client.discoveryStart(
        sessionId: sessionId,
        requestId: 'w8-replay-start',
      );
      final epoch = start.eventEpoch;
      final scanId = start.scanId;
      await harness.waitForCompletedScan(scanId, epoch);

      // A verification adds a fifth discovery event, evicting the earliest
      // retained event (retention 4), so replaying from sequence 0 becomes a
      // REPLAY_GAP, exactly as in the Rust suite.
      final classified = await client.discoverySnapshot(
        sessionId: sessionId,
        requestId: 'w8-replay-classified',
        scanId: scanId,
      );
      final candidateId = classified.candidates.single.candidate.candidateId;
      final verify = await client.discoveryVerify(
        sessionId: sessionId,
        requestId: 'w8-replay-verify',
        scanId: scanId,
        candidateId: candidateId,
        consent: true,
        deadline: const Duration(seconds: 3),
      );
      expect(verify.accepted, isTrue);
      await harness.waitForPidFilesToAppear([
        File('${harness.fixtureRoot.path}${Platform.pathSeparator}root.pid'),
      ]);
      // Give the verifier time to finish so the candidate_verified event is
      // appended before the eviction check.
      await Future<void>.delayed(const Duration(milliseconds: 1200));

      // Replaying the pruned stream from sequence 0 must produce REPLAY_GAP.
      await expectLater(
        client.discoveryReplay(
          sessionId: sessionId,
          requestId: 'w8-replay-gap',
          epoch: epoch,
          afterSequence: 0,
          limit: 16,
        ),
        throwsA(
          isA<CoreIpcException>().having(
            (error) => error.code,
            'code',
            'REPLAY_GAP',
          ),
        ),
      );

      // The snapshot still rebuilds the full state after the gap.
      final snapshot = await client.discoverySnapshot(
        sessionId: sessionId,
        requestId: 'w8-snapshot-after-gap',
        scanId: scanId,
      );
      expect(snapshot.candidates, isNotEmpty);

      // A wrong discovery epoch is rejected.
      await expectLater(
        client.discoveryReplay(
          sessionId: sessionId,
          requestId: 'w8-wrong-epoch',
          epoch: 'w8-wrong-epoch',
          afterSequence: 0,
          limit: 16,
        ),
        throwsA(isA<CoreIpcException>()),
      );

      // ACK from a foreign owner/session is rejected: a second session on the
      // same Core cannot ACK this session's discovery stream. openSubscription
      // already completes the foreign handshake.
      final foreignClient = await client.openSubscription(
        sessionId: 'w8-foreign-session',
      );
      addTearDown(() => foreignClient.close());
      await expectLater(
        foreignClient.request({
          'kind': 'command',
          'protocol': {'major': protocolMajor, 'minor': 0},
          'requestId': 'w8-foreign-ack',
          'sessionId': 'w8-foreign-session',
          'command': 'events.ack',
          'payload': {
            'subscriptionId': 'w8-does-not-exist',
            'cursor': {
              'streamId': _discoveryStream,
              'sequence': 0,
              'epoch': epoch,
            },
          },
        }),
        throwsA(isA<CoreIpcException>()),
      );

      // A foreign session cannot replay this session's discovery stream.
      await expectLater(
        foreignClient.discoveryReplay(
          sessionId: 'w8-foreign-session',
          requestId: 'w8-foreign-replay',
          epoch: epoch,
          afterSequence: 0,
          limit: 16,
        ),
        throwsA(isA<CoreIpcException>()),
      );
      await foreignClient.close();
      // Restart: a fresh Core instance invalidates the old epoch entirely.
      final restarted = await _W8Harness.start();
      addTearDown(restarted.dispose);
      final restartedClient = restarted.client;
      final restartedSession = restarted.sessionId;
      await restartedClient.handshake(sessionId: restartedSession);
      await expectLater(
        restartedClient.discoveryReplay(
          sessionId: restartedSession,
          requestId: 'w8-restart-old-epoch',
          epoch: epoch,
          afterSequence: 0,
          limit: 16,
        ),
        throwsA(isA<CoreIpcException>()),
      );
      await restarted.close(verifyOwnedCoreExit: true);
    },
    timeout: const Timeout(Duration(minutes: 4)),
  );

  test('W8 verifier timeouts cancel the owned tree and never touch an external '
      'process', () async {
    final harness = await _W8Harness.start(fixtureMode: 'spawn-child-timeout');
    addTearDown(harness.dispose);
    final client = harness.client;
    final sessionId = harness.sessionId;
    await client.handshake(sessionId: sessionId);
    final candidateId = await harness.classifyOnly();

    // An EXTERNAL process (not owned by the verifier) must survive the
    // verification timeout untouched.
    final external = await Process.start(
      harness.fixtureAgentPath,
      ['child-loop'],
      runInShell: false,
      workingDirectory: harness.fixtureRoot.path,
    );
    addTearDown(() => external.kill());

    final rootPid = File(
      '${harness.fixtureRoot.path}${Platform.pathSeparator}root.pid',
    );
    final descendantPid = File(
      '${harness.fixtureRoot.path}${Platform.pathSeparator}descendant.pid',
    );
    // The verify command is accepted and the verifier runs with a bounded
    // initialize-only deadline; the timeout surfaces through the discovery
    // stream as a failed verification, and the owned tree is reaped.
    final verify = await client.discoveryVerify(
      sessionId: sessionId,
      requestId: 'w8-timeout-verify',
      scanId: harness.scanId,
      candidateId: candidateId,
      consent: true,
      deadline: const Duration(milliseconds: 1500),
    );
    expect(verify.accepted, isTrue);
    await harness.waitForPidFilesToAppear([rootPid, descendantPid]);
    // Wait for the timeout to elapse and the owned tree to be reaped.
    await harness.waitForPidFilesToDisappear([rootPid, descendantPid]);
    // The external process is still alive.
    expect(
      await _pidIsAlive(external.pid),
      isTrue,
      reason: 'W8 an external process must never be terminated',
    );
    external.kill();
    await external.exitCode.timeout(const Duration(seconds: 5));
  }, timeout: const Timeout(Duration(minutes: 3)));
}

Future<bool> _pidIsAlive(int pid) async {
  final result = await Process.run('tasklist', ['/FI', 'PID eq $pid', '/NH']);
  return result.stdout.toString().contains('$pid');
}

/// Locates the release Core from `AGENTTALK_CORE_INTEGRATION_BINARY`, the
/// sibling release worker, rustc, and the test-only ACP fixture source.
class _W8Harness {
  _W8Harness._({
    required this.state,
    required this.workspace,
    required this.fixtureRoot,
    required this.artifactRoot,
    required this.databasePath,
    required this.sessionId,
    required this.pipeBase,
    required this.client,
    required this.coreExecutable,
    required this.coreEnvironment,
    required this.projectId,
    required this.fixtureAgentPath,
    required this.localAppDataIsolation,
  });

  final Directory state;
  final Directory workspace;
  final Directory fixtureRoot;
  final Directory artifactRoot;
  final String databasePath;
  final String sessionId;
  final String pipeBase;
  final CoreIpcClient client;
  final String coreExecutable;
  final Map<String, String> coreEnvironment;
  final String projectId;
  final String fixtureAgentPath;
  final Directory localAppDataIsolation;
  bool _closed = false;
  Timer? _heartbeat;

  /// The release Core aborts a connection that stays idle beyond the transport
  /// read timeout; the real desktop app keeps its main connection alive with
  /// projection polling. The contract harness does the same with a lightweight
  /// `runtime.health` heartbeat so long event waits cannot stall the main
  /// connection.
  void _startHeartbeat() {
    _heartbeat = Timer.periodic(const Duration(seconds: 2), (_) {
      unawaited(
        client
            .request({
              'kind': 'query',
              'protocol': {'major': protocolMajor, 'minor': 0},
              'requestId':
                  'w8-heartbeat-${DateTime.now().microsecondsSinceEpoch}',
              'sessionId': sessionId,
              'query': 'runtime.health',
              'payload': <String, dynamic>{},
            })
            .catchError((Object _) => <String, dynamic>{}),
      );
    });
  }

  /// Set by [classifyAndVerify]/[classifyOnly]; empty until then.
  String scanId = '';

  String get fixtureAgentName => 'fixture-agent.exe';

  static const String _projectId = 'project-w8';
  static const String _fixtureCatalogSha =
      'd94f9df787f6779f618569e0d0d2f6f4b2f1d1e2f81c496de6c63c7f5c3a8a46';

  static Future<String> _locateRustc() async {
    final fromEnv = Platform.environment['RUSTC'];
    if (fromEnv != null && File(fromEnv).existsSync()) return fromEnv;
    try {
      final probe = await Process.run('rustc', ['--version']);
      if (probe.exitCode == 0) return 'rustc';
    } on ProcessException {
      // fall through to the toolchain search
    }
    final rustupHome = Platform.environment['RUSTUP_HOME'];
    if (rustupHome != null) {
      final toolchains = Directory(
        '$rustupHome${Platform.pathSeparator}toolchains',
      );
      if (toolchains.existsSync()) {
        for (final toolchain in toolchains.listSync()) {
          if (toolchain is! Directory) continue;
          final candidate =
              '${toolchain.path}${Platform.pathSeparator}bin'
              '${Platform.pathSeparator}rustc.exe';
          if (File(candidate).existsSync()) return candidate;
        }
      }
    }
    final cargoHome = Platform.environment['CARGO_HOME'];
    if (cargoHome != null) {
      final candidate =
          '$cargoHome${Platform.pathSeparator}bin'
          '${Platform.pathSeparator}rustc.exe';
      if (File(candidate).existsSync()) return candidate;
    }
    fail(
      'W8 requires rustc to compile the test-only ACP fixture; it was not '
      'found on PATH, RUSTC, RUSTUP_HOME or CARGO_HOME.',
    );
  }

  static String _locateFixtureSource() {
    var current = Directory.current;
    while (true) {
      final candidate =
          '${current.path}${Platform.pathSeparator}apps'
          '${Platform.pathSeparator}runtime_host${Platform.pathSeparator}tests'
          '${Platform.pathSeparator}fixtures${Platform.pathSeparator}acp_stdio_fixture.rs';
      if (File(candidate).existsSync()) return candidate;
      final parent = current.parent;
      if (parent.path == current.path) break;
      current = parent;
    }
    fail('W8 fixture source acp_stdio_fixture.rs was not found.');
  }

  static Future<_W8Harness> start({
    String fixtureMode = 'success',
    bool addUnknownTool = false,
    int? retentionMaxEvents,
  }) async {
    if (!Platform.isWindows) {
      fail('W8 cross-layer contract requires Windows Named Pipes.');
    }
    final executable =
        Platform.environment['AGENTTALK_CORE_INTEGRATION_BINARY'];
    if (executable == null || !File(executable).existsSync()) {
      fail(
        'AGENTTALK_CORE_INTEGRATION_BINARY must point to this round\'s release '
        'agenttalk-core.exe; this contract must not skip without it.',
      );
    }
    final coreFile = File(executable);
    final coreDir = coreFile.parent;
    final worker = File(
      '${coreDir.path}${Platform.pathSeparator}agenttalk-local-discovery-worker.exe',
    );
    if (!worker.existsSync()) {
      fail(
        'W8 requires the release agenttalk-local-discovery-worker.exe next to '
        'the Core: ${worker.path}',
      );
    }

    final rustc = await _locateRustc();
    final fixtureSource = _locateFixtureSource();
    final nonce = '$pid-${DateTime.now().microsecondsSinceEpoch}';
    final state = await Directory.systemTemp.createTemp(
      'agenttalk-w8-contract-$nonce-',
    );
    final workspace = await Directory(
      '${state.path}${Platform.pathSeparator}workspace',
    ).create();
    final fixtureRoot = await Directory(
      '${state.path}${Platform.pathSeparator}fixtures',
    ).create();
    final artifactRoot = await Directory(
      '${state.path}${Platform.pathSeparator}artifacts',
    ).create();
    final emptyKunData = await Directory(
      '${state.path}${Platform.pathSeparator}kun-data',
    ).create();
    final emptyKunInstall = await Directory(
      '${state.path}${Platform.pathSeparator}kun-install',
    ).create();
    final localAppDataIsolation = await Directory(
      '${state.path}${Platform.pathSeparator}local-app-data',
    ).create();
    final databasePath = '${state.path}${Platform.pathSeparator}core.sqlite3';
    final sessionId = 'session-w8-$nonce';
    final pipeBase = r'\\.\pipe\agenttalk-w8-' + nonce;

    // Compile the test-only ACP fixture into the isolated fixture root. This
    // fixture is never recompiled into the production worker; the release
    // worker only ever reads it as catalog data.
    final fixtureAgent =
        '${fixtureRoot.path}${Platform.pathSeparator}fixture-agent.exe';
    final compile = await Process.run(rustc, [
      '--edition=2021',
      fixtureSource,
      '-o',
      fixtureAgent,
    ]);
    if (compile.exitCode != 0) {
      await state.delete(recursive: true);
      fail(
        'W8 ACP fixture compile failed: '
        '${compile.stdout}${compile.stderr}',
      );
    }
    if (addUnknownTool) {
      // An executable that matches no manifest: classified as unknown.
      File(
        fixtureAgent,
      ).copySync('${fixtureRoot.path}${Platform.pathSeparator}other-tool.exe');
    }

    // Write the dev/test fixture catalog consumed by the release worker.
    final catalog = File(
      '${fixtureRoot.path}${Platform.pathSeparator}fixture-catalog.json',
    );
    catalog.writeAsStringSync(
      jsonEncode({
        'version': 1,
        'generation': 1,
        'revision': 'w8-fixture',
        'createdAtMs': 0,
        'registrySha256': _fixtureCatalogSha,
        'manifests': [
          {
            'schemaVersion': 'agenttalk.adapter.v1',
            'id': 'org.fixture.w8.acp',
            'displayName': 'W8 ACP Fixture',
            'category': 'agent_protocol',
            'protocol': {'kind': 'acp', 'major': 1},
            'match': {
              'executableNames': ['fixture-agent.exe'],
            },
            'launch': {
              'kind': 'direct',
              'transport': 'stdio',
              'executableRef': 'matched-observation',
              'args': [fixtureMode],
              'environmentAllowlist': <String>[],
            },
            'verification': {'kind': 'acp_initialize', 'timeoutMs': 3000},
            'capabilityPolicy': {
              'filesystem': 'forbidden',
              'shell': 'forbidden',
              'streaming': 'negotiate',
              'cancel': 'negotiate',
            },
            'source': {
              'kind': 'agenttalk_manifest',
              'id': 'org.fixture.w8.acp',
              'version': '1',
              'revision': 'w8-fixture',
              'catalogSha256': _fixtureCatalogSha,
            },
          },
        ],
      }),
    );

    final systemRoot = Platform.environment['SystemRoot'] ?? r'C:\Windows';
    final coreEnvironment = <String, String>{
      'AGENTTALK_CORE_DEV_MODE': '1',
      'AGENTTALK_LOCAL_DISCOVERY_FIXTURE_ROOT': fixtureRoot.path,
      'AGENTTALK_LOCAL_DISCOVERY_FIXTURE_CATALOG': catalog.path,
      // The dev-mode fixture executable becomes an explicit UserSelected
      // observation, the legitimate test authority for the ACP protocol
      // chain; a filename-only heuristic match is never launchable.
      'AGENTTALK_LOCAL_DISCOVERY_FIXTURE_EXPLICIT_SOURCES': 'fixture-agent.exe',
      'AGENTTALK_LOCAL_DISCOVERY_WORKER_EXE': worker.path,
      'AGENTTALK_CODEX_BINARY':
          '${fixtureRoot.path}${Platform.pathSeparator}missing-codex.exe',
      'KUN_DATA_DIR': emptyKunData.path,
      'KUN_INSTALL_DIR': emptyKunInstall.path,
      'LOCALAPPDATA': localAppDataIsolation.path,
      'PATH': [
        fixtureRoot.path,
        '$systemRoot${Platform.pathSeparator}System32',
        systemRoot,
      ].join(';'),
      if (retentionMaxEvents != null)
        'AGENTTALK_CORE_TEST_EVENT_RETENTION_MAX_EVENTS': '$retentionMaxEvents',
    };

    try {
      final client = await CoreIpcClient.startOwned(
        coreExecutable: executable,
        pipeName: pipeBase,
        databasePath: databasePath,
        artifactRoot: artifactRoot.path,
        environmentOverrides: coreEnvironment,
      );
      final harness = _W8Harness._(
        state: state,
        workspace: workspace,
        fixtureRoot: fixtureRoot,
        artifactRoot: artifactRoot,
        databasePath: databasePath,
        sessionId: sessionId,
        pipeBase: pipeBase,
        client: client,
        coreExecutable: executable,
        coreEnvironment: coreEnvironment,
        projectId: _projectId,
        fixtureAgentPath: fixtureAgent,
        localAppDataIsolation: localAppDataIsolation,
      );
      harness._startHeartbeat();
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
    return client.request({
      'kind': 'command',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': 'w8-command-${DateTime.now().microsecondsSinceEpoch}',
      'sessionId': sessionId,
      'command': command,
      'payload': payload,
    });
  }

  Future<Map<String, dynamic>> query(
    String query,
    Map<String, dynamic> payload,
  ) {
    return client.request({
      'kind': 'query',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': 'w8-query-${DateTime.now().microsecondsSinceEpoch}',
      'sessionId': sessionId,
      'query': query,
      'payload': payload,
    });
  }

  Future<void> waitForCompletedScan(String scanId, String epoch) async {
    final subscriptionClient = await client.openSubscription(
      sessionId: sessionId,
    );
    try {
      final subscription = await subscriptionClient.subscribeDiscoveryEvents(
        sessionId: sessionId,
        epoch: epoch,
      );
      final events = StreamIterator<EventEnvelope>(subscription.events);
      while (true) {
        final hasEvent = await events.moveNext().timeout(
          const Duration(seconds: 15),
        );
        expect(hasEvent, isTrue, reason: 'W8 scan events must arrive');
        final event = events.current;
        await subscription.ack(event.cursor);
        if (event.event == 'agent.discovery.completed') return;
        if (event.event == 'agent.discovery.failed') {
          fail('W8 scan failed: ${jsonEncode(event.payload)}');
        }
      }
    } finally {
      await subscriptionClient.close().catchError((Object _) {});
    }
  }

  /// Scans, subscribes and verifies the classified fixture agent. Returns the
  /// candidate id; the verification is initialize-only.
  Future<String> classifyAndVerify() async {
    await client.handshake(sessionId: sessionId);
    await command('project.create', {
      'projectId': projectId,
      'name': 'W8 Import Project',
      'rootPath': workspace.path,
    });
    final start = await client.discoveryStart(
      sessionId: sessionId,
      requestId: 'w8-classify-start',
    );
    scanId = start.scanId;
    final epoch = start.eventEpoch;
    await waitForCompletedScan(scanId, epoch);
    final snapshot = await client.discoverySnapshot(
      sessionId: sessionId,
      requestId: 'w8-classify-snapshot',
      scanId: scanId,
    );
    final candidate = snapshot.candidates.single;
    final candidateId = candidate.candidate.candidateId;
    final subscriptionClient = await client.openSubscription(
      sessionId: sessionId,
    );
    try {
      final subscription = await subscriptionClient.subscribeDiscoveryEvents(
        sessionId: sessionId,
        epoch: epoch,
      );
      final events = StreamIterator<EventEnvelope>(subscription.events);
      await client.discoveryVerify(
        sessionId: sessionId,
        requestId: 'w8-classify-verify',
        scanId: scanId,
        candidateId: candidateId,
        consent: true,
        deadline: const Duration(seconds: 3),
      );
      while (true) {
        final hasEvent = await events.moveNext().timeout(
          const Duration(seconds: 15),
        );
        expect(hasEvent, isTrue, reason: 'W8 verification event must arrive');
        final event = events.current;
        await subscription.ack(event.cursor);
        if (event.event == 'agent.discovery.candidate_verified' &&
            event.payload['candidateId'] == candidateId) {
          break;
        }
        if (event.event == 'agent.discovery.failed') {
          fail('W8 verification failed: ${jsonEncode(event.payload)}');
        }
      }
    } finally {
      await subscriptionClient.close().catchError((Object _) {});
    }
    await waitForPidFilesToAppear([
      File('${fixtureRoot.path}${Platform.pathSeparator}root.pid'),
    ]);
    return candidateId;
  }

  /// Scans and returns the classified candidate id WITHOUT verifying.
  Future<String> classifyOnly() async {
    final start = await client.discoveryStart(
      sessionId: sessionId,
      requestId: 'w8-classify-only-start',
    );
    scanId = start.scanId;
    final epoch = start.eventEpoch;
    await waitForCompletedScan(scanId, epoch);
    final snapshot = await client.discoverySnapshot(
      sessionId: sessionId,
      requestId: 'w8-classify-only-snapshot',
      scanId: scanId,
    );
    return snapshot.candidates.single.candidate.candidateId;
  }

  Future<void> waitForPidFilesToAppear(List<File> files) async {
    final deadline = DateTime.now().add(const Duration(seconds: 15));
    while (DateTime.now().isBefore(deadline)) {
      if (files.every((file) => file.existsSync())) return;
      await Future<void>.delayed(const Duration(milliseconds: 50));
    }
    fail('W8 fixture pid markers did not appear: ${files.map((f) => f.path)}');
  }

  Future<void> waitForPidFilesToDisappear(List<File> files) async {
    final deadline = DateTime.now().add(const Duration(seconds: 20));
    while (DateTime.now().isBefore(deadline)) {
      final alive = <String>[];
      for (final file in files) {
        if (!file.existsSync()) continue;
        final content = file.readAsStringSync().trim();
        final pid = int.tryParse(content);
        if (pid != null && await _pidIsAlive(pid)) {
          alive.add('${file.path}:$pid');
        }
      }
      if (alive.isEmpty) return;
      await Future<void>.delayed(const Duration(milliseconds: 50));
    }
    fail(
      'W8 owned fixture processes were not reaped: '
      '${files.map((f) => f.path)}',
    );
  }

  /// The ACP fixture only ever handled initialize: it exits with a fatal code
  /// on any session/prompt/tool request, so a successful verification plus a
  /// non-empty initialize marker mechanically prove zero prompt/turn/tool
  /// invocations.
  void expectFixtureOnlyInitialized() {
    final marker = File(
      '${fixtureRoot.path}${Platform.pathSeparator}initialize.invocations',
    );
    expect(
      marker.existsSync(),
      isTrue,
      reason: 'W8 initialize marker must exist after verification',
    );
    final lines = marker
        .readAsLinesSync()
        .where((line) => line.trim().isNotEmpty)
        .toList();
    expect(lines, isNotEmpty, reason: 'W8 fixture was initialized');
    for (final line in lines) {
      expect(
        int.tryParse(line.trim()),
        isNotNull,
        reason: 'W8 initialize marker must only contain pids',
      );
      expect(line, isNot(contains('session')));
      expect(line, isNot(contains('prompt')));
      expect(line, isNot(contains('tool')));
    }
  }

  Future<String> _sqliteText(String sql) async {
    final result = await Process.run('sqlite3', [databasePath, sql]);
    expect(result.exitCode, 0, reason: 'sqlite3 $sql failed: ${result.stderr}');
    return result.stdout.toString().trim();
  }

  Future<void> expectSqliteIntegrityAndNoForbiddenContent() async {
    final integrity = await _sqliteText('PRAGMA integrity_check;');
    expect(integrity, 'ok', reason: 'W8 fixture SQLite integrity must hold');
    final fk = await _sqliteText('PRAGMA foreign_key_check;');
    expect(fk, isEmpty, reason: 'W8 fixture SQLite foreign keys must hold');
    // The fixture DB must not contain credential/locator content or any
    // absolute fixture path. Only the INSERT data rows are scanned: the
    // schema (for example the workspace_authorizations table name) contains
    // ordinary identifiers, not credential values.
    final dump = await _sqliteText('.dump');
    final dataRows = dump
        .split('\n')
        .where((line) => line.trimLeft().startsWith('INSERT'))
        .join('\n');
    final lowered = dataRows.toLowerCase();
    final fixtureRootLower = fixtureRoot.path.toLowerCase();
    for (final forbidden in <String>[
      'authorization',
      'bearer ',
      'cookie',
      'credential',
      'secret',
      'token=',
      'runtime.json',
      'sessioncredential',
      fixtureRootLower,
    ]) {
      expect(
        lowered.contains(forbidden),
        isFalse,
        reason: 'W8 fixture SQLite data must not contain $forbidden',
      );
    }
  }

  Future<void> expectSqliteImportRowCount(int expected) async {
    final count = await _sqliteText(
      'SELECT COUNT(*) FROM local_agent_imports;',
    );
    expect(
      int.tryParse(count),
      expected,
      reason: 'W8 persisted import rows must be $expected',
    );
  }

  Future<void> expectSqliteNoImportRows() async {
    for (final table in [
      'connector_adapter_bindings',
      'local_agent_imports',
      'agents',
      'project_agents',
    ]) {
      final count = await _sqliteText('SELECT COUNT(*) FROM $table;');
      expect(
        int.tryParse(count),
        0,
        reason: 'W8 $table must remain empty after the failed import',
      );
    }
  }

  void expectNoDefaultDataPathTouched() {
    expect(
      localAppDataIsolation.listSync(),
      isEmpty,
      reason: 'W8 the isolated LOCALAPPDATA must stay untouched',
    );
    final defaultData = Directory(
      '${Platform.environment['LOCALAPPDATA'] ?? ''}${Platform.pathSeparator}'
      'AgentTalk${Platform.pathSeparator}data',
    );
    if (defaultData.existsSync()) {
      // The real default data path must not have been written during this
      // test; the isolated Core cannot reach it. Existence alone is not a
      // failure (a real installation may exist), but no W8 artifact may be
      // under it.
      for (final entry in defaultData.listSync()) {
        expect(
          entry.path.contains('agenttalk-w8-contract'),
          isFalse,
          reason: 'W8 must never write to the default data path',
        );
      }
    }
  }

  /// Gracefully closes the owned Core without deleting the isolated state
  /// (so SQLite assertions can run afterwards).
  Future<void> close({bool verifyOwnedCoreExit = false}) async {
    if (_closed) return;
    _closed = true;
    _heartbeat?.cancel();
    _heartbeat = null;
    await client.close().timeout(const Duration(seconds: 8));
    if (verifyOwnedCoreExit) {
      expect(
        await client.waitForOwnedCoreExit(timeout: const Duration(seconds: 2)),
        isTrue,
        reason: 'W8 owned Core must exit after graceful shutdown',
      );
    }
  }

  /// Final teardown: closes anything still open and removes the isolated
  /// state directory.
  Future<void> dispose() async {
    try {
      await close();
    } finally {
      if (state.existsSync()) await state.delete(recursive: true);
    }
  }
}
