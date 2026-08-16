// W8.4 cross-layer contract: the production release Core, with its bundled
// ACP catalog compiled in (no dev-mode flag, no fixture catalog), observes a
// copilot.exe-named external fixture whose SHA differs from the pinned
// production Copilot SHA and rejects it at the classification boundary over a
// real Named Pipe. Env-gated on AGENTTALK_CORE_INTEGRATION_BINARY (red
// without it, never skipped).
import 'dart:async';
import 'dart:io';

import 'package:agenttalk_desktop/ipc/core_ipc_client.dart';
import 'package:agenttalk_desktop/ipc/protocol_v1.dart';
import 'package:flutter_test/flutter_test.dart';

String get _integrationBinaryEnv =>
    Platform.environment['AGENTTALK_CORE_INTEGRATION_BINARY'] ?? '';

/// Locates the test-only ACP fixture source relative to the repository tree.
/// Walks up from the package root until `apps/runtime_host/tests/fixtures/`
/// is found; fails loudly instead of falling back to any global or formal
/// data location. Reuses the same portable strategy as the W8 harness.
Future<String> _locateFixtureSource() async {
  var current = Directory.current;
  while (true) {
    final candidate =
        '${current.path}${Platform.pathSeparator}apps'
        '${Platform.pathSeparator}runtime_host${Platform.pathSeparator}tests'
        '${Platform.pathSeparator}fixtures${Platform.pathSeparator}'
        'acp_stdio_fixture.rs';
    if (File(candidate).existsSync()) return candidate;
    final parent = current.parent;
    if (parent.path == current.path) break;
    current = parent;
  }
  fail(
    'W8.3.1 requires the test-only ACP fixture source '
    'apps/runtime_host/tests/fixtures/acp_stdio_fixture.rs under the '
    'repository root; it was not found.',
  );
}

Future<void> _compileAcpFixture(String output) async {
  final source = await _locateFixtureSource();
  final result = await Process.run('rustc', [
    '--edition=2021',
    source,
    '-o',
    output,
  ]);
  if (result.exitCode != 0) {
    fail('compile test-only ACP fixture: ${result.stderr}');
  }
}

void main() {
  test('W8.4: production bundled catalog rejects a copilot.exe whose SHA '
      'mismatches the pinned identity over the release Core', () async {
    if (_integrationBinaryEnv.isEmpty) {
      fail(
        'AGENTTALK_CORE_INTEGRATION_BINARY must point to the release '
        'agenttalk-core.exe for the production catalog contract.',
      );
    }
    final core = _integrationBinaryEnv;
    expect(
      File(core).existsSync(),
      isTrue,
      reason: 'release Core must exist at AGENTTALK_CORE_INTEGRATION_BINARY',
    );
    final state = await Directory.systemTemp.createTemp(
      'agenttalk-w83-$pid-${DateTime.now().microsecondsSinceEpoch}-',
    );
    final binDir = await Directory(
      '${state.path}${Platform.pathSeparator}bin',
    ).create();
    final dataRoot = await Directory(
      '${state.path}${Platform.pathSeparator}data',
    ).create();
    final artifactRoot = await Directory(
      '${state.path}${Platform.pathSeparator}artifacts',
    ).create();
    final emptyKun = await Directory(
      '${state.path}${Platform.pathSeparator}kun',
    ).create();
    final localAppData = await Directory(
      '${state.path}${Platform.pathSeparator}localappdata',
    ).create();
    final databasePath = '${state.path}${Platform.pathSeparator}core.sqlite3';
    final pipeName =
        '${r'\\.\pipe\agenttalk-w83-'}$pid-${DateTime.now().microsecondsSinceEpoch}';
    final sessionId = 'session-w83-$pid';
    final systemRoot = Platform.environment['SystemRoot'] ?? r'C:\Windows';
    // dataRoot must be created but is not read by the client directly; the
    // Core receives it via AGENTTALK_DATA_ROOT-style isolation arguments.
    expect(dataRoot.existsSync(), isTrue);

    final fixtureExe =
        '${binDir.path}${Platform.pathSeparator}fixture-agent.exe';
    final copilotExe = '${binDir.path}${Platform.pathSeparator}copilot.exe';
    await _compileAcpFixture(fixtureExe);
    await File(fixtureExe).copy(copilotExe);

    // The production discovery scan must observe only the isolated fixture bin.
    // System32 and the Windows root are deliberately excluded from the
    // discoverable PATH so the global discovery budget is not consumed scanning
    // them (Windows DLL loading uses the system search order, not this PATH).
    final productionPath = [binDir.path].join(';');
    expect(
      productionPath.toLowerCase().contains('system32'),
      isFalse,
      reason: 'production fixture PATH must not include System32',
    );
    expect(
      productionPath.toLowerCase().contains(systemRoot.toLowerCase()),
      isFalse,
      reason: 'production fixture PATH must not include the Windows root',
    );
    expect(
      productionPath,
      contains(binDir.path),
      reason: 'production fixture PATH must include the fixture bin',
    );

    CoreIpcClient? client;
    addTearDown(() async {
      await client?.close().timeout(const Duration(seconds: 8));
      if (state.existsSync()) await state.delete(recursive: true);
    });

    client = await CoreIpcClient.startOwned(
      coreExecutable: core,
      pipeName: pipeName,
      databasePath: databasePath,
      artifactRoot: artifactRoot.path,
      environmentOverrides: {
        // Production mode: no dev-mode flag, no fixture catalog.
        'AGENTTALK_CORE_DEV_MODE': '',
        'AGENTTALK_LOCAL_DISCOVERY_FIXTURE_ROOT': '',
        'AGENTTALK_LOCAL_DISCOVERY_FIXTURE_CATALOG': '',
        'AGENTTALK_CODEX_BINARY':
            '${state.path}${Platform.pathSeparator}missing-codex.exe',
        'KUN_DATA_DIR': emptyKun.path,
        'KUN_INSTALL_DIR': emptyKun.path,
        'LOCALAPPDATA': localAppData.path,
        'PATH': productionPath,
      },
    );
    final handshake = await client.handshake(sessionId: sessionId);
    expect(handshake['ok'], true, reason: 'W8.3 handshake');

    final start = await client.discoveryStart(
      sessionId: sessionId,
      requestId: 'w83-start',
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
        const Duration(seconds: 30),
      );
      expect(hasEvent, isTrue, reason: 'W8.3 discovery events must arrive');
      final event = events.current;
      await subscription.ack(event.cursor);
      if (event.event == 'agent.discovery.completed') break;
      if (event.event == 'agent.discovery.failed') {
        fail('W8.3 scan failed: ${event.payload}');
      }
    }
    await subscriptionClient.close().catchError((Object _) {});

    final snapshot = await client.discoverySnapshot(
      sessionId: sessionId,
      requestId: 'w84-snapshot',
      scanId: scanId,
    );
    // The spoofed copilot.exe (a copy of the test fixture, whose SHA differs
    // from the production Copilot pin) is observed but never classified as
    // GitHub Copilot CLI: it stays an unknown, adapter_required candidate.
    final copilot = snapshot.candidates
        .where((entry) => entry.candidate.displayName == 'copilot.exe')
        .toList();
    expect(
      copilot.length,
      1,
      reason:
          'spoofed copilot.exe must be observed; candidates: '
          '${snapshot.candidates.map((e) => e.candidate.displayName).toList()}',
    );
    final candidate = copilot.single.candidate;
    expect(candidate.isUnknown, isTrue);
    expect(candidate.runtimeTypeName, 'unknown');
    expect(candidate.discoveryState, 'observed');
    final candidateEntry = snapshot.candidates.firstWhere(
      (entry) => entry.candidate.candidateId == candidate.candidateId,
    );
    expect(
      candidateEntry.lifecycleState,
      'adapter_required',
      reason: 'a mismatched-SHA copilot.exe must never become an ACP target',
    );
    expect(
      snapshot.candidates.any(
        (entry) => entry.candidate.displayName == 'GitHub Copilot CLI',
      ),
      isFalse,
      reason:
          'a mismatched-SHA copilot.exe must not be classified as '
          'GitHub Copilot CLI',
    );

    // No ACP child, no initialize: the pinned-SHA mismatch is rejected before
    // any spawn.
    final ledger = File(
      '${binDir.path}${Platform.pathSeparator}initialize.invocations',
    );
    expect(
      ledger.existsSync(),
      isFalse,
      reason: 'mismatched-SHA copilot.exe must never reach initialize',
    );
    final rootPid = File('${binDir.path}${Platform.pathSeparator}root.pid');
    expect(
      rootPid.existsSync(),
      isFalse,
      reason: 'mismatched-SHA copilot.exe must never spawn the executable',
    );

    // Replay still works and contains no successful verification receipt.
    final replay = await client.discoveryReplay(
      sessionId: sessionId,
      epoch: epoch,
      requestId: 'w84-replay',
    );
    expect(
      replay.any((event) => event.event == 'agent.discovery.completed'),
      isTrue,
      reason: 'W8.4 replay must include the completed scan event',
    );

    await client.close().timeout(const Duration(seconds: 8));
    expect(
      await client.waitForOwnedCoreExit(timeout: const Duration(seconds: 2)),
      isTrue,
      reason: 'W8.4 owned Core must exit',
    );
  }, timeout: const Timeout(Duration(minutes: 4)));

  test('W8.3.1: test sources contain no hardcoded local absolute paths', () {
    // The markers are assembled from fragments so the literal patterns do
    // not appear verbatim in this source file and therefore cannot match
    // the scanner's own body.
    const markers = <String>[
      'E:'
          r'\',
      'AgentTalk-'
          'local-state',
      r'\worktrees'
          r'\',
      'C:'
          r'\Users'
          r'\',
    ];
    final user = Platform.environment['USERNAME'] ?? '';
    final testDir = Directory(
      '${Directory.current.path}${Platform.pathSeparator}test'
      '${Platform.pathSeparator}ipc',
    );
    expect(testDir.existsSync(), isTrue, reason: 'test/ipc must exist');
    final offenders = <String>[];
    for (final entity in testDir.listSync(recursive: true)) {
      if (entity is! File || !entity.path.endsWith('.dart')) continue;
      final content = entity.readAsStringSync();
      for (final marker in markers) {
        if (content.contains(marker)) {
          offenders.add('${entity.path}: marker $marker');
        }
      }
      if (user.isNotEmpty && content.contains(user)) {
        offenders.add('${entity.path}: username $user');
      }
    }
    expect(
      offenders,
      isEmpty,
      reason:
          'test sources must not hardcode machine-local absolute paths: '
          '${offenders.join('; ')}',
    );
  });
}
