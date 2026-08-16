import 'dart:async';
import 'dart:io';

import 'package:agenttalk_desktop/ipc/core_ipc_client.dart';
import 'package:agenttalk_desktop/ipc/protocol_v1.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  test(
    'Flutter Core IPC drives the frozen local contract over a real Named Pipe',
    () async {
      if (!Platform.isWindows) {
        fail('The real Core contract requires Windows Named Pipes.');
      }
      final executable =
          Platform.environment['AGENTTALK_CORE_INTEGRATION_BINARY'];
      if (executable == null || !File(executable).existsSync()) {
        fail(
          'AGENTTALK_CORE_INTEGRATION_BINARY must point to the release '
          'agenttalk-core.exe for the real Core contract.',
        );
      }

      final state = await Directory.systemTemp.createTemp(
        'agenttalk-core-contract-',
      );
      final workspace = await Directory(
        '${state.path}${Platform.pathSeparator}workspace',
      ).create();
      final database = '${state.path}${Platform.pathSeparator}core.sqlite3';
      final sessionId = 'session-core-contract-${pid.toString()}';
      final pipeName = r'\\.\pipe\agenttalk-core-contract-' + pid.toString();
      CoreIpcClient? client;
      CoreIpcClient? subscriptionClient;
      CoreEventSubscription? subscription;
      StreamIterator<EventEnvelope>? eventIterator;
      var requestNumber = 0;

      Future<Map<String, dynamic>> command(
        String name,
        Map<String, dynamic> payload,
      ) {
        requestNumber += 1;
        return client!.request({
          'kind': 'command',
          'protocol': {'major': protocolMajor, 'minor': 0},
          'requestId': 'contract-command-$requestNumber',
          'sessionId': sessionId,
          'command': name,
          'payload': payload,
        });
      }

      Future<Map<String, dynamic>> query(
        String name,
        Map<String, dynamic> payload,
      ) {
        requestNumber += 1;
        return client!.request({
          'kind': 'query',
          'protocol': {'major': protocolMajor, 'minor': 0},
          'requestId': 'contract-query-$requestNumber',
          'sessionId': sessionId,
          'query': name,
          'payload': payload,
        });
      }

      Future<EventEnvelope> nextEvent() async {
        final iterator = eventIterator;
        final activeSubscription = subscription;
        if (iterator == null || activeSubscription == null) {
          throw StateError('event subscription is not initialized');
        }
        if (!await iterator.moveNext().timeout(const Duration(seconds: 10))) {
          throw StateError('Core event stream ended before the expected event');
        }
        final event = iterator.current;
        final snapshot = await query(
          'projection.snapshot',
          <String, dynamic>{},
        );
        expect(snapshot['ok'], true);
        await activeSubscription.ack(event.cursor);
        expect(activeSubscription.lastAckedCursor, event.cursor);
        return event;
      }

      Future<Set<String>> consumeRunEvents(String runId) async {
        final events = <String>{};
        while (!events.contains('execution.completed') &&
            !events.contains('execution.failed') &&
            !events.contains('execution.cancelled') &&
            !events.contains('execution.interrupted')) {
          final event = await nextEvent();
          if (event.executionRunId == runId) {
            events.add(event.event);
            if (event.event == 'context.sealed') {
              expect(event.payload['metadataOnly'], true);
            }
            if (event.event == 'output.delta') {
              expect(event.payload['delta'], isNotEmpty);
            }
          }
        }
        return events;
      }

      Future<void> consumeProjectionEvent() async {
        while (true) {
          final event = await nextEvent();
          if (event.event == 'projection.changed') return;
        }
      }

      addTearDown(() async {
        await eventIterator?.cancel();
        await subscriptionClient?.close().timeout(const Duration(seconds: 3));
        await client?.close().timeout(const Duration(seconds: 3));
        if (state.existsSync()) await state.delete(recursive: true);
      });

      client = await CoreIpcClient.startOwned(
        coreExecutable: executable,
        pipeName: pipeName,
        databasePath: database,
        environmentOverrides: const {
          'AGENTTALK_CORE_RUNTIME': 'mock',
          'AGENTTALK_CORE_DEV_MODE': '1',
        },
      );
      final handshake = await client.handshake(sessionId: sessionId);
      expect(handshake['ok'], true);

      final health = await query('runtime.health', <String, dynamic>{});
      expect(health['payload']['status'], 'ready');
      final models = await query('runtime.models', <String, dynamic>{});
      expect(models['payload']['models'], contains('mock-default'));
      final initialSnapshot = await query(
        'projection.snapshot',
        <String, dynamic>{},
      );
      expect(initialSnapshot['payload'], isA<Map<String, dynamic>>());

      subscriptionClient = await client.openSubscription(sessionId: sessionId);
      subscription = await subscriptionClient.subscribeEvents(
        sessionId: sessionId,
        afterCursor: StreamCursor(
          streamId: 'core-events',
          sequence: 0,
          epoch: subscriptionClient.serverEpoch,
        ),
      );
      eventIterator = StreamIterator<EventEnvelope>(subscription.events);

      const projectId = 'contract-project';
      const conversationId = 'contract-conversation';
      const primaryAgentId = 'contract-agent-primary';
      const writeAgentId = 'contract-agent-write';
      const foreignAgentId = 'contract-agent-foreign';

      await command('project.create', <String, dynamic>{
        'projectId': projectId,
        'name': 'Core Contract Project',
        'rootPath': workspace.path,
      });
      await consumeProjectionEvent();
      await command('agent.create', <String, dynamic>{
        'agentId': primaryAgentId,
        'name': 'Primary',
        'role': 'builder',
        'specialty': 'integration',
        'systemPrompt': 'local fixture agent',
      });
      await consumeProjectionEvent();
      await command('agent.create', <String, dynamic>{
        'agentId': writeAgentId,
        'name': 'Writer',
        'role': 'builder',
        'specialty': 'integration',
        'systemPrompt': 'local fixture agent',
      });
      await consumeProjectionEvent();
      await command('agent.create', <String, dynamic>{
        'agentId': foreignAgentId,
        'name': 'Foreign',
        'role': 'builder',
        'specialty': 'integration',
        'systemPrompt': 'local fixture agent',
      });
      await consumeProjectionEvent();
      await command('conversation.create', <String, dynamic>{
        'conversationId': conversationId,
        'projectId': projectId,
        'title': 'Core contract conversation',
      });
      await consumeProjectionEvent();
      await command('workspace.authorize', <String, dynamic>{
        'projectId': projectId,
        'rootPath': workspace.path,
      });
      await consumeProjectionEvent();

      await command('project_agent.set', <String, dynamic>{
        'projectId': projectId,
        'agentId': primaryAgentId,
        'enabled': true,
        'workspaceAccess': 'read_only',
        'modelSelectionMode': 'pinned',
        'modelId': 'mock-default',
        'candidateModelListMode': 'inherit',
        'candidateModelListRevision': 0,
      });
      await consumeProjectionEvent();
      await command('project_agent.set', <String, dynamic>{
        'projectId': projectId,
        'agentId': writeAgentId,
        'enabled': true,
        'workspaceAccess': 'workspace_write',
      });
      await consumeProjectionEvent();

      final rosterSnapshot = await query(
        'projection.snapshot',
        <String, dynamic>{},
      );
      final assignments =
          (rosterSnapshot['payload']['assignments'] as List<dynamic>)
              .whereType<Map<String, dynamic>>()
              .toList();
      expect(
        assignments.map((entry) => entry['agentId']),
        containsAll(<String>[primaryAgentId, writeAgentId]),
      );
      expect(
        assignments.map((entry) => entry['agentId']),
        isNot(contains(foreignAgentId)),
      );

      await expectLater(
        command('execution.start', <String, dynamic>{
          'executionRunId': 'contract-foreign-run',
          'collaborationRunId': 'contract-collaboration',
          'projectId': projectId,
          'conversationId': conversationId,
          'agentId': foreignAgentId,
          'workspaceAccess': 'read_only',
          'canonicalCwd': workspace.path,
          'currentTask': 'must be rejected by the project roster',
        }),
        throwsA(isA<CoreIpcException>()),
      );

      final collaboration = await command(
        'collaboration.create',
        <String, dynamic>{
          'projectId': projectId,
          'collaborationRunId': 'contract-collaboration',
          'rootAgentIds': <String>[primaryAgentId, writeAgentId],
          'maxCalls': 2,
          'maxDepth': 1,
          'status': 'pending',
          'autoDispatchHandoffs': false,
        },
      );
      expect(collaboration['ok'], true);
      await consumeProjectionEvent();

      final readOnlyStart = await command('execution.start', <String, dynamic>{
        'executionRunId': 'contract-read-only-run',
        'collaborationRunId': 'contract-collaboration',
        'projectId': projectId,
        'conversationId': conversationId,
        'agentId': primaryAgentId,
        'workspaceAccess': 'read_only',
        'canonicalCwd': workspace.path,
        'currentTask': 'assemble and complete the metadata-only context',
      });
      expect(readOnlyStart['ok'], true);
      expect(
        readOnlyStart['payload']['run']['scope']['workspaceAccess'],
        'ReadOnly',
      );
      final readOnlyEvents = await consumeRunEvents('contract-read-only-run');
      expect(readOnlyEvents, contains('output.delta'));
      expect(readOnlyEvents, contains('execution.completed'));
      expect(readOnlyEvents, contains('context.sealed'));

      final selectionSnapshot = await query(
        'model_selection.snapshot',
        <String, dynamic>{'executionRunId': 'contract-read-only-run'},
      );
      expect(
        selectionSnapshot['payload']['modelSnapshot']['modelId'],
        'mock-default',
      );

      final writeStart = await command('execution.start', <String, dynamic>{
        'executionRunId': 'contract-write-run',
        'collaborationRunId': 'contract-collaboration',
        'projectId': projectId,
        'conversationId': conversationId,
        'agentId': writeAgentId,
        'workspaceAccess': 'workspace_write',
        'canonicalCwd': workspace.path,
        'currentTask': 'complete with workspace write scope',
      });
      expect(
        writeStart['payload']['run']['scope']['workspaceAccess'],
        'WorkspaceWrite',
      );
      final writeEvents = await consumeRunEvents('contract-write-run');
      expect(writeEvents, contains('execution.completed'));

      final retry = await command('execution.retry', <String, dynamic>{
        'executionRunId': 'contract-retry-run',
        'sourceExecutionRunId': 'contract-read-only-run',
        'currentTask': 'retry with the frozen model selection',
      });
      expect(
        retry['payload']['sourceExecutionRunId'],
        'contract-read-only-run',
      );
      final retryEvents = await consumeRunEvents('contract-retry-run');
      expect(retryEvents, contains('execution.completed'));
      final retrySelection = await query(
        'model_selection.snapshot',
        <String, dynamic>{'executionRunId': 'contract-retry-run'},
      );
      expect(
        retrySelection['payload']['modelSnapshot']['modelId'],
        'mock-default',
      );

      final rerun = await command('execution.rerun_current', <String, dynamic>{
        'executionRunId': 'contract-rerun-run',
        'sourceExecutionRunId': 'contract-read-only-run',
        'currentTask': 'rerun against current settings',
      });
      expect(
        rerun['payload']['sourceExecutionRunId'],
        'contract-read-only-run',
      );
      final rerunEvents = await consumeRunEvents('contract-rerun-run');
      expect(rerunEvents, contains('execution.completed'));

      final replay = await client.replayEvents(
        sessionId: sessionId,
        afterSequence: 0,
      );
      expect(replay, isNotEmpty);
      expect(replay.every((event) => event['event'] is String), true);

      for (var index = 0; index < 270; index += 1) {
        await command('project.update', <String, dynamic>{
          'projectId': projectId,
          'name': 'Core Contract Project $index',
          'rootPath': workspace.path,
          'archived': false,
        });
        await consumeProjectionEvent();
      }
      await expectLater(
        client.replayEvents(sessionId: sessionId, afterSequence: 0),
        throwsA(
          isA<CoreIpcException>().having(
            (error) => error.code,
            'code',
            'REPLAY_GAP',
          ),
        ),
      );

      expect(client.ownsCoreProcess, isTrue);
      expect(client.ownedCoreProcessId, isNotNull);
      await subscriptionClient.close().timeout(const Duration(seconds: 3));
      await client.close().timeout(const Duration(seconds: 5));
      expect(
        await client.waitForOwnedCoreExit(timeout: const Duration(seconds: 1)),
        isTrue,
      );
    },
    timeout: const Timeout(Duration(minutes: 2)),
  );
}
