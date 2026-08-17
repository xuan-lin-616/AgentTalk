import 'dart:async';
import 'dart:typed_data';

import 'package:agenttalk_desktop/ipc/core_ipc_client.dart';
import 'package:agenttalk_desktop/ipc/protocol_v1.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  test('request rejects a response with a different requestId', () async {
    final pipe = _FakePipe(responseRequestId: 'unexpected-request');
    final client = _clientFor(pipe);
    addTearDown(client.close);

    await expectLater(
      client.request(_query('expected-request')),
      throwsA(
        isA<CoreIpcException>().having(
          (error) => error.message,
          'message',
          contains('requestId mismatch'),
        ),
      ),
    );
  });

  test('request rejects a response with a different protocol major', () async {
    final pipe = _FakePipe(responseProtocolMajor: protocolMajor + 1);
    final client = _clientFor(pipe);
    addTearDown(client.close);

    await expectLater(
      client.request(_query('protocol-request')),
      throwsA(
        isA<CoreIpcException>().having(
          (error) => error.message,
          'message',
          contains('unsupported protocol major'),
        ),
      ),
    );
  });

  test('requests serialize complete write/read exchanges', () async {
    final pipe = _FakePipe();
    final client = _clientFor(pipe);
    addTearDown(client.close);

    final first = client.request(_query('request-1'));
    final second = client.request(_query('request-2'));
    final responses = await Future.wait([first, second]);

    expect(responses.map((response) => response['requestId']), [
      'request-1',
      'request-2',
    ]);
    expect(pipe.writtenRequestIds, ['request-1', 'request-2']);
    expect(pipe.readRequestIds, ['request-1', 'request-2']);
  });

  test('close cancels a pending exchange and is idempotent', () async {
    final pipe = _FakePipe(respond: false);
    final client = _clientFor(pipe);

    final response = client.request(_query('request-before-close'));
    final queuedResponse = client.request(
      _query('request-queued-before-close'),
    );
    final stopwatch = Stopwatch()..start();
    final firstClose = client.close();
    final secondClose = client.close();

    expect(identical(firstClose, secondClose), isTrue);
    await expectLater(
      response,
      throwsA(
        isA<CoreIpcException>().having(
          (error) => error.code,
          'code',
          'CLIENT_CLOSED',
        ),
      ),
    );
    await expectLater(
      queuedResponse,
      throwsA(
        isA<CoreIpcException>().having(
          (error) => error.code,
          'code',
          'CLIENT_CLOSED',
        ),
      ),
    );
    await firstClose;
    stopwatch.stop();
    expect(stopwatch.elapsed, lessThan(const Duration(seconds: 2)));
    expect(pipe.closeCount, 1);

    await client.close();
    expect(pipe.closeCount, 1);
    await expectLater(
      client.request(_query('request-after-close')),
      throwsA(isA<CoreIpcException>()),
    );
  });

  test(
    'retryExecution maps the new Run and preserves source identity',
    () async {
      final pipe = _FakePipe(
        responsePayload: const {
          'run': {'id': 'retry-run-1', 'status': 'Completed'},
          'sourceExecutionRunId': 'source-run-1',
        },
      );
      final client = _clientFor(pipe);
      addTearDown(client.close);

      final result = await client.retryExecution(
        sessionId: 'session-test-123456',
        executionRunId: 'retry-run-1',
        sourceExecutionRunId: 'source-run-1',
        currentTask: 'retry task',
        deadlineMs: 2500,
      );

      expect(result.run['id'], 'retry-run-1');
      expect(result.sourceExecutionRunId, 'source-run-1');
      expect(pipe.writtenCommands, ['execution.retry']);
      expect(pipe.writtenDeadlineMs, [2500]);
      expect(pipe.writtenPayloads.single, {
        'executionRunId': 'retry-run-1',
        'sourceExecutionRunId': 'source-run-1',
        'currentTask': 'retry task',
      });
    },
  );

  test(
    'rerunCurrentExecution uses a separate current-settings command',
    () async {
      final pipe = _FakePipe(
        responsePayload: const {
          'run': {'id': 'rerun-current-1', 'status': 'Completed'},
          'sourceExecutionRunId': 'source-run-1',
        },
      );
      final client = _clientFor(pipe);
      addTearDown(client.close);

      final result = await client.rerunCurrentExecution(
        sessionId: 'session-test-123456',
        executionRunId: 'rerun-current-1',
        sourceExecutionRunId: 'source-run-1',
        currentTask: 'resolve current settings',
        connectorId: 'mock',
        modelId: 'current-model',
      );

      expect(result.run['id'], 'rerun-current-1');
      expect(pipe.writtenCommands, ['execution.rerun_current']);
      expect(pipe.writtenPayloads.single, {
        'executionRunId': 'rerun-current-1',
        'sourceExecutionRunId': 'source-run-1',
        'currentTask': 'resolve current settings',
        'connectorId': 'mock',
        'modelId': 'current-model',
      });
    },
  );

  test('searchMessages maps the Core messages response payload', () async {
    final pipe = _FakePipe(
      responsePayload: const {
        'messages': [
          {
            'id': 'message-1',
            'conversationId': 'conversation-1',
            'content': 'search hit',
          },
        ],
      },
    );
    final client = _clientFor(pipe);
    addTearDown(client.close);

    final results = await client.searchMessages(
      sessionId: 'session-test-123456',
      query: 'search',
      conversationId: 'conversation-1',
    );

    expect(results.single['content'], 'search hit');
    expect(pipe.writtenRequestIds, hasLength(1));
  });

  test(
    'generateSummary sends a scoped command and returns metadata only',
    () async {
      final pipe = _FakePipe(
        responsePayload: const {
          'summary': {
            'id': 'summary-1',
            'version': 1,
            'artifactId': 'artifact-1',
          },
          'generator': 'local-deterministic-v1',
          'messageCount': 2,
          'projection': {'summaries': []},
        },
      );
      final client = _clientFor(pipe);
      addTearDown(client.close);

      final result = await client.generateSummary(
        sessionId: 'session-test-123456',
        scopeId: 'conversation-1',
      );

      expect(result['generator'], 'local-deterministic-v1');
      expect(pipe.writtenCommands, ['summary.generate']);
      expect(pipe.writtenPayloads.single, {'scopeId': 'conversation-1'});
      expect(pipe.writtenPayloads.single.containsKey('content'), isFalse);
    },
  );

  test('querySummaryContent is an explicit bounded body query', () async {
    final pipe = _FakePipe(
      responsePayload: const {
        'summaryId': 'summary-1',
        'content': 'Summary body',
      },
    );
    final client = _clientFor(pipe);
    addTearDown(client.close);

    final result = await client.querySummaryContent(
      sessionId: 'session-test-123456',
      summaryId: 'summary-1',
    );

    expect(result['content'], 'Summary body');
    expect(pipe.writtenQueries, ['summary.content']);
    expect(pipe.writtenPayloads.single, {'summaryId': 'summary-1'});
  });

  test(
    'queryArtifactContent decodes one bounded range with EOF metadata',
    () async {
      final pipe = _FakePipe(
        responsePayload: const {
          'artifactId': 'artifact-1',
          'sha256':
              'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
          'offset': 3,
          'size': 6,
          'chunkBase64': 'BAUG',
          'chunkBytes': 3,
          'eof': true,
        },
      );
      final client = _clientFor(pipe);
      addTearDown(client.close);

      final chunk = await client.queryArtifactContent(
        sessionId: 'session-test-123456',
        artifactId: 'artifact-1',
        offset: 3,
        limit: 3,
      );

      expect(chunk, isA<ArtifactContentChunk>());
      expect(chunk.artifactId, 'artifact-1');
      expect(chunk.offset, 3);
      expect(chunk.size, 6);
      expect(chunk.bytes, <int>[4, 5, 6]);
      expect(chunk.eof, isTrue);
      expect(pipe.writtenQueries, ['artifact.content']);
      expect(pipe.writtenPayloads.single, {
        'artifactId': 'artifact-1',
        'offset': 3,
        'limit': 3,
      });
    },
  );

  test('queryArtifactContent rejects a malformed or oversized chunk', () async {
    final pipe = _FakePipe(
      responsePayload: const {
        'artifactId': 'artifact-1',
        'sha256': 'short',
        'offset': 0,
        'size': 1,
        'chunkBase64': 'AQI=',
        'chunkBytes': 2,
        'eof': true,
      },
    );
    final client = _clientFor(pipe);
    addTearDown(client.close);

    await expectLater(
      client.queryArtifactContent(
        sessionId: 'session-test-123456',
        artifactId: 'artifact-1',
        offset: 0,
        limit: 1,
      ),
      throwsA(isA<CoreIpcException>()),
    );
  });

  test(
    'storeArtifact sends bounded bytes and keeps response body-free',
    () async {
      final pipe = _FakePipe(
        responsePayload: const {
          'created': true,
          'alreadyPresent': false,
          'bodyStored': true,
          'projection': {
            'artifacts': [
              {'id': 'artifact-1', 'size': 3},
            ],
          },
        },
      );
      final client = _clientFor(pipe);
      addTearDown(client.close);

      final response = await client.storeArtifact(
        sessionId: 'session-test-123456',
        artifactId: 'artifact-1',
        sha256: 'a' * 64,
        size: 3,
        mime: 'text/plain',
        relativePath: 'notes/example.txt',
        body: Uint8List.fromList(<int>[1, 2, 3]),
      );

      expect(response['bodyStored'], true);
      expect(pipe.writtenCommands, ['artifact.store']);
      expect(pipe.writtenPayloads.single['bodyBase64'], 'AQID');
      expect(pipe.writtenPayloads.single['size'], 3);
      expect(pipe.writtenPayloads.single.containsKey('content'), isFalse);
    },
  );

  test(
    'importAttachmentFile sends one-time path and validates body-free metadata',
    () async {
      final pipe = _FakePipe(
        responsePayload: const {
          'created': true,
          'alreadyPresent': false,
          'artifactCreated': true,
          'artifactAlreadyPresent': false,
          'bodyStored': true,
          'artifact': {
            'id': 'artifact-file-1',
            'sha256':
                'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
            'size': 614400,
            'mime': 'application/octet-stream',
            'relativePath': null,
          },
          'attachment': {
            'attachmentId': 'attachment-file-1',
            'artifactId': 'artifact-file-1',
            'messageId': 'message-1',
            'fileName': 'selected.bin',
            'sha256':
                'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
            'size': 614400,
            'ordinal': 1,
          },
          'projection': {'attachments': <Object>[]},
        },
      );
      final client = _clientFor(pipe);
      addTearDown(client.close);

      final response = await client.importAttachmentFile(
        sessionId: 'session-test-123456',
        attachmentId: 'attachment-file-1',
        artifactId: 'artifact-file-1',
        messageId: 'message-1',
        sourcePath: r'C:\Selected\selected.bin',
        mime: 'application/octet-stream',
        ordinal: 1,
      );

      expect(response['bodyStored'], true);
      expect(pipe.writtenCommands, ['attachment.import_file']);
      expect(
        pipe.writtenPayloads.single['sourcePath'],
        r'C:\Selected\selected.bin',
      );
      expect(response.containsKey('sourcePath'), isFalse);
      expect(
        response['attachment'],
        isNot(containsPair('sourcePath', anything)),
      );
    },
  );

  test('importAttachmentFile rejects a leaked source path', () async {
    final pipe = _FakePipe(
      responsePayload: const {
        'created': true,
        'alreadyPresent': false,
        'artifactCreated': true,
        'artifactAlreadyPresent': false,
        'bodyStored': true,
        'sourcePath': r'C:\Private\selected.bin',
        'artifact': <String, Object?>{},
        'attachment': <String, Object?>{},
        'projection': <String, Object?>{},
      },
    );
    final client = _clientFor(pipe);
    addTearDown(client.close);

    await expectLater(
      client.importAttachmentFile(
        sessionId: 'session-test-123456',
        attachmentId: 'attachment-file-1',
        artifactId: 'artifact-file-1',
        messageId: 'message-1',
        sourcePath: r'C:\Private\selected.bin',
        mime: 'application/octet-stream',
        ordinal: 0,
      ),
      throwsA(isA<CoreIpcException>()),
    );
  });

  test('storeAttachment sends typed association metadata', () async {
    final pipe = _FakePipe(
      responsePayload: const {
        'created': true,
        'alreadyPresent': false,
        'projection': {
          'attachments': [
            {
              'attachmentId': 'attachment-1',
              'artifactId': 'artifact-1',
              'messageId': 'message-1',
              'ordinal': 0,
            },
          ],
        },
      },
    );
    final client = _clientFor(pipe);
    addTearDown(client.close);

    final response = await client.storeAttachment(
      sessionId: 'session-test-123456',
      attachmentId: 'attachment-1',
      artifactId: 'artifact-1',
      messageId: 'message-1',
      ordinal: 0,
      fileName: 'example.txt',
      sha256: 'a' * 64,
      size: 3,
    );

    expect(response['created'], true);
    expect(pipe.writtenCommands, ['attachment.store']);
    expect(pipe.writtenPayloads.single['attachmentId'], 'attachment-1');
    expect(pipe.writtenPayloads.single['messageId'], 'message-1');
    expect(pipe.writtenPayloads.single['ordinal'], 0);
    expect(pipe.writtenPayloads.single.containsKey('bodyBase64'), isFalse);
  });

  test('queryRuntimeModels maps the versioned Core catalog payload', () async {
    final pipe = _FakePipe(
      responsePayload: const {
        'schemaVersion': 'runtime.models.v1',
        'runtimeId': 'mock-runtime',
        'models': ['runtime-model-1'],
        'modelMetadata': [
          {'modelId': 'runtime-model-1', 'availability': 'available'},
        ],
      },
    );
    final client = _clientFor(pipe);
    addTearDown(client.close);

    final catalog = await client.queryRuntimeModels(
      sessionId: 'session-test-123456',
    );

    expect(catalog['schemaVersion'], 'runtime.models.v1');
    expect(catalog['modelMetadata'], isA<List<dynamic>>());
    expect(pipe.writtenRequestIds, hasLength(1));
  });

  test(
    'orchestration snapshot and recovery queries stay metadata-only',
    () async {
      final pipe = _FakePipe(
        responsePayload: const {
          'run': {'runId': 'run-1', 'status': 'pending'},
          'nodes': <dynamic>[],
          'attempts': <dynamic>[],
          'machineAcceptances': <dynamic>[],
          'artifactBindings': <dynamic>[],
        },
      );
      final client = _clientFor(pipe);
      addTearDown(client.close);

      final snapshot = await client.queryOrchestrationRunSnapshot(
        sessionId: 'session-test-123456',
        runId: 'run-1',
      );
      expect(snapshot['run'], isA<Map<String, dynamic>>());
      expect(pipe.writtenQueries, ['orchestration.run.snapshot']);
      expect(pipe.writtenPayloads.single, {'runId': 'run-1'});

      final recoveryPipe = _FakePipe(
        responsePayload: const {
          'runId': 'run-1',
          'coordinatorGeneration': 2,
          'nodes': <dynamic>[],
        },
      );
      final recoveryClient = _clientFor(recoveryPipe);
      addTearDown(recoveryClient.close);
      final recovery = await recoveryClient.queryOrchestrationRecoveryState(
        sessionId: 'session-test-123456',
        runId: 'run-1',
      );
      expect(recovery['coordinatorGeneration'], 2);
      expect(recoveryPipe.writtenQueries, ['orchestration.run.recovery_state']);
    },
  );

  test(
    'createOrchestrationRun sends sealed facts and validates projection',
    () async {
      final pipe = _FakePipe(
        responsePayload: const {
          'created': true,
          'run': {
            'runId': 'run-1',
            'briefSnapshotId':
                'sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
          },
          'projection': {
            'run': {'runId': 'run-1'},
          },
        },
      );
      final client = _clientFor(pipe);
      addTearDown(client.close);

      final result = await client.createOrchestrationRun(
        sessionId: 'session-test-123456',
        projectId: 'project-1',
        runId: 'run-1',
        briefSnapshotId:
            'sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
        briefTreeDigest:
            'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
        dagSnapshotDigest:
            'cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc',
        roleBindingSnapshotDigest:
            'dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd',
      );

      expect(result['created'], true);
      expect(pipe.writtenCommands, ['orchestration.run.create']);
      expect(pipe.writtenPayloads.single, {
        'projectId': 'project-1',
        'runId': 'run-1',
        'briefSnapshotId':
            'sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
        'briefTreeDigest':
            'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
        'dagSnapshotDigest':
            'cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc',
        'roleBindingSnapshotDigest':
            'dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd',
      });
    },
  );

  test(
    'createOrchestrationRun rejects non-canonical digest inputs locally',
    () async {
      final pipe = _FakePipe();
      final client = _clientFor(pipe);
      addTearDown(client.close);

      await expectLater(
        client.createOrchestrationRun(
          sessionId: 'session-test-123456',
          projectId: 'project-1',
          runId: 'run-1',
          briefSnapshotId: 'sha256:${'A' * 64}',
          briefTreeDigest: 'b' * 64,
          dagSnapshotDigest: 'c' * 64,
          roleBindingSnapshotDigest: 'd' * 64,
        ),
        throwsA(isA<CoreIpcException>()),
      );
      expect(pipe.writtenCommands, isEmpty);
    },
  );

  test(
    'orchestration task lifecycle sends typed control-plane commands',
    () async {
      final pipe = _FakePipe(
        responsePayload: const {
          'created': true,
          'nodeId': 'node-1',
          'status': 'pending',
        },
      );
      final client = _clientFor(pipe);
      addTearDown(client.close);

      final inserted = await client.insertOrchestrationTaskNode(
        sessionId: 'session-test-123456',
        runId: 'run-1',
        nodeId: 'node-1',
        nodeKey: 'architect',
      );
      expect(inserted['status'], 'pending');

      final readyPipe = _FakePipe(
        responsePayload: const {
          'changed': true,
          'nodeId': 'node-1',
          'status': 'ready',
        },
      );
      final readyClient = _clientFor(readyPipe);
      addTearDown(readyClient.close);
      final ready = await readyClient.markOrchestrationTaskReady(
        sessionId: 'session-test-123456',
        nodeId: 'node-1',
        inputArtifactSetDigest:
            'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
        roleId: 'architect',
        acceptanceContractRef:
            'sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
      );
      expect(ready['status'], 'ready');

      final startPipe = _FakePipe(
        responsePayload: const {
          'started': true,
          'outcome': {
            'nodeId': 'node-1',
            'attemptId': 'node-1:attempt:1',
            'attemptNo': 1,
            'leaseEpoch': 1,
          },
        },
      );
      final startClient = _clientFor(startPipe);
      addTearDown(startClient.close);
      final started = await startClient.startOrchestrationTask(
        sessionId: 'session-test-123456',
        nodeId: 'node-1',
        fromExecutionRunId: 'execution-1',
        leaseOwner: 'core-instance-1',
      );
      expect(started['outcome'], isA<Map<String, dynamic>>());
      expect(startPipe.writtenCommands, ['orchestration.task.start']);
    },
  );

  test(
    'orchestration delivery and acceptance clients keep authority payloads nested',
    () async {
      final deliveryPipe = _FakePipe(
        responsePayload: const {
          'deliveryId': 'handoff-1',
          'replayed': false,
          'journaled': true,
        },
      );
      final deliveryClient = _clientFor(deliveryPipe);
      addTearDown(deliveryClient.close);
      final delivery = await deliveryClient.recordOrchestrationDelivery(
        sessionId: 'session-test-123456',
        delivery: const {'deliveryId': 'handoff-1'},
        bindings: const [
          {'bindingId': 'binding-1'},
        ],
      );
      expect(delivery['journaled'], true);
      expect(deliveryPipe.writtenCommands, ['orchestration.delivery.record']);
      expect(deliveryPipe.writtenPayloads.single, {
        'delivery': {'deliveryId': 'handoff-1'},
        'bindings': [
          {'bindingId': 'binding-1'},
        ],
      });

      final acceptancePipe = _FakePipe(
        responsePayload: const {
          'acceptanceId': 'acceptance-1',
          'verdict': 'accepted',
          'replayed': false,
          'recorded': true,
        },
      );
      final acceptanceClient = _clientFor(acceptancePipe);
      addTearDown(acceptanceClient.close);
      final acceptance = await acceptanceClient.recordOrchestrationAcceptance(
        sessionId: 'session-test-123456',
        acceptance: const {
          'acceptanceId': 'acceptance-1',
          'verdict': 'accepted',
        },
      );
      expect(acceptance['verdict'], 'accepted');
      expect(acceptancePipe.writtenCommands, [
        'orchestration.acceptance.record',
      ]);
    },
  );

  test(
    'orchestration milestone and receipt clients validate sealed facts',
    () async {
      final milestonePipe = _FakePipe(
        responsePayload: const {
          'runId': 'run-1',
          'milestoneId': 'milestone-1',
          'status': 'awaiting_approval',
        },
      );
      final milestoneClient = _clientFor(milestonePipe);
      addTearDown(milestoneClient.close);
      final milestone = await milestoneClient.ensureOrchestrationMilestone(
        sessionId: 'session-test-123456',
        runId: 'run-1',
        milestoneId: 'milestone-1',
        milestoneKey: 'review',
        briefTreeDigest: 'a' * 64,
        presentedArtifactSetDigest: 'b' * 64,
        acceptanceEvidenceDigest: 'c' * 64,
      );
      expect(milestone['status'], 'awaiting_approval');
      expect(milestonePipe.writtenCommands, ['orchestration.milestone.ensure']);

      final receiptPipe = _FakePipe(
        responsePayload: const {
          'receiptId': 'receipt-1',
          'decision': 'approve',
          'replayed': false,
          'recorded': true,
        },
      );
      final receiptClient = _clientFor(receiptPipe);
      addTearDown(receiptClient.close);
      final receipt = await receiptClient.recordOrchestrationHumanReceipt(
        sessionId: 'session-test-123456',
        receiptId: 'receipt-1',
        runId: 'run-1',
        milestoneId: 'milestone-1',
        requestId: 'human-request-1',
        semanticPayloadHash: 'd' * 64,
        decision: 'approve',
        expectedVersion: 1,
        briefTreeDigest: 'a' * 64,
        presentedArtifactSetDigest: 'b' * 64,
        acceptanceEvidenceDigest: 'c' * 64,
      );
      expect(receipt['decision'], 'approve');
      expect(receiptPipe.writtenCommands, ['orchestration.receipt.record']);
    },
  );

  test(
    'orchestration graph binding keeps sealed authority facts grouped',
    () async {
      final pipe = _FakePipe(
        responsePayload: const {
          'runId': 'run-1',
          'edges': 1,
          'edgePorts': 1,
          'roleBindings': 1,
          'contextAuthorities': 1,
        },
      );
      final client = _clientFor(pipe);
      addTearDown(client.close);
      final result = await client.bindOrchestrationGraphFacts(
        sessionId: 'session-test-123456',
        runId: 'run-1',
        edges: const [
          {'edgeId': 'edge-1'},
        ],
        edgePorts: const [
          {'edgePortId': 'edge-port-1'},
        ],
        roleBindings: const [
          {'roleId': 'architect'},
        ],
        contextAuthorities: const [
          {'contextManifestRefId': 'context-1'},
        ],
      );
      expect(result['edges'], 1);
      expect(pipe.writtenCommands, ['orchestration.graph.bind']);
    },
  );

  test(
    'local discovery queries use empty payloads and reject extra fields',
    () async {
      const responsePayload = {
        'discoveries': [
          {
            'connectorId': 'local.codex',
            'runtimeType': 'codex',
            'displayName': 'Codex (local executable)',
            'availability': 'unconfigured',
            'models': <String>[],
            'catalogRevision': null,
            'source': 'kind=codex-executable;binary=C:\\fixture\\codex.exe',
            'requiresConfiguration': true,
          },
          {
            'connectorId': 'local.kun.shared-runtime',
            'runtimeType': 'kun',
            'displayName': 'Kun Shared Runtime',
            'availability': 'available',
            'models': ['kun-model-a'],
            'catalogRevision': '42',
            'source': 'kind=kun-shared-runtime;port=32123',
            'requiresConfiguration': false,
          },
        ],
      };
      final pipe = _FakePipe(responsePayload: responsePayload);
      final client = _clientFor(pipe);
      addTearDown(client.close);

      final connectors = await client.discoverLocalConnectors(
        sessionId: 'session-test-123456',
      );
      final agents = await client.scanLocalAgents(
        sessionId: 'session-test-123456',
      );

      expect(connectors.discoveries.map((entry) => entry.connectorId), [
        'local.codex',
        'local.kun.shared-runtime',
      ]);
      expect(
        agents.discoveries
            .singleWhere((entry) => entry.connectorRuntimeType == 'kun')
            .models,
        ['kun-model-a'],
      );
      expect(pipe.writtenQueries, ['connector.discover', 'agent.scan_local']);
      expect(pipe.writtenPayloads, [<String, dynamic>{}, <String, dynamic>{}]);

      final credentialBearing = <String, dynamic>{
        ...responsePayload,
        'token': 'must-not-cross-ipc',
      };
      expect(
        () => LocalConnectorDiscoveryResult.fromResponse({
          'payload': credentialBearing,
        }, 'connector.discover'),
        throwsA(isA<CoreIpcException>()),
      );
    },
  );

  test(
    'model selection client uses typed scoped commands and queries',
    () async {
      const target = IdentityModelTarget(
        identityScope: 'conversation_agent',
        agentId: 'agent-1',
        conversationId: 'conversation-1',
      );
      final mutationPipe = _FakePipe(
        responsePayload: const {
          'changed': true,
          'projection': <String, dynamic>{},
        },
      );
      final mutationClient = _clientFor(mutationPipe);
      addTearDown(mutationClient.close);
      final option = IdentityModelOptionMetadata(
        id: 'option-1',
        target: target,
        modelId: 'model-1',
        displayName: 'Model 1',
        connectorId: 'mock',
        source: 'manual',
        availability: 'unverified',
        isDefault: true,
        sortOrder: 0,
      );

      await mutationClient.upsertIdentityModelOption(
        sessionId: 'session-test-123456',
        option: option,
      );
      expect(mutationPipe.writtenCommands, ['identity_model_option.upsert']);
      expect(
        mutationPipe.writtenPayloads.single['identityScope'],
        'conversation_agent',
      );
      expect(mutationPipe.writtenPayloads.single['modelId'], 'model-1');
      expect(
        mutationPipe.writtenPayloads.single.containsKey('apiKey'),
        isFalse,
      );

      final queryPipe = _FakePipe(
        responsePayload: const {
          'target': <String, dynamic>{},
          'connectorId': 'mock',
          'options': [
            {
              'id': 'option-1',
              'scope': 'conversation_agent',
              'agentId': 'agent-1',
              'projectId': null,
              'conversationId': 'conversation-1',
              'modelId': 'model-1',
              'displayName': 'Model 1',
              'connectorId': 'mock',
              'source': 'manual',
              'availability': 'unverified',
              'isDefault': true,
              'sortOrder': 0,
              'catalogRevision': null,
              'contextWindow': null,
              'reasoningEfforts': <String>[],
              'serviceTiers': <String>[],
            },
          ],
        },
      );
      final queryClient = _clientFor(queryPipe);
      addTearDown(queryClient.close);
      final options = await queryClient.queryIdentityModelOptions(
        sessionId: 'session-test-123456',
        target: target,
        connectorId: 'mock',
      );
      expect(options.single.modelId, 'model-1');
      expect(options.single.target.conversationId, 'conversation-1');
      expect(queryPipe.writtenQueries, ['identity_model_options.list']);

      final snapshotPipe = _FakePipe(
        responsePayload: const {
          'executionRunId': 'run-1',
          'modelSnapshot': {'runId': 'run-1', 'modelId': 'model-1'},
          'selectionSnapshot': {
            'runId': 'run-1',
            'version': 2,
            'effectiveModelId': 'model-1',
          },
        },
      );
      final snapshotClient = _clientFor(snapshotPipe);
      addTearDown(snapshotClient.close);
      final snapshot = await snapshotClient.queryModelSelectionSnapshot(
        sessionId: 'session-test-123456',
        executionRunId: 'run-1',
      );
      expect(snapshot.selectionSnapshot?['effectiveModelId'], 'model-1');
      expect(snapshotPipe.writtenQueries, ['model_selection.snapshot']);
    },
  );

  test(
    'model selection client rejects invalid pinned/list combinations',
    () async {
      final pipe = _FakePipe();
      final client = _clientFor(pipe);
      addTearDown(client.close);

      await expectLater(
        client.setProjectAgentModelSelection(
          sessionId: 'session-test-123456',
          projectId: 'project-1',
          agentId: 'agent-1',
          enabled: true,
          workspaceAccess: 'none',
          modelSelectionMode: 'inherit',
          modelId: 'forbidden-model',
          candidateModelListMode: 'inherit',
          candidateModelListRevision: 0,
        ),
        throwsA(isA<CoreIpcException>()),
      );
      expect(pipe.writtenCommands, isEmpty);
    },
  );

  test('queryConnectorHealth maps the typed credential-free payload', () async {
    final pipe = _FakePipe(
      responsePayload: const {
        'schemaVersion': 'connector.health.v1',
        'scopeId': 'desktop',
        'connector': {
          'connectorId': 'connector-1',
          'displayName': 'Local Mock',
          'providerType': 'mock',
          'runtimeType': 'mock',
          'enabled': true,
          'status': 'ready',
          'availability': 'available',
          'ok': true,
          'verified': false,
          'verification': 'local_adapter_only',
          'runtimeId': 'mock',
          'runtimeVersion': 'mock-1',
          'runtimeOwned': true,
          'capabilities': {
            'streaming': true,
            'cancel': true,
            'filesystem': false,
            'shell': false,
          },
          'authReferencePresent': true,
          'healthDetailPresent': true,
          'healthDetailRedacted': true,
        },
      },
    );
    final client = _clientFor(pipe);
    addTearDown(client.close);

    final result = await client.queryConnectorHealth(
      sessionId: 'session-test-123456',
      scopeId: 'desktop',
      connectorId: 'connector-1',
    );

    expect(result.schemaVersion, 'connector.health.v1');
    expect(result.scopeId, 'desktop');
    expect(result.connector.connectorId, 'connector-1');
    expect(result.connector.runtimeVersion, 'mock-1');
    expect(result.connector.capabilities.streaming, isTrue);
    expect(result.connector.capabilities.shell, isFalse);
    expect(result.connector.authReferencePresent, isTrue);
    expect(result.connector.healthDetailRedacted, isTrue);
    expect(pipe.writtenCommands, isEmpty);
    expect(pipe.writtenPayloads.single, {
      'scopeId': 'desktop',
      'connectorId': 'connector-1',
    });
    expect(pipe.writtenPayloads.single.containsKey('token'), isFalse);
    expect(pipe.writtenPayloads.single.containsKey('credential'), isFalse);
  });

  test(
    'queryConnectorHealth rejects malformed or credential-bearing payloads',
    () async {
      final malformedPayloads = <Map<String, dynamic>>[
        {
          'schemaVersion': 'wrong.version',
          'scopeId': 'desktop',
          'connector': <String, dynamic>{},
        },
        {
          'schemaVersion': 'connector.health.v1',
          'scopeId': 'desktop',
          'connector': {
            'connectorId': 'connector-1',
            'displayName': 'Local Mock',
            'providerType': 'mock',
            'runtimeType': 'mock',
            'enabled': true,
            'status': 'ready',
            'availability': 'available',
            'ok': true,
            'verified': false,
            'verification': 'local_adapter_only',
            'runtimeId': 'mock',
            'runtimeVersion': null,
            'runtimeOwned': true,
            'capabilities': {
              'streaming': true,
              'cancel': true,
              'filesystem': false,
              'shell': false,
            },
            'authReferencePresent': false,
            'healthDetailPresent': false,
            'healthDetailRedacted': true,
            'credential': 'must-not-cross-ipc',
          },
        },
      ];

      for (final responsePayload in malformedPayloads) {
        final pipe = _FakePipe(responsePayload: responsePayload);
        final client = _clientFor(pipe);
        addTearDown(client.close);

        await expectLater(
          client.queryConnectorHealth(
            sessionId: 'session-test-123456',
            scopeId: 'desktop',
            connectorId: 'connector-1',
          ),
          throwsA(
            isA<CoreIpcException>().having(
              (error) => error.message,
              'message',
              contains('connector.health'),
            ),
          ),
        );
      }
    },
  );

  test('config transfer methods map safe export and import payloads', () async {
    final exportPipe = _FakePipe(
      responsePayload: const {
        'config': {
          'schemaVersion': 'config.transfer.v1',
          'project': {'id': 'project-1', 'rootPath': null},
          'agents': [],
        },
      },
    );
    final exportClient = _clientFor(exportPipe);
    addTearDown(exportClient.close);
    final config = await exportClient.exportProjectConfig(
      sessionId: 'session-test-123456',
      projectId: 'project-1',
    );
    expect(config['schemaVersion'], 'config.transfer.v1');
    expect(exportPipe.writtenCommands, ['config.export']);

    final importPipe = _FakePipe(
      responsePayload: const {
        'success': true,
        'newProjectId': 'imported-project-1',
        'importedAgents': 2,
        'importedConversations': 1,
        'importedWorkflows': 1,
        'workspaceRebindRequired': true,
        'projection': {'projects': []},
      },
    );
    final importClient = _clientFor(importPipe);
    addTearDown(importClient.close);
    final result = await importClient.importProjectConfig(
      sessionId: 'session-test-123456',
      config: config,
    );
    expect(result.newProjectId, 'imported-project-1');
    expect(result.workspaceRebindRequired, isTrue);
    expect(importPipe.writtenCommands, ['config.import']);
    expect(importPipe.writtenPayloads.single.keys, ['config']);
  });

  test('Connector profile methods keep CRUD payloads metadata-only', () async {
    const profile = ConnectorProfileMetadata(
      scopeId: 'desktop',
      connectorId: 'connector-1',
      displayName: 'Local Mock',
      providerType: 'mock',
      runtimeTypeName: 'mock',
      enabled: true,
      authEnvKey: 'AGENTTALK_TEST_KEY',
    );

    final queryPipe = _FakePipe(
      responsePayload: const {
        'scopeId': 'desktop',
        'connectorProfiles': [
          {
            'scopeId': 'desktop',
            'connectorId': 'connector-1',
            'displayName': 'Local Mock',
            'providerType': 'mock',
            'runtimeType': 'mock',
            'enabled': true,
            'authEnvKey': 'AGENTTALK_TEST_KEY',
          },
        ],
      },
    );
    final queryClient = _clientFor(queryPipe);
    addTearDown(queryClient.close);
    final profiles = await queryClient.queryConnectorProfiles(
      sessionId: 'session-test-123456',
    );
    expect(profiles.single.connectorId, 'connector-1');
    expect(queryPipe.readKinds, ['response']);

    final createPipe = _FakePipe(
      responsePayload: const {
        'created': true,
        'alreadyPresent': false,
        'connectorProfile': {
          'scopeId': 'desktop',
          'connectorId': 'connector-1',
          'displayName': 'Local Mock',
          'providerType': 'mock',
          'runtimeType': 'mock',
          'enabled': true,
          'authEnvKey': 'AGENTTALK_TEST_KEY',
        },
        'projection': {'connectorProfiles': []},
      },
    );
    final createClient = _clientFor(createPipe);
    addTearDown(createClient.close);
    final created = await createClient.createConnectorProfile(
      sessionId: 'session-test-123456',
      profile: profile,
    );
    expect(created.changed, isTrue);
    expect(createPipe.writtenCommands, ['connector.create']);
    expect(createPipe.writtenPayloads.single.keys, [
      'scopeId',
      'connectorId',
      'displayName',
      'providerType',
      'runtimeType',
      'enabled',
      'authEnvKey',
    ]);
    expect(createPipe.writtenPayloads.single.containsKey('token'), isFalse);
    expect(createPipe.writtenPayloads.single.containsKey('endpoint'), isFalse);

    final removePipe = _FakePipe(
      responsePayload: const {
        'removed': true,
        'alreadyAbsent': false,
        'scopeId': 'desktop',
        'connectorId': 'connector-1',
        'projection': {'connectorProfiles': []},
      },
    );
    final removeClient = _clientFor(removePipe);
    addTearDown(removeClient.close);
    final removed = await removeClient.removeConnectorProfile(
      sessionId: 'session-test-123456',
      connectorId: 'connector-1',
    );
    expect(removed.removed, isTrue);
    expect(removePipe.writtenCommands, ['connector.remove']);
  });

  test('queryRetrievalSources maps scoped metadata response', () async {
    final pipe = _FakePipe(
      responsePayload: const {
        'retrievalSources': [
          {
            'id': 'retrieval-1',
            'scopeId': 'conversation-1',
            'citation': 'docs/README.md#intro',
            'sha256':
                'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
            'tokenCount': 8,
          },
        ],
      },
    );
    final client = _clientFor(pipe);
    addTearDown(client.close);

    final results = await client.queryRetrievalSources(
      sessionId: 'session-test-123456',
      scopeId: 'conversation-1',
      sourceIds: const ['retrieval-1'],
    );

    expect(results.single['id'], 'retrieval-1');
    expect(pipe.writtenRequestIds, hasLength(1));
  });

  test('queryRetrievalPreview sends an explicit scoped typed query', () async {
    final pipe = _FakePipe(
      responsePayload: const {
        'retrievalVersion': 'retrieval.preview.v1',
        'queryHash': 'hash-1',
        'capabilities': {'openSource': false, 'promptInsert': false},
        'hits': [
          {
            'hitId': 'hit-1',
            'sourceType': 'document',
            'sourceObjectId': 'doc-1',
            'snippet': 'bounded result',
            'matchReason': 'title_and_body',
            'score': 0.91,
            'estimatedTokens': 12,
            'permissionDecision': 'allowed',
          },
        ],
      },
    );
    final client = _clientFor(pipe);
    addTearDown(client.close);

    final result = await client.queryRetrievalPreview(
      sessionId: 'session-test-123456',
      project: 'project-1',
      conversation: 'conversation-1',
      agent: null,
      query: 'bounded query',
      scope: 'conversation',
      sourceTypes: const ['document', 'memory'],
      limit: 8,
    );

    expect(result.retrievalVersion, 'retrieval.preview.v1');
    expect(result.hits.single.permissionDecision, 'allowed');
    expect(pipe.writtenPayloads.single, {
      'project': 'project-1',
      'conversation': 'conversation-1',
      'agent': null,
      'query': 'bounded query',
      'scope': 'conversation',
      'sourceTypes': ['document', 'memory'],
      'limit': 8,
      'mode': 'exact',
    });
  });

  test(
    'queryRetrievalPreview can request the explicit local vector fixture',
    () async {
      final pipe = _FakePipe(
        responsePayload: const {
          'retrievalVersion': 'local-vector-fixture-v1',
          'queryHash': 'hash-vector',
          'capabilities': {
            'semantic': true,
            'semanticUnavailableReason':
                'local_deterministic_fixture_not_live_provider',
            'embeddingProvider': 'local_fixture',
            'embeddingDimension': 32,
          },
          'hits': [
            {
              'hitId': 'message:message-1',
              'sourceType': 'message',
              'sourceObjectId': 'message-1',
              'snippet': 'local vector result',
              'matchReason': 'local_vector_similarity',
              'matchMethod': 'local_vector_fixture',
              'score': 0.84,
              'estimatedTokens': 4,
              'permissionDecision': 'not_applicable',
            },
          ],
        },
      );
      final client = _clientFor(pipe);
      addTearDown(client.close);

      final result = await client.queryRetrievalPreview(
        sessionId: 'session-test-123456',
        project: 'project-1',
        conversation: 'conversation-1',
        agent: 'agent-1',
        query: 'semantic fixture',
        scope: 'conversation',
        sourceTypes: const ['message'],
        limit: 4,
        mode: 'vector_fixture',
      );

      expect(result.retrievalVersion, 'local-vector-fixture-v1');
      expect(result.capabilities['embeddingProvider'], 'local_fixture');
      expect(pipe.writtenPayloads.single['mode'], 'vector_fixture');
    },
  );

  test(
    'queryRetrievalPreview rejects an unscoped request before IPC',
    () async {
      final pipe = _FakePipe();
      final client = _clientFor(pipe);
      addTearDown(client.close);

      await expectLater(
        client.queryRetrievalPreview(
          sessionId: 'session-test-123456',
          project: null,
          conversation: null,
          agent: null,
          query: 'do not search globally',
          scope: 'project',
          sourceTypes: const ['document'],
          limit: 8,
        ),
        throwsA(isA<CoreIpcException>()),
      );
      expect(pipe.writtenPayloads, isEmpty);
    },
  );

  test(
    'retrieval selection and feedback methods preserve scoped contracts',
    () async {
      final selectionPipe = _FakePipe(
        responsePayload: const {
          'created': true,
          'alreadyPresent': false,
          'projection': {'retrievalSelections': []},
        },
      );
      final selectionClient = _clientFor(selectionPipe);
      addTearDown(selectionClient.close);
      final selection = await selectionClient.storeRetrievalSelection(
        sessionId: 'session-test-123456',
        selectionId: 'selection-1',
        scope: 'conversation',
        scopeId: 'conversation-1',
        projectId: 'project-1',
        conversationId: 'conversation-1',
        retrievalVersion: 'v1',
        queryHash:
            'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
        items: const [
          {
            'sourceId': 'retrieval-1',
            'sourceHash':
                'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
            'rank': 1,
            'scoreMilli': 900,
            'matchMethod': 'explicit_selection',
            'reason': 'explicit_user_choice',
          },
        ],
      );
      expect(selection['created'], isTrue);
      expect(selectionPipe.writtenCommands, contains('retrieval.select'));

      final feedbackPipe = _FakePipe(
        responsePayload: const {
          'created': true,
          'alreadyPresent': false,
          'projection': {'retrievalFeedback': []},
        },
      );
      final feedbackClient = _clientFor(feedbackPipe);
      addTearDown(feedbackClient.close);
      final feedback = await feedbackClient.storeRetrievalFeedback(
        sessionId: 'session-test-123456',
        feedbackId: 'feedback-1',
        selectionId: 'selection-1',
        scopeId: 'conversation-1',
        sourceId: 'retrieval-1',
        label: 'helpful',
        reason: 'exact_match',
      );
      expect(feedback['created'], isTrue);
      expect(feedbackPipe.writtenCommands, contains('retrieval.feedback'));
    },
  );

  test(
    'typed collaboration and handoff methods map projection mutations',
    () async {
      final pipe = _FakePipe(
        responsePayload: const {
          'created': true,
          'alreadyPresent': false,
          'projection': {
            'projects': [],
            'collaborationRuns': [],
            'handoffs': [],
          },
        },
      );
      final client = _clientFor(pipe);
      addTearDown(client.close);

      final collaboration = await client.createCollaboration(
        sessionId: 'session-test-123456',
        projectId: 'project-1',
        collaborationRunId: 'collaboration-1',
        rootAgentIds: const ['agent-1'],
      );
      final handoff = await client.createHandoff(
        sessionId: 'session-test-123456',
        handoffId: 'handoff-1',
        collaborationRunId: 'collaboration-1',
        fromExecutionRunId: 'run-1',
        sourceMessageId: 'message-1',
        fromAgentId: 'agent-1',
        toAgentId: 'agent-2',
        task: 'handoff task',
      );

      expect(collaboration.created, isTrue);
      expect(handoff.projection['handoffs'], isEmpty);
      expect(pipe.writtenCommands, ['collaboration.create', 'handoff.create']);
      expect(pipe.writtenPayloads.last['status'], 'proposed');
    },
  );

  test('typed Handoff transition methods map approval responses', () async {
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
    addTearDown(client.close);

    final result = await client.transitionHandoff(
      sessionId: 'session-test-123456',
      handoffId: 'handoff-1',
      targetStatus: 'approved',
    );

    expect(result.status, 'approved');
    expect(result.changed, isTrue);
    expect(pipe.writtenCommands, ['handoff.approve']);
    expect(pipe.writtenPayloads.single, {'handoffId': 'handoff-1'});
  });

  test('typed creation methods reject required fields before IPC', () async {
    final pipe = _FakePipe();
    final client = _clientFor(pipe);
    addTearDown(client.close);

    await expectLater(
      client.createCollaboration(
        sessionId: 'session-test-123456',
        projectId: 'project-1',
        collaborationRunId: 'collaboration-1',
        rootAgentIds: const [],
      ),
      throwsA(isA<CoreIpcException>()),
    );
    await expectLater(
      client.createHandoff(
        sessionId: 'session-test-123456',
        handoffId: '',
        collaborationRunId: 'collaboration-1',
        fromExecutionRunId: 'run-1',
        sourceMessageId: 'message-1',
        fromAgentId: 'agent-1',
        toAgentId: 'agent-2',
        task: 'handoff task',
      ),
      throwsA(isA<CoreIpcException>()),
    );
    await expectLater(
      client.startExecution(
        sessionId: 'session-test-123456',
        executionRunId: 'run-deadline-invalid',
        collaborationRunId: 'collaboration-1',
        projectId: 'project-1',
        conversationId: 'conversation-1',
        agentId: 'agent-1',
        currentTask: 'deadline validation',
        deadlineMs: 0,
      ),
      throwsA(isA<CoreIpcException>()),
    );
    expect(pipe.writtenCommands, isEmpty);
  });

  test('typed handoff dispatch maps deferred child Run response', () async {
    final pipe = _FakePipe(
      responsePayload: const {
        'created': true,
        'alreadyAtTarget': false,
        'childExecutionRunId': 'handoff-child-1',
        'runtimeStarted': false,
        'runtimeDispatch': 'deferred',
        'projection': {'runs': [], 'handoffs': []},
      },
    );
    final client = _clientFor(pipe);
    addTearDown(client.close);

    final result = await client.dispatchHandoff(
      sessionId: 'session-test-123456',
      handoffId: 'handoff-1',
    );

    expect(result.created, isTrue);
    expect(result.childExecutionRunId, 'handoff-child-1');
    expect(result.runtimeStarted, isFalse);
    expect(result.runtimeDispatch, 'deferred');
    expect(pipe.writtenCommands, ['handoff.dispatch']);
  });

  test(
    'handshake records the server epoch for cursor-aware reconnect',
    () async {
      final pipe = _FakePipe(
        responsePayload: const {'serverEpoch': 'core-current-epoch'},
      );
      final client = CoreIpcClient.forTesting(
        read: pipe.read,
        write: pipe.write,
        close: pipe.close,
        sessionCredential: 'credential-test-123456789012345678901234',
      );
      addTearDown(client.close);

      await client.handshake(
        sessionId: 'session-test-123456',
        lastSeen: const StreamCursor(
          streamId: 'core-events',
          sequence: 4,
          epoch: 'core-current-epoch',
        ),
      );

      expect(client.serverEpoch, 'core-current-epoch');
    },
  );

  test(
    'preserves REPLAY_GAP code and recovery details from ErrorEnvelope',
    () async {
      final pipe = _FakePipe(
        responseError: const {
          'code': 'REPLAY_GAP',
          'message': 'snapshot required',
          'retryable': true,
          'details': {
            'streamId': 'core-events',
            'epoch': 'epoch-1',
            'recovery': 'snapshot_then_subscribe_from_resume_cursor',
            'resumeCursor': {
              'streamId': 'core-events',
              'sequence': 8,
              'epoch': 'epoch-1',
            },
            'headCursor': {
              'streamId': 'core-events',
              'sequence': 12,
              'epoch': 'epoch-1',
            },
            'oldestAvailableCursor': {
              'streamId': 'core-events',
              'sequence': 9,
              'epoch': 'epoch-1',
            },
          },
        },
      );
      final client = _clientFor(pipe);
      addTearDown(client.close);

      await expectLater(
        client.replayEvents(sessionId: 'session-test-123456', afterSequence: 0),
        throwsA(
          isA<CoreIpcException>()
              .having((error) => error.code, 'code', 'REPLAY_GAP')
              .having((error) => error.isReplayGap, 'isReplayGap', isTrue)
              .having(
                (error) => error.replayGap?.resumeCursor?.sequence,
                'resume cursor',
                8,
              ),
        ),
      );
    },
  );

  test(
    'reader parses unsolicited events without polluting a request response',
    () async {
      final pipe = _FakePipe();
      final client = _clientFor(pipe);
      addTearDown(client.close);

      pipe.queueBeforeNextResponse(_event(sequence: 1));
      final response = await client.request(_query('request-after-event'));

      expect(response['requestId'], 'request-after-event');
      expect(pipe.readKinds, ['event', 'response']);
    },
  );

  test(
    'subscription buffers startup events and exposes ack/unsubscribe APIs',
    () async {
      const afterCursor = StreamCursor(
        streamId: 'core-events',
        sequence: 4,
        epoch: 'epoch-1',
      );
      final pipe = _FakePipe(
        responsePayload: const {'serverEpoch': 'epoch-1'},
        subscriptionPayload: {
          'subscriptionId': 'subscription-1',
          'streamId': 'core-events',
          'cursor': afterCursor.toJson(),
        },
      );
      final client = CoreIpcClient.forTesting(
        read: pipe.read,
        write: pipe.write,
        close: pipe.close,
        sessionCredential: 'credential-test-123456789012345678901234',
      );
      addTearDown(client.close);

      await client.handshake(sessionId: 'session-test-123456');
      pipe.queueBeforeNextResponse(_event(sequence: 5));
      final subscription = await client.subscribeEvents(
        sessionId: 'session-test-123456',
        afterCursor: afterCursor,
      );

      final startupEvent = await subscription.events.first;
      expect(startupEvent.cursor.sequence, 5);
      expect(subscription.lastEventCursor.sequence, 5);

      final ackResponse = await subscription.ack(
        const StreamCursor(
          streamId: 'core-events',
          sequence: 5,
          epoch: 'epoch-1',
        ),
      );
      expect(ackResponse['ok'], true);
      expect(pipe.writtenCommands, contains('events.ack'));

      await expectLater(
        subscription.ack(
          const StreamCursor(
            streamId: 'other-stream',
            sequence: 5,
            epoch: 'epoch-1',
          ),
        ),
        throwsA(isA<CoreIpcException>()),
      );

      final unsubscribeResponse = await subscription.unsubscribe();
      expect(unsubscribeResponse['ok'], true);
      expect(subscription.isActive, isFalse);
      expect(pipe.writtenCommands, contains('events.unsubscribe'));
    },
  );

  test(
    'subscription rejects an event with a wrong subscriptionId or cursor',
    () async {
      const cursor = StreamCursor(
        streamId: 'core-events',
        sequence: 4,
        epoch: 'epoch-1',
      );
      final pipe = _FakePipe(
        subscriptionPayload: {
          'subscriptionId': 'subscription-1',
          'streamId': 'core-events',
          'cursor': cursor.toJson(),
        },
      );
      final client = CoreIpcClient.forTesting(
        read: pipe.read,
        write: pipe.write,
        close: pipe.close,
        sessionCredential: 'credential-test-123456789012345678901234',
      );
      addTearDown(client.close);

      await client.handshake(sessionId: 'session-test-123456');
      final subscription = await client.subscribeEvents(
        sessionId: 'session-test-123456',
        afterCursor: cursor,
      );
      final errorExpectation = expectLater(
        subscription.events,
        emitsError(isA<CoreIpcException>()),
      );
      pipe.emit(_event(sequence: 5, subscriptionId: 'wrong-subscription'));
      await errorExpectation;
      expect(subscription.isActive, isFalse);
    },
  );

  test(
    'subscription rejects an event with a mismatched cursor stream',
    () async {
      const cursor = StreamCursor(
        streamId: 'core-events',
        sequence: 4,
        epoch: 'epoch-1',
      );
      final pipe = _FakePipe(
        subscriptionPayload: {
          'subscriptionId': 'subscription-1',
          'streamId': 'core-events',
          'cursor': cursor.toJson(),
        },
      );
      final client = CoreIpcClient.forTesting(
        read: pipe.read,
        write: pipe.write,
        close: pipe.close,
        sessionCredential: 'credential-test-123456789012345678901234',
      );
      addTearDown(client.close);

      await client.handshake(sessionId: 'session-test-123456');
      final subscription = await client.subscribeEvents(
        sessionId: 'session-test-123456',
        afterCursor: cursor,
      );
      final errorExpectation = expectLater(
        subscription.events,
        emitsError(isA<CoreIpcException>()),
      );
      pipe.emit(_event(sequence: 5, streamId: 'wrong-stream'));
      await errorExpectation;
      expect(subscription.isActive, isFalse);
    },
  );
}

CoreIpcClient _clientFor(_FakePipe pipe) {
  return CoreIpcClient.forTesting(
    read: pipe.read,
    write: pipe.write,
    close: pipe.close,
  );
}

Map<String, dynamic> _query(String requestId) => {
  'kind': 'query',
  'protocol': {'major': protocolMajor, 'minor': 0},
  'requestId': requestId,
  'sessionId': 'session-test-123456',
  'query': 'runtime.health',
  'payload': <String, dynamic>{},
};

class _FakePipe {
  _FakePipe({
    this.responseRequestId,
    this.responseProtocolMajor = protocolMajor,
    this.responsePayload,
    this.responseError,
    this.subscriptionPayload,
    this.respond = true,
  });

  final String? responseRequestId;
  final int responseProtocolMajor;
  final Map<String, dynamic>? responsePayload;
  final Map<String, dynamic>? responseError;
  final Map<String, dynamic>? subscriptionPayload;
  final bool respond;
  final IpcFrameCodec _codec = const IpcFrameCodec();
  final List<String> writtenRequestIds = [];
  final List<String> writtenCommands = [];
  final List<String> writtenQueries = [];
  final List<Map<String, dynamic>> writtenPayloads = [];
  final List<int?> writtenDeadlineMs = [];
  final List<String> readKinds = [];
  final List<Map<String, dynamic>> _beforeNextResponse = [];
  final List<_QueuedFrame> _frames = [];
  final List<Completer<void>> _readWaiters = [];
  final List<String> readRequestIds = [];
  bool _awaitingResponse = false;
  bool _closed = false;
  int closeCount = 0;

  void queueBeforeNextResponse(Map<String, dynamic> frame) {
    _beforeNextResponse.add(frame);
  }

  void emit(Map<String, dynamic> frame) {
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
    writtenDeadlineMs.add(request['deadlineMs'] as int?);
    final payload = request['payload'];
    if (payload is Map<String, dynamic>) writtenPayloads.add(payload);
    if (!respond) {
      _awaitingResponse = true;
      return;
    }
    final responsePayload = command == 'events.subscribe'
        ? subscriptionPayload
        : this.responsePayload;
    for (final unsolicited in _beforeNextResponse) {
      _enqueueFrame(unsolicited, isResponse: false);
    }
    _beforeNextResponse.clear();
    _enqueueFrame({
      'kind': responseError == null ? 'response' : 'error',
      'protocol': {'major': responseProtocolMajor, 'minor': 0},
      'requestId': responseRequestId ?? requestId,
      if (responseError == null) ...{
        'ok': true,
        'payload': responsePayload ?? <String, dynamic>{'status': 'ready'},
      } else
        ...responseError!,
    }, isResponse: true);
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
      final decoded = _codec.decodeJson(frame.bytes);
      readKinds.add(decoded['kind'] as String);
      if (frame.isResponse) {
        readRequestIds.add(decoded['requestId'] as String);
        _awaitingResponse = false;
      }
    }
    return chunk;
  }

  Future<void> close() async {
    await Future<void>.delayed(const Duration(milliseconds: 1));
    _closed = true;
    closeCount += 1;
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

Map<String, dynamic> _event({
  required int sequence,
  String subscriptionId = 'subscription-1',
  String streamId = 'core-events',
}) => EventEnvelope(
  eventId: 'event-$sequence',
  sessionId: 'session-test-123456',
  subscriptionId: subscriptionId,
  cursor: StreamCursor(
    streamId: streamId,
    sequence: sequence,
    epoch: 'epoch-1',
  ),
  event: 'output.delta',
  occurredAt: DateTime.utc(1970, 1, 1),
  payload: const {'delta': 'hello'},
).toJson();
