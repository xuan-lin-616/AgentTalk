import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:agenttalk_desktop/ipc/core_ipc_client.dart';
import 'package:agenttalk_desktop/ipc/local_discovery.dart';
import 'package:agenttalk_desktop/ipc/protocol_v1.dart';
import 'package:flutter_test/flutter_test.dart';

class _Pipe {
  _Pipe();

  final IpcFrameCodec _codec = const IpcFrameCodec();
  final List<String> writtenCommands = [];
  final List<String> writtenQueries = [];
  final List<Map<String, dynamic>> writtenPayloads = [];
  final List<_Frame> _frames = [];
  final List<Completer<void>> _readWaiters = [];
  bool _closed = false;
  bool _awaitingResponse = false;

  Map<String, dynamic> Function(Map<String, dynamic> request)? responder;

  void emit(Map<String, dynamic> frame, {bool isResponse = false}) {
    _frames.add(_Frame(_codec.encodeJson(frame), isResponse));
    _wakeReaders();
  }

  Future<void> write(Uint8List frame) async {
    await Future<void>.delayed(const Duration(milliseconds: 1));
    if (_awaitingResponse) {
      throw StateError('request written before response read');
    }
    final request = _codec.decodeJson(frame);
    final command = request['command'];
    if (command is String) writtenCommands.add(command);
    final query = request['query'];
    if (query is String) writtenQueries.add(query);
    final payload = request['payload'];
    if (payload is Map<String, dynamic>) writtenPayloads.add(payload);
    final response = responder?.call(request);
    if (response == null) {
      _awaitingResponse = true;
      return;
    }
    emit(response, isResponse: true);
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
      if (frame.isResponse) _awaitingResponse = false;
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

  void _wakeReaders() {
    final waiters = List<Completer<void>>.from(_readWaiters);
    _readWaiters.clear();
    for (final waiter in waiters) {
      if (!waiter.isCompleted) waiter.complete();
    }
  }
}

class _Frame {
  _Frame(this.bytes, this.isResponse);

  final Uint8List bytes;
  final bool isResponse;
  int offset = 0;
}

Map<String, dynamic> _ok(
  Map<String, dynamic> request,
  Map<String, dynamic> payload,
) => {
  'kind': 'response',
  'protocol': {'major': protocolMajor, 'minor': 0},
  'requestId': request['requestId'],
  'ok': true,
  'payload': payload,
};

Map<String, dynamic> _err(
  Map<String, dynamic> request,
  String code,
  String message,
) => {
  'kind': 'error',
  'protocol': {'major': protocolMajor, 'minor': 0},
  'requestId': request['requestId'],
  'code': code,
  'message': message,
  'retryable': false,
};

CoreIpcClient _clientFor(_Pipe pipe) => CoreIpcClient.forTesting(
  read: pipe.read,
  write: pipe.write,
  close: pipe.close,
  sessionId: 'session-ipc-test',
  serverEpoch: 'core-epoch',
);

Map<String, dynamic> _projection({
  String candidateId = 'candidate-a',
  String category = 'agent_runtime',
}) => {
  'candidateId': candidateId,
  'category': category,
  'connectorId': 'local-$candidateId',
  'runtimeType': 'acp',
  'displayName': 'Fixture Agent',
  'availability': 'unconfigured',
  'models': <String>['model-a'],
  'catalogRevision': null,
  'requiresConfiguration': true,
  'sourceKind': 'executable_inventory',
  'sourceKinds': <String>['executable_inventory'],
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
  'compatibilityState': 'not_verified',
  'authState': 'unknown',
  'healthState': 'not_checked',
  'evidenceSummary': <String>['executable_inventory'],
  'diagnostics': <Map<String, dynamic>>[],
};

Map<String, dynamic> _planPayload({
  String planId = 'plan-1',
  String? modelSelection = 'model-b',
  String targetProjectId = 'project-1',
  Map<String, dynamic>? capabilities,
}) => {
  'schemaVersion': 'agent.import.plan.v1',
  'planId': planId,
  'scanId': 'scan-1',
  'candidateId': 'candidate-a',
  'targetProjectId': targetProjectId,
  'modelSelection': modelSelection,
  'actions': <String>['create_connector_profile'],
  'connector': {'id': 'local-candidate-a', 'displayName': 'Fixture Agent'},
  'adapter': {
    'kind': 'acp',
    'protocolMajor': 1,
    'manifestId': 'org.fixture.acp',
    'manifestSha256': 'a' * 64,
    'candidateBindingDigest': 'b' * 64,
  },
  'capabilities':
      capabilities ??
      const <String, dynamic>{
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
};

void main() {
  group('request shapes', () {
    test(
      'discoveryStart sends agent.discovery.start with an empty payload',
      () async {
        final pipe = _Pipe();
        pipe.responder = (request) => _ok(request, {
          'scanId': 'scan-1',
          'accepted': true,
          'state': 'running',
          'eventStream': {
            'streamId': 'local-discovery-events',
            'epoch': 'epoch-1',
          },
        });
        final client = _clientFor(pipe);
        addTearDown(client.close);

        final result = await client.discoveryStart(
          sessionId: 'session-ipc-test',
          requestId: 'start-1',
        );
        expect(pipe.writtenCommands, ['agent.discovery.start']);
        expect(pipe.writtenPayloads.single, isEmpty);
        expect(result.scanId, 'scan-1');
        expect(result.eventStreamId, 'local-discovery-events');
        expect(result.eventEpoch, 'epoch-1');
      },
    );

    test(
      'discoveryStart sends an explicit executable path when selected',
      () async {
        final pipe = _Pipe();
        pipe.responder = (request) => _ok(request, {
          'scanId': 'scan-explicit',
          'accepted': true,
          'state': 'running',
          'eventStream': {
            'streamId': 'local-discovery-events',
            'epoch': 'epoch-explicit',
          },
        });
        final client = _clientFor(pipe);
        addTearDown(client.close);

        await client.discoveryStart(
          sessionId: 'session-ipc-test',
          requestId: 'start-explicit',
          explicitExecutablePath: r'C:\\Agents\\fixture.exe',
        );
        expect(pipe.writtenCommands, ['agent.discovery.start']);
        expect(pipe.writtenPayloads.single, {
          'explicitExecutablePath': r'C:\\Agents\\fixture.exe',
        });
      },
    );

    test('discoverySnapshot sends scanId only', () async {
      final pipe = _Pipe();
      pipe.responder = (request) => _ok(request, {
        'schemaVersion': 'agent.discovery.snapshot.v1',
        'scanId': 'scan-1',
        'state': 'completed',
        'candidates': <Map<String, dynamic>>[
          {
            'candidateId': 'candidate-a',
            'candidate': _projection(),
            'verification': null,
            'lifecycleState': 'identified',
          },
        ],
        'diagnostics': <Map<String, dynamic>>[],
      });
      final client = _clientFor(pipe);
      addTearDown(client.close);

      final snapshot = await client.discoverySnapshot(
        sessionId: 'session-ipc-test',
        requestId: 'snap-1',
        scanId: 'scan-1',
      );
      expect(pipe.writtenQueries, ['agent.discovery.snapshot']);
      expect(pipe.writtenPayloads.single, {'scanId': 'scan-1'});
      expect(snapshot.scanId, 'scan-1');
      expect(snapshot.state, 'completed');
      expect(snapshot.candidates.single.candidate.displayName, 'Fixture Agent');
      expect(snapshot.candidates.single.lifecycleState, 'identified');
    });

    test('discoveryVerify sends scanId/candidateId/consent', () async {
      final pipe = _Pipe();
      pipe.responder = (request) => _ok(request, {
        'scanId': 'scan-1',
        'candidateId': 'candidate-a',
        'accepted': true,
        'state': 'verifying',
      });
      final client = _clientFor(pipe);
      addTearDown(client.close);

      await client.discoveryVerify(
        sessionId: 'session-ipc-test',
        requestId: 'verify-1',
        scanId: 'scan-1',
        candidateId: 'candidate-a',
        consent: true,
      );
      expect(pipe.writtenCommands, ['agent.discovery.verify']);
      expect(pipe.writtenPayloads.single['consent'], true);
      expect(
        pipe.writtenPayloads.single.keys,
        containsAll(['scanId', 'candidateId']),
      );
    });

    test('discoveryDismiss sends scanId/candidateId only', () async {
      final pipe = _Pipe();
      pipe.responder = (request) => _ok(request, {
        'scanId': 'scan-1',
        'candidateId': 'candidate-a',
        'dismissed': true,
      });
      final client = _clientFor(pipe);
      addTearDown(client.close);

      final result = await client.discoveryDismiss(
        sessionId: 'session-ipc-test',
        requestId: 'dismiss-1',
        scanId: 'scan-1',
        candidateId: 'candidate-a',
      );
      expect(result.dismissed, true);
      expect(pipe.writtenPayloads.single.keys, {'scanId', 'candidateId'});
    });

    test(
      'importPlan sends only the allowlisted business fields with null modelSelection',
      () async {
        final pipe = _Pipe();
        pipe.responder = (request) => _ok(request, {
          'schemaVersion': 'agent.import.plan.v1',
          'planId': 'plan-1',
          'scanId': 'scan-1',
          'candidateId': 'candidate-a',
          'targetProjectId': 'project-1',
          'modelSelection': null,
          'actions': <String>['create_connector_profile'],
          'connector': {
            'id': 'local-candidate-a',
            'displayName': 'Fixture Agent',
          },
          'adapter': {
            'kind': 'acp',
            'protocolMajor': 1,
            'manifestId': 'org.fixture.acp',
            'manifestSha256': 'a' * 64,
            'candidateBindingDigest': 'b' * 64,
          },
          'capabilities': <String, dynamic>{
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
        final client = _clientFor(pipe);
        addTearDown(client.close);

        final plan = await client.importPlan(
          sessionId: 'session-ipc-test',
          requestId: 'plan-1',
          scanId: 'scan-1',
          candidateId: 'candidate-a',
          projectId: 'project-1',
        );
        expect(pipe.writtenQueries, ['agent.import.plan']);
        expect(
          pipe.writtenPayloads.single.keys,
          {'scanId', 'candidateId', 'projectId', 'modelSelection'},
          reason: 'import plan must submit only allowlisted fields',
        );
        expect(pipe.writtenPayloads.single['modelSelection'], isNull);
        // Binding/fingerprint material never enters renderer state.
        expect(plan.planId, 'plan-1');
        expect(plan.readOnly, true);
        expect(plan.manifestId, 'org.fixture.acp');
      },
    );

    test(
      'pinned modelSelection submits exactly one normalized model id',
      () async {
        final pipe = _Pipe();
        pipe.responder = (request) => _ok(request, {
          'schemaVersion': 'agent.import_local.v1',
          'importId': 'import-1',
          'connectorId': 'local-candidate-a',
          'agentId': 'agent-1',
          'projectId': 'project-1',
          'reused': false,
          'eventSequence': 3,
        });
        final client = _clientFor(pipe);
        addTearDown(client.close);

        await client.importLocal(
          sessionId: 'session-ipc-test',
          requestId: 'import-1',
          scanId: 'scan-1',
          candidateId: 'candidate-a',
          projectId: 'project-1',
          modelSelection: 'model-b',
        );
        expect(pipe.writtenCommands, ['agent.import_local']);
        expect(pipe.writtenPayloads.single['modelSelection'], 'model-b');
        expect(pipe.writtenPayloads.single.keys, isNot(contains('planId')));
        expect(pipe.writtenPayloads.single.keys, isNot(contains('binding')));
      },
    );
  });

  group('strict parsing and privacy', () {
    test('start response with unknown eventStream is rejected', () async {
      final pipe = _Pipe();
      pipe.responder = (request) => _ok(request, {
        'scanId': 'scan-1',
        'accepted': true,
        'state': 'running',
        'eventStream': {'streamId': 'core-events', 'epoch': 'e'},
      });
      final client = _clientFor(pipe);
      addTearDown(client.close);

      await expectLater(
        client.discoveryStart(sessionId: 'session-ipc-test', requestId: 's'),
        throwsA(isA<CoreIpcException>()),
      );
    });

    test(
      'snapshot rejects a candidate carrying a forbidden path/PID/token field',
      () async {
        final pipe = _Pipe();
        final leaked = _projection();
        leaked['executablePath'] = 'C:\\secret\\agent.exe';
        pipe.responder = (request) => _ok(request, {
          'schemaVersion': 'agent.discovery.snapshot.v1',
          'scanId': 'scan-1',
          'state': 'completed',
          'candidates': <Map<String, dynamic>>[
            {
              'candidateId': 'candidate-a',
              'candidate': leaked,
              'verification': null,
              'lifecycleState': 'identified',
            },
          ],
          'diagnostics': <Map<String, dynamic>>[],
        });
        final client = _clientFor(pipe);
        addTearDown(client.close);

        await expectLater(
          client.discoverySnapshot(
            sessionId: 'session-ipc-test',
            requestId: 's',
            scanId: 'scan-1',
          ),
          throwsA(isA<CoreIpcException>()),
        );
      },
    );

    test(
      'snapshot rejects compound path/PID/port keys in any casing but not public fields',
      () async {
        for (final key in [
          'sourcePath',
          'sourcepath',
          'SOURCEPATH',
          'binaryPath',
          'binarypath',
          'BINARYPATH',
          'executable_path',
          'listenerPort',
          'listenerport',
          'LISTENERPORT',
          'processPid',
          'processpid',
          'PROCESSPID',
          'process_pid',
        ]) {
          final pipe = _Pipe();
          final leaked = _projection();
          leaked[key] = 'sensitive';
          pipe.responder = (request) => _ok(request, {
            'schemaVersion': 'agent.discovery.snapshot.v1',
            'scanId': 'scan-1',
            'state': 'completed',
            'candidates': <Map<String, dynamic>>[
              {
                'candidateId': 'candidate-a',
                'candidate': leaked,
                'verification': null,
                'lifecycleState': 'identified',
              },
            ],
            'diagnostics': <Map<String, dynamic>>[],
          });
          final client = _clientFor(pipe);
          addTearDown(client.close);
          await expectLater(
            client.discoverySnapshot(
              sessionId: 'session-ipc-test',
              requestId: 's',
              scanId: 'scan-1',
            ),
            throwsA(isA<CoreIpcException>()),
            reason: 'compound key $key must be rejected',
          );
        }

        for (final key in [
          'transport',
          'importId',
          'sourceKind',
          'sourceKinds',
        ]) {
          final pipe = _Pipe();
          final benign = _projection();
          benign[key] = 'safe-value';
          pipe.responder = (request) => _ok(request, {
            'schemaVersion': 'agent.discovery.snapshot.v1',
            'scanId': 'scan-1',
            'state': 'completed',
            'candidates': <Map<String, dynamic>>[
              {
                'candidateId': 'candidate-a',
                'candidate': benign,
                'verification': null,
                'lifecycleState': 'identified',
              },
            ],
            'diagnostics': <Map<String, dynamic>>[],
          });
          final client = _clientFor(pipe);
          addTearDown(client.close);
          await client.discoverySnapshot(
            sessionId: 'session-ipc-test',
            requestId: 's',
            scanId: 'scan-1',
          );
        }
      },
    );

    test(
      'snapshot rejects an unknown category and unknown state enum values',
      () async {
        final pipe = _Pipe();
        final projection = _projection();
        projection['category'] = 'alien_category';
        pipe.responder = (request) => _ok(request, {
          'schemaVersion': 'agent.discovery.snapshot.v1',
          'scanId': 'scan-1',
          'state': 'completed',
          'candidates': <Map<String, dynamic>>[
            {
              'candidateId': 'candidate-a',
              'candidate': projection,
              'verification': null,
              'lifecycleState': 'identified',
            },
          ],
          'diagnostics': <Map<String, dynamic>>[],
        });
        final client = _clientFor(pipe);
        addTearDown(client.close);

        await expectLater(
          client.discoverySnapshot(
            sessionId: 'session-ipc-test',
            requestId: 's',
            scanId: 'scan-1',
          ),
          throwsA(isA<CoreIpcException>()),
        );

        final statePipe = _Pipe();
        final stateProjection = _projection();
        stateProjection['discoveryState'] = 'warped';
        statePipe.responder = (request) => _ok(request, {
          'schemaVersion': 'agent.discovery.snapshot.v1',
          'scanId': 'scan-1',
          'state': 'completed',
          'candidates': <Map<String, dynamic>>[
            {
              'candidateId': 'candidate-a',
              'candidate': stateProjection,
              'verification': null,
              'lifecycleState': 'identified',
            },
          ],
          'diagnostics': <Map<String, dynamic>>[],
        });
        final stateClient = _clientFor(statePipe);
        addTearDown(stateClient.close);
        await expectLater(
          stateClient.discoverySnapshot(
            sessionId: 'session-ipc-test',
            requestId: 's',
            scanId: 'scan-1',
          ),
          throwsA(isA<CoreIpcException>()),
        );
      },
    );

    test(
      'import plan DTO never exposes binding or fingerprint fields',
      () async {
        final plan = ImportPlan.fromResponse({
          'payload': {
            'schemaVersion': 'agent.import.plan.v1',
            'planId': 'plan-1',
            'scanId': 'scan-1',
            'candidateId': 'candidate-a',
            'targetProjectId': 'project-1',
            'modelSelection': null,
            'actions': <String>['create_connector_profile'],
            'connector': {'id': 'c', 'displayName': 'C'},
            'adapter': {
              'kind': 'acp',
              'protocolMajor': 1,
              'manifestId': 'm',
              'manifestSha256': 'a' * 64,
              'candidateBindingDigest': 'b' * 64,
            },
            'capabilities': <String, dynamic>{
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
          },
        });
        final fields = plan.toJsonPublic();
        expect(fields.containsKey('manifestSha256'), isFalse);
        expect(fields.containsKey('candidateBindingDigest'), isFalse);
      },
    );
  });

  group('typed errors and recovery', () {
    test('typed Core errors surface as CoreIpcException with the code', () async {
      final pipe = _Pipe();
      pipe.responder = (request) => _err(
        request,
        'DISCOVERY_OWNER_VERIFICATION_CAPACITY_EXHAUSTED',
        'discovery verification capacity for the authenticated owner is exhausted',
      );
      final client = _clientFor(pipe);
      addTearDown(client.close);

      await expectLater(
        client.discoveryVerify(
          sessionId: 'session-ipc-test',
          requestId: 'v',
          scanId: 'scan-1',
          candidateId: 'candidate-a',
          consent: true,
        ),
        throwsA(
          isA<CoreIpcException>().having(
            (error) => error.code,
            'code',
            'DISCOVERY_OWNER_VERIFICATION_CAPACITY_EXHAUSTED',
          ),
        ),
      );
    });

    test('replay-gap error carries replayGap details and is detectable', () {
      final error = CoreIpcException(
        'replay gap',
        code: 'REPLAY_GAP',
        details: const {
          'streamId': 'local-discovery-events',
          'epoch': 'e',
          'requiresSnapshot': true,
        },
      );
      expect(error.isReplayGap, isTrue);
      expect(error.replayGap?.streamId, 'local-discovery-events');
    });

    test(
      'discoveryReplay requests streamId + epoch and validates envelopes',
      () async {
        final pipe = _Pipe();
        pipe.responder = (request) => _ok(request, {
          'events': <Map<String, dynamic>>[
            {
              'kind': 'event',
              'protocol': {'major': protocolMajor, 'minor': 0},
              'eventId': 'event-1',
              'sessionId': 'session-ipc-test',
              'subscriptionId': 'sub-1',
              'cursor': {
                'streamId': 'local-discovery-events',
                'sequence': 1,
                'epoch': 'discovery-epoch-1',
              },
              'event': 'agent.discovery.completed',
              'occurredAt': '2026-01-01T00:00:00.000Z',
              'payload': {'scanId': 'scan-1', 'candidateCount': 4},
            },
          ],
        });
        final client = _clientFor(pipe);
        addTearDown(client.close);

        final events = await client.discoveryReplay(
          sessionId: 'session-ipc-test',
          requestId: 'replay-1',
          epoch: 'discovery-epoch-1',
        );
        expect(pipe.writtenQueries, ['events.replay']);
        expect(
          pipe.writtenPayloads.single['streamId'],
          'local-discovery-events',
        );
        expect(pipe.writtenPayloads.single['epoch'], 'discovery-epoch-1');
        expect(events.single.event, 'agent.discovery.completed');
        final summary = DiscoveryEventSummary.fromEnvelope(events.single);
        expect(summary.candidateCount, 4);
      },
    );

    test(
      'subscribeDiscoveryEvents uses the discovery stream cursor and ACK uses the subscription',
      () async {
        final pipe = _Pipe();
        pipe.responder = (request) {
          final command = request['command'];
          if (command == 'events.subscribe') {
            final payload = request['payload'] as Map<String, dynamic>;
            final afterCursor = payload['afterCursor'] as Map<String, dynamic>;
            return _ok(request, {
              'subscriptionId': 'sub-discovery',
              'streamId': 'local-discovery-events',
              'cursor': {
                'streamId': 'local-discovery-events',
                'sequence': afterCursor['sequence'],
                'epoch': 'discovery-epoch-1',
              },
              'maxInFlightEvents': 64,
              'maxInFlightBytes': 262144,
            });
          }
          if (command == 'events.ack' || command == 'events.unsubscribe') {
            return _ok(request, <String, dynamic>{});
          }
          return _err(request, 'INVALID_COMMAND', 'unexpected');
        };
        final client = _clientFor(pipe);
        addTearDown(client.close);

        final subscription = await client.subscribeDiscoveryEvents(
          sessionId: 'session-ipc-test',
          epoch: 'discovery-epoch-1',
          afterSequence: 17,
        );
        expect(subscription.streamId, 'local-discovery-events');
        final subscribePayload = pipe.writtenPayloads.first;
        expect(
          (subscribePayload['afterCursor'] as Map<String, dynamic>)['streamId'],
          'local-discovery-events',
        );
        expect(
          (subscribePayload['afterCursor'] as Map<String, dynamic>)['sequence'],
          17,
        );
        await subscription.ack(subscription.lastEventCursor);
        expect(pipe.writtenCommands, contains('events.ack'));
        await subscription.unsubscribe();
        expect(pipe.writtenCommands, contains('events.unsubscribe'));
      },
    );
  });

  group('W7.1 plan capabilities and nested boundaries', () {
    test('plan rejects nested sensitive capability keys (W7.1 red)', () {
      for (final capabilities in [
        {'loadSession': true, 'token': 'secret'},
        {'loadSession': true, 'path': r'C:\secret\agent.exe'},
        {'loadSession': true, 'pid': 1234},
        {'loadSession': true, 'port': 8080},
        {
          'loadSession': true,
          'credential': {'user': 'x'},
        },
        {
          'loadSession': true,
          'binding': {'digest': 'x'},
        },
        {'loadSession': true, 'locatorRef': 'x'},
        {'loadSession': true, 'cookie': 'session=1'},
        {'loadSession': true, 'authorization': 'Bearer x'},
      ]) {
        final plan = _planPayload(capabilities: capabilities);
        expect(
          () => ImportPlan.fromResponse({'payload': plan}),
          throwsA(isA<CoreIpcException>()),
          reason:
              'nested capabilities with sensitive keys must be rejected: $capabilities',
        );
      }
    });

    test(
      'plan rejects unknown, non-bool and case-variant capability keys (W7.1 red)',
      () {
        for (final capabilities in [
          {'loadSession': true, 'load_session': false},
          {'loadSession': 'yes'},
          {'LoadSession': true},
          {'loadSession': true, 'promptImage': 1},
          {'loadSession': true, 'extraCapability': true},
        ]) {
          final plan = _planPayload(capabilities: capabilities);
          expect(
            () => ImportPlan.fromResponse({'payload': plan}),
            throwsA(isA<CoreIpcException>()),
            reason:
                'capabilities must be exactly the seven booleans: $capabilities',
          );
        }
      },
    );

    test('plan rejects unexpected top-level fields including future, case and '
        'underscore variants (W7.2 red)', () {
      final keys = <String, Object?>{
        'futureField': false,
        'planMode': 'x',
        'FutureField': true,
        'future_field': true,
        'manifestSha256': 'a' * 64,
        'candidateBindingDigest': 'b' * 64,
      };
      for (final entry in keys.entries) {
        final plan = _planPayload();
        plan[entry.key] = entry.value;
        expect(
          () => ImportPlan.fromResponse({'payload': plan}),
          throwsA(isA<CoreIpcException>()),
          reason: 'top-level ${entry.key} must be rejected',
        );
      }
    });

    test('adapter private digest fields are the only allowed digest location '
        'and are dropped (W7.2 green)', () {
      final plan = ImportPlan.fromResponse({'payload': _planPayload()});
      final json = jsonEncode(plan.toJsonPublic());
      expect(json, isNot(contains('manifestSha256')));
      expect(json, isNot(contains('candidateBindingDigest')));
      expect(json, isNot(contains('digest')));
      expect(plan.planId, 'plan-1');
      expect(plan.manifestId, 'org.fixture.acp');
    });

    test('a fully legal plan still parses (W7.2 green)', () {
      final plan = ImportPlan.fromResponse({'payload': _planPayload()});
      expect(plan.scanId, 'scan-1');
      expect(plan.candidateId, 'candidate-a');
      expect(plan.targetProjectId, 'project-1');
      expect(plan.modelSelection, 'model-b');
      expect(plan.capabilities.loadSession, isTrue);
    });

    test('plan DTO never exposes a raw capabilities map (W7.1 red)', () {
      final plan = ImportPlan.fromResponse({'payload': _planPayload()});
      expect(
        plan.capabilities,
        isNot(isA<Map<String, dynamic>>()),
        reason: 'capabilities must be a strict typed summary, not a raw Map',
      );
    });

    test(
      'plan rejects unexpected top-level, connector and adapter fields (W7.1 red)',
      () {
        final mutations = <void Function(Map<String, dynamic> plan)>[
          (plan) => plan['locatorRef'] = r'C:\x',
          (plan) => plan['candidateBinding'] = 'x',
          (plan) =>
              (plan['connector'] as Map<String, dynamic>)['path'] = r'C:\x',
          (plan) =>
              (plan['connector'] as Map<String, dynamic>)['token'] = 'secret',
          (plan) =>
              (plan['adapter'] as Map<String, dynamic>)['executable'] = r'C:\x',
          (plan) => (plan['adapter'] as Map<String, dynamic>)['pid'] = 1,
        ];
        for (var i = 0; i < mutations.length; i++) {
          final plan = _planPayload();
          mutations[i](plan);
          try {
            ImportPlan.fromResponse({'payload': plan});
            // ignore: avoid_print
            print('MUTATION $i NOT REJECTED: $plan');
          } on CoreIpcException {
            // expected
          }
        }
        for (final mutate in mutations) {
          final plan = _planPayload();
          mutate(plan);
          expect(
            () => ImportPlan.fromResponse({'payload': plan}),
            throwsA(isA<CoreIpcException>()),
            reason: 'unexpected plan field must be rejected',
          );
        }
      },
    );

    test(
      'plan private fields never enter the DTO or the import request (W7.1)',
      () async {
        final plan = ImportPlan.fromResponse({'payload': _planPayload()});
        final json = jsonEncode(plan.toJsonPublic());
        expect(json, isNot(contains('manifestSha256')));
        expect(json, isNot(contains('candidateBindingDigest')));
        expect(json, isNot(contains('binding')));

        final pipe = _Pipe();
        pipe.responder = (request) => _ok(request, {
          'schemaVersion': 'agent.import_local.v1',
          'importId': 'import-1',
          'connectorId': 'local-candidate-a',
          'agentId': 'agent-1',
          'projectId': 'project-1',
          'reused': false,
          'eventSequence': 3,
        });
        final client = _clientFor(pipe);
        addTearDown(client.close);
        await client.importLocal(
          sessionId: 'session-ipc-test',
          requestId: 'import-1',
          scanId: 'scan-1',
          candidateId: 'candidate-a',
          projectId: 'project-1',
          modelSelection: 'model-b',
        );
        final payloadJson = jsonEncode(pipe.writtenPayloads.single);
        expect(payloadJson, isNot(contains('manifestSha256')));
        expect(payloadJson, isNot(contains('candidateBindingDigest')));
        expect(payloadJson, isNot(contains('binding')));
      },
    );

    test(
      'candidate verification rejects agentInfo with raw sensitive fields and unknown diagnostics (W7.1 red)',
      () {
        final pipe = _Pipe();
        final projection = _projection();
        pipe.responder = (request) => _ok(request, {
          'schemaVersion': 'agent.discovery.snapshot.v1',
          'scanId': 'scan-1',
          'state': 'completed',
          'candidates': <Map<String, dynamic>>[
            {
              'candidateId': 'candidate-a',
              'candidate': projection,
              'verification': {
                'candidateId': 'candidate-a',
                'status': 'verified',
                'compatibilityState': 'compatible',
                'authState': 'not_required',
                'requiresConfiguration': false,
                'protocolMajor': 1,
                'agentInfo': {'name': 'X', 'version': '1', 'path': r'C:\x'},
                'capabilities': <String, dynamic>{'loadSession': true},
                'diagnostic': 'sqlite_disk_io_error',
              },
              'lifecycleState': 'verified',
            },
          ],
          'diagnostics': <Map<String, dynamic>>[],
        });
        final client = _clientFor(pipe);
        addTearDown(client.close);
        expect(
          client.discoverySnapshot(
            sessionId: 'session-ipc-test',
            requestId: 's',
            scanId: 'scan-1',
          ),
          throwsA(isA<CoreIpcException>()),
        );
      },
    );
  });
}
