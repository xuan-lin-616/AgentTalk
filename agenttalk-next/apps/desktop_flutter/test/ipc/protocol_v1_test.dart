import 'dart:typed_data';

import 'package:agenttalk_desktop/ipc/protocol_v1.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  test('Dart IPC framing round trips a command envelope', () {
    const command = CommandEnvelope(
      requestId: 'req-1',
      sessionId: 'session-123456789',
      command: 'execution.start',
      payload: {'projectId': 'project-fixture'},
    );
    const codec = IpcFrameCodec();
    final frame = codec.encodeJson(command.toJson());
    final decoded = codec.decodeJson(frame);
    expect(decoded['kind'], 'command');
    expect(decoded['command'], 'execution.start');
    expect(decoded['payload']['projectId'], 'project-fixture');
  });

  test('Dart IPC framing rejects malformed lengths', () {
    const codec = IpcFrameCodec();
    expect(
      () => codec.decodeJson(Uint8List.fromList([0, 0, 0])),
      throwsFormatException,
    );
    expect(
      () => codec.decodeJson(Uint8List.fromList([0, 0, 0, 8, 1])),
      throwsFormatException,
    );
  });

  test('Dart event envelope round trips the durable cursor shape', () {
    final event = EventEnvelope(
      eventId: 'event-1',
      sessionId: 'session-123456789',
      cursor: const StreamCursor(
        streamId: 'core-events',
        sequence: 7,
        epoch: 'sqlite-event-store-v1',
      ),
      event: 'output.delta',
      occurredAt: DateTime.utc(1970, 1, 1),
      subscriptionId: 'subscription-1',
      executionRunId: 'run-1',
      payload: const {'delta': 'hello'},
    );
    final decoded = EventEnvelope.fromJson(event.toJson());
    expect(decoded.cursor.sequence, 7);
    expect(decoded.cursor.epoch, 'sqlite-event-store-v1');
    expect(decoded.subscriptionId, 'subscription-1');
    expect(decoded.event, 'output.delta');
    expect(decoded.payload['delta'], 'hello');
  });

  test(
    'Dart IPC subscription commands preserve subscription and cursor shape',
    () {
      const cursor = StreamCursor(
        streamId: 'core-events',
        sequence: 9,
        epoch: 'epoch-1',
      );
      final receipt = EventSubscriptionReceipt.fromJson({
        'subscriptionId': 'subscription-1',
        'streamId': 'core-events',
        'cursor': cursor.toJson(),
        'maxInFlightEvents': 64,
        'maxInFlightBytes': 262144,
      });
      expect(receipt.subscriptionId, 'subscription-1');
      expect(receipt.cursor.sequence, 9);

      final ack = EventAckCommand(
        requestId: 'ack-1',
        sessionId: 'session-1',
        subscriptionId: receipt.subscriptionId,
        cursor: cursor,
      ).toJson();
      expect(ack['command'], 'events.ack');
      expect(ack['payload']['cursor']['epoch'], 'epoch-1');

      final unsubscribe = EventUnsubscribeCommand(
        requestId: 'unsubscribe-1',
        sessionId: 'session-1',
        subscriptionId: receipt.subscriptionId,
      ).toJson();
      expect(unsubscribe['command'], 'events.unsubscribe');
      expect(unsubscribe['payload']['subscriptionId'], 'subscription-1');
    },
  );

  test('Dart IPC event and cursor parsers reject malformed identity', () {
    expect(
      () => StreamCursor.fromJson({'streamId': '', 'sequence': 1}),
      throwsFormatException,
    );
    expect(
      () => StreamCursor.fromJson({'streamId': 'core-events', 'sequence': -1}),
      throwsFormatException,
    );
    expect(
      () => EventEnvelope.fromJson({
        'kind': 'event',
        'eventId': 'event-1',
        'sessionId': 'session-1',
        'subscriptionId': '',
        'cursor': const StreamCursor(
          streamId: 'core-events',
          sequence: 1,
        ).toJson(),
        'event': 'output.delta',
        'occurredAt': '1970-01-01T00:00:00Z',
        'payload': <String, dynamic>{},
        'protocol': {'major': protocolMajor, 'minor': 0},
      }),
      throwsFormatException,
    );
    expect(
      () => EventSubscriptionReceipt.fromJson({
        'subscriptionId': 'subscription-1',
        'streamId': 'core-events',
        'cursor': {'streamId': 'core-events'},
      }),
      throwsFormatException,
    );
  });

  Map<String, dynamic> eventJson({
    String occurredAt = '1970-01-01T00:00:00.000Z',
  }) => {
    'kind': 'event',
    'eventId': 'event-1',
    'sessionId': 'session-123456789',
    'cursor': const StreamCursor(
      streamId: 'core-events',
      sequence: 1,
      epoch: 'epoch-1',
    ).toJson(),
    'subscriptionId': 'subscription-1',
    'executionRunId': 'run-1',
    'event': 'projection.changed',
    'occurredAt': occurredAt,
    'payload': <String, dynamic>{},
    'protocol': {'major': protocolMajor, 'minor': 0},
  };

  test('W8.1: event envelope parses a valid UTC RFC3339 occurredAt and always '
      'emits it on toJson', () {
    final event = EventEnvelope.fromJson(
      eventJson(occurredAt: '2023-11-14T22:13:20.123Z'),
    );
    expect(event.occurredAt, DateTime.utc(2023, 11, 14, 22, 13, 20, 123));
    expect(event.occurredAt.isUtc, isTrue);
    final serialized = event.toJson();
    expect(serialized['occurredAt'], '2023-11-14T22:13:20.123Z');
  });

  test('W8.1: event envelope rejects malformed, missing, empty, non-string, '
      'naive and invalid occurredAt values', () {
    // The legacy release Core format 1970-01-01T00:00:<seconds>.<millis>Z
    // is not valid RFC3339 and must fail closed.
    expect(
      () => EventEnvelope.fromJson(
        eventJson(occurredAt: '1970-01-01T00:00:1786705943.843Z'),
      ),
      throwsFormatException,
    );
    expect(
      () => EventEnvelope.fromJson(
        eventJson(occurredAt: '1970-01-01T00:00:60.000Z'),
      ),
      throwsFormatException,
    );
    // Missing occurredAt (schema-required).
    final missing = eventJson()..remove('occurredAt');
    expect(() => EventEnvelope.fromJson(missing), throwsFormatException);
    // Empty and non-string values.
    expect(
      () => EventEnvelope.fromJson(eventJson(occurredAt: '')),
      throwsFormatException,
    );
    final nonString = eventJson()..['occurredAt'] = 123;
    expect(() => EventEnvelope.fromJson(nonString), throwsFormatException);
    // Naive timestamps without a timezone are not UTC instants.
    expect(
      () => EventEnvelope.fromJson(
        eventJson(occurredAt: '2023-11-14T22:13:20.123'),
      ),
      throwsFormatException,
    );
    // Non-UTC offsets are not UTC instants.
    expect(
      () => EventEnvelope.fromJson(
        eventJson(occurredAt: '2023-11-14T22:13:20.123+02:00'),
      ),
      throwsFormatException,
    );
    // Unparseable garbage.
    expect(
      () => EventEnvelope.fromJson(eventJson(occurredAt: 'not-a-timestamp')),
      throwsFormatException,
    );
  });

  test(
    'W8.2: event envelope rejects extended years and impossible calendar dates',
    () {
      // Years beyond four digits violate RFC3339 full-date; Dart accepts
      // five-digit years, so the parser must reject them explicitly.
      expect(
        () => EventEnvelope.fromJson(
          eventJson(occurredAt: '10000-01-01T00:00:00.000Z'),
        ),
        throwsFormatException,
      );
      expect(
        () => EventEnvelope.fromJson(
          eventJson(occurredAt: '292278-01-01T00:00:00.000Z'),
        ),
        throwsFormatException,
      );
      // Impossible calendar dates must fail closed; Dart silently rolls them
      // forward (e.g. 2023-02-29 becomes 2023-03-01).
      expect(
        () => EventEnvelope.fromJson(
          eventJson(occurredAt: '2023-02-29T00:00:00.000Z'),
        ),
        throwsFormatException,
      );
      expect(
        () => EventEnvelope.fromJson(
          eventJson(occurredAt: '2024-02-30T00:00:00.000Z'),
        ),
        throwsFormatException,
      );
      expect(
        () => EventEnvelope.fromJson(
          eventJson(occurredAt: '2023-04-31T00:00:00.000Z'),
        ),
        throwsFormatException,
      );
      expect(
        () => EventEnvelope.fromJson(
          eventJson(occurredAt: '2021-02-29T00:00:00.000Z'),
        ),
        throwsFormatException,
      );
      expect(
        () => EventEnvelope.fromJson(
          eventJson(occurredAt: '1900-02-29T00:00:00.000Z'),
        ),
        throwsFormatException,
      );
      expect(
        () => EventEnvelope.fromJson(
          eventJson(occurredAt: '2023-00-15T00:00:00.000Z'),
        ),
        throwsFormatException,
      );
      expect(
        () => EventEnvelope.fromJson(
          eventJson(occurredAt: '2023-13-01T00:00:00.000Z'),
        ),
        throwsFormatException,
      );
    },
  );

  test(
    'W8.2: valid leap days and the year-9999 upper bound parse and round-trip',
    () {
      for (final value in [
        '2024-02-29T00:00:00.000Z',
        '2000-02-29T00:00:00.000Z',
        '9999-12-31T23:59:59.999Z',
      ]) {
        final event = EventEnvelope.fromJson(eventJson(occurredAt: value));
        expect(event.occurredAt.isUtc, isTrue);
        expect(event.toJson()['occurredAt'], value);
      }
    },
  );
}
