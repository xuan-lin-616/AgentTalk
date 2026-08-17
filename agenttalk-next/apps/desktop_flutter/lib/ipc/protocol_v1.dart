import 'dart:convert';
import 'dart:typed_data';

const int protocolMajor = 1;
const int defaultMaxMessageBytes = 1024 * 1024;
const String orchestrationRunSnapshotQuery = 'orchestration.run.snapshot';
const String orchestrationRunRecoveryStateQuery =
    'orchestration.run.recovery_state';

class ProtocolVersion {
  const ProtocolVersion(this.major, this.minor);
  final int major;
  final int minor;

  Map<String, dynamic> toJson() => {'major': major, 'minor': minor};

  factory ProtocolVersion.fromJson(Map<String, dynamic> json) {
    final major = json['major'];
    final minor = json['minor'];
    if (major is! int || minor is! int || major < 0 || minor < 0) {
      throw const FormatException('IPC protocol version is malformed');
    }
    return ProtocolVersion(major, minor);
  }
}

class StreamCursor {
  const StreamCursor({
    required this.streamId,
    required this.sequence,
    this.epoch,
  });
  final String streamId;
  final int sequence;
  final String? epoch;

  factory StreamCursor.fromJson(Map<String, dynamic> json) {
    final streamId = json['streamId'];
    final sequence = json['sequence'];
    final epoch = json['epoch'];
    if (streamId is! String ||
        streamId.isEmpty ||
        sequence is! int ||
        sequence < 0 ||
        (epoch != null && (epoch is! String || epoch.isEmpty))) {
      throw const FormatException('IPC stream cursor is malformed');
    }
    return StreamCursor(
      streamId: streamId,
      sequence: sequence,
      epoch: epoch as String?,
    );
  }

  Map<String, dynamic> toJson() => {
    'streamId': streamId,
    'sequence': sequence,
    if (epoch != null) 'epoch': epoch,
  };
}

class CommandEnvelope {
  const CommandEnvelope({
    required this.requestId,
    required this.sessionId,
    required this.command,
    required this.payload,
    this.deadlineMs,
    this.protocol = const ProtocolVersion(protocolMajor, 0),
  });

  final String requestId;
  final String sessionId;
  final String command;
  final Map<String, dynamic> payload;
  final int? deadlineMs;
  final ProtocolVersion protocol;

  Map<String, dynamic> toJson() => {
    'kind': 'command',
    'protocol': protocol.toJson(),
    'requestId': requestId,
    'sessionId': sessionId,
    'command': command,
    'payload': payload,
    if (deadlineMs != null) 'deadlineMs': deadlineMs,
  };
}

class EventEnvelope {
  const EventEnvelope({
    required this.eventId,
    required this.sessionId,
    required this.cursor,
    required this.event,
    required this.occurredAt,
    required this.payload,
    this.subscriptionId,
    this.executionRunId,
    this.protocol = const ProtocolVersion(protocolMajor, 0),
  });

  final String eventId;
  final String sessionId;
  final StreamCursor cursor;
  final String event;
  final DateTime occurredAt;
  final Map<String, dynamic> payload;
  final String? subscriptionId;
  final String? executionRunId;
  final ProtocolVersion protocol;

  factory EventEnvelope.fromJson(Map<String, dynamic> json) {
    final eventId = json['eventId'];
    final sessionId = json['sessionId'];
    final cursor = json['cursor'];
    final payload = json['payload'];
    final event = json['event'];
    final occurredAt = json['occurredAt'];
    final subscriptionId = json['subscriptionId'];
    final executionRunId = json['executionRunId'];
    final protocol = json['protocol'];
    if (json['kind'] != 'event' ||
        eventId is! String ||
        eventId.isEmpty ||
        sessionId is! String ||
        sessionId.isEmpty ||
        cursor is! Map<String, dynamic> ||
        event is! String ||
        event.isEmpty ||
        payload is! Map<String, dynamic> ||
        occurredAt is! String ||
        occurredAt.isEmpty ||
        !_isUtcInstant(occurredAt) ||
        (subscriptionId != null &&
            (subscriptionId is! String || subscriptionId.isEmpty)) ||
        (executionRunId != null && executionRunId is! String) ||
        protocol is! Map<String, dynamic>) {
      throw const FormatException('IPC event envelope is malformed');
    }
    return EventEnvelope(
      eventId: eventId,
      sessionId: sessionId,
      cursor: StreamCursor.fromJson(cursor),
      event: event,
      occurredAt: DateTime.parse(occurredAt),
      payload: payload,
      subscriptionId: subscriptionId as String?,
      executionRunId: executionRunId as String?,
      protocol: ProtocolVersion.fromJson(protocol),
    );
  }

  /// The IPC schema requires `occurredAt` to be a UTC RFC3339 instant. Dart's
  /// parser normalizes explicit offsets to UTC, rolls invalid seconds and
  /// impossible calendar dates forward, and accepts five-digit years, so the
  /// textual shape is validated strictly as well: a trailing `Z`, exactly four
  /// digit years, a real calendar date (leap years included), and an
  /// HH:MM:SS time part with valid ranges. Any malformed value fails closed.
  static bool _isUtcInstant(String value) {
    if (!value.endsWith('Z')) return false;
    final parsed = DateTime.tryParse(value);
    if (parsed == null || !parsed.isUtc) return false;
    final core = value.substring(0, value.length - 1);
    final parts = core.split('T');
    if (parts.length != 2) return false;
    final dateParts = parts[0].split('-');
    if (dateParts.length != 3) return false;
    // RFC3339 full-date requires exactly four digit years; extended years
    // must be rejected even though Dart parses them.
    final yearText = dateParts[0];
    if (yearText.length != 4 || !RegExp(r'^\d{4}$').hasMatch(yearText)) {
      return false;
    }
    final year = int.tryParse(yearText);
    final month = int.tryParse(dateParts[1]);
    final day = int.tryParse(dateParts[2]);
    if (year == null || month == null || day == null) return false;
    if (year < 0 || month < 1 || month > 12 || day < 1 || day > 31) {
      return false;
    }
    // Real calendar date: month day counts with leap-year rules. Dart rolls
    // impossible dates (e.g. 2023-02-29 -> 2023-03-01) forward, so the date
    // must be validated explicitly.
    final int daysInMonth;
    switch (month) {
      case 1 || 3 || 5 || 7 || 8 || 10 || 12:
        daysInMonth = 31;
      case 4 || 6 || 9 || 11:
        daysInMonth = 30;
      case 2:
        final isLeap = (year % 400 == 0) || (year % 4 == 0 && year % 100 != 0);
        daysInMonth = isLeap ? 29 : 28;
      default:
        return false;
    }
    if (day > daysInMonth) {
      return false;
    }
    final time = parts[1];
    final dotIndex = time.indexOf('.');
    final hms = dotIndex >= 0 ? time.substring(0, dotIndex) : time;
    final fraction = dotIndex >= 0 ? time.substring(dotIndex + 1) : '';
    if (fraction.isNotEmpty && !RegExp(r'^\d+$').hasMatch(fraction)) {
      return false;
    }
    final hmsParts = hms.split(':');
    if (hmsParts.length != 3) return false;
    final hh = int.tryParse(hmsParts[0]);
    final mm = int.tryParse(hmsParts[1]);
    final ss = int.tryParse(hmsParts[2]);
    if (hh == null || mm == null || ss == null) return false;
    if (hh < 0 || hh > 23 || mm < 0 || mm > 59 || ss < 0 || ss > 59) {
      return false;
    }
    return true;
  }

  Map<String, dynamic> toJson() => {
    'kind': 'event',
    'protocol': protocol.toJson(),
    'eventId': eventId,
    'sessionId': sessionId,
    'cursor': cursor.toJson(),
    if (subscriptionId != null) 'subscriptionId': subscriptionId,
    if (executionRunId != null) 'executionRunId': executionRunId,
    'event': event,
    'occurredAt': occurredAt.toUtc().toIso8601String(),
    'payload': payload,
  };
}

class EventSubscriptionReceipt {
  const EventSubscriptionReceipt({
    required this.subscriptionId,
    required this.streamId,
    required this.cursor,
    this.maxInFlightEvents,
    this.maxInFlightBytes,
  });

  final String subscriptionId;
  final String streamId;
  final StreamCursor cursor;
  final int? maxInFlightEvents;
  final int? maxInFlightBytes;

  factory EventSubscriptionReceipt.fromJson(Map<String, dynamic> json) {
    final subscriptionId = json['subscriptionId'];
    final streamId = json['streamId'];
    final cursor = json['cursor'];
    final maxInFlightEvents = json['maxInFlightEvents'];
    final maxInFlightBytes = json['maxInFlightBytes'];
    if (subscriptionId is! String ||
        subscriptionId.isEmpty ||
        streamId is! String ||
        streamId.isEmpty ||
        cursor is! Map<String, dynamic> ||
        (maxInFlightEvents != null &&
            (maxInFlightEvents is! int || maxInFlightEvents <= 0)) ||
        (maxInFlightBytes != null &&
            (maxInFlightBytes is! int || maxInFlightBytes <= 0))) {
      throw const FormatException(
        'IPC event subscription receipt is malformed',
      );
    }
    return EventSubscriptionReceipt(
      subscriptionId: subscriptionId,
      streamId: streamId,
      cursor: StreamCursor.fromJson(cursor),
      maxInFlightEvents: maxInFlightEvents as int?,
      maxInFlightBytes: maxInFlightBytes as int?,
    );
  }

  Map<String, dynamic> toJson() => {
    'subscriptionId': subscriptionId,
    'streamId': streamId,
    'cursor': cursor.toJson(),
    if (maxInFlightEvents != null) 'maxInFlightEvents': maxInFlightEvents,
    if (maxInFlightBytes != null) 'maxInFlightBytes': maxInFlightBytes,
  };
}

class EventAckCommand {
  const EventAckCommand({
    required this.requestId,
    required this.sessionId,
    required this.subscriptionId,
    required this.cursor,
    this.protocol = const ProtocolVersion(protocolMajor, 0),
  });

  final String requestId;
  final String sessionId;
  final String subscriptionId;
  final StreamCursor cursor;
  final ProtocolVersion protocol;

  Map<String, dynamic> toJson() => {
    'kind': 'command',
    'protocol': protocol.toJson(),
    'requestId': requestId,
    'sessionId': sessionId,
    'command': 'events.ack',
    'payload': {'subscriptionId': subscriptionId, 'cursor': cursor.toJson()},
  };
}

class EventUnsubscribeCommand {
  const EventUnsubscribeCommand({
    required this.requestId,
    required this.sessionId,
    required this.subscriptionId,
    this.protocol = const ProtocolVersion(protocolMajor, 0),
  });

  final String requestId;
  final String sessionId;
  final String subscriptionId;
  final ProtocolVersion protocol;

  Map<String, dynamic> toJson() => {
    'kind': 'command',
    'protocol': protocol.toJson(),
    'requestId': requestId,
    'sessionId': sessionId,
    'command': 'events.unsubscribe',
    'payload': {'subscriptionId': subscriptionId},
  };
}

/// The error envelope is kept separate from response payloads so callers can
/// preserve protocol error codes and recovery details without guessing from a
/// formatted exception string.
class IpcErrorEnvelope {
  const IpcErrorEnvelope({
    required this.requestId,
    required this.code,
    required this.message,
    required this.retryable,
    this.details,
    this.protocol = const ProtocolVersion(protocolMajor, 0),
  });

  final String requestId;
  final String code;
  final String message;
  final bool retryable;
  final Map<String, dynamic>? details;
  final ProtocolVersion protocol;

  factory IpcErrorEnvelope.fromJson(Map<String, dynamic> json) {
    final requestId = json['requestId'];
    final code = json['code'];
    final message = json['message'];
    final retryable = json['retryable'];
    final protocol = json['protocol'];
    final details = json['details'];
    if (json['kind'] != 'error' ||
        requestId is! String ||
        requestId.isEmpty ||
        code is! String ||
        code.isEmpty ||
        message is! String ||
        message.isEmpty ||
        retryable is! bool ||
        protocol is! Map<String, dynamic> ||
        (details != null && details is! Map<String, dynamic>)) {
      throw const FormatException('IPC error envelope is malformed');
    }
    return IpcErrorEnvelope(
      requestId: requestId,
      code: code,
      message: message,
      retryable: retryable,
      details: details as Map<String, dynamic>?,
      protocol: ProtocolVersion.fromJson(protocol),
    );
  }

  Map<String, dynamic> toJson() => {
    'kind': 'error',
    'protocol': protocol.toJson(),
    'requestId': requestId,
    'code': code,
    'message': message,
    'retryable': retryable,
    if (details != null) 'details': details,
  };
}

class IpcFrameCodec {
  const IpcFrameCodec({this.maximumBytes = defaultMaxMessageBytes});
  final int maximumBytes;

  Uint8List encodeJson(Map<String, dynamic> value) {
    final payload = Uint8List.fromList(utf8.encode(jsonEncode(value)));
    if (payload.length > maximumBytes) {
      throw FormatException('IPC message exceeds maximum size');
    }
    final frame = Uint8List(payload.length + 4);
    final view = ByteData.sublistView(frame);
    view.setUint32(0, payload.length, Endian.big);
    frame.setRange(4, frame.length, payload);
    return frame;
  }

  Map<String, dynamic> decodeJson(Uint8List frame) {
    if (frame.length < 4) throw const FormatException('IPC frame is too short');
    final declared = ByteData.sublistView(frame).getUint32(0, Endian.big);
    if (declared > maximumBytes || declared != frame.length - 4) {
      throw const FormatException('IPC frame length is invalid');
    }
    final payload = utf8.decode(frame.sublist(4));
    final decoded = jsonDecode(payload);
    if (decoded is! Map<String, dynamic>) {
      throw const FormatException('IPC payload must be a JSON object');
    }
    return decoded;
  }
}

class RuntimeHealth {
  const RuntimeHealth({this.status, this.safeDetails = const {}});

  final String? status;
  final Map<String, dynamic> safeDetails;

  static dynamic _sanitize(dynamic value) {
    if (value is Map<String, dynamic>) {
      final safe = <String, dynamic>{};
      for (final entry in value.entries) {
        final key = entry.key;
        final normalized = key
            .toLowerCase()
            .replaceAll('_', '')
            .replaceAll('-', '');
        if (normalized.contains('token') ||
            normalized.contains('apikey') ||
            normalized.contains('authorization') ||
            normalized.contains('cookie') ||
            normalized.contains('stdout') ||
            normalized.contains('stderr') ||
            normalized.contains('secret') ||
            normalized.contains('password') ||
            normalized.contains('credential') ||
            normalized == 'raw' ||
            normalized == 'rawcontent' ||
            normalized == 'content' ||
            normalized == 'text' ||
            normalized == 'provider') {
          continue;
        }
        safe[key] = _sanitize(entry.value);
      }
      return safe;
    } else if (value is List) {
      return value.map(_sanitize).toList();
    }
    return value;
  }

  factory RuntimeHealth.fromJson(Map<String, dynamic> json) {
    final status = json['status']?.toString();
    final safeDetails = _sanitize(json) as Map<String, dynamic>;
    safeDetails.remove('status');

    return RuntimeHealth(status: status, safeDetails: safeDetails);
  }
}
