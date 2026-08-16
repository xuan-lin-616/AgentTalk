import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:agenttalk_desktop/ipc/core_ipc_client.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  test(
    'external clients never own or request shutdown of a Core process',
    () async {
      var closeCalls = 0;
      var writeCalls = 0;
      final client = CoreIpcClient.forTesting(
        read: (_) => Uint8List(0),
        write: (_) => writeCalls += 1,
        close: () => closeCalls += 1,
        sessionCredential: 'x' * 32,
        sessionId: 'session-external-test-123456',
      );

      expect(client.ownsCoreProcess, isFalse);
      expect(client.ownedCoreProcessId, isNull);
      expect(await client.waitForOwnedCoreExit(), isTrue);
      await Future.wait<void>([client.close(), client.close()]);
      expect(closeCalls, 1);
      expect(writeCalls, 0);
    },
  );

  test(
    'owned close sends one shutdown and terminates only its owned process',
    () async {
      final process = await Process.start(
        r'C:\Windows\System32\timeout.exe',
        const ['/t', '60', '/nobreak'],
        runInShell: false,
      );
      var writeCalls = 0;
      var closeCalls = 0;
      final client = CoreIpcClient.forTesting(
        read: (_) async => Uint8List(0),
        write: (_) => writeCalls += 1,
        close: () => closeCalls += 1,
        sessionCredential: 'x' * 32,
        sessionId: 'session-owned-close-123456',
        ownedProcess: process,
      );

      await client.close().timeout(const Duration(seconds: 8));
      expect(writeCalls, 1);
      expect(closeCalls, 1);
      await process.exitCode.timeout(const Duration(seconds: 3));
      await client.close();
      expect(writeCalls, 1);
      expect(closeCalls, 1);
    },
    skip: !Platform.isWindows,
    timeout: const Timeout(Duration(seconds: 15)),
  );

  test(
    'Flutter Dart FFI client reaches the Rust Core host',
    () async {
      final coreEnv = Platform.environment['AGENTTALK_CORE_INTEGRATION_BINARY'];
      if (coreEnv == null || coreEnv.trim().isEmpty) {
        fail(
          'AGENTTALK_CORE_INTEGRATION_BINARY must point to the release '
          'agenttalk-core.exe for the FFI client contract.',
        );
      }
      final executable = coreEnv.trim();
      if (!File(executable).existsSync()) {
        fail('release Core must exist at AGENTTALK_CORE_INTEGRATION_BINARY');
      }
      if (!executable.toLowerCase().endsWith('.exe')) {
        fail('AGENTTALK_CORE_INTEGRATION_BINARY must name an .exe');
      }
      final temp = await Directory.systemTemp.createTemp(
        'agenttalk-core-dart-',
      );
      final pipe = r'\\.\pipe\agenttalk-core-dart-' + pid.toString();
      final database = '${temp.path}${Platform.pathSeparator}core.sqlite3';
      CoreIpcClient? client;
      addTearDown(() async {
        await client?.close().timeout(const Duration(seconds: 3));
        if (temp.existsSync()) await temp.delete(recursive: true);
      });

      client = await CoreIpcClient.startOwned(
        coreExecutable: executable,
        pipeName: pipe,
        databasePath: database,
        environmentOverrides: const {
          'AGENTTALK_CORE_RUNTIME': 'mock',
          'AGENTTALK_CORE_DEV_MODE': '1',
        },
      );

      expect(client.sessionCredential, isNotNull);
      expect(client.sessionCredential!.length, greaterThanOrEqualTo(32));
      final handshake = await client.handshake(
        sessionId: 'session-dart-test-123456',
      );
      expect(handshake['ok'], true);
      final health = await client.request({
        'kind': 'query',
        'protocol': {'major': 1, 'minor': 0},
        'requestId': 'health-dart',
        'sessionId': 'session-dart-test-123456',
        'query': 'runtime.health',
        'payload': {},
      });
      expect(health['requestId'], 'health-dart');
      expect(health['payload']['status'], 'ready');
      final snapshot = await client.request({
        'kind': 'query',
        'protocol': {'major': 1, 'minor': 0},
        'requestId': 'snapshot-dart',
        'sessionId': 'session-dart-test-123456',
        'query': 'projection.snapshot',
        'payload': {},
      });
      expect(snapshot['requestId'], 'snapshot-dart');
      expect(snapshot['payload'], isA<Map<String, dynamic>>());
      // execution.start requires a server-side persistent roster; this slice must not self-assign one.
    },
    skip: !Platform.isWindows,
    timeout: const Timeout(Duration(seconds: 20)),
  );

  test(
    'owned Core startup fails promptly when the child exits before binding',
    () async {
      const command = r'C:\Windows\System32\where.exe';
      final pipe = r'\\.\pipe\agenttalk-core-exit-' + pid.toString();
      CoreIpcException? failure;
      Object? actualError;
      try {
        await CoreIpcClient.startOwned(
          coreExecutable: command,
          pipeName: pipe,
          databasePath: 'unused.sqlite3',
        );
      } catch (error) {
        actualError = error;
        failure = error is CoreIpcException ? error : null;
      }
      expect(failure, isNotNull, reason: 'actual error: $actualError');
      expect(failure!.code, 'core_startup_failed');
      expect(failure.message, contains('Core 启动失败'));
      expect(failure.details?['technical'], isNotEmpty);
    },
    skip: !Platform.isWindows,
    timeout: const Timeout(Duration(seconds: 15)),
  );

  test(
    'owned Core startup cancellation cleans up before Named Pipe readiness',
    () async {
      final executable =
          Platform.environment['AGENTTALK_CORE_INTEGRATION_BINARY'];
      if (executable == null || executable.trim().isEmpty) {
        fail('AGENTTALK_CORE_INTEGRATION_BINARY must be set');
      }
      if (!File(executable).existsSync()) {
        fail('release Core must exist at AGENTTALK_CORE_INTEGRATION_BINARY');
      }
      var cancelled = false;
      Timer(const Duration(milliseconds: 100), () => cancelled = true);
      final state = await Directory.systemTemp.createTemp(
        'agenttalk-core-start-cancel-',
      );
      addTearDown(() async {
        if (state.existsSync()) await state.delete(recursive: true);
      });

      await expectLater(
        CoreIpcClient.startOwned(
          coreExecutable: executable,
          pipeName: r'\\.\pipe\agenttalk-core-start-cancel-' + pid.toString(),
          databasePath: '${state.path}${Platform.pathSeparator}core.sqlite3',
          environmentOverrides: const {
            'AGENTTALK_CORE_RUNTIME': 'mock',
            'AGENTTALK_CORE_DEV_MODE': '1',
            'AGENTTALK_CORE_STARTUP_DELAY_MS': '3000',
          },
          isCancelled: () => cancelled,
        ),
        throwsA(
          isA<CoreIpcException>().having(
            (error) => error.code,
            'code',
            'CLIENT_CLOSED',
          ),
        ),
      );
    },
    skip: !Platform.isWindows,
    timeout: const Timeout(Duration(seconds: 15)),
  );

  test(
    'owned Core startup preserves a bounded categorized stderr diagnostic',
    () async {
      final executable =
          Platform.environment['AGENTTALK_CORE_STARTUP_DIAGNOSTIC_BINARY'];
      final database =
          Platform.environment['AGENTTALK_CORE_STARTUP_DIAGNOSTIC_DATABASE'];
      if (executable == null ||
          database == null ||
          executable.trim().isEmpty ||
          database.trim().isEmpty ||
          !File(executable).existsSync()) {
        markTestSkipped(
          'AGENTTALK_CORE_STARTUP_DIAGNOSTIC_BINARY and '
          'AGENTTALK_CORE_STARTUP_DIAGNOSTIC_DATABASE must be set to a '
          'release Core and an incompatible database.',
        );
        return;
      }
      final failure =
          await CoreIpcClient.startOwned(
            coreExecutable: executable,
            pipeName: r'\\.\pipe\agenttalk-core-diagnostic-' + pid.toString(),
            databasePath: database,
          ).then<CoreIpcException?>(
            (_) => null,
            onError: (Object error, StackTrace _) =>
                error is CoreIpcException ? error : throw error,
          );
      expect(failure, isNotNull);
      expect(failure!.code, 'database_schema_incompatible');
      expect(failure.details?['stage'], 'schema_migration_preflight');
      expect(
        failure.details?['technical'],
        contains('migration checksum mismatch'),
      );
      expect(failure.message, contains('数据库版本不兼容'));
    },
    skip: !Platform.isWindows,
    timeout: const Timeout(Duration(seconds: 15)),
  );
}
