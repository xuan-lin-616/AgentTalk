import 'dart:async';
import 'dart:io';

import 'package:flutter/services.dart';

const appLifecycleChannelName = 'agenttalk/app_lifecycle';
const _testHangCloseEnvironment = 'AGENTTALK_TEST_HANG_CLOSE';

final MethodChannel _appLifecycleChannel = const MethodChannel(
  appLifecycleChannelName,
);

bool closeHandlerHangFixtureEnabled({Map<String, String>? environment}) {
  final env = environment ?? Platform.environment;
  return env['AGENTTALK_CORE_DEV_MODE'] == '1' &&
      env[_testHangCloseEnvironment] == '1';
}

Future<void> registerOwnedCoreProcess(int processId) async {
  if (processId <= 0) {
    throw ArgumentError.value(processId, 'processId', 'must be positive');
  }
  await _appLifecycleChannel.invokeMethod<void>('registerOwnedCore', processId);
}

Future<void> installAppLifecycleHandler({
  required Future<void> Function() onCloseRequested,
}) async {
  _appLifecycleChannel.setMethodCallHandler((call) async {
    if (call.method != 'requestClose') {
      throw MissingPluginException(
        'Unsupported lifecycle method ${call.method}',
      );
    }
    try {
      if (closeHandlerHangFixtureEnabled()) {
        // This is a development/test-only fixture. It deliberately never
        // returns so the Windows runner's bounded native fallback can be
        // verified without touching production data or configuration.
        await Completer<void>().future;
      }
      await onCloseRequested();
    } finally {
      // The Windows runner destroys the window only after owned Core cleanup
      // has completed. This also makes external Core mode a no-op for Core
      // process ownership: the Dart client has no owned process to stop.
      await _appLifecycleChannel.invokeMethod<void>('closeCompleted');
    }
  });
}
