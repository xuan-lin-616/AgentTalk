import 'package:agenttalk_desktop/platform/app_lifecycle.dart';
import 'package:flutter/services.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  TestWidgetsFlutterBinding.ensureInitialized();

  test('close-handler hang fixture requires development mode', () {
    expect(
      closeHandlerHangFixtureEnabled(
        environment: const {'AGENTTALK_TEST_HANG_CLOSE': '1'},
      ),
      isFalse,
    );
    expect(
      closeHandlerHangFixtureEnabled(
        environment: const {
          'AGENTTALK_CORE_DEV_MODE': '1',
          'AGENTTALK_TEST_HANG_CLOSE': '1',
        },
      ),
      isTrue,
    );
    expect(
      closeHandlerHangFixtureEnabled(
        environment: const {'AGENTTALK_CORE_DEV_MODE': '1'},
      ),
      isFalse,
    );
  });

  test(
    'owned Core registration forwards only the PID to Native runner',
    () async {
      final calls = <MethodCall>[];
      final messenger =
          TestDefaultBinaryMessengerBinding.instance.defaultBinaryMessenger;
      final channel = const MethodChannel(appLifecycleChannelName);
      messenger.setMockMethodCallHandler(channel, (call) async {
        calls.add(call);
        return null;
      });
      addTearDown(() => messenger.setMockMethodCallHandler(channel, null));

      await registerOwnedCoreProcess(1234);

      expect(calls, hasLength(1));
      expect(calls.single.method, 'registerOwnedCore');
      expect(calls.single.arguments, 1234);
    },
  );

  test(
    'owned Core registration rejects invalid PIDs before invoking Native runner',
    () async {
      await expectLater(
        registerOwnedCoreProcess(0),
        throwsA(isA<ArgumentError>()),
      );
    },
  );
}
