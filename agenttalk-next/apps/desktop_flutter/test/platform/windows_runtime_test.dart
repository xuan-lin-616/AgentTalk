import 'dart:convert';
import 'dart:io';

import 'package:crypto/crypto.dart' as crypto;
import 'package:agenttalk_desktop/platform/windows_runtime.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  test('resolves a flat bundle from the executable directory', () async {
    final root = await Directory.systemTemp.createTemp('agenttalk-bundle-');
    addTearDown(() => root.delete(recursive: true));
    final app = File(
      '${root.path}${Platform.pathSeparator}agenttalk_desktop.exe',
    );
    final core = File(
      '${root.path}${Platform.pathSeparator}agenttalk-core.exe',
    );
    await app.writeAsString('app-${DateTime.now().microsecondsSinceEpoch}');
    await core.writeAsString('core');
    String hash(File file) =>
        crypto.sha256.convert(file.readAsBytesSync()).toString();
    await File(
      '${root.path}${Platform.pathSeparator}agenttalk-bundle.manifest.json',
    ).writeAsString(
      jsonEncode({
        'source': {'gitSha': 'edb48b38ad8fbbbde74e027f4d0b083974ac96de'},
        'files': {
          'agenttalk_desktop.exe': hash(app),
          'agenttalk-core.exe': hash(core),
        },
      }),
    );

    final localAppData = '${root.path}${Platform.pathSeparator}local-app-data';
    final resolved = await resolveWindowsRuntime(
      resolvedExecutable: app.path,
      environment: {'LOCALAPPDATA': localAppData},
    );

    expect(resolved.mode, CoreLaunchMode.owned);
    expect(resolved.coreExecutable, core.absolute.path);
    expect(
      resolved.databasePath,
      '$localAppData${Platform.pathSeparator}AgentTalk'
      '${Platform.pathSeparator}data${Platform.pathSeparator}agenttalk-core.sqlite3',
    );
    expect(
      resolved.artifactRoot,
      endsWith(
        'AgentTalk${Platform.pathSeparator}data${Platform.pathSeparator}artifacts',
      ),
    );
  });

  test(
    'does not use the current working directory for Core resolution',
    () async {
      final root = await Directory.systemTemp.createTemp('agenttalk-cwd-');
      addTearDown(() => root.delete(recursive: true));
      final app = File(
        '${root.path}${Platform.pathSeparator}agenttalk_desktop.exe',
      );
      final core = File(
        '${root.path}${Platform.pathSeparator}agenttalk-core.exe',
      );
      await app.writeAsString('app');
      await core.writeAsString('core');
      String hash(File file) =>
          crypto.sha256.convert(file.readAsBytesSync()).toString();
      await File(
        '${root.path}${Platform.pathSeparator}agenttalk-bundle.manifest.json',
      ).writeAsString(
        jsonEncode({
          'source': {'gitSha': 'same-source'},
          'files': {
            'agenttalk_desktop.exe': hash(app),
            'agenttalk-core.exe': hash(core),
          },
        }),
      );

      final resolved = await resolveWindowsRuntime(
        resolvedExecutable: app.path,
        environment: {'LOCALAPPDATA': root.path},
      );
      expect(resolved.coreExecutable, core.absolute.path);
    },
  );

  test(
    'fails closed when the bundle manifest or binary identity is invalid',
    () async {
      final root = await Directory.systemTemp.createTemp('agenttalk-invalid-');
      addTearDown(() => root.delete(recursive: true));
      final app = File(
        '${root.path}${Platform.pathSeparator}agenttalk_desktop.exe',
      );
      final core = File(
        '${root.path}${Platform.pathSeparator}agenttalk-core.exe',
      );
      await app.writeAsString('app');
      await core.writeAsString('core');

      await expectLater(
        resolveWindowsRuntime(
          resolvedExecutable: app.path,
          environment: {'LOCALAPPDATA': root.path},
        ),
        throwsA(isA<WindowsRuntimeResolutionException>()),
      );
    },
  );

  test(
    'external mode resolves without an executable or database path',
    () async {
      final resolved = await resolveWindowsRuntime(
        environment: {
          'AGENTTALK_CORE_MODE': 'external',
          'AGENTTALK_CORE_PIPE': r'\\.\pipe\agenttalk-external-test',
          'AGENTTALK_CORE_SESSION_CREDENTIAL': 'x' * 32,
        },
      );

      expect(resolved.mode, CoreLaunchMode.external);
      expect(resolved.ownsCore, isFalse);
      expect(resolved.coreExecutable, isNull);
      expect(resolved.databasePath, isNull);
    },
  );

  test('empty and owned Core modes resolve to owned', () async {
    final root = await Directory.systemTemp.createTemp('agenttalk-mode-owned-');
    addTearDown(() => root.delete(recursive: true));
    final core = File(
      '${root.path}${Platform.pathSeparator}agenttalk-core.exe',
    );
    await core.writeAsString('fixture-core');

    for (final mode in <String?>[null, '', '   ', 'owned', 'OWNED']) {
      final resolved = await resolveWindowsRuntime(
        explicitCoreExecutable: core.path,
        explicitDatabasePath:
            '${root.path}${Platform.pathSeparator}data.sqlite3',
        environment: {'AGENTTALK_CORE_MODE': ?mode},
      );
      expect(resolved.mode, CoreLaunchMode.owned, reason: 'mode=$mode');
      expect(resolved.ownsCore, isTrue, reason: 'mode=$mode');
    }
  });

  test('unknown Core modes fail closed before any Core launch', () async {
    final root = await Directory.systemTemp.createTemp(
      'agenttalk-mode-invalid-',
    );
    addTearDown(() => root.delete(recursive: true));
    final core = File(
      '${root.path}${Platform.pathSeparator}agenttalk-core.exe',
    );
    await core.writeAsString('fixture-core');

    for (final mode in <String>[
      'externaltypo',
      'owned-typo',
      'unknown',
      'production',
      'EXTERNAL_TEST',
    ]) {
      await expectLater(
        resolveWindowsRuntime(
          explicitCoreExecutable: core.path,
          explicitDatabasePath:
              '${root.path}${Platform.pathSeparator}data.sqlite3',
          environment: {'AGENTTALK_CORE_MODE': mode},
        ),
        throwsA(
          isA<WindowsRuntimeResolutionException>().having(
            (error) => error.message,
            'message',
            contains('Unknown AGENTTALK_CORE_MODE'),
          ),
        ),
      );
    }
  });
}
