import 'dart:convert';
import 'dart:io';

import 'package:crypto/crypto.dart' as crypto;

enum CoreLaunchMode { owned, external }

class WindowsRuntimeResolutionException implements Exception {
  const WindowsRuntimeResolutionException(this.message);

  final String message;

  @override
  String toString() => message;
}

class WindowsRuntimeResolution {
  const WindowsRuntimeResolution._({
    required this.mode,
    required this.coreExecutable,
    required this.databasePath,
    required this.artifactRoot,
    required this.externalPipe,
    required this.externalSessionCredential,
    required this.bundleSourceSha,
  });

  const WindowsRuntimeResolution.owned({
    required String coreExecutable,
    required String databasePath,
    required String artifactRoot,
    String? bundleSourceSha,
  }) : this._(
         mode: CoreLaunchMode.owned,
         coreExecutable: coreExecutable,
         databasePath: databasePath,
         artifactRoot: artifactRoot,
         externalPipe: null,
         externalSessionCredential: null,
         bundleSourceSha: bundleSourceSha,
       );

  const WindowsRuntimeResolution.external({
    required String pipe,
    required String sessionCredential,
  }) : this._(
         mode: CoreLaunchMode.external,
         coreExecutable: null,
         databasePath: null,
         artifactRoot: null,
         externalPipe: pipe,
         externalSessionCredential: sessionCredential,
         bundleSourceSha: null,
       );

  final CoreLaunchMode mode;
  final String? coreExecutable;
  final String? databasePath;
  final String? artifactRoot;
  final String? externalPipe;
  final String? externalSessionCredential;
  final String? bundleSourceSha;

  bool get ownsCore => mode == CoreLaunchMode.owned;
}

String _joinPath(String first, String second) =>
    '$first${Platform.pathSeparator}$second';

String _fullPath(String value) => File(value).absolute.path;

String _bundleRoot({String? resolvedExecutable}) {
  final executable = resolvedExecutable ?? Platform.resolvedExecutable;
  return File(executable).absolute.parent.path;
}

String _requireEnvironment(
  Map<String, String> environment,
  String key,
  String message,
) {
  final value = environment[key]?.trim();
  if (value == null || value.isEmpty) {
    throw WindowsRuntimeResolutionException(message);
  }
  return value;
}

Future<WindowsRuntimeResolution> resolveWindowsRuntime({
  String? explicitCoreExecutable,
  String? explicitDatabasePath,
  Map<String, String>? environment,
  String? resolvedExecutable,
  bool isWindows = true,
}) async {
  if (!isWindows) {
    throw const WindowsRuntimeResolutionException(
      'AgentTalk Windows runtime is unavailable on this platform.',
    );
  }
  final env = environment ?? Platform.environment;
  final configuredMode = env['AGENTTALK_CORE_MODE']?.trim().toLowerCase();
  if (configuredMode != null &&
      configuredMode.isNotEmpty &&
      configuredMode != 'owned' &&
      configuredMode != 'external') {
    throw WindowsRuntimeResolutionException(
      'Unknown AGENTTALK_CORE_MODE value: ${env['AGENTTALK_CORE_MODE']}. Use owned or external.',
    );
  }
  if (configuredMode == 'external') {
    final pipe = _requireEnvironment(
      env,
      'AGENTTALK_CORE_PIPE',
      'External Core mode requires AGENTTALK_CORE_PIPE.',
    );
    final credential = _requireEnvironment(
      env,
      'AGENTTALK_CORE_SESSION_CREDENTIAL',
      'External Core mode requires AGENTTALK_CORE_SESSION_CREDENTIAL.',
    );
    return WindowsRuntimeResolution.external(
      pipe: pipe,
      sessionCredential: credential,
    );
  }

  final coreOverride =
      explicitCoreExecutable ?? env['AGENTTALK_CORE_EXECUTABLE'];
  final explicitCore = coreOverride?.trim();
  final hasExplicitCore = explicitCore?.isNotEmpty == true;
  final root = _bundleRoot(resolvedExecutable: resolvedExecutable);
  final selectedCore = hasExplicitCore
      ? explicitCore
      : _joinPath(root, 'agenttalk-core.exe');
  final corePath = _fullPath(
    selectedCore ?? _joinPath(root, 'agenttalk-core.exe'),
  );
  if (!File(corePath).existsSync()) {
    throw WindowsRuntimeResolutionException(
      hasExplicitCore
          ? 'Configured AgentTalk Core does not exist: $corePath'
          : 'AgentTalk bundle is incomplete: agenttalk-core.exe is missing next to the desktop executable.',
    );
  }

  String databasePath;
  final databaseOverride =
      explicitDatabasePath ?? env['AGENTTALK_CORE_DATABASE'];
  if (databaseOverride != null && databaseOverride.trim().isNotEmpty) {
    databasePath = _fullPath(databaseOverride);
  } else {
    final dataRootOverride = env['AGENTTALK_DATA_ROOT']?.trim();
    final dataRoot = dataRootOverride == null || dataRootOverride.isEmpty
        ? (() {
            final localAppData = env['LOCALAPPDATA']?.trim();
            if (localAppData == null || localAppData.isEmpty) {
              throw const WindowsRuntimeResolutionException(
                'Windows LocalAppData is unavailable; set AGENTTALK_DATA_ROOT only for an explicit development/test run.',
              );
            }
            return _joinPath(_joinPath(localAppData, 'AgentTalk'), 'data');
          })()
        : _fullPath(dataRootOverride);
    databasePath = _joinPath(dataRoot, 'agenttalk-core.sqlite3');
  }

  final databaseDirectory = File(databasePath).absolute.parent.path;
  final artifactRoot = _fullPath(
    env['AGENTTALK_CORE_ARTIFACT_ROOT']?.trim().isNotEmpty == true
        ? env['AGENTTALK_CORE_ARTIFACT_ROOT']!.trim()
        : _joinPath(databaseDirectory, 'artifacts'),
  );

  String? bundleSourceSha;
  if (!hasExplicitCore) {
    final manifestPath = _joinPath(root, 'agenttalk-bundle.manifest.json');
    final manifest = File(manifestPath);
    if (!manifest.existsSync()) {
      throw const WindowsRuntimeResolutionException(
        'AgentTalk bundle manifest is missing; refusing to launch an unverified Core.',
      );
    }
    final decoded = jsonDecode(await manifest.readAsString());
    if (decoded is! Map<String, dynamic>) {
      throw const WindowsRuntimeResolutionException(
        'AgentTalk bundle manifest is invalid; refusing to launch.',
      );
    }
    final source = decoded['source'];
    final files = decoded['files'];
    if (source is! Map<String, dynamic> ||
        source['gitSha'] is! String ||
        (source['gitSha'] as String).trim().isEmpty ||
        files is! Map<String, dynamic>) {
      throw const WindowsRuntimeResolutionException(
        'AgentTalk bundle manifest has no source identity or file hashes.',
      );
    }
    bundleSourceSha = (source['gitSha'] as String).trim();
    final expectedAppHash = files['agenttalk_desktop.exe'];
    final expectedCoreHash = files['agenttalk-core.exe'];
    if (expectedAppHash is! String || expectedCoreHash is! String) {
      throw const WindowsRuntimeResolutionException(
        'AgentTalk bundle manifest does not identify the desktop and Core binaries.',
      );
    }
    final actualAppHash = _sha256File(
      File(resolvedExecutable ?? Platform.resolvedExecutable),
    );
    final actualCoreHash = _sha256File(File(corePath));
    if (actualAppHash != expectedAppHash.toLowerCase() ||
        actualCoreHash != expectedCoreHash.toLowerCase()) {
      throw const WindowsRuntimeResolutionException(
        'AgentTalk bundle binary hash mismatch; refusing to launch a mismatched Core.',
      );
    }
  }

  return WindowsRuntimeResolution.owned(
    coreExecutable: corePath,
    databasePath: databasePath,
    artifactRoot: artifactRoot,
    bundleSourceSha: bundleSourceSha,
  );
}

String _sha256File(File file) {
  if (!file.existsSync()) {
    throw WindowsRuntimeResolutionException(
      'AgentTalk runtime binary is missing: ${file.path}',
    );
  }
  return crypto.sha256.convert(file.readAsBytesSync()).toString();
}
