import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';

import 'package:crypto/crypto.dart' as crypto;
import 'package:flutter/material.dart';

import 'gen/l10n.dart';
import 'ipc/core_ipc_client.dart';
import 'ipc/protocol_v1.dart';
import 'platform/app_lifecycle.dart';
import 'platform/folder_picker.dart';
import 'platform/windows_runtime.dart';
import 'ui/agent_identity_dialog.dart';
import 'ui/conversation_agent_assignment_panel.dart';
import 'ui/context_inspector_drawer.dart';
import 'ui/config_transfer_dialog.dart';
import 'ui/connector_center_dialog.dart';
import 'ui/diagnostics_metadata_panel.dart';
import 'ui/event_recovery_banner.dart';
import 'ui/message_search_dialog.dart';
import 'ui/memory_write_dialog.dart';
import 'ui/local_agent_scan_dialog.dart';
import 'ui/project_agent_assignment_panel.dart';
import 'ui/projection_entity_dialog.dart';
import 'ui/theme/studio_colors.dart';
import 'ui/retrieval_source_write_dialog.dart';
import 'ui/retrieval_selection_dialog.dart';
import 'ui/retrieval_preview_dialog.dart';
import 'ui/workflow_create_dialog.dart';
import 'ui/orchestration_panel.dart';

final _workspaceShellKey = GlobalKey<WorkspaceShellState>();

Future<void> main() async {
  WidgetsFlutterBinding.ensureInitialized();
  await installAppLifecycleHandler(
    onCloseRequested: () async {
      await _workspaceShellKey.currentState?.closeForApplication();
    },
  );
  runApp(AgentTalkDesktopApp(shellKey: _workspaceShellKey));
}

class AgentTalkDesktopApp extends StatefulWidget {
  const AgentTalkDesktopApp({
    super.key,
    this.shellKey,
    this.initialClient,
    this.initialSessionId,
    this.initialSnapshot,
    this.enableEventPolling = true,
  });

  final GlobalKey<WorkspaceShellState>? shellKey;
  final CoreIpcClient? initialClient;
  final String? initialSessionId;
  final Map<String, dynamic>? initialSnapshot;
  final bool enableEventPolling;

  @override
  State<AgentTalkDesktopApp> createState() => _AgentTalkDesktopAppState();
}

class _AgentTalkDesktopAppState extends State<AgentTalkDesktopApp> {
  ThemeMode _themeMode = _initialThemeMode();

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      onGenerateTitle: (context) => _appTitle(context),
      debugShowCheckedModeBanner: false,
      localizationsDelegates: AppLocalizations.localizationsDelegates,
      supportedLocales: AppLocalizations.supportedLocales,
      locale: const Locale('zh'),
      theme: buildStudioTheme(studioLightColorScheme),
      darkTheme: buildStudioTheme(studioDarkColorScheme),
      themeMode: _themeMode,
      home: WorkspaceShell(
        key: widget.shellKey,
        initialClient: widget.initialClient,
        initialSessionId: widget.initialSessionId,
        initialSnapshot: widget.initialSnapshot,
        enableEventPolling: widget.enableEventPolling,
        onToggleTheme: () => setState(() {
          _themeMode = _themeMode == ThemeMode.light
              ? ThemeMode.dark
              : ThemeMode.light;
        }),
      ),
    );
  }
}

ThemeMode _initialThemeMode() {
  final value = Platform.environment['AGENTTALK_INITIAL_THEME']
      ?.trim()
      .toLowerCase();
  return switch (value) {
    'dark' => ThemeMode.dark,
    'system' => ThemeMode.system,
    _ => ThemeMode.dark,
  };
}

class WorkspaceShell extends StatefulWidget {
  const WorkspaceShell({
    super.key,
    this.coreExecutable,
    this.databasePath,
    this.onToggleTheme,
    this.initialClient,
    this.initialSessionId,
    this.initialSnapshot,
    this.enableEventPolling = true,
    this.filePickerClient,
  });

  final String? coreExecutable;
  final String? databasePath;
  final VoidCallback? onToggleTheme;

  /// Supplies a connected client and projection for deterministic widget tests.
  final CoreIpcClient? initialClient;
  final String? initialSessionId;
  final Map<String, dynamic>? initialSnapshot;
  final bool enableEventPolling;
  final FilePickerClient? filePickerClient;

  @override
  State<WorkspaceShell> createState() => WorkspaceShellState();
}

class WorkspaceShellState extends State<WorkspaceShell> {
  CoreIpcClient? _client;
  CoreIpcClient? _eventClient;
  StreamSubscription<EventEnvelope>? _eventSubscriptionListener;
  Timer? _eventTimer;
  String? _sessionId;
  int _eventCursor = 0;
  bool _pollInFlight = false;
  bool _eventRecoveryBusy = false;
  bool _closing = false;
  Future<void>? _closeForApplicationFuture;
  Future<void>? _projectionLoadFuture;
  ReplayGapDetails? _eventRecovery;
  String? _eventRecoveryError;
  String? _activeProjectId;
  String? _activeConversationId;
  String? _activeSubscriptionId;
  String? _activeRetrievalSelectionId;
  double _leftPaneWidth = 266;
  double _rightPaneWidth = 336;
  bool _leftPaneVisible = true;
  bool _rightPaneVisible = true;
  Map<String, dynamic> _snapshot = const <String, dynamic>{};
  String _projectionStatus = '正在连接 AgentTalk Core';
  String? _coreDiagnosticDetails;

  final List<EventEnvelope> _eventQueue = [];
  bool _eventQueueProcessing = false;

  Future<void> _processEventQueue() async {
    if (_eventQueueProcessing || _eventQueue.isEmpty) return;
    _eventQueueProcessing = true;
    try {
      while (_eventQueue.isNotEmpty) {
        if (!mounted || _eventRecovery != null) {
          _eventQueue.clear();
          break;
        }
        final event = _eventQueue.first;
        await _handleEventSequence(event);
        if (_eventQueue.isNotEmpty && _eventQueue.first == event) {
          _eventQueue.removeAt(0);
        }
      }
    } finally {
      if (mounted) {
        setState(() {
          _eventQueueProcessing = false;
        });
      }
    }
  }

  Future<void> _handleEventSequence(EventEnvelope event) async {
    final client = _eventClient ?? _client;
    final sessionId = _sessionId;
    if (client == null || sessionId == null) return;

    try {
      if (event.event == 'projection.changed' ||
          event.event.startsWith('execution.')) {
        await _refreshProjection(client, sessionId);
      }

      final subscriptionId = _activeSubscriptionId;
      if (subscriptionId != null) {
        await client.ackEvents(
          subscriptionId: subscriptionId,
          cursor: event.cursor,
        );
        if (mounted) {
          setState(() {
            if (event.cursor.sequence > _eventCursor) {
              _eventCursor = event.cursor.sequence;
            }
          });
        }
      }
    } on Object catch (error) {
      if (mounted) {
        setState(() {
          _eventRecoveryError = _describeCoreError(error);
          _projectionStatus = '事件流异常：${_describeCoreError(error)}';
          if (error is CoreIpcException) {
            if (error.code == 'REPLAY_GAP' ||
                error.code == 'SUBSCRIPTION_OVERFLOW' ||
                error.code == 'CURSOR_EPOCH_MISMATCH' ||
                error.details?['requiresSnapshot'] == true) {
              _eventRecovery =
                  ReplayGapDetails.tryParse(error.details ?? {}) ??
                  ReplayGapDetails.fallback(epoch: client.serverEpoch);
            } else {
              _eventRecovery = ReplayGapDetails.fallback(
                epoch: client.serverEpoch,
              );
            }
          } else {
            _eventRecovery = ReplayGapDetails.fallback(
              epoch: client.serverEpoch,
            );
          }
        });
      }

      await _eventSubscriptionListener?.cancel();
      await _eventClient?.close();
      _activeSubscriptionId = null;
      _eventQueue.clear();
    }
  }

  @override
  void initState() {
    super.initState();
    final initialClient = widget.initialClient;
    if (initialClient != null) {
      _client = initialClient;
      _sessionId = widget.initialSessionId;
      _snapshot = widget.initialSnapshot ?? const <String, dynamic>{};
      final projects = _list(_snapshot, 'projects');
      final conversations = _list(_snapshot, 'conversations');
      _activeProjectId = projects.isEmpty
          ? null
          : projects.first['id']?.toString();
      _activeConversationId = conversations.isEmpty
          ? null
          : conversations.first['id']?.toString();
      _projectionStatus = '已连接本地 Core';
      if (widget.enableEventPolling) _startEventPolling();
      return;
    }
    final projectionLoad = _loadProjection();
    _projectionLoadFuture = projectionLoad;
    unawaited(
      projectionLoad.whenComplete(() {
        if (identical(_projectionLoadFuture, projectionLoad)) {
          _projectionLoadFuture = null;
        }
      }),
    );
  }

  Future<void> _loadProjection() async {
    if (!Platform.isWindows) return;
    final pipe = StringBuffer(r'\\.\pipe\agenttalk-core-flutter-')
      ..write(pid)
      ..write('-')
      ..write(Random.secure().nextInt(1 << 32));
    CoreIpcClient? client;
    try {
      late final WindowsRuntimeResolution runtime;
      try {
        runtime = await resolveWindowsRuntime(
          explicitCoreExecutable: widget.coreExecutable,
          explicitDatabasePath: widget.databasePath,
        );
      } catch (error) {
        throw CoreIpcException(
          'AgentTalk Core 启动配置不可用，请检查运行环境后重试。',
          code: 'runtime_configuration_unavailable',
          retryable: true,
          details: <String, dynamic>{
            'category': 'runtime_configuration_unavailable',
            'stage': 'environment_parameters',
            'technical': error.toString(),
          },
        );
      }
      if (_closing) return;
      client = runtime.mode == CoreLaunchMode.external
          ? await CoreIpcClient.connectExternal(
              pipeName: runtime.externalPipe!,
              sessionCredential: runtime.externalSessionCredential!,
              isCancelled: () => _closing,
            )
          : await CoreIpcClient.startOwned(
              coreExecutable: runtime.coreExecutable!,
              pipeName: pipe.toString(),
              databasePath: runtime.databasePath!,
              artifactRoot: runtime.artifactRoot,
              isCancelled: () => _closing,
            );
      if (client.ownsCoreProcess) {
        final ownedCorePid = client.ownedCoreProcessId;
        if (ownedCorePid == null) {
          throw const CoreIpcException(
            'Owned AgentTalk Core did not expose a process identity',
          );
        }
        // Register immediately after Process.start/connect succeeds so the
        // native WM_CLOSE fallback can clean up this exact child if Dart
        // shutdown is blocked.
        try {
          await registerOwnedCoreProcess(ownedCorePid);
        } catch (error) {
          throw CoreIpcException(
            'AgentTalk Core 进程托管失败，请重试启动。',
            code: 'job_object_registration_failed',
            retryable: true,
            details: <String, dynamic>{
              'category': 'job_object_registration_failed',
              'stage': 'job_object_registration',
              'technical': error.toString(),
            },
          );
        }
      }
      _client = client;
      if (_closing) {
        _client = null;
        await client.close();
        client = null;
        return;
      }
      final sessionId =
          'flutter-session-$pid-${DateTime.now().microsecondsSinceEpoch}';
      await client.handshake(sessionId: sessionId);
      final response = await client.request({
        'kind': 'query',
        'protocol': {'major': protocolMajor, 'minor': 0},
        'requestId': 'projection-$pid',
        'sessionId': sessionId,
        'query': 'projection.snapshot',
        'payload': <String, dynamic>{},
      });
      final payload = response['payload'];
      if (payload is! Map<String, dynamic>) {
        throw const CoreIpcException('Core projection payload is invalid');
      }
      if (!mounted) {
        if (identical(_client, client)) _client = null;
        await client.close();
        client = null;
        return;
      }
      if (_closing) {
        if (identical(_client, client)) _client = null;
        await client.close();
        client = null;
        return;
      }
      client = null;
      _sessionId = sessionId;
      final projects = _list(payload, 'projects');
      final conversations = _list(payload, 'conversations');
      _activeProjectId ??= projects.isEmpty
          ? null
          : projects.first['id']?.toString();
      _activeConversationId ??= conversations.isEmpty
          ? null
          : conversations.first['id']?.toString();
      setState(() {
        _snapshot = payload;
        _projectionStatus = '已连接本地 Core';
        _coreDiagnosticDetails = null;
      });
      _startEventPolling();
    } catch (error) {
      final failedClient = client;
      client = null;
      if (failedClient != null) {
        if (identical(_client, failedClient)) _client = null;
        await failedClient.close();
      }
      if (mounted && !_closing) {
        setState(() {
          _projectionStatus = '本地 Core 暂不可用：${_describeCoreError(error)}';
          _coreDiagnosticDetails = _diagnosticDetailsFor(error);
        });
      }
    }
  }

  Future<void> _retryProjection() async {
    if (_closing || _projectionLoadFuture != null) return;
    if (mounted) {
      setState(() {
        _projectionStatus = '正在启动 Core…';
        _coreDiagnosticDetails = null;
      });
    }
    final future = _loadProjection();
    _projectionLoadFuture = future;
    try {
      await future;
    } finally {
      if (identical(_projectionLoadFuture, future)) {
        _projectionLoadFuture = null;
      }
    }
  }

  @override
  void dispose() {
    unawaited(closeForApplication());
    super.dispose();
  }

  Future<void> closeForApplication() {
    final existing = _closeForApplicationFuture;
    if (existing != null) return existing;
    _closing = true;
    _eventTimer?.cancel();
    final future = () async {
      try {
        await _eventSubscriptionListener?.cancel().timeout(
          const Duration(seconds: 1),
        );
      } catch (_) {}
      _eventSubscriptionListener = null;
      final eventClient = _eventClient;
      final client = _client;
      _eventClient = null;
      _activeSubscriptionId = null;
      _eventQueue.clear();
      _client = null;
      final closeOperations = <Future<void>>[
        if (eventClient != null) eventClient.close(),
        if (client != null && !identical(client, eventClient)) client.close(),
      ];
      await Future.wait<void>(
        closeOperations.map((operation) async {
          try {
            await operation;
          } catch (_) {}
        }),
      );
      final projectionLoad = _projectionLoadFuture;
      if (projectionLoad != null) {
        try {
          await projectionLoad.timeout(const Duration(seconds: 3));
        } catch (_) {}
      }
    }().timeout(const Duration(seconds: 9), onTimeout: () {});
    _closeForApplicationFuture = future;
    return future;
  }

  void _startEventPolling() {
    _eventTimer?.cancel();
    _eventTimer = Timer.periodic(
      const Duration(seconds: 2),
      (_) => unawaited(_pollCoreEvents()),
    );
  }

  Future<void> _pollCoreEvents() async {
    final client = _client;
    final sessionId = _sessionId;
    if (client == null ||
        sessionId == null ||
        !mounted ||
        _eventRecovery != null ||
        _pollInFlight) {
      return;
    }
    _pollInFlight = true;
    try {
      final events = await client.replayEvents(
        sessionId: sessionId,
        afterSequence: _eventCursor,
      );
      var projectionChanged = false;
      for (final event in events) {
        final cursor = event['cursor'];
        final sequence = cursor is Map<String, dynamic>
            ? cursor['sequence']
            : event['sequence'];
        if (sequence is int && sequence > _eventCursor) {
          _eventCursor = sequence;
        }
        if (event['event'] == 'projection.changed') {
          projectionChanged = true;
        }
      }
      if (projectionChanged) await _refreshProjection(client, sessionId);
    } on CoreIpcException catch (error) {
      if (error.isReplayGap) {
        if (mounted) {
          setState(() {
            _eventRecovery =
                error.replayGap ??
                ReplayGapDetails.fallback(epoch: client.serverEpoch);
            _eventRecoveryError = error.message;
            _projectionStatus = '事件流需要重新获取快照后恢复';
          });
        }
        return;
      }
      try {
        final lastSeen = client.serverEpoch == null
            ? null
            : StreamCursor(
                streamId: 'core-events',
                sequence: _eventCursor,
                epoch: client.serverEpoch,
              );
        await client.reconnect(sessionId: sessionId, lastSeen: lastSeen);
        if (mounted) {
          setState(() => _projectionStatus = '已重新连接本地 Core');
        }
      } on Object catch (reconnectError) {
        if (client.serverEpoch != null) {
          try {
            await client.reconnect(sessionId: sessionId);
            _eventCursor = 0;
            await _refreshProjection(client, sessionId);
            if (mounted) {
              setState(() => _projectionStatus = '本地 Core 已重新连接并刷新快照');
            }
            return;
          } on Object catch (restartError) {
            // Fall through to the explicit pending state below.
            if (mounted) {
              setState(
                () => _projectionStatus =
                    '事件流异常：${_describeCoreError(restartError)}',
              );
            }
          }
        }
        if (!mounted) return;
        setState(
          () => _projectionStatus =
              '事件流正在等待重连：${_describeCoreError(reconnectError)}',
        );
      }
    } on Object catch (error) {
      if (mounted) {
        setState(
          () => _projectionStatus = '事件流异常：${_describeCoreError(error)}',
        );
      }
    } finally {
      _pollInFlight = false;
    }
  }

  StreamCursor? _recoveryCursor(ReplayGapDetails? details) {
    final serverEpoch = _client?.serverEpoch;
    final resumeCursor = details?.resumeCursor;
    if (resumeCursor != null &&
        (serverEpoch == null || resumeCursor.epoch == serverEpoch)) {
      return resumeCursor;
    }
    final headCursor = details?.headCursor;
    if (headCursor != null && serverEpoch != null) {
      return StreamCursor(
        streamId: headCursor.streamId,
        sequence: headCursor.sequence,
        epoch: serverEpoch,
      );
    }
    return null;
  }

  Future<void> _recoverEventStream({required bool subscribe}) async {
    if (_eventRecoveryBusy || _eventRecovery == null) return;
    final client = _client;
    final sessionId = _sessionId;
    if (client == null || sessionId == null) return;
    final details = _eventRecovery;
    setState(() {
      _eventRecoveryBusy = true;
      _eventRecoveryError = null;
      _projectionStatus = '正在刷新快照…';
    });
    CoreIpcClient? replacement;
    try {
      // The snapshot is the only state accepted after a bounded replay gap.
      await _refreshProjection(client, sessionId);
      final cursor = _recoveryCursor(details);
      if (subscribe) {
        if (cursor == null) {
          throw const CoreIpcException(
            'Cannot subscribe without a valid recovery cursor',
          );
        }
        replacement = await client.openSubscription(
          sessionId: sessionId,
          lastSeen: cursor,
        );
        final subscription = await replacement.subscribeEvents(
          sessionId: sessionId,
          afterCursor: cursor,
        );
        final listener = subscription.events.listen(
          (envelope) {
            if (!mounted) return;
            _eventQueue.add(envelope);
            unawaited(_processEventQueue());
          },
          onError: (Object error, StackTrace stackTrace) async {
            if (!mounted) return;
            await _eventSubscriptionListener?.cancel();
            await _eventClient?.close();
            _activeSubscriptionId = null;
            _eventQueue.clear();
            if (mounted) {
              setState(() {
                _eventRecoveryError = '$error';
                _projectionStatus = '事件订阅失败，已停止应用事件';
                _eventRecovery = details;
              });
            }
          },
        );
        await _eventSubscriptionListener?.cancel();
        await _eventClient?.close();
        _eventClient = replacement;
        _eventSubscriptionListener = listener;
        _activeSubscriptionId = subscription.subscriptionId;
        replacement = null;
        _eventTimer?.cancel();
        if (mounted) {
          setState(() {
            _eventRecovery = null;
            _eventRecoveryError = null;
            _projectionStatus = '事件订阅已恢复，序号 ${cursor.sequence}';
          });
        }
      } else {
        _eventCursor = details?.headCursor?.sequence ?? _eventCursor;
        _startEventPolling();
        if (mounted) {
          setState(() {
            _eventRecovery = null;
            _eventRecoveryError = null;
            _projectionStatus = '快照已刷新，事件流已切回轮询';
          });
        }
      }
    } on Object catch (error) {
      await replacement?.close();
      if (mounted) {
        setState(() {
          _eventRecoveryError = _describeCoreError(error);
          _projectionStatus = '事件恢复失败，已暂停应用事件';
        });
      }
    } finally {
      if (mounted) setState(() => _eventRecoveryBusy = false);
    }
  }

  String _describeCoreError(Object error) {
    if (error is CoreIpcException) return error.message;
    if (error is WindowsRuntimeResolutionException) {
      return 'AgentTalk Core 启动配置不可用，请检查运行环境后重试。';
    }
    return 'AgentTalk Core 启动失败，请打开诊断查看详情。';
  }

  String _diagnosticDetailsFor(Object error) {
    if (error is CoreIpcException) {
      final technical = error.details?['technical'];
      if (technical is String && technical.trim().isNotEmpty) {
        return _redactStartupDetails(technical);
      }
      final category = error.code ?? 'core_startup_failed';
      final stage = error.details?['stage'] ?? 'unknown';
      return 'category=$category stage=$stage';
    }
    return _redactStartupDetails(error.toString());
  }

  Future<void> _refreshProjection(
    CoreIpcClient client,
    String sessionId,
  ) async {
    final response = await client.request({
      'kind': 'query',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId':
          'projection-refresh-${DateTime.now().microsecondsSinceEpoch}',
      'sessionId': sessionId,
      'query': 'projection.snapshot',
      'payload': <String, dynamic>{},
    });
    final payload = response['payload'];
    if (payload is! Map<String, dynamic> || !mounted) return;
    final conversations = _list(payload, 'conversations');
    final projects = _list(payload, 'projects');
    if (_activeProjectId == null && projects.isNotEmpty) {
      _activeProjectId = projects.first['id']?.toString();
    }
    _activeConversationId ??= conversations.isEmpty
        ? null
        : conversations.first['id']?.toString();
    setState(() {
      _snapshot = payload;
      _projectionStatus = '已连接本地 Core';
    });
  }

  Future<bool> _sendMessage(
    String content,
    String draftId,
    List<_PendingAttachment> pendingAttachments,
  ) async {
    final client = _client;
    final sessionId = _sessionId;
    final conversations = _list(_snapshot, 'conversations');
    final projectId = _activeProjectId;
    if (client == null ||
        sessionId == null ||
        projectId == null ||
        projectId.isEmpty ||
        conversations.isEmpty) {
      return false;
    }
    final conversationId =
        _activeConversationId ?? conversations.first['id']?.toString();
    if (conversationId == null || conversationId.isEmpty) return false;
    final agentId = _firstProjectAgentId(_snapshot, projectId);
    if (agentId == null || agentId.isEmpty) {
      if (mounted) {
        setState(() => _projectionStatus = '当前项目没有可用智能体');
      }
      return false;
    }
    const senderId = 'user';
    final messageId = 'message-$draftId';
    final sequence =
        _list(_snapshot, 'messages')
            .where((message) => message['conversationId'] == conversationId)
            .length +
        1;
    try {
      final messageExists = _list(
        _snapshot,
        'messages',
      ).any((message) => message['id'] == messageId);
      if (!messageExists) {
        final response = await client.request({
          'kind': 'command',
          'protocol': {'major': protocolMajor, 'minor': 0},
          'requestId': messageId,
          'sessionId': sessionId,
          'command': 'message.create',
          'payload': {
            'messageId': messageId,
            'conversationId': conversationId,
            'senderId': senderId,
            'sequence': sequence,
            'content': content,
          },
        });
        final payload = response['payload'];
        final projection = payload is Map<String, dynamic>
            ? payload['projection']
            : null;
        if (!mounted || projection is! Map<String, dynamic>) return false;
        setState(() {
          _snapshot = projection;
          _projectionStatus = '消息已写入本地 Core';
        });
      }

      for (final indexed in pendingAttachments.indexed) {
        final ordinal = indexed.$1;
        final pending = indexed.$2;
        final identity = crypto.sha256
            .convert(utf8.encode('$messageId:${pending.selectionId}'))
            .toString()
            .substring(0, 40);
        final attachmentId = 'attachment-$identity';
        final artifactId = 'artifact-$identity';
        final alreadyAssociated = _list(
          _snapshot,
          'attachments',
        ).any((attachment) => attachment['attachmentId'] == attachmentId);
        if (alreadyAssociated) continue;
        final imported = await client.importAttachmentFile(
          sessionId: sessionId,
          attachmentId: attachmentId,
          artifactId: artifactId,
          messageId: messageId,
          sourcePath: pending.sourcePath,
          mime: _attachmentMime(pending.fileName),
          ordinal: ordinal,
        );
        final projection = imported['projection'];
        if (!mounted || projection is! Map<String, dynamic>) return false;
        setState(() {
          _snapshot = projection;
          _projectionStatus = '附件已导入本地 Core';
        });
      }

      final collaborationRunId = 'collaboration-$draftId';
      await client.createCollaboration(
        sessionId: sessionId,
        projectId: projectId,
        collaborationRunId: collaborationRunId,
        rootAgentIds: [agentId],
        maxCalls: 8,
        maxDepth: 5,
      );
      final execution = await client.startExecution(
        sessionId: sessionId,
        executionRunId: 'execution-$draftId',
        collaborationRunId: collaborationRunId,
        projectId: projectId,
        conversationId: conversationId,
        agentId: agentId,
        currentTask: content,
      );
      await _refreshProjection(client, sessionId);
      if (mounted) {
        setState(
          () => _projectionStatus =
              '运行已由本地 Core 完成：${execution.run['id'] ?? 'started'}',
        );
      }
      return true;
    } on Object {
      if (mounted) {
        setState(
          () => _projectionStatus = pendingAttachments.isEmpty
              ? '消息被本地 Core 拒绝'
              : '消息或附件被本地 Core 拒绝',
        );
      }
      return false;
    }
  }

  void _selectConversation(String conversationId) {
    if (!mounted) return;
    setState(() {
      _activeConversationId = conversationId;
      _projectionStatus = '已选择会话';
    });
  }

  void _selectProject(String projectId) {
    if (!mounted || projectId.isEmpty) return;
    setState(() {
      _activeProjectId = projectId;
      _projectionStatus = '已选择项目';
    });
  }

  Future<void> _showProjectPicker() async {
    if (!mounted) return;
    final projects = _list(_snapshot, 'projects');
    await showDialog<void>(
      context: context,
      builder: (dialogContext) => AlertDialog(
        title: const Text('项目'),
        content: SizedBox(
          width: 420,
          child: Column(
            mainAxisSize: MainAxisSize.min,
            children: [
              Align(
                alignment: Alignment.centerRight,
                child: FilledButton.tonalIcon(
                  onPressed: () async {
                    Navigator.of(dialogContext).pop();
                    await _showCreateProject();
                  },
                  icon: const Icon(Icons.add),
                  label: const Text('新建项目'),
                ),
              ),
              const SizedBox(height: 8),
              if (projects.isEmpty)
                const Align(
                  alignment: Alignment.centerLeft,
                  child: Text('暂无项目'),
                )
              else
                ListView.builder(
                  shrinkWrap: true,
                  itemCount: projects.length,
                  itemBuilder: (context, index) {
                    final project = projects[index];
                    final id = project['id']?.toString() ?? '';
                    return ListTile(
                      leading: const Icon(Icons.folder_outlined),
                      title: Text(project['name']?.toString() ?? '项目'),
                      subtitle: Text(id),
                      selected: id == _activeProjectId,
                      trailing: PopupMenuButton<String>(
                        tooltip: '项目操作',
                        onSelected: (action) async {
                          if (action == 'edit') {
                            await _showEditProject(project);
                          } else if (action == 'archive') {
                            await _confirmArchiveProject(project);
                          }
                        },
                        itemBuilder: (context) => const [
                          PopupMenuItem(value: 'edit', child: Text('编辑')),
                          PopupMenuItem(value: 'archive', child: Text('归档')),
                        ],
                      ),
                      onTap: () {
                        Navigator.of(dialogContext).pop();
                        _selectProject(id);
                      },
                    );
                  },
                ),
            ],
          ),
        ),
      ),
    );
  }

  Future<void> _showCreateProject() async {
    await showDialog<void>(
      context: context,
      builder: (context) => ProjectionEntityDialog(
        title: '新建项目',
        nameLabel: '项目名称',
        rootPathLabel: '项目根目录',
        onSubmit: (name, rootPath) => _createProject(name, rootPath),
      ),
    );
  }

  Future<void> _createProject(String name, String? rootPath) async {
    final client = _client;
    final sessionId = _sessionId;
    if (client == null || sessionId == null) {
      throw const CoreIpcException('Core projection is not connected');
    }
    final projectId = 'project-${DateTime.now().microsecondsSinceEpoch}';
    final projectPayload = <String, dynamic>{
      'projectId': projectId,
      'name': name,
    };
    if (rootPath != null) {
      projectPayload['rootPath'] = rootPath;
    }
    final response = await client.request({
      'kind': 'command',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': 'project-create-$projectId',
      'sessionId': sessionId,
      'command': 'project.create',
      'payload': projectPayload,
    });
    final payload = response['payload'];
    final projection = payload is Map<String, dynamic>
        ? payload['projection']
        : null;
    if (projection is! Map<String, dynamic>) {
      throw const CoreIpcException('Core project projection is invalid');
    }
    if (!mounted) return;
    setState(() {
      _snapshot = projection;
      _activeProjectId = projectId;
      _projectionStatus = '项目已创建';
    });
  }

  Future<void> _showEditProject(Map<String, dynamic> project) async {
    await showDialog<void>(
      context: context,
      builder: (context) => ProjectionEntityDialog(
        title: '编辑项目',
        nameLabel: '项目名称',
        rootPathLabel: '项目根目录',
        initialName: project['name']?.toString() ?? '',
        initialRootPath: project['rootPath']?.toString() ?? '',
        submitLabel: '保存',
        onSubmit: (name, rootPath) => _updateProject(project, name, rootPath),
      ),
    );
  }

  Future<void> _confirmArchiveProject(Map<String, dynamic> project) async {
    final confirmed = await showDialog<bool>(
      context: context,
      builder: (context) => AlertDialog(
        title: const Text('归档项目？'),
        content: Text(project['name']?.toString() ?? '项目'),
        actions: [
          TextButton(
            onPressed: () => Navigator.of(context).pop(false),
            child: const Text('取消'),
          ),
          FilledButton(
            onPressed: () => Navigator.of(context).pop(true),
            child: const Text('归档'),
          ),
        ],
      ),
    );
    if (confirmed == true) {
      await _updateProject(
        project,
        project['name']?.toString() ?? '项目',
        project['rootPath']?.toString(),
        archived: true,
      );
    }
  }

  Future<void> _updateProject(
    Map<String, dynamic> project,
    String name,
    String? rootPath, {
    bool archived = false,
  }) async {
    final client = _client;
    final sessionId = _sessionId;
    final projectId = project['id']?.toString();
    if (client == null || sessionId == null || projectId == null) {
      throw const CoreIpcException('Core projection is not connected');
    }
    final projectPayload = <String, dynamic>{
      'projectId': projectId,
      'name': name,
      'archived': archived,
    };
    if (rootPath != null && rootPath.isNotEmpty) {
      projectPayload['rootPath'] = rootPath;
    }
    final response = await client.request({
      'kind': 'command',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId':
          'project-update-$projectId-${DateTime.now().microsecondsSinceEpoch}',
      'sessionId': sessionId,
      'command': 'project.update',
      'payload': projectPayload,
    });
    final payload = response['payload'];
    final projection = payload is Map<String, dynamic>
        ? payload['projection']
        : null;
    if (projection is! Map<String, dynamic>) {
      throw const CoreIpcException('Core project projection is invalid');
    }
    if (!mounted) return;
    setState(() {
      _snapshot = projection;
      _projectionStatus = archived ? '项目已归档' : '项目已更新';
    });
  }

  Future<void> _showConversationPicker() async {
    if (!mounted) return;
    final conversations = _list(_snapshot, 'conversations');
    await showDialog<void>(
      context: context,
      builder: (dialogContext) => AlertDialog(
        title: const Text('会话'),
        content: SizedBox(
          width: 420,
          child: Column(
            mainAxisSize: MainAxisSize.min,
            children: [
              Align(
                alignment: Alignment.centerRight,
                child: FilledButton.tonalIcon(
                  onPressed: _activeProjectId == null
                      ? null
                      : () async {
                          Navigator.of(dialogContext).pop();
                          await _showCreateConversation();
                        },
                  icon: const Icon(Icons.add),
                  label: const Text('新建会话'),
                ),
              ),
              const SizedBox(height: 6),
              Align(
                alignment: Alignment.centerRight,
                child: OutlinedButton.icon(
                  onPressed: _activeConversationId == null
                      ? null
                      : () async {
                          Navigator.of(dialogContext).pop();
                          await _showConversationAgentSheet();
                        },
                  icon: const Icon(Icons.group_outlined),
                  label: const Text('配置会话智能体'),
                ),
              ),
              const SizedBox(height: 8),
              if (conversations.isEmpty)
                const Align(
                  alignment: Alignment.centerLeft,
                  child: Text('暂无会话'),
                )
              else
                ListView.builder(
                  shrinkWrap: true,
                  itemCount: conversations.length,
                  itemBuilder: (context, index) {
                    final conversation = conversations[index];
                    final id = conversation['id']?.toString() ?? '';
                    return ListTile(
                      leading: const Icon(Icons.chat_bubble_outline),
                      title: Text(conversation['title']?.toString() ?? '会话'),
                      subtitle: Text(id),
                      selected: id == _activeConversationId,
                      trailing: PopupMenuButton<String>(
                        tooltip: '会话操作',
                        onSelected: (action) async {
                          if (action == 'edit') {
                            await _showEditConversation(conversation);
                          }
                        },
                        itemBuilder: (context) => const [
                          PopupMenuItem(value: 'edit', child: Text('编辑')),
                        ],
                      ),
                      onTap: () {
                        Navigator.of(dialogContext).pop();
                        _selectConversation(id);
                      },
                    );
                  },
                ),
            ],
          ),
        ),
      ),
    );
  }

  Future<void> _showCreateConversation() async {
    await showDialog<void>(
      context: context,
      builder: (context) => ProjectionEntityDialog(
        title: '新建会话',
        nameLabel: '会话标题',
        onSubmit: (title, _) => _createConversation(title),
      ),
    );
  }

  Future<void> _showConversationAgentSheet() async {
    if (!mounted) return;
    await showModalBottomSheet<void>(
      context: context,
      isScrollControlled: true,
      builder: (context) => SafeArea(
        child: SingleChildScrollView(
          padding: const EdgeInsets.all(16),
          child: ConversationAgentAssignmentPanel(
            snapshot: _snapshot,
            conversationId: _activeConversationId,
            onSet: _setConversationAgentAssignment,
            onRemove: _removeConversationAgentAssignment,
          ),
        ),
      ),
    );
  }

  Future<void> _setConversationAgentAssignment({
    required String conversationId,
    required String agentId,
    required bool enabled,
  }) async {
    final client = _client;
    final sessionId = _sessionId;
    if (client == null || sessionId == null) {
      throw const CoreIpcException('Core projection is not connected');
    }
    final response = await client.request({
      'kind': 'command',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId':
          'conversation-agent-set-$conversationId-$agentId-${DateTime.now().microsecondsSinceEpoch}',
      'sessionId': sessionId,
      'command': 'conversation_agent.set',
      'payload': {
        'conversationId': conversationId,
        'agentId': agentId,
        'enabled': enabled,
      },
    });
    _applyProjectionMutation(response, '会话智能体分配已更新');
  }

  Future<void> _removeConversationAgentAssignment({
    required String conversationId,
    required String agentId,
  }) async {
    final client = _client;
    final sessionId = _sessionId;
    if (client == null || sessionId == null) {
      throw const CoreIpcException('Core projection is not connected');
    }
    final response = await client.request({
      'kind': 'command',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId':
          'conversation-agent-remove-$conversationId-$agentId-${DateTime.now().microsecondsSinceEpoch}',
      'sessionId': sessionId,
      'command': 'conversation_agent.remove',
      'payload': {'conversationId': conversationId, 'agentId': agentId},
    });
    _applyProjectionMutation(response, '会话智能体分配已移除');
  }

  Future<void> _createConversation(String title) async {
    final client = _client;
    final sessionId = _sessionId;
    final projectId = _activeProjectId;
    if (client == null || sessionId == null || projectId == null) {
      throw const CoreIpcException('Project/Core projection is not connected');
    }
    final conversationId =
        'conversation-${DateTime.now().microsecondsSinceEpoch}';
    final response = await client.request({
      'kind': 'command',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': 'conversation-create-$conversationId',
      'sessionId': sessionId,
      'command': 'conversation.create',
      'payload': {
        'conversationId': conversationId,
        'projectId': projectId,
        'title': title,
      },
    });
    final payload = response['payload'];
    final projection = payload is Map<String, dynamic>
        ? payload['projection']
        : null;
    if (projection is! Map<String, dynamic>) {
      throw const CoreIpcException('Core conversation projection is invalid');
    }
    if (!mounted) return;
    setState(() {
      _snapshot = projection;
      _activeConversationId = conversationId;
      _projectionStatus = '会话已创建';
    });
  }

  Future<void> _showEditConversation(Map<String, dynamic> conversation) async {
    await showDialog<void>(
      context: context,
      builder: (context) => ProjectionEntityDialog(
        title: '编辑会话',
        nameLabel: '会话标题',
        initialName: conversation['title']?.toString() ?? '',
        submitLabel: '保存',
        onSubmit: (title, _) => _updateConversation(conversation, title),
      ),
    );
  }

  Future<void> _updateConversation(
    Map<String, dynamic> conversation,
    String title,
  ) async {
    final client = _client;
    final sessionId = _sessionId;
    final conversationId = conversation['id']?.toString();
    if (client == null || sessionId == null || conversationId == null) {
      throw const CoreIpcException('Core projection is not connected');
    }
    final response = await client.request({
      'kind': 'command',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId':
          'conversation-update-$conversationId-${DateTime.now().microsecondsSinceEpoch}',
      'sessionId': sessionId,
      'command': 'conversation.update',
      'payload': {'conversationId': conversationId, 'title': title},
    });
    final payload = response['payload'];
    final projection = payload is Map<String, dynamic>
        ? payload['projection']
        : null;
    if (projection is! Map<String, dynamic>) {
      throw const CoreIpcException('Core conversation projection is invalid');
    }
    if (!mounted) return;
    setState(() {
      _snapshot = projection;
      _projectionStatus = '会话已更新';
    });
  }

  Future<void> _setIdentityModelDefault({
    required String identityScope,
    required String agentId,
    String? projectId,
    String? conversationId,
    required String connectorId,
    required String modelId,
  }) async {
    final client = _client;
    final sessionId = _sessionId;
    if (client == null || sessionId == null) {
      throw const CoreIpcException('Core projection is not connected');
    }
    final payload = await client.setIdentityModelOptionDefault(
      sessionId: sessionId,
      target: IdentityModelTarget(
        identityScope: identityScope,
        agentId: agentId,
        projectId: projectId,
        conversationId: conversationId,
      ),
      connectorId: connectorId,
      modelId: modelId,
    );
    final projection = payload['projection'];
    if (projection is! Map<String, dynamic>) {
      throw const CoreIpcException(
        'Identity model default projection payload is invalid',
      );
    }
    if (!mounted) return;
    setState(() {
      _snapshot = projection;
      _projectionStatus = '默认模型已更新';
    });
  }

  Future<void> _showDiagnostics() async {
    Map<String, dynamic> health = <String, dynamic>{
      'status': _projectionStatus,
    };
    Map<String, dynamic> runtimeModels = const <String, dynamic>{};
    final client = _client;
    final sessionId = _sessionId;
    if (client != null && sessionId != null) {
      try {
        final response = await client.request({
          'kind': 'query',
          'protocol': {'major': protocolMajor, 'minor': 0},
          'requestId': 'health-${DateTime.now().microsecondsSinceEpoch}',
          'sessionId': sessionId,
          'query': 'runtime.health',
          'payload': <String, dynamic>{},
        });
        if (response['payload'] is Map<String, dynamic>) {
          health = response['payload'] as Map<String, dynamic>;
        }
      } on Object {
        // Keep the last projection status visible when health is unavailable.
      }
      try {
        runtimeModels = await client.queryRuntimeModels(sessionId: sessionId);
      } on Object {
        // Keep diagnostics usable when the local Core predates runtime.models.
      }
    }
    if (!mounted) return;
    await showDialog<void>(
      context: context,
      builder: (context) => AlertDialog(
        title: const Text('高级诊断详情'),
        content: SizedBox(
          width: 460,
          child: SingleChildScrollView(
            child: DiagnosticsMetadataPanel(
              snapshot: _snapshot,
              health: RuntimeHealth.fromJson(health),
              runtimeModels: runtimeModels,
              projectionStatus: _projectionStatus,
              diagnosticDetails: _coreDiagnosticDetails,
              onRetryStartup: _coreDiagnosticDetails == null
                  ? null
                  : () {
                      Navigator.of(context).pop();
                      unawaited(_retryProjection());
                    },
              onSetModelDefault: client != null && sessionId != null
                  ? _setIdentityModelDefault
                  : null,
            ),
          ),
        ),
        actions: [
          TextButton(
            onPressed: () => Navigator.of(context).pop(),
            child: const Text('关闭'),
          ),
          FilledButton.icon(
            onPressed: client == null || sessionId == null
                ? null
                : () {
                    Navigator.of(context).pop();
                    unawaited(_showConnectorCenter());
                  },
            icon: const Icon(Icons.extension_outlined),
            label: const Text('管理 Connector'),
          ),
        ],
      ),
    );
  }

  Future<void> _showConnectorCenter() async {
    final client = _client;
    final sessionId = _sessionId;
    if (client == null || sessionId == null) {
      if (mounted) {
        ScaffoldMessenger.of(
          context,
        ).showSnackBar(const SnackBar(content: Text('请先连接 Core')));
      }
      return;
    }
    await showDialog<void>(
      context: context,
      builder: (context) => ConnectorCenterDialog(
        client: client,
        sessionId: sessionId,
        onProjectionChanged: (projection) {
          if (!mounted) return;
          setState(() {
            _snapshot = projection;
            _projectionStatus = 'Connector 元数据已更新';
          });
        },
      ),
    );
  }

  Future<void> _showConfigTransfer() async {
    final client = _client;
    final sessionId = _sessionId;
    final projectId = _activeProjectId;
    if (client == null || sessionId == null || projectId == null) {
      if (mounted) {
        ScaffoldMessenger.of(
          context,
        ).showSnackBar(const SnackBar(content: Text('请先连接 Core 并选择项目')));
      }
      return;
    }
    await showDialog<void>(
      context: context,
      builder: (context) => ConfigTransferDialog(
        client: client,
        sessionId: sessionId,
        projectId: projectId,
        onImported: (result) {
          if (!mounted) return;
          setState(() {
            _snapshot = result.projection;
            _activeProjectId = result.newProjectId;
            _activeConversationId = null;
            _projectionStatus = result.workspaceRebindRequired
                ? '配置已导入；请重新授权工作区'
                : '配置已导入';
          });
        },
      ),
    );
  }

  Future<void> _showMessageSearch() async {
    final client = _client;
    final sessionId = _sessionId;
    final conversationId = _activeConversationId;
    await showDialog<void>(
      context: context,
      builder: (context) => MessageSearchDialog(
        search: (query) {
          if (client == null || sessionId == null) {
            throw const CoreIpcException('Core projection is not connected');
          }
          return client.searchMessages(
            sessionId: sessionId,
            query: query,
            conversationId: conversationId,
          );
        },
      ),
    );
  }

  Future<void> _showStoreMemory() async {
    final scopeId = _activeConversationId ?? _activeProjectId;
    if (scopeId == null) {
      if (mounted) {
        ScaffoldMessenger.of(
          context,
        ).showSnackBar(const SnackBar(content: Text('请先选择项目或会话')));
      }
      return;
    }
    await showDialog<void>(
      context: context,
      builder: (context) =>
          MemoryWriteDialog(initialScopeId: scopeId, onSubmit: _storeMemory),
    );
  }

  Future<void> _storeMemory(
    String scopeId,
    String? agentId,
    String contentHash,
    bool confirmed,
  ) async {
    final client = _client;
    final sessionId = _sessionId;
    if (client == null || sessionId == null) {
      throw const CoreIpcException('Core projection is not connected');
    }
    final memoryId =
        'memory-${DateTime.now().microsecondsSinceEpoch}-${Random.secure().nextInt(1 << 32)}';
    final payload = <String, dynamic>{
      'memoryId': memoryId,
      'scopeId': scopeId,
      'contentHash': contentHash,
      'confirmed': confirmed,
    };
    if (agentId != null) payload['agentId'] = agentId;
    final response = await client.request({
      'kind': 'command',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': 'memory-store-$memoryId',
      'sessionId': sessionId,
      'command': 'memory.store',
      'payload': payload,
    });
    final responsePayload = response['payload'];
    final projection = responsePayload is Map<String, dynamic>
        ? responsePayload['projection']
        : null;
    if (projection is! Map<String, dynamic>) {
      throw const CoreIpcException('Core memory projection is invalid');
    }
    if (!mounted) return;
    setState(() {
      _snapshot = projection;
      _projectionStatus = '记忆已保存';
    });
  }

  Future<void> _generateSummary() async {
    final client = _client;
    final sessionId = _sessionId;
    final conversationId = _activeConversationId;
    if (client == null || sessionId == null) {
      throw const CoreIpcException('Core projection is not connected');
    }
    if (conversationId == null || conversationId.isEmpty) {
      throw const CoreIpcException('需要先选择会话才能生成摘要');
    }
    final result = await client.generateSummary(
      sessionId: sessionId,
      scopeId: conversationId,
    );
    final projection = result['projection'];
    if (projection is! Map<String, dynamic>) {
      throw const CoreIpcException('Core summary projection is invalid');
    }
    if (!mounted) return;
    setState(() {
      _snapshot = projection;
      _projectionStatus = '摘要已生成';
    });
  }

  Future<void> _showStoreRetrieval() async {
    final scopeId = _activeConversationId ?? _activeProjectId;
    if (scopeId == null) {
      if (mounted) {
        ScaffoldMessenger.of(
          context,
        ).showSnackBar(const SnackBar(content: Text('请先选择项目或会话')));
      }
      return;
    }
    await showDialog<void>(
      context: context,
      builder: (context) => RetrievalSourceWriteDialog(
        initialScopeId: scopeId,
        onSubmit: _storeRetrievalSource,
      ),
    );
  }

  Future<void> _storeRetrievalSource(
    String scopeId,
    String citation,
    String sha256,
    int tokenCount,
  ) async {
    final client = _client;
    final sessionId = _sessionId;
    if (client == null || sessionId == null) {
      throw const CoreIpcException('Core projection is not connected');
    }
    final sourceId =
        'retrieval-${DateTime.now().microsecondsSinceEpoch}-${Random.secure().nextInt(1 << 32)}';
    final response = await client.request({
      'kind': 'command',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': 'retrieval-store-$sourceId',
      'sessionId': sessionId,
      'command': 'retrieval.store',
      'payload': {
        'retrievalSourceId': sourceId,
        'scopeId': scopeId,
        'citation': citation,
        'sha256': sha256,
        'tokenCount': tokenCount,
      },
    });
    final payload = response['payload'];
    final projection = payload is Map<String, dynamic>
        ? payload['projection']
        : null;
    if (projection is! Map<String, dynamic>) {
      throw const CoreIpcException('Core retrieval projection is invalid');
    }
    if (!mounted) return;
    setState(() {
      _snapshot = projection;
      _projectionStatus = '检索来源已保存';
    });
  }

  Future<void> _showContextInspector() async {
    if (!mounted) return;
    var inspectorSnapshot = _snapshot;
    final client = _client;
    final sessionId = _sessionId;
    final scopeId = _activeConversationId ?? _activeProjectId;
    if (client != null && sessionId != null && scopeId != null) {
      try {
        final retrievalSources = await client.queryRetrievalSources(
          sessionId: sessionId,
          scopeId: scopeId,
        );
        inspectorSnapshot = Map<String, dynamic>.from(_snapshot)
          ..['retrievalSources'] = retrievalSources;
      } on Object {
        // Keep the last projection visible when scoped metadata is unavailable.
      }
      final summaries = _list(inspectorSnapshot, 'summaries');
      final scopedSummaries = summaries
          .where((summary) => summary['scopeId'] == scopeId)
          .toList(growable: false);
      if (scopedSummaries.isNotEmpty) {
        scopedSummaries.sort(
          (left, right) => (right['version'] as int? ?? 0).compareTo(
            left['version'] as int? ?? 0,
          ),
        );
        final summaryId = scopedSummaries.first['id'];
        if (summaryId is String && summaryId.isNotEmpty) {
          try {
            final content = await client.querySummaryContent(
              sessionId: sessionId,
              summaryId: summaryId,
            );
            final enriched = summaries
                .map((summary) {
                  if (summary['id'] != summaryId) return summary;
                  return <String, dynamic>{
                    ...summary,
                    'content': content['content'],
                  };
                })
                .toList(growable: false);
            inspectorSnapshot = Map<String, dynamic>.from(inspectorSnapshot)
              ..['summaries'] = enriched;
          } on Object {
            // Keep metadata visible when the explicit body query is unavailable.
          }
        }
      }
    }
    if (!mounted) return;
    await showModalBottomSheet<void>(
      context: context,
      isScrollControlled: true,
      builder: (context) => SafeArea(
        child: SizedBox(
          height: MediaQuery.sizeOf(context).height * .82,
          child: ContextInspectorDrawer(
            snapshot: inspectorSnapshot,
            projectId: _activeProjectId,
            conversationId: _activeConversationId,
            onSelectRetrievalSources: (sources) =>
                _showRetrievalSelectionDialog(sources, scopeId),
            onFeedbackRetrievalSource: _showRetrievalFeedbackDialog,
            onPreviewRetrieval: _showRetrievalPreview,
            onGenerateSummary: _activeConversationId?.isNotEmpty == true
                ? _generateSummary
                : null,
          ),
        ),
      ),
    );
  }

  Future<void> _showRetrievalPreview() async {
    final client = _client;
    final sessionId = _sessionId;
    final project = _activeProjectId;
    final conversation = _activeConversationId;
    final scope = conversation?.isNotEmpty == true
        ? 'conversation'
        : project?.isNotEmpty == true
        ? 'project'
        : null;
    if (client == null || sessionId == null || scope == null) {
      if (mounted) {
        ScaffoldMessenger.of(
          context,
        ).showSnackBar(const SnackBar(content: Text('请先选择项目或会话，禁止全局搜索')));
      }
      return;
    }
    await showDialog<void>(
      context: context,
      builder: (context) => RetrievalPreviewDialog(
        project: project,
        conversation: conversation,
        agent: null,
        scope: scope,
        preview: (previewRequest) => client.queryRetrievalPreview(
          sessionId: sessionId,
          project: previewRequest.project,
          conversation: previewRequest.conversation,
          agent: previewRequest.agent,
          query: previewRequest.query,
          scope: previewRequest.scope,
          sourceTypes: previewRequest.sourceTypes,
          limit: previewRequest.limit,
        ),
      ),
    );
  }

  Future<void> _showRetrievalSelectionDialog(
    List<Map<String, dynamic>> sources,
    String? scopeId,
  ) async {
    final client = _client;
    final sessionId = _sessionId;
    final projectId = _activeProjectId;
    if (client == null ||
        sessionId == null ||
        scopeId == null ||
        projectId == null) {
      if (mounted) {
        ScaffoldMessenger.of(
          context,
        ).showSnackBar(const SnackBar(content: Text('Core 或检索范围未就绪')));
      }
      return;
    }
    await showDialog<void>(
      context: context,
      builder: (context) => RetrievalSelectionDialog(
        sources: sources,
        onSubmit: (selected) async {
          final selectionId =
              'retrieval-selection-${DateTime.now().microsecondsSinceEpoch}';
          final sourceIds = selected
              .map((source) => source['id']?.toString() ?? '')
              .where((id) => id.isNotEmpty)
              .toList(growable: false);
          final queryHash = crypto.sha256
              .convert(
                utf8.encode(
                  'explicit-selection:$scopeId:${sourceIds.join(',')}',
                ),
              )
              .toString();
          final isConversation = scopeId == _activeConversationId;
          final items = selected
              .asMap()
              .entries
              .map((entry) {
                final source = entry.value;
                return <String, dynamic>{
                  'sourceId': source['id'],
                  'sourceHash': source['sha256'],
                  'rank': entry.key + 1,
                  'scoreMilli': 1000 - entry.key,
                  'matchMethod': 'explicit_selection',
                  'reason': 'explicit_user_choice',
                };
              })
              .toList(growable: false);
          final result = await client.storeRetrievalSelection(
            sessionId: sessionId,
            selectionId: selectionId,
            scope: isConversation ? 'conversation' : 'project',
            scopeId: scopeId,
            projectId: projectId,
            conversationId: isConversation ? scopeId : null,
            retrievalVersion: 'v1',
            queryHash: queryHash,
            items: items,
          );
          if (!mounted) return;
          setState(() {
            _activeRetrievalSelectionId = selectionId;
            _snapshot = result['projection'] as Map<String, dynamic>;
            _projectionStatus = '检索选择已保存';
          });
        },
      ),
    );
  }

  Future<void> _showRetrievalFeedbackDialog(String sourceId) async {
    final client = _client;
    final sessionId = _sessionId;
    final scopeId = _activeConversationId ?? _activeProjectId;
    final selectionId = _activeRetrievalSelectionId;
    if (client == null ||
        sessionId == null ||
        scopeId == null ||
        selectionId == null) {
      if (mounted) {
        ScaffoldMessenger.of(
          context,
        ).showSnackBar(const SnackBar(content: Text('请先保存一个检索选择')));
      }
      return;
    }
    await showDialog<void>(
      context: context,
      builder: (context) => RetrievalFeedbackDialog(
        sourceId: sourceId,
        onSubmit: (label, reason) async {
          final result = await client.storeRetrievalFeedback(
            sessionId: sessionId,
            feedbackId:
                'retrieval-feedback-${DateTime.now().microsecondsSinceEpoch}',
            selectionId: selectionId,
            scopeId: scopeId,
            sourceId: sourceId,
            label: label,
            reason: reason,
            createdAtMs: DateTime.now().millisecondsSinceEpoch,
          );
          if (!mounted) return;
          setState(() {
            _snapshot = result['projection'] as Map<String, dynamic>;
            _projectionStatus = '检索反馈已保存';
          });
        },
      ),
    );
  }

  Future<void> _showScanLocalAgents() async {
    final client = _client;
    final sessionId = _sessionId;
    if (client == null || sessionId == null) {
      if (mounted) {
        ScaffoldMessenger.of(
          context,
        ).showSnackBar(const SnackBar(content: Text('请先连接本地 Core')));
      }
      return;
    }
    final projectId = _activeProjectId;
    if (projectId == null || projectId.isEmpty) {
      if (mounted) {
        ScaffoldMessenger.of(
          context,
        ).showSnackBar(const SnackBar(content: Text('请先选择项目')));
      }
      return;
    }
    await showDialog<void>(
      context: context,
      builder: (context) => LocalAgentScanDialog(
        client: client,
        sessionId: sessionId,
        projectId: projectId,
        onImported: (result) =>
            unawaited(_refreshProjection(client, sessionId)),
        onManualAdd: () => unawaited(_showCreateAgent()),
      ),
    );
  }

  Future<void> _showCreateAgent({LocalConnectorDiscovery? discovery}) async {
    final client = _client;
    final sessionId = _sessionId;
    final projectId = _activeProjectId;
    if (client == null || sessionId == null || projectId == null) {
      if (mounted) {
        ScaffoldMessenger.of(
          context,
        ).showSnackBar(const SnackBar(content: Text('添加智能体前，请先创建或选择项目')));
      }
      return;
    }
    final connectorId = discovery?.connectorId ?? '';
    final initialModelId = discovery?.models.isNotEmpty == true
        ? discovery!.models.first
        : '';
    final knownCatalogModels = discovery == null
        ? const <String, List<String>>{}
        : <String, List<String>>{discovery.connectorId: discovery.models};
    String? createdAgentId;
    await showDialog<void>(
      context: context,
      builder: (context) => AgentIdentityDialog(
        title: discovery == null ? '新建智能体' : '确认并添加智能体',
        initialName: discovery?.displayName ?? '',
        initialRole: discovery == null ? '' : '项目智能体',
        initialSpecialty: '',
        initialSystemPrompt: '',
        initialConnectorId: connectorId,
        initialModelId: initialModelId,
        knownCatalogModels: knownCatalogModels,
        onSubmit: (input) async {
          createdAgentId ??= await _createAgentIdentity(
            client: client,
            sessionId: sessionId,
            input: input,
          );
          await _bindAgentModel(
            client: client,
            sessionId: sessionId,
            agentId: createdAgentId!,
            connectorId: input.connectorId,
            modelId: input.modelId,
          );
          final result = await _joinProjectAgent(
            client: client,
            sessionId: sessionId,
            projectId: projectId,
            agentId: createdAgentId!,
            connectorId: input.connectorId,
            modelId: input.modelId,
          );
          if (!mounted) return;
          setState(() {
            _snapshot = result;
            _projectionStatus = '智能体已创建并加入当前项目';
          });
        },
      ),
    );
  }

  Future<String> _createAgentIdentity({
    required CoreIpcClient client,
    required String sessionId,
    required AgentIdentityInput input,
  }) async {
    final agentId = 'agent-${DateTime.now().microsecondsSinceEpoch}';
    try {
      final response = await client.request({
        'kind': 'command',
        'protocol': {'major': protocolMajor, 'minor': 0},
        'requestId': 'agent-create-$agentId',
        'sessionId': sessionId,
        'command': 'agent.create',
        'payload': {
          'agentId': agentId,
          'name': input.name,
          'role': input.role,
          'specialty': input.specialty,
          'systemPrompt': input.systemPrompt,
        },
      });
      final payload = response['payload'];
      final projection = payload is Map<String, dynamic>
          ? payload['projection']
          : null;
      if (projection is! Map<String, dynamic>) {
        throw const CoreIpcException('智能体创建后的投影无效');
      }
      return agentId;
    } on Object catch (error) {
      throw CoreIpcException('创建智能体身份失败：$error');
    }
  }

  Future<void> _bindAgentModel({
    required CoreIpcClient client,
    required String sessionId,
    required String agentId,
    required String connectorId,
    required String modelId,
  }) async {
    try {
      final response = await client.setAgentModelBinding(
        sessionId: sessionId,
        agentId: agentId,
        connectorId: connectorId,
        modelId: modelId,
      );
      final projection = response['projection'];
      if (projection is! Map<String, dynamic>) {
        throw const CoreIpcException('智能体模型绑定后的投影无效');
      }
    } on Object catch (error) {
      throw CoreIpcException('绑定 Connector 与模型失败：$error');
    }
  }

  Future<Map<String, dynamic>> _joinProjectAgent({
    required CoreIpcClient client,
    required String sessionId,
    required String projectId,
    required String agentId,
    required String connectorId,
    required String modelId,
  }) async {
    try {
      final response = await client.setProjectAgentModelSelection(
        sessionId: sessionId,
        projectId: projectId,
        agentId: agentId,
        enabled: true,
        workspaceAccess: 'none',
        modelSelectionMode: 'pinned',
        modelId: modelId,
        candidateModelListMode: 'inherit',
        candidateModelListRevision: 0,
      );
      final projection = response['projection'];
      if (projection is! Map<String, dynamic>) {
        throw const CoreIpcException('项目智能体加入后的投影无效');
      }
      return projection;
    } on Object catch (error) {
      throw CoreIpcException('加入当前项目失败：$error');
    }
  }

  Future<void> _showEditAgent(Map<String, dynamic> agent) async {
    final client = _client;
    final sessionId = _sessionId;
    final projectId = _activeProjectId;
    final agentId = agent['id']?.toString();
    if (client == null || sessionId == null || agentId == null) {
      if (mounted) {
        ScaffoldMessenger.of(
          context,
        ).showSnackBar(const SnackBar(content: Text('请先连接 Core 并选择智能体')));
      }
      return;
    }
    final assignment = _projectAssignmentForAgent(
      _snapshot,
      projectId,
      agentId,
    );
    final connectorId =
        _agentField(agent, const ['connectorId', 'connector_id']) ?? '';
    final modelId = _agentField(agent, const ['modelId', 'model_id']) ?? '';
    final knownCatalogModels = <String, List<String>>{
      if (connectorId.isNotEmpty)
        connectorId: modelId.isNotEmpty ? <String>[modelId] : const <String>[],
    };
    await showDialog<void>(
      context: context,
      builder: (context) => AgentIdentityDialog(
        title: '编辑智能体',
        initialName: agent['name']?.toString() ?? '',
        initialRole: agent['role']?.toString() ?? '',
        initialSpecialty: agent['specialty']?.toString() ?? '',
        initialSystemPrompt: agent['systemPrompt']?.toString() ?? '',
        initialConnectorId: connectorId,
        initialModelId: modelId,
        knownCatalogModels: knownCatalogModels,
        client: client,
        sessionId: sessionId,
        target: IdentityModelTarget(
          identityScope: projectId == null ? 'base_agent' : 'project_agent',
          agentId: agentId,
          projectId: projectId,
        ),
        submitLabel: '保存并同步',
        onSubmit: (input) async {
          final response = await client.request({
            'kind': 'command',
            'protocol': {'major': protocolMajor, 'minor': 0},
            'requestId':
                'agent-update-$agentId-${DateTime.now().microsecondsSinceEpoch}',
            'sessionId': sessionId,
            'command': 'agent.update',
            'payload': {
              'agentId': agentId,
              'name': input.name,
              'role': input.role,
              'specialty': input.specialty,
              'systemPrompt': input.systemPrompt,
            },
          });
          final payload = response['payload'];
          final updatedProjection = payload is Map<String, dynamic>
              ? payload['projection']
              : null;
          if (updatedProjection is! Map<String, dynamic>) {
            throw const CoreIpcException('智能体更新后的投影无效');
          }
          await _bindAgentModel(
            client: client,
            sessionId: sessionId,
            agentId: agentId,
            connectorId: input.connectorId,
            modelId: input.modelId,
          );
          if (assignment != null && projectId != null) {
            final selection = await client.setProjectAgentModelSelection(
              sessionId: sessionId,
              projectId: projectId,
              agentId: agentId,
              enabled: assignment['enabled'] == true,
              workspaceAccess:
                  assignment['workspaceAccess']?.toString() ?? 'none',
              modelSelectionMode: 'pinned',
              modelId: input.modelId,
              candidateModelListMode: 'inherit',
              candidateModelListRevision: 0,
            );
            final projection = selection['projection'];
            if (projection is! Map<String, dynamic>) {
              throw const CoreIpcException('项目智能体更新后的投影无效');
            }
            if (!mounted) return;
            setState(() {
              _snapshot = projection;
              _projectionStatus = '智能体已更新并同步到当前项目';
            });
          } else if (mounted) {
            setState(() {
              _snapshot = updatedProjection;
              _projectionStatus = '智能体已更新';
            });
          }
        },
      ),
    );
  }

  Future<void> _showAgentSheet() async {
    if (!mounted) return;
    await showModalBottomSheet<void>(
      context: context,
      builder: (context) => SizedBox(
        height: 420,
        child: _AgentProjection(
          snapshot: _snapshot,
          status: _projectionStatus,
          projectId: _activeProjectId,
          onAdd: () => unawaited(_showCreateAgent()),
          onEdit: (agent) => unawaited(_showEditAgent(agent)),
          onManageAssignments: _showProjectAssignmentSheet,
          onScanLocal: _showScanLocalAgents,
        ),
      ),
    );
  }

  Future<void> _showProjectAssignmentSheet() async {
    if (!mounted) return;
    // `showModalBottomSheet` owns a separate route. A builder that captures
    // `_snapshot` directly would keep rendering the pre-mutation projection
    // after the parent shell receives the `project_agent.set` response.
    final sheetSnapshot = ValueNotifier<Map<String, dynamic>>(_snapshot);
    var sheetOpen = true;
    Future<void> setAssignment({
      required String projectId,
      required String agentId,
      required bool enabled,
      required String workspaceAccess,
    }) async {
      await _setProjectAgentAssignment(
        projectId: projectId,
        agentId: agentId,
        enabled: enabled,
        workspaceAccess: workspaceAccess,
      );
      if (mounted && sheetOpen) sheetSnapshot.value = _snapshot;
    }

    Future<void> removeAssignment({
      required String projectId,
      required String agentId,
    }) async {
      await _removeProjectAgentAssignment(
        projectId: projectId,
        agentId: agentId,
      );
      if (mounted && sheetOpen) sheetSnapshot.value = _snapshot;
    }

    try {
      await showModalBottomSheet<void>(
        context: context,
        isScrollControlled: true,
        builder: (context) => SafeArea(
          child: SingleChildScrollView(
            padding: const EdgeInsets.all(16),
            child: ValueListenableBuilder<Map<String, dynamic>>(
              valueListenable: sheetSnapshot,
              builder: (context, snapshot, child) =>
                  ProjectAgentAssignmentPanel.fromSnapshot(
                    snapshot: snapshot,
                    currentProjectId: _activeProjectId,
                    onSet: setAssignment,
                    onRemove: removeAssignment,
                  ),
            ),
          ),
        ),
      );
    } finally {
      sheetOpen = false;
      sheetSnapshot.dispose();
    }
  }

  Future<void> _setProjectAgentAssignment({
    required String projectId,
    required String agentId,
    required bool enabled,
    required String workspaceAccess,
  }) async {
    final client = _client;
    final sessionId = _sessionId;
    if (client == null || sessionId == null) {
      throw const CoreIpcException('Core projection is not connected');
    }
    final response = await client.request({
      'kind': 'command',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId':
          'project-agent-set-$projectId-$agentId-${DateTime.now().microsecondsSinceEpoch}',
      'sessionId': sessionId,
      'command': 'project_agent.set',
      'payload': {
        'projectId': projectId,
        'agentId': agentId,
        'enabled': enabled,
        'workspaceAccess': workspaceAccess,
      },
    });
    _applyProjectionMutation(response, '项目智能体分配已更新');
  }

  Future<void> _removeProjectAgentAssignment({
    required String projectId,
    required String agentId,
  }) async {
    final client = _client;
    final sessionId = _sessionId;
    if (client == null || sessionId == null) {
      throw const CoreIpcException('Core projection is not connected');
    }
    final response = await client.request({
      'kind': 'command',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId':
          'project-agent-remove-$projectId-$agentId-${DateTime.now().microsecondsSinceEpoch}',
      'sessionId': sessionId,
      'command': 'project_agent.remove',
      'payload': {'projectId': projectId, 'agentId': agentId},
    });
    _applyProjectionMutation(response, '项目智能体分配已移除');
  }

  void _applyProjectionMutation(Map<String, dynamic> response, String status) {
    final payload = response['payload'];
    final projection = payload is Map<String, dynamic>
        ? payload['projection']
        : null;
    if (projection is! Map<String, dynamic>) {
      throw const CoreIpcException(
        'Core projection mutation payload is invalid',
      );
    }
    if (!mounted) return;
    setState(() {
      _snapshot = projection;
      _projectionStatus = status;
    });
  }

  Future<void> _showWorkflowSheet() async {
    if (!mounted) return;
    await showModalBottomSheet<void>(
      context: context,
      builder: (context) => SizedBox(
        height: 420,
        child: _WorkflowProjection(
          snapshot: _snapshot,
          status: _projectionStatus,
          onCancel: _cancelExecution,
          onRetry: _retryExecution,
          onRerunCurrent: _rerunCurrentExecution,
          onCreate: _showCreateWorkflow,
          onCreateHandoff: _showCreateStructuredHandoff,
          onDispatchHandoff: _dispatchStructuredHandoff,
          onTransitionHandoff: _transitionStructuredHandoff,
        ),
      ),
    );
  }

  Future<void> _showCreateWorkflow() async {
    final projectId = _activeProjectId;
    if (projectId == null || projectId.isEmpty) {
      if (mounted) {
        ScaffoldMessenger.of(
          context,
        ).showSnackBar(const SnackBar(content: Text('请先选择项目')));
      }
      return;
    }
    await showDialog<void>(
      context: context,
      builder: (context) => WorkflowCreateDialog(
        initialAgentId: _firstProjectAgentId(_snapshot, projectId),
        onSubmit: (name, kind, agentId, promptSupplement) =>
            _createWorkflow(projectId, name, kind, agentId, promptSupplement),
      ),
    );
  }

  Future<void> _createWorkflow(
    String projectId,
    String name,
    String kind,
    String agentId,
    String promptSupplement,
  ) async {
    final client = _client;
    final sessionId = _sessionId;
    if (client == null || sessionId == null) {
      throw const CoreIpcException('Core projection is not connected');
    }
    final workflowId =
        'workflow-${DateTime.now().microsecondsSinceEpoch}-${Random.secure().nextInt(1 << 32)}';
    final response = await client.request({
      'kind': 'command',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': 'workflow-create-$workflowId',
      'sessionId': sessionId,
      'command': 'workflow.create',
      'payload': {
        'projectId': projectId,
        'workflowId': workflowId,
        'name': name,
        'kind': kind,
        'steps': [
          {
            'id': '$workflowId-step-1',
            'order': 1,
            'agentId': agentId,
            if (promptSupplement.isNotEmpty)
              'promptSupplement': promptSupplement,
          },
        ],
      },
    });
    final payload = response['payload'];
    final projection = payload is Map<String, dynamic>
        ? payload['projection']
        : null;
    if (projection is! Map<String, dynamic>) {
      throw const CoreIpcException('Core workflow projection is invalid');
    }
    if (!mounted) return;
    setState(() {
      _snapshot = projection;
      _activeProjectId = projectId;
      _projectionStatus = '工作流已创建';
    });
  }

  Future<void> _showCreateStructuredHandoff() async {
    final projectId = _activeProjectId;
    final client = _client;
    final sessionId = _sessionId;
    if (projectId == null || projectId.isEmpty) {
      if (mounted) {
        ScaffoldMessenger.of(
          context,
        ).showSnackBar(const SnackBar(content: Text('请先选择项目')));
      }
      return;
    }
    if (client == null || sessionId == null || sessionId.isEmpty) {
      if (mounted) {
        ScaffoldMessenger.of(
          context,
        ).showSnackBar(const SnackBar(content: Text('Core 未连接，无法创建结构化交接')));
      }
      return;
    }
    await showDialog<void>(
      context: context,
      builder: (context) => _StructuredHandoffDialog(
        projectId: projectId,
        roster: _projectRoster(_snapshot, projectId),
        executionRuns: _projectExecutionRuns(_snapshot, projectId),
        sourceMessages: _list(_snapshot, 'messages')
            .where(
              (message) =>
                  _activeConversationId == null ||
                  message['conversationId'] == _activeConversationId,
            )
            .toList(growable: false),
        onSubmit:
            (
              toAgentId,
              fromExecutionRunId,
              sourceMessageId,
              fromAgentId,
              task,
              reason,
              autoDispatch,
            ) => _createStructuredHandoff(
              client: client,
              sessionId: sessionId,
              projectId: projectId,
              toAgentId: toAgentId,
              fromExecutionRunId: fromExecutionRunId,
              sourceMessageId: sourceMessageId,
              fromAgentId: fromAgentId,
              task: task,
              reason: reason,
              autoDispatch: autoDispatch,
            ),
      ),
    );
  }

  Future<void> _createStructuredHandoff({
    required CoreIpcClient client,
    required String sessionId,
    required String projectId,
    required String toAgentId,
    required String fromExecutionRunId,
    required String sourceMessageId,
    required String fromAgentId,
    required String task,
    required String reason,
    required bool autoDispatch,
  }) async {
    final collaborationRunId =
        'collaboration-${DateTime.now().microsecondsSinceEpoch}';
    await client.createCollaboration(
      sessionId: sessionId,
      projectId: projectId,
      collaborationRunId: collaborationRunId,
      rootAgentIds: [toAgentId],
      maxCalls: 1,
      maxDepth: 1,
      autoDispatchHandoffs: autoDispatch,
    );
    final handoff = await client.createHandoff(
      sessionId: sessionId,
      handoffId: 'handoff-${DateTime.now().microsecondsSinceEpoch}',
      collaborationRunId: collaborationRunId,
      fromExecutionRunId: fromExecutionRunId,
      sourceMessageId: sourceMessageId,
      fromAgentId: fromAgentId,
      toAgentId: toAgentId,
      task: task,
      reason: reason,
      contextScope: 'conversation',
    );
    if (!mounted) return;
    setState(() {
      _snapshot = handoff.projection;
      _activeProjectId = projectId;
      _projectionStatus = '结构化交接已创建';
    });
  }

  Future<void> _dispatchStructuredHandoff(String handoffId) async {
    final client = _client;
    final sessionId = _sessionId;
    if (client == null || sessionId == null || handoffId.isEmpty) {
      if (mounted) {
        setState(() => _projectionStatus = 'Core 未连接，无法派发交接');
      }
      return;
    }
    try {
      final result = await client.dispatchHandoff(
        sessionId: sessionId,
        handoffId: handoffId,
      );
      if (!mounted) return;
      setState(() {
        _snapshot = result.projection;
        _projectionStatus = result.runtimeStarted ? '交接任务已启动' : '交接任务已创建，等待运行';
      });
    } on Object {
      if (mounted) {
        setState(() => _projectionStatus = '交接派发被 Core 拒绝');
      }
    }
  }

  Future<void> _transitionStructuredHandoff(
    String handoffId,
    String targetStatus,
  ) async {
    final client = _client;
    final sessionId = _sessionId;
    if (client == null || sessionId == null || handoffId.isEmpty) {
      if (mounted) {
        setState(() => _projectionStatus = 'Core 未连接，无法审核交接');
      }
      return;
    }
    try {
      final result = await client.transitionHandoff(
        sessionId: sessionId,
        handoffId: handoffId,
        targetStatus: targetStatus,
      );
      if (!mounted) return;
      setState(() {
        _snapshot = result.projection;
        _projectionStatus = result.changed
            ? '交接已变更为${_handoffStatusLabel(result.status)}'
            : '交接已经是${_handoffStatusLabel(result.status)}';
      });
    } on Object {
      if (mounted) {
        setState(() => _projectionStatus = '交接审核被 Core 拒绝');
      }
    }
  }

  Future<void> _cancelExecution(String runId) async {
    final client = _client;
    final sessionId = _sessionId;
    if (client == null || sessionId == null || runId.isEmpty) {
      if (mounted) setState(() => _projectionStatus = 'Core 未连接，无法取消运行');
      return;
    }
    try {
      await client.request({
        'kind': 'command',
        'protocol': {'major': protocolMajor, 'minor': 0},
        'requestId': 'cancel-$runId-${DateTime.now().microsecondsSinceEpoch}',
        'sessionId': sessionId,
        'command': 'execution.cancel',
        'payload': {'executionRunId': runId},
      });
      await _refreshProjection(client, sessionId);
      if (mounted) setState(() => _projectionStatus = '已请求取消运行');
    } on Object {
      if (mounted) {
        setState(() => _projectionStatus = '取消运行被 Core 拒绝');
      }
    }
  }

  Future<void> _retryExecution(String sourceRunId) =>
      _executeRetry(sourceRunId, rerunCurrent: false);

  Future<void> _rerunCurrentExecution(String sourceRunId) =>
      _executeRetry(sourceRunId, rerunCurrent: true);

  Future<void> _executeRetry(
    String sourceRunId, {
    required bool rerunCurrent,
  }) async {
    final client = _client;
    final sessionId = _sessionId;
    if (client == null || sessionId == null || sourceRunId.isEmpty) {
      if (mounted) setState(() => _projectionStatus = 'Core 未连接，无法重试运行');
      return;
    }
    final taskController = TextEditingController();
    final task = await showDialog<String>(
      context: context,
      builder: (dialogContext) => AlertDialog(
        title: Text(rerunCurrent ? '按当前设置重新运行' : '重试运行'),
        content: TextField(
          controller: taskController,
          autofocus: true,
          minLines: 2,
          maxLines: 5,
          decoration: const InputDecoration(
            labelText: '当前任务',
            hintText: '重新执行时必须显式提供当前任务',
          ),
        ),
        actions: [
          TextButton(
            onPressed: () => Navigator.of(dialogContext).pop(),
            child: const Text('取消'),
          ),
          FilledButton(
            onPressed: () =>
                Navigator.of(dialogContext).pop(taskController.text.trim()),
            child: Text(rerunCurrent ? '使用当前配置重跑' : '重试'),
          ),
        ],
      ),
    );
    taskController.dispose();
    if (!mounted || task == null || task.isEmpty) return;
    final newRunId =
        '${rerunCurrent ? 'rerun-current' : 'retry'}-$sourceRunId-${DateTime.now().microsecondsSinceEpoch}';
    try {
      final result = rerunCurrent
          ? await client.rerunCurrentExecution(
              sessionId: sessionId,
              executionRunId: newRunId,
              sourceExecutionRunId: sourceRunId,
              currentTask: task,
            )
          : await client.retryExecution(
              sessionId: sessionId,
              executionRunId: newRunId,
              sourceExecutionRunId: sourceRunId,
              currentTask: task,
            );
      await _refreshProjection(client, sessionId);
      if (mounted) {
        setState(
          () => _projectionStatus =
              '${rerunCurrent ? '已按当前设置重新运行' : '已重试运行'}：${result.run['id'] ?? newRunId}',
        );
      }
    } on Object catch (error) {
      if (mounted) {
        setState(
          () => _projectionStatus =
              '${rerunCurrent ? '重新运行' : '重试运行'}被 Core 拒绝：$error',
        );
      }
    }
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      body: SafeArea(
        child: Column(
          children: [
            if (_eventRecovery != null)
              EventRecoveryBanner(
                details: _eventRecovery,
                busy: _eventRecoveryBusy,
                errorMessage: _eventRecoveryError,
                onRefreshAndSubscribe: () =>
                    unawaited(_recoverEventStream(subscribe: true)),
                onRefreshAndPoll: () =>
                    unawaited(_recoverEventStream(subscribe: false)),
              ),
            Expanded(
              child: AbsorbPointer(
                absorbing: _eventRecovery != null,
                child: LayoutBuilder(
                  builder: (context, constraints) {
                    final compact = constraints.maxWidth < 1024;
                    return Column(
                      children: [
                        _Header(
                          compact: compact,
                          snapshot: _snapshot,
                          onProjectPressed: _showProjectPicker,
                          onConversationPressed: _showConversationPicker,
                          onConnectorCenterPressed: _showConnectorCenter,
                          onDiagnosticsPressed: _showDiagnostics,
                          onConfigTransferPressed: _showConfigTransfer,
                          onSearchPressed: _showMessageSearch,
                          onToggleTheme: widget.onToggleTheme,
                          onToggleLeftPane: () => setState(() {
                            _leftPaneVisible = !_leftPaneVisible;
                          }),
                          onToggleRightPane: () => setState(() {
                            _rightPaneVisible = !_rightPaneVisible;
                          }),
                          onShowAgentPanel: _showAgentSheet,
                          onShowWorkflowPanel: _showWorkflowSheet,
                        ),
                        Expanded(
                          child: compact
                              ? _ConversationProjection(
                                  snapshot: _snapshot,
                                  projectId: _activeProjectId,
                                  conversationId: _activeConversationId,
                                  onSend: _sendMessage,
                                  onCancel: _cancelExecution,
                                  onShowContext: _showContextInspector,
                                  onStoreMemory: _showStoreMemory,
                                  onStoreRetrieval: _showStoreRetrieval,
                                  filePickerClient: widget.filePickerClient,
                                )
                              : Row(
                                  children: [
                                    if (_leftPaneVisible) ...[
                                      SizedBox(
                                        width: _leftPaneWidth,
                                        child: _AgentProjection(
                                          snapshot: _snapshot,
                                          status: _projectionStatus,
                                          projectId: _activeProjectId,
                                          onAdd: () =>
                                              unawaited(_showCreateAgent()),
                                          onEdit: (agent) =>
                                              unawaited(_showEditAgent(agent)),
                                          onManageAssignments:
                                              _showProjectAssignmentSheet,
                                          onScanLocal: _showScanLocalAgents,
                                        ),
                                      ),
                                      _ResizeHandle(
                                        onDrag: (delta) => setState(() {
                                          _leftPaneWidth =
                                              (_leftPaneWidth + delta).clamp(
                                                220,
                                                420,
                                              );
                                        }),
                                      ),
                                    ],
                                    Expanded(
                                      flex: 57,
                                      child: _ConversationProjection(
                                        snapshot: _snapshot,
                                        projectId: _activeProjectId,
                                        conversationId: _activeConversationId,
                                        onSend: _sendMessage,
                                        onCancel: _cancelExecution,
                                        onShowContext: _showContextInspector,
                                        onStoreMemory: _showStoreMemory,
                                        onStoreRetrieval: _showStoreRetrieval,
                                        filePickerClient:
                                            widget.filePickerClient,
                                      ),
                                    ),
                                    if (_rightPaneVisible) ...[
                                      _ResizeHandle(
                                        onDrag: (delta) => setState(() {
                                          _rightPaneWidth =
                                              (_rightPaneWidth - delta).clamp(
                                                280,
                                                460,
                                              );
                                        }),
                                      ),
                                      SizedBox(
                                        width: _rightPaneWidth,
                                        child: Column(
                                          children: [
                                            Expanded(
                                              flex: 5,
                                              child: _WorkflowProjection(
                                                snapshot: _snapshot,
                                                status: _projectionStatus,
                                                onCancel: _cancelExecution,
                                                onRetry: _retryExecution,
                                                onRerunCurrent:
                                                    _rerunCurrentExecution,
                                                onCreate: _showCreateWorkflow,
                                                onCreateHandoff:
                                                    _showCreateStructuredHandoff,
                                                onDispatchHandoff:
                                                    _dispatchStructuredHandoff,
                                                onTransitionHandoff:
                                                    _transitionStructuredHandoff,
                                              ),
                                            ),
                                            const SizedBox(height: 8),
                                            Expanded(
                                              flex: 4,
                                              child: OrchestrationPanel(
                                                client: _client,
                                                sessionId: _sessionId,
                                              ),
                                            ),
                                          ],
                                        ),
                                      ),
                                    ],
                                  ],
                                ),
                        ),
                      ],
                    );
                  },
                ),
              ),
            ),
          ],
        ),
      ),
    );
  }
}

String _redactStartupDetails(String value) {
  var result = value.replaceAll(RegExp(r'[\r\n]+'), ' ').trim();
  final localAppData = Platform.environment['LOCALAPPDATA'];
  if (localAppData != null && localAppData.isNotEmpty) {
    result = result.replaceAll(localAppData, r'%LOCALAPPDATA%');
  }
  final secretPattern = RegExp(
    r'(authorization|cookie|token|api[_-]?key|password|secret)\s*[:=]\s*\S+',
    caseSensitive: false,
  );
  result = result.replaceAllMapped(
    secretPattern,
    (match) => '${match.group(1)}=<redacted>',
  );
  if (result.length > 4096) {
    result = '${result.substring(0, 4096)}...[truncated]';
  }
  return result;
}

class _ResizeHandle extends StatelessWidget {
  const _ResizeHandle({required this.onDrag});

  final ValueChanged<double> onDrag;

  @override
  Widget build(BuildContext context) {
    return MouseRegion(
      cursor: SystemMouseCursors.resizeColumn,
      child: GestureDetector(
        behavior: HitTestBehavior.opaque,
        onHorizontalDragUpdate: (details) => onDrag(details.delta.dx),
        child: const SizedBox(width: 8, child: VerticalDivider(width: 1)),
      ),
    );
  }
}

class _Header extends StatelessWidget {
  const _Header({
    required this.compact,
    required this.snapshot,
    this.onProjectPressed,
    this.onConversationPressed,
    this.onConnectorCenterPressed,
    this.onDiagnosticsPressed,
    this.onConfigTransferPressed,
    this.onSearchPressed,
    this.onToggleTheme,
    this.onToggleLeftPane,
    this.onToggleRightPane,
    this.onShowAgentPanel,
    this.onShowWorkflowPanel,
  });
  final bool compact;
  final Map<String, dynamic> snapshot;
  final VoidCallback? onProjectPressed;
  final VoidCallback? onConversationPressed;
  final VoidCallback? onConnectorCenterPressed;
  final VoidCallback? onDiagnosticsPressed;
  final VoidCallback? onConfigTransferPressed;
  final VoidCallback? onSearchPressed;
  final VoidCallback? onToggleTheme;
  final VoidCallback? onToggleLeftPane;
  final VoidCallback? onToggleRightPane;
  final VoidCallback? onShowAgentPanel;
  final VoidCallback? onShowWorkflowPanel;

  @override
  Widget build(BuildContext context) {
    final l10n = AppLocalizations.of(context);
    return SizedBox(
      height: 64,
      child: Padding(
        padding: const EdgeInsets.symmetric(horizontal: 20),
        child: Row(
          children: [
            Icon(
              Icons.auto_awesome,
              color: Theme.of(context).colorScheme.primary,
            ),
            const SizedBox(width: 10),
            Text(
              l10n?.title ?? 'AgentTalk',
              style: Theme.of(
                context,
              ).textTheme.titleMedium?.copyWith(fontWeight: FontWeight.w700),
            ),
            const SizedBox(width: 28),
            if (compact) ...[
              IconButton(
                tooltip: l10n?.project ?? '项目',
                onPressed: onProjectPressed,
                icon: const Icon(Icons.folder_open, size: 18),
              ),
              IconButton(
                tooltip: l10n?.conversation ?? '会话',
                onPressed: onConversationPressed,
                icon: const Icon(Icons.chat_bubble_outline, size: 18),
              ),
            ] else ...[
              OutlinedButton.icon(
                onPressed: onProjectPressed,
                icon: const Icon(Icons.folder_open, size: 16),
                label: Text(
                  _firstName(snapshot, 'projects', l10n?.project ?? '项目'),
                ),
              ),
              const SizedBox(width: 8),
              OutlinedButton.icon(
                onPressed: onConversationPressed,
                icon: const Icon(Icons.chat_bubble_outline, size: 16),
                label: Text(
                  _firstName(
                    snapshot,
                    'conversations',
                    l10n?.conversation ?? '会话',
                    'title',
                  ),
                ),
              ),
            ],
            const Spacer(),
            if (!compact)
              IconButton(
                tooltip: l10n?.connectorCenter ?? '连接器管理',
                onPressed: onConnectorCenterPressed,
                icon: const Icon(Icons.extension_outlined, size: 20),
              ),
            if (!compact)
              IconButton(
                tooltip: l10n?.diagnostics ?? '高级诊断',
                onPressed: onDiagnosticsPressed,
                icon: const Icon(Icons.monitor_heart_outlined, size: 20),
              ),
            IconButton(
              tooltip: '导入/导出配置',
              onPressed: onConfigTransferPressed,
              icon: const Icon(Icons.import_export, size: 20),
            ),
            if (!compact)
              IconButton(
                tooltip: l10n?.searchMessages ?? '搜索消息',
                onPressed: onSearchPressed,
                icon: const Icon(Icons.search, size: 20),
              ),
            if (!compact)
              IconButton(
                tooltip: l10n?.agentPanel ?? '智能体面板',
                onPressed: onToggleLeftPane,
                icon: const Icon(Icons.people_outline, size: 20),
              ),
            if (!compact)
              IconButton(
                tooltip: l10n?.workflowPanel ?? '工作流面板',
                onPressed: onToggleRightPane,
                icon: const Icon(Icons.view_sidebar_outlined, size: 20),
              ),
            if (compact)
              IconButton(
                tooltip: l10n?.agentPanel ?? '智能体面板',
                onPressed: onShowAgentPanel,
                icon: const Icon(Icons.people_outline, size: 20),
              ),
            if (compact)
              IconButton(
                tooltip: l10n?.workflowPanel ?? '工作流面板',
                onPressed: onShowWorkflowPanel,
                icon: const Icon(Icons.account_tree_outlined, size: 20),
              ),
            const SizedBox(width: 16),
            IconButton(
              tooltip: l10n?.toggleTheme ?? '切换主题',
              onPressed: onToggleTheme,
              icon: const Icon(Icons.brightness_6_outlined, size: 20),
            ),
          ],
        ),
      ),
    );
  }
}

String _appTitle(BuildContext context) {
  return AppLocalizations.of(context)?.title ?? 'AgentTalk';
}

class _AgentProjection extends StatelessWidget {
  const _AgentProjection({
    required this.snapshot,
    required this.status,
    this.projectId,
    this.onAdd,
    this.onEdit,
    this.onManageAssignments,
    this.onScanLocal,
  });

  final Map<String, dynamic> snapshot;
  final String status;
  final String? projectId;
  final VoidCallback? onAdd;
  final ValueChanged<Map<String, dynamic>>? onEdit;
  final VoidCallback? onManageAssignments;
  final VoidCallback? onScanLocal;

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);
    final cs = theme.colorScheme;
    final projectAgents = _projectAgents(snapshot, projectId);
    final hasProject = projectId?.isNotEmpty == true;
    final emptyTitle = hasProject ? '当前项目还没有智能体' : '还没有选择项目';
    final emptySubtitle = hasProject
        ? '添加或扫描后，智能体会出现在这里。'
        : '可以先扫描本地智能体；添加前需要创建或选择项目。';
    return ColoredBox(
      color: cs.surface,
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Row(
              children: [
                Expanded(
                  child: Text(
                    '智能体',
                    style: theme.textTheme.titleMedium?.copyWith(
                      fontWeight: FontWeight.w700,
                    ),
                  ),
                ),
                Container(
                  padding: const EdgeInsets.symmetric(
                    horizontal: 8,
                    vertical: 2,
                  ),
                  decoration: BoxDecoration(
                    color: cs.surfaceContainerHighest,
                    borderRadius: BorderRadius.circular(999),
                    border: Border.all(color: cs.outlineVariant),
                  ),
                  child: Text(
                    '${projectAgents.length}',
                    style: theme.textTheme.labelSmall?.copyWith(
                      fontWeight: FontWeight.w700,
                    ),
                  ),
                ),
              ],
            ),
            const SizedBox(height: 10),
            Wrap(
              spacing: 8,
              runSpacing: 8,
              children: [
                FilledButton.icon(
                  onPressed: onAdd,
                  icon: const Icon(Icons.add, size: 18),
                  label: const Text('添加智能体'),
                ),
                OutlinedButton.icon(
                  onPressed: onManageAssignments,
                  icon: const Icon(Icons.tune, size: 18),
                  label: const Text('管理分配'),
                ),
              ],
            ),
            const SizedBox(height: 14),
            Expanded(
              child: projectAgents.isEmpty
                  ? Align(
                      alignment: Alignment.topCenter,
                      child: Padding(
                        padding: const EdgeInsets.only(top: 8),
                        child: ConstrainedBox(
                          constraints: const BoxConstraints(maxWidth: 260),
                          child: Column(
                            mainAxisSize: MainAxisSize.min,
                            children: [
                              Icon(
                                Icons.smart_toy_outlined,
                                size: 42,
                                color: cs.onSurfaceVariant,
                              ),
                              const SizedBox(height: 10),
                              Text(
                                emptyTitle,
                                style: theme.textTheme.titleSmall?.copyWith(
                                  fontWeight: FontWeight.w700,
                                ),
                                textAlign: TextAlign.center,
                              ),
                              const SizedBox(height: 6),
                              Text(
                                emptySubtitle,
                                style: theme.textTheme.bodySmall?.copyWith(
                                  color: cs.onSurfaceVariant,
                                ),
                                textAlign: TextAlign.center,
                              ),
                              const SizedBox(height: 14),
                              FilledButton.icon(
                                onPressed: onAdd,
                                icon: const Icon(Icons.add, size: 18),
                                label: const Text('添加智能体'),
                              ),
                              const SizedBox(height: 8),
                              OutlinedButton.icon(
                                onPressed: onScanLocal,
                                icon: const Icon(
                                  Icons.radar_outlined,
                                  size: 18,
                                ),
                                label: const Text('扫描本地智能体'),
                              ),
                            ],
                          ),
                        ),
                      ),
                    )
                  : ListView.separated(
                      itemCount: projectAgents.length,
                      separatorBuilder: (context, index) =>
                          const SizedBox(height: 10),
                      itemBuilder: (context, index) {
                        final agent = projectAgents[index];
                        return _AgentRow(
                          icon: Icons.smart_toy_outlined,
                          label:
                              agent['name']?.toString() ??
                              agent['id']?.toString() ??
                              'Agent',
                          role: agent['role']?.toString() ?? '智能体',
                          specialty: agent['specialty']?.toString() ?? '',
                          status: _agentStatus(
                            snapshot,
                            agent['id']?.toString(),
                          ),
                          color: cs.primary,
                          onEdit: onEdit == null ? null : () => onEdit!(agent),
                        );
                      },
                    ),
            ),
            const SizedBox(height: 8),
            Text(
              status,
              style: theme.textTheme.bodySmall?.copyWith(
                color: cs.onSurfaceVariant,
              ),
            ),
          ],
        ),
      ),
    );
  }
}

class _AgentRow extends StatelessWidget {
  const _AgentRow({
    required this.icon,
    required this.label,
    required this.role,
    required this.specialty,
    required this.status,
    required this.color,
    this.onEdit,
  });
  final IconData icon;
  final String label;
  final String role;
  final String specialty;
  final String status;
  final Color color;
  final VoidCallback? onEdit;

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);
    final cs = theme.colorScheme;
    final statusColor = switch (status.toLowerCase()) {
      'running' || 'assembling' || '运行中' => cs.tertiary,
      'completed' || '就绪' || '待命' => Colors.green,
      'failed' || '失败' => cs.error,
      _ => cs.onSurfaceVariant,
    };
    return Card(
      margin: EdgeInsets.zero,
      color: cs.surfaceContainerLow,
      shape: RoundedRectangleBorder(
        borderRadius: BorderRadius.circular(12),
        side: BorderSide(color: cs.outlineVariant),
      ),
      child: Padding(
        padding: const EdgeInsets.all(12),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Row(
              children: [
                CircleAvatar(
                  radius: 14,
                  backgroundColor: color.withValues(alpha: .18),
                  child: Icon(icon, size: 15, color: color),
                ),
                const SizedBox(width: 8),
                Expanded(
                  child: Text(
                    label,
                    style: theme.textTheme.titleSmall?.copyWith(
                      fontWeight: FontWeight.w700,
                    ),
                    overflow: TextOverflow.ellipsis,
                  ),
                ),
                Container(
                  width: 8,
                  height: 8,
                  decoration: BoxDecoration(
                    color: statusColor,
                    shape: BoxShape.circle,
                  ),
                ),
                const SizedBox(width: 6),
                Text(
                  status,
                  style: theme.textTheme.labelSmall?.copyWith(
                    color: statusColor,
                    fontWeight: FontWeight.w700,
                  ),
                ),
                if (onEdit != null) ...[
                  const SizedBox(width: 4),
                  IconButton(
                    tooltip: '编辑智能体',
                    onPressed: onEdit,
                    icon: const Icon(Icons.more_vert, size: 18),
                  ),
                ],
              ],
            ),
            const SizedBox(height: 6),
            Text(
              role + (specialty.isNotEmpty ? ' · $specialty' : ''),
              style: theme.textTheme.bodySmall?.copyWith(
                color: cs.onSurfaceVariant,
              ),
              maxLines: 1,
              overflow: TextOverflow.ellipsis,
            ),
          ],
        ),
      ),
    );
  }
}

class _PendingAttachment {
  const _PendingAttachment({
    required this.selectionId,
    required this.sourcePath,
    required this.fileName,
  });

  final String selectionId;
  final String sourcePath;
  final String fileName;
}

String _attachmentFileName(String sourcePath) {
  final normalized = sourcePath.replaceAll('\\', '/');
  final segments = normalized
      .split('/')
      .where((segment) => segment.isNotEmpty)
      .toList(growable: false);
  return segments.isEmpty ? 'attachment' : segments.last;
}

String _attachmentMime(String fileName) {
  final lower = fileName.toLowerCase();
  if (lower.endsWith('.txt') || lower.endsWith('.md')) {
    return 'text/plain';
  }
  if (lower.endsWith('.json')) return 'application/json';
  if (lower.endsWith('.pdf')) return 'application/pdf';
  if (lower.endsWith('.png')) return 'image/png';
  if (lower.endsWith('.jpg') || lower.endsWith('.jpeg')) {
    return 'image/jpeg';
  }
  if (lower.endsWith('.gif')) return 'image/gif';
  if (lower.endsWith('.webp')) return 'image/webp';
  return 'application/octet-stream';
}

class _ConversationProjection extends StatefulWidget {
  const _ConversationProjection({
    required this.snapshot,
    this.projectId,
    this.conversationId,
    this.onSend,
    this.onCancel,
    this.onShowContext,
    this.onStoreMemory,
    this.onStoreRetrieval,
    this.filePickerClient,
  });

  final Map<String, dynamic> snapshot;
  final String? projectId;
  final String? conversationId;
  final Future<bool> Function(
    String content,
    String draftId,
    List<_PendingAttachment> pendingAttachments,
  )?
  onSend;
  final Future<void> Function(String runId)? onCancel;
  final Future<void> Function()? onShowContext;
  final Future<void> Function()? onStoreMemory;
  final Future<void> Function()? onStoreRetrieval;
  final FilePickerClient? filePickerClient;

  @override
  State<_ConversationProjection> createState() =>
      _ConversationProjectionState();
}

class _ConversationProjectionState extends State<_ConversationProjection> {
  final TextEditingController _composer = TextEditingController();
  final List<_PendingAttachment> _pendingAttachments = [];
  bool _sending = false;
  bool _pickingAttachment = false;
  String? _draftId;
  int _attachmentSequence = 0;

  @override
  void didUpdateWidget(covariant _ConversationProjection oldWidget) {
    super.didUpdateWidget(oldWidget);
    if (oldWidget.conversationId != widget.conversationId) {
      _pendingAttachments.clear();
      _draftId = null;
      _attachmentSequence = 0;
    }
  }

  @override
  void dispose() {
    _composer.dispose();
    super.dispose();
  }

  Future<void> _send() async {
    final content = _composer.text.trim();
    final onSend = widget.onSend;
    if (content.isEmpty || onSend == null || _sending) return;
    setState(() => _sending = true);
    try {
      final draftId = _draftId ??= DateTime.now().microsecondsSinceEpoch
          .toString();
      final accepted = await onSend(
        content,
        draftId,
        List<_PendingAttachment>.unmodifiable(_pendingAttachments),
      );
      if (mounted && accepted) {
        _composer.clear();
        setState(() {
          _pendingAttachments.clear();
          _draftId = null;
          _attachmentSequence = 0;
        });
      }
    } finally {
      if (mounted) setState(() => _sending = false);
    }
  }

  Future<void> _pickAttachment() async {
    if (_sending || _pickingAttachment) return;
    if (widget.conversationId?.isNotEmpty != true) {
      ScaffoldMessenger.of(
        context,
      ).showSnackBar(const SnackBar(content: Text('请先选择会话，再添加附件')));
      return;
    }
    setState(() => _pickingAttachment = true);
    final result = await (widget.filePickerClient ?? createFilePickerClient())
        .pickFile();
    if (!mounted) return;
    setState(() => _pickingAttachment = false);
    if (result.hasSelection) {
      final sourcePath = result.path!;
      if (_pendingAttachments.any(
        (attachment) => attachment.sourcePath == sourcePath,
      )) {
        ScaffoldMessenger.of(
          context,
        ).showSnackBar(const SnackBar(content: Text('该附件已在待发送列表中')));
        return;
      }
      setState(() {
        _pendingAttachments.add(
          _PendingAttachment(
            selectionId:
                '${DateTime.now().microsecondsSinceEpoch}-${_attachmentSequence++}',
            sourcePath: sourcePath,
            fileName: _attachmentFileName(sourcePath),
          ),
        );
      });
      return;
    }
    if (result.status == FilePickerStatus.cancelled) return;
    ScaffoldMessenger.of(context).showSnackBar(
      SnackBar(content: Text(result.message ?? '附件选择失败；未使用默认路径。')),
    );
  }

  void _removeAttachment(String selectionId) {
    if (_sending) return;
    setState(
      () => _pendingAttachments.removeWhere(
        (attachment) => attachment.selectionId == selectionId,
      ),
    );
  }

  Future<void> _cancelActiveRun(String runId) async {
    final onCancel = widget.onCancel;
    if (onCancel == null || runId.isEmpty) return;
    await onCancel(runId);
  }

  String? _activeRunId() {
    return activeRunIdForConversation(widget.snapshot, widget.conversationId);
  }

  void _selectComposerTool(String tool) {
    if (tool == 'Attachment') {
      unawaited(_pickAttachment());
      return;
    }
    if (tool == 'Agent picker') {
      unawaited(_showAgentMentionPicker());
      return;
    }
    if ((tool == 'Memory' || tool == 'Retrieval') &&
        widget.onShowContext != null) {
      unawaited(widget.onShowContext!());
      return;
    }
    if (tool == 'Memory write' && widget.onStoreMemory != null) {
      unawaited(widget.onStoreMemory!());
      return;
    }
    if (tool == 'Retrieval write' && widget.onStoreRetrieval != null) {
      unawaited(widget.onStoreRetrieval!());
      return;
    }
    ScaffoldMessenger.of(
      context,
    ).showSnackBar(SnackBar(content: Text('$tool 已准备好由 Core 接管')));
  }

  Future<void> _showAgentMentionPicker() async {
    final projectId = widget.projectId;
    final roster = projectId == null
        ? const <Map<String, dynamic>>[]
        : _projectRoster(widget.snapshot, projectId);
    if (roster.isEmpty) {
      ScaffoldMessenger.of(
        context,
      ).showSnackBar(const SnackBar(content: Text('当前项目还没有可指定的智能体')));
      return;
    }
    final selected = await showDialog<Map<String, dynamic>>(
      context: context,
      builder: (context) => AlertDialog(
        title: const Text('指定智能体'),
        content: SizedBox(
          width: 360,
          child: ListView.separated(
            shrinkWrap: true,
            itemCount: roster.length,
            separatorBuilder: (context, index) => const Divider(height: 1),
            itemBuilder: (context, index) {
              final agent = roster[index];
              return ListTile(
                leading: const Icon(Icons.alternate_email),
                title: Text(agent['label']?.toString() ?? '智能体'),
                subtitle: Text(agent['id']?.toString() ?? ''),
                onTap: () => Navigator.of(context).pop(agent),
              );
            },
          ),
        ),
        actions: [
          TextButton(
            onPressed: () => Navigator.of(context).pop(),
            child: const Text('取消'),
          ),
        ],
      ),
    );
    if (selected == null || !mounted) return;
    final label = selected['label']?.toString() ?? selected['id']?.toString();
    if (label == null || label.isEmpty) return;
    _insertMention(label);
  }

  void _insertMention(String label) {
    final mention = '@$label ';
    final value = _composer.value;
    final selection = value.selection;
    final start = selection.isValid ? selection.start : value.text.length;
    final end = selection.isValid ? selection.end : value.text.length;
    final text = value.text.replaceRange(start, end, mention);
    final offset = start + mention.length;
    _composer.value = TextEditingValue(
      text: text,
      selection: TextSelection.collapsed(offset: offset),
    );
  }

  @override
  Widget build(BuildContext context) {
    final messages = _list(widget.snapshot, 'messages')
        .where(
          (message) =>
              widget.conversationId == null ||
              message['conversationId'] == widget.conversationId,
        )
        .toList(growable: false);
    final activeRunId = _activeRunId();
    return Padding(
      padding: const EdgeInsets.symmetric(horizontal: 28, vertical: 22),
      child: Column(
        children: [
          Expanded(
            child: messages.isEmpty
                ? Center(
                    child: ConstrainedBox(
                      constraints: const BoxConstraints(maxWidth: 920),
                      child: Column(
                        mainAxisAlignment: MainAxisAlignment.center,
                        children: [
                          const Icon(
                            Icons.forum_outlined,
                            size: 48,
                            color: Color(0xff94a3b8),
                          ),
                          const SizedBox(height: 14),
                          Text(
                            '开始你的协作对话',
                            style: Theme.of(context).textTheme.headlineSmall
                                ?.copyWith(fontWeight: FontWeight.w700),
                          ),
                          const SizedBox(height: 8),
                          Text(
                            '输入消息，使用 @ 可指定智能体...',
                            style: Theme.of(context).textTheme.bodyMedium,
                          ),
                        ],
                      ),
                    ),
                  )
                : ListView.builder(
                    padding: const EdgeInsets.symmetric(vertical: 12),
                    itemCount: messages.length,
                    itemBuilder: (context, index) {
                      final message = messages[index];
                      return Align(
                        alignment: Alignment.centerRight,
                        child: ConstrainedBox(
                          constraints: const BoxConstraints(maxWidth: 720),
                          child: Card(
                            color: Theme.of(
                              context,
                            ).colorScheme.primaryContainer,
                            child: Padding(
                              padding: const EdgeInsets.all(12),
                              child: Text(message['content']?.toString() ?? ''),
                            ),
                          ),
                        ),
                      );
                    },
                  ),
          ),
          Text(
            '当前对话 ${messages.length} 条消息 · 工作区 ${_list(widget.snapshot, 'conversations').length} 个对话',
            style: Theme.of(context).textTheme.bodySmall,
          ),
          if (_pendingAttachments.isNotEmpty || _pickingAttachment) ...[
            const SizedBox(height: 8),
            ConstrainedBox(
              constraints: const BoxConstraints(maxWidth: 880),
              child: Semantics(
                label: '待发送附件 ${_pendingAttachments.length} 个',
                child: Wrap(
                  key: const ValueKey('composer-pending-attachments'),
                  spacing: 8,
                  runSpacing: 6,
                  crossAxisAlignment: WrapCrossAlignment.center,
                  children: [
                    if (_pickingAttachment)
                      const SizedBox(
                        width: 18,
                        height: 18,
                        child: CircularProgressIndicator(strokeWidth: 2),
                      ),
                    for (final attachment in _pendingAttachments)
                      InputChip(
                        key: ValueKey(
                          'composer-pending-attachment-${attachment.selectionId}',
                        ),
                        label: Text(attachment.fileName),
                        avatar: const Icon(
                          Icons.attach_file_outlined,
                          size: 16,
                        ),
                        onDeleted: _sending
                            ? null
                            : () => _removeAttachment(attachment.selectionId),
                      ),
                  ],
                ),
              ),
            ),
          ],
          const SizedBox(height: 8),
          ConstrainedBox(
            constraints: const BoxConstraints(maxWidth: 880),
            child: TextField(
              controller: _composer,
              minLines: 1,
              maxLines: 5,
              onSubmitted: (_) => _send(),
              decoration: InputDecoration(
                hintText: '输入消息，使用 @ 可指定智能体...',
                prefixIcon: PopupMenuButton<String>(
                  tooltip: '编写器工具',
                  onSelected: _selectComposerTool,
                  icon: const Icon(Icons.alternate_email),
                  itemBuilder: (context) => const [
                    PopupMenuItem(value: 'Agent picker', child: Text('指定智能体')),
                    PopupMenuItem(value: 'Attachment', child: Text('附件')),
                    PopupMenuItem(value: 'Memory', child: Text('记忆')),
                    PopupMenuItem(value: 'Memory write', child: Text('保存记忆')),
                    PopupMenuItem(value: 'Retrieval', child: Text('检索')),
                    PopupMenuItem(
                      value: 'Retrieval write',
                      child: Text('保存检索来源'),
                    ),
                  ],
                ),
                suffixIcon: activeRunId != null && widget.onCancel != null
                    ? IconButton(
                        tooltip: '停止当前运行',
                        onPressed: _sending
                            ? null
                            : () => unawaited(_cancelActiveRun(activeRunId)),
                        icon: const Icon(Icons.stop_circle_outlined),
                      )
                    : IconButton(
                        tooltip: '发送',
                        onPressed: _sending ? null : _send,
                        icon: _sending
                            ? const SizedBox(
                                width: 18,
                                height: 18,
                                child: CircularProgressIndicator(
                                  strokeWidth: 2,
                                ),
                              )
                            : const Icon(Icons.send),
                      ),
                border: OutlineInputBorder(
                  borderRadius: BorderRadius.circular(12),
                ),
              ),
            ),
          ),
        ],
      ),
    );
  }
}

class _StructuredHandoffDialog extends StatefulWidget {
  const _StructuredHandoffDialog({
    required this.projectId,
    required this.roster,
    required this.executionRuns,
    required this.sourceMessages,
    required this.onSubmit,
  });

  final String projectId;
  final List<Map<String, dynamic>> roster;
  final List<Map<String, dynamic>> executionRuns;
  final List<Map<String, dynamic>> sourceMessages;
  final Future<void> Function(
    String toAgentId,
    String fromExecutionRunId,
    String sourceMessageId,
    String fromAgentId,
    String task,
    String reason,
    bool autoDispatch,
  )
  onSubmit;

  @override
  State<_StructuredHandoffDialog> createState() =>
      _StructuredHandoffDialogState();
}

class _StructuredHandoffDialogState extends State<_StructuredHandoffDialog> {
  final _formKey = GlobalKey<FormState>();
  String? _toAgentId;
  String? _fromExecutionRunId;
  String? _sourceMessageId;
  final _taskController = TextEditingController();
  final _reasonController = TextEditingController();
  bool _autoDispatch = false;
  String? _error;
  bool _saving = false;

  @override
  void dispose() {
    _taskController.dispose();
    _reasonController.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return AlertDialog(
      title: const Text('创建结构化交接'),
      content: SizedBox(
        width: 480,
        child: Form(
          key: _formKey,
          child: Column(
            mainAxisSize: MainAxisSize.min,
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Text(
                '项目：${widget.projectId}',
                style: Theme.of(context).textTheme.bodySmall,
              ),
              const SizedBox(height: 12),
              DropdownButtonFormField<String>(
                key: const Key('handoff-source-run'),
                initialValue: _fromExecutionRunId,
                decoration: const InputDecoration(
                  labelText: '来源运行',
                  border: OutlineInputBorder(),
                ),
                items: widget.executionRuns
                    .map(
                      (run) => DropdownMenuItem<String>(
                        value: _executionRunId(run),
                        child: Text(_executionRunLabel(run)),
                      ),
                    )
                    .toList(growable: false),
                onChanged: _saving
                    ? null
                    : (value) => setState(() => _fromExecutionRunId = value),
                validator: (value) =>
                    value == null || value.isEmpty ? '请选择来源运行' : null,
              ),
              const SizedBox(height: 12),
              DropdownButtonFormField<String>(
                key: const Key('handoff-target-agent'),
                initialValue: _toAgentId,
                decoration: const InputDecoration(
                  labelText: '目标智能体（当前项目）',
                  border: OutlineInputBorder(),
                ),
                items: widget.roster
                    .map(
                      (agent) => DropdownMenuItem<String>(
                        value: agent['id']?.toString(),
                        child: Text(agent['label']?.toString() ?? '智能体'),
                      ),
                    )
                    .toList(growable: false),
                onChanged: _saving
                    ? null
                    : (value) => setState(() => _toAgentId = value),
                validator: (value) {
                  if (widget.roster.isEmpty) {
                    return '当前项目没有可用智能体';
                  }
                  return value == null || value.isEmpty ? '请选择目标智能体' : null;
                },
              ),
              const SizedBox(height: 12),
              DropdownButtonFormField<String>(
                key: const Key('handoff-source-message'),
                initialValue: _sourceMessageId,
                decoration: const InputDecoration(
                  labelText: '触发消息',
                  border: OutlineInputBorder(),
                ),
                items: widget.sourceMessages
                    .map(
                      (message) => DropdownMenuItem<String>(
                        value: message['id']?.toString(),
                        child: Text(
                          '${message['id'] ?? 'message'} · ${message['content'] ?? ''}',
                          overflow: TextOverflow.ellipsis,
                        ),
                      ),
                    )
                    .toList(growable: false),
                onChanged: _saving
                    ? null
                    : (value) => setState(() => _sourceMessageId = value),
                validator: (value) => widget.sourceMessages.isEmpty
                    ? '当前会话没有可作为触发源的消息'
                    : value == null || value.isEmpty
                    ? '请选择触发消息'
                    : null,
              ),
              const SizedBox(height: 12),
              TextFormField(
                key: const Key('handoff-task'),
                controller: _taskController,
                minLines: 2,
                maxLines: 4,
                decoration: const InputDecoration(
                  labelText: '结构化任务',
                  border: OutlineInputBorder(),
                ),
                validator: (value) =>
                    value == null || value.trim().isEmpty ? '请输入结构化任务' : null,
              ),
              const SizedBox(height: 12),
              TextFormField(
                key: const Key('handoff-reason'),
                controller: _reasonController,
                decoration: const InputDecoration(
                  labelText: '原因（可选）',
                  border: OutlineInputBorder(),
                ),
              ),
              CheckboxListTile(
                key: const Key('handoff-auto-dispatch'),
                contentPadding: EdgeInsets.zero,
                value: _autoDispatch,
                onChanged: _saving
                    ? null
                    : (value) => setState(() => _autoDispatch = value ?? false),
                title: const Text('批准后自动接力并启动子任务'),
                subtitle: const Text('仍由 Core 执行权限、深度和循环校验'),
              ),
              if (widget.executionRuns.isEmpty) ...[
                const SizedBox(height: 6),
                Text(
                  '当前项目没有可用运行',
                  style: TextStyle(color: Theme.of(context).colorScheme.error),
                ),
              ],
              if (_error != null) ...[
                const SizedBox(height: 10),
                Text(
                  _error!,
                  key: const Key('handoff-submit-error'),
                  style: TextStyle(color: Theme.of(context).colorScheme.error),
                ),
              ],
            ],
          ),
        ),
      ),
      actions: [
        TextButton(
          onPressed: _saving ? null : () => Navigator.of(context).pop(),
          child: const Text('取消'),
        ),
        FilledButton(
          key: const Key('handoff-submit'),
          onPressed: _saving ? null : _submit,
          child: _saving
              ? const SizedBox(
                  width: 18,
                  height: 18,
                  child: CircularProgressIndicator(strokeWidth: 2),
                )
              : const Text('创建'),
        ),
      ],
    );
  }

  Future<void> _submit() async {
    if (!(_formKey.currentState?.validate() ?? false)) return;
    final toAgentId = _toAgentId;
    final fromExecutionRunId = _fromExecutionRunId;
    final sourceMessageId = _sourceMessageId;
    final fromAgentId = widget.executionRuns
        .where((run) => _executionRunId(run) == fromExecutionRunId)
        .map((run) => run['agentId']?.toString())
        .firstWhere(
          (value) => value != null && value.isNotEmpty,
          orElse: () => null,
        );
    if (toAgentId == null ||
        fromExecutionRunId == null ||
        sourceMessageId == null ||
        fromAgentId == null) {
      return;
    }
    setState(() {
      _saving = true;
      _error = null;
    });
    try {
      await widget.onSubmit(
        toAgentId,
        fromExecutionRunId,
        sourceMessageId,
        fromAgentId,
        _taskController.text.trim(),
        _reasonController.text.trim(),
        _autoDispatch,
      );
      if (mounted) Navigator.of(context, rootNavigator: true).pop();
    } on Object catch (error) {
      if (!mounted) return;
      setState(() {
        _saving = false;
        _error = error.toString();
      });
    }
  }
}

class RunCard extends StatelessWidget {
  const RunCard({
    super.key,
    required this.runId,
    required this.agentId,
    required this.status,
    required this.canRetry,
    required this.canCancel,
    this.onRetry,
    this.onCancel,
    this.onRerunCurrent,
  });

  final String runId;
  final String agentId;
  final String status;
  final bool canRetry;
  final bool canCancel;
  final void Function(String runId)? onRetry;
  final void Function(String runId)? onCancel;
  final void Function(String runId)? onRerunCurrent;

  @override
  Widget build(BuildContext context) {
    Color? iconColor;
    IconData iconData = Icons.play_circle_outline;
    String displayStatus = status;
    bool isTerminal = false;

    switch (status.toLowerCase()) {
      case 'completed':
        iconColor = Colors.green;
        iconData = Icons.check_circle_outline;
        displayStatus = '已完成';
        isTerminal = true;
        break;
      case 'failed':
        iconColor = Colors.red;
        iconData = Icons.error_outline;
        displayStatus = '失败';
        isTerminal = true;
        break;
      case 'cancelled':
        iconColor = Colors.grey;
        iconData = Icons.cancel_outlined;
        displayStatus = '已取消';
        isTerminal = true;
        break;
      case 'interrupted':
        iconColor = Colors.amber;
        iconData = Icons.pause_circle_outline;
        displayStatus = '已中断';
        isTerminal = true;
        break;
    }

    final bool showRetry =
        isTerminal && status.toLowerCase() != 'completed' && canRetry;

    final runActions = <Widget>[
      if (showRetry && onRetry != null)
        IconButton(
          tooltip: '重试',
          onPressed: () => onRetry!(runId),
          icon: const Icon(Icons.refresh, size: 18),
        ),
      if (showRetry && onRerunCurrent != null)
        IconButton(
          tooltip: '按当前设置重新运行',
          onPressed: () => onRerunCurrent!(runId),
          icon: const Icon(Icons.replay_outlined, size: 18),
        ),
      if (!isTerminal && canCancel && onCancel != null)
        IconButton(
          tooltip: '取消运行',
          onPressed: () => onCancel!(runId),
          icon: const Icon(Icons.stop_circle_outlined, size: 18),
        ),
    ];
    return Semantics(
      label: '运行任务 $agentId: $displayStatus',
      child: ListTile(
        dense: true,
        contentPadding: EdgeInsets.zero,
        leading: isTerminal
            ? Icon(iconData, size: 18, color: iconColor)
            : const SizedBox(
                width: 18,
                height: 18,
                child: CircularProgressIndicator(strokeWidth: 2),
              ),
        title: Text(agentId),
        subtitle: Text(displayStatus),
        trailing: runActions.isEmpty
            ? null
            : Row(mainAxisSize: MainAxisSize.min, children: runActions),
      ),
    );
  }
}

class _WorkflowProjection extends StatelessWidget {
  const _WorkflowProjection({
    required this.snapshot,
    required this.status,
    this.onCancel,
    this.onRetry,
    this.onRerunCurrent,
    this.onCreate,
    this.onCreateHandoff,
    this.onDispatchHandoff,
    this.onTransitionHandoff,
  });

  final Map<String, dynamic> snapshot;
  final String status;
  final Future<void> Function(String runId)? onCancel;
  final Future<void> Function(String runId)? onRetry;
  final Future<void> Function(String runId)? onRerunCurrent;
  final Future<void> Function()? onCreate;
  final Future<void> Function()? onCreateHandoff;
  final Future<void> Function(String handoffId)? onDispatchHandoff;
  final Future<void> Function(String handoffId, String targetStatus)?
  onTransitionHandoff;

  @override
  Widget build(BuildContext context) {
    final workflows = _list(snapshot, 'workflows');
    final runs = _list(snapshot, 'runs');
    final collaborations = _list(snapshot, 'collaborationRuns');
    final handoffs = _list(snapshot, 'handoffs');
    return Padding(
      padding: const EdgeInsets.all(18),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Row(
            children: [
              Expanded(
                child: Text(
                  '@ 接力看板',
                  style: Theme.of(context).textTheme.titleMedium?.copyWith(
                    fontWeight: FontWeight.w700,
                  ),
                ),
              ),
              if (onCreate != null)
                TextButton.icon(
                  onPressed: onCreate,
                  icon: const Icon(Icons.add, size: 18),
                  label: const Text('创建工作流'),
                ),
              if (onCreateHandoff != null)
                IconButton(
                  tooltip: '创建结构化交接',
                  onPressed: onCreateHandoff,
                  icon: const Icon(Icons.redo_outlined, size: 18),
                ),
            ],
          ),
          const SizedBox(height: 16),
          Card(
            child: Padding(
              padding: EdgeInsets.all(16),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Icon(
                    Icons.account_tree_outlined,
                    color: Theme.of(context).colorScheme.onSurfaceVariant,
                  ),
                  const SizedBox(height: 10),
                  if (workflows.isEmpty &&
                      runs.isEmpty &&
                      collaborations.isEmpty &&
                      handoffs.isEmpty) ...[
                    const Text('还没有接力流程'),
                    const SizedBox(height: 4),
                    Text('0 个运行 · $status'),
                  ] else ...[
                    Text(
                      '${workflows.length} 个工作流 · ${runs.length} 个运行 · '
                      '${collaborations.length} 个协作',
                    ),
                    const SizedBox(height: 8),
                    ...runs.take(4).map((run) {
                      final runId =
                          run['id']?.toString() ??
                          run['executionRunId']?.toString() ??
                          '';
                      final runStatus = run['status']?.toString() ?? 'unknown';
                      final canCancel = _isActiveRunStatus(runStatus);
                      final canRetry = _isRetryableRunStatus(runStatus);
                      return RunCard(
                        runId: runId,
                        agentId: run['agentId']?.toString() ?? '运行',
                        status: runStatus,
                        canRetry: canRetry,
                        canCancel: canCancel,
                        onRetry: onRetry,
                        onRerunCurrent: onRerunCurrent,
                        onCancel: onCancel,
                      );
                    }),
                    if (handoffs.isNotEmpty) ...[
                      const Divider(height: 20),
                      Text(
                        '结构化交接',
                        style: Theme.of(context).textTheme.titleSmall,
                      ),
                      ...handoffs.take(4).map((handoff) {
                        final handoffId = handoff['id']?.toString() ?? '';
                        final handoffStatus =
                            handoff['status']?.toString() ?? 'unknown';
                        final canDispatch =
                            handoffStatus == 'approved' &&
                            onDispatchHandoff != null &&
                            handoffId.isNotEmpty;
                        final canApprove =
                            handoffStatus == 'proposed' &&
                            onTransitionHandoff != null &&
                            handoffId.isNotEmpty;
                        final canCancel =
                            (handoffStatus == 'proposed' ||
                                handoffStatus == 'approved' ||
                                handoffStatus == 'dispatched') &&
                            onTransitionHandoff != null &&
                            handoffId.isNotEmpty;
                        final transitionActions = <Widget>[
                          if (canApprove)
                            IconButton(
                              tooltip: '批准交接',
                              onPressed: () =>
                                  onTransitionHandoff!(handoffId, 'approved'),
                              icon: const Icon(Icons.check_circle_outline),
                            ),
                          if (canApprove)
                            IconButton(
                              tooltip: '拒绝交接',
                              onPressed: () =>
                                  onTransitionHandoff!(handoffId, 'rejected'),
                              icon: const Icon(Icons.block_outlined),
                            ),
                          if (canDispatch)
                            IconButton(
                              tooltip: '派发交接',
                              onPressed: () => onDispatchHandoff!(handoffId),
                              icon: const Icon(Icons.play_arrow_outlined),
                            ),
                          if (canCancel)
                            IconButton(
                              tooltip: '取消交接',
                              onPressed: () =>
                                  onTransitionHandoff!(handoffId, 'cancelled'),
                              icon: const Icon(Icons.cancel_outlined),
                            ),
                        ];
                        return ListTile(
                          dense: true,
                          contentPadding: EdgeInsets.zero,
                          leading: const Icon(Icons.redo_outlined, size: 18),
                          title: Text(
                            handoff['toAgentId']?.toString() ?? '智能体',
                          ),
                          subtitle: Text(_handoffStatusLabel(handoffStatus)),
                          trailing: transitionActions.isEmpty
                              ? null
                              : Wrap(spacing: 0, children: transitionActions),
                        );
                      }),
                    ],
                  ],
                ],
              ),
            ),
          ),
          const Spacer(),
          Text(
            'Flutter 只显示 Core 返回的状态，不自行推进运行。',
            style: Theme.of(context).textTheme.bodySmall,
          ),
        ],
      ),
    );
  }
}

List<Map<String, dynamic>> _list(Map<String, dynamic> snapshot, String key) {
  final value = snapshot[key];
  if (value is! List) return const <Map<String, dynamic>>[];
  return value.whereType<Map<String, dynamic>>().toList(growable: false);
}

List<Map<String, dynamic>> _projectRoster(
  Map<String, dynamic> snapshot,
  String projectId,
) {
  final agents = <String, Map<String, dynamic>>{
    for (final agent in _list(snapshot, 'agents'))
      if (agent['id']?.toString().isNotEmpty == true)
        agent['id'].toString(): agent,
  };
  return _list(snapshot, 'assignments')
      .where(
        (assignment) =>
            assignment['projectId'] == projectId &&
            assignment['enabled'] == true &&
            assignment['agentId']?.toString().isNotEmpty == true,
      )
      .map((assignment) {
        final id = assignment['agentId'].toString();
        final agent = agents[id];
        return <String, dynamic>{
          'id': id,
          'label': agent?['name']?.toString() ?? id,
        };
      })
      .toList(growable: false);
}

List<Map<String, dynamic>> _projectExecutionRuns(
  Map<String, dynamic> snapshot,
  String projectId,
) => _list(snapshot, 'runs')
    .where((run) {
      final runProjectId = run['projectId']?.toString();
      return runProjectId == null ||
          runProjectId.isEmpty ||
          runProjectId == projectId;
    })
    .where((run) => _executionRunId(run).isNotEmpty)
    .toList(growable: false);

String _executionRunId(Map<String, dynamic> run) =>
    run['id']?.toString() ?? run['executionRunId']?.toString() ?? '';

String _executionRunLabel(Map<String, dynamic> run) {
  final id = _executionRunId(run);
  final status = run['status']?.toString();
  return status == null || status.isEmpty
      ? id
      : '$id · ${_runStatusLabel(status)}';
}

List<Map<String, dynamic>> _agents(Map<String, dynamic> snapshot) =>
    _list(snapshot, 'agents');

List<Map<String, dynamic>> _projectAgents(
  Map<String, dynamic> snapshot,
  String? projectId,
) {
  if (projectId == null || projectId.isEmpty) return const [];
  final agentsById = <String, Map<String, dynamic>>{
    for (final agent in _agents(snapshot))
      if (agent['id']?.toString().isNotEmpty == true)
        agent['id'].toString(): agent,
  };
  return _list(snapshot, 'assignments')
      .where(
        (assignment) =>
            assignment['projectId']?.toString() == projectId &&
            assignment['enabled'] == true,
      )
      .map((assignment) => agentsById[assignment['agentId']?.toString()])
      .whereType<Map<String, dynamic>>()
      .toList(growable: false);
}

Map<String, dynamic>? _projectAssignmentForAgent(
  Map<String, dynamic> snapshot,
  String? projectId,
  String agentId,
) {
  if (projectId == null || projectId.isEmpty || agentId.isEmpty) return null;
  for (final assignment in _list(snapshot, 'assignments')) {
    if (assignment['projectId']?.toString() == projectId &&
        assignment['agentId']?.toString() == agentId) {
      return assignment;
    }
  }
  return null;
}

String? _agentField(Map<String, dynamic> agent, List<String> keys) {
  for (final key in keys) {
    final value = agent[key]?.toString();
    if (value != null && value.isNotEmpty) return value;
  }
  return null;
}

String? _firstProjectAgentId(Map<String, dynamic> snapshot, String projectId) {
  for (final assignment in _list(snapshot, 'assignments')) {
    if (assignment['projectId'] == projectId && assignment['enabled'] == true) {
      final agentId = assignment['agentId']?.toString();
      if (agentId != null && agentId.isNotEmpty) return agentId;
    }
  }
  return null;
}

String _agentStatus(Map<String, dynamic> snapshot, String? agentId) {
  if (agentId == null || agentId.isEmpty) return '待命';
  final runs = _list(
    snapshot,
    'runs',
  ).where((run) => run['agentId'] == agentId).toList(growable: false);
  if (runs.isEmpty) return '待命';
  return _runStatusLabel(runs.last['status']?.toString());
}

String _runStatusLabel(String? status) => switch (status?.toLowerCase()) {
  'pending' => '排队中',
  'assembling' => '准备中',
  'awaiting_approval' => '待确认',
  'running' => '运行中',
  'verifying' => '验证中',
  'completed' => '已完成',
  'failed' => '失败',
  'cancelled' => '已取消',
  'interrupted' => '已中断',
  _ => '待命',
};

String _handoffStatusLabel(String? status) => switch (status?.toLowerCase()) {
  'proposed' => '待审核',
  'approved' => '已批准',
  'rejected' => '已拒绝',
  'dispatched' => '已派发',
  'cancelled' => '已取消',
  'expired' => '已过期',
  _ => '未知',
};

bool _isActiveRunStatus(String status) => switch (status.toLowerCase()) {
  'pending' ||
  'assembling' ||
  'awaiting_approval' ||
  'running' ||
  'verifying' => true,
  _ => false,
};

bool _isRetryableRunStatus(String status) => switch (status.toLowerCase()) {
  'failed' || 'cancelled' || 'interrupted' => true,
  _ => false,
};

/// Finds an active Run already projected by Core for the selected
/// conversation. The Composer uses this only to expose an explicit cancel
/// command; Flutter never derives or mutates the Run state itself.
String? activeRunIdForConversation(
  Map<String, dynamic> snapshot,
  String? conversationId,
) {
  if (conversationId == null || conversationId.isEmpty) return null;
  for (final run in _list(snapshot, 'executionRuns')) {
    final status = run['status']?.toString() ?? '';
    final scope = run['scope'];
    final runConversationId =
        run['conversationId']?.toString() ??
        (scope is Map<String, dynamic>
            ? scope['conversationId']?.toString()
            : null);
    final runId = run['id']?.toString();
    if (runId != null &&
        runId.isNotEmpty &&
        runConversationId == conversationId &&
        _isActiveRunStatus(status)) {
      return runId;
    }
  }
  return null;
}

String _firstName(
  Map<String, dynamic> snapshot,
  String key,
  String fallback, [
  String labelKey = 'name',
]) {
  final values = _list(snapshot, key);
  if (values.isEmpty) return fallback;
  final value = values.first[labelKey]?.toString();
  return value == null || value.isEmpty ? fallback : value;
}
