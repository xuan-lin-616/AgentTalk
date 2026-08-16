import 'dart:async';
import 'dart:typed_data';

import 'package:agenttalk_desktop/gen/l10n.dart';
import 'package:agenttalk_desktop/ipc/core_ipc_client.dart';
import 'package:agenttalk_desktop/ipc/protocol_v1.dart';
import 'package:agenttalk_desktop/main.dart';
import 'package:agenttalk_desktop/platform/folder_picker.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  test(
    'activeRunIdForConversation only returns Core-projected active runs',
    () {
      const snapshot = <String, dynamic>{
        'executionRuns': [
          {
            'id': 'completed-run',
            'conversationId': 'conversation-a',
            'status': 'completed',
          },
          {
            'id': 'active-other',
            'conversationId': 'conversation-b',
            'status': 'running',
          },
          {
            'id': 'active-current',
            'scope': {'conversationId': 'conversation-a'},
            'status': 'verifying',
          },
        ],
      };
      expect(
        activeRunIdForConversation(snapshot, 'conversation-a'),
        'active-current',
      );
      expect(
        activeRunIdForConversation(snapshot, 'conversation-b'),
        'active-other',
      );
      expect(
        activeRunIdForConversation(snapshot, 'conversation-missing'),
        isNull,
      );
    },
  );

  testWidgets('Workspace shell renders the existing information architecture', (
    tester,
  ) async {
    tester.view.physicalSize = const Size(1400, 900);
    tester.view.devicePixelRatio = 1;
    addTearDown(tester.view.resetPhysicalSize);
    addTearDown(tester.view.resetDevicePixelRatio);
    await tester.pumpWidget(const AgentTalkDesktopApp());
    expect(find.text('AgentTalk'), findsOneWidget);
    expect(find.text('开始你的协作对话'), findsOneWidget);
    expect(find.text('@ 接力看板'), findsOneWidget);
  });

  testWidgets('no selected project still shows both add entries', (
    tester,
  ) async {
    tester.view.physicalSize = const Size(1400, 900);
    tester.view.devicePixelRatio = 1;
    addTearDown(tester.view.resetPhysicalSize);
    addTearDown(tester.view.resetDevicePixelRatio);
    final client = CoreIpcClient.forTesting(
      read: (_) => Uint8List(0),
      write: (_) {},
      close: () {},
    );
    addTearDown(() => unawaited(client.close()));
    await tester.pumpWidget(
      AgentTalkDesktopApp(
        initialClient: client,
        initialSessionId: 'golden-session',
        initialSnapshot: const <String, dynamic>{
          'projects': <Map<String, dynamic>>[],
          'conversations': <Map<String, dynamic>>[],
          'agents': <Map<String, dynamic>>[],
          'assignments': <Map<String, dynamic>>[],
          'messages': <Map<String, dynamic>>[],
          'executionRuns': <Map<String, dynamic>>[],
          'workflows': <Map<String, dynamic>>[],
          'runs': <Map<String, dynamic>>[],
          'collaborationRuns': <Map<String, dynamic>>[],
          'handoffs': <Map<String, dynamic>>[],
        },
        enableEventPolling: false,
      ),
    );
    await tester.pumpAndSettle();
    expect(find.text('还没有选择项目'), findsOneWidget);
    expect(find.text('可以先扫描本地智能体；添加前需要创建或选择项目。'), findsOneWidget);
    expect(find.text('添加智能体'), findsAtLeastNWidgets(2));
    expect(find.text('扫描本地智能体'), findsOneWidget);
    expect(tester.takeException(), isNull);
  });

  testWidgets('scan local agents requires a selected project', (tester) async {
    tester.view.physicalSize = const Size(1400, 900);
    tester.view.devicePixelRatio = 1;
    addTearDown(tester.view.resetPhysicalSize);
    addTearDown(tester.view.resetDevicePixelRatio);
    final pipe = _DiscoveryAppPipe(_w7Projection(withProject: false));
    final client = CoreIpcClient.forTesting(
      read: pipe.read,
      write: pipe.write,
      close: pipe.close,
      sessionId: 'session-widget-scan-no-project-123456',
    );
    addTearDown(() => unawaited(client.close()));

    await tester.pumpWidget(
      AgentTalkDesktopApp(
        initialClient: client,
        initialSessionId: 'session-widget-scan-no-project-123456',
        initialSnapshot: pipe.projection,
        enableEventPolling: false,
      ),
    );
    await tester.pumpAndSettle();

    await tester.tap(find.text('扫描本地智能体'));
    await tester.pumpAndSettle();

    // Importing an agent requires a project; the W7 dialog is not opened and
    // no discovery session is started.
    expect(pipe.writtenCommandsAll, isNot(contains('agent.discovery.start')));
    expect(find.text('请先选择项目'), findsWidgets);
    expect(tester.takeException(), isNull);
  });

  testWidgets('Workspace shell exposes labelled controls to semantics', (
    tester,
  ) async {
    final handle = tester.ensureSemantics();
    try {
      await tester.pumpWidget(const AgentTalkDesktopApp());
      await tester.pumpAndSettle();
      expect(find.byTooltip('切换主题'), findsOneWidget);
      expect(find.byTooltip('发送'), findsOneWidget);
    } finally {
      handle.dispose();
    }
  });

  testWidgets('compact shell keeps panel entry points without overflow', (
    tester,
  ) async {
    tester.view.physicalSize = const Size(800, 700);
    tester.view.devicePixelRatio = 1;
    addTearDown(tester.view.resetPhysicalSize);
    addTearDown(tester.view.resetDevicePixelRatio);
    await tester.pumpWidget(const AgentTalkDesktopApp());
    await tester.pumpAndSettle();
    expect(find.byTooltip('智能体面板'), findsOneWidget);
    expect(find.byTooltip('工作流面板'), findsOneWidget);
    expect(tester.takeException(), isNull);
  });

  testWidgets('desktop side panels can be toggled', (tester) async {
    tester.view.physicalSize = const Size(1400, 900);
    tester.view.devicePixelRatio = 1;
    addTearDown(tester.view.resetPhysicalSize);
    addTearDown(tester.view.resetDevicePixelRatio);
    await tester.pumpWidget(const AgentTalkDesktopApp());
    await tester.pumpAndSettle();
    expect(find.byTooltip('智能体面板'), findsOneWidget);
    await tester.tap(find.byTooltip('智能体面板'));
    await tester.pumpAndSettle();
    await tester.tap(find.byTooltip('智能体面板'));
    await tester.pumpAndSettle();
    expect(tester.takeException(), isNull);
  });

  testWidgets('composer Memory entry opens the read-only Context Inspector', (
    tester,
  ) async {
    await tester.pumpWidget(const AgentTalkDesktopApp());
    await tester.tap(find.byTooltip('编写器工具'));
    await tester.pumpAndSettle();
    await tester.tap(find.text('记忆').last);
    await tester.pumpAndSettle();
    expect(find.text('上下文检查器'), findsOneWidget);
    expect(find.text('只读'), findsOneWidget);
    expect(tester.takeException(), isNull);
  });

  testWidgets('composer Attachment entry queues only the selected basename', (
    tester,
  ) async {
    tester.view.physicalSize = const Size(1200, 800);
    tester.view.devicePixelRatio = 1;
    addTearDown(tester.view.resetPhysicalSize);
    addTearDown(tester.view.resetDevicePixelRatio);
    final client = CoreIpcClient.forTesting(
      read: (_) => Uint8List(0),
      write: (_) {},
      close: () {},
    );
    addTearDown(() => unawaited(client.close()));
    await tester.pumpWidget(
      _localizedApp(
        home: WorkspaceShell(
          initialClient: client,
          initialSessionId: 'session-widget-attachment-123456',
          initialSnapshot: const {
            'projects': [
              {'id': 'project-1', 'name': 'Project'},
            ],
            'conversations': [
              {
                'id': 'conversation-1',
                'projectId': 'project-1',
                'title': 'Conversation',
              },
            ],
            'messages': <Object>[],
            'executionRuns': <Object>[],
          },
          filePickerClient: const _SelectedFilePicker(
            r'C:\Private\selected-report.pdf',
          ),
        ),
      ),
    );

    await tester.tap(find.byTooltip('编写器工具'));
    await tester.pump();
    await tester.pump(const Duration(milliseconds: 300));
    await tester.tap(find.text('附件').last);
    await tester.pump();
    await tester.pump(const Duration(milliseconds: 100));

    expect(
      find.byKey(const ValueKey('composer-pending-attachments')),
      findsOneWidget,
    );
    expect(find.text('selected-report.pdf'), findsOneWidget);
    expect(find.text(r'C:\Private\selected-report.pdf'), findsNothing);
    expect(tester.takeException(), isNull);
  });

  testWidgets('Project assignment sheet refreshes after Core mutation', (
    tester,
  ) async {
    tester.view.physicalSize = const Size(1400, 900);
    tester.view.devicePixelRatio = 1;
    addTearDown(tester.view.resetPhysicalSize);
    addTearDown(tester.view.resetDevicePixelRatio);
    final initialSnapshot = <String, dynamic>{
      'projects': [
        {'id': 'project-1', 'name': 'Project'},
      ],
      'agents': [
        {'id': 'agent-1', 'name': 'Manual Agent'},
      ],
      'assignments': <Map<String, dynamic>>[],
      'conversations': <Map<String, dynamic>>[],
      'messages': <Map<String, dynamic>>[],
      'executionRuns': <Map<String, dynamic>>[],
    };
    final pipe = _AssignmentPipe(initialSnapshot);
    final client = CoreIpcClient.forTesting(
      read: pipe.read,
      write: pipe.write,
      close: pipe.close,
    );
    addTearDown(() => unawaited(client.close()));

    await tester.pumpWidget(
      _localizedApp(
        home: WorkspaceShell(
          initialClient: client,
          initialSessionId: 'session-widget-assignment-123456',
          initialSnapshot: initialSnapshot,
        ),
      ),
    );
    await tester.pumpAndSettle();

    await tester.tap(find.text('管理分配').first);
    await tester.pumpAndSettle();
    expect(
      find.byKey(const ValueKey('project-agent-assignment-no-assignments')),
      findsOneWidget,
    );

    await tester.tap(
      find.byKey(const ValueKey('project-agent-assignment-add')),
    );
    await tester.pumpAndSettle();
    await tester.tap(find.text('Manual Agent').last);
    await tester.pumpAndSettle();

    expect(pipe.writtenCommands, contains('project_agent.set'));
    expect(
      find.byKey(const ValueKey('project-agent-assignment-agent-1')),
      findsOneWidget,
    );
    expect(
      find.byKey(const ValueKey('project-agent-assignment-no-assignments')),
      findsNothing,
    );
    expect(tester.takeException(), isNull);
  });

  testWidgets(
    'local scan drives discovery and imports atomically without legacy '
    'create commands',
    (tester) async {
      tester.view.physicalSize = const Size(1400, 900);
      tester.view.devicePixelRatio = 1;
      addTearDown(tester.view.resetPhysicalSize);
      addTearDown(tester.view.resetDevicePixelRatio);
      final pipe = _DiscoveryAppPipe(_w7Projection());
      final client = CoreIpcClient.forTesting(
        read: pipe.read,
        write: pipe.write,
        close: pipe.close,
        sessionId: 'session-widget-scan-123456',
      );
      addTearDown(() => unawaited(client.close()));

      await tester.pumpWidget(
        AgentTalkDesktopApp(
          initialClient: client,
          initialSessionId: 'session-widget-scan-123456',
          initialSnapshot: pipe.projection,
          enableEventPolling: false,
        ),
      );
      await tester.pumpAndSettle();

      await tester.tap(find.text('扫描本地智能体'));
      await tester.pumpAndSettle();
      expect(pipe.writtenCommandsAll, contains('agent.discovery.start'));
      expect(
        pipe.writtenQueriesAll,
        isNot(contains('agent.scan_local')),
        reason: 'the legacy flat scan must not be the entry point',
      );
      expect(find.text('W7 Fixture Agent'), findsWidgets);
      // Nothing is created before an explicit user confirmation.
      expect(pipe.writtenCommandsAll, isNot(contains('agent.create')));

      // Explicit consent gates the initialize-only verification.
      await tester.tap(
        find.byKey(const Key('local-agent-verify-candidate-agent')),
      );
      await tester.pumpAndSettle();
      await tester.tap(
        find.byKey(const Key('local-agent-verify-consent-agree')),
      );
      await tester.pumpAndSettle();
      expect(pipe.writtenCommandsAll, contains('agent.discovery.verify'));

      // After verification the snapshot offers import; confirm the plan.
      await tester.tap(
        find.byKey(const Key('local-agent-import-candidate-verified')),
      );
      await tester.pumpAndSettle();
      expect(find.byKey(const Key('local-agent-import-plan')), findsOneWidget);
      await tester.tap(find.byKey(const Key('local-agent-import-confirm')));
      await tester.pumpAndSettle();
      await tester.tap(find.byKey(const Key('local-agent-import-done')));
      await tester.pumpAndSettle();

      expect(pipe.writtenCommandsAll, contains('agent.import_local'));
      expect(
        pipe.writtenCommandsAll,
        isNot(contains('agent.create')),
        reason: 'W7 import must not fall back to the legacy 3-command flow',
      );
      expect(
        pipe.writtenCommandsAll,
        isNot(contains('agent.model_binding.set')),
      );
      expect(pipe.writtenCommandsAll, isNot(contains('project_agent.set')));
      expect(tester.takeException(), isNull);
    },
  );

  testWidgets('local scan failure shows Chinese retry and can rescan', (
    tester,
  ) async {
    tester.view.physicalSize = const Size(1400, 900);
    tester.view.devicePixelRatio = 1;
    addTearDown(tester.view.resetPhysicalSize);
    addTearDown(tester.view.resetDevicePixelRatio);
    final pipe = _DiscoveryAppPipe(_w7Projection(), failFirstStart: true);
    final client = CoreIpcClient.forTesting(
      read: pipe.read,
      write: pipe.write,
      close: pipe.close,
      sessionId: 'session-widget-scan-failure-123456',
    );
    addTearDown(() => unawaited(client.close()));

    await tester.pumpWidget(
      AgentTalkDesktopApp(
        initialClient: client,
        initialSessionId: 'session-widget-scan-failure-123456',
        initialSnapshot: pipe.projection,
        enableEventPolling: false,
      ),
    );
    await tester.pumpAndSettle();

    await tester.tap(find.text('扫描本地智能体'));
    await tester.pumpAndSettle();
    expect(find.text('重新扫描'), findsOneWidget);
    expect(pipe.startCount, 1);

    await tester.tap(find.text('重新扫描'));
    await tester.pumpAndSettle();
    expect(pipe.startCount, 2);
    expect(find.text('W7 Fixture Agent'), findsWidgets);
    expect(tester.takeException(), isNull);
  });

  testWidgets('connector center refresh calls connector.discover', (
    tester,
  ) async {
    tester.view.physicalSize = const Size(1400, 900);
    tester.view.devicePixelRatio = 1;
    addTearDown(tester.view.resetPhysicalSize);
    addTearDown(tester.view.resetDevicePixelRatio);
    final pipe = _AssignmentPipe(
      _baseProjectSnapshot(),
      connectorDiscoveries: const [
        {
          'connectorId': 'local.codex',
          'runtimeType': 'codex',
          'displayName': 'Codex',
          'availability': 'authentication_required',
          'models': ['codex-model-a'],
          'catalogRevision': 'auth-required',
          'source': r'kind=codex;token=secret;binary=C:\secret\codex.exe',
          'requiresConfiguration': false,
        },
      ],
    );
    final client = CoreIpcClient.forTesting(
      read: pipe.read,
      write: pipe.write,
      close: pipe.close,
    );
    addTearDown(() => unawaited(client.close()));

    await tester.pumpWidget(
      AgentTalkDesktopApp(
        initialClient: client,
        initialSessionId: 'session-widget-connector-discover-123456',
        initialSnapshot: _baseProjectSnapshot(),
        enableEventPolling: false,
      ),
    );
    await tester.pumpAndSettle();

    await tester.tap(find.byTooltip('Connector 中心'));
    await tester.pumpAndSettle();
    expect(pipe.writtenQueries, contains('connector.discover'));
    expect(find.text('Codex'), findsWidgets);
    expect(find.text('需要认证'), findsWidgets);
    expect(find.textContaining('<redacted>'), findsWidgets);
    expect(find.textContaining('secret'), findsNothing);
    expect(tester.takeException(), isNull);
  });

  testWidgets('unassigned agents stay out of side list and mention picker', (
    tester,
  ) async {
    tester.view.physicalSize = const Size(1400, 900);
    tester.view.devicePixelRatio = 1;
    addTearDown(tester.view.resetPhysicalSize);
    addTearDown(tester.view.resetDevicePixelRatio);
    final snapshot = <String, dynamic>{
      'projects': [
        {'id': 'project-1', 'name': 'Project'},
      ],
      'agents': [
        {'id': 'agent-1', 'name': 'Joined Agent'},
        {'id': 'agent-2', 'name': 'Foreign Agent'},
      ],
      'assignments': [
        {'projectId': 'project-1', 'agentId': 'agent-1', 'enabled': true},
      ],
      'conversations': [
        {
          'id': 'conversation-1',
          'projectId': 'project-1',
          'title': 'Conversation',
        },
      ],
      'messages': <Map<String, dynamic>>[],
      'executionRuns': <Map<String, dynamic>>[],
    };
    final client = CoreIpcClient.forTesting(
      read: (_) => Uint8List(0),
      write: (_) {},
      close: () {},
    );
    addTearDown(() => unawaited(client.close()));

    await tester.pumpWidget(
      AgentTalkDesktopApp(
        initialClient: client,
        initialSessionId: 'session-widget-roster-123456',
        initialSnapshot: snapshot,
        enableEventPolling: false,
      ),
    );
    await tester.pumpAndSettle();

    expect(find.text('Joined Agent'), findsOneWidget);
    expect(find.text('Foreign Agent'), findsNothing);

    await tester.tap(find.byTooltip('编写器工具'));
    await tester.pumpAndSettle();
    await tester.tap(find.text('指定智能体').last);
    await tester.pumpAndSettle();
    expect(find.text('Joined Agent'), findsWidgets);
    expect(find.text('Foreign Agent'), findsNothing);
    expect(tester.takeException(), isNull);
  });

  testWidgets('Composer sends an attachment through the local Mock Runtime', (
    tester,
  ) async {
    tester.view.physicalSize = const Size(1400, 900);
    tester.view.devicePixelRatio = 1;
    addTearDown(tester.view.resetPhysicalSize);
    addTearDown(tester.view.resetDevicePixelRatio);
    final initialSnapshot = <String, dynamic>{
      'projects': [
        {'id': 'project-1', 'name': 'Project'},
      ],
      'agents': [
        {'id': 'agent-1', 'name': 'Manual Agent'},
      ],
      'assignments': [
        {
          'projectId': 'project-1',
          'agentId': 'agent-1',
          'enabled': true,
          'workspaceAccess': 'none',
        },
      ],
      'conversations': [
        {
          'id': 'conversation-1',
          'projectId': 'project-1',
          'title': 'Conversation',
        },
      ],
      'messages': <Map<String, dynamic>>[],
      'attachments': <Map<String, dynamic>>[],
      'artifacts': <Map<String, dynamic>>[],
      'executionRuns': <Map<String, dynamic>>[],
    };
    final pipe = _AssignmentPipe(initialSnapshot);
    final client = CoreIpcClient.forTesting(
      read: pipe.read,
      write: pipe.write,
      close: pipe.close,
    );
    addTearDown(() => unawaited(client.close()));

    await tester.pumpWidget(
      _localizedApp(
        home: WorkspaceShell(
          initialClient: client,
          initialSessionId: 'session-widget-mock-runtime-123456',
          initialSnapshot: initialSnapshot,
          filePickerClient: const _SelectedFilePicker(
            r'<AGENTTALK_STATE_ROOT>\artifacts\artifacts\manual-evidence\人工附件-验收-20260807.txt',
          ),
        ),
      ),
    );
    await tester.pumpAndSettle();

    await tester.tap(find.byTooltip('编写器工具'));
    await tester.pumpAndSettle();
    await tester.tap(find.text('附件').last);
    await tester.pumpAndSettle();
    expect(find.text('人工附件-验收-20260807.txt'), findsOneWidget);

    await tester.enterText(find.byType(TextField), 'verify attachment import');
    await tester.tap(find.byTooltip('发送'));
    await tester.pumpAndSettle();

    expect(
      pipe.writtenCommands,
      containsAll(<String>[
        'message.create',
        'attachment.import_file',
        'collaboration.create',
        'execution.start',
      ]),
    );
    expect(pipe.writtenQueries, contains('projection.snapshot'));
    expect(
      pipe.projection['messages'],
      contains(
        predicate<Map<String, dynamic>>((message) {
          return message['content'] == 'verify attachment import';
        }),
      ),
    );
    expect(pipe.projection['attachments'], hasLength(1));
    expect(pipe.projection['artifacts'], hasLength(1));
    expect(
      find.byKey(const ValueKey('composer-pending-attachments')),
      findsNothing,
    );
    expect(tester.takeException(), isNull);
  });
}

class _SelectedFilePicker implements FilePickerClient {
  const _SelectedFilePicker(this.path);

  final String path;

  @override
  Future<FilePickerResult> pickFile() async => FilePickerResult.selected(path);
}

Widget _localizedApp({required Widget home}) => MaterialApp(
  theme: ThemeData(useMaterial3: true),
  localizationsDelegates: AppLocalizations.localizationsDelegates,
  supportedLocales: AppLocalizations.supportedLocales,
  locale: const Locale('zh'),
  home: home,
);

Map<String, dynamic> _baseProjectSnapshot() => <String, dynamic>{
  'projects': [
    {'id': 'project-1', 'name': 'Project'},
  ],
  'agents': <Map<String, dynamic>>[],
  'assignments': <Map<String, dynamic>>[],
  'conversations': <Map<String, dynamic>>[],
  'messages': <Map<String, dynamic>>[],
  'executionRuns': <Map<String, dynamic>>[],
  'workflows': <Map<String, dynamic>>[],
  'runs': <Map<String, dynamic>>[],
  'collaborationRuns': <Map<String, dynamic>>[],
  'handoffs': <Map<String, dynamic>>[],
};

Map<String, dynamic> _w7Projection({bool withProject = true}) =>
    <String, dynamic>{
      if (withProject)
        'projects': [
          {'id': 'project-1', 'name': 'Project'},
        ],
      'agents': <Map<String, dynamic>>[],
      'assignments': <Map<String, dynamic>>[],
      'conversations': <Map<String, dynamic>>[],
      'messages': <Map<String, dynamic>>[],
      'executionRuns': <Map<String, dynamic>>[],
      'workflows': <Map<String, dynamic>>[],
      'runs': <Map<String, dynamic>>[],
      'collaborationRuns': <Map<String, dynamic>>[],
      'handoffs': <Map<String, dynamic>>[],
    };

Map<String, dynamic> _w7CandidateProjection({required String candidateId}) => {
  'candidateId': candidateId,
  'candidate': {
    'candidateId': candidateId,
    'category': 'agent_runtime',
    'connectorId': 'local-$candidateId',
    'runtimeType': 'acp',
    'displayName': 'W7 Fixture Agent',
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
  },
  if (candidateId == 'candidate-verified')
    'verification': {
      'candidateId': candidateId,
      'status': 'verified',
      'compatibilityState': 'compatible',
      'authState': 'not_required',
      'requiresConfiguration': false,
      'protocolMajor': 1,
      'agentInfo': {'name': 'W7 Fixture Agent', 'version': '1.0.0'},
      'capabilities': <String, dynamic>{
        'loadSession': true,
        'promptImage': false,
        'promptAudio': false,
        'promptEmbeddedContext': false,
        'mcpHttp': false,
        'mcpSse': false,
        'supportsLogout': false,
      },
    },
  'lifecycleState': candidateId == 'candidate-verified'
      ? 'verified'
      : 'identified',
};

/// App-level scripted pipe for the W7 discovery/import workflow. Extends the
/// assignment pipe so projection mutations keep working for other UI.
class _DiscoveryAppPipe extends _AssignmentPipe {
  _DiscoveryAppPipe(super.projection, {this.failFirstStart = false});

  bool failFirstStart;
  int startCount = 0;
  bool verified = false;
  final List<String> writtenCommandsAll = [];
  final List<String> writtenQueriesAll = [];

  static const String _scanId = 'scan-w7-app';
  static const String _epoch = 'discovery-epoch-app';

  @override
  Future<void> write(Uint8List frame) async {
    final request = _codec.decodeJson(frame);
    final requestId = request['requestId'] as String? ?? 'request';
    final command = request['command'];
    final query = request['query'];
    if (command is String) writtenCommandsAll.add(command);
    if (query is String) writtenQueriesAll.add(query);
    if (command == 'agent.discovery.start') {
      startCount += 1;
      if (failFirstStart && startCount == 1) {
        _enqueue({
          'kind': 'error',
          'protocol': {'major': protocolMajor, 'minor': 0},
          'requestId': requestId,
          'code': 'SCAN_FAILED',
          'message': 'local scan unavailable',
          'retryable': true,
        });
        return;
      }
      _enqueue({
        'kind': 'response',
        'protocol': {'major': protocolMajor, 'minor': 0},
        'requestId': requestId,
        'ok': true,
        'payload': {
          'scanId': _scanId,
          'accepted': true,
          'state': 'running',
          'eventStream': {
            'streamId': 'local-discovery-events',
            'epoch': _epoch,
          },
        },
      });
      return;
    }
    if (command == 'events.subscribe') {
      _enqueue({
        'kind': 'response',
        'protocol': {'major': protocolMajor, 'minor': 0},
        'requestId': requestId,
        'ok': true,
        'payload': {
          'subscriptionId': 'sub-w7-app',
          'streamId': 'local-discovery-events',
          'cursor': {
            'streamId': 'local-discovery-events',
            'sequence': 0,
            'epoch': _epoch,
          },
          'maxInFlightEvents': 64,
          'maxInFlightBytes': 262144,
        },
      });
      return;
    }
    if (command == 'events.ack' || command == 'events.unsubscribe') {
      _enqueue({
        'kind': 'response',
        'protocol': {'major': protocolMajor, 'minor': 0},
        'requestId': requestId,
        'ok': true,
        'payload': <String, dynamic>{},
      });
      return;
    }
    if (query == 'agent.discovery.snapshot') {
      final candidateId = verified ? 'candidate-verified' : 'candidate-agent';
      _enqueue({
        'kind': 'response',
        'protocol': {'major': protocolMajor, 'minor': 0},
        'requestId': requestId,
        'ok': true,
        'payload': {
          'schemaVersion': 'agent.discovery.snapshot.v1',
          'scanId': _scanId,
          'state': 'completed',
          'candidates': <Map<String, dynamic>>[
            _w7CandidateProjection(candidateId: candidateId),
          ],
          'diagnostics': <Map<String, dynamic>>[],
        },
      });
      return;
    }
    if (command == 'agent.discovery.verify') {
      verified = true;
      _enqueue({
        'kind': 'response',
        'protocol': {'major': protocolMajor, 'minor': 0},
        'requestId': requestId,
        'ok': true,
        'payload': {
          'scanId': _scanId,
          'candidateId': 'candidate-agent',
          'accepted': true,
          'state': 'verifying',
        },
      });
      return;
    }
    if (query == 'agent.import.plan') {
      _enqueue({
        'kind': 'response',
        'protocol': {'major': protocolMajor, 'minor': 0},
        'requestId': requestId,
        'ok': true,
        'payload': {
          'schemaVersion': 'agent.import.plan.v1',
          'planId': 'plan-w7-app',
          'scanId': _scanId,
          'candidateId': 'candidate-verified',
          'targetProjectId': 'project-1',
          'modelSelection': null,
          'actions': <String>[
            'create_connector_profile',
            'create_agent_identity',
          ],
          'connector': {
            'id': 'local-candidate-verified',
            'displayName': 'W7 Connector',
          },
          'adapter': {
            'kind': 'acp',
            'protocolMajor': 1,
            'manifestId': 'org.fixture.w7',
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
      return;
    }
    if (command == 'agent.import_local') {
      _enqueue({
        'kind': 'response',
        'protocol': {'major': protocolMajor, 'minor': 0},
        'requestId': requestId,
        'ok': true,
        'payload': {
          'schemaVersion': 'agent.import_local.v1',
          'importId': 'import-w7-app',
          'connectorId': 'local-candidate-verified',
          'agentId': 'agent-w7-app',
          'projectId': 'project-1',
          'reused': false,
          'eventSequence': 9,
        },
      });
      return;
    }
    await super.write(frame);
  }
}

class _AssignmentPipe {
  _AssignmentPipe(
    this._projection, {
    this.connectorDiscoveries = const <Map<String, dynamic>>[],
  });

  final IpcFrameCodec _codec = const IpcFrameCodec();
  Map<String, dynamic> _projection;
  final List<Map<String, dynamic>> connectorDiscoveries;
  final List<_AssignmentQueuedFrame> _frames = [];
  final List<Completer<void>> _readWaiters = [];
  final List<String> writtenCommands = [];
  final List<String> writtenQueries = [];
  bool _closed = false;

  Map<String, dynamic> get projection => _projection;

  Future<void> write(Uint8List frame) async {
    final request = _codec.decodeJson(frame);
    final requestId = request['requestId'] as String? ?? 'request';
    final command = request['command'];
    if (command is String) writtenCommands.add(command);
    final query = request['query'];
    if (query is String) writtenQueries.add(query);
    Map<String, dynamic> responsePayload = <String, dynamic>{'status': 'ready'};
    if (command == 'agent.create') {
      final payload = request['payload'];
      if (payload is Map) {
        final agents = _projection['agents'];
        final nextAgents = agents is List
            ? agents
                  .whereType<Map>()
                  .map((agent) => Map<String, dynamic>.from(agent))
                  .toList()
            : <Map<String, dynamic>>[];
        nextAgents.add(<String, dynamic>{
          'id': payload['agentId'],
          'name': payload['name'],
          'role': payload['role'],
          'specialty': payload['specialty'],
          'systemPrompt': payload['systemPrompt'],
        });
        _projection = <String, dynamic>{..._projection, 'agents': nextAgents};
      }
      responsePayload = <String, dynamic>{'projection': _projection};
    } else if (command == 'agent.update') {
      responsePayload = <String, dynamic>{'projection': _projection};
    } else if (command == 'agent.model_binding.set') {
      final payload = request['payload'];
      if (payload is Map) {
        final agentId = payload['agentId']?.toString() ?? '';
        final agents = _projection['agents'];
        final nextAgents = agents is List
            ? agents.whereType<Map>().map((agent) {
                final next = Map<String, dynamic>.from(agent);
                if (next['id']?.toString() == agentId) {
                  next['connectorId'] = payload['connectorId'];
                  next['modelId'] = payload['modelId'];
                }
                return next;
              }).toList()
            : <Map<String, dynamic>>[];
        _projection = <String, dynamic>{..._projection, 'agents': nextAgents};
      }
      responsePayload = <String, dynamic>{
        'changed': true,
        'projection': _projection,
      };
    } else if (command == 'project_agent.set') {
      final payload = request['payload'];
      if (payload is Map) {
        final projectId = payload['projectId']?.toString() ?? '';
        final agentId = payload['agentId']?.toString() ?? '';
        final assignments = _projection['assignments'];
        final nextAssignments = assignments is List
            ? assignments
                  .whereType<Map>()
                  .where((assignment) {
                    return assignment['projectId']?.toString() != projectId ||
                        assignment['agentId']?.toString() != agentId;
                  })
                  .map((assignment) => Map<String, dynamic>.from(assignment))
                  .toList()
            : <Map<String, dynamic>>[];
        nextAssignments.add({
          'projectId': projectId,
          'agentId': agentId,
          'enabled': payload['enabled'] == true,
          'workspaceAccess': payload['workspaceAccess']?.toString() ?? 'none',
          if (payload['modelSelectionMode'] != null)
            'modelSelectionMode': payload['modelSelectionMode'],
          if (payload['modelId'] != null) 'modelId': payload['modelId'],
        });
        _projection = <String, dynamic>{
          ..._projection,
          'assignments': nextAssignments,
        };
      }
      responsePayload = <String, dynamic>{
        'changed': true,
        'projection': _projection,
      };
    } else if (command == 'message.create') {
      final payload = request['payload'];
      if (payload is Map) {
        final messages = _projection['messages'];
        final nextMessages = messages is List
            ? messages
                  .whereType<Map>()
                  .map((message) => Map<String, dynamic>.from(message))
                  .toList()
            : <Map<String, dynamic>>[];
        nextMessages.add(<String, dynamic>{
          'id': payload['messageId'],
          'conversationId': payload['conversationId'],
          'senderId': payload['senderId'],
          'sequence': payload['sequence'],
          'content': payload['content'],
        });
        _projection = <String, dynamic>{
          ..._projection,
          'messages': nextMessages,
        };
      }
      responsePayload = <String, dynamic>{
        'created': true,
        'alreadyPresent': false,
        'projection': _projection,
      };
    } else if (command == 'attachment.import_file') {
      final payload = request['payload'];
      if (payload is Map) {
        final attachmentId = payload['attachmentId']?.toString() ?? '';
        final artifactId = payload['artifactId']?.toString() ?? '';
        final messageId = payload['messageId']?.toString() ?? '';
        final sourcePath = payload['sourcePath']?.toString() ?? '';
        final fileName = sourcePath.split(RegExp(r'[/\\]')).last;
        const sha256 =
            'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';
        final artifact = <String, dynamic>{
          'id': artifactId,
          'sha256': sha256,
          'size': 18,
        };
        final attachment = <String, dynamic>{
          'attachmentId': attachmentId,
          'artifactId': artifactId,
          'messageId': messageId,
          'ordinal': payload['ordinal'],
          'fileName': fileName,
          'sha256': sha256,
          'size': 18,
        };
        final attachments = _projection['attachments'];
        final artifacts = _projection['artifacts'];
        final nextAttachments = attachments is List
            ? attachments
                  .whereType<Map>()
                  .map((item) => Map<String, dynamic>.from(item))
                  .toList()
            : <Map<String, dynamic>>[];
        final nextArtifacts = artifacts is List
            ? artifacts
                  .whereType<Map>()
                  .map((item) => Map<String, dynamic>.from(item))
                  .toList()
            : <Map<String, dynamic>>[];
        nextAttachments.add(attachment);
        nextArtifacts.add(artifact);
        _projection = <String, dynamic>{
          ..._projection,
          'attachments': nextAttachments,
          'artifacts': nextArtifacts,
        };
        responsePayload = <String, dynamic>{
          'created': true,
          'alreadyPresent': false,
          'artifactCreated': true,
          'artifactAlreadyPresent': false,
          'bodyStored': true,
          'artifact': artifact,
          'attachment': attachment,
          'projection': _projection,
        };
      }
    } else if (command == 'collaboration.create') {
      responsePayload = <String, dynamic>{
        'created': true,
        'alreadyPresent': false,
        'projection': _projection,
      };
    } else if (command == 'execution.start') {
      final payload = request['payload'];
      responsePayload = <String, dynamic>{
        'run': <String, dynamic>{
          'id': payload is Map ? payload['executionRunId'] : 'mock-run',
          'status': 'completed',
        },
      };
    } else if (query == 'projection.snapshot') {
      responsePayload = _projection;
    } else if (query == 'connector.discover') {
      responsePayload = <String, dynamic>{'discoveries': connectorDiscoveries};
    } else if (query == 'connector.query') {
      responsePayload = const <String, dynamic>{
        'scopeId': 'desktop',
        'connectorProfiles': <Map<String, dynamic>>[],
      };
    }
    _enqueue({
      'kind': 'response',
      'protocol': {'major': protocolMajor, 'minor': 0},
      'requestId': requestId,
      'ok': true,
      'payload': responsePayload,
    });
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
    if (frame.offset == frame.bytes.length) _frames.removeAt(0);
    return chunk;
  }

  Future<void> close() async {
    _closed = true;
    _wakeReaders();
  }

  int get _availableBytes =>
      _frames.isEmpty ? 0 : _frames.first.bytes.length - _frames.first.offset;

  void _enqueue(Map<String, dynamic> value) {
    _frames.add(_AssignmentQueuedFrame(_codec.encodeJson(value)));
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

class _AssignmentQueuedFrame {
  _AssignmentQueuedFrame(this.bytes);

  final Uint8List bytes;
  int offset = 0;
}
