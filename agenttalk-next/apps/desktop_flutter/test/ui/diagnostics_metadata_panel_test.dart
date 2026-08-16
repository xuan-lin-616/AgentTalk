import 'package:agenttalk_desktop/gen/l10n.dart';
import 'package:agenttalk_desktop/ipc/protocol_v1.dart';
import 'package:agenttalk_desktop/ui/diagnostics_metadata_panel.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  testWidgets('renders Core health and projection metadata from a snapshot', (
    tester,
  ) async {
    await tester.pumpWidget(
      _host(
        const DiagnosticsMetadataPanel(
          snapshot: <String, dynamic>{
            'workflows': <Map<String, dynamic>>[
              {'id': 'workflow-1'},
            ],
            'modelSnapshots': <Map<String, dynamic>>[
              {'id': 'model-1'},
              {'id': 'model-2'},
            ],
            'summaries': <Map<String, dynamic>>[
              {'id': 'summary-1'},
              {'id': 'summary-2'},
              {'id': 'summary-3'},
            ],
            'memories': <Map<String, dynamic>>[
              {'id': 'memory-1'},
              {'id': 'memory-2'},
              {'id': 'memory-3'},
              {'id': 'memory-4'},
            ],
            'retrievalSources': <Map<String, dynamic>>[
              {'id': 'retrieval-1'},
              {'id': 'retrieval-2'},
              {'id': 'retrieval-3'},
              {'id': 'retrieval-4'},
              {'id': 'retrieval-5'},
            ],
            'contextManifests': <Map<String, dynamic>>[
              {'id': 'context-1'},
              {'id': 'context-2'},
              {'id': 'context-3'},
              {'id': 'context-4'},
              {'id': 'context-5'},
              {'id': 'context-6'},
            ],
          },
          health: RuntimeHealth(
            status: 'Core projection ready',
            safeDetails: {'runtime': 'Core'},
          ),
        ),
      ),
    );

    expect(find.text('高级诊断'), findsOneWidget);
    expect(find.text('运行状态与投影元数据'), findsOneWidget);
    expect(find.text('Core'), findsOneWidget);
    expect(find.text('已连接'), findsOneWidget);
    expect(find.text('工作流'), findsOneWidget);
    expect(find.text('模型快照'), findsOneWidget);
    expect(find.text('摘要'), findsOneWidget);
    expect(find.text('记忆'), findsOneWidget);
    expect(find.text('检索来源'), findsOneWidget);
    expect(find.text('Context 清单'), findsOneWidget);
    for (final count in ['1', '2', '3', '4', '5', '6']) {
      expect(find.text(count), findsOneWidget);
    }
    expect(tester.takeException(), isNull);
  });

  testWidgets('keeps the panel usable with an empty or partial snapshot', (
    tester,
  ) async {
    await tester.pumpWidget(
      _host(
        const DiagnosticsMetadataPanel(
          snapshot: <String, dynamic>{'memories': 'unexpected shape'},
          projectionStatus: 'Core projection unavailable',
        ),
      ),
    );

    expect(find.text('Core'), findsOneWidget);
    expect(find.text('不可用'), findsOneWidget);
    expect(find.text('0'), findsNWidgets(9));
    expect(tester.takeException(), isNull);
  });

  testWidgets('shows bounded startup diagnostics and exposes retry', (
    tester,
  ) async {
    var retries = 0;
    await tester.pumpWidget(
      _host(
        DiagnosticsMetadataPanel(
          snapshot: const <String, dynamic>{},
          projectionStatus: 'AgentTalk 数据库版本不兼容，请打开诊断查看详情。',
          diagnosticDetails: 'category=database_schema_incompatible',
          onRetryStartup: () => retries += 1,
        ),
      ),
    );

    expect(find.text('技术诊断详情'), findsOneWidget);
    expect(find.text('category=database_schema_incompatible'), findsOneWidget);
    await tester.tap(find.text('重试启动'));
    expect(retries, 1);
    expect(tester.takeException(), isNull);
  });

  testWidgets('renders Core connector health and model candidates', (
    tester,
  ) async {
    await tester.pumpWidget(
      _host(
        const ConnectorModelStatusPanel(
          snapshot: <String, dynamic>{
            'modelCandidates': <Map<String, dynamic>>[
              {
                'connectorId': 'mock-core',
                'modelId': 'mock-model-1',
                'available': true,
              },
            ],
          },
          health: RuntimeHealth(
            safeDetails: {
              'connectors': <Map<String, dynamic>>[
                {'name': 'Mock Core', 'status': 'ready', 'ok': true},
              ],
            },
          ),
          runtimeModels: <String, dynamic>{
            'runtimeId': 'mock-runtime',
            'modelMetadata': <Map<String, dynamic>>[
              {'modelId': 'runtime-model-1', 'availability': 'available'},
            ],
          },
        ),
      ),
    );

    expect(find.text('Connector 与模型'), findsOneWidget);
    expect(find.text('Mock Core'), findsOneWidget);
    expect(find.text('就绪'), findsOneWidget);
    expect(find.text('mock-model-1'), findsOneWidget);
    expect(find.text('mock-core · 可用'), findsOneWidget);
    expect(find.text('runtime-model-1'), findsOneWidget);
    expect(find.text('mock-runtime · 可用'), findsOneWidget);
    expect(tester.takeException(), isNull);
  });

  testWidgets('forwards an explicit identity model default selection', (
    tester,
  ) async {
    Map<String, Object?>? request;
    await tester.pumpWidget(
      _host(
        DiagnosticsMetadataPanel(
          snapshot: const <String, dynamic>{
            'identityModelOptions': <Map<String, dynamic>>[
              {
                'scope': 'project_agent',
                'agentId': 'agent-1',
                'projectId': 'project-1',
                'conversationId': null,
                'connectorId': 'connector-1',
                'modelId': 'model-a',
                'isDefault': true,
              },
              {
                'scope': 'project_agent',
                'agentId': 'agent-1',
                'projectId': 'project-1',
                'conversationId': null,
                'connectorId': 'connector-1',
                'modelId': 'model-b',
                'isDefault': false,
              },
            ],
          },
          onSetModelDefault:
              ({
                required identityScope,
                required agentId,
                projectId,
                conversationId,
                required connectorId,
                required modelId,
              }) async {
                request = <String, Object?>{
                  'identityScope': identityScope,
                  'agentId': agentId,
                  'projectId': projectId,
                  'conversationId': conversationId,
                  'connectorId': connectorId,
                  'modelId': modelId,
                };
              },
        ),
      ),
    );

    final dropdown = find.byType(DropdownButton<String>);
    await tester.ensureVisible(dropdown);
    await tester.tap(dropdown);
    await tester.pumpAndSettle();
    final modelB = find.text('model-b').last;
    await tester.ensureVisible(modelB);
    await tester.tap(modelB);
    await tester.pumpAndSettle();

    expect(request, <String, Object?>{
      'identityScope': 'project_agent',
      'agentId': 'agent-1',
      'projectId': 'project-1',
      'conversationId': null,
      'connectorId': 'connector-1',
      'modelId': 'model-b',
    });
    expect(
      tester
          .widget<DropdownButton<String>>(find.byType(DropdownButton<String>))
          .value,
      'model-b',
    );
    expect(tester.takeException(), isNull);
  });
}

Widget _host(Widget child) {
  return MaterialApp(
    theme: ThemeData(
      useMaterial3: true,
      colorScheme: ColorScheme.fromSeed(
        seedColor: const Color(0xff5558d9),
        brightness: Brightness.light,
      ),
      fontFamily: 'Segoe UI',
      scaffoldBackgroundColor: const Color(0xfff8fafc),
    ),
    localizationsDelegates: AppLocalizations.localizationsDelegates,
    supportedLocales: AppLocalizations.supportedLocales,
    locale: const Locale('zh'),
    home: Scaffold(
      body: SingleChildScrollView(
        padding: const EdgeInsets.all(24),
        child: child,
      ),
    ),
  );
}
