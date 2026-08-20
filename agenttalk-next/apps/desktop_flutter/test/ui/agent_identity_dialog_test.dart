import 'package:agenttalk_desktop/gen/l10n.dart';
import 'package:agenttalk_desktop/ipc/core_ipc_client.dart';
import 'package:agenttalk_desktop/ui/agent_identity_dialog.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  testWidgets('submits the complete agent identity form', (tester) async {
    AgentIdentityInput? submitted;
    await tester.pumpWidget(
      _host(
        AgentIdentityDialog(
          title: '新建智能体',
          initialConnectorId: 'local.codex',
          initialModelId: 'gpt-5-codex',
          knownCatalogModels: const {
            'local.codex': ['gpt-5-codex'],
          },
          onSubmit: (input) async {
            submitted = input;
          },
        ),
      ),
    );
    await tester.enterText(
      find.byWidgetPredicate(
        (widget) =>
            widget is TextField &&
            widget.decoration?.labelText == '显示名称 (Name)',
      ),
      'Agent',
    );
    await tester.enterText(
      find.byWidgetPredicate(
        (widget) =>
            widget is TextField && widget.decoration?.labelText == '角色 (Role)',
      ),
      'builder',
    );
    await tester.enterText(
      find.byWidgetPredicate(
        (widget) =>
            widget is TextField &&
            widget.decoration?.labelText == '专长 (Specialty)',
      ),
      'code',
    );
    await tester.enterText(
      find.byWidgetPredicate(
        (widget) =>
            widget is TextField &&
            widget.decoration?.labelText == '系统提示词 (System Prompt)',
      ),
      'You build safely',
    );
    await tester.tap(find.text('保存'));
    await tester.pumpAndSettle();
    expect(submitted?.name, 'Agent');
    expect(submitted?.connectorId, 'local.codex');
    expect(submitted?.modelId, 'gpt-5-codex');
    expect(find.byType(AgentIdentityDialog), findsNothing);
  });

  testWidgets('rejects incomplete identity', (tester) async {
    await tester.pumpWidget(
      _host(AgentIdentityDialog(title: '新建智能体', onSubmit: (input) async {})),
    );
    await tester.tap(find.text('保存'));
    await tester.pump();
    expect(find.textContaining('不能为空'), findsOneWidget);
    expect(tester.takeException(), isNull);
  });

  testWidgets('loads and merges the live catalog with saved model options', (
    tester,
  ) async {
    final catalogCalls = <String>[];
    final optionCalls = <String>[];
    await tester.pumpWidget(
      _host(
        AgentIdentityDialog(
          title: '编辑智能体',
          initialConnectorId: 'local.codex',
          knownCatalogModels: const {'local.codex': <String>[]},
          connectorModelsLoader: (connectorId) async {
            catalogCalls.add(connectorId);
            return _catalog(models: const ['codex-live-a', 'codex-live-b']);
          },
          identityModelOptionsLoader: (connectorId) async {
            optionCalls.add(connectorId);
            return const [
              IdentityModelOptionMetadata(
                id: 'saved-option-1',
                target: IdentityModelTarget(
                  identityScope: 'base_agent',
                  agentId: 'agent-1',
                ),
                modelId: 'codex-saved',
                displayName: 'Codex saved',
                connectorId: 'local.codex',
                source: 'manual',
                availability: 'unverified',
                isDefault: true,
                sortOrder: 0,
              ),
            ];
          },
          onSubmit: (input) async {},
        ),
      ),
    );
    await tester.pumpAndSettle();

    expect(catalogCalls, ['local.codex']);
    expect(optionCalls, ['local.codex']);
    expect(find.textContaining('已从 Connector 获取 2 个模型'), findsOneWidget);
    expect(find.text('codex-saved'), findsOneWidget);

    await tester.tap(find.byKey(const ValueKey('agent-identity-model-menu')));
    await tester.pumpAndSettle();
    expect(find.text('codex-live-a'), findsOneWidget);
    expect(find.text('codex-live-b'), findsOneWidget);
    expect(find.text('codex-saved'), findsWidgets);
  });

  testWidgets('empty live catalog keeps manual entry and marks it unverified', (
    tester,
  ) async {
    await tester.pumpWidget(
      _host(
        AgentIdentityDialog(
          title: '编辑智能体',
          initialConnectorId: 'local.codex',
          connectorModelsLoader: (connectorId) async => _catalog(),
          identityModelOptionsLoader: (connectorId) async => const [],
          onSubmit: (input) async {},
        ),
      ),
    );
    await tester.pumpAndSettle();

    expect(find.textContaining('没有提供可验证的模型目录'), findsOneWidget);
    await tester.enterText(_modelTextField(), 'gpt-custom-manual');
    await tester.pump();
    expect(
      tester.widget<TextField>(_modelTextField()).controller?.text,
      'gpt-custom-manual',
    );
    expect(find.text('已手动指定 (未验证)'), findsOneWidget);
    expect(tester.takeException(), isNull);
  });

  testWidgets(
    'authentication failure explains recovery without blocking input',
    (tester) async {
      await tester.pumpWidget(
        _host(
          AgentIdentityDialog(
            title: '编辑智能体',
            initialConnectorId: 'local.codex',
            connectorModelsLoader: (connectorId) async =>
                throw const CoreIpcException(
                  'runtime authentication failed',
                  code: 'CONNECTOR_RUNTIME_AUTHENTICATION_FAILED',
                ),
            identityModelOptionsLoader: (connectorId) async => const [],
            onSubmit: (input) async {},
          ),
        ),
      );
      await tester.pumpAndSettle();

      expect(find.textContaining('需要先完成认证'), findsOneWidget);
      expect(find.text('重试'), findsOneWidget);
      await tester.enterText(_modelTextField(), 'manual-after-auth-error');
      await tester.pump();
      expect(
        tester.widget<TextField>(_modelTextField()).controller?.text,
        'manual-after-auth-error',
      );
      expect(find.text('已手动指定 (未验证)'), findsOneWidget);
      expect(tester.takeException(), isNull);
    },
  );
}

Finder _modelTextField() =>
    find.byKey(const ValueKey('agent-identity-model-field'));

ConnectorModelCatalog _catalog({List<String> models = const <String>[]}) =>
    ConnectorModelCatalog(
      schemaVersion: 'connector.models.v1',
      scopeId: 'desktop',
      connectorId: 'local.codex',
      runtimeTypeName: 'codex',
      catalogRevision: 7,
      defaultModelId: models.isEmpty ? null : models.first,
      models: models,
      modelMetadata: models
          .map(
            (modelId) => ConnectorModelMetadata(
              modelId: modelId,
              availability: 'available',
              capabilities: const ConnectorModelCapabilities(
                streaming: true,
                cancel: true,
                filesystem: true,
                shell: true,
              ),
            ),
          )
          .toList(growable: false),
      availability: 'available',
    );

Widget _host(Widget child) => MaterialApp(
  theme: ThemeData(useMaterial3: true),
  localizationsDelegates: AppLocalizations.localizationsDelegates,
  supportedLocales: AppLocalizations.supportedLocales,
  locale: const Locale('zh'),
  home: Scaffold(body: child),
);
