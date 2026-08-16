import 'package:agenttalk_desktop/gen/l10n.dart';
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
}

Widget _host(Widget child) => MaterialApp(
  theme: ThemeData(useMaterial3: true),
  localizationsDelegates: AppLocalizations.localizationsDelegates,
  supportedLocales: AppLocalizations.supportedLocales,
  locale: const Locale('zh'),
  home: Scaffold(body: child),
);
