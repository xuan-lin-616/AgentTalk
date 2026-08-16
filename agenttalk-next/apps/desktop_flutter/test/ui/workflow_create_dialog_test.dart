import 'package:agenttalk_desktop/ui/workflow_create_dialog.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  testWidgets('submits a project-rostered workflow step', (tester) async {
    String? name;
    String? kind;
    String? agentId;
    String? promptSupplement;
    await tester.pumpWidget(
      _host(
        WorkflowCreateDialog(
          initialAgentId: 'agent-1',
          onSubmit:
              (submittedName, submittedKind, submittedAgent, prompt) async {
                name = submittedName;
                kind = submittedKind;
                agentId = submittedAgent;
                promptSupplement = prompt;
              },
        ),
      ),
    );

    await tester.enterText(find.byType(TextField).at(0), 'Review workflow');
    await tester.enterText(find.byType(TextField).at(1), 'Review the change');
    await tester.tap(find.text('创建'));
    await tester.pumpAndSettle();

    expect(name, 'Review workflow');
    expect(kind, 'sequential');
    expect(agentId, 'agent-1');
    expect(promptSupplement, 'Review the change');
    expect(find.byType(WorkflowCreateDialog), findsNothing);
    expect(tester.takeException(), isNull);
  });

  testWidgets('requires a project Agent before submitting', (tester) async {
    var called = false;
    await tester.pumpWidget(
      _host(
        WorkflowCreateDialog(
          initialAgentId: null,
          onSubmit: (_, _, _, _) async => called = true,
        ),
      ),
    );

    await tester.enterText(find.byType(TextField).first, 'Workflow');
    await tester.tap(find.text('创建'));
    await tester.pump();

    expect(called, isFalse);
    expect(find.text('当前项目没有可用智能体'), findsOneWidget);
    expect(find.text('请输入工作流名称并选择智能体'), findsOneWidget);
  });

  testWidgets('keeps the dialog open when Core rejects the workflow', (
    tester,
  ) async {
    await tester.pumpWidget(
      _host(
        WorkflowCreateDialog(
          initialAgentId: 'agent-1',
          onSubmit: (_, _, _, _) async => throw StateError('Core rejected'),
        ),
      ),
    );

    await tester.enterText(find.byType(TextField).first, 'Workflow');
    await tester.tap(find.text('创建'));
    await tester.pumpAndSettle();

    expect(find.byType(WorkflowCreateDialog), findsOneWidget);
    expect(find.textContaining('Core rejected'), findsOneWidget);
    expect(tester.takeException(), isNull);
  });
}

Widget _host(Widget child) => MaterialApp(
  theme: ThemeData(useMaterial3: true),
  home: Scaffold(body: child),
);
