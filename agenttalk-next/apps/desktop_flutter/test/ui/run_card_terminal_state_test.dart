import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:agenttalk_desktop/main.dart';

void main() {
  Widget buildAppWithRunStatus(String status) {
    return MaterialApp(
      home: Scaffold(
        body: RunCard(
          runId: 'run-1',
          agentId: 'agent-1',
          status: status,
          canRetry:
              status == 'failed' ||
              status == 'cancelled' ||
              status == 'interrupted',
          canCancel: false,
          onRetry: (id) {},
          onCancel: (id) {},
          onRerunCurrent: (id) {},
        ),
      ),
    );
  }

  testWidgets('RunCard renders completed state properly', (tester) async {
    final handle = tester.ensureSemantics();
    await tester.pumpWidget(buildAppWithRunStatus('completed'));
    await tester.pumpAndSettle();

    expect(find.text('已完成'), findsOneWidget);
    expect(find.byIcon(Icons.check_circle_outline), findsOneWidget);
    expect(
      find.byWidgetPredicate(
        (widget) =>
            widget is Semantics &&
            widget.properties.label == '运行任务 agent-1: 已完成',
      ),
      findsOneWidget,
    );

    expect(find.byTooltip('取消运行'), findsNothing);
    expect(find.byTooltip('重试'), findsNothing);
    expect(find.byTooltip('按当前设置重新运行'), findsNothing);

    handle.dispose();
  });

  testWidgets('RunCard renders failed state properly', (tester) async {
    final handle = tester.ensureSemantics();
    await tester.pumpWidget(buildAppWithRunStatus('failed'));
    await tester.pumpAndSettle();

    expect(find.text('失败'), findsOneWidget);
    expect(find.byIcon(Icons.error_outline), findsOneWidget);
    expect(
      find.byWidgetPredicate(
        (widget) =>
            widget is Semantics &&
            widget.properties.label == '运行任务 agent-1: 失败',
      ),
      findsOneWidget,
    );

    expect(find.byTooltip('取消运行'), findsNothing);
    expect(find.byTooltip('重试'), findsOneWidget);
    expect(find.byTooltip('按当前设置重新运行'), findsOneWidget);

    handle.dispose();
  });

  testWidgets('RunCard renders cancelled state properly', (tester) async {
    final handle = tester.ensureSemantics();
    await tester.pumpWidget(buildAppWithRunStatus('cancelled'));
    await tester.pumpAndSettle();

    expect(find.text('已取消'), findsOneWidget);
    expect(find.byIcon(Icons.cancel_outlined), findsOneWidget);
    expect(
      find.byWidgetPredicate(
        (widget) =>
            widget is Semantics &&
            widget.properties.label == '运行任务 agent-1: 已取消',
      ),
      findsOneWidget,
    );

    expect(find.byTooltip('取消运行'), findsNothing);
    expect(find.byTooltip('重试'), findsOneWidget);
    expect(find.byTooltip('按当前设置重新运行'), findsOneWidget);

    handle.dispose();
  });

  testWidgets('RunCard renders interrupted state properly', (tester) async {
    final handle = tester.ensureSemantics();
    await tester.pumpWidget(buildAppWithRunStatus('interrupted'));
    await tester.pumpAndSettle();

    expect(find.text('已中断'), findsOneWidget);
    expect(find.byIcon(Icons.pause_circle_outline), findsOneWidget);
    expect(
      find.byWidgetPredicate(
        (widget) =>
            widget is Semantics &&
            widget.properties.label == '运行任务 agent-1: 已中断',
      ),
      findsOneWidget,
    );

    expect(find.byTooltip('取消运行'), findsNothing);
    expect(find.byTooltip('重试'), findsOneWidget);
    expect(find.byTooltip('按当前设置重新运行'), findsOneWidget);

    handle.dispose();
  });
}
