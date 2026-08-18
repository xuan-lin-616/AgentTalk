import 'package:agenttalk_desktop/ipc/core_ipc_client.dart';
import 'package:agenttalk_desktop/ui/workbench/model_binding_matrix_view.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  testWidgets('ModelBindingMatrixView shows an explicit empty state', (
    tester,
  ) async {
    final client = CoreIpcClient.forTesting(
      read: (_) async => throw StateError('no ipc'),
      write: (_) {},
      close: () {},
    );
    addTearDown(client.close);
    await tester.pumpWidget(
      MaterialApp(
        home: Scaffold(
          body: ModelBindingMatrixView(
            client: client,
            sessionId: 'session-1',
            projectId: null,
            agents: const [],
          ),
        ),
      ),
    );
    await tester.pumpAndSettle();
    expect(find.text('模型绑定矩阵'), findsOneWidget);
    expect(find.text('暂无模型绑定'), findsOneWidget);
    expect(find.text('请先选择项目'), findsOneWidget);
  });
}
