import 'package:agenttalk_desktop/ui/memory_write_dialog.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  testWidgets('submits a scoped Core memory metadata command', (tester) async {
    String? submittedScope;
    String? submittedAgent;
    String? submittedHash;
    bool? submittedConfirmed;
    await tester.pumpWidget(
      _host(
        MemoryWriteDialog(
          initialScopeId: 'conversation-1',
          onSubmit: (scope, agent, hash, confirmed) async {
            submittedScope = scope;
            submittedAgent = agent;
            submittedHash = hash;
            submittedConfirmed = confirmed;
          },
        ),
      ),
    );

    await tester.enterText(find.byType(TextField).at(1), 'agent-1');
    await tester.enterText(
      find.byType(TextField).at(2),
      'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
    );
    await tester.tap(find.text('标记为已确认 Memory'));
    await tester.tap(find.text('保存'));
    await tester.pumpAndSettle();

    expect(submittedScope, 'conversation-1');
    expect(submittedAgent, 'agent-1');
    expect(submittedHash, List.filled(64, 'a').join());
    expect(submittedConfirmed, isTrue);
    expect(find.byType(MemoryWriteDialog), findsNothing);
  });

  testWidgets('rejects a non-SHA-256 content hash', (tester) async {
    var called = false;
    await tester.pumpWidget(
      _host(
        MemoryWriteDialog(
          initialScopeId: 'project-1',
          onSubmit: (scope, agent, hash, confirmed) async => called = true,
        ),
      ),
    );

    await tester.enterText(find.byType(TextField).at(2), 'short');
    await tester.tap(find.text('保存'));
    await tester.pump();

    expect(called, isFalse);
    expect(find.text('请输入 scope 与 64 位十六进制 content hash'), findsOneWidget);
  });
}

Widget _host(Widget child) {
  return MaterialApp(
    theme: ThemeData(useMaterial3: true),
    home: Scaffold(body: child),
  );
}
