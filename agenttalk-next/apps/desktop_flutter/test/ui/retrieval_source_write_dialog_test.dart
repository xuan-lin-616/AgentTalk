import 'package:agenttalk_desktop/ui/retrieval_source_write_dialog.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  testWidgets('submits retrieval metadata without source content', (
    tester,
  ) async {
    String? scope;
    String? citation;
    String? hash;
    int? tokenCount;
    await tester.pumpWidget(
      _host(
        RetrievalSourceWriteDialog(
          initialScopeId: 'conversation-1',
          onSubmit:
              (submittedScope, submittedCitation, submittedHash, tokens) async {
                scope = submittedScope;
                citation = submittedCitation;
                hash = submittedHash;
                tokenCount = tokens;
              },
        ),
      ),
    );

    final fields = find.byType(TextField);
    await tester.enterText(fields.at(1), 'docs/README.md#intro');
    await tester.enterText(
      fields.at(2),
      'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
    );
    await tester.enterText(fields.at(3), '128');
    await tester.tap(find.text('保存'));
    await tester.pumpAndSettle();

    expect(scope, 'conversation-1');
    expect(citation, 'docs/README.md#intro');
    expect(hash, List.filled(64, 'a').join());
    expect(tokenCount, 128);
    expect(find.byType(RetrievalSourceWriteDialog), findsNothing);
    expect(tester.takeException(), isNull);
  });

  testWidgets('rejects an invalid retrieval hash', (tester) async {
    var called = false;
    await tester.pumpWidget(
      _host(
        RetrievalSourceWriteDialog(
          initialScopeId: 'project-1',
          onSubmit: (_, _, _, _) async => called = true,
        ),
      ),
    );

    await tester.enterText(find.byType(TextField).at(1), 'citation');
    await tester.enterText(find.byType(TextField).at(2), 'short');
    await tester.tap(find.text('保存'));
    await tester.pump();

    expect(called, isFalse);
    expect(find.textContaining('64 位十六进制 SHA-256'), findsOneWidget);
  });
}

Widget _host(Widget child) => MaterialApp(
  theme: ThemeData(useMaterial3: true),
  home: Scaffold(body: child),
);
