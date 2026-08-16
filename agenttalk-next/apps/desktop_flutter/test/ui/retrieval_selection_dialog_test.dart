import 'package:agenttalk_desktop/ui/retrieval_selection_dialog.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  testWidgets('requires and submits an explicit Retrieval source selection', (
    tester,
  ) async {
    List<Map<String, dynamic>>? submitted;
    await tester.pumpWidget(
      MaterialApp(
        home: Scaffold(
          body: RetrievalSelectionDialog(
            sources: const [
              {
                'id': 'source-1',
                'citation': 'README#intro',
                'sha256':
                    'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
              },
            ],
            onSubmit: (value) async => submitted = value,
          ),
        ),
      ),
    );

    await tester.tap(find.byKey(const Key('retrieval-selection-submit')));
    await tester.pump();
    expect(find.byKey(const Key('retrieval-selection-error')), findsOneWidget);

    await tester.tap(find.byKey(const Key('retrieval-selection-source-1')));
    await tester.tap(find.byKey(const Key('retrieval-selection-submit')));
    await tester.pumpAndSettle();
    expect(submitted?.single['id'], 'source-1');
  });

  testWidgets('submits bounded Retrieval feedback choices', (tester) async {
    String? submittedLabel;
    String? submittedReason;
    await tester.pumpWidget(
      MaterialApp(
        home: Scaffold(
          body: RetrievalFeedbackDialog(
            sourceId: 'source-1',
            onSubmit: (label, reason) async {
              submittedLabel = label;
              submittedReason = reason;
            },
          ),
        ),
      ),
    );

    await tester.tap(find.byKey(const Key('retrieval-feedback-submit')));
    await tester.pumpAndSettle();
    expect(submittedLabel, 'helpful');
    expect(submittedReason, 'exact_match');
  });
}
