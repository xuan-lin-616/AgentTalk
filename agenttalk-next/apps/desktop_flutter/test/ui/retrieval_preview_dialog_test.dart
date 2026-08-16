import 'dart:async';

import 'package:agenttalk_desktop/ipc/retrieval_preview.dart';
import 'package:agenttalk_desktop/ui/retrieval_preview_dialog.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  testWidgets('does not search until submit and renders loading then success', (
    tester,
  ) async {
    final calls = <RetrievalPreviewRequest>[];
    final response = Completer<RetrievalPreviewResult>();
    await tester.pumpWidget(
      _host(
        RetrievalPreviewDialog(
          project: 'project-1',
          conversation: 'conversation-1',
          agent: null,
          scope: 'conversation',
          preview: (request) {
            calls.add(request);
            return response.future;
          },
        ),
      ),
    );

    expect(calls, isEmpty);
    await tester.enterText(
      find.byKey(const Key('retrieval-preview-query')),
      'Windows First',
    );
    await tester.tap(find.byKey(const Key('retrieval-preview-submit')));
    await tester.pump();
    expect(calls.single.scope, 'conversation');
    expect(calls.single.conversation, 'conversation-1');
    expect(find.byKey(const Key('retrieval-preview-loading')), findsOneWidget);

    response.complete(
      RetrievalPreviewResult(
        retrievalVersion: 'retrieval.preview.v1',
        queryHash: 'hash-1',
        capabilities: const {'openSource': false},
        hits: [
          RetrievalPreviewHit(
            hitId: 'hit-1',
            sourceType: 'memory',
            sourceObjectId: 'memory-1',
            snippet: 'x' * 400,
            matchReason: 'semantic_match',
            score: .8,
            estimatedTokens: 20,
            permissionDecision: 'allowed',
          ),
        ],
      ),
    );
    await tester.pumpAndSettle();

    expect(find.byKey(const Key('retrieval-preview-success')), findsOneWidget);
    expect(find.textContaining('检索版本：retrieval.preview.v1'), findsOneWidget);
    expect(find.text('允许'), findsOneWidget);
    expect(
      find.byKey(const Key('retrieval-preview-snippet-hit-1')),
      findsOneWidget,
    );
    final snippet = tester.widget<Text>(
      find.byKey(const Key('retrieval-preview-snippet-hit-1')),
    );
    expect(snippet.data!.length, lessThanOrEqualTo(280));
    expect(tester.takeException(), isNull);
  });

  testWidgets('renders empty and error states without a global search', (
    tester,
  ) async {
    var calls = 0;
    await tester.pumpWidget(
      _host(
        RetrievalPreviewDialog(
          project: null,
          conversation: null,
          agent: null,
          scope: 'project',
          preview: (_) async {
            calls += 1;
            return const RetrievalPreviewResult(
              retrievalVersion: 'v1',
              queryHash: 'hash',
              capabilities: <String, dynamic>{},
              hits: <RetrievalPreviewHit>[],
            );
          },
        ),
      ),
    );

    await tester.enterText(
      find.byKey(const Key('retrieval-preview-query')),
      'query',
    );
    await tester.tap(find.byKey(const Key('retrieval-preview-submit')));
    await tester.pumpAndSettle();
    expect(calls, 0);
    expect(find.byKey(const Key('retrieval-preview-error')), findsOneWidget);
    expect(find.textContaining('禁止全局搜索'), findsOneWidget);
  });

  testWidgets('renders an explicit empty result for the selected scope', (
    tester,
  ) async {
    await tester.pumpWidget(
      _host(
        RetrievalPreviewDialog(
          project: 'project-1',
          conversation: null,
          agent: null,
          scope: 'project',
          preview: (_) async => const RetrievalPreviewResult(
            retrievalVersion: 'retrieval.preview.v1',
            queryHash: 'hash-empty',
            capabilities: {'openSource': false},
            hits: [],
          ),
        ),
      ),
    );

    await tester.enterText(
      find.byKey(const Key('retrieval-preview-query')),
      'query',
    );
    await tester.tap(find.byKey(const Key('retrieval-preview-submit')));
    await tester.pumpAndSettle();
    expect(find.byKey(const Key('retrieval-preview-empty')), findsOneWidget);
    expect(find.text('暂无命中'), findsOneWidget);
    expect(find.textContaining('hash-empty'), findsOneWidget);
    expect(tester.takeException(), isNull);
  });

  testWidgets('renders Core errors explicitly', (tester) async {
    await tester.pumpWidget(
      _host(
        RetrievalPreviewDialog(
          project: 'project-1',
          conversation: null,
          agent: null,
          scope: 'project',
          preview: (_) async => throw StateError('Core unavailable'),
        ),
      ),
    );

    await tester.enterText(
      find.byKey(const Key('retrieval-preview-query')),
      'query',
    );
    await tester.tap(find.byKey(const Key('retrieval-preview-submit')));
    await tester.pumpAndSettle();
    expect(find.byKey(const Key('retrieval-preview-error')), findsOneWidget);
    expect(find.textContaining('Core unavailable'), findsOneWidget);
    expect(tester.takeException(), isNull);
  });
}

Widget _host(Widget child) => MaterialApp(
  theme: ThemeData(useMaterial3: true),
  home: Scaffold(body: child),
);
