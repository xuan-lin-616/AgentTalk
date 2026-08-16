import 'package:agenttalk_desktop/ui/context_inspector_drawer.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  testWidgets('renders populated collections, details, and selected source', (
    tester,
  ) async {
    await _withSemantics(tester, () async {
      await tester.pumpWidget(
        _host(
          const ContextInspectorDrawer(
            snapshot: _populatedSnapshot,
            selectedSourceId: 'source-1',
          ),
        ),
      );

      expect(find.text('上下文检查器'), findsOneWidget);
      expect(find.text('上下文清单'), findsOneWidget);
      expect(find.text('记忆'), findsOneWidget);
      expect(find.text('检索来源'), findsOneWidget);
      expect(find.text('摘要'), findsOneWidget);
      expect(find.text('制品'), findsOneWidget);
      expect(find.text('附件'), findsOneWidget);
      expect(
        find.byKey(const ValueKey('context-inspector-count-contextManifests')),
        findsOneWidget,
      );
      expect(
        find.byKey(const ValueKey('context-inspector-count-memories')),
        findsOneWidget,
      );
      expect(
        find.byKey(const ValueKey('context-inspector-count-retrievalSources')),
        findsOneWidget,
      );
      expect(
        find.byKey(const ValueKey('context-inspector-count-summaries')),
        findsOneWidget,
      );
      expect(
        find.byKey(const ValueKey('context-inspector-count-artifacts')),
        findsOneWidget,
      );
      expect(
        find.byKey(const ValueKey('context-inspector-count-attachments')),
        findsOneWidget,
      );
      expect(find.text('Conversation context'), findsOneWidget);
      expect(find.text('Windows First'), findsOneWidget);
      expect(find.text('Project documents'), findsOneWidget);
      expect(find.text('已选来源'), findsOneWidget);
      expect(find.bySemanticsLabel('上下文检查器抽屉'), findsOneWidget);
      expect(
        find.bySemanticsLabel(RegExp('检索来源 条目 Project documents，已选来源')),
        findsOneWidget,
      );
      expect(tester.takeException(), isNull);
      expect(tester.takeException(), isNull);
    });
  });

  testWidgets(
    'renders content for Memory and Summary but hides it for Manifest',
    (tester) async {
      await tester.pumpWidget(
        _host(
          const ContextInspectorDrawer(
            snapshot: <String, dynamic>{
              'contextManifests': <Map<String, dynamic>>[
                {
                  'id': 'manifest-1',
                  'name': 'Manifest',
                  'content': 'Hidden content',
                },
              ],
              'memories': <Map<String, dynamic>>[
                {
                  'id': 'memory-1',
                  'title': 'Memory',
                  'content': 'Visible memory content',
                },
              ],
              'summaries': <Map<String, dynamic>>[
                {
                  'id': 'summary-1',
                  'title': 'Summary',
                  'text': 'Visible summary text',
                },
              ],
            },
          ),
        ),
      );

      expect(find.text('Hidden content'), findsNothing);
      expect(find.text('Visible memory content'), findsOneWidget);
      expect(find.text('Visible summary text'), findsOneWidget);
      expect(tester.takeException(), isNull);
    },
  );

  testWidgets('shows an explicit empty state for every collection', (
    tester,
  ) async {
    await tester.pumpWidget(
      _host(
        const ContextInspectorDrawer(
          snapshot: <String, dynamic>{
            'contextManifests': <dynamic>[],
            'memories': <dynamic>[],
            'retrievalSources': <dynamic>[],
            'summaries': <dynamic>[],
          },
        ),
      ),
    );

    expect(find.text('暂无上下文清单'), findsOneWidget);
    expect(find.text('暂无记忆'), findsOneWidget);
    expect(find.text('暂无检索来源'), findsOneWidget);
    expect(find.text('暂无摘要'), findsOneWidget);
    expect(find.text('暂无制品'), findsOneWidget);
    expect(find.text('暂无附件'), findsOneWidget);
    expect(
      find.byKey(const ValueKey('context-inspector-empty-contextManifests')),
      findsOneWidget,
    );
    expect(
      find.byKey(const ValueKey('context-inspector-empty-memories')),
      findsOneWidget,
    );
    expect(
      find.byKey(const ValueKey('context-inspector-empty-retrievalSources')),
      findsOneWidget,
    );
    expect(
      find.byKey(const ValueKey('context-inspector-empty-summaries')),
      findsOneWidget,
    );
    expect(
      find.byKey(const ValueKey('context-inspector-empty-artifacts')),
      findsOneWidget,
    );
    expect(
      find.byKey(const ValueKey('context-inspector-empty-attachments')),
      findsOneWidget,
    );
    expect(tester.takeException(), isNull);
  });

  testWidgets('communicates loading with progress and semantics', (
    tester,
  ) async {
    await _withSemantics(tester, () async {
      await tester.pumpWidget(
        _host(
          const ContextInspectorDrawer(
            snapshot: <String, dynamic>{},
            loading: true,
          ),
        ),
      );

      expect(
        find.byKey(const ValueKey('context-inspector-loading')),
        findsOneWidget,
      );
      expect(find.text('正在加载上下文快照'), findsOneWidget);
      expect(find.bySemanticsLabel(RegExp('上下文检查器加载中')), findsOneWidget);
      expect(tester.takeException(), isNull);
    });
  });

  testWidgets('renders an error banner without throwing', (tester) async {
    await _withSemantics(tester, () async {
      await tester.pumpWidget(
        _host(
          const ContextInspectorDrawer(
            snapshot: <String, dynamic>{},
            error: 'Projection unavailable',
          ),
        ),
      );

      expect(find.text('错误状态'), findsOneWidget);
      expect(find.text('Projection unavailable'), findsOneWidget);
      expect(
        find.bySemanticsLabel(RegExp('错误状态: Projection unavailable')),
        findsOneWidget,
      );
      expect(tester.takeException(), isNull);
    });
  });

  testWidgets('exposes scoped Retrieval preview entry without global search', (
    tester,
  ) async {
    var opened = false;
    await tester.pumpWidget(
      _host(
        ContextInspectorDrawer(
          snapshot: const <String, dynamic>{},
          projectId: 'project-1',
          conversationId: 'conversation-1',
          agentId: null,
          onPreviewRetrieval: () async => opened = true,
        ),
      ),
    );

    expect(find.text('当前 scope：会话 · conversation-1'), findsOneWidget);
    await tester.tap(
      find.byKey(const ValueKey('context-inspector-retrieval-preview-button')),
    );
    await tester.pump();
    expect(opened, isTrue);
    expect(tester.takeException(), isNull);
  });

  testWidgets('exposes explicit Summary generation action', (tester) async {
    var generated = false;
    await tester.pumpWidget(
      _host(
        ContextInspectorDrawer(
          snapshot: _populatedSnapshot,
          onGenerateSummary: () async => generated = true,
        ),
      ),
    );

    final button = find.byKey(
      const ValueKey('context-inspector-generate-summary'),
    );
    expect(button, findsOneWidget);
    await tester.ensureVisible(button);
    await tester.pumpAndSettle();
    await tester.tap(button);
    await tester.pump();
    expect(generated, isTrue);
    expect(tester.takeException(), isNull);
  });

  testWidgets('scrolls in a narrow dark-theme viewport', (tester) async {
    tester.view.physicalSize = const Size(320, 480);
    tester.view.devicePixelRatio = 1;
    addTearDown(tester.view.resetPhysicalSize);
    addTearDown(tester.view.resetDevicePixelRatio);

    await tester.pumpWidget(
      _host(
        ContextInspectorDrawer(snapshot: _longSnapshot),
        brightness: Brightness.dark,
      ),
    );

    final scrollable = find.byKey(const ValueKey('context-inspector-scroll'));
    expect(scrollable, findsOneWidget);
    await tester.drag(scrollable, const Offset(0, -280));
    await tester.pump();
    expect(find.text('Long retrieval source 7'), findsOneWidget);
    expect(tester.takeException(), isNull);
  });
}

const Map<String, dynamic> _populatedSnapshot = {
  'contextManifests': <Map<String, dynamic>>[
    {
      'id': 'context-1',
      'name': 'Conversation context',
      'description': 'Current conversation context',
    },
  ],
  'memories': <Map<String, dynamic>>[
    {'id': 'memory-1', 'title': 'User preference', 'summary': 'Windows First'},
  ],
  'retrievalSources': <Map<String, dynamic>>[
    {
      'id': 'source-1',
      'name': 'Project documents',
      'type': 'local',
      'summary': 'Selected project source',
    },
    {'id': 'source-2', 'name': 'Conversation notes'},
  ],
  'summaries': <Map<String, dynamic>>[
    {
      'id': 'summary-1',
      'title': 'Conversation summary',
      'summary': 'A short summary',
    },
  ],
  'attachments': <Map<String, dynamic>>[
    {
      'attachmentId': 'attachment-1',
      'artifactId': 'artifact-1',
      'messageId': 'message-1',
      'ordinal': 0,
      'fileName': 'example.txt',
      'size': 12,
    },
  ],
};

final Map<String, dynamic> _longSnapshot = {
  'contextManifests': List<Map<String, dynamic>>.generate(
    4,
    (index) => {'id': 'context-$index', 'name': 'Context manifest $index'},
  ),
  'memories': List<Map<String, dynamic>>.generate(
    4,
    (index) => {'id': 'memory-$index', 'name': 'Memory $index'},
  ),
  'retrievalSources': List<Map<String, dynamic>>.generate(
    8,
    (index) => {'id': 'source-$index', 'name': 'Long retrieval source $index'},
  ),
  'summaries': List<Map<String, dynamic>>.generate(
    4,
    (index) => {'id': 'summary-$index', 'name': 'Summary $index'},
  ),
};

Widget _host(Widget child, {Brightness brightness = Brightness.light}) {
  return MaterialApp(
    theme: ThemeData(
      useMaterial3: true,
      colorScheme: ColorScheme.fromSeed(
        seedColor: const Color(0xff5558d9),
        brightness: brightness,
      ),
      fontFamily: 'Segoe UI',
    ),
    home: Scaffold(body: child),
  );
}

Future<void> _withSemantics(
  WidgetTester tester,
  Future<void> Function() body,
) async {
  final handle = tester.ensureSemantics();
  try {
    await body();
  } finally {
    handle.dispose();
  }
}
