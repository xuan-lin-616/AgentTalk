import 'package:agenttalk_desktop/gen/l10n.dart';
import 'package:agenttalk_desktop/ui/message_search_dialog.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  testWidgets('searches and renders Core message results', (tester) async {
    final queries = <String>[];
    await tester.pumpWidget(
      _host(
        MessageSearchDialog(
          search: (query) async {
            queries.add(query);
            return const [
              {
                'id': 'message-1',
                'conversationId': 'conversation-1',
                'content': 'Rust search result',
              },
            ];
          },
        ),
      ),
    );

    await tester.enterText(find.byType(TextField), 'Rust');
    await tester.testTextInput.receiveAction(TextInputAction.search);
    await tester.pumpAndSettle();

    expect(queries, ['Rust']);
    expect(find.text('Rust search result'), findsOneWidget);
    expect(find.text('conversation-1'), findsOneWidget);
    expect(tester.takeException(), isNull);
  });

  testWidgets('renders search errors without throwing', (tester) async {
    await tester.pumpWidget(
      _host(
        MessageSearchDialog(
          search: (_) async => throw StateError('Core unavailable'),
        ),
      ),
    );

    await tester.enterText(find.byType(TextField), 'message');
    await tester.tap(find.byTooltip('搜索消息'));
    await tester.pumpAndSettle();

    expect(find.textContaining('搜索失败：'), findsOneWidget);
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
