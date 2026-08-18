import 'package:agenttalk_desktop/ui/workbench/simple_markdown_text.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  testWidgets('SimpleMarkdownText renders code and bold blocks', (
    tester,
  ) async {
    await tester.pumpWidget(
      const MaterialApp(
        home: Scaffold(
          body: SimpleMarkdownText(
            text: '开头 **加粗** `inline_code`\n\n```dart\nint a = 1;\n```',
          ),
        ),
      ),
    );
    expect(find.textContaining('int a = 1;'), findsOneWidget);
    expect(find.textContaining('开头'), findsOneWidget);
  });

  testWidgets('SimpleMarkdownText redacts paths and credentials', (
    tester,
  ) async {
    await tester.pumpWidget(
      const MaterialApp(
        home: Scaffold(
          body: SimpleMarkdownText(
            text: r'token=abc123 C:\Users\me\secret.txt',
          ),
        ),
      ),
    );
    expect(find.textContaining('abc123'), findsNothing);
    expect(find.textContaining(r'C:\Users'), findsNothing);
  });
}
