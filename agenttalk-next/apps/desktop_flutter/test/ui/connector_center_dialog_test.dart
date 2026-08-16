import 'package:agenttalk_desktop/ipc/core_ipc_client.dart';
import 'package:agenttalk_desktop/ui/connector_center_dialog.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  testWidgets('Connector editor returns metadata without secret fields', (
    tester,
  ) async {
    await tester.pumpWidget(_host());
    final resultFuture = showDialog<ConnectorProfileMetadata>(
      context: tester.element(find.byType(Scaffold)),
      builder: (context) =>
          const ConnectorProfileEditorDialog(scopeId: 'desktop'),
    );

    await tester.pumpAndSettle();
    expect(find.text('认证环境变量名（可选）'), findsOneWidget);
    expect(find.textContaining('不会读取或保存变量值'), findsOneWidget);
    expect(find.text('Token'), findsNothing);
    expect(find.text('Endpoint'), findsNothing);
    expect(find.text('Header'), findsNothing);

    final fields = find.byType(TextFormField);
    await tester.enterText(fields.at(1), 'connector-1');
    await tester.enterText(fields.at(2), 'Local Mock');
    await tester.enterText(fields.at(3), 'mock');
    await tester.enterText(fields.at(4), 'mock');
    await tester.enterText(fields.at(5), 'AGENTTALK_TEST_KEY');
    await tester.tap(find.text('保存到 Core'));
    await tester.pumpAndSettle();

    final result = await resultFuture;
    expect(result, isNotNull);
    expect(result!.scopeId, 'desktop');
    expect(result.connectorId, 'connector-1');
    expect(result.authEnvKey, 'AGENTTALK_TEST_KEY');
    expect(result.toJson().containsKey('token'), isFalse);
    expect(result.toJson().containsKey('endpoint'), isFalse);
  });
}

Widget _host() {
  return MaterialApp(
    theme: ThemeData(useMaterial3: true),
    home: const Scaffold(body: SizedBox.shrink()),
  );
}
