import 'package:agenttalk_desktop/ui/projection_entity_dialog.dart';
import 'package:agenttalk_desktop/platform/folder_picker.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  testWidgets('submits a named entity and optional root path', (tester) async {
    String? name;
    String? rootPath;
    await tester.pumpWidget(
      _host(
        ProjectionEntityDialog(
          title: '新建 Project',
          nameLabel: 'Project 名称',
          rootPathLabel: 'Project 根目录',
          onSubmit: (submittedName, submittedRootPath) async {
            name = submittedName;
            rootPath = submittedRootPath;
          },
        ),
      ),
    );

    final fields = find.byType(TextField);
    await tester.enterText(fields.at(0), 'Demo Project');
    await tester.enterText(fields.at(1), r'E:\Workspace\demo');
    await tester.tap(find.text('创建'));
    await tester.pumpAndSettle();

    expect(name, 'Demo Project');
    expect(rootPath, r'E:\Workspace\demo');
    expect(find.byType(ProjectionEntityDialog), findsNothing);
    expect(tester.takeException(), isNull);
  });

  testWidgets('shows validation and callback errors', (tester) async {
    await tester.pumpWidget(
      _host(
        ProjectionEntityDialog(
          title: '新建 Conversation',
          nameLabel: 'Conversation 标题',
          onSubmit: (_, _) async => throw StateError('Core rejected'),
        ),
      ),
    );

    await tester.tap(find.text('创建'));
    await tester.pump();
    expect(find.text('名称不能为空'), findsOneWidget);
    await tester.enterText(find.byType(TextField), 'Conversation');
    await tester.tap(find.text('创建'));
    await tester.pumpAndSettle();
    expect(find.textContaining('Core rejected'), findsOneWidget);
    expect(tester.takeException(), isNull);
  });

  testWidgets(
    'uses the injected folder picker and submits only its root path',
    (tester) async {
      String? submittedRootPath;
      const selectedPath = r'C:\Workspace\picked';
      await tester.pumpWidget(
        _host(
          ProjectionEntityDialog(
            title: '新建 Project',
            nameLabel: 'Project 名称',
            rootPathLabel: 'Project 根目录',
            folderPickerClient: _FakeFolderPickerClient(
              const FolderPickerResult.selected(selectedPath),
            ),
            onSubmit: (name, rootPath) async {
              expect(name, 'Picked Project');
              submittedRootPath = rootPath;
            },
          ),
        ),
      );

      await tester.enterText(find.byType(TextField).first, 'Picked Project');
      await tester.tap(find.byKey(const Key('projection-entity-pick-folder')));
      await tester.pumpAndSettle();

      expect(find.text(selectedPath), findsOneWidget);
      await tester.tap(find.text('创建'));
      await tester.pumpAndSettle();
      expect(submittedRootPath, selectedPath);
      expect(tester.takeException(), isNull);
    },
  );

  testWidgets('shows an explicit picker refusal without choosing a fallback', (
    tester,
  ) async {
    await tester.pumpWidget(
      _host(
        ProjectionEntityDialog(
          title: '新建 Project',
          nameLabel: 'Project 名称',
          rootPathLabel: 'Project 根目录',
          folderPickerClient: _FakeFolderPickerClient(
            const FolderPickerResult.unavailable('测试环境未安装 Windows picker'),
          ),
          onSubmit: (_, _) async {},
        ),
      ),
    );

    await tester.tap(find.byKey(const Key('projection-entity-pick-folder')));
    await tester.pumpAndSettle();

    expect(find.text('测试环境未安装 Windows picker'), findsOneWidget);
    expect(find.byType(TextField).at(1), findsOneWidget);
    expect(
      (tester
              .widget<TextField>(find.byType(TextField).at(1))
              .controller
              ?.text ??
          ''),
      isEmpty,
    );
    expect(tester.takeException(), isNull);
  });
}

class _FakeFolderPickerClient implements FolderPickerClient {
  const _FakeFolderPickerClient(this.result);

  final FolderPickerResult result;

  @override
  Future<FolderPickerResult> pickFolder() async => result;
}

Widget _host(Widget child) => MaterialApp(
  theme: ThemeData(useMaterial3: true),
  home: Scaffold(body: child),
);
