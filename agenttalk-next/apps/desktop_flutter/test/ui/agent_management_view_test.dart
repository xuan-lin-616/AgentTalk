import 'package:agenttalk_desktop/ui/workbench/agent_management_view.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  testWidgets('no project shows actionable project guidance', (tester) async {
    await tester.pumpWidget(
      MaterialApp(
        home: Scaffold(
          body: AgentManagementView(
            snapshot: {},
            projectId: null,
            onCreateProject: () {},
            onSelectProject: () {},
          ),
        ),
      ),
    );
    expect(find.text('还没有选择项目'), findsOneWidget);
    expect(find.text('创建项目'), findsOneWidget);
    expect(find.text('选择项目'), findsOneWidget);
  });

  testWidgets('empty roster shows scan guidance', (tester) async {
    await tester.pumpWidget(
      MaterialApp(
        home: Scaffold(
          body: AgentManagementView(
            snapshot: {
              'projects': [
                {'id': 'project-1', 'name': 'P1'},
              ],
              'agents': <Map<String, dynamic>>[],
              'assignments': <Map<String, dynamic>>[],
              'runs': <Map<String, dynamic>>[],
            },
            projectId: 'project-1',
            onScanLocal: () {},
            onAdd: () {},
          ),
        ),
      ),
    );
    expect(find.text('当前项目还没有智能体'), findsOneWidget);
    expect(find.text('扫描本地智能体'), findsWidgets);
    expect(find.text('创建智能体'), findsWidgets);
  });

  testWidgets('real roster renders cards and counts from snapshot', (
    tester,
  ) async {
    await tester.pumpWidget(
      MaterialApp(
        home: Scaffold(
          body: AgentManagementView(
            snapshot: {
              'agents': [
                {
                  'id': 'agent-1',
                  'name': 'Alpha',
                  'role': 'collector',
                  'specialty': 'research',
                },
                {
                  'id': 'agent-2',
                  'name': 'Beta',
                  'role': 'writer',
                  'specialty': 'copy',
                },
              ],
              'assignments': [
                {
                  'projectId': 'project-1',
                  'agentId': 'agent-1',
                  'enabled': true,
                },
                {
                  'projectId': 'project-1',
                  'agentId': 'agent-2',
                  'enabled': true,
                },
              ],
              'runs': <Map<String, dynamic>>[],
            },
            projectId: 'project-1',
          ),
        ),
      ),
    );
    expect(find.text('Alpha'), findsOneWidget);
    expect(find.text('Beta'), findsOneWidget);
    expect(find.text('就绪'), findsWidgets);
    expect(find.text('已发现'), findsOneWidget);
    expect(find.text('2'), findsNWidgets(2));
  });
}
