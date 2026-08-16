import 'package:agenttalk_desktop/ui/project_agent_assignment_panel.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  testWidgets('renders the current Project assignments from a snapshot', (
    tester,
  ) async {
    await tester.pumpWidget(
      _host(
        ProjectAgentAssignmentPanel.fromSnapshot(
          snapshot: _snapshot(),
          currentProjectId: 'project-1',
          onSet:
              ({
                required projectId,
                required agentId,
                required enabled,
                required workspaceAccess,
              }) async {},
          onRemove: ({required projectId, required agentId}) async {},
        ),
      ),
    );

    expect(find.text('项目智能体分配'), findsOneWidget);
    expect(find.text('Demo Project · project-1'), findsOneWidget);
    expect(find.text('Builder Agent'), findsOneWidget);
    expect(find.text('工作区权限'), findsOneWidget);
    expect(find.text('只读'), findsOneWidget);
    expect(find.text('启用：是'), findsOneWidget);
    expect(find.text('Reviewer Agent'), findsNothing);
    expect(tester.takeException(), isNull);
  });

  testWidgets('forwards set requests for enabled and workspaceAccess changes', (
    tester,
  ) async {
    final requests = <String>[];
    await tester.pumpWidget(
      _host(
        ProjectAgentAssignmentPanel(
          projects: _projects,
          agents: _agents,
          assignments: _assignments,
          currentProjectId: 'project-1',
          onSet:
              ({
                required projectId,
                required agentId,
                required enabled,
                required workspaceAccess,
              }) async {
                requests.add('$projectId/$agentId/$enabled/$workspaceAccess');
              },
        ),
      ),
    );

    await tester.tap(find.byKey(const ValueKey('enabled-agent-1')));
    await tester.pumpAndSettle();
    expect(requests, ['project-1/agent-1/false/read_only']);

    await tester.tap(
      find.byKey(const ValueKey('workspace-access-dropdown-read_only')),
    );
    await tester.pump();
    await tester.tap(find.text('工作区写入').last);
    await tester.pumpAndSettle();
    expect(requests, [
      'project-1/agent-1/false/read_only',
      'project-1/agent-1/true/workspace_write',
    ]);
  });

  testWidgets('forwards remove and add requests without changing the roster', (
    tester,
  ) async {
    String? removed;
    String? added;
    await tester.pumpWidget(
      _host(
        ProjectAgentAssignmentPanel(
          projects: _projects,
          agents: _agents,
          assignments: _assignments,
          currentProjectId: 'project-1',
          onSet:
              ({
                required projectId,
                required agentId,
                required enabled,
                required workspaceAccess,
              }) async {
                added = '$projectId/$agentId/$enabled/$workspaceAccess';
              },
          onRemove: ({required projectId, required agentId}) async {
            removed = '$projectId/$agentId';
          },
        ),
      ),
    );

    await tester.tap(
      find.byKey(const ValueKey('remove-project-agent-agent-1')),
    );
    await tester.pumpAndSettle();
    expect(removed, 'project-1/agent-1');

    await tester.tap(
      find.byKey(const ValueKey('project-agent-assignment-add')),
    );
    await tester.pumpAndSettle();
    expect(find.text('Reviewer Agent'), findsOneWidget);
    await tester.tap(find.text('Reviewer Agent'));
    await tester.pumpAndSettle();
    expect(added, 'project-1/agent-2/true/none');
    expect(
      find.byKey(const ValueKey('project-agent-assignment-agent-2')),
      findsNothing,
    );
  });

  testWidgets('shows empty and error states', (tester) async {
    await tester.pumpWidget(
      _host(
        const ProjectAgentAssignmentPanel(
          projects: <Map<String, dynamic>>[],
          agents: <Map<String, dynamic>>[],
          assignments: <Map<String, dynamic>>[],
        ),
      ),
    );
    expect(find.text('没有选中的项目'), findsOneWidget);

    await tester.pumpWidget(
      _host(
        const ProjectAgentAssignmentPanel(
          projects: _projects,
          agents: _agents,
          assignments: <Map<String, dynamic>>[],
          currentProjectId: 'project-1',
          error: 'Core projection unavailable',
        ),
      ),
    );
    expect(find.text('错误状态'), findsOneWidget);
    expect(find.text('Core projection unavailable'), findsOneWidget);
    expect(tester.takeException(), isNull);
  });

  testWidgets('communicates loading and disabled semantics', (tester) async {
    await tester.pumpWidget(
      _host(
        const ProjectAgentAssignmentPanel(
          projects: _projects,
          agents: _agents,
          assignments: _assignments,
          currentProjectId: 'project-1',
          loading: true,
          disabled: true,
          disabledReason: 'host is refreshing',
        ),
      ),
    );

    expect(find.byType(LinearProgressIndicator), findsOneWidget);
    expect(find.text('加载中，分配操作暂不可用。'), findsOneWidget);
    expect(find.text('不可用：host is refreshing'), findsOneWidget);
    expect(
      tester
          .widget<Switch>(find.byKey(const ValueKey('enabled-agent-1')))
          .onChanged,
      isNull,
    );
    expect(
      tester
          .widget<PopupMenuButton<String>>(
            find.byKey(const ValueKey('project-agent-assignment-add')),
          )
          .enabled,
      isFalse,
    );
    expect(tester.takeException(), isNull);
  });

  testWidgets('surfaces callback errors and restores the pending state', (
    tester,
  ) async {
    await tester.pumpWidget(
      _host(
        ProjectAgentAssignmentPanel(
          projects: _projects,
          agents: _agents,
          assignments: _assignments,
          currentProjectId: 'project-1',
          onSet:
              ({
                required projectId,
                required agentId,
                required enabled,
                required workspaceAccess,
              }) async => throw StateError('Core rejected assignment'),
        ),
      ),
    );

    await tester.tap(find.byKey(const ValueKey('enabled-agent-1')));
    await tester.pumpAndSettle();
    expect(find.text('请求失败'), findsOneWidget);
    expect(find.text('Core rejected assignment'), findsOneWidget);
    expect(
      tester
          .widget<Switch>(find.byKey(const ValueKey('enabled-agent-1')))
          .onChanged,
      isNotNull,
    );
    expect(tester.takeException(), isNull);
  });
}

const List<Map<String, dynamic>> _projects = [
  {'id': 'project-1', 'name': 'Demo Project'},
  {'id': 'project-2', 'name': 'Other Project'},
];

const List<Map<String, dynamic>> _agents = [
  {'id': 'agent-1', 'name': 'Builder Agent'},
  {'id': 'agent-2', 'name': 'Reviewer Agent'},
];

const List<Map<String, dynamic>> _assignments = [
  {
    'projectId': 'project-1',
    'agentId': 'agent-1',
    'enabled': true,
    'workspaceAccess': 'read_only',
  },
];

Map<String, dynamic> _snapshot() => {
  'projects': _projects,
  'agents': _agents,
  'assignments': _assignments,
};

Widget _host(Widget child) {
  return MaterialApp(
    theme: ThemeData(useMaterial3: true),
    home: Scaffold(
      body: SingleChildScrollView(
        padding: const EdgeInsets.all(24),
        child: child,
      ),
    ),
  );
}
