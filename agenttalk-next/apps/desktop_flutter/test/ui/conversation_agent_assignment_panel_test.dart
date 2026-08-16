import 'package:agenttalk_desktop/ui/conversation_agent_assignment_panel.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  testWidgets('renders only Project-roster Conversation assignments', (
    tester,
  ) async {
    await tester.pumpWidget(
      _host(
        ConversationAgentAssignmentPanel(
          snapshot: _snapshot,
          conversationId: 'conversation-1',
          onSet:
              ({
                required conversationId,
                required agentId,
                required enabled,
              }) async {},
          onRemove: ({required conversationId, required agentId}) async {},
        ),
      ),
    );
    expect(find.text('会话智能体分配'), findsOneWidget);
    expect(find.text('Builder Agent'), findsOneWidget);
    expect(find.text('Other Agent'), findsNothing);
    expect(find.textContaining('面板不会扩展列表'), findsOneWidget);
    expect(tester.takeException(), isNull);
  });

  testWidgets('forwards set/remove and preserves scope', (tester) async {
    final calls = <String>[];
    await tester.pumpWidget(
      _host(
        ConversationAgentAssignmentPanel(
          snapshot: _snapshot,
          conversationId: 'conversation-1',
          onSet:
              ({
                required conversationId,
                required agentId,
                required enabled,
              }) async {
                calls.add('set:$conversationId/$agentId/$enabled');
              },
          onRemove: ({required conversationId, required agentId}) async {
            calls.add('remove:$conversationId/$agentId');
          },
        ),
      ),
    );
    await tester.tap(
      find.byKey(const ValueKey('conversation-enabled-agent-1')),
    );
    await tester.pumpAndSettle();
    await tester.tap(
      find.byKey(const ValueKey('remove-conversation-agent-agent-1')),
    );
    await tester.pumpAndSettle();
    expect(calls, [
      'set:conversation-1/agent-1/false',
      'remove:conversation-1/agent-1',
    ]);
  });

  testWidgets('empty roster inherits Project and has no expansion path', (
    tester,
  ) async {
    await tester.pumpWidget(
      _host(
        const ConversationAgentAssignmentPanel(
          snapshot: _emptyConversationSnapshot,
          conversationId: 'conversation-1',
        ),
      ),
    );
    expect(find.textContaining('继承项目列表'), findsOneWidget);
    expect(
      find.byKey(const ValueKey('conversation-agent-assignment-add')),
      findsOneWidget,
    );
    expect(tester.takeException(), isNull);
  });
}

const Map<String, dynamic> _snapshot = {
  'projects': [
    {'id': 'project-1', 'name': 'Demo Project'},
  ],
  'conversations': [
    {'id': 'conversation-1', 'projectId': 'project-1', 'title': 'Demo'},
  ],
  'agents': [
    {'id': 'agent-1', 'name': 'Builder Agent'},
    {'id': 'agent-2', 'name': 'Other Agent'},
  ],
  'assignments': [
    {'projectId': 'project-1', 'agentId': 'agent-1', 'enabled': true},
  ],
  'conversationAgents': [
    {'conversationId': 'conversation-1', 'agentId': 'agent-1', 'enabled': true},
    {'conversationId': 'conversation-1', 'agentId': 'agent-2', 'enabled': true},
  ],
};

const Map<String, dynamic> _emptyConversationSnapshot = {
  'projects': [
    {'id': 'project-1', 'name': 'Demo Project'},
  ],
  'conversations': [
    {'id': 'conversation-1', 'projectId': 'project-1', 'title': 'Demo'},
  ],
  'agents': [
    {'id': 'agent-1', 'name': 'Builder Agent'},
  ],
  'assignments': [
    {'projectId': 'project-1', 'agentId': 'agent-1', 'enabled': true},
  ],
  'conversationAgents': <Map<String, dynamic>>[],
};

Widget _host(Widget child) => MaterialApp(
  theme: ThemeData(useMaterial3: true),
  home: Scaffold(
    body: SingleChildScrollView(
      padding: const EdgeInsets.all(24),
      child: child,
    ),
  ),
);
