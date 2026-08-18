import 'package:agenttalk_desktop/ui/workbench/approval_panel.dart';
import 'package:agenttalk_desktop/ui/workbench/studio_approval_request.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  test('tool.requested parses with handoffId as an actionable approval', () {
    final approval = studioApprovalFromEventMap({
      'eventId': 'evt-1',
      'event': 'tool.requested',
      'occurredAt': '2026-08-19T10:00:00Z',
      'cursor': {'streamId': 'core-events', 'sequence': 1},
      'payload': {
        'toolName': 'write_file',
        'handoffId': 'handoff-1',
        'requestId': 'req-1',
      },
    });
    expect(approval, isNotNull);
    expect(approval!.kind, StudioApprovalKind.tool);
    expect(approval.hasApprovalCommand, isTrue);
    expect(approval.toolName, 'write_file');
  });

  test('tool.requested without handoffId is marked backend gap', () {
    final approval = studioApprovalFromEventMap({
      'eventId': 'evt-2',
      'event': 'tool.requested',
      'occurredAt': '2026-08-19T10:00:01Z',
      'cursor': {'streamId': 'core-events', 'sequence': 2},
      'payload': {'toolName': 'shell'},
    });
    expect(approval, isNotNull);
    expect(approval!.hasApprovalCommand, isFalse);
  });

  testWidgets('ApprovalPanel shows actionable and backend-gap requests', (
    tester,
  ) async {
    final requests = [
      StudioApprovalRequest(
        id: 'a-1',
        kind: StudioApprovalKind.handoff,
        occurredAt: DateTime.parse('2026-08-19T10:00:02Z'),
        message: '交接待审批：agent-1 → agent-2',
        handoffId: 'handoff-1',
      ),
      StudioApprovalRequest(
        id: 'a-2',
        kind: StudioApprovalKind.tool,
        occurredAt: DateTime.parse('2026-08-19T10:00:03Z'),
        message: '工具审批请求：shell',
      ),
    ];
    await tester.pumpWidget(
      MaterialApp(
        home: Scaffold(
          body: ApprovalPanel(
            requests: requests,
            busy: false,
            onApprove: (_) {},
            onReject: (_) {},
            onDismiss: (_) {},
          ),
        ),
      ),
    );
    expect(find.text('批准'), findsOneWidget);
    expect(find.text('拒绝'), findsOneWidget);
    expect(find.textContaining('后端待补'), findsOneWidget);
    expect(find.byKey(const Key('approval-approve-a-1')), findsOneWidget);
  });
}
