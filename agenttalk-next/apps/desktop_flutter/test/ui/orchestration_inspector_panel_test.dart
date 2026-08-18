import 'package:agenttalk_desktop/ui/workbench/orchestration_inspector_panel.dart';
import 'package:agenttalk_desktop/ui/workbench/orchestration_run_projection.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  testWidgets('OrchestrationInspectorPanel renders real projection sections', (
    tester,
  ) async {
    final projection = tryParseOrchestrationProjection({
      'run': {'runId': 'run-1', 'projectId': 'project-1', 'status': 'running'},
      'nodes': [
        {
          'nodeId': 'node-1',
          'nodeKey': 'collector',
          'status': 'running',
          'required': true,
          'attemptCount': 1,
          'maxAttempts': 3,
        },
      ],
      'edges': <Map<String, dynamic>>[],
      'attempts': <Map<String, dynamic>>[],
      'milestones': [
        {
          'milestoneId': 'm-1',
          'milestoneKey': 'brief',
          'status': 'awaiting_approval',
          'version': 1,
        },
      ],
      'deliveries': [
        {
          'deliveryId': 'delivery-1',
          'fromTaskNodeId': 'node-1',
          'toTaskNodeId': 'node-2',
          'artifactTransferSetDigest': 'a' * 64,
        },
      ],
      'machineAcceptances': <Map<String, dynamic>>[],
    })!;
    await tester.pumpWidget(
      MaterialApp(
        home: Scaffold(
          body: OrchestrationInspectorPanel(projection: projection),
        ),
      ),
    );
    expect(find.text('编排检查器 · run-1'), findsOneWidget);
    expect(find.text('collector'), findsOneWidget);
    expect(find.textContaining('交付物 Delivery · 1'), findsOneWidget);
    expect(find.textContaining('里程碑 Milestone · 1'), findsOneWidget);
  });
}
