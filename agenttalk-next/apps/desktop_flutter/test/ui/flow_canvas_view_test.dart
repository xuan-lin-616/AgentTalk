import 'package:agenttalk_desktop/ui/workbench/flow_canvas_view.dart';
import 'package:agenttalk_desktop/ui/workbench/orchestration_run_projection.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  test('parses a real orchestration projection shape', () {
    final projection = tryParseOrchestrationProjection({
      'run': {'runId': 'run-1', 'projectId': 'project-1', 'status': 'running'},
      'nodes': [
        {
          'nodeId': 'node-1',
          'nodeKey': 'collector',
          'status': 'completed',
          'required': true,
          'attemptCount': 1,
          'maxAttempts': 3,
          'roleId': 'role-1',
        },
        {
          'nodeId': 'node-2',
          'nodeKey': 'analyzer',
          'status': 'failed',
          'required': true,
          'attemptCount': 2,
          'maxAttempts': 3,
        },
      ],
      'edges': [
        {'edgeId': 'edge-1', 'fromNodeId': 'node-1', 'toNodeId': 'node-2'},
      ],
      'attempts': [
        {'attemptId': 'attempt-1', 'nodeId': 'node-1', 'status': 'completed'},
      ],
      'milestones': <Map<String, dynamic>>[],
      'deliveries': <Map<String, dynamic>>[],
      'machineAcceptances': <Map<String, dynamic>>[],
    });
    expect(projection, isNotNull);
    expect(projection!.runId, 'run-1');
    expect(projection.nodes, hasLength(2));
    expect(projection.edges, hasLength(1));
    expect(projection.nodes.last.isFaulted, isTrue);
  });

  test('parser fails closed for malformed projection', () {
    expect(tryParseOrchestrationProjection(null), isNull);
    expect(tryParseOrchestrationProjection({}), isNull);
    expect(
      tryParseOrchestrationProjection({
        'run': {'runId': 'run-1'},
        'nodes': 'not-a-list',
        'edges': <Map<String, dynamic>>[],
      }),
      isNull,
    );
  });

  testWidgets('FlowCanvasView shows an explicit empty state without a run', (
    tester,
  ) async {
    await tester.pumpWidget(
      const MaterialApp(home: Scaffold(body: FlowCanvasView(projection: null))),
    );
    expect(find.text('流程画布'), findsOneWidget);
    expect(find.textContaining('选择 Orchestration Run'), findsOneWidget);
  });

  testWidgets('FlowCanvasView renders real nodes from a projection', (
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
      'milestones': <Map<String, dynamic>>[],
      'deliveries': <Map<String, dynamic>>[],
      'machineAcceptances': <Map<String, dynamic>>[],
    })!;
    await tester.pumpWidget(
      MaterialApp(
        home: Scaffold(body: FlowCanvasView(projection: projection)),
      ),
    );
    expect(find.text('collector'), findsOneWidget);
    expect(find.textContaining('run-1'), findsWidgets);
    expect(tester.takeException(), isNull);
  });
}
