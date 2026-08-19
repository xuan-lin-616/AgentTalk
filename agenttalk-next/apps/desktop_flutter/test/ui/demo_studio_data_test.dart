import 'package:agenttalk_desktop/ui/workbench/demo_studio_data.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  test('dev-only demo snapshot contains four agents and one project', () {
    final snapshot = DemoStudioData.snapshot();
    expect(snapshot['agents'], hasLength(4));
    expect(snapshot['projects'], hasLength(1));
    expect(snapshot['assignments'], hasLength(4));
    expect(snapshot['messages'], isNotEmpty);
  });

  test('dev-only demo DAG contains five nodes and four edges', () {
    final projection = DemoStudioData.orchestrationProjection();
    expect(projection.nodes, hasLength(5));
    expect(projection.edges, hasLength(4));
    expect(projection.runId, 'demo-orchestration-run');
  });

  test('dev-only demo logs and stream deltas are non-empty', () {
    expect(DemoStudioData.eventLog(), isNotEmpty);
    expect(DemoStudioData.streamingDeltas(), isNotEmpty);
  });
}
