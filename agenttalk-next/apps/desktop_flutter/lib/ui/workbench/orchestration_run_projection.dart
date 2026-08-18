/// Read-only Dart projection of `orchestration.run.snapshot`.
///
/// The Core journal owns all orchestration state; this file only mirrors the
/// metadata the renderer is allowed to show. Parsing is fail-closed: unknown
/// shapes become an empty projection rather than a fake graph.
library;

class OrchestrationNode {
  const OrchestrationNode({
    required this.nodeId,
    required this.nodeKey,
    required this.status,
    required this.required,
    required this.attemptCount,
    required this.maxAttempts,
    this.roleId,
    this.activeAttemptId,
    this.terminalReason,
  });

  final String nodeId;
  final String nodeKey;
  final String status;
  final bool required;
  final int attemptCount;
  final int maxAttempts;
  final String? roleId;
  final String? activeAttemptId;
  final String? terminalReason;

  bool get isTerminal => status == 'completed' || status == 'sealed';
  bool get isFaulted => status == 'failed' || status == 'blocked';
  bool get isActive =>
      status == 'running' || status == 'sealing' || status == 'ready';
}

class OrchestrationEdge {
  const OrchestrationEdge({
    required this.edgeId,
    required this.fromNodeId,
    required this.toNodeId,
  });

  final String edgeId;
  final String fromNodeId;
  final String toNodeId;
}

class OrchestrationRunProjection {
  const OrchestrationRunProjection({
    required this.runId,
    required this.projectId,
    required this.status,
    required this.nodes,
    required this.edges,
    required this.attempts,
    required this.milestones,
    required this.deliveries,
    required this.machineAcceptances,
  });

  final String runId;
  final String projectId;
  final String status;
  final List<OrchestrationNode> nodes;
  final List<OrchestrationEdge> edges;
  final List<Map<String, dynamic>> attempts;
  final List<Map<String, dynamic>> milestones;
  final List<Map<String, dynamic>> deliveries;
  final List<Map<String, dynamic>> machineAcceptances;
}

OrchestrationRunProjection? tryParseOrchestrationProjection(
  Map<String, dynamic>? payload,
) {
  if (payload == null) return null;
  final run = payload['run'];
  final nodesJson = payload['nodes'];
  final edgesJson = payload['edges'];
  if (run is! Map<String, dynamic> ||
      nodesJson is! List ||
      edgesJson is! List) {
    return null;
  }
  final runId = run['runId']?.toString() ?? '';
  final projectId = run['projectId']?.toString() ?? '';
  if (runId.isEmpty) return null;

  final nodes = <OrchestrationNode>[];
  for (final nodeJson in nodesJson) {
    if (nodeJson is! Map<String, dynamic>) continue;
    final nodeId = nodeJson['nodeId']?.toString();
    if (nodeId == null || nodeId.isEmpty) continue;
    nodes.add(
      OrchestrationNode(
        nodeId: nodeId,
        nodeKey: nodeJson['nodeKey']?.toString() ?? nodeId,
        status: nodeJson['status']?.toString() ?? 'unknown',
        required: nodeJson['required'] == true,
        attemptCount: nodeJson['attemptCount'] as int? ?? 0,
        maxAttempts: nodeJson['maxAttempts'] as int? ?? 0,
        roleId: nodeJson['roleId']?.toString(),
        activeAttemptId: nodeJson['activeAttemptId']?.toString(),
        terminalReason: nodeJson['terminalReason']?.toString(),
      ),
    );
  }

  final edges = <OrchestrationEdge>[];
  for (final edgeJson in edgesJson) {
    if (edgeJson is! Map<String, dynamic>) continue;
    final edgeId = edgeJson['edgeId']?.toString();
    final from = edgeJson['fromNodeId']?.toString();
    final to = edgeJson['toNodeId']?.toString();
    if (edgeId == null || from == null || to == null) continue;
    edges.add(
      OrchestrationEdge(edgeId: edgeId, fromNodeId: from, toNodeId: to),
    );
  }

  return OrchestrationRunProjection(
    runId: runId,
    projectId: projectId,
    status: run['status']?.toString() ?? 'unknown',
    nodes: nodes,
    edges: edges,
    attempts: _asMapList(payload['attempts']),
    milestones: _asMapList(payload['milestones']),
    deliveries: _asMapList(payload['deliveries']),
    machineAcceptances: _asMapList(payload['machineAcceptances']),
  );
}

List<Map<String, dynamic>> _asMapList(dynamic value) {
  if (value is! List) return const [];
  return value.whereType<Map<String, dynamic>>().toList(growable: false);
}
