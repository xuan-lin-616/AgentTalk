import 'orchestration_run_projection.dart';
import 'studio_event_log.dart';

/// DEV-ONLY demo dataset for the visual acceptance build.
///
/// This file is intentionally not part of the production data path. It is
/// only reachable through the explicit debug-mode "演示数据" switch and is
/// never injected automatically. Real IPC remains the primary path; when a
/// real Core connects, the shell clears this data immediately.
abstract final class DemoStudioData {
  static const String projectId = 'demo-project';
  static const String conversationId = 'demo-conversation';

  static Map<String, dynamic> snapshot() => {
    'projects': [
      {
        'id': projectId,
        'name': '产品发布智能体流水线',
        'rootPath': null,
      },
    ],
    'agents': [
      {
        'id': 'agent-brief',
        'name': 'Brief 拆解智能体',
        'role': 'brief-sealer',
        'specialty': '需求拆解 / 里程碑规划',
      },
      {
        'id': 'agent-research',
        'name': '情报收集智能体',
        'role': 'collector',
        'specialty': '市场扫描 / 竞品分析',
      },
      {
        'id': 'agent-copy',
        'name': '文案生成智能体',
        'role': 'writer',
        'specialty': 'Markdown 长文 / 风格控制',
      },
      {
        'id': 'agent-review',
        'name': '验收审核智能体',
        'role': 'reviewer',
        'specialty': '事实核查 / 合规检查',
      },
    ],
    'assignments': [
      for (final agent in [
        'agent-brief',
        'agent-research',
        'agent-copy',
        'agent-review',
      ])
        {'projectId': projectId, 'agentId': agent, 'enabled': true},
    ],
    'conversations': [
      {'id': conversationId, 'projectId': projectId, 'title': '发布稿协同'},
    ],
    'messages': [
      {
        'id': 'demo-message-1',
        'conversationId': conversationId,
        'senderId': 'user',
        'content': '帮我生成产品发布稿，先做市场情报，再写正文。',
        'sequence': 1,
      },
      {
        'id': 'demo-message-2',
        'conversationId': conversationId,
        'senderId': 'agent-research',
        'content': '已收到。我将从 **市场扫描**、**竞品分析** 两个方向收集情报。',
        'sequence': 2,
      },
    ],
    'runs': [
      {
        'id': 'demo-run-1',
        'projectId': projectId,
        'agentId': 'agent-research',
        'status': 'completed',
      },
      {
        'id': 'demo-run-2',
        'projectId': projectId,
        'agentId': 'agent-copy',
        'status': 'running',
      },
    ],
    'workflows': <Map<String, dynamic>>[],
    'collaborationRuns': <Map<String, dynamic>>[],
    'handoffs': [
      {
        'id': 'demo-handoff-1',
        'fromAgentId': 'agent-research',
        'toAgentId': 'agent-copy',
        'status': 'approved',
      },
    ],
    'executionRuns': <Map<String, dynamic>>[],
  };

  static List<StudioLogEntry> eventLog() {
    final now = DateTime.now();
    return [
      StudioLogEntry(
        id: 'demo-log-1',
        occurredAt: now.subtract(const Duration(minutes: 6)),
        eventType: 'execution.created',
        message: '运行已创建：demo-run-1',
        level: StudioLogLevel.info,
      ),
      StudioLogEntry(
        id: 'demo-log-2',
        occurredAt: now.subtract(const Duration(minutes: 5, seconds: 30)),
        eventType: 'context.assembled',
        message: '上下文已组装',
        level: StudioLogLevel.success,
      ),
      StudioLogEntry(
        id: 'demo-log-3',
        occurredAt: now.subtract(const Duration(minutes: 4)),
        eventType: 'output.delta',
        message: '输出增量：竞品矩阵已生成',
        level: StudioLogLevel.info,
      ),
      StudioLogEntry(
        id: 'demo-log-4',
        occurredAt: now.subtract(const Duration(minutes: 2)),
        eventType: 'execution.completed',
        message: '运行已完成：demo-run-1',
        level: StudioLogLevel.success,
      ),
      StudioLogEntry(
        id: 'demo-log-5',
        occurredAt: now.subtract(const Duration(minutes: 1)),
        eventType: 'handoff.proposed',
        message: '交接待审批：agent-research → agent-copy',
        level: StudioLogLevel.warning,
      ),
      StudioLogEntry(
        id: 'demo-log-6',
        occurredAt: now,
        eventType: 'handoff.approved',
        message: '交接已批准：demo-handoff-1',
        level: StudioLogLevel.success,
      ),
    ];
  }

  static List<StudioStreamingDelta> streamingDeltas() {
    final now = DateTime.now();
    return [
      StudioStreamingDelta(
        id: 'demo-delta-1',
        occurredAt: now,
        delta: '正在撰写发布稿正文：\n\n',
        isComplete: false,
        conversationId: conversationId,
      ),
      StudioStreamingDelta(
        id: 'demo-delta-2',
        occurredAt: now,
        delta: '## 发布亮点\n\n- 更快的本地协作\n- 更强的审计能力\n\n',
        isComplete: false,
        conversationId: conversationId,
      ),
      StudioStreamingDelta(
        id: 'demo-delta-3',
        occurredAt: now,
        delta: '**以上为演示数据。**',
        isComplete: true,
        conversationId: conversationId,
      ),
    ];
  }

  static OrchestrationRunProjection orchestrationProjection() {
    return OrchestrationRunProjection(
      runId: 'demo-orchestration-run',
      projectId: projectId,
      status: 'running',
      nodes: [
        const OrchestrationNode(
          nodeId: 'node-brief',
          nodeKey: 'brief-seal',
          status: 'completed',
          required: true,
          attemptCount: 1,
          maxAttempts: 3,
          roleId: 'role-brief',
        ),
        const OrchestrationNode(
          nodeId: 'node-research',
          nodeKey: 'market-research',
          status: 'completed',
          required: true,
          attemptCount: 1,
          maxAttempts: 3,
          roleId: 'role-research',
        ),
        const OrchestrationNode(
          nodeId: 'node-copy',
          nodeKey: 'copywriting',
          status: 'running',
          required: true,
          attemptCount: 1,
          maxAttempts: 3,
          roleId: 'role-copy',
        ),
        const OrchestrationNode(
          nodeId: 'node-review',
          nodeKey: 'review',
          status: 'ready',
          required: true,
          attemptCount: 0,
          maxAttempts: 3,
          roleId: 'role-review',
        ),
        const OrchestrationNode(
          nodeId: 'node-publish',
          nodeKey: 'publish',
          status: 'idle',
          required: true,
          attemptCount: 0,
          maxAttempts: 3,
          roleId: 'role-publish',
        ),
      ],
      edges: [
        const OrchestrationEdge(
          edgeId: 'demo-edge-1',
          fromNodeId: 'node-brief',
          toNodeId: 'node-research',
        ),
        const OrchestrationEdge(
          edgeId: 'demo-edge-2',
          fromNodeId: 'node-research',
          toNodeId: 'node-copy',
        ),
        const OrchestrationEdge(
          edgeId: 'demo-edge-3',
          fromNodeId: 'node-copy',
          toNodeId: 'node-review',
        ),
        const OrchestrationEdge(
          edgeId: 'demo-edge-4',
          fromNodeId: 'node-review',
          toNodeId: 'node-publish',
        ),
      ],
      attempts: [
        {
          'attemptId': 'demo-attempt-1',
          'nodeId': 'node-brief',
          'status': 'completed',
        },
        {
          'attemptId': 'demo-attempt-2',
          'nodeId': 'node-research',
          'status': 'completed',
        },
        {
          'attemptId': 'demo-attempt-3',
          'nodeId': 'node-copy',
          'status': 'running',
        },
      ],
      milestones: [
        {
          'milestoneId': 'demo-milestone-1',
          'milestoneKey': 'brief-approval',
          'status': 'approved',
          'version': 1,
        },
      ],
      deliveries: [
        {
          'deliveryId': 'demo-delivery-1',
          'fromTaskNodeId': 'node-brief',
          'toTaskNodeId': 'node-research',
          'artifactTransferSetDigest': 'a' * 64,
        },
      ],
      machineAcceptances: [
        {
          'acceptanceId': 'demo-acceptance-1',
          'deliveryId': 'demo-delivery-1',
          'verdict': 'accepted',
          'verifierId': 'machine-verifier',
        },
      ],
    );
  }
}
