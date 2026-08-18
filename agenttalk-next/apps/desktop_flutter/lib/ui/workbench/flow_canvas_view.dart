import 'dart:math';

import 'package:flutter/material.dart';

import '../theme/studio_colors.dart';
import 'orchestration_run_projection.dart';

/// Read-only flow canvas for the sealed orchestration DAG.
///
/// Nodes and edges come exclusively from `orchestration.run.snapshot`. The
/// view performs a deterministic layout only; it does not support free-form
/// dragging or edge editing (v1 DAG is sealed).
class FlowCanvasView extends StatelessWidget {
  const FlowCanvasView({
    super.key,
    required this.projection,
    this.loading = false,
    this.error,
    this.onPickRun,
    this.onRetryNode,
    this.onCancelRun,
    this.busy = false,
  });

  final OrchestrationRunProjection? projection;
  final bool loading;
  final String? error;
  final VoidCallback? onPickRun;
  final ValueChanged<String>? onRetryNode;
  final VoidCallback? onCancelRun;
  final bool busy;

  @override
  Widget build(BuildContext context) {
    final cs = Theme.of(context).colorScheme;
    final projection = this.projection;
    if (loading) {
      return const Center(child: CircularProgressIndicator(strokeWidth: 2));
    }
    if (error != null) {
      return _CanvasEmptyState(
        icon: Icons.error_outline,
        title: '编排投影读取失败',
        subtitle: error!,
        actionLabel: onPickRun == null ? null : '重试读取',
        onAction: onPickRun,
      );
    }
    if (projection == null) {
      return _CanvasEmptyState(
        icon: Icons.account_tree_outlined,
        title: '流程画布',
        subtitle: '选择 Orchestration Run 后，这里会渲染 Core 返回的 sealed DAG。',
        actionLabel: onPickRun == null ? null : '选择 Run',
        onAction: onPickRun,
      );
    }
    if (projection.nodes.isEmpty) {
      return _CanvasEmptyState(
        icon: Icons.account_tree_outlined,
        title: 'Run 没有可显示的任务节点',
        subtitle: 'runId: ${projection.runId}',
        actionLabel: onPickRun == null ? null : '选择其他 Run',
        onAction: onPickRun,
      );
    }

    final layout = _DagLayout(projection);
    return Container(
      color: StudioColors.bgRoot,
      child: Column(
        children: [
          _CanvasToolbar(
            runId: projection.runId,
            status: projection.status,
            nodeCount: projection.nodes.length,
            edgeCount: projection.edges.length,
            busy: busy,
            onPickRun: onPickRun,
            onCancelRun: onCancelRun,
          ),
          Expanded(
            child: ClipRect(
              child: InteractiveViewer(
                constrained: false,
                boundaryMargin: const EdgeInsets.all(200),
                minScale: 0.25,
                maxScale: 2.5,
                child: SizedBox(
                  width: layout.canvasSize.width,
                  height: layout.canvasSize.height,
                  child: Stack(
                    children: [
                      Positioned.fill(
                        child: CustomPaint(
                          painter: _FlowCanvasPainter(
                            layout: layout,
                            lineColor: cs.outlineVariant,
                          ),
                        ),
                      ),
                      for (final entry in layout.nodeEntries)
                        Positioned(
                          left: entry.position.dx,
                          top: entry.position.dy,
                          child: _FlowNodeCard(
                            node: entry.node,
                            statusColor: _statusColor(entry.node.status),
                            onRetry:
                                onRetryNode == null || !entry.node.isFaulted
                                ? null
                                : () => onRetryNode!(entry.node.nodeId),
                          ),
                        ),
                    ],
                  ),
                ),
              ),
            ),
          ),
        ],
      ),
    );
  }
}

class _CanvasToolbar extends StatelessWidget {
  const _CanvasToolbar({
    required this.runId,
    required this.status,
    required this.nodeCount,
    required this.edgeCount,
    required this.busy,
    this.onPickRun,
    this.onCancelRun,
  });

  final String runId;
  final String status;
  final int nodeCount;
  final int edgeCount;
  final bool busy;
  final VoidCallback? onPickRun;
  final VoidCallback? onCancelRun;

  @override
  Widget build(BuildContext context) {
    return Container(
      height: 36,
      padding: const EdgeInsets.symmetric(horizontal: 10),
      decoration: const BoxDecoration(
        color: StudioColors.bgSurface,
        border: Border(bottom: BorderSide(color: StudioColors.borderSubtle)),
      ),
      child: Row(
        children: [
          const Icon(
            Icons.account_tree_outlined,
            size: 16,
            color: StudioColors.textSecondary,
          ),
          const SizedBox(width: 8),
          Expanded(
            child: Text(
              'Run: $runId · $status · $nodeCount 节点 / $edgeCount 连线',
              maxLines: 1,
              overflow: TextOverflow.ellipsis,
              style: const TextStyle(
                color: StudioColors.textSecondary,
                fontSize: 11,
              ),
            ),
          ),
          if (onPickRun != null)
            TextButton.icon(
              onPressed: busy ? null : onPickRun,
              icon: const Icon(Icons.folder_open, size: 16),
              label: const Text('选择 Run'),
            ),
          if (onCancelRun != null &&
              status != 'completed' &&
              status != 'cancelled')
            TextButton.icon(
              onPressed: busy ? null : onCancelRun,
              icon: const Icon(Icons.stop_circle_outlined, size: 16),
              label: const Text('取消 Run'),
            ),
        ],
      ),
    );
  }
}

class _FlowNodeCard extends StatelessWidget {
  const _FlowNodeCard({
    required this.node,
    required this.statusColor,
    this.onRetry,
  });

  final OrchestrationNode node;
  final Color statusColor;
  final VoidCallback? onRetry;

  @override
  Widget build(BuildContext context) {
    return Container(
      width: 190,
      padding: const EdgeInsets.all(12),
      decoration: BoxDecoration(
        color: StudioColors.bgCard,
        borderRadius: BorderRadius.circular(10),
        border: Border.all(color: statusColor.withValues(alpha: 0.55)),
        boxShadow: [
          BoxShadow(
            color: statusColor.withValues(alpha: 0.22),
            blurRadius: 16,
            spreadRadius: 1,
          ),
        ],
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        mainAxisSize: MainAxisSize.min,
        children: [
          Row(
            children: [
              Container(
                width: 8,
                height: 8,
                decoration: BoxDecoration(
                  color: statusColor,
                  shape: BoxShape.circle,
                ),
              ),
              const SizedBox(width: 8),
              Expanded(
                child: Text(
                  node.nodeKey,
                  maxLines: 1,
                  overflow: TextOverflow.ellipsis,
                  style: const TextStyle(
                    color: StudioColors.textPrimary,
                    fontSize: 12,
                    fontWeight: FontWeight.w700,
                  ),
                ),
              ),
              if (onRetry != null)
                InkWell(
                  onTap: onRetry,
                  borderRadius: BorderRadius.circular(4),
                  child: const Padding(
                    padding: EdgeInsets.all(2),
                    child: Icon(
                      Icons.refresh,
                      size: 16,
                      color: StudioColors.danger,
                    ),
                  ),
                ),
            ],
          ),
          const SizedBox(height: 6),
          Text(
            node.status,
            style: TextStyle(
              color: statusColor,
              fontSize: 10,
              fontWeight: FontWeight.w600,
            ),
          ),
          const SizedBox(height: 4),
          Text(
            '${node.attemptCount}/${node.maxAttempts} attempts'
            '${node.roleId == null ? '' : ' · role ${node.roleId}'}',
            maxLines: 1,
            overflow: TextOverflow.ellipsis,
            style: const TextStyle(
              color: StudioColors.textTertiary,
              fontSize: 9,
            ),
          ),
        ],
      ),
    );
  }
}

class _CanvasEmptyState extends StatelessWidget {
  const _CanvasEmptyState({
    required this.icon,
    required this.title,
    required this.subtitle,
    this.actionLabel,
    this.onAction,
  });

  final IconData icon;
  final String title;
  final String subtitle;
  final String? actionLabel;
  final VoidCallback? onAction;

  @override
  Widget build(BuildContext context) {
    return Container(
      color: StudioColors.bgRoot,
      alignment: Alignment.center,
      child: Column(
        mainAxisSize: MainAxisSize.min,
        children: [
          Icon(icon, size: 46, color: StudioColors.textTertiary),
          const SizedBox(height: 12),
          Text(
            title,
            style: const TextStyle(
              color: StudioColors.textPrimary,
              fontSize: 14,
              fontWeight: FontWeight.w600,
            ),
          ),
          const SizedBox(height: 6),
          Text(
            subtitle,
            textAlign: TextAlign.center,
            style: const TextStyle(
              color: StudioColors.textSecondary,
              fontSize: 11,
            ),
          ),
          if (actionLabel != null && onAction != null) ...[
            const SizedBox(height: 14),
            OutlinedButton.icon(
              onPressed: onAction,
              icon: const Icon(Icons.arrow_forward, size: 16),
              label: Text(actionLabel!),
            ),
          ],
        ],
      ),
    );
  }
}

class _DagLayout {
  _DagLayout(OrchestrationRunProjection projection) : _projection = projection {
    _compute();
  }

  final OrchestrationRunProjection _projection;
  late final Size canvasSize;
  late final List<({OrchestrationNode node, Offset position})> nodeEntries;

  static const double _nodeWidth = 190;
  static const double _nodeHeight = 92;
  static const double _columnGap = 260;
  static const double _rowGap = 140;
  static const double _margin = 60;

  void _compute() {
    final nodes = _projection.nodes;
    final depth = <String, int>{};
    final indegree = <String, int>{for (final node in nodes) node.nodeId: 0};
    final outgoing = <String, List<String>>{
      for (final node in nodes) node.nodeId: [],
    };
    for (final edge in _projection.edges) {
      if (indegree.containsKey(edge.fromNodeId) &&
          indegree.containsKey(edge.toNodeId)) {
        indegree[edge.toNodeId] = indegree[edge.toNodeId]! + 1;
        outgoing[edge.fromNodeId]!.add(edge.toNodeId);
      }
    }

    // Kahn topological ordering; deterministic because node order is stable.
    final queue = <String>[
      for (final node in nodes)
        if (indegree[node.nodeId] == 0) node.nodeId,
    ];
    final order = <String>[];
    final seen = <String>{};
    while (queue.isNotEmpty) {
      final id = queue.removeAt(0);
      if (!seen.add(id)) continue;
      order.add(id);
      for (final target in outgoing[id]!) {
        indegree[target] = indegree[target]! - 1;
        if (indegree[target] == 0) queue.add(target);
      }
    }
    for (final node in nodes) {
      if (!seen.contains(node.nodeId)) order.add(node.nodeId);
    }

    for (final id in order) {
      var maxPred = -1;
      for (final edge in _projection.edges) {
        if (edge.toNodeId == id) {
          final predDepth = depth[edge.fromNodeId];
          if (predDepth != null && predDepth > maxPred) maxPred = predDepth;
        }
      }
      depth[id] = maxPred + 1;
    }

    final byDepth = <int, List<String>>{};
    for (final id in order) {
      byDepth.putIfAbsent(depth[id]!, () => []).add(id);
    }
    final nodeById = {for (final node in nodes) node.nodeId: node};
    nodeEntries = [
      for (final entry in byDepth.entries)
        for (final id in entry.value.indexed)
          (
            node: nodeById[id.$2]!,
            position: Offset(
              _margin + entry.key * _columnGap,
              _margin + id.$1 * _rowGap,
            ),
          ),
    ];

    final maxDepth = depth.values.isEmpty ? 0 : depth.values.reduce(max);
    final maxRows = byDepth.values.isEmpty
        ? 0
        : byDepth.values.map((list) => list.length).reduce(max);
    canvasSize = Size(
      _margin * 2 + maxDepth * _columnGap + _nodeWidth,
      _margin * 2 + (maxRows > 0 ? (maxRows - 1) * _rowGap : 0) + _nodeHeight,
    );
  }

  Offset portPosition(String nodeId, {required bool output}) {
    final entry = nodeEntries.firstWhere(
      (entry) => entry.node.nodeId == nodeId,
    );
    final y = entry.position.dy + _nodeHeight / 2;
    return Offset(
      output ? entry.position.dx + _nodeWidth : entry.position.dx,
      y,
    );
  }
}

class _FlowCanvasPainter extends CustomPainter {
  _FlowCanvasPainter({required this.layout, required this.lineColor});

  final _DagLayout layout;
  final Color lineColor;

  @override
  void paint(Canvas canvas, Size size) {
    final dotPaint = Paint()
      ..color = StudioColors.borderSubtle
      ..strokeWidth = 1;
    const dotStep = 26.0;
    for (double x = 0; x < size.width; x += dotStep) {
      for (double y = 0; y < size.height; y += dotStep) {
        canvas.drawCircle(Offset(x, y), 1, dotPaint);
      }
    }

    for (final edge in layout._projection.edges) {
      final source = layout.portPosition(edge.fromNodeId, output: true);
      final target = layout.portPosition(edge.toNodeId, output: false);
      final controlDx = (target.dx - source.dx).abs().clamp(40.0, 180.0);
      final path = Path()
        ..moveTo(source.dx, source.dy)
        ..cubicTo(
          source.dx + controlDx,
          source.dy,
          target.dx - controlDx,
          target.dy,
          target.dx,
          target.dy,
        );
      final paint = Paint()
        ..color = lineColor
        ..strokeWidth = 1.6
        ..style = PaintingStyle.stroke;
      canvas.drawPath(path, paint);

      final arrowAngle = atan2(target.dy - source.dy, target.dx - source.dx);
      final arrowLength = 8.0;
      final arrowTip = Offset(target.dx - 6, target.dy);
      final arrowPath = Path()
        ..moveTo(arrowTip.dx, arrowTip.dy)
        ..lineTo(
          arrowTip.dx - arrowLength * cos(arrowAngle - pi / 6),
          arrowTip.dy - arrowLength * sin(arrowAngle - pi / 6),
        )
        ..lineTo(
          arrowTip.dx - arrowLength * cos(arrowAngle + pi / 6),
          arrowTip.dy - arrowLength * sin(arrowAngle + pi / 6),
        )
        ..close();
      canvas.drawPath(arrowPath, Paint()..color = lineColor);
    }
  }

  @override
  bool shouldRepaint(covariant _FlowCanvasPainter oldDelegate) {
    return oldDelegate.layout != layout || oldDelegate.lineColor != lineColor;
  }
}

Color _statusColor(String status) => switch (status) {
  'ready' => StudioColors.warning,
  'running' || 'sealing' => StudioColors.success,
  'completed' || 'sealed' => StudioColors.primaryHover,
  'failed' || 'blocked' => StudioColors.danger,
  'cancelled' => StudioColors.inactive,
  _ => StudioColors.inactive,
};
