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
    this.onShowInspector,
    this.busy = false,
  });

  final OrchestrationRunProjection? projection;
  final bool loading;
  final String? error;
  final VoidCallback? onPickRun;
  final ValueChanged<String>? onRetryNode;
  final VoidCallback? onCancelRun;
  final VoidCallback? onShowInspector;
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
        subtitle: '选择 Orchestration Run 后将渲染 Core 返回的 sealed DAG',
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
            onShowInspector: onShowInspector,
          ),
          Expanded(
            child: ClipRect(
              child: _FlowCanvasField(
                layout: layout,
                lineColor: cs.outlineVariant,
                nodeEntries: layout.nodeEntries,
                onRetryNode: onRetryNode,
              ),
            ),
          ),
        ],
      ),
    );
  }
}

class _FlowCanvasField extends StatefulWidget {
  const _FlowCanvasField({
    required this.layout,
    required this.lineColor,
    required this.nodeEntries,
    this.onRetryNode,
  });

  final _DagLayout layout;
  final Color lineColor;
  final List<({OrchestrationNode node, Offset position})> nodeEntries;
  final ValueChanged<String>? onRetryNode;

  @override
  State<_FlowCanvasField> createState() => _FlowCanvasFieldState();
}

class _FlowCanvasFieldState extends State<_FlowCanvasField>
    with SingleTickerProviderStateMixin {
  late final AnimationController _flowController = AnimationController(
    vsync: this,
    duration: const Duration(milliseconds: 1800),
  )..repeat();

  @override
  void dispose() {
    _flowController.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return InteractiveViewer(
      constrained: false,
      boundaryMargin: const EdgeInsets.all(200),
      minScale: 0.25,
      maxScale: 2.5,
      child: SizedBox(
        width: widget.layout.canvasSize.width,
        height: widget.layout.canvasSize.height,
        child: Stack(
          children: [
            Positioned.fill(
              child: AnimatedBuilder(
                animation: _flowController,
                builder: (context, _) => CustomPaint(
                  painter: _FlowCanvasPainter(
                    layout: widget.layout,
                    lineColor: widget.lineColor,
                    flowPhase: _flowController.value,
                  ),
                ),
              ),
            ),
            for (final entry in widget.nodeEntries)
              Positioned(
                left: entry.position.dx,
                top: entry.position.dy,
                child: _FlowNodeCard(
                  node: entry.node,
                  statusColor: _statusColor(entry.node.status),
                  onRetry: widget.onRetryNode == null || !entry.node.isFaulted
                      ? null
                      : () => widget.onRetryNode!(entry.node.nodeId),
                ),
              ),
          ],
        ),
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
    this.onShowInspector,
  });

  final String runId;
  final String status;
  final int nodeCount;
  final int edgeCount;
  final bool busy;
  final VoidCallback? onPickRun;
  final VoidCallback? onCancelRun;
  final VoidCallback? onShowInspector;

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
              icon: const Icon(Icons.folder_open_outlined, size: 16),
              label: const Text('选择 Run'),
            ),
          if (onShowInspector != null)
            TextButton.icon(
              onPressed: busy ? null : onShowInspector,
              icon: const Icon(Icons.fact_check_outlined, size: 16),
              label: const Text('详情'),
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

class _FlowNodeCard extends StatefulWidget {
  const _FlowNodeCard({
    required this.node,
    required this.statusColor,
    this.onRetry,
  });

  final OrchestrationNode node;
  final Color statusColor;
  final VoidCallback? onRetry;

  @override
  State<_FlowNodeCard> createState() => _FlowNodeCardState();
}

class _FlowNodeCardState extends State<_FlowNodeCard>
    with SingleTickerProviderStateMixin {
  late final AnimationController _pulseController = AnimationController(
    vsync: this,
    duration: const Duration(milliseconds: 1500),
  );

  bool get _pulsing =>
      widget.node.status == 'running' || widget.node.status == 'sealing';

  @override
  void initState() {
    super.initState();
    if (_pulsing) {
      _pulseController.repeat(reverse: true);
    }
  }

  @override
  void didUpdateWidget(covariant _FlowNodeCard oldWidget) {
    super.didUpdateWidget(oldWidget);
    if (_pulsing && !_pulseController.isAnimating) {
      _pulseController.repeat(reverse: true);
    } else if (!_pulsing && _pulseController.isAnimating) {
      _pulseController.stop();
      _pulseController.value = 0;
    }
  }

  @override
  void dispose() {
    _pulseController.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    final color = widget.statusColor;
    return AnimatedBuilder(
      animation: _pulseController,
      builder: (context, child) {
        final glowAlpha = _pulsing
            ? 0.18 + 0.16 * _pulseController.value
            : 0.22;
        final glowBlur = _pulsing ? 12.0 + 10.0 * _pulseController.value : 16.0;
        return Container(
          width: 190,
          padding: const EdgeInsets.all(12),
          decoration: BoxDecoration(
            color: StudioColors.bgCard,
            borderRadius: BorderRadius.circular(10),
            border: Border.all(color: color.withValues(alpha: 0.55)),
            boxShadow: [
              BoxShadow(
                color: color.withValues(alpha: glowAlpha),
                blurRadius: glowBlur,
                spreadRadius: 1,
              ),
            ],
          ),
          child: Stack(
            children: [
              Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                mainAxisSize: MainAxisSize.min,
                children: [
                  Row(
                    children: [
                      Container(
                        width: 8,
                        height: 8,
                        decoration: BoxDecoration(
                          color: color,
                          shape: BoxShape.circle,
                        ),
                      ),
                      const SizedBox(width: 8),
                      Expanded(
                        child: Text(
                          widget.node.nodeKey,
                          maxLines: 1,
                          overflow: TextOverflow.ellipsis,
                          style: const TextStyle(
                            color: StudioColors.textPrimary,
                            fontSize: 12,
                            fontWeight: FontWeight.w700,
                          ),
                        ),
                      ),
                      if (widget.onRetry != null)
                        InkWell(
                          onTap: widget.onRetry,
                          borderRadius: BorderRadius.circular(4),
                          child: const Padding(
                            padding: EdgeInsets.all(2),
                            child: Icon(
                              Icons.refresh_outlined,
                              size: 16,
                              color: StudioColors.danger,
                            ),
                          ),
                        ),
                    ],
                  ),
                  const SizedBox(height: 6),
                  Text(
                    widget.node.status,
                    style: TextStyle(
                      color: color,
                      fontSize: 10,
                      fontWeight: FontWeight.w600,
                    ),
                  ),
                  const SizedBox(height: 4),
                  Text(
                    '${widget.node.attemptCount}/${widget.node.maxAttempts} attempts'
                    '${widget.node.roleId == null ? '' : ' · role ${widget.node.roleId}'}',
                    maxLines: 1,
                    overflow: TextOverflow.ellipsis,
                    style: const TextStyle(
                      color: StudioColors.textTertiary,
                      fontSize: 9,
                    ),
                  ),
                ],
              ),
              const Positioned(left: -16, top: 34, child: _PortDot()),
              const Positioned(right: -16, top: 34, child: _PortDot()),
            ],
          ),
        );
      },
    );
  }
}

class _PortDot extends StatelessWidget {
  const _PortDot();

  @override
  Widget build(BuildContext context) {
    return Container(
      width: 8,
      height: 8,
      decoration: BoxDecoration(
        color: StudioColors.borderStrong,
        shape: BoxShape.circle,
        border: Border.all(color: StudioColors.bgRoot, width: 2),
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
    return Stack(
      children: [
        const Positioned.fill(
          child: CustomPaint(painter: FlowDotGridPainter()),
        ),
        Center(
          child: Container(
            constraints: const BoxConstraints(maxWidth: 420),
            margin: const EdgeInsets.all(24),
            padding: const EdgeInsets.fromLTRB(20, 18, 20, 16),
            decoration: BoxDecoration(
              color: StudioColors.bgCard,
              borderRadius: BorderRadius.circular(12),
              border: Border.all(color: StudioColors.borderSubtle),
            ),
            child: Column(
              mainAxisSize: MainAxisSize.min,
              children: [
                Icon(icon, size: 40, color: StudioColors.textTertiary),
                const SizedBox(height: 10),
                Text(
                  title,
                  textAlign: TextAlign.center,
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
                    height: 1.4,
                  ),
                ),
                if (actionLabel != null && onAction != null) ...[
                  const SizedBox(height: 14),
                  OutlinedButton.icon(
                    onPressed: onAction,
                    icon: const Icon(Icons.arrow_forward_outlined, size: 16),
                    label: Text(actionLabel!),
                  ),
                ],
              ],
            ),
          ),
        ),
      ],
    );
  }
}

/// Persistent dot-grid backdrop used by the canvas empty state.
class FlowDotGridPainter extends CustomPainter {
  const FlowDotGridPainter();

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
  }

  @override
  bool shouldRepaint(covariant FlowDotGridPainter oldDelegate) => false;
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
  _FlowCanvasPainter({
    required this.layout,
    required this.lineColor,
    required this.flowPhase,
  });

  final _DagLayout layout;
  final Color lineColor;
  final double flowPhase;

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

      final pathMetric = path.computeMetrics().firstOrNull;
      if (pathMetric != null && pathMetric.length > 0) {
        final flowOffset = (flowPhase * pathMetric.length) % pathMetric.length;
        final extract = pathMetric.extractPath(
          flowOffset,
          (flowOffset + 26).clamp(0, pathMetric.length).toDouble(),
        );
        final flowPaint = Paint()
          ..color = StudioColors.primaryHover.withValues(alpha: 0.9)
          ..strokeWidth = 2.4
          ..strokeCap = StrokeCap.round
          ..style = PaintingStyle.stroke;
        canvas.drawPath(extract, flowPaint);
      }

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
    return oldDelegate.layout != layout ||
        oldDelegate.lineColor != lineColor ||
        oldDelegate.flowPhase != flowPhase;
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
