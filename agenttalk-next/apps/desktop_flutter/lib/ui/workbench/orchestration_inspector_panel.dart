import 'package:flutter/material.dart';

import '../theme/studio_colors.dart';
import 'orchestration_run_projection.dart';

/// Read-only orchestration inspector for deliveries, machine acceptances and
/// milestones. Every row comes from `orchestration.run.snapshot`; this widget
/// performs no IPC and renders no synthetic records.
class OrchestrationInspectorPanel extends StatelessWidget {
  const OrchestrationInspectorPanel({
    super.key,
    required this.projection,
  });

  final OrchestrationRunProjection projection;

  @override
  Widget build(BuildContext context) {
    final sections = <_InspectorSection>[
      _InspectorSection(
        title: 'TaskNode / Attempt',
        icon: Icons.account_tree_outlined,
        count: projection.nodes.length,
        children: [
          for (final node in projection.nodes)
            ListTile(
              dense: true,
              contentPadding: EdgeInsets.zero,
              leading: Icon(_nodeIcon(node.status), size: 18),
              title: Text(
                node.nodeKey,
                style: const TextStyle(
                  color: StudioColors.textPrimary,
                  fontSize: 12,
                ),
              ),
              subtitle: Text(
                '${node.nodeId} · ${node.status} · ${node.attemptCount}/${node.maxAttempts}',
                style: const TextStyle(
                  color: StudioColors.textTertiary,
                  fontSize: 10,
                ),
              ),
            ),
        ],
      ),
      _InspectorSection(
        title: '交付物 Delivery',
        icon: Icons.inventory_2_outlined,
        count: projection.deliveries.length,
        children: [
          for (final delivery in projection.deliveries.take(20))
            _DigestTile(
              title: delivery['deliveryId']?.toString() ?? '-',
              subtitle:
                  '${delivery['fromTaskNodeId'] ?? '-'} → ${delivery['toTaskNodeId'] ?? '-'}',
              digest: delivery['artifactTransferSetDigest']?.toString(),
            ),
        ],
      ),
      _InspectorSection(
        title: '机器验收 Acceptance',
        icon: Icons.verified_outlined,
        count: projection.machineAcceptances.length,
        children: [
          for (final acceptance in projection.machineAcceptances.take(20))
            ListTile(
              dense: true,
              contentPadding: EdgeInsets.zero,
              leading: Icon(
                acceptance['verdict'] == 'accepted'
                    ? Icons.check_circle_outline
                    : Icons.cancel_outlined,
                size: 18,
              ),
              title: Text(
                '${acceptance['verdict'] ?? '-'} · verifier ${acceptance['verifierId'] ?? '-'}',
                style: const TextStyle(
                  color: StudioColors.textPrimary,
                  fontSize: 12,
                ),
              ),
              subtitle: Text(
                'delivery ${acceptance['deliveryId'] ?? '-'} · ${acceptance['resultDigest'] ?? '-'}',
                style: const TextStyle(
                  color: StudioColors.textTertiary,
                  fontSize: 10,
                ),
              ),
            ),
        ],
      ),
      _InspectorSection(
        title: '里程碑 Milestone',
        icon: Icons.fact_check_outlined,
        count: projection.milestones.length,
        children: [
          for (final milestone in projection.milestones.take(20))
            ListTile(
              dense: true,
              contentPadding: EdgeInsets.zero,
              leading: Icon(
                milestone['status'] == 'approved'
                    ? Icons.verified_outlined
                    : milestone['status'] == 'rejected'
                    ? Icons.cancel_outlined
                    : Icons.fact_check_outlined,
                size: 18,
              ),
              title: Text(
                milestone['milestoneKey']?.toString() ??
                    milestone['milestoneId']?.toString() ??
                    '-',
                style: const TextStyle(
                  color: StudioColors.textPrimary,
                  fontSize: 12,
                ),
              ),
              subtitle: Text(
                '${milestone['status'] ?? '-'} · v${milestone['version'] ?? '-'}',
                style: const TextStyle(
                  color: StudioColors.textTertiary,
                  fontSize: 10,
                ),
              ),
            ),
        ],
      ),
    ];

    return Container(
      width: 720,
      constraints: const BoxConstraints(maxHeight: 560),
      padding: const EdgeInsets.all(16),
      decoration: BoxDecoration(
        color: StudioColors.bgSurface,
        borderRadius: BorderRadius.circular(12),
        border: Border.all(color: StudioColors.borderSubtle),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        mainAxisSize: MainAxisSize.min,
        children: [
          Text(
            '编排检查器 · ${projection.runId}',
            style: const TextStyle(
              color: StudioColors.textPrimary,
              fontSize: 13,
              fontWeight: FontWeight.w700,
            ),
          ),
          const SizedBox(height: 4),
          Text(
            '状态 ${projection.status} · 只读（Core 是唯一状态写入者）',
            style: const TextStyle(
              color: StudioColors.textSecondary,
              fontSize: 10,
            ),
          ),
          const Divider(height: 20, color: StudioColors.borderSubtle),
          Flexible(
            child: ListView(
              shrinkWrap: true,
              children: [
                for (final section in sections) ...[
                  _SectionHeader(section: section),
                  if (section.children.isEmpty)
                    const Padding(
                      padding: EdgeInsets.only(left: 12, bottom: 8),
                      child: Text(
                        '暂无数据',
                        style: TextStyle(
                          color: StudioColors.textTertiary,
                          fontSize: 10,
                        ),
                      ),
                    )
                  else
                    ...section.children,
                  const SizedBox(height: 8),
                ],
              ],
            ),
          ),
        ],
      ),
    );
  }
}

class _InspectorSection {
  const _InspectorSection({
    required this.title,
    required this.icon,
    required this.count,
    required this.children,
  });

  final String title;
  final IconData icon;
  final int count;
  final List<Widget> children;
}

class _SectionHeader extends StatelessWidget {
  const _SectionHeader({required this.section});

  final _InspectorSection section;

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.symmetric(vertical: 6),
      child: Row(
        children: [
          Icon(section.icon, size: 16, color: StudioColors.primaryHover),
          const SizedBox(width: 8),
          Text(
            '${section.title} · ${section.count}',
            style: const TextStyle(
              color: StudioColors.textPrimary,
              fontSize: 12,
              fontWeight: FontWeight.w600,
            ),
          ),
        ],
      ),
    );
  }
}

class _DigestTile extends StatelessWidget {
  const _DigestTile({
    required this.title,
    required this.subtitle,
    this.digest,
  });

  final String title;
  final String subtitle;
  final String? digest;

  @override
  Widget build(BuildContext context) {
    final digestText = digest == null ? '-' : _shortDigest(digest!);
    return ListTile(
      dense: true,
      contentPadding: EdgeInsets.zero,
      leading: const Icon(
        Icons.inventory_2_outlined,
        size: 18,
        color: StudioColors.textSecondary,
      ),
      title: Text(
        title,
        style: const TextStyle(
          color: StudioColors.textPrimary,
          fontSize: 12,
        ),
      ),
      subtitle: Text(
        '$subtitle · digest $digestText',
        style: const TextStyle(
          color: StudioColors.textTertiary,
          fontSize: 10,
        ),
      ),
    );
  }
}

String _shortDigest(String value) {
  if (value.length <= 18) return value;
  return '${value.substring(0, 10)}…${value.substring(value.length - 8)}';
}

IconData _nodeIcon(String status) => switch (status) {
  'completed' => Icons.check_circle_outline,
  'running' => Icons.play_circle_outline,
  'sealing' => Icons.lock_clock_outlined,
  'failed' || 'blocked' => Icons.error_outline,
  'cancelled' => Icons.cancel_outlined,
  'ready' => Icons.radio_button_checked,
  _ => Icons.radio_button_unchecked,
};
