import 'package:flutter/material.dart';

import '../theme/studio_colors.dart';
import 'studio_approval_request.dart';

/// Floating HITL approval strip.
///
/// Renders real `tool.requested` / `handoff.proposed` events only. When the
/// event payload carries a `handoffId`, approve/reject are wired to
/// `handoff.approve` / `handoff.reject` by the shell. Tool requests without a
/// `handoffId` are shown read-only with an explicit backend gap marker.
class ApprovalPanel extends StatelessWidget {
  const ApprovalPanel({
    super.key,
    required this.requests,
    required this.busy,
    required this.onApprove,
    required this.onReject,
    required this.onDismiss,
  });

  final List<StudioApprovalRequest> requests;
  final bool busy;
  final ValueChanged<StudioApprovalRequest> onApprove;
  final ValueChanged<StudioApprovalRequest> onReject;
  final ValueChanged<StudioApprovalRequest> onDismiss;

  @override
  Widget build(BuildContext context) {
    if (requests.isEmpty) return const SizedBox.shrink();
    return Container(
      color: StudioColors.bgCard,
      padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 8),
      child: Row(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          const Icon(
            Icons.gavel_outlined,
            size: 18,
            color: StudioColors.warning,
          ),
          const SizedBox(width: 10),
          Expanded(
            child: Wrap(
              spacing: 10,
              runSpacing: 8,
              children: [
                for (final request in requests)
                  _ApprovalCard(
                    request: request,
                    busy: busy,
                    onApprove: () => onApprove(request),
                    onReject: () => onReject(request),
                    onDismiss: () => onDismiss(request),
                  ),
              ],
            ),
          ),
        ],
      ),
    );
  }
}

class _ApprovalCard extends StatelessWidget {
  const _ApprovalCard({
    required this.request,
    required this.busy,
    required this.onApprove,
    required this.onReject,
    required this.onDismiss,
  });

  final StudioApprovalRequest request;
  final bool busy;
  final VoidCallback onApprove;
  final VoidCallback onReject;
  final VoidCallback onDismiss;

  @override
  Widget build(BuildContext context) {
    final color = studioApprovalColor(request.kind);
    final canAct = request.hasApprovalCommand;
    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 6),
      decoration: BoxDecoration(
        color: StudioColors.bgSurface,
        borderRadius: BorderRadius.circular(8),
        border: Border.all(color: color.withValues(alpha: 0.55)),
      ),
      child: Row(
        mainAxisSize: MainAxisSize.min,
        children: [
          Icon(
            request.kind == StudioApprovalKind.tool
                ? Icons.handyman_outlined
                : Icons.redo_outlined,
            size: 16,
            color: color,
          ),
          const SizedBox(width: 8),
          ConstrainedBox(
            constraints: const BoxConstraints(maxWidth: 360),
            child: Text(
              request.message,
              maxLines: 1,
              overflow: TextOverflow.ellipsis,
              style: const TextStyle(
                color: StudioColors.textPrimary,
                fontSize: 11,
              ),
            ),
          ),
          const SizedBox(width: 8),
          if (canAct) ...[
            FilledButton(
              key: Key('approval-approve-${request.id}'),
              onPressed: busy ? null : onApprove,
              style: FilledButton.styleFrom(
                visualDensity: VisualDensity.compact,
                backgroundColor: StudioColors.success,
              ),
              child: const Text('批准'),
            ),
            const SizedBox(width: 6),
            OutlinedButton(
              key: Key('approval-reject-${request.id}'),
              onPressed: busy ? null : onReject,
              style: OutlinedButton.styleFrom(
                visualDensity: VisualDensity.compact,
              ),
              child: const Text('拒绝'),
            ),
          ] else ...[
            const Text(
              '后端待补：schema 缺 tool.approve/deny 命令', // TODO(接真实X)
              style: TextStyle(color: StudioColors.textTertiary, fontSize: 10),
            ),
          ],
          IconButton(
            key: Key('approval-dismiss-${request.id}'),
            tooltip: '忽略',
            visualDensity: VisualDensity.compact,
            onPressed: busy ? null : onDismiss,
            icon: const Icon(Icons.close, size: 14),
          ),
        ],
      ),
    );
  }
}
