import 'package:flutter/material.dart';

import '../theme/studio_colors.dart';

/// Header shown inside the workbench section.
///
/// It intentionally has no project/conversation pickers (those live in the
/// global [StudioTitleBar]). It only frames the current section and shows the
/// live Core projection status.
class StudioWorkbenchHeader extends StatelessWidget {
  const StudioWorkbenchHeader({
    super.key,
    required this.sectionTitle,
    required this.status,
    this.trailing,
  });

  final String sectionTitle;
  final String status;
  final Widget? trailing;

  @override
  Widget build(BuildContext context) {
    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 10),
      decoration: const BoxDecoration(
        color: StudioColors.bgSurface,
        border: Border(bottom: BorderSide(color: StudioColors.borderSubtle)),
      ),
      child: Row(
        children: [
          Text(
            sectionTitle,
            style: const TextStyle(
              color: StudioColors.textPrimary,
              fontSize: 14,
              fontWeight: FontWeight.w600,
            ),
          ),
          const SizedBox(width: 12),
          const Icon(
            Icons.edit_outlined,
            size: 14,
            color: StudioColors.textTertiary,
          ),
          const Spacer(),
          Text(
            status,
            style: const TextStyle(
              color: StudioColors.textSecondary,
              fontSize: 11,
            ),
          ),
          if (trailing != null) ...[const SizedBox(width: 12), trailing!],
        ],
      ),
    );
  }
}

/// Sealed-DAG canvas placeholder for Phase 1.
///
/// Phase 3 replaces this box with the real read-only flow canvas fed by
/// `orchestration.run.snapshot`. This placeholder renders no fake nodes or
/// edges; it is an explicit empty state.
class StudioCanvasPlaceholder extends StatelessWidget {
  const StudioCanvasPlaceholder({super.key});

  @override
  Widget build(BuildContext context) {
    return Container(
      color: StudioColors.bgRoot,
      alignment: Alignment.center,
      child: Column(
        mainAxisSize: MainAxisSize.min,
        children: [
          Icon(
            Icons.account_tree_outlined,
            size: 46,
            color: StudioColors.textTertiary,
          ),
          const SizedBox(height: 12),
          const Text(
            '流程画布',
            style: TextStyle(
              color: StudioColors.textPrimary,
              fontSize: 14,
              fontWeight: FontWeight.w600,
            ),
          ),
          const SizedBox(height: 6),
          const Text(
            'Phase 3 将在此接入真实 sealed DAG（orchestration.run.snapshot）',
            style: TextStyle(color: StudioColors.textSecondary, fontSize: 11),
          ),
        ],
      ),
    );
  }
}

/// Log panel placeholder for Phase 1.
///
/// Phase 2 replaces this box with the real event-fed execution log
/// (`events.subscribe(core-events)` + `events.replay`). No fake log lines are
/// rendered.
class StudioLogPlaceholder extends StatelessWidget {
  const StudioLogPlaceholder({super.key});

  @override
  Widget build(BuildContext context) {
    return Container(
      color: StudioColors.bgSurface,
      alignment: Alignment.center,
      child: Column(
        mainAxisSize: MainAxisSize.min,
        children: [
          Icon(
            Icons.list_alt_outlined,
            size: 34,
            color: StudioColors.textTertiary,
          ),
          const SizedBox(height: 8),
          const Text(
            '运行日志',
            style: TextStyle(
              color: StudioColors.textPrimary,
              fontSize: 12,
              fontWeight: FontWeight.w600,
            ),
          ),
          const SizedBox(height: 4),
          const Text(
            'Phase 2 将接入真实事件流日志',
            style: TextStyle(color: StudioColors.textSecondary, fontSize: 10),
          ),
        ],
      ),
    );
  }
}

/// Generic section placeholder used by knowledge/tools/logs/settings until
/// their full views land in later phases.
class StudioSectionPlaceholder extends StatelessWidget {
  const StudioSectionPlaceholder({
    super.key,
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
            style: const TextStyle(
              color: StudioColors.textSecondary,
              fontSize: 11,
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
    );
  }
}
