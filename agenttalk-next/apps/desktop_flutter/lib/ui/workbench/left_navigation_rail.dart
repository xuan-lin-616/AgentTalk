import 'package:flutter/material.dart';

import '../theme/studio_colors.dart';

/// Fixed studio navigation rail.
///
/// Section indices are owned by the shell (see [StudioSection] usage in
/// `main.dart`), not by this file. The rail only renders the selected state
/// and reports taps.
class LeftNavigationRail extends StatelessWidget {
  const LeftNavigationRail({
    super.key,
    required this.selectedIndex,
    required this.onSelect,
  });

  final int selectedIndex;
  final ValueChanged<int> onSelect;

  static const List<({IconData icon, String label})> items = [
    (icon: Icons.dashboard_outlined, label: '工作台'),
    (icon: Icons.smart_toy_outlined, label: '智能体管理'),
    (icon: Icons.assignment_outlined, label: '任务管理'),
    (icon: Icons.folder_open_rounded, label: '知识库'),
    (icon: Icons.build_outlined, label: '工具管理'),
    (icon: Icons.chat_bubble_outline_rounded, label: '对话中心'),
    (icon: Icons.list_alt_rounded, label: '日志中心'),
    (icon: Icons.settings_outlined, label: '设置'),
  ];

  @override
  Widget build(BuildContext context) {
    return Container(
      width: 150,
      decoration: const BoxDecoration(
        color: StudioColors.bgSurface,
        border: Border(right: BorderSide(color: StudioColors.borderSubtle)),
      ),
      child: Column(
        children: [
          const SizedBox(height: 12),
          for (final item in items.indexed)
            _StudioNavItem(
              icon: item.$2.icon,
              label: item.$2.label,
              selected: item.$1 == selectedIndex,
              onTap: () => onSelect(item.$1),
            ),
          const Spacer(),
          const SystemResourcePlaceholder(),
        ],
      ),
    );
  }
}

class _StudioNavItem extends StatelessWidget {
  const _StudioNavItem({
    required this.icon,
    required this.label,
    required this.selected,
    required this.onTap,
  });

  final IconData icon;
  final String label;
  final bool selected;
  final VoidCallback onTap;

  @override
  Widget build(BuildContext context) {
    return Container(
      margin: const EdgeInsets.symmetric(horizontal: 8, vertical: 3),
      child: Material(
        color: selected
            ? StudioColors.primary.withValues(alpha: 0.15)
            : Colors.transparent,
        borderRadius: BorderRadius.circular(6),
        child: InkWell(
          onTap: onTap,
          borderRadius: BorderRadius.circular(6),
          child: Padding(
            padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 8),
            child: Row(
              children: [
                Icon(
                  icon,
                  size: 16,
                  color: selected
                      ? StudioColors.primaryHover
                      : StudioColors.textSecondary,
                ),
                const SizedBox(width: 10),
                Text(
                  label,
                  style: TextStyle(
                    color: selected ? Colors.white : StudioColors.textSecondary,
                    fontSize: 12,
                    fontWeight: selected ? FontWeight.w600 : FontWeight.normal,
                  ),
                ),
              ],
            ),
          ),
        ),
      ),
    );
  }
}

/// Static hardware-monitor placeholder. The Core has no process/resource
/// telemetry in IPC v1, so no fake values are rendered. Phase 5 will decide
/// whether this stays a placeholder or gets removed.
class SystemResourcePlaceholder extends StatelessWidget {
  const SystemResourcePlaceholder({super.key});

  @override
  Widget build(BuildContext context) {
    return Container(
      padding: const EdgeInsets.all(12),
      decoration: const BoxDecoration(
        border: Border(top: BorderSide(color: StudioColors.borderSubtle)),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          const Row(
            mainAxisAlignment: MainAxisAlignment.spaceBetween,
            children: [
              Text(
                '系统资源',
                style: TextStyle(
                  color: StudioColors.textSecondary,
                  fontSize: 11,
                  fontWeight: FontWeight.w600,
                ),
              ),
              Icon(
                Icons.tune_rounded,
                size: 12,
                color: StudioColors.textTertiary,
              ),
            ],
          ),
          const SizedBox(height: 10),
          const _MetricRow(
            icon: Icons.memory_rounded,
            label: 'CPU',
            value: '--', // TODO(接真实X): Core 无进程监控能力
          ),
          const _MetricRow(
            icon: Icons.dns_outlined,
            label: '内存',
            value: '--', // TODO(接真实X): Core 无进程监控能力
          ),
          const _MetricRow(
            icon: Icons.developer_board_rounded,
            label: 'GPU',
            value: '--', // TODO(接真实X): Core 无进程监控能力
          ),
          const SizedBox(height: 10),
          const Text(
            '版本: v1.0.0 (本地版)',
            style: TextStyle(color: StudioColors.textTertiary, fontSize: 10),
          ),
        ],
      ),
    );
  }
}

class _MetricRow extends StatelessWidget {
  const _MetricRow({
    required this.icon,
    required this.label,
    required this.value,
  });

  final IconData icon;
  final String label;
  final String value;

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.symmetric(vertical: 3),
      child: Row(
        children: [
          Icon(icon, size: 13, color: StudioColors.textTertiary),
          const SizedBox(width: 6),
          Text(
            label,
            style: const TextStyle(
              color: StudioColors.textSecondary,
              fontSize: 10,
            ),
          ),
          const Spacer(),
          Text(
            value,
            textAlign: TextAlign.right,
            style: const TextStyle(
              color: StudioColors.textPrimary,
              fontSize: 10,
              fontWeight: FontWeight.w500,
            ),
          ),
        ],
      ),
    );
  }
}
