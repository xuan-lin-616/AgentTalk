import 'package:flutter/material.dart';

import '../../gen/l10n.dart';
import '../theme/studio_colors.dart';

/// Top-level studio title bar.
///
/// It is intentionally a pure layout component: all business actions are
/// injected as callbacks by WorkspaceShellState, which owns the IPC client
/// and the Core projection. No IPC and no mock data live here.
class StudioTitleBar extends StatelessWidget {
  const StudioTitleBar({
    super.key,
    required this.compact,
    required this.snapshot,
    required this.projectionStatus,
    this.onProjectPressed,
    this.onConversationPressed,
    this.onConnectorCenterPressed,
    this.onDiagnosticsPressed,
    this.onConfigTransferPressed,
    this.onSearchPressed,
    this.onToggleTheme,
    this.onToggleAgentPanel,
    this.onToggleWorkflowPanel,
    this.onShowAgentPanel,
    this.onShowWorkflowPanel,
  });

  final bool compact;
  final Map<String, dynamic> snapshot;
  final String projectionStatus;
  final VoidCallback? onProjectPressed;
  final VoidCallback? onConversationPressed;
  final VoidCallback? onConnectorCenterPressed;
  final VoidCallback? onDiagnosticsPressed;
  final VoidCallback? onConfigTransferPressed;
  final VoidCallback? onSearchPressed;
  final VoidCallback? onToggleTheme;
  final VoidCallback? onToggleAgentPanel;
  final VoidCallback? onToggleWorkflowPanel;
  final VoidCallback? onShowAgentPanel;
  final VoidCallback? onShowWorkflowPanel;

  @override
  Widget build(BuildContext context) {
    final l10n = AppLocalizations.of(context);
    return Container(
      height: 52,
      padding: const EdgeInsets.symmetric(horizontal: 16),
      decoration: const BoxDecoration(
        color: StudioColors.bgSurface,
        border: Border(bottom: BorderSide(color: StudioColors.borderSubtle)),
      ),
      child: Row(
        children: [
          const Icon(
            Icons.hub_outlined,
            color: StudioColors.nodeAnalyzer,
            size: 20,
          ),
          const SizedBox(width: 10),
          Text(
            l10n?.title ?? 'AgentTalk',
            style: Theme.of(context).textTheme.titleMedium?.copyWith(
              color: StudioColors.textPrimary,
              fontWeight: FontWeight.w700,
            ),
          ),
          const SizedBox(width: 14),
          Expanded(
            child: Row(
              children: [
                Flexible(
                  child: _ConnectionStatusChip(status: projectionStatus),
                ),
                const SizedBox(width: 12),
                if (compact) ...[
                  IconButton(
                    tooltip: l10n?.project ?? '项目',
                    onPressed: onProjectPressed,
                    icon: const Icon(Icons.folder_open_outlined, size: 18),
                  ),
                  IconButton(
                    tooltip: l10n?.conversation ?? '会话',
                    onPressed: onConversationPressed,
                    icon: const Icon(Icons.chat_bubble_outline, size: 18),
                  ),
                ] else ...[
                  Flexible(
                    child: _PickerButton(
                      icon: Icons.folder_open_outlined,
                      label: _firstName(
                        snapshot,
                        'projects',
                        l10n?.project ?? '项目',
                      ),
                      tooltip: l10n?.project ?? '项目',
                      onPressed: onProjectPressed,
                    ),
                  ),
                  const SizedBox(width: 8),
                  Flexible(
                    child: _PickerButton(
                      icon: Icons.chat_bubble_outline,
                      label: _firstName(
                        snapshot,
                        'conversations',
                        l10n?.conversation ?? '会话',
                        'title',
                      ),
                      tooltip: l10n?.conversation ?? '会话',
                      onPressed: onConversationPressed,
                    ),
                  ),
                ],
              ],
            ),
          ),
          const SizedBox(width: 8),
          if (!compact)
            IconButton(
              tooltip: l10n?.connectorCenter ?? '连接器管理',
              onPressed: onConnectorCenterPressed,
              icon: const Icon(Icons.extension_outlined, size: 20),
            ),
          if (!compact)
            IconButton(
              tooltip: l10n?.diagnostics ?? '高级诊断',
              onPressed: onDiagnosticsPressed,
              icon: const Icon(Icons.monitor_heart_outlined, size: 20),
            ),
          IconButton(
            tooltip: '导入/导出配置',
            onPressed: onConfigTransferPressed,
            icon: const Icon(Icons.import_export, size: 20),
          ),
          if (!compact)
            IconButton(
              tooltip: l10n?.searchMessages ?? '搜索消息',
              onPressed: onSearchPressed,
              icon: const Icon(Icons.search_outlined, size: 20),
            ),
          if (!compact)
            IconButton(
              tooltip: l10n?.agentPanel ?? '智能体面板',
              onPressed: onToggleAgentPanel,
              icon: const Icon(Icons.people_outline, size: 20),
            ),
          if (!compact)
            IconButton(
              tooltip: l10n?.workflowPanel ?? '工作流面板',
              onPressed: onToggleWorkflowPanel,
              icon: const Icon(Icons.view_sidebar_outlined, size: 20),
            ),
          if (compact)
            IconButton(
              tooltip: l10n?.agentPanel ?? '智能体面板',
              onPressed: onShowAgentPanel,
              icon: const Icon(Icons.people_outline, size: 20),
            ),
          if (compact)
            IconButton(
              tooltip: l10n?.workflowPanel ?? '工作流面板',
              onPressed: onShowWorkflowPanel,
              icon: const Icon(Icons.account_tree_outlined, size: 20),
            ),
          const SizedBox(width: 8),
          IconButton(
            tooltip: l10n?.toggleTheme ?? '切换主题',
            onPressed: onToggleTheme,
            icon: const Icon(Icons.brightness_6_outlined, size: 20),
          ),
        ],
      ),
    );
  }
}

class _PickerButton extends StatelessWidget {
  const _PickerButton({
    required this.icon,
    required this.label,
    required this.tooltip,
    this.onPressed,
  });

  final IconData icon;
  final String label;
  final String tooltip;
  final VoidCallback? onPressed;

  @override
  Widget build(BuildContext context) {
    return OutlinedButton.icon(
      onPressed: onPressed,
      icon: Icon(icon, size: 16),
      label: Text(label, maxLines: 1, overflow: TextOverflow.ellipsis),
    );
  }
}

class _ConnectionStatusChip extends StatelessWidget {
  const _ConnectionStatusChip({required this.status});

  final String status;

  @override
  Widget build(BuildContext context) {
    final connected =
        status.contains('已连接') ||
        status.contains('已恢复') ||
        status.contains('事件订阅');
    final color = connected ? StudioColors.success : StudioColors.warning;
    return ConstrainedBox(
      constraints: const BoxConstraints(maxWidth: 260),
      child: Container(
        padding: const EdgeInsets.symmetric(horizontal: 8, vertical: 3),
        decoration: BoxDecoration(
          color: color.withValues(alpha: 0.12),
          borderRadius: BorderRadius.circular(12),
        ),
        child: Row(
          mainAxisSize: MainAxisSize.min,
          children: [
            Container(
              width: 6,
              height: 6,
              decoration: BoxDecoration(color: color, shape: BoxShape.circle),
            ),
            const SizedBox(width: 6),
            Flexible(
              child: Text(
                connected ? '本地服务运行中' : status,
                maxLines: 1,
                overflow: TextOverflow.ellipsis,
                softWrap: false,
                style: TextStyle(color: color, fontSize: 11),
              ),
            ),
          ],
        ),
      ),
    );
  }
}

String _firstName(
  Map<String, dynamic> snapshot,
  String key,
  String fallback, [
  String labelKey = 'name',
]) {
  final value = snapshot[key];
  if (value is! List || value.isEmpty) return fallback;
  for (final entry in value) {
    if (entry is! Map<String, dynamic>) continue;
    final label = entry[labelKey]?.toString();
    if (label != null && label.isNotEmpty) return label;
  }
  return fallback;
}
