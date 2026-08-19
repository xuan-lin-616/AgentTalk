import 'package:flutter/material.dart';

import '../theme/studio_colors.dart';

/// Real-data agent management page.
///
/// Every count and card is derived from the Core projection snapshot passed
/// in by the shell (`agents`, `assignments`, `runs`). This widget performs no
/// IPC itself and contains no hardcoded mock agents, candidates, paths, or
/// statistic numbers.
class AgentManagementView extends StatefulWidget {
  const AgentManagementView({
    super.key,
    required this.snapshot,
    this.projectId,
    this.onAdd,
    this.onEdit,
    this.onScanLocal,
    this.onManageAssignments,
    this.onCreateProject,
    this.onSelectProject,
  });

  final Map<String, dynamic> snapshot;
  final String? projectId;
  final VoidCallback? onAdd;
  final ValueChanged<Map<String, dynamic>>? onEdit;
  final VoidCallback? onScanLocal;
  final VoidCallback? onManageAssignments;
  final VoidCallback? onCreateProject;
  final VoidCallback? onSelectProject;

  @override
  State<AgentManagementView> createState() => _AgentManagementViewState();
}

class _AgentManagementViewState extends State<AgentManagementView> {
  final TextEditingController _searchController = TextEditingController();
  String _query = '';
  String _filter = '全部';

  @override
  void dispose() {
    _searchController.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    final hasProject = widget.projectId?.isNotEmpty == true;
    final roster = _projectAgents(widget.snapshot, widget.projectId);
    final allAgents = _list(widget.snapshot, 'agents');
    final readyCount = roster
        .where((agent) {
          final status = _agentStatus(widget.snapshot, agent['id']?.toString());
          return status == '待命' ||
              status == '已完成' ||
              status == '失败' ||
              status == '已取消' ||
              status == '已中断';
        })
        .length;
    final query = _query.trim().toLowerCase();
    final visible = roster.where((agent) {
      if (query.isNotEmpty) {
        final name = agent['name']?.toString().toLowerCase() ?? '';
        final role = agent['role']?.toString().toLowerCase() ?? '';
        final specialty = agent['specialty']?.toString().toLowerCase() ?? '';
        if (!name.contains(query) &&
            !role.contains(query) &&
            !specialty.contains(query)) {
          return false;
        }
      }
      if (_filter == '就绪') {
        final status = _agentStatus(widget.snapshot, agent['id']?.toString());
        return status == '待命' ||
            status == '已完成' ||
            status == '失败' ||
            status == '已取消' ||
            status == '已中断';
      }
      if (_filter == '运行中') {
        final status = _agentStatus(widget.snapshot, agent['id']?.toString());
        return status == '运行中' ||
            status == '准备中' ||
            status == '排队中' ||
            status == '验证中';
      }
      if (_filter == '已停止') {
        final status = _agentStatus(widget.snapshot, agent['id']?.toString());
        return status == '已取消' ||
            status == '已中断' ||
            status == '失败';
      }
      return true;
    }).toList(growable: false);

    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        _StatsHeader(
          readyCount: readyCount,
          discoveredCount: allAgents.length,
        ),
        const Divider(height: 1, color: StudioColors.borderSubtle),
        _Toolbar(
          searchController: _searchController,
          filter: _filter,
          onQueryChanged: (value) => setState(() => _query = value),
          onFilterChanged: (value) => setState(() => _filter = value),
          onAdd: widget.onAdd,
          onScanLocal: widget.onScanLocal,
          onManageAssignments: widget.onManageAssignments,
        ),
        Expanded(child: _buildBody(hasProject, roster, visible)),
      ],
    );
  }

  Widget _buildBody(
    bool hasProject,
    List<Map<String, dynamic>> roster,
    List<Map<String, dynamic>> visible,
  ) {
    if (!hasProject) {
      return _EmptyState(
        icon: Icons.folder_open_outlined,
        title: '还没有选择项目',
        subtitle: '创建项目或从现有项目中选择一个，再开始管理智能体。',
        actions: [
          if (widget.onCreateProject != null)
            FilledButton.icon(
              onPressed: widget.onCreateProject,
              icon: const Icon(Icons.add_outlined, size: 16),
              label: const Text('创建项目'),
            ),
          if (widget.onSelectProject != null)
            OutlinedButton.icon(
              onPressed: widget.onSelectProject,
              icon: const Icon(Icons.folder_open_outlined, size: 16),
              label: const Text('选择项目'),
            ),
        ],
      );
    }
    if (roster.isEmpty) {
      return _EmptyState(
        icon: Icons.radar_outlined,
        title: '当前项目还没有智能体',
        subtitle: '扫描本地智能体，或手动创建智能体。',
        actions: [
          if (widget.onScanLocal != null)
            FilledButton.icon(
              onPressed: widget.onScanLocal,
              icon: const Icon(Icons.radar_outlined, size: 16),
              label: const Text('扫描本地智能体'),
            ),
          if (widget.onAdd != null)
            OutlinedButton.icon(
              onPressed: widget.onAdd,
              icon: const Icon(Icons.add_outlined, size: 16),
              label: const Text('创建智能体'),
            ),
        ],
      );
    }
    if (visible.isEmpty) {
      return const _EmptyState(
        icon: Icons.search_outlined,
        title: '没有匹配的智能体',
        subtitle: '尝试清空搜索或切换筛选条件。',
      );
    }
    return LayoutBuilder(
      builder: (context, constraints) {
        final columns = (constraints.maxWidth / 280).floor().clamp(1, 6);
        return GridView.builder(
          padding: const EdgeInsets.all(16),
          gridDelegate: SliverGridDelegateWithFixedCrossAxisCount(
            crossAxisCount: columns,
            crossAxisSpacing: 12,
            mainAxisSpacing: 12,
            childAspectRatio: 1.9,
          ),
          itemCount: visible.length,
          itemBuilder: (context, index) {
            final agent = visible[index];
            return _AgentCard(
              agent: agent,
              status: _agentStatus(widget.snapshot, agent['id']?.toString()),
              onEdit: widget.onEdit == null
                  ? null
                  : () => widget.onEdit!(agent),
            );
          },
        );
      },
    );
  }
}

class _StatsHeader extends StatelessWidget {
  const _StatsHeader({required this.readyCount, required this.discoveredCount});

  final int readyCount;
  final int discoveredCount;

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.fromLTRB(16, 14, 16, 14),
      child: Row(
        children: [
          _StatCard(
            icon: Icons.check_circle_outline,
            label: '就绪',
            value: readyCount,
            color: StudioColors.success,
          ),
          const SizedBox(width: 12),
          _StatCard(
            icon: Icons.radar_outlined,
            label: '已发现',
            value: discoveredCount,
            color: StudioColors.primaryHover,
          ),
        ],
      ),
    );
  }
}

class _StatCard extends StatelessWidget {
  const _StatCard({
    required this.icon,
    required this.label,
    required this.value,
    required this.color,
  });

  final IconData icon;
  final String label;
  final int value;
  final Color color;

  @override
  Widget build(BuildContext context) {
    return Expanded(
      child: Container(
        padding: const EdgeInsets.symmetric(horizontal: 14, vertical: 12),
        decoration: BoxDecoration(
          color: StudioColors.bgCard,
          borderRadius: BorderRadius.circular(10),
          border: Border.all(color: StudioColors.borderSubtle),
        ),
        child: Row(
          children: [
            Icon(icon, size: 18, color: color),
            const SizedBox(width: 10),
            Text(
              label,
              style: const TextStyle(
                color: StudioColors.textSecondary,
                fontSize: 12,
              ),
            ),
            const Spacer(),
            Text(
              '$value',
              style: const TextStyle(
                color: StudioColors.textPrimary,
                fontSize: 18,
                fontWeight: FontWeight.w700,
              ),
            ),
          ],
        ),
      ),
    );
  }
}

class _Toolbar extends StatelessWidget {
  const _Toolbar({
    required this.searchController,
    required this.filter,
    required this.onQueryChanged,
    required this.onFilterChanged,
    this.onAdd,
    this.onScanLocal,
    this.onManageAssignments,
  });

  final TextEditingController searchController;
  final String filter;
  final ValueChanged<String> onQueryChanged;
  final ValueChanged<String> onFilterChanged;
  final VoidCallback? onAdd;
  final VoidCallback? onScanLocal;
  final VoidCallback? onManageAssignments;

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.fromLTRB(16, 10, 16, 10),
      child: Wrap(
        spacing: 8,
        runSpacing: 8,
        crossAxisAlignment: WrapCrossAlignment.center,
        children: [
          SizedBox(
            width: 240,
            child: TextField(
              controller: searchController,
              onChanged: onQueryChanged,
              decoration: const InputDecoration(
                hintText: '搜索智能体',
                prefixIcon: Icon(Icons.search_outlined, size: 16),
                isDense: true,
              ),
            ),
          ),
          for (final label in const ['全部', '就绪', '运行中', '已停止'])
            ChoiceChip(
              label: Text(label),
              selected: filter == label,
              onSelected: (_) => onFilterChanged(label),
            ),
          const SizedBox(width: 4),
          if (onScanLocal != null)
            OutlinedButton.icon(
              onPressed: onScanLocal,
              icon: const Icon(Icons.radar_outlined, size: 16),
              label: const Text('扫描本地智能体'),
            ),
          if (onManageAssignments != null)
            OutlinedButton.icon(
              onPressed: onManageAssignments,
              icon: const Icon(Icons.tune_outlined, size: 16),
              label: const Text('管理分配'),
            ),
          if (onAdd != null)
            FilledButton.icon(
              onPressed: onAdd,
              icon: const Icon(Icons.add_outlined, size: 16),
              label: const Text('创建智能体'),
            ),
        ],
      ),
    );
  }
}

class _AgentCard extends StatelessWidget {
  const _AgentCard({
    required this.agent,
    required this.status,
    this.onEdit,
  });

  final Map<String, dynamic> agent;
  final String status;
  final VoidCallback? onEdit;

  @override
  Widget build(BuildContext context) {
    final color = switch (status) {
      '运行中' || '准备中' || '排队中' || '验证中' => StudioColors.success,
      '已完成' || '待命' => StudioColors.primaryHover,
      '失败' || '已取消' || '已中断' => StudioColors.danger,
      _ => StudioColors.inactive,
    };
    return Container(
      padding: const EdgeInsets.all(12),
      decoration: BoxDecoration(
        color: StudioColors.bgCard,
        borderRadius: BorderRadius.circular(10),
        border: Border.all(color: StudioColors.borderSubtle),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Row(
            children: [
              CircleAvatar(
                radius: 14,
                backgroundColor: color.withValues(alpha: 0.16),
                child: Icon(Icons.smart_toy_outlined, size: 15, color: color),
              ),
              const SizedBox(width: 8),
              Expanded(
                child: Text(
                  agent['name']?.toString() ?? agent['id']?.toString() ?? 'Agent',
                  maxLines: 1,
                  overflow: TextOverflow.ellipsis,
                  style: const TextStyle(
                    color: StudioColors.textPrimary,
                    fontSize: 12,
                    fontWeight: FontWeight.w700,
                  ),
                ),
              ),
              Container(
                width: 8,
                height: 8,
                decoration: BoxDecoration(color: color, shape: BoxShape.circle),
              ),
              const SizedBox(width: 6),
              Text(
                status,
                style: TextStyle(
                  color: color,
                  fontSize: 10,
                  fontWeight: FontWeight.w600,
                ),
              ),
              if (onEdit != null) ...[
                const SizedBox(width: 4),
                IconButton(
                  tooltip: '编辑智能体',
                  onPressed: onEdit,
                  icon: const Icon(Icons.more_vert_outlined, size: 16),
                  visualDensity: VisualDensity.compact,
                ),
              ],
            ],
          ),
          const SizedBox(height: 6),
          Text(
            '${agent['role']?.toString() ?? '智能体'}'
            '${agent['specialty']?.toString().isNotEmpty == true ? ' · ${agent['specialty']}' : ''}',
            maxLines: 1,
            overflow: TextOverflow.ellipsis,
            style: const TextStyle(
              color: StudioColors.textSecondary,
              fontSize: 10,
            ),
          ),
        ],
      ),
    );
  }
}

class _EmptyState extends StatelessWidget {
  const _EmptyState({
    required this.icon,
    required this.title,
    required this.subtitle,
    this.actions = const [],
  });

  final IconData icon;
  final String title;
  final String subtitle;
  final List<Widget> actions;

  @override
  Widget build(BuildContext context) {
    return Center(
      child: Column(
        mainAxisSize: MainAxisSize.min,
        children: [
          Icon(icon, size: 42, color: StudioColors.textTertiary),
          const SizedBox(height: 10),
          Text(
            title,
            style: const TextStyle(
              color: StudioColors.textPrimary,
              fontSize: 13,
              fontWeight: FontWeight.w700,
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
          if (actions.isNotEmpty) ...[
            const SizedBox(height: 14),
            Wrap(spacing: 8, children: actions),
          ],
        ],
      ),
    );
  }
}

List<Map<String, dynamic>> _list(Map<String, dynamic> snapshot, String key) {
  final value = snapshot[key];
  if (value is! List) return const <Map<String, dynamic>>[];
  return value.whereType<Map<String, dynamic>>().toList(growable: false);
}

List<Map<String, dynamic>> _projectAgents(
  Map<String, dynamic> snapshot,
  String? projectId,
) {
  if (projectId == null || projectId.isEmpty) return const [];
  final agentsById = <String, Map<String, dynamic>>{
    for (final agent in _list(snapshot, 'agents'))
      if (agent['id']?.toString().isNotEmpty == true)
        agent['id'].toString(): agent,
  };
  return _list(snapshot, 'assignments')
      .where(
        (assignment) =>
            assignment['projectId']?.toString() == projectId &&
            assignment['enabled'] == true,
      )
      .map((assignment) => agentsById[assignment['agentId']?.toString()])
      .whereType<Map<String, dynamic>>()
      .toList(growable: false);
}

String _agentStatus(Map<String, dynamic> snapshot, String? agentId) {
  if (agentId == null || agentId.isEmpty) return '待命';
  final runs = _list(
    snapshot,
    'runs',
  ).where((run) => run['agentId'] == agentId).toList(growable: false);
  if (runs.isEmpty) return '待命';
  return _runStatusLabel(runs.last['status']?.toString());
}

String _runStatusLabel(String? status) => switch (status?.toLowerCase()) {
  'pending' => '排队中',
  'assembling' => '准备中',
  'awaiting_approval' => '待确认',
  'running' => '运行中',
  'verifying' => '验证中',
  'completed' => '已完成',
  'failed' => '失败',
  'cancelled' => '已取消',
  'interrupted' => '已中断',
  _ => '待命',
};
