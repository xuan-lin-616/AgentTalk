import 'package:flutter/material.dart';

typedef ProjectAgentAssignmentSetRequest =
    Future<void> Function({
      required String projectId,
      required String agentId,
      required bool enabled,
      required String workspaceAccess,
    });

typedef ProjectAgentAssignmentRemoveRequest =
    Future<void> Function({required String projectId, required String agentId});

/// Displays the assignments already present in a Core projection.
///
/// The widget does not infer permissions, create agents, or mutate a roster.
/// It only renders the supplied snapshot and forwards set/remove requests to
/// the host. The host remains responsible for Core/IPC errors and for
/// supplying the refreshed snapshot after a successful mutation.
class ProjectAgentAssignmentPanel extends StatefulWidget {
  const ProjectAgentAssignmentPanel({
    super.key,
    this.snapshot,
    this.projects = const <Map<String, dynamic>>[],
    this.agents = const <Map<String, dynamic>>[],
    this.assignments = const <Map<String, dynamic>>[],
    this.currentProjectId,
    this.onSet,
    this.onRemove,
    this.loading = false,
    this.disabled = false,
    this.error,
    this.disabledReason,
  });

  /// Optional whole projection snapshot. When present it is used as the
  /// source for `projects`, `agents`, and `assignments`.
  final Map<String, dynamic>? snapshot;

  /// The existing Project roster, Agent roster, and persisted assignments.
  /// These lists are intentionally read-only inputs to the widget.
  final List<Map<String, dynamic>> projects;
  final List<Map<String, dynamic>> agents;
  final List<Map<String, dynamic>> assignments;

  /// The Project whose assignments should be shown. The widget does not pick
  /// a Project implicitly when this value is null.
  final String? currentProjectId;

  /// Requests are forwarded without local permission validation.
  final ProjectAgentAssignmentSetRequest? onSet;
  final ProjectAgentAssignmentRemoveRequest? onRemove;

  /// `loading` and `disabled` are host-owned state. Both disable mutations.
  final bool loading;
  final bool disabled;
  final String? error;
  final String? disabledReason;

  factory ProjectAgentAssignmentPanel.fromSnapshot({
    Key? key,
    required Map<String, dynamic> snapshot,
    String? currentProjectId,
    ProjectAgentAssignmentSetRequest? onSet,
    ProjectAgentAssignmentRemoveRequest? onRemove,
    bool loading = false,
    bool disabled = false,
    String? error,
    String? disabledReason,
  }) {
    return ProjectAgentAssignmentPanel(
      key: key,
      snapshot: snapshot,
      currentProjectId: currentProjectId,
      onSet: onSet,
      onRemove: onRemove,
      loading: loading,
      disabled: disabled,
      error: error,
      disabledReason: disabledReason,
    );
  }

  @override
  State<ProjectAgentAssignmentPanel> createState() =>
      _ProjectAgentAssignmentPanelState();
}

class _ProjectAgentAssignmentPanelState
    extends State<ProjectAgentAssignmentPanel> {
  String? _pendingAction;
  String? _actionError;

  bool get _interactionDisabled =>
      widget.loading || widget.disabled || _pendingAction != null;

  bool get _canSet => !_interactionDisabled && widget.onSet != null;

  bool get _canRemove => !_interactionDisabled && widget.onRemove != null;

  @override
  Widget build(BuildContext context) {
    final data = _snapshotData();
    final project = _findById(data.projects, widget.currentProjectId);
    final projectId = widget.currentProjectId;
    final projectAssignments = projectId == null
        ? const <Map<String, dynamic>>[]
        : data.assignments
              .where(
                (assignment) =>
                    _stringValue(assignment, ['projectId', 'project_id']) ==
                    projectId,
              )
              .toList(growable: false);
    final assignedAgentIds = projectAssignments
        .map((assignment) => _stringValue(assignment, ['agentId', 'agent_id']))
        .whereType<String>()
        .toSet();
    final availableAgents = data.agents
        .where((agent) {
          final agentId = _stringValue(agent, ['id', 'agentId', 'agent_id']);
          return agentId != null && !assignedAgentIds.contains(agentId);
        })
        .toList(growable: false);

    final externalError = widget.error;
    final effectiveDisabled = widget.disabled || widget.loading;

    return Semantics(
      container: true,
      enabled: !effectiveDisabled,
      label: '项目智能体分配面板',
      child: Card(
        clipBehavior: Clip.antiAlias,
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.stretch,
          children: [
            _buildHeader(context, project, availableAgents),
            if (widget.loading)
              Semantics(
                liveRegion: true,
                label: '正在加载分配',
                child: const LinearProgressIndicator(
                  key: ValueKey('project-agent-assignment-loading'),
                ),
              ),
            Padding(
              padding: const EdgeInsets.fromLTRB(16, 12, 16, 16),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.stretch,
                children: [
                  if (externalError != null) ...[
                    _StatusCard(
                      key: const ValueKey('project-agent-assignment-error'),
                      icon: Icons.error_outline,
                      title: '错误状态',
                      message: externalError,
                      color: Theme.of(context).colorScheme.error,
                    ),
                  ] else if (project == null) ...[
                    _StatusCard(
                      key: const ValueKey('project-agent-assignment-empty'),
                      icon: Icons.folder_open_outlined,
                      title: projectId == null ? '没有选中的项目' : '暂无可用项目',
                      message: widget.loading ? '加载中' : '选择一个现有项目后查看其智能体分配。',
                    ),
                  ] else ...[
                    _ProjectSummary(project: project),
                    const SizedBox(height: 12),
                    if (widget.loading && projectAssignments.isEmpty)
                      const _StatusCard(
                        icon: Icons.hourglass_empty,
                        title: '加载中',
                        message: '正在读取当前项目的分配。',
                      )
                    else if (projectAssignments.isEmpty)
                      const _StatusCard(
                        key: ValueKey(
                          'project-agent-assignment-no-assignments',
                        ),
                        icon: Icons.group_outlined,
                        title: '当前项目暂无智能体分配',
                        message: '可从现有智能体列表请求新的分配。',
                      )
                    else
                      ...projectAssignments.map(
                        (assignment) => _buildAssignmentRow(
                          context,
                          projectId!,
                          assignment,
                          data.agents,
                        ),
                      ),
                    const SizedBox(height: 12),
                    _buildAddAgentControl(context, availableAgents),
                  ],
                  if (_actionError != null) ...[
                    const SizedBox(height: 12),
                    _StatusCard(
                      key: const ValueKey(
                        'project-agent-assignment-action-error',
                      ),
                      icon: Icons.sync_problem_outlined,
                      title: '请求失败',
                      message: _actionError!,
                      color: Theme.of(context).colorScheme.error,
                    ),
                  ],
                  if (widget.disabled) ...[
                    const SizedBox(height: 12),
                    _DisabledNotice(reason: widget.disabledReason),
                  ],
                  if (widget.loading)
                    const Padding(
                      padding: EdgeInsets.only(top: 12),
                      child: Text('加载中，分配操作暂不可用。'),
                    ),
                ],
              ),
            ),
          ],
        ),
      ),
    );
  }

  Widget _buildHeader(
    BuildContext context,
    Map<String, dynamic>? project,
    List<Map<String, dynamic>> availableAgents,
  ) {
    final projectName = project == null
        ? '项目智能体分配'
        : _displayName(project, fallback: '项目');
    final projectId = widget.currentProjectId;
    final canAdd = project != null && availableAgents.isNotEmpty && _canSet;

    return Padding(
      padding: const EdgeInsets.fromLTRB(16, 16, 12, 12),
      child: Wrap(
        alignment: WrapAlignment.spaceBetween,
        crossAxisAlignment: WrapCrossAlignment.center,
        runSpacing: 8,
        children: [
          Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Text(
                '项目智能体分配',
                style: Theme.of(
                  context,
                ).textTheme.titleMedium?.copyWith(fontWeight: FontWeight.w700),
              ),
              const SizedBox(height: 4),
              Text(
                projectId == null ? projectName : '$projectName · $projectId',
                style: Theme.of(context).textTheme.bodySmall,
              ),
            ],
          ),
          PopupMenuButton<String>(
            key: const ValueKey('project-agent-assignment-add'),
            enabled: canAdd,
            tooltip: '添加现有智能体',
            onSelected: (agentId) => _requestSet(
              projectId: projectId!,
              agentId: agentId,
              enabled: true,
              workspaceAccess: 'none',
              actionKey: 'set:$agentId',
            ),
            itemBuilder: (context) => availableAgents
                .map(
                  (agent) => PopupMenuItem<String>(
                    value: _stringValue(agent, ['id', 'agentId', 'agent_id']),
                    child: Text(_displayName(agent, fallback: '智能体')),
                  ),
                )
                .toList(growable: false),
            child: const Padding(
              padding: EdgeInsets.symmetric(horizontal: 8, vertical: 4),
              child: Row(
                mainAxisSize: MainAxisSize.min,
                children: [
                  Icon(Icons.person_add_alt_1_outlined, size: 18),
                  SizedBox(width: 6),
                  Text('添加智能体'),
                ],
              ),
            ),
          ),
        ],
      ),
    );
  }

  Widget _buildAddAgentControl(
    BuildContext context,
    List<Map<String, dynamic>> availableAgents,
  ) {
    if (availableAgents.isNotEmpty) return const SizedBox.shrink();
    return Text(
      '当前列表中没有未分配智能体；面板不会创建或扩展列表。',
      style: Theme.of(context).textTheme.bodySmall,
    );
  }

  Widget _buildAssignmentRow(
    BuildContext context,
    String projectId,
    Map<String, dynamic> assignment,
    List<Map<String, dynamic>> agents,
  ) {
    final agentId =
        _stringValue(assignment, ['agentId', 'agent_id']) ?? 'unknown-agent';
    final agent = _findById(agents, agentId);
    final agentName = agent == null
        ? agentId
        : _displayName(agent, fallback: agentId);
    final enabled = _boolValue(assignment, ['enabled']) ?? false;
    final workspaceAccess =
        _stringValue(assignment, ['workspaceAccess', 'workspace_access']) ??
        'none';
    final rowPending =
        _pendingAction != null &&
        (_pendingAction == 'set:$agentId' ||
            _pendingAction == 'remove:$agentId');

    return Card(
      key: ValueKey('project-agent-assignment-$agentId'),
      margin: const EdgeInsets.only(bottom: 10),
      child: Padding(
        padding: const EdgeInsets.fromLTRB(12, 10, 8, 10),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.stretch,
          children: [
            Row(
              children: [
                CircleAvatar(
                  radius: 17,
                  backgroundColor: Theme.of(
                    context,
                  ).colorScheme.primaryContainer,
                  child: Icon(
                    Icons.smart_toy_outlined,
                    size: 18,
                    color: Theme.of(context).colorScheme.onPrimaryContainer,
                  ),
                ),
                const SizedBox(width: 10),
                Expanded(
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      Text(
                        agentName,
                        style: Theme.of(context).textTheme.titleSmall?.copyWith(
                          fontWeight: FontWeight.w600,
                        ),
                      ),
                      Text(
                        agent == null ? '智能体 ID：$agentId' : agentId,
                        style: Theme.of(context).textTheme.bodySmall,
                      ),
                    ],
                  ),
                ),
                if (rowPending)
                  const SizedBox(
                    key: ValueKey('project-agent-assignment-pending'),
                    width: 18,
                    height: 18,
                    child: CircularProgressIndicator(strokeWidth: 2),
                  ),
                IconButton(
                  key: ValueKey('remove-project-agent-$agentId'),
                  tooltip: '移除智能体分配',
                  onPressed: _canRemove
                      ? () => _requestRemove(
                          projectId: projectId,
                          agentId: agentId,
                          actionKey: 'remove:$agentId',
                        )
                      : null,
                  icon: const Icon(Icons.remove_circle_outline),
                ),
              ],
            ),
            const SizedBox(height: 8),
            Wrap(
              spacing: 12,
              runSpacing: 8,
              crossAxisAlignment: WrapCrossAlignment.center,
              children: [
                Semantics(
                  label: '$agentName 是否启用',
                  value: enabled.toString(),
                  child: Row(
                    mainAxisSize: MainAxisSize.min,
                    children: [
                      Switch(
                        key: ValueKey('enabled-$agentId'),
                        value: enabled,
                        onChanged: _canSet
                            ? (value) => _requestSet(
                                projectId: projectId,
                                agentId: agentId,
                                enabled: value,
                                workspaceAccess: workspaceAccess,
                                actionKey: 'set:$agentId',
                              )
                            : null,
                      ),
                      Text('启用：${enabled ? '是' : '否'}'),
                    ],
                  ),
                ),
                _WorkspaceAccessSelector(
                  key: ValueKey('workspace-access-$agentId'),
                  value: workspaceAccess,
                  enabled: _canSet,
                  onChanged: (value) => _requestSet(
                    projectId: projectId,
                    agentId: agentId,
                    enabled: enabled,
                    workspaceAccess: value,
                    actionKey: 'set:$agentId',
                  ),
                ),
              ],
            ),
          ],
        ),
      ),
    );
  }

  Future<void> _requestSet({
    required String projectId,
    required String agentId,
    required bool enabled,
    required String workspaceAccess,
    required String actionKey,
  }) async {
    final callback = widget.onSet;
    if (callback == null || _interactionDisabled || !mounted) return;
    setState(() {
      _pendingAction = actionKey;
      _actionError = null;
    });
    try {
      await callback(
        projectId: projectId,
        agentId: agentId,
        enabled: enabled,
        workspaceAccess: workspaceAccess,
      );
    } catch (error) {
      if (mounted) setState(() => _actionError = _errorMessage(error));
    } finally {
      if (mounted) setState(() => _pendingAction = null);
    }
  }

  Future<void> _requestRemove({
    required String projectId,
    required String agentId,
    required String actionKey,
  }) async {
    final callback = widget.onRemove;
    if (callback == null || _interactionDisabled || !mounted) return;
    setState(() {
      _pendingAction = actionKey;
      _actionError = null;
    });
    try {
      await callback(projectId: projectId, agentId: agentId);
    } catch (error) {
      if (mounted) setState(() => _actionError = _errorMessage(error));
    } finally {
      if (mounted) setState(() => _pendingAction = null);
    }
  }

  _PanelSnapshot _snapshotData() {
    final source = widget.snapshot;
    if (source != null) {
      return _PanelSnapshot(
        projects: _listFromSnapshot(source, 'projects'),
        agents: _listFromSnapshot(source, 'agents'),
        assignments: _listFromSnapshot(source, 'assignments'),
      );
    }
    return _PanelSnapshot(
      projects: widget.projects,
      agents: widget.agents,
      assignments: widget.assignments,
    );
  }
}

class _PanelSnapshot {
  const _PanelSnapshot({
    required this.projects,
    required this.agents,
    required this.assignments,
  });

  final List<Map<String, dynamic>> projects;
  final List<Map<String, dynamic>> agents;
  final List<Map<String, dynamic>> assignments;
}

class _ProjectSummary extends StatelessWidget {
  const _ProjectSummary({required this.project});

  final Map<String, dynamic> project;

  @override
  Widget build(BuildContext context) {
    final projectId = _stringValue(project, ['id', 'projectId', 'project_id']);
    return Container(
      padding: const EdgeInsets.all(12),
      decoration: BoxDecoration(
        color: Theme.of(context).colorScheme.surfaceContainerHighest,
        borderRadius: BorderRadius.circular(10),
      ),
      child: Wrap(
        spacing: 8,
        runSpacing: 6,
        children: [
          Chip(
            avatar: const Icon(Icons.folder_outlined, size: 17),
            label: Text(_displayName(project, fallback: '项目')),
          ),
          if (projectId != null) Chip(label: Text('项目 ID：$projectId')),
        ],
      ),
    );
  }
}

class _WorkspaceAccessSelector extends StatelessWidget {
  const _WorkspaceAccessSelector({
    super.key,
    required this.value,
    required this.enabled,
    required this.onChanged,
  });

  final String value;
  final bool enabled;
  final ValueChanged<String> onChanged;

  @override
  Widget build(BuildContext context) {
    final values = <String>{'none', 'read_only', 'workspace_write', value};
    return InputDecorator(
      decoration: const InputDecoration(
        labelText: '工作区权限',
        isDense: true,
        border: OutlineInputBorder(),
        contentPadding: EdgeInsets.symmetric(horizontal: 10, vertical: 2),
      ),
      child: DropdownButtonHideUnderline(
        child: DropdownButton<String>(
          key: ValueKey('workspace-access-dropdown-$value'),
          value: value,
          isDense: true,
          onChanged: enabled ? (next) => onChanged(next!) : null,
          items: values
              .map(
                (access) => DropdownMenuItem<String>(
                  value: access,
                  child: Text(_workspaceAccessLabel(access)),
                ),
              )
              .toList(growable: false),
        ),
      ),
    );
  }
}

String _workspaceAccessLabel(String value) => switch (value) {
  'none' => '无',
  'read_only' => '只读',
  'workspace_write' => '工作区写入',
  _ => value,
};

class _StatusCard extends StatelessWidget {
  const _StatusCard({
    super.key,
    required this.icon,
    required this.title,
    required this.message,
    this.color,
  });

  final IconData icon;
  final String title;
  final String message;
  final Color? color;

  @override
  Widget build(BuildContext context) {
    final foreground = color ?? Theme.of(context).colorScheme.onSurfaceVariant;
    return Container(
      padding: const EdgeInsets.all(14),
      decoration: BoxDecoration(
        color: Theme.of(context).colorScheme.surfaceContainerHighest,
        borderRadius: BorderRadius.circular(10),
      ),
      child: Row(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Icon(icon, color: foreground),
          const SizedBox(width: 10),
          Expanded(
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text(
                  title,
                  style: Theme.of(context).textTheme.titleSmall?.copyWith(
                    color: foreground,
                    fontWeight: FontWeight.w700,
                  ),
                ),
                const SizedBox(height: 4),
                Text(message),
              ],
            ),
          ),
        ],
      ),
    );
  }
}

class _DisabledNotice extends StatelessWidget {
  const _DisabledNotice({this.reason});

  final String? reason;

  @override
  Widget build(BuildContext context) {
    return Row(
      children: [
        const Icon(Icons.lock_outline, size: 18),
        const SizedBox(width: 8),
        Expanded(child: Text(reason == null ? '不可用' : '不可用：$reason')),
      ],
    );
  }
}

List<Map<String, dynamic>> _listFromSnapshot(
  Map<String, dynamic> snapshot,
  String key,
) {
  final value = snapshot[key];
  if (value is! List) return const <Map<String, dynamic>>[];
  return value
      .whereType<Map>()
      .map((item) => Map<String, dynamic>.from(item))
      .toList(growable: false);
}

Map<String, dynamic>? _findById(
  Iterable<Map<String, dynamic>> values,
  String? id,
) {
  if (id == null || id.isEmpty) return null;
  for (final value in values) {
    if (_stringValue(value, [
          'id',
          'projectId',
          'agentId',
          'project_id',
          'agent_id',
        ]) ==
        id) {
      return value;
    }
  }
  return null;
}

String? _stringValue(Map<String, dynamic> value, List<String> keys) {
  for (final key in keys) {
    final candidate = value[key];
    if (candidate is String && candidate.isNotEmpty) return candidate;
  }
  return null;
}

bool? _boolValue(Map<String, dynamic> value, List<String> keys) {
  for (final key in keys) {
    final candidate = value[key];
    if (candidate is bool) return candidate;
  }
  return null;
}

String _displayName(Map<String, dynamic> value, {required String fallback}) {
  return _stringValue(value, ['name', 'displayName', 'display_name']) ??
      fallback;
}

String _errorMessage(Object error) {
  final message = error.toString();
  return message.startsWith('Bad state: ')
      ? message.substring('Bad state: '.length)
      : message;
}
