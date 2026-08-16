import 'package:flutter/material.dart';

typedef ConversationAgentSetRequest =
    Future<void> Function({
      required String conversationId,
      required String agentId,
      required bool enabled,
    });

typedef ConversationAgentRemoveRequest =
    Future<void> Function({
      required String conversationId,
      required String agentId,
    });

class ConversationAgentAssignmentPanel extends StatefulWidget {
  const ConversationAgentAssignmentPanel({
    super.key,
    required this.snapshot,
    required this.conversationId,
    this.onSet,
    this.onRemove,
    this.loading = false,
    this.error,
  });

  final Map<String, dynamic> snapshot;
  final String? conversationId;
  final ConversationAgentSetRequest? onSet;
  final ConversationAgentRemoveRequest? onRemove;
  final bool loading;
  final String? error;

  @override
  State<ConversationAgentAssignmentPanel> createState() =>
      _ConversationAgentAssignmentPanelState();
}

class _ConversationAgentAssignmentPanelState
    extends State<ConversationAgentAssignmentPanel> {
  String? _pending;
  String? _actionError;

  bool get _disabled => widget.loading || _pending != null;

  @override
  Widget build(BuildContext context) {
    final conversations = _list(widget.snapshot, 'conversations');
    final projects = _list(widget.snapshot, 'projects');
    final agents = _list(widget.snapshot, 'agents');
    final projectAssignments = _list(widget.snapshot, 'assignments');
    final conversationAgents = _list(widget.snapshot, 'conversationAgents');
    final conversation = _findById(conversations, widget.conversationId);
    final projectId = conversation?['projectId']?.toString();
    final allowedAgentIds = projectAssignments
        .where(
          (item) =>
              item['projectId']?.toString() == projectId &&
              item['enabled'] == true,
        )
        .map((item) => item['agentId']?.toString())
        .whereType<String>()
        .toSet();
    final rows = conversationAgents
        .where(
          (item) =>
              item['conversationId']?.toString() == widget.conversationId &&
              allowedAgentIds.contains(item['agentId']?.toString()),
        )
        .toList(growable: false);
    final assignedIds = rows
        .map((item) => item['agentId']?.toString())
        .whereType<String>()
        .toSet();
    final available = agents
        .where((agent) {
          final id = agent['id']?.toString();
          return id != null &&
              allowedAgentIds.contains(id) &&
              !assignedIds.contains(id);
        })
        .toList(growable: false);

    return Semantics(
      container: true,
      label: '会话智能体分配面板',
      child: Card(
        child: Padding(
          padding: const EdgeInsets.all(16),
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.stretch,
            children: [
              Row(
                children: [
                  const Icon(Icons.chat_bubble_outline),
                  const SizedBox(width: 8),
                  Expanded(
                    child: Text(
                      '会话智能体分配',
                      style: Theme.of(context).textTheme.titleMedium?.copyWith(
                        fontWeight: FontWeight.w700,
                      ),
                    ),
                  ),
                  PopupMenuButton<String>(
                    key: const ValueKey('conversation-agent-assignment-add'),
                    enabled:
                        !_disabled &&
                        widget.onSet != null &&
                        available.isNotEmpty,
                    onSelected: (agentId) => _set(agentId, true),
                    itemBuilder: (context) => available
                        .map(
                          (agent) => PopupMenuItem<String>(
                            value: agent['id']?.toString(),
                            child: Text(agent['name']?.toString() ?? '智能体'),
                          ),
                        )
                        .toList(growable: false),
                    child: const Icon(Icons.person_add_alt_1_outlined),
                  ),
                ],
              ),
              if (widget.loading) const LinearProgressIndicator(),
              if (widget.error != null) _status('错误状态', widget.error!),
              if (widget.conversationId == null || conversation == null)
                _status('没有选中的会话', '选择一个现有会话后管理列表。')
              else if (rows.isEmpty)
                _status('继承项目列表', '当前会话没有收缩分配。')
              else
                ...rows.map((row) => _row(context, row, agents)),
              if (available.isEmpty && conversation != null)
                Text(
                  '当前项目列表中没有可收缩的智能体；面板不会扩展列表。',
                  style: Theme.of(context).textTheme.bodySmall,
                ),
              if (_actionError != null) _status('请求失败', _actionError!),
              if (projects.isEmpty && conversation != null)
                Text('项目投影不可用', style: Theme.of(context).textTheme.bodySmall),
            ],
          ),
        ),
      ),
    );
  }

  Widget _row(
    BuildContext context,
    Map<String, dynamic> row,
    List<Map<String, dynamic>> agents,
  ) {
    final agentId = row['agentId']?.toString() ?? 'unknown-agent';
    final agent = _findById(agents, agentId);
    final name = agent?['name']?.toString() ?? agentId;
    final enabled = row['enabled'] == true;
    final pending = _pending == agentId;
    return ListTile(
      key: ValueKey('conversation-agent-assignment-$agentId'),
      contentPadding: EdgeInsets.zero,
      leading: const Icon(Icons.smart_toy_outlined),
      title: Text(name),
      subtitle: Text('启用：${enabled ? '是' : '否'} · $agentId'),
      trailing: pending
          ? const SizedBox(
              width: 18,
              height: 18,
              child: CircularProgressIndicator(strokeWidth: 2),
            )
          : Row(
              mainAxisSize: MainAxisSize.min,
              children: [
                Switch(
                  key: ValueKey('conversation-enabled-$agentId'),
                  value: enabled,
                  onChanged: _disabled || widget.onSet == null
                      ? null
                      : (value) => _set(agentId, value),
                ),
                IconButton(
                  key: ValueKey('remove-conversation-agent-$agentId'),
                  tooltip: '移除会话智能体',
                  onPressed: _disabled || widget.onRemove == null
                      ? null
                      : () => _remove(agentId),
                  icon: const Icon(Icons.remove_circle_outline),
                ),
              ],
            ),
    );
  }

  Future<void> _set(String agentId, bool enabled) async {
    final callback = widget.onSet;
    final conversationId = widget.conversationId;
    if (callback == null || conversationId == null || _disabled) return;
    setState(() {
      _pending = agentId;
      _actionError = null;
    });
    try {
      await callback(
        conversationId: conversationId,
        agentId: agentId,
        enabled: enabled,
      );
    } catch (error) {
      if (mounted) setState(() => _actionError = _error(error));
    } finally {
      if (mounted) setState(() => _pending = null);
    }
  }

  Future<void> _remove(String agentId) async {
    final callback = widget.onRemove;
    final conversationId = widget.conversationId;
    if (callback == null || conversationId == null || _disabled) return;
    setState(() {
      _pending = agentId;
      _actionError = null;
    });
    try {
      await callback(conversationId: conversationId, agentId: agentId);
    } catch (error) {
      if (mounted) setState(() => _actionError = _error(error));
    } finally {
      if (mounted) setState(() => _pending = null);
    }
  }

  Widget _status(String title, String message) => Padding(
    padding: const EdgeInsets.only(top: 12),
    child: Text('$title：$message'),
  );
}

List<Map<String, dynamic>> _list(Map<String, dynamic> snapshot, String key) {
  final value = snapshot[key];
  if (value is! List) return const <Map<String, dynamic>>[];
  return value
      .whereType<Map>()
      .map(Map<String, dynamic>.from)
      .toList(growable: false);
}

Map<String, dynamic>? _findById(List<Map<String, dynamic>> values, String? id) {
  if (id == null) return null;
  for (final value in values) {
    if (value['id']?.toString() == id) return value;
  }
  return null;
}

String _error(Object error) {
  final value = error.toString();
  return value.startsWith('Bad state: ') ? value.substring(11) : value;
}
