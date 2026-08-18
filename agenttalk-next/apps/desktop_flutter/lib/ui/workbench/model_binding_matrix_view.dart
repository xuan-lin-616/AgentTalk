import 'package:flutter/material.dart';

import '../../ipc/core_ipc_client.dart';
import '../theme/studio_colors.dart';

/// Read-only model binding matrix.
///
/// The shell injects the current project roster; this view then queries
/// `identity_model_options.list` for the base-agent and project-agent scopes.
/// Editing is intentionally not implemented yet: v1 mutations
/// (`identity_model_option.upsert/default`) exist in the client, but a safe
/// matrix editor is deferred. // TODO(接真实X): add explicit upsert/default UI.
class ModelBindingMatrixView extends StatefulWidget {
  const ModelBindingMatrixView({
    super.key,
    required this.client,
    required this.sessionId,
    required this.projectId,
    required this.agents,
  });

  final CoreIpcClient client;
  final String sessionId;
  final String? projectId;
  final List<Map<String, dynamic>> agents;

  @override
  State<ModelBindingMatrixView> createState() => _ModelBindingMatrixViewState();
}

class _ModelBindingMatrixViewState extends State<ModelBindingMatrixView> {
  List<IdentityModelOptionMetadata> _options = const [];
  bool _loading = true;
  String? _error;

  @override
  void initState() {
    super.initState();
    _load();
  }

  @override
  void didUpdateWidget(covariant ModelBindingMatrixView oldWidget) {
    super.didUpdateWidget(oldWidget);
    if (oldWidget.projectId != widget.projectId ||
        oldWidget.sessionId != widget.sessionId ||
        oldWidget.agents.length != widget.agents.length) {
      _load();
    }
  }

  Future<void> _load() async {
    final projectId = widget.projectId;
    if (projectId == null || projectId.isEmpty || widget.agents.isEmpty) {
      setState(() {
        _loading = false;
        _options = const [];
      });
      return;
    }
    setState(() {
      _loading = true;
      _error = null;
    });
    final options = <IdentityModelOptionMetadata>[];
    try {
      for (final agent in widget.agents) {
        final agentId = agent['id']?.toString();
        if (agentId == null || agentId.isEmpty) continue;
        for (final target in [
          IdentityModelTarget(identityScope: 'base_agent', agentId: agentId),
          IdentityModelTarget(
            identityScope: 'project_agent',
            agentId: agentId,
            projectId: projectId,
          ),
        ]) {
          try {
            final scoped = await widget.client.queryIdentityModelOptions(
              sessionId: widget.sessionId,
              target: target,
            );
            options.addAll(scoped);
          } on Object {
            // A single scope can be empty while the other is populated.
          }
        }
      }
      if (!mounted) return;
      setState(() {
        _options = options;
        _loading = false;
      });
    } on Object catch (error) {
      if (!mounted) return;
      setState(() {
        _loading = false;
        _error = '$error';
      });
    }
  }

  @override
  Widget build(BuildContext context) {
    return Container(
      color: StudioColors.bgRoot,
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          const _MatrixHeader(),
          const Divider(height: 1, color: StudioColors.borderSubtle),
          Expanded(child: _buildBody()),
        ],
      ),
    );
  }

  Widget _buildBody() {
    if (_loading) {
      return const Center(child: CircularProgressIndicator(strokeWidth: 2));
    }
    if (_error != null) {
      return _MatrixEmpty(
        icon: Icons.error_outline,
        title: '模型矩阵读取失败',
        subtitle: _error!,
      );
    }
    if (_options.isEmpty) {
      return _MatrixEmpty(
        icon: Icons.table_chart_outlined,
        title: '暂无模型绑定',
        subtitle: widget.projectId == null || widget.projectId!.isEmpty
            ? '请先选择项目'
            : 'Core 没有返回当前项目智能体的 identity_model_options。',
      );
    }
    final byAgent = <String, List<IdentityModelOptionMetadata>>{};
    for (final option in _options) {
      byAgent.putIfAbsent(option.target.agentId, () => []).add(option);
    }
    return ListView(
      padding: const EdgeInsets.all(12),
      children: [
        for (final entry in byAgent.entries) ...[
          Text(
            entry.key,
            style: const TextStyle(
              color: StudioColors.textPrimary,
              fontSize: 12,
              fontWeight: FontWeight.w700,
            ),
          ),
          const SizedBox(height: 6),
          for (final option in entry.value)
            Card(
              margin: const EdgeInsets.only(bottom: 6),
              color: StudioColors.bgCard,
              shape: RoundedRectangleBorder(
                borderRadius: BorderRadius.circular(8),
                side: const BorderSide(color: StudioColors.borderSubtle),
              ),
              child: ListTile(
                dense: true,
                leading: Icon(
                  option.isDefault
                      ? Icons.star_outlined
                      : Icons.tune_outlined,
                  size: 18,
                  color: option.isDefault
                      ? StudioColors.warning
                      : StudioColors.textTertiary,
                ),
                title: Text(
                  '${option.displayName} (${option.modelId})',
                  style: const TextStyle(
                    color: StudioColors.textPrimary,
                    fontSize: 12,
                  ),
                ),
                subtitle: Text(
                  '${option.target.identityScope} · ${option.connectorId} · ${option.availability}'
                  '${option.contextWindow == null ? '' : ' · ${option.contextWindow} ctx'}',
                  style: const TextStyle(
                    color: StudioColors.textTertiary,
                    fontSize: 10,
                  ),
                ),
              ),
            ),
          const SizedBox(height: 8),
        ],
      ],
    );
  }
}

class _MatrixHeader extends StatelessWidget {
  const _MatrixHeader();

  @override
  Widget build(BuildContext context) {
    return const Padding(
      padding: EdgeInsets.symmetric(horizontal: 16, vertical: 10),
      child: Row(
        children: [
          Icon(Icons.table_chart_outlined, size: 18, color: StudioColors.primaryHover),
          SizedBox(width: 8),
          Text(
            '模型绑定矩阵',
            style: TextStyle(
              color: StudioColors.textPrimary,
              fontSize: 14,
              fontWeight: FontWeight.w600,
            ),
          ),
          Spacer(),
          Text(
            '只读 · 编辑 TODO(接真实X)',
            style: TextStyle(color: StudioColors.textTertiary, fontSize: 10),
          ),
        ],
      ),
    );
  }
}

class _MatrixEmpty extends StatelessWidget {
  const _MatrixEmpty({
    required this.icon,
    required this.title,
    required this.subtitle,
  });

  final IconData icon;
  final String title;
  final String subtitle;

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
        ],
      ),
    );
  }
}
