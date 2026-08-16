import 'package:flutter/material.dart';

import '../gen/l10n.dart';
import '../ipc/protocol_v1.dart';

typedef IdentityModelDefaultRequest =
    Future<void> Function({
      required String identityScope,
      required String agentId,
      String? projectId,
      String? conversationId,
      required String connectorId,
      required String modelId,
    });

/// A read-only view of Core health and projection metadata.
///
/// The panel intentionally accepts the same snapshot shape used by
/// `WorkspaceShell`, so it can be embedded in a dialog, sheet, or side panel
/// without owning IPC or projection state.
class DiagnosticsMetadataPanel extends StatelessWidget {
  const DiagnosticsMetadataPanel({
    super.key,
    required this.snapshot,
    this.health,
    this.runtimeModels = const <String, dynamic>{},
    this.projectionStatus = '核心投影未配置',
    this.diagnosticDetails,
    this.onRetryStartup,
    this.onSetModelDefault,
  });

  final Map<String, dynamic> snapshot;
  final RuntimeHealth? health;
  final Map<String, dynamic> runtimeModels;
  final String projectionStatus;
  final String? diagnosticDetails;
  final VoidCallback? onRetryStartup;
  final IdentityModelDefaultRequest? onSetModelDefault;

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);
    final l10n = AppLocalizations.of(context)!;
    final runtime = _valueOrFallback(health?.safeDetails['runtime'], 'Core');
    final status = _displayStatus(health?.status ?? projectionStatus);
    final metrics = <_MetadataMetric>[
      const _MetadataMetric('工作流', 'workflows', Icons.account_tree_outlined),
      const _MetadataMetric(
        '模型候选',
        'modelCandidates',
        Icons.view_list_outlined,
      ),
      const _MetadataMetric(
        '身份模型列表',
        'identityModelOptions',
        Icons.tune_outlined,
      ),
      const _MetadataMetric(
        '模型快照',
        'modelSnapshots',
        Icons.auto_awesome_motion_outlined,
      ),
      const _MetadataMetric(
        '选择快照',
        'modelSelectionSnapshots',
        Icons.lock_clock_outlined,
      ),
      const _MetadataMetric('摘要', 'summaries', Icons.notes_outlined),
      const _MetadataMetric('记忆', 'memories', Icons.memory_outlined),
      const _MetadataMetric(
        '检索来源',
        'retrievalSources',
        Icons.manage_search_outlined,
      ),
      const _MetadataMetric(
        'Context 清单',
        'contextManifests',
        Icons.inventory_2_outlined,
      ),
    ];

    return Card(
      clipBehavior: Clip.antiAlias,
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Row(
              children: [
                Icon(
                  Icons.monitor_heart_outlined,
                  color: theme.colorScheme.primary,
                ),
                const SizedBox(width: 10),
                Expanded(
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      Text(
                        l10n.advancedDiagnosticsTitle,
                        style: theme.textTheme.titleMedium?.copyWith(
                          fontWeight: FontWeight.w700,
                        ),
                      ),
                      Text(
                        l10n.advancedDiagnosticsSubtitle,
                        style: theme.textTheme.bodySmall,
                      ),
                    ],
                  ),
                ),
              ],
            ),
            const SizedBox(height: 12),
            ListTile(
              contentPadding: EdgeInsets.zero,
              leading: const Icon(Icons.storage_outlined),
              title: Text(runtime),
              subtitle: Text(status),
            ),
            if (diagnosticDetails != null) ...[
              const Divider(),
              const ListTile(
                contentPadding: EdgeInsets.zero,
                leading: Icon(Icons.bug_report_outlined),
                title: Text('技术诊断详情'),
              ),
              SelectableText(diagnosticDetails!),
            ],
            if (onRetryStartup != null) ...[
              const SizedBox(height: 8),
              Align(
                alignment: Alignment.centerRight,
                child: OutlinedButton.icon(
                  onPressed: onRetryStartup,
                  icon: const Icon(Icons.refresh),
                  label: Text(l10n.retryStartup),
                ),
              ),
            ],
            const Divider(),
            ...metrics.map(
              (metric) => ListTile(
                dense: true,
                contentPadding: EdgeInsets.zero,
                leading: Icon(metric.icon, size: 20),
                title: Text(metric.label),
                trailing: Text(
                  '${_list(snapshot, metric.snapshotKey).length}',
                  style: theme.textTheme.titleSmall?.copyWith(
                    fontWeight: FontWeight.w700,
                  ),
                ),
              ),
            ),
            const Divider(),
            Builder(
              builder: (context) {
                final manifests = _list(snapshot, 'contextManifests');
                if (manifests.isEmpty) return const SizedBox.shrink();
                return Column(
                  crossAxisAlignment: CrossAxisAlignment.stretch,
                  children: manifests.map((manifest) {
                    int totalTokens = 0;
                    int itemsWithHash = 0;
                    int totalDecisions = 0;
                    final items = manifest['items'] is List
                        ? manifest['items'] as List
                        : [];
                    for (final item in items) {
                      if (item is Map) {
                        if (item['tokenCount'] is int) {
                          totalTokens += item['tokenCount'] as int;
                        }
                        if (item['hash'] != null &&
                            item['hash'].toString().isNotEmpty) {
                          itemsWithHash++;
                        }
                        if (item['decisions'] is List) {
                          totalDecisions += (item['decisions'] as List).length;
                        }
                      }
                    }
                    if (manifest['totalTokens'] is int) {
                      totalTokens = manifest['totalTokens'] as int;
                    }
                    if (manifest['totalDecisions'] is int) {
                      totalDecisions = manifest['totalDecisions'] as int;
                    }
                    final runId = manifest['id']?.toString() ?? 'unknown_run';
                    return ListTile(
                      dense: true,
                      contentPadding: EdgeInsets.zero,
                      leading: const Icon(Icons.analytics_outlined, size: 20),
                      title: Text('Context V2 统计 ($runId)'),
                      subtitle: Text(
                        'Tokens: $totalTokens · Hashes: $itemsWithHash · Decisions: $totalDecisions',
                      ),
                    );
                  }).toList(),
                );
              },
            ),
            const SizedBox(height: 8),
            ConnectorModelStatusPanel(
              snapshot: snapshot,
              health: health,
              runtimeModels: runtimeModels,
            ),
            IdentityModelOptionsEditor(
              snapshot: snapshot,
              onSetDefault: onSetModelDefault,
            ),
          ],
        ),
      ),
    );
  }
}

class _MetadataMetric {
  const _MetadataMetric(this.label, this.snapshotKey, this.icon);

  final String label;
  final String snapshotKey;
  final IconData icon;
}

String _valueOrFallback(Object? value, String fallback) {
  final text = value?.toString();
  return text == null || text.isEmpty ? fallback : text;
}

String _displayStatus(String value) => switch (value) {
  'Core projection ready' => '已连接',
  'Core projection unavailable' => '不可用',
  'Core projection reconnected' => '已重新连接',
  _ => value,
};

List<Map<String, dynamic>> _list(Map<String, dynamic> snapshot, String key) {
  final value = snapshot[key];
  if (value is! List) return const <Map<String, dynamic>>[];
  return value.whereType<Map<String, dynamic>>().toList(growable: false);
}

/// Read-only connector/model status from Core-owned health and projection data.
///
/// Flutter does not infer capabilities or choose a model here. It only renders
/// the catalog and health facts already resolved by Core.
class ConnectorModelStatusPanel extends StatelessWidget {
  const ConnectorModelStatusPanel({
    super.key,
    required this.snapshot,
    this.health,
    this.runtimeModels = const <String, dynamic>{},
  });

  final Map<String, dynamic> snapshot;
  final RuntimeHealth? health;
  final Map<String, dynamic> runtimeModels;

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);
    final candidates = _list(snapshot, 'modelCandidates');
    final identityOptions = _list(snapshot, 'identityModelOptions');
    final selectionSnapshots = _list(snapshot, 'modelSelectionSnapshots');
    final runtimeCatalog = _list(runtimeModels, 'modelMetadata');
    final connectors = _healthList(health?.safeDetails['connectors']);
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        const Divider(),
        Text(
          'Connector 与模型',
          style: theme.textTheme.titleSmall?.copyWith(
            fontWeight: FontWeight.w700,
          ),
        ),
        const SizedBox(height: 6),
        if (connectors.isEmpty)
          const ListTile(
            dense: true,
            contentPadding: EdgeInsets.zero,
            leading: Icon(Icons.link_off_outlined, size: 20),
            title: Text('当前没有 Connector health 明细'),
            subtitle: Text('Core 尚未提供可显示的健康状态'),
          )
        else
          ...connectors.map(
            (connector) => ListTile(
              dense: true,
              contentPadding: EdgeInsets.zero,
              leading: Icon(
                connector['ok'] == true
                    ? Icons.check_circle_outline
                    : Icons.error_outline,
                color: connector['ok'] == true
                    ? theme.colorScheme.primary
                    : theme.colorScheme.error,
                size: 20,
              ),
              title: Text(_text(connector['name'], 'Connector')),
              subtitle: Text(
                _connectorStatusLabel(_text(connector['status'], '未知')),
              ),
            ),
          ),
        ...runtimeCatalog.map(
          (model) => ListTile(
            dense: true,
            contentPadding: EdgeInsets.zero,
            leading: const Icon(Icons.cloud_done_outlined, size: 20),
            title: Text(_text(model['modelId'], '未命名模型')),
            subtitle: Text(
              '${_text(runtimeModels['runtimeId'], 'runtime')} · '
              '${_availabilityText(model['availability'])}',
            ),
          ),
        ),
        if (candidates.isEmpty)
          const ListTile(
            dense: true,
            contentPadding: EdgeInsets.zero,
            leading: Icon(Icons.model_training_outlined, size: 20),
            title: Text('暂无模型候选'),
            subtitle: Text('模型目录由 Core/Runtime 提供'),
          )
        else
          ...candidates.map(
            (candidate) => ListTile(
              dense: true,
              contentPadding: EdgeInsets.zero,
              leading: Icon(
                candidate['available'] == true
                    ? Icons.check_circle_outline
                    : Icons.pause_circle_outline,
                size: 20,
              ),
              title: Text(_text(candidate['modelId'], '未命名模型')),
              subtitle: Text(
                '${_text(candidate['connectorId'], 'unknown connector')} · '
                '${candidate['available'] == true ? '可用' : '不可用'}',
              ),
            ),
          ),
        if (identityOptions.isNotEmpty) ...[
          const SizedBox(height: 8),
          Text(
            '身份模型候选列表',
            style: theme.textTheme.labelLarge?.copyWith(
              fontWeight: FontWeight.w700,
            ),
          ),
          ...identityOptions.map(
            (option) => ListTile(
              dense: true,
              contentPadding: EdgeInsets.zero,
              leading: Icon(
                option['isDefault'] == true
                    ? Icons.star_outline
                    : Icons.radio_button_unchecked,
                size: 20,
              ),
              title: Text(_text(option['modelId'], '未命名模型')),
              subtitle: Text(
                '${_text(option['scope'], '未知范围')} · '
                '${_text(option['connectorId'], '未知连接器')} · '
                '${_availabilityText(option['availability'])}',
              ),
            ),
          ),
        ],
        if (selectionSnapshots.isNotEmpty) ...[
          const SizedBox(height: 8),
          Text(
            '最近模型选择快照',
            style: theme.textTheme.labelLarge?.copyWith(
              fontWeight: FontWeight.w700,
            ),
          ),
          ...selectionSnapshots.reversed
              .take(5)
              .map(
                (snapshot) => ListTile(
                  dense: true,
                  contentPadding: EdgeInsets.zero,
                  leading: const Icon(Icons.lock_outline, size: 20),
                  title: Text(_text(snapshot['effectiveModelId'], '未解析模型')),
                  subtitle: Text(
                    'v${_text(snapshot['version'], '?')} · '
                    '${_text(snapshot['selectionSource'], '未知来源')}',
                  ),
                ),
              ),
        ],
      ],
    );
  }
}

/// A Core-backed, projection-only model list editor. The widget never picks a
/// model itself: it presents the persisted identity list and forwards the
/// user's explicit default change to the host callback.
class IdentityModelOptionsEditor extends StatefulWidget {
  const IdentityModelOptionsEditor({
    super.key,
    required this.snapshot,
    this.onSetDefault,
  });

  final Map<String, dynamic> snapshot;
  final IdentityModelDefaultRequest? onSetDefault;

  @override
  State<IdentityModelOptionsEditor> createState() =>
      _IdentityModelOptionsEditorState();
}

class _IdentityModelOptionsEditorState
    extends State<IdentityModelOptionsEditor> {
  final Map<String, String> _confirmedDefaults = <String, String>{};
  String? _pendingGroup;
  String? _error;

  Future<void> _setDefault({
    required String groupKey,
    required Map<String, dynamic> first,
    required String modelId,
  }) async {
    final callback = widget.onSetDefault;
    if (callback == null || _pendingGroup != null) return;
    setState(() {
      _pendingGroup = groupKey;
      _error = null;
    });
    try {
      await callback(
        identityScope: _text(first['scope'], 'base_agent'),
        agentId: _text(first['agentId'], ''),
        projectId: first['projectId'] as String?,
        conversationId: first['conversationId'] as String?,
        connectorId: _text(first['connectorId'], ''),
        modelId: modelId,
      );
      if (mounted) {
        setState(() => _confirmedDefaults[groupKey] = modelId);
      }
    } on Object catch (error) {
      if (mounted) setState(() => _error = error.toString());
    } finally {
      if (mounted) setState(() => _pendingGroup = null);
    }
  }

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);
    final options = _list(widget.snapshot, 'identityModelOptions');
    if (options.isEmpty) return const SizedBox.shrink();
    final groups = <String, List<Map<String, dynamic>>>{};
    for (final option in options) {
      final key = [
        option['scope'],
        option['agentId'],
        option['projectId'],
        option['conversationId'],
        option['connectorId'],
      ].join('|');
      (groups[key] ??= <Map<String, dynamic>>[]).add(option);
    }
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        const Divider(),
        Text(
          '模型列表编辑',
          style: theme.textTheme.titleSmall?.copyWith(
            fontWeight: FontWeight.w700,
          ),
        ),
        const SizedBox(height: 6),
        if (_error != null) ...[
          Text(
            _error!,
            style: theme.textTheme.bodySmall?.copyWith(
              color: theme.colorScheme.error,
            ),
          ),
          const SizedBox(height: 6),
        ],
        ...groups.values.map((group) {
          final first = group.first;
          final groupKey = [
            first['scope'],
            first['agentId'],
            first['projectId'],
            first['conversationId'],
            first['connectorId'],
          ].join('|');
          final defaults = group
              .where((option) => option['isDefault'] == true)
              .toList(growable: false);
          final selected =
              _confirmedDefaults[groupKey] ??
              (defaults.isNotEmpty
                  ? defaults.first['modelId']?.toString()
                  : null);
          final ids = group
              .map((option) => option['modelId']?.toString())
              .whereType<String>()
              .where((id) => id.isNotEmpty)
              .toSet()
              .toList(growable: false);
          return ListTile(
            dense: true,
            contentPadding: EdgeInsets.zero,
            leading: const Icon(Icons.tune_outlined, size: 20),
            title: Text(
              '${_text(first['scope'], 'unknown scope')} · '
              '${_text(first['connectorId'], 'unknown connector')}',
            ),
            subtitle: Text(
              '${_text(first['agentId'], 'unknown agent')} · '
              '${_text(first['projectId'] ?? first['conversationId'], 'base')}',
            ),
            trailing: DropdownButton<String>(
              value: ids.contains(selected) ? selected : null,
              hint: const Text('选择默认'),
              onChanged: widget.onSetDefault == null || _pendingGroup != null
                  ? null
                  : (modelId) {
                      if (modelId == null) return;
                      _setDefault(
                        groupKey: groupKey,
                        first: first,
                        modelId: modelId,
                      );
                    },
              items: ids
                  .map(
                    (id) =>
                        DropdownMenuItem<String>(value: id, child: Text(id)),
                  )
                  .toList(growable: false),
            ),
          );
        }),
      ],
    );
  }
}

List<Map<String, dynamic>> _healthList(Object? value) {
  if (value is! List) return const <Map<String, dynamic>>[];
  return value.whereType<Map<String, dynamic>>().toList(growable: false);
}

String _text(Object? value, String fallback) {
  final text = value?.toString();
  return text == null || text.isEmpty ? fallback : text;
}

String _availabilityText(Object? value) {
  switch (value?.toString()) {
    case 'available':
      return '可用';
    case 'unavailable':
      return '不可用';
    case 'unconfigured':
      return '需要配置';
    case 'authentication_required':
      return '需要认证';
    case null:
    case '':
      return '未知';
    default:
      return value.toString();
  }
}

String _connectorStatusLabel(String value) => switch (value) {
  'ready' => '就绪',
  'unavailable' => '不可用',
  'degraded' => '降级',
  _ => value,
};
