import 'package:flutter/material.dart';

/// A read-only inspector for the context-related parts of a Core snapshot.
///
/// The widget owns no projection state and does not perform IPC. It accepts a
/// snapshot so it can be placed in a Drawer, dialog, or side panel by the
/// host. Unknown or malformed collection values are treated as empty.
class ContextInspectorDrawer extends StatelessWidget {
  const ContextInspectorDrawer({
    super.key,
    required this.snapshot,
    this.selectedSourceId,
    this.loading = false,
    this.error,
    this.onSelectRetrievalSources,
    this.onFeedbackRetrievalSource,
    this.onGenerateSummary,
    this.projectId,
    this.conversationId,
    this.agentId,
    this.onPreviewRetrieval,
  });

  final Map<String, dynamic> snapshot;
  final String? selectedSourceId;
  final bool loading;
  final String? error;
  final Future<void> Function(List<Map<String, dynamic>> sources)?
  onSelectRetrievalSources;
  final Future<void> Function(String sourceId)? onFeedbackRetrievalSource;
  final Future<void> Function()? onGenerateSummary;
  final String? projectId;
  final String? conversationId;
  final String? agentId;
  final Future<void> Function()? onPreviewRetrieval;

  @override
  Widget build(BuildContext context) {
    final sections = <_InspectorSectionData>[
      _InspectorSectionData(
        keyName: 'contextManifests',
        title: '上下文清单',
        emptyMessage: '暂无上下文清单',
        icon: Icons.inventory_2_outlined,
        entries: _snapshotCollection(snapshot, 'contextManifests'),
      ),
      _InspectorSectionData(
        keyName: 'memories',
        title: '记忆',
        emptyMessage: '暂无记忆',
        icon: Icons.memory_outlined,
        entries: _snapshotCollection(snapshot, 'memories'),
      ),
      _InspectorSectionData(
        keyName: 'retrievalSources',
        title: '检索来源',
        emptyMessage: '暂无检索来源',
        icon: Icons.manage_search_outlined,
        entries: _snapshotCollection(snapshot, 'retrievalSources'),
        supportsSelection: true,
      ),
      _InspectorSectionData(
        keyName: 'summaries',
        title: '摘要',
        emptyMessage: '暂无摘要',
        icon: Icons.notes_outlined,
        entries: _snapshotCollection(snapshot, 'summaries'),
      ),
      _InspectorSectionData(
        keyName: 'artifacts',
        title: '制品',
        emptyMessage: '暂无制品',
        icon: Icons.attach_file_outlined,
        entries: _snapshotCollection(snapshot, 'artifacts'),
      ),
      _InspectorSectionData(
        keyName: 'attachments',
        title: '附件',
        emptyMessage: '暂无附件',
        icon: Icons.link_outlined,
        entries: _snapshotCollection(snapshot, 'attachments'),
      ),
    ];
    final horizontalPadding = MediaQuery.sizeOf(context).width < 360
        ? 12.0
        : 16.0;

    return Semantics(
      container: true,
      explicitChildNodes: true,
      label: '上下文检查器抽屉',
      child: Card(
        clipBehavior: Clip.antiAlias,
        child: SingleChildScrollView(
          key: const ValueKey('context-inspector-scroll'),
          padding: EdgeInsets.all(horizontalPadding),
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.stretch,
            children: [
              _buildHeader(context),
              if (loading) ...[
                const SizedBox(height: 12),
                Semantics(
                  liveRegion: true,
                  label: '上下文检查器加载中',
                  child: const Column(
                    crossAxisAlignment: CrossAxisAlignment.stretch,
                    children: [
                      LinearProgressIndicator(
                        key: ValueKey('context-inspector-loading'),
                      ),
                      SizedBox(height: 8),
                      Text('正在加载上下文快照'),
                    ],
                  ),
                ),
              ],
              if (error != null) ...[
                const SizedBox(height: 12),
                _StatusBanner(
                  key: const ValueKey('context-inspector-error'),
                  title: '错误状态',
                  message: error!.isEmpty ? '上下文快照不可用' : error!,
                  icon: Icons.error_outline,
                ),
              ],
              if (onPreviewRetrieval != null) ...[
                const SizedBox(height: 12),
                _RetrievalPreviewEntry(
                  projectId: projectId,
                  conversationId: conversationId,
                  agentId: agentId,
                  onPreviewRetrieval: onPreviewRetrieval!,
                ),
              ],
              const SizedBox(height: 16),
              ...sections.map(
                (section) => Padding(
                  key: ValueKey('context-inspector-section-${section.keyName}'),
                  padding: const EdgeInsets.only(bottom: 16),
                  child: _InspectorSection(
                    section: section,
                    selectedSourceId: selectedSourceId,
                    onSelectRetrievalSources:
                        section.keyName == 'retrievalSources'
                        ? onSelectRetrievalSources
                        : null,
                    onFeedbackRetrievalSource:
                        section.keyName == 'retrievalSources'
                        ? onFeedbackRetrievalSource
                        : null,
                    onGenerateSummary: section.keyName == 'summaries'
                        ? onGenerateSummary
                        : null,
                  ),
                ),
              ),
            ],
          ),
        ),
      ),
    );
  }

  Widget _buildHeader(BuildContext context) {
    final theme = Theme.of(context);
    return Semantics(
      container: true,
      explicitChildNodes: true,
      label: '上下文检查器标题，只读',
      child: Row(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Icon(Icons.account_tree_outlined, color: theme.colorScheme.primary),
          const SizedBox(width: 10),
          Expanded(
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text(
                  '上下文检查器',
                  style: theme.textTheme.titleMedium?.copyWith(
                    fontWeight: FontWeight.w700,
                  ),
                ),
                const SizedBox(height: 4),
                Text('只读查看上下文快照与来源明细', style: theme.textTheme.bodySmall),
              ],
            ),
          ),
          const SizedBox(width: 8),
          const Chip(label: Text('只读')),
        ],
      ),
    );
  }
}

class _RetrievalPreviewEntry extends StatelessWidget {
  const _RetrievalPreviewEntry({
    required this.projectId,
    required this.conversationId,
    required this.agentId,
    required this.onPreviewRetrieval,
  });

  final String? projectId;
  final String? conversationId;
  final String? agentId;
  final Future<void> Function() onPreviewRetrieval;

  @override
  Widget build(BuildContext context) {
    final scope = conversationId?.isNotEmpty == true
        ? '会话 · $conversationId'
        : projectId?.isNotEmpty == true
        ? '项目 · $projectId'
        : '未选择（禁止全局检索）';
    return Card(
      key: const ValueKey('context-inspector-retrieval-preview'),
      child: Padding(
        padding: const EdgeInsets.all(12),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.stretch,
          children: [
            Row(
              children: [
                Icon(
                  Icons.manage_search_outlined,
                  color: Theme.of(context).colorScheme.primary,
                ),
                const SizedBox(width: 8),
                const Expanded(
                  child: Text(
                    '检索预览',
                    style: TextStyle(fontWeight: FontWeight.w700),
                  ),
                ),
                FilledButton.tonalIcon(
                  key: const ValueKey(
                    'context-inspector-retrieval-preview-button',
                  ),
                  onPressed:
                      projectId?.isNotEmpty == true ||
                          conversationId?.isNotEmpty == true
                      ? onPreviewRetrieval
                      : null,
                  icon: const Icon(Icons.search, size: 17),
                  label: const Text('检索'),
                ),
              ],
            ),
            const SizedBox(height: 6),
            Text('当前 scope：$scope'),
            Text('智能体：${agentId?.isNotEmpty == true ? agentId : '无'}'),
            const SizedBox(height: 4),
            Text(
              '只读预览；查询需由用户提交，权限由 Core 返回并逐条展示。',
              style: Theme.of(context).textTheme.bodySmall,
            ),
          ],
        ),
      ),
    );
  }
}

class _InspectorSectionData {
  const _InspectorSectionData({
    required this.keyName,
    required this.title,
    required this.emptyMessage,
    required this.icon,
    required this.entries,
    this.supportsSelection = false,
  });

  final String keyName;
  final String title;
  final String emptyMessage;
  final IconData icon;
  final List<Map<String, dynamic>> entries;
  final bool supportsSelection;
}

class _InspectorSection extends StatelessWidget {
  const _InspectorSection({
    required this.section,
    required this.selectedSourceId,
    this.onSelectRetrievalSources,
    this.onFeedbackRetrievalSource,
    this.onGenerateSummary,
  });

  final _InspectorSectionData section;
  final String? selectedSourceId;
  final Future<void> Function(List<Map<String, dynamic>> sources)?
  onSelectRetrievalSources;
  final Future<void> Function(String sourceId)? onFeedbackRetrievalSource;
  final Future<void> Function()? onGenerateSummary;

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);
    final countLabel = '${section.title}：${section.entries.length}项';
    return Semantics(
      container: true,
      explicitChildNodes: true,
      label: countLabel,
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.stretch,
        children: [
          Row(
            children: [
              Icon(section.icon, size: 20, color: theme.colorScheme.primary),
              const SizedBox(width: 8),
              Expanded(
                child: Text(
                  section.title,
                  style: theme.textTheme.titleSmall?.copyWith(
                    fontWeight: FontWeight.w700,
                  ),
                ),
              ),
              if (onSelectRetrievalSources != null)
                TextButton.icon(
                  key: const ValueKey('context-inspector-select-retrieval'),
                  onPressed: section.entries.isEmpty
                      ? null
                      : () => onSelectRetrievalSources!(section.entries),
                  icon: const Icon(Icons.checklist_outlined, size: 16),
                  label: const Text('选择'),
                ),
              if (onGenerateSummary != null)
                TextButton.icon(
                  key: const ValueKey('context-inspector-generate-summary'),
                  onPressed: onGenerateSummary,
                  icon: const Icon(Icons.auto_awesome_outlined, size: 16),
                  label: const Text('生成'),
                ),
              Semantics(
                label: countLabel,
                child: Chip(
                  key: ValueKey('context-inspector-count-${section.keyName}'),
                  label: Text('${section.entries.length}'),
                  visualDensity: VisualDensity.compact,
                ),
              ),
            ],
          ),
          const SizedBox(height: 8),
          if (section.entries.isEmpty)
            _EmptyState(
              key: ValueKey('context-inspector-empty-${section.keyName}'),
              icon: section.icon,
              message: section.emptyMessage,
            )
          else
            ...section.entries.asMap().entries.map(
              (entry) => Padding(
                padding: const EdgeInsets.only(bottom: 8),
                child: _DetailTile(
                  key: ValueKey(
                    'context-inspector-entry-${section.keyName}-${entry.key}',
                  ),
                  sectionTitle: section.title,
                  item: entry.value,
                  selected:
                      section.supportsSelection &&
                      _matchesSelectedSource(entry.value, selectedSourceId),
                  onFeedback:
                      onFeedbackRetrievalSource != null &&
                          entry.value['id'] is String
                      ? () => onFeedbackRetrievalSource!(
                          entry.value['id'] as String,
                        )
                      : null,
                ),
              ),
            ),
        ],
      ),
    );
  }
}

class _DetailTile extends StatelessWidget {
  const _DetailTile({
    super.key,
    required this.sectionTitle,
    required this.item,
    required this.selected,
    this.onFeedback,
  });

  final String sectionTitle;
  final Map<String, dynamic> item;
  final bool selected;
  final VoidCallback? onFeedback;

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);
    final title = _itemTitle(item);
    final subtitle = _itemSubtitle(item, title);
    final preview = _itemPreview(item, sectionTitle);
    final semanticsLabel = selected
        ? '$sectionTitle 条目 $title，已选来源'
        : '$sectionTitle 条目 $title';
    final borderColor = selected
        ? theme.colorScheme.primary
        : theme.colorScheme.outlineVariant;

    return Semantics(
      container: true,
      explicitChildNodes: true,
      label: semanticsLabel,
      child: Container(
        decoration: BoxDecoration(
          color: selected
              ? theme.colorScheme.primaryContainer.withValues(alpha: .55)
              : theme.colorScheme.surfaceContainerHighest.withValues(
                  alpha: .45,
                ),
          border: Border.all(color: borderColor, width: selected ? 1.5 : 1),
          borderRadius: BorderRadius.circular(10),
        ),
        padding: const EdgeInsets.fromLTRB(12, 10, 12, 10),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.stretch,
          children: [
            Row(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Icon(
                  selected
                      ? Icons.radio_button_checked
                      : Icons.article_outlined,
                  size: 18,
                  color: selected
                      ? theme.colorScheme.primary
                      : theme.colorScheme.onSurfaceVariant,
                ),
                const SizedBox(width: 8),
                Expanded(
                  child: Text(
                    title,
                    maxLines: 2,
                    overflow: TextOverflow.ellipsis,
                    style: theme.textTheme.bodyMedium?.copyWith(
                      fontWeight: FontWeight.w600,
                    ),
                  ),
                ),
                if (selected) ...[
                  const SizedBox(width: 8),
                  const Chip(
                    key: ValueKey('context-inspector-selected-source'),
                    label: Text('已选来源'),
                    visualDensity: VisualDensity.compact,
                  ),
                ],
                if (onFeedback != null) ...[
                  const SizedBox(width: 4),
                  IconButton(
                    key: const ValueKey('context-inspector-feedback-source'),
                    tooltip: '检索反馈',
                    onPressed: onFeedback,
                    icon: const Icon(Icons.rate_review_outlined, size: 18),
                    visualDensity: VisualDensity.compact,
                  ),
                ],
              ],
            ),
            if (subtitle != null) ...[
              const SizedBox(height: 4),
              Text(subtitle, style: theme.textTheme.bodySmall),
            ],
            if (preview != null) ...[
              const SizedBox(height: 6),
              Text(preview, style: theme.textTheme.bodySmall),
            ],
          ],
        ),
      ),
    );
  }
}

class _EmptyState extends StatelessWidget {
  const _EmptyState({super.key, required this.icon, required this.message});

  final IconData icon;
  final String message;

  @override
  Widget build(BuildContext context) {
    final color = Theme.of(context).colorScheme.onSurfaceVariant;
    return Semantics(
      container: true,
      explicitChildNodes: true,
      label: '空状态：$message',
      child: Container(
        padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 11),
        decoration: BoxDecoration(
          color: Theme.of(context).colorScheme.surfaceContainerHighest,
          borderRadius: BorderRadius.circular(10),
        ),
        child: Row(
          children: [
            Icon(icon, size: 18, color: color),
            const SizedBox(width: 8),
            Expanded(child: Text(message)),
          ],
        ),
      ),
    );
  }
}

class _StatusBanner extends StatelessWidget {
  const _StatusBanner({
    super.key,
    required this.title,
    required this.message,
    required this.icon,
  });

  final String title;
  final String message;
  final IconData icon;

  @override
  Widget build(BuildContext context) {
    final color = Theme.of(context).colorScheme.error;
    return Semantics(
      liveRegion: true,
      explicitChildNodes: true,
      label: '$title: $message',
      child: Container(
        padding: const EdgeInsets.all(12),
        decoration: BoxDecoration(
          color: color.withValues(alpha: .12),
          borderRadius: BorderRadius.circular(10),
          border: Border.all(color: color.withValues(alpha: .45)),
        ),
        child: Row(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Icon(icon, color: color),
            const SizedBox(width: 10),
            Expanded(
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Text(
                    title,
                    style: Theme.of(context).textTheme.titleSmall?.copyWith(
                      color: color,
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
      ),
    );
  }
}

List<Map<String, dynamic>> _snapshotCollection(
  Map<String, dynamic> snapshot,
  String key,
) {
  final snakeKey = key.replaceAllMapped(
    RegExp(r'[A-Z]'),
    (match) => '_${match.group(0)!.toLowerCase()}',
  );
  final raw = snapshot.containsKey(key) ? snapshot[key] : snapshot[snakeKey];
  if (raw is! List) return const <Map<String, dynamic>>[];

  return raw
      .map((item) {
        if (item is Map) {
          final converted = <String, dynamic>{};
          item.forEach((key, value) {
            if (key is String) converted[key] = value;
          });
          return converted;
        }
        return <String, dynamic>{'value': item};
      })
      .toList(growable: false);
}

bool _matchesSelectedSource(
  Map<String, dynamic> item,
  String? selectedSourceId,
) {
  if (selectedSourceId == null || selectedSourceId.isEmpty) return false;
  return _firstString(item, const [
        'id',
        'sourceId',
        'source_id',
        'retrievalSourceId',
        'retrieval_source_id',
      ]) ==
      selectedSourceId;
}

String _itemTitle(Map<String, dynamic> item) {
  return _firstString(item, const [
        'fileName',
        'file_name',
        'name',
        'title',
        'label',
        'type',
        'attachmentId',
        'attachment_id',
        'id',
        'sourceId',
        'source_id',
      ]) ??
      '未命名条目';
}

String? _itemSubtitle(Map<String, dynamic> item, String title) {
  final value = _firstString(item, const [
    'id',
    'attachmentId',
    'attachment_id',
    'artifactId',
    'artifact_id',
    'messageId',
    'message_id',
    'sourceId',
    'source_id',
    'kind',
    'type',
    'status',
  ]);

  final parts = <String>[];
  if (value != null && value != title) {
    parts.add(value);
  }

  // Context V2 metadata
  if (item.containsKey('sealed') ||
      item.containsKey('hash') ||
      item.containsKey('tokenCount') ||
      item.containsKey('tokenBudget') ||
      item.containsKey('budget') ||
      item.containsKey('metadata') ||
      item.containsKey('decisions')) {
    if (item['sealed'] == true) parts.add('已封存');
    if (item['hash'] != null) {
      final hashStr = item['hash'].toString();
      if (hashStr.isNotEmpty) {
        parts.add(
          '哈希：${hashStr.length > 8 ? hashStr.substring(0, 8) : hashStr}',
        );
      }
    }
    if (item['metadata'] != null && item['metadata'] is Map) {
      final meta = item['metadata'] as Map;
      final allowedKeys = const [
        'tokens',
        'hashes',
        'action',
        'actions',
        'model',
        'modelSnapshots',
      ];
      final filteredMeta = meta.entries.where(
        (e) => allowedKeys.contains(e.key),
      );
      if (filteredMeta.isNotEmpty) {
        final metaStr = filteredMeta
            .map((e) => '${e.key}: ${e.value}')
            .join(', ');
        parts.add('元数据：{$metaStr}');
      }
    }
    if (item['tokenCount'] != null) {
      parts.add('${item['tokenCount']} 个 token');
    }
    if (item['tokenBudget'] != null) {
      parts.add('预算：${item['tokenBudget']} 个 token');
    } else if (item['budget'] != null) {
      parts.add('预算：${item['budget']}');
    }
    if (item['decisions'] != null && item['decisions'] is List) {
      final decisions = item['decisions'] as List;
      final count = decisions.length;
      final summary = decisions
          .map((d) {
            if (d is Map && d['action'] != null) return d['action'].toString();
            return d.toString();
          })
          .take(3)
          .join(', ');
      final suffix = count > 3 ? ', ...' : '';
      parts.add('$count 个决策（$summary$suffix）');
    }
  }

  if (parts.isEmpty) return null;
  return parts.join(' · ');
}

String? _itemPreview(Map<String, dynamic> item, String sectionTitle) {
  // Do not attempt to read or render 'content', 'text' or 'prompt' directly
  // to avoid rendering full bodies for Context V2 which can be huge or sensitive,
  // EXCEPT for Memory and Summary types where the content is meant to be displayed.
  final type = _firstString(item, const ['type', 'kind'])?.toLowerCase();
  final isMemoryOrSummary =
      type == 'memory' ||
      type == 'summary' ||
      sectionTitle == '记忆' ||
      sectionTitle == '摘要';

  final keys = isMemoryOrSummary
      ? const ['summary', 'description', 'preview', 'query', 'content', 'text']
      : const ['summary', 'description', 'preview', 'query'];

  final value = _firstString(item, keys);

  if (value != null && value.isNotEmpty) {
    return value.length > 180 ? '${value.substring(0, 177)}…' : value;
  }

  if (!isMemoryOrSummary &&
      (item.containsKey('content') ||
          item.containsKey('text') ||
          item.containsKey('prompt') ||
          item.containsKey('raw'))) {
    return '敏感内容已由界面隐藏';
  }

  return null;
}

String? _firstString(Map<String, dynamic> item, List<String> keys) {
  for (final key in keys) {
    final value = item[key];
    if (value == null) continue;
    final text = value.toString();
    if (text.isNotEmpty) return text;
  }
  return null;
}
