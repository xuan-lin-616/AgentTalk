import 'package:flutter/material.dart';

import '../ipc/retrieval_preview.dart';

enum RetrievalPreviewResultStatus { idle, loading, empty, error, success }

typedef RetrievalPreviewQuery =
    Future<RetrievalPreviewResult> Function(RetrievalPreviewRequest request);

/// Read-only retrieval result surface.
///
/// The query is sent only after an explicit user submit. The widget keeps the
/// query in a transient controller and renders Core-provided snippets only;
/// it does not persist prompts or source bodies.
class RetrievalPreviewDialog extends StatefulWidget {
  const RetrievalPreviewDialog({
    super.key,
    required this.preview,
    required this.project,
    required this.conversation,
    required this.agent,
    required this.scope,
    this.sourceTypes = const <String>['memory', 'retrieval', 'document'],
    this.limit = 8,
    this.initialQuery = '',
    this.mode = 'exact',
  });

  final RetrievalPreviewQuery preview;
  final String? project;
  final String? conversation;
  final String? agent;
  final String scope;
  final List<String> sourceTypes;
  final int limit;
  final String initialQuery;
  final String mode;

  @override
  State<RetrievalPreviewDialog> createState() => _RetrievalPreviewDialogState();
}

class _RetrievalPreviewDialogState extends State<RetrievalPreviewDialog> {
  late final TextEditingController _queryController = TextEditingController(
    text: widget.initialQuery,
  );
  RetrievalPreviewResultStatus _status = RetrievalPreviewResultStatus.idle;
  RetrievalPreviewResult? _result;
  String? _error;

  @override
  void dispose() {
    _queryController.dispose();
    super.dispose();
  }

  Future<void> _submit() async {
    final query = _queryController.text.trim();
    final scopeId = widget.scope == 'conversation'
        ? widget.conversation
        : widget.project;
    if (query.isEmpty) {
      setState(() {
        _status = RetrievalPreviewResultStatus.error;
        _error = '请输入检索问题或关键词';
      });
      return;
    }
    if (widget.scope != 'project' && widget.scope != 'conversation') {
      setState(() {
        _status = RetrievalPreviewResultStatus.error;
        _error = '检索范围无效，已禁止全局搜索';
      });
      return;
    }
    if (scopeId == null || scopeId.trim().isEmpty) {
      setState(() {
        _status = RetrievalPreviewResultStatus.error;
        _error = '缺少显式检索范围，已禁止全局搜索';
      });
      return;
    }
    setState(() {
      _status = RetrievalPreviewResultStatus.loading;
      _result = null;
      _error = null;
    });
    try {
      final result = await widget.preview(
        RetrievalPreviewRequest(
          project: widget.project,
          conversation: widget.conversation,
          agent: widget.agent,
          query: query,
          scope: widget.scope,
          sourceTypes: widget.sourceTypes,
          limit: widget.limit,
          mode: widget.mode,
        ),
      );
      if (!mounted) return;
      setState(() {
        _result = result;
        _status = result.hits.isEmpty
            ? RetrievalPreviewResultStatus.empty
            : RetrievalPreviewResultStatus.success;
      });
    } on Object catch (error) {
      if (!mounted) return;
      setState(() {
        _status = RetrievalPreviewResultStatus.error;
        _error = _displayError(error);
      });
    }
  }

  @override
  Widget build(BuildContext context) {
    return AlertDialog(
      title: Row(
        children: [
          const Icon(Icons.manage_search_outlined),
          const SizedBox(width: 8),
          const Expanded(child: Text('检索预览')),
          Chip(
            key: const Key('retrieval-preview-read-only'),
            label: const Text('只读'),
            visualDensity: VisualDensity.compact,
          ),
        ],
      ),
      content: SizedBox(
        width: 620,
        child: ConstrainedBox(
          constraints: const BoxConstraints(maxHeight: 560),
          child: SingleChildScrollView(
            key: const Key('retrieval-preview-scroll'),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.stretch,
              children: [
                _ScopeSummary(
                  project: widget.project,
                  conversation: widget.conversation,
                  agent: widget.agent,
                  scope: widget.scope,
                  sourceTypes: widget.sourceTypes,
                  limit: widget.limit,
                ),
                const SizedBox(height: 14),
                TextField(
                  key: const Key('retrieval-preview-query'),
                  controller: _queryController,
                  enabled: _status != RetrievalPreviewResultStatus.loading,
                  maxLines: 3,
                  textInputAction: TextInputAction.search,
                  onSubmitted: (_) => _submit(),
                  decoration: const InputDecoration(
                    labelText: '检索问题',
                    hintText: '输入本次检索问题或关键词',
                    border: OutlineInputBorder(),
                  ),
                ),
                const SizedBox(height: 12),
                _buildResult(context),
              ],
            ),
          ),
        ),
      ),
      actions: [
        TextButton(
          onPressed: _status == RetrievalPreviewResultStatus.loading
              ? null
              : () => Navigator.of(context).pop(),
          child: const Text('关闭'),
        ),
        FilledButton.icon(
          key: const Key('retrieval-preview-submit'),
          onPressed: _status == RetrievalPreviewResultStatus.loading
              ? null
              : _submit,
          icon: _status == RetrievalPreviewResultStatus.loading
              ? const SizedBox(
                  width: 16,
                  height: 16,
                  child: CircularProgressIndicator(strokeWidth: 2),
                )
              : const Icon(Icons.search),
          label: const Text('检索'),
        ),
      ],
    );
  }

  Widget _buildResult(BuildContext context) {
    switch (_status) {
      case RetrievalPreviewResultStatus.idle:
        return const _PreviewHint(
          key: Key('retrieval-preview-idle'),
          message: '检索只会在当前显式范围内执行。输入问题后点击“检索”。',
        );
      case RetrievalPreviewResultStatus.loading:
        return Semantics(
          liveRegion: true,
          label: '检索预览加载中',
          child: Column(
            key: Key('retrieval-preview-loading'),
            crossAxisAlignment: CrossAxisAlignment.stretch,
            children: [
              LinearProgressIndicator(),
              SizedBox(height: 8),
              Text('正在检索当前范围…'),
            ],
          ),
        );
      case RetrievalPreviewResultStatus.error:
        return _PreviewStatus(
          key: const Key('retrieval-preview-error'),
          title: '检索失败',
          message: _error ?? '检索预览暂不可用',
          icon: Icons.error_outline,
          isError: true,
        );
      case RetrievalPreviewResultStatus.empty:
        return Column(
          key: const Key('retrieval-preview-empty'),
          crossAxisAlignment: CrossAxisAlignment.stretch,
          children: [
            if (_result != null) _ResultMetadata(result: _result!),
            const SizedBox(height: 10),
            const _PreviewStatus(
              title: '暂无命中',
              message: '当前范围内没有可展示的结果；权限不会扩展到范围之外。',
              icon: Icons.search_off_outlined,
            ),
          ],
        );
      case RetrievalPreviewResultStatus.success:
        final result = _result!;
        return Column(
          key: const Key('retrieval-preview-success'),
          crossAxisAlignment: CrossAxisAlignment.stretch,
          children: [
            _ResultMetadata(result: result),
            const SizedBox(height: 10),
            ...result.hits.asMap().entries.map(
              (entry) => Padding(
                padding: const EdgeInsets.only(bottom: 10),
                child: _HitCard(index: entry.key, hit: entry.value),
              ),
            ),
          ],
        );
    }
  }
}

class _ScopeSummary extends StatelessWidget {
  const _ScopeSummary({
    required this.project,
    required this.conversation,
    required this.agent,
    required this.scope,
    required this.sourceTypes,
    required this.limit,
  });

  final String? project;
  final String? conversation;
  final String? agent;
  final String scope;
  final List<String> sourceTypes;
  final int limit;

  @override
  Widget build(BuildContext context) {
    final scopeId = scope == 'conversation' ? conversation : project;
    return Container(
      key: const Key('retrieval-preview-scope'),
      padding: const EdgeInsets.all(12),
      decoration: BoxDecoration(
        color: Theme.of(context).colorScheme.surfaceContainerHighest,
        borderRadius: BorderRadius.circular(10),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.stretch,
        children: [
          Text(
            '显式检索范围',
            style: Theme.of(
              context,
            ).textTheme.titleSmall?.copyWith(fontWeight: FontWeight.w700),
          ),
          const SizedBox(height: 5),
          Text('范围：${_scopeLabel(scope)} · ID：${scopeId ?? '未选择'}'),
          Text('项目：${project ?? '无'} · 会话：${conversation ?? '无'}'),
          Text(
            '智能体：${agent ?? '无'} · 来源类型：${sourceTypes.map(_sourceTypeLabel).join('、')} · 数量上限：$limit',
          ),
          const SizedBox(height: 5),
          Text(
            '权限：仅展示 Core 允许或受限返回的命中，不绕过权限。',
            style: Theme.of(context).textTheme.bodySmall,
          ),
        ],
      ),
    );
  }
}

class _ResultMetadata extends StatelessWidget {
  const _ResultMetadata({required this.result});

  final RetrievalPreviewResult result;

  @override
  Widget build(BuildContext context) {
    final capabilities = result.capabilities.entries
        .map((entry) => '${entry.key}: ${entry.value}')
        .join(' · ');
    return Container(
      key: const Key('retrieval-preview-metadata'),
      padding: const EdgeInsets.all(10),
      decoration: BoxDecoration(
        border: Border.all(color: Theme.of(context).colorScheme.outlineVariant),
        borderRadius: BorderRadius.circular(10),
      ),
      child: Text(
        '检索版本：${result.retrievalVersion}\n'
        '查询哈希：${result.queryHash}\n'
        '能力：${capabilities.isEmpty ? '无' : capabilities}\n'
        '权限：每条命中单独显示判定结果',
        key: const Key('retrieval-preview-metadata-text'),
      ),
    );
  }
}

class _HitCard extends StatelessWidget {
  const _HitCard({required this.index, required this.hit});

  final int index;
  final RetrievalPreviewHit hit;

  @override
  Widget build(BuildContext context) {
    return Card(
      key: ValueKey('retrieval-preview-hit-${hit.hitId}'),
      margin: EdgeInsets.zero,
      child: Padding(
        padding: const EdgeInsets.all(12),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.stretch,
          children: [
            Row(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Expanded(
                  child: Text(
                    '${index + 1}. ${hit.sourceType} · ${hit.sourceObjectId}',
                    maxLines: 2,
                    overflow: TextOverflow.ellipsis,
                    style: Theme.of(context).textTheme.titleSmall?.copyWith(
                      fontWeight: FontWeight.w700,
                    ),
                  ),
                ),
                const SizedBox(width: 8),
                Chip(
                  key: ValueKey('retrieval-preview-permission-${hit.hitId}'),
                  label: Text(_permissionLabel(hit.permissionDecision)),
                  visualDensity: VisualDensity.compact,
                ),
              ],
            ),
            const SizedBox(height: 6),
            Text(
              hit.boundedSnippet(),
              key: ValueKey('retrieval-preview-snippet-${hit.hitId}'),
              maxLines: 4,
              overflow: TextOverflow.ellipsis,
            ),
            const SizedBox(height: 6),
            Text(
              '命中原因：${hit.matchReason} · 分数：${hit.score} · 估算 token：${hit.estimatedTokens}',
              style: Theme.of(context).textTheme.bodySmall,
            ),
          ],
        ),
      ),
    );
  }
}

class _PreviewHint extends StatelessWidget {
  const _PreviewHint({super.key, required this.message});

  final String message;

  @override
  Widget build(BuildContext context) => Container(
    padding: const EdgeInsets.all(12),
    decoration: BoxDecoration(
      color: Theme.of(context).colorScheme.surfaceContainerHighest,
      borderRadius: BorderRadius.circular(10),
    ),
    child: Text(message),
  );
}

class _PreviewStatus extends StatelessWidget {
  const _PreviewStatus({
    super.key,
    required this.title,
    required this.message,
    required this.icon,
    this.isError = false,
  });

  final String title;
  final String message;
  final IconData icon;
  final bool isError;

  @override
  Widget build(BuildContext context) {
    final color = isError
        ? Theme.of(context).colorScheme.error
        : Theme.of(context).colorScheme.onSurfaceVariant;
    return Container(
      padding: const EdgeInsets.all(12),
      decoration: BoxDecoration(
        color: color.withValues(alpha: .1),
        borderRadius: BorderRadius.circular(10),
        border: Border.all(color: color.withValues(alpha: .4)),
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
                  style: TextStyle(color: color, fontWeight: FontWeight.w700),
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

String _scopeLabel(String value) => switch (value) {
  'project' => '项目',
  'conversation' => '会话',
  _ => value,
};

String _sourceTypeLabel(String value) => switch (value) {
  'memory' => '记忆',
  'retrieval' => '检索来源',
  'document' => '文档',
  _ => value,
};

String _permissionLabel(String value) => switch (value) {
  'allowed' => '允许',
  'restricted' => '受限',
  'denied' => '拒绝',
  _ => value,
};

String _displayError(Object error) {
  final text = error.toString();
  return text.startsWith('CoreIpcException: ')
      ? text.substring('CoreIpcException: '.length)
      : text;
}
