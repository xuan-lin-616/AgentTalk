import 'package:flutter/material.dart';

class RetrievalSelectionDialog extends StatefulWidget {
  const RetrievalSelectionDialog({
    super.key,
    required this.sources,
    required this.onSubmit,
  });

  final List<Map<String, dynamic>> sources;
  final Future<void> Function(List<Map<String, dynamic>> selected) onSubmit;

  @override
  State<RetrievalSelectionDialog> createState() =>
      _RetrievalSelectionDialogState();
}

class _RetrievalSelectionDialogState extends State<RetrievalSelectionDialog> {
  final Set<String> _selected = <String>{};
  bool _saving = false;
  String? _error;

  Future<void> _submit() async {
    if (_selected.isEmpty) {
      setState(() => _error = '至少选择一个检索来源');
      return;
    }
    setState(() {
      _saving = true;
      _error = null;
    });
    try {
      final selected = widget.sources
          .where((source) => _selected.contains(source['id']?.toString()))
          .toList(growable: false);
      await widget.onSubmit(selected);
      if (mounted) Navigator.of(context).pop();
    } on Object catch (error) {
      if (mounted) setState(() => _error = error.toString());
    } finally {
      if (mounted) setState(() => _saving = false);
    }
  }

  @override
  Widget build(BuildContext context) {
    return AlertDialog(
      title: const Text('选择检索来源'),
      content: SizedBox(
        width: 520,
        child: widget.sources.isEmpty
            ? const Text('当前范围没有可选择的来源')
            : SingleChildScrollView(
                child: Column(
                  mainAxisSize: MainAxisSize.min,
                  children: [
                    ...widget.sources.map((source) {
                      final id = source['id']?.toString() ?? '';
                      return CheckboxListTile(
                        key: ValueKey('retrieval-selection-$id'),
                        value: _selected.contains(id),
                        onChanged: _saving
                            ? null
                            : (value) => setState(() {
                                if (value == true) {
                                  _selected.add(id);
                                } else {
                                  _selected.remove(id);
                                }
                              }),
                        title: Text(source['citation']?.toString() ?? id),
                        subtitle: Text(id),
                        controlAffinity: ListTileControlAffinity.leading,
                      );
                    }),
                    if (_error != null)
                      Align(
                        alignment: Alignment.centerLeft,
                        child: Text(
                          _error!,
                          key: const Key('retrieval-selection-error'),
                          style: TextStyle(
                            color: Theme.of(context).colorScheme.error,
                          ),
                        ),
                      ),
                  ],
                ),
              ),
      ),
      actions: [
        TextButton(
          onPressed: _saving ? null : () => Navigator.of(context).pop(),
          child: const Text('取消'),
        ),
        FilledButton(
          key: const Key('retrieval-selection-submit'),
          onPressed: _saving ? null : _submit,
          child: _saving
              ? const SizedBox(
                  width: 18,
                  height: 18,
                  child: CircularProgressIndicator(strokeWidth: 2),
                )
              : const Text('保存选择'),
        ),
      ],
    );
  }
}

class RetrievalFeedbackDialog extends StatefulWidget {
  const RetrievalFeedbackDialog({
    super.key,
    required this.sourceId,
    required this.onSubmit,
  });

  final String sourceId;
  final Future<void> Function(String label, String reason) onSubmit;

  @override
  State<RetrievalFeedbackDialog> createState() =>
      _RetrievalFeedbackDialogState();
}

class _RetrievalFeedbackDialogState extends State<RetrievalFeedbackDialog> {
  String _label = 'helpful';
  String _reason = 'exact_match';
  bool _saving = false;
  String? _error;

  Future<void> _submit() async {
    setState(() {
      _saving = true;
      _error = null;
    });
    try {
      await widget.onSubmit(_label, _reason);
      if (mounted) Navigator.of(context).pop();
    } on Object catch (error) {
      if (mounted) setState(() => _error = error.toString());
    } finally {
      if (mounted) setState(() => _saving = false);
    }
  }

  @override
  Widget build(BuildContext context) {
    return AlertDialog(
      title: const Text('记录检索反馈'),
      content: SizedBox(
        width: 420,
        child: Column(
          mainAxisSize: MainAxisSize.min,
          children: [
            Align(
              alignment: Alignment.centerLeft,
              child: Text('来源：${widget.sourceId}'),
            ),
            DropdownButtonFormField<String>(
              key: const Key('retrieval-feedback-label'),
              initialValue: _label,
              decoration: const InputDecoration(labelText: '评价'),
              items: const [
                DropdownMenuItem(value: 'helpful', child: Text('有帮助')),
                DropdownMenuItem(value: 'not_helpful', child: Text('没有帮助')),
              ],
              onChanged: _saving
                  ? null
                  : (value) => setState(() => _label = value ?? _label),
            ),
            DropdownButtonFormField<String>(
              key: const Key('retrieval-feedback-reason'),
              initialValue: _reason,
              decoration: const InputDecoration(labelText: '原因'),
              items: const [
                DropdownMenuItem(value: 'exact_match', child: Text('精确命中')),
                DropdownMenuItem(value: 'irrelevant', child: Text('不相关')),
                DropdownMenuItem(value: 'stale_source', child: Text('来源过期')),
                DropdownMenuItem(value: 'wrong_scope', child: Text('范围错误')),
                DropdownMenuItem(value: 'duplicate', child: Text('重复来源')),
                DropdownMenuItem(value: 'permission', child: Text('权限问题')),
              ],
              onChanged: _saving
                  ? null
                  : (value) => setState(() => _reason = value ?? _reason),
            ),
            if (_error != null)
              Align(
                alignment: Alignment.centerLeft,
                child: Text(
                  _error!,
                  key: const Key('retrieval-feedback-error'),
                  style: TextStyle(color: Theme.of(context).colorScheme.error),
                ),
              ),
          ],
        ),
      ),
      actions: [
        TextButton(
          onPressed: _saving ? null : () => Navigator.of(context).pop(),
          child: const Text('取消'),
        ),
        FilledButton(
          key: const Key('retrieval-feedback-submit'),
          onPressed: _saving ? null : _submit,
          child: _saving
              ? const SizedBox(
                  width: 18,
                  height: 18,
                  child: CircularProgressIndicator(strokeWidth: 2),
                )
              : const Text('保存反馈'),
        ),
      ],
    );
  }
}
