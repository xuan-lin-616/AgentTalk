import 'package:flutter/material.dart';

class RetrievalSourceWriteDialog extends StatefulWidget {
  const RetrievalSourceWriteDialog({
    super.key,
    required this.initialScopeId,
    required this.onSubmit,
  });

  final String initialScopeId;
  final Future<void> Function(
    String scopeId,
    String citation,
    String sha256,
    int tokenCount,
  )
  onSubmit;

  @override
  State<RetrievalSourceWriteDialog> createState() =>
      _RetrievalSourceWriteDialogState();
}

class _RetrievalSourceWriteDialogState
    extends State<RetrievalSourceWriteDialog> {
  late final TextEditingController _scopeController;
  final TextEditingController _citationController = TextEditingController();
  final TextEditingController _hashController = TextEditingController();
  final TextEditingController _tokenCountController = TextEditingController(
    text: '0',
  );
  bool _saving = false;
  String? _error;

  @override
  void initState() {
    super.initState();
    _scopeController = TextEditingController(text: widget.initialScopeId);
  }

  @override
  void dispose() {
    _scopeController.dispose();
    _citationController.dispose();
    _hashController.dispose();
    _tokenCountController.dispose();
    super.dispose();
  }

  Future<void> _submit() async {
    if (_saving) return;
    final scopeId = _scopeController.text.trim();
    final citation = _citationController.text.trim();
    final sha256 = _hashController.text.trim().toLowerCase();
    final tokenCount = int.tryParse(_tokenCountController.text.trim());
    if (scopeId.isEmpty ||
        citation.isEmpty ||
        !RegExp(r'^[0-9a-f]{64}$').hasMatch(sha256) ||
        tokenCount == null ||
        tokenCount < 0) {
      setState(() => _error = '请输入范围、引用、64 位十六进制 SHA-256 与 token 数量');
      return;
    }
    setState(() {
      _saving = true;
      _error = null;
    });
    try {
      await widget.onSubmit(scopeId, citation, sha256, tokenCount);
      if (mounted) Navigator.of(context).pop();
    } on Object catch (error) {
      if (!mounted) return;
      setState(() => _error = error.toString());
    } finally {
      if (mounted) setState(() => _saving = false);
    }
  }

  @override
  Widget build(BuildContext context) {
    return AlertDialog(
      title: const Text('保存检索来源'),
      content: SizedBox(
        width: 480,
        child: Column(
          mainAxisSize: MainAxisSize.min,
          children: [
            TextField(
              controller: _scopeController,
              enabled: !_saving,
              decoration: const InputDecoration(labelText: '范围 ID'),
            ),
            TextField(
              controller: _citationController,
              enabled: !_saving,
              decoration: const InputDecoration(labelText: '引用'),
            ),
            TextField(
              controller: _hashController,
              enabled: !_saving,
              decoration: const InputDecoration(labelText: '内容 SHA-256'),
            ),
            TextField(
              controller: _tokenCountController,
              enabled: !_saving,
              keyboardType: TextInputType.number,
              decoration: const InputDecoration(labelText: 'Token 数量'),
            ),
            if (_error != null) ...[
              const SizedBox(height: 10),
              Align(
                alignment: Alignment.centerLeft,
                child: Text(
                  _error!,
                  style: TextStyle(color: Theme.of(context).colorScheme.error),
                ),
              ),
            ],
          ],
        ),
      ),
      actions: [
        TextButton(
          onPressed: _saving ? null : () => Navigator.of(context).pop(),
          child: const Text('取消'),
        ),
        FilledButton(
          onPressed: _saving ? null : _submit,
          child: _saving
              ? const SizedBox(
                  width: 18,
                  height: 18,
                  child: CircularProgressIndicator(strokeWidth: 2),
                )
              : const Text('保存'),
        ),
      ],
    );
  }
}
