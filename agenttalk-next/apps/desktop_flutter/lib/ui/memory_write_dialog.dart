import 'package:flutter/material.dart';

class MemoryWriteDialog extends StatefulWidget {
  const MemoryWriteDialog({
    super.key,
    required this.initialScopeId,
    required this.onSubmit,
  });

  final String initialScopeId;
  final Future<void> Function(
    String scopeId,
    String? agentId,
    String contentHash,
    bool confirmed,
  )
  onSubmit;

  @override
  State<MemoryWriteDialog> createState() => _MemoryWriteDialogState();
}

class _MemoryWriteDialogState extends State<MemoryWriteDialog> {
  late final TextEditingController _scopeController = TextEditingController(
    text: widget.initialScopeId,
  );
  final TextEditingController _agentController = TextEditingController();
  final TextEditingController _hashController = TextEditingController();
  bool _confirmed = false;
  bool _saving = false;

  @override
  void dispose() {
    _scopeController.dispose();
    _agentController.dispose();
    _hashController.dispose();
    super.dispose();
  }

  Future<void> _submit() async {
    final scopeId = _scopeController.text.trim();
    final agentId = _agentController.text.trim();
    final contentHash = _hashController.text.trim().toLowerCase();
    if (scopeId.isEmpty || !RegExp(r'^[0-9a-f]{64}$').hasMatch(contentHash)) {
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text('请输入 scope 与 64 位十六进制 content hash')),
      );
      return;
    }
    setState(() => _saving = true);
    try {
      await widget.onSubmit(
        scopeId,
        agentId.isEmpty ? null : agentId,
        contentHash,
        _confirmed,
      );
      if (mounted) Navigator.of(context).pop();
    } finally {
      if (mounted) setState(() => _saving = false);
    }
  }

  @override
  Widget build(BuildContext context) {
    return AlertDialog(
      title: const Text('保存 Memory'),
      content: SizedBox(
        width: 440,
        child: Column(
          mainAxisSize: MainAxisSize.min,
          children: [
            const Align(
              alignment: Alignment.centerLeft,
              child: Text('仅保存 Core Memory metadata 与 hash，不保存完整 Prompt。'),
            ),
            const SizedBox(height: 12),
            TextField(
              controller: _scopeController,
              decoration: const InputDecoration(
                labelText: 'Scope ID',
                hintText: 'project 或 conversation ID',
              ),
            ),
            TextField(
              controller: _agentController,
              decoration: const InputDecoration(labelText: '智能体 ID（可选）'),
            ),
            TextField(
              controller: _hashController,
              decoration: const InputDecoration(
                labelText: 'Content hash',
                hintText: '64 位 SHA-256 hex',
              ),
            ),
            CheckboxListTile(
              contentPadding: EdgeInsets.zero,
              value: _confirmed,
              onChanged: _saving
                  ? null
                  : (value) => setState(() => _confirmed = value ?? false),
              title: const Text('标记为已确认 Memory'),
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
          onPressed: _saving ? null : _submit,
          child: _saving
              ? const SizedBox(
                  width: 16,
                  height: 16,
                  child: CircularProgressIndicator(strokeWidth: 2),
                )
              : const Text('保存'),
        ),
      ],
    );
  }
}
