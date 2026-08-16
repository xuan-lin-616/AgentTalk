import 'package:flutter/material.dart';

class WorkflowCreateDialog extends StatefulWidget {
  const WorkflowCreateDialog({
    super.key,
    required this.initialAgentId,
    required this.onSubmit,
  });

  final String? initialAgentId;
  final Future<void> Function(
    String name,
    String kind,
    String agentId,
    String promptSupplement,
  )
  onSubmit;

  @override
  State<WorkflowCreateDialog> createState() => _WorkflowCreateDialogState();
}

class _WorkflowCreateDialogState extends State<WorkflowCreateDialog> {
  final TextEditingController _nameController = TextEditingController();
  final TextEditingController _promptController = TextEditingController();
  String _kind = 'sequential';
  String? _agentId;
  bool _saving = false;
  String? _error;

  @override
  void initState() {
    super.initState();
    _agentId = widget.initialAgentId;
  }

  @override
  void dispose() {
    _nameController.dispose();
    _promptController.dispose();
    super.dispose();
  }

  Future<void> _submit() async {
    final name = _nameController.text.trim();
    final agentId = _agentId;
    if (_saving) return;
    if (name.isEmpty || agentId == null || agentId.isEmpty) {
      ScaffoldMessenger.of(
        context,
      ).showSnackBar(const SnackBar(content: Text('请输入工作流名称并选择智能体')));
      return;
    }
    setState(() {
      _saving = true;
      _error = null;
    });
    try {
      await widget.onSubmit(
        name,
        _kind,
        agentId,
        _promptController.text.trim(),
      );
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
      title: const Text('创建接力工作流'),
      content: SizedBox(
        width: 440,
        child: Column(
          mainAxisSize: MainAxisSize.min,
          children: [
            TextField(
              controller: _nameController,
              enabled: !_saving,
              decoration: const InputDecoration(labelText: '工作流名称'),
            ),
            DropdownButtonFormField<String>(
              initialValue: _kind,
              decoration: const InputDecoration(labelText: '工作流类型'),
              items: const [
                DropdownMenuItem(value: 'sequential', child: Text('顺序执行')),
                DropdownMenuItem(value: 'parallel', child: Text('并行执行')),
                DropdownMenuItem(value: 'reviewer', child: Text('审阅流程')),
              ],
              onChanged: _saving
                  ? null
                  : (value) => setState(() => _kind = value ?? _kind),
            ),
            TextField(
              controller: _promptController,
              enabled: !_saving,
              decoration: const InputDecoration(labelText: '步骤提示补充（可选）'),
            ),
            if (_agentId != null)
              InputDecorator(
                decoration: const InputDecoration(labelText: '项目智能体'),
                child: Align(
                  alignment: Alignment.centerLeft,
                  child: Text(_agentId!),
                ),
              )
            else
              const Align(
                alignment: Alignment.centerLeft,
                child: Padding(
                  padding: EdgeInsets.only(top: 12),
                  child: Text('当前项目没有可用智能体'),
                ),
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
                  width: 16,
                  height: 16,
                  child: CircularProgressIndicator(strokeWidth: 2),
                )
              : const Text('创建'),
        ),
      ],
    );
  }
}
