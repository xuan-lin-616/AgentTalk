import 'package:flutter/material.dart';

import '../platform/folder_picker.dart';

typedef ProjectionEntitySubmit =
    Future<void> Function(String name, String? rootPath);

class ProjectionEntityDialog extends StatefulWidget {
  const ProjectionEntityDialog({
    super.key,
    required this.title,
    required this.nameLabel,
    required this.onSubmit,
    this.rootPathLabel,
    this.initialName = '',
    this.initialRootPath = '',
    this.submitLabel = '创建',
    this.folderPickerClient,
  });

  final String title;
  final String nameLabel;
  final String? rootPathLabel;
  final String initialName;
  final String initialRootPath;
  final String submitLabel;
  final ProjectionEntitySubmit onSubmit;
  final FolderPickerClient? folderPickerClient;

  @override
  State<ProjectionEntityDialog> createState() => _ProjectionEntityDialogState();
}

class _ProjectionEntityDialogState extends State<ProjectionEntityDialog> {
  late final TextEditingController _name;
  late final TextEditingController _rootPath;
  bool _saving = false;
  bool _pickingFolder = false;
  String? _error;

  @override
  void initState() {
    super.initState();
    _name = TextEditingController(text: widget.initialName);
    _rootPath = TextEditingController(text: widget.initialRootPath);
  }

  Future<void> _pickFolder() async {
    if (_saving || _pickingFolder) return;
    setState(() {
      _pickingFolder = true;
      _error = null;
    });
    final picker = widget.folderPickerClient ?? createFolderPickerClient();
    final result = await picker.pickFolder();
    if (!mounted) return;
    setState(() => _pickingFolder = false);
    if (result.hasSelection) {
      _rootPath
        ..text = result.path!
        ..selection = TextSelection.collapsed(offset: result.path!.length);
      return;
    }
    if (result.status == FolderPickerStatus.cancelled) return;
    setState(() => _error = result.message ?? '文件夹选择失败；请手动输入根目录。');
  }

  @override
  void dispose() {
    _name.dispose();
    _rootPath.dispose();
    super.dispose();
  }

  Future<void> _submit() async {
    final name = _name.text.trim();
    if (name.isEmpty || _saving) {
      setState(() => _error = '名称不能为空');
      return;
    }
    setState(() {
      _saving = true;
      _error = null;
    });
    try {
      final rootPath = _rootPath.text.trim();
      await widget.onSubmit(name, rootPath.isEmpty ? null : rootPath);
      if (mounted) Navigator.of(context).pop();
    } on Object catch (error) {
      if (!mounted) return;
      setState(() {
        _saving = false;
        _error = error.toString();
      });
    }
  }

  @override
  Widget build(BuildContext context) {
    return AlertDialog(
      title: Text(widget.title),
      content: SizedBox(
        width: 440,
        child: Column(
          mainAxisSize: MainAxisSize.min,
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            TextField(
              controller: _name,
              autofocus: true,
              enabled: !_saving,
              textInputAction: widget.rootPathLabel == null
                  ? TextInputAction.done
                  : TextInputAction.next,
              onSubmitted: widget.rootPathLabel == null
                  ? (_) => _submit()
                  : null,
              decoration: InputDecoration(
                labelText: widget.nameLabel,
                border: const OutlineInputBorder(),
              ),
            ),
            if (widget.rootPathLabel != null) ...[
              const SizedBox(height: 12),
              TextField(
                controller: _rootPath,
                enabled: !_saving && !_pickingFolder,
                decoration: InputDecoration(
                  labelText: widget.rootPathLabel,
                  helperText: '可手动输入；留空表示稍后再授权 workspace',
                  border: const OutlineInputBorder(),
                ),
              ),
              const SizedBox(height: 8),
              Align(
                alignment: Alignment.centerRight,
                child: OutlinedButton.icon(
                  key: const Key('projection-entity-pick-folder'),
                  onPressed: _saving || _pickingFolder ? null : _pickFolder,
                  icon: _pickingFolder
                      ? const SizedBox(
                          width: 16,
                          height: 16,
                          child: CircularProgressIndicator(strokeWidth: 2),
                        )
                      : const Icon(Icons.folder_open_outlined),
                  label: const Text('选择文件夹'),
                ),
              ),
            ],
            if (_error != null) ...[
              const SizedBox(height: 10),
              Text(
                _error!,
                style: TextStyle(color: Theme.of(context).colorScheme.error),
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
              : Text(widget.submitLabel),
        ),
      ],
    );
  }
}
