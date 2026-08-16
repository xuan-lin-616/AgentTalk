import 'dart:convert';

import 'package:flutter/material.dart';
import 'package:flutter/services.dart';

import '../ipc/core_ipc_client.dart';

class ConfigTransferDialog extends StatefulWidget {
  const ConfigTransferDialog({
    super.key,
    required this.client,
    required this.sessionId,
    required this.projectId,
    this.onImported,
  });

  final CoreIpcClient client;
  final String sessionId;
  final String projectId;
  final ValueChanged<ConfigImportResult>? onImported;

  @override
  State<ConfigTransferDialog> createState() => _ConfigTransferDialogState();
}

class _ConfigTransferDialogState extends State<ConfigTransferDialog> {
  final _importController = TextEditingController();
  final _exportController = TextEditingController();
  Map<String, dynamic>? _preview;
  String? _status;
  bool _exporting = false;
  bool _importing = false;

  @override
  void dispose() {
    _importController.dispose();
    _exportController.dispose();
    super.dispose();
  }

  Future<void> _export() async {
    setState(() {
      _exporting = true;
      _status = null;
    });
    try {
      final config = await widget.client.exportProjectConfig(
        sessionId: widget.sessionId,
        projectId: widget.projectId,
      );
      final text = const JsonEncoder.withIndent('  ').convert(config);
      _exportController.text = text;
      await Clipboard.setData(ClipboardData(text: text));
      if (!mounted) return;
      setState(() => _status = '配置已生成并复制到剪贴板；未包含 workspace 路径或凭据。');
    } on Object catch (error) {
      if (mounted) setState(() => _status = '导出失败：$error');
    } finally {
      if (mounted) setState(() => _exporting = false);
    }
  }

  void _previewImport() {
    try {
      final decoded = jsonDecode(_importController.text);
      if (decoded is! Map<String, dynamic>) {
        throw const FormatException('配置根节点必须是 object');
      }
      setState(() {
        _preview = decoded;
        _status = '配置格式有效；确认后由 Core 重新生成目标 ID。';
      });
    } on Object catch (error) {
      setState(() {
        _preview = null;
        _status = '配置预览失败：$error';
      });
    }
  }

  Future<void> _import() async {
    final preview = _preview;
    if (preview == null) return;
    setState(() {
      _importing = true;
      _status = null;
    });
    try {
      final result = await widget.client.importProjectConfig(
        sessionId: widget.sessionId,
        config: preview,
      );
      if (!mounted) return;
      widget.onImported?.call(result);
      Navigator.of(context).pop(result);
    } on Object catch (error) {
      if (mounted) setState(() => _status = '导入被 Core 拒绝：$error');
    } finally {
      if (mounted) setState(() => _importing = false);
    }
  }

  int _count(String key) {
    final value = _preview?[key];
    return value is List ? value.length : 0;
  }

  @override
  Widget build(BuildContext context) {
    return AlertDialog(
      title: const Row(
        children: [
          Icon(Icons.import_export),
          SizedBox(width: 8),
          Text('配置导入 / 导出'),
        ],
      ),
      content: SizedBox(
        width: 720,
        child: SingleChildScrollView(
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.stretch,
            children: [
              Text(
                '导出的是当前项目的安全元数据。导入后工作区需要在本机重新授权。',
                style: Theme.of(context).textTheme.bodySmall,
              ),
              const SizedBox(height: 12),
              Row(
                children: [
                  FilledButton.icon(
                    onPressed: _exporting ? null : _export,
                    icon: _exporting
                        ? const SizedBox(
                            width: 16,
                            height: 16,
                            child: CircularProgressIndicator(strokeWidth: 2),
                          )
                        : const Icon(Icons.download_outlined),
                    label: const Text('导出并复制'),
                  ),
                  const SizedBox(width: 8),
                  TextButton(
                    onPressed: _exportController.text.isEmpty
                        ? null
                        : () => Clipboard.setData(
                            ClipboardData(text: _exportController.text),
                          ),
                    child: const Text('再次复制'),
                  ),
                ],
              ),
              const SizedBox(height: 8),
              TextField(
                controller: _exportController,
                readOnly: true,
                maxLines: 5,
                decoration: const InputDecoration(
                  labelText: '导出 JSON',
                  border: OutlineInputBorder(),
                ),
              ),
              const SizedBox(height: 16),
              TextField(
                controller: _importController,
                maxLines: 6,
                onChanged: (_) => setState(() => _preview = null),
                decoration: const InputDecoration(
                  labelText: '粘贴要导入的 JSON',
                  hintText: '{"schemaVersion":"config.transfer.v1", ...}',
                  border: OutlineInputBorder(),
                ),
              ),
              const SizedBox(height: 8),
              Align(
                alignment: Alignment.centerLeft,
                child: OutlinedButton.icon(
                  onPressed: _importController.text.trim().isEmpty
                      ? null
                      : _previewImport,
                  icon: const Icon(Icons.visibility_outlined),
                  label: const Text('预览导入'),
                ),
              ),
              if (_preview != null) ...[
                const SizedBox(height: 8),
                Card(
                  child: Padding(
                    padding: const EdgeInsets.all(12),
                    child: Wrap(
                      spacing: 18,
                      runSpacing: 6,
                      children: [
                        Text('项目：${_preview!['project']?['name'] ?? '-'}'),
                        Text('智能体：${_count('agents')}'),
                        Text('会话：${_count('conversations')}'),
                        Text('工作流：${_count('workflowTemplates')}'),
                      ],
                    ),
                  ),
                ),
              ],
              if (_status != null) ...[
                const SizedBox(height: 8),
                Text(_status!, style: Theme.of(context).textTheme.bodySmall),
              ],
            ],
          ),
        ),
      ),
      actions: [
        TextButton(
          onPressed: _importing ? null : () => Navigator.of(context).pop(),
          child: const Text('关闭'),
        ),
        FilledButton.icon(
          onPressed: _preview == null || _importing ? null : _import,
          icon: _importing
              ? const SizedBox(
                  width: 16,
                  height: 16,
                  child: CircularProgressIndicator(strokeWidth: 2),
                )
              : const Icon(Icons.upload_outlined),
          label: const Text('确认导入'),
        ),
      ],
    );
  }
}
