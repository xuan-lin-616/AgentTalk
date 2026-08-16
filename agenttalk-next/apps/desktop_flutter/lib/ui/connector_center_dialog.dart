import 'dart:async';

import 'package:flutter/material.dart';

import '../gen/l10n.dart';
import '../ipc/core_ipc_client.dart';

class ConnectorCenterDialog extends StatefulWidget {
  const ConnectorCenterDialog({
    super.key,
    required this.client,
    required this.sessionId,
    this.onProjectionChanged,
  });

  final CoreIpcClient client;
  final String sessionId;
  final ValueChanged<Map<String, dynamic>>? onProjectionChanged;

  @override
  State<ConnectorCenterDialog> createState() => _ConnectorCenterDialogState();
}

class _ConnectorCenterDialogState extends State<ConnectorCenterDialog> {
  static const _scopeId = 'desktop';

  List<LocalConnectorDiscovery> _discoveries = const [];
  List<ConnectorProfileMetadata> _profiles = const [];
  Map<String, ConnectorHealthResult?> _healthByConnectorId = const {};
  bool _discovering = true;
  bool _loadingProfiles = true;
  String? _error;
  String? _status;
  bool _busy = false;

  @override
  void initState() {
    super.initState();
    unawaited(_reload());
  }

  Future<void> _reload() async {
    await Future.wait([_refreshDiscovery(), _reloadProfiles()]);
  }

  Future<void> _refreshDiscovery() async {
    if (mounted) {
      setState(() {
        _discovering = true;
        _error = null;
      });
    }
    try {
      final result = await widget.client.discoverLocalConnectors(
        sessionId: widget.sessionId,
      );
      if (!mounted) return;
      setState(() {
        _discoveries = result.discoveries;
      });
    } on Object catch (error) {
      if (!mounted) return;
      setState(() {
        _discoveries = const [];
        _error =
            '${AppLocalizations.of(context)!.connectorDiscoverFailed}$error';
      });
    } finally {
      if (mounted) {
        setState(() {
          _discovering = false;
        });
      }
    }
  }

  Future<void> _reloadProfiles() async {
    if (mounted) {
      setState(() {
        _loadingProfiles = true;
      });
    }
    try {
      final profiles = await widget.client.queryConnectorProfiles(
        sessionId: widget.sessionId,
        scopeId: _scopeId,
      );
      final healthByConnectorId = <String, ConnectorHealthResult?>{};
      for (final profile in profiles) {
        try {
          healthByConnectorId[profile.connectorId] = await widget.client
              .queryConnectorHealth(
                sessionId: widget.sessionId,
                scopeId: _scopeId,
                connectorId: profile.connectorId,
              );
        } on Object {
          healthByConnectorId[profile.connectorId] = null;
        }
      }
      if (!mounted) return;
      setState(() {
        _profiles = profiles;
        _healthByConnectorId = healthByConnectorId;
      });
    } on Object catch (error) {
      if (!mounted) return;
      setState(() {
        _error = 'Connector 查询失败：$error';
      });
    } finally {
      if (mounted) {
        setState(() {
          _loadingProfiles = false;
        });
      }
    }
  }

  Future<void> _edit([ConnectorProfileMetadata? existing]) async {
    final profile = await showDialog<ConnectorProfileMetadata>(
      context: context,
      builder: (context) =>
          ConnectorProfileEditorDialog(scopeId: _scopeId, existing: existing),
    );
    if (profile == null || !mounted) return;
    setState(() {
      _busy = true;
      _status = null;
      _error = null;
    });
    try {
      final result = existing == null
          ? await widget.client.createConnectorProfile(
              sessionId: widget.sessionId,
              profile: profile,
            )
          : await widget.client.updateConnectorProfile(
              sessionId: widget.sessionId,
              profile: profile,
            );
      widget.onProjectionChanged?.call(result.projection);
      await _reload();
      if (!mounted) return;
      setState(() {
        _status = result.changed
            ? (existing == null ? 'Connector 已创建' : 'Connector 已更新')
            : 'Connector 已是目标状态';
      });
    } on Object catch (error) {
      if (mounted) setState(() => _error = 'Connector 保存被 Core 拒绝：$error');
    } finally {
      if (mounted) setState(() => _busy = false);
    }
  }

  Future<void> _remove(ConnectorProfileMetadata profile) async {
    final confirmed = await showDialog<bool>(
      context: context,
      builder: (context) => AlertDialog(
        title: const Text('删除 Connector？'),
        content: Text('将删除「${profile.displayName}」的本地元数据。不会删除或读取认证值。'),
        actions: [
          TextButton(
            onPressed: () => Navigator.of(context).pop(false),
            child: const Text('取消'),
          ),
          FilledButton(
            onPressed: () => Navigator.of(context).pop(true),
            child: const Text('确认删除'),
          ),
        ],
      ),
    );
    if (confirmed != true || !mounted) return;
    setState(() {
      _busy = true;
      _status = null;
      _error = null;
    });
    try {
      final result = await widget.client.removeConnectorProfile(
        sessionId: widget.sessionId,
        scopeId: _scopeId,
        connectorId: profile.connectorId,
      );
      widget.onProjectionChanged?.call(result.projection);
      await _reload();
      if (!mounted) return;
      setState(() {
        _status = result.removed ? 'Connector 已删除' : 'Connector 已不存在';
      });
    } on Object catch (error) {
      if (mounted) setState(() => _error = 'Connector 删除被 Core 拒绝：$error');
    } finally {
      if (mounted) setState(() => _busy = false);
    }
  }

  @override
  Widget build(BuildContext context) {
    final l10n = AppLocalizations.of(context)!;
    final theme = Theme.of(context);
    return AlertDialog(
      title: const Row(
        children: [
          Icon(Icons.extension_outlined),
          SizedBox(width: 8),
          Text('Connector 管理'),
        ],
      ),
      content: SizedBox(
        width: 920,
        height: 640,
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.stretch,
          children: [
            Text(
              '刷新会真实调用 connector.discover；下面先展示本地候选，再显示已配置的 Connector。',
              style: theme.textTheme.bodySmall,
            ),
            const SizedBox(height: 12),
            Row(
              children: [
                FilledButton.icon(
                  onPressed: _busy ? null : _refreshDiscovery,
                  icon: _discovering
                      ? const SizedBox(
                          width: 16,
                          height: 16,
                          child: CircularProgressIndicator(strokeWidth: 2),
                        )
                      : const Icon(Icons.radar_outlined),
                  label: Text(l10n.connectorDiscoverRescan),
                ),
                const SizedBox(width: 8),
                OutlinedButton.icon(
                  onPressed: _busy ? null : () => _edit(),
                  icon: const Icon(Icons.add),
                  label: const Text('新增 Connector'),
                ),
              ],
            ),
            const SizedBox(height: 12),
            Expanded(child: _buildBody(context)),
            if (_status != null) ...[
              const SizedBox(height: 8),
              Text(_status!, style: theme.textTheme.bodySmall),
            ],
          ],
        ),
      ),
      actions: [
        TextButton(
          onPressed: _busy ? null : () => Navigator.of(context).pop(),
          child: Text(l10n.cancel),
        ),
      ],
    );
  }

  Widget _buildBody(BuildContext context) {
    if (_discovering && _discoveries.isEmpty && _loadingProfiles) {
      return const Center(child: CircularProgressIndicator());
    }
    final l10n = AppLocalizations.of(context)!;
    final theme = Theme.of(context);
    if (_error != null) {
      return Center(
        child: Column(
          mainAxisSize: MainAxisSize.min,
          children: [
            Icon(Icons.error_outline, color: theme.colorScheme.error, size: 40),
            const SizedBox(height: 8),
            Text(_error!, textAlign: TextAlign.center),
            const SizedBox(height: 8),
            OutlinedButton.icon(
              onPressed: _busy ? null : _refreshDiscovery,
              icon: const Icon(Icons.refresh),
              label: Text(l10n.connectorDiscoverRetry),
            ),
          ],
        ),
      );
    }
    return ListView(
      children: [
        _DiscoverySection(
          discoveries: _discoveries,
          discovering: _discovering,
          onRefresh: _busy ? null : _refreshDiscovery,
        ),
        const SizedBox(height: 16),
        Text(
          '已配置 Connector',
          style: theme.textTheme.titleSmall?.copyWith(
            fontWeight: FontWeight.w700,
          ),
        ),
        const SizedBox(height: 8),
        if (_loadingProfiles)
          const Center(
            child: Padding(
              padding: EdgeInsets.all(16),
              child: CircularProgressIndicator(),
            ),
          )
        else if (_profiles.isEmpty)
          const ListTile(
            contentPadding: EdgeInsets.zero,
            leading: Icon(Icons.link_off_outlined),
            title: Text('尚未配置 Connector'),
            subtitle: Text('可通过“新增 Connector”手动添加元数据。'),
          )
        else
          ..._profiles.map((profile) {
            final health = _healthByConnectorId[profile.connectorId]?.connector;
            return ListTile(
              contentPadding: EdgeInsets.zero,
              leading: Icon(
                profile.enabled ? Icons.link : Icons.link_off,
                color: profile.enabled
                    ? theme.colorScheme.primary
                    : theme.colorScheme.outline,
              ),
              title: Text(profile.displayName),
              subtitle: Text(
                '${profile.connectorId} · ${profile.providerType} · '
                '${profile.runtimeTypeName}\n'
                '${profile.authEnvKey == null ? '未绑定认证环境变量' : '认证环境变量：${profile.authEnvKey}'}\n'
                '健康：${health == null ? '不可用或未验证' : health.status} · '
                '${health?.verification ?? 'local_adapter_only'}',
              ),
              isThreeLine: true,
              trailing: Wrap(
                spacing: 0,
                children: [
                  IconButton(
                    tooltip: '编辑 Connector',
                    onPressed: _busy ? null : () => _edit(profile),
                    icon: const Icon(Icons.edit_outlined),
                  ),
                  IconButton(
                    tooltip: '删除 Connector',
                    onPressed: _busy ? null : () => _remove(profile),
                    icon: const Icon(Icons.delete_outline),
                  ),
                ],
              ),
            );
          }),
      ],
    );
  }
}

class _DiscoverySection extends StatelessWidget {
  const _DiscoverySection({
    required this.discoveries,
    required this.discovering,
    this.onRefresh,
  });

  final List<LocalConnectorDiscovery> discoveries;
  final bool discovering;
  final VoidCallback? onRefresh;

  @override
  Widget build(BuildContext context) {
    final l10n = AppLocalizations.of(context)!;
    final theme = Theme.of(context);
    final hasResults = discoveries.isNotEmpty;
    return Card(
      elevation: 0,
      color: theme.colorScheme.surfaceContainerLow,
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.stretch,
          children: [
            Row(
              children: [
                Expanded(
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      Text(
                        hasResults
                            ? l10n.connectorDiscoverSupported
                            : l10n.connectorDiscoverEmptyTitle,
                        style: theme.textTheme.titleMedium?.copyWith(
                          fontWeight: FontWeight.w700,
                        ),
                      ),
                      const SizedBox(height: 4),
                      Text(
                        hasResults
                            ? '本页展示 `connector.discover` 的本地候选。'
                            : l10n.connectorDiscoverEmptySubtitle,
                        style: theme.textTheme.bodySmall,
                      ),
                    ],
                  ),
                ),
                if (onRefresh != null)
                  FilledButton.icon(
                    onPressed: onRefresh,
                    icon: discovering
                        ? const SizedBox(
                            width: 16,
                            height: 16,
                            child: CircularProgressIndicator(strokeWidth: 2),
                          )
                        : const Icon(Icons.refresh, size: 18),
                    label: Text(l10n.connectorDiscoverRescan),
                  ),
              ],
            ),
            if (discovering) ...[
              const SizedBox(height: 12),
              const LinearProgressIndicator(),
            ],
            if (hasResults) ...[
              const SizedBox(height: 12),
              ...discoveries.map(
                (discovery) => Padding(
                  padding: const EdgeInsets.only(bottom: 12),
                  child: _DiscoveryCard(discovery: discovery),
                ),
              ),
            ] else if (!discovering) ...[
              const SizedBox(height: 12),
              Center(
                child: Column(
                  mainAxisSize: MainAxisSize.min,
                  children: [
                    Icon(
                      Icons.radar_outlined,
                      size: 40,
                      color: theme.colorScheme.onSurfaceVariant,
                    ),
                    const SizedBox(height: 8),
                    Text(l10n.connectorDiscoverNotFound),
                    const SizedBox(height: 8),
                    if (onRefresh != null)
                      OutlinedButton.icon(
                        onPressed: onRefresh,
                        icon: const Icon(Icons.refresh),
                        label: Text(l10n.connectorDiscoverRetry),
                      ),
                  ],
                ),
              ),
            ],
          ],
        ),
      ),
    );
  }
}

class _DiscoveryCard extends StatelessWidget {
  const _DiscoveryCard({required this.discovery});

  final LocalConnectorDiscovery discovery;

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);
    final availability = _discoveryAvailability(discovery);
    return Card(
      margin: EdgeInsets.zero,
      child: Padding(
        padding: const EdgeInsets.all(14),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Row(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Expanded(
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      Text(
                        discovery.displayName,
                        style: theme.textTheme.titleSmall?.copyWith(
                          fontWeight: FontWeight.w700,
                        ),
                      ),
                      const SizedBox(height: 2),
                      Text(
                        '${discovery.connectorId} · ${discovery.connectorRuntimeType}',
                        style: theme.textTheme.bodySmall,
                      ),
                    ],
                  ),
                ),
                Chip(
                  label: Text(availability),
                  side: BorderSide(color: theme.colorScheme.outlineVariant),
                  backgroundColor: theme.colorScheme.surfaceContainerHighest,
                ),
              ],
            ),
            const SizedBox(height: 12),
            Wrap(
              spacing: 8,
              runSpacing: 8,
              children: [
                _DiscoveryFieldChip(
                  label: 'connectorId',
                  value: discovery.connectorId,
                ),
                _DiscoveryFieldChip(
                  label: 'runtimeType',
                  value: discovery.connectorRuntimeType,
                ),
                _DiscoveryFieldChip(
                  label: 'displayName',
                  value: discovery.displayName,
                ),
                _DiscoveryFieldChip(label: 'availability', value: availability),
                _DiscoveryFieldChip(
                  label: 'models',
                  value: discovery.models.isEmpty
                      ? '—'
                      : discovery.models.join('，'),
                ),
                _DiscoveryFieldChip(
                  label: 'catalogRevision',
                  value: discovery.catalogRevision ?? '—',
                ),
                _DiscoveryFieldChip(
                  label: 'source',
                  value: _sanitizeDiscoverySource(discovery.source),
                ),
                _DiscoveryFieldChip(
                  label: 'requiresConfiguration',
                  value: discovery.requiresConfiguration ? '是' : '否',
                ),
              ],
            ),
          ],
        ),
      ),
    );
  }
}

class _DiscoveryFieldChip extends StatelessWidget {
  const _DiscoveryFieldChip({required this.label, required this.value});

  final String label;
  final String value;

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);
    return Container(
      constraints: const BoxConstraints(minWidth: 180),
      padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 10),
      decoration: BoxDecoration(
        color: theme.colorScheme.surfaceContainerHighest,
        borderRadius: BorderRadius.circular(10),
        border: Border.all(color: theme.colorScheme.outlineVariant),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Text(
            label,
            style: theme.textTheme.labelSmall?.copyWith(
              color: theme.colorScheme.onSurfaceVariant,
            ),
          ),
          const SizedBox(height: 4),
          Text(
            value,
            maxLines: 2,
            overflow: TextOverflow.ellipsis,
            style: theme.textTheme.bodySmall?.copyWith(
              fontWeight: FontWeight.w600,
            ),
          ),
        ],
      ),
    );
  }
}

String _discoveryAvailability(LocalConnectorDiscovery discovery) {
  switch (discovery.availability) {
    case 'available':
      return '可用';
    case 'unconfigured':
      return '需要配置';
    case 'authentication_required':
      return '需要认证';
    case 'unavailable':
      return discovery.models.isNotEmpty ? '部分可用' : '不可用';
    default:
      return '未知';
  }
}

String _sanitizeDiscoverySource(String source) {
  var value = source;
  value = value.replaceAllMapped(
    RegExp(
      r'\b(binary|path|token|authorization|cookie|secret|password|api[_-]?key)\s*=\s*[^;|, ]+',
      caseSensitive: false,
    ),
    (match) => '${match.group(1)}=<redacted>',
  );
  value = value.replaceAllMapped(
    RegExp(r'([A-Za-z]:\\[^;|, ]+)'),
    (_) => '<redacted-path>',
  );
  return value;
}

class ConnectorProfileEditorDialog extends StatefulWidget {
  const ConnectorProfileEditorDialog({
    super.key,
    required this.scopeId,
    this.existing,
  });

  final String scopeId;
  final ConnectorProfileMetadata? existing;

  @override
  State<ConnectorProfileEditorDialog> createState() =>
      _ConnectorProfileEditorDialogState();
}

class _ConnectorProfileEditorDialogState
    extends State<ConnectorProfileEditorDialog> {
  final _formKey = GlobalKey<FormState>();
  late final TextEditingController _connectorId;
  late final TextEditingController _displayName;
  late final TextEditingController _providerType;
  late final TextEditingController _runtimeType;
  late final TextEditingController _authEnvKey;
  late bool _enabled;

  bool get _editing => widget.existing != null;

  @override
  void initState() {
    super.initState();
    final existing = widget.existing;
    _connectorId = TextEditingController(text: existing?.connectorId ?? '');
    _displayName = TextEditingController(text: existing?.displayName ?? '');
    _providerType = TextEditingController(text: existing?.providerType ?? '');
    _runtimeType = TextEditingController(text: existing?.runtimeTypeName ?? '');
    _authEnvKey = TextEditingController(text: existing?.authEnvKey ?? '');
    _enabled = existing?.enabled ?? true;
  }

  @override
  void dispose() {
    _connectorId.dispose();
    _displayName.dispose();
    _providerType.dispose();
    _runtimeType.dispose();
    _authEnvKey.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return AlertDialog(
      title: Text(_editing ? '编辑 Connector' : '新增 Connector'),
      content: SizedBox(
        width: 560,
        child: Form(
          key: _formKey,
          child: SingleChildScrollView(
            child: Column(
              children: [
                TextFormField(
                  initialValue: widget.scopeId,
                  readOnly: true,
                  decoration: const InputDecoration(
                    labelText: 'Scope',
                    border: OutlineInputBorder(),
                  ),
                ),
                const SizedBox(height: 12),
                TextFormField(
                  controller: _connectorId,
                  readOnly: _editing,
                  validator: _required,
                  decoration: const InputDecoration(
                    labelText: 'Connector ID',
                    border: OutlineInputBorder(),
                  ),
                ),
                const SizedBox(height: 12),
                TextFormField(
                  controller: _displayName,
                  validator: _required,
                  decoration: const InputDecoration(
                    labelText: '显示名称',
                    border: OutlineInputBorder(),
                  ),
                ),
                const SizedBox(height: 12),
                TextFormField(
                  controller: _providerType,
                  validator: _required,
                  decoration: const InputDecoration(
                    labelText: 'Provider 类型',
                    hintText: '例如 openai-compatible',
                    border: OutlineInputBorder(),
                  ),
                ),
                const SizedBox(height: 12),
                TextFormField(
                  controller: _runtimeType,
                  validator: _required,
                  decoration: const InputDecoration(
                    labelText: 'Runtime 类型',
                    hintText: '例如 http 或 mock',
                    border: OutlineInputBorder(),
                  ),
                ),
                const SizedBox(height: 12),
                TextFormField(
                  controller: _authEnvKey,
                  decoration: const InputDecoration(
                    labelText: '认证环境变量名（可选）',
                    helperText: '只保存变量名；不会读取或保存变量值、Token、Header 或 Endpoint。',
                    border: OutlineInputBorder(),
                  ),
                ),
                SwitchListTile(
                  contentPadding: EdgeInsets.zero,
                  title: const Text('启用 Connector'),
                  value: _enabled,
                  onChanged: (value) => setState(() => _enabled = value),
                ),
              ],
            ),
          ),
        ),
      ),
      actions: [
        TextButton(
          onPressed: () => Navigator.of(context).pop(),
          child: const Text('取消'),
        ),
        FilledButton(onPressed: _save, child: const Text('保存到 Core')),
      ],
    );
  }

  String? _required(String? value) {
    return value == null || value.trim().isEmpty ? '不能为空' : null;
  }

  void _save() {
    if (!(_formKey.currentState?.validate() ?? false)) return;
    final authEnvKey = _authEnvKey.text.trim();
    Navigator.of(context).pop(
      ConnectorProfileMetadata(
        scopeId: widget.scopeId,
        connectorId: _connectorId.text.trim(),
        displayName: _displayName.text.trim(),
        providerType: _providerType.text.trim(),
        runtimeTypeName: _runtimeType.text.trim(),
        enabled: _enabled,
        authEnvKey: authEnvKey.isEmpty ? null : authEnvKey,
      ),
    );
  }
}
