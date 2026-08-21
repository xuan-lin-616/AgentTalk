import 'dart:async';

import 'package:flutter/material.dart';
import 'package:flutter/services.dart';

import '../ipc/core_ipc_client.dart';
import 'connector_center_dialog.dart' show ConnectorProfileEditorDialog;

class LocalAgentCenterDialog extends StatefulWidget {
  const LocalAgentCenterDialog({
    super.key,
    required this.client,
    required this.sessionId,
    this.projectId,
    this.onProjectionChanged,
    this.onScanLocalAgents,
    this.onManualAdd,
  });

  final CoreIpcClient client;
  final String sessionId;
  final String? projectId;
  final ValueChanged<Map<String, dynamic>>? onProjectionChanged;
  final VoidCallback? onScanLocalAgents;
  final VoidCallback? onManualAdd;

  @override
  State<LocalAgentCenterDialog> createState() => _LocalAgentCenterDialogState();
}

class _LocalAgentCenterDialogState extends State<LocalAgentCenterDialog> {
  static const _scopeId = 'desktop';

  static const List<_LocalIntegrationDescriptor> _integrations = [
    _LocalIntegrationDescriptor(
      id: 'local.codex',
      displayName: 'Codex',
      description: 'OpenAI Codex 本地 CLI，通过 codex app-server stdio 连接。',
      protocol: 'codex app-server (stdio JSON-RPC)',
      runtimeTypeName: 'codex',
      providerType: 'codex',
      installCommand: 'npm install -g @openai/codex',
    ),
    _LocalIntegrationDescriptor(
      id: 'local.claude-code',
      displayName: 'Claude Code',
      description: 'Anthropic Claude Code CLI，通过 claude --acp stdio ACP 连接。',
      protocol: 'ACP over stdio (claude --acp)',
      runtimeTypeName: 'claude-code',
      providerType: 'anthropic',
      installCommand: 'npm install -g @anthropic-ai/claude-code',
    ),
    _LocalIntegrationDescriptor(
      id: 'local.antigravity',
      displayName: 'Antigravity',
      description: 'Google Antigravity agy CLI，NDJSON 流连接。',
      protocol: 'agy stream-json (NDJSON stdio)',
      runtimeTypeName: 'antigravity',
      providerType: 'antigravity',
      installCommand: '',
    ),
  ];

  List<LocalConnectorDiscovery> _discoveries = const [];
  List<ConnectorProfileMetadata> _profiles = const [];
  Map<String, ConnectorHealthResult?> _healthByConnectorId = const {};
  bool _discovering = true;
  bool _loadingProfiles = true;
  bool _busy = false;
  String? _error;
  String? _status;

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
        _error = '本地智能体检测失败：$error';
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

  Future<void> _connect(_LocalIntegrationDescriptor integration) async {
    final profile = await showDialog<ConnectorProfileMetadata>(
      context: context,
      builder: (context) => ConnectorProfileEditorDialog(
        scopeId: _scopeId,
        initialConnectorId: integration.id,
        initialDisplayName: integration.displayName,
        initialProviderType: integration.providerType,
        initialRuntimeType: integration.runtimeTypeName,
      ),
    );
    if (profile == null || !mounted) return;
    setState(() {
      _busy = true;
      _status = null;
      _error = null;
    });
    try {
      final result = await widget.client.createConnectorProfile(
        sessionId: widget.sessionId,
        profile: profile,
      );
      widget.onProjectionChanged?.call(result.projection);
      await _reloadProfiles();
      if (!mounted) return;
      setState(() {
        _status = result.changed
            ? '${integration.displayName} 已配置为 ${integration.id}'
            : '${integration.displayName} 已是目标状态';
      });
    } on Object catch (error) {
      if (mounted) {
        setState(() => _error = 'Connector 保存被 Core 拒绝：$error');
      }
    } finally {
      if (mounted) setState(() => _busy = false);
    }
  }

  Future<void> _importAgent(_LocalIntegrationDescriptor integration) async {
    final projectId = widget.projectId;
    if (projectId == null || projectId.trim().isEmpty) {
      if (mounted) {
        ScaffoldMessenger.of(
          context,
        ).showSnackBar(const SnackBar(content: Text('请先在主界面选择项目，再导入本地智能体。')));
      }
      return;
    }
    setState(() {
      _busy = true;
      _status = null;
      _error = null;
    });
    try {
      // Ensure a Connector profile exists so Core can resolve runtimeType
      // (antigravity/codex/...) when this agent is used in a conversation.
      if (_profileFor(integration.id) == null) {
        final profileResult = await widget.client.createConnectorProfile(
          sessionId: widget.sessionId,
          profile: ConnectorProfileMetadata(
            scopeId: _scopeId,
            connectorId: integration.id,
            displayName: integration.displayName,
            providerType: integration.providerType,
            runtimeTypeName: integration.runtimeTypeName,
            enabled: true,
          ),
        );
        widget.onProjectionChanged?.call(profileResult.projection);
      }

      final agentId = 'agent-${DateTime.now().microsecondsSinceEpoch}';
      final createResponse = await widget.client.request({
        'kind': 'command',
        'protocol': {'major': 1, 'minor': 0},
        'requestId': 'agent-create-$agentId',
        'sessionId': widget.sessionId,
        'command': 'agent.create',
        'payload': {
          'agentId': agentId,
          'name': integration.displayName,
          'role': '项目智能体',
          'specialty': '本地智能体',
          'systemPrompt': '你是本地智能体，通过 AgentTalk 接入当前项目。',
        },
      });
      final createPayload = createResponse['payload'];
      final createProjection = createPayload is Map<String, dynamic>
          ? createPayload['projection']
          : null;
      if (createProjection is! Map<String, dynamic>) {
        throw const CoreIpcException('智能体创建后的投影无效');
      }

      await widget.client.setAgentModelBinding(
        sessionId: widget.sessionId,
        agentId: agentId,
        connectorId: integration.id,
      );
      final joinResult = await widget.client.setProjectAgentModelSelection(
        sessionId: widget.sessionId,
        projectId: projectId,
        agentId: agentId,
        enabled: true,
        workspaceAccess: 'none',
        modelSelectionMode: 'connector_default',
        candidateModelListMode: 'inherit',
        candidateModelListRevision: 0,
      );
      final joinedProjection = joinResult['projection'];
      if (joinedProjection is! Map<String, dynamic>) {
        throw const CoreIpcException('项目智能体加入后的投影无效');
      }
      widget.onProjectionChanged?.call(joinedProjection);
      await _reloadProfiles();
      if (!mounted) return;
      setState(() {
        _status = '${integration.displayName} 已导入并加入当前项目';
      });
    } on Object catch (error) {
      if (mounted) {
        setState(() => _error = '导入 ${integration.displayName} 失败：$error');
      }
    } finally {
      if (mounted) setState(() => _busy = false);
    }
  }

  Future<void> _edit(ConnectorProfileMetadata existing) async {
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
      final result = await widget.client.updateConnectorProfile(
        sessionId: widget.sessionId,
        profile: profile,
      );
      widget.onProjectionChanged?.call(result.projection);
      await _reloadProfiles();
      if (!mounted) return;
      setState(() {
        _status = result.changed ? 'Connector 已更新' : 'Connector 已是目标状态';
      });
    } on Object catch (error) {
      if (mounted) setState(() => _error = 'Connector 保存被 Core 拒绝：$error');
    } finally {
      if (mounted) setState(() => _busy = false);
    }
  }

  Future<void> _showInstallCommand(
    _LocalIntegrationDescriptor integration,
  ) async {
    final command = integration.installCommand.trim();
    if (command.isEmpty) {
      await showDialog<void>(
        context: context,
        builder: (context) => AlertDialog(
          title: Text('${integration.displayName} 安装'),
          content: const Text(
            '暂无官方一键安装命令。请通过 Antigravity 官方渠道安装 agy CLI 后重试检测。',
          ),
          actions: [
            TextButton(
              onPressed: () => Navigator.of(context).pop(),
              child: const Text('知道了'),
            ),
          ],
        ),
      );
      return;
    }
    if (!mounted) return;
    final confirmed = await showDialog<bool>(
      context: context,
      builder: (context) => AlertDialog(
        title: Text('安装 ${integration.displayName}'),
        content: Column(
          mainAxisSize: MainAxisSize.min,
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            const Text('AgentTalk 不会自动执行安装命令。请确认后复制命令，在终端中执行：'),
            const SizedBox(height: 12),
            Container(
              width: double.maxFinite,
              padding: const EdgeInsets.all(12),
              decoration: BoxDecoration(
                color: Theme.of(context).colorScheme.surfaceContainerHighest,
                borderRadius: BorderRadius.circular(8),
              ),
              child: SelectableText(
                command,
                style: const TextStyle(fontFamily: 'monospace'),
              ),
            ),
            const SizedBox(height: 12),
            const Text('安装完成后回到本页重新检测。'),
          ],
        ),
        actions: [
          TextButton(
            onPressed: () => Navigator.of(context).pop(false),
            child: const Text('取消'),
          ),
          FilledButton.icon(
            onPressed: () => Navigator.of(context).pop(true),
            icon: const Icon(Icons.content_copy),
            label: const Text('确认并复制命令'),
          ),
        ],
      ),
    );
    if (confirmed == true && mounted) {
      await Clipboard.setData(ClipboardData(text: command));
      if (mounted) {
        setState(() => _status = '安装命令已复制，请勿在 AgentTalk 内自动执行');
      }
    }
  }

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);
    return AlertDialog(
      title: const Row(
        children: [
          Icon(Icons.hub_outlined),
          SizedBox(width: 8),
          Text('本地智能体中心'),
        ],
      ),
      content: SizedBox(
        width: 960,
        height: 680,
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.stretch,
          children: [
            Text(
              '对已知主流智能体执行确定性检测（CLI 版本/登录态），并显示官方安装命令；'
              '连接会创建或更新 Connector 配置，不会静默执行任何安装命令。',
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
                      : const Icon(Icons.refresh),
                  label: const Text('重新检测'),
                ),
                const SizedBox(width: 8),
                OutlinedButton.icon(
                  onPressed: _busy ? null : widget.onScanLocalAgents,
                  icon: const Icon(Icons.radar_outlined),
                  label: const Text('扫描其他本地智能体'),
                ),
                const SizedBox(width: 8),
                OutlinedButton.icon(
                  onPressed: _busy ? null : widget.onManualAdd,
                  icon: const Icon(Icons.add),
                  label: const Text('手动添加'),
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
          child: const Text('关闭'),
        ),
      ],
    );
  }

  Widget _buildBody(BuildContext context) {
    if (_discovering && _discoveries.isEmpty && _loadingProfiles) {
      return const Center(child: CircularProgressIndicator());
    }
    if (_error != null && _discoveries.isEmpty) {
      return Center(
        child: Column(
          mainAxisSize: MainAxisSize.min,
          children: [
            Icon(
              Icons.error_outline,
              color: Theme.of(context).colorScheme.error,
              size: 40,
            ),
            const SizedBox(height: 8),
            Text(_error!, textAlign: TextAlign.center),
            const SizedBox(height: 8),
            OutlinedButton.icon(
              onPressed: _busy ? null : _refreshDiscovery,
              icon: const Icon(Icons.refresh),
              label: const Text('重试'),
            ),
          ],
        ),
      );
    }
    return ListView(
      children: [
        for (final integration in _integrations)
          _IntegrationCard(
            integration: integration,
            discovery: _discoveryFor(integration.id),
            profile: _profileFor(integration.id),
            health: _healthByConnectorId[integration.id]?.connector,
            busy: _busy,
            onConnect: () => _connect(integration),
            onEdit: (profile) => _edit(profile),
            onInstall: () => _showInstallCommand(integration),
            onImport: () => _importAgent(integration),
          ),
        const SizedBox(height: 16),
        Text(
          '已配置 Connector',
          style: Theme.of(
            context,
          ).textTheme.titleSmall?.copyWith(fontWeight: FontWeight.w700),
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
            subtitle: Text('从上方选择一个已安装智能体，或点击“手动添加”。'),
          )
        else
          ..._profiles.map((profile) {
            final health = _healthByConnectorId[profile.connectorId]?.connector;
            return ListTile(
              contentPadding: EdgeInsets.zero,
              leading: Icon(
                profile.enabled ? Icons.link : Icons.link_off,
                color: profile.enabled
                    ? Theme.of(context).colorScheme.primary
                    : Theme.of(context).colorScheme.outline,
              ),
              title: Text(profile.displayName),
              subtitle: Text(
                '${profile.connectorId} · ${profile.providerType} · '
                '${profile.runtimeTypeName}\n'
                '${profile.authEnvKey == null ? '未绑定认证环境变量' : '认证环境变量：${profile.authEnvKey}'}\n'
                '健康：${health == null ? '不可用或未验证' : health.status}',
              ),
              isThreeLine: true,
              trailing: IconButton(
                tooltip: '编辑 Connector',
                onPressed: _busy ? null : () => _edit(profile),
                icon: const Icon(Icons.edit_outlined),
              ),
            );
          }),
      ],
    );
  }

  LocalConnectorDiscovery? _discoveryFor(String integrationId) {
    for (final discovery in _discoveries) {
      if (discovery.connectorId == integrationId) return discovery;
    }
    return null;
  }

  ConnectorProfileMetadata? _profileFor(String integrationId) {
    for (final profile in _profiles) {
      if (profile.connectorId == integrationId) return profile;
    }
    return null;
  }
}

class _LocalIntegrationDescriptor {
  const _LocalIntegrationDescriptor({
    required this.id,
    required this.displayName,
    required this.description,
    required this.protocol,
    required this.runtimeTypeName,
    required this.providerType,
    required this.installCommand,
  });

  final String id;
  final String displayName;
  final String description;
  final String protocol;
  final String runtimeTypeName;
  final String providerType;
  final String installCommand;
}

class _IntegrationCard extends StatelessWidget {
  const _IntegrationCard({
    required this.integration,
    required this.discovery,
    required this.profile,
    required this.health,
    required this.busy,
    required this.onConnect,
    required this.onEdit,
    required this.onInstall,
    required this.onImport,
  });

  final _LocalIntegrationDescriptor integration;
  final LocalConnectorDiscovery? discovery;
  final ConnectorProfileMetadata? profile;
  final ConnectorHealth? health;
  final bool busy;
  final VoidCallback onConnect;
  final ValueChanged<ConnectorProfileMetadata> onEdit;
  final VoidCallback onInstall;
  final VoidCallback onImport;

  bool get _installed =>
      discovery != null && discovery!.availability != 'unavailable';

  bool get _canImport =>
      _installed && discovery!.availability != 'authentication_required';

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);
    final installed = _installed;
    final version = discovery?.catalogRevision;
    final status = installed
        ? switch (discovery!.availability) {
            'authentication_required' => '已安装 · 需要登录',
            _ => '已安装 · 可连接',
          }
        : '未安装';
    final isConfigured = profile != null;
    return Card(
      margin: const EdgeInsets.only(bottom: 10),
      child: Padding(
        padding: const EdgeInsets.all(14),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Row(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Icon(
                  installed ? Icons.smart_toy_outlined : Icons.smart_toy,
                  color: installed
                      ? theme.colorScheme.primary
                      : theme.colorScheme.outline,
                ),
                const SizedBox(width: 10),
                Expanded(
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      Text(
                        integration.displayName,
                        style: theme.textTheme.titleSmall?.copyWith(
                          fontWeight: FontWeight.w700,
                        ),
                      ),
                      const SizedBox(height: 2),
                      Text(
                        '${integration.id} · ${integration.protocol}',
                        style: theme.textTheme.bodySmall,
                      ),
                      const SizedBox(height: 4),
                      Text(
                        integration.description,
                        style: theme.textTheme.bodySmall?.copyWith(
                          color: theme.colorScheme.onSurfaceVariant,
                        ),
                      ),
                    ],
                  ),
                ),
                const SizedBox(width: 8),
                Chip(
                  label: Text(status),
                  side: BorderSide(color: theme.colorScheme.outlineVariant),
                  backgroundColor: installed
                      ? theme.colorScheme.primaryContainer
                      : theme.colorScheme.surfaceContainerHighest,
                ),
              ],
            ),
            if (installed && version != null) ...[
              const SizedBox(height: 6),
              Text(
                '检测版本：$version',
                style: theme.textTheme.bodySmall?.copyWith(
                  fontFamily: 'monospace',
                ),
              ),
            ],
            const SizedBox(height: 10),
            Row(
              children: [
                if (!installed)
                  OutlinedButton.icon(
                    onPressed: busy ? null : onInstall,
                    icon: const Icon(Icons.download_outlined),
                    label: const Text('一键安装（显示命令）'),
                  )
                else ...[
                  if (_canImport) ...[
                    FilledButton.icon(
                      onPressed: busy ? null : onImport,
                      icon: const Icon(Icons.person_add_alt_1_outlined),
                      label: const Text('导入'),
                    ),
                    const SizedBox(width: 8),
                  ],
                  if (isConfigured)
                    OutlinedButton.icon(
                      onPressed: busy ? null : () => onEdit(profile!),
                      icon: const Icon(Icons.settings_outlined),
                      label: const Text('配置'),
                    )
                  else
                    OutlinedButton.icon(
                      onPressed: busy ? null : onConnect,
                      icon: const Icon(Icons.link_outlined),
                      label: const Text('连接/配置'),
                    ),
                ],
              ],
            ),
            if (isConfigured && health != null) ...[
              const SizedBox(height: 6),
              Text(
                'Connector 健康：${health?.status ?? '不可用或未验证'}',
                style: theme.textTheme.bodySmall,
              ),
            ],
          ],
        ),
      ),
    );
  }
}
