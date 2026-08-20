import 'dart:async';

import 'package:flutter/material.dart';

import '../gen/l10n.dart';
import '../ipc/core_ipc_client.dart';

class AgentIdentityInput {
  const AgentIdentityInput({
    required this.name,
    required this.role,
    required this.specialty,
    required this.systemPrompt,
    required this.connectorId,
    required this.modelId,
  });

  final String name;
  final String role;
  final String specialty;
  final String systemPrompt;
  final String connectorId;
  final String modelId;
}

typedef AgentIdentitySubmit = Future<void> Function(AgentIdentityInput input);
typedef ConnectorModelsLoader =
    Future<ConnectorModelCatalog> Function(String connectorId);
typedef IdentityModelOptionsLoader =
    Future<List<IdentityModelOptionMetadata>> Function(String connectorId);

class AgentIdentityDialog extends StatefulWidget {
  const AgentIdentityDialog({
    super.key,
    required this.title,
    required this.onSubmit,
    this.initialName = '',
    this.initialRole = '',
    this.initialSpecialty = '',
    this.initialSystemPrompt = '',
    this.initialConnectorId = '',
    this.initialModelId = '',
    this.knownCatalogModels = const <String, List<String>>{},
    this.submitLabel,
    this.client,
    this.sessionId,
    this.target,
    this.connectorModelsLoader,
    this.identityModelOptionsLoader,
  });

  final String title;
  final AgentIdentitySubmit onSubmit;
  final String initialName;
  final String initialRole;
  final String initialSpecialty;
  final String initialSystemPrompt;
  final String initialConnectorId;
  final String initialModelId;
  final Map<String, List<String>> knownCatalogModels;
  final String? submitLabel;
  final CoreIpcClient? client;
  final String? sessionId;
  final IdentityModelTarget? target;
  final ConnectorModelsLoader? connectorModelsLoader;
  final IdentityModelOptionsLoader? identityModelOptionsLoader;

  @override
  State<AgentIdentityDialog> createState() => _AgentIdentityDialogState();
}

class _AgentIdentityDialogState extends State<AgentIdentityDialog> {
  late final TextEditingController _name;
  late final TextEditingController _role;
  late final TextEditingController _specialty;
  late final TextEditingController _systemPrompt;
  late final TextEditingController _connectorId;
  late final TextEditingController _modelId;
  bool _saving = false;
  String? _error;

  List<IdentityModelOptionMetadata>? _options;
  ConnectorModelCatalog? _catalog;
  bool _loadingOptions = false;
  bool _catalogLoaded = false;
  Object? _catalogFailure;
  Object? _optionsFailure;
  Timer? _connectorLoadDebounce;
  int _loadGeneration = 0;

  @override
  void initState() {
    super.initState();
    _name = TextEditingController(text: widget.initialName);
    _role = TextEditingController(text: widget.initialRole);
    _specialty = TextEditingController(text: widget.initialSpecialty);
    _systemPrompt = TextEditingController(text: widget.initialSystemPrompt);
    _connectorId = TextEditingController(text: widget.initialConnectorId);
    _modelId = TextEditingController(text: widget.initialModelId);
    _connectorId.addListener(_onConnectorChanged);
    _modelId.addListener(_onModelChanged);
    if (_connectorId.text.isNotEmpty) {
      _loadOptions();
    }
  }

  void _onConnectorChanged() {
    _connectorLoadDebounce?.cancel();
    _loadGeneration += 1;
    if (mounted) {
      setState(() {
        _catalog = null;
        _options = null;
        _catalogLoaded = false;
        _catalogFailure = null;
        _optionsFailure = null;
        _loadingOptions = false;
      });
    }
    if (_connectorId.text.trim().isNotEmpty) {
      _connectorLoadDebounce = Timer(
        const Duration(milliseconds: 250),
        _loadOptions,
      );
    }
  }

  void _onModelChanged() {
    setState(() {});
  }

  Future<void> _loadOptions() async {
    final connectorId = _connectorId.text.trim();
    final client = widget.client;
    final sessionId = widget.sessionId;
    final catalogLoader =
        widget.connectorModelsLoader ??
        (client != null && sessionId != null
            ? (String id) => client.queryConnectorModels(
                sessionId: sessionId,
                connectorId: id,
              )
            : null);
    final optionsLoader =
        widget.identityModelOptionsLoader ??
        (client != null && sessionId != null && widget.target != null
            ? (String id) => client.queryIdentityModelOptions(
                sessionId: sessionId,
                target: widget.target!,
                connectorId: id,
              )
            : null);
    if (connectorId.isEmpty ||
        (catalogLoader == null && optionsLoader == null)) {
      if (mounted) {
        setState(() {
          _catalog = null;
          _options = null;
          _catalogLoaded = false;
          _catalogFailure = null;
          _optionsFailure = null;
          _loadingOptions = false;
        });
      }
      return;
    }
    final generation = ++_loadGeneration;
    setState(() {
      _loadingOptions = true;
      _catalogFailure = null;
      _optionsFailure = null;
    });

    ConnectorModelCatalog? catalog;
    List<IdentityModelOptionMetadata>? options;
    Object? catalogFailure;
    Object? optionsFailure;
    if (catalogLoader != null) {
      try {
        catalog = await catalogLoader(connectorId);
      } catch (error) {
        catalogFailure = error;
      }
    }
    if (optionsLoader != null) {
      try {
        options = await optionsLoader(connectorId);
      } catch (error) {
        optionsFailure = error;
      }
    }
    if (!mounted ||
        generation != _loadGeneration ||
        _connectorId.text.trim() != connectorId) {
      return;
    }
    setState(() {
      if (catalogFailure == null && catalogLoader != null) {
        _catalog = catalog;
      }
      if (optionsFailure == null && optionsLoader != null) {
        _options = options;
      }
      _catalogLoaded = catalogLoader != null && catalogFailure == null;
      _catalogFailure = catalogFailure;
      _optionsFailure = optionsFailure;
      _loadingOptions = false;
    });
  }

  Future<void> _setDefaultOption(IdentityModelOptionMetadata option) async {
    if (widget.client == null ||
        widget.sessionId == null ||
        widget.target == null) {
      return;
    }
    try {
      await widget.client!.setIdentityModelOptionDefault(
        sessionId: widget.sessionId!,
        target: widget.target!,
        connectorId: option.connectorId,
        modelId: option.modelId,
      );
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          SnackBar(
            content: Text(
              '${AppLocalizations.of(context)!.setAsDefaultModelSuccess}${option.modelId}',
            ),
          ),
        );
        _loadOptions();
      }
    } catch (error) {
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          SnackBar(
            content: Text(
              '${AppLocalizations.of(context)!.setAsDefaultModelFailed}$error',
            ),
          ),
        );
      }
    }
  }

  @override
  void dispose() {
    _connectorLoadDebounce?.cancel();
    _loadGeneration += 1;
    _name.dispose();
    _role.dispose();
    _specialty.dispose();
    _systemPrompt.dispose();
    _connectorId.removeListener(_onConnectorChanged);
    _connectorId.dispose();
    _modelId.removeListener(_onModelChanged);
    _modelId.dispose();
    super.dispose();
  }

  List<String> _getAvailableModelIds() {
    final modelIds = <String>{};
    void addAll(Iterable<String> values) {
      for (final value in values) {
        if (value.trim().isNotEmpty) modelIds.add(value);
      }
    }

    addAll(_catalog?.models ?? const <String>[]);
    addAll(_options?.map((option) => option.modelId) ?? const <String>[]);
    final connectorId = _connectorId.text.trim();
    if (connectorId.isNotEmpty &&
        widget.knownCatalogModels.containsKey(connectorId)) {
      addAll(widget.knownCatalogModels[connectorId]!);
    }
    return modelIds.toList(growable: false);
  }

  bool get _isUnverifiedModel {
    final connectorId = _connectorId.text.trim();
    final modelId = _modelId.text.trim();
    if (modelId.isEmpty) return false;
    if (_catalog?.models.contains(modelId) == true ||
        widget.knownCatalogModels[connectorId]?.contains(modelId) == true) {
      return false;
    }
    final savedOptions = _options
        ?.where((option) => option.modelId == modelId)
        .toList(growable: false);
    return savedOptions == null ||
        savedOptions.isEmpty ||
        savedOptions.first.availability == 'unverified';
  }

  String _copy({required String zh, required String en}) =>
      Localizations.localeOf(context).languageCode == 'zh' ? zh : en;

  String _catalogFailureText(Object failure) {
    final code = failure is CoreIpcException ? failure.code : null;
    return switch (code) {
      'CONNECTOR_RUNTIME_AUTHENTICATION_FAILED' => _copy(
        zh: 'Connector 需要先完成认证。认证后点击“刷新”重新获取模型；也可以手动输入模型 ID（未验证）。',
        en: 'This connector needs authentication. Authenticate, then refresh the models, or enter a model ID manually (unverified).',
      ),
      'CONNECTOR_CATALOG_UNAVAILABLE' => _copy(
        zh: 'Connector 没有返回可验证的模型目录。可以刷新重试，或手动输入模型 ID（未验证）。',
        en: 'The connector did not return a verifiable model catalog. Retry, or enter a model ID manually (unverified).',
      ),
      'CONNECTOR_RUNTIME_UNAVAILABLE' ||
      'CONNECTOR_SHARED_RUNTIME_UNAVAILABLE' ||
      'CONNECTOR_RUNTIME_IDENTITY_MISMATCH' => _copy(
        zh: 'Connector 运行时当前不可用。请确认本地运行时可访问后刷新；也可以手动输入模型 ID（未验证）。',
        en: 'The connector runtime is unavailable. Check the local runtime and refresh, or enter a model ID manually (unverified).',
      ),
      'CONNECTOR_DISABLED' => _copy(
        zh: '这个 Connector 已停用。请先在 Connector 中心启用它，再刷新模型。',
        en: 'This connector is disabled. Enable it in Connector Center, then refresh the models.',
      ),
      'CONNECTOR_NOT_FOUND' => _copy(
        zh: '没有找到这个 Connector。请检查 Connector ID，或前往 Connector 中心重新配置。',
        en: 'This connector was not found. Check its ID or configure it again in Connector Center.',
      ),
      _ => _copy(
        zh: '无法从 Connector 获取模型目录。可以刷新重试，或手动输入模型 ID（未验证）。',
        en: 'The model catalog could not be loaded. Retry, or enter a model ID manually (unverified).',
      ),
    };
  }

  String get _emptyCatalogText => _copy(
    zh: 'Connector 当前没有提供可验证的模型目录。可以手动输入模型 ID；该值会标记为未验证。',
    en: 'The connector did not provide a verifiable model catalog. You can enter a model ID manually; it will be marked unverified.',
  );

  String get _optionsFailureText => _copy(
    zh: '已保存的身份模型候选暂时无法读取；实时目录和手动输入仍可使用。',
    en: 'Saved identity model options could not be loaded. The live catalog and manual entry remain available.',
  );

  Future<void> _submit() async {
    final name = _name.text.trim();
    final role = _role.text.trim();
    final specialty = _specialty.text.trim();
    final systemPrompt = _systemPrompt.text.trim();
    final connectorId = _connectorId.text.trim();
    final modelId = _modelId.text.trim();

    if (_saving) return;
    if (name.isEmpty ||
        role.isEmpty ||
        specialty.isEmpty ||
        systemPrompt.isEmpty ||
        connectorId.isEmpty ||
        modelId.isEmpty) {
      setState(
        () => _error = AppLocalizations.of(context)!.allFieldsCannotBeEmpty,
      );
      return;
    }
    setState(() {
      _saving = true;
      _error = null;
    });
    try {
      await widget.onSubmit(
        AgentIdentityInput(
          name: name,
          role: role,
          specialty: specialty,
          systemPrompt: systemPrompt,
          connectorId: connectorId,
          modelId: modelId,
        ),
      );
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
    final l10n = AppLocalizations.of(context)!;
    final theme = Theme.of(context);
    return AlertDialog(
      title: Text(widget.title),
      content: SizedBox(
        width: 560,
        child: SingleChildScrollView(
          child: Column(
            mainAxisSize: MainAxisSize.min,
            children: [
              _field(_name, l10n.displayNameLabel, hint: l10n.displayNameHint),
              const SizedBox(height: 10),
              _field(_role, l10n.roleLabel, hint: l10n.roleHint),
              const SizedBox(height: 10),
              _field(_specialty, l10n.specialtyLabel, hint: l10n.specialtyHint),
              const SizedBox(height: 10),
              _field(_systemPrompt, l10n.systemPromptLabel, maxLines: 4),
              const SizedBox(height: 10),
              Row(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Expanded(
                    child: DropdownMenu<String>(
                      controller: _connectorId,
                      label: Text(l10n.discoveryConnectorIdLabel),
                      expandedInsets: EdgeInsets.zero,
                      dropdownMenuEntries: widget.knownCatalogModels.keys
                          .map(
                            (connectorId) => DropdownMenuEntry(
                              value: connectorId,
                              label: connectorId,
                            ),
                          )
                          .toList(growable: false),
                    ),
                  ),
                  const SizedBox(width: 10),
                  Expanded(
                    child: Column(
                      crossAxisAlignment: CrossAxisAlignment.start,
                      mainAxisSize: MainAxisSize.min,
                      children: [
                        TextField(
                          key: const ValueKey('agent-identity-model-field'),
                          controller: _modelId,
                          enabled: !_saving,
                          decoration: InputDecoration(
                            labelText: '模型 ID',
                            suffixIcon: _getAvailableModelIds().isEmpty
                                ? null
                                : PopupMenuButton<String>(
                                    key: const ValueKey(
                                      'agent-identity-model-menu',
                                    ),
                                    tooltip: _copy(
                                      zh: '选择已发现的模型',
                                      en: 'Choose a discovered model',
                                    ),
                                    icon: const Icon(Icons.arrow_drop_down),
                                    onSelected: (modelId) {
                                      _modelId.text = modelId;
                                      _modelId.selection =
                                          TextSelection.collapsed(
                                            offset: modelId.length,
                                          );
                                    },
                                    itemBuilder: (context) =>
                                        _getAvailableModelIds()
                                            .map(
                                              (modelId) =>
                                                  PopupMenuItem<String>(
                                                    value: modelId,
                                                    child: Text(modelId),
                                                  ),
                                            )
                                            .toList(growable: false),
                                  ),
                          ),
                        ),
                        if (_isUnverifiedModel)
                          Padding(
                            padding: const EdgeInsets.only(top: 4, left: 4),
                            child: Text(
                              l10n.manuallySpecifiedUnverified,
                              style: TextStyle(
                                fontSize: 12,
                                color: theme.colorScheme.tertiary,
                              ),
                            ),
                          ),
                      ],
                    ),
                  ),
                  const SizedBox(width: 10),
                  IconButton(
                    icon: const Icon(Icons.refresh),
                    tooltip: l10n.refresh,
                    onPressed: _loadingOptions ? null : _loadOptions,
                  ),
                ],
              ),
              if (_loadingOptions)
                const Padding(
                  padding: EdgeInsets.symmetric(vertical: 8),
                  child: LinearProgressIndicator(),
                ),
              if (_catalogFailure != null)
                _catalogNotice(
                  theme: theme,
                  icon: Icons.cloud_off_outlined,
                  message: _catalogFailureText(_catalogFailure!),
                  isError: true,
                  showRetry: true,
                )
              else if (_catalogLoaded && (_catalog?.models.isEmpty ?? true))
                _catalogNotice(
                  theme: theme,
                  icon: Icons.info_outline,
                  message: _emptyCatalogText,
                  showRetry: true,
                )
              else if (_catalogLoaded && _catalog != null)
                _catalogNotice(
                  theme: theme,
                  icon: Icons.check_circle_outline,
                  message: _copy(
                    zh: '已从 Connector 获取 ${_catalog!.models.length} 个模型。',
                    en: 'Loaded ${_catalog!.models.length} models from the connector.',
                  ),
                ),
              if (_optionsFailure != null)
                _catalogNotice(
                  theme: theme,
                  icon: Icons.info_outline,
                  message: _optionsFailureText,
                ),
              if (_options != null && _options!.isNotEmpty) ...[
                const SizedBox(height: 10),
                Align(
                  alignment: Alignment.centerLeft,
                  child: Text(
                    l10n.availableModelsFromCore,
                    style: theme.textTheme.titleSmall?.copyWith(
                      fontWeight: FontWeight.w700,
                    ),
                  ),
                ),
                const SizedBox(height: 4),
                ConstrainedBox(
                  constraints: const BoxConstraints(maxHeight: 190),
                  child: Material(
                    color: theme.colorScheme.surfaceContainerLowest,
                    shape: RoundedRectangleBorder(
                      borderRadius: BorderRadius.circular(8),
                      side: BorderSide(color: theme.colorScheme.outlineVariant),
                    ),
                    clipBehavior: Clip.antiAlias,
                    child: ListView.separated(
                      shrinkWrap: true,
                      itemCount: _options!.length,
                      separatorBuilder: (context, index) => Divider(
                        height: 1,
                        color: theme.colorScheme.outlineVariant,
                      ),
                      itemBuilder: (context, index) {
                        final option = _options![index];
                        return ListTile(
                          dense: true,
                          title: Text(option.modelId),
                          subtitle: Text(
                            '${l10n.sourceLabel}${option.source} | ${l10n.availabilityLabel}${option.availability}',
                          ),
                          trailing: option.isDefault
                              ? Icon(
                                  Icons.check_circle,
                                  color: theme.colorScheme.primary,
                                )
                              : TextButton(
                                  onPressed: () => _setDefaultOption(option),
                                  child: Text(l10n.setAsDefault),
                                ),
                          onTap: () {
                            _modelId.text = option.modelId;
                          },
                        );
                      },
                    ),
                  ),
                ),
              ],
              if (_error != null) ...[
                const SizedBox(height: 10),
                Align(
                  alignment: Alignment.centerLeft,
                  child: Text(
                    _error!,
                    style: TextStyle(color: theme.colorScheme.error),
                  ),
                ),
              ],
            ],
          ),
        ),
      ),
      actions: [
        TextButton(
          onPressed: _saving ? null : () => Navigator.of(context).pop(),
          child: Text(l10n.cancel),
        ),
        FilledButton(
          onPressed: _saving ? null : _submit,
          child: _saving
              ? const SizedBox(
                  width: 18,
                  height: 18,
                  child: CircularProgressIndicator(strokeWidth: 2),
                )
              : Text(widget.submitLabel ?? l10n.save),
        ),
      ],
    );
  }

  Widget _field(
    TextEditingController controller,
    String label, {
    int maxLines = 1,
    String? hint,
  }) {
    return TextField(
      controller: controller,
      enabled: !_saving,
      maxLines: maxLines,
      decoration: InputDecoration(labelText: label, hintText: hint),
    );
  }

  Widget _catalogNotice({
    required ThemeData theme,
    required IconData icon,
    required String message,
    bool isError = false,
    bool showRetry = false,
  }) {
    final colors = theme.colorScheme;
    final background = isError
        ? colors.errorContainer
        : colors.secondaryContainer;
    final foreground = isError
        ? colors.onErrorContainer
        : colors.onSecondaryContainer;
    return Padding(
      padding: const EdgeInsets.only(top: 8),
      child: Container(
        width: double.infinity,
        padding: const EdgeInsets.fromLTRB(12, 10, 8, 10),
        decoration: BoxDecoration(
          color: background,
          borderRadius: BorderRadius.circular(8),
        ),
        child: Row(
          crossAxisAlignment: CrossAxisAlignment.center,
          children: [
            Icon(icon, size: 18, color: foreground),
            const SizedBox(width: 8),
            Expanded(
              child: Text(
                message,
                style: theme.textTheme.bodySmall?.copyWith(color: foreground),
              ),
            ),
            if (showRetry) ...[
              const SizedBox(width: 8),
              TextButton(
                onPressed: _loadingOptions ? null : _loadOptions,
                style: TextButton.styleFrom(foregroundColor: foreground),
                child: Text(_copy(zh: '重试', en: 'Retry')),
              ),
            ],
          ],
        ),
      ),
    );
  }
}
