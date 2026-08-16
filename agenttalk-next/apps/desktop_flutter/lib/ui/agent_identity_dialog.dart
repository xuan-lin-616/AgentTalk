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
  bool _loadingOptions = false;
  String? _optionsError;

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
    _loadOptions();
    setState(() {});
  }

  void _onModelChanged() {
    setState(() {});
  }

  Future<void> _loadOptions() async {
    final connectorId = _connectorId.text.trim();
    if (connectorId.isEmpty ||
        widget.client == null ||
        widget.sessionId == null ||
        widget.target == null) {
      if (mounted) {
        setState(() {
          _options = null;
          _optionsError = null;
          _loadingOptions = false;
        });
      }
      return;
    }
    setState(() {
      _loadingOptions = true;
      _optionsError = null;
    });
    try {
      final options = await widget.client!.queryIdentityModelOptions(
        sessionId: widget.sessionId!,
        target: widget.target!,
        connectorId: connectorId,
      );
      if (mounted && _connectorId.text.trim() == connectorId) {
        setState(() {
          _options = options;
          _loadingOptions = false;
        });
      }
    } catch (error) {
      if (mounted && _connectorId.text.trim() == connectorId) {
        setState(() {
          _optionsError = error.toString();
          _loadingOptions = false;
        });
      }
    }
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
    if (_options != null && _options!.isNotEmpty) {
      return _options!.map((e) => e.modelId).toList(growable: false);
    }
    final connectorId = _connectorId.text.trim();
    if (connectorId.isNotEmpty &&
        widget.knownCatalogModels.containsKey(connectorId)) {
      return widget.knownCatalogModels[connectorId]!;
    }
    return const <String>[];
  }

  bool get _isUnverifiedModel {
    final connectorId = _connectorId.text.trim();
    final modelId = _modelId.text.trim();
    if (modelId.isEmpty) return false;
    final knownForConnector = widget.knownCatalogModels[connectorId];
    if (knownForConnector == null || knownForConnector.isEmpty) {
      return false;
    }
    return !knownForConnector.contains(modelId);
  }

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
                        DropdownMenu<String>(
                          controller: _modelId,
                          label: const Text('模型 ID'),
                          expandedInsets: EdgeInsets.zero,
                          dropdownMenuEntries: _getAvailableModelIds()
                              .map(
                                (modelId) => DropdownMenuEntry(
                                  value: modelId,
                                  label: modelId,
                                ),
                              )
                              .toList(growable: false),
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
              if (_optionsError != null)
                Padding(
                  padding: const EdgeInsets.symmetric(vertical: 8),
                  child: Text(
                    '${l10n.catalogUnavailableOrLoadFailed}$_optionsError',
                    style: TextStyle(color: theme.colorScheme.error),
                  ),
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
                Container(
                  constraints: const BoxConstraints(maxHeight: 190),
                  decoration: BoxDecoration(
                    border: Border.all(color: theme.colorScheme.outlineVariant),
                    borderRadius: BorderRadius.circular(8),
                    color: theme.colorScheme.surfaceContainerLowest,
                  ),
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
}
