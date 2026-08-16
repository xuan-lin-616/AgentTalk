import 'dart:async';

import 'package:flutter/material.dart';

import '../gen/l10n.dart';
import '../ipc/core_ipc_client.dart';
import '../ipc/local_discovery.dart';
import 'discovery_error_text.dart';

/// The exact business intent an import plan was fetched for. A plan is only
/// confirmable while the current selection matches this binding.
class _PlanBinding {
  const _PlanBinding({
    required this.scanId,
    required this.candidateId,
    required this.projectId,
    required this.modelSelection,
  });

  final String scanId;
  final String candidateId;
  final String projectId;
  final String? modelSelection;

  bool matches({
    required String scanId,
    required String candidateId,
    required String projectId,
    required String? modelSelection,
  }) =>
      this.scanId == scanId &&
      this.candidateId == candidateId &&
      this.projectId == projectId &&
      this.modelSelection == modelSelection;
}

/// Read-only import plan + explicit confirmation + `agent.import_local`.
///
/// Model selection semantics:
/// - `connector_default` (modelSelection null) is a legal import option;
/// - a pinned model submits exactly one normalized model id;
/// - no plan, binding, or fingerprint material is ever sent back to Core.
class LocalAgentImportDialog extends StatefulWidget {
  const LocalAgentImportDialog({
    super.key,
    required this.client,
    required this.sessionId,
    required this.scanId,
    required this.candidate,
    required this.projectId,
  });

  final CoreIpcClient client;
  final String sessionId;
  final String scanId;
  final DiscoveryCandidate candidate;
  final String projectId;

  @override
  State<LocalAgentImportDialog> createState() => _LocalAgentImportDialogState();
}

class _LocalAgentImportDialogState extends State<LocalAgentImportDialog> {
  bool _connectorDefault = true;
  String? _pinnedModelId;
  ImportPlan? _plan;
  _PlanBinding? _planBinding;
  int _planGeneration = 0;
  bool _planLoading = false;
  bool _importing = false;
  String? _error;
  LocalAgentImportResult? _result;
  int _requestCounter = 0;

  @override
  void initState() {
    super.initState();
    _pinnedModelId = widget.candidate.models.isEmpty
        ? null
        : widget.candidate.models.first;
    unawaited(_loadPlan());
  }

  String _requestId(String prefix) {
    _requestCounter += 1;
    return '$prefix-${DateTime.now().microsecondsSinceEpoch}-$_requestCounter';
  }

  String? get _modelSelection {
    if (_connectorDefault) return null;
    final pinned = _pinnedModelId;
    // Fail closed: a pinned selection without a valid model id must never be
    // sent as a connector-default request. The state-machine guards below
    // prevent reaching a request in this state.
    if (pinned == null || pinned.trim().isEmpty) return null;
    return pinned.trim();
  }

  String get _scanId => widget.scanId;

  String get _candidateId => widget.candidate.candidateId;

  String get _projectId => widget.projectId;

  bool get _hasModels => widget.candidate.models.isNotEmpty;

  /// A pinned selection is only valid when it carries a normalized non-empty
  /// model id; without one the pinned state can never plan or import.
  bool get _pinnedSelectionValid =>
      !_connectorDefault &&
      _pinnedModelId != null &&
      _pinnedModelId!.trim().isNotEmpty;

  /// Confirm is only available while the currently loaded plan was fetched
  /// for exactly the current selection, the plan itself matches the request
  /// intent, and the pinned selection (when chosen) carries a valid model id.
  bool get _canConfirm {
    final plan = _plan;
    final binding = _planBinding;
    if (plan == null || binding == null) return false;
    if (!_connectorDefault && !_pinnedSelectionValid) return false;
    return binding.matches(
      scanId: _scanId,
      candidateId: _candidateId,
      projectId: _projectId,
      modelSelection: _modelSelection,
    );
  }

  Future<void> _loadPlan() async {
    if (_importing) return;
    // A pinned selection without a model id is invalid: never request a plan
    // (a request would silently look like connector-default), never confirm.
    if (!_connectorDefault && !_pinnedSelectionValid) {
      setState(() {
        _plan = null;
        _planBinding = null;
        _planLoading = false;
        _error = AppLocalizations.of(context)!.localAgentModelPinnedUnavailable;
      });
      return;
    }
    final scanId = _scanId;
    final candidateId = _candidateId;
    final projectId = _projectId;
    final modelSelection = _modelSelection;
    final generation = ++_planGeneration;
    // Any previous plan is immediately invalid: the selection changed, so the
    // old plan must never be confirmable for the new intent.
    setState(() {
      _plan = null;
      _planBinding = null;
      _planLoading = true;
      _error = null;
    });
    try {
      final plan = await widget.client.importPlan(
        sessionId: widget.sessionId,
        requestId: _requestId('import-plan'),
        scanId: scanId,
        candidateId: candidateId,
        projectId: projectId,
        modelSelection: modelSelection,
      );
      if (!mounted || generation != _planGeneration) return;
      // The client already validated the response against the request; the
      // binding is still recorded so a later selection change cannot reuse it.
      setState(() {
        _plan = plan;
        _planBinding = _PlanBinding(
          scanId: scanId,
          candidateId: candidateId,
          projectId: projectId,
          modelSelection: modelSelection,
        );
        _planLoading = false;
      });
    } on Object catch (error) {
      if (!mounted || generation != _planGeneration) return;
      setState(() {
        _plan = null;
        _planBinding = null;
        _planLoading = false;
        _error = discoveryErrorText(AppLocalizations.of(context)!, error);
      });
    }
  }

  Future<void> _confirmImport() async {
    final plan = _plan;
    final binding = _planBinding;
    if (_importing || plan == null || binding == null) return;
    // Fail closed unless the loaded plan exactly matches the request intent
    // and the pinned selection carries a valid model id.
    if (!_connectorDefault && !_pinnedSelectionValid) {
      setState(() {
        _plan = null;
        _planBinding = null;
        _error = AppLocalizations.of(context)!.localAgentModelPinnedUnavailable;
      });
      return;
    }
    if (!binding.matches(
      scanId: _scanId,
      candidateId: _candidateId,
      projectId: _projectId,
      modelSelection: _modelSelection,
    )) {
      setState(() {
        _plan = null;
        _planBinding = null;
        _error = AppLocalizations.of(context)!.localAgentErrorPlanMismatch;
      });
      return;
    }
    setState(() {
      _importing = true;
      _error = null;
    });
    try {
      final result = await widget.client.importLocal(
        sessionId: widget.sessionId,
        requestId: _requestId('import-local'),
        scanId: _scanId,
        candidateId: _candidateId,
        projectId: _projectId,
        modelSelection: _modelSelection,
      );
      if (!mounted) return;
      setState(() {
        _result = result;
      });
    } on Object catch (error) {
      if (!mounted) return;
      setState(() {
        _error = discoveryErrorText(AppLocalizations.of(context)!, error);
      });
    } finally {
      if (mounted) {
        setState(() {
          _importing = false;
        });
      }
    }
  }

  @override
  Widget build(BuildContext context) {
    final l10n = AppLocalizations.of(context)!;
    final theme = Theme.of(context);
    final busy = _planLoading || _importing;
    return AlertDialog(
      title: Text(l10n.localAgentImportDialogTitle),
      content: SizedBox(
        width: 560,
        child: SingleChildScrollView(
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.stretch,
            mainAxisSize: MainAxisSize.min,
            children: [
              Text(
                widget.candidate.displayName,
                style: theme.textTheme.titleMedium?.copyWith(
                  fontWeight: FontWeight.w700,
                ),
              ),
              const SizedBox(height: 4),
              Text(
                l10n.localAgentImportTargetProject(widget.projectId),
                style: theme.textTheme.bodySmall,
              ),
              const SizedBox(height: 14),
              Text(
                l10n.localAgentModelSelectionTitle,
                style: theme.textTheme.titleSmall,
              ),
              RadioGroup<bool>(
                groupValue: _connectorDefault,
                onChanged: (bool? value) {
                  setState(() {
                    _connectorDefault = value ?? true;
                  });
                  unawaited(_loadPlan());
                },
                child: Column(
                  crossAxisAlignment: CrossAxisAlignment.stretch,
                  children: [
                    RadioListTile<bool>(
                      key: const Key('local-agent-model-connector-default'),
                      value: true,
                      title: Text(l10n.localAgentModelConnectorDefault),
                      subtitle: Text(l10n.localAgentModelConnectorDefaultHint),
                      contentPadding: EdgeInsets.zero,
                    ),
                    RadioListTile<bool>(
                      key: const Key('local-agent-model-pinned'),
                      value: false,
                      title: Text(l10n.localAgentModelPinned),
                      // Without a model list a pinned selection cannot carry a
                      // valid model id, so the option is inert; connector
                      // default stays the only legal selection.
                      enabled: _hasModels,
                      contentPadding: EdgeInsets.zero,
                    ),
                  ],
                ),
              ),
              if (!_connectorDefault)
                Padding(
                  padding: const EdgeInsets.only(left: 16, bottom: 8),
                  child: DropdownButtonFormField<String>(
                    key: const Key('local-agent-model-pinned-dropdown'),
                    initialValue: _pinnedModelId,
                    items: [
                      for (final model in widget.candidate.models)
                        DropdownMenuItem(value: model, child: Text(model)),
                    ],
                    onChanged: busy
                        ? null
                        : (value) {
                            setState(() {
                              _pinnedModelId = value;
                            });
                            unawaited(_loadPlan());
                          },
                    decoration: InputDecoration(
                      labelText: l10n.localAgentModelPinnedLabel,
                      border: const OutlineInputBorder(),
                    ),
                  ),
                ),
              if (widget.candidate.models.isEmpty && !_connectorDefault)
                Padding(
                  padding: const EdgeInsets.only(left: 16, bottom: 8),
                  child: Text(
                    l10n.localAgentModelPinnedUnavailable,
                    style: theme.textTheme.bodySmall?.copyWith(
                      color: theme.colorScheme.error,
                    ),
                  ),
                ),
              const SizedBox(height: 14),
              _buildPlanSection(context),
              if (_error != null)
                Padding(
                  padding: const EdgeInsets.only(top: 10),
                  child: Text(
                    _error!,
                    style: theme.textTheme.bodySmall?.copyWith(
                      color: theme.colorScheme.error,
                    ),
                  ),
                ),
              if (_result != null)
                Padding(
                  padding: const EdgeInsets.only(top: 10),
                  child: _ImportReceipt(result: _result!),
                ),
            ],
          ),
        ),
      ),
      actions: [
        TextButton(
          onPressed: busy || _result != null
              ? null
              : () => Navigator.of(context).pop(),
          child: Text(l10n.cancel),
        ),
        if (_result == null)
          FilledButton.icon(
            key: const Key('local-agent-import-confirm'),
            onPressed: busy || !_canConfirm ? null : _confirmImport,
            icon: _importing
                ? const SizedBox(
                    width: 14,
                    height: 14,
                    child: CircularProgressIndicator(strokeWidth: 2),
                  )
                : const Icon(Icons.check, size: 18),
            label: Text(l10n.localAgentImportConfirm),
          )
        else
          FilledButton.icon(
            key: const Key('local-agent-import-done'),
            onPressed: () => Navigator.of(context).pop(_result),
            icon: const Icon(Icons.check_circle_outline, size: 18),
            label: Text(l10n.localAgentImportDone),
          ),
      ],
    );
  }

  Widget _buildPlanSection(BuildContext context) {
    final l10n = AppLocalizations.of(context)!;
    final theme = Theme.of(context);
    final plan = _plan;
    if (_planLoading && plan == null) {
      return Padding(
        padding: const EdgeInsets.symmetric(vertical: 12),
        child: Row(
          children: [
            const SizedBox(
              width: 16,
              height: 16,
              child: CircularProgressIndicator(strokeWidth: 2),
            ),
            const SizedBox(width: 10),
            Text(l10n.localAgentImportPlanLoading),
          ],
        ),
      );
    }
    if (plan == null) {
      return Text(
        l10n.localAgentImportPlanMissing,
        style: theme.textTheme.bodySmall?.copyWith(
          color: theme.colorScheme.onSurfaceVariant,
        ),
      );
    }
    return Container(
      key: const Key('local-agent-import-plan'),
      padding: const EdgeInsets.all(12),
      decoration: BoxDecoration(
        color: theme.colorScheme.surfaceContainerHighest,
        borderRadius: BorderRadius.circular(10),
        border: Border.all(color: theme.colorScheme.outlineVariant),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Row(
            children: [
              Text(
                l10n.localAgentImportPlanSummary,
                style: theme.textTheme.titleSmall?.copyWith(
                  fontWeight: FontWeight.w700,
                ),
              ),
              const Spacer(),
              Chip(
                label: Text(l10n.localAgentImportPlanReadOnly),
                visualDensity: VisualDensity.compact,
              ),
            ],
          ),
          const SizedBox(height: 8),
          _PlanRow(
            label: l10n.localAgentImportPlanConnector,
            value: plan.connectorDisplayName,
          ),
          _PlanRow(
            label: l10n.localAgentImportPlanAdapter,
            value: '${plan.adapterKind} · ${plan.manifestId}',
          ),
          _PlanRow(
            label: l10n.localAgentImportPlanProtocol,
            value: '${plan.protocolMajor}',
          ),
          if (plan.authRequired)
            _PlanRow(
              label: l10n.localAgentImportPlanAuth,
              value: l10n.localAgentImportPlanAuthRequired,
            ),
          if (_connectorDefault)
            _PlanRow(
              label: l10n.localAgentImportPlanModel,
              value: l10n.localAgentModelConnectorDefault,
            )
          else if (_modelSelection != null)
            _PlanRow(
              label: l10n.localAgentImportPlanModel,
              value: _modelSelection!,
            ),
          const SizedBox(height: 6),
          Text(
            '${l10n.localAgentImportPlanActions}${plan.actions.join(' · ')}',
            style: theme.textTheme.bodySmall?.copyWith(
              color: theme.colorScheme.onSurfaceVariant,
            ),
          ),
        ],
      ),
    );
  }
}

class _PlanRow extends StatelessWidget {
  const _PlanRow({required this.label, required this.value});

  final String label;
  final String value;

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);
    return Padding(
      padding: const EdgeInsets.symmetric(vertical: 2),
      child: Row(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          SizedBox(
            width: 96,
            child: Text(
              label,
              style: theme.textTheme.labelSmall?.copyWith(
                color: theme.colorScheme.onSurfaceVariant,
              ),
            ),
          ),
          Expanded(
            child: Text(
              value,
              style: theme.textTheme.bodySmall?.copyWith(
                fontWeight: FontWeight.w600,
              ),
            ),
          ),
        ],
      ),
    );
  }
}

class _ImportReceipt extends StatelessWidget {
  const _ImportReceipt({required this.result});

  final LocalAgentImportResult result;

  @override
  Widget build(BuildContext context) {
    final l10n = AppLocalizations.of(context)!;
    final theme = Theme.of(context);
    return Container(
      key: const Key('local-agent-import-success'),
      padding: const EdgeInsets.all(12),
      decoration: BoxDecoration(
        color: theme.colorScheme.secondaryContainer,
        borderRadius: BorderRadius.circular(10),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Row(
            children: [
              Icon(
                Icons.check_circle_outline,
                size: 18,
                color: theme.colorScheme.onSecondaryContainer,
              ),
              const SizedBox(width: 8),
              Text(
                result.reused
                    ? l10n.localAgentImportSuccessReused
                    : l10n.localAgentImportSuccess,
                style: theme.textTheme.titleSmall?.copyWith(
                  fontWeight: FontWeight.w700,
                ),
              ),
            ],
          ),
          const SizedBox(height: 6),
          Text(
            l10n.localAgentImportReceiptNote(
              result.agentId,
              result.connectorId,
            ),
            style: theme.textTheme.bodySmall?.copyWith(
              color: theme.colorScheme.onSecondaryContainer,
            ),
          ),
        ],
      ),
    );
  }
}
