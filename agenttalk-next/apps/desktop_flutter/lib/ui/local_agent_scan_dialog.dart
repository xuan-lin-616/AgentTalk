import 'dart:async';

import 'package:flutter/material.dart';

import '../gen/l10n.dart';
import '../ipc/core_ipc_client.dart';
import '../ipc/local_discovery.dart';
import '../ipc/protocol_v1.dart';
import '../platform/folder_picker.dart';
import 'discovery_error_text.dart';
import 'local_agent_import_dialog.dart';

/// Classified local Agent discovery and atomic import wizard.
///
/// Workflow: start a passive discovery session -> subscribe to
/// `local-discovery-events` -> render the classified snapshot (Agent / Model
/// Runtime / Tool Server / Unknown with four independent status dimensions)
/// -> explicit consent before initialize-only verification -> read-only
/// import plan -> explicit confirmation -> `agent.import_local` -> projection
/// refresh via [onImported].
///
/// The dialog never displays absolute paths, PIDs, ports, raw sources,
/// credentials, private bindings or fingerprints, and it never invents a
/// server-side cancel: closing the dialog stops local waiting and ignores
/// late results.
class LocalAgentScanDialog extends StatefulWidget {
  const LocalAgentScanDialog({
    super.key,
    required this.client,
    required this.sessionId,
    required this.projectId,
    required this.onImported,
    this.onManualAdd,
    this.filePickerClient,
  });

  final CoreIpcClient client;
  final String sessionId;
  final String? projectId;
  final ValueChanged<LocalAgentImportResult> onImported;
  final VoidCallback? onManualAdd;
  final FilePickerClient? filePickerClient;

  @override
  State<LocalAgentScanDialog> createState() => _LocalAgentScanDialogState();
}

class _LocalAgentScanDialogState extends State<LocalAgentScanDialog> {
  DiscoverySnapshot? _snapshot;
  String? _scanId;
  bool _starting = false;
  bool _refreshing = false;
  String? _error;
  String? _notice;
  CoreEventSubscription? _subscription;
  CoreIpcClient? _subscriptionClient;
  StreamSubscription<EventEnvelope>? _subscriptionStream;
  StreamCursor? _lastDiscoveryCursor;
  String? _snapshotPollingScanId;
  bool _subscribing = false;
  String? _busyCandidateId;
  int _requestCounter = 0;

  @override
  void initState() {
    super.initState();
    unawaited(_startScan());
  }

  @override
  void dispose() {
    unawaited(_releaseSubscription());
    _subscriptionStream?.cancel();
    super.dispose();
  }

  String _requestId(String prefix) {
    _requestCounter += 1;
    return '$prefix-${DateTime.now().microsecondsSinceEpoch}-$_requestCounter';
  }

  Future<void> _releaseSubscription() async {
    final subscription = _subscription;
    _subscription = null;
    final subscriptionClient = _subscriptionClient;
    _subscriptionClient = null;
    final stream = _subscriptionStream;
    _subscriptionStream = null;
    await stream?.cancel();
    if (subscription != null && subscription.isActive) {
      try {
        await subscription.unsubscribe();
      } on Object {
        // Best-effort release on dialog close; never surfaces to the UI.
      }
    }
    // A dedicated subscription connection (real Core) is closed here; the
    // mock transport shares the main client and must not be closed.
    if (subscriptionClient != null &&
        !identical(subscriptionClient, widget.client)) {
      try {
        await subscriptionClient.close();
      } on Object {
        // Best-effort release on dialog close; never surfaces to the UI.
      }
    }
  }

  Future<void> _startScan({String? explicitExecutablePath}) async {
    final client = widget.client;
    final sessionId = widget.sessionId;
    CoreIpcClient? pendingSubscriptionClient;
    if (mounted) {
      setState(() {
        _starting = true;
        _error = null;
        _notice = null;
      });
    }
    try {
      await _releaseSubscription();
      try {
        // Complete the dedicated connection handshake before production
        // discovery work starts. A real PATH/registry scan can outlive the
        // Named Pipe client's bounded read window; opening the connection
        // after start would then misreport a healthy stream as unavailable.
        pendingSubscriptionClient = await client.openSubscription(
          sessionId: sessionId,
        );
      } on Object catch (error) {
        if (mounted) {
          setState(() {
            _notice = _renderNoticeForEventError(error);
          });
        }
      }
      final start = await client.discoveryStart(
        sessionId: sessionId,
        requestId: _requestId('discovery-start'),
        explicitExecutablePath: explicitExecutablePath,
      );
      if (!mounted) {
        await _closeDedicatedSubscriptionClient(pendingSubscriptionClient);
        return;
      }
      setState(() {
        _scanId = start.scanId;
        _snapshot = null;
      });
      final subscriptionClient = pendingSubscriptionClient;
      pendingSubscriptionClient = null;
      unawaited(
        _subscribeAndRefresh(
          start.eventEpoch,
          subscriptionClient: subscriptionClient,
        ),
      );
    } on Object catch (error) {
      await _closeDedicatedSubscriptionClient(pendingSubscriptionClient);
      if (!mounted) return;
      setState(() {
        _error = _renderError(error);
      });
    } finally {
      if (mounted) {
        setState(() {
          _starting = false;
        });
      }
    }
  }

  Future<void> _pickExecutableAndScan() async {
    if (_starting || _busyCandidateId != null) return;
    final result = await (widget.filePickerClient ?? createFilePickerClient())
        .pickFile();
    if (!mounted || result.status == FilePickerStatus.cancelled) return;
    if (!result.hasSelection) {
      setState(() {
        _error = result.message ?? '文件选择失败；未使用默认路径。';
      });
      return;
    }
    await _startScan(explicitExecutablePath: result.path);
  }

  Future<void> _subscribeAndRefresh(
    String epoch, {
    required CoreIpcClient? subscriptionClient,
  }) async {
    final sessionId = widget.sessionId;
    var subscriptionEstablished = false;
    if (mounted && !_subscribing && subscriptionClient != null) {
      _subscribing = true;
      try {
        // The real Core binds a subscribed connection to events.ack /
        // events.unsubscribe only, so the discovery subscription lives on a
        // dedicated connection; commands and queries stay on the main client.
        // The mock transport reuses the main client, keeping tests unchanged.
        final previousCursor = _lastDiscoveryCursor;
        final afterSequence =
            previousCursor?.streamId == localDiscoveryEventStreamId &&
                previousCursor?.epoch == epoch
            ? previousCursor!.sequence
            : 0;
        final subscription = await subscriptionClient.subscribeDiscoveryEvents(
          sessionId: sessionId,
          epoch: epoch,
          afterSequence: afterSequence,
        );
        if (!mounted) {
          await subscription.unsubscribe().catchError(
            (Object _) => <String, dynamic>{},
          );
          await _closeDedicatedSubscriptionClient(subscriptionClient);
          return;
        }
        setState(() {
          _subscription = subscription;
          _subscriptionClient = subscriptionClient;
        });
        _subscriptionStream = subscription.events.listen(
          (event) => unawaited(_onDiscoveryEvent(event, subscription)),
          onError: (Object error) {
            // A subscription failure (for example a replay gap) degrades to
            // snapshot-driven refresh; the snapshot stays authoritative.
            if (!mounted) return;
            setState(() {
              _notice = _renderNoticeForEventError(error);
            });
            final scanId = _scanId;
            if (scanId != null) {
              unawaited(_pollSnapshotUntilTerminal(scanId));
            }
          },
        );
        subscriptionEstablished = true;
      } on Object catch (error) {
        await _closeDedicatedSubscriptionClient(subscriptionClient);
        if (!mounted) return;
        setState(() {
          _notice = _renderNoticeForEventError(error);
        });
      } finally {
        _subscribing = false;
      }
    }
    final scanId = _scanId;
    if (!subscriptionEstablished && scanId != null) {
      await _pollSnapshotUntilTerminal(scanId);
    } else {
      await _refreshSnapshot();
    }
  }

  Future<void> _closeDedicatedSubscriptionClient(CoreIpcClient? client) async {
    if (client == null || identical(client, widget.client)) return;
    await client.close().catchError((Object _) {});
  }

  Future<void> _pollSnapshotUntilTerminal(String scanId) async {
    if (_snapshotPollingScanId == scanId) return;
    _snapshotPollingScanId = scanId;
    try {
      for (var attempt = 0; attempt < 40; attempt++) {
        if (!mounted || _scanId != scanId) return;
        await _refreshSnapshot();
        if (!mounted || _scanId != scanId) return;
        final snapshot = _snapshot;
        if (snapshot?.scanId == scanId && snapshot?.state != 'running') return;
        if (snapshot == null && _error != null) return;
        await Future<void>.delayed(const Duration(milliseconds: 500));
      }
    } finally {
      if (_snapshotPollingScanId == scanId) {
        _snapshotPollingScanId = null;
      }
    }
  }

  Future<void> _onDiscoveryEvent(
    EventEnvelope envelope,
    CoreEventSubscription subscription,
  ) async {
    if (!mounted) return;
    DiscoveryEventSummary summary;
    try {
      summary = DiscoveryEventSummary.fromEnvelope(envelope);
    } on Object {
      return;
    }
    if (summary.type == 'agent.discovery.candidate_verified' ||
        summary.type == 'agent.discovery.candidate_observed' ||
        summary.type == 'agent.discovery.candidate_classified' ||
        summary.type == 'agent.discovery.completed' ||
        summary.type == 'agent.discovery.failed') {
      // Acknowledge the processed cursor before refreshing so the retained
      // discovery stream stays compact.
      try {
        await subscription.ack(envelope.cursor);
        final previous = _lastDiscoveryCursor;
        if (previous == null ||
            previous.streamId != envelope.cursor.streamId ||
            previous.epoch != envelope.cursor.epoch ||
            envelope.cursor.sequence > previous.sequence) {
          _lastDiscoveryCursor = envelope.cursor;
        }
      } on Object {
        // Snapshot refresh remains authoritative when ACK fails.
      }
      await _refreshSnapshot();
    }
  }

  Future<void> _refreshSnapshot() async {
    final scanId = _scanId;
    if (scanId == null || _refreshing) return;
    final client = widget.client;
    final sessionId = widget.sessionId;
    _refreshing = true;
    try {
      final snapshot = await client.discoverySnapshot(
        sessionId: sessionId,
        requestId: _requestId('discovery-snapshot'),
        scanId: scanId,
      );
      if (!mounted) return;
      setState(() {
        _snapshot = snapshot;
        _error = null;
      });
    } on Object catch (error) {
      if (!mounted) return;
      setState(() {
        if (_isMissingScanError(error)) {
          _snapshot = null;
        }
        _error = _renderError(error);
      });
    } finally {
      _refreshing = false;
    }
  }

  Future<void> _verifyCandidate(DiscoveryCandidate candidate) async {
    final client = widget.client;
    final sessionId = widget.sessionId;
    final scanId = _scanId;
    if (scanId == null || _busyCandidateId != null) return;
    final consent = await showDialog<bool>(
      context: context,
      builder: (context) => AlertDialog(
        title: Text(AppLocalizations.of(context)!.localAgentVerifyConsentTitle),
        content: Text(
          AppLocalizations.of(context)!.localAgentVerifyConsentBody,
        ),
        actions: [
          TextButton(
            key: const Key('local-agent-verify-consent-cancel'),
            onPressed: () => Navigator.of(context).pop(false),
            child: Text(AppLocalizations.of(context)!.cancel),
          ),
          FilledButton(
            key: const Key('local-agent-verify-consent-agree'),
            onPressed: () => Navigator.of(context).pop(true),
            child: Text(
              AppLocalizations.of(context)!.localAgentVerifyConsentAgree,
            ),
          ),
        ],
      ),
    );
    if (consent != true || !mounted) return;
    setState(() {
      _busyCandidateId = candidate.candidateId;
      _error = null;
    });
    try {
      await client.discoveryVerify(
        sessionId: sessionId,
        requestId: _requestId('discovery-verify'),
        scanId: scanId,
        candidateId: candidate.candidateId,
        consent: true,
      );
      if (!mounted) return;
      await _refreshSnapshot();
    } on Object catch (error) {
      if (!mounted) return;
      setState(() {
        _error = _renderError(error);
      });
    } finally {
      if (mounted) {
        setState(() {
          _busyCandidateId = null;
        });
      }
    }
  }

  Future<void> _dismissCandidate(DiscoveryCandidate candidate) async {
    final client = widget.client;
    final sessionId = widget.sessionId;
    final scanId = _scanId;
    if (scanId == null || _busyCandidateId != null) return;
    setState(() {
      _busyCandidateId = candidate.candidateId;
      _error = null;
    });
    try {
      await client.discoveryDismiss(
        sessionId: sessionId,
        requestId: _requestId('discovery-dismiss'),
        scanId: scanId,
        candidateId: candidate.candidateId,
      );
      if (!mounted) return;
      await _refreshSnapshot();
    } on Object catch (error) {
      if (!mounted) return;
      setState(() {
        _error = _renderError(error);
      });
    } finally {
      if (mounted) {
        setState(() {
          _busyCandidateId = null;
        });
      }
    }
  }

  Future<void> _openImport(DiscoveryCandidate candidate) async {
    final projectId = widget.projectId;
    final scanId = _scanId;
    if (projectId == null || projectId.isEmpty) {
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          SnackBar(
            content: Text(
              AppLocalizations.of(context)!.localAgentProjectRequired,
            ),
          ),
        );
      }
      return;
    }
    if (scanId == null || _busyCandidateId != null) return;
    final imported = await showDialog<LocalAgentImportResult>(
      context: context,
      builder: (context) => LocalAgentImportDialog(
        client: widget.client,
        sessionId: widget.sessionId,
        scanId: scanId,
        candidate: candidate,
        projectId: projectId,
      ),
    );
    if (imported != null && mounted) {
      widget.onImported(imported);
      if (imported.reused) {
        setState(() {
          _notice = AppLocalizations.of(context)!.localAgentImportReusedNotice;
        });
      }
      await _refreshSnapshot();
    }
  }

  String _renderError(Object error) {
    return discoveryErrorText(AppLocalizations.of(context)!, error);
  }

  bool _isMissingScanError(Object error) =>
      error is CoreIpcException &&
      (error.code == 'DISCOVERY_SCAN_NOT_FOUND' ||
          error.code == 'DISCOVERY_SCAN_EXPIRED');

  String _renderNoticeForEventError(Object error) {
    if (error is CoreIpcException && error.isReplayGap) {
      return AppLocalizations.of(context)!.localAgentEventReplayGapNotice;
    }
    return AppLocalizations.of(context)!.localAgentEventStreamNotice;
  }

  @override
  Widget build(BuildContext context) {
    final l10n = AppLocalizations.of(context)!;
    final theme = Theme.of(context);
    final busy = _busyCandidateId != null || _starting;
    final hasProject = widget.projectId?.isNotEmpty == true;
    return AlertDialog(
      title: Row(
        children: [
          const Icon(Icons.radar_outlined),
          const SizedBox(width: 8),
          Expanded(child: Text(l10n.localAgentScanDialogTitle)),
        ],
      ),
      content: SizedBox(
        width: 960,
        height: 660,
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.stretch,
          children: [
            Text(
              l10n.localAgentScanDialogDescription,
              style: theme.textTheme.bodySmall,
            ),
            if (!hasProject) ...[
              const SizedBox(height: 8),
              Container(
                key: const Key('local-agent-project-required'),
                padding: const EdgeInsets.symmetric(
                  horizontal: 12,
                  vertical: 10,
                ),
                decoration: BoxDecoration(
                  color: theme.colorScheme.secondaryContainer,
                  borderRadius: BorderRadius.circular(12),
                ),
                child: Row(
                  children: [
                    Icon(
                      Icons.info_outline,
                      size: 18,
                      color: theme.colorScheme.onSecondaryContainer,
                    ),
                    const SizedBox(width: 8),
                    Expanded(
                      child: Text(
                        l10n.localAgentProjectRequired,
                        style: theme.textTheme.bodySmall?.copyWith(
                          color: theme.colorScheme.onSecondaryContainer,
                        ),
                      ),
                    ),
                  ],
                ),
              ),
            ],
            const SizedBox(height: 12),
            Row(
              children: [
                FilledButton.icon(
                  onPressed: busy ? null : () => unawaited(_startScan()),
                  icon: _starting
                      ? const SizedBox(
                          width: 16,
                          height: 16,
                          child: CircularProgressIndicator(strokeWidth: 2),
                        )
                      : const Icon(Icons.radar_outlined, size: 18),
                  label: Text(l10n.localAgentRescan),
                ),
                const SizedBox(width: 8),
                OutlinedButton.icon(
                  onPressed: busy ? null : () => unawaited(_refreshSnapshot()),
                  icon: _refreshing
                      ? const SizedBox(
                          width: 16,
                          height: 16,
                          child: CircularProgressIndicator(strokeWidth: 2),
                        )
                      : const Icon(Icons.refresh, size: 18),
                  label: Text(l10n.refresh),
                ),
                const SizedBox(width: 8),
                OutlinedButton.icon(
                  key: const Key('local-agent-select-executable'),
                  onPressed: busy
                      ? null
                      : () => unawaited(_pickExecutableAndScan()),
                  icon: const Icon(Icons.folder_open_outlined, size: 18),
                  label: Text(l10n.localAgentSelectExecutable),
                ),
                if (widget.onManualAdd != null) ...[
                  const SizedBox(width: 8),
                  OutlinedButton.icon(
                    onPressed: busy ? null : widget.onManualAdd,
                    icon: const Icon(Icons.add, size: 18),
                    label: Text(l10n.localAgentManualAdd),
                  ),
                ],
              ],
            ),
            const SizedBox(height: 8),
            if (_notice != null)
              Padding(
                padding: const EdgeInsets.only(bottom: 8),
                child: Text(
                  _notice!,
                  style: theme.textTheme.bodySmall?.copyWith(
                    color: theme.colorScheme.tertiary,
                  ),
                ),
              ),
            if (_error != null)
              Padding(
                padding: const EdgeInsets.only(bottom: 8),
                child: Text(
                  _error!,
                  style: theme.textTheme.bodySmall?.copyWith(
                    color: theme.colorScheme.error,
                  ),
                ),
              ),
            Expanded(child: _buildContent(context)),
          ],
        ),
      ),
      actions: [
        TextButton(
          onPressed: () => Navigator.of(context).pop(),
          child: Text(l10n.cancel),
        ),
      ],
    );
  }

  Widget _buildContent(BuildContext context) {
    final l10n = AppLocalizations.of(context)!;
    if (_starting) {
      return Center(
        child: Column(
          mainAxisSize: MainAxisSize.min,
          children: [
            const CircularProgressIndicator(),
            const SizedBox(height: 12),
            Text(l10n.localAgentScanning),
          ],
        ),
      );
    }
    final snapshot = _snapshot;
    if (snapshot == null) {
      return Center(
        child: _error == null
            ? const CircularProgressIndicator()
            : Text(_error!, textAlign: TextAlign.center),
      );
    }
    if (snapshot.state == 'running') {
      return Center(
        child: Column(
          mainAxisSize: MainAxisSize.min,
          children: [
            const CircularProgressIndicator(),
            const SizedBox(height: 12),
            Text(l10n.localAgentScanning),
          ],
        ),
      );
    }
    if (snapshot.candidates.isEmpty) {
      return Center(
        child: Column(
          mainAxisSize: MainAxisSize.min,
          children: [
            Icon(
              Icons.smart_toy_outlined,
              size: 44,
              color: Theme.of(context).colorScheme.onSurfaceVariant,
            ),
            const SizedBox(height: 12),
            Text(l10n.localAgentNoCandidates, textAlign: TextAlign.center),
          ],
        ),
      );
    }
    final groups = _groupCandidates(snapshot);
    // Non-lazy so every group section exists in the tree (accessible and
    // findable regardless of scroll position); the dialog content is bounded.
    return SingleChildScrollView(
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.stretch,
        children: [
          for (final group in _orderedGroups)
            if (group == 'unknown' && groups[group]!.isNotEmpty)
              Padding(
                padding: const EdgeInsets.only(top: 10, bottom: 12),
                child: Card(
                  margin: EdgeInsets.zero,
                  clipBehavior: Clip.antiAlias,
                  child: ExpansionTile(
                    key: const Key('local-agent-unknown-expansion'),
                    initiallyExpanded: false,
                    title: Text(
                      '${_groupTitle(context, group)} (${groups[group]!.length})',
                      key: Key('discovery-group-$group'),
                      style: Theme.of(context).textTheme.titleSmall?.copyWith(
                        fontWeight: FontWeight.w700,
                      ),
                    ),
                    subtitle: Text(l10n.localAgentUnknownNeedsAdapter),
                    childrenPadding: const EdgeInsets.fromLTRB(16, 0, 16, 4),
                    children: [
                      for (final entry in groups[group]!)
                        _buildCandidateEntry(context, entry),
                    ],
                  ),
                ),
              )
            else ...[
              Padding(
                padding: const EdgeInsets.only(top: 10, bottom: 6),
                child: Text(
                  _groupTitle(context, group),
                  key: Key('discovery-group-$group'),
                  style: Theme.of(
                    context,
                  ).textTheme.titleSmall?.copyWith(fontWeight: FontWeight.w700),
                ),
              ),
              if (groups[group]?.isEmpty ?? true)
                Padding(
                  padding: const EdgeInsets.only(bottom: 12),
                  child: Text(
                    l10n.localAgentGroupEmpty,
                    style: Theme.of(context).textTheme.bodySmall?.copyWith(
                      color: Theme.of(context).colorScheme.onSurfaceVariant,
                    ),
                  ),
                )
              else
                for (final entry in groups[group]!)
                  _buildCandidateEntry(context, entry),
            ],
        ],
      ),
    );
  }

  Widget _buildCandidateEntry(BuildContext context, SnapshotCandidate entry) {
    final candidate = entry.candidate;
    final hasProject = widget.projectId?.isNotEmpty == true;
    final offersImport =
        candidate.isAgent &&
        (entry.verification?.status == 'verified' ||
            entry.verification?.status == 'auth_required');
    return Padding(
      padding: const EdgeInsets.only(bottom: 12),
      child: _DiscoveryCandidateCard(
        candidate: candidate,
        lifecycleState: entry.lifecycleState,
        verification: entry.verification,
        busy: _busyCandidateId == candidate.candidateId,
        onVerify:
            candidate.isAgent &&
                (candidate.hasUserSelectedEvidence ||
                    candidate.hasPackageBoundAdapterEvidence ||
                    candidate.hasBuiltInConnectorAdapterEvidence) &&
                entry.lifecycleState == 'identified'
            ? () => unawaited(_verifyCandidate(candidate))
            : null,
        showImport: offersImport,
        importBlockedReason: offersImport && !hasProject
            ? AppLocalizations.of(context)!.localAgentProjectRequired
            : null,
        onImport: offersImport && hasProject
            ? () => unawaited(_openImport(candidate))
            : null,
        onDismiss:
            candidate.isAgent && entry.lifecycleState != 'identity_changed'
            ? () => unawaited(_dismissCandidate(candidate))
            : null,
      ),
    );
  }

  static const List<String> _orderedGroups = [
    'agent_runtime',
    'model_runtime',
    'tool_service',
    'unknown',
  ];

  Map<String, List<SnapshotCandidate>> _groupCandidates(DiscoverySnapshot s) {
    final groups = <String, List<SnapshotCandidate>>{
      for (final group in _orderedGroups) group: <SnapshotCandidate>[],
    };
    for (final entry in s.candidates) {
      if (entry.candidate.isUnknown) {
        groups['unknown']!.add(entry);
      } else if (groups[entry.candidate.category] != null) {
        groups[entry.candidate.category]!.add(entry);
      } else {
        groups['unknown']!.add(entry);
      }
    }
    return groups;
  }

  String _groupTitle(BuildContext context, String group) {
    final l10n = AppLocalizations.of(context)!;
    switch (group) {
      case 'agent_runtime':
        return l10n.localAgentCategoryAgent;
      case 'model_runtime':
        return l10n.localAgentCategoryModelRuntime;
      case 'tool_service':
        return l10n.localAgentCategoryToolServer;
      default:
        return l10n.localAgentCategoryUnknown;
    }
  }
}

class _DiscoveryCandidateCard extends StatelessWidget {
  const _DiscoveryCandidateCard({
    required this.candidate,
    required this.lifecycleState,
    required this.verification,
    required this.busy,
    required this.showImport,
    required this.importBlockedReason,
    this.onVerify,
    this.onImport,
    this.onDismiss,
  });

  final DiscoveryCandidate candidate;
  final String lifecycleState;
  final CandidateVerification? verification;
  final bool busy;
  final bool showImport;
  final String? importBlockedReason;
  final VoidCallback? onVerify;
  final VoidCallback? onImport;
  final VoidCallback? onDismiss;

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);
    final l10n = AppLocalizations.of(context)!;
    final importButton = FilledButton.icon(
      key: Key('local-agent-import-${candidate.candidateId}'),
      onPressed: busy ? null : onImport,
      icon: const Icon(Icons.add_task_outlined, size: 18),
      label: Text(l10n.localAgentImport),
    );
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
                        candidate.displayName,
                        style: theme.textTheme.titleSmall?.copyWith(
                          fontWeight: FontWeight.w700,
                        ),
                      ),
                      const SizedBox(height: 2),
                      Text(
                        _lifecycleText(l10n, lifecycleState, candidate),
                        style: theme.textTheme.bodySmall,
                      ),
                    ],
                  ),
                ),
                Chip(
                  label: Text(_categoryText(l10n, candidate.category)),
                  side: BorderSide(color: theme.colorScheme.outlineVariant),
                  backgroundColor: theme.colorScheme.surfaceContainerHighest,
                ),
              ],
            ),
            const SizedBox(height: 10),
            Wrap(
              spacing: 8,
              runSpacing: 8,
              children: [
                _StatusChip(
                  label: l10n.localAgentStatusDiscovery,
                  value: _discoveryText(l10n, candidate.discoveryState),
                ),
                _StatusChip(
                  label: l10n.localAgentStatusCompatibility,
                  value: _compatibilityText(
                    l10n,
                    candidate.compatibilityState,
                    verification,
                  ),
                ),
                _StatusChip(
                  label: l10n.localAgentStatusAuth,
                  value: _authText(l10n, candidate.authState, verification),
                ),
                _StatusChip(
                  label: l10n.localAgentStatusHealth,
                  value: _healthText(l10n, candidate.healthState),
                ),
              ],
            ),
            if (candidate.evidenceSummary.isNotEmpty) ...[
              const SizedBox(height: 10),
              Wrap(
                spacing: 6,
                runSpacing: 6,
                children: [
                  for (final evidence in candidate.evidenceSummary)
                    Chip(
                      visualDensity: VisualDensity.compact,
                      label: Text(_evidenceText(l10n, evidence)),
                      side: BorderSide(color: theme.colorScheme.outlineVariant),
                      backgroundColor: theme.colorScheme.surfaceContainerHigh,
                    ),
                ],
              ),
            ],
            if (candidate.isUnknown) ...[
              const SizedBox(height: 10),
              Text(
                l10n.localAgentUnknownNeedsAdapter,
                style: theme.textTheme.bodySmall?.copyWith(
                  color: theme.colorScheme.onSurfaceVariant,
                ),
              ),
            ],
            if (!candidate.isUnknown &&
                candidate.compatibilityState == 'adapter_required') ...[
              const SizedBox(height: 10),
              Text(
                l10n.localAgentUnknownNeedsAdapter,
                style: theme.textTheme.bodySmall?.copyWith(
                  color: theme.colorScheme.onSurfaceVariant,
                ),
              ),
            ],
            if (candidate.category == 'model_runtime') ...[
              const SizedBox(height: 10),
              Text(
                l10n.localAgentModelRuntimeNote,
                style: theme.textTheme.bodySmall?.copyWith(
                  color: theme.colorScheme.onSurfaceVariant,
                ),
              ),
            ],
            if (candidate.category == 'tool_service') ...[
              const SizedBox(height: 10),
              Text(
                l10n.localAgentToolServerNote,
                style: theme.textTheme.bodySmall?.copyWith(
                  color: theme.colorScheme.onSurfaceVariant,
                ),
              ),
            ],
            const SizedBox(height: 12),
            Row(
              mainAxisAlignment: MainAxisAlignment.end,
              children: [
                if (onVerify != null)
                  FilledButton.tonalIcon(
                    key: Key('local-agent-verify-${candidate.candidateId}'),
                    onPressed: busy ? null : onVerify,
                    icon: busy
                        ? const SizedBox(
                            width: 14,
                            height: 14,
                            child: CircularProgressIndicator(strokeWidth: 2),
                          )
                        : const Icon(Icons.verified_outlined, size: 18),
                    label: Text(l10n.localAgentVerify),
                  ),
                if (showImport) ...[
                  const SizedBox(width: 8),
                  if (importBlockedReason != null)
                    Tooltip(message: importBlockedReason!, child: importButton)
                  else
                    importButton,
                ],
                if (onDismiss != null) ...[
                  const SizedBox(width: 8),
                  OutlinedButton.icon(
                    key: Key('local-agent-dismiss-${candidate.candidateId}'),
                    onPressed: busy ? null : onDismiss,
                    icon: const Icon(Icons.visibility_off_outlined, size: 18),
                    label: Text(l10n.localAgentDismiss),
                  ),
                ],
              ],
            ),
          ],
        ),
      ),
    );
  }
}

class _StatusChip extends StatelessWidget {
  const _StatusChip({required this.label, required this.value});

  final String label;
  final String value;

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);
    return Container(
      constraints: const BoxConstraints(minWidth: 150),
      padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 8),
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
          const SizedBox(height: 2),
          Text(
            value,
            style: theme.textTheme.bodySmall?.copyWith(
              fontWeight: FontWeight.w600,
            ),
          ),
        ],
      ),
    );
  }
}

String _categoryText(AppLocalizations l10n, String category) {
  switch (category) {
    case 'agent_runtime':
      return l10n.localAgentCategoryAgent;
    case 'model_runtime':
      return l10n.localAgentCategoryModelRuntime;
    case 'tool_service':
      return l10n.localAgentCategoryToolServer;
    default:
      return l10n.localAgentCategoryUnknown;
  }
}

String _discoveryText(AppLocalizations l10n, String state) {
  switch (state) {
    case 'observed':
      return l10n.localAgentDiscoveryObserved;
    case 'identified':
      return l10n.localAgentDiscoveryIdentified;
    case 'disappeared':
      return l10n.localAgentDiscoveryDisappeared;
    default:
      return state;
  }
}

String _compatibilityText(
  AppLocalizations l10n,
  String state,
  CandidateVerification? verification,
) {
  final status = verification?.status;
  if (status == 'verified' || status == 'auth_required') {
    return l10n.localAgentCompatibilityCompatible;
  }
  switch (state) {
    case 'compatible':
      return l10n.localAgentCompatibilityCompatible;
    case 'incompatible':
      return l10n.localAgentCompatibilityIncompatible;
    case 'adapter_required':
      return l10n.localAgentCompatibilityAdapterRequired;
    default:
      return l10n.localAgentCompatibilityNotVerified;
  }
}

String _authText(
  AppLocalizations l10n,
  String state,
  CandidateVerification? verification,
) {
  final status = verification?.status;
  if (status == 'auth_required') {
    return l10n.localAgentAuthRequired;
  }
  if (status == 'verified') {
    return l10n.localAgentAuthNotRequired;
  }
  switch (state) {
    case 'required':
      return l10n.localAgentAuthRequired;
    case 'ready':
      return l10n.localAgentAuthReady;
    case 'not_required':
      return l10n.localAgentAuthNotRequired;
    default:
      return l10n.localAgentAuthUnknown;
  }
}

String _healthText(AppLocalizations l10n, String state) {
  switch (state) {
    case 'ready':
      return l10n.localAgentHealthReady;
    case 'unavailable':
      return l10n.localAgentHealthUnavailable;
    case 'identity_mismatch':
      return l10n.localAgentHealthIdentityMismatch;
    default:
      return l10n.localAgentHealthNotChecked;
  }
}

String _lifecycleText(
  AppLocalizations l10n,
  String lifecycleState,
  DiscoveryCandidate candidate,
) {
  if (candidate.isUnknown) {
    return l10n.localAgentUnknownNeedsAdapter;
  }
  switch (lifecycleState) {
    case 'verified':
      return l10n.localAgentLifecycleVerified;
    case 'auth_required':
      return l10n.localAgentLifecycleAuthRequired;
    case 'verifying':
      return l10n.localAgentLifecycleVerifying;
    case 'identity_changed':
      return l10n.localAgentLifecycleIdentityChanged;
    case 'timeout':
    case 'cancelled':
    case 'not_verified':
      return l10n.localAgentLifecycleNotVerified;
    case 'identified':
      return l10n.localAgentLifecycleIdentified;
    case 'observed':
      return l10n.localAgentLifecycleObserved;
    default:
      return l10n.localAgentLifecycleNotVerified;
  }
}

String _evidenceText(AppLocalizations l10n, String evidence) {
  switch (evidence) {
    case 'executable_inventory':
      return l10n.localAgentEvidenceExecutableInventory;
    case 'windows_path_entry':
      return l10n.localAgentEvidenceWindowsPath;
    case 'windows_app_path_registry':
      return l10n.localAgentEvidenceAppPaths;
    case 'windows_package_inventory':
      return l10n.localAgentEvidencePackage;
    case 'loopback_listener':
      return l10n.localAgentEvidenceLoopback;
    case 'user_selected':
      return l10n.localAgentEvidenceUserSelected;
    case 'runtime_record':
      return l10n.localAgentEvidenceRuntimeRecord;
    case 'version_matched':
      return l10n.localAgentEvidenceVersionMatched;
    case 'build_matched':
      return l10n.localAgentEvidenceBuildMatched;
    case 'install_known':
      return l10n.localAgentEvidenceInstallKnown;
    case 'available':
      return l10n.localAgentEvidenceAvailable;
    case 'authentication_required':
      return l10n.localAgentEvidenceAuthRequired;
    case 'unconfigured':
      return l10n.localAgentEvidenceUnconfigured;
    case 'identity_mismatch':
      return l10n.localAgentEvidenceIdentityMismatch;
    case 'adapter_manifest':
      return l10n.localAgentEvidenceInstallKnown;
    case 'package_identity_matched':
      return l10n.localAgentEvidenceInstallKnown;
    case 'catalog_unavailable':
      return l10n.localAgentEvidenceCatalogUnavailable;
    default:
      return evidence;
  }
}
