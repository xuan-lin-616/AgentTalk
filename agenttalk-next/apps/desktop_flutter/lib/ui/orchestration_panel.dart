import 'dart:convert';

import 'package:crypto/crypto.dart' as crypto;
import 'package:flutter/material.dart';

import '../ipc/core_ipc_client.dart';

class OrchestrationPanel extends StatefulWidget {
  const OrchestrationPanel({
    super.key,
    required this.client,
    required this.sessionId,
  });

  final CoreIpcClient? client;
  final String? sessionId;

  @override
  State<OrchestrationPanel> createState() => _OrchestrationPanelState();
}

class _OrchestrationPanelState extends State<OrchestrationPanel> {
  final _runIdController = TextEditingController();
  Map<String, dynamic>? _projection;
  List<dynamic> _auditEvents = const [];
  String? _status;
  bool _busy = false;

  @override
  void dispose() {
    _runIdController.dispose();
    super.dispose();
  }

  Future<void> _loadRun() async {
    final client = widget.client;
    final sessionId = widget.sessionId;
    final runId = _runIdController.text.trim();
    if (client == null || sessionId == null || runId.isEmpty || _busy) return;
    setState(() {
      _busy = true;
      _status = '正在读取 Core 编排投影…';
    });
    try {
      final projection = await client.queryOrchestrationRunSnapshot(
        sessionId: sessionId,
        runId: runId,
      );
      final audit = await client.queryOrchestrationAuditEvents(
        sessionId: sessionId,
        runId: runId,
        afterSequence: -1,
        limit: 100,
      );
      if (!mounted) return;
      setState(() {
        _projection = projection;
        _auditEvents = (audit['events'] as List<dynamic>? ?? const []);
        _status = '已从 Core 读取 ${_auditEvents.length} 条审计事件';
      });
    } on Object catch (error) {
      if (!mounted) return;
      setState(() {
        _status = '读取失败：$error';
        _projection = null;
        _auditEvents = const [];
      });
    } finally {
      if (mounted) setState(() => _busy = false);
    }
  }

  Future<void> _chooseRun() async {
    final value = await showDialog<String>(
      context: context,
      builder: (dialogContext) => AlertDialog(
        title: const Text('读取 Orchestration Run'),
        content: TextField(
          controller: _runIdController,
          autofocus: true,
          decoration: const InputDecoration(
            labelText: 'Run ID',
            hintText: '输入 Core 中的 orchestration runId',
          ),
          onSubmitted: (value) => Navigator.of(dialogContext).pop(value.trim()),
        ),
        actions: [
          TextButton(
            onPressed: () => Navigator.of(dialogContext).pop(),
            child: const Text('取消'),
          ),
          FilledButton(
            onPressed: () =>
                Navigator.of(dialogContext).pop(_runIdController.text.trim()),
            child: const Text('读取'),
          ),
        ],
      ),
    );
    if (!mounted || value == null) return;
    _runIdController.text = value;
    await _loadRun();
  }

  Future<void> _cancelRun() async {
    final client = widget.client;
    final sessionId = widget.sessionId;
    final runId = _runIdController.text.trim();
    if (client == null || sessionId == null || runId.isEmpty || _busy) return;
    setState(() => _busy = true);
    try {
      await client.cancelOrchestrationRun(
        sessionId: sessionId,
        runId: runId,
        reason: 'cancelled_from_orchestration_panel',
      );
      if (mounted) setState(() => _busy = false);
      await _loadRun();
    } on Object catch (error) {
      if (mounted) setState(() => _status = '取消被 Core 拒绝：$error');
    } finally {
      if (mounted) setState(() => _busy = false);
    }
  }

  Future<void> _recordMilestoneDecision(
    Map<String, dynamic> milestone,
    String decision,
  ) async {
    final client = widget.client;
    final sessionId = widget.sessionId;
    final runId = _runIdController.text.trim();
    final milestoneId = milestone['milestoneId']?.toString() ?? '';
    final briefTreeDigest = milestone['briefTreeDigest']?.toString() ?? '';
    final artifactDigest =
        milestone['presentedArtifactSetDigest']?.toString() ?? '';
    final evidenceDigest =
        milestone['acceptanceEvidenceDigest']?.toString() ?? '';
    final expectedVersion = milestone['version'];
    if (client == null ||
        sessionId == null ||
        runId.isEmpty ||
        milestoneId.isEmpty ||
        expectedVersion is! int ||
        _busy) {
      return;
    }
    final requestId =
        'receipt-$runId-$milestoneId-${DateTime.now().microsecondsSinceEpoch}';
    final receiptId =
        'receipt-$runId-$milestoneId-${DateTime.now().microsecondsSinceEpoch}';
    final semanticPayloadHash = crypto.sha256
        .convert(
          utf8.encode(
            jsonEncode(<String, dynamic>{
              'runId': runId,
              'milestoneId': milestoneId,
              'decision': decision,
              'expectedVersion': expectedVersion,
              'briefTreeDigest': briefTreeDigest,
              'presentedArtifactSetDigest': artifactDigest,
              'acceptanceEvidenceDigest': evidenceDigest,
            }),
          ),
        )
        .toString();
    setState(() {
      _busy = true;
      _status = decision == 'approve' ? '正在提交批准…' : '正在提交拒绝…';
    });
    var succeeded = false;
    try {
      await client.recordOrchestrationHumanReceipt(
        sessionId: sessionId,
        receiptId: receiptId,
        runId: runId,
        milestoneId: milestoneId,
        requestId: requestId,
        semanticPayloadHash: semanticPayloadHash,
        decision: decision,
        expectedVersion: expectedVersion,
        briefTreeDigest: briefTreeDigest,
        presentedArtifactSetDigest: artifactDigest,
        acceptanceEvidenceDigest: evidenceDigest,
      );
      succeeded = true;
    } on Object catch (error) {
      if (mounted) setState(() => _status = '审批被 Core 拒绝：$error');
    } finally {
      if (mounted) setState(() => _busy = false);
    }
    if (succeeded && mounted) await _loadRun();
  }

  Future<void> _retryNode(String nodeId) async {
    final client = widget.client;
    final sessionId = widget.sessionId;
    if (client == null || sessionId == null || nodeId.isEmpty || _busy) return;
    setState(() {
      _busy = true;
      _status = '正在请求 Core 重试节点…';
    });
    var succeeded = false;
    try {
      await client.retryOrchestrationTask(sessionId: sessionId, nodeId: nodeId);
      succeeded = true;
    } on Object catch (error) {
      if (mounted) setState(() => _status = '重试被 Core 拒绝：$error');
    } finally {
      if (mounted) setState(() => _busy = false);
    }
    if (succeeded && mounted) await _loadRun();
  }

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);
    final run = _projection?['run'];
    final nodes = _projection?['nodes'] as List<dynamic>? ?? const [];
    final attempts = _projection?['attempts'] as List<dynamic>? ?? const [];
    final milestones = _projection?['milestones'] as List<dynamic>? ?? const [];
    final deliveries = _projection?['deliveries'] as List<dynamic>? ?? const [];
    final acceptances =
        _projection?['machineAcceptances'] as List<dynamic>? ?? const [];
    return Card(
      margin: EdgeInsets.zero,
      child: Padding(
        padding: const EdgeInsets.all(12),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Row(
              children: [
                Icon(
                  Icons.account_tree_outlined,
                  color: theme.colorScheme.primary,
                ),
                const SizedBox(width: 8),
                const Expanded(
                  child: Text(
                    'Orchestration',
                    style: TextStyle(fontWeight: FontWeight.w700),
                  ),
                ),
                IconButton(
                  tooltip: '读取编排投影',
                  onPressed: _busy ? null : _loadRun,
                  icon: const Icon(Icons.refresh),
                ),
              ],
            ),
            Align(
              alignment: Alignment.centerLeft,
              child: OutlinedButton.icon(
                key: const Key('orchestration-run-picker'),
                onPressed: _busy ? null : _chooseRun,
                icon: const Icon(Icons.search),
                label: Text(
                  _runIdController.text.isEmpty
                      ? '选择 Run 查看投影'
                      : 'Run: ${_runIdController.text}',
                ),
              ),
            ),
            if (_status != null) ...[
              const SizedBox(height: 6),
              Text(_status!, style: theme.textTheme.bodySmall),
            ],
            if (run is Map<String, dynamic>) ...[
              const SizedBox(height: 8),
              Wrap(
                spacing: 6,
                runSpacing: 6,
                children: [
                  _MetricChip(label: '状态', value: '${run['status'] ?? '-'}'),
                  _MetricChip(
                    label: 'generation',
                    value: '${run['coordinatorGeneration'] ?? '-'}',
                  ),
                  _MetricChip(label: '节点', value: '${nodes.length}'),
                  _MetricChip(label: 'Attempt', value: '${attempts.length}'),
                  _MetricChip(
                    label: 'Milestone',
                    value: '${milestones.length}',
                  ),
                  _MetricChip(label: 'Delivery', value: '${deliveries.length}'),
                  _MetricChip(
                    label: 'Acceptance',
                    value: '${acceptances.length}',
                  ),
                ],
              ),
              const SizedBox(height: 8),
              if (nodes.isNotEmpty)
                Flexible(
                  child: ListView(
                    shrinkWrap: true,
                    children: [
                      for (final node in nodes)
                        ListTile(
                          dense: true,
                          contentPadding: EdgeInsets.zero,
                          leading: Icon(
                            _statusIcon(node['status']?.toString()),
                          ),
                          title: Text(
                            node['nodeKey']?.toString() ??
                                node['nodeId']?.toString() ??
                                '-',
                          ),
                          subtitle: Text(
                            '${node['nodeId'] ?? '-'} · ${node['status'] ?? '-'}',
                          ),
                          trailing:
                              node['status'] == 'failed' ||
                                  node['status'] == 'blocked'
                              ? IconButton(
                                  key: Key(
                                    'orchestration-node-retry-${node['nodeId']}',
                                  ),
                                  tooltip: '重试节点',
                                  onPressed: _busy
                                      ? null
                                      : () => _retryNode(
                                          node['nodeId']?.toString() ?? '',
                                        ),
                                  icon: const Icon(Icons.replay),
                                )
                              : null,
                        ),
                    ],
                  ),
                ),
              if (milestones.isNotEmpty) ...[
                const SizedBox(height: 8),
                for (final milestone in milestones)
                  if (milestone is Map<String, dynamic>)
                    ListTile(
                      dense: true,
                      contentPadding: EdgeInsets.zero,
                      leading: Icon(
                        milestone['status'] == 'approved'
                            ? Icons.verified_outlined
                            : milestone['status'] == 'rejected'
                            ? Icons.cancel_outlined
                            : Icons.fact_check_outlined,
                      ),
                      title: Text(
                        milestone['milestoneKey']?.toString() ??
                            milestone['milestoneId']?.toString() ??
                            '-',
                      ),
                      subtitle: Text(
                        '${milestone['milestoneId'] ?? '-'} · ${milestone['status'] ?? '-'}',
                      ),
                      trailing: milestone['status'] == 'awaiting_approval'
                          ? Wrap(
                              spacing: 4,
                              children: [
                                OutlinedButton(
                                  key: Key(
                                    'orchestration-milestone-approve-${milestone['milestoneId']}',
                                  ),
                                  onPressed: _busy
                                      ? null
                                      : () => _recordMilestoneDecision(
                                          milestone,
                                          'approve',
                                        ),
                                  child: const Text('批准'),
                                ),
                                TextButton(
                                  key: Key(
                                    'orchestration-milestone-reject-${milestone['milestoneId']}',
                                  ),
                                  onPressed: _busy
                                      ? null
                                      : () => _recordMilestoneDecision(
                                          milestone,
                                          'reject',
                                        ),
                                  child: const Text('拒绝'),
                                ),
                              ],
                            )
                          : null,
                    ),
              ],
              if (run['status'] != 'completed' && run['status'] != 'cancelled')
                Align(
                  alignment: Alignment.centerRight,
                  child: TextButton.icon(
                    onPressed: _busy ? null : _cancelRun,
                    icon: const Icon(Icons.stop_circle_outlined),
                    label: const Text('取消 Run'),
                  ),
                ),
            ] else if (_status == null) ...[
              const SizedBox(height: 12),
              Text(
                '输入 Run ID 后，Core 会返回节点、Attempt、Delivery、Acceptance、Milestone 和审计事件元数据。',
                style: theme.textTheme.bodySmall,
              ),
            ],
          ],
        ),
      ),
    );
  }
}

class _MetricChip extends StatelessWidget {
  const _MetricChip({required this.label, required this.value});

  final String label;
  final String value;

  @override
  Widget build(BuildContext context) {
    return Chip(
      label: Text('$label: $value'),
      visualDensity: VisualDensity.compact,
    );
  }
}

IconData _statusIcon(String? status) => switch (status) {
  'completed' => Icons.check_circle_outline,
  'running' => Icons.play_circle_outline,
  'sealing' => Icons.lock_clock_outlined,
  'failed' || 'cancelled' => Icons.cancel_outlined,
  'blocked' => Icons.block_outlined,
  _ => Icons.radio_button_unchecked,
};
