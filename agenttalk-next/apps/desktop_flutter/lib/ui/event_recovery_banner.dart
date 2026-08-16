import 'package:agenttalk_desktop/ipc/core_ipc_client.dart';
import 'package:flutter/material.dart';

class EventRecoveryBanner extends StatelessWidget {
  const EventRecoveryBanner({
    super.key,
    required this.details,
    required this.busy,
    required this.onRefreshAndSubscribe,
    required this.onRefreshAndPoll,
    this.errorMessage,
  });

  final ReplayGapDetails? details;
  final bool busy;
  final VoidCallback onRefreshAndSubscribe;
  final VoidCallback onRefreshAndPoll;
  final String? errorMessage;

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);
    final resumeCursor = details?.resumeCursor;
    final epoch = details?.epoch ?? resumeCursor?.epoch;
    return Material(
      color: theme.colorScheme.errorContainer,
      child: Padding(
        padding: const EdgeInsets.fromLTRB(20, 12, 20, 12),
        child: Row(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Icon(Icons.sync_problem, color: theme.colorScheme.onErrorContainer),
            const SizedBox(width: 12),
            Expanded(
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Text(
                    '事件恢复暂停：需要刷新快照',
                    style: theme.textTheme.titleSmall?.copyWith(
                      color: theme.colorScheme.onErrorContainer,
                      fontWeight: FontWeight.w700,
                    ),
                  ),
                  const SizedBox(height: 4),
                  Text(
                    '${errorMessage ?? "事件序列缺口"}：已停止应用事件，当前界面状态可能过期。请刷新快照后选择恢复事件订阅，或明确回退到轮询。',
                    style: theme.textTheme.bodySmall?.copyWith(
                      color: theme.colorScheme.onErrorContainer,
                    ),
                  ),
                  if (resumeCursor != null || epoch != null) ...[
                    const SizedBox(height: 4),
                    Text(
                      '恢复序号：${resumeCursor?.sequence ?? '-'} · 版本：${epoch ?? '未知'}',
                      style: theme.textTheme.labelSmall?.copyWith(
                        color: theme.colorScheme.onErrorContainer,
                      ),
                    ),
                  ],
                  if (errorMessage != null) ...[
                    const SizedBox(height: 4),
                    Text(
                      errorMessage!,
                      style: theme.textTheme.labelSmall?.copyWith(
                        color: theme.colorScheme.onErrorContainer,
                      ),
                    ),
                  ],
                ],
              ),
            ),
            const SizedBox(width: 12),
            Wrap(
              spacing: 8,
              runSpacing: 8,
              alignment: WrapAlignment.end,
              children: [
                OutlinedButton(
                  key: const Key('event-recovery-subscribe'),
                  onPressed: busy ? null : onRefreshAndSubscribe,
                  child: Text(busy ? '刷新中…' : '刷新快照并恢复订阅'),
                ),
                FilledButton.tonal(
                  key: const Key('event-recovery-poll'),
                  onPressed: busy ? null : onRefreshAndPoll,
                  child: const Text('刷新快照并回退轮询'),
                ),
              ],
            ),
          ],
        ),
      ),
    );
  }
}
