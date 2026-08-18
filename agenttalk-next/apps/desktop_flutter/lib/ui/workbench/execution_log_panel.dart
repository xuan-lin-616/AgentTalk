import 'package:flutter/material.dart';
import 'package:intl/intl.dart';

import '../theme/studio_colors.dart';
import 'studio_event_log.dart';

/// Event-fed execution log panel.
///
/// The shell passes [entries] derived exclusively from real IPC events
/// (`events.replay` / `events.subscribe(core-events)`). When the Core has not
/// emitted anything yet the panel shows an explicit empty state; it never
/// renders synthetic log lines.
class ExecutionLogPanel extends StatelessWidget {
  const ExecutionLogPanel({
    super.key,
    required this.entries,
    this.maxEntries = 500,
    this.onClear,
  });

  final List<StudioLogEntry> entries;
  final int maxEntries;
  final VoidCallback? onClear;

  @override
  Widget build(BuildContext context) {
    final timeFormat = DateFormat('HH:mm:ss');
    final visible = entries.length > maxEntries
        ? entries.sublist(entries.length - maxEntries)
        : entries;
    return Container(
      decoration: const BoxDecoration(
        color: StudioColors.bgSurface,
        border: Border(top: BorderSide(color: StudioColors.borderSubtle)),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Padding(
            padding: const EdgeInsets.symmetric(horizontal: 14, vertical: 8),
            child: Row(
              mainAxisAlignment: MainAxisAlignment.spaceBetween,
              children: [
                const Text(
                  '运行日志',
                  style: TextStyle(
                    color: StudioColors.textPrimary,
                    fontSize: 12,
                    fontWeight: FontWeight.w600,
                  ),
                ),
                if (onClear != null)
                  InkWell(
                    onTap: onClear,
                    borderRadius: BorderRadius.circular(4),
                    child: const Padding(
                      padding: EdgeInsets.symmetric(horizontal: 6, vertical: 2),
                      child: Text(
                        '清空',
                        style: TextStyle(
                          color: StudioColors.textTertiary,
                          fontSize: 10,
                        ),
                      ),
                    ),
                  ),
              ],
            ),
          ),
          const Divider(height: 1, color: StudioColors.borderSubtle),
          Expanded(
            child: visible.isEmpty
                ? const _EmptyLogState()
                : ListView.builder(
                    padding: const EdgeInsets.symmetric(
                      horizontal: 14,
                      vertical: 6,
                    ),
                    itemCount: visible.length,
                    itemExtent: 24,
                    itemBuilder: (context, index) {
                      final entry = visible[index];
                      return _LogRow(entry: entry, timeFormat: timeFormat);
                    },
                  ),
          ),
        ],
      ),
    );
  }
}

class _EmptyLogState extends StatelessWidget {
  const _EmptyLogState();

  @override
  Widget build(BuildContext context) {
    return Center(
      child: Column(
        mainAxisSize: MainAxisSize.min,
        children: [
          Icon(
            Icons.list_alt_rounded,
            size: 34,
            color: StudioColors.textTertiary,
          ),
          const SizedBox(height: 8),
          const Text(
            '暂无运行日志',
            style: TextStyle(
              color: StudioColors.textPrimary,
              fontSize: 12,
              fontWeight: FontWeight.w600,
            ),
          ),
          const SizedBox(height: 4),
          const Text(
            '来自 Core 的事件将实时显示在这里',
            style: TextStyle(color: StudioColors.textSecondary, fontSize: 10),
          ),
        ],
      ),
    );
  }
}

class _LogRow extends StatelessWidget {
  const _LogRow({required this.entry, required this.timeFormat});

  final StudioLogEntry entry;
  final DateFormat timeFormat;

  @override
  Widget build(BuildContext context) {
    final color = studioLogLevelColor(entry.level);
    return Row(
      children: [
        Container(
          width: 6,
          height: 6,
          decoration: BoxDecoration(color: color, shape: BoxShape.circle),
        ),
        const SizedBox(width: 8),
        Text(
          timeFormat.format(entry.occurredAt),
          style: const TextStyle(
            color: StudioColors.textTertiary,
            fontSize: 10,
            fontFamily: 'monospace',
          ),
        ),
        const SizedBox(width: 10),
        Expanded(
          child: Text(
            entry.message,
            style: const TextStyle(
              color: StudioColors.textSecondary,
              fontSize: 10,
            ),
            overflow: TextOverflow.ellipsis,
          ),
        ),
      ],
    );
  }
}
