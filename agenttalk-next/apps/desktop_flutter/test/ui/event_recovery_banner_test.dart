import 'package:agenttalk_desktop/ipc/core_ipc_client.dart';
import 'package:agenttalk_desktop/ipc/protocol_v1.dart';
import 'package:agenttalk_desktop/ui/event_recovery_banner.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  testWidgets('explains REPLAY_GAP and exposes explicit recovery choices', (
    tester,
  ) async {
    var subscribePressed = false;
    var pollPressed = false;
    const details = ReplayGapDetails(
      streamId: 'core-events',
      epoch: 'epoch-1',
      resumeCursor: StreamCursor(
        streamId: 'core-events',
        sequence: 8,
        epoch: 'epoch-1',
      ),
      headCursor: StreamCursor(
        streamId: 'core-events',
        sequence: 12,
        epoch: 'epoch-1',
      ),
      oldestAvailableCursor: StreamCursor(
        streamId: 'core-events',
        sequence: 9,
        epoch: 'epoch-1',
      ),
    );

    await tester.pumpWidget(
      MaterialApp(
        home: EventRecoveryBanner(
          details: details,
          busy: false,
          errorMessage: 'REPLAY_GAP',
          onRefreshAndSubscribe: () => subscribePressed = true,
          onRefreshAndPoll: () => pollPressed = true,
        ),
      ),
    );

    expect(find.text('事件恢复暂停：需要刷新快照'), findsOneWidget);
    expect(find.textContaining('REPLAY_GAP'), findsNWidgets(2));
    expect(find.textContaining('恢复序号：8'), findsOneWidget);
    await tester.tap(find.byKey(const Key('event-recovery-subscribe')));
    await tester.tap(find.byKey(const Key('event-recovery-poll')));

    expect(subscribePressed, isTrue);
    expect(pollPressed, isTrue);
  });

  testWidgets('disables both recovery actions while snapshot refresh runs', (
    tester,
  ) async {
    const details = ReplayGapDetails(
      streamId: 'core-events',
      epoch: 'epoch-1',
      resumeCursor: StreamCursor(
        streamId: 'core-events',
        sequence: 8,
        epoch: 'epoch-1',
      ),
      headCursor: null,
      oldestAvailableCursor: null,
    );
    await tester.pumpWidget(
      MaterialApp(
        home: EventRecoveryBanner(
          details: details,
          busy: true,
          onRefreshAndSubscribe: () {},
          onRefreshAndPoll: () {},
        ),
      ),
    );

    expect(
      tester
          .widget<OutlinedButton>(
            find.byKey(const Key('event-recovery-subscribe')),
          )
          .onPressed,
      isNull,
    );
    expect(
      tester
          .widget<FilledButton>(find.byKey(const Key('event-recovery-poll')))
          .onPressed,
      isNull,
    );
  });
}
