// The status button and its model, driven by a fake [StatusSource].

import 'dart:async';

import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:tagsy_app/data/status_source.dart';
import 'package:tagsy_app/features/status/status_indicator.dart';
import 'package:tagsy_app/features/status/status_model.dart';
import 'package:tagsy_app/features/status/status_panel.dart';
import 'package:tagsy_app/rust/api.dart' as tagsy;

class FakeStatusSource implements StatusSource {
  FakeStatusSource({
    this.peers = const [],
    this.operations = const [],
    List<tagsy.ActivityEntry>? samples,
  }) : _samples = samples ?? [sample()];

  List<tagsy.ConnectedPeerDto> peers;
  List<tagsy.OperationEntry> operations;

  /// Activity samples, returned in order; the last one repeats.
  final List<tagsy.ActivityEntry> _samples;

  final connectionController = StreamController<tagsy.ConnectionUpdateDto>();
  final operationController = StreamController<tagsy.OperationUpdateDto>();

  @override
  Future<List<tagsy.ConnectedPeerDto>> connectedPeers() async => peers;

  @override
  Future<NextUpdate<tagsy.ConnectionUpdateDto>> connectionUpdates() async =>
      _next(connectionController.stream);

  @override
  Future<List<tagsy.OperationEntry>> listOperations() async => operations;

  @override
  Future<NextUpdate<tagsy.OperationUpdateDto>> operationUpdates() async =>
      _next(operationController.stream);

  @override
  Future<tagsy.ActivityEntry> activity() async =>
      _samples.length > 1 ? _samples.removeAt(0) : _samples.first;

  static NextUpdate<T> _next<T>(Stream<T> stream) {
    final iterator = StreamIterator(stream);
    return () async => await iterator.moveNext() ? iterator.current : null;
  }
}

tagsy.InboxActivityEntry inbox({bool busy = false, int queued = 0}) =>
    tagsy.InboxActivityEntry(queued: queued, busy: busy, processed: 0);

tagsy.ActivityEntry sample({
  bool idle = true,
  bool scanDone = true,
  int pullsRunning = 0,
  int pendingFilesystemEvents = 0,
  tagsy.InboxActivityEntry? catalog,
}) => tagsy.ActivityEntry(
  catalog: catalog ?? inbox(),
  syncDirectories: inbox(),
  peerSessions: const tagsy.SessionActivityEntry(
    busy: 0,
    outboundQueued: 0,
    processed: 0,
  ),
  pendingFilesystemEvents: pendingFilesystemEvents,
  initialScanComplete: scanDone,
  pullsQueued: 0,
  pullsRunning: pullsRunning,
  idle: idle,
);

tagsy.ConnectedPeerDto peer(String name) => tagsy.ConnectedPeerDto(
  peerName: name,
  publicKey: 'key-$name',
  direction: tagsy.ConnectionDirectionDto.outbound,
  since: DateTime.now().millisecondsSinceEpoch,
);

tagsy.OperationEntry operation(
  int id,
  String kind, {
  tagsy.OperationStatusDto status = const tagsy.OperationStatusDto.active(),
  int? done,
  int? total,
}) => tagsy.OperationEntry(
  id: BigInt.from(id),
  kind: kind,
  peerName: 'central',
  fileId: kind == 'receiving_file' ? 'f00dfeed0000' : null,
  status: status,
  progressDone: done == null ? null : BigInt.from(done),
  progressTotal: total == null ? null : BigInt.from(total),
  startedAt: id,
  updatedAt: id,
);

/// Let the model's async loops run.
Future<void> settle() => Future<void>.delayed(const Duration(milliseconds: 20));

void main() {
  group('StatusModel', () {
    test('tracks peers from the snapshot and the stream', () async {
      final source = FakeStatusSource(peers: [peer('central')]);
      final model = StatusModel(source)..start();
      await settle();
      expect(model.peers.map((p) => p.peerName), ['central']);

      source.connectionController
        ..add(tagsy.ConnectionUpdateDto.connected(peer: peer('laptop')))
        ..add(
          const tagsy.ConnectionUpdateDto.disconnected(
            publicKey: 'key-central',
          ),
        );
      await settle();
      expect(model.peers.map((p) => p.peerName), ['laptop']);
      model.dispose();
    });

    test(
      'separates connect attempts, running work and finished work',
      () async {
        final source = FakeStatusSource(
          operations: [
            operation(1, kConnectingKind),
            operation(2, 'receiving_file', done: 10, total: 100),
          ],
        );
        final model = StatusModel(source)..start();
        await settle();
        expect(model.connecting, isTrue);
        expect(model.activeOperations.map((op) => op.id), [BigInt.from(2)]);
        expect(model.busy, isTrue, reason: 'a transfer is running');

        source.operationController
          ..add(
            tagsy.OperationUpdateDto.updated(
              operation: operation(
                2,
                'receiving_file',
                status: const tagsy.OperationStatusDto.completed(),
              ),
            ),
          )
          ..add(
            tagsy.OperationUpdateDto.updated(
              operation: operation(
                1,
                kConnectingKind,
                status: const tagsy.OperationStatusDto.completed(),
              ),
            ),
          );
        await settle();
        expect(model.activeOperations, isEmpty);
        expect(model.connecting, isFalse);
        expect(
          model.recentOperations.map((op) => op.id),
          [BigInt.from(2)],
          reason: 'finished connect attempts are not listed as work',
        );
        model.dispose();
      },
    );

    test('clears busy only after two idle samples in a row', () async {
      final source = FakeStatusSource(
        samples: [sample(idle: false, pullsRunning: 1), sample(), sample()],
      );
      final model = StatusModel(
        source,
        pollInterval: const Duration(milliseconds: 30),
      )..start();
      await Future<void>.delayed(const Duration(milliseconds: 10));
      expect(model.busy, isTrue);
      await Future<void>.delayed(const Duration(milliseconds: 30));
      expect(model.busy, isTrue, reason: 'one idle sample is not enough');
      await Future<void>.delayed(const Duration(milliseconds: 40));
      expect(model.busy, isFalse);
      model.dispose();
    });
  });

  group('activityDetails', () {
    test('lists only what is not idle', () {
      expect(activityDetails(sample()), isEmpty);
      expect(
        activityDetails(
          sample(
            idle: false,
            scanDone: false,
            pullsRunning: 2,
            pendingFilesystemEvents: 1,
            catalog: inbox(busy: true, queued: 40),
          ),
        ),
        [
          'Scanning sync directories',
          'Applying catalog changes (40 queued)',
          '1 file change settling',
          'Transfers: 2 running, 0 queued',
        ],
      );
    });
  });

  group('StatusIndicator', () {
    Future<void> pumpIndicator(WidgetTester tester, StatusSource source) async {
      await tester.pumpWidget(
        MaterialApp(
          home: Scaffold(
            appBar: AppBar(actions: [StatusIndicator(source: source)]),
          ),
        ),
      );
      // Let the snapshots land (real async inside the fake).
      await tester.runAsync(settle);
      await tester.pump();
    }

    testWidgets('shows peers, busy ring and a count of running work', (
      tester,
    ) async {
      final source = FakeStatusSource(
        peers: [peer('central')],
        operations: [operation(7, 'receiving_file', done: 1024, total: 4096)],
        samples: [sample(idle: false, pullsRunning: 1)],
      );
      await pumpIndicator(tester, source);

      expect(find.byIcon(Icons.link), findsOneWidget);
      expect(find.byKey(const ValueKey('status-busy')), findsOneWidget);
      expect(find.text('1'), findsOneWidget);

      await tester.tap(find.byType(IconButton));
      // Not `pumpAndSettle`: the busy spinners animate indefinitely.
      await tester.pump();
      await tester.pump(const Duration(milliseconds: 500));
      expect(find.text('central'), findsWidgets);
      expect(find.text('Syncing…'), findsOneWidget);
      expect(find.text('Transfers: 1 running, 0 queued'), findsOneWidget);
      expect(find.text('Receiving file'), findsOneWidget);
      expect(find.textContaining('1.0 KiB / 4.0 KiB'), findsOneWidget);

      await tester.pumpWidget(const SizedBox());
    });

    testWidgets('is calm when alone and idle', (tester) async {
      await pumpIndicator(tester, FakeStatusSource());

      expect(find.byIcon(Icons.link_off), findsOneWidget);
      expect(find.byKey(const ValueKey('status-busy')), findsNothing);

      await tester.tap(find.byType(IconButton));
      await tester.pumpAndSettle(const Duration(milliseconds: 100));
      expect(find.text('No peers connected'), findsOneWidget);
      expect(find.text('Up to date'), findsOneWidget);
      expect(find.text('Nothing running'), findsOneWidget);

      await tester.pumpWidget(const SizedBox());
    });
  });
}
