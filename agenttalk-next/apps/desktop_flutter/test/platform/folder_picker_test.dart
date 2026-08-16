import 'package:agenttalk_desktop/platform/folder_picker.dart';
import 'package:flutter/services.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  TestWidgetsFlutterBinding.ensureInitialized();

  test('non-Windows factory returns an explicit unavailable result', () async {
    final picker = createFolderPickerClient(isWindows: false);

    final result = await picker.pickFolder();

    expect(result.status, FolderPickerStatus.unavailable);
    expect(result.path, isNull);
    expect(result.message, contains('未使用默认目录'));
  });

  test('method channel maps a selected path to the typed result', () async {
    const channel = MethodChannel(folderPickerChannelName);
    TestDefaultBinaryMessengerBinding.instance.defaultBinaryMessenger
        .setMockMethodCallHandler(channel, (call) async {
          expect(call.method, folderPickerMethodName);
          return r'C:\Workspace\demo';
        });
    addTearDown(
      () => TestDefaultBinaryMessengerBinding.instance.defaultBinaryMessenger
          .setMockMethodCallHandler(channel, null),
    );

    final result = await MethodChannelFolderPickerClient(
      channel: channel,
    ).pickFolder();

    expect(result.status, FolderPickerStatus.selected);
    expect(result.path, r'C:\Workspace\demo');
    expect(result.hasSelection, isTrue);
  });

  test('missing channel becomes an explicit unavailable result', () async {
    const channel = MethodChannel(folderPickerChannelName);

    final result = await MethodChannelFolderPickerClient(
      channel: channel,
    ).pickFolder();

    expect(result.status, FolderPickerStatus.unavailable);
    expect(result.path, isNull);
    expect(result.message, contains('请手动输入根目录'));
  });

  test('file picker maps a selected file path to its typed result', () async {
    const channel = MethodChannel(folderPickerChannelName);
    TestDefaultBinaryMessengerBinding.instance.defaultBinaryMessenger
        .setMockMethodCallHandler(channel, (call) async {
          expect(call.method, filePickerMethodName);
          return r'C:\Workspace\attachment.pdf';
        });
    addTearDown(
      () => TestDefaultBinaryMessengerBinding.instance.defaultBinaryMessenger
          .setMockMethodCallHandler(channel, null),
    );

    final result = await MethodChannelFilePickerClient(
      channel: channel,
    ).pickFile();

    expect(result.status, FilePickerStatus.selected);
    expect(result.path, r'C:\Workspace\attachment.pdf');
    expect(result.hasSelection, isTrue);
  });

  test('file picker maps a null channel result to cancelled', () async {
    const channel = MethodChannel(folderPickerChannelName);
    TestDefaultBinaryMessengerBinding.instance.defaultBinaryMessenger
        .setMockMethodCallHandler(channel, (call) async {
          expect(call.method, filePickerMethodName);
          return null;
        });
    addTearDown(
      () => TestDefaultBinaryMessengerBinding.instance.defaultBinaryMessenger
          .setMockMethodCallHandler(channel, null),
    );

    final result = await MethodChannelFilePickerClient(
      channel: channel,
    ).pickFile();

    expect(result.status, FilePickerStatus.cancelled);
    expect(result.path, isNull);
    expect(result.hasSelection, isFalse);
  });

  test('file picker maps a missing plugin to unavailable', () async {
    const channel = MethodChannel(folderPickerChannelName);

    final result = await MethodChannelFilePickerClient(
      channel: channel,
    ).pickFile();

    expect(result.status, FilePickerStatus.unavailable);
    expect(result.path, isNull);
    expect(result.message, contains('未使用默认路径'));
  });

  test('file picker maps a platform failure without leaking details', () async {
    const channel = MethodChannel(folderPickerChannelName);
    TestDefaultBinaryMessengerBinding.instance.defaultBinaryMessenger
        .setMockMethodCallHandler(channel, (call) async {
          expect(call.method, filePickerMethodName);
          throw PlatformException(
            code: 'failed',
            message: r'Could not open C:\Private\attachment.txt',
          );
        });
    addTearDown(
      () => TestDefaultBinaryMessengerBinding.instance.defaultBinaryMessenger
          .setMockMethodCallHandler(channel, null),
    );

    final result = await MethodChannelFilePickerClient(
      channel: channel,
    ).pickFile();

    expect(result.status, FilePickerStatus.failed);
    expect(result.path, isNull);
    expect(result.message, isNot(contains(r'C:\Private')));
  });

  test('file picker rejects an empty selected path', () async {
    const channel = MethodChannel(folderPickerChannelName);
    TestDefaultBinaryMessengerBinding.instance.defaultBinaryMessenger
        .setMockMethodCallHandler(channel, (call) async {
          expect(call.method, filePickerMethodName);
          return '   ';
        });
    addTearDown(
      () => TestDefaultBinaryMessengerBinding.instance.defaultBinaryMessenger
          .setMockMethodCallHandler(channel, null),
    );

    final result = await MethodChannelFilePickerClient(
      channel: channel,
    ).pickFile();

    expect(result.status, FilePickerStatus.failed);
    expect(result.path, isNull);
    expect(result.hasSelection, isFalse);
  });

  test('non-Windows file picker fails closed without a fallback', () async {
    final picker = createFilePickerClient(isWindows: false);

    final result = await picker.pickFile();

    expect(result.status, FilePickerStatus.unavailable);
    expect(result.path, isNull);
    expect(result.message, contains('未使用默认路径'));
  });
}
