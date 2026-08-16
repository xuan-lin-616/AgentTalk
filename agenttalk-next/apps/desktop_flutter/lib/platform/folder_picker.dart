import 'dart:io';

import 'package:flutter/services.dart';

const String folderPickerChannelName = 'agenttalk/folder_picker';
const String folderPickerMethodName = 'pickFolder';
const String filePickerMethodName = 'pickFile';

enum FolderPickerStatus { selected, cancelled, unavailable, failed }

class FolderPickerResult {
  const FolderPickerResult._({required this.status, this.path, this.message});

  const FolderPickerResult.selected(String path)
    : this._(status: FolderPickerStatus.selected, path: path);

  const FolderPickerResult.cancelled()
    : this._(status: FolderPickerStatus.cancelled);

  const FolderPickerResult.unavailable(String message)
    : this._(status: FolderPickerStatus.unavailable, message: message);

  const FolderPickerResult.failed(String message)
    : this._(status: FolderPickerStatus.failed, message: message);

  final FolderPickerStatus status;
  final String? path;
  final String? message;

  bool get hasSelection =>
      status == FolderPickerStatus.selected && path != null && path!.isNotEmpty;
}

abstract interface class FolderPickerClient {
  Future<FolderPickerResult> pickFolder();
}

class MethodChannelFolderPickerClient implements FolderPickerClient {
  MethodChannelFolderPickerClient({MethodChannel? channel})
    : _channel = channel ?? const MethodChannel(folderPickerChannelName);

  final MethodChannel _channel;

  @override
  Future<FolderPickerResult> pickFolder() async {
    try {
      final value = await _channel.invokeMethod<String>(folderPickerMethodName);
      if (value == null) return const FolderPickerResult.cancelled();
      final path = value.trim();
      if (path.isEmpty) {
        return const FolderPickerResult.failed(
          'Windows folder picker returned an empty path.',
        );
      }
      return FolderPickerResult.selected(path);
    } on MissingPluginException {
      return const FolderPickerResult.unavailable(
        'Windows 原生文件夹选择不可用；请手动输入根目录，未使用默认目录。',
      );
    } on PlatformException catch (error) {
      if (error.code == 'cancelled') {
        return const FolderPickerResult.cancelled();
      }
      if (error.code == 'channel-error' || error.code == 'unavailable') {
        return FolderPickerResult.unavailable(
          error.message ?? 'Windows 原生文件夹选择不可用；请手动输入根目录。',
        );
      }
      return FolderPickerResult.failed(error.message ?? '文件夹选择失败；请手动输入根目录。');
    } on Object catch (error) {
      return FolderPickerResult.failed('文件夹选择失败：$error');
    }
  }
}

class UnsupportedFolderPickerClient implements FolderPickerClient {
  const UnsupportedFolderPickerClient();

  @override
  Future<FolderPickerResult> pickFolder() async =>
      const FolderPickerResult.unavailable('当前平台不支持原生文件夹选择；请手动输入根目录，未使用默认目录。');
}

FolderPickerClient createFolderPickerClient({bool? isWindows}) {
  final supported = isWindows ?? Platform.isWindows;
  return supported
      ? MethodChannelFolderPickerClient()
      : const UnsupportedFolderPickerClient();
}

enum FilePickerStatus { selected, cancelled, unavailable, failed }

class FilePickerResult {
  const FilePickerResult._({required this.status, this.path, this.message});

  const FilePickerResult.selected(String path)
    : this._(status: FilePickerStatus.selected, path: path);

  const FilePickerResult.cancelled()
    : this._(status: FilePickerStatus.cancelled);

  const FilePickerResult.unavailable(String message)
    : this._(status: FilePickerStatus.unavailable, message: message);

  const FilePickerResult.failed(String message)
    : this._(status: FilePickerStatus.failed, message: message);

  final FilePickerStatus status;
  final String? path;
  final String? message;

  bool get hasSelection =>
      status == FilePickerStatus.selected && path != null && path!.isNotEmpty;
}

abstract interface class FilePickerClient {
  Future<FilePickerResult> pickFile();
}

class MethodChannelFilePickerClient implements FilePickerClient {
  MethodChannelFilePickerClient({MethodChannel? channel})
    : _channel = channel ?? const MethodChannel(folderPickerChannelName);

  final MethodChannel _channel;

  @override
  Future<FilePickerResult> pickFile() async {
    try {
      final value = await _channel.invokeMethod<String>(filePickerMethodName);
      if (value == null) return const FilePickerResult.cancelled();
      final path = value.trim();
      if (path.isEmpty) {
        return const FilePickerResult.failed(
          'Windows file picker returned an empty path; no file was selected.',
        );
      }
      return FilePickerResult.selected(path);
    } on MissingPluginException {
      return const FilePickerResult.unavailable(
        'Windows 原生文件选择不可用；未选择文件，未使用默认路径。',
      );
    } on PlatformException catch (error) {
      if (error.code == 'cancelled') {
        return const FilePickerResult.cancelled();
      }
      if (error.code == 'channel-error' || error.code == 'unavailable') {
        return const FilePickerResult.unavailable(
          'Windows 原生文件选择不可用；未选择文件，未使用默认路径。',
        );
      }
      return const FilePickerResult.failed('文件选择失败；未选择文件。');
    } on Object {
      return const FilePickerResult.failed('文件选择失败；未选择文件。');
    }
  }
}

class UnsupportedFilePickerClient implements FilePickerClient {
  const UnsupportedFilePickerClient();

  @override
  Future<FilePickerResult> pickFile() async =>
      const FilePickerResult.unavailable('当前平台不支持原生文件选择；未选择文件，未使用默认路径。');
}

FilePickerClient createFilePickerClient({bool? isWindows}) {
  final supported = isWindows ?? Platform.isWindows;
  return supported
      ? MethodChannelFilePickerClient()
      : const UnsupportedFilePickerClient();
}
