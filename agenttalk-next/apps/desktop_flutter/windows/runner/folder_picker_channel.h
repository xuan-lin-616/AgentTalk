#ifndef RUNNER_FOLDER_PICKER_CHANNEL_H_
#define RUNNER_FOLDER_PICKER_CHANNEL_H_

#include <flutter/encodable_value.h>
#include <flutter/flutter_engine.h>
#include <flutter/method_channel.h>

#include <memory>

#include <windows.h>

class FolderPickerChannel {
 public:
  FolderPickerChannel(flutter::FlutterEngine* engine, HWND owner_window);
  ~FolderPickerChannel();

  FolderPickerChannel(const FolderPickerChannel&) = delete;
  FolderPickerChannel& operator=(const FolderPickerChannel&) = delete;

 private:
  std::unique_ptr<flutter::MethodChannel<flutter::EncodableValue>> channel_;
};

#endif  // RUNNER_FOLDER_PICKER_CHANNEL_H_
