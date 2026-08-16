#include "folder_picker_channel.h"

#include <shobjidl.h>

#include <flutter/standard_method_codec.h>

#include <string>

namespace {

constexpr HRESULT kDialogCancelled = HRESULT_FROM_WIN32(ERROR_CANCELLED);

std::string Utf8FromWide(const std::wstring& value) {
  if (value.empty()) return {};
  const int size = WideCharToMultiByte(
      CP_UTF8, WC_ERR_INVALID_CHARS, value.data(), static_cast<int>(value.size()),
      nullptr, 0, nullptr, nullptr);
  if (size <= 0) return {};
  std::string result(size, '\0');
  if (WideCharToMultiByte(CP_UTF8, WC_ERR_INVALID_CHARS, value.data(),
                          static_cast<int>(value.size()), result.data(), size,
                          nullptr, nullptr) <= 0) {
    return {};
  }
  return result;
}

HRESULT PickFolder(HWND owner_window, std::wstring* path) {
  IFileOpenDialog* dialog = nullptr;
  HRESULT hr = CoCreateInstance(CLSID_FileOpenDialog, nullptr,
                                CLSCTX_INPROC_SERVER, IID_PPV_ARGS(&dialog));
  if (FAILED(hr)) return hr;

  DWORD options = 0;
  hr = dialog->GetOptions(&options);
  if (SUCCEEDED(hr)) {
    hr = dialog->SetOptions(options | FOS_PICKFOLDERS | FOS_FORCEFILESYSTEM);
  }
  if (SUCCEEDED(hr)) hr = dialog->Show(owner_window);
  if (hr == kDialogCancelled) {
    dialog->Release();
    return hr;
  }
  if (FAILED(hr)) {
    dialog->Release();
    return hr;
  }

  IShellItem* item = nullptr;
  hr = dialog->GetResult(&item);
  if (SUCCEEDED(hr)) {
    PWSTR raw_path = nullptr;
    hr = item->GetDisplayName(SIGDN_FILESYSPATH, &raw_path);
    if (SUCCEEDED(hr) && raw_path != nullptr) {
      *path = raw_path;
      CoTaskMemFree(raw_path);
    }
    item->Release();
  }
  dialog->Release();
  return hr;
}

HRESULT PickFile(HWND owner_window, std::wstring* path) {
  IFileOpenDialog* dialog = nullptr;
  HRESULT hr = CoCreateInstance(CLSID_FileOpenDialog, nullptr,
                                CLSCTX_INPROC_SERVER, IID_PPV_ARGS(&dialog));
  if (FAILED(hr)) return hr;

  DWORD options = 0;
  hr = dialog->GetOptions(&options);
  if (SUCCEEDED(hr)) {
    options &= ~(static_cast<DWORD>(FOS_PICKFOLDERS) |
                 static_cast<DWORD>(FOS_ALLOWMULTISELECT));
    hr = dialog->SetOptions(options | FOS_FORCEFILESYSTEM | FOS_FILEMUSTEXIST |
                            FOS_PATHMUSTEXIST);
  }
  if (SUCCEEDED(hr)) hr = dialog->Show(owner_window);
  if (hr == kDialogCancelled) {
    dialog->Release();
    return hr;
  }
  if (FAILED(hr)) {
    dialog->Release();
    return hr;
  }

  IShellItem* item = nullptr;
  hr = dialog->GetResult(&item);
  if (SUCCEEDED(hr)) {
    PWSTR raw_path = nullptr;
    hr = item->GetDisplayName(SIGDN_FILESYSPATH, &raw_path);
    if (SUCCEEDED(hr) && raw_path != nullptr) {
      *path = raw_path;
      CoTaskMemFree(raw_path);
    }
    item->Release();
  }
  dialog->Release();
  return hr;
}

}  // namespace

FolderPickerChannel::FolderPickerChannel(flutter::FlutterEngine* engine,
                                         HWND owner_window) {
  channel_ = std::make_unique<
      flutter::MethodChannel<flutter::EncodableValue>>(
      engine->messenger(), "agenttalk/folder_picker",
      &flutter::StandardMethodCodec::GetInstance());
  channel_->SetMethodCallHandler(
      [owner_window](const flutter::MethodCall<flutter::EncodableValue>& call,
                     std::unique_ptr<flutter::MethodResult<flutter::EncodableValue>>
                         result) {
        if (call.method_name() == "pickFolder") {
          std::wstring path;
          const HRESULT hr = PickFolder(owner_window, &path);
          if (hr == kDialogCancelled) {
            result->Success();
            return;
          }
          if (FAILED(hr)) {
            result->Error(
                "unavailable",
                "Windows folder picker could not be opened; enter a root path manually.");
            return;
          }

          const std::string utf8_path = Utf8FromWide(path);
          if (utf8_path.empty()) {
            result->Error("failed",
                          "Windows folder picker returned an empty path.");
            return;
          }
          result->Success(flutter::EncodableValue(utf8_path));
          return;
        }

        if (call.method_name() == "pickFile") {
          std::wstring path;
          const HRESULT hr = PickFile(owner_window, &path);
          if (hr == kDialogCancelled) {
            result->Success();
            return;
          }
          if (FAILED(hr)) {
            result->Error("failed", "Windows file picker failed.");
            return;
          }

          const std::string utf8_path = Utf8FromWide(path);
          if (utf8_path.empty()) {
            result->Error("failed", "Windows file picker failed.");
            return;
          }
          result->Success(flutter::EncodableValue(utf8_path));
          return;
        }

        result->NotImplemented();
      });
}

FolderPickerChannel::~FolderPickerChannel() {
  if (channel_) channel_->SetMethodCallHandler(nullptr);
}
