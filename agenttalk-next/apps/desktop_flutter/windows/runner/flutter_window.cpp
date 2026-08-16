#include "flutter_window.h"

#include <tlhelp32.h>

#include <limits>
#include <optional>

#include "flutter/generated_plugin_registrant.h"
#include <flutter/standard_method_codec.h>

namespace {

constexpr UINT kCloseCompletedMessage = WM_APP + 17;
constexpr UINT_PTR kCloseTimeoutTimerId = 1;
constexpr UINT kCloseTimeoutMs = 10000;
constexpr char kAppLifecycleChannel[] = "agenttalk/app_lifecycle";

bool IsDirectChildProcess(DWORD process_id) {
  if (process_id == 0 || process_id == GetCurrentProcessId()) return false;

  HANDLE snapshot = CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0);
  if (snapshot == INVALID_HANDLE_VALUE) return false;

  PROCESSENTRY32W entry{};
  entry.dwSize = sizeof(entry);
  bool is_direct_child = false;
  if (Process32FirstW(snapshot, &entry)) {
    do {
      if (entry.th32ProcessID == process_id) {
        is_direct_child = entry.th32ParentProcessID == GetCurrentProcessId();
        break;
      }
    } while (Process32NextW(snapshot, &entry));
  }
  CloseHandle(snapshot);
  return is_direct_child;
}

}  // namespace

FlutterWindow::FlutterWindow(const flutter::DartProject& project)
    : project_(project) {}

FlutterWindow::~FlutterWindow() {}

bool FlutterWindow::OnCreate() {
  if (!Win32Window::OnCreate()) {
    return false;
  }

  RECT frame = GetClientArea();

  // The size here must match the window dimensions to avoid unnecessary surface
  // creation / destruction in the startup path.
  flutter_controller_ = std::make_unique<flutter::FlutterViewController>(
      frame.right - frame.left, frame.bottom - frame.top, project_);
  // Ensure that basic setup of the controller was successful.
  if (!flutter_controller_->engine() || !flutter_controller_->view()) {
    return false;
  }
  RegisterPlugins(flutter_controller_->engine());
  folder_picker_channel_ = std::make_unique<FolderPickerChannel>(
      flutter_controller_->engine(), GetHandle());
  app_lifecycle_channel_ = std::make_unique<
      flutter::MethodChannel<flutter::EncodableValue>>(
      flutter_controller_->engine()->messenger(), kAppLifecycleChannel,
      &flutter::StandardMethodCodec::GetInstance());
  app_lifecycle_channel_->SetMethodCallHandler(
      [this](const flutter::MethodCall<flutter::EncodableValue>& call,
             std::unique_ptr<flutter::MethodResult<flutter::EncodableValue>>
                 result) {
        if (call.method_name() == "closeCompleted") {
          result->Success();
          if (GetHandle() != nullptr) {
            PostMessage(GetHandle(), kCloseCompletedMessage, 0, 0);
          }
          return;
        }
        if (call.method_name() == "registerOwnedCore") {
          const auto* arguments = call.arguments();
          int64_t process_id = 0;
          if (arguments != nullptr) {
            if (const auto* int32_value = std::get_if<int32_t>(arguments)) {
              process_id = *int32_value;
            } else if (const auto* int64_value = std::get_if<int64_t>(arguments)) {
              process_id = *int64_value;
            }
          }
          if (process_id <= 0 ||
              static_cast<uint64_t>(process_id) >
                  static_cast<uint64_t>(std::numeric_limits<DWORD>::max())) {
            result->Error("invalid_owned_core", "Owned Core PID is invalid.");
            return;
          }
          if (!RegisterOwnedCoreProcess(static_cast<DWORD>(process_id))) {
            result->Error(
                "owned_core_registration_failed",
                "Windows runner refused to own the requested Core process.");
            return;
          }
          result->Success();
          return;
        }
        result->NotImplemented();
      });
  SetChildContent(flutter_controller_->view()->GetNativeWindow());

  flutter_controller_->engine()->SetNextFrameCallback([&]() {
    this->Show();
  });

  // Flutter can complete the first frame before the "show window" callback is
  // registered. The following call ensures a frame is pending to ensure the
  // window is shown. It is a no-op if the first frame hasn't completed yet.
  flutter_controller_->ForceRedraw();

  return true;
}

void FlutterWindow::OnDestroy() {
  ReleaseOwnedCoreJob();
  if (GetHandle() != nullptr) {
    KillTimer(GetHandle(), kCloseTimeoutTimerId);
  }
  if (app_lifecycle_channel_) {
    app_lifecycle_channel_->SetMethodCallHandler(nullptr);
    app_lifecycle_channel_ = nullptr;
  }
  folder_picker_channel_ = nullptr;
  if (flutter_controller_) {
    flutter_controller_ = nullptr;
  }

  Win32Window::OnDestroy();
}

bool FlutterWindow::RegisterOwnedCoreProcess(DWORD process_id) {
  if (owned_core_job_ != nullptr) {
    return owned_core_pid_ == process_id;
  }
  if (!IsDirectChildProcess(process_id)) return false;

  constexpr DWORD kOwnedCoreAccess = PROCESS_QUERY_LIMITED_INFORMATION |
                                     PROCESS_SET_QUOTA | PROCESS_TERMINATE;
  HANDLE process = OpenProcess(kOwnedCoreAccess, FALSE, process_id);
  if (process == nullptr) return false;

  DWORD exit_code = 0;
  if (!GetExitCodeProcess(process, &exit_code) || exit_code != STILL_ACTIVE) {
    CloseHandle(process);
    return false;
  }

  HANDLE job = CreateJobObjectW(nullptr, nullptr);
  if (job == nullptr) {
    CloseHandle(process);
    return false;
  }

  JOBOBJECT_EXTENDED_LIMIT_INFORMATION limits{};
  limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
  if (!SetInformationJobObject(job, JobObjectExtendedLimitInformation, &limits,
                               sizeof(limits)) ||
      !AssignProcessToJobObject(job, process)) {
    CloseHandle(job);
    CloseHandle(process);
    return false;
  }

  CloseHandle(process);
  owned_core_job_ = job;
  owned_core_pid_ = process_id;
  return true;
}

void FlutterWindow::ReleaseOwnedCoreJob() {
  if (owned_core_job_ != nullptr) {
    CloseHandle(owned_core_job_);
    owned_core_job_ = nullptr;
  }
  owned_core_pid_ = 0;
}

LRESULT
FlutterWindow::MessageHandler(HWND hwnd, UINT const message,
                              WPARAM const wparam,
                              LPARAM const lparam) noexcept {
  if (message == WM_CLOSE && app_lifecycle_channel_ != nullptr) {
    if (!close_requested_) {
      close_requested_ = true;
      SetTimer(GetHandle(), kCloseTimeoutTimerId, kCloseTimeoutMs, nullptr);
      app_lifecycle_channel_->InvokeMethod(
        "requestClose", std::unique_ptr<flutter::EncodableValue>());
    }
    return 0;
  }

  if (message == kCloseCompletedMessage) {
    KillTimer(GetHandle(), kCloseTimeoutTimerId);
    Destroy();
    return 0;
  }

  if (message == WM_TIMER && wparam == kCloseTimeoutTimerId) {
    KillTimer(GetHandle(), kCloseTimeoutTimerId);
    Destroy();
    return 0;
  }

  // Give Flutter, including plugins, an opportunity to handle window messages.
  if (flutter_controller_) {
    std::optional<LRESULT> result =
        flutter_controller_->HandleTopLevelWindowProc(hwnd, message, wparam,
                                                      lparam);
    if (result) {
      return *result;
    }
  }

  switch (message) {
    case WM_FONTCHANGE:
      flutter_controller_->engine()->ReloadSystemFonts();
      break;
  }

  return Win32Window::MessageHandler(hwnd, message, wparam, lparam);
}
