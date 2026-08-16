# Windows 可直接运行包

## 运行布局

`build-windows-runnable-bundle.ps1` 生成项目外的 flat portable bundle：

```text
AgentTalk\\
  agenttalk_desktop.exe
  agenttalk-core.exe
  agenttalk-bundle.manifest.json
  flutter_windows.dll
  data\\
    flutter_assets\\
```

Flutter 默认只接受同一目录下、由 bundle manifest 认证过的 `agenttalk-core.exe`。manifest 记录构建时 Git SHA、IPC Schema SHA 和每个 bundle 文件的 SHA-256；缺失、篡改或二进制身份不一致时，应用向 UI 显示 fail-closed 错误，不回退到 PATH、当前工作目录、旧 worktree、旧 Electron 或临时 Cargo 目录。

## 构建

必须在 clean 的工作树上执行。脚本默认记录实际 branch、HEAD SHA、源码快照和 IPC Schema SHA；如需额外约束，可显式传入 `ExpectedBranch` 或 `ExpectedGitSha`。

```powershell
# Set the state root to a location outside the repository before building.
$env:AGENTTALK_STATE_ROOT = '<your-state-root>'
pwsh -NoProfile -ExecutionPolicy Bypass `
  -File .\agenttalk-next\scripts\build-windows-runnable-bundle.ps1 `
  -Clean `
  -OutputRoot (Join-Path $env:AGENTTALK_STATE_ROOT 'artifacts\windows-runnable-v1-fix\AgentTalk')
```

可选的显式身份约束示例：

```powershell
-ExpectedBranch main -ExpectedGitSha <owner-approved-sha>
```

构建脚本不绑定临时任务分支；合并到 `main` 后无需修改脚本即可重建。

默认输出为 `AGENTTALK_STATE_ROOT\\artifacts\\windows-runnable-v1\\AgentTalk`；未设置时使用项目目录同级的 `AgentTalk-local-state`。脚本会重新构建 Rust Core 和 Flutter Windows Release，确认构建前后 Git SHA/clean 状态与 IPC Schema SHA 未变化，然后校验完整目录并生成 manifest。脚本不制作安装器、不签名、不发布。

## 数据目录

生产默认数据库为：

```text
%LOCALAPPDATA%\\AgentTalk\\data\\agenttalk-core.sqlite3
```

附件/摘要等 Core-owned artifact 默认位于同一 canonical data root 的 `artifacts\\` 下。应用不会向源码目录、bundle 目录或 `legacy-electron/` 写入数据库。`AGENTTALK_DATA_ROOT`、`AGENTTALK_CORE_DATABASE` 和 `AGENTTALK_CORE_ARTIFACT_ROOT` 仅用于明确的开发/测试覆盖，不表示旧数据已迁移。

## Core 所有权与关闭

默认模式由 Flutter 启动本 bundle 内 Core，并在窗口关闭时通过现有 IPC v1 `shutdown_owned` 请求优雅退出，等待有界时间，最后只对本次 owned 子进程执行兜底终止。Core 启动成功后，Flutter 通过 `agenttalk/app_lifecycle` 向当前 Native runner 登记该直接子进程；runner 将它加入仅属于当前桌面实例的 Windows Job Object，并设置 `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`。因此 Dart close handler 永久挂起时，Native 10 秒 fallback 销毁窗口并释放 Job handle，当前 owned Core 也会被系统兜底终止；external Core 从不登记、不加入该 Job。普通请求在关闭开始时收到 `CLIENT_CLOSED`，不会阻塞关闭队列；Core 启动、handshake 和 snapshot 期间的启动任务也纳入关闭所有权。Flutter close 总上限为 9 秒，Core IPC close 总上限为 8 秒，Windows runner 在 10 秒仍未收到 `closeCompleted` 时允许窗口退出。

测试/开发 fixture 可在 `AGENTTALK_CORE_DEV_MODE=1` 下使用 `AGENTTALK_CORE_STARTUP_DELAY_MS`、`AGENTTALK_CORE_TEST_BEHAVIOR=handshake|snapshot|requests` 或 `AGENTTALK_TEST_HANG_CLOSE=1` 模拟启动、请求和 Dart close handler 挂起；生产默认不设置这些变量。close-handler fixture 只阻塞本次 Dart close 回调，不读取或写入正式数据。

永久挂起场景的三轮 Windows Smoke 入口为：

```powershell
pwsh -NoProfile -ExecutionPolicy Bypass `
  -File .\agenttalk-next\scripts\test-windows-close-handler-hang.ps1 `
  -BundleRoot $env:AGENTTALK_STATE_ROOT\artifacts\windows-runnable-v1-owner-gate\AgentTalk `
  -OutputRoot $env:AGENTTALK_STATE_ROOT\smoke\windows-close-handler-hang
```

该脚本从 Bundle 目录外的 cwd 启动完整 Bundle，显式清除 Core 路径/外部模式覆盖，记录 Flutter/Core PID、WM_CLOSE 时间和退出耗时，并只清理它自己启动的进程与轮次隔离数据。

外部 Core 测试模式：

```powershell
$env:AGENTTALK_CORE_MODE = 'external'
$env:AGENTTALK_CORE_PIPE = '\\.\pipe\agenttalk-external-test'
$env:AGENTTALK_CORE_SESSION_CREDENTIAL = '<test-only credential>'
```

external client 不持有 Core `Process`，关闭 Flutter 只关闭 IPC transport，不发送 `shutdown_owned`，外部 Core 继续运行。凭据只通过进程环境传递，不写入 manifest、日志、Git 或文档。
