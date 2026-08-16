# AgentTalk

本仓库发布 AgentTalk Flutter Windows + Rust Core/Runtime Host 新版，唯一权威源码边界是 `agenttalk-next/`。

本机的旧 Electron 对照目录不属于此仓库，保持在本机用于 UI、功能和行为参考；它被 Git 忽略，也不属于当前或可达 Git 历史。构建输出、候选包、release、备份、缓存、数据库和日志保存在项目外状态目录。

## 目录与状态边界

根目录只保留新版源码、产品级文档、Git 配置和必要治理工具。状态目录默认是项目目录同级的 `AgentTalk-local-state`，也可通过 `AGENTTALK_STATE_ROOT` 指定。

状态目录约定：

- `backups/`：恢复点和历史备份。
- `artifacts/`：开发输出、候选包、安装包和验收证据。
- `releases/`：不可覆盖的历史 release。
- `archives/`：incoming、规划历史和迁移归档。
- `config/`：受限本地配置；不进入 Git。
- `temp/`：可再生缓存、依赖、构建树和 bundled runtime。

数据库、导入、恢复和正式 migration 必须单独经过 Owner Gate；敏感配置不进入 Git。

## 运行与构建

构建前必须先设置工具根目录。`.\scripts\dev-env.ps1` 要求 `AGENTTALK_TOOLS_ROOT` 指向一个包含以下子目录的目录：

- `flutter`（含 `bin\flutter.bat`）
- `rustup`（Rust 工具链）
- `cargo`（Cargo home）

例如，在 PowerShell 中：

```powershell
# 仅设置当前进程的环境变量，不写入用户或系统环境变量。
$env:AGENTTALK_TOOLS_ROOT = '<tools-root>'  # 例：不含真实本机路径的目录
& .\scripts\dev-env.ps1
```

若未设置 `AGENTTALK_TOOLS_ROOT`，`dev-env.ps1` 会给出错误信息并停止，不会猜测任何硬编码目录。

```powershell
Set-Location .
& .\scripts\dev-env.ps1
Set-Location .\agenttalk-next
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked
cargo test --workspace --locked
Set-Location .\apps\desktop_flutter
dart format --set-exit-if-changed lib test
dart analyze
flutter test
flutter build windows --release
```

日常增量候选使用 `agenttalk-next/scripts/build-windows-incremental.ps1`；不可变候选使用 `agenttalk-next/scripts/package-windows.ps1`。两者默认把输出写入 `AGENTTALK_STATE_ROOT`，不会覆盖旧 release。

要生成可从完整目录直接双击运行的 Windows flat bundle，使用 `agenttalk-next/scripts/build-windows-runnable-bundle.ps1 -Clean`。它把 `agenttalk_desktop.exe` 与 `agenttalk-core.exe` 放在同一目录，默认数据目录为 `%LOCALAPPDATA%\AgentTalk\data\`，不要求设置 Core 或数据库环境变量。

## Git 与敏感内容

默认分支是 `main`。禁止提交 `.env`、API key、token、Authorization、Cookie、数据库 dump/SQLite/WAL/SHM、backups、release、artifacts、node_modules、Rust target、Flutter build、用户附件和 Kun/Codex dataDir。历史重写必须保留项目外恢复点，并使用正向 allowlist。

## 当前状态

`agenttalk-next` 是当前开发和发布路径。本次收口只处理 Git/GitHub 边界，不自动进入 Backend、Flutter 或真实 Provider 功能开发。
