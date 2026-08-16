# AgentTalk 工具链锁定记录

本文件只记录可复现的工具版本和安装位置，不记录凭据、环境变量值或用户私有路径之外的 secret。

## 已验证版本（2026-08-05）

| 工具 | 版本 | 来源/位置 | 状态 |
|---|---|---|---|
| Windows | 11 / 10.0.26200 | 系统 | PASS |
| PowerShell | 7.6.3；Windows PowerShell 5.1 | 系统 | PASS |
| Git | 2.54.0.windows.1 | Git for Windows | PASS，工作区不是 Git 仓库 |
| Docker | CLI/Server 29.6.2 | Docker Desktop | PASS |
| Node.js | v24.16.0 | Node.js 安装目录 | PASS |
| npm | 11.13.0 | Node.js | PASS |
| Flutter | 3.44.7 stable | `AGENTTALK_TOOLS_ROOT\flutter` | PASS |
| Dart | 3.12.2 | Flutter SDK | PASS |
| Rust | 1.97.1 stable MSVC | `AGENTTALK_TOOLS_ROOT\rustup` + `AGENTTALK_TOOLS_ROOT\cargo` | PASS |
| Cargo | 1.97.1 | `AGENTTALK_TOOLS_ROOT\cargo` | PASS |
| Rust target | x86_64-pc-windows-msvc | rustup | PASS |
| rustfmt/clippy | stable components | rustup | PASS |
| CMake | 3.22.1 | Android SDK CMake | PASS，未默认进 PATH |
| Ninja | 1.13.0 | Ninja 安装目录 | PASS |
| SQLite CLI | 3.50.6 | Android SDK platform-tools | PASS |
| MSVC/Windows SDK | MSVC 14.44.35207 / SDK 10.0.26100.0 | Visual Studio Build Tools | PASS |

## 隔离环境

执行 `.\scripts\dev-env.ps1` 只为当前 PowerShell 设置 `AGENTTALK_TOOLS_ROOT`、`RUSTUP_HOME`、`CARGO_HOME`、`PUB_CACHE` 和 PATH，不修改用户永久 PATH、HOME、CODEX_HOME 或 Kun dataDir。

执行 `.\scripts\toolchain-doctor.ps1` 可生成 `AGENTTALK_STATE_ROOT\artifacts\environment-doctor.json`（默认位于项目同级状态目录）。该报告只证明工具发现和版本，不证明真实 Runtime/Provider 协议或模型调用。

## 安装记录

- 官方 Rustup 安装器：`rustup-init-x86_64-pc-windows-msvc.exe`
- SHA-256：`86478E53F769379D7F0EBFA7C9AA97CB76CA92233F79AA2CC0DBEE2EFAAC73C7`
- 安装器保存在 `AGENTTALK_TOOLS_ROOT\installers`；Rust 工具链位于 `AGENTTALK_TOOLS_ROOT\rustup` 与 `AGENTTALK_TOOLS_ROOT\cargo`。
