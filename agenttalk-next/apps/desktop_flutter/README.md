# AgentTalk Desktop Flutter

Windows Release 默认从 `agenttalk_desktop.exe` 所在目录加载同目录的
`agenttalk-core.exe`，并使用 `%LOCALAPPDATA%\AgentTalk\data\` 作为新版
canonical 数据目录。完整 flat bundle 的构建入口和生命周期约束见
`../../docs/refactor/windows-runnable-bundle.md`。
