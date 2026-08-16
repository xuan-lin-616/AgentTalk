# AgentTalk 目录治理

- 一个产品、一个仓库；旧 Electron 只在 `legacy-electron/`，Flutter/Rust 只在 `agenttalk-next/`。
- GitHub 发布 allowlist 仅包含 `agenttalk-next/` 中的新版源码/配置/测试，以及 README、AGENTS、Git 配置、`.githooks/` 和服务新版工具链的根级脚本；旧版对照、迁移/候选报告和 parity fixtures 只保留本机或项目外状态目录。
- `/legacy-electron/` 是本机只读迁移参考，必须被忽略、不得暂存或上传；历史清理使用正向 allowlist/orphan baseline，禁止只过滤 `legacy-electron` 路径。
- 任何移动先更新 `restructure-move-manifest.json`，逐批使用绝对路径，验证源/目标和 SHA-256 后再进入下一批。
- 发现活动 ExecutionRun、未保存窗口、数据库写入或锁定文件时暂停移动；不得强杀用户会话、Kun、Codex 或 Docker/PostgreSQL。
- 不读取或输出 `.env`、密钥、token、Authorization、Cookie、数据库正文；敏感文件保持未跟踪并放在项目外状态目录。
- 构建输出、候选包、缓存和 SQLite 状态不得成为源码；不得创建第三套权威源码副本。
- 正式 migration、真实模型/Provider 调用、签名、发布、GitHub 创建和 push 都需要单独 Owner Gate；Owner 明确授权的本次 GitHub 收口仅允许创建私有 `AgentTalk`、添加 `origin` 并执行一次 `git push -u origin main`。
- 提交前执行 `git status --short`、secret scan、大文件检查、`git diff --cached --check`，只暂存授权路径。
