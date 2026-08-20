# Local ACP adapter manifests

Copy a manifest into `%LOCALAPPDATA%\AgentTalk\adapters\` and keep the `.json`
extension. AgentTalk reads this directory non-recursively during Core startup;
it never executes a manifest while scanning.

- `dsh.agenttalk-agent.json` models `node --import tsx ... --config ...`.
  Replace both `C:\path\to\deepseek-harness` placeholders with the absolute
  path to the tested checkout. `LX_API_KEY` is a credential slot name only;
  its value must remain in the parent process environment.
- `claude-code.agenttalk-agent.json` models a distribution that explicitly
  supports `claude --acp`. It must not be used for a Claude Code build whose
  own `--help` output does not advertise that flag.

Local manifests are passive match metadata, not executable trust. A PATH match
without an exact executable hash remains unable to verify; explicit file
selection retains the existing `UserSelected` identity gate.
