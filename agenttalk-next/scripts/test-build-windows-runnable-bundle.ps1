[CmdletBinding()]
param(
  [string]$ScriptPath = (Join-Path $PSScriptRoot 'build-windows-runnable-bundle.ps1')
)

$ErrorActionPreference = 'Stop'
$scriptText = Get-Content -Raw -LiteralPath (Resolve-Path -LiteralPath $ScriptPath)

if ($scriptText -match '\$expectedBranch\s*=') {
  throw 'Runnable bundle script still contains a hard-coded expected branch variable.'
}
if ($scriptText -notmatch '\[string\]\$ExpectedBranch') {
  throw 'Runnable bundle script does not expose ExpectedBranch as an optional constraint.'
}
if ($scriptText -notmatch '\[string\]\$ExpectedGitSha') {
  throw 'Runnable bundle script does not expose ExpectedGitSha as an optional constraint.'
}
if ($scriptText -notmatch 'branch constraint failed') {
  throw 'Runnable bundle script does not validate an explicitly requested branch constraint.'
}
if ($scriptText -notmatch 'Git SHA constraint failed') {
  throw 'Runnable bundle script does not validate an explicitly requested Git SHA constraint.'
}
if ($scriptText -notmatch 'diff --quiet --no-ext-diff') {
  throw 'Runnable bundle script does not use content-aware tracked-file checks.'
}
if ($scriptText -notmatch 'ls-files --others --exclude-standard') {
  throw 'Runnable bundle script does not reject untracked files.'
}
if ($scriptText -notmatch "dataDirectory = '%LOCALAPPDATA%\\AgentTalk\\data'") {
  throw 'Runnable bundle manifest dataDirectory is not represented with single path separators.'
}

Write-Output 'build-windows-runnable-bundle.ps1 parameterization regression: PASS'
