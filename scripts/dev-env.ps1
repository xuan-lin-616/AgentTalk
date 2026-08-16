[CmdletBinding()]
param(
  [string]$ToolsRoot = $(if ($env:AGENTTALK_TOOLS_ROOT) { $env:AGENTTALK_TOOLS_ROOT } else { throw 'AGENTTALK_TOOLS_ROOT is not set. Set it to the directory that contains flutter, rustup, and cargo.' })
)

$ErrorActionPreference = 'Stop'
$rustupHome = Join-Path $ToolsRoot 'rustup'
$cargoHome = Join-Path $ToolsRoot 'cargo'
$flutterRoot = Join-Path $ToolsRoot 'flutter'
$cmakeRoot = Join-Path $ToolsRoot 'AndroidSDK\cmake\3.22.1\bin'

$env:AGENTTALK_TOOLS_ROOT = $ToolsRoot
$env:RUSTUP_HOME = $rustupHome
$env:CARGO_HOME = $cargoHome
$env:PUB_CACHE = Join-Path $ToolsRoot 'pub-cache'
$env:Path = ((Join-Path $cargoHome 'bin'), (Join-Path $flutterRoot 'bin'), $cmakeRoot, $env:Path) -join ';'

Write-Output "AgentTalk tool environment configured for this PowerShell process."
Write-Output "ToolsRoot=$ToolsRoot"
Write-Output "RUSTUP_HOME=$env:RUSTUP_HOME"
Write-Output "CARGO_HOME=$env:CARGO_HOME"
Write-Output "PUB_CACHE=$env:PUB_CACHE"
Write-Output "Use .\scripts\toolchain-doctor.ps1 to collect read-only evidence."
