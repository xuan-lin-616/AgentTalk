[CmdletBinding()]
param(
  [string]$ToolsRoot = $(if ($env:AGENTTALK_TOOLS_ROOT) { $env:AGENTTALK_TOOLS_ROOT } else { throw 'AGENTTALK_TOOLS_ROOT is not set. Set it to the directory that contains flutter, rustup, and cargo.' }),
  [string]$OutputPath
)

$ErrorActionPreference = 'Continue'
$repo = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$stateRoot = if ($env:AGENTTALK_STATE_ROOT) {
  [System.IO.Path]::GetFullPath($env:AGENTTALK_STATE_ROOT)
} else {
  [System.IO.Path]::GetFullPath((Join-Path $repo '..\AgentTalk-local-state'))
}
if ([string]::IsNullOrWhiteSpace($OutputPath)) { $OutputPath = Join-Path $stateRoot 'artifacts\environment-doctor.json' }
$rustupHome = Join-Path $ToolsRoot 'rustup'
$cargoHome = Join-Path $ToolsRoot 'cargo'
$cmakePath = Join-Path $ToolsRoot 'AndroidSDK\cmake\3.22.1\bin\cmake.exe'
$checks = @()

function Add-ToolCheck([string]$Name, [string[]]$Candidates, [string[]]$VersionArgs = @('--version')) {
  $path = $null
  foreach ($candidate in $Candidates) {
    if ($candidate -and (Test-Path -LiteralPath $candidate)) { $path = (Resolve-Path -LiteralPath $candidate).Path; break }
    $command = Get-Command $candidate -ErrorAction SilentlyContinue
    if ($command) { $path = $command.Source; break }
  }
  $version = $null
  if ($path) {
    try { $version = (& $path @VersionArgs 2>&1 | Select-Object -First 1 | Out-String).Trim() } catch { $version = "ERROR: $($_.Exception.Message)" }
  }
  $script:checks += [ordered]@{ name = $Name; path = $path; version = $version; available = [bool]$path }
}

Add-ToolCheck 'node' @('node')
Add-ToolCheck 'npm' @('npm')
Add-ToolCheck 'flutter' @((Join-Path $ToolsRoot 'flutter\bin\flutter.bat'), 'flutter')
Add-ToolCheck 'dart' @((Join-Path $ToolsRoot 'flutter\bin\dart.bat'), 'dart')
Add-ToolCheck 'rustup' @((Join-Path $cargoHome 'bin\rustup.exe'), 'rustup')
Add-ToolCheck 'rustc' @((Join-Path $cargoHome 'bin\rustc.exe'), 'rustc')
Add-ToolCheck 'cargo' @((Join-Path $cargoHome 'bin\cargo.exe'), 'cargo')
Add-ToolCheck 'cmake' @($cmakePath, 'cmake')
Add-ToolCheck 'ninja' @((Join-Path $ToolsRoot 'AndroidSDK\cmake\3.22.1\bin\ninja.exe'), 'ninja')
Add-ToolCheck 'sqlite3' @('sqlite3')
Add-ToolCheck 'docker' @('docker')
Add-ToolCheck 'git' @('git')

$result = [ordered]@{
  generatedAt = (Get-Date).ToUniversalTime().ToString('o')
  toolsRoot = $ToolsRoot
  rustupHome = $rustupHome
  cargoHome = $cargoHome
  checks = $checks
  notes = @(
    'Read-only tool discovery; this script does not install, stop, or modify services.',
    'PostgreSQL CLI may be inside the Docker container; inspect the container separately.',
    'Availability does not prove runtime protocol or real-provider readiness.'
  )
}

$parent = Split-Path -Parent $OutputPath
if ($parent) { New-Item -ItemType Directory -Force $parent | Out-Null }
$result | ConvertTo-Json -Depth 6 | Set-Content -Encoding UTF8 -LiteralPath $OutputPath
$result | ConvertTo-Json -Depth 6
