[CmdletBinding()]
param(
  [string]$OutputRoot,
  [switch]$Clean
)

$ErrorActionPreference = 'Stop'
$repo = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$stateRoot = if ($env:AGENTTALK_STATE_ROOT) {
  [System.IO.Path]::GetFullPath($env:AGENTTALK_STATE_ROOT)
} else {
  [System.IO.Path]::GetFullPath((Join-Path $repo '..\AgentTalk-local-state'))
}
if ([string]::IsNullOrWhiteSpace($OutputRoot)) {
  $OutputRoot = Join-Path $stateRoot 'artifacts\dev\windows-x64'
}
$nextRoot = Join-Path $repo 'agenttalk-next'
$flutterRoot = Join-Path $nextRoot 'apps\desktop_flutter'
$toolsRoot = if ($env:AGENTTALK_TOOLS_ROOT) { $env:AGENTTALK_TOOLS_ROOT } else { throw 'AGENTTALK_TOOLS_ROOT is not set. Set it to the directory that contains flutter, rustup, and cargo.' }
$flutter = Join-Path $toolsRoot 'flutter\bin\flutter.bat'
$rustupHome = Join-Path $toolsRoot 'rustup'
$cargoHome = Join-Path $toolsRoot 'cargo'
$rustBin = Join-Path $rustupHome 'toolchains\stable-x86_64-pc-windows-msvc\bin'
$cargo = Join-Path $rustBin 'cargo.exe'
$target = 'x86_64-pc-windows-msvc'
$cargoTargetRoot = Join-Path $stateRoot 'temp\agenttalk-next\target'
$flutterBuildStateRoot = Join-Path $stateRoot 'temp\agenttalk-next\flutter-build'
$flutterBuildRoot = Join-Path $flutterRoot 'build'
$script:flutterBuildRestoreRequired = $false

function Get-TextSha256 {
  param([Parameter(Mandatory = $true)][string[]]$Lines)
  $payload = [System.Text.Encoding]::UTF8.GetBytes((($Lines -join "`n") + "`n"))
  $sha = [System.Security.Cryptography.SHA256]::Create()
  try { return ([BitConverter]::ToString($sha.ComputeHash($payload))).Replace('-', '').ToLowerInvariant() }
  finally { $sha.Dispose() }
}

function Test-IsCredentialLikePath {
  param([Parameter(Mandatory = $true)][string]$Path)
  return $Path -match '(^|[\\/])\.env($|[.\\/])' -or $Path -match '\.(pem|key|p12|pfx|kdbx)$'
}

function Get-SourceSnapshot {
  param([Parameter(Mandatory = $true)][string]$Root)
  $inputs = @(
    (Join-Path $Root 'Cargo.toml'),
    (Join-Path $Root 'Cargo.lock'),
    (Join-Path $Root 'crates'),
    (Join-Path $Root 'schemas'),
    (Join-Path $Root 'tests'),
    (Join-Path $Root 'fixtures'),
    (Join-Path $Root 'scripts'),
    (Join-Path $Root 'apps\agenttalk_core'),
    (Join-Path $Root 'apps\runtime_host'),
    (Join-Path $Root 'apps\desktop_flutter\pubspec.yaml'),
    (Join-Path $Root 'apps\desktop_flutter\pubspec.lock'),
    (Join-Path $Root 'apps\desktop_flutter\analysis_options.yaml'),
    (Join-Path $Root 'apps\desktop_flutter\lib'),
    (Join-Path $Root 'apps\desktop_flutter\test'),
    (Join-Path $Root 'apps\desktop_flutter\windows\CMakeLists.txt'),
    (Join-Path $Root 'apps\desktop_flutter\windows\runner')
  )
  $generated = '\\(build|target|\.dart_tool|ephemeral|\.git)(\\|$)'
  $files = @()
  foreach ($input in $inputs) {
    if (-not (Test-Path -LiteralPath $input)) { throw "Source input missing: $input" }
    if ((Get-Item -LiteralPath $input).PSIsContainer) {
      $files += @(Get-ChildItem -LiteralPath $input -Recurse -File -Force | Where-Object {
          $_.FullName -notmatch $generated -and -not (Test-IsCredentialLikePath $_.FullName)
        })
    } elseif (-not (Test-IsCredentialLikePath $input)) {
      $files += @(Get-Item -LiteralPath $input)
    }
  }
  $rootPath = (Resolve-Path -LiteralPath $Root).Path.TrimEnd('\')
  $records = @($files | Sort-Object FullName -Unique | ForEach-Object {
      $relative = $_.FullName.Substring($rootPath.Length + 1).Replace('\', '/')
      "$((Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant())  $relative"
    })
  [pscustomobject]@{ digest = Get-TextSha256 $records; fileCount = $records.Count }
}

function Sync-ChangedDirectory {
  param(
    [Parameter(Mandatory = $true)][string]$Source,
    [Parameter(Mandatory = $true)][string]$Destination
  )
  New-Item -ItemType Directory -Force -Path $Destination | Out-Null
  $sourceRoot = (Resolve-Path -LiteralPath $Source).Path.TrimEnd('\')
  $destinationRoot = [System.IO.Path]::GetFullPath($Destination).TrimEnd('\')
  $sourceFiles = @(Get-ChildItem -LiteralPath $sourceRoot -Recurse -File -Force)
  $sourceRelative = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
  $changed = 0
  foreach ($sourceFile in $sourceFiles) {
    $relative = $sourceFile.FullName.Substring($sourceRoot.Length + 1)
    $null = $sourceRelative.Add($relative)
    $destinationFile = Join-Path $destinationRoot $relative
    $parent = Split-Path -Parent $destinationFile
    New-Item -ItemType Directory -Force -Path $parent | Out-Null
    $needsCopy = -not (Test-Path -LiteralPath $destinationFile -PathType Leaf)
    if (-not $needsCopy) {
      $existing = Get-Item -LiteralPath $destinationFile
      $needsCopy = $existing.Length -ne $sourceFile.Length -or
        (Get-FileHash -LiteralPath $existing.FullName -Algorithm SHA256).Hash -ne
        (Get-FileHash -LiteralPath $sourceFile.FullName -Algorithm SHA256).Hash
    }
    if ($needsCopy) {
      $temporary = "$destinationFile.$([Guid]::NewGuid().ToString('N')).tmp"
      Copy-Item -LiteralPath $sourceFile.FullName -Destination $temporary -Force
      Move-Item -LiteralPath $temporary -Destination $destinationFile -Force
      $changed++
    }
  }
  $removed = 0
  $oldFiles = @(Get-ChildItem -LiteralPath $destinationRoot -Recurse -File -Force)
  foreach ($oldFile in $oldFiles) {
    $relative = $oldFile.FullName.Substring($destinationRoot.Length + 1)
    if (-not $sourceRelative.Contains($relative)) {
      Remove-Item -LiteralPath $oldFile.FullName -Force
      $removed++
    }
  }
  [pscustomobject]@{ changed = $changed; removed = $removed; sourceFiles = $sourceFiles.Count }
}

if (-not (Test-Path -LiteralPath $flutter)) { throw "Flutter tool not found at $flutter" }
if (-not (Test-Path -LiteralPath $cargo)) { throw "Cargo tool not found at $cargo" }
if (-not (Test-Path -LiteralPath $nextRoot)) { throw "agenttalk-next source root missing: $nextRoot" }

$resolvedOutput = if ([System.IO.Path]::IsPathRooted($OutputRoot)) {
  [System.IO.Path]::GetFullPath($OutputRoot)
} else {
  [System.IO.Path]::GetFullPath((Join-Path $repo $OutputRoot))
}
$lockPath = "$resolvedOutput.build.lock"
if (Test-Path -LiteralPath $lockPath) {
  throw "Incremental build output is locked by another or interrupted build: $lockPath"
}
New-Item -ItemType Directory -Force -Path $resolvedOutput | Out-Null
New-Item -ItemType File -Path $lockPath | Out-Null

$sourceBefore = Get-SourceSnapshot -Root $nextRoot
$startedUtc = [DateTime]::UtcNow
$oldErrorActionPreference = $ErrorActionPreference
$env:RUSTUP_HOME = $rustupHome
$env:CARGO_HOME = $cargoHome
$env:CARGO_TARGET_DIR = $cargoTargetRoot
$env:Path = "$rustBin;$cargoHome\bin;$env:Path"
try {
  Push-Location $nextRoot
  try {
    if ($Clean) {
      & $cargo clean --release --target $target
      if ($LASTEXITCODE -ne 0) { throw "cargo clean failed: $LASTEXITCODE" }
    }
    & $cargo build --release --target $target -p agenttalk-core
    if ($LASTEXITCODE -ne 0) { throw "cargo incremental release build failed: $LASTEXITCODE" }
  } finally { Pop-Location }

  Push-Location $flutterRoot
  try {
    if ((Test-Path -LiteralPath $flutterBuildRoot) -and (Test-Path -LiteralPath $flutterBuildStateRoot)) {
      throw "Flutter build trees exist in both source and state locations; inspect before continuing."
    }
    if (Test-Path -LiteralPath $flutterBuildStateRoot) {
      New-Item -ItemType Directory -Force -Path (Split-Path -Parent $flutterBuildRoot) | Out-Null
      Move-Item -LiteralPath $flutterBuildStateRoot -Destination $flutterBuildRoot
    }
    $script:flutterBuildRestoreRequired = $true
    if ($Clean) {
      & $flutter clean
      if ($LASTEXITCODE -ne 0) { throw "Flutter clean failed: $LASTEXITCODE" }
    }
    & $flutter build windows --release
    if ($LASTEXITCODE -ne 0) { throw "Flutter incremental Windows release build failed: $LASTEXITCODE" }
  } finally {
    Pop-Location
    if ($script:flutterBuildRestoreRequired -and (Test-Path -LiteralPath $flutterBuildRoot)) {
      if (Test-Path -LiteralPath $flutterBuildStateRoot) { throw "Flutter state build destination already exists during restore." }
      New-Item -ItemType Directory -Force -Path (Split-Path -Parent $flutterBuildStateRoot) | Out-Null
      Move-Item -LiteralPath $flutterBuildRoot -Destination $flutterBuildStateRoot
    }
  }

  $sourceAfter = Get-SourceSnapshot -Root $nextRoot
  if ($sourceBefore.digest -ne $sourceAfter.digest -or $sourceBefore.fileCount -ne $sourceAfter.fileCount) {
    throw "Source inputs changed during the incremental build; sync is blocked. before=$($sourceBefore.digest) after=$($sourceAfter.digest)"
  }
  $flutterRelease = Join-Path $flutterBuildStateRoot 'windows\x64\runner\Release'
  $coreExe = Join-Path $cargoTargetRoot "$target\release\agenttalk-core.exe"
  if (-not (Test-Path -LiteralPath $flutterRelease -PathType Container)) { throw "Flutter release directory missing" }
  if (-not (Test-Path -LiteralPath $coreExe -PathType Leaf)) { throw "Core release executable missing" }

  $appSync = Sync-ChangedDirectory -Source $flutterRelease -Destination (Join-Path $resolvedOutput 'app')
  $coreDirectory = Join-Path $resolvedOutput 'core'
  New-Item -ItemType Directory -Force -Path $coreDirectory, (Join-Path $resolvedOutput 'data') | Out-Null
  $coreDestination = Join-Path $coreDirectory 'agenttalk-core.exe'
  $coreChanged = $true
  if (Test-Path -LiteralPath $coreDestination -PathType Leaf) {
    $coreChanged = (Get-FileHash -LiteralPath $coreDestination -Algorithm SHA256).Hash -ne
      (Get-FileHash -LiteralPath $coreExe -Algorithm SHA256).Hash
  }
  if ($coreChanged) {
    $temporaryCore = "$coreDestination.$([Guid]::NewGuid().ToString('N')).tmp"
    Copy-Item -LiteralPath $coreExe -Destination $temporaryCore -Force
    Move-Item -LiteralPath $temporaryCore -Destination $coreDestination -Force
  }

  $launcher = @'
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$app = Join-Path $root 'app\agenttalk_desktop.exe'
$core = Join-Path $root 'core\agenttalk-core.exe'
$database = Join-Path $root 'data\agenttalk-core.sqlite3'
if (-not (Test-Path -LiteralPath $app)) { throw "app executable missing" }
if (-not (Test-Path -LiteralPath $core)) { throw "core executable missing" }
$env:AGENTTALK_CORE_EXECUTABLE = $core
$env:AGENTTALK_CORE_DATABASE = $database
$env:AGENTTALK_CORE_ARTIFACT_ROOT = Join-Path $root 'data\artifacts'
& $app
'@
  Set-Content -LiteralPath (Join-Path $resolvedOutput 'launch.ps1') -Value $launcher -Encoding utf8

  $payloadFiles = @(Get-ChildItem -LiteralPath $resolvedOutput -Recurse -File -Force |
    Where-Object { $_.FullName -ne $lockPath -and $_.Name -ne 'dev-manifest.json' } | Sort-Object FullName)
  $payloadRecords = @($payloadFiles | ForEach-Object {
      $relative = $_.FullName.Substring($resolvedOutput.Length + 1).Replace('\', '/')
      "$((Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant())  $relative"
    })
  $manifest = [ordered]@{
    schema = 1
    product = 'AgentTalk'
    mode = 'incremental-dev'
    output = $resolvedOutput
    clean = [bool]$Clean
    buildStartedUtc = $startedUtc.ToString('o')
    buildCompletedUtc = [DateTime]::UtcNow.ToString('o')
    source = [ordered]@{ snapshotSha256 = $sourceBefore.digest; fileCount = $sourceBefore.fileCount; unchangedDuringBuild = $true }
    synchronization = [ordered]@{ appChanged = $appSync.changed; appRemoved = $appSync.removed; appFiles = $appSync.sourceFiles; coreChanged = $coreChanged; payloadFileCount = $payloadFiles.Count }
    hashes = [ordered]@{ algorithm = 'SHA256'; records = $payloadRecords }
    nextRelease = 'Use package-windows.ps1 -Clean -Format both -MsixVersion <four-part-version> for an immutable candidate.'
  }
  $manifest | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath (Join-Path $resolvedOutput 'dev-manifest.json') -Encoding utf8
  Write-Output "Incremental Windows bundle synchronized at $resolvedOutput"
  Write-Output "Flutter files changed=$($appSync.changed), removed=$($appSync.removed); Core changed=$coreChanged"
} finally {
  if (Test-Path -LiteralPath $lockPath) { Remove-Item -LiteralPath $lockPath -Force }
  $ErrorActionPreference = $oldErrorActionPreference
}
