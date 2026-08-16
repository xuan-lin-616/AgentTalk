[CmdletBinding()]
param(
  [string]$OutputRoot,
  [switch]$Clean,
  [string]$ExpectedBranch,
  [string]$ExpectedGitSha
)

$ErrorActionPreference = 'Stop'
$repo = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$expectedSchemaSha = '362D8D8DAFCCB26062F9A4BA9D37062C395E9FEDE322CD6CCA79B36482F6BFDE'
$stateRoot = if ($env:AGENTTALK_STATE_ROOT) {
  [System.IO.Path]::GetFullPath($env:AGENTTALK_STATE_ROOT)
} else {
  [System.IO.Path]::GetFullPath((Join-Path $repo '..\AgentTalk-local-state'))
}
if ([string]::IsNullOrWhiteSpace($OutputRoot)) {
  $OutputRoot = Join-Path $stateRoot 'artifacts\windows-runnable-v1\AgentTalk'
}
$resolvedOutput = [System.IO.Path]::GetFullPath($OutputRoot)
$nextRoot = Join-Path $repo 'agenttalk-next'
$flutterRoot = Join-Path $nextRoot 'apps\desktop_flutter'
$toolsRoot = if ($env:AGENTTALK_TOOLS_ROOT) { $env:AGENTTALK_TOOLS_ROOT } else { throw 'AGENTTALK_TOOLS_ROOT is not set. Set it to the directory that contains flutter, rustup, and cargo.' }
$flutter = Join-Path $toolsRoot 'flutter\bin\flutter.bat'
$rustupHome = Join-Path $toolsRoot 'rustup'
$cargoHome = Join-Path $toolsRoot 'cargo'
$rustBin = Join-Path $rustupHome 'toolchains\stable-x86_64-pc-windows-msvc\bin'
$cargo = Join-Path $rustBin 'cargo.exe'
$target = 'x86_64-pc-windows-msvc'
$cargoTargetRoot = Join-Path $stateRoot 'temp\agenttalk-next\runnable-v1\target'
$flutterBuildStateRoot = Join-Path $stateRoot 'temp\agenttalk-next\runnable-v1\flutter-build'
$flutterBuildRoot = Join-Path $flutterRoot 'build'
$script:flutterBuildRestoreRequired = $false

function Invoke-GitText {
  param([Parameter(Mandatory = $true)][string[]]$Arguments)
  $value = & git -C $repo @Arguments
  if ($LASTEXITCODE -ne 0) { throw "git command failed: git -C $repo $($Arguments -join ' ')" }
  return ($value -join "`n").Trim()
}

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
    (Join-Path $Root 'fixtures'),
    (Join-Path $Root 'apps\agenttalk_core'),
    (Join-Path $Root 'apps\runtime_host'),
    (Join-Path $Root 'apps\desktop_flutter\pubspec.yaml'),
    (Join-Path $Root 'apps\desktop_flutter\pubspec.lock'),
    (Join-Path $Root 'apps\desktop_flutter\analysis_options.yaml'),
    (Join-Path $Root 'apps\desktop_flutter\lib'),
    (Join-Path $Root 'apps\desktop_flutter\test'),
    (Join-Path $Root 'apps\desktop_flutter\windows\CMakeLists.txt'),
    (Join-Path $Root 'apps\desktop_flutter\windows\runner'),
    (Join-Path $Root 'scripts')
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

if (-not (Get-Command git -ErrorAction SilentlyContinue)) { throw 'git executable is required' }
if (-not (Test-Path -LiteralPath $flutter -PathType Leaf)) { throw "Flutter tool not found: $flutter" }
if (-not (Test-Path -LiteralPath $cargo -PathType Leaf)) { throw "Cargo tool not found: $cargo" }
if (-not (Test-Path -LiteralPath $nextRoot -PathType Container)) { throw "Source root missing: $nextRoot" }

$branch = Invoke-GitText @('branch', '--show-current')
if (-not [string]::IsNullOrWhiteSpace($ExpectedBranch) -and $branch -ne $ExpectedBranch) {
  throw "Runnable bundle branch constraint failed: expected $ExpectedBranch, current branch is $branch"
}
$status = Invoke-GitText @('status', '--porcelain', '--untracked-files=all')
if (-not [string]::IsNullOrWhiteSpace($status)) { throw 'Git worktree must be clean before building the runnable bundle' }
$sourceSha = Invoke-GitText @('rev-parse', 'HEAD')
if (-not [string]::IsNullOrWhiteSpace($ExpectedGitSha) -and $sourceSha -ne $ExpectedGitSha) {
  throw "Runnable bundle Git SHA constraint failed: expected $ExpectedGitSha, current SHA is $sourceSha"
}
$schemaPath = Join-Path $nextRoot 'schemas\ipc\v1\protocol.schema.json'
$schemaSha = (Get-FileHash -LiteralPath $schemaPath -Algorithm SHA256).Hash.ToUpperInvariant()
if ($schemaSha -ne $expectedSchemaSha) { throw "IPC Schema SHA changed: expected $expectedSchemaSha, actual $schemaSha" }
$sourceBefore = Get-SourceSnapshot -Root $nextRoot
$buildStartedUtc = [DateTime]::UtcNow

if (Test-Path -LiteralPath $resolvedOutput) { throw "Output already exists; choose a new immutable output directory: $resolvedOutput" }
$staging = Join-Path (Split-Path -Parent $resolvedOutput) ('.AgentTalk-staging-' + [Guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Force -Path $staging | Out-Null
$oldTarget = $env:CARGO_TARGET_DIR
$oldRustupHome = $env:RUSTUP_HOME
$oldCargoHome = $env:CARGO_HOME
$oldPath = $env:Path
$env:RUSTUP_HOME = $rustupHome
$env:CARGO_HOME = $cargoHome
$env:CARGO_TARGET_DIR = $cargoTargetRoot
$env:Path = "$rustBin;$cargoHome\bin;$oldPath"
try {
  Push-Location $nextRoot
  try {
    if ($Clean) {
      & $cargo clean --release --target $target
      if ($LASTEXITCODE -ne 0) { throw "cargo clean failed: $LASTEXITCODE" }
    }
    & $cargo build --workspace --release --locked --target $target -p agenttalk-core
    if ($LASTEXITCODE -ne 0) { throw "Rust Core release build failed: $LASTEXITCODE" }
  } finally { Pop-Location }

  Push-Location $flutterRoot
  try {
    if ((Test-Path -LiteralPath $flutterBuildRoot) -and (Test-Path -LiteralPath $flutterBuildStateRoot)) {
      throw 'Flutter build exists in both source and state locations; inspect before continuing.'
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
    if ($LASTEXITCODE -ne 0) { throw "Flutter Windows release build failed: $LASTEXITCODE" }
  } finally {
    Pop-Location
    if ($script:flutterBuildRestoreRequired -and (Test-Path -LiteralPath $flutterBuildRoot)) {
      if (Test-Path -LiteralPath $flutterBuildStateRoot) { throw 'Flutter state build destination already exists during restore.' }
      New-Item -ItemType Directory -Force -Path (Split-Path -Parent $flutterBuildStateRoot) | Out-Null
      Move-Item -LiteralPath $flutterBuildRoot -Destination $flutterBuildStateRoot
    }
  }

  $sourceAfter = Get-SourceSnapshot -Root $nextRoot
  $headAfter = Invoke-GitText @('rev-parse', 'HEAD')
  $statusAfter = Invoke-GitText @('status', '--porcelain', '--untracked-files=all')
  if ($sourceBefore.digest -ne $sourceAfter.digest -or $sourceBefore.fileCount -ne $sourceAfter.fileCount) {
    throw 'Source inputs changed during build; bundle creation is blocked.'
  }
  if ($headAfter -ne $sourceSha -or -not [string]::IsNullOrWhiteSpace($statusAfter)) {
    throw 'Git source identity or clean state changed during build; bundle creation is blocked.'
  }

  $flutterRelease = Join-Path $flutterBuildStateRoot 'windows\x64\runner\Release'
  $coreExe = Join-Path $cargoTargetRoot "$target\release\agenttalk-core.exe"
  $workerExe = Join-Path $cargoTargetRoot "$target\release\agenttalk-local-discovery-worker.exe"
  $flutterExe = Join-Path $flutterRelease 'agenttalk_desktop.exe'
  foreach ($required in @($flutterRelease, $flutterExe, $coreExe, $workerExe, (Join-Path $flutterRelease 'flutter_windows.dll'), (Join-Path $flutterRelease 'data'), (Join-Path $flutterRelease 'data\flutter_assets'))) {
    if (-not (Test-Path -LiteralPath $required)) { throw "Required build output missing: $required" }
  }
  Copy-Item -Path (Join-Path $flutterRelease '*') -Destination $staging -Recurse -Force
  Copy-Item -LiteralPath $coreExe -Destination (Join-Path $staging 'agenttalk-core.exe') -Force
  Copy-Item -LiteralPath $workerExe -Destination (Join-Path $staging 'agenttalk-local-discovery-worker.exe') -Force

  $stagingFiles = @(Get-ChildItem -LiteralPath $staging -Recurse -File | Sort-Object FullName)
  $fileHashes = [ordered]@{}
  foreach ($file in $stagingFiles) {
    $relative = $file.FullName.Substring($staging.Length + 1).Replace('\', '/')
    $fileHashes[$relative] = (Get-FileHash -LiteralPath $file.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
  }
  foreach ($requiredFile in @('agenttalk_desktop.exe', 'agenttalk-core.exe', 'agenttalk-local-discovery-worker.exe')) {
    if (-not $fileHashes.Contains($requiredFile)) { throw "Bundle file missing: $requiredFile" }
  }
  $manifest = [ordered]@{
    schema = 1
    product = 'AgentTalk'
    architecture = 'windows-x64'
    bundleLayout = 'flat-root'
    buildStartedUtc = $buildStartedUtc.ToString('o')
    buildCompletedUtc = [DateTime]::UtcNow.ToString('o')
    source = [ordered]@{
      gitSha = $sourceSha
      branch = $branch
      sourceSnapshotSha256 = $sourceBefore.digest
      sourceFileCount = $sourceBefore.fileCount
      ipcSchemaSha256 = $schemaSha
      unchangedDuringBuild = $true
    }
    runtime = [ordered]@{
      coreExecutable = 'agenttalk-core.exe'
      workerExecutable = 'agenttalk-local-discovery-worker.exe'
      dataDirectory = '%LOCALAPPDATA%\AgentTalk\data'
      launchMode = 'owned-by-desktop'
      environmentOverrides = @('AGENTTALK_CORE_EXECUTABLE', 'AGENTTALK_CORE_DATABASE', 'AGENTTALK_DATA_ROOT', 'AGENTTALK_CORE_MODE')
    }
    files = $fileHashes
  }
  $manifest | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath (Join-Path $staging 'agenttalk-bundle.manifest.json') -Encoding utf8

  $manifestCheck = Get-Content -Raw -LiteralPath (Join-Path $staging 'agenttalk-bundle.manifest.json') | ConvertFrom-Json
  if ($manifestCheck.source.gitSha -ne $sourceSha -or $manifestCheck.files.'agenttalk-core.exe' -ne $fileHashes['agenttalk-core.exe']) {
    throw 'Generated bundle manifest failed self-check.'
  }
  New-Item -ItemType Directory -Force -Path (Split-Path -Parent $resolvedOutput) | Out-Null
  Move-Item -LiteralPath $staging -Destination $resolvedOutput
  $staging = $null
  Write-Output "Runnable Windows bundle prepared at $resolvedOutput"
  Write-Output "Git SHA: $sourceSha"
  Write-Output "Flutter: $(Join-Path $resolvedOutput 'agenttalk_desktop.exe')"
  Write-Output "Core: $(Join-Path $resolvedOutput 'agenttalk-core.exe')"
} finally {
  $env:CARGO_TARGET_DIR = $oldTarget
  $env:RUSTUP_HOME = $oldRustupHome
  $env:CARGO_HOME = $oldCargoHome
  $env:Path = $oldPath
  if ($null -ne $staging -and (Test-Path -LiteralPath $staging)) {
    Remove-Item -LiteralPath $staging -Recurse -Force
  }
}
