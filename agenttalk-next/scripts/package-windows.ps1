[CmdletBinding()]
param(
  [string]$Version = '0.1.0-local',
  [string]$OutputRoot,
  [string]$BuildId,
  [ValidateSet('portable', 'msix', 'both')]
  [string]$Format = 'portable',
  [string]$MsixOutputRoot,
  [string]$MsixVersion = '0.1.0.0',
  [switch]$Clean
)

$ErrorActionPreference = 'Stop'
$repo = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$stateRoot = if ($env:AGENTTALK_STATE_ROOT) {
  [System.IO.Path]::GetFullPath($env:AGENTTALK_STATE_ROOT)
} else {
  [System.IO.Path]::GetFullPath((Join-Path $repo '..\AgentTalk-local-state'))
}
if ([string]::IsNullOrWhiteSpace($OutputRoot)) { $OutputRoot = Join-Path $stateRoot 'artifacts\candidates' }
if ([string]::IsNullOrWhiteSpace($MsixOutputRoot)) { $MsixOutputRoot = Join-Path $stateRoot 'artifacts\installers' }
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
  param(
    [Parameter(Mandatory = $true)]
    [string[]]$Lines
  )

  $payload = [System.Text.Encoding]::UTF8.GetBytes((($Lines -join "`n") + "`n"))
  $sha = [System.Security.Cryptography.SHA256]::Create()
  try {
    return ([BitConverter]::ToString($sha.ComputeHash($payload))).Replace('-', '').ToLowerInvariant()
  } finally {
    $sha.Dispose()
  }
}

function Test-IsCredentialLikePath {
  param(
    [Parameter(Mandatory = $true)]
    [string]$Path
  )

  return $Path -match '(^|[\\/])\.env($|[.\\/])' -or
    $Path -match '\.(pem|key|p12|pfx|kdbx)$'
}

function Get-FileInventoryDigest {
  param(
    [Parameter(Mandatory = $true)]
    [string]$Root
  )

  $rootPath = (Resolve-Path -LiteralPath $Root).Path.TrimEnd('\')
  $allFiles = @(Get-ChildItem -LiteralPath $rootPath -Recurse -File -Force | Sort-Object FullName)
  $credentialLikeFiles = @($allFiles | Where-Object { Test-IsCredentialLikePath -Path $_.FullName })
  $files = @($allFiles | Where-Object { -not (Test-IsCredentialLikePath -Path $_.FullName) })
  $records = @(
    $files | ForEach-Object {
      $relative = $_.FullName.Substring($rootPath.Length + 1).Replace('\', '/')
      "$((Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant())  $relative"
    }
  )
  $records += @(
    $credentialLikeFiles | ForEach-Object {
      $relative = $_.FullName.Substring($rootPath.Length + 1).Replace('\', '/')
      "[credential-file-excluded] $relative $($_.Length) $($_.LastWriteTimeUtc.ToString('o'))"
    }
  )
  $totalBytes = ($files | Measure-Object -Property Length -Sum).Sum
  if ($null -eq $totalBytes) { $totalBytes = 0 }

  [pscustomobject]@{
    digest     = Get-TextSha256 -Lines $records
    fileCount  = $records.Count
    totalBytes = [int64]$totalBytes
  }
}

function Get-SourceSnapshot {
  param(
    [Parameter(Mandatory = $true)]
    [string]$Root
  )

  $sourceInputs = @(
    (Join-Path $Root 'Cargo.toml'),
    (Join-Path $Root 'Cargo.lock'),
    (Join-Path $Root 'crates'),
    (Join-Path $Root 'schemas'),
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
  $excludedGeneratedPath = '\\(build|target|\.dart_tool|ephemeral|\.git)(\\|$)'
  $files = @()

  foreach ($input in $sourceInputs) {
    if (-not (Test-Path -LiteralPath $input)) {
      throw "Source input missing: $input"
    }

    if ((Get-Item -LiteralPath $input).PSIsContainer) {
      $files += @(Get-ChildItem -LiteralPath $input -Recurse -File -Force | Where-Object {
          $_.FullName -notmatch $excludedGeneratedPath -and
          -not (Test-IsCredentialLikePath -Path $_.FullName)
        })
    } else {
      if (-not (Test-IsCredentialLikePath -Path $input)) {
        $files += @(Get-Item -LiteralPath $input)
      }
    }
  }

  $rootPath = (Resolve-Path -LiteralPath $Root).Path.TrimEnd('\')
  $records = @(
    $files | Sort-Object FullName -Unique | ForEach-Object {
      $relative = $_.FullName.Substring($rootPath.Length + 1).Replace('\', '/')
      "$((Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant())  $relative"
    }
  )

  [pscustomobject]@{
    digest    = Get-TextSha256 -Lines $records
    fileCount = $records.Count
  }
}

function Test-IsSameOrChildPath {
  param(
    [Parameter(Mandatory = $true)]
    [string]$Path,
    [Parameter(Mandatory = $true)]
    [string]$Parent
  )

  $pathFull = [System.IO.Path]::GetFullPath($Path).TrimEnd('\')
  $parentFull = [System.IO.Path]::GetFullPath($Parent).TrimEnd('\')
  return $pathFull.Equals($parentFull, [System.StringComparison]::OrdinalIgnoreCase) -or
    $pathFull.StartsWith($parentFull + '\', [System.StringComparison]::OrdinalIgnoreCase)
}

function Resolve-MakeAppx {
  $windowsKitRoot = Join-Path ${env:ProgramFiles(x86)} 'Windows Kits\10\bin'
  if (-not (Test-Path -LiteralPath $windowsKitRoot -PathType Container)) {
    throw "Windows SDK bin directory not found: $windowsKitRoot"
  }
  $tool = Get-ChildItem -LiteralPath $windowsKitRoot -Recurse -Filter 'makeappx.exe' -File |
    Where-Object { $_.FullName -match '\\x64\\makeappx\.exe$' } |
    Sort-Object FullName -Descending |
    Select-Object -First 1
  if ($null -eq $tool) {
    throw 'Windows SDK x64 makeappx.exe was not found'
  }
  return $tool.FullName
}

function Convert-ToMsixVersion {
  param(
    [Parameter(Mandatory = $true)]
    [string]$Value
  )

  if ($Value -notmatch '^\d+\.\d+\.\d+\.\d+$') {
    throw "MsixVersion must contain four numeric components, for example 0.1.0.1: $Value"
  }
  return $Value
}

function New-MsixAssets {
  param(
    [Parameter(Mandatory = $true)]
    [string]$Destination,
    [Parameter(Mandatory = $true)]
    [string]$IconPath
  )

  New-Item -ItemType Directory -Force -Path $Destination | Out-Null
  Add-Type -AssemblyName System.Drawing
  $icon = [System.Drawing.Icon]::ExtractAssociatedIcon($IconPath)
  if ($null -eq $icon) { throw "Unable to extract application icon: $IconPath" }
  $source = $icon.ToBitmap()
  try {
    foreach ($size in @(44, 150, 310)) {
      $bitmap = [System.Drawing.Bitmap]::new($size, $size)
      $graphics = [System.Drawing.Graphics]::FromImage($bitmap)
      try {
        $graphics.Clear([System.Drawing.Color]::Transparent)
        $graphics.InterpolationMode = [System.Drawing.Drawing2D.InterpolationMode]::HighQualityBicubic
        $graphics.DrawImage($source, 0, 0, $size, $size)
        $name = switch ($size) {
          44 { 'Square44x44Logo.png' }
          150 { 'Square150x150Logo.png' }
          310 { 'StoreLogo.png' }
        }
        $bitmap.Save((Join-Path $Destination $name), [System.Drawing.Imaging.ImageFormat]::Png)
      } finally {
        $graphics.Dispose()
        $bitmap.Dispose()
      }
    }
  } finally {
    $source.Dispose()
    $icon.Dispose()
  }
}

function New-MsixPackage {
  param(
    [Parameter(Mandatory = $true)]
    [string]$CandidateRoot,
    [Parameter(Mandatory = $true)]
    [string]$OutputRoot,
    [Parameter(Mandatory = $true)]
    [string]$PackageVersion,
    [Parameter(Mandatory = $true)]
    [string]$BuildId
  )

  $makeAppx = Resolve-MakeAppx
  $resolvedRoot = if ([System.IO.Path]::IsPathRooted($OutputRoot)) {
    [System.IO.Path]::GetFullPath($OutputRoot)
  } else {
    [System.IO.Path]::GetFullPath((Join-Path $repo $OutputRoot))
  }
  foreach ($protectedReleaseRoot in $protectedReleaseRoots) {
    if (Test-IsSameOrChildPath -Path $resolvedRoot -Parent $protectedReleaseRoot) {
      throw "MsixOutputRoot must not be the old release or a child of it: $resolvedRoot"
    }
  }

  $msixVersionValue = Convert-ToMsixVersion -Value $PackageVersion
  $packageName = "AgentTalk.Local_${msixVersionValue}_x64.msix"
  $msixPath = Join-Path $resolvedRoot $packageName
  $staging = Join-Path $resolvedRoot (".msix-staging-$BuildId")
  if (Test-Path -LiteralPath $msixPath) {
    throw "MSIX output already exists; refusing to overwrite it: $msixPath"
  }
  if (Test-Path -LiteralPath $staging) {
    throw "MSIX staging already exists; inspect it before reusing this BuildId: $staging"
  }

  New-Item -ItemType Directory -Force -Path $resolvedRoot, $staging | Out-Null
  try {
    Copy-Item -LiteralPath (Join-Path $CandidateRoot 'app') -Destination $staging -Recurse
    Copy-Item -LiteralPath (Join-Path $CandidateRoot 'core') -Destination $staging -Recurse
    $assets = Join-Path $staging 'Assets'
    New-MsixAssets -Destination $assets -IconPath (Join-Path $nextRoot 'apps\desktop_flutter\windows\runner\resources\app_icon.ico')
    $manifest = @"
<?xml version="1.0" encoding="utf-8"?>
<Package xmlns="http://schemas.microsoft.com/appx/manifest/foundation/windows10"
         xmlns:uap="http://schemas.microsoft.com/appx/manifest/uap/windows10"
         xmlns:rescap="http://schemas.microsoft.com/appx/manifest/foundation/windows10/restrictedcapabilities"
         IgnorableNamespaces="uap rescap">
  <Identity Name="AgentTalk.Local" Publisher="CN=AgentTalk Local" Version="$msixVersionValue" ProcessorArchitecture="x64" />
  <Properties>
    <DisplayName>AgentTalk</DisplayName>
    <PublisherDisplayName>AgentTalk Local</PublisherDisplayName>
    <Description>AgentTalk Flutter Desktop local candidate</Description>
    <Logo>Assets\StoreLogo.png</Logo>
  </Properties>
  <Resources>
    <Resource Language="zh-CN" />
  </Resources>
  <Dependencies>
    <TargetDeviceFamily Name="Windows.Desktop" MinVersion="10.0.17763.0" MaxVersionTested="10.0.26100.0" />
  </Dependencies>
  <Applications>
    <Application Id="AgentTalk" Executable="app\agenttalk_desktop.exe" EntryPoint="Windows.FullTrustApplication">
      <uap:VisualElements DisplayName="AgentTalk" Description="AgentTalk Flutter Desktop" BackgroundColor="#F8FAFC" Square44x44Logo="Assets\Square44x44Logo.png" Square150x150Logo="Assets\Square150x150Logo.png" />
    </Application>
  </Applications>
  <Capabilities>
    <rescap:Capability Name="runFullTrust" />
  </Capabilities>
</Package>
"@
    Set-Content -LiteralPath (Join-Path $staging 'AppxManifest.xml') -Value $manifest -Encoding utf8
    & $makeAppx pack /d $staging /p $msixPath /o
    if ($LASTEXITCODE -ne 0) { throw "makeappx pack failed: $LASTEXITCODE" }

    $hash = (Get-FileHash -LiteralPath $msixPath -Algorithm SHA256).Hash.ToLowerInvariant()
    $metadata = [ordered]@{
      schema = 1
      package = $packageName
      version = $msixVersionValue
      architecture = 'x64'
      compression = 'MSIX block-map/package compression'
      signed = $false
      signing = 'Owner-Gate/PENDING'
      sha256 = $hash
      bytes = (Get-Item -LiteralPath $msixPath).Length
      buildId = $BuildId
      updateModel = 'Windows App Installer/MSIX block-level update; publish a higher four-part version'
    }
    $metadata | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $resolvedRoot "$packageName.manifest.json") -Encoding utf8
    return [pscustomobject]@{
      path = $msixPath
      manifest = (Join-Path $resolvedRoot "$packageName.manifest.json")
      sha256 = $hash
      bytes = (Get-Item -LiteralPath $msixPath).Length
    }
  } finally {
    if (Test-Path -LiteralPath $staging) {
      Remove-Item -LiteralPath $staging -Recurse -Force
    }
  }
}

if ($Version -notmatch '^[A-Za-z0-9][A-Za-z0-9._-]*$') {
  throw "Version contains unsupported path characters: $Version"
}
if (-not [string]::IsNullOrWhiteSpace($BuildId) -and $BuildId -notmatch '^[A-Za-z0-9][A-Za-z0-9._-]*$') {
  throw "BuildId contains unsupported path characters: $BuildId"
}
if ($MsixVersion -notmatch '^\d+\.\d+\.\d+\.\d+$') {
  throw "MsixVersion must contain four numeric components: $MsixVersion"
}

$resolvedOutputRoot = if ([System.IO.Path]::IsPathRooted($OutputRoot)) {
  [System.IO.Path]::GetFullPath($OutputRoot)
} else {
  [System.IO.Path]::GetFullPath((Join-Path $repo $OutputRoot))
}
$protectedReleaseRoots = @(
  (Join-Path $stateRoot 'releases\release'),
  (Join-Path $stateRoot 'releases\release-roster-fix'),
  (Join-Path $stateRoot 'releases\release-tool-fix')
)
foreach ($protectedReleaseRoot in $protectedReleaseRoots) {
  if (Test-IsSameOrChildPath -Path $resolvedOutputRoot -Parent $protectedReleaseRoot) {
    throw "OutputRoot must not be the old release or a child of it: $resolvedOutputRoot"
  }
}

$env:RUSTUP_HOME = $rustupHome
$env:CARGO_HOME = $cargoHome
$env:CARGO_TARGET_DIR = $cargoTargetRoot
$env:Path = "$rustBin;$cargoHome\bin;$env:Path"

if (-not (Test-Path -LiteralPath $flutter)) { throw "Flutter tool not found at $flutter" }
if (-not (Test-Path -LiteralPath $cargo)) { throw "Cargo tool not found at $cargo" }
if (-not (Test-Path -LiteralPath $nextRoot)) { throw "agenttalk-next source root missing: $nextRoot" }

$sourceCapturedBeforeUtc = [DateTime]::UtcNow.ToString('o')
$sourceBefore = Get-SourceSnapshot -Root $nextRoot
$sourceShortHash = $sourceBefore.digest.Substring(0, 12)
$generatedBuildId = [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssfffZ')
if ([string]::IsNullOrWhiteSpace($BuildId)) {
  $BuildId = "$generatedBuildId-$sourceShortHash"
}
$candidateName = "agenttalk-next-windows-$Version-$BuildId"
$candidate = Join-Path $resolvedOutputRoot $candidateName
$staging = "$candidate.staging"

if (Test-Path -LiteralPath $candidate) {
  throw "Candidate output already exists; refusing to overwrite it: $candidate"
}
if (Test-Path -LiteralPath $staging) {
  throw "Staging output already exists; inspect it before reusing this BuildId: $staging"
}

$protectedReleaseStates = @(
  foreach ($protectedReleaseRoot in $protectedReleaseRoots) {
    if (Test-Path -LiteralPath $protectedReleaseRoot -PathType Container) {
      Write-Host "[package] hashing protected release: $protectedReleaseRoot"
      $inventory = Get-FileInventoryDigest -Root $protectedReleaseRoot
      Write-Host "[package] protected release digest complete: $($inventory.fileCount) records, $($inventory.totalBytes) bytes"
      [pscustomobject]@{
        path       = Split-Path -Leaf $protectedReleaseRoot
        digest     = $inventory.digest
        fileCount  = $inventory.fileCount
        totalBytes = $inventory.totalBytes
      }
    }
  }
)

$buildStartedUtc = [DateTime]::UtcNow
Push-Location $nextRoot
try {
  if ($Clean) {
    & $cargo clean --release --target $target
    if ($LASTEXITCODE -ne 0) { throw "cargo clean failed: $LASTEXITCODE" }
  }

  & $cargo build --release --target $target -p agenttalk-core
  if ($LASTEXITCODE -ne 0) { throw "cargo release build failed: $LASTEXITCODE" }
} finally {
  Pop-Location
}

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
  if ($LASTEXITCODE -ne 0) { throw "Flutter Windows release build failed: $LASTEXITCODE" }
} finally {
  Pop-Location
  if ($script:flutterBuildRestoreRequired -and (Test-Path -LiteralPath $flutterBuildRoot)) {
    if (Test-Path -LiteralPath $flutterBuildStateRoot) { throw "Flutter state build destination already exists during restore." }
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $flutterBuildStateRoot) | Out-Null
    Move-Item -LiteralPath $flutterBuildRoot -Destination $flutterBuildStateRoot
  }
}

$sourceCapturedAfterUtc = [DateTime]::UtcNow.ToString('o')
$sourceAfter = Get-SourceSnapshot -Root $nextRoot
if ($sourceBefore.digest -ne $sourceAfter.digest -or $sourceBefore.fileCount -ne $sourceAfter.fileCount) {
  throw "Source inputs changed during the build; candidate creation is blocked. before=$($sourceBefore.digest) after=$($sourceAfter.digest)"
}

$flutterRelease = Join-Path $flutterBuildStateRoot 'windows\x64\runner\Release'
$coreExe = Join-Path $cargoTargetRoot "$target\release\agenttalk-core.exe"
$flutterExe = Join-Path $flutterRelease 'agenttalk_desktop.exe'
if (-not (Test-Path -LiteralPath $flutterRelease -PathType Container)) { throw "Flutter release directory missing" }
if (-not (Test-Path -LiteralPath $coreExe -PathType Leaf)) { throw "Core release executable missing" }
if (-not (Test-Path -LiteralPath $flutterExe -PathType Leaf)) { throw "Flutter executable missing" }
if ($Clean -and (Get-Item -LiteralPath $coreExe).LastWriteTimeUtc -lt $buildStartedUtc) { throw "Core executable is older than this clean build" }
if ($Clean -and (Get-Item -LiteralPath $flutterExe).LastWriteTimeUtc -lt $buildStartedUtc) {
  throw "Flutter executable is older than this clean build"
}

New-Item -ItemType Directory -Force -Path $resolvedOutputRoot | Out-Null
New-Item -ItemType Directory -Force -Path $staging | Out-Null
$appDir = Join-Path $staging 'app'
$coreDir = Join-Path $staging 'core'
$dataDir = Join-Path $staging 'data'
New-Item -ItemType Directory -Force -Path $appDir, $coreDir, $dataDir | Out-Null
Copy-Item -Path (Join-Path $flutterRelease '*') -Destination $appDir -Recurse
Copy-Item -LiteralPath $coreExe -Destination (Join-Path $coreDir 'agenttalk-core.exe')

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
Set-Content -LiteralPath (Join-Path $staging 'launch.ps1') -Value $launcher -Encoding utf8

$releaseVerification = @(
  foreach ($protectedReleaseState in $protectedReleaseStates) {
    $protectedReleaseRoot = Join-Path $stateRoot (Join-Path 'releases' $protectedReleaseState.path)
    Write-Host "[package] rechecking protected release: $protectedReleaseRoot"
    $currentInventory = Get-FileInventoryDigest -Root $protectedReleaseRoot
    Write-Host "[package] protected release recheck complete: $($currentInventory.fileCount) records, $($currentInventory.totalBytes) bytes"
    if ($currentInventory.digest -ne $protectedReleaseState.digest -or
      $currentInventory.fileCount -ne $protectedReleaseState.fileCount -or
      $currentInventory.totalBytes -ne $protectedReleaseState.totalBytes) {
      throw "Protected old release changed during packaging: $($protectedReleaseState.path)"
    }
    [ordered]@{
      path              = $protectedReleaseState.path
      unchanged         = $true
      contentDigestSha256 = $currentInventory.digest
      fileCount         = $currentInventory.fileCount
      totalBytes        = $currentInventory.totalBytes
    }
  }
)

$manifest = [ordered]@{
  schema = 2
  product = 'AgentTalk'
  architecture = 'windows-x64'
  version = $Version
  candidate = $candidateName
  buildId = $BuildId
  mode = 'portable-local'
  format = $Format
  source = [ordered]@{
    root = 'agenttalk-next'
    strategy = if ($Clean) { 'clean-before-build' } else { 'incremental-build-cache' }
    snapshotSha256 = $sourceBefore.digest
    fileCount = $sourceBefore.fileCount
    capturedBeforeBuildUtc = $sourceCapturedBeforeUtc
    capturedAfterBuildUtc = $sourceCapturedAfterUtc
    unchangedDuringBuild = $true
    gitMetadataAvailable = (Test-Path -LiteralPath (Join-Path $nextRoot '.git'))
  }
  build = [ordered]@{
    cargo = if ($Clean) { 'cargo clean --release --target x86_64-pc-windows-msvc; cargo build --release --target x86_64-pc-windows-msvc -p agenttalk-core' } else { 'cargo build --release --target x86_64-pc-windows-msvc -p agenttalk-core' }
    flutter = if ($Clean) { 'flutter clean; flutter build windows --release' } else { 'flutter build windows --release' }
    completedAtUtc = [DateTime]::UtcNow.ToString('o')
  }
  core = 'core/agenttalk-core.exe'
  app = 'app/agenttalk_desktop.exe'
  database = 'data/agenttalk-core.sqlite3'
  hashes = [ordered]@{
    algorithm = 'SHA256'
    file = 'SHA256SUMS.txt'
    excludesSelf = $true
  }
  protectedOldRelease = $releaseVerification
  liveProviderStatus = 'Owner-Gate/PENDING'
  ownerGate = 'PENDING'
  sourcePostgresModified = $false
  rollback = [ordered]@{
    oldRelease = 'Preserved; this script never writes to release or historical release-* directories.'
    candidate = 'Stop the candidate first, then remove only this candidate directory and its isolated data if rollback is authorized.'
    restore = 'Restart the preserved old Electron/PostgreSQL/Gateway release and run its health/smoke checks.'
  }
}
$manifest | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath (Join-Path $staging 'manifest.json') -Encoding utf8

$files = @(Get-ChildItem -LiteralPath $staging -Recurse -File | Where-Object { $_.Name -ne 'SHA256SUMS.txt' } | Sort-Object FullName)
if ($files.Count -eq 0) { throw 'Candidate contains no files' }
$hashLines = @(
  $files | ForEach-Object {
    $relative = $_.FullName.Substring($staging.Length + 1).Replace('\', '/')
    "$((Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant())  $relative"
  }
)
Set-Content -LiteralPath (Join-Path $staging 'SHA256SUMS.txt') -Value $hashLines -Encoding ascii

$hashMismatches = @(
  Get-Content -LiteralPath (Join-Path $staging 'SHA256SUMS.txt') | ForEach-Object {
    if ($_ -notmatch '^([0-9a-fA-F]{64})  (.+)$') {
      "invalid line: $_"
    } else {
      $expected = $matches[1].ToLowerInvariant()
      $relative = $matches[2]
      $filePath = Join-Path $staging ($relative -replace '/', '\')
      if (-not (Test-Path -LiteralPath $filePath -PathType Leaf)) {
        "missing file: $relative"
      } elseif ((Get-FileHash -LiteralPath $filePath -Algorithm SHA256).Hash.ToLowerInvariant() -ne $expected) {
        "hash mismatch: $relative"
      }
    }
  }
)
if ($hashMismatches.Count -gt 0) {
  throw "SHA256SUMS.txt self-check failed: $($hashMismatches -join '; ')"
}

Move-Item -LiteralPath $staging -Destination $candidate
$staging = $null
Write-Output "Portable candidate prepared at $candidate"

if ($Format -in @('msix', 'both')) {
  $msix = New-MsixPackage -CandidateRoot $candidate -OutputRoot $MsixOutputRoot -PackageVersion $MsixVersion -BuildId $BuildId
  Write-Output "Unsigned MSIX prepared at $($msix.path)"
  Write-Output "MSIX SHA-256: $($msix.sha256)"
}
