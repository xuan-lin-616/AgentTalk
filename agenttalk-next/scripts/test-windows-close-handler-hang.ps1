[CmdletBinding()]
param(
  [Parameter(Mandatory = $true)]
  [string]$BundleRoot,
  [string]$OutputRoot,
  [int]$Rounds = 3
)

$ErrorActionPreference = 'Stop'
if ($Rounds -ne 3) { throw 'Owner Gate requires exactly three close-handler-hang rounds.' }

$bundle = (Resolve-Path -LiteralPath $BundleRoot).Path
$flutterExe = Join-Path $bundle 'agenttalk_desktop.exe'
$coreExe = Join-Path $bundle 'agenttalk-core.exe'
foreach ($required in @($flutterExe, $coreExe, (Join-Path $bundle 'agenttalk-bundle.manifest.json'), (Join-Path (Join-Path $bundle 'data') 'flutter_assets'))) {
  if (-not (Test-Path -LiteralPath $required)) { throw "Bundle input is missing: $required" }
}

if ([string]::IsNullOrWhiteSpace($OutputRoot)) {
  $stateRoot = if ($env:AGENTTALK_STATE_ROOT) { $env:AGENTTALK_STATE_ROOT } else { Join-Path $bundle '......' }
  $OutputRoot = Join-Path ([System.IO.Path]::GetFullPath($stateRoot)) 'smokewindows-close-handler-hang'
}
$output = [System.IO.Path]::GetFullPath($OutputRoot)
New-Item -ItemType Directory -Force -Path $output | Out-Null
$reportPath = Join-Path $output 'close-handler-hang-smoke.json'
$coreFullPath = [System.IO.Path]::GetFullPath($coreExe)

if (-not ('AgentTalkNativeWindow' -as [type])) {
  Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;

public static class AgentTalkNativeWindow
{
    [DllImport("user32.dll", SetLastError = true)]
    public static extern bool PostMessage(IntPtr hWnd, uint message, IntPtr wParam, IntPtr lParam);

    [DllImport("user32.dll")]
    public static extern bool IsWindow(IntPtr hWnd);
}
'@
}

function Get-BundleCoreChild {
  param([Parameter(Mandatory = $true)][int]$ParentProcessId)
  Get-CimInstance Win32_Process -Filter "ParentProcessId = $ParentProcessId" |
    Where-Object {
      $_.ExecutablePath -and
      ([System.IO.Path]::GetFullPath($_.ExecutablePath) -ieq $coreFullPath)
    }
}

function Stop-TrackedProcess {
  param([System.Diagnostics.Process]$Process)
  if ($null -eq $Process) { return }
  try { $Process.Refresh() } catch { return }
  if ($Process.HasExited) {
    try { $Process.WaitForExit(5000) } catch { }
    return
  }
  if (-not $Process.HasExited) {
    try { $Process.Kill() } catch { }
    try { $Process.WaitForExit(5000) } catch { }
  }
}

$records = [System.Collections.Generic.List[object]]::new()
$failed = $false
for ($round = 1; $round -le $Rounds; $round++) {
  $roundRoot = Join-Path $output ("round-{0}" -f $round)
  $workingDirectory = Join-Path $roundRoot 'cwd'
  $dataRoot = Join-Path $roundRoot 'data'
  New-Item -ItemType Directory -Force -Path $workingDirectory, $dataRoot | Out-Null
  $flutter = $null
  $core = $null
  $record = [ordered]@{
    round = $round
    bundle = $bundle
    flutterPid = $null
    corePid = $null
    wmCloseUtc = $null
    flutterExitedUtc = $null
    coreExitedUtc = $null
    flutterExitMilliseconds = $null
    coreExitMilliseconds = $null
    windowExited = $false
    result = 'FAIL'
    error = $null
  }
  try {
    $startInfo = [System.Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = $flutterExe
    $startInfo.WorkingDirectory = $workingDirectory
    $startInfo.UseShellExecute = $false
    $startInfo.CreateNoWindow = $true
    foreach ($key in @(
      'AGENTTALK_CORE_EXECUTABLE',
      'AGENTTALK_CORE_DATABASE',
      'AGENTTALK_CORE_ARTIFACT_ROOT',
      'AGENTTALK_CORE_MODE',
      'AGENTTALK_CORE_PIPE',
      'AGENTTALK_CORE_SESSION_CREDENTIAL'
    )) {
      [void]$startInfo.Environment.Remove($key)
    }
    $startInfo.Environment['AGENTTALK_CORE_DEV_MODE'] = '1'
    $startInfo.Environment['AGENTTALK_TEST_HANG_CLOSE'] = '1'
    $startInfo.Environment['AGENTTALK_DATA_ROOT'] = $dataRoot
    $startInfo.Environment['AGENTTALK_CORE_ARTIFACT_ROOT'] = Join-Path $dataRoot 'artifacts'

    $flutter = [System.Diagnostics.Process]::new()
    $flutter.StartInfo = $startInfo
    if (-not $flutter.Start()) { throw 'Flutter process did not start.' }
    $record.flutterPid = $flutter.Id

    $windowDeadline = [DateTime]::UtcNow.AddSeconds(45)
    do {
      $flutter.Refresh()
      if ($flutter.HasExited) { throw "Flutter exited before showing its window (exit code $($flutter.ExitCode))." }
      if ($flutter.MainWindowHandle -ne [IntPtr]::Zero) { break }
      Start-Sleep -Milliseconds 250
    } while ([DateTime]::UtcNow -lt $windowDeadline)
    $window = $flutter.MainWindowHandle
    if ($window -eq [IntPtr]::Zero) { throw 'Flutter window did not appear within 45 seconds.' }

    $coreDeadline = [DateTime]::UtcNow.AddSeconds(45)
    do {
      $coreCandidate = @(Get-BundleCoreChild -ParentProcessId $flutter.Id)
      if ($coreCandidate.Count -eq 1) { break }
      $flutter.Refresh()
      if ($flutter.HasExited) { throw 'Flutter exited before the owned Core was observed.' }
      Start-Sleep -Milliseconds 250
    } while ([DateTime]::UtcNow -lt $coreDeadline)
    $coreCandidate = @(Get-BundleCoreChild -ParentProcessId $flutter.Id)
    if ($coreCandidate.Count -ne 1) {
      throw "Expected exactly one Bundle-owned Core child, found $($coreCandidate.Count)."
    }
    $record.corePid = [int]$coreCandidate[0].ProcessId
    $core = Get-Process -Id $record.corePid -ErrorAction Stop

    # No close-completion callback can run while the development fixture is
    # awaiting forever; a roughly ten-second exit is the Native WM_TIMER path.
    $closeAt = [DateTime]::UtcNow
    $record.wmCloseUtc = $closeAt.ToString('o')
    if (-not [AgentTalkNativeWindow]::PostMessage($window, 0x0010, [IntPtr]::Zero, [IntPtr]::Zero)) {
      throw "PostMessage(WM_CLOSE) failed with Win32 error $([Runtime.InteropServices.Marshal]::GetLastWin32Error())."
    }

    $flutterDeadline = $closeAt.AddSeconds(25)
    do {
      $flutter.Refresh()
      if ($flutter.HasExited) { break }
      Start-Sleep -Milliseconds 100
    } while ([DateTime]::UtcNow -lt $flutterDeadline)
    if (-not $flutter.HasExited) { throw 'Flutter did not exit within the 25-second close bound.' }
    $flutterExitAt = [DateTime]::UtcNow
    $record.flutterExitedUtc = $flutterExitAt.ToString('o')
    $record.flutterExitMilliseconds = [int]($flutterExitAt - $closeAt).TotalMilliseconds
    $record.windowExited = -not [AgentTalkNativeWindow]::IsWindow($window)
    if (-not $record.windowExited) { throw 'Flutter process exited but its close target still reports as a window.' }

    $coreDeadline = $closeAt.AddSeconds(25)
    do {
      $core.Refresh()
      if ($core.HasExited) { break }
      Start-Sleep -Milliseconds 100
    } while ([DateTime]::UtcNow -lt $coreDeadline)
    if (-not $core.HasExited) { throw 'The owned Core remained after Flutter exited.' }
    $coreExitAt = [DateTime]::UtcNow
    $record.coreExitedUtc = $coreExitAt.ToString('o')
    $record.coreExitMilliseconds = [int]($coreExitAt - $closeAt).TotalMilliseconds

    if ($record.flutterExitMilliseconds -lt 8500 -or $record.flutterExitMilliseconds -gt 18000) {
      throw "Native close fallback timing was not approximately ten seconds: $($record.flutterExitMilliseconds) ms."
    }
    $record.result = 'PASS'
  } catch {
    $record.error = $_.Exception.Message
    $failed = $true
  } finally {
    Stop-TrackedProcess -Process $flutter
    Stop-TrackedProcess -Process $core
    try {
      if (Test-Path -LiteralPath $roundRoot) {
        Remove-Item -LiteralPath $roundRoot -Recurse -Force
      }
    } catch {
      if ($null -eq $record.error) { $record.error = "Test data cleanup failed: $($_.Exception.Message)" }
      $failed = $true
    }
    $records.Add([pscustomobject]$record)
  }
  if ($record.result -ne 'PASS') { break }
}

$roundItems = @($records.ToArray())
$allPassed = $roundItems.Count -eq $Rounds
foreach ($item in $roundItems) {
  if ($item.result -ne 'PASS') { $allPassed = $false }
}
$report = [ordered]@{
  schema = 1
  scenario = 'dart-close-handler-permanent-hang'
  bundle = $bundle
  core = $coreFullPath
  fixture = [ordered]@{
    AGENTTALK_CORE_DEV_MODE = '1'
    AGENTTALK_TEST_HANG_CLOSE = '1'
    coreExecutableEnvironmentOverride = $false
  }
  rounds = $roundItems
  allPassed = (-not $failed) -and $allPassed
}
$report | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $reportPath -Encoding utf8
Write-Output "Close-handler hang smoke report: $reportPath"
foreach ($item in $records) {
  Write-Output ("round={0} result={1} flutterPid={2} corePid={3} flutterExitMs={4} coreExitMs={5}" -f $item.round, $item.result, $item.flutterPid, $item.corePid, $item.flutterExitMilliseconds, $item.coreExitMilliseconds)
}
if (-not $report.allPassed) { throw "Close-handler hang smoke failed; see $reportPath" }
