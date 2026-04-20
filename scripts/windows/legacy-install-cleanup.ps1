[CmdletBinding()]
param(
  [Parameter(Mandatory = $true)]
  [string]$LegacyInstallDir,

  [Parameter(Mandatory = $true)]
  [string]$AppDataDir
)

$ErrorActionPreference = "Stop"

function Copy-LegacyLibraryIfNeeded {
  param(
    [Parameter(Mandatory = $true)]
    [string]$LegacyLibraryDir,

    [Parameter(Mandatory = $true)]
    [string]$AppDataRoot
  )

  if (!(Test-Path -LiteralPath $LegacyLibraryDir)) {
    return
  }

  $librariesRoot = Join-Path $AppDataRoot "libraries"
  $defaultLibraryDir = Join-Path $librariesRoot "default-library"
  $statePath = Join-Path $AppDataRoot "state.json"
  New-Item -ItemType Directory -Force -Path $librariesRoot | Out-Null

  if (!(Test-Path -LiteralPath $statePath) -and !(Test-Path -LiteralPath $defaultLibraryDir)) {
    Copy-Item -LiteralPath $LegacyLibraryDir -Destination $defaultLibraryDir -Recurse -Force
    return
  }

  $timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
  $backupLibraryDir = Join-Path $librariesRoot "migrated-install-library-$timestamp"
  Copy-Item -LiteralPath $LegacyLibraryDir -Destination $backupLibraryDir -Recurse -Force
}

function Remove-LegacyInstallDir {
  param(
    [Parameter(Mandatory = $true)]
    [string]$InstallDir
  )

  if (!(Test-Path -LiteralPath $InstallDir)) {
    return
  }

  $stagingDir = Join-Path $env:TEMP ("buquanshu-legacy-remove-" + [guid]::NewGuid().ToString("N"))
  Move-Item -LiteralPath $InstallDir -Destination $stagingDir -Force

  $longPath = "\\?\$stagingDir"
  Start-Process -FilePath "cmd.exe" -ArgumentList "/c", "rd /s /q `"$longPath`"" -Wait -NoNewWindow
  if (Test-Path -LiteralPath $stagingDir) {
    Remove-Item -LiteralPath $stagingDir -Recurse -Force
  }
}

if (!(Test-Path -LiteralPath $LegacyInstallDir)) {
  exit 0
}

$legacyLibraryDir = Join-Path $LegacyInstallDir "library"
Copy-LegacyLibraryIfNeeded -LegacyLibraryDir $legacyLibraryDir -AppDataRoot $AppDataDir
Remove-LegacyInstallDir -InstallDir $LegacyInstallDir

if (Test-Path -LiteralPath $LegacyInstallDir) {
  throw "Legacy install directory still exists: $LegacyInstallDir"
}
