$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$python = Join-Path $root ".venv\Scripts\python.exe"
$spec = Join-Path $root "packaging\recording-retrieval-service.spec"
$dist = Join-Path $root "dist"
$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$releaseDir = Join-Path $dist "releases"
$bundle = Join-Path $releaseDir "recording-retrieval-service-portable-$timestamp"
$latestBundle = Join-Path $dist "portable"
$pyinstallerBundle = Join-Path $dist "recording-retrieval-service"
$zipPath = Join-Path $dist "recording-retrieval-service-portable-$timestamp.zip"

if (-not (Test-Path $python)) {
  throw "Missing local virtual environment: $python"
}

& $python -m PyInstaller --noconfirm --distpath $dist --workpath (Join-Path $root "build\pyinstaller") $spec
if ($LASTEXITCODE -ne 0) {
  throw "PyInstaller build failed with exit code $LASTEXITCODE"
}

if (Test-Path $bundle) {
  Remove-Item -Recurse -Force $bundle
}
if (Test-Path $latestBundle) {
  Remove-Item -Recurse -Force $latestBundle
}

New-Item -ItemType Directory -Force -Path $bundle | Out-Null
New-Item -ItemType Directory -Force -Path $latestBundle | Out-Null
Copy-Item -Recurse -Force (Join-Path $pyinstallerBundle "*") $bundle
Copy-Item -Recurse -Force (Join-Path $pyinstallerBundle "*") $latestBundle
Copy-Item -Force (Join-Path $root "packaging\portable-start-service.cmd") (Join-Path $bundle "start-service.cmd")
Copy-Item -Force (Join-Path $root "packaging\portable-start-ui.cmd") (Join-Path $bundle "start-ui.cmd")
Copy-Item -Force (Join-Path $root "packaging\portable-start-service.cmd") (Join-Path $latestBundle "start-service.cmd")
Copy-Item -Force (Join-Path $root "packaging\portable-start-ui.cmd") (Join-Path $latestBundle "start-ui.cmd")

$sensitiveLegacyLlmPaths = @(
  (Join-Path $bundle "_internal\materials\source-profiles\LLM config.txt"),
  (Join-Path $latestBundle "_internal\materials\source-profiles\LLM config.txt")
)
foreach ($legacyLlmPath in $sensitiveLegacyLlmPaths) {
  if (Test-Path $legacyLlmPath) {
    Remove-Item -Force $legacyLlmPath
  }
}

if (Test-Path $zipPath) {
  Remove-Item -Force $zipPath
}
Compress-Archive -Path (Join-Path $bundle '*') -DestinationPath $zipPath
Write-Host "Portable directory: $bundle"
Write-Host "Latest portable directory: $latestBundle"
Write-Host "Portable zip: $zipPath"
