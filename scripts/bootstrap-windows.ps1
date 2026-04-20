Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$serviceRoot = Join-Path $repoRoot "tools\recording-retrieval-service\app"
$venvRoot = Join-Path $serviceRoot ".venv"
$venvPython = Join-Path $venvRoot "Scripts\python.exe"
$npmCli = (Get-Command npm.cmd -ErrorAction Stop).Source

function Resolve-PythonExecutable {
  $pyCommand = Get-Command py -ErrorAction SilentlyContinue
  if ($pyCommand) {
    try {
      $candidate = (& py -3.13 -c "import sys; print(sys.executable)" 2>$null).Trim()
      if ($candidate) {
        return $candidate
      }
    } catch {
    }
  }

  $pythonCommand = Get-Command python -ErrorAction SilentlyContinue
  if ($pythonCommand) {
    $candidate = (& python -c "import sys; print(sys.executable)").Trim()
    if ($candidate) {
      return $candidate
    }
  }

  throw "Python 3.13 was not found. Install Python 3.13.x first."
}

function Assert-NodeVersion {
  $nodeVersion = (& node -p "process.versions.node").Trim()
  if (-not $nodeVersion.StartsWith("22.")) {
    throw "Node.js 22.x is required. Current version: $nodeVersion"
  }
}

function Assert-PythonVersion([string]$pythonExe) {
  $pythonVersion = (& $pythonExe -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')").Trim()
  if ($pythonVersion -ne "3.13") {
    throw "Python 3.13.x is required. Current version: $pythonVersion"
  }
}

function Invoke-NpmCi([string]$mirrorUrl = "") {
  $stdoutFile = Join-Path $env:TEMP ("icm-npm-ci-" + [System.Guid]::NewGuid().ToString("N") + ".stdout.log")
  $stderrFile = Join-Path $env:TEMP ("icm-npm-ci-" + [System.Guid]::NewGuid().ToString("N") + ".stderr.log")
  $previousMirror = $env:ELECTRON_MIRROR
  try {
    if ($mirrorUrl) {
      $env:ELECTRON_MIRROR = $mirrorUrl
    } else {
      Remove-Item Env:ELECTRON_MIRROR -ErrorAction SilentlyContinue
    }

    $process = Start-Process `
      -FilePath $npmCli `
      -ArgumentList "ci" `
      -WorkingDirectory $repoRoot `
      -NoNewWindow `
      -Wait `
      -PassThru `
      -RedirectStandardOutput $stdoutFile `
      -RedirectStandardError $stderrFile

    $stdoutText = if (Test-Path $stdoutFile) { Get-Content -LiteralPath $stdoutFile -Raw } else { "" }
    $stderrText = if (Test-Path $stderrFile) { Get-Content -LiteralPath $stderrFile -Raw } else { "" }
    $combinedOutput = @($stdoutText, $stderrText) -join ""

    if ($stdoutText) {
      Write-Host $stdoutText.TrimEnd()
    }
    if ($stderrText) {
      Write-Host $stderrText.TrimEnd()
    }

    return @{
      ExitCode = $process.ExitCode
      Output = $combinedOutput
    }
  } finally {
    if ($null -eq $previousMirror) {
      Remove-Item Env:ELECTRON_MIRROR -ErrorAction SilentlyContinue
    } else {
      $env:ELECTRON_MIRROR = $previousMirror
    }
    Remove-Item -LiteralPath $stdoutFile -Force -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath $stderrFile -Force -ErrorAction SilentlyContinue
  }
}

function Invoke-NpmCiWithElectronFallback {
  $firstRun = Invoke-NpmCi
  if ($firstRun.ExitCode -eq 0) {
    return
  }

  $combinedOutput = $firstRun.Output
  $needsElectronMirrorRetry = $combinedOutput -match "node_modules\\electron" -and $combinedOutput -match "ETIMEDOUT"
  if (-not $needsElectronMirrorRetry) {
    throw "npm ci failed. Exit code: $($firstRun.ExitCode)"
  }

  Write-Warning "npm ci failed while downloading Electron from the default source. Retrying with the Electron mirror."
  $retryRun = Invoke-NpmCi "https://npmmirror.com/mirrors/electron/"
  if ($retryRun.ExitCode -ne 0) {
    throw "npm ci failed again even after enabling the Electron mirror. Exit code: $($retryRun.ExitCode)"
  }
}

Assert-NodeVersion
$pythonExe = Resolve-PythonExecutable
Assert-PythonVersion $pythonExe

Push-Location $repoRoot
try {
  Invoke-NpmCiWithElectronFallback
} finally {
  Pop-Location
}

if (-not (Test-Path $venvPython)) {
  & $pythonExe -m venv $venvRoot
}

& $venvPython -m pip install --upgrade pip wheel

Push-Location $serviceRoot
try {
  & $venvPython -m pip install -e ".[dev]"
} finally {
  Pop-Location
}

& $venvPython -m playwright install chromium

Write-Host "bootstrap:windows completed."
