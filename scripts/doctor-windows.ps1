Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$serviceRoot = Join-Path $repoRoot "tools\recording-retrieval-service\app"
$venvPython = Join-Path $serviceRoot ".venv\Scripts\python.exe"

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

function Assert-PathWritable([string]$targetPath) {
  New-Item -ItemType Directory -Force -Path $targetPath | Out-Null
  $probe = Join-Path $targetPath ".doctor-write-test"
  Set-Content -Path $probe -Value "ok" -Encoding UTF8
  Remove-Item -LiteralPath $probe -Force
}

Assert-NodeVersion
$pythonExe = Resolve-PythonExecutable
Assert-PythonVersion $pythonExe

if (-not (Test-Path (Join-Path $repoRoot "node_modules"))) {
  throw "node_modules is missing. Run npm run bootstrap:windows first."
}

if (-not (Test-Path $venvPython)) {
  throw "Retrieval service virtual environment is missing. Run npm run bootstrap:windows first."
}

& $venvPython -c "import importlib.util, sys; sys.exit(0 if importlib.util.find_spec('playwright') else 1)"
if ($LASTEXITCODE -ne 0) {
  throw "Playwright Python dependency is missing. Run npm run bootstrap:windows first."
}

$playwrightCheck = @'
from playwright.sync_api import sync_playwright

with sync_playwright() as playwright:
    browser = playwright.chromium.launch(headless=True)
    browser.close()
'@
$playwrightCheck | & $venvPython -
if ($LASTEXITCODE -ne 0) {
  throw "Playwright Chromium could not start. Re-run npm run bootstrap:windows."
}

Assert-PathWritable (Join-Path $repoRoot "output")
Assert-PathWritable (Join-Path $env:APPDATA "Introduction to Classical Music")

$portCheck = @'
const net = require("node:net");

function listen(port) {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.once("error", reject);
    server.listen(port, "127.0.0.1", () => resolve(server));
  });
}

async function findAvailablePort(preferredPort) {
  for (let candidate = preferredPort; candidate < preferredPort + 20; candidate += 1) {
    try {
      const server = await listen(candidate);
      await new Promise((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
      return candidate;
    } catch (error) {
      if (!error || error.code !== "EADDRINUSE") {
        throw error;
      }
    }
  }
  throw new Error(`No fallback port found after ${preferredPort}`);
}

async function verify(preferredPort) {
  let blocker = null;
  try {
    blocker = await listen(preferredPort);
  } catch (error) {
    if (!error || error.code !== "EADDRINUSE") {
      throw error;
    }
  }
  try {
    const fallback = await findAvailablePort(preferredPort);
    if (blocker && fallback !== preferredPort) {
      return;
    }
    if (!blocker && fallback === preferredPort) {
      return;
    }
    if (fallback === preferredPort) {
      throw new Error(`Fallback port was not selected for ${preferredPort}`);
    }
  } finally {
    if (blocker) {
      await new Promise((resolve, reject) => blocker.close((error) => error ? reject(error) : resolve()));
    }
  }
}

(async () => {
  await verify(4321);
  await verify(4322);
  await verify(4331);
  await verify(4789);
})();
'@
$portCheck | node -
if ($LASTEXITCODE -ne 0) {
  throw "Port fallback check failed."
}

Write-Host "doctor:windows passed."
