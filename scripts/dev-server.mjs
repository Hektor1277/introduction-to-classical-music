import { spawn } from "node:child_process";
import { once } from "node:events";
import { closeSync, openSync } from "node:fs";
import { access, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import process from "node:process";

import {
  buildCommandSpec,
  buildDetachedStdio,
  DEV_SERVER_TARGETS,
  buildDevServerUrl,
  findAvailablePort,
  resolveTargetSequence,
  resolveDevServerPaths,
} from "./lib/dev-server-manager.js";

const rootDir = process.cwd();
const npmCommand = process.platform === "win32" ? "npm.cmd" : "npm";
const recordingConfigPath = path.join(rootDir, "data", "automation", "recording-retrieval.local.json");
const sharedToolRootCandidates = [rootDir, path.resolve(rootDir, "..", "..")];

function parseArgs(argv) {
  const [command = "status", rawTarget = "all", ...rest] = argv;
  return {
    command,
    target: rawTarget,
    open: rest.includes("--open"),
  };
}

function getTargets(command, rawTarget) {
  const targets = resolveTargetSequence(rawTarget);
  return command === "stop" ? [...targets].reverse() : targets;
}

function stripUtf8Bom(value) {
  return value.charCodeAt(0) === 0xfeff ? value.slice(1) : value;
}

function getHealthUrl(target, url) {
  const healthPath = DEV_SERVER_TARGETS[target]?.healthPath || "/";
  if (healthPath === "/") {
    return url;
  }
  return `${url}${healthPath}`;
}

async function ensureFileExists(filePath) {
  await access(filePath);
}

async function resolveRecordingRetrievalServicePaths() {
  for (const candidateRoot of [...new Set(sharedToolRootCandidates)]) {
    const serviceCwd = path.join(candidateRoot, "tools", "recording-retrieval-service", "app");
    const servicePythonPath = path.join(serviceCwd, ".venv", "Scripts", "python.exe");
    try {
      await ensureFileExists(servicePythonPath);
      return { serviceCwd, servicePythonPath };
    } catch {
      // try next root candidate
    }
  }
  throw new Error("Recording retrieval service Python environment was not found in the current worktree or the primary repository root");
}

async function loadRecordingRetrievalConfig() {
  try {
    const content = stripUtf8Bom(await readFile(recordingConfigPath, "utf8"));
    return JSON.parse(content);
  } catch {
    return {
      enabled: true,
      baseUrl: `http://127.0.0.1:${DEV_SERVER_TARGETS.retrieval.preferredPort}`,
      timeoutMs: 180000,
      pollIntervalMs: 1200,
      expectedProtocolVersion: "v1",
      status: "",
    };
  }
}

async function saveRecordingRetrievalConfig(config) {
  await mkdir(path.dirname(recordingConfigPath), { recursive: true });
  await writeFile(recordingConfigPath, `${JSON.stringify(config, null, 2)}\n`, "utf8");
}

function getPortFromUrl(value, fallbackPort) {
  try {
    const url = new URL(String(value || ""));
    const parsed = Number(url.port || fallbackPort);
    return Number.isFinite(parsed) ? parsed : fallbackPort;
  } catch {
    return fallbackPort;
  }
}

async function runForeground(command, args, envPatch = {}) {
  const commandSpec = buildCommandSpec(process.platform, command, args);
  const child = spawn(commandSpec.command, commandSpec.args, {
    cwd: rootDir,
    env: { ...process.env, ...envPatch },
    stdio: "inherit",
  });
  const [code] = await once(child, "exit");
  if (code !== 0) {
    throw new Error(`Command failed: ${command} ${args.join(" ")}`);
  }
}

async function isUrlHealthy(url) {
  try {
    const response = await fetch(url, { redirect: "manual" });
    return response.status < 500;
  } catch {
    return false;
  }
}

async function waitForUrl(url, timeoutMs = 60000) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    if (await isUrlHealthy(url)) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 600));
  }
  throw new Error(`Timed out waiting for ${url}`);
}

function openBrowser(url) {
  if (process.platform === "win32") {
    spawn("cmd", ["/c", "start", "", url], {
      cwd: rootDir,
      detached: true,
      stdio: "ignore",
    }).unref();
    return;
  }

  if (process.platform === "darwin") {
    spawn("open", [url], { cwd: rootDir, detached: true, stdio: "ignore" }).unref();
    return;
  }

  spawn("xdg-open", [url], { cwd: rootDir, detached: true, stdio: "ignore" }).unref();
}

async function killProcessTree(pid) {
  if (!pid) {
    return;
  }

  if (process.platform === "win32") {
    const killer = spawn("taskkill", ["/PID", String(pid), "/T", "/F"], {
      cwd: rootDir,
      stdio: "ignore",
    });
    await once(killer, "exit");
    return;
  }

  process.kill(-pid, "SIGTERM");
}

async function readState(statePath) {
  try {
    return JSON.parse(await readFile(statePath, "utf8"));
  } catch {
    return null;
  }
}

async function removeState(statePath) {
  await rm(statePath, { force: true });
}

async function ensurePaths(target) {
  const paths = resolveDevServerPaths(rootDir, target);
  await mkdir(paths.stateDir, { recursive: true });
  await mkdir(paths.logDir, { recursive: true });
  return paths;
}

async function startTarget(target, options = {}) {
  const config = DEV_SERVER_TARGETS[target];
  const paths = await ensurePaths(target);
  const existing = await readState(paths.statePath);
  const existingUrl = existing?.url ? getHealthUrl(target, existing.url) : null;

  if (existingUrl && await isUrlHealthy(existingUrl)) {
    if (options.open) {
      openBrowser(existing.url);
    }
    if (target === "retrieval") {
      const currentConfig = await loadRecordingRetrievalConfig();
      await saveRecordingRetrievalConfig({
        ...currentConfig,
        enabled: true,
        baseUrl: existing.url,
        expectedProtocolVersion: "v1",
      });
    }
    return { ...existing, reused: true };
  }

  if (existing?.pid) {
    await killProcessTree(existing.pid).catch(() => {});
    await removeState(paths.statePath);
  }

  if (target === "site" || target === "owner") {
    await runForeground(process.execPath, ["scripts/prepare-shared-assets.mjs"]);
  }

  if (target === "site") {
    await runForeground(npmCommand, ["run", "build:indexes"]);
  } else if (target === "owner") {
    await runForeground(npmCommand, ["run", "runtime:build"]);
  }
  const retrievalConfig = target === "retrieval" ? await loadRecordingRetrievalConfig() : null;
  const preferredPort =
    target === "retrieval"
      ? getPortFromUrl(retrievalConfig?.baseUrl, config.preferredPort)
      : config.preferredPort;
  const port = await findAvailablePort(preferredPort);
  const url = buildDevServerUrl(port);
  const logFd = openSync(paths.logPath, "a");
  const startedAt = new Date().toISOString();
  let resolvedChild;
  if (target === "site") {
    const commandSpec = buildCommandSpec(process.platform, npmCommand, [
      "exec",
      "--",
      "astro",
      "dev",
      "--root",
      "apps/site",
      "--host",
      "127.0.0.1",
      "--port",
      String(port),
    ]);
    resolvedChild = spawn(commandSpec.command, commandSpec.args, {
      cwd: rootDir,
      env: { ...process.env },
      detached: true,
      stdio: buildDetachedStdio(logFd),
    });
  } else if (target === "retrieval") {
    const { serviceCwd, servicePythonPath } = await resolveRecordingRetrievalServicePaths();
    resolvedChild = spawn(servicePythonPath, ["-m", "app.main", "--mode", "service", "--host", "127.0.0.1", "--port", String(port)], {
      cwd: serviceCwd,
      env: { ...process.env },
      detached: true,
      stdio: buildDetachedStdio(logFd),
    });
  } else {
    resolvedChild = spawn(process.execPath, ["output/runtime/apps/owner/server/owner-app.js"], {
      cwd: rootDir,
      env: { ...process.env, OWNER_PORT: String(port) },
      detached: true,
      stdio: buildDetachedStdio(logFd),
    });
  }
  resolvedChild.unref();
  closeSync(logFd);

  try {
    await waitForUrl(getHealthUrl(target, url));
  } catch (error) {
    await killProcessTree(resolvedChild.pid).catch(() => {});
    throw error;
  }

  const nextState = {
    target,
    pid: resolvedChild.pid,
    port,
    url,
    startedAt,
    logPath: paths.logPath,
    cwd: rootDir,
  };

  await writeFile(paths.statePath, `${JSON.stringify(nextState, null, 2)}\n`, "utf8");
  if (target === "retrieval") {
    await saveRecordingRetrievalConfig({
      ...retrievalConfig,
      enabled: true,
      baseUrl: url,
      expectedProtocolVersion: "v1",
    });
  }

  if (options.open) {
    openBrowser(url);
  }

  return nextState;
}

async function stopTarget(target) {
  const paths = resolveDevServerPaths(rootDir, target);
  const state = await readState(paths.statePath);
  if (!state) {
    return { target, stopped: false, reason: "not-running" };
  }

  if (state.pid) {
    await killProcessTree(state.pid).catch(() => {});
  }
  await removeState(paths.statePath);
  return { target, stopped: true };
}

async function statusTarget(target) {
  const paths = resolveDevServerPaths(rootDir, target);
  const state = await readState(paths.statePath);
  if (!state) {
    return { target, running: false };
  }

  const healthy = state.url ? await isUrlHealthy(getHealthUrl(target, state.url)) : false;
  if (!healthy) {
    await removeState(paths.statePath);
    return { target, running: false, stale: true };
  }

  return { ...state, running: true };
}

const options = parseArgs(process.argv.slice(2));
const targets = getTargets(options.command, options.target);

const result = [];
for (const target of targets) {
  if (options.command === "start") {
    result.push(await startTarget(target, options));
    continue;
  }
  if (options.command === "stop") {
    result.push(await stopTarget(target));
    continue;
  }
  if (options.command === "status") {
    result.push(await statusTarget(target));
    continue;
  }
  throw new Error(`Unsupported command: ${options.command}`);
}

process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
