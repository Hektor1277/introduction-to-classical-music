import { createHash } from "node:crypto";
import net from "node:net";
import path from "node:path";

export const DEV_SERVER_TARGETS = {
  owner: {
    label: "owner",
    preferredPort: 4322,
    healthPath: "/",
  },
  retrieval: {
    label: "retrieval",
    preferredPort: 4780,
    healthPath: "/health",
  },
  site: {
    label: "site",
    preferredPort: 4321,
    healthPath: "/",
  },
};

export function createWorkspaceFingerprint(cwd) {
  return createHash("sha1").update(path.resolve(cwd)).digest("hex").slice(0, 10);
}

export function buildDevServerUrl(port) {
  return `http://127.0.0.1:${port}`;
}

function quoteWindowsCommandArg(value) {
  if (/[\s"]/u.test(value)) {
    return `"${String(value).replace(/"/gu, '\\"')}"`;
  }
  return String(value);
}

export function buildCommandSpec(platform, command, args = []) {
  if (platform === "win32" && /\.(cmd|bat)$/iu.test(command)) {
    const commandLine = [command, ...args].map(quoteWindowsCommandArg).join(" ");
    return {
      command: "cmd.exe",
      args: ["/d", "/s", "/c", commandLine],
    };
  }

  return { command, args };
}

export function buildDetachedStdio(logFd) {
  return ["ignore", logFd, logFd];
}

export function resolveTargetSequence(rawTarget) {
  if (rawTarget === "all") {
    return ["retrieval", "site", "owner"];
  }
  if (rawTarget === "owner") {
    return ["retrieval", "owner"];
  }
  if (!(rawTarget in DEV_SERVER_TARGETS)) {
    throw new Error(`Unsupported dev server target: ${rawTarget}`);
  }
  return [rawTarget];
}

export function resolveDevServerPaths(rootDir, target, cwd = rootDir) {
  const fingerprint = createWorkspaceFingerprint(cwd);
  const stateDir = path.join(rootDir, "output", "dev-processes");
  const logDir = path.join(stateDir, "logs");

  return {
    fingerprint,
    stateDir,
    logDir,
    statePath: path.join(stateDir, `${target}-${fingerprint}.json`),
    logPath: path.join(logDir, `${target}-${fingerprint}.log`),
  };
}

export async function isPortAvailable(port, host = "127.0.0.1") {
  return await new Promise((resolve) => {
    const server = net.createServer();
    server.unref();
    server.on("error", () => resolve(false));
    server.listen({ host, port }, () => {
      server.close(() => resolve(true));
    });
  });
}

export async function findAvailablePort(preferredPort, options = {}) {
  const host = options.host ?? "127.0.0.1";
  const maxAttempts = options.maxAttempts ?? 20;

  for (let offset = 0; offset <= maxAttempts; offset += 1) {
    const candidate = preferredPort + offset;
    if (await isPortAvailable(candidate, host)) {
      return candidate;
    }
  }

  throw new Error(`No available port found starting from ${preferredPort}`);
}
