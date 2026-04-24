import { afterEach, describe, expect, it } from "vitest";
import http from "node:http";

import {
  buildCommandSpec,
  buildDetachedStdio,
  buildDevServerUrl,
  createWorkspaceFingerprint,
  findAvailablePort,
  resolveTargetSequence,
  resolveDevServerPaths,
} from "../../scripts/lib/dev-server-manager.js";

const servers: http.Server[] = [];

afterEach(async () => {
  await Promise.all(
    servers.splice(0).map(
      (server) =>
        new Promise((resolve, reject) => {
          if (!server.listening) {
            resolve(undefined);
            return;
          }
          server.close((error) => (error ? reject(error) : resolve(undefined)));
        }),
    ),
  );
});

describe("dev process manager helpers", () => {
  it("creates stable workspace fingerprints and isolated state paths", () => {
    const first = createWorkspaceFingerprint("E:/repo/a");
    const second = createWorkspaceFingerprint("E:/repo/b");

    expect(first).toHaveLength(10);
    expect(first).not.toBe(second);
    expect(resolveDevServerPaths("E:/repo/a", "site", "E:/repo/a").statePath).toContain(`site-${first}.json`);
  });

  it("skips occupied ports when choosing the next available dev port", async () => {
    const busyServer = http.createServer((_request, response) => response.end("ok"));
    servers.push(busyServer);
    await new Promise<void>((resolve, reject) => {
      busyServer.once("error", reject);
      busyServer.listen(4551, "127.0.0.1", () => {
        busyServer.off("error", reject);
        resolve();
      });
    }).catch((error) => {
      const code = error instanceof Error && "code" in error ? String(error.code) : "";
      if (code === "EPERM") {
        return;
      }
      throw error;
    });

    if (!busyServer.listening) {
      return;
    }

    await expect(findAvailablePort(4551, { maxAttempts: 5 })).resolves.toBe(4552);
  });

  it("builds local loopback urls for launched servers", () => {
    expect(buildDevServerUrl(4321)).toBe("http://127.0.0.1:4321");
  });

  it("wraps npm commands through cmd.exe on Windows to avoid spawn EINVAL", () => {
    expect(buildCommandSpec("win32", "npm.cmd", ["run", "build:indexes"])).toEqual({
      command: "cmd.exe",
      args: ["/d", "/s", "/c", "npm.cmd run build:indexes"],
    });
  });

  it("uses direct log file descriptors for detached processes so the launcher can exit", () => {
    expect(buildDetachedStdio(17)).toEqual(["ignore", 17, 17]);
  });

  it("starts the retrieval service before owner flows that depend on it", () => {
    expect(resolveTargetSequence("all")).toEqual(["retrieval", "site", "owner"]);
    expect(resolveTargetSequence("owner")).toEqual(["retrieval", "owner"]);
    expect(resolveTargetSequence("site")).toEqual(["site"]);
  });
});
