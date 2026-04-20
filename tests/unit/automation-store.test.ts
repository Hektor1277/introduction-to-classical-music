import { mkdtemp, mkdir, writeFile, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { afterEach, describe, expect, it, vi } from "vitest";

const originalCwd = process.cwd();
const tempDirs: string[] = [];

afterEach(async () => {
  process.chdir(originalCwd);
  vi.resetModules();
  while (tempDirs.length > 0) {
    const target = tempDirs.pop();
    if (target) {
      await rm(target, { recursive: true, force: true });
    }
  }
});

describe("automation store config loading", () => {
  it("loads recording retrieval config even when the JSON file has a UTF-8 BOM", async () => {
    const tempRoot = await mkdtemp(path.join(os.tmpdir(), "classical-automation-store-"));
    tempDirs.push(tempRoot);
    await mkdir(path.join(tempRoot, "data", "automation"), { recursive: true });
    await writeFile(
      path.join(tempRoot, "data", "automation", "recording-retrieval.local.json"),
      `\uFEFF${JSON.stringify({
        enabled: true,
        baseUrl: "http://127.0.0.1:4793",
        timeoutMs: 180000,
        pollIntervalMs: 1200,
        expectedProtocolVersion: "v1",
        status: "",
      }, null, 2)}\n`,
      "utf8",
    );

    process.chdir(tempRoot);
    const { loadRecordingRetrievalConfig } = await import("../../packages/automation/src/automation-store.ts");

    await expect(loadRecordingRetrievalConfig()).resolves.toMatchObject({
      enabled: true,
      baseUrl: "http://127.0.0.1:4793",
      timeoutMs: 180000,
      pollIntervalMs: 1200,
      expectedProtocolVersion: "v1",
      status: "",
    });
  });
});
