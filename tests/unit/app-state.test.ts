import { afterEach, describe, expect, it, vi } from "vitest";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

const originalEnv = { ...process.env };
const tempDirs: string[] = [];

afterEach(async () => {
  process.env = { ...originalEnv };
  vi.resetModules();
  while (tempDirs.length > 0) {
    const target = tempDirs.pop();
    if (target) {
      await rm(target, { recursive: true, force: true });
    }
  }
});

describe("app state persistence", () => {
  it("persists and reloads the active library path with recent libraries", async () => {
    const tempRoot = await mkdtemp(path.join(os.tmpdir(), "classical-app-state-"));
    tempDirs.push(tempRoot);
    process.env.ICM_APP_DATA_DIR = tempRoot;

    const { loadAppState, saveAppState } = await import("../../packages/data-core/src/app-state.ts");
    await saveAppState({
      activeLibraryPath: "D:/Libraries/Classical",
      recentLibraries: ["D:/Libraries/Classical", "E:/Backups/Classical"],
    });

    await expect(loadAppState()).resolves.toEqual({
      activeLibraryPath: "D:/Libraries/Classical",
      recentLibraries: ["D:/Libraries/Classical", "E:/Backups/Classical"],
    });
  });
});
