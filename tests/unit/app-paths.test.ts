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

describe("app runtime path resolution", () => {
  it("resolves bundle-aware library and app-data paths from environment variables", async () => {
    const tempRoot = await mkdtemp(path.join(os.tmpdir(), "classical-app-paths-"));
    tempDirs.push(tempRoot);
    const libraryRoot = path.join(tempRoot, "libraries", "default");
    const appDataRoot = path.join(tempRoot, "user-data");
    process.env.ICM_ACTIVE_LIBRARY_DIR = libraryRoot;
    process.env.ICM_APP_DATA_DIR = appDataRoot;

    const { getRuntimePaths } = await import("../../packages/data-core/src/app-paths.ts");
    const paths = getRuntimePaths();

    expect(paths.mode).toBe("bundle");
    expect(paths.library.rootDir).toBe(libraryRoot);
    expect(paths.library.manifestPath).toBe(path.join(libraryRoot, "library.manifest.json"));
    expect(paths.library.contentLibraryDir).toBe(path.join(libraryRoot, "content", "library"));
    expect(paths.library.contentSiteDir).toBe(path.join(libraryRoot, "content", "site"));
    expect(paths.library.assetsDir).toBe(path.join(libraryRoot, "assets"));
    expect(paths.library.runtimeGeneratedDir).toBe(path.join(libraryRoot, "runtime", "generated"));
    expect(paths.library.buildSiteDir).toBe(path.join(libraryRoot, "build", "site"));
    expect(paths.appData.rootDir).toBe(appDataRoot);
    expect(paths.appData.settingsPath).toBe(path.join(appDataRoot, "settings.json"));
    expect(paths.appData.secretsPath).toBe(path.join(appDataRoot, "secrets.json"));
    expect(paths.appData.statePath).toBe(path.join(appDataRoot, "state.json"));
    expect(paths.appData.librariesDir).toBe(path.join(appDataRoot, "libraries"));
  });
});
