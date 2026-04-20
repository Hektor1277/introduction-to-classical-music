import { afterEach, describe, expect, it, vi } from "vitest";
import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

const tempDirs: string[] = [];

afterEach(async () => {
  vi.resetModules();
  while (tempDirs.length > 0) {
    const target = tempDirs.pop();
    if (target) {
      await rm(target, { recursive: true, force: true });
    }
  }
});

describe("library build asset sync", () => {
  it("copies bundle assets into the built site root as library-assets", async () => {
    const tempRoot = await mkdtemp(path.join(os.tmpdir(), "classical-library-build-"));
    tempDirs.push(tempRoot);
    const assetsDir = path.join(tempRoot, "assets");
    const buildSiteDir = path.join(tempRoot, "build", "site");
    await mkdir(path.join(assetsDir, "managed", "people"), { recursive: true });
    await writeFile(path.join(assetsDir, "managed", "people", "avatar.jpg"), "fixture", "utf8");

    const { syncLibraryAssetsToBuildSite } = await import("../../packages/data-core/src/library-build.ts");
    await syncLibraryAssetsToBuildSite({ assetsDir, buildSiteDir });

    await expect(readFile(path.join(buildSiteDir, "library-assets", "managed", "people", "avatar.jpg"), "utf8")).resolves.toBe("fixture");
  });
});
