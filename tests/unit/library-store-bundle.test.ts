import { afterEach, describe, expect, it, vi } from "vitest";
import { mkdtemp, readFile, rm } from "node:fs/promises";
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

describe("library store bundle mode", () => {
  it("writes content and generated artifacts into the active library bundle", async () => {
    const tempRoot = await mkdtemp(path.join(os.tmpdir(), "classical-library-store-"));
    tempDirs.push(tempRoot);
    const libraryRoot = path.join(tempRoot, "library");
    process.env.ICM_ACTIVE_LIBRARY_DIR = libraryRoot;
    process.env.ICM_APP_DATA_DIR = path.join(tempRoot, "appdata");

    const { ensureLibraryBundle } = await import("../../packages/data-core/src/library-bundle.ts");
    const { saveLibraryToDisk, saveSiteConfig, saveArticlesToDisk, writeGeneratedArtifacts } = await import("../../packages/data-core/src/library-store.ts");
    await ensureLibraryBundle(libraryRoot, { libraryName: "测试库" });

    await saveLibraryToDisk({
      composers: [],
      people: [],
      workGroups: [],
      works: [],
      recordings: [],
    });
    await saveSiteConfig({
      title: "测试站点",
      subtitle: "",
      description: "",
      heroIntro: "",
      composerDirectoryIntro: "",
      conductorDirectoryIntro: "",
      searchIntro: "",
      about: [],
      contact: {
        label: "",
        value: "",
      },
      copyrightNotice: "",
      lastImportedAt: "",
    });
    await saveArticlesToDisk([]);
    await writeGeneratedArtifacts();

    await expect(readFile(path.join(libraryRoot, "content", "library", "composers.json"), "utf8")).resolves.toContain("[]");
    await expect(readFile(path.join(libraryRoot, "content", "site", "config.json"), "utf8")).resolves.toContain("测试站点");
    await expect(readFile(path.join(libraryRoot, "runtime", "generated", "library.json"), "utf8")).resolves.toContain("\"composers\": []");
    await expect(readFile(path.join(libraryRoot, "runtime", "generated", "site.json"), "utf8")).resolves.toContain("测试站点");
  });
});
