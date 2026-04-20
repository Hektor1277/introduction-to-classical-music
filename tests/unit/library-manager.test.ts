import { afterEach, describe, expect, it, vi } from "vitest";
import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

const originalEnv = { ...process.env };
const tempDirs: string[] = [];

const defaultLibraryGuideAssets = [
  "launcher-home.png",
  "owner-overview.png",
  "owner-work-form.png",
  "owner-recording-form-top.png",
  "owner-recording-links-guide.png",
  "owner-auto-check.png",
  "owner-review.png",
  "owner-batch-update.png",
  "owner-articles.png",
  "retrieval-home.png",
];

async function seedDefaultLibraryGuideAssets(repoRoot: string) {
  const assetRoot = path.join(repoRoot, "materials", "default-library", "usage-guide");
  await mkdir(assetRoot, { recursive: true });
  await Promise.all(
    defaultLibraryGuideAssets.map((fileName) =>
      writeFile(path.join(assetRoot, fileName), `asset:${fileName}`, "utf8")),
  );
}

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

describe("library manager", () => {
  it("bootstraps and persists the default library at the configured root in bundle mode", async () => {
    const tempRoot = await mkdtemp(path.join(os.tmpdir(), "classical-library-manager-"));
    tempDirs.push(tempRoot);
    const repoRoot = path.join(tempRoot, "repo");
    const appDataRoot = path.join(tempRoot, "app-data");
    const defaultLibraryRoot = path.join(tempRoot, "installed-app", "library");
    await mkdir(path.join(repoRoot, "data", "library"), { recursive: true });
    await mkdir(path.join(repoRoot, "data", "site"), { recursive: true });
    await mkdir(path.join(repoRoot, "apps", "site", "public", "library-assets", "legacy"), { recursive: true });
    await writeFile(path.join(repoRoot, "data", "library", "composers.json"), "[]\n", "utf8");
    await writeFile(path.join(repoRoot, "data", "library", "people.json"), "[]\n", "utf8");
    await writeFile(path.join(repoRoot, "data", "library", "work-groups.json"), "[]\n", "utf8");
    await writeFile(path.join(repoRoot, "data", "library", "works.json"), "[]\n", "utf8");
    await writeFile(path.join(repoRoot, "data", "library", "recordings.json"), "[]\n", "utf8");
    await writeFile(path.join(repoRoot, "data", "site", "config.json"), "{}\n", "utf8");
    await writeFile(path.join(repoRoot, "data", "site", "articles.json"), "[]\n", "utf8");
    await seedDefaultLibraryGuideAssets(repoRoot);
    process.env.ICM_REPO_ROOT = repoRoot;
    process.env.ICM_APP_DATA_DIR = appDataRoot;
    process.env.ICM_DEFAULT_LIBRARY_DIR = defaultLibraryRoot;
    process.env.ICM_RUNTIME_MODE = "bundle";

    const { bootstrapActiveLibrary } = await import("../../packages/data-core/src/library-manager.ts");
    const summary = await bootstrapActiveLibrary({ defaultLibraryName: "My Library", seedFromLegacy: false });

    expect(summary.rootDir).toBe(defaultLibraryRoot);
    expect(summary.counts.total).toBe(0);
    const state = JSON.parse(await readFile(path.join(appDataRoot, "state.json"), "utf8")) as {
      activeLibraryPath?: string;
      recentLibraries?: string[];
    };
    expect(path.normalize(state.activeLibraryPath || "")).toBe(path.normalize(defaultLibraryRoot));
    await expect(readFile(path.join(summary.rootDir, "library.manifest.json"), "utf8")).resolves.toContain('"libraryName": "My Library"');
    const seededArticles = await readFile(path.join(summary.rootDir, "content", "site", "articles.json"), "utf8");
    expect(seededArticles).toContain("不全书使用手册");
    expect(seededArticles).toContain('"showOnHome": true');
    expect(seededArticles).toContain("/library-assets/managed/articles/usage-guide/launcher-home.png");
    await expect(
      readFile(path.join(summary.rootDir, "assets", "managed", "articles", "usage-guide", "launcher-home.png")),
    ).resolves.toBeDefined();
  });

  it("imports a library bundle into the managed libraries directory and activates it", async () => {
    const tempRoot = await mkdtemp(path.join(os.tmpdir(), "classical-library-import-"));
    tempDirs.push(tempRoot);
    const appDataRoot = path.join(tempRoot, "app-data");
    const sourceRoot = path.join(tempRoot, "external-library");
    await mkdir(path.join(sourceRoot, "content", "library"), { recursive: true });
    await mkdir(path.join(sourceRoot, "content", "site"), { recursive: true });
    await mkdir(path.join(sourceRoot, "assets", "managed"), { recursive: true });
    await mkdir(path.join(sourceRoot, "build", "site"), { recursive: true });
    await mkdir(path.join(sourceRoot, "runtime"), { recursive: true });
    await mkdir(path.join(sourceRoot, "exports"), { recursive: true });
    await writeFile(path.join(sourceRoot, "library.manifest.json"), JSON.stringify({
      schemaVersion: "library-bundle-v1",
      libraryId: "lib-1",
      libraryName: "Portable Library",
      createdAt: "2026-04-17T00:00:00.000Z",
      updatedAt: "2026-04-17T00:00:00.000Z",
      appMinVersion: "0.1.0",
    }, null, 2), "utf8");
    process.env.ICM_REPO_ROOT = tempRoot;
    process.env.ICM_APP_DATA_DIR = appDataRoot;
    process.env.ICM_RUNTIME_MODE = "bundle";

    const { importLibraryBundle } = await import("../../packages/data-core/src/library-manager.ts");
    const summary = await importLibraryBundle(sourceRoot);

    expect(summary.rootDir).toBe(path.join(appDataRoot, "libraries", "portable-library"));
    await expect(readFile(path.join(summary.rootDir, "library.manifest.json"), "utf8")).resolves.toContain('"libraryName": "Portable Library"');
    await expect(readFile(path.join(appDataRoot, "state.json"), "utf8")).resolves.toContain("portable-library");
  });

  it("hydrates an empty managed library from packaged seed data when legacy content is available", async () => {
    const tempRoot = await mkdtemp(path.join(os.tmpdir(), "classical-library-hydrate-"));
    tempDirs.push(tempRoot);
    const repoRoot = path.join(tempRoot, "repo");
    const appDataRoot = path.join(tempRoot, "app-data");
    const managedRoot = path.join(appDataRoot, "libraries", "my-library");
    await mkdir(path.join(repoRoot, "data", "library"), { recursive: true });
    await mkdir(path.join(repoRoot, "data", "site"), { recursive: true });
    await mkdir(path.join(repoRoot, "apps", "site", "public", "library-assets", "managed"), { recursive: true });
    await mkdir(path.join(managedRoot, "content", "library"), { recursive: true });
    await mkdir(path.join(managedRoot, "content", "site"), { recursive: true });
    await writeFile(path.join(repoRoot, "data", "library", "composers.json"), '[{"id":"c-1","slug":"mahler","name":"马勒","nameLatin":"Mahler","country":"","countries":[],"avatarSrc":"","aliases":[],"sortKey":"001","summary":"","imageSourceUrl":"","imageSourceKind":"","imageAttribution":"","imageUpdatedAt":"","roles":["composer"],"infoPanel":{"text":"","articleId":"","collectionLinks":[]}}]\n', "utf8");
    await writeFile(path.join(repoRoot, "data", "library", "people.json"), "[]\n", "utf8");
    await writeFile(path.join(repoRoot, "data", "library", "work-groups.json"), "[]\n", "utf8");
    await writeFile(path.join(repoRoot, "data", "library", "works.json"), "[]\n", "utf8");
    await writeFile(path.join(repoRoot, "data", "library", "recordings.json"), "[]\n", "utf8");
    await writeFile(path.join(repoRoot, "data", "site", "config.json"), "{}\n", "utf8");
    await writeFile(path.join(repoRoot, "data", "site", "articles.json"), "[]\n", "utf8");
    await writeFile(path.join(managedRoot, "library.manifest.json"), JSON.stringify({
      schemaVersion: "library-bundle-v1",
      libraryId: "managed-1",
      libraryName: "Hydrate Me",
      createdAt: "2026-04-17T00:00:00.000Z",
      updatedAt: "2026-04-17T00:00:00.000Z",
      appMinVersion: "0.1.0",
    }, null, 2), "utf8");
    await writeFile(path.join(appDataRoot, "state.json"), JSON.stringify({
      activeLibraryPath: managedRoot,
      recentLibraries: [managedRoot],
    }, null, 2), "utf8");
    process.env.ICM_REPO_ROOT = repoRoot;
    process.env.ICM_APP_DATA_DIR = appDataRoot;
    process.env.ICM_RUNTIME_MODE = "bundle";

    const { bootstrapActiveLibrary } = await import("../../packages/data-core/src/library-manager.ts");
    const summary = await bootstrapActiveLibrary({ defaultLibraryName: "Hydrate Me" });

    expect(summary.rootDir).toBe(managedRoot);
    expect(summary.counts.composers).toBe(1);
    await expect(readFile(path.join(managedRoot, "content", "library", "composers.json"), "utf8")).resolves.toContain('"name":"马勒"');
  });

  it("exports the active library bundle to a chosen parent directory", async () => {
    const tempRoot = await mkdtemp(path.join(os.tmpdir(), "classical-library-export-"));
    tempDirs.push(tempRoot);
    const appDataRoot = path.join(tempRoot, "app-data");
    const activeRoot = path.join(appDataRoot, "libraries", "portable-library");
    const exportRoot = path.join(tempRoot, "exports");
    await mkdir(path.join(activeRoot, "content", "library"), { recursive: true });
    await mkdir(path.join(activeRoot, "content", "site"), { recursive: true });
    await mkdir(path.join(activeRoot, "assets", "managed"), { recursive: true });
    await mkdir(path.join(activeRoot, "build", "site"), { recursive: true });
    await mkdir(path.join(activeRoot, "runtime"), { recursive: true });
    await mkdir(path.join(activeRoot, "exports"), { recursive: true });
    await writeFile(path.join(activeRoot, "content", "library", "composers.json"), "[]\n", "utf8");
    await writeFile(path.join(activeRoot, "content", "library", "people.json"), "[]\n", "utf8");
    await writeFile(path.join(activeRoot, "content", "library", "works.json"), "[]\n", "utf8");
    await writeFile(path.join(activeRoot, "content", "library", "recordings.json"), "[]\n", "utf8");
    await writeFile(path.join(activeRoot, "library.manifest.json"), JSON.stringify({
      schemaVersion: "library-bundle-v1",
      libraryId: "lib-1",
      libraryName: "Portable Library",
      createdAt: "2026-04-17T00:00:00.000Z",
      updatedAt: "2026-04-17T00:00:00.000Z",
      appMinVersion: "0.1.0",
    }, null, 2), "utf8");
    await writeFile(path.join(appDataRoot, "state.json"), JSON.stringify({
      activeLibraryPath: activeRoot,
      recentLibraries: [activeRoot],
    }, null, 2), "utf8");
    process.env.ICM_REPO_ROOT = tempRoot;
    process.env.ICM_APP_DATA_DIR = appDataRoot;
    process.env.ICM_RUNTIME_MODE = "bundle";

    const { activateLibrary, exportActiveLibraryBundle } = await import("../../packages/data-core/src/library-manager.ts");
    await activateLibrary(activeRoot);
    const result = await exportActiveLibraryBundle(exportRoot);

    expect(result.exported).toBe(true);
    expect(result.exportedRoot).toBe(path.join(exportRoot, "portable-library"));
    await expect(readFile(path.join(result.exportedRoot, "library.manifest.json"), "utf8")).resolves.toContain('"libraryName": "Portable Library"');
  });

  it("falls back to an empty managed library when no legacy seed source is available", async () => {
    const tempRoot = await mkdtemp(path.join(os.tmpdir(), "classical-library-empty-fallback-"));
    tempDirs.push(tempRoot);
    const repoRoot = path.join(tempRoot, "repo-without-legacy");
    const appDataRoot = path.join(tempRoot, "app-data");
    await mkdir(repoRoot, { recursive: true });
    await seedDefaultLibraryGuideAssets(repoRoot);
    process.env.ICM_REPO_ROOT = repoRoot;
    process.env.ICM_APP_DATA_DIR = appDataRoot;
    process.env.ICM_RUNTIME_MODE = "bundle";

    const { bootstrapActiveLibrary } = await import("../../packages/data-core/src/library-manager.ts");
    const summary = await bootstrapActiveLibrary({ defaultLibraryName: "Empty Library" });

    expect(summary.rootDir).toBe(path.join(appDataRoot, "libraries", "default-library"));
    await expect(readFile(path.join(summary.rootDir, "library.manifest.json"), "utf8")).resolves.toContain('"libraryName": "Empty Library"');
    await expect(readFile(path.join(appDataRoot, "state.json"), "utf8")).resolves.toContain("default-library");
  });

  it("falls back to the managed app-data library when the configured default root cannot be created", async () => {
    const tempRoot = await mkdtemp(path.join(os.tmpdir(), "classical-library-default-fallback-"));
    tempDirs.push(tempRoot);
    const repoRoot = path.join(tempRoot, "repo");
    const appDataRoot = path.join(tempRoot, "app-data");
    const blockedRoot = path.join(tempRoot, "blocked-root");
    await mkdir(repoRoot, { recursive: true });
    await seedDefaultLibraryGuideAssets(repoRoot);
    await writeFile(blockedRoot, "not-a-directory", "utf8");
    process.env.ICM_REPO_ROOT = repoRoot;
    process.env.ICM_APP_DATA_DIR = appDataRoot;
    process.env.ICM_DEFAULT_LIBRARY_DIR = path.join(blockedRoot, "library");
    process.env.ICM_RUNTIME_MODE = "bundle";

    const { bootstrapActiveLibrary } = await import("../../packages/data-core/src/library-manager.ts");
    const summary = await bootstrapActiveLibrary({ defaultLibraryName: "Fallback Library", seedFromLegacy: false });

    expect(summary.rootDir).toBe(path.join(appDataRoot, "libraries", "default-library"));
    await expect(readFile(path.join(summary.rootDir, "library.manifest.json"), "utf8")).resolves.toContain('"libraryName": "Fallback Library"');
  });

  it("recovers an existing managed library when saved active path is stale", async () => {
    const tempRoot = await mkdtemp(path.join(os.tmpdir(), "classical-library-recover-"));
    tempDirs.push(tempRoot);
    const repoRoot = path.join(tempRoot, "repo");
    const appDataRoot = path.join(tempRoot, "app-data");
    const recoveredRoot = path.join(appDataRoot, "libraries", "my-library");
    await mkdir(path.join(repoRoot, "data", "library"), { recursive: true });
    await mkdir(path.join(repoRoot, "data", "site"), { recursive: true });
    await mkdir(path.join(repoRoot, "apps", "site", "public", "library-assets", "managed"), { recursive: true });
    await mkdir(path.join(recoveredRoot, "content", "library"), { recursive: true });
    await mkdir(path.join(recoveredRoot, "content", "site"), { recursive: true });
    await writeFile(path.join(repoRoot, "data", "library", "composers.json"), '[{"id":"c-1","slug":"mahler","name":"椹嫆","nameLatin":"Mahler","country":"","countries":[],"avatarSrc":"","aliases":[],"sortKey":"001","summary":"","imageSourceUrl":"","imageSourceKind":"","imageAttribution":"","imageUpdatedAt":"","roles":["composer"],"infoPanel":{"text":"","articleId":"","collectionLinks":[]}}]\n', "utf8");
    await writeFile(path.join(repoRoot, "data", "library", "people.json"), "[]\n", "utf8");
    await writeFile(path.join(repoRoot, "data", "library", "work-groups.json"), "[]\n", "utf8");
    await writeFile(path.join(repoRoot, "data", "library", "works.json"), "[]\n", "utf8");
    await writeFile(path.join(repoRoot, "data", "library", "recordings.json"), "[]\n", "utf8");
    await writeFile(path.join(repoRoot, "data", "site", "config.json"), "{}\n", "utf8");
    await writeFile(path.join(repoRoot, "data", "site", "articles.json"), "[]\n", "utf8");
    await writeFile(path.join(recoveredRoot, "library.manifest.json"), JSON.stringify({
      schemaVersion: "library-bundle-v1",
      libraryId: "managed-2",
      libraryName: "Recovered Library",
      createdAt: "2026-04-17T00:00:00.000Z",
      updatedAt: "2026-04-17T00:00:00.000Z",
      appMinVersion: "0.1.0",
    }, null, 2), "utf8");
    await writeFile(path.join(appDataRoot, "state.json"), JSON.stringify({
      activeLibraryPath: path.join(appDataRoot, "libraries", "missing-library"),
      recentLibraries: [path.join(appDataRoot, "libraries", "missing-library")],
    }, null, 2), "utf8");
    process.env.ICM_REPO_ROOT = repoRoot;
    process.env.ICM_APP_DATA_DIR = appDataRoot;
    process.env.ICM_RUNTIME_MODE = "bundle";

    const { bootstrapActiveLibrary } = await import("../../packages/data-core/src/library-manager.ts");
    const summary = await bootstrapActiveLibrary({ defaultLibraryName: "Recovered Library" });

    expect(summary.rootDir).toBe(recoveredRoot);
    expect(summary.counts.composers).toBe(1);
    await expect(readFile(path.join(appDataRoot, "state.json"), "utf8")).resolves.toContain("my-library");
  });
});
