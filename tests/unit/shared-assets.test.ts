import { access, mkdtemp, mkdir, readlink, rm, writeFile } from "node:fs/promises";
import { spawnSync } from "node:child_process";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";

import {
  collectReferencedLegacyAssetRelativePaths,
  ensureSharedAssetLink,
  findSharedAssetSource,
  getLegacyArchivePath,
  getSharedAssetTarget,
  restoreLegacyAssetDirectory,
} from "../../scripts/lib/shared-assets.js";

const tempDirs: string[] = [];

afterEach(async () => {
  while (tempDirs.length > 0) {
    const target = tempDirs.pop();
    if (target) {
      await rm(target, { recursive: true, force: true });
    }
  }
});

describe("shared assets helpers", () => {
  it("targets the local worktree public asset path", () => {
    expect(getSharedAssetTarget("E:/repo/.worktrees/feature", "library-assets/legacy")).toBe(
      path.resolve("E:/repo/.worktrees/feature", "apps/site/public/library-assets/legacy"),
    );
  });

  it("finds the primary repository asset source outside the worktree", () => {
    const rootDir = "E:/repo/.worktrees/feature";
    const source = findSharedAssetSource(rootDir, "library-assets/legacy", [
      rootDir,
      "E:/repo",
      "E:/elsewhere",
    ]);
    expect(source).toBe(path.resolve("E:/repo", "apps/site/public/library-assets/legacy"));
  });

  it("rebuilds a broken shared-asset link instead of failing on an existing dead link", async () => {
    const sandboxRoot = await mkdtemp(path.join(os.tmpdir(), "classical-shared-assets-"));
    tempDirs.push(sandboxRoot);

    const repoRoot = path.join(sandboxRoot, "repo");
    const worktreeRoot = path.join(repoRoot, ".worktrees", "feature");
    const relativeAssetPath = "library-assets/legacy";
    const sourcePath = path.join(repoRoot, "apps", "site", "public", relativeAssetPath);

    await mkdir(sourcePath, { recursive: true });
    await writeFile(path.join(sourcePath, "cover.txt"), "ok\n", "utf8");

    const first = await ensureSharedAssetLink(worktreeRoot, relativeAssetPath, [worktreeRoot, repoRoot]);
    expect(first.status).toBe("linked");

    await rm(sourcePath, { recursive: true, force: true });
    const missing = await ensureSharedAssetLink(worktreeRoot, relativeAssetPath, [worktreeRoot, repoRoot]);
    expect(missing.status).toBe("missing-source");
    await expect(access(first.targetPath)).rejects.toThrow();

    await mkdir(sourcePath, { recursive: true });
    await writeFile(path.join(sourcePath, "cover.txt"), "restored\n", "utf8");

    const restored = await ensureSharedAssetLink(worktreeRoot, relativeAssetPath, [worktreeRoot, repoRoot]);
    expect(restored.status).toBe("linked");
    if (process.platform === "win32") {
      await expect(access(path.join(restored.targetPath, "cover.txt"))).resolves.toBeUndefined();
    } else {
      await expect(readlink(restored.targetPath)).resolves.toBe(sourcePath);
    }
  });

  it("treats an empty legacy placeholder directory as broken and reports missing source", async () => {
    const sandboxRoot = await mkdtemp(path.join(os.tmpdir(), "classical-shared-assets-"));
    tempDirs.push(sandboxRoot);

    const repoRoot = path.join(sandboxRoot, "repo");
    const worktreeRoot = path.join(repoRoot, ".worktrees", "feature");
    const relativeAssetPath = "library-assets/legacy";
    const targetPath = path.join(worktreeRoot, "apps", "site", "public", relativeAssetPath);

    await mkdir(targetPath, { recursive: true });

    const result = await ensureSharedAssetLink(worktreeRoot, relativeAssetPath, [worktreeRoot, repoRoot]);
    expect(result.status).toBe("missing-source");
    await expect(access(targetPath)).rejects.toThrow();
  });

  it("collects unique legacy asset paths referenced by recordings", async () => {
    const sandboxRoot = await mkdtemp(path.join(os.tmpdir(), "classical-shared-assets-"));
    tempDirs.push(sandboxRoot);

    const repoRoot = path.join(sandboxRoot, "repo");
    await mkdir(path.join(repoRoot, "data", "library"), { recursive: true });
    await writeFile(
      path.join(repoRoot, "data", "library", "recordings.json"),
      JSON.stringify(
        [
          {
            id: "recording-1",
            images: [
              { src: "/library-assets/legacy/pic/foo/bar.jpg" },
              { src: "/library-assets/legacy/pic/foo/bar.jpg" },
              { src: "https://example.com/cover.jpg" },
            ],
          },
          {
            id: "recording-2",
            images: [{ src: "/library-assets/legacy/pic/foo/baz.png" }],
          },
        ],
        null,
        2,
      ),
      "utf8",
    );

    const result = await collectReferencedLegacyAssetRelativePaths(repoRoot);
    expect(result).toEqual(["pic/foo/bar.jpg", "pic/foo/baz.png"]);
  });

  it("restores missing legacy recording assets from the local archive", async () => {
    const sandboxRoot = await mkdtemp(path.join(os.tmpdir(), "classical-shared-assets-"));
    tempDirs.push(sandboxRoot);

    const repoRoot = path.join(sandboxRoot, "repo");
    const archiveRoot = path.join(sandboxRoot, "archive-source");
    const sourceFile = path.join(archiveRoot, "an incomplete guide to classical music", "pic", "foo", "bar.jpg");
    const archivePath = getLegacyArchivePath(repoRoot);

    await mkdir(path.join(repoRoot, "data", "library"), { recursive: true });
    await mkdir(path.dirname(sourceFile), { recursive: true });
    await mkdir(path.dirname(archivePath), { recursive: true });

    await writeFile(
      path.join(repoRoot, "data", "library", "recordings.json"),
      JSON.stringify(
        [
          {
            id: "recording-1",
            images: [{ src: "/library-assets/legacy/pic/foo/bar.jpg" }],
          },
        ],
        null,
        2,
      ),
      "utf8",
    );
    await writeFile(sourceFile, "legacy-image", "utf8");

    const archiveResult = spawnSync("tar", ["-cf", archivePath, "-C", archiveRoot, "an incomplete guide to classical music"], {
      stdio: "pipe",
      encoding: "utf8",
    });
    expect(archiveResult.status).toBe(0);

    const restored = await restoreLegacyAssetDirectory(repoRoot);
    expect(restored?.status).toBe("restored-from-archive");
    expect(restored?.restoredCount).toBe(1);
    await expect(access(path.join(repoRoot, "apps", "site", "public", "library-assets", "legacy", "pic", "foo", "bar.jpg"))).resolves.toBeUndefined();
  });
});
