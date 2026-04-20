import { afterEach, describe, expect, it, vi } from "vitest";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

const tempDirs: string[] = [];

afterEach(async () => {
  delete process.env.ICM_ACTIVE_LIBRARY_DIR;
  delete process.env.ICM_APP_DATA_DIR;
  vi.resetModules();
  while (tempDirs.length > 0) {
    const target = tempDirs.pop();
    if (target) {
      await rm(target, { recursive: true, force: true });
    }
  }
});

describe("site asset resolution", () => {
  it("falls back to an empty image src when a local site asset does not exist", async () => {
    const { resolveSiteImageSrc } = await import("@/lib/site-assets");
    const resolved = resolveSiteImageSrc(
      "/library-assets/legacy/pic/马勒/交响曲/第五交响曲/巴尔沙伊1999.png",
      () => false,
    );

    expect(resolved).toBe("");
  });

  it("keeps remote image URLs unchanged", async () => {
    const { resolveSiteImageSrc } = await import("@/lib/site-assets");
    const resolved = resolveSiteImageSrc("https://img.example.com/cover.jpg", () => false);

    expect(resolved).toBe("https://img.example.com/cover.jpg");
  });

  it("filters out broken local recording images while preserving valid ones", async () => {
    const { filterRenderableRecordingImages } = await import("@/lib/site-assets");
    const images = filterRenderableRecordingImages(
      [
        { src: "/library-assets/legacy/pic/missing.png", alt: "broken" },
        { src: "/library-assets/legacy/pic/ok.png", alt: "ok" },
        { src: "https://img.example.com/remote.jpg", alt: "remote" },
      ],
      (src) => src.endsWith("/ok.png"),
    );

    expect(images).toEqual([
      { src: "/library-assets/legacy/pic/ok.png", alt: "ok" },
      { src: "https://img.example.com/remote.jpg", alt: "remote" },
    ]);
  });

  it("checks bundle assets from the active library when running in bundle mode", async () => {
    const tempRoot = await mkdtemp(path.join(os.tmpdir(), "site-assets-bundle-"));
    tempDirs.push(tempRoot);
    const libraryRoot = path.join(tempRoot, "library");
    const appDataRoot = path.join(tempRoot, "app-data");
    const assetPath = path.join(libraryRoot, "assets", "managed", "recordings", "cover.jpg");
    await mkdir(path.dirname(assetPath), { recursive: true });
    await writeFile(assetPath, "fixture", "utf8");
    process.env.ICM_ACTIVE_LIBRARY_DIR = libraryRoot;
    process.env.ICM_APP_DATA_DIR = appDataRoot;

    const { resolveSiteImageSrc } = await import("@/lib/site-assets");
    expect(resolveSiteImageSrc("/library-assets/managed/recordings/cover.jpg")).toBe("/library-assets/managed/recordings/cover.jpg");
  });
});
