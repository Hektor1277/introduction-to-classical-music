import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";

import { resolveLibraryAssetPath } from "@/lib/owner-assets";

describe("owner assets", () => {
  it("resolves encoded Chinese library asset paths", () => {
    const assetsRoot = fs.mkdtempSync(path.join(os.tmpdir(), "owner-assets-"));
    const assetDir = path.join(assetsRoot, "managed", "composers", "composer-布鲁克纳");
    const assetPath = path.join(assetDir, "composer-布鲁克纳-3c2a16492d.jpg");
    fs.mkdirSync(assetDir, { recursive: true });
    fs.writeFileSync(assetPath, "fixture");

    const resolved = resolveLibraryAssetPath(
      assetsRoot,
      "/library-assets/managed/composers/composer-%E5%B8%83%E9%B2%81%E5%85%8B%E7%BA%B3/composer-%E5%B8%83%E9%B2%81%E5%85%8B%E7%BA%B3-3c2a16492d.jpg",
    );

    expect(resolved).toBeTruthy();
    expect(path.normalize(resolved ?? "")).toContain(path.join("managed", "composers"));
    expect(path.basename(resolved ?? "")).toBe("composer-布鲁克纳-3c2a16492d.jpg");
    expect(path.basename(path.dirname(resolved ?? ""))).toBe("composer-布鲁克纳");
  });

  it("rejects traversal outside library-assets", () => {
    const assetsRoot = fs.mkdtempSync(path.join(os.tmpdir(), "owner-assets-"));
    const resolved = resolveLibraryAssetPath(assetsRoot, "/library-assets/../../package.json");

    expect(resolved).toBeNull();
  });
});
