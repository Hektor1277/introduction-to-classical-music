import { afterEach, describe, expect, it, vi } from "vitest";
import os from "node:os";
import path from "node:path";

afterEach(() => {
  delete process.env.ICM_ACTIVE_LIBRARY_DIR;
  delete process.env.ICM_APP_DATA_DIR;
  delete process.env.ICM_REPO_ROOT;
  vi.resetModules();
});

describe("site runtime", () => {
  it("uses the default output directory in legacy mode", async () => {
    process.env.ICM_REPO_ROOT = path.join(os.tmpdir(), "icm-site-runtime-legacy");
    const { createSiteBuildEnvironment } = await import("../../packages/data-core/src/site-runtime.ts");

    const env = createSiteBuildEnvironment({});
    expect(env.ICM_SITE_OUT_DIR).toBe(path.join(process.env.ICM_REPO_ROOT, "output", "site"));
    expect(env.ICM_ACTIVE_LIBRARY_DIR).toBeUndefined();
  });

  it("points Astro at the active library build/site directory in bundle mode", async () => {
    process.env.ICM_REPO_ROOT = path.join(os.tmpdir(), "icm-site-runtime-bundle");
    process.env.ICM_ACTIVE_LIBRARY_DIR = path.join(os.tmpdir(), "classical-library");
    process.env.ICM_APP_DATA_DIR = path.join(os.tmpdir(), "classical-app-data");
    const { createSiteBuildEnvironment } = await import("../../packages/data-core/src/site-runtime.ts");

    const env = createSiteBuildEnvironment(process.env);
    expect(env.ICM_ACTIVE_LIBRARY_DIR).toBe(process.env.ICM_ACTIVE_LIBRARY_DIR);
    expect(env.ICM_SITE_OUT_DIR).toBe(path.join(process.env.ICM_ACTIVE_LIBRARY_DIR, "build", "site"));
    expect(env.ICM_APP_DATA_DIR).toBe(process.env.ICM_APP_DATA_DIR);
  });
});
