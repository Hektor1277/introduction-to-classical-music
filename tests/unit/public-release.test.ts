import { describe, expect, it } from "vitest";
import { promises as fs } from "node:fs";
import path from "node:path";

async function readJson<T>(filePath: string): Promise<T> {
  return JSON.parse(await fs.readFile(path.resolve(filePath), "utf8")) as T;
}

describe("public release repository", () => {
  it("exposes public packaging scripts and metadata", async () => {
    const packageJson = await readJson<Record<string, unknown>>("package.json");
    const scripts = (packageJson.scripts || {}) as Record<string, string>;
    const build = (packageJson.build || {}) as Record<string, unknown>;
    const win = (build.win || {}) as Record<string, unknown>;

    expect(packageJson.private).not.toBe(true);
    expect(scripts["bootstrap:windows"]).toContain("bootstrap-windows");
    expect(scripts["doctor:windows"]).toContain("doctor-windows");
    expect(scripts["package:windows"]).toContain("desktop:dist");
    expect(scripts["package:windows"]).toContain("doctor:windows");
    expect(JSON.stringify(win.target || [])).toContain("nsis");
    expect(JSON.stringify(win.target || [])).not.toContain("portable");
    expect((build as Record<string, unknown>).executableName).toBe("不全书");
    expect((build.nsis as Record<string, unknown>).oneClick).toBe(false);
    expect((build.nsis as Record<string, unknown>).allowToChangeInstallationDirectory).toBe(true);
    expect((build.nsis as Record<string, unknown>).include).toBe("scripts/windows/installer-compat.nsh");
  });

  it("tracks the recording retrieval service source and ignores only local byproducts", async () => {
    const gitignore = await fs.readFile(path.resolve(".gitignore"), "utf8");
    const lines = gitignore.split(/\r?\n/).map((line) => line.trim());
    const bootstrapScript = await fs.readFile(path.resolve("scripts/bootstrap-windows.ps1"), "utf8");

    expect(lines).not.toContain("tools/recording-retrieval-service/app/");
    expect(gitignore).toContain("tools/recording-retrieval-service/app/.venv/");
    expect(gitignore).toContain("tools/recording-retrieval-service/app/dist/");
    expect(gitignore).toContain("tools/recording-retrieval-service/app/cache/");
    expect(bootstrapScript).toContain("Invoke-NpmCiWithElectronFallback");
    expect(bootstrapScript).toContain("ELECTRON_MIRROR");
  });

  it("includes public licensing and release governance docs", async () => {
    await expect(fs.access(path.resolve("LICENSE"))).resolves.toBeUndefined();
    await expect(fs.access(path.resolve("NOTICE"))).resolves.toBeUndefined();
    await expect(fs.access(path.resolve("LICENSE-CONTENT.md"))).resolves.toBeUndefined();
    await expect(fs.access(path.resolve("CONTRIBUTING.md"))).resolves.toBeUndefined();
    await expect(fs.access(path.resolve("SECURITY.md"))).resolves.toBeUndefined();
    await expect(fs.access(path.resolve("CODE_OF_CONDUCT.md"))).resolves.toBeUndefined();
    await expect(fs.access(path.resolve("CHANGELOG.md"))).resolves.toBeUndefined();
    await expect(fs.access(path.resolve("RELEASING.md"))).resolves.toBeUndefined();
    await expect(fs.access(path.resolve("scripts/windows/installer-compat.nsh"))).resolves.toBeUndefined();
    await expect(fs.access(path.resolve("scripts/windows/legacy-install-cleanup.ps1"))).resolves.toBeUndefined();
  });

  it("keeps public repository seed data empty and limited to the default manual workflow", async () => {
    const [composers, people, workGroups, works, recordings, entityVitalsReview, reviewQueue, personLinks, siteConfig, articles] = await Promise.all([
      readJson<unknown[]>("data/library/composers.json"),
      readJson<unknown[]>("data/library/people.json"),
      readJson<unknown[]>("data/library/work-groups.json"),
      readJson<unknown[]>("data/library/works.json"),
      readJson<unknown[]>("data/library/recordings.json"),
      readJson<unknown[]>("data/library/entity-vitals-review.json"),
      readJson<unknown[]>("data/library/review-queue.json"),
      readJson<Record<string, Record<string, string>>>("data/library/person-links.json"),
      readJson<Record<string, unknown>>("data/site/config.json"),
      readJson<unknown[]>("data/site/articles.json"),
    ]);

    expect(composers).toEqual([]);
    expect(people).toEqual([]);
    expect(workGroups).toEqual([]);
    expect(works).toEqual([]);
    expect(recordings).toEqual([]);
    expect(entityVitalsReview).toEqual([]);
    expect(reviewQueue).toEqual([]);
    expect(personLinks).toEqual({ canonicalPersonLinks: {} });
    expect(siteConfig.contact).toEqual({ label: "", value: "" });
    expect(siteConfig.lastImportedAt).toBe("");
    expect(articles).toEqual([]);
  });

  it("excludes local automation state and runtime snapshots from the public repository", async () => {
    const [batchEntries, runEntries] = await Promise.all([
      fs.readdir(path.resolve("data/automation/batches")),
      fs.readdir(path.resolve("data/automation/runs")),
    ]);

    expect(batchEntries.filter((entry) => entry !== ".gitkeep")).toEqual([]);
    expect(runEntries.filter((entry) => entry !== ".gitkeep")).toEqual([]);
    await expect(fs.access(path.resolve("data/automation/settings.local.json"))).rejects.toThrow();
    await expect(fs.access(path.resolve("data/automation/recording-retrieval.local.json"))).rejects.toThrow();
  });

  it("runs CI on both ubuntu and windows, including bootstrap and packaging checks", async () => {
    const workflow = await fs.readFile(path.resolve(".github/workflows/ci.yml"), "utf8");

    expect(workflow).toContain("ubuntu-latest");
    expect(workflow).toContain("windows-latest");
    expect(workflow).toContain("npm run bootstrap:windows");
    expect(workflow).toContain("npm run doctor:windows");
    expect(workflow).toContain("npm run package:windows");
  });

  it("boots the owner app with an empty managed library instead of legacy repo seed data", async () => {
    const ownerServer = await fs.readFile(path.resolve("apps/owner/server/owner-app.ts"), "utf8");

    expect(ownerServer).toContain("seedFromLegacy: false");
    expect(ownerServer).not.toContain("seedFromLegacy: true");
  });
});
