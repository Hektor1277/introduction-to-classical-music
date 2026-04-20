import { describe, expect, it } from "vitest";
import { promises as fs } from "node:fs";
import path from "node:path";

describe("static assets wiring", () => {
  it("declares favicon links in both the public site and owner HTML shells", async () => {
    const siteLayout = await fs.readFile(path.resolve("apps/site/src/layouts/BaseLayout.astro"), "utf8");
    const ownerIndex = await fs.readFile(path.resolve("apps/owner/web/index.html"), "utf8");
    const siteHeader = await fs.readFile(path.resolve("apps/site/src/components/SiteHeader.astro"), "utf8");
    const aboutPage = await fs.readFile(path.resolve("apps/site/src/pages/about.astro"), "utf8");
    const composerIndex = await fs.readFile(path.resolve("apps/site/src/pages/composers/index.astro"), "utf8");
    const conductorIndex = await fs.readFile(path.resolve("apps/site/src/pages/conductors/index.astro"), "utf8");
    const searchPage = await fs.readFile(path.resolve("apps/site/src/pages/search.astro"), "utf8");
    const homePage = await fs.readFile(path.resolve("apps/site/src/pages/index.astro"), "utf8");

    expect(siteLayout).toContain('href="/favicon.ico"');
    expect(siteLayout).toContain('href="/favicon.svg"');
    expect(ownerIndex).toContain('href="/favicon.ico"');
    expect(ownerIndex).toContain('href="/favicon.svg"');
    expect(ownerIndex).toContain('name="composerDirectoryIntro"');
    expect(ownerIndex).toContain('name="conductorDirectoryIntro"');
    expect(ownerIndex).toContain('name="searchIntro"');
    expect(ownerIndex).toContain('name="showOnHome"');
    expect(siteHeader).toContain('label: "关于"');
    expect(siteHeader).not.toContain('label: "About"');
    expect(aboutPage).toContain('<p class="section-heading__eyebrow">About</p>');
    expect(aboutPage).not.toContain("更新方式");
    expect(composerIndex).toContain("site.composerDirectoryIntro");
    expect(conductorIndex).toContain("site.conductorDirectoryIntro");
    expect(searchPage).toContain("site.searchIntro");
    expect(homePage).toContain("buildFeaturedArticles");
    expect(homePage).toContain("featuredArticles");
    expect(homePage).toContain("recommendationItems.length > 0");
  });

  it("serves favicon routes from the shared site public directory for the owner tool", async () => {
    const ownerServer = await fs.readFile(path.resolve("apps/owner/server/owner-app.ts"), "utf8");

    expect(ownerServer).toContain('app.get("/favicon.ico"');
    expect(ownerServer).toContain('app.get("/favicon.svg"');
    expect(ownerServer).toContain('readFile as readBinaryFile');
    expect(ownerServer).toContain('response.type("image/x-icon")');
    expect(ownerServer).toContain('response.type("image/svg+xml")');
  });

  it("proxies remote owner image previews through a same-origin route", async () => {
    const ownerServer = await fs.readFile(path.resolve("apps/owner/server/owner-app.ts"), "utf8");

    expect(ownerServer).toContain('app.get("/api/remote-image"');
    expect(ownerServer).toContain('new URL(String(request.query.url || ""))');
    expect(ownerServer).toContain('response.type(contentType)');
    expect(ownerServer).toContain('Buffer.from(await upstream.arrayBuffer())');
  });

  it("builds the release site from the bundled default library instead of legacy repo data", async () => {
    const buildScript = await fs.readFile(path.resolve("scripts/build-library-site.ts"), "utf8");
    const afterPackScript = await fs.readFile(path.resolve("scripts/electron-after-pack.mjs"), "utf8");

    expect(buildScript).toContain('release-default-library');
    expect(buildScript).toContain('release-appdata');
    expect(buildScript).toContain("rm(releaseLibraryRoot");
    expect(buildScript).toContain("rm(releaseAppDataRoot");
    expect(buildScript).toContain("bootstrapActiveLibrary");
    expect(buildScript).toContain("seedFromLegacy: false");
    expect(buildScript).toContain('defaultLibraryName: "默认资料库"');
    expect(afterPackScript).toContain("release-default-library");
    expect(afterPackScript).toContain('path.join(context.appOutDir, "library")');
  });
});
