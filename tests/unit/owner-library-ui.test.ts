import { describe, expect, it } from "vitest";
import { promises as fs } from "node:fs";
import path from "node:path";

describe("owner library management ui wiring", () => {
  it("exposes compact build, open, import, export, and refresh controls in the owner header", async () => {
    const ownerIndex = await fs.readFile(path.resolve("apps/owner/web/index.html"), "utf8");
    const ownerStyles = await fs.readFile(path.resolve("apps/owner/web/styles.css"), "utf8");

    expect(ownerIndex).toContain('id="owner-library-status"');
    expect(ownerIndex).toContain('id="rebuild-button"');
    expect(ownerIndex).toContain('id="library-open-button"');
    expect(ownerIndex).toContain('id="library-import-button"');
    expect(ownerIndex).toContain('id="library-export-button"');
    expect(ownerIndex).toContain('id="refresh-button"');
    expect(ownerIndex).toContain('class="owner-hero__actions"');
    expect(ownerStyles).toMatch(/html,\s*body\s*\{[\s\S]*overflow:\s*hidden/i);
    expect(ownerStyles).toMatch(/\.owner-shell\s*\{[\s\S]*height:\s*100vh/i);
    expect(ownerStyles).toMatch(/\.owner-hero__actions\s+button\s*\{[\s\S]*border:\s*0/i);
    expect(ownerStyles).toMatch(/\.owner-hero__actions\s+button\s*\+\s*button\s*\{[\s\S]*border-left:\s*1px\s+solid/i);
  });

  it("wires owner library actions to the new bundle-management endpoints", async () => {
    const ownerApp = await fs.readFile(path.resolve("apps/owner/web/app.js"), "utf8");
    const ownerServer = await fs.readFile(path.resolve("apps/owner/server/owner-app.ts"), "utf8");

    expect(ownerApp).toContain('/api/library/build-site');
    expect(ownerApp).toContain('/api/library/open-site');
    expect(ownerApp).toContain('/api/library/import');
    expect(ownerApp).toContain('/api/library/export');
    expect(ownerServer).toContain('app.post("/api/library/build-site"');
    expect(ownerServer).toContain('app.post("/api/library/open-site"');
    expect(ownerServer).toContain('app.post("/api/library/import"');
    expect(ownerServer).toContain('app.post("/api/library/export"');
  });

  it("exposes local resource link fields and open-resource wiring", async () => {
    const ownerIndex = await fs.readFile(path.resolve("apps/owner/web/index.html"), "utf8");
    const ownerApp = await fs.readFile(path.resolve("apps/owner/web/app.js"), "utf8");
    const ownerServer = await fs.readFile(path.resolve("apps/owner/server/owner-app.ts"), "utf8");

    expect(ownerIndex).toContain('name="linkType"');
    expect(ownerIndex).toContain('name="localPath"');
    expect(ownerIndex).toContain('data-link-dialog-action="browse-local"');
    expect(ownerApp).toContain("pickLocalResourceFile");
    expect(ownerApp).toContain('/api/open-resource');
    expect(ownerServer).toContain('app.post("/api/open-resource"');
  });
});
