import { afterEach, describe, expect, it, vi } from "vitest";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import http from "node:http";
import os from "node:os";
import path from "node:path";

const tempDirs: string[] = [];

async function canBindLoopbackPort() {
  const probe = http.createServer();
  return await new Promise<boolean>((resolve) => {
    probe.once("error", () => resolve(false));
    probe.listen(0, "127.0.0.1", () => {
      probe.close(() => resolve(true));
    });
  });
}

afterEach(async () => {
  vi.resetModules();
  while (tempDirs.length > 0) {
    const target = tempDirs.pop();
    if (target) {
      await rm(target, { recursive: true, force: true });
    }
  }
});

describe("local site server", () => {
  it("serves a built site directory over a local http url", async () => {
    if (!(await canBindLoopbackPort())) {
      return;
    }

    const tempRoot = await mkdtemp(path.join(os.tmpdir(), "classical-local-site-"));
    tempDirs.push(tempRoot);
    const siteRoot = path.join(tempRoot, "build", "site");
    await mkdir(siteRoot, { recursive: true });
    await writeFile(path.join(siteRoot, "index.html"), "<!doctype html><title>Library</title>", "utf8");

    const { createLocalSiteServer } = await import("../../packages/data-core/src/local-site-server.ts");
    const server = createLocalSiteServer({ preferredPort: 4591 });
    const started = await server.ensureStarted(siteRoot);
    const response = await fetch(started.url);
    const html = await response.text();
    await server.stop();

    expect(response.ok).toBe(true);
    expect(html).toContain("<title>Library</title>");
  });

  it("opens local resources through the same-origin bridge route", async () => {
    if (!(await canBindLoopbackPort())) {
      return;
    }

    const tempRoot = await mkdtemp(path.join(os.tmpdir(), "classical-local-site-"));
    tempDirs.push(tempRoot);
    const siteRoot = path.join(tempRoot, "build", "site");
    const mediaPath = path.join(tempRoot, "assets", "sample.mp3");
    await mkdir(siteRoot, { recursive: true });
    await mkdir(path.dirname(mediaPath), { recursive: true });
    await writeFile(path.join(siteRoot, "index.html"), "<!doctype html><title>Library</title>", "utf8");
    await writeFile(mediaPath, "sample", "utf8");

    const openedPaths: string[] = [];
    const { createLocalSiteServer } = await import("../../packages/data-core/src/local-site-server.ts");
    const server = createLocalSiteServer({
      preferredPort: 4592,
      onOpenLocalPath(targetPath) {
        openedPaths.push(targetPath);
      },
    });
    const started = await server.ensureStarted(siteRoot);
    const response = await fetch(`${started.url}/__local-resource?path=${encodeURIComponent(mediaPath)}`);
    const html = await response.text();
    await server.stop();

    expect(response.ok).toBe(true);
    expect(html).toContain("Local resource opened.");
    expect(openedPaths).toEqual([mediaPath]);
  });
});
