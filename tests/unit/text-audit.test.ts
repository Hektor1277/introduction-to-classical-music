import { afterEach, describe, expect, it, vi } from "vitest";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";

import { collectTextAuditIssues, hasReplacementChar, hasUtf8Bom } from "../../scripts/lib/text-audit.js";

const tempDirs: string[] = [];

afterEach(async () => {
  while (tempDirs.length > 0) {
    const target = tempDirs.pop();
    if (target) {
      await rm(target, { recursive: true, force: true });
    }
  }
});

describe("text audit", () => {
  it("detects BOM and replacement characters in tracked text files", async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), "classical-text-audit-"));
    tempDirs.push(root);

    await mkdir(path.join(root, "src"), { recursive: true });
    await writeFile(path.join(root, "src", "bom.ts"), "\uFEFFexport const value = 1;\n", "utf8");
    await writeFile(path.join(root, "src", "bad.ts"), 'export const label = "bad\uFFFDtext";\n', "utf8");
    await writeFile(path.join(root, "src", "icon.svg"), "\uFEFF<svg>\uFFFD</svg>\n", "utf8");

    const issues = await collectTextAuditIssues(root);

    expect(issues).toEqual([
      expect.objectContaining({ code: "replacement-char", file: path.join("src", "bad.ts") }),
      expect.objectContaining({ code: "utf8-bom", file: path.join("src", "bom.ts") }),
      expect.objectContaining({ code: "replacement-char", file: path.join("src", "icon.svg") }),
      expect.objectContaining({ code: "utf8-bom", file: path.join("src", "icon.svg") }),
    ]);
  });

  it("ignores build output, backups, runtime archives and non-text files", async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), "classical-text-audit-ignore-"));
    tempDirs.push(root);

    await mkdir(path.join(root, "output"), { recursive: true });
    await mkdir(path.join(root, ".codex-backups", "snapshot"), { recursive: true });
    await mkdir(path.join(root, "data", "automation", "runs"), { recursive: true });
    await mkdir(path.join(root, "tools", "recording-retrieval-service", "app", ".venv"), { recursive: true });
    await writeFile(path.join(root, "output", "ignored.ts"), 'export const ignored = "\\uFFFD";\n', "utf8");
    await writeFile(path.join(root, ".codex-backups", "snapshot", "ignored.js"), "\uFEFFbackup\n", "utf8");
    await writeFile(path.join(root, "data", "automation", "runs", "run.json"), "{\n  \"bad\": \"\uFFFD\"\n}\n", "utf8");
    await writeFile(path.join(root, "tools", "recording-retrieval-service", "app", ".venv", "ignored.js"), 'const bad = "\\uFFFD";\n', "utf8");
    await writeFile(path.join(root, "favicon.ico"), Buffer.from([0, 1, 2, 3]));

    await expect(collectTextAuditIssues(root)).resolves.toEqual([]);
  });

  it("skips unreadable directories without failing the whole audit", async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), "classical-text-audit-eperm-"));
    tempDirs.push(root);

    await mkdir(path.join(root, "src"), { recursive: true });
    await mkdir(path.join(root, "blocked"), { recursive: true });
    await writeFile(path.join(root, "src", "safe.ts"), "export const ok = true;\n", "utf8");

    const originalReaddir = fs.readdir.bind(fs);
    const readdirSpy = vi.spyOn(fs, "readdir").mockImplementation(async (targetPath, options) => {
      if (String(targetPath).endsWith(`${path.sep}blocked`)) {
        throw Object.assign(new Error("blocked"), { code: "EPERM" });
      }
      return originalReaddir(targetPath, options);
    });

    await expect(collectTextAuditIssues(root)).resolves.toEqual([]);

    readdirSpy.mockRestore();
  });

  it("exposes small helpers for BOM and replacement-char checks", () => {
    expect(hasUtf8Bom("\uFEFFhello")).toBe(true);
    expect(hasUtf8Bom("hello")).toBe(false);
    expect(hasReplacementChar("a\uFFFDb")).toBe(true);
    expect(hasReplacementChar("normal text")).toBe(false);
  });
});
