import { describe, expect, it } from "vitest";
import { promises as fs } from "node:fs";
import path from "node:path";

const repoRoot = path.resolve(".");
const ignoredDirs = new Set([
  ".git",
  ".astro",
  ".codex",
  ".codex-handoff",
  ".worktrees",
  ".venv",
  "node_modules",
  "output",
  "target",
  "dist",
  "coverage",
  "build",
]);
const allowedExtensions = new Set([
  ".md",
  ".txt",
  ".ps1",
  ".cmd",
  ".yml",
  ".yaml",
]);
const forbiddenPatterns = [
  /[A-Z]:\\/,
  /\/[A-Z]:\//,
  /C:\\Users\\/,
  /C:\/Users\//,
  /HIT-IVAFFR/,
  /Anaconda/i,
  /[\\/]\.codex[\\/]/,
];
const ignoredFiles = new Set(["package-lock.json", "MANUAL_TEST_CHECKLIST.local.md", "public-docs-hygiene.test.ts"]);

async function collectTextFiles(rootDir: string): Promise<string[]> {
  const entries = await fs.readdir(rootDir, { withFileTypes: true });
  const files: string[] = [];
  for (const entry of entries) {
    if (ignoredDirs.has(entry.name)) {
      continue;
    }
    const fullPath = path.join(rootDir, entry.name);
    if (entry.isDirectory()) {
      files.push(...(await collectTextFiles(fullPath)));
      continue;
    }
    if (ignoredFiles.has(entry.name) || !allowedExtensions.has(path.extname(entry.name))) {
      continue;
    }
    files.push(fullPath);
  }
  return files;
}

describe("public documentation hygiene", () => {
  it("does not hardcode local absolute Windows paths or personal workspace markers", async () => {
    const files = await collectTextFiles(repoRoot);
    const offenders: string[] = [];

    for (const filePath of files) {
      const content = await fs.readFile(filePath, "utf8");
      if (forbiddenPatterns.some((pattern) => pattern.test(content))) {
        offenders.push(path.relative(repoRoot, filePath));
      }
    }

    expect(offenders).toEqual([]);
  });
});
