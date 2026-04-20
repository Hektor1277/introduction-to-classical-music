import { promises as fs } from "node:fs";
import path from "node:path";

export const TEXT_AUDIT_IGNORED_DIRS = new Set([
  ".astro",
  ".codex-backups",
  ".git",
  ".playwright-cli",
  ".venv",
  ".worktrees",
  "node_modules",
  "output",
]);

export const TEXT_AUDIT_IGNORED_PATH_FRAGMENTS = [
  path.join("data", "automation", "runs"),
  path.join("tools", "recording-retrieval-service", "app", "dist"),
];

export const TEXT_AUDIT_EXTENSIONS = new Set([
  ".astro",
  ".bat",
  ".cmd",
  ".css",
  ".html",
  ".js",
  ".json",
  ".md",
  ".mjs",
  ".ps1",
  ".svg",
  ".ts",
  ".tsx",
  ".txt",
  ".yml",
  ".yaml",
]);

export function shouldAuditTextFile(filePath) {
  return TEXT_AUDIT_EXTENSIONS.has(path.extname(filePath).toLowerCase());
}

function shouldIgnorePath(fullPath) {
  const normalizedPath = path.normalize(fullPath);
  return TEXT_AUDIT_IGNORED_PATH_FRAGMENTS.some((fragment) => normalizedPath.includes(fragment));
}

export function hasUtf8Bom(content) {
  return content.charCodeAt(0) === 0xfeff;
}

export function hasReplacementChar(content) {
  return content.includes("\uFFFD");
}

function isSkippableFsError(error) {
  if (!error || typeof error !== "object" || !("code" in error)) {
    return false;
  }
  return error.code === "EACCES" || error.code === "EPERM" || error.code === "ENOENT";
}

async function* walkFiles(rootDir) {
  let entries;
  try {
    entries = await fs.readdir(rootDir, { withFileTypes: true });
  } catch (error) {
    if (isSkippableFsError(error)) {
      return;
    }
    throw error;
  }
  for (const entry of entries) {
    if (TEXT_AUDIT_IGNORED_DIRS.has(entry.name)) {
      continue;
    }

    const fullPath = path.join(rootDir, entry.name);
    if (shouldIgnorePath(fullPath)) {
      continue;
    }
    if (entry.isDirectory()) {
      yield* walkFiles(fullPath);
      continue;
    }

    if (entry.isFile()) {
      yield fullPath;
    }
  }
}

export async function collectTextAuditIssues(rootDir) {
  const issues = [];

  for await (const filePath of walkFiles(rootDir)) {
    if (!shouldAuditTextFile(filePath)) {
      continue;
    }

    let content;
    try {
      content = await fs.readFile(filePath, "utf8");
    } catch (error) {
      if (isSkippableFsError(error)) {
        continue;
      }
      throw error;
    }
    const relativePath = path.relative(rootDir, filePath);

    if (hasUtf8Bom(content)) {
      issues.push({
        code: "utf8-bom",
        file: relativePath,
        message: `${relativePath} contains a UTF-8 BOM`,
      });
    }

    if (hasReplacementChar(content)) {
      issues.push({
        code: "replacement-char",
        file: relativePath,
        message: `${relativePath} contains a Unicode replacement character`,
      });
    }
  }

  return issues.sort((left, right) => left.file.localeCompare(right.file) || left.code.localeCompare(right.code));
}
