import { existsSync } from "node:fs";
import { lstat, mkdir, readFile, readdir, realpath, rm, symlink } from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { spawnSync } from "node:child_process";

const LEGACY_ARCHIVE_NAME = "an incomplete guide to classical music.rar";
const LEGACY_ARCHIVE_ROOT = "an incomplete guide to classical music";

export function getSharedAssetTarget(rootDir, relativeAssetPath) {
  return path.resolve(rootDir, "apps", "site", "public", relativeAssetPath);
}

export function findSharedAssetSource(rootDir, relativeAssetPath, candidateRoots = [rootDir, path.resolve(rootDir, "..", "..")]) {
  const normalizedRoot = path.resolve(rootDir);
  for (const candidateRoot of candidateRoots) {
    const resolvedRoot = path.resolve(candidateRoot);
    if (resolvedRoot === normalizedRoot) {
      continue;
    }
    return path.resolve(resolvedRoot, "apps", "site", "public", relativeAssetPath);
  }
  return null;
}

async function readPathStats(targetPath) {
  try {
    return await lstat(targetPath);
  } catch (error) {
    if (error && typeof error === "object" && "code" in error && error.code === "ENOENT") {
      return null;
    }
    throw error;
  }
}

async function isReachable(targetPath) {
  try {
    await realpath(targetPath);
    return true;
  } catch {
    return false;
  }
}

async function isEmptyDirectory(targetPath) {
  try {
    const entries = await readdir(targetPath);
    return entries.length === 0;
  } catch {
    return false;
  }
}

function compact(value) {
  return String(value ?? "").trim();
}

export function getLegacyArchivePath(rootDir) {
  return path.resolve(rootDir, "materials", "archive", LEGACY_ARCHIVE_NAME);
}

export async function collectReferencedLegacyAssetRelativePaths(rootDir, relativeAssetPath = "library-assets/legacy") {
  const recordingsPath = path.resolve(rootDir, "data", "library", "recordings.json");
  const raw = await readFile(recordingsPath, "utf8");
  const recordings = JSON.parse(raw);
  const prefix = `/${relativeAssetPath.replace(/^\/+/, "").replace(/\\/g, "/")}/`;
  const seen = new Set();
  const relativePaths = [];
  for (const recording of Array.isArray(recordings) ? recordings : []) {
    for (const image of recording?.images || []) {
      const src = compact(image?.src).replace(/\\/g, "/");
      if (!src.startsWith(prefix)) {
        continue;
      }
      const relativePath = src.slice(prefix.length).replace(/^\/+/, "");
      if (!relativePath || seen.has(relativePath)) {
        continue;
      }
      seen.add(relativePath);
      relativePaths.push(relativePath);
    }
  }
  return relativePaths.sort((left, right) => left.localeCompare(right, "zh-CN"));
}

function isAcceptableTarWarning(stderr) {
  const message = compact(stderr);
  if (!message) {
    return true;
  }
  const lines = message
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
  if (lines.length === 0) {
    return true;
  }
  return lines.every(
    (line) =>
      line === "tar: Archive entry has empty or unreadable filename ... skipping." ||
      line === "tar: Error exit delayed from previous errors.",
  );
}

export async function restoreLegacyAssetDirectory(rootDir, relativeAssetPath = "library-assets/legacy") {
  if (relativeAssetPath !== "library-assets/legacy") {
    return null;
  }

  const targetPath = getSharedAssetTarget(rootDir, relativeAssetPath);
  const archivePath = getLegacyArchivePath(rootDir);
  if (!existsSync(archivePath)) {
    return {
      relativeAssetPath,
      targetPath,
      archivePath,
      status: "archive-missing",
      restoredCount: 0,
      missingCount: 0,
    };
  }

  const relativePaths = await collectReferencedLegacyAssetRelativePaths(rootDir, relativeAssetPath);
  const missingRelativePaths = relativePaths.filter((relativePath) => !existsSync(path.join(targetPath, relativePath)));
  if (missingRelativePaths.length === 0) {
    return {
      relativeAssetPath,
      targetPath,
      archivePath,
      status: "present",
      restoredCount: 0,
      missingCount: 0,
    };
  }

  await mkdir(targetPath, { recursive: true });
  const archiveEntries = missingRelativePaths.map((relativePath) => `${LEGACY_ARCHIVE_ROOT}/${relativePath.replace(/\\/g, "/")}`);
  const result = spawnSync("tar", ["-xf", archivePath, "-C", targetPath, "--strip-components=1", ...archiveEntries], {
    stdio: "pipe",
    encoding: "utf8",
  });

  if (result.error) {
    throw result.error;
  }

  const remainingMissing = missingRelativePaths.filter((relativePath) => !existsSync(path.join(targetPath, relativePath)));
  const restoredCount = missingRelativePaths.length - remainingMissing.length;
  const tarStatusAcceptable = result.status === 0 || (result.status === 1 && isAcceptableTarWarning(result.stderr));
  if (!tarStatusAcceptable) {
    return {
      relativeAssetPath,
      targetPath,
      archivePath,
      status: "archive-error",
      restoredCount,
      missingCount: remainingMissing.length,
      stderr: compact(result.stderr),
    };
  }

  return {
    relativeAssetPath,
    targetPath,
    archivePath,
    status: remainingMissing.length > 0 ? "restored-partial" : "restored-from-archive",
    restoredCount,
    missingCount: remainingMissing.length,
  };
}

export async function ensureSharedAssetLink(rootDir, relativeAssetPath, candidateRoots = [rootDir, path.resolve(rootDir, "..", "..")]) {
  const targetPath = getSharedAssetTarget(rootDir, relativeAssetPath);
  const existingStats = await readPathStats(targetPath);

  if (existingStats) {
    const isEmptyLegacyPlaceholder =
      relativeAssetPath === "library-assets/legacy" && existingStats.isDirectory() && (await isEmptyDirectory(targetPath));
    if ((existingStats.isSymbolicLink() && !(await isReachable(targetPath))) || isEmptyLegacyPlaceholder) {
      await rm(targetPath, { recursive: true, force: true });
    } else {
      return {
        relativeAssetPath,
        targetPath,
        status: existingStats.isSymbolicLink() ? "linked" : "present",
      };
    }
  }

  const restored = await restoreLegacyAssetDirectory(rootDir, relativeAssetPath);
  if (restored && (restored.status === "present" || restored.status === "restored-from-archive" || restored.status === "restored-partial")) {
    return restored;
  }

  const sourcePath = findSharedAssetSource(rootDir, relativeAssetPath, candidateRoots);
  if (!sourcePath || !(await isReachable(sourcePath))) {
    return {
      relativeAssetPath,
      targetPath,
      status: "missing-source",
    };
  }

  await mkdir(path.dirname(targetPath), { recursive: true });
  await symlink(sourcePath, targetPath, process.platform === "win32" ? "junction" : "dir");
  return {
    relativeAssetPath,
    sourcePath,
    targetPath,
    status: "linked",
  };
}
