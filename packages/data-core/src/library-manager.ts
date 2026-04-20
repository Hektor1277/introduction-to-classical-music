import { promises as fs } from "node:fs";
import path from "node:path";

import { getRuntimePaths } from "./app-paths.js";
import { loadAppState, saveAppState } from "./app-state.js";
import {
  createDefaultLibraryArticles,
  getDefaultLibraryDocumentationAssetDirSegments,
  getDefaultLibraryDocumentationAssetFileNames,
  getDefaultLibraryDocumentationSourceDir,
} from "./default-library-content.js";
import {
  copyLibraryBundle,
  ensureLibraryBundle,
  getLibraryBundleEntityCounts,
  libraryBundleHasLegacySeedSource,
  readLibraryBundleManifest,
  seedLibraryBundleFromLegacySource,
} from "./library-bundle.js";

function sanitizeDirectoryName(value: string) {
  return String(value || "library")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9\u4e00-\u9fa5-]+/gi, "-")
    .replace(/-{2,}/g, "-")
    .replace(/^-|-$/g, "") || "library";
}

async function pathExists(targetPath: string) {
  try {
    await fs.access(targetPath);
    return true;
  } catch {
    return false;
  }
}

function isManagedLibraryRoot(rootDir: string) {
  const librariesDir = path.resolve(getRuntimePaths().appData.librariesDir);
  const resolvedRoot = path.resolve(rootDir);
  return resolvedRoot === librariesDir || resolvedRoot.startsWith(`${librariesDir}${path.sep}`);
}

function resolvePreferredDefaultLibraryRoot() {
  const configuredRoot = String(process.env.ICM_DEFAULT_LIBRARY_DIR || "").trim();
  if (configuredRoot) {
    return path.resolve(configuredRoot);
  }
  return path.join(getRuntimePaths().appData.librariesDir, "default-library");
}

function isDefaultLibraryRoot(rootDir: string) {
  const resolvedRoot = path.resolve(rootDir);
  return resolvedRoot === path.resolve(resolvePreferredDefaultLibraryRoot()) || path.basename(resolvedRoot) === "default-library";
}

async function ensureDefaultLibraryDocumentation(rootDir: string) {
  if (!isDefaultLibraryRoot(rootDir)) {
    return;
  }

  const runtimePaths = getRuntimePaths();
  const resolvedRoot = path.resolve(rootDir);
  const articlesPath = path.join(resolvedRoot, "content", "site", "articles.json");
  const assetTargetDir = path.join(resolvedRoot, "assets", "managed", ...getDefaultLibraryDocumentationAssetDirSegments());
  const assetSourceDir = getDefaultLibraryDocumentationSourceDir(runtimePaths.repoRoot);
  let existingArticles: unknown[] = [];
  try {
    const raw = await fs.readFile(articlesPath, "utf8");
    const parsed = JSON.parse(raw);
    existingArticles = Array.isArray(parsed) ? parsed : [];
  } catch {
    existingArticles = [];
  }

  if (existingArticles.length > 0) {
    return;
  }

  await fs.mkdir(path.dirname(articlesPath), { recursive: true });
  await fs.mkdir(assetTargetDir, { recursive: true });
  for (const assetFileName of getDefaultLibraryDocumentationAssetFileNames()) {
    await fs.copyFile(path.join(assetSourceDir, assetFileName), path.join(assetTargetDir, assetFileName));
  }
  await fs.writeFile(articlesPath, `${JSON.stringify(createDefaultLibraryArticles(), null, 2)}\n`, "utf8");
}

async function hydrateEmptyManagedLibraryFromSeed(
  rootDir: string,
  options: {
    defaultLibraryName?: string;
    seedFromLegacy?: boolean;
  } = {},
) {
  if (!(options.seedFromLegacy ?? true) || !isManagedLibraryRoot(rootDir)) {
    return;
  }

  const runtimePaths = getRuntimePaths();
  if (!(await libraryBundleHasLegacySeedSource(runtimePaths.repoRoot))) {
    return;
  }

  const counts = await getLibraryBundleEntityCounts(rootDir);
  if (counts.total > 0) {
    return;
  }

  const existingManifest = await ensureLibraryBundle(rootDir, {
    libraryName: options.defaultLibraryName || "My Library",
  });
  await seedLibraryBundleFromLegacySource(runtimePaths.repoRoot, rootDir, {
    libraryName: existingManifest.manifest.libraryName || options.defaultLibraryName || "My Library",
  });
}

async function resolveUniqueManagedLibraryRoot(baseName: string) {
  const librariesDir = getRuntimePaths().appData.librariesDir;
  await fs.mkdir(librariesDir, { recursive: true });
  const slug = sanitizeDirectoryName(baseName);
  let attempt = 0;
  while (true) {
    const candidate = path.join(librariesDir, attempt === 0 ? slug : `${slug}-${attempt + 1}`);
    if (!(await pathExists(candidate))) {
      return candidate;
    }
    attempt += 1;
  }
}

async function resolveUniqueExportRoot(baseDir: string, baseName: string) {
  const slug = sanitizeDirectoryName(baseName);
  let attempt = 0;
  while (true) {
    const candidate = path.join(baseDir, attempt === 0 ? slug : `${slug}-${attempt + 1}`);
    if (!(await pathExists(candidate))) {
      return candidate;
    }
    attempt += 1;
  }
}

async function listManagedLibraryRoots() {
  const librariesDir = getRuntimePaths().appData.librariesDir;
  try {
    const entries = await fs.readdir(librariesDir, { withFileTypes: true });
    return entries
      .filter((entry) => entry.isDirectory())
      .map((entry) => path.join(librariesDir, entry.name));
  } catch {
    return [];
  }
}

async function resolveFallbackManagedLibraryRoot(candidatePaths: string[] = []) {
  for (const candidatePath of candidatePaths) {
    const resolvedPath = path.resolve(candidatePath || "");
    if (resolvedPath && await pathExists(resolvedPath)) {
      return resolvedPath;
    }
  }

  const libraryRoots = await listManagedLibraryRoots();
  if (libraryRoots.length === 0) {
    return "";
  }

  const rankedLibraries = await Promise.all(
    libraryRoots.map(async (rootDir) => {
      const counts = await getLibraryBundleEntityCounts(rootDir);
      let mtimeMs = 0;
      try {
        const stats = await fs.stat(rootDir);
        mtimeMs = stats.mtimeMs;
      } catch {
        mtimeMs = 0;
      }
      return {
        rootDir,
        counts,
        mtimeMs,
        isDefaultLibrary: path.basename(rootDir) === "default-library",
      };
    }),
  );

  rankedLibraries.sort((left, right) => {
    if (left.isDefaultLibrary !== right.isDefaultLibrary) {
      return left.isDefaultLibrary ? 1 : -1;
    }
    if (left.counts.total !== right.counts.total) {
      return right.counts.total - left.counts.total;
    }
    return right.mtimeMs - left.mtimeMs;
  });

  return rankedLibraries[0]?.rootDir || "";
}

async function finalizeActivatedLibrary(
  rootDir: string,
  options: {
    defaultLibraryName?: string;
    seedFromLegacy?: boolean;
  } = {},
) {
  await ensureDefaultLibraryDocumentation(rootDir);
  const summary = await activateLibrary(rootDir, options);
  if (!(options.seedFromLegacy ?? true) || summary.counts.total > 0 || !isManagedLibraryRoot(rootDir)) {
    return summary;
  }

  const runtimePaths = getRuntimePaths();
  if (!(await libraryBundleHasLegacySeedSource(runtimePaths.repoRoot))) {
    return summary;
  }

  await seedLibraryBundleFromLegacySource(runtimePaths.repoRoot, rootDir, {
    libraryName: summary.manifest.libraryName || options.defaultLibraryName || "My Library",
  });
  return activateLibrary(rootDir, options);
}

export async function activateLibrary(
  rootDir: string,
  options: {
    defaultLibraryName?: string;
    seedFromLegacy?: boolean;
  } = {},
) {
  const resolvedRoot = path.resolve(rootDir);
  process.env.ICM_RUNTIME_MODE = "bundle";
  process.env.ICM_ACTIVE_LIBRARY_DIR = resolvedRoot;
  if (options.seedFromLegacy ?? false) {
    await hydrateEmptyManagedLibraryFromSeed(resolvedRoot, options);
  }
  await ensureLibraryBundle(resolvedRoot);
  const state = await loadAppState();
  await saveAppState({
    activeLibraryPath: resolvedRoot,
    recentLibraries: [resolvedRoot, ...(state.recentLibraries || []).filter((item) => path.resolve(item) !== resolvedRoot)].slice(0, 8),
  });
  return getActiveLibrarySummary();
}

export async function getActiveLibrarySummary() {
  const runtimePaths = getRuntimePaths();
  const manifest = await ensureLibraryBundle(runtimePaths.library.rootDir);
  const counts = await getLibraryBundleEntityCounts(runtimePaths.library.rootDir);
  let lastBuiltAt = "";
  try {
    const indexStats = await fs.stat(path.join(runtimePaths.library.buildSiteDir, "index.html"));
    lastBuiltAt = indexStats.mtime.toISOString();
  } catch {
    lastBuiltAt = "";
  }
  return {
    mode: runtimePaths.mode,
    rootDir: runtimePaths.library.rootDir,
    buildSiteDir: runtimePaths.library.buildSiteDir,
    manifest: manifest.manifest,
    lastBuiltAt,
    counts,
  };
}

export async function bootstrapActiveLibrary(options: {
  defaultLibraryName?: string;
  seedFromLegacy?: boolean;
} = {}) {
  process.env.ICM_RUNTIME_MODE = "bundle";
  const envLibraryRoot = process.env.ICM_ACTIVE_LIBRARY_DIR ? path.resolve(process.env.ICM_ACTIVE_LIBRARY_DIR) : "";
  if (envLibraryRoot && await pathExists(envLibraryRoot)) {
    await hydrateEmptyManagedLibraryFromSeed(envLibraryRoot, options);
    return finalizeActivatedLibrary(envLibraryRoot, options);
  }

  const state = await loadAppState();
  const savedLibraryRoot = state.activeLibraryPath ? path.resolve(state.activeLibraryPath) : "";
  if (savedLibraryRoot && await pathExists(savedLibraryRoot)) {
    await hydrateEmptyManagedLibraryFromSeed(savedLibraryRoot, options);
    return finalizeActivatedLibrary(savedLibraryRoot, options);
  }

  const fallbackLibraryRoot = await resolveFallbackManagedLibraryRoot(state.recentLibraries || []);
  if (fallbackLibraryRoot) {
    await hydrateEmptyManagedLibraryFromSeed(fallbackLibraryRoot, options);
    return finalizeActivatedLibrary(fallbackLibraryRoot, options);
  }

  const targetRoot = resolvePreferredDefaultLibraryRoot();
  if (options.seedFromLegacy ?? true) {
    try {
      await seedLibraryBundleFromLegacySource(getRuntimePaths().repoRoot, targetRoot, {
        libraryName: options.defaultLibraryName || "My Library",
      });
    } catch {
      try {
        await ensureLibraryBundle(targetRoot, { libraryName: options.defaultLibraryName || "My Library" });
      } catch {
        const fallbackRoot = path.join(getRuntimePaths().appData.librariesDir, "default-library");
        await ensureLibraryBundle(fallbackRoot, { libraryName: options.defaultLibraryName || "My Library" });
        return finalizeActivatedLibrary(fallbackRoot, options);
      }
    }
  } else {
    try {
      await ensureLibraryBundle(targetRoot, { libraryName: options.defaultLibraryName || "My Library" });
    } catch {
      const fallbackRoot = path.join(getRuntimePaths().appData.librariesDir, "default-library");
      await ensureLibraryBundle(fallbackRoot, { libraryName: options.defaultLibraryName || "My Library" });
      return finalizeActivatedLibrary(fallbackRoot, options);
    }
  }
  return finalizeActivatedLibrary(targetRoot, options);
}

export async function importLibraryBundle(sourceRoot: string) {
  const resolvedSourceRoot = path.resolve(sourceRoot);
  const manifest = await readLibraryBundleManifest(resolvedSourceRoot);
  const targetRoot = await resolveUniqueManagedLibraryRoot(manifest.libraryName || path.basename(resolvedSourceRoot));
  await copyLibraryBundle(resolvedSourceRoot, targetRoot);
  return activateLibrary(targetRoot);
}

export async function exportActiveLibraryBundle(destinationDir: string) {
  const resolvedDestinationDir = path.resolve(destinationDir);
  await fs.mkdir(resolvedDestinationDir, { recursive: true });
  const summary = await getActiveLibrarySummary();
  const targetRoot = await resolveUniqueExportRoot(
    resolvedDestinationDir,
    summary.manifest.libraryName || path.basename(summary.rootDir),
  );
  await copyLibraryBundle(summary.rootDir, targetRoot);
  return {
    exported: true,
    sourceRoot: summary.rootDir,
    exportedRoot: targetRoot,
    manifest: summary.manifest,
    counts: summary.counts,
  };
}
