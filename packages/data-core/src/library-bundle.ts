import { randomUUID } from "node:crypto";
import { promises as fs } from "node:fs";
import path from "node:path";

import { APP_VERSION, LIBRARY_BUNDLE_SCHEMA_VERSION } from "./app-paths.js";

export type LibraryBundleManifest = {
  schemaVersion: typeof LIBRARY_BUNDLE_SCHEMA_VERSION;
  libraryId: string;
  libraryName: string;
  createdAt: string;
  updatedAt: string;
  appMinVersion: string;
};

export async function ensureLibraryBundle(
  rootDir: string,
  options: {
    libraryName?: string;
    appMinVersion?: string;
  } = {},
) {
  const resolvedRoot = path.resolve(rootDir);
  const manifestPath = path.join(resolvedRoot, "library.manifest.json");
  const now = new Date().toISOString();

  const directories = [
    path.join(resolvedRoot, "content", "library"),
    path.join(resolvedRoot, "content", "site"),
    path.join(resolvedRoot, "assets", "managed"),
    path.join(resolvedRoot, "assets", "imported"),
    path.join(resolvedRoot, "build", "site"),
    path.join(resolvedRoot, "runtime"),
    path.join(resolvedRoot, "exports"),
  ];

  await Promise.all(directories.map((directory) => fs.mkdir(directory, { recursive: true })));

  const scaffoldFiles = [
    { filePath: path.join(resolvedRoot, "content", "library", "composers.json"), contents: "[]\n" },
    { filePath: path.join(resolvedRoot, "content", "library", "people.json"), contents: "[]\n" },
    { filePath: path.join(resolvedRoot, "content", "library", "work-groups.json"), contents: "[]\n" },
    { filePath: path.join(resolvedRoot, "content", "library", "works.json"), contents: "[]\n" },
    { filePath: path.join(resolvedRoot, "content", "library", "recordings.json"), contents: "[]\n" },
    { filePath: path.join(resolvedRoot, "content", "library", "person-links.json"), contents: '{\n  "canonicalPersonLinks": {}\n}\n' },
    { filePath: path.join(resolvedRoot, "content", "library", "review-queue.json"), contents: "[]\n" },
    { filePath: path.join(resolvedRoot, "content", "library", "entity-vitals-review.json"), contents: "[]\n" },
    { filePath: path.join(resolvedRoot, "content", "site", "config.json"), contents: "{}\n" },
    { filePath: path.join(resolvedRoot, "content", "site", "articles.json"), contents: "[]\n" },
  ];

  await Promise.all(scaffoldFiles.map(async ({ filePath, contents }) => {
    try {
      await fs.access(filePath);
    } catch {
      await fs.writeFile(filePath, contents, "utf8");
    }
  }));

  let manifest: LibraryBundleManifest;
  try {
    manifest = JSON.parse(await fs.readFile(manifestPath, "utf8")) as LibraryBundleManifest;
  } catch {
    manifest = {
      schemaVersion: LIBRARY_BUNDLE_SCHEMA_VERSION,
      libraryId: randomUUID(),
      libraryName: options.libraryName || path.basename(resolvedRoot) || "My Library",
      createdAt: now,
      updatedAt: now,
      appMinVersion: options.appMinVersion || APP_VERSION,
    };
    await fs.writeFile(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`, "utf8");
  }

  return {
    rootDir: resolvedRoot,
    manifestPath,
    manifest,
  };
}

export async function readLibraryBundleManifest(rootDir: string) {
  const manifestPath = path.join(path.resolve(rootDir), "library.manifest.json");
  const raw = await fs.readFile(manifestPath, "utf8");
  return JSON.parse(raw) as LibraryBundleManifest;
}

async function readJsonArrayLength(filePath: string) {
  try {
    const content = await fs.readFile(filePath, "utf8");
    const parsed = JSON.parse(content);
    return Array.isArray(parsed) ? parsed.length : 0;
  } catch {
    return 0;
  }
}

export async function getLibraryBundleEntityCounts(rootDir: string) {
  const resolvedRoot = path.resolve(rootDir);
  const libraryDir = path.join(resolvedRoot, "content", "library");
  const [composers, people, works, recordings] = await Promise.all([
    readJsonArrayLength(path.join(libraryDir, "composers.json")),
    readJsonArrayLength(path.join(libraryDir, "people.json")),
    readJsonArrayLength(path.join(libraryDir, "works.json")),
    readJsonArrayLength(path.join(libraryDir, "recordings.json")),
  ]);

  return {
    composers,
    people,
    works,
    recordings,
    total: composers + people + works + recordings,
  };
}

export async function libraryBundleHasLegacySeedSource(sourceRoot: string) {
  const resolvedSourceRoot = path.resolve(sourceRoot);
  const requiredPaths = [
    path.join(resolvedSourceRoot, "data", "library", "composers.json"),
    path.join(resolvedSourceRoot, "data", "library", "people.json"),
    path.join(resolvedSourceRoot, "data", "library", "work-groups.json"),
    path.join(resolvedSourceRoot, "data", "library", "works.json"),
    path.join(resolvedSourceRoot, "data", "library", "recordings.json"),
    path.join(resolvedSourceRoot, "data", "site", "config.json"),
    path.join(resolvedSourceRoot, "data", "site", "articles.json"),
  ];

  try {
    await Promise.all(requiredPaths.map((targetPath) => fs.access(targetPath)));
    return true;
  } catch {
    return false;
  }
}

export async function copyLibraryBundle(sourceRoot: string, targetRoot: string) {
  const resolvedSourceRoot = path.resolve(sourceRoot);
  const resolvedTargetRoot = path.resolve(targetRoot);
  await fs.mkdir(path.dirname(resolvedTargetRoot), { recursive: true });
  await fs.rm(resolvedTargetRoot, { recursive: true, force: true });
  await fs.cp(resolvedSourceRoot, resolvedTargetRoot, { recursive: true, force: true });
  return ensureLibraryBundle(resolvedTargetRoot);
}

export async function seedLibraryBundleFromLegacySource(
  sourceRoot: string,
  libraryRoot: string,
  options: {
    libraryName?: string;
    appMinVersion?: string;
  } = {},
) {
  await ensureLibraryBundle(libraryRoot, options);

  const resolvedSourceRoot = path.resolve(sourceRoot);
  const resolvedLibraryRoot = path.resolve(libraryRoot);
  const copyPairs = [
    {
      from: path.join(resolvedSourceRoot, "data", "library"),
      to: path.join(resolvedLibraryRoot, "content", "library"),
    },
    {
      from: path.join(resolvedSourceRoot, "data", "site"),
      to: path.join(resolvedLibraryRoot, "content", "site"),
    },
    {
      from: path.join(resolvedSourceRoot, "apps", "site", "public", "library-assets"),
      to: path.join(resolvedLibraryRoot, "assets"),
    },
  ];

  for (const pair of copyPairs) {
    await fs.mkdir(pair.to, { recursive: true });
    await fs.cp(pair.from, pair.to, { recursive: true, force: true });
  }

  return ensureLibraryBundle(libraryRoot, options);
}
