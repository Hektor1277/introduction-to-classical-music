import { spawn } from "node:child_process";
import { once } from "node:events";
import { access, appendFile, mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";

import { APP_VERSION, getRuntimePaths } from "./app-paths.js";
import { syncLibraryAssetsToBuildSite } from "./library-build.js";
import { writeGeneratedArtifacts } from "./library-store.js";
import { createSiteBuildEnvironment } from "./site-runtime.js";

function resolveRuntimeWorkingDirectory(repoRoot: string) {
  const normalized = path.resolve(repoRoot);
  return normalized.endsWith(".asar") ? path.dirname(normalized) : normalized;
}

async function resolveAstroPackagePath(repoRoot: string) {
  const packagedPath = path.join(repoRoot, "node_modules", "astro", "package.json");
  try {
    await access(packagedPath);
    return packagedPath;
  } catch {
    const fallbackPath = path.join(process.cwd(), "node_modules", "astro", "package.json");
    await access(fallbackPath);
    return fallbackPath;
  }
}

async function writeBuildRuntimeLog(message: string) {
  try {
    const runtimePaths = getRuntimePaths();
    await mkdir(runtimePaths.appData.logsDir, { recursive: true });
    await appendFile(
      path.join(runtimePaths.appData.logsDir, "site-build-runtime.log"),
      `[${new Date().toISOString()}] ${message}\n`,
      "utf8",
    );
  } catch {
    // ignore build logging failures
  }
}

async function runAstroBuild() {
  const runtimePaths = getRuntimePaths();
  const env = createSiteBuildEnvironment();
  if (process.versions.electron) {
    env.ELECTRON_RUN_AS_NODE = "1";
  }
  const astroPackagePath = await resolveAstroPackagePath(runtimePaths.repoRoot);
  const astroPackage = JSON.parse(await readFile(astroPackagePath, "utf8")) as {
    bin?: string | Record<string, string>;
  };
  const astroBinRelativePath =
    typeof astroPackage.bin === "string"
      ? astroPackage.bin
      : typeof astroPackage.bin?.astro === "string"
        ? astroPackage.bin.astro
        : "astro.js";
  const astroCliPath = path.resolve(path.dirname(astroPackagePath), astroBinRelativePath);
  const astroRootDir = path.join(runtimePaths.repoRoot, "apps", "site");
  const astroCwd = resolveRuntimeWorkingDirectory(runtimePaths.repoRoot);
  await writeBuildRuntimeLog(`runAstroBuild: spawn ${process.execPath} ${astroCliPath} --root ${astroRootDir} cwd=${astroCwd}`);
  const child = spawn(process.execPath, [astroCliPath, "build", "--root", path.join(runtimePaths.repoRoot, "apps", "site")], {
    cwd: astroCwd,
    env,
    stdio: ["ignore", "pipe", "pipe"],
    windowsHide: true,
  });
  child.stdout?.on("data", (chunk) => {
    void writeBuildRuntimeLog(`runAstroBuild: stdout ${String(chunk).trimEnd()}`);
  });
  child.stderr?.on("data", (chunk) => {
    void writeBuildRuntimeLog(`runAstroBuild: stderr ${String(chunk).trimEnd()}`);
  });
  child.once("error", (error) => {
    void writeBuildRuntimeLog(`runAstroBuild: child error ${error.message}`);
  });
  const [code] = await once(child, "exit");
  await writeBuildRuntimeLog(`runAstroBuild: exit code=${String(code)}`);
  if (code !== 0) {
    throw new Error(`Astro build failed with exit code ${code}`);
  }
}

async function writeBuildMetadata() {
  const runtimePaths = getRuntimePaths();
  const metadataPath = path.join(runtimePaths.library.buildSiteDir, ".icm-build-meta.json");
  await mkdir(runtimePaths.library.buildSiteDir, { recursive: true });
  await writeFileIfChanged(
    metadataPath,
    `${JSON.stringify(
      {
        appVersion: APP_VERSION,
        builtAt: new Date().toISOString(),
      },
      null,
      2,
    )}\n`,
  );
}

async function writeFileIfChanged(targetPath: string, content: string) {
  try {
    const existing = await readFile(targetPath, "utf8");
    if (existing === content) {
      return;
    }
  } catch {
    // write below
  }
  await writeFile(targetPath, content, "utf8");
}

export async function buildLibrarySite(options: { includeLocalOnlyLinks?: boolean } = {}) {
  const runtimePaths = getRuntimePaths();
  await writeBuildRuntimeLog(`buildLibrarySite: begin root=${runtimePaths.library.rootDir}`);
  await writeBuildRuntimeLog("buildLibrarySite: writeGeneratedArtifacts:start");
  await writeGeneratedArtifacts(options);
  await writeBuildRuntimeLog("buildLibrarySite: writeGeneratedArtifacts:done");
  await writeBuildRuntimeLog("buildLibrarySite: runAstroBuild:start");
  await runAstroBuild();
  await writeBuildRuntimeLog("buildLibrarySite: runAstroBuild:done");
  await writeBuildRuntimeLog("buildLibrarySite: syncLibraryAssets:start");
  await syncLibraryAssetsToBuildSite({
    assetsDir: runtimePaths.library.assetsDir,
    buildSiteDir: runtimePaths.library.buildSiteDir,
  });
  await writeBuildRuntimeLog("buildLibrarySite: syncLibraryAssets:done");
  await writeBuildMetadata();
  await writeBuildRuntimeLog("buildLibrarySite: writeBuildMetadata:done");
  return {
    outputDir: runtimePaths.library.buildSiteDir,
  };
}
