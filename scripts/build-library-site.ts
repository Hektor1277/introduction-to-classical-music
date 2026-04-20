import { cp, mkdir, rm } from "node:fs/promises";
import path from "node:path";

import { buildLibrarySite } from "../packages/data-core/src/site-build-runner.js";
import { bootstrapActiveLibrary } from "../packages/data-core/src/library-manager.js";

async function main() {
  const repoRoot = path.resolve(process.env.ICM_REPO_ROOT || process.cwd());
  const releaseLibraryRoot = path.join(repoRoot, "output", "release-default-library");
  const releaseAppDataRoot = path.join(repoRoot, "output", "release-appdata");
  const outputSiteDir = path.join(repoRoot, "output", "site");

  process.env.ICM_RUNTIME_MODE = "bundle";
  process.env.ICM_REPO_ROOT = repoRoot;
  process.env.ICM_DEFAULT_LIBRARY_DIR = releaseLibraryRoot;
  process.env.ICM_ACTIVE_LIBRARY_DIR = releaseLibraryRoot;
  process.env.ICM_APP_DATA_DIR = releaseAppDataRoot;

  await rm(releaseLibraryRoot, { recursive: true, force: true });
  await rm(releaseAppDataRoot, { recursive: true, force: true });
  await bootstrapActiveLibrary({
    defaultLibraryName: "默认资料库",
    seedFromLegacy: false,
  });

  const result = await buildLibrarySite();

  await rm(outputSiteDir, { recursive: true, force: true });
  await mkdir(path.dirname(outputSiteDir), { recursive: true });
  await cp(result.outputDir, outputSiteDir, { recursive: true, force: true });

  process.stdout.write(`Built site into ${outputSiteDir}\n`);
}

main().catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.stack : String(error)}\n`);
  process.exitCode = 1;
});
