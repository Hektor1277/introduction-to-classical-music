import { promises as fs } from "node:fs";
import path from "node:path";

export async function syncLibraryAssetsToBuildSite(options: {
  assetsDir: string;
  buildSiteDir: string;
}) {
  const assetsDir = path.resolve(options.assetsDir);
  const buildSiteDir = path.resolve(options.buildSiteDir);
  const outputAssetsDir = path.join(buildSiteDir, "library-assets");

  await fs.mkdir(buildSiteDir, { recursive: true });
  await fs.rm(outputAssetsDir, { recursive: true, force: true });
  await fs.cp(assetsDir, outputAssetsDir, { recursive: true, force: true });

  return outputAssetsDir;
}
