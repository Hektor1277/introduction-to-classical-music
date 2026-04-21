import { readFile, rm } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "..");
const releasesRoot = path.join(repoRoot, "output", "releases");
const distRoot = path.join(repoRoot, "dist");
const packageJson = JSON.parse(await readFile(path.join(repoRoot, "package.json"), "utf8"));
const version = packageJson.version;

const targets = [
  path.join(releasesRoot, "win-unpacked"),
  path.join(releasesRoot, "builder-debug.yml"),
  path.join(releasesRoot, `BuQuanShu-Portable-${version}.exe`),
  path.join(releasesRoot, `BuQuanShu-Portable-${version}.zip`),
  path.join(distRoot, "win-unpacked"),
  path.join(distRoot, "builder-debug.yml"),
  path.join(distRoot, `BuQuanShu-Portable-${version}.exe`),
  path.join(distRoot, `BuQuanShu-Portable-${version}.zip`),
];

await Promise.all(targets.map((targetPath) => rm(targetPath, { recursive: true, force: true })));

console.log(`Cleaned portable release output for version ${version}`);
