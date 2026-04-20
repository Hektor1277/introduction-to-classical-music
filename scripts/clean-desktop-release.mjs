import { readFile, rm } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "..");
const releasesRoot = path.join(repoRoot, "output", "releases");
const packageJson = JSON.parse(await readFile(path.join(repoRoot, "package.json"), "utf8"));
const version = packageJson.version;

const targets = [
  path.join(releasesRoot, "win-unpacked"),
  path.join(releasesRoot, `不全书 Setup ${version}.exe`),
  path.join(releasesRoot, `不全书 Setup ${version}.exe.blockmap`),
  path.join(releasesRoot, `不全书 ${version}.exe`),
];

await Promise.all(targets.map((targetPath) => rm(targetPath, { recursive: true, force: true })));

console.log(`Cleaned desktop release output for version ${version}`);
