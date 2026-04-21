import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "..");
const outputDir = path.join(repoRoot, "output", "releases");
const tempConfigPath = path.join(outputDir, "electron-builder-portable.json");
const electronBuilderBin = path.join(
  repoRoot,
  "node_modules",
  ".bin",
  process.platform === "win32" ? "electron-builder.cmd" : "electron-builder",
);

const portableConfig = {
  extends: null,
  appId: "com.classical.guide.desktop.note",
  productName: "不全书",
  executableName: "不全书",
  directories: {
    output: outputDir,
  },
  artifactName: "BuQuanShu-Portable-${version}.${ext}",
  compression: "maximum",
  files: [
    "package.json",
    "apps/desktop/assets/**/*",
    "output/runtime/apps/desktop/**/*",
    "output/runtime/packages/**/*",
    "output/runtime/scripts/**/*",
    "output/site/**/*",
    "!**/*.map",
    "!**/tests/**/*",
    "!**/docs/**/*",
    "!**/.venv/**/*",
    "!**/.pytest_cache/**/*",
    "!**/tools/**/*",
    "!**/tmp/**/*",
    "!**/logs/**/*",
  ],
  extraResources: [
    {
      from: "scripts/portable-release.marker",
      to: "portable-release.marker",
    },
  ],
  asar: true,
  asarUnpack: [
    "node_modules/**/*",
    "output/runtime/apps/desktop/**/*",
    "output/runtime/packages/**/*",
    "output/runtime/scripts/**/*",
  ],
  afterPack: "scripts/electron-after-pack-portable.mjs",
  win: {
    icon: "apps/desktop/assets/app-icon.ico",
    signAndEditExecutable: false,
    target: ["portable", "zip"],
  },
};

await mkdir(outputDir, { recursive: true });
await writeFile(tempConfigPath, `${JSON.stringify(portableConfig, null, 2)}\n`, "utf8");

const args = ["--config", tempConfigPath, "--win", "portable", "zip", "--publish", "never"];
const child = spawn(electronBuilderBin, args, {
  cwd: repoRoot,
  stdio: "inherit",
  windowsHide: true,
  shell: process.platform === "win32",
});

const exitCode = await new Promise((resolve, reject) => {
  child.once("error", reject);
  child.once("exit", (code) => resolve(code ?? 1));
});

await rm(tempConfigPath, { force: true });

if (exitCode !== 0) {
  process.exit(exitCode);
}

const builderDebugDistPath = path.join(repoRoot, "dist", "builder-debug.yml");
const builderDebugOutputPath = path.join(outputDir, "builder-debug.yml");
try {
  const builderDebugContent = await readFile(builderDebugDistPath);
  await writeFile(builderDebugOutputPath, builderDebugContent);
} catch {
  // Ignore optional debug output when electron-builder does not emit it.
}

console.log("Built portable release artifacts");
