import process from "node:process";

import { ensureSharedAssetLink } from "./lib/shared-assets.js";

const rootDir = process.cwd();
const relativeAssets = ["library-assets/legacy"];

const results = [];
for (const relativeAssetPath of relativeAssets) {
  results.push(await ensureSharedAssetLink(rootDir, relativeAssetPath));
}

process.stdout.write(`${JSON.stringify(results, null, 2)}\n`);
