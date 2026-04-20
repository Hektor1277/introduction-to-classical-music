import path from "node:path";
import { fileURLToPath } from "node:url";

import { defineConfig } from "astro/config";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const siteRootDir = __dirname;

export default defineConfig({
  site: "https://classical-guide.local",
  output: "static",
  outDir: process.env.ICM_SITE_OUT_DIR || "../../output/site",
  vite: {
    resolve: {
      alias: {
        "@": path.resolve(siteRootDir, "src"),
      },
    },
  },
});
