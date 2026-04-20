import { defineConfig } from "vitest/config";
import path from "node:path";

export default defineConfig({
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "apps/site/src"),
    },
  },
  test: {
    environment: "node",
    include: ["tests/**/*.test.ts"],
    pool: "forks",
    fileParallelism: process.platform !== "win32",
    maxWorkers: process.platform === "win32" ? 1 : undefined,
    coverage: {
      reporter: ["text", "html"],
    },
  },
});
