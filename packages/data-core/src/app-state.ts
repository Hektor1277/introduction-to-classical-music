import { promises as fs } from "node:fs";

import { getRuntimePaths } from "./app-paths.js";

export type AppState = {
  activeLibraryPath: string;
  recentLibraries: string[];
};

const defaultAppState: AppState = {
  activeLibraryPath: "",
  recentLibraries: [],
};

async function ensureAppStateDir() {
  const runtimePaths = getRuntimePaths();
  await fs.mkdir(runtimePaths.appData.rootDir, { recursive: true });
}

export async function loadAppState(): Promise<AppState> {
  await ensureAppStateDir();
  const statePath = getRuntimePaths().appData.statePath;
  try {
    const content = await fs.readFile(statePath, "utf8");
    const parsed = JSON.parse(content) as Partial<AppState>;
    return {
      activeLibraryPath: String(parsed.activeLibraryPath || "").trim(),
      recentLibraries: Array.isArray(parsed.recentLibraries)
        ? parsed.recentLibraries.map((item) => String(item || "").trim()).filter(Boolean)
        : [],
    };
  } catch {
    return { ...defaultAppState };
  }
}

export async function saveAppState(state: Partial<AppState>) {
  await ensureAppStateDir();
  const current = await loadAppState();
  const nextState: AppState = {
    activeLibraryPath: String(state.activeLibraryPath ?? current.activeLibraryPath ?? "").trim(),
    recentLibraries: Array.isArray(state.recentLibraries)
      ? state.recentLibraries.map((item) => String(item || "").trim()).filter(Boolean)
      : current.recentLibraries,
  };
  await fs.writeFile(getRuntimePaths().appData.statePath, `${JSON.stringify(nextState, null, 2)}\n`, "utf8");
  return nextState;
}
