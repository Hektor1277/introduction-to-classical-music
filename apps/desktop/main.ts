import { app, BrowserWindow, dialog, ipcMain, shell } from "electron";
import type { OpenDialogOptions } from "electron";
import { spawn, type ChildProcess } from "node:child_process";
import { once } from "node:events";
import { appendFileSync, existsSync, mkdirSync } from "node:fs";
import { access, readFile, stat } from "node:fs/promises";
import net from "node:net";
import path from "node:path";
import { fileURLToPath } from "node:url";

type ManagedService = {
  child: ChildProcess;
  port: number;
  url: string;
};

type LibrarySummary = {
  mode: string;
  rootDir: string;
  buildSiteDir: string;
  manifest: {
    libraryName: string;
    schemaVersion: string;
  };
  lastBuiltAt: string;
  counts: {
    composers: number;
    people: number;
    works: number;
    recordings: number;
    total: number;
  };
};

type LocalSiteServer = {
  ensureStarted: (nextRootDir: string) => Promise<{ url: string; rootDir: string }>;
  stop: () => Promise<void>;
};

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const moduleLoadStartedAt = Date.now();
const desktopAppUserModelId = "com.classical.guide.desktop.note";
const shellRootDir = path.resolve(__dirname, "../../../../");
const runtimeRootDir = app.isPackaged ? shellRootDir.replace(`${path.sep}app.asar`, `${path.sep}app.asar.unpacked`) : shellRootDir;
const launcherHtmlPath = path.resolve(runtimeRootDir, "apps", "desktop", "launcher.html");
const desktopPreloadPath = path.resolve(shellRootDir, "output", "runtime", "apps", "desktop", "preload.js");
const desktopAssetDir = path.resolve(shellRootDir, "apps", "desktop", "assets");
const packagedPortableSiteDir = path.resolve(shellRootDir, "output", "site");
const portableReleaseMarkerPath = app.isPackaged ? path.join(process.resourcesPath, "portable-release.marker") : "";
const desktopIconPath = path.resolve(
  desktopAssetDir,
  process.platform === "win32" ? "app-icon.ico" : "app-icon.png",
);
const ownerServerScriptPath = path.resolve(runtimeRootDir, "output", "runtime", "apps", "owner", "server", "owner-app.js");
const retrievalPortableDir = path.resolve(runtimeRootDir, "tools", "recording-retrieval-service", "app", "dist", "portable");
const retrievalPythonPath = path.resolve(
  runtimeRootDir,
  "tools",
  "recording-retrieval-service",
  "app",
  ".venv",
  "Scripts",
  "python.exe",
);

let launcherWindow: BrowserWindow | null = null;
let ownerWindow: BrowserWindow | null = null;
let libraryWindow: BrowserWindow | null = null;
let retrievalWindow: BrowserWindow | null = null;
let ownerService: ManagedService | null = null;
let retrievalService: ManagedService | null = null;
let bootstrapPromise: Promise<LibrarySummary> | null = null;
let libraryManagerModulePromise: Promise<typeof import("../../packages/data-core/src/library-manager.js")> | null = null;
let siteBuildModulePromise: Promise<typeof import("../../packages/data-core/src/site-build-runner.js")> | null = null;
let localSiteServerPromise: Promise<LocalSiteServer> | null = null;
let siteSourceMtimePromise: Promise<number> | null = null;

if (process.platform === "win32") {
  app.setAppUserModelId(desktopAppUserModelId);
}

function resolvePreferredDefaultLibraryDir() {
  if (app.isPackaged) {
    return path.join(path.dirname(process.execPath), "library");
  }
  return path.join(shellRootDir, "output", "desktop-dev-library");
}

function isPortableRuntime() {
  return Boolean(process.env.PORTABLE_EXECUTABLE_DIR || process.env.PORTABLE_EXECUTABLE_FILE || (app.isPackaged && existsSync(portableReleaseMarkerPath)));
}

function primeRuntimeEnvironment() {
  process.env.ICM_REPO_ROOT = runtimeRootDir;
  process.env.ICM_APP_DATA_DIR = app.getPath("userData");
  process.env.ICM_RUNTIME_MODE = "bundle";
  process.env.ICM_DEFAULT_LIBRARY_DIR = resolvePreferredDefaultLibraryDir();
}

async function migrateLegacyInstalledLibraryIfNeeded() {
  if (!app.isPackaged || isPortableRuntime()) {
    return;
  }
  await access(resolvePreferredDefaultLibraryDir()).catch((error) => {
    writeDesktopLog("default installed library is missing", error);
  });
}

async function loadLibraryManagerModule() {
  if (!libraryManagerModulePromise) {
    libraryManagerModulePromise = import("../../packages/data-core/src/library-manager.js");
  }
  return libraryManagerModulePromise;
}

async function bootstrapActiveLibraryBundle(options: {
  defaultLibraryName?: string;
  seedFromLegacy?: boolean;
}) {
  const libraryManager = await loadLibraryManagerModule();
  return libraryManager.bootstrapActiveLibrary(options) as Promise<LibrarySummary>;
}

async function getActiveLibraryBundleSummary() {
  const libraryManager = await loadLibraryManagerModule();
  return libraryManager.getActiveLibrarySummary() as Promise<LibrarySummary>;
}

async function importLibraryBundleAt(sourceRoot: string) {
  const libraryManager = await loadLibraryManagerModule();
  return libraryManager.importLibraryBundle(sourceRoot) as Promise<LibrarySummary>;
}

async function exportActiveLibraryBundleTo(destinationDir: string) {
  const libraryManager = await loadLibraryManagerModule();
  return libraryManager.exportActiveLibraryBundle(destinationDir);
}

async function loadSiteBuildModule() {
  if (!siteBuildModulePromise) {
    siteBuildModulePromise = import("../../packages/data-core/src/site-build-runner.js");
  }
  return siteBuildModulePromise;
}

async function buildActiveLibrarySite(options: { includeLocalOnlyLinks?: boolean } = {}) {
  const siteBuildModule = await loadSiteBuildModule();
  return siteBuildModule.buildLibrarySite(options);
}

async function getSiteSourceReferenceMtime() {
  if (!siteSourceMtimePromise) {
    const referenceFiles = [
      path.join(runtimeRootDir, "apps", "site", "src", "components", "SiteHeader.astro"),
      path.join(runtimeRootDir, "apps", "site", "src", "pages", "about.astro"),
      path.join(runtimeRootDir, "apps", "site", "src", "styles", "global.css"),
      path.join(runtimeRootDir, "apps", "site", "src", "layouts", "BaseLayout.astro"),
      path.join(runtimeRootDir, "output", "runtime", "scripts", "build-library-site.js"),
    ];
    siteSourceMtimePromise = Promise.all(
      referenceFiles.map(async (filePath) => {
        try {
          return (await stat(filePath)).mtimeMs;
        } catch {
          return 0;
        }
      }),
    ).then((times) => Math.max(...times, 0));
  }
  return siteSourceMtimePromise;
}

async function shouldRebuildLibrarySite(summary: LibrarySummary) {
  const indexPath = path.join(summary.buildSiteDir, "index.html");
  const metadataPath = path.join(summary.buildSiteDir, ".icm-build-meta.json");
  let indexStat;
  try {
    indexStat = await stat(indexPath);
  } catch {
    return true;
  }

  try {
    const metadata = JSON.parse(await readFile(metadataPath, "utf8")) as { appVersion?: string };
    if (metadata.appVersion !== app.getVersion()) {
      return true;
    }
  } catch {
    return true;
  }

  const sourceReferenceMtime = await getSiteSourceReferenceMtime();
  return sourceReferenceMtime > indexStat.mtimeMs;
}

async function getLocalSiteServerInstance() {
  if (!localSiteServerPromise) {
    localSiteServerPromise = import("../../packages/data-core/src/local-site-server.js").then(({ createLocalSiteServer }) =>
      createLocalSiteServer({ preferredPort: 4331 }),
    );
  }
  return localSiteServerPromise;
}

async function ensureDesktopRuntimeReady() {
  if (!bootstrapPromise) {
    primeRuntimeEnvironment();
    await migrateLegacyInstalledLibraryIfNeeded();
    const startedAt = Date.now();
    bootstrapPromise = bootstrapActiveLibraryBundle({
      defaultLibraryName: "\u9ed8\u8ba4\u8d44\u6599\u5e93",
      seedFromLegacy: false,
    })
      .then((summary) => {
        writeDesktopLog(`desktop bootstrap ready in ${Date.now() - startedAt}ms root=${summary.rootDir}`);
        return summary;
      })
      .catch((error) => {
        bootstrapPromise = null;
        throw error;
      });
  }
  return bootstrapPromise;
}

function writeDesktopLog(message: string, error?: unknown) {
  try {
    const logDir = path.join(app.getPath("userData"), "logs");
    mkdirSync(logDir, { recursive: true });
    const details =
      error instanceof Error ? `${error.message}\n${error.stack || ""}` : error ? String(error) : "";
    appendFileSync(
      path.join(logDir, "desktop-runtime.log"),
      `[${new Date().toISOString()}] ${message}${details ? `\n${details}` : ""}\n`,
      "utf8",
    );
  } catch {
    // ignore logging failures
  }
}

function hasStartupFlag(flag: string) {
  return process.argv.includes(flag);
}

function getManagedProcessCwd() {
  return app.isPackaged ? runtimeRootDir : shellRootDir;
}

async function handleStartupOpenFlags() {
  writeDesktopLog(`startup argv: ${process.argv.join(" ")}`);
  if (hasStartupFlag("--open-library")) {
    writeDesktopLog("startup flag detected: --open-library");
    await openLocalLibrarySite();
  }
  if (hasStartupFlag("--open-owner")) {
    writeDesktopLog("startup flag detected: --open-owner");
    await openOwnerTool();
  }
  if (hasStartupFlag("--open-retrieval")) {
    writeDesktopLog("startup flag detected: --open-retrieval");
    await openRetrievalTool();
  }
}

function getSharedRuntimeEnv(extraEnv: NodeJS.ProcessEnv = {}) {
  primeRuntimeEnvironment();
  const sharedEnv: NodeJS.ProcessEnv = {
    ...process.env,
    ICM_REPO_ROOT: runtimeRootDir,
    ICM_APP_DATA_DIR: app.getPath("userData"),
    ICM_RUNTIME_MODE: "bundle",
    ICM_DEFAULT_LIBRARY_DIR: resolvePreferredDefaultLibraryDir(),
    ...extraEnv,
  };
  if (process.versions.electron) {
    sharedEnv.ELECTRON_RUN_AS_NODE = "1";
  }
  return sharedEnv;
}

async function findAvailablePort(preferredPort: number) {
  let port = preferredPort;
  while (true) {
    const available = await new Promise<boolean>((resolve) => {
      const tester = net.createServer();
      tester.once("error", () => resolve(false));
      tester.once("listening", () => tester.close(() => resolve(true)));
      tester.listen(port, "127.0.0.1");
    });
    if (available) {
      return port;
    }
    port += 1;
  }
}

async function waitForUrl(url: string, timeoutMs = 60000) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    try {
      const response = await fetch(url, { redirect: "manual" });
      if (response.status < 500) {
        return;
      }
    } catch {
      // keep waiting
    }
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  throw new Error(`Timed out waiting for ${url}`);
}

async function stopManagedService(service: ManagedService | null) {
  if (!service?.child || service.child.killed) {
    return;
  }
  if (process.platform === "win32") {
    const killer = spawn("taskkill", ["/PID", String(service.child.pid), "/T", "/F"], {
      windowsHide: true,
      stdio: "ignore",
    });
    await once(killer, "exit");
    return;
  }
  service.child.kill("SIGTERM");
}

function getAnyDesktopWindow() {
  return BrowserWindow.getFocusedWindow() ?? ownerWindow ?? launcherWindow ?? libraryWindow ?? retrievalWindow ?? undefined;
}

function createWindow(options: Electron.BrowserWindowConstructorOptions) {
  return new BrowserWindow({
    autoHideMenuBar: true,
    backgroundColor: "#efe3cf",
    icon: desktopIconPath,
    webPreferences: {
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: false,
      webSecurity: true,
      preload: desktopPreloadPath,
    },
    ...options,
  });
}

async function pickDirectory(title: string) {
  const dialogOptions: OpenDialogOptions = {
    title,
    properties: ["openDirectory"],
  };
  const parentWindow = getAnyDesktopWindow();
  const result = parentWindow ? await dialog.showOpenDialog(parentWindow, dialogOptions) : await dialog.showOpenDialog(dialogOptions);
  return { canceled: result.canceled, path: result.filePaths[0] || "" };
}

async function pickFile(title: string) {
  const dialogOptions: OpenDialogOptions = {
    title,
    properties: ["openFile"],
  };
  const parentWindow = getAnyDesktopWindow();
  const result = parentWindow ? await dialog.showOpenDialog(parentWindow, dialogOptions) : await dialog.showOpenDialog(dialogOptions);
  return { canceled: result.canceled, path: result.filePaths[0] || "" };
}

async function ensureLibraryWindow() {
  if (libraryWindow && !libraryWindow.isDestroyed()) {
    libraryWindow.focus();
    return libraryWindow;
  }
  libraryWindow = createWindow({
    width: 1400,
    height: 960,
  });
  libraryWindow.on("closed", () => {
    libraryWindow = null;
  });
  return libraryWindow;
}

async function openLocalLibrarySite() {
  writeDesktopLog("openLocalLibrarySite: begin");
  const startedAt = Date.now();
  await ensureDesktopRuntimeReady();
  let summary = await getActiveLibraryBundleSummary();
  if (await shouldRebuildLibrarySite(summary)) {
    writeDesktopLog("openLocalLibrarySite: build missing or stale, triggering Astro build");
    await buildActiveLibrarySite();
    summary = await getActiveLibraryBundleSummary();
  }
  const localSiteServer = await getLocalSiteServerInstance();
  const site = await localSiteServer.ensureStarted(summary.buildSiteDir);
  writeDesktopLog(`openLocalLibrarySite: ready at ${site.url} in ${Date.now() - startedAt}ms`);
  const window = await ensureLibraryWindow();
  await window.loadURL(site.url);
  return site.url;
}

async function openPortableLibrarySite() {
  writeDesktopLog("openPortableLibrarySite: begin");
  const startedAt = Date.now();
  const indexPath = path.join(packagedPortableSiteDir, "index.html");
  await access(indexPath);
  const localSiteServer = await getLocalSiteServerInstance();
  const site = await localSiteServer.ensureStarted(packagedPortableSiteDir);
  writeDesktopLog(`openPortableLibrarySite: ready at ${site.url} in ${Date.now() - startedAt}ms`);
  const window = await ensureLibraryWindow();
  await window.loadURL(site.url);
  return site.url;
}

async function ensureOwnerService() {
  writeDesktopLog("ensureOwnerService: begin");
  await ensureDesktopRuntimeReady();
  if (ownerService) {
    try {
      await waitForUrl(ownerService.url, 1200);
      writeDesktopLog(`ensureOwnerService: reuse ${ownerService.url}`);
      return ownerService;
    } catch {
      await stopManagedService(ownerService);
      ownerService = null;
    }
  }

  const port = await findAvailablePort(4322);
  const summary = await getActiveLibraryBundleSummary();
  const child = spawn(process.execPath, [ownerServerScriptPath], {
    cwd: getManagedProcessCwd(),
    env: getSharedRuntimeEnv({
      OWNER_PORT: String(port),
      LIBRARY_SITE_PORT: "4331",
      ICM_ACTIVE_LIBRARY_DIR: summary.rootDir,
    }),
    stdio: "ignore",
    windowsHide: true,
  });
  child.unref();
  const url = `http://127.0.0.1:${port}`;
  await waitForUrl(url);
  ownerService = { child, port, url };
  writeDesktopLog(`ensureOwnerService: ready at ${url}`);
  return ownerService;
}

async function openOwnerTool() {
  const service = await ensureOwnerService();
  if (ownerWindow && !ownerWindow.isDestroyed()) {
    ownerWindow.focus();
    await ownerWindow.loadURL(service.url);
    return service.url;
  }
  ownerWindow = createWindow({
    width: 1600,
    height: 980,
  });
  ownerWindow.on("closed", () => {
    ownerWindow = null;
  });
  await ownerWindow.loadURL(service.url);
  return service.url;
}

async function ensureRetrievalService() {
  writeDesktopLog("ensureRetrievalService: begin");
  await ensureDesktopRuntimeReady();
  if (retrievalService) {
    try {
      await waitForUrl(`${retrievalService.url}/health`, 1200);
      writeDesktopLog(`ensureRetrievalService: reuse ${retrievalService.url}`);
      return retrievalService;
    } catch {
      await stopManagedService(retrievalService);
      retrievalService = null;
    }
  }

  const port = await findAvailablePort(4780);
  const retrievalExecutablePath = app.isPackaged
    ? path.join(process.resourcesPath, "recording-retrieval-service", "recording-retrieval-service.exe")
    : path.join(retrievalPortableDir, "recording-retrieval-service.exe");
  let child: ChildProcess;
  try {
    await access(retrievalExecutablePath);
    child = spawn(retrievalExecutablePath, ["--mode", "service", "--host", "127.0.0.1", "--port", String(port)], {
      cwd: path.dirname(retrievalExecutablePath),
      env: { ...process.env },
      stdio: "ignore",
      windowsHide: true,
    });
  } catch {
    await access(retrievalPythonPath);
    child = spawn(retrievalPythonPath, ["-m", "app.main", "--mode", "service", "--host", "127.0.0.1", "--port", String(port)], {
      cwd: path.dirname(retrievalPythonPath),
      env: { ...process.env },
      stdio: "ignore",
      windowsHide: true,
    });
  }
  child.unref();
  const url = `http://127.0.0.1:${port}`;
  await waitForUrl(`${url}/health`);
  retrievalService = { child, port, url };
  writeDesktopLog(`ensureRetrievalService: ready at ${url}`);
  return retrievalService;
}

async function openRetrievalTool() {
  const service = await ensureRetrievalService();
  if (retrievalWindow && !retrievalWindow.isDestroyed()) {
    retrievalWindow.focus();
    await retrievalWindow.loadURL(service.url);
    return service.url;
  }
  retrievalWindow = createWindow({
    width: 1460,
    height: 940,
  });
  retrievalWindow.on("closed", () => {
    retrievalWindow = null;
  });
  await retrievalWindow.loadURL(service.url);
  return service.url;
}

async function createLauncherWindow() {
  launcherWindow = createWindow({
    width: 620,
    height: 760,
    minWidth: 620,
    maxWidth: 620,
    minHeight: 760,
    maxHeight: 760,
    resizable: false,
    maximizable: false,
    fullscreenable: false,
    frame: false,
    titleBarStyle: "hidden",
  });
  await launcherWindow.loadFile(launcherHtmlPath);
  launcherWindow.on("closed", () => {
    launcherWindow = null;
  });
}

ipcMain.handle("launcher:get-library-status", async () => {
  await ensureDesktopRuntimeReady();
  return getActiveLibraryBundleSummary();
});

ipcMain.handle("launcher:open-library", async () => {
  const url = await openLocalLibrarySite();
  return { url };
});

ipcMain.handle("launcher:open-owner", async () => {
  const url = await openOwnerTool();
  return { url };
});

ipcMain.handle("launcher:open-retrieval", async () => {
  const url = await openRetrievalTool();
  return { url };
});

ipcMain.handle("launcher:import-library", async () => {
  await ensureDesktopRuntimeReady();
  const picked = await pickDirectory("\u9009\u62e9\u8981\u5bfc\u5165\u7684\u8d44\u6599\u5e93\u76ee\u5f55");
  if (picked.canceled || !picked.path) {
    return { cancelled: true };
  }
  const summary = await importLibraryBundleAt(picked.path);
  return { cancelled: false, ...summary };
});

ipcMain.handle("launcher:export-library", async () => {
  await ensureDesktopRuntimeReady();
  const picked = await pickDirectory("\u9009\u62e9\u5bfc\u51fa\u76ee\u5f55");
  if (picked.canceled || !picked.path) {
    return { cancelled: true };
  }
  const result = await exportActiveLibraryBundleTo(picked.path);
  return { cancelled: false, ...result };
});

ipcMain.handle("launcher:open-library-folder", async () => {
  await ensureDesktopRuntimeReady();
  const summary = await getActiveLibraryBundleSummary();
  const error = await shell.openPath(summary.rootDir);
  if (error) {
    throw new Error(error);
  }
  return { rootDir: summary.rootDir };
});

ipcMain.handle("desktop:open-external", async (_event, target: string) => {
  await shell.openExternal(target);
});

ipcMain.handle("desktop:pick-library-folder", async () => {
  return pickDirectory("\u9009\u62e9\u8d44\u6599\u5e93\u76ee\u5f55");
});

ipcMain.handle("desktop:pick-local-resource-file", async () => {
  return pickFile("\u9009\u62e9\u672c\u5730\u8d44\u6e90\u6587\u4ef6");
});

ipcMain.handle("launcher:window-control", (event, action: string) => {
  const targetWindow = BrowserWindow.fromWebContents(event.sender);
  if (!targetWindow) {
    return { ok: false };
  }
  if (action === "minimize") {
    targetWindow.minimize();
    return { ok: true };
  }
  if (action === "maximize") {
    if (targetWindow.isMaximized()) {
      targetWindow.unmaximize();
    } else if (targetWindow.isMaximizable()) {
      targetWindow.maximize();
    }
    return { ok: true, maximized: targetWindow.isMaximized() };
  }
  if (action === "close") {
    targetWindow.close();
    return { ok: true };
  }
  return { ok: false };
});

app.whenReady().then(async () => {
  app.setAppUserModelId(desktopAppUserModelId);
  primeRuntimeEnvironment();
  writeDesktopLog(`app.whenReady: shellRoot=${shellRootDir} runtimeRoot=${runtimeRootDir}`);
  writeDesktopLog(`module pre-window setup in ${Date.now() - moduleLoadStartedAt}ms`);
  if (isPortableRuntime()) {
    writeDesktopLog("app.whenReady: portable runtime detected");
    await openPortableLibrarySite();
    writeDesktopLog("app.whenReady: portable startup complete");
    return;
  }
  await createLauncherWindow();
  writeDesktopLog("app.whenReady: launcher created");
  setTimeout(() => {
    void ensureDesktopRuntimeReady().catch((error) => {
      writeDesktopLog("desktop bootstrap failed", error);
    });
  }, 180);
  await handleStartupOpenFlags();
  writeDesktopLog("app.whenReady: startup complete");
});

app.on("window-all-closed", () => {
  if (process.platform !== "darwin") {
    app.quit();
  }
});

app.on("activate", async () => {
  if (BrowserWindow.getAllWindows().length === 0) {
    if (isPortableRuntime()) {
      await openPortableLibrarySite();
      return;
    }
    await createLauncherWindow();
  }
});

app.on("before-quit", async () => {
  const localSiteServer = localSiteServerPromise ? await localSiteServerPromise : null;
  await Promise.all([
    stopManagedService(ownerService),
    stopManagedService(retrievalService),
    localSiteServer?.stop() || Promise.resolve(),
  ]);
});
