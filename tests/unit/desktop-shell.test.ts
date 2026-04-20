import { describe, expect, it } from "vitest";
import { promises as fs } from "node:fs";
import path from "node:path";

describe("desktop shell scaffolding", () => {
  it("declares the Electron entrypoint and packaging scripts", async () => {
    const packageJson = JSON.parse(await fs.readFile(path.resolve("package.json"), "utf8"));

    expect(packageJson.main).toBe("output/runtime/apps/desktop/main.js");
    expect(packageJson.scripts["desktop:dev"]).toContain("electron");
    expect(packageJson.scripts["desktop:dist"]).toContain("electron-builder");
    expect(packageJson.scripts["desktop:dist"]).toContain("npm run build");
    expect(packageJson.scripts["desktop:dist"]).toContain("retrieval:portable:build");
    expect(packageJson.scripts["desktop:dist"]).toContain("clean-desktop-release.mjs");
    expect(packageJson.scripts["desktop:dist"]).toContain("patch-release-icons.mjs");
    expect(packageJson.scripts["retrieval:portable:build"]).toContain("build-portable.ps1");
    expect(packageJson.build?.extraResources?.[0]?.from).toBe("tools/recording-retrieval-service/app/dist/portable");
    expect(packageJson.build?.files).toContain("data/**/*");
    expect(packageJson.build?.files).toContain("output/site/**/*");
    expect(packageJson.build?.win?.signAndEditExecutable).toBe(false);
    expect(JSON.stringify(packageJson.build?.win?.target || [])).toContain("nsis");
    expect(JSON.stringify(packageJson.build?.win?.target || [])).not.toContain("portable");
  });

  it("includes launcher actions, library management hooks, and IPC bridges", async () => {
    const launcherHtml = await fs.readFile(path.resolve("apps/desktop/launcher.html"), "utf8");
    const preload = await fs.readFile(path.resolve("apps/desktop/preload.ts"), "utf8");
    const mainProcess = await fs.readFile(path.resolve("apps/desktop/main.ts"), "utf8");

    expect(launcherHtml).toContain('data-launch-action="library"');
    expect(launcherHtml).toContain('data-launch-action="owner"');
    expect(launcherHtml).toContain('data-launch-action="retrieval"');
    expect(launcherHtml).toContain('data-library-action="import"');
    expect(launcherHtml).toContain('data-library-action="export"');
    expect(launcherHtml).toContain('data-library-action="open-folder"');
    expect(launcherHtml).toContain('data-view-action="details"');
    expect(launcherHtml).toContain('data-view-action="back"');
    expect(launcherHtml).toContain('data-window-action="minimize"');
    expect(launcherHtml).toContain('data-window-action="close"');
    expect(launcherHtml).toContain("./assets/G-clef.png");
    expect(launcherHtml).toContain("./assets/C-clef.png");
    expect(launcherHtml).toContain("./assets/F-clef.png");

    expect(preload).toContain("openLibrary");
    expect(preload).toContain("openOwner");
    expect(preload).toContain("openRetrieval");
    expect(preload).toContain("importLibrary");
    expect(preload).toContain("exportLibrary");
    expect(preload).toContain("openLibraryFolder");
    expect(preload).toContain("pickLibraryFolder");
    expect(preload).toContain("pickLocalResourceFile");
    expect(preload).toContain("windowControl");

    expect(mainProcess).toContain('ipcMain.handle("launcher:open-library"');
    expect(mainProcess).toContain('ipcMain.handle("launcher:open-owner"');
    expect(mainProcess).toContain('ipcMain.handle("launcher:open-retrieval"');
    expect(mainProcess).toContain('ipcMain.handle("launcher:import-library"');
    expect(mainProcess).toContain('ipcMain.handle("launcher:export-library"');
    expect(mainProcess).toContain('ipcMain.handle("launcher:open-library-folder"');
    expect(mainProcess).toContain('ipcMain.handle("launcher:window-control"');
    expect(mainProcess).toContain('ipcMain.handle("desktop:pick-library-folder"');
    expect(mainProcess).toContain('ipcMain.handle("desktop:pick-local-resource-file"');
    expect(mainProcess).toContain("process.resourcesPath");
    expect(mainProcess).toContain("recording-retrieval-service.exe");
    expect(mainProcess).toContain("--open-library");
    expect(mainProcess).toContain("--open-owner");
    expect(mainProcess).toContain("--open-retrieval");
    expect(mainProcess).toContain("frame: false");
    expect(mainProcess).toContain("resizable: false");
    expect(mainProcess).toContain("nodeIntegration: false");
    expect(mainProcess).toContain("webSecurity: true");
    expect(mainProcess).toContain("ICM_DEFAULT_LIBRARY_DIR");
    expect(mainProcess).toContain('app.getPath("userData")');
    expect(mainProcess).toContain('path.dirname(process.execPath)');
    expect(mainProcess).toContain('"library"');
    expect(mainProcess).toContain("migrateLegacyInstalledLibraryIfNeeded");
    expect(mainProcess).toContain("seedFromLegacy: false");
    expect(mainProcess).toContain("PORTABLE_EXECUTABLE_DIR");
    expect(mainProcess).toContain("openPortableLibrarySite");
    expect(mainProcess).toContain(".icm-build-meta.json");
    expect(mainProcess).toContain("module pre-window setup");
    expect(mainProcess).toContain('import("../../packages/data-core/src/library-manager.js")');

    const afterPack = await fs.readFile(path.resolve("scripts/electron-after-pack.mjs"), "utf8");
    const cleanRelease = await fs.readFile(path.resolve("scripts/clean-desktop-release.mjs"), "utf8");
    const patchReleaseIcons = await fs.readFile(path.resolve("scripts/patch-release-icons.mjs"), "utf8");
    expect(cleanRelease).toContain("Cleaned desktop release output");
    expect(afterPack).toContain('import * as ResEdit from "resedit"');
    expect(afterPack).toContain("replaceIconsForResource");
    expect(afterPack).toContain("IconGroupEntry.fromEntries");
    expect(patchReleaseIcons).toContain('import * as ResEdit from "resedit"');
    expect(patchReleaseIcons).toContain("Patched unpacked release icons");
    expect(patchReleaseIcons).toContain("IconGroupEntry.fromEntries");
    expect(patchReleaseIcons).toContain("resolveCandidateExecutables");
    expect(patchReleaseIcons).not.toContain("Setup");
  });
});
