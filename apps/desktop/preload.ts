import { contextBridge, ipcRenderer } from "electron";

contextBridge.exposeInMainWorld("desktopLauncher", {
  getLibraryStatus: () => ipcRenderer.invoke("launcher:get-library-status"),
  openLibrary: () => ipcRenderer.invoke("launcher:open-library"),
  openOwner: () => ipcRenderer.invoke("launcher:open-owner"),
  openRetrieval: () => ipcRenderer.invoke("launcher:open-retrieval"),
  importLibrary: () => ipcRenderer.invoke("launcher:import-library"),
  exportLibrary: () => ipcRenderer.invoke("launcher:export-library"),
  openLibraryFolder: () => ipcRenderer.invoke("launcher:open-library-folder"),
  openExternal: (target: string) => ipcRenderer.invoke("desktop:open-external", target),
  pickLibraryFolder: () => ipcRenderer.invoke("desktop:pick-library-folder"),
  pickLocalResourceFile: () => ipcRenderer.invoke("desktop:pick-local-resource-file"),
  windowControl: (action: string) => ipcRenderer.invoke("launcher:window-control", action),
});
