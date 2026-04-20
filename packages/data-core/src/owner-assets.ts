import fs from "node:fs";
import path from "node:path";

export function resolveLibraryAssetPath(assetsRootInput: string, requestPath: string) {
  const assetsRoot = path.resolve(assetsRootInput);
  const normalizedRequestPath = decodeURIComponent(String(requestPath || ""))
    .replace(/^\/?library-assets\/?/, "")
    .replaceAll("/", path.sep);
  const resolvedPath = path.resolve(assetsRoot, normalizedRequestPath);

  if (!(resolvedPath === assetsRoot || resolvedPath.startsWith(`${assetsRoot}${path.sep}`))) {
    return null;
  }

  if (!fs.existsSync(resolvedPath)) {
    return null;
  }

  if (!fs.statSync(resolvedPath).isFile()) {
    return null;
  }

  return resolvedPath;
}
