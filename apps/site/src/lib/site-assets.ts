import fs from "node:fs";
import path from "node:path";

import type { RecordingImage } from "@/lib/schema";
import { getRuntimePaths } from "../../../../packages/data-core/src/app-paths.js";

function normalizeLocalAssetPath(src: string) {
  return decodeURIComponent(src.split(/[?#]/, 1)[0] || "")
    .replace(/^\/+/, "")
    .replace(/\//g, path.sep);
}

function resolveLocalAssetAbsolutePath(src: string) {
  const normalizedAssetPath = normalizeLocalAssetPath(src);
  const runtimePaths = getRuntimePaths();
  const assetsRoot = runtimePaths.mode === "bundle"
    ? runtimePaths.library.assetsDir
    : path.join(runtimePaths.sitePublicDir, "library-assets");
  const relativeAssetPath = normalizedAssetPath.replace(/^library-assets[\\/]/, "");
  return path.resolve(assetsRoot, relativeAssetPath);
}

export function isLocalSiteAssetPath(src: string) {
  const trimmed = String(src || "").trim();
  return Boolean(trimmed) && trimmed.startsWith("/") && !trimmed.startsWith("//");
}

function defaultAssetExists(src: string) {
  if (!isLocalSiteAssetPath(src)) {
    return true;
  }

  return fs.existsSync(resolveLocalAssetAbsolutePath(src));
}

export function resolveSiteImageSrc(src: string, assetExists: (src: string) => boolean = defaultAssetExists) {
  const trimmed = String(src || "").trim();
  if (!trimmed) {
    return "";
  }
  if (!isLocalSiteAssetPath(trimmed)) {
    return trimmed;
  }
  return assetExists(trimmed) ? trimmed : "";
}

export function filterRenderableRecordingImages(
  images: RecordingImage[],
  assetExists: (src: string) => boolean = defaultAssetExists,
) {
  return (images || [])
    .map((image) => ({
      ...image,
      src: resolveSiteImageSrc(image.src, assetExists),
    }))
    .filter((image) => image.src);
}
