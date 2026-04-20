import { existsSync } from "node:fs";
import path from "node:path";

const publicRoot = path.resolve(process.cwd(), "apps", "site", "public");

function compact(value: string | undefined | null) {
  return String(value ?? "").trim();
}

export function isLocalSiteAssetPath(value: string | undefined | null) {
  const normalized = compact(value);
  return Boolean(normalized) && normalized.startsWith("/") && !/^https?:\/\//i.test(normalized);
}

export function isMissingLocalSiteAsset(value: string | undefined | null) {
  if (!isLocalSiteAssetPath(value)) {
    return false;
  }
  const relativePath = compact(value).replace(/^\/+/, "");
  if (!relativePath) {
    return false;
  }
  return !existsSync(path.resolve(publicRoot, relativePath));
}

export function hasUsableImageSource(
  images: Array<{
    src?: string;
  }> = [],
) {
  return images.some((image) => {
    const src = compact(image?.src);
    if (!src) {
      return false;
    }
    if (/^https?:\/\//i.test(src)) {
      return true;
    }
    return !isMissingLocalSiteAsset(src);
  });
}
