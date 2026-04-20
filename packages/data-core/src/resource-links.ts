import type { ResourceLink } from "../../shared/src/schema.js";

export type ResourceLinkAuditIssue = {
  code: "invalid-url" | "platform-mismatch" | "title-looks-like-url";
  message: string;
  link: ResourceLink;
};

const platformLabels: Record<string, string> = {
  bilibili: "bilibili",
  youtube: "YouTube",
  netease: "网易云音乐",
  "apple-music": "Apple Music",
  "amazon-music": "Amazon Music",
  local: "本地文件",
  other: "其他资源",
};

export function detectPlatformFromUrl(url: string): ResourceLink["platform"] {
  if (/bilibili\.com/i.test(url)) {
    return "bilibili";
  }
  if (/youtube\.com|youtu\.be/i.test(url)) {
    return "youtube";
  }
  if (/music\.163\.com/i.test(url)) {
    return "netease";
  }
  if (/music\.apple\.com/i.test(url)) {
    return "apple-music";
  }
  if (/amazon\.[^/]+\/music/i.test(url)) {
    return "amazon-music";
  }
  return "other";
}

export function getPlatformBadgeLabel(platform: ResourceLink["platform"]) {
  return platformLabels[platform] ?? platformLabels.other;
}

export function normalizeResourceLink(link: ResourceLink): ResourceLink {
  const linkType = link.linkType === "local" ? "local" : "external";
  const normalizedUrl = (link.url ?? "").trim();
  const normalizedLocalPath = (link.localPath ?? "").trim();
  const detectedPlatform = /^https?:\/\//i.test(normalizedUrl) ? detectPlatformFromUrl(normalizedUrl) : link.platform;

  return {
    platform: link.platform || detectedPlatform || "other",
    url: normalizedUrl,
    localPath: normalizedLocalPath,
    title: (link.title ?? "").trim(),
    linkType,
    visibility: linkType === "local" ? "local-only" : link.visibility === "local-only" ? "local-only" : "public",
  };
}

export function auditResourceLinks(links: ResourceLink[]): ResourceLinkAuditIssue[] {
  const issues: ResourceLinkAuditIssue[] = [];

  for (const link of links.map(normalizeResourceLink)) {
    if (link.linkType === "local") {
      continue;
    }

    if (!/^https?:\/\//i.test(link.url)) {
      issues.push({
        code: "invalid-url",
        message: `Link is not a valid http/https URL: ${link.url}`,
        link,
      });
      continue;
    }

    const detectedPlatform = detectPlatformFromUrl(link.url);
    if (link.platform !== "other" && detectedPlatform !== "other" && detectedPlatform !== link.platform) {
      issues.push({
        code: "platform-mismatch",
        message: `Platform ${link.platform} does not match URL ${link.url}`,
        link,
      });
    }

    if (/^https?:\/\//i.test(link.title ?? "")) {
      issues.push({
        code: "title-looks-like-url",
        message: `Title appears to contain a URL: ${link.title}`,
        link,
      });
    }
  }

  return issues;
}

export function isRenderableResourceLink(link: ResourceLink, options: { includeLocalOnly?: boolean } = {}) {
  const normalized = normalizeResourceLink(link);
  if (normalized.linkType === "local") {
    return options.includeLocalOnly !== false && Boolean(normalized.localPath);
  }
  if (normalized.visibility === "local-only" && options.includeLocalOnly === false) {
    return false;
  }
  return Boolean(normalized.url);
}

export function filterRenderableResourceLinks(links: ResourceLink[], options: { includeLocalOnly?: boolean } = {}) {
  return links.map(normalizeResourceLink).filter((link) => isRenderableResourceLink(link, options));
}

export function sanitizeResourceLinksForSiteOutput(links: ResourceLink[], options: { includeLocalOnly?: boolean } = {}) {
  return links
    .map(normalizeResourceLink)
    .filter((link) => {
      if (options.includeLocalOnly === false && (link.linkType === "local" || link.visibility === "local-only")) {
        return false;
      }
      return Boolean(link.linkType === "local" ? link.localPath : link.url);
    });
}

export function getResourceLinkPresentation(link: ResourceLink) {
  const normalized = normalizeResourceLink(link);
  const href =
    normalized.linkType === "local"
      ? `/__local-resource?path=${encodeURIComponent(normalized.localPath || "")}`
      : normalized.url;

  return {
    href,
    label: getPlatformBadgeLabel(normalized.platform),
    metadataTitle: normalized.title || "",
    platform: normalized.platform,
    linkType: normalized.linkType,
    visibility: normalized.visibility,
    localPath: normalized.localPath || "",
  };
}
