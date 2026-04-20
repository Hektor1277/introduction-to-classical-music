import { promises as fs } from "node:fs";
import path from "node:path";

import {
  rankImageCandidates,
  type AutomationImageCandidate,
  type AutomationLinkCandidate,
  type AutomationProposal,
  type AutomationProposalEvidence,
} from "./automation.js";
import { generateConciseChineseSummary, isLlmConfigured, type LlmConfig } from "./llm.js";
import { hasUsableImageSource } from "./site-asset-health.js";
import { auditResourceLinks, detectPlatformFromUrl } from "../../data-core/src/resource-links.js";
import type { LibraryData, Recording, ResourceLink } from "../../shared/src/schema.js";

type HighQualitySource = {
  url: string;
  hostname: string;
  weight: number;
};

type RecordingContext = {
  composerName: string;
  workTitle: string;
  leadNames: string[];
  orchestraNames: string[];
  year: string;
  rawQuery: string;
};

type UrlCandidate = {
  url: string;
  title: string;
  snippet: string;
  sourceLabel: string;
  sourceKind: "high-quality" | "search" | "streaming" | "existing";
  weight: number;
};

type MetadataCandidate = {
  sourceUrl: string;
  title: string;
  description: string;
  imageUrl: string;
  imageTitle: string;
  year: string;
  label: string;
  releaseDate: string;
  venue: string;
  weight: number;
};

const defaultHeaders = {
  "User-Agent": "Mozilla/5.0 (compatible; ClassicalGuideBot/1.0)",
};

function compact(value: string) {
  return String(value ?? "").trim();
}

function dedupeBy<T>(items: T[], keyBuilder: (item: T) => string) {
  const seen = new Set<string>();
  return items.filter((item) => {
    const key = keyBuilder(item);
    if (!key || seen.has(key)) {
      return false;
    }
    seen.add(key);
    return true;
  });
}

function stripHtml(value: string) {
  return String(value ?? "").replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
}

function buildSearchNameParts(...values: Array<string | undefined>) {
  return [...new Set(values.map((value) => compact(value || "")).filter(Boolean))];
}

function clip(value: string, max = 180) {
  const normalized = compact(value);
  return normalized.length > max ? `${normalized.slice(0, max - 1).trim()}…` : normalized;
}

function getRecordingContext(recording: Recording, library: LibraryData): RecordingContext {
  const work = library.works.find((item) => item.id === recording.workId);
  const composer = work ? library.composers.find((item) => item.id === work.composerId) : null;
  const leadNames = recording.credits
    .filter((credit) => credit.role === "conductor" || credit.role === "soloist" || credit.role === "singer" || credit.role === "instrumentalist")
    .flatMap((credit) => {
      const person = library.people.find((item) => item.id === credit.personId);
      return buildSearchNameParts(credit.displayName, person?.name, person?.nameLatin);
    })
    .filter(Boolean);
  const orchestraNames = recording.credits
    .filter((credit) => credit.role === "orchestra")
    .flatMap((credit) => {
      const person = library.people.find((item) => item.id === credit.personId);
      return buildSearchNameParts(credit.displayName, person?.name, person?.nameLatin);
    })
    .filter(Boolean);
  const rawQuery = [
    ...buildSearchNameParts(composer?.name, composer?.nameLatin),
    ...buildSearchNameParts(work?.title, work?.titleLatin, recording.title),
    ...leadNames.slice(0, 2),
    ...orchestraNames.slice(0, 1),
    recording.performanceDateText || "",
  ]
    .filter(Boolean)
    .join(" ");

  return {
    composerName: buildSearchNameParts(composer?.name, composer?.nameLatin).join(" "),
    workTitle: buildSearchNameParts(work?.title, work?.titleLatin, recording.title).join(" "),
    leadNames,
    orchestraNames,
    year: compact(recording.performanceDateText),
    rawQuery: compact(rawQuery),
  };
}

function normalizeMatchText(value: string) {
  return String(value || "")
    .toLowerCase()
    .normalize("NFKC")
    .replace(/[\s·:：;；,.，。'"`()[\]{}<>|/\\_-]+/g, " ")
    .trim();
}

function buildRecordingSignature(context: RecordingContext) {
  const workTokens = normalizeMatchText(context.workTitle).split(" ").filter(Boolean);
  const composerTokens = normalizeMatchText(context.composerName).split(" ").filter(Boolean);
  const leadTokens = context.leadNames.flatMap((item) => normalizeMatchText(item).split(" ").filter(Boolean));
  const orchestraTokens = context.orchestraNames.flatMap((item) => normalizeMatchText(item).split(" ").filter(Boolean));
  return {
    workTokens,
    composerTokens,
    leadTokens,
    orchestraTokens,
    year: extractYear(context.year || context.rawQuery),
  };
}

function scoreRecordingMatch(text: string, context: RecordingContext) {
  const haystack = normalizeMatchText(text);
  if (!haystack) {
    return 0;
  }
  const signature = buildRecordingSignature(context);
  let score = 0;
  if (signature.workTokens.some((token) => haystack.includes(token))) {
    score += 4;
  }
  if (signature.composerTokens.some((token) => haystack.includes(token))) {
    score += 2;
  }
  if (signature.leadTokens.some((token) => haystack.includes(token))) {
    score += 3;
  }
  if (signature.orchestraTokens.some((token) => haystack.includes(token))) {
    score += 2;
  }
  if (signature.year && haystack.includes(signature.year)) {
    score += 2;
  }
  if (/biography|born|composer|founded|首席指挥|作曲家生平|担任/.test(haystack) && !/album|recording|symphony|concert|live|交响曲|录音|演出/.test(haystack)) {
    score -= 5;
  }
  return score;
}

function isUsableAlbumTitle(value: string) {
  const normalized = compact(value);
  if (!normalized) {
    return false;
  }
  if (/^https?:\/\//i.test(normalized)) {
    return false;
  }
  if (/youtube|bilibili|apple music|spotify|music\.apple/i.test(normalized)) {
    return false;
  }
  return true;
}

function isUsableNotesSummary(value: string) {
  const normalized = normalizeMatchText(value);
  if (!normalized) {
    return false;
  }
  const biographyLike = /biography|born|died|composer|生平|出生|逝世|人物/.test(normalized);
  const recordingLike = /recording|album|live|concert|symphony|release|label|录音|专辑|演出|交响曲/.test(normalized);
  if (biographyLike && !recordingLike) {
    return false;
  }
  return true;
}

function buildSearchQueries(context: RecordingContext) {
  const queries = [
    context.rawQuery,
    [context.workTitle, ...context.leadNames, context.year].filter(Boolean).join(" "),
    [context.composerName, context.workTitle, ...context.leadNames].filter(Boolean).join(" "),
  ]
    .map(compact)
    .filter(Boolean);

  return [...new Set(queries)];
}

async function fetchText(url: string, fetchImpl: typeof fetch) {
  const response = await fetchImpl(url, { headers: defaultHeaders });
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}: ${url}`);
  }
  return response.text();
}

function extractMetaContent(html: string, key: string, attr: "property" | "name" = "property") {
  const patternA = new RegExp(`<meta[^>]+${attr}=["']${key}["'][^>]+content=["']([^"']+)["']`, "i");
  const patternB = new RegExp(`<meta[^>]+content=["']([^"']+)["'][^>]+${attr}=["']${key}["']`, "i");
  return patternA.exec(html)?.[1] || patternB.exec(html)?.[1] || "";
}

function normalizeSearchHref(href: string, baseUrl: string) {
  const value = compact(href);
  if (!value || value.startsWith("#") || value.startsWith("javascript:")) {
    return "";
  }
  try {
    return new URL(value, baseUrl).toString();
  } catch {
    return "";
  }
}

function extractGoogleResultLinks(html: string) {
  return [...html.matchAll(/href="\/url\?q=([^"&]+)[^"]*"/g)]
    .map((match) => decodeURIComponent(match[1] || ""))
    .filter((url) => /^https?:\/\//i.test(url));
}

function extractBaiduResultLinks(html: string) {
  return [...html.matchAll(/<a[^>]+href="(https?:\/\/[^"]+)"[^>]*>/g)]
    .map((match) => match[1] || "")
    .filter((url) => /^https?:\/\//i.test(url));
}

function extractYoutubeLinks(html: string) {
  return dedupeBy(
    [...html.matchAll(/"url":"(\/watch\?v=[^"]+)"/g)]
      .map((match) => match[1]?.replace(/\\u0026/g, "&") || "")
      .map((href) => normalizeSearchHref(href, "https://www.youtube.com"))
      .filter(Boolean),
    (item) => item,
  );
}

function extractBilibiliLinks(html: string) {
  return dedupeBy(
    [...html.matchAll(/https?:\/\/www\.bilibili\.com\/video\/BV[0-9A-Za-z]+/g)].map((match) => match[0]),
    (item) => item,
  );
}

function extractAppleMusicLinks(html: string) {
  return dedupeBy(
    [...html.matchAll(/https?:\/\/music\.apple\.com\/[^"' ]+/g)].map((match) => match[0]),
    (item) => item,
  );
}

async function loadHighQualityRecordingSources(
  filePath = path.join(process.cwd(), "materials", "references", "High Quality Sources.txt"),
) {
  try {
    const source = await fs.readFile(filePath, "utf8");
    return source
      .split(/\r?\n/)
      .map((line) => compact(line))
      .filter(Boolean)
      .map((url, index) => {
        const parsed = new URL(url);
        return {
          url,
          hostname: parsed.hostname.toLowerCase(),
          weight: 140 - index * 8,
        } satisfies HighQualitySource;
      });
  } catch {
    return [] satisfies HighQualitySource[];
  }
}

function extractYear(value: string) {
  return value.match(/(19\d{2}|20\d{2})/)?.[1] || "";
}

function extractReleaseDate(value: string) {
  return (
    value.match(/(20\d{2}-\d{2}-\d{2})/)?.[1] ||
    value.match(/(20\d{2}\/\d{2}\/\d{2})/)?.[1] ||
    value.match(/(19\d{2}|20\d{2})/)?.[1] ||
    ""
  );
}

function extractLabel(value: string) {
  return (
    value.match(/Label[:：]?\s*([^|,/]+)/i)?.[1] ||
    value.match(/厂牌[:：]?\s*([^|,/]+)/i)?.[1] ||
    value.match(/发行商[:：]?\s*([^|,/]+)/i)?.[1] ||
    ""
  ).trim();
}

function extractVenue(value: string) {
  return (
    value.match(/(?:Venue|Location|Live at)[:：]?\s*([^|,/]+)/i)?.[1] ||
    value.match(/(?:地点|现场)[:：]?\s*([^|,/]+)/i)?.[1] ||
    ""
  ).trim();
}

async function searchStreamingCandidates(queries: string[], fetchImpl: typeof fetch) {
  const candidates: UrlCandidate[] = [];

  for (const query of queries.slice(0, 2)) {
    try {
      const youtubeHtml = await fetchText(`https://www.youtube.com/results?search_query=${encodeURIComponent(query)}`, fetchImpl);
      extractYoutubeLinks(youtubeHtml)
        .slice(0, 3)
        .forEach((url, index) => {
          candidates.push({
            url,
            title: query,
            snippet: "",
            sourceLabel: "YouTube Search",
            sourceKind: "streaming",
            weight: 90 - index * 6,
          });
        });
    } catch {}

    try {
      const bilibiliHtml = await fetchText(`https://search.bilibili.com/all?keyword=${encodeURIComponent(query)}`, fetchImpl);
      extractBilibiliLinks(bilibiliHtml)
        .slice(0, 3)
        .forEach((url, index) => {
          candidates.push({
            url,
            title: query,
            snippet: "",
            sourceLabel: "Bilibili Search",
            sourceKind: "streaming",
            weight: 96 - index * 6,
          });
        });
    } catch {}

    try {
      const appleHtml = await fetchText(`https://music.apple.com/us/search?term=${encodeURIComponent(query)}`, fetchImpl);
      extractAppleMusicLinks(appleHtml)
        .slice(0, 2)
        .forEach((url, index) => {
          candidates.push({
            url,
            title: query,
            snippet: "",
            sourceLabel: "Apple Music Search",
            sourceKind: "streaming",
            weight: 82 - index * 6,
          });
        });
    } catch {}
  }

  return candidates;
}

async function searchEngineCandidates(
  queries: string[],
  highQualitySources: HighQualitySource[],
  fetchImpl: typeof fetch,
) {
  const candidates: UrlCandidate[] = [];
  const hosts = highQualitySources.map((item) => item.hostname);

  for (const query of queries.slice(0, 2)) {
    try {
      const googleHtml = await fetchText(`https://www.google.com/search?q=${encodeURIComponent(query)}`, fetchImpl);
      extractGoogleResultLinks(googleHtml)
        .slice(0, 8)
        .forEach((url, index) => {
          const hostname = new URL(url).hostname.toLowerCase();
          const source = highQualitySources.find((item) => hostname.includes(item.hostname));
          candidates.push({
            url,
            title: query,
            snippet: "",
            sourceLabel: source ? `Google / ${source.hostname}` : "Google Search",
            sourceKind: source ? "high-quality" : "search",
            weight: source ? source.weight - index : 52 - index * 2,
          });
        });
    } catch {}

    try {
      const baiduHtml = await fetchText(`https://www.baidu.com/s?wd=${encodeURIComponent(query)}`, fetchImpl);
      extractBaiduResultLinks(baiduHtml)
        .slice(0, 8)
        .forEach((url, index) => {
          const hostname = new URL(url).hostname.toLowerCase();
          const source = highQualitySources.find((item) => hostname.includes(item.hostname));
          candidates.push({
            url,
            title: query,
            snippet: "",
            sourceLabel: source ? `Baidu / ${source.hostname}` : "Baidu Search",
            sourceKind: source ? "high-quality" : "search",
            weight: source ? source.weight - index : 48 - index * 2,
          });
        });
    } catch {}

    for (const host of hosts.slice(0, 4)) {
      try {
        const searchQuery = `site:${host} ${query}`;
        const googleHtml = await fetchText(`https://www.google.com/search?q=${encodeURIComponent(searchQuery)}`, fetchImpl);
        extractGoogleResultLinks(googleHtml)
          .slice(0, 2)
          .forEach((url, index) => {
            candidates.push({
              url,
              title: query,
              snippet: "",
              sourceLabel: `Google site:${host}`,
              sourceKind: "high-quality",
              weight: 118 - index * 3,
            });
          });
      } catch {}
    }
  }

  return candidates;
}

async function collectMetadataCandidates(candidates: UrlCandidate[], fetchImpl: typeof fetch) {
  const metadata: MetadataCandidate[] = [];

  for (const candidate of candidates.slice(0, 8)) {
    try {
      const html = await fetchText(candidate.url, fetchImpl);
      const title = stripHtml(extractMetaContent(html, "og:title") || html.match(/<title>([^<]+)<\/title>/i)?.[1] || "");
      const description = stripHtml(
        extractMetaContent(html, "og:description") || extractMetaContent(html, "description", "name") || "",
      );
      const imageUrl = compact(extractMetaContent(html, "og:image"));
      const sourceText = `${title} ${description}`;
      metadata.push({
        sourceUrl: candidate.url,
        title,
        description,
        imageUrl,
        imageTitle: title,
        year: extractYear(sourceText),
        label: extractLabel(sourceText),
        releaseDate: extractReleaseDate(sourceText),
        venue: extractVenue(sourceText),
        weight: candidate.weight,
      });
    } catch {}
  }

  return metadata;
}

function buildResourceLinkSuggestion(
  recording: Recording,
  candidates: UrlCandidate[],
  metadataCandidates: MetadataCandidate[],
  context: RecordingContext,
): AutomationProposal | null {
  const existingLinks = (recording.links || []).filter((link) => {
    const candidate = candidates.find((item) => item.url === link.url);
    const metadata = metadataCandidates.find((item) => item.sourceUrl === link.url);
    const score = scoreRecordingMatch(
      [link.title, candidate?.title, candidate?.snippet, metadata?.title, metadata?.description, link.url].filter(Boolean).join(" "),
      context,
    );
    return score >= 2;
  });
  const suggestedLinks = dedupeBy(
    candidates
      .filter((candidate) => {
        if (detectPlatformFromUrl(candidate.url) === "other" && !candidate.url) {
          return false;
        }
        const metadata = metadataCandidates.find((item) => item.sourceUrl === candidate.url);
        return (
          scoreRecordingMatch(
            [candidate.title, candidate.snippet, metadata?.title, metadata?.description, candidate.url].filter(Boolean).join(" "),
            context,
          ) >= 2
        );
      })
      .map((candidate) => ({
        platform: detectPlatformFromUrl(candidate.url),
        url: candidate.url,
        title: candidate.title || "",
        weight: candidate.weight,
      })),
    (item) => `${item.platform}|${item.url}`.toLowerCase(),
  );

  const newLinks = suggestedLinks.filter(
    (candidate) => !existingLinks.some((link) => link.url === candidate.url && link.platform === candidate.platform),
  );
  if (!newLinks.length) {
    return null;
  }

  const afterLinks = dedupeBy(
    [
      ...existingLinks.map((link) => ({ ...link, weight: 999 })),
      ...newLinks,
    ]
      .sort((left, right) => right.weight - left.weight)
      .map(({ platform, url, title }) => ({ platform, url, title })),
    (item) => `${item.platform}|${item.url}`.toLowerCase(),
  );

  return {
    id: `${recording.id}-resource-links-auto`,
    entityType: "recording",
    entityId: recording.id,
    summary: `补充资源链接：${recording.title}`,
    risk: "medium",
    status: "pending",
    sources: newLinks.map((item) => item.url),
    fields: [
      {
        path: "links",
        before: existingLinks,
        after: afterLinks,
      },
    ],
    warnings: [`自动搜索到 ${newLinks.length} 条新增资源候选，默认保留现有链接并增补。`],
  };
}

async function buildMetadataSuggestion(
  recording: Recording,
  context: RecordingContext,
  metadataCandidates: MetadataCandidate[],
  llmConfig?: LlmConfig,
  fetchImpl?: typeof fetch,
): Promise<AutomationProposal | null> {
  const best = metadataCandidates
    .filter((candidate) => scoreRecordingMatch([candidate.title, candidate.description, candidate.sourceUrl].join(" "), context) >= 4)
    .sort((left, right) => right.weight - left.weight)[0];
  if (!best) {
    return null;
  }

  const fields = [];
  if (!recording.performanceDateText && best.year) {
    fields.push({ path: "performanceDateText", before: recording.performanceDateText, after: best.year });
  }
  if (!recording.label && best.label) {
    fields.push({ path: "label", before: recording.label, after: best.label });
  }
  if (!recording.releaseDate && best.releaseDate) {
    fields.push({ path: "releaseDate", before: recording.releaseDate, after: best.releaseDate });
  }
  if (!recording.venueText && best.venue) {
    fields.push({ path: "venueText", before: recording.venueText, after: best.venue });
  }
  if (!recording.albumTitle && isUsableAlbumTitle(best.title)) {
    fields.push({ path: "albumTitle", before: recording.albumTitle, after: best.title });
  }

  const sourceText = [best.title, best.description].filter(Boolean).join(" ");
  if (!recording.notes && sourceText && isUsableNotesSummary(sourceText)) {
    const summary =
      llmConfig && isLlmConfigured(llmConfig) && fetchImpl
        ? await generateConciseChineseSummary({
            config: llmConfig,
            title: recording.title,
            sourceText,
            fetchImpl,
          })
        : clip(sourceText, 80);
    if (summary) {
      fields.push({ path: "notes", before: recording.notes, after: summary });
    }
  }

  if (!fields.length) {
    return null;
  }

  return {
    id: `${recording.id}-metadata-auto`,
    entityType: "recording",
    entityId: recording.id,
    summary: `补充版本信息：${recording.title}`,
    risk: "medium",
    status: "pending",
    sources: [best.sourceUrl],
    fields,
    warnings: ["版本信息来自自动搜索结果，应用前请优先核对时间、地点与发行信息。"],
  };
}

function buildImageSuggestion(recording: Recording, metadataCandidates: MetadataCandidate[]): AutomationProposal | null {
  if (hasUsableImageSource(recording.images || [])) {
    return null;
  }
  const isLikelyLive = !recording.albumTitle && Boolean(recording.performanceDateText || recording.venueText);
  const rawCandidates = metadataCandidates
    .filter((candidate) => candidate.imageUrl)
    .map(
      (candidate, index) =>
        ({
          id: `${recording.id}-image-${index}`,
          src: candidate.imageUrl,
          sourceUrl: candidate.sourceUrl,
          sourceKind: candidate.sourceUrl.includes("bilibili.com") || candidate.sourceUrl.includes("youtube.com") ? "streaming" : "official-site",
          attribution: candidate.sourceUrl,
          title: candidate.imageTitle || recording.title,
          width: 1200,
          height: 1200,
          score: candidate.weight,
        }) satisfies AutomationImageCandidate,
    );

  const imageCandidates = rankImageCandidates(
    {
      title: recording.albumTitle || recording.title,
      entityKind: "recording",
    },
    rawCandidates,
  );

  if (!imageCandidates.length) {
    return null;
  }

  return {
    id: `${recording.id}-image-auto`,
    entityType: "recording",
    entityId: recording.id,
    summary: isLikelyLive ? `补充现场图片候选：${recording.title}` : `补充版本封面：${recording.title}`,
    risk: "medium",
    status: "pending",
    sources: imageCandidates.map((candidate) => candidate.sourceUrl),
    fields: [],
    imageCandidates,
    warnings: isLikelyLive ? ["当前按现场版本处理，优先保留官方演出图候选。"] : [],
  };
}

function aggregateRecordingProposals(recording: Recording, proposals: AutomationProposal[]): AutomationProposal[] {
  const pending = proposals.filter((proposal) => proposal.fields.length > 0 || (proposal.imageCandidates?.length ?? 0) > 0);
  if (!pending.length) {
    return [];
  }

  const fields = [];
  const seenFields = new Set<string>();
  const warnings = new Set<string>();
  const sources = new Set<string>();
  const imageCandidates: AutomationImageCandidate[] = [];
  const evidence: AutomationProposalEvidence[] = [];
  const linkCandidates: AutomationLinkCandidate[] = [];

  for (const proposal of pending) {
    for (const source of proposal.sources || []) {
      if (source) {
        sources.add(source);
      }
    }
    for (const warning of proposal.warnings || []) {
      if (warning) {
        warnings.add(warning);
      }
    }
    for (const field of proposal.fields || []) {
      if (seenFields.has(field.path)) {
        continue;
      }
      seenFields.add(field.path);
      fields.push(field);
      evidence.push({
        field: field.path,
        sourceUrl: proposal.sources?.[0] || "",
        sourceLabel: proposal.summary,
        confidence: proposal.risk === "low" ? 0.85 : proposal.risk === "medium" ? 0.72 : 0.58,
      });
      if (field.path === "links" && Array.isArray(field.after)) {
        for (const item of field.after as ResourceLink[]) {
          if (item?.url) {
            linkCandidates.push({
              platform: item.platform,
              url: item.url,
              title: item.title || "",
              sourceLabel: proposal.summary,
              confidence: proposal.risk === "low" ? 0.85 : proposal.risk === "medium" ? 0.72 : 0.58,
            });
          }
        }
      }
    }
    for (const candidate of proposal.imageCandidates || []) {
      imageCandidates.push(candidate);
    }
  }

  return [
    {
      id: `${recording.id}-aggregate-auto`,
      entityType: "recording",
      entityId: recording.id,
      summary: pending.map((proposal) => proposal.summary).join("；") || `汇总版本自动检查：${recording.title}`,
      risk: pending.some((proposal) => proposal.risk === "high")
        ? "high"
        : pending.some((proposal) => proposal.risk === "medium")
          ? "medium"
          : "low",
      status: "pending",
      reviewState: "unseen",
      sources: [...sources],
      fields,
      warnings: [...warnings],
      imageCandidates,
      evidence,
      linkCandidates,
      selectedImageCandidateId: imageCandidates[0]?.id || "",
    },
  ];
}

export async function inspectRecordingEnhanced(
  recording: Recording,
  library: LibraryData,
  fetchImpl: typeof fetch,
  llmConfig?: LlmConfig,
) {
  const proposals: AutomationProposal[] = [];
  const issues = auditResourceLinks(recording.links);

  issues.forEach((issue) => {
    if (issue.code === "platform-mismatch") {
      const index = recording.links.findIndex((link) => link.url === issue.link.url && link.platform === issue.link.platform);
      if (index >= 0) {
        proposals.push({
          id: `${recording.id}-link-${index}-platform`,
          entityType: "recording",
          entityId: recording.id,
          summary: `修正资源平台：${recording.title}`,
          risk: "low",
          status: "pending",
          sources: [issue.link.url],
          fields: [
            {
              path: `links[${index}].platform`,
              before: recording.links[index]?.platform,
              after: detectPlatformFromUrl(issue.link.url),
            },
          ],
          warnings: [issue.message],
        });
      }
      return;
    }

    proposals.push({
      id: `${recording.id}-${issue.code}`,
      entityType: "recording",
      entityId: recording.id,
      summary: `资源链接需人工复核：${recording.title}`,
      risk: "high",
      status: "pending",
      sources: [issue.link.url],
      fields: [],
      warnings: [issue.message],
    });
  });

  const context = getRecordingContext(recording, library);
  const queries = buildSearchQueries(context);
  const highQualitySources = await loadHighQualityRecordingSources();
  const existingCandidates: UrlCandidate[] = (recording.links || []).map((link) => ({
    url: link.url,
    title: link.title || recording.title,
    snippet: "",
    sourceLabel: "Existing Link",
    sourceKind: "existing",
    weight: 120,
  }));
  const [streamingCandidates, searchCandidates] = await Promise.all([
    searchStreamingCandidates(queries, fetchImpl),
    searchEngineCandidates(queries, highQualitySources, fetchImpl),
  ]);
  const allCandidates = dedupeBy(
    [...existingCandidates, ...streamingCandidates, ...searchCandidates].sort((left, right) => right.weight - left.weight),
    (item) => item.url,
  );
  const matchedCandidates = allCandidates.filter((candidate) => {
    if (candidate.sourceKind === "existing") {
      return true;
    }
    return scoreRecordingMatch([candidate.title, candidate.snippet, candidate.url].join(" "), context) >= 2;
  });
  const metadataCandidates = (await collectMetadataCandidates(matchedCandidates, fetchImpl)).filter(
    (candidate) => scoreRecordingMatch([candidate.title, candidate.description, candidate.sourceUrl].join(" "), context) >= 4,
  );

  const resourceProposal = buildResourceLinkSuggestion(recording, matchedCandidates, metadataCandidates, context);
  if (resourceProposal) {
    proposals.push(resourceProposal);
  }

  const metadataProposal = await buildMetadataSuggestion(recording, context, metadataCandidates, llmConfig, fetchImpl);
  if (metadataProposal) {
    proposals.push(metadataProposal);
  }

  const imageProposal = buildImageSuggestion(recording, metadataCandidates);
  if (imageProposal) {
    proposals.push(imageProposal);
  }

  return aggregateRecordingProposals(recording, proposals);
}



