import {
  createAutomationRun,
  isSuspiciousImageCandidate,
  normalizeAutomationProposals,
  rankImageCandidates,
  type AutomationCheckCategory,
  type AutomationImageCandidate,
  type AutomationProposal,
  type AutomationProposalEvidence,
  type AutomationRun,
} from "./automation.js";
import {
  generateConciseChineseSummary,
  generateEntityKnowledgeCandidate,
  generateWorkKnowledgeCandidate,
  isLlmConfigured,
  type LlmConfig,
} from "./llm.js";
import {
  buildRecordingRetrievalRequest,
  executeRecordingRetrievalJob,
  translateRecordingRetrievalResultsToProposals,
  type ExecuteRecordingRetrievalJobOptions,
  type RecordingRetrievalProvider,
  type RecordingRetrievalRequestOptions,
} from "./recording-retrieval.js";
import { isMissingLocalSiteAsset } from "./site-asset-health.js";
import type { Composer, LibraryData, Person, PersonRole, Recording, Work } from "../../shared/src/schema.js";

export type AutomationCheckRequest = {
  categories?: AutomationCheckCategory[];
  entityTypes?: AutomationCheckCategory[];
  composerIds?: string[];
  personIds?: string[];
  workIds?: string[];
  conductorIds?: string[];
  orchestraIds?: string[];
  artistIds?: string[];
  recordingIds?: string[];
};

export type RunAutomationChecksOptions = {
  recordingProvider?: RecordingRetrievalProvider;
  recordingRequestOptions?: RecordingRetrievalRequestOptions;
  recordingExecutionOptions?: ExecuteRecordingRetrievalJobOptions;
};

type EntitySourceCandidate = {
  sourceUrl: string;
  sourceKind: "wikipedia" | "wikimedia-commons" | "baidu-baike" | "llm" | "other";
  sourceLabel: string;
  summary: string;
  imageUrl: string;
  imageAttribution: string;
  birthYear?: number;
  deathYear?: number;
  country?: string;
  displayName?: string;
  displayFullName?: string;
  displayLatinName?: string;
  aliases?: string[];
  abbreviations?: string[];
  confidence?: number;
  rationale?: string;
};

const countryPatterns = [
  { value: "Austria", tokens: ["Austria", "Austrian", "奥地利", "奥地利籍"] },
  { value: "Germany", tokens: ["Germany", "German", "德国", "德意志"] },
  { value: "France", tokens: ["France", "French", "法国", "法兰西"] },
  { value: "Finland", tokens: ["Finland", "Finnish", "芬兰"] },
  { value: "Russia", tokens: ["Russia", "Russian", "俄国", "俄罗斯"] },
  { value: "Hungary", tokens: ["Hungary", "Hungarian", "匈牙利"] },
  { value: "Czech Republic", tokens: ["Czech Republic", "Czech", "捷克"] },
  { value: "Netherlands", tokens: ["Netherlands", "Dutch", "荷兰"] },
  { value: "Italy", tokens: ["Italy", "Italian", "意大利"] },
  { value: "Sweden", tokens: ["Sweden", "Swedish", "瑞典"] },
  { value: "United Kingdom", tokens: ["United Kingdom", "British", "英国", "英格兰"] },
  { value: "United States", tokens: ["United States", "American", "美国"] },
  { value: "China", tokens: ["China", "Chinese", "中国"] },
  { value: "Japan", tokens: ["Japan", "Japanese", "日本"] },
  { value: "Argentina", tokens: ["Argentina", "Argentinian", "阿根廷"] },
  { value: "Israel", tokens: ["Israel", "Israeli", "以色列"] },
  { value: "India", tokens: ["India", "Indian", "印度"] },
  { value: "Austria-Hungary", tokens: ["Austria-Hungary", "奥匈帝国"] },
];

const artistRoles: PersonRole[] = ["soloist", "singer", "ensemble", "chorus", "instrumentalist"];

function getEntityAbbreviations(entity: Composer | Person) {
  return uniqueStrings((entity.aliases || []).filter((value) => /^[A-Z0-9][A-Z0-9 .&/-]{1,15}$/.test(String(value ?? "").trim())));
}

function getEntityShortChineseName(entity: Composer | Person) {
  const aliases = uniqueStrings(entity.aliases || []);
  return (
    aliases.find((value) => /[\u3400-\u9fff]/.test(value) && value.length < entity.name.length) ||
    aliases.find((value) => /[\u3400-\u9fff]/.test(value) && value !== entity.name) ||
    entity.name
  );
}

function describeFetchError(error: unknown) {
  if (!(error instanceof Error)) {
    return String(error);
  }
  const cause = error.cause && typeof error.cause === "object" ? error.cause : null;
  const code = cause && "code" in cause ? String(cause.code) : "";
  const details = cause && "message" in cause ? String(cause.message) : "";
  return [error.message, code, details].filter(Boolean).join(" | ");
}

function normalizeName(value: string) {
  return String(value || "")
    .toLowerCase()
    .normalize("NFKC")
    .replace(/[\s·.,'"()\-_/]+/g, "")
    .trim();
}

function extractYearPair(value: string) {
  const matched = value.match(/(1[6-9]\d{2}|20\d{2}).{0,8}(1[6-9]\d{2}|20\d{2})/);
  if (!matched) {
    return { birthYear: undefined, deathYear: undefined };
  }
  return {
    birthYear: Number(matched[1]),
    deathYear: Number(matched[2]),
  };
}

function extractCountry(value: string) {
  const text = String(value || "");
  if (/Argentine/i.test(text)) {
    return "Argentina";
  }
  return countryPatterns.find((item) => item.tokens.some((token) => text.includes(token)))?.value ?? "";
}

function stripHtml(value: string) {
  return value.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
}

function normalizeWhitespace(value: string) {
  return String(value || "")
    .replace(/\s+/g, " ")
    .replace(/[‐‑‒–—]+/g, "-")
    .trim();
}

function isBlockedBaiduResultPage(responseUrl: string, title: string, html: string) {
  const normalizedUrl = String(responseUrl || "").toLowerCase();
  const normalizedTitle = normalizeWhitespace(stripHtml(title)).toLowerCase();
  const normalizedHtml = normalizeWhitespace(stripHtml(html)).toLowerCase();
  return (
    normalizedUrl.includes("wappass.baidu.com") ||
    normalizedUrl.includes("captcha") ||
    normalizedTitle.includes("安全验证") ||
    normalizedTitle.includes("captcha") ||
    normalizedHtml.includes("安全验证") ||
    normalizedHtml.includes("请完成验证") ||
    normalizedHtml.includes("captcha")
  );
}

function sanitizeLatinDisplayName(value: string) {
  return normalizeWhitespace(
    String(value || "")
      .replace(/[,，]\s*(\d{3,4}|\d{1,2}\s*年.*)$/g, "")
      .replace(/\s*[（(]\s*(\d{3,4}|\d{1,2}\s*年.*)[)）]\s*$/g, "")
      .replace(/\s*(\d{4}\s*[-–—]\s*\d{4}|\d{4}\s*年.*)$/g, "")
      .replace(/\s+\|\s+.*$/, "")
      .replace(/[，,;；:：、\-\s]+$/g, "")
      .replace(/\s{2,}.*/, ""),
  );
}

function sanitizeBaiduTitle(value: string) {
  const baiduBoilerplatePattern =
    /内容开放|网络百科全书|自由的网络百科|百科全书/i;
  const normalized = normalizeWhitespace(String(value || ""));
  if (!normalized) {
    return "";
  }
  if (baiduBoilerplatePattern.test(normalized)) {
    return "";
  }
  return normalizeWhitespace(
    normalized
      .replace(/[_-—–―|s]*百度百科.*$/i, "")
      .replace(/s*-s*百度百科.*$/i, "")
      .replace(/s*百度百科.*$/i, ""),
  );
}

function extractLatinNameFromMixedText(value: string) {
  const matched = String(value || "").match(/[（(]([A-Za-zÀ-ÿ][^（）()]{2,120})[)）]/);
  return normalizeWhitespace(matched?.[1] || "");
}

function extractChineseLead(value: string) {
  const boilerplatePattern =
    /\u767e\u5ea6\u767e\u79d1\u662f\u4e00\u90e8\u5185\u5bb9\u5f00\u653e\u3001\u81ea\u7531\u7684\u7f51\u7edc\u767e\u79d1\u5168\u4e66|\u767e\u5ea6\u767e\u79d1\u662f\u4e00\u90e8\u5185\u5bb9\u5f00\u653e\u81ea\u7531\u7684\u7f51\u7edc\u767e\u79d1\u5168\u4e66|\u767e\u5ea6\u767e\u79d1/g;
  const clausePattern =
    /(\u662f|\u4e3a|\u7531|\u7cfb|\u4f4d\u4e8e|\u6210\u7acb\u4e8e|\u521b\u5efa\u4e8e|\u521b\u7acb\u4e8e|\u4e8e)[\s\S]*$/u;
  const extractedLead = normalizeWhitespace(stripHtml(String(value || "")))
    .replace(boilerplatePattern, "")
    .trim()
    .match(/^([\u3400-\u9fff\s]{2,40})/)?.[1];

  return normalizeWhitespace(extractedLead || "")
    .replace(/\s+/g, "")
    .replace(clausePattern, "");
}

function sanitizeChineseName(value: string) {
  const boilerplatePattern =
    /\u767e\u5ea6\u767e\u79d1\u662f\u4e00\u90e8\u5185\u5bb9\u5f00\u653e\u3001\u81ea\u7531\u7684\u7f51\u7edc\u767e\u79d1\u5168\u4e66|\u767e\u5ea6\u767e\u79d1\u662f\u4e00\u90e8\u5185\u5bb9\u5f00\u653e\u81ea\u7531\u7684\u7f51\u7edc\u767e\u79d1\u5168\u4e66|\u767e\u5ea6\u767e\u79d1/g;
  const clausePattern =
    /(\u662f|\u4e3a|\u7531|\u7cfb|\u4f4d\u4e8e|\u6210\u7acb\u4e8e|\u521b\u5efa\u4e8e|\u521b\u7acb\u4e8e|\u4e8e)[\s\S]*$/u;
  return normalizeWhitespace(
    String(value || "")
      .replace(/\([^()]{0,80}\)/g, " ")
      .replace(/[,:;|].*$/g, "")
      .replace(boilerplatePattern, "")
      .replace(clausePattern, "")
      .replace(/\s+/g, "")
      .trim(),
  );
}

function looksLikeGenericChineseDescriptor(value: string) {
  const normalized = sanitizeChineseName(value);
  if (!normalized || /[·•]/.test(normalized)) {
    return false;
  }
  if (
    /(作曲家|指挥家|演奏家|钢琴家|小提琴家|大提琴家|歌唱家|歌手|男高音|女高音|男中音|女中音|男低音|女低音|音乐家|艺术家|音乐总监|教授|学者)$/.test(
      normalized,
    )
  ) {
    return true;
  }
  return /^(中国|法国|德国|奥地利|意大利|英国|美国|俄罗斯|波兰|捷克|匈牙利|芬兰|挪威|瑞典|丹麦|荷兰|比利时|瑞士|日本|韩国|西班牙|葡萄牙|巴西|阿根廷|澳大利亚|加拿大|乌克兰|白俄罗斯|罗马尼亚|保加利亚|塞尔维亚|克罗地亚|斯洛伐克|斯洛文尼亚|爱尔兰|以色列|希腊|土耳其|古巴|墨西哥|智利).{0,8}(作曲家|指挥家|演奏家|钢琴家|小提琴家|大提琴家|歌唱家|歌手|男高音|女高音|男中音|女中音|男低音|女低音|音乐家|艺术家|音乐总监)$/.test(
    normalized,
  );
}

function looksLikeChineseName(value: string) {
  return /^[㐀-鿿路·]{2,24}$/.test(sanitizeChineseName(value));
}

function scoreChineseName(value: string, candidateScore: number, referenceName = "") {
  if (/(百度百科|内容开放|网络百科全书)/.test(String(value || ""))) {
    return -1;
  }
  if (/(是|为|由|系|位于|成立于|创建于|创立于).{2,}/u.test(String(value || ""))) {
    return -1;
  }
  const normalized = sanitizeChineseName(value);
  if (!looksLikeChineseName(normalized)) {
    return -1;
  }
  const separatorBoost = /[路·]/.test(normalized) ? 8 : 0;
  const lengthBoost = Math.min(normalized.length, 16);
  const sameAsReferencePenalty = normalized === sanitizeChineseName(referenceName) ? 4 : 0;
  return candidateScore + separatorBoost + lengthBoost - sameAsReferencePenalty;
}

function pickBestChineseFullName(entity: Composer | Person, candidates: EntitySourceCandidate[]) {
  const isLikelyFullChineseName = (value: string) => {
    const normalized = sanitizeChineseName(value);
    if (!looksLikeChineseName(normalized)) {
      return false;
    }
    if (normalized === sanitizeChineseName(entity.name || "")) {
      return true;
    }
    return /[璺穄]/.test(normalized) || normalized.length >= 4;
  };
  const options = [
    ...candidates.flatMap((candidate) => [
      { value: candidate.displayFullName, score: scoreEntityCandidate(candidate) + 16 },
      ...(candidate.aliases || []).map((value) => ({ value, score: scoreEntityCandidate(candidate) + 8 })),
      { value: extractChineseLead(candidate.summary), score: scoreEntityCandidate(candidate) + 4 },
    ]),
    { value: entity.name, score: 12 },
  ]
    .map((option) => ({
      value: sanitizeChineseName(option.value || ""),
      score: scoreChineseName(option.value || "", option.score, entity.name),
    }))
    .filter(
      (option) =>
        option.score >= 0 &&
        isLikelyFullChineseName(option.value) &&
        !looksLikeGenericChineseDescriptor(option.value) &&
        !/^(中国|法国|德国|奥地利|意大利|英国|美国|俄罗斯|波兰|捷克|匈牙利|芬兰|挪威|瑞典|丹麦|荷兰|比利时|瑞士|日本|韩国|西班牙|葡萄牙|巴西|阿根廷|澳大利亚|加拿大|乌克兰|白俄罗斯|罗马尼亚|保加利亚|塞尔维亚|克罗地亚|斯洛伐克|斯洛文尼亚|爱尔兰|以色列|希腊|土耳其|古巴|墨西哥|智利)?(作曲家|指挥家|演奏家|钢琴家|小提琴家|大提琴家|歌唱家|歌手|男高音|女高音|男中音|女中音|男低音|女低音|音乐家|艺术家|音乐总监)/.test(
          option.value,
        ),
    )
    .sort((left, right) => right.score - left.score);

  return options[0]?.value || "";
}

function pickBestChineseShortName(entity: Composer | Person, candidates: EntitySourceCandidate[], fullName: string) {
  const options = [
    ...candidates.flatMap((candidate) => [
      { value: candidate.displayName, score: scoreEntityCandidate(candidate) + 12 },
      ...(candidate.aliases || []).map((value) => ({ value, score: scoreEntityCandidate(candidate) + 5 })),
    ]),
    { value: getEntityShortChineseName(entity), score: 10 },
    { value: entity.name, score: 8 },
    { value: fullName, score: 4 },
  ]
    .map((option) => {
      const value = sanitizeChineseName(option.value || "");
      const shortPenalty = value.length > 6 ? 18 : 0;
      return {
        value,
        score: scoreChineseName(option.value || "", option.score, fullName) - shortPenalty,
      };
    })
    .filter((option) => option.score >= 0)
    .sort((left, right) => right.score - left.score);

  return options[0]?.value || "";
}

function extractMetaContent(html: string, key: string, attr: "property" | "name" = "property") {
  const patternA = new RegExp(`<meta[^>]+${attr}=["']${key}["'][^>]+content=["']([^"']+)["']`, "i");
  const patternB = new RegExp(`<meta[^>]+content=["']([^"']+)["'][^>]+${attr}=["']${key}["']`, "i");
  return html.match(patternA)?.[1] || html.match(patternB)?.[1] || "";
}

function extractBaiduBaikeImageUrl(html: string) {
  const patterns = [
    /https?:\/\/bkimg\.cdn\.bcebos\.com\/pic\/[^\s"'<>\\]+/gi,
    /https?:\/\/pic\.rmb\.bdstatic\.com\/[^\s"'<>\\]+/gi,
    /https?:\/\/baikebcs\.bdimg\.com\/[^\s"'<>\\]+/gi,
  ];

  for (const pattern of patterns) {
    const matches = html.match(pattern) || [];
    const usable = matches.find((value) => !/logo|favicon|default|placeholder|sprite/i.test(value));
    if (usable) {
      return usable.replace(/&amp;/g, "&");
    }
  }

  return "";
}

function uniqueStrings(values: Array<string | undefined>) {
  return [...new Set(values.map((value) => String(value ?? "").trim()).filter(Boolean))];
}

function deriveInstitutionAbbreviations(...values: Array<string | undefined>) {
  const stopwords = new Set([
    "the",
    "of",
    "and",
    "for",
    "des",
    "de",
    "del",
    "der",
    "die",
    "das",
    "du",
    "le",
    "la",
    "les",
    "los",
    "las",
    "und",
  ]);
  const abbreviations = new Set<string>();

  for (const value of values) {
    const normalized = normalizeWhitespace(value || "");
    if (!normalized || !looksLikeInstitutionAlias(normalized)) {
      continue;
    }
    const ascii = normalized
      .normalize("NFKD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/[^A-Za-z0-9/& -]/g, " ");
    const tokens = ascii
      .split(/[\s/&-]+/)
      .map((token) => token.trim())
      .filter((token) => token.length > 0)
      .filter((token) => !stopwords.has(token.toLowerCase()));
    if (tokens.length < 2) {
      continue;
    }
    const abbreviation = tokens
      .map((token) => token[0]?.toUpperCase() || "")
      .join("")
      .replace(/[^A-Z0-9]/g, "");
    if (abbreviation.length >= 2 && abbreviation.length <= 6) {
      abbreviations.add(abbreviation);
    }
  }

  return [...abbreviations];
}

function getCandidateAbbreviations(candidate: EntitySourceCandidate) {
  return uniqueStrings([
    ...(candidate.abbreviations || []),
    ...deriveInstitutionAbbreviations(
      candidate.displayLatinName,
      candidate.displayName,
      candidate.displayFullName,
      ...(candidate.aliases || []),
    ),
  ]);
}

function scoreEntityCandidate(candidate: EntitySourceCandidate) {
  const imageBoost = candidate.imageUrl ? 20 : 0;
  const summaryBoost = candidate.summary ? Math.min(candidate.summary.length / 6, 15) : 0;
  const abbreviations = getCandidateAbbreviations(candidate);
  const fieldBoost = [
    candidate.country,
    candidate.birthYear,
    candidate.deathYear,
    candidate.displayName,
    candidate.displayFullName,
    candidate.displayLatinName,
    candidate.aliases?.length,
    abbreviations.length,
  ].filter(Boolean).length * 6;
  const sourceBoost =
    candidate.sourceKind === "wikimedia-commons"
      ? 36
      : candidate.sourceKind === "wikipedia"
        ? 32
        : candidate.sourceKind === "baidu-baike"
          ? 30
          : candidate.sourceKind === "llm"
            ? 28
            : 16;
  const confidenceBoost = Math.round((candidate.confidence ?? 0.55) * 25);
  return sourceBoost + imageBoost + summaryBoost + fieldBoost + confidenceBoost;
}

async function fetchWikipediaEntityCandidate(name: string, fetchImpl: typeof fetch): Promise<EntitySourceCandidate | null> {
  const searchUrl = `https://en.wikipedia.org/w/api.php?action=query&list=search&format=json&origin=*&srlimit=1&srsearch=${encodeURIComponent(name)}`;
  let searchResponse;
  try {
    searchResponse = await fetchImpl(searchUrl, { headers: { "User-Agent": "Mozilla/5.0 (compatible; ClassicalGuideBot/1.0)" } });
  } catch (error) {
    throw new Error(`Wikipedia search request failed for ${name}: ${describeFetchError(error)}`);
  }
  if (!searchResponse.ok) {
    throw new Error(`Wikipedia search failed for ${name}: HTTP ${searchResponse.status}`);
  }

  const searchPayload = (await searchResponse.json()) as {
    query?: { search?: Array<{ title: string }> };
  };
  const title = searchPayload.query?.search?.[0]?.title;
  if (!title) {
    return null;
  }

  const summaryUrl = `https://en.wikipedia.org/api/rest_v1/page/summary/${encodeURIComponent(title)}`;
  let summaryResponse;
  try {
    summaryResponse = await fetchImpl(summaryUrl, { headers: { "User-Agent": "Mozilla/5.0 (compatible; ClassicalGuideBot/1.0)" } });
  } catch (error) {
    throw new Error(`Wikipedia summary request failed for ${title}: ${describeFetchError(error)}`);
  }
  if (!summaryResponse.ok) {
    return null;
  }

  const summaryPayload = (await summaryResponse.json()) as {
    extract?: string;
    description?: string;
    content_urls?: { desktop?: { page?: string } };
    thumbnail?: { source?: string };
    originalimage?: { source?: string };
    title?: string;
  };
  const text = `${summaryPayload.description ?? ""} ${summaryPayload.extract ?? ""}`;
  const years = extractYearPair(text);
  const country = extractCountry(text);

  return {
    sourceUrl: summaryPayload.content_urls?.desktop?.page || summaryUrl,
    sourceKind: summaryPayload.originalimage?.source || summaryPayload.thumbnail?.source ? "wikimedia-commons" : "wikipedia",
    sourceLabel: "Wikipedia",
    summary: summaryPayload.extract ?? "",
    imageUrl: summaryPayload.originalimage?.source || summaryPayload.thumbnail?.source || "",
    imageAttribution: summaryPayload.title ? `Wikipedia: ${summaryPayload.title}` : "Wikipedia",
    birthYear: years.birthYear ?? extractBirthYearFromSummary(text),
    deathYear: years.deathYear ?? extractDeathYearFromSummary(text),
    country,
    displayLatinName: sanitizeLatinDisplayName(summaryPayload.title || title || name),
    confidence: 0.82,
  };
}

async function fetchBaiduBaikeCandidate(name: string, fetchImpl: typeof fetch): Promise<EntitySourceCandidate | null> {
  const urls = [
    `https://baike.baidu.com/item/${encodeURIComponent(name)}`,
    `https://baike.baidu.com/search/word?word=${encodeURIComponent(name)}`,
  ];

  for (const url of urls) {
    let response;
    try {
      response = await fetchImpl(url, {
        headers: { "User-Agent": "Mozilla/5.0 (compatible; ClassicalGuideBot/1.0)" },
      });
    } catch (error) {
      throw new Error(`Baidu Baike request failed for ${name}: ${describeFetchError(error)}`);
    }

    if (!response.ok) {
      continue;
    }

    const html = await response.text();
    const rawTitle = extractMetaContent(html, "og:title") || html.match(/<title>([^<]+)<\/title>/i)?.[1] || "";
    const title = sanitizeBaiduTitle(rawTitle);
    const summary = stripHtml(extractMetaContent(html, "description", "name") || extractMetaContent(html, "og:description"));
    const imageUrl = extractBaiduBaikeImageUrl(html);
    const text = `${title} ${summary}`;
    const years = extractYearPair(text);
    const country = extractCountry(text);
    const displayLatinName = sanitizeLatinDisplayName(extractLatinNameFromMixedText(summary) || extractLatinNameFromMixedText(text));
    const displayFullName = extractChineseLead(title) || extractChineseLead(summary);
    const displayName = normalizeWhitespace(name) || displayFullName;

    if (!title && !summary && !imageUrl) {
      continue;
    }

    return {
      sourceUrl: response.url || url,
      sourceKind: "baidu-baike",
      sourceLabel: "Baidu Baike",
      summary,
      imageUrl,
      imageAttribution: title ? `Baidu Baike: ${stripHtml(title)}` : "Baidu Baike",
      birthYear: years.birthYear ?? extractBirthYearFromSummary(text),
      deathYear: years.deathYear ?? extractDeathYearFromSummary(text),
      country,
      displayName,
      displayFullName,
      displayLatinName,
      confidence: 0.72,
    };
  }

  return null;
}

async function fetchBaiduSearchSnippetCandidate(name: string, fetchImpl: typeof fetch): Promise<EntitySourceCandidate | null> {
  const url = `https://www.baidu.com/s?wd=${encodeURIComponent(name)}`;
  let response;
  try {
    response = await fetchImpl(url, {
      headers: { "User-Agent": "Mozilla/5.0 (compatible; ClassicalGuideBot/1.0)" },
    });
  } catch (error) {
    throw new Error(`Baidu search request failed for ${name}: ${describeFetchError(error)}`);
  }

  if (!response.ok) {
    throw new Error(`Baidu search failed for ${name}: HTTP ${response.status}`);
  }

  const html = await response.text();
  const rawTitle =
    html.match(/<h3[^>]*>\s*<a[^>]*>([^<]+)<\/a>/i)?.[1] ||
    html.match(/class=["']c-title["'][^>]*>\s*<a[^>]*>([^<]+)<\/a>/i)?.[1] ||
    html.match(/<title>([^<]+)<\/title>/i)?.[1] ||
    "";
  if (isBlockedBaiduResultPage(response.url || url, rawTitle, html)) {
    return null;
  }
  const title = sanitizeBaiduTitle(stripHtml(rawTitle));
  const summary =
    html.match(/class=["']c-abstract["'][^>]*>([\s\S]*?)<\/div>/i)?.[1] ||
    html.match(/class=["']content-right_8Zs40["'][^>]*>([\s\S]*?)<\/div>/i)?.[1] ||
    "";
  const text = stripHtml(`${title} ${summary}`);
  const years = extractYearPair(text);
  const country = extractCountry(text);
  const displayLatinName = extractLatinNameFromMixedText(text);
  const displayFullName = extractChineseLead(title) || extractChineseLead(text);
  const displayName = displayFullName || extractChineseLead(text) || title;

  if (!text) {
    return null;
  }

  return {
    sourceUrl: response.url || url,
    sourceKind: "other",
    sourceLabel: "Baidu Search",
    summary: text,
    imageUrl: "",
    imageAttribution: title ? `Baidu Search: ${stripHtml(title)}` : "Baidu Search",
    birthYear: years.birthYear ?? extractBirthYearFromSummary(text),
    deathYear: years.deathYear ?? extractDeathYearFromSummary(text),
    country,
    displayName,
    displayFullName,
    displayLatinName,
    confidence: 0.58,
  };
}

async function fetchWikimediaCommonsImageCandidate(name: string, fetchImpl: typeof fetch): Promise<EntitySourceCandidate | null> {
  const url = `https://commons.wikimedia.org/w/api.php?action=query&generator=search&gsrsearch=${encodeURIComponent(name)}&gsrnamespace=6&gsrlimit=5&prop=imageinfo&iiprop=url|extmetadata&iiurlwidth=1200&iilimit=1&format=json&origin=*`;
  let response;
  try {
    response = await fetchImpl(url, {
      headers: { "User-Agent": "Mozilla/5.0 (compatible; ClassicalGuideBot/1.0)" },
    });
  } catch (error) {
    throw new Error(`Wikimedia Commons request failed for ${name}: ${describeFetchError(error)}`);
  }

  if (!response.ok) {
    throw new Error(`Wikimedia Commons failed for ${name}: HTTP ${response.status}`);
  }

  const payload = (await response.json()) as {
    query?: {
      pages?: Record<
        string,
        {
          title?: string;
          imageinfo?: Array<{
            url?: string;
            thumburl?: string;
            extmetadata?: {
              Artist?: { value?: string };
              Credit?: { value?: string };
              LicenseShortName?: { value?: string };
            };
          }>;
        }
      >;
    };
  };

  const usable = Object.values(payload.query?.pages || {})
    .map((page) => {
      const info = page.imageinfo?.[0];
      const title = normalizeWhitespace((page.title || "").replace(/^File:/i, ""));
      const imageUrl = info?.thumburl || info?.url || "";
      const attribution = normalizeWhitespace(
        stripHtml(
          info?.extmetadata?.Artist?.value ||
            info?.extmetadata?.Credit?.value ||
            info?.extmetadata?.LicenseShortName?.value ||
            "Wikimedia Commons",
        ),
      );
      const score = (title.toLowerCase().includes(name.toLowerCase()) ? 2 : 0) + (/\.(jpe?g|png|webp)$/i.test(imageUrl) ? 1 : 0);
      return { title, imageUrl, attribution, score };
    })
    .filter((item) => item.imageUrl && !/logo|icon|signature|autograph|wordmark/i.test(`${item.title} ${item.imageUrl}`))
    .sort((left, right) => right.score - left.score)[0];

  if (!usable) {
    return null;
  }

  return {
    sourceUrl: url,
    sourceKind: "wikimedia-commons",
    sourceLabel: "Wikimedia Commons",
    summary: "",
    imageUrl: usable.imageUrl,
    imageAttribution: usable.attribution,
    displayLatinName: sanitizeLatinDisplayName(name),
    confidence: 0.76,
  };
}

async function fetchLlmEntityCandidate(
  entity: Composer | Person,
  entityType: "composer" | "person",
  llmConfig?: LlmConfig,
  fetchImpl?: typeof fetch,
): Promise<EntitySourceCandidate | null> {
  if (!isLlmConfigured(llmConfig)) {
    return null;
  }

  const title = uniqueStrings([entity.nameLatin, entity.name]).slice(0, 2).join(" / ");
  const candidate = await generateEntityKnowledgeCandidate({
    config: llmConfig,
    title,
    entityType,
    roles: "roles" in entity ? entity.roles : [],
    knownDisplayName: getEntityShortChineseName(entity),
    knownDisplayFullName: entity.name,
    knownDisplayLatinName: entity.nameLatin,
    knownAliases: entity.aliases,
    knownAbbreviations: getEntityAbbreviations(entity),
    fetchImpl,
  });
  if (!candidate) {
    return null;
  }

  return {
    sourceUrl: llmConfig.baseUrl,
    sourceKind: "llm",
    sourceLabel: "LLM",
    summary: candidate.summary || "",
    imageUrl: "",
    imageAttribution: llmConfig.model,
    birthYear: candidate.birthYear,
    deathYear: candidate.deathYear,
    country: candidate.country,
    displayName: candidate.displayName,
    displayFullName: candidate.displayFullName,
    displayLatinName: sanitizeLatinDisplayName(candidate.displayLatinName || ""),
    aliases: candidate.aliases,
    abbreviations: candidate.abbreviations,
    confidence: candidate.confidence ?? 0.65,
    rationale: candidate.rationale,
  };
}

async function collectEntitySourceCandidates(
  entity: Composer | Person,
  entityType: "composer" | "person",
  fetchImpl: typeof fetch,
  llmConfig?: LlmConfig,
) {
  const westernTerms = uniqueStrings([entity.nameLatin, entity.name, ...entity.aliases]).slice(0, 4);
  const chineseTerms = uniqueStrings([entity.name, ...entity.aliases]).slice(0, 4);

  const runUntilHit = async (terms: string[], resolver: (term: string) => Promise<EntitySourceCandidate | null>) => {
    const failures: string[] = [];
    for (const term of terms) {
      try {
        const candidate = await resolver(term);
        if (candidate) {
          return candidate;
        }
      } catch (error) {
        failures.push(`${term}: ${describeFetchError(error)}`);
      }
    }
    if (failures.length === terms.length && failures.length > 0) {
      throw new Error(failures.join(" || "));
    }
    return null;
  };

  const tasks: Array<{ label: string; run: () => Promise<EntitySourceCandidate | null> }> = [
    {
      label: "Wikipedia",
      run: () => runUntilHit(westernTerms.length ? westernTerms : [entity.name], (term) => fetchWikipediaEntityCandidate(term, fetchImpl)),
    },
    {
      label: "Baidu Baike",
      run: () => runUntilHit(chineseTerms.length ? chineseTerms : [entity.name], (term) => fetchBaiduBaikeCandidate(term, fetchImpl)),
    },
  ];

  if (isLlmConfigured(llmConfig)) {
    tasks.push({ label: "LLM", run: () => fetchLlmEntityCandidate(entity, entityType, llmConfig, fetchImpl) });
  }

  tasks.push({
    label: "Baidu Search",
    run: () => runUntilHit(chineseTerms.length ? chineseTerms : [entity.name], (term) => fetchBaiduSearchSnippetCandidate(term, fetchImpl)),
  });
  tasks.push({
    label: "Wikimedia Commons",
    run: () =>
      runUntilHit(
        westernTerms.length ? westernTerms : [entity.nameLatin || entity.name],
        (term) => fetchWikimediaCommonsImageCandidate(term, fetchImpl),
      ),
  });

  const settled = await Promise.allSettled(tasks.map((task) => task.run()));
  const candidates: EntitySourceCandidate[] = [];
  const errors: string[] = [];

  settled.forEach((result, index) => {
    const label = tasks[index]?.label || "Unknown source";
    if (result.status === "fulfilled") {
      if (result.value) {
        candidates.push(result.value);
      } else {
        errors.push(`${label}: no result`);
      }
      return;
    }
    errors.push(`${label}: ${describeFetchError(result.reason)}`);
  });

  return { candidates, errors };
}

function buildEntityImageCandidates(entity: Composer | Person, candidates: EntitySourceCandidate[]) {
  const rawCandidates: AutomationImageCandidate[] = candidates
    .filter((candidate) => candidate.imageUrl)
    .map((candidate, index) => {
      const sourceKind: AutomationImageCandidate["sourceKind"] =
        candidate.sourceKind === "llm"
          ? "other"
          : candidate.sourceKind === "baidu-baike"
            ? "other"
            : candidate.sourceKind;
      return {
        id: `${entity.id}-image-${index}`,
        src: candidate.imageUrl,
        sourceUrl: candidate.sourceUrl,
        sourceKind,
        attribution: candidate.imageAttribution,
        title: candidate.displayFullName || candidate.displayLatinName || entity.nameLatin || entity.name,
        width: 1200,
        height: 1200,
      };
    });

  return rankImageCandidates(
    {
      title: entity.nameLatin || entity.name,
      entityKind:
        "roles" in entity && (entity.roles.includes("orchestra") || entity.roles.includes("ensemble") || entity.roles.includes("chorus"))
          ? "group"
          : "person",
    },
    rawCandidates,
  );
}

type NamedEntityFieldPath =
  | "summary"
  | "country"
  | "birthYear"
  | "deathYear"
  | "displayName"
  | "displayFullName"
  | "displayLatinName"
  | "abbreviations"
  | "aliases";

function scoreEntityCandidateForField(entity: Composer | Person, candidate: EntitySourceCandidate, field: NamedEntityFieldPath) {
  const family = classifyNamedEntityFamily(entity);
  const sourceKind = candidate.sourceKind;
  const sourceLabel = candidate.sourceLabel;
  const abbreviations = getCandidateAbbreviations(candidate);
  const isGroundedReference = sourceLabel === "Wikipedia" || sourceLabel === "Baidu Baike" || sourceLabel === "Baidu Search";
  const summaryCountry = candidate.summary ? extractCountry(candidate.summary) : "";
  const summaryBirthYear = candidate.summary ? extractBirthYearFromSummary(candidate.summary) : undefined;
  const summaryDeathYear = candidate.summary ? extractDeathYearFromSummary(candidate.summary) : undefined;
  const hasInstitutionSignals = Boolean(
    looksLikeInstitutionAlias(candidate.displayLatinName || "") ||
      looksLikeInstitutionAlias(candidate.displayFullName || "") ||
      looksLikeInstitutionAlias(candidate.displayName || "") ||
      looksLikeInstitutionAlias(candidate.summary || ""),
  );
  const hasBiographySignals = Boolean(candidate.birthYear || candidate.deathYear || summaryBirthYear || summaryDeathYear);
  let score = scoreEntityCandidate(candidate);

  if (field === "birthYear" || field === "deathYear") {
    if (family === "orchestra") {
      return Number.NEGATIVE_INFINITY;
    }
    if (field === "deathYear" && (sourceKind === "llm" || sourceLabel === "LLM") && !summaryDeathYear) {
      return Number.NEGATIVE_INFINITY;
    }
    if (sourceKind === "llm" || sourceLabel === "LLM") {
      score -= 18;
    }
    if (isGroundedReference) {
      score += 24;
    }
    if (field === "birthYear" && summaryBirthYear && candidate.birthYear && summaryBirthYear !== candidate.birthYear) {
      score -= 80;
    }
    if (field === "deathYear" && summaryDeathYear && candidate.deathYear && summaryDeathYear !== candidate.deathYear) {
      score -= 80;
    }
    return score;
  }

  if (field === "country") {
    if (sourceKind === "llm" || sourceLabel === "LLM") {
      score -= 8;
    }
    if (isGroundedReference) {
      score += 14;
    }
    if (family === "orchestra" && hasInstitutionSignals) {
      score += 18;
    }
    if (family !== "orchestra" && hasBiographySignals) {
      score += 10;
    }
    if (summaryCountry && candidate.country && summaryCountry !== candidate.country) {
      score -= 80;
    }
    return score;
  }

  if (field === "abbreviations") {
    if (family !== "orchestra") {
      return Number.NEGATIVE_INFINITY;
    }
    if (abbreviations.length) {
      score += 35;
    }
    if (!hasInstitutionSignals) {
      score -= 30;
    }
    return score;
  }

  if (field === "aliases") {
    if (family === "orchestra" && abbreviations.length) {
      score += 16;
    }
    if (family !== "orchestra" && abbreviations.length) {
      score -= 16;
    }
    return score;
  }

  if (field === "summary") {
    if (family === "orchestra") {
      score += hasInstitutionSignals ? 18 : -25;
      if (hasBiographySignals) {
        score -= 40;
      }
    }
    if (family === "conductor" && /conductor|指挥/i.test(candidate.summary || "")) {
      score += 18;
    }
    if (family === "artist" && /soloist|singer|instrumentalist|pianist|violinist|cellist|tenor|soprano|钢琴|小提琴|大提琴|女高音|男高音|独奏|歌唱|演奏/i.test(candidate.summary || "")) {
      score += 14;
    }
    if (family === "composer" && /composer|作曲/i.test(candidate.summary || "")) {
      score += 14;
    }
    return score;
  }

  if ((field === "displayName" || field === "displayFullName") && family === "orchestra" && hasInstitutionSignals) {
    score += 10;
  }

  return score;
}

function chooseBestFieldCandidate(
  entity: Composer | Person,
  candidates: EntitySourceCandidate[],
  field: NamedEntityFieldPath,
  selector: (candidate: EntitySourceCandidate) => unknown,
) {
  const ranked = [...candidates]
    .filter((candidate) => selector(candidate))
    .map((candidate) => ({
      candidate,
      score: scoreEntityCandidateForField(entity, candidate, field),
    }))
    .filter((entry) => Number.isFinite(entry.score))
    .sort((left, right) => right.score - left.score);
  return ranked[0]?.candidate;
}

function summarySuggestsLivingPerson(value: string) {
  const summary = normalizeWhitespace(value);
  if (!summary) {
    return false;
  }
  return Boolean(extractBirthYearFromSummary(summary) && !extractDeathYearFromSummary(summary));
}

function filterEntityCandidatesByReferenceSummary(
  entity: Composer | Person,
  candidates: EntitySourceCandidate[],
  field: Extract<NamedEntityFieldPath, "birthYear" | "deathYear" | "country">,
  referenceSummary: string,
) {
  const summary = normalizeWhitespace(referenceSummary);
  if (!summary) {
    return candidates;
  }

  const referenceBirthYear = extractBirthYearFromSummary(summary);
  const referenceDeathYear = extractDeathYearFromSummary(summary);
  const referenceCountry = extractCountry(summary);
  const family = classifyNamedEntityFamily(entity);

  return candidates.filter((candidate) => {
    if (field === "birthYear") {
      return !(referenceBirthYear && candidate.birthYear && candidate.birthYear !== referenceBirthYear);
    }
    if (field === "deathYear") {
      if (summarySuggestsLivingPerson(summary) && candidate.deathYear) {
        return false;
      }
      return !(referenceDeathYear && candidate.deathYear && candidate.deathYear !== referenceDeathYear);
    }
    if (field === "country" && family === "orchestra") {
      return !(referenceCountry && candidate.country && candidate.country !== referenceCountry);
    }
    return true;
  });
}

function looksLikeInstitutionAlias(value: string) {
  return /philharmonic|orchestra|ensemble|chorus|symphony|quartet|trio|愛樂|爱乐|樂團|乐团|交响乐团|交響樂團|合唱團|合唱团/.test(
    normalizeWhitespace(value).toLowerCase(),
  );
}

function containsRecordingContextNoise(value: string) {
  return /(?:19\d{2}|20\d{2}|youtube|bilibili|apple music|spotify|live|recording|concert|album|version|录音|现场|演出|版本|专辑)/i.test(
    normalizeWhitespace(value),
  );
}

function isAllowedAliasForEntity(entity: Composer | Person, value: string) {
  const normalized = normalizeWhitespace(value);
  if (!normalized || sanitizeChineseName(normalized) === sanitizeChineseName(entity.name) || normalized === entity.nameLatin) {
    return false;
  }
  if (containsRecordingContextNoise(normalized)) {
    return false;
  }
  if ("roles" in entity && !entity.roles.some((role) => ["orchestra", "ensemble", "chorus"].includes(role))) {
    if (/^[A-Z]{2,10}$/.test(normalized) || looksLikeInstitutionAlias(normalized)) {
      return false;
    }
  }
  return true;
}

function mergeAliases(entity: Composer | Person, existing: string[], incoming: string[] = []) {
  return [
    ...new Set(
      [...existing, ...incoming]
        .map((value) => String(value ?? "").trim())
        .filter((value) => isAllowedAliasForEntity(entity, value)),
    ),
  ];
}

function applyFieldPreview<T extends Record<string, unknown>>(entity: T, fields: AutomationProposal["fields"]) {
  const next = structuredClone(entity);
  for (const field of fields) {
    const segments = field.path
      .replace(/\[(\d+)\]/g, ".$1")
      .split(".")
      .map((segment) => segment.trim())
      .filter(Boolean)
      .map((segment) => (/^\d+$/.test(segment) ? Number(segment) : segment));
    let current: unknown = next;
    for (let index = 0; index < segments.length - 1; index += 1) {
      const segment = segments[index];
      const nextSegment = segments[index + 1];
      if (typeof segment === "number") {
        if (!Array.isArray(current)) {
          current = undefined;
          break;
        }
        current[segment] ??= typeof nextSegment === "number" ? [] : {};
        current = current[segment];
        continue;
      }
      const record = current as Record<string, unknown>;
      record[segment] ??= typeof nextSegment === "number" ? [] : {};
      current = record[segment];
    }
    if (typeof current === "undefined") {
      continue;
    }
    const finalSegment = segments.at(-1);
    if (typeof finalSegment === "undefined") {
      continue;
    }
    if (typeof finalSegment === "number") {
      if (Array.isArray(current)) {
        current[finalSegment] = field.after;
      }
      continue;
    }
    (current as Record<string, unknown>)[finalSegment] = field.after;
  }
  return next;
}

function looksLikeShortChineseName(value: string) {
  const normalized = String(value || "").trim();
  return normalized.length > 0 && normalized.length <= 4;
}

function scoreChineseFullNameRichness(value: string) {
  const normalized = sanitizeChineseName(value);
  if (!looksLikeChineseName(normalized)) {
    return Number.NEGATIVE_INFINITY;
  }
  const separatorBonus = /[·•・]/.test(normalized) ? 12 : 0;
  const lengthScore = Math.min(normalized.length, 16);
  const shortPenalty = normalized.length <= 2 ? 20 : normalized.length === 3 ? 10 : 0;
  return separatorBonus + lengthScore - shortPenalty;
}

function shouldReplaceChineseFullName(currentValue: string, nextValue: string) {
  const current = sanitizeChineseName(currentValue);
  const next = sanitizeChineseName(nextValue);
  if (!next || next === current) {
    return false;
  }
  if (!current) {
    return true;
  }
  return scoreChineseFullNameRichness(next) > scoreChineseFullNameRichness(current);
}

function shouldRefreshEntityImage(entity: Composer | Person) {
  const haystack = `${entity.avatarSrc ?? ""} ${entity.imageSourceUrl ?? ""} ${entity.imageAttribution ?? ""} ${entity.imageSourceKind ?? ""}`;
  if (!String(entity.avatarSrc || "").trim()) {
    return true;
  }
  if (isMissingLocalSiteAsset(entity.avatarSrc || "")) {
    return true;
  }
  return isSuspiciousImageCandidate({
    id: `${entity.id}-current-image`,
    src: entity.avatarSrc,
    sourceUrl: entity.imageSourceUrl || entity.avatarSrc,
    sourceKind: entity.imageSourceKind || "other",
    attribution: entity.imageAttribution,
    title: entity.nameLatin || entity.name,
  }) || /baidu|baike|logo|favicon|placeholder|sprite|default/i.test(haystack);
}

function classifyNamedEntityFamily(entity: Composer | Person) {
  if (!("roles" in entity)) {
    return "composer" as const;
  }
  if (entity.roles.some((role) => ["orchestra", "ensemble", "chorus"].includes(role))) {
    return "orchestra" as const;
  }
  if (entity.roles.includes("conductor")) {
    return "conductor" as const;
  }
  if (entity.roles.some((role) => artistRoles.includes(role))) {
    return "artist" as const;
  }
  return "person" as const;
}

function extractBirthYearFromSummary(value: string) {
  const text = normalizeWhitespace(value);
  return (
    Number(text.match(/(1[6-9]\d{2}|20\d{2})年[^。；;]{0,16}(?:出生|生于|诞生)/)?.[1]) ||
    Number(text.match(/born[^0-9]{0,8}(1[6-9]\d{2}|20\d{2})/i)?.[1]) ||
    undefined
  );
}

function extractDeathYearFromSummary(value: string) {
  const text = normalizeWhitespace(value);
  return (
    Number(text.match(/(1[6-9]\d{2}|20\d{2})年[^。；;]{0,16}(?:去世|逝世|病逝|卒于)/)?.[1]) ||
    Number(text.match(/died[^0-9]{0,8}(1[6-9]\d{2}|20\d{2})/i)?.[1]) ||
    undefined
  );
}

function collectEntityCompletionIssues(entity: Composer | Person) {
  const issues: string[] = [];
  if (!String(entity.name || "").trim()) {
    issues.push("中文全名仍为空，未达到规范。");
  }
  if (!String(entity.nameLatin || "").trim()) {
    issues.push("英文或原文全名仍为空，未达到规范。");
  }
  if (looksLikeShortChineseName(getEntityShortChineseName(entity)) && getEntityShortChineseName(entity) === entity.name) {
    issues.push("缺少可区分的中文别名或简称，未达到规范。");
  }
  if ("roles" in entity && entity.roles.some((role) => ["orchestra", "ensemble", "chorus"].includes(role)) && getEntityAbbreviations(entity).length === 0) {
    issues.push("团体简称或缩写仍为空，未达到规范。");
  }
  if (shouldRefreshEntityImage(entity)) {
    issues.push("当前图片缺失或疑似为无效图片，仍需刷新以满足规范。");
  }
  return issues;
}

export function reviewAutomationProposalQuality(entity: Composer | Person, proposals: AutomationProposal[]) {
  const mergedPreview = applyFieldPreview(entity as Record<string, unknown>, proposals.flatMap((proposal) => proposal.fields || [])) as Composer | Person;
  const issues = collectEntityCompletionIssues(mergedPreview);
  const imageCandidates = proposals.flatMap((proposal) => proposal.imageCandidates || []);
  const hasUsableImageCandidate = imageCandidates.some((candidate) => !isSuspiciousImageCandidate(candidate));
  if (hasUsableImageCandidate && shouldRefreshEntityImage(mergedPreview)) {
    const imageIssue = "当前图片缺失或疑似为无效图片，仍需刷新以满足规范。";
    const issueIndex = issues.indexOf(imageIssue);
    if (issueIndex >= 0) {
      issues.splice(issueIndex, 1);
    }
  }
  if (imageCandidates.some((candidate) => isSuspiciousImageCandidate(candidate))) {
    issues.push("图片候选疑似为站点 logo 或占位图。");
  }
  const summary = String(mergedPreview.summary || "").trim();
  if (summary) {
    const summaryBirthYear = extractBirthYearFromSummary(summary);
    const summaryDeathYear = extractDeathYearFromSummary(summary);
    if (summaryBirthYear && mergedPreview.birthYear && summaryBirthYear !== mergedPreview.birthYear) {
      issues.push("生卒年份与当前摘要内容冲突，疑似提取错误。");
    }
    if (summaryDeathYear && mergedPreview.deathYear && summaryDeathYear !== mergedPreview.deathYear) {
      issues.push("生卒年份与当前摘要内容冲突，疑似提取错误。");
    }
    if (classifyNamedEntityFamily(mergedPreview) === "orchestra") {
      const summaryCountry = extractCountry(summary);
      if (summaryCountry && mergedPreview.country && summaryCountry !== mergedPreview.country) {
        issues.push("国家字段与当前摘要内容冲突，疑似提取错误。");
      }
    }
  }
  const hasChanges = proposals.some(
    (proposal) => (proposal.fields?.length ?? 0) > 0 || (proposal.imageCandidates?.length ?? 0) > 0 || (proposal.mergeCandidates?.length ?? 0) > 0,
  );
  const status = hasChanges ? (issues.length === 0 ? "ok" : "needs-attention") : issues.length === 0 ? "already-complete" : "needs-attention";
  return {
    ok: status === "ok",
    status,
    issues,
    preview: mergedPreview,
    hasChanges,
  };
}

export function reviewWorkAutomationProposalQuality(work: Work, composer: Composer | undefined, proposals: AutomationProposal[]) {
  const mergedPreview = applyFieldPreview(work as Record<string, unknown>, proposals.flatMap((proposal) => proposal.fields || [])) as Work;
  const issues: string[] = [];
  const summary = String(mergedPreview.summary || "").trim();
  const hasChanges = proposals.some(
    (proposal) => (proposal.fields?.length ?? 0) > 0 || (proposal.imageCandidates?.length ?? 0) > 0 || (proposal.mergeCandidates?.length ?? 0) > 0,
  );

  if (!mergedPreview.titleLatin && !mergedPreview.catalogue && !summary) {
    issues.push("作品基础信息仍为空，未形成可审查的补全结果。");
  }

  if (summary && composer) {
    const composerHints = [composer.name, composer.fullName, composer.nameLatin].filter(Boolean);
    if (composerHints.length > 0 && !composerHints.some((hint) => summary.includes(String(hint)))) {
      issues.push("作品摘要缺少作曲家上下文，仍需人工复核。");
    }
  }

  const status = hasChanges ? (issues.length === 0 ? "ok" : "needs-attention") : issues.length === 0 ? "already-complete" : "needs-attention";
  return {
    ok: status === "ok",
    status,
    issues,
    preview: mergedPreview,
    hasChanges,
  };
}

function extractYearLikeNumber(value: string) {
  const matched = String(value || "").match(/\b(1[6-9]\d{2}|20\d{2})\b/);
  return matched ? Number(matched[1]) : undefined;
}

const recordingMetadataPaths = new Set(["performanceDateText", "venueText", "albumTitle", "label", "releaseDate", "notes"]);
const recordingDateFieldPaths = new Set(["performanceDateText", "releaseDate"]);
const recordingVenueFieldPaths = new Set(["venueText"]);

function recordingWarningRequiresAttention(warning: string, changedFieldPaths: Set<string>, hasImageCandidates: boolean) {
  const normalized = String(warning || "").trim();
  if (!normalized) {
    return false;
  }
  if (normalized.includes("未达到最终采纳阈值")) {
    if (normalized.includes("performanceDateText")) {
      return changedFieldPaths.has("performanceDateText");
    }
    if (normalized.includes("venueText")) {
      return changedFieldPaths.has("venueText");
    }
    if (normalized.includes("albumTitle")) {
      return changedFieldPaths.has("albumTitle");
    }
    if (normalized.includes("label")) {
      return changedFieldPaths.has("label");
    }
    if (normalized.includes("releaseDate")) {
      return changedFieldPaths.has("releaseDate");
    }
    if (normalized.includes("封面") || normalized.includes("图片")) {
      return hasImageCandidates;
    }
    return false;
  }

  const isCandidateScopedWarning = /^(候选|记录\d+|第\d+条URL|URL\s*\d+)/.test(normalized);
  const isRejectedCandidateNote =
    /保守排除|已排除|被排除|被拒绝|证据不足|错误曲目|非单一录音版本|合集|传记内容/.test(normalized);
  const isExplicitConflictGate = /冲突|需人工复核|建议人工复核|应用前需人工复核/.test(normalized);
  if (isRejectedCandidateNote || (isCandidateScopedWarning && !isExplicitConflictGate)) {
    return false;
  }

  if (/多个候选(?:URL)?年份或地点不匹配/.test(normalized) && !isExplicitConflictGate) {
    return false;
  }

  if (/多个候选包含无关内容或错误日期/.test(normalized) && !isExplicitConflictGate) {
    return false;
  }

  if (/同一录音的不同上传|不同上传或剪辑版本|不同上传版本|剪辑版本|转载视频|B站转载|搬运内容/.test(normalized) && !isExplicitConflictGate) {
    return false;
  }

  if (/不同拼写|拼写（如|拼写\(如|但指向同一录音/.test(normalized) && !isExplicitConflictGate) {
    return false;
  }

  if (
    /B站视频.*来源不明|相同音乐节信息.*来源不明|转载来源或演奏者信息不明确|转载来源.*不明确|演奏者信息不明确/.test(normalized) &&
    !isExplicitConflictGate
  ) {
    return false;
  }

  if (/其他URL.*不匹配/.test(normalized) && !isExplicitConflictGate) {
    return false;
  }

  const touchesDate = /日期|年份|年分|performanceDateText|releaseDate/.test(normalized);
  const touchesVenue = /地点|场地|venueText/.test(normalized);
  if (touchesDate || touchesVenue) {
    if (touchesDate && [...recordingDateFieldPaths].some((path) => changedFieldPaths.has(path))) {
      return true;
    }
    if (touchesVenue && [...recordingVenueFieldPaths].some((path) => changedFieldPaths.has(path))) {
      return true;
    }
    return false;
  }

  if (/不同上传|编辑版本|搬运内容|合集/.test(normalized) && isExplicitConflictGate) {
    return [...recordingDateFieldPaths, ...recordingVenueFieldPaths].some((path) => changedFieldPaths.has(path));
  }

  if (/拼写变体|核心信息一致|平台偏好不完全一致/.test(normalized)) {
    return false;
  }

  return true;
}

export function reviewRecordingAutomationProposalQuality(recording: Recording, proposals: AutomationProposal[]) {
  const mergedPreview = applyFieldPreview(recording as Record<string, unknown>, proposals.flatMap((proposal) => proposal.fields || [])) as Recording;
  const issues: string[] = [];
  const imageCandidates = proposals.flatMap((proposal) => proposal.imageCandidates || []);
  const changedFieldPaths = new Set(proposals.flatMap((proposal) => (proposal.fields || []).map((field) => field.path)));
  const hasChanges = proposals.some(
    (proposal) =>
      (proposal.fields?.length ?? 0) > 0 ||
      (proposal.imageCandidates?.length ?? 0) > 0 ||
      (proposal.linkCandidates?.length ?? 0) > 0 ||
      (proposal.mergeCandidates?.length ?? 0) > 0,
  );

  const performanceYear = extractYearLikeNumber(mergedPreview.performanceDateText || recording.performanceDateText || "");
  const releaseYear = extractYearLikeNumber(mergedPreview.releaseDate || "");
  if (performanceYear && releaseYear && releaseYear < performanceYear) {
    issues.push("发行日期早于当前演出日期，疑似提取错误。");
  }

  const hasMetadataWarnings = proposals.some((proposal) => {
    if ((proposal.warnings?.length ?? 0) === 0) {
      return false;
    }
    if (!(proposal.fields || []).some((field) => recordingMetadataPaths.has(field.path))) {
      return false;
    }
    return (proposal.warnings || []).some((warning) =>
      recordingWarningRequiresAttention(warning, changedFieldPaths, imageCandidates.length > 0),
    );
  });
  if (hasMetadataWarnings) {
    issues.push("版本提案仍带有来源冲突警告，应用前需要人工复核。");
  }

  if (imageCandidates.some((candidate) => isSuspiciousImageCandidate(candidate))) {
    issues.push("版本图片候选疑似为站点 logo 或占位图。");
  }

  const status = hasChanges ? (issues.length === 0 ? "ok" : "needs-attention") : issues.length === 0 ? "already-complete" : "needs-attention";
  return {
    ok: status === "ok",
    status,
    issues,
    preview: mergedPreview,
    hasChanges,
  };
}

async function inspectNamedEntity(
  entity: Composer | Person,
  entityType: "composer" | "person",
  fetchImpl: typeof fetch,
  llmConfig?: LlmConfig,
): Promise<AutomationProposal | null> {
  const { candidates, errors } = await collectEntitySourceCandidates(entity, entityType, fetchImpl, llmConfig);
  const existingIssues = collectEntityCompletionIssues(entity);
  const warnings = [...errors];
  if (!candidates.length) {
    if (!existingIssues.length) {
      return null;
    }
    return {
      id: `${entity.id}-${entityType}-review-only`,
      kind: "update",
      entityType,
      entityId: entity.id,
      summary: `自动复查：${entity.name}`,
      risk: "high",
      status: "pending",
      sources: [],
      fields: [],
      imageCandidates: [],
      warnings: uniqueStrings([
        ...warnings,
        ...existingIssues,
        "本轮未获取到可靠来源，请人工补录、启用 LLM 或稍后重试。",
      ]),
    };
  }

  const fields = [] as AutomationProposal["fields"];

  const chineseFullName = pickBestChineseFullName(entity, candidates);
  const chineseShortName = pickBestChineseShortName(entity, candidates, chineseFullName || entity.name);

  const summaryCandidate = chooseBestFieldCandidate(entity, candidates, "summary", (candidate) => candidate.summary);
  if (!entity.summary && summaryCandidate?.summary) {
    const llmSummary =
      llmConfig && summaryCandidate.sourceKind !== "llm"
        ? await generateConciseChineseSummary({
            config: llmConfig,
            title: entity.nameLatin || entity.name,
            sourceText: summaryCandidate.summary,
            fetchImpl,
          })
        : "";
    fields.push({ path: "summary", before: entity.summary, after: llmSummary || summaryCandidate.summary });
  }

  const referenceSummary = summaryCandidate?.summary || "";
  const countryCandidate = chooseBestFieldCandidate(
    entity,
    filterEntityCandidatesByReferenceSummary(entity, candidates, "country", referenceSummary),
    "country",
    (candidate) => candidate.country,
  );
  if (!entity.country && countryCandidate?.country) {
    fields.push({ path: "country", before: entity.country, after: countryCandidate.country });
  }

  const birthYearCandidate = chooseBestFieldCandidate(
    entity,
    filterEntityCandidatesByReferenceSummary(entity, candidates, "birthYear", referenceSummary),
    "birthYear",
    (candidate) => candidate.birthYear,
  );
  if (!entity.birthYear && birthYearCandidate?.birthYear) {
    fields.push({ path: "birthYear", before: entity.birthYear, after: birthYearCandidate.birthYear });
  }

  const deathYearCandidate = chooseBestFieldCandidate(
    entity,
    filterEntityCandidatesByReferenceSummary(entity, candidates, "deathYear", referenceSummary),
    "deathYear",
    (candidate) => candidate.deathYear,
  );
  if (!entity.deathYear && deathYearCandidate?.deathYear) {
    fields.push({ path: "deathYear", before: entity.deathYear, after: deathYearCandidate.deathYear });
  }

  const displayNameCandidate = chooseBestFieldCandidate(entity, candidates, "displayName", (candidate) => candidate.displayName);
  const nextDisplayName =
    chineseShortName ||
    sanitizeChineseName(displayNameCandidate?.displayName || "") ||
    sanitizeChineseName(getEntityShortChineseName(entity) || entity.name || "");

  const displayFullNameCandidate = chooseBestFieldCandidate(entity, candidates, "displayFullName", (candidate) => candidate.displayFullName);
  const sanitizedDisplayFullName = sanitizeChineseName(displayFullNameCandidate?.displayFullName || "");
  const nextFullName =
    chineseFullName ||
    (looksLikeGenericChineseDescriptor(sanitizedDisplayFullName) ||
    /^(中国|法国|德国|奥地利|意大利|英国|美国|俄罗斯|波兰|捷克|匈牙利|芬兰|挪威|瑞典|丹麦|荷兰|比利时|瑞士|日本|韩国|西班牙|葡萄牙|巴西|阿根廷|澳大利亚|加拿大|乌克兰|白俄罗斯|罗马尼亚|保加利亚|塞尔维亚|克罗地亚|斯洛伐克|斯洛文尼亚|爱尔兰|以色列|希腊|土耳其|古巴|墨西哥|智利)?(作曲家|指挥家|演奏家|钢琴家|小提琴家|大提琴家|歌唱家|歌手|男高音|女高音|男中音|女中音|男低音|女低音|音乐家|艺术家|音乐总监)/.test(
      sanitizedDisplayFullName,
    )
      ? ""
      : sanitizedDisplayFullName) ||
    sanitizeChineseName(entity.name || "");
  if (shouldReplaceChineseFullName(entity.name, nextFullName)) {
    fields.push({ path: "name", before: entity.name, after: nextFullName });
  }

  const displayLatinNameCandidate = chooseBestFieldCandidate(entity, candidates, "displayLatinName", (candidate) => candidate.displayLatinName);
  if (!entity.nameLatin && displayLatinNameCandidate?.displayLatinName) {
    fields.push({ path: "nameLatin", before: entity.nameLatin, after: displayLatinNameCandidate.displayLatinName });
  }

  const abbreviationsCandidate = chooseBestFieldCandidate(entity, candidates, "abbreviations", (candidate) => getCandidateAbbreviations(candidate).length);
  const aliasesCandidate = chooseBestFieldCandidate(entity, candidates, "aliases", (candidate) => candidate.aliases?.length);
  const aliasIncoming = mergeAliases(entity, aliasesCandidate?.aliases || [], [
    ...(abbreviationsCandidate ? getCandidateAbbreviations(abbreviationsCandidate) : []),
    nextFullName,
    nextDisplayName,
  ]);
  if (aliasIncoming.length) {
    const mergedAliases = mergeAliases(entity, entity.aliases || [], aliasIncoming);
    if (mergedAliases.length > (entity.aliases?.length ?? 0)) {
      fields.push({ path: "aliases", before: entity.aliases, after: mergedAliases });
    }
  }

  const imageNeedsRefresh = shouldRefreshEntityImage(entity);
  const rawImageCandidates = buildEntityImageCandidates(entity, candidates).filter((candidate) => !isSuspiciousImageCandidate(candidate));
  const imageCandidates = imageNeedsRefresh || fields.length > 0 ? rawImageCandidates : [];
  warnings.push(
    ...uniqueStrings(
      candidates
        .filter((candidate) => candidate.sourceKind === "llm" && candidate.rationale)
        .map((candidate) => `LLM 说明：${candidate.rationale}`),
    ),
  );
  if (!imageCandidates.length && imageNeedsRefresh) {
    warnings.push("未找到可用图片候选。");
  }
  if (candidates.some((candidate) => candidate.imageUrl) && imageCandidates.length === 0 && imageNeedsRefresh) {
    warnings.push("现有图片候选已因疑似 logo、占位图或低质量而被过滤。");
  }
  if (imageNeedsRefresh && entity.avatarSrc) {
    warnings.push("当前图片疑似为 logo、占位图或低质量图片，正在尝试替换。");
  }

  const reviewIssues = collectEntityCompletionIssues(applyFieldPreview(entity as Record<string, unknown>, fields) as Composer | Person);
  if (fields.length === 0 && imageCandidates.length === 0) {
    if (!reviewIssues.length) {
      return null;
    }
    return {
      id: `${entity.id}-${entityType}-review-only`,
      kind: "update",
      entityType,
      entityId: entity.id,
      summary: `自动复查：${entity.name}`,
      risk: reviewIssues.length ? "medium" : "low",
      status: "pending",
      sources: uniqueStrings(candidates.map((candidate) => candidate.sourceUrl)),
      fields: [],
      imageCandidates: [],
      warnings: uniqueStrings([
        ...warnings,
        ...reviewIssues,
        reviewIssues.length ? "当前没有可直接采用的候选，请人工补录、启用 LLM 或再次检查。" : "本轮未发现需要新增的信息。",
      ]),
    };
  }

  return {
    id: `${entity.id}-${entityType}-auto`,
    kind: "update",
    entityType,
    entityId: entity.id,
    summary: `自动检查：${entity.name}`,
    risk: imageCandidates.length && fields.length ? "medium" : "low",
    status: "pending",
    sources: uniqueStrings(candidates.map((candidate) => candidate.sourceUrl)),
    fields,
    imageCandidates,
    warnings,
  };
}

function extractYoutubeThumbnail(url: string) {
  try {
    const parsed = new URL(url);
    let videoId = "";
    if (parsed.hostname.includes("youtu.be")) {
      videoId = parsed.pathname.replace(/^\//, "").trim();
    } else {
      videoId = parsed.searchParams.get("v") || "";
    }
    if (!videoId) {
      return null;
    }
    return {
      src: `https://i.ytimg.com/vi/${videoId}/hqdefault.jpg`,
      sourceUrl: url,
      sourceKind: "streaming" as const,
      attribution: "YouTube thumbnail",
      title: "YouTube thumbnail",
      width: 480,
      height: 360,
    };
  } catch {
    return null;
  }
}

async function fetchOpenGraphImageCandidate(url: string, fetchImpl: typeof fetch) {
  try {
    const response = await fetchImpl(url, {
      headers: { "User-Agent": "Mozilla/5.0 (compatible; ClassicalGuideBot/1.0)" },
    });
    if (!response.ok) {
      return null;
    }

    const html = await response.text();
    const imageUrl = extractMetaContent(html, "og:image");
    if (!imageUrl) {
      return null;
    }
    const title = extractMetaContent(html, "og:title") || html.match(/<title>([^<]+)<\/title>/i)?.[1] || "";
    const hostname = new URL(url).hostname.toLowerCase();
    const sourceKind: "streaming" | "official-site" = hostname.includes("youtube") || hostname.includes("bilibili") ? "streaming" : "official-site";

    return {
      src: imageUrl,
      sourceUrl: url,
      sourceKind,
      attribution: hostname,
      title: stripHtml(title),
      width: 1200,
      height: 1200,
    };
  } catch {
    return null;
  }
}

async function inspectRecordingsViaProvider(
  library: LibraryData,
  recordings: Recording[],
  fetchImpl: typeof fetch,
  options?: RunAutomationChecksOptions,
) {
  if (!options?.recordingProvider) {
    throw new Error("版本自动检索工具未配置或不可用，当前不会回退到本地版本自动检查。");
  }
  if (recordings.length === 0) {
    return {
      proposals: [] as AutomationProposal[],
      provider: undefined,
    };
  }

  const request = buildRecordingRetrievalRequest(library, recordings, options.recordingRequestOptions);
  const execution = await executeRecordingRetrievalJob(
    options.recordingProvider,
    request,
    fetchImpl,
    options.recordingExecutionOptions,
  );

  return {
    proposals: translateRecordingRetrievalResultsToProposals(library, execution),
    provider: execution.runtimeState,
  };
}

function normalizeComparableText(value: string) {
  return String(value || "")
    .toLowerCase()
    .normalize("NFKC")
    .replace(/[\s·.,'"()\-_/]+/g, " ")
    .trim();
}

function buildWorkSearchQueries(work: Work, library: LibraryData) {
  const composer = library.composers.find((item) => item.id === work.composerId);
  return [
    [composer?.name, composer?.nameLatin, work.title].filter(Boolean).join(" "),
    [composer?.nameLatin, work.titleLatin || work.title].filter(Boolean).join(" "),
    [work.title, work.titleLatin, work.catalogue].filter(Boolean).join(" "),
  ]
    .map((value) => normalizeWhitespace(value))
    .filter(Boolean);
}

function extractCatalogueFromText(value: string) {
  return (
    String(value || "").match(
      /(?:Op\.?\s*\d+[a-z0-9-]*|BWV\s*\d+[a-z0-9-]*|K(?:V)?\.?\s*\d+[a-z0-9-]*|D\.?\s*\d+[a-z0-9-]*|S\.?\s*\d+[a-z0-9-]*|Hob\.?\s*[A-Z0-9:. -]+|GMW\s*\d+[a-z0-9-]*|作品\s*\d+[a-z0-9-]*)/i,
    )?.[0] || ""
  ).trim();
}

function extractStructuredCatalogueFromText(value: string) {
void [extractYoutubeThumbnail, fetchOpenGraphImageCandidate, extractCatalogueFromText];
  const text = normalizeWhitespace(String(value || ""));
  const patterns: Array<{ pattern: RegExp; normalize?: (matched: string) => string }> = [
    { pattern: /\bOp\.?\s*\d+[a-z0-9-]*\b/i, normalize: (matched) => matched.replace(/^op\b\.?/i, "Op.").replace(/\s+/g, " ").trim() },
    { pattern: /\bBWV\s*\d+[a-z0-9-]*\b/i, normalize: (matched) => matched.replace(/\s+/g, " ").trim() },
    { pattern: /\bK(?:V)?\.?\s*\d+[a-z0-9-]*\b/i, normalize: (matched) => matched.replace(/\s+/g, " ").trim() },
    { pattern: /\bWAB\s*\d+[a-z0-9-]*\b/i, normalize: (matched) => matched.replace(/\s+/g, " ").trim() },
    { pattern: /\bGMW\s*\d+[a-z0-9-]*\b/i, normalize: (matched) => matched.replace(/\s+/g, " ").trim() },
    { pattern: /\bHob\.?\s*[A-Z0-9:. -]+\b/i, normalize: (matched) => matched.replace(/\s+/g, " ").trim() },
    { pattern: /\bD\.?\s*\d{1,3}[a-z0-9-]*\b/i, normalize: (matched) => matched.replace(/^d\b\.?/i, "D.").replace(/\s+/g, " ").trim() },
    { pattern: /\bS\.?\s*\d{1,3}[a-z0-9-]*\b/i, normalize: (matched) => matched.replace(/^s\b\.?/i, "S.").replace(/\s+/g, " ").trim() },
    { pattern: /作品\s*\d+[a-z0-9-]*/i, normalize: (matched) => `Op. ${matched.replace(/[^0-9a-z-]+/gi, "")}` },
  ];

  for (const { pattern, normalize } of patterns) {
    const matched = text.match(pattern)?.[0];
    if (matched) {
      return normalize ? normalize(matched) : matched.trim();
    }
  }

  return "";
}

function selectWorkLatinTitle(candidateTitle: string, work: Work) {
  const normalizedCandidate = normalizeWhitespace(candidateTitle);
  if (!normalizedCandidate || !/[A-Za-z]/.test(normalizedCandidate)) {
    return "";
  }
  if (normalizeComparableText(normalizedCandidate) === normalizeComparableText(work.titleLatin || work.title)) {
    return "";
  }
  return normalizedCandidate;
}

function parseChineseOrdinal(value: string) {
  const numerals: Record<string, number> = {
    零: 0,
    〇: 0,
    一: 1,
    二: 2,
    两: 2,
    三: 3,
    四: 4,
    五: 5,
    六: 6,
    七: 7,
    八: 8,
    九: 9,
  };
  let total = 0;
  let current = 0;
  for (const char of String(value || "")) {
    if (char === "十") {
      total += (current || 1) * 10;
      current = 0;
      continue;
    }
    current = numerals[char] ?? current;
  }
  return total + current;
}

function extractWorkHintTokens(work: Work) {
  const sourceText = [work.title, work.titleLatin, ...(work.aliases || [])].filter(Boolean).join(" ");
  const hints = new Set<string>();
  const normalized = normalizeComparableText(sourceText);
  normalized
    .split(" ")
    .filter(Boolean)
    .forEach((token) => hints.add(token));

  const ordinal = sourceText.match(/第([零〇一二两三四五六七八九十百]+)(?:号)?/);
  const arabicNumber = ordinal ? parseChineseOrdinal(ordinal[1]) : 0;
  if (arabicNumber > 0) {
    hints.add(String(arabicNumber));
    hints.add(`no ${arabicNumber}`);
  }
  if (/交响曲/.test(sourceText)) {
    hints.add("symphony");
  }
  if (/协奏曲/.test(sourceText)) {
    hints.add("concerto");
  }
  if (/奏鸣曲/.test(sourceText)) {
    hints.add("sonata");
  }
  if (/小提琴/.test(sourceText)) {
    hints.add("violin");
  }
  if (/钢琴/.test(sourceText)) {
    hints.add("piano");
  }
  if (/大提琴/.test(sourceText)) {
    hints.add("cello");
  }
  if (/歌剧/.test(sourceText)) {
    hints.add("opera");
  }
  if (/序曲/.test(sourceText)) {
    hints.add("overture");
  }
  return [...hints].filter((token) => token && (token.length >= 2 || /^\d+$/.test(token)));
}

function matchesWorkCandidate(work: Work, composer: Composer | undefined, ...values: string[]) {
  const haystack = normalizeComparableText(values.join(" "));
  if (!haystack) {
    return false;
  }
  const workTokens = extractWorkHintTokens(work);
  const composerTokens = [composer?.name || "", composer?.nameLatin || ""]
    .flatMap((value) => normalizeComparableText(value).split(" "))
    .filter((token) => token.length >= 2);
  const workMatched = workTokens.some((token) => haystack.includes(token));
  const composerMatched = composerTokens.length === 0 || composerTokens.some((token) => haystack.includes(token));
  return workMatched && composerMatched;
}

async function fetchWikipediaWorkCandidate(work: Work, library: LibraryData, fetchImpl: typeof fetch) {
  const queries = buildWorkSearchQueries(work, library);
  for (const query of queries) {
    try {
      const searchUrl = `https://en.wikipedia.org/w/api.php?action=query&list=search&format=json&origin=*&srlimit=5&srsearch=${encodeURIComponent(query)}`;
      const searchResponse = await fetchImpl(searchUrl);
      if (!searchResponse.ok) {
        continue;
      }
      const searchPayload = (await searchResponse.json().catch(() => ({}))) as {
        query?: { search?: Array<{ title?: string }> };
      };
      const composer = library.composers.find((item) => item.id === work.composerId);
      for (const searchResult of searchPayload.query?.search || []) {
        const title = normalizeWhitespace(searchResult.title || "");
        if (!title) {
          continue;
        }
        const summaryUrl = `https://en.wikipedia.org/api/rest_v1/page/summary/${encodeURIComponent(title)}`;
        const summaryResponse = await fetchImpl(summaryUrl);
        if (!summaryResponse.ok) {
          continue;
        }
        const summaryPayload = (await summaryResponse.json().catch(() => ({}))) as {
          title?: string;
          description?: string;
          extract?: string;
          content_urls?: { desktop?: { page?: string } };
        };
        const description = stripHtml(summaryPayload.description || "");
        const summary = stripHtml(summaryPayload.extract || "");
        const combinedText = `${summaryPayload.title || ""} ${description} ${summary}`;
        if (/disambiguation|may refer to/i.test(combinedText)) {
          continue;
        }
        if (!matchesWorkCandidate(work, composer, title, description, summary)) {
          continue;
        }
        return {
          sourceUrl: summaryPayload.content_urls?.desktop?.page || summaryUrl,
          sourceLabel: "Wikipedia",
          titleLatin: selectWorkLatinTitle(summaryPayload.title || title, work),
          catalogue: extractStructuredCatalogueFromText(summaryPayload.title || title) || extractStructuredCatalogueFromText(combinedText),
          summary,
          confidence: 0.86,
        };
      }
    } catch {
      continue;
    }
  }
  return null;
}

async function fetchBaiduWorkCandidate(work: Work, library: LibraryData, fetchImpl: typeof fetch) {
  const composer = library.composers.find((item) => item.id === work.composerId);
  for (const query of buildWorkSearchQueries(work, library)) {
    try {
      const candidate = await fetchBaiduSearchSnippetCandidate(query, fetchImpl);
      if (!candidate) {
        continue;
      }
      if (!matchesWorkCandidate(work, composer, candidate.summary, candidate.displayName || "", candidate.displayFullName || "")) {
        continue;
      }
      return {
        sourceUrl: candidate.sourceUrl,
        sourceLabel: candidate.sourceLabel,
        titleLatin: "",
        catalogue:
          extractStructuredCatalogueFromText(candidate.displayFullName || "") ||
          extractStructuredCatalogueFromText(candidate.displayName || "") ||
          extractStructuredCatalogueFromText(candidate.summary),
        summary: normalizeWhitespace(candidate.summary),
        confidence: candidate.confidence ?? 0.58,
      };
    } catch {
      continue;
    }
  }
  return null;
}

function isUsableWorkSummaryCandidate(
  candidate: { sourceLabel?: string; sourceUrl?: string; summary?: string } | null | undefined,
  work: Work,
  composer: Composer | undefined,
) {
  if (!candidate?.summary) {
    return false;
  }
  const summaryText = normalizeWhitespace(candidate.summary);
  if (!summaryText) {
    return false;
  }
  if (candidate.sourceLabel === "Baidu Search" && isBlockedBaiduResultPage(candidate.sourceUrl || "", candidate.sourceLabel || "", summaryText)) {
    return false;
  }
  return matchesWorkCandidate(work, composer, summaryText);
}

function ensureWorkSummaryHasComposerContext(work: Work, composer: Composer | undefined, summary: string) {
  const normalizedSummary = normalizeWhitespace(summary);
  if (!normalizedSummary) {
    return "";
  }
  const composerHints = uniqueStrings([composer?.name, composer?.fullName, composer?.nameLatin]);
  if (composerHints.some((hint) => normalizedSummary.includes(hint))) {
    return normalizedSummary;
  }
  const composerLabel = composer?.name || composer?.nameLatin || "";
  if (!composerLabel) {
    return normalizedSummary;
  }
  const workHints = uniqueStrings([work.title, work.titleLatin, ...work.aliases]);
  if (workHints.some((hint) => normalizedSummary.includes(hint))) {
    return `${composerLabel}：${normalizedSummary}`;
  }
  return `${composerLabel}${work.title}，${normalizedSummary}`;
}

function filterWorks(library: LibraryData, request: AutomationCheckRequest) {
  let works = library.works;

  if (request.workIds?.length) {
    const ids = new Set(request.workIds);
    works = works.filter((work) => ids.has(work.id));
  }

  if (request.composerIds?.length) {
    const composerIds = new Set(request.composerIds);
    works = works.filter((work) => composerIds.has(work.composerId));
  }

  if (request.recordingIds?.length) {
    const workIds = new Set(
      library.recordings.filter((recording) => request.recordingIds?.includes(recording.id)).map((recording) => recording.workId),
    );
    works = works.filter((work) => workIds.has(work.id));
  }

  if (request.conductorIds?.length || request.artistIds?.length || request.orchestraIds?.length) {
    const relatedRecordingWorkIds = new Set(filterRecordings(library, request).map((recording) => recording.workId));
    works = works.filter((work) => relatedRecordingWorkIds.has(work.id));
  }

  return works;
}

async function inspectWorkEnhanced(
  work: Work,
  library: LibraryData,
  fetchImpl: typeof fetch,
  llmConfig?: LlmConfig,
): Promise<AutomationProposal | null> {
  const composer = library.composers.find((item) => item.id === work.composerId);
  const candidates = [await fetchWikipediaWorkCandidate(work, library, fetchImpl), await fetchBaiduWorkCandidate(work, library, fetchImpl)].filter(
    Boolean,
  );
  if (llmConfig && isLlmConfigured(llmConfig)) {
    const llmCandidate = await generateWorkKnowledgeCandidate({
      config: llmConfig,
      title: work.title,
      composerName: composer?.name || "",
      composerLatinName: composer?.nameLatin || "",
      groupPath:
        (work.groupIds || [])
          .map((groupId) => library.workGroups.find((group) => group.id === groupId)?.path || [])
          .sort((left, right) => left.length - right.length)
          .at(-1) || [],
      knownTitleLatin: work.titleLatin,
      knownCatalogue: work.catalogue,
      knownSummary: work.summary,
      knownAliases: work.aliases,
      fetchImpl,
    });
    if (llmCandidate && (llmCandidate.titleLatin || llmCandidate.catalogue || llmCandidate.summary)) {
      candidates.push({
        sourceUrl: llmConfig.baseUrl,
        sourceLabel: "LLM",
        titleLatin: llmCandidate.titleLatin || "",
        catalogue: llmCandidate.catalogue || "",
        summary: llmCandidate.summary || "",
        confidence: llmCandidate.confidence ?? 0.62,
      });
    }
  }
  if (!candidates.length) {
    return null;
  }

  const latinCandidate = candidates.find((candidate) => candidate?.titleLatin);
  const catalogueCandidate = candidates.find((candidate) => candidate?.catalogue);
  const summaryCandidate =
    candidates.find((candidate) => candidate?.sourceLabel !== "LLM" && isUsableWorkSummaryCandidate(candidate, work, composer)) ||
    candidates.find((candidate) => candidate?.sourceLabel === "LLM" && candidate.summary);
  const fields: AutomationProposal["fields"] = [];
  if (!work.titleLatin && latinCandidate?.titleLatin) {
    fields.push({ path: "titleLatin", before: work.titleLatin, after: latinCandidate.titleLatin });
  }
  if (!work.catalogue && catalogueCandidate?.catalogue) {
    fields.push({ path: "catalogue", before: work.catalogue, after: catalogueCandidate.catalogue });
  }
  if (!work.summary && summaryCandidate?.summary) {
    const llmSummary =
      summaryCandidate.sourceLabel !== "LLM" && llmConfig && isLlmConfigured(llmConfig)
        ? await generateConciseChineseSummary({
            config: llmConfig,
            title: work.titleLatin || work.title,
            sourceText: summaryCandidate.summary,
            fetchImpl,
          })
        : "";
    fields.push({
      path: "summary",
      before: work.summary,
      after: ensureWorkSummaryHasComposerContext(work, composer, llmSummary || summaryCandidate.summary),
    });
  }

  if (!fields.length) {
    return null;
  }

  return {
    id: `${work.id}-work-auto`,
    kind: "update",
    entityType: "work",
    entityId: work.id,
    summary: `自动检查：${[composer?.name || composer?.nameLatin || "", work.title, catalogueCandidate?.catalogue || ""].filter(Boolean).join(" / ")}`,
    risk: candidates.every((candidate) => candidate?.sourceLabel === "LLM") ? "medium" : "low",
    status: "pending",
    sources: [...new Set(candidates.map((candidate) => candidate?.sourceUrl || "").filter(Boolean))],
    fields,
    evidence: fields
      .map((field) => {
        const matchedCandidate =
          field.path === "titleLatin"
            ? latinCandidate
            : field.path === "catalogue"
              ? catalogueCandidate
              : summaryCandidate;
        if (!matchedCandidate) {
          return null;
        }
        return {
          field: field.path,
          sourceUrl: matchedCandidate.sourceUrl,
          sourceLabel: matchedCandidate.sourceLabel,
          confidence: matchedCandidate.confidence,
        };
      })
      .filter((item): item is AutomationProposalEvidence => Boolean(item)),
    warnings: [],
  };
}

function selectPeopleByCategory(library: LibraryData, category: AutomationCheckCategory, request: AutomationCheckRequest) {
  if (category === "conductor") {
    const requestedIds = request.conductorIds?.length ? new Set(request.conductorIds) : null;
    return library.people.filter((person) => person.roles.includes("conductor") && (!requestedIds || requestedIds.has(person.id)));
  }

  if (category === "orchestra") {
    const requestedIds = request.orchestraIds?.length ? new Set(request.orchestraIds) : null;
    return library.people.filter((person) => person.roles.includes("orchestra") && (!requestedIds || requestedIds.has(person.id)));
  }

  const requestedIds = request.artistIds?.length ? new Set(request.artistIds) : null;
  return library.people.filter(
    (person) => person.roles.some((role) => artistRoles.includes(role)) && (!requestedIds || requestedIds.has(person.id)),
  );
}

function collectMergeKeys(person: Person) {
  return [
    normalizeName(person.name),
    normalizeName(person.nameLatin),
    ...person.aliases.map(normalizeName),
    ...getEntityAbbreviations(person).map(normalizeName),
  ].filter(Boolean);
}

function buildScopedMergePool(selectedPeople: Person[], mergePool: Person[]) {
  if (!selectedPeople.length) {
    return [];
  }
  const selectedKeys = new Set(selectedPeople.flatMap((person) => collectMergeKeys(person)));
  return mergePool.filter((person) => collectMergeKeys(person).some((key) => selectedKeys.has(key)));
}

function buildMergeProposals(people: Person[]) {
  const keyMap = new Map<string, Person[]>();

  people.forEach((person) => {
    const keys = new Set(collectMergeKeys(person));

    keys.forEach((key) => {
      const bucket = keyMap.get(key) ?? [];
      bucket.push(person);
      keyMap.set(key, bucket);
    });
  });

  const emitted = new Set<string>();
  const proposals: AutomationProposal[] = [];

  for (const [key, bucket] of keyMap.entries()) {
    const unique = [...new Map(bucket.map((item) => [item.id, item])).values()];
    if (!key || unique.length < 2) {
      continue;
    }
    const ids = unique.map((item) => item.id).sort();
    const signature = ids.join("|");
    if (emitted.has(signature)) {
      continue;
    }
    emitted.add(signature);

    proposals.push({
      id: `merge-${signature}`,
      kind: "merge",
      entityType: "person",
      entityId: ids[0],
      summary: `疑似重复人物：${unique.map((item) => item.name).join(" / ")}`,
      risk: "high",
      status: "pending",
      sources: [],
      fields: [],
      warnings: [`Close normalized key: ${key}`],
      mergeCandidates: unique.slice(1).map((item) => ({
        targetId: item.id,
        targetLabel: item.name,
        reason: `与 ${unique[0]?.name} 共享规范化名称或别名 ${key}`,
      })),
    });
  }

  return proposals;
}

function resolveCategories(request: AutomationCheckRequest): AutomationCheckCategory[] {
  const categories = request.categories?.length ? request.categories : request.entityTypes?.length ? request.entityTypes : [];
  return [...new Set(categories)].filter(Boolean) as AutomationCheckCategory[];
}

function filterRecordings(library: LibraryData, request: AutomationCheckRequest) {
  let recordings = library.recordings;

  if (request.recordingIds?.length) {
    const ids = new Set(request.recordingIds);
    recordings = recordings.filter((recording) => ids.has(recording.id));
  }

  if (request.workIds?.length) {
    const ids = new Set(request.workIds);
    recordings = recordings.filter((recording) => ids.has(recording.workId));
  }

  if (request.composerIds?.length) {
    const composerIds = new Set(request.composerIds);
    const allowedWorkIds = new Set(library.works.filter((work) => composerIds.has(work.composerId)).map((work) => work.id));
    recordings = recordings.filter((recording) => allowedWorkIds.has(recording.workId));
  }

  if (request.conductorIds?.length) {
    const ids = new Set(request.conductorIds);
    recordings = recordings.filter((recording) =>
      recording.credits.some((credit) => credit.role === "conductor" && credit.personId && ids.has(credit.personId)),
    );
  }

  if (request.artistIds?.length) {
    const ids = new Set(request.artistIds);
    recordings = recordings.filter((recording) =>
      recording.credits.some((credit) => credit.personId && ids.has(credit.personId) && credit.role !== "conductor"),
    );
  }

  if (request.orchestraIds?.length) {
    const ids = new Set(request.orchestraIds);
    recordings = recordings.filter((recording) =>
      recording.credits.some((credit) => credit.role === "orchestra" && credit.personId && ids.has(credit.personId)),
    );
  }

  return recordings;
}

export async function runAutomationChecks(
  library: LibraryData,
  request: AutomationCheckRequest,
  fetchImpl: typeof fetch = fetch,
  llmConfig?: LlmConfig,
  options?: RunAutomationChecksOptions,
): Promise<AutomationRun> {
  const categories = resolveCategories(request);
  const proposals: AutomationProposal[] = [];
  const notes: string[] = [];
  let provider: AutomationRun["provider"] | undefined;

  if (categories.includes("composer")) {
    const composers = request.composerIds?.length
      ? library.composers.filter((composer) => request.composerIds?.includes(composer.id))
      : library.composers;
    notes.push(`作曲家检查：${composers.length}`);
    for (const composer of composers) {
      const proposal = await inspectNamedEntity(composer, "composer", fetchImpl, llmConfig);
      if (proposal) {
        proposals.push(proposal);
      }
    }
  }

  const personCategories = categories.filter((category) => category === "conductor" || category === "orchestra" || category === "artist");
  if (personCategories.length > 0) {
    const selectedPeople = personCategories.flatMap((category) => selectPeopleByCategory(library, category, request));
    const uniquePeople = [...new Map(selectedPeople.map((person) => [person.id, person])).values()];
    notes.push(`人物检查：${uniquePeople.length}`);

    for (const person of uniquePeople) {
      const proposal = await inspectNamedEntity(person, "person", fetchImpl, llmConfig);
      if (proposal) {
        proposals.push(proposal);
      }
    }

    const mergePool = personCategories.flatMap((category) => selectPeopleByCategory(library, category, {}));
    const scopedMergePool = buildScopedMergePool(
      uniquePeople,
      [...new Map(mergePool.map((person) => [person.id, person])).values()],
    );
    proposals.push(...buildMergeProposals(scopedMergePool));
  }

  if (categories.includes("recording")) {
    const recordings = filterRecordings(library, request);
    notes.push(`版本检查：${recordings.length}`);
    const result = await inspectRecordingsViaProvider(library, recordings, fetchImpl, options);
    proposals.push(...result.proposals);
    provider = result.provider;
    if (result.provider) {
      notes.push(`版本自动检索工具：${result.provider.providerName} / ${result.provider.status}`);
    }
  }

  if (categories.includes("work")) {
    const works = filterWorks(library, request);
    notes.push(`作品检查：${works.length}`);
    for (const work of works) {
      const proposal = await inspectWorkEnhanced(work, library, fetchImpl, llmConfig);
      if (proposal) {
        proposals.push(proposal);
      }
    }
  }

  notes.push(`自动检查来源：Wikipedia / Baidu Baike${isLlmConfigured(llmConfig) ? " / LLM" : ""} / Baidu Search`);
  notes.push(isLlmConfigured(llmConfig) ? "LLM 已启用，并作为仅次于 Wikipedia / Baidu Baike 的候选来源参与字段判定。" : "LLM 未启用，当前为纯规则模式。");

  const normalizedProposals = normalizeAutomationProposals(proposals);
  if (normalizedProposals.length !== proposals.length) {
    notes.push(`自动检查入口已去重：${proposals.length - normalizedProposals.length} 条重复候选已被合并。`);
  }

  return createAutomationRun(library, {
    categories,
    proposals: normalizedProposals,
    notes,
    provider,
  });
}
