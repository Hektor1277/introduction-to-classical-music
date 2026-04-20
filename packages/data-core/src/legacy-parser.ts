import { load } from "cheerio";

import { detectPlatformFromUrl, normalizeResourceLink } from "./resource-links.js";
import type { Credit, RecordingImage, ResourceLink } from "../../shared/src/schema.js";

type ParsedLegacyRecording = {
  links: ResourceLink[];
  credits: Credit[];
  images: RecordingImage[];
  performanceDateText: string;
  venueText: string;
  albumTitle: string;
  label: string;
  releaseDate: string;
};

type ParsedLegacyPath = {
  composerName: string;
  groupPath: string[];
  workName: string;
  recordingFileName: string;
};

function normalizeWhitespace(value: string) {
  return value.replace(/\s+/g, " ").replace(/\u00a0/g, " ").trim();
}

function isPlaceholderCreditValue(value: string) {
  const normalized = normalizeWhitespace(value).toLowerCase();
  return !normalized || normalized === "-" || normalized === "unknown" || normalized === "未知";
}

function hasAnyKeyword(value: string, keywords: string[]) {
  return keywords.some((keyword) => value.includes(keyword));
}

function normalizeAsciiLabel(value: string) {
  return value.toLowerCase().replace(/[^a-z]+/g, " ").trim();
}

function matchesEnglishLabel(label: string, keywords: string[]) {
  const normalized = normalizeAsciiLabel(label);
  if (!normalized) {
    return false;
  }
  return keywords.some((keyword) => normalized === normalizeAsciiLabel(keyword));
}

function roleFromLabel(label: string): Credit["role"] {
  if (hasAnyKeyword(label, ["指挥", "执棒"]) || matchesEnglishLabel(label, ["conductor", "dirigent"])) {
    return "conductor";
  }
  if (
    hasAnyKeyword(label, ["乐团", "乐队", "管弦乐团", "管弦乐队", "交响乐团", "交响乐队"]) ||
    matchesEnglishLabel(label, ["orchestra", "philharmonic orchestra", "symphony orchestra", "philharmonic"])
  ) {
    return "orchestra";
  }
  if (hasAnyKeyword(label, ["合唱"]) || matchesEnglishLabel(label, ["chorus", "choir"])) {
    return "chorus";
  }
  if (
    hasAnyKeyword(label, ["女高音", "女中音", "次女高音", "男高音", "男中音", "男低音", "歌手", "主演"]) ||
    matchesEnglishLabel(label, ["soprano", "tenor", "baritone", "bass", "mezzo soprano", "singer", "vocal"])
  ) {
    return "singer";
  }
  if (hasAnyKeyword(label, ["组合", "四重奏", "三重奏"]) || matchesEnglishLabel(label, ["ensemble", "quartet", "trio"])) {
    return "ensemble";
  }
  return "soloist";
}

function splitDateAndVenue(raw: string) {
  const text = normalizeWhitespace(raw);
  const separators = ["，", ","];

  for (const separator of separators) {
    const separatorIndex = text.lastIndexOf(separator);
    if (separatorIndex <= 0) {
      continue;
    }

    const left = text.slice(0, separatorIndex).trim();
    const right = text.slice(separatorIndex + separator.length).trim();
    if (right && !/^\d{4}/.test(right)) {
      return {
        performanceDateText: left,
        venueText: right,
      };
    }
  }

  return {
    performanceDateText: text,
    venueText: "",
  };
}

export function parseLegacyWorkPath(value: string): ParsedLegacyPath {
  const normalized = value.replace(/\\/g, "/");
  const segments = normalized.split("/").filter(Boolean);

  if (segments.length < 5) {
    throw new Error(`Unsupported legacy path: ${value}`);
  }

  const recordingSegment = segments.at(-1) ?? "";
  const workName = segments.at(-2) ?? "";
  const groupPath = segments.slice(2, -2);

  return {
    composerName: segments[1] ?? "",
    groupPath,
    workName,
    recordingFileName: recordingSegment.replace(/\.htm$/i, ""),
  };
}

export function parseLegacyRecordingHtml(html: string): ParsedLegacyRecording {
  const $ = load(html);
  const links: ResourceLink[] = [];
  const credits: Credit[] = [];
  const images: RecordingImage[] = [];

  $("a").each((_, element) => {
    const href = $(element).attr("href")?.trim();
    if (!href || !/^https?:\/\//.test(href)) {
      return;
    }

    links.push(
      normalizeResourceLink({
        platform: detectPlatformFromUrl(href),
        url: href,
        localPath: "",
        title: normalizeWhitespace($(element).text()),
        linkType: "external",
        visibility: "public",
      }),
    );
  });

  $("img").each((_, element) => {
    const src = $(element).attr("src")?.trim();
    if (!src) {
      return;
    }

    images.push({
      src,
      alt: normalizeWhitespace($(element).attr("alt") ?? ""),
      kind: "other",
    });
  });

  let performanceDateText = "";
  let venueText = "";
  let albumTitle = "";
  let label = "";
  let releaseDate = "";

  $("p").each((_, element) => {
    const text = normalizeWhitespace($(element).text());
    if (!text) {
      return;
    }

    const match = text.match(/^([^：:]+)[：:]\s*(.+)$/);
    if (!match) {
      return;
    }

    const [, rawLabel, rawValue] = match;
    const labelText = normalizeWhitespace(rawLabel);
    const valueText = normalizeWhitespace(rawValue);

    if (!valueText) {
      return;
    }

    if (hasAnyKeyword(labelText, ["时间", "地点"]) || matchesEnglishLabel(labelText, ["date", "time", "venue", "location"])) {
      const split = splitDateAndVenue(valueText);
      performanceDateText = split.performanceDateText;
      venueText = split.venueText;
      return;
    }

    if (hasAnyKeyword(labelText, ["专辑", "唱片", "Album"]) || matchesEnglishLabel(labelText, ["album"])) {
      albumTitle = valueText;
      return;
    }

    if (
      hasAnyKeyword(labelText, ["发行商", "厂牌", "唱片公司", "Label"]) ||
      matchesEnglishLabel(labelText, ["label", "publisher"])
    ) {
      label = valueText;
      return;
    }

    if (hasAnyKeyword(labelText, ["发行日期", "出版日期", "Date"]) || matchesEnglishLabel(labelText, ["release", "release date", "date"])) {
      releaseDate = valueText;
      return;
    }

    if (
      hasAnyKeyword(labelText, [
        "乐团",
        "乐队",
        "管弦乐团",
        "管弦乐队",
        "交响乐团",
        "交响乐队",
        "指挥",
        "独奏",
        "钢琴",
        "小提琴",
        "中提琴",
        "大提琴",
        "歌手",
        "组合",
        "合唱",
        "主演",
        "女高音",
        "女中音",
        "次女高音",
        "男高音",
        "男中音",
        "男低音",
        "ensemble",
        "chorus",
      ]) ||
      matchesEnglishLabel(labelText, [
        "orchestra",
        "conductor",
        "soloist",
        "violin",
        "piano",
        "cello",
        "soprano",
        "tenor",
        "baritone",
        "bass",
        "ensemble",
        "chorus",
        "choir",
      ])
    ) {
      const localLink = $(element).find("a").first();
      const displayName = normalizeWhitespace(localLink.text() || valueText);
      if (isPlaceholderCreditValue(displayName)) {
        return;
      }
      credits.push({
        role: roleFromLabel(labelText),
        personId: "",
        displayName,
        label: labelText,
      });
    }
  });

  return {
    links,
    credits,
    images,
    performanceDateText,
    venueText,
    albumTitle,
    label,
    releaseDate,
  };
}
