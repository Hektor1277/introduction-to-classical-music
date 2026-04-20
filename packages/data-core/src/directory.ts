import { getCountryText, getCountryValues, getDisplayData, getPrimaryCountry, getWebsiteDisplay } from "../../shared/src/display.js";
import type { Composer, Person } from "../../shared/src/schema.js";

export type DirectorySortMode = "surname" | "birth" | "country";

export type DirectoryEntry = {
  id: string;
  href: string;
  shortName: string;
  fullName: string;
  fullNameHighlight: string;
  nameLatin: string;
  nameLatinHighlight: string;
  originalName: string;
  originalNameHighlight: string;
  summary: string;
  representativeWorks: string[];
  birthYear?: number;
  deathYear?: number;
  countryLabel: string;
  countrySortKey: string;
  countrySectionTitles: string[];
  surnameSortKey: string;
  quickKeys: {
    surname: string;
    birth: string;
    country: string;
  };
  avatarLabel: string;
  avatarSrc?: string;
};

export type DirectoryDisplayEntry = DirectoryEntry & {
  summaryExcerpt: string;
  representativeWorksLabel: string;
};

export type DirectorySection = {
  id: string;
  title: string;
  items: DirectoryEntry[];
};

export type DirectoryRailItem = {
  label: string;
  targetId: string;
};

export type DirectorySectionSet = {
  rail: DirectoryRailItem[];
  sections: DirectorySection[];
};

const particles = new Set(["van", "von", "de", "del", "da", "di", "du", "la", "le"]);

function dedupe<T>(items: T[]) {
  return [...new Set(items)];
}

function splitNameLatinVariants(value: string) {
  return String(value ?? "")
    .trim()
    .replace(/,\s*EN\s*:/gi, "|")
    .replace(/\bEN\s*:/gi, "|")
    .split("|")
    .map((item) => item.trim())
    .filter(Boolean);
}

function hasLatinScript(value: string) {
  return /[A-Za-z]/.test(value);
}

function tokenizeLatinName(value: string) {
  return String(value ?? "")
    .split(/[\s·・•‧]+/)
    .map((token) => token.trim())
    .filter(Boolean);
}

function normalizeAlphabetSectionKey(value: string) {
  const normalized = String(value ?? "").trim().normalize("NFKD");
  const latinMatch = normalized.match(/[A-Za-z]/);
  if (latinMatch?.[0]) {
    return latinMatch[0].toUpperCase();
  }
  return (normalized.charAt(0) || "#").toUpperCase();
}

function normalizeSummary(value: string) {
  return String(value || "")
    .replace(/\s+/g, " ")
    .replace(/[—–－]/g, "-")
    .replace(/[（]/g, "(")
    .replace(/[）]/g, ")")
    .trim();
}

function extractLifeSpan(text: string, birthYear?: number, deathYear?: number) {
  if (birthYear || deathYear) {
    return { birthYear, deathYear };
  }

  const normalizedText = normalizeSummary(text);
  const leadingText = normalizedText.split(/[。！？!?;]/, 1)[0] ?? normalizedText;
  const leadingWindow = normalizedText.slice(0, 120);
  const firstParenthetical = leadingWindow.match(/\(([^()]*)\)/)?.[1] ?? "";
  const parentheticalMatch = firstParenthetical.match(/(?:^|[，,]\s*)(1[6-9]\d{2}|20\d{2})[^()]{0,48}?-(1[6-9]\d{2}|20\d{2})/);
  if (parentheticalMatch) {
    return {
      birthYear: Number(parentheticalMatch[1]),
      deathYear: Number(parentheticalMatch[2]),
    };
  }

  const leadingMatch =
    leadingText.match(/^(1[6-9]\d{2}|20\d{2})年[^。！？!?;]{0,48}?-(1[6-9]\d{2}|20\d{2})年?/) ??
    leadingText.match(/^\b(1[6-9]\d{2}|20\d{2})\b[^.();]{0,24}?-\s*\b(1[6-9]\d{2}|20\d{2})\b/);
  if (leadingMatch) {
    return {
      birthYear: Number(leadingMatch[1]),
      deathYear: Number(leadingMatch[2]),
    };
  }

  const leadingBirthDeathMatch =
    leadingText.match(/^(1[6-9]\d{2}|20\d{2})年[^。！？!?;]{0,48}?(1[6-9]\d{2}|20\d{2})年[^。！？!?;]{0,16}(?:卒于|逝世|去世|病逝|去世于)/) ??
    leadingText.match(/^(1[6-9]\d{2}|20\d{2})[^.?!;]{0,48}?(1[6-9]\d{2}|20\d{2})[^.?!;]{0,16}(?:died|passed away)/i);
  if (leadingBirthDeathMatch) {
    return {
      birthYear: Number(leadingBirthDeathMatch[1]),
      deathYear: Number(leadingBirthDeathMatch[2]),
    };
  }

  return { birthYear, deathYear };
}

function extractCountry(entity: Pick<DirectorySource, "country" | "countries">) {
  const primaryCountry = getPrimaryCountry(entity);
  if (primaryCountry) {
    return {
      label: getCountryText(entity),
      sortKey: primaryCountry,
    };
  }

  return {
    label: "Unknown",
    sortKey: "ZZZ Unknown",
  };
}

function buildCountrySectionTitles(entity: Pick<DirectorySource, "country" | "countries">) {
  const values = getCountryValues(entity);
  return values.length ? values : ["Unknown"];
}

function extractLatinAndOriginal(nameLatin: string) {
  const variants = splitNameLatinVariants(nameLatin);
  const latin = variants.find((value) => hasLatinScript(value)) || variants[0] || "";
  const original = variants.find((value) => value !== latin) || "";
  return { latin, original };
}

function extractFullName(shortName: string, aliases: string[], summary: string, explicitFullName = "") {
  if (explicitFullName.trim()) {
    return explicitFullName.trim();
  }

  const alias = aliases.find((value) => value.includes(shortName)) ?? aliases[0];
  if (alias) {
    return alias.trim();
  }

  const firstSentence = summary
    .split(/[。!?]/)
    .map((value) => value.trim())
    .find(Boolean);

  if (firstSentence && firstSentence.includes(shortName)) {
    return firstSentence;
  }

  return shortName;
}

function extractLatinHighlight(latinName: string, fallback = "") {
  const tokens = tokenizeLatinName(latinName);
  for (let index = tokens.length - 1; index >= 0; index -= 1) {
    const token = tokens[index].replace(/[.,]/g, "");
    if (!particles.has(token.toLowerCase())) {
      return token;
    }
  }
  return fallback;
}

function createAvatarLabel(fullName: string, shortName: string) {
  const latinWords = fullName.split(/\s+/).filter((word) => /[A-Za-z]/.test(word));
  if (latinWords.length >= 2) {
    return `${latinWords[0][0]}${latinWords[latinWords.length - 1][0]}`.toUpperCase();
  }

  return shortName.replace(/[^\p{L}\p{N}]/gu, "").slice(0, 2).toUpperCase();
}

function createSurnameSortKey(latinName: string, fallbackName: string) {
  const tokens = tokenizeLatinName(latinName);
  for (let index = tokens.length - 1; index >= 0; index -= 1) {
    const token = tokens[index].replace(/[.,]/g, "");
    if (!particles.has(token.toLowerCase())) {
      return token;
    }
  }

  return fallbackName;
}

function createBirthKey(birthYear?: number) {
  if (!birthYear) {
    return "Unknown";
  }

  const decade = Math.floor(birthYear / 10) * 10;
  return `${decade}s`;
}

function clampText(value: string, maxLength: number) {
  const normalized = value.trim();
  if (!normalized) {
    return "";
  }

  return normalized.length > maxLength ? `${normalized.slice(0, maxLength).trim()}...` : normalized;
}

export function createDirectoryDisplayEntry(entry: DirectoryEntry): DirectoryDisplayEntry {
  const representativeWorks = entry.representativeWorks.slice(0, 3);
  return {
    ...entry,
    representativeWorks,
    summaryExcerpt: clampText(entry.summary, 60),
    representativeWorksLabel: representativeWorks.join(" / "),
  };
}

type DirectorySource = Pick<
  Person,
  | "id"
  | "slug"
  | "name"
  | "fullName"
  | "nameLatin"
  | "displayName"
  | "displayFullName"
  | "displayLatinName"
  | "aliases"
  | "abbreviations"
  | "summary"
  | "birthYear"
  | "deathYear"
  | "country"
  | "countries"
  | "avatarSrc"
> &
  Pick<
    Composer,
    | "id"
    | "slug"
    | "name"
    | "fullName"
    | "nameLatin"
    | "displayName"
    | "displayFullName"
    | "displayLatinName"
    | "aliases"
    | "abbreviations"
    | "summary"
    | "birthYear"
    | "deathYear"
    | "country"
    | "countries"
    | "avatarSrc"
  >;

function buildDirectoryEntry(
  entity: DirectorySource,
  options: {
    href: string;
    representativeWorks: string[];
  },
): DirectoryEntry {
  const display = getDisplayData(entity);
  const websiteDisplay = getWebsiteDisplay(entity);
  const { latin, original } = extractLatinAndOriginal(display.latin);
  const fullName = extractFullName(websiteDisplay.heading, entity.aliases ?? [], entity.summary ?? "", websiteDisplay.heading);
  const commonName = websiteDisplay.short || display.primary || websiteDisplay.heading;
  const fullNameHighlight = fullName.includes(commonName) ? commonName : "";
  const nameLatinHighlight = extractLatinHighlight(latin || display.latin || "", commonName);
  const originalNameHighlight = extractLatinHighlight(original, commonName);
  const { birthYear, deathYear } = extractLifeSpan(entity.summary ?? "", entity.birthYear, entity.deathYear);
  const country = extractCountry(entity);
  const surnameSortKey = createSurnameSortKey(latin || display.latin || "", websiteDisplay.heading);

  return {
    id: entity.id,
    href: options.href,
    shortName: websiteDisplay.heading,
    fullName: websiteDisplay.short,
    fullNameHighlight,
    nameLatin: latin || display.latin || "",
    nameLatinHighlight,
    originalName: original,
    originalNameHighlight,
    summary: entity.summary ?? "",
    representativeWorks: dedupe(options.representativeWorks).slice(0, 4),
    birthYear,
    deathYear,
    countryLabel: country.label,
    countrySortKey: country.sortKey,
    countrySectionTitles: buildCountrySectionTitles(entity),
    surnameSortKey,
    quickKeys: {
      surname: normalizeAlphabetSectionKey(surnameSortKey),
      birth: createBirthKey(birthYear),
      country: (country.sortKey.charAt(0) || "#").toUpperCase(),
    },
    avatarLabel: createAvatarLabel(fullName, websiteDisplay.short || websiteDisplay.heading),
    avatarSrc: entity.avatarSrc?.trim() || undefined,
  };
}

export function buildPersonDirectoryEntry(
  person: Pick<
    Person,
    | "id"
    | "slug"
    | "name"
    | "fullName"
    | "nameLatin"
    | "displayName"
    | "displayFullName"
    | "displayLatinName"
    | "aliases"
    | "abbreviations"
    | "summary"
    | "birthYear"
    | "deathYear"
    | "country"
    | "countries"
    | "avatarSrc"
  >,
  options: {
    href: string;
    representativeWorks: string[];
  },
): DirectoryEntry {
  return buildDirectoryEntry(person, options);
}

export function buildComposerDirectoryEntry(
  composer: Pick<
    Composer,
    | "id"
    | "slug"
    | "name"
    | "fullName"
    | "nameLatin"
    | "displayName"
    | "displayFullName"
    | "displayLatinName"
    | "aliases"
    | "abbreviations"
    | "summary"
    | "birthYear"
    | "deathYear"
    | "country"
    | "countries"
    | "avatarSrc"
  >,
  options: {
    href: string;
    representativeWorks: string[];
  },
): DirectoryEntry {
  return buildDirectoryEntry(composer, options);
}

function createSectionId(mode: DirectorySortMode, value: string) {
  if (mode !== "country") {
    return value;
  }

  const slug = value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");

  return `country-${slug || "unknown"}`;
}

export function buildDirectorySections(entries: DirectoryEntry[], mode: DirectorySortMode): DirectorySectionSet {
  const sortedEntries = [...entries].sort((left, right) => {
    if (mode === "surname") {
      return left.surnameSortKey.localeCompare(right.surnameSortKey, "en");
    }
    if (mode === "birth") {
      return (left.birthYear ?? Number.MAX_SAFE_INTEGER) - (right.birthYear ?? Number.MAX_SAFE_INTEGER);
    }

    return (
      left.countrySortKey.localeCompare(right.countrySortKey, "en") ||
      left.surnameSortKey.localeCompare(right.surnameSortKey, "en")
    );
  });

  if (mode === "country") {
    const grouped = new Map<string, DirectorySection>();
    for (const entry of sortedEntries) {
      for (const title of entry.countrySectionTitles) {
        const sectionId = createSectionId(mode, title);
        if (!grouped.has(sectionId)) {
          grouped.set(sectionId, {
            id: sectionId,
            title,
            items: [],
          });
        }
        grouped.get(sectionId)?.items.push(entry);
      }
    }

    const sections = [...grouped.values()].sort((left, right) => left.title.localeCompare(right.title, "en"));
    const railMap = new Map<string, string>();
    for (const section of sections) {
      const label = (section.title.charAt(0) || "#").toUpperCase();
      if (!railMap.has(label)) {
        railMap.set(label, section.id);
      }
    }

    return {
      rail: [...railMap.entries()].map(([label, targetId]) => ({ label, targetId })),
      sections,
    };
  }

  const grouped = new Map<string, DirectorySection>();
  for (const entry of sortedEntries) {
    const sectionId =
      mode === "surname"
        ? entry.quickKeys.surname
        : mode === "birth"
          ? entry.quickKeys.birth
          : entry.quickKeys.country;

    if (!grouped.has(sectionId)) {
      grouped.set(sectionId, {
        id: sectionId,
        title: sectionId,
        items: [],
      });
    }

    grouped.get(sectionId)?.items.push(entry);
  }

  const sections = [...grouped.values()];
  return {
    rail: sections.map((section) => ({ label: section.id, targetId: section.id })),
    sections,
  };
}
