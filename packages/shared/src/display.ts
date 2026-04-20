import {
  deriveRecordingPresentationFamily,
  normalizeRecordingWorkTypeHintValue,
} from "./recording-rules.js";
import type { Credit, LibraryData, Person, PersonRole, Recording } from "./schema.js";

type NamedEntity = {
  id: string;
  name: string;
  fullName?: string;
  nameLatin?: string;
  displayName?: string;
  displayFullName?: string;
  displayLatinName?: string;
  aliases?: string[];
  abbreviations?: string[];
  country?: string;
  countries?: string[];
  birthYear?: number;
  deathYear?: number;
  summary?: string;
};

export type NormalizedDisplay = {
  primary: string;
  full: string;
  latin: string;
  aliases: string[];
  abbreviations: string[];
};

export type WebsiteDisplay = {
  heading: string;
  short: string;
  latin: string;
};

export type RecordingListEntryDisplay = {
  title: string;
  secondaryText: string;
  metaText: string;
  noteExcerpt: string;
};

export type RecordingDailyDisplay = {
  title: string;
  subtitle: string;
  workPrimary: string;
  workSecondary: string;
  composerPrimary: string;
  composerSecondary: string;
  principalPrimary: string;
  principalSecondary: string;
  supportingPrimary: string;
  supportingSecondary: string;
  ensemblePrimary: string;
  ensembleSecondary: string;
  datePlacePrimary: string;
  datePlaceSecondary: string;
};

export type RecordingDisplayModel = {
  title: string;
  subtitle: string;
  secondaryText: string;
  metaText: string;
  noteExcerpt: string;
  daily: RecordingDailyDisplay;
};

export type LibraryDataIssueCategory =
  | "name-normalization"
  | "year-conflict"
  | "country-missing"
  | "abbreviation-missing"
  | "summary-missing"
  | "relation-missing"
  | "relation-mismatch";

export type LibraryDataIssue = {
  entityType: "composer" | "person" | "work" | "recording";
  entityId: string;
  category: LibraryDataIssueCategory;
  message: string;
};

const shortTitleDelimiter = " - ";
const secondaryDelimiter = " / ";
const metaDelimiter = " 路 ";

function compact(value: unknown) {
  return String(value ?? "").trim();
}

function hasGarbledSummaryText(value: string) {
  return /\?{3,}|\uFFFD/u.test(value);
}

function hasPlaceholderSummaryText(value: string) {
  const normalized = compact(value);
  return normalized === "暂无简介" || normalized === "暂无简介。";
}

function isMissingSummaryValue(value: unknown) {
  const normalized = compact(value);
  return !normalized || hasPlaceholderSummaryText(normalized) || hasGarbledSummaryText(normalized);
}

function dedupe(values: Array<string | undefined>) {
  return [...new Set(values.map((value) => compact(value ?? "")).filter(Boolean))];
}

export function getCountryValues(entity: Pick<NamedEntity, "country" | "countries">) {
  return dedupe([...(entity.countries ?? []), entity.country]);
}

export function getPrimaryCountry(entity: Pick<NamedEntity, "country" | "countries">) {
  return getCountryValues(entity)[0] || "";
}

export function getCountryText(entity: Pick<NamedEntity, "country" | "countries">) {
  return getCountryValues(entity).join(secondaryDelimiter);
}

function looksLikeChineseText(value: string) {
  return /[\u3400-\u9fff]/.test(value);
}

function looksLikeAbbreviation(value: string) {
  return /^[A-Z0-9][A-Z0-9 .&/-]{1,11}$/.test(value.trim());
}

function derivePrimaryName(entity: NamedEntity) {
  const explicit = compact(entity.displayName);
  if (explicit) {
    return explicit;
  }
  const canonical = compact(entity.name);
  const aliases = dedupe([...(entity.aliases ?? [])]);
  const shorterChineseAlias =
    aliases.find((value) => looksLikeChineseText(value) && compact(value).length < canonical.length) ||
    aliases.find((value) => looksLikeChineseText(value) && compact(value) !== canonical);
  return compact(shorterChineseAlias || canonical);
}

function deriveChineseFullName(entity: NamedEntity, primary: string) {
  const explicit = compact(entity.displayFullName || entity.fullName || "");
  if (explicit) {
    return explicit;
  }

  const canonical = compact(entity.name);
  if (looksLikeChineseText(canonical)) {
    return canonical;
  }

  const aliases = dedupe([...(entity.aliases ?? []), entity.name, entity.displayName]);
  const aliasMatch =
    aliases.find((value) => looksLikeChineseText(value) && compact(value).length > compact(primary).length && compact(value).includes(primary)) ||
    aliases.find((value) => looksLikeChineseText(value) && compact(value).length > compact(primary).length);

  return compact(aliasMatch || canonical || primary);
}

function deriveShortLatinName(entity: NamedEntity) {
  const explicit = compact(entity.displayLatinName);
  if (explicit) {
    return explicit;
  }
  const latin = compact(entity.nameLatin);
  if (!latin) {
    return "";
  }
  const tokens = latin
    .split(/\s+/)
    .map((token) => token.trim())
    .filter(Boolean)
    .filter((token) => !["von", "van", "de", "del", "der", "di", "da", "la", "le"].includes(token.toLowerCase()));
  return compact(tokens[tokens.length - 1] || latin);
}

export function normalizeSearchText(value: string) {
  return String(value ?? "")
    .normalize("NFKC")
    .toLowerCase()
    .replace(/[·•・・,.;:'"`()（）\[\]\-_/\\]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function excerpt(value: string, maxLength: number) {
  const normalized = compact(value);
  if (!normalized) {
    return "";
  }
  return normalized.length > maxLength ? `${normalized.slice(0, maxLength).trim()}…` : normalized;
}

export function getDisplayData(entity: NamedEntity): NormalizedDisplay {
  const primary = derivePrimaryName(entity);
  const full = deriveChineseFullName(entity, primary);
  const latin = compact(entity.displayLatinName || entity.nameLatin);
  const abbreviations = dedupe([
    ...(entity.abbreviations ?? []),
    ...(entity.aliases ?? []).filter((value) => looksLikeAbbreviation(value)),
  ]);

  return {
    primary,
    full,
    latin,
    aliases: dedupe([...(entity.aliases ?? []), entity.name, entity.fullName, entity.nameLatin]),
    abbreviations,
  };
}

function hasCanonicalDisplayNames(entity: NamedEntity) {
  const display = getDisplayData(entity);
  return Boolean(compact(display.full) && compact(display.latin));
}

function hasDistinctChineseShortAndFullNames(entity: NamedEntity) {
  const display = getDisplayData(entity);
  const primary = compact(display.primary);
  const full = compact(display.full);
  if (!primary || !full || primary !== full) {
    return true;
  }
  return primary.length > 3;
}

function creditRoleCompatibleWithPerson(role: Credit["role"], person: Person) {
  const personRoles = new Set(person.roles || []);
  if (role === "orchestra") {
    return personRoles.has("orchestra") || personRoles.has("ensemble") || personRoles.has("chorus");
  }
  if (role === "ensemble") {
    return personRoles.has("ensemble") || personRoles.has("orchestra") || personRoles.has("chorus");
  }
  if (role === "chorus") {
    return personRoles.has("chorus") || personRoles.has("ensemble");
  }
  if (role === "conductor") {
    return personRoles.has("conductor");
  }
  if (role === "singer") {
    return personRoles.has("singer");
  }
  if (role === "soloist" || role === "instrumentalist") {
    return personRoles.has("soloist") || personRoles.has("instrumentalist");
  }
  return true;
}

export function getEntitySearchTexts(entity: NamedEntity) {
  const display = getDisplayData(entity);
  const primaryText = display.full || display.primary;
  const secondaryParts = dedupe([
    display.primary && display.primary !== primaryText ? display.primary : "",
    display.latin,
    ...(display.abbreviations ?? []),
  ]);
  return {
    primaryText,
    secondaryText: secondaryParts.join(secondaryDelimiter),
    matchTokens: dedupe([
      display.primary,
      display.full,
      display.latin,
      ...(display.aliases ?? []),
      ...(display.abbreviations ?? []),
      ...getCountryValues(entity),
    ]),
    aliasTokens: dedupe([...(display.aliases ?? []), ...(display.abbreviations ?? [])]),
  };
}

export function getWebsiteDisplay(entity: NamedEntity): WebsiteDisplay {
  const display = getDisplayData(entity);
  const heading = display.full || display.primary;
  const short = display.primary !== heading ? display.primary : "";
  return {
    heading,
    short,
    latin: display.latin,
  };
}

function getPersonById(library: LibraryData, personId?: string) {
  if (!personId) {
    return undefined;
  }
  return library.people.find((person) => person.id === personId);
}

function displayForCredit(credit: Credit, library: LibraryData) {
  const person = getPersonById(library, credit.personId);
  const fallback = compact(credit.displayName);
  const fallbackKey = credit.personId || normalizeSearchText(fallback);
  if (!person) {
    return {
      key: fallbackKey,
      role: credit.role,
      personId: compact(credit.personId),
      primary: fallback,
      fullLabel: fallback,
      secondary: fallback,
      shortLabel: fallback,
    };
  }

  const display = getDisplayData(person);
  const fullLabel =
    credit.role === "orchestra" || credit.role === "ensemble" || credit.role === "chorus"
      ? display.full || display.primary
      : display.primary;
  const shortLabel =
    credit.role === "orchestra" || credit.role === "ensemble" || credit.role === "chorus"
      ? display.abbreviations[0] || display.primary
      : display.primary;
  const secondary =
    credit.role === "orchestra" || credit.role === "ensemble" || credit.role === "chorus"
      ? display.latin || display.full || display.primary
      : deriveShortLatinName(person) || display.latin || display.full || display.primary;

  return {
    key: compact(person.id) || fallbackKey,
    role: credit.role,
    personId: compact(person.id),
    primary: display.primary,
    fullLabel,
    secondary,
    shortLabel,
  };
}

function creditGroup(credits: Credit[], roles: PersonRole[]) {
  return credits.filter((credit) => roles.includes(credit.role));
}

function joinRecordingTitleParts(parts: string[]) {
  return dedupe(parts.map((value) => compact(value))).join(shortTitleDelimiter);
}

function joinCreditNames(items: Array<{ primary: string; fullLabel: string }>, mode: "primary" | "fullLabel") {
  return dedupe(items.map((item) => item[mode])).join(secondaryDelimiter);
}

type RecordingCreditDisplay = ReturnType<typeof displayForCredit>;

function uniqueCreditDisplays(items: RecordingCreditDisplay[]) {
  const seen = new Set<string>();
  const nextItems: RecordingCreditDisplay[] = [];
  for (const item of items) {
    if (!item.key || seen.has(item.key)) {
      continue;
    }
    seen.add(item.key);
    nextItems.push(item);
  }
  return nextItems;
}

function excludeCreditDisplays(items: RecordingCreditDisplay[], blockedKeys: Set<string>) {
  return items.filter((item) => item.key && !blockedKeys.has(item.key));
}

function collectCreditDisplays(recording: Recording, library: LibraryData) {
  const grouped = {
    conductors: uniqueCreditDisplays(creditGroup(recording.credits, ["conductor"]).map((credit) => displayForCredit(credit, library))),
    orchestras: uniqueCreditDisplays(creditGroup(recording.credits, ["orchestra"]).map((credit) => displayForCredit(credit, library))),
    groups: uniqueCreditDisplays(creditGroup(recording.credits, ["ensemble", "chorus"]).map((credit) => displayForCredit(credit, library))),
    soloists: uniqueCreditDisplays(creditGroup(recording.credits, ["soloist", "instrumentalist"]).map((credit) => displayForCredit(credit, library))),
    singers: uniqueCreditDisplays(creditGroup(recording.credits, ["singer"]).map((credit) => displayForCredit(credit, library))),
  };
  const conductorKeys = new Set(grouped.conductors.map((item) => item.key).filter(Boolean));
  grouped.soloists = excludeCreditDisplays(grouped.soloists, conductorKeys);
  grouped.singers = excludeCreditDisplays(grouped.singers, conductorKeys);
  return grouped;
}

function joinDisplayValues(items: RecordingCreditDisplay[], key: "primary" | "fullLabel" | "secondary" | "shortLabel") {
  return dedupe(items.map((item) => compact(item[key]))).join(shortTitleDelimiter);
}

function buildRecordingDatePlace(recording: Recording) {
  return {
    primary: compact(recording.performanceDateText),
    secondary: compact(recording.venueText),
    combined: dedupe([recording.performanceDateText, recording.venueText]).join(metaDelimiter),
  };
}

function buildBrokenRecordingWorkLines(recording: Recording, library: LibraryData) {
  return buildRecordingWorkLines(recording, library);
  /*
  const work = library.works.find((item) => item.id === recording.workId);
  const composer = work ? library.composers.find((item) => item.id === work.composerId) : undefined;
  if (work) {
    return {
      primary: [work.title, composer ? getWebsiteDisplay(composer).heading : "未知作曲家"].filter(Boolean).join(" / "),
      secondary: dedupe([work.titleLatin, work.catalogue, composer ? getDisplayData(composer).secondary : ""]).join(secondaryDelimiter),
    };
  }
  return {
    primary: work ? `${composer ? getWebsiteDisplay(composer).heading : "未知作曲家"} / ${work.title}` : "",
    secondary: dedupe([work?.titleLatin, work?.catalogue]).join(secondaryDelimiter),
  };
}

*/
}
function buildLegacyRecordingWorkLines(recording: Recording, library: LibraryData) {
  const work = library.works.find((item) => item.id === recording.workId);
  const composer = work ? library.composers.find((item) => item.id === work.composerId) : undefined;

  if (!work) {
    return {
      primary: "",
      secondary: "",
      composerPrimary: "",
      composerSecondary: "",
    };
  }

  const composerDisplay = composer ? getWebsiteDisplay(composer) : null;
  return {
    primary: [work.title, composerDisplay?.heading || "未知作曲家"].filter(Boolean).join(" / "),
    secondary: dedupe([work.titleLatin, work.catalogue, composerDisplay?.latin || ""]).join(secondaryDelimiter),
  };
}

function buildRecordingWorkLines(recording: Recording, library: LibraryData) {
  const work = library.works.find((item) => item.id === recording.workId);
  const composer = work ? library.composers.find((item) => item.id === work.composerId) : undefined;

  if (!work) {
    return {
      primary: "",
      secondary: "",
      composerPrimary: "",
      composerSecondary: "",
    };
  }

  const composerDisplay = composer ? getWebsiteDisplay(composer) : null;

  return {
    primary: work.title,
    secondary: dedupe([work.titleLatin, work.catalogue]).join(" | "),
    composerPrimary: composerDisplay?.heading || "\u672a\u77e5\u4f5c\u66f2\u5bb6",
    composerSecondary: composerDisplay?.latin || "",
  };
}

function buildDailyDisplay(
  title: string,
  subtitle: string,
  workPrimary: string,
  workSecondary: string,
  composerPrimary: string,
  composerSecondary: string,
  principalPrimary: string,
  principalSecondary: string,
  supportingPrimary: string,
  supportingSecondary: string,
  ensemblePrimary: string,
  ensembleSecondary: string,
  datePlacePrimary: string,
  datePlaceSecondary: string,
): RecordingDailyDisplay {
  return {
    title,
    subtitle,
    workPrimary,
    workSecondary,
    composerPrimary,
    composerSecondary,
    principalPrimary,
    principalSecondary,
    supportingPrimary,
    supportingSecondary,
    ensemblePrimary,
    ensembleSecondary,
    datePlacePrimary,
    datePlaceSecondary,
  };
}

export function buildRecordingDisplayModel(recording: Recording, library: LibraryData): RecordingDisplayModel {
  const credits = collectCreditDisplays(recording, library);
  const workTypeHint = normalizeRecordingWorkTypeHintValue(recording.workTypeHint);
  const presentationFamily = deriveRecordingPresentationFamily({
    workTypeHint,
    conductorCount: credits.conductors.length,
    orchestraCount: credits.orchestras.length,
    soloistCount: credits.soloists.length,
    singerCount: credits.singers.length,
    ensembleCount: credits.groups.length,
  });
  const eventMeta = buildRecordingDatePlace(recording);
  const workLines = buildRecordingWorkLines(recording, library);
  const conductorPrimary = joinDisplayValues(credits.conductors, "primary");
  const conductorSecondary = joinDisplayValues(credits.conductors, "secondary");
  const soloistPrimary = joinDisplayValues(credits.soloists, "primary");
  const soloistSecondary = joinDisplayValues(credits.soloists, "secondary");
  const singerPrimary = joinDisplayValues(credits.singers, "primary");
  const singerSecondary = joinDisplayValues(credits.singers, "secondary");
  const orchestraPrimary = joinDisplayValues(credits.orchestras, "fullLabel");
  const orchestraSecondary = joinDisplayValues(credits.orchestras, "secondary");
  const groupPrimary = joinDisplayValues(credits.groups, "fullLabel");
  const groupSecondary = joinDisplayValues(credits.groups, "secondary");
  const combinedEnsemblePrimary = dedupe([orchestraPrimary, groupPrimary]).join(shortTitleDelimiter);
  const combinedEnsembleSecondary = dedupe([orchestraSecondary, groupSecondary]).join(shortTitleDelimiter);
  const titleEventPrimary = eventMeta.primary || eventMeta.secondary;

  let title = "";
  let subtitle = "";
  let principalPrimary = "";
  let principalSecondary = "";
  let supportingPrimary = "";
  let supportingSecondary = "";
  let ensemblePrimary = combinedEnsemblePrimary;
  let ensembleSecondary = combinedEnsembleSecondary;

  if (presentationFamily === "concerto") {
    principalPrimary = conductorPrimary;
    principalSecondary = conductorSecondary;
    supportingPrimary = soloistPrimary;
    supportingSecondary = soloistSecondary;
    title = joinRecordingTitleParts([principalPrimary, supportingPrimary, ensemblePrimary, titleEventPrimary]);
    subtitle = joinRecordingTitleParts([principalSecondary, supportingSecondary, ensembleSecondary, titleEventPrimary]);
  } else if (presentationFamily === "opera") {
    principalPrimary = conductorPrimary;
    principalSecondary = conductorSecondary;
    supportingPrimary = singerPrimary || soloistPrimary;
    supportingSecondary = singerSecondary || soloistSecondary;
    title = joinRecordingTitleParts([principalPrimary, supportingPrimary, ensemblePrimary, titleEventPrimary]);
    subtitle = joinRecordingTitleParts([principalSecondary, supportingSecondary, ensembleSecondary, titleEventPrimary]);
  } else if (presentationFamily === "solo" || presentationFamily === "chamber") {
    const leadPrimary = groupPrimary || soloistPrimary || singerPrimary;
    const leadSecondary = groupSecondary || soloistSecondary || singerSecondary;
    const collaboratorPrimary = !groupPrimary && soloistPrimary && soloistPrimary !== leadPrimary ? soloistPrimary : "";
    const collaboratorSecondary = !groupSecondary && soloistSecondary && soloistSecondary !== leadSecondary ? soloistSecondary : "";
    principalPrimary = leadPrimary;
    principalSecondary = leadSecondary;
    supportingPrimary = collaboratorPrimary;
    supportingSecondary = collaboratorSecondary;
    ensemblePrimary = combinedEnsemblePrimary;
    ensembleSecondary = combinedEnsembleSecondary;
    title =
      joinRecordingTitleParts([leadPrimary, presentationFamily === "solo" ? eventMeta.secondary : supportingPrimary || eventMeta.secondary, eventMeta.primary]) ||
      joinRecordingTitleParts([leadPrimary, eventMeta.primary]) ||
      compact(recording.title);
    subtitle =
      joinRecordingTitleParts([leadSecondary, presentationFamily === "solo" ? eventMeta.secondary : supportingSecondary || eventMeta.secondary, eventMeta.primary]) ||
      joinRecordingTitleParts([leadSecondary, eventMeta.primary]);
  } else {
    principalPrimary = conductorPrimary;
    principalSecondary = conductorSecondary;
    supportingPrimary = soloistPrimary || singerPrimary;
    supportingSecondary = soloistSecondary || singerSecondary;
    title = joinRecordingTitleParts([principalPrimary, ensemblePrimary, titleEventPrimary]) || compact(recording.title);
    subtitle = joinRecordingTitleParts([principalSecondary, ensembleSecondary, titleEventPrimary]);
  }

  const safeTitle = title || compact(recording.title) || "*";
  const safeSubtitle = subtitle || eventMeta.combined || safeTitle;
  const metaText = eventMeta.combined;
  const dailyDetailPrimary = eventMeta.primary;
  const dailyDetailSecondary = eventMeta.secondary;

  return {
    title: safeTitle,
    subtitle: safeSubtitle,
    secondaryText: safeSubtitle,
    metaText,
    noteExcerpt: excerpt(recording.notes || "", 120),
    daily: buildDailyDisplay(
      safeTitle,
      safeSubtitle,
      workLines.primary,
      workLines.secondary,
      workLines.composerPrimary,
      workLines.composerSecondary,
      "",
      "",
      "",
      "",
      "",
      "",
      dailyDetailPrimary,
      dailyDetailSecondary,
    ),
  };
}

export function buildRecordingDisplayTitle(recording: Recording, library: LibraryData) {
  return buildRecordingDisplayModel(recording, library).title;
}

export function buildRecordingListEntry(recording: Recording, library: LibraryData): RecordingListEntryDisplay {
  const model = buildRecordingDisplayModel(recording, library);
  return {
    title: model.title,
    secondaryText: model.secondaryText,
    metaText: model.metaText,
    noteExcerpt: model.noteExcerpt,
  };
}

function collectLegacyLibraryDataIssues(library: LibraryData): LibraryDataIssue[] {
  void library;
  /*
  const issues: LibraryDataIssue[] = [];

  for (const composer of library.composers) {
    const display = getDisplayData(composer);
    const composerCompat = composer as Composer;
    if (!composerCompat.displayFullName || !composerCompat.displayLatinName) {
      issues.push({
        entityType: "composer",
        entityId: composer.id,
        category: "name-normalization",
        message: `作曲家 ${display.primary} 缺少规范全名或外文名。`,
      });
    }
    if (composer.name && composerCompat.fullName && composer.name === composerCompat.fullName && composer.name.length <= 4) {
      issues.push({
        entityType: "composer",
        entityId: composer.id,
        category: "name-normalization",
        message: `作曲家 ${display.primary} 的中文名与中文全名未区分。`,
      });
    }
    if (!getCountryValues(composer).length) {
      issues.push({
        entityType: "composer",
        entityId: composer.id,
        category: "country-missing",
        message: `作曲家 ${display.primary} 缺少国家信息。`,
      });
    }
  }

  for (const person of library.people) {
    const display = getDisplayData(person);
    const personCompat = person as Person;
    if (!personCompat.displayFullName || !personCompat.displayLatinName) {
      issues.push({
        entityType: "person",
        entityId: person.id,
        category: "name-normalization",
        message: `人物 ${display.primary} 缺少规范全名或外文名。`,
      });
    }
    if (person.name && personCompat.fullName && person.name === personCompat.fullName && person.name.length <= 4) {
      issues.push({
        entityType: "person",
        entityId: person.id,
        category: "name-normalization",
        message: `人物 ${display.primary} 的中文名与中文全名未区分。`,
      });
    }
    if (person.birthYear && person.deathYear && person.birthYear > person.deathYear) {
      issues.push({
        entityType: "person",
        entityId: person.id,
        category: "year-conflict",
        message: `人物 ${display.primary} 的生卒年份冲突。`,
      });
    }
    if (person.roles.some((role) => role === "orchestra" || role === "ensemble" || role === "chorus") && (personCompat.abbreviations?.length ?? 0) === 0) {
      issues.push({
        entityType: "person",
        entityId: person.id,
        category: "abbreviation-missing",
        message: `团体 ${display.primary} 缺少简称或缩写。`,
      });
    }
    if (!person.summary) {
      issues.push({
        entityType: "person",
        entityId: person.id,
        category: "summary-missing",
        message: `人物 ${display.primary} 缺少简介。`,
      });
    }
  }

  return issues;
}
  */
  return [];
}

function collectBrokenLibraryDataIssues(library: LibraryData): LibraryDataIssue[] {
  void library;
  /*
  const issues: LibraryDataIssue[] = [];

  for (const composer of library.composers) {
    const display = getDisplayData(composer);
    const composerCompat = composer as Composer;
    if (!composerCompat.displayFullName || !composerCompat.displayLatinName) {
      issues.push({
        entityType: "composer",
        entityId: composer.id,
        category: "name-normalization",
        message: `作曲家 ${display.primary} 缺少规范全名或外文名。`,
      });
    }
    if (composer.name && composerCompat.fullName && composer.name === composerCompat.fullName && composer.name.length <= 4) {
      issues.push({
        entityType: "composer",
        entityId: composer.id,
        category: "name-normalization",
        message: `作曲家 ${display.primary} 的中文名与中文全名未区分。`,
      });
    }
    if (!getCountryValues(composer).length) {
      issues.push({
        entityType: "composer",
        entityId: composer.id,
        category: "country-missing",
        message: `作曲家 ${display.primary} 缺少国家信息。`,
      });
    }
  }

  for (const person of library.people) {
    const display = getDisplayData(person);
    const personCompat = person as Person;
    if (!personCompat.displayFullName || !personCompat.displayLatinName) {
      issues.push({
        entityType: "person",
        entityId: person.id,
        category: "name-normalization",
        message: `人物 ${display.primary} 缺少规范全名或外文名。`,
      });
    }
    if (person.name && personCompat.fullName && person.name === personCompat.fullName && person.name.length <= 4) {
      issues.push({
        entityType: "person",
        entityId: person.id,
        category: "name-normalization",
        message: `人物 ${display.primary} 的中文名与中文全名未区分。`,
      });
    }
    if (person.birthYear && person.deathYear && person.birthYear > person.deathYear) {
      issues.push({
        entityType: "person",
        entityId: person.id,
        category: "year-conflict",
        message: `人物 ${display.primary} 的生卒年份冲突。`,
      });
    }
    if (person.roles.some((role) => role === "orchestra" || role === "ensemble" || role === "chorus") && (personCompat.abbreviations?.length ?? 0) === 0) {
      issues.push({
        entityType: "person",
        entityId: person.id,
        category: "abbreviation-missing",
        message: `团体 ${display.primary} 缺少简称或缩写。`,
      });
    }
    if (!person.summary) {
      issues.push({
        entityType: "person",
        entityId: person.id,
        category: "summary-missing",
        message: `人物 ${display.primary} 缺少简介。`,
      });
    }
  }

  return issues;
}
  */
  return [];
}

void [joinCreditNames, buildBrokenRecordingWorkLines, buildLegacyRecordingWorkLines, collectLegacyLibraryDataIssues, collectBrokenLibraryDataIssues];

export function collectLibraryDataIssues(library: LibraryData): LibraryDataIssue[] {
  const issues: LibraryDataIssue[] = [];
  const worksByComposerId = new Map<string, number>();
  const recordingsByWorkId = new Map<string, number>();
  const workMap = new Map(library.works.map((work) => [work.id, work]));
  const personMap = new Map(library.people.map((person) => [person.id, person]));

  for (const work of library.works) {
    worksByComposerId.set(work.composerId, (worksByComposerId.get(work.composerId) ?? 0) + 1);
  }
  for (const recording of library.recordings) {
    recordingsByWorkId.set(recording.workId, (recordingsByWorkId.get(recording.workId) ?? 0) + 1);
  }

  for (const composer of library.composers) {
    const display = getDisplayData(composer);
    if (!hasCanonicalDisplayNames(composer)) {
      issues.push({
        entityType: "composer",
        entityId: composer.id,
        category: "name-normalization",
        message: "composer " + display.primary + " missing canonical full name or latin name",
      });
    }
    if (!hasDistinctChineseShortAndFullNames(composer)) {
      issues.push({
        entityType: "composer",
        entityId: composer.id,
        category: "name-normalization",
        message: "composer " + display.primary + " has no distinct short/full Chinese names",
      });
    }
    if (!composer.country) {
      issues.push({
        entityType: "composer",
        entityId: composer.id,
        category: "country-missing",
        message: "composer " + display.primary + " missing country",
      });
    }
    if (!worksByComposerId.get(composer.id)) {
      issues.push({
        entityType: "composer",
        entityId: composer.id,
        category: "relation-missing",
        message: "composer " + display.primary + " has no linked works",
      });
    }
  }

  for (const person of library.people) {
    const display = getDisplayData(person);
    if (!hasCanonicalDisplayNames(person)) {
      issues.push({
        entityType: "person",
        entityId: person.id,
        category: "name-normalization",
        message: "person " + display.primary + " missing canonical full name or latin name",
      });
    }
    if (!hasDistinctChineseShortAndFullNames(person)) {
      issues.push({
        entityType: "person",
        entityId: person.id,
        category: "name-normalization",
        message: "person " + display.primary + " has no distinct short/full Chinese names",
      });
    }
    if (person.birthYear && person.deathYear && person.birthYear > person.deathYear) {
      issues.push({
        entityType: "person",
        entityId: person.id,
        category: "year-conflict",
        message: "person " + display.primary + " has conflicting birth/death years",
      });
    }
    if (isMissingSummaryValue(person.summary)) {
      issues.push({
        entityType: "person",
        entityId: person.id,
        category: "summary-missing",
        message: "person " + display.primary + " missing usable summary",
      });
    }
    if (!getCountryValues(person).length) {
      issues.push({
        entityType: "person",
        entityId: person.id,
        category: "country-missing",
        message: "person " + display.primary + " missing country",
      });
    }
  }

  for (const work of library.works) {
    if (!recordingsByWorkId.get(work.id)) {
      issues.push({
        entityType: "work",
        entityId: work.id,
        category: "relation-missing",
        message: "work " + compact(work.title) + " has no linked recordings",
      });
    }
    if (!isMissingSummaryValue(work.summary)) {
      continue;
    }
    issues.push({
      entityType: "work",
      entityId: work.id,
      category: "summary-missing",
      message: "work " + compact(work.title) + " missing usable summary",
    });
  }

  for (const recording of library.recordings) {
    const work = workMap.get(recording.workId);
    if (!work) {
      issues.push({
        entityType: "recording",
        entityId: recording.id,
        category: "relation-missing",
        message: "recording " + compact(recording.title) + " has no linked work",
      });
    }

    for (const credit of recording.credits) {
      const displayName = compact(credit.displayName);
      if (!displayName) {
        continue;
      }
      if (!compact(credit.personId)) {
        issues.push({
          entityType: "recording",
          entityId: recording.id,
          category: "relation-missing",
          message: "recording " + compact(recording.title) + " has an unlinked " + credit.role + " credit for " + displayName,
        });
        continue;
      }
      const personId = compact(credit.personId);
      const person = personMap.get(personId);
      if (!person) {
        issues.push({
          entityType: "recording",
          entityId: recording.id,
          category: "relation-missing",
          message: "recording " + compact(recording.title) + " references a missing linked person " + personId,
        });
        continue;
      }
      if (!creditRoleCompatibleWithPerson(credit.role, person)) {
        issues.push({
          entityType: "recording",
          entityId: recording.id,
          category: "relation-mismatch",
          message:
            "recording " +
            compact(recording.title) +
            " links " +
            displayName +
            " as " +
            credit.role +
            " but person roles are " +
            (person.roles.join("/") || "unknown"),
        });
      }
    }
  }

  return issues;
}
