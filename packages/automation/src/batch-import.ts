import { promises as fs } from "node:fs";
import path from "node:path";

import { findPersonForCredit } from "../../data-core/src/person-cleanup.js";
import { detectPlatformFromUrl } from "../../data-core/src/resource-links.js";
import {
  lookupOrchestraReference,
  lookupPersonReference,
  parseOrchestraReferenceText,
  type ReferenceRegistry,
} from "../../data-core/src/reference-registry.js";
import {
  buildBatchRecordingCredits,
  buildBatchRecordingTitle,
  getBatchRecordingTemplateSpec,
  normalizeRecordingWorkTypeHintValue,
} from "../../shared/src/recording-rules.js";
import { validateLibrary, type Composer, type Credit, type LibraryData, type Person, type PersonRole, type Recording, type Work } from "../../shared/src/schema.js";
import { createEntityId, createSlug, createSortKey, ensureUniqueValue } from "../../shared/src/slug.js";

export type BatchDraftReviewState = "unconfirmed" | "confirmed" | "discarded";

export type BatchDraftEntry<T> = {
  draftId: string;
  entityType: "composer" | "person" | "work" | "recording";
  sourceLine: string;
  notes: string[];
  reviewState: BatchDraftReviewState;
  entity: T;
};

export type BatchDraftEntities = {
  composers: BatchDraftEntry<Composer>[];
  people: BatchDraftEntry<Person>[];
  works: BatchDraftEntry<Work>[];
  recordings: BatchDraftEntry<Recording>[];
};

export type BatchCreatedEntityRefs = {
  composers: string[];
  people: string[];
  workGroups: string[];
  works: string[];
  recordings: string[];
};

export type AnalyzeBatchImportResult = {
  composerId: string;
  workId: string;
  selectedComposerId: string;
  selectedWorkId: string;
  workTypeHint: string;
  draftLibrary: LibraryData;
  createdEntityRefs: BatchCreatedEntityRefs;
  draftEntities: BatchDraftEntities;
  warnings: string[];
  parseNotes: string[];
  llmUsed: boolean;
};

type AnalyzeBatchImportOptions = {
  sourceText: string;
  library: LibraryData;
  composerId?: string;
  workId?: string;
  workTypeHint?: string;
  referenceRegistry?: ReferenceRegistry;
};

function compact(value: unknown) {
  return String(value ?? "").trim();
}

function cloneLibrary(library: LibraryData): LibraryData {
  return structuredClone(library);
}

export function cloneBatchDraftEntities(draftEntities: BatchDraftEntities): BatchDraftEntities {
  return structuredClone(draftEntities);
}

function emptyInfoPanel() {
  return { text: "", articleId: "", collectionLinks: [] };
}

function uniqueStrings(values: unknown[]) {
  const seen = new Set<string>();
  const items: string[] = [];
  for (const value of values) {
    const normalized = compact(value);
    if (!normalized || seen.has(normalized)) {
      continue;
    }
    seen.add(normalized);
    items.push(normalized);
  }
  return items;
}

function uniqueId(prefix: string, value: string, ids: Set<string>) {
  const nextId = ensureUniqueValue(createEntityId(prefix, value), ids);
  ids.add(nextId);
  return nextId;
}

function uniqueSlug(value: string, slugs: Set<string>) {
  const nextSlug = ensureUniqueValue(createSlug(value), slugs);
  slugs.add(nextSlug);
  return nextSlug;
}

function createDraftEntry<T extends Composer | Person | Work | Recording>(
  entityType: BatchDraftEntry<T>["entityType"],
  entity: T,
  sourceLine: string,
  notes: string[] = [],
): BatchDraftEntry<T> {
  return {
    draftId: `${entityType}:${entity.id}`,
    entityType,
    sourceLine,
    notes,
    reviewState: "unconfirmed",
    entity,
  };
}

function upsertById<T extends { id: string }>(collection: T[], entity: T) {
  const index = collection.findIndex((item) => item.id === entity.id);
  if (index >= 0) {
    collection[index] = entity;
    return;
  }
  collection.push(entity);
}

function primaryPersonRoleFromCredit(role: Credit["role"]): PersonRole {
  if (role === "instrumentalist") {
    return "instrumentalist";
  }
  if (role === "conductor" || role === "soloist" || role === "singer" || role === "ensemble" || role === "orchestra" || role === "chorus") {
    return role;
  }
  return "other";
}

function createCoarsePersonForCredit(
  draftLibrary: LibraryData,
  credit: Credit,
  sourceLine: string,
  draftEntities: BatchDraftEntities,
  createdEntityRefs: BatchCreatedEntityRefs,
  personIds: Set<string>,
  personSlugs: Set<string>,
) {
  const displayName = compact(credit.displayName);
  const role = primaryPersonRoleFromCredit(credit.role);
  const person: Person = {
    id: uniqueId(`person-${role}`, displayName, personIds),
    slug: uniqueSlug(displayName, personSlugs),
    name: displayName,
    fullName: "",
    nameLatin: /[A-Za-z]/.test(displayName) ? displayName : "",
    displayName: displayName,
    displayFullName: "",
    displayLatinName: /[A-Za-z]/.test(displayName) ? displayName : "",
    country: "",
    countries: [],
    avatarSrc: "",
    aliases: [],
    sortKey: createSortKey(draftLibrary.people.length),
    summary: "",
    imageSourceUrl: "",
    imageSourceKind: "",
    imageAttribution: "",
    imageUpdatedAt: "",
    infoPanel: emptyInfoPanel(),
    roles: [role],
  };
  draftLibrary.people.push(person);
  createdEntityRefs.people.push(person.id);
  draftEntities.people.push(
    createDraftEntry("person", person, sourceLine, [`auto-linked-from=${credit.role}`]),
  );
  return person;
}

function ensureBatchPeopleForCredits(
  draftLibrary: LibraryData,
  credits: Credit[],
  sourceLine: string,
  draftEntities: BatchDraftEntities,
  createdEntityRefs: BatchCreatedEntityRefs,
) {
  const personIds = new Set(draftLibrary.people.map((item) => item.id));
  const personSlugs = new Set(draftLibrary.people.map((item) => item.slug));

  for (const credit of credits) {
    if (compact(credit.personId) || !compact(credit.displayName)) {
      continue;
    }
    const matchedPerson = findPersonForCredit(draftLibrary, credit.role, credit.displayName);
    if (matchedPerson) {
      credit.personId = matchedPerson.id;
      continue;
    }
    const createdPerson = createCoarsePersonForCredit(
      draftLibrary,
      credit,
      sourceLine,
      draftEntities,
      createdEntityRefs,
      personIds,
      personSlugs,
    );
    credit.personId = createdPerson.id;
  }
}

function strictTemplateFieldCount(workTypeHint: string) {
  return getBatchRecordingTemplateSpec(workTypeHint).fieldCount;
}

function normalizeLooseBatchSeparator(line: string) {
  return String(line ?? "")
    .normalize("NFKC")
    .replace(/[｜￨│┃┆丨]/g, "|")
    .replace(/\s+[~～—–－]+\s+/g, " | ")
    .replace(/\s*\|\s*/g, " | ");
}

export function normalizeBatchImportSource(sourceText: string, workTypeHint: string) {
  const normalizedWorkTypeHint = normalizeRecordingWorkTypeHintValue(workTypeHint);
  const expectedFieldCount = strictTemplateFieldCount(normalizedWorkTypeHint);

  return String(sourceText ?? "")
    .replace(/\r\n/g, "\n")
    .split("\n")
    .map((line) => compact(normalizeLooseBatchSeparator(line)))
    .filter(Boolean)
    .map((line) => {
      const rawSlots = line.split("|").map((item) => compact(item));
      if (rawSlots.length === expectedFieldCount - 1) {
        return [...rawSlots, "-"].join(" | ");
      }
      return rawSlots.join(" | ");
    })
    .join("\n");
}

function splitStrictBatchLine(line: string) {
  return line.split("|").map((item) => compact(item));
}

function parseStrictBatchLinks(sourceText: string) {
  if (!compact(sourceText) || compact(sourceText) === "-") {
    return [];
  }
  return uniqueStrings(String(sourceText).split(",")).map((url) => ({
    platform: detectPlatformFromUrl(url),
    url,
    localPath: "",
    title: "",
    linkType: "external" as const,
    visibility: "public" as const,
  }));
}

function buildStrictRecordingTitle(workTypeHint: string, slots: string[]) {
  return buildBatchRecordingTitle(workTypeHint, slots);
}

function buildStrictRecordingCredits(workTypeHint: string, slots: string[]) {
  return buildBatchRecordingCredits(workTypeHint, slots) as Credit[];
}

function buildStrictBatchParseNotes(workTypeHint: string) {
  return getBatchRecordingTemplateSpec(workTypeHint).parseNotes;
}

export function parseOrchestraAbbreviationText(sourceText: string) {
  const entries = Object.create(null) as Record<string, string>;
  for (const entry of parseOrchestraReferenceText(sourceText)) {
    for (const abbreviation of entry.abbreviations) {
      const key = abbreviation.toUpperCase();
      if (!entries[key]) {
        entries[key] = entry.preferredValue || entry.canonicalLatin;
      }
    }
  }
  return entries;
}

export async function loadOrchestraAbbreviationMap(
  filePath = path.join(process.cwd(), "materials", "references", "Orchestra Abbreviation Comparison.txt"),
) {
  const candidates = [filePath];

  for (const candidate of candidates) {
    try {
      const sourceText = await fs.readFile(candidate, "utf8");
      return parseOrchestraAbbreviationText(sourceText);
    } catch {
      // try next candidate
    }
  }

  return {};
}

function normalizeBatchValue(value: string, resolver: (input: string) => string) {
  const normalized = compact(value);
  if (!normalized || normalized === "-") {
    return normalized;
  }
  return normalized
    .split(/\s*\+\s*/g)
    .map((item) => compact(item))
    .filter(Boolean)
    .map((item) => resolver(item))
    .join(" + ");
}

function normalizeBatchSlotsWithReferenceRegistry(workTypeHint: string, slots: string[], referenceRegistry?: ReferenceRegistry) {
  if (!referenceRegistry) {
    return slots;
  }

  const resolvePerson = (value: string, roles: string | string[]) =>
    lookupPersonReference(referenceRegistry, value, roles)?.preferredValue || compact(value);
  const resolveOrchestra = (value: string) =>
    lookupOrchestraReference(referenceRegistry, value)?.preferredValue ||
    lookupPersonReference(referenceRegistry, value, ["ensemble", "global"])?.preferredValue ||
    compact(value);

  if (workTypeHint === "concerto") {
    return [
      normalizeBatchValue(slots[0] || "", (value) => resolvePerson(value, ["soloist", "instrumentalist", "pianist", "violinist", "global"])),
      normalizeBatchValue(slots[1] || "", (value) => resolvePerson(value, ["conductor", "global"])),
      normalizeBatchValue(slots[2] || "", resolveOrchestra),
      slots[3] || "",
      slots[4] || "",
    ];
  }

  if (workTypeHint === "opera_vocal") {
    return [
      normalizeBatchValue(slots[0] || "", (value) => resolvePerson(value, ["conductor", "global"])),
      normalizeBatchValue(slots[1] || "", (value) => resolvePerson(value, ["singer", "soloist", "soprano", "tenor", "baritone", "global"])),
      normalizeBatchValue(slots[2] || "", resolveOrchestra),
      slots[3] || "",
      slots[4] || "",
    ];
  }

  if (workTypeHint === "chamber_solo") {
    return [
      normalizeBatchValue(slots[0] || "", (value) => resolvePerson(value, ["soloist", "instrumentalist", "singer", "global"])),
      normalizeBatchValue(slots[1] || "", (value) => {
        const orchestraMatch = resolveOrchestra(value);
        if (orchestraMatch !== compact(value)) {
          return orchestraMatch;
        }
        return resolvePerson(value, ["ensemble", "soloist", "instrumentalist", "singer", "global"]);
      }),
      slots[2] || "",
      slots[3] || "",
    ];
  }

  return [
    normalizeBatchValue(slots[0] || "", (value) => resolvePerson(value, ["conductor", "global"])),
    normalizeBatchValue(slots[1] || "", resolveOrchestra),
    slots[2] || "",
    slots[3] || "",
  ];
}

function collectBatchSelections(baseLibrary: LibraryData, fullDraftLibrary: LibraryData, draftEntities: BatchDraftEntities) {
  const baseComposerIds = new Set(baseLibrary.composers.map((item) => item.id));
  const baseWorkGroupIds = new Set(baseLibrary.workGroups.map((item) => item.id));
  const baseWorkIds = new Set(baseLibrary.works.map((item) => item.id));
  const basePersonIds = new Set(baseLibrary.people.map((item) => item.id));
  const recordingMap = new Map((draftEntities.recordings || []).map((entry) => [entry.entity.id, entry]));
  const composerMap = new Map((fullDraftLibrary.composers || []).map((item) => [item.id, item]));
  const workGroupMap = new Map((fullDraftLibrary.workGroups || []).map((item) => [item.id, item]));
  const workMap = new Map((fullDraftLibrary.works || []).map((item) => [item.id, item]));
  const personMap = new Map((fullDraftLibrary.people || []).map((item) => [item.id, item]));
  const selectedRecordingIds = new Set<string>();
  const selectedComposerIds = new Set<string>();
  const selectedWorkGroupIds = new Set<string>();
  const selectedWorkIds = new Set<string>();
  const selectedPersonIds = new Set<string>();

  for (const entry of draftEntities.composers || []) {
    if (entry.reviewState === "confirmed") {
      selectedComposerIds.add(entry.entity.id);
    }
  }
  for (const entry of draftEntities.people || []) {
    if (entry.reviewState === "confirmed") {
      selectedPersonIds.add(entry.entity.id);
    }
  }
  for (const entry of draftEntities.works || []) {
    if (entry.reviewState === "confirmed") {
      selectedWorkIds.add(entry.entity.id);
    }
  }
  for (const entry of draftEntities.recordings || []) {
    if (entry.reviewState === "confirmed") {
      selectedRecordingIds.add(entry.entity.id);
      if (!baseWorkIds.has(entry.entity.workId)) {
        selectedWorkIds.add(entry.entity.workId);
      }
      for (const credit of entry.entity.credits || []) {
        const personId = compact(credit.personId);
        if (personId && !basePersonIds.has(personId)) {
          selectedPersonIds.add(personId);
        }
      }
    }
  }

  for (const workId of [...selectedWorkIds]) {
    const work = workMap.get(workId);
    if (!work) {
      continue;
    }
    if (!baseComposerIds.has(work.composerId)) {
      selectedComposerIds.add(work.composerId);
    }
    for (const groupId of work.groupIds || []) {
      if (!baseWorkGroupIds.has(groupId)) {
        selectedWorkGroupIds.add(groupId);
      }
    }
  }

  const nextLibrary = cloneLibrary(baseLibrary);
  for (const composerId of selectedComposerIds) {
    const composer = composerMap.get(composerId);
    if (composer) {
      upsertById(nextLibrary.composers, composer);
    }
  }
  for (const groupId of selectedWorkGroupIds) {
    const group = workGroupMap.get(groupId);
    if (group) {
      upsertById(nextLibrary.workGroups, group);
    }
  }
  for (const workId of selectedWorkIds) {
    const work = workMap.get(workId);
    if (work) {
      upsertById(nextLibrary.works, work);
    }
  }
  for (const personId of selectedPersonIds) {
    const person = personMap.get(personId);
    if (person) {
      upsertById(nextLibrary.people, person);
    }
  }
  for (const recordingId of selectedRecordingIds) {
    const entry = recordingMap.get(recordingId);
    if (entry) {
      upsertById(nextLibrary.recordings, entry.entity);
    }
  }

  return {
    draftLibrary: validateLibrary(nextLibrary),
    createdEntityRefs: {
      composers: [...selectedComposerIds],
      people: [...selectedPersonIds],
      workGroups: [...selectedWorkGroupIds],
      works: [...selectedWorkIds],
      recordings: [...selectedRecordingIds],
    } satisfies BatchCreatedEntityRefs,
  };
}

export function buildConfirmedBatchSelection(baseLibrary: LibraryData, fullDraftLibrary: LibraryData, draftEntities: BatchDraftEntities) {
  return collectBatchSelections(baseLibrary, fullDraftLibrary, draftEntities);
}

export async function analyzeBatchImport(options: AnalyzeBatchImportOptions): Promise<AnalyzeBatchImportResult> {
  const composerId = compact(options.composerId);
  const workId = compact(options.workId);
  if (!composerId || !workId) {
    throw new Error("批量导入前必须先选定作曲家和作品。");
  }

  const composer = options.library.composers.find((item) => item.id === composerId);
  const work = options.library.works.find((item) => item.id === workId);
  if (!composer) {
    throw new Error(`未找到已选作曲家：${composerId}`);
  }
  if (!work) {
    throw new Error(`未找到已选作品：${workId}`);
  }
  if (work.composerId !== composer.id) {
    throw new Error("所选作品不属于当前作曲家。");
  }

  const workTypeHint = normalizeRecordingWorkTypeHintValue(options.workTypeHint);
  const sourceText = normalizeBatchImportSource(options.sourceText ?? "", workTypeHint);
  const lines = sourceText
    .split("\n")
    .map((line) => compact(line))
    .filter(Boolean);
  if (lines.length === 0) {
    throw new Error("批量导入文本不能为空。");
  }

  const draftLibrary = cloneLibrary(options.library);
  const draftEntities: BatchDraftEntities = {
    composers: [],
    people: [],
    works: [],
    recordings: [],
  };
  const createdEntityRefs: BatchCreatedEntityRefs = {
    composers: [],
    people: [],
    workGroups: [],
    works: [],
    recordings: [],
  };
  const warnings: string[] = [];
  const parseNotes = [...buildStrictBatchParseNotes(workTypeHint), `已选作曲家：${composer.name}`, `已选作品：${work.title}`];

  for (const line of lines) {
    const slots = normalizeBatchSlotsWithReferenceRegistry(workTypeHint, splitStrictBatchLine(line), options.referenceRegistry);
    const fieldCount = strictTemplateFieldCount(workTypeHint);
    if (slots.length !== fieldCount) {
      throw new Error(`批量导入模板不合法：${line}。当前 ${workTypeHint} 模板要求 ${fieldCount} 个字段，并使用 | 分隔。`);
    }

    const year =
      workTypeHint === "concerto" || workTypeHint === "opera_vocal"
        ? compact(slots[3])
        : workTypeHint === "chamber_solo"
          ? compact(slots[2])
          : compact(slots[2]);
    const linkSlot =
      workTypeHint === "concerto" || workTypeHint === "opera_vocal"
        ? slots[4]
        : workTypeHint === "chamber_solo"
          ? slots[3]
          : slots[3];
    const links = parseStrictBatchLinks(linkSlot || "");
    const title = buildStrictRecordingTitle(workTypeHint, slots) || line;
    const recordingIds = new Set(draftLibrary.recordings.map((item) => item.id));
    const recordingSlugs = new Set(draftLibrary.recordings.map((item) => item.slug));
    const recording: Recording = {
      id: uniqueId("recording", `${work.id}-${title}`, recordingIds),
      workId: work.id,
      slug: uniqueSlug(title, recordingSlugs),
      title,
      workTypeHint,
      sortKey: createSortKey(draftLibrary.recordings.length),
      isPrimaryRecommendation: false,
      updatedAt: new Date().toISOString(),
      images: [],
      credits: buildStrictRecordingCredits(workTypeHint, slots),
      links,
      notes: "",
      performanceDateText: year && year !== "-" ? year : "",
      venueText: "",
      albumTitle: "",
      label: "",
      releaseDate: "",
      infoPanel: emptyInfoPanel(),
    };
    ensureBatchPeopleForCredits(draftLibrary, recording.credits, line, draftEntities, createdEntityRefs);
    draftLibrary.recordings.push(recording);
    createdEntityRefs.recordings.push(recording.id);
    draftEntities.recordings.push(
      createDraftEntry("recording", recording, line, [
        `workTypeHint=${workTypeHint}`,
        ...(links.length ? [`links=${links.length}`] : []),
      ]),
    );
  }

  return {
    composerId,
    workId,
    selectedComposerId: composerId,
    selectedWorkId: workId,
    workTypeHint,
    draftLibrary: validateLibrary(draftLibrary),
    createdEntityRefs,
    draftEntities,
    warnings,
    parseNotes,
    llmUsed: false,
  };
}

