import { buildRecordingDisplayTitle } from "../../shared/src/display.js";
import { resolveRecordingWorkTypeHintValue } from "../../shared/src/recording-rules.js";
import { validateLibrary, type Credit, type InfoPanel, type LibraryData, type ResourceLink } from "../../shared/src/schema.js";
import { createEntityId, createSlug, createSortKey, ensureUniqueValue } from "../../shared/src/slug.js";

export type OwnerEditableEntityType = "composer" | "person" | "work" | "recording";
export type OwnerEntityRelation = {
  entityType: OwnerEditableEntityType;
  id: string;
  label: string;
  title: string;
  subtitle: string;
  groupLabel: string;
};

type OwnerEntityPayload = {
  id?: string;
  slug?: string;
  sortKey?: string;
  name?: string;
  nameLatin?: string;
  country?: string | string[];
  countries?: string[];
  avatarSrc?: string;
  imageSourceUrl?: string;
  imageSourceKind?: string;
  imageAttribution?: string;
  imageUpdatedAt?: string;
  birthYear?: number | string;
  deathYear?: number | string;
  roles?: string[];
  aliases?: string[];
  summary?: string;
  infoPanel?: Partial<InfoPanel>;
  composerId?: string;
  title?: string;
  titleLatin?: string;
  catalogue?: string;
  groupPath?: string[];
  workId?: string;
  workTypeHint?: string;
  conductorPersonId?: string;
  orchestraPersonId?: string;
  isPrimaryRecommendation?: boolean;
  images?: LibraryData["recordings"][number]["images"];
  credits?: Credit[];
  links?: ResourceLink[];
  notes?: string;
  performanceDateText?: string;
  venueText?: string;
  albumTitle?: string;
  label?: string;
  releaseDate?: string;
};

export function compactText(value: unknown) {
  return String(value ?? "").trim();
}

function uniqueStrings(values: unknown[]) {
  const seen = new Set<string>();
  const items: string[] = [];
  for (const value of values) {
    const normalized = compactText(value);
    if (!normalized || seen.has(normalized)) {
      continue;
    }
    seen.add(normalized);
    items.push(normalized);
  }
  return items;
}

function nextSortKey(collection: ArrayLike<unknown>) {
  return createSortKey(collection.length);
}

function parseNumber(value: unknown) {
  if (value === undefined || value === null || value === "") {
    return undefined;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function parseCountryValues(value: unknown) {
  if (Array.isArray(value)) {
    return uniqueStrings(value);
  }
  return uniqueStrings(String(value ?? "").split(/[\/,\n;；、|]+/g));
}

function parseList(value: unknown) {
  return Array.isArray(value) ? uniqueStrings(value) : [];
}

function normalizeGroupPath(pathValue: unknown) {
  if (!Array.isArray(pathValue)) {
    return [];
  }
  return pathValue.map((segment) => compactText(segment)).filter(Boolean);
}

function normalizeWorkComparableText(value: unknown) {
  return compactText(value)
    .toLowerCase()
    .replace(/[，。、:：·•()[\]{}]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function escapeRegExp(value: unknown) {
  return String(value ?? "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function stripCatalogueFromWorkSegment(segment: unknown, catalogue: unknown) {
  const normalizedSegment = compactText(segment);
  const normalizedCatalogue = compactText(catalogue);
  if (!normalizedSegment || !normalizedCatalogue) {
    return normalizedSegment;
  }
  if (normalizeWorkComparableText(normalizedSegment) === normalizeWorkComparableText(normalizedCatalogue)) {
    return "";
  }
  const escapedCatalogue = escapeRegExp(normalizedCatalogue).replace(/\s+/g, "\\s+");
  const trailingPatterns = [
    new RegExp(`(?:\\s*[,，、·•-]\\s*|\\s+)\\(?${escapedCatalogue}\\)?$`, "i"),
    new RegExp(`\\(${escapedCatalogue}\\)$`, "i"),
  ];
  return trailingPatterns.reduce((currentValue, pattern) => currentValue.replace(pattern, "").trim(), normalizedSegment);
}

function buildWorkSlugSource(payload: OwnerEntityPayload) {
  return [payload.title, stripCatalogueFromWorkSegment(payload.titleLatin || "", payload.catalogue || ""), payload.catalogue]
    .filter(Boolean)
    .join(" ");
}

function buildWorkInferenceText(payload: OwnerEntityPayload) {
  return [compactText(payload.title || ""), stripCatalogueFromWorkSegment(payload.titleLatin || "", payload.catalogue || ""), compactText(payload.catalogue || "")]
    .filter(Boolean)
    .join(" ");
}

function inferWorkGroupPath(payload: OwnerEntityPayload) {
  const text = buildWorkInferenceText(payload);
  if (!text) {
    return [];
  }

  const concertoInstrumentRules = [
    { pattern: /(钢琴|piano)/i, path: ["协奏曲", "钢琴协奏曲"] },
    { pattern: /(小提琴|violin)/i, path: ["协奏曲", "小提琴协奏曲"] },
    { pattern: /(大提琴|cello)/i, path: ["协奏曲", "大提琴协奏曲"] },
    { pattern: /(中提琴|viola)/i, path: ["协奏曲", "中提琴协奏曲"] },
    { pattern: /(长笛|flute)/i, path: ["协奏曲", "长笛协奏曲"] },
    { pattern: /(单簧管|clarinet)/i, path: ["协奏曲", "单簧管协奏曲"] },
    { pattern: /(双簧管|oboe)/i, path: ["协奏曲", "双簧管协奏曲"] },
    { pattern: /(巴松|bassoon)/i, path: ["协奏曲", "巴松协奏曲"] },
    { pattern: /(小号|trumpet)/i, path: ["协奏曲", "小号协奏曲"] },
    { pattern: /(圆号|horn)/i, path: ["协奏曲", "圆号协奏曲"] },
  ];

  if (/(协奏曲|concerto|concertante)/i.test(text)) {
    return concertoInstrumentRules.find((rule) => rule.pattern.test(text))?.path || ["协奏曲"];
  }
  if (/(歌剧|opera)/i.test(text)) {
    return ["歌剧与声乐", "歌剧"];
  }
  if (/(安魂曲|requiem)/i.test(text)) {
    return ["歌剧与声乐", "安魂曲"];
  }
  if (/(弥撒|mass)/i.test(text)) {
    return ["歌剧与声乐", "弥撒"];
  }
  if (/(清唱剧|神剧|oratorio|cantata)/i.test(text)) {
    return ["歌剧与声乐", "清唱剧"];
  }
  if (/(声乐|vocal|lied|song cycle)/i.test(text)) {
    return ["歌剧与声乐"];
  }
  if (/(交响曲|symphon)/i.test(text)) {
    return ["交响曲"];
  }
  if (/(交响诗|tone poem)/i.test(text)) {
    return ["管弦乐", "交响诗"];
  }
  if (/(序曲|overture)/i.test(text)) {
    return ["管弦乐", "序曲"];
  }
  if (/(组曲|suite)/i.test(text)) {
    return ["管弦乐", "组曲"];
  }
  if (/(舞剧|芭蕾|ballet)/i.test(text)) {
    return ["管弦乐", "舞剧与芭蕾"];
  }
  if (/(交响|管弦|orchestral)/i.test(text)) {
    return ["管弦乐"];
  }
  if (/(奏鸣曲|sonata)/i.test(text)) {
    return ["室内乐与独奏", "奏鸣曲"];
  }
  if (/(四重奏|quartet)/i.test(text)) {
    return ["室内乐与独奏", "四重奏"];
  }
  if (/(五重奏|quintet)/i.test(text)) {
    return ["室内乐与独奏", "五重奏"];
  }
  if (/(三重奏|trio)/i.test(text)) {
    return ["室内乐与独奏", "三重奏"];
  }
  if (/(二重奏|duo)/i.test(text)) {
    return ["室内乐与独奏", "二重奏"];
  }
  if (/(独奏|solo|partita)/i.test(text)) {
    return ["室内乐与独奏", "独奏"];
  }
  if (/(室内乐|chamber)/i.test(text)) {
    return ["室内乐与独奏"];
  }
  return [];
}

function getPersonById(library: LibraryData, personId: string) {
  return (library.people || []).find((person) => person.id === personId) || null;
}

function getWorkById(library: LibraryData, workId: string) {
  return (library.works || []).find((work) => work.id === workId) || null;
}

function getWorkGroupsByWork(library: LibraryData, work?: LibraryData["works"][number] | null) {
  const groupIds = Array.isArray(work?.groupIds) ? work.groupIds : [];
  return groupIds
    .map((groupId) => (library.workGroups || []).find((group) => group.id === groupId))
    .filter((group): group is LibraryData["workGroups"][number] => Boolean(group));
}

function resolveRecordingWorkTypeHint(library: LibraryData, value: unknown, workId: string) {
  const work = getWorkById(library, workId);
  return resolveRecordingWorkTypeHintValue(value, work, getWorkGroupsByWork(library, work));
}

export function ensureWorkGroups(library: LibraryData, composerId: string, groupPathInput: unknown) {
  const groupPath = normalizeGroupPath(groupPathInput);
  const groupIds: string[] = [];

  for (const title of groupPath) {
    const nextPath = [
      ...groupIds
        .map((id) => library.workGroups.find((item) => item.id === id)?.title)
        .map((value) => compactText(value))
        .filter(Boolean),
      title,
    ];
    const nextPathKey = nextPath.join("/");
    const existing = library.workGroups.find(
      (group) =>
        group.composerId === composerId &&
        normalizeGroupPath(group.path).join("/") === nextPathKey,
    );
    if (existing) {
      groupIds.push(existing.id);
      continue;
    }
    const group = {
      id: createEntityId(`group-${composerId}`, nextPath.join("-")),
      composerId,
      title,
      slug: createSlug(title),
      path: nextPath,
      sortKey: nextSortKey(library.workGroups),
    };
    library.workGroups.push(group);
    groupIds.push(group.id);
  }

  return groupIds;
}

function upsertRecordingCredit(credits: Credit[], nextCredit: Credit) {
  const nextCredits = [...credits];
  const matchedIndex = nextCredits.findIndex(
    (credit) =>
      credit.role === nextCredit.role &&
      compactText(credit.personId || "") &&
      compactText(credit.personId || "") === compactText(nextCredit.personId || ""),
  );
  if (matchedIndex >= 0) {
    nextCredits[matchedIndex] = {
      ...nextCredits[matchedIndex],
      ...nextCredit,
    };
    return nextCredits;
  }
  nextCredits.unshift(nextCredit);
  return nextCredits;
}

function buildCanonicalRecordingCredit(library: LibraryData, role: Credit["role"], personId: string | undefined) {
  const normalizedPersonId = compactText(personId || "");
  if (!normalizedPersonId) {
    return null;
  }
  const person = getPersonById(library, normalizedPersonId);
  if (!person) {
    return null;
  }
  return {
    role,
    personId: person.id,
    displayName: person.name || person.nameLatin || person.id,
    label: "",
  } as Credit;
}

export function buildRecordingEntity(library: LibraryData, payload: OwnerEntityPayload, infoPanel: InfoPanel, timestamp: string) {
  const existing = payload.id ? library.recordings.find((item) => item.id === payload.id) : null;
  const workId = compactText(payload.workId || existing?.workId || "");
  let credits = Array.isArray(payload.credits) ? [...payload.credits] : existing?.credits ? [...existing.credits] : [];
  const canonicalConductorCredit = buildCanonicalRecordingCredit(library, "conductor", payload.conductorPersonId);
  const canonicalOrchestraCredit = buildCanonicalRecordingCredit(library, "orchestra", payload.orchestraPersonId);

  if (canonicalConductorCredit) {
    credits = upsertRecordingCredit(credits, canonicalConductorCredit);
  }
  if (canonicalOrchestraCredit) {
    credits = upsertRecordingCredit(credits, canonicalOrchestraCredit);
  }
  if (canonicalConductorCredit?.personId) {
    credits = credits.filter(
      (credit) =>
        !["soloist", "instrumentalist", "singer"].includes(credit.role) ||
        compactText(credit.personId || "") !== canonicalConductorCredit.personId,
    );
  }

  const baseRecording = {
    id: existing?.id || payload.id || "",
    workId,
    slug: compactText(existing?.slug || payload.slug || ""),
    title: compactText(payload.title || existing?.title || ""),
    workTypeHint: resolveRecordingWorkTypeHint(library, payload.workTypeHint || existing?.workTypeHint, workId),
    sortKey: compactText(existing?.sortKey || payload.sortKey || nextSortKey(library.recordings)),
    isPrimaryRecommendation: Boolean(payload.isPrimaryRecommendation),
    updatedAt: timestamp,
    images: payload.images || existing?.images || [],
    credits,
    links: payload.links || existing?.links || [],
    notes: compactText(payload.notes || existing?.notes || ""),
    performanceDateText: compactText(payload.performanceDateText || existing?.performanceDateText || ""),
    venueText: compactText(payload.venueText || existing?.venueText || ""),
    albumTitle: compactText(payload.albumTitle || existing?.albumTitle || ""),
    label: compactText(payload.label || existing?.label || ""),
    releaseDate: compactText(payload.releaseDate || existing?.releaseDate || ""),
    infoPanel,
  };

  const computedTitle = buildRecordingDisplayTitle(baseRecording, library) || compactText(payload.title || existing?.title || "") || "未命名版本";
  const existingIds = new Set((library.recordings || []).map((item) => item.id));
  const computedId =
    existing?.id ||
    payload.id ||
    ensureUniqueValue(
      createEntityId("recording", compactText(payload.slug || "") || computedTitle || payload.title || workId || "recording"),
      existingIds,
    );
  return {
    ...baseRecording,
    id: computedId,
    slug: compactText(existing?.slug || payload.slug || "") || createSlug(computedTitle),
    title: computedTitle,
  };
}

export function buildOwnerEntity(library: LibraryData, entityType: OwnerEditableEntityType, payload: OwnerEntityPayload) {
  const timestamp = new Date().toISOString();
  const infoPanel: InfoPanel = {
    text: compactText(payload.infoPanel?.text || ""),
    articleId: compactText(payload.infoPanel?.articleId || ""),
    collectionLinks: Array.isArray(payload.infoPanel?.collectionLinks) ? payload.infoPanel.collectionLinks : [],
  };
  const workSlugSource = buildWorkSlugSource(payload);

  if (entityType === "composer") {
    return {
      id: payload.id || createEntityId("composer", compactText(payload.slug || payload.name || "")),
      slug: compactText(payload.slug || "") || createSlug(payload.name || ""),
      name: compactText(payload.name || ""),
      nameLatin: compactText(payload.nameLatin || ""),
      country: parseCountryValues(payload.countries || payload.country)[0] || "",
      countries: parseCountryValues(payload.countries || payload.country),
      avatarSrc: compactText(payload.avatarSrc || ""),
      imageSourceUrl: compactText(payload.imageSourceUrl || ""),
      imageSourceKind: compactText(payload.imageSourceKind || ""),
      imageAttribution: compactText(payload.imageAttribution || ""),
      imageUpdatedAt: compactText(payload.imageUpdatedAt || ""),
      birthYear: parseNumber(payload.birthYear),
      deathYear: parseNumber(payload.deathYear),
      roles: payload.roles?.length ? payload.roles : ["composer"],
      aliases: parseList(payload.aliases),
      sortKey: compactText(payload.sortKey || "") || nextSortKey(library.composers),
      summary: compactText(payload.summary || ""),
      infoPanel,
    };
  }

  if (entityType === "person") {
    return {
      id: payload.id || createEntityId("person", compactText(payload.slug || payload.name || "")),
      slug: compactText(payload.slug || "") || createSlug(payload.name || ""),
      name: compactText(payload.name || ""),
      nameLatin: compactText(payload.nameLatin || ""),
      country: parseCountryValues(payload.countries || payload.country)[0] || "",
      countries: parseCountryValues(payload.countries || payload.country),
      avatarSrc: compactText(payload.avatarSrc || ""),
      imageSourceUrl: compactText(payload.imageSourceUrl || ""),
      imageSourceKind: compactText(payload.imageSourceKind || ""),
      imageAttribution: compactText(payload.imageAttribution || ""),
      imageUpdatedAt: compactText(payload.imageUpdatedAt || ""),
      birthYear: parseNumber(payload.birthYear),
      deathYear: parseNumber(payload.deathYear),
      roles: payload.roles?.length ? payload.roles : ["other"],
      aliases: parseList(payload.aliases),
      sortKey: compactText(payload.sortKey || "") || nextSortKey(library.people),
      summary: compactText(payload.summary || ""),
      infoPanel,
    };
  }

  if (entityType === "work") {
    const normalizedGroupPath = normalizeGroupPath(payload.groupPath || []);
    const groupIds = ensureWorkGroups(
      library,
      compactText(payload.composerId || ""),
      normalizedGroupPath.length ? normalizedGroupPath : inferWorkGroupPath(payload),
    );
    return {
      id: payload.id || createEntityId("work", compactText(payload.slug || workSlugSource || payload.title || "")),
      composerId: compactText(payload.composerId || ""),
      groupIds,
      slug: compactText(payload.slug || "") || createSlug(workSlugSource || payload.title || ""),
      title: compactText(payload.title || ""),
      titleLatin: compactText(payload.titleLatin || ""),
      aliases: parseList(payload.aliases),
      catalogue: compactText(payload.catalogue || ""),
      summary: compactText(payload.summary || ""),
      infoPanel,
      sortKey: compactText(payload.sortKey || "") || nextSortKey(library.works),
      updatedAt: timestamp,
    };
  }

  return buildRecordingEntity(library, payload, infoPanel, timestamp);
}

export function normalizeOwnerManagedLibrary(library: LibraryData): LibraryData {
  const nextLibrary = structuredClone(library) as LibraryData;
  const duplicateGroupIdMap = new Map<string, string>();
  const canonicalGroups: LibraryData["workGroups"] = [];
  const canonicalGroupIdByKey = new Map<string, string>();

  for (const group of nextLibrary.workGroups || []) {
    const normalizedPath = normalizeGroupPath(group.path);
    if (!normalizedPath.length) {
      continue;
    }
    const key = `${group.composerId}::${normalizedPath.join("/")}`;
    const existingId = canonicalGroupIdByKey.get(key);
    if (existingId) {
      duplicateGroupIdMap.set(group.id, existingId);
      continue;
    }
    canonicalGroupIdByKey.set(key, group.id);
    canonicalGroups.push({
      ...group,
      title: compactText(group.title || normalizedPath[normalizedPath.length - 1] || ""),
      path: normalizedPath,
      slug: compactText(group.slug || "") || createSlug(normalizedPath[normalizedPath.length - 1] || group.id),
    });
  }

  const canonicalGroupIds = new Set(canonicalGroups.map((group) => group.id));
  const referencedGroupIds = new Set<string>();
  nextLibrary.works = nextLibrary.works.map((work) => {
    const groupIds = uniqueStrings((work.groupIds || []).map((groupId) => duplicateGroupIdMap.get(groupId) || groupId)).filter((groupId) =>
      canonicalGroupIds.has(groupId),
    );
    groupIds.forEach((groupId) => referencedGroupIds.add(groupId));
    return {
      ...work,
      groupIds,
    };
  });

  nextLibrary.workGroups = canonicalGroups.filter((group) => referencedGroupIds.has(group.id));
  return nextLibrary;
}

function getNormalizedOwnerLibrary(library: LibraryData) {
  return validateLibrary(normalizeOwnerManagedLibrary(library));
}

function getManagedComposers(library: LibraryData) {
  const normalizedLibrary = getNormalizedOwnerLibrary(library);
  const composerMap = new Map<string, LibraryData["composers"][number]>();
  [...(normalizedLibrary.composers || []), ...((normalizedLibrary.people || []).filter((item) => item.roles.includes("composer")))].forEach((item) => {
    if (item.id && !composerMap.has(item.id)) {
      composerMap.set(item.id, item);
    }
  });
  return [...composerMap.values()];
}

function getManagedComposerById(library: LibraryData, composerId: string) {
  return getManagedComposers(library).find((item) => item.id === composerId) || null;
}

function entityCollectionByType(library: LibraryData, entityType: OwnerEditableEntityType) {
  if (entityType === "composer") {
    return getManagedComposers(library);
  }
  if (entityType === "person") {
    return library.people;
  }
  if (entityType === "work") {
    return library.works;
  }
  return library.recordings;
}

function getPersonGroupLabel(person: LibraryData["people"][number]) {
  if ((person.roles || []).some((role) => role === "orchestra" || role === "ensemble" || role === "chorus")) {
    return "团体";
  }
  if ((person.roles || []).includes("composer")) {
    return "作曲家";
  }
  return "人物";
}

function buildOwnerEntityRelation(library: LibraryData, entityType: OwnerEditableEntityType, entity: unknown): OwnerEntityRelation | null {
  if (!entity || typeof entity !== "object") {
    return null;
  }

  if (entityType === "composer") {
    const composer = entity as LibraryData["composers"][number];
    return {
      entityType,
      id: composer.id,
      label: [composer.name, composer.nameLatin].filter(Boolean).join(" / "),
      title: composer.name || composer.nameLatin || composer.id,
      subtitle: compactText(composer.country),
      groupLabel: "作曲家",
    };
  }

  if (entityType === "person") {
    const person = entity as LibraryData["people"][number];
    return {
      entityType,
      id: person.id,
      label: [person.name, person.nameLatin].filter(Boolean).join(" / "),
      title: person.name || person.nameLatin || person.id,
      subtitle: compactText(person.country),
      groupLabel: getPersonGroupLabel(person),
    };
  }

  if (entityType === "work") {
    const work = entity as LibraryData["works"][number];
    const composer = getManagedComposerById(library, work.composerId);
    return {
      entityType,
      id: work.id,
      label: [work.title, stripCatalogueFromWorkSegment(work.titleLatin || "", work.catalogue || ""), work.catalogue].filter(Boolean).join(" / "),
      title: work.title,
      subtitle: [composer?.name, composer?.nameLatin].filter(Boolean).join(" / "),
      groupLabel: "作品",
    };
  }

  const recording = entity as LibraryData["recordings"][number];
  const work = getWorkById(library, recording.workId);
  const composer = getManagedComposerById(library, work?.composerId || "");
  return {
    entityType,
    id: recording.id,
    label: buildRecordingDisplayTitle(recording, library),
    title: buildRecordingDisplayTitle(recording, library),
    subtitle: [composer?.name, work?.title, recording.performanceDateText, recording.venueText].filter(Boolean).join(" / "),
    groupLabel: "版本",
  };
}

export function collectOwnerEntityRelations(library: LibraryData, entityType: OwnerEditableEntityType, entityId: string): OwnerEntityRelation[] {
  const normalizedLibrary = getNormalizedOwnerLibrary(library);
  const results: OwnerEntityRelation[] = [];
  const seen = new Set<string>();
  const append = (nextType: OwnerEditableEntityType, nextEntity: unknown) => {
    const relation = buildOwnerEntityRelation(normalizedLibrary, nextType, nextEntity);
    if (!relation) {
      return;
    }
    const key = `${relation.entityType}:${relation.id}`;
    if (seen.has(key)) {
      return;
    }
    seen.add(key);
    results.push(relation);
  };

  if (entityType === "recording") {
    const recording = normalizedLibrary.recordings.find((item) => item.id === entityId);
    if (!recording) {
      return results;
    }
    const work = getWorkById(normalizedLibrary, recording.workId);
    if (work) {
      append("work", work);
    }
    for (const credit of recording.credits || []) {
      const personId = compactText(credit.personId || "");
      const person = personId ? getPersonById(normalizedLibrary, personId) : null;
      if (person) {
        append("person", person);
      }
    }
    return results;
  }

  if (entityType === "work") {
    const work = getWorkById(normalizedLibrary, entityId);
    if (!work) {
      return results;
    }
    const composerPerson = getPersonById(normalizedLibrary, work.composerId);
    const composer = getManagedComposerById(normalizedLibrary, work.composerId);
    append(composerPerson ? "person" : "composer", composerPerson || composer);
    normalizedLibrary.recordings.filter((item) => item.workId === entityId).forEach((item) => append("recording", item));
    return results;
  }

  if (entityType === "person" || entityType === "composer") {
    const person = getPersonById(normalizedLibrary, entityId);
    normalizedLibrary.recordings
      .filter((item) => (item.credits || []).some((credit) => compactText(credit.personId || "") === entityId))
      .forEach((item) => append("recording", item));
    if (person?.roles?.includes("composer") || entityType === "composer") {
      normalizedLibrary.works.filter((item) => item.composerId === entityId).forEach((item) => append("work", item));
    }
    return results;
  }

  return results;
}

export function assertOwnerEntityCanDelete(library: LibraryData, entityType: OwnerEditableEntityType, entityId: string) {
  const normalizedLibrary = getNormalizedOwnerLibrary(library);

  if (entityType === "recording") {
    return;
  }

  if (entityType === "work") {
    const dependentRecording = normalizedLibrary.recordings.find((item) => item.workId === entityId);
    if (dependentRecording) {
      throw new Error("该作品仍被版本“" + dependentRecording.title + "”引用，无法删除。");
    }
    return;
  }

  if (entityType === "person" || entityType === "composer") {
    const dependentRecording = normalizedLibrary.recordings.find((item) => (item.credits || []).some((credit) => compactText(credit.personId || "") === entityId));
    if (dependentRecording) {
      throw new Error("该人物仍被版本“" + dependentRecording.title + "”引用，无法删除。");
    }
    const person = getPersonById(normalizedLibrary, entityId);
    if (entityType === "composer" || person?.roles?.includes("composer")) {
      const dependentWork = normalizedLibrary.works.find((item) => item.composerId === entityId);
      if (dependentWork) {
        throw new Error("该作曲家仍拥有作品“" + dependentWork.title + "”，无法删除。");
      }
      const dependentGroup = normalizedLibrary.workGroups.find((item) => item.composerId === entityId);
      if (dependentGroup) {
        throw new Error("该作曲家仍拥有作品分组“" + dependentGroup.title + "”，无法删除。");
      }
    }
  }
}

export function removeOwnerEntity(library: LibraryData, entityType: OwnerEditableEntityType, entityId: string): LibraryData {
  const normalizedLibrary = structuredClone(getNormalizedOwnerLibrary(library));
  const collection = entityCollectionByType(normalizedLibrary, entityType);
  if (!collection.some((item) => item.id === entityId)) {
    throw new Error("Entity not found");
  }
  assertOwnerEntityCanDelete(normalizedLibrary, entityType, entityId);

  if (entityType === "recording") {
    normalizedLibrary.recordings = normalizedLibrary.recordings.filter((item) => item.id !== entityId);
    return getNormalizedOwnerLibrary(normalizedLibrary);
  }

  if (entityType === "work") {
    normalizedLibrary.works = normalizedLibrary.works.filter((item) => item.id !== entityId);
    return getNormalizedOwnerLibrary(normalizedLibrary);
  }

  if (entityType === "person") {
    normalizedLibrary.people = normalizedLibrary.people.filter((item) => item.id !== entityId);
    normalizedLibrary.composers = normalizedLibrary.composers.filter((item) => item.id !== entityId);
    return getNormalizedOwnerLibrary(normalizedLibrary);
  }

  normalizedLibrary.composers = normalizedLibrary.composers.filter((item) => item.id !== entityId);
  normalizedLibrary.people = normalizedLibrary.people.filter((item) => item.id !== entityId || !item.roles.includes("composer"));
  return getNormalizedOwnerLibrary(normalizedLibrary);
}
