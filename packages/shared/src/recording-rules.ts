export const recordingWorkTypeHintValues = ["orchestral", "concerto", "opera_vocal", "chamber_solo", "unknown"] as const;

export type RecordingWorkTypeHintValue = (typeof recordingWorkTypeHintValues)[number];
export type RecordingPresentationFamily = "orchestral" | "concerto" | "opera" | "solo" | "chamber" | "unknown";
export type RecordingDraftCreditRole = "conductor" | "orchestra" | "chorus" | "soloist" | "singer" | "ensemble";
export type RecordingDraftCredit = {
  role: RecordingDraftCreditRole;
  displayName: string;
  personId: "";
  label: "";
};

export type RecordingFamilyDerivationInput = {
  workTypeHint?: unknown;
  conductorCount?: number;
  orchestraCount?: number;
  soloistCount?: number;
  singerCount?: number;
  ensembleCount?: number;
};

export type RecordingBatchTemplateSpec = {
  fieldCount: number;
  parseNotes: string[];
};

export type RecordingWorkLike = {
  title?: unknown;
  titleLatin?: unknown;
  groupIds?: unknown[];
};

export type RecordingWorkGroupLike = {
  id?: unknown;
  title?: unknown;
  path?: unknown[];
};

function compact(value: unknown) {
  return String(value ?? "").trim();
}

function textIncludes(value: string, pattern: RegExp) {
  return pattern.test(value);
}

function collectWorkContextTexts(work?: RecordingWorkLike | null, workGroups: RecordingWorkGroupLike[] = []) {
  if (!work) {
    return [];
  }
  const groupIdSet = new Set((work.groupIds || []).map((groupId) => compact(groupId)).filter(Boolean));
  const relatedGroups = workGroups.filter((group) => groupIdSet.has(compact(group.id)));

  return [
    compact(work.title),
    compact(work.titleLatin),
    ...relatedGroups.flatMap((group) => [compact(group.title), ...(Array.isArray(group.path) ? group.path.map(compact) : [])]),
  ].filter(Boolean);
}

export function normalizeRecordingWorkTypeHintValue(value: unknown): RecordingWorkTypeHintValue {
  const normalized = compact(value).toLowerCase();
  return recordingWorkTypeHintValues.includes(normalized as RecordingWorkTypeHintValue)
    ? (normalized as RecordingWorkTypeHintValue)
    : "unknown";
}

export function getRecordingWorkTypeHintLabel(value: unknown) {
  const normalized = normalizeRecordingWorkTypeHintValue(value);
  if (normalized === "concerto") {
    return "协奏曲";
  }
  if (normalized === "opera_vocal") {
    return "歌剧与声乐";
  }
  if (normalized === "chamber_solo") {
    return "室内乐与独奏";
  }
  if (normalized === "orchestral") {
    return "管弦乐";
  }
  return "未分类";
}

export function inferRecordingWorkTypeHintFromTexts(values: unknown[]): RecordingWorkTypeHintValue {
  const text = values
    .map((value) => compact(value).toLowerCase())
    .filter(Boolean)
    .join(" ");

  if (!text) {
    return "unknown";
  }
  if (textIncludes(text, /(协奏曲|concerto|concertante)/i)) {
    return "concerto";
  }
  if (textIncludes(text, /(歌剧|声乐|清唱剧|弥撒|安魂曲|opera|vocal|oratorio|mass|requiem|cantata)/i)) {
    return "opera_vocal";
  }
  if (textIncludes(text, /(奏鸣曲|重奏|室内乐|独奏|无伴奏|变奏曲|小提琴奏鸣曲|钢琴奏鸣曲|sonata|quartet|quintet|trio|duo|chamber|solo|partita)/i)) {
    return "chamber_solo";
  }
  if (textIncludes(text, /(交响|交响诗|序曲|管弦|舞剧|芭蕾|组曲|symphon|orchestral|tone poem|overture|ballet|suite)/i)) {
    return "orchestral";
  }
  return "unknown";
}

export function inferRecordingWorkTypeHintFromWork(work?: RecordingWorkLike | null, workGroups: RecordingWorkGroupLike[] = []) {
  return inferRecordingWorkTypeHintFromTexts(collectWorkContextTexts(work, workGroups));
}

export function resolveRecordingWorkTypeHintValue(
  value: unknown,
  work?: RecordingWorkLike | null,
  workGroups: RecordingWorkGroupLike[] = [],
) {
  const normalized = normalizeRecordingWorkTypeHintValue(value);
  if (normalized !== "unknown") {
    return normalized;
  }
  return inferRecordingWorkTypeHintFromWork(work, workGroups);
}

function nonPlaceholder(value: string) {
  return compact(value) && compact(value) !== "-";
}

function splitBatchCreditNames(value: string) {
  return compact(value)
    .split(/\s*\+\s*/g)
    .map((item) => compact(item))
    .filter(Boolean);
}

function inferBatchEnsembleRole(value: string, fallbackRole: Extract<RecordingDraftCreditRole, "orchestra" | "chorus" | "ensemble">) {
  const normalized = compact(value).toLowerCase();
  if (!normalized) {
    return fallbackRole;
  }
  if (/(chorus|choir|合唱)/i.test(normalized)) {
    return "chorus";
  }
  if (/(orchestra|philharmonic|symphony|sinfonieorchester|philharmoniker|orkester|orquesta|orchestre|kapelle|zenekara|乐团|乐队)/i.test(normalized)) {
    return "orchestra";
  }
  if (/(ensemble|quartet|quintet|trio|duo|octet|nonet|重奏|组合)/i.test(normalized)) {
    return "ensemble";
  }
  return fallbackRole;
}

function looksLikeBatchEnsemble(value: string) {
  return inferBatchEnsembleRole(value, "ensemble") !== "ensemble" || /(ensemble|quartet|quintet|trio|duo|octet|nonet|重奏|组合)/i.test(compact(value));
}

export function deriveRecordingPresentationFamily({
  workTypeHint,
  conductorCount = 0,
  orchestraCount = 0,
  soloistCount = 0,
  singerCount = 0,
  ensembleCount = 0,
}: RecordingFamilyDerivationInput): RecordingPresentationFamily {
  const normalizedHint = normalizeRecordingWorkTypeHintValue(workTypeHint);
  const featuredPerformerCount = soloistCount + singerCount;

  if (normalizedHint === "concerto") {
    return "concerto";
  }
  if (normalizedHint === "opera_vocal") {
    return "opera";
  }
  if (normalizedHint === "chamber_solo") {
    return ensembleCount > 0 || featuredPerformerCount > 1 ? "chamber" : "solo";
  }
  if (normalizedHint === "orchestral") {
    return "orchestral";
  }

  if (conductorCount > 0 || orchestraCount > 0) {
    return "orchestral";
  }
  if (ensembleCount > 0 || featuredPerformerCount > 1) {
    return "chamber";
  }
  if (featuredPerformerCount === 1 && conductorCount === 0 && orchestraCount === 0) {
    return "solo";
  }
  return "unknown";
}

export function getBatchRecordingTemplateSpec(workTypeHint: unknown): RecordingBatchTemplateSpec {
  const normalizedHint = normalizeRecordingWorkTypeHintValue(workTypeHint);

  if (normalizedHint === "concerto") {
    return {
      fieldCount: 5,
      parseNotes: ["模板：独奏者 | 指挥 | 乐团 | 年份 | 链接列表"],
    };
  }
  if (normalizedHint === "opera_vocal") {
    return {
      fieldCount: 5,
      parseNotes: ["模板：指挥 | 主演/卡司 | 乐团/合唱 | 年份 | 链接列表"],
    };
  }
  if (normalizedHint === "chamber_solo") {
    return {
      fieldCount: 4,
      parseNotes: ["模板：主奏/组合 | 协作者/地点 | 年份 | 链接列表"],
    };
  }
  return {
    fieldCount: 4,
    parseNotes: ["模板：指挥 | 乐团 | 年份 | 链接列表"],
  };
}

export function buildBatchRecordingTitle(workTypeHint: unknown, slots: string[]) {
  const normalizedHint = normalizeRecordingWorkTypeHintValue(workTypeHint);

  if (normalizedHint === "concerto") {
    const [soloist, conductor, orchestra, year] = slots;
    return [soloist, conductor, orchestra, year].filter(nonPlaceholder).join(" - ");
  }
  if (normalizedHint === "opera_vocal") {
    const [conductor, cast, ensemble, year] = slots;
    return [conductor, cast, ensemble, year].filter(nonPlaceholder).join(" - ");
  }
  if (normalizedHint === "chamber_solo") {
    const [lead, collaboratorOrPlace, year] = slots;
    return [lead, collaboratorOrPlace, year].filter(nonPlaceholder).join(" - ");
  }

  const [conductor, orchestra, year] = slots;
  return [conductor, orchestra, year].filter(nonPlaceholder).join(" - ");
}

export function buildBatchRecordingCredits(workTypeHint: unknown, slots: string[]): RecordingDraftCredit[] {
  const normalizedHint = normalizeRecordingWorkTypeHintValue(workTypeHint);
  const credits: RecordingDraftCredit[] = [];
  const pushCredit = (role: RecordingDraftCreditRole, displayName: string) => {
    if (!nonPlaceholder(displayName)) {
      return;
    }
    credits.push({
      role,
      displayName: compact(displayName),
      personId: "",
      label: "",
    });
  };
  const pushSplitCredits = (
    fallbackRole: Extract<RecordingDraftCreditRole, "orchestra" | "chorus" | "ensemble" | "soloist" | "singer">,
    rawValue: string,
    { requireEnsembleLike = false }: { requireEnsembleLike?: boolean } = {},
  ) => {
    for (const item of splitBatchCreditNames(rawValue)) {
      if (requireEnsembleLike && !looksLikeBatchEnsemble(item)) {
        continue;
      }
      if (fallbackRole === "soloist" || fallbackRole === "singer") {
        pushCredit(fallbackRole, item);
        continue;
      }
      pushCredit(inferBatchEnsembleRole(item, fallbackRole), item);
    }
  };

  if (normalizedHint === "concerto") {
    pushSplitCredits("soloist", slots[0] || "");
    pushCredit("conductor", slots[1] || "");
    pushSplitCredits("orchestra", slots[2] || "");
    return credits;
  }
  if (normalizedHint === "opera_vocal") {
    pushCredit("conductor", slots[0] || "");
    pushSplitCredits("singer", slots[1] || "");
    pushSplitCredits("ensemble", slots[2] || "");
    return credits;
  }
  if (normalizedHint === "chamber_solo") {
    pushSplitCredits("soloist", slots[0] || "");
    pushSplitCredits("ensemble", slots[1] || "", { requireEnsembleLike: true });
    return credits;
  }

  pushCredit("conductor", slots[0] || "");
  pushSplitCredits("ensemble", slots[1] || "");
  return credits;
}
