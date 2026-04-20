import { buildRecordingDisplayTitle, normalizeSearchText } from "../../shared/src/display.js";
import {
  deriveRecordingPresentationFamily,
  normalizeRecordingWorkTypeHintValue,
  resolveRecordingWorkTypeHintValue,
} from "../../shared/src/recording-rules.js";
import type { Composer, LibraryData, Person, Recording } from "../../shared/src/schema.js";
import type { ReviewQueueEntry } from "./library-store.js";
import { findCanonicalReplacementForGroupPerson, isPollutedGroupIdentity } from "./person-cleanup.js";

export type LibraryAuditSeverity = "info" | "warning" | "error";
export type LibraryAuditEntityType = "composer" | "person" | "work" | "recording";
export type LibraryAuditIssueCode =
  | "placeholder-entity"
  | "person-suspicious-ensemble-name"
  | "person-polluted-group-identity"
  | "recording-suspicious-metadata"
  | "recording-missing-credit-role"
  | "recording-work-type-conflict"
  | "recording-title-credit-mismatch";
export type LibraryAuditResolutionHint = "auto-fixable" | "manual-backfill";

export type LibraryAuditIssue = {
  code: LibraryAuditIssueCode;
  severity: LibraryAuditSeverity;
  entityType: LibraryAuditEntityType;
  entityId: string;
  message: string;
  source: string;
  suggestedFix: string;
  resolutionHint?: LibraryAuditResolutionHint;
  sourcePath?: string;
  details?: string[];
};

export type RecordingIssueHint = {
  resolutionHint: LibraryAuditResolutionHint;
  details?: string[];
  waivedMissingRoles?: string[];
};

export type LibraryAuditOptions = {
  reviewQueue?: ReviewQueueEntry[];
  recordingIssueHints?: Record<string, RecordingIssueHint>;
};

export type ManualBackfillQueueEntry = {
  entityId: string;
  issueCode: LibraryAuditIssueCode;
  composerId: string;
  composerName: string;
  workId: string;
  workTitle: string;
  recordingId: string;
  recordingTitle: string;
  workTypeHint: string;
  missingRoles: string[];
  sourcePath: string;
  details: string[];
};

export type ManualBackfillQueueGroup = {
  composerId: string;
  composerName: string;
  workId: string;
  workTitle: string;
  itemCount: number;
  entries: ManualBackfillQueueEntry[];
};

export type ManualBackfillReference = {
  total: number;
  groups: ManualBackfillQueueGroup[];
  entries: ManualBackfillQueueEntry[];
};

function compact(value: unknown) {
  return String(value ?? "").trim();
}

function compareText(left: string, right: string) {
  return left.localeCompare(right, "zh-Hans-CN-u-co-pinyin");
}

function isPlaceholderValue(value: unknown) {
  const normalized = compact(value).toLowerCase();
  return !normalized || normalized === "-" || normalized === "unknown" || normalized === "未知" || normalized === "未填写";
}

function isPlaceholderRecordingMetadataValue(value: unknown) {
  const normalized = compact(value).toLowerCase();
  return !normalized || normalized === "-" || normalized === "*" || normalized === "unknown" || normalized === "未知";
}

function hasPlaceholderIdentity(entity: Pick<Person | Composer, "id" | "name">) {
  return compact(entity.id) === "person-item" || isPlaceholderValue(entity.name);
}

function issue(input: LibraryAuditIssue): LibraryAuditIssue {
  return input;
}

function countCredits(recording: Pick<Recording, "credits">) {
  const credits = recording.credits || [];
  return {
    conductorCount: credits.filter((credit) => credit.role === "conductor").length,
    orchestraCount: credits.filter((credit) => credit.role === "orchestra").length,
    soloistCount: credits.filter((credit) => credit.role === "soloist" || credit.role === "instrumentalist").length,
    singerCount: credits.filter((credit) => credit.role === "singer").length,
    ensembleCount: credits.filter((credit) => credit.role === "ensemble" || credit.role === "chorus").length,
  };
}

function getRecordingWorkContext(library: LibraryData, recording: Recording) {
  const work = (library.works || []).find((item) => item.id === recording.workId) || null;
  const workGroups = (work?.groupIds || [])
    .map((groupId) => (library.workGroups || []).find((group) => group.id === groupId))
    .filter((group): group is LibraryData["workGroups"][number] => Boolean(group));
  return { work, workGroups };
}

function findReviewQueueSourcePath(
  reviewQueue: ReviewQueueEntry[] | undefined,
  entityType: LibraryAuditEntityType,
  entityId: string,
) {
  return (
    reviewQueue?.find((entry) => entry.entityType === entityType && entry.entityId === entityId && compact(entry.sourcePath))
      ?.sourcePath || ""
  );
}

export function getRecordingMissingCreditRoles(recording: Pick<Recording, "credits" | "workTypeHint">) {
  const counts = countCredits(recording);
  const family = deriveRecordingPresentationFamily({
    workTypeHint: recording.workTypeHint,
    ...counts,
  });
  const ensembleCount = counts.orchestraCount + counts.ensembleCount;
  const featuredCount = counts.soloistCount + counts.singerCount;
  const missingRoles: string[] = [];

  if (family === "orchestral") {
    if (counts.conductorCount === 0) {
      missingRoles.push("conductor");
    }
    if (ensembleCount === 0) {
      missingRoles.push("orchestra_or_ensemble");
    }
  } else if (family === "concerto") {
    if (featuredCount === 0) {
      missingRoles.push("soloist");
    }
    if (ensembleCount === 0) {
      missingRoles.push("orchestra_or_ensemble");
    }
  } else if (family === "opera") {
    if (counts.conductorCount === 0) {
      missingRoles.push("conductor");
    }
    if (featuredCount === 0) {
      missingRoles.push("singer_or_soloist");
    }
    if (ensembleCount === 0) {
      missingRoles.push("orchestra_or_ensemble");
    }
  } else if (family === "solo") {
    if (featuredCount === 0 && ensembleCount === 0) {
      missingRoles.push("soloist");
    }
  } else if (family === "chamber") {
    if (ensembleCount === 0 && featuredCount < 2) {
      missingRoles.push("ensemble_or_multiple_soloists");
    }
  }

  return missingRoles;
}

function buildRecordingIssueHint(recording: Recording, options: LibraryAuditOptions) {
  const sourcePath =
    findReviewQueueSourcePath(options.reviewQueue, "recording", recording.id) || compact(recording.legacyPath);
  const explicitHint = options.recordingIssueHints?.[recording.id];

  return {
    sourcePath,
    resolutionHint:
      explicitHint?.resolutionHint ||
      (sourcePath || compact(recording.legacyPath) ? ("auto-fixable" as const) : ("manual-backfill" as const)),
    details: explicitHint?.details || [],
    waivedMissingRoles: (explicitHint?.waivedMissingRoles || []).map((role) => compact(role)).filter(Boolean),
  };
}

function applyWaivedMissingRoles(missingRoles: string[], waivedMissingRoles: string[]) {
  if (!waivedMissingRoles.length) {
    return missingRoles;
  }
  const waived = new Set(waivedMissingRoles.map((role) => compact(role)));
  return missingRoles.filter((role) => !waived.has(compact(role)));
}

function auditPlaceholderEntities(library: LibraryData) {
  const issues: LibraryAuditIssue[] = [];

  for (const composer of library.composers || []) {
    if (!hasPlaceholderIdentity(composer)) {
      continue;
    }
    issues.push(
      issue({
        code: "placeholder-entity",
        severity: "error",
        entityType: "composer",
        entityId: composer.id,
        message: `作曲家条目仍是占位值：${compact(composer.name) || composer.id}`,
        source: "composers",
        suggestedFix: "将该占位作曲家替换为正式条目，或先解除所有引用后再删除。",
      }),
    );
  }

  for (const person of library.people || []) {
    if (!hasPlaceholderIdentity(person)) {
      continue;
    }
    issues.push(
      issue({
        code: "placeholder-entity",
        severity: "error",
        entityType: "person",
        entityId: person.id,
        message: `人物或团体条目仍是占位值：${compact(person.name) || person.id}`,
        source: "people",
        suggestedFix: "将该占位人物或团体迁移为正式实体，或在清理引用后删除。",
      }),
    );
  }

  for (const recording of library.recordings || []) {
    const placeholderCredit = (recording.credits || []).find(
      (credit) => compact(credit.personId) === "person-item" || isPlaceholderValue(credit.displayName),
    );
    if (!placeholderCredit) {
      continue;
    }
    issues.push(
      issue({
        code: "placeholder-entity",
        severity: "error",
        entityType: "recording",
        entityId: recording.id,
        message: `版本条目仍引用占位 credit：${compact(placeholderCredit.displayName) || compact(placeholderCredit.personId)}`,
        source: "recordings.credits",
        suggestedFix: "回读原始 archive 并为该 credit 绑定正式人物或团体条目。",
      }),
    );
  }

  return issues;
}

function hasCompositeEnsembleMarker(value: string) {
  const normalized = compact(value);
  return /(?:\s+\/\s+|\s+&\s+|\bcurrently\b|\([^)]+\))/.test(normalized);
}

function looksAmbiguousUppercaseAbbreviation(value: string) {
  return /\b[A-Z]{2,5}\b(?:\s*&\s*\b[A-Z]{1,5}\b)+/.test(compact(value));
}

function auditSuspiciousEnsemblePeople(library: LibraryData) {
  const issues: LibraryAuditIssue[] = [];

  for (const person of library.people || []) {
    const roles = new Set(person.roles || []);
    if (!roles.has("orchestra") && !roles.has("ensemble") && !roles.has("chorus")) {
      continue;
    }
    if (!hasCompositeEnsembleMarker(person.name) && !looksAmbiguousUppercaseAbbreviation(person.name)) {
      continue;
    }
    issues.push(
      issue({
        code: "person-suspicious-ensemble-name",
        severity: "warning",
        entityType: "person",
        entityId: person.id,
        message: `团体名称疑似混入复合署名或历史注记：${compact(person.name)}`,
        source: "people.name",
        suggestedFix: "优先拆分为多个结构化 credit；若只是历史别名或括注，则回绑到正式团体条目并降为 alias。",
      }),
    );
  }

  return issues;
}

function auditPollutedGroupIdentities(library: LibraryData) {
  const issues: LibraryAuditIssue[] = [];

  for (const person of library.people || []) {
    if (!isPollutedGroupIdentity(person)) {
      continue;
    }
    const replacement = findCanonicalReplacementForGroupPerson(library, person);
    issues.push(
      issue({
        code: "person-polluted-group-identity",
        severity: "warning",
        entityType: "person",
        entityId: person.id,
        message: `团体条目 identity 疑似混入 metadata 污染：${compact(person.slug) || compact(person.name)}`,
        source: "people.slug",
        suggestedFix: replacement
          ? `rebind references to canonical group ${replacement.id} and remove the polluted duplicate entry`
          : "review the polluted group slug manually and normalize it before further cleanup",
      }),
    );
  }

  return issues;
}

function auditRecordingSuspiciousMetadata(library: LibraryData) {
  const issues: LibraryAuditIssue[] = [];

  for (const recording of library.recordings || []) {
    const performanceDateText = compact(recording.performanceDateText);
    const venueText = compact(recording.venueText);
    const details: string[] = [];

    if (isPlaceholderRecordingMetadataValue(recording.performanceDateText) && performanceDateText === "*") {
      details.push("performanceDateText contains placeholder '*'");
    } else if (performanceDateText && !/\d/.test(performanceDateText) && !venueText) {
      details.push(`performanceDateText looks like venue text: ${performanceDateText}`);
    }

    if (isPlaceholderRecordingMetadataValue(recording.venueText) && venueText === "*") {
      details.push("venueText contains placeholder '*'");
    }

    if (details.length === 0) {
      continue;
    }

    issues.push(
      issue({
        code: "recording-suspicious-metadata",
        severity: "warning",
        entityType: "recording",
        entityId: recording.id,
        message: `版本元数据存在可规范化的占位值或错位字段：${details.join("; ")}`,
        source: "recordings.performanceDateText",
        suggestedFix: "按当前录音清洗规则清空占位值，并将误写进 performanceDateText 的地点文本迁移到 venueText。",
      }),
    );
  }

  return issues;
}

function auditRecordingRequiredCredits(library: LibraryData, options: LibraryAuditOptions) {
  const issues: LibraryAuditIssue[] = [];

  for (const recording of library.recordings || []) {
    const counts = countCredits(recording);
    const family = deriveRecordingPresentationFamily({
      workTypeHint: recording.workTypeHint,
      ...counts,
    });
    const hint = buildRecordingIssueHint(recording, options);
    const missing = applyWaivedMissingRoles(getRecordingMissingCreditRoles(recording), hint.waivedMissingRoles);

    if (missing.length === 0) {
      continue;
    }

    issues.push(
      issue({
        code: "recording-missing-credit-role",
        severity: "error",
        entityType: "recording",
        entityId: recording.id,
        message: `版本缺少 ${family} 体裁所需的关键署名：${missing.join(", ")}`,
        source: "recordings.credits",
        suggestedFix: `补齐 ${missing.join(", ")} 对应的 credit，并优先从正式人物或团体条目中选择。`,
        resolutionHint: hint.resolutionHint,
        sourcePath: hint.sourcePath || undefined,
        details: hint.details?.length ? hint.details : undefined,
      }),
    );
  }

  return issues;
}

function auditRecordingWorkTypeConflicts(library: LibraryData) {
  const issues: LibraryAuditIssue[] = [];

  for (const recording of library.recordings || []) {
    const normalized = normalizeRecordingWorkTypeHintValue(recording.workTypeHint);
    if (normalized === "unknown") {
      continue;
    }
    const { work, workGroups } = getRecordingWorkContext(library, recording);
    const inferred = resolveRecordingWorkTypeHintValue("unknown", work, workGroups);
    if (inferred === "unknown" || inferred === normalized) {
      continue;
    }
    issues.push(
      issue({
        code: "recording-work-type-conflict",
        severity: "warning",
        entityType: "recording",
        entityId: recording.id,
        message: `版本体裁 ${normalized} 与所属作品推断体裁 ${inferred} 不一致。`,
        source: "recordings.workTypeHint",
        suggestedFix: "核对作品分组与版本体裁，必要时统一修正 workTypeHint 或作品分组。",
      }),
    );
  }

  return issues;
}

function looksStructuredTitle(title: string) {
  return /[|/-]/.test(title);
}

function auditRecordingTitleCreditMismatches(library: LibraryData) {
  const issues: LibraryAuditIssue[] = [];

  for (const recording of library.recordings || []) {
    const storedTitle = compact(recording.title);
    if (!storedTitle || !looksStructuredTitle(storedTitle)) {
      continue;
    }
    const normalizedStored = normalizeSearchText(storedTitle);
    const derivedTitle = compact(buildRecordingDisplayTitle(recording, library));
    const normalizedDerived = normalizeSearchText(derivedTitle);
    if (!normalizedStored || !normalizedDerived || normalizedStored === normalizedDerived) {
      continue;
    }
    issues.push(
      issue({
        code: "recording-title-credit-mismatch",
        severity: "warning",
        entityType: "recording",
        entityId: recording.id,
        message: `版本标题与结构化 credit 推导标题不一致：stored="${storedTitle}" derived="${derivedTitle}"`,
        source: "recordings.title",
        suggestedFix: "优先保留结构化 credits，按当前显示规则重建版本标题并人工复核。",
      }),
    );
  }

  return issues;
}

export function auditLibraryData(library: LibraryData, options: LibraryAuditOptions = {}): LibraryAuditIssue[] {
  return [
    ...auditPlaceholderEntities(library),
    ...auditSuspiciousEnsemblePeople(library),
    ...auditPollutedGroupIdentities(library),
    ...auditRecordingSuspiciousMetadata(library),
    ...auditRecordingRequiredCredits(library, options),
    ...auditRecordingWorkTypeConflicts(library),
    ...auditRecordingTitleCreditMismatches(library),
  ];
}

export function summarizeLibraryAuditIssues(issues: LibraryAuditIssue[]) {
  const byCode = Object.fromEntries(
    [...new Set(issues.map((entry) => entry.code))].sort().map((code) => [
      code,
      issues.filter((entry) => entry.code === code).length,
    ]),
  );
  const bySeverity = Object.fromEntries(
    [...new Set(issues.map((entry) => entry.severity))].sort().map((severity) => [
      severity,
      issues.filter((entry) => entry.severity === severity).length,
    ]),
  );
  const byEntityType = Object.fromEntries(
    [...new Set(issues.map((entry) => entry.entityType))].sort().map((entityType) => [
      entityType,
      issues.filter((entry) => entry.entityType === entityType).length,
    ]),
  );
  const byResolutionHint = Object.fromEntries(
    [...new Set(issues.map((entry) => entry.resolutionHint).filter(Boolean) as LibraryAuditResolutionHint[])]
      .sort()
      .map((resolutionHint) => [
        resolutionHint,
        issues.filter((entry) => entry.resolutionHint === resolutionHint).length,
      ]),
  );

  return {
    total: issues.length,
    byCode,
    bySeverity,
    byEntityType,
    byResolutionHint,
  };
}

export function buildManualBackfillQueue(
  library: LibraryData,
  issues: LibraryAuditIssue[],
): ManualBackfillQueueEntry[] {
  const composerMap = new Map(library.composers.map((composer) => [composer.id, composer]));
  const workMap = new Map(library.works.map((work) => [work.id, work]));
  const entries: ManualBackfillQueueEntry[] = [];

  for (const auditIssue of issues) {
    if (auditIssue.resolutionHint !== "manual-backfill" || auditIssue.entityType !== "recording") {
      continue;
    }
    if (auditIssue.code !== "recording-missing-credit-role") {
      continue;
    }

    const recording = library.recordings.find((entry) => entry.id === auditIssue.entityId);
    if (!recording) {
      continue;
    }
    const work = workMap.get(recording.workId);
    if (!work) {
      continue;
    }
    const composer = composerMap.get(work.composerId);
    if (!composer) {
      continue;
    }

    entries.push({
      entityId: auditIssue.entityId,
      issueCode: auditIssue.code,
      composerId: composer.id,
      composerName: compact(composer.name),
      workId: work.id,
      workTitle: compact(work.title),
      recordingId: recording.id,
      recordingTitle: compact(buildRecordingDisplayTitle(recording, library)) || compact(recording.title),
      workTypeHint: normalizeRecordingWorkTypeHintValue(recording.workTypeHint),
      missingRoles: getRecordingMissingCreditRoles(recording),
      sourcePath: compact(auditIssue.sourcePath) || compact(recording.legacyPath),
      details: auditIssue.details || [],
    });
  }

  return entries.sort((left, right) => {
    return (
      compareText(left.composerName, right.composerName) ||
      compareText(left.workTitle, right.workTitle) ||
      compareText(left.recordingTitle, right.recordingTitle) ||
      compareText(left.entityId, right.entityId)
    );
  });
}

export function groupManualBackfillQueue(entries: ManualBackfillQueueEntry[]): ManualBackfillQueueGroup[] {
  const groups = new Map<string, ManualBackfillQueueGroup>();

  for (const entry of entries) {
    const key = `${entry.composerId}::${entry.workId}`;
    const existing = groups.get(key);
    if (existing) {
      existing.entries.push(entry);
      existing.itemCount += 1;
      continue;
    }
    groups.set(key, {
      composerId: entry.composerId,
      composerName: entry.composerName,
      workId: entry.workId,
      workTitle: entry.workTitle,
      itemCount: 1,
      entries: [entry],
    });
  }

  return [...groups.values()]
    .map((group) => ({
      ...group,
      entries: [...group.entries].sort((left, right) => compareText(left.recordingTitle, right.recordingTitle)),
    }))
    .sort((left, right) => compareText(left.composerName, right.composerName) || compareText(left.workTitle, right.workTitle));
}

export function buildManualBackfillReference(entries: ManualBackfillQueueEntry[]): ManualBackfillReference {
  return {
    total: entries.length,
    groups: groupManualBackfillQueue(entries),
    entries,
  };
}
