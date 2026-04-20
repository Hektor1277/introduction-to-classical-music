import type { LibraryData, Recording } from "../../shared/src/schema.js";
import type { RecordingRetrievalProviderStatus } from "./recording-retrieval.js";

export type RecordingRetrievalAuditGroupKey =
  | "missingAlbumTitle"
  | "missingLabel"
  | "missingReleaseDate"
  | "missingImages";

export type RecordingRetrievalAuditGroup = {
  key: RecordingRetrievalAuditGroupKey;
  label: string;
  totalCandidates: number;
  selectedRecordingIds: string[];
};

export type RecordingRetrievalAuditTarget = {
  recordingId: string;
  title: string;
  groupKeys: RecordingRetrievalAuditGroupKey[];
};

export type RecordingRetrievalAuditPlan = {
  groups: RecordingRetrievalAuditGroup[];
  targets: RecordingRetrievalAuditTarget[];
  totalCandidates: number;
  totalTargets: number;
};

export type RecordingRetrievalAuditResult = {
  recordingId: string;
  title: string;
  groupKeys: RecordingRetrievalAuditGroupKey[];
  providerStatus: RecordingRetrievalProviderStatus;
  reviewStatus: "ok" | "needs-attention" | "already-complete";
  proposalCount: number;
  proposalFields: string[];
  warnings: string[];
  issues: string[];
};

export type RecordingRetrievalAuditGroupSummary = {
  key: RecordingRetrievalAuditGroupKey;
  label: string;
  sampleCount: number;
  providerStatusCounts: Partial<Record<RecordingRetrievalProviderStatus, number>>;
  reviewStatusCounts: Partial<Record<RecordingRetrievalAuditResult["reviewStatus"], number>>;
  topFieldPaths: string[];
  topWarnings: string[];
};

export type RecordingRetrievalWarningCorpusEntry = {
  signature: string;
  count: number;
  examples: string[];
  reviewStatusCounts: Partial<Record<RecordingRetrievalAuditResult["reviewStatus"], number>>;
  groupKeys: RecordingRetrievalAuditGroupKey[];
};

export type RecordingRetrievalAuditSummary = {
  totalTargets: number;
  providerStatusCounts: Partial<Record<RecordingRetrievalProviderStatus, number>>;
  reviewStatusCounts: Partial<Record<RecordingRetrievalAuditResult["reviewStatus"], number>>;
  groups: RecordingRetrievalAuditGroupSummary[];
  warningCorpus: RecordingRetrievalWarningCorpusEntry[];
};

export type RecordingRetrievalAuditReport = {
  serviceBaseUrl: string;
  sampleSizePerGroup: number;
  requestTimeoutMs: number;
  executionTimeoutMs: number;
  plan: RecordingRetrievalAuditPlan;
  summary: RecordingRetrievalAuditSummary;
  samples: RecordingRetrievalAuditResult[];
};

type RecordingRetrievalAuditProposalLike = {
  fields?: Array<{ path: string }>;
  warnings?: string[];
};

type RecordingRetrievalAuditReviewLike = {
  status: RecordingRetrievalAuditResult["reviewStatus"];
  issues: string[];
};

const groupDefinitions: Array<{
  key: RecordingRetrievalAuditGroupKey;
  label: string;
  predicate: (recording: Recording) => boolean;
}> = [
  { key: "missingAlbumTitle", label: "缺专辑名", predicate: (recording) => !String(recording.albumTitle || "").trim() },
  { key: "missingLabel", label: "缺厂牌", predicate: (recording) => !String(recording.label || "").trim() },
  { key: "missingReleaseDate", label: "缺发行日期", predicate: (recording) => !String(recording.releaseDate || "").trim() },
  { key: "missingImages", label: "缺图片", predicate: (recording) => (recording.images?.length || 0) === 0 },
];

export function getRecordingRetrievalAuditGroupKeys(recording: Recording): RecordingRetrievalAuditGroupKey[] {
  return groupDefinitions.filter((definition) => definition.predicate(recording)).map((definition) => definition.key);
}

export function buildRecordingRetrievalAuditTarget(
  recording: Recording,
  groupKeys: RecordingRetrievalAuditGroupKey[] = getRecordingRetrievalAuditGroupKeys(recording),
): RecordingRetrievalAuditTarget {
  return {
    recordingId: recording.id,
    title: recording.title,
    groupKeys,
  };
}

function incrementCounter<T extends string>(counter: Partial<Record<T, number>>, key: T) {
  counter[key] = (counter[key] || 0) + 1;
}

function sortCounterKeys(counter: Record<string, number>) {
  return Object.entries(counter)
    .sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0]))
    .map(([key]) => key);
}

function uniqueStrings(values: Array<string | undefined>) {
  return [...new Set(values.map((value) => String(value ?? "").trim()).filter(Boolean))];
}

function formatCountMap(counter: Record<string, number> | Partial<Record<string, number>>) {
  const entries = Object.entries(counter).flatMap(([key, count]) => (typeof count === "number" && count > 0 ? [[key, count] as const] : []));
  if (entries.length === 0) {
    return "none";
  }
  return entries
    .sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0]))
    .map(([key, count]) => `${key}:${count}`)
    .join(", ");
}

function normalizeWarningSignature(warning: string) {
  return warning
    .replace(/第[一二三四五六七八九十百千万0-9]+条URL/g, "第#条URL")
    .replace(/\d+(?:(?:、|,|，|和|及|与)\d+)*号URL/g, "#号URL")
    .replace(/候选\d+(?:(?:-\d+)|(?:、|,|，|和|及|与)\d+)*/g, "候选#")
    .replace(/记录\d+(?:(?:、|,|，|和|及|与)\d+)*/g, "记录#")
    .replace(/URL\s*\d+/gi, "URL #")
    .replace(/\b\d{4}年/g, "<year>年")
    .replace(/多个B站视频标注为<year>年.+?音乐节，但转载来源或演奏者信息不明确，需谨慎验证/g, "多个B站视频标注为<year>年<festival>，但转载来源或演奏者信息不明确，需谨慎验证");
}

function buildWarningCorpus(results: RecordingRetrievalAuditResult[]): RecordingRetrievalWarningCorpusEntry[] {
  const corpus = new Map<
    string,
    {
      count: number;
      examples: string[];
      reviewStatusCounts: Partial<Record<RecordingRetrievalAuditResult["reviewStatus"], number>>;
      groupKeys: RecordingRetrievalAuditGroupKey[];
    }
  >();

  for (const result of results) {
    for (const warning of result.warnings) {
      const signature = normalizeWarningSignature(warning);
      const existing = corpus.get(signature) ?? {
        count: 0,
        examples: [],
        reviewStatusCounts: {},
        groupKeys: [],
      };
      existing.count += 1;
      existing.examples = uniqueStrings([...existing.examples, warning]);
      incrementCounter(existing.reviewStatusCounts, result.reviewStatus);
      existing.groupKeys = uniqueStrings([...existing.groupKeys, ...result.groupKeys]) as RecordingRetrievalAuditGroupKey[];
      corpus.set(signature, existing);
    }
  }

  return [...corpus.entries()]
    .map(([signature, entry]) => ({
      signature,
      count: entry.count,
      examples: entry.examples,
      reviewStatusCounts: entry.reviewStatusCounts,
      groupKeys: entry.groupKeys,
    }))
    .sort((left, right) => right.count - left.count || left.signature.localeCompare(right.signature));
}

export function formatRecordingRetrievalAuditMarkdown(report: RecordingRetrievalAuditReport) {
  const lines = [
    "# Recording Live Audit Report",
    "",
    "## Run Config",
    `- serviceBaseUrl: \`${report.serviceBaseUrl}\``,
    `- sampleSizePerGroup: \`${report.sampleSizePerGroup}\``,
    `- requestTimeoutMs: \`${report.requestTimeoutMs}\``,
    `- executionTimeoutMs: \`${report.executionTimeoutMs}\``,
    `- totalTargets: \`${report.summary.totalTargets}\``,
    `- providerStatusCounts: \`${formatCountMap(report.summary.providerStatusCounts)}\``,
    `- reviewStatusCounts: \`${formatCountMap(report.summary.reviewStatusCounts)}\``,
    "",
    "## Groups",
    ...report.summary.groups.map(
      (group) =>
        `- \`${group.key}\` ${group.label}: samples=${group.sampleCount}; review=${formatCountMap(group.reviewStatusCounts)}; provider=${formatCountMap(group.providerStatusCounts)}`,
    ),
    "",
    "## Warning Corpus",
    ...(report.summary.warningCorpus.length > 0
      ? report.summary.warningCorpus.map(
          (entry) =>
            `- \`${entry.signature}\` x${entry.count}; review=${formatCountMap(entry.reviewStatusCounts)}; groups=${entry.groupKeys.join(",")}; examples=${entry.examples.join(" | ")}`,
        )
      : ["- none"]),
    "",
    "## Samples",
    ...report.samples.flatMap((sample) => [
      `### \`${sample.recordingId}\` ${sample.reviewStatus}`,
      `- title: ${sample.title}`,
      `- groups: ${sample.groupKeys.join(", ") || "none"}`,
      `- providerStatus: ${sample.providerStatus}`,
      `- proposalCount: ${sample.proposalCount}`,
      `- proposalFields: ${sample.proposalFields.join(", ") || "none"}`,
      `- warnings: ${sample.warnings.join(" | ") || "none"}`,
      `- issues: ${sample.issues.join(" | ") || "none"}`,
      "",
    ]),
  ];

  return `${lines.join("\n").trim()}\n`;
}

export function buildRecordingRetrievalAuditPlan(library: LibraryData, options: { sampleSizePerGroup?: number } = {}): RecordingRetrievalAuditPlan {
  const sampleSizePerGroup = Math.max(1, options.sampleSizePerGroup ?? 3);
  const groups: RecordingRetrievalAuditGroup[] = [];
  const targetMap = new Map<string, RecordingRetrievalAuditTarget>();
  const assignmentCounts = new Map<string, number>();

  for (const definition of groupDefinitions) {
    const candidates = library.recordings.filter(definition.predicate);
    const selected = [...candidates]
      .sort((left, right) => {
        const leftCount = assignmentCounts.get(left.id) ?? 0;
        const rightCount = assignmentCounts.get(right.id) ?? 0;
        return leftCount - rightCount || left.sortKey.localeCompare(right.sortKey) || left.id.localeCompare(right.id);
      })
      .slice(0, sampleSizePerGroup);
    groups.push({
      key: definition.key,
      label: definition.label,
      totalCandidates: candidates.length,
      selectedRecordingIds: selected.map((recording) => recording.id),
    });

    for (const recording of selected) {
      const existing = targetMap.get(recording.id);
      if (existing) {
        existing.groupKeys.push(definition.key);
      } else {
        targetMap.set(recording.id, buildRecordingRetrievalAuditTarget(recording, [definition.key]));
      }
      assignmentCounts.set(recording.id, (assignmentCounts.get(recording.id) ?? 0) + 1);
    }
  }

  return {
    groups,
    targets: [...targetMap.values()],
    totalCandidates: groups.reduce((sum, group) => sum + group.totalCandidates, 0),
    totalTargets: targetMap.size,
  };
}

export function buildRecordingRetrievalAuditResult(input: {
  target: RecordingRetrievalAuditTarget;
  recording: Pick<Recording, "id" | "title">;
  providerStatus: RecordingRetrievalProviderStatus;
  providerError?: string;
  proposals: RecordingRetrievalAuditProposalLike[];
  review: RecordingRetrievalAuditReviewLike;
}): RecordingRetrievalAuditResult {
  const warnings = uniqueStrings([input.providerError, ...input.proposals.flatMap((proposal) => proposal.warnings || [])]);
  const issues = [...input.review.issues];
  let reviewStatus = input.review.status;

  if (
    ["failed", "timed_out", "unavailable", "canceled"].includes(input.providerStatus) ||
    (input.providerStatus === "partial" && input.proposals.length === 0)
  ) {
    reviewStatus = "needs-attention";
    issues.unshift(`外部检索状态为 ${input.providerStatus}，本轮抽样未得到可直接采纳的版本提案。`);
  }

  return {
    recordingId: input.recording.id,
    title: input.recording.title,
    groupKeys: input.target.groupKeys,
    providerStatus: input.providerStatus,
    reviewStatus,
    proposalCount: input.proposals.length,
    proposalFields: uniqueStrings(input.proposals.flatMap((proposal) => (proposal.fields || []).map((field) => field.path))),
    warnings,
    issues: uniqueStrings(issues),
  };
}

export function summarizeRecordingRetrievalAudit(results: RecordingRetrievalAuditResult[]): RecordingRetrievalAuditSummary {
  const providerStatusCounts: Partial<Record<RecordingRetrievalProviderStatus, number>> = {};
  const reviewStatusCounts: Partial<Record<RecordingRetrievalAuditResult["reviewStatus"], number>> = {};
  const groupBuckets = new Map<RecordingRetrievalAuditGroupKey, RecordingRetrievalAuditResult[]>();

  for (const result of results) {
    incrementCounter(providerStatusCounts, result.providerStatus);
    incrementCounter(reviewStatusCounts, result.reviewStatus);
    for (const key of result.groupKeys) {
      const bucket = groupBuckets.get(key) ?? [];
      bucket.push(result);
      groupBuckets.set(key, bucket);
    }
  }

  return {
    totalTargets: results.length,
    providerStatusCounts,
    reviewStatusCounts,
    warningCorpus: buildWarningCorpus(results),
    groups: groupDefinitions.map((definition) => {
      const bucket = groupBuckets.get(definition.key) ?? [];
      const groupProviderStatusCounts: Partial<Record<RecordingRetrievalProviderStatus, number>> = {};
      const groupReviewStatusCounts: Partial<Record<RecordingRetrievalAuditResult["reviewStatus"], number>> = {};
      const fieldCounter: Record<string, number> = {};
      const warningCounter: Record<string, number> = {};

      for (const result of bucket) {
        incrementCounter(groupProviderStatusCounts, result.providerStatus);
        incrementCounter(groupReviewStatusCounts, result.reviewStatus);
        for (const field of result.proposalFields) {
          fieldCounter[field] = (fieldCounter[field] || 0) + 1;
        }
        for (const warning of result.warnings) {
          warningCounter[warning] = (warningCounter[warning] || 0) + 1;
        }
      }

      return {
        key: definition.key,
        label: definition.label,
        sampleCount: bucket.length,
        providerStatusCounts: groupProviderStatusCounts,
        reviewStatusCounts: groupReviewStatusCounts,
        topFieldPaths: sortCounterKeys(fieldCounter),
        topWarnings: sortCounterKeys(warningCounter),
      } satisfies RecordingRetrievalAuditGroupSummary;
    }),
  };
}
