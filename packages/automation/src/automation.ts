import type { LibraryData, MediaSourceKind } from "../../shared/src/schema.js";

import type { RecordingRetrievalProviderRuntimeState } from "./recording-retrieval.js";
export type AutomationEntityType = "composer" | "person" | "work" | "recording";
export type AutomationCheckCategory = "composer" | "conductor" | "orchestra" | "artist" | "work" | "recording";
export type AutomationRisk = "low" | "medium" | "high";
export type AutomationProposalStatus = "pending" | "applied" | "ignored";
export type AutomationProposalKind = "update" | "merge";
export type AutomationReviewState = "unseen" | "viewed" | "edited" | "confirmed" | "discarded";

export type AutomationFieldPatch = {
  path: string;
  before: unknown;
  after: unknown;
};

export type AutomationImageCandidate = {
  id: string;
  src: string;
  sourceUrl: string;
  sourceKind: MediaSourceKind | "other";
  attribution: string;
  width?: number;
  height?: number;
  title?: string;
  score?: number;
};

export type AutomationMergeCandidate = {
  targetId: string;
  targetLabel: string;
  reason: string;
};

export type AutomationProposalEvidence = {
  field: string;
  sourceUrl: string;
  sourceLabel: string;
  confidence: number;
  note?: string;
};

export type AutomationLinkCandidate = {
  platform: string;
  url: string;
  title?: string;
  sourceLabel?: string;
  confidence?: number;
};

export type AutomationProposal = {
  id: string;
  kind?: AutomationProposalKind;
  entityType: AutomationEntityType;
  entityId: string;
  summary: string;
  risk: AutomationRisk;
  status?: AutomationProposalStatus;
  reviewState?: AutomationReviewState;
  sources: string[];
  fields: AutomationFieldPatch[];
  warnings?: string[];
  imageCandidates?: AutomationImageCandidate[];
  mergeCandidates?: AutomationMergeCandidate[];
  selectedImageCandidateId?: string;
  evidence?: AutomationProposalEvidence[];
  linkCandidates?: AutomationLinkCandidate[];
};

export type AutomationSnapshot = {
  id: string;
  proposalId: string;
  entityType: AutomationEntityType;
  entityId: string;
  before: Record<string, unknown>;
  after: Record<string, unknown>;
  createdAt: string;
};

export type AutomationRun = {
  id: string;
  createdAt: string;
  categories: AutomationCheckCategory[];
  proposals: AutomationProposal[];
  snapshots: AutomationSnapshot[];
  notes: string[];
  provider?: RecordingRetrievalProviderRuntimeState;
  summary: {
    total: number;
    pending: number;
    applied: number;
    ignored: number;
  };
};

export type AutomationRunInput = {
  categories: AutomationCheckCategory[];
  proposals: AutomationProposal[];
  notes?: string[];
  provider?: RecordingRetrievalProviderRuntimeState;
};

export type ImageRankingRequest = {
  title: string;
  entityKind: "person" | "group" | "recording";
};

const suspiciousImagePattern =
  /(^|[^a-z])(logo|icon|badge|brand|site\s*logo|baike[-_ ]?logo|baidubaike|baidu[-_ ]?baike|baidu[-_ ]?logo|bd[-_ ]?logo|favicon|placeholder|sprite|default[-_ ]?image|no[-_ ]?image|wordmark|signature|autograph|watermark)([^a-z]|$)/i;

function cloneLibrary<T>(library: T): T {
  return structuredClone(library);
}

function uniqueStrings(values: Array<string | undefined | null>) {
  return [...new Set(values.map((value) => String(value ?? "").trim()).filter(Boolean))];
}

function stableSerialize(value: unknown): string {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value);
  }

  if (Array.isArray(value)) {
    return `[${value.map((item) => stableSerialize(item)).join(",")}]`;
  }

  return `{${Object.entries(value as Record<string, unknown>)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, item]) => `${JSON.stringify(key)}:${stableSerialize(item)}`)
    .join(",")}}`;
}

function normalizeSummaryKey(summary: string) {
  return summary.replace(/\s+/g, " ").trim().toLowerCase();
}

function mergeProposalFields(fields: AutomationFieldPatch[] = []) {
  return [...new Map(fields.map((field) => [field.path, field])).values()];
}

function mergeProposalImages(candidates: AutomationImageCandidate[] = []) {
  return [...new Map(candidates.map((candidate) => [candidate.id, candidate])).values()];
}

function mergeProposalMergeCandidates(candidates: AutomationMergeCandidate[] = []) {
  return [...new Map(candidates.map((candidate) => [candidate.targetId, candidate])).values()];
}

function mergeProposalEvidence(items: AutomationProposalEvidence[] = []) {
  return [
    ...new Map(items.map((item) => [`${item.field}|${item.sourceUrl}|${item.sourceLabel}`, item])).values(),
  ];
}

function mergeProposalLinks(items: AutomationLinkCandidate[] = []) {
  return [...new Map(items.map((item) => [item.url, item])).values()];
}

function getReviewStatePriority(reviewState: AutomationReviewState | undefined) {
  switch (reviewState) {
    case "confirmed":
      return 4;
    case "edited":
      return 3;
    case "viewed":
      return 2;
    case "discarded":
      return 1;
    default:
      return 0;
  }
}

function getSemanticProposalKey(proposal: AutomationProposal) {
  const baseKey = [proposal.kind ?? "update", proposal.entityType, proposal.entityId].join("|");

  if ((proposal.fields?.length ?? 0) > 0) {
    const fieldKey = mergeProposalFields(proposal.fields)
      .map((field) => `${field.path}=>${stableSerialize(field.after)}`)
      .sort((left, right) => left.localeCompare(right))
      .join("||");
    return `${baseKey}|fields|${fieldKey}`;
  }

  if ((proposal.mergeCandidates?.length ?? 0) > 0) {
    const mergeKey = mergeProposalMergeCandidates(proposal.mergeCandidates)
      .map((candidate) => `${candidate.targetId}=>${candidate.reason}`)
      .sort((left, right) => left.localeCompare(right))
      .join("||");
    return `${baseKey}|merge|${mergeKey}`;
  }

  if ((proposal.imageCandidates?.length ?? 0) > 0) {
    const imageKey = mergeProposalImages(proposal.imageCandidates)
      .map((candidate) => candidate.sourceUrl || candidate.src)
      .filter(Boolean)
      .sort((left, right) => left.localeCompare(right))
      .join("||");
    return `${baseKey}|images|${imageKey}`;
  }

  if ((proposal.linkCandidates?.length ?? 0) > 0) {
    const linkKey = mergeProposalLinks(proposal.linkCandidates)
      .map((candidate) => candidate.url)
      .sort((left, right) => left.localeCompare(right))
      .join("||");
    return `${baseKey}|links|${linkKey}`;
  }

  return `${baseKey}|summary|${normalizeSummaryKey(proposal.summary)}`;
}

function mergeAutomationProposal(existing: AutomationProposal, incoming: AutomationProposal): AutomationProposal {
  const nextReviewState =
    getReviewStatePriority(existing.reviewState) >= getReviewStatePriority(incoming.reviewState)
      ? existing.reviewState
      : incoming.reviewState;

  return {
    ...existing,
    summary: existing.summary.length >= incoming.summary.length ? existing.summary : incoming.summary,
    risk:
      existing.risk === "high" || incoming.risk === "high"
        ? "high"
        : existing.risk === "medium" || incoming.risk === "medium"
          ? "medium"
          : "low",
    status:
      existing.status === "applied" || incoming.status === "applied"
        ? "applied"
        : existing.status === "ignored" && incoming.status === "ignored"
          ? "ignored"
          : "pending",
    reviewState: nextReviewState ?? "unseen",
    sources: uniqueStrings([...(existing.sources || []), ...(incoming.sources || [])]),
    fields: mergeProposalFields([...(existing.fields || []), ...(incoming.fields || [])]),
    warnings: uniqueStrings([...(existing.warnings || []), ...(incoming.warnings || [])]),
    imageCandidates: mergeProposalImages([...(existing.imageCandidates || []), ...(incoming.imageCandidates || [])]),
    mergeCandidates: mergeProposalMergeCandidates([
      ...(existing.mergeCandidates || []),
      ...(incoming.mergeCandidates || []),
    ]),
    evidence: mergeProposalEvidence([...(existing.evidence || []), ...(incoming.evidence || [])]),
    linkCandidates: mergeProposalLinks([...(existing.linkCandidates || []), ...(incoming.linkCandidates || [])]),
    selectedImageCandidateId: existing.selectedImageCandidateId || incoming.selectedImageCandidateId || "",
  };
}

function summarizeAutomationRunCore(run: AutomationRun): AutomationRun {
  const pending = run.proposals.filter((proposal) => proposal.status === "pending").length;
  const applied = run.proposals.filter((proposal) => proposal.status === "applied").length;
  const ignored = run.proposals.filter((proposal) => proposal.status === "ignored").length;

  return {
    ...run,
    summary: {
      total: run.proposals.length,
      pending,
      applied,
      ignored,
    },
  };
}

export function normalizeAutomationProposals(proposals: AutomationProposal[] = []) {
  const mergedById = new Map<string, AutomationProposal>();
  const mergedBySemanticKey = new Map<string, AutomationProposal>();

  for (const proposal of proposals) {
    const normalizedProposal: AutomationProposal = {
      ...proposal,
      kind: proposal.kind ?? "update",
      status: proposal.status ?? "pending",
      reviewState: proposal.reviewState ?? "unseen",
      sources: [...(proposal.sources || [])],
      fields: [...(proposal.fields || [])],
      warnings: [...(proposal.warnings || [])],
      imageCandidates: [...(proposal.imageCandidates || [])],
      mergeCandidates: [...(proposal.mergeCandidates || [])],
      selectedImageCandidateId: proposal.selectedImageCandidateId || "",
      evidence: [...(proposal.evidence || [])],
      linkCandidates: [...(proposal.linkCandidates || [])],
    };
    const semanticKey = getSemanticProposalKey(normalizedProposal);
    const existing = mergedById.get(normalizedProposal.id) ?? mergedBySemanticKey.get(semanticKey);
    if (!existing) {
      const mergedProposal = {
        ...normalizedProposal,
        sources: uniqueStrings(normalizedProposal.sources),
        fields: mergeProposalFields(normalizedProposal.fields),
        warnings: uniqueStrings(normalizedProposal.warnings || []),
        imageCandidates: mergeProposalImages(normalizedProposal.imageCandidates),
        mergeCandidates: mergeProposalMergeCandidates(normalizedProposal.mergeCandidates),
        evidence: mergeProposalEvidence(normalizedProposal.evidence),
        linkCandidates: mergeProposalLinks(normalizedProposal.linkCandidates),
      };
      mergedById.set(normalizedProposal.id, mergedProposal);
      mergedBySemanticKey.set(semanticKey, mergedProposal);
      continue;
    }

    const mergedProposal = mergeAutomationProposal(existing, normalizedProposal);
    mergedById.set(existing.id, mergedProposal);
    mergedById.set(normalizedProposal.id, mergedProposal);
    mergedBySemanticKey.set(semanticKey, mergedProposal);
    mergedBySemanticKey.set(getSemanticProposalKey(mergedProposal), mergedProposal);
  }

  return [...new Set(mergedBySemanticKey.values())];
}

export function normalizeAutomationRun(run: AutomationRun): AutomationRun {
  return summarizeAutomationRunCore({
    ...run,
    proposals: normalizeAutomationProposals(run.proposals || []),
    notes: uniqueStrings(run.notes || []),
    snapshots: [...new Map((run.snapshots || []).map((snapshot) => [snapshot.id, snapshot])).values()],
  });
}

export function summarizeAutomationRun(run: AutomationRun): AutomationRun {
  return normalizeAutomationRun(run);
}

function findEntityCollection(library: LibraryData, entityType: AutomationEntityType) {
  if (entityType === "composer") {
    return library.composers;
  }
  if (entityType === "person") {
    return library.people;
  }
  if (entityType === "work") {
    return library.works;
  }
  return library.recordings;
}

function parsePathSegments(path: string) {
  return path
    .replace(/\[(\d+)\]/g, ".$1")
    .split(".")
    .map((segment) => segment.trim())
    .filter(Boolean)
    .map((segment) => (/^\d+$/.test(segment) ? Number(segment) : segment));
}

function setPath(target: Record<string, unknown>, path: string, value: unknown) {
  const segments = parsePathSegments(path);
  let current: unknown = target;

  for (let index = 0; index < segments.length - 1; index += 1) {
    const segment = segments[index];
    const nextSegment = segments[index + 1];

    if (typeof segment === "number") {
      if (!Array.isArray(current)) {
        throw new Error(`Path ${path} does not point to an array`);
      }
      current[segment] ??= typeof nextSegment === "number" ? [] : {};
      current = current[segment];
      continue;
    }

    const record = current as Record<string, unknown>;
    record[segment] ??= typeof nextSegment === "number" ? [] : {};
    current = record[segment];
  }

  const finalSegment = segments.at(-1);
  if (typeof finalSegment === "undefined") {
    return;
  }

  if (typeof finalSegment === "number") {
    if (!Array.isArray(current)) {
      throw new Error(`Path ${path} does not point to an array`);
    }
    current[finalSegment] = value;
    return;
  }

  (current as Record<string, unknown>)[finalSegment] = value;
}

export function createAutomationRun(_library: LibraryData, input: AutomationRunInput): AutomationRun {
  const createdAt = new Date().toISOString();
  return summarizeAutomationRun({
    id: `run-${createdAt.replace(/[:.]/g, "-")}`,
    createdAt,
    categories: [...input.categories],
    proposals: input.proposals.map((proposal) => ({
      ...proposal,
      kind: proposal.kind ?? "update",
      status: proposal.status ?? "pending",
      reviewState: proposal.reviewState ?? "unseen",
      warnings: proposal.warnings ?? [],
      imageCandidates: proposal.imageCandidates ?? [],
      mergeCandidates: proposal.mergeCandidates ?? [],
      selectedImageCandidateId: proposal.selectedImageCandidateId ?? "",
      evidence: proposal.evidence ?? [],
      linkCandidates: proposal.linkCandidates ?? [],
    })),
    snapshots: [],
    notes: input.notes ?? [],
    provider: input.provider,
    summary: {
      total: 0,
      pending: 0,
      applied: 0,
      ignored: 0,
    },
  });
}

export function collectAutomationProposalApplyBlockers(proposal: AutomationProposal) {
  if (proposal.kind === "merge") {
    return ["合并候选不能直接应用，请先人工处理关联关系。"];
  }
  const reasons: string[] = [];
  const hasDirectChanges = (proposal.fields?.length ?? 0) > 0 || (proposal.imageCandidates?.length ?? 0) > 0;
  if (!hasDirectChanges) {
    reasons.push("该候选没有可直接写入的字段或图片，只能人工复核。");
  }
  if (proposal.risk === "high") {
    reasons.push("高风险候选不能直接应用，请先人工复核。");
  }

  return [...new Set(reasons)];
}

export function canApplyAutomationProposal(proposal: AutomationProposal) {
  return collectAutomationProposalApplyBlockers(proposal).length === 0;
}

export function applyAutomationProposal(library: LibraryData, run: AutomationRun, proposalId: string) {
  const proposal = run.proposals.find((item) => item.id === proposalId);
  if (!proposal) {
    throw new Error(`Unknown proposal: ${proposalId}`);
  }
  if (!canApplyAutomationProposal(proposal)) {
    throw new Error(`Proposal cannot be applied: ${proposalId}`);
  }

  const nextLibrary = cloneLibrary(library);
  const collection = findEntityCollection(nextLibrary, proposal.entityType);
  const entity = collection.find((item) => item.id === proposal.entityId) as Record<string, unknown> | undefined;
  if (!entity) {
    throw new Error(`Unknown entity for proposal: ${proposal.entityId}`);
  }

  const before: Record<string, unknown> = {};
  const after: Record<string, unknown> = {};
  for (const field of proposal.fields) {
    before[field.path] = field.before;
    setPath(entity, field.path, field.after);
    after[field.path] = field.after;
  }

  const snapshot: AutomationSnapshot = {
    id: `snapshot-${proposal.id}-${Date.now()}`,
    proposalId: proposal.id,
    entityType: proposal.entityType,
    entityId: proposal.entityId,
    before,
    after,
    createdAt: new Date().toISOString(),
  };

  const nextRun = summarizeAutomationRun({
    ...run,
    proposals: run.proposals.map((item) => (item.id === proposal.id ? { ...item, status: "applied" } : item)),
    snapshots: [...run.snapshots, snapshot],
  });

  return {
    library: nextLibrary,
    run: nextRun,
    snapshot,
  };
}

export function applyPendingAutomationProposals(library: LibraryData, run: AutomationRun) {
  let nextLibrary = cloneLibrary(library);
  let nextRun = run;
  const snapshots: AutomationSnapshot[] = [];

  for (const proposal of nextRun.proposals) {
    if (proposal.status !== "pending" || !canApplyAutomationProposal(proposal)) {
      continue;
    }
    const applied = applyAutomationProposal(nextLibrary, nextRun, proposal.id);
    nextLibrary = applied.library;
    nextRun = applied.run;
    snapshots.push(applied.snapshot);
  }

  return {
    library: nextLibrary,
    run: nextRun,
    snapshots,
  };
}

export function revertAutomationProposal(library: LibraryData, run: AutomationRun, snapshotId: string) {
  const snapshot = run.snapshots.find((item) => item.id === snapshotId);
  if (!snapshot) {
    throw new Error(`Unknown snapshot: ${snapshotId}`);
  }

  const nextLibrary = cloneLibrary(library);
  const collection = findEntityCollection(nextLibrary, snapshot.entityType);
  const entity = collection.find((item) => item.id === snapshot.entityId) as Record<string, unknown> | undefined;
  if (!entity) {
    throw new Error(`Unknown entity for snapshot: ${snapshot.entityId}`);
  }

  for (const [path, value] of Object.entries(snapshot.before)) {
    setPath(entity, path, value);
  }

  return nextLibrary;
}

export function ignoreAutomationProposal(run: AutomationRun, proposalId: string) {
  return summarizeAutomationRun({
    ...run,
    proposals: run.proposals.map((proposal) =>
      proposal.id === proposalId ? { ...proposal, status: "ignored", reviewState: "discarded" } : proposal,
    ),
  });
}

export function ignorePendingAutomationProposals(run: AutomationRun) {
  return summarizeAutomationRun({
    ...run,
    proposals: run.proposals.map((proposal) =>
      proposal.status === "pending" ? { ...proposal, status: "ignored", reviewState: "discarded" } : proposal,
    ),
  });
}

export function updateAutomationProposalReview(
  run: AutomationRun,
  proposalId: string,
  reviewState: AutomationReviewState,
  selectedImageCandidateId?: string,
) {
  const reviewStatus =
    reviewState === "discarded"
      ? "ignored"
      : reviewState === "viewed" || reviewState === "edited" || reviewState === "confirmed"
        ? "pending"
        : undefined;

  return summarizeAutomationRun({
    ...run,
    proposals: run.proposals.map((proposal) =>
      proposal.id === proposalId
        ? {
            ...proposal,
            reviewState,
            status: proposal.status === "applied" ? "applied" : reviewStatus ?? proposal.status ?? "pending",
            selectedImageCandidateId:
              typeof selectedImageCandidateId === "string" ? selectedImageCandidateId : proposal.selectedImageCandidateId || "",
          }
        : proposal,
    ),
  });
}

export function isSuspiciousImageCandidate(candidate: AutomationImageCandidate) {
  const haystack = `${candidate.src} ${candidate.sourceUrl} ${candidate.attribution ?? ""} ${candidate.title ?? ""}`;
  return suspiciousImagePattern.test(haystack);
}

function scoreImageCandidate(request: ImageRankingRequest, candidate: AutomationImageCandidate) {
  const width = candidate.width ?? 0;
  const height = candidate.height ?? 0;
  const minDimension = Math.min(width, height);
  const aspectRatio = width && height ? width / height : 0;
  const squarePenalty = aspectRatio ? Math.abs(1 - aspectRatio) * 25 : 18;
  const sourceBoost =
    candidate.sourceKind === "wikimedia-commons"
      ? 24
      : candidate.sourceKind === "wikipedia" || candidate.sourceKind === "wikidata"
        ? 18
        : candidate.sourceKind === "streaming"
          ? 16
          : candidate.sourceKind === "official-site"
            ? 14
            : 4;
  const titleBoost = candidate.sourceUrl.toLowerCase().includes(request.title.toLowerCase().replace(/\s+/g, "-"))
    ? 8
    : candidate.title?.toLowerCase().includes(request.title.toLowerCase())
      ? 8
      : 0;
  const resolutionBoost = Math.min(minDimension / 40, 40);
  const attributionBoost = candidate.attribution ? 4 : 0;
  const watermarkPenalty = /watermark|sample|sprite/i.test(`${candidate.title ?? ""} ${candidate.sourceUrl}`) ? 16 : 0;
  const suspiciousPenalty = isSuspiciousImageCandidate(candidate) ? 48 : 0;

  return Math.max(0, sourceBoost + titleBoost + resolutionBoost + attributionBoost - squarePenalty - watermarkPenalty - suspiciousPenalty);
}

export function rankImageCandidates(request: ImageRankingRequest, candidates: AutomationImageCandidate[]) {
  return [...candidates]
    .map((candidate) => ({
      ...candidate,
      score: scoreImageCandidate(request, candidate),
    }))
    .sort((left, right) => (right.score ?? 0) - (left.score ?? 0));
}

