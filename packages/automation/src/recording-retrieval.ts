import { randomUUID } from "node:crypto";

import type { RecordingWorkTypeHintValue } from "../../shared/src/recording-rules.js";
import type {
  AutomationFieldPatch,
  AutomationImageCandidate,
  AutomationLinkCandidate,
  AutomationProposal,
  AutomationProposalEvidence,
} from "./automation.js";
import { detectPlatformFromUrl } from "../../data-core/src/resource-links.js";
import type { Credit, LibraryData, Recording } from "../../shared/src/schema.js";

export type RecordingWorkTypeHint = RecordingWorkTypeHintValue;
export type RecordingRetrievalRequestedField =
  | "links"
  | "performanceDateText"
  | "venueText"
  | "albumTitle"
  | "label"
  | "releaseDate"
  | "notes"
  | "images";
export type RecordingRetrievalProviderStatus =
  | "unavailable"
  | "queued"
  | "running"
  | "partial"
  | "succeeded"
  | "failed"
  | "timed_out"
  | "canceled";
export type RecordingRetrievalJobItemStatus = "queued" | "running" | "succeeded" | "partial" | "failed" | "not_found";

export type RecordingRetrievalSeed = {
  title: string;
  composerName: string;
  composerNameLatin: string;
  workTitle: string;
  workTitleLatin: string;
  catalogue: string;
  performanceDateText: string;
  venueText: string;
  albumTitle: string;
  label: string;
  releaseDate: string;
  credits: Credit[];
  links: Array<{ platform: string; url: string; title?: string }>;
  notes: string;
};

export type RecordingRetrievalItem = {
  itemId: string;
  recordingId: string;
  workId: string;
  composerId: string;
  workTypeHint: RecordingWorkTypeHint;
  sourceLine: string;
  seed: RecordingRetrievalSeed;
  requestedFields: RecordingRetrievalRequestedField[];
};

export type RecordingRetrievalRequest = {
  requestId: string;
  source: {
    kind: "owner-entity-check" | "owner-batch-check";
    ownerRunId?: string;
    batchSessionId?: string;
    requestedBy: "owner-tool";
  };
  items: RecordingRetrievalItem[];
  options: {
    maxConcurrency: number;
    timeoutMs: number;
    returnPartialResults: boolean;
  };
};

export type RecordingRetrievalProviderHealth = {
  service: string;
  version: string;
  protocolVersion: "v1";
  status: "ok";
};

export type RecordingRetrievalAcceptedJob = {
  jobId: string;
  requestId: string;
  status: "accepted";
  itemCount: number;
  acceptedAt: string;
};

export type RecordingRetrievalProgress = {
  total: number;
  completed: number;
  succeeded: number;
  partial: number;
  failed: number;
  notFound: number;
};

export type RecordingRetrievalLogEntry = {
  timestamp: string;
  level?: "info" | "warning" | "error";
  message: string;
  itemId?: string;
};

export type RecordingRetrievalJobStatusItem = {
  itemId: string;
  status: RecordingRetrievalJobItemStatus;
  message?: string;
};

export type RecordingRetrievalJobStatus = {
  jobId: string;
  requestId: string;
  status: RecordingRetrievalProviderStatus;
  progress: RecordingRetrievalProgress;
  items: RecordingRetrievalJobStatusItem[];
  logs: RecordingRetrievalLogEntry[];
  error?: string;
  completedAt?: string;
};

export type RecordingRetrievalEvidenceItem = {
  field: string;
  sourceUrl: string;
  sourceLabel: string;
  confidence: number;
  note?: string;
};

export type RecordingRetrievalLinkCandidate = {
  platform?: string;
  url: string;
  title?: string;
  sourceLabel?: string;
  confidence?: number;
};

export type RecordingRetrievalImageCandidate = {
  id?: string;
  src: string;
  sourceUrl?: string;
  sourceKind?: string;
  attribution?: string;
  title?: string;
  width?: number;
  height?: number;
};

export type RecordingRetrievalResultPayload = {
  performanceDateText?: string;
  venueText?: string;
  albumTitle?: string;
  label?: string;
  releaseDate?: string;
  notes?: string;
  links?: RecordingRetrievalLinkCandidate[];
  images?: RecordingRetrievalImageCandidate[];
};

export type RecordingRetrievalResultItem = {
  itemId: string;
  status: RecordingRetrievalJobItemStatus;
  confidence: number;
  warnings: string[];
  result: RecordingRetrievalResultPayload;
  evidence: RecordingRetrievalEvidenceItem[];
  linkCandidates: RecordingRetrievalLinkCandidate[];
  imageCandidates: RecordingRetrievalImageCandidate[];
  logs: RecordingRetrievalLogEntry[];
};

export type RecordingRetrievalResults = {
  jobId: string;
  requestId: string;
  status: Exclude<RecordingRetrievalProviderStatus, "queued" | "running" | "unavailable">;
  completedAt: string;
  items: RecordingRetrievalResultItem[];
};

export type RecordingRetrievalProviderRuntimeState = {
  providerName: string;
  providerJobId?: string;
  requestId: string;
  submittedAt?: string;
  lastSyncedAt?: string;
  phase: "submitting" | "running" | "partial" | "completed" | "failed";
  status: RecordingRetrievalProviderStatus;
  progress?: RecordingRetrievalProgress;
  logs: RecordingRetrievalLogEntry[];
  error?: string;
};

export type RecordingRetrievalExecution = {
  accepted: RecordingRetrievalAcceptedJob;
  status: RecordingRetrievalJobStatus;
  results: RecordingRetrievalResults;
  runtimeState: RecordingRetrievalProviderRuntimeState;
};

export type RecordingRetrievalProvider = {
  name: "recording-retrieval-service";
  protocolVersion: "v1";
  checkHealth(fetchImpl?: typeof fetch): Promise<RecordingRetrievalProviderHealth>;
  createJob(request: RecordingRetrievalRequest, fetchImpl?: typeof fetch): Promise<RecordingRetrievalAcceptedJob>;
  getJob(jobId: string, fetchImpl?: typeof fetch): Promise<RecordingRetrievalJobStatus>;
  getResults(jobId: string, fetchImpl?: typeof fetch): Promise<RecordingRetrievalResults>;
  cancelJob(jobId: string, fetchImpl?: typeof fetch): Promise<RecordingRetrievalJobStatus>;
};

export type RecordingRetrievalSourceOptions = {
  kind?: "owner-entity-check" | "owner-batch-check";
  ownerRunId?: string;
  batchSessionId?: string;
};

export type RecordingRetrievalSeedOverrides = Record<
  string,
  {
    sourceLine?: string;
    workTypeHint?: RecordingWorkTypeHint;
  }
>;

export type RecordingRetrievalRequestOptions = {
  source?: RecordingRetrievalSourceOptions;
  overrides?: RecordingRetrievalSeedOverrides;
  maxConcurrency?: number;
  timeoutMs?: number;
  returnPartialResults?: boolean;
};

export type ExecuteRecordingRetrievalJobOptions = {
  pollIntervalMs?: number;
  timeoutMs?: number;
  onStatus?: (state: RecordingRetrievalProviderRuntimeState) => void;
};

export function defaultRecordingRequestedFields(): RecordingRetrievalRequestedField[] {
  return ["links", "performanceDateText", "venueText", "albumTitle", "label", "releaseDate", "notes", "images"];
}

function isObject(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function compact(value: unknown) {
  return String(value ?? "").trim();
}

function normalizeWorkTypeHint(value: unknown): RecordingWorkTypeHint {
  const normalized = compact(value).toLowerCase();
  if (normalized === "orchestral" || normalized === "concerto" || normalized === "opera_vocal" || normalized === "chamber_solo") {
    return normalized;
  }
  return "unknown";
}

function normalizeCredits(credits: Credit[]) {
  return (credits || []).map((credit) => ({
    role: credit.role,
    personId: compact(credit.personId),
    displayName: compact(credit.displayName),
    label: compact(credit.label),
  }));
}

export function buildRecordingRetrievalRequest(
  library: LibraryData,
  recordings: Recording[],
  options: RecordingRetrievalRequestOptions = {},
): RecordingRetrievalRequest {
  const requestId = randomUUID();
  const items = recordings.map((recording) => {
    const work = library.works.find((item) => item.id === recording.workId);
    const composer = work ? library.composers.find((item) => item.id === work.composerId) : undefined;
    const override = options.overrides?.[recording.id];

    return {
      itemId: recording.id,
      recordingId: recording.id,
      workId: recording.workId,
      composerId: work?.composerId || "",
      workTypeHint: normalizeWorkTypeHint(override?.workTypeHint),
      sourceLine: compact(override?.sourceLine),
      seed: {
        title: recording.title,
        composerName: composer?.name || "",
        composerNameLatin: composer?.nameLatin || "",
        workTitle: work?.title || "",
        workTitleLatin: work?.titleLatin || "",
        catalogue: work?.catalogue || "",
        performanceDateText: recording.performanceDateText || "",
        venueText: recording.venueText || "",
        albumTitle: recording.albumTitle || "",
        label: recording.label || "",
        releaseDate: recording.releaseDate || "",
        credits: normalizeCredits(recording.credits),
        links: (recording.links || []).map((link) => ({
          platform: link.platform,
          url: link.url,
          title: compact(link.title),
        })),
        notes: recording.notes || "",
      },
      requestedFields: defaultRecordingRequestedFields(),
    } satisfies RecordingRetrievalItem;
  });

  return {
    requestId,
    source: {
      kind: options.source?.kind || "owner-entity-check",
      ownerRunId: compact(options.source?.ownerRunId),
      batchSessionId: compact(options.source?.batchSessionId),
      requestedBy: "owner-tool",
    },
    items,
    options: {
      maxConcurrency: options.maxConcurrency ?? 4,
      timeoutMs: options.timeoutMs ?? 180000,
      returnPartialResults: options.returnPartialResults ?? true,
    },
  };
}

function ensureResponseShape<T>(payload: unknown, guard: (value: unknown) => value is T, label: string) {
  if (!guard(payload)) {
    throw new Error(`${label} 响应格式不合法。`);
  }
  return payload;
}

function isHealth(value: unknown): value is RecordingRetrievalProviderHealth {
  return (
    isObject(value) &&
    compact(value.service) === "recording-retrieval-service" &&
    compact(value.protocolVersion) === "v1" &&
    compact(value.status) === "ok"
  );
}

function isAcceptedJob(value: unknown): value is RecordingRetrievalAcceptedJob {
  return isObject(value) && Boolean(compact(value.jobId)) && compact(value.requestId) !== "" && compact(value.status) === "accepted";
}

function isProgress(value: unknown): value is RecordingRetrievalProgress {
  return (
    isObject(value) &&
    typeof value.total === "number" &&
    typeof value.completed === "number" &&
    typeof value.succeeded === "number" &&
    typeof value.partial === "number" &&
    typeof value.failed === "number" &&
    typeof value.notFound === "number"
  );
}

function isLogEntry(value: unknown): value is RecordingRetrievalLogEntry {
  return isObject(value) && Boolean(compact(value.timestamp)) && Boolean(compact(value.message));
}

function isJobStatusItem(value: unknown): value is RecordingRetrievalJobStatusItem {
  return isObject(value) && Boolean(compact(value.itemId)) && Boolean(compact(value.status));
}

function isJobStatus(value: unknown): value is RecordingRetrievalJobStatus {
  return (
    isObject(value) &&
    Boolean(compact(value.jobId)) &&
    Boolean(compact(value.requestId)) &&
    Boolean(compact(value.status)) &&
    isProgress(value.progress) &&
    Array.isArray(value.items) &&
    value.items.every(isJobStatusItem) &&
    Array.isArray(value.logs) &&
    value.logs.every(isLogEntry)
  );
}

function isEvidence(value: unknown): value is RecordingRetrievalEvidenceItem {
  return (
    isObject(value) &&
    Boolean(compact(value.field)) &&
    Boolean(compact(value.sourceUrl)) &&
    Boolean(compact(value.sourceLabel)) &&
    typeof value.confidence === "number"
  );
}

function isLinkCandidate(value: unknown): value is RecordingRetrievalLinkCandidate {
  return isObject(value) && Boolean(compact(value.url));
}

function isImageCandidate(value: unknown): value is RecordingRetrievalImageCandidate {
  return isObject(value) && Boolean(compact(value.src));
}

function isResultPayload(value: unknown): value is RecordingRetrievalResultPayload {
  return isObject(value);
}

function isResultItem(value: unknown): value is RecordingRetrievalResultItem {
  return (
    isObject(value) &&
    Boolean(compact(value.itemId)) &&
    Boolean(compact(value.status)) &&
    typeof value.confidence === "number" &&
    Array.isArray(value.warnings) &&
    value.warnings.every((item) => typeof item === "string") &&
    isResultPayload(value.result) &&
    Array.isArray(value.evidence) &&
    value.evidence.every(isEvidence) &&
    Array.isArray(value.linkCandidates) &&
    value.linkCandidates.every(isLinkCandidate) &&
    Array.isArray(value.imageCandidates) &&
    value.imageCandidates.every(isImageCandidate) &&
    Array.isArray(value.logs) &&
    value.logs.every(isLogEntry)
  );
}

function isResults(value: unknown): value is RecordingRetrievalResults {
  return (
    isObject(value) &&
    Boolean(compact(value.jobId)) &&
    Boolean(compact(value.requestId)) &&
    Boolean(compact(value.status)) &&
    Boolean(compact(value.completedAt)) &&
    Array.isArray(value.items) &&
    value.items.every(isResultItem)
  );
}

async function parseJsonResponse(response: Response, label: string) {
  if (!response.ok) {
    const body = await response.text().catch(() => "");
    throw new Error(`${label} 失败：${response.status} ${body}`.trim());
  }
  return response.json().catch(() => {
    throw new Error(`${label} 返回了无法解析的 JSON。`);
  });
}

export function createHttpRecordingRetrievalProvider(options: { baseUrl: string }): RecordingRetrievalProvider {
  const baseUrl = String(options.baseUrl || "").replace(/\/+$/, "");
  const request = async (pathname: string, init: RequestInit | undefined, fetchImpl: typeof fetch) => {
    const response = await fetchImpl(`${baseUrl}${pathname}`, {
      ...init,
      headers: {
        "content-type": "application/json",
        ...(init?.headers || {}),
      },
    });
    return response;
  };

  return {
    name: "recording-retrieval-service",
    protocolVersion: "v1",
    async checkHealth(fetchImpl = fetch) {
      const payload = await parseJsonResponse(await request("/health", undefined, fetchImpl), "health");
      return ensureResponseShape(payload, isHealth, "health");
    },
    async createJob(jobRequest, fetchImpl = fetch) {
      const payload = await parseJsonResponse(
        await request("/v1/jobs", { method: "POST", body: JSON.stringify(jobRequest) }, fetchImpl),
        "createJob",
      );
      return ensureResponseShape(payload, isAcceptedJob, "createJob");
    },
    async getJob(jobId, fetchImpl = fetch) {
      const payload = await parseJsonResponse(await request(`/v1/jobs/${encodeURIComponent(jobId)}`, undefined, fetchImpl), "getJob");
      return ensureResponseShape(payload, isJobStatus, "getJob");
    },
    async getResults(jobId, fetchImpl = fetch) {
      const payload = await parseJsonResponse(
        await request(`/v1/jobs/${encodeURIComponent(jobId)}/results`, undefined, fetchImpl),
        "getResults",
      );
      return ensureResponseShape(payload, isResults, "getResults");
    },
    async cancelJob(jobId, fetchImpl = fetch) {
      const payload = await parseJsonResponse(
        await request(`/v1/jobs/${encodeURIComponent(jobId)}/cancel`, { method: "POST" }, fetchImpl),
        "cancelJob",
      );
      return ensureResponseShape(payload, isJobStatus, "cancelJob");
    },
  };
}

function toRuntimeState(
  requestId: string,
  providerName: string,
  phase: RecordingRetrievalProviderRuntimeState["phase"],
  status: RecordingRetrievalProviderStatus,
  accepted?: RecordingRetrievalAcceptedJob,
  state?: RecordingRetrievalJobStatus,
  error?: string,
): RecordingRetrievalProviderRuntimeState {
  return {
    providerName,
    providerJobId: accepted?.jobId || state?.jobId,
    requestId,
    submittedAt: accepted?.acceptedAt,
    lastSyncedAt: new Date().toISOString(),
    phase,
    status,
    progress: state?.progress,
    logs: state?.logs || [],
    error,
  };
}

export async function executeRecordingRetrievalJob(
  provider: RecordingRetrievalProvider,
  request: RecordingRetrievalRequest,
  fetchImpl: typeof fetch = fetch,
  options: ExecuteRecordingRetrievalJobOptions = {},
): Promise<RecordingRetrievalExecution> {
  const emit = (state: RecordingRetrievalProviderRuntimeState) => options.onStatus?.(state);
  try {
    const health = await provider.checkHealth(fetchImpl);
    if (health.protocolVersion !== "v1") {
      throw new Error(`版本自动检索工具协议版本不匹配：${health.protocolVersion}`);
    }
  } catch (error) {
    const state = toRuntimeState(request.requestId, provider.name, "failed", "unavailable", undefined, undefined, String(error));
    emit(state);
    throw new Error(`版本自动检索工具不可用：${error instanceof Error ? error.message : String(error)}`);
  }

  emit(toRuntimeState(request.requestId, provider.name, "submitting", "queued"));
  const accepted = await provider.createJob(request, fetchImpl);
  const startedAt = Date.now();
  const timeoutMs = options.timeoutMs ?? request.options.timeoutMs ?? 180000;
  const pollIntervalMs = options.pollIntervalMs ?? 1200;
  let lastStatus = await provider.getJob(accepted.jobId, fetchImpl);
  emit(
    toRuntimeState(
      request.requestId,
      provider.name,
      lastStatus.status === "partial" ? "partial" : "running",
      lastStatus.status,
      accepted,
      lastStatus,
      lastStatus.error,
    ),
  );

  while (["queued", "running"].includes(lastStatus.status)) {
    if (Date.now() - startedAt > timeoutMs) {
      try {
        await provider.cancelJob(accepted.jobId, fetchImpl);
      } catch {
        // ignore cancel failure on timeout path
      }
      const state = toRuntimeState(request.requestId, provider.name, "failed", "timed_out", accepted, lastStatus, "外部检索超时。");
      emit(state);
      throw new Error("版本自动检索超时。");
    }
    await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
    lastStatus = await provider.getJob(accepted.jobId, fetchImpl);
    emit(
      toRuntimeState(
        request.requestId,
        provider.name,
        lastStatus.status === "partial" ? "partial" : "running",
        lastStatus.status,
        accepted,
        lastStatus,
        lastStatus.error,
      ),
    );
  }

  const results = await provider.getResults(accepted.jobId, fetchImpl);
  const seen = new Set<string>();
  for (const item of results.items) {
    if (!item.itemId || seen.has(item.itemId)) {
      throw new Error("版本自动检索工具返回了缺失或重复的 itemId，已拒收整批结果。");
    }
    seen.add(item.itemId);
  }
  const runtimeState = toRuntimeState(
    request.requestId,
    provider.name,
    results.status === "failed" || results.status === "timed_out" || results.status === "canceled" ? "failed" : "completed",
    results.status,
    accepted,
    lastStatus,
    lastStatus.error,
  );
  emit(runtimeState);
  return {
    accepted,
    status: lastStatus,
    results,
    runtimeState,
  };
}

function dedupeByUrl<T extends { url?: string; src?: string }>(items: T[], key: "url" | "src") {
  const seen = new Set<string>();
  return items.filter((item) => {
    const raw = compact(item[key]);
    if (!raw) {
      return false;
    }
    const value = raw.toLowerCase();
    if (seen.has(value)) {
      return false;
    }
    seen.add(value);
    return true;
  });
}

function uniqueSourceList(item: RecordingRetrievalResultItem) {
  return [
    ...new Set(
      [
        ...item.evidence.map((evidence) => evidence.sourceUrl),
        ...item.linkCandidates.map((candidate) => candidate.url),
        ...(item.result.links || []).map((candidate) => candidate.url),
        ...item.imageCandidates.map((candidate) => compact(candidate.sourceUrl)),
        ...(item.result.images || []).map((candidate) => compact(candidate.sourceUrl)),
      ]
        .map(compact)
        .filter(Boolean),
    ),
  ];
}

function sameJson(left: unknown, right: unknown) {
  return JSON.stringify(left) === JSON.stringify(right);
}

function normalizeLinkCandidate(candidate: RecordingRetrievalLinkCandidate): AutomationLinkCandidate | null {
  const url = compact(candidate.url);
  if (!url) {
    return null;
  }
  return {
    platform: compact(candidate.platform) || detectPlatformFromUrl(url),
    url,
    title: compact(candidate.title),
    sourceLabel: compact(candidate.sourceLabel),
    confidence: typeof candidate.confidence === "number" ? candidate.confidence : undefined,
  };
}

function normalizeImageCandidate(candidate: RecordingRetrievalImageCandidate, index: number): AutomationImageCandidate | null {
  const src = compact(candidate.src);
  if (!src) {
    return null;
  }
  return {
    id: compact(candidate.id) || `image-${index + 1}`,
    src,
    sourceUrl: compact(candidate.sourceUrl) || src,
    sourceKind: (compact(candidate.sourceKind) || "other") as AutomationImageCandidate["sourceKind"],
    attribution: compact(candidate.attribution),
    title: compact(candidate.title),
    width: typeof candidate.width === "number" ? candidate.width : undefined,
    height: typeof candidate.height === "number" ? candidate.height : undefined,
  };
}

function buildScalarFieldPatches(recording: Recording, result: RecordingRetrievalResultPayload): AutomationFieldPatch[] {
  const patches: AutomationFieldPatch[] = [];
  for (const field of ["performanceDateText", "venueText", "albumTitle", "label", "releaseDate", "notes"] as const) {
    const nextValue = compact(result[field]);
    if (!nextValue || nextValue === compact(recording[field])) {
      continue;
    }
    patches.push({
      path: field,
      before: recording[field] || "",
      after: nextValue,
    });
  }
  return patches;
}

function mergeLinks(recording: Recording, result: RecordingRetrievalResultPayload) {
  const merged = dedupeByUrl(
    [
      ...(recording.links || []).map((link) => ({
        platform: link.platform,
        url: link.url,
        title: compact(link.title),
      })),
      ...((result.links || []).map((candidate) => ({
        platform: compact(candidate.platform) || detectPlatformFromUrl(candidate.url),
        url: compact(candidate.url),
        title: compact(candidate.title),
      })) || []),
    ],
    "url",
  ).map((item) => ({
    platform: item.platform || detectPlatformFromUrl(item.url || ""),
    url: item.url || "",
    title: item.title || "",
  }));
  return merged.filter((item) => item.url);
}

export function translateRecordingRetrievalResultsToProposals(
  library: LibraryData,
  execution: RecordingRetrievalExecution,
): AutomationProposal[] {
  const proposals: AutomationProposal[] = [];

  for (const item of execution.results.items) {
    if (!["succeeded", "partial"].includes(item.status)) {
      continue;
    }
    const recording = library.recordings.find((entry) => entry.id === item.itemId);
    if (!recording) {
      continue;
    }

    const fields: AutomationFieldPatch[] = buildScalarFieldPatches(recording, item.result);
    const mergedLinks = mergeLinks(recording, item.result);
    if (mergedLinks.length > 0 && !sameJson(mergedLinks, recording.links || [])) {
      fields.push({
        path: "links",
        before: recording.links || [],
        after: mergedLinks,
      });
    }

    const linkCandidates = dedupeByUrl(
      [...(item.linkCandidates || []), ...(item.result.links || [])]
        .map(normalizeLinkCandidate)
        .filter(Boolean) as AutomationLinkCandidate[],
      "url",
    );
    const imageCandidates = dedupeByUrl(
      [...(item.imageCandidates || []), ...(item.result.images || [])]
        .map((candidate, index) => normalizeImageCandidate(candidate, index))
        .filter(Boolean) as AutomationImageCandidate[],
      "src",
    );
    const evidence = (item.evidence || []).map(
      (entry) =>
        ({
          field: entry.field,
          sourceUrl: entry.sourceUrl,
          sourceLabel: entry.sourceLabel,
          confidence: entry.confidence,
          note: compact(entry.note),
        }) satisfies AutomationProposalEvidence,
    );

    if (fields.length === 0 && imageCandidates.length === 0) {
      continue;
    }

    proposals.push({
      id: `proposal-${item.itemId}`,
      entityType: "recording",
      entityId: recording.id,
      summary: `补充版本检索结果：${recording.title}`,
      risk: item.status === "partial" || item.confidence < 0.7 ? "medium" : "low",
      sources: uniqueSourceList(item),
      fields,
      warnings: item.warnings || [],
      evidence,
      linkCandidates,
      imageCandidates,
    });
  }

  return proposals;
}



