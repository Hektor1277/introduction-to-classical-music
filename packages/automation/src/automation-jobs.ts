import { createAutomationRun, normalizeAutomationProposals, type AutomationCheckCategory, type AutomationRun } from "./automation.js";
import {
  reviewAutomationProposalQuality,
  reviewRecordingAutomationProposalQuality,
  reviewWorkAutomationProposalQuality,
  runAutomationChecks,
  type AutomationCheckRequest,
  type RunAutomationChecksOptions,
} from "./automation-checks.js";
import { getDisplayData, getWebsiteDisplay } from "../../shared/src/display.js";
import { reviewAutomationProposalWithLlm, type LlmConfig } from "./llm.js";
import { mergeProposalReviewResults } from "./proposal-review.js";
import {
  buildRecordingRetrievalAuditResult,
  buildRecordingRetrievalAuditTarget,
  summarizeRecordingRetrievalAudit,
  type RecordingRetrievalAuditResult,
  type RecordingRetrievalAuditSummary,
} from "./recording-retrieval-audit.js";
import type { Composer, LibraryData, Person, Work } from "../../shared/src/schema.js";

export type AutomationJobStatus = "queued" | "preparing" | "running" | "completed" | "cancelled";
export type AutomationJobItemStatus =
  | "queued"
  | "running"
  | "succeeded"
  | "failed"
  | "skipped"
  | "completed-nochange"
  | "needs-attention";

export type AutomationJobError = {
  code:
    | "fetch-failed"
    | "selection-empty"
    | "job-cancelled"
    | "quality-failed"
    | "no-candidate"
    | "needs-attention"
    | "unknown";
  message: string;
  entityType?: string;
  entityId?: string;
};

export type AutomationJobSelectionItem = {
  category: AutomationCheckCategory;
  entityId: string;
  label: string;
  description: string;
};

export type AutomationJobSelectionGroup = {
  category: AutomationCheckCategory;
  items: AutomationJobSelectionItem[];
};

export type AutomationSelectionPreview = {
  total: number;
  groups: AutomationJobSelectionGroup[];
};

export type AutomationJobProgress = {
  total: number;
  processed: number;
  succeeded: number;
  failed: number;
  skipped: number;
  unchanged: number;
  attention: number;
};

export type AutomationJobEvent = {
  timestamp: string;
  phase: "prepare" | "selection" | "fetch" | "complete" | "error" | "review";
  message: string;
  entityType?: string;
  entityId?: string;
};

export type AutomationJobItemRecord = AutomationJobSelectionItem & {
  status: AutomationJobItemStatus;
  events: AutomationJobEvent[];
  errors: string[];
  runId?: string;
  reviewIssues?: string[];
  recordingAuditResult?: RecordingRetrievalAuditResult;
};

export type AutomationJobRecordingAudit = {
  results: RecordingRetrievalAuditResult[];
  summary: RecordingRetrievalAuditSummary;
};

export type AutomationJobRecord = {
  id: string;
  status: AutomationJobStatus;
  request: AutomationCheckRequest;
  progress: AutomationJobProgress;
  currentItem?: AutomationJobSelectionItem;
  currentItemIds: string[];
  items: AutomationJobItemRecord[];
  selectedItemId?: string;
  events: AutomationJobEvent[];
  errors: AutomationJobError[];
  selection: AutomationSelectionPreview;
  run?: AutomationRun;
  recordingAudit?: AutomationJobRecordingAudit;
  createdAt: string;
  completedAt?: string;
};

type CreateJobInput = {
  library: LibraryData;
  request: AutomationCheckRequest;
  fetchImpl?: typeof fetch;
  llmConfig?: LlmConfig;
  maxConcurrency?: number;
  runChecksImpl?: typeof runAutomationChecks;
  runChecksOptions?: RunAutomationChecksOptions;
  onCompleted?: (job: AutomationJobRecord) => Promise<void> | void;
};

function nowIso() {
  return new Date().toISOString();
}

function withMessage(error: unknown) {
  return error instanceof Error ? error.message : String(error);
}

function uniqueCategories(request: AutomationCheckRequest) {
  const categories = request.categories?.length ? request.categories : request.entityTypes?.length ? request.entityTypes : [];
  return [...new Set(categories)].filter(Boolean) as AutomationCheckCategory[];
}

function groupByCategory(items: AutomationJobSelectionItem[]) {
  const map = new Map<AutomationCheckCategory, AutomationJobSelectionItem[]>();
  for (const item of items) {
    const bucket = map.get(item.category) ?? [];
    bucket.push(item);
    map.set(item.category, bucket);
  }

  return [...map.entries()].map(([category, groupedItems]) => ({
    category,
    items: groupedItems,
  }));
}

function uniqueStrings(values: Array<string | undefined | null>) {
  return [...new Set(values.map((value) => String(value ?? "").trim()).filter(Boolean))];
}

function buildRecordingAuditGateMessage(summary: RecordingRetrievalAuditSummary) {
  const needsAttentionCount = summary.reviewStatusCounts["needs-attention"] || 0;
  const topGroups = summary.groups
    .filter((group) => (group.reviewStatusCounts["needs-attention"] || 0) > 0)
    .slice(0, 3)
    .map((group) => `${group.label} ${group.reviewStatusCounts["needs-attention"]}/${group.sampleCount}`)
    .join("；");
  const topWarnings = summary.groups
    .flatMap((group) => group.topWarnings || [])
    .filter(Boolean)
    .slice(0, 3)
    .join("；");
  const fragments = [
    `录音在线审计：${needsAttentionCount}/${summary.totalTargets} 条需要关注`,
    topGroups ? `高风险字段组：${topGroups}` : "",
    topWarnings ? `高频警告：${topWarnings}` : "",
  ].filter(Boolean);
  return fragments.join("；");
}

function mergeRunProposals(runs: AutomationRun[]) {
  return normalizeAutomationProposals(runs.flatMap((run) => run.proposals || []));
}

function pickPeople(
  library: LibraryData,
  predicate: (person: LibraryData["people"][number]) => boolean,
  explicitIds?: string[],
) {
  const idFilter = explicitIds?.length ? new Set(explicitIds) : null;
  return library.people.filter((person) => predicate(person) && (!idFilter || idFilter.has(person.id)));
}

function filterRecordingsByRequest(library: LibraryData, request: AutomationCheckRequest) {
  let selectedRecordings = library.recordings;

  if (request.recordingIds?.length) {
    const ids = new Set(request.recordingIds);
    selectedRecordings = selectedRecordings.filter((recording) => ids.has(recording.id));
  }
  if (request.workIds?.length) {
    const ids = new Set(request.workIds);
    selectedRecordings = selectedRecordings.filter((recording) => ids.has(recording.workId));
  }
  if (request.composerIds?.length) {
    const workIds = new Set(library.works.filter((work) => request.composerIds?.includes(work.composerId)).map((work) => work.id));
    selectedRecordings = selectedRecordings.filter((recording) => workIds.has(recording.workId));
  }
  if (request.conductorIds?.length) {
    const ids = new Set(request.conductorIds);
    selectedRecordings = selectedRecordings.filter((recording) =>
      recording.credits.some((credit) => credit.role === "conductor" && credit.personId && ids.has(credit.personId)),
    );
  }
  if (request.artistIds?.length) {
    const ids = new Set(request.artistIds);
    selectedRecordings = selectedRecordings.filter((recording) =>
      recording.credits.some((credit) => credit.personId && ids.has(credit.personId)),
    );
  }
  if (request.orchestraIds?.length) {
    const ids = new Set(request.orchestraIds);
    selectedRecordings = selectedRecordings.filter((recording) =>
      recording.credits.some((credit) => credit.role === "orchestra" && credit.personId && ids.has(credit.personId)),
    );
  }

  return selectedRecordings;
}

function filterWorksByRequest(library: LibraryData, request: AutomationCheckRequest) {
  let selectedWorks = library.works;

  if (request.workIds?.length) {
    const ids = new Set(request.workIds);
    selectedWorks = selectedWorks.filter((work) => ids.has(work.id));
  }

  if (request.composerIds?.length) {
    const composerIds = new Set(request.composerIds);
    selectedWorks = selectedWorks.filter((work) => composerIds.has(work.composerId));
  }

  if (request.recordingIds?.length) {
    const workIds = new Set(
      library.recordings.filter((recording) => request.recordingIds?.includes(recording.id)).map((recording) => recording.workId),
    );
    selectedWorks = selectedWorks.filter((work) => workIds.has(work.id));
  }

  if (request.conductorIds?.length || request.artistIds?.length || request.orchestraIds?.length) {
    const relatedRecordingWorkIds = new Set(filterRecordingsByRequest(library, request).map((recording) => recording.workId));
    selectedWorks = selectedWorks.filter((work) => relatedRecordingWorkIds.has(work.id));
  }

  return selectedWorks;
}

function buildRelatedEntitySets(library: LibraryData, request: AutomationCheckRequest) {
  const recordings = filterRecordingsByRequest(library, request);
  const workIds = new Set(recordings.map((recording) => recording.workId));
  const composerIds = new Set(
    library.works.filter((work) => workIds.has(work.id)).map((work) => work.composerId),
  );
  const conductorIds = new Set<string>();
  const orchestraIds = new Set<string>();
  const artistIds = new Set<string>();

  for (const recording of recordings) {
    for (const credit of recording.credits) {
      if (!credit.personId) {
        continue;
      }
      if (credit.role === "conductor") {
        conductorIds.add(credit.personId);
      } else if (credit.role === "orchestra") {
        orchestraIds.add(credit.personId);
      } else if (["soloist", "singer", "ensemble", "chorus", "instrumentalist"].includes(credit.role)) {
        artistIds.add(credit.personId);
      }
    }
  }

  return {
    recordings,
    workIds,
    composerIds,
    conductorIds,
    orchestraIds,
    artistIds,
  };
}

export function previewAutomationSelection(library: LibraryData, request: AutomationCheckRequest): AutomationSelectionPreview {
  const categories = uniqueCategories(request);
  const items: AutomationJobSelectionItem[] = [];
  const related = buildRelatedEntitySets(library, request);

  if (categories.includes("composer")) {
    const explicitIds = request.composerIds?.length ? new Set(request.composerIds) : null;
    const constrainedByRelations =
      related.recordings.length > 0 &&
      (request.workIds?.length ||
        request.recordingIds?.length ||
        request.conductorIds?.length ||
        request.artistIds?.length ||
        request.orchestraIds?.length);
    const selected = library.composers.filter((composer) => {
      if (explicitIds && !explicitIds.has(composer.id)) {
        return false;
      }
      if (constrainedByRelations) {
        return related.composerIds.has(composer.id);
      }
      return true;
    });
    items.push(
      ...selected.map((composer) => ({
        category: "composer" as const,
        entityId: composer.id,
        label: getWebsiteDisplay(composer).short || getWebsiteDisplay(composer).heading,
        description: getDisplayData(composer).latin || composer.country || "",
      })),
    );
  }

  if (categories.includes("conductor")) {
    items.push(
      ...pickPeople(
        library,
        (person) =>
          person.roles.includes("conductor") &&
          (!related.recordings.length ||
            !(
              request.composerIds?.length ||
              request.workIds?.length ||
              request.recordingIds?.length ||
              request.orchestraIds?.length ||
              request.artistIds?.length
            ) ||
            related.conductorIds.has(person.id)),
        request.conductorIds,
      ).map((person) => ({
        category: "conductor" as const,
        entityId: person.id,
        label: getWebsiteDisplay(person).short || getWebsiteDisplay(person).heading,
        description: getDisplayData(person).latin || person.country || "",
      })),
    );
  }

  if (categories.includes("orchestra")) {
    items.push(
      ...pickPeople(
        library,
        (person) =>
          person.roles.includes("orchestra") &&
          (!related.recordings.length ||
            !(
              request.composerIds?.length ||
              request.workIds?.length ||
              request.recordingIds?.length ||
              request.conductorIds?.length ||
              request.artistIds?.length
            ) ||
            related.orchestraIds.has(person.id)),
        request.orchestraIds,
      ).map((person) => ({
        category: "orchestra" as const,
        entityId: person.id,
        label: getWebsiteDisplay(person).short || getWebsiteDisplay(person).heading,
        description: [getDisplayData(person).abbreviations.join(" / "), getDisplayData(person).latin || person.country || ""]
          .filter(Boolean)
          .join(" / "),
      })),
    );
  }

  if (categories.includes("artist")) {
    items.push(
      ...pickPeople(
        library,
        (person) =>
          person.roles.some((role) => ["soloist", "singer", "ensemble", "chorus", "instrumentalist"].includes(role)) &&
          (!related.recordings.length ||
            !(
              request.composerIds?.length ||
              request.workIds?.length ||
              request.recordingIds?.length ||
              request.conductorIds?.length ||
              request.orchestraIds?.length
            ) ||
            related.artistIds.has(person.id)),
        request.artistIds,
      ).map((person) => ({
        category: "artist" as const,
        entityId: person.id,
        label: getWebsiteDisplay(person).short || getWebsiteDisplay(person).heading,
        description: getDisplayData(person).latin || person.country || "",
      })),
    );
  }

  if (categories.includes("work")) {
    items.push(
      ...filterWorksByRequest(library, request).map((work) => ({
        category: "work" as const,
        entityId: work.id,
        label: work.title,
        description: [work.titleLatin, work.catalogue].filter(Boolean).join(" / "),
      })),
    );
  }

  if (categories.includes("recording")) {
    items.push(
      ...related.recordings.map((recording) => ({
        category: "recording" as const,
        entityId: recording.id,
        label: recording.title,
        description: [recording.performanceDateText, recording.venueText].filter(Boolean).join(" / "),
      })),
    );
  }

  return {
    total: items.length,
    groups: groupByCategory(items),
  };
}

function requestForSelectionItem(item: AutomationJobSelectionItem): AutomationCheckRequest {
  if (item.category === "composer") {
    return { categories: ["composer"], composerIds: [item.entityId] };
  }
  if (item.category === "conductor") {
    return { categories: ["conductor"], conductorIds: [item.entityId] };
  }
  if (item.category === "orchestra") {
    return { categories: ["orchestra"], orchestraIds: [item.entityId] };
  }
  if (item.category === "artist") {
    return { categories: ["artist"], artistIds: [item.entityId] };
  }
  if (item.category === "work") {
    return { categories: ["work"], workIds: [item.entityId] };
  }
  return { categories: ["recording"], recordingIds: [item.entityId] };
}

function buildJobItems(selection: AutomationSelectionPreview): AutomationJobItemRecord[] {
  return selection.groups.flatMap((group) =>
    group.items.map((item) => ({
      ...item,
      status: "queued" as const,
      events: [],
      errors: [],
    })),
  );
}

function isNamedEntityCategory(category: AutomationCheckCategory) {
  return category === "composer" || category === "conductor" || category === "orchestra" || category === "artist";
}

function findEntityForItem(library: LibraryData, item: AutomationJobSelectionItem): Composer | Person | undefined {
  if (item.category === "composer") {
    return library.composers.find((composer) => composer.id === item.entityId);
  }
  return library.people.find((person) => person.id === item.entityId);
}

function findWorkForItem(library: LibraryData, item: AutomationJobSelectionItem): Work | undefined {
  if (item.category !== "work") {
    return undefined;
  }
  return library.works.find((work) => work.id === item.entityId);
}

function findRecordingForItem(library: LibraryData, item: AutomationJobSelectionItem) {
  if (item.category !== "recording") {
    return undefined;
  }
  return library.recordings.find((recording) => recording.id === item.entityId);
}

export function createAutomationJobManager() {
  const jobs = new Map<string, AutomationJobRecord>();

  const setJob = (job: AutomationJobRecord) => {
    jobs.set(job.id, job);
    return job;
  };

  const getItemRecord = (job: AutomationJobRecord, entityId: string) => job.items.find((item) => item.entityId === entityId);

  const appendEvent = (jobId: string, event: AutomationJobEvent) => {
    const job = jobs.get(jobId);
    if (!job) {
      return;
    }
    job.events.push(event);
  };

  const appendItemEvent = (
    job: AutomationJobRecord,
    item: AutomationJobItemRecord,
    phase: AutomationJobEvent["phase"],
    message: string,
  ) => {
    const event = {
      timestamp: nowIso(),
      phase,
      message,
      entityType: item.category,
      entityId: item.entityId,
    } satisfies AutomationJobEvent;
    item.events.push(event);
    job.events.push(event);
  };

  const clearCurrentItem = (job: AutomationJobRecord, item: AutomationJobItemRecord) => {
    job.currentItemIds = job.currentItemIds.filter((currentId) => currentId !== item.entityId);
    if (job.currentItem?.entityId === item.entityId) {
      job.currentItem = undefined;
    }
  };

  const failItem = (job: AutomationJobRecord, item: AutomationJobItemRecord, error: unknown, code: AutomationJobError["code"] = "fetch-failed") => {
    const message = withMessage(error);
    item.status = "failed";
    item.errors.push(message);
    job.progress.processed += 1;
    job.progress.failed += 1;
    clearCurrentItem(job, item);
    job.errors.push({
      code,
      message,
      entityType: item.category,
      entityId: item.entityId,
    });
    appendItemEvent(job, item, "error", message);
  };

  const completeItem = (
    job: AutomationJobRecord,
    item: AutomationJobItemRecord,
    status: Exclude<AutomationJobItemStatus, "queued" | "running" | "failed" | "skipped">,
    message: string,
    runId?: string,
  ) => {
    item.status = status;
    item.runId = runId;
    job.progress.processed += 1;
    if (status === "succeeded") {
      job.progress.succeeded += 1;
    } else if (status === "completed-nochange") {
      job.progress.unchanged += 1;
    } else if (status === "needs-attention") {
      job.progress.attention += 1;
    }
    clearCurrentItem(job, item);
    appendItemEvent(job, item, "complete", message);
  };

  const skipItem = (job: AutomationJobRecord, item: AutomationJobItemRecord, message: string) => {
    item.status = "skipped";
    item.errors.push(message);
    job.progress.processed += 1;
    job.progress.skipped += 1;
    clearCurrentItem(job, item);
    appendItemEvent(job, item, "complete", message);
  };

  const runSingleItem = async (job: AutomationJobRecord, item: AutomationJobItemRecord, input: CreateJobInput) => {
    if (jobs.get(job.id)?.status === "cancelled") {
      skipItem(job, item, "Job cancelled.");
      return null;
    }

    const runChecksImpl = input.runChecksImpl ?? runAutomationChecks;
    item.status = "running";
    job.currentItem = item;
    job.currentItemIds.push(item.entityId);
    appendItemEvent(job, item, "fetch", `Starting auto-check: ${item.label}`);

    const run = await runChecksImpl(
      input.library,
      requestForSelectionItem(item),
      input.fetchImpl,
      input.llmConfig,
      input.runChecksOptions,
    );
    item.runId = run.id;
    const proposals = run.proposals.filter((proposal) => proposal.entityId === item.entityId);

    if (isNamedEntityCategory(item.category) || item.category === "work" || item.category === "recording") {
      let review:
        | ReturnType<typeof reviewAutomationProposalQuality>
        | ReturnType<typeof reviewWorkAutomationProposalQuality>
        | ReturnType<typeof reviewRecordingAutomationProposalQuality>;
      let reviewTitle = item.label;
      let reviewEntityType: "composer" | "person" | "work" | "recording" = "work";
      let reviewRoles: string[] = [];
      let currentEntity: Record<string, unknown>;

      if (isNamedEntityCategory(item.category)) {
        const entity = findEntityForItem(input.library, item);
        if (!entity) {
          throw new Error(`未找到待审查人物：${item.entityId}`);
        }
        review = reviewAutomationProposalQuality(entity, proposals);
        reviewTitle = entity.nameLatin || entity.name;
        reviewEntityType = item.category === "composer" ? "composer" : "person";
        reviewRoles = "roles" in entity ? [...entity.roles] : [];
        currentEntity = entity as unknown as Record<string, unknown>;
      } else if (item.category === "work") {
        const work = findWorkForItem(input.library, item);
        if (!work) {
          throw new Error(`未找到待审查作品：${item.entityId}`);
        }
        const composer = input.library.composers.find((candidate) => candidate.id === work.composerId);
        review = reviewWorkAutomationProposalQuality(work, composer, proposals);
        reviewTitle = [composer?.nameLatin || composer?.name || "", work.title].filter(Boolean).join(" / ");
        reviewEntityType = "work";
        currentEntity = work as unknown as Record<string, unknown>;
      } else {
        const recording = findRecordingForItem(input.library, item);
        if (!recording) {
          throw new Error(`未找到待审查版本：${item.entityId}`);
        }
        review = reviewRecordingAutomationProposalQuality(recording, proposals);
        reviewTitle = recording.title;
        reviewEntityType = "recording";
        currentEntity = recording as unknown as Record<string, unknown>;
      }

      if (proposals.length > 0 && input.llmConfig && reviewEntityType !== "recording") {
        const llmReview = await reviewAutomationProposalWithLlm({
          config: input.llmConfig,
          entityType: reviewEntityType,
          title: reviewTitle,
          roles: reviewRoles,
          current: currentEntity,
          preview: review.preview as Record<string, unknown>,
          fields: proposals.flatMap((proposal) => proposal.fields || []),
          sources: [...new Set(proposals.flatMap((proposal) => proposal.sources || []))],
          evidence: proposals.flatMap((proposal) => proposal.evidence || []),
          fetchImpl: input.fetchImpl ?? fetch,
        });
        review = mergeProposalReviewResults(review, llmReview);
      }

      item.reviewIssues = review.issues;
      if (item.category === "recording") {
        const recording = findRecordingForItem(input.library, item);
        if (recording && run.provider) {
          item.recordingAuditResult = buildRecordingRetrievalAuditResult({
            target: buildRecordingRetrievalAuditTarget(recording),
            recording,
            providerStatus: run.provider.status,
            providerError: run.provider.error,
            proposals,
            review: {
              status: review.status as "ok" | "needs-attention" | "already-complete",
              issues: review.issues,
            },
          });
        }
      }

      if (review.status === "needs-attention") {
        const reviewMessage =
          proposals.length > 0
            ? `Auto review completed: ${item.label} still needs attention.`
            : `Auto review completed: ${item.label} remains incomplete without usable candidates.`;
        appendItemEvent(job, item, "review", reviewMessage);
        completeItem(job, item, "needs-attention", reviewMessage, run.id);
        job.errors.push({
          code: proposals.length > 0 ? "needs-attention" : "no-candidate",
          message: review.issues.join("; ") || "No new proposals were accepted and the item still needs review.",
          entityType: item.category,
          entityId: item.entityId,
        });
        return run;
      }

      if (review.status === "already-complete") {
        const reviewMessage = `Auto review completed: ${item.label} is already complete.`;
        appendItemEvent(job, item, "review", reviewMessage);
        completeItem(job, item, "completed-nochange", reviewMessage, run.id);
        return run;
      }

      if (!review.ok) {
        item.reviewIssues = review.issues;
        throw new Error(review.issues.join("; ") || "Auto review failed.");
      }

      appendItemEvent(job, item, "review", `Auto review passed: ${item.label} has reviewable proposals.`);
    }

    if (run.proposals.length === 0) {
      completeItem(job, item, "completed-nochange", `Auto check completed: ${item.label} produced no new proposals.`, run.id);
      return run;
    }

    completeItem(job, item, "succeeded", `Auto check completed: ${item.label} produced reviewable proposals.`, run.id);
    return run;

  };

  const runJob = async (jobId: string, input: CreateJobInput) => {
    const job = jobs.get(jobId);
    if (!job) {
      return;
    }

    const items = job.items;
    if (items.length === 0) {
      job.status = "completed";
      job.completedAt = nowIso();
      job.errors.push({
        code: "selection-empty",
        message: "Current filters did not match any items.",
      });
      return;
    }

    job.status = "running";
    appendEvent(jobId, {
      timestamp: nowIso(),
      phase: "selection",
      message: `Selected ${items.length} items for this run.`, 
    });

    const proposalRuns: AutomationRun[] = [];
    const maxConcurrency = Math.max(1, Math.min(input.maxConcurrency ?? 6, 12));
    let cursor = 0;

    const worker = async () => {
      while (true) {
        if (jobs.get(jobId)?.status === "cancelled") {
          return;
        }
        const index = cursor;
        cursor += 1;
        const item = items[index];
        if (!item) {
          return;
        }
        try {
          const run = await runSingleItem(job, item, input);
          if (run) {
            proposalRuns.push(run);
          }
        } catch (error) {
          failItem(job, item, error, item.reviewIssues?.length ? "quality-failed" : "fetch-failed");
        }
      }
    };

    await Promise.all(Array.from({ length: Math.min(maxConcurrency, items.length) }, () => worker()));

    const mergedRun = createAutomationRun(input.library, {
      categories: uniqueCategories(input.request),
      proposals: mergeRunProposals(proposalRuns),
      notes: uniqueStrings(proposalRuns.flatMap((run) => run.notes)),
      provider: proposalRuns.map((run) => run.provider).find(Boolean),
    });

    job.run = mergedRun;
    const recordingAuditResults = job.items
      .map((item) => item.recordingAuditResult)
      .filter((result): result is RecordingRetrievalAuditResult => Boolean(result));
    if (recordingAuditResults.length > 0) {
      const summary = summarizeRecordingRetrievalAudit(recordingAuditResults);
      job.recordingAudit = {
        results: recordingAuditResults,
        summary,
      };
      const gateMessage = buildRecordingAuditGateMessage(summary);
      appendEvent(jobId, {
        timestamp: nowIso(),
        phase: "review",
        message: gateMessage,
        entityType: "recording",
      });
      if ((summary.reviewStatusCounts["needs-attention"] || 0) > 0) {
        job.errors.push({
          code: "needs-attention",
          message: gateMessage,
          entityType: "recording",
        });
      }
    }
    job.currentItem = undefined;
    job.currentItemIds = [];
    job.selectedItemId =
      job.items.find((item) => item.status === "needs-attention")?.entityId ||
      job.items.find((item) => item.status === "failed")?.entityId ||
      job.items.find((item) => item.status === "succeeded")?.entityId ||
      job.items.find((item) => item.status === "completed-nochange")?.entityId ||
      job.items[0]?.entityId ||
      "";

    if (jobs.get(jobId)?.status === "cancelled") {
      job.errors.push({
        code: "job-cancelled",
        message: "Job cancelled.",
      });
    }

    job.status = jobs.get(jobId)?.status === "cancelled" ? "cancelled" : "completed";
    job.completedAt = nowIso();
    appendEvent(jobId, {
      timestamp: nowIso(),
      phase: "complete",
      message: `Job finished: ${job.progress.succeeded} succeeded, ${job.progress.unchanged} unchanged, ${job.progress.attention} needs attention, ${job.progress.failed} failed, ${job.progress.skipped} skipped.`,
    });

    if (input.onCompleted) {
      await input.onCompleted(job);
    }
  };

  return {
    previewSelection(library: LibraryData, request: AutomationCheckRequest) {
      return previewAutomationSelection(library, request);
    },
    createJob(input: CreateJobInput) {
      const id = `job-${Date.now()}-${Math.random().toString(16).slice(2, 8)}`;
      const selection = previewAutomationSelection(input.library, input.request);
      const items = buildJobItems(selection);
      const job = setJob({
        id,
        status: "preparing",
        request: input.request,
        progress: {
          total: selection.total,
          processed: 0,
          succeeded: 0,
          failed: 0,
          skipped: 0,
          unchanged: 0,
          attention: 0,
        },
        currentItemIds: [],
        items,
        selectedItemId: items[0]?.entityId || "",
        events: [
          {
            timestamp: nowIso(),
            phase: "prepare",
            message: "Job created. Preparing auto-check run.",
          },
        ],
        errors: [],
        selection,
        createdAt: nowIso(),
      });

      void runJob(id, input);
      return job;
    },
    getJob(jobId: string) {
      return jobs.get(jobId);
    },
    listJobs() {
      return [...jobs.values()].sort((left, right) => right.createdAt.localeCompare(left.createdAt));
    },
    cancelJob(jobId: string) {
      const job = jobs.get(jobId);
      if (job) {
        job.status = "cancelled";
      }
      return job;
    },
    selectJobItem(jobId: string, entityId: string) {
      const job = jobs.get(jobId);
      if (!job || !getItemRecord(job, entityId)) {
        return job;
      }
      job.selectedItemId = entityId;
      return job;
    },
    async waitForJob(jobId: string) {
      while (true) {
        const job = jobs.get(jobId);
        if (!job || ["completed", "cancelled"].includes(job.status)) {
          return job;
        }
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
    },
  };
}



