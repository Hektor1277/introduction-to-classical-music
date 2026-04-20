export const REVIEW_PAGE_SIZE = 5;

export const buildDataAttributeSelector = (attributeName, value) =>
  `[${attributeName}="${String(value ?? "").replaceAll("\\", "\\\\").replaceAll('"', '\\"')}"]`;

const dedupeProposalsById = (proposals) => {
  const seen = new Set();
  return (proposals || []).filter((proposal) => {
    const id = String(proposal?.id || "");
    if (!id) {
      return true;
    }
    if (seen.has(id)) {
      return false;
    }
    seen.add(id);
    return true;
  });
};

const stableSerialize = (value) => JSON.stringify(value ?? null);

export const getProposalApplyBlockers = (proposal) => {
  if (!proposal) {
    return ["候选不存在，无法直接应用。"];
  }

  if (proposal?.kind === "merge") {
    return ["合并候选不能直接应用，请先人工处理关联关系。"];
  }
  const reasons = [];
  const hasDirectChanges = (proposal?.fields?.length ?? 0) > 0 || (proposal?.imageCandidates?.length ?? 0) > 0;
  if (!hasDirectChanges) {
    reasons.push("该候选没有可直接写入的字段或图片，只能人工复核。");
  }
  if (proposal?.risk === "high") {
    reasons.push("高风险候选不能直接应用，请先人工复核。");
  }

  return [...new Set(reasons)];
};

export const isProposalDirectlyApplicable = (proposal) => getProposalApplyBlockers(proposal).length === 0;

export const buildExcerpt = (value, maxLength = 120) => {
  const normalized = String(value ?? "").trim();
  if (!normalized) {
    return { text: "", truncated: false };
  }
  if (normalized.length <= maxLength) {
    return { text: normalized, truncated: false };
  }
  return {
    text: `${normalized.slice(0, Math.max(1, maxLength - 1)).trimEnd()}…`,
    truncated: true,
  };
};

export const paginateItems = (items, page = 1, pageSize = REVIEW_PAGE_SIZE) => {
  const safePageSize = Math.max(1, Number(pageSize) || REVIEW_PAGE_SIZE);
  const totalItems = Array.isArray(items) ? items.length : 0;
  const totalPages = Math.max(1, Math.ceil(totalItems / safePageSize));
  const safePage = Math.min(Math.max(1, Number(page) || 1), totalPages);
  const startIndex = (safePage - 1) * safePageSize;
  return {
    page: safePage,
    pageSize: safePageSize,
    totalItems,
    totalPages,
    items: (items || []).slice(startIndex, startIndex + safePageSize),
  };
};

export const getProposalsForReviewAction = (proposals, action, options = {}) => {
  const scope = options.scope === "page" ? "page" : "all";
  const scopeIds = new Set((options.scopeIds || []).map((value) => String(value)));
  const normalizedProposals = dedupeProposalsById(proposals);
  const scopedProposals =
    scope === "page"
      ? normalizedProposals.filter((proposal) => scopeIds.has(String(proposal?.id || "")))
      : normalizedProposals;

  if (action === "apply-confirmed") {
    return scopedProposals.filter(
      (proposal) =>
        proposal?.reviewState === "confirmed" &&
        proposal?.status === "pending" &&
        isProposalDirectlyApplicable(proposal),
    );
  }

  if (action === "ignore-pending") {
    return scopedProposals.filter((proposal) => proposal?.status === "pending");
  }

  return [];
};

export const getBlockedProposalsForReviewAction = (proposals, action, options = {}) => {
  if (action !== "apply-confirmed") {
    return [];
  }

  const scope = options.scope === "page" ? "page" : "all";
  const scopeIds = new Set((options.scopeIds || []).map((value) => String(value)));
  const normalizedProposals = dedupeProposalsById(proposals);
  const scopedProposals =
    scope === "page"
      ? normalizedProposals.filter((proposal) => scopeIds.has(String(proposal?.id || "")))
      : normalizedProposals;

  return scopedProposals
    .filter((proposal) => proposal?.reviewState === "confirmed" && proposal?.status === "pending")
    .map((proposal) => ({
      proposal,
      reasons: getProposalApplyBlockers(proposal),
    }))
    .filter((entry) => entry.reasons.length > 0);
};

export const buildBlockedReviewActionMessage = (blockedEntries, action = "apply-confirmed") => {
  if (!Array.isArray(blockedEntries) || blockedEntries.length === 0) {
    return "";
  }

  const actionLabel = action === "apply-confirmed" ? "已确认候选" : "当前候选";
  const lines = blockedEntries.slice(0, 5).map((entry) => {
    const summary = String(entry?.proposal?.summary || entry?.proposal?.id || "未命名候选").trim();
    return `${summary}：${(entry?.reasons || []).join("；")}`;
  });
  const suffix = blockedEntries.length > 5 ? `；其余 ${blockedEntries.length - 5} 条请先逐条处理。` : "";
  return `${actionLabel}中包含 ${blockedEntries.length} 条被阻止的候选：${lines.join("；")}${suffix}`;
};

export const filterPendingProposalsForDisplay = (proposals) =>
  dedupeProposalsById(proposals).filter((proposal) => proposal?.status === "pending");

/**
 * @param {Record<string, any> | null | undefined} proposal
 * @param {{ selectedImageCandidateId?: string, fieldsPatchMap?: Record<string, any> } | null | undefined} [draft]
 */
export const hasProposalDraftChanges = (proposal, draft = {}) => {
  if (!proposal) {
    return false;
  }

  const safeDraft = draft && typeof draft === "object" ? draft : {};

  const currentSelectedId = String(proposal.selectedImageCandidateId || "");
  if (
    typeof safeDraft.selectedImageCandidateId === "string" &&
    safeDraft.selectedImageCandidateId !== currentSelectedId
  ) {
    return true;
  }

  const patchMap = safeDraft.fieldsPatchMap || {};
  for (const field of proposal.fields || []) {
    if (!(field.path in patchMap)) {
      continue;
    }
    if (stableSerialize(patchMap[field.path]) !== stableSerialize(field.after)) {
      return true;
    }
  }

  return false;
};

/**
 * @param {Record<string, any> | null | undefined} proposal
 * @param {{ selectedImageCandidateId?: string, fieldsPatchMap?: Record<string, any> } | null | undefined} [liveDraft]
 * @param {{ selectedImageCandidateId?: string, fieldsPatchMap?: Record<string, any> } | null | undefined} [storedDraft]
 */
export const resolveProposalDraft = (proposal, liveDraft = null, storedDraft = null) => {
  if (hasProposalDraftChanges(proposal, liveDraft)) {
    return liveDraft;
  }
  if (hasProposalDraftChanges(proposal, storedDraft)) {
    return storedDraft;
  }
  return liveDraft || storedDraft || null;
};

/**
 * @param {Record<string, any> | null | undefined} proposal
 * @param {{ selectedImageCandidateId?: string, fieldsPatchMap?: Record<string, any> } | null | undefined} [draft]
 */
export const applyProposalDraft = (proposal, draft = {}) => {
  if (!proposal) {
    return proposal;
  }

  const safeDraft = draft && typeof draft === "object" ? draft : {};

  const patchMap = safeDraft.fieldsPatchMap || {};
  return {
    ...proposal,
    selectedImageCandidateId:
      typeof safeDraft.selectedImageCandidateId === "string"
        ? safeDraft.selectedImageCandidateId
        : proposal.selectedImageCandidateId || "",
    fields: (proposal.fields || []).map((field) =>
      field.path in patchMap
        ? {
            ...field,
            after: patchMap[field.path],
          }
        : field,
    ),
  };
};
