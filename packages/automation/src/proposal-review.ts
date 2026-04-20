import type { LlmProposalReview } from "./llm.js";

type ReviewBase = {
  ok: boolean;
  status: string;
  issues: string[];
  preview: unknown;
  hasChanges: boolean;
};

const PROPOSAL_REVIEW_CONFIDENCE_FLOOR = 0.75;

function formatNormalizedValueSuggestion(normalizedValue: Record<string, unknown> | undefined) {
  if (!normalizedValue) {
    return "";
  }

  const entries = Object.entries(normalizedValue)
    .map(([field, value]) => {
      const normalized = typeof value === "string" ? value.trim() : JSON.stringify(value);
      return normalized ? `${field}=${normalized}` : "";
    })
    .filter(Boolean);

  return entries.length > 0 ? `建议标准化：${entries.join("；")}` : "";
}

export function mergeProposalReviewResults<T extends ReviewBase>(review: T, llmReview: LlmProposalReview | null) {
  if (!llmReview) {
    return review;
  }

  const issues = [...review.issues];
  const lowConfidence =
    typeof llmReview.confidence === "number" &&
    Number.isFinite(llmReview.confidence) &&
    llmReview.confidence < PROPOSAL_REVIEW_CONFIDENCE_FLOOR;

  if (llmReview.status === "needs-attention") {
    issues.push(...llmReview.issues);
    issues.push(...llmReview.reasons);
    if (llmReview.rejectBecause) {
      issues.push(llmReview.rejectBecause);
    }
    const normalizedValueSuggestion = formatNormalizedValueSuggestion(llmReview.normalizedValue);
    if (normalizedValueSuggestion) {
      issues.push(normalizedValueSuggestion);
    }
  }

  if (lowConfidence) {
    issues.push(`LLM 复核置信度过低：${llmReview.confidence?.toFixed(2)}`);
  }

  const status =
    llmReview.status === "needs-attention" || lowConfidence
      ? "needs-attention"
      : review.status === "already-complete" && review.hasChanges
        ? "ok"
        : review.status;

  return {
    ...review,
    ok: status === "ok",
    status,
    issues: [...new Set(issues)],
  };
}
