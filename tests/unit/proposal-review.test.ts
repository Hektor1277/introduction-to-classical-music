import { describe, expect, it } from "vitest";

import { mergeProposalReviewResults } from "@/lib/proposal-review";

describe("proposal review merge", () => {
  it("keeps normalized values as suggestions instead of mutating the preview payload", () => {
    const preview = {
      country: "Germany",
      displayFullName: "安东·布鲁克纳",
    };
    const review = {
      ok: true,
      status: "ok",
      issues: [] as string[],
      preview,
      hasChanges: true,
    };

    const merged = mergeProposalReviewResults(review, {
      verdict: "reject",
      status: "needs-attention",
      issues: ["候选值与现有规范字段冲突"],
      reasons: ["现有中文全名已经完整"],
      rejectBecause: "禁止低质量候选覆盖正式字段",
      normalizedValue: { country: "Austria" },
      confidence: 0.91,
    });

    expect(merged.status).toBe("needs-attention");
    expect(merged.ok).toBe(false);
    expect(merged.preview).toBe(preview);
    expect(merged.preview).toEqual({
      country: "Germany",
      displayFullName: "安东·布鲁克纳",
    });
    expect(merged.issues).toEqual(
      expect.arrayContaining([
        "候选值与现有规范字段冲突",
        "现有中文全名已经完整",
        "禁止低质量候选覆盖正式字段",
        "建议标准化：country=Austria",
      ]),
    );
  });

  it("forces manual review when llm confidence is below the safety floor", () => {
    const merged = mergeProposalReviewResults(
      {
        ok: true,
        status: "ok",
        issues: [] as string[],
        preview: { titleLatin: "Symphony No. 7" },
        hasChanges: true,
      },
      {
        verdict: "accept",
        status: "ok",
        issues: [],
        reasons: [],
        confidence: 0.42,
      },
    );

    expect(merged.status).toBe("needs-attention");
    expect(merged.ok).toBe(false);
    expect(merged.issues).toContain("LLM 复核置信度过低：0.42");
  });
});
