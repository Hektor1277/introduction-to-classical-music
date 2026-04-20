import { describe, expect, it } from "vitest";

import { reviewRecordingAutomationProposalQuality } from "@/lib/automation-checks";
import type { AutomationProposal } from "@/lib/automation";
import type { Recording } from "@/lib/schema";

function createRecording(overrides: Partial<Recording> = {}): Recording {
  return {
    id: "recording-bohm-1976",
    workId: "work-beethoven-7",
    slug: "bohm-1976",
    title: "Bohm - Vienna Philharmonic - 1976",
    sortKey: "0010",
    isPrimaryRecommendation: true,
    updatedAt: "2026-03-25T00:00:00.000Z",
    images: [],
    credits: [],
    links: [],
    notes: "",
    performanceDateText: "1976",
    venueText: "Musikverein",
    albumTitle: "",
    label: "",
    releaseDate: "",
    infoPanel: { text: "", articleId: "", collectionLinks: [], collectionUrl: "" },
    ...overrides,
  };
}

function createProposal(overrides: Partial<AutomationProposal> = {}): AutomationProposal {
  return {
    id: "proposal-recording-bohm-1976",
    kind: "update",
    entityType: "recording",
    entityId: "recording-bohm-1976",
    summary: "补充版本检索结果：Bohm - Vienna Philharmonic - 1976",
    risk: "medium",
    status: "pending",
    reviewState: "unseen",
    sources: ["https://example.com/release"],
    fields: [],
    warnings: [],
    imageCandidates: [],
    mergeCandidates: [],
    selectedImageCandidateId: "",
    evidence: [],
    linkCandidates: [],
    ...overrides,
  };
}

describe("recording automation proposal quality review", () => {
  it("flags release dates earlier than the known performance year", () => {
    const review = reviewRecordingAutomationProposalQuality(createRecording(), [
      createProposal({
        fields: [{ path: "releaseDate", before: "", after: "1975" }],
      }),
    ]);

    expect(review.status).toBe("needs-attention");
    expect(review.issues.length).toBeGreaterThan(0);
  });

  it("keeps a clean image-only proposal reviewable", () => {
    const review = reviewRecordingAutomationProposalQuality(createRecording(), [
      createProposal({
        risk: "low",
        imageCandidates: [
          {
            id: "cover-1",
            src: "https://cdn.example.com/bohm-cover.jpg",
            sourceUrl: "https://example.com/release",
            sourceKind: "official-site",
            attribution: "example.com",
            title: "Bohm Beethoven 7",
            width: 1200,
            height: 1200,
          },
        ],
      }),
    ]);

    expect(review.status).toBe("ok");
    expect(review.ok).toBe(true);
    expect(review.issues).toEqual([]);
  });

  it("flags metadata proposals that still carry hard conflict warnings", () => {
    const review = reviewRecordingAutomationProposalQuality(createRecording(), [
      createProposal({
        fields: [
          { path: "releaseDate", before: "", after: "1983" },
          { path: "albumTitle", before: "", after: "The Originals: Bruckner Symphony No. 7" },
        ],
        warnings: ["多个候选URL日期或地点不匹配"],
      }),
    ]);

    expect(review.status).toBe("needs-attention");
    expect(review.issues.length).toBeGreaterThan(0);
  });

  it("does not block metadata-only proposals because unrelated venue threshold warnings remain", () => {
    const review = reviewRecordingAutomationProposalQuality(createRecording(), [
      createProposal({
        fields: [
          { path: "albumTitle", before: "", after: "Beethoven Recital" },
          { path: "label", before: "", after: "Philips" },
        ],
        warnings: ["venueText 未达到最终采纳阈值。"],
      }),
    ]);

    expect(review.status).toBe("ok");
    expect(review.issues).toEqual([]);
  });

  it("keeps date-related warnings blocking when the proposal changes date metadata", () => {
    const review = reviewRecordingAutomationProposalQuality(createRecording(), [
      createProposal({
        fields: [{ path: "releaseDate", before: "", after: "1978" }],
        warnings: ["部分候选URL表演者或年份不匹配"],
      }),
    ]);

    expect(review.status).toBe("needs-attention");
    expect(review.issues.length).toBeGreaterThan(0);
  });

  it("treats spelling-variant notes as non-blocking when core metadata is otherwise consistent", () => {
    const review = reviewRecordingAutomationProposalQuality(createRecording(), [
      createProposal({
        fields: [
          { path: "albumTitle", before: "", after: "Historic Violin Sonatas" },
          { path: "label", before: "", after: "Philips" },
        ],
        warnings: ["部分URL标题或描述存在拼写变体（如Mogilevsky/Moguilewsky），但核心信息一致"],
      }),
    ]);

    expect(review.status).toBe("ok");
    expect(review.issues).toEqual([]);
  });

  it("does not block on candidate-elimination notes that only explain rejected urls", () => {
    const review = reviewRecordingAutomationProposalQuality(createRecording(), [
      createProposal({
        fields: [{ path: "releaseDate", before: "", after: "1978" }],
        warnings: ["第7条URL标注年份为1953，可能为不同版本"],
      }),
    ]);

    expect(review.status).toBe("ok");
    expect(review.issues).toEqual([]);
  });

  it("keeps candidate-level hard conflict warnings blocking when they describe a real date and venue conflict", () => {
    const review = reviewRecordingAutomationProposalQuality(createRecording(), [
      createProposal({
        fields: [{ path: "releaseDate", before: "", after: "1978" }],
        warnings: ["URL 3 与当前版本年份和地点冲突，疑似不同场次录音，应用前需人工复核"],
      }),
    ]);

    expect(review.status).toBe("needs-attention");
    expect(review.issues.length).toBeGreaterThan(0);
  });

  it("does not block on grouped mismatch summaries when accepted metadata already comes from surviving candidates", () => {
    const review = reviewRecordingAutomationProposalQuality(createRecording(), [
      createProposal({
        fields: [
          { path: "performanceDateText", before: "1976", after: "February 2-5 1976" },
          { path: "releaseDate", before: "", after: "1984" },
          { path: "albumTitle", before: "", after: "Richard Strauss: Eine Alpensinfonie" },
        ],
        warnings: ["多个候选URL年份或地点不匹配", "部分URL为合集或传记内容"],
      }),
    ]);

    expect(review.status).toBe("ok");
    expect(review.issues).toEqual([]);
  });

  it("does not block on reupload notes when the warning only explains alternate uploads of the same recording", () => {
    const review = reviewRecordingAutomationProposalQuality(createRecording(), [
      createProposal({
        fields: [
          { path: "albumTitle", before: "", after: "Historic Violin Sonatas" },
          { path: "label", before: "", after: "Pearl" },
          { path: "releaseDate", before: "", after: "1992" },
        ],
        warnings: ["部分URL可能为同一录音的不同上传或剪辑版本"],
      }),
    ]);

    expect(review.status).toBe("ok");
    expect(review.issues).toEqual([]);
  });

  it("does not block on grouped rejected-candidate summaries about irrelevant or wrongly dated results", () => {
    const review = reviewRecordingAutomationProposalQuality(createRecording(), [
      createProposal({
        fields: [
          { path: "performanceDateText", before: "1976", after: "February 2-5 1976" },
          { path: "albumTitle", before: "", after: "Richard Strauss: Eine Alpensinfonie" },
          { path: "label", before: "", after: "DG" },
        ],
        warnings: ["多个候选包含无关内容或错误日期"],
      }),
    ]);

    expect(review.status).toBe("ok");
    expect(review.issues).toEqual([]);
  });

  it("does not block on wording variants that omit the url qualifier in grouped mismatch summaries", () => {
    const review = reviewRecordingAutomationProposalQuality(createRecording(), [
      createProposal({
        fields: [
          { path: "performanceDateText", before: "1976", after: "August 28, 1982" },
          { path: "albumTitle", before: "", after: "Richard Strauss: Eine Alpensinfonie" },
        ],
        warnings: ["多个候选年份或地点不匹配"],
      }),
    ]);

    expect(review.status).toBe("ok");
    expect(review.issues).toEqual([]);
  });

  it("does not block on spelling-variant warnings that explicitly still point to the same recording", () => {
    const review = reviewRecordingAutomationProposalQuality(createRecording(), [
      createProposal({
        fields: [
          { path: "albumTitle", before: "", after: "Historic Violin Sonatas" },
          { path: "label", before: "", after: "Pearl" },
        ],
        warnings: ["部分URL标题使用不同拼写（如Moguilewsky vs Mogilevsky），但指向同一录音"],
      }),
    ]);

    expect(review.status).toBe("ok");
    expect(review.issues).toEqual([]);
  });

  it("does not block on same-festival bilibili reupload notes when the source mismatch is only provenance uncertainty", () => {
    const review = reviewRecordingAutomationProposalQuality(createRecording(), [
      createProposal({
        fields: [
          { path: "performanceDateText", before: "1976", after: "Beethovenfest Bonn 1970" },
          { path: "releaseDate", before: "", after: "1991" },
        ],
        warnings: ["多个B站视频标注相同音乐节信息但来源不明，需谨慎验证"],
      }),
    ]);

    expect(review.status).toBe("ok");
    expect(review.issues).toEqual([]);
  });

  it("does not block on broader bilibili provenance notes when they only say the repost source or performer info is unclear", () => {
    const review = reviewRecordingAutomationProposalQuality(createRecording(), [
      createProposal({
        fields: [
          { path: "performanceDateText", before: "1976", after: "Beethovenfest Bonn 1970" },
          { path: "releaseDate", before: "", after: "1991" },
        ],
        warnings: ["多个B站视频标注为1970年波恩音乐节，但转载来源或演奏者信息不明确，需谨慎验证"],
      }),
    ]);

    expect(review.status).toBe("ok");
    expect(review.issues).toEqual([]);
  });

  it("does not block on catch-all rejected-candidate summaries about other urls not matching year or performers", () => {
    const review = reviewRecordingAutomationProposalQuality(createRecording(), [
      createProposal({
        fields: [
          { path: "performanceDateText", before: "1976", after: "1980 Vienna" },
          { path: "albumTitle", before: "", after: "Beethoven: Symphony No. 9" },
        ],
        warnings: ["其他URL年份、乐团或独唱家不匹配"],
      }),
    ]);

    expect(review.status).toBe("ok");
    expect(review.issues).toEqual([]);
  });

  it("does not block notes-only proposals when warnings only explain why rejected candidates were not used", () => {
    const review = reviewRecordingAutomationProposalQuality(createRecording(), [
      createProposal({
        fields: [{ path: "notes", before: "", after: "候选7标题为卡农，与当前作品不符，已保留为说明。"}],
        warnings: ["记录7标题为卡农，与阿尔卑斯山交响曲不符，但日期地点匹配，需谨慎验证内容"],
      }),
    ]);

    expect(review.status).toBe("ok");
    expect(review.issues).toEqual([]);
  });
});
