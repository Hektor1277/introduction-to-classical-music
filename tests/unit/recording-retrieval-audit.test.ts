import { describe, expect, it } from "vitest";

import {
  buildRecordingRetrievalAuditPlan,
  buildRecordingRetrievalAuditResult,
  formatRecordingRetrievalAuditMarkdown,
  summarizeRecordingRetrievalAudit,
} from "@/lib/recording-retrieval-audit";
import { validateLibrary } from "@/lib/schema";

const library = validateLibrary({
  composers: [
    {
      id: "composer-beethoven",
      slug: "beethoven",
      name: "贝多芬",
      fullName: "",
      nameLatin: "Ludwig van Beethoven",
      displayName: "贝多芬",
      displayFullName: "",
      displayLatinName: "Ludwig van Beethoven",
      country: "Germany",
      avatarSrc: "",
      aliases: [],
      abbreviations: [],
      sortKey: "0010",
      summary: "",
      infoPanel: { text: "", articleId: "", collectionLinks: [] },
      imageSourceUrl: "",
      imageSourceKind: "",
      imageAttribution: "",
      imageUpdatedAt: "",
    },
  ],
  people: [],
  workGroups: [
    {
      id: "group-symphony",
      composerId: "composer-beethoven",
      title: "交响曲",
      slug: "symphony",
      path: ["交响曲"],
      sortKey: "0010",
    },
  ],
  works: [
    {
      id: "work-beethoven-5",
      composerId: "composer-beethoven",
      groupIds: ["group-symphony"],
      slug: "beethoven-5",
      title: "第五交响曲",
      titleLatin: "Symphony No. 5",
      aliases: [],
      catalogue: "Op. 67",
      summary: "",
      infoPanel: { text: "", articleId: "", collectionLinks: [] },
      sortKey: "0010",
      updatedAt: "2026-03-25T00:00:00.000Z",
    },
  ],
  recordings: [
    {
      id: "recording-a",
      workId: "work-beethoven-5",
      slug: "recording-a",
      title: "Recording A",
      sortKey: "0010",
      isPrimaryRecommendation: true,
      updatedAt: "2026-03-25T00:00:00.000Z",
      images: [],
      credits: [],
      links: [],
      notes: "",
      performanceDateText: "1976",
      venueText: "",
      albumTitle: "",
      label: "",
      releaseDate: "",
      infoPanel: { text: "", articleId: "", collectionLinks: [], collectionUrl: "" },
    },
    {
      id: "recording-b",
      workId: "work-beethoven-5",
      slug: "recording-b",
      title: "Recording B",
      sortKey: "0020",
      isPrimaryRecommendation: false,
      updatedAt: "2026-03-25T00:00:00.000Z",
      images: [{ src: "/cover.jpg", alt: "cover" }],
      credits: [],
      links: [],
      notes: "",
      performanceDateText: "1980",
      venueText: "",
      albumTitle: "Official Release",
      label: "",
      releaseDate: "",
      infoPanel: { text: "", articleId: "", collectionLinks: [], collectionUrl: "" },
    },
    {
      id: "recording-c",
      workId: "work-beethoven-5",
      slug: "recording-c",
      title: "Recording C",
      sortKey: "0030",
      isPrimaryRecommendation: false,
      updatedAt: "2026-03-25T00:00:00.000Z",
      images: [],
      credits: [],
      links: [],
      notes: "",
      performanceDateText: "1990",
      venueText: "",
      albumTitle: "Has Metadata",
      label: "DG",
      releaseDate: "1991",
      infoPanel: { text: "", articleId: "", collectionLinks: [], collectionUrl: "" },
    },
  ],
});

describe("recording retrieval audit helpers", () => {
  it("builds grouped live-audit targets from missing recording fields", () => {
    const plan = buildRecordingRetrievalAuditPlan(library, { sampleSizePerGroup: 1 });

    expect(plan.groups).toEqual([
      expect.objectContaining({ key: "missingAlbumTitle", totalCandidates: 1, selectedRecordingIds: ["recording-a"] }),
      expect.objectContaining({ key: "missingLabel", totalCandidates: 2, selectedRecordingIds: ["recording-b"] }),
      expect.objectContaining({ key: "missingReleaseDate", totalCandidates: 2, selectedRecordingIds: ["recording-a"] }),
      expect.objectContaining({ key: "missingImages", totalCandidates: 2, selectedRecordingIds: ["recording-c"] }),
    ]);
    expect(plan.targets).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ recordingId: "recording-a", groupKeys: ["missingAlbumTitle", "missingReleaseDate"] }),
        expect.objectContaining({ recordingId: "recording-b", groupKeys: ["missingLabel"] }),
        expect.objectContaining({ recordingId: "recording-c", groupKeys: ["missingImages"] }),
      ]),
    );
    expect(plan.totalTargets).toBe(3);
  });

  it("summarizes live-audit results by group and review status", () => {
    const summary = summarizeRecordingRetrievalAudit([
      {
        recordingId: "recording-a",
        title: "Recording A",
        groupKeys: ["missingAlbumTitle", "missingLabel"],
        providerStatus: "partial",
        reviewStatus: "needs-attention",
        proposalCount: 1,
        proposalFields: ["albumTitle", "label"],
        warnings: ["第一条URL指挥不符"],
        issues: ["版本提案仍带有来源冲突警告，应用前需要人工复核。"],
      },
      {
        recordingId: "recording-b",
        title: "Recording B",
        groupKeys: ["missingLabel", "missingReleaseDate"],
        providerStatus: "succeeded",
        reviewStatus: "ok",
        proposalCount: 1,
        proposalFields: ["label", "releaseDate"],
        warnings: [],
        issues: [],
      },
    ]);

    expect(summary.totalTargets).toBe(2);
    expect(summary.reviewStatusCounts).toEqual({ ok: 1, "needs-attention": 1 });
    expect(summary.providerStatusCounts).toEqual({ partial: 1, succeeded: 1 });
    expect(summary.warningCorpus).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          signature: "第#条URL指挥不符",
          count: 1,
          examples: ["第一条URL指挥不符"],
          reviewStatusCounts: { "needs-attention": 1 },
          groupKeys: ["missingAlbumTitle", "missingLabel"],
        }),
      ]),
    );
    expect(summary.groups).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          key: "missingLabel",
          sampleCount: 2,
          reviewStatusCounts: { ok: 1, "needs-attention": 1 },
          topFieldPaths: ["label", "albumTitle", "releaseDate"],
        }),
        expect.objectContaining({
          key: "missingReleaseDate",
          sampleCount: 1,
          reviewStatusCounts: { ok: 1 },
          topWarnings: [],
        }),
      ]),
    );
  });

  it("normalizes warning wording drift into a reusable corpus while keeping raw examples", () => {
    const summary = summarizeRecordingRetrievalAudit([
      {
        recordingId: "recording-a",
        title: "Recording A",
        groupKeys: ["missingReleaseDate"],
        providerStatus: "partial",
        reviewStatus: "needs-attention",
        proposalCount: 1,
        proposalFields: ["releaseDate"],
        warnings: [
          "多个B站视频标注为1970年波恩音乐节，但转载来源或演奏者信息不明确，需谨慎验证",
          "候选2-7的演奏者与草稿不符",
        ],
        issues: ["版本提案仍带有来源冲突警告，应用前需要人工复核。"],
      },
      {
        recordingId: "recording-b",
        title: "Recording B",
        groupKeys: ["missingImages"],
        providerStatus: "partial",
        reviewStatus: "ok",
        proposalCount: 1,
        proposalFields: ["notes"],
        warnings: [
          "多个B站视频标注为1982年萨尔茨堡音乐节，但转载来源或演奏者信息不明确，需谨慎验证",
          "候选8-10的演奏者与草稿不符",
        ],
        issues: [],
      },
    ]);

    expect(summary.warningCorpus).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          signature: "多个B站视频标注为<year>年<festival>，但转载来源或演奏者信息不明确，需谨慎验证",
          count: 2,
          examples: [
            "多个B站视频标注为1970年波恩音乐节，但转载来源或演奏者信息不明确，需谨慎验证",
            "多个B站视频标注为1982年萨尔茨堡音乐节，但转载来源或演奏者信息不明确，需谨慎验证",
          ],
        }),
        expect.objectContaining({
          signature: "候选#的演奏者与草稿不符",
          count: 2,
        }),
      ]),
    );
  });

  it("normalizes repeated record-number warnings into stable signatures", () => {
    const summary = summarizeRecordingRetrievalAudit([
      {
        recordingId: "recording-a",
        title: "Recording A",
        groupKeys: ["missingReleaseDate"],
        providerStatus: "partial",
        reviewStatus: "ok",
        proposalCount: 1,
        proposalFields: ["releaseDate"],
        warnings: ["记录7、10的年份与草案不符", "记录6、9、10的演奏者与草案不符"],
        issues: [],
      },
      {
        recordingId: "recording-b",
        title: "Recording B",
        groupKeys: ["missingImages"],
        providerStatus: "partial",
        reviewStatus: "ok",
        proposalCount: 1,
        proposalFields: ["notes"],
        warnings: ["记录2、4的年份与草案不符", "记录1、3、8的演奏者与草案不符"],
        issues: [],
      },
    ]);

    expect(summary.warningCorpus).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          signature: "记录#的年份与草案不符",
          count: 2,
        }),
        expect.objectContaining({
          signature: "记录#的演奏者与草案不符",
          count: 2,
        }),
      ]),
    );
  });

  it("normalizes record-number warnings joined by conjunctions", () => {
    const summary = summarizeRecordingRetrievalAudit([
      {
        recordingId: "recording-a",
        title: "Recording A",
        groupKeys: ["missingAlbumTitle"],
        providerStatus: "succeeded",
        reviewStatus: "ok",
        proposalCount: 1,
        proposalFields: ["albumTitle"],
        warnings: ["记录6和9虽提及正确日期地点，但为合集视频而非完整录音"],
        issues: [],
      },
      {
        recordingId: "recording-b",
        title: "Recording B",
        groupKeys: ["missingReleaseDate"],
        providerStatus: "partial",
        reviewStatus: "ok",
        proposalCount: 1,
        proposalFields: ["releaseDate"],
        warnings: ["记录3及5虽提及正确日期地点，但为合集视频而非完整录音"],
        issues: [],
      },
    ]);

    expect(summary.warningCorpus).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          signature: "记录#虽提及正确日期地点，但为合集视频而非完整录音",
          count: 2,
        }),
      ]),
    );
  });

  it("normalizes multi-candidate warnings into stable signatures", () => {
    const summary = summarizeRecordingRetrievalAudit([
      {
        recordingId: "recording-a",
        title: "Recording A",
        groupKeys: ["missingLabel"],
        providerStatus: "partial",
        reviewStatus: "ok",
        proposalCount: 1,
        proposalFields: ["label"],
        warnings: ["候选6、8、9、10因演奏者或年份不符被排除"],
        issues: [],
      },
      {
        recordingId: "recording-b",
        title: "Recording B",
        groupKeys: ["missingImages"],
        providerStatus: "partial",
        reviewStatus: "ok",
        proposalCount: 1,
        proposalFields: ["images"],
        warnings: ["候选2-5因演奏者或年份不符被排除"],
        issues: [],
      },
    ]);

    expect(summary.warningCorpus).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          signature: "候选#因演奏者或年份不符被排除",
          count: 2,
        }),
      ]),
    );
  });

  it("normalizes numbered-url warnings into stable signatures", () => {
    const summary = summarizeRecordingRetrievalAudit([
      {
        recordingId: "recording-a",
        title: "Recording A",
        groupKeys: ["missingLabel"],
        providerStatus: "partial",
        reviewStatus: "needs-attention",
        proposalCount: 1,
        proposalFields: ["label"],
        warnings: ["5、6、8、9、10号URL演奏者或年份不匹配，已排除"],
        issues: [],
      },
      {
        recordingId: "recording-b",
        title: "Recording B",
        groupKeys: ["missingImages"],
        providerStatus: "partial",
        reviewStatus: "needs-attention",
        proposalCount: 1,
        proposalFields: ["images"],
        warnings: ["7号URL演奏者或年份不匹配，已排除"],
        issues: [],
      },
    ]);

    expect(summary.warningCorpus).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          signature: "#号URL演奏者或年份不匹配，已排除",
          count: 2,
        }),
      ]),
    );
  });

  it("formats a markdown archive summary for live audit reports", () => {
    const summary = summarizeRecordingRetrievalAudit([
      {
        recordingId: "recording-a",
        title: "Recording A",
        groupKeys: ["missingReleaseDate"],
        providerStatus: "partial",
        reviewStatus: "needs-attention",
        proposalCount: 1,
        proposalFields: ["releaseDate"],
        warnings: ["记录7、10的年份与草案不符"],
        issues: ["版本提案仍带有来源冲突警告，应用前需要人工复核。"],
      },
      {
        recordingId: "recording-b",
        title: "Recording B",
        groupKeys: ["missingImages"],
        providerStatus: "succeeded",
        reviewStatus: "ok",
        proposalCount: 1,
        proposalFields: ["notes"],
        warnings: ["记录2、4的年份与草案不符"],
        issues: [],
      },
    ]);

    const markdown = formatRecordingRetrievalAuditMarkdown({
      serviceBaseUrl: "http://127.0.0.1:4793",
      sampleSizePerGroup: 1,
      requestTimeoutMs: 90000,
      executionTimeoutMs: 120000,
      plan: {
        groups: [
          { key: "missingReleaseDate", label: "缺发行日期", totalCandidates: 1, selectedRecordingIds: ["recording-a"] },
          { key: "missingImages", label: "缺图片", totalCandidates: 1, selectedRecordingIds: ["recording-b"] },
        ],
        targets: [
          { recordingId: "recording-a", title: "Recording A", groupKeys: ["missingReleaseDate"] },
          { recordingId: "recording-b", title: "Recording B", groupKeys: ["missingImages"] },
        ],
        totalCandidates: 2,
        totalTargets: 2,
      },
      summary,
      samples: [
        {
          recordingId: "recording-a",
          title: "Recording A",
          groupKeys: ["missingReleaseDate"],
          providerStatus: "partial",
          reviewStatus: "needs-attention",
          proposalCount: 1,
          proposalFields: ["releaseDate"],
          warnings: ["记录7、10的年份与草案不符"],
          issues: ["版本提案仍带有来源冲突警告，应用前需要人工复核。"],
        },
        {
          recordingId: "recording-b",
          title: "Recording B",
          groupKeys: ["missingImages"],
          providerStatus: "succeeded",
          reviewStatus: "ok",
          proposalCount: 1,
          proposalFields: ["notes"],
          warnings: ["记录2、4的年份与草案不符"],
          issues: [],
        },
      ],
    });

    expect(markdown).toContain("# Recording Live Audit Report");
    expect(markdown).toContain("- serviceBaseUrl: `http://127.0.0.1:4793`");
    expect(markdown).toContain("## Warning Corpus");
    expect(markdown).toContain("`记录#的年份与草案不符` x2");
    expect(markdown).toContain("## Samples");
    expect(markdown).toContain("### `recording-a` needs-attention");
  });

  it("marks provider failures as needs-attention even when no proposal is returned", () => {
    const result = buildRecordingRetrievalAuditResult({
      target: {
        recordingId: "recording-a",
        title: "Recording A",
        groupKeys: ["missingAlbumTitle"],
      },
      recording: {
        id: "recording-a",
        title: "Recording A",
      },
      providerStatus: "timed_out",
      providerError: "外部检索超时。",
      proposals: [],
      review: {
        status: "already-complete",
        issues: [],
      },
    });

    expect(result.reviewStatus).toBe("needs-attention");
    expect(result.warnings).toEqual(["外部检索超时。"]);
    expect(result.issues).toEqual(["外部检索状态为 timed_out，本轮抽样未得到可直接采纳的版本提案。"]);
  });
});
