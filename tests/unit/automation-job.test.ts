import { describe, expect, it } from "vitest";

import { createAutomationRun } from "@/lib/automation";
import { createAutomationJobManager } from "@/lib/automation-jobs";
import { validateLibrary } from "@/lib/schema";

const library = validateLibrary({
  composers: [
    {
      id: "beethoven",
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
      sortKey: "beethoven",
      summary: "",
    },
  ],
  people: [
    {
      id: "kleiber",
      slug: "kleiber",
      name: "克莱伯",
      fullName: "",
      nameLatin: "Carlos Kleiber",
      displayName: "克莱伯",
      displayFullName: "",
      displayLatinName: "Carlos Kleiber",
      country: "Germany",
      avatarSrc: "",
      roles: ["conductor"],
      aliases: ["Carlos Kleiber"],
      abbreviations: [],
      sortKey: "kleiber",
      summary: "",
    },
  ],
  workGroups: [],
  works: [],
  recordings: [],
});

describe("automation jobs", () => {
  it("marks recording jobs as needing attention when provider metadata conflicts with the known performance year", async () => {
    const recordingLibrary = validateLibrary({
      ...library,
      workGroups: [
        {
          id: "group-beethoven-symphony",
          composerId: "beethoven",
          title: "交响曲",
          slug: "symphony",
          path: ["交响曲"],
          sortKey: "0010",
        },
      ],
      works: [
        {
          id: "work-beethoven-7",
          composerId: "beethoven",
          groupIds: ["group-beethoven-symphony"],
          slug: "beethoven-7",
          title: "第七交响曲",
          titleLatin: "Symphony No. 7",
          aliases: [],
          catalogue: "Op. 92",
          summary: "",
          infoPanel: { text: "", articleId: "", collectionLinks: [], collectionUrl: "" },
          sortKey: "0010",
          updatedAt: "2026-03-25T00:00:00.000Z",
        },
      ],
      recordings: [
        {
          id: "recording-bohm-1976",
          workId: "work-beethoven-7",
          slug: "bohm-1976",
          title: "伯姆 - 维也纳爱乐乐团 - 1976",
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
        },
      ],
    });

    const manager = createAutomationJobManager();
    const job = manager.createJob({
      library: recordingLibrary,
      request: { categories: ["recording"], recordingIds: ["recording-bohm-1976"] },
      runChecksImpl: async () =>
        createAutomationRun(recordingLibrary, {
          categories: ["recording"],
          proposals: [
            {
              id: "recording-bohm-1976-release-date",
              entityType: "recording",
              entityId: "recording-bohm-1976",
              summary: "补充版本检索结果：伯姆 - 维也纳爱乐乐团 - 1976",
              risk: "medium",
              sources: ["https://example.com/release"],
              fields: [{ path: "releaseDate", before: "", after: "1975" }],
              warnings: [],
            },
          ],
        }),
    });

    await manager.waitForJob(job.id);
    const current = manager.getJob(job.id);

    expect(current?.items[0]?.status).toBe("needs-attention");
    expect(current?.items[0]?.reviewIssues).toEqual(expect.arrayContaining(["发行日期早于当前演出日期，疑似提取错误。"]));
    expect(current?.errors[0]?.code).toBe("needs-attention");
  });

  it("stores recording live-audit summaries on completed recording jobs", async () => {
    const recordingLibrary = validateLibrary({
      ...library,
      workGroups: [
        {
          id: "group-beethoven-sonata",
          composerId: "beethoven",
          title: "奏鸣曲",
          slug: "sonata",
          path: ["奏鸣曲"],
          sortKey: "0010",
        },
      ],
      works: [
        {
          id: "work-beethoven-23",
          composerId: "beethoven",
          groupIds: ["group-beethoven-sonata"],
          slug: "beethoven-23",
          title: "第二十三号奏鸣曲“热情”",
          titleLatin: "Piano Sonata No. 23",
          aliases: [],
          catalogue: "Op. 57",
          summary: "",
          infoPanel: { text: "", articleId: "", collectionLinks: [], collectionUrl: "" },
          sortKey: "0010",
          updatedAt: "2026-03-25T00:00:00.000Z",
        },
      ],
      recordings: [
        {
          id: "recording-arrau-1970",
          workId: "work-beethoven-23",
          slug: "arrau-1970",
          title: "阿劳 - Beethovenfest Bonn 1970",
          sortKey: "0010",
          isPrimaryRecommendation: true,
          updatedAt: "2026-03-25T00:00:00.000Z",
          images: [],
          credits: [],
          links: [],
          notes: "",
          performanceDateText: "1970",
          venueText: "Bonn",
          albumTitle: "",
          label: "",
          releaseDate: "",
          infoPanel: { text: "", articleId: "", collectionLinks: [], collectionUrl: "" },
        },
      ],
    });

    const manager = createAutomationJobManager();
    const job = manager.createJob({
      library: recordingLibrary,
      request: { categories: ["recording"], recordingIds: ["recording-arrau-1970"] },
      runChecksImpl: async () =>
        createAutomationRun(recordingLibrary, {
          categories: ["recording"],
          provider: {
            providerName: "recording-retrieval-service",
            requestId: "req-1",
            status: "partial",
            phase: "completed",
            logs: [],
            error: "",
          },
          proposals: [
            {
              id: "recording-arrau-1970-metadata",
              entityType: "recording",
              entityId: "recording-arrau-1970",
              summary: "补充版本检索结果：阿劳 - Beethovenfest Bonn 1970",
              risk: "medium",
              sources: ["https://example.com/album"],
              fields: [
                { path: "albumTitle", before: "", after: "Beethoven Recital" },
                { path: "label", before: "", after: "Philips" },
              ],
              warnings: ["venueText 未达到最终采纳阈值。"],
            },
          ],
        }),
    });

    await manager.waitForJob(job.id);
    const current = manager.getJob(job.id);

    expect(current?.recordingAudit?.summary.totalTargets).toBe(1);
    expect(current?.recordingAudit?.summary.reviewStatusCounts.ok).toBe(1);
    expect(current?.recordingAudit?.results[0]?.groupKeys).toEqual(
      expect.arrayContaining(["missingAlbumTitle", "missingLabel", "missingReleaseDate", "missingImages"]),
    );
    expect(current?.errors.some((error) => error.entityType === "recording" && error.message.includes("录音在线审计"))).toBe(false);
  });

  it("creates an async job with progress and structured failures", async () => {
    const manager = createAutomationJobManager();
    const job = manager.createJob({
      library,
      request: { categories: ["composer", "conductor"] },
      fetchImpl: async () => {
        throw new Error("network down");
      },
    });

    await manager.waitForJob(job.id);
    const current = manager.getJob(job.id);

    expect(current?.status).toBe("completed");
    expect(current?.progress.total).toBe(2);
    expect(current?.progress.processed).toBe(2);
    expect(current?.progress.failed).toBe(0);
    expect(current?.progress.attention).toBe(2);
    expect(current?.items.every((item) => item.status === "needs-attention")).toBe(true);
    expect(current?.errors[0]?.code).toBe("needs-attention");
  });

  it("runs selected items concurrently and stores per-item statuses", async () => {
    const multiComposerLibrary = validateLibrary({
      ...library,
      composers: [
        {
          ...library.composers[0],
          id: "beethoven",
          slug: "beethoven",
          name: "贝多芬",
          displayName: "贝多芬",
        },
        {
          ...library.composers[0],
          id: "bruckner",
          slug: "bruckner",
          name: "布鲁克纳",
          nameLatin: "Anton Bruckner",
          displayName: "布鲁克纳",
          displayLatinName: "Anton Bruckner",
        },
        {
          ...library.composers[0],
          id: "mahler",
          slug: "mahler",
          name: "马勒",
          nameLatin: "Gustav Mahler",
          displayName: "马勒",
          displayLatinName: "Gustav Mahler",
        },
      ],
      people: [],
    });

    let active = 0;
    let maxActive = 0;
    const manager = createAutomationJobManager();
    const job = manager.createJob({
      library: multiComposerLibrary,
      request: { categories: ["composer"] },
      maxConcurrency: 3,
      runChecksImpl: async (_library, request) => {
        const composerId = request.composerIds?.[0] || "unknown";
        active += 1;
        maxActive = Math.max(maxActive, active);
        await new Promise((resolve) => setTimeout(resolve, 40));
        active -= 1;
        return createAutomationRun(multiComposerLibrary, {
          categories: ["composer"],
          proposals: [
            {
              id: `${composerId}-proposal`,
              entityType: "composer",
              entityId: composerId,
              summary: `补充 ${composerId}`,
              risk: "low",
              sources: ["https://example.com/source"],
              fields: [
                { path: "fullName", before: "", after: `${composerId}-full` },
                { path: "displayFullName", before: "", after: `${composerId}-full` },
              ],
            },
          ],
        });
      },
    });

    await manager.waitForJob(job.id);
    const current = manager.getJob(job.id);

    expect(maxActive).toBeGreaterThan(1);
    expect(current?.items).toHaveLength(3);
    expect(current?.items.every((item) => item.status === "needs-attention")).toBe(true);
    expect(current?.progress.succeeded).toBe(0);
    expect(current?.progress.attention).toBe(3);
  });

  it("marks a finished item as needing attention when post-check still finds missing fields", async () => {
    const manager = createAutomationJobManager();
    const job = manager.createJob({
      library,
      request: { categories: ["composer"], composerIds: ["beethoven"] },
      runChecksImpl: async () =>
        createAutomationRun(library, {
          categories: ["composer"],
          proposals: [
            {
              id: "beethoven-country-only",
              entityType: "composer",
              entityId: "beethoven",
              summary: "只补国家",
              risk: "low",
              sources: ["https://example.com/source"],
              fields: [{ path: "country", before: "Germany", after: "Austria" }],
            },
          ],
        }),
    });

    await manager.waitForJob(job.id);
    const current = manager.getJob(job.id);

    expect(current?.progress.failed).toBe(0);
    expect(current?.progress.succeeded).toBe(0);
    expect(current?.progress.attention).toBe(1);
    expect(current?.items[0]?.status).toBe("needs-attention");
    expect(current?.items[0]?.reviewIssues?.some((message) => message.includes("全名") || message.includes("规范"))).toBe(true);
  });

  it("uses llm review to keep a work proposal in attention state when the reviewer rejects it", async () => {
    const workLibrary = validateLibrary({
      composers: [
        {
          id: "beethoven",
          slug: "beethoven",
          name: "Beethoven",
          fullName: "Ludwig van Beethoven",
          nameLatin: "Ludwig van Beethoven",
          displayName: "Beethoven",
          displayFullName: "Ludwig van Beethoven",
          displayLatinName: "Ludwig van Beethoven",
          country: "Germany",
          avatarSrc: "",
          aliases: [],
          abbreviations: [],
          sortKey: "0010",
          summary: "German composer.",
        },
      ],
      people: [],
      workGroups: [
        {
          id: "group-symphony",
          composerId: "beethoven",
          title: "Symphony",
          slug: "symphony",
          path: ["Symphony"],
          sortKey: "0010",
        },
      ],
      works: [
        {
          id: "beethoven-7",
          composerId: "beethoven",
          groupIds: ["group-symphony"],
          slug: "beethoven-7",
          title: "Symphony No. 7",
          titleLatin: "",
          aliases: [],
          catalogue: "",
          summary: "",
          infoPanel: { text: "", articleId: "", collectionUrl: "" },
          sortKey: "0010",
          updatedAt: "2026-03-17T00:00:00.000Z",
        },
      ],
      recordings: [],
    });

    const manager = createAutomationJobManager();
    const job = manager.createJob({
      library: workLibrary,
      request: { categories: ["work"], workIds: ["beethoven-7"] },
      llmConfig: {
        enabled: true,
        baseUrl: "https://api.example.com/v1",
        apiKey: "secret-key",
        model: "deepseek-chat",
        timeoutMs: 30000,
      },
      fetchImpl: async (url) => {
        const value = String(url);
        if (value.includes("/chat/completions")) {
          return new Response(
            JSON.stringify({
              choices: [
                {
                  message: {
                    content: JSON.stringify({
                      status: "needs-attention",
                      issues: ["仅有 LLM 来源，缺少可交叉验证的外部依据。"],
                      confidence: 0.62,
                      rationale: "This proposal is plausible but not grounded.",
                    }),
                  },
                },
              ],
            }),
            { status: 200 },
          );
        }
        throw new Error(`unexpected fetch: ${value}`);
      },
      runChecksImpl: async () =>
        createAutomationRun(workLibrary, {
          categories: ["work"],
          proposals: [
            {
              id: "beethoven-7-proposal",
              entityType: "work",
              entityId: "beethoven-7",
              summary: "fill work fields",
              risk: "low",
              sources: ["https://api.example.com/v1/chat/completions"],
              fields: [
                { path: "titleLatin", before: "", after: "Symphony No. 7 in A major, Op. 92" },
                { path: "catalogue", before: "", after: "Op. 92" },
                { path: "summary", before: "", after: "贝多芬的重要交响曲之一。" },
              ],
              evidence: [
                {
                  field: "summary",
                  sourceLabel: "LLM",
                  sourceUrl: "https://api.example.com/v1/chat/completions",
                  confidence: 0.62,
                },
              ],
            },
          ],
        }),
    });

    await manager.waitForJob(job.id);
    const current = manager.getJob(job.id);

    expect(current?.items[0]?.status).toBe("needs-attention");
    expect(current?.items[0]?.reviewIssues).toContain("仅有 LLM 来源，缺少可交叉验证的外部依据。");
  });

  it("keeps a proposal in attention state when llm accepts it with low confidence", async () => {
    const workLibrary = validateLibrary({
      composers: [
        {
          id: "beethoven",
          slug: "beethoven",
          name: "Beethoven",
          fullName: "Ludwig van Beethoven",
          nameLatin: "Ludwig van Beethoven",
          displayName: "Beethoven",
          displayFullName: "Ludwig van Beethoven",
          displayLatinName: "Ludwig van Beethoven",
          country: "Germany",
          avatarSrc: "",
          aliases: [],
          abbreviations: [],
          sortKey: "0010",
          summary: "German composer.",
        },
      ],
      people: [],
      workGroups: [
        {
          id: "group-symphony",
          composerId: "beethoven",
          title: "Symphony",
          slug: "symphony",
          path: ["Symphony"],
          sortKey: "0010",
        },
      ],
      works: [
        {
          id: "beethoven-5",
          composerId: "beethoven",
          groupIds: ["group-symphony"],
          slug: "beethoven-5",
          title: "Symphony No. 5",
          titleLatin: "",
          aliases: [],
          catalogue: "",
          summary: "",
          infoPanel: { text: "", articleId: "", collectionUrl: "" },
          sortKey: "0020",
          updatedAt: "2026-03-17T00:00:00.000Z",
        },
      ],
      recordings: [],
    });

    const manager = createAutomationJobManager();
    const job = manager.createJob({
      library: workLibrary,
      request: { categories: ["work"], workIds: ["beethoven-5"] },
      llmConfig: {
        enabled: true,
        baseUrl: "https://api.example.com/v1",
        apiKey: "secret-key",
        model: "deepseek-chat",
        timeoutMs: 30000,
      },
      fetchImpl: async (url) => {
        const value = String(url);
        if (value.includes("/chat/completions")) {
          return new Response(
            JSON.stringify({
              choices: [
                {
                  message: {
                    content: JSON.stringify({
                      verdict: "accept",
                      status: "ok",
                      issues: [],
                      reasons: [],
                      confidence: 0.42,
                      rationale: "Low-confidence normalization.",
                    }),
                  },
                },
              ],
            }),
            { status: 200 },
          );
        }
        throw new Error(`unexpected fetch: ${value}`);
      },
      runChecksImpl: async () =>
        createAutomationRun(workLibrary, {
          categories: ["work"],
          proposals: [
            {
              id: "beethoven-5-proposal",
              entityType: "work",
              entityId: "beethoven-5",
              summary: "fill work fields",
              risk: "low",
              sources: ["https://api.example.com/v1/chat/completions"],
              fields: [{ path: "titleLatin", before: "", after: "Symphony No. 5 in C minor, Op. 67" }],
              evidence: [
                {
                  field: "titleLatin",
                  sourceLabel: "LLM",
                  sourceUrl: "https://api.example.com/v1/chat/completions",
                  confidence: 0.42,
                },
              ],
            },
          ],
        }),
    });

    await manager.waitForJob(job.id);
    const current = manager.getJob(job.id);

    expect(current?.items[0]?.status).toBe("needs-attention");
    expect(current?.items[0]?.reviewIssues).toContain("LLM 复核置信度过低：0.42");
  });

  it("builds a concrete selection preview before running a job", () => {
    const manager = createAutomationJobManager();
    const preview = manager.previewSelection(library, {
      categories: ["composer", "conductor"],
      composerIds: ["beethoven"],
      conductorIds: ["kleiber"],
    });

    expect(preview.groups.find((group) => group.category === "composer")?.items).toHaveLength(1);
    expect(preview.groups.find((group) => group.category === "conductor")?.items).toHaveLength(1);
    expect(preview.total).toBe(2);
  });

  it("filters people previews by related recordings when composer or work constraints are present", () => {
    const relatedLibrary = validateLibrary({
      composers: library.composers,
      people: [
        ...library.people,
        {
          id: "abbado",
          slug: "abbado",
          name: "\u963f\u5df4\u591a",
          fullName: "",
          nameLatin: "Claudio Abbado",
          displayName: "\u963f\u5df4\u591a",
          displayFullName: "",
          displayLatinName: "Claudio Abbado",
          country: "Italy",
          avatarSrc: "",
          roles: ["conductor"],
          aliases: [],
          abbreviations: [],
          sortKey: "abbado",
          summary: "",
          infoPanel: { text: "", articleId: "", collectionUrl: "" },
          imageSourceUrl: "",
          imageSourceKind: "",
          imageAttribution: "",
          imageUpdatedAt: "",
        },
        {
          id: "vpo",
          slug: "vpo",
          name: "\u7ef4\u4e5f\u7eb3\u7231\u4e50",
          fullName: "",
          nameLatin: "Wiener Philharmoniker",
          displayName: "\u7ef4\u4e5f\u7eb3\u7231\u4e50",
          displayFullName: "",
          displayLatinName: "Wiener Philharmoniker",
          country: "Austria",
          avatarSrc: "",
          roles: ["orchestra"],
          aliases: [],
          abbreviations: ["VPO"],
          sortKey: "vpo",
          summary: "",
          infoPanel: { text: "", articleId: "", collectionUrl: "" },
          imageSourceUrl: "",
          imageSourceKind: "",
          imageAttribution: "",
          imageUpdatedAt: "",
        },
      ],
      workGroups: [
        {
          id: "group-beethoven-symphony",
          composerId: "beethoven",
          title: "\u4ea4\u54cd\u66f2",
          slug: "symphony",
          path: ["\u4ea4\u54cd\u66f2"],
          sortKey: "0010",
        },
      ],
      works: [
        {
          id: "beethoven-7",
          composerId: "beethoven",
          groupIds: ["group-beethoven-symphony"],
          slug: "beethoven-7",
          title: "\u7b2c\u4e03\u4ea4\u54cd\u66f2",
          titleLatin: "",
          aliases: [],
          catalogue: "",
          summary: "",
          infoPanel: { text: "", articleId: "", collectionUrl: "" },
          sortKey: "0010",
          updatedAt: "2026-03-13T00:00:00.000Z",
        },
      ],
      recordings: [
        {
          id: "recording-beethoven-7-abbado",
          workId: "beethoven-7",
          slug: "recording-beethoven-7-abbado",
          title: "\u963f\u5df4\u591a 1988",
          sortKey: "0010",
          isPrimaryRecommendation: false,
          updatedAt: "2026-03-13T00:00:00.000Z",
          images: [],
          credits: [
            { role: "conductor", personId: "abbado", displayName: "\u963f\u5df4\u591a" },
            { role: "orchestra", personId: "vpo", displayName: "\u7ef4\u4e5f\u7eb3\u7231\u4e50" },
          ],
          links: [],
          notes: "",
          performanceDateText: "1988",
          venueText: "",
          albumTitle: "",
          label: "",
          releaseDate: "",
          infoPanel: { text: "", articleId: "", collectionUrl: "" },
        },
      ],
    });

    const manager = createAutomationJobManager();
    const conductorPreview = manager.previewSelection(relatedLibrary, {
      categories: ["conductor"],
      composerIds: ["beethoven"],
    });
    const orchestraPreview = manager.previewSelection(relatedLibrary, {
      categories: ["orchestra"],
      workIds: ["beethoven-7"],
    });

    expect(conductorPreview.groups[0]?.items.map((item) => item.entityId)).toEqual(["abbado"]);
    expect(orchestraPreview.groups[0]?.items.map((item) => item.entityId)).toEqual(["vpo"]);
  });

  it("deduplicates identical proposal ids when a job merges multiple per-item runs", async () => {
    const duplicateLibrary = validateLibrary({
      ...library,
      people: [
        {
          ...library.people[0],
          id: "berlin-phil",
          slug: "berlin-phil",
          name: "柏林爱乐乐团",
          nameLatin: "Berliner Philharmoniker",
          displayName: "柏林爱乐乐团",
          displayLatinName: "Berliner Philharmoniker",
          roles: ["orchestra"],
          aliases: ["BPO"],
          abbreviations: ["BPO"],
          sortKey: "0100",
        },
        {
          ...library.people[0],
          id: "berlin-phil-duplicate",
          slug: "berlin-phil-duplicate",
          name: "柏林爱乐",
          nameLatin: "Berliner Philharmoniker",
          displayName: "柏林爱乐",
          displayLatinName: "Berliner Philharmoniker",
          roles: ["orchestra"],
          aliases: ["Berlin Philharmonic Orchestra"],
          abbreviations: ["BPO"],
          sortKey: "0101",
        },
      ],
    });

    const manager = createAutomationJobManager();
    const job = manager.createJob({
      library: duplicateLibrary,
      request: { categories: ["orchestra"], orchestraIds: ["berlin-phil", "berlin-phil-duplicate"] },
      runChecksImpl: async (_library, request) =>
        createAutomationRun(duplicateLibrary, {
          categories: ["orchestra"],
          proposals: [
            {
              id: "merge-berlin-phil|berlin-phil-duplicate",
              kind: "merge",
              entityType: "person",
              entityId: request.orchestraIds?.[0] || "berlin-phil",
              summary: "疑似重复人物：柏林爱乐乐团 / 柏林爱乐",
              risk: "high",
              sources: [],
              fields: [],
              mergeCandidates: [
                {
                  targetId: "berlin-phil-duplicate",
                  targetLabel: "柏林爱乐",
                  reason: "共享规范化名称",
                },
              ],
            },
          ],
        }),
    });

    await manager.waitForJob(job.id);
    const current = manager.getJob(job.id);

    expect(current?.run?.proposals).toHaveLength(1);
    expect(current?.run?.proposals[0]?.id).toBe("merge-berlin-phil|berlin-phil-duplicate");
  });

  it("previews work selections directly when requesting work auto-check", () => {
    const libraryWithWork = validateLibrary({
      ...library,
      workGroups: [
        {
          id: "group-beethoven-symphony",
          composerId: "beethoven",
          title: "交响曲",
          slug: "symphony",
          path: ["交响曲"],
          sortKey: "0010",
        },
      ],
      works: [
        {
          id: "beethoven-7",
          composerId: "beethoven",
          groupIds: ["group-beethoven-symphony"],
          slug: "beethoven-7",
          title: "第七交响曲",
          titleLatin: "Symphony No. 7",
          aliases: [],
          catalogue: "Op. 92",
          summary: "",
          infoPanel: { text: "", articleId: "", collectionUrl: "" },
          sortKey: "0010",
          updatedAt: "2026-03-15T00:00:00.000Z",
        },
      ],
      recordings: [],
    });

    const manager = createAutomationJobManager();
    const preview = manager.previewSelection(libraryWithWork, {
      categories: ["work"],
      workIds: ["beethoven-7"],
    });

    expect(preview.total).toBe(1);
    expect(preview.groups[0]?.category).toBe("work");
    expect(preview.groups[0]?.items[0]).toEqual(
      expect.objectContaining({
        entityId: "beethoven-7",
        label: "第七交响曲",
        description: "Symphony No. 7 / Op. 92",
      }),
    );
  });
});
