import { describe, expect, it } from "vitest";

import {
  applyAutomationProposal,
  applyPendingAutomationProposals,
  canApplyAutomationProposal,
  createAutomationRun,
  ignoreAutomationProposal,
  ignorePendingAutomationProposals,
  rankImageCandidates,
  revertAutomationProposal,
  summarizeAutomationRun,
  updateAutomationProposalReview,
} from "@/lib/automation";
import type {
  RecordingRetrievalProvider,
  RecordingRetrievalRequest,
  RecordingRetrievalResultItem,
} from "@/lib/recording-retrieval";
import { runAutomationChecks } from "@/lib/automation-checks";
import { validateLibrary } from "@/lib/schema";

const library = validateLibrary({
  composers: [
    {
      id: "beethoven",
      slug: "beethoven",
      name: "贝多芬",
      fullName: "路德维希·凡·贝多芬",
      nameLatin: "Ludwig van Beethoven",
      country: "Germany",
      avatarSrc: "",
      aliases: [],
      sortKey: "beethoven",
      summary: "德国作曲家。",
    },
  ],
  people: [
    {
      id: "kleiber",
      slug: "kleiber",
      name: "卡洛斯·克莱伯",
      fullName: "Carlos Kleiber",
      nameLatin: "Carlos Kleiber",
      country: "Germany",
      avatarSrc: "",
      roles: ["conductor"],
      aliases: ["Carlos Kleiber"],
      sortKey: "kleiber",
      summary: "著名指挥家。",
    },
    {
      id: "kleiber-duplicate",
      slug: "kleiber-duplicate",
      name: "老克莱伯",
      fullName: "Carlos Kleiber",
      nameLatin: "Carlos Kleiber",
      country: "Germany",
      avatarSrc: "",
      roles: ["conductor"],
      aliases: ["Carlos Kleiber"],
      sortKey: "kleiber-duplicate",
      summary: "重复条目。",
    },
    {
      id: "pollini",
      slug: "pollini",
      name: "波里尼",
      fullName: "Maurizio Pollini",
      nameLatin: "Maurizio Pollini",
      country: "Italy",
      avatarSrc: "",
      roles: ["soloist"],
      aliases: ["Maurizio Pollini"],
      sortKey: "pollini",
      summary: "钢琴家。",
    },
  ],
  workGroups: [
    {
      id: "beethoven-sonata",
      composerId: "beethoven",
      title: "钢琴奏鸣曲",
      slug: "sonata",
      path: ["钢琴奏鸣曲"],
      sortKey: "0100",
    },
  ],
  works: [
    {
      id: "appassionata",
      composerId: "beethoven",
      groupIds: ["beethoven-sonata"],
      slug: "appassionata",
      title: "第二十三钢琴奏鸣曲“热情”",
      titleLatin: "Piano Sonata No. 23 'Appassionata'",
      aliases: [],
      catalogue: "Op. 57",
      summary: "贝多芬钢琴奏鸣曲。",
      sortKey: "0200",
      updatedAt: "2026-03-08T00:00:00.000Z",
    },
  ],
  recordings: [
    {
      id: "cutner-appassionata",
      workId: "appassionata",
      slug: "cutner-appassionata",
      title: "Solomon Cutner",
      sortKey: "0100",
      isPrimaryRecommendation: true,
      updatedAt: "2026-03-08T00:00:00.000Z",
      images: [],
      credits: [
        { role: "conductor", displayName: "Carlos Kleiber", personId: "kleiber" },
        { role: "soloist", displayName: "Maurizio Pollini", personId: "pollini" },
      ],
      links: [{ platform: "bilibili", url: "https://www.bilibili.com/video/BV1Qd4y1B7N9", title: "BV1Qd4y1B7N9" }],
      notes: "",
      performanceDateText: "",
      venueText: "",
      albumTitle: "",
      label: "",
      releaseDate: "",
    },
  ],
});

function createMockRecordingProvider(
  itemsBuilder: () => RecordingRetrievalResultItem[] = () => [],
): RecordingRetrievalProvider {
  const acceptedAt = "2026-03-15T00:00:00.000Z";
  return {
    name: "recording-retrieval-service",
    protocolVersion: "v1",
    checkHealth: async () => ({
      service: "recording-retrieval-service",
      version: "1.0.0",
      protocolVersion: "v1",
      status: "ok",
    }),
    createJob: async (request: RecordingRetrievalRequest) => ({
      jobId: "provider-job-1",
      requestId: request.requestId,
      status: "accepted",
      itemCount: request.items.length,
      acceptedAt,
    }),
    getJob: async () => ({
      jobId: "provider-job-1",
      requestId: "request-1",
      status: "succeeded",
      progress: {
        total: 1,
        completed: 1,
        succeeded: 1,
        partial: 0,
        failed: 0,
        notFound: 0,
      },
      items: [{ itemId: "cutner-appassionata", status: "succeeded" }],
      logs: [{ timestamp: acceptedAt, message: "done" }],
    }),
    getResults: async () => ({
      jobId: "provider-job-1",
      requestId: "request-1",
      status: "succeeded",
      completedAt: acceptedAt,
      items: itemsBuilder(),
    }),
    cancelJob: async () => ({
      jobId: "provider-job-1",
      requestId: "request-1",
      status: "canceled",
      progress: {
        total: 1,
        completed: 1,
        succeeded: 0,
        partial: 0,
        failed: 1,
        notFound: 0,
      },
      items: [{ itemId: "cutner-appassionata", status: "failed" }],
      logs: [{ timestamp: acceptedAt, message: "canceled" }],
    }),
  };
}

describe("automation proposals", () => {
  it("applies, ignores and reverts proposals without mutating unrelated data", () => {
    const run = createAutomationRun(library, {
      categories: ["artist"],
      proposals: [
        {
          id: "proposal-1",
          entityType: "person",
          entityId: "kleiber",
          summary: "补充图片来源与国家别名",
          risk: "low",
          sources: ["https://commons.wikimedia.org/wiki/File:Carlos_Kleiber.jpg"],
          fields: [
            { path: "avatarSrc", before: "", after: "/library-assets/people/kleiber-a1.jpg" },
            { path: "imageSourceUrl", before: "", after: "https://commons.wikimedia.org/wiki/File:Carlos_Kleiber.jpg" },
            { path: "aliases", before: ["Carlos Kleiber"], after: ["Carlos Kleiber", "克莱伯"] },
          ],
        },
      ],
    });

    const applied = applyAutomationProposal(library, run, "proposal-1");
    expect(applied.library.people[0]?.avatarSrc).toBe("/library-assets/people/kleiber-a1.jpg");
    expect(applied.library.people[0]?.aliases).toContain("克莱伯");
    expect(applied.snapshot.after.avatarSrc).toBe("/library-assets/people/kleiber-a1.jpg");

    const reverted = revertAutomationProposal(applied.library, applied.run, applied.snapshot.id);
    expect(reverted.people[0]?.avatarSrc).toBe("");
    expect(reverted.people[0]?.aliases).toEqual(["Carlos Kleiber"]);

    const ignoredRun = ignoreAutomationProposal(run, "proposal-1");
    expect(ignoredRun.proposals[0]?.status).toBe("ignored");
  });

  it("supports batch apply and batch ignore for pending proposals", () => {
    const run = createAutomationRun(library, {
      categories: ["recording"],
      proposals: [
        {
          id: "proposal-a",
          entityType: "recording",
          entityId: "cutner-appassionata",
          summary: "修正平台",
          risk: "low",
          sources: ["https://www.youtube.com/watch?v=SayJA16R0ZQ"],
          fields: [{ path: "links[0].platform", before: "bilibili", after: "youtube" }],
        },
        {
          id: "proposal-b",
          entityType: "person",
          entityId: "kleiber",
          summary: "仅供审查",
          kind: "merge",
          risk: "high",
          sources: [],
          fields: [],
        },
      ],
    });

    const applied = applyPendingAutomationProposals(library, run);
    expect(applied.run.summary.applied).toBe(1);
    expect(applied.library.recordings[0]?.links[0]?.platform).toBe("youtube");

    const ignored = ignorePendingAutomationProposals(run);
    expect(ignored.summary.ignored).toBe(2);
  });

  it("rejects high-risk and merge proposals from direct apply eligibility", () => {
    expect(
      canApplyAutomationProposal({
        id: "proposal-safe",
        entityType: "person",
        entityId: "kleiber",
        summary: "安全候选",
        risk: "low",
        sources: [],
        fields: [{ path: "country", before: "Germany", after: "Austria" }],
      }),
    ).toBe(true);

    expect(
      canApplyAutomationProposal({
        id: "proposal-high-risk",
        entityType: "person",
        entityId: "kleiber",
        summary: "高风险候选",
        risk: "high",
        sources: [],
        fields: [{ path: "country", before: "Germany", after: "Austria" }],
      }),
    ).toBe(false);

    expect(
      canApplyAutomationProposal({
        id: "proposal-merge",
        entityType: "person",
        entityId: "kleiber",
        summary: "合并候选",
        kind: "merge",
        risk: "high",
        sources: [],
        fields: [],
      }),
    ).toBe(false);
  });

  it("synchronizes proposal status with review state", () => {
    const run = createAutomationRun(library, {
      categories: ["artist"],
      proposals: [
        {
          id: "proposal-review",
          entityType: "person",
          entityId: "kleiber",
          summary: "同步 review 与 status",
          risk: "low",
          sources: [],
          fields: [{ path: "country", before: "Germany", after: "Austria" }],
        },
      ],
    });

    const viewed = updateAutomationProposalReview(run, "proposal-review", "viewed");
    expect(viewed.proposals[0]?.reviewState).toBe("viewed");
    expect(viewed.proposals[0]?.status).toBe("pending");

    const discarded = updateAutomationProposalReview(viewed, "proposal-review", "discarded");
    expect(discarded.proposals[0]?.reviewState).toBe("discarded");
    expect(discarded.proposals[0]?.status).toBe("ignored");

    const confirmed = updateAutomationProposalReview(discarded, "proposal-review", "confirmed");
    expect(confirmed.proposals[0]?.reviewState).toBe("confirmed");
    expect(confirmed.proposals[0]?.status).toBe("pending");
  });

  it("ranks image candidates by score before selection", () => {
    const ranked = rankImageCandidates(
      {
        title: "Carlos Kleiber",
        entityKind: "person",
      },
      [
        {
          id: "low",
          src: "https://example.com/low.jpg",
          sourceUrl: "https://example.com/low",
          sourceKind: "other",
          width: 320,
          height: 120,
          attribution: "",
        },
        {
          id: "high",
          src: "https://commons.wikimedia.org/high.jpg",
          sourceUrl: "https://commons.wikimedia.org/wiki/File:Carlos_Kleiber.jpg",
          sourceKind: "wikimedia-commons",
          width: 1200,
          height: 1200,
          attribution: "Wikimedia Commons",
        },
        {
          id: "logo",
          src: "https://baike.baidu.com/logo.png",
          sourceUrl: "https://baike.baidu.com/logo.png",
          sourceKind: "official-site",
          width: 1200,
          height: 1200,
          attribution: "Baidu Baike logo",
          title: "Baidu logo",
        },
      ],
    );

    expect(ranked[0]?.id).toBe("high");
    expect(ranked.at(-1)?.id).toBe("logo");
    expect(ranked[0]?.score).toBeGreaterThan(ranked[1]?.score ?? 0);
  });
});

describe("automation checks", () => {
  it("filters recordings by conductor and produces merge review proposals for duplicate people", async () => {
    const fetchImpl: typeof fetch = async (url) => {
      const value = String(url);
      if (value.includes("w/api.php")) {
        return new Response(JSON.stringify({ query: { search: [{ title: "Carlos Kleiber" }] } }), { status: 200 });
      }
      if (value.includes("google.com/search")) {
        return new Response(
          '<html><body><a href="/url?q=https%3A%2F%2Fwww.example-classical.com%2Frecordings%2Fkleiber-beethoven&sa=U">result</a></body></html>',
          { status: 200 },
        );
      }
      if (value.includes("api/rest_v1/page/summary")) {
        return new Response(
          JSON.stringify({
            extract: "Carlos Kleiber was a German conductor.",
            description: "German conductor",
            content_urls: { desktop: { page: "https://en.wikipedia.org/wiki/Carlos_Kleiber" } },
            originalimage: { source: "https://upload.wikimedia.org/example.jpg" },
            title: "Carlos Kleiber",
          }),
          { status: 200 },
        );
      }
      if (value.includes("www.example-classical.com/recordings/kleiber-beethoven")) {
        return new Response(
          '<html><head><title>Kleiber Beethoven</title><meta property="og:title" content="Kleiber Beethoven Appassionata"><meta property="og:description" content="Label: DG | Venue: Vienna | 1963-01-01"><meta property="og:image" content="https://cdn.example.com/cover.jpg"></head></html>',
          { status: 200 },
        );
      }
      return new Response("<html></html>", { status: 200 });
    };
    const recordingProvider = createMockRecordingProvider(() => [
      {
        itemId: "cutner-appassionata",
        status: "succeeded",
        confidence: 0.84,
        warnings: [],
        result: {
          label: "DG",
          releaseDate: "1963",
          links: [{ url: "https://www.example-classical.com/recordings/kleiber-beethoven", platform: "other" }],
        },
        evidence: [{ field: "label", sourceUrl: "https://www.example-classical.com/recordings/kleiber-beethoven", sourceLabel: "Example", confidence: 0.84 }],
        linkCandidates: [{ url: "https://www.example-classical.com/recordings/kleiber-beethoven", platform: "other", confidence: 0.84 }],
        imageCandidates: [],
        logs: [{ timestamp: "2026-03-15T00:00:00.000Z", message: "done" }],
      },
    ]);

    const run = await runAutomationChecks(
      library,
      {
        categories: ["conductor", "recording"],
        conductorIds: ["kleiber"],
      },
      fetchImpl,
      undefined,
      { recordingProvider },
    );

    expect(run.categories).toEqual(["conductor", "recording"]);
    expect(run.notes.some((note) => note.includes("人物检查"))).toBe(true);
    expect(run.proposals.some((proposal) => proposal.summary.includes("疑似重复人物"))).toBe(true);
    expect(run.proposals.some((proposal) => proposal.entityType === "recording")).toBe(true);
  });

  it("limits person merge proposals to the requested orchestra scope instead of scanning unrelated duplicates", async () => {
    const scopedLibrary = validateLibrary({
      ...library,
      people: [
        ...library.people,
        {
          id: "bpo",
          slug: "berlin-phil",
          name: "柏林爱乐乐团",
          fullName: "柏林爱乐乐团",
          nameLatin: "Berliner Philharmoniker",
          country: "Germany",
          avatarSrc: "",
          roles: ["orchestra"],
          aliases: ["Berlin Philharmonic Orchestra"],
          sortKey: "bpo",
          summary: "德国乐团。",
        },
        {
          id: "bpo-duplicate",
          slug: "berlin-phil-duplicate",
          name: "柏林爱乐",
          fullName: "柏林爱乐乐团",
          nameLatin: "Berliner Philharmoniker",
          country: "Germany",
          avatarSrc: "",
          roles: ["orchestra"],
          aliases: ["Berlin Philharmonic Orchestra"],
          sortKey: "bpo-duplicate",
          summary: "重复乐团。",
        },
        {
          id: "munich-phil",
          slug: "munich-phil",
          name: "慕尼黑爱乐乐团",
          fullName: "慕尼黑爱乐乐团",
          nameLatin: "Munich Philharmonic Orchestra",
          country: "Germany",
          avatarSrc: "",
          roles: ["orchestra"],
          aliases: ["Munich Philharmonic Orchestra"],
          sortKey: "munich-phil",
          summary: "德国乐团。",
        },
        {
          id: "munich-phil-duplicate",
          slug: "munich-phil-duplicate",
          name: "慕尼黑爱乐",
          fullName: "慕尼黑爱乐乐团",
          nameLatin: "Munich Philharmonic Orchestra",
          country: "Germany",
          avatarSrc: "",
          roles: ["orchestra"],
          aliases: ["Munich Philharmonic Orchestra"],
          sortKey: "munich-phil-duplicate",
          summary: "重复乐团。",
        },
      ],
    });

    const run = await runAutomationChecks(scopedLibrary, { categories: ["orchestra"], orchestraIds: ["bpo"] }, async () => {
      throw new Error("blocked");
    });
    const mergeSummaries = run.proposals.filter((proposal) => proposal.kind === "merge").map((proposal) => proposal.summary);

    expect(mergeSummaries).toHaveLength(1);
    expect(mergeSummaries[0]).toContain("柏林爱乐");
    expect(mergeSummaries[0]).not.toContain("慕尼黑爱乐");
  });

  it("creates recording proposals from the external retrieval provider without mutating formal data", async () => {
    const recordingLibrary = validateLibrary({
      ...library,
      recordings: [
        {
          ...library.recordings[0],
          images: [
            {
              src: "/library-assets/legacy/recordings/missing-cover.jpg",
              alt: "broken local cover",
            },
          ],
          links: [{ platform: "youtube", url: "https://www.youtube.com/watch?v=SayJA16R0ZQ", title: "Video" }],
        },
      ],
    });

    const run = await runAutomationChecks(recordingLibrary, { categories: ["recording"] }, fetch, undefined, {
      recordingProvider: createMockRecordingProvider(() => [
        {
          itemId: "cutner-appassionata",
          status: "succeeded",
          confidence: 0.91,
          warnings: [],
          result: {
            links: [{ url: "https://www.youtube.com/watch?v=SayJA16R0ZQ", platform: "youtube" }],
            images: [{ src: "https://cdn.example.com/cover.jpg", sourceUrl: "https://cdn.example.com/cover.jpg", sourceKind: "official-site" }],
          },
          evidence: [],
          linkCandidates: [{ url: "https://www.youtube.com/watch?v=SayJA16R0ZQ", platform: "youtube" }],
          imageCandidates: [{ src: "https://cdn.example.com/cover.jpg", sourceUrl: "https://cdn.example.com/cover.jpg", sourceKind: "official-site" }],
          logs: [{ timestamp: "2026-03-15T00:00:00.000Z", message: "done" }],
        },
      ]),
    });
    const coverProposal = run.proposals.find((proposal) => proposal.entityType === "recording");

    expect(coverProposal).toBeTruthy();
    expect(coverProposal?.imageCandidates?.length).toBeGreaterThan(0);
    expect(recordingLibrary.recordings[0]?.images).toHaveLength(1);
  });

  it("fails explicitly when recording checks run without an external retrieval provider", async () => {
    await expect(runAutomationChecks(library, { categories: ["recording"] }, fetch)).rejects.toThrow(
      "版本自动检索工具未配置或不可用",
    );
  });

  it("drops suspicious baidu logo image candidates for named entities", async () => {
    const namedLibrary = validateLibrary({
      composers: [
        {
          id: "berlioz",
          slug: "berlioz",
          name: "柏辽兹",
          fullName: "",
          nameLatin: "Hector Berlioz",
          displayName: "柏辽兹",
          displayFullName: "",
          displayLatinName: "Hector Berlioz",
          country: "",
          avatarSrc: "",
          aliases: [],
          abbreviations: [],
          sortKey: "berlioz",
          summary: "",
        },
      ],
      people: [],
      workGroups: [],
      works: [],
      recordings: [],
    });

    const run = await runAutomationChecks(
      namedLibrary,
      { categories: ["composer"], composerIds: ["berlioz"] },
      async (url) => {
        const value = String(url);
        if (value.includes("w/api.php") || value.includes("api/rest_v1/page/summary")) {
          throw new Error("blocked");
        }
        return {
          ok: true,
          url: "https://baike.baidu.com/item/%E6%9F%8F%E8%BE%BD%E5%85%B9",
          text: async () =>
            '<html><head><meta property="og:title" content="柏辽兹"><meta name="description" content="Hector Berlioz French composer, 1803-1869."><meta property="og:image" content="https://baike.baidu.com/logo.png"></head></html>',
        } as Response;
      },
    );

    expect(run.proposals).toHaveLength(1);
    expect(run.proposals[0]?.imageCandidates).toHaveLength(0);
    expect(run.proposals[0]?.warnings?.some((warning) => warning.includes("图片"))).toBe(true);
  });

  it("still surfaces named-entity image candidates when the current local image path is broken", async () => {
    const namedLibrary = validateLibrary({
      composers: [
        {
          id: "karajan",
          slug: "karajan",
          name: "卡拉扬",
          fullName: "赫伯特·冯·卡拉扬",
          nameLatin: "Herbert von Karajan",
          displayName: "卡拉扬",
          displayFullName: "赫伯特·冯·卡拉扬",
          displayLatinName: "Herbert von Karajan",
          country: "Austria",
          avatarSrc: "/library-assets/legacy/people/karajan-missing.jpg",
          aliases: [],
          abbreviations: [],
          sortKey: "karajan",
          summary: "奥地利指挥家。",
        },
      ],
      people: [],
      workGroups: [],
      works: [],
      recordings: [],
    });

    const run = await runAutomationChecks(
      namedLibrary,
      { categories: ["composer"], composerIds: ["karajan"] },
      async (url) => {
        const value = String(url);
        if (value.includes("w/api.php")) {
          return new Response(JSON.stringify({ query: { search: [{ title: "Herbert von Karajan" }] } }), { status: 200 });
        }
        if (value.includes("api/rest_v1/page/summary")) {
          return new Response(
            JSON.stringify({
              title: "Herbert von Karajan",
              description: "Austrian conductor",
              extract: "Herbert von Karajan was an Austrian conductor born in 1908.",
              content_urls: { desktop: { page: "https://en.wikipedia.org/wiki/Herbert_von_Karajan" } },
              originalimage: { source: "https://upload.wikimedia.org/example/karajan.jpg" },
            }),
            { status: 200 },
          );
        }
        throw new Error(`unexpected fetch: ${value}`);
      },
    );

    expect(run.proposals[0]?.imageCandidates?.length).toBeGreaterThan(0);
    expect(run.proposals[0]?.warnings?.some((warning) => String(warning).includes("当前图片"))).toBe(true);
  });

  it("keeps image candidates visible on named-entity proposals that already contain field updates", async () => {
    const namedLibrary = validateLibrary({
      composers: [
        {
          id: "karajan",
          slug: "karajan",
          name: "卡拉扬",
          fullName: "赫伯特·冯·卡拉扬",
          nameLatin: "Herbert von Karajan",
          displayName: "卡拉扬",
          displayFullName: "赫伯特·冯·卡拉扬",
          displayLatinName: "Herbert von Karajan",
          country: "",
          avatarSrc: "https://cdn.example.com/existing-karajan.jpg",
          aliases: [],
          abbreviations: [],
          sortKey: "karajan",
          summary: "",
        },
      ],
      people: [],
      workGroups: [],
      works: [],
      recordings: [],
    });

    const run = await runAutomationChecks(
      namedLibrary,
      { categories: ["composer"], composerIds: ["karajan"] },
      async (url) => {
        const value = String(url);
        if (value.includes("w/api.php")) {
          return new Response(JSON.stringify({ query: { search: [{ title: "Herbert von Karajan" }] } }), { status: 200 });
        }
        if (value.includes("api/rest_v1/page/summary")) {
          return new Response(
            JSON.stringify({
              title: "Herbert von Karajan",
              description: "Austrian conductor",
              extract: "Herbert von Karajan was an Austrian conductor born in 1908.",
              content_urls: { desktop: { page: "https://en.wikipedia.org/wiki/Herbert_von_Karajan" } },
              originalimage: { source: "https://upload.wikimedia.org/example/karajan.jpg" },
            }),
            { status: 200 },
          );
        }
        throw new Error(`unexpected fetch: ${value}`);
      },
    );

    expect(run.proposals[0]?.fields?.some((field) => field.path === "country")).toBe(true);
    expect(run.proposals[0]?.imageCandidates?.length).toBeGreaterThan(0);
  });

  it("still generates named-entity proposals when Wikipedia and Baidu are unavailable but LLM returns structured knowledge", async () => {
    const incompleteLibrary = validateLibrary({
      composers: [
        {
          id: "bruckner",
          slug: "bruckner",
          name: "布鲁克纳",
          fullName: "",
          nameLatin: "Anton Bruckner",
          displayName: "布鲁克纳",
          displayFullName: "",
          displayLatinName: "Anton Bruckner",
          country: "",
          avatarSrc: "",
          aliases: [],
          abbreviations: [],
          sortKey: "bruckner",
          summary: "",
        },
      ],
      people: [],
      workGroups: [],
      works: [],
      recordings: [],
    });

    const llmConfig = {
      enabled: true,
      baseUrl: "https://api.example.com/v1",
      apiKey: "secret-key",
      model: "deepseek-reasoner",
      timeoutMs: 30000,
    };

    const fetchImpl: typeof fetch = async (url, init) => {
      const value = String(url);
      if (value.includes("api.example.com/v1/chat/completions")) {
        const body = JSON.parse(String(init?.body || "{}"));
        const primaryAttempt = body.response_format?.type === "json_object";
        return new Response(
          JSON.stringify({
            choices: [
              {
                message: {
                  content: primaryAttempt
                    ? JSON.stringify({ normalizedTitle: "Anton Bruckner" })
                    : JSON.stringify({
                        displayName: "布鲁克纳",
                        displayFullName: "安东·布鲁克纳",
                        displayLatinName: "Anton Bruckner",
                        aliases: ["布鲁克纳"],
                        abbreviations: [],
                        country: "Austria",
                        birthYear: 1824,
                        deathYear: 1896,
                        summary: "奥地利作曲家，布鲁克纳交响曲代表人物。",
                        confidence: 0.88,
                        rationale: "LLM 依据常见中文译名、英文全名与音乐史常识给出补全。",
                      }),
                },
              },
            ],
          }),
          { status: 200 },
        );
      }
      throw new Error(`blocked: ${value}`);
    };

    const run = await runAutomationChecks(
      incompleteLibrary,
      { categories: ["composer"], composerIds: ["bruckner"] },
      fetchImpl as typeof fetch,
      llmConfig,
    );

    expect(run.proposals).toHaveLength(1);
    expect(run.notes.some((note) => note.includes("LLM 已启用"))).toBe(true);
    expect(run.proposals[0]?.fields).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ path: "name", after: "安东·布鲁克纳" }),
        expect.objectContaining({ path: "aliases", after: expect.arrayContaining(["安东·布鲁克纳"]) }),
        expect.objectContaining({ path: "country", after: "Austria" }),
      ]),
    );
  });

  it("prefers grounded conductor biography fields over a richer but conflicting llm candidate", async () => {
    const peopleLibrary = validateLibrary({
      composers: [],
      people: [
        {
          id: "barenboim",
          slug: "daniel-barenboim",
          name: "Daniel Barenboim",
          fullName: "Daniel Barenboim",
          nameLatin: "Daniel Barenboim",
          displayName: "Daniel Barenboim",
          displayFullName: "Daniel Barenboim",
          displayLatinName: "Daniel Barenboim",
          country: "",
          avatarSrc: "",
          roles: ["conductor"],
          aliases: [],
          abbreviations: [],
          sortKey: "0010",
          summary: "",
        },
      ],
      workGroups: [],
      works: [],
      recordings: [],
    });

    const llmConfig = {
      enabled: true,
      baseUrl: "https://api.example.com/v1",
      apiKey: "secret-key",
      model: "deepseek-chat",
      timeoutMs: 30000,
    };

    const run = await runAutomationChecks(
      peopleLibrary,
      { categories: ["conductor"], conductorIds: ["barenboim"] },
      async (url, init) => {
        const value = String(url);
        if (value.includes("w/api.php") && value.includes("wikipedia.org")) {
          return new Response(
            JSON.stringify({
              query: {
                search: [{ title: "Daniel Barenboim" }],
              },
            }),
            { status: 200 },
          );
        }
        if (value.includes("api/rest_v1/page/summary")) {
          return new Response(
            JSON.stringify({
              title: "Daniel Barenboim",
              description: "Argentine-born conductor and pianist",
              extract: "Daniel Barenboim was born in 1942 and is an Argentine-born conductor and pianist.",
              content_urls: { desktop: { page: "https://en.wikipedia.org/wiki/Daniel_Barenboim" } },
              originalimage: { source: "https://upload.wikimedia.org/barenboim.jpg" },
            }),
            { status: 200 },
          );
        }
        if (value.includes("/chat/completions")) {
          return new Response(
            JSON.stringify({
              choices: [
                {
                  message: {
                    content: JSON.stringify({
                      displayName: "丹尼尔·巴伦博伊姆",
                      displayFullName: "丹尼尔·巴伦博伊姆",
                      displayLatinName: "Daniel Barenboim",
                      aliases: ["巴伦博伊姆"],
                      country: "United Kingdom",
                      birthYear: 1992,
                      deathYear: 2023,
                      summary: "著名指挥家与钢琴家。",
                      confidence: 0.94,
                      rationale: "LLM assembled a richer candidate.",
                    }),
                  },
                },
              ],
            }),
            { status: 200 },
          );
        }
        throw new Error(`blocked: ${value} ${String(init?.method || "GET")}`);
      },
      llmConfig,
    );

    const proposal = run.proposals[0];
    expect(proposal).toBeTruthy();
    expect(proposal?.fields).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ path: "country", after: "Argentina" }),
        expect.objectContaining({ path: "birthYear", after: 1942 }),
      ]),
    );
    expect(proposal?.fields.some((field) => field.path === "deathYear")).toBe(false);
  });

  it("keeps life-year fields aligned with the selected living-person summary across conflicting grounded sources", async () => {
    const peopleLibrary = validateLibrary({
      composers: [],
      people: [
        {
          id: "barenboim",
          slug: "daniel-barenboim",
          name: "Daniel Barenboim",
          fullName: "Daniel Barenboim",
          nameLatin: "Daniel Barenboim",
          displayName: "Daniel Barenboim",
          displayFullName: "Daniel Barenboim",
          displayLatinName: "Daniel Barenboim",
          country: "",
          avatarSrc: "",
          roles: ["conductor"],
          aliases: [],
          abbreviations: [],
          sortKey: "0011",
          summary: "",
        },
      ],
      workGroups: [],
      works: [],
      recordings: [],
    });

    const run = await runAutomationChecks(
      peopleLibrary,
      { categories: ["conductor"], conductorIds: ["barenboim"] },
      async (url) => {
        const value = String(url);
        if (value.includes("w/api.php") && value.includes("wikipedia.org")) {
          return new Response(
            JSON.stringify({
              query: {
                search: [{ title: "Daniel Barenboim" }],
              },
            }),
            { status: 200 },
          );
        }
        if (value.includes("api/rest_v1/page/summary")) {
          return new Response(
            JSON.stringify({
              title: "Daniel Barenboim",
              description: "Argentine-born conductor and pianist",
              extract: "Daniel Barenboim was born in 1942 and is an Argentine-born conductor and pianist.",
              content_urls: { desktop: { page: "https://en.wikipedia.org/wiki/Daniel_Barenboim" } },
              originalimage: { source: "https://upload.wikimedia.org/barenboim.jpg" },
            }),
            { status: 200 },
          );
        }
        if (value.includes("baike.baidu.com")) {
          return new Response(
            '<html><head><meta property="og:title" content="丹尼尔·巴伦博伊姆 - 百度百科" /><meta name="description" content="丹尼尔·巴伦博伊姆（1942—2023）是阿根廷指挥家、钢琴家。" /></head><body></body></html>',
            { status: 200 },
          );
        }
        if (value.includes("baidu.com/s?")) {
          return new Response("<html><body>no result</body></html>", { status: 200 });
        }
        if (value.includes("commons.wikimedia.org")) {
          return new Response(
            JSON.stringify({
              query: {
                pages: {
                  1: {
                    title: "File:Daniel_Barenboim.jpg",
                    imageinfo: [
                      {
                        url: "https://upload.wikimedia.org/barenboim.jpg",
                        thumburl: "https://upload.wikimedia.org/barenboim.jpg",
                        extmetadata: { Artist: { value: "Wikimedia Commons" } },
                      },
                    ],
                  },
                },
              },
            }),
            { status: 200 },
          );
        }
        throw new Error(`blocked: ${value}`);
      },
    );

    const proposal = run.proposals[0];
    expect(proposal).toBeTruthy();
    expect(proposal?.fields).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ path: "country", after: "Argentina" }),
        expect.objectContaining({ path: "birthYear", after: 1942 }),
      ]),
    );
    expect(proposal?.fields.some((field) => field.path === "deathYear")).toBe(false);
  });

  it("treats orchestras as institutions and does not generate life-year fields", async () => {
    const peopleLibrary = validateLibrary({
      composers: [],
      people: [
        {
          id: "berlin-phil",
          slug: "berlin-phil",
          name: "Berlin Philharmonic",
          fullName: "Berlin Philharmonic",
          nameLatin: "Berlin Philharmonic",
          displayName: "Berlin Philharmonic",
          displayFullName: "Berlin Philharmonic",
          displayLatinName: "Berlin Philharmonic",
          country: "",
          avatarSrc: "",
          roles: ["orchestra"],
          aliases: [],
          abbreviations: [],
          sortKey: "0100",
          summary: "",
        },
      ],
      workGroups: [],
      works: [],
      recordings: [],
    });

    const llmConfig = {
      enabled: true,
      baseUrl: "https://api.example.com/v1",
      apiKey: "secret-key",
      model: "deepseek-chat",
      timeoutMs: 30000,
    };

    const run = await runAutomationChecks(
      peopleLibrary,
      { categories: ["orchestra"], orchestraIds: ["berlin-phil"] },
      async (url) => {
        const value = String(url);
        if (value.includes("w/api.php") && value.includes("wikipedia.org")) {
          return new Response(
            JSON.stringify({
              query: {
                search: [{ title: "Berlin Philharmonic" }],
              },
            }),
            { status: 200 },
          );
        }
        if (value.includes("api/rest_v1/page/summary")) {
          return new Response(
            JSON.stringify({
              title: "Berlin Philharmonic",
              description: "German orchestra",
              extract: "The Berlin Philharmonic is a German orchestra based in Berlin.",
              content_urls: { desktop: { page: "https://en.wikipedia.org/wiki/Berlin_Philharmonic" } },
            }),
            { status: 200 },
          );
        }
        if (value.includes("/chat/completions")) {
          return new Response(
            JSON.stringify({
              choices: [
                {
                  message: {
                    content: JSON.stringify({
                      displayName: "柏林爱乐",
                      displayFullName: "柏林爱乐乐团",
                      displayLatinName: "Berlin Philharmonic",
                      aliases: ["柏林爱乐"],
                      abbreviations: ["BPO"],
                      country: "Germany",
                      birthYear: 1882,
                      deathYear: 2024,
                      summary: "德国柏林的著名管弦乐团。",
                      confidence: 0.9,
                      rationale: "Institution profile",
                    }),
                  },
                },
              ],
            }),
            { status: 200 },
          );
        }
        throw new Error(`blocked: ${value}`);
      },
      llmConfig,
    );

    const proposal = run.proposals[0];
    expect(proposal).toBeTruthy();
    expect(proposal?.fields.some((field) => field.path === "birthYear")).toBe(false);
    expect(proposal?.fields.some((field) => field.path === "deathYear")).toBe(false);
    expect(proposal?.fields).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ path: "country", after: "Germany" }),
        expect.objectContaining({ path: "aliases", after: expect.arrayContaining(["BPO"]) }),
      ]),
    );
  });

  it("derives a usable orchestra abbreviation from institution-style latin names when sources omit one", async () => {
    const peopleLibrary = validateLibrary({
      composers: [],
      people: [
        {
          id: "cologne-symphony",
          slug: "cologne-symphony",
          name: "Cologne Symphony Orchestra",
          fullName: "Cologne Symphony Orchestra",
          nameLatin: "Cologne Symphony Orchestra",
          displayName: "Cologne Symphony Orchestra",
          displayFullName: "Cologne Symphony Orchestra",
          displayLatinName: "Cologne Symphony Orchestra",
          country: "",
          avatarSrc: "",
          roles: ["orchestra"],
          aliases: [],
          abbreviations: [],
          sortKey: "0101",
          summary: "",
        },
      ],
      workGroups: [],
      works: [],
      recordings: [],
    });

    const run = await runAutomationChecks(
      peopleLibrary,
      { categories: ["orchestra"], orchestraIds: ["cologne-symphony"] },
      async (url) => {
        const value = String(url);
        if (value.includes("w/api.php") && value.includes("wikipedia.org")) {
          return new Response(
            JSON.stringify({
              query: {
                search: [{ title: "Cologne Symphony Orchestra" }],
              },
            }),
            { status: 200 },
          );
        }
        if (value.includes("api/rest_v1/page/summary")) {
          return new Response(
            JSON.stringify({
              title: "Cologne Symphony Orchestra",
              description: "German orchestra",
              extract: "The Cologne Symphony Orchestra is a German orchestra based in Cologne.",
              content_urls: { desktop: { page: "https://en.wikipedia.org/wiki/Cologne_Symphony_Orchestra" } },
            }),
            { status: 200 },
          );
        }
        if (value.includes("baike.baidu.com")) {
          return new Response(
            '<html><head><meta property="og:title" content="科隆交响乐团 - 百度百科" /><meta name="description" content="科隆交响乐团是德国科隆的管弦乐团。" /></head><body></body></html>',
            { status: 200 },
          );
        }
        if (value.includes("baidu.com/s?")) {
          return new Response("<html><body>no result</body></html>", { status: 200 });
        }
        if (value.includes("commons.wikimedia.org")) {
          return new Response(JSON.stringify({ query: { pages: {} } }), { status: 200 });
        }
        throw new Error(`blocked: ${value}`);
      },
    );

    const proposal = run.proposals[0];
    expect(proposal).toBeTruthy();
    expect(proposal?.fields).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ path: "aliases", after: expect.arrayContaining(["CSO"]) }),
      ]),
    );
  });

  it("checks work entities directly without falling through to recording checks", async () => {
    const workLibrary = validateLibrary({
      composers: [
        {
          id: "beethoven",
          slug: "beethoven",
          name: "贝多芬",
          fullName: "路德维希·凡·贝多芬",
          nameLatin: "Ludwig van Beethoven",
          country: "Germany",
          avatarSrc: "",
          aliases: [],
          sortKey: "beethoven",
          summary: "德国作曲家。",
        },
      ],
      people: [],
      workGroups: [
        {
          id: "group-symphony",
          composerId: "beethoven",
          title: "交响曲",
          slug: "symphony",
          path: ["交响曲"],
          sortKey: "0010",
        },
      ],
      works: [
        {
          id: "beethoven-5",
          composerId: "beethoven",
          groupIds: ["group-symphony"],
          slug: "beethoven-5",
          title: "第五交响曲",
          titleLatin: "",
          aliases: [],
          catalogue: "",
          summary: "",
          infoPanel: { text: "", articleId: "", collectionUrl: "" },
          sortKey: "0010",
          updatedAt: "2026-03-15T00:00:00.000Z",
        },
      ],
      recordings: [
        {
          id: "recording-beethoven-5-1963",
          workId: "beethoven-5",
          slug: "recording-beethoven-5-1963",
          title: "克莱伯 1963",
          sortKey: "0010",
          isPrimaryRecommendation: false,
          updatedAt: "2026-03-15T00:00:00.000Z",
          images: [],
          credits: [],
          links: [],
          notes: "",
          performanceDateText: "1963",
          venueText: "",
          albumTitle: "",
          label: "",
          releaseDate: "",
          infoPanel: { text: "", articleId: "", collectionUrl: "" },
        },
      ],
    });

    const fetchImpl: typeof fetch = async (input) => {
      const url = String(input);
      if (url.includes("w/api.php")) {
        return new Response(
          JSON.stringify({
            query: {
              search: [{ title: "Symphony No. 5 (Beethoven)" }],
            },
          }),
          { status: 200 },
        );
      }
      if (url.includes("/page/summary/")) {
        return new Response(
          JSON.stringify({
            title: "Symphony No. 5 (Beethoven)",
            extract: "Symphony No. 5 in C minor, Op. 67 is a symphony by Ludwig van Beethoven.",
            content_urls: {
              desktop: {
                page: "https://en.wikipedia.org/wiki/Symphony_No._5_(Beethoven)",
              },
            },
          }),
          { status: 200 },
        );
      }
      throw new Error(`unexpected fetch: ${url}`);
    };

    const run = await runAutomationChecks(workLibrary, { categories: ["work"], workIds: ["beethoven-5"] }, fetchImpl);

    expect(run.categories).toEqual(["work"]);
    expect(run.proposals).toHaveLength(1);
    expect(run.proposals[0]?.entityType).toBe("work");
    expect(run.proposals[0]?.entityId).toBe("beethoven-5");
    expect(run.proposals[0]?.fields).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ path: "titleLatin", after: "Symphony No. 5 (Beethoven)" }),
        expect.objectContaining({ path: "catalogue", after: "Op. 67" }),
        expect.objectContaining({ path: "summary", after: expect.stringContaining("Beethoven") }),
      ]),
    );
  });

  it("keeps incomplete works reviewable when Wikipedia search is ambiguous and fallback sources still provide evidence", async () => {
    const workLibrary = validateLibrary({
      composers: [
        {
          id: "tchaikovsky",
          slug: "tchaikovsky",
          name: "柴可夫斯基",
          fullName: "彼得·伊里奇·柴可夫斯基",
          nameLatin: "Pyotr Ilyich Tchaikovsky",
          country: "Russia",
          avatarSrc: "",
          aliases: [],
          sortKey: "0010",
          summary: "",
        },
      ],
      people: [],
      workGroups: [
        {
          id: "group-symphony",
          composerId: "tchaikovsky",
          title: "交响曲",
          slug: "symphony",
          path: ["交响曲"],
          sortKey: "0010",
        },
      ],
      works: [
        {
          id: "tchaikovsky-5",
          composerId: "tchaikovsky",
          groupIds: ["group-symphony"],
          slug: "tchaikovsky-5",
          title: "第五交响曲",
          titleLatin: "",
          aliases: [],
          catalogue: "",
          summary: "",
          infoPanel: { text: "", articleId: "", collectionLinks: [] },
          sortKey: "0010",
          updatedAt: "2026-03-15T00:00:00.000Z",
        },
      ],
      recordings: [],
    });

    const fetchImpl: typeof fetch = async (input) => {
      const url = String(input);
      if (url.includes("w/api.php")) {
        return new Response(
          JSON.stringify({
            query: {
              search: [{ title: "Symphony No. 5" }],
            },
          }),
          { status: 200 },
        );
      }
      if (url.includes("/page/summary/")) {
        return new Response(
          JSON.stringify({
            title: "Symphony No. 5",
            extract: "This is a disambiguation page.",
            content_urls: {
              desktop: {
                page: "https://en.wikipedia.org/wiki/Symphony_No._5",
              },
            },
          }),
          { status: 200 },
        );
      }
      if (url.includes("baidu.com/s?")) {
        return new Response(
          '<html><body><h3><a href="https://baike.baidu.com/item/%E7%AC%AC%E4%BA%94%E4%BA%A4%E5%93%8D%E6%9B%B2">柴可夫斯基第五交响曲</a></h3><div class="c-abstract">《第五交响曲》是柴可夫斯基创作的 e 小调第五交响曲，作品64。</div></body></html>',
          { status: 200 },
        );
      }
      throw new Error(`unexpected fetch: ${url}`);
    };

    const run = await runAutomationChecks(workLibrary, { categories: ["work"], workIds: ["tchaikovsky-5"] }, fetchImpl);
    const proposal = run.proposals[0];

    expect(proposal?.entityType).toBe("work");
    expect(proposal?.summary).toContain("柴可夫斯基");
    expect(proposal?.summary).toContain("第五交响曲");
    expect(proposal?.fields).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ path: "catalogue", after: "Op. 64" }),
        expect.objectContaining({ path: "summary", after: expect.stringContaining("柴可夫斯基") }),
      ]),
    );
    expect(proposal?.evidence?.some((item) => item.sourceLabel.includes("Baidu"))).toBe(true);
  });

  it("re-injects composer context when concise work summaries would otherwise omit the composer", async () => {
    const workLibrary = validateLibrary({
      composers: [
        {
          id: "beethoven",
          slug: "beethoven",
          name: "贝多芬",
          fullName: "路德维希·范·贝多芬",
          nameLatin: "Ludwig van Beethoven",
          country: "Germany",
          avatarSrc: "",
          aliases: [],
          sortKey: "0010",
          summary: "",
        },
      ],
      people: [],
      workGroups: [
        {
          id: "group-symphony",
          composerId: "beethoven",
          title: "交响曲",
          slug: "symphony",
          path: ["交响曲"],
          sortKey: "0010",
        },
      ],
      works: [
        {
          id: "beethoven-6",
          composerId: "beethoven",
          groupIds: ["group-symphony"],
          slug: "beethoven-6",
          title: "第六交响曲“田园”",
          titleLatin: "",
          aliases: [],
          catalogue: "",
          summary: "",
          infoPanel: { text: "", articleId: "", collectionLinks: [] },
          sortKey: "0010",
          updatedAt: "2026-03-17T00:00:00.000Z",
        },
      ],
      recordings: [],
    });

    const llmConfig = {
      enabled: true,
      baseUrl: "https://api.example.com/v1",
      apiKey: "secret-key",
      model: "deepseek-chat",
      timeoutMs: 30000,
    };

    let chatCalls = 0;
    const run = await runAutomationChecks(
      workLibrary,
      { categories: ["work"], workIds: ["beethoven-6"] },
      async (url, init) => {
        const value = String(url);
        if (value.includes("w/api.php") && value.includes("wikipedia.org")) {
          return new Response(
            JSON.stringify({
              query: {
                search: [{ title: "Symphony No. 6 (Beethoven)" }],
              },
            }),
            { status: 200 },
          );
        }
        if (value.includes("api/rest_v1/page/summary")) {
          return new Response(
            JSON.stringify({
              title: "Symphony No. 6 (Beethoven)",
              description: "Symphony by Beethoven",
              extract: "Symphony No. 6 in F major, Op. 68, also known as the Pastoral Symphony, is a symphony by Ludwig van Beethoven.",
              content_urls: { desktop: { page: "https://en.wikipedia.org/wiki/Symphony_No._6_(Beethoven)" } },
            }),
            { status: 200 },
          );
        }
        if (value.includes("baidu.com/s?")) {
          return new Response("<html><body>no result</body></html>", { status: 200 });
        }
        if (value.includes("/chat/completions")) {
          chatCalls += 1;
          if (chatCalls === 1) {
            return new Response(
              JSON.stringify({
                choices: [
                  {
                    message: {
                      content: "以田园风景与自然意象著称的交响曲。",
                    },
                  },
                ],
              }),
              { status: 200 },
            );
          }
          return new Response(
            JSON.stringify({
              choices: [
                {
                  message: {
                    content: JSON.stringify({
                      status: "ok",
                      issues: [],
                      confidence: 0.92,
                      rationale: "grounded",
                    }),
                  },
                },
              ],
            }),
            { status: 200 },
          );
        }
        throw new Error(`blocked: ${value} ${String(init?.method || "GET")}`);
      },
      llmConfig,
    );

    const proposal = run.proposals[0];
    const summaryField = proposal?.fields.find((field) => field.path === "summary");
    expect(summaryField?.after).toContain("贝多芬");
    expect(String(summaryField?.after || "")).toContain("田园");
  });

  it("prefers structured catalogue patterns for works and does not turn years into Deutsch numbers", async () => {
    const workLibrary = validateLibrary({
      composers: [
        {
          id: "bruckner",
          slug: "bruckner",
          name: "布鲁克纳",
          fullName: "安东·布鲁克纳",
          nameLatin: "Anton Bruckner",
          country: "Austria",
          avatarSrc: "",
          aliases: [],
          sortKey: "0010",
          summary: "",
        },
      ],
      people: [],
      workGroups: [
        {
          id: "group-symphony",
          composerId: "bruckner",
          title: "交响曲",
          slug: "symphony",
          path: ["交响曲"],
          sortKey: "0010",
        },
      ],
      works: [
        {
          id: "bruckner-7",
          composerId: "bruckner",
          groupIds: ["group-symphony"],
          slug: "bruckner-7",
          title: "第七交响曲",
          titleLatin: "",
          aliases: [],
          catalogue: "",
          summary: "",
          infoPanel: { text: "", articleId: "", collectionLinks: [] },
          sortKey: "0010",
          updatedAt: "2026-03-18T00:00:00.000Z",
        },
      ],
      recordings: [],
    });

    const fetchImpl: typeof fetch = async (url) => {
      const value = String(url);
      if (value.includes("w/api.php")) {
        return new Response(
          JSON.stringify({
            query: {
              search: [{ title: "Symphony No. 7 (Bruckner)" }],
            },
          }),
          { status: 200 },
        );
      }
      if (value.includes("/page/summary/")) {
        return new Response(
          JSON.stringify({
            title: "Symphony No. 7 (Bruckner)",
            extract:
              "Symphony No. 7 in E major, WAB 107 is a symphony by Anton Bruckner. It was composed between 1881 and 1883 and premiered in 1884.",
            content_urls: {
              desktop: {
                page: "https://en.wikipedia.org/wiki/Symphony_No._7_(Bruckner)",
              },
            },
          }),
          { status: 200 },
        );
      }
      if (value.includes("baidu.com/s?")) {
        return new Response("<html><body>no result</body></html>", { status: 200 });
      }
      throw new Error(`unexpected fetch: ${value}`);
    };

    const run = await runAutomationChecks(workLibrary, { categories: ["work"], workIds: ["bruckner-7"] }, fetchImpl);
    const proposal = run.proposals[0];

    expect(proposal?.fields).toEqual(
      expect.arrayContaining([expect.objectContaining({ path: "catalogue", after: "WAB 107" })]),
    );
    expect(proposal?.fields).not.toEqual(
      expect.arrayContaining([expect.objectContaining({ path: "catalogue", after: "D 1883" })]),
    );
  });

  it("does not replace an existing full Chinese name with a short alias candidate", async () => {
    const namedLibrary = validateLibrary({
      composers: [],
      people: [
        {
          id: "tchaikovsky",
          slug: "tchaikovsky",
          name: "彼得·伊里奇·柴可夫斯基",
          fullName: "彼得·伊里奇·柴可夫斯基",
          nameLatin: "Pyotr Ilyich Tchaikovsky",
          displayName: "柴可夫斯基",
          displayFullName: "彼得·伊里奇·柴可夫斯基",
          displayLatinName: "Pyotr Ilyich Tchaikovsky",
          country: "",
          avatarSrc: "",
          roles: ["conductor"],
          aliases: [],
          abbreviations: [],
          sortKey: "0010",
          summary: "",
        },
      ],
      workGroups: [],
      works: [],
      recordings: [],
    });

    const llmConfig = {
      enabled: true,
      baseUrl: "https://api.example.com/v1",
      apiKey: "secret-key",
      model: "deepseek-chat",
      timeoutMs: 30000,
    };

    const run = await runAutomationChecks(
      namedLibrary,
      { categories: ["conductor"], conductorIds: ["tchaikovsky"] },
      async (url, init) => {
        const value = String(url);
        if (value.includes("/chat/completions")) {
          return new Response(
            JSON.stringify({
              choices: [
                {
                  message: {
                    content: JSON.stringify({
                      displayName: "彼得",
                      displayFullName: "",
                      displayLatinName: "Pyotr Ilyich Tchaikovsky",
                      aliases: ["彼得"],
                      country: "Russia",
                      summary: "彼得是俄罗斯作曲家。",
                      confidence: 0.72,
                      rationale: "short alias candidate",
                    }),
                  },
                },
              ],
            }),
            { status: 200 },
          );
        }
        throw new Error(`blocked: ${value} ${String(init?.method || "GET")}`);
      },
      llmConfig,
    );

    const proposal = run.proposals[0];
    expect(proposal).toBeTruthy();
    expect(proposal?.fields.some((field) => field.path === "name")).toBe(false);
  });

  it("falls back to structured LLM work knowledge when web sources produce no usable fields", async () => {
    const workLibrary = validateLibrary({
      composers: [
        {
          id: "tchaikovsky",
          slug: "tchaikovsky",
          name: "柴可夫斯基",
          fullName: "彼得·伊里奇·柴可夫斯基",
          nameLatin: "Pyotr Ilyich Tchaikovsky",
          country: "Russia",
          avatarSrc: "",
          aliases: [],
          sortKey: "0010",
          summary: "",
        },
      ],
      people: [],
      workGroups: [
        {
          id: "group-symphony",
          composerId: "tchaikovsky",
          title: "交响曲",
          slug: "symphony",
          path: ["交响曲"],
          sortKey: "0010",
        },
      ],
      works: [
        {
          id: "tchaikovsky-5",
          composerId: "tchaikovsky",
          groupIds: ["group-symphony"],
          slug: "tchaikovsky-5",
          title: "第五交响曲",
          titleLatin: "",
          aliases: [],
          catalogue: "",
          summary: "",
          infoPanel: { text: "", articleId: "", collectionLinks: [] },
          sortKey: "0010",
          updatedAt: "2026-03-15T00:00:00.000Z",
        },
      ],
      recordings: [],
    });

    const llmConfig = {
      enabled: true,
      baseUrl: "https://api.example.com/v1",
      apiKey: "secret-key",
      model: "deepseek-chat",
      timeoutMs: 30000,
    };

    const fetchImpl: typeof fetch = async (url, _init) => {
      const value = String(url);
      if (value.includes("w/api.php")) {
        return new Response(JSON.stringify({ query: { search: [] } }), { status: 200 });
      }
      if (value.includes("baidu.com/s?")) {
        return new Response(
          '<html><body><h3><a href="https://example.com/beethoven-5">贝多芬第五交响曲</a></h3><div class="c-abstract">《第五交响曲》是贝多芬创作的交响曲，作品号67。</div></body></html>',
          { status: 200 },
        );
      }
      if (value.includes("/chat/completions")) {
        return new Response(
          JSON.stringify({
            choices: [
              {
                message: {
                  content: JSON.stringify({
                    titleLatin: "Symphony No. 5 in E minor, Op. 64",
                    catalogue: "Op. 64",
                    summary: "柴可夫斯基后期代表性交响曲之一，以贯穿全曲的命运主题著称。",
                    aliases: ["e小调第五交响曲"],
                    confidence: 0.74,
                    rationale: "作品标题与作曲家信息明确，可稳定补出常用英文名与作品号。",
                  }),
                },
              },
            ],
          }),
          { status: 200 },
        ) as Response;
      }
      throw new Error(`unexpected fetch: ${value}`);
    };

    const run = await runAutomationChecks(
      workLibrary,
      { categories: ["work"], workIds: ["tchaikovsky-5"] },
      fetchImpl,
      llmConfig,
    );
    const proposal = run.proposals[0];

    expect(proposal?.entityType).toBe("work");
    expect(proposal?.risk).toBe("medium");
    expect(proposal?.fields).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ path: "titleLatin", after: "Symphony No. 5 in E minor, Op. 64" }),
        expect.objectContaining({ path: "catalogue", after: "Op. 64" }),
      ]),
    );
    expect(proposal?.evidence?.some((item) => item.sourceLabel === "LLM")).toBe(true);
  });

  it("ignores blocked Baidu captcha pages for works and keeps the summary grounded in usable knowledge", async () => {
    const workLibrary = validateLibrary({
      composers: [
        {
          id: "tchaikovsky",
          slug: "tchaikovsky",
          name: "柴可夫斯基",
          fullName: "彼得·伊里奇·柴可夫斯基",
          nameLatin: "Pyotr Ilyich Tchaikovsky",
          country: "Russia",
          avatarSrc: "",
          aliases: [],
          sortKey: "0010",
          summary: "",
        },
      ],
      people: [],
      workGroups: [
        {
          id: "group-symphony",
          composerId: "tchaikovsky",
          title: "交响曲",
          slug: "symphony",
          path: ["交响曲"],
          sortKey: "0010",
        },
      ],
      works: [
        {
          id: "tchaikovsky-5",
          composerId: "tchaikovsky",
          groupIds: ["group-symphony"],
          slug: "tchaikovsky-5",
          title: "第五交响曲",
          titleLatin: "",
          aliases: [],
          catalogue: "",
          summary: "",
          infoPanel: { text: "", articleId: "", collectionLinks: [] },
          sortKey: "0010",
          updatedAt: "2026-03-15T00:00:00.000Z",
        },
      ],
      recordings: [],
    });

    const llmConfig = {
      enabled: true,
      baseUrl: "https://api.example.com/v1",
      apiKey: "secret-key",
      model: "deepseek-chat",
      timeoutMs: 30000,
    };

    let chatCalls = 0;
    const fetchImpl: typeof fetch = async (url, _init) => {
      const value = String(url);
      if (value.includes("w/api.php")) {
        return new Response(JSON.stringify({ query: { search: [{ title: "Symphony No. 5" }] } }), { status: 200 });
      }
      if (value.includes("/page/summary/")) {
        return new Response(
          JSON.stringify({
            title: "Symphony No. 5",
            extract: "This is a disambiguation page.",
            content_urls: {
              desktop: {
                page: "https://en.wikipedia.org/wiki/Symphony_No._5",
              },
            },
          }),
          { status: 200 },
        );
      }
      if (value.includes("baidu.com/s?")) {
        return {
          ok: true,
          status: 200,
          url: "https://wappass.baidu.com/static/captcha/tuxing_v2.html",
          text: async () => "<html><head><title>百度安全验证</title></head><body>请完成验证后继续访问</body></html>",
        } as Response;
      }
      if (value.includes("/chat/completions")) {
        chatCalls += 1;
        if (chatCalls === 1) {
          return new Response(
            JSON.stringify({
              choices: [
                {
                  message: {
                    content: JSON.stringify({
                      titleLatin: "Symphony No. 5 in E minor, Op. 64",
                      catalogue: "Op. 64",
                      summary: "柴可夫斯基后期代表性交响曲之一，以贯穿全曲的命运主题著称。",
                      aliases: ["e小调第五交响曲"],
                      confidence: 0.76,
                      rationale: "作曲家与作品序号明确，可稳定补出英文标题、作品号与简介。",
                    }),
                  },
                },
              ],
            }),
            { status: 200 },
          ) as Response;
        }
        return new Response(
          JSON.stringify({
            choices: [
              {
                message: {
                  content: "第五交响曲是贝多芬创作的交响曲，作品号67，以命运动机闻名。",
                },
              },
            ],
          }),
          { status: 200 },
        ) as Response;
      }
      throw new Error(`unexpected fetch: ${value}`);
    };

    const run = await runAutomationChecks(
      workLibrary,
      { categories: ["work"], workIds: ["tchaikovsky-5"] },
      fetchImpl,
      llmConfig,
    );
    const proposal = run.proposals[0];
    const summaryField = proposal?.fields.find((field) => field.path === "summary");
    const summaryEvidence = proposal?.evidence?.find((item) => item.field === "summary");

    expect(proposal?.entityType).toBe("work");
    expect(summaryField?.after).toContain("柴可夫斯基");
    expect(String(summaryField?.after || "")).not.toContain("贝多芬");
    expect(summaryEvidence?.sourceLabel).toBe("LLM");
  });

  it("rejects Baidu boilerplate titles when proposing Chinese full names for orchestras", async () => {
    const orchestraLibrary = validateLibrary({
      composers: [],
      people: [
        {
          id: "munich-philharmonic",
          slug: "munich-philharmonic",
          name: "Munich Philharmonic Orchestra",
          fullName: "",
          nameLatin: "",
          displayName: "Munich Philharmonic Orchestra",
          displayFullName: "",
          displayLatinName: "",
          country: "",
          avatarSrc: "",
          roles: ["orchestra"],
          aliases: ["MPO"],
          abbreviations: [],
          sortKey: "0010",
          summary: "",
        },
      ],
      workGroups: [],
      works: [],
      recordings: [],
    });

    const run = await runAutomationChecks(
      orchestraLibrary,
      { categories: ["orchestra"], orchestraIds: ["munich-philharmonic"] },
      async (url) => {
        const value = String(url);
        if (value.includes("wikipedia.org")) {
          throw new Error("blocked");
        }
        if (value.includes("baike.baidu.com")) {
          return new Response(
            '<html><head><meta property="og:title" content="百度百科是一部内容开放、自由的网络百科全书"><meta name="description" content="慕尼黑爱乐乐团是德国主要交响乐团之一，以深厚的艺术传统和高水准演奏闻名。"></head></html>',
            { status: 200 },
          );
        }
        if (value.includes("www.baidu.com/s?")) {
          return new Response(
            '<html><body><h3><a href="https://example.com">慕尼黑爱乐乐团</a></h3><div class="c-abstract">慕尼黑爱乐乐团是德国主要交响乐团之一。</div></body></html>',
            { status: 200 },
          );
        }
        return new Response("<html></html>", { status: 200 });
      },
    );

    const proposal = run.proposals[0];
    const nameField = proposal?.fields.find((field) => field.path === "name");

    expect(proposal?.entityType).toBe("person");
    expect(nameField?.after).toBe("慕尼黑爱乐乐团");
    expect(String(nameField?.after || "")).not.toContain("百度百科是一部内容开放");
  });

  it("does not replace an existing Chinese full name with a shorter alias candidate", async () => {
    const namedLibrary = validateLibrary({
      composers: [
        {
          id: "tchaikovsky",
          slug: "tchaikovsky",
          name: "彼得·伊里奇·柴可夫斯基",
          fullName: "彼得·伊里奇·柴可夫斯基",
          nameLatin: "Peter Ilyich Tchaikovsky",
          displayName: "柴可夫斯基",
          displayFullName: "彼得·伊里奇·柴可夫斯基",
          displayLatinName: "Peter Ilyich Tchaikovsky",
          country: "",
          avatarSrc: "",
          aliases: ["柴可夫斯基", "Tchaikovsky"],
          abbreviations: [],
          sortKey: "0010",
          summary: "",
        },
      ],
      people: [],
      workGroups: [],
      works: [],
      recordings: [],
    });

    const run = await runAutomationChecks(
      namedLibrary,
      { categories: ["composer"], composerIds: ["tchaikovsky"] },
      async (url) => {
        const value = String(url);
        if (value.includes("w/api.php")) {
          return new Response(
            JSON.stringify({
              query: {
                search: [{ title: "Pyotr Ilyich Tchaikovsky" }],
              },
            }),
            { status: 200 },
          );
        }
        if (value.includes("api/rest_v1/page/summary")) {
          return new Response(
            JSON.stringify({
              title: "Pyotr Ilyich Tchaikovsky",
              description: "Russian composer",
              extract: "Pyotr Ilyich Tchaikovsky was a Russian composer born in 1840.",
              content_urls: { desktop: { page: "https://en.wikipedia.org/wiki/Pyotr_Ilyich_Tchaikovsky" } },
            }),
            { status: 200 },
          );
        }
        if (value.includes("baike.baidu.com")) {
          return new Response(
            '<html><head><meta property="og:title" content="彼得" /><meta name="description" content="彼得·伊里奇·柴可夫斯基（Peter Ilyich Tchaikovsky）是俄罗斯作曲家。"></head></html>',
            { status: 200 },
          );
        }
        if (value.includes("www.baidu.com/s?")) {
          return new Response(
            '<html><body><h3><a href="https://baike.baidu.com/item/tchaikovsky">彼得</a></h3><div class="c-abstract">彼得·伊里奇·柴可夫斯基（Peter Ilyich Tchaikovsky）是俄罗斯作曲家。</div></body></html>',
            { status: 200 },
          );
        }
        throw new Error(`unexpected fetch: ${value}`);
      },
    );

    const proposal = run.proposals[0];
    const nameField = proposal?.fields.find((field) => field.path === "name");

    expect(nameField).toBeUndefined();
    expect(proposal?.fields).toEqual(
      expect.arrayContaining([expect.objectContaining({ path: "country", after: "Russia" })]),
    );
  });

  it("does not replace an existing person name with a generic role descriptor from Baidu sources", async () => {
    const namedLibrary = validateLibrary({
      composers: [],
      people: [
        {
          id: "welser-most",
          slug: "welser-most",
          name: "弗朗茨·威尔瑟-莫斯特",
          fullName: "Franz Welser-Möst",
          nameLatin: "Franz Welser-Möst",
          country: "",
          avatarSrc: "",
          roles: ["conductor"],
          aliases: ["Franz Welser-Möst"],
          sortKey: "0010",
          summary: "",
        },
      ],
      workGroups: [],
      works: [],
      recordings: [],
    });

    const run = await runAutomationChecks(
      namedLibrary,
      { categories: ["conductor"], conductorIds: ["welser-most"] },
      async (url) => {
        const value = String(url);
        if (value.includes("w/api.php")) {
          return new Response(
            JSON.stringify({
              query: {
                search: [{ title: "Franz Welser-Möst" }],
              },
            }),
            { status: 200 },
          );
        }
        if (value.includes("api/rest_v1/page/summary")) {
          return new Response(
            JSON.stringify({
              title: "Franz Welser-Möst",
              description: "Austrian conductor",
              extract: "Franz Welser-Möst is an Austrian conductor born in 1960.",
              content_urls: { desktop: { page: "https://en.wikipedia.org/wiki/Franz_Welser-M%C3%B6st" } },
            }),
            { status: 200 },
          );
        }
        if (value.includes("baike.baidu.com")) {
          return new Response(
            '<html><head><meta property="og:title" content="奥地利指挥家" /><meta name="description" content="弗朗茨·威尔瑟-莫斯特（Franz Welser-Möst），奥地利指挥家，1960年出生。"></head></html>',
            { status: 200 },
          );
        }
        if (value.includes("www.baidu.com/s?")) {
          return new Response(
            '<html><body><h3><a href="https://baike.baidu.com/item/welser-most">奥地利指挥家</a></h3><div class="c-abstract">弗朗茨·威尔瑟-莫斯特（Franz Welser-Möst），奥地利指挥家，1960年出生。</div></body></html>',
            { status: 200 },
          );
        }
        throw new Error(`unexpected fetch: ${value}`);
      },
    );

    const proposal = run.proposals[0];
    const nameField = proposal?.fields.find((field) => field.path === "name");

    expect(nameField).toBeUndefined();
    expect(proposal?.fields).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ path: "country", after: "Austria" }),
        expect.objectContaining({ path: "birthYear", after: 1960 }),
      ]),
    );
  });

  it("normalizes duplicated proposal ids within a run", () => {
    const run = summarizeAutomationRun({
      id: "run-duplicate-proposals",
      createdAt: "2026-03-21T00:00:00.000Z",
      categories: ["conductor"],
      proposals: [
        {
          id: "merge-abbado|boult",
          kind: "merge",
          entityType: "person",
          entityId: "abbado",
          summary: "疑似重复人物：克劳迪奥·阿巴多 / 欧内斯特·布尔",
          risk: "high",
          status: "pending",
          reviewState: "viewed",
          sources: ["en.wikipedia.org"],
          fields: [],
          warnings: ["Close normalized key: claudioabbado"],
          mergeCandidates: [{ targetId: "boult", targetLabel: "欧内斯特·布尔", reason: "同名归并" }],
          imageCandidates: [],
          evidence: [],
          linkCandidates: [],
          selectedImageCandidateId: "",
        },
        {
          id: "merge-abbado|boult",
          kind: "merge",
          entityType: "person",
          entityId: "abbado",
          summary: "疑似重复人物：克劳迪奥·阿巴多 / 欧内斯特·布尔",
          risk: "high",
          status: "pending",
          reviewState: "confirmed",
          sources: ["baike.baidu.com"],
          fields: [],
          warnings: ["Close normalized key: claudioabbado"],
          mergeCandidates: [{ targetId: "boult", targetLabel: "欧内斯特·布尔", reason: "同名归并" }],
          imageCandidates: [],
          evidence: [],
          linkCandidates: [],
          selectedImageCandidateId: "",
        },
      ],
      snapshots: [],
      notes: ["note-a", "note-a", "note-b"],
      summary: {
        total: 2,
        pending: 2,
        applied: 0,
        ignored: 0,
      },
    });

    expect(run.proposals).toHaveLength(1);
    expect(run.proposals[0]?.reviewState).toBe("confirmed");
    expect(run.proposals[0]?.sources).toEqual(["en.wikipedia.org", "baike.baidu.com"]);
    expect(run.notes).toEqual(["note-a", "note-b"]);
    expect(run.summary.total).toBe(1);
  });

  it("coalesces semantically duplicated update proposals even when their ids differ", () => {
    const run = summarizeAutomationRun({
      id: "run-semantic-duplicate-proposals",
      createdAt: "2026-03-22T00:00:00.000Z",
      categories: ["orchestra"],
      proposals: [
        {
          id: "munich-name-from-wikipedia",
          kind: "update",
          entityType: "person",
          entityId: "kleiber",
          summary: "自动检查：卡洛斯·克莱伯",
          risk: "low",
          status: "pending",
          reviewState: "viewed",
          sources: ["en.wikipedia.org"],
          fields: [{ path: "country", before: "", after: "Germany" }],
          warnings: ["Wikipedia"],
          imageCandidates: [],
          evidence: [
            {
              field: "country",
              sourceUrl: "https://en.wikipedia.org/wiki/Carlos_Kleiber",
              sourceLabel: "Wikipedia",
              confidence: 0.9,
            },
          ],
          linkCandidates: [],
          selectedImageCandidateId: "",
        },
        {
          id: "munich-name-from-baidu",
          kind: "update",
          entityType: "person",
          entityId: "kleiber",
          summary: "自动检查：卡洛斯·克莱伯",
          risk: "medium",
          status: "pending",
          reviewState: "confirmed",
          sources: ["baike.baidu.com"],
          fields: [{ path: "country", before: "", after: "Germany" }],
          warnings: ["Baidu"],
          imageCandidates: [],
          evidence: [
            {
              field: "country",
              sourceUrl: "https://baike.baidu.com/item/Carlos_Kleiber",
              sourceLabel: "Baidu Baike",
              confidence: 0.72,
            },
          ],
          linkCandidates: [],
          selectedImageCandidateId: "",
        },
      ],
      snapshots: [],
      notes: [],
      summary: {
        total: 2,
        pending: 2,
        applied: 0,
        ignored: 0,
      },
    });

    expect(run.proposals).toHaveLength(1);
    expect(run.proposals[0]?.reviewState).toBe("confirmed");
    expect(run.proposals[0]?.sources).toEqual(["en.wikipedia.org", "baike.baidu.com"]);
    expect(run.proposals[0]?.warnings).toEqual(["Wikipedia", "Baidu"]);
    expect(run.proposals[0]?.evidence).toHaveLength(2);
    expect(run.summary.total).toBe(1);
  });
});
