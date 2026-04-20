import { describe, expect, it } from "vitest";

import { validateLibrary, type LibraryData, type Recording } from "@/lib/schema";
import type { ReviewQueueEntry } from "../../packages/data-core/src/library-store.js";
import {
  auditLibraryData,
  buildManualBackfillQueue,
  buildManualBackfillReference,
  groupManualBackfillQueue,
} from "../../packages/data-core/src/library-audit.js";

function buildBaseLibrary(): LibraryData {
  return validateLibrary({
    composers: [
      {
        id: "composer-beethoven",
        slug: "beethoven",
        name: "贝多芬",
        fullName: "路德维希·凡·贝多芬",
        nameLatin: "Ludwig van Beethoven",
        country: "Germany",
        avatarSrc: "",
        aliases: [],
        sortKey: "0010",
        summary: "",
      },
    ],
    people: [
      {
        id: "person-karajan",
        slug: "karajan",
        name: "卡拉扬",
        fullName: "赫伯特·冯·卡拉扬",
        nameLatin: "Herbert von Karajan",
        country: "Austria",
        avatarSrc: "",
        roles: ["conductor"],
        aliases: ["Herbert von Karajan"],
        sortKey: "0010",
        summary: "",
      },
      {
        id: "person-bpo",
        slug: "berliner-philharmoniker",
        name: "柏林爱乐乐团",
        fullName: "柏林爱乐乐团",
        nameLatin: "Berliner Philharmoniker",
        country: "Germany",
        avatarSrc: "",
        roles: ["orchestra"],
        aliases: ["BPO"],
        sortKey: "0011",
        summary: "",
      },
      {
        id: "person-anne",
        slug: "annie-fischer",
        name: "安妮·费舍尔",
        fullName: "安妮·费舍尔",
        nameLatin: "Annie Fischer",
        country: "Hungary",
        avatarSrc: "",
        roles: ["soloist"],
        aliases: [],
        sortKey: "0012",
        summary: "",
      },
    ],
    workGroups: [
      {
        id: "group-beethoven-symphony",
        composerId: "composer-beethoven",
        title: "交响曲",
        slug: "symphony",
        path: ["交响曲"],
        sortKey: "0010",
      },
      {
        id: "group-beethoven-concerto",
        composerId: "composer-beethoven",
        title: "钢琴协奏曲",
        slug: "piano-concerto",
        path: ["协奏曲", "钢琴协奏曲"],
        sortKey: "0020",
      },
    ],
    works: [
      {
        id: "work-beethoven-7",
        composerId: "composer-beethoven",
        groupIds: ["group-beethoven-symphony"],
        slug: "symphony-7",
        title: "第七交响曲",
        titleLatin: "Symphony No. 7 in A major, Op. 92",
        aliases: [],
        catalogue: "Op. 92",
        summary: "",
        sortKey: "0010",
        updatedAt: "2026-03-21T00:00:00.000Z",
      },
      {
        id: "work-beethoven-op54",
        composerId: "composer-beethoven",
        groupIds: ["group-beethoven-concerto"],
        slug: "op54",
        title: "a小调钢琴协奏曲",
        titleLatin: "Piano Concerto, Op. 54",
        aliases: [],
        catalogue: "Op. 54",
        summary: "",
        sortKey: "0020",
        updatedAt: "2026-03-21T00:00:00.000Z",
      },
    ],
    recordings: [
      {
        id: "recording-reference",
        workId: "work-beethoven-7",
        slug: "karajan-1977",
        title: "卡拉扬 - 柏林爱乐乐团 - 1977",
        workTypeHint: "orchestral",
        sortKey: "0010",
        isPrimaryRecommendation: false,
        updatedAt: "2026-03-21T00:00:00.000Z",
        images: [],
        credits: [
          { role: "conductor", personId: "person-karajan", displayName: "卡拉扬", label: "指挥" },
          { role: "orchestra", personId: "person-bpo", displayName: "柏林爱乐乐团", label: "乐团" },
        ],
        links: [],
        notes: "",
        performanceDateText: "1977",
        venueText: "Berlin",
        albumTitle: "",
        label: "",
        releaseDate: "",
        infoPanel: { text: "", articleId: "", collectionLinks: [] },
        legacyPath: "",
      },
    ],
  });
}

function replaceRecording(library: LibraryData, recording: Recording): LibraryData {
  return {
    ...library,
    recordings: [recording],
  };
}

describe("auditLibraryData", () => {
  it("flags placeholder entities kept in the library", () => {
    const library = {
      ...buildBaseLibrary(),
      people: [
        ...buildBaseLibrary().people,
        {
          id: "person-item",
          slug: "person-item",
          name: "-",
          nameLatin: "",
          country: "",
          avatarSrc: "",
          roles: ["orchestra" as const],
          aliases: [],
          sortKey: "9999",
          summary: "",
          infoPanel: { text: "", articleId: "", collectionLinks: [] },
          imageSourceUrl: "",
          imageSourceKind: "",
          imageAttribution: "",
          imageUpdatedAt: "",
        },
      ],
    };

    const issues = auditLibraryData(validateLibrary(library));

    expect(issues).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          code: "placeholder-entity",
          severity: "error",
          entityType: "person",
          entityId: "person-item",
          source: "people",
        }),
      ]),
    );
  });

  it("flags recordings that miss required credit roles for their family", () => {
    const library = buildBaseLibrary();
    const brokenRecording: Recording = {
      ...library.recordings[0],
      id: "recording-concerto-missing-soloist",
      workId: "work-beethoven-op54",
      slug: "broken-concerto",
      title: "卡拉扬 - 柏林爱乐乐团 - 1977",
      workTypeHint: "concerto" as const,
      credits: [
        { role: "conductor", personId: "person-karajan", displayName: "卡拉扬", label: "指挥" },
        { role: "orchestra", personId: "person-bpo", displayName: "柏林爱乐乐团", label: "乐团" },
      ],
    };

    const issues = auditLibraryData(replaceRecording(library, brokenRecording));

    expect(issues).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          code: "recording-missing-credit-role",
          severity: "error",
          entityType: "recording",
          entityId: "recording-concerto-missing-soloist",
          source: "recordings.credits",
          suggestedFix: expect.stringContaining("soloist"),
        }),
      ]),
    );
  });

  it("annotates missing credit issues with source path and default auto-fixable hint when legacy source exists", () => {
    const library = buildBaseLibrary();
    const brokenRecording: Recording = {
      ...library.recordings[0],
      id: "recording-concerto-missing-orchestra",
      workId: "work-beethoven-op54",
      slug: "broken-concerto",
      title: "安妮·费舍尔 - 1977",
      workTypeHint: "concerto" as const,
      legacyPath: "作曲家/罗伯特·舒曼/钢琴协奏曲/a小调钢琴协奏曲/福斯特_1953.htm",
      credits: [{ role: "soloist", personId: "person-anne", displayName: "安妮·费舍尔", label: "钢琴" }],
    };
    const reviewQueue: ReviewQueueEntry[] = [
      {
        entityId: "recording-concerto-missing-orchestra",
        entityType: "recording",
        issue: "missing-image",
        sourcePath: "作曲家/罗伯特·舒曼/钢琴协奏曲/a小调钢琴协奏曲/福斯特_1953.htm",
      },
    ];

    const issues = auditLibraryData(replaceRecording(library, brokenRecording), { reviewQueue });

    expect(issues).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          code: "recording-missing-credit-role",
          entityId: "recording-concerto-missing-orchestra",
          sourcePath: "作曲家/罗伯特·舒曼/钢琴协奏曲/a小调钢琴协奏曲/福斯特_1953.htm",
          resolutionHint: "auto-fixable",
        }),
      ]),
    );
  });

  it("allows audit hints to downgrade unresolved credit gaps to manual backfill", () => {
    const library = buildBaseLibrary();
    const brokenRecording: Recording = {
      ...library.recordings[0],
      id: "recording-choral-missing-ensemble",
      slug: "choral-gap",
      title: "伯恩斯坦",
      workTypeHint: "orchestral" as const,
      legacyPath: "作曲家/贝多芬/交响曲/第九交响曲“合唱”/伯恩斯坦1989.htm",
      credits: [{ role: "conductor", personId: "person-karajan", displayName: "卡拉扬", label: "指挥" }],
    };
    const reviewQueue: ReviewQueueEntry[] = [
      {
        entityId: "recording-choral-missing-ensemble",
        entityType: "recording",
        issue: "missing-performance-date",
        sourcePath: "作曲家/贝多芬/交响曲/第九交响曲“合唱”/伯恩斯坦1989.htm",
      },
    ];

    const issues = auditLibraryData(replaceRecording(library, brokenRecording), {
      reviewQueue,
      recordingIssueHints: {
        "recording-choral-missing-ensemble": {
          resolutionHint: "manual-backfill",
          details: ["archive 中没有可解析的乐团或合唱署名"],
        },
      },
    });

    expect(issues).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          code: "recording-missing-credit-role",
          entityId: "recording-choral-missing-ensemble",
          resolutionHint: "manual-backfill",
          details: ["archive 中没有可解析的乐团或合唱署名"],
        }),
      ]),
    );
  });

  it("suppresses missing credit issues when the role is explicitly waived as confirmed unknown", () => {
    const library = buildBaseLibrary();
    const brokenRecording: Recording = {
      ...library.recordings[0],
      id: "recording-concerto-confirmed-unknown-orchestra",
      workId: "work-beethoven-op54",
      slug: "confirmed-unknown-orchestra",
      title: "安妮·费舍尔 - 未知乐团",
      workTypeHint: "concerto" as const,
      legacyPath: "作曲家/罗伯特·舒曼/钢琴协奏曲/a小调钢琴协奏曲/福斯特_1953.htm",
      credits: [{ role: "soloist", personId: "person-anne", displayName: "安妮·费舍尔", label: "钢琴" }],
    };

    const issues = auditLibraryData(replaceRecording(library, brokenRecording), {
      recordingIssueHints: {
        "recording-concerto-confirmed-unknown-orchestra": {
          resolutionHint: "manual-backfill",
          waivedMissingRoles: ["orchestra_or_ensemble"],
          details: ["人工确认该条目乐团未知，不再继续追补。"],
        },
      },
    });

    expect(issues).not.toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          code: "recording-missing-credit-role",
          entityId: "recording-concerto-confirmed-unknown-orchestra",
        }),
      ]),
    );
  });

  it("flags suspicious ensemble names that should stay in manual cleanup queue", () => {
    const library = {
      ...buildBaseLibrary(),
      people: [
        ...buildBaseLibrary().people,
        {
          id: "person-hko-and-ro",
          slug: "hko-and-ro",
          name: "HKO & RO",
          fullName: "HKO & RO",
          nameLatin: "",
          country: "",
          avatarSrc: "",
          roles: ["orchestra" as const],
          aliases: [],
          sortKey: "9998",
          summary: "",
          infoPanel: { text: "", articleId: "", collectionLinks: [] },
          imageSourceUrl: "",
          imageSourceKind: "",
          imageAttribution: "",
          imageUpdatedAt: "",
        },
      ],
    };

    const issues = auditLibraryData(validateLibrary(library));

    expect(issues).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          code: "person-suspicious-ensemble-name",
          severity: "warning",
          entityType: "person",
          entityId: "person-hko-and-ro",
          source: "people.name",
        }),
      ]),
    );
  });

  it("flags polluted group identities that should be rebound to a stronger canonical entry", () => {
    const library = {
      ...buildBaseLibrary(),
      people: [
        ...buildBaseLibrary().people,
        {
          id: "person-leningrad",
          slug: "leningrad-philharmonic-orchestra",
          name: "列宁格勒爱乐乐团",
          fullName: "列宁格勒爱乐乐团",
          nameLatin: "Leningrad Philharmonic Orchestra",
          country: "Russia",
          avatarSrc: "",
          roles: ["orchestra" as const],
          aliases: ["Saint Petersburg Philharmonic Orchestra"],
          sortKey: "0100",
          summary: "正式条目",
          infoPanel: { text: "", articleId: "", collectionLinks: [] },
          imageSourceUrl: "",
          imageSourceKind: "",
          imageAttribution: "",
          imageUpdatedAt: "",
        },
        {
          id: "person-leningrad-polluted",
          slug: "leningrad-philharmonic-orchestra时间-地点-1979-东京",
          name: "列宁格勒爱乐乐团",
          fullName: "列宁格勒爱乐乐团",
          nameLatin: "Leningrad Philharmonic Orchestra",
          country: "Russia",
          avatarSrc: "",
          roles: ["orchestra" as const],
          aliases: ["LPO"],
          sortKey: "0110",
          summary: "",
          infoPanel: { text: "", articleId: "", collectionLinks: [] },
          imageSourceUrl: "",
          imageSourceKind: "",
          imageAttribution: "",
          imageUpdatedAt: "",
        },
      ],
    };

    const issues = auditLibraryData(validateLibrary(library));

    expect(issues).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          code: "person-polluted-group-identity",
          severity: "warning",
          entityType: "person",
          entityId: "person-leningrad-polluted",
          source: "people.slug",
          suggestedFix: expect.stringContaining("canonical"),
        }),
      ]),
    );
  });

  it("flags conflicts between recording work type and related work groups", () => {
    const library = buildBaseLibrary();
    const conflictingRecording: Recording = {
      ...library.recordings[0],
      id: "recording-type-conflict",
      slug: "type-conflict",
      workTypeHint: "concerto" as const,
    };

    const issues = auditLibraryData(replaceRecording(library, conflictingRecording));

    expect(issues).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          code: "recording-work-type-conflict",
          severity: "warning",
          entityType: "recording",
          entityId: "recording-type-conflict",
          source: "recordings.workTypeHint",
        }),
      ]),
    );
  });

  it("flags structured titles that contradict normalized credit-derived titles", () => {
    const library = buildBaseLibrary();
    const mismatchedRecording: Recording = {
      ...library.recordings[0],
      id: "recording-title-mismatch",
      slug: "title-mismatch",
      title: "阿巴多 - 芝加哥交响乐团 - 1984",
      performanceDateText: "1977",
      venueText: "",
    };

    const issues = auditLibraryData(replaceRecording(library, mismatchedRecording));

    expect(issues).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          code: "recording-title-credit-mismatch",
          severity: "warning",
          entityType: "recording",
          entityId: "recording-title-mismatch",
          source: "recordings.title",
        }),
      ]),
    );
  });

  it("flags placeholder or venue-like values left in recording metadata fields", () => {
    const library = buildBaseLibrary();
    const suspiciousRecording: Recording = {
      ...library.recordings[0],
      id: "recording-suspicious-metadata",
      slug: "suspicious-metadata",
      title: "Karajan - BPO - *",
      performanceDateText: "*",
      venueText: "",
    };

    const issues = auditLibraryData(replaceRecording(library, suspiciousRecording));

    expect(issues).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          code: "recording-suspicious-metadata",
          severity: "warning",
          entityType: "recording",
          entityId: "recording-suspicious-metadata",
          source: "recordings.performanceDateText",
        }),
      ]),
    );
  });

  it("builds a structured manual backfill queue for unresolved recording credit gaps", () => {
    const library = buildBaseLibrary();
    const brokenRecording: Recording = {
      ...library.recordings[0],
      id: "recording-choral-missing-ensemble",
      slug: "choral-gap",
      title: "第九交响曲“合唱”",
      workTypeHint: "orchestral" as const,
      legacyPath: "作曲家/贝多芬/交响曲/第九交响曲“合唱”/伯恩斯坦1989.htm",
      credits: [{ role: "conductor", personId: "person-karajan", displayName: "伯恩斯坦", label: "指挥" }],
    };

    const issues = auditLibraryData(replaceRecording(library, brokenRecording), {
      recordingIssueHints: {
        "recording-choral-missing-ensemble": {
          resolutionHint: "manual-backfill",
          details: ["archive 中缺少可解析的乐团信息，需人工补录。"],
        },
      },
    });

    const queue = buildManualBackfillQueue(replaceRecording(library, brokenRecording), issues);
    const groups = groupManualBackfillQueue(queue);

    expect(queue).toEqual([
      expect.objectContaining({
        entityId: "recording-choral-missing-ensemble",
        composerId: "composer-beethoven",
        workId: "work-beethoven-7",
        workTypeHint: "orchestral",
        missingRoles: ["orchestra_or_ensemble"],
        sourcePath: "作曲家/贝多芬/交响曲/第九交响曲“合唱”/伯恩斯坦1989.htm",
        details: ["archive 中缺少可解析的乐团信息，需人工补录。"],
      }),
    ]);

    expect(groups).toEqual([
      expect.objectContaining({
        composerId: "composer-beethoven",
        workId: "work-beethoven-7",
        itemCount: 1,
        entries: [
          expect.objectContaining({
            entityId: "recording-choral-missing-ensemble",
          }),
        ],
      }),
    ]);
  });

  it("builds a stable unresolved manual backfill reference payload", () => {
    const queue = [
      {
        entityId: "recording-1",
        issueCode: "recording-missing-credit-role" as const,
        composerId: "composer-beethoven",
        composerName: "贝多芬",
        workId: "work-beethoven-7",
        workTitle: "第七交响曲",
        recordingId: "recording-1",
        recordingTitle: "卡拉扬 - 柏林爱乐乐团 - 1977",
        workTypeHint: "orchestral",
        missingRoles: ["orchestra_or_ensemble"],
        sourcePath: "legacy/recording-1.htm",
        details: ["archive 缂哄皯鍏抽敭缃插悕"],
      },
    ];

    const reference = buildManualBackfillReference(queue);

    expect(reference).toEqual({
      total: 1,
      groups: [
        expect.objectContaining({
          composerId: "composer-beethoven",
          workId: "work-beethoven-7",
          itemCount: 1,
        }),
      ],
      entries: queue,
    });
  });
});
