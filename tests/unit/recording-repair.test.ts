import { describe, expect, it } from "vitest";

import { validateLibrary } from "@/lib/schema";
import {
  backfillRecordingWorkTypeHints,
  normalizeRecordingMetadata,
  rebuildRecordingDerivedFields,
  recordingNeedsLegacyRepair,
  repairRecordingFromLegacyParse,
} from "../../packages/data-core/src/recording-repair.js";

describe("recording repair helpers", () => {
  it("backfills unknown recording work type hints from related work context", () => {
    const library = validateLibrary({
      composers: [
        {
          id: "composer-schumann",
          slug: "schumann",
          name: "舒曼",
          fullName: "罗伯特·舒曼",
          nameLatin: "Robert Schumann",
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
          id: "group-schumann-concerto",
          composerId: "composer-schumann",
          title: "钢琴协奏曲",
          slug: "piano-concerto",
          path: ["协奏曲", "钢琴协奏曲"],
          sortKey: "0010",
        },
      ],
      works: [
        {
          id: "work-schumann-op54",
          composerId: "composer-schumann",
          groupIds: ["group-schumann-concerto"],
          slug: "op54",
          title: "a小调钢琴协奏曲",
          titleLatin: "Piano Concerto, Op. 54",
          aliases: [],
          catalogue: "Op. 54",
          summary: "",
          sortKey: "0010",
          updatedAt: "2026-03-21T00:00:00.000Z",
        },
      ],
      recordings: [
        {
          id: "recording-op54-kletzki",
          workId: "work-schumann-op54",
          slug: "kletzki-1954",
          title: "克列茨基 1954",
          workTypeHint: "unknown",
          sortKey: "0010",
          isPrimaryRecommendation: false,
          updatedAt: "2026-03-21T00:00:00.000Z",
          images: [],
          credits: [],
          links: [],
          notes: "",
          performanceDateText: "1954",
          venueText: "",
          albumTitle: "",
          label: "",
          releaseDate: "",
          infoPanel: { text: "", articleId: "", collectionLinks: [] },
          legacyPath: "",
        },
      ],
    });

    const repaired = backfillRecordingWorkTypeHints(library);

    expect(repaired.recordings[0]?.workTypeHint).toBe("concerto");
  });

  it("replaces placeholder orchestra credits with parsed legacy credits and preserves valid existing links", () => {
    const library = validateLibrary({
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
          id: "person-item",
          slug: "item",
          name: "-",
          fullName: "-",
          nameLatin: "",
          country: "",
          avatarSrc: "",
          roles: ["orchestra"],
          aliases: [],
          sortKey: "0001",
          summary: "",
        },
        {
          id: "person-furtwangler",
          slug: "furtwangler",
          name: "威尔海姆·富特文格勒",
          fullName: "威尔海姆·富特文格勒",
          nameLatin: "Wilhelm Furtwangler",
          country: "Germany",
          avatarSrc: "",
          roles: ["conductor"],
          aliases: [],
          sortKey: "0010",
          summary: "",
        },
        {
          id: "person-bayreuth",
          slug: "bayreuth-festival-orchestra-chorus",
          name: "拜罗伊特节日剧院合唱团与管弦乐团",
          fullName: "拜罗伊特节日剧院合唱团与管弦乐团",
          nameLatin: "Bayreuth Festival Orchestra & Chorus",
          country: "Germany",
          avatarSrc: "",
          roles: ["orchestra", "chorus"],
          aliases: ["Bayreuth Festival Orchestra & Chorus"],
          sortKey: "0011",
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
      ],
      works: [
        {
          id: "work-beethoven-9",
          composerId: "composer-beethoven",
          groupIds: ["group-beethoven-symphony"],
          slug: "symphony-9",
          title: "第九交响曲“合唱”",
          titleLatin: "Symphony No. 9 in D minor, Op. 125",
          aliases: [],
          catalogue: "Op. 125",
          summary: "",
          sortKey: "0010",
          updatedAt: "2026-03-21T00:00:00.000Z",
        },
      ],
      recordings: [
        {
          id: "recording-beethoven-9-furt-1951",
          workId: "work-beethoven-9",
          slug: "furt-1951",
          title: "富特 1951",
          workTypeHint: "unknown",
          sortKey: "0010",
          isPrimaryRecommendation: false,
          updatedAt: "2026-03-21T00:00:00.000Z",
          images: [],
          credits: [
            { role: "orchestra", personId: "person-item", displayName: "-", label: "乐团" },
            { role: "conductor", personId: "person-furtwangler", displayName: "威尔海姆·富特文格勒", label: "文件名推断" },
          ],
          links: [],
          notes: "",
          performanceDateText: "",
          venueText: "",
          albumTitle: "",
          label: "",
          releaseDate: "",
          infoPanel: { text: "", articleId: "", collectionLinks: [] },
          legacyPath: "作曲家/贝多芬/交响曲/第九交响曲“合唱”/富特1951.htm",
        },
      ],
    });

    const repaired = repairRecordingFromLegacyParse(library, library.recordings[0], {
      credits: [
        { role: "orchestra", personId: "", displayName: "Bayreuth Festival Orchestra & Chorus", label: "乐团" },
        { role: "conductor", personId: "", displayName: "威尔海姆·富特文格勒", label: "指挥" },
      ],
      performanceDateText: "29 July 1951, at Festspielhaus",
      venueText: "in Bayreuth",
      albumTitle: "Bayreuth 1951",
      label: "Orfeo",
      releaseDate: "1952",
      images: [],
      links: [],
    });

    expect(repaired.workTypeHint).toBe("orchestral");
    expect(repaired.credits).toEqual([
      {
        role: "conductor",
        personId: "person-furtwangler",
        displayName: "威尔海姆·富特文格勒",
        label: "文件名推断",
      },
      {
        role: "orchestra",
        personId: "person-bayreuth",
        displayName: "拜罗伊特节日剧院合唱团与管弦乐团",
        label: "乐团",
      },
    ]);
    expect(repaired.performanceDateText).toBe("29 July 1951, at Festspielhaus");
    expect(repaired.venueText).toBe("in Bayreuth");
    expect(repaired.albumTitle).toBe("Bayreuth 1951");
    expect(repaired.label).toBe("Orfeo");
    expect(repaired.releaseDate).toBe("1952");
  });

  it("marks recordings with missing required ensemble credits for legacy repair", () => {
    const library = validateLibrary({
      composers: [
        {
          id: "composer-schumann",
          slug: "schumann",
          name: "舒曼",
          fullName: "罗伯特·舒曼",
          nameLatin: "Robert Schumann",
          country: "Germany",
          avatarSrc: "",
          aliases: [],
          sortKey: "0010",
          summary: "",
        },
      ],
      people: [
        {
          id: "person-foster",
          slug: "sidney-foster",
          name: "福斯特",
          fullName: "西德尼·福斯特",
          nameLatin: "Sidney Foster",
          country: "United States",
          avatarSrc: "",
          roles: ["soloist"],
          aliases: [],
          sortKey: "0010",
          summary: "",
        },
      ],
      workGroups: [
        {
          id: "group-schumann-concerto",
          composerId: "composer-schumann",
          title: "钢琴协奏曲",
          slug: "piano-concerto",
          path: ["协奏曲", "钢琴协奏曲"],
          sortKey: "0010",
        },
      ],
      works: [
        {
          id: "work-schumann-op54",
          composerId: "composer-schumann",
          groupIds: ["group-schumann-concerto"],
          slug: "op54",
          title: "a小调钢琴协奏曲",
          titleLatin: "Piano Concerto in A minor, Op. 54",
          aliases: [],
          catalogue: "Op. 54",
          summary: "",
          sortKey: "0010",
          updatedAt: "2026-03-22T00:00:00.000Z",
        },
      ],
      recordings: [
        {
          id: "recording-op54-foster-1953",
          workId: "work-schumann-op54",
          slug: "foster-1953",
          title: "福斯特 - 1953",
          workTypeHint: "concerto",
          sortKey: "0010",
          isPrimaryRecommendation: false,
          updatedAt: "2026-03-22T00:00:00.000Z",
          images: [],
          credits: [{ role: "soloist", personId: "person-foster", displayName: "西德尼·福斯特", label: "独奏" }],
          links: [],
          notes: "",
          performanceDateText: "1953",
          venueText: "",
          albumTitle: "",
          label: "",
          releaseDate: "",
          infoPanel: { text: "", articleId: "", collectionLinks: [] },
          legacyPath: "legacy/op54-foster-1953.htm",
        },
      ],
    });

    expect(recordingNeedsLegacyRepair(library, library.recordings[0])).toBe(true);
  });

  it("does not flag structurally valid recordings only because optional venue or release metadata is blank", () => {
    const library = validateLibrary({
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
          aliases: [],
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
          aliases: [],
          sortKey: "0011",
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
          updatedAt: "2026-03-22T00:00:00.000Z",
        },
      ],
      recordings: [
        {
          id: "recording-beethoven-7-karajan-1963",
          workId: "work-beethoven-7",
          slug: "karajan-1963",
          title: "卡拉扬 - 柏林爱乐乐团 - 1963",
          workTypeHint: "orchestral",
          sortKey: "0010",
          isPrimaryRecommendation: false,
          updatedAt: "2026-03-22T00:00:00.000Z",
          images: [],
          credits: [
            { role: "conductor", personId: "person-karajan", displayName: "卡拉扬", label: "指挥" },
            { role: "orchestra", personId: "person-bpo", displayName: "柏林爱乐乐团", label: "乐团" },
          ],
          links: [],
          notes: "",
          performanceDateText: "1963",
          venueText: "",
          albumTitle: "",
          label: "",
          releaseDate: "",
          infoPanel: { text: "", articleId: "", collectionLinks: [] },
          legacyPath: "legacy/beethoven-7-karajan-1963.htm",
        },
      ],
    });

    expect(recordingNeedsLegacyRepair(library, library.recordings[0])).toBe(false);
  });

  it("promotes venueText that actually names an orchestra into a structured orchestra credit", () => {
    const library = validateLibrary({
      composers: [
        {
          id: "composer-mahler",
          slug: "mahler",
          name: "马勒",
          fullName: "古斯塔夫·马勒",
          nameLatin: "Gustav Mahler",
          country: "Austria",
          avatarSrc: "",
          aliases: [],
          sortKey: "0010",
          summary: "",
        },
      ],
      people: [
        {
          id: "person-sinopoli",
          slug: "sinopoli",
          name: "西诺波利",
          fullName: "朱塞佩·西诺波利",
          nameLatin: "Giuseppe Sinopoli",
          country: "Italy",
          avatarSrc: "",
          roles: ["conductor"],
          aliases: [],
          sortKey: "0010",
          summary: "",
        },
        {
          id: "person-dresden",
          slug: "staatskapelle-dresden",
          name: "德累斯顿国立乐团",
          fullName: "德累斯顿国立乐团",
          nameLatin: "Sächsische Staatskapelle Dresden",
          country: "Germany",
          avatarSrc: "",
          roles: ["orchestra"],
          aliases: ["Sächsische Staatskapelle Dresden"],
          sortKey: "0011",
          summary: "",
        },
      ],
      workGroups: [
        {
          id: "group-mahler-symphony",
          composerId: "composer-mahler",
          title: "交响曲",
          slug: "symphony",
          path: ["交响曲"],
          sortKey: "0010",
        },
      ],
      works: [
        {
          id: "work-mahler-5",
          composerId: "composer-mahler",
          groupIds: ["group-mahler-symphony"],
          slug: "symphony-5",
          title: "第五交响曲",
          titleLatin: "Symphony No. 5",
          aliases: [],
          catalogue: "",
          summary: "",
          sortKey: "0010",
          updatedAt: "2026-03-22T00:00:00.000Z",
        },
      ],
      recordings: [
        {
          id: "recording-mahler-5-sinopoli-1999",
          workId: "work-mahler-5",
          slug: "sinopoli-1999",
          title: "西诺波利 - 1999",
          workTypeHint: "orchestral",
          sortKey: "0010",
          isPrimaryRecommendation: false,
          updatedAt: "2026-03-22T00:00:00.000Z",
          images: [],
          credits: [{ role: "conductor", personId: "person-sinopoli", displayName: "西诺波利", label: "指挥" }],
          links: [],
          notes: "",
          performanceDateText: "1999",
          venueText: "Sächsische Staatskapelle Dresden",
          albumTitle: "",
          label: "",
          releaseDate: "",
          infoPanel: { text: "", articleId: "", collectionLinks: [] },
          legacyPath: "legacy/mahler-5-sinopoli-1999.htm",
        },
      ],
    });

    const repaired = rebuildRecordingDerivedFields(library, library.recordings[0]);

    expect(repaired.credits).toEqual([
      { role: "conductor", personId: "person-sinopoli", displayName: "西诺波利", label: "指挥" },
      { role: "orchestra", personId: "person-dresden", displayName: "德累斯顿国立乐团", label: "地点回填乐团" },
    ]);
    expect(repaired.venueText).toBe("");
  });

  it("swaps performance and venue fields when the venue carries the date-like value", () => {
    const library = validateLibrary({
      composers: [
        {
          id: "composer-placeholder",
          slug: "placeholder",
          name: "占位作曲家",
          fullName: "占位作曲家",
          nameLatin: "",
          country: "",
          avatarSrc: "",
          aliases: [],
          sortKey: "0001",
          summary: "",
        },
      ],
      people: [],
      workGroups: [
        {
          id: "group-placeholder",
          composerId: "composer-placeholder",
          title: "占位分组",
          slug: "placeholder-group",
          path: ["占位分组"],
          sortKey: "0001",
        },
      ],
      works: [
        {
          id: "work-placeholder",
          composerId: "composer-placeholder",
          groupIds: ["group-placeholder"],
          slug: "placeholder",
          title: "占位作品",
          titleLatin: "",
          aliases: [],
          catalogue: "",
          summary: "",
          sortKey: "0001",
          updatedAt: "2026-03-22T00:00:00.000Z",
        },
      ],
      recordings: [
        {
          id: "recording-metadata-swap",
          workId: "work-placeholder",
          slug: "metadata-swap",
          title: "测试版本",
          workTypeHint: "unknown",
          sortKey: "0010",
          isPrimaryRecommendation: false,
          updatedAt: "2026-03-22T00:00:00.000Z",
          images: [],
          credits: [],
          links: [],
          notes: "",
          performanceDateText: "莫斯科音乐学院大音乐厅",
          venueText: "1954",
          albumTitle: "",
          label: "",
          releaseDate: "",
          infoPanel: { text: "", articleId: "", collectionLinks: [] },
          legacyPath: "",
        },
      ],
    });

    const repaired = rebuildRecordingDerivedFields(library, library.recordings[0]);

    expect(repaired.performanceDateText).toBe("1954");
    expect(repaired.venueText).toBe("莫斯科音乐学院大音乐厅");
  });
  it("clears placeholder metadata values before rebuilding the display title", () => {
    const library = validateLibrary({
      composers: [
        {
          id: "composer-placeholder",
          slug: "placeholder",
          name: "Placeholder Composer",
          fullName: "Placeholder Composer",
          nameLatin: "Placeholder Composer",
          country: "",
          avatarSrc: "",
          aliases: [],
          sortKey: "0001",
          summary: "",
        },
      ],
      people: [
        {
          id: "person-soloist",
          slug: "soloist",
          name: "Soloist",
          fullName: "Soloist",
          nameLatin: "Soloist",
          country: "",
          avatarSrc: "",
          roles: ["soloist"],
          aliases: [],
          sortKey: "0001",
          summary: "",
        },
      ],
      workGroups: [
        {
          id: "group-chamber",
          composerId: "composer-placeholder",
          title: "Chamber",
          slug: "chamber",
          path: ["Chamber"],
          sortKey: "0001",
        },
      ],
      works: [
        {
          id: "work-placeholder",
          composerId: "composer-placeholder",
          groupIds: ["group-chamber"],
          slug: "work-placeholder",
          title: "Placeholder Work",
          titleLatin: "",
          aliases: [],
          catalogue: "",
          summary: "",
          sortKey: "0001",
          updatedAt: "2026-03-25T00:00:00.000Z",
        },
      ],
      recordings: [
        {
          id: "recording-placeholder-metadata",
          workId: "work-placeholder",
          slug: "placeholder-metadata",
          title: "Soloist - *",
          workTypeHint: "chamber_solo",
          sortKey: "0001",
          isPrimaryRecommendation: false,
          updatedAt: "2026-03-25T00:00:00.000Z",
          images: [],
          credits: [{ role: "soloist", personId: "person-soloist", displayName: "Soloist", label: "soloist" }],
          links: [],
          notes: "",
          performanceDateText: "*",
          venueText: "",
          albumTitle: "",
          label: "",
          releaseDate: "",
          infoPanel: { text: "", articleId: "", collectionLinks: [] },
          legacyPath: "",
        },
      ],
    });

    const repaired = rebuildRecordingDerivedFields(library, library.recordings[0]);

    expect(repaired.performanceDateText).toBe("");
    expect(repaired.venueText).toBe("");
    expect(repaired.title).toBe("Soloist");
  });

  it("moves venue-like text out of performanceDateText when no real date is present", () => {
    const normalized = normalizeRecordingMetadata({
      performanceDateText: "Amsterdam",
      venueText: "",
    });

    expect(normalized).toEqual({
      performanceDateText: "",
      venueText: "Amsterdam",
    });
  });
});
