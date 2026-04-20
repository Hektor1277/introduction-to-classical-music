import { describe, expect, it } from "vitest";

import { validateLibrary, type LibraryData, type Recording } from "@/lib/schema";
import {
  normalizeRecordingCredits,
  normalizeRecordingMetadata,
  rebuildRecordingDerivedFields,
} from "../../packages/data-core/src/recording-repair.js";

function buildLibrary(): LibraryData {
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
        aliases: ["BPO"],
        sortKey: "0011",
        summary: "",
      },
      {
        id: "person-bayreuth-chorus",
        slug: "bayreuth-chorus",
        name: "拜罗伊特节日剧院合唱团",
        fullName: "拜罗伊特节日剧院合唱团",
        nameLatin: "Bayreuth Festival Chorus",
        country: "Germany",
        avatarSrc: "",
        roles: ["chorus"],
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
    ],
    works: [
      {
        id: "work-beethoven-3",
        composerId: "composer-beethoven",
        groupIds: ["group-beethoven-symphony"],
        slug: "symphony-3",
        title: "第三交响曲“英雄”",
        titleLatin: "Symphony No. 3 in E-flat major, Op. 55",
        aliases: [],
        catalogue: "Op. 55",
        summary: "",
        sortKey: "0010",
        updatedAt: "2026-03-21T00:00:00.000Z",
      },
    ],
    recordings: [
      {
        id: "recording-reference",
        workId: "work-beethoven-3",
        slug: "karajan-1971",
        title: "卡拉扬- 1971",
        workTypeHint: "unknown",
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
        performanceDateText: "1971",
        venueText: "",
        albumTitle: "",
        label: "",
        releaseDate: "",
        infoPanel: { text: "", articleId: "", collectionLinks: [] },
        legacyPath: "",
      },
    ],
  });
}

describe("recording cleanup normalization helpers", () => {
  it("splits legacy date-place composites into date and venue fields", () => {
    const normalized = normalizeRecordingMetadata({
      performanceDateText: "29 July 1951, at Festspielhaus / in Bayreuth",
      venueText: "",
    });

    expect(normalized).toEqual({
      performanceDateText: "29 July 1951, at Festspielhaus",
      venueText: "in Bayreuth",
    });
  });

  it("normalizes ensemble-like credit roles to the canonical person role when possible", () => {
    const library = buildLibrary();
    const normalizedCredits = normalizeRecordingCredits(library, [
      {
        role: "ensemble",
        personId: "person-bpo",
        displayName: "柏林爱乐乐团",
        label: "乐团",
      },
      {
        role: "ensemble",
        personId: "person-bayreuth-chorus",
        displayName: "拜罗伊特节日剧院合唱团",
        label: "合唱",
      },
    ]);

    expect(normalizedCredits).toEqual([
      {
        role: "orchestra",
        personId: "person-bpo",
        displayName: "柏林爱乐乐团",
        label: "乐团",
      },
      {
        role: "chorus",
        personId: "person-bayreuth-chorus",
        displayName: "拜罗伊特节日剧院合唱团",
        label: "合唱",
      },
    ]);
  });

  it("rebuilds title and work type from structured credits and work context", () => {
    const library = buildLibrary();
    const rebuilt = rebuildRecordingDerivedFields(library, library.recordings[0]);

    expect(rebuilt.workTypeHint).toBe("orchestral");
    expect(rebuilt.title).toBe("卡拉扬 - 柏林爱乐乐团 - 1971");
  });

  it("preserves the normalized credits while rebuilding derived fields", () => {
    const library = buildLibrary();
    const rebuilt = rebuildRecordingDerivedFields(library, {
      ...library.recordings[0],
      credits: [
        { role: "conductor", personId: "person-karajan", displayName: "卡拉扬", label: "指挥" },
        { role: "ensemble", personId: "person-bpo", displayName: "柏林爱乐乐团", label: "乐团" },
      ],
      performanceDateText: "29 July 1951, at Festspielhaus / in Bayreuth",
      venueText: "",
      title: "富特- 1951",
    } as Recording);

    expect(rebuilt.credits).toEqual([
      { role: "conductor", personId: "person-karajan", displayName: "卡拉扬", label: "指挥" },
      { role: "orchestra", personId: "person-bpo", displayName: "柏林爱乐乐团", label: "乐团" },
    ]);
    expect(rebuilt.performanceDateText).toBe("29 July 1951, at Festspielhaus");
    expect(rebuilt.venueText).toBe("in Bayreuth");
  });
});
