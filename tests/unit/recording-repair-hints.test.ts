import { describe, expect, it } from "vitest";

import { validateLibrary, type LibraryData, type Recording } from "@/lib/schema";
import { classifyRecordingLegacyRepairHint } from "../../packages/data-core/src/recording-repair.js";

function buildLibrary(): LibraryData {
  return validateLibrary({
    composers: [
      {
        id: "composer-schumann",
        slug: "schumann",
        name: "罗伯特·舒曼",
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
        id: "person-rudin",
        slug: "alexander-rudin",
        name: "亚历山大·鲁丁",
        fullName: "亚历山大·鲁丁",
        nameLatin: "Alexander Rudin",
        country: "Russia",
        avatarSrc: "",
        roles: ["conductor"],
        aliases: [],
        sortKey: "0010",
        summary: "",
      },
      {
        id: "person-moscow-state-symphony",
        slug: "moscow-state-symphony-orchestra",
        name: "莫斯科国立交响乐团",
        fullName: "莫斯科国立交响乐团",
        nameLatin: "Moscow State Symphony Orchestra",
        country: "Russia",
        avatarSrc: "",
        roles: ["orchestra"],
        aliases: [],
        sortKey: "0011",
        summary: "",
      },
    ],
    workGroups: [
      {
        id: "group-schumann-concerto",
        composerId: "composer-schumann",
        title: "协奏曲",
        slug: "concerto",
        path: ["协奏曲"],
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
        updatedAt: "2026-03-23T00:00:00.000Z",
      },
    ],
    recordings: [],
  });
}

function buildRecording(): Recording {
  return {
    id: "recording-schumann-op54-rudin",
    workId: "work-schumann-op54",
    slug: "rudin",
    title: "鲁丁",
    workTypeHint: "concerto",
    sortKey: "0010",
    isPrimaryRecommendation: false,
    updatedAt: "2026-03-23T00:00:00.000Z",
    images: [],
    credits: [
      {
        role: "conductor",
        personId: "person-rudin",
        displayName: "亚历山大·鲁丁",
        label: "指挥",
      },
    ],
    links: [],
    notes: "",
    performanceDateText: "",
    venueText: "",
    albumTitle: "",
    label: "",
    releaseDate: "",
    infoPanel: { text: "", articleId: "", collectionLinks: [] },
    legacyPath: "作曲家/罗伯特·舒曼/钢琴协奏曲/a小调钢琴协奏曲/维尔萨拉泽&鲁丁.htm",
  };
}

describe("classifyRecordingLegacyRepairHint", () => {
  it("returns auto-fixable when parsed legacy source contains the missing ensemble credit", () => {
    const library = buildLibrary();
    const recording = buildRecording();

    const hint = classifyRecordingLegacyRepairHint(library, recording, {
      credits: [
        {
          role: "soloist",
          personId: "",
          displayName: "伊丽莎·维尔萨拉泽",
          label: "钢琴",
        },
        {
          role: "conductor",
          personId: "",
          displayName: "亚历山大·鲁丁",
          label: "指挥",
        },
        {
          role: "orchestra",
          personId: "",
          displayName: "莫斯科国立交响乐团",
          label: "乐团",
        },
      ],
      performanceDateText: "",
      venueText: "莫斯科音乐学院大音乐厅",
      albumTitle: "",
      label: "",
      releaseDate: "",
      images: [],
      links: [],
    });

    expect(hint).toEqual({
      resolutionHint: "auto-fixable",
    });
  });

  it("returns manual-backfill when parsed legacy source still lacks the required ensemble credit", () => {
    const library = buildLibrary();
    const recording = buildRecording();

    const hint = classifyRecordingLegacyRepairHint(library, recording, {
      credits: [
        {
          role: "soloist",
          personId: "",
          displayName: "伊丽莎·维尔萨拉泽",
          label: "钢琴",
        },
        {
          role: "conductor",
          personId: "",
          displayName: "亚历山大·鲁丁",
          label: "指挥",
        },
      ],
      performanceDateText: "",
      venueText: "莫斯科音乐学院大音乐厅",
      albumTitle: "",
      label: "",
      releaseDate: "",
      images: [],
      links: [],
    });

    expect(hint).toEqual({
      resolutionHint: "manual-backfill",
      details: ["archive 中缺少可解析的关键署名：orchestra_or_ensemble"],
    });
  });
});
