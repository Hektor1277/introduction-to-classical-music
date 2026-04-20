import { describe, expect, it } from "vitest";

import { getAffectedPaths, mergeLibraryEntities } from "@/lib/owner-tools";
import { validateLibrary } from "@/lib/schema";

const library = validateLibrary({
  composers: [
    {
      id: "beethoven",
      slug: "beethoven",
      name: "贝多芬",
      nameLatin: "Ludwig van Beethoven",
      aliases: [],
      sortKey: "beethoven",
      summary: "德国作曲家。",
    },
  ],
  people: [
    {
      id: "karajan",
      slug: "karajan",
      name: "赫伯特·冯·卡拉扬",
      nameLatin: "Herbert von Karajan",
      roles: ["conductor"],
      aliases: [],
      sortKey: "karajan",
      summary: "奥地利指挥家。",
    },
  ],
  workGroups: [
    {
      id: "beethoven-symphony",
      composerId: "beethoven",
      title: "交响曲",
      slug: "交响曲",
      path: ["交响曲"],
      sortKey: "0100",
    },
  ],
  works: [
    {
      id: "beethoven-symphony-7",
      composerId: "beethoven",
      groupIds: ["beethoven-symphony"],
      slug: "第七交响曲",
      title: "第七交响曲",
      titleLatin: "Symphony No. 7 in A major, Op. 92",
      aliases: [],
      catalogue: "Op. 92",
      summary: "贝多芬第七交响曲。",
      sortKey: "0700",
      updatedAt: "2026-03-07T00:00:00.000Z",
    },
  ],
  recordings: [
    {
      id: "karajan-1963",
      workId: "beethoven-symphony-7",
      slug: "karajan-1963",
      title: "卡拉扬 1963",
      sortKey: "0100",
      isPrimaryRecommendation: true,
      updatedAt: "2026-03-07T00:00:00.000Z",
      images: [],
      credits: [{ role: "conductor", personId: "karajan", displayName: "赫伯特·冯·卡拉扬" }],
      links: [{ platform: "bilibili", url: "https://www.bilibili.com/video/BV1ut4y1d7VM" }],
      notes: "",
      performanceDateText: "1963",
      venueText: "Berlin",
      albumTitle: "",
      label: "",
      releaseDate: "",
    },
  ],
});

describe("getAffectedPaths", () => {
  it("returns the relevant public pages for a recording edit", () => {
    const paths = getAffectedPaths(library, "recording", "karajan-1963");

    expect(paths).toContain("/");
    expect(paths).toContain("/works/beethoven-symphony-7/");
    expect(paths).toContain("/recordings/karajan-1963/");
    expect(paths).toContain("/conductors/karajan/");
    expect(paths).toContain("/search/");
  });

  it("includes projected composer pages when a multi-role person is edited", () => {
    const multiRoleLibrary = validateLibrary({
      ...library,
      people: [
        ...library.people,
        {
          id: "mahler",
          slug: "gustav-mahler",
          name: "古斯塔夫·马勒",
          nameLatin: "Gustav Mahler",
          roles: ["composer", "conductor"],
          aliases: [],
          sortKey: "mahler",
          summary: "测试人物。",
        },
      ],
    });

    const paths = getAffectedPaths(multiRoleLibrary, "person", "mahler");

    expect(paths).toContain("/composers/gustav-mahler/");
    expect(paths).toContain("/conductors/gustav-mahler/");
  });
});

describe("mergeLibraryEntities", () => {
  it("merges duplicate people into a primary entity and rewires recording credits", () => {
    const duplicatedLibrary = validateLibrary({
      ...library,
      people: [
        ...library.people,
        {
          id: "karajan-legacy",
          slug: "karajan-legacy",
          name: "卡拉扬",
          nameLatin: "",
          roles: ["conductor"],
          aliases: ["Herbert Karajan"],
          sortKey: "karajan-legacy",
          summary: "",
        },
      ],
      recordings: [
        {
          ...library.recordings[0],
          credits: [{ role: "conductor", personId: "karajan-legacy", displayName: "卡拉扬" }],
        },
      ],
    });

    const merged = mergeLibraryEntities(duplicatedLibrary, "person", "karajan", "karajan-legacy");

    expect(merged.people).toHaveLength(1);
    expect(merged.people[0]?.aliases).toEqual(expect.arrayContaining(["卡拉扬", "Herbert Karajan"]));
    expect(merged.recordings[0]?.credits[0]?.personId).toBe("karajan");
  });
});
