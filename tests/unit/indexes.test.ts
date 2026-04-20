import { describe, expect, it } from "vitest";

import { validateArticles } from "@/lib/articles";
import { buildIndexes } from "@/lib/indexes";
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
      birthYear: 1770,
      deathYear: 1827,
      aliases: [],
      sortKey: "beethoven",
      summary: "德国作曲家。",
    },
  ],
  people: [
    {
      id: "karajan",
      slug: "karajan",
      name: "卡拉扬",
      fullName: "赫伯特·冯·卡拉扬",
      nameLatin: "Herbert von Karajan",
      country: "Austria",
      avatarSrc: "",
      birthYear: 1908,
      deathYear: 1989,
      roles: ["conductor"],
      aliases: [],
      sortKey: "karajan",
      summary: "奥地利指挥家。",
    },
    {
      id: "karajan-alias",
      slug: "old-karajan",
      name: "老卡拉扬",
      fullName: "",
      nameLatin: "Karajan",
      country: "",
      avatarSrc: "",
      roles: ["conductor"],
      aliases: [],
      sortKey: "karajan-2",
      summary: "卡拉扬的简称。",
    },
    {
      id: "annie-fischer",
      slug: "annie-fischer",
      name: "安妮·费舍尔",
      fullName: "安妮·费舍尔",
      nameLatin: "Annie Fischer",
      country: "Hungary",
      avatarSrc: "",
      birthYear: 1914,
      deathYear: 1995,
      roles: ["soloist"],
      aliases: [],
      sortKey: "annie-fischer",
      summary: "匈牙利钢琴家。",
    },
    {
      id: "berlin-phil",
      slug: "berlin-phil",
      name: "柏林爱乐乐团",
      fullName: "",
      nameLatin: "Berliner Philharmoniker",
      country: "Germany",
      avatarSrc: "",
      roles: ["orchestra"],
      aliases: ["Berliner Philharmonic"],
      sortKey: "berlin-phil",
      summary: "德国乐团。",
    },
  ],
  workGroups: [
    {
      id: "beethoven-sonata",
      composerId: "beethoven",
      title: "奏鸣曲",
      slug: "sonatas",
      path: ["奏鸣曲"],
      sortKey: "0100",
    },
    {
      id: "beethoven-piano-sonata",
      composerId: "beethoven",
      title: "钢琴奏鸣曲",
      slug: "piano-sonatas",
      path: ["奏鸣曲", "钢琴奏鸣曲"],
      sortKey: "0110",
    },
  ],
  works: [
    {
      id: "beethoven-appassionata",
      composerId: "beethoven",
      groupIds: ["beethoven-sonata", "beethoven-piano-sonata"],
      slug: "appassionata",
      title: "第二十三号奏鸣曲《热情》",
      titleLatin: "Piano Sonata No. 23 in F minor, Op. 57",
      aliases: ["热情"],
      catalogue: "Op. 57",
      summary: "贝多芬钢琴奏鸣曲。",
      sortKey: "2300",
      updatedAt: "2026-03-07T00:00:00.000Z",
    },
  ],
  recordings: [
    {
      id: "annie-fischer-1980",
      workId: "beethoven-appassionata",
      slug: "annie-fischer-1980",
      title: "安妮·费舍尔 1980",
      sortKey: "0100",
      isPrimaryRecommendation: true,
      updatedAt: "2026-03-07T00:00:00.000Z",
      images: [],
      credits: [
        { role: "soloist", personId: "annie-fischer", displayName: "安妮·费舍尔" },
        { role: "conductor", personId: "karajan-alias", displayName: "老卡拉扬" },
        { role: "orchestra", personId: "berlin-phil", displayName: "柏林爱乐乐团" },
      ],
      links: [{ platform: "youtube", url: "https://www.youtube.com/watch?v=123456" }],
      notes: "",
      performanceDateText: "1980",
      venueText: "",
      albumTitle: "",
      label: "",
      releaseDate: "",
    },
  ],
});

const personLinks = {
  canonicalPersonLinks: {
    "karajan-alias": "karajan",
  },
};

const articles = validateArticles([
  {
    id: "guide-getting-started",
    slug: "getting-started",
    title: "不全书使用文档",
    summary: "帮助用户快速导入库、构建站点并开始浏览。",
    markdown: "## 安装\n\n先打开维护工具，再构建站点。",
    showOnHome: true,
    createdAt: "2026-04-19T00:00:00.000Z",
    updatedAt: "2026-04-19T00:00:00.000Z",
  },
]);

describe("buildIndexes", () => {
  it("builds a composer tree that stays type-first", () => {
    const indexes = buildIndexes(library, personLinks);
    const composerTree = indexes.composerTree.beethoven;

    expect(composerTree?.children[0]?.title).toBe("奏鸣曲");
    expect(composerTree?.children[0]?.children[0]?.title).toBe("钢琴奏鸣曲");
    expect(composerTree?.children[0]?.children[0]?.works[0]?.href).toBe("/works/beethoven-appassionata/");
  });

  it("keeps work paths consistent across conductor and search indexes", () => {
    const indexes = buildIndexes(library, personLinks);
    const conductorEntry = indexes.conductorIndex.karajan;
    const searchWorkEntry = indexes.searchIndex.find(
      (entry) => entry.kind === "work" && entry.id === "beethoven-appassionata",
    );

    expect(conductorEntry?.groups[0]?.works[0]?.href).toBe("/works/beethoven-appassionata/");
    expect(searchWorkEntry?.href).toBe("/works/beethoven-appassionata/");
  });

  it("includes recordings in search and keeps canonical orchestra names ahead of aliases", () => {
    const indexes = buildIndexes(library, personLinks);
    const types = new Set<string>(indexes.searchIndex.map((entry) => entry.kind));
    const orchestraEntry = indexes.searchIndex.find((entry) => entry.kind === "orchestra" && entry.id === "berlin-phil");
    const recordingEntry = indexes.searchIndex.find((entry) => entry.kind === "recording" && entry.id === "annie-fischer-1980");

    expect(types.has("recording")).toBe(true);
    expect(indexes.searchIndex.some((entry) => entry.kind === "conductor" && entry.id === "karajan")).toBe(true);
    expect(indexes.searchIndex.some((entry) => entry.kind === "person" && entry.id === "annie-fischer")).toBe(true);
    expect(indexes.searchIndex.some((entry) => entry.kind === "orchestra" && entry.id === "berlin-phil")).toBe(true);
    expect(orchestraEntry?.primaryText).toBe("柏林爱乐乐团");
    expect(recordingEntry?.primaryText).toBe("卡拉扬 - 柏林爱乐乐团 - 1980");
  });

  it("merges alias conductor credits into the canonical conductor page", () => {
    const indexes = buildIndexes(library, personLinks);

    expect(indexes.conductorIndex.karajan?.groups[0]?.works[0]?.recordings[0]?.id).toBe("annie-fischer-1980");
    expect(indexes.conductorIndex["karajan-alias"]).toBeUndefined();
  });

  it("builds an orchestra index and orchestra search hrefs", () => {
    const indexes = buildIndexes(library, personLinks);

    expect(indexes.orchestraIndex["berlin-phil"]?.groups[0]?.works[0]?.recordings[0]?.id).toBe("annie-fischer-1980");
    expect(indexes.orchestraIndex["berlin-phil"]?.groups[0]?.works[0]?.recordings[0]?.title).toBe("卡拉扬 - 柏林爱乐乐团 - 1980");
    expect(indexes.searchIndex.find((entry) => entry.id === "berlin-phil")?.href).toBe("/orchestras/berlin-phil/");
  });

  it("does not duplicate the catalogue when a work's original title already contains it", () => {
    const enrichedLibrary = {
      ...library,
      works: [
        ...library.works,
        {
          id: "bruckner-7",
          composerId: "beethoven",
          groupIds: ["beethoven-sonata"],
          slug: "bruckner-7",
          title: "第七交响曲",
          titleLatin: "Symphony No.7 in E major, WAB 107",
          aliases: [],
          catalogue: "WAB 107",
          summary: "",
          sortKey: "0300",
          updatedAt: "2026-03-18T00:00:00.000Z",
        },
      ],
    };
    const indexes = buildIndexes(enrichedLibrary, personLinks);
    const searchWorkEntry = indexes.searchIndex.find((entry) => entry.id === "bruckner-7");

    expect(searchWorkEntry?.primaryText).toBe("第七交响曲 / Symphony No.7 in E major / WAB 107");
  });

  it("includes articles in the search index", () => {
    const indexes = buildIndexes(library, personLinks, articles);
    const articleEntry = indexes.searchIndex.find((entry) => entry.kind === "article" && entry.id === "guide-getting-started");

    expect(articleEntry).toBeDefined();
    expect(articleEntry?.href).toBe("/columns/getting-started/");
    expect(articleEntry?.primaryText).toBe("不全书使用文档");
    expect(articleEntry?.secondaryText).toContain("帮助用户快速导入库");
  });
});
