import { describe, expect, it } from "vitest";
import { promises as fs } from "node:fs";
import path from "node:path";

import { validateArticles } from "@/lib/articles";
import { validateLibrary } from "@/lib/schema";
import type { SiteConfig } from "@/lib/library-store";
import { buildFeaturedArticles, buildRecentWorkUpdates, mergeSiteConfigPatch } from "@/lib/site-content";

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
  people: [],
  workGroups: [
    {
      id: "group-symphony",
      composerId: "beethoven",
      title: "交响曲",
      slug: "symphonies",
      path: ["交响曲"],
      sortKey: "0100",
    },
  ],
  works: [
    {
      id: "work-5",
      composerId: "beethoven",
      groupIds: ["group-symphony"],
      slug: "symphony-5",
      title: "第五交响曲",
      titleLatin: "",
      aliases: [],
      catalogue: "Op. 67",
      summary: "",
      sortKey: "0100",
      updatedAt: "2026-03-01T00:00:00.000Z",
    },
    {
      id: "work-7",
      composerId: "beethoven",
      groupIds: ["group-symphony"],
      slug: "symphony-7",
      title: "第七交响曲",
      titleLatin: "",
      aliases: [],
      catalogue: "Op. 92",
      summary: "",
      sortKey: "0200",
      updatedAt: "2026-03-02T00:00:00.000Z",
    },
  ],
  recordings: [
    {
      id: "recording-1",
      workId: "work-5",
      slug: "recording-1",
      title: "版本 1",
      sortKey: "0100",
      isPrimaryRecommendation: true,
      updatedAt: "2026-03-07T10:00:00.000Z",
      images: [],
      credits: [],
      links: [{ platform: "youtube", url: "https://www.youtube.com/watch?v=1", title: "" }],
      notes: "",
      performanceDateText: "",
      venueText: "",
      albumTitle: "",
      label: "",
      releaseDate: "",
    },
    {
      id: "recording-2",
      workId: "work-5",
      slug: "recording-2",
      title: "版本 2",
      sortKey: "0200",
      isPrimaryRecommendation: false,
      updatedAt: "2026-03-06T10:00:00.000Z",
      images: [],
      credits: [],
      links: [{ platform: "youtube", url: "https://www.youtube.com/watch?v=2", title: "" }],
      notes: "",
      performanceDateText: "",
      venueText: "",
      albumTitle: "",
      label: "",
      releaseDate: "",
    },
    {
      id: "recording-3",
      workId: "work-7",
      slug: "recording-3",
      title: "版本 3",
      sortKey: "0300",
      isPrimaryRecommendation: false,
      updatedAt: "2026-03-05T10:00:00.000Z",
      images: [],
      credits: [],
      links: [{ platform: "youtube", url: "https://www.youtube.com/watch?v=3", title: "" }],
      notes: "",
      performanceDateText: "",
      venueText: "",
      albumTitle: "",
      label: "",
      releaseDate: "",
    },
  ],
});

const articles = validateArticles([
  {
    id: "usage-guide",
    slug: "usage-guide",
    title: "不全书使用文档",
    summary: "介绍导入库、构建站点与日常维护流程。",
    markdown: "# 使用文档",
    showOnHome: true,
    createdAt: "2026-04-19T00:00:00.000Z",
    updatedAt: "2026-04-19T10:00:00.000Z",
  },
  {
    id: "piano-guide",
    slug: "piano-guide",
    title: "百大钢琴家目录",
    summary: "按时代与风格梳理代表钢琴家。",
    markdown: "# 钢琴家",
    showOnHome: false,
    createdAt: "2026-04-18T00:00:00.000Z",
    updatedAt: "2026-04-18T12:00:00.000Z",
  },
]);

describe("buildRecentWorkUpdates", () => {
  it("returns unique work-level updates instead of recording-level duplicates", () => {
    expect(buildRecentWorkUpdates(library, 5)).toEqual([
      {
        composerName: "路德维希·凡·贝多芬",
        href: "/works/work-5/",
        updatedAt: "2026-03-07",
        workId: "work-5",
        workTitle: "第五交响曲",
      },
      {
        composerName: "路德维希·凡·贝多芬",
        href: "/works/work-7/",
        updatedAt: "2026-03-05",
        workId: "work-7",
        workTitle: "第七交响曲",
      },
    ]);
  });
});

describe("buildFeaturedArticles", () => {
  it("returns only homepage-featured articles in reverse update order", () => {
    expect(buildFeaturedArticles(articles, 10)).toEqual([
      {
        id: "usage-guide",
        slug: "usage-guide",
        title: "不全书使用文档",
        summary: "介绍导入库、构建站点与日常维护流程。",
        href: "/columns/usage-guide/",
        updatedAt: "2026-04-19",
      },
    ]);
  });
});

describe("mergeSiteConfigPatch", () => {
  it("merges editable site fields and preserves unspecified values", () => {
    const current: SiteConfig = {
      title: "古典导聆不全书",
      subtitle: "公益性的古典音乐版本导聆目录",
      description: "旧描述",
      heroIntro: "旧首页引言",
      composerDirectoryIntro: "旧作曲家导语",
      conductorDirectoryIntro: "旧指挥导语",
      searchIntro: "旧搜索导语",
      about: ["第一段", "第二段"],
      contact: {
        label: "联系",
        value: "QQ 123456",
      },
      copyrightNotice: "旧版权",
      lastImportedAt: "2026-03-07T15:13:50.982Z",
    };

    expect(
      mergeSiteConfigPatch(current, {
        subtitle: "新的副标题",
        about: ["新的第一段"],
        contact: {
          value: "QQ 439183718",
        },
      }),
    ).toEqual({
      title: "古典导聆不全书",
      subtitle: "新的副标题",
      description: "旧描述",
      heroIntro: "旧首页引言",
      composerDirectoryIntro: "旧作曲家导语",
      conductorDirectoryIntro: "旧指挥导语",
      searchIntro: "旧搜索导语",
      about: ["新的第一段"],
      contact: {
        label: "联系",
        value: "QQ 439183718",
      },
      copyrightNotice: "旧版权",
      lastImportedAt: "2026-03-07T15:13:50.982Z",
    });
  });
});

describe("site recording presentation wiring", () => {
  it("feeds the homepage daily recommendations from the structured recording display model", async () => {
    const page = await fs.readFile(path.resolve("apps/site/src/pages/index.astro"), "utf8");
    const component = await fs.readFile(path.resolve("apps/site/src/components/DailyRecommendations.astro"), "utf8");

    expect(page).toContain("buildRecordingDisplayModel");
    expect(page).toContain("composerPrimary: display.daily.composerPrimary");
    expect(page).toContain("composerSecondary: display.daily.composerSecondary");
    expect(component.indexOf("daily-card__work-group")).toBeLessThan(component.indexOf("daily-card__recording-group"));
    expect(component).toContain("daily-card__composer");
    expect(component).toContain("daily-card__line--fade");
    expect(component).toContain("daily-card__line--empty");
    expect(component).toContain('const lineValue = (value: string) => value || "\\u00a0";');
    expect(component).toContain('title={item.title || undefined}');
    expect(component).toContain('title={item.subtitle || undefined}');
    expect(component).toContain("datePlacePrimary");
    expect(page).toContain("buildFeaturedArticles");
    expect(page).toContain("featuredArticles");
  });
});
