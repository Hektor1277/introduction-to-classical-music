import { describe, expect, it } from "vitest";

import { buildComposerDirectoryEntry, buildDirectorySections, buildPersonDirectoryEntry, createDirectoryDisplayEntry } from "@/lib/directory";

describe("buildDirectorySections", () => {
  const entries = [
    buildComposerDirectoryEntry(
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
        summary: "德国作曲家。",
      },
      {
        href: "/composers/beethoven/",
        representativeWorks: ["第三交响曲《英雄》", "第五交响曲《命运》"],
      },
    ),
    buildComposerDirectoryEntry(
      {
        id: "berlioz",
        slug: "berlioz",
        name: "柏辽兹",
        fullName: "赫克托·路易·柏辽兹",
        nameLatin: "Hector Louis Berlioz",
        country: "France",
        avatarSrc: "",
        birthYear: 1803,
        deathYear: 1869,
        aliases: [],
        summary: "法国作曲家。",
      },
      {
        href: "/composers/berlioz/",
        representativeWorks: ["幻想交响曲"],
      },
    ),
  ];

  it("groups entries alphabetically by surname", () => {
    const grouped = buildDirectorySections(entries, "surname");

    expect(grouped.rail).toEqual([{ label: "B", targetId: "B" }]);
    expect(grouped.sections[0]?.items.map((item) => item.id)).toEqual(["beethoven", "berlioz"]);
  });

  it("normalizes mixed-script surname initials into a single latin section", () => {
    const grouped = buildDirectorySections(
      [
        buildPersonDirectoryEntry(
          {
            id: "zender",
            slug: "zender",
            name: "汉斯·岑德",
            fullName: "汉斯·岑德",
            nameLatin: "Hans Zender",
            country: "Germany",
            avatarSrc: "",
            birthYear: 1936,
            deathYear: 2019,
            aliases: [],
            summary: "德国指挥家。",
          },
          {
            href: "/conductors/hans-zender/",
            representativeWorks: [],
          },
        ),
        buildPersonDirectoryEntry(
          {
            id: "mravinsky",
            slug: "mravinsky",
            name: "叶夫根尼·亚历山德罗维奇·穆拉文斯基",
            fullName: "叶夫根尼·亚历山德罗维奇·穆拉文斯基",
            nameLatin: "Евгений Александрович Мравинский | Evgeni·Mravinsky",
            country: "Soviet Union",
            avatarSrc: "",
            birthYear: 1903,
            deathYear: 1988,
            aliases: [],
            summary: "苏联指挥家。",
          },
          {
            href: "/conductors/evgeni-mravinsky/",
            representativeWorks: [],
          },
        ),
      ],
      "surname",
    );

    expect(grouped.rail).toEqual([
      { label: "M", targetId: "M" },
      { label: "Z", targetId: "Z" },
    ]);
    expect(grouped.sections.map((section) => section.title)).toEqual(["M", "Z"]);
    expect(grouped.sections[0]?.items.map((item) => item.id)).toEqual(["mravinsky"]);
  });

  it("groups entries chronologically by birth decade", () => {
    const grouped = buildDirectorySections(entries, "birth");

    expect(grouped.rail).toEqual([
      { label: "1770s", targetId: "1770s" },
      { label: "1800s", targetId: "1800s" },
    ]);
    expect(grouped.sections[0]?.title).toBe("1770s");
    expect(grouped.sections[1]?.title).toBe("1800s");
  });

  it("groups entries alphabetically by country", () => {
    const grouped = buildDirectorySections(entries, "country");

    expect(grouped.rail).toEqual([
      { label: "F", targetId: "country-france" },
      { label: "G", targetId: "country-germany" },
    ]);
    expect(grouped.sections.map((section) => section.title)).toEqual(["France", "Germany"]);
  });

  it("creates a fixed-height display model with clamped summary and representative works", () => {
    const display = createDirectoryDisplayEntry({
      ...entries[0],
      summary:
        "贝多芬的简介被故意拉长，用来测试目录条目在统一固定高度布局下是否会提前被截断，并且不会因为内容过长而把单条目录项撑得比其他条目更高。",
      representativeWorks: [
        "第三交响曲《英雄》",
        "第五交响曲《命运》",
        "第九交响曲《合唱》",
        "第二十三钢琴奏鸣曲《热情》",
        "庄严弥撒",
      ],
    });

    expect(display.summaryExcerpt.length).toBeLessThan(display.summary.length);
    expect(display.representativeWorks).toHaveLength(3);
    expect(display.representativeWorksLabel).toContain("第三交响曲《英雄》");
  });

  it("derives separate latin highlights for common surnames even when the chinese short name differs", () => {
    const entry = buildComposerDirectoryEntry(
      {
        id: "schumann",
        slug: "schumann",
        name: "罗伯特·舒曼",
        fullName: "罗伯特·舒曼",
        nameLatin: "Robert Schumann",
        country: "Germany",
        avatarSrc: "",
        birthYear: 1810,
        deathYear: 1856,
        aliases: ["舒曼"],
        summary: "德国作曲家。",
      },
      {
        href: "/composers/schumann/",
        representativeWorks: [],
      },
    );

    expect(entry.fullNameHighlight).toBe("舒曼");
    expect(entry.nameLatinHighlight).toBe("Schumann");
  });

  it("does not infer life span from later study-year ranges", () => {
    const entry = buildPersonDirectoryEntry(
      {
        id: "blomstedt",
        slug: "herbert-blomstedt",
        name: "赫伯特·布隆斯泰特",
        fullName: "赫伯特·布隆斯泰特",
        nameLatin: "Herbert Blomstedt",
        country: "",
        avatarSrc: "",
        aliases: [],
        summary:
          "赫伯特·布隆斯泰特出生于美国斯普林菲尔德，父母是瑞典人，两岁时随父母迁回瑞典。布隆斯泰特最早于1945-1950年在斯德哥尔摩皇家音乐学院学习，后来又在乌普萨拉大学继续深造。",
      },
      {
        href: "/conductors/herbert-blomstedt/",
        representativeWorks: [],
      },
    );

    expect(entry.birthYear).toBeUndefined();
    expect(entry.deathYear).toBeUndefined();
  });

  it("does not infer country from incidental mentions in summary", () => {
    const entry = buildPersonDirectoryEntry(
      {
        id: "barenboim",
        slug: "daniel-barenboim",
        name: "丹尼尔·巴伦博伊姆",
        fullName: "丹尼尔·巴伦博伊姆",
        nameLatin: "Daniel Barenboim",
        country: "",
        avatarSrc: "",
        aliases: [],
        summary:
          "丹尼尔·巴伦博伊姆生于阿根廷，拥有以色列和巴勒斯坦双重国籍，第二次世界大战期间长期在英国各地演出，并在柏林与芝加哥担任过多个艺术职位。",
      },
      {
        href: "/conductors/daniel-barenboim/",
        representativeWorks: [],
      },
    );

    expect(entry.countryLabel).toBe("Unknown");
  });

  it("renders multiple countries in the visible country label while keeping grouping stable", () => {
    const entry = buildPersonDirectoryEntry(
      {
        id: "barenboim",
        slug: "daniel-barenboim",
        name: "丹尼尔·巴伦博伊姆",
        fullName: "丹尼尔·巴伦博伊姆",
        nameLatin: "Daniel Barenboim",
        country: "Argentina",
        countries: ["Argentina", "Israel", "Palestine"],
        avatarSrc: "",
        aliases: [],
        summary: "阿根廷-以色列钢琴家、指挥家。",
      },
      {
        href: "/conductors/daniel-barenboim/",
        representativeWorks: [],
      },
    );

    expect(entry.countryLabel).toBe("Argentina / Israel / Palestine");
    expect(entry.countrySortKey).toBe("Argentina");
    expect(entry.quickKeys.country).toBe("A");
  });

  it("splits multi-country entries into separate country sections", () => {
    const grouped = buildDirectorySections(
      [
        buildPersonDirectoryEntry(
          {
            id: "barenboim",
            slug: "daniel-barenboim",
            name: "丹尼尔·巴伦博伊姆",
            fullName: "丹尼尔·巴伦博伊姆",
            nameLatin: "Daniel Barenboim",
            country: "Argentina",
            countries: ["Argentina", "Israel", "Palestine"],
            avatarSrc: "",
            aliases: [],
            summary: "阿根廷裔钢琴家、指挥家。",
          },
          {
            href: "/conductors/daniel-barenboim/",
            representativeWorks: [],
          },
        ),
      ],
      "country",
    );

    expect(grouped.rail).toEqual([
      { label: "A", targetId: "country-argentina" },
      { label: "I", targetId: "country-israel" },
      { label: "P", targetId: "country-palestine" },
    ]);
    expect(grouped.sections.map((section) => section.title)).toEqual(["Argentina", "Israel", "Palestine"]);
    expect(grouped.sections.every((section) => section.items.map((item) => item.id).includes("barenboim"))).toBe(true);
  });

  it("infers life span from a leading birth-and-death statement without a dash pair", () => {
    const entry = buildPersonDirectoryEntry(
      {
        id: "abendroth",
        slug: "hermann-abendroth",
        name: "阿本德罗特",
        fullName: "阿本德罗特",
        nameLatin: "Hermann Abendroth",
        country: "",
        avatarSrc: "",
        aliases: [],
        summary:
          "1883年1月19日生于法兰克福，1956年5月29日卒于耶拿。德国指挥家、教育家。1903-1904年任慕尼黑管弦乐团指挥，1907-1911年任吕贝克歌剧院首席指挥。",
      },
      {
        href: "/conductors/hermann-abendroth/",
        representativeWorks: [],
      },
    );

    expect(entry.birthYear).toBe(1883);
    expect(entry.deathYear).toBe(1956);
  });

  it("does not replace a valid life span with later tenure ranges", () => {
    const entry = buildPersonDirectoryEntry(
      {
        id: "bertini",
        slug: "gary-bertini",
        name: "加里·贝蒂尼",
        fullName: "加里·贝蒂尼",
        nameLatin: "Gary Bertini",
        country: "",
        avatarSrc: "",
        aliases: [],
        summary:
          "加里·贝蒂尼（Gary Bertini，1927年5月1日－2005年3月17日），以色列著名指挥家。曾担任耶路撒冷交响乐团首席指挥（1978-1986）、底特律交响乐团音乐指导（1981-1983）、科隆广播交响乐团首席指挥（1983-1991）。",
      },
      {
        href: "/conductors/gary-bertini/",
        representativeWorks: [],
      },
    );

    expect(entry.birthYear).toBe(1927);
    expect(entry.deathYear).toBe(2005);
  });
});
