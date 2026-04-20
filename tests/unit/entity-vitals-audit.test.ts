import { describe, expect, it } from "vitest";

import { validateLibrary } from "@/lib/schema";
import { auditEntityVitals, extractLifeSpanFromSummary } from "../../packages/data-core/src/entity-vitals-audit.js";

describe("auditEntityVitals", () => {
  it("extracts birth and death years from dotted or spaced summary dates", () => {
    expect(extractLifeSpanFromSummary("卡尔·舒里希特（Carl Schuricht 1880.7.3–1967.1.7），德国指挥家。")).toEqual({
      birthYear: 1880,
      deathYear: 1967,
    });
    expect(extractLifeSpanFromSummary("乔纳森·诺特（Jonathan Nott，1963 年出生于英国索利哈尔）是一位英国指挥家。")).toEqual({
      birthYear: 1963,
      deathYear: undefined,
    });
  });

  it("flags non-group people that still miss a birth year", () => {
    const library = validateLibrary({
      composers: [],
      people: [
        {
          id: "person-gatti",
          slug: "daniele-gatti",
          name: "丹尼尔·加蒂",
          fullName: "丹尼尔·加蒂",
          nameLatin: "Daniele Gatti",
          country: "Italy",
          avatarSrc: "",
          aliases: [],
          sortKey: "0001",
          summary: "丹尼尔·加蒂（Daniele Gatti，1961年11月6日－），意大利指挥家。",
          infoPanel: { text: "", articleId: "", collectionLinks: [] },
          imageSourceUrl: "",
          imageSourceKind: "",
          imageAttribution: "",
          imageUpdatedAt: "",
          roles: ["conductor"],
        },
      ],
      workGroups: [],
      works: [],
      recordings: [],
    });

    const result = auditEntityVitals(library);

    expect(result.issues).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          code: "birthyear-missing",
          entityId: "person-gatti",
        }),
      ]),
    );
  });

  it("flags a missing death year when the summary already exposes a full life span", () => {
    const library = validateLibrary({
      composers: [],
      people: [
        {
          id: "person-ozawa",
          slug: "seiji-ozawa",
          name: "小泽征尔",
          fullName: "小泽征尔",
          nameLatin: "Seiji Ozawa",
          country: "Japan",
          birthYear: 1935,
          avatarSrc: "",
          aliases: [],
          sortKey: "0002",
          summary: "小泽征尔（1935年9月1日-2024年2月6日），日本指挥家。",
          infoPanel: { text: "", articleId: "", collectionLinks: [] },
          imageSourceUrl: "",
          imageSourceKind: "",
          imageAttribution: "",
          imageUpdatedAt: "",
          roles: ["conductor"],
        },
      ],
      workGroups: [],
      works: [],
      recordings: [],
    });

    const result = auditEntityVitals(library);

    expect(result.issues).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          code: "deathyear-missing",
          entityId: "person-ozawa",
        }),
      ]),
    );
  });

  it("does not infer a birth year from later career dates when the summary lacks a life-span lead", () => {
    const library = validateLibrary({
      composers: [],
      people: [
        {
          id: "person-welser-most",
          slug: "franz-welser-most",
          name: "弗朗茨·威尔瑟-莫斯特",
          fullName: "弗朗茨·威尔瑟-莫斯特",
          nameLatin: "Franz Welser-Möst",
          country: "Austria",
          birthYear: 1960,
          avatarSrc: "",
          aliases: [],
          sortKey: "0003",
          summary:
            "威尔瑟-莫斯特生于林茨，早年学习小提琴和钢琴，1974年进入林茨音乐中学，16岁起便开始指挥学校的乐团和合唱团。1980年至1984年他在慕尼黑音乐学院学习，但未毕业。",
          infoPanel: { text: "", articleId: "", collectionLinks: [] },
          imageSourceUrl: "",
          imageSourceKind: "",
          imageAttribution: "",
          imageUpdatedAt: "",
          roles: ["conductor"],
        },
      ],
      workGroups: [],
      works: [],
      recordings: [],
    });

    const result = auditEntityVitals(library);

    expect(result.issues).not.toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          code: "summary-birthyear-conflict",
          entityId: "person-welser-most",
        }),
      ]),
    );
  });

  it("flags summary year conflicts on composers", () => {
    const library = validateLibrary({
      composers: [
        {
          id: "composer-beethoven",
          slug: "beethoven",
          name: "路德维希·凡·贝多芬",
          fullName: "路德维希·凡·贝多芬",
          nameLatin: "Ludwig van Beethoven",
          country: "Germany",
          avatarSrc: "",
          birthYear: 1803,
          deathYear: 1804,
          aliases: ["贝多芬"],
          sortKey: "0010",
          summary: "路德维希·凡·贝多芬（Ludwig van Beethoven，1770年12月16日或17日—1827年3月26日），德国作曲家。",
          infoPanel: { text: "", articleId: "", collectionLinks: [] },
          imageSourceUrl: "",
          imageSourceKind: "",
          imageAttribution: "",
          imageUpdatedAt: "",
          roles: ["composer"],
        },
      ],
      people: [],
      workGroups: [],
      works: [],
      recordings: [],
    });

    const result = auditEntityVitals(library);

    expect(result.issues).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          code: "summary-birthyear-conflict",
          entityId: "composer-beethoven",
        }),
        expect.objectContaining({
          code: "summary-deathyear-conflict",
          entityId: "composer-beethoven",
        }),
      ]),
    );
  });

  it("does not misread the death year as the birth year when a valid span is present", () => {
    const library = validateLibrary({
      composers: [
        {
          id: "composer-mahler",
          slug: "mahler",
          name: "古斯塔夫·马勒",
          fullName: "古斯塔夫·马勒",
          nameLatin: "Gustav Mahler",
          country: "Austria",
          avatarSrc: "",
          birthYear: 1860,
          deathYear: 1911,
          aliases: [],
          sortKey: "0011",
          summary: "古斯塔夫·马勒（Gustav Mahler，1860年7月7日－1911年5月18日），奥地利作曲家及指挥家。",
          infoPanel: { text: "", articleId: "", collectionLinks: [] },
          imageSourceUrl: "",
          imageSourceKind: "",
          imageAttribution: "",
          imageUpdatedAt: "",
          roles: ["composer"],
        },
      ],
      people: [],
      workGroups: [],
      works: [],
      recordings: [],
    });

    const result = auditEntityVitals(library);

    expect(result.issues).not.toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          code: "summary-birthyear-conflict",
          entityId: "composer-mahler",
        }),
      ]),
    );
  });

  it("flags groups that still carry life-span fields", () => {
    const library = validateLibrary({
      composers: [],
      people: [
        {
          id: "person-bpo",
          slug: "berliner-philharmoniker",
          name: "柏林爱乐乐团",
          fullName: "柏林爱乐乐团",
          nameLatin: "Berliner Philharmoniker",
          country: "Germany",
          avatarSrc: "",
          birthYear: 1882,
          aliases: ["BPO"],
          sortKey: "0010",
          summary: "德国柏林的主要交响乐团。",
          infoPanel: { text: "", articleId: "", collectionLinks: [] },
          imageSourceUrl: "",
          imageSourceKind: "",
          imageAttribution: "",
          imageUpdatedAt: "",
          roles: ["orchestra"],
        },
      ],
      workGroups: [],
      works: [],
      recordings: [],
    });

    const result = auditEntityVitals(library);

    expect(result.issues).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          code: "group-has-life-span",
          entityId: "person-bpo",
        }),
      ]),
    );
  });

  it("does not treat repertoire mentions as country conflicts", () => {
    const library = validateLibrary({
      composers: [],
      people: [
        {
          id: "person-osr",
          slug: "swiss-romande-orchestra",
          name: "瑞士罗曼德管弦乐团",
          fullName: "瑞士罗曼德管弦乐团",
          nameLatin: "Orchestre de la Suisse Romande",
          country: "Switzerland",
          avatarSrc: "",
          aliases: [],
          sortKey: "0012",
          summary: "瑞士罗曼德管弦乐团，由指挥家安塞梅于1918年在日内瓦创立，以其对法国和瑞士作曲家作品的精湛演绎而闻名。",
          infoPanel: { text: "", articleId: "", collectionLinks: [] },
          imageSourceUrl: "",
          imageSourceKind: "",
          imageAttribution: "",
          imageUpdatedAt: "",
          roles: ["orchestra"],
        },
      ],
      workGroups: [],
      works: [],
      recordings: [],
    });

    const result = auditEntityVitals(library);

    expect(result.issues).not.toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          code: "summary-country-conflict",
          entityId: "person-osr",
        }),
      ]),
    );
  });

  it("does not treat later study-year ranges as a life span", () => {
    const library = validateLibrary({
      composers: [],
      people: [
        {
          id: "person-blomstedt",
          slug: "herbert-blomstedt",
          name: "赫伯特·布隆斯泰特",
          fullName: "赫伯特·布隆斯泰特",
          nameLatin: "Herbert Blomstedt",
          country: "Sweden",
          avatarSrc: "",
          birthYear: 1927,
          aliases: [],
          sortKey: "0013",
          summary:
            "赫伯特·布隆斯泰特出生于美国斯普林菲尔德，父母是瑞典人，两岁时随父母迁回瑞典。布隆斯泰特最早于1945-1950年在斯德哥尔摩皇家音乐学院学习，后来又在乌普萨拉大学和茱莉亚学院继续深造。",
          infoPanel: { text: "", articleId: "", collectionLinks: [] },
          imageSourceUrl: "",
          imageSourceKind: "",
          imageAttribution: "",
          imageUpdatedAt: "",
          roles: ["conductor"],
        },
      ],
      workGroups: [],
      works: [],
      recordings: [],
    });

    const result = auditEntityVitals(library);

    expect(result.issues).not.toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          code: "summary-birthyear-conflict",
          entityId: "person-blomstedt",
        }),
      ]),
    );
  });

  it("flags invalid life span and missing country", () => {
    const library = validateLibrary({
      composers: [],
      people: [
        {
          id: "person-karajan",
          slug: "karajan",
          name: "赫伯特·冯·卡拉扬",
          fullName: "赫伯特·冯·卡拉扬",
          nameLatin: "Herbert von Karajan",
          country: "",
          avatarSrc: "",
          birthYear: 1908,
          deathYear: 989,
          aliases: ["卡拉扬"],
          sortKey: "0010",
          summary: "赫伯特·冯·卡拉扬（Herbert von Karajan，1908年4月5日－1989年7月16日），奥地利指挥家。",
          infoPanel: { text: "", articleId: "", collectionLinks: [] },
          imageSourceUrl: "",
          imageSourceKind: "",
          imageAttribution: "",
          imageUpdatedAt: "",
          roles: ["conductor"],
        },
      ],
      workGroups: [],
      works: [],
      recordings: [],
    });

    const result = auditEntityVitals(library);

    expect(result.issues).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          code: "invalid-life-span",
          entityId: "person-karajan",
        }),
        expect.objectContaining({
          code: "summary-deathyear-conflict",
          entityId: "person-karajan",
        }),
        expect.objectContaining({
          code: "country-missing",
          entityId: "person-karajan",
        }),
      ]),
    );

    expect(result.issues).not.toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          code: "summary-birthyear-conflict",
          entityId: "person-karajan",
        }),
      ]),
    );
  });

  it("treats summary country hits as valid when they are included in a multi-country entity", () => {
    const library = validateLibrary({
      composers: [],
      people: [
        {
          id: "person-barenboim",
          slug: "daniel-barenboim",
          name: "丹尼尔·巴伦博伊姆",
          fullName: "丹尼尔·巴伦博伊姆",
          nameLatin: "Daniel Barenboim",
          country: "",
          countries: ["Argentina", "Israel", "Palestine"],
          avatarSrc: "",
          birthYear: 1942,
          aliases: ["巴伦博伊姆"],
          sortKey: "0014",
          summary: "丹尼尔·巴伦博伊姆（Daniel Barenboim），1942年出生于阿根廷布宜诺斯艾利斯，拥有以色列和巴勒斯坦国籍。",
          infoPanel: { text: "", articleId: "", collectionLinks: [] },
          imageSourceUrl: "",
          imageSourceKind: "",
          imageAttribution: "",
          imageUpdatedAt: "",
          roles: ["conductor"],
        },
      ],
      workGroups: [],
      works: [],
      recordings: [],
    });

    const result = auditEntityVitals(library);

    expect(result.issues).not.toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          code: "country-missing",
          entityId: "person-barenboim",
        }),
        expect.objectContaining({
          code: "summary-country-conflict",
          entityId: "person-barenboim",
        }),
      ]),
    );
  });

  it("ignores later collaboration-country mentions outside the opening identity sentence", () => {
    const library = validateLibrary({
      composers: [],
      people: [
        {
          id: "person-barenboim-extended",
          slug: "daniel-barenboim-extended",
          name: "丹尼尔·巴伦博伊姆",
          fullName: "丹尼尔·巴伦博伊姆",
          nameLatin: "Daniel Barenboim",
          country: "",
          countries: ["Argentina", "Israel"],
          avatarSrc: "",
          birthYear: 1942,
          aliases: ["巴伦博伊姆"],
          sortKey: "0015",
          summary:
            "丹尼尔·巴伦博伊姆（Daniel Barenboim），1942年11月15日出生于阿根廷布宜诺斯艾利斯，拥有以色列和巴勒斯坦双重国籍的钢琴演奏者、指挥家。1955年，作为钢琴家在巴黎举行职业首演，此后逐渐成为世界乐坛知名度最高的钢琴家和指挥家之一。1965--1975年间，以钢琴家和指挥家身份与英国室内乐团频繁合作，并进行国际巡演，后来受邀于英国交响乐团做客座指挥。",
          infoPanel: { text: "", articleId: "", collectionLinks: [] },
          imageSourceUrl: "",
          imageSourceKind: "",
          imageAttribution: "",
          imageUpdatedAt: "",
          roles: ["conductor"],
        },
      ],
      workGroups: [],
      works: [],
      recordings: [],
    });

    const result = auditEntityVitals(library);

    expect(result.issues).not.toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          code: "summary-country-conflict",
          entityId: "person-barenboim-extended",
        }),
      ]),
    );
  });
});
