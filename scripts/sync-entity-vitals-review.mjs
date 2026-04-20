import fs from "node:fs";
import path from "node:path";

import { extractLifeSpanFromSummary } from "../output/runtime/packages/data-core/src/entity-vitals-audit.js";

const rootDir = process.cwd();
const reviewPath = path.join(rootDir, "data/library/entity-vitals-review.json");
const peoplePath = path.join(rootDir, "data/library/people.json");

const groupRoles = new Set(["orchestra", "ensemble", "chorus"]);

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function writeJson(filePath, value) {
  fs.writeFileSync(filePath, `${JSON.stringify(value, null, 2)}\n`, "utf8");
}

function compact(value) {
  return String(value ?? "").trim();
}

function dedupeStrings(values) {
  return [...new Set((values || []).map((value) => compact(value)).filter(Boolean))];
}

function dedupeSources(values) {
  const seen = new Set();
  const next = [];
  for (const item of values || []) {
    const label = compact(item?.label);
    const url = compact(item?.url);
    const key = `${label}::${url}`;
    if (!label || seen.has(key)) {
      continue;
    }
    seen.add(key);
    next.push(url ? { label, url } : { label });
  }
  return next;
}

function mergeEntry(existing, incoming) {
  return {
    entityType: incoming.entityType,
    entityId: incoming.entityId,
    set: {
      ...(existing?.set || {}),
      ...(incoming.set || {}),
    },
    sources: dedupeSources([...(existing?.sources || []), ...(incoming.sources || [])]),
    notes: compact(incoming.notes) || compact(existing?.notes),
    removeFields: dedupeStrings([...(existing?.removeFields || []), ...(incoming.removeFields || [])]),
  };
}

function upsertReviewEntry(reviewEntries, incoming) {
  const index = reviewEntries.findIndex(
    (entry) => entry.entityType === incoming.entityType && entry.entityId === incoming.entityId,
  );
  if (index === -1) {
    reviewEntries.push(mergeEntry(null, incoming));
    return;
  }
  reviewEntries[index] = mergeEntry(reviewEntries[index], incoming);
}

function localSummarySource() {
  return [{ label: "Current library summary" }];
}

const manualEntries = [
  {
    entityType: "person",
    entityId: "person-乔治-路德维希约胡姆",
    set: {
      birthYear: 1909,
      deathYear: 1970,
      summary:
        "乔治-路德维希·约胡姆（Georg-Ludwig Jochum，1909年9月10日—1970年11月18日）是德国指挥家，长期活跃于德奥乐坛，以布鲁克纳、理查·施特劳斯等作品的演绎闻名。",
    },
    sources: [{ label: "Wikidata: Georg-Ludwig Jochum", url: "https://www.wikidata.org/wiki/Q91843" }],
    notes: "Wikidata gives Georg-Ludwig Jochum's life dates as 1909-1970.",
  },
  {
    entityType: "person",
    entityId: "person-保罗克列茨基",
    set: {
      birthYear: 1900,
      deathYear: 1973,
      roles: ["conductor"],
      summary:
        "保罗·克列茨基（Paul Kletzki，1900年3月21日—1973年3月5日）是波兰指挥家、作曲家，后长期在瑞士与西欧乐坛发展，以德奥交响曲与协奏曲诠释见长。",
    },
    sources: [{ label: "Wikidata: Paul Kletzki", url: "https://www.wikidata.org/wiki/Q31261" }],
    notes: "Wikidata gives Paul Kletzki's life dates as 1900-1973; his library role is normalized back to conductor after duplicate filename soloist credits were removed.",
  },
  {
    entityType: "person",
    entityId: "person-弗里茨莱纳",
    set: {
      birthYear: 1888,
      deathYear: 1963,
      summary:
        "弗里茨·莱纳（Fritz Reiner，1888年12月19日—1963年11月15日）是匈牙利裔美国指挥家，长期活跃于美国乐坛，以芝加哥交响乐团时期的录音和严谨精准的指挥风格闻名。",
    },
    sources: [{ label: "Wikidata: Fritz Reiner", url: "https://www.wikidata.org/wiki/Q364179" }],
    notes: "Wikidata gives Fritz Reiner's life dates as 1888-1963.",
  },
  {
    entityType: "person",
    entityId: "person-彼得厄特沃什",
    set: {
      birthYear: 1944,
      deathYear: 2024,
      summary:
        "彼得·厄特沃什（Peter Eötvös，1944年1月2日—2024年3月24日）是匈牙利作曲家、指挥家，长期活跃于当代音乐领域，以歌剧创作和现代乐队作品诠释著称。",
    },
    sources: [{ label: "Wikidata: Peter Eötvös", url: "https://www.wikidata.org/wiki/Q389851" }],
    notes: "Wikidata gives Peter Eötvös's life dates as 1944-2024.",
  },
  {
    entityType: "person",
    entityId: "person-瓦茨拉夫纽曼",
    set: {
      birthYear: 1920,
      deathYear: 1995,
      summary:
        "瓦茨拉夫·纽曼（Václav Neumann，1920年9月29日—1995年9月2日）是捷克指挥家，长期与捷克爱乐及布拉格乐坛联系紧密，以捷克与德奥作品诠释见长。",
    },
    sources: [{ label: "Wikidata: Václav Neumann", url: "https://www.wikidata.org/wiki/Q450793" }],
    notes: "Wikidata gives Václav Neumann's life dates as 1920-1995.",
  },
  {
    entityType: "person",
    entityId: "person-约第沙瓦尔",
    set: {
      birthYear: 1941,
      summary:
        "约第·沙瓦尔（Jordi Savall，1941年8月1日—）是西班牙维奥尔琴演奏家、指挥家和古乐推动者，以历史演奏实践和伊比利亚、巴洛克曲目的复兴工作闻名。",
    },
    sources: [{ label: "Opera Online: Jordi Savall", url: "https://www.opera-online.com/en/items/personnalities/jordi-savall-1941" }],
    notes: "Opera Online lists Jordi Savall as born in 1941.",
  },
  {
    entityType: "person",
    entityId: "person-埃莉索维尔萨拉泽",
    set: {
      birthYear: 1942,
      summary:
        "埃莉索·维尔萨拉泽（Eliso Virsaladze，1942年9月14日—）是格鲁吉亚钢琴家，生于第比利斯，以舒曼、舒伯特和俄罗斯作品诠释见长，长期在莫斯科与慕尼黑任教。",
    },
    sources: [{ label: "Wikidata: Eliso Virsaladze", url: "https://www.wikidata.org/wiki/Q273667" }],
    notes: "Wikidata lists Eliso Virsaladze as born in 1942.",
  },
  {
    entityType: "person",
    entityId: "person-弗拉基米尔阿什肯纳齐",
    set: {
      birthYear: 1937,
      summary:
        "弗拉基米尔·阿什肯纳齐（Vladimir Ashkenazy，1937年7月6日—）是俄罗斯裔冰岛钢琴家、指挥家，以俄奥德核心曲目诠释和广泛录音闻名。",
    },
    sources: [{ label: "Wikidata: Vladimir Ashkenazy", url: "https://www.wikidata.org/wiki/Q157785" }],
    notes: "Wikidata lists Vladimir Ashkenazy as born in 1937.",
  },
  {
    entityType: "person",
    entityId: "person-斯维亚托斯拉夫特奥菲洛维奇里赫特",
    set: {
      birthYear: 1915,
      deathYear: 1997,
      summary:
        "斯维亚托斯拉夫·特奥菲洛维奇·里赫特（Sviatoslav Richter，1915年3月20日—1997年8月1日）是苏联钢琴家，20世纪最重要的钢琴演奏家之一，以广阔曲目和强烈个性化诠释闻名。",
    },
    sources: [{ label: "Wikidata: Sviatoslav Richter", url: "https://www.wikidata.org/wiki/Q124890" }],
    notes: "Wikidata gives Sviatoslav Richter's life dates as 1915-1997.",
  },
  {
    entityType: "person",
    entityId: "person-玛丽亚伊斯拉列夫娜格林伯格",
    set: {
      birthYear: 1908,
      deathYear: 1978,
      summary:
        "玛丽亚·伊斯拉列夫娜·格林伯格（Maria Grinberg，1908年9月6日—1978年7月14日）是苏联钢琴家，生于敖德萨，以贝多芬、舒曼、舒伯特等作品的录音与演出闻名。",
    },
    sources: [{ label: "Wikipedia: Maria Grinberg", url: "https://en.wikipedia.org/wiki/Maria_Grinberg" }],
    notes: "Wikipedia gives Maria Grinberg's life dates as 1908-1978.",
  },
  {
    entityType: "person",
    entityId: "person-阿德利纳德劳拉",
    set: {
      birthYear: 1872,
      deathYear: 1961,
      summary:
        "阿德利纳·德·劳拉（Adelina de Lara，1872年1月23日—1961年11月25日）是英国钢琴家，克拉拉·舒曼的学生之一，以舒曼、勃拉姆斯等作品演奏闻名。",
    },
    sources: [{ label: "Wikidata: Adelina de Lara", url: "https://www.wikidata.org/wiki/Q15452779" }],
    notes: "Wikidata gives Adelina de Lara's life dates as 1872-1961.",
  },
  {
    entityType: "person",
    entityId: "person-丹尼斯瓦里翁",
    set: {
      name: "丹尼斯·瓦里翁",
      birthYear: 1968,
      aliases: ["丹尼斯.瓦里翁"],
      summary:
        "丹尼斯·瓦里翁（Dénes Várjon，1968年—）是匈牙利钢琴家，活跃于独奏与室内乐舞台，以德奥与中欧曲目诠释见长。",
    },
    sources: [{ label: "Kronberg Academy: Dénes Várjon", url: "https://www.kronbergacademy.de/en/person/denes-varjon" }],
    notes: "Kronberg Academy lists Dénes Várjon as born in 1968; the Chinese display name is normalized to use a middle dot.",
  },
  {
    entityType: "person",
    entityId: "person-山根美代子",
    set: {
      name: "山根弥生子",
      birthYear: 1933,
      aliases: ["山根美代子"],
      summary:
        "山根弥生子（Yaeko Yamane，1933年—）是日本钢琴家，长期活跃于独奏、室内乐与伴奏舞台。",
    },
    sources: [
      { label: "NDL Search: 山根弥生子", url: "https://ndlsearch.ndl.go.jp/en/books/R100000002-I033998214" },
      { label: "CDJournal: Yaeko Yamane", url: "https://artist.cdjournal.com/a/yamane-yaeko/152410" },
    ],
    notes: "Japanese sources identify the pianist as 山根弥生子 / Yaeko Yamane and list her as born in 1933.",
  },
  {
    entityType: "person",
    entityId: "person-让富尼埃-jean-fournier",
    set: {
      name: "让·富尼埃",
      birthYear: 1911,
      deathYear: 2003,
      aliases: ["让·富尼埃(Jean Fournier)"],
      summary:
        "让·富尼埃（Jean Fournier，1911年12月6日—2003年1月25日）是法国小提琴家，20世纪法国小提琴学派的重要代表人物之一。",
    },
    sources: [{ label: "Wikipedia: Jean Fournier", url: "https://en.wikipedia.org/wiki/Jean_Fournier_(violinist)" }],
    notes: "Jean Fournier's violinist entry gives life dates 1911-2003 and supports removing the parenthetical Latin-name duplication from the Chinese display name.",
  },
  {
    entityType: "person",
    entityId: "person-罗伯特卡萨德修",
    set: {
      birthYear: 1899,
      deathYear: 1972,
      summary:
        "罗伯特·卡萨德修（Robert Casadesus，1899年4月8日—1972年9月19日）是法国钢琴家、作曲家，以清晰克制的法式风格和莫扎特、拉威尔、德彪西诠释闻名。",
    },
    sources: [{ label: "Britannica: Robert Casadesus", url: "https://www.britannica.com/biography/Robert-Casadesus" }],
    notes: "Britannica gives Robert Casadesus's life dates as 1899-1972.",
  },
  {
    entityType: "person",
    entityId: "person-马克斯艾格",
    set: {
      birthYear: 1916,
      summary:
        "马克斯·艾格（Max Egger，1916年—2008年）是瑞士钢琴家与钢琴教育家，长期活跃于独奏、广播录音和音乐教育领域。",
      deathYear: 2008,
    },
    sources: [{ label: "Unsere Geschichte: Max Egger", url: "https://unseregeschichte.ch/entries/EyoKZAN6zl5" }],
    notes: "The Swiss biography and anniversary record support Max Egger's life dates as 1916-2008.",
  },
  {
    entityType: "person",
    entityId: "person-海因里希-霍尔莱瑟",
    set: {
      name: "海因里希·霍尔莱瑟",
      birthYear: 1913,
      deathYear: 2006,
      aliases: ["海因里希•霍尔莱瑟"],
      summary:
        "海因里希·霍尔莱瑟（Heinrich Hollreiser，1913年7月24日—2006年7月24日）是德国指挥家，长期活跃于德语歌剧和交响乐舞台，与巴伐利亚及德国南部乐坛联系密切。",
    },
    sources: [{ label: "Wikipedia: Heinrich Hollreiser", url: "https://en.wikipedia.org/wiki/Heinrich_Hollreiser" }],
    notes: "Wikipedia gives Heinrich Hollreiser's life dates as 1913-2006; the Chinese display name is normalized to use a middle dot.",
  },
  {
    entityType: "person",
    entityId: "person-康科迪-杰拉伯特",
    set: {
      name: "康科迪·杰拉伯特",
      birthYear: 1882,
      deathYear: 1944,
      aliases: ["康科迪・杰拉伯特"],
      roles: ["other"],
      summary:
        "康科迪·杰拉伯特（Concordi Gelabert，1882年1月28日—1944年4月21日）是加泰罗尼亚音乐评论家、教师和艺术推动者，活跃于巴塞罗那音乐界。",
    },
    sources: [{ label: "DadesCat: Concordi Gelabert i Alart", url: "https://dadescat.com/2020/03/14/gelabert-i-alart-concordi/" }],
    notes: "The Catalan biography identifies Concordi Gelabert as a critic, pedagogue and artistic figure rather than a formal conductor entry, so the person role is normalized to other while keeping recording credits intact.",
  },
  {
    entityType: "person",
    entityId: "person-皮埃尔巴尔比泽",
    set: {
      birthYear: 1922,
      deathYear: 1990,
      country: "France",
      countries: ["France", "Chile"],
      summary:
        "皮埃尔·巴尔比泽（Pierre Barbizet，1922年9月20日—1990年1月19日）是法国钢琴家和音乐教育家，生于智利阿里卡，长期在马赛学习、演出并主持当地音乐学院。",
    },
    sources: [{ label: "Wikipedia FR: Pierre Barbizet", url: "https://fr.wikipedia.org/wiki/Pierre_Barbizet" }],
    notes: "French-language sources identify Pierre Barbizet as a French pianist born in Chile, so the primary country is normalized to France with Chile retained as a secondary country.",
  },
  {
    entityType: "person",
    entityId: "person-弗里茨莱纳",
    set: {
      country: "United States",
      countries: ["United States", "Hungary"],
      summary:
        "弗里茨·莱纳（Fritz Reiner，1888年12月19日—1963年11月15日）是匈牙利出生、后归化美国的指挥家，长期活跃于美国乐坛，以芝加哥交响乐团时期的录音和严谨精准的指挥风格闻名。",
    },
    sources: [{ label: "Wikidata: Fritz Reiner", url: "https://www.wikidata.org/wiki/Q364179" }],
    notes: "The summary and country fields are aligned around Reiner's Hungarian birth and American conducting career.",
  },
  {
    entityType: "person",
    entityId: "person-汉斯克纳佩兹布什",
    set: {
      birthYear: 1888,
      deathYear: 1965,
      summary:
        "汉斯·克纳佩兹布什（Hans Knappertsbusch，1888年3月12日—1965年10月25日）是德国指挥家，以瓦格纳、布鲁克纳和德奥歌剧传统诠释闻名。",
    },
    sources: [{ label: "Britannica: Hans Knappertsbusch", url: "https://www.britannica.com/biography/Hans-Knappertsbusch" }],
    notes: "Britannica gives Hans Knappertsbusch's life dates as 1888-1965.",
  },
  {
    entityType: "person",
    entityId: "person-西奥多库伦齐斯",
    set: {
      birthYear: 1972,
      summary:
        "西奥多·库伦齐斯（Teodor Currentzis，1972年2月24日—）是希腊裔俄籍指挥家、音乐家，长期活跃于俄罗斯与欧洲乐坛，以强烈个性化的歌剧和交响诠释闻名。",
    },
    sources: [{ label: "Opera Online: Teodor Currentzis", url: "https://www.opera-online.com/en/items/personnalities/teodor-currentzis-1972" }],
    notes: "Opera Online lists Teodor Currentzis as born in 1972.",
  },
  {
    entityType: "person",
    entityId: "person-安特耶魏特哈斯",
    set: {
      birthYear: 1966,
      summary:
        "安特耶·魏特哈斯（Antje Weithaas，1966年—）是德国古典小提琴家，以独奏、室内乐和教学活动闻名。",
    },
    sources: [{ label: "Wikipedia: Antje Weithaas", url: "https://en.wikipedia.org/wiki/Antje_Weithaas" }],
    notes: "Reference biographies list Antje Weithaas as born in 1966.",
  },
  {
    entityType: "person",
    entityId: "person-拉度鲁普",
    set: {
      nameLatin: "Radu Lupu",
      birthYear: 1945,
      deathYear: 2022,
      summary:
        "拉度·鲁普（Radu Lupu，1945年11月30日—2022年4月17日）是罗马尼亚钢琴家，20世纪后半叶最受推崇的钢琴家之一，以舒伯特、勃拉姆斯、莫扎特和贝多芬诠释闻名。",
    },
    sources: [{ label: "Wikipedia: Radu Lupu", url: "https://en.wikipedia.org/wiki/Radu_Lupu" }],
    notes: "Wikipedia gives Radu Lupu's life dates as 1945-2022 and confirms the standard Latin spelling.",
  },
  {
    entityType: "person",
    entityId: "person-克里斯托夫佩里克",
    set: {
      birthYear: 1946,
      summary:
        "克里斯托夫·佩里克（Christof Prick，1946年—）是德国指挥家，在英语语境中亦常使用 Christof Perick 的拼写，曾任多家德国歌剧院和乐团音乐总监。",
    },
    sources: [
      { label: "Christof Prick Homepage", url: "https://www.christof-prick.de/" },
      { label: "Wikipedia: Christof Perick", url: "https://en.wikipedia.org/wiki/Christof_Perick" },
    ],
    notes: "Christof Prick / Perick reference biographies list him as born in 1946.",
  },
  {
    entityType: "person",
    entityId: "person-乔治赛尔",
    set: { roles: ["conductor"] },
    sources: localSummarySource(),
    notes: "The soloist role came from duplicate filename backfill credits and is normalized away.",
  },
  {
    entityType: "person",
    entityId: "person-伯纳德约翰赫尔曼海廷克",
    set: { roles: ["conductor"] },
    sources: localSummarySource(),
    notes: "The soloist role came from duplicate filename backfill credits and is normalized away.",
  },
  {
    entityType: "person",
    entityId: "person-卡尔伯姆",
    set: { roles: ["conductor"] },
    sources: localSummarySource(),
    notes: "The soloist role came from duplicate filename backfill credits and is normalized away.",
  },
  {
    entityType: "person",
    entityId: "person-卡罗马里亚朱里尼",
    set: { roles: ["conductor"] },
    sources: localSummarySource(),
    notes: "The soloist role came from duplicate filename backfill credits and is normalized away.",
  },
  {
    entityType: "person",
    entityId: "person-叶夫根尼亚历山德罗维奇穆拉文斯基",
    set: { roles: ["conductor"] },
    sources: localSummarySource(),
    notes: "The soloist role came from duplicate filename backfill credits and is normalized away.",
  },
  {
    entityType: "person",
    entityId: "person-威尔海姆富特文格勒",
    set: { roles: ["conductor"] },
    sources: localSummarySource(),
    notes: "The soloist role came from duplicate filename backfill credits and is normalized away.",
  },
  {
    entityType: "person",
    entityId: "person-布鲁诺瓦尔特",
    set: { roles: ["conductor"] },
    sources: localSummarySource(),
    notes: "The soloist role came from duplicate filename backfill credits and is normalized away.",
  },
  {
    entityType: "person",
    entityId: "person-弗朗茨威尔瑟-莫斯特",
    set: { roles: ["conductor"] },
    sources: localSummarySource(),
    notes: "The soloist role came from duplicate filename backfill credits and is normalized away.",
  },
  {
    entityType: "person",
    entityId: "person-德米特里米特罗普洛斯",
    set: { roles: ["conductor"] },
    sources: localSummarySource(),
    notes: "The soloist role came from duplicate filename backfill credits and is normalized away.",
  },
  {
    entityType: "person",
    entityId: "person-赫伯特冯卡拉扬",
    set: { roles: ["conductor"] },
    sources: localSummarySource(),
    notes: "The soloist role came from duplicate filename backfill credits and is normalized away.",
  },
  {
    entityType: "person",
    entityId: "person-赫伯特托尔松布隆斯泰特",
    set: { roles: ["conductor"] },
    sources: localSummarySource(),
    notes: "The soloist role came from duplicate filename backfill credits and is normalized away.",
  },
  {
    entityType: "person",
    entityId: "person-阿尔图罗托斯卡尼尼",
    set: { roles: ["conductor"] },
    sources: localSummarySource(),
    notes: "The soloist role came from duplicate filename backfill credits and is normalized away.",
  },
];

const reviewEntries = readJson(reviewPath);
const people = readJson(peoplePath);

let autoGeneratedCount = 0;

for (const person of people) {
  const roles = Array.isArray(person.roles) ? person.roles : [];
  if (roles.some((role) => groupRoles.has(role))) {
    continue;
  }

  const span = extractLifeSpanFromSummary(person.summary || "");
  const set = {};
  if (!person.birthYear && span.birthYear) {
    set.birthYear = span.birthYear;
  }
  if (!person.deathYear && span.deathYear) {
    set.deathYear = span.deathYear;
  }
  if (Object.keys(set).length === 0) {
    continue;
  }

  autoGeneratedCount += 1;
  upsertReviewEntry(reviewEntries, {
    entityType: "person",
    entityId: person.id,
    set,
    sources: localSummarySource(),
    notes: "Extracted from the current library summary via entity-vitals audit.",
  });
}

for (const entry of manualEntries) {
  upsertReviewEntry(reviewEntries, entry);
}

writeJson(reviewPath, reviewEntries);

console.log(
  JSON.stringify(
    {
      reviewPath,
      autoGeneratedCount,
      manualEntryCount: manualEntries.length,
      reviewCount: reviewEntries.length,
    },
    null,
    2,
  ),
);
