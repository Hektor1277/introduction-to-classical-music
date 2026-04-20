import type { Composer, LibraryData, Person } from "../../shared/src/schema.js";
import { getCountryText, getCountryValues } from "../../shared/src/display.js";

export type EntityVitalsAuditIssueCode =
  | "birthyear-missing"
  | "deathyear-missing"
  | "invalid-life-span"
  | "summary-birthyear-conflict"
  | "summary-deathyear-conflict"
  | "summary-country-conflict"
  | "group-has-life-span"
  | "country-missing";

export type EntityVitalsAuditIssue = {
  code: EntityVitalsAuditIssueCode;
  entityType: "composer" | "person";
  entityId: string;
  name: string;
  message: string;
  details?: string[];
};

export type EntityVitalsAuditSummary = {
  totalIssues: number;
  byCode: Record<EntityVitalsAuditIssueCode, number>;
  byEntityType: Record<"composer" | "person", number>;
};

export type EntityVitalsAuditResult = {
  summary: EntityVitalsAuditSummary;
  issues: EntityVitalsAuditIssue[];
};

type NamedEntity = Composer | Person;

type CountryPattern = {
  value: string;
  english: string[];
  chinese: string[];
};

const countryPatterns: CountryPattern[] = [
  { value: "Austria", english: ["Austria", "Austrian"], chinese: ["奥地利", "奥地利人"] },
  { value: "Germany", english: ["Germany", "German"], chinese: ["德国", "德国人"] },
  { value: "France", english: ["France", "French"], chinese: ["法国", "法国人"] },
  { value: "Finland", english: ["Finland", "Finnish"], chinese: ["芬兰", "芬兰人"] },
  { value: "Russia", english: ["Russia", "Russian"], chinese: ["俄罗斯", "俄国", "俄国人", "俄罗斯人"] },
  { value: "Hungary", english: ["Hungary", "Hungarian"], chinese: ["匈牙利", "匈牙利人"] },
  { value: "Czech Republic", english: ["Czech Republic", "Czech"], chinese: ["捷克", "捷克人"] },
  { value: "Netherlands", english: ["Netherlands", "Dutch"], chinese: ["荷兰", "荷兰人"] },
  { value: "Italy", english: ["Italy", "Italian"], chinese: ["意大利", "意大利人"] },
  { value: "Sweden", english: ["Sweden", "Swedish"], chinese: ["瑞典", "瑞典人"] },
  { value: "United Kingdom", english: ["United Kingdom", "British", "English"], chinese: ["英国", "英格兰", "英国人", "英格兰人"] },
  { value: "United States", english: ["United States", "American"], chinese: ["美国", "美国人"] },
  { value: "China", english: ["China", "Chinese"], chinese: ["中国", "中国人"] },
  { value: "Japan", english: ["Japan", "Japanese"], chinese: ["日本", "日本人"] },
  { value: "India", english: ["India", "Indian"], chinese: ["印度", "印度人"] },
  { value: "Israel", english: ["Israel", "Israeli"], chinese: ["以色列", "以色列人"] },
  { value: "Poland", english: ["Poland", "Polish"], chinese: ["波兰", "波兰人"] },
  { value: "Spain", english: ["Spain", "Spanish"], chinese: ["西班牙", "西班牙人"] },
  { value: "Switzerland", english: ["Switzerland", "Swiss"], chinese: ["瑞士", "瑞士人"] },
  { value: "Chile", english: ["Chile", "Chilean"], chinese: ["智利", "智利人"] },
  { value: "Croatia", english: ["Croatia", "Croatian"], chinese: ["克罗地亚", "克罗地亚人"] },
  { value: "Argentina", english: ["Argentina", "Argentinian", "Argentine"], chinese: ["阿根廷", "阿根廷人"] },
  { value: "Georgia", english: ["Georgia", "Georgian"], chinese: ["格鲁吉亚", "格鲁吉亚人"] },
  { value: "Soviet Union", english: ["Soviet Union", "Soviet"], chinese: ["苏联", "前苏联"] },
  { value: "International", english: ["International"], chinese: ["国际"] },
];

const professionMarkers = [
  "作曲家",
  "指挥家",
  "钢琴家",
  "小提琴家",
  "中提琴家",
  "大提琴家",
  "男高音",
  "女高音",
  "演奏家",
  "音乐家",
  "歌唱家",
  "歌手",
  "乐团",
  "管弦乐团",
  "交响乐团",
  "合唱团",
  "乐队",
  "ensemble",
  "orchestra",
  "conductor",
  "composer",
  "pianist",
  "violinist",
  "violist",
  "cellist",
  "musician",
];

function compact(value: unknown) {
  return String(value ?? "").trim();
}

function normalizeWhitespace(value: string) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function extractSummaryIdentityWindow(value: string) {
  const text = normalizeSummary(value);
  const leadingSentence = text.split(/[銆傦紒锛?!?;.!?]/, 1)[0] ?? text;
  return leadingSentence.slice(0, 180);
}

function normalizeSummary(value: string) {
  void extractSummaryIdentityWindow;
  return normalizeWhitespace(value)
    .replace(/[—–－﹣]/g, "-")
    .replace(/[（]/g, "(")
    .replace(/[）]/g, ")");
}

function extractSummaryIdentityPrefix(value: string) {
  return normalizeSummary(value).slice(0, 120);
}

export function extractLifeSpanFromSummary(value: string) {
  const text = normalizeSummary(value);
  const leadingText = text.split(/[。.!?]/, 1)[0] ?? text;
  const parentheticalText = text.match(/\(([^()]*)\)/)?.[1] ?? "";

  const findSpanYears = (segment: string) => {
    const matches = Array.from(segment.matchAll(/\b(1[6-9]\d{2}|20\d{2})\b/g));
    if (matches.length < 2) {
      return null;
    }

    for (let index = 0; index < matches.length - 1; index += 1) {
      const current = matches[index];
      const next = matches[index + 1];
      const between = segment.slice((current.index ?? 0) + current[0].length, next.index ?? 0);
      if ((current.index ?? 999) > 64) {
        continue;
      }
      if (!/[-–—－~至]/.test(between)) {
        continue;
      }
      return {
        birthYear: Number(current[1]),
        deathYear: Number(next[1]),
      };
    }

    return null;
  };

  const spanMatch = findSpanYears(parentheticalText) ?? findSpanYears(leadingText.slice(0, 96));
  if (spanMatch) {
    return {
      birthYear: spanMatch.birthYear,
      deathYear: spanMatch.deathYear,
    };
  }

  const birthYear =
    Number(text.match(/(?:出生于|生于|诞生于|诞生在|born[^0-9]{0,8})(1[6-9]\d{2}|20\d{2})/i)?.[1]) ||
    Number(text.match(/(1[6-9]\d{2}|20\d{2})\s*年[^。；;]{0,24}(?:出生|生于|诞生)/)?.[1]) ||
    Number(leadingText.match(/^(?:[^0-9]{0,8})?(1[6-9]\d{2}|20\d{2})\s*年[^。；;]{0,12}(?:出生|生于|诞生)/)?.[1]) ||
    undefined;
  const deathYear =
    Number(text.match(/(?:卒于|逝世于|去世于|died[^0-9]{0,8})(1[6-9]\d{2}|20\d{2})/i)?.[1]) ||
    Number(text.match(/(1[6-9]\d{2}|20\d{2})\s*年[^。；;]{0,24}(?:去世|逝世|辞世|病逝)/)?.[1]) ||
    undefined;

  return { birthYear, deathYear };
}

function extractSummaryCountryCandidates(value: string) {
  const text = extractSummaryIdentityPrefix(value);
  const candidates = new Set<string>();
  const compoundPrefixes = [
    { pattern: /苏联俄罗斯(?:作曲家|指挥家|钢琴家|小提琴家|中提琴家|大提琴家|男高音|女高音|演奏家|音乐家|歌唱家|歌手)/, values: ["Soviet Union", "Russia"] },
  ];

  for (const item of compoundPrefixes) {
    if (item.pattern.test(text)) {
      for (const value of item.values) {
        candidates.add(value);
      }
    }
  }

  for (const item of countryPatterns) {
    const chinesePattern = new RegExp(`(?:${item.chinese.join("|")})(?:裔|籍)?(?:${professionMarkers.join("|")})`);
    const englishPattern = new RegExp(`\\b(?:${item.english.join("|")})\\b(?:[-\\s]+(?:born|based))?[-\\s]+(?:${professionMarkers.join("|")})`, "i");

    if (chinesePattern.test(text) || englishPattern.test(text)) {
      candidates.add(item.value);
    }
  }

  return Array.from(candidates);
}

function isGroupEntity(entity: NamedEntity) {
  return "roles" in entity && entity.roles.some((role) => role === "orchestra" || role === "ensemble" || role === "chorus");
}

function issue(code: EntityVitalsAuditIssueCode, entityType: "composer" | "person", entity: NamedEntity, message: string, details?: string[]) {
  return {
    code,
    entityType,
    entityId: entity.id,
    name: entity.name,
    message,
    details,
  } satisfies EntityVitalsAuditIssue;
}

function auditEntity(entity: NamedEntity, entityType: "composer" | "person") {
  const issues: EntityVitalsAuditIssue[] = [];
  const summaryLifeSpan = extractLifeSpanFromSummary(entity.summary || "");
  const summaryCountryCandidates = extractSummaryCountryCandidates(entity.summary || "");
  const summaryCountry = summaryCountryCandidates.length === 1 ? summaryCountryCandidates[0] : "";
  const countryValues = getCountryValues(entity);
  const details = [entity.nameLatin, getCountryText(entity), entity.birthYear, entity.deathYear].filter(Boolean).map((value) => String(value));
  const isGroup = isGroupEntity(entity);

  if (!isGroup && !entity.birthYear) {
    issues.push(issue("birthyear-missing", entityType, entity, `${entity.name} 缺少 birthYear`, details));
  }

  if (!isGroup && summaryLifeSpan.deathYear && !entity.deathYear) {
    issues.push(issue("deathyear-missing", entityType, entity, `${entity.name} 缺少 deathYear`, [compact(entity.summary).slice(0, 160)]));
  }

  if (entity.birthYear && entity.deathYear && entity.birthYear > entity.deathYear) {
    issues.push(
      issue(
        "invalid-life-span",
        entityType,
        entity,
        `${entity.name} 的生卒年顺序冲突：${entity.birthYear} > ${entity.deathYear}`,
        details,
      ),
    );
  }

  if (summaryLifeSpan.birthYear && entity.birthYear && summaryLifeSpan.birthYear !== entity.birthYear) {
    issues.push(
      issue(
        "summary-birthyear-conflict",
        entityType,
        entity,
        `${entity.name} 的 birthYear=${entity.birthYear} 与摘要中的 ${summaryLifeSpan.birthYear} 不一致`,
        [compact(entity.summary).slice(0, 160)],
      ),
    );
  }

  if (summaryLifeSpan.deathYear && entity.deathYear && summaryLifeSpan.deathYear !== entity.deathYear) {
    issues.push(
      issue(
        "summary-deathyear-conflict",
        entityType,
        entity,
        `${entity.name} 的 deathYear=${entity.deathYear} 与摘要中的 ${summaryLifeSpan.deathYear} 不一致`,
        [compact(entity.summary).slice(0, 160)],
      ),
    );
  }

  if (summaryCountry && countryValues.length && !countryValues.includes(summaryCountry)) {
    issues.push(
      issue(
        "summary-country-conflict",
        entityType,
        entity,
        `${entity.name} 的 country=${getCountryText(entity)} 与摘要识别出的 ${summaryCountry} 不一致`,
        [compact(entity.summary).slice(0, 160)],
      ),
    );
  }

  if (!countryValues.length) {
    issues.push(issue("country-missing", entityType, entity, `${entity.name} 缺少 country`, details));
  }

  if (isGroup && (entity.birthYear || entity.deathYear)) {
    issues.push(
      issue(
        "group-has-life-span",
        entityType,
        entity,
        `${entity.name} 是团体条目，但仍写入了生卒年字段`,
        details,
      ),
    );
  }

  return issues;
}

export function auditEntityVitals(library: LibraryData): EntityVitalsAuditResult {
  const issues = [
    ...library.composers.flatMap((composer) => auditEntity(composer, "composer")),
    ...library.people.flatMap((person) => auditEntity(person, "person")),
  ];

  const byCode = {
    "birthyear-missing": 0,
    "deathyear-missing": 0,
    "invalid-life-span": 0,
    "summary-birthyear-conflict": 0,
    "summary-deathyear-conflict": 0,
    "summary-country-conflict": 0,
    "group-has-life-span": 0,
    "country-missing": 0,
  } satisfies Record<EntityVitalsAuditIssueCode, number>;
  const byEntityType = {
    composer: 0,
    person: 0,
  } satisfies Record<"composer" | "person", number>;

  for (const item of issues) {
    byCode[item.code] += 1;
    byEntityType[item.entityType] += 1;
  }

  return {
    summary: {
      totalIssues: issues.length,
      byCode,
      byEntityType,
    },
    issues,
  };
}
