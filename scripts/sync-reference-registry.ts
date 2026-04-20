import { promises as fs } from "node:fs";

import { loadLibraryFromDisk } from "../packages/data-core/src/library-store.js";
import {
  buildOrchestraReferenceEntry,
  buildPersonReferenceEntry,
  consolidateOrchestraReferenceEntries,
  consolidatePersonReferenceEntries,
  findMatchingOrchestraReferenceEntries,
  findMatchingPersonReferenceEntries,
  getOrchestraReferenceDefaultPath,
  getPersonReferenceDefaultPath,
  mergeOrchestraReferenceEntries,
  mergePersonReferenceEntries,
  parseOrchestraReferenceText,
  parsePersonAliasReferenceText,
  type OrchestraReferenceEntry,
  type PersonReferenceEntry,
} from "../packages/data-core/src/reference-registry.js";
import type { Composer, Person } from "../packages/shared/src/schema.js";

function compact(value: unknown) {
  return String(value ?? "").trim();
}

function dedupeValues(values: Array<unknown>) {
  const seen = new Set<string>();
  const items: string[] = [];
  for (const value of values) {
    const normalized = compact(value);
    if (!normalized || seen.has(normalized)) {
      continue;
    }
    seen.add(normalized);
    items.push(normalized);
  }
  return items;
}

function looksLikeChineseText(value: string) {
  return /[\u3400-\u9fff]/u.test(value);
}

function looksLikeAbbreviation(value: string) {
  return /^[A-Z0-9][A-Z0-9 .&/-]{1,15}$/.test(compact(value));
}

function normalizeLookupKey(value: unknown) {
  return compact(value)
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^\p{Letter}\p{Number}\u3400-\u9fff]+/gu, "");
}

function mergeValueLists(...valueGroups: string[][]) {
  return dedupeValues(valueGroups.flat());
}

function sortChineseValues(values: string[], preferred: string) {
  void mergeValueLists;
  return dedupeValues([
    preferred,
    ...values.filter((value) => value !== preferred),
  ]).sort((left, right) => {
    if (left === preferred) return -1;
    if (right === preferred) return 1;
    return left.localeCompare(right, "zh-Hans-CN");
  });
}

function sortLatinValues(values: string[], preferred: string) {
  return dedupeValues([
    preferred,
    ...values.filter((value) => value !== preferred),
  ]).sort((left, right) => {
    if (left === preferred) return -1;
    if (right === preferred) return 1;
    return left.localeCompare(right, "en");
  });
}

function deriveOrchestraValues(person: Person) {
  const aliases = dedupeValues(person.aliases || []);
  const abbreviations = aliases.filter((value) => looksLikeAbbreviation(value));
  const chineseValues = dedupeValues([person.name, ...aliases.filter((value) => looksLikeChineseText(value))]);
  const latinValues = dedupeValues([person.nameLatin, ...aliases.filter((value) => !looksLikeChineseText(value) && !looksLikeAbbreviation(value))]);
  return {
    abbreviations,
    chineseValues,
    latinValues,
  };
}

function determinePersonSection(entity: Composer | Person, entityType: "composer" | "person") {
  if (entityType === "composer") {
    return "composer";
  }
  const roles = new Set((entity as Person).roles || []);
  if (roles.has("conductor")) {
    return "conductor";
  }
  if (roles.has("orchestra") || roles.has("ensemble") || roles.has("chorus")) {
    return "ensemble";
  }
  if (roles.has("soloist") || roles.has("instrumentalist") || roles.has("singer")) {
    return "soloist";
  }
  return "global";
}

function derivePersonValues(entity: Composer | Person) {
  const aliases = dedupeValues(entity.aliases || []);
  const chineseValues = dedupeValues([entity.name, ...aliases.filter((value) => looksLikeChineseText(value))]);
  const latinValues = dedupeValues([entity.nameLatin, ...aliases.filter((value) => !looksLikeChineseText(value))]);
  return {
    chineseValues,
    latinValues,
  };
}

function formatOrchestraEntry(entry: OrchestraReferenceEntry) {
  return dedupeValues([
    ...entry.abbreviations,
    ...sortChineseValues(entry.chineseValues, entry.preferredValue),
    ...sortLatinValues(entry.latinValues, entry.canonicalLatin),
  ]).join(" = ");
}

function formatPersonEntry(entry: PersonReferenceEntry) {
  return dedupeValues([
    ...sortChineseValues(entry.chineseValues, entry.preferredValue),
    ...sortLatinValues(entry.latinValues, entry.canonicalLatin),
  ]).join(" = ");
}

function buildOrchestraReferenceText(entries: OrchestraReferenceEntry[]) {
  const header = [
    "# 乐团名称对照表",
    "# 用法：",
    "# 1. 每行一个映射组，使用 = 连接缩写、中文译名、原文主名、原文别名等。",
    "# 2. 系统会双向读取：输入缩写、中文译名、原文别名，都可以回查规范名称。",
    "# 3. 推荐顺序：缩写 = 中文常用名 = 中文别名 = Latin/原文主名 = Latin/原文别名。",
    "",
  ];
  const body = [...entries]
    .sort((left, right) => left.preferredValue.localeCompare(right.preferredValue, "zh-Hans-CN"))
    .map(formatOrchestraEntry);
  return `${[...header, ...body].join("\n")}\n`;
}

function buildPersonReferenceText(entries: PersonReferenceEntry[]) {
  const sectionOrder = ["global", "composer", "conductor", "soloist", "pianist", "violinist", "soprano", "tenor", "baritone", "ensemble"];
  const header = [
    "# 人物姓名映射文档",
    "# 用法：",
    "# 1. 使用 #section-name 定义角色分组，例如 #global、#conductor、#soloist、#composer、#pianist。",
    "# 2. 每行一个映射组，使用 = 连接不同语言、不同译名、不同写法。",
    "# 3. 建议按“中文常用名 = 中文别名 = Latin/原文短名 = Latin/原文全名”填写。",
    "# 4. 系统会双向读取：输入中文可展开 Latin/原文，输入 Latin/原文也可回查中文或缩写。",
    "# 5. #global 中的映射适用于所有角色；角色分组中的映射会在对应角色里优先使用。",
    "",
  ];
  const groups = new Map<string, PersonReferenceEntry[]>();
  for (const role of sectionOrder) {
    groups.set(role, []);
  }
  for (const entry of entries) {
    const role = groups.has(entry.role) ? entry.role : "global";
    groups.get(role)?.push(entry);
  }

  const lines = [...header];
  for (const role of sectionOrder) {
    lines.push(`#${role}`);
    const roleEntries = [...(groups.get(role) || [])].sort((left, right) => left.preferredValue.localeCompare(right.preferredValue, "zh-Hans-CN"));
    for (const entry of roleEntries) {
      lines.push(formatPersonEntry(entry));
    }
    lines.push("");
  }
  return `${lines.join("\n").trimEnd()}\n`;
}

function sanitizeOrchestraEntryForTarget(entry: OrchestraReferenceEntry, target: OrchestraReferenceEntry, candidates: OrchestraReferenceEntry[]) {
  const targetKeys = new Set(target.values.map((value) => normalizeLookupKey(value)).filter(Boolean));
  const conflictingKeys = new Set(
    candidates
      .filter((candidate) => candidate !== target)
      .flatMap((candidate) => candidate.values.map((value) => normalizeLookupKey(value)).filter(Boolean)),
  );
  const sanitizedValues = dedupeValues(
    entry.values.filter((value) => {
      const key = normalizeLookupKey(value);
      if (!key) {
        return false;
      }
      if (targetKeys.has(key)) {
        return true;
      }
      return !conflictingKeys.has(key);
    }),
  );
  return buildOrchestraReferenceEntry(sanitizedValues.length ? sanitizedValues : target.values);
}

function sanitizePersonEntryForTarget(entry: PersonReferenceEntry, target: PersonReferenceEntry, candidates: PersonReferenceEntry[]) {
  const targetKeys = new Set(target.values.map((value) => normalizeLookupKey(value)).filter(Boolean));
  const conflictingKeys = new Set(
    candidates
      .filter((candidate) => candidate !== target)
      .flatMap((candidate) => candidate.values.map((value) => normalizeLookupKey(value)).filter(Boolean)),
  );
  const sanitizedValues = dedupeValues(
    entry.values.filter((value) => {
      const key = normalizeLookupKey(value);
      if (!key) {
        return false;
      }
      if (targetKeys.has(key)) {
        return true;
      }
      return !conflictingKeys.has(key);
    }),
  );
  return buildPersonReferenceEntry(target.role, sanitizedValues.length ? sanitizedValues : target.values);
}

async function main() {
  const library = await loadLibraryFromDisk();
  const orchestraPath = getOrchestraReferenceDefaultPath();
  const personPath = getPersonReferenceDefaultPath();
  const [existingOrchestraSource, existingPersonSource] = await Promise.all([
    fs.readFile(orchestraPath, "utf8").catch(() => ""),
    fs.readFile(personPath, "utf8").catch(() => ""),
  ]);

  const existingOrchestraEntries = parseOrchestraReferenceText(existingOrchestraSource);
  const ambiguousOrchestraEntries: OrchestraReferenceEntry[] = [];
  let orchestraEntries: OrchestraReferenceEntry[] = [];
  for (const person of library.people.filter((item) => item.roles.some((role) => ["orchestra", "ensemble", "chorus"].includes(role)))) {
    const derived = deriveOrchestraValues(person);
    orchestraEntries.push({
      preferredValue: derived.chineseValues[0] || derived.latinValues[0] || person.name,
      canonicalLatin: derived.latinValues[0] || "",
      values: dedupeValues([...derived.abbreviations, ...derived.chineseValues, ...derived.latinValues]),
      chineseValues: derived.chineseValues,
      latinValues: derived.latinValues,
      abbreviations: derived.abbreviations,
    });
  }
  for (const entry of existingOrchestraEntries) {
    const matches = findMatchingOrchestraReferenceEntries(entry, orchestraEntries);
    if (matches.length === 1) {
      const target = matches[0];
      const sanitizedEntry = sanitizeOrchestraEntryForTarget(entry, target, orchestraEntries);
      orchestraEntries = orchestraEntries.map((candidate) =>
        candidate === target ? mergeOrchestraReferenceEntries(candidate, sanitizedEntry) : candidate,
      );
      continue;
    }
    if (matches.length === 0) {
      orchestraEntries.push(entry);
      continue;
    }
    ambiguousOrchestraEntries.push(entry);
  }
  orchestraEntries = consolidateOrchestraReferenceEntries(orchestraEntries);

  const existingPersonEntries = parsePersonAliasReferenceText(existingPersonSource);
  const ambiguousPersonEntries: PersonReferenceEntry[] = [];
  let personEntries: PersonReferenceEntry[] = [];
  const derivedPeople: Array<{ role: string; values: { chineseValues: string[]; latinValues: string[] } }> = [
    ...library.composers.map((composer) => ({
      role: determinePersonSection(composer, "composer"),
      values: derivePersonValues(composer),
    })),
    ...library.people.map((person) => ({
      role: determinePersonSection(person, "person"),
      values: derivePersonValues(person),
    })).filter((entry) => entry.role !== "ensemble"),
  ];
  for (const derivedPerson of derivedPeople) {
    personEntries.push({
      role: derivedPerson.role,
      preferredValue: derivedPerson.values.chineseValues[0] || derivedPerson.values.latinValues[0] || "",
      canonicalLatin: derivedPerson.values.latinValues[0] || "",
      values: dedupeValues([...derivedPerson.values.chineseValues, ...derivedPerson.values.latinValues]),
      chineseValues: derivedPerson.values.chineseValues,
      latinValues: derivedPerson.values.latinValues,
    });
  }
  for (const entry of existingPersonEntries.filter((candidate) => candidate.role !== "ensemble")) {
    const matches = findMatchingPersonReferenceEntries(entry, personEntries);
    if (matches.length === 1) {
      const target = matches[0];
      const sanitizedEntry = sanitizePersonEntryForTarget(entry, target, personEntries);
      personEntries = personEntries.map((candidate) =>
        candidate === target ? mergePersonReferenceEntries(candidate, sanitizedEntry) : candidate,
      );
      continue;
    }
    if (matches.length === 0) {
      personEntries.push(entry);
      continue;
    }
    ambiguousPersonEntries.push(entry);
  }

  personEntries = consolidatePersonReferenceEntries(personEntries);
  personEntries.push(
    ...orchestraEntries.map<PersonReferenceEntry>((entry) => ({
      role: "ensemble",
      preferredValue: entry.preferredValue,
      canonicalLatin: entry.canonicalLatin,
      values: dedupeValues([...entry.chineseValues, ...entry.latinValues, ...entry.abbreviations]),
      chineseValues: entry.chineseValues,
      latinValues: dedupeValues([...entry.latinValues, ...entry.abbreviations]),
    })),
  );
  personEntries = consolidatePersonReferenceEntries(personEntries);

  await Promise.all([
    fs.writeFile(orchestraPath, buildOrchestraReferenceText(orchestraEntries), "utf8"),
    fs.writeFile(personPath, buildPersonReferenceText(personEntries), "utf8"),
  ]);

  console.log(
    JSON.stringify(
      {
        orchestraEntries: orchestraEntries.length,
        personEntries: personEntries.length,
        skippedAmbiguousOrchestraEntries: ambiguousOrchestraEntries.length,
        skippedAmbiguousPersonEntries: ambiguousPersonEntries.length,
        orchestraPath,
        personPath,
      },
      null,
      2,
    ),
  );
}

await main();
