import { promises as fs } from "node:fs";
import path from "node:path";

export type OrchestraReferenceEntry = {
  preferredValue: string;
  canonicalLatin: string;
  values: string[];
  chineseValues: string[];
  latinValues: string[];
  abbreviations: string[];
};

export type PersonReferenceEntry = {
  role: string;
  preferredValue: string;
  canonicalLatin: string;
  values: string[];
  chineseValues: string[];
  latinValues: string[];
};

export type ReferenceRegistry = {
  orchestraEntries: OrchestraReferenceEntry[];
  personEntries: PersonReferenceEntry[];
  orchestraLookup: Map<string, OrchestraReferenceEntry[]>;
  personLookup: Map<string, PersonReferenceEntry[]>;
};

export type ReferenceRegistryIssue = {
  code: "ambiguous_orchestra_abbreviation" | "duplicate_orchestra_identity" | "duplicate_person_identity";
  scope: "orchestra" | "person";
  lookupValue: string;
  role?: string;
  preferredValues: string[];
};

type BuildReferenceRegistryOptions = {
  orchestraSourceText?: string;
  personSourceText?: string;
  orchestraEntries?: OrchestraReferenceEntry[];
  personEntries?: PersonReferenceEntry[];
};

type LoadReferenceRegistryOptions = {
  orchestraPath?: string;
  personPath?: string;
};

const orchestraReferenceDefaultPath = path.join(process.cwd(), "materials", "references", "Orchestra Abbreviation Comparison.txt");
const personReferenceDefaultPath = path.join(process.cwd(), "materials", "references", "person-name-aliases.txt");

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

function splitReferenceValues(line: string) {
  return dedupeValues(
    line
      .split("=")
      .map((item) => compact(item))
      .filter(Boolean),
  );
}

function selectCanonicalLatin(values: string[], strategy: "first" | "longest" = "first") {
  const latinCandidates = values.filter((value) => !looksLikeChineseText(value) && !looksLikeAbbreviation(value));
  if (latinCandidates.length === 0) {
    return values.find((value) => !looksLikeChineseText(value)) || "";
  }
  if (strategy === "longest") {
    return [...latinCandidates].sort((left, right) => right.length - left.length)[0] || "";
  }
  return latinCandidates[0] || "";
}

export function buildOrchestraReferenceEntry(values: string[]): OrchestraReferenceEntry {
  const orderedValues = dedupeValues(values);
  const chineseValues = orderedValues.filter((value) => looksLikeChineseText(value));
  const abbreviations = orderedValues.filter((value) => looksLikeAbbreviation(value));
  const latinValues = orderedValues.filter((value) => !looksLikeChineseText(value) && !looksLikeAbbreviation(value));
  const canonicalLatin = selectCanonicalLatin(orderedValues, "first");
  return {
    preferredValue: chineseValues[0] || canonicalLatin || orderedValues[0] || "",
    canonicalLatin,
    values: orderedValues,
    chineseValues,
    latinValues,
    abbreviations,
  };
}

export function buildPersonReferenceEntry(role: string, values: string[]): PersonReferenceEntry {
  const orderedValues = dedupeValues(values);
  const chineseValues = orderedValues.filter((value) => looksLikeChineseText(value));
  const latinValues = orderedValues.filter((value) => !looksLikeChineseText(value));
  const canonicalLatin = selectCanonicalLatin(orderedValues, "longest");
  return {
    role: compact(role).toLowerCase() || "global",
    preferredValue: chineseValues[0] || canonicalLatin || orderedValues[0] || "",
    canonicalLatin,
    values: orderedValues,
    chineseValues,
    latinValues,
  };
}

function appendLookupEntry<T>(lookup: Map<string, T[]>, key: string, entry: T) {
  const bucket = lookup.get(key) ?? [];
  if (!bucket.includes(entry)) {
    bucket.push(entry);
  }
  lookup.set(key, bucket);
}

function buildOverlapKeySet(values: string[]) {
  return new Set(values.map((value) => normalizeLookupKey(value)).filter(Boolean));
}

function entriesOverlap(leftValues: string[], rightValues: string[]) {
  const leftKeys = buildOverlapKeySet(leftValues);
  for (const key of buildOverlapKeySet(rightValues)) {
    if (leftKeys.has(key)) {
      return true;
    }
  }
  return false;
}

function filterStrongOrchestraIdentityValues(entry: OrchestraReferenceEntry) {
  return entry.values.filter((value) => !looksLikeAbbreviation(value));
}

function orchestraEntriesMatch(left: OrchestraReferenceEntry, right: OrchestraReferenceEntry) {
  const samePreferred = normalizeLookupKey(left.preferredValue) && normalizeLookupKey(left.preferredValue) === normalizeLookupKey(right.preferredValue);
  const sameCanonicalLatin = normalizeLookupKey(left.canonicalLatin) && normalizeLookupKey(left.canonicalLatin) === normalizeLookupKey(right.canonicalLatin);
  return samePreferred || sameCanonicalLatin || entriesOverlap(filterStrongOrchestraIdentityValues(left), filterStrongOrchestraIdentityValues(right));
}

function personEntriesMatch(left: PersonReferenceEntry, right: PersonReferenceEntry) {
  if (left.role !== right.role) {
    return false;
  }
  const samePreferred = normalizeLookupKey(left.preferredValue) && normalizeLookupKey(left.preferredValue) === normalizeLookupKey(right.preferredValue);
  const sameCanonicalLatin = normalizeLookupKey(left.canonicalLatin) && normalizeLookupKey(left.canonicalLatin) === normalizeLookupKey(right.canonicalLatin);
  return samePreferred || sameCanonicalLatin || entriesOverlap(left.values, right.values);
}

function sortChineseValues(values: string[], preferred: string) {
  return dedupeValues([preferred, ...values.filter((value) => value !== preferred)]).sort((left, right) => {
    if (left === preferred) return -1;
    if (right === preferred) return 1;
    return left.localeCompare(right, "zh-Hans-CN");
  });
}

function sortLatinValues(values: string[], preferred: string) {
  return dedupeValues([preferred, ...values.filter((value) => value !== preferred)]).sort((left, right) => {
    if (left === preferred) return -1;
    if (right === preferred) return 1;
    return left.localeCompare(right, "en");
  });
}

export function mergeOrchestraReferenceEntries(primary: OrchestraReferenceEntry, secondary: OrchestraReferenceEntry): OrchestraReferenceEntry {
  const abbreviations = dedupeValues([...(primary.abbreviations || []), ...(secondary.abbreviations || [])]).sort((left, right) =>
    left.localeCompare(right, "en"),
  );
  const preferredValue = primary.preferredValue || secondary.preferredValue;
  const canonicalLatin = primary.canonicalLatin || secondary.canonicalLatin;
  const chineseValues = sortChineseValues(dedupeValues([...primary.chineseValues, ...secondary.chineseValues]), preferredValue);
  const latinValues = sortLatinValues(dedupeValues([...primary.latinValues, ...secondary.latinValues]), canonicalLatin);
  return {
    preferredValue,
    canonicalLatin,
    values: dedupeValues([...abbreviations, ...chineseValues, ...latinValues]),
    chineseValues,
    latinValues,
    abbreviations,
  };
}

export function mergePersonReferenceEntries(primary: PersonReferenceEntry, secondary: PersonReferenceEntry): PersonReferenceEntry {
  const preferredValue = primary.preferredValue || secondary.preferredValue;
  const canonicalLatin = primary.canonicalLatin || secondary.canonicalLatin;
  const chineseValues = sortChineseValues(dedupeValues([...primary.chineseValues, ...secondary.chineseValues]), preferredValue);
  const latinValues = sortLatinValues(dedupeValues([...primary.latinValues, ...secondary.latinValues]), canonicalLatin);
  return {
    role: primary.role,
    preferredValue,
    canonicalLatin,
    values: dedupeValues([...chineseValues, ...latinValues]),
    chineseValues,
    latinValues,
  };
}

export function findMatchingOrchestraReferenceEntries(entry: OrchestraReferenceEntry, candidates: OrchestraReferenceEntry[]) {
  return candidates.filter((candidate) => orchestraEntriesMatch(entry, candidate));
}

export function findUniqueOrchestraReferenceMergeTarget(entry: OrchestraReferenceEntry, candidates: OrchestraReferenceEntry[]) {
  const matches = findMatchingOrchestraReferenceEntries(entry, candidates);
  return matches.length === 1 ? matches[0] : null;
}

export function consolidateOrchestraReferenceEntries(entries: OrchestraReferenceEntry[]) {
  const pending = [...entries];
  const consolidated: OrchestraReferenceEntry[] = [];
  while (pending.length > 0) {
    const seed = pending.shift();
    if (!seed) {
      continue;
    }
    let merged = seed;
    let changed = true;
    while (changed) {
      changed = false;
      for (let index = pending.length - 1; index >= 0; index -= 1) {
        const candidate = pending[index];
        if (!candidate || !orchestraEntriesMatch(merged, candidate)) {
          continue;
        }
        merged = mergeOrchestraReferenceEntries(merged, candidate);
        pending.splice(index, 1);
        changed = true;
      }
    }
    consolidated.push(merged);
  }
  return consolidated;
}

export function findMatchingPersonReferenceEntries(entry: PersonReferenceEntry, candidates: PersonReferenceEntry[]) {
  return candidates.filter((candidate) => personEntriesMatch(entry, candidate));
}

export function findUniquePersonReferenceMergeTarget(entry: PersonReferenceEntry, candidates: PersonReferenceEntry[]) {
  const matches = findMatchingPersonReferenceEntries(entry, candidates);
  return matches.length === 1 ? matches[0] : null;
}

export function consolidatePersonReferenceEntries(entries: PersonReferenceEntry[]) {
  const pending = [...entries];
  const consolidated: PersonReferenceEntry[] = [];
  while (pending.length > 0) {
    const seed = pending.shift();
    if (!seed) {
      continue;
    }
    let merged = seed;
    let changed = true;
    while (changed) {
      changed = false;
      for (let index = pending.length - 1; index >= 0; index -= 1) {
        const candidate = pending[index];
        if (!candidate || !personEntriesMatch(merged, candidate)) {
          continue;
        }
        merged = mergePersonReferenceEntries(merged, candidate);
        pending.splice(index, 1);
        changed = true;
      }
    }
    consolidated.push(merged);
  }
  return consolidated;
}

export function auditReferenceRegistry(registry: ReferenceRegistry) {
  const issues: ReferenceRegistryIssue[] = [];
  const seenIssueKeys = new Set<string>();

  for (const entry of registry.orchestraEntries) {
    const matches = findMatchingOrchestraReferenceEntries(entry, registry.orchestraEntries).filter((candidate) => candidate !== entry);
    if (matches.length === 0) {
      continue;
    }
    const preferredValues = dedupeValues([entry.preferredValue, ...matches.map((candidate) => candidate.preferredValue)]);
    const issueKey = `duplicate_orchestra_identity::${preferredValues.join("::")}`;
    if (seenIssueKeys.has(issueKey)) {
      continue;
    }
    seenIssueKeys.add(issueKey);
    issues.push({
      code: "duplicate_orchestra_identity",
      scope: "orchestra",
      lookupValue: entry.canonicalLatin || entry.preferredValue,
      preferredValues,
    });
  }

  for (const entry of registry.personEntries) {
    const matches = findMatchingPersonReferenceEntries(entry, registry.personEntries).filter((candidate) => candidate !== entry);
    if (matches.length === 0) {
      continue;
    }
    const preferredValues = dedupeValues([entry.preferredValue, ...matches.map((candidate) => candidate.preferredValue)]);
    const issueKey = `duplicate_person_identity::${entry.role}::${preferredValues.join("::")}`;
    if (seenIssueKeys.has(issueKey)) {
      continue;
    }
    seenIssueKeys.add(issueKey);
    issues.push({
      code: "duplicate_person_identity",
      scope: "person",
      lookupValue: entry.canonicalLatin || entry.preferredValue,
      role: entry.role,
      preferredValues,
    });
  }

  for (const entry of registry.orchestraEntries) {
    for (const abbreviation of entry.abbreviations) {
      const matches = lookupOrchestraReferences(registry, abbreviation);
      const uniqueMatches = dedupeValues(matches.map((candidate) => `${candidate.preferredValue}::${candidate.canonicalLatin}`));
      if (uniqueMatches.length <= 1) {
        continue;
      }
      const preferredValues = dedupeValues(matches.map((candidate) => candidate.preferredValue));
      const issueKey = `ambiguous_orchestra_abbreviation::${normalizeLookupKey(abbreviation)}::${preferredValues.join("::")}`;
      if (seenIssueKeys.has(issueKey)) {
        continue;
      }
      seenIssueKeys.add(issueKey);
      issues.push({
        code: "ambiguous_orchestra_abbreviation",
        scope: "orchestra",
        lookupValue: abbreviation,
        preferredValues,
      });
    }
  }

  return issues;
}

export function parseOrchestraReferenceText(sourceText: string) {
  const entries: OrchestraReferenceEntry[] = [];
  for (const rawLine of String(sourceText ?? "").split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith("#")) {
      continue;
    }
    const values = splitReferenceValues(line);
    if (values.length === 0) {
      continue;
    }
    entries.push(buildOrchestraReferenceEntry(values));
  }
  return entries;
}

export function parsePersonAliasReferenceText(sourceText: string) {
  const entries: PersonReferenceEntry[] = [];
  let currentRole = "global";
  for (const rawLine of String(sourceText ?? "").split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line) {
      continue;
    }
    if (line.startsWith("#")) {
      if (/^#[A-Za-z][\w-]*$/.test(line)) {
        currentRole = line.slice(1).toLowerCase();
      }
      continue;
    }
    const values = splitReferenceValues(line);
    if (values.length === 0) {
      continue;
    }
    entries.push(buildPersonReferenceEntry(currentRole, values));
  }
  return entries;
}

export function buildReferenceRegistry(options: BuildReferenceRegistryOptions = {}): ReferenceRegistry {
  const orchestraEntries = options.orchestraEntries ?? parseOrchestraReferenceText(options.orchestraSourceText || "");
  const personEntries = options.personEntries ?? parsePersonAliasReferenceText(options.personSourceText || "");
  const orchestraLookup = new Map<string, OrchestraReferenceEntry[]>();
  const personLookup = new Map<string, PersonReferenceEntry[]>();

  for (const entry of orchestraEntries) {
    for (const value of entry.values) {
      const key = normalizeLookupKey(value);
      if (!key) {
        continue;
      }
      appendLookupEntry(orchestraLookup, key, entry);
    }
  }

  for (const entry of personEntries) {
    for (const value of entry.values) {
      const key = normalizeLookupKey(value);
      if (!key) {
        continue;
      }
      appendLookupEntry(personLookup, key, entry);
    }
  }

  return {
    orchestraEntries,
    personEntries,
    orchestraLookup,
    personLookup,
  };
}

export function lookupOrchestraReference(registry: ReferenceRegistry, value: string) {
  const matches = registry.orchestraLookup.get(normalizeLookupKey(value)) ?? [];
  if (matches.length === 0) {
    return null;
  }
  const uniqueMatches = [...new Map(matches.map((entry) => [`${entry.preferredValue}::${entry.canonicalLatin}`, entry])).values()];
  return uniqueMatches.length === 1 ? uniqueMatches[0] : null;
}

export function lookupOrchestraReferences(registry: ReferenceRegistry, value: string) {
  return registry.orchestraLookup.get(normalizeLookupKey(value)) ?? [];
}

export function lookupPersonReference(registry: ReferenceRegistry, value: string, role?: string | string[]) {
  const entries = registry.personLookup.get(normalizeLookupKey(value)) ?? [];
  if (entries.length === 0) {
    return null;
  }

  const requestedRoles = dedupeValues(Array.isArray(role) ? role : role ? [role] : []).map((item) => item.toLowerCase());
  for (const requestedRole of requestedRoles) {
    const matched = entries.find((entry) => entry.role === requestedRole);
    if (matched) {
      return matched;
    }
  }

  const globalEntry = entries.find((entry) => entry.role === "global");
  if (globalEntry) {
    return globalEntry;
  }

  return entries[0] || null;
}

async function readOptionalTextFile(filePath: string) {
  try {
    return await fs.readFile(filePath, "utf8");
  } catch {
    return "";
  }
}

export async function loadReferenceRegistry(options: LoadReferenceRegistryOptions = {}) {
  const [orchestraSourceText, personSourceText] = await Promise.all([
    readOptionalTextFile(options.orchestraPath || orchestraReferenceDefaultPath),
    readOptionalTextFile(options.personPath || personReferenceDefaultPath),
  ]);
  return buildReferenceRegistry({ orchestraSourceText, personSourceText });
}

export function getOrchestraReferenceDefaultPath() {
  return orchestraReferenceDefaultPath;
}

export function getPersonReferenceDefaultPath() {
  return personReferenceDefaultPath;
}
