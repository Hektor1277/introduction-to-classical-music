import { createEntityId, createSlug, createSortKey } from "../../shared/src/slug.js";
import type { Credit, LibraryData, Person, PersonRole, Recording } from "../../shared/src/schema.js";

function compact(value: unknown) {
  return String(value ?? "").trim();
}

function isFileNameBackfillLabel(value: unknown) {
  return compact(value).includes("文件名补");
}

function normalizeNameKey(value: unknown) {
  return compact(value)
    .normalize("NFKC")
    .toLowerCase()
    .replace(/[\s'"`"鈥溾€濃€樷€?,;:!?()[\]{}\-_/\\|&]+/g, "");
}

export function isPlaceholderValue(value: unknown) {
  const normalized = normalizeNameKey(value);
  return !normalized || normalized === "-" || normalized === "unknown" || normalized === "未知" || normalized === "未填写";
}

function expandSearchCandidates(value: string) {
  const original = compact(value);
  const withoutParentheses = compact(original.replace(/\([^)]*\)/g, " "));
  const parentheticalMatches = [...original.matchAll(/\(([^)]+)\)/g)].map((match) => compact(match[1]));
  const sanitized = [original, withoutParentheses, ...parentheticalMatches]
    .map((candidate) => compact(candidate).replace(/\bcurrently\b/gi, " ").replace(/\s+/g, " ").trim())
    .filter(Boolean);
  return [...new Set(sanitized)];
}

export function isPlaceholderPerson(person: Pick<Person, "id" | "name">) {
  return compact(person.id) === "person-item" || isPlaceholderValue(person.name);
}

function dedupeCredits(credits: Recording["credits"]) {
  const seen = new Set<string>();
  const nextCredits: Recording["credits"] = [];
  for (const credit of credits) {
    const key = [compact(credit.role), compact(credit.personId), compact(credit.displayName)].join("::");
    if (!key || seen.has(key)) {
      continue;
    }
    seen.add(key);
    nextCredits.push({
      ...credit,
      personId: compact(credit.personId),
      displayName: compact(credit.displayName),
      label: compact(credit.label),
    });
  }
  return nextCredits;
}

function personMatchesName(person: Person, value: string) {
  const targets = [...new Set(expandSearchCandidates(value).map((candidate) => normalizeNameKey(candidate)).filter(Boolean))];
  if (targets.length === 0) {
    return false;
  }
  const candidates = [
    person.name,
    person.fullName,
    person.displayName,
    person.displayFullName,
    person.nameLatin,
    person.displayLatinName,
    ...(person.aliases || []),
  ];
  return candidates.some((candidate) => targets.includes(normalizeNameKey(candidate)));
}

const ORCHESTRA_HINTS = [
  "orchestra",
  "philharmonic",
  "philharmoniker",
  "symphony",
  "sinfonieorchester",
  "orkester",
  "orquesta",
  "orchestre",
  "kapelle",
  "zenekara",
  "乐团",
  "乐队",
];

const CHORUS_HINTS = ["chorus", "choir", "合唱"];
const ENSEMBLE_HINTS = ["ensemble", "quartet", "trio", "重奏", "组合"];

function classifyEnsemblePart(value: string): Credit["role"] | null {
  const normalized = compact(value).toLowerCase();
  if (!normalized) {
    return null;
  }
  if (CHORUS_HINTS.some((hint) => normalized.includes(hint))) {
    return "chorus";
  }
  if (ORCHESTRA_HINTS.some((hint) => normalized.includes(hint))) {
    return "orchestra";
  }
  if (ENSEMBLE_HINTS.some((hint) => normalized.includes(hint))) {
    return "ensemble";
  }
  return null;
}

function deriveSharedEnsemblePrefix(value: string) {
  const normalized = compact(value);
  return compact(
    normalized.replace(
      /\s+(?:orchestra|philharmonic(?:\s+orchestra)?|symphony(?:\s+orchestra)?|sinfonieorchester|philharmoniker|orkester|orquesta|orchestre|kapelle|zenekara|乐团|乐队)$/i,
      "",
    ),
  );
}

function expandGenericEnsemblePart(value: string, siblingParts: string[]) {
  const normalized = compact(value);
  if (!/^(chorus|choir|合唱)$/i.test(normalized)) {
    return normalized;
  }
  const orchestraPart = siblingParts.find((part) => classifyEnsemblePart(part) === "orchestra");
  const prefix = orchestraPart ? deriveSharedEnsemblePrefix(orchestraPart) : "";
  return prefix ? `${prefix} ${normalized}` : normalized;
}

function splitCompositeEnsembleCredit(credit: Credit): Credit[] {
  if (!isEnsembleRole(credit.role)) {
    return [credit];
  }

  const displayName = compact(credit.displayName);
  if (!displayName || !/[\/&]/.test(displayName)) {
    return [credit];
  }

  const rawParts = displayName
    .split(/\s*(?:\/|&)\s*/g)
    .map((part) => compact(part))
    .filter(Boolean);
  if (rawParts.length < 2) {
    return [credit];
  }

  const expandedParts = rawParts.map((part) => expandGenericEnsemblePart(part, rawParts));
  const splitCredits = expandedParts.map((part) => {
    const role = classifyEnsemblePart(part);
    if (!role) {
      return null;
    }
    return {
      ...credit,
      role,
      personId: "",
      displayName: part,
      label: compact(credit.label) ? `${compact(credit.label)}拆分` : "复合署名拆分",
    } satisfies Credit;
  });

  if (splitCredits.some((entry) => entry === null)) {
    return [credit];
  }

  return splitCredits as Credit[];
}

function isSuspiciousCompositeEnsemblePerson(person: Person) {
  if (!person.roles.some((role) => isEnsembleRole(role))) {
    return false;
  }
  const name = compact(person.name);
  return /(?:\s+\/\s+|\s+&\s+|\bcurrently\b|\([^)]+\))/.test(name) || /\b[A-Z]{2,5}\b(?:\s*&\s*\b[A-Z]{1,5}\b)+/.test(name);
}

const pollutedGroupSlugMarkerPattern = /(时间|地点|currently|current|chn)/i;

function stripPollutedGroupMarkers(value: string) {
  return compact(value)
    .replace(pollutedGroupSlugMarkerPattern, " ")
    .replace(/\b\d{4}\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeGroupIdentityKey(value: unknown) {
  return normalizeNameKey(stripPollutedGroupMarkers(compact(value)));
}

function isWeakGroupIdentityValue(value: unknown) {
  const original = compact(value);
  const normalized = normalizeGroupIdentityKey(original);
  if (!normalized) {
    return true;
  }
  return /^[A-Z0-9]{2,6}$/i.test(original) || normalized.length <= 4;
}

function levenshteinDistance(left: string, right: string) {
  if (left === right) {
    return 0;
  }
  if (!left) {
    return right.length;
  }
  if (!right) {
    return left.length;
  }

  const previous = Array.from({ length: right.length + 1 }, (_, index) => index);
  const current = new Array(right.length + 1).fill(0);

  for (let leftIndex = 1; leftIndex <= left.length; leftIndex += 1) {
    current[0] = leftIndex;
    for (let rightIndex = 1; rightIndex <= right.length; rightIndex += 1) {
      const cost = left[leftIndex - 1] === right[rightIndex - 1] ? 0 : 1;
      current[rightIndex] = Math.min(
        current[rightIndex - 1] + 1,
        previous[rightIndex] + 1,
        previous[rightIndex - 1] + cost,
      );
    }
    for (let rightIndex = 0; rightIndex <= right.length; rightIndex += 1) {
      previous[rightIndex] = current[rightIndex];
    }
  }

  return previous[right.length];
}

function hasNearDuplicateIdentity(leftValues: string[], rightValues: string[]) {
  for (const left of leftValues) {
    for (const right of rightValues) {
      if (!left || !right) {
        continue;
      }
      if (left === right) {
        return true;
      }
      if (Math.min(left.length, right.length) < 12) {
        continue;
      }
      if (Math.abs(left.length - right.length) > 2) {
        continue;
      }
      const commonPrefixLength = (() => {
        let index = 0;
        while (index < left.length && index < right.length && left[index] === right[index]) {
          index += 1;
        }
        return index;
      })();
      if (commonPrefixLength < 8 && !left.includes(right) && !right.includes(left)) {
        continue;
      }
      if (levenshteinDistance(left, right) <= 2) {
        return true;
      }
    }
  }
  return false;
}

export function isPollutedGroupIdentity(person: Person) {
  if (!person.roles.some((role) => isEnsembleRole(role))) {
    return false;
  }
  return pollutedGroupSlugMarkerPattern.test(compact(person.slug));
}

function isEnsembleRole(role: Credit["role"] | PersonRole) {
  return role === "orchestra" || role === "ensemble" || role === "chorus";
}

function ensembleCompatible(role: Credit["role"], person: Person) {
  const personRoles = new Set(person.roles || []);
  if (role === "orchestra") {
    return personRoles.has("orchestra") || personRoles.has("ensemble") || personRoles.has("chorus");
  }
  if (role === "ensemble") {
    return personRoles.has("ensemble") || personRoles.has("orchestra") || personRoles.has("chorus");
  }
  if (role === "chorus") {
    return personRoles.has("chorus") || personRoles.has("ensemble");
  }
  return true;
}

function creditRoleCompatibleWithPerson(role: Credit["role"], person: Person) {
  if (isEnsembleRole(role)) {
    return ensembleCompatible(role, person);
  }
  const personRoles = new Set(person.roles || []);
  if (role === "conductor") {
    return personRoles.has("conductor");
  }
  if (role === "singer") {
    return personRoles.has("singer");
  }
  if (role === "soloist" || role === "instrumentalist") {
    return personRoles.has("soloist") || personRoles.has("instrumentalist");
  }
  return true;
}

function personQualityScore(person: Person) {
  let score = 0;
  const aliases = person.aliases || [];
  if (compact(person.country)) {
    score += 3;
  }
  if (compact(person.summary)) {
    score += 4;
  }
  if (compact(person.avatarSrc)) {
    score += 2;
  }
  if (compact(person.imageSourceUrl)) {
    score += 2;
  }
  if (compact(person.imageAttribution)) {
    score += 1;
  }
  if (aliases.length) {
    score += Math.min(aliases.length, 6);
  }
  if (compact(person.nameLatin)) {
    score += 1;
  }
  if (compact(person.name) && compact(person.nameLatin) && normalizeNameKey(person.name) !== normalizeNameKey(person.nameLatin)) {
    score += 2;
  }
  if (compact(person.fullName) || compact(person.displayName) || compact(person.displayFullName) || compact(person.displayLatinName)) {
    score += 1;
  }
  return score;
}

function personMatchScore(person: Person, target: string, options?: { allowPartial?: boolean; minPartialLength?: number }) {
  const normalizedTarget = normalizeNameKey(target);
  if (!normalizedTarget) {
    return 0;
  }
  const allowPartial = options?.allowPartial ?? true;
  const minPartialLength = options?.minPartialLength ?? 5;
  let bestScore = 0;
  for (const candidate of [
    person.name,
    person.fullName,
    person.displayName,
    person.displayFullName,
    person.nameLatin,
    person.displayLatinName,
    ...(person.aliases || []),
  ]) {
    const normalizedCandidate = normalizeNameKey(candidate);
    if (!normalizedCandidate) {
      continue;
    }
    if (normalizedCandidate === normalizedTarget) {
      bestScore = Math.max(bestScore, 100 + normalizedTarget.length);
      continue;
    }
    if (
      allowPartial &&
      normalizedTarget.length >= minPartialLength &&
      (normalizedCandidate.includes(normalizedTarget) || normalizedTarget.includes(normalizedCandidate))
    ) {
      bestScore = Math.max(bestScore, 10 + Math.min(normalizedCandidate.length, normalizedTarget.length));
    }
  }
  return bestScore;
}

function expandThinPersonSourceNames(person: Person) {
  const rawValues = [
    person.name,
    person.fullName,
    person.displayName,
    person.displayFullName,
    person.nameLatin,
    person.displayLatinName,
    ...(person.aliases || []),
  ]
    .map((value) => compact(value))
    .filter(Boolean);
  const expanded = new Set<string>();

  for (const rawValue of rawValues) {
    expanded.add(normalizeNameKey(rawValue));

    const strippedDigits = compact(rawValue.replace(/(?:18|19|20)\d{2}(?:s)?/gi, " ").replace(/\b\d{2}s\b/gi, " "));
    if (strippedDigits) {
      expanded.add(normalizeNameKey(strippedDigits));
      const tokens = strippedDigits
        .split(/[\s\-_/.,()]+/)
        .map((token) => compact(token))
        .filter(Boolean);
      if (tokens.length > 1) {
        const longestToken = [...tokens].sort((left, right) => right.length - left.length)[0];
        if (longestToken) {
          expanded.add(normalizeNameKey(longestToken));
        }
      }
    }
  }

  return [...expanded].filter(Boolean);
}

function uniqueNormalizedGroupIdentityNames(person: Person) {
  const values = [
    person.name,
    person.fullName,
    person.displayName,
    person.displayFullName,
    person.nameLatin,
    person.displayLatinName,
    person.slug,
    ...(person.aliases || []),
  ]
    .filter((value) => !isWeakGroupIdentityValue(value))
    .map((value) => normalizeGroupIdentityKey(value))
    .filter(Boolean);
  return [...new Set(values)];
}

function isThinDuplicateIndividualPerson(person: Person) {
  if (person.roles.some((role) => isEnsembleRole(role))) {
    return false;
  }
  if (isPlaceholderPerson(person)) {
    return false;
  }
  return (
    !compact(person.country) &&
    !compact(person.summary) &&
    !compact(person.avatarSrc) &&
    !compact(person.imageSourceUrl) &&
    !compact(person.imageAttribution) &&
    !compact(person.fullName) &&
    !compact(person.displayName) &&
    !compact(person.displayFullName) &&
    !compact(person.displayLatinName) &&
    (person.aliases || []).length <= 1
  );
}

function personSupportsFeaturedRole(person: Person) {
  const roles = new Set(person.roles || []);
  return roles.has("soloist") || roles.has("instrumentalist") || roles.has("singer");
}

function personIsConductorOnly(person: Person) {
  const roles = new Set(person.roles || []);
  return roles.has("conductor") && !personSupportsFeaturedRole(person) && !roles.has("composer");
}

function findCanonicalReplacementForThinPerson(library: LibraryData, person: Person) {
  if (!isThinDuplicateIndividualPerson(person)) {
    return null;
  }
  const sourceNames = expandThinPersonSourceNames(person).filter((value) => value.length >= 2);
  if (sourceNames.length === 0) {
    return null;
  }
  const candidates = (library.people || [])
    .filter((candidate) => candidate.id !== person.id)
    .filter((candidate) => !isPlaceholderPerson(candidate))
    .filter((candidate) => !candidate.roles.some((role) => isEnsembleRole(role)))
    .map((candidate) => ({
      person: candidate,
      score: sourceNames.reduce(
        (best, sourceName) =>
          Math.max(
            best,
            personMatchScore(candidate, sourceName, {
              minPartialLength: /[\u3400-\u9fff]/.test(sourceName) ? 2 : 3,
            }),
          ),
        0,
      ),
      quality: personQualityScore(candidate),
    }))
    .filter((candidate) => candidate.score > 0)
    .sort((left, right) => right.score - left.score || right.quality - left.quality);

  const replacement = candidates[0] || null;
  if (!replacement) {
    return null;
  }
  if ((candidates[1]?.score || 0) === replacement.score) {
    return null;
  }
  return replacement.quality > personQualityScore(person) ? replacement.person : null;
}

function isThinDuplicateGroupPerson(person: Person) {
  if (!person.roles.some((role) => isEnsembleRole(role))) {
    return false;
  }
  if (isPlaceholderPerson(person)) {
    return false;
  }
  return (
    !compact(person.country) &&
    !compact(person.summary) &&
    !compact(person.avatarSrc) &&
    !compact(person.imageSourceUrl) &&
    !compact(person.imageAttribution) &&
    (person.aliases || []).length === 0 &&
    !compact(person.fullName) &&
    !compact(person.displayName) &&
    !compact(person.displayFullName) &&
    !compact(person.displayLatinName)
  );
}

function isLowRiskDuplicateGroupPerson(person: Person) {
  return isThinDuplicateGroupPerson(person) || isPollutedGroupIdentity(person);
}

export function findCanonicalReplacementForGroupPerson(library: LibraryData, person: Person) {
  if (!isLowRiskDuplicateGroupPerson(person)) {
    return null;
  }
  const sourceNames = uniqueNormalizedGroupIdentityNames(person);
  const sourceNameSet = new Set(sourceNames);
  const sourceSlug = compact(person.slug);
  if (!sourceNameSet.size) {
    if (!sourceSlug) {
      return null;
    }
  }
  const candidates = (library.people || [])
    .filter((candidate) => candidate.id !== person.id)
    .filter((candidate) => !isPlaceholderPerson(candidate))
    .filter((candidate) => candidate.roles.some((role) => isEnsembleRole(role)) && ensembleCompatible(primaryRoleFromPerson(person), candidate))
    .filter((candidate) => {
      const candidateSlug = compact(candidate.slug);
      const candidateNames = uniqueNormalizedGroupIdentityNames(candidate);
      const exactMatch =
        (sourceSlug && candidateSlug && normalizeGroupIdentityKey(sourceSlug) === normalizeGroupIdentityKey(candidateSlug)) ||
        candidateNames.some((value) => sourceNameSet.has(value));
      if (exactMatch) {
        return true;
      }
      return hasNearDuplicateIdentity(sourceNames, candidateNames);
    })
    .sort((left, right) => personQualityScore(right) - personQualityScore(left));
  const replacement = candidates[0] || null;
  if (!replacement) {
    return null;
  }
  return personQualityScore(replacement) > personQualityScore(person) ? replacement : null;
}

export function findPersonForCredit(library: LibraryData, role: Credit["role"], displayName: string) {
  const target = compact(displayName);
  if (!target || isPlaceholderValue(target)) {
    return null;
  }
  if (isEnsembleRole(role)) {
    const candidates = (library.people || [])
      .filter((person) => !isPlaceholderPerson(person) && creditRoleCompatibleWithPerson(role, person))
      .filter((person) => personMatchesName(person, target))
      .sort((left, right) => personQualityScore(right) - personQualityScore(left));
    return candidates[0] || null;
  }
  const candidates = (library.people || [])
    .filter((person) => !isPlaceholderPerson(person) && creditRoleCompatibleWithPerson(role, person))
    .map((person) => ({
      person,
      score: personMatchScore(person, target),
      quality: personQualityScore(person),
    }))
    .filter((candidate) => candidate.score > 0)
    .sort((left, right) => right.score - left.score || right.quality - left.quality);
  if (candidates.length === 0) {
    return null;
  }
  if (!isEnsembleRole(role) && (candidates[1]?.score || 0) === candidates[0].score) {
    return null;
  }
  return candidates[0]?.person || null;
}

function canonicalCreditDisplayName(person: Person) {
  return compact(person.name || person.fullName || person.nameLatin);
}

function primaryRoleFromPerson(person: Person): Credit["role"] {
  if ((person.roles || []).includes("orchestra")) {
    return "orchestra";
  }
  if ((person.roles || []).includes("chorus")) {
    return "chorus";
  }
  if ((person.roles || []).includes("ensemble")) {
    return "ensemble";
  }
  return "ensemble";
}

function createFormalPersonForCredit(library: LibraryData, credit: Credit) {
  const displayName = compact(credit.displayName);
  const role = credit.role === "chorus" ? "chorus" : credit.role === "ensemble" ? "ensemble" : "orchestra";
  const person: Person = {
    id: createEntityId(`person-${role}`, displayName),
    slug: createSlug(displayName),
    name: displayName,
    nameLatin: /[A-Za-z]/.test(displayName) ? displayName : "",
    country: "",
    countries: [],
    avatarSrc: "",
    aliases: [],
    sortKey: createSortKey((library.people || []).length + 1),
    summary: "",
    imageSourceUrl: "",
    imageSourceKind: "",
    imageAttribution: "",
    imageUpdatedAt: "",
    infoPanel: { text: "", articleId: "", collectionLinks: [] },
    roles: [role satisfies PersonRole],
  };
  return {
    ...library,
    people: [...(library.people || []), person],
  };
}

export function ensurePeopleForCredits(library: LibraryData, credits: Credit[]) {
  let nextLibrary = library;
  for (const credit of credits.flatMap((entry) => splitCompositeEnsembleCredit(entry))) {
    if (!["orchestra", "ensemble", "chorus"].includes(credit.role)) {
      continue;
    }
    if (isPlaceholderValue(credit.displayName)) {
      continue;
    }
    const matchedPerson = findPersonForCredit(nextLibrary, credit.role, credit.displayName);
    if (matchedPerson) {
      continue;
    }
    nextLibrary = createFormalPersonForCredit(nextLibrary, credit);
  }
  return nextLibrary;
}

function extractMetadataCandidates(recording: Pick<Recording, "performanceDateText" | "venueText">) {
  const texts = [compact(recording.venueText), compact(recording.performanceDateText)].filter(Boolean);
  const candidates = new Set<string>();
  for (const text of texts) {
    candidates.add(text);
    for (const piece of text.split("/")) {
      const trimmedPiece = compact(piece);
      if (trimmedPiece) {
        candidates.add(trimmedPiece);
      }
    }
    for (const piece of text.split(",")) {
      const trimmedPiece = compact(piece);
      if (trimmedPiece && !/^\d{4}([./-]\d{1,2}([./-]\d{1,2})?)?$/.test(trimmedPiece)) {
        candidates.add(trimmedPiece);
      }
    }
  }
  return [...candidates];
}

function inferEnsemblePersonFromMetadata(library: LibraryData, recording: Recording) {
  let nextLibrary = library;
  const venueCandidates = new Set(extractMetadataCandidates({ venueText: recording.venueText, performanceDateText: "" }));
  const performanceDateCandidates = new Set(
    extractMetadataCandidates({ venueText: "", performanceDateText: recording.performanceDateText }),
  );
  const candidates = extractMetadataCandidates(recording);
  for (const candidate of candidates) {
    const inferredRole = classifyEnsemblePart(candidate);
    if (!inferredRole) {
      continue;
    }
    const sourceField = venueCandidates.has(candidate)
      ? "venueText"
      : performanceDateCandidates.has(candidate)
        ? "performanceDateText"
        : null;
    const matched = findPersonForCredit(nextLibrary, inferredRole, candidate);
    if (matched && ensembleCompatible(inferredRole, matched)) {
      return {
        library: nextLibrary,
        person: matched,
        sourceField,
      };
    }
    nextLibrary = createFormalPersonForCredit(nextLibrary, {
      role: inferredRole,
      personId: "",
      displayName: candidate,
      label: "元数据推断乐团",
    });
    const created = findPersonForCredit(nextLibrary, inferredRole, candidate);
    if (created && ensembleCompatible(inferredRole, created)) {
      return {
        library: nextLibrary,
        person: created,
        sourceField,
      };
    }
  }
  return {
    library: nextLibrary,
    person: null,
    sourceField: null,
  };
}

export function repairRecordingPeople(library: LibraryData, recording: Recording) {
  let nextLibrary = library;
  const nextCredits: Recording["credits"] = [];
  let nextVenueText = recording.venueText;
  const redirectedThinGroupIds = new Map<string, Person>();
  const redirectedThinPersonIds = new Map<string, Person>();
  const suspiciousCompositeGroupIds = new Map<string, Person>();

  for (const person of nextLibrary.people || []) {
    const replacement = findCanonicalReplacementForGroupPerson(nextLibrary, person);
    if (replacement) {
      redirectedThinGroupIds.set(person.id, replacement);
    }
    const thinPersonReplacement = findCanonicalReplacementForThinPerson(nextLibrary, person);
    if (thinPersonReplacement) {
      redirectedThinPersonIds.set(person.id, thinPersonReplacement);
    }
    if (isSuspiciousCompositeEnsemblePerson(person)) {
      suspiciousCompositeGroupIds.set(person.id, person);
    }
  }

  for (const currentCredit of (recording.credits || []).flatMap((entry) => splitCompositeEnsembleCredit(entry))) {
    let nextCredit: Credit = {
      ...currentCredit,
      personId: compact(currentCredit.personId),
      displayName: compact(currentCredit.displayName),
      label: compact(currentCredit.label),
    };

    const ambiguousCompositePerson = suspiciousCompositeGroupIds.get(compact(nextCredit.personId));
    if (ambiguousCompositePerson) {
      nextCredit = {
        ...nextCredit,
        role: primaryRoleFromPerson(ambiguousCompositePerson),
        personId: "",
        displayName: compact(nextCredit.displayName) || canonicalCreditDisplayName(ambiguousCompositePerson),
      };
    }

    const redirectedPerson = redirectedThinGroupIds.get(compact(nextCredit.personId));
    if (redirectedPerson) {
      nextCredit = {
        ...nextCredit,
        role: primaryRoleFromPerson(redirectedPerson),
        personId: redirectedPerson.id,
        displayName: canonicalCreditDisplayName(redirectedPerson),
      };
    }

    const redirectedThinPerson = redirectedThinPersonIds.get(compact(nextCredit.personId));
    if (redirectedThinPerson) {
      if (creditRoleCompatibleWithPerson(nextCredit.role, redirectedThinPerson)) {
        nextCredit = {
          ...nextCredit,
          personId: redirectedThinPerson.id,
          displayName: canonicalCreditDisplayName(redirectedThinPerson),
        };
      } else if (
        (nextCredit.role === "soloist" || nextCredit.role === "instrumentalist" || nextCredit.role === "singer") &&
        personIsConductorOnly(redirectedThinPerson) &&
        (recording.credits || []).some(
          (credit) =>
            credit !== currentCredit &&
            (credit.role === "soloist" || credit.role === "instrumentalist" || credit.role === "singer") &&
            !isPlaceholderValue(credit.displayName),
        )
      ) {
        continue;
      }
    }

    if (!isPlaceholderValue(nextCredit.displayName)) {
      if (["orchestra", "ensemble", "chorus"].includes(nextCredit.role) && !ambiguousCompositePerson) {
        nextLibrary = ensurePeopleForCredits(nextLibrary, [nextCredit]);
      }
      const matchedPerson = ambiguousCompositePerson ? null : findPersonForCredit(nextLibrary, nextCredit.role, nextCredit.displayName);
      if (matchedPerson) {
        nextCredit = {
          ...nextCredit,
          role: ["orchestra", "ensemble", "chorus"].includes(nextCredit.role) ? primaryRoleFromPerson(matchedPerson) : nextCredit.role,
          personId: matchedPerson.id,
          displayName: canonicalCreditDisplayName(matchedPerson),
        };
      }
    }

    nextCredits.push(nextCredit);
  }

  const conductorIds = new Set(
    nextCredits
      .filter((credit) => credit.role === "conductor")
      .map((credit) => compact(credit.personId))
      .filter(Boolean),
  );
  const normalizedCredits = nextCredits.filter((credit) => {
    if (credit.role !== "soloist" && credit.role !== "instrumentalist" && credit.role !== "singer") {
      return true;
    }
    if (!isFileNameBackfillLabel(credit.label)) {
      return true;
    }
    return !conductorIds.has(compact(credit.personId));
  });

  const hasEnsembleCredit = normalizedCredits.some((credit) => ["orchestra", "ensemble", "chorus"].includes(credit.role));
  if (!hasEnsembleCredit) {
    const inferred = inferEnsemblePersonFromMetadata(nextLibrary, recording);
    nextLibrary = inferred.library;
    if (inferred.person) {
      if (inferred.sourceField === "venueText") {
        nextVenueText = "";
      }
      normalizedCredits.push({
        role: primaryRoleFromPerson(inferred.person),
        personId: inferred.person.id,
        displayName: canonicalCreditDisplayName(inferred.person),
        label: "元数据推断",
      });
    }
  }

  return {
    library: nextLibrary,
    recording: {
      ...recording,
      credits: dedupeCredits(normalizedCredits),
      venueText: nextVenueText,
    },
  };
}

export function stripUnusedPlaceholderPeople(library: LibraryData) {
  const referencedPersonIds = new Set(
    (library.recordings || []).flatMap((recording) => (recording.credits || []).map((credit) => compact(credit.personId)).filter(Boolean)),
  );
  return {
    ...library,
    people: (library.people || []).filter((person) => {
      if (referencedPersonIds.has(person.id)) {
        return true;
      }
      if (isPlaceholderPerson(person)) {
        return false;
      }
      if (isSuspiciousCompositeEnsemblePerson(person)) {
        return false;
      }
      if (findCanonicalReplacementForThinPerson(library, person)) {
        return false;
      }
      if (findCanonicalReplacementForGroupPerson(library, person)) {
        return false;
      }
      return true;
    }),
  };
}

export function cleanupLibraryPeople(library: LibraryData) {
  let nextLibrary = library;
  const nextRecordings: Recording[] = [];
  for (const recording of library.recordings || []) {
    const repaired = repairRecordingPeople(nextLibrary, recording);
    nextLibrary = repaired.library;
    nextRecordings.push(repaired.recording);
  }
  return stripUnusedPlaceholderPeople({
    ...nextLibrary,
    recordings: nextRecordings,
  });
}
