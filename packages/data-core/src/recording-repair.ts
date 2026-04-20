import { parseLegacyRecordingHtml } from "./legacy-parser.js";
import type { LibraryData, Person, Recording } from "../../shared/src/schema.js";
import { deriveRecordingPresentationFamily, resolveRecordingWorkTypeHintValue } from "../../shared/src/recording-rules.js";
import { buildRecordingDisplayTitle } from "../../shared/src/display.js";
import { findPersonForCredit, isPlaceholderValue } from "./person-cleanup.js";

type ParsedLegacyRecording = ReturnType<typeof parseLegacyRecordingHtml>;
export type RecordingLegacyRepairHint = {
  resolutionHint: "auto-fixable" | "manual-backfill";
  details?: string[];
};

function compact(value: unknown) {
  return String(value ?? "").trim();
}

function isPlaceholderRecordingMetadataValue(value: unknown) {
  const normalized = compact(value).toLowerCase();
  return !normalized || normalized === "-" || normalized === "*" || normalized === "unknown" || normalized === "未知";
}

function isPlaceholderCredit(credit: { personId?: unknown; displayName?: unknown }) {
  return compact(credit.personId) === "person-item" || isPlaceholderValue(credit.displayName);
}

function canonicalCreditDisplayName(person: Person, role: string) {
  if (role === "orchestra" || role === "ensemble" || role === "chorus") {
    return compact(person.name || person.fullName || person.nameLatin);
  }
  return compact(person.name || person.fullName || person.nameLatin);
}

function buildPatchedCredit(library: LibraryData, credit: Recording["credits"][number]) {
  const matchedPerson = findPersonForCredit(library, credit.role, credit.displayName);
  if (!matchedPerson) {
    return {
      ...credit,
      personId: compact(credit.personId),
      displayName: compact(credit.displayName),
      label: compact(credit.label),
    };
  }
  return {
    ...credit,
    personId: matchedPerson.id,
    displayName: canonicalCreditDisplayName(matchedPerson, credit.role),
    label: compact(credit.label),
  };
}

function getRecordingWorkContext(library: LibraryData, recording: Recording) {
  const work = (library.works || []).find((item) => item.id === recording.workId) || null;
  const workGroups = (work?.groupIds || [])
    .map((groupId) => (library.workGroups || []).find((group) => group.id === groupId))
    .filter((group): group is LibraryData["workGroups"][number] => Boolean(group));
  return { work, workGroups };
}

function canonicalizeCreditRole(person: Person | null, role: Recording["credits"][number]["role"]) {
  if (!person) {
    return role;
  }
  if (role !== "ensemble" && role !== "orchestra" && role !== "chorus") {
    return role;
  }
  const roles = new Set(person.roles || []);
  if (roles.has("orchestra")) {
    return "orchestra";
  }
  if (roles.has("chorus")) {
    return "chorus";
  }
  if (roles.has("ensemble")) {
    return "ensemble";
  }
  return role;
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
    nextCredits.push(credit);
  }
  return nextCredits;
}

export function normalizeRecordingCredits(library: LibraryData, credits: Recording["credits"]) {
  const normalizedCredits = credits.map((credit) => {
    const person = compact(credit.personId) ? (library.people || []).find((item) => item.id === compact(credit.personId)) || null : null;
    return {
      ...credit,
      role: canonicalizeCreditRole(person, credit.role),
      personId: compact(credit.personId),
      displayName: compact(credit.displayName),
      label: compact(credit.label),
    };
  });
  return dedupeCredits(normalizedCredits);
}

function looksDateLike(value: string) {
  return /\d/.test(value);
}

function maybePromoteVenueToEnsembleCredit(
  library: LibraryData,
  recording: Pick<Recording, "credits" | "venueText">,
  metadata: { performanceDateText: string; venueText: string },
) {
  const hasEnsembleCredit = (recording.credits || []).some((credit) =>
    ["orchestra", "ensemble", "chorus"].includes(credit.role),
  );
  if (hasEnsembleCredit || !metadata.venueText) {
    return null;
  }

  const matchedPerson = findPersonForCredit(library, "orchestra", metadata.venueText);
  if (!matchedPerson) {
    return null;
  }

  return {
    credit: {
      role: "orchestra" as const,
      personId: matchedPerson.id,
      displayName: canonicalCreditDisplayName(matchedPerson, "orchestra"),
      label: "地点回填乐团",
    },
    venueText: "",
  };
}

function countCredits(recording: Pick<Recording, "credits" | "workTypeHint">) {
  const credits = recording.credits || [];
  return {
    conductorCount: credits.filter((credit) => credit.role === "conductor").length,
    orchestraCount: credits.filter((credit) => credit.role === "orchestra").length,
    soloistCount: credits.filter((credit) => credit.role === "soloist" || credit.role === "instrumentalist").length,
    singerCount: credits.filter((credit) => credit.role === "singer").length,
    ensembleCount: credits.filter((credit) => credit.role === "ensemble" || credit.role === "chorus").length,
  };
}

function getMissingRequiredCreditRoles(recording: Pick<Recording, "credits" | "workTypeHint">) {
  const counts = countCredits(recording);
  const family = deriveRecordingPresentationFamily({
    workTypeHint: recording.workTypeHint,
    ...counts,
  });
  const ensembleCount = counts.orchestraCount + counts.ensembleCount;
  const featuredCount = counts.soloistCount + counts.singerCount;
  const missingRoles: string[] = [];

  if (family === "orchestral") {
    if (counts.conductorCount === 0) {
      missingRoles.push("conductor");
    }
    if (ensembleCount === 0) {
      missingRoles.push("orchestra_or_ensemble");
    }
  } else if (family === "concerto") {
    if (featuredCount === 0) {
      missingRoles.push("soloist");
    }
    if (ensembleCount === 0) {
      missingRoles.push("orchestra_or_ensemble");
    }
  } else if (family === "opera") {
    if (counts.conductorCount === 0) {
      missingRoles.push("conductor");
    }
    if (featuredCount === 0) {
      missingRoles.push("singer_or_soloist");
    }
    if (ensembleCount === 0) {
      missingRoles.push("orchestra_or_ensemble");
    }
  } else if (family === "solo") {
    if (featuredCount === 0 && ensembleCount === 0) {
      missingRoles.push("soloist");
    }
  } else if (family === "chamber") {
    if (ensembleCount === 0 && featuredCount < 2) {
      missingRoles.push("ensemble_or_multiple_soloists");
    }
  }

  return missingRoles;
}

export function classifyRecordingLegacyRepairHint(
  library: LibraryData,
  recording: Recording,
  parsed: ParsedLegacyRecording,
): RecordingLegacyRepairHint {
  const preview = repairRecordingFromLegacyParse(library, recording, parsed);
  const missingRoles = getMissingRequiredCreditRoles(preview);

  if (missingRoles.length === 0) {
    return {
      resolutionHint: "auto-fixable",
    };
  }

  return {
    resolutionHint: "manual-backfill",
    details: [`archive 中缺少可解析的关键署名：${missingRoles.join(", ")}`],
  };
}

export function recordingNeedsLegacyRepair(
  library: LibraryData,
  recording: Pick<
    Recording,
    | "legacyPath"
    | "credits"
    | "performanceDateText"
    | "venueText"
    | "albumTitle"
    | "label"
    | "releaseDate"
    | "workTypeHint"
    | "workId"
  >,
) {
  if (!compact(recording.legacyPath)) {
    return false;
  }

  if (hasPlaceholderCredits(recording)) {
    return true;
  }

  const { work, workGroups } = getRecordingWorkContext(library, recording as Recording);
  const nextWorkTypeHint = resolveRecordingWorkTypeHintValue(recording.workTypeHint, work, workGroups);
  return getMissingRequiredCreditRoles({
    credits: recording.credits || [],
    workTypeHint: nextWorkTypeHint,
  }).length > 0;
}

export function normalizeRecordingMetadata(recording: Pick<Recording, "performanceDateText" | "venueText">) {
  let performanceDateText = isPlaceholderRecordingMetadataValue(recording.performanceDateText)
    ? ""
    : compact(recording.performanceDateText);
  let venueText = isPlaceholderRecordingMetadataValue(recording.venueText) ? "" : compact(recording.venueText);

  if (performanceDateText && venueText && !looksDateLike(performanceDateText) && looksDateLike(venueText)) {
    [performanceDateText, venueText] = [venueText, performanceDateText];
  }

  if (performanceDateText && !venueText && !looksDateLike(performanceDateText)) {
    venueText = performanceDateText;
    performanceDateText = "";
  }

  if (venueText || !performanceDateText.includes(" / ")) {
    return {
      performanceDateText,
      venueText,
    };
  }
  const [nextPerformanceDateText, ...restVenueParts] = performanceDateText.split(" / ");
  return {
    performanceDateText: compact(nextPerformanceDateText),
    venueText: compact(restVenueParts.join(" / ")),
  };
}

export function rebuildRecordingDerivedFields(library: LibraryData, recording: Recording): Recording {
  const { work, workGroups } = getRecordingWorkContext(library, recording);
  const metadata = normalizeRecordingMetadata(recording);
  const credits = normalizeRecordingCredits(library, recording.credits || []);
  const promotedVenueCredit = maybePromoteVenueToEnsembleCredit(library, { credits, venueText: recording.venueText }, metadata);
  const nextCredits = promotedVenueCredit ? normalizeRecordingCredits(library, [...credits, promotedVenueCredit.credit]) : credits;
  const nextRecording = {
    ...recording,
    credits: nextCredits,
    ...metadata,
    ...(promotedVenueCredit ? { venueText: promotedVenueCredit.venueText } : {}),
    workTypeHint: resolveRecordingWorkTypeHintValue(recording.workTypeHint, work, workGroups),
  };
  return {
    ...nextRecording,
    title: buildRecordingDisplayTitle(nextRecording, library) || compact(recording.title),
  };
}

export function backfillRecordingWorkTypeHints(library: LibraryData): LibraryData {
  return {
    ...library,
    recordings: (library.recordings || []).map((recording) => rebuildRecordingDerivedFields(library, recording)),
  };
}

function hasPlaceholderCredits(recording: Pick<Recording, "credits">) {
  return (recording.credits || []).some((credit) => isPlaceholderCredit(credit));
}

export function repairRecordingFromLegacyParse(library: LibraryData, recording: Recording, parsed: ParsedLegacyRecording): Recording {
  const keptCredits = (recording.credits || []).filter((credit) => !isPlaceholderCredit(credit));
  const nextCredits = [...keptCredits];

  for (const parsedCredit of parsed.credits || []) {
    if (isPlaceholderValue(parsedCredit.displayName)) {
      continue;
    }
    const existingSameRole = nextCredits.find((credit) => credit.role === parsedCredit.role);
    if (existingSameRole) {
      continue;
    }
    nextCredits.push(
      buildPatchedCredit(library, {
        ...parsedCredit,
        personId: compact(parsedCredit.personId),
        displayName: compact(parsedCredit.displayName),
        label: compact(parsedCredit.label),
      }),
    );
  }

  return rebuildRecordingDerivedFields(library, {
    ...recording,
    credits: nextCredits,
    performanceDateText:
      compact(parsed.performanceDateText) || compact(recording.performanceDateText) || compact(recording.venueText),
    venueText: compact(parsed.venueText) || recording.venueText,
    albumTitle: compact(parsed.albumTitle) || recording.albumTitle,
    label: compact(parsed.label) || recording.label,
    releaseDate: compact(parsed.releaseDate) || recording.releaseDate,
    links: (recording.links || []).length > 0 ? recording.links : parsed.links,
    images: (recording.images || []).length > 0 ? recording.images : parsed.images,
  });
}
