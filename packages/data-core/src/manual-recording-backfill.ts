import type { Credit, LibraryData, Recording } from "../../shared/src/schema.js";
import { ensurePeopleForCredits, findPersonForCredit } from "./person-cleanup.js";
import { rebuildRecordingDerivedFields } from "./recording-repair.js";

type ManualRecordingMetadata = Partial<Pick<Recording, "performanceDateText" | "venueText" | "albumTitle" | "label" | "releaseDate">>;

export type ManualRecordingBackfillEntry = {
  recordingId: string;
  removeCredits?: Array<Partial<Pick<Credit, "role" | "displayName" | "personId">>>;
  credits?: Array<Pick<Credit, "role" | "displayName"> & Partial<Pick<Credit, "label" | "personId">>>;
  metadata?: ManualRecordingMetadata;
  waivedMissingRoles?: string[];
};

function compact(value: unknown) {
  return String(value ?? "").trim();
}

function normalizeManualCredit(credit: Pick<Credit, "role" | "displayName"> & Partial<Pick<Credit, "label" | "personId">>): Credit {
  return {
    role: credit.role,
    displayName: compact(credit.displayName),
    label: compact(credit.label),
    personId: compact(credit.personId),
  };
}

function mergeCredits(existing: Credit[], additions: Credit[]) {
  const seen = new Set(existing.map((credit) => `${compact(credit.role)}::${compact(credit.personId)}::${compact(credit.displayName)}`));
  const merged = [...existing];
  for (const credit of additions) {
    const key = `${compact(credit.role)}::${compact(credit.personId)}::${compact(credit.displayName)}`;
    if (!compact(credit.displayName) || seen.has(key)) {
      continue;
    }
    seen.add(key);
    merged.push(credit);
  }
  return merged;
}

function removeCredits(existing: Credit[], removals: Array<Partial<Pick<Credit, "role" | "displayName" | "personId">>>) {
  if (!removals.length) {
    return existing;
  }
  return existing.filter((credit) => {
    return !removals.some((removal) => {
      const roleMatches = compact(removal.role) ? compact(removal.role) === compact(credit.role) : true;
      const displayNameMatches = compact(removal.displayName) ? compact(removal.displayName) === compact(credit.displayName) : true;
      const personIdMatches = compact(removal.personId) ? compact(removal.personId) === compact(credit.personId) : true;
      return roleMatches && displayNameMatches && personIdMatches;
    });
  });
}

function normalizeManualMetadata(metadata: ManualRecordingMetadata | undefined) {
  if (!metadata) {
    return {};
  }

  const nextMetadata: ManualRecordingMetadata = {};
  for (const key of ["performanceDateText", "venueText", "albumTitle", "label", "releaseDate"] as const) {
    if (Object.prototype.hasOwnProperty.call(metadata, key)) {
      nextMetadata[key] = compact(metadata[key]);
    }
  }
  return nextMetadata;
}

export function applyManualRecordingBackfills(library: LibraryData, entries: ManualRecordingBackfillEntry[]) {
  let nextLibrary = library;
  let changed = false;

  for (const entry of entries || []) {
    const recordingIndex = (nextLibrary.recordings || []).findIndex((recording) => recording.id === entry.recordingId);
    if (recordingIndex < 0) {
      continue;
    }

    const normalizedCredits = (entry.credits || []).map(normalizeManualCredit).filter((credit) => compact(credit.displayName));
    const removals: Array<Partial<Pick<Credit, "role" | "displayName" | "personId">>> = (entry.removeCredits || []).map((credit) => ({
      role: credit.role,
      displayName: compact(credit.displayName),
      personId: compact(credit.personId),
    }));
    const metadata = normalizeManualMetadata(entry.metadata);
    if (normalizedCredits.length === 0 && removals.length === 0 && Object.keys(metadata).length === 0) {
      continue;
    }

    if (normalizedCredits.length > 0) {
      nextLibrary = ensurePeopleForCredits(nextLibrary, normalizedCredits);
    }

    const resolvedCredits = normalizedCredits.map((credit) => {
      const matchedPerson = compact(credit.personId)
        ? (nextLibrary.people || []).find((person) => person.id === compact(credit.personId)) || null
        : findPersonForCredit(nextLibrary, credit.role, credit.displayName);
      return matchedPerson
        ? {
            ...credit,
            personId: matchedPerson.id,
            displayName: compact(matchedPerson.name || matchedPerson.nameLatin || credit.displayName),
          }
        : credit;
    });

    const nextRecordings = [...nextLibrary.recordings];
    const currentRecording = nextRecordings[recordingIndex];
    const cleanedCredits = removeCredits(currentRecording.credits || [], removals);
    const mergedCredits = mergeCredits(cleanedCredits, resolvedCredits);
    const rebuiltRecording = rebuildRecordingDerivedFields(nextLibrary, {
      ...currentRecording,
      ...metadata,
      credits: mergedCredits,
    });
    nextRecordings[recordingIndex] = rebuiltRecording;
    nextLibrary = {
      ...nextLibrary,
      recordings: nextRecordings,
    };
    changed = true;
  }

  return changed ? nextLibrary : library;
}
