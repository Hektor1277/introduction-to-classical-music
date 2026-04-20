import { buildIndexes } from "../../data-core/src/indexes.js";
import type { LibraryData } from "../../shared/src/schema.js";

export type EditableEntityType = "composer" | "person" | "work" | "recording";

function compact(value: unknown) {
  return String(value ?? "").trim();
}

function uniqueStrings(values: Array<unknown>) {
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

function mergeScalar<T>(primaryValue: T, duplicateValue: T) {
  if (Array.isArray(primaryValue) || Array.isArray(duplicateValue)) {
    return primaryValue;
  }
  if (primaryValue && typeof primaryValue === "object") {
    return Object.keys(primaryValue as Record<string, unknown>).length > 0 ? primaryValue : duplicateValue;
  }
  if (duplicateValue && typeof duplicateValue === "object") {
    return duplicateValue;
  }
  const primaryText = compact(primaryValue);
  return primaryText ? primaryValue : duplicateValue;
}

function mergeAliases(primary: { name?: string; nameLatin?: string; aliases?: string[] }, duplicate: { name?: string; nameLatin?: string; aliases?: string[] }) {
  return uniqueStrings([
    ...(primary.aliases || []),
    ...(duplicate.aliases || []),
    duplicate.name,
    duplicate.nameLatin,
  ]).filter((value) => value !== compact(primary.name) && value !== compact(primary.nameLatin));
}

export function getAffectedPaths(library: LibraryData, entityType: EditableEntityType, entityId: string) {
  const indexes = buildIndexes(library);
  const paths = new Set<string>(["/", "/search/", "/about/"]);

  if (entityType === "composer") {
    const composer = library.composers.find((item) => item.id === entityId);
    if (composer) {
      paths.add(`/composers/${composer.slug}/`);
      paths.add("/composers/");
    }
    return [...paths];
  }

  if (entityType === "person") {
    const person = library.people.find((item) => item.id === entityId);
    if (person) {
      const href = indexes.personIndex[person.id]?.href;
      if (href) {
        paths.add(href);
      }
      if (person.roles.includes("composer")) {
        paths.add("/composers/");
        paths.add(`/composers/${person.slug}/`);
      }
      if (person.roles.includes("conductor")) {
        paths.add("/conductors/");
      }
      if (person.roles.includes("orchestra")) {
        paths.add(`/orchestras/${person.slug}/`);
      }
    }
    return [...paths];
  }

  if (entityType === "work") {
    const work = library.works.find((item) => item.id === entityId);
    if (work) {
      paths.add(`/works/${work.id}/`);
      const composer = library.composers.find((item) => item.id === work.composerId);
      if (composer) {
        paths.add(`/composers/${composer.slug}/`);
      }
    }
    return [...paths];
  }

  const recording = library.recordings.find((item) => item.id === entityId);
  if (!recording) {
    return [...paths];
  }

  paths.add(`/recordings/${recording.id}/`);

  const work = library.works.find((item) => item.id === recording.workId);
  if (work) {
    paths.add(`/works/${work.id}/`);
    const composer = library.composers.find((item) => item.id === work.composerId);
    if (composer) {
      paths.add(`/composers/${composer.slug}/`);
    }
  }

  for (const credit of recording.credits) {
    if (!credit.personId) {
      continue;
    }

    const href = indexes.personIndex[credit.personId]?.href;
    if (href) {
      paths.add(href);
    }
  }

  return [...paths];
}

export function mergeLibraryEntities(
  library: LibraryData,
  entityType: Exclude<EditableEntityType, "recording">,
  primaryId: string,
  duplicateId: string,
) {
  if (primaryId === duplicateId) {
    throw new Error("Primary and duplicate entities must be different.");
  }

  const nextLibrary = structuredClone(library) as LibraryData;
  const collection =
    entityType === "composer"
      ? nextLibrary.composers
      : entityType === "person"
        ? nextLibrary.people
        : nextLibrary.works;
  const primaryIndex = collection.findIndex((item) => item.id === primaryId);
  const duplicateIndex = collection.findIndex((item) => item.id === duplicateId);

  if (primaryIndex < 0 || duplicateIndex < 0) {
    throw new Error("Entity not found.");
  }

  const primary = collection[primaryIndex] as Record<string, unknown>;
  const duplicate = collection[duplicateIndex] as Record<string, unknown>;
  const merged = { ...primary } as Record<string, unknown>;

  for (const [key, value] of Object.entries(duplicate)) {
    if (key === "id" || key === "slug" || key === "sortKey" || key === "updatedAt" || key === "roles") {
      continue;
    }
    if (key === "aliases") {
      continue;
    }
    merged[key] = mergeScalar(merged[key], value);
  }

  if (entityType === "person") {
    const primaryRoles = Array.isArray(primary.roles) ? primary.roles : [];
    const duplicateRoles = Array.isArray(duplicate.roles) ? duplicate.roles : [];
    merged.roles = uniqueStrings([...primaryRoles, ...duplicateRoles]);
  }

  merged.aliases = mergeAliases(
    {
      name: compact(primary.name),
      nameLatin: compact(primary.nameLatin),
      aliases: Array.isArray(primary.aliases) ? (primary.aliases as string[]) : [],
    },
    {
      name: compact(duplicate.name),
      nameLatin: compact(duplicate.nameLatin),
      aliases: Array.isArray(duplicate.aliases) ? (duplicate.aliases as string[]) : [],
    },
  );

  collection[primaryIndex] = merged as (typeof collection)[number];
  collection.splice(duplicateIndex, 1);

  if (entityType === "person") {
    nextLibrary.recordings = nextLibrary.recordings.map((recording) => ({
      ...recording,
      credits: (recording.credits || []).map((credit) =>
        credit.personId === duplicateId
          ? {
              ...credit,
              personId: primaryId,
            }
          : credit,
      ),
    }));
  }

  if (entityType === "composer") {
    nextLibrary.workGroups = nextLibrary.workGroups.map((group) =>
      group.composerId === duplicateId
        ? {
            ...group,
            composerId: primaryId,
          }
        : group,
    );
    nextLibrary.works = nextLibrary.works.map((work) =>
      work.composerId === duplicateId
        ? {
            ...work,
            composerId: primaryId,
          }
        : work,
    );
  }

  if (entityType === "work") {
    nextLibrary.recordings = nextLibrary.recordings.map((recording) =>
      recording.workId === duplicateId
        ? {
            ...recording,
            workId: primaryId,
          }
        : recording,
    );
  }

  return nextLibrary;
}


