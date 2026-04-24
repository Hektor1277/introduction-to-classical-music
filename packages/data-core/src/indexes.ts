import { buildRecordingDisplayTitle, getEntitySearchTexts, getWebsiteDisplay } from "../../shared/src/display.js";
import type { Composer, LibraryData, Person, Recording, Work } from "../../shared/src/schema.js";
import type { Article } from "./articles.js";

export type PersonLinkConfig = {
  canonicalPersonLinks: Record<string, string>;
};

type ComposerTreeWork = {
  id: string;
  title: string;
  href: string;
};

type ComposerTreeNode = {
  id: string;
  title: string;
  href: string;
  children: ComposerTreeNode[];
  works: ComposerTreeWork[];
  sortKey: string;
};

export type SearchEntry = {
  id: string;
  kind: "composer" | "workGroup" | "work" | "recording" | "conductor" | "orchestra" | "person" | "article";
  primaryText: string;
  secondaryText: string;
  href: string;
  matchTokens: string[];
  aliasTokens: string[];
};

type RelationshipRecordingEntry = {
  id: string;
  title: string;
  href: string;
};

type RelationshipWorkEntry = {
  workId: string;
  title: string;
  href: string;
  recordings: RelationshipRecordingEntry[];
};

type RelationshipComposerGroup = {
  composerId: string;
  composerName: string;
  works: RelationshipWorkEntry[];
};

export type RelationshipIndexEntry = {
  personId: string;
  name: string;
  href: string;
  groups: RelationshipComposerGroup[];
};

export type PersonIndexEntry = {
  id: string;
  canonicalId: string;
  name: string;
  href: string;
  roles: string[];
  appearanceCount: number;
};

export type LibraryIndexes = {
  stats: {
    composerCount: number;
    workCount: number;
    recordingCount: number;
    conductorCount: number;
    orchestraCount: number;
    lastUpdatedAt: string;
  };
  composerTree: Record<string, ComposerTreeNode>;
  canonicalPeople: Record<string, string>;
  conductorIndex: Record<string, RelationshipIndexEntry>;
  orchestraIndex: Record<string, RelationshipIndexEntry>;
  personIndex: Record<string, PersonIndexEntry>;
  searchIndex: SearchEntry[];
};

function workHref(work: Work) {
  return `/works/${work.id}/`;
}

function recordingHref(recording: Recording) {
  return `/recordings/${recording.id}/`;
}

function composerHref(composer: Composer) {
  return `/composers/${composer.slug}/`;
}

function personHref(person: Person) {
  if (person.roles.includes("conductor")) {
    return `/conductors/${person.slug}/`;
  }
  if (person.roles.includes("orchestra")) {
    return `/orchestras/${person.slug}/`;
  }
  return `/people/${person.slug}/`;
}

function sortByKey<T extends { sortKey: string; title?: string; name?: string }>(items: T[]) {
  return [...items].sort((left, right) => {
    const key = left.sortKey.localeCompare(right.sortKey, "zh-Hans-CN");
    if (key !== 0) {
      return key;
    }

    return (left.title ?? left.name ?? "").localeCompare(right.title ?? right.name ?? "", "zh-Hans-CN");
  });
}

function buildComposerRoot(composer: Composer): ComposerTreeNode {
  return {
    id: composer.id,
    title: getWebsiteDisplay(composer).heading,
    href: composerHref(composer),
    children: [],
    works: [],
    sortKey: composer.sortKey,
  };
}

function dedupe(values: string[]) {
  return [...new Set(values.filter(Boolean))];
}

function normalizeWorkComparableText(value: string) {
  return String(value ?? "")
    .trim()
    .toLowerCase()
    .replace(/[，,:：;；()[\]{}]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function escapeRegExp(value: string) {
  return String(value ?? "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function stripCatalogueFromWorkSegment(segment: string, catalogue: string) {
  const normalizedSegment = String(segment ?? "").trim();
  const normalizedCatalogue = String(catalogue ?? "").trim();
  if (!normalizedSegment || !normalizedCatalogue) {
    return normalizedSegment;
  }
  if (normalizeWorkComparableText(normalizedSegment) === normalizeWorkComparableText(normalizedCatalogue)) {
    return "";
  }
  const escapedCatalogue = escapeRegExp(normalizedCatalogue).replace(/\s+/g, "\\s+");
  const trailingPatterns = [
    new RegExp(`(?:\\s*[,，:：;；/|·-]\\s*|\\s+)\\(?${escapedCatalogue}\\)?$`, "i"),
    new RegExp(`\\(${escapedCatalogue}\\)$`, "i"),
  ];
  return trailingPatterns.reduce((currentValue, pattern) => currentValue.replace(pattern, "").trim(), normalizedSegment);
}

function resolveCanonicalPersonId(personId: string, personLinks: PersonLinkConfig) {
  const seen = new Set<string>();
  let currentId = personId;

  while (personLinks.canonicalPersonLinks[currentId] && !seen.has(currentId)) {
    seen.add(currentId);
    currentId = personLinks.canonicalPersonLinks[currentId];
  }

  return currentId;
}

function normalizeRecordingForDisplay(recording: Recording, personLinks: PersonLinkConfig, personMap: Map<string, Person>) {
  const nextCredits = recording.credits.map((credit) => {
    if (!credit.personId) {
      return credit;
    }
    const canonicalPersonId = resolveCanonicalPersonId(credit.personId, personLinks);
    const person = personMap.get(canonicalPersonId) ?? personMap.get(credit.personId);
    if (!person) {
      return credit;
    }
    return {
      ...credit,
      personId: person.id,
    };
  });

  return {
    ...recording,
    credits: nextCredits,
  };
}

function ensureRelationshipEntry(
  bucket: Record<string, RelationshipIndexEntry>,
  library: LibraryData,
  person: Person,
  composer: Composer,
  work: Work,
  recording: Recording,
) {
  let entry = bucket[person.id];
  if (!entry) {
    entry = {
      personId: person.id,
      name: getWebsiteDisplay(person).heading,
      href: personHref(person),
      groups: [],
    };
    bucket[person.id] = entry;
  }

  let composerGroup = entry.groups.find((group) => group.composerId === composer.id);
  if (!composerGroup) {
    composerGroup = {
      composerId: composer.id,
      composerName: getWebsiteDisplay(composer).heading,
      works: [],
    };
    entry.groups.push(composerGroup);
    entry.groups.sort((left, right) => left.composerName.localeCompare(right.composerName, "zh-Hans-CN"));
  }

  let workEntry = composerGroup.works.find((item) => item.workId === work.id);
  if (!workEntry) {
    workEntry = {
      workId: work.id,
      title: work.title,
      href: workHref(work),
      recordings: [],
    };
    composerGroup.works.push(workEntry);
    composerGroup.works.sort((left, right) => left.title.localeCompare(right.title, "zh-Hans-CN"));
  }

  workEntry.recordings.push({
    id: recording.id,
    title: buildRecordingDisplayTitle(recording, library),
    href: recordingHref(recording),
  });
}

function collectPersonAliases(library: LibraryData, personLinks: PersonLinkConfig) {
  const aliasMap = new Map<string, Person[]>();

  for (const person of library.people) {
    const canonicalId = resolveCanonicalPersonId(person.id, personLinks);
    const bucket = aliasMap.get(canonicalId) ?? [];
    bucket.push(person);
    aliasMap.set(canonicalId, bucket);
  }

  return aliasMap;
}

function createSearchPersonKind(person: Person): SearchEntry["kind"] {
  if (person.roles.includes("conductor")) {
    return "conductor";
  }
  if (person.roles.includes("orchestra")) {
    return "orchestra";
  }
  return "person";
}

function groupTitlePath(work: Work, groupMap: Map<string, { title: string }>) {
  return work.groupIds.map((groupId) => groupMap.get(groupId)?.title).filter((value): value is string => Boolean(value));
}

function buildWorkSearchPrimaryText(work: Work) {
  return dedupe([work.title, stripCatalogueFromWorkSegment(work.titleLatin, work.catalogue), work.catalogue]).join(" / ");
}

function buildWorkSearchSecondaryText(composerHeading: string, groupPath: string[]) {
  return dedupe([composerHeading, ...groupPath]).join(" / ");
}

function createSearchEntry(input: Omit<SearchEntry, "matchTokens" | "aliasTokens"> & { matchTokens?: string[]; aliasTokens?: string[] }) {
  return {
    ...input,
    matchTokens: dedupe((input.matchTokens ?? []).map((value) => value.trim()).filter(Boolean)),
    aliasTokens: dedupe((input.aliasTokens ?? []).map((value) => value.trim()).filter(Boolean)),
  };
}

export function buildIndexes(
  library: LibraryData,
  personLinks: PersonLinkConfig = { canonicalPersonLinks: {} },
  articles: Article[] = [],
): LibraryIndexes {
  const composerMap = new Map(library.composers.map((composer) => [composer.id, composer]));
  const groupMap = new Map(library.workGroups.map((group) => [group.id, group]));
  const workMap = new Map(library.works.map((work) => [work.id, work]));
  const personMap = new Map(library.people.map((person) => [person.id, person]));
  const aliasMap = collectPersonAliases(library, personLinks);

  const composerTree: Record<string, ComposerTreeNode> = Object.fromEntries(
    library.composers.map((composer) => [composer.id, buildComposerRoot(composer)]),
  );

  for (const work of sortByKey(library.works)) {
    const composer = composerMap.get(work.composerId);
    if (!composer) {
      continue;
    }

    let currentNode = composerTree[composer.id];

    for (const groupId of work.groupIds) {
      const group = groupMap.get(groupId);
      if (!group) {
        continue;
      }

      let nextNode = currentNode.children.find((node) => node.id === group.id);
      if (!nextNode) {
        nextNode = {
          id: group.id,
          title: group.title,
          href: `${composerHref(composer)}#${group.id}`,
          children: [],
          works: [],
          sortKey: group.sortKey,
        };
        currentNode.children.push(nextNode);
        currentNode.children = sortByKey(currentNode.children);
      }
      currentNode = nextNode;
    }

    currentNode.works.push({
      id: work.id,
      title: work.title,
      href: workHref(work),
    });
    currentNode.works.sort((left, right) => left.title.localeCompare(right.title, "zh-Hans-CN"));
  }

  const conductorIndex: Record<string, RelationshipIndexEntry> = {};
  const orchestraIndex: Record<string, RelationshipIndexEntry> = {};
  const personAppearances = new Map<string, number>();

  for (const recording of sortByKey(library.recordings)) {
    const displayRecording = normalizeRecordingForDisplay(recording, personLinks, personMap);
    const work = workMap.get(recording.workId);
    if (!work) {
      continue;
    }

    const composer = composerMap.get(work.composerId);
    if (!composer) {
      continue;
    }

    for (const credit of recording.credits) {
      if (!credit.personId) {
        continue;
      }

      const canonicalPersonId = resolveCanonicalPersonId(credit.personId, personLinks);
      const person = personMap.get(canonicalPersonId) ?? personMap.get(credit.personId);
      if (!person) {
        continue;
      }

      personAppearances.set(person.id, (personAppearances.get(person.id) ?? 0) + 1);

      if (credit.role === "conductor" && person.roles.includes("conductor")) {
        ensureRelationshipEntry(conductorIndex, library, person, composer, work, displayRecording);
      }

      if (credit.role === "orchestra" && person.roles.includes("orchestra")) {
        ensureRelationshipEntry(orchestraIndex, library, person, composer, work, displayRecording);
      }
    }
  }

  const canonicalPeople: Record<string, string> = {};
  const personIndex: Record<string, PersonIndexEntry> = {};
  for (const person of library.people) {
    const canonicalId = resolveCanonicalPersonId(person.id, personLinks);
    canonicalPeople[person.id] = canonicalId;
    if (canonicalId !== person.id) {
      continue;
    }

    personIndex[person.id] = {
      id: person.id,
      canonicalId,
      name: getWebsiteDisplay(person).heading,
      href: personHref(person),
      roles: [...person.roles],
      appearanceCount: personAppearances.get(person.id) ?? 0,
    };
  }

  const searchIndex: SearchEntry[] = [];

  for (const composer of sortByKey(library.composers)) {
    const search = getEntitySearchTexts(composer);
    searchIndex.push(
      createSearchEntry({
        id: composer.id,
        kind: "composer",
        primaryText: search.primaryText,
        secondaryText: search.secondaryText,
        href: composerHref(composer),
        matchTokens: search.matchTokens,
        aliasTokens: search.aliasTokens,
      }),
    );
  }

  for (const person of sortByKey(library.people).filter((item) => resolveCanonicalPersonId(item.id, personLinks) === item.id)) {
    const aliases = aliasMap.get(person.id) ?? [person];
    const search = getEntitySearchTexts(person);
    searchIndex.push(
      createSearchEntry({
        id: person.id,
        kind: createSearchPersonKind(person),
        primaryText: search.primaryText,
        secondaryText: search.secondaryText || person.roles.join(" / "),
        href: personHref(person),
        matchTokens: [
          ...search.matchTokens,
          ...aliases.flatMap((alias) => [
            alias.name,
            alias.nameLatin,
            ...(alias.aliases ?? []),
          ]),
        ],
        aliasTokens: [...search.aliasTokens],
      }),
    );
  }

  for (const group of sortByKey(library.workGroups)) {
    const composer = composerMap.get(group.composerId);
    if (!composer) {
      continue;
    }

    searchIndex.push(
      createSearchEntry({
        id: group.id,
        kind: "workGroup",
        primaryText: group.title,
        secondaryText: getWebsiteDisplay(composer).heading,
        href: `${composerHref(composer)}#${group.id}`,
        matchTokens: [group.title, ...group.path, getWebsiteDisplay(composer).heading],
        aliasTokens: [],
      }),
    );
  }

  for (const work of sortByKey(library.works)) {
    const composer = composerMap.get(work.composerId);
    if (!composer) {
      continue;
    }
    const groupPath = groupTitlePath(work, groupMap);
    const composerHeading = getWebsiteDisplay(composer).heading;

    searchIndex.push(
      createSearchEntry({
        id: work.id,
        kind: "work",
        primaryText: buildWorkSearchPrimaryText(work),
        secondaryText: buildWorkSearchSecondaryText(composerHeading, groupPath),
        href: workHref(work),
        matchTokens: [work.title, work.titleLatin, work.catalogue, ...work.aliases, ...groupPath, composerHeading],
        aliasTokens: work.aliases,
      }),
    );
  }

  for (const recording of sortByKey(library.recordings)) {
    const displayRecording = normalizeRecordingForDisplay(recording, personLinks, personMap);
    const work = workMap.get(recording.workId);
    if (!work) {
      continue;
    }
    const composer = composerMap.get(work.composerId);
    const composerHeading = composer ? getWebsiteDisplay(composer).heading : "";
    const title = buildRecordingDisplayTitle(displayRecording, library);
    const creditTokens = [...recording.credits, ...displayRecording.credits]
      .flatMap((credit) => [credit.displayName, credit.label, credit.personId])
      .filter((value): value is string => Boolean(value));

    searchIndex.push(
      createSearchEntry({
        id: displayRecording.id,
        kind: "recording",
        primaryText: title,
        secondaryText: dedupe([work.title, composerHeading]).join(" / "),
        href: recordingHref(displayRecording),
        matchTokens: [
          title,
          work.title,
          work.titleLatin,
          work.catalogue,
          composerHeading,
          displayRecording.performanceDateText,
          displayRecording.venueText,
          displayRecording.albumTitle,
          displayRecording.label,
          ...creditTokens,
        ],
        aliasTokens: [],
      }),
    );
  }

  for (const article of [...articles].sort((left, right) => right.updatedAt.localeCompare(left.updatedAt))) {
    searchIndex.push(
      createSearchEntry({
        id: article.id,
        kind: "article",
        primaryText: article.title,
        secondaryText: article.summary,
        href: `/columns/${article.slug}/`,
        matchTokens: [article.title, article.slug, article.summary, article.markdown],
        aliasTokens: [],
      }),
    );
  }

  const conductorCount = library.people.filter((person) => person.roles.includes("conductor")).length;
  const orchestraCount = library.people.filter((person) => person.roles.includes("orchestra")).length;
  const lastUpdatedAt = sortByKey(library.works).at(-1)?.updatedAt ?? "";

  return {
    stats: {
      composerCount: library.composers.length,
      workCount: library.works.length,
      recordingCount: library.recordings.length,
      conductorCount,
      orchestraCount,
      lastUpdatedAt,
    },
    composerTree,
    canonicalPeople,
    conductorIndex,
    orchestraIndex,
    personIndex,
    searchIndex,
  };
}

