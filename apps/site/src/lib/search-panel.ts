export type SearchPanelKind = "composer" | "workGroup" | "work" | "recording" | "conductor" | "orchestra" | "person" | "article";

export type SearchPanelEntry = {
  id: string;
  kind: SearchPanelKind;
  primaryText: string;
  secondaryText: string;
  href: string;
  matchTokens: string[];
  aliasTokens: string[];
};

export type SearchPanelGroup = {
  kind: SearchPanelKind;
  page: number;
  pageSize: number;
  totalItems: number;
  totalPages: number;
  items: SearchPanelEntry[];
};

export type SearchPanelBuildState = {
  groups: SearchPanelGroup[];
  pagedKinds: SearchPanelKind[];
};

export const SEARCH_PANEL_KIND_ORDER: SearchPanelKind[] = [
  "composer",
  "conductor",
  "orchestra",
  "workGroup",
  "work",
  "recording",
  "article",
  "person",
];

type SearchPanelBuildOptions = {
  previewLimit?: number;
  queryPageSize?: number;
};

const DEFAULT_PREVIEW_LIMIT = 5;
const DEFAULT_QUERY_PAGE_SIZE = 10;
const DISPLAYABLE_KINDS: SearchPanelKind[] = ["composer", "conductor", "orchestra", "work", "recording", "article", "person"];

export function normalizeSearchQuery(value: string) {
  return String(value ?? "")
    .normalize("NFKC")
    .toLowerCase()
    .replace(/[路，。,'"“”‘’`?:;!()[\]{}\\/_-]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function matchesSearchEntry(entry: SearchPanelEntry, normalizedQuery: string) {
  if (!normalizedQuery) {
    return true;
  }

  const queryTokens = normalizedQuery.split(" ").filter(Boolean);
  const pool = [...(entry.matchTokens ?? []), ...(entry.aliasTokens ?? []), entry.primaryText, entry.secondaryText]
    .map((value) => normalizeSearchQuery(value))
    .filter(Boolean);

  return queryTokens.every((queryToken) => pool.some((token) => token.includes(queryToken)));
}

export function buildSearchGroups(
  entries: SearchPanelEntry[],
  query: string,
  pageByKind: Partial<Record<SearchPanelKind, number>>,
  options: SearchPanelBuildOptions = {},
) {
  const normalizedQuery = normalizeSearchQuery(query);
  const previewLimit = Math.max(1, Number(options.previewLimit) || DEFAULT_PREVIEW_LIMIT);
  const queryPageSize = Math.max(1, Number(options.queryPageSize) || DEFAULT_QUERY_PAGE_SIZE);
  const grouped = new Map<SearchPanelKind, SearchPanelEntry[]>();

  for (const entry of entries || []) {
    if (!DISPLAYABLE_KINDS.includes(entry.kind)) {
      continue;
    }
    if (!matchesSearchEntry(entry, normalizedQuery)) {
      continue;
    }
    const bucket = grouped.get(entry.kind) ?? [];
    bucket.push(entry);
    grouped.set(entry.kind, bucket);
  }

  const groups = SEARCH_PANEL_KIND_ORDER.filter((kind) => DISPLAYABLE_KINDS.includes(kind) && grouped.has(kind)).map((kind) => {
    const items = grouped.get(kind) ?? [];
    if (!normalizedQuery) {
      return {
        kind,
        page: 1,
        pageSize: previewLimit,
        totalItems: items.length,
        totalPages: 1,
        items: items.slice(0, previewLimit),
      } satisfies SearchPanelGroup;
    }

    const totalPages = Math.max(1, Math.ceil(items.length / queryPageSize));
    const safePage = Math.min(Math.max(1, Number(pageByKind?.[kind]) || 1), totalPages);
    const startIndex = (safePage - 1) * queryPageSize;

    return {
      kind,
      page: safePage,
      pageSize: queryPageSize,
      totalItems: items.length,
      totalPages,
      items: items.slice(startIndex, startIndex + queryPageSize),
    } satisfies SearchPanelGroup;
  });

  if (!normalizedQuery) {
    return groups;
  }

  const pagedKinds = SEARCH_PANEL_KIND_ORDER.filter((kind) => {
    const page = Number(pageByKind?.[kind]) || 1;
    return page > 1;
  });

  if (!pagedKinds.length) {
    return groups;
  }

  return groups.filter((group) => pagedKinds.includes(group.kind));
}
